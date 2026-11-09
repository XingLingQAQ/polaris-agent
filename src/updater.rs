use anyhow::{bail, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::mpsc;
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use url::Url;

const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_BINARY_BYTES: u64 = 64 * 1024 * 1024;
const VERSION_OUTPUT_LIMIT: u64 = 4096;
const VERSION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct Manifest {
    version: String,
    tag: String,
    repository: String,
    platforms: Vec<Release>,
}

#[derive(Deserialize)]
struct Release {
    key: String,
    file: String,
    size: u64,
    sha256: String,
    version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateResult {
    AlreadyCurrent,
    Applied,
    Staged,
    RestartPending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCheck {
    pub current: String,
    pub latest: String,
    pub available: bool,
    pub platform: &'static str,
}

pub fn check() -> Result<UpdateCheck> {
    let (_, _, manifest) = fetch_manifest()?;
    let current = env!("CARGO_PKG_VERSION");
    let latest = manifest.version;
    Ok(UpdateCheck {
        current: current.into(),
        available: update_available(&latest, current)?,
        latest,
        platform: platform_key(),
    })
}

pub fn update_running(cfg: &crate::config::Config) -> Result<UpdateResult> {
    #[cfg(not(windows))]
    let _ = cfg;
    let outcome = update(false)?;
    #[cfg(windows)]
    if outcome == UpdateResult::Staged {
        if let Err(error) = schedule_windows_update(cfg) {
            if let Ok(executable) = std::env::current_exe() {
                if let Ok(stage) = stage_path(&executable) {
                    let _ = fs::remove_file(stage);
                }
            }
            return Err(error);
        }
        return Ok(UpdateResult::RestartPending);
    }
    Ok(outcome)
}

#[cfg(windows)]
fn schedule_windows_update(cfg: &crate::config::Config) -> Result<()> {
    use std::os::windows::process::CommandExt;
    let installation = crate::management::load(&cfg.config_path)?;
    anyhow::ensure!(
        installation.manager == "windows-task",
        "automatic updates require an installed scheduled task"
    );
    anyhow::ensure!(
        std::env::current_exe()?.canonicalize()? == installation.executable.canonicalize()?,
        "the running Agent does not match its installation"
    );
    let executable = installation.executable;
    let stage = stage_path(&executable)?;
    let version = executable_version(&stage)?;
    let digest = hex::encode(Sha256::digest(fs::read(&stage)?));
    let script = stage.with_extension("update.ps1");
    let ready = stage.with_extension("ready");
    anyhow::ensure!(
        !script.exists() && !ready.exists(),
        "an update handoff is already present"
    );
    let mut helper = None;
    let outcome = (|| -> Result<()> {
        crate::storage::write_private(&script, include_bytes!("updater/windows-update.ps1"))?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(executable.with_extension("update.log"))?;
        helper = Some(
            Command::new("powershell.exe")
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&script)
                .arg("-AgentProcessId")
                .arg(std::process::id().to_string())
                .arg("-Executable")
                .arg(&executable)
                .arg("-Staged")
                .arg(&stage)
                .arg("-TaskName")
                .arg(&installation.service_name)
                .arg("-ReadyFile")
                .arg(&ready)
                .arg("-ExpectedHash")
                .arg(&digest)
                .arg("-ExpectedVersion")
                .arg(&version)
                .stdin(Stdio::null())
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                // Detach from the task's job before the monitored process exits. / 监控进程退出前脱离计划任务的作业对象。
                .creation_flags(0x0100_0000 | 0x0800_0000 | 0x0000_0200)
                .spawn()
                .context("cannot start the Windows update helper")?,
        );
        let helper = helper.as_mut().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if matches!(fs::read_to_string(&ready).as_deref(), Ok("ready")) {
                return Ok(());
            }
            if let Some(status) = helper.try_wait()? {
                bail!("Windows update helper failed: {status}; see the update log");
            }
            if Instant::now() >= deadline {
                bail!("Windows update helper did not become ready; current Agent is still running");
            }
            thread::sleep(Duration::from_millis(50));
        }
    })();
    if outcome.is_err() {
        if let Some(mut helper) = helper {
            let _ = helper.kill();
            let _ = helper.wait();
        }
        let _ = fs::remove_file(&script);
        let _ = fs::remove_file(&ready);
    }
    outcome
}

pub fn update(rollback: bool) -> Result<UpdateResult> {
    let executable = std::env::current_exe().context("cannot locate the running executable")?;
    if rollback {
        rollback_executable(&executable)?;
        return Ok(UpdateResult::Applied);
    }
    if is_container() {
        bail!("Agent self-update is disabled in containers; replace the container image");
    }

    let (repository, client, manifest) = fetch_manifest()?;
    let current = env!("CARGO_PKG_VERSION");
    if !update_available(&manifest.version, current)? {
        println!("polaris-agent is already at {current}");
        return Ok(UpdateResult::AlreadyCurrent);
    }
    let platform = platform_key();
    let release = manifest
        .platforms
        .iter()
        .find(|item| item.key == platform)
        .context("the release manifest has no build for this platform")?;
    validate_release(release, &manifest.version, platform)?;
    if release.size > MAX_BINARY_BYTES {
        bail!("release exceeds the 64 MiB download limit");
    }

    let binary_url = release_url(&repository, &manifest.tag, &release.file);
    let response = github_get(&client, &binary_url)?;
    let bytes = read_bounded(response.into_reader(), MAX_BINARY_BYTES)?;
    verify_package(&bytes, release)?;

    #[cfg(windows)]
    {
        if !bytes.starts_with(b"MZ") {
            bail!("download is not a Windows executable");
        }
        let staged = stage_path(&executable)?;
        write_stage(&staged, &bytes)?;
        println!(
            "verified Agent {} staged at {}; stop the service and replace the executable manually",
            manifest.version,
            staged.display()
        );
        return Ok(UpdateResult::Staged);
    }

    #[cfg(unix)]
    {
        install_unix(&executable, &bytes, &manifest.version)?;
        println!(
            "installed polaris-agent {}; restart the service or run it again",
            manifest.version
        );
        Ok(UpdateResult::Applied)
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (executable, bytes, manifest.version);
        bail!("live Agent replacement is unsupported on this platform; use the installer")
    }
}

fn repository() -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(include_str!("../release-repository.json"))?;
    let repo = value["repository"].as_str().context("missing release repository")?;
    let parts: Vec<_> = repo.split('/').collect();
    anyhow::ensure!(parts.len() == 2 && parts.iter().all(|part| !part.is_empty() && part.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))), "invalid release repository");
    Ok(repo.into())
}

fn release_url(repository: &str, tag: &str, file: &str) -> String {
    format!("https://github.com/{repository}/releases/download/{tag}/{file}")
}

fn github_get(client: &ureq::Agent, url: &str) -> Result<ureq::Response> {
    let mut next = url.to_owned();
    for _ in 0..5 {
        let parsed = Url::parse(&next)?;
        anyhow::ensure!(parsed.scheme() == "https" && parsed.username().is_empty() && parsed.password().is_none() && parsed.port().is_none() && matches!(parsed.host_str(), Some("api.github.com" | "github.com" | "release-assets.githubusercontent.com")), "invalid GitHub release redirect");
        let response = match client.get(&next).set("User-Agent", "polaris-agent").set("Accept", "application/vnd.github+json").call() {
            Ok(response) | Err(ureq::Error::Status(_, response)) => response,
            Err(error) => return Err(error.into()),
        };
        if (300..400).contains(&response.status()) {
            let location = response.header("Location").context("release redirect has no location")?;
            next = parsed.join(location)?.to_string();
            continue;
        }
        if response.status() >= 400 {
            bail!("GitHub release request returned HTTP {}", response.status());
        }
        return Ok(response);
    }
    bail!("too many GitHub release redirects")
}

fn fetch_manifest() -> Result<(String, ureq::Agent, Manifest)> {
    let repository = repository()?;
    let client = ureq::AgentBuilder::new().redirects(0).timeout_connect(Duration::from_secs(5)).timeout_read(Duration::from_secs(20)).timeout_write(Duration::from_secs(20)).build();
    let api = format!("https://api.github.com/repos/{repository}/releases/latest");
    let response = github_get(&client, &api).with_context(|| format!("cannot query latest Agent release for {repository}; check that a release exists and GitHub is reachable"))?;
    let metadata: serde_json::Value = serde_json::from_slice(&read_bounded(response.into_reader(), 1024 * 1024)?)?;
    let tag = metadata["tag_name"].as_str().context("release has no tag")?;
    let version = tag.strip_prefix('v').context("invalid release tag")?;
    let components: Vec<_> = version.split('.').collect();
    anyhow::ensure!(components.len() == 3 && components.iter().all(|part| !part.is_empty() && (part.len() == 1 || !part.starts_with('0')) && part.bytes().all(|c| c.is_ascii_digit())), "invalid release tag");
    anyhow::ensure!(newer_version(version, "0.0.0") || version == "0.0.0", "invalid release version");
    anyhow::ensure!(metadata["draft"] == false && metadata["prerelease"] == false, "release is not stable");
    let assets = metadata["assets"].as_array().context("release has no assets")?;
    let asset_valid = |name: &str, size: Option<u64>| -> bool {
        let matches: Vec<_> = assets.iter().filter(|item| item["name"].as_str() == Some(name)).collect();
        matches.len() == 1 && matches[0]["browser_download_url"].as_str() == Some(release_url(&repository, tag, name).as_str()) && size.map_or(true, |n| matches[0]["size"].as_u64() == Some(n))
    };
    anyhow::ensure!(asset_valid("manifest.json", None), "release manifest is missing");
    let raw = read_bounded(github_get(&client, &release_url(&repository, tag, "manifest.json"))?.into_reader(), MAX_MANIFEST_BYTES)?;
    let manifest: Manifest = serde_json::from_slice(&raw).context("invalid release manifest")?;
    anyhow::ensure!(manifest.repository == repository && manifest.tag == tag && manifest.version == version && !manifest.platforms.is_empty(), "release manifest identity mismatch");
    let mut seen = std::collections::HashSet::new();
    for release in &manifest.platforms {
        anyhow::ensure!(["linux-x86_64", "linux-aarch64", "windows-x86_64", "darwin-x86_64", "darwin-aarch64"].contains(&release.key.as_str()) && seen.insert(&release.key), "invalid release platform");
        validate_release(release, version, &release.key)?;
        anyhow::ensure!(asset_valid(&release.file, Some(release.size)), "release asset missing or size mismatch");
    }
    Ok((repository, client, manifest))
}

fn update_available(latest: &str, current: &str) -> Result<bool> {
    if latest == current { return Ok(false); }
    if !newer_version(latest, current) { bail!("release {latest} is not newer than {current}"); }
    Ok(true)
}
fn platform_key() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("macos", "x86_64") => "darwin-x86_64",
        ("macos", "aarch64") => "darwin-aarch64",
        ("windows", "x86_64") => "windows-x86_64",
        _ => "unsupported",
    }
}

fn validate_release(release: &Release, version: &str, platform: &str) -> Result<()> {
    let extension = if platform == "windows-x86_64" {
        ".exe"
    } else {
        ""
    };
    let expected = format!("polaris-agent-{version}-{platform}{extension}");
    if release.version != version
        || release.file != expected
        || release.key != platform
        || release.size == 0
        || release.size > MAX_BINARY_BYTES
        || release.sha256.len() != 64
        || !release.sha256.bytes().all(|b| b.is_ascii_hexdigit())
    {
        bail!("invalid release entry for {platform}");
    }
    Ok(())
}

fn verify_package(bytes: &[u8], release: &Release) -> Result<()> {
    if bytes.len() as u64 != release.size
        || hex::encode(Sha256::digest(bytes)) != release.sha256.to_ascii_lowercase()
    {
        bail!("release size or SHA-256 does not match the manifest");
    }
    Ok(())
}

fn newer_version(candidate: &str, current: &str) -> bool {
    fn parts(value: &str) -> Option<[u64; 3]> {
        let values: Vec<_> = value
            .split('.')
            .map(str::parse)
            .collect::<std::result::Result<_, _>>()
            .ok()?;
        values.try_into().ok()
    }
    matches!((parts(candidate), parts(current)), (Some(next), Some(now)) if next > now)
}

fn read_bounded(reader: impl Read, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("download exceeds its size limit");
    }
    Ok(bytes)
}

fn stage_path(executable: &Path) -> Result<std::path::PathBuf> {
    let parent = executable
        .parent()
        .context("executable has no parent directory")?;
    let name = executable
        .file_name()
        .context("executable has no file name")?
        .to_string_lossy();
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    Ok(parent.join(format!(".{name}.update-{}{suffix}", std::process::id())))
}

fn write_stage(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .context("cannot create staged Agent file")?;
    let result = file.write_all(bytes).and_then(|_| file.sync_all());
    if let Err(error) = result {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(error).context("cannot write staged Agent file");
    }
    Ok(())
}

#[cfg(unix)]
fn replace_staged_with<F>(stage: &Path, executable: &Path, rename: F) -> std::io::Result<()>
where
    F: FnOnce(&Path, &Path) -> std::io::Result<()>,
{
    rename(stage, executable)
}

#[cfg(unix)]
fn validate_executable(path: &Path, expected: &str) -> Result<()> {
    let (status, output) = run_version(path)?;
    let output = String::from_utf8_lossy(&output);
    if !status || output.trim() != format!("polaris-agent {expected}") {
        bail!("staged Agent version check failed");
    }
    Ok(())
}

fn run_version(path: &Path) -> Result<(bool, Vec<u8>)> {
    let mut child = Command::new(path)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("Agent version check could not start")?;
    let stdout = child
        .stdout
        .take()
        .context("Agent version check has no stdout")?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = sender.send(read_bounded(stdout, VERSION_OUTPUT_LIMIT));
    });
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if started.elapsed() >= VERSION_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let output = receiver
        .recv_timeout(Duration::from_millis(250))
        .map_err(|_| anyhow::anyhow!("Agent version output timed out"))??;
    Ok((status.is_some_and(|value| value.success()), output))
}

#[cfg(unix)]
// 校验暂存程序后保留硬链接备份，最后以重命名替换运行文件。
// Validate the staged executable, retain a hard-link backup, then rename into place.
fn install_unix(executable: &Path, bytes: &[u8], version: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let stage = stage_path(executable)?;
    write_stage(&stage, bytes)?;
    let permissions = match fs::metadata(executable) {
        Ok(metadata) => metadata.permissions(),
        Err(error) => {
            let _ = fs::remove_file(&stage);
            return Err(error).context("cannot read current Agent permissions");
        }
    };
    if let Err(error) = fs::set_permissions(&stage, fs::Permissions::from_mode(permissions.mode()))
    {
        let _ = fs::remove_file(&stage);
        return Err(error).context("cannot preserve Agent permissions");
    }
    if let Err(error) = validate_executable(&stage, version) {
        let _ = fs::remove_file(&stage);
        return Err(error);
    }

    let parent = executable
        .parent()
        .context("executable has no parent directory")?;
    let previous = parent.join(format!(
        ".{}.previous",
        executable.file_name().unwrap().to_string_lossy()
    ));
    let backup_stage = parent.join(format!(
        ".{}.previous-{}",
        executable.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    if let Err(error) = fs::hard_link(executable, &backup_stage) {
        let _ = fs::remove_file(&stage);
        return Err(error).context("cannot create previous-version rollback copy");
    }
    if let Err(error) = fs::rename(&backup_stage, &previous) {
        let _ = fs::remove_file(&backup_stage);
        let _ = fs::remove_file(&stage);
        return Err(error).context("cannot retain previous Agent version");
    }
    if let Err(error) = replace_staged_with(&stage, executable, |from, to| fs::rename(from, to)) {
        let _ = fs::remove_file(&stage);
        return Err(error)
            .context("atomic Agent replacement failed; original executable is intact");
    }
    Ok(())
}

fn rollback_executable(executable: &Path) -> Result<()> {
    let parent = executable
        .parent()
        .context("executable has no parent directory")?;
    let name = executable
        .file_name()
        .context("executable has no file name")?
        .to_string_lossy();
    let previous = parent.join(format!(".{name}.previous"));
    if !previous.is_file() {
        bail!("no previous Agent binary is available");
    }
    let candidate_version = executable_version(&previous)?;
    let current_version = env!("CARGO_PKG_VERSION");
    if candidate_version == current_version {
        bail!("previous binary is not an older version");
    }

    #[cfg(unix)]
    {
        let temporary = parent.join(format!(".{name}.rollback-{}", std::process::id()));
        fs::hard_link(executable, &temporary)
            .context("cannot preserve current Agent for rollback recovery")?;
        if let Err(error) = fs::rename(&previous, executable) {
            let _ = fs::remove_file(&temporary);
            return Err(error)
                .context("Agent rollback replacement failed; current executable is intact");
        }
        fs::rename(&temporary, &previous)
            .context("rollback installed, but current version could not be retained")?;
        println!("restored polaris-agent {candidate_version}; run polaris-agent rollback again to switch back");
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _ = (executable, candidate_version);
        bail!("Windows rollback requires manual replacement of the staged binary")
    }
}

fn executable_version(path: &Path) -> Result<String> {
    let (status, output) = run_version(path)?;
    if !status {
        bail!("previous Agent version check failed");
    }
    let line = String::from_utf8_lossy(&output).trim().to_string();
    line.strip_prefix("polaris-agent ")
        .map(str::to_owned)
        .context("invalid previous Agent version output")
}

pub fn is_container() -> bool {
    #[cfg(target_os = "linux")]
    {
        Path::new("/.dockerenv").exists()
            || fs::read_to_string("/proc/1/cgroup")
                .map(|value| {
                    value.contains("docker")
                        || value.contains("containerd")
                        || value.contains("kubepods")
                })
                .unwrap_or(false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn package_verification_rejects_bad_hash_and_truncation() {
        let bytes = b"release binary";
        let release = Release {
            key: "linux-x86_64".into(),
            file: "polaris-agent-0.2.0-linux-x86_64".into(),
            size: bytes.len() as u64,
            sha256: hex::encode(Sha256::digest(bytes)),
            version: "0.2.0".into(),
        };
        assert!(verify_package(bytes, &release).is_ok());
        let mut bad_hash = Release {
            sha256: "0".repeat(64),
            ..release
        };
        assert!(verify_package(bytes, &bad_hash).is_err());
        bad_hash.sha256 = hex::encode(Sha256::digest(bytes));
        bad_hash.size += 1;
        assert!(verify_package(bytes, &bad_hash).is_err());
    }

    #[test]
    fn manifest_platform_and_filename_must_match() {
        let release = Release {
            key: "linux-aarch64".into(),
            file: "polaris-agent-0.2.0-linux-aarch64".into(),
            size: 1,
            sha256: "a".repeat(64),
            version: "0.2.0".into(),
        };
        assert!(validate_release(&release, "0.2.0", "linux-x86_64").is_err());
        assert!(validate_release(&release, "0.2.0", "linux-aarch64").is_ok());
    }

    #[test]
    fn version_check_is_bounded_and_exact() {
        let dir = std::env::temp_dir().join(format!(
            "uptime-polaris-version-check-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let target = dir.join("fixture.sh");
        fs::write(&target, "#!/bin/sh\nprintf 'polaris-agent 0.2.0\\n'\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
            assert!(validate_executable(&target, "0.2.0").is_ok());
            assert!(validate_executable(&target, "0.2").is_err());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn replace_failure_preserves_original_and_config() {
        let dir = fixture_dir("replace-failure");
        let executable = dir.join("agent");
        let config = dir.join("config.json");
        let stage = dir.join("stage");
        fs::write(&executable, b"old image").unwrap();
        fs::write(&config, br#"{"token":"fixture","report_interval":60}"#).unwrap();
        fs::write(&stage, b"new image").unwrap();
        let result = replace_staged_with(&stage, &executable, |_, _| {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&executable).unwrap(), b"old image");
        assert_eq!(
            fs::read(&config).unwrap(),
            br#"{"token":"fixture","report_interval":60}"#
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn successful_swap_leaves_config_and_previous_image_available() {
        use std::os::unix::fs::PermissionsExt;

        let dir = fixture_dir("swap");
        let executable = dir.join("polaris-agent");
        let config = dir.join("config.json");
        let stage = dir.join("stage");
        let previous = dir.join(".polaris-agent.previous");
        fs::write(&executable, b"old binary").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o750)).unwrap();
        fs::write(&config, br#"{"token":"kept","report_interval":60}"#).unwrap();
        fs::write(&stage, b"new binary").unwrap();
        fs::hard_link(&executable, &previous).unwrap();
        replace_staged_with(&stage, &executable, fs::rename).unwrap();
        assert_eq!(fs::read(&executable).unwrap(), b"new binary");
        assert_eq!(fs::read(&previous).unwrap(), b"old binary");
        assert_eq!(
            fs::read(&config).unwrap(),
            br#"{"token":"kept","report_interval":60}"#
        );
        assert_eq!(
            fs::metadata(&executable).unwrap().permissions().mode() & 0o777,
            0o750
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn rollback_swaps_back_and_keeps_the_new_image_as_next_rollback() {
        let dir = fixture_dir("rollback");
        let executable = dir.join("polaris-agent");
        let previous = dir.join(".polaris-agent.previous");
        fs::write(&executable, "#!/bin/sh\necho 'polaris-agent 0.2.0'\n").unwrap();
        fs::write(&previous, "#!/bin/sh\necho 'polaris-agent 0.1.0'\n").unwrap();
        for path in [&executable, &previous] {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        rollback_executable(&executable).unwrap();
        assert_eq!(executable_version(&executable).unwrap(), "0.1.0");
        assert_eq!(executable_version(&previous).unwrap(), "0.2.0");
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    fn fixture_dir(name: &str) -> std::path::PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "uptime-polaris-{name}-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn versions_must_be_newer() {
        assert!(newer_version("0.2.0", "0.1.9"));
        assert!(!newer_version("0.2.0", "0.2.0"));
        assert!(!newer_version("0.1.9", "0.2.0"));
    }

    #[cfg(windows)]
    #[test]
    fn staged_windows_binary_keeps_an_executable_extension() {
        let staged = stage_path(Path::new(r"C:\Agent\polaris-agent.exe")).unwrap();
        assert_eq!(staged.extension().unwrap(), "exe");
        assert_eq!(staged.parent().unwrap(), Path::new(r"C:\Agent"));
    }

    #[test]
    fn update_availability_is_strictly_newer() {
        assert!(update_available("0.4.0", "0.3.0").unwrap());
        assert!(!update_available("0.3.0", "0.3.0").unwrap());
        assert!(update_available("0.2.9", "0.3.0").is_err());
    }
}
