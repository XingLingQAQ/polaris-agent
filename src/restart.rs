use anyhow::Result;

#[cfg(not(windows))]
pub fn prepare(_cfg: &crate::config::Config) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
pub fn prepare(cfg: &crate::config::Config) -> Result<()> {
    use anyhow::{bail, Context};
    use std::{fs, os::windows::process::CommandExt, process::{Command, Stdio}, thread, time::{Duration, Instant}};

    let installation = crate::management::load(&cfg.config_path)?;
    anyhow::ensure!(installation.manager == "windows-task", "restart requires an installed scheduled task");
    let executable = std::env::current_exe()?.canonicalize()?;
    anyhow::ensure!(executable == installation.executable.canonicalize()?, "the running Agent does not match its installation");
    let directory = executable.parent().context("executable has no directory")?;
    let script = directory.join(format!(".polaris-restart-{}.ps1", std::process::id()));
    let ready = directory.join(format!(".polaris-restart-{}.ready", std::process::id()));
    anyhow::ensure!(!script.exists() && !ready.exists(), "a restart handoff is already present");
    crate::storage::write_private(&script, include_bytes!("updater/windows-restart.ps1"))?;
    let outcome = (|| -> Result<()> {
        let log = fs::OpenOptions::new().create(true).append(true).open(executable.with_extension("restart.log"))?;
        let mut helper = Command::new("powershell.exe")
            .args(["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File"])
            .arg(&script).arg("-AgentProcessId").arg(std::process::id().to_string())
            .arg("-Executable").arg(&executable).arg("-TaskName").arg(&installation.service_name)
            .arg("-ReadyFile").arg(&ready).stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?)).stderr(Stdio::from(log))
            .creation_flags(0x0100_0000 | 0x0800_0000 | 0x0000_0200)
            .spawn().context("cannot start the Windows restart helper")?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if matches!(fs::read_to_string(&ready).as_deref(), Ok("ready")) { return Ok(()); }
            if let Some(status) = helper.try_wait()? { bail!("Windows restart helper failed: {status}"); }
            if Instant::now() >= deadline {
                let _ = helper.kill();
                let _ = helper.wait();
                bail!("Windows restart helper did not become ready");
            }
            thread::sleep(Duration::from_millis(50));
        }
    })();
    if outcome.is_err() { let _ = fs::remove_file(&script); let _ = fs::remove_file(&ready); }
    outcome
}
