use super::{
    save, CommandAction, Installation, ProfileChange, Registration, Scope, COMMAND_NAME,
    SHORT_COMMAND_NAME,
};
use anyhow::{ensure, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};

const MARKER: &str = "polaris-agent managed command";

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn script(installation: &Installation, name: &str) -> Result<String> {
    let executable = installation.executable.to_string_lossy();
    let config = installation.config.to_string_lossy();
    ensure!(
        !executable.contains(['\n', '\r']) && !config.contains(['\n', '\r']),
        "command paths cannot contain newlines"
    );
    if cfg!(windows) {
        // Leave batch context before unregister removes this file.
        // 注销删除此文件前先退出批处理上下文。
        Ok(format!("@echo off\r\nrem {MARKER}\r\n(goto) 2>nul & powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"%~dp0{name}.ps1\" %*\r\n"))
    } else {
        Ok(format!(
            "#!/bin/sh\n# {MARKER}\nPOLARIS_AGENT_CONFIG={} exec {} \"$@\"\n",
            quote(&config),
            quote(&executable)
        ))
    }
}

#[cfg(windows)]
fn powershell_script(installation: &Installation) -> String {
    let executable = installation
        .executable
        .to_string_lossy()
        .replace('\'', "''");
    let config = installation.config.to_string_lossy().replace('\'', "''");
    format!("\u{feff}# {MARKER}\r\n$env:POLARIS_AGENT_CONFIG = '{config}'\r\n& '{executable}' @args\r\nexit $LASTEXITCODE\r\n")
}

fn command_files(
    installation: &Installation,
    path: &Path,
    direct: bool,
) -> Result<Vec<(PathBuf, String)>> {
    let directory = path.parent().context("command path has no parent")?;
    let short_path = directory.join(if cfg!(windows) {
        "pa.cmd"
    } else {
        SHORT_COMMAND_NAME
    });
    let mut files = Vec::new();
    for (name, path) in [
        (COMMAND_NAME, path.to_path_buf()),
        (SHORT_COMMAND_NAME, short_path),
    ] {
        if name == COMMAND_NAME && direct {
            continue;
        }
        files.push((path.clone(), script(installation, name)?));
        #[cfg(windows)]
        files.push((path.with_extension("ps1"), powershell_script(installation)));
    }
    Ok(files)
}

fn check_owned(path: &Path, expected: &str) -> Result<()> {
    // 只处理内容完全匹配的普通文件，避免覆盖链接或其他安装的命令。
    // Only handle regular files with exact contents; avoid links and other installations' commands.
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("cannot inspect command file"),
    };
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_file(),
        "command path is linked or is not a file: {}",
        path.display()
    );
    ensure!(
        fs::read_to_string(path).unwrap_or_default() == expected,
        "command file was modified or belongs to another installation: {}",
        path.display()
    );
    Ok(())
}

fn target(installation: &Installation, scope: Scope) -> Result<(PathBuf, PathBuf)> {
    #[cfg(windows)]
    let directory = installation
        .executable
        .parent()
        .context("executable has no parent directory")?
        .join("commands");
    #[cfg(not(windows))]
    let directory = if scope == Scope::System {
        PathBuf::from("/usr/local/bin")
    } else {
        PathBuf::from(
            std::env::var_os("HOME").context("HOME is required for user command registration")?,
        )
        .join(".local/bin")
    };
    let _ = (installation, scope);
    let path = directory.join(if cfg!(windows) {
        "polaris-agent.cmd"
    } else {
        COMMAND_NAME
    });
    Ok((directory, path))
}

fn profile_changes(directory: &Path, scope: Scope) -> Result<Vec<ProfileChange>> {
    if cfg!(windows)
        || scope == Scope::System
        || std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .any(|path| path == directory)
    {
        return Ok(vec![]);
    }
    let home = PathBuf::from(std::env::var_os("HOME").context("HOME is required")?);
    let shell = std::env::var("SHELL").unwrap_or_default();
    let files: Vec<_> = if shell.ends_with("zsh") {
        vec![".zprofile", ".zshrc"]
    } else if shell.ends_with("fish") {
        vec![".config/fish/conf.d/polaris-agent.fish"]
    } else if shell.ends_with("bash") {
        vec![".profile", ".bashrc"]
    } else {
        vec![".profile"]
    };
    let body = if shell.ends_with("fish") {
        format!("fish_add_path {}", quote(&directory.to_string_lossy()))
    } else {
        format!(
            "export PATH={}:\"$PATH\"",
            quote(&directory.to_string_lossy())
        )
    };
    let block = format!("\n# >>> {MARKER} >>>\n{body}\n# <<< {MARKER} <<<\n");
    Ok(files
        .into_iter()
        .map(|file| {
            let path = home.join(file);
            ProfileChange {
                created: !path.exists(),
                path,
                block: block.clone(),
            }
        })
        .collect())
}

fn edit_profile(change: &ProfileChange, add: bool) -> Result<()> {
    // 标记块用于精确识别本次注册写入的 PATH 配置。
    // Markers identify the PATH configuration written by this registration.
    if let Ok(metadata) = fs::symlink_metadata(&change.path) {
        ensure!(
            !metadata.file_type().is_symlink() && metadata.is_file(),
            "refusing to edit a linked or non-file profile: {}",
            change.path.display()
        );
    }
    let content = match fs::read_to_string(&change.path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error).context("cannot read shell profile"),
    };
    if add {
        if !content.contains(&change.block) {
            ensure!(
                !content.contains(MARKER),
                "another Polaris command registration already owns this shell profile"
            );
            if let Some(parent) = change.path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&change.path, content + &change.block)?;
        }
    } else if content.contains(&change.block) {
        let updated = content.replace(&change.block, "");
        if change.created && updated.is_empty() {
            fs::remove_file(&change.path)?;
        } else {
            fs::write(&change.path, updated)?;
        }
    }
    Ok(())
}

#[cfg(windows)]
fn registry_path(scope: Scope, directory: &Path, remove: bool) -> Result<bool> {
    use winreg::{
        enums::{HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE, KEY_READ, KEY_WRITE, REG_EXPAND_SZ},
        RegKey, RegValue,
    };
    let (hive, key) = if scope == Scope::System {
        (
            HKEY_LOCAL_MACHINE,
            "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment",
        )
    } else {
        (HKEY_CURRENT_USER, "Environment")
    };
    let (key, _) = RegKey::predef(hive)
        .create_subkey_with_flags(key, KEY_READ | KEY_WRITE)
        .context("cannot update PATH; system registration requires administrator rights")?;
    let old: String = match key.get_value("Path") {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error).context("cannot read the existing PATH value"),
    };
    let directory = directory.to_string_lossy();
    let matches = |entry: &str| {
        entry
            .trim()
            .trim_end_matches(['\\', '/'])
            .eq_ignore_ascii_case(directory.trim_end_matches(['\\', '/']))
    };
    let present = old.split(';').any(matches);
    if (!remove && present) || (remove && !present) {
        return Ok(false);
    }
    let next = if remove {
        old.split(';')
            .filter(|entry| !matches(entry))
            .collect::<Vec<_>>()
            .join(";")
    } else if old.is_empty() {
        directory.into_owned()
    } else {
        format!("{};{}", old.trim_end_matches(';'), directory)
    };
    let bytes = next
        .encode_utf16()
        .chain(Some(0))
        .flat_map(u16::to_le_bytes)
        .collect();
    key.set_raw_value(
        "Path",
        &RegValue {
            bytes,
            vtype: REG_EXPAND_SZ,
        },
    )?;
    let message: Vec<u16> = "Environment\0".encode_utf16().collect();
    unsafe {
        use windows_sys::Win32::UI::WindowsAndMessaging::{
            SendMessageTimeoutW, HWND_BROADCAST, SMTO_ABORTIFHUNG, WM_SETTINGCHANGE,
        };
        SendMessageTimeoutW(
            HWND_BROADCAST,
            WM_SETTINGCHANGE,
            0,
            message.as_ptr() as isize,
            SMTO_ABORTIFHUNG,
            1000,
            std::ptr::null_mut(),
        );
    }
    Ok(true)
}

fn register(
    mut installation: Installation,
    scope: Scope,
    directory: Option<PathBuf>,
    no_path: bool,
    dry_run: bool,
) -> Result<()> {
    let (mut directory_path, mut path) = target(&installation, scope)?;
    if let Some(directory) = directory {
        ensure!(
            directory.is_absolute(),
            "command directory must be an absolute path"
        );
        path = directory.join(if cfg!(windows) {
            "polaris-agent.cmd"
        } else {
            COMMAND_NAME
        });
        directory_path = directory;
    }
    let directory = directory_path;
    let direct = path == installation.executable;
    let files = command_files(&installation, &path, direct)?;
    for (path, content) in &files {
        check_owned(path, content)?;
    }
    let mut profiles = if no_path {
        vec![]
    } else {
        profile_changes(&directory, scope)?
    };
    if let Some(previous) = &installation.registration {
        ensure!(
            previous.path == path && previous.scope == scope,
            "unregister the existing command before changing its scope"
        );
        for profile in &previous.profiles {
            if let Some(current) = profiles
                .iter_mut()
                .find(|current| current.path == profile.path)
            {
                current.created = profile.created;
            } else {
                profiles.push(profile.clone());
            }
        }
    }
    if dry_run {
        for (path, _) in &files {
            println!(
                "register command: {} -> {}",
                path.display(),
                installation.executable.display()
            );
        }
        for profile in profiles {
            println!("PATH profile: {}", profile.path.display());
        }
        return Ok(());
    }
    let previous = installation.registration.clone();
    installation.registration = Some(Registration {
        path: path.clone(),
        directory: directory.clone(),
        scope,
        path_added: false,
        direct,
        profiles,
    });
    save(&installation)?;
    fs::create_dir_all(&directory)?;
    for (path, content) in &files {
        fs::write(path, content)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
    }
    #[cfg(windows)]
    {
        let added = if no_path {
            false
        } else {
            registry_path(scope, &directory, false)?
        };
        installation.registration.as_mut().unwrap().path_added = added
            || previous.is_some_and(|record| {
                record.path_added && record.directory == directory && record.scope == scope
            });
    }
    #[cfg(not(windows))]
    let _ = previous;
    for profile in &installation.registration.as_ref().unwrap().profiles {
        edit_profile(profile, true)?;
    }
    save(&installation)?;
    println!(
        "registered {} and {} in {}",
        COMMAND_NAME,
        SHORT_COMMAND_NAME,
        directory.display()
    );
    println!("Open a new terminal if the command is not available in the current session.");
    Ok(())
}

fn unregister(mut installation: Installation, dry_run: bool) -> Result<()> {
    let Some(record) = installation.registration.clone() else {
        println!("no command registration exists");
        return Ok(());
    };
    if dry_run {
        println!("unregister command: {}", record.path.display());
        return Ok(());
    }
    let files = command_files(&installation, &record.path, record.direct)?;
    for (path, content) in &files {
        check_owned(path, content)?;
    }
    for (path, _) in &files {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("cannot remove command file"),
        }
    }
    #[cfg(windows)]
    let _ = fs::remove_dir(&record.directory);
    #[cfg(windows)]
    if record.path_added {
        registry_path(record.scope, &record.directory, true)?;
    }
    for profile in &record.profiles {
        edit_profile(profile, false)?;
    }
    installation.registration = None;
    save(&installation)?;
    println!("command registration removed; Agent binary and configuration retained");
    Ok(())
}

pub fn execute(installation: Installation, action: CommandAction) -> Result<()> {
    match action {
        CommandAction::Register {
            scope,
            directory,
            no_path,
            dry_run,
        } => {
            let scope = scope.unwrap_or(installation.scope);
            register(installation, scope, directory, no_path, dry_run)
        }
        CommandAction::Unregister { dry_run } => unregister(installation, dry_run),
        CommandAction::Status => {
            if let Some(record) = &installation.registration {
                println!(
                    "command: {}\npath: {}\nexists: {}",
                    COMMAND_NAME,
                    record.path.display(),
                    record.path.exists()
                );
                let short = record.directory.join(if cfg!(windows) {
                    "pa.cmd"
                } else {
                    SHORT_COMMAND_NAME
                });
                println!(
                    "short command: {}\npath: {}\nexists: {}",
                    SHORT_COMMAND_NAME,
                    short.display(),
                    short.exists()
                );
            } else {
                println!("command is not registered");
            }
            Ok(())
        }
    }
}
