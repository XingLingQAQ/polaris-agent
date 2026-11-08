use anyhow::{Context, Result};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

/// 以私有权限写入临时文件后原子替换配置。
/// Write a private temporary file and atomically replace the configuration.
pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    write_private_using(path, bytes, replace_file)
}

fn write_private_using(
    path: &Path,
    bytes: &[u8],
    replace: impl FnOnce(&Path, &Path) -> Result<()>,
) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temp = create_private_temp(path, bytes)?;
    if let Err(error) = replace(&temp, path) {
        let _ = fs::remove_file(temp);
        return Err(error);
    }
    Ok(())
}

fn create_private_temp(path: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut random = [0u8; 16];
    getrandom::getrandom(&mut random)
        .context("operating system could not provide secure randomness")?;
    let temp = parent.join(format!(".uptime-polaris-{}.tmp", hex::encode(random)));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .context("cannot create private temporary file")?;
    #[cfg(windows)]
    {
        if let Err(error) = secure_windows_temp(&temp) {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
    }
    if let Err(error) = file.write_all(bytes).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temp);
        return Err(error).context("cannot write protected temporary file");
    }
    Ok(temp)
}

fn replace_file(temp: &Path, target: &Path) -> Result<()> {
    fs::rename(temp, target).context("cannot atomically replace protected config file")
}

#[cfg(windows)]
fn secure_windows_temp(path: &Path) -> Result<()> {
    use std::process::Command;
    let output = Command::new("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .context("cannot determine the current Windows SID")?;
    let text = String::from_utf8_lossy(&output.stdout);
    let sid = text
        .split('"')
        .find(|field| field.starts_with("S-1-"))
        .context("cannot parse the current Windows SID")?;
    let user = format!("*{sid}:F");
    let output = Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(user)
        .args(["*S-1-5-18:F", "*S-1-5-32-544:F"])
        .output()
        .context("cannot set private Windows ACL")?;
    if !output.status.success() {
        anyhow::bail!("cannot set private Windows ACL")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protected_writes_are_atomic_and_private() {
        let root = std::env::temp_dir().canonicalize().unwrap();
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).unwrap();
        let directory = root.join(format!("polaris-storage-{}", hex::encode(random)));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("config.json");
        write_private(&path, b"first").unwrap();
        let result = write_private_using(&path, b"second", |_, _| {
            anyhow::bail!("fixture replace failure")
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"first");
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        write_private(&path, b"second").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        #[cfg(windows)]
        {
            let acl = std::process::Command::new("icacls")
                .arg(&path)
                .output()
                .unwrap();
            assert!(acl.status.success());
            assert!(!String::from_utf8_lossy(&acl.stdout).contains("(I)"));
        }
        assert!(directory.starts_with(root));
        fs::remove_dir_all(directory).unwrap();
    }
}
