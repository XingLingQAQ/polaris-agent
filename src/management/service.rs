use super::{Installation, Scope, ServiceAction, ServiceArgs};
use anyhow::{bail, Context, Result};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    process::Command,
};

// 根据安装时记录的管理器生成参数列表，避免经 shell 拼接执行。
// Build argument lists for the recorded manager without shell interpolation.
fn plan(installation: &Installation, action: &ServiceAction) -> Result<Vec<(String, Vec<String>)>> {
    let name = &installation.service_name;
    let operation = match action {
        ServiceAction::Start => "start",
        ServiceAction::Stop => "stop",
        ServiceAction::Restart => "restart",
        ServiceAction::Status => "status",
        ServiceAction::Logs { .. } => "logs",
    };
    let single = |program: &str, args: Vec<String>| Ok(vec![(program.into(), args)]);
    match installation.manager.as_str() {
        "systemd" => {
            let mut args = if installation.scope == Scope::User {
                vec!["--user".into()]
            } else {
                vec![]
            };
            if let ServiceAction::Logs { follow, lines } = action {
                args.extend([
                    "-u".into(),
                    name.clone(),
                    "-n".into(),
                    lines.to_string(),
                    "--no-pager".into(),
                ]);
                if *follow {
                    args.push("--follow".into());
                }
                single("journalctl", args)
            } else {
                args.extend([operation.into(), name.clone()]);
                single("systemctl", args)
            }
        }
        "launchd" => {
            let domain = if installation.scope == Scope::System {
                "system".to_string()
            } else {
                let user_id = installation
                    .user_id
                    .as_deref()
                    .context("launchd user ID is missing from installation information")?;
                anyhow::ensure!(
                    !user_id.is_empty() && user_id.bytes().all(|byte| byte.is_ascii_digit()),
                    "invalid launchd user ID"
                );
                format!("gui/{user_id}")
            };
            let target = format!("{domain}/{name}");
            match action {
                ServiceAction::Start => {
                    let plist = installation
                        .extra_path
                        .as_ref()
                        .context("launchd plist path is missing")?
                        .to_string_lossy()
                        .into_owned();
                    single("launchctl", vec!["bootstrap".into(), domain, plist])
                }
                ServiceAction::Stop => single("launchctl", vec!["bootout".into(), target]),
                ServiceAction::Restart => {
                    single("launchctl", vec!["kickstart".into(), "-k".into(), target])
                }
                ServiceAction::Status => single("launchctl", vec!["print".into(), target]),
                ServiceAction::Logs { .. } => Ok(vec![]),
            }
        }
        "windows-task" => {
            let task = |verb: &str| {
                (
                    "schtasks.exe".into(),
                    vec![verb.into(), "/TN".into(), name.clone()],
                )
            };
            match action {
                ServiceAction::Start => Ok(vec![task("/Run")]),
                ServiceAction::Stop => Ok(vec![task("/End")]),
                ServiceAction::Restart => Ok(vec![task("/End"), task("/Run")]),
                ServiceAction::Status => single(
                    "schtasks.exe",
                    vec![
                        "/Query".into(),
                        "/TN".into(),
                        name.clone(),
                        "/V".into(),
                        "/FO".into(),
                        "LIST".into(),
                    ],
                ),
                ServiceAction::Logs { .. } => Ok(vec![]),
            }
        }
        "openrc" => {
            if matches!(action, ServiceAction::Logs { .. }) {
                Ok(vec![])
            } else {
                single("rc-service", vec![name.clone(), operation.into()])
            }
        }
        "supervisord" => {
            if let ServiceAction::Logs { follow, .. } = action {
                let mut args = vec!["tail".into()];
                if *follow {
                    args.push("-f".into());
                }
                args.extend([name.clone(), "stdout".into()]);
                single("supervisorctl", args)
            } else {
                single("supervisorctl", vec![operation.into(), name.clone()])
            }
        }
        "s6" => {
            let directory = format!("/etc/services.d/{name}");
            match action {
                ServiceAction::Status => single("s6-svstat", vec![directory]),
                ServiceAction::Logs { .. } => Ok(vec![]),
                _ => single(
                    "s6-svc",
                    vec![
                        match action {
                            ServiceAction::Start => "-u",
                            ServiceAction::Stop => "-d",
                            _ => "-r",
                        }
                        .into(),
                        directory,
                    ],
                ),
            }
        }
        "docker" => {
            let compose = installation
                .extra_path
                .as_ref()
                .context("Docker Compose path is missing")?;
            let mut args = vec![
                "compose".into(),
                "-f".into(),
                compose.to_string_lossy().into_owned(),
            ];
            if let ServiceAction::Logs { follow, lines } = action {
                args.extend(["logs".into(), "--tail".into(), lines.to_string()]);
                if *follow {
                    args.push("--follow".into());
                }
            } else {
                args.push(
                    if operation == "status" {
                        "ps"
                    } else {
                        operation
                    }
                    .into(),
                );
            }
            single("docker", args)
        }
        _ if matches!(action, ServiceAction::Logs { .. }) => Ok(vec![]),
        _ => bail!(
            "this installation has no supported service manager; use polaris-agent run --config {}",
            installation.config.display()
        ),
    }
}

/// 执行服务操作；无管理器日志命令时读取安装记录中的日志文件。
/// Run service actions; read the recorded log file when the manager has no log command.
pub fn execute(installation: &Installation, args: ServiceArgs) -> Result<()> {
    let commands = plan(installation, &args.action)?;
    if commands.is_empty() {
        let ServiceAction::Logs { follow, lines } = args.action else {
            return Ok(());
        };
        let path = installation
            .log_path
            .as_ref()
            .context("no log file was registered for this installation")?;
        if args.dry_run {
            println!(
                "logs: {} (last {lines} lines, follow={follow})",
                path.display()
            );
            return Ok(());
        }
        return print_log(path, lines, follow);
    }
    for (index, (program, values)) in commands.iter().enumerate() {
        if args.dry_run {
            println!("{program} {}", values.join(" "));
            continue;
        }
        let status = Command::new(program)
            .args(values)
            .status()
            .with_context(|| format!("cannot start {program}"))?;
        if !status.success() && !(index == 0 && commands.len() == 2) {
            bail!(
                "{program} failed with {status}; check service permissions and installation state"
            );
        }
    }
    Ok(())
}

fn print_log(path: &std::path::Path, lines: u32, follow: bool) -> Result<()> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("cannot open log: {}", path.display()))?;
    let length = file.metadata()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(1024 * 1024)))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let content = String::from_utf8_lossy(&bytes);
    let entries: Vec<_> = content.lines().collect();
    for line in entries
        .iter()
        .skip(entries.len().saturating_sub(lines as usize))
    {
        println!("{line}");
    }
    while follow && !crate::shutdown::requested() {
        std::thread::sleep(std::time::Duration::from_millis(400));
        if file.metadata()?.len() < file.stream_position()? {
            file.seek(SeekFrom::Start(0))?;
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if !bytes.is_empty() {
            std::io::stdout().write_all(&bytes)?;
            std::io::stdout().flush()?;
        }
    }
    Ok(())
}
