use anyhow::{ensure, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

mod registration;
mod service;

pub const COMMAND_NAME: &str = "polaris-agent";
pub const SHORT_COMMAND_NAME: &str = "pa";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    System,
    #[default]
    User,
}

#[derive(Debug, Clone)]
pub struct ServiceArgs {
    pub action: ServiceAction,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Subcommand)]
pub enum ServiceAction {
    /// Start the installed service.
    /// 启动已安装的服务。
    Start,
    /// Stop the installed service.
    /// 停止已安装的服务。
    Stop,
    /// Restart the installed service.
    /// 重启已安装的服务。
    Restart,
    /// Show the installed service's status.
    /// 显示已安装服务的状态。
    Status,
    /// Read the installed service's logs.
    /// 查看已安装服务的日志。
    Logs {
        #[arg(short, long)]
        follow: bool,
        #[arg(short = 'n', long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=10000))]
        lines: u32,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum CommandAction {
    Register {
        #[arg(long, value_enum)]
        scope: Option<Scope>,
        #[arg(long)]
        directory: Option<PathBuf>,
        #[arg(
            long,
            help = "Create the wrapper without changing PATH or shell profiles"
        )]
        no_path: bool,
        #[arg(long)]
        dry_run: bool,
    },
    Unregister {
        #[arg(long)]
        dry_run: bool,
    },
    Status,
}

#[derive(Debug, Clone, Args)]
pub struct RecordArgs {
    #[arg(long)]
    pub manager: String,
    #[arg(long)]
    pub service_name: String,
    #[arg(long, value_enum)]
    pub scope: Scope,
    #[arg(long)]
    pub mode: String,
    #[arg(long)]
    pub install_dir: Option<PathBuf>,
    #[arg(long)]
    pub log_path: Option<PathBuf>,
    #[arg(long)]
    pub extra_path: Option<PathBuf>,
    #[arg(long)]
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileChange {
    pub path: PathBuf,
    pub block: String,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub path: PathBuf,
    pub directory: PathBuf,
    pub scope: Scope,
    pub path_added: bool,
    pub direct: bool,
    pub profiles: Vec<ProfileChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// 安装记录将服务管理与命令注册绑定到具体配置文件。
/// An installation record binds service management and command registration to a config file.
pub struct Installation {
    pub schema: u32,
    pub executable: PathBuf,
    pub config: PathBuf,
    pub manager: String,
    pub service_name: String,
    pub scope: Scope,
    pub mode: String,
    pub install_dir: Option<PathBuf>,
    pub log_path: Option<PathBuf>,
    pub extra_path: Option<PathBuf>,
    pub user_id: Option<String>,
    pub registration: Option<Registration>,
}

fn metadata_path(config: &Path) -> PathBuf {
    config
        .parent()
        .unwrap_or(Path::new("."))
        .join("install.json")
}

fn absolute(path: &Path) -> Result<PathBuf> {
    let resolved = path.canonicalize()?;
    #[cfg(windows)]
    {
        let text = resolved.to_string_lossy();
        if let Some(rest) = text.strip_prefix("\\\\?\\UNC\\") {
            return Ok(PathBuf::from(format!("\\\\{rest}")));
        }
        if let Some(rest) = text.strip_prefix("\\\\?\\") {
            return Ok(PathBuf::from(rest));
        }
    }
    Ok(resolved)
}

/// 加载并校验与指定配置文件对应的安装记录。
/// Load and validate the installation record for the specified configuration.
pub fn load(config: &Path) -> Result<Installation> {
    let config = absolute(config)
        .with_context(|| format!("cannot locate configuration: {}", config.display()))?;
    let raw = match std::fs::read(metadata_path(&config)) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(error).context(
                "installation information is missing; reinstall with the current installer",
            )
        }
        Err(error) => return Err(error).context(
            "cannot read installation information; use the installation owner or an administrator",
        ),
    };
    let installation: Installation =
        serde_json::from_slice(&raw).context("invalid installation information")?;
    ensure!(
        installation.schema == 1 && installation.config == config,
        "installation information belongs to another configuration"
    );
    ensure!(
        !installation.service_name.is_empty()
            && installation
                .service_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.@-".contains(&byte)),
        "invalid registered service name"
    );
    Ok(installation)
}

fn save(installation: &Installation) -> Result<()> {
    crate::storage::write_private(
        &metadata_path(&installation.config),
        &serde_json::to_vec_pretty(installation)?,
    )
}

/// 记录安装信息，保留已有的命令注册状态。
/// Record installation details while preserving existing command registration.
pub fn record(config: &Path, args: RecordArgs) -> Result<()> {
    let config =
        absolute(config).context("configuration must exist before installation is registered")?;
    ensure!(
        !args.service_name.is_empty()
            && args
                .service_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.@-".contains(&byte)),
        "invalid service name"
    );
    let registration = if metadata_path(&config).exists() {
        load(&config)?.registration
    } else {
        None
    };
    save(&Installation {
        schema: 1,
        executable: absolute(&std::env::current_exe()?)?,
        config,
        manager: args.manager,
        service_name: args.service_name,
        scope: args.scope,
        mode: args.mode,
        install_dir: args.install_dir,
        log_path: args.log_path,
        extra_path: args.extra_path,
        user_id: args.user_id,
        registration,
    })
}

pub fn manage_service(config: &Path, args: ServiceArgs) -> Result<()> {
    service::execute(&load(config)?, args)
}

pub fn manage_command(config: &Path, action: CommandAction) -> Result<()> {
    registration::execute(load(config)?, action)
}
