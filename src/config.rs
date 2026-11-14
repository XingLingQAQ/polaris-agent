//! Configuration loading: CLI > environment variables > config.json
//!
//! All three sources are merged; higher priority wins on conflict.
//! 合并命令行、环境变量和配置文件；冲突时优先采用优先级更高的来源。

use anyhow::{Context as _, Result};
use clap::Parser;
use serde::Deserialize;
use std::{env, path::PathBuf};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "polaris-agent",
    version,
    subcommand_required = true,
    arg_required_else_help = true,
    about = "Uptime Polaris host monitoring agent",
    after_help = "Short command: pa status | pa restart | pa logs -f"
)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Option<crate::cli::AgentCommand>,

    #[arg(
        long,
        global = true,
        help = "Show service commands without executing them"
    )]
    pub dry_run: bool,

    #[arg(long, global = true)]
    pub log_file: Option<PathBuf>,

    /// Server origin or Agent endpoint.
    /// 服务器源地址或 Agent 端点。
    #[arg(long, env = "POLARIS_AGENT_ENDPOINT", global = true)]
    pub endpoint: Option<String>,

    /// Per-machine authentication token.
    /// 每台机器独立的身份验证令牌。
    #[arg(long, env = "POLARIS_AGENT_TOKEN", global = true)]
    pub token: Option<String>,

    #[arg(long, global = true)]
    pub sample_interval: Option<u64>,

    #[arg(long, global = true)]
    pub report_interval: Option<u64>,

    #[arg(long, global = true)]
    pub include_nics: Option<String>,
    #[arg(long, global = true)]
    pub exclude_nics: Option<String>,
    #[arg(long, global = true)]
    pub include_mounts: Option<String>,
    #[arg(long, global = true)]
    pub exclude_mounts: Option<String>,

    /// Accept invalid TLS certificates
    /// 接受无效的 TLS 证书。
    #[arg(long, env = "POLARIS_AGENT_INSECURE", num_args = 0..=1, default_missing_value = "true", require_equals = true, global = true)]
    pub insecure: Option<bool>,

    /// Optional path to config.json (default: ./config.json when exists)
    /// 可选的 config.json 路径（默认使用存在的 ./config.json）。
    #[arg(long, env = "POLARIS_AGENT_CONFIG", global = true)]
    pub config: Option<PathBuf>,

    /// Check for Agent releases after a healthy report
    /// 成功上报后检查 Agent 新版本。
    #[arg(long, env = "POLARIS_AGENT_AUTO_UPDATE", num_args = 0..=1, default_missing_value = "true", require_equals = true, global = true)]
    pub auto_update: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    endpoint: Option<String>,
    token: Option<String>,
    sample_interval: Option<u64>,
    report_interval: Option<u64>,
    include_nics: Option<Vec<String>>,
    exclude_nics: Option<Vec<String>>,
    include_mounts: Option<Vec<String>>,
    exclude_mounts: Option<Vec<String>>,
    insecure: Option<bool>,
    auto_update: Option<bool>,
    log_file: Option<PathBuf>,
    #[serde(rename = "runtime_revision")]
    _runtime_revision: Option<String>,
    service_tier: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeOverrides {
    pub sample_interval: bool,
    pub report_interval: bool,
    pub include_nics: bool,
    pub exclude_nics: bool,
    pub include_mounts: bool,
    pub exclude_mounts: bool,
    pub auto_update: bool,
}

#[derive(Debug, Clone)]
/// 合并配置来源后供采样、上报和服务管理使用的参数。
/// Merged configuration for sampling, reporting, and service management.
pub struct Config {
    pub endpoint: String,
    pub token: String,
    pub sample_interval_secs: u64,
    pub report_interval_secs: u64,
    /// Whether report_interval came from CLI, environment, or config.json.
    /// report_interval 是否由命令行、环境变量或 config.json 显式指定。
    pub report_interval_explicit: bool,
    pub insecure: bool,
    pub include_nics: Vec<String>,
    pub exclude_nics: Vec<String>,
    pub include_mounts: Vec<String>,
    pub exclude_mounts: Vec<String>,
    pub auto_update: bool,
    pub log_file: Option<PathBuf>,
    pub service_tier: Option<String>,
    pub config_path: PathBuf,
    pub runtime_overrides: RuntimeOverrides,
}

fn resolve_auto_update(cli_or_env: Option<bool>, file: Option<bool>) -> bool {
    cli_or_env.or(file).unwrap_or(false)
}

impl Config {
    pub(crate) fn validate_document(value: &serde_json::Value) -> Result<()> {
        let _: FileConfig = serde_json::from_value(value.clone())
            .context("configuration contains unsupported fields or values")?;
        Ok(())
    }

    pub fn from_args(args: CliArgs) -> Result<Self> {
        let config_path = Self::resolve_config_path(args.config.as_deref());
        let file_cfg = Self::load_file(if args.config.is_some() || config_path.is_file() {
            Some(&config_path)
        } else {
            None
        })?;

        let runtime_overrides = RuntimeOverrides {
            sample_interval: args.sample_interval.is_some() || env::var("POLARIS_AGENT_SAMPLE_INTERVAL").ok().and_then(|v| v.parse::<u64>().ok()).is_some(),
            report_interval: args.report_interval.is_some() || env::var("POLARIS_AGENT_REPORT_INTERVAL").ok().and_then(|v| v.parse::<u64>().ok()).is_some(),
            include_nics: args.include_nics.is_some() || env::var("POLARIS_AGENT_INCLUDE_NICS").is_ok(),
            exclude_nics: args.exclude_nics.is_some() || env::var("POLARIS_AGENT_EXCLUDE_NICS").is_ok(),
            include_mounts: args.include_mounts.is_some() || env::var("POLARIS_AGENT_INCLUDE_MOUNTS").is_ok(),
            exclude_mounts: args.exclude_mounts.is_some() || env::var("POLARIS_AGENT_EXCLUDE_MOUNTS").is_ok(),
            auto_update: args.auto_update.is_some(),
        };

        // CLI(env via clap)优先于 file
        // CLI (including environment via clap) takes precedence over the file.
        let endpoint = args
            .endpoint
            .or(file_cfg.endpoint)
            .context("endpoint is required (--endpoint / POLARIS_AGENT_ENDPOINT / config.json)")?;
        let token = args
            .token
            .or(file_cfg.token)
            .filter(|value| !value.is_empty())
            .context("token required (--token / POLARIS_AGENT_TOKEN / config.json)")?;
        let env_sample = env::var("POLARIS_AGENT_SAMPLE_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok());
        let env_report = env::var("POLARIS_AGENT_REPORT_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok());
        // 显式上报周期不随后端类型调整；仅默认值由探测结果决定。
        // An explicit report interval is never reshaped by the backend type; only the default follows discovery.
        let report_interval_explicit =
            args.report_interval.is_some() || env_report.is_some() || file_cfg.report_interval.is_some();
        let (sample_interval_secs, report_interval_secs) = resolve_intervals(
            args.sample_interval,
            env_sample,
            file_cfg.sample_interval,
            args.report_interval,
            env_report,
            file_cfg.report_interval,
        );
        let insecure = merge_insecure(args.insecure, file_cfg.insecure);
        let auto_update = resolve_auto_update(
            args.auto_update,
            file_cfg.auto_update,
        );

        let list = |cli: Option<String>, name: &str, file: Option<Vec<String>>| {
            cli.or_else(|| env::var(name).ok())
                .map(|s| split_filter(&s))
                .or(file)
                .unwrap_or_default()
        };
        let include_nics = list(
            args.include_nics,
            "POLARIS_AGENT_INCLUDE_NICS",
            file_cfg.include_nics,
        );
        let exclude_nics = list(
            args.exclude_nics,
            "POLARIS_AGENT_EXCLUDE_NICS",
            file_cfg.exclude_nics,
        );
        let include_mounts = list(
            args.include_mounts,
            "POLARIS_AGENT_INCLUDE_MOUNTS",
            file_cfg.include_mounts,
        );
        let exclude_mounts = list(
            args.exclude_mounts,
            "POLARIS_AGENT_EXCLUDE_MOUNTS",
            file_cfg.exclude_mounts,
        );

        validate_intervals(sample_interval_secs, report_interval_secs)?;

        Ok(Self {
            endpoint,
            token,
            sample_interval_secs,
            report_interval_secs,
            report_interval_explicit,
            insecure,
            include_nics,
            exclude_nics,
            include_mounts,
            exclude_mounts,
            auto_update,
            log_file: args.log_file.or(file_cfg.log_file),
            service_tier: file_cfg.service_tier,
            config_path,
            runtime_overrides,
        })
    }

    pub fn resolve_config_path(path: Option<&std::path::Path>) -> PathBuf {
        if let Some(path) = path {
            return path.to_path_buf();
        }
        let local = PathBuf::from("config.json");
        if local.is_file() {
            return local;
        }
        let mut candidates = Vec::new();
        if let Ok(executable) = env::current_exe() {
            if let Some(directory) = executable.parent() {
                candidates.push(directory.join("config.json"));
                if let Some(parent) = directory.parent() {
                    candidates.push(parent.join("config/config.json"));
                }
            }
        }
        for name in ["polaris-agent"] {
            #[cfg(windows)]
            for variable in ["PROGRAMDATA", "LOCALAPPDATA"] {
                if let Some(directory) = env::var_os(variable) {
                    candidates.push(PathBuf::from(directory).join(name).join("config.json"));
                }
            }
            #[cfg(not(windows))]
            {
                if let Some(directory) = env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
                {
                    candidates.push(directory.join(name).join("config.json"));
                }
                candidates.push(PathBuf::from("/etc").join(name).join("config.json"));
            }
        }
        candidates
            .into_iter()
            .find(|candidate| candidate.is_file())
            .unwrap_or(local)
    }

    fn load_file(path: Option<&std::path::Path>) -> Result<FileConfig> {
        let path = match path {
            Some(p) => Some(p.to_path_buf()),
            None => {
                let default = PathBuf::from("config.json");
                if default.exists() {
                    Some(default)
                } else {
                    None
                }
            }
        };

        match path {
            Some(p) => {
                let raw = std::fs::read_to_string(&p)
                    .with_context(|| format!("failed to read config: {}", p.display()))?;
                let cfg: FileConfig = serde_json::from_str(&raw)
                    .with_context(|| format!("failed to parse config: {}", p.display()))?;
                Ok(cfg)
            }
            None => Ok(FileConfig::default()),
        }
    }
}

fn split_filter(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .collect()
}

fn resolve_intervals(
    cli_sample: Option<u64>,
    env_sample: Option<u64>,
    file_sample: Option<u64>,
    cli_report: Option<u64>,
    env_report: Option<u64>,
    file_report: Option<u64>,
) -> (u64, u64) {
    let sample = cli_sample.or(env_sample).or(file_sample).unwrap_or(5);
    // 未显式指定上报周期时，默认值不低于采样周期，避免长采样周期无法启动。
    // Without an explicit report interval, the default never falls below the sample interval.
    let report = cli_report.or(env_report).or(file_report).unwrap_or_else(|| 60.max(sample));
    (sample, report)
}

fn merge_insecure(cli: Option<bool>, file: Option<bool>) -> bool {
    cli.or(file).unwrap_or(false)
}

fn validate_intervals(sample: u64, report: u64) -> Result<()> {
    if sample == 0 || report == 0 {
        anyhow::bail!("sample and report intervals must be >= 1 second");
    }
    if sample > report {
        anyhow::bail!("sample interval must not exceed report interval");
    }
    if report > 1800 {
        anyhow::bail!("report interval must be <= 1800 seconds");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_overrides_follow_explicit_cli_values() {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).unwrap();
        let directory = std::env::temp_dir().join(format!("polaris-config-{}", hex::encode(random)));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("config.json");
        std::fs::write(&path, br#"{"endpoint":"https://example.test","token":"fixture","sample_interval":9,"auto_update":true}"#).unwrap();
        let args = CliArgs::try_parse_from([
            "polaris-agent", "run", "--config", path.to_str().unwrap(),
            "--sample-interval", "5", "--include-nics", "eth0", "--auto-update=false",
        ]).unwrap();
        let cfg = Config::from_args(args).unwrap();
        assert_eq!(cfg.sample_interval_secs, 5);
        assert!(!cfg.auto_update);
        assert!(cfg.runtime_overrides.sample_interval);
        assert!(cfg.runtime_overrides.include_nics);
        assert!(cfg.runtime_overrides.auto_update);
        assert!(!cfg.report_interval_explicit);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn file_report_interval_counts_as_explicit() {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).unwrap();
        let directory = std::env::temp_dir().join(format!("polaris-config-{}", hex::encode(random)));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("config.json");
        std::fs::write(&path, br#"{"endpoint":"https://example.test","token":"fixture","report_interval":90}"#).unwrap();
        let args = CliArgs::try_parse_from([
            "polaris-agent", "run", "--config", path.to_str().unwrap(),
        ]).unwrap();
        let cfg = Config::from_args(args).unwrap();
        assert_eq!(cfg.report_interval_secs, 90);
        assert!(cfg.report_interval_explicit);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn parse_file_config_minimal() {
        let json = r#"{"endpoint":"wss://x/","token":"abc"}"#;
        let cfg: FileConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.endpoint.unwrap(), "wss://x/");
        assert_eq!(cfg.token.unwrap(), "abc");
        assert!(cfg.sample_interval.is_none());
        assert!(cfg.insecure.is_none());
    }

    #[test]
    fn retired_update_source_is_rejected() {
        assert!(serde_json::from_str::<FileConfig>(r#"{"update_source":"https://example.test"}"#).is_err());
    }

    #[test]
    fn parse_file_config_full() {
        let json = r#"{
            "endpoint": "wss://x/",
            "token": "abc",
            "sample_interval": 10,
            "insecure": true
        }"#;
        let cfg: FileConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.sample_interval, Some(10));
        assert_eq!(cfg.insecure, Some(true));
    }

    #[test]
    fn auto_update_is_opt_in_with_cli_and_environment_precedence() {
        assert!(!resolve_auto_update(None, None));
        assert!(resolve_auto_update(None, Some(true)));
        assert!(!resolve_auto_update(Some(false), Some(true)));
        assert!(resolve_auto_update(Some(true), Some(false)));
    }

    #[test]
    fn explicit_defaults_override_file_values() {
        assert!(!merge_insecure(Some(false), Some(true)));
    }

    #[test]
    fn cadence_sources_are_cli_then_environment_then_file() {
        assert_eq!(
            resolve_intervals(Some(7), Some(6), Some(3), Some(90), Some(80), Some(60)),
            (7, 90)
        );
        assert_eq!(
            resolve_intervals(None, Some(8), Some(4), None, None, Some(30)),
            (8, 30)
        );
        assert_eq!(
            resolve_intervals(None, None, None, None, None, None),
            (5, 60)
        );
        assert_eq!(
            resolve_intervals(Some(90), None, None, None, None, None),
            (90, 90)
        );
    }

    #[test]
    fn long_sample_interval_builds_with_a_matching_default_report_interval() {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).unwrap();
        let directory = std::env::temp_dir().join(format!("polaris-config-{}", hex::encode(random)));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("config.json");
        std::fs::write(&path, br#"{"endpoint":"https://example.test","token":"fixture","sample_interval":90}"#).unwrap();
        let args = CliArgs::try_parse_from([
            "polaris-agent", "run", "--config", path.to_str().unwrap(),
        ]).unwrap();
        let cfg = Config::from_args(args).unwrap();
        assert_eq!(cfg.sample_interval_secs, 90);
        assert_eq!(cfg.report_interval_secs, 90);
        assert!(!cfg.report_interval_explicit);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn cadence_bounds_reject_unsupported_configurations() {
        assert!(validate_intervals(5, 180).is_ok());
        assert!(validate_intervals(10, 5).is_err());
        assert!(validate_intervals(5, 1801).is_err());
    }
}
