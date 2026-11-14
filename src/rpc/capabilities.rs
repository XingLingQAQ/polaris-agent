//! Deployment discovery: the public `/api/capabilities` document says whether
//! the backend is the Worker build or the Go build, and that decides the
//! default reporting cadence. The Worker bills per row and expects batched
//! minute reports; the Go server stores every sample, so it expects one report
//! per sample to stay real time.
//!
//! 部署类型探测：公共 `/api/capabilities` 文档标明后端是 Worker 还是 Go 版本，据此决定默认上报周期。
//! Worker 按行计费、按分钟批量入库；Go 服务端逐样本入库，因此需要每个样本即时上报。

use std::time::Duration;

use anyhow::{Context as _, Result};
use serde::Deserialize;
use tracing::debug;
use url::Url;

use super::endpoint;
use crate::config::Config;

/// A capability probe that cannot answer in this budget is not worth delaying startup.
/// 能力探测不应因网络迟缓而推迟启动。
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ATTEMPTS: u32 = 2;
const MAX_BODY: usize = 64 * 1024;

/// Deployment kind reported by `GET /api/capabilities`.
/// `GET /api/capabilities` 返回的部署类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Runtime {
    Worker,
    Go,
}

impl Runtime {
    pub fn as_str(self) -> &'static str {
        match self {
            Runtime::Worker => "worker",
            Runtime::Go => "go",
        }
    }

    /// Default report interval for this deployment. Never below the sample
    /// interval, so a long sampling period cannot invert the cadence.
    /// 该部署的默认上报周期；始终不低于采样周期，避免长采样周期导致上报周期更短。
    pub fn default_report_interval(self, sample_interval_secs: u64) -> u64 {
        match self {
            Runtime::Worker => 60.max(sample_interval_secs),
            Runtime::Go => sample_interval_secs,
        }
    }
}

#[derive(Deserialize)]
struct CapabilitiesDocument {
    runtime: String,
}

fn parse_runtime(body: &[u8]) -> Result<Runtime> {
    let document: CapabilitiesDocument =
        serde_json::from_slice(body).context("capability document is not valid JSON")?;
    match document.runtime.as_str() {
        "worker" => Ok(Runtime::Worker),
        "go" => Ok(Runtime::Go),
        other => anyhow::bail!("unsupported backend runtime: {other}"),
    }
}

/// `https://host/<prefix>/api/capabilities` for the configured endpoint.
/// 由配置的端点推导 `https://host/<prefix>/api/capabilities`。
fn capabilities_url(cfg: &Config) -> Result<Url> {
    let mut url = endpoint::derive(cfg)?.http_push;
    let path = url.path().to_owned();
    let prefix = path
        .split("/api/public/agent/push/")
        .next()
        .unwrap_or_default();
    url.set_path(&format!("{prefix}/api/capabilities"));
    Ok(url)
}

/// Query the backend deployment type, retrying once.
/// 查询后端部署类型，最多重试一次。
pub async fn discover(cfg: &Config) -> Result<Runtime> {
    let url = capabilities_url(cfg)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .danger_accept_invalid_certs(cfg.insecure)
        .user_agent(concat!("polaris-agent/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("build capability client")?;
    let mut last_error = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match fetch(&client, &url).await {
            Ok(runtime) => return Ok(runtime),
            Err(error) => {
                debug!(attempt, err = %error, "capability discovery attempt failed");
                last_error = Some(error);
            }
        }
    }
    Err(last_error.expect("at least one discovery attempt"))
}

async fn fetch(client: &reqwest::Client, url: &Url) -> Result<Runtime> {
    let response = client
        .get(url.clone())
        .timeout(DISCOVERY_TIMEOUT)
        .send()
        .await
        .context("capability request failed")?;
    anyhow::ensure!(
        response.status().is_success(),
        "capability request returned {}",
        response.status()
    );
    let bytes = crate::probes::read_limited(response, MAX_BODY).await?;
    parse_runtime(&bytes)
}

/// Explicit configuration wins; otherwise the discovered runtime picks the
/// default, and a failed discovery keeps the configured value (60s by default).
/// 显式配置优先；否则按探测到的部署类型取默认值；探测失败保留配置值（默认 60 秒）。
pub fn effective_report_interval(
    sample_interval_secs: u64,
    report_interval_secs: u64,
    report_interval_explicit: bool,
    runtime: Option<Runtime>,
) -> u64 {
    if report_interval_explicit {
        return report_interval_secs;
    }
    runtime.map_or(report_interval_secs, |runtime| {
        runtime.default_report_interval(sample_interval_secs)
    })
}

/// Apply the discovered runtime to the cadence before the first sample.
/// 在首次采样前按探测到的部署类型应用上报周期。
pub fn apply_reporting_runtime(cfg: &mut Config, runtime: Option<Runtime>) {
    cfg.report_interval_secs = effective_report_interval(
        cfg.sample_interval_secs,
        cfg.report_interval_secs,
        cfg.report_interval_explicit,
        runtime,
    );
    debug_assert!(cfg.report_interval_secs >= cfg.sample_interval_secs);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.into(),
            token: "fixture".into(),
            sample_interval_secs: 5,
            report_interval_secs: 60,
            report_interval_explicit: false,
            insecure: false,
            include_nics: vec![],
            exclude_nics: vec![],
            include_mounts: vec![],
            exclude_mounts: vec![],
            auto_update: false,
            log_file: None,
            service_tier: None,
            config_path: std::path::PathBuf::from("unused"),
            runtime_overrides: Default::default(),
        }
    }

    #[test]
    fn worker_keeps_the_minute_cadence() {
        assert_eq!(Runtime::Worker.default_report_interval(5), 60);
        assert_eq!(
            effective_report_interval(5, 60, false, Some(Runtime::Worker)),
            60
        );
    }

    #[test]
    fn worker_default_never_undercuts_a_long_sample_interval() {
        assert_eq!(Runtime::Worker.default_report_interval(90), 90);
        assert_eq!(
            effective_report_interval(90, 60, false, Some(Runtime::Worker)),
            90
        );
        let mut cfg = config("https://monitor.test");
        cfg.sample_interval_secs = 90;
        apply_reporting_runtime(&mut cfg, Some(Runtime::Worker));
        assert_eq!(cfg.report_interval_secs, 90);
        assert!(cfg.report_interval_secs >= cfg.sample_interval_secs);
    }

    #[test]
    fn go_reports_every_sample() {
        assert_eq!(Runtime::Go.default_report_interval(5), 5);
        assert_eq!(
            effective_report_interval(5, 60, false, Some(Runtime::Go)),
            5
        );
        assert_eq!(
            effective_report_interval(10, 60, false, Some(Runtime::Go)),
            10
        );
    }

    #[test]
    fn explicit_report_interval_survives_discovery() {
        assert_eq!(
            effective_report_interval(5, 120, true, Some(Runtime::Go)),
            120
        );
        assert_eq!(
            effective_report_interval(5, 120, true, Some(Runtime::Worker)),
            120
        );
    }

    #[test]
    fn failed_discovery_keeps_the_default_report_interval() {
        assert_eq!(effective_report_interval(5, 60, false, None), 60);
    }

    #[test]
    fn capability_url_follows_the_endpoint_prefix() {
        assert_eq!(
            capabilities_url(&config("https://monitor.test"))
                .unwrap()
                .as_str(),
            "https://monitor.test/api/capabilities"
        );
        assert_eq!(
            capabilities_url(&config(
                "https://monitor.test/uptime/api/public/agent/token"
            ))
            .unwrap()
            .as_str(),
            "https://monitor.test/uptime/api/capabilities"
        );
    }

    #[test]
    fn capability_document_accepts_both_runtimes_and_ignores_extra_fields() {
        let worker = br#"{"runtime":"worker","apiVersion":1,"probeSource":"worker","monitoring":{"types":["http"]},"themes":{"storage":"r2","installationAvailable":true}}"#;
        let go = br#"{"runtime":"go","apiVersion":1,"probeSource":"server","themes":{"storage":"filesystem","installationAvailable":true}}"#;
        assert_eq!(parse_runtime(worker).unwrap(), Runtime::Worker);
        assert_eq!(parse_runtime(go).unwrap(), Runtime::Go);
        assert!(parse_runtime(br#"{"runtime":"rust"}"#).is_err());
        assert!(parse_runtime(b"<html>200</html>").is_err());
    }

    #[test]
    fn applying_a_runtime_updates_only_the_default() {
        let mut cfg = config("https://monitor.test");
        apply_reporting_runtime(&mut cfg, Some(Runtime::Go));
        assert_eq!(cfg.report_interval_secs, 5);
        let mut cfg = config("https://monitor.test");
        cfg.report_interval_explicit = true;
        apply_reporting_runtime(&mut cfg, Some(Runtime::Go));
        assert_eq!(cfg.report_interval_secs, 60);
    }
}
