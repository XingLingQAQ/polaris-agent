use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, atomic::{AtomicU8, Ordering}};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::{commands::Signal, config::Config, rpc::endpoint};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// 控制台可远程调整的运行参数，不包含连接凭据。
/// Runtime settings remotely adjustable from the console, excluding connection credentials.
pub struct RuntimeConfig {
    pub sample_interval_secs: u64,
    pub report_interval_secs: u64,
    pub include_nics: Vec<String>,
    pub exclude_nics: Vec<String>,
    pub include_mounts: Vec<String>,
    pub exclude_mounts: Vec<String>,
    pub auto_update: bool,
}

impl RuntimeConfig {
    pub fn current(cfg: &Config) -> Self {
        Self {
            sample_interval_secs: cfg.sample_interval_secs,
            report_interval_secs: cfg.report_interval_secs,
            include_nics: cfg.include_nics.clone(),
            exclude_nics: cfg.exclude_nics.clone(),
            include_mounts: cfg.include_mounts.clone(),
            exclude_mounts: cfg.exclude_mounts.clone(),
            auto_update: cfg.auto_update && auto_update_supported(cfg),
        }
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.sample_interval_secs >= 1
                && self.sample_interval_secs <= self.report_interval_secs
                && self.report_interval_secs <= 1800,
            "invalid sample/report interval"
        );
        for values in [
            &self.include_nics,
            &self.exclude_nics,
            &self.include_mounts,
            &self.exclude_mounts,
        ] {
            anyhow::ensure!(
                values.len() <= 32
                    && values.iter().all(|value| !value.is_empty()
                        && value.chars().count() <= 256
                        && !value.chars().any(char::is_control)),
                "invalid interface/mount filter"
            );
        }
        Ok(())
    }

    fn prepare(&self, cfg: &Config, revision: &str) -> Result<(Config, Vec<u8>)> {
        self.validate()?;
        anyhow::ensure!(valid_revision(revision), "invalid configuration revision");
        let current = Self::current(cfg);
        // 显式 CLI 或环境变量配置优先，远程配置只能保持这些字段的当前值。
        // Explicit CLI or environment settings take priority; remote updates must preserve their values.
        let locks = &cfg.runtime_overrides;
        let conflicts = [
            (locks.sample_interval && self.sample_interval_secs != current.sample_interval_secs, "sample_interval"),
            (locks.report_interval && self.report_interval_secs != current.report_interval_secs, "report_interval"),
            (locks.include_nics && !same_members(&self.include_nics, &current.include_nics), "include_nics"),
            (locks.exclude_nics && !same_members(&self.exclude_nics, &current.exclude_nics), "exclude_nics"),
            (locks.include_mounts && !same_members(&self.include_mounts, &current.include_mounts), "include_mounts"),
            (locks.exclude_mounts && !same_members(&self.exclude_mounts, &current.exclude_mounts), "exclude_mounts"),
            (locks.auto_update && self.auto_update != cfg.auto_update, "auto_update"),
        ];
        if let Some((_, name)) = conflicts.iter().find(|(conflict, _)| *conflict) {
            anyhow::bail!("{name} is controlled by CLI or environment");
        }
        anyhow::ensure!(!self.auto_update || auto_update_supported(cfg), "this installation cannot restart after an automatic update");
        let mut document: Value = match std::fs::read(&cfg.config_path) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).context("cannot parse existing configuration")?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({}),
            Err(error) => return Err(error).context("cannot read existing configuration"),
        };
        Config::validate_document(&document)?;
        let object = document
            .as_object_mut()
            .context("existing configuration is not an object")?;
        let mut updates = serde_json::to_value(self)?.as_object().unwrap().clone();
        updates.remove("sample_interval_secs");
        updates.remove("report_interval_secs");
        updates.insert("sample_interval".into(), json!(self.sample_interval_secs));
        updates.insert("report_interval".into(), json!(self.report_interval_secs));
        updates.insert("runtime_revision".into(), json!(revision));
        object.extend(updates);
        let document = serde_json::to_vec_pretty(&document)?;
        let mut next = cfg.clone();
        next.sample_interval_secs = self.sample_interval_secs;
        next.report_interval_secs = self.report_interval_secs;
        if !locks.include_nics { next.include_nics = self.include_nics.clone(); }
        if !locks.exclude_nics { next.exclude_nics = self.exclude_nics.clone(); }
        if !locks.include_mounts { next.include_mounts = self.include_mounts.clone(); }
        if !locks.exclude_mounts { next.exclude_mounts = self.exclude_mounts.clone(); }
        next.auto_update = self.auto_update;
        Ok((next, document))
    }
}

fn same_members(left: &[String], right: &[String]) -> bool {
    use std::collections::BTreeSet;
    left.iter().collect::<BTreeSet<_>>() == right.iter().collect::<BTreeSet<_>>()
}

pub fn auto_update_supported(cfg: &Config) -> bool {
    if !crate::commands::is_supervised(cfg.service_tier.as_deref()) || crate::updater::is_container() { return false; }
    #[cfg(windows)]
    return crate::management::load(&cfg.config_path).is_ok_and(|installation| installation.manager == "windows-task");
    #[cfg(not(windows))]
    true
}

fn valid_revision(revision: &str) -> bool {
    revision.len() == 32
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

const PENDING: u8 = 0;
const CLAIMED: u8 = 1;
const CANCELLED: u8 = 2;

type Completion = oneshot::Receiver<Result<(), String>>;

#[derive(Default)]
struct ClaimState(AtomicU8);

impl ClaimState {
    fn claim(&self) -> bool {
        self.0.compare_exchange(PENDING, CLAIMED, Ordering::SeqCst, Ordering::SeqCst).is_ok()
    }

    fn cancel(&self) -> bool {
        self.0.compare_exchange(PENDING, CANCELLED, Ordering::SeqCst, Ordering::SeqCst).is_ok()
    }
}

struct CancelOnDrop(Arc<ClaimState>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

// 超时只取消未认领的配置；采样器已认领时继续等待，避免误报写入结果。
// A timeout cancels only unclaimed updates; wait for a claimed update to avoid misreporting its commit.
async fn wait_for_claim(state: &ClaimState, mut receipt: oneshot::Receiver<Completion>, timeout: Duration) -> Result<Completion> {
    match tokio::time::timeout(timeout, &mut receipt).await {
        Ok(result) => result.context("sampler configuration was interrupted"),
        Err(_) if state.cancel() => anyhow::bail!("sampler configuration timed out"),
        Err(_) => receipt.await.context("sampler configuration was interrupted"),
    }
}

pub struct Reconfiguration {
    next: Box<Config>,
    document: Vec<u8>,
    claim: oneshot::Sender<Completion>,
    state: Arc<ClaimState>,
}

impl Reconfiguration {
    /// 仅认领未取消的配置；持久化成功后才交给采样器应用。
    /// Claim only uncancelled updates; hand them to the sampler only after persistence succeeds.
    pub fn claim_and_commit(self) -> Option<(Box<Config>, oneshot::Sender<Result<(), String>>)> {
        if !self.state.claim() {
            return None;
        }
        let (applied, completion) = oneshot::channel();
        if self.claim.send(completion).is_err() {
            return None;
        }
        if let Err(error) = crate::storage::write_private(&self.next.config_path, &self.document) {
            let _ = applied.send(Err(format!("{error:#}")));
            return None;
        }
        Some((self.next, applied))
    }
}

pub struct SyncState {
    revision: Option<String>,
    error: Option<String>,
}

impl SyncState {
    pub fn new(cfg: &Config) -> Self {
        let revision = std::fs::read(&cfg.config_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|value| {
                value
                    .get("runtime_revision")
                    .and_then(Value::as_str)
                    .filter(|value| valid_revision(value))
                    .map(str::to_owned)
            });
        Self {
            revision,
            error: None,
        }
    }

    async fn exchange(&self, client: &reqwest::Client, cfg: &Config) -> Result<Option<Desired>> {
        let mut url = endpoint::derive(cfg)?.http_push;
        let path = url.path().replacen("/push/", "/config/", 1);
        url.set_path(&path);
        let response = client.post(url).timeout(std::time::Duration::from_secs(15)).json(&json!({
            "revision": self.revision,
            "config": RuntimeConfig::current(cfg),
            "installation": {
                "service_tier": cfg.service_tier,
                "config_path": cfg.config_path.to_string_lossy(),
                "executable_path": std::env::current_exe().map(|path| path.to_string_lossy().into_owned()).unwrap_or_default(),
                "auto_update_supported": auto_update_supported(cfg),
            },
            "error": self.error,
        })).send().await?;
        anyhow::ensure!(
            response.status().is_success(),
            "configuration exchange rejected: {}",
            response.status()
        );
        let bytes = crate::probes::read_limited(response, 64 * 1024).await?;
        let reply: Reply = serde_json::from_slice(&bytes)?;
        Ok(reply.runtime_config)
    }

    pub async fn synchronize(
        &mut self,
        client: &reqwest::Client,
        cfg: &mut Config,
        signals: &mpsc::Sender<Signal>,
    ) -> Result<()> {
        let Some(desired) = self.exchange(client, cfg).await? else {
            return Ok(());
        };
        let outcome = async {
            let (next, document) = desired.config.prepare(cfg, &desired.revision)?;
            let (claim, claimed) = oneshot::channel();
            let state = Arc::new(ClaimState::default());
            let _cancel_on_drop = CancelOnDrop(state.clone());
            signals
                .send(Signal::Reconfigure(Reconfiguration { next: Box::new(next.clone()), document, claim, state: state.clone() }))
                .await
                .context("sampler configuration channel closed")?;
            let completion = wait_for_claim(&state, claimed, Duration::from_secs(45)).await?;
            completion.await.context("sampler configuration was interrupted")?
                .map_err(anyhow::Error::msg)?;
            *cfg = next;
            self.revision = Some(desired.revision);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        self.error = outcome
            .as_ref()
            .err()
            .map(|error| format!("{error:#}").chars().take(256).collect());
        let _ = self.exchange(client, cfg).await;
        outcome
    }
}

#[derive(Deserialize)]
struct Desired {
    revision: String,
    config: RuntimeConfig,
}
#[derive(Deserialize)]
struct Reply {
    runtime_config: Option<Desired>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_fields_and_unsafe_intervals() {
        let valid = json!({"sample_interval_secs":5,"report_interval_secs":60,"include_nics":[],"exclude_nics":[],"include_mounts":[],"exclude_mounts":[],"auto_update":false});
        let mut config: RuntimeConfig = serde_json::from_value(valid.clone()).unwrap();
        assert!(config.validate().is_ok());
        config.sample_interval_secs = 61;
        assert!(config.validate().is_err());
        config.sample_interval_secs = 0;
        assert!(config.validate().is_err());
        let mut unsafe_value = valid;
        unsafe_value["endpoint"] = json!("https://other.invalid");
        assert!(serde_json::from_value::<RuntimeConfig>(unsafe_value).is_err());
        assert!(!valid_revision("../../config"));
    }
    fn fixture_config(path: std::path::PathBuf) -> Config {
        Config {
            endpoint: "https://example.test".into(), token: "fixture".into(),
            sample_interval_secs: 5, report_interval_secs: 60, report_interval_explicit: false, insecure: false,
            include_nics: vec![], exclude_nics: vec![], include_mounts: vec![], exclude_mounts: vec![],
            auto_update: false, log_file: None, service_tier: None,
            config_path: path, runtime_overrides: Default::default(),
        }
    }

    #[tokio::test]
    async fn cancelled_configuration_never_commits_and_claimed_configuration_does() {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).unwrap();
        let directory = std::env::temp_dir().join(format!("polaris-config-{}", hex::encode(random)));
        std::fs::create_dir(&directory).unwrap();
        let cfg = fixture_config(directory.join("config.json"));
        std::fs::write(&cfg.config_path, br#"{"endpoint":"https://example.test","token":"fixture"}"#).unwrap();
        let revision = "0123456789abcdef0123456789abcdef";
        let mut desired = RuntimeConfig::current(&cfg);
        desired.sample_interval_secs = 10;
        let (next, document) = desired.prepare(&cfg, revision).unwrap();
        let (claim, receipt) = oneshot::channel();
        let state = Arc::new(ClaimState::default());
        assert!(wait_for_claim(&state, receipt, Duration::from_millis(10)).await.is_err());
        assert!(Reconfiguration { next: Box::new(next.clone()), document: document.clone(), claim, state }.claim_and_commit().is_none());
        assert!(!std::fs::read_to_string(&cfg.config_path).unwrap().contains("runtime_revision"));
        let (claim, receipt) = oneshot::channel();
        let state = Arc::new(ClaimState::default());
        let (committed, applied) = Reconfiguration { next: Box::new(next), document, claim, state }.claim_and_commit().unwrap();
        assert_eq!(committed.sample_interval_secs, 10);
        applied.send(Ok(())).unwrap();
        assert!(receipt.await.unwrap().await.unwrap().is_ok());
        assert!(std::fs::read_to_string(&cfg.config_path).unwrap().contains(revision));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_local_runtime_values_cannot_be_overridden() {
        let mut cfg = fixture_config("unused".into());
        cfg.runtime_overrides.sample_interval = true;
        cfg.runtime_overrides.auto_update = true;
        let mut desired = RuntimeConfig::current(&cfg);
        desired.sample_interval_secs = 10;
        assert!(desired.prepare(&cfg, "0123456789abcdef0123456789abcdef").unwrap_err().to_string().contains("sample_interval"));
        desired.sample_interval_secs = 5;
        desired.auto_update = true;
        assert!(desired.prepare(&cfg, "0123456789abcdef0123456789abcdef").unwrap_err().to_string().contains("auto_update"));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_waits_for_a_claim_that_won_before_receipt_delivery() {
        let state = Arc::new(ClaimState::default());
        let (claim, receipt) = oneshot::channel();
        let waiting_state = state.clone();
        let waiter = tokio::spawn(async move {
            let completion = wait_for_claim(&waiting_state, receipt, Duration::from_secs(45)).await?;
            completion.await.context("completion interrupted")?.map_err(anyhow::Error::msg)
        });
        tokio::task::yield_now().await;
        assert!(state.claim());
        tokio::time::advance(Duration::from_secs(45)).await;
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        let (applied, completion) = oneshot::channel();
        claim.send(completion).unwrap();
        applied.send(Ok(())).unwrap();
        assert!(waiter.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn aborting_waiter_cancels_an_unclaimed_queued_configuration() {
        let state = Arc::new(ClaimState::default());
        let (claim, receipt) = oneshot::channel();
        let guard = CancelOnDrop(state.clone());
        let waiter_state = state.clone();
        let waiter = tokio::spawn(async move {
            let _guard = guard;
            wait_for_claim(&waiter_state, receipt, Duration::from_secs(45)).await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.is_err());
        let cfg = fixture_config("unused".into());
        assert!(Reconfiguration { next: Box::new(cfg), document: vec![], claim, state }.claim_and_commit().is_none());
    }

    #[test]
    fn locked_filter_lists_compare_members_not_order_or_duplicates() {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).unwrap();
        let path = std::env::temp_dir().join(format!("polaris-filter-{}", hex::encode(random)));
        let mut cfg = fixture_config(path);
        cfg.include_nics = vec!["eth0".into(), "eth1".into(), "eth0".into()];
        cfg.exclude_nics = vec!["lo".into(), "docker".into(), "lo".into()];
        cfg.include_mounts = vec!["/".into(), "/data".into(), "/".into()];
        cfg.exclude_mounts = vec!["/tmp".into(), "/proc".into(), "/tmp".into()];
        cfg.runtime_overrides.include_nics = true;
        cfg.runtime_overrides.exclude_nics = true;
        cfg.runtime_overrides.include_mounts = true;
        cfg.runtime_overrides.exclude_mounts = true;
        let mut desired = RuntimeConfig::current(&cfg);
        desired.include_nics = vec!["eth1".into(), "eth0".into()];
        desired.exclude_nics = vec!["docker".into(), "lo".into()];
        desired.include_mounts = vec!["/data".into(), "/".into()];
        desired.exclude_mounts = vec!["/proc".into(), "/tmp".into()];
        desired.report_interval_secs = 120;
        let revision = "0123456789abcdef0123456789abcdef";
        let (next, _) = desired.prepare(&cfg, revision).unwrap();
        assert_eq!(next.report_interval_secs, 120);
        assert_eq!(next.include_nics, cfg.include_nics);
        assert_eq!(next.exclude_nics, cfg.exclude_nics);
        assert_eq!(next.include_mounts, cfg.include_mounts);
        assert_eq!(next.exclude_mounts, cfg.exclude_mounts);
        for field in 0..4 {
            let mut changed = desired.clone();
            match field {
                0 => changed.include_nics.push("ETH0".into()),
                1 => changed.exclude_nics.push("extra".into()),
                2 => changed.include_mounts.push("/DATA".into()),
                _ => changed.exclude_mounts.push("/extra".into()),
            }
            assert!(changed.prepare(&cfg, revision).is_err());
        }
    }

}
