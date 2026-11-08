//! Whitelisted remote commands and their execution receipts.
//! 允许执行的远程命令及其执行回执。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

const MAX_COMMANDS: usize = 8;

/// What the probes task asks the main loop to do.
///
/// `Exit` carries its own receipt: the main loop is the only one that can end
/// the process, and it must not do that before the answer is delivered.
/// 探针任务向主循环发送的操作；只有主循环能退出进程，必须先送达 `Exit` 回执。
pub enum Signal {
    /// Send a fresh `basicReport` instead of waiting for the next reconnect.
    /// 立即发送新的 `basicReport`，不等待下次重连。
    ReReport,
    Reconfigure(crate::runtime_config::Reconfiguration),
    /// Leave the supervisor loop. Fires once the command result was accepted.
    /// 命令结果被接受后退出主管理循环。
    Exit(oneshot::Receiver<()>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Upgrade,
    Restart,
    ReReport,
    CheckUpdate,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Command {
    pub id: String,
    pub action: Action,
    pub lease_token: String,
}

#[derive(Clone, Serialize)]
pub struct CommandResultBody {
    pub id: String,
    pub lease_token: String,
    pub ok: bool,
    pub message: String,
}

pub fn decode_commands(body: &[u8]) -> Vec<Command> {
    let Ok(response) = serde_json::from_slice::<Value>(body) else {
        return Vec::new();
    };
    let Some(items) = response.get("commands").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .take(MAX_COMMANDS)
        .filter_map(|item| serde_json::from_value(item.clone()).ok())
        .filter(valid_command)
        .collect()
}

fn valid_command(command: &Command) -> bool {
    !command.id.is_empty()
        && command.id.len() <= 256
        && !command.lease_token.is_empty()
        && command.lease_token.len() <= 256
}

// Only tier A restarts an exited process.
// 只有 A 级会在进程退出后重启。
pub fn is_supervised(tier: Option<&str>) -> bool {
    tier == Some("A")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(commands: Value) -> Value {
        serde_json::json!({ "jobs": [], "commands": commands })
    }

    #[test]
    fn decodes_known_actions_and_drops_everything_unusable() {
        let decoded = decode_commands(
            &serde_json::to_vec(&response(serde_json::json!([
                {"id": "a", "action": "upgrade", "lease_token": "lease"},
                {"id": "b", "action": "restart", "lease_token": "lease"},
                {"id": "c", "action": "re_report", "lease_token": "lease"},
                {"id": "d", "action": "self_destruct", "lease_token": "lease"},
                {"id": "e", "action": "restart"},
                {"id": "", "action": "restart", "lease_token": "lease"},
                {"id": "g", "action": "restart", "lease_token": ""},
                {"id": 7, "action": "restart", "lease_token": "lease"},
                {"id": "i", "action": ["restart"], "lease_token": "lease"},
                "not an object at all",
            ])))
            .unwrap(),
        );
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].action, Action::Upgrade);
        assert_eq!(decoded[1].action, Action::Restart);
        assert_eq!(decoded[2].action, Action::ReReport);
        assert_eq!(decoded[0].id, "a");
        assert_eq!(decoded[2].lease_token, "lease");
    }

    #[test]
    fn absent_empty_or_unparseable_commands_yield_nothing() {
        assert!(decode_commands(br#"{"jobs": []}"#).is_empty());
        assert!(decode_commands(b"<html>200</html>").is_empty());
        assert!(
            decode_commands(&serde_json::to_vec(&response(serde_json::json!([]))).unwrap())
                .is_empty()
        );
        assert!(decode_commands(&serde_json::to_vec(&response(Value::Null)).unwrap()).is_empty());
        assert!(decode_commands(
            &serde_json::to_vec(&response(serde_json::json!("restart"))).unwrap()
        )
        .is_empty());
    }

    #[test]
    fn only_a_known_service_tier_justifies_an_exit() {
        for tier in ["A"] {
            assert!(is_supervised(Some(tier)), "{tier} supervises the agent");
        }
        for tier in [
            "B",
            "C",
            "D",
            "manual",
            "",
            "weird",
            "systemctl",
            "systemd",
            "openrc",
            "supervisord",
            "s6",
        ] {
            assert!(!is_supervised(Some(tier)), "{tier} does not supervise it");
        }
        assert!(!is_supervised(None));
    }

    /// Tier B is "started now, restarted on boot" - it does **not** come back
    /// after the process exits. Treating it as supervised would let a `restart`
    /// command kill a working agent and leave the host unmonitored until the
    /// next reboot, so this is asserted separately from the general list above.
    /// B 级只在当前启动和开机时运行，进程退出后不会重启；误判会导致重启命令后停止监控直至重启系统。
    #[test]
    fn tier_b_is_not_supervised_because_cron_only_fires_on_boot() {
        assert!(!is_supervised(Some("B")));
    }
}
