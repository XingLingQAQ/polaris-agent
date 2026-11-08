//! Phase 4 protocol contract tests.
//!
//! These tests assert the JSON-RPC envelope the agent emits matches the
//! worker's expected shape byte-for-byte (per field). Any drift here means
//! the worker will fail to parse our messages.
//!
//! 协议契约测试逐字段检查 Agent 发出的 JSON-RPC 封装是否符合 Worker 的要求；偏差会导致解析失败。

use serde_json::{json, Value};
use polaris_agent::commands::{decode_commands, Action};
use polaris_agent::protocol::{build_basic_report, build_report};
use polaris_agent::{
    AgentExtras, FinalizedAggregate, InterfaceStats, MountStats, RpcRequest, Sampler, SysSnapshot,
};

fn value_of(req: &RpcRequest) -> Value {
    serde_json::to_value(req).expect("request must serialize")
}

#[test]
fn rpc_envelope_has_jsonrpc_2_0() {
    let req = build_report(&SysSnapshot::default(), 123, &[]);
    let v = value_of(&req);
    assert_eq!(v["jsonrpc"], "2.0");
    assert!(v["id"].is_null(), "report must be a notification (id=null)");
    assert_eq!(v["method"], "report");
}

#[test]
fn basic_report_envelope() {
    let req = build_basic_report(&AgentExtras::default(), "ws", 60).expect("basicReport");
    let v = value_of(&req);
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], json!(1));
    assert_eq!(v["method"], "basicReport");

    let p = &v["params"];
    for key in [
        "name",
        "os",
        "arch",
        "kernel",
        "cpu_model",
        "cpu_cores",
        "mem_total",
        "version",
        "transport",
        "publicIpv6",
        "report_interval_secs",
    ] {
        assert!(p.get(key).is_some(), "missing key: {key}\n{p:#}");
    }
    assert!(p["cpu_cores"].is_u64() || p["cpu_cores"].is_i64());
    assert!(p["mem_total"].is_u64());
}

/// The transport is how the console answers "is this fleet on the socket or
/// the HTTP fallback" - the first question when a monitor goes quiet behind a
/// proxy. It has to survive serialisation verbatim.
///
/// The capability list is asserted exactly, not by `contains`: the *set* is the
/// contract. A missing entry makes the server refuse work the agent could do, and
/// an extra entry it does not implement makes the server send work it cannot -
/// both are silent, and both are only visible if the whole list is pinned.
/// 传输方式必须原样序列化，以便控制台区分 WebSocket 与 HTTP 回退。
/// 能力列表须精确匹配：缺少或多报能力都会造成服务端错误地分配或拒绝工作。
#[test]
fn basic_report_carries_the_transport() {
    let ws = value_of(&build_basic_report(&AgentExtras::default(), "ws", 60).unwrap());
    assert_eq!(ws["params"]["transport"], "ws");
    assert_eq!(
        ws["params"]["capabilities"],
        if polaris_agent::probe_network::supports_icmp() {
            json!(["probe_http", "probe_tcp", "probe_udp", "probe_ssl", "probe_domain", "probe_dns", "commands", "config_v1", "probe_icmp"])
        } else {
            json!(["probe_http", "probe_tcp", "probe_udp", "probe_ssl", "probe_domain", "probe_dns", "commands", "config_v1"])
        }
    );

    let http = value_of(&build_basic_report(&AgentExtras::default(), "http", 60).unwrap());
    assert_eq!(http["params"]["transport"], "http");
}

#[test]
fn report_envelope_and_nesting() {
    let snap = SysSnapshot {
        cpu_usage: 23.5,
        mem_total: 8_589_934_592,
        mem_used: 3_221_225_472,
        swap_total: 0,
        swap_used: 0,
        load1: 0.50,
        load5: 0.30,
        load15: 0.20,
        disk_total: 107_374_182_400,
        disk_used: 42_949_672_960,
        net_up_bps: 10240,
        net_down_bps: 51200,
        net_total_up: 1_099_511_627_776,
        net_total_down: 2_199_023_255_552,
        tcp_conns: 12,
        udp_conns: 1,
        uptime_secs: 864_000,
        process_count: 156,
        message: String::new(),
        interfaces: vec![],
        mounts: vec![],
    };

    let summary = FinalizedAggregate {
        bucket_ts: 120,
        sample_count: 2,
        cpu_samples: 2,
        cpu_avg: Some(20.0),
        cpu_max: Some(30.0),
        mem_samples: 2,
        mem_pct_avg: Some(40.0),
        mem_pct_max: Some(50.0),
        load_1_avg: Some(0.5),
        load_5_avg: Some(0.3),
        net_up_bytes: Some(100),
        net_down_bytes: Some(200),
        duration_sec: 60,
        network_duration_sec: 60,
    };
    let req = build_report(&snap, 180, std::slice::from_ref(&summary));
    let v = value_of(&req);
    let p = &v["params"];

    assert_eq!(p["cpu"]["usage"], json!(23.5));
    assert_eq!(p["ram"]["total"], json!(8_589_934_592u64));
    assert_eq!(p["ram"]["used"], json!(3_221_225_472u64));
    assert_eq!(p["swap"]["total"], json!(0));
    assert_eq!(p["swap"]["used"], json!(0));
    assert_eq!(p["load"]["load1"], json!(0.5));
    assert_eq!(p["load"]["load5"], json!(0.3));
    assert_eq!(p["load"]["load15"], json!(0.2));
    assert_eq!(p["disk"]["total"], json!(107_374_182_400u64));
    assert_eq!(p["disk"]["used"], json!(42_949_672_960u64));
    assert_eq!(p["network"]["up"], json!(10_240u64));
    assert_eq!(p["network"]["down"], json!(51_200u64));
    assert_eq!(p["network"]["totalUp"], json!(1_099_511_627_776u64));
    assert_eq!(p["network"]["totalDown"], json!(2_199_023_255_552u64));
    assert_eq!(p["connections"]["tcp"], json!(12u64));
    assert_eq!(p["connections"]["udp"], json!(1u64));
    assert_eq!(p["uptime"], json!(864_000u64));
    assert_eq!(p["process"], json!(156u64));
    assert_eq!(p["message"], json!(""));
    assert_eq!(p["observed_at"], json!(180));
    assert_eq!(p["summaries"][0]["sample_count"], json!(2));
    assert_eq!(p["summaries"][0]["cpu_avg"], json!(20.0));
    assert_eq!(p["summaries"][0]["cpu_max"], json!(30.0));
}

#[test]
fn check_update_action_decodes() {
    let body = serde_json::to_vec(&json!({
        "jobs": [],
        "commands": [{"id": "a", "action": "check_update", "lease_token": "lease"}],
    }))
    .unwrap();
    let decoded = decode_commands(&body);
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].action, Action::CheckUpdate);
    assert_eq!(decoded[0].id, "a");
}

#[test]
fn unknown_actions_are_still_rejected() {
    let body = serde_json::to_vec(&json!({
        "jobs": [],
        "commands": [
            {"id": "a", "action": "check_updates", "lease_token": "lease"},
            {"id": "b", "action": "self_destruct", "lease_token": "lease"},
        ],
    }))
    .unwrap();
    assert!(decode_commands(&body).is_empty());
}

#[test]
fn sampler_first_frame_reports_zero_rate() {
    // First `capture()` has no previous network sample, so up/down rates
    // must be 0 — diff-of-one-frame is undefined.
    // 首次采样没有先前网络计数器，无法计算差值，因此上下行速率为 0。
    let mut s = Sampler::new(5);
    let snap = s.capture();
    assert_eq!(snap.net_up_bps, 0, "first frame must have up=0");
    assert_eq!(snap.net_down_bps, 0, "first frame must have down=0");
    // cumulative counters are plain u64s; reachability of Sampler::capture
    // already proves they're wired.
    // 累计计数器是 u64；能够调用 Sampler::capture 已证明其接入。
}

#[test]
fn sampler_returns_snapshot_with_all_fields() {
    let mut s = Sampler::new(5);
    let snap = s.capture();
    // Fields must exist and be serializable — spot-check via JSON round-trip
    // 字段必须存在且可序列化；通过 JSON 往返抽查。
    let v = serde_json::to_value(&snap).unwrap();
    let json_keys = [
        "cpu_usage",
        "mem_total",
        "mem_used",
        "swap_total",
        "swap_used",
        "load1",
        "load5",
        "load15",
        "disk_total",
        "disk_used",
        "net_up_bps",
        "net_down_bps",
        "net_total_up",
        "net_total_down",
        "tcp_conns",
        "udp_conns",
        "uptime_secs",
        "process_count",
        "message",
    ];
    for k in json_keys {
        assert!(v.get(k).is_some(), "SysSnapshot missing field: {k}");
    }
}

#[test]
fn report_serialises_interfaces_and_mounts() {
    let snap = SysSnapshot {
        interfaces: vec![
            InterfaceStats {
                name: "eth0".to_string(),
                up_rate: 1024,
                down_rate: 2048,
                total_up_bytes: 123_456_789,
                total_down_bytes: 987_654_321,
            },
            InterfaceStats {
                name: "wlan0".to_string(),
                up_rate: 0,
                down_rate: 512,
                total_up_bytes: 1_000,
                total_down_bytes: 2_000,
            },
        ],
        mounts: vec![
            MountStats {
                name: "/".to_string(),
                total_bytes: 107_374_182_400,
                used_bytes: 42_949_672_960,
                free_bytes: 64_424_509_440,
                used_percent: 40.0,
            },
            MountStats {
                name: "/boot".to_string(),
                total_bytes: 1_073_741_824,
                used_bytes: 268_435_456,
                free_bytes: 805_306_368,
                used_percent: 25.0,
            },
        ],
        ..SysSnapshot::default()
    };

    let req = build_report(&snap, 123, &[]);
    let v = value_of(&req);
    let p = &v["params"];

    let ifaces = p["interfaces"]
        .as_array()
        .expect("interfaces must be an array");
    assert_eq!(ifaces.len(), 2);
    assert_eq!(ifaces[0]["name"], json!("eth0"));
    assert_eq!(ifaces[0]["up_rate"], json!(1024u64));
    assert_eq!(ifaces[0]["down_rate"], json!(2048u64));
    assert_eq!(ifaces[0]["total_up_bytes"], json!(123_456_789u64));
    assert_eq!(ifaces[0]["total_down_bytes"], json!(987_654_321u64));
    assert_eq!(ifaces[1]["name"], json!("wlan0"));

    let mounts = p["mounts"].as_array().expect("mounts must be an array");
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts[0]["name"], json!("/"));
    assert_eq!(mounts[0]["total_bytes"], json!(107_374_182_400u64));
    assert_eq!(mounts[0]["used_bytes"], json!(42_949_672_960u64));
    assert_eq!(mounts[0]["free_bytes"], json!(64_424_509_440u64));
    assert_eq!(mounts[0]["used_percent"], json!(40.0));
    assert_eq!(mounts[1]["name"], json!("/boot"));
}

#[test]
fn report_serialises_empty_interfaces_and_mounts_as_null() {
    // Empty Vec must land on the wire as JSON null (not []), so the worker
    // can distinguish "agent does not populate this" from "agent populated
    // but saw nothing".
    // 空 Vec 在线路上序列化为 JSON null，以区分未采集与采集后为空。
    let snap = SysSnapshot::default();
    let req = build_report(&snap, 123, &[]);
    let v = value_of(&req);
    let p = &v["params"];
    assert!(p["interfaces"].is_null(), "empty interfaces must be null");
    assert!(p["mounts"].is_null(), "empty mounts must be null");
}
