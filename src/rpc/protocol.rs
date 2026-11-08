//! Builders for Agent JSON-RPC messages; wire fields must match src/lib/agent/protocol.ts.
//! Agent JSON-RPC 消息构造器；传输字段必须与 src/lib/agent/protocol.ts 一致。

use anyhow::Result;
use serde_json::json;

use super::RpcRequest;
use crate::sysstat::{AgentExtras, SysSnapshot};

/// Identity report with network metadata, capabilities and the chosen transport.
/// 构造包含网络元数据、能力和当前传输方式的身份上报帧。
pub fn build_basic_report(
    extras: &AgentExtras,
    transport: &str,
    report_interval_secs: u64,
) -> Result<RpcRequest> {
    let name = hostname();
    let mut capabilities = vec!["probe_http", "probe_tcp", "probe_udp", "probe_ssl", "probe_domain", "probe_dns", "commands", "config_v1"];
    if crate::probe_network::supports_icmp() { capabilities.push("probe_icmp"); }
    let params = json!({
        "name": name,
        "os": os_name(),
        "arch": arch_name(),
        "kernel": kernel_version(),
        "cpu_model": cpu_model(),
        "cpu_cores": cpu_cores(),
        "mem_total": mem_total(),
        "version": env!("CARGO_PKG_VERSION"),
        "transport": transport,
        "report_interval_secs": report_interval_secs,
        "capabilities": capabilities,
        "publicIp": extras.public_ip,
        "publicIpv6": extras.public_ipv6,
        "isp": extras.isp,
        "region": extras.region,
        "virtualization": extras.virtualization,
    });
    Ok(RpcRequest::with_id(1, "basicReport", params))
}

/// Build metrics with observation time and finalized minute summaries.
/// WebSocket send assigns a request ID and waits for an ACK; empty detail arrays serialize as null.
/// 构造包含观测时间、采样指标和已结束分钟摘要的上报帧。
/// WebSocket 发送时分配请求 ID 并等待确认；空明细数组序列化为 null。
pub fn build_report(
    s: &SysSnapshot,
    observed_at: i64,
    summaries: &[crate::sysstat::FinalizedAggregate],
) -> RpcRequest {
    let interfaces = if s.interfaces.is_empty() {
        serde_json::Value::Null
    } else {
        json!(s.interfaces)
    };
    let mounts = if s.mounts.is_empty() {
        serde_json::Value::Null
    } else {
        json!(s.mounts)
    };
    let params = json!({
        "cpu":    { "usage": s.cpu_usage },
        "ram":    { "total": s.mem_total, "used": s.mem_used },
        "swap":   { "total": s.swap_total, "used": s.swap_used },
        "load":   { "load1": s.load1, "load5": s.load5, "load15": s.load15 },
        "disk":   { "total": s.disk_total, "used": s.disk_used },
        "network": {
            "up": s.net_up_bps,
            "down": s.net_down_bps,
            "totalUp": s.net_total_up,
            "totalDown": s.net_total_down,
        },
        "connections": { "tcp": s.tcp_conns, "udp": s.udp_conns },
        "uptime":  s.uptime_secs,
        "process": s.process_count,
        "message": s.message,
        "observed_at": observed_at,
        "summaries": summaries,
        "interfaces": interfaces,
        "mounts": mounts,
    });
    RpcRequest::notification("report", params)
}

// ---------------------------------------------------------------------------
// Static host info — fetched on demand so tests don't need sysinfo.
// 按需获取静态主机信息，使测试不依赖 sysinfo。
// ---------------------------------------------------------------------------

fn hostname() -> String {
    sysinfo::System::host_name().unwrap_or_else(|| "unknown-host".to_string())
}

/// Use a distribution display name for fleet OS icons, or the OS family when unavailable.
/// 设备列表图标使用发行版显示名称；不可用时使用操作系统家族名称。
fn os_name() -> String {
    distro_name().unwrap_or_else(|| std::env::consts::OS.to_string())
}

#[cfg(target_os = "linux")]
fn distro_name() -> Option<String> {
    // `PRETTY_NAME` is the human-readable line: "Ubuntu 24.04.1 LTS". The
    // quoted value may contain `=` or spaces, so the split is on the first one
    // only, and the surrounding quotes are stripped.
    // `PRETTY_NAME` 为可读名称；值可包含等号或空格，只按首个等号分割并去除外围引号。
    let text = std::fs::read_to_string("/etc/os-release")
        .or_else(|_| std::fs::read_to_string("/usr/lib/os-release"))
        .ok()?;
    text.lines()
        .find_map(|line| line.strip_prefix("PRETTY_NAME="))
        .map(|value| value.trim().trim_matches('"').to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(target_os = "macos")]
fn distro_name() -> Option<String> {
    // macOS has no os-release; `sw_vers -productName` is "macOS" and
    // `-productVersion` is "14.5". The pair is what an operator recognises.
    // macOS 没有 os-release；使用 sw_vers 的产品名称和版本供操作员识别。
    let name = std::process::Command::new("sw_vers")
        .arg("-productName")
        .output()
        .ok()?;
    let version = std::process::Command::new("sw_vers")
        .arg("-productVersion")
        .output()
        .ok()?;
    let name = String::from_utf8_lossy(&name.stdout).trim().to_string();
    let version = String::from_utf8_lossy(&version.stdout).trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some(if version.is_empty() {
        name
    } else {
        format!("{name} {version}")
    })
}

#[cfg(target_os = "windows")]
fn distro_name() -> Option<String> {
    use winreg::{enums::HKEY_LOCAL_MACHINE, RegKey};

    // Registry strings are Unicode; cmd output uses the machine's OEM code page. / 注册表字符串使用 Unicode，cmd 输出使用系统 OEM 编码。
    let key = RegKey::predef(HKEY_LOCAL_MACHINE)
        .open_subkey(r"SOFTWARE\Microsoft\Windows NT\CurrentVersion")
        .ok()?;
    let product: String = key.get_value("ProductName").ok()?;
    let build: String = key.get_value("CurrentBuildNumber").unwrap_or_default();
    let display: String = key.get_value("DisplayVersion").unwrap_or_default();
    let revision: u32 = key.get_value("UBR").unwrap_or_default();
    Some(windows_display_name(&product, &display, &build, revision))
}

#[cfg(any(target_os = "windows", test))]
fn windows_display_name(product: &str, display: &str, build: &str, revision: u32) -> String {
    let product = if build.parse::<u32>().unwrap_or_default() >= 22000 {
        product.replacen("Windows 10", "Windows 11", 1)
    } else {
        product.to_owned()
    };
    let mut name = product.trim().to_owned();
    if !display.trim().is_empty() {
        name.push_str(&format!(" {}", display.trim()));
    }
    if !build.is_empty() && build.bytes().all(|byte| byte.is_ascii_digit()) {
        name.push_str(&format!(" (10.0.{build}.{revision})"));
    }
    name
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn distro_name() -> Option<String> {
    None
}

fn arch_name() -> String {
    std::env::consts::ARCH.to_string()
}

fn kernel_version() -> String {
    sysinfo::System::kernel_version().unwrap_or_else(|| "unknown".to_string())
}

fn cpu_model() -> String {
    let mut sys = sysinfo::System::new();
    sys.refresh_cpu();
    sys.cpus()
        .first()
        .map(|c| c.brand().trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-cpu".to_string())
}

fn cpu_cores() -> u64 {
    let mut sys = sysinfo::System::new();
    sys.refresh_cpu();
    sys.cpus().len() as u64
}

fn mem_total() -> u64 {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    sys.total_memory()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_version_uses_unicode_and_identifies_windows_11() {
        assert_eq!(windows_display_name("Windows 10 Pro", "24H2", "26100", 33158), "Windows 11 Pro 24H2 (10.0.26100.33158)");
        assert_eq!(windows_display_name("Windows 10 专业版", "22H2", "19045", 1), "Windows 10 专业版 22H2 (10.0.19045.1)");
        assert_eq!(windows_display_name("Windows Server 2025", "", "26100", 1), "Windows Server 2025 (10.0.26100.1)");
    }
}
