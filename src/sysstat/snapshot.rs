//! Point-in-time system metrics; rpc::protocol converts them to wire field names.
//! 时点系统指标；rpc::protocol 将其转换为传输字段名称。

use serde::Serialize;

/// Aggregated system stats at a single sample point.
///
/// All byte counts are raw bytes; all rates are bytes/second.
/// 单次采样的系统统计；字节数为原始字节，速率为字节/秒。
#[derive(Debug, Clone, Serialize)]
pub struct SysSnapshot {
    /// CPU usage percent, 0..100, rounded to 2 decimal places.
    /// CPU 使用率，0 到 100，保留两位小数。
    pub cpu_usage: f64,

    /// Total physical memory, bytes.
    /// 物理内存总量，单位为字节。
    pub mem_total: u64,
    /// Used memory (excluding buffers/cache where sysinfo can determine), bytes.
    /// 已用内存，单位为字节；sysinfo 可识别时排除缓冲区和缓存。
    pub mem_used: u64,

    /// Total swap, bytes.
    /// 交换空间总量，单位为字节。
    pub swap_total: u64,
    /// Used swap, bytes.
    /// 已用交换空间，单位为字节。
    pub swap_used: u64,

    pub load1: f64,
    pub load5: f64,
    pub load15: f64,

    /// Total size of primary disk, bytes.
    /// 主磁盘总容量，单位为字节。
    pub disk_total: u64,
    /// Used bytes on primary disk.
    /// 主磁盘已用字节数。
    pub disk_used: u64,

    /// Current outbound rate, bytes/sec.
    /// 当前出站速率，单位为字节/秒。
    pub net_up_bps: u64,
    /// Current inbound rate, bytes/sec.
    /// 当前入站速率，单位为字节/秒。
    pub net_down_bps: u64,
    /// Cumulative bytes sent since boot.
    /// 自启动以来累计发送的字节数。
    pub net_total_up: u64,
    /// Cumulative bytes received since boot.
    /// 自启动以来累计接收的字节数。
    pub net_total_down: u64,

    /// Open TCP connections.
    /// 已打开的 TCP 连接数。
    pub tcp_conns: u64,
    /// Open UDP sockets.
    /// 已打开的 UDP 套接字数。
    pub udp_conns: u64,

    /// System uptime, seconds.
    /// 系统运行时间，单位为秒。
    pub uptime_secs: u64,
    /// Number of running processes.
    /// 正在运行的进程数。
    pub process_count: u64,

    /// Optional free-text message for the dashboard (currently unused).
    /// 控制台可选的自由文本消息（目前未使用）。
    pub message: String,

    /// Per-NIC stats. Empty on a fresh first sample or builds that choose
    /// not to populate it — servers must tolerate absence.
    /// 各网卡统计；首次采样或未填充的构建为空，服务端须容忍缺失。
    pub interfaces: Vec<InterfaceStats>,

    /// Per-mount disk stats. Empty on agents that do not populate it.
    /// 各挂载点的磁盘统计；未填充时为空。
    pub mounts: Vec<MountStats>,
}

/// One network interface's counters at the sample point.
///
/// Rates are bytes/sec over the previous sample interval; lifetime totals
/// are raw bytes since boot. Naming mirrors the wire contract.
/// 单个网卡的采样计数器；速率按上一采样间隔计算，累计值为启动后的字节数，命名遵循传输契约。
#[derive(Debug, Clone, Serialize)]
pub struct InterfaceStats {
    pub name: String,
    /// Current outbound rate, bytes/sec.
    /// 当前出站速率，单位为字节/秒。
    pub up_rate: u64,
    /// Current inbound rate, bytes/sec.
    /// 当前入站速率，单位为字节/秒。
    pub down_rate: u64,
    /// Lifetime bytes sent.
    /// 累计发送字节数。
    pub total_up_bytes: u64,
    /// Lifetime bytes received.
    /// 累计接收字节数。
    pub total_down_bytes: u64,
}

/// One mounted filesystem's usage at the sample point.
/// 单个挂载文件系统的采样使用量。
#[derive(Debug, Clone, Serialize)]
pub struct MountStats {
    /// Mount point path ("/", "/boot", "C:\\", ...).
    /// 挂载点路径（如 "/"、"/boot"、"C:\\"）。
    pub name: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    /// Percent used, 0..100, rounded to 2 decimal places.
    /// 使用百分比，0 到 100，保留两位小数。
    pub used_percent: f64,
}

impl Default for SysSnapshot {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            mem_total: 0,
            mem_used: 0,
            swap_total: 0,
            swap_used: 0,
            load1: 0.0,
            load5: 0.0,
            load15: 0.0,
            disk_total: 0,
            disk_used: 0,
            net_up_bps: 0,
            net_down_bps: 0,
            net_total_up: 0,
            net_total_down: 0,
            tcp_conns: 0,
            udp_conns: 0,
            uptime_secs: 0,
            process_count: 0,
            message: String::new(),
            interfaces: Vec::new(),
            mounts: Vec::new(),
        }
    }
}
