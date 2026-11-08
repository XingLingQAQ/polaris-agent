//! Cross-platform system statistics via the `sysinfo` crate.
//!
//! Platform-specific details are abstracted by sysinfo itself, so a single
//! implementation serves Linux / macOS / Windows.
//! 通过 sysinfo 提供跨平台系统统计；同一实现适用于 Linux、macOS 和 Windows。

mod aggregate;
mod snapshot;
mod network_route;

pub use aggregate::{AggregateSample, FinalizedAggregate, MinuteAggregator};
pub use snapshot::*;

pub mod extras;
pub use extras::AgentExtras;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use sysinfo::{Disks, Networks, System};

/// Stateful sampler: tracks previous network counters so we can derive
/// rates (bytes/sec) by differentiating over the sampling interval.
/// 有状态采样器：保留先前的网络计数器，按采样间隔计算字节/秒速率。
pub struct Sampler {
    sys: System,
    disks: Disks,
    networks: Networks,
    last_net: Option<NetCounters>,
    last_net_per_nic: HashMap<String, NetCounters>,
    last_tick: Option<Instant>,
    interval: Duration,
    include_nics: Vec<String>,
    exclude_nics: Vec<String>,
    include_mounts: Vec<String>,
    exclude_mounts: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct NetCounters {
    rx: u64,
    tx: u64,
}

impl Sampler {
    pub fn new(interval_secs: u64) -> Self {
        Self::build(interval_secs, &[], &[], &[], &[])
    }

    pub fn with_config(interval_secs: u64, cfg: &crate::config::Config) -> Self {
        Self::build(
            interval_secs,
            &cfg.include_nics,
            &cfg.exclude_nics,
            &cfg.include_mounts,
            &cfg.exclude_mounts,
        )
    }

    fn build(
        interval_secs: u64,
        include_nics: &[String],
        exclude_nics: &[String],
        include_mounts: &[String],
        exclude_mounts: &[String],
    ) -> Self {
        let mut sys = System::new_all();
        // Warm-up: first refresh sets baselines; cpu_usage needs two samples
        // to be meaningful — we do the first one here and the loop's refresh
        // will land at least one tick later.
        // 首次刷新建立基线；CPU 使用率需要两次采样，循环中的下一次刷新至少间隔一个周期。
        sys.refresh_all();
        let networks = Networks::new_with_refreshed_list();
        let disks = Disks::new_with_refreshed_list();
        Self {
            sys,
            disks,
            networks,
            last_net: None,
            last_net_per_nic: HashMap::new(),
            last_tick: None,
            interval: Duration::from_secs(interval_secs.max(1)),
            include_nics: include_nics.to_vec(),
            exclude_nics: exclude_nics.to_vec(),
            include_mounts: include_mounts.to_vec(),
            exclude_mounts: exclude_mounts.to_vec(),
        }
    }

    /// Refresh all metrics and return a snapshot. Called once per interval.
    /// 刷新全部指标并返回快照，每个采样间隔调用一次。
    pub fn capture(&mut self) -> SysSnapshot {
        self.sys.refresh_all();
        self.networks.refresh();
        self.disks.refresh();

        let now = Instant::now();
        let elapsed = self
            .last_tick
            .map(|t| now.duration_since(t))
            .unwrap_or(self.interval);
        let elapsed_secs = elapsed.as_secs_f64().max(0.001);

        // ==== CPU ====
        // CPU 使用率 / CPU usage
        let cpu_usage = self.sys.global_cpu_info().cpu_usage() as f64;
        let cpu_usage = crate::util::round2(cpu_usage);

        // ==== Memory ====
        // 内存 / Memory
        let mem_total = self.sys.total_memory();
        let mem_used = self.sys.used_memory();
        let swap_total = self.sys.total_swap();
        let swap_used = self.sys.used_swap();

        // ==== Load average ====
        // 平均负载 / Load average
        let load = System::load_average();

        // ==== Disk ====
        // Sum all physical disks; on multi-mount Linux this over-counts the
        // root filesystem if multiple partitions are mounted. Komari parity:
        // they pick the mount containing "/" (or "C:\" on Windows). We take
        // the "biggest" disk as a reasonable one-line summary.
        // 磁盘：对所有物理磁盘求和时，Linux 的多个挂载分区可能导致根文件系统重复计数。
        // Komari 选择包含 /（Windows 上为 C:\）的挂载点；此处以最大磁盘作为单行摘要。

        // ==== Network ====
        // 网络 / Network
        let (net_rx_total, net_tx_total) = self
            .networks
            .iter()
            .filter(|(name, _)| selected(name, &self.include_nics, &self.exclude_nics))
            .map(|(_, data)| data)
            .fold((0u64, 0u64), |(rx, tx), data| {
                (rx + data.total_received(), tx + data.total_transmitted())
            });

        let cur = NetCounters {
            rx: net_rx_total,
            tx: net_tx_total,
        };

        let (down_bps, up_bps) = match self.last_net {
            Some(prev) => {
                let drx = cur.rx.saturating_sub(prev.rx);
                let dtx = cur.tx.saturating_sub(prev.tx);
                (
                    (drx as f64 / elapsed_secs) as u64,
                    (dtx as f64 / elapsed_secs) as u64,
                )
            }
            None => (0, 0),
        };

        self.last_net = Some(cur);
        self.last_tick = Some(now);

        // ==== Per-NIC stats ====
        // sysinfo 0.30 semantics: `received()`/`transmitted()` are the bytes
        // *since the last refresh of this Networks object*, not rates. We
        // differentiate ourselves over `elapsed_secs` to keep interval math
        // consistent with the aggregate numbers above.
        // 按网卡统计：sysinfo 返回自上次刷新以来的字节数，按经过秒数计算速率以匹配总计。
        let mut interfaces: Vec<InterfaceStats> = Vec::new();
        let mut new_per_nic: HashMap<String, NetCounters> = HashMap::new();
        for (name, data) in self.networks.iter() {
            if !selected(name, &self.include_nics, &self.exclude_nics) {
                continue;
            }
            let cur = NetCounters {
                rx: data.total_received(),
                tx: data.total_transmitted(),
            };
            let (down_rate, up_rate) = match self.last_net_per_nic.get(name) {
                Some(prev) => {
                    let drx = cur.rx.saturating_sub(prev.rx);
                    let dtx = cur.tx.saturating_sub(prev.tx);
                    (
                        (drx as f64 / elapsed_secs) as u64,
                        (dtx as f64 / elapsed_secs) as u64,
                    )
                }
                None => (0, 0),
            };
            new_per_nic.insert(name.clone(), cur);
            interfaces.push(InterfaceStats {
                name: name.clone(),
                up_rate,
                down_rate,
                total_up_bytes: cur.tx,
                total_down_bytes: cur.rx,
            });
        }
        self.last_net_per_nic = new_per_nic;

        // ==== Per-mount disk stats ====
        // 按挂载点统计磁盘 / Per-mount disk statistics
        let mut mounts: Vec<MountStats> = Vec::new();
        for d in self.disks.iter() {
            let name = d.mount_point().to_string_lossy().into_owned();
            if !selected(&name, &self.include_mounts, &self.exclude_mounts) {
                continue;
            }
            let total = d.total_space();
            if total == 0 {
                continue;
            }
            let free = d.available_space();
            let used = total.saturating_sub(free);
            let used_percent = crate::util::round2((used as f64 / total as f64) * 100.0);
            mounts.push(MountStats {
                name,
                total_bytes: total,
                used_bytes: used,
                free_bytes: free,
                used_percent,
            });
        }

        let (disk_total, disk_used) = mounts
            .iter()
            .max_by(|a, b| {
                (a.used_bytes as f64 / a.total_bytes as f64)
                    .total_cmp(&(b.used_bytes as f64 / b.total_bytes as f64))
            })
            .map(|m| (m.total_bytes, m.used_bytes))
            .unwrap_or((0, 0));

        // ==== Connections (tcp/udp) — sysinfo 0.30 不提供,返回 0 占位 ====
        // 真实实现可以读 /proc/net/{tcp,udp} 或用 `netstat2`,但我们保持
        // 跨平台一致性:不支持就返回 0,协议字段仍存在。
        // Connections: sysinfo 0.30 does not supply TCP/UDP counts. Return zero where unsupported
        // for cross-platform consistency, while retaining the protocol fields.
        let (tcp_conns, udp_conns) = count_connections();

        // ==== Uptime ====
        // 运行时间 / Uptime
        let uptime_secs = System::uptime();

        // ==== Process count ====
        // 进程数 / Process count
        let process_count = self.sys.processes().len() as u64;

        SysSnapshot {
            cpu_usage,
            mem_total,
            mem_used,
            swap_total,
            swap_used,
            load1: crate::util::round2(load.one),
            load5: crate::util::round2(load.five),
            load15: crate::util::round2(load.fifteen),
            disk_total,
            disk_used,
            net_up_bps: up_bps,
            net_down_bps: down_bps,
            net_total_up: net_tx_total,
            net_total_down: net_rx_total,
            tcp_conns,
            udp_conns,
            uptime_secs,
            process_count,
            message: String::new(),
            interfaces,
            mounts,
        }
    }
}

fn selected(name: &str, include: &[String], exclude: &[String]) -> bool {
    let included =
        include.is_empty() || include.iter().any(|pattern| matches_filter(name, pattern));
    included && !exclude.iter().any(|pattern| matches_filter(name, pattern))
}

#[cfg(test)]
mod filter_tests {
    use super::selected;

    #[test]
    fn include_and_exclude_filters_scope_metrics() {
        let include = vec!["eth*".to_owned()];
        let exclude = vec!["eth-guest".to_owned()];
        assert!(selected("eth0", &include, &exclude));
        assert!(!selected("eth-guest", &include, &exclude));
        assert!(!selected("wlan0", &include, &exclude));
        assert!(selected("/", &[], &[]));
    }
}

fn matches_filter(value: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    match pattern.strip_prefix('*').and_then(|p| p.strip_suffix('*')) {
        Some(part) => value.contains(part),
        None if pattern.starts_with('*') => value.ends_with(&pattern[1..]),
        None if pattern.ends_with('*') => value.starts_with(&pattern[..pattern.len() - 1]),
        None => value == pattern,
    }
}

#[allow(dead_code)]
fn pick_primary_disk(disks: &Disks) -> (u64, u64) {
    let mut total = 0u64;
    let mut used = 0u64;
    let mut biggest: Option<(u64, u64)> = None;

    for d in disks.iter() {
        let t = d.total_space();
        let avail = d.available_space();
        if t == 0 {
            continue;
        }
        let u = t.saturating_sub(avail);
        if biggest.map(|(bt, _)| t > bt).unwrap_or(true) {
            biggest = Some((t, u));
        }
        total = total.saturating_add(t);
        used = used.saturating_add(u);
    }

    // 返回最大一块盘的容量,而不是求和,更接近 Komari 单盘语义
    // Return the largest disk rather than the sum, closer to Komari's single-disk semantics.
    biggest.unwrap_or((total, used))
}

#[cfg(target_os = "linux")]
fn count_connections() -> (u64, u64) {
    // 尝试数 /proc/net/tcp(6) + /proc/net/udp(6) 的行数
    // Try counting entries in /proc/net/tcp(6) and /proc/net/udp(6).
    fn count_lines(path: &str) -> u64 {
        std::fs::read_to_string(path)
            .ok()
            .map(|s| s.lines().skip(1).count() as u64) // skip header / 跳过表头
            .unwrap_or(0)
    }
    let tcp = count_lines("/proc/net/tcp") + count_lines("/proc/net/tcp6");
    let udp = count_lines("/proc/net/udp") + count_lines("/proc/net/udp6");
    (tcp, udp)
}

#[cfg(not(target_os = "linux"))]
fn count_connections() -> (u64, u64) {
    (0, 0)
}
