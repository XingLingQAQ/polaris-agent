use std::net::IpAddr;
#[cfg(target_os = "linux")]
use std::net::{Ipv4Addr, UdpSocket};

// 剔除本地与基准测试地址段，避免把 fake-IP 当作公网身份。
// Reject local and benchmark address ranges so fake-IP addresses are not reported as public identities.
pub(super) fn public_address(value: &str) -> Option<IpAddr> {
    let address: IpAddr = value.trim().parse().ok()?;
    let usable = match address {
        IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !(ip.octets()[0] == 198 && matches!(ip.octets()[1], 18 | 19))
        }
        IpAddr::V6(ip) => {
            !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && ip.segments()[0] & 0xfe00 != 0xfc00
                && ip.segments()[0] & 0xffc0 != 0xfe80
        }
    };
    usable.then_some(address)
}

#[cfg(any(not(target_os = "windows"), test))]
pub(super) fn virtual_interface(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "tun",
        "tap",
        "clash",
        "mihomo",
        "tailscale",
        "wireguard",
        "vpn",
        "utun",
    ]
    .iter()
    .any(|marker| name.contains(marker))
        || name.starts_with("wg")
}

#[cfg(target_os = "windows")]
pub(super) fn physical_address() -> Option<IpAddr> {
    use std::os::windows::process::CommandExt;
    let script = r#"
$ErrorActionPreference = 'Stop'
foreach ($adapter in [System.Net.NetworkInformation.NetworkInterface]::GetAllNetworkInterfaces()) {
    if ($adapter.OperationalStatus -ne 'Up') { continue }
    if ($adapter.NetworkInterfaceType -notin 'Ethernet','Wireless80211') { continue }
    if (($adapter.Name + ' ' + $adapter.Description) -match '(?i)tun|tap|clash|mihomo|tailscale|wireguard|vpn') { continue }
    $properties = $adapter.GetIPProperties()
    if (-not ($properties.GatewayAddresses | Where-Object { $_.Address.ToString() -notin '0.0.0.0','::' })) { continue }
    foreach ($unicast in $properties.UnicastAddresses) {
        $ip = $unicast.Address
        if ($ip.AddressFamily -eq 'InterNetwork' -and $ip.ToString() -notmatch '^(127\.|169\.254\.|198\.(18|19)\.)') {
            [Console]::WriteLine($ip.ToString())
            exit 0
        }
    }
    foreach ($unicast in $properties.UnicastAddresses) {
        $ip = $unicast.Address
        if ($ip.AddressFamily -eq 'InterNetworkV6' -and -not $ip.IsIPv6LinkLocal -and -not $ip.IsIPv6SiteLocal -and -not $ip.IsIPv6Multicast -and $ip.ToString() -notmatch '^(::|::1|f[c-d])$|^f[c-d]') {
            [Console]::WriteLine($ip.ToString())
            exit 0
        }
    }
}
"#;
    let output = std::process::Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
        ])
        .creation_flags(0x08000000)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

#[cfg(target_os = "linux")]
pub(super) fn physical_address() -> Option<IpAddr> {
    let routes = std::fs::read_to_string("/proc/net/route").ok()?;
    let mut candidates = routes
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 8 || fields[1] != "00000000" || virtual_interface(fields[0]) {
                return None;
            }
            let gateway = Ipv4Addr::from(u32::from_str_radix(fields[2], 16).ok()?.to_le_bytes());
            let flags = u32::from_str_radix(fields[3], 16).ok()?;
            if gateway.is_unspecified() || flags & 3 != 3 {
                return None;
            }
            Some((fields[6].parse::<u32>().ok()?, gateway))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(metric, _)| *metric);
    for (_, gateway) in candidates {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
        if socket.connect((gateway, 9)).is_ok() {
            let address = socket.local_addr().ok()?.ip();
            if !address.is_loopback() && !address.is_unspecified() {
                return Some(address);
            }
        }
    }
    let routes = std::fs::read_to_string("/proc/net/ipv6_route").ok()?;
    let mut candidates = routes
        .lines()
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 10
                || fields[0] != "00000000000000000000000000000000"
                || fields[1] != "00"
                || virtual_interface(fields[9])
            {
                return None;
            }
            let bytes = hex::decode(fields[4]).ok()?;
            let gateway = std::net::Ipv6Addr::from(<[u8; 16]>::try_from(bytes).ok()?);
            if gateway.is_unspecified() {
                return None;
            }
            let index = std::fs::read_to_string(format!("/sys/class/net/{}/ifindex", fields[9]))
                .ok()?
                .trim()
                .parse::<u32>()
                .ok()?;
            Some((u32::from_str_radix(fields[5], 16).ok()?, gateway, index))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(metric, _, _)| *metric);
    for (_, gateway, index) in candidates {
        let socket = UdpSocket::bind((std::net::Ipv6Addr::UNSPECIFIED, 0)).ok()?;
        if socket
            .connect(std::net::SocketAddrV6::new(gateway, 9, 0, index))
            .is_ok()
        {
            let address = socket.local_addr().ok()?.ip();
            if public_address(&address.to_string()).is_some() {
                return Some(address);
            }
        }
    }
    None
}

#[cfg(target_os = "macos")]
pub(super) fn physical_address() -> Option<IpAddr> {
    let output = std::process::Command::new("/sbin/route")
        .args(["-n", "get", "default"])
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    let interface = text
        .lines()
        .find_map(|line| line.trim().strip_prefix("interface:").map(str::trim))?;
    if virtual_interface(interface) {
        return None;
    }
    let address = std::process::Command::new("/usr/sbin/ipconfig")
        .args(["getifaddr", interface])
        .output()
        .ok()?;
    if !address.status.success() {
        return None;
    }
    String::from_utf8(address.stdout).ok()?.trim().parse().ok()
}

#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
pub(super) fn physical_address() -> Option<IpAddr> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_local_and_fake_public_addresses() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.0.1",
            "198.18.0.2",
            "198.19.1.1",
            "::1",
            "fe80::1",
            "fd00::1",
        ] {
            assert!(public_address(address).is_none(), "{address}");
        }
        assert!(public_address("1.1.1.1").is_some());
        assert!(public_address("2606:4700:4700::1111").is_some());
    }

    #[test]
    fn excludes_tunnel_adapters_without_excluding_vm_network_cards() {
        for name in ["Mihomo", "Meta Tunnel", "Tailscale", "utun3", "tun0", "wg0"] {
            assert!(virtual_interface(name));
        }
        for name in ["Ethernet", "Red Hat VirtIO Ethernet Adapter", "eth0", "en0"] {
            assert!(!virtual_interface(name));
        }
    }
}
