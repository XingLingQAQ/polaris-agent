//! One-shot host extras sent in `basicReport`: public IP, ISP, region,
//! virtualization type.
//!
//! Why in `basicReport` instead of per-second `report`:
//!   - public WAN IP changes slowly (typical DHCP lease is hours-to-days)
//!   - virtualization type never changes
//!   - the probe itself is an outbound HTTPS call — doing it every tick
//!     would be pointless traffic
//!
//! Failure modes are deliberately silent: if all resolvers fail we just
//! send `null` / `"unknown"` fields. The server treats missing/null as
//! "agent has no info", and the agent still reports fine.
//!
//! 在 basicReport 中一次性发送公网 IP、ISP、地区和虚拟化类型，而非每秒 report：
//! 公网 IP 变化缓慢（常见 DHCP 租期为数小时至数天），虚拟化类型不会改变，探测本身还需要
//! 出站 HTTPS 请求，每周期执行只会产生无意义的流量。
//! 所有解析服务失败时有意静默发送 null 或 unknown；服务器将缺失值视为 Agent 无信息，正常上报仍可继续。

use super::network_route::{physical_address, public_address};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

/// Public-IP / geographic / virtualization bundle.
/// 公网 IP、地理位置和虚拟化信息。
#[derive(Debug, Clone, Default)]
pub struct AgentExtras {
    pub public_ip: Option<String>,
    pub public_ipv6: Option<String>,
    pub isp: Option<String>,
    pub region: Option<String>,
    /// "physical" | "kvm" | "vmware" | "virtualbox" | "hyperv" | "xen"
    /// | "docker" | "unknown". Always present.
    /// 虚拟化类型，始终存在；取值见上方列表。
    pub virtualization: String,
}

impl AgentExtras {
    pub fn fallback() -> Self {
        Self {
            public_ip: None,
            public_ipv6: None,
            isp: None,
            region: None,
            virtualization: detect_virtualization(),
        }
    }
}

const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// Network identity follows the physical route; virtualization is local. / 网络身份使用真实网卡出口，虚拟化信息从本机读取。
pub async fn collect() -> AgentExtras {
    let virtualization = detect_virtualization();
    let source = tokio::task::spawn_blocking(physical_address)
        .await
        .unwrap_or_default();
    let mut net = NetExtras::default();
    if let Some(source) = source {
        let _ =
            tokio::time::timeout(Duration::from_secs(20), collect_network(source, &mut net)).await;
    }

    AgentExtras {
        public_ip: net.public_ip,
        public_ipv6: net.public_ipv6,
        isp: net.isp,
        region: net.region,
        virtualization,
    }
}

#[derive(Default)]
struct NetExtras {
    public_ip: Option<String>,
    public_ipv6: Option<String>,
    isp: Option<String>,
    region: Option<String>,
}

fn http_client() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(HTTP_TIMEOUT)
        .connect_timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("polaris-agent/", env!("CARGO_PKG_VERSION")))
}

async fn response_bytes(mut response: reqwest::Response) -> Option<Vec<u8>> {
    if !response.status().is_success() {
        return None;
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len() + chunk.len() > 64 * 1024 {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    Some(bytes)
}

async fn query_json(client: &reqwest::Client, url: &str) -> Option<serde_json::Value> {
    let response = client.get(url).send().await.ok()?;
    serde_json::from_slice(&response_bytes(response).await?).ok()
}

async fn real_addresses(client: &reqwest::Client, host: &str, source: IpAddr) -> Vec<SocketAddr> {
    let matches_family = |ip: IpAddr| ip.is_ipv4() == source.is_ipv4();
    if let Ok(Ok(addresses)) =
        tokio::time::timeout(Duration::from_secs(3), tokio::net::lookup_host((host, 443))).await
    {
        let usable = addresses
            .filter(|address| {
                matches_family(address.ip()) && public_address(&address.ip().to_string()).is_some()
            })
            .collect::<Vec<_>>();
        if !usable.is_empty() {
            return usable;
        }
    }
    // Fake-IP 不能交给真实网卡；DNS 请求的出口不作为机器身份。 / Never send Fake-IP to the physical route; DNS egress is not host identity.
    let record_type = if source.is_ipv4() { "A" } else { "AAAA" };
    for resolver in [
        "https://dns.google/resolve",
        "https://cloudflare-dns.com/dns-query",
    ] {
        let response = client
            .get(resolver)
            .header("accept", "application/dns-json")
            .query(&[("name", host), ("type", record_type)])
            .send()
            .await;
        let Some(bytes) = (match response {
            Ok(response) => response_bytes(response).await,
            Err(_) => None,
        }) else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            continue;
        };
        if value.get("Status").and_then(serde_json::Value::as_u64) != Some(0) {
            continue;
        }
        let addresses = value
            .get("Answer")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|answer| public_address(answer.get("data")?.as_str()?))
            .filter(|ip| matches_family(*ip))
            .map(|ip| SocketAddr::new(ip, 443))
            .collect::<Vec<_>>();
        if !addresses.is_empty() {
            return addresses;
        }
    }
    Vec::new()
}

async fn collect_network(source: IpAddr, out: &mut NetExtras) {
    let Ok(resolver) = http_client().build() else {
        return;
    };
    for (host, path) in [
        ("myip.ipip.net", "/json"),
        ("api64.ipify.org", "/?format=json"),
        ("ifconfig.me", "/ip"),
    ] {
        let addresses = real_addresses(&resolver, host, source).await;
        if addresses.is_empty() {
            continue;
        }
        let Ok(direct) = http_client()
            .local_address(source)
            .resolve_to_addrs(host, &addresses)
            .build()
        else {
            continue;
        };
        let Ok(response) = direct.get(format!("https://{host}{path}")).send().await else {
            continue;
        };
        let Some(bytes) = response_bytes(response).await else {
            continue;
        };
        let value = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
        let ip = value
            .as_ref()
            .and_then(|value| value.get("ip").or_else(|| value.pointer("/data/ip")))
            .and_then(serde_json::Value::as_str)
            .or_else(|| std::str::from_utf8(&bytes).ok());
        if let Some(address) = ip.and_then(public_address) {
            apply_public_ip(&address.to_string(), out);
            break;
        }
    }
    let Some(address) = out
        .public_ip
        .as_deref()
        .or(out.public_ipv6.as_deref())
        .and_then(public_address)
    else {
        return;
    };
    // 明确查询已发现的 IP，查询服务本身可经 TUN 访问。 / Query the discovered IP explicitly; the geographic lookup may traverse TUN.
    if let Some(value) = query_json(&resolver, &format!("https://ipwho.is/{address}")).await {
        if value.get("success").and_then(serde_json::Value::as_bool) == Some(true) {
            apply_geo_for(
                address,
                &serde_json::json!({
                    "ip": value.get("ip"), "city": value.get("city"),
                    "region": value.get("region"), "country": value.get("country_code"),
                    "org": value.pointer("/connection/isp"),
                }),
                out,
            );
        }
    }
    if out.region.is_none() || out.isp.is_none() {
        if let Some(value) =
            query_json(&resolver, &format!("https://ipinfo.io/{address}/json")).await
        {
            apply_geo_for(address, &value, out);
        }
    }
}

fn apply_geo_for(address: IpAddr, value: &serde_json::Value, out: &mut NetExtras) {
    if value
        .get("ip")
        .and_then(serde_json::Value::as_str)
        .and_then(public_address)
        == Some(address)
    {
        apply_ipinfo(value, out);
    }
}

fn apply_public_ip(ip: &str, out: &mut NetExtras) {
    match ip.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(value)) => {
            out.public_ip.get_or_insert_with(|| value.to_string());
        }
        Ok(std::net::IpAddr::V6(value)) => {
            out.public_ipv6.get_or_insert_with(|| value.to_string());
        }
        Err(_) => {}
    }
}

fn apply_ipinfo(v: &serde_json::Value, out: &mut NetExtras) {
    if let Some(ip) = v.get("ip").and_then(|s| s.as_str()) {
        apply_public_ip(ip, out);
    }
    if out.isp.is_none() {
        // ipinfo "org" is "AS12345 Comcast Corporation" — strip the ASN prefix.
        // ipinfo 的 org 包含 ASN 前缀；将其移除。
        if let Some(org) = v.get("org").and_then(|s| s.as_str()) {
            let cleaned = org
                .split_once(' ')
                .filter(|(prefix, _)| {
                    prefix.strip_prefix("AS").is_some_and(|value| {
                        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                    })
                })
                .map(|(_, rest)| rest)
                .unwrap_or(org)
                .trim()
                .to_string();
            if !cleaned.is_empty() {
                out.isp = Some(cleaned);
            }
        }
    }
    if out.region.is_none() {
        let city = v.get("city").and_then(|s| s.as_str()).unwrap_or("").trim();
        let region = v
            .get("region")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .trim();
        let country = v
            .get("country")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        let country = if country.len() == 2 && country.bytes().all(|byte| byte.is_ascii_uppercase())
        {
            country
        } else {
            String::new()
        };
        let assembled = if !city.is_empty() && !country.is_empty() {
            format!("{}, {}", city, country)
        } else if !region.is_empty() && !country.is_empty() {
            format!("{}, {}", region, country)
        } else {
            country
        };
        if !assembled.is_empty() {
            out.region = Some(assembled);
        }
    }
}

// ---------------------------------------------------------------------------
// Virtualization detection
// 虚拟化检测 / Virtualization detection
// ---------------------------------------------------------------------------

/// Returns one of "kvm" | "vmware" | "virtualbox" | "hyperv" | "xen"
/// | "docker" | "physical" | "unknown".
///
/// "physical" is the affirmative answer when we successfully read vendor
/// strings and none matched any hypervisor signature. "unknown" is used
/// when we couldn't gather any signal at all.
/// 返回上方列出的 kvm、vmware、virtualbox、hyperv、xen、docker、physical 或 unknown。
/// 成功读取厂商信息但未匹配到虚拟机特征时明确返回 physical；完全无法获取信号时返回 unknown。
fn detect_virtualization() -> String {
    detect_virtualization_platform().unwrap_or_else(|| "unknown".to_string())
}

#[cfg(target_os = "linux")]
fn detect_virtualization_platform() -> Option<String> {
    // Container fast-path: PID 1 in a container usually leaves a trace here.
    // 容器快速检测：容器中的 PID 1 通常会在此留下痕迹。
    if let Ok(s) = std::fs::read_to_string("/proc/1/cgroup") {
        let l = s.to_ascii_lowercase();
        if l.contains("docker") || l.contains("kubepods") || l.contains("containerd") {
            return Some("docker".to_string());
        }
    }
    if std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists()
    {
        return Some("docker".to_string());
    }

    let vendor = std::fs::read_to_string("/sys/class/dmi/id/sys_vendor").ok();
    let product = std::fs::read_to_string("/sys/class/dmi/id/product_name").ok();

    if vendor.is_none() && product.is_none() {
        return None; // no DMI info at all → unknown / 无 DMI 信息，返回 unknown
    }

    let hay = format!(
        "{}\n{}",
        vendor.unwrap_or_default(),
        product.unwrap_or_default()
    );
    Some(classify_dmi(&hay))
}

#[cfg(target_os = "windows")]
fn detect_virtualization_platform() -> Option<String> {
    use winreg::enums::HKEY_LOCAL_MACHINE;
    use winreg::RegKey;

    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    let key = hklm
        .open_subkey(r"SYSTEM\CurrentControlSet\Control\SystemInformation")
        .ok()?;
    let manufacturer: String = key.get_value("SystemManufacturer").unwrap_or_default();
    let product: String = key.get_value("SystemProductName").unwrap_or_default();
    if manufacturer.is_empty() && product.is_empty() {
        return None;
    }
    let hay = format!("{}\n{}", manufacturer, product);
    Some(classify_dmi(&hay))
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn detect_virtualization_platform() -> Option<String> {
    // macOS / others: no cheap probe, return unknown rather than wrong.
    // macOS 及其他平台没有低成本检测方式，返回 unknown。
    None
}

/// Match DMI vendor/product strings against known hypervisor signatures.
/// Case-insensitive because vendors are sloppy with casing.
/// 不区分大小写地匹配 DMI 厂商和产品信息与已知虚拟化特征。
fn classify_dmi(haystack: &str) -> String {
    let h = haystack.to_ascii_lowercase();

    // Order matters: "Microsoft Corporation" ships both Hyper-V and
    // physical Surface hardware — the Hyper-V string always appears with
    // "Virtual Machine" in the product name, so check for that pair.
    // 顺序很重要：Microsoft 同时生产 Hyper-V 和实体 Surface，须结合产品名 Virtual Machine 判断。
    if h.contains("hyper-v") || (h.contains("microsoft") && h.contains("virtual machine")) {
        return "hyperv".to_string();
    }
    if h.contains("vmware") {
        return "vmware".to_string();
    }
    if h.contains("virtualbox") || h.contains("innotek") {
        return "virtualbox".to_string();
    }
    if h.contains("kvm") || h.contains("qemu") {
        return "kvm".to_string();
    }
    if h.contains("xen") {
        return "xen".to_string();
    }
    if h.contains("docker") || h.contains("kata") {
        return "docker".to_string();
    }
    // Amazon EC2, Google Compute Engine, OpenStack, Alibaba Cloud all use
    // KVM under the hood but their DMI strings don't always say so — leave
    // those as "physical" rather than guessing.
    // Amazon EC2、Google Compute Engine、OpenStack 和 Alibaba Cloud 底层均使用 KVM，
    // 但其 DMI 字符串未必体现；不作猜测，保留 physical。
    "physical".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geographic_results_must_match_the_discovered_ip() {
        let mut extras = NetExtras::default();
        let address = "192.0.2.1".parse().unwrap();
        apply_public_ip("192.0.2.1", &mut extras);
        apply_geo_for(
            address,
            &serde_json::json!({"ip":"203.0.113.1","country":"US","city":"Proxy"}),
            &mut extras,
        );
        assert!(extras.region.is_none());
        apply_geo_for(
            address,
            &serde_json::json!({"ip":"192.0.2.1","country":"CN","city":"Shanghai"}),
            &mut extras,
        );
        assert_eq!(extras.region.as_deref(), Some("Shanghai, CN"));
    }

    #[tokio::test]
    async fn direct_client_ignores_proxy_and_preserves_resolved_host() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, source) = listener.accept().await.unwrap();
            assert!(source.ip().is_loopback());
            let mut bytes = [0u8; 4096];
            let length = stream.read(&mut bytes).await.unwrap();
            let request = String::from_utf8_lossy(&bytes[..length]);
            assert!(request
                .to_ascii_lowercase()
                .contains("host: identity.fixture:"));
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\nConnection: close\r\n\r\n{\"ip\":\"192.0.2.1\"}").await.unwrap();
        });
        let client = http_client()
            .local_address("127.0.0.1".parse::<IpAddr>().unwrap())
            .resolve("identity.fixture", address)
            .build()
            .unwrap();
        let value = query_json(
            &client,
            &format!("http://identity.fixture:{}/", address.port()),
        )
        .await
        .unwrap();
        assert_eq!(value["ip"], "192.0.2.1");
        server.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses public IP discovery services"]
    async fn live_physical_network_identity() {
        let extras = collect().await;
        assert!(extras.public_ip.is_some() || extras.public_ipv6.is_some());
        assert!(extras.region.is_some());
        println!(
            "physical route country: {}",
            extras
                .region
                .as_deref()
                .unwrap()
                .rsplit(", ")
                .next()
                .unwrap()
        );
    }

    #[test]
    fn geo_keeps_country_without_city_and_preserves_provider_names() {
        let mut extras = NetExtras::default();
        apply_ipinfo(
            &serde_json::json!({"ip":"2001:db8::1","country":" sg ","org":"Example Network"}),
            &mut extras,
        );
        assert_eq!(extras.region.as_deref(), Some("SG"));
        assert_eq!(extras.public_ipv6.as_deref(), Some("2001:db8::1"));
        assert_eq!(extras.isp.as_deref(), Some("Example Network"));
        let mut city = NetExtras::default();
        apply_ipinfo(
            &serde_json::json!({"ip":"192.0.2.1","country":"CN","city":"Shanghai","org":"AS64500 Example Network"}),
            &mut city,
        );
        assert_eq!(city.region.as_deref(), Some("Shanghai, CN"));
        assert_eq!(city.isp.as_deref(), Some("Example Network"));
        let mut invalid = NetExtras::default();
        apply_ipinfo(
            &serde_json::json!({"ip":"rate limit exceeded","country":"unknown"}),
            &mut invalid,
        );
        assert!(invalid.public_ip.is_none() && invalid.region.is_none());
    }

    #[test]
    fn classify_hyperv() {
        assert_eq!(
            classify_dmi("Microsoft Corporation\nVirtual Machine"),
            "hyperv"
        );
    }

    #[test]
    fn classify_vmware() {
        assert_eq!(
            classify_dmi("VMware, Inc.\nVMware Virtual Platform"),
            "vmware"
        );
    }

    #[test]
    fn classify_kvm_via_qemu() {
        assert_eq!(
            classify_dmi("QEMU\nStandard PC (i440FX + PIIX, 1996)"),
            "kvm"
        );
    }

    #[test]
    fn classify_kvm_via_kvm_string() {
        assert_eq!(classify_dmi("Red Hat\nKVM"), "kvm");
    }

    #[test]
    fn classify_virtualbox() {
        assert_eq!(classify_dmi("innotek GmbH\nVirtualBox"), "virtualbox");
    }

    #[test]
    fn classify_physical_dell() {
        assert_eq!(classify_dmi("Dell Inc.\nPowerEdge R640"), "physical");
    }

    #[test]
    fn classify_case_insensitive() {
        assert_eq!(classify_dmi("VMWARE, INC.\nVMWARE7,1"), "vmware");
    }
}
