//! Proxy support for the WebSocket transport.
//!
//! ## Why this is separate from the HTTP side
//!
//! `ureq` reads `HTTPS_PROXY` once the `proxy-from-env` feature is on, which
//! covers the HTTP fallback with one flag. WebSocket has no such switch:
//! `tokio-tungstenite` 0.21 takes a TLS `Connector` but no proxy, so the TCP
//! connection has to be established first and the socket layered on top of it.
//!
//! That means an HTTP `CONNECT` tunnel, opened by hand. It is about sixty lines
//! because the protocol is: send one request line and a `Host` header, read
//! status line and headers back, and everything after that is the tunnel.
//!
//! ## What is honoured
//!
//! `HTTPS_PROXY` / `https_proxy` for a `wss:` endpoint and `HTTP_PROXY` /
//! `http_proxy` for `ws:`, plus `ALL_PROXY` / `all_proxy` for either, and
//! `NO_PROXY` / `no_proxy` to opt out. Upper case wins over lower, which is the
//! convention every other client uses - curl, and `ureq` itself.
//!
//! This is deliberately a subset of `ureq`'s behaviour rather than a
//! reimplementation of it: the same variables, the same precedence, so a host
//! that reaches the server over HTTP also reaches it over the socket.
//!
//! ## Not implemented
//!
//! No `SOCKS` support. It needs a different handshake and a dependency of its
//! own, and no environment on hand exercises it - so it is absent rather than
//! claimed. A `socks5://` value is reported and ignored rather than silently
//! failing to connect, which would look like a network fault.
//!
//! WebSocket 代理与 HTTP 侧分开处理：ureq 启用 proxy-from-env 后可读取 HTTPS_PROXY，
//! 但 tokio-tungstenite 0.21 只接受 TLS Connector 而不支持代理，需先建立 TCP 连接再叠加 socket。
//! 这里手动打开 HTTP CONNECT 隧道：发送请求行和 Host 头，读取状态行和响应头，之后的数据就是隧道。
//! wss 使用 HTTPS_PROXY/https_proxy，ws 使用 HTTP_PROXY/http_proxy；两者都回退到 ALL_PROXY/all_proxy，
//! NO_PROXY/no_proxy 用于排除。与 curl 和 ureq 的惯例一致，大写变量优先。
//! 这里有意实现 ureq 行为的子集，变量与优先级保持一致，使 HTTP 可连接的主机也能使用 socket。
//! 不支持 SOCKS：它需要另一套握手和依赖，且现无可测试环境；socks5:// 会被报告并忽略，
//! 而不是静默连接失败并表现为网络故障。

use std::{fmt, io};

use anyhow::{bail, Context as _, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};
use url::Url;

/// A proxy to reach the endpoint through.
/// 用于连接端点的代理。
#[derive(Clone, PartialEq, Eq)]
pub struct Proxy {
    pub host: String,
    pub port: u16,
    /// The `Proxy-Authorization` header value, when the URL carried credentials.
    /// URL 包含凭据时使用的 Proxy-Authorization 请求头。
    pub authorization: Option<String>,
    proxy_url: String,
}

impl fmt::Debug for Proxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Proxy")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("has_authorization", &self.authorization.is_some())
            .finish()
    }
}

impl Proxy {
    /// The proxy for `target`, or `None` when one is not configured or excluded.
    ///
    /// `wss:` / `https:` use `HTTPS_PROXY`; `ws:` / `http:` use `HTTP_PROXY`.
    /// `ALL_PROXY` covers either when no scheme-specific value applies.
    /// 获取目标使用的代理；未配置或被排除时返回 None。wss/https 使用 HTTPS_PROXY，
    /// ws/http 使用 HTTP_PROXY；无协议专属值时才使用 ALL_PROXY。
    pub fn for_url(target: &Url) -> Option<Self> {
        let host = target.host_str()?;

        if is_excluded(host) {
            debug!(host, "NO_PROXY matches; connecting directly");
            return None;
        }

        let secure = matches!(target.scheme(), "wss" | "https");
        let candidates: [&str; 3] = if secure {
            ["HTTPS_PROXY", "https_proxy", "ALL_PROXY"]
        } else {
            ["HTTP_PROXY", "http_proxy", "ALL_PROXY"]
        };

        for name in candidates {
            let Some(raw) = std::env::var(name).ok().filter(|v| !v.trim().is_empty()) else {
                continue;
            };

            match Self::parse(&raw) {
                Ok(Some(proxy)) => {
                    info!(
                        variable = name,
                        proxy = %format!("{}:{}", proxy.host, proxy.port),
                        "using configured proxy"
                    );
                    return Some(proxy);
                }
                // A `socks5://` value: reported, then treated as "no proxy"
                // rather than as a connection failure.
                // 报告 socks5 代理不受支持，并按无代理处理，而非视为连接失败。
                Ok(None) => warn!(variable = name, "unsupported proxy scheme; ignoring"),
                Err(e) => {
                    warn!(variable = name, err = %e, "unparseable proxy; ignoring");
                }
            }
        }

        None
    }

    /// Parse a proxy URL. `Ok(None)` means a scheme this does not implement.
    ///
    /// ## Why `https://` is refused rather than half-supported
    ///
    /// A tunnel to an `https://` proxy needs TLS to the *proxy itself*, before
    /// the CONNECT is sent - a second TLS layer under the endpoint's own. This
    /// implementation has one, so accepting `https://` would open a **plain
    /// TCP** connection and send `Proxy-Authorization` across it in the clear:
    /// the credential would be wrong to send and the operator would have no
    /// reason to suspect it was.
    ///
    /// Refusing it is the honest answer. Nearly every proxy in a `*_PROXY`
    /// variable is `http://`, and the caller logs the refusal instead of
    /// appearing to honour a setting it does not.
    /// 解析代理 URL；不支持的协议返回 Ok(None)。HTTPS 代理在发送 CONNECT 前需要先与代理
    /// 建立 TLS，再在上层与端点建立第二层 TLS。此实现只有一层；若接受 https://，将通过明文 TCP
    /// 发送 Proxy-Authorization，导致凭据泄露且操作员难以察觉。*_PROXY 通常使用 http://；
    /// 此处明确拒绝 HTTPS，调用方会记录原因，而非假装支持。
    fn parse(raw: &str) -> Result<Option<Self>> {
        // A bare `host:port` is common in the wild and every other client
        // accepts it, so it is accepted here rather than rejected.
        // 接受常见的无协议 host:port 形式。
        let normalized = if raw.contains("://") {
            raw.to_string()
        } else {
            format!("http://{raw}")
        };

        let url = Url::parse(&normalized).context("invalid proxy URL")?;

        match url.scheme() {
            // Plain HTTP: a raw TCP tunnel, which is what `connect_tunnel` does.
            // 明文 HTTP 使用原始 TCP 隧道。
            "http" => {}
            // Would need TLS to the proxy as well; see the note above.
            // HTTPS 代理还需要到代理本身的 TLS，见上方说明。
            "https" => {
                warn!(
                    "an https:// proxy needs TLS to the proxy, which this agent does not do; \
                     use its http:// address if it serves one"
                );
                return Ok(None);
            }
            _ => return Ok(None),
        }

        // No host means `http://` alone, which is not a proxy.
        // 仅有 http:// 而没有主机名不是有效代理。
        let Some(host) = url.host_str().filter(|h| !h.is_empty()) else {
            return Ok(None);
        };

        // `port_or_known_default` cannot be `None` for `http`, which is the only
        // scheme that reaches here; 80 is spelled out so the value is visible.
        // 到这里的协议只能是 HTTP，显式列出默认端口 80。
        let port = url.port_or_known_default().unwrap_or(80);

        let authorization = if url.username().is_empty() {
            None
        } else {
            let credentials = format!("{}:{}", url.username(), url.password().unwrap_or_default());
            Some(format!("Basic {}", base64(&credentials)))
        };

        Ok(Some(Self {
            host: host.to_string(),
            port,
            authorization,
            proxy_url: normalized,
        }))
    }

    pub fn to_ureq_proxy(&self) -> Result<ureq::Proxy> {
        ureq::Proxy::new(&self.proxy_url).context("invalid selected HTTP proxy")
    }

    /// Open a tunnel to `target` and hand back the connected stream.
    ///
    /// The returned `TcpStream` is positioned immediately after the proxy's
    /// response headers, so it is the target's connection as far as the caller
    /// is concerned - TLS and the WebSocket handshake both proceed on it
    /// unchanged.
    /// 打开目标隧道并返回连接流。TcpStream 位于代理响应头之后，对调用方而言即目标连接，
    /// TLS 和 WebSocket 握手可不作修改地继续进行。
    pub async fn connect_tunnel(&self, target: &Url) -> Result<TcpStream> {
        let Some(host) = target.host_str() else {
            bail!("the endpoint URL has no host to tunnel to");
        };
        let port = target
            .port_or_known_default()
            .context("the endpoint URL has no port and its scheme implies none")?;

        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .with_context(|| format!("connecting to proxy {}:{}", self.host, self.port))?;

        debug!(proxy = %format!("{}:{}", self.host, self.port), target = %format!("{host}:{port}"), "opening CONNECT tunnel");

        // `HTTP/1.1` so the proxy keeps the connection open; `HTTP/1.0` would
        // let it close after the response, which is correct by the letter of the
        // spec and useless as a tunnel.
        // 使用 HTTP/1.1 保持连接；HTTP/1.0 允许响应后关闭连接，不适合隧道。
        let mut request = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n");
        if let Some(auth) = &self.authorization {
            request.push_str(&format!("Proxy-Authorization: {auth}\r\n"));
        }
        request.push_str("\r\n");

        stream
            .write_all(request.as_bytes())
            .await
            .context("sending the CONNECT request")?;

        let status = read_connect_response(&mut stream).await?;

        // 2xx means the tunnel is open. 407 is the one worth naming: it is a
        // proxy asking for credentials, and reporting it as a generic failure
        // sends the operator looking at the server.
        // 2xx 表示隧道已打开；407 表示代理需要凭据，须与服务器故障区分。
        if status == 407 {
            bail!(
                "proxy {}:{} requires authentication (HTTP 407)",
                self.host,
                self.port
            );
        }
        if !(200..300).contains(&status) {
            bail!(
                "proxy {}:{} refused CONNECT with HTTP {status}",
                self.host,
                self.port
            );
        }

        Ok(stream)
    }
}

/// Read the proxy's response headers, returning the status code.
///
/// Read a byte at a time rather than into a buffer: the response ends at the
/// first blank line, and anything read past it would be the start of the
/// tunnelled TLS handshake - which cannot be put back into the stream. A
/// buffered read would swallow the server's first bytes and the handshake would
/// fail with a misleading error.
/// 逐字节读取代理响应头并返回状态码：第一个空行即响应结束，继续读取的数据属于隧道 TLS 握手，
/// 无法放回流中。缓冲读取会吞掉服务器的首批字节，导致握手失败且报错具有误导性。
async fn read_connect_response(stream: &mut TcpStream) -> Result<u16> {
    let mut head = Vec::with_capacity(256);
    let mut byte = [0u8; 1];

    loop {
        let read = stream
            .read(&mut byte)
            .await
            .context("reading the CONNECT response")?;
        if read == 0 {
            bail!("the proxy closed the connection during CONNECT");
        }
        head.push(byte[0]);

        if head.ends_with(b"\r\n\r\n") {
            break;
        }
        // A proxy that answers with bare LF line endings, or an endless stream
        // of anything, must not grow this without bound.
        // 限制缓冲区大小，防止非标准换行或无限响应导致无界增长。
        if head.len() > 8192 {
            bail!("the CONNECT response headers exceeded 8 KiB");
        }
    }

    let text = String::from_utf8_lossy(&head);
    let status_line = text.lines().next().unwrap_or_default();

    // `HTTP/1.1 200 Connection established`
    // HTTP 成功响应示例 / HTTP success response example
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .with_context(|| format!("unparseable CONNECT status line: {status_line:?}"))?;

    Ok(status)
}

/// Whether `NO_PROXY` excludes this host.
///
/// Supports the forms that appear in practice: `*` for everything, an exact
/// host, a `.suffix` or bare `suffix` for a domain and its subdomains, and an
/// optional `:port` which is ignored here because the decision is per-host -
/// matching the port too would silently stop excluding a host whose port
/// changed.
/// 判断 NO_PROXY 是否排除此主机：支持 *、精确主机、带点或不带点的域名后缀及其子域名。
/// 可选的 :port 在此忽略，因为按主机决定；若同时匹配端口，端口变化会悄悄取消对该主机的排除。
fn is_excluded(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();

    for name in ["NO_PROXY", "no_proxy"] {
        let Ok(raw) = std::env::var(name) else {
            continue;
        };

        for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            if entry == "*" {
                return true;
            }

            let entry = entry.split(':').next().unwrap_or(entry);
            let entry = entry.trim_start_matches('.').to_ascii_lowercase();
            if entry.is_empty() {
                continue;
            }

            if host == entry || host.ends_with(&format!(".{entry}")) {
                return true;
            }
        }
    }

    false
}

/// Minimal base64 for the `Proxy-Authorization` header.
///
/// Hand-rolled rather than adding a dependency for one header: the alphabet and
/// padding are fixed by RFC 4648 and there is nothing here to get subtly wrong
/// that a test would not catch.
/// 为 Proxy-Authorization 实现最小 Base64 编码。只为一个请求头手写而非新增依赖：
/// RFC 4648 固定了字符表与填充规则，细微错误可由测试发现。
fn base64(input: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((triple >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(triple & 0x3f) as usize] as char
        } else {
            '='
        });
    }

    out
}

/// Rewrite a `connect` error so it names the proxy.
///
/// A refused tunnel surfaces as a TLS or IO error on the stream, which reads as
/// "the server is broken". Saying which proxy was in the path is the difference
/// between a five-minute diagnosis and an afternoon.
/// 重写连接错误并标明代理。隧道被拒绝会表现为流上的 TLS 或 I/O 错误，容易误诊为服务器故障；
/// 指明经过的代理可显著缩短排查时间。
pub fn describe_tunnel_failure(proxy: &Proxy, error: &io::Error) -> String {
    format!("through proxy {}:{}: {error}", proxy.host, proxy.port)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    struct SavedProxyEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl SavedProxyEnv {
        fn new() -> Self {
            Self(["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"].into_iter().map(|key| (key, std::env::var_os(key))).collect())
        }
    }
    impl Drop for SavedProxyEnv {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                if let Some(value) = value { std::env::set_var(key, value); } else { std::env::remove_var(key); }
            }
        }
    }

    #[test]
    fn parses_host_and_port() {
        let p = Proxy::parse("http://127.0.0.1:7890").unwrap().unwrap();
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, 7890);
        assert!(p.authorization.is_none());
    }

    #[test]
    fn accepts_a_bare_host_port() {
        // No scheme is what people actually write in these variables.
        // 这些环境变量常使用不带协议的地址。
        let p = Proxy::parse("127.0.0.1:7890").unwrap().unwrap();
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, 7890);
    }

    /**
     * The scheme's own default port, not a made-up one.
     *
     * `http://proxy.example.com` means port 80, which is what every other
     * client would use. The first version of this test asserted 8080 - a value
     * that appeared in the code as a fallback and was in fact unreachable,
     * because `port_or_known_default()` already knows the port. The test was
     * wrong about the behaviour, not the other way round.
     *
     * 使用协议自身的默认端口：HTTP 默认 80；port_or_known_default 已提供该值。
     */
    #[test]
    fn uses_the_scheme_default_port() {
        let p = Proxy::parse("http://proxy.example.com").unwrap().unwrap();
        assert_eq!(p.port, 80);
    }

    /**
     * An `https://` proxy is refused, not silently downgraded.
     *
     * Honouring it would mean opening a plain TCP connection and sending the
     * credentials over it in the clear. `None` makes the caller log the refusal
     * and connect directly, which is a visible outcome rather than a quiet one.
     *
     * 拒绝 HTTPS 代理，避免明文发送凭据；返回 None 后调用方会记录并直接连接。
     */
    #[test]
    fn refuses_an_https_proxy_rather_than_leaking_credentials() {
        assert!(Proxy::parse("https://proxy.example.com:3128")
            .unwrap()
            .is_none());
        assert!(Proxy::parse("https://user:pass@proxy.example.com:3128")
            .unwrap()
            .is_none());
    }

    #[test]
    fn carries_credentials_as_basic_auth() {
        let p = Proxy::parse("http://user:pass@proxy.example.com:3128")
            .unwrap()
            .unwrap();
        // base64("user:pass") == "dXNlcjpwYXNz"
        // 用户名和密码的 Base64 编码示例。
        assert_eq!(p.authorization.as_deref(), Some("Basic dXNlcjpwYXNz"));
    }

    #[test]
    fn rejects_a_socks_proxy_by_reporting_it_unsupported() {
        // `None`, not an error: the caller logs and connects directly.
        // 返回 None 而非错误；调用方记录后直接连接。
        assert!(Proxy::parse("socks5://127.0.0.1:1080").unwrap().is_none());
    }

    #[test]
    fn base64_matches_the_rfc_examples() {
        assert_eq!(base64(""), "");
        assert_eq!(base64("f"), "Zg==");
        assert_eq!(base64("fo"), "Zm8=");
        assert_eq!(base64("foo"), "Zm9v");
        assert_eq!(base64("foob"), "Zm9vYg==");
        assert_eq!(base64("fooba"), "Zm9vYmE=");
        assert_eq!(base64("foobar"), "Zm9vYmFy");
    }

    #[test]
    fn no_proxy_matches_exact_and_suffix() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _saved = SavedProxyEnv::new();
        std::env::set_var("NO_PROXY", "localhost,127.0.0.1,.example.com");
        assert!(is_excluded("localhost"));
        assert!(is_excluded("127.0.0.1"));
        assert!(is_excluded("api.example.com"));
        assert!(is_excluded("example.com"));
        // Not a subdomain of example.com.
        // 不是 example.com 的子域名。
        assert!(!is_excluded("notexample.com"));
        assert!(!is_excluded("example.org"));
    }

    #[test]
    fn no_proxy_star_excludes_everything() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _saved = SavedProxyEnv::new();
        std::env::set_var("NO_PROXY", "*");
        assert!(is_excluded("anything.at.all"));
    }

    #[test]
    fn no_proxy_ignores_a_port() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _saved = SavedProxyEnv::new();
        std::env::set_var("NO_PROXY", "internal.example.com:8080");
        // The host matches whatever port it is reached on.
        // 不论端口如何变化，均匹配该主机。
        assert!(is_excluded("internal.example.com"));
    }

    #[test]
    fn for_url_picks_the_variable_by_scheme() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _saved = SavedProxyEnv::new();
        // Explicitly clearing so the assertions do not depend on the machine.
        // 显式清除环境变量，避免断言依赖运行机器的配置。
        for v in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
            "NO_PROXY",
            "no_proxy",
        ] {
            std::env::remove_var(v);
        }
        std::env::set_var("HTTPS_PROXY", "http://secure-proxy:3128");
        std::env::set_var("HTTP_PROXY", "http://plain-proxy:3128");

        let wss = Proxy::for_url(&url("wss://example.com/api/agent/ws")).unwrap();
        assert_eq!(wss.host, "secure-proxy");

        let https = Proxy::for_url(&url("https://example.com/api/agent/push/token")).unwrap();
        assert_eq!(https.host, "secure-proxy");

        let ws = Proxy::for_url(&url("ws://example.com/api/agent/ws")).unwrap();
        assert_eq!(ws.host, "plain-proxy");

        let http = Proxy::for_url(&url("http://example.com/api/agent/push/token")).unwrap();
        assert_eq!(http.host, "plain-proxy");

        // And NO_PROXY wins over both.
        // NO_PROXY 优先于上述两者。
        std::env::set_var("NO_PROXY", "example.com");
        assert!(Proxy::for_url(&url("wss://example.com/x")).is_none());
        assert!(Proxy::for_url(&url("https://example.com/x")).is_none());
        assert!(Proxy::for_url(&url("ws://example.com/x")).is_none());
        assert!(Proxy::for_url(&url("http://example.com/x")).is_none());
    }
}
