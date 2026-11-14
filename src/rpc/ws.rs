//! WebSocket requests wait for persistence ACKs; main.rs owns reconnection and transport selection.
//! WebSocket 请求等待持久化确认；重连和传输选择由 main.rs 管理。

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async_tls_with_config, Connector, MaybeTlsStream,
    WebSocketStream,
};
use tracing::{debug, warn};
use url::Url;

use super::proxy::Proxy;
use super::RpcRequest;
use crate::config::Config;

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const SEND_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// One WebSocket connection.
/// 单个 WebSocket 连接。
pub struct AgentClient {
    ws: Ws,
    next_id: u64,
}

impl AgentClient {
    /// Connect with three attempts; connect_with_attempts accepts a separate retry budget.
    /// 最多尝试连接三次；connect_with_attempts 可指定独立的重试次数。
    pub async fn connect(cfg: &Config) -> Result<Self> {
        Self::connect_with_attempts(cfg, 3).await
    }

    /// Connect, trying at most `attempts` times.
    /// 最多尝试连接 `attempts` 次。
    pub async fn connect_with_attempts(cfg: &Config, attempts: u32) -> Result<Self> {
        let url = build_url(cfg)?;
        let mut attempt = 0u32;
        let mut backoff = Duration::from_secs(1);
        let max_attempts = attempts.max(1);

        loop {
            attempt += 1;
            debug!(attempt, "connecting");
            let outcome = tokio::time::timeout(CONNECT_TIMEOUT, connect_once(&url, cfg)).await;

            match outcome {
                Ok(Ok(ws)) => {
                    debug!("connected");
                    return Ok(Self { ws, next_id: 1 });
                }
                Ok(Err(failure)) => {
                    // Only handshake failures are terminal; a status-bearing failure means the server was reached. / 只有握手失败才算终止；能拿到状态码说明已到达服务器。
                    if let ConnectFailure::Handshake(e) = &failure {
                        // A rejected token is terminal; retrying hides the real cause. / Token 被拒绝即终止；重试只会掩盖原因。
                        if is_auth_rejection(e) {
                            anyhow::bail!(
                                "server rejected the token (HTTP 401). Check --token against the console."
                            );
                        }
                        // A 404 on the upgrade means the endpoint is wrong; report it, do not retry. / 升级返回 404 表示端点错误；报告而不是重试。
                        if is_not_found(e) {
                            anyhow::bail!("no WebSocket endpoint (HTTP 404). Check --endpoint.");
                        }
                    }
                    warn!(err = %failure, attempt, "connect failed");
                }
                Err(_) => {
                    warn!(attempt, "connect timed out");
                }
            }
            if attempt >= max_attempts {
                anyhow::bail!("failed to connect after {attempt} attempt(s)");
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Assign a request ID, send the frame and wait for its persistence ACK.
    /// On failure, the caller decides whether to reconnect.
    /// 分配请求 ID、发送帧并等待持久化确认；失败时由调用方决定是否重连。
    pub async fn send(&mut self, req: RpcRequest) -> Result<()> {
        if self.send_status(req).await? { Ok(()) } else { anyhow::bail!("report deferred") }
    }

    pub async fn send_status(&mut self, mut req: RpcRequest) -> Result<bool> {
        req.id = serde_json::Value::from(self.next_id);
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let text = serde_json::to_string(&req).context("serialize rpc request")?;
        tokio::time::timeout(SEND_TIMEOUT, self.ws.send(Message::Text(text)))
            .await
            .context("ws send timed out")?
            .context("ws send")?;
        let id = req.id;
        tokio::time::timeout(SEND_TIMEOUT, async {
            loop {
                let frame = self
                    .next_frame()
                    .await
                    .context("websocket closed before acknowledgement")?;
                if let Message::Text(text) = frame {
                    let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) else {
                        continue;
                    };
                    if response.get("id") != Some(&id)
                        || response.get("jsonrpc").and_then(|v| v.as_str()) != Some("2.0")
                    {
                        continue;
                    }
                    if response
                        .get("result")
                        .and_then(|v| v.get("ack"))
                        .and_then(|v| v.as_bool())
                        == Some(true)
                        && response
                            .get("result")
                            .and_then(|v| v.get("ignored"))
                            .and_then(|v| v.as_bool())
                            .is_some()
                    {
                        let result = &response["result"];
                        let deferred = match result.get("deferred") {
                            None => false,
                            Some(value) => value.as_bool().context("invalid deferred acknowledgement")?,
                        };
                        if deferred && result["ignored"] == true {
                            anyhow::bail!("deferred acknowledgement cannot be ignored");
                        }
                        return Ok(!deferred);
                    }
                    anyhow::bail!("websocket request was rejected")
                }
            }
        })
        .await
        .context("ws acknowledgement timed out")?
    }

    pub async fn ping(&mut self) -> Result<()> {
        tokio::time::timeout(SEND_TIMEOUT, self.ws.send(Message::Ping(Vec::new())))
            .await
            .context("ws ping timed out")?
            .context("ws ping")
    }

    /// Read the next frame, replying to Ping and closing on Close or I/O failure.
    /// 读取下一帧，响应 Ping，并在 Close 或 I/O 失败时关闭连接。
    #[allow(dead_code)]
    pub async fn next_frame(&mut self) -> Option<Message> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Ping(payload))) => {
                    if self.ws.send(Message::Pong(payload)).await.is_err() {
                        return None;
                    }
                    return Some(Message::Pong(Vec::new()));
                }
                Some(Ok(Message::Close(frame))) => {
                    let invalid_close = frame.as_ref().map(|f| f.code)
                        == Some(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Abnormal);
                    let reply = if invalid_close {
                        Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                            reason: "".into(),
                        })
                    } else {
                        frame
                    };
                    let _ = self.ws.send(Message::Close(reply)).await;
                    return None;
                }
                Some(Ok(Message::Pong(_))) => return Some(Message::Pong(Vec::new())),
                Some(Ok(Message::Frame(_))) => continue,
                Some(Ok(message)) => return Some(message),
                None | Some(Err(_)) => return None,
            }
        }
    }
}

/// Bad token: the server answers the handshake with 401. / Token 错误：服务端在握手时返回 401。
fn is_auth_rejection(e: &tokio_tungstenite::tungstenite::Error) -> bool {
    use tokio_tungstenite::tungstenite::Error;
    match e {
        Error::Http(resp) => matches!(resp.status().as_u16(), 401 | 403),
        _ => false,
    }
}

/// The endpoint is not there. Also terminal: retrying a wrong URL cannot fix
/// it, and on a network that blocks WebSocket this is the shape the refusal
/// most often takes once a proxy has rewritten the request.
/// 端点不存在时停止重试；代理改写请求后阻止 WebSocket 也常表现为此错误。
fn is_not_found(e: &tokio_tungstenite::tungstenite::Error) -> bool {
    use tokio_tungstenite::tungstenite::Error;
    match e {
        Error::Http(resp) => resp.status().as_u16() == 404,
        _ => false,
    }
}

/// Compiled-in `webpki-roots`, because slim containers may have no CA store. / 使用编译内置根证书，因为精简容器可能没有 CA 存储。
fn tls_connector(insecure: bool) -> Result<Connector> {
    if insecure {
        warn!("--insecure: TLS certificate verification is DISABLED");
        let cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        return Ok(Connector::Rustls(Arc::new(cfg)));
    }

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(tls_connector_with_roots(roots))
}

fn tls_connector_with_roots(roots: rustls::RootCertStore) -> Connector {
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Connector::Rustls(Arc::new(cfg))
}

/// Accepts any certificate. Only reachable through an explicit `--insecure`.
/// 接受任意证书；只有显式指定 `--insecure` 才会使用。
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
        ]
    }
}



/** Only handshake failures carry an HTTP status for credential/endpoint rejection checks. / 只有握手失败包含可用于判断凭据或端点拒绝的 HTTP 状态。 */
#[derive(Debug)]
enum ConnectFailure {
    /// The socket reached the server and the upgrade was refused.
    /// 连接已到达服务器，但升级被拒绝。
    Handshake(tokio_tungstenite::tungstenite::Error),
    /// The connection never got that far: DNS, TCP, TLS or a proxy tunnel.
    /// 连接尚未到达服务器：DNS、TCP、TLS 或代理隧道失败。
    Transport(anyhow::Error),
}

impl std::fmt::Display for ConnectFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handshake(e) => write!(f, "{e}"),
            Self::Transport(e) => write!(f, "{e:#}"),
        }
    }
}

/// One connection attempt, direct or through a proxy; the TLS and WebSocket layers are shared. / 一次连接尝试，直连或经代理；TLS 和 WebSocket 层共用。
async fn connect_once(url: &Url, cfg: &Config) -> Result<Ws, ConnectFailure> {
    let connector = tls_connector(cfg.insecure).map_err(ConnectFailure::Transport)?;

    match Proxy::for_url(url) {
        None => {
            let (ws, _resp) =
                connect_async_tls_with_config(url.as_str(), None, false, Some(connector))
                    .await
                    .map_err(ConnectFailure::Handshake)?;
            Ok(ws)
        }
        Some(proxy) => {
            // TLS runs over the tunnel, so the certificate is still checked against the endpoint name. / TLS 在隧道上运行，证书仍按端点名称校验。
            let stream = proxy
                .connect_tunnel(url)
                .await
                .map_err(ConnectFailure::Transport)?;

            let (ws, _resp) =
                client_async_tls_with_config(url.as_str(), stream, None, Some(connector))
                    .await
                    .map_err(ConnectFailure::Handshake)?;
            Ok(ws)
        }
    }
}

fn build_url(cfg: &Config) -> Result<Url> {
    Ok(super::endpoint::derive(cfg)?.websocket)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};

    #[test]
    fn url_gets_token_appended() {
        let cfg = Config {
            endpoint: "https://example.com".to_string(),
            token: "abc123".to_string(),
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

            config_path: std::path::PathBuf::from("config.json"),
            runtime_overrides: Default::default(),
        };
        let url = build_url(&cfg).unwrap();
        assert_eq!(url.as_str(), "wss://example.com/api/public/agent/abc123");
    }

    #[test]
    fn url_preserves_existing_token() {
        let cfg = Config {
            endpoint: "wss://example.com/api/public/agent/from_url".to_string(),
            token: "from_flag".to_string(),
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

            config_path: std::path::PathBuf::from("config.json"),
            runtime_overrides: Default::default(),
        };
        let url = build_url(&cfg).unwrap();
        assert_eq!(url.as_str(), "wss://example.com/api/public/agent/from_url");
    }

    #[tokio::test]
    async fn deferred_ack_does_not_block_the_next_report() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for deferred in [true, false] {
                let request: serde_json::Value = match ws.next().await.unwrap().unwrap() {
                    Message::Text(text) => serde_json::from_str(&text).unwrap(),
                    frame => panic!("unexpected frame: {frame:?}"),
                };
                ws.send(Message::Text(serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "result":{"ack":true, "ignored":false, "deferred":deferred}}).to_string())).await.unwrap();
            }
        });
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await.unwrap();
        let mut client = AgentClient { ws, next_id: 1 };
        assert!(!client.send_status(RpcRequest::notification("report", serde_json::json!({}))).await.unwrap());
        assert!(client.send_status(RpcRequest::notification("report", serde_json::json!({}))).await.unwrap());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn report_waits_for_its_correlated_ack_after_a_stale_ack() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let request: serde_json::Value = match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => serde_json::from_str(&text).unwrap(),
                frame => panic!("unexpected frame: {frame:?}"),
            };
            assert_eq!(request["method"], "report");
            let id = request["id"].as_u64().unwrap();
            ws.send(Message::Text(serde_json::json!({"jsonrpc":"2.0","id":id + 1,"result":{"ack":true,"ignored":false}}).to_string())).await.unwrap();
            ws.send(Message::Text(
                serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"ack":true,"ignored":false}})
                    .to_string(),
            ))
            .await
            .unwrap();
        });
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await.unwrap();
        let mut client = AgentClient { ws, next_id: 1 };
        assert!(client
            .send(RpcRequest::notification("report", serde_json::json!({})))
            .await
            .is_ok());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn websocket_tls_accepts_trusted_hostname_and_rejects_untrusted_or_wrong_name() {
        for (trusted, correct_name) in [(true, true), (false, true), (true, false)] {
            let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let der = rustls::pki_types::CertificateDer::from(certificate.serialize_der().unwrap());
            let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.serialize_private_key_der());
            let server_config = rustls::ServerConfig::builder()
                .with_no_client_auth().with_single_cert(vec![der.clone()], key.into()).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                if let Ok(tls) = tokio_rustls::TlsAcceptor::from(Arc::new(server_config)).accept(socket).await {
                    if let Ok(mut ws) = tokio_tungstenite::accept_async(tls).await {
                        let request: serde_json::Value = match ws.next().await.unwrap().unwrap() {
                            Message::Text(text) => serde_json::from_str(&text).unwrap(),
                            frame => panic!("unexpected frame: {frame:?}"),
                        };
                        ws.send(Message::Text(serde_json::json!({"jsonrpc":"2.0","id":request["id"],"result":{"ack":true,"ignored":false}}).to_string())).await.unwrap();
                    }
                }
            });
            let connector = if trusted {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(der).unwrap();
                tls_connector_with_roots(roots)
            } else {
                tls_connector(false).unwrap()
            };
            let host = if correct_name { "localhost" } else { "127.0.0.1" };
            let result = connect_async_tls_with_config(format!("wss://{host}:{port}"), None, false, Some(connector)).await;
            if trusted && correct_name {
                let (ws, _) = result.unwrap();
                let mut client = AgentClient { ws, next_id: 1 };
                assert!(client.send(RpcRequest::notification("report", serde_json::json!({}))).await.is_ok());
            } else {
                assert!(result.is_err(), "invalid certificate unexpectedly accepted");
            }
            server.await.unwrap();
        }
    }

}
