use super::{now, AddonResult, SslRequest};
use crate::{rpc::proxy::Proxy, util::bounded_text};
use anyhow::{anyhow, Result};
use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use url::Url;

#[derive(Debug)]
struct ObservingVerifier {
    standard: Arc<WebPkiServerVerifier>,
    certificate: Arc<Mutex<Option<Vec<u8>>>>,
}

impl ServerCertVerifier for ObservingVerifier {
    fn verify_server_cert(
        &self,
        certificate: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        ocsp: &[u8],
        time: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        // 保留过期证书供展示，但仍执行完整的标准验证。 / Keep expired certificates for display while retaining standard verification.
        if certificate.as_ref().len() <= 65536 {
            if let Ok(mut captured) = self.certificate.lock() {
                *captured = Some(certificate.as_ref().to_vec());
            }
        }
        self.standard
            .verify_server_cert(certificate, intermediates, name, ocsp, time)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.standard
            .verify_tls12_signature(message, certificate, signature)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.standard
            .verify_tls13_signature(message, certificate, signature)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.standard.supported_verify_schemes()
    }
}

pub(super) async fn check(request: &SslRequest) -> Result<AddonResult> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    check_with_roots(request, roots).await
}

async fn check_with_roots(request: &SslRequest, roots: RootCertStore) -> Result<AddonResult> {
    let captured = Arc::new(Mutex::new(None));
    let standard = WebPkiServerVerifier::builder(Arc::new(roots)).build()?;
    let verifier = Arc::new(ObservingVerifier {
        standard,
        certificate: captured.clone(),
    });
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    let authority = if request.host.contains(':') {
        format!("[{}]", request.host)
    } else {
        request.host.clone()
    };
    let target = Url::parse(&format!("wss://{authority}:{}/", request.port))?;
    let socket = match Proxy::for_url(&target) {
        Some(proxy) => proxy.connect_tunnel(&target).await?,
        None => TcpStream::connect((request.host.as_str(), request.port)).await?,
    };
    let name = ServerName::try_from(request.host.clone())
        .map_err(|_| anyhow!("invalid TLS server name"))?;
    let connection = TlsConnector::from(Arc::new(config))
        .connect(name, socket)
        .await;
    let trusted = connection.is_ok();
    let failure = connection.as_ref().err().map(ToString::to_string);
    drop(connection);
    let bytes = captured
        .lock()
        .ok()
        .and_then(|value| value.clone())
        .ok_or_else(|| anyhow!("TLS handshake did not provide a certificate"))?;
    let (_, certificate) = x509_parser::parse_x509_certificate(&bytes)
        .map_err(|_| anyhow!("could not read the TLS certificate"))?;
    let expiry = certificate.validity().not_after.timestamp();
    let observed_at = now();
    let valid = trusted
        && certificate.validity().not_before.timestamp() <= observed_at as i64
        && expiry > observed_at as i64;
    let days = (expiry - observed_at as i64) as f64 / 86400.0;
    let issuer = bounded_text(
        &certificate.issuer().to_string().replace(['\n', '\r'], " "),
        160,
    );
    let date = time::OffsetDateTime::from_unix_timestamp(expiry)?.date();
    let mut message = format!(
        "SSL: {}:{} · expires {date} · {} days left · {issuer}",
        request.host,
        request.port,
        days.round()
    );
    if let Some(failure) = failure {
        message.push_str(&format!(" · TLS verification failed: {failure}"));
    }
    Ok(AddonResult {
        status: if valid && days > f64::from(request.warn_days) {
            1
        } else {
            2
        },
        days_left: Some(days),
        message: bounded_text(&message, 512),
        observed_at,
        tls_valid: Some(valid),
        records: None,
        rcode: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::PrivatePkcs8KeyDer;

    async fn fixture(expired: bool) -> (u16, CertificateDer<'static>, tokio::task::JoinHandle<()>) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]);
        params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        params.not_after = rcgen::date_time_ymd(if expired { 2021 } else { 2035 }, 1, 1);
        let certificate = rcgen::Certificate::from_params(params).unwrap();
        let der = CertificateDer::from(certificate.serialize_der().unwrap());
        let key = PrivatePkcs8KeyDer::from(certificate.serialize_private_key_der());
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![der.clone()], key.into())
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                .accept(socket)
                .await;
        });
        (port, der, server)
    }

    #[tokio::test]
    async fn reads_the_served_certificate_and_keeps_trust_expiry_and_hostname_errors() {
        for (expired, trusted, host, healthy) in [
            (false, true, "localhost", true),
            (false, false, "localhost", false),
            (true, true, "localhost", false),
            (false, true, "127.0.0.1", false),
        ] {
            let (port, certificate, server) = fixture(expired).await;
            let mut roots = RootCertStore::empty();
            if trusted {
                roots.add(certificate).unwrap();
            } else {
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            let result = check_with_roots(
                &SslRequest {
                    host: host.into(),
                    port,
                    warn_days: 14,
                },
                roots,
            )
            .await
            .unwrap();
            assert_eq!(result.status, if healthy { 1 } else { 2 });
            assert_eq!(result.tls_valid, Some(healthy));
            assert!(result.days_left.is_some());
            if expired {
                assert!(result.days_left.unwrap() < 0.0);
            }
            server.await.unwrap();
        }
    }
}
