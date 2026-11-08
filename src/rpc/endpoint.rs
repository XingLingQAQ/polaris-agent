use anyhow::{bail, Context, Result};
use url::Url;

use crate::config::Config;

#[derive(Debug, Clone)]
/// 同一服务端派生的 WebSocket 和 HTTP 上报地址。
/// WebSocket and HTTP reporting endpoints derived from the same server.
pub struct Endpoints {
    pub websocket: Url,
    pub http_push: Url,
}

/// 解析站点地址或 Agent 地址，并生成两种传输端点。
/// Resolve a site or Agent address into endpoints for both transports.
pub fn derive(cfg: &Config) -> Result<Endpoints> {
    let mut base = Url::parse(&cfg.endpoint).context("invalid endpoint URL")?;
    let secure = match base.scheme() {
        "http" | "ws" => false,
        "https" | "wss" => true,
        scheme => bail!("unsupported endpoint scheme: {scheme}; use http(s):// or ws(s)://"),
    };
    let path = base.path().trim_end_matches('/');
    let (prefix, path_token) = if let Some((prefix, token)) = path.rsplit_once("/agent/") {
        if !token.is_empty() {
            (format!("{prefix}/agent"), Some(percent_decode(token)?))
        } else {
            (path.to_owned(), None)
        }
    } else {
        (path.to_owned(), None)
    };
    let route = if prefix.is_empty() {
        "/api/public/agent"
    } else {
        &prefix
    };
    if !route.ends_with("/api/public/agent") {
        bail!("endpoint must be a site base URL or /api/public/agent endpoint")
    }
    let query_token = base
        .query_pairs()
        .find(|(k, _)| k == "token")
        .map(|(_, v)| v.into_owned());
    // 地址中的 token 优先于配置项；查询参数优先于路径。
    // A token in the address takes precedence over configuration; query precedes path.
    let token = query_token
        .or(path_token)
        .unwrap_or_else(|| cfg.token.clone());
    base.set_query(None);
    base.set_fragment(None);
    base.set_path(route);
    base.set_scheme(if secure { "wss" } else { "ws" })
        .map_err(|_| anyhow::anyhow!("invalid endpoint scheme"))?;
    base.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("endpoint cannot contain path segments"))?
        .pop_if_empty()
        .push(&token);
    let websocket = base.clone();
    base.set_scheme(if secure { "https" } else { "http" })
        .map_err(|_| anyhow::anyhow!("invalid endpoint scheme"))?;
    base.set_path(route);
    {
        let mut segments = base
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("endpoint cannot contain path segments"))?;
        segments.pop_if_empty();
        segments.push("push").push(&token);
    }
    Ok(Endpoints {
        websocket,
        http_push: base,
    })
}

fn percent_decode(input: &str) -> Result<String> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                bail!("invalid percent escape in endpoint token");
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])?;
            decoded.push(
                u8::from_str_radix(hex, 16).context("invalid percent escape in endpoint token")?,
            );
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(decoded).context("endpoint token is not valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: &str, token: &str) -> Config {
        Config {
            endpoint: endpoint.into(),
            token: token.into(),
            sample_interval_secs: 5,
            report_interval_secs: 60,
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
        }
    }

    #[test]
    fn base_and_complete_endpoints_map_to_both_routes() {
        let endpoints = derive(&config("https://site.test", "a/b ?")).unwrap();
        assert_eq!(
            endpoints.websocket.as_str(),
            "wss://site.test/api/public/agent/a%2Fb%20%3F"
        );
        assert_eq!(
            endpoints.http_push.as_str(),
            "https://site.test/api/public/agent/push/a%2Fb%20%3F"
        );
        let endpoints = derive(&config("wss://site.test/api/public/agent/old", "new")).unwrap();
        assert_eq!(endpoints.websocket.path(), "/api/public/agent/old");
        assert_eq!(endpoints.http_push.path(), "/api/public/agent/push/old");
        let endpoints = derive(&config("http://site.test", "t")).unwrap();
        assert_eq!(endpoints.websocket.scheme(), "ws");
        assert_eq!(endpoints.http_push.scheme(), "http");
    }

    #[test]
    fn rejects_unsupported_schemes_and_strips_query_credentials() {
        assert!(derive(&config("ftp://site.test/api/public/agent", "t")).is_err());
        let endpoints = derive(&config(
            "https://site.test/api/public/agent?token=secret&x=1",
            "ignored",
        ))
        .unwrap();
        assert!(endpoints.websocket.query().is_none());
        assert_eq!(endpoints.websocket.path(), "/api/public/agent/secret");
    }

    #[test]
    fn encoded_path_token_is_decoded_once_and_reencoded_as_a_segment() {
        let endpoints = derive(&config(
            "https://site.test/api/public/agent/a%2Fb%3F",
            "ignored",
        ))
        .unwrap();
        assert_eq!(endpoints.websocket.path(), "/api/public/agent/a%2Fb%3F");
        assert_eq!(
            endpoints.http_push.path(),
            "/api/public/agent/push/a%2Fb%3F"
        );
    }
}
