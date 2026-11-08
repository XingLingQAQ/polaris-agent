mod ssl;

use crate::util::bounded_text;
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::{sync::OnceCell, time::Instant};

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AddonRequest {
    pub ssl: Option<SslRequest>,
    pub domain: Option<DomainRequest>,
    pub dns: Option<DnsRequest>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SslRequest {
    pub host: String,
    pub port: u16,
    pub warn_days: u32,
}
#[derive(Clone, Debug, Deserialize)]
pub struct DomainRequest {
    pub domain: String,
    pub warn_days: u32,
}
#[derive(Clone, Debug, Deserialize)]
pub struct DnsRequest {
    pub hostname: String,
    pub record_type: String,
    pub resolver: String,
    pub expected: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AddonResult {
    pub status: u8,
    pub days_left: Option<f64>,
    pub message: String,
    pub observed_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_valid: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rcode: Option<String>,
}

impl AddonResult {
    fn failed(kind: &str, error: impl std::fmt::Display) -> Self {
        Self {
            status: 2,
            days_left: None,
            message: bounded_text(&format!("{kind} check failed: {error}"), 512),
            observed_at: now(),
            tls_valid: (kind == "SSL").then_some(false),
            records: None,
            rcode: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AddonResults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl: Option<AddonResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<AddonResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns: Option<AddonResult>,
}

pub async fn run(
    client: &reqwest::Client,
    requested: &AddonRequest,
    deadline: Instant,
) -> AddonResults {
    let (ssl, domain, dns) = tokio::join!(
        timed("SSL", requested.ssl.as_ref(), deadline, |request| {
            ssl::check(request)
        }),
        timed("Domain", requested.domain.as_ref(), deadline, |request| {
            check_domain(client, request)
        }),
        timed(
            "DNS",
            requested.dns.as_ref(),
            deadline,
            |request| check_dns(client, request)
        ),
    );
    AddonResults { ssl, domain, dns }
}

async fn timed<'a, T, F, Fut>(
    kind: &str,
    request: Option<&'a T>,
    deadline: Instant,
    execute: F,
) -> Option<AddonResult>
where
    F: FnOnce(&'a T) -> Fut,
    Fut: std::future::Future<Output = Result<AddonResult>>,
{
    let request = request?;
    Some(
        match tokio::time::timeout_at(deadline, execute(request)).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => AddonResult::failed(kind, error),
            Err(_) => AddonResult::failed(kind, "timed out"),
        },
    )
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn json(client: &reqwest::Client, url: &str, parameters: &[(&str, &str)]) -> Result<Value> {
    let response = client
        .get(url)
        .query(parameters)
        .header("accept", "application/json, application/rdap+json")
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    read_json(response).await
}

async fn read_json(mut response: reqwest::Response) -> Result<Value> {
    if !response.status().is_success() {
        bail!("HTTP {}", response.status().as_u16());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        if body.len() + chunk.len() > 1024 * 1024 {
            bail!("response is too large");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&body)?)
}

async fn check_dns(client: &reqwest::Client, request: &DnsRequest) -> Result<AddonResult> {
    let response = client
        .get(&request.resolver)
        .query(&[("name", &request.hostname), ("type", &request.record_type)])
        .header("accept", "application/dns-json")
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let response = read_json(response).await?;
    dns_reading(&response, request)
}

fn dns_reading(response: &Value, request: &DnsRequest) -> Result<AddonResult> {
    let status = response
        .get("Status")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("invalid resolver response"))?;
    let rcode = match status {
        0 => "NOERROR",
        1 => "FORMERR",
        2 => "SERVFAIL",
        3 => "NXDOMAIN",
        4 => "NOTIMP",
        5 => "REFUSED",
        _ => "UNKNOWN",
    };
    let record_type = match request.record_type.as_str() {
        "A" => 1,
        "AAAA" => 28,
        "CNAME" => 5,
        "MX" => 15,
        "TXT" => 16,
        "NS" => 2,
        _ => bail!("unsupported DNS record type"),
    };
    let answers = response.get("Answer").and_then(Value::as_array);
    let records: Vec<String> = answers
        .into_iter()
        .flatten()
        .filter(|answer| answer.get("type").and_then(Value::as_u64) == Some(record_type))
        .filter_map(|answer| answer.get("data").and_then(Value::as_str))
        .take(64)
        .map(|value| bounded_text(value, 2048))
        .collect();
    let matches = status == 0
        && !records.is_empty()
        && (request.expected.is_empty()
            || records
                .iter()
                .any(|value| value.contains(&request.expected)));
    let message = if matches {
        format!("DNS {}: {}", request.record_type, records.join(", "))
    } else if status != 0 {
        format!("DNS {}: {rcode}", request.record_type)
    } else if records.is_empty() {
        format!("DNS: no {} records", request.record_type)
    } else {
        format!(
            "DNS {}: answer does not match the expected value",
            request.record_type
        )
    };
    Ok(AddonResult {
        status: if matches { 1 } else { 2 },
        days_left: None,
        message: bounded_text(&message, 512),
        observed_at: now(),
        tls_valid: None,
        records: Some(records),
        rcode: Some(rcode.into()),
    })
}

static RDAP_BOOTSTRAP: OnceCell<Value> = OnceCell::const_new();

async fn check_domain(client: &reqwest::Client, request: &DomainRequest) -> Result<AddonResult> {
    if request.domain.is_empty() {
        bail!("target has no registrable domain");
    }
    let bootstrap = RDAP_BOOTSTRAP
        .get_or_try_init(|| json(client, "https://data.iana.org/rdap/dns.json", &[]))
        .await?;
    let tld = request.domain.rsplit('.').next().unwrap_or_default();
    let services = bootstrap
        .get("services")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("invalid registry directory"))?;
    let service = services.iter().find(|service| {
        service
            .get(0)
            .and_then(Value::as_array)
            .is_some_and(|names| names.iter().any(|name| name.as_str() == Some(tld)))
    });
    let urls = service
        .and_then(|service| service.get(1))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("no RDAP service for this domain"))?;
    let mut last_error = anyhow!("no HTTPS RDAP service");
    for base in urls
        .iter()
        .filter_map(Value::as_str)
        .filter(|base| base.starts_with("https://"))
        .take(3)
    {
        let url = format!("{}/domain/{}", base.trim_end_matches('/'), request.domain);
        match json(client, &url, &[]).await {
            Ok(value) => match domain_expiry(&value) {
                Some(expiry) => {
                    let days = (expiry - now() as i64) as f64 / 86400.0;
                    return Ok(AddonResult {
                        status: if days > f64::from(request.warn_days) {
                            1
                        } else {
                            2
                        },
                        days_left: Some(days),
                        message: format!(
                            "Domain: {} · expires {} · {} days left",
                            request.domain,
                            OffsetDateTime::from_unix_timestamp(expiry)?.date(),
                            days.round()
                        ),
                        observed_at: now(),
                        tls_valid: None,
                        records: None,
                        rcode: None,
                    });
                }
                None => last_error = anyhow!("registry did not provide an expiry date"),
            },
            Err(error) => last_error = error,
        }
    }
    Err(last_error)
}

fn domain_expiry(value: &Value) -> Option<i64> {
    value
        .get("events")?
        .as_array()?
        .iter()
        .filter(|event| event.get("eventAction").and_then(Value::as_str) == Some("expiration"))
        .filter_map(|event| {
            OffsetDateTime::parse(event.get("eventDate")?.as_str()?, &Rfc3339)
                .ok()
                .map(OffsetDateTime::unix_timestamp)
        })
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dns_preserves_matching_records_and_reports_missing_or_mismatched_answers() {
        let mut request = DnsRequest {
            hostname: "fixture.test".into(),
            record_type: "A".into(),
            resolver: "https://resolver.test".into(),
            expected: "192.0.2.1".into(),
        };
        let response = serde_json::json!({"Status":0,"Answer":[{"type":5,"data":"alias.test"},{"type":1,"data":"192.0.2.1"}]});
        assert_eq!(dns_reading(&response, &request).unwrap().status, 1);
        request.expected = "192.0.2.2".into();
        assert_eq!(dns_reading(&response, &request).unwrap().status, 2);
        assert_eq!(
            dns_reading(&serde_json::json!({"Status":3}), &request)
                .unwrap()
                .rcode
                .as_deref(),
            Some("NXDOMAIN")
        );
        assert_eq!(
            dns_reading(&serde_json::json!({"Status":0,"Answer":[]}), &request)
                .unwrap()
                .status,
            2
        );
    }
    #[test]
    fn domain_dates_require_expiration_events_and_valid_timestamps() {
        assert_eq!(
            domain_expiry(
                &serde_json::json!({"events":[{"eventAction":"expiration","eventDate":"2030-01-01T00:00:00Z"}]})
            ),
            Some(1893456000)
        );
        assert_eq!(
            domain_expiry(
                &serde_json::json!({"events":[{"eventAction":"last changed","eventDate":"2030-01-01T00:00:00Z"}]})
            ),
            None
        );
        assert_eq!(
            domain_expiry(
                &serde_json::json!({"events":[{"eventAction":"expiration","eventDate":"2030-02-30T00:00:00Z"}]})
            ),
            None
        );
    }
}
