//! The HTTP fallback transport.
//!
//! Posts the same JSON-RPC frames the socket carries, to
//! `/api/public/agent/push/<token>`. See the handler's own doc comment for why
//! a second transport exists at all - the short version is that WebSocket is
//! the first thing a corporate proxy refuses, and when it does the monitor
//! reads "no agent connected" with nothing to explain it.
//!
//! **Stateless on purpose.** Each call is one request, so a failure is a
//! failed request rather than a dead connection that has to be noticed. That
//! makes this the more robust of the two on a bad link, which is the opposite
//! of the usual assumption and the reason the agent can simply fall back here
//! when the socket will not come up.
//!
//! HTTP 备用传输向 `/api/public/agent/push/<token>` 发送与 WebSocket 相同的 JSON-RPC 帧。
//! 企业代理经常首先拒绝 WebSocket，此时监控会显示“无 Agent 连接”却没有原因；处理器文档有更多背景。
//! 此传输刻意保持无状态：每次调用是一个请求，因此失败是请求失败，而非需要检测的死连接。
//! 这与通常的假设相反，使它在连接不佳时更稳健，WebSocket 无法建立时 Agent 可回退到此处。

use std::time::Duration;

use anyhow::{Context as _, Result};
use tracing::debug;

use super::endpoint;
use super::proxy::Proxy;
use super::RpcRequest;
use crate::config::Config;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_POST_WORK: usize = 40;

/// Posts frames to the HTTP endpoint.
/// 将帧发送到 HTTP 端点。
pub struct HttpTransport {
    url: String,
    agent: ureq::Agent,
    insecure_https: bool,
}

impl HttpTransport {
    pub fn new(cfg: &Config) -> Result<Self> {
        let endpoints = endpoint::derive(cfg)?;
        let proxy = Proxy::for_url(&endpoints.http_push)
            .map(|proxy| proxy.to_ureq_proxy())
            .transpose()?;
        let mut agent = ureq::AgentBuilder::new()
            .try_proxy_from_env(false)
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("polaris-agent/", env!("CARGO_PKG_VERSION")));
        if let Some(proxy) = proxy {
            agent = agent.proxy(proxy);
        }
        Ok(Self {
            url: endpoints.http_push.to_string(),
            insecure_https: cfg.insecure && endpoints.http_push.scheme() == "https",
            agent: agent.build(),
        })
    }

    /// POST one frame. Returns an error the caller can use to decide whether
    /// to keep this transport.
    /// POST 单帧；返回错误供调用方决定是否继续使用此传输方式。
    pub async fn send(&self, req: &RpcRequest) -> Result<()> {
        if self.send_status(req).await? { Ok(()) } else { anyhow::bail!("report deferred") }
    }

    pub async fn send_status(&self, req: &RpcRequest) -> Result<bool> {
        let statuses = self.send_batch_status(std::slice::from_ref(req)).await?;
        Ok(statuses[0])
    }

    pub async fn send_batch(&self, reqs: &[RpcRequest]) -> Result<()> {
        if self.send_batch_status(reqs).await?.into_iter().all(|delivered| delivered) { Ok(()) } else { anyhow::bail!("report deferred") }
    }

    pub async fn send_batch_status(&self, reqs: &[RpcRequest]) -> Result<Vec<bool>> {
        if self.insecure_https {
            anyhow::bail!("HTTP fallback cannot honor --insecure for HTTPS; use a trusted certificate or allow the WebSocket transport");
        }
        let mut statuses = Vec::with_capacity(reqs.len());
        for range in batch_ranges(reqs)? {
            let chunk = &reqs[range];
            let body = if chunk.len() == 1 {
                serde_json::to_string(&chunk[0]).context("serialize rpc request")?
            } else {
                serde_json::to_string(chunk).context("serialize rpc batch")?
            };
            let (status, result) = self.post(body, chunk.len()).await?;
            validate_acknowledgements(&result, chunk)?;
            debug!(status, frames = chunk.len(), applied = result.applied, "http batch chunk accepted");
            statuses.extend(result.acks.into_iter().map(|ack| !ack.deferred));
        }
        Ok(statuses)
    }

    async fn post(&self, body: String, expected: usize) -> Result<(u16, PushResult)> {
        let agent = self.agent.clone();
        let url = self.url.clone();
        tokio::task::spawn_blocking(move || {
            match agent
                .post(&url)
                .set("content-type", "application/json")
                .send_string(&body)
            {
                Ok(response) => {
                    let status = response.status();
                    let body = response.into_string().context("read HTTP push response")?;
                    let result = parse_success(&body, expected)?;
                    Ok((status, result))
                }
                Err(ureq::Error::Status(code, response)) => {
                    let detail = response.into_string().unwrap_or_default();
                    anyhow::bail!("HTTP push rejected: {code} {detail}");
                }
                Err(error) => Err(anyhow::Error::new(error).context("HTTP push failed")),
            }
        })
        .await
        .context("HTTP worker panicked")?
    }
}

fn frame_work(req: &RpcRequest) -> usize {
    match req.method.as_str() {
        "report" => {
            2 + req
                .params
                .get("summaries")
                .and_then(|v| v.as_array())
                .map_or(0, Vec::len)
        }
        "basicReport" => 2,
        _ => 1,
    }
}

// 按服务端处理成本拆批，同时保留帧的原有顺序。
// Split batches by server processing cost while preserving frame order.
fn batch_ranges(reqs: &[RpcRequest]) -> Result<Vec<std::ops::Range<usize>>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut work = 0;
    for (index, req) in reqs.iter().enumerate() {
        let cost = frame_work(req);
        if cost > MAX_POST_WORK {
            anyhow::bail!("one frame exceeds the HTTP push work budget");
        }
        if index > start && work + cost > MAX_POST_WORK {
            ranges.push(start..index);
            start = index;
            work = 0;
        }
        work += cost;
    }
    if start < reqs.len() {
        ranges.push(start..reqs.len());
    }
    Ok(ranges)
}

#[derive(serde::Deserialize)]
struct PushResult {
    ok: bool,
    applied: usize,
    acks: Vec<PushAck>,
}

#[derive(serde::Deserialize)]
struct PushAck {
    id: serde_json::Value,
    ack: bool,
    ignored: bool,
    #[serde(default)]
    deferred: bool,
}

fn validate_acknowledgements(result: &PushResult, requests: &[RpcRequest]) -> Result<()> {
    if result.acks.len() != requests.len() || result.acks.iter().zip(requests).any(|(ack, request)| ack.id != request.id || ack.deferred && request.method != "report") {
        anyhow::bail!("HTTP push acknowledgements did not match requests");
    }
    Ok(())
}

fn parse_success(body: &str, expected: usize) -> Result<PushResult> {
    let result: PushResult =
        serde_json::from_str(body).context("HTTP push returned invalid JSON")?;
    if !result.ok || result.applied < expected || result.acks.len() != expected || result.acks.iter().any(|ack| !ack.ack || ack.deferred && ack.ignored) {
        anyhow::bail!("HTTP push applied {} of {expected} frames", result.applied);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame(method: &str, summaries: usize) -> RpcRequest {
        RpcRequest::notification(method, json!({ "summaries": vec![{}; summaries] }))
    }

    #[test]
    fn html_200_is_not_a_successful_push() {
        assert!(parse_success("<html>ok</html>", 1).is_err());
        assert!(parse_success(r#"{"ok":false,"applied":0}"#, 1).is_err());
        assert!(parse_success(r#"{"ok":true,"applied":1,"acks":[{"id":null,"ack":true,"ignored":false}]}"#, 1).is_ok());
    }

    #[test]
    fn acknowledgements_match_order_ids_and_deferred_types() {
        let requests = vec![RpcRequest::with_id(1, "report", json!({})), RpcRequest::with_id(2, "report", json!({}))];
        let result = parse_success(r#"{"ok":true,"applied":2,"acks":[{"id":1,"ack":true,"ignored":false,"deferred":true},{"id":2,"ack":true,"ignored":false}]}"#, 2).unwrap();
        assert!(validate_acknowledgements(&result, &requests).is_ok());
        assert_eq!(result.acks.iter().map(|ack| !ack.deferred).collect::<Vec<_>>(), vec![false, true]);
        let reversed = vec![requests[1].clone(), requests[0].clone()];
        assert!(validate_acknowledgements(&result, &reversed).is_err());
        assert!(parse_success(r#"{"ok":true,"applied":1,"acks":[{"id":1,"ack":true,"ignored":true,"deferred":true}]}"#, 1).is_err());
        assert!(parse_success(r#"{"ok":true,"applied":1,"acks":[{"id":1,"ack":true,"ignored":false,"deferred":"yes"}]}"#, 1).is_err());
    }

    #[test]
    fn batch_chunks_stay_inside_the_worker_write_budget() {
        let reports: Vec<_> = (0..60).map(|_| frame("report", 32)).collect();
        let ranges = batch_ranges(&reports).unwrap();
        assert!(ranges.iter().all(|range| range
            .clone()
            .map(|i| frame_work(&reports[i]))
            .sum::<usize>()
            <= MAX_POST_WORK));
        assert_eq!(
            ranges.iter().map(|r| r.len()).collect::<Vec<_>>(),
            vec![1; 60]
        );
    }

    #[test]
    fn basic_reports_share_chunks_but_frame_count_remains_bounded() {
        let frames: Vec<_> = (0..20).map(|_| frame("basicReport", 0)).collect();
        assert_eq!(batch_ranges(&frames).unwrap(), vec![0..20]);
    }
}
