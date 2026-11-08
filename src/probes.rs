use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use url::Url;

use crate::{
    commands::{self, Action, Command, CommandResultBody, Signal},
    config::Config,
    probe_addons::{AddonRequest, AddonResults},
    rpc::endpoint,
    updater::UpdateResult,
};

const MAX_JOBS: usize = 4;
const MAX_BODY: usize = 1024 * 1024;
const MAX_QUEUE: usize = 128;
const POLL_INTERVAL: Duration = Duration::from_secs(30);
const RESULT_RETRY_INTERVAL: Duration = Duration::from_secs(5);

const UNSUPERVISED: &str = "refused: nothing supervises this agent, so exiting would stop monitoring until someone starts it by hand";

#[derive(Clone, Deserialize)]
struct Job {
    addons: AddonRequest,
    run_id: String,
    monitor_id: String,
    target: String,
    #[serde(rename = "type")]
    kind: String,
    method: String,
    options: Value,
    expected_status: String,
    keyword: Option<String>,
    timeout_ms: u64,
    lease_expires_at: u64,
    lease_token: String,
}

#[derive(Clone, Serialize)]
struct ResultBody {
    addons: AddonResults,
    run_id: String,
    lease_token: String,
    observed_at: u64,
    status: u8,
    status_code: u16,
    latency_ms: u64,
    message: String,
}

struct Outcome {
    status: u8,
    status_code: u16,
    message: String,
}

fn target_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent(concat!("polaris-agent/", env!("CARGO_PKG_VERSION")))
}

/// 轮询探测任务和控制命令，并通过独立上传循环回传结果。
/// Poll probe jobs and control commands; return results through a separate upload loop.
pub async fn run(mut cfg: Config, signals: mpsc::Sender<Signal>) {
    let Ok((jobs_url, results_url, commands_url)) = job_urls(&cfg) else {
        return;
    };
    let control_client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .danger_accept_invalid_certs(cfg.insecure)
        .user_agent(concat!("polaris-agent/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    // 目标探测使用独立客户端，不继承控制面允许的不安全 TLS 设置。
    // Probe targets with a separate client that does not inherit insecure control-plane TLS settings.
    let target_client = match target_client_builder().build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    let (result_tx, result_rx) = mpsc::channel(MAX_QUEUE);
    tokio::spawn(upload_loop(control_client.clone(), results_url, result_rx));
    let (command_tx, command_rx) = mpsc::channel(MAX_QUEUE);
    // Started before the first poll so an answer can still be sent when the
    // main loop has already gone: the exit is signalled, never performed here.
    // 首次轮询前启动上传循环；此处只发送退出信号，确保主循环退出前仍可上报结果。
    tokio::spawn(upload_loop(
        control_client.clone(),
        commands_url,
        command_rx,
    ));
    let mut ticker = tokio::time::interval(POLL_INTERVAL);
    let mut configuration = crate::runtime_config::SyncState::new(&cfg);
    loop {
        ticker.tick().await;
        if configuration
            .synchronize(&control_client, &mut cfg, &signals)
            .await
            .is_err()
        {
            tracing::debug!("runtime configuration synchronization deferred");
        }
        let response = match control_client
            .get(jobs_url.clone())
            .timeout(Duration::from_secs(15))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response,
            _ => continue,
        };
        let bytes = match read_limited(response, MAX_BODY).await {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let Ok(valid) = decode_jobs(&bytes) else {
            continue;
        };
        // Worth stopping the jobs for: the batch belongs to the machine this
        // command is about to restart or replace.
        // 命令即将重启或替换本机，停止同批探测任务。
        let superseded = handle_commands(&bytes, &command_tx, &signals, &cfg).await;
        if superseded {
            continue;
        }
        let mut completed = stream::iter(valid)
            .map(|job| {
                let client = target_client.clone();
                async move { execute(&client, job).await }
            })
            .buffer_unordered(MAX_JOBS);
        let _ = forward_completed(&mut completed, &result_tx).await;
    }
}

/// Runs this response's commands and reports whether one of them is ending the
/// process, in which case the jobs in the same batch are pointless.
/// 执行响应中的命令；若命令将结束进程，同批探测任务无需执行。
async fn handle_commands(
    bytes: &[u8],
    results: &mpsc::Sender<PendingCommand>,
    signals: &mpsc::Sender<Signal>,
    cfg: &Config,
) -> bool {
    for command in commands::decode_commands(bytes) {
        if run_command(&command, results, signals, cfg).await {
            return true;
        }
    }
    false
}

async fn run_command(
    command: &Command,
    results: &mpsc::Sender<PendingCommand>,
    signals: &mpsc::Sender<Signal>,
    cfg: &Config,
) -> bool {
    let (body, ends_process) = match command.action {
        Action::ReReport => match signals.send(Signal::ReReport).await {
            Ok(()) => (ack(command, "a fresh basicReport was requested"), false),
            Err(_) => return false,
        },
        // Checked before the download, not after: an unsupervised agent has no
        // use for a binary it must not start running.
        // 下载前检查：无守护进程的 Agent 无法启动新二进制，无需下载。
        Action::Restart | Action::Upgrade
            if !commands::is_supervised(cfg.service_tier.as_deref()) =>
        {
            (refuse(command), false)
        }
        Action::Restart => {
            let config = cfg.clone();
            match tokio::task::spawn_blocking(move || crate::restart::prepare(&config)).await {
                Ok(Ok(())) => (ack(command, "restart handoff prepared"), true),
                Ok(Err(error)) => (fail(command, format!("restart failed: {error:#}")), false),
                Err(error) => (fail(command, format!("restart worker failed: {error}")), false),
            }
        },
        Action::Upgrade => run_update(command, cfg).await,
        Action::CheckUpdate => (run_check(command).await, false),
    };
    if !ends_process {
        let _ = results.send(PendingCommand::report(body)).await;
        return false;
    }
    // Queued before the exit is signalled: the main loop waits on this receipt,
    // so the answer is on the wire before the process it describes goes away.
    // 先排队结果再发送退出信号；主循环等待回执，确保进程退出前结果已发送。
    let (delivered, receipt) = oneshot::channel();
    if results
        .send(PendingCommand::report_then(body, delivered))
        .await
        .is_err()
    {
        return false;
    }
    let _ = signals.send(Signal::Exit(receipt)).await;
    true
}

/// Returns the answer and whether the process must exit for it to take effect.
/// 返回命令结果及其生效是否需要退出进程。
async fn run_update(command: &Command, cfg: &Config) -> (CommandResultBody, bool) {
    let update_config = cfg.clone();
    // The download is blocking ureq I/O and can take a while on a slow link;
    // on the runtime it would stall every other task's timer.
    // 下载使用阻塞 ureq I/O；直接在运行时执行会阻塞其他任务的定时器。
    let outcome =
        tokio::task::spawn_blocking(move || crate::updater::update_running(&update_config)).await;
    match outcome {
        // Applied has already replaced the binary, so continuing would run the
        // old image from memory against a new file on disk.
        // 更新已替换磁盘上的程序；继续运行会使内存中的旧程序与新文件不一致。
        Ok(Ok(UpdateResult::Applied)) => (
            ack(command, "update applied; restarting into the new binary"),
            true,
        ),
        Ok(Ok(UpdateResult::AlreadyCurrent)) => {
            (ack(command, "already running the newest release"), false)
        }
        Ok(Ok(UpdateResult::RestartPending)) => (
            ack(
                command,
                "verified update ready; the update helper will restart this Agent",
            ),
            true,
        ),
        Ok(Ok(UpdateResult::Staged)) => (
            ack(
                command,
                "release staged; it needs a manual replacement of the binary",
            ),
            false,
        ),
        Ok(Err(err)) => (fail(command, format!("update failed: {err}")), false),
        Err(err) => (fail(command, format!("update worker failed: {err}")), false),
    }
}

async fn run_check(command: &Command) -> CommandResultBody {
    let outcome = tokio::task::spawn_blocking(crate::updater::check).await;
    match outcome {
        Ok(Ok(check)) if check.available => ack(
            command,
            &format!("update available: {} -> {}", check.current, check.latest),
        ),
        Ok(Ok(check)) => ack(command, &format!("up to date ({})", check.current)),
        Ok(Err(err)) => fail(command, format!("check failed: {err}")),
        Err(err) => fail(command, format!("check failed: {err}")),
    }
}

fn ack(command: &Command, message: &str) -> CommandResultBody {
    CommandResultBody {
        id: command.id.clone(),
        lease_token: command.lease_token.clone(),
        ok: true,
        message: message.into(),
    }
}

fn refuse(command: &Command) -> CommandResultBody {
    fail(command, UNSUPERVISED)
}

fn fail(command: &Command, message: impl Into<String>) -> CommandResultBody {
    CommandResultBody {
        id: command.id.clone(),
        lease_token: command.lease_token.clone(),
        ok: false,
        message: message.into().chars().take(512).collect(),
    }
}

async fn forward_completed<S>(completed: &mut S, sender: &mpsc::Sender<ResultBody>)
where
    S: futures_util::Stream<Item = Option<ResultBody>> + Unpin,
{
    while let Some(result) = completed.next().await {
        if let Some(result) = result {
            if sender.send(result).await.is_err() {
                break;
            }
        }
    }
}

/// An answer the server still owes a lease for.
///
/// Both kinds share one queue because both obey the same rule: a rejected
/// upload is kept and retried until the server accepts it or says the lease is
/// gone.
/// 尚需服务端确认租约的结果；两类结果共用队列，直到接受或租约失效才停止重试。
trait Reportable: serde::Serialize + Send + 'static {
    fn ack(body: &[u8]) -> Result<()>;

    /// Called once the server has accepted this answer.
    /// 服务端接受此结果后调用。
    fn delivered(self);
}

impl Reportable for ResultBody {
    fn ack(body: &[u8]) -> Result<()> {
        parse_result_ack(body)
    }

    fn delivered(self) {}
}

/// A command answer, with the receipt the exit waits on.
/// 命令结果及退出操作等待的回执。
struct PendingCommand {
    body: CommandResultBody,
    delivered: Option<oneshot::Sender<()>>,
}

impl Serialize for PendingCommand {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.body.serialize(serializer)
    }
}

impl PendingCommand {
    fn report(body: CommandResultBody) -> Self {
        Self {
            body,
            delivered: None,
        }
    }

    fn report_then(body: CommandResultBody, delivered: oneshot::Sender<()>) -> Self {
        Self {
            body,
            delivered: Some(delivered),
        }
    }
}

impl Reportable for PendingCommand {
    fn ack(body: &[u8]) -> Result<()> {
        parse_command_ack(body)
    }

    fn delivered(self) {
        if let Some(delivered) = self.delivered {
            let _ = delivered.send(());
        }
    }
}

async fn upload_loop<T: Reportable>(
    client: reqwest::Client,
    url: Url,
    mut incoming: mpsc::Receiver<T>,
) {
    let mut pending = VecDeque::new();
    let mut retrying = false;
    let mut retry = tokio::time::interval(RESULT_RETRY_INTERVAL);
    retry.tick().await;
    loop {
        if pending.is_empty() {
            match incoming.recv().await {
                Some(result) => pending.push_back(result),
                None => return,
            }
        }
        if !retrying {
            if upload_pending(&client, &url, &mut pending).await.is_err() {
                tracing::warn!("probe result delivery failed; retaining results for retry");
                retrying = true;
            } else {
                continue;
            }
        }
        tokio::select! {
            result = incoming.recv(), if pending.len() < MAX_QUEUE => match result {
                Some(result) => pending.push_back(result),
                None => return,
            },
            _ = retry.tick() => retrying = false,
        }
    }
}

async fn upload_pending<T: Reportable>(
    client: &reqwest::Client,
    url: &Url,
    pending: &mut VecDeque<T>,
) -> Result<()> {
    while let Some(result) = pending.front() {
        let response = client
            .post(url.clone())
            .json(result)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("probe result upload failed")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = read_limited(response, 4096).await;
            if let Ok(body) = body {
                if invalidated_lease(status.as_u16(), &body) {
                    pending.pop_front();
                    tracing::warn!(
                        http_status = status.as_u16(),
                        "discarded result for invalidated probe lease"
                    );
                    continue;
                }
            }
            anyhow::bail!("probe result rejected: HTTP {}", status)
        }
        let body = read_limited(response, 4096).await?;
        T::ack(&body)?;
        if let Some(delivered) = pending.pop_front() {
            delivered.delivered();
        }
    }
    Ok(())
}

fn invalidated_lease(status: u16, body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    matches!(
        (status, value.get("error").and_then(Value::as_str)),
        (409, Some("stale")) | (404, Some("not_found"))
    )
}

async fn execute(client: &reqwest::Client, job: Job) -> Option<ResultBody> {
    let start = Instant::now();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    let budget = job.timeout_ms.min(
        job.lease_expires_at
            .saturating_sub(now_ms)
            .saturating_sub(1000),
    );
    if budget == 0 {
        return None;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(budget);
    let primary = async {
        let outcome = tokio::time::timeout_at(deadline, run_target(client, &job))
            .await
            .unwrap_or(Outcome {
                status: 0,
                status_code: 0,
                message: "probe timed out".into(),
            });
        (outcome, start.elapsed())
    };
    let ((outcome, primary_elapsed), addons) = tokio::join!(
        primary,
        crate::probe_addons::run(client, &job.addons, deadline)
    );
    let mut status = outcome.status;
    let mut message = outcome.message;
    if message.is_empty() && outcome.status == 1 {
        if let Some(limit) = job
            .options
            .get("maxLatencyMs")
            .and_then(Value::as_f64)
            .filter(|limit| limit.is_finite() && *limit > 0.0)
        {
            if primary_elapsed.as_secs_f64() * 1000.0 > limit {
                status = 2;
                message = "response exceeded configured latency threshold".into();
            }
        }
    }
    Some(ResultBody {
        addons,
        run_id: job.run_id,
        lease_token: job.lease_token,
        observed_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        status,
        status_code: outcome.status_code,
        latency_ms: primary_elapsed.as_millis().min(3_600_000) as u64,
        message: crate::util::bounded_text(&message, 512),
    })
}

async fn run_target(client: &reqwest::Client, job: &Job) -> Outcome {
    if job.kind == "udp" || job.kind == "icmp" {
        let result = if job.kind == "udp" { crate::probe_network::udp(&job.target, &job.options).await }
            else { crate::probe_network::icmp(&job.target).await };
        return match result { Ok(()) => Outcome { status: 1, status_code: 0, message: String::new() },
            Err(error) => down(&format!("{} probe failed: {error}", job.kind.to_uppercase())) };
    }
    if job.kind == "tcp" {
        return run_tcp(job).await;
    }
    let response = match run_http(client, job).await {
        Ok(response) => response,
        Err(_) => return down("HTTP request failed"),
    };
    let code = response.status().as_u16();
    let header_value = job
        .options
        .get("headerName")
        .and_then(Value::as_str)
        .and_then(|name| response.headers().get(name))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if !expected_status(&job.expected_status, code) {
        return Outcome {
            status: 0,
            status_code: code,
            message: format!("HTTP {code} not in configured status"),
        };
    }
    let keyword = job.keyword.as_deref().unwrap_or("");
    let json_path = job.options.get("jsonPath").and_then(Value::as_str);
    let header_name = job.options.get("headerName").and_then(Value::as_str);
    let mut message = String::new();
    if !keyword.is_empty() || json_path.is_some() {
        let bytes = match read_limited(response, MAX_BODY).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return Outcome {
                    status: 0,
                    status_code: code,
                    message: "response body unavailable or too large".into(),
                }
            }
        };
        let body = String::from_utf8_lossy(&bytes);
        if !keyword.is_empty() && !body.contains(keyword) {
            message = "configured keyword not found".into();
        }
        if let Some(path) = json_path {
            let parsed: Value = match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(_) => {
                    return Outcome {
                        status: 0,
                        status_code: code,
                        message: "response body is not JSON".into(),
                    }
                }
            };
            let expected = job
                .options
                .get("jsonExpected")
                .and_then(Value::as_str)
                .unwrap_or("");
            if json_at(&parsed, path).map(value_text).as_deref() != Some(expected) {
                message = "configured JSON value did not match".into();
            }
        }
    }
    if header_name.is_some() {
        let expected = job
            .options
            .get("headerExpected")
            .and_then(Value::as_str)
            .unwrap_or("");
        match header_value.as_deref() {
            Some(value) if expected.is_empty() || value.contains(expected) => (),
            _ => message = "configured response header did not match".into(),
        }
    }
    Outcome {
        status: u8::from(message.is_empty()),
        status_code: code,
        message,
    }
}

async fn run_http(client: &reqwest::Client, job: &Job) -> Result<reqwest::Response> {
    let mut request = client.request(
        reqwest::Method::from_bytes(job.method.as_bytes())?,
        &job.target,
    );
    if let Some(headers) = job.options.get("headers").and_then(Value::as_object) {
        for (key, value) in headers {
            if let Some(value) = value.as_str() {
                request = request.header(key, value);
            }
        }
    }
    if let Some(auth) = job.options.get("auth") {
        match auth.get("type").and_then(Value::as_str) {
            Some("bearer") => {
                request =
                    request.bearer_auth(auth.get("token").and_then(Value::as_str).unwrap_or(""))
            }
            Some("basic") => {
                request = request.basic_auth(
                    auth.get("username").and_then(Value::as_str).unwrap_or(""),
                    Some(auth.get("password").and_then(Value::as_str).unwrap_or("")),
                )
            }
            _ => (),
        }
    }
    if let Some(body) = job.options.get("body").and_then(Value::as_str) {
        if !body.is_empty() {
            request = request.body(body.to_owned());
        }
    }
    request
        .timeout(Duration::from_millis(job.timeout_ms))
        .send()
        .await
        .context("HTTP target request failed")
}

async fn run_tcp(job: &Job) -> Outcome {
    let target = job.target.strip_prefix("tcp://").unwrap_or(&job.target);
    let parsed = if let Some(v6) = target.strip_prefix('[').and_then(|v| v.split_once("]:")) {
        (v6.0.to_owned(), v6.1.parse::<u16>().ok())
    } else if let Some((host, port)) = target.rsplit_once(':') {
        (host.to_owned(), port.parse::<u16>().ok())
    } else {
        return down("invalid TCP target");
    };
    let Some(port) = parsed.1.filter(|p| *p > 0) else {
        return down("invalid TCP port");
    };
    let addrs = match tokio::net::lookup_host((parsed.0.as_str(), port)).await {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(_) => return down("TCP host did not resolve"),
    };
    if addrs.is_empty() {
        return down("TCP host did not resolve");
    }
    let mut stream = None;
    for addr in addrs {
        if let Ok(connected) = TcpStream::connect(addr).await {
            stream = Some(connected);
            break;
        }
    }
    let Some(mut stream) = stream else {
        return down("TCP connection failed");
    };
    if let Some(send) = job
        .options
        .get("send")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        let line = if send.ends_with(['\r', '\n']) {
            send.to_owned()
        } else {
            format!("{send}\r\n")
        };
        if stream.write_all(line.as_bytes()).await.is_err() {
            return down("TCP write failed");
        }
    }
    if let Some(expect) = job
        .options
        .get("expect")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        let mut banner = Vec::with_capacity(512);
        let mut chunk = [0u8; 512];
        while banner.len() < 4096 {
            let count = match stream.read(&mut chunk).await {
                Ok(n) => n,
                Err(_) => return down("TCP read failed"),
            };
            if count == 0 {
                break;
            }
            banner.extend_from_slice(&chunk[..count]);
            if String::from_utf8_lossy(&banner).contains(expect) {
                break;
            }
        }
        if !String::from_utf8_lossy(&banner).contains(expect) {
            return down("configured TCP response was not found");
        }
    }
    Outcome {
        status: 1,
        status_code: 0,
        message: String::new(),
    }
}

pub(crate) async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("response body read failed")?;
        if body.len() + chunk.len() > limit {
            anyhow::bail!("response body too large")
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn valid_job(job: &Job) -> bool {
    if job.run_id.is_empty()
        || job.run_id.len() > 256
        || job.monitor_id.is_empty()
        || job.lease_token.is_empty()
        || job.lease_token.len() > 256
        || job.timeout_ms == 0
        || job.timeout_ms > 3_600_000
    {
        return false;
    }
    if job.kind == "http" {
        return Url::parse(&job.target)
            .map(|u| matches!(u.scheme(), "http" | "https") && u.host().is_some())
            .unwrap_or(false)
            && !job.method.is_empty()
            && job.method.len() <= 32;
    }
    matches!(job.kind.as_str(), "tcp" | "udp" | "icmp") && !job.target.is_empty() && job.target.len() <= 512
}

fn job_urls(cfg: &Config) -> Result<(Url, Url, Url)> {
    let push = endpoint::derive(cfg)?.http_push;
    let jobs = Url::parse(&push.as_str().replacen("/push/", "/jobs/", 1))?;
    let results = Url::parse(&push.as_str().replacen("/push/", "/results/", 1))?;
    let commands = Url::parse(&format!(
        "{}/result",
        push.as_str().replacen("/push/", "/commands/", 1)
    ))?;
    Ok((jobs, results, commands))
}

fn parse_result_ack(body: &[u8]) -> Result<()> {
    let ack: Value =
        serde_json::from_slice(body).context("invalid probe result acknowledgement")?;
    if ack.get("ok").and_then(Value::as_bool) != Some(true)
        || ack.get("duplicate").and_then(Value::as_bool).is_none()
    {
        anyhow::bail!("probe result was not acknowledged")
    }
    Ok(())
}

fn parse_command_ack(body: &[u8]) -> Result<()> {
    let ack: Value =
        serde_json::from_slice(body).context("invalid command result acknowledgement")?;
    if ack.get("ok").and_then(Value::as_bool) != Some(true) {
        anyhow::bail!("command result was not acknowledged")
    }
    Ok(())
}

fn decode_jobs(body: &[u8]) -> Result<Vec<Job>> {
    let response: Value = serde_json::from_slice(body).context("invalid probe job response")?;
    let jobs = response
        .get("jobs")
        .and_then(Value::as_array)
        .context("probe job response has no jobs array")?;
    Ok(jobs
        .iter()
        .take(MAX_JOBS)
        .filter_map(|item| serde_json::from_value(item.clone()).ok())
        .filter(valid_job)
        .collect())
}

fn expected_status(spec: &str, code: u16) -> bool {
    spec.split(',').any(|part| {
        let part = part.trim();
        if let Some((a, b)) = part.split_once('-') {
            return a
                .trim()
                .parse::<u16>()
                .ok()
                .zip(b.trim().parse::<u16>().ok())
                .map(|(a, b)| (a..=b).contains(&code))
                .unwrap_or(false);
        }
        part.parse::<u16>().ok() == Some(code)
    })
}

fn json_at<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for part in path.split('.') {
        let (key, index) = if let Some((key, raw)) = part.split_once('[') {
            (key, raw.strip_suffix(']')?.parse::<usize>().ok())
        } else {
            (part, None)
        };
        if !key.is_empty() {
            current = current.get(key)?;
        }
        if let Some(index) = index {
            current = current.get(index)?;
        }
    }
    Some(current)
}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
fn down(message: &str) -> Outcome {
    Outcome {
        status: 0,
        status_code: 0,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn fixture(response: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = stream.read(&mut request).await;
            let _ = stream.write_all(response).await;
        });
        format!("http://{address}")
    }

    async fn idle_fixture(delay: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(delay).await;
        });
        format!("http://{address}")
    }

    async fn read_request(stream: &mut TcpStream) -> Value {
        let mut bytes = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "fixture request ended early");
            bytes.extend_from_slice(&chunk[..count]);
            let Some(split) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..split]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() >= split + 4 + length {
                return serde_json::from_slice(&bytes[split + 4..split + 4 + length]).unwrap();
            }
        }
    }

    async fn respond(stream: &mut TcpStream, status: u16, body: &str) {
        let label = if status == 200 {
            "OK"
        } else if status == 404 {
            "Not Found"
        } else if status == 409 {
            "Conflict"
        } else {
            "Service Unavailable"
        };
        let response = format!(
            "HTTP/1.1 {status} {label}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    fn result(run_id: &str) -> ResultBody {
        ResultBody {
            addons: AddonResults::default(),
            run_id: run_id.into(),
            lease_token: "lease".into(),
            observed_at: 1,
            status: 1,
            status_code: 200,
            latency_ms: 1,
            message: String::new(),
        }
    }

    fn job(target: String) -> Job {
        Job {
            addons: AddonRequest::default(),
            run_id: "run".into(),
            monitor_id: "monitor".into(),
            target,
            kind: "http".into(),
            method: "GET".into(),
            options: Value::Null,
            expected_status: "200-299".into(),
            keyword: None,
            timeout_ms: 500,
            lease_expires_at: u64::MAX,
            lease_token: "lease".into(),
        }
    }

    #[tokio::test]
    async fn slow_addons_do_not_inflate_primary_latency() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let resolver = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let resolver_url = format!("http://{}/dns-query", resolver.local_addr().unwrap());
        let responder = tokio::spawn(async move {
            let (mut stream, _) = resolver.accept().await.unwrap();
            let mut buffer = [0; 4096];
            stream.read(&mut buffer).await.unwrap();
            tokio::time::sleep(Duration::from_millis(600)).await;
            let body = r#"{"Status":0,"Answer":[{"type":1,"data":"192.0.2.1"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let target =
            fixture(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await;
        let mut task = job(target);
        task.timeout_ms = 2000;
        task.options = serde_json::json!({"maxLatencyMs":400});
        task.addons.dns = Some(crate::probe_addons::DnsRequest {
            hostname: "example.com".into(),
            record_type: "A".into(),
            resolver: resolver_url,
            expected: "192.0.2.1".into(),
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let start = Instant::now();
        let result = execute(&client, task).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(600));
        assert!(
            result.latency_ms < 400,
            "primary latency: {}",
            result.latency_ms
        );
        assert_eq!(result.status, 1);
        assert_eq!(result.addons.dns.unwrap().status, 1);
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn http_fixtures_check_success_status_keyword_and_body_cap() {
        let client = reqwest::Client::builder().build().unwrap();
        let target =
            fixture(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nready")
                .await;
        let mut healthy = job(target);
        healthy.keyword = Some("ready".into());
        assert_eq!(run_target(&client, &healthy).await.status, 1);

        let target =
            fixture(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        assert_eq!(run_target(&client, &job(target)).await.status, 0);

        let body = "x".repeat(MAX_BODY + 1);
        let response: &'static [u8] = Box::leak(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .into_bytes()
            .into_boxed_slice(),
        );
        let target = fixture(response).await;
        let mut bounded = job(target);
        bounded.keyword = Some("absent".into());
        assert_eq!(run_target(&client, &bounded).await.status, 0);
    }

    #[tokio::test]
    async fn slow_success_is_reported_as_warning() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            socket.read(&mut request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let mut slow = job(format!("http://{address}"));
        slow.options = serde_json::json!({"maxLatencyMs": 1});
        let result = execute(&reqwest::Client::new(), slow).await.unwrap();
        assert_eq!(result.status, 2);
        assert_eq!(result.status_code, 200);
    }

    #[tokio::test]
    async fn target_deadline_bounds_a_stalled_http_request() {
        let target = idle_fixture(Duration::from_millis(500)).await;
        let mut stalled = job(target);
        stalled.timeout_ms = 50;
        let result = execute(&reqwest::Client::new(), stalled).await.unwrap();
        assert_eq!(result.status, 0);
        assert!(result.latency_ms < 300);
    }

    #[tokio::test]
    async fn rejected_result_stays_queued_until_a_valid_duplicate_ack() {
        let client = reqwest::Client::new();
        let mut pending = VecDeque::from([ResultBody {
            addons: AddonResults::default(),
            run_id: "run".into(),
            lease_token: "lease".into(),
            observed_at: 1,
            status: 1,
            status_code: 200,
            latency_ms: 1,
            message: String::new(),
        }]);
        let rejected = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rejected_url = Url::parse(&format!(
            "http://{}/results/token",
            rejected.local_addr().unwrap()
        ))
        .unwrap();
        let rejected_server = tokio::spawn(async move {
            let (mut stream, _) = rejected.accept().await.unwrap();
            let mut request = vec![0; 2048];
            let _ = stream.read(&mut request).await;
            stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        assert!(upload_pending(&client, &rejected_url, &mut pending)
            .await
            .is_err());
        rejected_server.await.unwrap();
        assert_eq!(pending.len(), 1);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/results/token",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let ack_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 2048];
            let _ = stream.read(&mut request).await;
            let body = r#"{"ok":true,"duplicate":true}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        upload_pending(&client, &url, &mut pending).await.unwrap();
        ack_server.await.unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn stale_and_missing_leases_are_dropped_before_the_next_valid_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/results/token",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let responses = [
                (409, r#"{"error":"stale"}"#),
                (404, r#"{"error":"not_found"}"#),
                (200, r#"{"ok":true,"duplicate":false}"#),
            ];
            let mut received = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                received.push(
                    read_request(&mut stream).await["run_id"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
                respond(&mut stream, status, body).await;
            }
            received
        });
        let client = reqwest::Client::new();
        let mut pending = VecDeque::from([result("expired"), result("deleted"), result("valid")]);
        upload_pending(&client, &url, &mut pending).await.unwrap();
        assert!(pending.is_empty());
        assert_eq!(server.await.unwrap(), ["expired", "deleted", "valid"]);
    }

    #[tokio::test]
    async fn a_fast_result_uploads_while_its_slow_batch_peer_is_running() {
        use futures_util::FutureExt;
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        let slow_finished = Arc::new(AtomicBool::new(false));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/results/token",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let slow_seen_by_first_upload = slow_finished.clone();
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first_tx = Some(first_tx);
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                if index == 0 {
                    let run_id = request["run_id"].as_str().unwrap().to_owned();
                    let slow_done = slow_seen_by_first_upload.load(Ordering::SeqCst);
                    let _ = first_tx.take().unwrap().send((run_id, slow_done));
                }
                respond(&mut stream, 200, r#"{"ok":true,"duplicate":false}"#).await;
            }
        });

        let client = reqwest::Client::new();
        let (sender, receiver) = mpsc::channel(MAX_QUEUE);
        let uploader = tokio::spawn(upload_loop(client, url, receiver));
        let slow_done = slow_finished.clone();
        let mut completed = stream::iter(vec![
            async move {
                tokio::time::sleep(Duration::from_millis(250)).await;
                slow_done.store(true, Ordering::SeqCst);
                Some(result("slow"))
            }
            .boxed(),
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Some(result("fast"))
            }
            .boxed(),
        ])
        .buffer_unordered(2);
        forward_completed(&mut completed, &sender).await;
        drop(sender);

        assert_eq!(first_rx.await.unwrap(), ("fast".into(), false));
        server.await.unwrap();
        uploader.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_fixture_checks_banner_and_connection_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"SSH-2.0-fixture\r\n").await.unwrap();
        });
        let mut probe = job(format!("{address}"));
        probe.kind = "tcp".into();
        probe.options = serde_json::json!({"expect":"SSH-2.0"});
        assert_eq!(run_tcp(&probe).await.status, 1);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        probe.target = address.to_string();
        assert_eq!(run_tcp(&probe).await.status, 0);
    }

    #[test]
    fn rejects_malformed_jobs_and_accepts_ipv6_target_shape() {
        let mut item = job("https://example.test/".into());
        assert!(valid_job(&item));
        item.target = "file:///etc/passwd".into();
        assert!(!valid_job(&item));
        item.kind = "tcp".into();
        item.target = "[::1]:443".into();
        assert!(valid_job(&item));
        item.timeout_ms = 0;
        assert!(!valid_job(&item));
    }

    #[test]
    fn result_ack_requires_json_success_and_allows_duplicate_replay() {
        assert!(parse_result_ack(br#"{"ok":true,"duplicate":false}"#).is_ok());
        assert!(parse_result_ack(br#"{"ok":true,"duplicate":true}"#).is_ok());
        assert!(parse_result_ack(b"<html>200</html>").is_err());
        assert!(parse_result_ack(br#"{"ok":false,"duplicate":false}"#).is_err());
    }

    #[test]
    fn only_backend_confirmed_missing_leases_are_terminal() {
        assert!(invalidated_lease(409, br#"{"error":"stale"}"#));
        assert!(invalidated_lease(404, br#"{"error":"not_found"}"#));
        assert!(!invalidated_lease(409, br#"{"error":"conflict"}"#));
        assert!(!invalidated_lease(400, br#"{"error":"invalid"}"#));
        assert!(!invalidated_lease(401, br#"{"error":"invalid token"}"#));
        assert!(!invalidated_lease(503, br#"{"error":"stale"}"#));
        assert!(!invalidated_lease(409, b"<html>stale</html>"));
    }

    #[test]
    fn job_poll_rejects_html_success_and_caps_invalid_job_batches() {
        assert!(decode_jobs(b"<html>200</html>").is_err());
        let encoded = serde_json::json!({
            "run_id":"run", "monitor_id":"monitor", "target":"http://127.0.0.1/", "addons":{},
            "type":"http", "method":"GET", "options":{}, "expected_status":"200-299",
            "keyword":null, "timeout_ms":500, "lease_expires_at":u64::MAX, "lease_token":"lease"
        });
        let response = serde_json::json!({ "jobs": (0..MAX_JOBS + 2).map(|_| encoded.clone()).collect::<Vec<_>>() });
        assert_eq!(
            decode_jobs(&serde_json::to_vec(&response).unwrap())
                .unwrap()
                .len(),
            MAX_JOBS
        );
    }

    #[test]
    fn job_routes_keep_an_encoded_endpoint_token() {
        let cfg = Config {
            endpoint: "https://host.test/api/public/agent/a%2Fb".into(),
            token: "ignored".into(),
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

            config_path: std::path::PathBuf::from("unused"),
            runtime_overrides: Default::default(),
        };
        let (jobs, results, commands) = job_urls(&cfg).unwrap();
        assert_eq!(jobs.path(), "/api/public/agent/jobs/a%2Fb");
        assert_eq!(results.path(), "/api/public/agent/results/a%2Fb");
        assert_eq!(commands.path(), "/api/public/agent/commands/a%2Fb/result");
    }

    #[test]
    fn command_ack_requires_json_success() {
        assert!(parse_command_ack(br#"{"ok":true}"#).is_ok());
        assert!(parse_command_ack(br#"{"ok":false}"#).is_err());
        assert!(parse_command_ack(b"<html>200</html>").is_err());
    }

    #[test]
    fn an_unsupervised_agent_refuses_to_end_itself() {
        let command = Command {
            id: "id".into(),
            action: Action::Restart,
            lease_token: "lease".into(),
        };
        let refused = refuse(&command);
        assert!(!refused.ok);
        assert_eq!(refused.id, "id");
        assert!(refused.message.contains("unsupervised") || refused.message.contains("supervises"));

        let accepted = ack(&command, "fine");
        assert!(accepted.ok);
        assert_eq!(accepted.lease_token, "lease");
    }

    #[test]
    fn command_results_send_back_the_lease_they_were_issued_under() {
        let pending = PendingCommand::report(CommandResultBody {
            id: "command-id".into(),
            lease_token: "lease-token".into(),
            ok: true,
            message: "done".into(),
        });
        assert_eq!(
            serde_json::to_value(&pending).unwrap(),
            serde_json::json!({
                "id": "command-id",
                "lease_token": "lease-token",
                "ok": true,
                "message": "done",
            })
        );
    }
    #[tokio::test]
    async fn probe_http_tls_checks_certificate_chain_and_hostname() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (trusted, correct_name) in [(true, true), (false, true), (true, false)] {
            let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let bytes = certificate.serialize_der().unwrap();
            let der = rustls::pki_types::CertificateDer::from(bytes.clone());
            let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.serialize_private_key_der());
            let config = rustls::ServerConfig::builder()
                .with_no_client_auth().with_single_cert(vec![der], key.into()).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                if let Ok(mut tls) = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)).accept(socket).await {
                    let mut request = [0; 4096];
                    let _ = tls.read(&mut request).await;
                    let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await;
                }
            });
            let mut builder = target_client_builder().no_proxy();
            if trusted { builder = builder.add_root_certificate(reqwest::Certificate::from_der(&bytes).unwrap()); }
            let client = builder.build().unwrap();
            let host = if correct_name { "localhost" } else { "127.0.0.1" };
            let result = client.get(format!("https://{host}:{port}/")).send().await;
            if trusted && correct_name {
                assert_eq!(result.unwrap().text().await.unwrap(), "ok");
            } else {
                assert!(result.is_err(), "invalid certificate unexpectedly accepted");
            }
            server.await.unwrap();
        }
    }

}
