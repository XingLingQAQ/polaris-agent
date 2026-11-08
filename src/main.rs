//! Supervise sampling and reporting sessions; switch to HTTP after repeated WebSocket failures and periodically retry WebSocket.
//! 管理采样和上报会话；WebSocket 多次失败后切换 HTTP，并定期重试 WebSocket。

use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

use tracing::{error, info, warn};
use clap::Parser;
use polaris_agent::commands::Signal;
use polaris_agent::config::{CliArgs, Config};
use polaris_agent::queue::FrameQueue;
use polaris_agent::rpc::{self, AgentClient, HttpTransport};
use polaris_agent::shutdown;
use polaris_agent::sysstat;

/// Wait after an unexpected failure. Long enough not to spin against a server
/// that is down, short enough that a transient fault costs little.
/// 异常失败后的等待时间：避免对故障服务器空转，同时限制瞬时故障的影响。
const RESTART_PAUSE: Duration = Duration::from_secs(5);

/// Failed WebSocket sessions before switching to HTTP.
/// 切换到 HTTP 前允许的 WebSocket 会话失败次数。
const WS_ATTEMPTS_BEFORE_FALLBACK: u32 = 3;

/// Interval for WebSocket recovery checks while reporting over HTTP.
/// HTTP 上报期间检查 WebSocket 恢复的间隔。
const WS_RETRY_INTERVAL: Duration = Duration::from_secs(300);
const MAX_SUMMARIES_PER_REPORT: usize = 32;
const AUTO_UPDATE_INTERVAL: Duration = Duration::from_secs(86_400);

/// Bound the wait for a restart command receipt before allowing the process to exit.
/// 限制退出前等待重启命令回执的时间。
const EXIT_DELIVERY_GRACE: Duration = Duration::from_secs(15);

enum SessionEnd {
    Ended,
    Recovered(AgentClient),
    Updated,
    /// A command requested exit; its receipt wait has completed or timed out.
    /// 命令请求退出，回执等待已完成或超时。
    Commanded,
    Reconfigured(polaris_agent::runtime_config::Reconfiguration),
}

struct UpdateSchedule {
    enabled: bool,
    last_attempt: Option<tokio::time::Instant>,
}

fn update_due(
    enabled: bool,
    last_attempt: Option<tokio::time::Instant>,
    now: tokio::time::Instant,
) -> bool {
    enabled
        && last_attempt.map_or(true, |last| {
            now.duration_since(last) >= AUTO_UPDATE_INTERVAL
        })
}

fn update_should_exit(result: anyhow::Result<polaris_agent::updater::UpdateResult>) -> bool {
    matches!(result, Ok(polaris_agent::updater::UpdateResult::Applied | polaris_agent::updater::UpdateResult::RestartPending))
}

async fn check_auto_update(cfg: &Config, schedule: &mut UpdateSchedule) -> bool {
    let now = tokio::time::Instant::now();
    if !update_due(schedule.enabled, schedule.last_attempt, now) {
        return false;
    }
    schedule.last_attempt = Some(now);
    let update_config = cfg.clone();
    let result = tokio::task::spawn_blocking(move || {
        polaris_agent::updater::update_running(&update_config)
    })
    .await;
    match result {
        Ok(Ok(outcome)) => {
            if update_should_exit(Ok(outcome)) {
                info!("Agent update ready; exiting for the managed restart");
                true
            } else if outcome == polaris_agent::updater::UpdateResult::Staged {
                warn!("automatic update returned a manual staging result; keeping current process running");
                false
            } else {
                false
            }
        }
        Ok(Err(err)) => {
            warn!(err = %err, "automatic Agent update failed; retrying after backoff");
            false
        }
        Err(err) => {
            warn!(err = %err, "automatic Agent update worker failed; retrying after backoff");
            false
        }
    }
}

async fn recovery_loop<T, F, Fut>(
    delay: Duration,
    mut connect: F,
    sender: tokio::sync::mpsc::Sender<T>,
) where
    T: Send + 'static,
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    loop {
        tokio::time::sleep(delay).await;
        match connect().await {
            Ok(client) => {
                let _ = sender.send(client).await;
                return;
            }
            Err(err) => warn!(err = %err, "websocket recovery attempt failed"),
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Wait for the next command signal.
///
/// A closed channel means the probes task has stopped - it gives up when it
/// cannot resolve its own URLs. Parking instead of returning keeps the
/// session's other arms live; this future simply never fires again.
/// 等待下一条命令信号。通道关闭表示探针任务因无法解析自身 URL 而停止；
/// 保持等待而非返回，可让会话其他分支继续运行，此 future 不会再次触发。
async fn next_signal(rx: &mut tokio::sync::mpsc::Receiver<Signal>) -> Signal {
    match rx.recv().await {
        Some(signal) => signal,
        None => std::future::pending().await,
    }
}

/// Hold the process open until the command's answer reaches the server.
///
/// A server that never learns the restart happened still holds the lease and
/// can re-issue the command to the agent it just restarted, so the receipt is
/// worth waiting for. It is not worth waiting forever: a server that cannot be
/// reached must not be able to veto an operator's restart.
/// 保持进程运行，直到命令结果送达服务器。若服务器不知道重启已发生，会继续持有租约并可能
/// 再次向刚重启的 Agent 下发命令，因此值得等待回执；但不能无限等待，让断网阻止操作员重启。
async fn wait_for_delivery(receipt: tokio::sync::oneshot::Receiver<()>) {
    if tokio::time::timeout(EXIT_DELIVERY_GRACE, receipt)
        .await
        .is_err()
    {
        warn!("command result was not acknowledged before the exit deadline");
    }
}

/// One last attempt to deliver what is queued, so a clean stop does not
/// discard the final samples.
/// 最后尝试发送队列，避免正常停止时丢失末尾采样。
async fn flush_http(pending: &mut FrameQueue, http: &HttpTransport) {
    if pending.is_empty() {
        return;
    }
    let queued = pending.take();
    match http.send_batch_status(&queued).await {
        Ok(statuses) => {
            let deferred: Vec<_> = queued.into_iter().zip(statuses).filter_map(|(frame, delivered)| (!delivered).then_some(frame)).collect();
            restore_deferred(pending, deferred);
        },
        Err(e) => {
            warn!(err = %e, "http: final flush failed");
            // Put them back so a caller that reconnects still has them; the
            // queue is owned above this session.
            // 队列由会话外持有；放回失败的帧供后续重连使用。
            pending.restore_front(queued);
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Before anything that could block: a stop arriving during startup should
    // still be honoured rather than ignored until the first report.
    // 在可能阻塞的操作前安装处理器，及时响应启动期间的停止请求。
    shutdown::install();
    let args = CliArgs::parse();
    let Some(cfg) = polaris_agent::cli::dispatch(args)? else { return Ok(()); };
    polaris_agent::cli::initialize_logging(cfg.log_file.clone())?;

    info!(
        token_len = cfg.token.len(),
        sample_interval = cfg.sample_interval_secs,
        report_interval = cfg.report_interval_secs,
        version = env!("CARGO_PKG_VERSION"),
        "starting polaris-agent"
    );

    // One-shot extras: public IP / ISP / region / virtualization. Network
    // failures are silent by design - nulls get sent and the probe reports
    // fine. Collected once, not per reconnect, so a flapping link does not
    // re-hit the resolvers every few seconds.
    // 公网 IP、ISP、地区及虚拟化信息只采集一次；网络失败有意静默处理，发送 null 仍可正常上报。
    // 重连时不重新采集，避免网络反复中断时每隔几秒查询解析服务。
    let extras = sysstat::extras::collect().await;
    info!(
        public_ip = ?extras.public_ip,
        region = ?extras.region,
        virtualization = %extras.virtualization,
        "extras collected"
    );

    run_forever(cfg, extras).await
}

/// Which transport the loop is currently using.
/// 当前上报循环使用的传输方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    WebSocket,
    Http,
}

/// Never returns unless the process is asked to stop.
/// 持续运行，直到收到停止请求。
struct SamplingState {
    aggregator: sysstat::MinuteAggregator,
    finalized: VecDeque<sysstat::FinalizedAggregate>,
    latest: (sysstat::SysSnapshot, i64),
    samples: tokio::sync::mpsc::Receiver<(sysstat::SysSnapshot, i64)>,
    sampler_task: AbortOnDrop,
}

fn start_sampler(mut sampler: sysstat::Sampler, interval_secs: u64) -> (tokio::sync::mpsc::Receiver<(sysstat::SysSnapshot, i64)>, AbortOnDrop) {
    start_sample_task(move || (sampler.capture(), observed_at()), Duration::from_secs(interval_secs))
}

fn start_sample_task(mut capture: impl FnMut() -> (sysstat::SysSnapshot, i64) + Send + 'static, interval: Duration) -> (tokio::sync::mpsc::Receiver<(sysstat::SysSnapshot, i64)>, AbortOnDrop) {
    let (sender, receiver) = tokio::sync::mpsc::channel(4096);
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if sender.try_send(capture()).is_err() {
                warn!("sample buffer full or receiver closed; sample discarded");
            }
        }
    });
    (receiver, AbortOnDrop(task))
}

impl SamplingState {
    fn new(cfg: &Config) -> Self {
        let mut sampler = sysstat::Sampler::with_config(cfg.sample_interval_secs, cfg);
        let mut aggregator = sysstat::MinuteAggregator::default();
        let mut finalized = VecDeque::new();
        let latest = record_aggregate(sampler.capture(), observed_at(), &mut aggregator, &mut finalized);
        let (samples, sampler_task) = start_sampler(sampler, cfg.sample_interval_secs);
        Self { aggregator, finalized, latest, samples, sampler_task }
    }

    async fn reconfigure(&mut self, cfg: &Config) {
        self.sampler_task.0.abort();
        let _ = (&mut self.sampler_task.0).await;
        drain_samples(&mut self.samples, &mut self.aggregator, &mut self.finalized, &mut self.latest);
        let sampler = sysstat::Sampler::with_config(cfg.sample_interval_secs, cfg);
        (self.samples, self.sampler_task) = start_sampler(sampler, cfg.sample_interval_secs);
    }
}

fn drain_samples(samples: &mut tokio::sync::mpsc::Receiver<(sysstat::SysSnapshot, i64)>, aggregator: &mut sysstat::MinuteAggregator, finalized: &mut VecDeque<sysstat::FinalizedAggregate>, latest: &mut (sysstat::SysSnapshot, i64)) {
    while let Ok((snapshot, at)) = samples.try_recv() {
        *latest = record_aggregate(snapshot, at, aggregator, finalized);
    }
}

async fn stop_and_drain(sampler_task: &mut AbortOnDrop, samples: &mut tokio::sync::mpsc::Receiver<(sysstat::SysSnapshot, i64)>, aggregator: &mut sysstat::MinuteAggregator, finalized: &mut VecDeque<sysstat::FinalizedAggregate>, latest: &mut (sysstat::SysSnapshot, i64)) {
    sampler_task.0.abort();
    let _ = (&mut sampler_task.0).await;
    drain_samples(samples, aggregator, finalized, latest);
}

async fn run_forever(mut cfg: Config, extras: sysstat::AgentExtras) -> anyhow::Result<()> {
    // The probes task cannot send a report or end the process itself: the
    // transports live here, and so does the loop that decides when to leave.
    // 探针任务不能自行上报或退出；传输和退出决策由此循环负责。
    let (signal_tx, mut signal_rx) = tokio::sync::mpsc::channel(4);
    let probe_task = tokio::spawn(polaris_agent::probes::run(cfg.clone(), signal_tx));
    let _probe_task = AbortOnDrop(probe_task);
    let mut backoff = RESTART_PAUSE;
    let mut transport = Transport::WebSocket;
    let mut ws_failures: u32 = 0;
    let mut recovered_ws = None;
    let mut update_schedule = UpdateSchedule {
        enabled: cfg.auto_update && polaris_agent::runtime_config::auto_update_supported(&cfg),
        last_attempt: None,
    };
    if cfg.auto_update && !update_schedule.enabled {
        warn!("automatic Agent updates require a supervised installation outside containers");
    }

    // Keep undelivered frames across reporting sessions.
    // 跨上报会话保留未送达的帧。
    let mut pending = FrameQueue::default();
    let mut sampling = SamplingState::new(&cfg);

    // Built once. Rebuilding per attempt would re-parse the URL every five
    // seconds on a host that cannot reach us at all.
    // 只构建一次，避免完全无法连接时每五秒重试都重新解析 URL。
    let http = HttpTransport::new(&cfg)?;

    loop {
        if shutdown::requested() {
            info!("shutdown signalled - exiting cleanly");
            return Ok(());
        }

        let outcome = match transport {
            Transport::WebSocket => {
                let attempt = std::panic::AssertUnwindSafe(session_ws(
                    &cfg,
                    &extras,
                    &mut pending,
                    &mut sampling,
                    recovered_ws.take(),
                    &mut update_schedule,
                    &mut signal_rx,
                ));
                match futures_util::FutureExt::catch_unwind(attempt).await {
                    Ok(r) => r,
                    Err(_) => {
                        error!("websocket session panicked");
                        Err(anyhow::anyhow!("panicked"))
                    }
                }
            }
            Transport::Http => {
                session_http(
                    &cfg,
                    &extras,
                    &http,
                    &mut pending,
                    &mut sampling,
                    &mut update_schedule,
                    &mut signal_rx,
                )
                .await
            }
        };

        if shutdown::requested() {
            info!("shutdown signalled - exiting cleanly");
            return Ok(());
        }

        match outcome {
            Ok(SessionEnd::Reconfigured(request)) => {
                let Some((next, applied)) = request.claim_and_commit() else {
                    continue;
                };
                cfg = *next;
                sampling.reconfigure(&cfg).await;
                update_schedule.enabled =
                    cfg.auto_update && polaris_agent::runtime_config::auto_update_supported(&cfg);
                recovered_ws = None;
                let _ = applied.send(Ok(()));
                continue;
            }
            Ok(SessionEnd::Updated) => return Ok(()),
            Ok(SessionEnd::Commanded) => return Ok(()),
            Ok(SessionEnd::Recovered(client)) => {
                recovered_ws = Some(client);
                backoff = RESTART_PAUSE;
                info!("websocket recovered; switching back");
                transport = Transport::WebSocket;
            }
            Ok(SessionEnd::Ended) if transport == Transport::WebSocket => {
                // A clean end means the connection dropped, which is ordinary.
                // 正常结束表示连接断开，属于常见情况。
                ws_failures = 0;
                backoff = RESTART_PAUSE;
            }
            Err(e) if transport == Transport::WebSocket => {
                warn!(err = %e, "websocket session ended with an error");
                ws_failures += 1;
                if ws_failures >= WS_ATTEMPTS_BEFORE_FALLBACK {
                    warn!(
                        attempts = ws_failures,
                        "websocket is not coming up; falling back to HTTP POST"
                    );
                    transport = Transport::Http;
                    ws_failures = 0;
                    backoff = RESTART_PAUSE;
                }
            }
            Ok(SessionEnd::Ended) => {
                backoff = RESTART_PAUSE;
            }
            Err(e) => {
                warn!(err = %e, "http push session ended with an error");
            }
        }

        // While on HTTP, keep testing the socket. A network that blocks
        // WebSocket may stop, and without this the agent would stay on the
        // slower transport for the rest of its life.
        // HTTP 上报期间继续测试 WebSocket，以便网络限制解除后切回。
        let deadline = tokio::time::Instant::now() + backoff;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                Some((snapshot, at)) = sampling.samples.recv() => {
                    sampling.latest = record_aggregate(snapshot, at, &mut sampling.aggregator, &mut sampling.finalized);
                }
                _ = shutdown::wait() => {
                    info!("shutdown signalled during backoff - exiting cleanly");
                    return Ok(());
                }
            }
        }
        // Capped: a host that cannot reach the server retries every 30s
        // forever rather than drifting into hourly attempts.
        // 退避上限为 30 秒，无法连接的主机将每 30 秒持续重试，而不会逐渐延长至每小时。
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// One WebSocket connection's lifetime: connect, announce, flush anything the
/// previous session could not send, then report until it breaks.
/// 单次 WebSocket 会话：连接、声明身份、发送上一会话未能送达的帧，并持续上报直至连接中断。
async fn session_ws(
    cfg: &Config,
    extras: &sysstat::AgentExtras,
    pending: &mut FrameQueue,
    sampling: &mut SamplingState,
    recovered: Option<AgentClient>,
    update_schedule: &mut UpdateSchedule,
    signals: &mut tokio::sync::mpsc::Receiver<Signal>,
) -> anyhow::Result<SessionEnd> {
    let mut client = match recovered {
        Some(client) => client,
        None => AgentClient::connect(cfg).await?,
    };

    let basic = rpc::protocol::build_basic_report(extras, "ws", cfg.report_interval_secs)?;
    client.send(basic).await?;
    info!("basicReport sent, agent registered");

    // Announce current identity before flushing queued reports.
    // 发送积压上报前先声明当前身份。
    if !pending.is_empty() {
        let queued = pending.take();
        let count = queued.len();
        let mut deferred = Vec::new();
        let mut iter = queued.into_iter();
        while let Some(frame) = iter.next() {
            match client.send_status(frame.clone()).await {
                Ok(true) => {},
                Ok(false) => deferred.push(frame),
                Err(e) => {
                    deferred.push(frame);
                    deferred.extend(iter);
                    pending.restore_front(deferred);
                    return Err(e);
                }
            }
        }
        restore_deferred(pending, deferred);
        info!(frames = count, "replayed queued frames");
    }

    let SamplingState { aggregator, finalized, latest, samples, sampler_task } = sampling;
    drain_samples(samples, aggregator, finalized, latest);
    let mut report_ticker = tokio::time::interval(Duration::from_secs(cfg.report_interval_secs));
    report_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    report_ticker.tick().await;
    let (initial_report, sent_summaries) = queued_report(latest, finalized);
    let delivered = match client.send_status(initial_report).await {
        Ok(true) => { acknowledge_summaries(finalized, sent_summaries); true },
        Ok(false) => { queue_snapshot(pending, latest); false },
        Err(err) => {
            queue_snapshot(pending, latest);
            return Err(err);
        }
    };
    if can_auto_update(delivered, pending, finalized) && samples.is_empty() && check_auto_update(cfg, update_schedule).await {
        stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
        flush_final_ws(&mut client, finalized, latest).await;
        return Ok(SessionEnd::Updated);
    }
    let mut keepalive = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            Some((snapshot, at)) = samples.recv() => {
                *latest = record_aggregate(snapshot, at, aggregator, finalized);
                continue;
            }
            _ = report_ticker.tick() => {}
            frame = client.next_frame() => {
                if frame.is_none() {
                    anyhow::bail!("websocket closed by server");
                }
                continue;
            }
            _ = keepalive.tick() => {
                client.ping().await?;
                continue;
            }
            signal = next_signal(signals) => match signal {
                Signal::Reconfigure(request) => {
                    let (report, sent_summaries) = queued_report(latest, finalized);
                    if matches!(client.send_status(report).await, Ok(true)) {
                        acknowledge_summaries(finalized, sent_summaries);
                    } else {
                        queue_snapshot(pending, latest);
                    }
                    return Ok(SessionEnd::Reconfigured(request));
                }
                Signal::ReReport => {
                    let basic = rpc::protocol::build_basic_report(extras, "ws", cfg.report_interval_secs)?;
                    client.send(basic).await?;
                    info!("re_report command: sent a fresh basicReport");
                    continue;
                }
                Signal::Exit(receipt) => {
                    info!("restart command accepted; ending the session so the process can exit");
                    wait_for_delivery(receipt).await;
                    stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
                    flush_final_ws(&mut client, finalized, latest).await;
                    return Ok(SessionEnd::Commanded);
                }
            },
            _ = shutdown::wait() => {
                info!("shutdown signalled - ending session");
                stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
                flush_final_ws(&mut client, finalized, latest).await;
                return Ok(SessionEnd::Ended);
            }
        }

        let (report, sent_summaries) = queued_report(latest, finalized);

        let delivered = match client.send_status(report).await {
            Ok(true) => { acknowledge_summaries(finalized, sent_summaries); true },
            Ok(false) => { queue_snapshot(pending, latest); false },
            Err(e) => {
                warn!(err = %e, "report send failed; retaining summaries and reconnecting");
                queue_snapshot(pending, latest);
                return Err(e);
            }
        };
        if can_auto_update(delivered, pending, finalized) && samples.is_empty() && check_auto_update(cfg, update_schedule).await {
            stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
            flush_final_ws(&mut client, finalized, latest).await;
            return Ok(SessionEnd::Updated);
        }
    }
}

fn can_auto_update(delivered: bool, pending: &FrameQueue, finalized: &VecDeque<sysstat::FinalizedAggregate>) -> bool {
    delivered && pending.is_empty() && finalized.is_empty()
}

fn restore_deferred(pending: &mut FrameQueue, mut deferred: Vec<rpc::RpcRequest>) {
    let deferred_limit = polaris_agent::queue::DEFAULT_CAPACITY / 2;
    if deferred.len() > deferred_limit {
        let excess = deferred.len() - deferred_limit;
        deferred.drain(..excess);
        warn!(dropped = excess, "deferred report queue full; dropping oldest deferred frames");
    }
    pending.restore_front(deferred);
}

fn queue_snapshot(pending: &mut FrameQueue, latest: &(sysstat::SysSnapshot, i64)) {
    let frame = rpc::protocol::build_report(&latest.0, latest.1, &[]);
    if pending.push_reserving_prefix(frame, polaris_agent::queue::DEFAULT_CAPACITY / 2) {
        warn!("report queue full; replacing oldest unreserved snapshot");
    }
}

fn observed_at() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn record_aggregate(
    snapshot: sysstat::SysSnapshot,
    observed_at: i64,
    aggregator: &mut sysstat::MinuteAggregator,
    finalized: &mut VecDeque<sysstat::FinalizedAggregate>,
) -> (sysstat::SysSnapshot, i64) {
    let mem_pct = (snapshot.mem_total > 0)
        .then_some(snapshot.mem_used as f64 * 100.0 / snapshot.mem_total as f64);
    if let Some(bucket) = aggregator.push(
        observed_at,
        sysstat::AggregateSample {
            cpu: Some(snapshot.cpu_usage),
            mem_pct,
            load1: Some(snapshot.load1),
            load5: Some(snapshot.load5),
            net_up_total: Some(snapshot.net_total_up),
            net_down_total: Some(snapshot.net_total_down),
        },
    ) {
        if finalized.len() == 128 {
            warn!("summary buffer full; dropping oldest minute");
            finalized.pop_front();
        }
        finalized.push_back(bucket);
    }
    (snapshot, observed_at)
}

async fn flush_final_ws(client: &mut AgentClient, finalized: &mut VecDeque<sysstat::FinalizedAggregate>, latest: &(sysstat::SysSnapshot, i64)) {
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (report, count) = queued_report(latest, finalized);
            if !matches!(client.send_status(report).await, Ok(true)) { break; }
            if count.is_empty() { break; }
            acknowledge_summaries(finalized, count);
            if finalized.is_empty() { break; }
        }
    }).await;
}

async fn flush_final_http(http: &HttpTransport, finalized: &mut VecDeque<sysstat::FinalizedAggregate>, latest: &(sysstat::SysSnapshot, i64)) {
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (report, count) = queued_report(latest, finalized);
            if !matches!(http.send_status(&report).await, Ok(true)) { break; }
            if count.is_empty() { break; }
            acknowledge_summaries(finalized, count);
            if finalized.is_empty() { break; }
        }
    }).await;
}

fn queued_report(
    latest: &(sysstat::SysSnapshot, i64),
    finalized: &VecDeque<sysstat::FinalizedAggregate>,
) -> (rpc::RpcRequest, Vec<usize>) {
    let indices: Vec<_> = finalized.iter().enumerate()
        .filter(|(_, summary)| summary.bucket_ts <= latest.1)
        .take(MAX_SUMMARIES_PER_REPORT)
        .map(|(index, _)| index)
        .collect();
    let summaries: Vec<_> = indices.iter().map(|&index| finalized[index].clone()).collect();
    (rpc::protocol::build_report(&latest.0, latest.1, &summaries), indices)
}

fn acknowledge_summaries(finalized: &mut VecDeque<sysstat::FinalizedAggregate>, delivered: Vec<usize>) {
    for index in delivered.into_iter().rev() {
        finalized.remove(index);
    }
}

/// HTTP reporting retries queued frames on later report ticks.
/// Identity is retried until acknowledged and refreshed daily; finalized summaries can backfill minute history.
/// HTTP 上报在后续周期重试队列；身份确认前持续重试并每日刷新，分钟汇总可用于回填。
async fn session_http(
    cfg: &Config,
    extras: &sysstat::AgentExtras,
    http: &HttpTransport,
    pending: &mut FrameQueue,
    sampling: &mut SamplingState,
    update_schedule: &mut UpdateSchedule,
    signals: &mut tokio::sync::mpsc::Receiver<Signal>,
) -> anyhow::Result<SessionEnd> {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel(1);
    let retry_cfg = cfg.clone();
    let _retry_task = AbortOnDrop(tokio::spawn(recovery_loop(
        WS_RETRY_INTERVAL,
        move || {
            let cfg = retry_cfg.clone();
            async move { AgentClient::connect(&cfg).await }
        },
        ws_tx,
    )));
    let basic = rpc::protocol::build_basic_report(extras, "http", cfg.report_interval_secs)?;
    // An announce failure is not fatal here - unlike the socket case, where a
    // refused upgrade means there is no channel at all. The request may simply
    // have been unlucky, and the loop below will try again.
    // HTTP 身份声明失败不致命，可能只是此次请求不顺利，下方循环会重试；
    // 与 WebSocket 不同，升级被拒绝意味着根本没有通道。
    let mut last_basic_report = None;
    if let Err(e) = http.send(&basic).await {
        warn!(err = %e, "http: basicReport failed; will retry with the first report");
    } else {
        info!("basicReport posted over HTTP, agent registered");
        last_basic_report = Some(tokio::time::Instant::now());
    }

    let SamplingState { aggregator, finalized, latest, samples, sampler_task } = sampling;
    drain_samples(samples, aggregator, finalized, latest);
    let mut report_ticker = tokio::time::interval(Duration::from_secs(cfg.report_interval_secs));
    report_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    report_ticker.tick().await;
    let (initial_report, sent_summaries) = queued_report(latest, finalized);
    match http.send_status(&initial_report).await {
        Ok(true) => {
            acknowledge_summaries(finalized, sent_summaries);
            if can_auto_update(true, pending, finalized) && samples.is_empty() && check_auto_update(cfg, update_schedule).await {
                stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
                flush_final_http(http, finalized, latest).await;
                return Ok(SessionEnd::Updated);
            }
        }
        Ok(false) => queue_snapshot(pending, latest),
        Err(err) => {
            warn!(err = %err, "http: initial report failed; keeping summaries for retry");
            queue_snapshot(pending, latest);
        }
    }

    let mut consecutive_failures: u32 = 0;

    loop {
        tokio::select! {
            Some((snapshot, at)) = samples.recv() => {
                *latest = record_aggregate(snapshot, at, aggregator, finalized);
                continue;
            }
            _ = report_ticker.tick() => {}
            Some(mut client) = ws_rx.recv() => {
                let announce = rpc::protocol::build_basic_report(extras, "ws", cfg.report_interval_secs)?;
                if let Err(err) = client.send(announce).await {
                    warn!(err = %err, "websocket recovery announce failed");
                    anyhow::bail!("websocket recovery announce failed: {err:#}");
                }
                return Ok(SessionEnd::Recovered(client));
            }
            _ = shutdown::wait() => {
                info!("shutdown signalled - ending session");
                flush_http(pending, http).await;
                stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
                flush_final_http(http, finalized, latest).await;
                return Ok(SessionEnd::Ended);
            }
            signal = next_signal(signals) => match signal {
                Signal::Reconfigure(request) => {
                    flush_http(pending, http).await;
                    let (report, sent_summaries) = queued_report(latest, finalized);
                    if matches!(http.send_status(&report).await, Ok(true)) {
                        acknowledge_summaries(finalized, sent_summaries);
                    } else {
                        queue_snapshot(pending, latest);
                    }
                    return Ok(SessionEnd::Reconfigured(request));
                }
                Signal::ReReport => {
                    let basic = rpc::protocol::build_basic_report(extras, "http", cfg.report_interval_secs)?;
                    match http.send(&basic).await {
                        Ok(()) => {
                            last_basic_report = Some(tokio::time::Instant::now());
                            info!("re_report command: posted a fresh basicReport");
                        }
                        Err(err) => warn!(err = %err, "http: re_report basicReport failed"),
                    }
                    continue;
                }
                Signal::Exit(receipt) => {
                    info!("restart command accepted; exiting so the service manager can restart the process");
                    flush_http(pending, http).await;
                    wait_for_delivery(receipt).await;
                    stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
                    flush_final_http(http, finalized, latest).await;
                    return Ok(SessionEnd::Commanded);
                }
            },
        }

        if last_basic_report.map_or(true, |at| at.elapsed() >= Duration::from_secs(86_400)) {
            let basic =
                rpc::protocol::build_basic_report(extras, "http", cfg.report_interval_secs)?;
            match http.send(&basic).await {
                Ok(()) => last_basic_report = Some(tokio::time::Instant::now()),
                Err(err) => warn!(err = %err, "http: basicReport retry failed"),
            }
        }

        let (report, sent_summaries) = queued_report(latest, finalized);
        let mut queued = pending.take();
        queued.push(report);
        match http.send_batch_status(&queued).await {
            Ok(mut statuses) => {
                let report_delivered = statuses.pop().unwrap_or(false);
                queued.pop();
                let deferred: Vec<_> = queued.into_iter().zip(statuses).filter_map(|(frame, delivered)| (!delivered).then_some(frame)).collect();
                restore_deferred(pending, deferred);
                if report_delivered {
                    acknowledge_summaries(finalized, sent_summaries);
                } else {
                    queue_snapshot(pending, latest);
                }
                if consecutive_failures > 0 {
                    info!(
                        failures = consecutive_failures,
                        frames = pending.len(),
                        "http: link recovered"
                    );
                }
                consecutive_failures = 0;
                if can_auto_update(report_delivered, pending, finalized) && samples.is_empty() && check_auto_update(cfg, update_schedule).await {
                    stop_and_drain(sampler_task, samples, aggregator, finalized, latest).await;
                    flush_final_http(http, finalized, latest).await;
                    return Ok(SessionEnd::Updated);
                }
            }
            Err(e) => {
                queued.pop();
                pending.restore_front(queued);
                queue_snapshot(pending, latest);
                consecutive_failures += 1;
                // Logged at a rising interval rather than every tick: a host
                // that is offline for an hour should not fill the journal with
                // 720 identical lines.
                // 按递增间隔而非每个周期记录失败，避免主机离线一小时时日志出现 720 行相同内容。
                if consecutive_failures == 1 || consecutive_failures % 12 == 0 {
                    warn!(
                        err = %e,
                        failures = consecutive_failures,
                        queued = pending.len(),
                        "http: send failed; keeping frames queued"
                    );
                }
                // A sustained failure is worth reporting upward, but not
                // immediately: the next tick may well succeed, and ending the
                // session to reconnect would be a step backwards for a
                // transport whose whole advantage is that it has no connection
                // to lose.
                // 持续失败时才向上报告；下一周期可能成功，而且 HTTP 的优势是不依赖持久连接，
                // 因单次请求失败而结束会话并重连反而倒退。
                if consecutive_failures >= 60 {
                    anyhow::bail!("http push has failed {consecutive_failures} times in a row");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(bucket_ts: i64) -> sysstat::FinalizedAggregate {
        sysstat::FinalizedAggregate {
            bucket_ts,
            sample_count: 12,
            cpu_samples: 12,
            cpu_avg: Some(25.0),
            cpu_max: Some(50.0),
            mem_samples: 12,
            mem_pct_avg: Some(40.0),
            mem_pct_max: Some(60.0),
            load_1_avg: Some(0.5),
            load_5_avg: Some(0.7),
            net_up_bytes: Some(500),
            net_down_bytes: Some(800),
            network_duration_sec: 55,
            duration_sec: 55,
        }
    }

    #[test]
    fn prolonged_outage_reports_replay_payloads_in_bounded_exact_once_chunks() {
        let mut finalized: VecDeque<_> = (0..128).map(|minute| summary(minute * 60)).collect();
        let latest = (sysstat::SysSnapshot::default(), 10_000);
        let (failed, _) = queued_report(&latest, &finalized);
        let (retry, _) = queued_report(&latest, &finalized);
        assert_eq!(
            serde_json::to_value(failed).unwrap(),
            serde_json::to_value(retry).unwrap()
        );

        let mut delivered = Vec::new();
        while !finalized.is_empty() {
            let (frame, count) = queued_report(&latest, &finalized);
            let summaries = frame.params["summaries"].as_array().unwrap();
            assert!(!summaries.is_empty() && summaries.len() <= MAX_SUMMARIES_PER_REPORT);
            delivered.extend(
                summaries
                    .iter()
                    .map(|row| row["bucket_ts"].as_i64().unwrap()),
            );
            acknowledge_summaries(&mut finalized, count);
        }
        assert_eq!(
            delivered,
            (0..128).map(|minute| minute * 60).collect::<Vec<_>>()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sampling_continues_while_transport_waits_for_ack() {
        let mut n = 0;
        let (mut samples, _task) = start_sample_task(move || {
            n += 1;
            (sysstat::SysSnapshot::default(), 55 + n * 5)
        }, Duration::from_secs(5));
        tokio::task::yield_now().await;
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(5)).await;
            tokio::task::yield_now().await;
        }
        let mut aggregator = sysstat::MinuteAggregator::default();
        let mut finalized = VecDeque::new();
        let mut latest = (sysstat::SysSnapshot::default(), 55);
        drain_samples(&mut samples, &mut aggregator, &mut finalized, &mut latest);
        assert_eq!(latest.1, 70);
        assert_eq!(finalized.len(), 0);
        let (snapshot, at) = (sysstat::SysSnapshot::default(), 120);
        latest = record_aggregate(snapshot, at, &mut aggregator, &mut finalized);
        assert_eq!(latest.1, 120);
        assert_eq!(finalized[0].bucket_ts, 60);
        assert_eq!(finalized[0].sample_count, 3);
    }

    #[test]
    fn future_summaries_wait_without_blocking_current_minutes() {
        let mut finalized = VecDeque::from(vec![summary(365 * 86_400), summary(120), summary(180)]);
        let latest = (sysstat::SysSnapshot::default(), 180);
        let (frame, indices) = queued_report(&latest, &finalized);
        let buckets: Vec<_> = frame.params["summaries"].as_array().unwrap().iter().map(|s| s["bucket_ts"].as_i64().unwrap()).collect();
        assert_eq!(buckets, vec![120, 180]);
        acknowledge_summaries(&mut finalized, indices);
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].bucket_ts, 365 * 86_400);
    }

    #[test]
    fn deferred_capacity_reserves_space_for_current_failed_report() {
        let mut pending = FrameQueue::default();
        let frames = (0..60).map(|n| rpc::RpcRequest::notification("report", serde_json::json!({"sequence": n}))).collect();
        restore_deferred(&mut pending, frames);
        assert_eq!(pending.len(), 30);
        let latest = (sysstat::SysSnapshot::default(), 120);
        queue_snapshot(&mut pending, &latest);
        queue_snapshot(&mut pending, &latest);
        for _ in 0..28 { queue_snapshot(&mut pending, &latest); }
        assert_eq!(pending.len(), 60);
        queue_snapshot(&mut pending, &latest);
        let frames = pending.take();
        assert_eq!(frames.len(), 60);
        assert_eq!(frames[0].params["sequence"], 30);
        assert_eq!(frames[29].params["sequence"], 59);
        assert_eq!(frames[30].params["observed_at"], 120);
        assert_eq!(frames[59].params["observed_at"], 120);
    }

    #[test]
    fn auto_update_waits_for_acknowledgement_and_backlog() {
        let mut pending = FrameQueue::default();
        let mut finalized = VecDeque::new();
        assert!(!can_auto_update(false, &pending, &finalized));
        assert!(can_auto_update(true, &pending, &finalized));
        pending.push(rpc::RpcRequest::notification("report", serde_json::json!({})));
        assert!(!can_auto_update(true, &pending, &finalized));
        pending.take();
        finalized.push_back(summary(120));
        assert!(!can_auto_update(true, &pending, &finalized));
    }

    #[test]
    fn transport_switch_preserves_open_minute_and_unacknowledged_summaries() {
        let sample = |up| sysstat::AggregateSample {
            cpu: Some(30.0), mem_pct: Some(40.0), load1: Some(0.5), load5: Some(0.7),
            net_up_total: Some(up), net_down_total: Some(up * 2),
        };
        let mut aggregator = sysstat::MinuteAggregator::default();
        let mut finalized = VecDeque::new();
        assert!(aggregator.push(55, sample(100)).is_none());
        // WebSocket disconnect, then HTTP takes ownership of the same state.
        // WebSocket 断开后，HTTP 接管同一状态。
        finalized.push_back(aggregator.push(60, sample(110)).unwrap());
        let latest = (sysstat::SysSnapshot::default(), 60);
        let (failed_http, _) = queued_report(&latest, &finalized);
        let (retry_ws, count) = queued_report(&latest, &finalized);
        assert_eq!(serde_json::to_value(failed_http).unwrap(), serde_json::to_value(&retry_ws).unwrap());
        assert_eq!(retry_ws.params["summaries"][0]["bucket_ts"], 0);
        acknowledge_summaries(&mut finalized, count);
        assert!(finalized.is_empty());
        let next = aggregator.push(120, sample(120)).unwrap();
        assert_eq!(next.bucket_ts, 60);
        assert_eq!(next.net_up_bytes, Some(10));
        finalized.push_back(next);
        let (report, count) = queued_report(&latest, &finalized);
        assert_eq!(report.params["summaries"].as_array().unwrap().len(), 1);
        assert_eq!(report.params["summaries"][0]["bucket_ts"], 60);
        acknowledge_summaries(&mut finalized, count);
        assert!(finalized.is_empty());
    }

    #[tokio::test]
    async fn websocket_replay_retains_deferred_and_sends_later_frames() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for index in 0..4 {
                let request: serde_json::Value = match ws.next().await.unwrap().unwrap() {
                    Message::Text(text) => serde_json::from_str(&text).unwrap(),
                    frame => panic!("unexpected frame: {frame:?}"),
                };
                if index == 1 { assert_eq!(request["params"]["sequence"], 1); }
                if index == 2 { assert_eq!(request["params"]["sequence"], 2); }
                ws.send(Message::Text(serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "result":{"ack":true,"ignored":false,"deferred":index == 1}}).to_string())).await.unwrap();
            }
            ws.close(None).await.unwrap();
        });
        let cfg = Config {
            endpoint: format!("ws://{address}"), token: "fixture".into(), sample_interval_secs: 5,
            report_interval_secs: 60, insecure: false, include_nics: vec![], exclude_nics: vec![],
            include_mounts: vec![], exclude_mounts: vec![], auto_update: false,
            log_file: None, service_tier: None, config_path: std::path::PathBuf::from("unused"),
            runtime_overrides: Default::default(),
        };
        let mut pending = FrameQueue::default();
        for sequence in 1..=2 { pending.push(rpc::RpcRequest::notification("report", serde_json::json!({"sequence":sequence}))); }
        let mut schedule = UpdateSchedule { enabled: false, last_attempt: None };
        let (_sender, mut signals) = tokio::sync::mpsc::channel(1);
        let _ = session_ws(&cfg, &sysstat::AgentExtras::fallback(), &mut pending, &mut SamplingState::new(&cfg), None, &mut schedule, &mut signals).await;
        let queued = pending.take();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].params["sequence"], 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_flush_keeps_failed_and_unsent_frames() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for index in 0..3 {
                let request: serde_json::Value = match ws.next().await.unwrap().unwrap() {
                    Message::Text(text) => serde_json::from_str(&text).unwrap(),
                    frame => panic!("unexpected frame: {frame:?}"),
                };
                if index == 2 {
                    assert_eq!(request["params"]["sequence"], 2);
                    ws.close(None).await.unwrap();
                    break;
                }
                ws.send(Message::Text(
                    serde_json::json!({
                        "jsonrpc": "2.0", "id": request["id"],
                        "result": { "ack": true, "ignored": false }
                    })
                    .to_string(),
                ))
                .await
                .unwrap();
            }
        });
        let cfg = Config {
            endpoint: format!("ws://{address}"),
            token: "fixture".into(),
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
        let frames: Vec<_> = (1..=3)
            .map(|sequence| {
                rpc::RpcRequest::notification(
                    "report",
                    serde_json::json!({
                        "sequence": sequence, "summaries": [summary(sequence * 60)]
                    }),
                )
            })
            .collect();
        let expected = serde_json::to_value(&frames[1..]).unwrap();
        let mut pending = FrameQueue::default();
        for frame in frames {
            pending.push(frame);
        }
        let mut update_schedule = UpdateSchedule {
            enabled: false,
            last_attempt: None,
        };
        let (_sender, mut signals) = tokio::sync::mpsc::channel(1);
        let result = session_ws(
            &cfg,
            &sysstat::AgentExtras::fallback(),
            &mut pending,
            &mut SamplingState::new(&cfg),
            None,
            &mut update_schedule,
            &mut signals,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(serde_json::to_value(pending.take()).unwrap(), expected);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn recovery_task_is_cancelled_when_its_session_ends() {
        let task = tokio::spawn(std::future::pending::<()>());
        let handle = task.abort_handle();
        let owner = AbortOnDrop(task);
        drop(owner);
        tokio::task::yield_now().await;
        assert!(handle.is_finished());
    }

    #[tokio::test]
    async fn recovery_retries_on_the_injected_schedule_until_connected() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempt_count = attempts.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(recovery_loop(
            Duration::from_millis(5),
            move || {
                let n = attempt_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        anyhow::bail!("temporary failure");
                    }
                    Ok(17)
                }
            },
            tx,
        ));

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap(),
            Some(17)
        );
        task.await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn automatic_update_is_checked_at_most_once_per_day_after_a_report() {
        let now = tokio::time::Instant::now();
        assert!(update_due(true, None, now));
        assert!(!update_due(false, None, now));
        assert!(!update_due(
            true,
            Some(now),
            now + Duration::from_secs(86_399)
        ));
        assert!(update_due(true, Some(now), now + AUTO_UPDATE_INTERVAL));
    }

    #[test]
    fn only_an_applied_or_handed_off_update_exits_for_restart() {
        assert!(update_should_exit(Ok(
            polaris_agent::updater::UpdateResult::Applied
        )));
        assert!(update_should_exit(Ok(polaris_agent::updater::UpdateResult::RestartPending)));
        assert!(!update_should_exit(Ok(
            polaris_agent::updater::UpdateResult::AlreadyCurrent
        )));
        assert!(!update_should_exit(Ok(
            polaris_agent::updater::UpdateResult::Staged
        )));
        assert!(!update_should_exit(Err(anyhow::anyhow!("fixture failure"))));
    }
}
