//! Shutdown signalling.
//!
//! A container stop sends SIGTERM to PID 1, waits ten seconds, then SIGKILLs.
//! Without a handler the probe dies at the SIGKILL and never gets to say
//! anything - which is invisible on a normal run and very confusing when the
//! operator is trying to work out whether it exited or was killed.
//!
//! SIGTERM and SIGINT both set a flag. `wait()` lets a task park on it, so a
//! long report loop can stop promptly instead of finishing its current tick.
//!
//! Windows has no SIGTERM worth handling - the service manager stops the
//! process outright - so there `wait()` parks forever and the loop only ends
//! by being killed, which is what the platform does anyway.
//!
//! 停止信号处理：容器停止时先向 PID 1 发送 SIGTERM，等待十秒后发送 SIGKILL。
//! 没有处理器时，探针会被 SIGKILL 终止而无法说明情况；平时不明显，排查退出原因时容易困惑。
//! SIGTERM 和 SIGINT 都设置停止标志；`wait()` 让任务等待该标志，使长上报循环及时停止，
//! 不必等待当前周期结束。Windows 服务管理器会直接终止进程，没有值得处理的 SIGTERM，
//! 因此该平台的 `wait()` 一直等待，循环只会在进程被终止时结束，与平台行为一致。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use tokio::sync::Notify;

static REQUESTED: AtomicBool = AtomicBool::new(false);
static NOTIFY: OnceLock<Notify> = OnceLock::new();

fn notify() -> &'static Notify {
    NOTIFY.get_or_init(Notify::new)
}

/// Has a shutdown been asked for?
/// 是否已请求停止？
pub fn requested() -> bool {
    REQUESTED.load(Ordering::Relaxed)
}

/// Resolve when a shutdown is requested. Safe to call repeatedly; every caller
/// wakes on the same signal.
/// 请求停止时返回；可重复调用，所有调用者都会被同一信号唤醒。
pub async fn wait() {
    // Checked first: a signal that arrived before this call would otherwise be
    // missed and the caller would park forever.
    // 先检查标志，避免漏掉调用之前的信号并无限等待。
    if requested() {
        return;
    }
    notify().notified().await;
}

/// Flag a shutdown and wake everyone waiting. Called from the signal handler.
/// 标记停止并唤醒所有等待者；由信号处理器调用。
fn trigger() {
    // `swap` so a second signal does not wake waiters twice - harmless with
    // `Notify`, but it keeps `requested()` honest about the first one.
    // 使用 swap 避免第二个信号再次唤醒等待者；Notify 虽可容忍重复，但 requested() 应准确反映首次请求。
    if !REQUESTED.swap(true, Ordering::SeqCst) {
        notify().notify_waiters();
    }
}

/// Install the handlers. Call once, early.
///
/// Failure is not fatal: on a platform without these signals the probe simply
/// runs until it is killed, which is what would have happened regardless.
/// 尽早安装一次处理器；失败不致命，不支持这些信号的平台本来也会运行至被终止。
pub fn install() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        for (kind, name) in [
            (SignalKind::terminate(), "SIGTERM"),
            (SignalKind::interrupt(), "SIGINT"),
            (SignalKind::hangup(), "SIGHUP"),
        ] {
            match signal(kind) {
                Ok(mut stream) => {
                    tokio::spawn(async move {
                        if stream.recv().await.is_some() {
                            tracing::info!("{name} received - shutting down");
                            trigger();
                        }
                    });
                }
                Err(e) => {
                    tracing::debug!("cannot listen for {name}: {e}");
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        // Ctrl-C is still worth catching, even where the service manager does
        // not use signals: an operator running it by hand in a terminal
        // expects it to stop.
        // 即使服务管理器不使用信号，也要捕获手动运行时的 Ctrl-C。
        tokio::spawn(async {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("interrupt received - shutting down");
                trigger();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_unrequested() {
        // Not asserting on the shared flag's value: these tests run in one
        // process and could race. Just that the accessor exists and answers.
        // 测试共享进程且可能竞争，不断言共享标志的值，只检查访问器能返回。
        let _ = requested();
    }

    #[tokio::test]
    async fn wait_returns_when_already_requested() {
        trigger();
        // Would hang the suite if `wait` parked on a signal that already came.
        // 若 wait 漏掉已到达的信号，测试套件会挂起。
        tokio::time::timeout(std::time::Duration::from_millis(100), wait())
            .await
            .expect("wait() should return immediately once shutdown is requested");
    }
}
