//! Bounded queue of undelivered frames, oldest first.
//! Reports may include finalized minute summaries; overflow drops the oldest frame and its summaries.
//! 未送达帧的有界队列，按时间从旧到新排列。
//! 上报可能包含已完成的分钟汇总；溢出时丢弃最旧的帧及其汇总。

use std::collections::VecDeque;

use crate::rpc::RpcRequest;

/// Maximum queued frames; coverage depends on the configured report interval.
/// 队列帧数上限；覆盖时长取决于配置的上报间隔。
pub const DEFAULT_CAPACITY: usize = 60;

pub struct FrameQueue {
    frames: VecDeque<RpcRequest>,
    capacity: usize,
}

impl FrameQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            frames: VecDeque::with_capacity(capacity.min(64)),
            capacity: capacity.max(1),
        }
    }

    /// Hold a frame that could not be delivered.
    /// 暂存未能送达的帧。
    pub fn push(&mut self, frame: RpcRequest) {
        if self.frames.len() == self.capacity {
            // Front is the oldest; the newest reading is what matters.
            // 队首最旧；需要保留最新读数。
            self.frames.pop_front();
        }
        self.frames.push_back(frame);
    }

    /// 尽量保留正在发送的队首帧；队列全被保留时仍淘汰最后一帧。
    /// Preserve the in-flight prefix where possible; evict the last frame if the whole queue is reserved.
    pub fn push_reserving_prefix(&mut self, frame: RpcRequest, reserved: usize) -> bool {
        let replaced = self.frames.len() == self.capacity;
        if replaced {
            self.frames.remove(reserved.min(self.frames.len() - 1));
        }
        self.frames.push_back(frame);
        replaced
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Take everything, oldest first. The caller owns the frames now, so a
    /// failed flush must push them back rather than assume they survived.
    /// 按从旧到新的顺序取出全部帧；发送失败时调用方须将其放回。
    pub fn take(&mut self) -> Vec<RpcRequest> {
        self.frames.drain(..).collect()
    }

    /// Restore failed frames ahead of newer ones, dropping the oldest if capacity is exceeded.
    /// 将失败帧放回较新帧之前；超出容量时丢弃最旧帧。
    pub fn restore_front(&mut self, frames: Vec<RpcRequest>) {
        let mut combined: VecDeque<RpcRequest> = frames.into();
        combined.append(&mut self.frames);
        while combined.len() > self.capacity {
            combined.pop_front();
        }
        self.frames = combined;
    }
}

impl Default for FrameQueue {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame(n: u64) -> RpcRequest {
        RpcRequest::notification("report", json!({ "n": n }))
    }

    fn n_of(req: &RpcRequest) -> u64 {
        req.params["n"].as_u64().unwrap()
    }

    #[test]
    fn take_returns_oldest_first() {
        let mut q = FrameQueue::new(10);
        for i in 1..=3 {
            q.push(frame(i));
        }
        let got: Vec<u64> = q.take().iter().map(n_of).collect();
        assert_eq!(got, vec![1, 2, 3]);
        assert!(q.is_empty(), "take should drain");
    }

    #[test]
    fn full_queue_drops_the_oldest() {
        let mut q = FrameQueue::new(3);
        for i in 1..=5 {
            q.push(frame(i));
        }
        // 1 and 2 were pushed out; the newest reading is what the badge shows.
        // 1 和 2 被挤出；状态标记显示最新读数。
        let got: Vec<u64> = q.take().iter().map(n_of).collect();
        assert_eq!(got, vec![3, 4, 5]);
    }

    #[test]
    fn restore_front_keeps_order_ahead_of_newer_frames() {
        let mut q = FrameQueue::new(10);
        let taken = {
            for i in 1..=3 {
                q.push(frame(i));
            }
            q.take()
        };
        // A newer frame arrives while the flush is in flight.
        // 发送过程中有更新的帧到达。
        q.push(frame(9));
        q.restore_front(taken);

        let got: Vec<u64> = q.take().iter().map(n_of).collect();
        assert_eq!(
            got,
            vec![1, 2, 3, 9],
            "restored frames must stay oldest-first"
        );
    }

    #[test]
    fn restore_respects_capacity() {
        let mut q = FrameQueue::new(2);
        q.restore_front(vec![frame(1), frame(2), frame(3)]);
        assert_eq!(q.len(), 2, "restore must not exceed the bound");
        // 容量不足时淘汰旧帧，保留最新两帧。
        // Evict older frames on overflow, keeping the newest two.
        let got: Vec<u64> = q.take().iter().map(n_of).collect();
        assert_eq!(got, vec![2, 3]);
    }

    #[test]
    fn restore_prefers_queued_frames_over_newer_ones_when_over_capacity() {
        // Order is what the server applies, so restored (older) frames must
        // come first - but when the bound cannot hold everything, the newest
        // readings are the ones worth keeping.
        // 服务端按顺序应用，因此恢复的旧帧排在前面；容量不足时保留最新读数。
        let mut q = FrameQueue::new(2);
        q.push(frame(8));
        q.push(frame(9));
        q.restore_front(vec![frame(1), frame(2)]);

        let got: Vec<u64> = q.take().iter().map(n_of).collect();
        assert_eq!(got, vec![8, 9], "the newest frames win the bound");
    }

    #[test]
    fn capacity_of_zero_is_clamped() {
        // A misconfigured bound should not make push a no-op that silently
        // drops every frame.
        // 容量配置错误时，push 不应悄悄丢弃所有帧。
        let mut q = FrameQueue::new(0);
        q.push(frame(1));
        assert_eq!(q.len(), 1);
    }
}
