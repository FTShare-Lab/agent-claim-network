//! Session 内 subagent 活动通知。
//!
//! 该通道只负责唤醒同一进程内的等待器，不承载持久化事实。
//! 等待器收到通知后必须回读 subagent store，避免 watch 合并事件影响正确性。

use tokio::sync::watch;

#[derive(Clone)]
pub(super) struct DelegationActivityHub {
    tx: watch::Sender<u64>,
}

impl DelegationActivityHub {
    pub(super) fn new() -> Self {
        let (tx, _) = watch::channel(0);
        Self { tx }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }

    /// 在状态已成功落盘后递增 revision，合并连续通知是允许的。
    pub(super) fn publish(&self) {
        self.tx
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }
}
