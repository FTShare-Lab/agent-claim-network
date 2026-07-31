//! agent 端 trait 定义。
//!
//! 本地存储与外部协作边界通过这些 trait 接入：
//! - `LocalClaimStore`：agent 自身本机的 claim / trace 文件读写（始终本地，不变）
//! - `InboxReader`：读取并维护自己本机的 inbox
//! - `MemoryStore`：读写 agent 私有 markdown memory（始终本地，不进入团队共享存储）
//!
//! 设计纪律：agent 模块**不持有**其他 agent 的存储句柄，也不暴露 PathBuf
//! 给业务层（见 `docs/architecture.md` 的 Agent 数据边界）。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::claim::{Claim, ClaimId, DisputeId, InboxId, InboxMessage, Trace};
use crate::memory::{MemoryApplyReport, MemoryOp, MemorySnapshot};

#[async_trait]
pub trait LocalClaimStore: Send + Sync {
    async fn write_claim(&self, claim: &Claim) -> anyhow::Result<()>;
    async fn write_trace(&self, trace: &Trace) -> anyhow::Result<()>;
    async fn list_local_claims(&self) -> anyhow::Result<Vec<Claim>>;
}

#[async_trait]
pub trait ReportedDisputeClaimSetStore: Send + Sync {
    async fn contains_claim_set(&self, claims: &[ClaimId]) -> anyhow::Result<bool>;

    async fn record_claim_set(
        &self,
        claims: &[ClaimId],
        dispute_id: &DisputeId,
        reported_at: DateTime<Utc>,
    ) -> anyhow::Result<()>;
}

#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn read_memory(&self) -> anyhow::Result<String>;
    async fn read_user(&self) -> anyhow::Result<String>;
    async fn read_snapshot(&self) -> anyhow::Result<MemorySnapshot>;
    async fn apply_ops(&self, ops: &[MemoryOp]) -> anyhow::Result<MemoryApplyReport>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedInboxMessage {
    pub message: InboxMessage,
    pub lease_id: Option<String>,
}

impl ClaimedInboxMessage {
    pub fn from_message(message: InboxMessage) -> Self {
        Self {
            message,
            lease_id: None,
        }
    }
}

#[async_trait]
pub trait InboxReader: Send + Sync {
    /// 列出当前所有尚未 ack 的消息，按业务事件时间升序排列。
    async fn list_pending(&self) -> anyhow::Result<Vec<InboxMessage>>;
    /// 标记消息已处理（本地实现写入带 handled_at 的 .done.yaml，并做幂等去重）。
    async fn ack(&self, msg_id: &InboxId) -> anyhow::Result<()>;
    /// 把刚从 maintainer pull 下来的消息落地到本机 inbox 目录。
    /// 已存在同 id 的 pending / done 文件时跳过（at-most-once 下游幂等保护）。
    async fn accept_pulled(&self, msg: &InboxMessage) -> anyhow::Result<()>;

    /// 原子领取当前可处理的 pending 消息。默认实现兼容旧的无租约 inbox reader。
    async fn claim_pending(&self) -> anyhow::Result<Vec<ClaimedInboxMessage>> {
        Ok(self
            .list_pending()
            .await?
            .into_iter()
            .map(ClaimedInboxMessage::from_message)
            .collect())
    }

    /// ack 已领取的消息。默认退化为按消息 id ack。
    async fn ack_claimed(&self, claimed: &ClaimedInboxMessage) -> anyhow::Result<()> {
        self.ack(&claimed.message.id).await
    }

    /// 释放已领取但未成功处理的消息，让后续 inbox 扫描可以重试。
    async fn release_claimed(&self, _claimed: &ClaimedInboxMessage) -> anyhow::Result<()> {
        Ok(())
    }
}
