//! claim 模块。
//!
//! 本模块定义 Agent Claim Network 的全部领域实体：
//! - `Claim`：agent 在某 scope 下持有的判断
//! - `Policy`：team 行动约束
//! - `Trace`：一次任务中 input/output claim 的关联记录
//! - `Dispute`：claim 之间的不兼容标记
//! - `InboxMessage`：team store → agent 的下行消息
//! - `OutboxEntry`：maintainer 端的待投递台账记录
//!
//! 以及统一的 ID 生成（8 位 hex 随机 + 类型前缀），见 `id` 子模块。

#[allow(clippy::module_inception)]
mod claim;
mod dispute;
pub mod id;
mod inbox;
mod outbox;
mod policy;
mod trace;

pub use claim::{Claim, ClaimStatus, Confidence};
pub use dispute::{Dispute, DisputeReportValidationError, DisputeStatus};
pub use id::{
    AgentId, ClaimId, DisputeId, IdError, InboxId, MaintainerActionId, PolicyId, SessionId,
    SourceId, TraceId,
};
pub use inbox::{InboxMessage, InboxMessageKind};
pub use outbox::{DeliveredMark, InboxAckRequest, OfferedMark, OutboxEntry, OutboxTarget};
pub use policy::{Policy, PolicyMessageType, PolicyStatus};
pub use trace::Trace;
