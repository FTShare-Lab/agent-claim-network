//! agent 模块。
//!
//! 对外暴露：
//! - `traits`：`LocalClaimStore` / `InboxReader` / `MemoryStore`
//! - `fs`：上述 trait 的本地文件系统实现（`LocalFs*`）
//! - `runner`：`AgentRunner` 编排器（接 agent traits + `RouterClient` + `MaintainerClient`）
//!
//! 具体运行方式由 bootstrap 装配；agent 业务代码只看这层抽象，不接触 PathBuf。

mod context;
mod dispute_report;
pub mod fs;
mod inbox;
pub(crate) mod maintainer_upload;
mod prepare;
pub mod runner;
mod runner_finalize;
mod runner_trace;
mod session_engine;
pub mod traits;
mod user_shell;

pub use context::AgentContext;
pub(crate) use inbox::PromptInboxJsonGenerator;
pub use runner::{
    AgentRunner, InboxProcessFailure, InboxProcessFailureKind, InboxProcessReport,
    TeamServiceConnectionStatus, TeamServicesConnectionStatus,
};
pub use session_engine::{
    SessionCompactionNoopReason, SessionCompactionResult, SessionEngine, SessionEngineOptions,
    SessionEvent, SessionFinalizeReport, SessionRuntimeStatus, SessionStartReport,
    SessionTurnControl, SessionTurnControlReceiver,
};
pub(crate) use session_engine::{
    SessionFinalizeOnceOutcome, SessionFinalizePreemptionControl, SessionRecapPreemptionControl,
};
pub use traits::{InboxReader, LocalClaimStore, MemoryStore, ReportedDisputeClaimSetStore};
pub use user_shell::{UserShellCommandOutput, UserShellCommandStatus};
