//! Session delegation 模块。
//!
//! 本模块承载主 agent 私有的 session-scoped 委托任务：类型、落盘 store 和后续 runner。
//! delegation 不是 ACN Agent，不拥有 inbox/finalize/claim 主体生命周期。对外优先暴露
//! 有界摘要和显式读取接口，避免污染主 session 上下文。

mod activity;
mod compaction;
mod llm_executor;
mod runner;
mod store;
mod types;

pub use llm_executor::LlmDelegationExecutor;
pub use runner::{
    DelegationExecutionContext, DelegationExecutionError, DelegationExecutionOutcome,
    DelegationExecutor, DelegationProgressSink, DelegationRunner, DelegationRunnerConfig,
    DelegationRunnerError, DelegationWaitConfig,
};
pub use store::{read_mode_from_json, DelegationListPage, DelegationStore, DelegationStoreError};
pub use types::{
    DelegationArtifactRef, DelegationCreateRequest, DelegationEvent, DelegationEventKind,
    DelegationId, DelegationMetadata, DelegationProgress, DelegationRead, DelegationReadMode,
    DelegationResult, DelegationStatus, DelegationSummary, DelegationTranscriptEntry,
    DelegationTranscriptKind, DelegationTranscriptMessageSource, DelegationUpdate,
};
