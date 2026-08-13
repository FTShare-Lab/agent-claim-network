//! Dispute 仲裁的持久状态、上下文、模型编排、Resolution 提交与恢复。

mod context;
mod evaluator;
mod observation;
mod resolution;
mod resolution_events;
mod service;
mod store;
mod types;
mod worker;

pub(crate) use context::ArbitrationContextBuilder;
pub use evaluator::{phase_timeout, ArbitrationEvaluator, LlmArbitrationEvaluator};
pub use observation::ObservationService;
pub use resolution::{HumanResolutionInput, RejectResolutionInput, ResolutionService};
pub use resolution_events::{spawn_resolution_event_scheduler, ResolutionEventScheduler};
pub use service::{
    is_analysis_conflict, is_analysis_retry, AnalysisConflict, AnalysisRetry, ArbitrationService,
    ReportDisputeResult, SystemArbitrationClock,
};
pub use store::ArbitrationStore;
pub use types::{
    AnalysisError, AnalysisJob, AnalysisLease, AnalysisPhase, AnalysisSource, AnalysisState,
    ArbitrationAnalysis, ArbitrationAnalysisId, ArbitrationProposal, ArbitrationResolutionRecord,
    ArbitrationVerification, AutomaticAnalysisRound, ClaimAssessmentVerification, ClaimObservation,
    ContextWarning, DeliveryIntent, FrozenArbitrationContext, HolderObservation,
    MaintainerDisputeRecord, ObservationState, PendingResolutionDelivery, ResolutionEventTarget,
    ResolutionObservation,
};
pub use worker::{spawn_arbitration_scheduler, ArbitrationScheduler};
