//! Maintainer 仲裁的私有分析、恢复与观测持久化类型。

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::claim::{
    AgentId, ArbitrationResolutionId, Claim, ClaimAssessment, ClaimId, ClaimStatus, Dispute,
    DisputeId, DisputeResolution, InboxId, InboxMessage, MaintainerActionId, Policy,
};
use crate::config::ArbitrationMode;
use crate::router::{CandidateClaim, DisputeRef};
use crate::time::{serde_utc, serde_utc_opt};

pub const ARBITRATION_SCHEMA_VERSION: u32 = 2;
pub const ARBITRATION_PROMPT_VERSION: &str = "maintainer-dispute-arbitration-v8";
pub const CURRENT_SEMANTIC_PROJECTION_VERSION: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ArbitrationAnalysisId(String);

impl ArbitrationAnalysisId {
    pub fn random() -> Self {
        let mut bytes = [0_u8; 8];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(format!("analysis_{}", hex::encode(bytes)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArbitrationAnalysisId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ArbitrationAnalysisId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value
            .strip_prefix("analysis_")
            .ok_or_else(|| "analysis id 必须以 analysis_ 开头".to_string())?;
        if suffix.len() != 16
            || !suffix
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
        {
            return Err("analysis id 后缀必须是 16 位小写 hex".into());
        }
        Ok(Self(value.to_string()))
    }
}

impl<'de> Deserialize<'de> for ArbitrationAnalysisId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisSource {
    Automatic,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AnalysisJob {
    pub dispute_id: DisputeId,
    pub analysis_id: ArbitrationAnalysisId,
    pub source: AnalysisSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintainerDisputeRecord {
    #[serde(flatten)]
    pub dispute: Dispute,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<DisputeResolution>,
}

impl From<Dispute> for MaintainerDisputeRecord {
    fn from(dispute: Dispute) -> Self {
        Self {
            dispute,
            resolution: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisState {
    Pending,
    WaitingContext,
    WaitingReanalysis,
    Proposing,
    Verifying,
    Approved,
    Unresolved,
    Failed,
    Adopting,
    Adopted,
}

impl AnalysisState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Approved | Self::Unresolved | Self::Failed | Self::Adopted
        )
    }

    pub const fn is_recoverable(self) -> bool {
        matches!(
            self,
            Self::Pending
                | Self::WaitingContext
                | Self::WaitingReanalysis
                | Self::Proposing
                | Self::Verifying
                | Self::Adopting
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisPhase {
    Proposal,
    Verification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalysisLease {
    pub token: String,
    pub phase: AnalysisPhase,
    #[serde(with = "serde_utc")]
    pub expires_at: DateTime<Utc>,
    #[serde(with = "serde_utc")]
    pub renewed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWarning {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriorResolutionContext {
    pub dispute_id: DisputeId,
    pub resolution: DisputeResolution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenArbitrationContext {
    #[serde(with = "serde_utc")]
    pub generated_at: DateTime<Utc>,
    pub dispute: Dispute,
    pub direct_claims: Vec<Claim>,
    pub source_claims: Vec<Claim>,
    pub policies: Vec<Policy>,
    pub router_candidate_claims: Vec<ArbitrationRouterCandidate>,
    pub router_disputes: Vec<DisputeRef>,
    pub prior_resolutions: Vec<PriorResolutionContext>,
    #[serde(default)]
    pub warnings: Vec<ContextWarning>,
}

/// Maintainer 模型只接收 Router candidate 的 Claim 内容。旧 V4 冻结上下文中的
/// lifecycle 派生字段仍可反序列化，但新写入不会保存或传给模型。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ArbitrationRouterCandidate {
    pub claim: Claim,
}

impl<'de> Deserialize<'de> for ArbitrationRouterCandidate {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum CompatibleCandidate {
            Current(Claim),
            Legacy(CandidateClaim),
        }

        Ok(match CompatibleCandidate::deserialize(deserializer)? {
            CompatibleCandidate::Current(claim) => Self { claim },
            CompatibleCandidate::Legacy(candidate) => Self {
                claim: candidate.claim,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrationProposal {
    pub resolution_type: crate::claim::ResolutionType,
    pub resolution_basis: crate::claim::ResolutionBasis,
    pub conclusion: String,
    pub claim_assessments: Vec<ClaimAssessment>,
    pub confidence: f64,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human_review_reason: Option<String>,
    pub reasoning: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationVerdict {
    Approve,
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAssessmentVerification {
    pub claim_id: ClaimId,
    pub agreed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrationVerification {
    pub verdict: VerificationVerdict,
    pub resolution_type_agreed: bool,
    pub resolution_basis_agreed: bool,
    pub conclusion_agreed: bool,
    pub claim_assessments: Vec<ClaimAssessmentVerification>,
    pub confidence: f64,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    pub reasoning: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalysisError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutomaticAnalysisRound {
    pub round: u32,
    #[serde(with = "serde_utc")]
    pub started_at: DateTime<Utc>,
    #[serde(default, with = "serde_utc_opt")]
    pub completed_at: Option<DateTime<Utc>>,
    pub semantic_projection_version: u32,
    pub semantic_fingerprint: String,
    pub context_snapshot_hash: String,
    pub proposal: ArbitrationProposal,
    pub verification: ArbitrationVerification,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_change_reason: Option<String>,
}

const fn initial_analysis_round() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrationAnalysis {
    pub schema_version: u32,
    pub analysis_id: ArbitrationAnalysisId,
    pub dispute_id: DisputeId,
    pub source: AnalysisSource,
    /// Automatic Analysis 与原始上报之间的 create-once 绑定，用于双文件写入崩溃后的安全重放。
    /// Manual Analysis 不需要该字段；旧记录缺失时保持可读。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_snapshot: Option<Dispute>,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "serde_utc")]
    pub updated_at: DateTime<Utc>,
    pub prompt_version: String,
    pub mode: ArbitrationMode,
    pub model: String,
    pub confidence_threshold: f64,
    pub semantic_projection_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_snapshot_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<FrozenArbitrationContext>,
    pub state: AnalysisState,
    #[serde(default = "initial_analysis_round")]
    pub analysis_round: u32,
    #[serde(default)]
    pub rounds: Vec<AutomaticAnalysisRound>,
    #[serde(default)]
    pub context_change_count: u32,
    #[serde(default, with = "serde_utc_opt")]
    pub next_retry_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_change_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<AnalysisLease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<ArbitrationProposal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<ArbitrationVerification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_id: Option<ArbitrationResolutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_resolution: Option<ArbitrationResolutionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<AnalysisError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adoption_blocked_reason: Option<String>,
    #[serde(default)]
    pub context_prepare_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryTargetIntent {
    pub inbox_id: InboxId,
    pub target_agent: AgentId,
    pub inbox_message: InboxMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryIntent {
    pub policy: Policy,
    pub maintainer_action_id: MaintainerActionId,
    pub targets: Vec<DeliveryTargetIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResolutionEventTarget {
    pub dispute_id: DisputeId,
    pub resolution_id: ArbitrationResolutionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingResolutionDelivery {
    pub schema_version: u32,
    pub target: ResolutionEventTarget,
    /// Resolution 的固定提交意图。新记录始终携带；旧记录缺失时仍可从当前
    /// resolution.yaml 恢复投递。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_record: Option<Box<ArbitrationResolutionRecord>>,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default, with = "serde_utc_opt")]
    pub next_retry_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrationResolutionRecord {
    pub schema_version: u32,
    pub resolution_id: ArbitrationResolutionId,
    pub dispute_id: DisputeId,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    pub resolution: DisputeResolution,
    pub dispute_snapshot: Dispute,
    pub direct_claim_snapshots: Vec<Claim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_snapshot_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analysis_source_id: Option<ArbitrationAnalysisId>,
    /// 仅用于读取旧 Resolution YAML；新写入与 API 输出都不再暴露 attempt。
    #[serde(default, rename = "source_attempt_id", skip_serializing)]
    pub legacy_source_attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_intent: Option<DeliveryIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_source_resolution_id: Option<ArbitrationResolutionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationState {
    NotDelivered,
    DeliveredUnobserved,
    ObservedConverged,
    ObservedDiverged,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimObservation {
    pub claim_id: ClaimId,
    #[serde(default)]
    pub claim_name: String,
    pub recommended_status: ClaimStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_status: Option<ClaimStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_statement: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_statement: Option<String>,
    #[serde(default)]
    pub policy_provenance_present: bool,
    #[serde(default)]
    pub matched: bool,
    #[serde(default)]
    pub mismatch_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderObservation {
    pub agent_id: AgentId,
    pub state: ObservationState,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub delivery_observed: bool,
    #[serde(default, with = "serde_utc_opt")]
    pub delivered_at: Option<DateTime<Utc>>,
    #[serde(default, with = "serde_utc_opt")]
    pub last_observed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub assessment_count: usize,
    #[serde(default)]
    pub matched_count: usize,
    #[serde(default)]
    pub claims: Vec<ClaimObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionObservation {
    pub resolution_id: ArbitrationResolutionId,
    pub dispute_id: DisputeId,
    #[serde(with = "serde_utc")]
    pub observed_at: DateTime<Utc>,
    #[serde(default)]
    pub holders: Vec<HolderObservation>,
}
