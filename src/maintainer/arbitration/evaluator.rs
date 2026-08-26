//! proposal / verification 的 provider-neutral 结构化模型调用。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use crate::api::{
    ensure_compaction_request_within_context_window, BufferedProviderRuntime,
    ProviderRuntimeFallbackScope, SessionTurnMessage, StructuredJsonCaller,
};
use crate::claim::{ClaimId, ResolutionBasis, ResolutionType};
use crate::config::LlmChatConfig;
use crate::prompt::PromptRegistry;

use super::resolution::validate_assessments;
use super::types::{
    ArbitrationProposal, ArbitrationVerification, FrozenArbitrationContext, VerificationVerdict,
};

const PROPOSAL_PROMPT: &str = "maintainer_arbitration_proposal";
const VERIFICATION_PROMPT: &str = "maintainer_arbitration_verification";

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct ArbitrationOutputValidationError(String);

impl ArbitrationOutputValidationError {
    fn new(error: anyhow::Error) -> Self {
        Self(error.to_string())
    }
}

#[async_trait]
pub trait ArbitrationEvaluator: Send + Sync {
    async fn propose(
        &self,
        context: &FrozenArbitrationContext,
    ) -> anyhow::Result<ArbitrationProposal>;

    async fn verify(
        &self,
        context: &FrozenArbitrationContext,
        proposal: &ArbitrationProposal,
    ) -> anyhow::Result<ArbitrationVerification>;
}

pub struct LlmArbitrationEvaluator {
    caller: Arc<StructuredJsonCaller>,
    prompts: Arc<PromptRegistry>,
    confidence_threshold: f64,
    context_window: usize,
    max_tokens: u32,
}

impl LlmArbitrationEvaluator {
    pub fn new(
        caller: Arc<StructuredJsonCaller>,
        prompts: Arc<PromptRegistry>,
        llm: &LlmChatConfig,
        confidence_threshold: f64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            caller,
            prompts,
            confidence_threshold,
            context_window: llm.context_window,
            max_tokens: llm.max_tokens,
        })
    }

    async fn call<T, V>(
        &self,
        prompt_name: &str,
        payload: &impl Serialize,
        validate: V,
    ) -> anyhow::Result<T>
    where
        V: FnMut(Value) -> anyhow::Result<T>,
    {
        let system_prompt = self
            .prompts
            .render(
                prompt_name,
                minijinja::context! {
                    confidence_threshold => self.confidence_threshold,
                },
            )
            .with_context(|| format!("渲染仲裁 prompt 失败: {prompt_name}"))?;
        let user_text = serde_json::to_string_pretty(payload)?;
        let messages = vec![SessionTurnMessage::user_text(user_text)];
        let context_window = self.context_window;
        let max_tokens = self.max_tokens;
        self.caller
            .generate_json_streaming_validated_with_guarded_attempts(
                system_prompt,
                messages,
                BufferedProviderRuntime::new(ProviderRuntimeFallbackScope::new_root()),
                validate,
                |_, _, _| {},
                move |system, attempt_messages| {
                    ensure_compaction_request_within_context_window(
                        system,
                        attempt_messages,
                        context_window,
                        max_tokens,
                    )
                    .context("完整仲裁上下文超过 maintainer.llm.context_window")
                },
            )
            .await
    }
}

#[derive(Serialize)]
struct VerificationPayload<'a> {
    frozen_context: &'a FrozenArbitrationContext,
    proposal: &'a ArbitrationProposal,
}

#[async_trait]
impl ArbitrationEvaluator for LlmArbitrationEvaluator {
    async fn propose(
        &self,
        context: &FrozenArbitrationContext,
    ) -> anyhow::Result<ArbitrationProposal> {
        self.call(PROPOSAL_PROMPT, context, |value| {
            let proposal: ArbitrationProposal = serde_json::from_value(value)?;
            validate_proposal(context, &proposal).map_err(ArbitrationOutputValidationError::new)?;
            Ok(proposal)
        })
        .await
    }

    async fn verify(
        &self,
        context: &FrozenArbitrationContext,
        proposal: &ArbitrationProposal,
    ) -> anyhow::Result<ArbitrationVerification> {
        self.call(
            VERIFICATION_PROMPT,
            &VerificationPayload {
                frozen_context: context,
                proposal,
            },
            |value| {
                let verification: ArbitrationVerification = serde_json::from_value(value)?;
                validate_verification(context, proposal, &verification)
                    .map_err(ArbitrationOutputValidationError::new)?;
                Ok(verification)
            },
        )
        .await
    }
}

pub(super) fn validate_proposal(
    context: &FrozenArbitrationContext,
    proposal: &ArbitrationProposal,
) -> anyhow::Result<()> {
    if !proposal.confidence.is_finite() || !(0.0..=1.0).contains(&proposal.confidence) {
        anyhow::bail!("proposal confidence 必须在 [0,1]");
    }
    if proposal.conclusion.trim().is_empty() || proposal.reasoning.trim().is_empty() {
        anyhow::bail!("proposal conclusion/reasoning 不能为空");
    }
    let direct_ids: Vec<ClaimId> = context
        .direct_claims
        .iter()
        .map(|claim| claim.id.clone())
        .collect();
    if proposal.resolution_type == ResolutionType::Unresolved {
        if proposal.resolution_basis != ResolutionBasis::InsufficientEvidence {
            anyhow::bail!("unresolved proposal 必须使用 insufficient_evidence basis");
        }
        if !proposal.claim_assessments.is_empty() {
            anyhow::bail!("unresolved proposal 不能包含 Claim 修改建议");
        }
    } else {
        if proposal.resolution_basis == ResolutionBasis::InsufficientEvidence {
            anyhow::bail!("resolved proposal 不能使用 insufficient_evidence basis");
        }
        if proposal.claim_assessments.is_empty() {
            anyhow::bail!("resolved proposal 必须完整且唯一覆盖全部直接 Claim");
        }
        validate_assessments(&direct_ids, &proposal.claim_assessments)?;
    }
    let visible = visible_evidence_ids(context);
    let evidence_refs: BTreeSet<&str> = proposal.evidence_refs.iter().map(String::as_str).collect();
    if evidence_refs.len() != proposal.evidence_refs.len() {
        anyhow::bail!("proposal evidence_refs 不能包含重复 ID");
    }
    let invalid: Vec<&str> = proposal
        .evidence_refs
        .iter()
        .map(String::as_str)
        .filter(|id| !visible.contains(*id))
        .collect();
    if !invalid.is_empty() {
        anyhow::bail!(
            "proposal evidence_refs 包含输入外 ID: {}",
            invalid.join(", ")
        );
    }
    let missing_direct: Vec<&str> = direct_ids
        .iter()
        .map(ClaimId::as_str)
        .filter(|id| !evidence_refs.contains(*id))
        .collect();
    if !missing_direct.is_empty() {
        anyhow::bail!(
            "proposal evidence_refs 必须覆盖全部直接 Claim，缺少: {}",
            missing_direct.join(", ")
        );
    }
    Ok(())
}

pub(super) fn validate_verification(
    context: &FrozenArbitrationContext,
    proposal: &ArbitrationProposal,
    verification: &ArbitrationVerification,
) -> anyhow::Result<()> {
    if !verification.confidence.is_finite() || !(0.0..=1.0).contains(&verification.confidence) {
        anyhow::bail!("verification confidence 必须在 [0,1]");
    }
    if verification.reasoning.trim().is_empty() {
        anyhow::bail!("verification reasoning 不能为空");
    }
    if verification.verdict == VerificationVerdict::Unresolved {
        if !verification.claim_assessments.is_empty() {
            anyhow::bail!("unresolved verification 不能包含逐 Claim 建议");
        }
    } else {
        if proposal.resolution_type == ResolutionType::Unresolved {
            anyhow::bail!("unresolved proposal 不能被 verification approve");
        }
        let expected: BTreeSet<ClaimId> = context
            .direct_claims
            .iter()
            .map(|claim| claim.id.clone())
            .collect();
        let actual: BTreeSet<ClaimId> = verification
            .claim_assessments
            .iter()
            .map(|assessment| assessment.claim_id.clone())
            .collect();
        if actual.len() != verification.claim_assessments.len() || actual != expected {
            anyhow::bail!("approved verification 必须完整且唯一覆盖全部直接 Claim");
        }
        if verification
            .claim_assessments
            .iter()
            .any(|assessment| assessment.reason.trim().is_empty())
        {
            anyhow::bail!("verification 的每条 Claim assessment reason 不能为空");
        }
    }
    Ok(())
}

fn visible_evidence_ids(context: &FrozenArbitrationContext) -> BTreeSet<&str> {
    let mut visible = BTreeSet::new();
    visible.insert(context.dispute.id.as_str());
    for claim in context
        .direct_claims
        .iter()
        .chain(context.source_claims.iter())
    {
        visible.insert(claim.id.as_str());
    }
    for policy in &context.policies {
        visible.insert(policy.id.as_str());
    }
    for candidate in &context.router_candidate_claims {
        visible.insert(candidate.claim.id.as_str());
    }
    for dispute in &context.router_disputes {
        visible.insert(dispute.id.as_str());
    }
    for resolution in &context.prior_resolutions {
        visible.insert(resolution.resolution.resolution_id.as_str());
    }
    visible
}

pub fn lease_base_duration(llm: &LlmChatConfig) -> anyhow::Result<Duration> {
    let attempts = u64::from(llm.retry_count).saturating_add(1);
    let provider = Duration::from_secs(llm.timeout_secs)
        .checked_mul(u32::try_from(attempts).unwrap_or(u32::MAX))
        .ok_or_else(|| anyhow::anyhow!("maintainer.llm lease 基础时长溢出"))?;
    let mut backoff = Duration::ZERO;
    for retry in 0..llm.retry_count {
        let multiplier = 1_u64.checked_shl(retry.min(63)).unwrap_or(u64::MAX);
        let millis = llm
            .retry_base_delay_ms
            .saturating_mul(multiplier)
            .min(llm.retry_max_delay_ms);
        backoff = backoff.saturating_add(Duration::from_millis(millis));
    }
    Ok(provider.saturating_add(backoff))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;
    use crate::api::{
        ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
        ProviderStreamFailure, ProviderStreamOutputMode, SessionTurnMessage,
    };
    use crate::claim::{
        AgentId, Claim, ClaimStatus, Confidence, Dispute, DisputeStatus, ResolutionBasis,
        ResolutionType,
    };

    struct RecordingProvider {
        requests: Mutex<Vec<ProviderRequest>>,
    }

    struct DelayedFallbackProvider {
        requests: Mutex<Vec<ProviderRequest>>,
    }

    struct SequencedProvider {
        requests: Mutex<Vec<ProviderRequest>>,
        responses: Mutex<VecDeque<String>>,
    }

    #[async_trait]
    impl ProviderAdapter for RecordingProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            Ok(ProviderResponse {
                assistant_message: SessionTurnMessage::assistant_text(r#"{"ok":true}"#),
                stop: ProviderStop::Done,
            })
        }
    }

    #[async_trait]
    impl ProviderAdapter for DelayedFallbackProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            let stream = request.stream;
            self.requests.lock().await.push(request);
            if stream {
                tokio::time::sleep(Duration::from_secs(2)).await;
                return Err(ProviderStreamFailure::new("stream timed out").into());
            }
            Ok(ProviderResponse {
                assistant_message: SessionTurnMessage::assistant_text(r#"{"ok":true}"#),
                stop: ProviderStop::Done,
            })
        }
    }

    #[async_trait]
    impl ProviderAdapter for SequencedProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            let response = self
                .responses
                .lock()
                .await
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("缺少测试 provider response"))?;
            Ok(ProviderResponse {
                assistant_message: SessionTurnMessage::assistant_text(response),
                stop: ProviderStop::Done,
            })
        }
    }

    fn frozen_context() -> FrozenArbitrationContext {
        let holder = AgentId::new("agent-a").unwrap();
        let claim = Claim {
            id: "claim_11111111".parse().unwrap(),
            name: "current-path".into(),
            statement: "use the current path".into(),
            scope: "example / current-path".into(),
            holder: holder.clone(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "current evidence".into(),
        };
        FrozenArbitrationContext {
            generated_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            dispute: Dispute {
                id: "dispute_11111111".parse().unwrap(),
                name: "current-path dispute".into(),
                reporter_agent_id: holder,
                claims: vec![claim.id.clone()],
                summary: "determine the current path".into(),
                status: DisputeStatus::Open,
                created_at: "2026-08-01T01:00:00Z".parse().unwrap(),
                resolved_at: None,
            },
            direct_claims: vec![claim],
            source_claims: Vec::new(),
            policies: Vec::new(),
            router_candidate_claims: Vec::new(),
            router_disputes: Vec::new(),
            prior_resolutions: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn evaluator_with_responses(
        responses: Vec<String>,
    ) -> (LlmArbitrationEvaluator, Arc<SequencedProvider>) {
        let provider = Arc::new(SequencedProvider {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        });
        let caller = Arc::new(StructuredJsonCaller::new(
            provider.clone(),
            512,
            1,
            Duration::ZERO,
            Duration::ZERO,
        ));
        let llm = LlmChatConfig {
            max_tokens: 512,
            context_window: 32_000,
            retry_count: 1,
            ..LlmChatConfig::default()
        };
        let evaluator = LlmArbitrationEvaluator::new(
            caller,
            Arc::new(PromptRegistry::bundled().unwrap()),
            &llm,
            0.9,
        )
        .unwrap();
        (evaluator, provider)
    }

    fn invalid_resolved_proposal(claim_id: &str) -> String {
        json!({
            "resolution_type": "conflict_resolved",
            "resolution_basis": "insufficient_evidence",
            "conclusion": "resolved without enough evidence",
            "claim_assessments": [{
                "claim_id": claim_id,
                "recommended_status": "active",
                "assessment": "keep current",
                "recommended_scope": null,
                "recommended_statement": null,
                "reason": "current evidence"
            }],
            "confidence": 0.95,
            "evidence_refs": [claim_id],
            "missing_evidence": [],
            "human_review_reason": null,
            "reasoning": "first attempt"
        })
        .to_string()
    }

    #[test]
    fn lease_base_duration_covers_attempts_and_backoff() {
        let config = LlmChatConfig {
            timeout_secs: 10,
            retry_count: 2,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 1_000,
            ..LlmChatConfig::default()
        };
        assert_eq!(
            lease_base_duration(&config).unwrap(),
            Duration::from_millis(30_300)
        );
    }

    #[tokio::test]
    async fn arbitration_uses_main_buffered_streaming_path() {
        let provider = Arc::new(RecordingProvider {
            requests: Mutex::new(Vec::new()),
        });
        let caller = Arc::new(StructuredJsonCaller::new(
            provider.clone(),
            512,
            0,
            Duration::ZERO,
            Duration::ZERO,
        ));
        let evaluator = LlmArbitrationEvaluator::new(
            caller,
            Arc::new(PromptRegistry::bundled().unwrap()),
            &LlmChatConfig::default(),
            0.9,
        )
        .unwrap();

        let value: serde_json::Value = evaluator
            .call(PROPOSAL_PROMPT, &json!({"input": "test"}), Ok)
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert!(requests[0].stream);
        assert_eq!(
            requests[0].stream_output_mode,
            ProviderStreamOutputMode::Buffered
        );
        assert_eq!(requests[0].retry_count_override, None);
        assert!(requests[0].runtime_chain_id.is_some());
        assert!(requests[0].runtime_fallback_scope.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn streaming_failure_can_fallback_after_the_old_phase_deadline() {
        let provider = Arc::new(DelayedFallbackProvider {
            requests: Mutex::new(Vec::new()),
        });
        let caller = Arc::new(StructuredJsonCaller::new(
            provider.clone(),
            512,
            0,
            Duration::ZERO,
            Duration::ZERO,
        ));
        let llm = LlmChatConfig {
            timeout_secs: 1,
            retry_count: 0,
            ..LlmChatConfig::default()
        };
        let evaluator = LlmArbitrationEvaluator::new(
            caller,
            Arc::new(PromptRegistry::bundled().unwrap()),
            &llm,
            0.9,
        )
        .unwrap();

        let value: serde_json::Value = evaluator
            .call(PROPOSAL_PROMPT, &json!({"input": "test"}), Ok)
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
    }

    #[tokio::test]
    async fn proposal_protocol_error_is_corrected_within_structured_retry_budget() {
        let context = frozen_context();
        let claim_id = context.direct_claims[0].id.as_str();
        let invalid = invalid_resolved_proposal(claim_id);
        let corrected = json!({
            "resolution_type": "unresolved",
            "resolution_basis": "insufficient_evidence",
            "conclusion": "more evidence is required",
            "claim_assessments": [],
            "confidence": 0.4,
            "evidence_refs": [claim_id],
            "missing_evidence": ["current baseline confirmation"],
            "human_review_reason": null,
            "reasoning": "corrected attempt"
        })
        .to_string();
        let (evaluator, provider) = evaluator_with_responses(vec![invalid, corrected]);

        let proposal = evaluator.propose(&context).await.unwrap();

        assert_eq!(proposal.resolution_type, ResolutionType::Unresolved);
        assert_eq!(
            proposal.resolution_basis,
            ResolutionBasis::InsufficientEvidence
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].messages.len(), 2);
        let correction = match &requests[1].messages[1].content[0] {
            crate::api::SessionTurnContentBlock::Text { text } => text,
            _ => panic!("retry correction must be text"),
        };
        assert!(correction.contains("resolved proposal 不能使用 insufficient_evidence basis"));
    }

    #[tokio::test]
    async fn exhausted_proposal_protocol_error_remains_classified_as_invalid() {
        let context = frozen_context();
        let invalid = invalid_resolved_proposal(context.direct_claims[0].id.as_str());
        let (evaluator, provider) = evaluator_with_responses(vec![invalid.clone(), invalid]);

        let error = evaluator.propose(&context).await.unwrap_err();

        assert!(error
            .downcast_ref::<ArbitrationOutputValidationError>()
            .is_some());
        assert_eq!(provider.requests.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn verification_protocol_error_is_corrected_within_structured_retry_budget() {
        let context = frozen_context();
        let claim_id = context.direct_claims[0].id.clone();
        let proposal: ArbitrationProposal = serde_json::from_value(json!({
            "resolution_type": "conflict_resolved",
            "resolution_basis": "direct_analysis",
            "conclusion": "use the current path",
            "claim_assessments": [{
                "claim_id": claim_id,
                "recommended_status": "active",
                "assessment": "keep current",
                "recommended_scope": null,
                "recommended_statement": null,
                "reason": "direct analysis"
            }],
            "confidence": 0.95,
            "evidence_refs": [claim_id],
            "missing_evidence": [],
            "human_review_reason": null,
            "reasoning": "proposal reasoning"
        }))
        .unwrap();
        validate_proposal(&context, &proposal).unwrap();
        let invalid = json!({
            "verdict": "approve",
            "resolution_type_agreed": true,
            "resolution_basis_agreed": true,
            "conclusion_agreed": true,
            "claim_assessments": [],
            "confidence": 0.95,
            "missing_evidence": [],
            "reasoning": "missing required assessments"
        })
        .to_string();
        let corrected = json!({
            "verdict": "unresolved",
            "resolution_type_agreed": false,
            "resolution_basis_agreed": false,
            "conclusion_agreed": false,
            "claim_assessments": [],
            "confidence": 0.4,
            "missing_evidence": ["independent confirmation"],
            "reasoning": "corrected verification"
        })
        .to_string();
        let (evaluator, provider) = evaluator_with_responses(vec![invalid, corrected]);

        let verification = evaluator.verify(&context, &proposal).await.unwrap();

        assert_eq!(verification.verdict, VerificationVerdict::Unresolved);
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].messages.len(), 2);
    }
}
