//! proposal / verification 的 provider-neutral 结构化模型调用。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use serde::Serialize;

use crate::api::{
    ensure_compaction_request_within_context_window, SessionTurnMessage,
    StructuredJsonAttemptRequest, StructuredJsonCaller,
};
use crate::config::LlmChatConfig;
use crate::prompt::PromptRegistry;

use super::types::{ArbitrationProposal, ArbitrationVerification, FrozenArbitrationContext};

const PROPOSAL_PROMPT: &str = "maintainer_arbitration_proposal";
const VERIFICATION_PROMPT: &str = "maintainer_arbitration_verification";

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
    phase_timeout: Duration,
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
            phase_timeout: phase_timeout(llm)?,
        })
    }

    pub fn phase_timeout(&self) -> Duration {
        self.phase_timeout
    }

    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        prompt_name: &str,
        payload: &impl Serialize,
    ) -> anyhow::Result<T> {
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
        let call = self.caller.generate_json_validated_with_guarded_attempts(
            StructuredJsonAttemptRequest::retryable_provider(system_prompt, messages),
            |value| serde_json::from_value(value).map_err(anyhow::Error::from),
            |_, _, _| {},
            |_| std::future::ready(()),
            move |system, attempt_messages| {
                ensure_compaction_request_within_context_window(
                    system,
                    attempt_messages,
                    context_window,
                    max_tokens,
                )
                .context("完整仲裁上下文超过 maintainer.llm.context_window")
            },
        );
        tokio::time::timeout(self.phase_timeout, call)
            .await
            .map_err(|_| anyhow::anyhow!("仲裁模型阶段超时: {:?}", self.phase_timeout))?
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
        self.call(PROPOSAL_PROMPT, context).await
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
        )
        .await
    }
}

pub fn phase_timeout(llm: &LlmChatConfig) -> anyhow::Result<Duration> {
    let attempts = u64::from(llm.retry_count).saturating_add(1);
    let provider = Duration::from_secs(llm.timeout_secs)
        .checked_mul(u32::try_from(attempts).unwrap_or(u32::MAX))
        .ok_or_else(|| anyhow::anyhow!("maintainer.llm phase timeout 溢出"))?;
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
    use super::*;

    #[test]
    fn phase_timeout_covers_attempts_and_backoff() {
        let config = LlmChatConfig {
            timeout_secs: 10,
            retry_count: 2,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 1_000,
            ..LlmChatConfig::default()
        };
        assert_eq!(
            phase_timeout(&config).unwrap(),
            Duration::from_millis(30_300)
        );
    }
}
