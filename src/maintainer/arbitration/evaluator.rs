//! proposal / verification 的 provider-neutral 结构化模型调用。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use serde::Serialize;

use crate::api::{
    ensure_compaction_request_within_context_window, BufferedProviderRuntime,
    ProviderRuntimeFallbackScope, SessionTurnMessage, StructuredJsonCaller,
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
        self.caller
            .generate_json_streaming_validated_with_guarded_attempts(
                system_prompt,
                messages,
                BufferedProviderRuntime::new(ProviderRuntimeFallbackScope::new_root()),
                |value| serde_json::from_value(value).map_err(anyhow::Error::from),
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
    use async_trait::async_trait;
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;
    use crate::api::{
        ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
        ProviderStreamFailure, ProviderStreamOutputMode, SessionTurnMessage,
    };

    struct RecordingProvider {
        requests: Mutex<Vec<ProviderRequest>>,
    }

    struct DelayedFallbackProvider {
        requests: Mutex<Vec<ProviderRequest>>,
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
            .call(PROPOSAL_PROMPT, &json!({"input": "test"}))
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
            .call(PROPOSAL_PROMPT, &json!({"input": "test"}))
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
    }
}
