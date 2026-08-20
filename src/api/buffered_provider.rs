//! 内部任务的完整流式结果聚合与 non-streaming fallback。

use std::sync::Arc;

use crate::api::{
    ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderRuntimeChainId,
    ProviderRuntimeFallbackScope, ProviderStreamOutputMode,
};

use super::provider::ProviderStreamFailure;

#[derive(Debug, Clone)]
pub(crate) struct BufferedProviderRuntime {
    pub(crate) chain_id: ProviderRuntimeChainId,
    pub(crate) fallback_scope: ProviderRuntimeFallbackScope,
}

impl BufferedProviderRuntime {
    pub(crate) fn new(fallback_scope: ProviderRuntimeFallbackScope) -> Self {
        Self {
            chain_id: ProviderRuntimeChainId::new(),
            fallback_scope,
        }
    }
}

/// 先等待流式请求完整结束；只有可安全重放的 streaming 故障才降到 non-streaming。
pub(crate) async fn send_buffered_with_fallback(
    provider: &Arc<dyn ProviderAdapter>,
    mut request: ProviderRequest,
) -> anyhow::Result<ProviderResponse> {
    request.stream = true;
    request.stream_output_mode = ProviderStreamOutputMode::Buffered;
    let mut noop = |_event: ProviderEvent| {};
    match provider.send(request.clone(), &mut noop).await {
        Ok(response) => Ok(response),
        Err(error) if error.downcast_ref::<ProviderStreamFailure>().is_some() => {
            if let Some(chain_id) = request.runtime_chain_id {
                provider.discard_runtime_chain(chain_id).await;
            }
            request.stream = false;
            request.stream_output_mode = ProviderStreamOutputMode::Buffered;
            request.runtime_chain_id = None;
            request.runtime_fallback_scope = None;
            provider.send(request, &mut noop).await
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::api::provider::{
        ProviderNoConsumableOutput, ProviderTerminalFailure, ProviderTransport,
    };
    use crate::api::{ProviderStop, SessionTurnContentBlock, SessionTurnMessage, ToolSpec};

    struct QueueProvider {
        outcomes: Mutex<VecDeque<anyhow::Result<ProviderResponse>>>,
        requests: Mutex<Vec<ProviderRequest>>,
        discarded_chains: Mutex<Vec<ProviderRuntimeChainId>>,
    }

    #[async_trait]
    impl ProviderAdapter for QueueProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().unwrap().push(request);
            emit(ProviderEvent::AssistantTextDelta {
                text: "partial".into(),
            });
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| anyhow::bail!("missing fake outcome"))
        }

        async fn discard_runtime_chain(&self, chain_id: ProviderRuntimeChainId) {
            self.discarded_chains.lock().unwrap().push(chain_id);
        }
    }

    fn request() -> ProviderRequest {
        ProviderRequest {
            system_prompt: "system".into(),
            messages: vec![SessionTurnMessage::user_text("payload")],
            tools: Vec::<ToolSpec>::new(),
            max_tokens: 128,
            stream: true,
            stream_output_mode: ProviderStreamOutputMode::Buffered,
            runtime_chain_id: Some(ProviderRuntimeChainId::new()),
            runtime_fallback_scope: Some(ProviderRuntimeFallbackScope::new_root()),
            recovery_interrupt: None,
            retry_count_override: None,
        }
    }

    fn response() -> ProviderResponse {
        ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text("done")],
                provider_replay: None,
            },
            stop: ProviderStop::Done,
        }
    }

    #[tokio::test]
    async fn stream_failure_discards_partial_and_falls_back_once() {
        let provider = Arc::new(QueueProvider {
            outcomes: Mutex::new(VecDeque::from([
                Err(ProviderStreamFailure::new("broken stream").into()),
                Ok(response()),
            ])),
            requests: Mutex::new(Vec::new()),
            discarded_chains: Mutex::new(Vec::new()),
        });
        let erased: Arc<dyn ProviderAdapter> = provider.clone();

        let result = send_buffered_with_fallback(&erased, request())
            .await
            .unwrap();

        assert_eq!(result.stop, ProviderStop::Done);
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
        assert!(requests[1].runtime_chain_id.is_none());
        assert!(requests[1].runtime_fallback_scope.is_none());
        assert_eq!(provider.discarded_chains.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn terminal_failure_never_falls_back() {
        let provider = Arc::new(QueueProvider {
            outcomes: Mutex::new(VecDeque::from([Err(ProviderTerminalFailure::new(
                "invalid request",
            )
            .into())])),
            requests: Mutex::new(Vec::new()),
            discarded_chains: Mutex::new(Vec::new()),
        });
        let erased: Arc<dyn ProviderAdapter> = provider.clone();

        let error = send_buffered_with_fallback(&erased, request())
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<ProviderTerminalFailure>().is_some());
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
        assert!(provider.discarded_chains.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn completed_no_consumable_output_never_changes_transport() {
        let provider = Arc::new(QueueProvider {
            outcomes: Mutex::new(VecDeque::from([Err(ProviderNoConsumableOutput::new(
                ProviderTransport::ResponsesSse,
                "reasoning only",
            )
            .into())])),
            requests: Mutex::new(Vec::new()),
            discarded_chains: Mutex::new(Vec::new()),
        });
        let erased: Arc<dyn ProviderAdapter> = provider.clone();

        let error = send_buffered_with_fallback(&erased, request())
            .await
            .unwrap_err();

        let no_output = error.downcast_ref::<ProviderNoConsumableOutput>().unwrap();
        assert_eq!(no_output.transport(), ProviderTransport::ResponsesSse);
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
        assert!(provider.discarded_chains.lock().unwrap().is_empty());
    }
}
