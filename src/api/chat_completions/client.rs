use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rand::Rng;

use crate::api::endpoint::{resolve_llm_endpoint, LlmEndpointKind};
use crate::api::llm_http::{read_llm_error_body, LlmHttpError, LlmHttpPhase};
use crate::api::ProviderRecoveryInterrupt;

use super::protocol::{ChatCompletionRequest, ChatCompletionResponse};
use super::redact_chat_error_body;
use super::streaming::{drain_sse_frames, sse_frame_data, ChatStreamAccumulator};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatStreamEvent {
    ContentDelta { text: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ChatCompletionsError {
    #[error("{0}")]
    Http(#[from] LlmHttpError),
    #[error("LLM provider authentication failed (401): {0}")]
    Auth(String),
    #[error("LLM provider returned HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("LLM response JSON parse failed: {0}")]
    ResponseJson(#[from] serde_json::Error),
    #[error("Chat Completions endpoint 配置无效: {0}")]
    InvalidEndpoint(String),
    #[error("Chat Completions 输出不符合预期: {reason}; raw={raw}")]
    OutputShape { reason: String, raw: String },
    #[error("Chat Completions streaming 响应损坏或未完整结束: {reason}")]
    StreamFailure { reason: String, raw: String },
    #[error("Chat Completions upstream failed: code={code:?}, {message}")]
    Failed {
        code: Option<String>,
        message: String,
    },
    #[error("Chat Completions recovery interrupted")]
    RecoveryInterrupted,
    #[error("Chat Completions request preparation failed: {reason}")]
    RequestPreparation { reason: String },
}

pub struct ChatCompletionsClient {
    http: reqwest::Client,
    endpoint: Arc<String>,
    api_key: Arc<String>,
    retry_count: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    timeout: Duration,
}

impl ChatCompletionsClient {
    pub fn new(
        endpoint: String,
        api_key: String,
        timeout: Duration,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Result<Self, ChatCompletionsError> {
        let endpoint = resolve_llm_endpoint(&endpoint, LlmEndpointKind::OpenAiChatCompletions)
            .map_err(|error| ChatCompletionsError::InvalidEndpoint(error.to_string()))?;
        let http = crate::http_client_builder_for_endpoint(&endpoint)
            .timeout(timeout)
            .build()
            .map_err(|error| {
                ChatCompletionsError::Http(LlmHttpError::new(
                    error,
                    LlmHttpPhase::BuildClient,
                    Some(timeout),
                ))
            })?;
        Ok(Self {
            http,
            endpoint: Arc::new(endpoint),
            api_key: Arc::new(api_key),
            retry_count,
            retry_base_delay,
            retry_max_delay,
            timeout,
        })
    }

    fn http_error(&self, error: reqwest::Error, phase: LlmHttpPhase) -> ChatCompletionsError {
        ChatCompletionsError::Http(LlmHttpError::new(error, phase, Some(self.timeout)))
    }

    pub(crate) fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub async fn send(
        &self,
        request: &ChatCompletionRequest,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        self.send_with_retry_count(request, self.retry_count, emit)
            .await
    }

    pub async fn send_with_retry_count(
        &self,
        request: &ChatCompletionRequest,
        retry_count: u32,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        self.send_with_retry_count_and_mode(request, retry_count, false, emit)
            .await
    }

    pub(crate) async fn send_with_retry_count_and_mode(
        &self,
        request: &ChatCompletionRequest,
        retry_count: u32,
        retry_after_partial: bool,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        self.send_with_retry_count_and_mode_and_interrupt(
            request,
            retry_count,
            retry_after_partial,
            None,
            emit,
        )
        .await
    }

    pub(crate) async fn send_with_retry_count_and_mode_and_interrupt(
        &self,
        request: &ChatCompletionRequest,
        retry_count: u32,
        retry_after_partial: bool,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        let mut noop = |_| Ok(());
        self.send_with_retry_count_and_mode_and_interrupt_and_start_hook(
            request,
            retry_count,
            retry_after_partial,
            recovery_interrupt,
            emit,
            &mut noop,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "请求发送边界需同时携带 retry、恢复中断与 WAL start hook"
    )]
    pub(crate) async fn send_with_retry_count_and_mode_and_interrupt_and_start_hook(
        &self,
        request: &ChatCompletionRequest,
        retry_count: u32,
        retry_after_partial: bool,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
        request_started: &mut (dyn FnMut(bool) -> Result<(), ChatCompletionsError> + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        if request.stream {
            self.send_streaming_with_retry(
                request,
                retry_count,
                retry_after_partial,
                recovery_interrupt,
                emit,
                request_started,
            )
            .await
        } else {
            self.send_json_with_retry(request, retry_count, recovery_interrupt, request_started)
                .await
        }
    }

    async fn send_json_with_retry(
        &self,
        request: &ChatCompletionRequest,
        retry_count: u32,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        request_started: &mut (dyn FnMut(bool) -> Result<(), ChatCompletionsError> + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        let mut last_retryable = None;
        let mut previous_attempt_ambiguous = false;
        for attempt in 0..=retry_count {
            ensure_recovery_active(recovery_interrupt)?;
            match self
                .send_json_once(request, previous_attempt_ambiguous, request_started)
                .await
            {
                Ok(value) => return Ok(value),
                Err(e) if is_retryable(&e) && attempt < retry_count => {
                    previous_attempt_ambiguous = !matches!(
                        &e,
                        ChatCompletionsError::Auth(_) | ChatCompletionsError::Status { .. }
                    );
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Chat Completions 调用失败，{}ms 后重试 ({}/{}): {}",
                        backoff.as_millis(),
                        attempt + 1,
                        retry_count,
                        e
                    );
                    last_retryable = Some(e);
                    wait_for_backoff(backoff, recovery_interrupt).await?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(
            last_retryable.unwrap_or_else(|| ChatCompletionsError::OutputShape {
                reason: "retry loop 未返回结果".into(),
                raw: String::new(),
            }),
        )
    }

    async fn send_json_once(
        &self,
        request: &ChatCompletionRequest,
        previous_attempt_ambiguous: bool,
        request_started: &mut (dyn FnMut(bool) -> Result<(), ChatCompletionsError> + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        let pending = self
            .http
            .post(self.endpoint.as_str())
            .bearer_auth(self.api_key.as_str())
            .header("content-type", "application/json")
            .json(request);
        request_started(previous_attempt_ambiguous)?;
        let resp = pending
            .send()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::SendRequest))?;
        response_json(resp, self.timeout).await
    }

    async fn send_streaming_with_retry(
        &self,
        request: &ChatCompletionRequest,
        retry_count: u32,
        retry_after_partial: bool,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
        request_started: &mut (dyn FnMut(bool) -> Result<(), ChatCompletionsError> + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        let mut last_retryable = None;
        let mut previous_attempt_ambiguous = false;
        for attempt in 0..=retry_count {
            ensure_recovery_active(recovery_interrupt)?;
            let mut emitted = false;
            let result = {
                let mut tracking_emit = |event| {
                    emitted = true;
                    emit(event);
                };
                self.send_streaming_once(
                    request,
                    previous_attempt_ambiguous,
                    &mut tracking_emit,
                    request_started,
                )
                .await
            };
            match result {
                Ok(value) => return Ok(value),
                Err(e)
                    if (!emitted || retry_after_partial)
                        && is_retryable(&e)
                        && attempt < retry_count =>
                {
                    previous_attempt_ambiguous = !matches!(
                        &e,
                        ChatCompletionsError::Auth(_) | ChatCompletionsError::Status { .. }
                    );
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Chat Completions stream 调用失败，{}ms 后重试 ({}/{}): {}",
                        backoff.as_millis(),
                        attempt + 1,
                        retry_count,
                        e
                    );
                    last_retryable = Some(e);
                    wait_for_backoff(backoff, recovery_interrupt).await?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(
            last_retryable.unwrap_or_else(|| ChatCompletionsError::OutputShape {
                reason: "stream retry loop 未返回结果".into(),
                raw: String::new(),
            }),
        )
    }

    async fn send_streaming_once(
        &self,
        request: &ChatCompletionRequest,
        previous_attempt_ambiguous: bool,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
        request_started: &mut (dyn FnMut(bool) -> Result<(), ChatCompletionsError> + Send),
    ) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        let pending = self
            .http
            .post(self.endpoint.as_str())
            .bearer_auth(self.api_key.as_str())
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .json(request);
        request_started(previous_attempt_ambiguous)?;
        let resp = pending
            .send()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::SendRequest))?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(ChatCompletionsError::Auth(redact_chat_error_body(&body)));
        }
        if !status.is_success() {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(ChatCompletionsError::Status {
                status: status.as_u16(),
                body: redact_chat_error_body(&body),
            });
        }

        let mut sse_buffer = Vec::new();
        let mut accumulator = ChatStreamAccumulator::default();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| ChatCompletionsError::StreamFailure {
                reason: self
                    .http_error(error, LlmHttpPhase::ReadStreamBody)
                    .to_string(),
                raw: String::new(),
            })?;
            sse_buffer.extend_from_slice(&chunk);
            for frame in drain_sse_frames(&mut sse_buffer) {
                if let Some(data) = sse_frame_data(&frame)? {
                    accumulator.apply_frame(&data, emit)?;
                }
            }
        }
        if !sse_buffer.is_empty() {
            if let Some(data) = sse_frame_data(&sse_buffer)? {
                accumulator.apply_frame(&data, emit)?;
            }
        }
        accumulator.finish()
    }
}

async fn response_json(
    resp: reqwest::Response,
    timeout: Duration,
) -> Result<ChatCompletionResponse, ChatCompletionsError> {
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        let body = read_llm_error_body(resp, timeout).await;
        return Err(ChatCompletionsError::Auth(redact_chat_error_body(&body)));
    }
    if !status.is_success() {
        let body = read_llm_error_body(resp, timeout).await;
        return Err(ChatCompletionsError::Status {
            status: status.as_u16(),
            body: redact_chat_error_body(&body),
        });
    }
    let body = resp.text().await.map_err(|error| {
        ChatCompletionsError::Http(LlmHttpError::new(
            error,
            LlmHttpPhase::ReadResponseBody,
            Some(timeout),
        ))
    })?;
    serde_json::from_str(&body).map_err(ChatCompletionsError::ResponseJson)
}

fn is_retryable(error: &ChatCompletionsError) -> bool {
    match error {
        ChatCompletionsError::Http(error) => error.is_retryable(),
        ChatCompletionsError::Status { status, .. } => *status == 429 || *status >= 500,
        ChatCompletionsError::Failed { code, .. } => code.as_deref().is_some_and(|code| {
            matches!(
                code,
                "rate_limit_error"
                    | "rate_limit_exceeded"
                    | "server_error"
                    | "api_error"
                    | "overloaded_error"
                    | "internal_server_error"
                    | "service_unavailable"
                    | "temporarily_unavailable"
            )
        }),
        ChatCompletionsError::StreamFailure { .. } => true,
        ChatCompletionsError::Auth(_)
        | ChatCompletionsError::ResponseJson(_)
        | ChatCompletionsError::InvalidEndpoint(_)
        | ChatCompletionsError::OutputShape { .. }
        | ChatCompletionsError::RecoveryInterrupted
        | ChatCompletionsError::RequestPreparation { .. } => false,
    }
}

fn ensure_recovery_active(
    recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
) -> Result<(), ChatCompletionsError> {
    if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
        return Err(ChatCompletionsError::RecoveryInterrupted);
    }
    Ok(())
}

async fn wait_for_backoff(
    delay: Duration,
    recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
) -> Result<(), ChatCompletionsError> {
    if delay.is_zero() {
        return ensure_recovery_active(recovery_interrupt);
    }
    match recovery_interrupt {
        Some(interrupt) => {
            tokio::select! {
                _ = tokio::time::sleep(delay) => Ok(()),
                _ = interrupt.cancelled() => Err(ChatCompletionsError::RecoveryInterrupted),
            }
        }
        None => {
            tokio::time::sleep(delay).await;
            Ok(())
        }
    }
}

pub(crate) fn is_stream_failure(error: &ChatCompletionsError) -> bool {
    matches!(error, ChatCompletionsError::StreamFailure { .. })
        || matches!(error, ChatCompletionsError::Http(error) if error.is_retryable())
        || matches!(error, ChatCompletionsError::Status { status, .. } if *status == 429 || *status >= 500)
        || matches!(error, ChatCompletionsError::Failed { code: Some(code), .. } if matches!(code.as_str(),
            "rate_limit_error" | "rate_limit_exceeded" | "server_error" | "api_error"
                | "overloaded_error" | "internal_server_error" | "service_unavailable"
                | "temporarily_unavailable"))
}

fn compute_backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(10)).unwrap_or(u32::MAX);
    let raw = base.saturating_mul(factor);
    let capped = raw.min(max);
    let center = u64::try_from(capped.as_millis()).unwrap_or(u64::MAX);
    if center == 0 {
        return Duration::ZERO;
    }
    let half = center / 2;
    let low = center.saturating_sub(half);
    let high = center.saturating_add(half);
    let jittered = rand::thread_rng().gen_range(low..=high);
    let max_ms = u64::try_from(max.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(jittered.min(max_ms))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::super::REDACTED_CHAT_PAYLOAD;
    use super::*;
    use crate::api::chat_completions::{ChatMessage, ChatToolCall};

    #[tokio::test]
    async fn chat_completions_parses_non_stream_text() {
        let body = json!({
            "choices": [{
                "message": {"role": "assistant", "content": "hello"},
                "finish_reason": "stop"
            }]
        })
        .to_string();
        let (endpoint, _requests) = spawn_server("application/json", body).await;
        let client = test_client(endpoint);

        let response = client.send(&request(false), &mut |_| {}).await.unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("hello")
        );
        assert_eq!(
            response.choices[0].finish_reason,
            Some(crate::api::ChatFinishReason::Stop)
        );
    }

    #[tokio::test]
    async fn chat_completions_treats_null_tool_calls_as_empty() {
        let body = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "hello",
                    "tool_calls": null
                },
                "finish_reason": "stop"
            }]
        })
        .to_string();
        let (endpoint, _requests) = spawn_server("application/json", body).await;
        let client = test_client(endpoint);

        let response = client.send(&request(false), &mut |_| {}).await.unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("hello")
        );
        assert!(response.choices[0].message.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn recovery_interrupt_stops_chat_retry_backoff_and_future_attempts() {
        let interrupt = ProviderRecoveryInterrupt::new();
        let cancel = interrupt.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel.cancel();
        });

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_backoff(Duration::from_secs(60), Some(&interrupt)),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert!(matches!(error, ChatCompletionsError::RecoveryInterrupted));
    }

    #[tokio::test]
    async fn cancelled_recovery_does_not_start_chat_transport() {
        let interrupt = ProviderRecoveryInterrupt::new();
        interrupt.cancel();
        let client = test_client("http://127.0.0.1:9".into());

        for stream in [false, true] {
            let error = client
                .send_with_retry_count_and_mode_and_interrupt(
                    &request(stream),
                    1,
                    false,
                    Some(&interrupt),
                    &mut |_| {},
                )
                .await
                .unwrap_err();
            assert!(matches!(error, ChatCompletionsError::RecoveryInterrupted));
        }
    }

    #[tokio::test]
    async fn chat_completions_parses_non_stream_tool_calls() {
        let body = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "file_read", "arguments": "{\"path\":\"Cargo.toml\"}"}
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        })
        .to_string();
        let (endpoint, _requests) = spawn_server("application/json", body).await;
        let client = test_client(endpoint);

        let response = client.send(&request(false), &mut |_| {}).await.unwrap();

        assert_eq!(response.choices[0].message.tool_calls[0].id, "call_1");
        assert_eq!(
            response.choices[0].finish_reason,
            Some(crate::api::ChatFinishReason::ToolCalls)
        );
    }

    #[tokio::test]
    async fn chat_completions_streams_text_delta() {
        let body = sse_response(vec![
            json!({"choices":[{"delta":{"role":"assistant"}}]}),
            json!({"choices":[{"delta":{"content":"hel"}}]}),
            json!({"choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}]}),
        ]);
        let (endpoint, _requests) = spawn_server("text/event-stream", body).await;
        let client = test_client(endpoint);
        let mut events = Vec::new();

        let response = client
            .send(&request(true), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("hello")
        );
        assert_eq!(
            events,
            vec![
                ChatStreamEvent::ContentDelta { text: "hel".into() },
                ChatStreamEvent::ContentDelta { text: "lo".into() }
            ]
        );
    }

    #[tokio::test]
    async fn chat_completions_stream_treats_null_tool_calls_as_empty_delta() {
        let body = sse_response(vec![
            json!({"choices":[{"delta":{"role":"assistant","tool_calls":null}}]}),
            json!({"choices":[{"delta":{"content":"ok","tool_calls":null},"finish_reason":"stop"}]}),
        ]);
        let (endpoint, _requests) = spawn_server("text/event-stream", body).await;
        let client = test_client(endpoint);

        let response = client.send(&request(true), &mut |_| {}).await.unwrap();

        assert_eq!(response.choices[0].message.content.as_deref(), Some("ok"));
        assert!(response.choices[0].message.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn chat_completions_stream_parses_final_usage_chunk() {
        let body = sse_response(vec![
            json!({"choices":[{"delta":{"role":"assistant"}}]}),
            json!({"choices":[], "usage": {"total_tokens": 5}}),
            json!({"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}),
            json!({
                "choices": [],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 2,
                    "total_tokens": 12
                }
            }),
        ]);
        let (endpoint, _requests) = spawn_server("text/event-stream", body).await;
        let client = test_client(endpoint);

        let response = client.send(&request(true), &mut |_| {}).await.unwrap();

        assert_eq!(
            response
                .usage
                .as_ref()
                .and_then(|usage| usage.get("total_tokens"))
                .and_then(serde_json::Value::as_u64),
            Some(12)
        );
    }

    #[tokio::test]
    async fn chat_completions_streams_tool_call_arguments() {
        let body = sse_response(vec![
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"file_read","arguments":"{\"path\""}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"Cargo.toml\"}"}}]},"finish_reason":"tool_calls"}]}),
        ]);
        let (endpoint, _requests) = spawn_server("text/event-stream", body).await;
        let client = test_client(endpoint);

        let response = client.send(&request(true), &mut |_| {}).await.unwrap();

        assert_eq!(
            response.choices[0].message.tool_calls,
            vec![ChatToolCall::function(
                "call_1",
                "file_read",
                "{\"path\":\"Cargo.toml\"}"
            )]
        );
        assert_eq!(
            response.choices[0].finish_reason,
            Some(crate::api::ChatFinishReason::ToolCalls)
        );
    }

    #[tokio::test]
    async fn chat_completions_retries_malformed_sse_json_before_visible_text() {
        let success = sse_response(vec![json!({
            "choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]
        })]);
        let (endpoint, requests) =
            spawn_body_sequence(vec!["data: {broken-json}\n\n".into(), success]).await;
        let client = ChatCompletionsClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let response = client.send(&request(true), &mut |_| {}).await.unwrap();

        assert_eq!(response.choices[0].message.content.as_deref(), Some("ok"));
        assert_eq!(requests.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn transient_sse_error_marks_the_retry_ambiguous() {
        let first = sse_response(vec![json!({
            "error": {
                "code": "server_error",
                "message": "retry later"
            }
        })]);
        let second = sse_response(vec![json!({
            "error": {
                "code": "invalid_request_error",
                "message": "invalid input"
            }
        })]);
        let (endpoint, requests) = spawn_body_sequence(vec![first, second]).await;
        let client = ChatCompletionsClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut starts = Vec::new();

        let error = client
            .send_with_retry_count_and_mode_and_interrupt_and_start_hook(
                &request(true),
                1,
                false,
                None,
                &mut |_| {},
                &mut |previous_attempt_ambiguous| {
                    starts.push(previous_attempt_ambiguous);
                    Ok(())
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ChatCompletionsError::Failed { code: Some(code), .. }
                if code == "invalid_request_error"
        ));
        assert_eq!(starts, vec![false, true]);
        assert_eq!(requests.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn chat_completions_timeout_error_names_llm_timeout() {
        let endpoint = spawn_hanging_server().await;
        let client = ChatCompletionsClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_millis(50),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let error = client
            .send(&request(false), &mut |_| {})
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("LLM request timeout after 50ms"));
        assert!(error.contains("sending LLM request"));
    }

    #[tokio::test]
    async fn chat_completions_stream_body_error_names_stream_phase() {
        let body = sse_response(vec![json!({
            "choices": [{
                "delta": {
                    "role": "assistant",
                    "content": "hello"
                }
            }]
        })]);
        let endpoint = spawn_raw_response(format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
            body.len() + 64,
            body
        ))
        .await;
        let client = test_client(endpoint);

        let error = client
            .send(&request(true), &mut |_| {})
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("LLM response body"));
        assert!(error.contains("reading LLM stream response body"));
    }

    #[tokio::test]
    async fn chat_http_errors_redact_echoed_request_payloads() {
        let media_secret = "B".repeat(300);
        let system_secret = "private-system-prompt";
        let user_secret = "private-user-prompt";
        let tool_secret = "private-tool-argument";
        for stream in [false, true] {
            let body = json!({
                "error": {
                    "code":"content_filter",
                    "message":format!("blocked: {user_secret}"),
                    "system_prompt":system_secret,
                    "tool_input":tool_secret,
                    "image":media_secret
                },
                "request": {
                    "messages": [
                        {"role":"system", "content":system_secret},
                        {"role":"user", "content":user_secret},
                        {"role":"assistant", "tool_calls":[{
                            "function":{"arguments":tool_secret}
                        }]}
                    ]
                }
            })
            .to_string();
            let endpoint = spawn_raw_response(format!(
                "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            ))
            .await;
            let client = test_client(endpoint);

            let error = client
                .send(&request(stream), &mut |_| {})
                .await
                .unwrap_err()
                .to_string();

            assert!(error.contains("content_filter"));
            assert!(error.contains(REDACTED_CHAT_PAYLOAD));
            assert!(!error.contains(system_secret));
            assert!(!error.contains(user_secret));
            assert!(!error.contains(tool_secret));
            assert!(!error.contains(&media_secret));
        }
    }

    fn request(stream: bool) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "test-model".into(),
            messages: vec![ChatMessage::user("hello")],
            reasoning_effort: None,
            tools: None,
            max_tokens: 128,
            stream,
            stream_options: None,
            temperature: None,
            top_p: None,
        }
    }

    fn test_client(endpoint: String) -> ChatCompletionsClient {
        ChatCompletionsClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
    }

    fn sse_response(events: Vec<serde_json::Value>) -> String {
        let mut body = String::new();
        for event in events {
            body.push_str("data: ");
            body.push_str(&event.to_string());
            body.push_str("\n\n");
        }
        body.push_str("data: [DONE]\n\n");
        body
    }

    async fn spawn_hanging_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let _ = socket.read(&mut buf).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_raw_response(response: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let _ = socket.read(&mut buf).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn spawn_body_sequence(bodies: Vec<String>) -> (String, tokio::task::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let expected = bodies.len();
        let handle = tokio::spawn(async move {
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = socket.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            expected
        });
        (format!("http://{addr}"), handle)
    }

    async fn spawn_server(
        content_type: &'static str,
        body: String,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let requests_for_task = requests.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let n = socket.read(&mut buf).await.unwrap();
            requests_for_task
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buf[..n]).to_string());
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (format!("http://{addr}"), requests)
    }
}
