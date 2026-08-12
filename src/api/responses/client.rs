use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rand::Rng;

use super::protocol::{reduce_response_value, ReducedResponses, ResponsesRequest};
use super::redact_responses_error_body;
use super::streaming::ResponsesSseDecoder;
use super::websocket::{ResponsesWebSocketTransport, WebSocketSendOutcome};
use crate::api::endpoint::{resolve_llm_endpoint, LlmEndpointKind};
use crate::api::evaluation_usage::{record_evaluation_request_started, record_evaluation_usage};
use crate::api::llm_http::{read_llm_error_body, LlmHttpError, LlmHttpPhase};
use crate::api::{ProviderRecoveryInterrupt, ProviderRuntimeChainId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponsesStreamEvent {
    TextDelta { text: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ResponsesError {
    #[error("{0}")]
    Http(#[from] LlmHttpError),
    #[error("LLM provider authentication failed (401): {0}")]
    Auth(String),
    #[error("LLM provider returned HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("Responses response JSON parse failed: {0}")]
    ResponseJson(#[from] serde_json::Error),
    #[error("Responses endpoint 配置无效: {0}")]
    InvalidEndpoint(String),
    #[error("Responses 输出不符合预期: {reason}")]
    OutputShape { reason: String },
    #[error("Responses streaming 响应损坏或未完整结束: {reason}")]
    StreamFailure { reason: String },
    #[error("Responses upstream failed: {message}")]
    Failed { message: String },
    #[error("Responses 返回未完成终态: {reason}")]
    Incomplete { reason: String },
    #[error("Responses recovery interrupted")]
    RecoveryInterrupted,
}

pub struct ResponsesClient {
    http: reqwest::Client,
    endpoint: Arc<String>,
    api_key: Arc<String>,
    retry_count: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    timeout: Duration,
    websocket: Option<Arc<ResponsesWebSocketTransport>>,
}

impl ResponsesClient {
    pub fn new(
        endpoint: String,
        api_key: String,
        timeout: Duration,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Result<Self, ResponsesError> {
        let builder = crate::http_client_builder().timeout(timeout);
        let http = builder.build().map_err(|error| {
            ResponsesError::Http(LlmHttpError::new(
                error,
                LlmHttpPhase::BuildClient,
                Some(timeout),
            ))
        })?;
        Ok(Self {
            http,
            endpoint: Arc::new(
                resolve_llm_endpoint(&endpoint, LlmEndpointKind::OpenAiResponses)
                    .map_err(|error| ResponsesError::InvalidEndpoint(error.to_string()))?,
            ),
            api_key: Arc::new(api_key),
            retry_count,
            retry_base_delay,
            retry_max_delay,
            timeout,
            websocket: None,
        })
    }

    pub(crate) fn with_websockets(mut self, pool_capacity: usize) -> Result<Self, ResponsesError> {
        self.websocket = Some(Arc::new(ResponsesWebSocketTransport::new(
            self.endpoint.as_str(),
            Arc::clone(&self.api_key),
            pool_capacity,
            self.timeout,
        )?));
        Ok(self)
    }

    pub(crate) async fn discard_runtime_chain(&self, chain_id: ProviderRuntimeChainId) {
        if let Some(websocket) = &self.websocket {
            websocket.discard_runtime_chain(chain_id).await;
        }
    }

    pub(crate) fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn websockets_enabled(&self) -> bool {
        self.websocket.is_some()
    }

    fn http_error(&self, error: reqwest::Error, phase: LlmHttpPhase) -> ResponsesError {
        ResponsesError::Http(LlmHttpError::new(error, phase, Some(self.timeout)))
    }

    pub async fn send(
        &self,
        request: &ResponsesRequest,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, ResponsesError> {
        self.send_with_retry_count(request, self.retry_count, emit)
            .await
    }

    pub async fn send_with_retry_count(
        &self,
        request: &ResponsesRequest,
        retry_count: u32,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, ResponsesError> {
        self.send_with_retry_count_for_runtime_chain(request, retry_count, None, None, emit)
            .await
    }

    pub(crate) async fn send_with_retry_count_for_runtime_chain(
        &self,
        request: &ResponsesRequest,
        retry_count: u32,
        runtime_chain_id: Option<ProviderRuntimeChainId>,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, ResponsesError> {
        ensure_recovery_active(recovery_interrupt)?;
        if request.stream {
            if let (Some(websocket), Some(runtime_chain_id)) = (&self.websocket, runtime_chain_id) {
                match websocket
                    .send_with_retry_count(
                        request,
                        runtime_chain_id,
                        retry_count,
                        self.retry_base_delay,
                        self.retry_max_delay,
                        recovery_interrupt,
                        emit,
                    )
                    .await?
                {
                    WebSocketSendOutcome::Response(response) => return Ok(response),
                    WebSocketSendOutcome::FallbackToHttp => {
                        ensure_recovery_active(recovery_interrupt)?;
                    }
                }
            }
            self.send_streaming_with_retry(request, retry_count, recovery_interrupt, emit)
                .await
        } else {
            self.send_json_with_retry(request, retry_count, recovery_interrupt)
                .await
        }
    }

    async fn send_json_with_retry(
        &self,
        request: &ResponsesRequest,
        retry_count: u32,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
    ) -> Result<ReducedResponses, ResponsesError> {
        let mut last_retryable = None;
        for attempt in 0..=retry_count {
            ensure_recovery_active(recovery_interrupt)?;
            match self.send_json_once(request).await {
                Ok(value) => return Ok(value),
                Err(error) if is_retryable(&error) && attempt < retry_count => {
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Responses 调用失败，{}ms 后重试 ({}/{}): {}",
                        backoff.as_millis(),
                        attempt + 1,
                        retry_count,
                        error
                    );
                    last_retryable = Some(error);
                    wait_for_backoff(backoff, recovery_interrupt).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(
            last_retryable.unwrap_or_else(|| ResponsesError::OutputShape {
                reason: "retry loop 未返回结果".into(),
            }),
        )
    }

    async fn send_json_once(
        &self,
        request: &ResponsesRequest,
    ) -> Result<ReducedResponses, ResponsesError> {
        let request_sequence = record_evaluation_request_started();
        let response = self
            .http
            .post(self.endpoint.as_str())
            .bearer_auth(self.api_key.as_str())
            .header("content-type", "application/json")
            .json(request)
            .send()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::SendRequest))?;
        let reduced = response_json(response, self.timeout).await?;
        record_evaluation_usage(
            request_sequence,
            reduced.usage.as_ref(),
            reduced.model.as_deref(),
        );
        Ok(reduced)
    }

    async fn send_streaming_with_retry(
        &self,
        request: &ResponsesRequest,
        retry_count: u32,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, ResponsesError> {
        let mut last_retryable = None;
        for attempt in 0..=retry_count {
            ensure_recovery_active(recovery_interrupt)?;
            let mut emitted_visible_text = false;
            let result = {
                let mut tracking_emit = |event| {
                    emitted_visible_text = true;
                    emit(event);
                };
                self.send_streaming_once(request, &mut tracking_emit).await
            };
            match result {
                Ok(value) => return Ok(value),
                Err(error)
                    if !emitted_visible_text && is_retryable(&error) && attempt < retry_count =>
                {
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Responses stream 调用失败，{}ms 后重试 ({}/{}): {}",
                        backoff.as_millis(),
                        attempt + 1,
                        retry_count,
                        error
                    );
                    last_retryable = Some(error);
                    wait_for_backoff(backoff, recovery_interrupt).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(
            last_retryable.unwrap_or_else(|| ResponsesError::OutputShape {
                reason: "stream retry loop 未返回结果".into(),
            }),
        )
    }

    async fn send_streaming_once(
        &self,
        request: &ResponsesRequest,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, ResponsesError> {
        let request_sequence = record_evaluation_request_started();
        let response = self
            .http
            .post(self.endpoint.as_str())
            .bearer_auth(self.api_key.as_str())
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .json(request)
            .send()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::SendRequest))?;
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let body = read_llm_error_body(response, self.timeout).await;
            return Err(ResponsesError::Auth(redact_responses_error_body(&body)));
        }
        if !status.is_success() {
            let body = read_llm_error_body(response, self.timeout).await;
            return Err(ResponsesError::Status {
                status: status.as_u16(),
                body: redact_responses_error_body(&body),
            });
        }

        let mut decoder = ResponsesSseDecoder::default();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| ResponsesError::StreamFailure {
                reason: self
                    .http_error(error, LlmHttpPhase::ReadStreamBody)
                    .to_string(),
            })?;
            decoder.push_chunk(&chunk, emit)?;
        }
        let reduced = decoder.finish(emit)?;
        record_evaluation_usage(
            request_sequence,
            reduced.usage.as_ref(),
            reduced.model.as_deref(),
        );
        Ok(reduced)
    }
}

async fn response_json(
    response: reqwest::Response,
    timeout: Duration,
) -> Result<ReducedResponses, ResponsesError> {
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        let body = read_llm_error_body(response, timeout).await;
        return Err(ResponsesError::Auth(redact_responses_error_body(&body)));
    }
    if !status.is_success() {
        let body = read_llm_error_body(response, timeout).await;
        return Err(ResponsesError::Status {
            status: status.as_u16(),
            body: redact_responses_error_body(&body),
        });
    }
    let body = response.text().await.map_err(|error| {
        ResponsesError::Http(LlmHttpError::new(
            error,
            LlmHttpPhase::ReadResponseBody,
            Some(timeout),
        ))
    })?;
    let value = serde_json::from_str(&body)?;
    reduce_response_value(value)
}

fn is_retryable(error: &ResponsesError) -> bool {
    match error {
        ResponsesError::Http(error) => error.is_retryable(),
        ResponsesError::Status { status, .. } => *status == 429 || *status >= 500,
        ResponsesError::StreamFailure { .. } => true,
        ResponsesError::Auth(_)
        | ResponsesError::ResponseJson(_)
        | ResponsesError::InvalidEndpoint(_)
        | ResponsesError::OutputShape { .. }
        | ResponsesError::Failed { .. }
        | ResponsesError::Incomplete { .. }
        | ResponsesError::RecoveryInterrupted => false,
    }
}

pub(crate) fn is_stream_recovery_failure(error: &ResponsesError) -> bool {
    matches!(error, ResponsesError::StreamFailure { .. })
        || matches!(error, ResponsesError::Http(error) if error.is_retryable())
}

fn ensure_recovery_active(
    recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
) -> Result<(), ResponsesError> {
    if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
        return Err(ResponsesError::RecoveryInterrupted);
    }
    Ok(())
}

async fn wait_for_backoff(
    backoff: Duration,
    recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
) -> Result<(), ResponsesError> {
    if backoff.is_zero() {
        return ensure_recovery_active(recovery_interrupt);
    }
    match recovery_interrupt {
        Some(interrupt) => {
            tokio::select! {
                biased;
                _ = interrupt.cancelled() => Err(ResponsesError::RecoveryInterrupted),
                _ = tokio::time::sleep(backoff) => Ok(()),
            }
        }
        None => {
            tokio::time::sleep(backoff).await;
            Ok(())
        }
    }
}

pub(super) fn compute_backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(10)).unwrap_or(u32::MAX);
    let capped = base.saturating_mul(factor).min(max);
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
    use std::sync::Arc;

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::api::{with_evaluation_usage_recording, EvaluationUsageRecorder};

    #[tokio::test]
    async fn client_parses_matching_json_and_sse_results() {
        let output = vec![
            json!({"type":"reasoning","id":"rs_1","encrypted_content":"opaque"}),
            json!({"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello"}]}),
        ];
        let json_body = json!({
            "status":"completed","output":output,"usage":{"total_tokens":12}
        })
        .to_string();
        let sse_body = format!(
            "data: {}\n\ndata: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_item.done","output_index":0,"item":output[0]}),
            json!({"type":"response.output_item.done","output_index":1,"item":output[1]}),
            json!({"type":"response.completed","response":{"status":"completed","output":output,"usage":{"total_tokens":12}}})
        );
        let (json_endpoint, _) = spawn_server("application/json", json_body).await;
        let (sse_endpoint, _) = spawn_server("text/event-stream", sse_body).await;

        let json_result = test_client(json_endpoint)
            .send(&request(false), &mut |_| {})
            .await
            .unwrap();
        let sse_result = test_client(sse_endpoint)
            .send(&request(true), &mut |_| {})
            .await
            .unwrap();

        assert_eq!(json_result, sse_result);
    }

    #[tokio::test]
    async fn client_streams_visible_text_delta() {
        let item = json!({"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello"}]});
        let body = format!(
            "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_text.delta","delta":"hel"}),
            json!({"type":"response.output_text.delta","delta":"lo"}),
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":{"status":"completed","output":[item]}})
        );
        let (endpoint, captured_request) = spawn_server("text/event-stream", body).await;
        let mut events = Vec::new();

        let result = test_client(endpoint)
            .send(&request(true), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(result.output_text, "hello");
        assert_eq!(
            events,
            vec![
                ResponsesStreamEvent::TextDelta { text: "hel".into() },
                ResponsesStreamEvent::TextDelta { text: "lo".into() }
            ]
        );
        let captured_request = captured_request.await.unwrap();
        assert!(captured_request
            .to_ascii_lowercase()
            .contains("authorization: bearer test-key"));
    }

    #[tokio::test]
    async fn client_records_native_responses_usage_for_evaluation() {
        let body = json!({
            "status": "completed",
            "model": "response-model",
            "output": [{
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "ok"}]
            }],
            "usage": {
                "input_tokens": 11,
                "input_tokens_details": {"cached_tokens": 7},
                "output_tokens": 5,
                "output_tokens_details": {"reasoning_tokens": 3}
            }
        })
        .to_string();
        let (endpoint, _) = spawn_server("application/json", body).await;
        let recorder = Arc::new(EvaluationUsageRecorder::default());

        with_evaluation_usage_recording(recorder.clone(), async {
            test_client(endpoint)
                .send(&request(false), &mut |_| {})
                .await
                .unwrap();
        })
        .await;

        let records = recorder.take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].model.as_deref(), Some("response-model"));
        assert!(records[0].is_complete);
        assert_eq!(records[0].input_tokens, 11);
        assert_eq!(records[0].output_tokens, 5);
        assert_eq!(records[0].cache_read_tokens, 7);
        assert_eq!(records[0].reasoning_tokens, 3);
    }

    #[tokio::test]
    async fn client_retries_json_retryable_status() {
        let success = json!({
            "status":"completed",
            "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]
        })
        .to_string();
        let (endpoint, requests) =
            spawn_status_sequence(vec![(500, "temporary".into()), (200, success)]).await;
        let client = ResponsesClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let response = client.send(&request(false), &mut |_| {}).await.unwrap();

        assert_eq!(response.output_text, "ok");
        assert_eq!(requests.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn client_retries_streaming_failure_before_any_visible_text() {
        let item = json!({
            "type":"message","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"ok"}]
        });
        let success = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":{"status":"completed","output":[item]}})
        );
        let (endpoint, requests) =
            spawn_status_sequence(vec![(500, "temporary".into()), (200, success)]).await;
        let client = ResponsesClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let response = client.send(&request(true), &mut |_| {}).await.unwrap();

        assert_eq!(response.output_text, "ok");
        assert_eq!(requests.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn client_retries_malformed_sse_json_before_visible_text() {
        let item = json!({
            "type":"message","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"ok"}]
        });
        let success = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":{"status":"completed","output":[item]}})
        );
        let (endpoint, requests) = spawn_status_sequence(vec![
            (200, "data: {broken-json}\n\n".into()),
            (200, success),
        ])
        .await;
        let client = ResponsesClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let response = client.send(&request(true), &mut |_| {}).await.unwrap();

        assert_eq!(response.output_text, "ok");
        assert_eq!(requests.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn client_does_not_retry_stream_after_visible_text() {
        let (endpoint, requests) = spawn_truncated_stream().await;
        let client = ResponsesClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut events = Vec::new();

        let error = client
            .send(&request(true), &mut |event| events.push(event))
            .await
            .unwrap_err();

        assert!(is_retryable(&error));
        assert_eq!(
            events,
            vec![ResponsesStreamEvent::TextDelta {
                text: "partial".into()
            }]
        );
        assert_eq!(requests.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn client_redacts_request_and_replay_echoed_by_http_error() {
        let secret = "opaque-reasoning-replay";
        let body = json!({
            "error": {
                "message":"invalid request",
                "request": {
                    "input":[{"type":"reasoning","encrypted_content":secret}],
                    "reasoning":{"effort":"high"}
                }
            }
        })
        .to_string();
        let (endpoint, requests) = spawn_status_sequence(vec![(400, body)]).await;

        let error = test_client(endpoint)
            .send(&request(false), &mut |_| {})
            .await
            .unwrap_err();
        let display = error.to_string();

        assert!(!display.contains(secret));
        assert!(display.contains("redacted Responses request/replay payload"));
        assert_eq!(requests.await.unwrap(), 1);
    }

    fn request(stream: bool) -> ResponsesRequest {
        ResponsesRequest {
            model: "test-model".into(),
            instructions: "system".into(),
            input: vec![json!({"role":"user","content":[{"type":"input_text","text":"hello"}]})],
            tools: Vec::new(),
            max_output_tokens: 128,
            stream,
            store: false,
            include: None,
            reasoning: None,
        }
    }

    fn test_client(endpoint: String) -> ResponsesClient {
        ResponsesClient::new(
            endpoint,
            "test-key".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
    }

    async fn spawn_server(
        content_type: &'static str,
        body: String,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
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
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{address}/v1/responses"), handle)
    }

    async fn spawn_status_sequence(
        responses: Vec<(u16, String)>,
    ) -> (String, tokio::task::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let expected = responses.len();
        let handle = tokio::spawn(async move {
            for (status, body) in responses {
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
                let reason = if status == 200 { "OK" } else { "Test Error" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            expected
        });
        (format!("http://{address}/v1/responses"), handle)
    }

    async fn spawn_truncated_stream() -> (String, tokio::task::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
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
            let body = format!(
                "data: {}\n\n",
                json!({"type":"response.output_text.delta","delta":"partial"})
            );
            let declared_length = body.len().saturating_add(64);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {declared_length}\r\nconnection: close\r\n\r\n{body}"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            drop(socket);

            match tokio::time::timeout(Duration::from_millis(500), listener.accept()).await {
                Ok(Ok((mut retry, _))) => {
                    let success = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{success}",
                        success.len()
                    );
                    retry.write_all(response.as_bytes()).await.unwrap();
                    2
                }
                _ => 1,
            }
        });
        (format!("http://{address}/v1/responses"), handle)
    }
}
