//! Anthropic Messages SSE 流式响应解析。
//!
//! 本模块把 text_delta / input_json_delta 累积成既可展示的运行时事件，
//! 也可持久化到 session transcript 的完整 assistant content blocks。
//! 主 `anthropic.rs` 只负责 Anthropic 协议转换；工具回环由 provider-neutral
//! `AgentTurnLoop` 编排。

use futures::StreamExt;
use serde_json::{json, Value};

use super::protocol::{
    has_tool_use_block, ApiMessage, ContinuedAssistantTurn, CreateMessageRequest,
};
use super::{
    compute_backoff, is_stream_retryable, AnthropicError, AnthropicMessagesClient,
    CONTINUATION_TRIGGER, MAX_CONTINUATION_TURNS,
};
use crate::api::continuation::append_with_overlap_dedupe;
use crate::api::llm_http::{read_llm_error_body, LlmHttpPhase};
use crate::api::SessionTurnEvent;
use crate::api::{
    context_usage_from_anthropic_committed_usage, context_usage_from_anthropic_input_usage,
    ProviderRecoveryInterrupt,
};

impl AnthropicMessagesClient {
    #[cfg(test)]
    pub(super) async fn send_text_with_continuation_streaming_for_provider(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_streaming_for_provider_with_retry_count(
            system,
            messages,
            tools,
            max_tokens,
            self.retry_count,
            emit,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn send_text_with_continuation_streaming_for_provider_with_retry_count(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_streaming_with_policy(
            system,
            messages,
            tools,
            max_tokens,
            retry_count,
            true,
            emit,
            false,
            false,
            None,
            None,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "streaming continuation 的 retry、emit 与 request observer 需显式穿过边界"
    )]
    pub(super) async fn send_text_with_continuation_streaming_for_provider_with_retry_count_observed(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        allow_continuation: bool,
        retry_after_partial: bool,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        observer: &mut super::AnthropicContinuationRequestObserver<'_>,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_streaming_with_policy(
            system,
            messages,
            tools,
            max_tokens,
            retry_count,
            allow_continuation,
            emit,
            false,
            retry_after_partial,
            Some(observer),
            recovery_interrupt,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "continuation 策略参数需显式穿过协议边界，避免隐藏 retry 语义"
    )]
    async fn send_text_with_continuation_streaming_with_policy(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        allow_continuation: bool,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        error_on_unresolved_max_tokens: bool,
        retry_after_partial: bool,
        mut request_observer: Option<&mut super::AnthropicContinuationRequestObserver<'_>>,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let mut merged_text = String::new();
        let mut last_response: Option<Value> = None;
        let mut last_blocks = Vec::new();
        let mut last_stop_reason = String::from("end_turn");
        let mut replay_messages = Vec::new();
        let max_continuation_turns = if allow_continuation {
            MAX_CONTINUATION_TURNS
        } else {
            0
        };

        for round in 0..=max_continuation_turns {
            if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
                if last_stop_reason == "max_tokens"
                    && last_response.is_some()
                    && !has_tool_use_block(&last_blocks)
                {
                    super::discard_pending_anthropic_continuation(
                        messages,
                        &mut replay_messages,
                        request_observer.as_deref_mut(),
                    )?;
                    recovery_interrupt
                        .expect("cancelled recovery interrupt must be present")
                        .preserve_successful_response();
                    last_stop_reason = "end_turn".into();
                    break;
                }
                return Err(AnthropicError::RecoveryInterrupted);
            }
            if let Some(observer) = request_observer.as_deref_mut() {
                observer.before_request().await?;
            }
            if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
                if last_stop_reason == "max_tokens"
                    && last_response.is_some()
                    && !has_tool_use_block(&last_blocks)
                {
                    if let Some(observer) = request_observer.as_deref_mut() {
                        observer.abandon_before_send().await?;
                    }
                    super::discard_pending_anthropic_continuation(
                        messages,
                        &mut replay_messages,
                        request_observer.as_deref_mut(),
                    )?;
                    recovery_interrupt
                        .expect("cancelled recovery interrupt must be present")
                        .preserve_successful_response();
                    last_stop_reason = "end_turn".into();
                    break;
                }
                return Err(AnthropicError::RecoveryInterrupted);
            }
            let body = self.request_for(
                system,
                messages.clone(),
                tools.clone(),
                max_tokens,
                Some(true),
            );
            let mut request_start_recorded = false;
            let response_result = {
                let mut request_started = |previous_attempt_ambiguous| {
                    if let Some(observer) = request_observer.as_deref_mut() {
                        observer.request_started(previous_attempt_ambiguous)?;
                    }
                    request_start_recorded = true;
                    Ok(())
                };
                self.send_stream_with_retry_and_start_hook(
                    &body,
                    retry_count,
                    retry_after_partial,
                    recovery_interrupt,
                    emit,
                    &mut request_started,
                )
                .await
            };
            let response_turn = match response_result {
                Ok(response) => response,
                Err(AnthropicError::RecoveryInterrupted)
                    if !request_start_recorded
                        && last_stop_reason == "max_tokens"
                        && last_response.is_some()
                        && !has_tool_use_block(&last_blocks) =>
                {
                    if let Some(observer) = request_observer.as_deref_mut() {
                        observer.abandon_before_send().await?;
                    }
                    super::discard_pending_anthropic_continuation(
                        messages,
                        &mut replay_messages,
                        request_observer.as_deref_mut(),
                    )?;
                    recovery_interrupt
                        .expect("RecoveryInterrupted requires a recovery interrupt")
                        .preserve_successful_response();
                    last_stop_reason = "end_turn".into();
                    break;
                }
                Err(error) => return Err(error),
            };
            if let Some(observer) = request_observer.as_deref_mut() {
                if response_turn.final_stop_reason == "refusal" {
                    observer.request_outcome_resolved()?;
                } else {
                    observer.response_accepted().await?;
                }
            }
            let assistant_replay = json!({
                "role": "assistant",
                "content": response_turn.final_blocks.clone(),
            });
            messages.push(ApiMessage::raw(assistant_replay.clone()));
            replay_messages.push(assistant_replay.clone());
            let round_text = response_turn.merged_text.clone();
            append_with_overlap_dedupe(&mut merged_text, &response_turn.merged_text);
            last_stop_reason = response_turn.final_stop_reason.clone();
            last_blocks = response_turn.final_blocks;
            last_response = Some(response_turn.final_response);

            if last_stop_reason != "max_tokens" {
                break;
            }
            if has_tool_use_block(&last_blocks) {
                break;
            }
            if let Some(interrupt) = recovery_interrupt.filter(|interrupt| interrupt.is_cancelled())
            {
                interrupt.preserve_successful_response();
                last_stop_reason = "end_turn".into();
                break;
            }
            if round == max_continuation_turns && error_on_unresolved_max_tokens {
                return Err(AnthropicError::OutputShape {
                    reason: format!(
                        "assistant max_tokens continuation 超过上限: {}",
                        max_continuation_turns + 1
                    ),
                    raw: merged_text,
                });
            }
            if round == max_continuation_turns {
                break;
            }
            let continuation = json!({"role": "user", "content": CONTINUATION_TRIGGER});
            messages.push(ApiMessage::raw(continuation.clone()));
            replay_messages.push(continuation.clone());
            if let Some(observer) = request_observer.as_deref_mut() {
                observer.push_round(vec![assistant_replay, continuation], round_text);
            }
        }

        let final_response = last_response.ok_or_else(|| AnthropicError::OutputShape {
            reason: "空响应：未获得 assistant 回合".into(),
            raw: String::new(),
        })?;
        Ok(ContinuedAssistantTurn {
            final_response,
            final_blocks: last_blocks,
            final_stop_reason: last_stop_reason,
            merged_text,
            replay_messages,
        })
    }

    async fn send_stream_once(
        &self,
        body: &CreateMessageRequest,
        previous_attempt_ambiguous: bool,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        request_started: &mut (dyn FnMut(bool) -> Result<(), AnthropicError> + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let pending = self
            .http
            .post(self.endpoint.as_str())
            .header("x-api-key", self.api_key.as_str())
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .json(body);
        request_started(previous_attempt_ambiguous)?;
        let resp = pending
            .send()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::SendRequest))?;

        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(AnthropicError::Auth(super::redact_anthropic_error_body(
                &body,
            )));
        }
        if !status.is_success() {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(AnthropicError::Status {
                status: status.as_u16(),
                body: super::redact_anthropic_error_body(&body),
            });
        }

        let mut sse_buffer = Vec::new();
        let mut builder = StreamingAssistantTurn::default();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| AnthropicError::StreamFailure {
                reason: self
                    .http_error(error, LlmHttpPhase::ReadStreamBody)
                    .to_string(),
                raw: String::new(),
            })?;
            sse_buffer.extend_from_slice(&chunk);
            for frame in drain_sse_frames(&mut sse_buffer) {
                if let Some(data) = sse_frame_data(&frame)? {
                    if data.trim() == "[DONE]" {
                        continue;
                    }
                    let event = serde_json::from_str::<Value>(&data).map_err(|error| {
                        AnthropicError::StreamFailure {
                            reason: format!("SSE data JSON 解析失败: {error}"),
                            raw: String::new(),
                        }
                    })?;
                    builder.apply_event(&event, emit)?;
                }
            }
        }
        if !sse_buffer.is_empty() {
            if let Some(data) = sse_frame_data(&sse_buffer)? {
                if data.trim() != "[DONE]" {
                    let event = serde_json::from_str::<Value>(&data).map_err(|error| {
                        AnthropicError::StreamFailure {
                            reason: format!("SSE data JSON 解析失败: {error}"),
                            raw: String::new(),
                        }
                    })?;
                    builder.apply_event(&event, emit)?;
                }
            }
        }
        builder.finish()
    }

    async fn send_stream_with_retry_and_start_hook(
        &self,
        body: &CreateMessageRequest,
        retry_count: u32,
        retry_after_partial: bool,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        request_started: &mut (dyn FnMut(bool) -> Result<(), AnthropicError> + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let mut last_retryable: Option<AnthropicError> = None;
        let mut previous_attempt_ambiguous = false;
        for attempt in 0..=retry_count {
            super::ensure_anthropic_recovery_active(recovery_interrupt)?;
            let mut replay_blocking_event_emitted = false;
            let result = {
                let mut tracking_emit = |event| {
                    if event_blocks_stream_retry(&event) {
                        replay_blocking_event_emitted = true;
                    }
                    emit(event);
                };
                self.send_stream_once(
                    body,
                    previous_attempt_ambiguous,
                    &mut tracking_emit,
                    request_started,
                )
                .await
            };
            match result {
                Ok(turn) => return Ok(turn),
                Err(e)
                    if (!replay_blocking_event_emitted || retry_after_partial)
                        && is_stream_retryable(&e)
                        && attempt < retry_count =>
                {
                    previous_attempt_ambiguous =
                        !matches!(&e, AnthropicError::Auth(_) | AnthropicError::Status { .. });
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Anthropic stream 调用失败，{}ms 后重试 ({}/{}): {}",
                        backoff.as_millis(),
                        attempt + 1,
                        retry_count,
                        e
                    );
                    last_retryable = Some(e);
                    super::wait_for_anthropic_backoff(backoff, recovery_interrupt).await?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(
            last_retryable.unwrap_or_else(|| AnthropicError::OutputShape {
                reason: "stream retry loop 未返回结果".into(),
                raw: String::new(),
            }),
        )
    }
}

fn event_blocks_stream_retry(event: &SessionTurnEvent) -> bool {
    !matches!(event, SessionTurnEvent::ContextUsageUpdated { .. })
}

#[derive(Debug)]
struct StreamingBlock {
    value: Value,
    input_json: String,
    finished: bool,
}

#[derive(Debug, Default)]
struct StreamingAssistantTurn {
    blocks: Vec<Option<StreamingBlock>>,
    stop_reason: Option<String>,
    merged_text: String,
    usage: Option<Value>,
    saw_message_start: bool,
    saw_message_stop: bool,
}

impl StreamingAssistantTurn {
    fn apply_event(
        &mut self,
        event: &Value,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<(), AnthropicError> {
        if self.saw_message_stop {
            return Err(AnthropicError::StreamFailure {
                reason: "message_stop 后仍收到 stream event".into(),
                raw: event.to_string(),
            });
        }
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                if self.saw_message_start {
                    return Err(AnthropicError::StreamFailure {
                        reason: "重复的 message_start".into(),
                        raw: event.to_string(),
                    });
                }
                self.saw_message_start = true;
                if let Some(usage) = event
                    .get("message")
                    .and_then(|message| message.get("usage"))
                    .cloned()
                {
                    if let Some(snapshot) = context_usage_from_anthropic_input_usage(&usage) {
                        emit(SessionTurnEvent::ContextUsageUpdated { usage: snapshot });
                    }
                    merge_usage(&mut self.usage, usage);
                }
            }
            Some("content_block_start") => {
                let index = sse_index(event)?;
                let block = event.get("content_block").cloned().ok_or_else(|| {
                    AnthropicError::StreamFailure {
                        reason: "content_block_start 缺少 content_block".into(),
                        raw: event.to_string(),
                    }
                })?;
                self.start_block(index, block)?;
            }
            Some("content_block_delta") => {
                let index = sse_index(event)?;
                let delta = event
                    .get("delta")
                    .ok_or_else(|| AnthropicError::StreamFailure {
                        reason: "content_block_delta 缺少 delta".into(),
                        raw: event.to_string(),
                    })?;
                self.apply_content_delta(index, delta, emit)?;
            }
            Some("content_block_stop") => {
                let index = sse_index(event)?;
                self.finish_block(index)?;
            }
            Some("message_delta") => {
                if let Some(stop_reason) = event
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.stop_reason = Some(stop_reason.to_string());
                }
                if let Some(usage) = event.get("usage").cloned() {
                    merge_usage(&mut self.usage, usage);
                }
            }
            Some("message_stop") => {
                if !self.saw_message_start {
                    return Err(AnthropicError::StreamFailure {
                        reason: "message_stop 早于 message_start".into(),
                        raw: event.to_string(),
                    });
                }
                self.saw_message_stop = true;
                if let Some(snapshot) = self
                    .usage
                    .as_ref()
                    .and_then(context_usage_from_anthropic_committed_usage)
                {
                    emit(SessionTurnEvent::ContextUsageUpdated { usage: snapshot });
                }
            }
            Some("ping") => {}
            Some("error") => {
                return Err(anthropic_stream_error_event(event));
            }
            other => {
                return Err(AnthropicError::StreamFailure {
                    reason: format!("未知 Anthropic stream event: {other:?}"),
                    raw: event.to_string(),
                });
            }
        }
        Ok(())
    }

    fn apply_content_delta(
        &mut self,
        index: usize,
        delta: &Value,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<(), AnthropicError> {
        let block = self.block_mut(index)?;
        if block.finished {
            return Err(AnthropicError::StreamFailure {
                reason: format!("stream delta 引用了已结束的 content block: {index}"),
                raw: delta.to_string(),
            });
        }
        match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => {
                require_stream_block_type(block, index, "text_delta", "text")?;
                let text = required_stream_delta_string(delta, "text_delta", "text")?;
                append_json_string_field(&mut block.value, "text", text);
                self.merged_text.push_str(text);
                if !text.is_empty() {
                    emit(SessionTurnEvent::AssistantTextDelta {
                        text: text.to_string(),
                    });
                }
            }
            Some("input_json_delta") => {
                require_stream_block_type(block, index, "input_json_delta", "tool_use")?;
                let partial =
                    required_stream_delta_string(delta, "input_json_delta", "partial_json")?;
                block.input_json.push_str(partial);
            }
            Some("thinking_delta") => {
                require_stream_block_type(block, index, "thinking_delta", "thinking")?;
                let thinking = required_stream_delta_string(delta, "thinking_delta", "thinking")?;
                append_json_string_field(&mut block.value, "thinking", thinking);
            }
            Some("signature_delta") => {
                require_stream_block_type(block, index, "signature_delta", "thinking")?;
                let signature =
                    required_stream_delta_string(delta, "signature_delta", "signature")?;
                append_json_string_field(&mut block.value, "signature", signature);
            }
            other => {
                return Err(AnthropicError::StreamFailure {
                    reason: format!("未知 content_block_delta type: {other:?}"),
                    raw: delta.to_string(),
                });
            }
        }
        Ok(())
    }

    fn finish_block(&mut self, index: usize) -> Result<(), AnthropicError> {
        let block = self.block_mut(index)?;
        if block.finished {
            return Err(AnthropicError::StreamFailure {
                reason: format!("重复的 content_block_stop: {index}"),
                raw: String::new(),
            });
        }
        if block.value.get("type").and_then(Value::as_str) == Some("tool_use")
            && !block.input_json.trim().is_empty()
        {
            let input = serde_json::from_str::<Value>(&block.input_json).map_err(|e| {
                AnthropicError::StreamFailure {
                    reason: format!("tool_use input_json_delta 解析失败: {e}"),
                    raw: block.input_json.clone(),
                }
            })?;
            if let Some(object) = block.value.as_object_mut() {
                object.insert("input".into(), input);
            }
        }
        block.finished = true;
        Ok(())
    }

    fn finish(self) -> Result<ContinuedAssistantTurn, AnthropicError> {
        if !self.saw_message_stop {
            return Err(AnthropicError::StreamFailure {
                reason: "stream closed before message_stop".into(),
                raw: String::new(),
            });
        }
        if !self.saw_message_start {
            return Err(AnthropicError::StreamFailure {
                reason: "stream 缺少 message_start".into(),
                raw: String::new(),
            });
        }
        if self.blocks.iter().flatten().any(|block| !block.finished) {
            return Err(AnthropicError::StreamFailure {
                reason: "message_stop 前存在未结束的 content block".into(),
                raw: String::new(),
            });
        }
        let stop_reason = super::validated_anthropic_stop_reason(self.stop_reason.as_deref())
            .map_err(|_| AnthropicError::StreamFailure {
                reason: "stream 缺少有效 stop_reason".into(),
                raw: String::new(),
            })?;
        let final_blocks = self
            .blocks
            .into_iter()
            .map(|block| {
                block
                    .map(|block| block.value)
                    .ok_or_else(|| AnthropicError::StreamFailure {
                        reason: "stream content block index 不连续".into(),
                        raw: String::new(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let final_response = json!({
            "content": final_blocks,
            "stop_reason": stop_reason.clone(),
        });
        Ok(ContinuedAssistantTurn {
            final_blocks,
            final_stop_reason: stop_reason,
            merged_text: self.merged_text,
            final_response,
            replay_messages: Vec::new(),
        })
    }

    fn start_block(&mut self, index: usize, block: Value) -> Result<(), AnthropicError> {
        let expected = self.blocks.len();
        if index != expected {
            return Err(AnthropicError::StreamFailure {
                reason: format!(
                    "stream content_block_start index 不连续: expected={expected}, actual={index}"
                ),
                raw: block.to_string(),
            });
        }
        self.blocks.push(Some(StreamingBlock {
            value: block,
            input_json: String::new(),
            finished: false,
        }));
        Ok(())
    }

    fn block_mut(&mut self, index: usize) -> Result<&mut StreamingBlock, AnthropicError> {
        self.blocks
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(|| AnthropicError::StreamFailure {
                reason: format!("stream delta 引用了未开始的 content block: {index}"),
                raw: String::new(),
            })
    }
}

fn anthropic_stream_error_event(event: &Value) -> AnthropicError {
    let error = event.get("error").and_then(Value::as_object);
    let error_type = super::classified_anthropic_error_type(&event.to_string());
    let message = error
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("upstream stream failed");
    let safe_error_type = error_type
        .as_deref()
        .and_then(super::safe_anthropic_error_type);
    let reason = format!(
        "Anthropic stream 返回 error event: type={} message={}",
        safe_error_type.unwrap_or("unknown"),
        super::redact_anthropic_error_body(message)
    );
    match safe_error_type {
        Some("rate_limit_error" | "api_error" | "overloaded_error" | "server_error") => {
            AnthropicError::TransientFailure { reason }
        }
        Some(error_type) if crate::api::is_provider_media_error_code(error_type) => {
            AnthropicError::MediaRejected {
                source: Box::new(AnthropicError::RequestRejected { reason }),
            }
        }
        Some(
            "invalid_request"
            | "invalid_request_error"
            | "invalid_prompt"
            | "context_length_exceeded"
            | "content_filter"
            | "content_policy_violation"
            | "safety_violation",
        ) => AnthropicError::RequestRejected { reason },
        _ => AnthropicError::TerminalFailure { reason },
    }
}

fn require_stream_block_type(
    block: &StreamingBlock,
    index: usize,
    delta_type: &str,
    expected_block_type: &str,
) -> Result<(), AnthropicError> {
    let actual_block_type = block.value.get("type").and_then(Value::as_str);
    if actual_block_type == Some(expected_block_type) {
        return Ok(());
    }
    Err(AnthropicError::StreamFailure {
        reason: format!(
            "stream {delta_type} 与 content block 类型不匹配: index={index}, expected={expected_block_type}, actual={actual_block_type:?}"
        ),
        raw: String::new(),
    })
}

fn required_stream_delta_string<'a>(
    delta: &'a Value,
    delta_type: &str,
    field: &str,
) -> Result<&'a str, AnthropicError> {
    delta
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| AnthropicError::StreamFailure {
            reason: format!("stream {delta_type} 缺少字符串字段 {field}"),
            raw: String::new(),
        })
}

fn merge_usage(current: &mut Option<Value>, update: Value) {
    let Some(update_object) = update.as_object() else {
        return;
    };
    let target = current.get_or_insert_with(|| json!({}));
    let Some(target_object) = target.as_object_mut() else {
        *target = Value::Object(update_object.clone());
        return;
    };
    for (key, value) in update_object {
        target_object.insert(key.clone(), value.clone());
    }
}

fn append_json_string_field(value: &mut Value, field: &str, suffix: &str) {
    let current = value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Some(object) = value.as_object_mut() {
        object.insert(field.into(), Value::String(format!("{current}{suffix}")));
    }
}

fn sse_index(event: &Value) -> Result<usize, AnthropicError> {
    let raw = event.get("index").and_then(Value::as_u64).ok_or_else(|| {
        AnthropicError::StreamFailure {
            reason: "stream event 缺少 index".into(),
            raw: event.to_string(),
        }
    })?;
    usize::try_from(raw).map_err(|_| AnthropicError::StreamFailure {
        reason: "stream event index 超出 usize 范围".into(),
        raw: event.to_string(),
    })
}

fn drain_sse_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while let Some((frame_end, separator_len)) = find_sse_frame_separator(buffer) {
        let frame = buffer[..frame_end].to_vec();
        buffer.drain(..frame_end + separator_len);
        frames.push(frame);
    }
    frames
}

fn find_sse_frame_separator(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf < crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

fn sse_frame_data(frame: &[u8]) -> Result<Option<String>, AnthropicError> {
    let frame = std::str::from_utf8(frame).map_err(|e| AnthropicError::StreamFailure {
        reason: format!("SSE frame 不是合法 UTF-8: {e}"),
        raw: String::new(),
    })?;
    let mut data_lines = Vec::new();
    for raw_line in frame.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.strip_prefix(' ').unwrap_or(data));
        }
    }
    if data_lines.is_empty() {
        Ok(None)
    } else {
        Ok(Some(data_lines.join("\n")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::api::provider::{ProviderRequestRejected, ProviderTerminalFailure};
    use crate::api::{
        AnthropicProviderAdapter, ProviderAdapter, ProviderRequest, ProviderRequestObserver,
        SessionTurnMessage,
    };

    #[derive(Default)]
    struct RecordingRequestObserver {
        requests: Vec<Vec<SessionTurnMessage>>,
        previous_attempt_ambiguity: Vec<bool>,
        resolved: usize,
        accepted: usize,
    }

    struct CancellingContinuationPreflightObserver {
        interrupt: ProviderRecoveryInterrupt,
        requests: Vec<Vec<SessionTurnMessage>>,
        started: usize,
        abandoned: usize,
    }

    #[async_trait]
    impl ProviderRequestObserver for RecordingRequestObserver {
        async fn before_provider_request(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.requests.push(messages.to_vec());
            Ok(())
        }

        fn provider_request_started_after(
            &mut self,
            _messages: &[SessionTurnMessage],
            previous_attempt_ambiguous: bool,
        ) -> anyhow::Result<()> {
            self.previous_attempt_ambiguity
                .push(previous_attempt_ambiguous);
            Ok(())
        }

        async fn provider_response_accepted(
            &mut self,
            _messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.accepted += 1;
            Ok(())
        }

        fn provider_request_outcome_resolved(
            &mut self,
            _messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.resolved += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl ProviderRequestObserver for CancellingContinuationPreflightObserver {
        async fn before_provider_request(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.requests.push(messages.to_vec());
            if self.requests.len() == 2 {
                self.interrupt.cancel();
            }
            Ok(())
        }

        fn provider_request_started(
            &mut self,
            _messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.started += 1;
            Ok(())
        }

        async fn provider_request_abandoned_before_send(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            assert_eq!(Some(messages), self.requests.last().map(Vec::as_slice));
            self.abandoned += 1;
            Ok(())
        }
    }

    #[test]
    fn content_block_start_rejects_sparse_huge_index_without_allocating() {
        let mut turn = StreamingAssistantTurn::default();
        let huge_index = usize::MAX as u64;
        let event = json!({
            "type": "content_block_start",
            "index": huge_index,
            "content_block": {"type": "text", "text": ""}
        });

        let error = turn.apply_event(&event, &mut |_| {}).unwrap_err();

        assert!(error.to_string().contains("index 不连续"));
        assert!(turn.blocks.is_empty());
    }

    #[test]
    fn invalid_utf8_sse_frame_is_stream_failure() {
        let error = sse_frame_data(b"data: \xff").unwrap_err();

        assert!(matches!(error, AnthropicError::StreamFailure { .. }));
    }

    #[test]
    fn deterministic_request_error_event_is_rejected() {
        let mut turn = StreamingAssistantTurn::default();
        let error = turn
            .apply_event(
                &json!({
                    "type":"error",
                    "error":{"type":"invalid_request_error","message":"invalid request"}
                }),
                &mut |_| {},
            )
            .unwrap_err();

        assert!(matches!(error, AnthropicError::RequestRejected { .. }));
    }

    #[test]
    fn stream_error_classifies_context_media_and_invalid_request() {
        for message in [
            "unsupported image format",
            "invalid image",
            "unsupported media type",
        ] {
            assert!(matches!(
                anthropic_stream_error_event(&json!({
                    "type": "error", "error": {"type": "invalid_request_error", "message": message}
                })),
                AnthropicError::MediaRejected { .. }
            ));
        }
        for error_type in ["context_length_exceeded", "invalid_request"] {
            let error = anthropic_stream_error_event(&json!({
                "type":"error",
                "error":{"type":error_type,"message":"rejected"}
            }));
            assert!(
                matches!(error, AnthropicError::RequestRejected { .. }),
                "type={error_type}"
            );
        }

        let media = anthropic_stream_error_event(&json!({
            "type":"error",
            "error":{"type":"unsupported_media_type","message":"rejected"}
        }));
        assert!(matches!(media, AnthropicError::MediaRejected { .. }));
    }

    #[test]
    fn stream_error_redacts_unknown_type_and_free_text_message() {
        let error = anthropic_stream_error_event(&json!({
            "type":"error",
            "error":{
                "type":"private-tool-output",
                "message":"private prompt copied by upstream"
            }
        }));
        let display = error.to_string();

        assert!(matches!(error, AnthropicError::TerminalFailure { .. }));
        assert!(display.contains("type=unknown"));
        assert!(display.contains("redacted Anthropic request/replay payload"));
        assert!(!display.contains("private-tool-output"));
        assert!(!display.contains("private prompt copied by upstream"));
    }

    #[tokio::test]
    async fn unknown_stream_error_preserves_provider_request_history() {
        let body = sse_response(vec![json!({
            "type":"error",
            "error":{"message":"future provider failure"}
        })]);
        let (endpoint, requests) = spawn_sse_server(vec![body]).await;
        let adapter = AnthropicProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let error = adapter
            .send(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 128,
                    stream: true,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    allow_continuation: false,
                    retry_count_override: Some(0),
                },
                &mut |_| {},
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<ProviderTerminalFailure>().is_some());
        assert!(error.downcast_ref::<ProviderRequestRejected>().is_none());
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn transient_error_events_are_resolved_failures_without_secret_leakage() {
        for error_type in ["rate_limit_error", "api_error", "overloaded_error"] {
            let mut turn = StreamingAssistantTurn::default();
            let error = turn
                .apply_event(
                    &json!({
                        "type":"error",
                        "error":{
                            "type":error_type,
                            "message":"temporarily unavailable",
                            "api_key":"secret-value"
                        }
                    }),
                    &mut |_| {},
                )
                .unwrap_err();

            assert!(matches!(error, AnthropicError::TransientFailure { .. }));
            assert!(!error.to_string().contains("secret-value"));
        }

        let mut turn = StreamingAssistantTurn::default();
        let null_type = turn
            .apply_event(
                &json!({
                    "type":"error",
                    "error":{
                        "type":null,
                        "code":"rate_limit_error",
                        "message":"retry later"
                    }
                }),
                &mut |_| {},
            )
            .unwrap_err();
        assert!(matches!(null_type, AnthropicError::TransientFailure { .. }));
    }

    #[test]
    fn content_block_start_requires_next_consecutive_index() {
        let mut turn = StreamingAssistantTurn::default();
        let first = json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        });
        turn.apply_event(&first, &mut |_| {}).unwrap();
        let skipped = json!({
            "type": "content_block_start",
            "index": 2,
            "content_block": {"type": "text", "text": ""}
        });

        let error = turn.apply_event(&skipped, &mut |_| {}).unwrap_err();

        assert!(error.to_string().contains("expected=1, actual=2"));
        assert_eq!(turn.blocks.len(), 1);
    }

    #[test]
    fn streaming_thinking_and_signature_are_reduced_exactly_without_visible_delta() {
        let mut turn = StreamingAssistantTurn::default();
        let mut events = Vec::new();
        for event in [
            json!({"type":"message_start", "message":{"usage":{"input_tokens":3}}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"thinking", "thinking":"", "signature":""}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"thinking_delta", "thinking":"private thought"}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"signature_delta", "signature":"opaque-signature"}
            }),
            json!({"type":"content_block_stop", "index":0}),
            json!({
                "type":"content_block_start", "index":1,
                "content_block":{"type":"text", "text":""}
            }),
            json!({
                "type":"content_block_delta", "index":1,
                "delta":{"type":"text_delta", "text":"visible answer"}
            }),
            json!({"type":"content_block_stop", "index":1}),
            json!({
                "type":"message_delta", "delta":{"stop_reason":"end_turn"},
                "usage":{"output_tokens":9}
            }),
            json!({"type":"message_stop"}),
        ] {
            turn.apply_event(&event, &mut |event| events.push(event))
                .unwrap();
        }

        let completed = turn.finish().unwrap();

        assert_eq!(
            completed.final_blocks[0],
            json!({
                "type":"thinking",
                "thinking":"private thought",
                "signature":"opaque-signature"
            })
        );
        assert_eq!(
            completed.final_blocks[1],
            json!({"type":"text", "text":"visible answer"})
        );
        assert_eq!(completed.merged_text, "visible answer");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SessionTurnEvent::AssistantTextDelta { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn streaming_rejects_delta_block_type_mismatch_without_visible_output() {
        for (block, delta, expected_error) in [
            (
                json!({"type":"thinking", "thinking":"", "signature":""}),
                json!({"type":"text_delta", "text":"must stay private"}),
                "text_delta",
            ),
            (
                json!({"type":"text", "text":""}),
                json!({"type":"thinking_delta", "thinking":"private"}),
                "thinking_delta",
            ),
            (
                json!({"type":"text", "text":""}),
                json!({"type":"input_json_delta", "partial_json":"{}"}),
                "input_json_delta",
            ),
            (
                json!({"type":"text", "text":""}),
                json!({"type":"signature_delta", "signature":"opaque"}),
                "signature_delta",
            ),
        ] {
            let mut turn = StreamingAssistantTurn::default();
            turn.apply_event(
                &json!({
                    "type":"content_block_start",
                    "index":0,
                    "content_block":block
                }),
                &mut |_| {},
            )
            .unwrap();
            let mut visible_events = Vec::new();

            let error = turn
                .apply_event(
                    &json!({
                        "type":"content_block_delta",
                        "index":0,
                        "delta":delta
                    }),
                    &mut |event| visible_events.push(event),
                )
                .unwrap_err();

            assert!(error.to_string().contains(expected_error));
            assert!(error.to_string().contains("类型不匹配"));
            assert!(visible_events.is_empty());
        }
    }

    #[test]
    fn streaming_rejects_missing_or_non_string_delta_payloads() {
        for invalid_value in [None, Some(Value::Null), Some(json!(7))] {
            for (block, delta_type, field) in [
                (json!({"type":"text", "text":""}), "text_delta", "text"),
                (
                    json!({"type":"tool_use", "id":"toolu_1", "name":"file_read", "input":{}}),
                    "input_json_delta",
                    "partial_json",
                ),
                (
                    json!({"type":"thinking", "thinking":"", "signature":""}),
                    "thinking_delta",
                    "thinking",
                ),
                (
                    json!({"type":"thinking", "thinking":"", "signature":""}),
                    "signature_delta",
                    "signature",
                ),
            ] {
                let mut turn = StreamingAssistantTurn::default();
                turn.apply_event(
                    &json!({
                        "type":"content_block_start",
                        "index":0,
                        "content_block":block
                    }),
                    &mut |_| {},
                )
                .unwrap();
                let mut delta = json!({"type":delta_type});
                if let Some(value) = invalid_value.clone() {
                    delta[field] = value;
                }
                let mut visible_events = Vec::new();

                let error = turn
                    .apply_event(
                        &json!({
                            "type":"content_block_delta",
                            "index":0,
                            "delta":delta
                        }),
                        &mut |event| visible_events.push(event),
                    )
                    .unwrap_err();

                assert!(error.to_string().contains(delta_type));
                assert!(error.to_string().contains(field));
                assert!(visible_events.is_empty());
            }
        }
    }

    #[test]
    fn streaming_rejects_blank_stop_reason() {
        let mut turn = StreamingAssistantTurn::default();
        for event in [
            json!({"type":"message_start", "message":{}}),
            json!({"type":"message_delta", "delta":{"stop_reason":"  "}}),
            json!({"type":"message_stop"}),
        ] {
            turn.apply_event(&event, &mut |_| {}).unwrap();
        }

        let error = match turn.finish() {
            Ok(_) => panic!("blank stop_reason must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("缺少有效 stop_reason"));
    }

    #[test]
    fn streaming_terminal_rejects_unfinished_content_block() {
        let mut turn = StreamingAssistantTurn::default();
        for event in [
            json!({"type":"message_start", "message":{}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"thinking", "thinking":"", "signature":""}
            }),
            json!({
                "type":"message_delta", "delta":{"stop_reason":"end_turn"}
            }),
            json!({"type":"message_stop"}),
        ] {
            turn.apply_event(&event, &mut |_| {}).unwrap();
        }

        let error = match turn.finish() {
            Ok(_) => panic!("unfinished block must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("未结束的 content block"));
    }

    #[test]
    fn streaming_context_window_stop_preserves_complete_partial_blocks() {
        let mut turn = StreamingAssistantTurn::default();
        let mut events = Vec::new();
        for event in [
            json!({"type":"message_start", "message":{"usage":{"input_tokens":120}}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"thinking", "thinking":"", "signature":""}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"thinking_delta", "thinking":"private-context"}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"signature_delta", "signature":"sig-context"}
            }),
            json!({"type":"content_block_stop", "index":0}),
            json!({
                "type":"content_block_start", "index":1,
                "content_block":{"type":"text", "text":""}
            }),
            json!({
                "type":"content_block_delta", "index":1,
                "delta":{"type":"text_delta", "text":"visible partial"}
            }),
            json!({"type":"content_block_stop", "index":1}),
            json!({
                "type":"message_delta",
                "delta":{"stop_reason":"model_context_window_exceeded"},
                "usage":{"output_tokens":8}
            }),
            json!({"type":"message_stop"}),
        ] {
            turn.apply_event(&event, &mut |event| events.push(event))
                .unwrap();
        }

        let completed = turn.finish().unwrap();

        assert_eq!(completed.final_stop_reason, "model_context_window_exceeded");
        assert_eq!(completed.merged_text, "visible partial");
        assert_eq!(completed.final_blocks[0]["thinking"], "private-context");
        assert_eq!(completed.final_blocks[0]["signature"], "sig-context");
        assert_eq!(completed.final_blocks[1]["text"], "visible partial");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SessionTurnEvent::AssistantTextDelta { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn ctx_usage_only_event_does_not_block_stream_retry() {
        let first_body = sse_response(vec![json!({
            "type": "message_start",
            "message": {
                "usage": {
                    "input_tokens": 1,
                    "cache_creation_input_tokens": 2,
                    "cache_read_input_tokens": 3
                }
            }
        })]);
        let second_body = sse_response(vec![
            json!({
                "type": "message_start",
                "message": {
                    "usage": {
                        "input_tokens": 1,
                        "cache_creation_input_tokens": 2,
                        "cache_read_input_tokens": 3
                    }
                }
            }),
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            }),
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "ok"}
            }),
            json!({"type": "content_block_stop", "index": 0}),
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn"},
                "usage": {"output_tokens": 2}
            }),
            json!({"type": "message_stop"}),
        ]);
        let (endpoint, requests) = spawn_sse_server(vec![first_body, second_body]).await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut messages = vec![ApiMessage::structured(
            "user",
            vec![json!({"type":"text", "text":"hello"})],
        )];
        let mut events = Vec::new();

        let turn = client
            .send_text_with_continuation_streaming_for_provider(
                "system",
                &mut messages,
                None,
                128,
                &mut |event| events.push(event),
            )
            .await
            .unwrap();

        assert_eq!(turn.merged_text, "ok");
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ContextUsageUpdated { usage }
                    if usage.used_tokens == 6
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ContextUsageUpdated { usage }
                    if usage.used_tokens == 8
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::AssistantTextDelta { text }
                    if text == "ok"
            )
        }));
        assert_eq!(
            turn.replay_messages,
            vec![json!({
                "role":"assistant",
                "content":[{"type":"text", "text":"ok"}]
            })]
        );
    }

    #[tokio::test]
    async fn max_token_streaming_continuation_preserves_private_message_sequence() {
        let first = sse_response(vec![
            json!({"type":"message_start", "message":{"usage":{"input_tokens":1}}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"thinking", "thinking":"", "signature":""}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"thinking_delta", "thinking":"private-one"}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"signature_delta", "signature":"sig-one"}
            }),
            json!({"type":"content_block_stop", "index":0}),
            json!({
                "type":"content_block_start", "index":1,
                "content_block":{"type":"text", "text":""}
            }),
            json!({
                "type":"content_block_delta", "index":1,
                "delta":{"type":"text_delta", "text":"first "}
            }),
            json!({"type":"content_block_stop", "index":1}),
            json!({"type":"message_delta", "delta":{"stop_reason":"max_tokens"}}),
            json!({"type":"message_stop"}),
        ]);
        let second = sse_response(vec![
            json!({"type":"message_start", "message":{"usage":{"input_tokens":2}}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"redacted_thinking", "data":"opaque-two"}
            }),
            json!({"type":"content_block_stop", "index":0}),
            json!({
                "type":"content_block_start", "index":1,
                "content_block":{"type":"text", "text":""}
            }),
            json!({
                "type":"content_block_delta", "index":1,
                "delta":{"type":"text_delta", "text":"second"}
            }),
            json!({"type":"content_block_stop", "index":1}),
            json!({"type":"message_delta", "delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_stop"}),
        ]);
        let (endpoint, requests) = spawn_sse_server(vec![first, second]).await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut messages = vec![ApiMessage::structured(
            "user",
            vec![json!({"type":"text", "text":"hello"})],
        )];
        let mut recording = RecordingRequestObserver::default();
        let mut request_observer = super::super::AnthropicContinuationRequestObserver {
            messages: vec![SessionTurnMessage::user_text("hello")],
            model: "test-model".into(),
            observer: &mut recording,
        };

        let turn = client
            .send_text_with_continuation_streaming_for_provider_with_retry_count_observed(
                "system",
                &mut messages,
                None,
                128,
                0,
                true,
                false,
                &mut |_| {},
                &mut request_observer,
                None,
            )
            .await
            .unwrap();
        drop(request_observer);

        assert_eq!(turn.merged_text, "first second");
        assert_eq!(turn.replay_messages.len(), 3);
        assert_eq!(
            turn.replay_messages[0]["content"][0]["thinking"],
            "private-one"
        );
        assert_eq!(
            turn.replay_messages[0]["content"][0]["signature"],
            "sig-one"
        );
        assert_eq!(turn.replay_messages[1]["role"], "user");
        assert_eq!(turn.replay_messages[1]["content"], CONTINUATION_TRIGGER);
        assert_eq!(turn.replay_messages[2]["content"][0]["data"], "opaque-two");
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(messages.len(), 4);
        let replayed_messages = serde_json::to_value(&messages[1..]).unwrap();
        assert!(replayed_messages.to_string().contains("private-one"));
        assert!(replayed_messages.to_string().contains(CONTINUATION_TRIGGER));
        assert_eq!(recording.requests.len(), 2);
        assert_eq!(recording.accepted, 2);
        assert!(recording.requests[1].starts_with(&recording.requests[0]));
        let observed_second = serde_json::to_value(super::super::session_turn_messages_to_api(
            recording.requests[1].clone(),
            "test-model",
        ))
        .unwrap();
        let captured_second: Value = serde_json::from_str(
            captured[1]
                .split_once("\r\n\r\n")
                .map(|(_, body)| body)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            observed_second, captured_second["messages"],
            "streaming observer 必须映射为同一份 Anthropic messages"
        );
    }

    #[tokio::test]
    async fn streaming_continuation_rejection_preserves_the_accepted_first_round() {
        let first = sse_response(vec![
            json!({"type":"message_start", "message":{"usage":{"input_tokens":1}}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"text", "text":""}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"text_delta", "text":"kept"}
            }),
            json!({"type":"content_block_stop", "index":0}),
            json!({"type":"message_delta", "delta":{"stop_reason":"max_tokens"}}),
            json!({"type":"message_stop"}),
        ]);
        let second = json!({
            "type":"error",
            "error":{"type":"invalid_request_error", "message":"continuation rejected"}
        })
        .to_string();
        let (endpoint, requests) = spawn_http_responses(vec![
            (200, "text/event-stream", first),
            (400, "application/json", second),
        ])
        .await;
        let adapter = AnthropicProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut observer = RecordingRequestObserver::default();

        let error = adapter
            .send_with_request_observer(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 128,
                    stream: true,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    allow_continuation: true,
                    retry_count_override: None,
                },
                &mut |_| {},
                &mut observer,
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<ProviderRequestRejected>().is_some());
        assert_eq!(observer.accepted, 1);
        assert_eq!(observer.resolved, 1);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn transient_error_event_marks_the_retry_ambiguous() {
        let first = sse_response(vec![json!({
            "type":"error",
            "error":{
                "type":"rate_limit_error",
                "message":"retry later"
            }
        })]);
        let second = json!({
            "type":"error",
            "error":{
                "type":"invalid_request_error",
                "message":"invalid input"
            }
        })
        .to_string();
        let (endpoint, requests) = spawn_http_responses(vec![
            (200, "text/event-stream", first),
            (400, "application/json", second),
        ])
        .await;
        let adapter = AnthropicProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut observer = RecordingRequestObserver::default();

        let error = adapter
            .send_with_request_observer(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 128,
                    stream: true,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    allow_continuation: false,
                    retry_count_override: None,
                },
                &mut |_| {},
                &mut observer,
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<ProviderRequestRejected>().is_some());
        assert_eq!(observer.previous_attempt_ambiguity, vec![false, true]);
        assert_eq!(observer.resolved, 1);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn safe_steer_during_streaming_continuation_wal_keeps_anthropic_partial() {
        let first = sse_response(vec![
            json!({"type":"message_start", "message":{"usage":{"input_tokens":1}}}),
            json!({
                "type":"content_block_start", "index":0,
                "content_block":{"type":"text", "text":""}
            }),
            json!({
                "type":"content_block_delta", "index":0,
                "delta":{"type":"text_delta", "text":"partial-answer"}
            }),
            json!({"type":"content_block_stop", "index":0}),
            json!({"type":"message_delta", "delta":{"stop_reason":"max_tokens"}}),
            json!({"type":"message_stop"}),
        ]);
        let (endpoint, requests) = spawn_sse_server(vec![first]).await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            32,
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let interrupt = ProviderRecoveryInterrupt::new();
        let mut messages = vec![ApiMessage::structured(
            "user",
            vec![json!({"type":"text", "text":"hello"})],
        )];
        let mut recording = CancellingContinuationPreflightObserver {
            interrupt: interrupt.clone(),
            requests: Vec::new(),
            started: 0,
            abandoned: 0,
        };
        let mut request_observer = super::super::AnthropicContinuationRequestObserver {
            messages: vec![SessionTurnMessage::user_text("hello")],
            model: "test-model".into(),
            observer: &mut recording,
        };

        let turn = client
            .send_text_with_continuation_streaming_for_provider_with_retry_count_observed(
                "system",
                &mut messages,
                None,
                32,
                0,
                true,
                false,
                &mut |_| {},
                &mut request_observer,
                Some(&interrupt),
            )
            .await
            .unwrap();
        drop(request_observer);

        assert_eq!(turn.final_stop_reason, "end_turn");
        assert_eq!(turn.merged_text, "partial-answer");
        assert_eq!(turn.replay_messages.len(), 1);
        assert!(interrupt.should_preserve_successful_response());
        assert_eq!(recording.requests.len(), 2);
        assert_eq!(recording.started, 1);
        assert_eq!(recording.abandoned, 1);
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn stream_body_error_names_stream_phase() {
        let body = sse_response(vec![
            json!({
                "type": "message_start",
                "message": {
                    "usage": {
                        "input_tokens": 1
                    }
                }
            }),
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            }),
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "partial"}
            }),
        ]);
        let endpoint = spawn_raw_response(format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
            body.len() + 64,
            body
        ))
        .await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut messages = vec![ApiMessage::structured(
            "user",
            vec![json!({"type":"text", "text":"hello"})],
        )];

        let result = client
            .send_text_with_continuation_streaming_for_provider(
                "system",
                &mut messages,
                None,
                128,
                &mut |_| {},
            )
            .await;
        let error = match result {
            Ok(_) => panic!("expected stream body error"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("LLM response body"));
        assert!(error.contains("reading LLM stream response body"));
    }

    async fn spawn_sse_server(bodies: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_task = requests.clone();
        tokio::spawn(async move {
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                requests_for_task
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..n]).to_string());
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{addr}"), requests)
    }

    async fn spawn_http_responses(
        responses: Vec<(u16, &'static str, String)>,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_task = Arc::clone(&requests);
        tokio::spawn(async move {
            for (status, content_type, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0; 8192];
                let read = socket.read(&mut buffer).await.unwrap();
                requests_for_task
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buffer[..read]).to_string());
                let reason = if status == 200 { "OK" } else { "Bad Request" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{addr}"), requests)
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

    fn sse_response(events: Vec<Value>) -> String {
        let mut body = String::new();
        for event in events {
            body.push_str("data: ");
            body.push_str(&event.to_string());
            body.push_str("\n\n");
        }
        body
    }
}
