use std::collections::BTreeMap;

use serde_json::Value;

use super::{
    redact_responses_error_body, reduce_response_value, ReducedResponses, ResponsesError,
    ResponsesStreamEvent,
};

#[derive(Default)]
pub(super) struct ResponsesSseDecoder {
    buffer: Vec<u8>,
    accumulator: ResponsesStreamAccumulator,
}

impl ResponsesSseDecoder {
    pub(super) fn push_chunk(
        &mut self,
        chunk: &[u8],
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<(), ResponsesError> {
        self.buffer.extend_from_slice(chunk);
        for frame in drain_sse_frames(&mut self.buffer) {
            if let Some(data) = sse_frame_data(&frame)? {
                self.accumulator.apply_frame(&data, emit)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, ResponsesError> {
        if !self.buffer.is_empty() {
            if let Some(data) = sse_frame_data(&self.buffer)? {
                self.accumulator.apply_frame(&data, emit)?;
            }
            self.buffer.clear();
        }
        let value = self.accumulator.finish()?;
        reduce_response_value(value)
    }
}

#[derive(Default)]
struct ResponsesStreamAccumulator {
    output_items: BTreeMap<usize, Value>,
    terminal_response: Option<Value>,
    terminal_status: Option<&'static str>,
}

impl ResponsesStreamAccumulator {
    fn apply_frame(
        &mut self,
        data: &str,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<(), ResponsesError> {
        if data.trim() == "[DONE]" {
            return Ok(());
        }
        let event: Value =
            serde_json::from_str(data).map_err(|error| ResponsesError::StreamFailure {
                reason: format!("SSE data JSON 解析失败: {error}"),
            })?;
        let kind = event.get("type").and_then(Value::as_str).ok_or_else(|| {
            ResponsesError::StreamFailure {
                reason: "SSE event 缺少 type".into(),
            }
        })?;
        if self.terminal_response.is_some() {
            return Err(ResponsesError::StreamFailure {
                reason: format!("Responses terminal event 后仍收到 {kind}"),
            });
        }
        match kind {
            "response.created" | "response.output_item.added" | "response.in_progress" => {}
            "response.output_text.delta" | "response.refusal.delta" => {
                let delta = event.get("delta").and_then(Value::as_str).ok_or_else(|| {
                    ResponsesError::StreamFailure {
                        reason: format!("{kind} 缺少 delta"),
                    }
                })?;
                if !delta.is_empty() {
                    emit(ResponsesStreamEvent::TextDelta {
                        text: delta.to_string(),
                    });
                }
            }
            "response.output_item.done" => {
                let index = event
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok())
                    .ok_or_else(|| ResponsesError::StreamFailure {
                        reason: "response.output_item.done 缺少合法 output_index".into(),
                    })?;
                let item = event
                    .get("item")
                    .filter(|item| item.is_object())
                    .cloned()
                    .ok_or_else(|| ResponsesError::StreamFailure {
                        reason: "response.output_item.done 缺少 item object".into(),
                    })?;
                if self.output_items.insert(index, item).is_some() {
                    return Err(ResponsesError::StreamFailure {
                        reason: format!("response.output_item.done output_index={index} 重复"),
                    });
                }
            }
            "response.completed" | "response.incomplete" => {
                if self.terminal_response.is_some() {
                    return Err(ResponsesError::StreamFailure {
                        reason: "Responses SSE 收到重复 terminal event".into(),
                    });
                }
                self.terminal_status = Some(if kind == "response.completed" {
                    "completed"
                } else {
                    "incomplete"
                });
                self.terminal_response = Some(
                    event
                        .get("response")
                        .filter(|response| response.is_object())
                        .cloned()
                        .ok_or_else(|| ResponsesError::StreamFailure {
                            reason: format!("{kind} 缺少 response object"),
                        })?,
                );
            }
            "response.failed" => {
                return Err(ResponsesError::Failed {
                    message: event_error_message(
                        event.get("response").and_then(|r| r.get("error")),
                    ),
                });
            }
            "error" => {
                return Err(ResponsesError::Failed {
                    message: event_error_message(Some(&event)),
                });
            }
            _ => {
                log::debug!(target: "api", "忽略未消费的 Responses SSE event type={kind}");
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Value, ResponsesError> {
        let mut response = self
            .terminal_response
            .ok_or_else(|| ResponsesError::StreamFailure {
                reason: "Responses SSE 在 terminal event 前结束".into(),
            })?;
        let response_object =
            response
                .as_object_mut()
                .ok_or_else(|| ResponsesError::StreamFailure {
                    reason: "Responses terminal response 不是 object".into(),
                })?;
        let actual_status = response_object
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| ResponsesError::StreamFailure {
                reason: "Responses terminal response 缺少 status".into(),
            })?;
        let expected_status =
            self.terminal_status
                .ok_or_else(|| ResponsesError::StreamFailure {
                    reason: "Responses SSE 缺少 terminal status".into(),
                })?;
        if actual_status != expected_status {
            return Err(ResponsesError::StreamFailure {
                reason: format!(
                    "Responses terminal event/status 不一致: event={expected_status}, response={actual_status}"
                ),
            });
        }
        let mut expected_index = 0usize;
        let mut output = Vec::with_capacity(self.output_items.len());
        for (index, item) in self.output_items {
            if index != expected_index {
                return Err(ResponsesError::StreamFailure {
                    reason: format!(
                        "Responses SSE output_index 不连续: 期望 {expected_index}，实际 {index}"
                    ),
                });
            }
            expected_index = expected_index.saturating_add(1);
            output.push(item);
        }
        // 完整 output/replay 只以 output_item.done 为权威。terminal response 仅提供
        // status/usage 等终态元数据，其 output 可能省略 item 或使用不同的可选字段形状。
        response_object.insert("output".into(), Value::Array(output));
        Ok(response)
    }
}

fn event_error_message(error: Option<&Value>) -> String {
    let message = error
        .and_then(Value::as_object)
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("upstream Responses stream failed");
    redact_responses_error_body(message)
}

fn drain_sse_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while let Some((position, delimiter_len)) = find_frame_end(buffer) {
        let frame = buffer.drain(..position).collect::<Vec<_>>();
        buffer.drain(..delimiter_len);
        frames.push(frame);
    }
    frames
}

fn sse_frame_data(frame: &[u8]) -> Result<Option<String>, ResponsesError> {
    let text = std::str::from_utf8(frame).map_err(|error| ResponsesError::StreamFailure {
        reason: format!("Responses SSE frame 不是 UTF-8: {error}"),
    })?;
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>();
    if data.is_empty() {
        Ok(None)
    } else {
        Ok(Some(data.join("\n")))
    }
}

fn find_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::api::ResponsesTerminal;

    fn sse_event(value: Value) -> String {
        format!("event: {}\ndata: {}\n\n", value["type"], value)
    }

    #[test]
    fn invalid_utf8_sse_frame_is_stream_failure() {
        let error = sse_frame_data(b"data: \xff").unwrap_err();

        assert!(matches!(error, ResponsesError::StreamFailure { .. }));
    }

    #[test]
    fn decoder_handles_arbitrary_chunks_and_uses_done_items_as_authority() {
        let done_item = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"hello","annotations":[],"future":true}]
        });
        let body = [
            sse_event(json!({"type":"response.created","response":{"status":"in_progress","output":[]}})),
            sse_event(json!({"type":"response.output_text.delta","delta":"hel"})),
            sse_event(json!({"type":"response.output_text.delta","delta":"lo"})),
            sse_event(json!({"type":"response.output_item.done","output_index":0,"item":done_item.clone()})),
            sse_event(json!({"type":"response.completed","response":{"status":"completed","output":[{
                "type":"message","id":"msg_1","role":"assistant","status":"completed","content":[]
            }],"usage":{"total_tokens":9}}})),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        let mut events = Vec::new();
        for chunk in body.as_bytes().chunks(3) {
            decoder
                .push_chunk(chunk, &mut |event| events.push(event))
                .unwrap();
        }

        let reduced = decoder.finish(&mut |event| events.push(event)).unwrap();

        assert_eq!(reduced.output_text, "hello");
        assert_eq!(reduced.output_items, vec![done_item]);
        assert_eq!(reduced.usage, Some(json!({"total_tokens":9})));
        assert_eq!(
            events,
            vec![
                ResponsesStreamEvent::TextDelta { text: "hel".into() },
                ResponsesStreamEvent::TextDelta { text: "lo".into() }
            ]
        );
    }

    #[test]
    fn decoder_streams_and_reduces_refusal() {
        let refusal = json!({
            "type":"message",
            "id":"msg_refusal",
            "role":"assistant",
            "status":"completed",
            "content":[{"type":"refusal","refusal":"request refused"}]
        });
        let body = [
            sse_event(json!({"type":"response.refusal.delta","delta":"request "})),
            sse_event(json!({"type":"response.refusal.delta","delta":"refused"})),
            sse_event(json!({
                "type":"response.output_item.done",
                "output_index":0,
                "item":refusal.clone()
            })),
            sse_event(json!({
                "type":"response.completed",
                "response":{"status":"completed","output":[]}
            })),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        let mut events = Vec::new();
        decoder
            .push_chunk(body.as_bytes(), &mut |event| events.push(event))
            .unwrap();

        let reduced = decoder.finish(&mut |event| events.push(event)).unwrap();

        assert_eq!(reduced.output_text, "request refused");
        assert_eq!(reduced.output_items, vec![refusal]);
        assert_eq!(
            events,
            vec![
                ResponsesStreamEvent::TextDelta {
                    text: "request ".into()
                },
                ResponsesStreamEvent::TextDelta {
                    text: "refused".into()
                }
            ]
        );
    }

    #[test]
    fn decoder_rejects_eof_without_terminal_event() {
        let mut decoder = ResponsesSseDecoder::default();
        decoder
            .push_chunk(
                sse_event(json!({"type":"response.output_text.delta","delta":"partial"}))
                    .as_bytes(),
                &mut |_| {},
            )
            .unwrap();

        let error = decoder.finish(&mut |_| {}).unwrap_err();

        assert!(error.to_string().contains("terminal event"));
    }

    #[test]
    fn decoder_rejects_tool_item_after_terminal_event() {
        let body = [
            sse_event(json!({
                "type":"response.completed",
                "response":{"status":"completed","output":[]}
            })),
            sse_event(json!({
                "type":"response.output_item.done",
                "output_index":0,
                "item":{
                    "type":"function_call",
                    "call_id":"call_late",
                    "name":"file_write",
                    "arguments":"{}"
                }
            })),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();

        let error = decoder
            .push_chunk(body.as_bytes(), &mut |_| {})
            .unwrap_err();

        assert!(matches!(error, ResponsesError::StreamFailure { .. }));
    }

    #[test]
    fn decoder_accepts_crlf_frames_and_rejects_terminal_status_mismatch() {
        let item = json!({
            "type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]
        });
        let crlf = format!(
            "data: {}\r\n\r\ndata: {}\r\n\r\n",
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":{"status":"completed","output":[item]}})
        );
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(crlf.as_bytes(), &mut |_| {}).unwrap();
        assert_eq!(
            decoder.finish(&mut |_| {}).unwrap().terminal,
            ResponsesTerminal::Completed
        );

        let mismatch = format!(
            "data: {}\n\n",
            json!({"type":"response.completed","response":{"status":"incomplete","output":[]}})
        );
        let mut decoder = ResponsesSseDecoder::default();
        decoder
            .push_chunk(mismatch.as_bytes(), &mut |_| {})
            .unwrap();
        let error = decoder.finish(&mut |_| {}).unwrap_err();
        assert!(error.to_string().contains("event/status"));
    }

    #[test]
    fn decoder_uses_complete_done_items_when_terminal_envelope_omits_output() {
        let reasoning = json!({
            "type":"reasoning","id":"rs_1","encrypted_content":"opaque"
        });
        let call = json!({
            "type":"function_call","status":"completed","call_id":"call_1",
            "name":"file_read","arguments":"{}"
        });
        let body = [
            sse_event(json!({
                "type":"response.output_item.done","output_index":0,"item":reasoning.clone()
            })),
            sse_event(json!({
                "type":"response.output_item.done","output_index":1,"item":call.clone()
            })),
            sse_event(json!({"type":"response.completed","response":{
                "status":"completed"
            }})),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(body.as_bytes(), &mut |_| {}).unwrap();

        let reduced = decoder.finish(&mut |_| {}).unwrap();

        assert_eq!(reduced.output_items, vec![reasoning, call]);
        assert_eq!(reduced.function_calls.len(), 1);
    }

    #[test]
    fn decoder_ignores_terminal_output_without_done_items() {
        let item = json!({
            "type":"message","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"ok"}]
        });
        let body = sse_event(json!({"type":"response.completed","response":{
            "status":"completed","output":[item]
        }}));
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(body.as_bytes(), &mut |_| {}).unwrap();

        let reduced = decoder.finish(&mut |_| {}).unwrap();

        assert!(reduced.output_items.is_empty());
        assert!(reduced.output_text.is_empty());
    }

    #[test]
    fn decoder_ignores_unmatched_terminal_output_and_keeps_done_items() {
        let message = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"ok"}]
        });
        let call = json!({
            "type":"function_call","call_id":"call_1","name":"file_read","arguments":"{}"
        });
        let body = [
            sse_event(json!({
                "type":"response.output_item.done","output_index":0,"item":message.clone()
            })),
            sse_event(json!({"type":"response.completed","response":{
                "status":"completed","output":[call]
            }})),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(body.as_bytes(), &mut |_| {}).unwrap();

        let reduced = decoder.finish(&mut |_| {}).unwrap();

        assert_eq!(reduced.output_items, vec![message]);
        assert_eq!(reduced.output_text, "ok");
        assert!(reduced.function_calls.is_empty());
    }

    #[test]
    fn decoder_rejects_duplicate_gapped_and_malformed_done_items() {
        let message = json!({
            "type":"message","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"ok"}]
        });
        let cases = [
            (
                [
                    sse_event(json!({
                        "type":"response.output_item.done","output_index":0,
                        "item":message.clone()
                    })),
                    sse_event(json!({
                        "type":"response.output_item.done","output_index":0,
                        "item":message.clone()
                    })),
                ]
                .concat(),
                "重复",
            ),
            (
                [
                    sse_event(json!({
                        "type":"response.output_item.done","output_index":1,
                        "item":message.clone()
                    })),
                    sse_event(json!({"type":"response.completed","response":{
                        "status":"completed"
                    }})),
                ]
                .concat(),
                "不连续",
            ),
            (
                sse_event(json!({
                    "type":"response.output_item.done","output_index":0,"item":"invalid"
                })),
                "item object",
            ),
            (
                [
                    sse_event(json!({
                        "type":"response.output_item.done","output_index":0,"item":{}
                    })),
                    sse_event(json!({"type":"response.completed","response":{
                        "status":"completed"
                    }})),
                ]
                .concat(),
                "缺少 type",
            ),
        ];

        for (body, expected) in cases {
            let mut decoder = ResponsesSseDecoder::default();
            let result = decoder
                .push_chunk(body.as_bytes(), &mut |_| {})
                .and_then(|()| decoder.finish(&mut |_| {}));
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn decoder_ignores_terminal_item_shape_differences() {
        let done = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"authoritative done text"}]
        });
        let terminal = json!({
            "type":"message","role":"assistant",
            "content":[{"type":"output_text","text":"terminal summary"}]
        });
        let body = [
            sse_event(json!({
                "type":"response.output_item.done","output_index":0,"item":done.clone()
            })),
            sse_event(json!({"type":"response.completed","response":{
                "status":"completed","output":[terminal]
            }})),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(body.as_bytes(), &mut |_| {}).unwrap();

        let reduced = decoder.finish(&mut |_| {}).unwrap();

        assert_eq!(reduced.output_items, vec![done]);
        assert_eq!(reduced.output_text, "authoritative done text");
    }

    #[test]
    fn decoder_returns_max_output_tokens_incomplete() {
        let item = json!({
            "type":"message","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"partial"}]
        });
        let body = [
            sse_event(json!({"type":"response.output_item.done","output_index":0,"item":item.clone()})),
            sse_event(json!({"type":"response.incomplete","response":{
                "status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[item]
            }})),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(body.as_bytes(), &mut |_| {}).unwrap();

        let reduced = decoder.finish(&mut |_| {}).unwrap();

        assert_eq!(
            reduced.terminal,
            super::super::ResponsesTerminal::MaxOutputTokens
        );
    }

    #[test]
    fn decoder_rejects_incomplete_function_call_in_completed_response() {
        let item = json!({
            "type":"function_call","status":"incomplete","call_id":"call_1",
            "name":"file_write","arguments":"{}"
        });
        let body = [
            sse_event(
                json!({"type":"response.output_item.done","output_index":0,"item":item.clone()}),
            ),
            sse_event(json!({"type":"response.completed","response":{
                "status":"completed","output":[item]
            }})),
        ]
        .concat();
        let mut decoder = ResponsesSseDecoder::default();
        decoder.push_chunk(body.as_bytes(), &mut |_| {}).unwrap();

        let error = decoder.finish(&mut |_| {}).unwrap_err();

        assert!(error.to_string().contains("未完成"));
    }

    #[test]
    fn decoder_surfaces_failed_and_error_events_without_payload_dump() {
        for event in [
            json!({"type":"response.failed","response":{"error":{"message":"model failed","secret":"opaque"}}}),
            json!({"type":"error","message":"bad request","secret":"opaque"}),
        ] {
            let mut decoder = ResponsesSseDecoder::default();
            let error = decoder
                .push_chunk(sse_event(event).as_bytes(), &mut |_| {})
                .unwrap_err();
            assert!(!error.to_string().contains("opaque"));
        }
    }

    #[test]
    fn decoder_redacts_replay_echoed_inside_error_message() {
        let secret = "opaque-reasoning-replay";
        let event = json!({
            "type":"error",
            "message":format!(
                "invalid request: {}",
                json!({"input":[{"type":"reasoning","encrypted_content":secret}]})
            )
        });
        let mut decoder = ResponsesSseDecoder::default();

        let error = decoder
            .push_chunk(sse_event(event).as_bytes(), &mut |_| {})
            .unwrap_err();
        let display = error.to_string();

        assert!(!display.contains(secret));
        assert!(display.contains("redacted Responses request/replay payload"));
    }
}
