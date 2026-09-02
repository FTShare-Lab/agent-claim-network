//! OpenAI-compatible Responses HTTP JSON/SSE 协议层。
//!
//! 本模块只负责 `/responses` 请求 DTO、HTTP retry、SSE 终态与完整 output item
//! 归一。canonical session/replay 转换由上层 provider adapter 负责。

mod client;
mod protocol;
mod streaming;
mod websocket;

const REDACTED_RESPONSES_PAYLOAD: &str = "[redacted Responses request/replay payload]";

/// 部分兼容网关把上游 WebSocket 的 1009 大消息关闭包装成 500/502。
/// 只有同时保留关闭码与明确尺寸原因时才识别，避免把普通网关故障误判为请求过大。
pub(super) fn is_explicit_websocket_message_too_big(error: &ResponsesError) -> bool {
    let ResponsesError::Status { status, body } = error else {
        return false;
    };
    if !matches!(*status, 500 | 502) {
        return false;
    }
    let normalized = body.to_ascii_lowercase();
    normalized.contains("1009") && normalized.contains("message too big")
}

pub(super) fn redact_responses_error_body(body: &str) -> String {
    let redacted = match serde_json::from_str::<serde_json::Value>(body) {
        Ok(mut value) => {
            redact_responses_json_value(&mut value);
            value.to_string()
        }
        Err(_) if contains_responses_payload_key(body) => REDACTED_RESPONSES_PAYLOAD.into(),
        Err(_) => body.to_string(),
    };
    crate::api::redact_media_error_body(&redacted)
}

fn redact_responses_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                if matches!(
                    key.as_str(),
                    "request"
                        | "request_body"
                        | "instructions"
                        | "input"
                        | "output"
                        | "reasoning"
                        | "encrypted_content"
                ) {
                    *child = serde_json::Value::String(REDACTED_RESPONSES_PAYLOAD.into());
                } else {
                    redact_responses_json_value(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                redact_responses_json_value(child);
            }
        }
        serde_json::Value::String(text) => {
            if let Ok(mut embedded) = serde_json::from_str::<serde_json::Value>(text) {
                redact_responses_json_value(&mut embedded);
                *text = embedded.to_string();
            } else if contains_responses_payload_key(text) {
                *text = REDACTED_RESPONSES_PAYLOAD.into();
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn contains_responses_payload_key(text: &str) -> bool {
    const KEYS: &[&str] = &[
        "request",
        "request_body",
        "instructions",
        "input",
        "output",
        "reasoning",
        "encrypted_content",
    ];
    let compact = text
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    KEYS.iter().any(|key| {
        [
            format!("\"{key}\":"),
            format!("\\\"{key}\\\":"),
            format!("'{key}':"),
            format!("\\'{key}\\':"),
        ]
        .iter()
        .any(|pattern| compact.contains(pattern))
    })
}

pub(crate) use client::is_stream_recovery_failure;
pub use client::{ResponsesClient, ResponsesError, ResponsesStreamEvent};
pub(crate) use protocol::RESPONSES_TRUNCATION_DISABLED;
pub use protocol::{
    reduce_response_value, ReducedResponses, ResponsesFunctionCall, ResponsesReasoning,
    ResponsesRequest, ResponsesTerminal, ResponsesTool,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_websocket_message_too_big_requires_status_code_and_reason() {
        for status in [500, 502] {
            assert!(is_explicit_websocket_message_too_big(
                &ResponsesError::Status {
                    status,
                    body: "upstream closed (1009 MESSAGE TOO BIG): message too big".into(),
                }
            ));
        }
        for error in [
            ResponsesError::Status {
                status: 502,
                body: "ordinary bad gateway".into(),
            },
            ResponsesError::Status {
                status: 502,
                body: "message too big".into(),
            },
            ResponsesError::Status {
                status: 413,
                body: "upstream closed (1009 message too big)".into(),
            },
        ] {
            assert!(!is_explicit_websocket_message_too_big(&error));
        }
    }

    #[test]
    fn responses_error_redaction_removes_nested_and_embedded_replay() {
        let secret = "opaque-private-replay";
        let body = serde_json::json!({
            "error": {
                "message": format!(
                    "rejected body: {}",
                    serde_json::json!({
                        "input": [{"type":"reasoning","encrypted_content":secret}]
                    })
                ),
                "request": {
                    "instructions":"private system prompt",
                    "reasoning":{"effort":"high"}
                },
                "code":"invalid_request"
            }
        })
        .to_string();

        let redacted = redact_responses_error_body(&body);

        assert!(redacted.contains("invalid_request"));
        assert!(!redacted.contains(secret));
        assert!(!redacted.contains("private system prompt"));
        assert!(redacted.contains(REDACTED_RESPONSES_PAYLOAD));
    }

    #[test]
    fn responses_error_redaction_detects_spaced_and_single_quoted_echoes() {
        for body in [
            r#"invalid request: {\"input\" : [{\"encrypted_content\" : \"secret-a\"}]}"#,
            "invalid request: {'reasoning' : {'encrypted_content' : 'secret-b'}}",
        ] {
            let redacted = redact_responses_error_body(body);

            assert_eq!(redacted, REDACTED_RESPONSES_PAYLOAD);
            assert!(!redacted.contains("secret-"));
        }
    }
}
