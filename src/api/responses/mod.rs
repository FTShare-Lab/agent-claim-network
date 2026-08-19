//! OpenAI-compatible Responses HTTP JSON/SSE 协议层。
//!
//! 本模块只负责 `/responses` 请求 DTO、HTTP retry、SSE 终态与完整 output item
//! 归一。canonical session/replay 转换由上层 provider adapter 负责。

mod client;
mod protocol;
mod streaming;
mod websocket;

const REDACTED_RESPONSES_PAYLOAD: &str = "[redacted Responses request/replay payload]";

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
