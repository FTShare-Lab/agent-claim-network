//! OpenAI-compatible Responses HTTP JSON/SSE 协议层。
//!
//! 本模块只负责 `/responses` 请求 DTO、HTTP retry、SSE 终态与完整 output item
//! 归一。canonical session/replay 转换由上层 provider adapter 负责。

mod client;
mod protocol;
mod streaming;
mod websocket;

pub(super) fn is_transient_error_code(code: &str) -> bool {
    super::is_provider_transient_error_code(code)
}

pub(super) fn is_deterministic_request_error_code(code: &str) -> bool {
    super::is_provider_explicit_request_error_code(code)
}

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
    normalized.contains("websocket_message_too_big")
        || normalized.contains("1009") && normalized.contains("message too big")
}

pub(super) fn normalize_responses_error_body(body: &str) -> String {
    let mut error =
        serde_json::json!({"message": crate::api::provider_error_display_message(body)});
    if let Some(code) = classified_responses_error_code(body) {
        error["code"] = serde_json::Value::String(code);
    }
    serde_json::json!({"error": error}).to_string()
}

pub(super) fn normalize_responses_error_message_with_code(
    message: &str,
    code: Option<&str>,
) -> String {
    let normalized = message.to_ascii_lowercase();
    let classified = if normalized.contains("1009") && normalized.contains("message too big") {
        "websocket_message_too_big".to_string()
    } else {
        match code {
            Some(code)
                if crate::api::is_provider_non_request_error_code(code)
                    || !matches!(code, "invalid_request" | "invalid_request_error") =>
            {
                code.to_string()
            }
            _ => classified_responses_error_code(message)
                .or_else(|| code.map(str::to_string))
                .unwrap_or_else(|| "redacted".into()),
        }
    };
    format!("{classified}: {message}")
}

pub(super) fn safe_responses_error_code(code: &str) -> Option<&str> {
    super::safe_provider_error_code(code, super::ProviderErrorCodeProtocol::Responses)
}

fn classified_responses_error_code(body: &str) -> Option<String> {
    let normalized = body.to_ascii_lowercase();
    if normalized.contains("1009") && normalized.contains("message too big") {
        return Some("websocket_message_too_big".into());
    }
    let structured_code = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| structured_responses_error_code(&value).map(str::to_string));
    if let Some(code) = structured_code.as_deref() {
        if crate::api::is_provider_non_request_error_code(code)
            || !matches!(code, "invalid_request" | "invalid_request_error")
        {
            return Some(
                safe_responses_error_code(code)
                    .unwrap_or("redacted")
                    .to_string(),
            );
        }
    }
    let classification_text =
        crate::api::provider_error_message(body).unwrap_or_else(|| body.to_string());
    if crate::api::is_context_window_error_body(&classification_text) {
        return Some("context_length_exceeded".into());
    }
    if crate::api::is_content_policy_error_body(&classification_text) {
        return Some(content_policy_code(&classification_text).into());
    }
    structured_code
        .as_deref()
        .and_then(safe_responses_error_code)
        .map(str::to_string)
}

fn structured_responses_error_code(value: &serde_json::Value) -> Option<&str> {
    crate::api::structured_provider_error_code(value)
}

fn content_policy_code(body: &str) -> &'static str {
    let normalized = body.to_ascii_lowercase();
    if normalized.contains("content_policy_violation") {
        "content_policy_violation"
    } else if normalized.contains("safety_violation") {
        "safety_violation"
    } else {
        "content_filter"
    }
}

pub(crate) use client::is_stream_recovery_failure;
pub use client::{ResponsesClient, ResponsesError, ResponsesStreamEvent};
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
    fn responses_error_display_preserves_message_but_omits_other_fields() {
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

        let redacted = normalize_responses_error_body(&body);

        assert!(redacted.contains("invalid_request"));
        assert!(redacted.contains(secret));
        assert!(!redacted.contains("private system prompt"));
    }

    #[test]
    fn responses_error_display_preserves_plain_text() {
        for body in [
            r#"invalid request: {\"input\" : [{\"encrypted_content\" : \"secret-a\"}]}"#,
            "invalid request: {'reasoning' : {'encrypted_content' : 'secret-b'}}",
        ] {
            let redacted = normalize_responses_error_body(body);

            assert!(redacted.contains("secret-"));
        }
    }

    #[test]
    fn generic_code_only_classifies_the_error_message() {
        let body = r#"{"error":{"code":"invalid_request_error","message":"invalid tool schema"},"request":{"input":"maximum context length content_filter"}}"#;

        let redacted = normalize_responses_error_body(body);

        assert!(redacted.contains("invalid_request_error"));
        assert!(!redacted.contains("context_length_exceeded"));
        assert!(!redacted.contains("content_filter"));
    }

    #[test]
    fn redaction_preserves_the_difference_between_absent_and_unknown_codes() {
        let without_code =
            normalize_responses_error_body(r#"{"error":{"message":"ordinary invalid parameter"}}"#);
        assert!(crate::api::provider_error_code(&without_code).is_none());
        assert!(crate::api::is_provider_request_error(422, &without_code));

        let unknown_code = normalize_responses_error_body(
            r#"{"error":{"code":"future_error","message":"maximum context length"}}"#,
        );
        assert_eq!(
            crate::api::provider_error_code(&unknown_code).as_deref(),
            Some("redacted")
        );
        assert!(!crate::api::is_provider_request_error(400, &unknown_code));
    }
}
