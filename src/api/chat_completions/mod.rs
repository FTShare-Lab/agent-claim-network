//! OpenAI-compatible Chat Completions 协议层。
//!
//! 本模块只负责通用 `/chat/completions` HTTP、DTO、SSE 解析和 retry。
//! 上层 provider adapter / router rerank 负责业务 prompt、canonical message 转换和输出解释。

mod client;
mod protocol;
mod streaming;

pub(super) fn normalize_chat_error_body(body: &str) -> String {
    let structured_code = structured_chat_error_code(body);
    let safe_code = safe_chat_error_code(body);
    let classification_text =
        crate::api::provider_error_message(body).unwrap_or_else(|| body.to_string());
    let code = match (structured_code.as_deref(), safe_code.as_deref()) {
        (Some(_), None) => Some("redacted"),
        (_, Some(code)) if crate::api::is_provider_non_request_error_code(code) => Some(code),
        (_, Some(code)) if !matches!(code, "invalid_request" | "invalid_request_error") => {
            Some(code)
        }
        _ if crate::api::is_context_window_error_body(&classification_text) => {
            Some("context_length_exceeded")
        }
        _ if crate::api::is_content_policy_error_body(&classification_text) => {
            Some(content_policy_code(&classification_text))
        }
        (_, Some(code)) => Some(code),
        _ => None,
    };
    let mut error =
        serde_json::json!({"message": crate::api::provider_error_display_message(body)});
    if let Some(code) = code {
        error["code"] = serde_json::Value::String(code.to_string());
    }
    serde_json::json!({"error": error}).to_string()
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

fn safe_chat_error_code(body: &str) -> Option<String> {
    let code = structured_chat_error_code(body)?;
    super::safe_provider_error_code(&code, super::ProviderErrorCodeProtocol::ChatCompletions)
        .is_some()
        .then_some(code)
}

fn structured_chat_error_code(body: &str) -> Option<String> {
    crate::api::provider_error_code(body)
}

pub(crate) use client::is_stream_failure;
pub use client::{ChatCompletionsClient, ChatCompletionsError, ChatStreamEvent};
pub use protocol::{
    ChatCompletionChoice, ChatCompletionMessage, ChatCompletionRequest, ChatCompletionResponse,
    ChatContentPart, ChatFileData, ChatFinishReason, ChatImageUrl, ChatMessage, ChatMessageContent,
    ChatStreamOptions, ChatTool, ChatToolCall, ChatToolCallFunction,
};

#[cfg(test)]
mod tests {
    use super::normalize_chat_error_body;

    #[test]
    fn normalization_preserves_context_limit_classification_and_message() {
        let secret = "private prompt copied by upstream";
        let body = format!(r#"{{"error":{{"message":"prompt is too long: {secret}"}}}}"#);

        let redacted = normalize_chat_error_body(&body);

        assert!(redacted.contains("context_length_exceeded"));
        assert!(redacted.contains(secret));
    }

    #[test]
    fn normalization_preserves_content_policy_classification_and_message() {
        let secret = "private tool arguments copied by upstream";
        let body =
            format!(r#"{{"error":{{"code":"content_filter","message":"blocked: {secret}"}}}}"#);

        let redacted = normalize_chat_error_body(&body);

        assert!(redacted.contains("content_filter"));
        assert!(redacted.contains(secret));
    }

    #[test]
    fn structured_non_request_code_blocks_free_text_reclassification() {
        let body =
            r#"{"error":{"code":"rate_limit_error","message":"echo: maximum context length"}}"#;

        let redacted = normalize_chat_error_body(body);

        assert!(redacted.contains("rate_limit_error"));
        assert!(!redacted.contains("context_length_exceeded"));
        assert!(redacted.contains("maximum context length"));
    }

    #[test]
    fn null_code_falls_back_to_structured_type() {
        for error_type in [
            "authentication_error",
            "rate_limit_error",
            "model_not_found",
        ] {
            let body = format!(
                r#"{{"error":{{"code":null,"type":"{error_type}","message":"invalid request"}}}}"#
            );
            let redacted = normalize_chat_error_body(&body);

            assert_eq!(
                crate::api::provider_error_code(&redacted).as_deref(),
                Some(error_type)
            );
            assert!(!crate::api::is_provider_request_error(400, &redacted));
        }
    }

    #[test]
    fn generic_code_only_classifies_the_error_message() {
        let body = r#"{"error":{"code":"invalid_request_error","message":"invalid tool schema"},"request":{"input":"maximum context length content_filter"}}"#;

        let redacted = normalize_chat_error_body(body);

        assert!(redacted.contains("invalid_request_error"));
        assert!(!redacted.contains("context_length_exceeded"));
        assert!(!redacted.contains("content_filter"));
    }

    #[test]
    fn redaction_preserves_the_difference_between_absent_and_unknown_codes() {
        let without_code =
            normalize_chat_error_body(r#"{"error":{"message":"ordinary invalid parameter"}}"#);
        assert!(crate::api::provider_error_code(&without_code).is_none());
        assert!(crate::api::is_provider_request_error(400, &without_code));

        let unknown_code = normalize_chat_error_body(
            r#"{"error":{"code":"future_error","message":"maximum context length"}}"#,
        );
        assert_eq!(
            crate::api::provider_error_code(&unknown_code).as_deref(),
            Some("redacted")
        );
        assert!(!crate::api::is_provider_request_error(400, &unknown_code));
    }
}
