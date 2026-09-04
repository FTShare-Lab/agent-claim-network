//! LLM 客户端抽象。
//!
//! - `ProviderAdapter`：交互式 session 主路径使用的 provider-neutral 接口
//! - `AnthropicProviderAdapter`：调用 Anthropic Messages API，并只做协议转换
//! - `placeholder`：把 LLM 一次响应里的 `$new_claim_N$` / `$new_dispute_N$`
//!   占位符替换为真实 id（导出 `resolve_placeholders` 给 runner 用）
//!
//! 具体实现切换在 `bootstrap` 阶段完成（取决于 config 里的 `[agent.llm].provider`）。

mod anthropic;
mod buffered_provider;
mod chat_completions;
mod compaction_projection;
mod continuation;
mod embedding;
mod endpoint;
mod llm_http;
mod memory_review_loop;
mod openai_compatible_chat;
mod openai_compatible_responses;
mod placeholder;
mod provider;
mod responses;
mod structured_json;
mod token_estimate;
mod tool_boundary;
mod turn_loop;
mod types;

pub(crate) use turn_loop::replace_media_before_latest_recovery_boundary;

fn is_context_window_error_body(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    body.contains("context_length_exceeded")
        || body.contains("maximum context length")
        || body.contains("prompt is too long")
        || body.contains("prompt too long")
        || body.contains("input is too long")
        || body.contains("input exceeds the model context window")
        || body.contains("input exceeds the context window")
        || body.contains("context window exceeded")
        || body.contains("context window is full")
        || body.contains("exceeds the context length")
}

fn is_content_policy_error_body(body: &str) -> bool {
    body.to_ascii_lowercase()
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .any(|token| {
            matches!(
                token,
                "content_filter" | "content_policy_violation" | "safety_violation"
            )
        })
}

fn is_provider_request_rejection_status(status: u16) -> bool {
    matches!(status, 400 | 415 | 422)
}

fn is_provider_request_too_large_code(code: &str) -> bool {
    matches!(
        code,
        "request_too_large" | "request_entity_too_large" | "payload_too_large"
    )
}

fn provider_error_code(body: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(body).ok()?;
    structured_provider_error_code(&value).map(str::to_string)
}

fn structured_provider_error_code(value: &serde_json::Value) -> Option<&str> {
    let codes = [
        "/code",
        "/error/code",
        "/error/error/code",
        "/response/error/code",
        "/response/error/error/code",
    ];
    let types = [
        "/error/type",
        "/error/error/type",
        "/response/error/type",
        "/response/error/error/type",
        "/type",
    ];
    let read = |pointer| {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .filter(|code| *code != "error" && !code.starts_with("response."))
    };
    let code = codes.into_iter().find_map(read);
    let known = |code: &&str| {
        is_provider_non_request_error_code(code)
            || is_provider_deterministic_request_error_code(code)
            || is_provider_request_too_large_code(code)
            || *code == "websocket_message_too_big"
    };
    // 未识别的细粒度 code 不应遮蔽协议已声明的错误类别。
    code.filter(known)
        .or_else(|| types.into_iter().filter_map(read).find(known))
        .or(code)
        .or_else(|| types.into_iter().find_map(read))
}

fn is_provider_media_error_code(code: &str) -> bool {
    matches!(
        code,
        "invalid_image"
            | "invalid_image_url"
            | "image_too_large"
            | "unsupported_image"
            | "unsupported_media_type"
    )
}

fn is_provider_media_error(code: Option<&str>, message: &str) -> bool {
    if code.is_some_and(is_provider_non_request_error_code)
        || code.is_some_and(is_context_window_error_body)
        || code.is_some_and(is_content_policy_error_body)
        || is_context_window_error_body(message)
        || is_content_policy_error_body(message)
    {
        return false;
    }
    if code.is_some_and(is_provider_media_error_code) {
        return true;
    }
    let message = message.to_ascii_lowercase();
    [
        "invalid image",
        "invalid_image",
        "invalid_image_url",
        "image_too_large",
        "unsupported_image",
        "unsupported_media_type",
        "unsupported image",
        "unsupported media type",
        "image format is not supported",
        "image is too large",
        "invalid pdf",
        "unsupported pdf",
        "pdf is not supported",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

fn is_provider_media_error_body(body: &str) -> bool {
    is_provider_media_error(
        provider_error_code(body).as_deref(),
        &provider_error_message(body).unwrap_or_else(|| body.to_string()),
    )
}

fn provider_error_message(body: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(body).ok()?;
    value
        .get("error")
        .and_then(serde_json::Value::as_object)
        .and_then(|error| {
            error.get("message").or_else(|| {
                error
                    .get("error")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|nested| nested.get("message"))
            })
        })
        .or_else(|| value.pointer("/response/error/message"))
        .or_else(|| value.get("message"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn is_provider_non_request_error_code(code: &str) -> bool {
    matches!(
        code,
        "authentication_error"
            | "invalid_api_key"
            | "permission_error"
            | "not_found_error"
            | "model_not_found"
            | "rate_limit_error"
            | "rate_limit_exceeded"
            | "server_error"
            | "api_error"
            | "overloaded_error"
            | "internal_server_error"
            | "service_unavailable"
            | "temporarily_unavailable"
    )
}

fn is_provider_deterministic_request_error_code(code: &str) -> bool {
    matches!(
        code,
        "invalid_request" | "invalid_request_error" | "invalid_prompt"
    ) || is_context_window_error_body(code)
        || is_content_policy_error_body(code)
        || is_provider_media_error_code(code)
}

fn is_provider_request_error(status: u16, body: &str) -> bool {
    match provider_error_code(body).as_deref() {
        Some(code) if is_provider_non_request_error_code(code) => false,
        Some(code) if is_provider_deterministic_request_error_code(code) => true,
        Some(_) => false,
        None => is_provider_request_rejection_status(status),
    }
}

fn is_provider_request_too_large(status: u16, _body: &str) -> bool {
    status == 413
}

#[cfg(test)]
mod rejection_classification_tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn media_recovery_is_consistent_across_http_adapters() {
        for protocol in 0..3 {
            for (code, message, expected_requests) in [
                ("invalid_value", "invalid tool schema", 1),
                ("unsupported_media_type", "unsupported image", 2),
                ("invalid_image", "invalid image", 2),
                (
                    "context_length_exceeded",
                    "maximum context length exceeded",
                    1,
                ),
                ("content_policy_violation", "content policy rejected", 1),
            ] {
                let requests = Arc::new(tokio::sync::Mutex::new(Vec::new()));
                let captured = requests.clone();
                let app = axum::Router::new().fallback(move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let captured = captured.clone();
                    async move {
                        captured.lock().await.push(body);
                        (axum::http::StatusCode::BAD_REQUEST, axum::Json(serde_json::json!({
                            "error": {"code": code, "type": "invalid_request_error", "message": message}
                        })))
                    }
                });
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
                let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
                let timeout = Duration::from_secs(5);
                let provider: Arc<dyn ProviderAdapter> = match protocol {
                    0 => Arc::new(
                        OpenAiCompatibleResponsesProviderAdapter::new(
                            "test-key".into(),
                            endpoint,
                            "test-model".into(),
                            timeout,
                            0,
                            Duration::ZERO,
                            Duration::ZERO,
                        )
                        .unwrap(),
                    ),
                    1 => Arc::new(
                        OpenAiCompatibleChatProviderAdapter::new(
                            "test-key".into(),
                            endpoint,
                            "test-model".into(),
                            timeout,
                            0,
                            Duration::ZERO,
                            Duration::ZERO,
                        )
                        .unwrap(),
                    ),
                    _ => Arc::new(
                        AnthropicProviderAdapter::new(
                            "test-key".into(),
                            endpoint,
                            "test-model".into(),
                            128,
                            timeout,
                            0,
                            Duration::ZERO,
                            Duration::ZERO,
                        )
                        .unwrap(),
                    ),
                };
                let tools = Arc::new(
                    crate::tool::ToolRegistry::new(&crate::config::ToolConfig::default()).unwrap(),
                );
                let turn_loop = AgentTurnLoop::new(provider, tools, 128);
                let mut history = SessionTurnMessage::user_text("historical image");
                history.content.push(SessionTurnContentBlock::Image {
                    media_type: "image/png".into(),
                    data: "aW1hZ2U=".into(),
                });
                let mut warnings = Vec::new();
                let error = turn_loop
                    .run_session_turn(
                        SessionTurnRequest {
                            current_session_id: None,
                            current_turn_id: None,
                            system_prompt: "test".into(),
                            history: vec![history],
                            user_text: "new text".into(),
                            user_attachments: Vec::new(),
                            skill_instructions: Vec::new(),
                        },
                        &mut |event| {
                            if let SessionTurnEvent::Warning { message } = event {
                                warnings.push(message);
                            }
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(
                    error.downcast_ref::<ProviderRequestRejected>().is_some(),
                    "protocol={protocol} code={code}: {error:#}"
                );
                assert_eq!(
                    requests.lock().await.len(),
                    expected_requests,
                    "protocol={protocol} code={code}"
                );
                assert_eq!(
                    warnings.len(),
                    expected_requests - 1,
                    "protocol={protocol} code={code}"
                );
                server.abort();
            }
        }
    }

    #[test]
    fn unknown_detail_preserves_known_request_type_through_redaction() {
        let body = serde_json::json!({"error": {
            "code": "invalid_value", "type": "invalid_request_error",
            "message": "Invalid value for input[2]"
        }})
        .to_string();
        for redacted in [
            super::responses::redact_responses_error_body(&body),
            super::chat_completions::redact_chat_error_body(&body),
        ] {
            assert!(
                super::is_provider_request_error(400, &redacted),
                "{redacted}"
            );
        }
        assert!(!super::is_provider_request_error(
            400,
            r#"{"error":{"code":"unrecognized_code"}}"#
        ));
    }
}

pub use anthropic::{AnthropicError, AnthropicProviderAdapter};
pub(crate) use buffered_provider::{send_buffered_with_fallback, BufferedProviderRuntime};
pub use chat_completions::{
    ChatCompletionChoice, ChatCompletionMessage, ChatCompletionRequest, ChatCompletionResponse,
    ChatCompletionsClient, ChatCompletionsError, ChatContentPart, ChatFileData, ChatFinishReason,
    ChatImageUrl, ChatMessage, ChatMessageContent, ChatStreamEvent, ChatStreamOptions, ChatTool,
    ChatToolCall, ChatToolCallFunction,
};
pub use compaction_projection::{
    active_segment_has_large_tool_result, active_segment_messages, active_segments_hash,
    context_recovery_protected_tail_from_marker, context_recovery_protected_tail_segments,
    context_recovery_tail_marker, estimated_projected_segment_tokens,
    large_tool_result_omission_text, project_turn_message_for_safe_transcript,
    project_turn_message_tool_results, project_turn_messages_for_safe_transcript,
    project_turn_messages_tool_results, provider_anchor_end_index, provider_safe_segments,
    trailing_model_context_segments, MessageRange, ProviderProjectionBudget,
};
pub(crate) use compaction_projection::{
    ensure_compaction_request_within_context_window, omit_turn_messages_tool_results,
    project_compaction_input_media, project_compaction_input_tool_results,
    strip_file_edit_authority_compaction_notices, FILE_EDIT_AUTHORITY_COMPACTION_NOTICE,
};
pub use embedding::{
    build_embedding_client, ArkMultimodalEmbeddingClient, EmbeddingCacheFingerprint,
    EmbeddingClient, OpenAiCompatibleEmbeddingClient,
};
pub use memory_review_loop::{MemoryReviewLoop, MEMORY_REVIEW_MAX_TOOL_LOOP_TURNS};
pub use openai_compatible_chat::{OpenAiCompatibleChatError, OpenAiCompatibleChatProviderAdapter};
pub use openai_compatible_responses::{
    OpenAiCompatibleResponsesError, OpenAiCompatibleResponsesProviderAdapter,
};
pub use placeholder::{resolve_placeholders, PlaceholderError};
pub(crate) use provider::ProviderRequestRejected;
pub(crate) use provider::ProviderTransport;
pub use provider::{
    assistant_text_from_message, context_usage_from_anthropic_committed_usage,
    context_usage_from_anthropic_input_usage, context_usage_from_openai_usage,
    ContextUsageSnapshot, ContextUsageSource, ProviderAdapter, ProviderEvent,
    ProviderHistoryMediaPolicy, ProviderRecoveryInterrupt, ProviderReplayIdentity,
    ProviderReplayProtocol, ProviderRequest, ProviderRequestObserver, ProviderResponse,
    ProviderRuntimeChainId, ProviderRuntimeFallbackScope, ProviderStop, ProviderStreamOutputMode,
    ToolSpec,
};
#[cfg(test)]
pub(crate) use provider::{
    ProviderContextWindowExceeded, ProviderMediaRejected, ProviderRequestTooLarge,
    ProviderStreamFailure, ProviderTerminalFailure,
};
pub use responses::{
    ReducedResponses, ResponsesClient, ResponsesError, ResponsesFunctionCall, ResponsesReasoning,
    ResponsesRequest, ResponsesStreamEvent, ResponsesTerminal, ResponsesTool,
};
#[cfg(test)]
pub(crate) use structured_json::StructuredJsonNoConsumableOutput;
pub(crate) use structured_json::{
    structured_json_business_retryable, structured_json_no_consumable_transport,
    StructuredJsonAttemptRequest,
};
pub use structured_json::{StructuredJsonAttemptReport, StructuredJsonCaller};
pub use token_estimate::{
    estimate_json_tokens, estimate_provider_replay_tokens,
    estimate_provider_request_context_tokens, estimate_session_turn_messages_tokens,
    estimate_text_tokens,
};
pub(crate) use tool_boundary::ToolBoundaryControl;
pub use turn_loop::ProviderRejectedRequestRecovery;
pub(crate) use turn_loop::SessionTurnHooks;
pub use turn_loop::{
    AgentTurnLoop, SessionTurnContextAppender, SessionTurnEventRecorder, SessionTurnPreflight,
};
pub use types::{
    AvailableSkill, ClaimAttributeUpdateInternalizeItem, ClaimAttributeUpdateInternalizeRequest,
    ClaimDraft, CompletedSessionTurnMessage, DisputeDraft, InboxInternalizeKind,
    InternalizeOutcome, InternalizeRequest, MemoryReviewRequest, ModelContextSource,
    ProviderReplayState, RecapOutcome, SessionAttachment, SessionCompactionOutcome,
    SessionCompactionRequest, SessionRecapRequest, SessionSearchSummaryOutcome,
    SessionSearchSummaryRequest, SessionTurn, SessionTurnContentBlock, SessionTurnEvent,
    SessionTurnInterrupted, SessionTurnMessage, SessionTurnRequest, ToolCallSkipReason,
    ToolExecutionOutcome, TurnMessage,
};

#[cfg(test)]
mod tests {
    use super::{
        is_content_policy_error_body, is_context_window_error_body, is_provider_request_error,
        is_provider_request_rejection_status,
    };

    #[test]
    fn context_window_error_body_recognizes_common_provider_shapes() {
        for body in [
            r#"{"error":{"code":"context_length_exceeded","message":"maximum context length is 128000 tokens"}}"#,
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long"}}"#,
            "Input exceeds the model context window",
            "context window exceeded",
        ] {
            assert!(is_context_window_error_body(body), "body={body}");
        }
    }

    #[test]
    fn context_window_error_body_does_not_guess_from_unrelated_bad_requests() {
        for body in [
            "unsupported image content type",
            "request body exceeds gateway byte limit",
            "invalid tool schema",
        ] {
            assert!(!is_context_window_error_body(body), "body={body}");
        }
    }

    #[test]
    fn content_policy_error_body_requires_an_explicit_policy_code() {
        for body in [
            r#"{"error":{"code":"content_filter"}}"#,
            r#"{"error":{"type":"content_policy_violation"}}"#,
            "request rejected: safety_violation",
        ] {
            assert!(is_content_policy_error_body(body), "body={body}");
        }
        for body in [
            "unsupported image content type",
            "content filter configuration is invalid",
            "generic safety check failed",
        ] {
            assert!(!is_content_policy_error_body(body), "body={body}");
        }
    }

    #[test]
    fn request_rejection_status_excludes_ambiguous_and_special_failures() {
        for status in [401, 403, 404, 408, 409, 413, 423, 425, 429, 499, 500] {
            assert!(!is_provider_request_rejection_status(status));
        }
        for status in [400, 415, 422] {
            assert!(is_provider_request_rejection_status(status));
        }
    }

    #[test]
    fn structured_error_code_overrides_http_status_for_request_rejection() {
        for code in [
            "authentication_error",
            "invalid_api_key",
            "permission_error",
            "not_found_error",
            "model_not_found",
            "rate_limit_error",
            "server_error",
        ] {
            let body = serde_json::json!({"error":{"code":code}}).to_string();
            assert!(!is_provider_request_error(400, &body), "code={code}");
        }
        let null_code =
            serde_json::json!({"error":{"code":null,"type":"authentication_error"}}).to_string();
        assert!(!is_provider_request_error(400, &null_code));
        for code in [
            "context_length_exceeded",
            "content_filter",
            "invalid_prompt",
            "unsupported_media_type",
        ] {
            let body = serde_json::json!({"error":{"code":code}}).to_string();
            assert!(is_provider_request_error(403, &body), "code={code}");
        }
        for code in ["redacted", "unknown_provider_error"] {
            let body = serde_json::json!({"error":{"code":code}}).to_string();
            assert!(!is_provider_request_error(400, &body), "code={code}");
        }
        assert!(!is_provider_request_error(403, "redacted"));
    }
}
