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
pub(crate) mod evaluation_usage;
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

const MAX_REDACTED_MEDIA_ERROR_BODY_CHARS: usize = 1_000;

fn redact_media_error_body(body: &str) -> String {
    let mut redacted = match serde_json::from_str::<serde_json::Value>(body) {
        Ok(mut value) => {
            redact_media_json_value(&mut value);
            value.to_string()
        }
        Err(_) => body.to_string(),
    };
    redacted = redact_inline_base64_payloads(&redacted);
    truncate_chars(&redacted, MAX_REDACTED_MEDIA_ERROR_BODY_CHARS)
}

fn redact_media_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                if matches!(key.as_str(), "data" | "file_data" | "url") {
                    if let serde_json::Value::String(text) = child {
                        if looks_like_media_payload(text) {
                            *text = "[redacted media payload]".into();
                            continue;
                        }
                    }
                }
                redact_media_json_value(child);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                redact_media_json_value(child);
            }
        }
        serde_json::Value::String(text) => {
            *text = redact_inline_base64_payloads(text);
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn looks_like_media_payload(text: &str) -> bool {
    text.starts_with("data:") || text.len() > 256 && text.chars().all(is_base64_payload_char)
}

fn redact_inline_base64_payloads(input: &str) -> String {
    const MARKER: &str = ";base64,";
    let mut out = String::new();
    let mut rest = input;
    while let Some(pos) = rest.find(MARKER) {
        let marker_end = pos + MARKER.len();
        out.push_str(&rest[..marker_end]);
        let after_marker = &rest[marker_end..];
        let payload_len = after_marker
            .find(|ch| !is_base64_payload_char(ch))
            .unwrap_or(after_marker.len());
        out.push_str("[redacted media payload]");
        rest = &after_marker[payload_len..];
    }
    out.push_str(rest);
    out
}

fn is_base64_payload_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '+' | '/' | '=' | '-' | '_')
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (count, ch) in text.chars().enumerate() {
        if count >= max_chars {
            out.push_str("...[truncated]");
            return out;
        }
        out.push(ch);
    }
    out
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
pub use evaluation_usage::{
    with_evaluation_usage_recording, EvaluationUsage, EvaluationUsageRecorder,
};
pub use memory_review_loop::{MemoryReviewLoop, MEMORY_REVIEW_MAX_TOOL_LOOP_TURNS};
pub use openai_compatible_chat::{OpenAiCompatibleChatError, OpenAiCompatibleChatProviderAdapter};
pub use openai_compatible_responses::{
    OpenAiCompatibleResponsesError, OpenAiCompatibleResponsesProviderAdapter,
};
pub use placeholder::{resolve_placeholders, PlaceholderError};
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
pub(crate) use provider::{ProviderRequestTooLarge, ProviderStreamFailure};
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
    use super::redact_media_error_body;

    #[test]
    fn media_error_body_redacts_json_media_fields() {
        let body = serde_json::json!({
            "error": {
                "message": "bad image",
                "source": {"data": "A".repeat(300)}
            },
            "file_data": "data:application/pdf;base64,QUJD"
        })
        .to_string();

        let redacted = redact_media_error_body(&body);

        assert!(redacted.contains("bad image"));
        assert!(redacted.contains("[redacted media payload]"));
        assert!(!redacted.contains(&"A".repeat(300)));
        assert!(!redacted.contains("QUJD"));
    }

    #[test]
    fn media_error_body_redacts_inline_data_urls_and_truncates() {
        let body = format!(
            "bad request data:image/png;base64,{} {}",
            "A".repeat(300),
            "B".repeat(2_000)
        );

        let redacted = redact_media_error_body(&body);

        assert!(redacted.contains("data:image/png;base64,[redacted media payload]"));
        assert!(!redacted.contains(&"A".repeat(300)));
        assert!(redacted.contains("[truncated]"));
        assert!(redacted.chars().count() <= 1_020);
    }
}
