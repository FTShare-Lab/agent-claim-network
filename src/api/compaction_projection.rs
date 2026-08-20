//! provider history 压缩投影的通用纯逻辑。
//!
//! 本模块只处理 `SessionTurnMessage` 层面的安全切段、tool_result / summary 媒体投影和
//! token 估算，不读写 session/delegation，也不理解 recap 或 claim 语义。

use serde::Serialize;

use super::{
    estimate_provider_request_context_tokens, estimate_session_turn_messages_tokens,
    SessionTurnContentBlock, SessionTurnMessage,
};

const STABLE_HASH_OFFSET: u64 = 0xcbf29ce484222325;
const STABLE_HASH_PRIME: u64 = 0x100000001b3;
const COMPACTION_MEDIA_METADATA_MAX_CHARS: usize = 128;

pub(crate) const FILE_EDIT_AUTHORITY_COMPACTION_NOTICE: &str = "File permission boundary: this compaction cleared runtime file-edit authority derived from earlier file_read or @file content, even if a read remains mentioned in this summary or a preserved raw tail. Treat those reads as historical context, not current authorization. Before file_patch or file_write on an existing file, establish fresh authority with file_read and follow required_read.";

pub(crate) fn strip_file_edit_authority_compaction_notices(messages: &mut [SessionTurnMessage]) {
    for message in messages {
        for block in &mut message.content {
            if let SessionTurnContentBlock::Text { text } = block {
                let is_compaction_wrapper = text.contains("<compacted_session_context>")
                    || text.contains("<compacted_current_turn_progress>");
                if is_compaction_wrapper {
                    *text = text.replace(FILE_EDIT_AUTHORITY_COMPACTION_NOTICE, "");
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderProjectionBudget {
    pub tail_token_limit: usize,
    pub tail_hard_token_limit: usize,
    pub tail_previous_real_user_turns: usize,
    pub tool_result_raw_max_chars: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageRange {
    pub start: usize,
    pub end: usize,
}

pub fn project_turn_message_tool_results(
    message: SessionTurnMessage,
    tool_result_raw_max_chars: usize,
) -> SessionTurnMessage {
    project_turn_message_tool_results_with(
        message,
        tool_result_raw_max_chars,
        large_tool_result_omission_text,
    )
}

fn project_turn_message_tool_results_with(
    mut message: SessionTurnMessage,
    tool_result_raw_max_chars: usize,
    omission_text: fn(usize) -> String,
) -> SessionTurnMessage {
    for block in &mut message.content {
        if let SessionTurnContentBlock::ToolResult { content, .. } = block {
            let original_chars = content.chars().count();
            if original_chars > tool_result_raw_max_chars {
                *content = omission_text(original_chars);
            }
        }
    }
    message
}

pub fn project_turn_messages_tool_results(
    messages: impl IntoIterator<Item = SessionTurnMessage>,
    tool_result_raw_max_chars: usize,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(|message| project_turn_message_tool_results(message, tool_result_raw_max_chars))
        .collect()
}

pub(crate) fn project_compaction_input_tool_results(
    messages: impl IntoIterator<Item = SessionTurnMessage>,
    tool_result_raw_max_chars: usize,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(|message| {
            project_turn_message_tool_results_with(
                message,
                tool_result_raw_max_chars,
                compaction_input_tool_result_omission_text,
            )
        })
        .collect()
}

pub(crate) fn omit_turn_messages_tool_results(
    messages: impl IntoIterator<Item = SessionTurnMessage>,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(|mut message| {
            for block in &mut message.content {
                if let SessionTurnContentBlock::ToolResult { content, .. } = block {
                    *content = compaction_input_tool_result_omission_text(content.chars().count());
                }
            }
            message
        })
        .collect()
}

pub(crate) fn project_compaction_input_media(
    mut message: SessionTurnMessage,
) -> SessionTurnMessage {
    message.provider_replay = None;
    for block in &mut message.content {
        let omission = match block {
            SessionTurnContentBlock::Image { media_type, data } => Some(
                compaction_input_media_omission_text("image", media_type, None, data.len()),
            ),
            SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            } => Some(compaction_input_media_omission_text(
                "document",
                media_type,
                filename.as_deref(),
                data.len(),
            )),
            SessionTurnContentBlock::Text { .. }
            | SessionTurnContentBlock::ModelContext { .. }
            | SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => None,
        };
        if let Some(omission) = omission {
            *block = SessionTurnContentBlock::text(omission);
        }
    }
    message
}

pub(crate) fn ensure_compaction_request_within_context_window(
    system_prompt: &str,
    messages: &[SessionTurnMessage],
    context_window: usize,
    max_tokens: u32,
) -> anyhow::Result<()> {
    let input_tokens =
        estimate_provider_request_context_tokens(system_prompt, messages, &[]).used_tokens;
    let output_tokens = usize::try_from(max_tokens).unwrap_or(usize::MAX);
    let total_tokens = input_tokens.saturating_add(output_tokens);
    if total_tokens > context_window {
        anyhow::bail!(
            "compaction summary request exceeds context window: estimated input tokens={input_tokens}, reserved output tokens={output_tokens}, total tokens={total_tokens}, context window={context_window}"
        );
    }
    Ok(())
}

fn compaction_input_tool_result_omission_text(original_chars: usize) -> String {
    format!(
        "[tool_result omitted from compaction summary input; original_chars={original_chars}. \
Exact output is unavailable in this request.]"
    )
}

fn compaction_input_media_omission_text(
    kind: &str,
    media_type: &str,
    filename: Option<&str>,
    base64_chars: usize,
) -> String {
    let media_type = bounded_media_metadata(media_type);
    let filename = filename
        .map(bounded_media_metadata)
        .map(|filename| format!("; filename={filename}"))
        .unwrap_or_default();
    format!(
        "[{kind} omitted from compaction summary input; media_type={media_type}{filename}; \
base64_chars={base64_chars}. Exact media is unavailable in this request.]"
    )
}

fn bounded_media_metadata(value: &str) -> String {
    value
        .chars()
        .take(COMPACTION_MEDIA_METADATA_MAX_CHARS)
        .map(|ch| if ch.is_control() { '�' } else { ch })
        .collect()
}

/// 把 provider message 投影为可进入摘要、审计和可读 transcript 的 canonical 形态。
/// provider replay 只服务同协议请求；媒体在这些派生路径中只保留不可逆占位信息。
pub fn project_turn_message_for_safe_transcript(
    mut message: SessionTurnMessage,
) -> SessionTurnMessage {
    message.provider_replay = None;
    message.content = message
        .content
        .into_iter()
        .map(|block| match block {
            SessionTurnContentBlock::Image { media_type, data } => {
                let media_type = bounded_media_metadata(&media_type);
                SessionTurnContentBlock::text(format!(
                    "[image attachment media_type={media_type} base64_bytes={}]",
                    data.len()
                ))
            }
            SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            } => {
                let media_type = bounded_media_metadata(&media_type);
                let text = match filename {
                    Some(filename) => {
                        let filename = bounded_media_metadata(&filename);
                        format!(
                            "[document attachment media_type={media_type} filename={filename} base64_bytes={}]",
                            data.len()
                        )
                    }
                    None => format!(
                        "[document attachment media_type={media_type} base64_bytes={}]",
                        data.len()
                    ),
                };
                SessionTurnContentBlock::text(text)
            }
            other => other,
        })
        .collect();
    message
}

pub fn project_turn_messages_for_safe_transcript(
    messages: impl IntoIterator<Item = SessionTurnMessage>,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(project_turn_message_for_safe_transcript)
        .collect()
}

pub fn large_tool_result_omission_text(original_chars: usize) -> String {
    format!(
        "[large tool_result omitted from raw compact tail; original_chars={original_chars}. The compaction summary keeps the key facts. Re-call the tool if exact output is needed.]"
    )
}

pub fn provider_safe_segments(active_suffix: &[SessionTurnMessage]) -> Vec<MessageRange> {
    let mut ranges = Vec::new();
    let mut index = provider_anchor_end_index(active_suffix);
    while index < active_suffix.len() {
        let message = &active_suffix[index];
        if message.role == "assistant" {
            let tool_use_ids = message
                .content
                .iter()
                .filter_map(|block| match block {
                    SessionTurnContentBlock::ToolUse { id, .. } => Some(id.as_str()),
                    SessionTurnContentBlock::Text { .. }
                    | SessionTurnContentBlock::ModelContext { .. }
                    | SessionTurnContentBlock::SkillInstructions { .. }
                    | SessionTurnContentBlock::Image { .. }
                    | SessionTurnContentBlock::Document { .. }
                    | SessionTurnContentBlock::ToolResult { .. } => None,
                })
                .collect::<Vec<_>>();
            if tool_use_ids.is_empty() {
                ranges.push(MessageRange {
                    start: index,
                    end: index + 1,
                });
                index += 1;
                continue;
            }
            let Some(tool_result) = active_suffix.get(index + 1) else {
                break;
            };
            if tool_result.role != "user" || !tool_result_contains_all(tool_result, &tool_use_ids) {
                break;
            }
            ranges.push(MessageRange {
                start: index,
                end: index + 2,
            });
            index += 2;
            continue;
        }

        if message.role == "user" && !message_contains_tool_result(message) {
            ranges.push(MessageRange {
                start: index,
                end: index + 1,
            });
            index += 1;
            continue;
        }

        break;
    }
    ranges
}

/// 返回 active history 尾部尚未投递的独立 ModelContext 所占安全段数。
///
/// context appender 会在 compaction 前先冻结并持久化这些消息；本次压缩必须原样保留，
/// 否则 transcript 会记录一份 Provider 从未实际看见的快照。
pub fn trailing_model_context_segments(
    messages: &[SessionTurnMessage],
    segments: &[MessageRange],
) -> usize {
    segments
        .iter()
        .rev()
        .take_while(|segment| {
            segment.end == segment.start.saturating_add(1)
                && messages
                    .get(segment.start)
                    .is_some_and(|message| message.model_context_snapshot().is_some())
        })
        .count()
}

/// active suffix 的不可压缩前缀：零到多条初始 ModelContext，加上首条真实 user/objective。
pub fn provider_anchor_end_index(messages: &[SessionTurnMessage]) -> usize {
    let context_end = messages
        .iter()
        .take_while(|message| message.model_context_snapshot().is_some())
        .count();
    if messages.get(context_end).is_some_and(|message| {
        message.role == "user"
            && message.model_context_snapshot().is_none()
            && !message_contains_tool_result(message)
    }) {
        return context_end.saturating_add(1);
    }
    context_end.max(usize::from(!messages.is_empty()))
}

/// provider 明确报告上下文窗口耗尽后，最近的 partial assistant 及其配对 user
/// message 必须原样参与下一次请求。普通 compaction 不使用这个额外保护边界。
pub fn context_recovery_protected_tail_segments(
    messages: &[SessionTurnMessage],
    segments: &[MessageRange],
) -> usize {
    let Some(last) = segments.last() else {
        return 0;
    };
    if last.end != messages.len() {
        return 0;
    }

    if last.end.saturating_sub(last.start) == 2 {
        // assistant tool_use + 对应 tool_result 是一个不可拆分的安全段。
        return 1;
    }

    if messages
        .get(last.start)
        .is_some_and(|message| message.role == "user")
        && segments.len() >= 2
    {
        let previous = &segments[segments.len() - 2];
        if previous.end == last.start
            && previous.end.saturating_sub(previous.start) == 1
            && messages
                .get(previous.start)
                .is_some_and(|message| message.role == "assistant")
        {
            // 无工具的截断回复由 assistant partial + 内部 continuation 两段组成。
            return 2;
        }
    }

    1
}

/// 记录第一次 context-window 恢复必须保留的 assistant 起点。
///
/// 后续 provider/tool 轮次可能继续增长 active tail；调用方用这个稳定消息重新定位，
/// 从而持续保护整条恢复链，而不是每次只保护最新的一对消息。
pub fn context_recovery_tail_marker(
    messages: &[SessionTurnMessage],
    segments: &[MessageRange],
) -> Option<SessionTurnMessage> {
    let protected = context_recovery_protected_tail_segments(messages, segments);
    if protected == 0 || protected > segments.len() {
        return None;
    }
    let start = segments.get(segments.len() - protected)?.start;
    messages.get(start).cloned()
}

/// 根据第一次恢复时记录的 assistant 消息，计算当前必须保留的完整 active tail。
pub fn context_recovery_protected_tail_from_marker(
    messages: &[SessionTurnMessage],
    segments: &[MessageRange],
    marker: &SessionTurnMessage,
) -> Option<usize> {
    let marker_segment = segments
        .iter()
        .position(|segment| messages.get(segment.start) == Some(marker))?;
    Some(segments.len().saturating_sub(marker_segment))
}

pub fn tool_result_contains_all(message: &SessionTurnMessage, tool_use_ids: &[&str]) -> bool {
    tool_use_ids.iter().all(|id| {
        message.content.iter().any(|block| {
            matches!(
                block,
                SessionTurnContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == id
            )
        })
    })
}

pub fn message_contains_tool_result(message: &SessionTurnMessage) -> bool {
    message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::ToolResult { .. }))
}

pub fn active_segment_messages<'a>(
    active_suffix: &'a [SessionTurnMessage],
    segments: &[MessageRange],
) -> Vec<&'a SessionTurnMessage> {
    segments
        .iter()
        .flat_map(|range| active_suffix[range.start..range.end].iter())
        .collect()
}

pub fn active_segments_hash(
    active_suffix: &[SessionTurnMessage],
    segments: &[MessageRange],
) -> anyhow::Result<String> {
    let mut hash = STABLE_HASH_OFFSET;
    for message in active_segment_messages(active_suffix, segments) {
        stable_hash_json(&mut hash, message)?;
    }
    Ok(format!("{hash:016x}"))
}

pub fn estimated_projected_segment_tokens(
    active_suffix: &[SessionTurnMessage],
    segment: &MessageRange,
    tool_result_raw_max_chars: usize,
) -> usize {
    let projected = active_suffix[segment.start..segment.end]
        .iter()
        .cloned()
        .map(|message| project_turn_message_tool_results(message, tool_result_raw_max_chars))
        .collect::<Vec<_>>();
    estimate_session_turn_messages_tokens(&projected)
}

pub fn active_segment_has_large_tool_result(
    active_suffix: &[SessionTurnMessage],
    segment: &MessageRange,
    tool_result_raw_max_chars: usize,
) -> bool {
    active_suffix[segment.start..segment.end]
        .iter()
        .flat_map(|message| message.content.iter())
        .any(|block| {
            matches!(
                block,
                SessionTurnContentBlock::ToolResult { content, .. }
                    if content.chars().count() > tool_result_raw_max_chars
            )
        })
}

fn stable_hash_json<T: Serialize>(hash: &mut u64, value: &T) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    stable_hash_update(hash, &bytes);
    stable_hash_update(hash, b"\n");
    Ok(())
}

fn stable_hash_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(STABLE_HASH_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::api::ProviderReplayState;

    fn tool_result(content: &str) -> SessionTurnMessage {
        SessionTurnMessage {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: content.into(),
            }],
            provider_replay: None,
        }
    }

    #[test]
    fn authority_notice_filter_only_changes_compaction_wrappers() {
        let mut messages = vec![
            SessionTurnMessage::user_text(format!(
                "user quoted: {FILE_EDIT_AUTHORITY_COMPACTION_NOTICE}"
            )),
            SessionTurnMessage::user_text(format!(
                "<compacted_session_context>\n{FILE_EDIT_AUTHORITY_COMPACTION_NOTICE}\nsummary\n</compacted_session_context>"
            )),
        ];

        strip_file_edit_authority_compaction_notices(&mut messages);

        let user_message = serde_json::to_string(&messages[0]).unwrap();
        let compacted_message = serde_json::to_string(&messages[1]).unwrap();
        assert!(user_message.contains("runtime file-edit authority"));
        assert!(!compacted_message.contains("runtime file-edit authority"));
        assert!(compacted_message.contains("summary"));
    }

    #[test]
    fn omission_projection_replaces_even_small_tool_results() {
        let projected = omit_turn_messages_tool_results(vec![tool_result("short output")]);

        let serialized = serde_json::to_string(&projected).expect("serialize projection");
        assert!(serialized.contains("tool_result omitted from compaction summary input"));
        assert!(serialized.contains("original_chars=12"));
        assert!(!serialized.contains("short output"));
    }

    #[test]
    fn compaction_input_projection_omits_only_results_above_limit() {
        let projected = project_compaction_input_tool_results(
            vec![tool_result("short"), tool_result("long output")],
            5,
        );

        let serialized = serde_json::to_string(&projected).expect("serialize projection");
        assert!(serialized.contains("short"));
        assert!(serialized.contains("tool_result omitted from compaction summary input"));
        assert!(serialized.contains("original_chars=11"));
        assert!(!serialized.contains("long output"));
        assert!(!serialized.contains("raw compact tail"));
    }

    #[test]
    fn compaction_media_projection_replaces_payloads_with_bounded_metadata() {
        let projected = project_compaction_input_media(
            SessionTurnMessage::user_content(vec![
                SessionTurnContentBlock::image("image/png", "IMAGE_BASE64_PAYLOAD"),
                SessionTurnContentBlock::document_named(
                    "application/pdf",
                    "DOCUMENT_BASE64_PAYLOAD",
                    format!("{}\nignored", "report".repeat(40)),
                ),
                SessionTurnContentBlock::text("keep me"),
            ])
            .with_provider_replay(ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: vec![json!({
                    "type": "reasoning",
                    "encrypted_content": "REPLAY_PAYLOAD"
                })],
            }),
        );

        let serialized = serde_json::to_string(&projected).expect("serialize projection");
        assert_eq!(projected.provider_replay, None);
        assert!(serialized.contains("image omitted from compaction summary input"));
        assert!(serialized.contains("media_type=image/png"));
        assert!(serialized.contains("base64_chars=20"));
        assert!(serialized.contains("document omitted from compaction summary input"));
        assert!(serialized.contains("media_type=application/pdf"));
        assert!(serialized.contains("filename="));
        assert!(!serialized.contains("ignored"));
        assert!(!serialized.contains("IMAGE_BASE64_PAYLOAD"));
        assert!(!serialized.contains("DOCUMENT_BASE64_PAYLOAD"));
        assert!(!serialized.contains("REPLAY_PAYLOAD"));
        assert!(serialized.contains("keep me"));
    }

    #[test]
    fn compaction_request_budget_reserves_max_output_tokens() {
        let messages = vec![SessionTurnMessage::user_text("A".repeat(20))];
        let estimated_input =
            estimate_provider_request_context_tokens("system", &messages, &[]).used_tokens;

        ensure_compaction_request_within_context_window(
            "system",
            &messages,
            estimated_input + 40,
            40,
        )
        .expect("estimated input plus output reserve should fit exactly");
        let error = ensure_compaction_request_within_context_window(
            "system",
            &messages,
            estimated_input + 39,
            40,
        )
        .expect_err("output reserve should push request over the window");

        assert!(error.to_string().contains("reserved output tokens=40"));
        assert!(error.to_string().contains("estimated input tokens="));
    }

    #[test]
    fn compaction_request_budget_uses_shared_chars_per_token_estimate() {
        let messages = vec![SessionTurnMessage::user_text("你好世界".repeat(100))];
        let estimated_input =
            estimate_provider_request_context_tokens("system", &messages, &[]).used_tokens;

        ensure_compaction_request_within_context_window("system", &messages, estimated_input, 0)
            .expect("exact estimate should fit");
        ensure_compaction_request_within_context_window(
            "system",
            &messages,
            estimated_input - 1,
            0,
        )
        .expect_err("one token below the shared estimate should fail");
    }

    #[test]
    fn compaction_request_budget_does_not_count_ascii_bytes_as_tokens() {
        let messages = vec![SessionTurnMessage::user_text("A".repeat(200_000))];

        ensure_compaction_request_within_context_window("system", &messages, 200_000, 65_536)
            .expect("ordinary long ASCII compaction input should remain usable");
    }

    #[test]
    fn safe_transcript_projection_drops_replay_and_raw_media() {
        let message = SessionTurnMessage::user_content(vec![
            SessionTurnContentBlock::text("inspect attachments"),
            SessionTurnContentBlock::image("image/png", "RAW_IMAGE"),
            SessionTurnContentBlock::Document {
                media_type: "application/pdf".into(),
                data: "RAW_PDF".into(),
                filename: Some("brief.pdf".into()),
            },
        ])
        .with_provider_replay(ProviderReplayState::OpenAiResponses {
            model: Some("test-model".into()),
            items: vec![json!({"type":"reasoning","encrypted_content":"RAW_REPLAY"})],
        });

        let projected = project_turn_message_for_safe_transcript(message);
        let rendered = serde_json::to_string(&projected).unwrap();

        assert_eq!(projected.provider_replay, None);
        assert!(!rendered.contains("RAW_IMAGE"));
        assert!(!rendered.contains("RAW_PDF"));
        assert!(!rendered.contains("RAW_REPLAY"));
        assert!(rendered.contains("image attachment media_type=image/png"));
        assert!(rendered.contains("filename=brief.pdf"));
    }

    #[test]
    fn context_recovery_protects_partial_and_internal_continuation() {
        let messages = vec![
            SessionTurnMessage::user_text("current task"),
            SessionTurnMessage::assistant_text("older progress"),
            SessionTurnMessage::assistant_text("latest partial"),
            SessionTurnMessage::user_text("continue internally"),
        ];
        let segments = provider_safe_segments(&messages);

        assert_eq!(
            context_recovery_protected_tail_segments(&messages, &segments),
            2
        );
    }

    #[test]
    fn context_recovery_marker_keeps_the_entire_later_recovery_chain() {
        let mut messages = vec![
            SessionTurnMessage::user_text("current task"),
            SessionTurnMessage::assistant_text("first partial"),
            SessionTurnMessage::user_text("continue internally"),
        ];
        let first_segments = provider_safe_segments(&messages);
        let marker = context_recovery_tail_marker(&messages, &first_segments)
            .expect("first context recovery should establish a marker");

        messages.extend([
            SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "lookup".into(),
                    input: json!({}),
                }],
                provider_replay: None,
            },
            SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "result".into(),
            }]),
            SessionTurnMessage::assistant_text("second partial"),
            SessionTurnMessage::user_text("continue internally again"),
        ]);
        let later_segments = provider_safe_segments(&messages);

        assert_eq!(
            context_recovery_protected_tail_from_marker(&messages, &later_segments, &marker),
            Some(5)
        );
    }

    #[test]
    fn context_recovery_protects_complete_tool_pair_as_one_segment() {
        let messages = vec![
            SessionTurnMessage::user_text("current task"),
            SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "lookup".into(),
                    input: json!({}),
                }],
                provider_replay: None,
            },
            tool_result("done"),
        ];
        let segments = provider_safe_segments(&messages);

        assert_eq!(
            context_recovery_protected_tail_segments(&messages, &segments),
            1
        );
    }

    #[test]
    fn trailing_model_contexts_are_counted_as_mandatory_compaction_segments() {
        let messages = vec![
            SessionTurnMessage::user_text("current task"),
            SessionTurnMessage::assistant_text("completed an earlier step"),
            SessionTurnMessage::model_context(
                crate::api::ModelContextSource::Runtime,
                "<runtime_context>current_date: 2026-08-11</runtime_context>",
            ),
            SessionTurnMessage::model_context(
                crate::api::ModelContextSource::BackgroundProcess,
                "<background_processes>state=completed</background_processes>",
            ),
        ];
        let segments = provider_safe_segments(&messages);

        assert_eq!(trailing_model_context_segments(&messages, &segments), 2);

        let with_real_user_tail =
            [messages, vec![SessionTurnMessage::user_text("new request")]].concat();
        let segments = provider_safe_segments(&with_real_user_tail);
        assert_eq!(
            trailing_model_context_segments(&with_real_user_tail, &segments),
            0
        );
    }
}
