//! SessionEngine compaction/provider projection 纯逻辑。
//!
//! 本模块维护 compacted context 投影、active turn 安全切段、tail budget 选择
//! 与 session message token 估算。它不读写 session、不调用 LLM，只被
//! SessionEngine 的 preflight/manual compaction 流程复用。

#[cfg(test)]
use anyhow::Context;
use num_traits::ToPrimitive;

use crate::api::{
    active_segment_has_large_tool_result as shared_active_segment_has_large_tool_result,
    active_segments_hash as shared_active_segments_hash, estimate_json_tokens,
    estimate_session_turn_messages_tokens, estimate_text_tokens,
    estimated_projected_segment_tokens, large_tool_result_omission_text,
    project_turn_message_tool_results, provider_safe_segments, SessionTurnMessage,
};
pub(super) use crate::api::{active_segment_messages, MessageRange, ProviderProjectionBudget};
use crate::session::{
    ActiveTurnCompactionCursor, SessionCompactionState, SessionContentBlock, SessionMessage,
    SessionMessageRole,
};

use super::transcript::{
    flatten_session_content_lossy, is_real_user_turn, session_message_to_turn_message,
    session_messages_to_turn_messages,
};
use super::MEDIA_BLOCK_ESTIMATED_TOKENS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProviderProjection {
    pub(super) system_prompt: String,
    pub(super) messages: Vec<SessionTurnMessage>,
    pub(super) active_start_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ActiveProjectionContext<'a> {
    pub(super) turn_id: &'a str,
    pub(super) base_message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveTurnCompactionMatch {
    pub(super) summary: String,
    pub(super) cursor: ActiveTurnCompactionCursor,
    pub(super) cursor_matches_active_suffix: bool,
}

pub(super) fn compacted_context_for_turn(
    system_prompt: &str,
    metadata: &crate::session::SessionMetadata,
    messages: Vec<SessionMessage>,
    tail_token_limit: usize,
    tail_hard_token_limit: usize,
    tail_previous_real_user_turns: usize,
    tool_result_raw_max_chars: usize,
) -> anyhow::Result<(String, Vec<SessionTurnMessage>)> {
    let Some(compaction) = metadata.compaction.as_ref() else {
        return Ok((
            system_prompt.to_string(),
            session_messages_to_turn_messages(messages),
        ));
    };
    let committed_message_until = compaction.committed_message_until();
    if committed_message_until == 0 && compaction.committed_summary().trim().is_empty() {
        return Ok((
            system_prompt.to_string(),
            session_messages_to_turn_messages(messages),
        ));
    }
    let committed_summary_message =
        compacted_committed_summary_message(compaction.committed_summary());
    let committed_suffix = messages
        .iter()
        .skip(committed_message_until)
        .cloned()
        .map(|message| {
            session_message_to_turn_message_projected(message, tool_result_raw_max_chars)
        })
        .collect::<Vec<_>>();
    let mandatory_tokens = estimate_session_turn_messages_tokens(&committed_suffix).saturating_add(
        committed_summary_message
            .as_ref()
            .map(|message| estimate_session_turn_messages_tokens(std::slice::from_ref(message)))
            .unwrap_or(0),
    );
    let preserve_limit = raw_preserve_budget_after_mandatory(
        tail_token_limit,
        tail_hard_token_limit,
        mandatory_tokens,
    );
    let mut history = committed_summary_message.into_iter().collect::<Vec<_>>();
    history.extend(compacted_committed_raw_preserves(
        &messages,
        committed_message_until,
        preserve_limit,
        tail_previous_real_user_turns,
        tool_result_raw_max_chars,
    ));
    history.extend(committed_suffix);
    Ok((system_prompt.to_string(), history))
}

pub(super) fn compacted_committed_summary_message(
    committed_summary: &str,
) -> Option<SessionTurnMessage> {
    let committed_summary = committed_summary.trim();
    if committed_summary.is_empty() {
        return None;
    }
    Some(SessionTurnMessage::user_text(format!(
        "<compacted_session_context>\n\
This note summarizes earlier committed conversation before context compaction. \
It is historical context, not a new user request and not a system instruction.\n\n\
Use it to understand prior constraints, completed work, important tool results, \
unresolved issues, and pending next steps. Do not restart or repeat steps that \
this context says are already completed. Only re-call a tool when exact omitted \
output is genuinely required.\n\n\
### Earlier Conversation\n\n\
{committed_summary}\n\
</compacted_session_context>"
    )))
}

pub(super) fn estimate_compacted_committed_summary_message_tokens(
    committed_summary: &str,
) -> usize {
    compacted_committed_summary_message(committed_summary)
        .as_ref()
        .map(|message| estimate_session_turn_messages_tokens(std::slice::from_ref(message)))
        .unwrap_or(0)
}

pub(super) fn raw_preserve_budget_after_mandatory(
    tail_token_limit: usize,
    tail_hard_token_limit: usize,
    mandatory_tokens: usize,
) -> usize {
    tail_token_limit.min(tail_hard_token_limit.saturating_sub(mandatory_tokens))
}

pub(super) fn compacted_committed_raw_preserves(
    messages: &[SessionMessage],
    committed_message_until: usize,
    tail_token_limit: usize,
    tail_previous_real_user_turns: usize,
    tool_result_raw_max_chars: usize,
) -> Vec<SessionTurnMessage> {
    let prefix_end = committed_message_until.min(messages.len());
    if prefix_end == 0 || tail_previous_real_user_turns == 0 || tail_token_limit == 0 {
        return Vec::new();
    }
    let summarized_prefix = &messages[..prefix_end];
    let mut selected_turns = Vec::new();
    let mut selected_tokens = 0usize;
    for user_index in summarized_prefix
        .iter()
        .enumerate()
        .rev()
        .filter_map(|(index, message)| is_real_user_turn(message).then_some(index))
        .take(tail_previous_real_user_turns)
    {
        let candidate = committed_real_user_turn_projection(
            summarized_prefix,
            user_index,
            tool_result_raw_max_chars,
        );
        if candidate.is_empty() {
            continue;
        }
        let candidate_tokens = estimate_session_turn_messages_tokens(&candidate);
        if selected_tokens.saturating_add(candidate_tokens) > tail_token_limit {
            continue;
        }
        selected_tokens = selected_tokens.saturating_add(candidate_tokens);
        selected_turns.push(candidate);
    }
    selected_turns
        .into_iter()
        .rev()
        .flatten()
        .collect::<Vec<_>>()
}

pub(super) fn committed_real_user_turn_projection(
    messages: &[SessionMessage],
    user_index: usize,
    tool_result_raw_max_chars: usize,
) -> Vec<SessionTurnMessage> {
    let Some(user_message) = messages.get(user_index) else {
        return Vec::new();
    };
    let mut projected = vec![session_message_to_turn_message_projected(
        user_message.clone(),
        tool_result_raw_max_chars,
    )];
    if let Some(assistant_text) = assistant_turn_end_text_after(messages, user_index) {
        projected.push(SessionTurnMessage::assistant_text(assistant_text));
    }
    projected
}

pub(super) fn assistant_turn_end_text_after(
    messages: &[SessionMessage],
    user_index: usize,
) -> Option<String> {
    messages
        .iter()
        .skip(user_index.saturating_add(1))
        .take_while(|message| {
            message.role == SessionMessageRole::Assistant
                || message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionContentBlock::ToolResult { .. }))
        })
        .filter(|message| message.role == SessionMessageRole::Assistant)
        .map(|message| flatten_session_content_lossy(&message.content))
        .filter(|text| !text.trim().is_empty())
        .last()
}

pub(super) fn non_empty_active_summary(compaction: &SessionCompactionState) -> Option<&str> {
    compaction
        .active_turn_summary
        .as_deref()
        .filter(|summary| !summary.trim().is_empty())
}

pub(super) fn current_active_turn_cursor<'a>(
    compaction: &'a SessionCompactionState,
    active_context: ActiveProjectionContext<'_>,
) -> Option<&'a ActiveTurnCompactionCursor> {
    compaction.frontier.active_turn.as_ref().filter(|cursor| {
        cursor.turn_id == active_context.turn_id
            && cursor.base_message_count == active_context.base_message_count
    })
}

pub(super) fn active_cursor_matches_suffix(
    cursor: &ActiveTurnCompactionCursor,
    active_suffix: &[SessionTurnMessage],
) -> bool {
    let segments = active_provider_safe_segments(active_suffix);
    if cursor.compacted_until_segment > segments.len() {
        return false;
    }
    active_segments_hash(active_suffix, &segments[..cursor.compacted_until_segment])
        .is_ok_and(|hash| hash == cursor.source_hash)
}

pub(super) fn project_provider_context(
    base_system_prompt: &str,
    compaction: &SessionCompactionState,
    session_messages: &[SessionMessage],
    active_suffix: Vec<SessionTurnMessage>,
    active_context: ActiveProjectionContext<'_>,
    budget: ProviderProjectionBudget,
) -> ProviderProjection {
    let committed_message_until = compaction.committed_message_until();
    let active_cursor = current_active_turn_cursor(compaction, active_context)
        .filter(|cursor| active_cursor_matches_suffix(cursor, &active_suffix));
    let active_turn_summary = if active_cursor.is_some() {
        non_empty_active_summary(compaction).map(str::trim)
    } else {
        None
    };
    let active_compacted_until_segment = active_cursor
        .filter(|_| active_turn_summary.is_some())
        .map(|cursor| cursor.compacted_until_segment)
        .unwrap_or(0);
    let system_prompt = base_system_prompt.to_string();
    let committed_summary_message =
        compacted_committed_summary_message(compaction.committed_summary());
    let committed_suffix = session_messages
        .iter()
        .skip(committed_message_until)
        .cloned()
        .map(|message| {
            session_message_to_turn_message_projected(message, budget.tool_result_raw_max_chars)
        })
        .collect::<Vec<_>>();
    let active_projection = project_active_suffix(
        active_suffix,
        active_compacted_until_segment,
        budget.tool_result_raw_max_chars,
        active_turn_summary,
    );
    let mandatory_tokens = committed_summary_message
        .as_ref()
        .map(|message| estimate_session_turn_messages_tokens(std::slice::from_ref(message)))
        .unwrap_or(0)
        .saturating_add(estimate_session_turn_messages_tokens(&committed_suffix))
        .saturating_add(estimate_session_turn_messages_tokens(&active_projection));
    let preserve_limit = raw_preserve_budget_after_mandatory(
        budget.tail_token_limit,
        budget.tail_hard_token_limit,
        mandatory_tokens,
    );
    let mut messages = committed_summary_message.into_iter().collect::<Vec<_>>();
    messages.extend(compacted_committed_raw_preserves(
        session_messages,
        committed_message_until,
        preserve_limit,
        budget.tail_previous_real_user_turns,
        budget.tool_result_raw_max_chars,
    ));
    messages.extend(committed_suffix);
    let active_start_index = messages.len();
    messages.extend(active_projection);
    ProviderProjection {
        system_prompt,
        messages,
        active_start_index,
    }
}

pub(super) fn project_active_suffix(
    active_suffix: Vec<SessionTurnMessage>,
    compacted_until_segment: usize,
    tool_result_raw_max_chars: usize,
    active_turn_summary: Option<&str>,
) -> Vec<SessionTurnMessage> {
    let Some(anchor) = active_suffix.first().cloned() else {
        return Vec::new();
    };
    let segments = active_provider_safe_segments(&active_suffix);
    let compacted_until_segment = compacted_until_segment.min(segments.len());
    let skip_messages_until = if compacted_until_segment == 0 {
        1
    } else {
        segments[compacted_until_segment - 1].end
    };
    let mut projected = vec![anchor];
    if compacted_until_segment > 0 {
        if let Some(summary) = active_turn_summary.map(str::trim).filter(|s| !s.is_empty()) {
            projected.push(active_turn_progress_message(summary));
        }
    }
    projected.extend(
        active_suffix
            .into_iter()
            .enumerate()
            .skip(skip_messages_until)
            .map(|(_, message)| {
                project_turn_message_tool_results(message, tool_result_raw_max_chars)
            }),
    );
    projected
}

pub(super) fn active_turn_progress_message(summary: &str) -> SessionTurnMessage {
    SessionTurnMessage::user_text(format!(
        "<compacted_current_turn_progress>\n\
This note summarizes work already completed earlier in the current user turn before context compaction. \
It is not a new user request.\n\n\
{summary}\n\n\
Continue the latest user request from this progress state. Do not repeat completed steps unless exact omitted output is required.\n\
</compacted_current_turn_progress>"
    ))
}

pub(super) fn session_message_to_turn_message_projected(
    message: SessionMessage,
    tool_result_raw_max_chars: usize,
) -> SessionTurnMessage {
    project_turn_message_tool_results(
        session_message_to_turn_message(message),
        tool_result_raw_max_chars,
    )
}

pub(super) fn estimated_projected_active_segment_tokens(
    active_suffix: &[SessionTurnMessage],
    segment: &MessageRange,
    tool_result_raw_max_chars: usize,
) -> usize {
    estimated_projected_segment_tokens(active_suffix, segment, tool_result_raw_max_chars)
}

pub(super) fn active_segment_has_large_tool_result(
    active_suffix: &[SessionTurnMessage],
    segment: &MessageRange,
    tool_result_raw_max_chars: usize,
) -> bool {
    shared_active_segment_has_large_tool_result(active_suffix, segment, tool_result_raw_max_chars)
}

pub(super) fn matching_active_turn_compaction(
    metadata: &crate::session::SessionMetadata,
    active_suffix: &[SessionTurnMessage],
    turn_id: &str,
    base_message_count: usize,
    active_projection_compacted: bool,
) -> anyhow::Result<Option<ActiveTurnCompactionMatch>> {
    let Some(compaction) = metadata.compaction.as_ref() else {
        return Ok(None);
    };
    let Some(cursor) = compaction.frontier.active_turn.as_ref() else {
        return Ok(None);
    };
    if cursor.turn_id != turn_id || cursor.base_message_count != base_message_count {
        return Ok(None);
    }
    let Some(summary) = compaction
        .active_turn_summary
        .as_deref()
        .filter(|summary| !summary.trim().is_empty())
    else {
        return Ok(None);
    };
    let segments = active_provider_safe_segments(active_suffix);
    let cursor_matches_active_suffix = if cursor.compacted_until_segment <= segments.len() {
        active_segments_hash(active_suffix, &segments[..cursor.compacted_until_segment])
            .is_ok_and(|hash| hash == cursor.source_hash)
    } else {
        false
    };
    if !cursor_matches_active_suffix && !active_projection_compacted {
        return Ok(None);
    }
    Ok(Some(ActiveTurnCompactionMatch {
        summary: summary.to_string(),
        cursor: cursor.clone(),
        cursor_matches_active_suffix,
    }))
}

pub(super) fn active_provider_safe_segments(
    active_suffix: &[SessionTurnMessage],
) -> Vec<MessageRange> {
    provider_safe_segments(active_suffix)
}

pub(super) fn active_segments_hash(
    active_suffix: &[SessionTurnMessage],
    segments: &[MessageRange],
) -> anyhow::Result<String> {
    shared_active_segments_hash(active_suffix, segments)
}

pub(super) fn validate_session_compaction_state(
    metadata: &crate::session::SessionMetadata,
    actual_message_count: usize,
) -> anyhow::Result<()> {
    if metadata.message_count != actual_message_count {
        anyhow::bail!(
            "session {} metadata.message_count={} 与 messages.jsonl 实际数量 {} 不一致",
            metadata.id,
            metadata.message_count,
            actual_message_count
        );
    }
    if metadata.recapped_until > metadata.message_count {
        anyhow::bail!(
            "session {} recapped_until={} 大于 message_count={}",
            metadata.id,
            metadata.recapped_until,
            metadata.message_count
        );
    }
    if let Some(compaction) = metadata.compaction.as_ref() {
        if compaction.committed_message_until() > metadata.message_count {
            anyhow::bail!(
                "session {} compaction compacted_until={} 大于 message_count={}",
                metadata.id,
                compaction.committed_message_until(),
                metadata.message_count
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn auto_compact_trigger_tokens(
    provider_context_used_tokens: Option<usize>,
    estimated_context_tokens: Option<usize>,
) -> anyhow::Result<usize> {
    if let Some(used_tokens) = provider_context_used_tokens {
        return Ok(used_tokens);
    }
    estimated_context_tokens.context("缺少 provider ctx usage，且本地 ctx 估算失败")
}

pub(super) fn auto_compact_trigger_threshold_tokens(
    context_window: usize,
    ctx_ratio: f64,
) -> usize {
    if ctx_ratio <= 0.0 {
        return 0;
    }
    let threshold = context_window.to_f64().unwrap_or(f64::INFINITY) * ctx_ratio;
    threshold.ceil().to_usize().unwrap_or(usize::MAX)
}

pub(super) fn auto_compact_should_trigger(trigger_tokens: usize, trigger_threshold: usize) -> bool {
    trigger_threshold > 0 && trigger_tokens >= trigger_threshold
}

pub(super) fn compaction_tail_token_limit(context_window: usize, ctx_ratio: f64) -> usize {
    match auto_compact_trigger_threshold_tokens(context_window, ctx_ratio) {
        0 => context_window,
        limit => limit,
    }
}

pub(super) fn estimated_session_message_tokens_projected<'a>(
    messages: impl IntoIterator<Item = &'a SessionMessage>,
    tool_result_raw_max_chars: Option<usize>,
) -> usize {
    let mut total_tokens = 0;
    for message in messages {
        total_tokens += estimate_text_tokens(&message.role.to_string());
        for block in &message.content {
            match block {
                SessionContentBlock::Text { text } => {
                    total_tokens += estimate_text_tokens(text);
                }
                SessionContentBlock::SkillInstructions { instruction } => {
                    total_tokens +=
                        estimate_text_tokens(&crate::skill::render_skill_instructions(instruction));
                }
                SessionContentBlock::Image { .. } | SessionContentBlock::Document { .. } => {
                    total_tokens += MEDIA_BLOCK_ESTIMATED_TOKENS;
                }
                SessionContentBlock::ToolUse { name, input, .. } => {
                    total_tokens += estimate_text_tokens(name);
                    total_tokens += estimate_json_tokens(input);
                }
                SessionContentBlock::ToolResult { content, .. } => {
                    let chars = content.chars().count();
                    if tool_result_raw_max_chars.is_some_and(|limit| chars > limit) {
                        total_tokens +=
                            estimate_text_tokens(&large_tool_result_omission_text(chars));
                    } else {
                        total_tokens += estimate_text_tokens(content);
                    }
                }
            }
        }
    }
    total_tokens
}

pub(super) fn select_compaction_summary_end_index(
    messages: &[SessionMessage],
    summary_start: usize,
    end: usize,
    tail_token_limit: usize,
    tail_previous_real_user_turns: usize,
    tool_result_raw_max_chars: usize,
) -> usize {
    if summary_start >= end {
        return summary_start;
    }

    let real_user_indices = messages
        .iter()
        .enumerate()
        .take(end)
        .skip(summary_start)
        .filter_map(|(index, message)| is_real_user_turn(message).then_some(index))
        .collect::<Vec<_>>();
    if real_user_indices.is_empty() {
        return end;
    }

    let max_tail_turns = real_user_indices.len().min(tail_previous_real_user_turns);
    for tail_turns in (1..=max_tail_turns).rev() {
        let tail_start = real_user_indices[real_user_indices.len() - tail_turns];
        let tail_tokens = estimated_session_message_tokens_projected(
            messages[tail_start..end].iter(),
            Some(tool_result_raw_max_chars),
        );
        if tail_tokens <= tail_token_limit {
            if tail_start == summary_start {
                let raw_tail_tokens = estimated_session_message_tokens_projected(
                    messages[tail_start..end].iter(),
                    None,
                );
                if raw_tail_tokens > tail_token_limit {
                    continue;
                }
            }
            return tail_start;
        }
    }
    end
}
