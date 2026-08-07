//! delegation 内部上下文压缩。
//!
//! 本模块只维护子代理 provider history 的压缩投影与落盘状态。它复用
//! provider-neutral JSON 调用与 token 估算，不把 delegation 升级成完整 session。

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::{
    context_recovery_protected_tail_from_marker, ensure_compaction_request_within_context_window,
    estimate_provider_request_context_tokens, estimate_session_turn_messages_tokens,
    estimated_projected_segment_tokens, omit_turn_messages_tool_results,
    project_compaction_input_media, project_compaction_input_tool_results,
    project_turn_message_for_safe_transcript, project_turn_message_tool_results,
    project_turn_messages_tool_results, provider_safe_segments, ContextUsageSnapshot,
    ContextUsageSource, ProviderProjectionBudget, SessionTurnContentBlock, SessionTurnEvent,
    SessionTurnMessage, SessionTurnPreflight, StructuredJsonAttemptRequest, StructuredJsonCaller,
    ToolSpec, FILE_EDIT_AUTHORITY_COMPACTION_NOTICE,
};
use crate::config::SessionCompactionConfig;
use crate::prompt::PromptRegistry;

use super::runner::DelegationProgressSink;
use super::types::{
    DelegationCompactionEventKind, DelegationCompactionState, DelegationEventKind,
    DelegationMetadata, DelegationTranscriptEntry, DelegationTranscriptKind,
    DelegationTranscriptMessageSource,
};

const PROMPT_SUBAGENTS_COMPACTION: &str = "subagents_compaction";
const DELEGATION_COMPACTION_SCHEMA_VERSION: u8 = 1;
const DEFAULT_COMPACTION_REASON: &str = "provider context reached subagent auto compact threshold";
const CONTEXT_WINDOW_RECOVERY_REASON: &str = "provider reported model context window exceeded";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderContextUsageAnchor {
    provider_message_count: usize,
    used_tokens: usize,
}

pub struct DelegationPreflightCompactor {
    metadata: DelegationMetadata,
    progress: DelegationProgressSink,
    prompt_registry: Arc<PromptRegistry>,
    json_caller: Arc<StructuredJsonCaller>,
    tool_specs: Vec<ToolSpec>,
    compaction: SessionCompactionConfig,
    context_window: usize,
    provider_context_anchor: Option<ProviderContextUsageAnchor>,
    compacted_since_last_check: bool,
    context_window_recovery_requested: bool,
    context_window_recovery_tail_marker: Option<SessionTurnMessage>,
}

impl DelegationPreflightCompactor {
    pub fn new(
        metadata: DelegationMetadata,
        progress: DelegationProgressSink,
        prompt_registry: Arc<PromptRegistry>,
        json_caller: Arc<StructuredJsonCaller>,
        tool_specs: Vec<ToolSpec>,
        compaction: SessionCompactionConfig,
        context_window: usize,
    ) -> Self {
        Self {
            metadata,
            progress,
            prompt_registry,
            json_caller,
            tool_specs,
            compaction,
            context_window,
            provider_context_anchor: None,
            compacted_since_last_check: false,
            context_window_recovery_requested: false,
            context_window_recovery_tail_marker: None,
        }
    }

    fn trigger_context_tokens(
        &self,
        system_prompt: &str,
        provider_messages: &[SessionTurnMessage],
    ) -> usize {
        let full_estimate = estimate_provider_request_context_tokens(
            system_prompt,
            provider_messages,
            &self.tool_specs,
        )
        .used_tokens;
        self.provider_context_anchor
            .filter(|anchor| anchor.provider_message_count <= provider_messages.len())
            .map(|anchor| {
                anchor
                    .used_tokens
                    .saturating_add(estimate_session_turn_messages_tokens(
                        &provider_messages[anchor.provider_message_count..],
                    ))
                    .max(full_estimate)
            })
            .unwrap_or(full_estimate)
    }

    async fn maybe_compact(
        &mut self,
        system_prompt: &str,
        provider_messages: &mut Vec<SessionTurnMessage>,
        runtime_projection_tokens: usize,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        let forced_context_recovery = std::mem::take(&mut self.context_window_recovery_requested);
        let Some(trigger_threshold) =
            auto_compact_threshold(self.context_window, self.compaction.auto_compact_ctx_ratio)
        else {
            if forced_context_recovery {
                anyhow::bail!("子任务上下文已满，但自动压缩已关闭。请拆分任务后重试。");
            }
            return Ok(());
        };
        let trigger_tokens = self
            .trigger_context_tokens(system_prompt, provider_messages)
            .saturating_add(runtime_projection_tokens);
        if !forced_context_recovery && trigger_tokens < trigger_threshold {
            return Ok(());
        }
        let hard_threshold = hard_threshold(self.context_window);
        if provider_messages.len() < 4 {
            if forced_context_recovery || trigger_tokens >= hard_threshold {
                let error = "子任务上下文已满，但没有可安全压缩的历史。请拆分任务后重试。";
                self.record_hard_failure(error).await?;
                anyhow::bail!(error);
            }
            return Ok(());
        }

        let segments = provider_safe_segments(provider_messages);
        let protected_tail_segments = match self.context_window_recovery_tail_marker.as_ref() {
            Some(marker) => {
                context_recovery_protected_tail_from_marker(provider_messages, &segments, marker)
                    .context("子任务续写状态异常，无法自动恢复")?
            }
            None => 0,
        };
        if forced_context_recovery && protected_tail_segments == 0 {
            anyhow::bail!("子任务续写状态异常，无法自动恢复");
        }
        let Some(plan) = self.build_plan(
            provider_messages,
            runtime_projection_tokens,
            protected_tail_segments,
        ) else {
            if forced_context_recovery || trigger_tokens >= hard_threshold {
                let error = "子任务上下文已满，但没有可安全压缩的历史。请拆分任务后重试。";
                self.record_hard_failure(error).await?;
                anyhow::bail!(error);
            }
            return Ok(());
        };

        emit(SessionTurnEvent::CompactionStarted {
            compact_start_index: plan.compact_start_index,
            compact_end_index: plan.compact_end_index,
            recap_start_index: plan.compact_start_index,
            recap_end_index: plan.compact_end_index,
        });
        self.progress
            .append_compaction_event(DelegationCompactionEventKind::Started {
                compact_start_index: plan.compact_start_index,
                compact_end_index: plan.compact_end_index,
                reason: if forced_context_recovery {
                    CONTEXT_WINDOW_RECOVERY_REASON.to_string()
                } else {
                    DEFAULT_COMPACTION_REASON.to_string()
                },
            })
            .await?;
        let checkpoint = json!({
            "schema_version": DELEGATION_COMPACTION_SCHEMA_VERSION,
            "compact_start_index": plan.compact_start_index,
            "compact_end_index": plan.compact_end_index,
            "reason": if forced_context_recovery {
                CONTEXT_WINDOW_RECOVERY_REASON
            } else {
                DEFAULT_COMPACTION_REASON
            },
            "created_at": Utc::now(),
        });
        self.progress
            .write_compaction_checkpoint(&checkpoint)
            .await?;

        let result = self.generate_summary(&plan).await;
        let summary = match result {
            Ok(summary) => summary,
            Err(error) => {
                let error_text = error.to_string();
                self.progress
                    .append_compaction_event(DelegationCompactionEventKind::Failed {
                        error: error_text.clone(),
                    })
                    .await?;
                self.progress
                    .append_transcript_entry(DelegationTranscriptEntry {
                        at: Utc::now(),
                        kind: DelegationTranscriptKind::CompactionFailed {
                            error: error_text.clone(),
                        },
                    })
                    .await?;
                self.progress
                    .clear_compaction_checkpoint()
                    .await
                    .context("清理失败的 subagent compaction checkpoint 失败")?;
                emit(SessionTurnEvent::CompactionFailed {
                    error: error_text.clone(),
                });
                if forced_context_recovery || trigger_tokens >= hard_threshold {
                    self.record_hard_failure(&error_text).await?;
                    return Err(error);
                }
                return Ok(());
            }
        };
        let projected_messages = plan.projected_messages(&summary);
        if let Err(error) = self.ensure_compacted_projection_within_hard_budget(
            &projected_messages,
            runtime_projection_tokens,
        ) {
            let error_text = error.to_string();
            self.progress
                .append_transcript_entry(DelegationTranscriptEntry {
                    at: Utc::now(),
                    kind: DelegationTranscriptKind::CompactionFailed {
                        error: error_text.clone(),
                    },
                })
                .await?;
            self.progress
                .clear_compaction_checkpoint()
                .await
                .context("清理超预算的 subagent compaction checkpoint 失败")?;
            emit(SessionTurnEvent::CompactionFailed {
                error: error_text.clone(),
            });
            self.record_hard_failure(&error_text).await?;
            return Err(error);
        }

        let state = DelegationCompactionState {
            schema_version: DELEGATION_COMPACTION_SCHEMA_VERSION,
            compacted_until: plan.compact_end_index,
            summary: summary.clone(),
            summary_updated_at: Utc::now(),
        };
        self.progress.write_compaction_state(&state).await?;
        self.progress
            .append_compaction_event(DelegationCompactionEventKind::Completed {
                compacted_until: state.compacted_until,
                summary_chars: summary.chars().count(),
            })
            .await?;
        self.progress
            .append_transcript_entry(DelegationTranscriptEntry {
                at: Utc::now(),
                kind: DelegationTranscriptKind::CompactionBoundary {
                    compacted_until: state.compacted_until,
                    summary: summary.clone(),
                },
            })
            .await?;
        self.progress.clear_compaction_checkpoint().await?;
        *provider_messages = projected_messages;
        let projected_tokens = self
            .trigger_context_tokens(system_prompt, provider_messages)
            .saturating_add(runtime_projection_tokens);
        if forced_context_recovery && projected_tokens >= trigger_tokens {
            log::warn!(
                target: "delegation",
                "subagent context recovery compaction 未缩小请求：压缩前估算 {trigger_tokens} tokens，压缩后估算 {projected_tokens} tokens"
            );
            let error = "子任务压缩后仍超过上下文限制。请拆分任务后重试。";
            self.record_hard_failure(error).await?;
            anyhow::bail!(error);
        }
        self.compacted_since_last_check = true;
        emit(SessionTurnEvent::CompactionCompleted {
            compacted_until: state.compacted_until,
            recapped_until: 0,
            new_claim_ids: Vec::new(),
            updated_claim_ids: Vec::new(),
            used_claim_ids: Vec::new(),
            new_dispute_ids: Vec::new(),
        });
        self.provider_context_anchor = None;
        Ok(())
    }

    /// 动态 runtime projection 不进入 delegation transcript，因此不能直接交给 compactor；
    /// 但它必须预留在这一次请求的 token 预算中。
    pub async fn before_provider_request_with_runtime_reserve(
        &mut self,
        system_prompt: &str,
        provider_messages: &mut Vec<SessionTurnMessage>,
        runtime_projection_tokens: usize,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        self.maybe_compact(
            system_prompt,
            provider_messages,
            runtime_projection_tokens,
            emit,
        )
        .await?;
        let projected_tokens = estimate_provider_request_context_tokens(
            system_prompt,
            provider_messages,
            &self.tool_specs,
        )
        .used_tokens
        .saturating_add(runtime_projection_tokens);
        let hard_limit = hard_threshold(self.context_window);
        if projected_tokens >= hard_limit {
            self.record_hard_failure(
                "subagent context plus runtime projection is too large to send safely",
            )
            .await?;
            anyhow::bail!(
                "subagent context plus runtime projection is too large to send safely: estimated tokens={projected_tokens}, hard limit={hard_limit}"
            );
        }
        Ok(())
    }

    pub(crate) fn take_compacted_since_last_check(&mut self) -> bool {
        std::mem::take(&mut self.compacted_since_last_check)
    }

    fn build_plan(
        &self,
        provider_messages: &[SessionTurnMessage],
        runtime_projection_tokens: usize,
        protected_tail_segments: usize,
    ) -> Option<CompactionPlan> {
        let ranges = self.select_compaction_ranges(
            provider_messages,
            runtime_projection_tokens,
            protected_tail_segments,
        )?;
        let compact_start_index = 1;
        let compact_end_index = ranges.compact_end_index;
        if compact_end_index <= compact_start_index {
            return None;
        }
        let anchor = provider_messages.first()?.clone();
        let compact_source = provider_messages
            .get(compact_start_index..compact_end_index)?
            .iter()
            .cloned()
            .map(project_compaction_input_media)
            .collect::<Vec<_>>();
        let compact_messages_with_large_tool_results_omitted =
            project_compaction_input_tool_results(
                compact_source.clone(),
                self.compaction.tool_result_raw_max_chars,
            );
        let compact_messages_with_tool_results_omitted =
            omit_turn_messages_tool_results(compact_source.clone());
        let tail = provider_messages
            .get(ranges.tail_start_index..)?
            .iter()
            .cloned()
            .enumerate()
            .map(|(offset, message)| {
                let original_index = ranges.tail_start_index.saturating_add(offset);
                if ranges
                    .protected_tail_start_index
                    .is_some_and(|start| original_index >= start)
                {
                    message
                } else {
                    project_turn_message_tool_results(
                        message,
                        self.compaction.tool_result_raw_max_chars,
                    )
                }
            })
            .collect();
        Some(CompactionPlan {
            compact_start_index,
            compact_end_index,
            anchor,
            compact_messages: compact_source,
            compact_messages_with_large_tool_results_omitted,
            compact_messages_with_tool_results_omitted,
            tail,
        })
    }

    fn select_compaction_ranges(
        &self,
        provider_messages: &[SessionTurnMessage],
        runtime_projection_tokens: usize,
        protected_tail_segments: usize,
    ) -> Option<CompactionRanges> {
        if provider_messages.len() < 2 {
            return None;
        }
        let segments = provider_safe_segments(provider_messages);
        if segments.is_empty() {
            return None;
        }
        let compactable_segments = segments.len().saturating_sub(protected_tail_segments);
        if compactable_segments == 0 {
            return None;
        }
        let covered_end = segments.last().map(|segment| segment.end).unwrap_or(1);
        let suffix_start = if covered_end < provider_messages.len() {
            covered_end
        } else {
            provider_messages.len()
        };
        let budget = self.provider_projection_budget(runtime_projection_tokens);
        let anchor_tokens =
            estimate_session_turn_messages_tokens(std::slice::from_ref(&provider_messages[0]));
        let fixed_tail_start = if protected_tail_segments > 0 {
            segments[compactable_segments].start
        } else {
            suffix_start
        };
        let suffix_tokens = if fixed_tail_start < provider_messages.len() {
            if protected_tail_segments > 0 {
                estimate_session_turn_messages_tokens(&provider_messages[fixed_tail_start..])
            } else {
                estimate_session_turn_messages_tokens(&project_turn_messages_tool_results(
                    provider_messages[fixed_tail_start..].to_vec(),
                    budget.tool_result_raw_max_chars,
                ))
            }
        } else {
            0
        };
        let mandatory_tokens = anchor_tokens.saturating_add(suffix_tokens);
        let mut remaining = budget.tail_token_limit.min(
            budget
                .tail_hard_token_limit
                .saturating_sub(mandatory_tokens),
        );
        let max_tail_segments = budget.tail_previous_real_user_turns.max(1);
        let mut selected_segments = 0usize;
        let mut tail_start_index = fixed_tail_start;
        for segment in segments[..compactable_segments].iter().rev() {
            if segment.end > fixed_tail_start {
                continue;
            }
            if selected_segments >= max_tail_segments {
                break;
            }
            let segment_tokens = estimated_projected_segment_tokens(
                provider_messages,
                segment,
                budget.tool_result_raw_max_chars,
            );
            if segment_tokens > remaining {
                break;
            }
            remaining = remaining.saturating_sub(segment_tokens);
            selected_segments = selected_segments.saturating_add(1);
            tail_start_index = segment.start;
        }
        Some(CompactionRanges {
            compact_end_index: tail_start_index,
            tail_start_index,
            protected_tail_start_index: (protected_tail_segments > 0)
                .then(|| segments[compactable_segments].start),
        })
    }

    fn provider_projection_budget(
        &self,
        runtime_projection_tokens: usize,
    ) -> ProviderProjectionBudget {
        ProviderProjectionBudget {
            tail_token_limit: compaction_tail_token_limit(
                self.context_window,
                self.compaction.tail_target_ctx_ratio,
            )
            .saturating_sub(runtime_projection_tokens),
            tail_hard_token_limit: compaction_tail_token_limit(
                self.context_window,
                self.compaction.tail_hard_ctx_ratio,
            )
            .saturating_sub(runtime_projection_tokens),
            tail_previous_real_user_turns: self.compaction.tail_previous_real_user_turns,
            tool_result_raw_max_chars: self.compaction.tool_result_raw_max_chars,
        }
    }

    fn ensure_compacted_projection_within_hard_budget(
        &self,
        projected_messages: &[SessionTurnMessage],
        runtime_projection_tokens: usize,
    ) -> anyhow::Result<()> {
        let hard_limit =
            compaction_tail_token_limit(self.context_window, self.compaction.tail_hard_ctx_ratio);
        let raw_tail_tokens = estimate_session_turn_messages_tokens(projected_messages);
        let combined_tail_tokens = raw_tail_tokens.saturating_add(runtime_projection_tokens);
        if hard_limit == 0 || combined_tail_tokens > hard_limit {
            anyhow::bail!(
                "Compacted subagent provider projection still exceeds hard tail budget: estimated raw tail tokens={raw_tail_tokens}, runtime projection tokens={runtime_projection_tokens}, combined tail tokens={combined_tail_tokens}, hard tail budget={hard_limit}."
            );
        }
        Ok(())
    }

    async fn generate_summary(&self, plan: &CompactionPlan) -> anyhow::Result<String> {
        let prior_summary = self
            .progress
            .read_compaction_state()
            .await?
            .map(|state| state.summary)
            .filter(|summary| !summary.trim().is_empty());
        let system_prompt = self
            .prompt_registry
            .render(
                PROMPT_SUBAGENTS_COMPACTION,
                minijinja::context! {
                    summary_max_chars => self.compaction.summary_max_chars,
                },
            )
            .context("渲染 subagents_compaction prompt 失败")?;
        let objective_anchor = project_compaction_input_media(plan.anchor.clone());
        let mut payload = DelegationCompactionPayload {
            subagent_id: self.metadata.id.to_string(),
            parent_session_id: self.metadata.parent_session_id.to_string(),
            title: self.metadata.title.clone(),
            role: self.metadata.role.clone(),
            objective: self.metadata.objective.clone(),
            constraints: self.metadata.constraints.clone(),
            objective_anchor,
            prior_summary,
            compact_start_index: plan.compact_start_index,
            compact_end_index: plan.compact_end_index,
            transcript: plan.compact_messages.clone(),
            summary_max_chars: self.compaction.summary_max_chars,
        };
        let mut user_text = serde_json::to_string_pretty(&payload)?;
        let mut provider_messages = vec![SessionTurnMessage::user_text(user_text.clone())];
        if let Err(full_error) = ensure_compaction_request_within_context_window(
            &system_prompt,
            &provider_messages,
            self.context_window,
            self.json_caller.max_tokens(),
        ) {
            payload.transcript = plan
                .compact_messages_with_large_tool_results_omitted
                .clone();
            user_text = serde_json::to_string_pretty(&payload)?;
            provider_messages = vec![SessionTurnMessage::user_text(user_text.clone())];
            if let Err(large_omission_error) = ensure_compaction_request_within_context_window(
                &system_prompt,
                &provider_messages,
                self.context_window,
                self.json_caller.max_tokens(),
            ) {
                payload.transcript = plan.compact_messages_with_tool_results_omitted.clone();
                user_text = serde_json::to_string_pretty(&payload)?;
                provider_messages = vec![SessionTurnMessage::user_text(user_text)];
                ensure_compaction_request_within_context_window(
                    &system_prompt,
                    &provider_messages,
                    self.context_window,
                    self.json_caller.max_tokens(),
                )
                .with_context(|| {
                    format!(
                        "subagent compaction summary request remains over budget after omitting all tool results; full input error: {full_error:#}; large-tool-result omission error: {large_omission_error:#}"
                    )
                })?;
            }
        }
        let value = self
            .json_caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::compaction(system_prompt, provider_messages),
                |value| parse_summary(value, self.compaction.summary_max_chars),
                |_, _, _| {},
                |_| std::future::ready(()),
                |system_prompt, attempt_messages| {
                    ensure_compaction_request_within_context_window(
                        system_prompt,
                        attempt_messages,
                        self.context_window,
                        self.json_caller.max_tokens(),
                    )
                    .context("subagent compaction summary provider attempt exceeds context window")
                },
            )
            .await?;
        Ok(value)
    }

    async fn record_hard_failure(&self, error: &str) -> anyhow::Result<()> {
        self.progress
            .append_compaction_event(DelegationCompactionEventKind::Failed {
                error: error.to_string(),
            })
            .await?;
        self.progress
            .record_event(DelegationEventKind::CompactionFailed {
                error: error.to_string(),
            })
            .await?;
        self.progress
            .update(
                Some("compaction_failed".to_string()),
                format!("subagent compaction failed: {error}"),
                Vec::new(),
            )
            .await?;
        Ok(())
    }

    /// runtime-only projection 已从 provider messages 移除后，旧 provider usage 不能继续作为
    /// 本轮 compaction 的 anchor，否则会把已删除 projection 与新的 reserve 重复计数。
    pub(crate) fn clear_runtime_projection_context_anchor(&mut self) {
        self.provider_context_anchor = None;
    }
}

#[async_trait]
impl SessionTurnPreflight for DelegationPreflightCompactor {
    async fn before_provider_request(
        &mut self,
        system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        self.maybe_compact(system_prompt, provider_messages, 0, emit)
            .await
    }

    fn observe_provider_context_usage(
        &mut self,
        provider_message_count: usize,
        usage: ContextUsageSnapshot,
    ) {
        if usage.source == ContextUsageSource::Provider {
            self.provider_context_anchor = Some(ProviderContextUsageAnchor {
                provider_message_count,
                used_tokens: usage.used_tokens,
            });
        }
    }

    fn clear_provider_context_usage(&mut self) {
        self.provider_context_anchor = None;
    }

    fn request_context_window_recovery(
        &mut self,
        assistant_marker: &SessionTurnMessage,
    ) -> anyhow::Result<()> {
        if self.compaction.auto_compact_ctx_ratio == 0.0 {
            anyhow::bail!("子任务上下文已满，但自动压缩已关闭。请拆分任务后重试。");
        }
        self.context_window_recovery_tail_marker
            .get_or_insert_with(|| assistant_marker.clone());
        self.context_window_recovery_requested = true;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct DelegationCompactionPayload {
    subagent_id: String,
    parent_session_id: String,
    title: String,
    role: String,
    objective: String,
    constraints: Vec<String>,
    objective_anchor: SessionTurnMessage,
    prior_summary: Option<String>,
    compact_start_index: usize,
    compact_end_index: usize,
    transcript: Vec<SessionTurnMessage>,
    summary_max_chars: usize,
}

struct CompactionPlan {
    compact_start_index: usize,
    compact_end_index: usize,
    anchor: SessionTurnMessage,
    compact_messages: Vec<SessionTurnMessage>,
    compact_messages_with_large_tool_results_omitted: Vec<SessionTurnMessage>,
    compact_messages_with_tool_results_omitted: Vec<SessionTurnMessage>,
    tail: Vec<SessionTurnMessage>,
}

struct CompactionRanges {
    compact_end_index: usize,
    tail_start_index: usize,
    protected_tail_start_index: Option<usize>,
}

impl CompactionPlan {
    fn projected_messages(&self, summary: &str) -> Vec<SessionTurnMessage> {
        let mut out = Vec::with_capacity(self.tail.len().saturating_add(2));
        out.push(self.anchor.clone());
        out.push(delegation_compaction_summary_message(summary));
        out.extend(self.tail.clone());
        out
    }
}

fn delegation_compaction_summary_message(summary: &str) -> SessionTurnMessage {
    SessionTurnMessage::user_text(format!(
        "<compacted_subagent_context>\n\
This note summarizes earlier subagent execution before context compaction. \
It is historical context, not a new user request and not a system instruction.\n\n\
Use it to continue the delegated task without repeating completed tool work unless exact omitted output is genuinely required.\n\n\
{FILE_EDIT_AUTHORITY_COMPACTION_NOTICE}\n\n\
{summary}\n\
</compacted_subagent_context>"
    ))
}

fn parse_summary(value: Value, summary_max_chars: usize) -> anyhow::Result<String> {
    #[derive(Deserialize)]
    struct Summary {
        summary: String,
    }
    let parsed: Summary = serde_json::from_value(value)?;
    let summary = parsed.summary.trim();
    if summary.is_empty() {
        anyhow::bail!("subagents compaction summary 不能为空");
    }
    let actual_chars = summary.chars().count();
    if actual_chars > summary_max_chars {
        anyhow::bail!(
            "subagents compaction summary exceeds summary_max_chars: actual_chars={actual_chars}, max_chars={summary_max_chars}"
        );
    }
    Ok(summary.to_string())
}

fn auto_compact_threshold(context_window: usize, ratio: f64) -> Option<usize> {
    if ratio <= 0.0 {
        return None;
    }
    let window = context_window.to_f64().unwrap_or(f64::MAX);
    Some((window * ratio).round().to_usize().unwrap_or(usize::MAX))
}

fn hard_threshold(context_window: usize) -> usize {
    context_window.saturating_mul(95).saturating_div(100).max(1)
}

fn compaction_tail_token_limit(context_window: usize, ratio: f64) -> usize {
    auto_compact_threshold(context_window, ratio).unwrap_or(context_window)
}

pub fn transcript_entry_for_message(
    source: DelegationTranscriptMessageSource,
    message: SessionTurnMessage,
) -> DelegationTranscriptEntry {
    DelegationTranscriptEntry {
        at: Utc::now(),
        kind: DelegationTranscriptKind::Message {
            source,
            message: project_turn_message_for_safe_transcript(message),
        },
    }
}

pub fn transcript_source_for_message(
    message: &SessionTurnMessage,
) -> DelegationTranscriptMessageSource {
    if message.role == "assistant" {
        return DelegationTranscriptMessageSource::Assistant;
    }
    if message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::ToolResult { .. }))
    {
        return DelegationTranscriptMessageSource::ToolResult;
    }
    DelegationTranscriptMessageSource::Objective
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::api::{
        ProviderAdapter, ProviderEvent, ProviderReplayState, ProviderRequest, ProviderResponse,
        ProviderStop,
    };
    use crate::claim::{AgentId, SessionId};
    use crate::delegation::{
        DelegationCreateRequest, DelegationId, DelegationProgressSink, DelegationRead,
        DelegationReadMode, DelegationStatus, DelegationStore,
    };

    use super::*;

    struct JsonProvider {
        responses: Mutex<VecDeque<anyhow::Result<ProviderResponse>>>,
        requests: Mutex<Vec<ProviderRequest>>,
    }

    impl JsonProvider {
        fn new(responses: Vec<anyhow::Result<ProviderResponse>>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                requests: Mutex::new(Vec::new()),
            }
        }

        async fn requests(&self) -> Vec<ProviderRequest> {
            self.requests.lock().await.clone()
        }
    }

    #[async_trait]
    impl ProviderAdapter for JsonProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            self.responses
                .lock()
                .await
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("json provider response exhausted"))?
        }
    }

    fn json_response(summary: &str) -> anyhow::Result<ProviderResponse> {
        Ok(ProviderResponse {
            assistant_message: SessionTurnMessage::assistant_text(format!(
                r#"{{"summary":{}}}"#,
                serde_json::to_string(summary).expect("summary json")
            )),
            stop: ProviderStop::Done,
        })
    }

    fn request() -> DelegationCreateRequest {
        DelegationCreateRequest {
            parent_session_id: SessionId::from_str("session_aaaaaaaa").expect("valid session id"),
            parent_turn_id: "turn-1".into(),
            owner_agent_id: AgentId::new("agent-a").expect("valid agent id"),
            title: "long context task".into(),
            role: "context verifier".into(),
            objective: "remember early facts and continue after compaction".into(),
            constraints: vec!["preserve prior facts".into()],
        }
    }

    async fn started_delegation() -> (
        tempfile::TempDir,
        DelegationStore,
        DelegationMetadata,
        DelegationProgressSink,
    ) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store
            .create_with_id_factory(request(), || {
                DelegationId::from_str("subagent_11111111").expect("valid delegation id")
            })
            .await
            .expect("create delegation");
        let metadata = store.start(&metadata.id).await.expect("start delegation");
        let progress = DelegationProgressSink::for_test(store.clone(), metadata.id.clone());
        (dir, store, metadata, progress)
    }

    fn compacting_config() -> SessionCompactionConfig {
        SessionCompactionConfig {
            auto_compact_ctx_ratio: 0.1,
            tail_previous_real_user_turns: 2,
            summary_max_chars: 2000,
            ..SessionCompactionConfig::default()
        }
    }

    fn compacting_config_with_small_tool_results() -> SessionCompactionConfig {
        let mut cfg = compacting_config();
        cfg.tool_result_raw_max_chars = 64;
        cfg
    }

    fn compactable_messages() -> Vec<SessionTurnMessage> {
        vec![
            SessionTurnMessage::user_text("objective anchor"),
            SessionTurnMessage::assistant_text("old raw assistant detail should be compacted"),
            SessionTurnMessage::user_text("early fact: alpha-token must be remembered"),
            SessionTurnMessage::assistant_text("processed early alpha raw detail"),
            SessionTurnMessage::user_text("recent tail request: continue with beta"),
            SessionTurnMessage::assistant_text("recent tail answer beta"),
        ]
    }

    fn tool_use_message(id: &str, name: &str) -> SessionTurnMessage {
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: id.into(),
                name: name.into(),
                input: serde_json::json!({"path": "target/manual-subagents-work/large.txt"}),
            }],
        }
    }

    fn tool_result_message(id: &str, payload: &str) -> SessionTurnMessage {
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: id.into(),
                content: serde_json::json!({"ok": true, "output": payload}).to_string(),
            }],
        }
    }

    fn compactable_messages_with_large_tool_results() -> Vec<SessionTurnMessage> {
        let compact_payload = format!("COMPACT_RANGE_RAW_TOOL_RESULT_{}", "A".repeat(300));
        let tail_payload = format!("TAIL_RAW_TOOL_RESULT_{}", "B".repeat(300));
        vec![
            SessionTurnMessage::user_text("objective anchor"),
            SessionTurnMessage::assistant_text("old assistant note before tool"),
            tool_use_message("toolu_compact", "code_run"),
            tool_result_message("toolu_compact", &compact_payload),
            SessionTurnMessage::assistant_text("recent assistant note"),
            tool_use_message("toolu_tail", "code_run"),
            tool_result_message("toolu_tail", &tail_payload),
        ]
    }

    fn message_text(messages: &[SessionTurnMessage]) -> String {
        serde_json::to_string(messages).expect("serialize messages")
    }

    fn compactor(
        metadata: DelegationMetadata,
        progress: DelegationProgressSink,
        provider: Arc<JsonProvider>,
    ) -> DelegationPreflightCompactor {
        compactor_with_config(metadata, progress, provider, compacting_config())
    }

    fn compactor_with_config(
        metadata: DelegationMetadata,
        progress: DelegationProgressSink,
        provider: Arc<JsonProvider>,
        mut config: SessionCompactionConfig,
    ) -> DelegationPreflightCompactor {
        // Summary 请求 guard 会计入完整 JSON 和输出预留；同步放大测试窗口并缩放
        // ratio，保持这些 planner 测试原有的 100/200/300 token 绝对阈值。
        const TEST_CONTEXT_SCALE: f64 = 8.0;
        config.auto_compact_ctx_ratio /= TEST_CONTEXT_SCALE;
        config.tail_target_ctx_ratio /= TEST_CONTEXT_SCALE;
        config.tail_hard_ctx_ratio /= TEST_CONTEXT_SCALE;
        compactor_with_limits(metadata, progress, provider, config, 512, 8_000)
    }

    fn compactor_with_limits(
        metadata: DelegationMetadata,
        progress: DelegationProgressSink,
        provider: Arc<JsonProvider>,
        config: SessionCompactionConfig,
        max_tokens: u32,
        context_window: usize,
    ) -> DelegationPreflightCompactor {
        compactor_with_limits_and_retries(
            metadata,
            progress,
            provider,
            config,
            max_tokens,
            context_window,
            0,
        )
    }

    fn compactor_with_limits_and_retries(
        metadata: DelegationMetadata,
        progress: DelegationProgressSink,
        provider: Arc<JsonProvider>,
        config: SessionCompactionConfig,
        max_tokens: u32,
        context_window: usize,
        retry_count: u32,
    ) -> DelegationPreflightCompactor {
        DelegationPreflightCompactor::new(
            metadata,
            progress,
            Arc::new(PromptRegistry::bundled().expect("bundled prompts")),
            Arc::new(StructuredJsonCaller::new(
                provider,
                max_tokens,
                retry_count,
                Duration::ZERO,
                Duration::ZERO,
            )),
            Vec::new(),
            config,
            context_window,
        )
    }

    #[test]
    fn compaction_summary_message_is_user_fragment() {
        let message = delegation_compaction_summary_message("done");
        assert_eq!(message.role, "user");
        let serialized = serde_json::to_string(&message).unwrap();
        assert!(serialized.contains("compacted_subagent_context"));
        assert!(serialized.contains("runtime file-edit authority"));
        assert!(serialized.contains("required_read"));
        assert!(serialized.contains("done"));
    }

    #[test]
    fn parse_summary_rejects_overlong_output_instead_of_truncating() {
        let error = parse_summary(json!({"summary": "五个字符啊"}), 4).unwrap_err();

        assert!(error.to_string().contains("actual_chars=5"));
        assert!(error.to_string().contains("max_chars=4"));
    }

    #[test]
    fn delegation_transcript_message_drops_replay_and_raw_media() {
        let message = SessionTurnMessage::assistant_text("visible answer").with_provider_replay(
            ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: vec![json!({
                    "type": "reasoning",
                    "encrypted_content": "TRANSCRIPT_REPLAY"
                })],
            },
        );
        let mut message = message;
        message.content.push(SessionTurnContentBlock::image(
            "image/png",
            "TRANSCRIPT_IMAGE",
        ));

        let entry =
            transcript_entry_for_message(DelegationTranscriptMessageSource::Assistant, message);
        let rendered = serde_json::to_string(&entry).unwrap();

        assert!(!rendered.contains("TRANSCRIPT_REPLAY"));
        assert!(!rendered.contains("TRANSCRIPT_IMAGE"));
        assert!(rendered.contains("image attachment media_type=image/png"));
    }

    #[tokio::test]
    async fn compaction_summary_projection_drops_replay_and_raw_media() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor(metadata, progress, provider);
        let mut provider_messages = compactable_messages();
        provider_messages[1].provider_replay = Some(ProviderReplayState::OpenAiResponses {
            model: Some("test-model".into()),
            items: vec![json!({
                "type": "reasoning",
                "encrypted_content": "COMPACTION_REPLAY"
            })],
        });
        provider_messages[1]
            .content
            .push(SessionTurnContentBlock::image(
                "image/png",
                "COMPACTION_IMAGE",
            ));

        let plan = compactor
            .build_plan(&provider_messages, 0, 0)
            .expect("projection plan");
        let compact_text = message_text(&plan.compact_messages);

        assert!(!compact_text.contains("COMPACTION_REPLAY"));
        assert!(!compact_text.contains("COMPACTION_IMAGE"));
        assert!(compact_text.contains("image omitted from compaction summary input"));
    }

    #[tokio::test]
    async fn forced_context_recovery_keeps_latest_anthropic_partial_pair_out_of_summary() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor(metadata, progress, provider);
        let mut provider_messages = compactable_messages();
        provider_messages.push(
            SessionTurnMessage::assistant_text("LATEST_CONTEXT_PARTIAL").with_provider_replay(
                ProviderReplayState::AnthropicMessages {
                    model: "test-model".into(),
                    messages: vec![json!({
                        "role":"assistant",
                        "content":[{
                            "type":"thinking",
                            "thinking":"PRIVATE_CONTEXT_REASONING",
                            "signature":"PRIVATE_CONTEXT_SIGNATURE"
                        }, {"type":"text", "text":"LATEST_CONTEXT_PARTIAL"}]
                    })],
                },
            ),
        );
        provider_messages.push(SessionTurnMessage::user_text(
            "INTERNAL_CONTEXT_CONTINUATION",
        ));

        let plan = compactor
            .build_plan(&provider_messages, 0, 2)
            .expect("forced recovery should compact older segments");
        let summary_input = serde_json::to_string(&plan.compact_messages).unwrap();
        let raw_tail = serde_json::to_string(&plan.tail).unwrap();

        assert!(!summary_input.contains("LATEST_CONTEXT_PARTIAL"));
        assert!(!summary_input.contains("PRIVATE_CONTEXT_REASONING"));
        assert!(!summary_input.contains("PRIVATE_CONTEXT_SIGNATURE"));
        assert!(raw_tail.contains("LATEST_CONTEXT_PARTIAL"));
        assert!(raw_tail.contains("PRIVATE_CONTEXT_REASONING"));
        assert!(raw_tail.contains("PRIVATE_CONTEXT_SIGNATURE"));
        assert!(raw_tail.contains("INTERNAL_CONTEXT_CONTINUATION"));
    }

    #[tokio::test]
    async fn context_recovery_marker_precedes_later_steering() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let mut compactor = compactor(metadata, progress, provider);
        let marker = SessionTurnMessage::assistant_text("CONTEXT_PARTIAL").with_provider_replay(
            ProviderReplayState::AnthropicMessages {
                model: "test-model".into(),
                messages: vec![json!({
                    "role":"assistant",
                    "content":[{"type":"text", "text":"CONTEXT_PARTIAL"}]
                })],
            },
        );
        compactor
            .request_context_window_recovery(&marker)
            .expect("recovery marker should be established at the stop response");
        let mut provider_messages = compactable_messages();
        provider_messages.extend([
            marker,
            SessionTurnMessage::user_text("INTERNAL_CONTINUATION"),
            SessionTurnMessage::user_text("LATER_PARENT_STEERING"),
        ]);
        let segments = provider_safe_segments(&provider_messages);
        let protected = context_recovery_protected_tail_from_marker(
            &provider_messages,
            &segments,
            compactor
                .context_window_recovery_tail_marker
                .as_ref()
                .expect("marker should remain available"),
        )
        .expect("the marker should survive later steering");

        let plan = compactor
            .build_plan(&provider_messages, 0, protected)
            .expect("older segments should remain compactable");
        let summary_input = serde_json::to_string(&plan.compact_messages).unwrap();
        let raw_tail = serde_json::to_string(&plan.tail).unwrap();

        assert!(!summary_input.contains("CONTEXT_PARTIAL"));
        assert!(!summary_input.contains("INTERNAL_CONTINUATION"));
        assert!(!summary_input.contains("LATER_PARENT_STEERING"));
        assert!(raw_tail.contains("CONTEXT_PARTIAL"));
        assert!(raw_tail.contains("INTERNAL_CONTINUATION"));
        assert!(raw_tail.contains("LATER_PARENT_STEERING"));
    }

    #[tokio::test]
    async fn protected_context_tool_result_is_not_truncated() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor(metadata, progress, provider);
        let protected_result = "PROTECTED_TOOL_RESULT".repeat(1_024);
        let mut provider_messages = compactable_messages();
        provider_messages.extend([
            SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_context".into(),
                    name: "lookup".into(),
                    input: json!({}),
                }],
            },
            SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_context".into(),
                content: protected_result.clone(),
            }]),
        ]);

        let plan = compactor
            .build_plan(&provider_messages, 0, 1)
            .expect("older segments should remain compactable");
        let summary_input = serde_json::to_string(&plan.compact_messages).unwrap();
        let raw_tail = serde_json::to_string(&plan.tail).unwrap();

        assert!(!summary_input.contains(&protected_result));
        assert!(raw_tail.contains(&protected_result));
        assert!(!raw_tail.contains("large tool_result omitted"));
    }

    #[tokio::test]
    async fn protected_raw_tail_is_mandatory_when_selecting_older_segments() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor(metadata, progress, provider);
        let mut provider_messages = compactable_messages();
        provider_messages[4] = SessionTurnMessage::user_text("U".repeat(200));
        provider_messages[5] = SessionTurnMessage::assistant_text("A".repeat(200));
        provider_messages.extend([
            SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_context_budget".into(),
                    name: "lookup".into(),
                    input: json!({}),
                }],
            },
            SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_context_budget".into(),
                content: "R".repeat(600),
            }]),
        ]);
        let segments = provider_safe_segments(&provider_messages);
        let protected_start = segments[segments.len() - 1].start;
        let raw_mandatory = estimate_session_turn_messages_tokens(&provider_messages[..1])
            .saturating_add(estimate_session_turn_messages_tokens(
                &provider_messages[protected_start..],
            ));
        let base_budget = compactor.provider_projection_budget(0);
        assert!(base_budget.tail_token_limit > raw_mandatory);
        let runtime_reserve = base_budget.tail_token_limit - raw_mandatory;

        let ranges = compactor
            .select_compaction_ranges(&provider_messages, runtime_reserve, 1)
            .expect("older segments should remain compactable");

        assert_eq!(ranges.tail_start_index, segments[segments.len() - 2].start);
        assert!(ranges.tail_start_index > 4);
        assert_eq!(ranges.protected_tail_start_index, Some(protected_start));
    }

    #[tokio::test]
    async fn forced_context_recovery_rejects_when_only_protected_partial_pair_exists() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor(metadata, progress, provider);
        let provider_messages = vec![
            SessionTurnMessage::user_text("objective anchor"),
            SessionTurnMessage::assistant_text("LATEST_CONTEXT_PARTIAL"),
            SessionTurnMessage::user_text("INTERNAL_CONTEXT_CONTINUATION"),
        ];

        assert!(compactor.build_plan(&provider_messages, 0, 2).is_none());
    }

    #[tokio::test]
    async fn overlong_summary_repairs_exhaust_without_advancing_subagent_cursor() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![
            json_response("five!"),
            json_response("still too long"),
        ]));
        let mut config = compacting_config();
        config.summary_max_chars = 4;
        let mut compactor = compactor_with_limits_and_retries(
            metadata.clone(),
            progress,
            Arc::clone(&provider),
            config,
            512,
            8_000,
            1,
        );
        compactor.observe_provider_context_usage(
            6,
            ContextUsageSnapshot {
                used_tokens: 1_000,
                source: ContextUsageSource::Provider,
            },
        );
        let mut provider_messages = compactable_messages();
        let original_messages = provider_messages.clone();
        let mut events = Vec::new();

        compactor
            .before_provider_request(
                &mut "stable subagent system prompt".to_string(),
                &mut provider_messages,
                &mut |event| events.push(event),
            )
            .await
            .expect("below the hard threshold the subagent should keep raw history");

        assert_eq!(provider.requests().await.len(), 2);
        assert_eq!(provider_messages, original_messages);
        assert!(store
            .read_compaction_state(&metadata.id)
            .await
            .unwrap()
            .is_none());
        assert!(!store
            .delegation_dir(&metadata.id)
            .join("compaction_checkpoint.json")
            .exists());
        assert!(events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionFailed { .. })));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionCompleted { .. })));
    }

    #[tokio::test]
    async fn trigger_context_tokens_adds_messages_after_provider_anchor() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let mut compactor = compactor(metadata, progress, provider);
        let provider_messages = vec![
            SessionTurnMessage::user_text("request"),
            SessionTurnMessage::assistant_text("calling tool"),
            tool_result_message("toolu_1", "tool output added after provider response"),
        ];
        compactor.observe_provider_context_usage(
            2,
            ContextUsageSnapshot {
                used_tokens: 1_200,
                source: ContextUsageSource::Provider,
            },
        );
        let full_estimate = estimate_provider_request_context_tokens(
            "system",
            &provider_messages,
            &compactor.tool_specs,
        )
        .used_tokens;
        let expected = 1_200usize
            .saturating_add(estimate_session_turn_messages_tokens(
                &provider_messages[2..],
            ))
            .max(full_estimate);

        assert_eq!(
            compactor.trigger_context_tokens("system", &provider_messages),
            expected
        );

        compactor.observe_provider_context_usage(
            provider_messages.len(),
            ContextUsageSnapshot {
                used_tokens: 1,
                source: ContextUsageSource::Estimate,
            },
        );
        assert_eq!(
            compactor.trigger_context_tokens("system", &provider_messages),
            expected,
            "local estimates must not replace the provider anchor"
        );

        compactor.observe_provider_context_usage(
            provider_messages.len().saturating_add(1),
            ContextUsageSnapshot {
                used_tokens: usize::MAX,
                source: ContextUsageSource::Provider,
            },
        );
        assert_eq!(
            compactor.trigger_context_tokens("system", &provider_messages),
            full_estimate,
            "an out-of-range provider anchor must fall back to the full local estimate"
        );
    }

    #[tokio::test]
    async fn plan_keeps_full_summary_input_and_projects_large_tool_results_for_fallback_and_tail() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response(
            "summary keeps large tool facts without raw output",
        )]));
        let compactor = compactor_with_config(
            metadata,
            progress,
            provider,
            compacting_config_with_small_tool_results(),
        );
        let provider_messages = compactable_messages_with_large_tool_results();
        let plan = compactor
            .build_plan(&provider_messages, 0, 0)
            .expect("projection plan");

        let compact_text = message_text(&plan.compact_messages);
        assert!(compact_text.contains("COMPACT_RANGE_RAW_TOOL_RESULT"));
        let fallback_text = message_text(&plan.compact_messages_with_large_tool_results_omitted);
        assert!(fallback_text.contains("tool_result omitted from compaction summary input"));
        assert!(fallback_text.contains("original_chars="));
        assert!(!fallback_text.contains("COMPACT_RANGE_RAW_TOOL_RESULT"));

        let projected_text = message_text(&plan.projected_messages("summary"));
        assert!(projected_text.contains("large tool_result omitted"));
        assert!(!projected_text.contains("TAIL_RAW_TOOL_RESULT"));
        assert!(projected_text.contains("recent assistant note"));
    }

    #[tokio::test]
    async fn summary_projects_media_but_runtime_projection_keeps_anchor_and_tail() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response(
            "bounded media summary",
        )]));
        let compactor = compactor_with_limits(
            metadata,
            progress,
            Arc::clone(&provider),
            compacting_config_with_small_tool_results(),
            512,
            40_000,
        );
        let mut provider_messages = compactable_messages_with_large_tool_results();
        provider_messages[0] = SessionTurnMessage::user_content(vec![
            SessionTurnContentBlock::text("objective anchor"),
            SessionTurnContentBlock::image(
                "image/png",
                format!("RAW_ANCHOR_IMAGE_{}", "A".repeat(100_000)),
            ),
        ]);
        provider_messages[1] = SessionTurnMessage {
            role: "assistant".into(),
            content: vec![
                SessionTurnContentBlock::text("old assistant note before tool"),
                SessionTurnContentBlock::document_named(
                    "application/pdf",
                    format!("RAW_COMPACT_DOCUMENT_{}", "B".repeat(100_000)),
                    "analysis.pdf",
                ),
            ],
            provider_replay: None,
        };
        provider_messages[6]
            .content
            .push(SessionTurnContentBlock::image(
                "image/webp",
                format!("RAW_TAIL_IMAGE_{}", "C".repeat(100_000)),
            ));

        let plan = compactor
            .build_plan(&provider_messages, 0, 0)
            .expect("media projection plan");
        let compact_input = message_text(&plan.compact_messages);
        assert!(compact_input.contains("document omitted from compaction summary input"));
        assert!(compact_input.contains("analysis.pdf"));
        assert!(!compact_input.contains("RAW_COMPACT_DOCUMENT"));

        compactor
            .generate_summary(&plan)
            .await
            .expect("media-projected summary request");

        let requests = provider.requests().await;
        assert_eq!(requests.len(), 1);
        let summary_payload = message_text(&requests[0].messages);
        assert!(summary_payload.contains("image omitted from compaction summary input"));
        assert!(summary_payload.contains("document omitted from compaction summary input"));
        assert!(!summary_payload.contains("RAW_ANCHOR_IMAGE"));
        assert!(!summary_payload.contains("RAW_COMPACT_DOCUMENT"));

        let runtime_projection = message_text(&plan.projected_messages("bounded media summary"));
        assert!(runtime_projection.contains("RAW_ANCHOR_IMAGE"));
        assert!(runtime_projection.contains("RAW_TAIL_IMAGE"));
    }

    #[tokio::test]
    async fn summary_request_omits_only_large_tool_results_when_full_input_is_over_budget() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response("bounded summary")]));
        let compactor = compactor_with_limits(
            metadata,
            progress,
            Arc::clone(&provider),
            compacting_config(),
            256,
            4_000,
        );
        let compact_source = vec![
            tool_result_message(
                "toolu_large",
                &format!("RAW_DELEGATION_SUMMARY_{}", "X".repeat(40_000)),
            ),
            tool_result_message("toolu_small", "SMALL_DELEGATION_SUMMARY"),
        ];
        let plan =
            CompactionPlan {
                compact_start_index: 1,
                compact_end_index: 3,
                anchor: SessionTurnMessage::user_text("objective anchor"),
                compact_messages: compact_source.clone(),
                compact_messages_with_large_tool_results_omitted:
                    project_compaction_input_tool_results(compact_source.clone(), 128),
                compact_messages_with_tool_results_omitted: omit_turn_messages_tool_results(
                    compact_source,
                ),
                tail: Vec::new(),
            };

        let summary = compactor
            .generate_summary(&plan)
            .await
            .expect("large-only omission summary");

        assert_eq!(summary, "bounded summary");
        let requests = provider.requests().await;
        assert_eq!(requests.len(), 1);
        let payload = message_text(&requests[0].messages);
        assert!(payload.contains("tool_result omitted from compaction summary input"));
        assert!(!payload.contains("RAW_DELEGATION_SUMMARY"));
        assert!(payload.contains("SMALL_DELEGATION_SUMMARY"));
    }

    #[tokio::test]
    async fn summary_request_omits_all_tool_results_when_large_only_projection_is_over_budget() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response("bounded summary")]));
        let compactor = compactor_with_limits(
            metadata,
            progress,
            Arc::clone(&provider),
            compacting_config(),
            256,
            4_000,
        );
        let raw_tool_result = format!("RAW_DELEGATION_SUMMARY_{}", "X".repeat(40_000));
        let compact_source = vec![tool_result_message("toolu_summary", &raw_tool_result)];
        let plan = CompactionPlan {
            compact_start_index: 1,
            compact_end_index: 2,
            anchor: SessionTurnMessage::user_text("objective anchor"),
            compact_messages: compact_source.clone(),
            compact_messages_with_large_tool_results_omitted: compact_source.clone(),
            compact_messages_with_tool_results_omitted: omit_turn_messages_tool_results(
                compact_source,
            ),
            tail: Vec::new(),
        };

        let summary = compactor
            .generate_summary(&plan)
            .await
            .expect("fallback summary");

        assert_eq!(summary, "bounded summary");
        let requests = provider.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].retry_count_override, Some(0));
        let payload = message_text(&requests[0].messages);
        assert!(payload.contains("tool_result omitted from compaction summary input"));
        assert!(!payload.contains("RAW_DELEGATION_SUMMARY"));
    }

    #[tokio::test]
    async fn summary_request_fails_locally_when_output_reserve_fills_context_window() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor_with_limits(
            metadata,
            progress,
            Arc::clone(&provider),
            compacting_config(),
            256,
            256,
        );
        let plan = CompactionPlan {
            compact_start_index: 1,
            compact_end_index: 2,
            anchor: SessionTurnMessage::user_text("objective anchor"),
            compact_messages: vec![SessionTurnMessage::assistant_text("plain summary input")],
            compact_messages_with_large_tool_results_omitted: vec![
                SessionTurnMessage::assistant_text("plain summary input"),
            ],
            compact_messages_with_tool_results_omitted: vec![SessionTurnMessage::assistant_text(
                "plain summary input",
            )],
            tail: Vec::new(),
        };

        let error = compactor
            .generate_summary(&plan)
            .await
            .expect_err("summary payload cannot fit beside max output reserve");

        assert!(error
            .to_string()
            .contains("remains over budget after omitting all tool results"));
        assert!(provider.requests().await.is_empty());
    }

    #[tokio::test]
    async fn summary_request_rechecks_budget_before_json_retry() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let invalid = Ok(ProviderResponse {
            assistant_message: SessionTurnMessage::assistant_text("界".repeat(4_000)),
            stop: ProviderStop::Done,
        });
        let provider = Arc::new(JsonProvider::new(vec![
            invalid,
            json_response("must not be requested"),
        ]));
        let compactor = compactor_with_limits_and_retries(
            metadata,
            progress,
            Arc::clone(&provider),
            compacting_config(),
            256,
            1_500,
            1,
        );
        let plan = CompactionPlan {
            compact_start_index: 1,
            compact_end_index: 2,
            anchor: SessionTurnMessage::user_text("objective anchor"),
            compact_messages: vec![SessionTurnMessage::assistant_text("plain summary input")],
            compact_messages_with_large_tool_results_omitted: vec![
                SessionTurnMessage::assistant_text("plain summary input"),
            ],
            compact_messages_with_tool_results_omitted: vec![SessionTurnMessage::assistant_text(
                "plain summary input",
            )],
            tail: Vec::new(),
        };

        let error = compactor
            .generate_summary(&plan)
            .await
            .expect_err("retry correction should exceed the local compaction budget");

        assert!(error
            .to_string()
            .contains("provider attempt exceeds context window"));
        assert_eq!(provider.requests().await.len(), 1);
    }

    #[tokio::test]
    async fn runtime_projection_tokens_are_reserved_from_delegation_tail_budget() {
        let (_dir, _store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(Vec::new()));
        let compactor = compactor(metadata, progress, provider);

        let budget = compactor.provider_projection_budget(75);

        assert_eq!(budget.tail_token_limit, 125);
        assert_eq!(budget.tail_hard_token_limit, 225);
    }

    #[tokio::test]
    async fn compaction_rejects_combined_projection_before_persisting_completion() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response(&"S".repeat(1400))]));
        let mut compactor = compactor(metadata.clone(), progress, provider);
        compactor.observe_provider_context_usage(
            6,
            ContextUsageSnapshot {
                used_tokens: 500,
                source: ContextUsageSource::Provider,
            },
        );
        let system_prompt = "stable subagent system prompt".to_string();
        let mut provider_messages = compactable_messages();
        let original_messages = provider_messages.clone();
        let mut events = Vec::new();

        let error = compactor
            .before_provider_request_with_runtime_reserve(
                &system_prompt,
                &mut provider_messages,
                75,
                &mut |event| events.push(event),
            )
            .await
            .expect_err("combined compacted projection should exceed hard tail");

        let error = error.to_string();
        assert!(error.contains("estimated raw tail tokens="));
        assert!(error.contains("runtime projection tokens=75"));
        assert!(error.contains("combined tail tokens="));
        assert!(error.contains("hard tail budget=300"));
        assert_eq!(provider_messages, original_messages);
        assert!(
            store
                .read_compaction_state(&metadata.id)
                .await
                .expect("read compaction state")
                .is_none(),
            "failed projection must not persist completed compaction state"
        );
        assert!(
            !store
                .delegation_dir(&metadata.id)
                .join("compaction_checkpoint.json")
                .exists(),
            "failed projection should clear the in-flight checkpoint"
        );
        let entries = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript");
        assert!(entries.iter().any(|entry| matches!(
            entry.kind,
            DelegationTranscriptKind::CompactionFailed { .. }
        )));
        assert!(!entries.iter().any(|entry| matches!(
            entry.kind,
            DelegationTranscriptKind::CompactionBoundary { .. }
        )));
        assert!(events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionFailed { .. })));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionCompleted { .. })));
    }

    #[tokio::test]
    async fn provider_anchor_suffix_tool_result_triggers_compaction_before_hard_guard() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response("bounded summary")]));
        let mut compactor = compactor_with_limits(
            metadata.clone(),
            progress,
            Arc::clone(&provider),
            compacting_config_with_small_tool_results(),
            512,
            8_000,
        );
        let mut provider_messages = compactable_messages_with_large_tool_results();
        let provider_message_count = provider_messages.len().saturating_sub(1);
        provider_messages[provider_message_count] = tool_result_message(
            "toolu_tail",
            &format!("ANCHOR_SUFFIX_RAW_TOOL_RESULT_{}", "Z".repeat(40_000)),
        );
        compactor.observe_provider_context_usage(
            provider_message_count,
            ContextUsageSnapshot {
                used_tokens: 700,
                source: ContextUsageSource::Provider,
            },
        );
        let mut events = Vec::new();

        compactor
            .before_provider_request_with_runtime_reserve(
                "stable subagent system prompt",
                &mut provider_messages,
                0,
                &mut |event| events.push(event),
            )
            .await
            .expect("large tool result after provider anchor should compact before hard guard");

        assert_eq!(provider.requests().await.len(), 1);
        assert!(events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionStarted { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionCompleted { .. })));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionFailed { .. })));
        assert!(store
            .read_compaction_state(&metadata.id)
            .await
            .expect("read compaction state")
            .is_some());
        let projected = message_text(&provider_messages);
        assert!(projected.contains("<compacted_subagent_context>"));
        assert!(!projected.contains("ANCHOR_SUFFIX_RAW_TOOL_RESULT"));
    }

    #[tokio::test]
    async fn transcript_can_keep_raw_tool_result_when_projection_omits_it() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        let raw_payload = format!("RAW_TRANSCRIPT_TOOL_RESULT_{}", "C".repeat(300));
        progress
            .append_transcript_entry(transcript_entry_for_message(
                DelegationTranscriptMessageSource::ToolResult,
                tool_result_message("toolu_raw", &raw_payload),
            ))
            .await
            .expect("write raw transcript");

        let entries = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript");
        let serialized = serde_json::to_string(&entries).expect("serialize transcript");
        assert!(serialized.contains("RAW_TRANSCRIPT_TOOL_RESULT"));
        assert!(!serialized.contains("large tool_result omitted"));
    }

    #[tokio::test]
    async fn preflight_uses_full_summary_input_but_projected_tail_and_transcript_keeps_raw() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response(
            "summary records compact and tail tool facts",
        )]));
        let mut compactor = compactor_with_config(
            metadata.clone(),
            progress.clone(),
            Arc::clone(&provider),
            compacting_config_with_small_tool_results(),
        );
        let mut provider_messages = compactable_messages_with_large_tool_results();
        for message in &provider_messages {
            progress
                .append_transcript_entry(transcript_entry_for_message(
                    transcript_source_for_message(message),
                    message.clone(),
                ))
                .await
                .expect("append raw transcript");
        }
        compactor.observe_provider_context_usage(
            provider_messages.len(),
            ContextUsageSnapshot {
                used_tokens: 500,
                source: ContextUsageSource::Provider,
            },
        );
        let mut system_prompt = "stable subagent system prompt".to_string();

        compactor
            .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |_| {})
            .await
            .expect("compact with full summary input and projected tail");

        let requests = provider.requests().await;
        assert_eq!(requests.len(), 1);
        let compaction_payload = message_text(&requests[0].messages);
        assert!(compaction_payload.contains("COMPACT_RANGE_RAW_TOOL_RESULT"));

        let projected_history = message_text(&provider_messages);
        assert!(projected_history.contains("large tool_result omitted"));
        assert!(!projected_history.contains("TAIL_RAW_TOOL_RESULT"));

        let transcript = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript");
        let transcript_text = serde_json::to_string(&transcript).expect("serialize transcript");
        assert!(transcript_text.contains("COMPACT_RANGE_RAW_TOOL_RESULT"));
        assert!(transcript_text.contains("TAIL_RAW_TOOL_RESULT"));
        assert!(transcript_text.contains("compaction_boundary"));
    }

    #[tokio::test]
    async fn preflight_compacts_long_history_and_persists_debug_files() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        let provider = Arc::new(JsonProvider::new(vec![json_response(
            "summary retains alpha-token and says next step is beta",
        )]));
        let mut compactor = compactor(metadata.clone(), progress, Arc::clone(&provider));
        compactor.observe_provider_context_usage(
            6,
            ContextUsageSnapshot {
                used_tokens: 500,
                source: ContextUsageSource::Provider,
            },
        );
        let mut system_prompt = "stable subagent system prompt".to_string();
        let original_system_prompt = system_prompt.clone();
        let mut provider_messages = compactable_messages();
        let mut events = Vec::new();

        compactor
            .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |event| {
                events.push(event);
            })
            .await
            .expect("compact before provider request");

        assert_eq!(system_prompt, original_system_prompt);
        assert!(events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionStarted { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::CompactionCompleted { .. })));
        let projected = message_text(&provider_messages);
        assert!(projected.contains("<compacted_subagent_context>"));
        assert!(projected.contains("summary retains alpha-token"));
        assert!(projected.contains("recent tail request: continue with beta"));
        assert!(!projected.contains("processed early alpha raw detail"));

        let state = store
            .read_compaction_state(&metadata.id)
            .await
            .expect("read compaction state")
            .expect("compaction state written");
        assert_eq!(
            state.summary,
            "summary retains alpha-token and says next step is beta"
        );
        assert_eq!(state.compacted_until, 4);
        assert!(
            !store
                .delegation_dir(&metadata.id)
                .join("compaction_checkpoint.json")
                .exists(),
            "successful compaction should clear checkpoint"
        );
        assert!(store
            .delegation_dir(&metadata.id)
            .join("compaction_events.jsonl")
            .exists());
        let entries = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript");
        assert!(entries.iter().any(|entry| matches!(
            entry.kind,
            DelegationTranscriptKind::CompactionBoundary { .. }
        )));

        match store
            .read(&metadata.id, DelegationReadMode::Summary)
            .await
            .expect("read summary")
        {
            DelegationRead::Summary {
                compaction_summary, ..
            } => assert_eq!(
                compaction_summary.as_deref(),
                Some("summary retains alpha-token and says next step is beta")
            ),
            other => panic!("unexpected read: {other:?}"),
        }
    }

    #[tokio::test]
    async fn later_compaction_merges_prior_summary_for_continued_work() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        progress
            .write_compaction_state(&DelegationCompactionState {
                schema_version: DELEGATION_COMPACTION_SCHEMA_VERSION,
                compacted_until: 4,
                summary: "prior summary remembers alpha-token".into(),
                summary_updated_at: Utc::now(),
            })
            .await
            .expect("seed prior compaction");
        let provider = Arc::new(JsonProvider::new(vec![json_response(
            "merged summary keeps alpha-token and adds beta-token",
        )]));
        let mut compactor = compactor(metadata.clone(), progress, Arc::clone(&provider));
        compactor.observe_provider_context_usage(
            6,
            ContextUsageSnapshot {
                used_tokens: 500,
                source: ContextUsageSource::Provider,
            },
        );
        let mut system_prompt = "stable subagent system prompt".to_string();
        let mut provider_messages = compactable_messages();
        let mut events = Vec::new();

        compactor
            .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |event| {
                events.push(event);
            })
            .await
            .expect("compact with prior summary");

        let requests = provider.requests().await;
        assert_eq!(requests.len(), 1);
        let payload = message_text(&requests[0].messages);
        assert!(payload.contains("prior summary remembers alpha-token"));
        assert!(payload.contains("early fact: alpha-token must be remembered"));

        let projected = message_text(&provider_messages);
        assert!(projected.contains("merged summary keeps alpha-token"));
        assert!(projected.contains("recent tail request: continue with beta"));
        let state = store
            .read_compaction_state(&metadata.id)
            .await
            .expect("read compaction state")
            .expect("state");
        assert_eq!(
            state.summary,
            "merged summary keeps alpha-token and adds beta-token"
        );
    }

    #[tokio::test]
    async fn abandon_while_compaction_artifacts_exist_keeps_files_readable() {
        let (_dir, store, metadata, progress) = started_delegation().await;
        progress
            .write_compaction_checkpoint(&json!({
                "schema_version": DELEGATION_COMPACTION_SCHEMA_VERSION,
                "compact_start_index": 1,
                "compact_end_index": 4,
                "reason": "test in-flight compaction"
            }))
            .await
            .expect("write checkpoint");
        progress
            .append_compaction_event(DelegationCompactionEventKind::Started {
                compact_start_index: 1,
                compact_end_index: 4,
                reason: "test in-flight compaction".into(),
            })
            .await
            .expect("write compaction event");
        progress
            .append_transcript_entry(DelegationTranscriptEntry {
                at: Utc::now(),
                kind: DelegationTranscriptKind::CompactionFailed {
                    error: "interrupted during test".into(),
                },
            })
            .await
            .expect("write transcript");

        let updated = store
            .abandon_unfinished_for_session(&metadata.parent_session_id, "session closed")
            .await
            .expect("abandon unfinished delegation");

        assert_eq!(updated.len(), 1);
        let metadata = store.load(&metadata.id).await.expect("load metadata");
        assert_eq!(metadata.status, DelegationStatus::Abandoned);
        assert!(metadata.completed_at.is_some());
        assert!(
            store
                .delegation_dir(&metadata.id)
                .join("compaction_checkpoint.json")
                .exists(),
            "abandon should not erase debug checkpoint"
        );
        let entries = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript after abandon");
        assert!(entries.iter().any(|entry| matches!(
            entry.kind,
            DelegationTranscriptKind::CompactionFailed { .. }
        )));
        let events = store
            .read_events_tail(&metadata.id, 10)
            .await
            .expect("read events after abandon");
        assert!(events
            .iter()
            .any(|event| matches!(event.kind, DelegationEventKind::Abandoned { .. })));
    }
}
