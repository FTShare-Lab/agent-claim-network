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
    estimate_provider_request_context_tokens, estimate_session_turn_messages_tokens,
    estimated_projected_segment_tokens, project_turn_messages_tool_results, provider_safe_segments,
    ContextUsageSnapshot, ContextUsageSource, ProviderProjectionBudget, SessionTurnContentBlock,
    SessionTurnEvent, SessionTurnMessage, SessionTurnPreflight, StructuredJsonCaller, ToolSpec,
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

pub struct DelegationPreflightCompactor {
    metadata: DelegationMetadata,
    progress: DelegationProgressSink,
    prompt_registry: Arc<PromptRegistry>,
    json_caller: Arc<StructuredJsonCaller>,
    tool_specs: Vec<ToolSpec>,
    compaction: SessionCompactionConfig,
    context_window: usize,
    provider_context_anchor: Option<ContextUsageSnapshot>,
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
        }
    }

    async fn maybe_compact(
        &mut self,
        system_prompt: &str,
        provider_messages: &mut Vec<SessionTurnMessage>,
        runtime_projection_tokens: usize,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        let Some(trigger_threshold) =
            auto_compact_threshold(self.context_window, self.compaction.auto_compact_ctx_ratio)
        else {
            return Ok(());
        };
        let estimate = estimate_provider_request_context_tokens(
            system_prompt,
            provider_messages,
            &self.tool_specs,
        );
        let trigger_tokens = self
            .provider_context_anchor
            .filter(|usage| usage.source == ContextUsageSource::Provider)
            .map(|usage| usage.used_tokens)
            .unwrap_or(estimate.used_tokens)
            .saturating_add(runtime_projection_tokens);
        if trigger_tokens < trigger_threshold {
            return Ok(());
        }
        let hard_threshold = hard_threshold(self.context_window);
        if provider_messages.len() < 4 {
            if trigger_tokens >= hard_threshold {
                self.record_hard_failure("subagent context is too large to compact safely: not enough completed message history")
                    .await?;
                anyhow::bail!(
                    "subagent context is too large to compact safely: not enough completed message history"
                );
            }
            return Ok(());
        }

        let Some(plan) = self.build_plan(provider_messages, runtime_projection_tokens) else {
            if trigger_tokens >= hard_threshold {
                self.record_hard_failure("subagent context is too large to compact safely: no compactable transcript range")
                    .await?;
                anyhow::bail!(
                    "subagent context is too large to compact safely: no compactable transcript range"
                );
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
                reason: DEFAULT_COMPACTION_REASON.to_string(),
            })
            .await?;
        let checkpoint = json!({
            "schema_version": DELEGATION_COMPACTION_SCHEMA_VERSION,
            "compact_start_index": plan.compact_start_index,
            "compact_end_index": plan.compact_end_index,
            "reason": DEFAULT_COMPACTION_REASON,
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
                let _ = self.progress.clear_compaction_checkpoint().await;
                emit(SessionTurnEvent::CompactionFailed {
                    error: error_text.clone(),
                });
                if trigger_tokens >= hard_threshold {
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
            let _ = self.progress.clear_compaction_checkpoint().await;
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
        emit(SessionTurnEvent::CompactionCompleted {
            compacted_until: state.compacted_until,
            recapped_until: 0,
            new_claim_ids: Vec::new(),
            updated_claim_ids: Vec::new(),
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

    fn build_plan(
        &self,
        provider_messages: &[SessionTurnMessage],
        runtime_projection_tokens: usize,
    ) -> Option<CompactionPlan> {
        let ranges = self.select_compaction_ranges(provider_messages, runtime_projection_tokens)?;
        let compact_start_index = 1;
        let compact_end_index = ranges.compact_end_index;
        if compact_end_index <= compact_start_index {
            return None;
        }
        let anchor = provider_messages.first()?.clone();
        let compact_messages = project_turn_messages_tool_results(
            provider_messages
                .get(compact_start_index..compact_end_index)?
                .to_vec(),
            self.compaction.tool_result_raw_max_chars,
        );
        let tail = project_turn_messages_tool_results(
            provider_messages.get(ranges.tail_start_index..)?.to_vec(),
            self.compaction.tool_result_raw_max_chars,
        );
        Some(CompactionPlan {
            compact_start_index,
            compact_end_index,
            anchor,
            compact_messages,
            tail,
        })
    }

    fn select_compaction_ranges(
        &self,
        provider_messages: &[SessionTurnMessage],
        runtime_projection_tokens: usize,
    ) -> Option<CompactionRanges> {
        if provider_messages.len() < 2 {
            return None;
        }
        let segments = provider_safe_segments(provider_messages);
        if segments.is_empty() {
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
        let suffix_tokens = if suffix_start < provider_messages.len() {
            estimate_session_turn_messages_tokens(&project_turn_messages_tool_results(
                provider_messages[suffix_start..].to_vec(),
                budget.tool_result_raw_max_chars,
            ))
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
        let mut tail_start_index = suffix_start;
        for segment in segments.iter().rev() {
            if segment.end > suffix_start {
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
        let payload = DelegationCompactionPayload {
            subagent_id: self.metadata.id.to_string(),
            parent_session_id: self.metadata.parent_session_id.to_string(),
            title: self.metadata.title.clone(),
            role: self.metadata.role.clone(),
            objective: self.metadata.objective.clone(),
            constraints: self.metadata.constraints.clone(),
            objective_anchor: plan.anchor.clone(),
            prior_summary,
            compact_start_index: plan.compact_start_index,
            compact_end_index: plan.compact_end_index,
            transcript: plan.compact_messages.clone(),
            summary_max_chars: self.compaction.summary_max_chars,
        };
        let user_text = serde_json::to_string_pretty(&payload)?;
        let value = self
            .json_caller
            .generate_json_validated_with_retry_notice(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
                |value| parse_summary(value, self.compaction.summary_max_chars),
                |_, _, _| {},
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
        _provider_message_count: usize,
        usage: ContextUsageSnapshot,
    ) {
        self.provider_context_anchor = Some(usage);
    }

    fn clear_provider_context_usage(&mut self) {
        self.provider_context_anchor = None;
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
    tail: Vec<SessionTurnMessage>,
}

struct CompactionRanges {
    compact_end_index: usize,
    tail_start_index: usize,
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
    let mut out = String::new();
    for ch in summary.chars().take(summary_max_chars) {
        out.push(ch);
    }
    Ok(out)
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
        kind: DelegationTranscriptKind::Message { source, message },
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
        ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
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
        config: SessionCompactionConfig,
    ) -> DelegationPreflightCompactor {
        DelegationPreflightCompactor::new(
            metadata,
            progress,
            Arc::new(PromptRegistry::bundled().expect("bundled prompts")),
            Arc::new(StructuredJsonCaller::new(
                provider,
                512,
                0,
                Duration::ZERO,
                Duration::ZERO,
            )),
            Vec::new(),
            config,
            1000,
        )
    }

    #[test]
    fn compaction_summary_message_is_user_fragment() {
        let message = delegation_compaction_summary_message("done");
        assert_eq!(message.role, "user");
        let serialized = serde_json::to_string(&message).unwrap();
        assert!(serialized.contains("compacted_subagent_context"));
        assert!(serialized.contains("done"));
    }

    #[tokio::test]
    async fn projection_replaces_large_tool_results_in_compact_range_and_tail() {
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
            .build_plan(&provider_messages, 0)
            .expect("projection plan");

        let compact_text = message_text(&plan.compact_messages);
        assert!(compact_text.contains("large tool_result omitted"));
        assert!(compact_text.contains("original_chars="));
        assert!(!compact_text.contains("COMPACT_RANGE_RAW_TOOL_RESULT"));

        let projected_text = message_text(&plan.projected_messages("summary"));
        assert!(projected_text.contains("large tool_result omitted"));
        assert!(!projected_text.contains("TAIL_RAW_TOOL_RESULT"));
        assert!(projected_text.contains("recent assistant note"));
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
    async fn preflight_uses_projection_but_transcript_keeps_raw_tool_output() {
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
            .expect("compact with projected large tool outputs");

        let requests = provider.requests().await;
        assert_eq!(requests.len(), 1);
        let compaction_payload = message_text(&requests[0].messages);
        assert!(compaction_payload.contains("large tool_result omitted"));
        assert!(!compaction_payload.contains("COMPACT_RANGE_RAW_TOOL_RESULT"));

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
