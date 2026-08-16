//! SessionEngine session finalize / recap 流程。
//!
//! 本模块维护 finalize 状态转换、recap JSON 生成、finalize checkpoint
//! 与 prepared claim/dispute/trace 的落库上传。它保持原 SessionEngine public
//! 方法签名不变，供 TUI 和 supervisor 继续调用。

use anyhow::Context;
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;

use crate::agent::prepare::{allowed_claim_ids_for_recap, llm_visible_claims};
use crate::agent::runner_finalize::prepare_recap_value;
use crate::agent::runner_trace::trace_name_from_task;
use crate::api::SessionTurnMessage;
use crate::claim::{Claim, ClaimId, Dispute, SessionId, SourceId, TraceId};
use crate::session::{
    replay_turn_journal, FinalizeCheckpoint, FinalizeCheckpointStatus, SessionHandle,
    SessionMessage, SessionStatus,
};
use crate::storage::FileLockGuard;

use super::compaction_projection::validate_session_compaction_state;
use super::events::emit_warnings;
use super::transcript::{session_messages_to_turn_transcript, session_trace_text};
use super::{
    checkpoint_trace_id, hash_session_segment, merge_finalize_reports,
    report_from_finalize_checkpoint, stable_hash_json, validate_finalize_checkpoint_segment,
    RecoverableCompactionPreparationError, SessionEngine, SessionEvent, SessionFinalizeReport,
    SessionRecapBackgroundProcessCompletion, SessionRecapBackgroundProcessProjection,
    SessionRecapPayload, SessionRuntimeStatus, PROMPT_SESSION_RECAP, RECAP_INSTRUCTION,
    STABLE_HASH_OFFSET,
};

const FINALIZE_BACKGROUND_COMPLETION_MAX_ITEMS: usize = 64;
const FINALIZE_BACKGROUND_COMPLETION_ID_MAX_CHARS: usize = 256;

pub(super) enum FinalizeTraceInput<'a> {
    Messages(&'a [SessionMessage]),
    Frozen(&'a str),
}

fn bounded_completion_id(value: &str) -> String {
    value
        .chars()
        .take(FINALIZE_BACKGROUND_COMPLETION_ID_MAX_CHARS)
        .collect()
}

pub(super) fn hash_finalize_recap_input(
    messages: &[SessionMessage],
    background: &SessionRecapBackgroundProcessProjection,
) -> anyhow::Result<String> {
    if background.items.is_empty() {
        return hash_session_segment(messages);
    }
    let mut hash = STABLE_HASH_OFFSET;
    for message in messages {
        stable_hash_json(&mut hash, message)?;
    }
    stable_hash_json(&mut hash, background)?;
    Ok(format!("{hash:016x}"))
}

fn finalize_trace_text(
    messages: &[SessionMessage],
    background: &SessionRecapBackgroundProcessProjection,
) -> anyhow::Result<String> {
    let mut text = session_trace_text(messages);
    if !background.items.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str("<background_process_completions>\n");
        text.push_str(&serde_json::to_string(background)?);
        text.push_str("\n</background_process_completions>");
    }
    Ok(text)
}

impl SessionEngine {
    pub async fn mark_session_finalizing<F>(
        &self,
        session: &mut SessionHandle,
        emit: &mut F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent),
    {
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some()
            || metadata.status == SessionStatus::Closed
            || metadata.closed_at.is_some()
        {
            anyhow::bail!("session {} 已关闭，不能进入 finalizing", metadata.id);
        }
        session.mark_finalizing(Utc::now()).await?;
        self.append_session_event_log(session, "INFO", "Finalize enqueued")
            .await;
        // root session 进入 finalizing 后必须立刻回收其受管 terminal；delegation store
        // 持久化失败不能把本地进程清理短路掉。
        self.settle_processes_for_session_finalization(session, emit)
            .await?;
        self.abandon_session_delegations(session, "session finalizing")
            .await?;
        Ok(())
    }

    pub async fn finalize_existing_session<F>(
        &self,
        session_id: &SessionId,
        emit: F,
    ) -> anyhow::Result<SessionFinalizeReport>
    where
        F: FnMut(SessionEvent),
    {
        let mut emit = emit;
        let mut session = self.load_existing_session(session_id).await?;
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            return Ok(SessionFinalizeReport::default());
        }
        if metadata.status == SessionStatus::Open {
            self.mark_session_finalizing(&mut session, &mut emit)
                .await?;
        }
        self.finalize_session(&mut session, emit).await
    }

    pub async fn finalize_session<F>(
        &self,
        session: &mut SessionHandle,
        mut emit: F,
    ) -> anyhow::Result<SessionFinalizeReport>
    where
        F: FnMut(SessionEvent),
    {
        let _finalize_guard = FileLockGuard::lock_exclusive(&session.paths.finalize_lock).await?;
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            return Ok(SessionFinalizeReport::default());
        }
        if metadata.status == SessionStatus::Open {
            self.mark_session_finalizing(session, &mut emit).await?;
        } else {
            // 旧的 finalizing session 可能来自一次中断/失败的 finalize；它不会再次经过
            // mark_session_finalizing，仍要保证 retry 时收回 root-session 进程。
            self.settle_processes_for_session_finalization(session, &mut emit)
                .await?;
            self.abandon_session_delegations(session, "session finalizing")
                .await?;
        }
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Finalizing,
        });
        emit(SessionEvent::FinalizeStarted);
        self.append_session_event_log(session, "INFO", "Finalize started")
            .await;
        let result = self.finalize_session_inner(session).await;
        match result {
            Ok(report) => {
                self.abandon_session_delegations_best_effort(session, "session finalized")
                    .await;
                emit_warnings(&report.warnings, &mut emit);
                self.append_session_warnings_log(session, &report.warnings)
                    .await;
                emit(SessionEvent::FinalizeCompleted {
                    trace_id: report.trace_id.clone(),
                    new_claim_ids: report.new_claim_ids.clone(),
                    updated_claim_ids: report.updated_claim_ids.clone(),
                    new_dispute_ids: report.new_dispute_ids.clone(),
                });
                self.append_session_event_log(
                    session,
                    "INFO",
                    format!(
                        "Finalize completed: trace_id={} new_claims={} updated_claims={} new_disputes={}",
                        report
                            .trace_id
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "None".into()),
                        report.new_claim_ids.len(),
                        report.updated_claim_ids.len(),
                        report.new_dispute_ids.len()
                    ),
                )
                .await;
                self.emit_local_claims_updated(&mut emit).await;
                emit(SessionEvent::SessionClosed);
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Closed,
                });
                Ok(report)
            }
            Err(e) => {
                let error = e.to_string();
                emit(SessionEvent::FinalizeFailed {
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Finalize failed: {error}"),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                Err(e)
            }
        }
    }

    pub(super) async fn finalize_session_inner(
        &self,
        session: &mut SessionHandle,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            anyhow::bail!("session {} 已 finalize，不能重复 finalize", metadata.id);
        }
        if metadata.status == SessionStatus::Closed || metadata.closed_at.is_some() {
            anyhow::bail!("session {} 已关闭，不能重复关闭", metadata.id);
        }
        let started_with_unrecapped_messages = metadata.message_count > metadata.recapped_until;
        let mut recovered_report = SessionFinalizeReport::default();
        if let Some(outcome) = self
            .recover_matching_compaction_checkpoint(session, None, None)
            .await?
        {
            let recapped_until = session.read_metadata().await?.recapped_until;
            self.append_compaction_audit_completed(
                session,
                &outcome.audit_ids,
                &outcome,
                recapped_until,
                outcome.recovered,
            )
            .await;
            recovered_report = merge_finalize_reports(recovered_report, outcome.report);
            let metadata = session.read_metadata().await?;
            if metadata.finalized_at.is_some() {
                anyhow::bail!("session {} 已 finalize，不能重复 finalize", metadata.id);
            }
            if metadata.status == SessionStatus::Closed || metadata.closed_at.is_some() {
                anyhow::bail!("session {} 已关闭，不能重复关闭", metadata.id);
            }
        }
        if let Some(report) = self.recover_legacy_finalize_checkpoint(session).await? {
            return Ok(merge_finalize_reports(recovered_report, report));
        }
        let metadata = session.read_metadata().await?;
        let background_process_completions = self
            .session_recap_background_process_completions(session)
            .await?;
        if metadata.message_count == 0 && background_process_completions.items.is_empty() {
            session.mark_finalized(Utc::now()).await?;
            if let Err(e) = self.delete_empty_session(&metadata.id).await {
                log::warn!(
                    target: "agent",
                    "清理空 session 失败 ({}): {e:#}",
                    metadata.id
                );
            }
            return Ok(recovered_report);
        }

        let session_messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, session_messages.len())?;
        let recapped_until = metadata.recapped_until;
        if recapped_until == metadata.message_count
            && background_process_completions.items.is_empty()
        {
            session.mark_finalized(Utc::now()).await?;
            recovered_report.finalized_unrecapped_messages = started_with_unrecapped_messages;
            return Ok(recovered_report);
        }
        let mut report = self
            .finalize_message_segment_checkpointed(
                session,
                &session_messages,
                recapped_until,
                metadata.message_count,
                &background_process_completions,
            )
            .await?;
        report = merge_finalize_reports(recovered_report, report);
        session
            .recap_and_mark_finalized_with_background_cursor(
                metadata.message_count,
                background_process_completions.consumed_through_seq,
                Utc::now(),
            )
            .await?;
        report.advanced_recapped_until = true;
        report.finalized_unrecapped_messages = true;
        Ok(std::mem::take(&mut report))
    }

    async fn recover_legacy_finalize_checkpoint(
        &self,
        session: &mut SessionHandle,
    ) -> anyhow::Result<Option<SessionFinalizeReport>> {
        let metadata = session.read_metadata().await?;
        if metadata.recap_background_completion_until_seq.is_some() {
            return Ok(None);
        }
        let Some(checkpoint) = session.read_finalize_checkpoint().await? else {
            return Ok(None);
        };
        let session_messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, session_messages.len())?;
        let background_process_completions = self
            .session_recap_background_process_completions(session)
            .await?;
        let current_segment = session_messages
            .get(metadata.recapped_until..metadata.message_count)
            .with_context(|| {
                format!(
                    "legacy session finalize 范围越界: [{}, {})",
                    metadata.recapped_until, metadata.message_count
                )
            })?;
        let legacy_background_process_completions = self
            .legacy_session_recap_background_process_completions(session)
            .await;
        let legacy_input_hash =
            hash_finalize_recap_input(current_segment, &legacy_background_process_completions)?;
        let matches_current_input = checkpoint.recap_start_index == metadata.recapped_until
            && checkpoint.recap_end_index == metadata.message_count
            && checkpoint.recap_segment_hash == legacy_input_hash;
        if !matches_current_input {
            log::warn!(
                target: "agent",
                "丢弃与当前输入不匹配的旧 finalize checkpoint: session={} checkpoint_range=[{}, {}) current_range=[{}, {})",
                metadata.id,
                checkpoint.recap_start_index,
                checkpoint.recap_end_index,
                metadata.recapped_until,
                metadata.message_count
            );
            session
                .discard_legacy_finalize_checkpoint_and_advance_recap_background_cursor(
                    background_process_completions.consumed_through_seq,
                )
                .await?;
            return Ok(None);
        }

        let mut report = match checkpoint.status {
            FinalizeCheckpointStatus::Prepared => {
                self.apply_finalize_checkpoint(session, checkpoint).await?
            }
            FinalizeCheckpointStatus::Applied => {
                report_from_finalize_checkpoint(&checkpoint, Vec::new())
            }
        };
        session
            .recap_and_mark_finalized_with_background_cursor(
                metadata.message_count,
                background_process_completions.consumed_through_seq,
                Utc::now(),
            )
            .await?;
        report.advanced_recapped_until = true;
        report.finalized_unrecapped_messages = true;
        Ok(Some(report))
    }

    pub(super) async fn legacy_session_recap_background_process_completions(
        &self,
        session: &SessionHandle,
    ) -> SessionRecapBackgroundProcessProjection {
        let projection = replay_turn_journal(session.read_turn_journal().await);
        let mut items = projection
            .turns
            .into_iter()
            .filter(|turn| {
                matches!(
                    turn.status,
                    Some(crate::session::TurnJournalStatus::Committed)
                ) && turn.canonical_user_content_hash.is_some()
            })
            .flat_map(|turn| {
                let turn_id = bounded_completion_id(&turn.turn_id);
                turn.tool_calls.into_iter().filter_map(move |tool| {
                    tool.background_completion.map(|completion| {
                        SessionRecapBackgroundProcessCompletion {
                            turn_id: turn_id.clone(),
                            tool_use_id: bounded_completion_id(&tool.tool_use_id),
                            process_id: bounded_completion_id(&completion.process_id),
                            status: bounded_completion_id(&completion.status),
                            exit_code: completion.exit_code,
                            signal: completion.signal,
                            success: completion.success,
                        }
                    })
                })
            })
            .collect::<Vec<_>>();
        let omitted_older_count = items
            .len()
            .saturating_sub(FINALIZE_BACKGROUND_COMPLETION_MAX_ITEMS);
        if omitted_older_count > 0 {
            items.drain(..omitted_older_count);
        }
        SessionRecapBackgroundProcessProjection {
            consumed_through_seq: 0,
            omitted_older_count,
            items,
        }
    }

    pub(super) async fn session_recap_background_process_completions(
        &self,
        session: &SessionHandle,
    ) -> anyhow::Result<SessionRecapBackgroundProcessProjection> {
        let read = session.read_turn_journal().await;
        for warning in &read.warnings {
            log::warn!(
                target: "agent",
                "finalize background completion journal 读取降级 session={} line={:?}: {}",
                session.metadata.id,
                warning.line,
                warning.message
            );
        }
        let consumed_cursor = session
            .read_metadata()
            .await?
            .recap_background_completion_until_seq
            .unwrap_or(0);
        let mut consumed_through_seq = consumed_cursor;
        let mut items = Vec::new();
        for event in read.events {
            if event.seq <= consumed_cursor {
                continue;
            }
            let crate::session::TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id,
                process_id,
                status,
                exit_code,
                signal,
                success,
                ..
            } = event.kind
            else {
                continue;
            };
            consumed_through_seq = consumed_through_seq.max(event.seq);
            items.push(SessionRecapBackgroundProcessCompletion {
                turn_id: bounded_completion_id(&event.turn_id),
                tool_use_id: bounded_completion_id(&tool_use_id),
                process_id: bounded_completion_id(&process_id),
                status: bounded_completion_id(&status),
                exit_code,
                signal,
                success,
            });
        }
        let omitted_older_count = items
            .len()
            .saturating_sub(FINALIZE_BACKGROUND_COMPLETION_MAX_ITEMS);
        if omitted_older_count > 0 {
            items.drain(..omitted_older_count);
        }
        Ok(SessionRecapBackgroundProcessProjection {
            consumed_through_seq,
            omitted_older_count,
            items,
        })
    }

    pub(super) async fn prepare_finalize_segment(
        &self,
        session_messages: &[SessionMessage],
        fallback_scope: crate::api::ProviderRuntimeFallbackScope,
    ) -> anyhow::Result<(Vec<ClaimId>, Vec<Claim>, Vec<Dispute>)> {
        let background_process_completions = SessionRecapBackgroundProcessProjection {
            consumed_through_seq: 0,
            omitted_older_count: 0,
            items: Vec::new(),
        };
        self.prepare_finalize_segment_with_background(
            session_messages,
            &background_process_completions,
            fallback_scope,
        )
        .await
    }

    async fn prepare_finalize_segment_with_background(
        &self,
        session_messages: &[SessionMessage],
        background_process_completions: &SessionRecapBackgroundProcessProjection,
        fallback_scope: crate::api::ProviderRuntimeFallbackScope,
    ) -> anyhow::Result<(Vec<ClaimId>, Vec<Claim>, Vec<Dispute>)> {
        let transcript = session_messages_to_turn_transcript(session_messages);
        if transcript.is_empty() && background_process_completions.items.is_empty() {
            log::debug!(
                target: "agent",
                "agent {} recap/finalize 跳过空有效输入",
                self.agent.agent_id
            );
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let local_claims = llm_visible_claims(self.agent.claim_store.list_local_claims().await?);
        let local_by_id: FxHashMap<ClaimId, Claim> = local_claims
            .iter()
            .map(|claim| (claim.id.clone(), claim.clone()))
            .collect();
        let allowed = allowed_claim_ids_for_recap(&local_claims, &transcript);
        let payload = SessionRecapPayload {
            instruction: RECAP_INSTRUCTION,
            transcript: &transcript,
            background_process_completions: (!background_process_completions.items.is_empty())
                .then_some(background_process_completions),
            local_claims: &local_claims,
        };
        let system_prompt = self
            .prompt_registry
            .render(PROMPT_SESSION_RECAP, ())
            .context("渲染 session_recap prompt 失败")?;
        let user_text = serde_json::to_string_pretty(&payload)?;
        let agent_id = self.agent.agent_id.clone();
        self.json_caller
            .generate_json_streaming_validated_with_retry_notice(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
                crate::api::BufferedProviderRuntime::new(fallback_scope),
                |raw| prepare_recap_value(raw, &agent_id, &allowed, &local_by_id, Utc::now()),
                |retry_index, retry_total, error| {
                    log::warn!(
                        target: "agent",
                        "agent {agent_id} finalize_session 输出无效，重试 ({retry_index}/{retry_total}): {error:#}"
                    );
                },
            )
            .await
            .map_err(|source| RecoverableCompactionPreparationError::other(source).into())
    }

    async fn finalize_message_segment_checkpointed(
        &self,
        session: &SessionHandle,
        all_messages: &[SessionMessage],
        recap_start_index: usize,
        recap_end_index: usize,
        background_process_completions: &SessionRecapBackgroundProcessProjection,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let segment = all_messages
            .get(recap_start_index..recap_end_index)
            .with_context(|| {
                format!("session finalize 范围越界: [{recap_start_index}, {recap_end_index})")
            })?;
        let segment_hash = hash_finalize_recap_input(segment, background_process_completions)?;
        if let Some(checkpoint) = session.read_finalize_checkpoint().await? {
            let same_range = checkpoint.recap_start_index == recap_start_index
                && checkpoint.recap_end_index == recap_end_index;
            if same_range && checkpoint.recap_segment_hash == segment_hash {
                return match checkpoint.status {
                    FinalizeCheckpointStatus::Prepared => {
                        self.apply_finalize_checkpoint(session, checkpoint).await
                    }
                    FinalizeCheckpointStatus::Applied => {
                        Ok(report_from_finalize_checkpoint(&checkpoint, Vec::new()))
                    }
                };
            }
            if same_range && checkpoint.status == FinalizeCheckpointStatus::Prepared {
                validate_finalize_checkpoint_segment(&checkpoint, &segment_hash)?;
            }
            if same_range {
                log::info!(
                    target: "agent",
                    "忽略上一代已应用的 finalize checkpoint，生成新的 recap 代次: session={} range=[{}, {})",
                    session.metadata.id,
                    recap_start_index,
                    recap_end_index
                );
            }
        }

        let (used_claim_ids, prepared_claims, prepared_disputes) = self
            .prepare_finalize_segment_with_background(
                segment,
                background_process_completions,
                session.runtime_fallback_scope(),
            )
            .await?;
        let trace_text = finalize_trace_text(segment, background_process_completions)?;
        let trace_created_at = Utc::now();
        let trace_id = checkpoint_trace_id(
            &trace_text,
            &used_claim_ids,
            &prepared_claims,
            trace_created_at,
        );
        let checkpoint = FinalizeCheckpoint {
            recap_start_index,
            recap_end_index,
            recap_segment_hash: segment_hash,
            prepared_claims,
            prepared_disputes,
            used_claim_ids,
            trace_text,
            trace_created_at,
            trace_id,
            status: FinalizeCheckpointStatus::Prepared,
        };
        session.write_finalize_checkpoint(&checkpoint).await?;
        self.apply_finalize_checkpoint(session, checkpoint).await
    }

    async fn apply_finalize_checkpoint(
        &self,
        session: &SessionHandle,
        checkpoint: FinalizeCheckpoint,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let report = self
            .apply_prepared_finalize_batch(
                FinalizeTraceInput::Frozen(&checkpoint.trace_text),
                checkpoint.used_claim_ids.clone(),
                checkpoint.prepared_claims.clone(),
                checkpoint.prepared_disputes.clone(),
                checkpoint.trace_created_at,
                checkpoint.trace_id.clone(),
            )
            .await?;
        let applied_checkpoint = FinalizeCheckpoint {
            status: FinalizeCheckpointStatus::Applied,
            ..checkpoint
        };
        session
            .write_finalize_checkpoint(&applied_checkpoint)
            .await?;
        Ok(report)
    }

    pub(super) async fn apply_prepared_finalize_batch(
        &self,
        trace_input: FinalizeTraceInput<'_>,
        used_claim_ids: Vec<ClaimId>,
        prepared_claims: Vec<Claim>,
        prepared_disputes: Vec<Dispute>,
        trace_created_at: DateTime<Utc>,
        checkpoint_trace_id: Option<TraceId>,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let mut new_claim_ids = Vec::with_capacity(prepared_claims.len());
        let mut updated_claim_ids = Vec::new();
        let mut output_claim_ids = Vec::with_capacity(prepared_claims.len());
        let mut claims_to_upload = Vec::with_capacity(prepared_claims.len());
        for claim in prepared_claims {
            self.agent.claim_store.write_claim(&claim).await?;
            if claim.updated_at.is_some() {
                updated_claim_ids.push(claim.id.clone());
            } else {
                new_claim_ids.push(claim.id.clone());
            }
            output_claim_ids.push(claim.id.clone());
            claims_to_upload.push(claim);
        }

        let trace_text = match trace_input {
            FinalizeTraceInput::Messages(messages) => session_trace_text(messages),
            FinalizeTraceInput::Frozen(text) => text.to_string(),
        };
        let trace_id = if !output_claim_ids.is_empty() || !used_claim_ids.is_empty() {
            let trace_name = trace_name_from_task(&trace_text);
            let input_claims = used_claim_ids
                .iter()
                .cloned()
                .map(SourceId::Claim)
                .collect();
            match checkpoint_trace_id {
                Some(id) => Some(
                    self.runner
                        .write_trace_with_id(
                            id,
                            trace_name,
                            trace_text,
                            input_claims,
                            output_claim_ids.clone(),
                            trace_created_at,
                        )
                        .await?,
                ),
                None => Some(
                    self.runner
                        .write_trace(
                            trace_name,
                            trace_text,
                            input_claims,
                            output_claim_ids.clone(),
                            trace_created_at,
                        )
                        .await?,
                ),
            }
        } else {
            None
        };

        let mut disputes_to_upload = Vec::new();
        if self.runner.team_services_configured() {
            disputes_to_upload.reserve(prepared_disputes.len());
            for dispute in prepared_disputes {
                if !self.runner.dispute_claim_set_reported(&dispute).await? {
                    disputes_to_upload.push(dispute);
                }
            }
        }
        let upload_report = self
            .runner
            .upload_maintainer_batch(claims_to_upload, disputes_to_upload.clone())
            .await?;
        let warnings = upload_report.warning.into_iter().collect();
        let mut new_dispute_ids = Vec::with_capacity(disputes_to_upload.len());
        for dispute in disputes_to_upload {
            if self.runner.record_dispute_if_new(&dispute).await? {
                new_dispute_ids.push(dispute.id.clone());
            }
        }

        Ok(SessionFinalizeReport {
            trace_id,
            new_claim_ids,
            updated_claim_ids,
            used_claim_ids,
            new_dispute_ids,
            advanced_recapped_until: false,
            finalized_unrecapped_messages: false,
            warnings,
        })
    }
}
