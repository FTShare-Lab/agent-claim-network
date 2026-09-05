//! SessionEngine session finalize / recap 流程。
//!
//! 本模块维护 finalize 状态转换、recap JSON 生成、finalize checkpoint
//! 与 prepared claim/dispute/trace 的落库上传。它保持原 SessionEngine public
//! 方法签名不变，供 TUI 和 supervisor 继续调用。

use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use rustc_hash::FxHashMap;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::agent::claims::claim_revision;
use crate::agent::prepare::{allowed_claim_ids_for_recap, llm_visible_claims};
use crate::agent::runner_finalize::prepare_recap_value;
use crate::agent::runner_trace::trace_name_from_task;
use crate::api::SessionTurnMessage;
use crate::claim::{Claim, ClaimId, Dispute, SessionId, SourceId};
use crate::session::{
    finalize_checkpoint_covers_pending_range, replay_turn_journal, FinalizeCheckpoint,
    FinalizeCheckpointStatus, FinalizeClaimRevision, SessionHandle, SessionMessage, SessionStatus,
};
use crate::storage::{paths, FileLockGuard};

use super::compaction_projection::validate_session_compaction_state;
use super::events::emit_warnings;
use super::transcript::{session_messages_to_turn_transcript_with_memory_mode, session_trace_text};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecapRetryMode {
    Configured,
    SingleAttempt,
}

#[derive(Clone, Copy)]
struct FinalizeSegmentExecution<'a> {
    retry_mode: RecapRetryMode,
    preemption: Option<&'a SessionRecapPreemptionControl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionRecapPreemptionPhase {
    Cancelable,
    CancelRequested,
    Prepared,
    FinishedBeforePrepared,
    FinishedAfterPrepared,
}

/// Supervisor Running Recap 的 Prepared 前抢占边界。
///
/// `phase` 锁会跨越 Prepared checkpoint 的原子写；Finalize enqueue 与该写入
/// 只能有一方先完成判定，避免“已经 Prepared 但仍被取消”的竞态。
pub(crate) struct SessionPreparedPreemptionControl {
    cancel: CancellationToken,
    phase: Mutex<SessionRecapPreemptionPhase>,
}

pub(crate) type SessionRecapPreemptionControl = SessionPreparedPreemptionControl;
pub(crate) type SessionFinalizePreemptionControl = SessionPreparedPreemptionControl;

impl SessionPreparedPreemptionControl {
    pub(crate) fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            phase: Mutex::new(SessionRecapPreemptionPhase::Cancelable),
        }
    }

    pub(crate) async fn request_before_prepared(&self) -> bool {
        let mut phase = self.phase.lock().await;
        if *phase != SessionRecapPreemptionPhase::Cancelable {
            return false;
        }
        *phase = SessionRecapPreemptionPhase::CancelRequested;
        self.cancel.cancel();
        true
    }

    #[cfg(test)]
    pub(crate) async fn was_preempted_before_prepared(&self) -> bool {
        *self.phase.lock().await == SessionRecapPreemptionPhase::CancelRequested
    }

    pub(crate) async fn finish(&self) -> bool {
        let mut phase = self.phase.lock().await;
        let was_preempted = *phase == SessionRecapPreemptionPhase::CancelRequested;
        *phase = match *phase {
            SessionRecapPreemptionPhase::Prepared
            | SessionRecapPreemptionPhase::FinishedAfterPrepared => {
                SessionRecapPreemptionPhase::FinishedAfterPrepared
            }
            SessionRecapPreemptionPhase::Cancelable
            | SessionRecapPreemptionPhase::CancelRequested
            | SessionRecapPreemptionPhase::FinishedBeforePrepared => {
                SessionRecapPreemptionPhase::FinishedBeforePrepared
            }
        };
        was_preempted
    }

    pub(crate) async fn finished_before_prepared(&self) -> bool {
        *self.phase.lock().await == SessionRecapPreemptionPhase::FinishedBeforePrepared
    }

    async fn mark_existing_checkpoint_prepared(&self) {
        let mut phase = self.phase.lock().await;
        // checkpoint 已经是持久副作用。即使取消请求先到、当前 worker 随后才取得
        // finalize.lock 并观察到它，也必须让 Prepared 边界获胜。
        *phase = SessionRecapPreemptionPhase::Prepared;
    }

    async fn commit_prepared<F>(&self, commit: F) -> anyhow::Result<bool>
    where
        F: Future<Output = anyhow::Result<()>>,
    {
        let mut phase = self.phase.lock().await;
        if *phase == SessionRecapPreemptionPhase::CancelRequested {
            return Ok(false);
        }
        commit.await?;
        *phase = SessionRecapPreemptionPhase::Prepared;
        Ok(true)
    }

    async fn is_cancel_requested(&self) -> bool {
        *self.phase.lock().await == SessionRecapPreemptionPhase::CancelRequested
    }

    async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
}

#[derive(Debug, thiserror::Error)]
#[error("session recap was preempted by same-session finalize before Prepared")]
struct SessionRecapPreemptedBeforePrepared;

#[derive(Debug)]
pub(crate) enum SessionFinalizeOnceOutcome {
    Completed(SessionFinalizeReport),
    PreemptedBeforePrepared,
}

struct PendingFinalizeLocalApply {
    report: SessionFinalizeReport,
    claims: Vec<Claim>,
    disputes: Vec<Dispute>,
}

struct PendingFinalizeUpload {
    local: PendingFinalizeLocalApply,
    applied_checkpoint: FinalizeCheckpoint,
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
        match self
            .finalize_existing_session_with_retry_mode(
                session_id,
                emit,
                RecapRetryMode::Configured,
                None,
            )
            .await?
        {
            SessionFinalizeOnceOutcome::Completed(report) => Ok(report),
            SessionFinalizeOnceOutcome::PreemptedBeforePrepared => {
                anyhow::bail!("foreground finalize cannot be preempted")
            }
        }
    }

    pub(crate) async fn finalize_existing_session_once<F>(
        &self,
        session_id: &SessionId,
        emit: F,
    ) -> anyhow::Result<SessionFinalizeReport>
    where
        F: FnMut(SessionEvent),
    {
        match self
            .finalize_existing_session_with_retry_mode(
                session_id,
                emit,
                RecapRetryMode::SingleAttempt,
                None,
            )
            .await?
        {
            SessionFinalizeOnceOutcome::Completed(report) => Ok(report),
            SessionFinalizeOnceOutcome::PreemptedBeforePrepared => {
                anyhow::bail!("finalize without preemption control was preempted")
            }
        }
    }

    pub(crate) async fn finalize_existing_session_once_with_preemption<F>(
        &self,
        session_id: &SessionId,
        emit: F,
        preemption: Arc<SessionFinalizePreemptionControl>,
    ) -> anyhow::Result<SessionFinalizeOnceOutcome>
    where
        F: FnMut(SessionEvent),
    {
        self.finalize_existing_session_with_retry_mode(
            session_id,
            emit,
            RecapRetryMode::SingleAttempt,
            Some(preemption.as_ref()),
        )
        .await
    }

    /// 将 Open session 的 canonical message recap 到冻结 target；不改变 session 生命周期。
    #[cfg(test)]
    pub(crate) async fn recap_existing_session_until(
        &self,
        session_id: &SessionId,
        recap_end_index: usize,
    ) -> anyhow::Result<SessionFinalizeReport> {
        self.recap_existing_session_until_inner(session_id, recap_end_index, None)
            .await
    }

    pub(crate) async fn recap_existing_session_until_with_preemption(
        &self,
        session_id: &SessionId,
        recap_end_index: usize,
        preemption: Arc<SessionRecapPreemptionControl>,
    ) -> anyhow::Result<SessionFinalizeReport> {
        self.recap_existing_session_until_inner(
            session_id,
            recap_end_index,
            Some(preemption.as_ref()),
        )
        .await
    }

    async fn recap_existing_session_until_inner(
        &self,
        session_id: &SessionId,
        recap_end_index: usize,
        preemption: Option<&SessionRecapPreemptionControl>,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let mut session = self.load_existing_session(session_id).await?;
        let recap_guard = FileLockGuard::lock_exclusive(&session.paths.finalize_lock);
        let _recap_guard = match preemption {
            Some(preemption) => {
                tokio::select! {
                    biased;
                    _ = preemption.cancelled() => {
                        return Ok(SessionFinalizeReport::default());
                    }
                    guard = recap_guard => guard?,
                }
            }
            None => recap_guard.await?,
        };
        let metadata = session.read_metadata().await?;
        if metadata.status != SessionStatus::Open
            || metadata.finalized_at.is_some()
            || metadata.closed_at.is_some()
        {
            return Ok(SessionFinalizeReport::default());
        }
        if recap_end_index > metadata.message_count {
            anyhow::bail!(
                "session {session_id} recap target {recap_end_index} 超过 message_count {}",
                metadata.message_count
            );
        }
        if metadata.recapped_until >= recap_end_index {
            return Ok(SessionFinalizeReport::default());
        }

        let messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, messages.len())?;
        let background_process_completions = SessionRecapBackgroundProcessProjection {
            consumed_through_seq: 0,
            omitted_older_count: 0,
            items: Vec::new(),
        };
        let mut report = match self
            .finalize_message_segment_checkpointed(
                &session,
                &messages,
                metadata.recapped_until,
                recap_end_index,
                &background_process_completions,
                FinalizeSegmentExecution {
                    retry_mode: RecapRetryMode::SingleAttempt,
                    preemption,
                },
            )
            .await
        {
            Ok(report) => report,
            Err(error)
                if error
                    .downcast_ref::<SessionRecapPreemptedBeforePrepared>()
                    .is_some() =>
            {
                return Ok(SessionFinalizeReport::default());
            }
            Err(error) => return Err(error),
        };
        session.advance_recapped_until(recap_end_index).await?;
        report.advanced_recapped_until = true;
        Ok(report)
    }

    async fn finalize_existing_session_with_retry_mode<F>(
        &self,
        session_id: &SessionId,
        emit: F,
        retry_mode: RecapRetryMode,
        preemption: Option<&SessionFinalizePreemptionControl>,
    ) -> anyhow::Result<SessionFinalizeOnceOutcome>
    where
        F: FnMut(SessionEvent),
    {
        let mut emit = emit;
        let mut session = self.load_existing_session(session_id).await?;
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            return Ok(SessionFinalizeOnceOutcome::Completed(
                SessionFinalizeReport::default(),
            ));
        }
        if metadata.status == SessionStatus::Open {
            self.mark_session_finalizing(&mut session, &mut emit)
                .await?;
        }
        self.finalize_session_with_retry_mode(&mut session, emit, retry_mode, preemption)
            .await
    }

    pub async fn finalize_session<F>(
        &self,
        session: &mut SessionHandle,
        emit: F,
    ) -> anyhow::Result<SessionFinalizeReport>
    where
        F: FnMut(SessionEvent),
    {
        match self
            .finalize_session_with_retry_mode(session, emit, RecapRetryMode::Configured, None)
            .await?
        {
            SessionFinalizeOnceOutcome::Completed(report) => Ok(report),
            SessionFinalizeOnceOutcome::PreemptedBeforePrepared => {
                anyhow::bail!("foreground finalize cannot be preempted")
            }
        }
    }

    async fn finalize_session_with_retry_mode<F>(
        &self,
        session: &mut SessionHandle,
        mut emit: F,
        retry_mode: RecapRetryMode,
        preemption: Option<&SessionFinalizePreemptionControl>,
    ) -> anyhow::Result<SessionFinalizeOnceOutcome>
    where
        F: FnMut(SessionEvent),
    {
        let finalize_guard = FileLockGuard::lock_exclusive(&session.paths.finalize_lock);
        let _finalize_guard = match preemption {
            Some(preemption) => {
                tokio::select! {
                    biased;
                    _ = preemption.cancelled() => {
                        return Ok(SessionFinalizeOnceOutcome::PreemptedBeforePrepared);
                    }
                    guard = finalize_guard => guard?,
                }
            }
            None => finalize_guard.await?,
        };
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            return Ok(SessionFinalizeOnceOutcome::Completed(
                SessionFinalizeReport::default(),
            ));
        }
        if let Some(preemption) = preemption {
            let current_checkpoint =
                session
                    .read_finalize_checkpoint()
                    .await?
                    .is_some_and(|checkpoint| {
                        finalize_checkpoint_covers_pending_range(
                            &checkpoint,
                            metadata.recapped_until,
                            metadata.message_count,
                        )
                    });
            if current_checkpoint {
                preemption.mark_existing_checkpoint_prepared().await;
            } else if preemption.is_cancel_requested().await {
                return Ok(SessionFinalizeOnceOutcome::PreemptedBeforePrepared);
            }
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
        let result = self
            .finalize_session_inner_with_retry_mode(session, retry_mode, preemption)
            .await;
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
                Ok(SessionFinalizeOnceOutcome::Completed(report))
            }
            Err(error)
                if error
                    .downcast_ref::<SessionRecapPreemptedBeforePrepared>()
                    .is_some() =>
            {
                self.append_session_event_log(
                    session,
                    "INFO",
                    "Finalize preempted by resume before Prepared",
                )
                .await;
                Ok(SessionFinalizeOnceOutcome::PreemptedBeforePrepared)
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

    async fn finalize_session_inner_with_retry_mode(
        &self,
        session: &mut SessionHandle,
        retry_mode: RecapRetryMode,
        preemption: Option<&SessionFinalizePreemptionControl>,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            anyhow::bail!("session {} 已 finalize，不能重复 finalize", metadata.id);
        }
        if metadata.status == SessionStatus::Closed || metadata.closed_at.is_some() {
            anyhow::bail!("session {} 已关闭，不能重复关闭", metadata.id);
        }
        if let Some(preemption) = preemption {
            if preemption.is_cancel_requested().await {
                return Err(SessionRecapPreemptedBeforePrepared.into());
            }
        }
        let started_with_unrecapped_messages = metadata.message_count > metadata.recapped_until;
        let mut recovered_report = SessionFinalizeReport::default();
        if let Some(outcome) = self
            .recover_matching_compaction_checkpoint(session, None, None)
            .await?
        {
            self.append_compaction_audit_completed(
                session,
                &outcome.audit_ids,
                &outcome,
                outcome.recovered,
            )
            .await;
            let metadata = session.read_metadata().await?;
            if metadata.finalized_at.is_some() {
                anyhow::bail!("session {} 已 finalize，不能重复 finalize", metadata.id);
            }
            if metadata.status == SessionStatus::Closed || metadata.closed_at.is_some() {
                anyhow::bail!("session {} 已关闭，不能重复关闭", metadata.id);
            }
        }
        if let Some(report) = self
            .recover_current_recap_checkpoint_prefix(session)
            .await?
        {
            recovered_report = merge_finalize_reports(recovered_report, report);
        }
        if let Some(report) = self.recover_legacy_finalize_checkpoint(session).await? {
            return Ok(merge_finalize_reports(recovered_report, report));
        }
        let metadata = session.read_metadata().await?;
        let background_process_completions = self
            .session_recap_background_process_completions(session)
            .await?;
        if metadata.message_count == 0 && background_process_completions.items.is_empty() {
            let finalized_at = Utc::now();
            let committed = match preemption {
                Some(preemption) => {
                    preemption
                        .commit_prepared(async {
                            session.mark_finalized(finalized_at).await?;
                            Ok(())
                        })
                        .await?
                }
                None => {
                    session.mark_finalized(finalized_at).await?;
                    true
                }
            };
            if !committed {
                return Err(SessionRecapPreemptedBeforePrepared.into());
            }
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
            let finalized_at = Utc::now();
            let committed = match preemption {
                Some(preemption) => {
                    preemption
                        .commit_prepared(async {
                            session.mark_finalized(finalized_at).await?;
                            Ok(())
                        })
                        .await?
                }
                None => {
                    session.mark_finalized(finalized_at).await?;
                    true
                }
            };
            if !committed {
                return Err(SessionRecapPreemptedBeforePrepared.into());
            }
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
                FinalizeSegmentExecution {
                    retry_mode,
                    preemption,
                },
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

    /// Finalize 先兑现同 session Recap 已持久化的 checkpoint 前缀，再处理剩余消息与
    /// Finalize 专属 background completion。这样 Prepared 获胜后即使 Recap attempt
    /// 随后失败或进程退出，也不会被高优先级 Finalize 越过。
    async fn recover_current_recap_checkpoint_prefix(
        &self,
        session: &mut SessionHandle,
    ) -> anyhow::Result<Option<SessionFinalizeReport>> {
        let metadata = session.read_metadata().await?;
        // 缺失 cursor 表示 v0.2.3 legacy finalize checkpoint，继续交给原迁移路径处理。
        if metadata.recap_background_completion_until_seq.is_none() {
            return Ok(None);
        }
        let Some(checkpoint) = session.read_finalize_checkpoint().await? else {
            return Ok(None);
        };
        if !finalize_checkpoint_covers_pending_range(
            &checkpoint,
            metadata.recapped_until,
            metadata.message_count,
        ) {
            return Ok(None);
        }
        let messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, messages.len())?;
        let segment = messages
            .get(checkpoint.recap_start_index..checkpoint.recap_end_index)
            .with_context(|| {
                format!(
                    "共享 recap checkpoint 范围越界: [{}, {})",
                    checkpoint.recap_start_index, checkpoint.recap_end_index
                )
            })?;
        let recap_input = SessionRecapBackgroundProcessProjection {
            consumed_through_seq: 0,
            omitted_older_count: 0,
            items: Vec::new(),
        };
        let recap_hash = hash_finalize_recap_input(segment, &recap_input)?;
        if checkpoint.recap_segment_hash != recap_hash {
            return Ok(None);
        }

        let recap_end_index = checkpoint.recap_end_index;
        let mut report = match checkpoint.status {
            FinalizeCheckpointStatus::Prepared => {
                self.apply_finalize_checkpoint(session, checkpoint).await?
            }
            FinalizeCheckpointStatus::Applied => {
                self.finish_finalize_upload(report_from_finalize_checkpoint(
                    &checkpoint,
                    Vec::new(),
                ))
                .await?
            }
        };
        session.advance_recapped_until(recap_end_index).await?;
        report.advanced_recapped_until = true;
        log::info!(
            target: "agent",
            "Finalize 已恢复共享 recap checkpoint 前缀: session={} recapped_until={}",
            metadata.id,
            recap_end_index
        );
        Ok(Some(report))
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
                self.finish_finalize_upload(report_from_finalize_checkpoint(
                    &checkpoint,
                    Vec::new(),
                ))
                .await?
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

    async fn prepare_finalize_segment_with_background(
        &self,
        session_messages: &[SessionMessage],
        background_process_completions: &SessionRecapBackgroundProcessProjection,
        fallback_scope: crate::api::ProviderRuntimeFallbackScope,
        retry_mode: RecapRetryMode,
    ) -> anyhow::Result<(Vec<ClaimId>, Vec<Claim>, Vec<Dispute>)> {
        let memory_enabled = self.turn_loop.tool_registry().memory_enabled();
        let transcript =
            session_messages_to_turn_transcript_with_memory_mode(session_messages, memory_enabled);
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
            .render(
                PROMPT_SESSION_RECAP,
                serde_json::json!({
                    "memory_enabled": memory_enabled,
                }),
            )
            .context("渲染 session_recap prompt 失败")?;
        let user_text = serde_json::to_string_pretty(&payload)?;
        let agent_id = self.agent.agent_id.clone();
        let messages = vec![SessionTurnMessage::user_text(user_text)];
        let result = match retry_mode {
            RecapRetryMode::Configured => self
                .json_caller
                .generate_json_streaming_validated_with_retry_notice(
                    system_prompt,
                    messages,
                    crate::api::BufferedProviderRuntime::new(fallback_scope),
                    |raw| prepare_recap_value(raw, &agent_id, &allowed, &local_by_id, Utc::now()),
                    |retry_index, retry_total, error| {
                        log::warn!(
                            target: "agent",
                            "agent {agent_id} recap/finalize 输出无效，重试 ({retry_index}/{retry_total}): {error:#}"
                        );
                    },
                )
                .await,
            RecapRetryMode::SingleAttempt => self
                .json_caller
                .generate_json_validated_once(
                    system_prompt,
                    messages,
                    |raw| prepare_recap_value(raw, &agent_id, &allowed, &local_by_id, Utc::now()),
                )
                .await,
        };
        result.map_err(|source| RecoverableCompactionPreparationError::other(source).into())
    }

    async fn finalize_message_segment_checkpointed(
        &self,
        session: &SessionHandle,
        all_messages: &[SessionMessage],
        recap_start_index: usize,
        recap_end_index: usize,
        background_process_completions: &SessionRecapBackgroundProcessProjection,
        execution: FinalizeSegmentExecution<'_>,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let FinalizeSegmentExecution {
            retry_mode,
            preemption,
        } = execution;
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
                if let Some(preemption) = preemption {
                    preemption.mark_existing_checkpoint_prepared().await;
                }
                return match checkpoint.status {
                    FinalizeCheckpointStatus::Prepared => {
                        self.apply_finalize_checkpoint(session, checkpoint).await
                    }
                    FinalizeCheckpointStatus::Applied => {
                        self.finish_finalize_upload(report_from_finalize_checkpoint(
                            &checkpoint,
                            Vec::new(),
                        ))
                        .await
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

        let report = {
            let knowledge_guard =
                FileLockGuard::lock_exclusive(paths::agent_home_knowledge_apply_lock_path(
                    self.runner.maintainer_upload_queue.agent_home(),
                ));
            let _knowledge_guard = match preemption {
                Some(preemption) => {
                    tokio::select! {
                        biased;
                        _ = preemption.cancelled() => {
                            return Err(SessionRecapPreemptedBeforePrepared.into());
                        }
                        guard = knowledge_guard => guard?,
                    }
                }
                None => knowledge_guard.await?,
            };
            self.runner.recover_pending_claim_edit_locked().await?;
            let prepare = self.prepare_finalize_segment_with_background(
                segment,
                background_process_completions,
                session.runtime_fallback_scope(),
                retry_mode,
            );
            let prepared = match preemption {
                Some(preemption) => {
                    tokio::select! {
                        biased;
                        _ = preemption.cancelled() => {
                            return Err(SessionRecapPreemptedBeforePrepared.into());
                        }
                        prepared = prepare => prepared,
                    }
                }
                None => prepare.await,
            };
            let (used_claim_ids, prepared_claims, prepared_disputes) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    if let Some(preemption) = preemption {
                        if preemption.is_cancel_requested().await {
                            return Err(SessionRecapPreemptedBeforePrepared.into());
                        }
                    }
                    return Err(error);
                }
            };
            let trace_text = finalize_trace_text(segment, background_process_completions)?;
            let trace_created_at = Utc::now();
            let trace_id = checkpoint_trace_id(
                &trace_text,
                &used_claim_ids,
                &prepared_claims,
                trace_created_at,
            );
            let local_claims = self.agent.claim_store.list_local_claims().await?;
            let local_by_id = local_claims
                .into_iter()
                .map(|claim| (claim.id.clone(), claim))
                .collect::<FxHashMap<_, _>>();
            let expected_claim_revisions = prepared_claims
                .iter()
                .map(|claim| {
                    let preimage_hash = if claim.updated_at.is_some() {
                        Some(claim_revision(local_by_id.get(&claim.id).ok_or_else(
                            || {
                                anyhow::anyhow!(
                                    "finalize prepared update claim={} 不在本地输入中",
                                    claim.id
                                )
                            },
                        )?)?)
                    } else {
                        None
                    };
                    Ok(FinalizeClaimRevision {
                        claim_id: claim.id.clone(),
                        preimage_hash,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let checkpoint = FinalizeCheckpoint {
                recap_start_index,
                recap_end_index,
                recap_segment_hash: segment_hash,
                prepared_claims,
                expected_claim_revisions,
                prepared_disputes,
                used_claim_ids,
                trace_text,
                trace_created_at,
                trace_id,
                status: FinalizeCheckpointStatus::Prepared,
            };
            let committed = match preemption {
                Some(preemption) => {
                    preemption
                        .commit_prepared(async {
                            session.write_finalize_checkpoint(&checkpoint).await?;
                            Ok(())
                        })
                        .await?
                }
                None => {
                    session.write_finalize_checkpoint(&checkpoint).await?;
                    true
                }
            };
            if !committed {
                return Err(SessionRecapPreemptedBeforePrepared.into());
            }
            self.apply_finalize_checkpoint_local_and_commit(session, checkpoint)
                .await?
        };
        self.finish_finalize_upload(report).await
    }

    async fn apply_finalize_checkpoint(
        &self,
        session: &SessionHandle,
        checkpoint: FinalizeCheckpoint,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let report = {
            let _knowledge_guard =
                FileLockGuard::lock_exclusive(paths::agent_home_knowledge_apply_lock_path(
                    self.runner.maintainer_upload_queue.agent_home(),
                ))
                .await?;
            self.runner.recover_pending_claim_edit_locked().await?;
            self.apply_finalize_checkpoint_local_and_commit(session, checkpoint)
                .await?
        };
        self.finish_finalize_upload(report).await
    }

    async fn apply_finalize_checkpoint_local_and_commit(
        &self,
        session: &SessionHandle,
        checkpoint: FinalizeCheckpoint,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let mut pending = self
            .apply_finalize_checkpoint_local(&session.metadata.id, checkpoint)
            .await?;
        pending.applied_checkpoint.prepared_claims = pending.local.claims.clone();
        pending.applied_checkpoint.trace_id = pending.local.report.trace_id.clone();
        self.runner
            .stage_maintainer_batch(
                std::mem::take(&mut pending.local.claims),
                pending.local.disputes.clone(),
            )
            .await?;
        let mut new_dispute_ids = Vec::with_capacity(pending.local.disputes.len());
        for dispute in &pending.local.disputes {
            if self.runner.record_dispute_if_new(dispute).await? {
                new_dispute_ids.push(dispute.id.clone());
            }
        }
        pending.local.report.new_dispute_ids = new_dispute_ids;
        session
            .write_finalize_checkpoint(&pending.applied_checkpoint)
            .await?;
        Ok(pending.local.report)
    }

    async fn apply_finalize_checkpoint_local(
        &self,
        session_id: &SessionId,
        checkpoint: FinalizeCheckpoint,
    ) -> anyhow::Result<PendingFinalizeUpload> {
        let local = self
            .apply_prepared_finalize_batch_local(session_id, &checkpoint)
            .await?;
        Ok(PendingFinalizeUpload {
            local,
            applied_checkpoint: FinalizeCheckpoint {
                status: FinalizeCheckpointStatus::Applied,
                ..checkpoint
            },
        })
    }

    async fn apply_prepared_finalize_batch_local(
        &self,
        session_id: &SessionId,
        checkpoint: &FinalizeCheckpoint,
    ) -> anyhow::Result<PendingFinalizeLocalApply> {
        let FinalizeCheckpoint {
            trace_text,
            used_claim_ids,
            prepared_claims,
            expected_claim_revisions,
            prepared_disputes,
            trace_created_at,
            trace_id: mut checkpoint_trace_id,
            ..
        } = checkpoint.clone();
        let mut new_claim_ids = Vec::with_capacity(prepared_claims.len());
        let mut updated_claim_ids = Vec::new();
        let mut warnings = Vec::new();
        let expected_by_id = expected_claim_revisions
            .into_iter()
            .map(|revision| (revision.claim_id, revision.preimage_hash))
            .collect::<FxHashMap<_, _>>();
        let current_by_id = self
            .agent
            .claim_store
            .list_local_claims()
            .await?
            .into_iter()
            .map(|claim| (claim.id.clone(), claim))
            .collect::<FxHashMap<_, _>>();
        let mut output_claim_ids = Vec::with_capacity(prepared_claims.len());
        let mut claims_to_upload = Vec::with_capacity(prepared_claims.len());
        for claim in prepared_claims {
            let current = current_by_id.get(&claim.id);
            let should_apply = match expected_by_id.get(&claim.id) {
                None if current.is_some_and(|current| {
                    current
                        .updated_at
                        .is_some_and(|updated_at| updated_at > trace_created_at)
                }) =>
                {
                    false
                }
                None => true,
                Some(_) if current == Some(&claim) => false,
                Some(None) => current.is_none(),
                Some(Some(expected_hash)) => current
                    .map(claim_revision)
                    .transpose()?
                    .is_some_and(|current_hash| current_hash == *expected_hash),
            };
            let already_applied = current == Some(&claim);
            if !should_apply && !already_applied {
                let warning = format!(
                    "session={} claim={} 在 finalize checkpoint prepared 后已变更，旧更新已 superseded",
                    session_id, claim.id
                );
                log::warn!(target: "agent", "{warning}");
                warnings.push(warning);
                checkpoint_trace_id = None;
                continue;
            }
            if should_apply {
                self.agent.claim_store.write_claim(&claim).await?;
            }
            if claim.updated_at.is_some() {
                updated_claim_ids.push(claim.id.clone());
            } else {
                new_claim_ids.push(claim.id.clone());
            }
            output_claim_ids.push(claim.id.clone());
            claims_to_upload.push(claim);
        }

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
        Ok(PendingFinalizeLocalApply {
            report: SessionFinalizeReport {
                trace_id,
                new_claim_ids,
                updated_claim_ids,
                used_claim_ids,
                new_dispute_ids: Vec::new(),
                advanced_recapped_until: false,
                finalized_unrecapped_messages: false,
                warnings,
            },
            claims: claims_to_upload,
            disputes: disputes_to_upload,
        })
    }

    async fn finish_finalize_upload(
        &self,
        mut report: SessionFinalizeReport,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let upload_report = self
            .runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await?;
        report.warnings.extend(upload_report.warning);
        Ok(report)
    }
}

#[cfg(test)]
mod preemption_tests {
    use std::sync::Arc;

    use tokio::sync::oneshot;

    use super::SessionRecapPreemptionControl;

    #[tokio::test]
    async fn cancellation_before_prepared_prevents_checkpoint_commit() {
        let control = SessionRecapPreemptionControl::new();
        assert!(control.request_before_prepared().await);

        let committed = control
            .commit_prepared(async { panic!("cancelled recap must not commit Prepared") })
            .await
            .unwrap();

        assert!(!committed);
        assert!(control.was_preempted_before_prepared().await);
    }

    #[tokio::test]
    async fn prepared_commit_wins_over_later_cancellation_request() {
        let control = Arc::new(SessionRecapPreemptionControl::new());
        let (commit_started_tx, commit_started_rx) = oneshot::channel();
        let (allow_commit_tx, allow_commit_rx) = oneshot::channel();
        let commit_control = Arc::clone(&control);
        let commit = tokio::spawn(async move {
            commit_control
                .commit_prepared(async move {
                    let _ = commit_started_tx.send(());
                    allow_commit_rx.await.unwrap();
                    Ok(())
                })
                .await
        });
        commit_started_rx.await.unwrap();

        let cancel_control = Arc::clone(&control);
        let mut cancel =
            tokio::spawn(async move { cancel_control.request_before_prepared().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut cancel)
                .await
                .is_err()
        );

        let _ = allow_commit_tx.send(());
        assert!(commit.await.unwrap().unwrap());
        assert!(!cancel.await.unwrap());
        assert!(!control.was_preempted_before_prepared().await);
    }

    #[tokio::test]
    async fn observed_existing_checkpoint_wins_over_earlier_cancellation_request() {
        let control = SessionRecapPreemptionControl::new();
        assert!(control.request_before_prepared().await);

        control.mark_existing_checkpoint_prepared().await;

        assert!(!control.finish().await);
    }

    #[tokio::test]
    async fn completed_recap_no_longer_accepts_preemption() {
        let control = SessionRecapPreemptionControl::new();

        assert!(!control.finish().await);
        assert!(control.finished_before_prepared().await);
        assert!(!control.request_before_prepared().await);
    }
}
