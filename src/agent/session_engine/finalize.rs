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
    FinalizeCheckpoint, FinalizeCheckpointStatus, SessionHandle, SessionMessage, SessionStatus,
};
use crate::storage::FileLockGuard;

use super::compaction_projection::validate_session_compaction_state;
use super::events::emit_warnings;
use super::transcript::{session_messages_to_turn_transcript, session_trace_text};
use super::{
    checkpoint_trace_id, hash_session_segment, merge_finalize_reports,
    report_from_finalize_checkpoint, validate_finalize_checkpoint_segment, SessionEngine,
    SessionEvent, SessionFinalizeReport, SessionRecapPayload, SessionRuntimeStatus,
    PROMPT_SESSION_RECAP, RECAP_INSTRUCTION,
};

impl SessionEngine {
    pub async fn mark_session_finalizing(&self, session: &mut SessionHandle) -> anyhow::Result<()> {
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
        self.cleanup_processes_for_session(&session.metadata.id)
            .await;
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
        let mut session = self.load_existing_session(session_id).await?;
        let metadata = session.read_metadata().await?;
        if metadata.finalized_at.is_some() {
            return Ok(SessionFinalizeReport::default());
        }
        if metadata.status == SessionStatus::Open {
            self.mark_session_finalizing(&mut session).await?;
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
            self.mark_session_finalizing(session).await?;
        } else {
            // 旧的 finalizing session 可能来自一次中断/失败的 finalize；它不会再次经过
            // mark_session_finalizing，仍要保证 retry 时收回 root-session 进程。
            self.cleanup_processes_for_session(&session.metadata.id)
                .await;
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
        let metadata = session.read_metadata().await?;
        if metadata.message_count == 0 {
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
        if recapped_until == metadata.message_count {
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
            )
            .await?;
        report = merge_finalize_reports(recovered_report, report);
        session
            .recap_and_mark_finalized(metadata.message_count, Utc::now())
            .await?;
        report.advanced_recapped_until = true;
        report.finalized_unrecapped_messages = true;
        Ok(std::mem::take(&mut report))
    }
    pub(super) async fn prepare_finalize_segment(
        &self,
        session_messages: &[SessionMessage],
    ) -> anyhow::Result<(Vec<ClaimId>, Vec<Claim>, Vec<Dispute>)> {
        let transcript = session_messages_to_turn_transcript(session_messages);
        let local_claims = llm_visible_claims(self.agent.claim_store.list_local_claims().await?);
        let local_by_id: FxHashMap<ClaimId, Claim> = local_claims
            .iter()
            .map(|claim| (claim.id.clone(), claim.clone()))
            .collect();
        let allowed = allowed_claim_ids_for_recap(&local_claims, &transcript);
        let payload = SessionRecapPayload {
            instruction: RECAP_INSTRUCTION,
            transcript: &transcript,
            local_claims: &local_claims,
        };
        let mut last_err = None;
        for attempt in 0..=self.runner.llm_retry_count {
            let raw = match self.generate_recap_json(&payload).await {
                Ok(raw) => raw,
                Err(e) if attempt < self.runner.llm_retry_count => {
                    log::warn!(
                        target: "agent",
                        "agent {} finalize_session JSON 生成失败，重试 ({}/{}): {e:#}",
                        self.agent.agent_id,
                        attempt + 1,
                        self.runner.llm_retry_count
                    );
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            };
            match prepare_recap_value(
                raw,
                &self.agent.agent_id,
                &allowed,
                &local_by_id,
                Utc::now(),
            ) {
                Ok(prepared) => return Ok(prepared),
                Err(e) if attempt < self.runner.llm_retry_count => {
                    log::warn!(
                        target: "agent",
                        "agent {} finalize_session 输出未通过协议校验，重试 ({}/{}): {e:#}",
                        self.agent.agent_id,
                        attempt + 1,
                        self.runner.llm_retry_count
                    );
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("finalize_session retry loop 未返回结果")))
    }

    async fn generate_recap_json(
        &self,
        payload: &SessionRecapPayload<'_>,
    ) -> anyhow::Result<serde_json::Value> {
        let system_prompt = self
            .prompt_registry
            .render(PROMPT_SESSION_RECAP, ())
            .context("渲染 session_recap prompt 失败")?;
        let user_text = serde_json::to_string_pretty(payload)?;
        self.json_caller
            .generate_json(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
            )
            .await
    }

    async fn finalize_message_segment_checkpointed(
        &self,
        session: &SessionHandle,
        all_messages: &[SessionMessage],
        recap_start_index: usize,
        recap_end_index: usize,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let segment = all_messages
            .get(recap_start_index..recap_end_index)
            .with_context(|| {
                format!("session finalize 范围越界: [{recap_start_index}, {recap_end_index})")
            })?;
        let segment_hash = hash_session_segment(segment)?;
        match session.read_finalize_checkpoint().await? {
            Some(checkpoint)
                if checkpoint.recap_start_index == recap_start_index
                    && checkpoint.recap_end_index == recap_end_index =>
            {
                validate_finalize_checkpoint_segment(&checkpoint, &segment_hash)?;
                match checkpoint.status {
                    FinalizeCheckpointStatus::Prepared => {
                        self.apply_finalize_checkpoint(session, checkpoint, segment)
                            .await
                    }
                    FinalizeCheckpointStatus::Applied => {
                        Ok(report_from_finalize_checkpoint(&checkpoint, Vec::new()))
                    }
                }
            }
            _ => {
                let (used_claim_ids, prepared_claims, prepared_disputes) =
                    self.prepare_finalize_segment(segment).await?;
                let trace_text = session_trace_text(segment);
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
                self.apply_finalize_checkpoint(session, checkpoint, segment)
                    .await
            }
        }
    }

    async fn apply_finalize_checkpoint(
        &self,
        session: &SessionHandle,
        checkpoint: FinalizeCheckpoint,
        session_messages: &[SessionMessage],
    ) -> anyhow::Result<SessionFinalizeReport> {
        let report = self
            .apply_prepared_finalize_batch(
                session_messages,
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
        session_messages: &[SessionMessage],
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

        let trace_text = session_trace_text(session_messages);
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
