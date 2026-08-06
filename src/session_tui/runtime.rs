//! TUI session worker runtime。
//!
//! 本模块负责把 start/turn/finalize 放进 tokio task，并用 turn id 过滤过期事件。
//! UI 状态机只接收 `WorkerEvent`，不直接持有底层 worker 的回调细节。

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agent::{
    SessionCompactionNoopReason, SessionCompactionResult, SessionEngine, SessionEvent,
    SessionTurnControl, SessionTurnControlReceiver,
};
use crate::claim::SessionId;
use crate::mcp::connection_manager::McpRuntimeState;

use super::input_queue::QueuedInput;

const TURN_JOURNAL_FALLBACK_WARNING: &str =
    "Turn journal 不完整，已合并可用 journal 与 canonical transcript 恢复历史。";
const UNALIGNED_JOURNAL_TURN_NOTICE: &str =
    "此 journal turn 无法与 canonical 历史唯一对齐；显示位置仅供降级恢复参考。";
const RESUME_HISTORY_REAL_USER_TURNS: usize = 10;

pub(super) enum WorkerEvent {
    Session {
        task_id: Option<u64>,
        event: SessionEvent,
    },
    StartFinished(anyhow::Result<crate::agent::SessionStartReport>),
    ResumeListLoaded {
        sessions: Vec<crate::session::ResumedSessionSummary>,
    },
    ResumeSessionLoaded {
        result: anyhow::Result<ResumeSessionOutcome>,
    },
    TurnFinished {
        turn_id: u64,
        result: anyhow::Result<crate::session::SessionHandle>,
    },
    UserShellCommandFinished {
        task_id: u64,
        result: anyhow::Result<crate::session::SessionHandle>,
    },
    CompactFinished {
        task_id: u64,
        result: anyhow::Result<CompactWorkerOutcome>,
    },
    InboxFinished {
        task_id: u64,
        result: anyhow::Result<InboxWorkerOutcome>,
    },
    FinalizeFinished {
        task_id: u64,
        result: anyhow::Result<()>,
    },
    FinalizeEnqueueFinished {
        task_id: u64,
        result: FinalizeEnqueueOutcome,
    },
    McpOperationFinished {
        server_name: String,
        operation_id: u64,
        outcome: McpOperationOutcome,
    },
}

pub(super) struct McpOperationOutcome {
    pub(super) snapshot: McpRuntimeState,
    pub(super) error: Option<String>,
}

pub(super) struct ResumeSessionOutcome {
    pub(super) session: crate::session::SessionHandle,
    pub(super) inbox_report: crate::agent::InboxProcessReport,
    pub(super) last_turns: Vec<crate::session::HistoricalTimelineTurn>,
    pub(super) turn_count: usize,
    pub(super) local_claim_count: Option<usize>,
    pub(super) context_used_tokens: Option<usize>,
    pub(super) journal_warning: Option<String>,
}

pub(super) enum CompactWorkerOutcome {
    Compacted(crate::session::SessionHandle),
    Noop {
        session: crate::session::SessionHandle,
        reason: SessionCompactionNoopReason,
    },
}

pub(super) struct InboxWorkerOutcome {
    pub(super) session: crate::session::SessionHandle,
    pub(super) report: crate::agent::InboxProcessReport,
}

pub(super) enum FinalizeEnqueueOutcome {
    Enqueued {
        job_id: String,
        session_id: crate::claim::SessionId,
    },
    Fallback {
        session: Box<crate::session::SessionHandle>,
        error: anyhow::Error,
    },
}

pub(super) struct ActiveTurn {
    pub(super) id: u64,
    pub(super) handle: JoinHandle<()>,
    phase: ActiveTurnPhase,
    pending_steers: Vec<PendingSteerInput>,
    pending_cancel: bool,
    control: SessionTurnControl,
}

pub(super) struct PendingSteerInput {
    pub(super) sequence: u64,
    pub(super) input: QueuedInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveTurnPhase {
    Running,
    Committed,
}

impl ActiveTurn {
    pub(super) fn new(id: u64, handle: JoinHandle<()>, control: SessionTurnControl) -> Self {
        Self {
            id,
            handle,
            phase: ActiveTurnPhase::Running,
            pending_steers: Vec::new(),
            pending_cancel: false,
            control,
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test(id: u64, handle: JoinHandle<()>) -> Self {
        Self {
            id,
            handle,
            phase: ActiveTurnPhase::Running,
            pending_steers: Vec::new(),
            pending_cancel: false,
            control: SessionTurnControl::acknowledged_for_test(),
        }
    }

    fn accepts_tool_boundary_interrupt(&self) -> bool {
        self.phase == ActiveTurnPhase::Running && !self.handle.is_finished()
    }

    async fn request_tool_boundary_steer(&mut self, sequence: u64, input: &QueuedInput) -> bool {
        if !self.accepts_tool_boundary_interrupt() || self.pending_cancel {
            return false;
        }
        let text = input.text().to_string();
        if !self.control.request_tool_boundary_steer(text.clone()).await {
            return false;
        }
        self.pending_steers.push(PendingSteerInput {
            sequence,
            input: input.clone(),
        });
        true
    }

    fn request_tool_boundary_cancel(&mut self, reason: String) -> bool {
        if !self.accepts_tool_boundary_interrupt() || self.pending_cancel {
            return false;
        }
        if !self.control.request_tool_boundary_cancel_now(reason) {
            return false;
        }
        self.pending_cancel = true;
        true
    }

    pub(super) fn take_pending_steer_input(&mut self) -> Option<QueuedInput> {
        if self.pending_steers.is_empty() {
            return None;
        }
        if self.pending_steers.len() == 1 {
            return self.pending_steers.pop().map(|pending| pending.input);
        }
        let text = self.pending_steer_preview_text().unwrap_or_default();
        self.pending_steers.clear();
        Some(QueuedInput::from_text(text))
    }

    pub(super) fn pending_steer_preview_text(&self) -> Option<String> {
        if self.pending_steers.is_empty() {
            return None;
        }
        Some(
            self.pending_steers
                .iter()
                .map(|pending| pending.input.text())
                .collect::<Vec<_>>()
                .join("\n\n"),
        )
    }

    pub(super) fn take_pending_steer_inputs_for_restore(&mut self) -> Vec<PendingSteerInput> {
        std::mem::take(&mut self.pending_steers)
    }

    pub(super) fn pending_cancel_requested(&self) -> bool {
        self.pending_cancel
    }
}

pub(super) enum ActiveSessionTask {
    Turn(Box<ActiveTurn>),
    UserShellCommand(ActiveShell),
    Compact(u64),
    Inbox(u64),
    Finalize(u64),
}

pub(super) struct ActiveShell {
    pub(super) id: u64,
    cancel: CancellationToken,
    cancelling: bool,
}

impl ActiveShell {
    fn new(id: u64, cancel: CancellationToken) -> Self {
        Self {
            id,
            cancel,
            cancelling: false,
        }
    }
}

impl ActiveSessionTask {
    fn as_turn_mut(&mut self) -> Option<&mut ActiveTurn> {
        match self {
            Self::Turn(turn) => Some(turn.as_mut()),
            Self::UserShellCommand(_) | Self::Compact(_) | Self::Inbox(_) | Self::Finalize(_) => {
                None
            }
        }
    }

    fn as_turn(&self) -> Option<&ActiveTurn> {
        match self {
            Self::Turn(turn) => Some(turn.as_ref()),
            Self::UserShellCommand(_) | Self::Compact(_) | Self::Inbox(_) | Self::Finalize(_) => {
                None
            }
        }
    }

    fn as_shell_mut(&mut self) -> Option<&mut ActiveShell> {
        match self {
            Self::UserShellCommand(shell) => Some(shell),
            Self::Turn(_) | Self::Compact(_) | Self::Inbox(_) | Self::Finalize(_) => None,
        }
    }
}

pub(super) struct SessionTaskState {
    pub(super) current: Option<ActiveSessionTask>,
    next_task_id: u64,
}

impl Default for SessionTaskState {
    fn default() -> Self {
        Self {
            current: None,
            next_task_id: 1,
        }
    }
}

impl SessionTaskState {
    fn allocate_task_id(&mut self) -> u64 {
        let task_id = self.next_task_id;
        self.next_task_id = self.next_task_id.saturating_add(1);
        task_id
    }

    pub(super) fn task_running(&self) -> bool {
        self.current.is_some()
    }

    pub(super) fn has_active_turn(&self) -> bool {
        matches!(self.current, Some(ActiveSessionTask::Turn(_)))
    }

    pub(super) fn can_request_tool_boundary_cancel(&self) -> bool {
        self.current
            .as_ref()
            .and_then(ActiveSessionTask::as_turn)
            .is_some_and(ActiveTurn::accepts_tool_boundary_interrupt)
    }

    pub(super) fn pending_cancel_requested(&self) -> bool {
        self.current
            .as_ref()
            .and_then(ActiveSessionTask::as_turn)
            .is_some_and(ActiveTurn::pending_cancel_requested)
    }

    pub(super) fn active_turn_id(&self) -> Option<u64> {
        self.current
            .as_ref()
            .and_then(ActiveSessionTask::as_turn)
            .map(|turn| turn.id)
    }

    pub(super) fn has_active_shell(&self) -> bool {
        matches!(self.current, Some(ActiveSessionTask::UserShellCommand(_)))
    }

    pub(super) fn current_turn_matches(&self, turn_id: u64) -> bool {
        matches!(self.current.as_ref().and_then(ActiveSessionTask::as_turn), Some(turn) if turn.id == turn_id)
    }

    pub(super) fn current_task_matches(&self, task_id: Option<u64>) -> bool {
        match (self.current.as_ref(), task_id) {
            (Some(ActiveSessionTask::Turn(turn)), Some(task_id)) => turn.id == task_id,
            (Some(ActiveSessionTask::UserShellCommand(shell)), Some(task_id)) => {
                shell.id == task_id
            }
            (Some(ActiveSessionTask::Compact(active_id)), Some(task_id)) => *active_id == task_id,
            (Some(ActiveSessionTask::Inbox(active_id)), Some(task_id)) => *active_id == task_id,
            (Some(ActiveSessionTask::Finalize(active_id)), Some(task_id)) => *active_id == task_id,
            (_, None) => true,
            _ => false,
        }
    }

    pub(super) fn mark_turn_committed(&mut self, turn_id: u64) {
        if let Some(turn) = self
            .current
            .as_mut()
            .and_then(ActiveSessionTask::as_turn_mut)
            .filter(|turn| turn.id == turn_id)
        {
            turn.phase = ActiveTurnPhase::Committed;
        }
    }

    pub(super) async fn request_tool_boundary_steer(
        &mut self,
        sequence: u64,
        input: &QueuedInput,
    ) -> bool {
        let Some(turn) = self
            .current
            .as_mut()
            .and_then(ActiveSessionTask::as_turn_mut)
        else {
            return false;
        };
        turn.request_tool_boundary_steer(sequence, input).await
    }

    pub(super) fn request_tool_boundary_cancel(&mut self, reason: &str) -> bool {
        let Some(turn) = self
            .current
            .as_mut()
            .and_then(ActiveSessionTask::as_turn_mut)
        else {
            return false;
        };
        turn.request_tool_boundary_cancel(reason.to_string())
    }

    pub(super) fn pending_steer_preview_text(&self) -> Option<String> {
        self.current
            .as_ref()
            .and_then(ActiveSessionTask::as_turn)
            .and_then(ActiveTurn::pending_steer_preview_text)
    }

    pub(super) fn cancel_active_shell(&mut self) -> bool {
        let Some(shell) = self
            .current
            .as_mut()
            .and_then(ActiveSessionTask::as_shell_mut)
        else {
            return false;
        };
        if shell.cancelling {
            return false;
        }
        shell.cancelling = true;
        shell.cancel.cancel();
        true
    }

    pub(super) fn finish_turn(&mut self, turn_id: u64) -> Option<ActiveTurn> {
        if !self.current_turn_matches(turn_id) {
            return None;
        }
        let Some(ActiveSessionTask::Turn(turn)) = self.current.take() else {
            return None;
        };
        Some(*turn)
    }

    pub(super) fn finish_compact(&mut self, task_id: u64) -> bool {
        if matches!(self.current, Some(ActiveSessionTask::Compact(active_id)) if active_id == task_id)
        {
            self.current = None;
            true
        } else {
            false
        }
    }

    pub(super) fn finish_inbox(&mut self, task_id: u64) -> bool {
        if matches!(self.current, Some(ActiveSessionTask::Inbox(active_id)) if active_id == task_id)
        {
            self.current = None;
            true
        } else {
            false
        }
    }

    pub(super) fn finish_shell(&mut self, task_id: u64) -> bool {
        if matches!(self.current, Some(ActiveSessionTask::UserShellCommand(ref shell)) if shell.id == task_id)
        {
            self.current = None;
            true
        } else {
            false
        }
    }

    pub(super) fn finalize_running(&self) -> bool {
        matches!(self.current, Some(ActiveSessionTask::Finalize(_)))
    }

    pub(super) fn finish_finalize(&mut self, task_id: u64) -> bool {
        if matches!(self.current, Some(ActiveSessionTask::Finalize(active_id)) if active_id == task_id)
        {
            self.current = None;
            true
        } else {
            false
        }
    }

    pub(super) fn spawn_tracked_turn(
        &mut self,
        engine: SessionEngine,
        session: crate::session::SessionHandle,
        input: QueuedInput,
        worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) {
        let turn_id = self.allocate_task_id();
        let (control, control_rx) = SessionTurnControl::channel();
        let handle = spawn_turn_worker(engine, session, input, worker_tx, turn_id, control_rx);
        self.current = Some(ActiveSessionTask::Turn(Box::new(ActiveTurn::new(
            turn_id, handle, control,
        ))));
    }

    pub(super) fn spawn_tracked_user_shell_command(
        &mut self,
        engine: SessionEngine,
        session: crate::session::SessionHandle,
        command: String,
        worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) {
        let task_id = self.allocate_task_id();
        let cancel = CancellationToken::new();
        spawn_user_shell_worker(engine, session, command, worker_tx, task_id, cancel.clone());
        self.current = Some(ActiveSessionTask::UserShellCommand(ActiveShell::new(
            task_id, cancel,
        )));
    }

    pub(super) fn spawn_tracked_compact(
        &mut self,
        engine: SessionEngine,
        session: crate::session::SessionHandle,
        worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) {
        let task_id = self.allocate_task_id();
        self.current = Some(ActiveSessionTask::Compact(task_id));
        spawn_compact_worker(engine, session, worker_tx, task_id);
    }

    pub(super) fn spawn_tracked_inbox(
        &mut self,
        engine: SessionEngine,
        session: crate::session::SessionHandle,
        worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) {
        let task_id = self.allocate_task_id();
        self.current = Some(ActiveSessionTask::Inbox(task_id));
        spawn_inbox_worker(engine, session, worker_tx, task_id);
    }

    pub(super) fn spawn_tracked_finalize(
        &mut self,
        engine: SessionEngine,
        session: crate::session::SessionHandle,
        worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) {
        let task_id = self.allocate_task_id();
        self.current = Some(ActiveSessionTask::Finalize(task_id));
        spawn_finalize_worker(engine, session, worker_tx, task_id);
    }

    pub(super) fn spawn_tracked_finalize_enqueue(
        &mut self,
        engine: SessionEngine,
        session: crate::session::SessionHandle,
        supervisor: crate::supervisor::SupervisorLaunchConfig,
        worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) {
        let task_id = self.allocate_task_id();
        self.current = Some(ActiveSessionTask::Finalize(task_id));
        spawn_finalize_enqueue_worker(engine, session, supervisor, worker_tx, task_id);
    }
}

pub(super) fn spawn_start_worker(
    engine: SessionEngine,
    max_attempts: usize,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let event_tx = worker_tx.clone();
        let result = engine
            .start_session(max_attempts, move |event| {
                let _ = event_tx.send(WorkerEvent::Session {
                    task_id: None,
                    event,
                });
            })
            .await;
        if let Ok(report) = &result {
            match engine
                .estimate_session_context_tokens(&report.session)
                .await
            {
                Ok(used_tokens) => {
                    let _ = worker_tx.send(WorkerEvent::Session {
                        task_id: None,
                        event: SessionEvent::ContextUsageUpdated { used_tokens },
                    });
                }
                Err(e) => {
                    log::warn!(
                        target: "session_tui",
                        "Session start 后估算 ctx 失败: {e:#}"
                    );
                }
            }
        }
        let _ = worker_tx.send(WorkerEvent::StartFinished(result));
    })
}

pub(super) fn spawn_resume_list_worker(
    engine: SessionEngine,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = engine.list_resumable_sessions().await;
        match result {
            Ok(sessions) => {
                let _ = worker_tx.send(WorkerEvent::ResumeListLoaded { sessions });
            }
            Err(error) => {
                let _ = worker_tx.send(WorkerEvent::ResumeSessionLoaded { result: Err(error) });
            }
        }
    })
}

pub(super) fn spawn_resume_open_worker(
    engine: SessionEngine,
    session_id: SessionId,
    temporary_session_id: Option<SessionId>,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = async {
            let session = engine.reopen_existing_session(&session_id).await?;
            let inbox_report = engine.process_inbox_for_resume(&session).await?;
            let messages = session.read_messages().await?;
            let journal_read = session.read_turn_journal().await;
            let (last_turns, journal_warning) =
                select_resume_history(&messages, journal_read, RESUME_HISTORY_REAL_USER_TURNS);
            let turn_count = crate::session::count_real_user_turns(&messages);
            let context_used_tokens = match engine.estimate_session_context_tokens(&session).await {
                Ok(used_tokens) => Some(used_tokens),
                Err(e) => {
                    log::warn!(
                        target: "session_tui",
                        "Resume 后估算 ctx 失败: {e:#}"
                    );
                    None
                }
            };
            let local_claim_count = match engine.local_claim_count().await {
                Ok(total) => Some(total),
                Err(e) => {
                    log::warn!(
                        target: "session_tui",
                        "Resume 后刷新 local claim 计数失败: {e:#}"
                    );
                    None
                }
            };
            if let Some(temporary_session_id) = temporary_session_id {
                match engine.delete_empty_session(&temporary_session_id).await {
                    Ok(true) => {}
                    Ok(false) => log::warn!(
                        target: "session_tui",
                        "Resume 后临时空 session 未被删除: {temporary_session_id}"
                    ),
                    Err(e) => log::warn!(
                        target: "session_tui",
                        "Resume 后删除临时空 session 失败 ({temporary_session_id}): {e:#}"
                    ),
                }
            }
            anyhow::Ok(ResumeSessionOutcome {
                session,
                inbox_report,
                last_turns,
                turn_count,
                local_claim_count,
                context_used_tokens,
                journal_warning,
            })
        }
        .await;
        let _ = worker_tx.send(WorkerEvent::ResumeSessionLoaded { result });
    })
}

fn select_resume_history(
    messages: &[crate::session::SessionMessage],
    journal_read: crate::session::TurnJournalRead,
    limit: usize,
) -> (Vec<crate::session::HistoricalTimelineTurn>, Option<String>) {
    let journal_has_read_warnings = !journal_read.warnings.is_empty();
    if journal_read.events.is_empty() {
        let warning = resume_fallback_journal_warning(journal_has_read_warnings, messages);
        return (
            crate::session::extract_last_n_timeline_turns(messages, limit),
            warning,
        );
    }

    let projection = crate::session::replay_turn_journal(journal_read);
    let canonical = crate::session::extract_last_n_timeline_turns(messages, limit);
    let journal = crate::session::extract_last_n_timeline_turns_from_journal(&projection, limit);
    if !journal_has_read_warnings && journal_covers_canonical_suffix(&canonical, &journal) {
        return (journal, None);
    }

    (
        merge_resume_turns(canonical, journal, limit),
        journal_has_read_warnings.then(|| TURN_JOURNAL_FALLBACK_WARNING.to_string()),
    )
}

fn journal_covers_canonical_suffix(
    canonical: &[crate::session::HistoricalTimelineTurn],
    journal: &[crate::session::HistoricalTimelineTurn],
) -> bool {
    let committed = journal
        .iter()
        .filter(|turn| turn.status == Some(crate::session::TurnJournalStatus::Committed))
        .collect::<Vec<_>>();
    if canonical.len() > committed.len() {
        return false;
    }
    committed[committed.len().saturating_sub(canonical.len())..]
        .iter()
        .zip(canonical)
        .all(|(journal_turn, canonical_turn)| {
            resume_turn_user_identity_matches(canonical_turn, journal_turn)
                && canonical_turn.assistant_text == journal_turn.assistant_text
        })
}

fn merge_resume_turns(
    base: Vec<crate::session::HistoricalTimelineTurn>,
    journal: Vec<crate::session::HistoricalTimelineTurn>,
    limit: usize,
) -> Vec<crate::session::HistoricalTimelineTurn> {
    let match_matrix = unique_resume_turn_matches(&base, &journal);
    let matches = resume_turn_lcs_lengths(&match_matrix);
    let mut merged = Vec::with_capacity(base.len().saturating_add(journal.len()));
    let (mut base_index, mut journal_index) = (0usize, 0usize);
    while base_index < base.len() && journal_index < journal.len() {
        if match_matrix[base_index][journal_index] {
            if matches[base_index + 1][journal_index] == matches[base_index][journal_index] {
                merged.push((base[base_index].clone(), ResumeTurnOrigin::Canonical));
                base_index = base_index.saturating_add(1);
            } else {
                merged.push((
                    merge_matched_resume_turn(&base[base_index], &journal[journal_index]),
                    ResumeTurnOrigin::Canonical,
                ));
                base_index = base_index.saturating_add(1);
                journal_index = journal_index.saturating_add(1);
            }
        } else if matches[base_index + 1][journal_index] >= matches[base_index][journal_index + 1] {
            merged.push((base[base_index].clone(), ResumeTurnOrigin::Canonical));
            base_index = base_index.saturating_add(1);
        } else {
            merged.push((
                mark_unaligned_journal_turn(journal[journal_index].clone()),
                ResumeTurnOrigin::UnalignedJournal,
            ));
            journal_index = journal_index.saturating_add(1);
        }
    }
    merged.extend(
        base.into_iter()
            .skip(base_index)
            .map(|turn| (turn, ResumeTurnOrigin::Canonical)),
    );
    merged.extend(
        journal
            .into_iter()
            .skip(journal_index)
            .map(mark_unaligned_journal_turn)
            .map(|turn| (turn, ResumeTurnOrigin::UnalignedJournal)),
    );
    limit_merged_resume_turns(merged, limit)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResumeTurnOrigin {
    Canonical,
    UnalignedJournal,
}

fn limit_merged_resume_turns(
    merged: Vec<(crate::session::HistoricalTimelineTurn, ResumeTurnOrigin)>,
    limit: usize,
) -> Vec<crate::session::HistoricalTimelineTurn> {
    let canonical_count = merged
        .iter()
        .filter(|(_, origin)| *origin == ResumeTurnOrigin::Canonical)
        .count();
    let journal_count = merged.len().saturating_sub(canonical_count);
    let canonical_to_skip = canonical_count.saturating_sub(limit);
    let journal_capacity = limit.saturating_sub(canonical_count.min(limit));
    let journal_to_skip = journal_count.saturating_sub(journal_capacity);
    let (mut canonical_seen, mut journal_seen) = (0usize, 0usize);

    merged
        .into_iter()
        .filter_map(|(turn, origin)| match origin {
            ResumeTurnOrigin::Canonical => {
                let keep = canonical_seen >= canonical_to_skip;
                canonical_seen = canonical_seen.saturating_add(1);
                keep.then_some(turn)
            }
            ResumeTurnOrigin::UnalignedJournal => {
                let keep = journal_seen >= journal_to_skip;
                journal_seen = journal_seen.saturating_add(1);
                keep.then_some(turn)
            }
        })
        .collect()
}

fn resume_turn_lcs_lengths(matches: &[Vec<bool>]) -> Vec<Vec<usize>> {
    let base_len = matches.len();
    let journal_len = matches.first().map_or(0, Vec::len);
    let mut lengths = vec![vec![0usize; journal_len.saturating_add(1)]; base_len.saturating_add(1)];
    for base_index in (0..base_len).rev() {
        for journal_index in (0..journal_len).rev() {
            lengths[base_index][journal_index] = if matches[base_index][journal_index] {
                lengths[base_index + 1][journal_index + 1].saturating_add(1)
            } else {
                lengths[base_index + 1][journal_index].max(lengths[base_index][journal_index + 1])
            };
        }
    }
    lengths
}

fn unique_resume_turn_matches(
    base: &[crate::session::HistoricalTimelineTurn],
    journal: &[crate::session::HistoricalTimelineTurn],
) -> Vec<Vec<bool>> {
    let compatible = base
        .iter()
        .map(|base_turn| {
            journal
                .iter()
                .map(|journal_turn| resume_turns_compatible(base_turn, journal_turn))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let base_match_counts = compatible
        .iter()
        .map(|row| row.iter().filter(|matched| **matched).count())
        .collect::<Vec<_>>();
    let journal_match_counts = (0..journal.len())
        .map(|journal_index| compatible.iter().filter(|row| row[journal_index]).count())
        .collect::<Vec<_>>();
    compatible
        .into_iter()
        .enumerate()
        .map(|(base_index, row)| {
            row.into_iter()
                .enumerate()
                .map(|(journal_index, matched)| {
                    matched
                        && base_match_counts[base_index] == 1
                        && journal_match_counts[journal_index] == 1
                })
                .collect()
        })
        .collect()
}

fn resume_turns_compatible(
    base: &crate::session::HistoricalTimelineTurn,
    journal: &crate::session::HistoricalTimelineTurn,
) -> bool {
    resume_turn_user_identity_matches(base, journal)
        && match (&base.assistant_text, &journal.assistant_text) {
            (Some(base_text), Some(journal_text)) => base_text == journal_text,
            _ => true,
        }
}

fn resume_turn_user_identity_matches(
    canonical: &crate::session::HistoricalTimelineTurn,
    journal: &crate::session::HistoricalTimelineTurn,
) -> bool {
    match journal.canonical_user_content_hash.as_deref() {
        Some(journal_hash) => {
            canonical.canonical_user_content_hash.as_deref() == Some(journal_hash)
        }
        None => canonical.user_text == journal.user_text,
    }
}

fn merge_matched_resume_turn(
    canonical: &crate::session::HistoricalTimelineTurn,
    journal: &crate::session::HistoricalTimelineTurn,
) -> crate::session::HistoricalTimelineTurn {
    let mut merged = journal.clone();
    if merged.assistant_text.is_none() {
        merged.assistant_text.clone_from(&canonical.assistant_text);
        merged.assistant_completed |= canonical.assistant_completed;
        if let Some(text) = canonical.assistant_text.as_deref() {
            let notice = if merged.timeline_items.is_empty() {
                "journal 缺少 assistant timeline；assistant 内容已从 canonical transcript 降级恢复。"
                    .to_string()
            } else {
                format!(
                    "journal 缺少 assistant timeline；以下内容从 canonical transcript 降级恢复，原始相对顺序未知：\n{text}"
                )
            };
            append_recovery_notice(&mut merged, notice);
        }
    }
    merged
}

fn mark_unaligned_journal_turn(
    mut turn: crate::session::HistoricalTimelineTurn,
) -> crate::session::HistoricalTimelineTurn {
    append_recovery_notice(&mut turn, UNALIGNED_JOURNAL_TURN_NOTICE.to_string());
    turn
}

fn append_recovery_notice(turn: &mut crate::session::HistoricalTimelineTurn, notice: String) {
    match turn.recovery_notice.as_mut() {
        Some(existing) => {
            existing.push('\n');
            existing.push_str(&notice);
        }
        None => turn.recovery_notice = Some(notice),
    }
}

fn resume_fallback_journal_warning(
    journal_has_warnings: bool,
    messages: &[crate::session::SessionMessage],
) -> Option<String> {
    if journal_has_warnings || !messages.is_empty() {
        Some(TURN_JOURNAL_FALLBACK_WARNING.to_string())
    } else {
        None
    }
}

fn spawn_turn_worker(
    engine: SessionEngine,
    mut session: crate::session::SessionHandle,
    input: QueuedInput,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    turn_id: u64,
    control_rx: SessionTurnControlReceiver,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let event_tx = worker_tx.clone();
        let text = input.text().to_string();
        let skill_source_text = input.command_text().to_string();
        let attachments = input.attachments().to_vec();
        let result = engine
            .run_turn_with_attachments_and_skill_source_controlled(
                &mut session,
                text,
                attachments,
                Some(skill_source_text),
                Some(control_rx),
                move |event| {
                    let _ = event_tx.send(WorkerEvent::Session {
                        task_id: Some(turn_id),
                        event,
                    });
                },
            )
            .await
            .map(|_| session);
        let _ = worker_tx.send(WorkerEvent::TurnFinished { turn_id, result });
    })
}

fn spawn_user_shell_worker(
    engine: SessionEngine,
    mut session: crate::session::SessionHandle,
    command: String,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    task_id: u64,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let event_tx = worker_tx.clone();
        let result = engine
            .run_user_shell_command(&mut session, command, cancel, move |event| {
                let _ = event_tx.send(WorkerEvent::Session {
                    task_id: Some(task_id),
                    event,
                });
            })
            .await
            .map(|_| session);
        let _ = worker_tx.send(WorkerEvent::UserShellCommandFinished { task_id, result });
    });
}

fn spawn_compact_worker(
    engine: SessionEngine,
    mut session: crate::session::SessionHandle,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    task_id: u64,
) {
    tokio::spawn(async move {
        let event_tx = worker_tx.clone();
        let result = engine
            .compact_session_checkpoint(&mut session, move |event| {
                let _ = event_tx.send(WorkerEvent::Session {
                    task_id: Some(task_id),
                    event,
                });
            })
            .await
            .map(|outcome| match outcome {
                SessionCompactionResult::Compacted(_) => CompactWorkerOutcome::Compacted(session),
                SessionCompactionResult::Noop(reason) => {
                    CompactWorkerOutcome::Noop { session, reason }
                }
            });
        let _ = worker_tx.send(WorkerEvent::CompactFinished { task_id, result });
    });
}

fn spawn_inbox_worker(
    engine: SessionEngine,
    session: crate::session::SessionHandle,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    task_id: u64,
) {
    tokio::spawn(async move {
        let event_tx = worker_tx.clone();
        let result = engine
            .process_inbox_during_session(&session, move |event| {
                let _ = event_tx.send(WorkerEvent::Session {
                    task_id: Some(task_id),
                    event,
                });
            })
            .await
            .map(|report| InboxWorkerOutcome { session, report });
        let _ = worker_tx.send(WorkerEvent::InboxFinished { task_id, result });
    });
}

fn spawn_finalize_worker(
    engine: SessionEngine,
    mut session: crate::session::SessionHandle,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    task_id: u64,
) {
    tokio::spawn(async move {
        let event_tx = worker_tx.clone();
        let result = engine
            .finalize_session(&mut session, move |event| {
                let _ = event_tx.send(WorkerEvent::Session {
                    task_id: Some(task_id),
                    event,
                });
            })
            .await
            .map(|_| ());
        let _ = worker_tx.send(WorkerEvent::FinalizeFinished { task_id, result });
    });
}

fn spawn_finalize_enqueue_worker(
    engine: SessionEngine,
    mut session: crate::session::SessionHandle,
    supervisor: crate::supervisor::SupervisorLaunchConfig,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    task_id: u64,
) {
    tokio::spawn(async move {
        match async {
            let metadata = session.read_metadata().await?;
            if !finalize_needs_background_job(&metadata) {
                return anyhow::Ok(None);
            }
            engine.mark_session_finalizing(&mut session).await?;
            let job_id =
                crate::supervisor::enqueue_finalize(&supervisor, session.metadata.id.clone())
                    .await?;
            anyhow::Ok(Some(job_id))
        }
        .await
        {
            Ok(Some(job_id)) => {
                let result = FinalizeEnqueueOutcome::Enqueued {
                    job_id,
                    session_id: session.metadata.id.clone(),
                };
                let _ = worker_tx.send(WorkerEvent::FinalizeEnqueueFinished { task_id, result });
            }
            Ok(None) => {
                let event_tx = worker_tx.clone();
                let result = engine
                    .finalize_session(&mut session, move |event| {
                        let _ = event_tx.send(WorkerEvent::Session {
                            task_id: Some(task_id),
                            event,
                        });
                    })
                    .await
                    .map(|_| ());
                let _ = worker_tx.send(WorkerEvent::FinalizeFinished { task_id, result });
            }
            Err(error) => {
                let result = FinalizeEnqueueOutcome::Fallback {
                    session: Box::new(session),
                    error,
                };
                let _ = worker_tx.send(WorkerEvent::FinalizeEnqueueFinished { task_id, result });
            }
        }
    });
}

fn finalize_needs_background_job(metadata: &crate::session::SessionMetadata) -> bool {
    metadata.message_count > metadata.recapped_until
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::api::ToolExecutionOutcome;
    use crate::claim::{AgentId, SessionId};
    use crate::session::{
        SessionContentBlock, SessionMessage, SessionMessageRole, TurnJournalEvent,
        TurnJournalEventKind, TurnJournalRead, TurnJournalStatus, TurnJournalWarning,
    };
    use chrono::Utc;

    #[test]
    fn finalize_running_only_matches_finalize_and_finish_clears_task() {
        let mut state = SessionTaskState {
            current: Some(ActiveSessionTask::Compact(1)),
            next_task_id: 2,
        };
        assert!(!state.finalize_running());
        assert!(!state.finish_finalize(1));
        assert!(matches!(state.current, Some(ActiveSessionTask::Compact(_))));

        state.current = Some(ActiveSessionTask::Finalize(2));

        assert!(state.finalize_running());
        assert!(!state.finish_finalize(1));
        assert!(state.finish_finalize(2));
        assert!(!state.task_running());
    }

    #[test]
    fn resume_fallback_warns_when_canonical_messages_exist_without_journal() {
        let messages = vec![SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::text("old session")],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        }];

        assert_eq!(
            resume_fallback_journal_warning(false, &messages).as_deref(),
            Some(TURN_JOURNAL_FALLBACK_WARNING)
        );
        assert_eq!(
            resume_fallback_journal_warning(true, &[]).as_deref(),
            Some(TURN_JOURNAL_FALLBACK_WARNING)
        );
        assert!(resume_fallback_journal_warning(false, &[]).is_none());
    }

    #[test]
    fn resume_history_deduplicates_committed_text_attachment_turn() {
        let now = Utc::now();
        let user_text = "请检查 @src/lib.rs";
        let attachment_text =
            "Attached file: lib.rs\nPath: /workspace/src/lib.rs\n\nfn very_long() {}";
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![
                    SessionContentBlock::text(user_text),
                    SessionContentBlock::text(attachment_text),
                ],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text("已检查")],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            },
        ];
        let journal_read = TurnJournalRead {
            events: vec![
                TurnJournalEvent {
                    seq: 1,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::UserInputAccepted {
                        text: user_text.into(),
                    },
                },
                TurnJournalEvent {
                    seq: 2,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::CanonicalUserMessage {
                        content_hash: Some(
                            crate::session::canonical_user_content_hash(&messages[0].content)
                                .unwrap(),
                        ),
                        content: None,
                    },
                },
                TurnJournalEvent {
                    seq: 3,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::AssistantCompleted {
                        text: "已检查".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 4,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::Committed,
                    },
                },
            ],
            warnings: Vec::new(),
        };

        let (turns, warning) = select_resume_history(&messages, journal_read, 10);

        assert!(warning.is_none());
        assert_eq!(turns.len(), 1, "已提交附件 turn 不应在恢复时重复显示");
        assert_eq!(turns[0].user_text, user_text);
        assert_eq!(turns[0].assistant_text.as_deref(), Some("已检查"));
        assert!(!turns[0].user_text.contains("fn very_long"));
    }

    #[test]
    fn resume_history_uses_hash_to_align_same_prompt_with_different_attachments() {
        let now = Utc::now();
        let user = |index, attachment: &str| SessionMessage {
            index,
            role: SessionMessageRole::User,
            content: vec![
                SessionContentBlock::text("重试"),
                SessionContentBlock::text(format!(
                    "Attached file: input.txt\nPath: /tmp/input.txt\n\n{attachment}"
                )),
            ],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        };
        let assistant = |index| SessionMessage {
            index,
            role: SessionMessageRole::Assistant,
            content: vec![SessionContentBlock::text("完成")],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        };
        let messages = vec![
            user(0, "first"),
            assistant(1),
            user(2, "second"),
            assistant(3),
        ];
        let first_hash = crate::session::canonical_user_content_hash(&messages[0].content).unwrap();
        let second_hash =
            crate::session::canonical_user_content_hash(&messages[2].content).unwrap();
        assert_ne!(first_hash, second_hash);
        let journal_read = TurnJournalRead {
            events: vec![
                TurnJournalEvent {
                    seq: 1,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::UserInputAccepted {
                        text: "重试".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 2,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::CanonicalUserMessage {
                        content_hash: Some(first_hash),
                        content: None,
                    },
                },
                TurnJournalEvent {
                    seq: 3,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::AssistantCompleted {
                        text: "完成".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 4,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::Committed,
                    },
                },
                TurnJournalEvent {
                    seq: 5,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::UserInputAccepted {
                        text: "重试".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 6,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::CanonicalUserMessage {
                        content_hash: Some(second_hash),
                        content: None,
                    },
                },
                TurnJournalEvent {
                    seq: 7,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::AssistantCompleted {
                        text: "完成".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 8,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::Committed,
                    },
                },
            ],
            warnings: Vec::new(),
        };

        let (turns, warning) = select_resume_history(&messages, journal_read, 10);

        assert!(warning.is_none());
        assert_eq!(turns.len(), 2);
        assert!(turns.iter().all(|turn| turn.user_text == "重试"));
        assert!(turns.iter().all(|turn| turn.recovery_notice.is_none()));
    }

    #[test]
    fn resume_history_deduplicates_committed_directory_context_turn() {
        let now = Utc::now();
        let user_text = "请检查 @src/";
        let stored_text =
            "请检查 @src/\n\n[Referenced directory: src/]\nResolved path: /workspace/src\nlib.rs";
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text(stored_text)],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text("已检查目录")],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            },
        ];
        let journal_read = TurnJournalRead {
            events: vec![
                TurnJournalEvent {
                    seq: 1,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::UserInputAccepted {
                        text: stored_text.into(),
                    },
                },
                TurnJournalEvent {
                    seq: 2,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::CanonicalUserMessage {
                        content_hash: Some(
                            crate::session::canonical_user_content_hash(&messages[0].content)
                                .unwrap(),
                        ),
                        content: None,
                    },
                },
                TurnJournalEvent {
                    seq: 3,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::AssistantCompleted {
                        text: "已检查目录".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 4,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::Committed,
                    },
                },
            ],
            warnings: Vec::new(),
        };

        let (turns, warning) = select_resume_history(&messages, journal_read, 10);

        assert!(warning.is_none());
        assert_eq!(turns.len(), 1, "目录上下文不能在恢复时形成重复 turn");
        assert_eq!(turns[0].user_text, user_text);
        assert!(!turns[0].user_text.contains("Referenced directory"));
        assert!(!turns[0].user_text.contains("Resolved path"));
    }

    #[test]
    fn resume_history_keeps_healthy_journal_diff_when_bad_tail_requires_canonical_fallback() {
        let now = Utc::now();
        let message = |index, role, text: &str| SessionMessage {
            index,
            role,
            content: vec![SessionContentBlock::text(text)],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        };
        let messages = vec![
            message(0, SessionMessageRole::User, "仅存在于 canonical 的请求"),
            message(1, SessionMessageRole::Assistant, "canonical 回复"),
            message(2, SessionMessageRole::User, "改文件"),
            message(3, SessionMessageRole::Assistant, "改完了"),
        ];
        let change = crate::tool::diff::compute_file_change(
            "note.txt",
            crate::tool::diff::FileChangeKind::Modified,
            "old\n",
            "new\n",
            20,
        )
        .expect("测试修改应产生 FileChange");
        let journal_read = TurnJournalRead {
            events: vec![
                TurnJournalEvent {
                    seq: 1,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::UserInputAccepted {
                        text: "改文件".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 2,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::ToolCallStarted {
                        tool_use_id: "toolu_1".into(),
                        name: "file_patch".into(),
                        summary: r#"tool file_patch {"path":"note.txt"}"#.into(),
                        input_preview: r#"{"path":"note.txt"}"#.into(),
                        input_truncated: false,
                    },
                },
                TurnJournalEvent {
                    seq: 3,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::ToolCallCompleted {
                        tool_use_id: "toolu_1".into(),
                        summary: "tool file_patch ok".into(),
                        outcome: Some(ToolExecutionOutcome::Completed),
                        output_preview: r#"{"status":"success"}"#.into(),
                        output_truncated: false,
                        file_change: Some(change.clone()),
                    },
                },
                TurnJournalEvent {
                    seq: 4,
                    turn_id: "turn_2".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::Committed,
                    },
                },
            ],
            warnings: vec![TurnJournalWarning {
                line: Some(5),
                message: "跳过坏 turn journal JSONL 行: EOF while parsing".into(),
            }],
        };

        // RED：resume 的选择逻辑应成为可独立测试的接口；实现需把健康 journal turn
        // 与缺失部分的 canonical fallback 合并，而不是因任意 warning 丢弃全部 diff。
        let (turns, warning) =
            select_resume_history(&messages, journal_read, RESUME_HISTORY_REAL_USER_TURNS);

        assert!(warning.is_some(), "坏尾行仍应向用户给出降级提示");
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].user_text, "仅存在于 canonical 的请求");
        assert!(turns[0].tool_calls.is_empty());
        assert_eq!(turns[1].user_text, "改文件");
        assert!(turns[1].timeline_items.iter().any(|item| matches!(
            item,
            crate::session::TurnJournalTimelineItem::ToolCall(tool)
                if tool.file_change.as_ref() == Some(&change)
        )));
    }

    #[test]
    fn resume_history_silently_merges_normal_interrupted_and_failed_turns() {
        let now = Utc::now();
        let mut messages = Vec::new();
        let mut events = Vec::new();
        let mut seq = 1_u64;

        for turn_number in 1..=12_usize {
            let request = format!("request {turn_number}");
            let response = format!("response {turn_number}");
            let message_index = turn_number.saturating_sub(1).saturating_mul(2);
            messages.push(SessionMessage {
                index: message_index,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text(request.clone())],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            });
            messages.push(SessionMessage {
                index: message_index.saturating_add(1),
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text(response.clone())],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            });

            let turn_id = format!("turn_{turn_number}");
            events.push(TurnJournalEvent {
                seq,
                turn_id: turn_id.clone(),
                created_at: now,
                kind: TurnJournalEventKind::UserInputAccepted { text: request },
            });
            seq = seq.saturating_add(1);
            events.push(TurnJournalEvent {
                seq,
                turn_id: turn_id.clone(),
                created_at: now,
                kind: TurnJournalEventKind::AssistantCompleted { text: response },
            });
            seq = seq.saturating_add(1);
            let status = match turn_number {
                5 => TurnJournalStatus::InterruptedByUser,
                9 => TurnJournalStatus::Failed,
                _ => TurnJournalStatus::Committed,
            };
            events.push(TurnJournalEvent {
                seq,
                turn_id,
                created_at: now,
                kind: TurnJournalEventKind::TurnFinished { status },
            });
            seq = seq.saturating_add(1);
        }

        // 最近十条 journal 中有两个合法但非 committed 的终态。它们不表示 journal
        // 损坏；resume 应保留其 timeline/status，并静默合并 canonical 历史。
        let (turns, warning) = select_resume_history(
            &messages,
            TurnJournalRead {
                events,
                warnings: Vec::new(),
            },
            10,
        );

        assert!(warning.is_none());
        assert_eq!(turns.len(), 10);
        assert!(turns
            .iter()
            .any(|turn| turn.status == Some(TurnJournalStatus::InterruptedByUser)));
        assert!(turns
            .iter()
            .any(|turn| turn.status == Some(TurnJournalStatus::Failed)));
    }

    #[test]
    fn resume_history_silently_merges_journal_turn_without_terminal_event() {
        let now = Utc::now();
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("request")],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text("partial response")],
                created_at: now,
                model: "test-model".into(),
                provider_replay: None,
            },
        ];
        let journal_read = TurnJournalRead {
            events: vec![
                TurnJournalEvent {
                    seq: 1,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::UserInputAccepted {
                        text: "request".into(),
                    },
                },
                TurnJournalEvent {
                    seq: 2,
                    turn_id: "turn_1".into(),
                    created_at: now,
                    kind: TurnJournalEventKind::AssistantDelta {
                        text: "partial response".into(),
                    },
                },
            ],
            warnings: Vec::new(),
        };

        let (_, warning) = select_resume_history(&messages, journal_read, 10);

        assert!(warning.is_none());
    }

    #[test]
    fn ambiguous_duplicate_user_text_keeps_journal_diff_separate() {
        let canonical_turn = || crate::session::HistoricalTimelineTurn {
            user_text: "重试".into(),
            canonical_user_content_hash: None,
            assistant_text: None,
            assistant_completed: false,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };
        let change = crate::tool::diff::compute_file_change(
            "note.txt",
            crate::tool::diff::FileChangeKind::Modified,
            "old\n",
            "new\n",
            20,
        )
        .expect("测试修改应产生 FileChange");
        let tool = crate::session::TurnJournalToolCall {
            tool_use_id: "toolu_1".into(),
            name: "file_patch".into(),
            started_summary: "tool file_patch".into(),
            input_preview: String::new(),
            input_truncated: false,
            latest_progress: None,
            completed_summary: Some("tool file_patch ok".into()),
            interrupted_summary: None,
            skipped_summary: None,
            skip_reason: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            output_preview: None,
            output_truncated: false,
            file_change: Some(change),
        };
        let journal = crate::session::HistoricalTimelineTurn {
            user_text: "重试".into(),
            canonical_user_content_hash: None,
            assistant_text: None,
            assistant_completed: false,
            status: Some(TurnJournalStatus::InterruptedByUser),
            tool_calls: vec![tool.clone()],
            timeline_items: vec![crate::session::TurnJournalTimelineItem::ToolCall(Box::new(
                tool,
            ))],
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };

        let merged =
            merge_resume_turns(vec![canonical_turn(), canonical_turn()], vec![journal], 10);

        assert_eq!(merged.len(), 3, "歧义 turn 不应被强行合并");
        assert!(merged[0].tool_calls.is_empty());
        assert!(merged[1].tool_calls.is_empty());
        assert!(merged[2].tool_calls[0].file_change.is_some());
        assert!(merged[2]
            .recovery_notice
            .as_deref()
            .is_some_and(|notice| notice.contains("无法与 canonical 历史唯一对齐")));
    }

    #[test]
    fn missing_journal_assistant_is_explicit_recovery_content_without_fabricated_timeline_order() {
        let canonical = crate::session::HistoricalTimelineTurn {
            user_text: "改文件".into(),
            canonical_user_content_hash: None,
            assistant_text: Some("canonical 最终回复".into()),
            assistant_completed: true,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };
        let tool = crate::session::TurnJournalToolCall {
            tool_use_id: "toolu_1".into(),
            name: "file_patch".into(),
            started_summary: "tool file_patch".into(),
            input_preview: String::new(),
            input_truncated: false,
            latest_progress: None,
            completed_summary: Some("tool file_patch ok".into()),
            interrupted_summary: None,
            skipped_summary: None,
            skip_reason: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            output_preview: None,
            output_truncated: false,
            file_change: None,
        };
        let journal = crate::session::HistoricalTimelineTurn {
            user_text: "改文件".into(),
            canonical_user_content_hash: None,
            assistant_text: None,
            assistant_completed: false,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: vec![tool.clone()],
            timeline_items: vec![crate::session::TurnJournalTimelineItem::ToolCall(Box::new(
                tool,
            ))],
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };

        assert!(!journal_covers_canonical_suffix(
            std::slice::from_ref(&canonical),
            std::slice::from_ref(&journal),
        ));
        let merged = merge_resume_turns(vec![canonical], vec![journal], 10);

        assert_eq!(merged.len(), 1);
        let notice = merged[0]
            .recovery_notice
            .as_deref()
            .expect("缺失 assistant timeline 必须生成恢复提示");
        assert!(notice.contains("原始相对顺序未知"));
        assert!(notice.contains("canonical 最终回复"));
        assert!(!merged[0].timeline_items.iter().any(|item| matches!(
            item,
            crate::session::TurnJournalTimelineItem::Assistant { .. }
        )));

        let mut state = super::super::state::SessionTuiState::new();
        state.push_historical_timeline_turns(&merged);
        let rendered = state.transcript_text();
        assert_eq!(rendered.matches("canonical 最终回复").count(), 1);
        assert!(rendered.contains("原始相对顺序未知"));
    }

    #[test]
    fn missing_journal_assistant_without_timeline_renders_canonical_once() {
        let canonical = crate::session::HistoricalTimelineTurn {
            user_text: "恢复请求".into(),
            canonical_user_content_hash: None,
            assistant_text: Some("canonical 完整回复".into()),
            assistant_completed: true,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };
        let journal = crate::session::HistoricalTimelineTurn {
            user_text: "恢复请求".into(),
            canonical_user_content_hash: None,
            assistant_text: None,
            assistant_completed: false,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };

        let merged = merge_resume_turns(vec![canonical], vec![journal], 10);

        assert_eq!(merged.len(), 1);
        let notice = merged[0]
            .recovery_notice
            .as_deref()
            .expect("缺失 assistant timeline 必须生成恢复提示");
        assert!(!notice.contains("canonical 完整回复"));
        let mut state = super::super::state::SessionTuiState::new();
        state.push_historical_timeline_turns(&merged);
        assert_eq!(
            state
                .transcript_text()
                .matches("canonical 完整回复")
                .count(),
            1
        );
    }

    #[test]
    fn missing_journal_assistant_with_incomplete_tool_keeps_recovery_in_scrollback() {
        let canonical = crate::session::HistoricalTimelineTurn {
            user_text: "恢复请求".into(),
            canonical_user_content_hash: None,
            assistant_text: Some("canonical 工具后回复".into()),
            assistant_completed: true,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };
        let tool = crate::session::TurnJournalToolCall {
            tool_use_id: "toolu_pending".into(),
            name: "file_patch".into(),
            started_summary: "tool file_patch".into(),
            input_preview: String::new(),
            input_truncated: false,
            latest_progress: None,
            completed_summary: None,
            interrupted_summary: None,
            skipped_summary: None,
            skip_reason: None,
            outcome: None,
            output_preview: None,
            output_truncated: false,
            file_change: None,
        };
        let journal = crate::session::HistoricalTimelineTurn {
            user_text: "恢复请求".into(),
            canonical_user_content_hash: None,
            assistant_text: None,
            assistant_completed: false,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: vec![tool.clone()],
            timeline_items: vec![crate::session::TurnJournalTimelineItem::ToolCall(Box::new(
                tool,
            ))],
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        };

        let merged = merge_resume_turns(vec![canonical], vec![journal], 10);
        let mut state = super::super::state::SessionTuiState::new();
        state.push_historical_timeline_turns(&merged);
        let scrollback = state
            .scrollback_lines(96)
            .lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            scrollback.contains("canonical 工具后回复"),
            "canonical 恢复内容应进入 scrollback: {scrollback}"
        );
        assert!(
            scrollback.contains("Recovery notice:"),
            "恢复提示应进入 scrollback: {scrollback}"
        );
    }

    #[test]
    fn resume_limit_prioritizes_duplicate_canonical_turns_over_unaligned_journal_turns() {
        let canonical = ["canonical 回复一", "canonical 回复二"]
            .into_iter()
            .map(|assistant| crate::session::HistoricalTimelineTurn {
                user_text: "重试".into(),
                canonical_user_content_hash: None,
                assistant_text: Some(assistant.into()),
                assistant_completed: true,
                status: Some(TurnJournalStatus::Committed),
                tool_calls: Vec::new(),
                timeline_items: Vec::new(),
                user_steers: Vec::new(),
                recovery_notice: None,
                turn_status_detail: None,
            })
            .collect::<Vec<_>>();
        let journal = (0..2)
            .map(|_| crate::session::HistoricalTimelineTurn {
                user_text: "重试".into(),
                canonical_user_content_hash: None,
                assistant_text: None,
                assistant_completed: false,
                status: Some(TurnJournalStatus::InterruptedByUser),
                tool_calls: Vec::new(),
                timeline_items: Vec::new(),
                user_steers: Vec::new(),
                recovery_notice: None,
                turn_status_detail: None,
            })
            .collect::<Vec<_>>();

        let merged = merge_resume_turns(canonical, journal, 2);

        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged
                .iter()
                .filter_map(|turn| turn.assistant_text.as_deref())
                .collect::<Vec<_>>(),
            vec!["canonical 回复一", "canonical 回复二"]
        );
        assert!(merged.iter().all(|turn| turn.recovery_notice.is_none()));
    }

    #[test]
    fn warning_fallback_preserves_earlier_interrupted_file_diff() {
        let now = Utc::now();
        let message = |index, role, text: &str| SessionMessage {
            index,
            role,
            content: vec![SessionContentBlock::text(text)],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        };
        let messages = vec![
            message(0, SessionMessageRole::User, "first"),
            message(1, SessionMessageRole::Assistant, "partial"),
            message(2, SessionMessageRole::User, "second"),
            message(3, SessionMessageRole::Assistant, "done"),
        ];
        let change = crate::tool::diff::compute_file_change(
            "note.txt",
            crate::tool::diff::FileChangeKind::Modified,
            "old\n",
            "new\n",
            20,
        )
        .expect("测试修改应产生 FileChange");
        let event = |seq, turn_id: &str, kind| TurnJournalEvent {
            seq,
            turn_id: turn_id.into(),
            created_at: now,
            kind,
        };
        let journal_read = TurnJournalRead {
            events: vec![
                event(
                    1,
                    "turn_1",
                    TurnJournalEventKind::UserInputAccepted {
                        text: "first".into(),
                    },
                ),
                event(
                    2,
                    "turn_1",
                    TurnJournalEventKind::ToolCallStarted {
                        tool_use_id: "toolu_1".into(),
                        name: "file_patch".into(),
                        summary: "tool file_patch".into(),
                        input_preview: String::new(),
                        input_truncated: false,
                    },
                ),
                event(
                    3,
                    "turn_1",
                    TurnJournalEventKind::ToolCallCompleted {
                        tool_use_id: "toolu_1".into(),
                        summary: "tool file_patch ok".into(),
                        outcome: Some(ToolExecutionOutcome::Completed),
                        output_preview: String::new(),
                        output_truncated: false,
                        file_change: Some(change),
                    },
                ),
                event(
                    4,
                    "turn_1",
                    TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::InterruptedByUser,
                    },
                ),
                event(
                    5,
                    "turn_2",
                    TurnJournalEventKind::UserInputAccepted {
                        text: "second".into(),
                    },
                ),
                event(
                    6,
                    "turn_2",
                    TurnJournalEventKind::TurnFinished {
                        status: TurnJournalStatus::Committed,
                    },
                ),
            ],
            warnings: vec![TurnJournalWarning {
                line: Some(7),
                message: "bad tail".into(),
            }],
        };

        let (turns, warning) = select_resume_history(&messages, journal_read, 10);

        assert!(warning.is_some());
        let interrupted = turns
            .iter()
            .find(|turn| turn.status == Some(TurnJournalStatus::InterruptedByUser))
            .expect("应保留 interrupted turn");
        assert!(interrupted.tool_calls[0].file_change.is_some());
    }

    #[test]
    fn finalize_background_job_only_needed_for_unrecapped_messages() {
        let mut metadata = crate::session::SessionMetadata {
            id: SessionId::from_str("session_1234abcd").unwrap(),
            agent_id: AgentId::new("agent-a").unwrap(),
            status: crate::session::SessionStatus::Open,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
            source: "test".into(),
            model: "test-model".into(),
            system_prompt_path: "system.md".into(),
            message_count: 2,
            finalized_at: None,
            recapped_until: 2,
            compaction: None,
        };

        assert!(!finalize_needs_background_job(&metadata));

        metadata.recapped_until = 1;
        assert!(finalize_needs_background_job(&metadata));

        metadata.message_count = 0;
        metadata.recapped_until = 0;
        assert!(!finalize_needs_background_job(&metadata));
    }

    #[tokio::test]
    async fn running_turn_uses_pending_steer_control_instead_of_hard_cancel() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let mut state = SessionTaskState {
            current: Some(ActiveSessionTask::Turn(Box::new(ActiveTurn {
                id: 7,
                handle,
                phase: ActiveTurnPhase::Running,
                pending_steers: Vec::new(),
                pending_cancel: false,
                control: SessionTurnControl::acknowledged_for_test(),
            }))),
            next_task_id: 8,
        };

        assert!(
            state
                .request_tool_boundary_steer(0, &QueuedInput::from_text("first steer"))
                .await
        );
        assert!(
            state
                .request_tool_boundary_steer(1, &QueuedInput::from_text("second steer"))
                .await
        );
        state.mark_turn_committed(7);
        assert!(
            !state
                .request_tool_boundary_steer(2, &QueuedInput::from_text("late steer"))
                .await
        );
        assert!(!state.request_tool_boundary_cancel("late cancel"));

        let mut active = state.finish_turn(7).unwrap();
        let pending = active.take_pending_steer_input().unwrap();

        assert_eq!(pending.text(), "first steer\n\nsecond steer");
        active.handle.abort();
    }

    #[tokio::test]
    async fn tool_boundary_cancel_preserves_pending_steers_for_restore() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let mut state = SessionTaskState {
            current: Some(ActiveSessionTask::Turn(Box::new(ActiveTurn {
                id: 7,
                handle,
                phase: ActiveTurnPhase::Running,
                pending_steers: Vec::new(),
                pending_cancel: false,
                control: SessionTurnControl::acknowledged_for_test(),
            }))),
            next_task_id: 8,
        };

        assert!(
            state
                .request_tool_boundary_steer(11, &QueuedInput::from_text("steer"))
                .await
        );
        assert!(state.request_tool_boundary_cancel("user cancelled turn"));

        let mut active = state.finish_turn(7).unwrap();
        let pending = active.take_pending_steer_inputs_for_restore();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, 11);
        assert_eq!(pending[0].input.text(), "steer");
        active.handle.abort();
    }

    #[tokio::test]
    async fn repeated_tool_boundary_cancel_is_ignored_while_pending() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let mut state = SessionTaskState {
            current: Some(ActiveSessionTask::Turn(Box::new(ActiveTurn {
                id: 7,
                handle,
                phase: ActiveTurnPhase::Running,
                pending_steers: Vec::new(),
                pending_cancel: false,
                control: SessionTurnControl::acknowledged_for_test(),
            }))),
            next_task_id: 8,
        };

        assert!(state.request_tool_boundary_cancel("first cancel"));
        assert!(state.pending_cancel_requested());
        assert!(!state.request_tool_boundary_cancel("second cancel"));

        let active = state.finish_turn(7).unwrap();
        active.handle.abort();
    }

    #[tokio::test]
    async fn tool_boundary_cancel_restores_pending_steer_draft_metadata() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let mut state = SessionTaskState {
            current: Some(ActiveSessionTask::Turn(Box::new(ActiveTurn {
                id: 7,
                handle,
                phase: ActiveTurnPhase::Running,
                pending_steers: Vec::new(),
                pending_cancel: false,
                control: SessionTurnControl::acknowledged_for_test(),
            }))),
            next_task_id: 8,
        };
        let pasted = format!("//! {}\nfn main() {{}}", "x".repeat(1200));
        let placeholder = "[Pasted Content 1220 chars #1]".to_string();
        let draft = super::super::bottom_pane::InputDraft {
            text: placeholder.clone(),
            pending_pastes: vec![(placeholder.clone(), pasted.clone())],
            attachments: Vec::new(),
        };

        assert!(
            state
                .request_tool_boundary_steer(13, &QueuedInput::new(pasted.clone(), draft))
                .await
        );
        assert!(state.request_tool_boundary_cancel("user cancelled turn"));

        let mut active = state.finish_turn(7).unwrap();
        let mut pending = active.take_pending_steer_inputs_for_restore();
        assert_eq!(pending.len(), 1);
        let pending = pending.remove(0);
        assert_eq!(pending.sequence, 13);
        let restored = pending.input.into_draft();
        assert_eq!(restored.visible_text(), placeholder);
        assert_eq!(restored.expanded_text(), pasted);
        active.handle.abort();
    }
}
