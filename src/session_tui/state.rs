//! TUI session 展示状态投影。
//!
//! `SessionTuiState` 只消费稳定的 `SessionEvent` 和本地 UI 意图，维护可渲染状态。
//! session 生命周期、turn 提交和 finalize 落盘仍由 `SessionEngine` 与 app runtime 负责。

use crate::agent::{SessionEvent, SessionRuntimeStatus, TeamServicesConnectionStatus};
use crate::api::{ToolCallSkipReason, ToolExecutionOutcome};
use crate::attachment::{AttachmentLimits, NormalizedMedia};
use crate::config::{AttachmentConfig, DEFAULT_LLM_CONTEXT_WINDOW};
use crate::delegation::{DelegationId, DelegationStatus, DelegationSummary};
use crate::mcp::connection_manager::McpRuntimeState;
#[cfg(test)]
use crate::session::HistoricalTurn;
use crate::session::{HistoricalTimelineTurn, TurnJournalStatus, TurnJournalTimelineItem};

use chrono::Local;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use std::collections::BTreeSet;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::at_path_completion::{AtPathCompletionLimits, AtPathDirectoryEntry};
use super::attachment::AttachmentError;
use super::bottom_pane::{BottomPane, InputDraft};
use super::cell::user_text_display_lines;
use super::input_queue::{InputQueueState, PendingInputPreview, QueuedInput};
use super::mcp_panel::{McpPanelKeyAction, McpPanelRequest, McpPanelState};
use super::process_panel::{ProcessPanelKeyAction, ProcessPanelState, ProcessTerminationTarget};
use super::runtime::McpOperationOutcome;
use super::theme::{blue_style, muted_style, surface_style};
use super::transcript::{ScrollbackLines, ShellCommandCompletion, TranscriptState};
use super::turn_animation::TurnAnimationState;

const FOCUS_INPUT_GRACE: Duration = Duration::from_secs(90);
/// 低于此宽度时把后台状态改为短标签，并把 subagent 终态 notice 单列，避免硬切单词。
const BACKGROUND_STATUS_COMPACT_WIDTH: usize = 64;
pub(super) const ATTACHMENT_STEER_QUEUE_NOTICE: &str = "附件输入已排队，不能打断注入当前 turn。";
pub(super) const SLASH_COMMAND_STEER_QUEUE_NOTICE: &str =
    "Slash command输入已排队，不能打断注入当前 turn。";
#[derive(Debug, Clone)]
pub struct SessionTuiState {
    pub agent_id: Option<String>,
    pub model_name: Option<String>,
    pub session_id: Option<String>,
    pub status: SessionRuntimeStatus,
    pub message_count: usize,
    pub turn_count: usize,
    context_used_tokens: Option<usize>,
    context_window_tokens: usize,
    bottom_pane: BottomPane,
    input_queue: InputQueueState,
    transcript: TranscriptState,
    network: NetworkSnapshot,
    workspace_label: String,
    branch_label: String,
    focus_accumulated: Duration,
    focus_last_sample: Instant,
    focus_last_user_activity: Option<Instant>,
    turn_animation: TurnAnimationState,
    pending_user_echo: Option<String>,
    pending_tool_boundary_steer: Option<String>,
    interrupted_background_processes: BTreeSet<String>,
    turn_in_flight: bool,
    committed_turn_finishing: bool,
    shell_in_flight: bool,
    foreground_task_started_at: Option<Instant>,
    status_notice: Option<String>,
    start_separator_flushed: bool,
    attachment_cfg: AttachmentConfig,
    mcp_panel: McpPanelState,
    process_panel: ProcessPanelState,
    delegation_panel: DelegationPanelState,
    input_revision: u64,
    at_path_scan_generation: u64,
    pending_at_path_scans: BTreeSet<u64>,
    pending_clipboard_image_reads: usize,
    next_input_submission_sequence: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct NetworkSnapshot {
    pub(super) local_claims_total: Option<usize>,
    pub(super) last_router_lookup: Option<RouterLookupSnapshot>,
    pub(super) last_contribution: Option<ContributionSnapshot>,
    pub(super) team_services: TeamServicesConnectionStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct DelegationPanelState {
    visible: bool,
    summaries: Vec<DelegationSummary>,
    error: Option<String>,
    scroll: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RouterLookupSnapshot {
    pub(super) candidate_claims: usize,
    pub(super) disputes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContributionSnapshot {
    pub(super) kind: ContributionKind,
    pub(super) processed: Option<usize>,
    pub(super) new_claims: usize,
    pub(super) updated_claims: usize,
    pub(super) deprecated_claims: usize,
    pub(super) new_disputes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContributionKind {
    Inbox,
    Compact,
    Finalize,
}

fn is_elapsed_title_status(status: SessionRuntimeStatus) -> bool {
    matches!(
        status,
        SessionRuntimeStatus::Initializing
            | SessionRuntimeStatus::Running
            | SessionRuntimeStatus::SyncingInbox
            | SessionRuntimeStatus::Compacting
            | SessionRuntimeStatus::Finalizing
    )
}

impl Default for SessionTuiState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            agent_id: None,
            model_name: None,
            session_id: None,
            status: SessionRuntimeStatus::Open,
            message_count: 0,
            turn_count: 0,
            context_used_tokens: None,
            context_window_tokens: DEFAULT_LLM_CONTEXT_WINDOW,
            bottom_pane: BottomPane::default(),
            input_queue: InputQueueState::default(),
            transcript: TranscriptState::default(),
            network: NetworkSnapshot::default(),
            // 占位，真实值由 detect_workspace_context() 在 spawn_blocking 里探测后注入，
            // 避免在 async 上下文（Default 经 run_session_tui 构造）同步阻塞 git 子进程。
            workspace_label: "--".into(),
            branch_label: "--".into(),
            focus_accumulated: Duration::ZERO,
            focus_last_sample: now,
            focus_last_user_activity: None,
            turn_animation: TurnAnimationState::default(),
            pending_user_echo: None,
            pending_tool_boundary_steer: None,
            interrupted_background_processes: BTreeSet::new(),
            turn_in_flight: false,
            committed_turn_finishing: false,
            shell_in_flight: false,
            foreground_task_started_at: None,
            status_notice: None,
            start_separator_flushed: false,
            attachment_cfg: AttachmentConfig::default(),
            mcp_panel: McpPanelState::default(),
            process_panel: ProcessPanelState::default(),
            delegation_panel: DelegationPanelState::default(),
            input_revision: 0,
            at_path_scan_generation: 0,
            pending_at_path_scans: BTreeSet::new(),
            pending_clipboard_image_reads: 0,
            next_input_submission_sequence: 0,
        }
    }
}

impl SessionTuiState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn foreground_task_elapsed_secs(&self) -> u64 {
        self.foreground_task_started_at
            .map(|started_at| started_at.elapsed().as_secs())
            .unwrap_or(0)
    }

    fn bump_input_revision(&mut self) {
        self.input_revision = self.input_revision.saturating_add(1);
    }

    fn bump_input_revision_if_draft_changed(&mut self, before: InputDraft) {
        if self.bottom_pane.current_draft() != before {
            self.bump_input_revision();
        }
    }

    pub fn apply_event(&mut self, event: SessionEvent) {
        self.refresh_focus_timer();
        match event {
            SessionEvent::StartupProgress { label } => {
                if self.status != SessionRuntimeStatus::Initializing {
                    self.foreground_task_started_at = Some(Instant::now());
                }
                self.status = SessionRuntimeStatus::Initializing;
                self.transcript.set_activity(Some(label));
            }
            SessionEvent::SessionStarted {
                session_id,
                agent_id,
            } => {
                self.session_id = Some(session_id.to_string());
                self.agent_id = Some(agent_id.to_string());
                self.bottom_pane.set_finalize_failed(false);
            }
            SessionEvent::StatusChanged { status } => {
                let was_foreground_task = is_elapsed_title_status(self.status);
                let is_foreground_task = is_elapsed_title_status(status);
                if is_foreground_task && (!was_foreground_task || self.status != status) {
                    self.foreground_task_started_at = Some(Instant::now());
                } else if !is_foreground_task {
                    self.foreground_task_started_at = None;
                }
                self.status = status;
                if matches!(
                    status,
                    SessionRuntimeStatus::Open
                        | SessionRuntimeStatus::Error
                        | SessionRuntimeStatus::Closed
                ) {
                    self.clear_settled_status_notice();
                }
                match status {
                    SessionRuntimeStatus::Initializing => {
                        self.transcript
                            .set_activity(Some("initializing session...".into()));
                    }
                    SessionRuntimeStatus::Running => {
                        let activity = if self.shell_in_flight {
                            "running shell command..."
                        } else {
                            "thinking..."
                        };
                        self.transcript.set_activity(Some(activity.into()));
                    }
                    SessionRuntimeStatus::SyncingInbox => {
                        self.transcript
                            .set_activity(Some("syncing inbox...".into()));
                    }
                    SessionRuntimeStatus::Compacting => {
                        self.transcript.set_activity(None);
                    }
                    SessionRuntimeStatus::Finalizing => {
                        self.transcript
                            .set_activity(Some(self.finalizing_activity_label()));
                    }
                    SessionRuntimeStatus::Open
                    | SessionRuntimeStatus::Error
                    | SessionRuntimeStatus::Closed => {
                        self.transcript.set_activity(None);
                    }
                }
            }
            SessionEvent::Warning { message } => {
                self.transcript.push_warning(format!("Warning: {message}"));
            }
            SessionEvent::ContextUsageUpdated { used_tokens } => {
                if self.status != SessionRuntimeStatus::Compacting {
                    self.context_used_tokens = Some(used_tokens);
                }
            }
            SessionEvent::UserMessageAccepted { text } => {
                self.network.clear_last_contribution();
                if self.pending_user_echo.is_some() {
                    // `begin_pending_turn` 已用 composer 原文创建活动用户气泡；引擎事件仅确认
                    // 接受，不能用模型侧展开文本再追加一条或替换展示内容。
                    self.pending_user_echo = None;
                } else {
                    self.transcript.push_user(text);
                }
            }
            SessionEvent::AssistantTextDelta { text } => {
                self.transcript.set_activity(None);
                self.transcript.push_assistant_delta(text);
            }
            SessionEvent::AssistantMessageCompleted { text } => {
                self.transcript.set_activity(None);
                self.transcript.complete_assistant_message(text);
            }
            SessionEvent::NonStreamingFallbackAttemptStarted {
                attempt,
                max_attempts,
            } => {
                self.transcript.set_activity(Some(format!(
                    "Falling Back to non-streaming · Retrying {attempt}/{max_attempts}..."
                )));
            }
            SessionEvent::NonStreamingFallbackSucceeded { text } => {
                self.transcript.complete_assistant_message(text);
                if self.status == SessionRuntimeStatus::Running {
                    self.transcript.set_activity(Some("thinking...".into()));
                }
            }
            SessionEvent::ToolCallStarted { id, name, summary } => {
                self.transcript.set_activity(None);
                self.transcript.push_tool_started(id, name, summary);
            }
            SessionEvent::ToolCallSkipped {
                id,
                name,
                summary,
                reason,
            } => {
                self.transcript.set_activity(None);
                self.transcript.push_tool_skipped(id, name, summary, reason);
            }
            SessionEvent::ToolCallProgress { id, summary } => {
                self.transcript.update_tool_progress(id, summary);
            }
            SessionEvent::ToolCallCompleted {
                id,
                summary,
                file_change,
                outcome,
            } => {
                self.network.record_tool_summary(&summary);
                self.transcript
                    .complete_tool(id, summary, file_change, outcome);
                if self.status == SessionRuntimeStatus::Running {
                    self.transcript.set_activity(Some("thinking...".into()));
                }
            }
            SessionEvent::ToolCallInterrupted { id, summary } => {
                self.capture_interrupted_background_process(&summary);
                self.transcript.interrupt_tool(id, summary);
            }
            SessionEvent::BackgroundProcessCompleted {
                process_id,
                owner_agent_id: _,
                owner_root_session_id: _,
                owner_subagent_id,
                status,
                exit_code,
                signal,
            } => {
                let owner = owner_subagent_id.as_deref().unwrap_or("main");
                let terminal_status = signal
                    .map(|signal| format!("signal {signal}"))
                    .or_else(|| exit_code.map(|code| format!("exit {code}")))
                    .unwrap_or_else(|| "exit unknown".into());
                self.push_system(format!(
                    "Background process ID={process_id} owner={owner} {status} ({terminal_status})"
                ));
            }
            SessionEvent::BackgroundProcessStarted { .. }
            | SessionEvent::BackgroundProcessOutput { .. }
            | SessionEvent::BackgroundProcessStateChanged { .. } => {
                // 生命周期信号用于 journal / 外部控制面；TUI 仅在 terminal completion 时写入
                // 一行稳定的 transcript，避免后台输出触发滚动噪声。
            }
            SessionEvent::UserShellCommandStarted { command } => {
                self.network.clear_last_contribution();
                self.status = SessionRuntimeStatus::Running;
                self.foreground_task_started_at = Some(Instant::now());
                self.shell_in_flight = true;
                self.transcript
                    .set_activity(Some("running shell command...".into()));
                self.transcript.push_shell_started(command);
            }
            SessionEvent::UserShellCommandCompleted {
                command,
                status,
                exit_code,
                duration_ms,
                stdout,
                stderr,
                truncated,
                message_count,
            } => {
                self.message_count = message_count;
                self.status = SessionRuntimeStatus::Open;
                self.foreground_task_started_at = None;
                self.shell_in_flight = false;
                self.transcript.set_activity(None);
                self.transcript.complete_shell(
                    command,
                    ShellCommandCompletion {
                        status,
                        exit_code,
                        duration_ms,
                        stdout,
                        stderr,
                        truncated,
                    },
                );
            }
            SessionEvent::UserShellCommandFailed { command, error } => {
                self.status = SessionRuntimeStatus::Open;
                self.foreground_task_started_at = None;
                self.shell_in_flight = false;
                self.transcript.set_activity(None);
                self.transcript.fail_shell(command, error);
            }
            SessionEvent::LocalClaimsUpdated { total } => {
                self.network.local_claims_total = Some(total);
            }
            SessionEvent::TurnCommitted { message_count } => {
                self.message_count = message_count;
                self.turn_count = self.turn_count.saturating_add(1);
                self.status = SessionRuntimeStatus::Open;
                self.foreground_task_started_at = None;
                self.turn_in_flight = false;
                self.committed_turn_finishing = true;
                self.transcript.set_activity(None);
                self.transcript.commit_active_turn();
                self.turn_animation.finish_success();
                self.pending_user_echo = None;
                self.clear_pending_tool_boundary_steer();
            }
            SessionEvent::TurnCancelled { reason } => {
                self.cancel_running_turn_without_restoring_queue(reason);
            }
            SessionEvent::TurnInterrupted { reason } => {
                self.interrupt_running_turn_for_steer(reason);
            }
            SessionEvent::TurnFailed { error } => {
                self.fail_running_turn_without_restoring_queue(error);
            }
            SessionEvent::FinalizeStarted => {
                self.transcript
                    .set_activity(Some(self.finalizing_activity_label()));
                self.push_system("Finalize started");
            }
            SessionEvent::FinalizeCompleted {
                trace_id,
                new_claim_ids,
                updated_claim_ids,
                new_dispute_ids,
            } => {
                self.transcript.set_activity(None);
                self.network.last_contribution = Some(ContributionSnapshot {
                    kind: ContributionKind::Finalize,
                    processed: None,
                    new_claims: new_claim_ids.len(),
                    updated_claims: updated_claim_ids.len(),
                    deprecated_claims: 0,
                    new_disputes: new_dispute_ids.len(),
                });
                self.push_system(format!(
                    "Finalize completed: trace={} new_claims={} updated_claims={} new_disputes={}",
                    trace_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "none".into()),
                    new_claim_ids.len(),
                    updated_claim_ids.len(),
                    new_dispute_ids.len()
                ));
            }
            SessionEvent::FinalizeFailed { error } => {
                self.transcript.set_activity(None);
                self.mark_finalize_failed();
                self.push_error(format!("Finalize failed: {error}"));
            }
            SessionEvent::CompactionStarted {
                compact_start_index: _,
                compact_end_index: _,
                recap_start_index: _,
                recap_end_index: _,
            } => {
                if self.status != SessionRuntimeStatus::Compacting {
                    self.foreground_task_started_at = Some(Instant::now());
                }
                self.status = SessionRuntimeStatus::Compacting;
                self.transcript.set_activity(None);
            }
            SessionEvent::CompactionCompleted {
                compacted_until: _,
                recapped_until: _,
                new_claim_ids,
                updated_claim_ids,
                used_claim_ids: _,
                new_dispute_ids,
            } => {
                if self.turn_in_flight {
                    self.status = SessionRuntimeStatus::Running;
                    self.foreground_task_started_at = Some(Instant::now());
                }
                self.transcript.set_activity(None);
                self.network.last_contribution = Some(ContributionSnapshot {
                    kind: ContributionKind::Compact,
                    processed: None,
                    new_claims: new_claim_ids.len(),
                    updated_claims: updated_claim_ids.len(),
                    deprecated_claims: 0,
                    new_disputes: new_dispute_ids.len(),
                });
            }
            SessionEvent::CompactionFailed { error } => {
                self.status = SessionRuntimeStatus::Error;
                self.foreground_task_started_at = None;
                self.transcript.set_activity(None);
                self.push_error(compaction_failure_message(error));
            }
            SessionEvent::InboxStarted => {
                self.transcript
                    .set_activity(Some("syncing inbox...".into()));
                self.transcript
                    .push_system_after_flushed_user("Inbox started");
            }
            SessionEvent::TeamServicesConnectionUpdated { status } => {
                self.network.team_services = status;
            }
            SessionEvent::InboxCompleted {
                processed,
                new_claim_ids,
                updated_claim_ids,
                new_dispute_ids,
                deprecated_claim_ids,
            } => {
                self.transcript.set_activity(None);
                self.network.last_contribution = Some(ContributionSnapshot {
                    kind: ContributionKind::Inbox,
                    processed: Some(processed),
                    new_claims: new_claim_ids.len(),
                    updated_claims: updated_claim_ids.len(),
                    deprecated_claims: deprecated_claim_ids.len(),
                    new_disputes: new_dispute_ids.len(),
                });
                self.push_system(format!(
                    "Inbox completed: processed={} new_claims={} updated_claims={} deprecated_claims={} new_disputes={}",
                    processed,
                    new_claim_ids.len(),
                    updated_claim_ids.len(),
                    deprecated_claim_ids.len(),
                    new_dispute_ids.len()
                ));
            }
            SessionEvent::InboxFailed { error } => {
                self.transcript.set_activity(None);
                self.push_error(format!("Inbox failed: {error}"));
            }
            SessionEvent::SessionClosed => {
                self.foreground_task_started_at = None;
                self.turn_in_flight = false;
                self.shell_in_flight = false;
                self.committed_turn_finishing = false;
                self.transcript.set_activity(None);
                let label = self.session_id.as_deref().unwrap_or("session");
                self.push_system(format!("Session {label} closed"));
            }
        }
    }

    pub fn status_label(&self) -> &'static str {
        match self.status {
            SessionRuntimeStatus::Initializing => "initializing",
            SessionRuntimeStatus::Open => "open",
            SessionRuntimeStatus::Running => "running",
            SessionRuntimeStatus::SyncingInbox => "syncing inbox",
            SessionRuntimeStatus::Compacting => "compacting",
            SessionRuntimeStatus::Finalizing => "finalizing",
            SessionRuntimeStatus::Error => "error",
            SessionRuntimeStatus::Closed => "closed",
        }
    }

    #[cfg(test)]
    pub fn transcript_text(&self) -> String {
        self.transcript.transcript_text()
    }

    pub fn input(&self) -> &str {
        self.bottom_pane.input()
    }

    pub(super) fn workspace_label(&self) -> &str {
        &self.workspace_label
    }

    pub(super) fn branch_label(&self) -> &str {
        &self.branch_label
    }

    /// 注入由 [`detect_workspace_context`] 在 spawn_blocking 中探测到的 workspace / git 分支标签。
    /// 注：branch 仅在启动探测一次，进程运行期切换分支不会刷新。
    pub(super) fn set_workspace_context(&mut self, workspace_label: String, branch_label: String) {
        self.workspace_label = workspace_label;
        self.branch_label = branch_label;
    }

    pub(super) fn focus_label(&self) -> String {
        format_focus_duration(self.focus_accumulated)
    }

    fn finalizing_activity_label(&self) -> String {
        match self.session_id.as_deref().filter(|id| !id.is_empty()) {
            Some(session_id) => format!("finalizing {session_id}..."),
            None => "finalizing session...".into(),
        }
    }

    pub(super) fn context_label(&self) -> String {
        format!(
            "{}k/{}",
            tokens_to_display_k(self.context_used_tokens.unwrap_or(0)),
            context_window_label(self.context_window_tokens)
        )
    }

    pub(super) fn set_context_window(&mut self, context_window_tokens: usize) {
        self.context_window_tokens = context_window_tokens;
    }

    pub(super) fn set_mcp_runtime(&mut self, config_path: PathBuf, snapshot: McpRuntimeState) {
        self.mcp_panel.set_runtime(config_path, snapshot);
    }

    pub(super) fn open_mcp_panel(&mut self) {
        self.delegation_panel.visible = false;
        self.process_panel.close();
        self.mcp_panel.open();
    }

    pub(super) fn mcp_panel_visible(&self) -> bool {
        self.mcp_panel.visible()
    }

    pub(super) fn open_process_panel(&mut self) {
        self.delegation_panel.visible = false;
        self.mcp_panel.close();
        self.process_panel.open();
    }

    pub(super) fn process_panel_visible(&self) -> bool {
        self.process_panel.visible()
    }

    pub(super) fn set_process_snapshots(
        &mut self,
        rows: Vec<crate::tool::ProcessSnapshot>,
    ) -> bool {
        self.process_panel.update(rows)
    }

    pub(super) fn set_process_panel_notice(&mut self, notice: impl Into<String>) {
        self.process_panel.set_notice(notice);
    }

    pub(super) fn mark_process_terminating(&mut self, target: &ProcessTerminationTarget) {
        self.process_panel.mark_terminating(target);
    }

    pub(super) fn handle_process_panel_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> ProcessPanelKeyAction {
        self.process_panel.handle_key(key)
    }

    pub(super) fn process_panel_lines(
        &self,
        width: u16,
        height: u16,
    ) -> Option<Vec<ratatui::text::Line<'static>>> {
        self.process_panel
            .visible()
            .then(|| self.process_panel.render_lines(width, height))
    }

    pub(super) fn set_mcp_notice(&mut self, notice: impl Into<String>) {
        self.mcp_panel.set_notice(notice);
    }

    pub(super) fn set_status_notice(&mut self, notice: impl Into<String>) {
        self.status_notice = Some(notice.into());
    }

    pub(super) fn clear_status_notice(&mut self) {
        self.status_notice = None;
    }

    pub(super) fn status_notice_text(&self) -> Option<&str> {
        self.status_notice.as_deref()
    }

    pub(super) fn clear_status_notice_for_new_turn(&mut self) {
        if self.status_notice_is_delegation_terminal()
            && self.delegation_panel_has_unfinished_work()
        {
            return;
        }
        self.clear_status_notice();
    }

    fn clear_settled_status_notice(&mut self) {
        if self.status_notice_is_delegation_terminal()
            && !self.delegation_panel.summaries.is_empty()
        {
            return;
        }
        self.clear_status_notice();
    }

    fn status_notice_is_delegation_terminal(&self) -> bool {
        self.status_notice
            .as_deref()
            .is_some_and(|notice| notice.trim_start().starts_with("Subagent "))
    }

    fn delegation_terminal_notice_text(&self) -> Option<&str> {
        self.status_notice
            .as_deref()
            .filter(|_| self.status_notice_is_delegation_terminal())
    }

    fn delegation_panel_has_unfinished_work(&self) -> bool {
        self.delegation_panel.summaries.iter().any(|summary| {
            matches!(
                summary.status,
                DelegationStatus::Queued | DelegationStatus::Running
            )
        })
    }

    pub(super) fn begin_mcp_request(&mut self, request: &McpPanelRequest) -> u64 {
        self.mcp_panel.begin_request(request)
    }

    pub(super) fn finish_mcp_request(
        &mut self,
        server_name: &str,
        operation_id: u64,
        outcome: McpOperationOutcome,
    ) -> bool {
        self.mcp_panel
            .finish_request(server_name, operation_id, outcome)
    }

    pub(super) fn handle_mcp_panel_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> McpPanelKeyAction {
        self.mcp_panel.handle_key(key)
    }

    pub(super) fn mcp_panel_lines(
        &self,
        width: u16,
        height: u16,
    ) -> Option<Vec<ratatui::text::Line<'static>>> {
        self.mcp_panel
            .visible()
            .then(|| self.mcp_panel.render_lines(width, height))
    }

    pub(super) fn set_delegation_summaries(&mut self, summaries: Vec<DelegationSummary>) -> bool {
        if self.delegation_panel.summaries == summaries && self.delegation_panel.error.is_none() {
            return false;
        }
        self.delegation_panel.summaries = summaries;
        self.delegation_panel.error = None;
        true
    }

    pub(super) fn set_delegation_snapshot_error(&mut self, error: impl Into<String>) -> bool {
        let error = error.into();
        if self.delegation_panel.summaries.is_empty()
            && self.delegation_panel.error.as_deref() == Some(error.as_str())
        {
            return false;
        }
        self.delegation_panel.summaries.clear();
        self.delegation_panel.error = Some(error);
        true
    }

    pub(super) fn open_delegation_panel(&mut self) {
        self.delegation_panel.visible = true;
        self.mcp_panel.close();
        self.process_panel.close();
        self.delegation_panel.scroll = 0;
    }

    pub(super) fn close_delegation_panel(&mut self) {
        self.delegation_panel.visible = false;
    }

    pub(super) fn delegation_panel_visible(&self) -> bool {
        self.delegation_panel.visible
    }

    pub(super) fn scroll_delegation_panel_up(&mut self, rows: usize) {
        self.delegation_panel.scroll = self.delegation_panel.scroll.saturating_sub(rows.max(1));
    }

    pub(super) fn scroll_delegation_panel_down(&mut self, rows: usize) {
        self.delegation_panel.scroll = self.delegation_panel.scroll.saturating_add(rows.max(1));
    }

    pub(super) fn scroll_delegation_panel_home(&mut self) {
        self.delegation_panel.scroll = 0;
    }

    pub(super) fn scroll_delegation_panel_end(&mut self) {
        self.delegation_panel.scroll = usize::MAX;
    }

    fn delegation_status_summary_text(&self, compact: bool) -> Option<String> {
        if self.delegation_panel.error.is_some() {
            return Some("Subagents: status unavailable".into());
        }
        let counts = DelegationStatusCounts::from_summaries(&self.delegation_panel.summaries);
        (counts.total > 0).then(|| {
            format!(
                "Subagents: {}",
                delegation_status_summary_label(&counts, compact)
            )
        })
    }

    /// 以宽度为输入生成完整或紧凑的后台状态区，保留进程在 subagent 前的固定顺序。
    pub(super) fn background_status_lines(
        &self,
        width: usize,
    ) -> Vec<ratatui::text::Line<'static>> {
        let width = width.max(1);
        let full_process = self.process_panel.background_status_text(false);
        let full_subagents = self.delegation_status_summary_text(false).map(|summary| {
            let mut parts = vec![summary];
            if let Some(notice) = self.delegation_terminal_notice_text() {
                parts.push(notice.to_string());
            }
            parts.push("/subagents".into());
            parts.join(" · ")
        });
        let compact = width < BACKGROUND_STATUS_COMPACT_WIDTH
            || full_process
                .as_ref()
                .is_some_and(|text| UnicodeWidthStr::width(text.as_str()) > width)
            || full_subagents
                .as_ref()
                .is_some_and(|text| UnicodeWidthStr::width(text.as_str()) > width);
        let mut lines = Vec::new();
        if let Some(process) = self.process_panel.background_status_text(compact) {
            lines.push(ratatui::text::Line::styled(process, blue_style()));
        }
        if compact {
            if let Some(summary) = self.delegation_status_summary_text(true) {
                lines.push(ratatui::text::Line::styled(
                    format!("{summary} · /subagents"),
                    blue_style(),
                ));
                if let Some(notice) = self.delegation_terminal_notice_text() {
                    lines.push(ratatui::text::Line::styled(
                        format!("↳ {notice}"),
                        blue_style(),
                    ));
                }
            }
        } else if let Some(subagents) = full_subagents {
            lines.push(ratatui::text::Line::styled(subagents, blue_style()));
        }
        if let Some(line) = self.status_notice_line() {
            lines.push(line);
        }
        lines
    }

    pub(super) fn delegation_panel_lines(
        &self,
        width: u16,
        height: u16,
    ) -> Option<Vec<ratatui::text::Line<'static>>> {
        self.delegation_panel
            .visible
            .then(|| render_delegation_panel(&self.delegation_panel, width, height))
    }

    pub(super) fn refresh_focus_timer(&mut self) {
        let now = Instant::now();
        let delta = focused_duration_between_samples(
            self.focus_last_sample,
            now,
            self.focus_task_is_active(),
            self.focus_last_user_activity,
        );
        self.focus_accumulated = self.focus_accumulated.saturating_add(delta);
        self.focus_last_sample = now;
    }

    pub(super) fn record_user_focus_activity(&mut self) {
        self.refresh_focus_timer();
        self.focus_last_user_activity = Some(Instant::now());
    }

    fn focus_task_is_active(&self) -> bool {
        self.turn_in_flight
            || matches!(
                self.status,
                SessionRuntimeStatus::Running
                    | SessionRuntimeStatus::SyncingInbox
                    | SessionRuntimeStatus::Compacting
                    | SessionRuntimeStatus::Finalizing
            )
    }

    #[cfg(test)]
    pub(super) fn set_focus_duration_for_test(&mut self, duration: Duration) {
        self.focus_accumulated = duration;
        self.focus_last_sample = Instant::now();
    }

    pub fn push_input_char(&mut self, c: char) {
        self.bottom_pane.push_char(c);
        self.bump_input_revision();
    }

    #[cfg(test)]
    pub fn push_input_text(&mut self, text: &str) {
        self.bottom_pane.push_text(text);
        self.bump_input_revision();
    }

    pub fn push_pasted_text(&mut self, text: &str) {
        self.bottom_pane.push_paste_text(text);
        self.bump_input_revision();
    }

    pub(super) fn set_attachment_config(&mut self, attachment_cfg: AttachmentConfig) {
        self.bottom_pane
            .set_at_path_highlight(attachment_cfg.enabled);
        self.attachment_cfg = attachment_cfg;
    }

    pub(super) fn set_at_path_completion_config(
        &mut self,
        workspace_root: PathBuf,
        limits: AtPathCompletionLimits,
    ) {
        self.bottom_pane
            .set_at_path_completion_config(workspace_root, limits);
    }

    pub(super) fn begin_at_path_scan(&mut self) -> Option<(u64, PathBuf, usize)> {
        let (directory, max_entries) = self.bottom_pane.at_path_scan_request()?;
        self.at_path_scan_generation = self.at_path_scan_generation.saturating_add(1);
        let generation = self.at_path_scan_generation;
        if !self.bottom_pane.mark_at_path_scan_started(&directory) {
            return None;
        }
        self.pending_at_path_scans.insert(generation);
        Some((generation, directory, max_entries))
    }

    pub(super) fn apply_at_path_directory_read(
        &mut self,
        generation: u64,
        directory: PathBuf,
        result: Result<Vec<AtPathDirectoryEntry>, String>,
    ) -> bool {
        if !self.pending_at_path_scans.remove(&generation) {
            return false;
        }
        self.bottom_pane
            .apply_at_path_directory_read(directory, result)
    }

    pub(super) fn attachment_limits(&self) -> AttachmentLimits {
        AttachmentLimits::from(&self.attachment_cfg)
    }

    pub(super) fn attachments_enabled(&self) -> bool {
        self.attachment_cfg.enabled
    }

    pub(super) fn at_path_workspace_root(&self) -> &Path {
        self.bottom_pane.at_path_workspace_root()
    }

    /// Ctrl+V 的同步预检：功能开关与数量上限。通过后由调用方发起
    /// spawn_blocking 读剪贴板，结果经 `AppEvent::ClipboardImageRead` 回灌。
    pub(super) fn begin_clipboard_image_read(
        &self,
    ) -> Result<Option<(AttachmentLimits, u64)>, AttachmentError> {
        if !self.attachment_cfg.enabled || !self.attachment_cfg.clipboard_image_enabled {
            return Ok(None);
        }
        let limits = self.attachment_limits();
        let next_count = self
            .bottom_pane
            .effective_attachment_count()
            .saturating_add(self.pending_clipboard_image_reads)
            .saturating_add(1);
        if next_count > limits.max_files_per_turn {
            return Err(AttachmentError::TooManyFiles {
                actual: next_count,
                limit: limits.max_files_per_turn,
            });
        }
        Ok(Some((limits, self.input_revision)))
    }

    pub(super) fn mark_clipboard_image_read_started(&mut self) {
        self.pending_clipboard_image_reads = self.pending_clipboard_image_reads.saturating_add(1);
    }

    /// 把规格化完成的剪贴板图片挂成 `[Image #N]` 附件。
    pub(super) fn apply_clipboard_image_read(
        &mut self,
        input_revision: u64,
        result: Result<Option<NormalizedMedia>, String>,
    ) {
        self.pending_clipboard_image_reads = self.pending_clipboard_image_reads.saturating_sub(1);
        match result {
            Ok(Some(media)) if input_revision == self.input_revision => {
                self.bottom_pane.push_clipboard_image(media);
            }
            Ok(Some(_)) => self.push_system("输入内容已变化，请重新添加图片"),
            Ok(None) => self.push_system("剪贴板中没有图片，文本粘贴请用 Cmd+V"),
            Err(message) => self.push_error(format!("Clipboard attach failed: {message}")),
        }
    }

    pub(super) fn restore_input_drafts_preserving_current(&mut self, drafts: Vec<InputDraft>) {
        let current = self.bottom_pane.take_draft();
        let mut iter = drafts.into_iter().filter(|draft| !draft.is_visible_empty());
        let mut merged = match iter.next() {
            Some(draft) => draft,
            None => {
                if !current.is_visible_empty() {
                    self.bottom_pane.set_draft(current);
                    self.bump_input_revision();
                }
                return;
            }
        };
        for draft in iter {
            merged.append_with_newline(draft);
        }
        if !current.is_visible_empty() {
            merged.append_with_newline(current);
        }
        self.bottom_pane.set_draft(merged);
        self.bump_input_revision();
    }

    pub(super) fn next_input_submission_sequence(&mut self) -> u64 {
        let sequence = self.next_input_submission_sequence;
        self.next_input_submission_sequence = self.next_input_submission_sequence.saturating_add(1);
        sequence
    }

    pub(super) fn current_input_submission_sequence(&self) -> u64 {
        self.next_input_submission_sequence
    }

    /// Ctrl+O：光标处附件的预览命中判定。
    pub(super) fn preview_target_at_cursor(&self) -> super::bottom_pane::PreviewHit {
        self.bottom_pane.preview_target_at_cursor()
    }

    pub fn push_input_newline(&mut self) {
        self.bottom_pane.push_newline();
        self.bump_input_revision();
    }

    pub fn pop_input_char(&mut self) {
        let before = self.bottom_pane.current_draft();
        self.bottom_pane.pop_char();
        self.bump_input_revision_if_draft_changed(before);
    }

    pub fn delete_input_char(&mut self) {
        let before = self.bottom_pane.current_draft();
        self.bottom_pane.delete_char();
        self.bump_input_revision_if_draft_changed(before);
    }

    pub fn move_input_left(&mut self) {
        self.bottom_pane.move_left();
    }

    pub fn move_input_right(&mut self) {
        self.bottom_pane.move_right();
    }

    pub fn move_input_word_left(&mut self) {
        self.bottom_pane.move_word_left();
    }

    pub fn move_input_word_right(&mut self) {
        self.bottom_pane.move_word_right();
    }

    pub fn move_input_home(&mut self) {
        self.bottom_pane.move_home();
    }

    pub fn move_input_end(&mut self) {
        self.bottom_pane.move_end();
    }

    pub(super) fn move_input_up(&mut self, width: u16) -> bool {
        self.bottom_pane.move_up(width)
    }

    pub(super) fn move_input_down(&mut self, width: u16) -> bool {
        self.bottom_pane.move_down(width)
    }

    pub(super) fn slash_menu_visible(&self) -> bool {
        self.bottom_pane.slash_menu_visible()
    }

    pub(super) fn at_path_menu_visible(&self) -> bool {
        self.bottom_pane.at_path_menu_visible()
    }

    pub(super) fn dismiss_at_path_menu(&mut self) -> bool {
        self.bottom_pane.dismiss_at_path_menu()
    }

    /// 注入 workspace skills 到 slash 命令目录（App 启动时调用一次）。
    pub fn set_slash_skills<'a>(&mut self, skills: impl IntoIterator<Item = (&'a str, &'a str)>) {
        self.bottom_pane.set_slash_skills(skills);
    }

    pub(super) fn slash_catalog(&self) -> &super::slash_command::SlashCommandCatalog {
        self.bottom_pane.slash_catalog()
    }

    pub(super) fn accept_inline_slash_hint(&mut self) -> bool {
        let accepted = self.bottom_pane.accept_inline_slash_hint();
        if accepted {
            self.bump_input_revision();
        }
        accepted
    }

    pub(super) fn select_previous_slash_completion(&mut self) -> bool {
        self.bottom_pane.select_previous_slash_completion()
    }

    pub(super) fn select_next_slash_completion(&mut self) -> bool {
        self.bottom_pane.select_next_slash_completion()
    }

    pub(super) fn select_previous_at_path_completion(&mut self) -> bool {
        self.bottom_pane.select_previous_at_path_completion()
    }

    pub(super) fn select_next_at_path_completion(&mut self) -> bool {
        self.bottom_pane.select_next_at_path_completion()
    }

    pub(super) fn accept_at_path_completion(&mut self) -> bool {
        let accepted = self.bottom_pane.accept_at_path_completion();
        if accepted {
            self.bump_input_revision();
        }
        accepted
    }

    pub(super) fn accept_slash_completion(&mut self) -> bool {
        let accepted = self.bottom_pane.accept_slash_completion();
        if accepted {
            self.bump_input_revision();
        }
        accepted
    }

    pub(super) fn input_cursor_at_end(&self) -> bool {
        self.bottom_pane.cursor_at_end()
    }

    #[cfg(test)]
    pub fn take_input(&mut self) -> String {
        let before = self.bottom_pane.current_draft();
        let input = self.bottom_pane.take();
        self.bump_input_revision_if_draft_changed(before);
        input
    }

    pub(super) fn take_input_draft(&mut self) -> InputDraft {
        let before = self.bottom_pane.current_draft();
        let draft = self.bottom_pane.take_draft();
        self.bump_input_revision_if_draft_changed(before);
        draft
    }

    pub(super) fn clear_input(&mut self) {
        let before = self.bottom_pane.current_draft();
        self.bottom_pane.clear_input();
        self.bump_input_revision_if_draft_changed(before);
    }

    pub(super) fn record_submitted_draft(&mut self, draft: InputDraft) {
        self.bottom_pane.record_submitted_draft(draft);
    }

    pub(super) fn push_command_echo(&mut self, text: String) {
        self.transcript.push_user(text);
    }

    pub(super) fn push_failed_input(&mut self, text: String, error: impl Into<String>) {
        self.transcript.push_user(text);
        self.transcript.push_error(error);
    }

    pub(super) fn input_accepts_text(&self) -> bool {
        !self.bottom_pane.finalize_failed() && super::bottom_pane::input_accepts_text(self.status)
    }

    pub(super) fn finalize_failed(&self) -> bool {
        self.bottom_pane.finalize_failed()
    }

    pub(super) fn mark_finalize_failed(&mut self) {
        self.bottom_pane.set_finalize_failed(true);
    }

    pub(super) fn recall_previous_input(&mut self) -> bool {
        let recalled = self.bottom_pane.recall_previous_input();
        if recalled {
            self.bump_input_revision();
        }
        recalled
    }

    pub(super) fn recall_next_input(&mut self) -> bool {
        let recalled = self.bottom_pane.recall_next_input();
        if recalled {
            self.bump_input_revision();
        }
        recalled
    }

    pub fn push_help(&mut self) {
        self.transcript.push_help();
    }

    pub fn push_error(&mut self, message: impl Into<String>) {
        self.transcript.push_error(message);
    }

    pub fn push_system(&mut self, message: impl Into<String>) {
        self.transcript.push_system(message);
    }

    pub(super) fn last_committed_assistant_text(&self) -> Option<&str> {
        self.transcript.last_committed_assistant_text()
    }

    pub(super) fn reset_for_resumed_session(&mut self) {
        self.transcript.clear();
        self.network = NetworkSnapshot::default();
        self.context_used_tokens = None;
        self.turn_animation.reset();
        self.pending_user_echo = None;
        self.clear_pending_tool_boundary_steer();
        self.interrupted_background_processes.clear();
        self.clear_status_notice();
        self.turn_in_flight = false;
        self.committed_turn_finishing = false;
        self.shell_in_flight = false;
        self.foreground_task_started_at = None;
        self.bottom_pane.set_finalize_failed(false);
        self.start_separator_flushed = false;
        self.delegation_panel = DelegationPanelState::default();
        self.process_panel = ProcessPanelState::default();
    }

    #[cfg(test)]
    pub(super) fn push_historical_turns(&mut self, turns: &[HistoricalTurn]) {
        for turn in turns {
            self.transcript.push_user(turn.user_text.clone());
            if let Some(text) = &turn.assistant_text {
                self.transcript.push_assistant_delta(text.clone());
                self.transcript.complete_assistant_message(text.clone());
            }
        }
    }

    pub(super) fn push_historical_timeline_turns(&mut self, turns: &[HistoricalTimelineTurn]) {
        for turn in turns {
            self.transcript.push_user(turn.user_text.clone());
            if turn.timeline_items.is_empty() {
                self.push_legacy_historical_timeline_turn(turn);
            } else {
                for item in &turn.timeline_items {
                    match item {
                        TurnJournalTimelineItem::Assistant { text, completed } => {
                            if *completed {
                                self.transcript.push_assistant_delta(text.clone());
                                self.transcript.complete_assistant_message(text.clone());
                            } else {
                                self.transcript.push_historical_assistant(text.clone());
                            }
                        }
                        TurnJournalTimelineItem::ToolCall(tool) => {
                            self.push_historical_tool_call(tool);
                        }
                    }
                }
            }
            if let Some(notice) = turn.recovery_notice.as_deref() {
                self.push_system(format!("Recovery notice: {notice}"));
            }
            for steer in &turn.user_steers {
                self.push_system(format!("User steer: {steer}"));
            }
            if let Some(detail) = turn.turn_status_detail.as_deref() {
                self.push_system(uppercase_ascii_initial(detail));
            } else if let Some(status) = turn
                .status
                .filter(|status| *status != TurnJournalStatus::Committed)
            {
                self.push_system(format!("Turn {}", status.as_str()));
            } else if !turn.assistant_completed && turn.assistant_text.is_some() {
                self.push_system("Turn partial assistant replayed");
            }
        }
    }

    fn push_legacy_historical_timeline_turn(&mut self, turn: &HistoricalTimelineTurn) {
        let assistant_after_tools = turn.status == Some(TurnJournalStatus::Committed)
            && turn.assistant_completed
            && !turn.tool_calls.is_empty();
        if !assistant_after_tools {
            if let Some(text) = &turn.assistant_text {
                self.transcript.push_assistant_delta(text.clone());
                self.transcript.complete_assistant_message(text.clone());
            }
        }
        for tool in &turn.tool_calls {
            self.push_historical_tool_call(tool);
        }
        if assistant_after_tools {
            if let Some(text) = &turn.assistant_text {
                self.transcript.push_assistant_delta(text.clone());
                self.transcript.complete_assistant_message(text.clone());
            }
        }
    }

    fn push_historical_tool_call(&mut self, tool: &crate::session::TurnJournalToolCall) {
        if let Some(skipped) = &tool.skipped_summary {
            self.transcript.push_tool_skipped(
                tool.tool_use_id.clone(),
                tool.name.clone(),
                skipped.clone(),
                tool.skip_reason
                    .unwrap_or(ToolCallSkipReason::TurnInterruptedBeforeDispatch),
            );
            return;
        }
        self.transcript.push_tool_started(
            tool.tool_use_id.clone(),
            tool.name.clone(),
            tool.started_summary.clone(),
        );
        if let Some(progress) = &tool.latest_progress {
            self.transcript
                .update_tool_progress(tool.tool_use_id.clone(), progress.clone());
        }
        if let Some(completed) = &tool.completed_summary {
            let outcome = tool
                .outcome
                .unwrap_or_else(|| Self::legacy_tool_outcome_from_completed_summary(completed));
            self.transcript.complete_tool(
                tool.tool_use_id.clone(),
                completed.clone(),
                tool.file_change.clone(),
                outcome,
            );
        } else if let Some(interrupted) = &tool.interrupted_summary {
            self.transcript
                .interrupt_tool(tool.tool_use_id.clone(), interrupted.clone());
        } else {
            // resume 历史不能留下 live ToolCell，否则后续 recovery/status 会被挡在
            // scrollback 之外。缺少 completed 事件时按降级失败收口，且不渲染 diff。
            self.transcript.complete_tool(
                tool.tool_use_id.clone(),
                format!("tool {} failed Journal replay incomplete", tool.name),
                None,
                ToolExecutionOutcome::DispatchFailure,
            );
        }
    }

    /// 仅供缺少 outcome 的旧 turn journal 使用；新事件必须携带 typed outcome。
    fn legacy_tool_outcome_from_completed_summary(summary: &str) -> ToolExecutionOutcome {
        let status = summary
            .strip_prefix("tool ")
            .and_then(|rest| rest.split_once(' '))
            .map(|(_, status_and_detail)| status_and_detail)
            .unwrap_or(summary);
        if status == "failed" || status.starts_with("failed ") {
            ToolExecutionOutcome::BusinessFailure
        } else {
            ToolExecutionOutcome::Completed
        }
    }

    pub fn begin_pending_turn(&mut self, text: String) {
        self.refresh_focus_timer();
        self.clear_status_notice_for_new_turn();
        self.network.clear_last_contribution();
        self.pending_user_echo = Some(text.clone());
        self.clear_pending_tool_boundary_steer();
        self.interrupted_background_processes.clear();
        self.status = SessionRuntimeStatus::Running;
        self.foreground_task_started_at = Some(Instant::now());
        self.turn_in_flight = true;
        self.committed_turn_finishing = false;
        self.turn_animation.begin_turn();
        self.transcript.set_activity(Some("thinking...".into()));
        self.transcript.push_active_user(text);
    }

    pub fn queue_pending_turn(&mut self, input: impl Into<QueuedInput>) {
        self.input_queue.enqueue(input.into());
    }

    pub(super) fn set_pending_tool_boundary_steer(&mut self, text: Option<String>) {
        self.pending_tool_boundary_steer = text.filter(|text| !text.trim().is_empty());
    }

    pub(super) fn clear_pending_tool_boundary_steer(&mut self) {
        self.pending_tool_boundary_steer = None;
    }

    pub(super) fn pending_tool_boundary_steer_lines(
        &self,
        width: u16,
    ) -> Vec<ratatui::text::Line<'static>> {
        let Some(text) = &self.pending_tool_boundary_steer else {
            return Vec::new();
        };
        let mut lines = user_text_display_lines(text, width);
        while lines
            .last()
            .is_some_and(|line| line.spans.is_empty() && line.width() == 0)
        {
            lines.pop();
        }
        lines
    }

    pub fn mark_turn_finished(&mut self) {
        self.refresh_focus_timer();
        self.turn_in_flight = false;
        self.committed_turn_finishing = false;
        self.foreground_task_started_at = None;
        self.clear_pending_tool_boundary_steer();
        self.clear_settled_status_notice();
    }

    #[cfg(test)]
    pub fn fail_running_turn(&mut self, error: impl Into<String>) {
        self.fail_running_turn_inner(error, true);
    }

    pub(super) fn fail_running_turn_without_restoring_queue(&mut self, error: impl Into<String>) {
        self.fail_running_turn_inner(error, false);
    }

    fn fail_running_turn_inner(&mut self, error: impl Into<String>, restore_queue: bool) {
        self.refresh_focus_timer();
        self.status = SessionRuntimeStatus::Error;
        self.foreground_task_started_at = None;
        self.turn_in_flight = false;
        self.committed_turn_finishing = false;
        self.turn_animation.cancel_turn();
        self.transcript.set_activity(None);
        self.pending_user_echo = None;
        self.clear_pending_tool_boundary_steer();
        self.transcript.release_active_user();
        self.transcript.clear_active_assistant();
        if restore_queue {
            self.restore_queued_inputs_to_composer();
        }
        self.transcript
            .push_turn_error(turn_failure_message(error.into()));
    }

    #[cfg(test)]
    pub fn cancel_running_turn(&mut self, reason: impl Into<String>) {
        self.cancel_running_turn_inner(reason, true);
    }

    fn cancel_running_turn_without_restoring_queue(&mut self, reason: impl Into<String>) {
        self.cancel_running_turn_inner(reason, false);
    }

    fn cancel_running_turn_inner(&mut self, reason: impl Into<String>, restore_queue: bool) {
        self.refresh_focus_timer();
        self.status = SessionRuntimeStatus::Open;
        self.foreground_task_started_at = None;
        self.turn_in_flight = false;
        self.committed_turn_finishing = false;
        self.turn_animation.cancel_turn();
        self.transcript.set_activity(None);
        self.pending_user_echo = None;
        self.clear_pending_tool_boundary_steer();
        self.transcript.release_active_user();
        self.transcript.clear_active_assistant();
        if restore_queue {
            self.restore_queued_inputs_to_composer();
        }
        let reason = reason.into();
        if !self.interrupted_background_processes.is_empty() {
            let process_ids = self
                .interrupted_background_processes
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            let message = if process_ids.len() == 1 {
                format!(
                    "Interrupted · process {} continues in background",
                    process_ids[0]
                )
            } else {
                format!(
                    "Interrupted · processes {} continue in background",
                    process_ids.join(" / ")
                )
            };
            self.push_system(message);
            self.interrupted_background_processes.clear();
        } else if reason.trim().is_empty() {
            self.push_system("Turn cancelled");
        } else {
            self.push_system(format!("Turn cancelled: {reason}"));
        }
    }

    pub(super) fn prepare_failed_turn_without_restoring_queue(&mut self) {
        self.refresh_focus_timer();
        self.turn_in_flight = false;
        self.committed_turn_finishing = false;
        self.foreground_task_started_at = None;
        self.turn_animation.cancel_turn();
        self.transcript.set_activity(None);
        self.pending_user_echo = None;
        self.clear_pending_tool_boundary_steer();
        self.transcript.release_active_user();
        self.transcript.clear_active_assistant();
    }

    pub fn interrupt_running_turn_for_steer(&mut self, reason: impl Into<String>) {
        self.refresh_focus_timer();
        self.status = SessionRuntimeStatus::Open;
        self.foreground_task_started_at = None;
        self.turn_in_flight = false;
        self.committed_turn_finishing = false;
        self.turn_animation.cancel_turn();
        self.transcript.set_activity(None);
        self.pending_user_echo = None;
        self.clear_pending_tool_boundary_steer();
        self.interrupted_background_processes.clear();
        self.transcript.release_active_user();
        self.transcript.clear_active_assistant();
        let reason = reason.into();
        if reason.trim().is_empty() {
            self.push_system("Turn interrupted");
        } else {
            self.push_system(format!("Turn interrupted: {reason}"));
        }
    }

    fn capture_interrupted_background_process(&mut self, summary: &str) {
        let Some(process_id) = summary
            .strip_prefix("Interrupted · process ")
            .and_then(|rest| rest.strip_suffix(" continues in background"))
        else {
            return;
        };
        if process_id.len() == 8 && process_id.chars().all(|ch| ch.is_ascii_hexdigit()) {
            self.interrupted_background_processes
                .insert(process_id.to_string());
        }
    }

    pub fn restore_queued_inputs_to_composer(&mut self) {
        if self.input_queue.is_empty() {
            return;
        }
        let before = self.bottom_pane.current_draft();
        let current_draft = self.bottom_pane.take_draft();
        if let Some(restored) = self.input_queue.drain_for_restore(current_draft) {
            self.bottom_pane.set_draft(restored);
        }
        self.bump_input_revision_if_draft_changed(before);
    }

    pub(super) fn drain_queued_inputs_for_restore_before(
        &mut self,
        restore_before: u64,
    ) -> Vec<QueuedInput> {
        self.input_queue
            .drain_inputs_for_restore_before(restore_before)
    }

    pub(super) fn restore_latest_queued_input_to_composer(&mut self) -> bool {
        let Some(input) = self.input_queue.pop_latest() else {
            return false;
        };
        let before = self.bottom_pane.current_draft();
        self.bottom_pane.set_draft(input.into_draft());
        self.bump_input_revision_if_draft_changed(before);
        true
    }

    pub fn has_turn_in_flight(&self) -> bool {
        self.turn_in_flight
    }

    pub(super) fn slash_steer_notice_should_queue(&self) -> bool {
        self.turn_in_flight || self.committed_turn_finishing
    }

    pub fn has_interruptible_task_in_flight(&self) -> bool {
        self.turn_in_flight || self.shell_in_flight
    }

    pub(super) fn shell_in_flight(&self) -> bool {
        self.shell_in_flight
    }

    pub(super) fn running_task_label(&self) -> Option<&'static str> {
        if self.shell_in_flight {
            Some("shell command")
        } else if self.turn_in_flight {
            Some("turn")
        } else {
            None
        }
    }

    pub(super) fn turn_animation_is_active(&self) -> bool {
        self.turn_animation_can_tick() && self.turn_animation.is_active()
    }

    pub(super) fn settle_turn_animation_before_command(&mut self) {
        self.turn_animation.complete_finalizing_drop();
    }

    pub(super) fn tick_turn_animation(&mut self, width: u16, height_budget: usize) -> bool {
        if !self.turn_animation_can_tick() {
            return false;
        }
        self.turn_animation.tick_if_visible(width, height_budget)
    }

    pub fn pop_queued_turn(&mut self) -> Option<QueuedInput> {
        self.input_queue.pop_next()
    }

    #[cfg(test)]
    pub fn queued_count(&self) -> usize {
        self.input_queue.len()
    }

    pub(super) fn bottom_pane(&self) -> &BottomPane {
        &self.bottom_pane
    }

    pub(super) fn pending_input_preview(&self) -> PendingInputPreview {
        self.input_queue.preview()
    }

    pub(super) fn queued_count_for_render(&self) -> usize {
        self.input_queue.queued_count()
    }

    pub(super) fn history_render_lines_with_width(
        &self,
        width: u16,
    ) -> Vec<ratatui::text::Line<'static>> {
        self.transcript.render_lines_with_width(width)
    }

    pub(super) fn scrollback_lines(&self, width: u16) -> ScrollbackLines {
        self.transcript.scrollback_lines(width)
    }

    pub(super) fn mark_scrollback_flushed(&mut self, entry_count: usize) {
        self.transcript.mark_scrollback_flushed(entry_count);
    }

    /// 全屏重排（hard_clear）前重置 flush 游标，使下一帧把整段历史按当前宽度重新 emit。
    ///
    /// 该函数必须且只能与 terminal 的 Purge+Clear 全屏重排同帧使用：hard_clear 会 Purge
    /// 掉终端 scrollback（含 welcome 横幅），因此这里**无条件**把 start_separator_flushed
    /// 置回 false，让横幅随历史一起重发，保证重排后 scrollback 顶部仍有欢迎卡片；否则会向
    /// 已清空的 scrollback 漏掉横幅 / 或（若不 Purge）向其重复 append 历史。
    pub(super) fn reset_flushed_for_hard_clear(&mut self) {
        self.transcript.reset_scrollback_flushed();
        self.start_separator_flushed = false;
    }

    pub(super) fn status_notice_line(&self) -> Option<ratatui::text::Line<'static>> {
        // subagent 的最新终态 notice 已合并到 `Subagents:` 行，不能在底栏重复占一行。
        if self.status_notice_is_delegation_terminal()
            && (self.delegation_panel.error.is_some()
                || !self.delegation_panel.summaries.is_empty())
        {
            return None;
        }
        self.status_notice
            .as_ref()
            .map(|notice| ratatui::text::Line::styled(notice.clone(), blue_style()))
    }

    pub(super) fn active_timeline_lines(&self, width: u16) -> Vec<ratatui::text::Line<'static>> {
        self.transcript.active_timeline_lines(width)
    }

    #[cfg(test)]
    pub(super) fn active_assistant_lines(&self, width: u16) -> Vec<ratatui::text::Line<'static>> {
        self.transcript.active_assistant_lines(width)
    }

    pub(super) fn has_active_user(&self) -> bool {
        self.transcript.has_active_user()
    }

    pub(super) fn network_snapshot(&self) -> &NetworkSnapshot {
        &self.network
    }

    pub(super) fn team_services_connection_status(&self) -> TeamServicesConnectionStatus {
        self.network.team_services
    }

    pub(super) fn running_turn_animation_lines(
        &self,
        width: u16,
        height_budget: usize,
    ) -> Vec<ratatui::text::Line<'static>> {
        if !self.turn_animation_can_display() || !self.turn_animation.has_visible_board() {
            return Vec::new();
        }
        self.turn_animation.render_lines(width, height_budget)
    }

    fn turn_animation_can_tick(&self) -> bool {
        matches!(
            self.status,
            SessionRuntimeStatus::Running | SessionRuntimeStatus::Open
        )
    }

    fn turn_animation_can_display(&self) -> bool {
        matches!(
            self.status,
            SessionRuntimeStatus::Running
                | SessionRuntimeStatus::Open
                | SessionRuntimeStatus::Error
        )
    }

    pub(super) fn start_separator_pending(&self) -> bool {
        !self.start_separator_flushed
    }

    pub(super) fn mark_start_separator_flushed(&mut self) {
        self.start_separator_flushed = true;
    }
}

fn compaction_failure_message(error: String) -> String {
    if error.starts_with("Compaction failed repeatedly.") {
        error
    } else {
        format!("Compaction failed: {error}")
    }
}

fn turn_failure_message(error: String) -> String {
    if error.starts_with("Context compaction failed:") {
        error
    } else {
        format!("Turn failed: {error}")
    }
}

fn uppercase_ascii_initial(text: &str) -> String {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    if first.is_ascii_lowercase() {
        format!(
            "{}{rest}",
            first.to_ascii_uppercase(),
            rest = chars.as_str()
        )
    } else {
        text.to_string()
    }
}

fn render_delegation_panel(
    panel: &DelegationPanelState,
    width: u16,
    height: u16,
) -> Vec<ratatui::text::Line<'static>> {
    let width = usize::from(width.max(1));
    let height = usize::from(height);
    if height == 0 {
        return Vec::new();
    }
    let layout = delegation_table_layout(width);
    let mut lines = Vec::new();
    lines.push(ratatui::text::Line::styled(
        truncate_display("Session Subagents  read-only", width),
        blue_style(),
    ));
    if height == 1 {
        return lines;
    }
    if panel.summaries.is_empty() {
        lines.push(ratatui::text::Line::default());
    } else {
        lines.push(ratatui::text::Line::styled(
            delegation_panel_row(
                "Hash",
                "Status",
                "Title",
                "Role",
                "Update_time",
                "Latest",
                &layout,
            ),
            muted_style(),
        ));
    }
    if height == 2 {
        return lines;
    }

    let body = delegation_panel_body_lines(panel, &layout, width);
    let body_budget = height.saturating_sub(lines.len());
    let max_scroll = body.len().saturating_sub(body_budget);
    let scroll = panel.scroll.min(max_scroll);
    lines.extend(body.into_iter().skip(scroll).take(body_budget));
    while lines.len() < height {
        lines.push(ratatui::text::Line::default());
    }
    lines
}

#[derive(Default)]
struct DelegationStatusCounts {
    queued: usize,
    running: usize,
    completed: usize,
    failed: usize,
    abandoned: usize,
    total: usize,
}

impl DelegationStatusCounts {
    fn from_summaries(summaries: &[DelegationSummary]) -> Self {
        let mut counts = Self::default();
        for summary in summaries {
            counts.total = counts.total.saturating_add(1);
            match summary.status {
                DelegationStatus::Queued => counts.queued = counts.queued.saturating_add(1),
                DelegationStatus::Running => counts.running = counts.running.saturating_add(1),
                DelegationStatus::Completed => {
                    counts.completed = counts.completed.saturating_add(1);
                }
                DelegationStatus::Failed => counts.failed = counts.failed.saturating_add(1),
                DelegationStatus::Abandoned => {
                    counts.abandoned = counts.abandoned.saturating_add(1);
                }
            }
        }
        counts
    }
}

fn delegation_status_summary_label(counts: &DelegationStatusCounts, compact: bool) -> String {
    let mut parts = Vec::new();
    push_status_part(
        &mut parts,
        counts.completed,
        if compact { "done" } else { "completed" },
    );
    push_status_part(&mut parts, counts.failed, "failed");
    push_status_part(&mut parts, counts.abandoned, "abandoned");
    push_status_part(
        &mut parts,
        counts.running,
        if compact { "run" } else { "running" },
    );
    push_status_part(&mut parts, counts.queued, "queued");
    parts.join(" · ")
}

fn push_status_part(parts: &mut Vec<String>, count: usize, label: &str) {
    if count > 0 {
        parts.push(format!("{count} {label}"));
    }
}

fn delegation_panel_body_lines(
    panel: &DelegationPanelState,
    layout: &DelegationTableLayout,
    width: usize,
) -> Vec<ratatui::text::Line<'static>> {
    let mut lines = Vec::new();
    if let Some(error) = panel.error.as_deref() {
        lines.push(ratatui::text::Line::styled(
            truncate_display(
                &format!("Subagent snapshot unavailable: {}", single_line(error)),
                width,
            ),
            muted_style(),
        ));
        return lines;
    }
    if panel.summaries.is_empty() {
        lines.push(ratatui::text::Line::from(truncate_display(
            "No subagents in this session.",
            width,
        )));
        return lines;
    }
    for summary in &panel.summaries {
        let detail = delegation_primary_detail(summary);
        let updated = summary
            .updated_at
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let row = delegation_panel_row_spans(
            delegation_hash(summary.id.as_str()),
            delegation_status_label(summary.status),
            summary.status,
            &summary.title,
            &summary.role,
            &updated,
            &detail,
            layout,
        );
        lines.push(Line::from(row));
        for detail in delegation_detail_lines(summary) {
            lines.push(ratatui::text::Line::styled(
                truncate_display(&format!("  {detail}"), width),
                muted_style(),
            ));
        }
    }
    lines
}

fn delegation_panel_row(
    hash: &str,
    status: &str,
    title: &str,
    role: &str,
    update_time: &str,
    detail: &str,
    layout: &DelegationTableLayout,
) -> String {
    let hash = pad_cell(&single_line(hash), layout.hash);
    let status = pad_cell(&single_line(status), layout.status);
    let title = pad_cell(&single_line(title), layout.title);
    let role = pad_cell(&single_line(role), layout.role);
    let update_time = pad_cell(&single_line(update_time), layout.update_time);
    let detail = truncate_display(&single_line(detail), layout.latest);
    format!("{hash}  {status}  {title}  {role}  {update_time}  {detail}")
}

#[allow(
    clippy::too_many_arguments,
    reason = "delegation table renderer keeps individual columns explicit to preserve alignment"
)]
fn delegation_panel_row_spans(
    hash: &str,
    status_label: &str,
    status_kind: DelegationStatus,
    title: &str,
    role: &str,
    update_time: &str,
    detail: &str,
    layout: &DelegationTableLayout,
) -> Vec<Span<'static>> {
    let hash = pad_cell(&single_line(hash), layout.hash);
    let status = pad_cell(&single_line(status_label), layout.status);
    let title = pad_cell(&single_line(title), layout.title);
    let role = pad_cell(&single_line(role), layout.role);
    let update_time = pad_cell(&single_line(update_time), layout.update_time);
    let detail = truncate_display(&single_line(detail), layout.latest);
    vec![
        Span::styled(hash, surface_style()),
        Span::styled("  ".to_string(), surface_style()),
        Span::styled(status, delegation_status_style(status_kind)),
        Span::styled("  ".to_string(), surface_style()),
        Span::styled(title, surface_style()),
        Span::styled("  ".to_string(), surface_style()),
        Span::styled(role, surface_style()),
        Span::styled("  ".to_string(), surface_style()),
        Span::styled(update_time, surface_style()),
        Span::styled("  ".to_string(), surface_style()),
        Span::styled(detail, surface_style()),
    ]
}

#[derive(Debug, Clone, Copy)]
struct DelegationTableLayout {
    hash: usize,
    status: usize,
    title: usize,
    role: usize,
    update_time: usize,
    latest: usize,
}

fn delegation_table_layout(width: usize) -> DelegationTableLayout {
    let hash = 8usize;
    let status = 9usize;
    let update_time = 19usize;
    let gap_width = 10usize;
    let available = width
        .saturating_sub(hash)
        .saturating_sub(status)
        .saturating_sub(update_time)
        .saturating_sub(gap_width)
        .max(1);
    let mut title = available
        .saturating_mul(35)
        .saturating_div(100)
        .clamp(12, 30);
    title = title.min(available.saturating_sub(2).max(1));
    let remaining = available.saturating_sub(title);
    let mut role = available
        .saturating_mul(22)
        .saturating_div(100)
        .clamp(8, 20);
    role = role.min(remaining.saturating_sub(1).max(1));
    let latest = available.saturating_sub(title).saturating_sub(role).max(1);
    DelegationTableLayout {
        hash,
        status,
        title,
        role,
        update_time,
        latest,
    }
}

fn delegation_hash(id: &str) -> &str {
    id.strip_prefix(DelegationId::PREFIX).unwrap_or(id)
}

fn delegation_primary_detail(summary: &DelegationSummary) -> String {
    if let Some(error) = summary.error_summary.as_deref() {
        return format!("Error: {}", single_line(error));
    }
    if matches!(summary.status, DelegationStatus::Completed) {
        if let Some(progress) = summary.progress_summary.as_deref() {
            return format!("Completed: {}", single_line(progress));
        }
        return "Completed".to_string();
    }
    if matches!(
        summary.status,
        DelegationStatus::Failed | DelegationStatus::Abandoned
    ) {
        if let Some(progress) = summary.progress_summary.as_deref() {
            return single_line(progress);
        }
    } else if let Some(progress) = summary.progress_summary.as_deref() {
        return single_line(progress);
    } else if let Some(step) = summary.current_step.as_deref() {
        return single_line(step);
    }
    String::new()
}

fn delegation_detail_lines(summary: &DelegationSummary) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(changed) = changed_files_detail(summary) {
        lines.push(changed);
    }
    let terminal = matches!(
        summary.status,
        DelegationStatus::Completed | DelegationStatus::Failed | DelegationStatus::Abandoned
    );
    if !terminal {
        if let Some(step) = summary.current_step.as_deref() {
            lines.push(format!("step: {}", single_line(step)));
        }
    }
    if let Some(progress) = summary.progress_summary.as_deref() {
        lines.push(format!("latest: {}", single_line(progress)));
    }
    if let Some(error) = summary.error_summary.as_deref() {
        lines.push(format!("Error: {}", single_line(error)));
    }
    lines
}

fn changed_files_detail(summary: &DelegationSummary) -> Option<String> {
    if summary.changed_files.is_empty() {
        return None;
    }
    let shown_limit = 3usize;
    let shown = summary
        .changed_files
        .iter()
        .take(shown_limit)
        .map(|path| single_line(path))
        .collect::<Vec<_>>()
        .join(", ");
    let omitted = summary.changed_files.len().saturating_sub(shown_limit);
    let prefix = if omitted > 0 {
        format!("changed(+{omitted} more): ")
    } else {
        "changed: ".to_string()
    };
    Some(format!("{prefix}{shown}"))
}

fn delegation_status_label(status: DelegationStatus) -> &'static str {
    match status {
        DelegationStatus::Queued => "queued",
        DelegationStatus::Running => "running",
        DelegationStatus::Completed => "completed",
        DelegationStatus::Failed => "failed",
        DelegationStatus::Abandoned => "abandoned",
    }
}

fn delegation_status_style(status: DelegationStatus) -> Style {
    match status {
        DelegationStatus::Running => surface_style()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        DelegationStatus::Queued => blue_style(),
        DelegationStatus::Failed => surface_style().fg(Color::Red).add_modifier(Modifier::BOLD),
        DelegationStatus::Completed => surface_style()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        DelegationStatus::Abandoned => muted_style(),
    }
}

fn single_line(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch == '\n' || ch == '\r' || ch.is_control() {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn pad_cell(value: &str, width: usize) -> String {
    let value = truncate_display(value, width);
    let display_width = UnicodeWidthStr::width(value.as_str());
    if display_width >= width {
        return value;
    }
    format!("{value}{}", " ".repeat(width - display_width))
}

fn truncate_display(value: &str, max_cols: usize) -> String {
    if UnicodeWidthStr::width(value) <= max_cols {
        return value.to_string();
    }
    if max_cols <= 1 {
        return ".".into();
    }
    let target = max_cols.saturating_sub(1);
    let mut out = String::new();
    let mut width = 0usize;
    for ch in value.chars() {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width.saturating_add(ch_width) > target {
            break;
        }
        out.push(ch);
        width = width.saturating_add(ch_width);
    }
    out.push('.');
    out
}

impl NetworkSnapshot {
    fn clear_last_contribution(&mut self) {
        self.last_contribution = None;
    }

    fn record_tool_summary(&mut self, summary: &str) {
        if let Some(snapshot) = parse_router_lookup_summary(summary) {
            self.last_router_lookup = Some(snapshot);
        }
    }
}

fn parse_router_lookup_summary(summary: &str) -> Option<RouterLookupSnapshot> {
    if !summary.starts_with("tool consult_router ok ") {
        return None;
    }
    Some(RouterLookupSnapshot {
        candidate_claims: extract_metric(summary, "claims=")?,
        disputes: extract_metric(summary, "disputes=")?,
    })
}

fn extract_metric(summary: &str, key: &str) -> Option<usize> {
    let start = summary.find(key)? + key.len();
    let digits = summary[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// 探测 workspace 路径标签与 git 分支标签。内部调用阻塞 I/O（git 子进程），
/// 因此必须在 `tokio::task::spawn_blocking` 里调用，不能直接在 async 上下文执行。
pub(super) fn detect_workspace_context(workspace_root: PathBuf) -> (String, String) {
    (
        workspace_path_label(&workspace_root),
        current_git_branch_label(&workspace_root),
    )
}

fn workspace_path_label(workspace_root: &Path) -> String {
    let Some(path) = workspace_root.to_str().filter(|path| !path.is_empty()) else {
        return "--".into();
    };
    let Some(home) = env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|home| !home.as_os_str().is_empty())
    else {
        return path.to_string();
    };
    if workspace_root == home {
        return "~".into();
    }
    if let Ok(relative) = workspace_root.strip_prefix(&home) {
        if relative.as_os_str().is_empty() {
            "~".into()
        } else {
            format!("~/{}", relative.display())
        }
    } else {
        path.to_string()
    }
}

fn current_git_branch_label(workspace_root: &Path) -> String {
    let Ok(output) = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(workspace_root)
        .output()
    else {
        return "--".into();
    };
    if !output.status.success() {
        return "--".into();
    }
    let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if branch.is_empty() {
        "--".into()
    } else {
        branch
    }
}

fn format_focus_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    if total_seconds < 60 {
        return format!("{}s", total_seconds.max(1));
    }
    let minutes = total_seconds / 60;
    let hours = minutes / 60;
    if hours == 0 {
        format!("{minutes}m")
    } else {
        format!("{hours}h{}m", minutes % 60)
    }
}

fn focused_duration_between_samples(
    last_sample: Instant,
    now: Instant,
    task_active: bool,
    last_user_activity: Option<Instant>,
) -> Duration {
    if task_active {
        return now.checked_duration_since(last_sample).unwrap_or_default();
    }
    let Some(last_user_activity) = last_user_activity else {
        return Duration::ZERO;
    };
    let active_from = last_sample.max(last_user_activity);
    let active_until = last_user_activity
        .checked_add(FOCUS_INPUT_GRACE)
        .map_or(now, |deadline| deadline.min(now));
    active_until
        .checked_duration_since(active_from)
        .unwrap_or_default()
}

fn tokens_to_display_k(tokens: usize) -> usize {
    tokens.saturating_add(500) / 1000
}

fn context_window_label(tokens: usize) -> String {
    if tokens < 1_000 {
        return tokens.to_string();
    }
    if tokens < 1_000_000 {
        return format!("{}k", tokens / 1_000);
    }
    let tenths = tokens.saturating_add(50_000) / 100_000;
    format!("{}.{}M", tenths / 10, tenths % 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clipboard_image(data: &str) -> crate::attachment::NormalizedMedia {
        crate::attachment::NormalizedMedia {
            media_type: "image/png".into(),
            data: data.into(),
            kind: crate::attachment::AttachmentKind::Image,
            source_name: "clipboard image".into(),
        }
    }

    #[test]
    fn context_window_label_matches_configured_budget_units() {
        assert_eq!(context_window_label(999), "999");
        assert_eq!(context_window_label(256_256), "256k");
        assert_eq!(context_window_label(1_526_467), "1.5M");
    }

    #[test]
    fn focus_grace_counts_only_the_sample_interval_before_its_deadline() {
        let base = Instant::now();
        let last_user_activity = Some(base);

        assert_eq!(
            focused_duration_between_samples(
                base + Duration::from_secs(70),
                base + Duration::from_secs(120),
                false,
                last_user_activity,
            ),
            Duration::from_secs(20)
        );
        assert_eq!(
            focused_duration_between_samples(
                base + Duration::from_secs(100),
                base + Duration::from_secs(120),
                false,
                last_user_activity,
            ),
            Duration::ZERO
        );
    }

    #[test]
    fn active_focus_task_counts_the_full_sample_interval() {
        let base = Instant::now();

        assert_eq!(
            focused_duration_between_samples(
                base + Duration::from_secs(70),
                base + Duration::from_secs(120),
                true,
                Some(base),
            ),
            Duration::from_secs(50)
        );
    }

    #[test]
    fn context_label_uses_configured_window() {
        let mut state = SessionTuiState::new();
        state.set_context_window(256_256);
        state.apply_event(SessionEvent::ContextUsageUpdated {
            used_tokens: 42_000,
        });

        assert_eq!(state.context_label(), "42k/256k");
    }

    #[test]
    fn workspace_path_label_uses_home_relative_path_when_possible() {
        let Some(home) = env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|home| !home.as_os_str().is_empty())
        else {
            return;
        };

        assert_eq!(workspace_path_label(&home), "~");
        assert_eq!(
            workspace_path_label(&home.join("Workspace").join("ft")),
            "~/Workspace/ft"
        );
    }

    #[test]
    fn workspace_path_label_keeps_absolute_path_outside_home() {
        let path = Path::new("/__acn_workspace_label_test__");

        assert_eq!(workspace_path_label(path), "/__acn_workspace_label_test__");
    }

    #[test]
    fn accepted_expanded_model_text_does_not_duplicate_visible_user_echo() {
        let mut state = SessionTuiState::new();
        state.begin_pending_turn("请检查 @src/".into());

        state.apply_event(SessionEvent::UserMessageAccepted {
            text: "请检查 @src/\n\n[Referenced directory: src/]\nlib.rs".into(),
        });

        let transcript = state.transcript_text();
        assert_eq!(transcript.matches("请检查 @src/").count(), 1);
        assert!(!transcript.contains("[Referenced directory:"));
        assert!(!transcript.contains("lib.rs"));
    }

    #[test]
    fn stale_at_path_scan_result_is_discarded_without_blocking_current_scan() {
        let mut state = SessionTuiState::new();
        state.set_at_path_completion_config(
            PathBuf::from("/workspace"),
            AtPathCompletionLimits::default(),
        );
        state.push_input_text("@");
        let (first_generation, first_directory, _) = state.begin_at_path_scan().unwrap();

        state.push_input_char('s');
        // 同目录只过滤已请求快照，不重复扫描；切换到子目录后才会开始新 generation。
        state.push_input_char('r');
        let entries = vec![AtPathDirectoryEntry {
            file_name: std::ffi::OsString::from("src"),
            kind: super::super::at_path_completion::AtPathCandidateKind::Directory,
            protected: false,
        }];
        assert!(state.apply_at_path_directory_read(first_generation, first_directory, Ok(entries),));
        assert!(state.at_path_menu_visible());
        assert!(state.accept_at_path_completion());
        let (second_generation, second_directory, _) = state.begin_at_path_scan().unwrap();

        assert!(!state.apply_at_path_directory_read(
            first_generation,
            PathBuf::from("/workspace"),
            Ok(Vec::new()),
        ));
        assert!(state.apply_at_path_directory_read(
            second_generation,
            second_directory,
            Ok(Vec::new()),
        ));
    }

    #[test]
    fn stale_clipboard_image_result_does_not_attach_to_changed_input() {
        let mut state = SessionTuiState::new();
        state.push_input_text("first");
        let Some((_limits, revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();
        state.push_input_text(" changed");

        state.apply_clipboard_image_read(revision, Ok(Some(clipboard_image("QUJD"))));

        assert_eq!(state.input(), "first changed");
        assert_eq!(state.bottom_pane.effective_attachment_count(), 0);
        assert!(state
            .transcript_text()
            .contains("输入内容已变化，请重新添加图片"));
    }

    #[test]
    fn stale_clipboard_image_result_does_not_attach_to_restored_queue() {
        let mut state = SessionTuiState::new();
        state.push_input_text("current");
        let Some((_limits, revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();
        state.queue_pending_turn("queued");

        state.restore_queued_inputs_to_composer();
        state.apply_clipboard_image_read(revision, Ok(Some(clipboard_image("QUJD"))));

        assert_eq!(state.input(), "queued\ncurrent");
        assert_eq!(state.bottom_pane.effective_attachment_count(), 0);
    }

    #[test]
    fn stale_clipboard_image_result_does_not_attach_to_latest_restored_queue() {
        let mut state = SessionTuiState::new();
        state.push_input_text("current");
        let Some((_limits, revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();
        state.queue_pending_turn("queued");

        assert!(state.restore_latest_queued_input_to_composer());
        state.apply_clipboard_image_read(revision, Ok(Some(clipboard_image("QUJD"))));

        assert_eq!(state.input(), "queued");
        assert_eq!(state.bottom_pane.effective_attachment_count(), 0);
    }

    #[test]
    fn noop_backspace_and_delete_do_not_stale_clipboard_image_result() {
        let mut state = SessionTuiState::new();
        let Some((_limits, revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();

        state.pop_input_char();
        state.delete_input_char();
        state.apply_clipboard_image_read(revision, Ok(Some(clipboard_image("QUJD"))));

        assert_eq!(state.input(), "[Image #1]");
        assert_eq!(state.bottom_pane.effective_attachment_count(), 1);
    }

    #[test]
    fn empty_take_input_draft_does_not_stale_clipboard_image_result() {
        let mut state = SessionTuiState::new();
        let Some((_limits, revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();

        let draft = state.take_input_draft();
        assert!(draft.expanded_text().trim().is_empty());
        state.apply_clipboard_image_read(revision, Ok(Some(clipboard_image("QUJD"))));

        assert_eq!(state.input(), "[Image #1]");
        assert_eq!(state.bottom_pane.effective_attachment_count(), 1);
    }

    #[test]
    fn concurrent_clipboard_image_reads_from_same_draft_all_attach() {
        let mut state = SessionTuiState::new();
        state.push_input_text("see");
        let Some((_limits, first_revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();
        let Some((_limits, second_revision)) = state.begin_clipboard_image_read().unwrap() else {
            panic!("Clipboard image should be enabled by default");
        };
        state.mark_clipboard_image_read_started();
        assert_eq!(first_revision, second_revision);

        state.apply_clipboard_image_read(first_revision, Ok(Some(clipboard_image("QUJD"))));
        state.apply_clipboard_image_read(second_revision, Ok(Some(clipboard_image("REVG"))));

        assert_eq!(state.input(), "see [Image #1] [Image #2]");
        assert_eq!(state.bottom_pane.effective_attachment_count(), 2);
    }
}
