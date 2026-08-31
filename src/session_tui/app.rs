//! 交互式 session TUI 顶层应用。
//!
//! `SessionTuiApp` 统一协调 UI 事件、应用事件和 session worker 事件。
//! 具体展示由 `ChatWidget` 负责，应用层只维护事件循环和会话状态转换。

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use chrono::Utc;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout, Duration, MissedTickBehavior};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::agent::{
    SessionCompactionNoopReason, SessionEngine, SessionEvent, SessionRuntimeStatus,
};
use crate::claim::SessionId;
use crate::config::AgentSessionTuiConfig;
use crate::delegation::{DelegationStatus, DelegationStore, DelegationSummary};
use crate::mcp::connection_manager::{
    McpConnectionManager, McpRuntimeState, McpRuntimeTransition, McpServerStatus,
};
use crate::mcp::redact::redact_mcp_sensitive_text;
use crate::skill::SkillSummary;
use crate::supervisor::SupervisorLaunchConfig;

use super::app_event::{AppEvent, AppEventSender};
use super::at_path_completion::{read_directory_entries, AtPathCompletionLimits};
use super::attachment::{
    prepare_preview_files, PreviewFailure, PreviewFile, PreviewTarget, ResolvedAtPaths,
};
use super::bottom_pane::{classify_input, input_accepts_text, InputAction, InputDraft};
use super::chat_widget::ChatWidget;
use super::cleanup_housekeeping::{
    spawn_session_cleanup_housekeeping, SessionCleanupActivity, SessionCleanupHousekeepingConfig,
};
use super::input_queue::QueuedInput;
use super::mcp_panel::McpPanelRequest;
use super::process_panel::{ProcessPanelKeyAction, ProcessTerminationTarget};
use super::runtime::{
    spawn_recap_enqueue_worker, spawn_resume_history_worker, spawn_resume_inbox_worker,
    spawn_resume_list_worker, spawn_resume_preflight_worker, spawn_start_worker,
    CompactWorkerOutcome, FinalizeEnqueueOutcome, McpOperationOutcome, PendingSteerInput,
    ResumeHistoryOutcome, ResumeSessionReservation, SessionTaskState, WorkerEvent,
};
use super::session_picker::SessionPickerState;
use super::slash_command::SlashCommandCatalog;
use super::state::ATTACHMENT_STEER_QUEUE_NOTICE;
use super::tui::{Tui, TuiEvent};

const RESIZE_REDRAW_DEBOUNCE: Duration = Duration::from_millis(250);
/// 确认终止后，给用户一次可感知的 optimistic `terminating` 反馈。
///
/// 这不是进程终止的 timeout；它只保证已经实际 draw 的状态不会在同一个事件循环内被
/// authoritative snapshot 立即抹掉。
const PROCESS_TERMINATE_OPTIMISTIC_VISIBLE_FOR: Duration = Duration::from_millis(100);
/// Enable/Disable 先完成配置持久化；退出窗口只用于收束后续 transport/connection 工作。
const MCP_OPERATION_EXIT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const TURN_CANCEL_PENDING_NOTICE: &str = "Turn cancel pending: settling active tool calls";
const FIRST_DELEGATION_NOTICE_GRACE_SECS: i64 = 60;
const DELEGATION_NOTICE_PREFIX: &str = "Subagent ";
const SKILLS_NAME_COL_WIDTH: usize = 28;
const SKILLS_DESC_COL_WIDTH: usize = 77;
const RECAP_ENQUEUE_WARNING: &str = "Background recap could not be queued and will retry later.";

pub(super) fn recap_enqueue_warning(result: &anyhow::Result<()>) -> Option<&'static str> {
    result.as_ref().err().map(|_| RECAP_ENQUEUE_WARNING)
}

fn is_ctrl_c_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
        && key.modifiers.contains(KeyModifiers::CONTROL)
}

fn compaction_noop_notice(reason: SessionCompactionNoopReason) -> &'static str {
    match reason {
        SessionCompactionNoopReason::NothingNew => "Nothing new to compact.",
        SessionCompactionNoopReason::RawTailWithinBudget => {
            "New history is still within the compact raw-tail budget; No compaction needed."
        }
    }
}

fn tui_delegation_status_label(status: DelegationStatus) -> &'static str {
    match status {
        DelegationStatus::Queued => "queued",
        DelegationStatus::Running => "running",
        DelegationStatus::Completed => "completed",
        DelegationStatus::Failed => "failed",
        DelegationStatus::Abandoned => "abandoned",
    }
}

fn tui_delegation_notice_text(value: &str) -> String {
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

fn status_notice_blocks_delegation_notice(notice: Option<&str>) -> bool {
    notice.is_some_and(|notice| {
        let notice = notice.trim();
        !notice.is_empty() && !notice.starts_with(DELEGATION_NOTICE_PREFIX)
    })
}

/// 运行 TUI session。创建 session，普通输入触发 turn，`/exit` finalize。
pub async fn run_session_tui(engine: SessionEngine, max_attempts: usize) -> anyhow::Result<()> {
    run_session_tui_with_resume(
        engine,
        max_attempts,
        StartupResume::None,
        AgentSessionTuiConfig::default(),
        None,
    )
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupResume {
    None,
    Picker,
    Session(crate::claim::SessionId),
}

pub async fn run_session_tui_with_resume(
    engine: SessionEngine,
    max_attempts: usize,
    startup_resume: StartupResume,
    tui_config: AgentSessionTuiConfig,
    supervisor: Option<SupervisorLaunchConfig>,
) -> anyhow::Result<()> {
    run_session_tui_with_resume_and_cleanup(
        engine,
        max_attempts,
        startup_resume,
        tui_config,
        supervisor,
        None,
    )
    .await
}

pub async fn run_session_tui_with_resume_and_cleanup(
    engine: SessionEngine,
    max_attempts: usize,
    startup_resume: StartupResume,
    tui_config: AgentSessionTuiConfig,
    supervisor: Option<SupervisorLaunchConfig>,
    cleanup_housekeeping: Option<SessionCleanupHousekeepingConfig>,
) -> anyhow::Result<()> {
    let mut app = SessionTuiApp::new(
        engine,
        max_attempts,
        startup_resume,
        tui_config,
        supervisor,
        cleanup_housekeeping,
    )?;
    app.run().await
}

struct SessionTuiApp {
    engine: SessionEngine,
    max_attempts: usize,
    tui: Tui,
    chat_widget: ChatWidget,
    app_event_rx: mpsc::UnboundedReceiver<AppEvent>,
    worker_tx: mpsc::UnboundedSender<WorkerEvent>,
    worker_rx: mpsc::UnboundedReceiver<WorkerEvent>,
    start_handle: Option<JoinHandle<()>>,
    resume_handle: Option<JoinHandle<()>>,
    session: Option<crate::session::SessionHandle>,
    _runtime_lease: Option<crate::session::SessionRuntimeLease>,
    current_session_has_real_user_input: bool,
    resume_switch_pending: bool,
    finalize_continuation: Option<SessionFinalizeContinuation>,
    session_task: SessionTaskState,
    session_picker: Option<SessionPickerState>,
    mcp_manager: Option<Arc<McpConnectionManager>>,
    mcp_operation_tasks: JoinSet<()>,
    /// Ctrl+O 任务独立保留临时路径，确保 completion 尚未被事件循环消费时退出也能清理。
    preview_tasks: JoinSet<Vec<PathBuf>>,
    supervisor: Option<SupervisorLaunchConfig>,
    cleanup_housekeeping: Option<SessionCleanupHousekeepingConfig>,
    cleanup_activity: SessionCleanupActivity,
    cleanup_housekeeping_handle: Option<JoinHandle<()>>,
    startup_resume: StartupResume,
    resize_render_handle: Option<JoinHandle<()>>,
    /// Ctrl+O 预览剪贴板图片时临时写出的文件，退出前删除。
    preview_temp_files: Vec<std::path::PathBuf>,
    next_input_sequence_to_submit: u64,
    pending_input_submissions: BTreeMap<u64, PendingInputSubmission>,
    skipped_input_submission_sequences: BTreeSet<u64>,
    restore_async_input_sequences_before: u64,
    defer_input_restores_until_turn_id: Option<u64>,
    deferred_input_restores: BTreeMap<u64, InputDraft>,
    delegation_tracking_session: Option<SessionId>,
    delegation_terminal_seen: BTreeSet<String>,
    delegation_snapshot_initialized: bool,
    delegation_snapshot_last_error: Option<String>,
    process_snapshot_generation: u64,
    process_snapshot_in_flight: bool,
    /// 每个 optimistic `/ps` terminate 都必须先实际绘制一帧，再允许 worker 覆盖为
    /// authoritative snapshot；不能用时间延迟猜测 TUI 是否已经完成 draw。
    process_termination_render_acks: BTreeMap<u64, oneshot::Sender<()>>,
}

enum SessionSwitchTarget {
    New,
    Resume(Box<ResumeSessionReservation>),
}

enum FinalizeContinuation<T> {
    Exit,
    Switch(T),
}

type SessionFinalizeContinuation = FinalizeContinuation<SessionSwitchTarget>;

enum FinalizeSuccessAction<T> {
    Exit,
    Install(T),
}

enum PendingInputSubmission {
    Ready {
        input: QueuedInput,
        restore_to_composer: bool,
        record_history: bool,
    },
    AttachFailed {
        draft: InputDraft,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputSubmissionRoute {
    Queue,
    Dispatch,
    Reject,
}

impl SessionTuiApp {
    fn new(
        engine: SessionEngine,
        max_attempts: usize,
        startup_resume: StartupResume,
        tui_config: AgentSessionTuiConfig,
        supervisor: Option<SupervisorLaunchConfig>,
        cleanup_housekeeping: Option<SessionCleanupHousekeepingConfig>,
    ) -> anyhow::Result<Self> {
        let tui = Tui::enter()?;
        let (app_event_tx, app_event_rx) = AppEventSender::channel();
        let cleanup_activity = SessionCleanupActivity::new();
        let mut chat_widget = ChatWidget::new(app_event_tx);
        chat_widget.set_tui_config(tui_config);
        chat_widget.state_mut().set_at_path_completion_config(
            engine.workspace_root().to_path_buf(),
            AtPathCompletionLimits::default(),
        );
        chat_widget.state_mut().agent_id = Some(engine.agent_id().to_string());
        chat_widget.state_mut().model_name = Some(engine.session_model().to_string());
        chat_widget
            .state_mut()
            .set_context_window(engine.context_window());
        chat_widget
            .state_mut()
            .set_attachment_config(engine.attachment_config().clone());
        chat_widget.refresh_at_path_completion();
        chat_widget.state_mut().set_slash_skills(
            engine
                .available_skills()
                .iter()
                .map(|skill| (skill.name.as_str(), skill.description.as_str())),
        );
        let mcp_manager = engine.mcp_manager();
        if let Some(manager) = &mcp_manager {
            let snapshot = manager.snapshot_sync();
            let warnings = mcp_startup_warnings(&snapshot);
            chat_widget
                .state_mut()
                .set_mcp_runtime(manager.config_path().to_path_buf(), snapshot);
            for warning in warnings {
                chat_widget
                    .state_mut()
                    .push_system(format!("Warning: {warning}"));
            }
        }
        chat_widget.handle_session_event(SessionEvent::StartupProgress {
            label: "initializing agent...".into(),
        });
        let (worker_tx, worker_rx) = mpsc::unbounded_channel::<WorkerEvent>();
        Ok(Self {
            engine,
            max_attempts,
            tui,
            chat_widget,
            app_event_rx,
            worker_tx,
            worker_rx,
            start_handle: None,
            resume_handle: None,
            session: None,
            _runtime_lease: None,
            current_session_has_real_user_input: false,
            resume_switch_pending: false,
            finalize_continuation: None,
            session_task: SessionTaskState::default(),
            session_picker: None,
            mcp_manager,
            mcp_operation_tasks: JoinSet::new(),
            preview_tasks: JoinSet::new(),
            supervisor,
            cleanup_housekeeping,
            cleanup_activity,
            cleanup_housekeeping_handle: None,
            startup_resume,
            resize_render_handle: None,
            preview_temp_files: Vec::new(),
            next_input_sequence_to_submit: 0,
            pending_input_submissions: BTreeMap::new(),
            skipped_input_submission_sequences: BTreeSet::new(),
            restore_async_input_sequences_before: 0,
            defer_input_restores_until_turn_id: None,
            deferred_input_restores: BTreeMap::new(),
            delegation_tracking_session: None,
            delegation_terminal_seen: BTreeSet::new(),
            delegation_snapshot_initialized: false,
            delegation_snapshot_last_error: None,
            process_snapshot_generation: 0,
            process_snapshot_in_flight: false,
            process_termination_render_acks: BTreeMap::new(),
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        // workspace / git 分支探测含阻塞子进程调用，放进 spawn_blocking，避免阻塞 tokio 运行时线程。
        // 在首帧之前注入，footer 直接显示真实标签、无 "--" 闪烁；探测失败则保留占位。
        let workspace_root = self.engine.workspace_root().to_path_buf();
        if let Ok((workspace_label, branch_label)) = tokio::task::spawn_blocking(move || {
            super::state::detect_workspace_context(workspace_root)
        })
        .await
        {
            self.chat_widget
                .state_mut()
                .set_workspace_context(workspace_label, branch_label);
        }
        match std::mem::replace(&mut self.startup_resume, StartupResume::None) {
            StartupResume::Session(session_id) => {
                self.start_resume_open(session_id)?;
            }
            startup_resume => {
                self.startup_resume = startup_resume;
                self.start_handle = Some(spawn_start_worker(
                    self.engine.clone(),
                    self.max_attempts,
                    self.worker_tx.clone(),
                ));
            }
        }
        self.tui
            .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
        self.cleanup_housekeeping_handle = spawn_session_cleanup_housekeeping(
            self.cleanup_housekeeping.take(),
            self.cleanup_activity.clone(),
        );
        self.update_cleanup_busy();
        let mut animation_tick = tokio::time::interval(Duration::from_millis(140));
        animation_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(1));
        heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let loop_result: anyhow::Result<()> = async {
            loop {
                tokio::select! {
                    _ = animation_tick.tick() => {
                        if self.tick_turn_animation()? {
                            self.tui.render_requester().schedule_render();
                        }
                    }
                    _ = heartbeat_tick.tick() => {
                        self.refresh_delegation_snapshot().await?;
                        let rewrite_scrollback = self.refresh_background_process_completions().await;
                        // 底栏的 `Processes:` 与 `/ps` 共用同一份 snapshot；即使面板关闭也要
                        // 刷新，避免后台 entry 的数量在用户重新打开前一直陈旧。
                        self.refresh_process_snapshot();
                        self.refresh_mcp_panel();
                        // 时间型 UI（如 `/ps` ELAPSED 与底栏 focus）不一定改变 snapshot；
                        // heartbeat 仍需请求一帧，避免 turn idle 后画面冻结。
                        if rewrite_scrollback {
                            // 已提交 turn 的 cell 位于终端原生 scrollback，普通增量帧只能
                            // 重画底部 live region；复用 resize 的 Purge + 全历史 reflow 才能
                            // 原位替换旧的 `Process running in background` 投影。
                            self.tui.draw_after_state_reload(
                                &mut self.chat_widget,
                                self.session_picker.as_ref(),
                            )?;
                        } else {
                            self.tui.render_requester().schedule_render();
                        }
                    }
                    maybe_worker_event = self.worker_rx.recv() => {
                        if let Some(worker_event) = maybe_worker_event {
                            if self.handle_worker_event(worker_event)? {
                                break;
                            }
                            self.update_cleanup_busy();
                        }
                    }
                    maybe_app_event = self.app_event_rx.recv() => {
                        if let Some(app_event) = maybe_app_event {
                            if self.handle_app_event(app_event).await? {
                                break;
                            }
                            self.update_cleanup_busy();
                        }
                    }
                    tui_event = self.tui.recv_event() => {
                        let tui_event = tui_event?;
                        if self.handle_tui_event(tui_event)? {
                            break;
                        }
                        self.update_cleanup_busy();
                    }
                }
            }
            Ok(())
        }
        .await;

        self.tui.stop_reader();
        self.stop_cleanup_housekeeping().await;
        drain_mcp_operation_tasks(
            &mut self.mcp_operation_tasks,
            self.mcp_manager.as_deref(),
            MCP_OPERATION_EXIT_DRAIN_TIMEOUT,
        )
        .await;
        if let Some(handle) = self.resize_render_handle.take() {
            handle.abort();
        }
        cleanup_preview_temp_files(&mut self.preview_temp_files, &mut self.preview_tasks).await;
        // `/exit` 会在 finalize 中按 session 收束；这里覆盖 TUI 初始化/事件循环失败及
        // 正常 runtime 返回，避免受管 terminal 仅因没有走到 finalize 而存活到宿主退出。
        self.engine.shutdown_background_processes().await;
        loop_result?;
        Ok(())
    }

    async fn stop_cleanup_housekeeping(&mut self) {
        let Some(handle) = self.cleanup_housekeeping_handle.take() else {
            return;
        };
        self.cleanup_activity.request_shutdown();
        let _ = handle.await;
    }

    fn update_cleanup_busy(&self) {
        self.cleanup_activity.set_busy(
            self.session_task.task_running()
                || self.start_handle.is_some()
                || self.resume_handle.is_some(),
        );
    }

    async fn refresh_delegation_snapshot(&mut self) -> anyhow::Result<bool> {
        let Some((session_id, session_dir)) = self
            .session
            .as_ref()
            .map(|session| (session.metadata.id.clone(), session.paths.dir.clone()))
        else {
            self.delegation_tracking_session = None;
            self.delegation_terminal_seen.clear();
            self.delegation_snapshot_initialized = false;
            self.delegation_snapshot_last_error = None;
            return Ok(self
                .chat_widget
                .state_mut()
                .set_delegation_summaries(Vec::new()));
        };
        if self.delegation_tracking_session.as_ref() != Some(&session_id) {
            self.delegation_tracking_session = Some(session_id);
            self.delegation_terminal_seen.clear();
            self.delegation_snapshot_initialized = false;
            self.delegation_snapshot_last_error = None;
        }
        let summaries = match DelegationStore::new(session_dir).list_strict().await {
            Ok(summaries) => summaries,
            Err(error) => {
                let error = format!("{error:#}");
                if self.delegation_snapshot_last_error.as_deref() != Some(error.as_str()) {
                    log::warn!(target: "session_tui", "读取 delegation snapshot 失败: {error}");
                    self.delegation_snapshot_last_error = Some(error);
                }
                return Ok(self
                    .chat_widget
                    .state_mut()
                    .set_delegation_snapshot_error("Read failed"));
            }
        };
        self.delegation_snapshot_last_error = None;
        let notice_changed = self.update_delegation_terminal_notice(&summaries);
        let summaries_changed = self
            .chat_widget
            .state_mut()
            .set_delegation_summaries(summaries);
        Ok(notice_changed || summaries_changed)
    }

    fn update_delegation_terminal_notice(&mut self, summaries: &[DelegationSummary]) -> bool {
        if !self.delegation_snapshot_initialized {
            let now = Utc::now();
            for summary in summaries.iter().filter(|summary| {
                matches!(
                    summary.status,
                    DelegationStatus::Completed
                        | DelegationStatus::Failed
                        | DelegationStatus::Abandoned
                )
            }) {
                let age_secs = now
                    .signed_duration_since(summary.updated_at)
                    .num_seconds()
                    .abs();
                if age_secs > FIRST_DELEGATION_NOTICE_GRACE_SECS {
                    self.delegation_terminal_seen.insert(summary.id.to_string());
                }
            }
            self.delegation_snapshot_initialized = true;
        }
        if status_notice_blocks_delegation_notice(self.chat_widget.state().status_notice_text()) {
            return false;
        }
        let latest_terminal = summaries
            .iter()
            .filter(|summary| {
                matches!(
                    summary.status,
                    DelegationStatus::Completed
                        | DelegationStatus::Failed
                        | DelegationStatus::Abandoned
                ) && !self.delegation_terminal_seen.contains(summary.id.as_str())
            })
            .max_by(|a, b| {
                a.updated_at
                    .cmp(&b.updated_at)
                    .then_with(|| a.id.cmp(&b.id))
            });
        let Some(summary) = latest_terminal else {
            return false;
        };
        self.delegation_terminal_seen.insert(summary.id.to_string());
        self.chat_widget.state_mut().set_status_notice(format!(
            "Subagent '{}' {}",
            tui_delegation_notice_text(&summary.title),
            tui_delegation_status_label(summary.status)
        ));
        true
    }

    fn tick_turn_animation(&mut self) -> anyhow::Result<bool> {
        if self.session_picker.is_some() || !self.chat_widget.state().turn_animation_is_active() {
            return Ok(false);
        }
        let (width, height) = self.tui.terminal_size()?;
        let height_budget = self.chat_widget.turn_animation_height_budget(width, height);
        Ok(self
            .chat_widget
            .state_mut()
            .tick_turn_animation(width, height_budget))
    }

    fn handle_worker_event(&mut self, worker_event: WorkerEvent) -> anyhow::Result<bool> {
        match worker_event {
            WorkerEvent::Session { task_id, event } => {
                if !self.session_task.current_task_matches(task_id) {
                    return Ok(false);
                }
                if let SessionEvent::RecapRequested {
                    session_id,
                    recap_end_index,
                } = &event
                {
                    spawn_recap_enqueue_worker(
                        self.supervisor.clone(),
                        session_id.clone(),
                        *recap_end_index,
                        self.worker_tx.clone(),
                    );
                    return Ok(false);
                }
                if let Some(turn_id) = task_id {
                    if let SessionEvent::TurnCommitted { .. } = &event {
                        self.session_task.mark_turn_committed(turn_id);
                    }
                }
                if matches!(&event, SessionEvent::UserMessageAccepted { .. }) {
                    self.current_session_has_real_user_input = true;
                }
                let should_restore_late_async_inputs = matches!(
                    &event,
                    SessionEvent::TurnCancelled { .. } | SessionEvent::TurnFailed { .. }
                );
                if !matches!(
                    event,
                    SessionEvent::StatusChanged {
                        status: SessionRuntimeStatus::Open
                    }
                ) || self.session.is_some()
                {
                    self.chat_widget.handle_session_event(event);
                }
                if self
                    .chat_widget
                    .state_mut()
                    .take_scrollback_rewrite_required()
                {
                    // completion 也可能由新 turn 的持久化屏障或 `/exit` finalize worker
                    // 送达，而不是 1 秒 heartbeat；这些路径同样必须重写原生 scrollback。
                    self.tui.draw_after_state_reload(
                        &mut self.chat_widget,
                        self.session_picker.as_ref(),
                    )?;
                    return Ok(false);
                }
                if should_restore_late_async_inputs {
                    self.mark_pending_async_inputs_for_restore();
                }
            }
            WorkerEvent::StartFinished(result) => match result {
                Ok(report) => {
                    self.invalidate_process_panel_snapshot();
                    self.chat_widget.handle_session_event(
                        SessionEvent::TeamServicesConnectionUpdated {
                            status: report.inbox_report.team_services,
                        },
                    );
                    self._runtime_lease = Some(report.runtime_lease);
                    self.session = Some(report.session);
                    self.current_session_has_real_user_input = false;
                    self.start_handle = None;
                    self.chat_widget
                        .handle_session_event(SessionEvent::StatusChanged {
                            status: SessionRuntimeStatus::Open,
                        });
                    match std::mem::replace(&mut self.startup_resume, StartupResume::None) {
                        StartupResume::None => {
                            self.maybe_dispatch_next_queued_input()?;
                            self.tui.draw_after_state_reload(
                                &mut self.chat_widget,
                                self.session_picker.as_ref(),
                            )?;
                            return Ok(false);
                        }
                        StartupResume::Picker => self.start_resume()?,
                        StartupResume::Session(session_id) => self.start_resume_open(session_id)?,
                    }
                }
                Err(e) => {
                    self.start_handle = None;
                    let state = self.chat_widget.state_mut();
                    state.status = SessionRuntimeStatus::Error;
                    state.push_error(format!("Session start failed: {e:#}"));
                    state.restore_queued_inputs_to_composer();
                    self.mark_pending_async_inputs_for_restore();
                    self.tui.render_requester().schedule_render();
                }
            },
            WorkerEvent::ResumeListLoaded { sessions } => {
                self.resume_handle = None;
                self.session_picker = Some(SessionPickerState::new(
                    sessions,
                    self.chat_widget.event_tx(),
                ));
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::ResumeListFailed(error) => {
                self.resume_handle = None;
                let state = self.chat_widget.state_mut();
                if self.session.is_none() {
                    state.status = SessionRuntimeStatus::Error;
                }
                state.push_error(format!("Resume failed: {error:#}"));
                self.restore_queued_inputs_after_resume_interrupted();
                self.mark_pending_async_inputs_for_restore();
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::ResumeSessionReserved { result } => {
                self.resume_handle = None;
                let is_session_switch = std::mem::take(&mut self.resume_switch_pending);
                match result {
                    Ok(reservation) if is_session_switch => {
                        self.continue_session_switch(SessionSwitchTarget::Resume(Box::new(
                            reservation,
                        )))?;
                    }
                    Ok(reservation) => self.begin_resume_session_startup(reservation)?,
                    Err(e) => {
                        let state = self.chat_widget.state_mut();
                        if self.session.is_none() {
                            state.status = SessionRuntimeStatus::Error;
                        }
                        state.push_error(format!("Resume failed: {e:#}"));
                        self.restore_queued_inputs_after_resume_interrupted();
                        self.mark_pending_async_inputs_for_restore();
                    }
                }
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::ResumeHistoryLoaded { result } => {
                self.resume_handle = None;
                match result {
                    Ok(outcome) => self.install_resume_history_and_start_inbox(outcome)?,
                    Err(error) => {
                        let state = self.chat_widget.state_mut();
                        state.status = SessionRuntimeStatus::Error;
                        state.push_error(format!("Resume failed: {error:#}"));
                        state.restore_queued_inputs_to_composer();
                        self.mark_pending_async_inputs_for_restore();
                        self.tui.render_requester().schedule_render();
                    }
                }
            }
            WorkerEvent::ResumeInboxFinished { session, result } => {
                self.resume_handle = None;
                self.session = Some(session);
                self.current_session_has_real_user_input = true;
                if let Err(error) = result {
                    log::warn!(target: "session_tui", "Resume inbox sync failed: {error:#}");
                    let state = self.chat_widget.state_mut();
                    state.finish_resume_inbox_with_warning(
                        "Warning: Inbox sync failed; run /inbox to retry.",
                    );
                }
                self.maybe_dispatch_next_queued_input()?;
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::TurnFinished { turn_id, result } => {
                let Some(mut active) = self.session_task.finish_turn(turn_id) else {
                    return Ok(false);
                };
                self.chat_widget.state_mut().mark_turn_finished();
                match result {
                    Ok(updated_session) => {
                        self.session = Some(updated_session);
                        if active.pending_cancel_requested() {
                            let pending_steers = active.take_pending_steer_inputs_for_restore();
                            self.restore_cancelled_turn_inputs(pending_steers);
                            self.tui.render_requester().schedule_render();
                        } else if let Some(pending_steer) = active.take_pending_steer_input() {
                            self.submit_input(pending_steer)?;
                        } else {
                            self.maybe_dispatch_next_queued_input()?;
                        }
                    }
                    Err(e) => {
                        let pending_steers = active.take_pending_steer_inputs_for_restore();
                        let should_push_error =
                            self.chat_widget.state().status != SessionRuntimeStatus::Error;
                        if should_push_error {
                            self.chat_widget
                                .state_mut()
                                .fail_running_turn_without_restoring_queue(format!("{e:#}"));
                        } else {
                            self.chat_widget
                                .state_mut()
                                .prepare_failed_turn_without_restoring_queue();
                        }
                        self.restore_cancelled_turn_inputs(pending_steers);
                        self.mark_pending_async_inputs_for_restore();
                        self.tui.render_requester().schedule_render();
                    }
                }
            }
            WorkerEvent::UserShellCommandFinished { task_id, result } => {
                if !self.session_task.finish_shell(task_id) {
                    return Ok(false);
                }
                match result {
                    Ok(updated_session) => {
                        self.session = Some(updated_session);
                        self.maybe_dispatch_next_queued_input()?;
                    }
                    Err(e) => {
                        if self.chat_widget.state().status != SessionRuntimeStatus::Error {
                            self.chat_widget
                                .state_mut()
                                .push_error(format!("Shell command failed: {e:#}"));
                        }
                        self.chat_widget
                            .state_mut()
                            .restore_queued_inputs_to_composer();
                        self.mark_pending_async_inputs_for_restore();
                    }
                }
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::FinalizeFinished { task_id, result } => match result {
                Ok(()) => {
                    if !self.session_task.finish_finalize(task_id) {
                        return Ok(false);
                    }
                    let continuation = self
                        .finalize_continuation
                        .take()
                        .unwrap_or(FinalizeContinuation::Exit);
                    return self.complete_finalize_continuation(continuation);
                }
                Err(e) => {
                    if !self.session_task.finish_finalize(task_id) {
                        return Ok(false);
                    }
                    if let Some(FinalizeContinuation::Switch(target)) =
                        self.finalize_continuation.take()
                    {
                        self.restore_after_switch_finalize_failure(target);
                    }
                    self.chat_widget
                        .handle_session_event(SessionEvent::StatusChanged {
                            status: SessionRuntimeStatus::Error,
                        });
                    let state = self.chat_widget.state_mut();
                    state.mark_finalize_failed();
                    state.push_error(format!(
                        "Finalize failed; session remains finalizing: {e:#}"
                    ));
                    self.tui.render_requester().schedule_render();
                }
            },
            WorkerEvent::FinalizeEnqueueFinished { task_id, result } => {
                if !self.session_task.finish_finalize(task_id) {
                    return Ok(false);
                }
                match result {
                    FinalizeEnqueueOutcome::Enqueued { job_id, session_id } => {
                        let continuation = self
                            .finalize_continuation
                            .take()
                            .unwrap_or(FinalizeContinuation::Exit);
                        match finalize_success_action(continuation) {
                            FinalizeSuccessAction::Exit => {
                                self.chat_widget.state_mut().push_system(
                                    finalize_enqueue_exit_message(&job_id, &session_id),
                                );
                                self.tui
                                    .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
                                return Ok(true);
                            }
                            FinalizeSuccessAction::Install(target) => {
                                self.begin_session_switch_target(target)?;
                                return Ok(false);
                            }
                        }
                    }
                    FinalizeEnqueueOutcome::Fallback { session, error } => {
                        self.chat_widget.state_mut().push_system(format!(
                            "Background finalize unavailable, finalizing here: {error:#}"
                        ));
                        self.tui.render_requester().schedule_render();
                        let runtime_lease = self.runtime_lease_for_worker()?;
                        self.session_task.spawn_tracked_finalize(
                            self.engine.clone(),
                            *session,
                            runtime_lease,
                            self.worker_tx.clone(),
                        );
                    }
                }
            }
            WorkerEvent::RecapEnqueueFinished { session_id, result } => {
                if !recap_enqueue_result_belongs_to_visible_session(
                    self.chat_widget.state().session_id.as_deref(),
                    &session_id,
                ) {
                    return Ok(false);
                }
                if let Some(message) = recap_enqueue_warning(&result) {
                    self.chat_widget
                        .handle_session_event(SessionEvent::Warning {
                            message: message.to_string(),
                        });
                    self.tui.render_requester().schedule_render();
                }
            }
            WorkerEvent::CompactFinished { task_id, result } => {
                if !self.session_task.finish_compact(task_id) {
                    return Ok(false);
                }
                match result {
                    Ok(CompactWorkerOutcome::Compacted(updated_session)) => {
                        self.session = Some(updated_session);
                        self.maybe_dispatch_next_queued_input()?;
                    }
                    Ok(CompactWorkerOutcome::Noop { session, reason }) => {
                        self.session = Some(session);
                        self.chat_widget
                            .state_mut()
                            .push_system(compaction_noop_notice(reason));
                        self.maybe_dispatch_next_queued_input()?;
                    }
                    Err(_) => {
                        self.chat_widget
                            .state_mut()
                            .restore_queued_inputs_to_composer();
                        self.mark_pending_async_inputs_for_restore();
                    }
                }
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::InboxFinished { task_id, result } => {
                if !self.session_task.finish_inbox(task_id) {
                    return Ok(false);
                }
                match result {
                    Ok(outcome) => {
                        self.chat_widget.handle_session_event(
                            SessionEvent::TeamServicesConnectionUpdated {
                                status: outcome.report.team_services,
                            },
                        );
                        self.session = Some(outcome.session);
                        self.maybe_dispatch_next_queued_input()?;
                        let (width, height) = self.tui.terminal_size()?;
                        if self
                            .chat_widget
                            .welcome_team_status_is_visible(width, height)
                        {
                            self.tui.draw_after_state_reload(
                                &mut self.chat_widget,
                                self.session_picker.as_ref(),
                            )?;
                            return Ok(false);
                        }
                    }
                    Err(_) => {
                        self.maybe_dispatch_next_queued_input()?;
                    }
                }
                self.tui.render_requester().schedule_render();
            }
            WorkerEvent::McpOperationFinished {
                server_name,
                operation_id,
                outcome,
            } => {
                let applied = self.chat_widget.state_mut().finish_mcp_request(
                    &server_name,
                    operation_id,
                    outcome,
                );
                if applied {
                    self.tui.render_requester().schedule_render();
                }
            }
        }
        Ok(false)
    }

    async fn handle_app_event(&mut self, app_event: AppEvent) -> anyhow::Result<bool> {
        match app_event {
            AppEvent::SubmitInput { sequence, input } => {
                self.handle_ready_input(sequence, input).await?
            }
            AppEvent::SteerInput { sequence, input } => {
                self.handle_ready_steer_input(sequence, input).await?
            }
            AppEvent::ClipboardImageRead {
                interaction_generation,
                input_revision,
                result,
            } => {
                if !interaction_event_belongs_to_current(
                    self.chat_widget.state().interaction_generation(),
                    interaction_generation,
                ) {
                    self.chat_widget.state_mut().discard_clipboard_image_read();
                    return Ok(false);
                }
                self.chat_widget
                    .state_mut()
                    .apply_clipboard_image_read(input_revision, result);
                self.tui.render_requester().schedule_render();
            }
            AppEvent::AtPathDirectoryScan {
                generation,
                directory,
                max_entries,
            } => {
                let tx = self.chat_widget.event_tx();
                tokio::spawn(async move {
                    let result = read_directory_entries(&directory, max_entries).await;
                    tx.at_path_directory_read(generation, directory, result);
                });
            }
            AppEvent::AtPathDirectoryRead {
                generation,
                directory,
                result,
            } => {
                if self
                    .chat_widget
                    .state_mut()
                    .apply_at_path_directory_read(generation, directory, result)
                {
                    self.tui.render_requester().schedule_render();
                }
            }
            AppEvent::AtPathResolved {
                sequence,
                expanded_input,
                draft,
                result,
            } => {
                self.handle_at_path_resolved(sequence, expanded_input, draft, result)
                    .await?
            }
            AppEvent::PreviewAttachment {
                interaction_generation,
                targets,
            } => {
                let tx = self.chat_widget.event_tx();
                let workspace_root = self.engine.workspace_root().to_path_buf();
                spawn_preview_task(
                    &mut self.preview_tasks,
                    tx,
                    interaction_generation,
                    targets,
                    workspace_root,
                );
            }
            AppEvent::PreviewLaunched {
                interaction_generation,
                result,
            } => {
                self.preview_temp_files
                    .extend(preview_temporary_paths(&result));
                match result {
                    Ok(files) => {
                        if !interaction_event_belongs_to_current(
                            self.chat_widget.state().interaction_generation(),
                            interaction_generation,
                        ) {
                            return Ok(false);
                        }
                        let labels = files
                            .iter()
                            .map(|file| file.label.as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        let hint = if files.len() > 1 {
                            format!("已在默认应用打开 {} 个附件: {labels}", files.len())
                        } else {
                            format!("已在默认应用打开: {labels}")
                        };
                        self.chat_widget.state_mut().push_system(hint);
                        self.tui.render_requester().schedule_render();
                    }
                    Err(failure) => {
                        if !interaction_event_belongs_to_current(
                            self.chat_widget.state().interaction_generation(),
                            interaction_generation,
                        ) {
                            return Ok(false);
                        }
                        self.chat_widget
                            .state_mut()
                            .push_error(format!("Preview failed: {}", failure.message));
                        self.tui.render_requester().schedule_render();
                    }
                }
            }
            AppEvent::ClipboardTextWritten {
                interaction_generation,
                result,
            } => {
                if !interaction_event_belongs_to_current(
                    self.chat_widget.state().interaction_generation(),
                    interaction_generation,
                ) {
                    return Ok(false);
                }
                match result {
                    Ok(()) => self
                        .chat_widget
                        .state_mut()
                        .push_system("Assistant 回复已复制至剪切板。"),
                    Err(message) => self
                        .chat_widget
                        .state_mut()
                        .push_error(format!("复制 Assistant 回复失败: {message}")),
                }
                self.tui.render_requester().schedule_render();
            }
            AppEvent::McpPanelRequest(request) => self.start_mcp_panel_request(request),
            AppEvent::ProcessPanelAction(ProcessPanelKeyAction::Terminate { target }) => {
                self.terminate_process_from_panel(target)
            }
            AppEvent::ProcessPanelAction(ProcessPanelKeyAction::Refresh) => {
                self.refresh_process_snapshot()
            }
            AppEvent::ProcessPanelAction(ProcessPanelKeyAction::None) => {}
            AppEvent::ProcessPanelSnapshot {
                session_id,
                generation,
                rows,
                notice,
            } => {
                let is_current = self
                    .session
                    .as_ref()
                    .is_some_and(|session| session.metadata.id == session_id)
                    && generation == self.process_snapshot_generation;
                if !is_current {
                    return Ok(false);
                }
                self.process_snapshot_in_flight = false;
                let mut changed = self.chat_widget.state_mut().set_process_snapshots(rows);
                if let Some(notice) = notice {
                    self.chat_widget
                        .state_mut()
                        .set_process_panel_notice(notice);
                    changed = true;
                }
                if changed {
                    self.tui.render_requester().schedule_render();
                }
            }
            AppEvent::ExitRequested => return self.request_exit(),
            AppEvent::InterruptRequested => return Ok(self.interrupt()),
            AppEvent::PickerSessionSelected(session_id) => {
                self.session_picker = None;
                self.chat_widget.state_mut().bump_interaction_generation();
                self.start_resume_open(session_id)?;
            }
            AppEvent::PickerCancelled => {
                self.session_picker = None;
                self.restore_queued_inputs_after_resume_interrupted();
                self.tui.render_requester().schedule_render();
            }
            AppEvent::RenderRequested => self.tui.render_requester().schedule_render(),
            AppEvent::ResizeRenderRequested => self
                .tui
                .draw_after_resize(&mut self.chat_widget, self.session_picker.as_ref())?,
        }
        Ok(false)
    }

    async fn handle_at_path_resolved(
        &mut self,
        sequence: u64,
        expanded_input: String,
        draft: InputDraft,
        result: Result<ResolvedAtPaths, String>,
    ) -> anyhow::Result<()> {
        let restore_to_composer = self.take_pending_async_input_restore(sequence);
        match result {
            Ok(resolved) => {
                let input_text =
                    append_directory_context(&expanded_input, &resolved.directory_context);
                self.pending_input_submissions.insert(
                    sequence,
                    PendingInputSubmission::Ready {
                        input: QueuedInput::with_extra_attachments(
                            input_text,
                            draft,
                            resolved.attachments,
                        )
                        .with_submission_sequence(sequence),
                        restore_to_composer,
                        record_history: !restore_to_composer,
                    },
                );
            }
            Err(message) => {
                self.pending_input_submissions.insert(
                    sequence,
                    PendingInputSubmission::AttachFailed { draft, message },
                );
            }
        }
        self.flush_ready_input_submissions()
    }

    fn mark_pending_async_inputs_for_restore(&mut self) {
        self.restore_async_input_sequences_before = self
            .restore_async_input_sequences_before
            .max(self.chat_widget.state().current_input_submission_sequence());
        if let Some(turn_id) = self.session_task.active_turn_id() {
            self.defer_input_restores_until_turn_id = Some(turn_id);
        }
    }

    fn take_pending_async_input_restore(&mut self, sequence: u64) -> bool {
        async_input_sequence_should_restore(sequence, self.restore_async_input_sequences_before)
    }

    async fn handle_ready_input(
        &mut self,
        sequence: u64,
        input: QueuedInput,
    ) -> anyhow::Result<()> {
        self.pending_input_submissions.insert(
            sequence,
            PendingInputSubmission::Ready {
                input: input.with_submission_sequence(sequence),
                restore_to_composer: false,
                // 普通输入也必须等前序 @path 预检完成后再记录历史；否则 A 正在
                // 异步解析时提交普通 B，会先写 B、后写 A，导致 ↑ 的顺序倒置。
                record_history: true,
            },
        );
        self.flush_ready_input_submissions()
    }

    async fn handle_ready_steer_input(
        &mut self,
        sequence: u64,
        input: QueuedInput,
    ) -> anyhow::Result<()> {
        if !input.attachments().is_empty() {
            let state = self.chat_widget.state_mut();
            state.set_status_notice(ATTACHMENT_STEER_QUEUE_NOTICE);
            state.push_system(ATTACHMENT_STEER_QUEUE_NOTICE);
            self.tui.render_requester().schedule_render();
            return self.handle_ready_input(sequence, input).await;
        }
        if !input_can_interrupt_and_steer(&input, self.chat_widget.state().slash_catalog()) {
            return self.handle_ready_input(sequence, input).await;
        }
        let input = input.with_submission_sequence(sequence);
        if self
            .session_task
            .request_tool_boundary_steer(sequence, &input)
            .await
        {
            // 已被当前 turn 立即接纳的纯文本 steer 不经过普通队列；保留既有的
            // 历史语义，但不能让它等待尚在预检的附件输入。
            self.chat_widget
                .state_mut()
                .record_submitted_draft(input.draft().clone());
            self.skip_input_submission_sequence(sequence);
            let pending_steer = self.session_task.pending_steer_preview_text();
            self.chat_widget
                .state_mut()
                .set_pending_tool_boundary_steer(pending_steer);
            self.tui.render_requester().schedule_render();
            return Ok(());
        }
        self.handle_ready_input(sequence, input).await
    }

    fn flush_ready_input_submissions(&mut self) -> anyhow::Result<()> {
        let mut drafts_to_restore = Vec::new();
        while let Some(submission) = self
            .pending_input_submissions
            .remove(&self.next_input_sequence_to_submit)
        {
            let sequence = self.next_input_sequence_to_submit;
            self.advance_input_submission_sequence();
            match submission {
                PendingInputSubmission::Ready {
                    input,
                    restore_to_composer,
                    record_history,
                } => {
                    if pending_submission_should_restore(
                        sequence,
                        restore_to_composer,
                        self.restore_async_input_sequences_before,
                    ) {
                        let draft = input.into_draft();
                        if self.pending_restore_should_wait_for_turn(sequence) {
                            self.deferred_input_restores.insert(sequence, draft);
                        } else {
                            drafts_to_restore.push(draft);
                        }
                    } else {
                        self.restore_ready_input_drafts(&mut drafts_to_restore);
                        if record_history {
                            self.chat_widget
                                .state_mut()
                                .record_submitted_draft(input.draft().clone());
                        }
                        self.submit_input(input)?;
                    }
                }
                PendingInputSubmission::AttachFailed { draft, message } => {
                    let state = self.chat_widget.state_mut();
                    state.record_submitted_draft(draft.clone());
                    state.push_failed_input(
                        draft.visible_text().to_string(),
                        format!("Attach failed: {message}"),
                    );
                    self.tui.render_requester().schedule_render();
                }
            }
        }
        self.restore_ready_input_drafts(&mut drafts_to_restore);
        Ok(())
    }

    fn restore_ready_input_drafts(&mut self, drafts: &mut Vec<InputDraft>) {
        if drafts.is_empty() {
            return;
        }
        let drafts = std::mem::take(drafts);
        self.chat_widget
            .state_mut()
            .restore_input_drafts_preserving_current(drafts);
        self.tui.render_requester().schedule_render();
    }

    fn pending_restore_should_wait_for_turn(&self, sequence: u64) -> bool {
        pending_restore_should_wait_for_turn(
            self.session_task.active_turn_id(),
            self.defer_input_restores_until_turn_id,
            sequence,
            self.restore_async_input_sequences_before,
        )
    }

    fn restore_cancelled_turn_inputs(&mut self, pending_steers: Vec<PendingSteerInput>) {
        self.defer_input_restores_until_turn_id = None;
        let restore_before = self.restore_async_input_sequences_before;
        let mut restore_entries = Vec::new();
        for (order, (sequence, draft)) in std::mem::take(&mut self.deferred_input_restores)
            .into_iter()
            .enumerate()
        {
            restore_entries.push(SequencedRestoreDraft {
                sequence: Some(sequence),
                order,
                draft,
            });
        }
        let base_order = restore_entries.len();
        for (offset, input) in self
            .chat_widget
            .state_mut()
            .drain_queued_inputs_for_restore_before(restore_before)
            .into_iter()
            .enumerate()
        {
            restore_entries.push(SequencedRestoreDraft {
                sequence: input.submission_sequence(),
                order: base_order.saturating_add(offset),
                draft: input.into_draft(),
            });
        }
        let base_order = restore_entries.len();
        for (offset, pending_steer) in pending_steers.into_iter().enumerate() {
            restore_entries.push(SequencedRestoreDraft {
                sequence: Some(pending_steer.sequence),
                order: base_order.saturating_add(offset),
                draft: pending_steer.input.into_draft(),
            });
        }
        let drafts = ordered_restore_drafts(restore_entries);
        if !drafts.is_empty() {
            self.chat_widget
                .state_mut()
                .restore_input_drafts_preserving_current(drafts);
            self.tui.render_requester().schedule_render();
        }
    }

    fn skip_input_submission_sequence(&mut self, sequence: u64) {
        mark_input_submission_sequence_skipped(
            &mut self.next_input_sequence_to_submit,
            &mut self.skipped_input_submission_sequences,
            sequence,
        );
    }

    fn advance_input_submission_sequence(&mut self) {
        advance_input_submission_sequence(
            &mut self.next_input_sequence_to_submit,
            &mut self.skipped_input_submission_sequences,
        );
    }

    fn handle_tui_event(&mut self, tui_event: TuiEvent) -> anyhow::Result<bool> {
        match tui_event {
            TuiEvent::Key(key) => {
                self.cleanup_activity.record_user_activity();
                if let Some(picker) = self.session_picker.as_mut() {
                    picker.handle_key_event(key);
                    self.tui.render_requester().schedule_render();
                } else if !management_panel_blocks_global_interrupt(
                    self.chat_widget.state().process_panel_visible(),
                    self.chat_widget.state().mcp_panel_visible(),
                ) && is_ctrl_c_key(key)
                    && self.chat_widget.state().input().is_empty()
                    && self.app_has_interruptible_work()
                {
                    return Ok(self.interrupt());
                } else {
                    let width = self.tui.render_width()?;
                    self.chat_widget.handle_key_event_for_width(key, width);
                }
            }
            TuiEvent::Paste(text) => {
                self.cleanup_activity.record_user_activity();
                if self.session_picker.is_none() {
                    self.chat_widget.handle_paste(text);
                }
            }
            TuiEvent::Resize => self.schedule_resize_render(),
            TuiEvent::Render => {
                self.tui
                    .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
                self.acknowledge_process_termination_render();
            }
        }
        Ok(false)
    }

    fn schedule_resize_render(&mut self) {
        if let Some(handle) = self.resize_render_handle.take() {
            handle.abort();
        }
        let app_event_tx = self.chat_widget.event_tx();
        self.resize_render_handle = Some(tokio::spawn(async move {
            sleep(RESIZE_REDRAW_DEBOUNCE).await;
            app_event_tx.send(AppEvent::ResizeRenderRequested);
        }));
    }

    fn submit_input(&mut self, input: QueuedInput) -> anyhow::Result<()> {
        let finalize_failed = self.chat_widget.state().finalize_failed();
        if finalize_failed {
            return Ok(());
        }
        let dispatch_blocked = input_dispatch_is_blocked(
            self.session_task.task_running(),
            self.start_handle.is_some(),
            self.resume_handle.is_some(),
        );
        let session_can_dispatch = self.session_can_dispatch_input();
        let action = classify_input(
            input.command_text(),
            self.chat_widget.state().slash_catalog(),
        );
        match route_input_submission(
            &action,
            finalize_failed,
            dispatch_blocked,
            session_can_dispatch,
            input_can_be_queued(self.chat_widget.state().status),
        ) {
            InputSubmissionRoute::Queue => {
                self.chat_widget.state_mut().queue_pending_turn(input);
                self.tui.render_requester().schedule_render();
            }
            InputSubmissionRoute::Dispatch => {
                let dispatch_next_after_input = session_can_dispatch
                    && !matches!(
                        action,
                        InputAction::Mcp | InputAction::Ps | InputAction::Subagents
                    );
                self.dispatch_input(input)?;
                if dispatch_next_after_input {
                    self.maybe_dispatch_next_queued_input()?;
                }
            }
            InputSubmissionRoute::Reject => {
                self.chat_widget
                    .state_mut()
                    .push_error("Session is not accepting input");
                self.tui.render_requester().schedule_render();
            }
        }
        Ok(())
    }

    fn dispatch_input(&mut self, input: QueuedInput) -> anyhow::Result<()> {
        let action = classify_input(
            input.command_text(),
            self.chat_widget.state().slash_catalog(),
        );
        if command_echoes(&action) {
            self.chat_widget
                .state_mut()
                .push_command_echo(input.text().to_string());
        }
        match action {
            InputAction::Send(_) => self.start_turn(input)?,
            InputAction::ShellCommand(command) => {
                let state = self.chat_widget.state_mut();
                state.settle_turn_animation_before_command();
                if command.trim().is_empty() {
                    state.push_failed_input(
                        input.command_text().to_string(),
                        "Shell command is empty",
                    );
                    self.tui.render_requester().schedule_render();
                } else {
                    self.start_user_shell_command(command)?;
                }
            }
            InputAction::Inbox => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.start_inbox()?;
            }
            InputAction::Compact => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.start_compact()?;
            }
            InputAction::Copy => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.copy_last_assistant_response();
            }
            InputAction::New => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.start_new_session()?;
            }
            InputAction::Resume => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.start_resume()?;
            }
            InputAction::Exit => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.start_finalize(FinalizeContinuation::Exit)?;
            }
            InputAction::Help => {
                let state = self.chat_widget.state_mut();
                state.settle_turn_animation_before_command();
                state.push_help();
                self.tui.render_requester().schedule_render();
            }
            InputAction::Skills => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                let text = skills_help_text(self.engine.available_skills());
                self.chat_widget.state_mut().push_system(text);
                self.tui.render_requester().schedule_render();
            }
            InputAction::Mcp => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.open_mcp_panel();
            }
            InputAction::Ps => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.open_process_panel();
            }
            InputAction::Subagents => {
                self.chat_widget
                    .state_mut()
                    .settle_turn_animation_before_command();
                self.open_delegation_panel();
            }
            InputAction::Unknown(command) => {
                let state = self.chat_widget.state_mut();
                state.settle_turn_animation_before_command();
                state.push_error(format!("Unknown command: {command}"));
                self.tui.render_requester().schedule_render();
            }
            InputAction::Ignore => {}
        }
        Ok(())
    }

    fn open_mcp_panel(&mut self) {
        if !self.mcp_panel_can_open() {
            self.chat_widget
                .state_mut()
                .set_status_notice("MCP panel is available when the current turn is idle.");
            self.tui.render_requester().schedule_render();
            return;
        }
        self.chat_widget.state_mut().clear_status_notice();
        if let Some(manager) = &self.mcp_manager {
            self.chat_widget
                .state_mut()
                .set_mcp_runtime(manager.config_path().to_path_buf(), manager.snapshot_sync());
        }
        self.chat_widget.state_mut().open_mcp_panel();
        self.tui.render_requester().schedule_render();
    }

    fn mcp_panel_can_open(&self) -> bool {
        mcp_panel_can_open_from_parts(
            self.session_picker.is_some(),
            self.session_task.task_running(),
            self.start_handle.is_some(),
            self.resume_handle.is_some(),
            self.chat_widget.state().status,
        )
    }

    fn open_process_panel(&mut self) {
        self.chat_widget.state_mut().clear_status_notice();
        self.chat_widget.state_mut().open_process_panel();
        self.refresh_process_snapshot();
        self.tui.render_requester().schedule_render();
    }

    fn open_delegation_panel(&mut self) {
        self.chat_widget.state_mut().clear_status_notice();
        self.chat_widget.state_mut().open_delegation_panel();
        self.tui.render_requester().schedule_render();
    }

    fn invalidate_process_panel_snapshot(&mut self) {
        self.process_snapshot_generation = self.process_snapshot_generation.wrapping_add(1);
        self.process_snapshot_in_flight = false;
        // sender drop 会唤醒后台 worker；它随后携带旧 generation 回灌，UI 会安全忽略。
        self.process_termination_render_acks.clear();
    }

    fn acknowledge_process_termination_render(&mut self) {
        let acknowledgements = std::mem::take(&mut self.process_termination_render_acks);
        for (_, acknowledgement) in acknowledgements {
            let _ = acknowledgement.send(());
        }
    }

    /// `/mcp` 是 active-turn live panel；连接状态可以在没有面板操作的情况下由 MCP tool
    /// 异步改变，因此打开时不能只读取一次 snapshot。
    fn refresh_mcp_panel(&mut self) -> bool {
        if !self.chat_widget.state().mcp_panel_visible() {
            return false;
        }
        let Some(manager) = &self.mcp_manager else {
            return false;
        };
        self.chat_widget
            .state_mut()
            .set_mcp_runtime(manager.config_path().to_path_buf(), manager.snapshot_sync());
        true
    }

    fn refresh_process_snapshot(&mut self) {
        if self.process_snapshot_in_flight {
            return;
        }
        let Some(session) = self.session.as_ref() else {
            return;
        };
        self.process_snapshot_generation = self.process_snapshot_generation.wrapping_add(1);
        self.process_snapshot_in_flight = true;
        let engine = self.engine.clone();
        let session_id = session.metadata.id.clone();
        let generation = self.process_snapshot_generation;
        let tx = self.chat_widget.event_tx();
        tokio::spawn(async move {
            tx.process_panel_snapshot(
                session_id.clone(),
                generation,
                engine.process_snapshots_for_session(&session_id).await,
                None,
            );
        });
    }

    async fn refresh_background_process_completions(&mut self) -> bool {
        let Some(session) = self.session.as_ref() else {
            return false;
        };
        let events = self
            .engine
            .drain_background_process_completions(session)
            .await;
        for event in events {
            self.chat_widget.state_mut().apply_event(event);
        }
        self.chat_widget
            .state_mut()
            .take_scrollback_rewrite_required()
    }

    fn terminate_process_from_panel(&mut self, target: ProcessTerminationTarget) {
        let Some(session) = self.session.as_ref() else {
            return;
        };
        self.chat_widget
            .state_mut()
            .mark_process_terminating(&target);
        self.process_snapshot_generation = self.process_snapshot_generation.wrapping_add(1);
        // 终止是 mutation：在它带回与本次 generation 对应的 snapshot 前，不能让周期性
        // refresh 启动更高代读取，否则旧的 running snapshot 可能覆盖 optimistic terminating。
        self.process_snapshot_in_flight = true;
        let generation = self.process_snapshot_generation;
        let (render_acknowledgement, render_observed) = oneshot::channel();
        self.process_termination_render_acks
            .insert(generation, render_acknowledgement);
        // 确认页切回列表后必须至少 draw 一帧 optimistic 状态；后台 worker 会等这次
        // draw 的 ack 后才读取 authoritative snapshot，避免黄色状态只存在于内存。
        self.tui.render_requester().schedule_render();
        let engine = self.engine.clone();
        let session_id = session.metadata.id.clone();
        let tx = self.chat_widget.event_tx();
        tokio::spawn(async move {
            // 先确认用户已经看到 optimistic `terminating`，再保留一个短暂可感知的显示窗口，
            // 才开始可能立即让 watcher 移除 entry 的硬终止。只让 snapshot 延后不足以保证
            // 该状态真正可见。
            let _ = render_observed.await;
            sleep(PROCESS_TERMINATE_OPTIMISTIC_VISIBLE_FOR).await;
            let notice = engine
                .terminate_process_for_session(
                    &session_id,
                    &target.process_id,
                    target.subagent_id.as_deref(),
                    target.instance_id,
                )
                .await
                .err()
                .map(|error| {
                    let message = error.to_string();
                    if message.contains("already exited") {
                        "Already exited".into()
                    } else {
                        format!("Terminate failed: {message}")
                    }
                });
            // 无论 hard terminate 成功与否都重新读取 authoritative snapshot：自然退出与
            // 发信号之间的竞态不能把乐观 `terminating` 行长期留在面板里。
            let rows = engine.process_snapshots_for_session(&session_id).await;
            tx.process_panel_snapshot(session_id, generation, rows, notice);
        });
    }

    fn start_mcp_panel_request(&mut self, request: McpPanelRequest) {
        let Some(manager) = self.mcp_manager.clone() else {
            self.chat_widget
                .state_mut()
                .set_mcp_notice("MCP manager is not available in this session.");
            self.tui.render_requester().schedule_render();
            return;
        };
        let server_name = request.server_name().to_string();
        let runtime_transition = match &request {
            McpPanelRequest::Reconnect { .. }
            | McpPanelRequest::SetEnabled { enabled: true, .. } => {
                manager.begin_server_reconnecting_runtime(&server_name)
            }
            McpPanelRequest::SetEnabled { enabled: false, .. } => {
                manager.begin_server_disabled_runtime(&server_name)
            }
        };
        let operation_generation = runtime_transition.generation();
        let operation_id = self.chat_widget.state_mut().begin_mcp_request(&request);
        let worker_tx = self.worker_tx.clone();
        while let Some(result) = self.mcp_operation_tasks.try_join_next() {
            report_mcp_operation_task_result(result);
        }
        self.mcp_operation_tasks.spawn(async move {
            let (error, is_disable_request) =
                execute_mcp_panel_request(Arc::clone(&manager), request, runtime_transition).await;
            if let Some(error) = error.as_ref().filter(|_| !is_disable_request) {
                // lifecycle 已摘除旧 client；无论是关闭超时还是持久化/建连失败，都不能把旧 ready
                // snapshot 回滚回来。generation 校验也保证过期操作不会覆盖后续状态。
                manager.mark_server_failed_runtime_if_current(
                    &server_name,
                    operation_generation,
                    error.clone(),
                );
            };
            let outcome = McpOperationOutcome {
                snapshot: manager.snapshot_sync(),
                error,
            };
            let _ = worker_tx.send(WorkerEvent::McpOperationFinished {
                server_name,
                operation_id,
                outcome,
            });
        });
        self.tui.render_requester().schedule_render();
    }

    fn session_can_dispatch_input(&self) -> bool {
        self.session.is_some()
            && !input_dispatch_is_blocked(
                self.session_task.task_running(),
                self.start_handle.is_some(),
                self.resume_handle.is_some(),
            )
            && self.chat_widget.state().input_accepts_text()
    }

    fn runtime_lease_for_worker(&self) -> anyhow::Result<crate::session::SessionRuntimeLease> {
        self._runtime_lease
            .as_ref()
            .map(crate::session::SessionRuntimeLease::clone_for_worker)
            .ok_or_else(|| anyhow::anyhow!("session runtime lease is unavailable"))
    }

    fn maybe_dispatch_next_queued_input(&mut self) -> anyhow::Result<()> {
        while self.session_can_dispatch_input() {
            let Some(next_input) = self.chat_widget.state_mut().pop_queued_turn() else {
                return Ok(());
            };
            self.dispatch_input(next_input)?;
            if self.session_task.task_running() {
                return Ok(());
            }
        }
        Ok(())
    }

    fn restore_queued_inputs_after_resume_interrupted(&mut self) {
        if !resume_interruption_can_restore_queued_inputs(
            self.session_task.task_running(),
            self.start_handle.is_some(),
            self.resume_handle.is_some(),
        ) {
            return;
        }
        self.chat_widget
            .state_mut()
            .restore_queued_inputs_to_composer();
        self.mark_pending_async_inputs_for_restore();
    }

    fn start_turn(&mut self, input: QueuedInput) -> anyhow::Result<()> {
        let Some(session) = self.session.clone() else {
            return Ok(());
        };
        let runtime_lease = self.runtime_lease_for_worker()?;
        let visible_text = input.command_text().to_string();
        self.chat_widget
            .state_mut()
            .begin_pending_turn(visible_text);
        self.tui
            .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
        self.session_task.spawn_tracked_turn(
            self.engine.clone(),
            session,
            runtime_lease,
            input,
            self.worker_tx.clone(),
        );
        Ok(())
    }

    fn start_user_shell_command(&mut self, command: String) -> anyhow::Result<()> {
        let Some(session) = self.session.clone() else {
            return Ok(());
        };
        let runtime_lease = self.runtime_lease_for_worker()?;
        self.tui
            .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
        self.session_task.spawn_tracked_user_shell_command(
            self.engine.clone(),
            session,
            runtime_lease,
            command,
            self.worker_tx.clone(),
        );
        Ok(())
    }

    fn request_exit(&mut self) -> anyhow::Result<bool> {
        if self.chat_widget.state().finalize_failed() {
            return Ok(true);
        }
        if self.resume_handle.is_some() {
            self.chat_widget
                .state_mut()
                .push_system("Session switch is running, please wait");
            self.tui.render_requester().schedule_render();
            return Ok(false);
        }
        if self.session_task.task_running() {
            self.chat_widget
                .state_mut()
                .push_system("Session task is running");
            self.tui.render_requester().schedule_render();
            return Ok(false);
        }

        if exit_request_is_noop(
            self.chat_widget.state().status,
            self.session_task.finalize_running(),
            self.resume_handle.is_some(),
        ) {
            return Ok(false);
        }

        if self.session.is_some() {
            self.chat_widget
                .state_mut()
                .settle_turn_animation_before_command();
            self.start_finalize(FinalizeContinuation::Exit)
        } else {
            self.chat_widget
                .state_mut()
                .push_system("Exit requested during initialization");
            self.abort_start_and_restore_queue();
            Ok(true)
        }
    }

    fn start_compact(&mut self) -> anyhow::Result<()> {
        let Some(session) = self.session.clone() else {
            return Ok(());
        };
        let runtime_lease = self.runtime_lease_for_worker()?;
        self.session_task.spawn_tracked_compact(
            self.engine.clone(),
            session,
            runtime_lease,
            self.worker_tx.clone(),
        );
        self.tui.render_requester().schedule_render();
        Ok(())
    }

    fn start_inbox(&mut self) -> anyhow::Result<()> {
        let Some(session) = self.session.clone() else {
            return Ok(());
        };
        let runtime_lease = self.runtime_lease_for_worker()?;
        self.session_task.spawn_tracked_inbox(
            self.engine.clone(),
            session,
            runtime_lease,
            self.worker_tx.clone(),
        );
        self.tui.render_requester().schedule_render();
        Ok(())
    }

    fn continue_session_switch(&mut self, target: SessionSwitchTarget) -> anyhow::Result<()> {
        let current_is_empty = !current_session_has_content(
            self.session
                .as_ref()
                .map(|session| session.metadata.message_count),
            self.current_session_has_real_user_input,
        );
        if current_is_empty {
            let target_deletes_old_session = matches!(
                &target,
                SessionSwitchTarget::Resume(reservation)
                    if reservation.temporary_session_id.is_some()
            );
            let old_session_id = self
                .session
                .as_ref()
                .map(|session| session.metadata.id.clone());
            self.begin_session_switch_target(target)?;
            if let Some(session_id) = old_session_id.filter(|_| !target_deletes_old_session) {
                self.schedule_empty_session_delete(session_id, "切换后临时空 session");
            }
            return Ok(());
        }
        self.start_finalize(FinalizeContinuation::Switch(target))?;
        Ok(())
    }

    fn begin_session_switch_target(&mut self, target: SessionSwitchTarget) -> anyhow::Result<()> {
        match target {
            SessionSwitchTarget::New => self.begin_new_session_startup()?,
            SessionSwitchTarget::Resume(reservation) => {
                self.begin_resume_session_startup(*reservation)?;
            }
        }
        Ok(())
    }

    fn begin_new_session_startup(&mut self) -> anyhow::Result<()> {
        self.invalidate_process_panel_snapshot();
        self.session = None;
        self._runtime_lease = None;
        self.current_session_has_real_user_input = false;
        let state = self.chat_widget.state_mut();
        state.reset_for_session_switch();
        state.session_id = None;
        state.agent_id = Some(self.engine.agent_id().to_string());
        state.model_name = Some(self.engine.session_model().to_string());
        state.set_context_window(self.engine.context_window());
        state.message_count = 0;
        state.turn_count = 0;
        state.apply_event(SessionEvent::StartupProgress {
            label: "initializing agent...".into(),
        });
        self.tui
            .draw_after_state_reload(&mut self.chat_widget, self.session_picker.as_ref())?;
        self.start_handle = Some(spawn_start_worker(
            self.engine.clone(),
            self.max_attempts,
            self.worker_tx.clone(),
        ));
        Ok(())
    }

    fn begin_resume_session_startup(
        &mut self,
        mut reservation: ResumeSessionReservation,
    ) -> anyhow::Result<()> {
        self.invalidate_process_panel_snapshot();
        let temporary_session_id = reservation.temporary_session_id.take();
        self.session = Some(reservation.session);
        self._runtime_lease = Some(reservation.runtime_lease);
        self.current_session_has_real_user_input = true;
        let session = self
            .session
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("resume target session is unavailable"))?;
        let state = self.chat_widget.state_mut();
        state.reset_for_session_switch();
        state.session_id = Some(session.metadata.id.to_string());
        state.agent_id = Some(session.metadata.agent_id.to_string());
        state.model_name = Some(self.engine.session_model().to_string());
        state.set_context_window(self.engine.context_window());
        state.message_count = session.metadata.message_count;
        state.turn_count = 0;
        state.apply_event(SessionEvent::StartupProgress {
            label: "loading session history...".into(),
        });
        if let Some(session_id) = temporary_session_id {
            self.schedule_empty_session_delete(session_id, "Resume 后临时空 session");
        }
        self.tui
            .draw_after_state_reload(&mut self.chat_widget, self.session_picker.as_ref())?;
        self.resume_handle = Some(spawn_resume_history_worker(
            self.engine.clone(),
            session,
            self.worker_tx.clone(),
        ));
        Ok(())
    }

    fn install_resume_history_and_start_inbox(
        &mut self,
        outcome: ResumeHistoryOutcome,
    ) -> anyhow::Result<()> {
        let session = outcome.session;
        let state = self.chat_widget.state_mut();
        state.message_count = session.metadata.message_count;
        state.turn_count = outcome.turn_count;
        if let Some(total) = outcome.local_claim_count {
            state.apply_event(SessionEvent::LocalClaimsUpdated { total });
        }
        if let Some(used_tokens) = outcome.context_used_tokens {
            state.apply_event(SessionEvent::ContextUsageUpdated { used_tokens });
        }
        state.push_historical_timeline_turns(&outcome.last_turns);
        if let Some(warning) = outcome.journal_warning {
            state.push_system(warning);
        }
        state.push_system(format!("Session {} resumed.", session.metadata.id));
        self.session = Some(session.clone());
        self.tui
            .draw_after_state_reload(&mut self.chat_widget, self.session_picker.as_ref())?;
        self.resume_handle = Some(spawn_resume_inbox_worker(
            self.engine.clone(),
            session,
            self.worker_tx.clone(),
        ));
        Ok(())
    }

    fn schedule_empty_session_delete(&self, session_id: SessionId, context: &'static str) {
        let engine = self.engine.clone();
        tokio::spawn(async move {
            match engine.delete_empty_session(&session_id).await {
                Ok(true) => {}
                Ok(false) => {
                    log::warn!(target: "session_tui", "{context} 未被删除: {session_id}")
                }
                Err(error) => log::warn!(
                    target: "session_tui",
                    "删除{context}失败 ({session_id}): {error:#}"
                ),
            }
        });
    }

    fn start_resume(&mut self) -> anyhow::Result<()> {
        if self.start_handle.is_some() {
            self.chat_widget
                .state_mut()
                .push_system("Session still initializing, please wait.");
            self.tui.render_requester().schedule_render();
            return Ok(());
        }
        if self.session_task.task_running()
            || self.resume_handle.is_some()
            || self.resume_switch_pending
            || self.finalize_continuation.is_some()
        {
            self.chat_widget
                .state_mut()
                .push_system("A task is running, please wait.");
            self.tui.render_requester().schedule_render();
            return Ok(());
        }
        self.resume_handle = Some(spawn_resume_list_worker(
            self.engine.clone(),
            self.worker_tx.clone(),
        ));
        self.tui.render_requester().schedule_render();
        Ok(())
    }

    fn start_resume_open(&mut self, session_id: crate::claim::SessionId) -> anyhow::Result<()> {
        self.session_picker = None;
        if self.resume_handle.is_some() {
            self.tui.render_requester().schedule_render();
            return Ok(());
        }
        let temporary_session_id = self
            .session
            .as_ref()
            .filter(|session| {
                !current_session_has_content(
                    Some(session.metadata.message_count),
                    self.current_session_has_real_user_input,
                )
            })
            .map(|session| session.metadata.id.clone());
        self.resume_switch_pending = self.session.is_some();
        self.resume_handle = Some(spawn_resume_preflight_worker(
            self.engine.clone(),
            session_id,
            temporary_session_id,
            self.worker_tx.clone(),
        ));
        self.tui.render_requester().schedule_render();
        Ok(())
    }

    fn start_new_session(&mut self) -> anyhow::Result<()> {
        if self.start_handle.is_some() {
            self.chat_widget
                .state_mut()
                .push_system("Session still initializing, please wait.");
            self.tui.render_requester().schedule_render();
            return Ok(());
        }
        if self.session.is_none()
            || self.session_task.task_running()
            || self.resume_handle.is_some()
            || self.resume_switch_pending
            || self.finalize_continuation.is_some()
        {
            self.chat_widget
                .state_mut()
                .push_system("A task is running, please wait.");
            self.tui.render_requester().schedule_render();
            return Ok(());
        }
        self.chat_widget.state_mut().bump_interaction_generation();
        let current_is_empty = !current_session_has_content(
            self.session
                .as_ref()
                .map(|session| session.metadata.message_count),
            self.current_session_has_real_user_input,
        );
        if current_is_empty {
            let old_session_id = self
                .session
                .as_ref()
                .map(|session| session.metadata.id.clone());
            self.begin_new_session_startup()?;
            if let Some(session_id) = old_session_id {
                self.schedule_empty_session_delete(session_id, "切换后临时空 session");
            }
        } else {
            self.start_finalize(FinalizeContinuation::Switch(SessionSwitchTarget::New))?;
        }
        Ok(())
    }

    fn interrupt(&mut self) -> bool {
        if self.session_task.can_request_tool_boundary_cancel() {
            let already_pending = self.session_task.pending_cancel_requested();
            let cancel_requested = !already_pending
                && self
                    .session_task
                    .request_tool_boundary_cancel("user cancelled turn");
            if cancel_requested {
                let state = self.chat_widget.state_mut();
                state.set_status_notice(TURN_CANCEL_PENDING_NOTICE);
                state.push_system(TURN_CANCEL_PENDING_NOTICE);
            }
            self.tui.render_requester().schedule_render();
            if cancel_requested {
                self.mark_pending_async_inputs_for_restore();
            }
            false
        } else if self.session_task.cancel_active_shell() {
            self.chat_widget
                .state_mut()
                .push_system("Shell command cancelling");
            self.tui.render_requester().schedule_render();
            false
        } else if self.session_task.has_active_shell() {
            self.chat_widget
                .state_mut()
                .push_system("Shell command is cancelling");
            self.tui.render_requester().schedule_render();
            false
        } else if self.session_task.has_active_turn() {
            self.chat_widget
                .state_mut()
                .push_system("Turn is finishing");
            self.tui.render_requester().schedule_render();
            false
        } else if self.session_task.task_running() {
            self.chat_widget
                .state_mut()
                .push_system("Session task is running");
            self.tui.render_requester().schedule_render();
            false
        } else if let Some(handle) = self.start_handle.take() {
            handle.abort();
            self.chat_widget
                .state_mut()
                .restore_queued_inputs_to_composer();
            self.mark_pending_async_inputs_for_restore();
            self.tui.render_requester().schedule_render();
            true
        } else {
            false
        }
    }

    fn app_has_interruptible_work(&self) -> bool {
        self.session_task.task_running() || self.start_handle.is_some()
    }

    fn abort_start_and_restore_queue(&mut self) {
        if let Some(handle) = self.start_handle.take() {
            handle.abort();
        }
        self.chat_widget
            .state_mut()
            .restore_queued_inputs_to_composer();
        self.mark_pending_async_inputs_for_restore();
        self.tui.render_requester().schedule_render();
    }

    fn copy_last_assistant_response(&mut self) {
        let Some(text) = self
            .chat_widget
            .state()
            .last_committed_assistant_text()
            .map(ToString::to_string)
        else {
            self.chat_widget
                .state_mut()
                .push_system("暂无可复制的 Assistant 回复。");
            self.tui.render_requester().schedule_render();
            return;
        };
        let tx = self.chat_widget.event_tx();
        let interaction_generation = self.chat_widget.state().interaction_generation();
        tokio::spawn(async move {
            tx.clipboard_text_written(interaction_generation, write_text_to_clipboard(text).await);
        });
    }

    fn complete_finalize_continuation(
        &mut self,
        continuation: SessionFinalizeContinuation,
    ) -> anyhow::Result<bool> {
        match finalize_success_action(continuation) {
            FinalizeSuccessAction::Exit => {
                // Finalize worker 会先发 SessionClosed，再发 FinalizeFinished。SessionClosed
                // 只排队重绘；这里退出前必须同步画最后一帧，避免 drop 时清掉尚未落入
                // scrollback 的 "{session_id} closed"。
                self.tui
                    .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
                Ok(true)
            }
            FinalizeSuccessAction::Install(target) => {
                self.begin_session_switch_target(target)?;
                Ok(false)
            }
        }
    }

    fn restore_after_switch_finalize_failure(&mut self, target: SessionSwitchTarget) {
        match target {
            SessionSwitchTarget::New => {}
            SessionSwitchTarget::Resume(_reservation) => {
                // 薄 reservation 不回滚目标状态；drop lease 后它会按 Interrupted 再次可见。
            }
        }
    }

    fn start_finalize(
        &mut self,
        continuation: SessionFinalizeContinuation,
    ) -> anyhow::Result<bool> {
        if self.resume_handle.is_some() {
            self.chat_widget
                .state_mut()
                .push_system("Resume is running, please wait");
            self.tui.render_requester().schedule_render();
            return Ok(false);
        }
        let Some(session) = self.session.clone() else {
            return Ok(false);
        };
        if self.session_task.finalize_running() {
            return Ok(false);
        }
        self.finalize_continuation = Some(continuation);
        self.chat_widget
            .handle_session_event(SessionEvent::StatusChanged {
                status: SessionRuntimeStatus::Finalizing,
            });
        self.tui
            .draw(&mut self.chat_widget, self.session_picker.as_ref())?;
        let runtime_lease = self.runtime_lease_for_worker()?;
        if let Some(supervisor) = self.supervisor.clone() {
            self.session_task.spawn_tracked_finalize_enqueue(
                self.engine.clone(),
                session,
                runtime_lease,
                supervisor,
                self.worker_tx.clone(),
            );
        } else {
            self.session_task.spawn_tracked_finalize(
                self.engine.clone(),
                session,
                runtime_lease,
                self.worker_tx.clone(),
            );
        }
        Ok(false)
    }
}

impl Drop for SessionTuiApp {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_housekeeping_handle.take() {
            self.cleanup_activity.request_shutdown();
            drop(handle);
        }
    }
}

fn finalize_enqueue_exit_message(job_id: &str, session_id: &crate::claim::SessionId) -> String {
    format!(
        "\nBackground finalize enqueued: {job_id}\nCheck with: acn supervisor jobs\nResume this session with: --resume {session_id}\n\n"
    )
}

fn recap_enqueue_result_belongs_to_visible_session(
    visible_session_id: Option<&str>,
    result_session_id: &SessionId,
) -> bool {
    visible_session_id == Some(result_session_id.as_str())
}

fn interaction_event_belongs_to_current(current_generation: u64, event_generation: u64) -> bool {
    current_generation == event_generation
}

fn finalize_success_action<T>(continuation: FinalizeContinuation<T>) -> FinalizeSuccessAction<T> {
    match continuation {
        FinalizeContinuation::Exit => FinalizeSuccessAction::Exit,
        FinalizeContinuation::Switch(target) => FinalizeSuccessAction::Install(target),
    }
}

fn append_directory_context(user_text: &str, directory_context: &str) -> String {
    if directory_context.is_empty() {
        return user_text.to_string();
    }
    let mut expanded = user_text.to_string();
    if !expanded.ends_with('\n') {
        expanded.push('\n');
    }
    expanded.push('\n');
    expanded.push_str(directory_context);
    expanded
}

fn preview_temporary_paths(result: &Result<Vec<PreviewFile>, PreviewFailure>) -> Vec<PathBuf> {
    match result {
        Ok(files) => files
            .iter()
            .filter(|file| file.temporary)
            .map(|file| file.path.clone())
            .collect(),
        Err(failure) => failure.temporary_paths.clone(),
    }
}

fn spawn_preview_task(
    tasks: &mut JoinSet<Vec<PathBuf>>,
    tx: AppEventSender,
    interaction_generation: u64,
    targets: Vec<PreviewTarget>,
    workspace_root: PathBuf,
) {
    tasks.spawn(async move {
        let result = preview_attachments_task(targets, workspace_root).await;
        let temporary_paths = preview_temporary_paths(&result);
        tx.preview_launched(interaction_generation, result);
        temporary_paths
    });
}

async fn cleanup_preview_temp_files(
    tracked_paths: &mut Vec<PathBuf>,
    tasks: &mut JoinSet<Vec<PathBuf>>,
) {
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(paths) => tracked_paths.extend(paths),
            Err(error) => {
                log::warn!(target: "session_tui", "附件预览任务收束失败: {error}")
            }
        }
    }
    tracked_paths.sort();
    tracked_paths.dedup();
    // Preview.app 已把成功打开的内容载入内存；删除底层文件不影响已打开的窗口。
    for path in tracked_paths.drain(..) {
        let _ = tokio::fs::remove_file(path).await;
    }
}

/// 把一组附件落成本地文件并用 `open` 交给系统默认应用（图片 / PDF 为
/// Preview.app）。`open` 每次调用都会确定性地打开新窗口并前置，没有
/// Quick Look 面板复用导致"看着还是上一张"的问题；文件准备走 spawn_blocking。
async fn preview_attachments_task(
    targets: Vec<PreviewTarget>,
    workspace_root: PathBuf,
) -> Result<Vec<PreviewFile>, PreviewFailure> {
    if !cfg!(target_os = "macos") {
        return Err(PreviewFailure {
            message: "附件预览仅支持 macOS".into(),
            temporary_paths: Vec::new(),
        });
    }
    let files =
        match tokio::task::spawn_blocking(move || prepare_preview_files(targets, &workspace_root))
            .await
        {
            Ok(Ok(files)) => files,
            Ok(Err(error)) => {
                return Err(PreviewFailure {
                    message: error.source.to_string(),
                    temporary_paths: error.temporary_paths,
                });
            }
            Err(error) => {
                return Err(PreviewFailure {
                    message: format!("预览准备任务失败: {error}"),
                    temporary_paths: Vec::new(),
                });
            }
        };
    if files.is_empty() {
        return Err(PreviewFailure {
            message: "没有可预览的附件".into(),
            temporary_paths: Vec::new(),
        });
    }
    let temporary_paths = files
        .iter()
        .filter(|file| file.temporary)
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let mut command = tokio::process::Command::new("open");
    for file in &files {
        command.arg(&file.path);
    }
    // open 把文件交给 LaunchServices 后立即退出，等它的退出码即可拿到失败原因
    let status = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map_err(|error| PreviewFailure {
            message: format!("执行 open 失败: {error}"),
            temporary_paths: temporary_paths.clone(),
        })?;
    if !status.success() {
        return Err(PreviewFailure {
            message: format!("Open 退出异常: {status}"),
            temporary_paths,
        });
    }
    Ok(files)
}

async fn write_text_to_clipboard(text: String) -> Result<(), String> {
    if !cfg!(target_os = "macos") {
        return Err("当前仅支持 macOS 剪贴板写入".into());
    }
    let mut child = tokio::process::Command::new("pbcopy")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("启动 pbcopy 失败: {e}"))?;
    let Some(mut stdin) = child.stdin.take() else {
        return Err("Pbcopy stdin 不可用".into());
    };
    stdin
        .write_all(text.as_bytes())
        .await
        .map_err(|e| format!("写入 pbcopy 失败: {e}"))?;
    drop(stdin);
    let status = child
        .wait()
        .await
        .map_err(|e| format!("等待 pbcopy 失败: {e}"))?;
    if !status.success() {
        return Err(format!("Pbcopy 退出异常: {status}"));
    }
    Ok(())
}

fn input_can_be_queued(status: SessionRuntimeStatus) -> bool {
    input_accepts_text(status)
}

fn input_dispatch_is_blocked(
    session_task_running: bool,
    start_running: bool,
    resume_running: bool,
) -> bool {
    session_task_running || start_running || resume_running
}

fn route_input_submission(
    action: &InputAction,
    input_disabled_after_finalize_failure: bool,
    dispatch_blocked: bool,
    session_can_dispatch: bool,
    input_can_be_queued: bool,
) -> InputSubmissionRoute {
    if input_disabled_after_finalize_failure {
        InputSubmissionRoute::Reject
    } else if matches!(
        action,
        InputAction::Mcp | InputAction::Ps | InputAction::Subagents
    ) {
        // 管理面板只是前台 live view；运行中的 turn 不能把它们排入 queued input。
        InputSubmissionRoute::Dispatch
    } else if dispatch_blocked && input_can_be_queued {
        InputSubmissionRoute::Queue
    } else if session_can_dispatch {
        InputSubmissionRoute::Dispatch
    } else if input_can_be_queued {
        InputSubmissionRoute::Queue
    } else {
        InputSubmissionRoute::Reject
    }
}

fn exit_request_is_noop(
    status: SessionRuntimeStatus,
    finalize_running: bool,
    resume_running: bool,
) -> bool {
    resume_running
        || finalize_running
        || matches!(
            status,
            SessionRuntimeStatus::Finalizing | SessionRuntimeStatus::Closed
        )
}

fn mcp_panel_can_open_from_parts(
    picker_open: bool,
    session_task_running: bool,
    start_running: bool,
    resume_running: bool,
    status: SessionRuntimeStatus,
) -> bool {
    let _ = (session_task_running, start_running, resume_running, status);
    !picker_open
}

fn management_panel_blocks_global_interrupt(
    process_panel_visible: bool,
    mcp_panel_visible: bool,
) -> bool {
    process_panel_visible || mcp_panel_visible
}

fn command_echoes(action: &InputAction) -> bool {
    !matches!(
        action,
        InputAction::Send(_)
            | InputAction::ShellCommand(_)
            | InputAction::Mcp
            | InputAction::Ps
            | InputAction::Subagents
            | InputAction::Ignore
    )
}

fn mcp_startup_warnings(snapshot: &McpRuntimeState) -> Vec<String> {
    let mut warnings = snapshot
        .servers
        .values()
        .filter(|server| server.status == McpServerStatus::Failed)
        .map(|server| {
            format!(
                "MCP server {} failed: {}",
                server.name,
                redact_mcp_sensitive_text(server.last_error.as_deref().unwrap_or("Unknown error"))
            )
        })
        .collect::<Vec<_>>();
    if let Some(error) = &snapshot.startup_error {
        warnings.insert(
            0,
            format!(
                "MCP initialization failed: {}",
                redact_mcp_sensitive_text(error)
            ),
        );
    }
    warnings
}

fn normalized_skill_description(description: &str) -> String {
    let collapsed = description.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        "--".to_string()
    } else {
        collapsed
    }
}

fn truncate_to_display_width(text: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if UnicodeWidthStr::width(text) <= max_width {
        return text.to_string();
    }
    if max_width <= 1 {
        return "…".to_string();
    }
    let target = max_width.saturating_sub(1);
    let mut out = String::new();
    let mut width = 0usize;
    for ch in text.chars() {
        let ch_width = ch.width().unwrap_or(0);
        if width.saturating_add(ch_width) > target {
            break;
        }
        out.push(ch);
        width = width.saturating_add(ch_width);
    }
    out.push('…');
    out
}

fn pad_to_display_width(text: &str, width: usize) -> String {
    let used = UnicodeWidthStr::width(text);
    if used >= width {
        text.to_string()
    } else {
        format!("{text}{}", " ".repeat(width - used))
    }
}

fn skill_table_row(name: &str, description: &str) -> String {
    format!(
        "{}  {}",
        pad_to_display_width(
            &truncate_to_display_width(name, SKILLS_NAME_COL_WIDTH),
            SKILLS_NAME_COL_WIDTH
        ),
        truncate_to_display_width(description, SKILLS_DESC_COL_WIDTH)
    )
}

fn skills_help_text(skills: &[SkillSummary]) -> String {
    if skills.is_empty() {
        return "Available skills\n(no workspace skills loaded)".to_string();
    }
    let mut lines = vec![
        "Available skills".to_string(),
        skill_table_row("Name", "Desc"),
    ];
    for skill in skills {
        let description = normalized_skill_description(&skill.description);
        lines.push(skill_table_row(skill.name.trim(), &description));
    }
    lines.join("\n")
}

fn resume_interruption_can_restore_queued_inputs(
    session_task_running: bool,
    start_running: bool,
    resume_running: bool,
) -> bool {
    !session_task_running && !start_running && !resume_running
}

fn async_input_sequence_should_restore(sequence: u64, restore_before: u64) -> bool {
    sequence < restore_before
}

fn pending_submission_should_restore(
    sequence: u64,
    explicit_restore: bool,
    restore_before: u64,
) -> bool {
    explicit_restore || async_input_sequence_should_restore(sequence, restore_before)
}

fn current_session_has_content(
    current_message_count: Option<usize>,
    has_real_user_input: bool,
) -> bool {
    current_message_count.is_some_and(|message_count| message_count != 0 || has_real_user_input)
}

fn pending_restore_should_wait_for_turn(
    active_turn_id: Option<u64>,
    defer_until_turn_id: Option<u64>,
    sequence: u64,
    restore_before: u64,
) -> bool {
    active_turn_id.is_some()
        && active_turn_id == defer_until_turn_id
        && async_input_sequence_should_restore(sequence, restore_before)
}

struct SequencedRestoreDraft {
    sequence: Option<u64>,
    order: usize,
    draft: InputDraft,
}

fn ordered_restore_drafts(mut entries: Vec<SequencedRestoreDraft>) -> Vec<InputDraft> {
    entries.sort_by_key(|entry| (entry.sequence.unwrap_or(u64::MAX), entry.order));
    entries.into_iter().map(|entry| entry.draft).collect()
}

fn input_can_interrupt_and_steer(input: &QueuedInput, catalog: &SlashCommandCatalog) -> bool {
    input.attachments().is_empty()
        && matches!(
            classify_input(input.command_text(), catalog),
            InputAction::Send(_)
        )
}

fn mark_input_submission_sequence_skipped(
    next_sequence: &mut u64,
    skipped: &mut BTreeSet<u64>,
    sequence: u64,
) {
    if sequence < *next_sequence {
        return;
    }
    if sequence == *next_sequence {
        advance_input_submission_sequence(next_sequence, skipped);
    } else {
        skipped.insert(sequence);
    }
}

fn advance_input_submission_sequence(next_sequence: &mut u64, skipped: &mut BTreeSet<u64>) {
    *next_sequence = next_sequence.saturating_add(1);
    while skipped.remove(&*next_sequence) {
        *next_sequence = next_sequence.saturating_add(1);
    }
}

/// 执行已在 UI 线程同步切换 generation 的 MCP panel lifecycle 操作。
async fn execute_mcp_panel_request(
    manager: Arc<McpConnectionManager>,
    request: McpPanelRequest,
    runtime_transition: McpRuntimeTransition,
) -> (Option<String>, bool) {
    let operation_generation = runtime_transition.generation();
    let is_disable_request = matches!(&request, McpPanelRequest::SetEnabled { enabled: false, .. });
    let error = match request {
        McpPanelRequest::SetEnabled {
            server_name,
            enabled: false,
        } => {
            // 用户选择必须先落盘；旧 transport 的完整收束可能接近退出 drain 上限，不能让
            // 资源回收延迟反过来取消持久化，导致下一次启动重新启用 server。
            let persistence_error = manager.disable_server(&server_name).await.err();
            let release_error = runtime_transition.wait_for_transport_release().await.err();
            persistence_error
                .or(release_error)
                .map(|error| error.to_string())
        }
        McpPanelRequest::Reconnect { server_name } => {
            // replacement 必须等待旧 transport 真正退出，避免同 server 同时存在两条 session。
            match runtime_transition.wait_for_transport_release().await {
                Ok(()) => manager
                    .reconnect_server_if_current(&server_name, operation_generation)
                    .await
                    .err()
                    .map(|error| error.to_string()),
                Err(error) => Some(error.to_string()),
            }
        }
        McpPanelRequest::SetEnabled {
            server_name,
            enabled: true,
        } => match runtime_transition.wait_for_transport_release().await {
            Ok(()) => manager
                .enable_server_if_current(&server_name, operation_generation)
                .await
                .err()
                .map(|error| error.to_string()),
            Err(error) => Some(error.to_string()),
        },
    };
    (error, is_disable_request)
}

async fn drain_mcp_operation_tasks(
    tasks: &mut JoinSet<()>,
    manager: Option<&McpConnectionManager>,
    drain_timeout: Duration,
) {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            report_mcp_operation_task_result(result);
        }
    };
    if timeout(drain_timeout, drain).await.is_err() {
        log::warn!(
            "MCP lifecycle operations did not finish within {:?} during TUI exit",
            drain_timeout
        );
        // Enable/Reconnect 可能仍在 connect handshake 内。先让 manager 标记并取消 attempt，
        // shutdown 才会等待 pending transport 的 release fence；若先 abort worker，guard 会在
        // cancellation 标记前完成 fence，异步 close 可能随 Tokio runtime 退出而被截断。
        if let Some(manager) = manager {
            manager.shutdown().await;
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

fn report_mcp_operation_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        log::warn!("MCP lifecycle operation task failed: {error}");
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::{delete, post};
    use axum::{Json, Router};
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    use super::*;
    use crate::mcp::config::{
        read_mcp_json_config, write_mcp_json_config_atomic, McpJsonConfig, McpServerConfig,
    };

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn preview_shutdown_cleans_temp_file_before_completion_event_is_consumed() {
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
        use base64::Engine as _;

        let temp_dir = tempfile::tempdir().unwrap();
        let missing_path = temp_dir.path().join("missing-preview-target");
        let (sender, mut receiver) = AppEventSender::channel();
        let mut tasks = JoinSet::new();
        spawn_preview_task(
            &mut tasks,
            sender,
            7,
            vec![
                PreviewTarget::InlineImage {
                    name: "[Image #1]".into(),
                    media_type: "image/png".into(),
                    data: BASE64_STANDARD.encode(b"temporary preview image"),
                },
                PreviewTarget::AtPath {
                    raw_path: missing_path.to_string_lossy().into_owned(),
                },
            ],
            temp_dir.path().to_path_buf(),
        );

        let mut tracked_paths = Vec::new();
        cleanup_preview_temp_files(&mut tracked_paths, &mut tasks).await;

        let AppEvent::PreviewLaunched {
            interaction_generation,
            result: Err(failure),
        } = receiver.try_recv().unwrap()
        else {
            panic!("expected queued preview failure");
        };
        assert_eq!(interaction_generation, 7);
        assert_eq!(failure.temporary_paths.len(), 1);
        assert!(!failure.temporary_paths[0].exists());
    }

    #[tokio::test]
    async fn tui_disable_persists_config_when_transport_release_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new()
                .route(
                    "/mcp",
                    post(|Json(payload): Json<Value>| async move {
                        let id = payload.get("id").cloned().unwrap_or(Value::Null);
                        match payload.get("method").and_then(Value::as_str) {
                            Some("server/discover") => Json(json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {"code": -32601, "message": "Method not found"}
                            }))
                            .into_response(),
                            Some("notifications/initialized") => {
                                StatusCode::ACCEPTED.into_response()
                            }
                            Some("initialize") => {
                                let mut headers = HeaderMap::new();
                                headers.insert(
                                    "Mcp-Session-Id",
                                    HeaderValue::from_static("test-session"),
                                );
                                (
                                    headers,
                                    Json(json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "protocolVersion": "2025-11-25",
                                            "capabilities": {"tools": {}},
                                            "serverInfo": {"name": "app-test", "version": "1.0.0"}
                                        }
                                    })),
                                )
                                    .into_response()
                            }
                            Some("tools/list") => Json(json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {"tools": []}
                            }))
                            .into_response(),
                            _ => Json(json!({"jsonrpc": "2.0", "id": id, "result": {}}))
                                .into_response(),
                        }
                    }),
                )
                .route(
                    "/mcp",
                    delete(|| async {
                        // 模拟 close_with_timeout 已取消 driver 但远端 DELETE 永远不返回。
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        StatusCode::NO_CONTENT
                    }),
                );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path.clone(),
            dir.path().to_path_buf(),
            None,
        ));
        manager.refresh_all().await.unwrap();

        let transition = manager.begin_server_disabled_runtime("http_server");
        let operation_manager = Arc::clone(&manager);
        let operation = tokio::spawn(async move {
            execute_mcp_panel_request(
                operation_manager,
                McpPanelRequest::SetEnabled {
                    server_name: "http_server".to_string(),
                    enabled: false,
                },
                transition,
            )
            .await
        });
        timeout(Duration::from_secs(1), async {
            loop {
                if read_mcp_json_config(&path).await.unwrap().servers["http_server"].enabled
                    == Some(false)
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Disable 必须在等待旧 transport 释放前先持久化");
        assert!(
            !operation.is_finished(),
            "fixture 的旧 transport 此时必须仍在关闭窗口内"
        );
        let (error, is_disable_request) = operation.await.unwrap();

        assert!(is_disable_request);
        assert!(
            error.is_some(),
            "Release timeout should remain visible to the panel"
        );
        assert_eq!(
            manager.snapshot_sync().servers["http_server"].status,
            McpServerStatus::Disabled
        );
        let persisted = read_mcp_json_config(&path).await.unwrap();
        assert_eq!(
            persisted.servers["http_server"].enabled,
            Some(false),
            "TUI Disable must persist even if old transport enters quarantine"
        );
    }

    #[tokio::test]
    async fn accepted_tui_disable_is_drained_before_app_exit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec!["-c".to_string(), "exit 0".to_string()],
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path.clone(),
            dir.path().to_path_buf(),
            None,
        ));
        let transition = manager.begin_server_disabled_runtime("stdio_server");
        let operation_gate = Arc::new(tokio::sync::Notify::new());
        let operation_release = Arc::clone(&operation_gate);
        let operation_manager = Arc::clone(&manager);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            operation_release.notified().await;
            let (error, is_disable_request) = execute_mcp_panel_request(
                operation_manager,
                McpPanelRequest::SetEnabled {
                    server_name: "stdio_server".to_string(),
                    enabled: false,
                },
                transition,
            )
            .await;
            assert!(is_disable_request);
            assert!(error.is_none(), "unexpected disable error: {error:?}");
        });
        let release = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            operation_gate.notify_one();
        });

        drain_mcp_operation_tasks(
            &mut tasks,
            Some(manager.as_ref()),
            MCP_OPERATION_EXIT_DRAIN_TIMEOUT,
        )
        .await;
        release.await.unwrap();

        let persisted = read_mcp_json_config(&path).await.unwrap();
        assert_eq!(persisted.servers["stdio_server"].enabled, Some(false));
        manager.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tui_exit_waits_for_cancelled_connect_transport_release() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("slow_initialize_stdio_mock.sh");
        let pid_path = dir.path().join("slow_initialize.pid");
        tokio::fs::write(
            &script_path,
            r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf '%s\n' "$$" > "$MCP_TUI_EXIT_PID_FILE"
      sleep 30
      ;;
  esac
done
"#,
        )
        .await
        .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_TUI_EXIT_PID_FILE".to_string(),
            pid_path.display().to_string(),
        );
        let mut server = McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            env,
            Vec::new(),
        );
        server.startup_timeout_secs = Some(30);
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert("stdio_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));
        let transition = manager.begin_server_reconnecting_runtime("stdio_server");
        let operation_manager = Arc::clone(&manager);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            execute_mcp_panel_request(
                operation_manager,
                McpPanelRequest::Reconnect {
                    server_name: "stdio_server".to_string(),
                },
                transition,
            )
            .await;
        });
        wait_for_test_file(&pid_path).await;
        let pid = tokio::fs::read_to_string(&pid_path)
            .await
            .unwrap()
            .trim()
            .to_string();

        drain_mcp_operation_tasks(
            &mut tasks,
            Some(manager.as_ref()),
            Duration::from_millis(20),
        )
        .await;
        manager.shutdown().await;

        assert!(
            wait_for_test_pid_exit(&pid, Duration::from_millis(500)).await,
            "TUI exit returned before the cancelled MCP connect transport released PID {pid}"
        );
    }

    #[cfg(unix)]
    async fn wait_for_test_pid_exit(pid: &str, limit: Duration) -> bool {
        timeout(limit, async {
            loop {
                let status = tokio::process::Command::new("kill")
                    .args(["-0", pid])
                    .stderr(Stdio::null())
                    .status()
                    .await;
                if !matches!(status, Ok(status) if status.success()) {
                    return;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .is_ok()
    }

    async fn wait_for_test_file(path: &std::path::Path) {
        timeout(Duration::from_secs(2), async {
            loop {
                if tokio::fs::try_exists(path).await.unwrap_or(false) {
                    return;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("stdio fixture did not create its expected marker file");
    }

    #[test]
    fn compaction_noop_notice_distinguishes_nothing_new_from_raw_tail_budget() {
        assert_eq!(
            compaction_noop_notice(SessionCompactionNoopReason::NothingNew),
            "Nothing new to compact."
        );
        assert_eq!(
            compaction_noop_notice(SessionCompactionNoopReason::RawTailWithinBudget),
            "New history is still within the compact raw-tail budget; No compaction needed."
        );
    }

    #[test]
    fn delegation_terminal_notice_only_waits_for_non_delegation_notice() {
        assert!(!status_notice_blocks_delegation_notice(None));
        assert!(!status_notice_blocks_delegation_notice(Some("")));
        assert!(!status_notice_blocks_delegation_notice(Some(
            "Subagent old completed"
        )));
        assert!(status_notice_blocks_delegation_notice(Some(
            "MCP panel is available when the current turn is idle."
        )));
    }

    #[test]
    fn skills_help_text_renders_bounded_name_desc_table() {
        let text = skills_help_text(&[
            SkillSummary {
                name: "very-long-skill-name-that-will-be-truncated".into(),
                description: "项目文档索引 查询架构说明、配置参数、工具能力、会话恢复、团队协作、故障排查、版本兼容、测试约定、部署方式和发布流程。".into(),
                spec_path: std::path::PathBuf::from("/tmp/very-long/SKILL.md"),
            },
            SkillSummary {
                name: "plain".into(),
                description: "   ".into(),
                spec_path: std::path::PathBuf::from("/tmp/plain/SKILL.md"),
            },
        ]);

        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines[0], "Available skills");
        assert!(lines[1].starts_with("Name"));
        assert!(lines[1].contains("Desc"));
        assert!(lines[2].starts_with("very-long-skill-name-"));
        assert!(!lines[2].contains("will-be-truncated"));
        assert!(lines[2].ends_with('…'));
        assert!(lines[3].contains("--"));
        assert!(!text.contains("SKILL.md"));
        for line in lines.iter().skip(1) {
            assert!(
                unicode_width::UnicodeWidthStr::width(*line)
                    <= SKILLS_NAME_COL_WIDTH + 2 + SKILLS_DESC_COL_WIDTH,
                "Line exceeded table width: {line:?}"
            );
        }
    }

    #[test]
    fn input_can_be_queued_while_initializing_or_busy() {
        assert!(input_can_be_queued(SessionRuntimeStatus::Initializing));
        assert!(input_can_be_queued(SessionRuntimeStatus::Running));
        assert!(input_can_be_queued(SessionRuntimeStatus::SyncingInbox));
        assert!(input_can_be_queued(SessionRuntimeStatus::Compacting));
        assert!(input_can_be_queued(SessionRuntimeStatus::Open));
        assert!(!input_can_be_queued(SessionRuntimeStatus::Finalizing));
        assert!(!input_can_be_queued(SessionRuntimeStatus::Closed));
    }

    #[test]
    fn input_dispatch_is_blocked_while_resume_worker_runs() {
        assert!(input_dispatch_is_blocked(false, false, true));
        assert!(input_dispatch_is_blocked(false, true, false));
        assert!(input_dispatch_is_blocked(true, false, false));
        assert!(!input_dispatch_is_blocked(false, false, false));
    }

    #[test]
    fn session_switch_commands_queue_while_foreground_work_is_busy() {
        let catalog = SlashCommandCatalog::default();
        for command in ["/new", "/resume"] {
            let action = classify_input(command, &catalog);
            assert_eq!(
                route_input_submission(&action, false, true, false, true),
                InputSubmissionRoute::Queue
            );
        }
    }

    #[test]
    fn management_panels_dispatch_immediately_while_turn_is_running() {
        for command in ["/mcp", "/ps", "/subagents"] {
            let action = classify_input(command, &Default::default());
            assert_eq!(
                route_input_submission(&action, false, true, false, true),
                InputSubmissionRoute::Dispatch
            );
            assert_eq!(
                route_input_submission(&action, false, false, false, true),
                InputSubmissionRoute::Dispatch
            );
        }
    }

    #[test]
    fn management_panels_consume_ctrl_c_before_global_turn_cancel() {
        // `/ps` 确认页只允许白名单按键；`Ctrl-C` 不能越过 ChatWidget 的 panel 路由，
        // 变成全局 turn cancel。`/mcp` 也同样优先接收面板按键。
        assert!(management_panel_blocks_global_interrupt(true, false));
        assert!(management_panel_blocks_global_interrupt(false, true));
        assert!(!management_panel_blocks_global_interrupt(false, false));
    }

    #[test]
    fn finalize_failure_input_lock_rejects_all_submissions() {
        let catalog = SlashCommandCatalog::default();
        for action in [
            classify_input("继续说", &catalog),
            classify_input("/mcp", &catalog),
            classify_input("/subagents", &catalog),
            classify_input("/compact", &catalog),
            classify_input("!pwd", &catalog),
        ] {
            assert_eq!(
                route_input_submission(&action, true, false, true, true),
                InputSubmissionRoute::Reject
            );
            assert_eq!(
                route_input_submission(&action, true, true, true, true),
                InputSubmissionRoute::Reject
            );
        }
    }

    #[test]
    fn exit_request_is_noop_while_finalize_is_running_or_closed() {
        assert!(exit_request_is_noop(
            SessionRuntimeStatus::Open,
            true,
            false
        ));
        assert!(exit_request_is_noop(
            SessionRuntimeStatus::Open,
            false,
            true
        ));
        assert!(exit_request_is_noop(
            SessionRuntimeStatus::Finalizing,
            false,
            false
        ));
        assert!(exit_request_is_noop(
            SessionRuntimeStatus::Closed,
            false,
            false
        ));
        assert!(!exit_request_is_noop(
            SessionRuntimeStatus::Open,
            false,
            false
        ));
        assert!(!exit_request_is_noop(
            SessionRuntimeStatus::Error,
            false,
            false
        ));
    }

    #[test]
    fn mcp_panel_opens_during_active_turns_but_not_over_session_picker() {
        assert!(mcp_panel_can_open_from_parts(
            false,
            false,
            false,
            false,
            SessionRuntimeStatus::Open
        ));
        assert!(mcp_panel_can_open_from_parts(
            false,
            false,
            false,
            false,
            SessionRuntimeStatus::Running
        ));
        assert!(mcp_panel_can_open_from_parts(
            false,
            true,
            false,
            false,
            SessionRuntimeStatus::Open
        ));
        assert!(mcp_panel_can_open_from_parts(
            false,
            false,
            true,
            false,
            SessionRuntimeStatus::Open
        ));
        assert!(mcp_panel_can_open_from_parts(
            false,
            false,
            false,
            true,
            SessionRuntimeStatus::Open
        ));
        assert!(!mcp_panel_can_open_from_parts(
            true,
            false,
            false,
            false,
            SessionRuntimeStatus::Open
        ));
    }

    #[test]
    fn mcp_startup_warnings_include_top_level_initialization_error() {
        let snapshot = McpRuntimeState {
            servers: BTreeMap::new(),
            generations: BTreeMap::new(),
            startup_error: Some("bad json Authorization: Bearer secret-token".into()),
            workspace_root: None,
        };

        let warnings = mcp_startup_warnings(&snapshot);

        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("<redacted>"));
        assert!(!warnings[0].contains("secret-token"));
    }

    #[test]
    fn slash_command_actions_are_echoed_in_tui_transcript() {
        assert!(command_echoes(&InputAction::Help));
        assert!(command_echoes(&InputAction::Inbox));
        assert!(command_echoes(&InputAction::Compact));
        assert!(command_echoes(&InputAction::Copy));
        assert!(command_echoes(&InputAction::New));
        assert!(command_echoes(&InputAction::Resume));
        assert!(command_echoes(&InputAction::Skills));
        assert!(command_echoes(&InputAction::Exit));
        assert!(command_echoes(&InputAction::Unknown("/refresh".into())));
        assert!(!command_echoes(&InputAction::Mcp));
        assert!(!command_echoes(&InputAction::Ps));
        assert!(!command_echoes(&InputAction::Subagents));
        assert!(!command_echoes(&InputAction::Send("/help me".into())));
        assert!(!command_echoes(&InputAction::ShellCommand(
            "echo hi".into()
        )));
        assert!(!command_echoes(&InputAction::Ignore));
    }

    #[test]
    fn ctrl_enter_steer_accepts_only_plain_send_text() {
        let catalog = SlashCommandCatalog::default();
        assert!(input_can_interrupt_and_steer(
            &QueuedInput::from_text("调整当前方向"),
            &catalog
        ));
        assert!(!input_can_interrupt_and_steer(
            &QueuedInput::from_text("/help"),
            &catalog
        ));
        assert!(!input_can_interrupt_and_steer(
            &QueuedInput::from_text("/resume"),
            &catalog
        ));
        assert!(!input_can_interrupt_and_steer(
            &QueuedInput::from_text("!pwd"),
            &catalog
        ));
    }

    #[test]
    fn directory_context_is_hidden_from_display_draft_but_kept_in_model_text() {
        let draft = InputDraft::new("请看 @src/".into());
        let input = QueuedInput::with_extra_attachments(
            "请看 @src/\n\n[Referenced directory: src/]\nfile.rs".into(),
            draft,
            Vec::new(),
        );
        assert!(input.text().contains("[Referenced directory: src/]"));
        assert_eq!(input.command_text(), "请看 @src/");
    }

    #[test]
    fn append_directory_context_keeps_original_prompt_and_adds_separated_context() {
        assert_eq!(
            append_directory_context("请看 @src/", "[Referenced directory: src/]\nlib.rs"),
            "请看 @src/\n\n[Referenced directory: src/]\nlib.rs"
        );
        assert_eq!(append_directory_context("hello", ""), "hello");
    }

    #[test]
    fn finalize_enqueue_exit_message_matches_terminal_hint() {
        let session_id = "session_cf983ed9"
            .parse::<crate::claim::SessionId>()
            .unwrap();

        let message = finalize_enqueue_exit_message("job_1782441549295_83c4b4b8", &session_id);

        assert_eq!(
            message,
            "\nBackground finalize enqueued: job_1782441549295_83c4b4b8\nCheck with: acn supervisor jobs\nResume this session with: --resume session_cf983ed9\n\n"
        );
    }

    #[test]
    fn recap_enqueue_completion_is_scoped_to_visible_session() {
        let old_session_id = "session_cf983ed9".parse::<SessionId>().unwrap();

        assert!(recap_enqueue_result_belongs_to_visible_session(
            Some("session_cf983ed9"),
            &old_session_id
        ));
        assert!(!recap_enqueue_result_belongs_to_visible_session(
            Some("session_8a7b6c5d"),
            &old_session_id
        ));
        assert!(!recap_enqueue_result_belongs_to_visible_session(
            None,
            &old_session_id
        ));
    }

    #[test]
    fn interaction_completion_is_scoped_to_current_generation() {
        assert!(interaction_event_belongs_to_current(7, 7));
        assert!(!interaction_event_belongs_to_current(8, 7));
    }

    #[test]
    fn finalize_success_continuation_exits_or_installs_prepared_target() {
        assert!(matches!(
            finalize_success_action(FinalizeContinuation::<u8>::Exit),
            FinalizeSuccessAction::Exit
        ));
        assert!(matches!(
            finalize_success_action(FinalizeContinuation::Switch(7_u8)),
            FinalizeSuccessAction::Install(7)
        ));
    }

    #[test]
    fn session_switch_finalize_disables_new_input() {
        assert!(!input_accepts_text(SessionRuntimeStatus::Finalizing));
        let action = classify_input("must not be accepted", &Default::default());
        assert_eq!(
            route_input_submission(&action, false, true, false, false),
            InputSubmissionRoute::Reject
        );
    }

    #[test]
    fn resume_interruption_restores_queue_only_after_workers_stop() {
        assert!(resume_interruption_can_restore_queued_inputs(
            false, false, false
        ));
        assert!(!resume_interruption_can_restore_queued_inputs(
            true, false, false
        ));
        assert!(!resume_interruption_can_restore_queued_inputs(
            false, true, false
        ));
        assert!(!resume_interruption_can_restore_queued_inputs(
            false, false, true
        ));
    }

    #[test]
    fn current_session_content_includes_journal_only_user_input() {
        assert!(current_session_has_content(Some(0), true));
        assert!(current_session_has_content(Some(1), false));
        assert!(!current_session_has_content(Some(0), false));
        assert!(!current_session_has_content(None, true));
    }

    #[test]
    fn pending_async_input_restore_cutoff_matches_allocated_sequences_at_cancel() {
        let restore_before = 2;

        assert!(async_input_sequence_should_restore(0, restore_before));
        assert!(async_input_sequence_should_restore(1, restore_before));
        assert!(!async_input_sequence_should_restore(2, restore_before));
    }

    #[test]
    fn pending_submission_restore_cutoff_applies_to_buffered_non_async_entries() {
        let restore_before = 2;

        assert!(pending_submission_should_restore(1, false, restore_before));
        assert!(!pending_submission_should_restore(2, false, restore_before));
        assert!(pending_submission_should_restore(2, true, restore_before));
    }

    #[test]
    fn pending_restore_waits_only_for_the_turn_that_requested_restore() {
        assert!(pending_restore_should_wait_for_turn(Some(7), Some(7), 1, 2));
        assert!(!pending_restore_should_wait_for_turn(
            Some(8),
            Some(7),
            1,
            2
        ));
        assert!(!pending_restore_should_wait_for_turn(None, Some(7), 1, 2));
        assert!(!pending_restore_should_wait_for_turn(
            Some(7),
            Some(7),
            2,
            2
        ));
    }

    #[test]
    fn ordered_restore_drafts_preserves_async_before_later_pending_steer() {
        let drafts = ordered_restore_drafts(vec![
            SequencedRestoreDraft {
                sequence: Some(1),
                order: 0,
                draft: InputDraft::new("pending steer B".into()),
            },
            SequencedRestoreDraft {
                sequence: Some(0),
                order: 1,
                draft: InputDraft::new("async attachment A".into()),
            },
        ]);
        let mut state = super::super::TuiState::new();
        state.push_input_text("current draft C");
        state.restore_input_drafts_preserving_current(drafts);

        assert_eq!(
            state.input(),
            "async attachment A\npending steer B\ncurrent draft C"
        );
    }

    #[test]
    fn ordered_restore_drafts_keeps_unsequenced_entries_after_sequenced_inputs() {
        let drafts = ordered_restore_drafts(vec![
            SequencedRestoreDraft {
                sequence: None,
                order: 0,
                draft: InputDraft::new("legacy queued".into()),
            },
            SequencedRestoreDraft {
                sequence: Some(3),
                order: 1,
                draft: InputDraft::new("sequenced steer".into()),
            },
        ]);
        let restored = drafts
            .into_iter()
            .map(|draft| draft.expanded_text().to_string())
            .collect::<Vec<_>>();

        assert_eq!(restored, vec!["sequenced steer", "legacy queued"]);
    }

    #[test]
    fn skipped_input_submission_sequence_unblocks_later_buffered_entries() {
        let mut next = 0;
        let mut skipped = BTreeSet::new();

        mark_input_submission_sequence_skipped(&mut next, &mut skipped, 1);
        assert_eq!(next, 0);
        assert!(skipped.contains(&1));

        advance_input_submission_sequence(&mut next, &mut skipped);
        assert_eq!(next, 2);
        assert!(skipped.is_empty());
    }
}
