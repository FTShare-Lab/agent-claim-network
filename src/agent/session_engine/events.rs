//! SessionEngine 对外事件与运行状态。
//!
//! 本模块只定义 TUI / supervisor 消费的稳定事件 DTO，
//! 以及 session turn event 到 session event 的轻量映射。业务流程仍由 facade 编排。

use crate::agent::runner::TeamServicesConnectionStatus;
use crate::agent::user_shell::UserShellCommandStatus;
use crate::api::{SessionTurnEvent, ToolCallSkipReason, ToolExecutionOutcome};
use crate::claim::{AgentId, ClaimId, DisputeId, SessionId, TraceId};
use crate::tool::diff::FileChange;

pub(super) fn preflight_session_event_to_turn_event(
    event: SessionEvent,
) -> Option<SessionTurnEvent> {
    match event {
        SessionEvent::Warning { message } => Some(SessionTurnEvent::Warning { message }),
        SessionEvent::CompactionStarted {
            compact_start_index,
            compact_end_index,
            recap_start_index,
            recap_end_index,
        } => Some(SessionTurnEvent::CompactionStarted {
            compact_start_index,
            compact_end_index,
            recap_start_index,
            recap_end_index,
        }),
        SessionEvent::CompactionCompleted { compacted_until } => {
            Some(SessionTurnEvent::CompactionCompleted { compacted_until })
        }
        SessionEvent::RecapRequested {
            session_id,
            recap_end_index,
        } => Some(SessionTurnEvent::RecapRequested {
            session_id,
            recap_end_index,
        }),
        SessionEvent::CompactionFailed { error } => {
            Some(SessionTurnEvent::CompactionFailed { error })
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    StartupProgress {
        label: String,
    },
    SessionStarted {
        session_id: SessionId,
        agent_id: AgentId,
    },
    StatusChanged {
        status: SessionRuntimeStatus,
    },
    /// 当前可见 turn 的 journal 标识；tool_use_id 只在该 turn 内唯一。
    TurnStarted {
        turn_id: String,
    },
    Warning {
        message: String,
    },
    TeamServicesConnectionUpdated {
        status: TeamServicesConnectionStatus,
    },
    UserMessageAccepted {
        text: String,
    },
    AssistantTextDelta {
        text: String,
    },
    AssistantOutputDiscarded,
    AssistantMessageCompleted {
        text: String,
    },
    NonStreamingFallbackAttemptStarted {
        attempt: u32,
        max_attempts: u32,
    },
    NonStreamingFallbackSucceeded {
        text: String,
    },
    ToolCallStarted {
        id: String,
        name: String,
        summary: String,
    },
    ToolCallSkipped {
        id: String,
        name: String,
        summary: String,
        reason: ToolCallSkipReason,
    },
    ToolCallProgress {
        id: String,
        summary: String,
    },
    ToolCallCompleted {
        id: String,
        summary: String,
        /// file 类工具修改成功时采集的 diff，供 TUI history 区渲染。
        file_change: Option<FileChange>,
        outcome: ToolExecutionOutcome,
    },
    ToolCallInterrupted {
        id: String,
        summary: String,
    },
    /// 受管 terminal 已登记并交给独立 watcher，不对应原始 tool result 的第二次完成。
    BackgroundProcessStarted {
        process_id: String,
        owner_agent_id: String,
        owner_root_session_id: String,
        owner_subagent_id: Option<String>,
    },
    /// 后台输出有新增；正文仍由 `write_stdin` / process snapshot 有界读取，避免 journal
    /// 持久化逐字节 output event。
    BackgroundProcessOutput {
        process_id: String,
        owner_agent_id: String,
        owner_root_session_id: String,
        owner_subagent_id: Option<String>,
    },
    BackgroundProcessStateChanged {
        process_id: String,
        owner_agent_id: String,
        owner_root_session_id: String,
        owner_subagent_id: Option<String>,
        status: String,
    },
    /// 独立 watcher 的后台 terminal 完成通知，不对应第二个 tool result。
    BackgroundProcessCompleted {
        process_id: String,
        /// 与 originating_tool_use_id 组合成 session 内稳定的展示关联键。
        originating_turn_id: Option<String>,
        /// 创建该后台进程的 code_run tool_use。仅供控制面更新原展示 cell。
        originating_tool_use_id: Option<String>,
        owner_agent_id: String,
        owner_root_session_id: String,
        owner_subagent_id: Option<String>,
        status: String,
        exit_code: Option<i32>,
        signal: Option<i32>,
        success: bool,
    },
    UserShellCommandStarted {
        command: String,
    },
    UserShellCommandCompleted {
        command: String,
        status: UserShellCommandStatus,
        exit_code: Option<i32>,
        duration_ms: u128,
        stdout: String,
        stderr: String,
        truncated: bool,
        message_count: usize,
    },
    UserShellCommandFailed {
        command: String,
        error: String,
    },
    ContextUsageUpdated {
        used_tokens: usize,
    },
    LocalClaimsUpdated {
        total: usize,
    },
    TurnCommitted {
        message_count: usize,
    },
    TurnCancelled {
        reason: String,
    },
    TurnInterrupted {
        reason: String,
    },
    TurnFailed {
        error: String,
    },
    FinalizeStarted,
    FinalizeCompleted {
        trace_id: Option<TraceId>,
        new_claim_ids: Vec<ClaimId>,
        updated_claim_ids: Vec<ClaimId>,
        new_dispute_ids: Vec<DisputeId>,
    },
    FinalizeFailed {
        error: String,
    },
    CompactionStarted {
        compact_start_index: usize,
        compact_end_index: usize,
        recap_start_index: usize,
        recap_end_index: usize,
    },
    CompactionCompleted {
        compacted_until: usize,
    },
    /// 请求 TUI 将冻结 target 的 recap 异步投递给 supervisor。
    RecapRequested {
        session_id: SessionId,
        recap_end_index: usize,
    },
    CompactionFailed {
        error: String,
    },
    InboxStarted,
    InboxCompleted {
        processed: usize,
        new_claim_ids: Vec<ClaimId>,
        updated_claim_ids: Vec<ClaimId>,
        new_dispute_ids: Vec<DisputeId>,
        deprecated_claim_ids: Vec<ClaimId>,
    },
    InboxFailed {
        error: String,
    },
    SessionClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRuntimeStatus {
    Initializing,
    Open,
    Running,
    SyncingInbox,
    Compacting,
    Finalizing,
    Error,
    Closed,
}

pub(super) fn emit_warnings<F>(warnings: &[String], emit: &mut F)
where
    F: FnMut(SessionEvent),
{
    for warning in warnings {
        emit(SessionEvent::Warning {
            message: warning.clone(),
        });
    }
}
