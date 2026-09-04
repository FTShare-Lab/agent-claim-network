//! TUI 内部应用事件。
//!
//! 组件通过 `AppEventSender` 上抛用户意图，顶层 `SessionTuiApp` 统一决定
//! session 生命周期、turn 调度和退出顺序，避免底部输入区直接操纵 runtime。

use tokio::sync::mpsc;

use crate::attachment::NormalizedMedia;
use crate::claim::SessionId;

use super::at_path_completion::AtPathDirectoryEntry;
use super::attachment::{PreviewFailure, PreviewFile, PreviewTarget, ResolvedAtPaths};
use super::bottom_pane::InputDraft;
use super::input_queue::QueuedInput;
use super::mcp_panel::McpPanelRequest;
use super::process_panel::ProcessPanelKeyAction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AppEvent {
    SubmitInput {
        sequence: u64,
        input: QueuedInput,
    },
    SteerInput {
        sequence: u64,
        input: QueuedInput,
    },
    /// 剪贴板图片在 spawn_blocking 中读取 / 规格化后的回灌结果。
    ClipboardImageRead {
        interaction_generation: u64,
        input_revision: u64,
        result: Result<Option<NormalizedMedia>, String>,
    },
    /// 请求异步读取 `@path` 当前一级目录。
    AtPathDirectoryScan {
        generation: u64,
        directory: std::path::PathBuf,
        max_entries: usize,
    },
    /// `@path` 菜单一级目录扫描的回灌结果；generation 用于丢弃过期输入请求。
    AtPathDirectoryRead {
        generation: u64,
        directory: std::path::PathBuf,
        result: Result<Vec<AtPathDirectoryEntry>, String>,
    },
    /// `@path` 文件附件与目录上下文在 spawn_blocking 中解析后的回灌结果。
    AtPathResolved {
        sequence: u64,
        submitted_during_startup_recovery: bool,
        expanded_input: String,
        draft: InputDraft,
        result: Result<ResolvedAtPaths, String>,
    },
    /// Ctrl+O：请求预览附件（光标命中的一个，或输入框里的全部；
    /// 由 App 层负责落盘与拉起 Quick Look）。
    PreviewAttachment {
        interaction_generation: u64,
        targets: Vec<PreviewTarget>,
    },
    /// 预览文件准备 / 拉起完成的回灌结果（临时文件需登记以便退出清理）。
    PreviewLaunched {
        interaction_generation: u64,
        result: Result<Vec<PreviewFile>, PreviewFailure>,
    },
    /// `/copy` 写入系统剪贴板后的回灌结果。
    ClipboardTextWritten {
        interaction_generation: u64,
        result: Result<(), String>,
    },
    /// `/mcp` 面板触发的 server 操作。
    McpPanelRequest(McpPanelRequest),
    ProcessPanelAction(ProcessPanelKeyAction),
    ProcessPanelSnapshot {
        session_id: SessionId,
        generation: u64,
        rows: Vec<crate::tool::ProcessSnapshot>,
        notice: Option<String>,
    },
    ExitRequested,
    InterruptRequested,
    PickerSessionSelected(SessionId),
    PickerCancelled,
    RenderRequested,
    ResizeRenderRequested,
}

#[derive(Clone, Debug)]
pub(super) struct AppEventSender {
    tx: mpsc::UnboundedSender<AppEvent>,
}

impl AppEventSender {
    pub(super) fn channel() -> (Self, mpsc::UnboundedReceiver<AppEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    pub(super) fn send(&self, event: AppEvent) {
        let _ = self.tx.send(event);
    }

    pub(super) fn submit_input(&self, sequence: u64, input: QueuedInput) {
        self.send(AppEvent::SubmitInput { sequence, input });
    }

    pub(super) fn steer_input(&self, sequence: u64, input: QueuedInput) {
        self.send(AppEvent::SteerInput { sequence, input });
    }

    pub(super) fn clipboard_image_read(
        &self,
        interaction_generation: u64,
        input_revision: u64,
        result: Result<Option<NormalizedMedia>, String>,
    ) {
        self.send(AppEvent::ClipboardImageRead {
            interaction_generation,
            input_revision,
            result,
        });
    }

    pub(super) fn at_path_directory_scan(
        &self,
        generation: u64,
        directory: std::path::PathBuf,
        max_entries: usize,
    ) {
        self.send(AppEvent::AtPathDirectoryScan {
            generation,
            directory,
            max_entries,
        });
    }

    pub(super) fn at_path_directory_read(
        &self,
        generation: u64,
        directory: std::path::PathBuf,
        result: Result<Vec<AtPathDirectoryEntry>, String>,
    ) {
        self.send(AppEvent::AtPathDirectoryRead {
            generation,
            directory,
            result,
        });
    }

    pub(super) fn preview_attachment(
        &self,
        interaction_generation: u64,
        targets: Vec<PreviewTarget>,
    ) {
        self.send(AppEvent::PreviewAttachment {
            interaction_generation,
            targets,
        });
    }

    pub(super) fn preview_launched(
        &self,
        interaction_generation: u64,
        result: Result<Vec<PreviewFile>, PreviewFailure>,
    ) {
        self.send(AppEvent::PreviewLaunched {
            interaction_generation,
            result,
        });
    }

    pub(super) fn clipboard_text_written(
        &self,
        interaction_generation: u64,
        result: Result<(), String>,
    ) {
        self.send(AppEvent::ClipboardTextWritten {
            interaction_generation,
            result,
        });
    }

    pub(super) fn mcp_panel_request(&self, request: McpPanelRequest) {
        self.send(AppEvent::McpPanelRequest(request));
    }

    pub(super) fn process_panel_action(&self, action: ProcessPanelKeyAction) {
        self.send(AppEvent::ProcessPanelAction(action));
    }

    pub(super) fn process_panel_snapshot(
        &self,
        session_id: SessionId,
        generation: u64,
        rows: Vec<crate::tool::ProcessSnapshot>,
        notice: Option<String>,
    ) {
        self.send(AppEvent::ProcessPanelSnapshot {
            session_id,
            generation,
            rows,
            notice,
        });
    }

    pub(super) fn at_path_resolved(
        &self,
        sequence: u64,
        submitted_during_startup_recovery: bool,
        expanded_input: String,
        draft: InputDraft,
        result: Result<ResolvedAtPaths, String>,
    ) {
        self.send(AppEvent::AtPathResolved {
            sequence,
            submitted_during_startup_recovery,
            expanded_input,
            draft,
            result,
        });
    }

    pub(super) fn request_exit(&self) {
        self.send(AppEvent::ExitRequested);
    }

    pub(super) fn interrupt(&self) {
        self.send(AppEvent::InterruptRequested);
    }

    pub(super) fn request_render(&self) {
        self.send(AppEvent::RenderRequested);
    }

    pub(super) fn request_resize_render(&self) {
        self.send(AppEvent::ResizeRenderRequested);
    }
}
