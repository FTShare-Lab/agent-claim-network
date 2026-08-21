//! 受管后台终端的进程域。
//!
//! 本模块负责 `code_run` 进程的 owner 隔离、生命周期与有界输出；MCP stdio child 不进入这里。
//! 对外核心类型是 `ProcessManager`、`ProcessOwner` 和 `ManagedProcess`。

mod manager;
mod output;
mod process_group;
mod pty;

pub(crate) use manager::{
    BackgroundProcessEvent, ManagedProcess, ProcessCompletion, ProcessCompletionDeliveryReceipt,
    ProcessDeliveryReceipt, ProcessManager, ProcessOwner, ProcessState, PtyInput,
    TerminateRequestResult,
};
pub(crate) use output::OutputCursor;
pub(crate) use process_group::{
    configure_process_group, observe_child_exit_without_reap, reap_direct_child_blocking,
    spawn_direct_child_reaper, terminate_process_group,
};
pub(crate) use pty::{spawn_pty, PtySpawned, PtyWatcherParts};
