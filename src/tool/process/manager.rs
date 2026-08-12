use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::RngCore;
use tokio::sync::{mpsc, Mutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use super::output::{BoundedOutput, OutputCursor, ProcessOutput};
use super::process_group::{
    signal_process_group, terminate_process_group, ProcessGroupSignalResult,
};

/// ACN 进程会话 ID，不复用 OS PID。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ProcessId(String);

impl ProcessId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn random() -> Self {
        let mut bytes = [0_u8; 4];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(hex::encode(bytes))
    }
}

/// 同一个 root session 内的模型可见性边界。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ProcessOwner {
    pub(crate) owner_agent_id: String,
    pub(crate) root_session_id: String,
    pub(crate) subagent_id: Option<String>,
}

impl ProcessOwner {
    #[cfg(test)]
    pub(crate) fn main(root_session_id: impl Into<String>) -> Self {
        Self::main_for_agent("unknown-agent", root_session_id)
    }

    pub(crate) fn main_for_agent(
        owner_agent_id: impl Into<String>,
        root_session_id: impl Into<String>,
    ) -> Self {
        Self {
            owner_agent_id: owner_agent_id.into(),
            root_session_id: root_session_id.into(),
            subagent_id: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn subagent(
        root_session_id: impl Into<String>,
        subagent_id: impl Into<String>,
    ) -> Self {
        Self::subagent_for_agent("unknown-agent", root_session_id, subagent_id)
    }

    pub(crate) fn subagent_for_agent(
        owner_agent_id: impl Into<String>,
        root_session_id: impl Into<String>,
        subagent_id: impl Into<String>,
    ) -> Self {
        Self {
            owner_agent_id: owner_agent_id.into(),
            root_session_id: root_session_id.into(),
            subagent_id: Some(subagent_id.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessState {
    /// 已登记但控制句柄仍在挂载，不能被容量淘汰。
    Starting,
    Running,
    Terminating,
    Finished {
        exit_code: Option<i32>,
        signal: Option<i32>,
        success: bool,
    },
    Terminated {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    Error,
}

/// 一次 runtime/TUI 硬终止请求的线性化结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminateRequestResult {
    Requested,
    AlreadyTerminating,
    AlreadyExited,
}

/// 受管 terminal 的非 tool-result 生命周期事件。输出事件只表示“有新 output 可刷新”，
/// 不携带字节正文，避免把高频数据写入 journal。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackgroundProcessEvent {
    Started {
        process_id: String,
        owner: ProcessOwner,
    },
    Output {
        process_id: String,
        owner: ProcessOwner,
    },
    StateChanged {
        process_id: String,
        owner: ProcessOwner,
        status: String,
    },
}

/// 进程 watcher 独立于发起它的 tool call；完成后以此事件通知 TUI/journal，绝不伪装成
/// 原 code_run 的第二个 ToolCallCompleted。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessCompletion {
    pub(crate) root_session_id: String,
    pub(crate) owner: ProcessOwner,
    pub(crate) process_id: String,
    /// 区分被容量淘汰后重用同一 logical process_id 的两次 allocation；不是 OS PID。
    /// lifecycle context 会把它作为稳定语义字段提供给模型。
    pub(crate) instance_id: u64,
    pub(crate) status: String,
    pub(crate) exit_code: Option<i32>,
    pub(crate) signal: Option<i32>,
    pub(crate) finished_at: SystemTime,
    /// 终态运行时长必须在完成瞬间固定；不能在动态上下文投影时继续按 wall clock 增长。
    pub(crate) elapsed_minutes: u64,
}

/// completion notification 随 provider request 提交时使用的内部回执。
///
/// logical ID 可以在旧 entry 被淘汰后重用，因此不能只靠 `process_id` 删除已成功投递的
/// notification；必须同时绑定 allocation instance。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ProcessCompletionDeliveryReceipt {
    process_id: String,
    instance_id: u64,
}

/// 一个 tool result 携带的输出交付回执。它只在紧随该 tool result 的 provider request
/// 成功后提交；不是模型可见协议字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessDeliveryReceipt {
    pub(crate) process_id: String,
    /// 同一个 8 位 logical ID 在旧 entry 被移除后可以重抽；receipt 必须同时绑定这次
    /// allocation，不能让迟到的 provider response 提交给后来复用同 ID 的 entry。
    instance_id: u64,
    owner: ProcessOwner,
    pub(crate) stdout_cursor: OutputCursor,
    pub(crate) stderr_cursor: OutputCursor,
    pub(crate) final_result: bool,
}

impl ProcessState {
    pub(crate) fn is_live(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Terminating)
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Starting | Self::Running => "running",
            Self::Terminating => "terminating",
            Self::Finished { .. } => "finished",
            Self::Terminated { .. } => "terminated",
            Self::Error => "error",
        }
    }
}

pub(crate) struct PtyInput {
    pub(crate) bytes: Vec<u8>,
    // permit 与排队字节同生命周期；writer 接收并完成写入、或 receiver 被丢弃后才释放，
    // 因而 128 条 message 上限之外还有严格的总字节上限。
    _byte_permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
struct PtyInputChannel {
    sender: mpsc::Sender<PtyInput>,
    byte_budget: Arc<Semaphore>,
    max_bytes: usize,
}

enum ProcessInput {
    Pty(PtyInputChannel),
}

struct ProcessControl {
    process_group_id: Option<i32>,
    input: Option<ProcessInput>,
}

#[derive(Debug, Clone, Copy)]
struct OutputDeliveryState {
    committed_stdout: OutputCursor,
    committed_stderr: OutputCursor,
    pending: Option<(OutputCursor, OutputCursor)>,
    inflight: bool,
}

/// completion 事件与最终输出的 provider 交付可以并发到达。这个状态把两者线性化：
/// 一旦 final tool result 已被 provider 成功确认，迟到 watcher 不得再次把同一实例放回
/// 动态上下文的 completion notification 队列。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionRegistration {
    Unrecorded,
    Registered,
    FinalOutputDelivered,
}

#[derive(Debug, Clone)]
struct ProviderCompletionNotification {
    completion: ProcessCompletion,
    receipt: ProcessCompletionDeliveryReceipt,
    inflight: bool,
}

/// 仅用于覆盖 reserve 后 handoff 的 hard-abort 回归窗口，不参与生产运行时。
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct ProcessHandoffTestGate {
    entered: Notify,
    release: Notify,
}

#[cfg(test)]
impl ProcessHandoffTestGate {
    pub(crate) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Debug, Default)]
struct ProcessManagerLifecycle {
    /// 关闭标记只用于阻止 cleanup 期间的迟到 reserve / completion；它们不能随着历史
    /// session 永久积累。超过容量的最旧标记会被移除，此时对应 entry 已经被 cleanup
    /// 从 store 撤下，不会再污染新的 process entry。
    closed_owners: BTreeSet<ProcessOwner>,
    closed_owner_order: VecDeque<ProcessOwner>,
    closed_root_sessions: BTreeSet<String>,
    closed_root_session_order: VecDeque<String>,
}

const MAX_CLOSED_LIFECYCLE_MARKERS: usize = 1024;

impl ProcessManagerLifecycle {
    fn mark_owner_closed(&mut self, owner: ProcessOwner) {
        if self.closed_owners.insert(owner.clone()) {
            self.closed_owner_order.push_back(owner);
        }
        while self.closed_owner_order.len() > MAX_CLOSED_LIFECYCLE_MARKERS {
            if let Some(expired) = self.closed_owner_order.pop_front() {
                self.closed_owners.remove(&expired);
            }
        }
    }

    fn mark_root_session_closed(&mut self, root_session_id: String) {
        if self.closed_root_sessions.insert(root_session_id.clone()) {
            self.closed_root_session_order.push_back(root_session_id);
        }
        while self.closed_root_session_order.len() > MAX_CLOSED_LIFECYCLE_MARKERS {
            if let Some(expired) = self.closed_root_session_order.pop_front() {
                self.closed_root_sessions.remove(&expired);
            }
        }
    }
}

impl std::fmt::Debug for ProcessControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessControl")
            .field("process_group_id", &self.process_group_id)
            .field("stdin_open", &self.input.is_some())
            .finish()
    }
}

/// 受管进程的共享状态。watcher 独立于 tool future，因而初始 yield 结束或调用 future 被 abort 后
/// 仍持续 drain 输出并推进终态。
#[derive(Debug)]
pub(crate) struct ManagedProcess {
    pub(crate) id: ProcessId,
    pub(crate) instance_id: u64,
    pub(crate) owner: ProcessOwner,
    /// 创建此终端 session 的 tool_use。它只用于 explicit cancel 强制 abort 后，
    /// 将仍存活的受管进程精确回写到对应的 Interrupted cell，不能作为模型可见 ID。
    originating_tool_use_id: Option<String>,
    pub(crate) command: String,
    pub(crate) code_type: String,
    pub(crate) cwd: String,
    pub(crate) tty: bool,
    pub(crate) started_at: SystemTime,
    finished_at: Mutex<Option<SystemTime>>,
    pub(crate) state: Mutex<ProcessState>,
    pub(crate) stdout: Mutex<BoundedOutput>,
    pub(crate) stderr: Mutex<BoundedOutput>,
    control: Mutex<ProcessControl>,
    /// 所有向受管 PGID 发信号的路径都先取得此 gate。watcher 在真正 reap root 前独占它，
    /// 再清空 PGID；这样不会在 PID/PGID 已可能复用后向无关进程组发信号。
    process_group_signal_gate: Arc<Mutex<()>>,
    /// Drop 路径不能 await Tokio mutex。PGID 另存为短临界区的同步锁，使 runtime 异常
    /// 丢弃最后一个 manager handle 时仍能无条件取得受管进程组并发出 SIGKILL。
    drop_process_group_id: StdMutex<Option<i32>>,
    terminal: Notify,
    last_access_ms: AtomicU64,
    /// owner/root cleanup 是 entry 的永久生命周期边界。有限的 manager close marker
    /// 仅用于拒绝同时发生的 reserve，不能承担迟到 watcher 的长期正确性。
    lifecycle_closed: AtomicBool,
    /// 输出游标只有在承载该 tool result 的 provider request 成功后才能推进。
    output_delivery: Mutex<OutputDeliveryState>,
    completion_registration: Mutex<CompletionRegistration>,
}

impl ManagedProcess {
    fn new(
        identity: (ProcessId, u64, Option<i32>, Option<String>),
        owner: ProcessOwner,
        command: String,
        code_type: String,
        cwd: String,
        tty: bool,
        output_buffer_bytes: (usize, usize),
    ) -> Self {
        let (id, instance_id, process_group_id, originating_tool_use_id) = identity;
        Self {
            id,
            instance_id,
            owner,
            originating_tool_use_id,
            command,
            code_type,
            cwd,
            tty,
            started_at: SystemTime::now(),
            finished_at: Mutex::new(None),
            state: Mutex::new(ProcessState::Starting),
            stdout: Mutex::new(BoundedOutput::new(output_buffer_bytes.0)),
            stderr: Mutex::new(BoundedOutput::new(output_buffer_bytes.1)),
            control: Mutex::new(ProcessControl {
                process_group_id,
                input: None,
            }),
            process_group_signal_gate: Arc::new(Mutex::new(())),
            drop_process_group_id: StdMutex::new(process_group_id),
            terminal: Notify::new(),
            last_access_ms: AtomicU64::new(now_ms()),
            lifecycle_closed: AtomicBool::new(false),
            output_delivery: Mutex::new(OutputDeliveryState {
                committed_stdout: OutputCursor(0),
                committed_stderr: OutputCursor(0),
                pending: None,
                inflight: false,
            }),
            completion_registration: Mutex::new(CompletionRegistration::Unrecorded),
        }
    }

    pub(crate) async fn attach_pipe(&self, process_group_id: Option<i32>) {
        let _signal_gate = self.process_group_signal_gate.lock().await;
        self.set_drop_process_group_id(process_group_id);
        let terminating = matches!(*self.state.lock().await, ProcessState::Terminating);
        {
            let mut control = self.control.lock().await;
            control.process_group_id = process_group_id;
        }
        if terminating {
            if let Some(process_group_id) = process_group_id {
                let _ = terminate_process_group(process_group_id, libc::SIGKILL);
            }
        }
    }

    pub(crate) async fn attach_pty(
        &self,
        sender: mpsc::Sender<PtyInput>,
        input_byte_budget: Arc<Semaphore>,
        max_input_bytes: usize,
        process_group_id: Option<i32>,
    ) {
        let _signal_gate = self.process_group_signal_gate.lock().await;
        self.set_drop_process_group_id(process_group_id);
        let terminating = matches!(*self.state.lock().await, ProcessState::Terminating);
        {
            let mut control = self.control.lock().await;
            control.process_group_id = process_group_id;
            control.input = Some(ProcessInput::Pty(PtyInputChannel {
                sender,
                byte_budget: input_byte_budget,
                max_bytes: max_input_bytes,
            }));
        }
        if terminating {
            if let Some(process_group_id) = process_group_id {
                let _ = terminate_process_group(process_group_id, libc::SIGKILL);
            }
        }
    }

    /// 句柄挂载后才允许对该 entry 做普通运行态管理。
    pub(crate) async fn mark_running(&self) {
        let mut state = self.state.lock().await;
        if matches!(*state, ProcessState::Starting) {
            *state = ProcessState::Running;
        }
    }

    pub(crate) async fn append_stdout(&self, bytes: &[u8]) {
        self.stdout.lock().await.append(bytes);
        self.touch();
    }

    pub(crate) async fn append_stderr(&self, bytes: &[u8]) {
        self.stderr.lock().await.append(bytes);
        self.touch();
    }

    /// watcher 因 drain grace 到期而停止 reader 时，明确标记本次输出非完整；未知的内核
    /// 缓冲尾部不能计入 omitted_bytes，但也绝不能被 cursor 悄悄跳过。
    pub(crate) async fn mark_output_incomplete(&self) {
        self.stdout.lock().await.mark_incomplete();
        self.stderr.lock().await.mark_incomplete();
        self.touch();
    }

    /// 所有 reader 已确认 EOF 后再 flush chunk 边界留下的半个 UTF-8 scalar。若 drain
    /// grace 超时，调用方必须改用 `mark_output_incomplete`，不能把未知尾部伪造为 EOF。
    pub(crate) async fn finish_output(&self) {
        self.stdout.lock().await.finish();
        self.stderr.lock().await.finish();
        self.touch();
    }

    #[cfg(test)]
    pub(crate) async fn output_snapshot(&self) -> (ProcessOutput, ProcessOutput) {
        let stdout = self.stdout.lock().await.snapshot();
        let stderr = self.stderr.lock().await.snapshot();
        self.touch();
        (stdout, stderr)
    }

    pub(crate) async fn output_since(
        &self,
        stdout_cursor: OutputCursor,
        stderr_cursor: OutputCursor,
    ) -> (ProcessOutput, ProcessOutput) {
        let stdout = self.stdout.lock().await.snapshot_since(stdout_cursor);
        let stderr = self.stderr.lock().await.snapshot_since(stderr_cursor);
        self.touch();
        (stdout, stderr)
    }

    pub(crate) async fn output_delivery_cursors(&self) -> (OutputCursor, OutputCursor) {
        let delivery = self.output_delivery.lock().await;
        delivery
            .pending
            .unwrap_or((delivery.committed_stdout, delivery.committed_stderr))
    }

    pub(crate) async fn has_uncommitted_output_delivery(&self) -> bool {
        self.output_delivery.lock().await.pending.is_some()
    }

    /// 每个进程同一时刻只允许一页等待 provider 确认。覆盖 pending cursor 会让同一
    /// tool batch 中较早、尚未交付的页面永久不可重读。
    pub(crate) async fn prepare_output_delivery(
        &self,
        stdout_cursor: OutputCursor,
        stderr_cursor: OutputCursor,
        final_result: bool,
    ) -> Option<ProcessDeliveryReceipt> {
        let mut delivery = self.output_delivery.lock().await;
        if delivery.pending.is_some() {
            return None;
        }
        delivery.pending = Some((stdout_cursor, stderr_cursor));
        self.touch();
        Some(ProcessDeliveryReceipt {
            process_id: self.id.as_str().to_string(),
            instance_id: self.instance_id,
            owner: self.owner.clone(),
            stdout_cursor,
            stderr_cursor,
            final_result,
        })
    }

    async fn begin_delivery(&self, receipt: &ProcessDeliveryReceipt) {
        let mut delivery = self.output_delivery.lock().await;
        if delivery.pending == Some((receipt.stdout_cursor, receipt.stderr_cursor)) {
            delivery.inflight = true;
        }
        drop(delivery);
    }

    async fn rollback_delivery(&self) {
        let mut delivery = self.output_delivery.lock().await;
        if delivery.inflight {
            delivery.pending = None;
            delivery.inflight = false;
        }
        drop(delivery);
    }

    /// 当前 turn 在下一次 provider request 前异常结束时，尚未送达的 receipt 不能占住
    /// delivery cursor；否则下一轮 poll 会把模型从未看到的输出静默跳过。
    async fn rollback_uncommitted_delivery(&self) {
        let mut delivery = self.output_delivery.lock().await;
        delivery.pending = None;
        delivery.inflight = false;
        drop(delivery);
    }

    /// 只撤销与指定回执完全对应的 pending cursor。用于 provider preflight 把某条
    /// tool_result 从实际请求中移除或替换时，不能连带清除同 owner 的其他待交付输出。
    async fn rollback_uncommitted_delivery_if_matches(&self, receipt: &ProcessDeliveryReceipt) {
        let mut delivery = self.output_delivery.lock().await;
        if delivery.pending == Some((receipt.stdout_cursor, receipt.stderr_cursor)) {
            delivery.pending = None;
            delivery.inflight = false;
        }
        drop(delivery);
    }

    async fn commit_delivery(&self, receipt: &ProcessDeliveryReceipt) -> bool {
        let mut delivery = self.output_delivery.lock().await;
        let committed = delivery.inflight
            && delivery.pending == Some((receipt.stdout_cursor, receipt.stderr_cursor));
        if committed {
            delivery.committed_stdout = receipt.stdout_cursor;
            delivery.committed_stderr = receipt.stderr_cursor;
            delivery.pending = None;
            delivery.inflight = false;
        }
        committed && receipt.final_result
    }

    /// 成功交付 final tool result 等价于该实例的终态已经交给模型；它必须压过任何迟到的
    /// watcher completion registration，避免已消费的 process 重新进入动态上下文。
    async fn mark_final_output_delivered(&self) {
        *self.completion_registration.lock().await = CompletionRegistration::FinalOutputDelivered;
    }

    pub(crate) async fn write(&self, bytes: Vec<u8>) -> Result<(), String> {
        // 入队必须在 control 外进行；即使未来调整 channel 策略，也不能让 runtime
        // terminate / owner cleanup 因等待 writer 而无法取得 PGID 发送 SIGKILL。
        let input = {
            let control = self.control.lock().await;
            match control.input.as_ref() {
                Some(ProcessInput::Pty(input)) => input.clone(),
                None => return Err("process stdin is closed".into()),
            }
        };
        if bytes.len() > input.max_bytes {
            return Err(format!(
                "PTY stdin write exceeds configured {} byte buffer limit",
                input.max_bytes
            ));
        }
        let byte_count = u32::try_from(bytes.len())
            .map_err(|_| "PTY stdin write is too large to account for".to_string())?;
        let byte_permit = input
            .byte_budget
            .clone()
            .try_acquire_many_owned(byte_count)
            .map_err(|_| {
                "PTY stdin buffer is full; poll or retry after output drains".to_string()
            })?;
        // 字节预算和消息数预算都是硬上限。不能 await sender::send：writer 卡住且
        // message queue 已满时，write_stdin 必须立即可重试，而非把当前 tool call 挂住。
        input
            .sender
            .try_send(PtyInput {
                bytes,
                _byte_permit: byte_permit,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    "PTY stdin buffer is full; poll or retry after output drains".to_string()
                }
                mpsc::error::TrySendError::Closed(_) => "PTY writer is closed".to_string(),
            })?;
        self.touch();
        Ok(())
    }

    pub(crate) async fn stdin_open(&self) -> bool {
        self.control.lock().await.input.is_some()
    }

    pub(crate) async fn close_stdin(&self) {
        self.control.lock().await.input = None;
    }

    pub(crate) async fn mark_finished(
        &self,
        exit_code: Option<i32>,
        signal: Option<i32>,
        success: bool,
    ) {
        {
            let mut state = self.state.lock().await;
            if !state.is_live() {
                return;
            }
            *state = if matches!(*state, ProcessState::Terminating) {
                ProcessState::Terminated { exit_code, signal }
            } else {
                ProcessState::Finished {
                    exit_code,
                    signal,
                    success,
                }
            };
        }
        *self.finished_at.lock().await = Some(SystemTime::now());
        self.close_stdin().await;
        self.touch();
        self.terminal.notify_waiters();
    }

    pub(crate) async fn mark_error(&self, message: &str) {
        // PTY 的 stdout/stderr 已在 master stream 合流，且其 stderr quota 为 0；错误必须
        // 写入同一 master 输出，否则模型只能看到 Error 状态而读不到诊断。
        if self.tty {
            self.append_stdout(message.as_bytes()).await;
        } else {
            self.append_stderr(message.as_bytes()).await;
        }
        {
            let mut state = self.state.lock().await;
            if !state.is_live() {
                return;
            }
            *state = ProcessState::Error;
        }
        *self.finished_at.lock().await = Some(SystemTime::now());
        self.close_stdin().await;
        self.touch();
        self.terminal.notify_waiters();
    }

    /// PTY/pipe 的 I/O worker 发生非 EOF 错误时，不能只让 worker 静默退出：根进程可能
    /// 仍然存活、而模型也已经无法再可靠地观察其输出。此时收束整个受管 PGID，并将 entry
    /// 变为可见的 Error 终态。
    pub(crate) async fn handle_io_failure(&self, message: &str) {
        if !self.state().await.is_live() {
            return;
        }
        match self.request_terminate(libc::SIGKILL).await {
            // reader/writer 与 child.wait 可以并发观察 terminal close。若 PGID 已经消失，
            // 交给 watcher 用真实 ExitStatus 定案，不能把正常退出的 race 覆盖成 Error。
            Ok(TerminateRequestResult::AlreadyExited) => return,
            Ok(TerminateRequestResult::Requested | TerminateRequestResult::AlreadyTerminating) => {}
            Err(error) => {
                self.append_stderr(
                    format!("process I/O failure cleanup signal failed: {error}\n").as_bytes(),
                )
                .await;
            }
        }
        self.mark_output_incomplete().await;
        self.mark_error(&format!("process I/O failed: {message}\n"))
            .await;
    }

    pub(crate) async fn state(&self) -> ProcessState {
        *self.state.lock().await
    }

    pub(crate) async fn finished_at(&self) -> Option<SystemTime> {
        *self.finished_at.lock().await
    }

    /// Ctrl-C 是软中断请求：目标可以捕获/忽略 SIGINT，entry 必须继续保持 running，
    /// 使后续 runtime/TUI 硬 terminate 仍能接管同一个进程组。
    pub(crate) async fn request_interrupt(&self, signal: i32) -> Result<(), String> {
        let _signal_gate = self.process_group_signal_gate.lock().await;
        let state = self.state.lock().await;
        if !state.is_live() {
            return Ok(());
        }
        let process_group_id = {
            let control = self.control.lock().await;
            control.process_group_id
        };
        if let Some(process_group_id) = process_group_id {
            terminate_process_group(process_group_id, signal)
                .map_err(|err| format!("interrupt process group failed: {err}"))?;
        }
        self.touch();
        Ok(())
    }

    /// runtime/TUI 的 hard terminate。状态转换与 PGID 信号在同一 state 临界区内完成，
    /// watcher 无法在两者之间把自然退出误当作仍可终止的 live entry。
    pub(crate) async fn request_terminate(
        &self,
        signal: i32,
    ) -> Result<TerminateRequestResult, String> {
        let _signal_gate = self.process_group_signal_gate.lock().await;
        let mut state = self.state.lock().await;
        let process_group_id = {
            let control = self.control.lock().await;
            control.process_group_id
        };
        match *state {
            ProcessState::Terminating => return Ok(TerminateRequestResult::AlreadyTerminating),
            ProcessState::Finished { .. }
            | ProcessState::Terminated { .. }
            | ProcessState::Error => {
                return Ok(TerminateRequestResult::AlreadyExited);
            }
            ProcessState::Starting | ProcessState::Running => {}
        }
        if let Some(process_group_id) = process_group_id {
            match signal_process_group(process_group_id, signal)
                .map_err(|err| format!("terminate process group failed: {err}"))?
            {
                ProcessGroupSignalResult::Delivered => {}
                ProcessGroupSignalResult::AlreadyExited => {
                    return Ok(TerminateRequestResult::AlreadyExited);
                }
            }
        }
        *state = ProcessState::Terminating;
        drop(state);
        self.touch();
        Ok(TerminateRequestResult::Requested)
    }

    /// root terminal 已自然退出后的后代清理不代表用户/runtime 主动 terminate；不能把
    /// 正常完成的 root 错标为 `terminated`。
    pub(crate) async fn kill_remaining_process_group(&self) -> Result<(), String> {
        let _signal_gate = self.process_group_signal_gate.lock().await;
        let process_group_id = {
            let control = self.control.lock().await;
            control.process_group_id
        };
        if let Some(process_group_id) = process_group_id {
            terminate_process_group(process_group_id, libc::SIGKILL)
                .map_err(|err| format!("terminate residual process group failed: {err}"))?;
        }
        Ok(())
    }

    /// root 已成为 zombie 后，watcher 必须先独占所有 PGID signal 路径，再真正 reap。
    /// guard 存活期间不得向该组发信号；reap 完毕后调用方会立刻 retire PGID。
    pub(crate) async fn acquire_root_reap_gate(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.process_group_signal_gate)
            .lock_owned()
            .await
    }

    /// 调用方持有 `acquire_root_reap_gate` 返回的 guard，且 root 已实际 reap 后调用。
    /// 从这一刻起旧 numeric PGID 可以被内核复用，所有 control/drop 路径都必须遗忘它。
    pub(crate) async fn retire_process_group_after_root_reap(&self) {
        self.set_drop_process_group_id(None);
        let mut control = self.control.lock().await;
        control.process_group_id = None;
        control.input = None;
    }

    pub(crate) async fn wait_for_terminal(&self, wait: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // `notify_waiters` 不保存 permit；先把 Notified future 注册进队列，再检查 state，
            // 才不会漏掉检查与 await 之间恰好发生的终态通知。
            let notified = self.terminal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.state().await.is_live() {
                return true;
            }
            if tokio::time::timeout_at(deadline, &mut notified)
                .await
                .is_err()
            {
                return !self.state().await.is_live();
            }
        }
    }

    pub(crate) fn last_access_ms(&self) -> u64 {
        self.last_access_ms.load(Ordering::Relaxed)
    }

    fn touch(&self) {
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
    }

    fn close_lifecycle(&self) {
        self.lifecycle_closed.store(true, Ordering::Release);
    }

    fn is_lifecycle_closed(&self) -> bool {
        self.lifecycle_closed.load(Ordering::Acquire)
    }

    fn set_drop_process_group_id(&self, process_group_id: Option<i32>) {
        let mut saved = match self.drop_process_group_id.lock() {
            Ok(saved) => saved,
            Err(poisoned) => poisoned.into_inner(),
        };
        *saved = process_group_id;
    }

    /// ProcessManager 在最后一个 Arc 被 drop 时没有 async runtime 可等待；此处通过
    /// 独立同步 PGID 槽保证仍可发出 SIGKILL。正常 owner/runtime shutdown 仍走完整 async 路径。
    fn best_effort_kill_on_manager_drop(&self) {
        let saved = match self.drop_process_group_id.lock() {
            Ok(saved) => saved,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(process_group_id) = *saved {
            let _ = terminate_process_group(process_group_id, libc::SIGKILL);
        }
    }

    #[cfg(test)]
    async fn process_group_ids_for_test(&self) -> (Option<i32>, Option<i32>) {
        let control_process_group_id = self.control.lock().await.process_group_id;
        let drop_process_group_id = match self.drop_process_group_id.lock() {
            Ok(saved) => *saved,
            Err(poisoned) => *poisoned.into_inner(),
        };
        (control_process_group_id, drop_process_group_id)
    }
}

/// root session 共享的受管进程表。模型访问按 `ProcessOwner` 过滤；运行时/TUI 可按 root id 聚合。
#[derive(Debug)]
pub(crate) struct ProcessManager {
    entries: Mutex<BTreeMap<ProcessId, Arc<ManagedProcess>>>,
    /// 已从容量表撤下、但还未向 live PGID 发出终止请求的 entry。它们不再对模型可见，
    /// 但在 cleanup request 真正线性化前仍属于 runtime 的资源所有权，shutdown 必须 drain。
    pending_eviction_cleanup: Mutex<BTreeMap<ProcessId, Arc<ManagedProcess>>>,
    completion_notifications: Mutex<BTreeMap<String, VecDeque<ProcessCompletion>>>,
    background_events: Mutex<BTreeMap<String, VecDeque<BackgroundProcessEvent>>>,
    provider_completion_notifications:
        Mutex<BTreeMap<ProcessOwner, VecDeque<ProviderCompletionNotification>>>,
    lifecycle: Mutex<ProcessManagerLifecycle>,
    output_buffer_bytes: usize,
    id_attempts: usize,
    next_instance_id: AtomicU64,
    #[cfg(test)]
    handoff_gate: Mutex<Option<Arc<ProcessHandoffTestGate>>>,
    #[cfg(test)]
    reservation_gate: Mutex<Option<Arc<ProcessHandoffTestGate>>>,
    #[cfg(test)]
    eviction_cleanup_gate: Mutex<Option<Arc<ProcessHandoffTestGate>>>,
    max_entries_per_owner: usize,
    protected_recent_entries: usize,
    /// TUI/journal fanout 只是一份可丢弃的观察投影；容量按单 owner entry 上限推导，
    /// 因此即使 root session 长时间没有前台 consumer 也不会无界积累。
    root_background_event_capacity: usize,
    /// completion 的可靠模型投递由 `provider_completion_notifications` 单独保证；
    /// 此队列只是 root 控制面的有界 fanout。
    root_completion_notification_capacity: usize,
    #[cfg(test)]
    test_id_candidates: std::sync::Mutex<VecDeque<ProcessId>>,
}

impl Drop for ProcessManager {
    fn drop(&mut self) {
        let mut handled = BTreeSet::new();
        if let Ok(entries) = self.entries.try_lock() {
            for process in entries.values() {
                if handled.insert((process.id.clone(), process.instance_id)) {
                    process.best_effort_kill_on_manager_drop();
                }
            }
        }
        if let Ok(pending) = self.pending_eviction_cleanup.try_lock() {
            for process in pending.values() {
                if handled.insert((process.id.clone(), process.instance_id)) {
                    process.best_effort_kill_on_manager_drop();
                }
            }
        }
    }
}

impl ProcessManager {
    pub(crate) fn new(
        output_buffer_bytes: usize,
        id_attempts: usize,
        max_entries_per_owner: usize,
        protected_recent_entries: usize,
    ) -> Self {
        let max_entries_per_owner = max_entries_per_owner.max(1);
        Self {
            entries: Mutex::new(BTreeMap::new()),
            pending_eviction_cleanup: Mutex::new(BTreeMap::new()),
            completion_notifications: Mutex::new(BTreeMap::new()),
            background_events: Mutex::new(BTreeMap::new()),
            provider_completion_notifications: Mutex::new(BTreeMap::new()),
            lifecycle: Mutex::new(ProcessManagerLifecycle::default()),
            output_buffer_bytes: output_buffer_bytes.max(1),
            id_attempts: id_attempts.max(1),
            next_instance_id: AtomicU64::new(1),
            #[cfg(test)]
            handoff_gate: Mutex::new(None),
            #[cfg(test)]
            reservation_gate: Mutex::new(None),
            #[cfg(test)]
            eviction_cleanup_gate: Mutex::new(None),
            // 一个 entry 在一次未 drain 的生命周期内最多产生 Started、Output 和两个
            // StateChanged fanout；这为单 owner 的完整短命令批次保留空间，同时仍是
            // 与 subagent 数量无关的 root-session 硬上限。
            root_background_event_capacity: max_entries_per_owner.saturating_mul(4).max(1),
            root_completion_notification_capacity: max_entries_per_owner,
            max_entries_per_owner,
            protected_recent_entries,
            #[cfg(test)]
            test_id_candidates: std::sync::Mutex::new(VecDeque::new()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn pause_next_handoff_for_test(&self) -> Arc<ProcessHandoffTestGate> {
        let gate = Arc::new(ProcessHandoffTestGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *self.handoff_gate.lock().await = Some(Arc::clone(&gate));
        gate
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_test_handoff_gate(&self) {
        let gate = self.handoff_gate.lock().await.take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn pause_next_reservation_for_test(&self) -> Arc<ProcessHandoffTestGate> {
        let gate = Arc::new(ProcessHandoffTestGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *self.reservation_gate.lock().await = Some(Arc::clone(&gate));
        gate
    }

    #[cfg(test)]
    async fn wait_for_test_reservation_gate(&self) {
        let gate = self.reservation_gate.lock().await.take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }

    #[cfg(test)]
    async fn pause_next_eviction_cleanup_for_test(&self) -> Arc<ProcessHandoffTestGate> {
        let gate = Arc::new(ProcessHandoffTestGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *self.eviction_cleanup_gate.lock().await = Some(Arc::clone(&gate));
        gate
    }

    #[cfg(test)]
    async fn wait_for_test_eviction_cleanup_gate(&self) {
        let gate = self.eviction_cleanup_gate.lock().await.take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }

    pub(crate) async fn record_started(&self, process: &ManagedProcess) {
        if process.is_lifecycle_closed() {
            return;
        }
        self.record_background_event(BackgroundProcessEvent::Started {
            process_id: process.id.as_str().to_string(),
            owner: process.owner.clone(),
        })
        .await;
    }

    pub(crate) async fn record_output(&self, process: &ManagedProcess) {
        if process.is_lifecycle_closed() {
            return;
        }
        self.record_background_event(BackgroundProcessEvent::Output {
            process_id: process.id.as_str().to_string(),
            owner: process.owner.clone(),
        })
        .await;
    }

    pub(crate) async fn record_state_changed(&self, process: &ManagedProcess) {
        if process.is_lifecycle_closed() {
            return;
        }
        self.record_background_event(BackgroundProcessEvent::StateChanged {
            process_id: process.id.as_str().to_string(),
            owner: process.owner.clone(),
            status: process.state().await.label().to_string(),
        })
        .await;
    }

    async fn record_background_event(&self, event: BackgroundProcessEvent) {
        let (root_session_id, process_id, owner, is_output) = match &event {
            BackgroundProcessEvent::Started { process_id, owner }
            | BackgroundProcessEvent::Output { process_id, owner }
            | BackgroundProcessEvent::StateChanged {
                process_id, owner, ..
            } => (
                owner.root_session_id.clone(),
                process_id.clone(),
                owner.clone(),
                matches!(&event, BackgroundProcessEvent::Output { .. }),
            ),
        };
        let lifecycle = self.lifecycle.lock().await;
        if lifecycle.closed_root_sessions.contains(&root_session_id)
            || lifecycle.closed_owners.contains(&owner)
        {
            return;
        }
        let mut events = self.background_events.lock().await;
        let queue = events.entry(root_session_id).or_default();
        if is_output {
            // output 只是一条“可刷新”信号；同一 entry 在 TUI 下一次 tick 前的多次 drain
            // 合并为一个事件，避免无界 journal/event 压力。
            queue.retain(|existing| {
                !matches!(
                    existing,
                    BackgroundProcessEvent::Output {
                        process_id: existing_id,
                        owner: existing_owner,
                    } if existing_id == &process_id && existing_owner == &owner
                )
            });
        }
        // 这份队列只驱动 TUI/journal 的即时 fanout。它不是模型完成通知的可靠存储：
        // D21 要求的至少一次投递由下方独立的 per-owner provider 队列承担。root session
        // 可能有任意多 subagent，不能把每个 owner 的 64-entry 上限误当成 root 队列上限。
        while queue.len() >= self.root_background_event_capacity {
            queue.pop_front();
        }
        queue.push_back(event);
    }

    pub(crate) async fn take_background_events_for_root(
        &self,
        root_session_id: &str,
    ) -> Vec<BackgroundProcessEvent> {
        self.background_events
            .lock()
            .await
            .remove(root_session_id)
            .map(VecDeque::into_iter)
            .map(Iterator::collect)
            .unwrap_or_default()
    }

    /// 原子预留逻辑 ID；达到 owner 容量时先淘汰 terminal，再收束最旧 live entry。
    #[cfg(test)]
    pub(crate) async fn reserve(
        self: &Arc<Self>,
        owner: ProcessOwner,
        command: String,
        code_type: String,
        cwd: String,
        tty: bool,
    ) -> Result<Arc<ManagedProcess>, String> {
        self.reserve_with_process_group(owner, command, code_type, cwd, tty, None, None)
            .await
    }

    /// 登记与 PGID 绑定在同一个 manager 线性化点；handoff task 尚未开始时 shutdown
    /// 也能完整清理已 spawn 的受管进程组。
    #[allow(
        clippy::too_many_arguments,
        reason = "登记线性化点必须原子接收完整 session 元数据、PGID 与 tool_use 归属"
    )]
    pub(crate) async fn reserve_with_process_group(
        self: &Arc<Self>,
        owner: ProcessOwner,
        command: String,
        code_type: String,
        cwd: String,
        tty: bool,
        process_group_id: Option<i32>,
        originating_tool_use_id: Option<String>,
    ) -> Result<Arc<ManagedProcess>, String> {
        #[cfg(test)]
        self.wait_for_test_reservation_gate().await;
        let evicted = {
            let lifecycle = self.lifecycle.lock().await;
            if lifecycle
                .closed_root_sessions
                .contains(&owner.root_session_id)
                || lifecycle.closed_owners.contains(&owner)
            {
                return Err("background process owner is shutting down".into());
            }
            let mut entries = self.entries.lock().await;
            let mut pending_eviction_cleanup = self.pending_eviction_cleanup.lock().await;
            // D21 要求每个 completed process 的最小事实至少进入一次成功的 provider
            // request，不能为了固定队列长度丢弃最老通知。正常上限是 64；若一个 owner
            // 已满且恰好发生一次 live eviction，pending entry 会使最终队列短暂达到 65。
            // 因此在新的 reservation 前施加背压，并且同一 owner 同时只允许一个 eviction
            // cleanup 在飞，保证未确认 notification 的内存上界为 max_entries + 1。
            let pending_notification_count = self
                .provider_completion_notifications
                .lock()
                .await
                .get(&owner)
                .map(VecDeque::len)
                .unwrap_or_default();
            if pending_notification_count >= self.max_entries_per_owner {
                return Err(format!(
                    "background process completion notifications are awaiting provider delivery for owner; limit is {}",
                    self.max_entries_per_owner
                ));
            }
            if pending_eviction_cleanup
                .values()
                .any(|entry| entry.owner == owner)
            {
                return Err(
                    "background process eviction cleanup is still pending for owner; retry the code_run request"
                        .into(),
                );
            }
            let mut owned = entries
                .values()
                .filter(|entry| entry.owner == owner)
                .cloned()
                .collect::<Vec<_>>();
            owned.sort_by_key(|entry| std::cmp::Reverse(entry.last_access_ms()));
            let protected = owned
                .iter()
                .take(self.protected_recent_entries)
                .map(|entry| entry.id.clone())
                .collect::<Vec<_>>();
            let mut removable = owned
                .into_iter()
                .filter(|entry| !protected.contains(&entry.id))
                .collect::<Vec<_>>();
            removable.sort_by_key(|entry| entry.last_access_ms());

            let owner_count = entries
                .values()
                .filter(|entry| entry.owner == owner)
                .count();
            let evicted = if owner_count >= self.max_entries_per_owner {
                // 不能把 `try_lock` 的竞争误判为 terminal：那样容量压力下可能错误杀掉
                // 一个 live entry。只要没有确认的 terminal 且任一状态锁正忙，就保守拒绝
                // 本次 reservation；调用方可以重试，现有进程绝不会因此被错杀。
                let mut terminal = None;
                let mut live_candidate = None;
                let mut state_busy = false;
                for entry in &removable {
                    match entry.state.try_lock() {
                        Ok(state) if !state.is_live() => {
                            terminal = Some(Arc::clone(entry));
                            break;
                        }
                        Ok(state) if !matches!(*state, ProcessState::Starting) => {
                            if live_candidate.is_none() {
                                live_candidate = Some(Arc::clone(entry));
                            }
                        }
                        Ok(_) => {}
                        Err(_) => state_busy = true,
                    }
                }
                let candidate =
                    terminal.or_else(|| (!state_busy).then_some(live_candidate).flatten());
                if candidate.is_none() {
                    if state_busy {
                        return Err(
                            "background process capacity state is busy; retry the code_run request"
                                .into(),
                        );
                    }
                    return Err(format!(
                        "background process capacity reached for owner; the most recent {} entries are protected",
                        self.protected_recent_entries
                    ));
                }
                candidate
            } else {
                None
            };

            let mut new_process = None;
            for _ in 0..self.id_attempts {
                let id = self.next_process_id();
                if entries.contains_key(&id) || pending_eviction_cleanup.contains_key(&id) {
                    continue;
                }
                let process = Arc::new(ManagedProcess::new(
                    (
                        id.clone(),
                        self.next_instance_id.fetch_add(1, Ordering::Relaxed),
                        process_group_id,
                        originating_tool_use_id.clone(),
                    ),
                    owner.clone(),
                    command.clone(),
                    code_type.clone(),
                    cwd.clone(),
                    tty,
                    if tty {
                        // PTY 已将 stdout/stderr 多路复用到 master stream；stderr 不再额外占用
                        // 一个字节，保证单进程总 buffer 恰为配置上限。
                        (self.output_buffer_bytes, 0)
                    } else {
                        (
                            self.output_buffer_bytes / 2,
                            self.output_buffer_bytes
                                .saturating_sub(self.output_buffer_bytes / 2),
                        )
                    },
                ));
                new_process = Some(process);
                break;
            }
            let process = new_process.ok_or_else(|| {
                format!(
                    "unable to allocate unique process_id after {} attempts",
                    self.id_attempts
                )
            })?;
            if let Some(evicted) = &evicted {
                entries.remove(&evicted.id);
                pending_eviction_cleanup.insert(evicted.id.clone(), Arc::clone(evicted));
            }
            entries.insert(process.id.clone(), Arc::clone(&process));
            drop(pending_eviction_cleanup);
            drop(lifecycle);
            (evicted, process)
        };

        if let Some(evicted) = evicted.0 {
            // 新 reservation 已经插入；从此处到 caller 接住 `Arc<ManagedProcess>` 之间不能
            // 再 await，否则 hard abort 会触发 spawn guard 却无法撤下新的 Starting entry。
            // 清理被淘汰 entry 的所有权由 manager 自己持有的 detached task 接管。
            let manager = Arc::clone(self);
            tokio::spawn(async move {
                manager.cleanup_evicted_entry(evicted).await;
            });
        }
        Ok(evicted.1)
    }

    /// 容量淘汰后的收束在 reservation 返回后异步执行，避免 caller abort 留下新 entry。
    async fn cleanup_evicted_entry(self: Arc<Self>, evicted: Arc<ManagedProcess>) {
        #[cfg(test)]
        self.wait_for_test_eviction_cleanup_gate().await;
        if evicted.state().await.is_live() {
            // entry 已从容量表撤下，watcher 仍持有 Arc 并会在终态时生成
            // `final_output_available=false` notification；不能等待它。pending 标记必须保留
            // 到 watcher 已实际登记该 notification，避免下一次 reserve 在这一小段窗口内
            // 再次淘汰并突破 owner 的未投递 completion 上界。
            if matches!(
                evicted.request_terminate(libc::SIGKILL).await,
                Ok(TerminateRequestResult::Requested)
            ) {
                self.record_state_changed(&evicted).await;
            }
        } else {
            // terminal entry 可能刚在 watcher 的 mark_finished 与 record_completion 之间被
            // 淘汰；立即补记最小事件。record_completion 对同一 id 去重。
            self.record_completion(&evicted).await;
        }
    }

    /// detached eviction task 与 session shutdown 可以并发执行。仅移除相同 allocation，
    /// 不能让一个稍后重用相同 logical ID 的 entry 被旧 task 从 pending store 中撤下。
    async fn remove_pending_eviction_cleanup(&self, expected: &ManagedProcess) {
        let mut pending = self.pending_eviction_cleanup.lock().await;
        if pending.get(&expected.id).is_some_and(|current| {
            current.instance_id == expected.instance_id && current.owner == expected.owner
        }) {
            pending.remove(&expected.id);
        }
    }

    fn next_process_id(&self) -> ProcessId {
        #[cfg(test)]
        if let Ok(mut candidates) = self.test_id_candidates.lock() {
            if let Some(candidate) = candidates.pop_front() {
                return candidate;
            }
        }
        ProcessId::random()
    }

    #[cfg(test)]
    fn set_test_id_candidates(&self, candidates: impl IntoIterator<Item = ProcessId>) {
        if let Ok(mut pending) = self.test_id_candidates.lock() {
            pending.extend(candidates);
        }
    }

    pub(crate) async fn find_for_owner(
        &self,
        owner: &ProcessOwner,
        id: &str,
    ) -> Option<Arc<ManagedProcess>> {
        let entry = self
            .entries
            .lock()
            .await
            .get(&ProcessId(id.to_string()))
            .cloned()?;
        (entry.owner == *owner).then_some(entry)
    }

    /// parent agent 的受控观察/中断/终止入口：只在同一 agent、同一 root session 内
    /// 定位 live entry。
    /// 子 agent 仍须走 `find_for_owner`，不能借此访问 parent 或 sibling。
    pub(crate) async fn find_live_for_root(
        &self,
        root_owner: &ProcessOwner,
        id: &str,
    ) -> Option<Arc<ManagedProcess>> {
        let entry = self
            .entries
            .lock()
            .await
            .get(&ProcessId(id.to_string()))
            .cloned()?;
        if entry.owner.owner_agent_id != root_owner.owner_agent_id
            || entry.owner.root_session_id != root_owner.root_session_id
            || !entry.state().await.is_live()
        {
            return None;
        }
        Some(entry)
    }

    pub(crate) async fn live_for_owner(&self, owner: &ProcessOwner) -> Vec<Arc<ManagedProcess>> {
        let entries = self.entries.lock().await;
        let owner_entries = entries
            .values()
            .filter(|entry| &entry.owner == owner)
            .cloned()
            .collect::<Vec<_>>();
        drop(entries);
        let mut result = Vec::new();
        for entry in owner_entries {
            if entry.state().await.is_live() {
                result.push(entry);
            }
        }
        result.sort_by(|left, right| left.id.cmp(&right.id));
        result
    }

    /// 返回由同一 tool_use 登记且仍处于 live 状态的受管 session。用于 explicit
    /// cancel 强制 drop tool future 后补齐 Interrupted 提示；绝不按 owner 的所有 live
    /// entry 泛化，避免把早先已经后台化的进程误报为本次调用的 continuation。
    pub(crate) async fn live_ids_for_owner_and_tool_use(
        &self,
        owner: &ProcessOwner,
        tool_use_id: &str,
    ) -> Vec<String> {
        let entries = self.entries.lock().await;
        let selected = entries
            .values()
            .filter(|entry| {
                &entry.owner == owner
                    && entry.originating_tool_use_id.as_deref() == Some(tool_use_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        drop(entries);
        let mut ids = Vec::new();
        for entry in selected {
            if entry.state().await.is_live() {
                ids.push(entry.id.as_str().to_string());
            }
        }
        ids.sort();
        ids
    }

    pub(crate) async fn live_for_root(&self, root_session_id: &str) -> Vec<Arc<ManagedProcess>> {
        let entries = self.entries.lock().await;
        let root_entries = entries
            .values()
            .filter(|entry| entry.owner.root_session_id == root_session_id)
            .cloned()
            .collect::<Vec<_>>();
        drop(entries);
        let mut result = Vec::new();
        for entry in root_entries {
            if entry.state().await.is_live() {
                result.push(entry);
            }
        }
        result.sort_by(|left, right| left.id.cmp(&right.id));
        result
    }

    /// 以 TUI confirmation snapshot 的完整 identity 线性化终止请求。logical process_id
    /// 可在旧 entry 被移除后复用；必须同时匹配 owner 与 allocation instance，不能先按 ID
    /// 查询再在锁外终止一个后来重用的 entry。
    pub(crate) async fn terminate_live_for_root_matching(
        &self,
        root_session_id: &str,
        process_id: &str,
        owner: &ProcessOwner,
        instance_id: u64,
        signal: i32,
    ) -> Result<TerminateRequestResult, String> {
        let entry = {
            let entries = self.entries.lock().await;
            let Some(entry) = entries.get(&ProcessId(process_id.to_string())) else {
                return Err("process has already exited".into());
            };
            if entry.owner.root_session_id != root_session_id
                || entry.owner != *owner
                || entry.instance_id != instance_id
            {
                return Err("process has already exited".into());
            }
            Arc::clone(entry)
        };
        let result = entry.request_terminate(signal).await?;
        if matches!(result, TerminateRequestResult::Requested) {
            self.record_state_changed(&entry).await;
        }
        Ok(result)
    }

    pub(crate) async fn retained_for_owner(
        &self,
        owner: &ProcessOwner,
    ) -> Vec<Arc<ManagedProcess>> {
        let entries = self.entries.lock().await;
        let mut owner_entries = entries
            .values()
            .filter(|entry| &entry.owner == owner)
            .cloned()
            .collect::<Vec<_>>();
        drop(entries);
        owner_entries.sort_by(|left, right| left.id.cmp(&right.id));
        owner_entries
    }

    pub(crate) async fn begin_deliveries(&self, receipts: &[ProcessDeliveryReceipt]) {
        let entries = self.entries.lock().await;
        let selected = receipts
            .iter()
            .filter_map(|receipt| {
                entries
                    .get(&ProcessId(receipt.process_id.clone()))
                    .filter(|entry| {
                        entry.instance_id == receipt.instance_id && entry.owner == receipt.owner
                    })
                    .map(|entry| (Arc::clone(entry), receipt.clone()))
            })
            .collect::<Vec<_>>();
        drop(entries);
        for (entry, receipt) in selected {
            entry.begin_delivery(&receipt).await;
        }
    }

    /// 新 request 开始前只回滚上一轮未成功响应的 in-flight receipt；尚未进入 provider
    /// request 的新 tool result 保持 pending，不能被提前丢弃。
    pub(crate) async fn rollback_inflight_deliveries_for_owner(&self, owner: &ProcessOwner) {
        let entries = self.retained_for_owner(owner).await;
        for entry in entries {
            entry.rollback_delivery().await;
        }
    }

    /// 只在一个 turn 已失败/取消后调用；包括尚未发起 provider request 的 pending receipt。
    pub(crate) async fn rollback_uncommitted_deliveries_for_owner(&self, owner: &ProcessOwner) {
        let entries = self.retained_for_owner(owner).await;
        for entry in entries {
            entry.rollback_uncommitted_delivery().await;
        }
    }

    /// 撤销没有进入本次 provider request 的精确回执，不影响同 owner 的其他进程。
    pub(crate) async fn rollback_uncommitted_deliveries(
        &self,
        receipts: &[ProcessDeliveryReceipt],
    ) {
        let entries = self.entries.lock().await;
        let selected = receipts
            .iter()
            .filter_map(|receipt| {
                entries
                    .get(&ProcessId(receipt.process_id.clone()))
                    .filter(|entry| {
                        entry.instance_id == receipt.instance_id && entry.owner == receipt.owner
                    })
                    .map(|entry| (Arc::clone(entry), receipt.clone()))
            })
            .collect::<Vec<_>>();
        drop(entries);
        for (entry, receipt) in selected {
            entry
                .rollback_uncommitted_delivery_if_matches(&receipt)
                .await;
        }
    }

    pub(crate) async fn commit_deliveries(&self, receipts: &[ProcessDeliveryReceipt]) {
        let entries = self.entries.lock().await;
        let selected = receipts
            .iter()
            .filter_map(|receipt| {
                entries
                    .get(&ProcessId(receipt.process_id.clone()))
                    .filter(|entry| {
                        entry.instance_id == receipt.instance_id && entry.owner == receipt.owner
                    })
                    .map(|entry| (Arc::clone(entry), receipt.clone()))
            })
            .collect::<Vec<_>>();
        drop(entries);

        let mut terminal_to_remove = Vec::new();
        for (entry, receipt) in selected {
            if entry.commit_delivery(&receipt).await && !entry.state().await.is_live() {
                entry.mark_final_output_delivered().await;
                terminal_to_remove.push((entry.id.clone(), entry));
            }
        }
        // `mark_final_output_delivered` 与 `record_completion` 共用 entry mutex：若 watcher
        // 恰好先登记了 completion，本处在 provider 成功后也必须撤掉那条尚未消费的最小
        // 通知；若 watcher 尚未到达，它之后会直接观察到 FinalOutputDelivered 并跳过。
        for (_, entry) in &terminal_to_remove {
            self.remove_completion_notification_after_final_output(entry)
                .await;
        }
        let mut entries = self.entries.lock().await;
        for (id, expected) in terminal_to_remove {
            if entries
                .get(&id)
                .is_some_and(|current| Arc::ptr_eq(current, &expected))
            {
                entries.remove(&id);
            }
        }
    }

    /// spawn/attach/watcher 交接期间若 tool future 被强制 drop，撤下该 reservation 并终止
    /// 已经挂上的进程组。该路径不能留下永远处于 Starting 的 entry。
    pub(crate) async fn abort_reservation(&self, expected: Arc<ManagedProcess>) {
        let removed = {
            let mut entries = self.entries.lock().await;
            if entries
                .get(&expected.id)
                .is_some_and(|current| Arc::ptr_eq(current, &expected))
            {
                entries.remove(&expected.id);
                true
            } else {
                false
            }
        };
        if removed && expected.state().await.is_live() {
            let _ = expected.request_terminate(libc::SIGKILL).await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn pending_completion_notifications_for_owner(
        &self,
        owner: &ProcessOwner,
    ) -> Vec<ProcessCompletion> {
        self.provider_completion_notifications
            .lock()
            .await
            .get(owner)
            .map(|notifications| {
                notifications
                    .iter()
                    .map(|notification| notification.completion.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) async fn begin_completion_notification_delivery(
        &self,
        owner: &ProcessOwner,
    ) -> Vec<ProcessCompletionDeliveryReceipt> {
        self.begin_completion_notification_delivery_snapshot(owner)
            .await
            .0
    }

    /// 原子地冻结本次 provider request 要投递的 completion notification。投递 ID 与
    /// snapshot 必须来自同一个临界区；否则 watcher 恰好完成时会出现“成功响应提交了未放进
    /// projection 的 notification”的丢失窗口。
    pub(crate) async fn begin_completion_notification_delivery_snapshot(
        &self,
        owner: &ProcessOwner,
    ) -> (
        Vec<ProcessCompletionDeliveryReceipt>,
        Vec<ProcessCompletion>,
    ) {
        let mut notifications = self.provider_completion_notifications.lock().await;
        let Some(owner_notifications) = notifications.get_mut(owner) else {
            return (Vec::new(), Vec::new());
        };
        let mut receipts = Vec::new();
        let mut snapshot = Vec::new();
        for notification in owner_notifications.iter_mut() {
            if !notification.inflight {
                receipts.push(notification.receipt.clone());
                snapshot.push(notification.completion.clone());
                notification.inflight = true;
            }
        }
        (receipts, snapshot)
    }

    pub(crate) async fn rollback_completion_notification_delivery(&self, owner: &ProcessOwner) {
        if let Some(notifications) = self
            .provider_completion_notifications
            .lock()
            .await
            .get_mut(owner)
        {
            for notification in notifications {
                notification.inflight = false;
            }
        }
    }

    pub(crate) async fn commit_completion_notification_delivery(
        &self,
        owner: &ProcessOwner,
        receipts: &[ProcessCompletionDeliveryReceipt],
    ) {
        let receipts = receipts.iter().collect::<BTreeSet<_>>();
        let mut notifications = self.provider_completion_notifications.lock().await;
        let should_remove_owner = if let Some(owner_notifications) = notifications.get_mut(owner) {
            owner_notifications.retain(|notification| {
                !(notification.inflight && receipts.contains(&notification.receipt))
            });
            owner_notifications.is_empty()
        } else {
            false
        };
        if should_remove_owner {
            notifications.remove(owner);
        }
    }

    pub(crate) async fn cleanup_owner(&self, owner: &ProcessOwner) {
        let owner_entries = {
            let mut lifecycle = self.lifecycle.lock().await;
            lifecycle.mark_owner_closed(owner.clone());
            self.provider_completion_notifications
                .lock()
                .await
                .remove(owner);
            for events in self.background_events.lock().await.values_mut() {
                events.retain(|event| match event {
                    BackgroundProcessEvent::Started {
                        owner: event_owner, ..
                    }
                    | BackgroundProcessEvent::Output {
                        owner: event_owner, ..
                    }
                    | BackgroundProcessEvent::StateChanged {
                        owner: event_owner, ..
                    } => event_owner != owner,
                });
            }
            let mut entries = self.entries.lock().await;
            let owner_entries = entries
                .values()
                .filter(|entry| &entry.owner == owner)
                .cloned()
                .collect::<Vec<_>>();
            for entry in &owner_entries {
                entry.close_lifecycle();
                entries.remove(&entry.id);
            }
            let mut pending = self.pending_eviction_cleanup.lock().await;
            let pending_entries = pending
                .values()
                .filter(|entry| &entry.owner == owner)
                .cloned()
                .collect::<Vec<_>>();
            for entry in &pending_entries {
                entry.close_lifecycle();
                pending.remove(&entry.id);
            }
            drop(pending);
            drop(entries);
            drop(lifecycle);
            owner_entries
                .into_iter()
                .chain(pending_entries)
                .collect::<Vec<_>>()
        };
        for entry in owner_entries {
            if entry.state().await.is_live() {
                let _ = entry.request_terminate(libc::SIGKILL).await;
            }
        }
    }

    pub(crate) async fn cleanup_root_session(&self, root_session_id: &str) {
        let root_entries = {
            let mut lifecycle = self.lifecycle.lock().await;
            lifecycle.mark_root_session_closed(root_session_id.to_string());
            self.completion_notifications
                .lock()
                .await
                .remove(root_session_id);
            self.background_events.lock().await.remove(root_session_id);
            self.provider_completion_notifications
                .lock()
                .await
                .retain(|owner, _| owner.root_session_id != root_session_id);
            let mut entries = self.entries.lock().await;
            let root_entries = entries
                .values()
                .filter(|entry| entry.owner.root_session_id == root_session_id)
                .cloned()
                .collect::<Vec<_>>();
            for entry in &root_entries {
                entry.close_lifecycle();
                entries.remove(&entry.id);
            }
            let mut pending = self.pending_eviction_cleanup.lock().await;
            let pending_entries = pending
                .values()
                .filter(|entry| entry.owner.root_session_id == root_session_id)
                .cloned()
                .collect::<Vec<_>>();
            for entry in &pending_entries {
                entry.close_lifecycle();
                pending.remove(&entry.id);
            }
            drop(pending);
            drop(entries);
            drop(lifecycle);
            root_entries
                .into_iter()
                .chain(pending_entries)
                .collect::<Vec<_>>()
        };
        for entry in root_entries {
            if entry.state().await.is_live() {
                let _ = entry.request_terminate(libc::SIGKILL).await;
            }
        }
    }

    /// ACN runtime 正常退出时收束当前 registry 下全部 root session 的受管进程。
    ///
    /// 这里先撤下 entries，避免迟到 watcher 把 completion 重新投递给已经关闭的 runtime；
    /// 真正的 PGID 终止在锁外逐项执行，不能跨 await 持有全局 store mutex。
    pub(crate) async fn shutdown_all(&self) {
        let entries = {
            let mut lifecycle = self.lifecycle.lock().await;
            let mut entries = self.entries.lock().await;
            let mut pending = self.pending_eviction_cleanup.lock().await;
            let active_entries = std::mem::take(&mut *entries)
                .into_values()
                .collect::<Vec<_>>();
            let pending_entries = std::mem::take(&mut *pending)
                .into_values()
                .collect::<Vec<_>>();
            for entry in active_entries.iter().chain(pending_entries.iter()) {
                entry.close_lifecycle();
                lifecycle.mark_root_session_closed(entry.owner.root_session_id.clone());
                lifecycle.mark_owner_closed(entry.owner.clone());
            }
            self.completion_notifications.lock().await.clear();
            self.background_events.lock().await.clear();
            self.provider_completion_notifications.lock().await.clear();
            drop(pending);
            drop(entries);
            active_entries
                .into_iter()
                .chain(pending_entries)
                .collect::<Vec<_>>()
        };
        for entry in entries {
            if entry.state().await.is_live() {
                let _ = entry.request_terminate(libc::SIGKILL).await;
            }
        }
    }

    /// 抽取当前 root session 尚未交给控制面的完成通知。每个 root queue 有界，overflow
    /// 时宁可由动态 `Recently completed` 投影恢复，也不无界保留通知。
    pub(crate) async fn take_completions_for_root(
        &self,
        root_session_id: &str,
    ) -> Vec<ProcessCompletion> {
        self.completion_notifications
            .lock()
            .await
            .remove(root_session_id)
            .map(VecDeque::into_iter)
            .map(Iterator::collect)
            .unwrap_or_default()
    }

    pub(crate) async fn record_completion(&self, process: &ManagedProcess) {
        if process.is_lifecycle_closed() {
            return;
        }
        let state = process.state().await;
        if state.is_live() {
            return;
        }
        // watcher 的 completion registration 与 final output delivery 需要串行；不能让
        // `write_stdin` 已确认交付且 entry 已移除后，迟到 watcher 重新注入 notification。
        let mut registration = process.completion_registration.lock().await;
        if !matches!(*registration, CompletionRegistration::Unrecorded) {
            // eviction cleanup 可能在 watcher 已完成登记后才得到调度。虽然不用再次写入
            // notification，但这次 terminal observation 已经证明 eviction 的可靠投递前提
            // 成立，必须撤下 reservation 留下的 pending marker；否则 owner 会永久背压。
            drop(registration);
            self.remove_pending_eviction_cleanup(process).await;
            return;
        }
        let (exit_code, signal) = match state {
            ProcessState::Finished {
                exit_code, signal, ..
            }
            | ProcessState::Terminated { exit_code, signal } => (exit_code, signal),
            ProcessState::Starting
            | ProcessState::Running
            | ProcessState::Terminating
            | ProcessState::Error => (None, None),
        };
        let finished_at = process.finished_at().await.unwrap_or_else(SystemTime::now);
        let elapsed_minutes = finished_at
            .duration_since(process.started_at)
            .map(|duration| duration.as_secs() / 60)
            .unwrap_or_default();
        let completion = ProcessCompletion {
            root_session_id: process.owner.root_session_id.clone(),
            owner: process.owner.clone(),
            process_id: process.id.as_str().to_string(),
            instance_id: process.instance_id,
            status: state.label().to_string(),
            exit_code,
            signal,
            finished_at,
            elapsed_minutes,
        };
        let lifecycle = self.lifecycle.lock().await;
        if lifecycle
            .closed_root_sessions
            .contains(&completion.root_session_id)
            || lifecycle.closed_owners.contains(&completion.owner)
        {
            return;
        }
        let mut notifications = self.completion_notifications.lock().await;
        let queue = notifications
            .entry(completion.root_session_id.clone())
            .or_default();
        if !queue
            .iter()
            .any(|existing| existing.instance_id == completion.instance_id)
        {
            // 根控制面的完成 fanout 在 headless session 中没有 consumer 时也必须有界。
            // 不丢 D21 的可靠投递：同一 completion 随后会进入独立的 per-owner provider
            // queue，直到一次 provider request 成功确认。
            while queue.len() >= self.root_completion_notification_capacity {
                queue.pop_front();
            }
            queue.push_back(completion.clone());
        }
        drop(notifications);
        let mut provider_notifications = self.provider_completion_notifications.lock().await;
        let queue = provider_notifications
            .entry(completion.owner.clone())
            .or_default();
        if !queue
            .iter()
            .any(|existing| existing.receipt.instance_id == completion.instance_id)
        {
            // reserve 的 owner backpressure 保证这里通常不超过 `limit`；一次 live
            // eviction 与所有现存 entry 同时完成时，最多额外保留一个。绝不能像旧实现
            // 那样 pop_front 丢弃尚未成功投递的事实。
            let maximum_retained = self.max_entries_per_owner.saturating_add(1);
            if queue.len() >= maximum_retained {
                log::error!(
                    target: "tool",
                    "background completion notification invariant exceeded for owner {}; retaining notification instead of dropping it",
                    completion.owner.root_session_id,
                );
            }
            queue.push_back(ProviderCompletionNotification {
                receipt: ProcessCompletionDeliveryReceipt {
                    process_id: completion.process_id.clone(),
                    instance_id: completion.instance_id,
                },
                completion,
                inflight: false,
            });
        }
        drop(provider_notifications);
        drop(lifecycle);
        *registration = CompletionRegistration::Registered;
        drop(registration);
        // live eviction 的 pending 标记直到 completion 已被 provider 队列保留后才释放。
        // cleanup_owner/root/shutdown 会抢先撤下该标记并直接终止 entry，因此不会漏清理。
        self.remove_pending_eviction_cleanup(process).await;
    }

    /// final `write_stdin` tool result 已被 provider 确认后，移除同一 allocation 此前可能
    /// 由 watcher 登记的最小 completion notification。它们表达相同的终态事实，后者不能
    /// 让已经成功消费并移除的 entry 在后续 request 中复活。
    async fn remove_completion_notification_after_final_output(&self, process: &ManagedProcess) {
        let mut notifications = self.provider_completion_notifications.lock().await;
        let should_remove_owner = if let Some(queue) = notifications.get_mut(&process.owner) {
            queue.retain(|notification| notification.receipt.instance_id != process.instance_id);
            queue.is_empty()
        } else {
            false
        };
        if should_remove_owner {
            notifications.remove(&process.owner);
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::super::process_group::configure_process_group;
    use super::*;

    fn manager() -> Arc<ProcessManager> {
        Arc::new(ProcessManager::new(32, 4, 64, 8))
    }

    #[tokio::test]
    async fn reserve_mints_distinct_ids_across_owners() {
        let manager = manager();
        let main = manager
            .reserve(
                ProcessOwner::main("session"),
                "echo main".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        let child = manager
            .reserve(
                ProcessOwner::subagent("session", "child"),
                "echo child".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        assert_ne!(main.id, child.id);
        assert_eq!(main.id.as_str().len(), 8);
    }

    #[tokio::test]
    async fn lifecycle_events_and_completion_preserve_full_process_owner() {
        let manager = manager();
        let owner = ProcessOwner::main_for_agent("agent-a", "session");
        let process = manager
            .reserve(
                owner.clone(),
                "printf lifecycle".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect("process fixture should reserve");

        manager.record_started(&process).await;
        process.mark_running().await;
        manager.record_state_changed(&process).await;
        process.append_stdout(b"lifecycle\n").await;
        manager.record_output(&process).await;
        process.mark_finished(Some(0), None, true).await;
        manager.record_state_changed(&process).await;
        manager.record_completion(&process).await;

        let events = manager.take_background_events_for_root("session").await;
        assert_eq!(events.len(), 4);
        for event in &events {
            let event_owner = match event {
                BackgroundProcessEvent::Started { owner, .. }
                | BackgroundProcessEvent::Output { owner, .. }
                | BackgroundProcessEvent::StateChanged { owner, .. } => owner,
            };
            assert_eq!(event_owner, &owner);
        }
        assert!(matches!(events[0], BackgroundProcessEvent::Started { .. }));
        assert!(
            matches!(events[1], BackgroundProcessEvent::StateChanged { ref status, .. } if status == "running")
        );
        assert!(matches!(events[2], BackgroundProcessEvent::Output { .. }));
        assert!(
            matches!(events[3], BackgroundProcessEvent::StateChanged { ref status, .. } if status == "finished")
        );

        let completions = manager.take_completions_for_root("session").await;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].owner, owner);
        assert_eq!(completions[0].status, "finished");
    }

    #[tokio::test]
    async fn final_output_delivery_blocks_late_completion_reinjection() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let process = manager
            .reserve(
                owner.clone(),
                "printf final".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect("fixture should reserve");
        process.mark_finished(Some(0), None, true).await;
        let receipt = process
            .prepare_output_delivery(OutputCursor(0), OutputCursor(0), true)
            .await
            .expect("first output page should reserve the delivery slot");
        manager
            .begin_deliveries(std::slice::from_ref(&receipt))
            .await;
        manager
            .commit_deliveries(std::slice::from_ref(&receipt))
            .await;

        // 模拟 watcher 在 final tool result 已获 provider 成功确认、entry 已移除之后才
        // 继续到 record_completion。它不能把同一 instance 重新送进动态上下文。
        manager.record_completion(&process).await;
        assert!(manager
            .pending_completion_notifications_for_owner(&owner)
            .await
            .is_empty());
        assert!(manager
            .find_for_owner(&owner, process.id.as_str())
            .await
            .is_none());
    }

    #[tokio::test]
    async fn closed_entry_blocks_late_watcher_events_after_close_marker_eviction() {
        let manager = manager();
        let owner = ProcessOwner::main("closed-session");
        let process = manager
            .reserve(
                owner.clone(),
                "sleep 300".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect("fixture should reserve");
        manager.cleanup_owner(&owner).await;

        // close marker 是有界 admission fuse；模拟大量其他 owner 收束后，旧 marker 已被
        // 淘汰。迟到 watcher 仍持有 process Arc，但 entry 自身的 closed bit 必须继续阻止
        // 它复建 event/completion queue。
        for index in 0..=MAX_CLOSED_LIFECYCLE_MARKERS {
            manager
                .cleanup_owner(&ProcessOwner::main(format!("other-session-{index}")))
                .await;
        }
        assert!(!manager
            .lifecycle
            .lock()
            .await
            .closed_owners
            .contains(&owner));

        process.mark_finished(Some(0), None, true).await;
        manager.record_started(&process).await;
        manager.record_output(&process).await;
        manager.record_state_changed(&process).await;
        manager.record_completion(&process).await;

        assert!(manager
            .take_background_events_for_root("closed-session")
            .await
            .is_empty());
        assert!(manager
            .take_completions_for_root("closed-session")
            .await
            .is_empty());
        assert!(manager
            .pending_completion_notifications_for_owner(&owner)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn unconsumed_root_fanout_queues_stay_bounded_across_short_processes() {
        // 以最小 owner entry 上限构造 manager，模拟没有 TUI/session-engine drain 的
        // headless root session 连续完成多个短命令。可靠 provider notification 每轮确认，
        // 此处只断言可丢弃的 root fanout 不会绕过 entry 上限而长期增长。
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
        let mut owners = Vec::new();
        let mut latest_id = String::new();

        for index in 0..6 {
            // root session 可以有任意多个 subagent；每个 owner 都在自己的 1-entry
            // 限制内，因此这个 fixture 直接覆盖此前会无界增长的 root fanout 路径。
            let owner = ProcessOwner::subagent("session", format!("child-{index}"));
            let process = manager
                .reserve(
                    owner.clone(),
                    format!("printf {index}"),
                    "bash".into(),
                    "/tmp".into(),
                    false,
                )
                .await
                .expect("short-process fixture should reserve");
            latest_id = process.id.as_str().to_string();
            manager.record_started(&process).await;
            process.mark_running().await;
            manager.record_state_changed(&process).await;
            process.append_stdout(b"done\n").await;
            manager.record_output(&process).await;
            process.mark_finished(Some(0), None, true).await;
            manager.record_state_changed(&process).await;
            manager.record_completion(&process).await;

            let (receipts, _) = manager
                .begin_completion_notification_delivery_snapshot(&owner)
                .await;
            manager
                .commit_completion_notification_delivery(&owner, &receipts)
                .await;
            owners.push(owner);
        }

        let events = manager.take_background_events_for_root("session").await;
        assert_eq!(
            events.len(),
            manager.root_background_event_capacity,
            "root event fanout must retain only its bounded most-recent window"
        );
        assert!(events.iter().all(|event| match event {
            BackgroundProcessEvent::Started { process_id, .. }
            | BackgroundProcessEvent::Output { process_id, .. }
            | BackgroundProcessEvent::StateChanged { process_id, .. } => process_id == &latest_id,
        }));

        let completions = manager.take_completions_for_root("session").await;
        assert_eq!(
            completions.len(),
            manager.root_completion_notification_capacity,
            "root completion fanout must not accumulate after its consumers stop draining"
        );
        assert_eq!(completions[0].process_id, latest_id);
        for owner in owners {
            assert!(manager
                .pending_completion_notifications_for_owner(&owner)
                .await
                .is_empty());
        }
    }

    #[tokio::test]
    async fn reserve_retries_colliding_ids_and_reports_exhaustion() {
        let manager = Arc::new(ProcessManager::new(32, 2, 8, 0));
        let owner = ProcessOwner::main("session");
        manager.set_test_id_candidates([
            ProcessId("11111111".into()),
            ProcessId("11111111".into()),
            ProcessId("22222222".into()),
        ]);
        let first = manager
            .reserve(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        let second = manager
            .reserve(
                owner.clone(),
                "sleep 2".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(first.id.as_str(), "11111111");
        assert_eq!(second.id.as_str(), "22222222");

        manager
            .set_test_id_candidates([ProcessId("22222222".into()), ProcessId("22222222".into())]);
        let error = manager
            .reserve(owner, "sleep 3".into(), "bash".into(), "/tmp".into(), false)
            .await
            .expect_err("all configured candidates collide");
        assert!(error.contains("unable to allocate unique process_id after 2 attempts"));
    }

    #[tokio::test]
    async fn live_for_owner_does_not_cross_owner_boundary() {
        let manager = manager();
        let main_owner = ProcessOwner::main("session");
        let child_owner = ProcessOwner::subagent("session", "child");
        let _main = manager
            .reserve(
                main_owner.clone(),
                "echo main".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        let _child = manager
            .reserve(
                child_owner.clone(),
                "echo child".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        assert_eq!(manager.live_for_owner(&main_owner).await.len(), 1);
        assert_eq!(manager.live_for_owner(&child_owner).await.len(), 1);
    }

    #[tokio::test]
    async fn live_ids_for_tool_use_only_reports_its_own_registered_entry() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let selected = manager
            .reserve_with_process_group(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
                None,
                Some("toolu_selected".into()),
            )
            .await
            .expect("reserve selected entry");
        let _other = manager
            .reserve_with_process_group(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
                None,
                Some("toolu_other".into()),
            )
            .await
            .expect("reserve other entry");

        assert_eq!(
            manager
                .live_ids_for_owner_and_tool_use(&owner, "toolu_selected")
                .await,
            vec![selected.id.as_str().to_string()]
        );
    }

    #[tokio::test]
    async fn terminate_request_rejects_an_entry_that_exited_after_a_stale_snapshot() {
        let manager = manager();
        let entry = manager
            .reserve(
                ProcessOwner::main("session"),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        entry.mark_running().await;
        assert!(entry.state().await.is_live());

        // 模拟 `/ps` 已经取得 live snapshot、用户确认前 watcher 完成自然退出。
        entry.mark_finished(Some(0), None, true).await;
        assert_eq!(
            entry.request_terminate(libc::SIGKILL).await.unwrap(),
            TerminateRequestResult::AlreadyExited
        );
    }

    #[tokio::test]
    async fn root_reap_retires_control_and_drop_pgid_before_pid_can_be_reused() {
        let manager = manager();
        let entry = manager
            .reserve_with_process_group(
                ProcessOwner::main("session"),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
                Some(42),
                None,
            )
            .await
            .expect("fixture should reserve");

        let root_reap_gate = entry.acquire_root_reap_gate().await;
        entry.retire_process_group_after_root_reap().await;
        drop(root_reap_gate);

        assert_eq!(entry.process_group_ids_for_test().await, (None, None));
    }

    #[tokio::test]
    async fn protected_recent_entries_are_never_evicted_to_exceed_owner_capacity() {
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 1));
        let owner = ProcessOwner::main("session");
        let _first = manager
            .reserve(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        let error = manager
            .reserve(owner, "sleep 2".into(), "bash".into(), "/tmp".into(), false)
            .await
            .expect_err("the protected entry must prevent an over-capacity insert");
        assert!(error.contains("capacity reached"));
    }

    #[tokio::test]
    async fn terminal_entry_is_evicted_before_live_entry_when_capacity_is_full() {
        let manager = Arc::new(ProcessManager::new(32, 4, 2, 0));
        let owner = ProcessOwner::main("session");
        let terminal = manager
            .reserve(
                owner.clone(),
                "true".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        terminal.mark_finished(Some(0), None, true).await;
        let live = manager
            .reserve(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        let replacement = manager
            .reserve(
                owner.clone(),
                "sleep 2".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        assert!(manager
            .find_for_owner(&owner, terminal.id.as_str())
            .await
            .is_none());
        assert!(manager
            .find_for_owner(&owner, live.id.as_str())
            .await
            .is_some());
        assert!(manager
            .find_for_owner(&owner, replacement.id.as_str())
            .await
            .is_some());
        // reserve 后的容量清理由 detached task 承接；这里等待其最小 completion
        // notification，而不是假设 Tokio 已在本次同步断言前调度该 task。
        let notifications = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notifications = manager
                    .pending_completion_notifications_for_owner(&owner)
                    .await;
                if !notifications.is_empty() {
                    return notifications;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal eviction must eventually enqueue its completion notification");
        assert_eq!(
            notifications
                .iter()
                .map(|completion| completion.process_id.as_str())
                .collect::<Vec<_>>(),
            vec![terminal.id.as_str()]
        );
        // watcher 在 reserve 之后再次 record_completion 时也不能重复投递。
        manager.record_completion(&terminal).await;
        assert_eq!(
            manager
                .pending_completion_notifications_for_owner(&owner)
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn capacity_refuses_to_evict_live_entry_when_terminal_state_is_busy() {
        let manager = Arc::new(ProcessManager::new(32, 4, 2, 0));
        let owner = ProcessOwner::main("session");
        let terminal = manager
            .reserve(
                owner.clone(),
                "true".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        terminal.mark_finished(Some(0), None, true).await;
        let live = manager
            .reserve(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        // 模拟 watcher 正在推进 terminal 状态：reserve 不能把 lock 竞争误判为 terminal，
        // 否则会退而错误地淘汰 live entry。
        let state_guard = terminal.state.lock().await;
        let error = manager
            .reserve(
                owner.clone(),
                "replacement".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect_err("state contention must ask caller to retry");
        assert!(error.contains("state is busy"));
        drop(state_guard);

        assert!(manager
            .find_for_owner(&owner, terminal.id.as_str())
            .await
            .is_some());
        assert!(manager
            .find_for_owner(&owner, live.id.as_str())
            .await
            .is_some());
        assert!(live.state().await.is_live());
    }

    #[tokio::test]
    async fn capacity_reservation_returns_before_evicted_cleanup_can_block() {
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
        let owner = ProcessOwner::main("session");
        let old = manager
            .reserve(
                owner.clone(),
                "old".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        old.mark_running().await;
        let cleanup_gate = manager.pause_next_eviction_cleanup_for_test().await;
        manager.set_test_id_candidates([ProcessId("c0ffee00".into())]);
        let reserving_manager = Arc::clone(&manager);
        let reservation = tokio::spawn(async move {
            reserving_manager
                .reserve(owner, "new".into(), "bash".into(), "/tmp".into(), false)
                .await
        });

        let replacement = tokio::time::timeout(Duration::from_millis(200), reservation)
            .await
            .expect("new reservation must not await evicted entry cleanup")
            .expect("reservation task should not panic")
            .expect("capacity replacement should succeed");
        assert_eq!(replacement.id.as_str(), "c0ffee00");
        assert!(manager
            .find_for_owner(&ProcessOwner::main("session"), replacement.id.as_str())
            .await
            .is_some());
        tokio::time::timeout(
            Duration::from_millis(200),
            cleanup_gate.wait_until_entered(),
        )
        .await
        .expect("eviction cleanup must run detached from the successful reservation");
        cleanup_gate.release();
    }

    #[tokio::test]
    async fn owner_cleanup_drains_live_entry_pending_eviction_cleanup() {
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
        let owner = ProcessOwner::main("session");
        let evicted = manager
            .reserve(
                owner.clone(),
                "old".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        evicted.mark_running().await;
        let cleanup_gate = manager.pause_next_eviction_cleanup_for_test().await;
        let _replacement = manager
            .reserve(
                owner.clone(),
                "new".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(200),
            cleanup_gate.wait_until_entered(),
        )
        .await
        .expect("eviction task must be paused after reservation");

        // evicted entry 已从 entries 移除，仍必须由 owner cleanup 收束，而不能依赖
        // 此时被暂停的 detached eviction task。
        manager.cleanup_owner(&owner).await;
        assert_eq!(evicted.state().await, ProcessState::Terminating);
        assert!(manager.pending_eviction_cleanup.lock().await.is_empty());
        cleanup_gate.release();
    }

    #[tokio::test]
    async fn root_and_runtime_cleanup_drain_live_pending_evictions() {
        for cleanup in ["root", "runtime"] {
            let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
            let owner = ProcessOwner::main("session");
            let evicted = manager
                .reserve(
                    owner.clone(),
                    "old".into(),
                    "bash".into(),
                    "/tmp".into(),
                    false,
                )
                .await
                .unwrap();
            evicted.mark_running().await;
            let cleanup_gate = manager.pause_next_eviction_cleanup_for_test().await;
            let _replacement = manager
                .reserve(owner, "new".into(), "bash".into(), "/tmp".into(), false)
                .await
                .unwrap();
            tokio::time::timeout(
                Duration::from_millis(200),
                cleanup_gate.wait_until_entered(),
            )
            .await
            .expect("eviction task must be paused after reservation");

            if cleanup == "root" {
                manager.cleanup_root_session("session").await;
            } else {
                manager.shutdown_all().await;
            }
            assert_eq!(evicted.state().await, ProcessState::Terminating);
            assert!(manager.pending_eviction_cleanup.lock().await.is_empty());
            cleanup_gate.release();
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn dropping_last_manager_handle_kills_registered_process_group() {
        let mut command = tokio::process::Command::new("sleep");
        command.arg("30");
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("sleep fixture should spawn");
        let process_group_id = i32::try_from(child.id().expect("child PID should be available"))
            .expect("Unix child PID must fit i32");

        let manager = manager();
        let process = manager
            .reserve_with_process_group(
                ProcessOwner::main("session"),
                "sleep 30".into(),
                "bash".into(),
                "/tmp".into(),
                false,
                Some(process_group_id),
                None,
            )
            .await
            .expect("registered process fixture should reserve");
        process.mark_running().await;
        drop(process);
        drop(manager);

        let terminal = tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("manager drop should promptly terminate its process group")
            .expect("sleep child should be waitable after manager drop");
        assert!(!terminal.success());
    }

    #[tokio::test]
    async fn unacknowledged_completion_notifications_backpressure_new_reservations() {
        let manager = Arc::new(ProcessManager::new(32, 4, 2, 0));
        let owner = ProcessOwner::main("session");
        for command in ["first", "second"] {
            let entry = manager
                .reserve(
                    owner.clone(),
                    command.into(),
                    "bash".into(),
                    "/tmp".into(),
                    false,
                )
                .await
                .unwrap();
            entry.mark_finished(Some(0), None, true).await;
            manager.record_completion(&entry).await;
        }

        let error = manager
            .reserve(
                owner.clone(),
                "must-wait".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect_err("undelivered completion facts must apply owner-local backpressure");
        assert!(error.contains("awaiting provider delivery"));
        assert_eq!(
            manager
                .pending_completion_notifications_for_owner(&owner)
                .await
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn terminal_eviction_does_not_drop_its_completion_before_delivery() {
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
        let owner = ProcessOwner::main("session");
        let old = manager
            .reserve(
                owner.clone(),
                "old".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        old.mark_finished(Some(0), None, true).await;
        let cleanup_gate = manager.pause_next_eviction_cleanup_for_test().await;
        let _replacement = manager
            .reserve(
                owner.clone(),
                "replacement".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(200),
            cleanup_gate.wait_until_entered(),
        )
        .await
        .expect("terminal eviction cleanup must be pending");

        let error = manager
            .reserve(
                owner.clone(),
                "third".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect_err("one owner cannot accumulate multiple eviction cleanups");
        assert!(error.contains("eviction cleanup is still pending"));

        cleanup_gate.release();
        let notifications = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notifications = manager
                    .pending_completion_notifications_for_owner(&owner)
                    .await;
                if !notifications.is_empty() {
                    return notifications;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("evicted terminal notification must be retained");
        assert_eq!(
            notifications
                .iter()
                .map(|completion| completion.instance_id)
                .collect::<Vec<_>>(),
            vec![old.instance_id]
        );
        let error = manager
            .reserve(
                owner,
                "after-eviction".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect_err("notification remains until provider delivery succeeds");
        assert!(error.contains("awaiting provider delivery"));
    }

    #[tokio::test]
    async fn terminal_eviction_releases_pending_marker_when_completion_was_already_registered() {
        let manager = Arc::new(ProcessManager::new(32, 4, 2, 0));
        let owner = ProcessOwner::main("session");
        let terminal = manager
            .reserve(
                owner.clone(),
                "terminal".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        terminal.mark_finished(Some(0), None, true).await;
        // 真实竞态：watcher 先于随后发生的容量淘汰完成登记。
        manager.record_completion(&terminal).await;
        let live = manager
            .reserve(
                owner.clone(),
                "live".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        live.mark_running().await;
        let cleanup_gate = manager.pause_next_eviction_cleanup_for_test().await;
        let _replacement = manager
            .reserve(
                owner.clone(),
                "replacement".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(200),
            cleanup_gate.wait_until_entered(),
        )
        .await
        .expect("terminal eviction cleanup must be pending");
        cleanup_gate.release();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !manager
                    .pending_eviction_cleanup
                    .lock()
                    .await
                    .contains_key(&terminal.id)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("already-registered terminal completion must release eviction pending marker");

        // Pending marker 不再泄漏，下一次容量 reservation 不会被错误拒绝。
        manager
            .reserve(
                owner,
                "after-terminal-eviction".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect("completed eviction cleanup must not permanently backpressure owner");
    }

    #[tokio::test]
    async fn live_eviction_remains_pending_until_completion_is_registered() {
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
        let owner = ProcessOwner::main("session");
        let old = manager
            .reserve(
                owner.clone(),
                "old".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        old.mark_running().await;
        let cleanup_gate = manager.pause_next_eviction_cleanup_for_test().await;
        let _replacement = manager
            .reserve(
                owner.clone(),
                "replacement".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(200),
            cleanup_gate.wait_until_entered(),
        )
        .await
        .expect("live eviction task must be pending");
        cleanup_gate.release();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if manager
                    .pending_eviction_cleanup
                    .lock()
                    .await
                    .contains_key(&old.id)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("live entry stays pending after SIGKILL until watcher completion");
        let error = manager
            .reserve(
                owner.clone(),
                "third".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect_err("pending live eviction must backpressure another replacement");
        assert!(error.contains("eviction cleanup is still pending"));

        old.mark_finished(None, Some(libc::SIGKILL), false).await;
        manager.record_completion(&old).await;
        assert!(manager.pending_eviction_cleanup.lock().await.is_empty());
        assert_eq!(
            manager
                .pending_completion_notifications_for_owner(&owner)
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn io_failure_marks_live_entry_error_and_closes_stdin() {
        let manager = manager();
        let entry = manager
            .reserve(
                ProcessOwner::main("session"),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                true,
            )
            .await
            .unwrap();
        let (input_tx, _input_rx) = mpsc::channel(1);
        entry
            .attach_pty(input_tx, Arc::new(Semaphore::new(16)), 16, None)
            .await;
        entry.mark_running().await;

        entry.handle_io_failure("fixture reader failed").await;

        assert_eq!(entry.state().await, ProcessState::Error);
        assert!(!entry.stdin_open().await);
        let (stdout, _) = entry.output_snapshot().await;
        assert!(String::from_utf8_lossy(&stdout.bytes).contains("reader failed"));
    }

    #[tokio::test]
    async fn pty_input_queue_is_bounded_by_bytes_not_only_message_count() {
        let manager = manager();
        let entry = manager
            .reserve(
                ProcessOwner::main("session"),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                true,
            )
            .await
            .unwrap();
        let (input_tx, _input_rx) = mpsc::channel(8);
        entry
            .attach_pty(input_tx, Arc::new(Semaphore::new(4)), 4, None)
            .await;

        entry.write(b"abcd".to_vec()).await.unwrap();
        let error = entry
            .write(b"e".to_vec())
            .await
            .expect_err("queued bytes must not exceed the configured budget");
        assert!(error.contains("PTY stdin buffer is full"));
        let oversized = entry
            .write(b"abcde".to_vec())
            .await
            .expect_err("one write must not exceed the configured byte budget");
        assert!(oversized.contains("PTY stdin write exceeds configured 4 byte buffer limit"));
    }

    #[tokio::test]
    async fn pty_input_queue_is_bounded_by_messages_when_byte_budget_remains() {
        let manager = manager();
        let entry = manager
            .reserve(
                ProcessOwner::main("session"),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                true,
            )
            .await
            .unwrap();
        // 保留 receiver 但不读取，模拟 blocking PTY writer；消息槽先满而字节预算仍有余量。
        let (input_tx, _input_rx) = mpsc::channel(1);
        entry
            .attach_pty(input_tx, Arc::new(Semaphore::new(16)), 16, None)
            .await;

        entry.write(b"a".to_vec()).await.unwrap();
        let error = tokio::time::timeout(Duration::from_millis(50), entry.write(b"b".to_vec()))
            .await
            .expect("full PTY message queue must not await writer drain")
            .expect_err("full PTY message queue must return a retryable error");
        assert!(error.contains("PTY stdin buffer is full"));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn io_failure_after_process_group_exits_defers_to_watcher_terminal_status() {
        let manager = manager();
        let entry = manager
            .reserve_with_process_group(
                ProcessOwner::main("session"),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
                // macOS/Linux PID upper bounds are below i32::MAX, so this cannot target a
                // real process group and kill(-pgid, SIGKILL) deterministically returns ESRCH.
                Some(i32::MAX),
                None,
            )
            .await
            .unwrap();
        entry.mark_running().await;

        entry.handle_io_failure("late reader close").await;

        assert_eq!(entry.state().await, ProcessState::Running);
    }

    #[tokio::test]
    async fn starting_entry_is_not_evictable_before_control_is_attached() {
        let manager = Arc::new(ProcessManager::new(32, 4, 1, 0));
        let owner = ProcessOwner::main("session");
        let starting = manager
            .reserve(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();

        let error = manager
            .reserve(
                owner.clone(),
                "sleep 2".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .expect_err("Starting entry must not be evicted before PGID attachment");
        assert!(error.contains("capacity reached"));
        assert!(manager
            .find_for_owner(&owner, starting.id.as_str())
            .await
            .is_some());
    }

    #[tokio::test]
    async fn final_delivery_removes_terminal_entry_only_after_commit() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let entry = manager
            .reserve(
                owner.clone(),
                "printf done".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        entry.append_stdout(b"done").await;
        entry.mark_finished(Some(0), None, true).await;
        let (stdout, stderr) = entry.output_snapshot().await;
        let receipt = entry
            .prepare_output_delivery(stdout.cursor, stderr.cursor, true)
            .await
            .expect("first output page should reserve the delivery slot");

        manager
            .begin_deliveries(std::slice::from_ref(&receipt))
            .await;
        manager.rollback_inflight_deliveries_for_owner(&owner).await;
        assert!(manager
            .find_for_owner(&owner, entry.id.as_str())
            .await
            .is_some());

        let retry_receipt = entry
            .prepare_output_delivery(stdout.cursor, stderr.cursor, true)
            .await
            .expect("rolled-back output page should reserve the delivery slot");
        manager
            .begin_deliveries(std::slice::from_ref(&retry_receipt))
            .await;
        manager
            .commit_deliveries(std::slice::from_ref(&retry_receipt))
            .await;
        assert!(manager
            .find_for_owner(&owner, entry.id.as_str())
            .await
            .is_none());
    }

    #[tokio::test]
    async fn stale_delivery_receipt_cannot_commit_a_reused_process_id() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        manager.set_test_id_candidates([ProcessId("deadbeef".into())]);
        let old = manager
            .reserve(
                owner.clone(),
                "printf old".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        old.append_stdout(b"old").await;
        let (stdout, stderr) = old.output_snapshot().await;
        let receipt = old
            .prepare_output_delivery(stdout.cursor, stderr.cursor, false)
            .await
            .expect("first output page should reserve the delivery slot");

        // 模拟容量/LRU 已经撤下旧 terminal 后，8-hex ID 被后续 allocation 重抽。
        manager.entries.lock().await.remove(&old.id);
        manager.set_test_id_candidates([ProcessId("deadbeef".into())]);
        let replacement = manager
            .reserve(
                owner.clone(),
                "printf replacement".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(replacement.id, old.id);
        assert_ne!(replacement.instance_id, old.instance_id);

        manager
            .begin_deliveries(std::slice::from_ref(&receipt))
            .await;
        manager
            .commit_deliveries(std::slice::from_ref(&receipt))
            .await;

        let (committed_stdout, committed_stderr) = replacement.output_delivery_cursors().await;
        assert_eq!(committed_stdout, OutputCursor(0));
        assert_eq!(committed_stderr, OutputCursor(0));
        assert!(manager
            .find_for_owner(&owner, replacement.id.as_str())
            .await
            .is_some());
    }

    #[tokio::test]
    async fn completion_notifications_keep_distinct_instances_when_process_id_is_reused() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        manager.set_test_id_candidates([ProcessId("deadbeef".into())]);
        let old = manager
            .reserve(
                owner.clone(),
                "printf old".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        old.mark_finished(Some(0), None, true).await;
        manager.record_completion(&old).await;

        // 模拟容量淘汰已经从 retained entry 删除旧对象，但 provider 尚未成功消费其
        // completion notification。后续 allocation 可以重用同一 8-hex logical ID。
        manager.entries.lock().await.remove(&old.id);
        manager.set_test_id_candidates([ProcessId("deadbeef".into())]);
        let replacement = manager
            .reserve(
                owner.clone(),
                "printf replacement".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        replacement.mark_finished(Some(0), None, true).await;
        manager.record_completion(&replacement).await;
        assert_eq!(replacement.id, old.id);
        assert_ne!(replacement.instance_id, old.instance_id);

        let (receipts, snapshot) = manager
            .begin_completion_notification_delivery_snapshot(&owner)
            .await;
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot
            .iter()
            .all(|completion| completion.process_id == "deadbeef"));
        assert_ne!(snapshot[0].instance_id, snapshot[1].instance_id);

        manager
            .commit_completion_notification_delivery(&owner, &receipts[..1])
            .await;
        let pending = manager
            .pending_completion_notifications_for_owner(&owner)
            .await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].instance_id, replacement.instance_id);
    }

    #[tokio::test]
    async fn abort_reservation_removes_starting_entry() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let entry = manager
            .reserve(
                owner.clone(),
                "sleep 300".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        assert!(matches!(entry.state().await, ProcessState::Starting));

        manager.abort_reservation(Arc::clone(&entry)).await;

        assert!(manager
            .find_for_owner(&owner, entry.id.as_str())
            .await
            .is_none());
    }

    #[test]
    fn lifecycle_close_markers_are_bounded() {
        let mut lifecycle = ProcessManagerLifecycle::default();
        for index in 0..=MAX_CLOSED_LIFECYCLE_MARKERS {
            lifecycle.mark_root_session_closed(format!("session-{index}"));
            lifecycle.mark_owner_closed(ProcessOwner::main(format!("owner-{index}")));
        }
        assert_eq!(
            lifecycle.closed_root_sessions.len(),
            MAX_CLOSED_LIFECYCLE_MARKERS
        );
        assert_eq!(lifecycle.closed_owners.len(), MAX_CLOSED_LIFECYCLE_MARKERS);
        assert!(!lifecycle.closed_root_sessions.contains("session-0"));
        assert!(!lifecycle
            .closed_owners
            .contains(&ProcessOwner::main("owner-0")));
    }

    #[tokio::test]
    async fn completion_notification_retries_until_provider_success() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let entry = manager
            .reserve(
                owner.clone(),
                "false".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        entry.mark_finished(Some(1), None, false).await;
        manager.record_completion(&entry).await;

        let first = manager.begin_completion_notification_delivery(&owner).await;
        assert_eq!(
            first
                .iter()
                .map(|receipt| receipt.process_id.as_str())
                .collect::<Vec<_>>(),
            vec![entry.id.as_str()]
        );
        manager
            .rollback_completion_notification_delivery(&owner)
            .await;
        let retry = manager.begin_completion_notification_delivery(&owner).await;
        assert_eq!(retry, first);
        manager
            .commit_completion_notification_delivery(&owner, &retry)
            .await;
        assert!(manager
            .pending_completion_notifications_for_owner(&owner)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn completion_delivery_snapshot_is_atomic_with_its_delivery_ids() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let first = manager
            .reserve(
                owner.clone(),
                "first".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        first.mark_finished(Some(0), None, true).await;
        manager.record_completion(&first).await;

        let (delivery_ids, snapshot) = manager
            .begin_completion_notification_delivery_snapshot(&owner)
            .await;
        assert_eq!(
            delivery_ids
                .iter()
                .map(|receipt| receipt.process_id.as_str())
                .collect::<Vec<_>>(),
            vec![first.id.as_str()]
        );
        assert_eq!(
            snapshot
                .iter()
                .map(|completion| completion.process_id.as_str())
                .collect::<Vec<_>>(),
            vec![first.id.as_str()]
        );

        let second = manager
            .reserve(
                owner.clone(),
                "second".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        second.mark_finished(Some(0), None, true).await;
        manager.record_completion(&second).await;
        manager
            .commit_completion_notification_delivery(&owner, &delivery_ids)
            .await;

        assert_eq!(
            manager
                .pending_completion_notifications_for_owner(&owner)
                .await
                .iter()
                .map(|completion| completion.process_id.as_str())
                .collect::<Vec<_>>(),
            vec![second.id.as_str()]
        );
    }

    #[tokio::test]
    async fn owner_cleanup_removes_entries_and_blocks_late_completion_notifications() {
        let manager = manager();
        let owner = ProcessOwner::main("session");
        let entry = manager
            .reserve(
                owner.clone(),
                "sleep 1".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .unwrap();
        manager.cleanup_owner(&owner).await;
        entry.mark_finished(Some(0), None, true).await;
        manager.record_completion(&entry).await;

        assert!(manager.retained_for_owner(&owner).await.is_empty());
        assert!(manager
            .pending_completion_notifications_for_owner(&owner)
            .await
            .is_empty());
        assert!(manager
            .reserve(
                owner,
                "should fail".into(),
                "bash".into(),
                "/tmp".into(),
                false,
            )
            .await
            .is_err());
    }
}
