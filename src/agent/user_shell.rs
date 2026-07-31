//! 用户主动触发的 `!` shell command 执行器。
//!
//! 本模块只负责一次性子进程执行、输出截断和 transcript record 格式化。
//! 它不维护持久 shell 状态；`export` / `cd` 等只影响本次子进程。

use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::task::JoinSet;
use tokio::time::{self, Instant as TokioInstant};
use tokio_util::sync::CancellationToken;

use crate::config::{UserShellConfig, USER_SHELL_DRAIN_GRACE_MS, USER_SHELL_TERMINATION_GRACE_MS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserShellCommandStatus {
    Completed,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserShellCommandOutput {
    pub status: UserShellCommandStatus,
    pub exit_code: Option<i32>,
    pub duration_ms: u128,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum UserShellError {
    #[error("user shell command is disabled")]
    Disabled,
    #[error("shell command 不能为空")]
    EmptyCommand,
    #[error("不支持的 shell: {0}")]
    UnsupportedShell(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone, Copy)]
struct UserShellLifecycleTimings {
    drain_grace: Duration,
    termination_grace: Duration,
}

impl Default for UserShellLifecycleTimings {
    fn default() -> Self {
        Self {
            drain_grace: Duration::from_millis(USER_SHELL_DRAIN_GRACE_MS),
            termination_grace: Duration::from_millis(USER_SHELL_TERMINATION_GRACE_MS),
        }
    }
}

pub async fn run_user_shell_command(
    cfg: &UserShellConfig,
    workspace_root: &Path,
    command: &str,
    cancel: CancellationToken,
) -> Result<UserShellCommandOutput, UserShellError> {
    run_user_shell_command_with_timings(
        cfg,
        workspace_root,
        command,
        cancel,
        UserShellLifecycleTimings::default(),
    )
    .await
}

async fn run_user_shell_command_with_timings(
    cfg: &UserShellConfig,
    workspace_root: &Path,
    command: &str,
    cancel: CancellationToken,
    timings: UserShellLifecycleTimings,
) -> Result<UserShellCommandOutput, UserShellError> {
    if !cfg.enabled {
        return Err(UserShellError::Disabled);
    }
    let command = command.trim();
    if command.is_empty() {
        return Err(UserShellError::EmptyCommand);
    }

    let spec = ShellSpec::resolve(&cfg.shell)?;
    let mut shell_command = spec.command(command, cfg.login_shell);
    prepare_process_group(&mut shell_command);
    let child = shell_command
        .current_dir(workspace_root)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut child = ManagedChild::new(child);

    let stdout = child.child.stdout.take();
    let stderr = child.child.stderr.take();
    let output_budget = Arc::new(AtomicUsize::new(cfg.max_output_chars));
    let stdout_capture = Arc::new(Mutex::new(PipeCapture::default()));
    let stderr_capture = Arc::new(Mutex::new(PipeCapture::default()));
    let mut pipe_readers = JoinSet::new();
    pipe_readers.spawn(read_pipe_limited(
        stdout,
        Arc::clone(&output_budget),
        Arc::clone(&stdout_capture),
    ));
    pipe_readers.spawn(read_pipe_limited(
        stderr,
        output_budget,
        Arc::clone(&stderr_capture),
    ));
    let started = Instant::now();
    let deadline = TokioInstant::now() + Duration::from_secs(cfg.timeout_secs);

    let mut status = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            UserShellCommandStatus::Cancelled
        }
        _ = time::sleep_until(deadline) => UserShellCommandStatus::TimedOut,
        wait_result = child.wait_parent() => {
            wait_result?;
            UserShellCommandStatus::Completed
        }
    };

    let mut pipe_output_forced_short = status != UserShellCommandStatus::Completed;
    if status == UserShellCommandStatus::Completed {
        match drain_pipe_readers(&mut pipe_readers, deadline, timings.drain_grace, &cancel).await? {
            PipeDrainOutcome::Drained => child.reap_and_disarm().await?,
            PipeDrainOutcome::GraceExpired => pipe_output_forced_short = true,
            PipeDrainOutcome::TimedOut => {
                status = UserShellCommandStatus::TimedOut;
                pipe_output_forced_short = true;
            }
            PipeDrainOutcome::Cancelled => {
                status = UserShellCommandStatus::Cancelled;
                pipe_output_forced_short = true;
            }
        }
    }

    if pipe_output_forced_short {
        let interrupt_already_triggered = status != UserShellCommandStatus::Completed;
        if let Some(interrupted_status) = child
            .terminate(
                timings.termination_grace,
                deadline,
                &cancel,
                interrupt_already_triggered,
            )
            .await?
        {
            status = interrupted_status;
        }
        abort_pipe_readers(&mut pipe_readers).await;
    }

    let stdout = snapshot_pipe_output(&stdout_capture)?;
    let stderr = snapshot_pipe_output(&stderr_capture)?;

    Ok(UserShellCommandOutput {
        status,
        exit_code: child.exit_code,
        duration_ms: started.elapsed().as_millis(),
        truncated: pipe_output_forced_short || stdout.truncated || stderr.truncated,
        stdout: stdout.text,
        stderr: stderr.text,
    })
}

pub fn format_user_shell_command_record(command: &str, output: &UserShellCommandOutput) -> String {
    let mut result = format!(
        "Status: {}\nExit code: {}\nDuration: {} seconds\nStdout:\n{}\nStderr:\n{}",
        output.status.as_record_label(),
        output
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "none".into()),
        format_seconds_4(output.duration_ms),
        escape_xml_text(&output.stdout),
        escape_xml_text(&output.stderr)
    );
    if output.truncated {
        result.push_str("\nOutput truncated: true");
    }
    format!(
        "<user_shell_command>\n<command>\n{}\n</command>\n<result>\n{result}\n</result>\n</user_shell_command>",
        escape_xml_text(command)
    )
}

impl UserShellCommandStatus {
    pub fn as_record_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ShellKind {
    Unix,
    PowerShell,
    Cmd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellSpec {
    program: String,
    kind: ShellKind,
}

impl ShellSpec {
    fn resolve(raw: &str) -> Result<Self, UserShellError> {
        let raw = raw.trim();
        if raw == "auto" {
            return Ok(auto_shell());
        }
        let path = Path::new(raw);
        if path.is_absolute() {
            let kind = shell_kind_from_name(path.file_name().and_then(|name| name.to_str()));
            return Ok(Self {
                program: raw.to_string(),
                kind,
            });
        }
        let kind = match raw {
            "sh" | "bash" | "zsh" => ShellKind::Unix,
            "pwsh" | "powershell" => ShellKind::PowerShell,
            "cmd" => ShellKind::Cmd,
            other => return Err(UserShellError::UnsupportedShell(other.to_string())),
        };
        Ok(Self {
            program: raw.to_string(),
            kind,
        })
    }

    fn command(&self, script: &str, login_shell: bool) -> Command {
        let mut cmd = Command::new(&self.program);
        match self.kind {
            ShellKind::Unix => {
                cmd.arg(if login_shell { "-lc" } else { "-c" }).arg(script);
            }
            ShellKind::PowerShell => {
                cmd.arg("-Command").arg(script);
            }
            ShellKind::Cmd => {
                cmd.arg("/C").arg(script);
            }
        }
        cmd
    }
}

fn auto_shell() -> ShellSpec {
    if cfg!(windows) {
        return ShellSpec {
            program: "pwsh".into(),
            kind: ShellKind::PowerShell,
        };
    }
    let shell = std::env::var("SHELL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "/bin/sh".into());
    let kind = shell_kind_from_name(
        PathBuf::from(&shell)
            .file_name()
            .and_then(|name| name.to_str()),
    );
    ShellSpec {
        program: shell,
        kind,
    }
}

fn shell_kind_from_name(name: Option<&str>) -> ShellKind {
    match name.map(str::to_ascii_lowercase).as_deref() {
        Some("pwsh" | "powershell" | "powershell.exe") => ShellKind::PowerShell,
        Some("cmd" | "cmd.exe") => ShellKind::Cmd,
        _ => ShellKind::Unix,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LimitedPipeOutput {
    text: String,
    truncated: bool,
}

#[derive(Debug, Default)]
struct PipeCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn read_pipe_limited(
    pipe: Option<impl AsyncRead + Unpin>,
    remaining_budget: Arc<AtomicUsize>,
    capture: Arc<Mutex<PipeCapture>>,
) -> std::io::Result<()> {
    let Some(mut pipe) = pipe else {
        return Ok(());
    };
    let mut buf = [0u8; 8192];
    loop {
        let read = pipe.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        let allowed = reserve_output_budget(&remaining_budget, read);
        let mut capture = capture
            .lock()
            .map_err(|_| std::io::Error::other("user shell pipe capture lock poisoned"))?;
        if allowed > 0 {
            capture.bytes.extend_from_slice(&buf[..allowed]);
        }
        if allowed < read {
            capture.truncated = true;
        }
    }
    Ok(())
}

fn reserve_output_budget(remaining_budget: &AtomicUsize, requested: usize) -> usize {
    let previous = remaining_budget.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
        Some(remaining.saturating_sub(requested))
    });
    match previous {
        Ok(remaining) | Err(remaining) => remaining.min(requested),
    }
}

fn snapshot_pipe_output(
    capture: &Arc<Mutex<PipeCapture>>,
) -> Result<LimitedPipeOutput, UserShellError> {
    capture
        .lock()
        .map(|capture| LimitedPipeOutput {
            text: String::from_utf8_lossy(&capture.bytes).into_owned(),
            truncated: capture.truncated,
        })
        .map_err(|_| {
            UserShellError::Io(std::io::Error::other(
                "user shell pipe capture lock poisoned",
            ))
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipeDrainOutcome {
    Drained,
    GraceExpired,
    TimedOut,
    Cancelled,
}

async fn drain_pipe_readers(
    readers: &mut JoinSet<std::io::Result<()>>,
    deadline: TokioInstant,
    drain_grace: Duration,
    cancel: &CancellationToken,
) -> Result<PipeDrainOutcome, UserShellError> {
    let drain_deadline = TokioInstant::now() + drain_grace;
    loop {
        if readers.is_empty() {
            return Ok(PipeDrainOutcome::Drained);
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(PipeDrainOutcome::Cancelled),
            _ = time::sleep_until(deadline) => return Ok(PipeDrainOutcome::TimedOut),
            _ = time::sleep_until(drain_deadline) => {
                return Ok(PipeDrainOutcome::GraceExpired);
            }
            joined = readers.join_next() => {
                if let Some(joined) = joined {
                    joined??;
                }
            }
        }
    }
}

async fn abort_pipe_readers(readers: &mut JoinSet<std::io::Result<()>>) {
    readers.abort_all();
    while readers.join_next().await.is_some() {}
}

fn prepare_process_group(command: &mut Command) {
    prepare_process_group_impl(command);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn prepare_process_group_impl(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn prepare_process_group_impl(_command: &mut Command) {}

struct ManagedChild {
    kill_guard: ProcessTreeKillGuard,
    child: Child,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    parent_pid: Option<u32>,
    parent_exit_observed: bool,
    exit_code: Option<i32>,
    reaped: bool,
}

impl ManagedChild {
    fn new(child: Child) -> Self {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let parent_pid = child.id();
        Self {
            kill_guard: ProcessTreeKillGuard::new(&child),
            child,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            parent_pid,
            parent_exit_observed: false,
            exit_code: None,
            reaped: false,
        }
    }

    async fn wait_parent(&mut self) -> std::io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if let Some(parent_pid) = self.parent_pid {
            if let Err(err) = wait_for_parent_exit_without_reaping(parent_pid).await {
                if err.raw_os_error() == Some(libc::ECHILD) {
                    // ECHILD 表示 parent 已不再可 wait；先解除裸 PGID guard，避免误杀复用 ID。
                    self.kill_guard.disarm();
                }
                return Err(err);
            }
            self.parent_exit_observed = true;
            return Ok(());
        }

        // 非 macOS/Linux 没有本模块验证过的 WNOWAIT 路径，退化为直接 wait/reap。
        let status = self.child.wait().await?;
        self.record_exit(status);
        Ok(())
    }

    fn record_exit(&mut self, status: ExitStatus) {
        self.parent_exit_observed = true;
        self.exit_code = status.code();
        self.reaped = true;
    }

    async fn reap_and_disarm(&mut self) -> std::io::Result<()> {
        if self.reaped {
            self.kill_guard.disarm();
            return Ok(());
        }
        let wait_result = self.child.wait().await;
        // child.wait 一旦返回就可能已释放 PID；必须在任何下一次 await/返回前解除 PGID guard。
        self.kill_guard.disarm();
        let status = wait_result?;
        self.record_exit(status);
        Ok(())
    }

    async fn terminate(
        &mut self,
        termination_grace: Duration,
        deadline: TokioInstant,
        cancel: &CancellationToken,
        interrupt_already_triggered: bool,
    ) -> std::io::Result<Option<UserShellCommandStatus>> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let interrupted_status = if self.kill_guard.has_process_group() {
            self.kill_guard.signal(libc::SIGTERM);
            let interrupted_status = wait_termination_grace(
                termination_grace,
                deadline,
                cancel,
                interrupt_already_triggered,
            )
            .await;
            self.kill_guard.kill_now();
            if !self.reaped {
                // process-group signal 是 best effort；再 kill leader，保证后续 wait 有界收敛。
                let _ = self.child.start_kill();
            }
            interrupted_status
        } else {
            if !self.reaped {
                self.child.start_kill()?;
            }
            None
        };

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let interrupted_status = {
            // 其他平台没有本模块验证过的稳定 process-group 身份；安全降级为 direct child。
            let _ = (
                termination_grace,
                deadline,
                cancel,
                interrupt_already_triggered,
            );
            if !self.reaped {
                self.child.start_kill()?;
            }
            None
        };

        if !self.parent_exit_observed {
            self.wait_parent().await?;
        }
        self.reap_and_disarm().await?;
        Ok(interrupted_status)
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        // 先在 leader 身份仍由 child/zombie 固定时清理组，再 disarm；随后 Child 才执行 kill_on_drop。
        self.kill_guard.kill_now();
        self.kill_guard.disarm();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
const PARENT_EXIT_OBSERVE_INTERVAL: Duration = Duration::from_millis(10);

/// 观察 parent shell 退出但保留其 waitable/zombie 身份，直到 process-group 清理完成。
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn wait_for_parent_exit_without_reaping(parent_pid: u32) -> std::io::Result<()> {
    loop {
        if parent_exit_is_waitable(parent_pid)? {
            return Ok(());
        }
        time::sleep(PARENT_EXIT_OBSERVE_INTERVAL).await;
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn parent_exit_is_waitable(parent_pid: u32) -> std::io::Result<bool> {
    let id: libc::id_t = parent_pid;
    loop {
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        // SAFETY: info 指向有效可写 siginfo_t；P_PID 只查询本进程刚 spawn 的 child。
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                id,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            // SAFETY: waitid 成功后初始化 siginfo_t；预先清零使无事件时 si_pid 为 0。
            let info = unsafe { info.assume_init() };
            // SAFETY: WEXITED 返回的 siginfo_t 使用 SIGCHLD 布局，si_pid 在两平台均有效。
            return Ok(unsafe { info.si_pid() } != 0);
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(err);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn wait_termination_grace(
    termination_grace: Duration,
    deadline: TokioInstant,
    cancel: &CancellationToken,
    interrupt_already_triggered: bool,
) -> Option<UserShellCommandStatus> {
    if interrupt_already_triggered {
        // cancel/deadline 已经成为本次返回状态，不能再让 TERM grace 延迟响应。
        return None;
    }
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Some(UserShellCommandStatus::Cancelled),
        _ = time::sleep_until(deadline) => Some(UserShellCommandStatus::TimedOut),
        _ = time::sleep(termination_grace) => None,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ProcessTreeKillGuard {
    pgid: Option<i32>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl ProcessTreeKillGuard {
    fn new(child: &Child) -> Self {
        Self {
            pgid: child.id().and_then(|pid| i32::try_from(pid).ok()),
        }
    }

    fn has_process_group(&self) -> bool {
        self.pgid.is_some()
    }

    fn signal(&self, signal: i32) {
        if let Some(pgid) = self.pgid {
            // SAFETY: pgid 来自配置为独立 process group leader 的已启动 child。
            unsafe {
                libc::kill(-pgid, signal);
            }
        }
    }

    fn kill_now(&self) {
        self.signal(libc::SIGKILL);
    }

    fn disarm(&mut self) {
        self.pgid = None;
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for ProcessTreeKillGuard {
    fn drop(&mut self) {
        self.kill_now();
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
struct ProcessTreeKillGuard;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl ProcessTreeKillGuard {
    fn new(_child: &Child) -> Self {
        Self
    }

    fn kill_now(&self) {}

    fn disarm(&mut self) {}
}

fn escape_xml_text(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn format_seconds_4(duration_ms: u128) -> String {
    let secs = duration_ms / 1000;
    let frac_10k = (duration_ms % 1000) * 10;
    format!("{secs}.{frac_10k:04}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn cfg() -> UserShellConfig {
        UserShellConfig {
            enabled: true,
            timeout_secs: 5,
            max_output_chars: 2000,
            shell: "sh".into(),
            login_shell: false,
        }
    }

    #[cfg(windows)]
    fn windows_cfg() -> UserShellConfig {
        UserShellConfig {
            enabled: true,
            timeout_secs: 5,
            max_output_chars: 2000,
            shell: "cmd".into(),
            login_shell: false,
        }
    }

    fn lifecycle_timings(
        drain_grace_ms: u64,
        termination_grace_ms: u64,
    ) -> UserShellLifecycleTimings {
        UserShellLifecycleTimings {
            drain_grace: Duration::from_millis(drain_grace_ms),
            termination_grace: Duration::from_millis(termination_grace_ms),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn shell_quote_path(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "'\"'\"'"))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn term_resistant_background_script(
        pid_file: &Path,
        ready_file: &Path,
        term_file: &Path,
    ) -> String {
        let pid_arg = shell_quote_path(pid_file);
        let ready_arg = shell_quote_path(ready_file);
        let term_arg = shell_quote_path(term_file);
        format!(
            "sh -c 'trap \"echo term > \\\"$3\\\"\" TERM; echo $$ > \"$1\"; echo ready > \"$2\"; while :; do sleep 1; done' worker {pid_arg} {ready_arg} {term_arg} & while [ ! -s {ready_arg} ]; do sleep 0.01; done; printf parent-done"
        )
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    async fn wait_for_pid_file(path: &Path) -> i32 {
        let deadline = TokioInstant::now() + Duration::from_secs(2);
        loop {
            if let Ok(raw) = tokio::fs::read_to_string(path).await {
                if let Ok(pid) = raw.trim().parse() {
                    return pid;
                }
            }
            assert!(
                TokioInstant::now() < deadline,
                "background process did not publish pid"
            );
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    async fn wait_for_marker(path: &Path, description: &str) {
        let deadline = TokioInstant::now() + Duration::from_secs(2);
        loop {
            if tokio::fs::try_exists(path).await.unwrap_or(false) {
                return;
            }
            assert!(TokioInstant::now() < deadline, "{description}");
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn process_exists(pid: i32) -> bool {
        // SAFETY: signal 0 不发送信号，只查询测试刚启动的 pid 是否仍存在。
        let result = unsafe { libc::kill(pid, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    async fn assert_process_gone(pid: i32) {
        let deadline = TokioInstant::now() + Duration::from_secs(2);
        while process_exists(pid) && TokioInstant::now() < deadline {
            time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!process_exists(pid), "background pid {pid} leaked");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_observes_parent_exit_without_reaping_leader() {
        let (observed_before_reap, still_waitable, exit_code) =
            time::timeout(Duration::from_secs(2), async {
                let mut command = Command::new("sh");
                command
                    .arg("-c")
                    .arg("exit 7")
                    .kill_on_drop(true)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                prepare_process_group(&mut command);
                let spawned = command.spawn().unwrap();
                let parent_pid = spawned.id().unwrap();
                let mut child = ManagedChild::new(spawned);

                child.wait_parent().await.unwrap();
                let observed_before_reap = child.parent_exit_observed && !child.reaped;
                let still_waitable = parent_exit_is_waitable(parent_pid).unwrap();
                child.reap_and_disarm().await.unwrap();
                (observed_before_reap, still_waitable, child.exit_code)
            })
            .await
            .expect("WNOWAIT parent observation must not hang");

        assert!(observed_before_reap);
        assert!(still_waitable);
        assert_eq!(exit_code, Some(7));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_runs_in_workspace_and_captures_output() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("note.txt"), "hello\n")
            .await
            .unwrap();

        let output = time::timeout(
            Duration::from_secs(2),
            run_user_shell_command(
                &cfg(),
                dir.path(),
                "pwd; cat note.txt; printf 'warn\\n' >&2",
                CancellationToken::new(),
            ),
        )
        .await
        .expect("foreground command test must not hang")
        .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Completed);
        assert_eq!(output.exit_code, Some(0));
        assert!(output
            .stdout
            .contains(dir.path().to_string_lossy().as_ref()));
        assert!(output.stdout.contains("hello\n"));
        assert_eq!(output.stderr, "warn\n");
        assert!(!output.truncated);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_records_non_zero_exit_code() {
        let dir = tempfile::tempdir().unwrap();
        let output = time::timeout(
            Duration::from_secs(2),
            run_user_shell_command(&cfg(), dir.path(), "exit 7", CancellationToken::new()),
        )
        .await
        .expect("non-zero command test must not hang")
        .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Completed);
        assert_eq!(output.exit_code, Some(7));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_times_out_and_truncates_output() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.timeout_secs = 1;
        cfg.max_output_chars = 5;

        let output = time::timeout(
            Duration::from_secs(3),
            run_user_shell_command(
                &cfg,
                dir.path(),
                "printf abcdef; sleep 10",
                CancellationToken::new(),
            ),
        )
        .await
        .expect("internal timeout test must not hang")
        .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::TimedOut);
        assert_eq!(output.stdout, "abcde");
        assert!(output.truncated);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_timeout_kills_process_group_and_returns_quickly() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.timeout_secs = 1;

        let result = tokio::time::timeout(
            Duration::from_secs(3),
            run_user_shell_command(
                &cfg,
                dir.path(),
                "sh -c 'sleep 10'",
                CancellationToken::new(),
            ),
        )
        .await;

        let output = result
            .expect("timeout should not hang waiting for descendant process")
            .unwrap();
        assert_eq!(output.status, UserShellCommandStatus::TimedOut);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_timeout_kills_term_ignoring_descendant_and_returns_quickly() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.timeout_secs = 1;

        let result = tokio::time::timeout(
            Duration::from_secs(4),
            run_user_shell_command(
                &cfg,
                dir.path(),
                "sh -c 'trap \"\" TERM; while true; do sleep 1; done'",
                CancellationToken::new(),
            ),
        )
        .await;

        let output = result
            .expect("timeout should kill TERM-ignoring descendants")
            .unwrap();
        assert_eq!(output.status, UserShellCommandStatus::TimedOut);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_drain_grace_cleans_background_group_and_keeps_partial_output() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let pid_arg = shell_quote_path(&pid_file);
        let ready_arg = shell_quote_path(&ready_file);
        let script = format!(
            "sh -c 'echo $$ > \"$1\"; printf background-prefix; echo ready > \"$2\"; sleep 30' worker {pid_arg} {ready_arg} & while [ ! -s {ready_arg} ]; do sleep 0.01; done; printf parent-prefix"
        );
        let cfg = cfg();

        let output = time::timeout(
            Duration::from_secs(2),
            run_user_shell_command_with_timings(
                &cfg,
                dir.path(),
                &script,
                CancellationToken::new(),
                lifecycle_timings(75, 75),
            ),
        )
        .await
        .expect("drain grace must bound inherited pipe lifetime")
        .unwrap();
        let pid = wait_for_pid_file(&pid_file).await;

        assert_eq!(output.status, UserShellCommandStatus::Completed);
        assert_eq!(output.exit_code, Some(0));
        assert!(output.stdout.contains("background-prefix"));
        assert!(output.stdout.contains("parent-prefix"));
        assert!(output.truncated);
        assert_process_gone(pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_drain_cleanup_escalates_for_term_ignoring_descendant() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let pid_arg = shell_quote_path(&pid_file);
        let ready_arg = shell_quote_path(&ready_file);
        let script = format!(
            "sh -c 'trap \"\" TERM; echo $$ > \"$1\"; echo ready > \"$2\"; while :; do sleep 1; done' worker {pid_arg} {ready_arg} & while [ ! -s {ready_arg} ]; do sleep 0.01; done; printf parent-done"
        );
        let cfg = cfg();

        let output = time::timeout(
            Duration::from_secs(2),
            run_user_shell_command_with_timings(
                &cfg,
                dir.path(),
                &script,
                CancellationToken::new(),
                lifecycle_timings(50, 100),
            ),
        )
        .await
        .expect("TERM-ignoring descendant must be escalated to KILL")
        .unwrap();
        let pid = wait_for_pid_file(&pid_file).await;

        assert_eq!(output.status, UserShellCommandStatus::Completed);
        assert!(output.truncated);
        assert_process_gone(pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_cancel_during_pipe_drain_skips_long_termination_grace() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let pid_arg = shell_quote_path(&pid_file);
        let ready_arg = shell_quote_path(&ready_file);
        let script = format!(
            "sh -c 'echo $$ > \"$1\"; echo ready > \"$2\"; sleep 30' worker {pid_arg} {ready_arg} & while [ ! -s {ready_arg} ]; do sleep 0.01; done; printf parent-done"
        );
        let cfg = cfg();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let workspace = dir.path().to_path_buf();
        let handle = tokio::spawn(async move {
            run_user_shell_command_with_timings(
                &cfg,
                &workspace,
                &script,
                task_cancel,
                lifecycle_timings(5_000, 5_000),
            )
            .await
        });
        let pid = wait_for_pid_file(&pid_file).await;
        time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();

        let output = time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("cancel during drain must return promptly")
            .unwrap()
            .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Cancelled);
        assert_eq!(output.exit_code, Some(0));
        assert!(output.truncated);
        assert_process_gone(pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_cancel_during_termination_grace_escalates_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let term_file = dir.path().join("background.term");
        let script = term_resistant_background_script(&pid_file, &ready_file, &term_file);
        let cfg = cfg();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let workspace = dir.path().to_path_buf();
        let handle = tokio::spawn(async move {
            run_user_shell_command_with_timings(
                &cfg,
                &workspace,
                &script,
                task_cancel,
                lifecycle_timings(50, 5_000),
            )
            .await
        });
        let pid = wait_for_pid_file(&pid_file).await;
        wait_for_marker(&term_file, "TERM handler did not run before cancellation").await;
        cancel.cancel();

        let output = time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("cancel during TERM grace must trigger immediate KILL")
            .unwrap()
            .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Cancelled);
        assert_eq!(output.exit_code, Some(0));
        assert!(output.truncated);
        assert_process_gone(pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_deadline_during_pipe_drain_skips_long_termination_grace() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let pid_arg = shell_quote_path(&pid_file);
        let ready_arg = shell_quote_path(&ready_file);
        let script = format!(
            "sh -c 'echo $$ > \"$1\"; echo ready > \"$2\"; sleep 30' worker {pid_arg} {ready_arg} & while [ ! -s {ready_arg} ]; do sleep 0.01; done; printf parent-done"
        );
        let mut cfg = cfg();
        cfg.timeout_secs = 1;
        let started = Instant::now();

        let output = time::timeout(
            Duration::from_secs(3),
            run_user_shell_command_with_timings(
                &cfg,
                dir.path(),
                &script,
                CancellationToken::new(),
                lifecycle_timings(5_000, 5_000),
            ),
        )
        .await
        .expect("original deadline must still bound pipe drain")
        .unwrap();
        let pid = wait_for_pid_file(&pid_file).await;

        assert_eq!(output.status, UserShellCommandStatus::TimedOut);
        assert_eq!(output.exit_code, Some(0));
        assert!(output.truncated);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_process_gone(pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_deadline_during_termination_grace_escalates_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let term_file = dir.path().join("background.term");
        let script = term_resistant_background_script(&pid_file, &ready_file, &term_file);
        let mut cfg = cfg();
        cfg.timeout_secs = 1;
        let started = Instant::now();

        let output = time::timeout(
            Duration::from_secs(3),
            run_user_shell_command_with_timings(
                &cfg,
                dir.path(),
                &script,
                CancellationToken::new(),
                lifecycle_timings(50, 5_000),
            ),
        )
        .await
        .expect("deadline during TERM grace must trigger immediate KILL")
        .unwrap();
        let pid = wait_for_pid_file(&pid_file).await;

        assert!(
            tokio::fs::try_exists(&term_file).await.unwrap(),
            "TERM grace was not entered before deadline"
        );
        assert_eq!(output.status, UserShellCommandStatus::TimedOut);
        assert_eq!(output.exit_code, Some(0));
        assert!(output.truncated);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_process_gone(pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_future_abort_kills_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let leader_file = dir.path().join("leader.pid");
        let pid_file = dir.path().join("background.pid");
        let ready_file = dir.path().join("background.ready");
        let leader_arg = shell_quote_path(&leader_file);
        let pid_arg = shell_quote_path(&pid_file);
        let ready_arg = shell_quote_path(&ready_file);
        let script = format!(
            "echo $$ > {leader_arg}; sh -c 'echo $$ > \"$1\"; echo ready > \"$2\"; sleep 30' worker {pid_arg} {ready_arg} & while [ ! -s {ready_arg} ]; do sleep 0.01; done; printf parent-done"
        );
        let mut cfg = cfg();
        cfg.timeout_secs = 30;
        let workspace = dir.path().to_path_buf();
        let handle = tokio::spawn(async move {
            run_user_shell_command_with_timings(
                &cfg,
                &workspace,
                &script,
                CancellationToken::new(),
                lifecycle_timings(5_000, 100),
            )
            .await
        });
        let leader_pid = wait_for_pid_file(&leader_file).await;
        let descendant_pid = wait_for_pid_file(&pid_file).await;
        let leader_pid_u32 = u32::try_from(leader_pid).unwrap();
        let observe_deadline = TokioInstant::now() + Duration::from_secs(2);
        while !parent_exit_is_waitable(leader_pid_u32).unwrap()
            && TokioInstant::now() < observe_deadline
        {
            time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            parent_exit_is_waitable(leader_pid_u32).unwrap(),
            "outer leader must remain waitable before abort"
        );

        handle.abort();
        let join_error = handle
            .await
            .expect_err("outer user-shell future should be aborted");

        assert!(join_error.is_cancelled());
        assert_process_gone(leader_pid).await;
        assert_process_gone(descendant_pid).await;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_large_output_is_bounded_while_reading() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.max_output_chars = 32;

        let output = time::timeout(
            Duration::from_secs(2),
            run_user_shell_command(
                &cfg,
                dir.path(),
                "printf '%*s' 10000 '' | tr ' ' x",
                CancellationToken::new(),
            ),
        )
        .await
        .expect("bounded output test must not hang")
        .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Completed);
        assert!(output.stdout.len() <= 32);
        assert!(output.truncated);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn user_shell_cancel_kills_child() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let handle = tokio::spawn(async move {
            run_user_shell_command(&cfg(), dir.path(), "sleep 10", cancel_for_task).await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let output = time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("cancelled command must not hang")
            .unwrap()
            .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Cancelled);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn user_shell_windows_direct_child_cancel_fallback_returns() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let workspace = dir.path().to_path_buf();
        let handle = tokio::spawn(async move {
            run_user_shell_command(
                &windows_cfg(),
                &workspace,
                "for /L %i in (1,1,2147483647) do @rem busy",
                task_cancel,
            )
            .await
        });
        time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();

        let output = time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("Windows direct-child cancel fallback must not hang")
            .unwrap()
            .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Cancelled);
        assert!(output.truncated);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn user_shell_windows_parent_exit_aborts_descendant_held_pipe() {
        let dir = tempfile::tempdir().unwrap();
        let holder = dir.path().join("holder.cmd");
        let release = dir.path().join("release.flag");
        let done = dir.path().join("done.flag");
        tokio::fs::write(
            &holder,
            "@echo off\r\nfor /L %%i in (1,1,5) do (\r\n  if exist release.flag goto done\r\n  ping -n 2 127.0.0.1 >NUL\r\n)\r\nexit /B 1\r\n:done\r\necho done>done.flag\r\n",
        )
        .await
        .unwrap();
        let cfg = windows_cfg();

        let output = time::timeout(
            Duration::from_secs(2),
            run_user_shell_command_with_timings(
                &cfg,
                dir.path(),
                "start \"\" /B cmd /D /S /C holder.cmd & echo parent-done",
                CancellationToken::new(),
                lifecycle_timings(50, 100),
            ),
        )
        .await
        .expect("Windows reader-abort fallback must bound descendant-held pipe")
        .unwrap();

        assert_eq!(output.status, UserShellCommandStatus::Completed);
        assert!(output.stdout.contains("parent-done"));
        assert!(output.truncated);

        tokio::fs::write(&release, b"release").await.unwrap();
        wait_for_marker(&done, "controlled Windows descendant did not exit").await;
    }

    #[test]
    fn user_shell_record_uses_user_shell_command_tags() {
        let record = format_user_shell_command_record(
            "echo hi",
            &UserShellCommandOutput {
                status: UserShellCommandStatus::Completed,
                exit_code: Some(0),
                duration_ms: 12,
                stdout: "hi\n".into(),
                stderr: String::new(),
                truncated: false,
            },
        );

        assert!(record.starts_with("<user_shell_command>"));
        assert!(record.contains("<command>\necho hi\n</command>"));
        assert!(record.contains("Exit code: 0"));
        assert!(record.ends_with("</user_shell_command>"));
    }

    #[test]
    fn user_shell_record_escapes_xml_like_content() {
        let record = format_user_shell_command_record(
            "echo </command>",
            &UserShellCommandOutput {
                status: UserShellCommandStatus::Completed,
                exit_code: Some(0),
                duration_ms: 12,
                stdout: "</result>\n".into(),
                stderr: "<user_shell_command>".into(),
                truncated: false,
            },
        );

        assert!(record.contains("echo &lt;/command&gt;"));
        assert!(record.contains("&lt;/result&gt;"));
        assert!(record.contains("&lt;user_shell_command&gt;"));
    }
}
