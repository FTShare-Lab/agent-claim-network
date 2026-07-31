//! 轻量后台 supervisor。
//!
//! v1 只承载 session finalize job：TUI enqueue 后立即退出，supervisor 串行执行
//! finalize。它是按需启动、空闲退出的普通子进程，不注册 OS service。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

use crate::agent::{SessionEngine, SessionEvent};
use crate::build_info::BuildIdentity;
use crate::claim::{AgentId, SessionId};
#[cfg(target_os = "macos")]
use crate::config::DEFAULT_SUPERVISOR_NOTIFICATION_TIMEOUT_MS;
#[cfg(any(target_os = "macos", test))]
use crate::config::SUPERVISOR_NOTIFICATION_ICON_FILE_NAME;
use crate::config::{
    default_id_mint_max_attempts, DEFAULT_SUPERVISOR_IDLE_TIMEOUT_SECS,
    DEFAULT_SUPERVISOR_IPC_TIMEOUT_MS, DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS,
    DEFAULT_SUPERVISOR_LOCK_TIMEOUT_MS, DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS,
    DEFAULT_SUPERVISOR_STOP_WAIT_TIMEOUT_MS, DEFAULT_SUPERVISOR_UPDATE_SHUTDOWN_TIMEOUT_MS,
};
#[cfg(target_os = "macos")]
use crate::storage::write_text_atomic;
use crate::storage::{mint_unique_id_in_dir, paths, read_yaml, write_yaml_atomic, FileLockGuard};

#[cfg(target_os = "macos")]
use rusttype::{point, Font, Scale};

#[derive(Debug, Clone)]
pub struct SupervisorLaunchConfig {
    pub agent_home: PathBuf,
    pub config_path: PathBuf,
    pub upstream: Option<String>,
    pub cd: Option<PathBuf>,
    pub notify_on_finalize_completion: bool,
}

impl SupervisorLaunchConfig {
    pub fn new(
        agent_home: PathBuf,
        config_path: PathBuf,
        upstream: Option<String>,
        cd: Option<PathBuf>,
        notify_on_finalize_completion: bool,
    ) -> Self {
        Self {
            agent_home,
            config_path,
            upstream,
            cd,
            notify_on_finalize_completion,
        }
    }

    fn paths(&self) -> SupervisorPaths {
        SupervisorPaths::new(&self.agent_home)
    }
}

#[derive(Debug, Clone)]
pub struct SupervisorPaths {
    supervisor_dir: PathBuf,
    jobs_dir: PathBuf,
    socket_path: PathBuf,
    pid_path: PathBuf,
    process_lock_path: PathBuf,
    log_path: PathBuf,
}

impl SupervisorPaths {
    pub fn new(agent_home: &Path) -> Self {
        let supervisor_dir = paths::agent_home_supervisor_dir(agent_home);
        Self {
            jobs_dir: paths::agent_home_supervisor_jobs_dir(agent_home),
            socket_path: supervisor_socket_path(agent_home),
            pid_path: paths::agent_home_supervisor_pid_path(agent_home),
            process_lock_path: paths::agent_home_supervisor_launch_lock_path(agent_home),
            log_path: supervisor_dir.join("supervisor.log"),
            supervisor_dir,
        }
    }

    #[cfg(any(target_os = "macos", test))]
    fn notification_icon_path(&self) -> PathBuf {
        self.supervisor_dir
            .join(SUPERVISOR_NOTIFICATION_ICON_FILE_NAME)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorStatusSnapshot {
    pub runtime_state: SupervisorRuntimeState,
    pub pid: Option<u32>,
    pub started_at: Option<DateTime<Utc>>,
    pub build: Option<BuildIdentity>,
    pub queue: SupervisorQueueSummary,
    pub current_job: Option<SupervisorJobView>,
    pub socket_path: PathBuf,
    pub pid_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorRuntimeState {
    Running,
    Stopped,
    Stuck { ipc_error: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SupervisorQueueSummary {
    pub total: usize,
    pub queued: usize,
    pub running: usize,
    pub succeeded: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorJobView {
    pub id: String,
    pub agent_id: Option<AgentId>,
    pub session_id: SessionId,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorStopReport {
    pub was_running: bool,
    pub stopped: bool,
    pub pid: Option<u32>,
}

/// supervisor shutdown 预检得到的进程状态；只有经过 IPC 与 PID 文件双重确认才算运行中。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifiedSupervisorState {
    Stopped,
    Running { pid: u32 },
}

/// shutdown 后持有 supervisor 生命周期锁，避免旧实例在 replacement 准备完成前被重新拉起。
pub struct SupervisorShutdownGuard {
    _process_lock: FileLockGuard,
}

#[derive(Clone)]
struct SupervisorSharedState {
    agent_id: AgentId,
    notify_tx: mpsc::UnboundedSender<()>,
    stop_requested: CancellationToken,
    last_activity: Arc<AtomicU64>,
    started_at: DateTime<Utc>,
    stopping: Arc<AtomicBool>,
    lifecycle_gate: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SupervisorRequest {
    Ping,
    Status,
    Stop,
    EnqueueFinalize {
        session_id: SessionId,
        #[serde(
            default = "default_notify_on_completion",
            skip_serializing_if = "is_true"
        )]
        notify_on_completion: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SupervisorResponse {
    Pong,
    Status {
        pid: u32,
        started_at: DateTime<Utc>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        build: Option<BuildIdentity>,
    },
    Enqueued {
        job_id: String,
    },
    Stopping,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SupervisorJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl SupervisorJobStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SupervisorJobKind {
    Finalize { session_id: SessionId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SupervisorJob {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<AgentId>,
    kind: SupervisorJobKind,
    status: SupervisorJobStatus,
    attempts: u32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    #[serde(
        default = "default_notify_on_completion",
        skip_serializing_if = "is_true"
    )]
    notify_on_completion: bool,
}

fn default_notify_on_completion() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

pub async fn ensure_supervisor_running(config: &SupervisorLaunchConfig) -> anyhow::Result<()> {
    ensure_supervisor_running_with(config, spawn_supervisor_process).await
}

async fn ensure_supervisor_running_with(
    config: &SupervisorLaunchConfig,
    spawn: impl Fn(&SupervisorLaunchConfig) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let paths = config.paths();
    match supervisor_build_identity(&paths).await {
        Ok(build) if supervisor_build_matches_current(build.as_ref()) => return Ok(()),
        Ok(previous_build) => {
            log::info!(
                target: "supervisor",
                "接管旧 supervisor: previous={previous_build:?}, current={:?}",
                BuildIdentity::current()
            );
            let expected = preflight_supervisor_shutdown(&config.agent_home).await?;
            let guard = shutdown_verified_supervisor(&config.agent_home, expected).await?;
            spawn(config)?;
            drop(guard);
            return wait_for_current_supervisor(&paths).await;
        }
        Err(_) => {}
    }

    {
        let _guard = FileLockGuard::lock_exclusive_timeout(
            &paths.process_lock_path,
            Duration::from_millis(DEFAULT_SUPERVISOR_LOCK_TIMEOUT_MS),
        )
        .await?;
        match supervisor_build_identity(&paths).await {
            Ok(build) if supervisor_build_matches_current(build.as_ref()) => return Ok(()),
            Ok(build) => {
                anyhow::bail!(
                    "supervisor 在未持有生命周期锁时响应了 IPC，拒绝覆盖: build={build:?}"
                );
            }
            Err(_) => {}
        }
        remove_stale_socket(&paths.socket_path).await;
        spawn(config)?;
    }
    wait_for_current_supervisor(&paths).await
}

pub async fn enqueue_finalize(
    config: &SupervisorLaunchConfig,
    session_id: SessionId,
) -> anyhow::Result<String> {
    ensure_supervisor_running(config).await?;
    match send_request(
        &config.paths(),
        SupervisorRequest::EnqueueFinalize {
            session_id,
            notify_on_completion: config.notify_on_finalize_completion,
        },
    )
    .await?
    {
        SupervisorResponse::Enqueued { job_id } => Ok(job_id),
        SupervisorResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("supervisor 返回了非 enqueue 响应: {other:?}"),
    }
}

pub async fn supervisor_status(agent_home: &Path) -> anyhow::Result<SupervisorStatusSnapshot> {
    let paths = SupervisorPaths::new(agent_home);
    let jobs = read_jobs(&paths).await?;
    let queue = SupervisorQueueSummary::from_jobs(&jobs);
    let current_job = jobs
        .iter()
        .filter(|job| job.status == SupervisorJobStatus::Running)
        .min_by(|a, b| {
            a.started_at
                .cmp(&b.started_at)
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.id.cmp(&b.id))
        })
        .map(job_to_view);

    let (runtime_state, pid, started_at, build) = match send_request(
        &paths,
        SupervisorRequest::Status,
    )
    .await
    {
        Ok(SupervisorResponse::Status {
            pid,
            started_at,
            build,
        }) => (
            SupervisorRuntimeState::Running,
            Some(pid),
            Some(started_at),
            build,
        ),
        Ok(SupervisorResponse::Error { message }) => anyhow::bail!(message),
        Ok(other) => anyhow::bail!("supervisor 返回了非 status 响应: {other:?}"),
        Err(err) if ipc_error_indicates_unavailable(&err) => (
            SupervisorRuntimeState::Stopped,
            read_pid_file(&paths).await?,
            None,
            None,
        ),
        Err(err) => {
            let pid = read_pid_file(&paths).await?;
            let process_probe = probe_process(pid);
            if process_probe == ProcessProbe::Alive {
                (
                    SupervisorRuntimeState::Stuck {
                        ipc_error: format!("{err:#}"),
                    },
                    pid,
                    None,
                    None,
                )
            } else {
                return Err(err.context(format!(
                        "查询 supervisor status 失败: socket={}, pid_file={}, pid={pid:?}, process_probe={process_probe:?}",
                        paths.socket_path.display(),
                        paths.pid_path.display()
                    )));
            }
        }
    };

    Ok(SupervisorStatusSnapshot {
        runtime_state,
        pid,
        started_at,
        build,
        queue,
        current_job,
        socket_path: paths.socket_path,
        pid_path: paths.pid_path,
    })
}

pub async fn supervisor_jobs(agent_home: &Path) -> anyhow::Result<Vec<SupervisorJobView>> {
    let paths = SupervisorPaths::new(agent_home);
    let mut jobs = read_jobs(&paths).await?;
    sort_jobs_by_created_at(&mut jobs);
    Ok(jobs.iter().map(job_to_view).collect())
}

/// 为强制替换预检 supervisor；无法通过专属 IPC 确认身份的存活 PID 会阻止终止。
pub async fn preflight_supervisor_shutdown(
    agent_home: &Path,
) -> anyhow::Result<VerifiedSupervisorState> {
    let paths = SupervisorPaths::new(agent_home);
    let pid_file = read_pid_file(&paths).await?;
    match send_request(&paths, SupervisorRequest::Status).await {
        Ok(SupervisorResponse::Status { pid, .. }) => {
            match pid_file {
                Some(stored_pid) if stored_pid == pid => {}
                Some(stored_pid) => anyhow::bail!(
                    "supervisor IPC PID {pid} 与 PID 文件 {stored_pid} 不一致 (pid_file={})",
                    paths.pid_path.display()
                ),
                None => anyhow::bail!(
                    "supervisor IPC 返回 PID {pid}，但 PID 文件不存在 ({})",
                    paths.pid_path.display()
                ),
            }
            match probe_process(Some(pid)) {
                ProcessProbe::Alive => Ok(VerifiedSupervisorState::Running { pid }),
                ProcessProbe::NotRunning => anyhow::bail!(
                    "supervisor IPC 返回 PID {pid}，但该进程已不存在 (socket={})",
                    paths.socket_path.display()
                ),
                ProcessProbe::InvalidPid => {
                    anyhow::bail!("supervisor IPC 返回非法 PID {pid}")
                }
            }
        }
        Ok(SupervisorResponse::Error { message }) => {
            classify_unconfirmed_supervisor(&paths, pid_file, &message)
        }
        Ok(other) => classify_unconfirmed_supervisor(
            &paths,
            pid_file,
            &format!("supervisor 返回了非 status 响应: {other:?}"),
        ),
        Err(err) => classify_unconfirmed_supervisor(&paths, pid_file, &format!("{err:#}")),
    }
}

/// 终止已预检的 supervisor，并持有生命周期锁直到调用方完成替换准备。
pub async fn shutdown_verified_supervisor(
    agent_home: &Path,
    expected: VerifiedSupervisorState,
) -> anyhow::Result<SupervisorShutdownGuard> {
    let paths = SupervisorPaths::new(agent_home);
    let current = preflight_supervisor_shutdown(agent_home).await?;
    match (expected, current) {
        (
            VerifiedSupervisorState::Running { pid: expected_pid },
            VerifiedSupervisorState::Running { pid: current_pid },
        ) if expected_pid == current_pid => {
            kill_supervisor_process(current_pid)?;
        }
        (VerifiedSupervisorState::Running { .. }, VerifiedSupervisorState::Stopped) => {
            // 预检后自然退出等价于已完成 shutdown，不需要再发信号。
        }
        (VerifiedSupervisorState::Stopped, VerifiedSupervisorState::Stopped) => {}
        (expected, current) => anyhow::bail!(
            "supervisor 状态在 shutdown 预检后发生变化: expected={expected:?}, current={current:?}"
        ),
    }

    // 被 SIGKILL 的 supervisor 可能暂时以 zombie PID 可见；生命周期锁释放才表示它已不再执行。
    let process_lock = FileLockGuard::lock_exclusive_timeout(
        &paths.process_lock_path,
        Duration::from_millis(DEFAULT_SUPERVISOR_UPDATE_SHUTDOWN_TIMEOUT_MS),
    )
    .await
    .context("等待 supervisor 进程锁释放失败")?;
    cleanup_runtime_files(&paths).await;
    Ok(SupervisorShutdownGuard {
        _process_lock: process_lock,
    })
}

fn classify_unconfirmed_supervisor(
    paths: &SupervisorPaths,
    pid_file: Option<u32>,
    ipc_error: &str,
) -> anyhow::Result<VerifiedSupervisorState> {
    match probe_process(pid_file) {
        ProcessProbe::NotRunning => Ok(VerifiedSupervisorState::Stopped),
        ProcessProbe::Alive => anyhow::bail!(
            "supervisor PID {} 仍存活，但无法通过 IPC 确认身份 (socket={}, error={ipc_error})",
            pid_file.unwrap_or_default(),
            paths.socket_path.display()
        ),
        ProcessProbe::InvalidPid => anyhow::bail!(
            "supervisor PID 文件包含非法 PID {:?} ({})",
            pid_file,
            paths.pid_path.display()
        ),
    }
}

fn kill_supervisor_process(pid: u32) -> anyhow::Result<()> {
    let raw_pid = libc::pid_t::try_from(pid).context("supervisor PID 超出平台范围")?;
    if raw_pid <= 0 {
        anyhow::bail!("拒绝终止非法 supervisor PID {pid}");
    }
    // SAFETY: PID 已经通过 supervisor 专属 IPC、PID 文件和正数范围三重校验。
    let result = unsafe { libc::kill(raw_pid, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error).with_context(|| format!("终止 supervisor PID {pid} 失败"))
}

pub async fn stop_supervisor(agent_home: &Path) -> anyhow::Result<SupervisorStopReport> {
    let paths = SupervisorPaths::new(agent_home);
    let pid = read_pid_file(&paths).await?;
    match send_request(&paths, SupervisorRequest::Stop).await {
        Ok(SupervisorResponse::Stopping) => {
            let stopped = wait_for_supervisor_shutdown(&paths).await;
            Ok(SupervisorStopReport {
                was_running: true,
                stopped,
                pid,
            })
        }
        Ok(SupervisorResponse::Error { message }) => anyhow::bail!(message),
        Ok(other) => anyhow::bail!("supervisor 返回了非 stop 响应: {other:?}"),
        Err(err) if ipc_error_indicates_unavailable(&err) => Ok(SupervisorStopReport {
            was_running: false,
            stopped: true,
            pid,
        }),
        Err(err) => Err(err.context("请求 supervisor stop 失败")),
    }
}

pub async fn run_supervisor(engine: SessionEngine, agent_home: PathBuf) -> anyhow::Result<()> {
    let paths = SupervisorPaths::new(&agent_home);
    let agent_id = engine.agent_id().clone();
    let started_at = Utc::now();
    fs::create_dir_all(&paths.supervisor_dir).await?;
    fs::create_dir_all(&paths.jobs_dir).await?;
    if ping(&paths).await.is_ok() {
        anyhow::bail!("supervisor 已在运行");
    }
    let _process_guard = FileLockGuard::lock_exclusive_timeout(
        &paths.process_lock_path,
        Duration::from_millis(DEFAULT_SUPERVISOR_LOCK_TIMEOUT_MS),
    )
    .await
    .context("获取 supervisor 进程锁失败")?;
    remove_stale_socket(&paths.socket_path).await;

    let listener = UnixListener::bind(&paths.socket_path)
        .with_context(|| format!("绑定 supervisor UDS: {}", paths.socket_path.display()))?;
    write_pid_file(&paths).await?;
    reset_stale_running_jobs(&paths).await?;
    append_supervisor_log(&paths, "supervisor started").await;

    let (notify_tx, notify_rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let accept_cancel = CancellationToken::new();
    let last_activity = Arc::new(AtomicU64::new(now_millis()));
    let running_job = Arc::new(AtomicBool::new(false));
    let stopping = Arc::new(AtomicBool::new(false));
    let lifecycle_gate = Arc::new(Mutex::new(()));
    let shared_state = SupervisorSharedState {
        agent_id,
        notify_tx,
        stop_requested: cancel.clone(),
        last_activity: last_activity.clone(),
        started_at,
        stopping: stopping.clone(),
        lifecycle_gate,
    };

    let accept_handle = tokio::spawn(accept_loop(
        listener,
        paths.clone(),
        accept_cancel.clone(),
        shared_state.clone(),
    ));
    let worker_handle = tokio::spawn(worker_loop(
        engine,
        paths.clone(),
        notify_rx,
        running_job.clone(),
        shared_state.clone(),
    ));

    let idle_timeout = Duration::from_secs(DEFAULT_SUPERVISOR_IDLE_TIMEOUT_SECS);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = sleep(Duration::from_secs(5)) => {}
        }
        if running_job.load(Ordering::Relaxed) {
            continue;
        }
        if has_queued_jobs(&paths).await.unwrap_or(true) {
            continue;
        }
        let elapsed = now_millis().saturating_sub(last_activity.load(Ordering::Relaxed));
        if elapsed >= millis_u64(idle_timeout) {
            append_supervisor_log(&paths, "supervisor idle timeout reached").await;
            stopping.store(true, Ordering::Release);
            cancel.cancel();
            break;
        }
    }

    if let Err(err) = worker_handle.await {
        log::warn!(target: "supervisor", "worker loop join failed: {err}");
    }
    accept_cancel.cancel();
    if let Err(err) = accept_handle.await {
        log::warn!(target: "supervisor", "accept loop join failed: {err}");
    }
    cleanup_runtime_files(&paths).await;
    append_supervisor_log(&paths, "supervisor stopped").await;
    Ok(())
}

async fn accept_loop(
    listener: UnixListener,
    paths: SupervisorPaths,
    accept_cancel: CancellationToken,
    shared: SupervisorSharedState,
) {
    loop {
        tokio::select! {
            _ = accept_cancel.cancelled() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        shared.last_activity.store(now_millis(), Ordering::Relaxed);
                        let paths = paths.clone();
                        let shared = shared.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_client(stream, &paths, &shared).await {
                                append_supervisor_log(&paths, format!("client error: {err:#}")).await;
                            }
                        });
                    }
                    Err(err) => {
                        append_supervisor_log(&paths, format!("accept error: {err}")).await;
                        break;
                    }
                }
            }
        }
    }
}

async fn handle_client(
    stream: UnixStream,
    paths: &SupervisorPaths,
    shared: &SupervisorSharedState,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    let mut should_stop = false;
    let response = match lines.next_line().await? {
        Some(line) => match serde_json::from_str::<SupervisorRequest>(&line) {
            Ok(SupervisorRequest::Ping) => SupervisorResponse::Pong,
            Ok(SupervisorRequest::Status) => SupervisorResponse::Status {
                pid: std::process::id(),
                started_at: shared.started_at,
                build: Some(BuildIdentity::current()),
            },
            Ok(SupervisorRequest::Stop) => {
                let _guard = shared.lifecycle_gate.lock().await;
                shared.stopping.store(true, Ordering::Release);
                shared.stop_requested.cancel();
                should_stop = true;
                SupervisorResponse::Stopping
            }
            Ok(SupervisorRequest::EnqueueFinalize {
                session_id,
                notify_on_completion,
            }) => {
                let _guard = shared.lifecycle_gate.lock().await;
                if shared.stopping.load(Ordering::Acquire) || shared.stop_requested.is_cancelled() {
                    SupervisorResponse::Error {
                        message: "supervisor 正在停止，拒绝新 finalize job".into(),
                    }
                } else {
                    let job = create_finalize_job(
                        paths,
                        &shared.agent_id,
                        session_id,
                        notify_on_completion,
                    )
                    .await?;
                    let _ = shared.notify_tx.send(());
                    SupervisorResponse::Enqueued { job_id: job.id }
                }
            }
            Err(err) => SupervisorResponse::Error {
                message: format!("invalid supervisor request: {err}"),
            },
        },
        None => SupervisorResponse::Error {
            message: "empty supervisor request".into(),
        },
    };
    let mut line = serde_json::to_vec(&response)?;
    line.push(b'\n');
    let write_result = write_half.write_all(&line).await;
    if should_stop {
        append_supervisor_log(paths, "supervisor stop requested").await;
    }
    write_result?;
    Ok(())
}

async fn worker_loop(
    engine: SessionEngine,
    paths: SupervisorPaths,
    mut notify_rx: mpsc::UnboundedReceiver<()>,
    running_job: Arc<AtomicBool>,
    shared: SupervisorSharedState,
) {
    loop {
        tokio::select! {
            _ = shared.stop_requested.cancelled() => break,
            _ = notify_rx.recv() => {}
            _ = sleep(Duration::from_secs(1)) => {}
        }
        if shared.stop_requested.is_cancelled() || shared.stopping.load(Ordering::Acquire) {
            break;
        }
        loop {
            if shared.stop_requested.is_cancelled() || shared.stopping.load(Ordering::Acquire) {
                break;
            }
            let job = match next_queued_job(&paths).await {
                Ok(Some(job)) => job,
                Ok(None) => break,
                Err(err) => {
                    append_supervisor_log(&paths, format!("queue scan error: {err:#}")).await;
                    break;
                }
            };
            let _guard = shared.lifecycle_gate.lock().await;
            if shared.stop_requested.is_cancelled() || shared.stopping.load(Ordering::Acquire) {
                break;
            }
            running_job.store(true, Ordering::Relaxed);
            drop(_guard);
            shared.last_activity.store(now_millis(), Ordering::Relaxed);
            let requeued = if let Err(err) = run_job(&engine, &paths, job).await {
                append_supervisor_log(&paths, format!("job runner error: {err:#}")).await;
                false
            } else {
                has_queued_jobs(&paths).await.unwrap_or(false)
            };
            running_job.store(false, Ordering::Relaxed);
            shared.last_activity.store(now_millis(), Ordering::Relaxed);
            if requeued {
                tokio::select! {
                    _ = shared.stop_requested.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

async fn run_job(
    engine: &SessionEngine,
    paths: &SupervisorPaths,
    mut job: SupervisorJob,
) -> anyhow::Result<()> {
    job.status = SupervisorJobStatus::Running;
    job.attempts = job.attempts.saturating_add(1);
    job.started_at = Some(Utc::now());
    job.updated_at = Utc::now();
    job.last_error = None;
    write_job(paths, &job).await?;

    let result = match &job.kind {
        SupervisorJobKind::Finalize { session_id } => {
            append_supervisor_log(paths, format!("finalize job {} started", job.id)).await;
            engine
                .finalize_existing_session(session_id, |event| {
                    log_supervisor_session_event(&job.id, &event);
                })
                .await
        }
    };

    match result {
        Ok(report) => {
            job.status = SupervisorJobStatus::Succeeded;
            job.finished_at = Some(Utc::now());
            job.updated_at = Utc::now();
            write_job(paths, &job).await?;
            if job.notify_on_completion && finalize_report_should_notify_success(&report) {
                notify_finalize_success(paths, &job, &report).await;
            }
            append_supervisor_log(
                paths,
                format!(
                    "finalize job {} succeeded trace={} claims={} disputes={}",
                    job.id,
                    report
                        .trace_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "None".into()),
                    report.new_claim_ids.len(),
                    report.new_dispute_ids.len()
                ),
            )
            .await;
        }
        Err(err) => {
            let message = err.to_string();
            if job.attempts < DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS {
                job.status = SupervisorJobStatus::Queued;
                job.finished_at = None;
            } else {
                job.status = SupervisorJobStatus::Failed;
                job.finished_at = Some(Utc::now());
            }
            job.updated_at = Utc::now();
            job.last_error = Some(message.clone());
            write_job(paths, &job).await?;
            if job.status == SupervisorJobStatus::Failed {
                if job.notify_on_completion {
                    notify_finalize_failure(paths, &job, &message).await;
                }
                append_supervisor_log(paths, format!("finalize job {} failed: {message}", job.id))
                    .await;
            } else {
                append_supervisor_log(
                    paths,
                    format!(
                        "finalize job {} failed attempt {}/{} and was requeued: {message}",
                        job.id, job.attempts, DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS
                    ),
                )
                .await;
            }
        }
    }
    Ok(())
}

async fn create_finalize_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    notify_on_completion: bool,
) -> anyhow::Result<SupervisorJob> {
    create_finalize_job_with_id_factory(
        paths,
        agent_id,
        session_id,
        notify_on_completion,
        next_job_id,
        default_id_mint_max_attempts(),
    )
    .await
}

/// 原子申领 job ID 后写入初始记录，避免首次持久化覆盖同 ID 的既有 job。
async fn create_finalize_job_with_id_factory<F>(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    notify_on_completion: bool,
    id_factory: F,
    max_id_attempts: usize,
) -> anyhow::Result<SupervisorJob>
where
    F: FnMut() -> String,
{
    fs::create_dir_all(&paths.jobs_dir).await?;
    // `mint_unique_id_in_dir` 以 create_new 原子落下 0 字节占位；只有拿到该
    // 占位的调用方才能随后用原子写替换它，碰撞则重抽而不会覆写已有 job。
    let id = mint_unique_id_in_dir(&paths.jobs_dir, id_factory, max_id_attempts)
        .await
        .context("原子申领 supervisor job id 失败")?;
    let now = Utc::now();
    let job = SupervisorJob {
        id,
        agent_id: Some(agent_id.clone()),
        kind: SupervisorJobKind::Finalize { session_id },
        status: SupervisorJobStatus::Queued,
        attempts: 0,
        created_at: now,
        updated_at: now,
        started_at: None,
        finished_at: None,
        last_error: None,
        notify_on_completion,
    };
    write_reserved_job(paths, &job).await?;
    Ok(job)
}

async fn next_queued_job(paths: &SupervisorPaths) -> anyhow::Result<Option<SupervisorJob>> {
    let mut jobs = read_jobs(paths).await?;
    jobs.retain(|job| job.status == SupervisorJobStatus::Queued);
    jobs.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(jobs.into_iter().next())
}

async fn has_queued_jobs(paths: &SupervisorPaths) -> anyhow::Result<bool> {
    Ok(next_queued_job(paths).await?.is_some())
}

async fn reset_stale_running_jobs(paths: &SupervisorPaths) -> anyhow::Result<()> {
    let mut jobs = read_jobs(paths).await?;
    for job in &mut jobs {
        if job.status == SupervisorJobStatus::Running {
            job.status = SupervisorJobStatus::Queued;
            job.updated_at = Utc::now();
            job.last_error = Some("recovered stale running job after supervisor start".into());
            write_job(paths, job).await?;
        }
    }
    Ok(())
}

async fn read_jobs(paths: &SupervisorPaths) -> anyhow::Result<Vec<SupervisorJob>> {
    let mut entries = match fs::read_dir(&paths.jobs_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut jobs = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
            continue;
        }
        match read_yaml::<SupervisorJob>(&path).await {
            Ok(job) => {
                let stem = path.file_stem().and_then(|stem| stem.to_str());
                if stem != Some(job.id.as_str()) {
                    let message = format!(
                        "skip supervisor job {}: filename stem {:?} does not match payload id {}",
                        path.display(),
                        stem,
                        job.id
                    );
                    log::warn!(target: "supervisor", "{message}");
                    append_supervisor_log(paths, message).await;
                    continue;
                }
                jobs.push(job);
            }
            Err(err) => {
                let message = format!("skip malformed supervisor job {}: {err:#}", path.display());
                log::warn!(target: "supervisor", "{message}");
                append_supervisor_log(paths, message).await;
            }
        }
    }
    Ok(jobs)
}

impl SupervisorQueueSummary {
    fn from_jobs(jobs: &[SupervisorJob]) -> Self {
        let mut summary = Self {
            total: jobs.len(),
            ..Self::default()
        };
        for job in jobs {
            match job.status {
                SupervisorJobStatus::Queued => summary.queued += 1,
                SupervisorJobStatus::Running => summary.running += 1,
                SupervisorJobStatus::Succeeded => summary.succeeded += 1,
                SupervisorJobStatus::Failed => summary.failed += 1,
            }
        }
        summary
    }
}

fn sort_jobs_by_created_at(jobs: &mut [SupervisorJob]) {
    jobs.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn job_to_view(job: &SupervisorJob) -> SupervisorJobView {
    let session_id = match &job.kind {
        SupervisorJobKind::Finalize { session_id } => session_id.clone(),
    };
    SupervisorJobView {
        id: job.id.clone(),
        agent_id: job.agent_id.clone(),
        session_id,
        status: job.status.as_str().to_string(),
        created_at: job.created_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        attempts: job.attempts,
        last_error: job.last_error.clone(),
    }
}

async fn write_job(paths: &SupervisorPaths, job: &SupervisorJob) -> anyhow::Result<()> {
    fs::create_dir_all(&paths.jobs_dir).await?;
    let path = job_path(paths, &job.id);
    let stored = read_yaml::<SupervisorJob>(&path)
        .await
        .with_context(|| format!("读取既有 supervisor job 失败: {}", path.display()))?;
    if stored.id != job.id {
        anyhow::bail!(
            "拒绝覆写 supervisor job {}：文件 payload id 为 {}",
            path.display(),
            stored.id
        );
    }
    write_yaml_atomic(&path, job).await?;
    Ok(())
}

/// 用已原子申领的 0 字节占位发布新 job。
async fn write_reserved_job(paths: &SupervisorPaths, job: &SupervisorJob) -> anyhow::Result<()> {
    let path = job_path(paths, &job.id);
    let metadata = fs::metadata(&path)
        .await
        .with_context(|| format!("读取 supervisor job 占位失败: {}", path.display()))?;
    if metadata.len() != 0 {
        anyhow::bail!(
            "拒绝发布 supervisor job {}：原子申领的占位已不再为空文件",
            path.display()
        );
    }
    write_yaml_atomic(&path, job).await?;
    Ok(())
}

fn job_path(paths: &SupervisorPaths, job_id: &str) -> PathBuf {
    paths.jobs_dir.join(format!("{job_id}.yaml"))
}

async fn ping(paths: &SupervisorPaths) -> anyhow::Result<()> {
    match send_request(paths, SupervisorRequest::Ping).await? {
        SupervisorResponse::Pong => Ok(()),
        other => anyhow::bail!("unexpected supervisor ping response: {other:?}"),
    }
}

async fn read_pid_file(paths: &SupervisorPaths) -> anyhow::Result<Option<u32>> {
    let raw = match fs::read_to_string(&paths.pid_path).await {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let pid = trimmed
        .parse::<u32>()
        .with_context(|| format!("解析 supervisor pid 文件失败: {}", paths.pid_path.display()))?;
    Ok(Some(pid))
}

async fn wait_for_supervisor_shutdown(paths: &SupervisorPaths) -> bool {
    let timeout = Duration::from_millis(DEFAULT_SUPERVISOR_STOP_WAIT_TIMEOUT_MS);
    let deadline = Instant::now() + timeout;
    loop {
        if !path_exists(&paths.socket_path).await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn path_exists(path: &Path) -> bool {
    match fs::metadata(path).await {
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

async fn send_request(
    paths: &SupervisorPaths,
    request: SupervisorRequest,
) -> anyhow::Result<SupervisorResponse> {
    tokio::time::timeout(
        Duration::from_millis(DEFAULT_SUPERVISOR_IPC_TIMEOUT_MS),
        send_request_inner(paths, request),
    )
    .await
    .context("supervisor IPC 超时")?
}

async fn send_request_inner(
    paths: &SupervisorPaths,
    request: SupervisorRequest,
) -> anyhow::Result<SupervisorResponse> {
    let stream = UnixStream::connect(&paths.socket_path)
        .await
        .with_context(|| {
            format!(
                "连接 supervisor socket 失败: {}",
                paths.socket_path.display()
            )
        })?;
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_vec(&request)?;
    line.push(b'\n');
    write_half.write_all(&line).await?;
    let mut lines = BufReader::new(read_half).lines();
    let line = lines
        .next_line()
        .await?
        .context("supervisor 关闭连接且未返回响应")?;
    Ok(serde_json::from_str(&line)?)
}

fn ipc_error_indicates_unavailable(err: &anyhow::Error) -> bool {
    err.chain().any(|source| {
        source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                matches!(
                    io_err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                )
            })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessProbe {
    Alive,
    NotRunning,
    InvalidPid,
}

fn probe_process(pid: Option<u32>) -> ProcessProbe {
    let Some(pid) = pid else {
        return ProcessProbe::NotRunning;
    };
    let Ok(raw_pid) = libc::pid_t::try_from(pid) else {
        return ProcessProbe::InvalidPid;
    };
    if raw_pid <= 0 {
        return ProcessProbe::InvalidPid;
    }

    // SAFETY: kill(pid, 0) 不发送信号，只做进程存在性与权限检查；pid 已转成正数 pid_t。
    let result = unsafe { libc::kill(raw_pid, 0) };
    process_probe_from_kill_result(result, std::io::Error::last_os_error().raw_os_error())
}

fn process_probe_from_kill_result(result: i32, raw_os_error: Option<i32>) -> ProcessProbe {
    if result == 0 {
        return ProcessProbe::Alive;
    }
    match raw_os_error {
        Some(code) if code == libc::EPERM => ProcessProbe::Alive,
        Some(code) if code == libc::ESRCH => ProcessProbe::NotRunning,
        Some(code) if code == libc::EINVAL => ProcessProbe::InvalidPid,
        _ => ProcessProbe::NotRunning,
    }
}

fn spawn_supervisor_process(config: &SupervisorLaunchConfig) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("定位当前 acn 可执行文件失败")?;
    let mut command = std::process::Command::new(exe);
    command
        .arg("supervisor")
        .arg("run")
        .arg("--config")
        .arg(&config.config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(upstream) = &config.upstream {
        command.arg("--upstream").arg(upstream);
    }
    if let Some(cd) = &config.cd {
        command.arg("--cd").arg(cd);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command.spawn().context("启动 acn supervisor 失败")?;
    Ok(())
}

async fn wait_for_current_supervisor(paths: &SupervisorPaths) -> anyhow::Result<()> {
    let timeout = Duration::from_millis(DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS);
    let deadline = Instant::now() + timeout;
    loop {
        if supervisor_build_identity(paths)
            .await
            .is_ok_and(|build| build.is_some_and(|build| build.matches_current()))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(supervisor_startup_timeout_error(timeout));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn supervisor_startup_timeout_error(timeout: Duration) -> anyhow::Error {
    anyhow::anyhow!("supervisor 在 {} 秒内未就绪", timeout.as_secs())
}

async fn supervisor_build_identity(
    paths: &SupervisorPaths,
) -> anyhow::Result<Option<BuildIdentity>> {
    match send_request(paths, SupervisorRequest::Status).await? {
        SupervisorResponse::Status { build, .. } => Ok(build),
        other => anyhow::bail!("unexpected supervisor status response: {other:?}"),
    }
}

fn supervisor_build_matches_current(build: Option<&BuildIdentity>) -> bool {
    build.is_some_and(BuildIdentity::matches_current)
}

fn next_job_id() -> String {
    let mut salt = [0_u8; 4];
    rand::thread_rng().fill_bytes(&mut salt);
    format!(
        "job_{}_{:08x}",
        Utc::now().timestamp_millis(),
        u32::from_be_bytes(salt)
    )
}

fn supervisor_socket_path(agent_home: &Path) -> PathBuf {
    let hash = stable_path_hash(&agent_home.display().to_string());
    std::env::temp_dir().join(format!("acn-supervisor-{hash:016x}.sock"))
}

async fn write_pid_file(paths: &SupervisorPaths) -> anyhow::Result<()> {
    fs::create_dir_all(&paths.supervisor_dir).await?;
    fs::write(&paths.pid_path, std::process::id().to_string()).await?;
    Ok(())
}

async fn remove_stale_socket(path: &Path) {
    match fs::remove_file(path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            log::warn!(
                target: "supervisor",
                "清理 stale supervisor socket 失败 ({}): {err}",
                path.display()
            );
        }
    }
}

async fn cleanup_runtime_files(paths: &SupervisorPaths) {
    remove_stale_socket(&paths.socket_path).await;
    match fs::remove_file(&paths.pid_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => log::warn!(
            target: "supervisor",
            "清理 supervisor pid 失败 ({}): {err}",
            paths.pid_path.display()
        ),
    }
}

async fn append_supervisor_log(paths: &SupervisorPaths, message: impl AsRef<str>) {
    let message = message.as_ref();
    if let Some(parent) = paths.log_path.parent() {
        if let Err(err) = fs::create_dir_all(parent).await {
            log::warn!(target: "supervisor", "创建 supervisor log 目录失败: {err}");
            return;
        }
    }
    let line = format!("{} {message}\n", Utc::now().to_rfc3339());
    let result = async {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&paths.log_path)
            .await?;
        file.write_all(line.as_bytes()).await
    }
    .await;
    if let Err(err) = result {
        log::warn!(
            target: "supervisor",
            "写 supervisor log 失败 ({}): {err}",
            paths.log_path.display()
        );
    }
}

fn log_supervisor_session_event(job_id: &str, event: &SessionEvent) {
    match event {
        SessionEvent::FinalizeStarted => {
            log::info!(target: "supervisor", "job {job_id} finalize started");
        }
        SessionEvent::FinalizeCompleted {
            trace_id,
            new_claim_ids,
            updated_claim_ids,
            new_dispute_ids,
        } => {
            log::info!(
                target: "supervisor",
                "job {job_id} finalize completed trace={} new_claims={} updated_claims={} disputes={}",
                trace_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "None".into()),
                new_claim_ids.len(),
                updated_claim_ids.len(),
                new_dispute_ids.len()
            );
        }
        SessionEvent::FinalizeFailed { error } => {
            log::warn!(target: "supervisor", "job {job_id} finalize failed: {error}");
        }
        _ => {}
    }
}

async fn notify_finalize_success(
    paths: &SupervisorPaths,
    job: &SupervisorJob,
    report: &crate::agent::SessionFinalizeReport,
) {
    let session_id = job_session_id(job);
    let trace = report
        .trace_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "None".into());
    let body = format!(
        "{session_id} finalized: trace={trace}, new_claims={}, new_disputes={}",
        report.new_claim_ids.len(),
        report.new_dispute_ids.len()
    );
    notify_macos(paths, "ACN finalize completed", &body).await;
}

fn finalize_report_should_notify_success(report: &crate::agent::SessionFinalizeReport) -> bool {
    report.finalized_unrecapped_messages
}

async fn notify_finalize_failure(paths: &SupervisorPaths, job: &SupervisorJob, error: &str) {
    let session_id = job_session_id(job);
    let body = format!(
        "{session_id} finalize failed: {}",
        truncate_notification(error)
    );
    notify_macos(paths, "ACN finalize failed", &body).await;
}

async fn notify_macos(paths: &SupervisorPaths, title: &str, body: &str) {
    #[cfg(not(target_os = "macos"))]
    let _ = (paths, title, body);

    #[cfg(target_os = "macos")]
    notify_macos_inner(paths, title, body).await;
}

#[cfg(target_os = "macos")]
async fn notify_macos_inner(paths: &SupervisorPaths, title: &str, body: &str) {
    if send_macos_notification(paths, title, body).await {
        return;
    }
    notify_macos_with_osascript(title, body).await;
}

#[cfg(target_os = "macos")]
async fn send_macos_notification(paths: &SupervisorPaths, title: &str, body: &str) -> bool {
    let icon_path = ensure_notification_icon(paths).await;
    let send_title = title.to_string();
    let send_body = body.to_string();
    let send_icon_path = icon_path;
    let send = tokio::task::spawn_blocking(move || {
        send_macos_notification_blocking(send_title, send_body, send_icon_path)
    });
    match tokio::time::timeout(
        Duration::from_millis(DEFAULT_SUPERVISOR_NOTIFICATION_TIMEOUT_MS),
        send,
    )
    .await
    {
        Ok(Ok(Ok(()))) => true,
        Ok(Ok(Err(err))) => {
            log::warn!(target: "supervisor", "发送 macOS 图标通知失败，将退回 osascript: {err:#}");
            false
        }
        Ok(Err(err)) => {
            log::warn!(target: "supervisor", "发送 macOS 图标通知任务异常，将退回 osascript: {err}");
            false
        }
        Err(_) => {
            log::warn!(target: "supervisor", "发送 macOS 图标通知超时，将退回 osascript");
            false
        }
    }
}

#[cfg(target_os = "macos")]
fn send_macos_notification_blocking(
    title: String,
    body: String,
    icon_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    static APP_SET_RESULT: std::sync::OnceLock<Result<(), String>> = std::sync::OnceLock::new();
    let app_set_result = APP_SET_RESULT.get_or_init(|| {
        mac_notification_sys::set_application("com.apple.ScriptEditor2")
            .map_err(|err| err.to_string())
    });
    if let Err(err) = app_set_result {
        anyhow::bail!("设置 macOS 通知发送应用失败: {err}");
    }

    let icon_path = icon_path.map(|path| path.display().to_string());
    let mut notification = mac_notification_sys::Notification::new();
    notification.title(&title).message(&body).asynchronous(true);
    if let Some(icon_path) = icon_path.as_deref() {
        notification.app_icon(icon_path);
    }
    notification
        .send()
        .map(|_| ())
        .map_err(|err| anyhow::anyhow!("发送 macOS 通知失败: {err}"))
}

#[cfg(target_os = "macos")]
async fn ensure_notification_icon(paths: &SupervisorPaths) -> Option<PathBuf> {
    let path = paths.notification_icon_path();
    let bytes = match tokio::task::spawn_blocking(notification_icon_png).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            log::warn!(target: "supervisor", "生成 ACN 通知图标失败: {err:#}");
            return None;
        }
        Err(err) => {
            log::warn!(target: "supervisor", "生成 ACN 通知图标任务失败: {err}");
            return None;
        }
    };
    match fs::read(&path).await {
        Ok(existing) if existing == bytes => return Some(path),
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            log::warn!(
                target: "supervisor",
                "读取 ACN 通知图标失败 ({}): {err}",
                path.display()
            );
        }
    }
    if let Err(err) = write_text_atomic(&path, &bytes).await {
        log::warn!(
            target: "supervisor",
            "写入 ACN 通知图标失败 ({}): {err}",
            path.display()
        );
        return None;
    }
    Some(path)
}

#[cfg(any(target_os = "macos", test))]
fn notification_icon_png() -> anyhow::Result<Vec<u8>> {
    const SIZE: u32 = 256;
    const RADIUS: u32 = 44;
    const BORDER: u32 = 8;
    let transparent = image::Rgba([0x00, 0x00, 0x00, 0x00]);
    let background = image::Rgba([0xef, 0xf6, 0xff, 0xff]);
    let border = image::Rgba([0xbf, 0xdb, 0xfe, 0xff]);
    let text = image::Rgba([0x1d, 0x4e, 0xd8, 0xff]);

    let mut img = image::RgbaImage::from_pixel(SIZE, SIZE, transparent);
    for y in 0..SIZE {
        for x in 0..SIZE {
            if !rounded_rect_contains(x, y, SIZE, RADIUS) {
                continue;
            }
            let color = if rounded_rect_contains_inset(x, y, SIZE, RADIUS, BORDER) {
                background
            } else {
                border
            };
            img.put_pixel(x, y, color);
        }
    }

    draw_acn_mark(&mut img, text);

    let mut bytes = Vec::new();
    image::DynamicImage::ImageRgba8(img)
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .context("编码 ACN 通知图标 PNG 失败")?;
    Ok(bytes)
}

#[cfg(any(target_os = "macos", test))]
fn draw_acn_mark(img: &mut image::RgbaImage, color: image::Rgba<u8>) {
    #[cfg(target_os = "macos")]
    {
        if let Err(err) = draw_acn_mark_times_new_roman(img, color) {
            log::warn!(
                target: "supervisor",
                "Times New Roman 通知图标绘制失败，回退几何字标: {err:#}"
            );
        } else {
            return;
        }
    }

    draw_acn_mark_fallback(img, color);
}

#[cfg(target_os = "macos")]
fn draw_acn_mark_times_new_roman(
    img: &mut image::RgbaImage,
    color: image::Rgba<u8>,
) -> anyhow::Result<()> {
    let font = load_times_new_roman_font()?;
    let scale = Scale::uniform(118.0);
    let spacing = 1.0;
    let v_metrics = font.v_metrics(scale);
    let chars = ['A', 'C', 'N'];
    let baseline = v_metrics.ascent;
    let mut caret_x = 0.0_f32;
    let mut placements = Vec::with_capacity(chars.len());
    for ch in chars {
        let glyph = font.glyph(ch).scaled(scale);
        let advance = glyph.h_metrics().advance_width;
        placements.push((ch, caret_x));
        caret_x += advance + spacing;
    }

    let mut bounds: Option<rusttype::Rect<i32>> = None;
    for (ch, x) in &placements {
        let positioned = font
            .glyph(*ch)
            .scaled(scale)
            .positioned(point(*x, baseline));
        if let Some(bb) = positioned.pixel_bounding_box() {
            bounds = Some(match bounds {
                Some(acc) => rusttype::Rect {
                    min: rusttype::point(acc.min.x.min(bb.min.x), acc.min.y.min(bb.min.y)),
                    max: rusttype::point(acc.max.x.max(bb.max.x), acc.max.y.max(bb.max.y)),
                },
                None => bb,
            });
        }
    }
    let Some(bounds) = bounds else {
        anyhow::bail!("Times New Roman 字形没有可用像素边界");
    };

    let shift_x =
        (img.width() as f32 - (bounds.max.x - bounds.min.x) as f32) / 2.0 - bounds.min.x as f32;
    let shift_y =
        (img.height() as f32 - (bounds.max.y - bounds.min.y) as f32) / 2.0 - bounds.min.y as f32;
    for (ch, x) in placements {
        let positioned = font
            .glyph(ch)
            .scaled(scale)
            .positioned(point(x + shift_x, baseline + shift_y));
        draw_positioned_glyph(img, &positioned, color);
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn load_times_new_roman_font() -> anyhow::Result<Font<'static>> {
    const FONT_CANDIDATES: &[&str] = &[
        "/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf",
        "/Library/Fonts/Times New Roman Bold.ttf",
        "/System/Library/Fonts/Supplemental/Times New Roman.ttf",
        "/Library/Fonts/Times New Roman.ttf",
    ];
    for path in FONT_CANDIDATES {
        match std::fs::read(path) {
            Ok(bytes) => {
                if let Some(font) = Font::try_from_vec(bytes) {
                    return Ok(font);
                }
                log::warn!(target: "supervisor", "Times New Roman 字体文件不可用: {path}");
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                log::warn!(target: "supervisor", "读取 Times New Roman 字体失败 ({path}): {err}");
            }
        }
    }
    anyhow::bail!("未找到可用的 Times New Roman 字体")
}

#[cfg(target_os = "macos")]
fn draw_positioned_glyph(
    img: &mut image::RgbaImage,
    glyph: &rusttype::PositionedGlyph<'_>,
    color: image::Rgba<u8>,
) {
    let Some(bb) = glyph.pixel_bounding_box() else {
        return;
    };
    let width = i32::try_from(img.width()).unwrap_or(i32::MAX);
    let height = i32::try_from(img.height()).unwrap_or(i32::MAX);
    glyph.draw(|x, y, coverage| {
        let px = bb.min.x + i32::try_from(x).unwrap_or(i32::MAX);
        let py = bb.min.y + i32::try_from(y).unwrap_or(i32::MAX);
        if px < 0 || py < 0 || px >= width || py >= height {
            return;
        }
        blend_coverage_pixel(img, px as u32, py as u32, color, coverage);
    });
}

#[cfg(target_os = "macos")]
fn blend_coverage_pixel(
    img: &mut image::RgbaImage,
    x: u32,
    y: u32,
    color: image::Rgba<u8>,
    coverage: f32,
) {
    let coverage = coverage.clamp(0.0, 1.0);
    let bg = img.get_pixel(x, y).0;
    let fg = color.0;
    let inv = 1.0 - coverage;
    let blend = |bg: u8, fg: u8| -> u8 {
        (f32::from(bg) * inv + f32::from(fg) * coverage)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    img.put_pixel(
        x,
        y,
        image::Rgba([
            blend(bg[0], fg[0]),
            blend(bg[1], fg[1]),
            blend(bg[2], fg[2]),
            255,
        ]),
    );
}

#[cfg(any(target_os = "macos", test))]
fn draw_acn_mark_fallback(img: &mut image::RgbaImage, color: image::Rgba<u8>) {
    draw_thick_line(img, (45, 174), (76, 82), 16, color);
    draw_thick_line(img, (76, 82), (107, 174), 16, color);
    draw_thick_line(img, (60, 133), (92, 133), 14, color);

    draw_thick_line(img, (164, 94), (124, 94), 16, color);
    draw_thick_line(img, (116, 103), (116, 153), 16, color);
    draw_thick_line(img, (124, 162), (164, 162), 16, color);

    draw_thick_line(img, (186, 92), (186, 164), 16, color);
    draw_thick_line(img, (234, 92), (234, 164), 16, color);
    draw_thick_line(img, (186, 92), (234, 164), 16, color);
}

#[cfg(any(target_os = "macos", test))]
fn rounded_rect_contains(x: u32, y: u32, size: u32, radius: u32) -> bool {
    let max = i64::from(size.saturating_sub(1));
    let radius = i64::from(radius);
    let x = i64::from(x);
    let y = i64::from(y);
    let center_x = if x < radius {
        radius
    } else if x > max.saturating_sub(radius) {
        max.saturating_sub(radius)
    } else {
        x
    };
    let center_y = if y < radius {
        radius
    } else if y > max.saturating_sub(radius) {
        max.saturating_sub(radius)
    } else {
        y
    };
    let dx = x - center_x;
    let dy = y - center_y;
    dx * dx + dy * dy <= radius * radius
}

#[cfg(any(target_os = "macos", test))]
fn rounded_rect_contains_inset(x: u32, y: u32, size: u32, radius: u32, inset: u32) -> bool {
    let inner_size = size.saturating_sub(inset.saturating_mul(2));
    if inner_size == 0
        || x < inset
        || y < inset
        || x >= size.saturating_sub(inset)
        || y >= size.saturating_sub(inset)
    {
        return false;
    }
    rounded_rect_contains(
        x - inset,
        y - inset,
        inner_size,
        radius.saturating_sub(inset),
    )
}

#[cfg(any(target_os = "macos", test))]
fn draw_thick_line(
    img: &mut image::RgbaImage,
    start: (u32, u32),
    end: (u32, u32),
    thickness: u32,
    color: image::Rgba<u8>,
) {
    let min_x = start.0.min(end.0).saturating_sub(thickness);
    let max_x = start
        .0
        .max(end.0)
        .saturating_add(thickness)
        .min(img.width().saturating_sub(1));
    let min_y = start.1.min(end.1).saturating_sub(thickness);
    let max_y = start
        .1
        .max(end.1)
        .saturating_add(thickness)
        .min(img.height().saturating_sub(1));
    let radius = f64::from(thickness) / 2.0;
    let radius_sq = radius * radius;
    for y in min_y..=max_y {
        for x in min_x..=max_x {
            let px = f64::from(x) + 0.5;
            let py = f64::from(y) + 0.5;
            if distance_to_segment_squared(px, py, start, end) <= radius_sq {
                img.put_pixel(x, y, color);
            }
        }
    }
}

#[cfg(any(target_os = "macos", test))]
fn distance_to_segment_squared(px: f64, py: f64, start: (u32, u32), end: (u32, u32)) -> f64 {
    let x0 = f64::from(start.0);
    let y0 = f64::from(start.1);
    let x1 = f64::from(end.0);
    let y1 = f64::from(end.1);
    let vx = x1 - x0;
    let vy = y1 - y0;
    let wx = px - x0;
    let wy = py - y0;
    let len_sq = vx * vx + vy * vy;
    let t = if len_sq <= f64::EPSILON {
        0.0
    } else {
        ((wx * vx + wy * vy) / len_sq).clamp(0.0, 1.0)
    };
    let projection_x = x0 + t * vx;
    let projection_y = y0 + t * vy;
    let dx = px - projection_x;
    let dy = py - projection_y;
    dx * dx + dy * dy
}

#[cfg(target_os = "macos")]
async fn notify_macos_with_osascript(title: &str, body: &str) {
    let script = format!(
        "display notification {} with title {}",
        applescript_string(body),
        applescript_string(title)
    );
    let status = tokio::process::Command::new("osascript")
        .arg("-e")
        .arg(script)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let status = tokio::time::timeout(
        Duration::from_millis(DEFAULT_SUPERVISOR_NOTIFICATION_TIMEOUT_MS),
        status,
    )
    .await;
    match status {
        Ok(Ok(status)) if status.success() => {}
        Ok(Ok(status)) => log::warn!(target: "supervisor", "发送 macOS 通知退出异常: {status}"),
        Ok(Err(err)) => log::warn!(target: "supervisor", "发送 macOS 通知失败: {err}"),
        Err(_) => log::warn!(target: "supervisor", "发送 macOS 通知超时"),
    }
}

#[cfg(any(target_os = "macos", test))]
fn applescript_string(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

fn truncate_notification(value: &str) -> String {
    const LIMIT: usize = 160;
    if value.chars().count() <= LIMIT {
        return value.to_string();
    }
    value.chars().take(LIMIT).collect()
}

fn job_session_id(job: &SupervisorJob) -> String {
    match &job.kind {
        SupervisorJobKind::Finalize { session_id } => session_id.to_string(),
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn millis_u64(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn stable_path_hash(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    const UPDATE_HOLDER_AGENT_HOME_ENV: &str = "ACN_TEST_UPDATE_HOLDER_AGENT_HOME";
    const TAKEOVER_HOLDER_AGENT_HOME_ENV: &str = "ACN_TEST_TAKEOVER_HOLDER_AGENT_HOME";
    const TAKEOVER_HOLDER_BUILD_ENV: &str = "ACN_TEST_TAKEOVER_HOLDER_BUILD";

    fn queued_finalize_job(id: &str, session_id: SessionId) -> SupervisorJob {
        let now = Utc::now();
        SupervisorJob {
            id: id.to_owned(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            kind: SupervisorJobKind::Finalize { session_id },
            status: SupervisorJobStatus::Queued,
            attempts: 0,
            created_at: now,
            updated_at: now,
            started_at: None,
            finished_at: None,
            last_error: None,
            notify_on_completion: true,
        }
    }

    #[test]
    fn applescript_string_escapes_quotes_and_backslashes() {
        assert_eq!(applescript_string(r#"a "b" \ c"#), r#""a \"b\" \\ c""#);
    }

    #[test]
    fn supervisor_socket_path_stays_in_temp_dir() {
        let path = supervisor_socket_path(Path::new("/very/long/agent/home"));
        assert_eq!(path.parent(), Some(std::env::temp_dir().as_path()));
        assert!(path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("acn-supervisor-")));
    }

    #[test]
    fn notification_icon_path_uses_supervisor_dir() {
        let paths = SupervisorPaths::new(Path::new("/tmp/acn-agent"));
        assert_eq!(
            paths.notification_icon_path(),
            paths
                .supervisor_dir
                .join(SUPERVISOR_NOTIFICATION_ICON_FILE_NAME)
        );
    }

    #[test]
    fn notification_icon_png_is_valid_blue_acn_tile() -> anyhow::Result<()> {
        let bytes = notification_icon_png()?;
        let decoded = image::load_from_memory(&bytes)?.to_rgba8();

        assert_eq!(decoded.width(), 256);
        assert_eq!(decoded.height(), 256);
        assert_eq!(decoded.get_pixel(0, 0).0[3], 0);
        assert_eq!(decoded.get_pixel(128, 128).0[3], 255);
        assert!(decoded
            .pixels()
            .any(|pixel| pixel.0 == [0x1d, 0x4e, 0xd8, 0xff]));

        Ok(())
    }

    #[test]
    fn supervisor_request_round_trips_enqueue_finalize() {
        let request = SupervisorRequest::EnqueueFinalize {
            session_id: "session_1234abcd".parse().unwrap(),
            notify_on_completion: true,
        };
        let json = serde_json::to_string(&request).unwrap();

        assert_eq!(
            json,
            r#"{"type":"enqueue_finalize","session_id":"session_1234abcd"}"#
        );
        assert_eq!(
            serde_json::from_str::<SupervisorRequest>(&json).unwrap(),
            request
        );
        let quiet = SupervisorRequest::EnqueueFinalize {
            session_id: "session_1234abcd".parse().unwrap(),
            notify_on_completion: false,
        };
        let quiet_json = serde_json::to_string(&quiet).unwrap();
        assert_eq!(
            quiet_json,
            r#"{"type":"enqueue_finalize","session_id":"session_1234abcd","notify_on_completion":false}"#
        );
        assert_eq!(
            serde_json::from_str::<SupervisorRequest>(&quiet_json).unwrap(),
            quiet
        );
    }

    #[test]
    fn supervisor_request_round_trips_status_and_stop() {
        let status = serde_json::to_string(&SupervisorRequest::Status).unwrap();
        let stop = serde_json::to_string(&SupervisorRequest::Stop).unwrap();

        assert_eq!(status, r#"{"type":"status"}"#);
        assert_eq!(stop, r#"{"type":"stop"}"#);
        assert_eq!(
            serde_json::from_str::<SupervisorRequest>(&status).unwrap(),
            SupervisorRequest::Status
        );
        assert_eq!(
            serde_json::from_str::<SupervisorRequest>(&stop).unwrap(),
            SupervisorRequest::Stop
        );
    }

    #[test]
    fn supervisor_status_response_accepts_legacy_payload_without_build() {
        let response = serde_json::from_str::<SupervisorResponse>(
            r#"{"type":"status","pid":42,"started_at":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();

        assert!(matches!(
            response,
            SupervisorResponse::Status {
                pid: 42,
                build: None,
                ..
            }
        ));
    }

    #[test]
    fn supervisor_build_match_requires_current_version_and_commit() {
        assert!(supervisor_build_matches_current(Some(
            &BuildIdentity::current()
        )));
        assert!(!supervisor_build_matches_current(None));
        assert!(!supervisor_build_matches_current(Some(&BuildIdentity {
            version: env!("CARGO_PKG_VERSION").into(),
            commit: "previous".into(),
        })));
    }

    #[test]
    fn supervisor_startup_timeout_and_error_report_five_seconds() {
        let timeout = Duration::from_millis(DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS);

        assert_eq!(timeout, Duration::from_secs(5));
        assert_eq!(
            supervisor_startup_timeout_error(timeout).to_string(),
            "supervisor 在 5 秒内未就绪"
        );
    }

    #[test]
    fn ipc_error_classification_only_treats_missing_socket_as_unavailable() {
        let not_found = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound));
        let refused =
            anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        let timed_out = anyhow::anyhow!("supervisor IPC 超时");

        assert!(ipc_error_indicates_unavailable(&not_found));
        assert!(ipc_error_indicates_unavailable(&refused));
        assert!(!ipc_error_indicates_unavailable(&timed_out));
    }

    #[test]
    fn process_probe_classifies_kill_zero_errno() {
        assert_eq!(probe_process(None), ProcessProbe::NotRunning);
        assert_eq!(process_probe_from_kill_result(0, None), ProcessProbe::Alive);
        assert_eq!(
            process_probe_from_kill_result(-1, Some(libc::EPERM)),
            ProcessProbe::Alive
        );
        assert_eq!(
            process_probe_from_kill_result(-1, Some(libc::ESRCH)),
            ProcessProbe::NotRunning
        );
        assert_eq!(
            process_probe_from_kill_result(-1, Some(libc::EINVAL)),
            ProcessProbe::InvalidPid
        );
    }

    #[tokio::test]
    async fn supervisor_status_keeps_unavailable_ipc_without_pid_stopped() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        if let Some(parent) = paths.socket_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let listener = UnixListener::bind(&paths.socket_path)?;
        drop(listener);

        let status = supervisor_status(dir.path()).await?;

        assert_eq!(status.runtime_state, SupervisorRuntimeState::Stopped);
        assert_eq!(status.pid, None);
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_status_keeps_timed_out_ipc_without_pid_as_error() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        if let Some(parent) = paths.socket_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let _listener = UnixListener::bind(&paths.socket_path)?;

        let err = supervisor_status(dir.path()).await.unwrap_err();
        let details = format!("{err:#}");

        assert!(details.contains("查询 supervisor status 失败"));
        assert!(details.contains("pid=None"));
        assert!(details.contains("process_probe=NotRunning"));
        assert!(details.contains(&paths.socket_path.display().to_string()));
        assert!(details.contains(&paths.pid_path.display().to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_status_keeps_timed_out_ipc_with_invalid_pid_as_error() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.supervisor_dir).await?;
        fs::write(&paths.pid_path, "0").await?;
        if let Some(parent) = paths.socket_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let _listener = UnixListener::bind(&paths.socket_path)?;

        let err = supervisor_status(dir.path()).await.unwrap_err();
        let details = format!("{err:#}");

        assert!(details.contains("查询 supervisor status 失败"));
        assert!(details.contains("pid=Some(0)"));
        assert!(details.contains("process_probe=InvalidPid"));
        assert!(details.contains(&paths.socket_path.display().to_string()));
        assert!(details.contains(&paths.pid_path.display().to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_status_reports_stuck_when_ipc_times_out_and_pid_is_alive(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.supervisor_dir).await?;
        fs::write(&paths.pid_path, std::process::id().to_string()).await?;
        if let Some(parent) = paths.socket_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let _listener = UnixListener::bind(&paths.socket_path)?;

        let status = supervisor_status(dir.path()).await?;

        assert_eq!(status.pid, Some(std::process::id()));
        assert!(matches!(
            status.runtime_state,
            SupervisorRuntimeState::Stuck { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn update_preflight_treats_missing_supervisor_as_stopped() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let state = preflight_supervisor_shutdown(dir.path()).await?;

        assert_eq!(state, VerifiedSupervisorState::Stopped);
        Ok(())
    }

    #[tokio::test]
    async fn update_preflight_rejects_live_pid_without_confirming_ipc() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.supervisor_dir).await?;
        fs::write(&paths.pid_path, std::process::id().to_string()).await?;

        let error = preflight_supervisor_shutdown(dir.path())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("仍存活，但无法通过 IPC 确认身份"));
        assert!(error.contains(&paths.socket_path.display().to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn update_preflight_requires_ipc_and_pid_file_to_match() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.supervisor_dir).await?;
        fs::write(&paths.pid_path, "42").await?;
        if let Some(parent) = paths.socket_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let listener = UnixListener::bind(&paths.socket_path)?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let _ = lines.next_line().await.unwrap();
            let response = SupervisorResponse::Status {
                pid: std::process::id(),
                started_at: Utc::now(),
                build: None,
            };
            let mut bytes = serde_json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            write_half.write_all(&bytes).await.unwrap();
        });

        let error = preflight_supervisor_shutdown(dir.path())
            .await
            .unwrap_err()
            .to_string();
        server.await?;
        remove_stale_socket(&paths.socket_path).await;

        assert!(error.contains("与 PID 文件 42 不一致"));
        Ok(())
    }

    #[tokio::test]
    async fn update_shutdown_guard_blocks_supervisor_restart_until_drop() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());

        let guard =
            shutdown_verified_supervisor(dir.path(), VerifiedSupervisorState::Stopped).await?;
        assert!(FileLockGuard::try_lock_exclusive(&paths.process_lock_path)
            .await?
            .is_none());

        drop(guard);
        let reacquired = FileLockGuard::try_lock_exclusive(&paths.process_lock_path).await?;
        assert!(reacquired.is_some());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn update_shutdown_kills_verified_supervisor_subprocess() -> anyhow::Result<()> {
        if let Some(agent_home) = std::env::var_os(UPDATE_HOLDER_AGENT_HOME_ENV) {
            return run_test_supervisor_holder(PathBuf::from(agent_home), None).await;
        }

        let dir = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let mut child = tokio::process::Command::new(executable)
            .arg("--exact")
            .arg("supervisor::tests::update_shutdown_kills_verified_supervisor_subprocess")
            .arg("--nocapture")
            .env(UPDATE_HOLDER_AGENT_HOME_ENV, dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let child_pid = child.id().context("测试 supervisor holder 缺少 PID")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let state = loop {
            match preflight_supervisor_shutdown(dir.path()).await {
                Ok(state @ VerifiedSupervisorState::Running { .. }) => break state,
                result if Instant::now() >= deadline => {
                    let _ = child.kill().await;
                    anyhow::bail!("测试 supervisor holder 未就绪: {result:?}");
                }
                _ => sleep(Duration::from_millis(25)).await,
            }
        };
        assert_eq!(state, VerifiedSupervisorState::Running { pid: child_pid });

        let guard = shutdown_verified_supervisor(dir.path(), state).await?;
        let status = tokio::time::timeout(Duration::from_secs(2), child.wait()).await??;

        assert!(!status.success());
        let paths = SupervisorPaths::new(dir.path());
        assert!(FileLockGuard::try_lock_exclusive(&paths.process_lock_path)
            .await?
            .is_none());
        drop(guard);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ensure_supervisor_replaces_legacy_build_and_waits_for_current_build(
    ) -> anyhow::Result<()> {
        if let Some(agent_home) = std::env::var_os(TAKEOVER_HOLDER_AGENT_HOME_ENV) {
            let build = match std::env::var(TAKEOVER_HOLDER_BUILD_ENV).as_deref() {
                Ok("current") => Some(BuildIdentity::current()),
                Ok("legacy") => None,
                other => anyhow::bail!("无效 takeover holder build: {other:?}"),
            };
            return run_test_supervisor_holder(PathBuf::from(agent_home), build).await;
        }

        let dir = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let test_name =
            "supervisor::tests::ensure_supervisor_replaces_legacy_build_and_waits_for_current_build";
        let spawn_holder = |build: &str| -> anyhow::Result<tokio::process::Child> {
            tokio::process::Command::new(&executable)
                .arg("--exact")
                .arg(test_name)
                .arg("--nocapture")
                .env(TAKEOVER_HOLDER_AGENT_HOME_ENV, dir.path())
                .env(TAKEOVER_HOLDER_BUILD_ENV, build)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .context("启动测试 supervisor holder 失败")
        };

        let mut legacy = spawn_holder("legacy")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match preflight_supervisor_shutdown(dir.path()).await {
                Ok(VerifiedSupervisorState::Running { .. }) => break,
                result if Instant::now() >= deadline => {
                    let _ = legacy.kill().await;
                    anyhow::bail!("legacy supervisor holder 未就绪: {result:?}");
                }
                _ => sleep(Duration::from_millis(25)).await,
            }
        }

        let launch = SupervisorLaunchConfig::new(
            dir.path().to_path_buf(),
            dir.path().join("config.toml"),
            None,
            None,
            true,
        );
        let (replacement_tx, replacement_rx) = std::sync::mpsc::channel();
        ensure_supervisor_running_with(&launch, |_| {
            let replacement = spawn_holder("current")?;
            replacement_tx
                .send(replacement)
                .map_err(|_| anyhow::anyhow!("记录 replacement supervisor 失败"))
        })
        .await?;

        let status = supervisor_status(dir.path()).await?;
        assert_eq!(status.runtime_state, SupervisorRuntimeState::Running);
        assert_eq!(status.build, Some(BuildIdentity::current()));
        let replacement_state = preflight_supervisor_shutdown(dir.path()).await?;
        let guard = shutdown_verified_supervisor(dir.path(), replacement_state).await?;
        drop(guard);

        let mut replacement = replacement_rx
            .recv()
            .context("未收到 replacement supervisor child")?;
        let legacy_status = tokio::time::timeout(Duration::from_secs(2), legacy.wait()).await??;
        let replacement_status =
            tokio::time::timeout(Duration::from_secs(2), replacement.wait()).await??;
        assert!(!legacy_status.success());
        assert!(!replacement_status.success());
        Ok(())
    }

    #[cfg(unix)]
    async fn run_test_supervisor_holder(
        agent_home: PathBuf,
        build: Option<BuildIdentity>,
    ) -> anyhow::Result<()> {
        let paths = SupervisorPaths::new(&agent_home);
        fs::create_dir_all(&paths.supervisor_dir)
            .await
            .context("创建测试 supervisor 目录失败")?;
        let _process_lock = FileLockGuard::lock_exclusive(&paths.process_lock_path)
            .await
            .context("获取测试 supervisor 生命周期锁失败")?;
        remove_stale_socket(&paths.socket_path).await;
        let listener = UnixListener::bind(&paths.socket_path).with_context(|| {
            format!(
                "绑定测试 supervisor socket 失败: {}",
                paths.socket_path.display()
            )
        })?;
        fs::write(&paths.pid_path, std::process::id().to_string())
            .await
            .context("写入测试 supervisor PID 失败")?;
        loop {
            let (stream, _) = listener.accept().await?;
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let Some(line) = lines.next_line().await? else {
                continue;
            };
            let response = match serde_json::from_str::<SupervisorRequest>(&line) {
                Ok(SupervisorRequest::Status) => SupervisorResponse::Status {
                    pid: std::process::id(),
                    started_at: Utc::now(),
                    build: build.clone(),
                },
                _ => SupervisorResponse::Error {
                    message: "test holder only supports status".into(),
                },
            };
            let mut bytes = serde_json::to_vec(&response)?;
            bytes.push(b'\n');
            write_half.write_all(&bytes).await?;
        }
    }

    #[tokio::test]
    async fn handle_client_rejects_enqueue_after_stop_gate() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let (notify_tx, _notify_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let shared = SupervisorSharedState {
            agent_id,
            notify_tx,
            stop_requested: cancel,
            last_activity: Arc::new(AtomicU64::new(now_millis())),
            started_at: Utc::now(),
            stopping: Arc::new(AtomicBool::new(true)),
            lifecycle_gate: Arc::new(Mutex::new(())),
        };
        let (client, server) = UnixStream::pair()?;

        let server_fut = handle_client(server, &paths, &shared);
        let client_fut = async move {
            let (read_half, mut write_half) = client.into_split();
            let request = SupervisorRequest::EnqueueFinalize {
                session_id: "session_1234abcd".parse().unwrap(),
                notify_on_completion: true,
            };
            let mut line = serde_json::to_vec(&request).unwrap();
            line.push(b'\n');
            write_half.write_all(&line).await.unwrap();
            let line = BufReader::new(read_half)
                .lines()
                .next_line()
                .await
                .unwrap()
                .unwrap();
            serde_json::from_str::<SupervisorResponse>(&line).unwrap()
        };

        let (server_result, response) = tokio::join!(server_fut, client_fut);

        server_result?;
        assert_eq!(
            response,
            SupervisorResponse::Error {
                message: "supervisor 正在停止，拒绝新 finalize job".into()
            }
        );
        assert!(!paths.jobs_dir.exists());
        Ok(())
    }

    #[tokio::test]
    async fn create_finalize_job_persists_notification_preference() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;

        let job = create_finalize_job(&paths, &agent_id, session_id, false).await?;
        let stored = read_jobs(&paths)
            .await?
            .into_iter()
            .find(|stored| stored.id == job.id)
            .context("created job was not persisted")?;

        assert!(!job.notify_on_completion);
        assert!(!stored.notify_on_completion);
        Ok(())
    }

    #[tokio::test]
    async fn create_finalize_job_retries_when_id_collides_without_overwriting_existing_job(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let colliding_id = "job_collision";
        let unique_id = "job_unique";
        let existing = queued_finalize_job(colliding_id, "session_11111111".parse()?);
        write_yaml_atomic(&job_path(&paths, colliding_id), &existing).await?;

        let attempts = Cell::new(0usize);
        let created = create_finalize_job_with_id_factory(
            &paths,
            &agent_id,
            "session_22222222".parse()?,
            true,
            || {
                let attempt = attempts.get();
                attempts.set(attempt + 1);
                if attempt == 0 {
                    colliding_id.to_owned()
                } else {
                    unique_id.to_owned()
                }
            },
            2,
        )
        .await?;

        assert_eq!(created.id, unique_id);
        assert_eq!(attempts.get(), 2);
        let preserved = read_yaml::<SupervisorJob>(&job_path(&paths, colliding_id)).await?;
        assert_eq!(preserved, existing);
        let stored = read_yaml::<SupervisorJob>(&job_path(&paths, unique_id)).await?;
        assert_eq!(stored, created);
        Ok(())
    }

    #[tokio::test]
    async fn read_jobs_skips_filename_payload_id_mismatch() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let mismatched = queued_finalize_job("job_payload", "session_11111111".parse()?);
        let valid = queued_finalize_job("job_valid", "session_22222222".parse()?);
        write_yaml_atomic(&job_path(&paths, "job_filename"), &mismatched).await?;
        write_yaml_atomic(&job_path(&paths, &valid.id), &valid).await?;

        let jobs = read_jobs(&paths).await?;

        assert_eq!(jobs, vec![valid]);
        let log = fs::read_to_string(&paths.log_path).await?;
        assert!(log.contains(
            "filename stem Some(\"job_filename\") does not match payload id job_payload"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn read_jobs_skips_unfinished_id_reservation() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.jobs_dir).await?;
        let reservation = job_path(&paths, "job_reserved");
        fs::File::create(&reservation).await?;

        let jobs = read_jobs(&paths).await?;

        assert!(jobs.is_empty());
        let log = fs::read_to_string(&paths.log_path).await?;
        assert!(log.contains("skip malformed supervisor job"));
        Ok(())
    }

    #[tokio::test]
    async fn write_job_overwrites_matching_existing_job() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let mut updated = queued_finalize_job("job_safe_update", "session_11111111".parse()?);
        write_yaml_atomic(&job_path(&paths, &updated.id), &updated).await?;
        updated.status = SupervisorJobStatus::Running;
        updated.attempts = 1;
        updated.started_at = Some(Utc::now());

        write_job(&paths, &updated).await?;

        let stored = read_yaml::<SupervisorJob>(&job_path(&paths, &updated.id)).await?;
        assert_eq!(stored, updated);
        Ok(())
    }

    #[tokio::test]
    async fn write_job_rejects_filename_payload_id_mismatch() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let stored = queued_finalize_job("job_payload", "session_11111111".parse()?);
        let update = queued_finalize_job("job_filename", "session_22222222".parse()?);
        let path = job_path(&paths, &update.id);
        write_yaml_atomic(&path, &stored).await?;

        let err = write_job(&paths, &update).await.unwrap_err();

        assert!(err.to_string().contains("拒绝覆写 supervisor job"));
        let after = read_yaml::<SupervisorJob>(&path).await?;
        assert_eq!(after, stored);
        Ok(())
    }

    #[test]
    fn supervisor_job_without_agent_id_stays_readable() {
        let yaml = r#"
id: job_1
kind:
  type: finalize
  session_id: session_1234abcd
status: queued
attempts: 0
created_at: "2026-06-25T00:00:00Z"
updated_at: "2026-06-25T00:00:00Z"
"#;

        let job = serde_yaml_ng::from_str::<SupervisorJob>(yaml).unwrap();

        assert_eq!(job.agent_id, None);
        assert!(job.notify_on_completion);
        assert_eq!(job_session_id(&job), "session_1234abcd");
    }

    #[test]
    fn supervisor_job_view_exposes_agent_session_and_status() {
        let now = Utc::now();
        let job = SupervisorJob {
            id: "job_1".to_string(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            kind: SupervisorJobKind::Finalize {
                session_id: "session_1234abcd".parse().unwrap(),
            },
            status: SupervisorJobStatus::Running,
            attempts: 2,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            finished_at: None,
            last_error: Some("retrying".to_string()),
            notify_on_completion: false,
        };

        let view = job_to_view(&job);

        assert_eq!(view.agent_id.as_ref().unwrap().as_str(), "agent-a");
        assert_eq!(view.session_id.as_str(), "session_1234abcd");
        assert_eq!(view.status, "running");
        assert_eq!(view.attempts, 2);
        assert_eq!(view.last_error.as_deref(), Some("retrying"));
    }

    #[test]
    fn supervisor_queue_summary_counts_statuses() {
        let now = Utc::now();
        let jobs = [
            SupervisorJob {
                id: "job_1".to_string(),
                agent_id: Some(AgentId::new("agent-a").unwrap()),
                kind: SupervisorJobKind::Finalize {
                    session_id: "session_11111111".parse().unwrap(),
                },
                status: SupervisorJobStatus::Queued,
                attempts: 0,
                created_at: now,
                updated_at: now,
                started_at: None,
                finished_at: None,
                last_error: None,
                notify_on_completion: true,
            },
            SupervisorJob {
                id: "job_2".to_string(),
                agent_id: Some(AgentId::new("agent-a").unwrap()),
                kind: SupervisorJobKind::Finalize {
                    session_id: "session_22222222".parse().unwrap(),
                },
                status: SupervisorJobStatus::Failed,
                attempts: 3,
                created_at: now,
                updated_at: now,
                started_at: Some(now),
                finished_at: Some(now),
                last_error: Some("failed".to_string()),
                notify_on_completion: true,
            },
        ];

        let summary = SupervisorQueueSummary::from_jobs(&jobs);

        assert_eq!(summary.total, 2);
        assert_eq!(summary.queued, 1);
        assert_eq!(summary.running, 0);
        assert_eq!(summary.succeeded, 0);
        assert_eq!(summary.failed, 1);
    }

    #[test]
    fn finalize_success_notification_requires_finalized_unrecapped_messages() {
        let mut report = crate::agent::SessionFinalizeReport::default();

        assert!(!finalize_report_should_notify_success(&report));

        report.advanced_recapped_until = true;
        assert!(!finalize_report_should_notify_success(&report));

        report.finalized_unrecapped_messages = true;
        assert!(finalize_report_should_notify_success(&report));
    }
}
