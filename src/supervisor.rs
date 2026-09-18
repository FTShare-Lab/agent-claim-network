//! 轻量后台 supervisor。
//!
//! 承载 session recap / finalize job：TUI enqueue 后不等待后台模型执行，supervisor
//! 按优先级串行处理。它是按需启动、空闲退出的普通子进程，不注册 OS service。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use chrono::{DateTime, Utc};
use rand::RngCore;
use ring::digest::{Context as DigestContext, SHA256};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

use crate::agent::{
    SessionEngine, SessionEvent, SessionFinalizeOnceOutcome, SessionFinalizePreemptionControl,
    SessionFinalizeReport, SessionRecapPreemptionControl,
};
use crate::build_info::BuildIdentity;
use crate::claim::{AgentId, SessionId};
#[cfg(target_os = "macos")]
use crate::config::DEFAULT_SUPERVISOR_NOTIFICATION_TIMEOUT_MS;
#[cfg(any(target_os = "macos", test))]
use crate::config::SUPERVISOR_NOTIFICATION_ICON_FILE_NAME;
use crate::config::{
    default_id_mint_max_attempts, Config, ResolvedUpstream, DEFAULT_SUPERVISOR_IDLE_TIMEOUT_SECS,
    DEFAULT_SUPERVISOR_IPC_TIMEOUT_MS, DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS,
    DEFAULT_SUPERVISOR_LOCK_TIMEOUT_MS, DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS,
    DEFAULT_SUPERVISOR_STOP_WAIT_TIMEOUT_MS, DEFAULT_SUPERVISOR_UPDATE_SHUTDOWN_TIMEOUT_MS,
};
use crate::session::{
    finalize_checkpoint_covers_pending_range, SessionHandle, SessionMetadata, SessionPaths,
    SessionStatus,
};
#[cfg(test)]
use crate::session::{FinalizeCheckpoint, FinalizeCheckpointStatus};
#[cfg(target_os = "macos")]
use crate::storage::write_text_atomic;
use crate::storage::{mint_unique_id_in_dir, paths, read_yaml, write_yaml_atomic, FileLockGuard};

const SUPERVISOR_RUNTIME_FINGERPRINT_SCHEMA: u32 = 1;
const SUPERVISOR_STOPPING_MESSAGE: &str = "supervisor 正在停止，拒绝新 job";
const RESUME_FINALIZE_LIVENESS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorRuntimeFingerprint {
    pub schema: u32,
    pub digest: String,
}

impl SupervisorRuntimeFingerprint {
    fn short(&self) -> &str {
        self.digest.get(..12).unwrap_or(self.digest.as_str())
    }
}

/// 对 recap/finalize supervisor 实际使用的配置快照生成不含明文凭据的稳定身份。
pub fn runtime_fingerprint(
    cfg: &Config,
    upstream: &ResolvedUpstream,
) -> anyhow::Result<SupervisorRuntimeFingerprint> {
    let serialized = serde_json::to_vec(cfg).context("序列化 supervisor 配置快照失败")?;
    Ok(runtime_fingerprint_from_parts(
        &serialized,
        &upstream.name,
        cfg.agent.llm.api_key.as_deref(),
        upstream.acn_key.as_deref(),
    ))
}

fn runtime_fingerprint_from_parts(
    serialized_config: &[u8],
    upstream_name: &str,
    llm_key: Option<&str>,
    upstream_key: Option<&str>,
) -> SupervisorRuntimeFingerprint {
    let mut digest = DigestContext::new(&SHA256);
    update_fingerprint_part(&mut digest, b"acn-supervisor-runtime-v1");
    update_fingerprint_part(&mut digest, serialized_config);
    update_fingerprint_part(&mut digest, upstream_name.as_bytes());
    update_optional_fingerprint_part(&mut digest, llm_key.map(str::as_bytes));
    update_optional_fingerprint_part(&mut digest, upstream_key.map(str::as_bytes));
    SupervisorRuntimeFingerprint {
        schema: SUPERVISOR_RUNTIME_FINGERPRINT_SCHEMA,
        digest: hex::encode(digest.finish().as_ref()),
    }
}

fn update_fingerprint_part(digest: &mut DigestContext, value: &[u8]) {
    digest.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn update_optional_fingerprint_part(digest: &mut DigestContext, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            digest.update(&[1]);
            update_fingerprint_part(digest, value);
        }
        None => digest.update(&[0]),
    }
}

#[cfg(target_os = "macos")]
use rusttype::{point, Font, Scale};

#[derive(Debug, Clone)]
pub struct SupervisorLaunchConfig {
    pub agent_home: PathBuf,
    pub config_path: PathBuf,
    pub upstream: Option<String>,
    pub notify_on_finalize_completion: bool,
    pub runtime_fingerprint: SupervisorRuntimeFingerprint,
}

impl SupervisorLaunchConfig {
    pub fn new(
        agent_home: PathBuf,
        config_path: PathBuf,
        upstream: Option<String>,
        notify_on_finalize_completion: bool,
        runtime_fingerprint: SupervisorRuntimeFingerprint,
    ) -> Self {
        Self {
            agent_home,
            config_path,
            upstream,
            notify_on_finalize_completion,
            runtime_fingerprint,
        }
    }

    fn paths(&self) -> SupervisorPaths {
        SupervisorPaths::new(&self.agent_home)
    }
}

#[derive(Debug, Clone)]
pub struct SupervisorPaths {
    agent_home: PathBuf,
    supervisor_dir: PathBuf,
    jobs_dir: PathBuf,
    socket_path: PathBuf,
    pid_path: PathBuf,
    process_lock_path: PathBuf,
    transition_lock_path: PathBuf,
    log_path: PathBuf,
    launch_log_path: PathBuf,
}

impl SupervisorPaths {
    pub fn new(agent_home: &Path) -> Self {
        let supervisor_dir = paths::agent_home_supervisor_dir(agent_home);
        Self {
            agent_home: agent_home.to_path_buf(),
            jobs_dir: paths::agent_home_supervisor_jobs_dir(agent_home),
            socket_path: supervisor_socket_path(agent_home),
            pid_path: paths::agent_home_supervisor_pid_path(agent_home),
            process_lock_path: paths::agent_home_supervisor_launch_lock_path(agent_home),
            transition_lock_path: paths::agent_home_supervisor_transition_lock_path(agent_home),
            log_path: supervisor_dir.join("supervisor.log"),
            launch_log_path: supervisor_dir.join("supervisor-launch.log"),
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
    pub runtime_fingerprint: Option<SupervisorRuntimeFingerprint>,
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
    pub kind: String,
    pub session_id: SessionId,
    pub recap_end_index: Option<usize>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub attempts: u32,
    pub manual_retries: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizingSessionDiagnostic {
    Queued { job_id: String },
    Running { job_id: String },
    Failed { job_id: String },
    RunningWithoutJob,
    Orphaned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum FinalizingResumeTakeover {
    Opened {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_id: Option<String>,
    },
    WaitForFinalize {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_id: Option<String>,
    },
    ReopenClosed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_id: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "snake_case")]
pub enum SupervisorRetryTarget {
    Session { session_id: SessionId },
    Job { job_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorRetryReport {
    pub session_id: SessionId,
    pub job_id: String,
    pub previous_attempts: u32,
    pub manual_retries: u32,
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
    _transition_lock: FileLockGuard,
    _process_lock: FileLockGuard,
}

impl SupervisorShutdownGuard {
    fn release_process_lock(self) -> FileLockGuard {
        let Self {
            _transition_lock,
            _process_lock,
        } = self;
        drop(_process_lock);
        _transition_lock
    }
}

#[derive(Clone)]
struct SupervisorSharedState {
    agent_id: AgentId,
    notify_tx: mpsc::UnboundedSender<()>,
    stop_requested: CancellationToken,
    last_activity: Arc<AtomicU64>,
    started_at: DateTime<Utc>,
    runtime_fingerprint: SupervisorRuntimeFingerprint,
    stopping: Arc<AtomicBool>,
    lifecycle_gate: Arc<Mutex<()>>,
    running_recap: Arc<Mutex<Option<RunningRecap>>>,
    running_finalize: Arc<Mutex<Option<RunningFinalize>>>,
}

#[derive(Clone)]
struct RunningRecap {
    job_id: String,
    session_id: SessionId,
    preemption: Arc<SessionRecapPreemptionControl>,
}

#[derive(Clone)]
struct RunningFinalize {
    job_id: String,
    session_id: SessionId,
    preemption: Arc<SessionFinalizePreemptionControl>,
    resume_target: Arc<Mutex<Option<usize>>>,
    resume_result_tx: watch::Sender<Option<RunningFinalizeResumeResult>>,
}

struct RunningJobControls {
    recap_preemption: Option<Arc<SessionRecapPreemptionControl>>,
    finalize_preemption: Option<Arc<SessionFinalizePreemptionControl>>,
    finalize_resume_target: Option<Arc<Mutex<Option<usize>>>>,
    finalize_result_tx: Option<watch::Sender<Option<RunningFinalizeResumeResult>>>,
}

#[derive(Debug, Clone)]
enum RunningFinalizeResumeResult {
    Opened,
    WaitForFinalize,
    AttemptFinishedBeforePrepared,
    Failed(String),
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
    EnqueueRecap {
        session_id: SessionId,
        recap_end_index: usize,
    },
    RetryFinalize {
        target: SupervisorRetryTarget,
        #[serde(
            default = "default_notify_on_completion",
            skip_serializing_if = "is_true"
        )]
        notify_on_completion: bool,
    },
    ResumeFinalizing {
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        runtime_fingerprint: Option<SupervisorRuntimeFingerprint>,
    },
    Enqueued {
        job_id: String,
    },
    Retried {
        session_id: SessionId,
        job_id: String,
        previous_attempts: u32,
        manual_retries: u32,
    },
    ResumeTakeover {
        outcome: FinalizingResumeTakeover,
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
    Finalize {
        session_id: SessionId,
    },
    Recap {
        session_id: SessionId,
        recap_end_index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SupervisorJob {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<AgentId>,
    kind: SupervisorJobKind,
    status: SupervisorJobStatus,
    attempts: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    manual_retries: u32,
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

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

pub async fn ensure_supervisor_running(config: &SupervisorLaunchConfig) -> anyhow::Result<()> {
    ensure_supervisor_running_with(config, spawn_supervisor_process).await
}

async fn ensure_supervisor_running_with<F, Fut>(
    config: &SupervisorLaunchConfig,
    spawn: F,
) -> anyhow::Result<()>
where
    F: Fn(SupervisorLaunchConfig) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let paths = config.paths();
    if let Ok((build, fingerprint)) = supervisor_runtime_identity(&paths).await {
        if supervisor_runtime_matches(config, build.as_ref(), fingerprint.as_ref()) {
            return Ok(());
        }
    }

    let transition_guard = FileLockGuard::lock_exclusive_timeout(
        &paths.transition_lock_path,
        Duration::from_millis(DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS),
    )
    .await?;
    match supervisor_runtime_identity(&paths).await {
        Ok((build, fingerprint))
            if supervisor_runtime_matches(config, build.as_ref(), fingerprint.as_ref()) =>
        {
            Ok(())
        }
        Ok((previous_build, previous_fingerprint)) => {
            log::info!(
                target: "supervisor",
                "接管旧 supervisor: previous_build={previous_build:?}, previous_runtime={}, current_build={:?}, current_runtime={}",
                previous_fingerprint
                    .as_ref()
                    .map(SupervisorRuntimeFingerprint::short)
                    .unwrap_or("legacy"),
                BuildIdentity::current(),
                config.runtime_fingerprint.short()
            );
            let expected = preflight_supervisor_shutdown(&config.agent_home).await?;
            request_graceful_takeover_stop(&paths).await;
            let guard = shutdown_verified_supervisor_with_transition_lock(
                &config.agent_home,
                expected,
                transition_guard,
            )
            .await?;
            spawn(config.clone()).await?;
            let transition_guard = guard.release_process_lock();
            let ready = wait_for_current_supervisor(&paths, &config.runtime_fingerprint).await;
            drop(transition_guard);
            ready
        }
        Err(_) => {
            let process_guard = FileLockGuard::lock_exclusive_timeout(
                &paths.process_lock_path,
                Duration::from_millis(DEFAULT_SUPERVISOR_LOCK_TIMEOUT_MS),
            )
            .await?;
            match supervisor_runtime_identity(&paths).await {
                    Ok((build, fingerprint))
                        if supervisor_runtime_matches(
                            config,
                            build.as_ref(),
                            fingerprint.as_ref(),
                        ) => return Ok(()),
                    Ok((build, fingerprint)) => anyhow::bail!(
                        "supervisor 在持有进程锁时响应了 IPC，拒绝覆盖: build={build:?}, runtime={fingerprint:?}"
                    ),
                    Err(_) => {}
                }
            remove_stale_socket(&paths.socket_path).await;
            spawn(config.clone()).await?;
            drop(process_guard);
            let ready = wait_for_current_supervisor(&paths, &config.runtime_fingerprint).await;
            drop(transition_guard);
            ready
        }
    }
}

pub async fn enqueue_finalize(
    config: &SupervisorLaunchConfig,
    session_id: SessionId,
) -> anyhow::Result<String> {
    let request = SupervisorRequest::EnqueueFinalize {
        session_id,
        notify_on_completion: config.notify_on_finalize_completion,
    };
    match send_request(&config.paths(), request.clone()).await {
        Ok(SupervisorResponse::Enqueued { job_id }) => return Ok(job_id),
        Ok(SupervisorResponse::Error { message }) if message == SUPERVISOR_STOPPING_MESSAGE => {
            let _ = wait_for_supervisor_shutdown(&config.paths()).await;
        }
        Ok(SupervisorResponse::Error { message }) => anyhow::bail!(message),
        Ok(other) => anyhow::bail!("supervisor 返回了非 enqueue 响应: {other:?}"),
        Err(error) if ipc_error_indicates_unavailable(&error) => {}
        Err(error) => return Err(error.context("请求 supervisor enqueue 失败")),
    }

    ensure_supervisor_running(config).await?;
    match send_request(&config.paths(), request).await? {
        SupervisorResponse::Enqueued { job_id } => Ok(job_id),
        SupervisorResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("supervisor 返回了非 enqueue 响应: {other:?}"),
    }
}

pub async fn enqueue_recap(
    config: &SupervisorLaunchConfig,
    session_id: SessionId,
    recap_end_index: usize,
) -> anyhow::Result<String> {
    let request = SupervisorRequest::EnqueueRecap {
        session_id,
        recap_end_index,
    };
    match send_request(&config.paths(), request.clone()).await {
        Ok(SupervisorResponse::Enqueued { job_id }) => return Ok(job_id),
        Ok(SupervisorResponse::Error { message }) if message == SUPERVISOR_STOPPING_MESSAGE => {
            let _ = wait_for_supervisor_shutdown(&config.paths()).await;
        }
        Ok(SupervisorResponse::Error { message }) => anyhow::bail!(message),
        Ok(other) => anyhow::bail!("supervisor 返回了非 recap enqueue 响应: {other:?}"),
        Err(error) if ipc_error_indicates_unavailable(&error) => {}
        Err(error) => return Err(error.context("请求 supervisor recap enqueue 失败")),
    }

    ensure_supervisor_running(config).await?;
    match send_request(&config.paths(), request).await? {
        SupervisorResponse::Enqueued { job_id } => Ok(job_id),
        SupervisorResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("supervisor 返回了非 recap enqueue 响应: {other:?}"),
    }
}

pub async fn retry_finalize(
    config: &SupervisorLaunchConfig,
    target: SupervisorRetryTarget,
) -> anyhow::Result<SupervisorRetryReport> {
    ensure_supervisor_running(config).await?;
    match send_request(
        &config.paths(),
        SupervisorRequest::RetryFinalize {
            target,
            notify_on_completion: config.notify_on_finalize_completion,
        },
    )
    .await?
    {
        SupervisorResponse::Retried {
            session_id,
            job_id,
            previous_attempts,
            manual_retries,
        } => Ok(SupervisorRetryReport {
            session_id,
            job_id,
            previous_attempts,
            manual_retries,
        }),
        SupervisorResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("supervisor 返回了非 retry 响应: {other:?}"),
    }
}

pub async fn resume_finalizing_session(
    config: &SupervisorLaunchConfig,
    session_id: SessionId,
) -> anyhow::Result<FinalizingResumeTakeover> {
    ensure_supervisor_running(config).await?;
    let paths = config.paths();
    let outcome = request_resume_finalizing_takeover(config, &paths, session_id.clone()).await?;
    match outcome {
        FinalizingResumeTakeover::WaitForFinalize {
            job_id: Some(job_id),
        } => {
            wait_for_resume_finalize_job(&paths, &session_id, &job_id).await?;
            // Finalize 先提交 Closed，再提交 job Succeeded。等待观察到 Closed 后必须
            // 回到 lifecycle gate 对账旧 job，才能允许下一轮 Open。
            match request_resume_finalizing_takeover(config, &paths, session_id).await? {
                FinalizingResumeTakeover::ReopenClosed {
                    job_id: reconciled_job_id,
                } => Ok(FinalizingResumeTakeover::ReopenClosed {
                    job_id: reconciled_job_id.or(Some(job_id)),
                }),
                other => anyhow::bail!(
                    "supervisor returned an unexpected post-finalize resume outcome: {other:?}"
                ),
            }
        }
        other => Ok(other),
    }
}

/// Closed Resume 只在现有 Supervisor 可达时对账上一轮 Finalize job。
/// 明确没有 Supervisor 时沿用原 reopen；旧 job 会在下次 Supervisor 启动时按
/// 既有 Open/Closed stale recovery 收敛，不能反过来阻断普通 Closed Resume。
pub async fn reconcile_closed_session_for_resume(
    config: &SupervisorLaunchConfig,
    session_id: SessionId,
) -> anyhow::Result<()> {
    let paths = config.paths();
    let outcome = match request_resume_finalizing_takeover(config, &paths, session_id.clone()).await
    {
        Ok(outcome) => outcome,
        Err(error) if ipc_error_indicates_unavailable(&error) => {
            log::warn!(
                target: "supervisor",
                "Closed resume skipped stale Finalize reconciliation because Supervisor is unavailable: session={} error={error:#}",
                session_id
            );
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    match outcome {
        FinalizingResumeTakeover::ReopenClosed { .. } => Ok(()),
        other => {
            anyhow::bail!("supervisor returned an unexpected Closed resume outcome: {other:?}")
        }
    }
}

async fn request_resume_finalizing_takeover(
    config: &SupervisorLaunchConfig,
    paths: &SupervisorPaths,
    session_id: SessionId,
) -> anyhow::Result<FinalizingResumeTakeover> {
    // 该请求会原子改变 job/session，服务端不会随客户端 deadline 自动取消。必须等待同一
    // 连接返回权威结果，不能套用普通 1.5 秒 IPC 超时后把成功接管误报成失败。
    let outcome = match send_request_inner(
        paths,
        SupervisorRequest::ResumeFinalizing {
            session_id,
            notify_on_completion: config.notify_on_finalize_completion,
        },
    )
    .await?
    {
        SupervisorResponse::ResumeTakeover { outcome } => outcome,
        SupervisorResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("supervisor 返回了非 resume takeover 响应: {other:?}"),
    };
    Ok(outcome)
}

async fn wait_for_resume_finalize_job(
    paths: &SupervisorPaths,
    session_id: &SessionId,
    job_id: &str,
) -> anyhow::Result<()> {
    let mut next_liveness_check = Instant::now() + RESUME_FINALIZE_LIVENESS_INTERVAL;
    loop {
        let session_paths = SessionPaths::new(&paths.agent_home, session_id);
        let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
        if metadata.status == SessionStatus::Closed
            && metadata.closed_at.is_some()
            && metadata.finalized_at.is_some()
        {
            return Ok(());
        }
        let jobs = read_jobs(paths).await?;
        let job = jobs
            .iter()
            .find(|job| job.id == job_id)
            .with_context(|| format!("resume recovery job {job_id} disappeared"))?;
        match job.status {
            SupervisorJobStatus::Queued | SupervisorJobStatus::Running => {
                if Instant::now() >= next_liveness_check {
                    let supervisor_alive = matches!(
                        send_request(paths, SupervisorRequest::Status).await,
                        Ok(SupervisorResponse::Status { .. })
                    );
                    if !supervisor_alive {
                        log::warn!(
                            target: "supervisor",
                            "resume recovery wait lost supervisor: session={} job={}",
                            session_id,
                            job_id
                        );
                        anyhow::bail!(
                            "This session is still finalizing; wait for finalization to complete before resuming."
                        );
                    }
                    next_liveness_check = Instant::now() + RESUME_FINALIZE_LIVENESS_INTERVAL;
                }
                sleep(Duration::from_millis(100)).await;
            }
            SupervisorJobStatus::Succeeded => {
                anyhow::bail!(
                    "resume recovery job {job_id} succeeded but session {session_id} is not Closed"
                );
            }
            SupervisorJobStatus::Failed => anyhow::bail!(
                "This session is still finalizing; wait for finalization to complete before resuming."
            ),
        }
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

    let (runtime_state, pid, started_at, build, runtime_fingerprint) = match send_request(
        &paths,
        SupervisorRequest::Status,
    )
    .await
    {
        Ok(SupervisorResponse::Status {
            pid,
            started_at,
            build,
            runtime_fingerprint,
        }) => (
            SupervisorRuntimeState::Running,
            Some(pid),
            Some(started_at),
            build,
            runtime_fingerprint,
        ),
        Ok(SupervisorResponse::Error { message }) => anyhow::bail!(message),
        Ok(other) => anyhow::bail!("supervisor 返回了非 status 响应: {other:?}"),
        Err(err) if ipc_error_indicates_unavailable(&err) => (
            SupervisorRuntimeState::Stopped,
            read_pid_file(&paths).await?,
            None,
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
        runtime_fingerprint,
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

/// 解释一个 `Finalizing` session 为什么暂时不能 resume。
///
/// 历史成功 job 不属于当前未完成的 finalize；没有未成功 job 时，通过非阻塞探测
/// `finalize.lock` 区分真实执行中的前台 finalize 与需要恢复的孤儿状态。
pub async fn diagnose_finalizing_session(
    agent_home: &Path,
    session_id: &SessionId,
) -> anyhow::Result<FinalizingSessionDiagnostic> {
    let paths = SupervisorPaths::new(agent_home);
    let session_paths = SessionPaths::new(agent_home, session_id);
    let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml)
        .await
        .with_context(|| format!("读取 session {session_id} metadata 失败"))?;
    if metadata.status != SessionStatus::Finalizing {
        anyhow::bail!(
            "session {session_id} 当前状态为 {:?}，不是 Finalizing",
            metadata.status
        );
    }

    let jobs = read_jobs(&paths).await?;
    let matching = unresolved_finalize_jobs(&jobs, session_id);
    ensure_unique_unresolved_job(session_id, &matching)?;
    if let Some(job) = matching.first() {
        return Ok(match job.status {
            SupervisorJobStatus::Queued => FinalizingSessionDiagnostic::Queued {
                job_id: job.id.clone(),
            },
            SupervisorJobStatus::Running => FinalizingSessionDiagnostic::Running {
                job_id: job.id.clone(),
            },
            SupervisorJobStatus::Failed => FinalizingSessionDiagnostic::Failed {
                job_id: job.id.clone(),
            },
            SupervisorJobStatus::Succeeded => FinalizingSessionDiagnostic::Orphaned,
        });
    }

    match FileLockGuard::try_lock_exclusive(&session_paths.finalize_lock).await? {
        Some(_guard) => Ok(FinalizingSessionDiagnostic::Orphaned),
        None => Ok(FinalizingSessionDiagnostic::RunningWithoutJob),
    }
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
    let transition_lock = FileLockGuard::lock_exclusive_timeout(
        &paths.transition_lock_path,
        Duration::from_millis(DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS),
    )
    .await
    .context("获取 supervisor 接管锁失败")?;
    shutdown_verified_supervisor_with_transition_lock(agent_home, expected, transition_lock).await
}

async fn shutdown_verified_supervisor_with_transition_lock(
    agent_home: &Path,
    expected: VerifiedSupervisorState,
    transition_lock: FileLockGuard,
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
        _transition_lock: transition_lock,
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

async fn request_graceful_takeover_stop(paths: &SupervisorPaths) {
    match send_request(paths, SupervisorRequest::Stop).await {
        Ok(SupervisorResponse::Stopping) => {
            let _ = wait_for_supervisor_shutdown(paths).await;
        }
        Ok(SupervisorResponse::Error { message }) => {
            log::warn!(target: "supervisor", "旧 supervisor 拒绝 graceful takeover: {message}");
        }
        Ok(other) => {
            log::warn!(target: "supervisor", "旧 supervisor 返回了非 stop 响应: {other:?}");
        }
        Err(error) => {
            log::debug!(target: "supervisor", "旧 supervisor graceful takeover 不可用，将执行验证接管: {error:#}");
        }
    }
}

pub async fn run_supervisor(
    engine: SessionEngine,
    agent_home: PathBuf,
    runtime_fingerprint: SupervisorRuntimeFingerprint,
) -> anyhow::Result<()> {
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
    set_socket_owner_only(&paths.socket_path).await?;
    write_pid_file(&paths).await?;
    reconcile_stale_running_jobs(&paths).await?;
    append_supervisor_log(&paths, "supervisor started").await;

    let (notify_tx, notify_rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let accept_cancel = CancellationToken::new();
    let last_activity = Arc::new(AtomicU64::new(now_millis()));
    let running_job = Arc::new(AtomicBool::new(false));
    let stopping = Arc::new(AtomicBool::new(false));
    let lifecycle_gate = Arc::new(Mutex::new(()));
    let running_recap = Arc::new(Mutex::new(None));
    let running_finalize = Arc::new(Mutex::new(None));
    let shared_state = SupervisorSharedState {
        agent_id,
        notify_tx,
        stop_requested: cancel.clone(),
        last_activity: last_activity.clone(),
        started_at,
        runtime_fingerprint,
        stopping: stopping.clone(),
        lifecycle_gate,
        running_recap,
        running_finalize,
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
        if begin_idle_shutdown_if_due(&paths, &shared_state, &running_job, idle_timeout).await {
            append_supervisor_log(&paths, "supervisor idle timeout reached").await;
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

async fn begin_idle_shutdown_if_due(
    paths: &SupervisorPaths,
    shared: &SupervisorSharedState,
    running_job: &AtomicBool,
    idle_timeout: Duration,
) -> bool {
    let _guard = shared.lifecycle_gate.lock().await;
    if shared.stopping.load(Ordering::Acquire)
        || shared.stop_requested.is_cancelled()
        || running_job.load(Ordering::Relaxed)
        || has_queued_jobs(paths).await.unwrap_or(true)
    {
        return false;
    }
    let elapsed = now_millis().saturating_sub(shared.last_activity.load(Ordering::Relaxed));
    if elapsed < millis_u64(idle_timeout) {
        return false;
    }
    shared.stopping.store(true, Ordering::Release);
    shared.stop_requested.cancel();
    true
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
    validate_supervisor_peer(&stream)?;
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
                runtime_fingerprint: Some(shared.runtime_fingerprint.clone()),
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
                        message: SUPERVISOR_STOPPING_MESSAGE.into(),
                    }
                } else {
                    match enqueue_finalize_job(
                        paths,
                        &shared.agent_id,
                        session_id,
                        notify_on_completion,
                    )
                    .await
                    {
                        Ok(job) => {
                            request_same_session_recap_preemption(paths, shared, &job).await;
                            let _ = shared.notify_tx.send(());
                            SupervisorResponse::Enqueued { job_id: job.id }
                        }
                        Err(error) => SupervisorResponse::Error {
                            message: format!("{error:#}"),
                        },
                    }
                }
            }
            Ok(SupervisorRequest::EnqueueRecap {
                session_id,
                recap_end_index,
            }) => {
                let _guard = shared.lifecycle_gate.lock().await;
                if shared.stopping.load(Ordering::Acquire) || shared.stop_requested.is_cancelled() {
                    SupervisorResponse::Error {
                        message: SUPERVISOR_STOPPING_MESSAGE.into(),
                    }
                } else {
                    match create_recap_job(paths, &shared.agent_id, session_id, recap_end_index)
                        .await
                    {
                        Ok(job) => {
                            let _ = shared.notify_tx.send(());
                            SupervisorResponse::Enqueued { job_id: job.id }
                        }
                        Err(error) => SupervisorResponse::Error {
                            message: format!("{error:#}"),
                        },
                    }
                }
            }
            Ok(SupervisorRequest::RetryFinalize {
                target,
                notify_on_completion,
            }) => {
                let _guard = shared.lifecycle_gate.lock().await;
                if shared.stopping.load(Ordering::Acquire) || shared.stop_requested.is_cancelled() {
                    SupervisorResponse::Error {
                        message: SUPERVISOR_STOPPING_MESSAGE.into(),
                    }
                } else {
                    match retry_finalize_job(paths, &shared.agent_id, target, notify_on_completion)
                        .await
                    {
                        Ok(report) => {
                            let _ = shared.notify_tx.send(());
                            SupervisorResponse::Retried {
                                session_id: report.session_id,
                                job_id: report.job_id,
                                previous_attempts: report.previous_attempts,
                                manual_retries: report.manual_retries,
                            }
                        }
                        Err(error) => SupervisorResponse::Error {
                            message: format!("{error:#}"),
                        },
                    }
                }
            }
            Ok(SupervisorRequest::ResumeFinalizing {
                session_id,
                notify_on_completion,
            }) => {
                match resume_finalizing_takeover(paths, shared, session_id, notify_on_completion)
                    .await
                {
                    Ok(outcome) => SupervisorResponse::ResumeTakeover { outcome },
                    Err(error) => SupervisorResponse::Error {
                        message: format!("{error:#}"),
                    },
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

async fn resume_finalizing_takeover(
    paths: &SupervisorPaths,
    shared: &SupervisorSharedState,
    session_id: SessionId,
    notify_on_completion: bool,
) -> anyhow::Result<FinalizingResumeTakeover> {
    'takeover: loop {
        let lifecycle_guard = shared.lifecycle_gate.lock().await;
        if shared.stopping.load(Ordering::Acquire) || shared.stop_requested.is_cancelled() {
            anyhow::bail!(SUPERVISOR_STOPPING_MESSAGE);
        }
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml)
            .await
            .with_context(|| format!("读取 session {session_id} metadata 失败"))?;
        if metadata.id != session_id || metadata.agent_id != shared.agent_id {
            anyhow::bail!("session {session_id} does not belong to the current agent");
        }
        let jobs = read_jobs(paths).await?;
        if metadata.status == SessionStatus::Closed
            && metadata.closed_at.is_some()
            && metadata.finalized_at.is_some()
        {
            let matching = unresolved_finalize_jobs(&jobs, &session_id);
            ensure_unique_unresolved_job(&session_id, &matching)?;
            let reconciled_job_id = if let Some(job) = matching.first() {
                validate_job_agent(job, &shared.agent_id)?;
                let mut reconciled = (**job).clone();
                let previous_status = reconciled.status.clone();
                let now = Utc::now();
                reconciled.status = SupervisorJobStatus::Succeeded;
                reconciled.finished_at = Some(now);
                reconciled.updated_at = now;
                reconciled.last_error = None;
                write_job(paths, &reconciled).await?;
                append_supervisor_log(
                paths,
                format!(
                    "resume reconciled closed session finalize job {} session={} previous_status={}",
                    reconciled.id,
                    session_id,
                    previous_status.as_str()
                ),
            )
            .await;
                Some(reconciled.id)
            } else {
                None
            };
            return Ok(FinalizingResumeTakeover::ReopenClosed {
                job_id: reconciled_job_id,
            });
        }
        if metadata.status == SessionStatus::Open
            && metadata.closed_at.is_none()
            && metadata.finalized_at.is_none()
        {
            return Ok(FinalizingResumeTakeover::Opened { job_id: None });
        }
        if metadata.status != SessionStatus::Finalizing
            || metadata.closed_at.is_some()
            || metadata.finalized_at.is_some()
        {
            anyhow::bail!("session {session_id} is not a resumable Finalizing session");
        }
        let recap_end_index = metadata.message_count;
        let has_checkpoint = SessionHandle::new(metadata.clone(), session_paths.clone())
            .read_finalize_checkpoint()
            .await?
            .is_some_and(|checkpoint| {
                finalize_checkpoint_covers_pending_range(
                    &checkpoint,
                    metadata.recapped_until,
                    metadata.message_count,
                )
            });
        let matching = unresolved_finalize_jobs(&jobs, &session_id);
        ensure_unique_unresolved_job(&session_id, &matching)?;

        let Some(job) = matching.first().copied() else {
            let Some(_finalize_guard) =
                FileLockGuard::try_lock_exclusive(&session_paths.finalize_lock).await?
            else {
                drop(lifecycle_guard);
                return Ok(FinalizingResumeTakeover::WaitForFinalize { job_id: None });
            };
            if has_checkpoint {
                let recovery = create_resume_recovery_finalize_job(
                    paths,
                    &shared.agent_id,
                    session_id.clone(),
                    notify_on_completion,
                )
                .await?;
                append_supervisor_log(
                    paths,
                    format!(
                        "resume created orphan checkpoint recovery finalize job {} session={}",
                        recovery.id, session_id
                    ),
                )
                .await;
                let _ = shared.notify_tx.send(());
                return Ok(FinalizingResumeTakeover::WaitForFinalize {
                    job_id: Some(recovery.id),
                });
            }

            let recap_job = if metadata.recapped_until < recap_end_index {
                Some(
                    create_recap_job(paths, &shared.agent_id, session_id.clone(), recap_end_index)
                        .await?,
                )
            } else {
                None
            };
            if let Err(open_error) =
                open_finalizing_session_for_resume(paths, &shared.agent_id, &session_id).await
            {
                if let Some(recap_job) = recap_job.as_ref() {
                    let recovery = recap_job_as_finalize_recovery(
                        recap_job,
                        &session_id,
                        notify_on_completion,
                        Utc::now(),
                    );
                    write_job(paths, &recovery).await?;
                }
                return Err(open_error);
            }
            append_supervisor_log(
                paths,
                format!(
                    "resume reopened orphan finalizing session {} target={} recap_job={}",
                    session_id,
                    recap_end_index,
                    recap_job
                        .as_ref()
                        .map(|job| job.id.as_str())
                        .unwrap_or("none")
                ),
            )
            .await;
            if recap_job.is_some() {
                let _ = shared.notify_tx.send(());
            }
            return Ok(FinalizingResumeTakeover::Opened {
                job_id: recap_job.map(|job| job.id),
            });
        };
        validate_job_agent(job, &shared.agent_id)?;

        // worker 在 lifecycle gate 内先登记 Running Finalize，随后才把 job YAML
        // 从 Queued 改为 Running。这个极短窗口也必须由 worker 独占转换。
        let running = shared
            .running_finalize
            .lock()
            .await
            .clone()
            .filter(|running| running.job_id == job.id && running.session_id == session_id);
        let recovered_running_job;
        let recovered_unregistered_running =
            job.status == SupervisorJobStatus::Running && running.is_none();
        let job = if recovered_unregistered_running {
            // 磁盘仍为 Running 但已无匹配进程内登记，只可能是 worker 在终态
            // job 写入前后异常退出。此时不能返回 Wait：活着的 Supervisor 不会
            // 再选择 Running job，等待方也就永远不会收敛。复用既有 stale-running
            // attempt 语义后再继续本次接管。
            recovered_running_job = recover_stale_running_job(
            job,
            metadata.status,
            "recovered unregistered running finalize job during resume takeover".into(),
            "unregistered running finalize job exhausted supervisor retry budget before resume takeover"
                .into(),
        );
            write_job(paths, &recovered_running_job).await?;
            append_supervisor_log(
            paths,
            format!(
                "resume recovered unregistered running finalize job {} session={} status={} attempts={}",
                recovered_running_job.id,
                session_id,
                recovered_running_job.status.as_str(),
                recovered_running_job.attempts
            ),
        )
        .await;
            &recovered_running_job
        } else {
            job
        };

        if has_checkpoint {
            if job.status == SupervisorJobStatus::Failed {
                let recovered = reset_finalize_job_for_resume_recovery(job);
                write_job(paths, &recovered).await?;
                append_supervisor_log(
                paths,
                format!(
                    "resume reset checkpoint recovery finalize job {} session={} previous_attempts={}",
                    job.id, session_id, job.attempts
                ),
            )
            .await;
                let _ = shared.notify_tx.send(());
            } else if recovered_unregistered_running && job.status == SupervisorJobStatus::Queued {
                let _ = shared.notify_tx.send(());
            }
            return Ok(FinalizingResumeTakeover::WaitForFinalize {
                job_id: Some(job.id.clone()),
            });
        }

        if let Some(running) = running {
            let mut result_rx = running.resume_result_tx.subscribe();
            *running.resume_target.lock().await = Some(recap_end_index);
            let preemption_requested = running.preemption.request_before_prepared().await;
            if !preemption_requested && !running.preemption.finished_before_prepared().await {
                return Ok(FinalizingResumeTakeover::WaitForFinalize {
                    job_id: Some(job.id.clone()),
                });
            }
            if preemption_requested {
                append_supervisor_log(
                paths,
                format!(
                    "finalize job {} preemption requested by same-session resume session={} before Prepared",
                    job.id, session_id
                ),
            )
            .await;
            }
            let job_id = job.id.clone();
            // 等待方不能持有 Sender；worker 清除 running 登记后，异常退出至少会关闭
            // channel，让 Resume 返回失败而不是永久停在 Resuming。
            drop(running);
            drop(lifecycle_guard);
            result_rx
                .changed()
                .await
                .context("running finalize ended without a resume takeover result")?;
            let resume_result = result_rx.borrow().clone();
            return match resume_result {
                Some(RunningFinalizeResumeResult::Opened) => Ok(FinalizingResumeTakeover::Opened {
                    job_id: Some(job_id),
                }),
                Some(RunningFinalizeResumeResult::WaitForFinalize) => {
                    Ok(FinalizingResumeTakeover::WaitForFinalize {
                        job_id: Some(job_id),
                    })
                }
                Some(RunningFinalizeResumeResult::AttemptFinishedBeforePrepared) => {
                    continue 'takeover;
                }
                Some(RunningFinalizeResumeResult::Failed(message)) => anyhow::bail!(message),
                None => anyhow::bail!("running finalize returned an empty resume takeover result"),
            };
        }

        return match job.status {
            SupervisorJobStatus::Queued | SupervisorJobStatus::Failed => {
                let job_id = job.id.clone();
                convert_finalize_job_and_open(paths, &shared.agent_id, job, recap_end_index)
                    .await?;
                let _ = shared.notify_tx.send(());
                Ok(FinalizingResumeTakeover::Opened {
                    job_id: Some(job_id),
                })
            }
            SupervisorJobStatus::Running => {
                anyhow::bail!("unregistered finalize job {} remained running", job.id)
            }
            SupervisorJobStatus::Succeeded => {
                anyhow::bail!(
                    "unresolved finalize selection included succeeded job {}",
                    job.id
                )
            }
        };
    }
}

fn converted_recap_job(
    original: &SupervisorJob,
    session_id: &SessionId,
    recap_end_index: usize,
    updated_at: DateTime<Utc>,
) -> SupervisorJob {
    SupervisorJob {
        id: original.id.clone(),
        agent_id: original.agent_id.clone(),
        kind: SupervisorJobKind::Recap {
            session_id: session_id.clone(),
            recap_end_index,
        },
        status: SupervisorJobStatus::Queued,
        attempts: 0,
        manual_retries: 0,
        created_at: original.created_at,
        updated_at,
        started_at: None,
        finished_at: None,
        last_error: None,
        notify_on_completion: false,
    }
}

fn reset_finalize_job_for_resume_recovery(original: &SupervisorJob) -> SupervisorJob {
    SupervisorJob {
        status: SupervisorJobStatus::Queued,
        attempts: 0,
        updated_at: Utc::now(),
        started_at: None,
        finished_at: None,
        last_error: None,
        ..original.clone()
    }
}

fn recap_job_as_finalize_recovery(
    original: &SupervisorJob,
    session_id: &SessionId,
    notify_on_completion: bool,
    updated_at: DateTime<Utc>,
) -> SupervisorJob {
    SupervisorJob {
        id: original.id.clone(),
        agent_id: original.agent_id.clone(),
        kind: SupervisorJobKind::Finalize {
            session_id: session_id.clone(),
        },
        status: SupervisorJobStatus::Queued,
        attempts: 0,
        manual_retries: 0,
        created_at: original.created_at,
        updated_at,
        started_at: None,
        finished_at: None,
        last_error: None,
        notify_on_completion,
    }
}

async fn open_finalizing_session_for_resume(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: &SessionId,
) -> anyhow::Result<()> {
    let session_paths = SessionPaths::new(&paths.agent_home, session_id);
    let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
    if metadata.agent_id != *agent_id {
        anyhow::bail!("session {session_id} belongs to a different agent");
    }
    let mut session = SessionHandle::new(metadata, session_paths);
    session
        .mark_open_after_finalize_takeover(Utc::now())
        .await?;
    Ok(())
}

async fn convert_finalize_job_and_open(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    original: &SupervisorJob,
    recap_end_index: usize,
) -> anyhow::Result<SupervisorJob> {
    let session_id = finalize_job_session_id(original)
        .context("resume takeover target is not a finalize job")?
        .clone();
    let converted = converted_recap_job(original, &session_id, recap_end_index, Utc::now());
    write_job(paths, &converted).await?;

    if let Err(open_error) = open_finalizing_session_for_resume(paths, agent_id, &session_id).await
    {
        let restore_result = write_job(paths, original).await;
        return match restore_result {
            Ok(()) => Err(open_error),
            Err(restore_error) => anyhow::bail!(
                "opening session after recap conversion failed: {open_error}; restoring finalize job failed: {restore_error:#}"
            ),
        };
    }
    append_supervisor_log(
        paths,
        format!(
            "finalize job {} session={} status={} target={} converted by resume before Prepared",
            original.id,
            session_id,
            original.status.as_str(),
            recap_end_index
        ),
    )
    .await;
    Ok(converted)
}

async fn request_same_session_recap_preemption(
    paths: &SupervisorPaths,
    shared: &SupervisorSharedState,
    finalize_job: &SupervisorJob,
) {
    let SupervisorJobKind::Finalize { session_id } = &finalize_job.kind else {
        return;
    };
    let running = shared.running_recap.lock().await.clone();
    let Some(running) = running.filter(|running| &running.session_id == session_id) else {
        return;
    };
    if running.preemption.request_before_prepared().await {
        append_supervisor_log(
            paths,
            format!(
                "recap job {} preemption requested by same-session finalize job {} session={} before Prepared",
                running.job_id, finalize_job.id, session_id
            ),
        )
        .await;
    }
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
            let _guard = shared.lifecycle_gate.lock().await;
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
            let job_id = job.id.clone();
            let recap_preemption = matches!(&job.kind, SupervisorJobKind::Recap { .. })
                .then(|| Arc::new(SessionRecapPreemptionControl::new()));
            let (finalize_preemption, finalize_resume_target, finalize_result_tx) =
                if matches!(&job.kind, SupervisorJobKind::Finalize { .. }) {
                    let preemption = Arc::new(SessionFinalizePreemptionControl::new());
                    let resume_target = Arc::new(Mutex::new(None));
                    let (result_tx, _result_rx) = watch::channel(None);
                    (Some(preemption), Some(resume_target), Some(result_tx))
                } else {
                    (None, None, None)
                };
            if let (SupervisorJobKind::Recap { session_id, .. }, Some(preemption)) =
                (&job.kind, recap_preemption.as_ref())
            {
                *shared.running_recap.lock().await = Some(RunningRecap {
                    job_id: job_id.clone(),
                    session_id: session_id.clone(),
                    preemption: Arc::clone(preemption),
                });
            }
            if let (
                SupervisorJobKind::Finalize { session_id },
                Some(preemption),
                Some(resume_target),
                Some(result_tx),
            ) = (
                &job.kind,
                finalize_preemption.as_ref(),
                finalize_resume_target.as_ref(),
                finalize_result_tx.as_ref(),
            ) {
                *shared.running_finalize.lock().await = Some(RunningFinalize {
                    job_id: job_id.clone(),
                    session_id: session_id.clone(),
                    preemption: Arc::clone(preemption),
                    resume_target: Arc::clone(resume_target),
                    resume_result_tx: result_tx.clone(),
                });
            }
            running_job.store(true, Ordering::Relaxed);
            drop(_guard);
            shared.last_activity.store(now_millis(), Ordering::Relaxed);
            let controls = RunningJobControls {
                recap_preemption: recap_preemption.clone(),
                finalize_preemption: finalize_preemption.clone(),
                finalize_resume_target: finalize_resume_target.clone(),
                finalize_result_tx: finalize_result_tx.clone(),
            };
            let runner_error = match run_job(&engine, &paths, job, controls, &shared).await {
                Ok(()) => None,
                Err(err) => {
                    append_supervisor_log(&paths, format!("job runner error: {err:#}")).await;
                    Some(err)
                }
            };
            let _guard = shared.lifecycle_gate.lock().await;
            let requeued = if let Some(error) = runner_error.as_ref() {
                match reconcile_running_job_after_runner_error(&paths, &job_id, error).await {
                    Ok(requeued) => requeued,
                    Err(recovery_error) => {
                        append_supervisor_log(
                            &paths,
                            format!(
                                "job {} runner error recovery failed: {recovery_error:#}",
                                job_id
                            ),
                        )
                        .await;
                        false
                    }
                }
            } else {
                has_queued_jobs(&paths).await.unwrap_or(false)
            };
            if recap_preemption.is_some() {
                let mut running_recap = shared.running_recap.lock().await;
                if running_recap
                    .as_ref()
                    .is_some_and(|running| running.job_id == job_id)
                {
                    *running_recap = None;
                }
            }
            if finalize_preemption.is_some() {
                let mut running_finalize = shared.running_finalize.lock().await;
                if running_finalize
                    .as_ref()
                    .is_some_and(|running| running.job_id == job_id)
                {
                    *running_finalize = None;
                }
            }
            if let (Some(error), Some(result_tx)) =
                (runner_error.as_ref(), finalize_result_tx.as_ref())
            {
                let _ = result_tx.send(Some(RunningFinalizeResumeResult::Failed(format!(
                    "This session is still finalizing; wait for finalization to complete before resuming. ({error:#})"
                ))));
            }
            running_job.store(false, Ordering::Relaxed);
            drop(_guard);
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
    controls: RunningJobControls,
    shared: &SupervisorSharedState,
) -> anyhow::Result<()> {
    let RunningJobControls {
        recap_preemption,
        finalize_preemption,
        finalize_resume_target,
        finalize_result_tx,
    } = controls;
    job.status = SupervisorJobStatus::Running;
    job.attempts = job.attempts.saturating_add(1);
    job.started_at = Some(Utc::now());
    job.updated_at = Utc::now();
    job.last_error = None;
    write_job(paths, &job).await?;

    let kind = job.kind.clone();
    let mut finalize_finished_before_prepared = false;
    let result = match &kind {
        SupervisorJobKind::Finalize { session_id } => {
            append_supervisor_log(paths, format!("finalize job {} started", job.id)).await;
            let preemption = finalize_preemption
                .as_ref()
                .context("Finalize job 缺少 Prepared 前抢占控制器")?;
            let outcome = engine
                .finalize_existing_session_once_with_preemption(
                    session_id,
                    |event| {
                        log_supervisor_session_event(&job.id, &event);
                    },
                    Arc::clone(preemption),
                )
                .await;
            let _ = preemption.finish().await;
            finalize_finished_before_prepared = preemption.finished_before_prepared().await;
            match outcome {
                Ok(SessionFinalizeOnceOutcome::Completed(report)) => {
                    if let Some(result_tx) = &finalize_result_tx {
                        let _ = result_tx.send(Some(RunningFinalizeResumeResult::WaitForFinalize));
                    }
                    Ok(report)
                }
                Ok(SessionFinalizeOnceOutcome::PreemptedBeforePrepared) => {
                    let _guard = shared.lifecycle_gate.lock().await;
                    let recap_end_index = *finalize_resume_target
                        .as_ref()
                        .context("Finalize resume preemption target is unavailable")?
                        .lock()
                        .await;
                    let recap_end_index = recap_end_index
                        .context("Finalize resume preemption target was not frozen")?;
                    match convert_finalize_job_and_open(
                        paths,
                        &shared.agent_id,
                        &job,
                        recap_end_index,
                    )
                    .await
                    {
                        Ok(_) => {
                            if let Some(result_tx) = &finalize_result_tx {
                                let _ = result_tx.send(Some(RunningFinalizeResumeResult::Opened));
                            }
                            let _ = shared.notify_tx.send(());
                            return Ok(());
                        }
                        Err(convert_error) => {
                            let message = format!(
                                "This session is still finalizing; wait for finalization to complete before resuming. ({convert_error:#})"
                            );
                            if let Some(result_tx) = &finalize_result_tx {
                                let _ = result_tx
                                    .send(Some(RunningFinalizeResumeResult::Failed(message)));
                            }
                            append_supervisor_log(
                                paths,
                                format!(
                                    "running finalize job {} resume conversion failed; continuing same attempt: {convert_error:#}",
                                    job.id
                                ),
                            )
                            .await;
                        }
                    }
                    drop(_guard);
                    engine
                        .finalize_existing_session_once(session_id, |event| {
                            log_supervisor_session_event(&job.id, &event);
                        })
                        .await
                }
                Err(error) => Err(error),
            }
        }
        SupervisorJobKind::Recap {
            session_id,
            recap_end_index,
        } => {
            append_supervisor_log(
                paths,
                format!(
                    "recap job {} started session={} target={} attempt={}",
                    job.id, session_id, recap_end_index, job.attempts
                ),
            )
            .await;
            let preemption = recap_preemption
                .as_ref()
                .context("Recap job 缺少 Prepared 前抢占控制器")?;
            engine
                .recap_existing_session_until_with_preemption(
                    session_id,
                    *recap_end_index,
                    Arc::clone(preemption),
                )
                .await
        }
    };

    let recap_was_preempted = match recap_preemption.as_ref() {
        Some(preemption) => preemption.finish().await,
        None => false,
    };
    let result = if recap_was_preempted {
        Ok(SessionFinalizeReport::default())
    } else {
        result
    };

    match result {
        Ok(report) => {
            job.status = SupervisorJobStatus::Succeeded;
            job.finished_at = Some(Utc::now());
            job.updated_at = Utc::now();
            write_job(paths, &job).await?;
            match &kind {
                SupervisorJobKind::Finalize { .. } => {
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
                            report.new_claim_ids.len() + report.updated_claim_ids.len(),
                            report.new_dispute_ids.len()
                        ),
                    )
                    .await;
                }
                SupervisorJobKind::Recap {
                    session_id,
                    recap_end_index,
                } => {
                    let metadata = read_yaml::<SessionMetadata>(
                        &SessionPaths::new(&paths.agent_home, session_id).session_yaml,
                    )
                    .await
                    .ok();
                    let subsumed =
                        recap_report_was_subsumed_by_finalize(&report, metadata.as_ref());
                    if recap_was_preempted {
                        append_supervisor_log(
                            paths,
                            format!(
                                "recap job {} succeeded no-op: preempted before Prepared and subsumed by finalize session={} target={}",
                                job.id, session_id, recap_end_index
                            ),
                        )
                        .await;
                    } else if subsumed {
                        append_supervisor_log(
                            paths,
                            format!(
                                "recap job {} succeeded no-op: subsumed by finalize session={} target={}",
                                job.id, session_id, recap_end_index
                            ),
                        )
                        .await;
                    } else {
                        let cursor = metadata
                            .as_ref()
                            .map(|metadata| metadata.recapped_until)
                            .unwrap_or(*recap_end_index);
                        append_supervisor_log(
                            paths,
                            format!(
                                "recap job {} succeeded session={} target={} recapped_until={} claims={} disputes={}",
                                job.id,
                                session_id,
                                recap_end_index,
                                cursor,
                                report.new_claim_ids.len() + report.updated_claim_ids.len(),
                                report.new_dispute_ids.len()
                            ),
                        )
                        .await;
                    }
                }
            }
        }
        Err(err) => {
            let message = err.to_string();
            apply_job_attempt_failure(&mut job, message.clone());
            write_job(paths, &job).await?;
            if job.status == SupervisorJobStatus::Failed {
                if matches!(&kind, SupervisorJobKind::Finalize { .. }) && job.notify_on_completion {
                    notify_finalize_failure(paths, &job, &message).await;
                }
                append_supervisor_log(
                    paths,
                    format!("{} job {} failed: {message}", job_kind_label(&kind), job.id),
                )
                .await;
            } else {
                append_supervisor_log(
                    paths,
                    format!(
                        "{} job {} failed attempt {}/{} and was requeued: {message}",
                        job_kind_label(&kind),
                        job.id,
                        job.attempts,
                        DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS
                    ),
                )
                .await;
            }
            if let Some(result_tx) = finalize_result_tx.as_ref() {
                // 转换失败已经向等待方发送了 authoritative Failed；随后继续执行的
                // 原 Finalize 即使也失败，也不能把该结果覆盖成可重新接管。
                if result_tx.borrow().is_none() {
                    if finalize_finished_before_prepared {
                        let _guard = shared.lifecycle_gate.lock().await;
                        let mut running_finalize = shared.running_finalize.lock().await;
                        if running_finalize
                            .as_ref()
                            .is_some_and(|running| running.job_id == job.id)
                        {
                            *running_finalize = None;
                        }
                        let _ = result_tx.send(Some(
                            RunningFinalizeResumeResult::AttemptFinishedBeforePrepared,
                        ));
                    } else {
                        let _ = result_tx.send(Some(RunningFinalizeResumeResult::Failed(format!(
                            "This session is still finalizing; wait for finalization to complete before resuming. ({err:#})"
                        ))));
                    }
                }
            }
        }
    }
    Ok(())
}

fn apply_job_attempt_failure(job: &mut SupervisorJob, message: String) {
    let now = Utc::now();
    if job.attempts < DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS {
        job.status = SupervisorJobStatus::Queued;
        job.finished_at = None;
    } else {
        job.status = SupervisorJobStatus::Failed;
        job.finished_at = Some(now);
    }
    job.updated_at = now;
    job.last_error = Some(message);
}

fn recover_stale_running_job(
    original: &SupervisorJob,
    session_status: SessionStatus,
    retry_message: String,
    exhausted_message: String,
) -> SupervisorJob {
    let mut recovered = original.clone();
    let now = Utc::now();
    match (&recovered.kind, session_status) {
        (SupervisorJobKind::Finalize { .. }, SessionStatus::Closed | SessionStatus::Open)
        | (SupervisorJobKind::Recap { .. }, SessionStatus::Finalizing | SessionStatus::Closed) => {
            recovered.status = SupervisorJobStatus::Succeeded;
            recovered.finished_at = Some(now);
            recovered.last_error = None;
        }
        (SupervisorJobKind::Finalize { .. }, SessionStatus::Finalizing)
        | (SupervisorJobKind::Recap { .. }, SessionStatus::Open) => {
            if recovered.attempts >= DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS {
                recovered.status = SupervisorJobStatus::Failed;
                recovered.finished_at = Some(now);
                recovered.last_error = Some(exhausted_message);
            } else {
                recovered.status = SupervisorJobStatus::Queued;
                recovered.finished_at = None;
                recovered.last_error = Some(retry_message);
            }
        }
    }
    recovered.updated_at = now;
    recovered
}

async fn reconcile_running_job_after_runner_error(
    paths: &SupervisorPaths,
    job_id: &str,
    error: &anyhow::Error,
) -> anyhow::Result<bool> {
    let jobs = read_jobs(paths).await?;
    let job = jobs
        .iter()
        .find(|job| job.id == job_id)
        .with_context(|| format!("runner error recovery job {job_id} disappeared"))?;
    if job.status != SupervisorJobStatus::Running {
        return Ok(job.status == SupervisorJobStatus::Queued);
    }

    let session_id = job_session_id_ref(job);
    let session_status = read_yaml::<SessionMetadata>(
        &SessionPaths::new(&paths.agent_home, session_id).session_yaml,
    )
    .await
    .with_context(|| {
        format!("读取 runner error job {job_id} 的 session {session_id} metadata 失败")
    })?
    .status;
    let recovered = recover_stale_running_job(
        job,
        session_status,
        format!("recovered running job after runner error: {error:#}"),
        format!("running job exhausted supervisor retry budget after runner error: {error:#}"),
    );
    write_job(paths, &recovered).await?;
    append_supervisor_log(
        paths,
        format!(
            "job {} runner error state reconciled status={} attempts={}",
            recovered.id,
            recovered.status.as_str(),
            recovered.attempts
        ),
    )
    .await;
    Ok(recovered.status == SupervisorJobStatus::Queued)
}

fn recap_report_was_subsumed_by_finalize(
    report: &SessionFinalizeReport,
    metadata: Option<&SessionMetadata>,
) -> bool {
    !report.advanced_recapped_until
        && metadata.is_some_and(|metadata| {
            metadata.status != SessionStatus::Open
                || metadata.finalized_at.is_some()
                || metadata.closed_at.is_some()
        })
}

fn job_kind_label(kind: &SupervisorJobKind) -> &'static str {
    match kind {
        SupervisorJobKind::Finalize { .. } => "finalize",
        SupervisorJobKind::Recap { .. } => "recap",
    }
}

async fn enqueue_finalize_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    notify_on_completion: bool,
) -> anyhow::Result<SupervisorJob> {
    validate_finalizing_session(paths, agent_id, &session_id).await?;
    let jobs = read_jobs(paths).await?;
    let matching = unresolved_finalize_jobs(&jobs, &session_id);
    ensure_unique_unresolved_job(&session_id, &matching)?;
    if let Some(job) = matching.first() {
        validate_job_agent(job, agent_id)?;
        return match job.status {
            SupervisorJobStatus::Queued | SupervisorJobStatus::Running => Ok((**job).clone()),
            SupervisorJobStatus::Failed => anyhow::bail!(
                "session {session_id} 的 finalize job {} 已失败；请使用 `acn supervisor retry {session_id}`",
                job.id
            ),
            // `matching` 已排除成功 job；保留穷尽分支避免隐藏未来状态扩展。
            SupervisorJobStatus::Succeeded => anyhow::bail!(
                "session {session_id} 的未成功 job 过滤结果包含 succeeded job {}",
                job.id
            ),
        };
    }

    create_finalize_job(paths, agent_id, session_id, notify_on_completion).await
}

async fn retry_finalize_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    target: SupervisorRetryTarget,
    notify_on_completion: bool,
) -> anyhow::Result<SupervisorRetryReport> {
    let jobs = read_jobs(paths).await?;
    let (session_id, existing_job) = match target {
        SupervisorRetryTarget::Session { session_id } => {
            let matching = unresolved_finalize_jobs(&jobs, &session_id);
            ensure_unique_unresolved_job(&session_id, &matching)?;
            (session_id, matching.first().map(|job| (**job).clone()))
        }
        SupervisorRetryTarget::Job { job_id } => {
            let job = jobs
                .iter()
                .find(|job| job.id == job_id)
                .with_context(|| format!("未找到 supervisor job {job_id}"))?;
            let session_id = finalize_job_session_id(job)
                .with_context(|| format!("supervisor job {job_id} 不是 finalize job"))?
                .clone();
            let matching = unresolved_finalize_jobs(&jobs, &session_id);
            ensure_unique_unresolved_job(&session_id, &matching)?;
            (session_id, Some(job.clone()))
        }
    };

    validate_finalizing_session(paths, agent_id, &session_id).await?;
    let mut finalize_guard = None;
    let mut job = match existing_job {
        Some(job) => job,
        None => {
            let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
            let guard = FileLockGuard::try_lock_exclusive(&session_paths.finalize_lock)
                .await?
                .with_context(|| format!("session {session_id} 正在 finalize，无需 retry"))?;
            finalize_guard = Some(guard);

            // 获取 finalize 锁后重新检查持久状态，避免把刚开始的真实 finalize 误判为孤儿。
            validate_finalizing_session(paths, agent_id, &session_id).await?;
            let refreshed_jobs = read_jobs(paths).await?;
            let matching = unresolved_finalize_jobs(&refreshed_jobs, &session_id);
            ensure_unique_unresolved_job(&session_id, &matching)?;
            match matching.first() {
                Some(job) => (**job).clone(),
                None => {
                    let job = create_recovery_finalize_job(
                        paths,
                        agent_id,
                        session_id.clone(),
                        notify_on_completion,
                    )
                    .await?;
                    append_supervisor_log(
                        paths,
                        format!(
                            "manual retry created orphan finalize job {} session={} manual_retries={}",
                            job.id, session_id, job.manual_retries
                        ),
                    )
                    .await;
                    return Ok(SupervisorRetryReport {
                        session_id,
                        job_id: job.id,
                        previous_attempts: 0,
                        manual_retries: job.manual_retries,
                    });
                }
            }
        }
    };

    validate_job_agent(&job, agent_id)?;
    match job.status {
        SupervisorJobStatus::Failed => {}
        SupervisorJobStatus::Queued | SupervisorJobStatus::Running => anyhow::bail!(
            "session {session_id} 的 finalize job {} 当前为 {}，无需 retry",
            job.id,
            job.status.as_str()
        ),
        SupervisorJobStatus::Succeeded => anyhow::bail!(
            "session {session_id} 的 finalize job {} 已成功，不能 retry",
            job.id
        ),
    }

    let previous_attempts = job.attempts;
    let previous_error = job.last_error.clone().unwrap_or_else(|| "-".into());
    job.status = SupervisorJobStatus::Queued;
    job.attempts = 0;
    job.manual_retries = job.manual_retries.saturating_add(1);
    job.updated_at = Utc::now();
    job.started_at = None;
    job.finished_at = None;
    write_job(paths, &job).await?;
    drop(finalize_guard);
    append_supervisor_log(
        paths,
        format!(
            "manual retry queued finalize job {} session={} previous_attempts={} manual_retries={} previous_error={}",
            job.id, session_id, previous_attempts, job.manual_retries, previous_error
        ),
    )
    .await;
    Ok(SupervisorRetryReport {
        session_id,
        job_id: job.id,
        previous_attempts,
        manual_retries: job.manual_retries,
    })
}

async fn validate_finalizing_session(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: &SessionId,
) -> anyhow::Result<SessionMetadata> {
    let session_paths = SessionPaths::new(&paths.agent_home, session_id);
    let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml)
        .await
        .with_context(|| format!("读取 session {session_id} metadata 失败"))?;
    if metadata.id != *session_id {
        anyhow::bail!(
            "session metadata id {} 与请求的 {session_id} 不一致",
            metadata.id
        );
    }
    if metadata.agent_id != *agent_id {
        anyhow::bail!(
            "session {session_id} 属于 agent {}，不是当前 agent {agent_id}",
            metadata.agent_id
        );
    }
    if metadata.status != SessionStatus::Finalizing
        || metadata.finalized_at.is_some()
        || metadata.closed_at.is_some()
    {
        anyhow::bail!(
            "session {session_id} 当前状态为 {:?}，只有未完成的 Finalizing session 可以 retry",
            metadata.status
        );
    }
    Ok(metadata)
}

fn validate_job_agent(job: &SupervisorJob, agent_id: &AgentId) -> anyhow::Result<()> {
    if let Some(job_agent_id) = &job.agent_id {
        if job_agent_id != agent_id {
            anyhow::bail!(
                "supervisor job {} 属于 agent {job_agent_id}，不是当前 agent {agent_id}",
                job.id
            );
        }
    }
    Ok(())
}

fn unresolved_finalize_jobs<'a>(
    jobs: &'a [SupervisorJob],
    session_id: &SessionId,
) -> Vec<&'a SupervisorJob> {
    jobs.iter()
        .filter(|job| {
            finalize_job_session_id(job) == Some(session_id)
                && job.status != SupervisorJobStatus::Succeeded
        })
        .collect()
}

fn ensure_unique_unresolved_job(
    session_id: &SessionId,
    jobs: &[&SupervisorJob],
) -> anyhow::Result<()> {
    if jobs.len() > 1 {
        anyhow::bail!(unresolved_job_invariant_error(session_id, jobs));
    }
    Ok(())
}

fn unresolved_job_invariant_error(session_id: &SessionId, jobs: &[&SupervisorJob]) -> String {
    let mut ids = jobs.iter().map(|job| job.id.as_str()).collect::<Vec<_>>();
    ids.sort_unstable();
    format!(
        "session {session_id} 存在多个未成功 finalize job，违反唯一性约束: {}",
        ids.join(", ")
    )
}

fn finalize_job_session_id(job: &SupervisorJob) -> Option<&SessionId> {
    match &job.kind {
        SupervisorJobKind::Finalize { session_id } => Some(session_id),
        SupervisorJobKind::Recap { .. } => None,
    }
}

fn job_session_id_ref(job: &SupervisorJob) -> &SessionId {
    match &job.kind {
        SupervisorJobKind::Finalize { session_id }
        | SupervisorJobKind::Recap { session_id, .. } => session_id,
    }
}

async fn create_finalize_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    notify_on_completion: bool,
) -> anyhow::Result<SupervisorJob> {
    create_finalize_job_record(
        paths,
        agent_id,
        session_id,
        FinalizeJobInitialState {
            notify_on_completion,
            manual_retries: 0,
            last_error: None,
        },
        next_job_id,
        default_id_mint_max_attempts(),
    )
    .await
}

async fn create_recap_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    recap_end_index: usize,
) -> anyhow::Result<SupervisorJob> {
    let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
    let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml)
        .await
        .with_context(|| format!("读取 recap session {session_id} metadata 失败"))?;
    if metadata.id != session_id {
        anyhow::bail!(
            "session metadata id {} 与 recap 请求的 {session_id} 不一致",
            metadata.id
        );
    }
    if metadata.agent_id != *agent_id {
        anyhow::bail!(
            "session {session_id} 属于 agent {}，不是当前 agent {agent_id}",
            metadata.agent_id
        );
    }
    if recap_end_index > metadata.message_count {
        anyhow::bail!(
            "session {session_id} recap target {recap_end_index} 超过 message_count {}",
            metadata.message_count
        );
    }

    fs::create_dir_all(&paths.jobs_dir).await?;
    let id = mint_unique_id_in_dir(&paths.jobs_dir, next_job_id, default_id_mint_max_attempts())
        .await
        .context("原子申领 recap supervisor job id 失败")?;
    let now = Utc::now();
    let job = SupervisorJob {
        id,
        agent_id: Some(agent_id.clone()),
        kind: SupervisorJobKind::Recap {
            session_id,
            recap_end_index,
        },
        status: SupervisorJobStatus::Queued,
        attempts: 0,
        manual_retries: 0,
        created_at: now,
        updated_at: now,
        started_at: None,
        finished_at: None,
        last_error: None,
        notify_on_completion: false,
    };
    write_reserved_job(paths, &job).await?;
    Ok(job)
}

async fn create_recovery_finalize_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    notify_on_completion: bool,
) -> anyhow::Result<SupervisorJob> {
    create_finalize_job_record(
        paths,
        agent_id,
        session_id,
        FinalizeJobInitialState {
            notify_on_completion,
            manual_retries: 1,
            last_error: Some(
                "manual retry recovered Finalizing session without a supervisor job".into(),
            ),
        },
        next_job_id,
        default_id_mint_max_attempts(),
    )
    .await
}

async fn create_resume_recovery_finalize_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    notify_on_completion: bool,
) -> anyhow::Result<SupervisorJob> {
    create_finalize_job_record(
        paths,
        agent_id,
        session_id,
        FinalizeJobInitialState {
            notify_on_completion,
            manual_retries: 0,
            last_error: None,
        },
        next_job_id,
        default_id_mint_max_attempts(),
    )
    .await
}

/// 原子申领 job ID 后写入初始记录，避免首次持久化覆盖同 ID 的既有 job。
#[cfg(test)]
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
    create_finalize_job_record(
        paths,
        agent_id,
        session_id,
        FinalizeJobInitialState {
            notify_on_completion,
            manual_retries: 0,
            last_error: None,
        },
        id_factory,
        max_id_attempts,
    )
    .await
}

struct FinalizeJobInitialState {
    notify_on_completion: bool,
    manual_retries: u32,
    last_error: Option<String>,
}

async fn create_finalize_job_record<F>(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    session_id: SessionId,
    initial: FinalizeJobInitialState,
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
        manual_retries: initial.manual_retries,
        created_at: now,
        updated_at: now,
        started_at: None,
        finished_at: None,
        last_error: initial.last_error,
        notify_on_completion: initial.notify_on_completion,
    };
    write_reserved_job(paths, &job).await?;
    Ok(job)
}

async fn next_queued_job(paths: &SupervisorPaths) -> anyhow::Result<Option<SupervisorJob>> {
    let mut jobs = read_jobs(paths).await?;
    jobs.retain(|job| job.status == SupervisorJobStatus::Queued);
    jobs.sort_by(|a, b| {
        supervisor_job_priority(&a.kind)
            .cmp(&supervisor_job_priority(&b.kind))
            .then_with(|| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| a.id.cmp(&b.id))
            })
    });
    Ok(jobs.into_iter().next())
}

fn supervisor_job_priority(kind: &SupervisorJobKind) -> u8 {
    match kind {
        SupervisorJobKind::Finalize { .. } => 0,
        SupervisorJobKind::Recap { .. } => 1,
    }
}

async fn has_queued_jobs(paths: &SupervisorPaths) -> anyhow::Result<bool> {
    Ok(next_queued_job(paths).await?.is_some())
}

async fn reconcile_stale_running_jobs(paths: &SupervisorPaths) -> anyhow::Result<()> {
    let mut jobs = read_jobs(paths).await?;
    for job in &mut jobs {
        if job.status != SupervisorJobStatus::Running {
            continue;
        }

        let session_id = job_session_id_ref(job).clone();
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        let session_status = read_yaml::<SessionMetadata>(&session_paths.session_yaml)
            .await
            .with_context(|| {
                format!(
                    "读取 stale running job {} 的 session {session_id} metadata 失败",
                    job.id
                )
            })?
            .status;
        let recap_subsumed = matches!(
            (&job.kind, &session_status),
            (
                SupervisorJobKind::Recap { .. },
                SessionStatus::Finalizing | SessionStatus::Closed
            )
        );
        // finalize 先提交 session，再提交 job。若进程在两次原子写之间退出，或
        // session 已经被 resume，旧 Running job 不能再次关闭新的会话周期。
        *job = recover_stale_running_job(
            job,
            session_status,
            "recovered stale running job after supervisor start".into(),
            "stale running job exhausted supervisor retry budget before recovery".into(),
        );
        write_job(paths, job).await?;
        if recap_subsumed {
            append_supervisor_log(
                paths,
                format!(
                    "recap job {} succeeded no-op during stale recovery: subsumed by finalize session={}",
                    job.id, session_id
                ),
            )
            .await;
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
    let (kind, session_id, recap_end_index) = match &job.kind {
        SupervisorJobKind::Finalize { session_id } => {
            ("finalize".to_string(), session_id.clone(), None)
        }
        SupervisorJobKind::Recap {
            session_id,
            recap_end_index,
        } => (
            "recap".to_string(),
            session_id.clone(),
            Some(*recap_end_index),
        ),
    };
    SupervisorJobView {
        id: job.id.clone(),
        agent_id: job.agent_id.clone(),
        kind,
        session_id,
        recap_end_index,
        status: job.status.as_str().to_string(),
        created_at: job.created_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        attempts: job.attempts,
        manual_retries: job.manual_retries,
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
    validate_supervisor_peer(&stream)?;
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

fn validate_supervisor_peer(stream: &UnixStream) -> anyhow::Result<()> {
    let peer_uid = stream
        .peer_cred()
        .context("读取 supervisor IPC 对端身份失败")?
        .uid();
    // SAFETY: geteuid 无参数且只读取当前进程的有效用户 ID。
    let current_uid = unsafe { libc::geteuid() };
    validate_supervisor_peer_uid(peer_uid, current_uid)
}

fn validate_supervisor_peer_uid(
    peer_uid: libc::uid_t,
    current_uid: libc::uid_t,
) -> anyhow::Result<()> {
    if peer_uid != current_uid {
        anyhow::bail!(
            "拒绝不同用户的 supervisor IPC 对端: peer_uid={peer_uid}, current_uid={current_uid}"
        );
    }
    Ok(())
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

async fn spawn_supervisor_process(config: SupervisorLaunchConfig) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("定位当前 acn 可执行文件失败")?;
    let paths = config.paths();
    fs::create_dir_all(&paths.supervisor_dir).await?;
    let launch_log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.launch_log_path)
        .await
        .with_context(|| {
            format!(
                "打开 supervisor launch log 失败: {}",
                paths.launch_log_path.display()
            )
        })?
        .into_std()
        .await;
    let mut command = tokio::process::Command::new(exe);
    command
        .arg("supervisor")
        .arg("run")
        .arg("--config")
        .arg(&config.config_path)
        .arg("--runtime-fingerprint")
        .arg(&config.runtime_fingerprint.digest)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(launch_log));
    if let Some(upstream) = &config.upstream {
        command.arg("--upstream").arg(upstream);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    command.spawn().context("启动 acn supervisor 失败")?;
    Ok(())
}

async fn wait_for_current_supervisor(
    paths: &SupervisorPaths,
    expected_fingerprint: &SupervisorRuntimeFingerprint,
) -> anyhow::Result<()> {
    let timeout = Duration::from_millis(DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS);
    let deadline = Instant::now() + timeout;
    loop {
        if supervisor_runtime_identity(paths)
            .await
            .is_ok_and(|(build, fingerprint)| {
                build.is_some_and(|build| build.matches_current())
                    && fingerprint.as_ref() == Some(expected_fingerprint)
            })
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

async fn supervisor_runtime_identity(
    paths: &SupervisorPaths,
) -> anyhow::Result<(Option<BuildIdentity>, Option<SupervisorRuntimeFingerprint>)> {
    match send_request(paths, SupervisorRequest::Status).await? {
        SupervisorResponse::Status {
            build,
            runtime_fingerprint,
            ..
        } => Ok((build, runtime_fingerprint)),
        other => anyhow::bail!("unexpected supervisor status response: {other:?}"),
    }
}

fn supervisor_runtime_matches(
    config: &SupervisorLaunchConfig,
    build: Option<&BuildIdentity>,
    fingerprint: Option<&SupervisorRuntimeFingerprint>,
) -> bool {
    build.is_some_and(BuildIdentity::matches_current)
        && fingerprint == Some(&config.runtime_fingerprint)
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

async fn set_socket_owner_only(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .await
        .with_context(|| format!("设置 supervisor UDS 权限失败: {}", path.display()))
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
    job_session_id_ref(job).to_string()
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
    const CONCURRENT_TAKEOVER_HOLDER_AGENT_HOME_ENV: &str =
        "ACN_TEST_CONCURRENT_TAKEOVER_HOLDER_AGENT_HOME";
    const CONCURRENT_TAKEOVER_HOLDER_BUILD_ENV: &str = "ACN_TEST_CONCURRENT_TAKEOVER_HOLDER_BUILD";
    const ENQUEUE_HOLDER_AGENT_HOME_ENV: &str = "ACN_TEST_ENQUEUE_HOLDER_AGENT_HOME";

    fn test_runtime_fingerprint(label: &str) -> SupervisorRuntimeFingerprint {
        SupervisorRuntimeFingerprint {
            schema: SUPERVISOR_RUNTIME_FINGERPRINT_SCHEMA,
            digest: hex::encode(ring::digest::digest(&SHA256, label.as_bytes()).as_ref()),
        }
    }

    fn test_launch_config(
        agent_home: PathBuf,
        runtime_fingerprint: SupervisorRuntimeFingerprint,
    ) -> SupervisorLaunchConfig {
        SupervisorLaunchConfig::new(
            agent_home.clone(),
            agent_home.join("config.toml"),
            None,
            true,
            runtime_fingerprint,
        )
    }

    fn test_shared_state(agent_id: AgentId) -> SupervisorSharedState {
        let (notify_tx, _notify_rx) = mpsc::unbounded_channel();
        SupervisorSharedState {
            agent_id,
            notify_tx,
            stop_requested: CancellationToken::new(),
            last_activity: Arc::new(AtomicU64::new(now_millis())),
            started_at: Utc::now(),
            runtime_fingerprint: test_runtime_fingerprint("current"),
            stopping: Arc::new(AtomicBool::new(false)),
            lifecycle_gate: Arc::new(Mutex::new(())),
            running_recap: Arc::new(Mutex::new(None)),
            running_finalize: Arc::new(Mutex::new(None)),
        }
    }

    fn test_finalize_checkpoint(
        recap_start_index: usize,
        recap_end_index: usize,
        status: FinalizeCheckpointStatus,
    ) -> FinalizeCheckpoint {
        FinalizeCheckpoint {
            recap_start_index,
            recap_end_index,
            recap_segment_hash: "test-segment-hash".into(),
            prepared_claims: Vec::new(),
            expected_claim_revisions: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: Vec::new(),
            trace_text: "test checkpoint".into(),
            trace_created_at: Utc::now(),
            trace_id: None,
            status,
        }
    }

    #[test]
    fn runtime_fingerprint_covers_config_upstream_and_credentials() {
        let baseline = runtime_fingerprint_from_parts(
            br#"{"model":"model-a"}"#,
            "default",
            Some("llm-key-a"),
            Some("team-key-a"),
        );

        for changed in [
            runtime_fingerprint_from_parts(
                br#"{"model":"model-b"}"#,
                "default",
                Some("llm-key-a"),
                Some("team-key-a"),
            ),
            runtime_fingerprint_from_parts(
                br#"{"model":"model-a"}"#,
                "other",
                Some("llm-key-a"),
                Some("team-key-a"),
            ),
            runtime_fingerprint_from_parts(
                br#"{"model":"model-a"}"#,
                "default",
                Some("llm-key-b"),
                Some("team-key-a"),
            ),
            runtime_fingerprint_from_parts(
                br#"{"model":"model-a"}"#,
                "default",
                Some("llm-key-a"),
                Some("team-key-b"),
            ),
        ] {
            assert_ne!(baseline, changed);
        }
        assert_eq!(baseline.digest.len(), 64);
        assert!(!baseline.digest.contains("llm-key-a"));
        assert!(!baseline.digest.contains("team-key-a"));
    }

    fn queued_finalize_job(id: &str, session_id: SessionId) -> SupervisorJob {
        let now = Utc::now();
        SupervisorJob {
            id: id.to_owned(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            kind: SupervisorJobKind::Finalize { session_id },
            status: SupervisorJobStatus::Queued,
            attempts: 0,
            manual_retries: 0,
            created_at: now,
            updated_at: now,
            started_at: None,
            finished_at: None,
            last_error: None,
            notify_on_completion: true,
        }
    }

    fn queued_recap_job(id: &str, session_id: SessionId, recap_end_index: usize) -> SupervisorJob {
        let now = Utc::now();
        SupervisorJob {
            id: id.to_owned(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            kind: SupervisorJobKind::Recap {
                session_id,
                recap_end_index,
            },
            status: SupervisorJobStatus::Queued,
            attempts: 0,
            manual_retries: 0,
            created_at: now,
            updated_at: now,
            started_at: None,
            finished_at: None,
            last_error: None,
            notify_on_completion: false,
        }
    }

    #[test]
    fn recap_job_fails_on_the_fifth_outer_attempt() {
        let mut job = queued_recap_job("job_recap", "session_11111111".parse().unwrap(), 4);

        for attempt in 1..DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS {
            job.attempts = attempt;
            apply_job_attempt_failure(&mut job, format!("attempt {attempt}"));
            assert_eq!(job.status, SupervisorJobStatus::Queued);
            assert!(job.finished_at.is_none());
        }

        job.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        apply_job_attempt_failure(&mut job, "attempt 5".into());
        assert_eq!(job.status, SupervisorJobStatus::Failed);
        assert!(job.finished_at.is_some());
        assert_eq!(job.last_error.as_deref(), Some("attempt 5"));
    }

    #[test]
    fn recap_result_is_subsumed_only_when_finalize_won_without_cursor_progress() {
        let now = Utc::now();
        let mut metadata = SessionMetadata {
            id: "session_11111111".parse().unwrap(),
            agent_id: AgentId::new("agent-a").unwrap(),
            status: SessionStatus::Finalizing,
            created_at: now,
            updated_at: now,
            closed_at: None,
            source: "tui".into(),
            model: "test-model".into(),
            system_prompt_path: "system_prompt.md".into(),
            message_count: 4,
            finalized_at: None,
            recapped_until: 4,
            provider_background_completion_until_seq: None,
            recap_background_completion_until_seq: None,
            compaction: None,
        };
        let mut report = SessionFinalizeReport::default();

        assert!(recap_report_was_subsumed_by_finalize(
            &report,
            Some(&metadata)
        ));

        report.advanced_recapped_until = true;
        assert!(!recap_report_was_subsumed_by_finalize(
            &report,
            Some(&metadata)
        ));

        report.advanced_recapped_until = false;
        metadata.status = SessionStatus::Open;
        assert!(!recap_report_was_subsumed_by_finalize(
            &report,
            Some(&metadata)
        ));
    }

    async fn write_test_session(
        paths: &SupervisorPaths,
        agent_id: &AgentId,
        session_id: &SessionId,
        status: SessionStatus,
    ) -> anyhow::Result<()> {
        let session_paths = SessionPaths::new(&paths.agent_home, session_id);
        fs::create_dir_all(&session_paths.dir).await?;
        let now = Utc::now();
        let terminal_at = (status == SessionStatus::Closed).then_some(now);
        let metadata = SessionMetadata {
            id: session_id.clone(),
            agent_id: agent_id.clone(),
            status,
            created_at: now,
            updated_at: now,
            closed_at: terminal_at,
            source: "tui".into(),
            model: "test-model".into(),
            system_prompt_path: "system_prompt.md".into(),
            message_count: 0,
            finalized_at: terminal_at,
            recapped_until: 0,
            provider_background_completion_until_seq: Some(0),
            recap_background_completion_until_seq: Some(0),
            compaction: None,
        };
        write_yaml_atomic(&session_paths.session_yaml, &metadata).await?;
        Ok(())
    }

    async fn set_test_session_message_count(
        paths: &SupervisorPaths,
        session_id: &SessionId,
        message_count: usize,
        recapped_until: usize,
    ) -> anyhow::Result<()> {
        let session_paths = SessionPaths::new(&paths.agent_home, session_id);
        let mut metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
        metadata.message_count = message_count;
        metadata.recapped_until = recapped_until;
        write_yaml_atomic(&session_paths.session_yaml, &metadata).await?;
        Ok(())
    }

    #[test]
    fn resume_conversion_preserves_identity_and_resets_finalize_execution_fields() {
        let now = Utc::now();
        let original = SupervisorJob {
            id: "job_original".into(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            kind: SupervisorJobKind::Finalize {
                session_id: "session_1234abcd".parse().unwrap(),
            },
            status: SupervisorJobStatus::Failed,
            attempts: 5,
            manual_retries: 2,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            finished_at: Some(now),
            last_error: Some("failed".into()),
            notify_on_completion: true,
        };
        let converted =
            converted_recap_job(&original, &"session_1234abcd".parse().unwrap(), 17, now);

        assert_eq!(converted.id, original.id);
        assert_eq!(converted.agent_id, original.agent_id);
        assert_eq!(converted.created_at, original.created_at);
        assert!(matches!(
            converted.kind,
            SupervisorJobKind::Recap {
                recap_end_index: 17,
                ..
            }
        ));
        assert_eq!(converted.status, SupervisorJobStatus::Queued);
        assert_eq!(converted.attempts, 0);
        assert_eq!(converted.manual_retries, 0);
        assert_eq!(converted.started_at, None);
        assert_eq!(converted.finished_at, None);
        assert_eq!(converted.last_error, None);
        assert!(!converted.notify_on_completion);
    }

    #[tokio::test]
    async fn queued_finalize_resume_converts_same_job_and_opens_session() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 7, 2).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.attempts = 3;
        job.manual_retries = 1;
        write_job(&paths, &job).await?;
        let shared = test_shared_state(agent_id.clone());

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::Opened {
                job_id: Some(job.id.clone())
            }
        );
        let jobs = read_jobs(&paths).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, job.id);
        assert!(matches!(
            jobs[0].kind,
            SupervisorJobKind::Recap {
                recap_end_index: 7,
                ..
            }
        ));
        assert_eq!(jobs[0].attempts, 0);
        assert!(!jobs[0].notify_on_completion);
        let metadata = read_yaml::<SessionMetadata>(
            &SessionPaths::new(&paths.agent_home, &session_id).session_yaml,
        )
        .await?;
        assert_eq!(metadata.status, SessionStatus::Open);
        Ok(())
    }

    #[tokio::test]
    async fn resume_keeps_finalize_job_and_session_when_checkpoint_already_exists(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 7, 2).await?;
        let job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        write_yaml_atomic(
            &session_paths.finalize_checkpoint_yaml,
            &test_finalize_checkpoint(2, 7, FinalizeCheckpointStatus::Prepared),
        )
        .await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::WaitForFinalize {
                job_id: Some(job.id.clone())
            }
        );
        assert_eq!(read_jobs(&paths).await?, vec![job]);
        let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
        assert_eq!(metadata.status, SessionStatus::Finalizing);
        assert!(path_exists(&session_paths.finalize_checkpoint_yaml).await);
        Ok(())
    }

    #[tokio::test]
    async fn resume_requeues_unregistered_running_finalize_with_checkpoint_before_waiting(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 7, 2).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = 2;
        job.started_at = Some(Utc::now());
        write_job(&paths, &job).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        write_yaml_atomic(
            &session_paths.finalize_checkpoint_yaml,
            &test_finalize_checkpoint(2, 7, FinalizeCheckpointStatus::Prepared),
        )
        .await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id, true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::WaitForFinalize {
                job_id: Some(job.id.clone())
            }
        );
        let recovered = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(recovered.id, job.id);
        assert_eq!(recovered.status, SupervisorJobStatus::Queued);
        assert_eq!(recovered.attempts, 2);
        assert!(recovered
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("unregistered running finalize")));
        assert!(path_exists(&session_paths.finalize_checkpoint_yaml).await);
        Ok(())
    }

    #[tokio::test]
    async fn failed_finalize_with_checkpoint_is_reset_for_one_resume_recovery_round(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Failed;
        job.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        job.manual_retries = 3;
        job.finished_at = Some(Utc::now());
        job.last_error = Some("checkpoint upload failed".into());
        write_job(&paths, &job).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        write_yaml_atomic(
            &session_paths.finalize_checkpoint_yaml,
            &test_finalize_checkpoint(0, 0, FinalizeCheckpointStatus::Applied),
        )
        .await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id, true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::WaitForFinalize {
                job_id: Some(job.id.clone())
            }
        );
        let recovered = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(recovered.id, job.id);
        assert_eq!(recovered.status, SupervisorJobStatus::Queued);
        assert_eq!(recovered.attempts, 0);
        assert_eq!(recovered.manual_retries, 3);
        assert!(recovered.notify_on_completion);
        assert_eq!(recovered.started_at, None);
        assert_eq!(recovered.finished_at, None);
        assert_eq!(recovered.last_error, None);
        Ok(())
    }

    #[tokio::test]
    async fn consumed_applied_recap_checkpoint_does_not_block_finalize_conversion(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 4, 2).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Failed;
        job.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        job.finished_at = Some(Utc::now());
        job.last_error = Some("previous finalize failed".into());
        write_job(&paths, &job).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        write_yaml_atomic(
            &session_paths.finalize_checkpoint_yaml,
            &test_finalize_checkpoint(0, 2, FinalizeCheckpointStatus::Applied),
        )
        .await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::Opened {
                job_id: Some(job.id.clone())
            }
        );
        let converted = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(converted.id, job.id);
        assert!(matches!(
            converted.kind,
            SupervisorJobKind::Recap {
                recap_end_index: 4,
                ..
            }
        ));
        let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
        assert_eq!(metadata.status, SessionStatus::Open);
        assert!(path_exists(&session_paths.finalize_checkpoint_yaml).await);
        Ok(())
    }

    #[tokio::test]
    async fn resume_checkpoint_wait_fails_when_supervisor_disappears() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abce".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;

        let error = tokio::time::timeout(
            Duration::from_secs(3),
            wait_for_resume_finalize_job(&paths, &session_id, &job.id),
        )
        .await
        .context("resume wait did not notice the stopped supervisor")?
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "This session is still finalizing; wait for finalization to complete before resuming."
        );
        let stored = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(stored.id, job.id);
        assert_eq!(stored.status, SupervisorJobStatus::Queued);
        Ok(())
    }

    #[tokio::test]
    async fn resume_reports_foreground_finalize_as_wait_without_job() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        let _finalize_guard = FileLockGuard::lock_exclusive(&session_paths.finalize_lock).await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::WaitForFinalize { job_id: None }
        );
        assert!(read_jobs(&paths).await?.is_empty());
        let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
        assert_eq!(metadata.status, SessionStatus::Finalizing);
        Ok(())
    }

    #[tokio::test]
    async fn orphan_checkpoint_resume_creates_finalize_recovery_job() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        write_yaml_atomic(
            &session_paths.finalize_checkpoint_yaml,
            &test_finalize_checkpoint(0, 0, FinalizeCheckpointStatus::Prepared),
        )
        .await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        let FinalizingResumeTakeover::WaitForFinalize {
            job_id: Some(job_id),
        } = outcome
        else {
            anyhow::bail!("unexpected takeover outcome: {outcome:?}");
        };
        let job = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(job.id, job_id);
        assert!(matches!(
            job.kind,
            SupervisorJobKind::Finalize {
                session_id: ref stored_session_id
            } if stored_session_id == &session_id
        ));
        assert_eq!(job.status, SupervisorJobStatus::Queued);
        assert_eq!(job.attempts, 0);
        assert_eq!(job.manual_retries, 0);
        assert!(job.notify_on_completion);
        Ok(())
    }

    #[tokio::test]
    async fn orphan_without_checkpoint_opens_and_only_enqueues_real_recap_backlog(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let with_backlog: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &with_backlog, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &with_backlog, 6, 2).await?;
        let with_backlog_paths = SessionPaths::new(&paths.agent_home, &with_backlog);
        let mut metadata = read_yaml::<SessionMetadata>(&with_backlog_paths.session_yaml).await?;
        metadata.recap_background_completion_until_seq = Some(11);
        write_yaml_atomic(&with_backlog_paths.session_yaml, &metadata).await?;
        let shared = test_shared_state(agent_id.clone());

        let outcome =
            resume_finalizing_takeover(&paths, &shared, with_backlog.clone(), false).await?;

        let FinalizingResumeTakeover::Opened {
            job_id: Some(recap_job_id),
        } = outcome
        else {
            anyhow::bail!("unexpected takeover outcome: {outcome:?}");
        };
        let jobs = read_jobs(&paths).await?;
        let recap_job = jobs.iter().find(|job| job.id == recap_job_id).unwrap();
        assert!(matches!(
            recap_job.kind,
            SupervisorJobKind::Recap {
                recap_end_index: 6,
                ..
            }
        ));
        assert!(!recap_job.notify_on_completion);
        let metadata = read_yaml::<SessionMetadata>(&with_backlog_paths.session_yaml).await?;
        assert_eq!(metadata.status, SessionStatus::Open);
        assert_eq!(metadata.recap_background_completion_until_seq, Some(11));

        let without_backlog: SessionId = "session_8765dcba".parse()?;
        write_test_session(
            &paths,
            &agent_id,
            &without_backlog,
            SessionStatus::Finalizing,
        )
        .await?;
        set_test_session_message_count(&paths, &without_backlog, 4, 4).await?;
        let outcome =
            resume_finalizing_takeover(&paths, &shared, without_backlog.clone(), false).await?;
        assert_eq!(outcome, FinalizingResumeTakeover::Opened { job_id: None });
        assert_eq!(read_jobs(&paths).await?.len(), 1);
        let metadata = read_yaml::<SessionMetadata>(
            &SessionPaths::new(&paths.agent_home, &without_backlog).session_yaml,
        )
        .await?;
        assert_eq!(metadata.status, SessionStatus::Open);
        Ok(())
    }

    #[tokio::test]
    async fn running_resume_conversion_failure_preserves_same_finalize_attempt_for_requeue(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Open).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = 2;
        job.started_at = Some(Utc::now());
        write_job(&paths, &job).await?;

        let error = convert_finalize_job_and_open(&paths, &agent_id, &job, 0)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not closed") || error.to_string().contains("Open"));
        let mut stored = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(stored, job);
        apply_job_attempt_failure(&mut stored, "original finalize failed".into());
        assert_eq!(stored.attempts, 2);
        assert_eq!(stored.status, SupervisorJobStatus::Queued);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resume_conversion_atomic_write_failure_keeps_finalize_bytes_and_session_state(
    ) -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.attempts = 2;
        write_job(&paths, &job).await?;
        let stored_path = job_path(&paths, &job.id);
        let original_bytes = fs::read(&stored_path).await?;
        let original_permissions = fs::metadata(&paths.jobs_dir).await?.permissions();
        fs::set_permissions(&paths.jobs_dir, std::fs::Permissions::from_mode(0o500)).await?;

        let result = convert_finalize_job_and_open(&paths, &agent_id, &job, 0).await;

        fs::set_permissions(&paths.jobs_dir, original_permissions).await?;
        assert!(result.is_err());
        assert_eq!(fs::read(&stored_path).await?, original_bytes);
        assert_eq!(read_jobs(&paths).await?, vec![job]);
        let metadata = read_yaml::<SessionMetadata>(
            &SessionPaths::new(&paths.agent_home, &session_id).session_yaml,
        )
        .await?;
        assert_eq!(metadata.status, SessionStatus::Finalizing);
        Ok(())
    }

    #[tokio::test]
    async fn closed_resume_reconciles_old_finalize_before_next_finalize_cycle() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Closed).await?;
        let mut old_job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        old_job.status = SupervisorJobStatus::Running;
        old_job.attempts = 1;
        old_job.started_at = Some(Utc::now());
        write_job(&paths, &old_job).await?;
        let shared = test_shared_state(agent_id.clone());

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::ReopenClosed {
                job_id: Some(old_job.id.clone())
            }
        );
        let reconciled = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(reconciled.status, SupervisorJobStatus::Succeeded);
        assert_eq!(reconciled.last_error, None);

        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        let metadata = read_yaml::<SessionMetadata>(&session_paths.session_yaml).await?;
        let mut session = SessionHandle::new(metadata, session_paths);
        session.mark_open(Utc::now()).await?;
        session.mark_finalizing(Utc::now()).await?;
        let next_job = enqueue_finalize_job(&paths, &agent_id, session_id, true).await?;
        assert_ne!(next_job.id, old_job.id);
        assert_eq!(next_job.status, SupervisorJobStatus::Queued);
        Ok(())
    }

    #[tokio::test]
    async fn running_finalize_resume_is_converted_only_by_registered_worker() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 9, 1).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = 1;
        write_job(&paths, &job).await?;

        let shared = test_shared_state(agent_id.clone());
        let preemption = Arc::new(SessionFinalizePreemptionControl::new());
        let resume_target = Arc::new(Mutex::new(None));
        let (result_tx, _result_rx) = watch::channel(None);
        *shared.running_finalize.lock().await = Some(RunningFinalize {
            job_id: job.id.clone(),
            session_id: session_id.clone(),
            preemption: Arc::clone(&preemption),
            resume_target: Arc::clone(&resume_target),
            resume_result_tx: result_tx.clone(),
        });

        let takeover_paths = paths.clone();
        let takeover_shared = shared.clone();
        let takeover_session_id = session_id.clone();
        let takeover = tokio::spawn(async move {
            resume_finalizing_takeover(&takeover_paths, &takeover_shared, takeover_session_id, true)
                .await
        });
        while !preemption.was_preempted_before_prepared().await {
            tokio::task::yield_now().await;
        }
        let _guard = shared.lifecycle_gate.lock().await;
        let target = resume_target.lock().await.unwrap();
        convert_finalize_job_and_open(&paths, &agent_id, &job, target).await?;
        let _ = result_tx.send(Some(RunningFinalizeResumeResult::Opened));
        drop(_guard);

        assert_eq!(
            takeover.await??,
            FinalizingResumeTakeover::Opened {
                job_id: Some(job.id.clone())
            }
        );
        let stored = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(stored.id, job.id);
        assert!(matches!(stored.kind, SupervisorJobKind::Recap { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn running_finalize_failure_before_prepared_is_rechecked_and_converted(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 9, 1).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        job.started_at = Some(Utc::now());
        write_job(&paths, &job).await?;

        let shared = test_shared_state(agent_id);
        let preemption = Arc::new(SessionFinalizePreemptionControl::new());
        assert!(!preemption.finish().await);
        assert!(preemption.finished_before_prepared().await);
        let resume_target = Arc::new(Mutex::new(None));
        let (result_tx, _result_rx) = watch::channel(None);
        *shared.running_finalize.lock().await = Some(RunningFinalize {
            job_id: job.id.clone(),
            session_id: session_id.clone(),
            preemption,
            resume_target: Arc::clone(&resume_target),
            resume_result_tx: result_tx.clone(),
        });

        let takeover_paths = paths.clone();
        let takeover_shared = shared.clone();
        let takeover_session_id = session_id.clone();
        let takeover = tokio::spawn(async move {
            resume_finalizing_takeover(&takeover_paths, &takeover_shared, takeover_session_id, true)
                .await
        });
        while resume_target.lock().await.is_none() {
            tokio::task::yield_now().await;
        }

        let _guard = shared.lifecycle_gate.lock().await;
        let mut failed = job.clone();
        apply_job_attempt_failure(&mut failed, "provider failed before Prepared".into());
        write_job(&paths, &failed).await?;
        *shared.running_finalize.lock().await = None;
        let _ = result_tx.send(Some(
            RunningFinalizeResumeResult::AttemptFinishedBeforePrepared,
        ));
        drop(_guard);

        assert_eq!(
            takeover.await??,
            FinalizingResumeTakeover::Opened {
                job_id: Some(job.id.clone())
            }
        );
        let stored = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(stored.id, job.id);
        assert!(matches!(
            stored.kind,
            SupervisorJobKind::Recap {
                recap_end_index: 9,
                ..
            }
        ));
        assert_eq!(stored.status, SupervisorJobStatus::Queued);
        assert_eq!(stored.attempts, 0);
        assert!(!stored.notify_on_completion);
        let metadata = read_yaml::<SessionMetadata>(
            &SessionPaths::new(&paths.agent_home, &session_id).session_yaml,
        )
        .await?;
        assert_eq!(metadata.status, SessionStatus::Open);
        Ok(())
    }

    #[tokio::test]
    async fn resume_recovers_unregistered_running_finalize_instead_of_waiting_forever(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 9, 1).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = 2;
        job.started_at = Some(Utc::now());
        write_job(&paths, &job).await?;
        let shared = test_shared_state(agent_id);

        let outcome = resume_finalizing_takeover(&paths, &shared, session_id.clone(), true).await?;

        assert_eq!(
            outcome,
            FinalizingResumeTakeover::Opened {
                job_id: Some(job.id.clone())
            }
        );
        let stored = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(stored.id, job.id);
        assert!(matches!(
            stored.kind,
            SupervisorJobKind::Recap {
                recap_end_index: 9,
                ..
            }
        ));
        assert_eq!(stored.status, SupervisorJobStatus::Queued);
        assert_eq!(stored.attempts, 0);
        let metadata = read_yaml::<SessionMetadata>(
            &SessionPaths::new(&paths.agent_home, &session_id).session_yaml,
        )
        .await?;
        assert_eq!(metadata.status, SessionStatus::Open);
        Ok(())
    }

    #[tokio::test]
    async fn runner_error_reconciles_persisted_running_job_without_refunding_attempt(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id, true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = 2;
        job.started_at = Some(Utc::now());
        write_job(&paths, &job).await?;

        let requeued = reconcile_running_job_after_runner_error(
            &paths,
            &job.id,
            &anyhow::anyhow!("terminal job write failed"),
        )
        .await?;

        assert!(requeued);
        let stored = read_jobs(&paths).await?.pop().unwrap();
        assert_eq!(stored.status, SupervisorJobStatus::Queued);
        assert_eq!(stored.attempts, 2);
        assert!(stored
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("terminal job write failed")));
        Ok(())
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

    #[tokio::test]
    async fn supervisor_socket_permissions_are_owner_only() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("supervisor.sock");
        let _listener = UnixListener::bind(&path)?;

        set_socket_owner_only(&path).await?;

        let mode = fs::metadata(&path).await?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_peer_accepts_same_effective_uid() -> anyhow::Result<()> {
        let (stream, _peer) = UnixStream::pair()?;

        validate_supervisor_peer(&stream)?;

        Ok(())
    }

    #[test]
    fn supervisor_peer_rejects_different_uid() {
        let error = validate_supervisor_peer_uid(1001, 1000)
            .unwrap_err()
            .to_string();

        assert!(error.contains("拒绝不同用户的 supervisor IPC 对端"));
        assert!(error.contains("peer_uid=1001"));
        assert!(error.contains("current_uid=1000"));
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
    fn supervisor_request_round_trips_enqueue_recap() {
        let request = SupervisorRequest::EnqueueRecap {
            session_id: "session_1234abcd".parse().unwrap(),
            recap_end_index: 42,
        };
        let json = serde_json::to_string(&request).unwrap();

        assert_eq!(
            json,
            r#"{"type":"enqueue_recap","session_id":"session_1234abcd","recap_end_index":42}"#
        );
        assert_eq!(
            serde_json::from_str::<SupervisorRequest>(&json).unwrap(),
            request
        );
    }

    #[test]
    fn supervisor_request_round_trips_retry_finalize_targets() {
        for request in [
            SupervisorRequest::RetryFinalize {
                target: SupervisorRetryTarget::Session {
                    session_id: "session_1234abcd".parse().unwrap(),
                },
                notify_on_completion: true,
            },
            SupervisorRequest::RetryFinalize {
                target: SupervisorRetryTarget::Job {
                    job_id: "job_123_abcdef01".into(),
                },
                notify_on_completion: false,
            },
        ] {
            let json = serde_json::to_string(&request).unwrap();
            assert_eq!(
                serde_json::from_str::<SupervisorRequest>(&json).unwrap(),
                request
            );
        }
    }

    #[tokio::test]
    async fn resume_mutation_waits_past_the_generic_ipc_deadline_for_authoritative_response(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        remove_stale_socket(&paths.socket_path).await;
        let listener = UnixListener::bind(&paths.socket_path)?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (read_half, mut write_half) = stream.into_split();
            let line = BufReader::new(read_half)
                .lines()
                .next_line()
                .await?
                .context("missing resume request")?;
            assert!(matches!(
                serde_json::from_str::<SupervisorRequest>(&line)?,
                SupervisorRequest::ResumeFinalizing { .. }
            ));
            sleep(Duration::from_millis(
                DEFAULT_SUPERVISOR_IPC_TIMEOUT_MS + 200,
            ))
            .await;
            let response = SupervisorResponse::ResumeTakeover {
                outcome: FinalizingResumeTakeover::Opened {
                    job_id: Some("job_delayed".into()),
                },
            };
            let mut bytes = serde_json::to_vec(&response)?;
            bytes.push(b'\n');
            write_half.write_all(&bytes).await?;
            anyhow::Ok(())
        });

        let response = tokio::time::timeout(
            Duration::from_millis(DEFAULT_SUPERVISOR_IPC_TIMEOUT_MS + 2_000),
            send_request_inner(
                &paths,
                SupervisorRequest::ResumeFinalizing {
                    session_id: "session_1234abcd".parse()?,
                    notify_on_completion: true,
                },
            ),
        )
        .await
        .context("resume mutation did not return its authoritative response")??;

        assert_eq!(
            response,
            SupervisorResponse::ResumeTakeover {
                outcome: FinalizingResumeTakeover::Opened {
                    job_id: Some("job_delayed".into())
                }
            }
        );
        server.await??;
        remove_stale_socket(&paths.socket_path).await;
        Ok(())
    }

    #[tokio::test]
    async fn closed_resume_reconciliation_allows_unavailable_supervisor() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = test_launch_config(
            dir.path().to_path_buf(),
            test_runtime_fingerprint("closed-resume"),
        );

        reconcile_closed_session_for_resume(&config, "session_1234abcd".parse()?).await?;

        Ok(())
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
    fn supervisor_runtime_match_requires_current_build_and_fingerprint() {
        let fingerprint = test_runtime_fingerprint("current");
        let launch = test_launch_config(PathBuf::from("/tmp/agent-a"), fingerprint.clone());

        assert!(supervisor_runtime_matches(
            &launch,
            Some(&BuildIdentity::current()),
            Some(&fingerprint)
        ));
        assert!(!supervisor_runtime_matches(
            &launch,
            Some(&BuildIdentity::current()),
            Some(&test_runtime_fingerprint("previous"))
        ));
        assert!(!supervisor_runtime_matches(
            &launch,
            None,
            Some(&fingerprint)
        ));
        assert!(!supervisor_runtime_matches(
            &launch,
            Some(&BuildIdentity {
                version: env!("CARGO_PKG_VERSION").into(),
                commit: "previous".into(),
            }),
            Some(&fingerprint)
        ));
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
    async fn running_finalize_resume_returns_if_worker_registration_disappears(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        set_test_session_message_count(&paths, &session_id, 9, 1).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Running;
        job.attempts = 1;
        write_job(&paths, &job).await?;

        let shared = test_shared_state(agent_id);
        let preemption = Arc::new(SessionFinalizePreemptionControl::new());
        let resume_target = Arc::new(Mutex::new(None));
        let (result_tx, _result_rx) = watch::channel(None);
        *shared.running_finalize.lock().await = Some(RunningFinalize {
            job_id: job.id,
            session_id: session_id.clone(),
            preemption: Arc::clone(&preemption),
            resume_target,
            resume_result_tx: result_tx.clone(),
        });

        let takeover_paths = paths.clone();
        let takeover_shared = shared.clone();
        let takeover = tokio::spawn(async move {
            resume_finalizing_takeover(&takeover_paths, &takeover_shared, session_id, true).await
        });
        while !preemption.was_preempted_before_prepared().await {
            tokio::task::yield_now().await;
        }
        *shared.running_finalize.lock().await = None;
        drop(result_tx);

        let joined = tokio::time::timeout(Duration::from_secs(1), takeover)
            .await
            .context("Resume remained blocked after the Finalize worker disappeared")?;
        let result = joined?;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("running finalize ended without a resume takeover result"));
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
                runtime_fingerprint: None,
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
            return run_test_supervisor_holder(PathBuf::from(agent_home), None, None).await;
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
    async fn ensure_supervisor_replaces_stale_runtime_and_waits_for_current_identity(
    ) -> anyhow::Result<()> {
        if let Some(agent_home) = std::env::var_os(TAKEOVER_HOLDER_AGENT_HOME_ENV) {
            let (build, runtime_fingerprint) =
                match std::env::var(TAKEOVER_HOLDER_BUILD_ENV).as_deref() {
                    Ok("current") => (
                        Some(BuildIdentity::current()),
                        Some(test_runtime_fingerprint("replacement")),
                    ),
                    Ok("stale") => (
                        Some(BuildIdentity::current()),
                        Some(test_runtime_fingerprint("stale")),
                    ),
                    other => anyhow::bail!("无效 takeover holder build: {other:?}"),
                };
            return run_test_supervisor_holder(
                PathBuf::from(agent_home),
                build,
                runtime_fingerprint,
            )
            .await;
        }

        let dir = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let test_name =
            "supervisor::tests::ensure_supervisor_replaces_stale_runtime_and_waits_for_current_identity";
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

        let mut stale = spawn_holder("stale")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match preflight_supervisor_shutdown(dir.path()).await {
                Ok(VerifiedSupervisorState::Running { .. }) => break,
                result if Instant::now() >= deadline => {
                    let _ = stale.kill().await;
                    anyhow::bail!("stale supervisor holder 未就绪: {result:?}");
                }
                _ => sleep(Duration::from_millis(25)).await,
            }
        }

        let runtime_fingerprint = test_runtime_fingerprint("replacement");
        let launch = test_launch_config(dir.path().to_path_buf(), runtime_fingerprint.clone());
        let (replacement_tx, replacement_rx) = std::sync::mpsc::channel();
        ensure_supervisor_running_with(&launch, |_| {
            std::future::ready((|| -> anyhow::Result<()> {
                let replacement = spawn_holder("current")?;
                replacement_tx
                    .send(replacement)
                    .map_err(|_| anyhow::anyhow!("记录 replacement supervisor 失败"))
            })())
        })
        .await?;

        let status = supervisor_status(dir.path()).await?;
        assert_eq!(status.runtime_state, SupervisorRuntimeState::Running);
        assert_eq!(status.build, Some(BuildIdentity::current()));
        assert_eq!(status.runtime_fingerprint, Some(runtime_fingerprint));
        ensure_supervisor_running_with(&launch, |_| async {
            anyhow::bail!("相同运行身份不应重复拉起 supervisor")
        })
        .await?;
        let replacement_state = preflight_supervisor_shutdown(dir.path()).await?;
        let guard = shutdown_verified_supervisor(dir.path(), replacement_state).await?;
        drop(guard);

        let mut replacement = replacement_rx
            .recv()
            .context("未收到 replacement supervisor child")?;
        let stale_status = tokio::time::timeout(Duration::from_secs(2), stale.wait()).await??;
        let replacement_status =
            tokio::time::timeout(Duration::from_secs(2), replacement.wait()).await??;
        assert!(!stale_status.success());
        assert!(!replacement_status.success());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn concurrent_ensure_rechecks_identity_inside_transition_lock() -> anyhow::Result<()> {
        if let Some(agent_home) = std::env::var_os(CONCURRENT_TAKEOVER_HOLDER_AGENT_HOME_ENV) {
            let runtime_fingerprint =
                match std::env::var(CONCURRENT_TAKEOVER_HOLDER_BUILD_ENV).as_deref() {
                    Ok("replacement") => test_runtime_fingerprint("replacement"),
                    Ok("stale") => test_runtime_fingerprint("stale"),
                    other => anyhow::bail!("无效 concurrent takeover holder build: {other:?}"),
                };
            return run_test_supervisor_holder(
                PathBuf::from(agent_home),
                Some(BuildIdentity::current()),
                Some(runtime_fingerprint),
            )
            .await;
        }

        let dir = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let test_name =
            "supervisor::tests::concurrent_ensure_rechecks_identity_inside_transition_lock";
        let spawn_holder = |build: &str| -> anyhow::Result<tokio::process::Child> {
            tokio::process::Command::new(&executable)
                .arg("--exact")
                .arg(test_name)
                .arg("--nocapture")
                .env(CONCURRENT_TAKEOVER_HOLDER_AGENT_HOME_ENV, dir.path())
                .env(CONCURRENT_TAKEOVER_HOLDER_BUILD_ENV, build)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .context("启动 concurrent takeover 测试 holder 失败")
        };

        let mut stale = spawn_holder("stale")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match supervisor_runtime_identity(&SupervisorPaths::new(dir.path())).await {
                Ok(_) => break,
                result if Instant::now() >= deadline => {
                    let _ = stale.kill().await;
                    anyhow::bail!("concurrent stale supervisor holder 未就绪: {result:?}");
                }
                _ => sleep(Duration::from_millis(25)).await,
            }
        }

        let paths = SupervisorPaths::new(dir.path());
        let transition_blocker = FileLockGuard::lock_exclusive_timeout(
            &paths.transition_lock_path,
            Duration::from_secs(1),
        )
        .await?;
        let launch = test_launch_config(
            dir.path().to_path_buf(),
            test_runtime_fingerprint("replacement"),
        );
        let spawn_calls = Arc::new(AtomicU64::new(0));
        let (replacement_tx, replacement_rx) = std::sync::mpsc::channel();
        let spawn_ensure = || {
            let launch = launch.clone();
            let executable = executable.clone();
            let agent_home = dir.path().to_path_buf();
            let spawn_calls = spawn_calls.clone();
            let replacement_tx = replacement_tx.clone();
            tokio::spawn(async move {
                ensure_supervisor_running_with(&launch, move |_| {
                    let result = (|| -> anyhow::Result<()> {
                        if spawn_calls.fetch_add(1, Ordering::SeqCst) != 0 {
                            anyhow::bail!("并发 ensure 不应重复拉起 replacement supervisor");
                        }
                        let replacement = tokio::process::Command::new(&executable)
                            .arg("--exact")
                            .arg(test_name)
                            .arg("--nocapture")
                            .env(CONCURRENT_TAKEOVER_HOLDER_AGENT_HOME_ENV, &agent_home)
                            .env(CONCURRENT_TAKEOVER_HOLDER_BUILD_ENV, "replacement")
                            .stdin(Stdio::null())
                            .stdout(Stdio::null())
                            .stderr(Stdio::inherit())
                            .spawn()
                            .context("启动 concurrent replacement supervisor 失败")?;
                        replacement_tx
                            .send(replacement)
                            .map_err(|_| anyhow::anyhow!("记录 concurrent replacement 失败"))
                    })();
                    std::future::ready(result)
                })
                .await
            })
        };
        let first = spawn_ensure();
        let second = spawn_ensure();
        sleep(Duration::from_millis(100)).await;
        assert_eq!(spawn_calls.load(Ordering::SeqCst), 0);
        drop(transition_blocker);

        first.await??;
        second.await??;
        assert_eq!(spawn_calls.load(Ordering::SeqCst), 1);
        let status = supervisor_status(dir.path()).await?;
        assert_eq!(
            status.runtime_fingerprint,
            Some(test_runtime_fingerprint("replacement"))
        );

        let state = preflight_supervisor_shutdown(dir.path()).await?;
        let guard = shutdown_verified_supervisor(dir.path(), state).await?;
        drop(guard);
        let mut replacement = replacement_rx
            .try_recv()
            .context("未收到 concurrent replacement supervisor child")?;
        let stale_status = tokio::time::timeout(Duration::from_secs(2), stale.wait()).await??;
        let replacement_status =
            tokio::time::timeout(Duration::from_secs(2), replacement.wait()).await??;
        assert!(!stale_status.success());
        assert!(!replacement_status.success());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn enqueue_uses_healthy_mismatched_runtime_without_takeover() -> anyhow::Result<()> {
        if let Some(agent_home) = std::env::var_os(ENQUEUE_HOLDER_AGENT_HOME_ENV) {
            return run_test_supervisor_holder(
                PathBuf::from(agent_home),
                Some(BuildIdentity::current()),
                Some(test_runtime_fingerprint("active")),
            )
            .await;
        }

        let dir = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let mut active = tokio::process::Command::new(executable)
            .arg("--exact")
            .arg("supervisor::tests::enqueue_uses_healthy_mismatched_runtime_without_takeover")
            .arg("--nocapture")
            .env(ENQUEUE_HOLDER_AGENT_HOME_ENV, dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .context("启动 enqueue 测试 supervisor holder 失败")?;
        let active_pid = active.id().context("enqueue 测试 holder 缺少 PID")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match preflight_supervisor_shutdown(dir.path()).await {
                Ok(VerifiedSupervisorState::Running { .. }) => break,
                result if Instant::now() >= deadline => {
                    let _ = active.kill().await;
                    anyhow::bail!("enqueue 测试 supervisor holder 未就绪: {result:?}");
                }
                _ => sleep(Duration::from_millis(25)).await,
            }
        }

        let stale_launch = test_launch_config(
            dir.path().to_path_buf(),
            test_runtime_fingerprint("stale-caller"),
        );
        let job_id = enqueue_finalize(&stale_launch, "session_1234abcd".parse()?).await?;

        assert_eq!(job_id, "job_test_holder");
        assert_eq!(active.id(), Some(active_pid));
        assert!(active.try_wait()?.is_none());
        let status = supervisor_status(dir.path()).await?;
        assert_eq!(
            status.runtime_fingerprint,
            Some(test_runtime_fingerprint("active"))
        );

        let state = preflight_supervisor_shutdown(dir.path()).await?;
        let guard = shutdown_verified_supervisor(dir.path(), state).await?;
        drop(guard);
        let exit = tokio::time::timeout(Duration::from_secs(2), active.wait()).await??;
        assert!(!exit.success());
        Ok(())
    }

    #[cfg(unix)]
    async fn run_test_supervisor_holder(
        agent_home: PathBuf,
        build: Option<BuildIdentity>,
        runtime_fingerprint: Option<SupervisorRuntimeFingerprint>,
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
                    runtime_fingerprint: runtime_fingerprint.clone(),
                },
                Ok(SupervisorRequest::EnqueueFinalize { .. }) => SupervisorResponse::Enqueued {
                    job_id: "job_test_holder".into(),
                },
                _ => SupervisorResponse::Error {
                    message: "test holder only supports status and enqueue".into(),
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
            runtime_fingerprint: test_runtime_fingerprint("current"),
            stopping: Arc::new(AtomicBool::new(true)),
            lifecycle_gate: Arc::new(Mutex::new(())),
            running_recap: Arc::new(Mutex::new(None)),
            running_finalize: Arc::new(Mutex::new(None)),
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
                message: "supervisor 正在停止，拒绝新 job".into()
            }
        );
        assert!(!paths.jobs_dir.exists());
        Ok(())
    }

    #[tokio::test]
    async fn idle_shutdown_rechecks_queue_after_waiting_for_enqueue_gate() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let (notify_tx, _notify_rx) = mpsc::unbounded_channel();
        let shared = SupervisorSharedState {
            agent_id: agent_id.clone(),
            notify_tx,
            stop_requested: CancellationToken::new(),
            last_activity: Arc::new(AtomicU64::new(0)),
            started_at: Utc::now(),
            runtime_fingerprint: test_runtime_fingerprint("current"),
            stopping: Arc::new(AtomicBool::new(false)),
            lifecycle_gate: Arc::new(Mutex::new(())),
            running_recap: Arc::new(Mutex::new(None)),
            running_finalize: Arc::new(Mutex::new(None)),
        };
        let running_job = Arc::new(AtomicBool::new(false));
        let enqueue_guard = shared.lifecycle_gate.lock().await;
        let shutdown = {
            let paths = paths.clone();
            let shared = shared.clone();
            let running_job = running_job.clone();
            tokio::spawn(async move {
                begin_idle_shutdown_if_due(&paths, &shared, &running_job, Duration::from_millis(1))
                    .await
            })
        };
        tokio::task::yield_now().await;

        let job = enqueue_finalize_job(&paths, &agent_id, session_id, true).await?;
        drop(enqueue_guard);

        assert!(!shutdown.await?);
        assert!(!shared.stopping.load(Ordering::Acquire));
        assert!(!shared.stop_requested.is_cancelled());
        assert_eq!(
            next_queued_job(&paths).await?.map(|job| job.id),
            Some(job.id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn enqueue_finalize_is_idempotent_per_session() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;

        let first = enqueue_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        let second = enqueue_finalize_job(&paths, &agent_id, session_id, false).await?;

        assert_eq!(first.id, second.id);
        assert!(second.notify_on_completion);
        assert_eq!(read_jobs(&paths).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn enqueue_recap_keeps_overlapping_jobs_and_disables_notifications() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Open).await?;

        let first = create_recap_job(&paths, &agent_id, session_id.clone(), 0).await?;
        let second = create_recap_job(&paths, &agent_id, session_id.clone(), 0).await?;

        assert_ne!(first.id, second.id);
        assert!(!first.notify_on_completion);
        assert!(!second.notify_on_completion);
        let jobs = read_jobs(&paths).await?;
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|job| matches!(
            &job.kind,
            SupervisorJobKind::Recap {
                session_id: stored_session_id,
                recap_end_index: 0,
            } if stored_session_id == &session_id
        )));
        Ok(())
    }

    #[tokio::test]
    async fn finalize_preemption_only_targets_running_recap_from_same_session() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let recap_session: SessionId = "session_11111111".parse()?;
        let other_session: SessionId = "session_22222222".parse()?;
        let preemption = Arc::new(SessionRecapPreemptionControl::new());
        let (notify_tx, _notify_rx) = mpsc::unbounded_channel();
        let shared = SupervisorSharedState {
            agent_id: AgentId::new("agent-a")?,
            notify_tx,
            stop_requested: CancellationToken::new(),
            last_activity: Arc::new(AtomicU64::new(now_millis())),
            started_at: Utc::now(),
            runtime_fingerprint: test_runtime_fingerprint("current"),
            stopping: Arc::new(AtomicBool::new(false)),
            lifecycle_gate: Arc::new(Mutex::new(())),
            running_recap: Arc::new(Mutex::new(Some(RunningRecap {
                job_id: "job_recap".into(),
                session_id: recap_session.clone(),
                preemption: Arc::clone(&preemption),
            }))),
            running_finalize: Arc::new(Mutex::new(None)),
        };

        let other_finalize = queued_finalize_job("job_finalize_b", other_session);
        request_same_session_recap_preemption(&paths, &shared, &other_finalize).await;
        assert!(!preemption.was_preempted_before_prepared().await);

        let same_finalize = queued_finalize_job("job_finalize_a", recap_session);
        request_same_session_recap_preemption(&paths, &shared, &same_finalize).await;
        assert!(preemption.was_preempted_before_prepared().await);
        Ok(())
    }

    #[tokio::test]
    async fn finalize_jobs_have_global_priority_over_older_recap_jobs() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.jobs_dir).await?;
        let recap_session = "session_11111111".parse()?;
        let finalize_session = "session_22222222".parse()?;
        let mut recap = queued_recap_job("job_recap", recap_session, 10);
        let mut finalize = queued_finalize_job("job_finalize", finalize_session);
        recap.created_at = Utc::now() - chrono::Duration::seconds(10);
        finalize.created_at = Utc::now();
        write_yaml_atomic(&job_path(&paths, &recap.id), &recap).await?;
        write_yaml_atomic(&job_path(&paths, &finalize.id), &finalize).await?;

        let selected = next_queued_job(&paths).await?.context("queued job")?;

        assert_eq!(selected.id, finalize.id);
        Ok(())
    }

    #[tokio::test]
    async fn preempting_finalize_does_not_jump_an_older_finalize() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        fs::create_dir_all(&paths.jobs_dir).await?;
        let session_a: SessionId = "session_11111111".parse()?;
        let session_b: SessionId = "session_22222222".parse()?;
        let now = Utc::now();
        let mut recap_a = queued_recap_job("job_recap_a", session_a.clone(), 10);
        let mut finalize_b = queued_finalize_job("job_finalize_b", session_b);
        let mut finalize_a = queued_finalize_job("job_finalize_a", session_a);
        recap_a.created_at = now - chrono::Duration::seconds(3);
        finalize_b.created_at = now - chrono::Duration::seconds(2);
        finalize_a.created_at = now - chrono::Duration::seconds(1);
        write_yaml_atomic(&job_path(&paths, &recap_a.id), &recap_a).await?;
        write_yaml_atomic(&job_path(&paths, &finalize_b.id), &finalize_b).await?;
        write_yaml_atomic(&job_path(&paths, &finalize_a.id), &finalize_a).await?;

        let selected = next_queued_job(&paths).await?.context("queued job")?;

        assert_eq!(selected.id, finalize_b.id);
        Ok(())
    }

    #[tokio::test]
    async fn enqueue_finalize_creates_new_job_after_historical_successes() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;

        let mut first = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        first.status = SupervisorJobStatus::Succeeded;
        write_job(&paths, &first).await?;
        let mut second = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        second.status = SupervisorJobStatus::Succeeded;
        write_job(&paths, &second).await?;

        let current = enqueue_finalize_job(&paths, &agent_id, session_id.clone(), false).await?;
        let jobs = read_jobs(&paths).await?;

        assert_ne!(current.id, first.id);
        assert_ne!(current.id, second.id);
        assert_eq!(current.status, SupervisorJobStatus::Queued);
        assert_eq!(jobs.len(), 3);
        assert_eq!(unresolved_finalize_jobs(&jobs, &session_id).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn enqueue_finalize_rejects_failed_job_with_retry_hint() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Failed;
        job.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        write_job(&paths, &job).await?;

        let error = enqueue_finalize_job(&paths, &agent_id, session_id.clone(), true)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains(&job.id));
        assert!(error.contains(&format!("acn supervisor retry {session_id}")));
        assert_eq!(read_jobs(&paths).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn retry_failed_job_resets_attempt_budget_and_preserves_history() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), false).await?;
        job.status = SupervisorJobStatus::Failed;
        job.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        job.manual_retries = 2;
        job.started_at = Some(Utc::now());
        job.finished_at = Some(Utc::now());
        job.last_error = Some("provider unavailable".into());
        write_job(&paths, &job).await?;

        let report = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Session {
                session_id: session_id.clone(),
            },
            true,
        )
        .await?;
        let stored = read_yaml::<SupervisorJob>(&job_path(&paths, &job.id)).await?;

        assert_eq!(report.session_id, session_id);
        assert_eq!(report.job_id, job.id);
        assert_eq!(
            report.previous_attempts,
            DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS
        );
        assert_eq!(report.manual_retries, 3);
        assert_eq!(stored.status, SupervisorJobStatus::Queued);
        assert_eq!(stored.attempts, 0);
        assert_eq!(stored.manual_retries, 3);
        assert_eq!(stored.started_at, None);
        assert_eq!(stored.finished_at, None);
        assert_eq!(stored.last_error.as_deref(), Some("provider unavailable"));
        assert!(!stored.notify_on_completion);
        Ok(())
    }

    #[tokio::test]
    async fn retry_by_job_id_resolves_the_same_session_and_job() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Failed;
        job.attempts = 4;
        job.last_error = Some("failed".into());
        write_job(&paths, &job).await?;

        let report = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Job {
                job_id: job.id.clone(),
            },
            true,
        )
        .await?;

        assert_eq!(report.session_id, session_id);
        assert_eq!(report.job_id, job.id);
        assert_eq!(report.previous_attempts, 4);
        assert_eq!(report.manual_retries, 1);
        Ok(())
    }

    #[tokio::test]
    async fn retry_session_recovers_finalizing_session_without_a_job() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;

        let report = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Session {
                session_id: session_id.clone(),
            },
            false,
        )
        .await?;
        let jobs = read_jobs(&paths).await?;

        assert_eq!(report.session_id, session_id);
        assert_eq!(report.previous_attempts, 0);
        assert_eq!(report.manual_retries, 1);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, report.job_id);
        assert_eq!(jobs[0].status, SupervisorJobStatus::Queued);
        assert_eq!(jobs[0].manual_retries, 1);
        assert!(!jobs[0].notify_on_completion);
        assert!(jobs[0].last_error.as_deref().is_some_and(|message| {
            message.contains("recovered Finalizing session without a supervisor job")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn retry_session_creates_recovery_job_after_historical_success() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut historical =
            create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        historical.status = SupervisorJobStatus::Succeeded;
        write_job(&paths, &historical).await?;

        let report = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Session {
                session_id: session_id.clone(),
            },
            false,
        )
        .await?;
        let jobs = read_jobs(&paths).await?;

        assert_ne!(report.job_id, historical.id);
        assert_eq!(jobs.len(), 2);
        assert_eq!(unresolved_finalize_jobs(&jobs, &session_id).len(), 1);
        assert_eq!(report.manual_retries, 1);
        Ok(())
    }

    #[tokio::test]
    async fn retry_session_does_not_create_job_while_foreground_finalize_holds_lock(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        let _finalize_guard = FileLockGuard::lock_exclusive(&session_paths.finalize_lock).await?;

        let error = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Session {
                session_id: session_id.clone(),
            },
            true,
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("正在 finalize，无需 retry"));
        assert!(read_jobs(&paths).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn finalizing_session_diagnostic_distinguishes_failed_running_and_orphaned(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        job.status = SupervisorJobStatus::Failed;
        write_job(&paths, &job).await?;

        assert_eq!(
            diagnose_finalizing_session(&paths.agent_home, &session_id).await?,
            FinalizingSessionDiagnostic::Failed {
                job_id: job.id.clone()
            }
        );

        job.status = SupervisorJobStatus::Succeeded;
        write_job(&paths, &job).await?;
        let session_paths = SessionPaths::new(&paths.agent_home, &session_id);
        let finalize_guard = FileLockGuard::lock_exclusive(&session_paths.finalize_lock).await?;
        assert_eq!(
            diagnose_finalizing_session(&paths.agent_home, &session_id).await?,
            FinalizingSessionDiagnostic::RunningWithoutJob
        );
        drop(finalize_guard);
        assert_eq!(
            diagnose_finalizing_session(&paths.agent_home, &session_id).await?,
            FinalizingSessionDiagnostic::Orphaned
        );
        Ok(())
    }

    #[tokio::test]
    async fn retry_job_id_cannot_recover_session_without_a_job() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;

        let error = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Job {
                job_id: "job_missing".into(),
            },
            true,
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("未找到 supervisor job job_missing"));
        assert!(read_jobs(&paths).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn retry_rejects_non_finalizing_sessions_and_non_failed_jobs() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;

        for status in [SessionStatus::Open, SessionStatus::Closed] {
            write_test_session(&paths, &agent_id, &session_id, status).await?;
            let error = retry_finalize_job(
                &paths,
                &agent_id,
                SupervisorRetryTarget::Session {
                    session_id: session_id.clone(),
                },
                true,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(error.contains("只有未完成的 Finalizing session 可以 retry"));
        }

        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut job = create_finalize_job(&paths, &agent_id, session_id.clone(), true).await?;
        for status in [SupervisorJobStatus::Queued, SupervisorJobStatus::Running] {
            job.status = status.clone();
            write_job(&paths, &job).await?;
            let error = retry_finalize_job(
                &paths,
                &agent_id,
                SupervisorRetryTarget::Session {
                    session_id: session_id.clone(),
                },
                true,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(error.contains("无需 retry"));
        }

        job.status = SupervisorJobStatus::Succeeded;
        write_job(&paths, &job).await?;
        let error = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Job {
                job_id: job.id.clone(),
            },
            true,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("不能 retry"));
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_session_jobs_are_reported_as_invariant_corruption() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id = "session_1234abcd".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let first = queued_finalize_job("job_first", session_id.clone());
        let second = queued_finalize_job("job_second", session_id.clone());
        write_yaml_atomic(&job_path(&paths, &first.id), &first).await?;
        write_yaml_atomic(&job_path(&paths, &second.id), &second).await?;

        let enqueue_error = enqueue_finalize_job(&paths, &agent_id, session_id.clone(), true)
            .await
            .unwrap_err()
            .to_string();
        let retry_error = retry_finalize_job(
            &paths,
            &agent_id,
            SupervisorRetryTarget::Job {
                job_id: first.id.clone(),
            },
            true,
        )
        .await
        .unwrap_err()
        .to_string();

        for error in [enqueue_error, retry_error] {
            assert!(error.contains("违反唯一性约束"));
            assert!(error.contains("job_first"));
            assert!(error.contains("job_second"));
        }
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
    async fn stale_running_job_is_requeued_without_refunding_attempt() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let session_id: SessionId = "session_11111111".parse()?;
        write_test_session(&paths, &agent_id, &session_id, SessionStatus::Finalizing).await?;
        let mut running = queued_finalize_job("job_running", session_id);
        running.status = SupervisorJobStatus::Running;
        running.attempts = 2;
        running.started_at = Some(Utc::now());
        let mut failed = queued_finalize_job("job_failed", "session_22222222".parse()?);
        failed.status = SupervisorJobStatus::Failed;
        failed.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        failed.finished_at = Some(Utc::now());
        let exhausted_session: SessionId = "session_33333333".parse()?;
        write_test_session(&paths, &agent_id, &exhausted_session, SessionStatus::Open).await?;
        let mut exhausted = queued_recap_job("job_exhausted", exhausted_session, 0);
        exhausted.status = SupervisorJobStatus::Running;
        exhausted.attempts = DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS;
        exhausted.started_at = Some(Utc::now());
        write_yaml_atomic(&job_path(&paths, &running.id), &running).await?;
        write_yaml_atomic(&job_path(&paths, &failed.id), &failed).await?;
        write_yaml_atomic(&job_path(&paths, &exhausted.id), &exhausted).await?;

        reconcile_stale_running_jobs(&paths).await?;

        let recovered = read_yaml::<SupervisorJob>(&job_path(&paths, &running.id)).await?;
        assert_eq!(recovered.status, SupervisorJobStatus::Queued);
        assert_eq!(recovered.attempts, 2);
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("recovered stale running job after supervisor start")
        );
        let terminal = read_yaml::<SupervisorJob>(&job_path(&paths, &failed.id)).await?;
        assert_eq!(terminal, failed);
        let exhausted = read_yaml::<SupervisorJob>(&job_path(&paths, "job_exhausted")).await?;
        assert_eq!(exhausted.status, SupervisorJobStatus::Failed);
        assert_eq!(exhausted.attempts, DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS);
        assert!(exhausted.finished_at.is_some());
        assert_eq!(
            exhausted.last_error.as_deref(),
            Some("stale running job exhausted supervisor retry budget before recovery")
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_recap_requeues_open_but_is_subsumed_by_finalize_states() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let cases = [
            (
                "session_11111111",
                SessionStatus::Open,
                SupervisorJobStatus::Queued,
            ),
            (
                "session_22222222",
                SessionStatus::Finalizing,
                SupervisorJobStatus::Succeeded,
            ),
            (
                "session_33333333",
                SessionStatus::Closed,
                SupervisorJobStatus::Succeeded,
            ),
        ];
        for (index, (session_id, status, _)) in cases.iter().enumerate() {
            let session_id: SessionId = session_id.parse()?;
            write_test_session(&paths, &agent_id, &session_id, *status).await?;
            let mut job = queued_recap_job(&format!("job_recap_{index}"), session_id, 0);
            job.status = SupervisorJobStatus::Running;
            job.attempts = 2;
            job.started_at = Some(Utc::now());
            write_yaml_atomic(&job_path(&paths, &job.id), &job).await?;
        }

        reconcile_stale_running_jobs(&paths).await?;

        for (index, (_, _, expected)) in cases.iter().enumerate() {
            let job = read_yaml::<SupervisorJob>(&job_path(&paths, &format!("job_recap_{index}")))
                .await?;
            assert_eq!(&job.status, expected);
            assert_eq!(job.attempts, 2);
        }
        let log = fs::read_to_string(&paths.log_path).await?;
        assert_eq!(log.matches("subsumed by finalize").count(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn stale_running_job_keeps_state_when_session_metadata_is_unreadable(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let mut running = queued_finalize_job("job_running", "session_11111111".parse()?);
        running.status = SupervisorJobStatus::Running;
        running.attempts = 2;
        running.started_at = Some(Utc::now());
        write_yaml_atomic(&job_path(&paths, &running.id), &running).await?;

        let error = reconcile_stale_running_jobs(&paths)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("读取 stale running job job_running"));
        let preserved = read_yaml::<SupervisorJob>(&job_path(&paths, &running.id)).await?;
        assert_eq!(preserved, running);
        Ok(())
    }

    #[tokio::test]
    async fn stale_running_job_does_not_reprocess_closed_or_reopened_session() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let paths = SupervisorPaths::new(dir.path());
        let agent_id = AgentId::new("agent-a")?;
        let closed_session: SessionId = "session_11111111".parse()?;
        let reopened_session: SessionId = "session_22222222".parse()?;
        write_test_session(&paths, &agent_id, &closed_session, SessionStatus::Closed).await?;
        write_test_session(&paths, &agent_id, &reopened_session, SessionStatus::Open).await?;

        for (job_id, session_id) in [
            ("job_closed", closed_session),
            ("job_reopened", reopened_session),
        ] {
            let mut job = queued_finalize_job(job_id, session_id);
            job.status = SupervisorJobStatus::Running;
            job.attempts = 1;
            job.started_at = Some(Utc::now());
            write_yaml_atomic(&job_path(&paths, &job.id), &job).await?;
        }

        reconcile_stale_running_jobs(&paths).await?;

        for job_id in ["job_closed", "job_reopened"] {
            let recovered = read_yaml::<SupervisorJob>(&job_path(&paths, job_id)).await?;
            assert_eq!(recovered.status, SupervisorJobStatus::Succeeded);
            assert_eq!(recovered.attempts, 1);
            assert!(recovered.finished_at.is_some());
            assert!(recovered.last_error.is_none());
        }
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
        assert_eq!(job.manual_retries, 0);
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
            manual_retries: 1,
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
        assert_eq!(view.manual_retries, 1);
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
                manual_retries: 0,
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
                manual_retries: 0,
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
