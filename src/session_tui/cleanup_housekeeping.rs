//! TUI 启动后的旧 session 后台清理。
//!
//! 这里实现 best-effort housekeeping：延迟到空闲期执行，受 marker 限流，
//! TUI 退出时取消尚在等待期的清理；清理一旦开始则由 app 等待收尾。
//! 本模块不参与 finalize/supervisor 链路。

use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::claim::AgentId;
use crate::session::{cleanup_old_sessions, SessionCleanupAbortCheck, SessionCleanupConfig};
use crate::storage::{paths, write_text_atomic, FileLockGuard};

const CLEANUP_WAITING: u8 = 0;
const CLEANUP_STARTED: u8 = 1;
const CLEANUP_SHUTDOWN: u8 = 2;

#[derive(Debug, Clone)]
pub struct SessionCleanupHousekeepingConfig {
    pub agent_id: AgentId,
    pub agent_home: PathBuf,
    pub retention_days: u32,
    pub sqlite_busy_timeout: Duration,
    pub timing: SessionCleanupHousekeepingTiming,
}

#[derive(Debug, Clone)]
pub struct SessionCleanupHousekeepingTiming {
    pub initial_delay: Duration,
    pub idle_grace: Duration,
    pub retry_delay: Duration,
    pub marker_interval: Duration,
}

impl Default for SessionCleanupHousekeepingTiming {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(10 * 60),
            idle_grace: Duration::from_secs(60),
            retry_delay: Duration::from_secs(10 * 60),
            marker_interval: Duration::from_secs(24 * 60 * 60),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SessionCleanupActivity {
    last_user_activity_ms: Arc<AtomicU64>,
    busy: Arc<AtomicBool>,
    lifecycle: Arc<AtomicU8>,
    shutdown: Arc<Notify>,
}

impl SessionCleanupActivity {
    pub(super) fn new() -> Self {
        Self {
            last_user_activity_ms: Arc::new(AtomicU64::new(now_ms())),
            busy: Arc::new(AtomicBool::new(true)),
            lifecycle: Arc::new(AtomicU8::new(CLEANUP_WAITING)),
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub(super) fn record_user_activity(&self) {
        self.last_user_activity_ms
            .store(now_ms(), Ordering::Relaxed);
    }

    pub(super) fn set_busy(&self, busy: bool) {
        self.busy.store(busy, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn cleanup_started(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == CLEANUP_STARTED
    }

    pub(super) fn request_shutdown(&self) {
        if self
            .lifecycle
            .compare_exchange(
                CLEANUP_WAITING,
                CLEANUP_SHUTDOWN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.shutdown.notify_waiters();
        }
    }

    fn try_mark_cleanup_started(&self) -> bool {
        self.lifecycle
            .compare_exchange(
                CLEANUP_WAITING,
                CLEANUP_STARTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn shutdown_requested(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == CLEANUP_SHUTDOWN
    }

    fn is_idle(&self, idle_grace: Duration) -> bool {
        if self.busy.load(Ordering::Relaxed) {
            return false;
        }
        let now = now_ms();
        let last = self.last_user_activity_ms.load(Ordering::Relaxed);
        now.saturating_sub(last) >= duration_ms_u64(idle_grace)
    }

    fn should_abort_cleanup_since(&self, started_at_ms: u64) -> bool {
        self.shutdown_requested()
            || self.busy.load(Ordering::Relaxed)
            || self.last_user_activity_ms.load(Ordering::Relaxed) > started_at_ms
    }
}

pub(super) fn spawn_session_cleanup_housekeeping(
    config: Option<SessionCleanupHousekeepingConfig>,
    activity: SessionCleanupActivity,
) -> Option<JoinHandle<()>> {
    let config = config?;
    if config.retention_days == 0 {
        return None;
    }
    Some(tokio::spawn(async move {
        if marker_recent(&config.agent_home, config.timing.marker_interval)
            .await
            .unwrap_or(false)
        {
            return;
        }
        if sleep_or_shutdown(&activity, config.timing.initial_delay).await {
            return;
        }
        loop {
            if marker_recent(&config.agent_home, config.timing.marker_interval)
                .await
                .unwrap_or(false)
            {
                return;
            }
            if !activity.is_idle(config.timing.idle_grace) {
                if sleep_or_shutdown(&activity, config.timing.retry_delay).await {
                    return;
                }
                continue;
            }
            if activity.shutdown_requested() {
                return;
            }
            if !activity.try_mark_cleanup_started() {
                return;
            }
            let started_at_ms = now_ms();
            let activity_for_abort = activity.clone();
            let abort_check: SessionCleanupAbortCheck =
                Arc::new(move || activity_for_abort.should_abort_cleanup_since(started_at_ms));
            run_cleanup_once(config, Some(abort_check)).await;
            return;
        }
    }))
}

async fn sleep_or_shutdown(activity: &SessionCleanupActivity, duration: Duration) -> bool {
    let shutdown = activity.shutdown.notified();
    tokio::pin!(shutdown);
    if activity.shutdown_requested() {
        return true;
    }
    tokio::select! {
        _ = sleep(duration) => activity.shutdown_requested(),
        _ = &mut shutdown => true,
    }
}

async fn run_cleanup_once(
    config: SessionCleanupHousekeepingConfig,
    abort_check: Option<SessionCleanupAbortCheck>,
) {
    let lock_path = paths::agent_home_session_cleanup_lock_path(&config.agent_home);
    let _guard = match FileLockGuard::lock_exclusive_timeout(&lock_path, Duration::ZERO).await {
        Ok(guard) => guard,
        Err(e) => {
            log::debug!(
                target: "session_cleanup",
                "Session cleanup skipped because lock is unavailable ({}): {e:#}",
                lock_path.display()
            );
            return;
        }
    };
    if marker_recent(&config.agent_home, config.timing.marker_interval)
        .await
        .unwrap_or(false)
    {
        return;
    }
    let cutoff = Utc::now() - ChronoDuration::days(i64::from(config.retention_days));
    let marker_abort_check = abort_check.clone();
    match cleanup_old_sessions(SessionCleanupConfig {
        agent_id: config.agent_id.clone(),
        agent_home: config.agent_home.clone(),
        cutoff,
        apply: true,
        sqlite_busy_timeout: config.sqlite_busy_timeout,
        abort_check,
    })
    .await
    {
        Ok(report) => {
            let aborted =
                report.aborted || marker_abort_check.as_ref().is_some_and(|check| check());
            if report.errors == 0 && !aborted {
                if let Err(e) = write_cleanup_marker(&config.agent_home).await {
                    log::warn!(
                        target: "session_cleanup",
                        "Session cleanup completed but marker write failed: {e:#}"
                    );
                }
            } else if aborted {
                log::debug!(
                    target: "session_cleanup",
                    "Session cleanup aborted before completion; marker not written"
                );
            } else {
                log::warn!(
                    target: "session_cleanup",
                    "Session cleanup completed with errors; marker not written: errors={}",
                    report.errors
                );
            }
            log::info!(
                target: "session_cleanup",
                "Session cleanup completed: scanned={} eligible={} deleted={} skipped={} sqlite_purged={} errors={}",
                report.scanned,
                report.eligible,
                report.deleted,
                report.skipped,
                report.sqlite_purged,
                report.errors
            );
        }
        Err(e) => {
            log::warn!(target: "session_cleanup", "Session cleanup failed: {e:#}");
        }
    }
}

async fn marker_recent(agent_home: &Path, marker_interval: Duration) -> anyhow::Result<bool> {
    let marker = paths::agent_home_session_cleanup_marker_path(agent_home);
    let metadata = match tokio::fs::metadata(&marker).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let modified = metadata.modified()?;
    let elapsed = SystemTime::now()
        .duration_since(modified)
        .unwrap_or(Duration::ZERO);
    Ok(elapsed < marker_interval)
}

async fn write_cleanup_marker(agent_home: &Path) -> anyhow::Result<()> {
    let marker = paths::agent_home_session_cleanup_marker_path(agent_home);
    write_text_atomic(&marker, Utc::now().to_rfc3339().as_bytes()).await?;
    Ok(())
}

fn now_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retention_zero_disables_housekeeping() {
        let dir = tempfile::tempdir().unwrap();
        let config = SessionCleanupHousekeepingConfig {
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_home: dir.path().join("agent-a"),
            retention_days: 0,
            sqlite_busy_timeout: Duration::from_millis(500),
            timing: SessionCleanupHousekeepingTiming::default(),
        };

        let handle =
            spawn_session_cleanup_housekeeping(Some(config), SessionCleanupActivity::new());

        assert!(handle.is_none());
    }

    #[tokio::test]
    async fn recent_marker_skips_housekeeping() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().join("agent-a");
        write_cleanup_marker(&agent_home).await.unwrap();

        assert!(
            marker_recent(&agent_home, Duration::from_secs(24 * 60 * 60))
                .await
                .unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_before_cleanup_does_not_write_marker() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().join("agent-a");
        let config = SessionCleanupHousekeepingConfig {
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_home: agent_home.clone(),
            retention_days: 30,
            sqlite_busy_timeout: Duration::from_millis(500),
            timing: SessionCleanupHousekeepingTiming {
                initial_delay: Duration::from_secs(60),
                idle_grace: Duration::ZERO,
                retry_delay: Duration::from_secs(60),
                marker_interval: Duration::from_secs(24 * 60 * 60),
            },
        };

        let activity = SessionCleanupActivity::new();
        let handle = spawn_session_cleanup_housekeeping(Some(config), activity.clone())
            .expect("Housekeeping should spawn");
        tokio::task::yield_now().await;
        activity.request_shutdown();
        handle.await.unwrap();

        assert!(!paths::agent_home_session_cleanup_marker_path(&agent_home).exists());
        assert!(!activity.cleanup_started());
    }

    #[tokio::test]
    async fn successful_cleanup_writes_marker() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().join("agent-a");
        let config = SessionCleanupHousekeepingConfig {
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_home: agent_home.clone(),
            retention_days: 30,
            sqlite_busy_timeout: Duration::from_millis(500),
            timing: SessionCleanupHousekeepingTiming::default(),
        };

        run_cleanup_once(config, None).await;

        assert!(paths::agent_home_session_cleanup_marker_path(&agent_home).exists());
    }

    #[tokio::test]
    async fn aborted_cleanup_does_not_write_marker() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().join("agent-a");
        let config = SessionCleanupHousekeepingConfig {
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_home: agent_home.clone(),
            retention_days: 30,
            sqlite_busy_timeout: Duration::from_millis(500),
            timing: SessionCleanupHousekeepingTiming::default(),
        };

        run_cleanup_once(config, Some(Arc::new(|| true))).await;

        assert!(!paths::agent_home_session_cleanup_marker_path(&agent_home).exists());
    }
}
