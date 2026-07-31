use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::auth::{AuthVerifier, TeamAuthStore};
use crate::maintainer::{history::HistoryStore, Maintainer};
use crate::router::RouterClient;
use crate::time::{serde_utc_opt, truncate_to_second};

use super::auth::AdminAuth;

#[derive(Clone)]
pub struct AppState {
    pub maintainer: Arc<Maintainer>,
    pub history_store: HistoryStore,
    pub router_client: Arc<dyn RouterClient>,
    pub auth: AuthVerifier,
    pub auth_store: TeamAuthStore,
    pub maintainer_team_auth_enabled: bool,
    pub router_team_auth_enabled: bool,
    pub frontend_dist_dir: PathBuf,
    pub sweep_scheduler: SweepScheduler,
    pub admin_auth: Option<AdminAuth>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SweepScheduleStatus {
    pub tick_interval_secs: u64,
    #[serde(with = "serde_utc_opt")]
    pub last_auto_sweep_at: Option<DateTime<Utc>>,
    #[serde(default, with = "serde_utc_opt")]
    pub next_sweep_at: Option<DateTime<Utc>>,
    pub last_auto_trigger: Option<String>,
}

#[derive(Clone)]
pub struct SweepScheduler {
    inner: Arc<Mutex<SweepScheduleStatus>>,
}

impl SweepScheduler {
    pub fn new(tick_interval_secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SweepScheduleStatus {
                tick_interval_secs,
                last_auto_sweep_at: None,
                next_sweep_at: None,
                last_auto_trigger: None,
            })),
        }
    }

    pub async fn mark_auto_sweep_finished(&self, triggered_at: DateTime<Utc>, trigger: &str) {
        let mut status = self.inner.lock().await;
        status.last_auto_sweep_at = Some(truncate_to_second(triggered_at));
        status.last_auto_trigger = Some(trigger.to_string());
    }

    pub async fn mark_next_sweep_at(&self, next_sweep_at: DateTime<Utc>) {
        let mut status = self.inner.lock().await;
        status.next_sweep_at = Some(truncate_to_second(next_sweep_at));
    }

    pub async fn mark_next_after(&self, from: DateTime<Utc>, interval: Duration) {
        if let Ok(delta) = chrono::Duration::from_std(interval) {
            self.mark_next_sweep_at(from + delta).await;
        }
    }

    pub async fn status(&self) -> SweepScheduleStatus {
        self.inner.lock().await.clone()
    }
}
