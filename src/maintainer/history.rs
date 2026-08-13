//! Maintainer 审计与历史记录。
//!
//! 所有 admin 工作台需要追溯的事件统一写入滚动 JSONL，避免调试和长期运行时产生大量小 YAML 文件。

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::claim::{AgentId, ArbitrationResolutionId, DisputeId, PolicyId, PolicyStatus};
use crate::config::MaintainerHistoryConfig;
use crate::router::RouterQueryResult;
use crate::storage::{append_jsonl_record, paths, read_jsonl_records, JsonlRotationConfig};
use crate::time::serde_utc;

use super::arbitration::ObservationState;
use super::{ClaimSweepReport, DeliveryMessageType};

pub const STREAM_POLICY_EVENTS: &str = "policy_events";
pub const STREAM_DISPUTE_RESOLUTION_EVENTS: &str = "dispute_resolution_events";
pub const STREAM_SWEEP_RUNS: &str = "sweep_runs";
pub const STREAM_HTTP_AUDIT_LOGS: &str = "http_audit_logs";
pub const STREAM_ROUTER_QUERY_AUDIT_LOGS: &str = "router_query_audit_logs";
pub const STREAM_AGENT_ACTIVITY_EVENTS: &str = "agent_activity_events";
pub const STREAM_RESOLUTION_OBSERVATION_EVENTS: &str = "resolution_observation_events";

#[derive(Clone)]
pub struct HistoryStore {
    team_root: PathBuf,
    rotation: JsonlRotationConfig,
    lock: Arc<Mutex<()>>,
}

impl HistoryStore {
    pub fn new(team_root: PathBuf, config: MaintainerHistoryConfig) -> Self {
        Self {
            team_root,
            rotation: JsonlRotationConfig {
                max_file_bytes: config.max_file_bytes,
                backup_count: config.backup_count,
            },
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_defaults(team_root: PathBuf) -> Self {
        Self::new(team_root, MaintainerHistoryConfig::default())
    }

    pub async fn write_policy_event(&self, record: &PolicyEventRecord) -> anyhow::Result<()> {
        self.write_record(STREAM_POLICY_EVENTS, record).await
    }

    pub async fn write_dispute_resolution_event(
        &self,
        record: &DisputeResolutionEventRecord,
    ) -> anyhow::Result<()> {
        self.write_record(STREAM_DISPUTE_RESOLUTION_EVENTS, record)
            .await
    }

    /// Resolution 恢复路径可能重复执行；稳定 event_id 已存在时不再追加。
    pub async fn ensure_policy_event(&self, record: &PolicyEventRecord) -> anyhow::Result<()> {
        let _guard = self.lock.lock().await;
        if self
            .list_records::<PolicyEventRecord>(STREAM_POLICY_EVENTS)
            .await?
            .iter()
            .any(|existing| existing.event_id == record.event_id)
        {
            return Ok(());
        }
        append_jsonl_record(
            &paths::team_store_maintainer_history_current_path(
                &self.team_root,
                STREAM_POLICY_EVENTS,
            ),
            record,
            self.rotation,
        )
        .await
    }

    /// 与 Resolution 的 durable commit 共用幂等键，响应丢失或重启不会重复记账。
    pub async fn ensure_dispute_resolution_event(
        &self,
        record: &DisputeResolutionEventRecord,
    ) -> anyhow::Result<()> {
        let _guard = self.lock.lock().await;
        if self
            .list_records::<DisputeResolutionEventRecord>(STREAM_DISPUTE_RESOLUTION_EVENTS)
            .await?
            .iter()
            .any(|existing| existing.event_id == record.event_id)
        {
            return Ok(());
        }
        append_jsonl_record(
            &paths::team_store_maintainer_history_current_path(
                &self.team_root,
                STREAM_DISPUTE_RESOLUTION_EVENTS,
            ),
            record,
            self.rotation,
        )
        .await
    }

    pub async fn write_sweep_run(&self, record: &SweepRunRecord) -> anyhow::Result<()> {
        self.write_record(STREAM_SWEEP_RUNS, record).await
    }

    pub async fn write_http_audit_log(&self, record: &HttpAuditRecord) -> anyhow::Result<()> {
        self.write_record(STREAM_HTTP_AUDIT_LOGS, record).await
    }

    pub async fn write_agent_activity(&self, record: &AgentActivityRecord) -> anyhow::Result<()> {
        self.write_record(STREAM_AGENT_ACTIVITY_EVENTS, record)
            .await
    }

    pub async fn write_router_query_audit(
        &self,
        record: &RouterQueryAuditRecord,
    ) -> anyhow::Result<()> {
        self.write_record(STREAM_ROUTER_QUERY_AUDIT_LOGS, record)
            .await
    }

    pub async fn write_resolution_observation_event(
        &self,
        record: &ResolutionObservationEventRecord,
    ) -> anyhow::Result<()> {
        self.write_record(STREAM_RESOLUTION_OBSERVATION_EVENTS, record)
            .await
    }

    pub async fn list_policy_events(&self) -> anyhow::Result<Vec<PolicyEventRecord>> {
        self.list_records(STREAM_POLICY_EVENTS).await
    }

    pub async fn list_dispute_resolution_events(
        &self,
    ) -> anyhow::Result<Vec<DisputeResolutionEventRecord>> {
        self.list_records(STREAM_DISPUTE_RESOLUTION_EVENTS).await
    }

    pub async fn list_sweep_runs(&self) -> anyhow::Result<Vec<SweepRunRecord>> {
        self.list_records(STREAM_SWEEP_RUNS).await
    }

    pub async fn list_http_audit_logs(&self) -> anyhow::Result<Vec<HttpAuditRecord>> {
        self.list_records(STREAM_HTTP_AUDIT_LOGS).await
    }

    pub async fn list_agent_activity_events(&self) -> anyhow::Result<Vec<AgentActivityRecord>> {
        self.list_records(STREAM_AGENT_ACTIVITY_EVENTS).await
    }

    pub async fn list_router_query_audits(&self) -> anyhow::Result<Vec<RouterQueryAuditRecord>> {
        self.list_records(STREAM_ROUTER_QUERY_AUDIT_LOGS).await
    }

    pub async fn list_resolution_observation_events(
        &self,
    ) -> anyhow::Result<Vec<ResolutionObservationEventRecord>> {
        self.list_records(STREAM_RESOLUTION_OBSERVATION_EVENTS)
            .await
    }

    async fn write_record<T: Serialize>(&self, stream: &str, record: &T) -> anyhow::Result<()> {
        let _guard = self.lock.lock().await;
        append_jsonl_record(
            &paths::team_store_maintainer_history_current_path(&self.team_root, stream),
            record,
            self.rotation,
        )
        .await
    }

    async fn list_records<T: DeserializeOwned>(&self, stream: &str) -> anyhow::Result<Vec<T>> {
        let dir = paths::team_store_maintainer_history_stream_dir(&self.team_root, stream);
        read_jsonl_records(&dir).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEventKind {
    PolicyUpdatePublished,
    ClaimAttributeUpdatePublished,
    PolicyDeprecated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyEventRecord {
    pub event_id: String,
    pub policy_id: PolicyId,
    pub event_kind: PolicyEventKind,
    #[serde(with = "serde_utc")]
    pub occurred_at: DateTime<Utc>,
    pub policy_name: String,
    pub policy_scope: String,
    pub policy_status: PolicyStatus,
    pub message_type: DeliveryMessageType,
    pub target_agents: Vec<AgentId>,
    pub statement: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisputeResolutionEventRecord {
    pub event_id: String,
    pub dispute_id: DisputeId,
    #[serde(with = "serde_utc")]
    pub occurred_at: DateTime<Utc>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepRunRecord {
    pub run_id: String,
    #[serde(with = "serde_utc")]
    pub triggered_at: DateTime<Utc>,
    pub trigger: String,
    pub report: ClaimSweepReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpAuditRecord {
    pub audit_id: String,
    #[serde(with = "serde_utc")]
    pub occurred_at: DateTime<Utc>,
    pub method: String,
    pub path: String,
    pub status_code: u16,
    pub duration_ms: u64,
    pub source_ip: Option<String>,
    pub request_body: String,
    pub response_body: String,
    pub resource_id: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentActivityKind {
    InboxPulled,
    ClaimUploaded,
    DisputeReported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentActivityRecord {
    pub event_id: String,
    pub agent_id: AgentId,
    pub activity_kind: AgentActivityKind,
    #[serde(with = "serde_utc")]
    pub occurred_at: DateTime<Utc>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterQueryAuditRecord {
    pub query_id: String,
    #[serde(with = "serde_utc")]
    pub occurred_at: DateTime<Utc>,
    pub scope: String,
    pub semantic_query: Option<String>,
    pub result: RouterQueryResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionObservationEventRecord {
    pub event_id: String,
    pub resolution_id: ArbitrationResolutionId,
    pub dispute_id: DisputeId,
    pub agent_id: AgentId,
    #[serde(with = "serde_utc")]
    pub occurred_at: DateTime<Utc>,
    pub previous_state: Option<ObservationState>,
    pub current_state: ObservationState,
    pub reasons: Vec<String>,
}

pub fn fresh_record_id(prefix: &str) -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    format!(
        "{prefix}_{}_{}",
        Utc::now().timestamp_millis(),
        hex::encode(buf)
    )
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::claim::{PolicyId, PolicyStatus};

    fn history_store(team_root: PathBuf) -> HistoryStore {
        HistoryStore::new(
            team_root,
            MaintainerHistoryConfig {
                max_file_bytes: 512,
                backup_count: 2,
            },
        )
    }

    fn policy_event(id: &str) -> PolicyEventRecord {
        PolicyEventRecord {
            event_id: id.into(),
            policy_id: PolicyId::from_str("policy_aaaaaaaa").unwrap(),
            event_kind: PolicyEventKind::PolicyUpdatePublished,
            occurred_at: Utc::now(),
            policy_name: "policy".into(),
            policy_scope: "scope".into(),
            policy_status: PolicyStatus::Active,
            message_type: DeliveryMessageType::PolicyUpdate,
            target_agents: vec![],
            statement: "statement".into(),
        }
    }

    #[tokio::test]
    async fn history_store_writes_and_lists_policy_events() {
        let team = tempfile::tempdir().unwrap();
        let store = history_store(team.path().to_path_buf());
        store
            .write_policy_event(&policy_event("event_a"))
            .await
            .unwrap();
        store
            .write_policy_event(&policy_event("event_b"))
            .await
            .unwrap();

        let records = store.list_policy_events().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].event_id, "event_a");
        assert_eq!(records[1].event_id, "event_b");
    }

    #[tokio::test]
    async fn history_store_rotates_policy_events() {
        let team = tempfile::tempdir().unwrap();
        let store = history_store(team.path().to_path_buf());
        for idx in 0..20 {
            store
                .write_policy_event(&policy_event(&format!("event_{idx}")))
                .await
                .unwrap();
        }

        let dir =
            paths::team_store_maintainer_history_stream_dir(team.path(), STREAM_POLICY_EVENTS);
        let mut archives = tokio::fs::read_dir(&dir).await.unwrap();
        let mut archive_count = 0;
        while let Some(entry) = archives.next_entry().await.unwrap() {
            let file_name = entry.file_name();
            if file_name.to_string_lossy().starts_with("archive_") {
                archive_count += 1;
            }
        }
        assert!(archive_count <= 2);
        assert!(!store.list_policy_events().await.unwrap().is_empty());
    }
}
