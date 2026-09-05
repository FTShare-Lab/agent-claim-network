//! Tool registry 测试的共享 fixture 与跨域基础断言。
//!
//! 各领域测试按生产模块拆分，并从这里复用 registry、router 与 delegation fixture。

mod command;
mod delegation;
mod file;
mod integration;
mod registry;
mod session;
mod web;

use super::*;
use crate::agent::fs::LocalFsMemoryStore;
use crate::claim::{
    AgentId, Claim, ClaimId, ClaimStatus, Confidence, DisputeId, DisputeStatus, SessionId,
};
use crate::config::ToolConfig;
use crate::delegation::{
    DelegationCreateRequest, DelegationExecutionContext, DelegationExecutionError,
    DelegationExecutionOutcome, DelegationProgressSink, DelegationStatus, DelegationStore,
};
use crate::router::{
    CandidateClaim, DisputeRef, RetrievalDebug, RouterQueryResult, ScopeOverviewItem,
    ScopesOverviewSnapshot,
};
use crate::session::{NewSessionMessage, SessionMessageRole, SessionStore};
use crate::session_search::{SessionSearchConfig, SessionSearchService, SessionSearchSummarizer};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

struct UnusedSessionSearchSummarizer;

#[async_trait]
impl SessionSearchSummarizer for UnusedSessionSearchSummarizer {
    async fn summarize_session_search(
        &self,
        _request: crate::api::SessionSearchSummaryRequest,
    ) -> anyhow::Result<crate::api::SessionSearchSummaryOutcome> {
        anyhow::bail!("session search summary path should not be called by these tests")
    }
}

fn test_tool_config(workspace_root: &Path) -> ToolConfig {
    ToolConfig {
        workspace_root: workspace_root.to_path_buf(),
        ..Default::default()
    }
}

#[test]
fn tool_registry_rejects_pty_input_budget_above_configured_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.background_process_pty_input_buffer_bytes =
        MAX_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES + 1;
    let error = match ToolRegistry::new(&config) {
        Ok(_) => {
            panic!("registry must reject a PTY stdin budget above its configured capacity")
        }
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("background process tool limits are invalid"));
}

#[test]
fn web_search_definition_uses_nested_max_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.web.max_count = 4;

    let definition = ToolRegistry::new(&config)
        .unwrap()
        .definitions()
        .into_iter()
        .find(|definition| definition.name == "web_search")
        .expect("web_search tool should be registered");

    assert_eq!(
        definition.input_schema["properties"]["count"]["maximum"],
        serde_json::json!(4)
    );
}

fn file_tool_context(session_id: &SessionId) -> ToolDispatchContext {
    ToolDispatchContext {
        current_session_id: Some(session_id.clone()),
        ..ToolDispatchContext::default()
    }
}

#[test]
fn configured_process_owner_identity_is_preserved_for_main_and_subagent() {
    let dir = tempfile::tempdir().unwrap();
    let agent_id = AgentId::new("agent-a").unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_process_owner_agent_id(agent_id);
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let main = registry.process_owner(&ToolDispatchContext {
        current_session_id: Some(session_id.clone()),
        ..ToolDispatchContext::default()
    });
    assert_eq!(main.owner_agent_id, "agent-a");
    assert_eq!(main.root_session_id, session_id.as_str());
    assert_eq!(main.subagent_id, None);

    let child = registry
        .for_delegation(None)
        .process_owner(&ToolDispatchContext {
            current_session_id: Some(session_id),
            current_turn_id: Some("subagent_12345678".into()),
            ..ToolDispatchContext::default()
        });
    assert_eq!(child.owner_agent_id, "agent-a");
    assert_eq!(child.subagent_id.as_deref(), Some("subagent_12345678"));
}

async fn dispatch_file_tool(
    registry: &ToolRegistry,
    session_id: &SessionId,
    name: &str,
    input: Value,
) -> Value {
    registry
        .dispatch_with_context(name, input, file_tool_context(session_id))
        .await
        .expect("file tool 调用不应产生 runtime error")
        .output
}

async fn full_file_read(registry: &ToolRegistry, session_id: &SessionId, path: &str) -> Value {
    dispatch_file_tool(
        registry,
        session_id,
        "file_read",
        json!({
            "path": path,
            "count": 10_000,
            "show_linenos": false,
        }),
    )
    .await
}

fn tiny_png_bytes() -> Vec<u8> {
    let img = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut out = Vec::new();
    img.write_to(&mut std::io::Cursor::new(&mut out), image::ImageFormat::Png)
        .expect("编码测试 PNG 不应失败");
    out
}

struct EnvVarGuard {
    key: &'static str,
    original: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let original = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

struct TestRouter {
    result: RouterQueryResult,
    overview: ScopesOverviewSnapshot,
}

#[async_trait]
impl RouterClient for TestRouter {
    async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        Ok(self.result.clone())
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        Ok(self.overview.clone())
    }
}

struct ImmediateDelegationExecutor;

#[async_trait]
impl DelegationExecutor for ImmediateDelegationExecutor {
    async fn execute(
        &self,
        context: DelegationExecutionContext,
        progress: DelegationProgressSink,
    ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
        progress
            .update(Some("done".into()), "done", Vec::new())
            .await
            .map_err(|err| DelegationExecutionError::new(err.to_string()))?;
        Ok(DelegationExecutionOutcome {
            summary: format!("done {}", context.metadata.id),
            changed_files: Vec::new(),
            artifacts: Vec::new(),
        })
    }
}

struct WaitingDelegationExecutor {
    slow_started: Arc<Notify>,
    late_started: Arc<Notify>,
    progress_gate: Arc<Notify>,
    progress_recorded: Arc<Notify>,
    release: Arc<Notify>,
    late_release: Arc<Notify>,
}

#[async_trait]
impl DelegationExecutor for WaitingDelegationExecutor {
    async fn execute(
        &self,
        context: DelegationExecutionContext,
        progress: DelegationProgressSink,
    ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
        if context.metadata.title == "fast" {
            return Ok(DelegationExecutionOutcome {
                summary: "fast completed".into(),
                changed_files: Vec::new(),
                artifacts: Vec::new(),
            });
        }
        if context.metadata.title == "late" {
            self.late_started.notify_one();
            self.late_release.notified().await;
            return Ok(DelegationExecutionOutcome {
                summary: "late completed".into(),
                changed_files: Vec::new(),
                artifacts: Vec::new(),
            });
        }
        self.slow_started.notify_one();
        self.progress_gate.notified().await;
        progress
            .update(Some("working".into()), "progress persisted", Vec::new())
            .await
            .map_err(|err| DelegationExecutionError::new(err.to_string()))?;
        self.progress_recorded.notify_one();
        self.release.notified().await;
        Ok(DelegationExecutionOutcome {
            summary: "slow completed".into(),
            changed_files: Vec::new(),
            artifacts: Vec::new(),
        })
    }
}

async fn wait_test_registry() -> (
    tempfile::TempDir,
    Arc<ToolRegistry>,
    SessionId,
    Arc<WaitingDelegationExecutor>,
    DelegationStore,
    Arc<Notify>,
    Arc<Notify>,
) {
    let dir = tempfile::tempdir().unwrap();
    let agents_root = dir.path().join("agents");
    let agent_home = agents_root.join("agent-a");
    let agent_id = AgentId::new("agent-a").unwrap();
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let delegation_store = DelegationStore::new_for_session(
        crate::storage::paths::agent_home_session_dir(&agent_home, &session_id),
        session_id.clone(),
    );
    SessionStore::new(agents_root)
        .create_with_id_factory(&agent_id, "system", || session_id.clone(), 1)
        .await
        .unwrap();
    let executor = Arc::new(WaitingDelegationExecutor {
        slow_started: Arc::new(Notify::new()),
        late_started: Arc::new(Notify::new()),
        progress_gate: Arc::new(Notify::new()),
        progress_recorded: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        late_release: Arc::new(Notify::new()),
    });
    let wait_snapshot_resolved = Arc::new(Notify::new());
    let wait_blocking = Arc::new(Notify::new());
    let registry = Arc::new(
        ToolRegistry::new(&test_tool_config(dir.path()))
            .unwrap()
            .with_delegation_executor(
                agent_home,
                agent_id,
                executor.clone(),
                DelegationRunnerConfig {
                    max_concurrent: 2,
                    wall_timeout: Duration::from_secs(15),
                    wait: DelegationWaitConfig {
                        default_timeout: Duration::from_secs(1),
                        min_timeout: Duration::from_secs(1),
                        max_timeout: Duration::from_secs(10),
                    },
                },
            )
            .with_wait_subagents_snapshot_notify(
                Arc::clone(&wait_snapshot_resolved),
                Arc::clone(&wait_blocking),
            ),
    );
    (
        dir,
        registry,
        session_id,
        executor,
        delegation_store,
        wait_snapshot_resolved,
        wait_blocking,
    )
}

async fn create_wait_test_subagent(
    registry: &ToolRegistry,
    session_id: &SessionId,
    title: &str,
) -> String {
    registry
        .dispatch_with_context(
            "create_subagent",
            json!({
                "title": title,
                "role": "wait verifier",
                "objective": "exercise wait_subagents",
            }),
            ToolDispatchContext {
                current_session_id: Some(session_id.clone()),
                current_turn_id: Some(format!("turn_{title}")),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap()
        .output["subagent"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn wait_test_context(session_id: &SessionId) -> ToolDispatchContext {
    ToolDispatchContext {
        current_session_id: Some(session_id.clone()),
        ..ToolDispatchContext::default()
    }
}

fn sample_claim() -> Claim {
    Claim {
        id: ClaimId::random(),
        name: "router_tool_claim".into(),
        statement: "router tool returns prompt-visible claims".into(),
        scope: "router/tool".into(),
        holder: AgentId::new("agent-b").unwrap(),
        confidence: Confidence::High,
        status: ClaimStatus::Active,
        created_at: "2026-05-20T00:00:00Z".parse().unwrap(),
        updated_at: None,
        source_claim_ids: vec![],
        evidence_summary: "test evidence".into(),
    }
}

fn sample_router_result() -> RouterQueryResult {
    RouterQueryResult {
        candidate_claims: vec![],
        disputes: vec![],
        retrieval_debug: None,
    }
}

fn sample_scopes_overview() -> ScopesOverviewSnapshot {
    ScopesOverviewSnapshot {
        scopes: vec![ScopeOverviewItem {
            scope: "router/tool".into(),
            active_claims: 2,
            stale_claims: 1,
            open_disputes: 1,
            resolved_disputes: 0,
            latest_claim_created_at: "2026-05-20T00:00:00Z".parse().unwrap(),
        }],
        claim_summaries: None,
    }
}
