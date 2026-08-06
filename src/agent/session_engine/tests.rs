//! SessionEngine 的单元测试集合。
//!
//! 这些测试原本内联在 `session_engine.rs`，迁移到独立文件仅为降低
//! facade 文件体积；测试模块路径、断言语义和 helper 可见性保持不变。

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::{mpsc, Mutex, Notify};

use super::super::fs::{
    LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore, LocalFsReportedDisputeClaimSetStore,
};
use super::super::inbox::InboxJsonGenerator;
use super::super::maintainer_upload::LocalFsMaintainerUploadQueue;
use super::super::runner::AgentRunner;
use super::{
    active_provider_safe_segments, active_segments_hash, append_acn_md,
    auto_compact_should_trigger, auto_compact_trigger_threshold_tokens,
    auto_compact_trigger_tokens, compacted_committed_summary_message, compacted_context_for_turn,
    compaction_tail_token_limit, compaction_transcript_projection, delegation_summary_projection,
    estimate_compacted_committed_summary_message_tokens,
    estimated_session_message_tokens_projected, finish_cancelled_turn_journal,
    hash_session_segment, is_canonical_messages_committed_error, parse_compaction_summary_outcome,
    project_provider_context, select_compaction_summary_end_index,
    session_compaction_transcript_projection, session_messages_to_provider_turn_messages,
    session_messages_to_turn_messages, session_messages_to_turn_transcript,
    spawn_turn_control_journal_forwarder, ActiveProjectionContext, CompactionAuditScope,
    CompactionAuditSummaryContext, CompactionAuditTrigger, CompactionRanges,
    CompactionSummaryInputs, ManualCompactionOutcome, PreflightCompactionRequest,
    PreflightCompactor, ProviderContextUsageAnchor, ProviderProjectionBudget,
    SessionCompactionNoopReason, SessionCompactionResult, SessionEngine, SessionEvent,
    SessionTurnCommittedPostCommitError, TurnJournalEmitter, TurnJournalSink,
    COMPACTION_CHECKPOINT_SCHEMA_VERSION, DELEGATION_PROJECTION_MAX_CHARS,
    DELEGATION_PROJECTION_MAX_ITEMS, MEDIA_BLOCK_ESTIMATED_TOKENS,
};
use crate::agent::{
    InboxReader, LocalClaimStore, MemoryStore, ReportedDisputeClaimSetStore, SessionRuntimeStatus,
    SessionTurnControl,
};
use crate::api::{
    estimate_session_turn_messages_tokens, estimate_text_tokens, AgentTurnLoop,
    CompletedSessionTurnMessage, ContextUsageSnapshot, ContextUsageSource, InboxInternalizeKind,
    InternalizeRequest, MemoryReviewLoop, ProviderAdapter, ProviderEvent,
    ProviderHistoryMediaPolicy, ProviderReplayProtocol, ProviderReplayState, ProviderRequest,
    ProviderResponse, ProviderStop, SessionAttachment, SessionTurnContentBlock, SessionTurnEvent,
    SessionTurnMessage, SessionTurnPreflight, StructuredJsonCaller, ToolCallSkipReason,
    TurnMessage,
};
use crate::claim::{
    AgentId, Claim, ClaimId, ClaimStatus, Confidence, Dispute, DisputeId, InboxId, InboxMessage,
    SessionId,
};
use crate::config::{
    AgentSessionTurnJournalConfig, SessionCompactionConfig, ToolConfig, UserShellConfig,
};
use crate::delegation::{
    DelegationCreateRequest, DelegationExecutionContext, DelegationExecutionError,
    DelegationExecutionOutcome, DelegationExecutor, DelegationProgressSink, DelegationRunnerConfig,
    DelegationStatus, DelegationStore,
};
use crate::maintainer::traits::MaintainerClient;
use crate::prompt::PromptRegistry;
use crate::router::{AgentQuery, RouterClient, RouterQueryResult, ScopesOverviewSnapshot};
use crate::session::{
    canonical_user_content_hash, replay_turn_journal, ActiveTurnCompactionCursor,
    CompactionAppliedReport, CompactionCheckpoint, CompactionCheckpointStatus, NewSessionMessage,
    SessionCompactionState, SessionContentBlock, SessionMessage, SessionMessageRole,
    SessionMetadata, SessionStatus, SessionStore, TurnJournalEventKind, TurnJournalFlush,
    TurnJournalNonStreamingFallbackState, TurnJournalProjection, TurnJournalStatus,
    TurnJournalTurn,
};
use crate::skill::{SkillInstructions, SkillSummary};
use crate::tool::{ToolDispatchContext, ToolRegistry};
use serde_json::json;

enum ProviderStep {
    Response {
        response: ProviderResponse,
        events: Vec<ProviderEvent>,
    },
    ResponseAndCancel {
        response: ProviderResponse,
        events: Vec<ProviderEvent>,
        control: SessionTurnControl,
    },
    ResponseAndSteer {
        response: ProviderResponse,
        events: Vec<ProviderEvent>,
        control: SessionTurnControl,
    },
    JsonByRequestKind {
        compaction_response: Option<ProviderResponse>,
        recap_response: Option<ProviderResponse>,
    },
    Error {
        message: &'static str,
        events: Vec<ProviderEvent>,
    },
}

struct RecordingProvider {
    steps: Mutex<VecDeque<ProviderStep>>,
    requests: Mutex<Vec<ProviderRequest>>,
}

impl RecordingProvider {
    fn new(steps: Vec<ProviderStep>) -> Self {
        Self {
            steps: Mutex::new(VecDeque::from(steps)),
            requests: Mutex::new(Vec::new()),
        }
    }

    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ProviderAdapter for RecordingProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let request_for_kind = request.clone();
        self.requests.lock().await.push(request);
        let next_step = {
            let mut steps = self.steps.lock().await;
            if let Some(ProviderStep::JsonByRequestKind {
                compaction_response,
                recap_response,
            }) = steps.front_mut()
            {
                let selected = if request_for_kind.system_prompt.contains("session 历史压缩")
                    || request_for_kind.system_prompt.contains("committed_summary")
                {
                    compaction_response.take()
                } else if request_for_kind.system_prompt.contains("复盘阶段")
                    || request_for_kind.system_prompt.contains("new_claims")
                {
                    recap_response.take()
                } else {
                    anyhow::bail!("recording provider could not classify JSON request")
                };
                if compaction_response.is_none() && recap_response.is_none() {
                    steps.pop_front();
                }
                return selected.ok_or_else(|| {
                    anyhow::anyhow!("recording provider JSON response already consumed")
                });
            }
            steps.pop_front()
        };
        match next_step {
            Some(ProviderStep::Response { response, events }) => {
                for event in events {
                    emit(event);
                }
                Ok(response)
            }
            Some(ProviderStep::ResponseAndCancel {
                response,
                events,
                control,
            }) => {
                for event in events {
                    emit(event);
                }
                assert!(control.request_tool_boundary_cancel_now("cancel after provider response"));
                Ok(response)
            }
            Some(ProviderStep::ResponseAndSteer {
                response,
                events,
                control,
            }) => {
                for event in events {
                    emit(event);
                }
                assert!(
                    control
                        .request_tool_boundary_steer("steer after provider response")
                        .await
                );
                Ok(response)
            }
            Some(ProviderStep::JsonByRequestKind { .. }) => {
                anyhow::bail!("recording provider JSON response was not handled")
            }
            Some(ProviderStep::Error { message, events }) => {
                for event in events {
                    emit(event);
                }
                anyhow::bail!(message)
            }
            None => anyhow::bail!("recording provider response exhausted"),
        }
    }
}

struct BlockingAfterFileReadProvider {
    calls: AtomicUsize,
    second_call_started: Notify,
}

impl BlockingAfterFileReadProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            second_call_started: Notify::new(),
        }
    }
}

#[async_trait]
impl ProviderAdapter for BlockingAfterFileReadProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        _request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_read".into(),
                        name: "file_read".into(),
                        input: json!({"path": "note.txt", "show_linenos": false}),
                    }],
                    provider_replay: None,
                },
                stop: ProviderStop::ToolUse,
            }),
            1 => {
                self.second_call_started.notify_one();
                std::future::pending().await
            }
            call => anyhow::bail!("unexpected provider call {call}"),
        }
    }
}

struct FinalizingStateCheckingProvider {
    session_yaml: PathBuf,
}

#[async_trait]
impl ProviderAdapter for FinalizingStateCheckingProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        _request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let metadata = crate::storage::read_yaml::<SessionMetadata>(&self.session_yaml).await?;
        assert_eq!(metadata.status, SessionStatus::Finalizing);
        Ok(provider_response(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        ))
    }
}

struct NoopInboxGenerator;

#[async_trait]
impl InboxJsonGenerator for NoopInboxGenerator {
    async fn generate_json(
        &self,
        _kind: InboxInternalizeKind,
        _request: InternalizeRequest,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(json!({}))
    }
}

struct EmptyRouterClient;

#[async_trait]
impl RouterClient for EmptyRouterClient {
    async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        Ok(RouterQueryResult {
            candidate_claims: Vec::new(),
            disputes: Vec::new(),
            retrieval_debug: None,
        })
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        Ok(ScopesOverviewSnapshot::default())
    }
}

struct NoopMaintainerClient;

#[async_trait]
impl MaintainerClient for NoopMaintainerClient {
    async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
        Ok(Vec::new())
    }

    async fn ack_inbox(&self, _agent_id: &AgentId, _inbox_ids: &[InboxId]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn upload_claim(&self, _claim: &Claim) -> anyhow::Result<()> {
        Ok(())
    }

    async fn report_dispute(&self, _dispute: &Dispute) -> anyhow::Result<()> {
        Ok(())
    }
}

struct NoopDelegationExecutor;

#[async_trait]
impl DelegationExecutor for NoopDelegationExecutor {
    async fn execute(
        &self,
        _context: DelegationExecutionContext,
        _progress: DelegationProgressSink,
    ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
        Ok(DelegationExecutionOutcome {
            summary: "noop".into(),
            changed_files: Vec::new(),
            artifacts: Vec::new(),
        })
    }
}

fn provider_response(text: &str) -> ProviderResponse {
    ProviderResponse {
        assistant_message: SessionTurnMessage::assistant_text(text),
        stop: ProviderStop::Done,
    }
}

fn response_step(text: &str, events: Vec<ProviderEvent>) -> ProviderStep {
    ProviderStep::Response {
        response: provider_response(text),
        events,
    }
}

fn tool_use_step(id: &str, name: &str, input: serde_json::Value) -> ProviderStep {
    ProviderStep::Response {
        response: ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: id.into(),
                    name: name.into(),
                    input,
                }],
                provider_replay: None,
            },
            stop: ProviderStop::ToolUse,
        },
        events: Vec::new(),
    }
}

fn json_by_request_kind_step(compaction_text: &str, recap_text: &str) -> ProviderStep {
    ProviderStep::JsonByRequestKind {
        compaction_response: Some(provider_response(compaction_text)),
        recap_response: Some(provider_response(recap_text)),
    }
}

fn error_step(message: &'static str, events: Vec<ProviderEvent>) -> ProviderStep {
    ProviderStep::Error { message, events }
}

fn exhausted_stream_failure_steps(
    message: &'static str,
    partial: &'static str,
) -> Vec<ProviderStep> {
    let mut steps = vec![error_step(
        message,
        vec![ProviderEvent::AssistantTextDelta {
            text: partial.into(),
        }],
    )];
    for _ in 0..5 {
        steps.push(error_step("non-streaming fallback failed", Vec::new()));
    }
    steps
}

fn last_user_text(request: &ProviderRequest) -> String {
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "user")
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|block| match block {
                    SessionTurnContentBlock::Text { text } => Some(text.as_str()),
                    SessionTurnContentBlock::SkillInstructions { .. } => None,
                    SessionTurnContentBlock::Image { .. }
                    | SessionTurnContentBlock::Document { .. }
                    | SessionTurnContentBlock::ToolUse { .. }
                    | SessionTurnContentBlock::ToolResult { .. } => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn text_content(message: &SessionMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            SessionContentBlock::Text { text } => Some(text.as_str()),
            SessionContentBlock::SkillInstructions { .. } => None,
            SessionContentBlock::Image { .. }
            | SessionContentBlock::Document { .. }
            | SessionContentBlock::ToolUse { .. }
            | SessionContentBlock::ToolResult { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn recv_journal_kind_and_ack(
    rx: &mut mpsc::UnboundedReceiver<super::TurnJournalCommand>,
) -> TurnJournalEventKind {
    let command = recv_journal_command(rx).await;
    let super::TurnJournalCommand { kind, ack, .. } = command;
    if let Some(ack) = ack {
        let _ = ack.send(Ok(()));
    }
    kind
}

async fn recv_journal_command(
    rx: &mut mpsc::UnboundedReceiver<super::TurnJournalCommand>,
) -> super::TurnJournalCommand {
    rx.recv().await.unwrap()
}

#[tokio::test]
async fn committed_turn_control_forwarder_discards_late_control_events() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sink = TurnJournalSink { tx };
    let (control, receiver) = SessionTurnControl::channel();
    let mut forwarder = spawn_turn_control_journal_forwarder(sink, receiver);

    forwarder.wait_initial_drain().await;
    forwarder.set_drain_on_shutdown(false);
    assert!(!tokio::time::timeout(
        Duration::from_secs(1),
        control.request_tool_boundary_steer("late steer")
    )
    .await
    .unwrap());
    forwarder.shutdown.cancel();
    forwarder.handle.await.unwrap();

    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn turn_control_forwarder_records_interrupt_requested_before_pending() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sink = TurnJournalSink { tx };
    let (control, receiver) = SessionTurnControl::channel();
    let forwarder = spawn_turn_control_journal_forwarder(sink, receiver);

    let request = tokio::spawn(async move {
        control
            .request_tool_boundary_steer("change direction")
            .await
    });
    let mut kinds = Vec::new();
    for _ in 0..3 {
        kinds.push(recv_journal_kind_and_ack(&mut rx).await);
    }
    assert!(request.await.unwrap());
    forwarder.shutdown.cancel();
    forwarder.handle.await.unwrap();

    assert!(matches!(
        kinds.as_slice(),
        [
            TurnJournalEventKind::UserSteerSubmitted { .. },
            TurnJournalEventKind::InterruptRequested { .. },
            TurnJournalEventKind::InterruptPending { .. },
        ]
    ));
}

#[tokio::test]
async fn tool_boundary_control_waits_for_durable_journal_ack_before_cancelling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sink = TurnJournalSink { tx };
    let (control, receiver) = SessionTurnControl::channel();
    let tool_boundary_control = receiver.tool_boundary_control();
    let forwarder = spawn_turn_control_journal_forwarder(sink, receiver);

    let request =
        tokio::spawn(async move { control.request_tool_boundary_steer("durable steer").await });

    let command = recv_journal_command(&mut rx).await;
    let super::TurnJournalCommand { kind, ack, .. } = command;
    assert!(matches!(
        kind,
        TurnJournalEventKind::UserSteerSubmitted { .. }
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!tool_boundary_control.is_cancelled());
    ack.unwrap().send(Ok(())).unwrap();

    let command = recv_journal_command(&mut rx).await;
    let super::TurnJournalCommand { kind, ack, .. } = command;
    assert!(matches!(
        kind,
        TurnJournalEventKind::InterruptRequested { .. }
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!tool_boundary_control.is_cancelled());
    ack.unwrap().send(Ok(())).unwrap();

    let command = recv_journal_command(&mut rx).await;
    let super::TurnJournalCommand { kind, ack, .. } = command;
    assert!(matches!(
        kind,
        TurnJournalEventKind::InterruptPending { .. }
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!tool_boundary_control.is_cancelled());
    ack.unwrap().send(Ok(())).unwrap();

    assert!(request.await.unwrap());
    assert!(tool_boundary_control.is_cancelled());
    forwarder.shutdown.cancel();
    forwarder.handle.await.unwrap();
}

#[tokio::test]
async fn cancel_keeps_reason_when_pending_steer_ack_completes_later() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sink = TurnJournalSink { tx };
    let (control, receiver) = SessionTurnControl::channel();
    let tool_boundary_control = receiver.tool_boundary_control();
    let interrupt_status = receiver.interrupt_status_cell();
    let forwarder = spawn_turn_control_journal_forwarder(sink, receiver);
    let steer_control = control.clone();
    let steer = tokio::spawn(async move {
        steer_control
            .request_tool_boundary_steer("change direction")
            .await
    });

    let command = recv_journal_command(&mut rx).await;
    let super::TurnJournalCommand { kind, ack, .. } = command;
    assert!(matches!(
        kind,
        TurnJournalEventKind::UserSteerSubmitted { .. }
    ));

    assert!(control.request_tool_boundary_cancel_now("user cancelled turn"));
    assert_eq!(
        tool_boundary_control.cancel_reason(),
        Some(ToolCallSkipReason::TurnCancelledBeforeDispatch)
    );

    ack.unwrap().send(Ok(())).unwrap();
    for expected in ["steer interrupt requested", "steer interrupt pending"] {
        let command = recv_journal_command(&mut rx).await;
        let super::TurnJournalCommand { kind, ack, .. } = command;
        match expected {
            "steer interrupt requested" => {
                assert!(matches!(
                    kind,
                    TurnJournalEventKind::InterruptRequested { .. }
                ));
            }
            "steer interrupt pending" => {
                assert!(matches!(
                    kind,
                    TurnJournalEventKind::InterruptPending { .. }
                ));
            }
            _ => unreachable!("test only enumerates expected steer journal events"),
        }
        ack.unwrap().send(Ok(())).unwrap();
    }
    assert!(steer.await.unwrap());
    assert_eq!(
        tool_boundary_control.cancel_reason(),
        Some(ToolCallSkipReason::TurnCancelledBeforeDispatch)
    );
    assert_eq!(
        interrupt_status.lock().unwrap().as_ref(),
        Some(&TurnJournalStatus::Cancelled)
    );

    for expected in ["cancel interrupt requested", "cancel interrupt pending"] {
        let command = recv_journal_command(&mut rx).await;
        let super::TurnJournalCommand { kind, ack, .. } = command;
        match expected {
            "cancel interrupt requested" => {
                assert!(matches!(
                    kind,
                    TurnJournalEventKind::InterruptRequested { .. }
                ));
            }
            "cancel interrupt pending" => {
                assert!(matches!(
                    kind,
                    TurnJournalEventKind::InterruptPending { .. }
                ));
            }
            _ => unreachable!("test only enumerates expected cancel journal events"),
        }
        ack.unwrap().send(Ok(())).unwrap();
    }
    forwarder.shutdown.cancel();
    forwarder.handle.await.unwrap();
}

#[tokio::test]
async fn tool_boundary_cancel_returns_before_journal_forwarder_drains() {
    let (control, receiver) = SessionTurnControl::channel();
    let tool_boundary_control = receiver.tool_boundary_control();
    let interrupt_status = receiver.interrupt_status_cell();

    let accepted = tokio::time::timeout(
        Duration::from_millis(50),
        control.request_tool_boundary_cancel("user cancelled turn"),
    )
    .await
    .expect("cancel request should not wait for journal forwarder");

    assert!(accepted);
    assert!(tool_boundary_control.is_cancelled());
    assert_eq!(
        interrupt_status.lock().unwrap().as_ref(),
        Some(&TurnJournalStatus::Cancelled)
    );
}

#[tokio::test]
async fn cancelled_turn_journal_settlement_is_bounded_when_writer_stalls() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let emitter = TurnJournalEmitter::new(tx, Duration::from_secs(3600), usize::MAX);
    let writer = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });
    let started = std::time::Instant::now();

    tokio::time::timeout(
        Duration::from_millis(250),
        finish_cancelled_turn_journal(emitter, writer, None),
    )
    .await
    .expect("cancelled turn must not wait indefinitely for a stalled journal writer");
    assert!(
        started.elapsed() < Duration::from_millis(180),
        "cancelled journal settlement exceeded the shared 100ms grace"
    );
}

fn build_test_engine(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
) -> (SessionEngine, SessionStore) {
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    build_test_engine_with_tools(dir, provider, tools)
}

fn build_local_test_engine(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
) -> (SessionEngine, SessionStore) {
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    build_test_engine_with_team_mode(dir, provider, tools, Vec::new(), false)
}

fn build_test_engine_with_delegation_host(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
) -> (SessionEngine, SessionStore) {
    let agent = AgentId::new("agent-a").unwrap();
    let agents_root = dir.path().join("agents");
    let agent_home = agents_root.join(agent.as_str());
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(
        ToolRegistry::new(&tool_config)
            .unwrap()
            .with_delegation_executor(
                agent_home,
                agent,
                Arc::new(NoopDelegationExecutor),
                DelegationRunnerConfig::default(),
            ),
    );
    build_test_engine_with_tools(dir, provider, tools)
}

fn build_test_engine_with_tools(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
    tools: Arc<ToolRegistry>,
) -> (SessionEngine, SessionStore) {
    build_test_engine_with_tools_and_skills(dir, provider, tools, Vec::new())
}

fn build_test_engine_with_tools_and_skills(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
    tools: Arc<ToolRegistry>,
    available_skills: Vec<SkillSummary>,
) -> (SessionEngine, SessionStore) {
    build_test_engine_with_team_mode(dir, provider, tools, available_skills, true)
}

fn build_test_engine_with_team_mode(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
    tools: Arc<ToolRegistry>,
    available_skills: Vec<SkillSummary>,
    team_services_configured: bool,
) -> (SessionEngine, SessionStore) {
    let agent = AgentId::new("agent-a").unwrap();
    let agents_root = dir.path().join("agents");
    let agent_home = agents_root.join(agent.as_str());
    let claim_store: Arc<dyn LocalClaimStore> =
        Arc::new(LocalFsClaimStore::new(agent_home.clone()));
    let reported_store: Arc<dyn ReportedDisputeClaimSetStore> =
        Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone()));
    let inbox: Arc<dyn InboxReader> = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
    let memory_store: Arc<dyn MemoryStore> = Arc::new(LocalFsMemoryStore::new(
        agent_home.clone(),
        1024,
        1024,
        false,
    ));
    let upload_queue = Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone()));
    let runner = Arc::new(if team_services_configured {
        AgentRunner::new(
            agent,
            Arc::new(NoopInboxGenerator),
            claim_store,
            reported_store,
            inbox,
            memory_store,
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            upload_queue,
            0,
            available_skills,
        )
    } else {
        AgentRunner::new_local(
            agent,
            Arc::new(NoopInboxGenerator),
            claim_store,
            reported_store,
            inbox,
            memory_store,
            upload_queue,
            0,
            available_skills,
        )
    });
    let provider_for_loops = provider;
    let turn_loop = Arc::new(AgentTurnLoop::new(
        provider_for_loops.clone(),
        tools.clone(),
        1024,
    ));
    let memory_review_loop = Arc::new(MemoryReviewLoop::new(
        provider_for_loops.clone(),
        tools,
        2,
        1024,
    ));
    let json_caller = Arc::new(StructuredJsonCaller::new(
        provider_for_loops,
        1024,
        0,
        Duration::from_millis(1),
        Duration::from_millis(1),
    ));
    let store = SessionStore::new(agents_root);
    let engine = SessionEngine::new(
        runner,
        turn_loop,
        memory_review_loop,
        json_caller,
        Arc::new(PromptRegistry::bundled().unwrap()),
        store.clone(),
        super::super::session_engine::SessionEngineOptions {
            compaction: SessionCompactionConfig {
                auto_compact_ctx_ratio: 0.0,
                ..Default::default()
            },
            skills: crate::config::AgentSessionSkillConfig::default(),
            context_window: 200_000,
            user_shell: UserShellConfig::default(),
            workspace_root: dir.path().to_path_buf(),
            turn_journal: AgentSessionTurnJournalConfig {
                delta_snapshot_interval_ms: 10_000,
                delta_snapshot_chars: 2,
                ..Default::default()
            },
            subagent_max_concurrent: 7,
        },
    )
    .with_session_metadata("test", "test-model");
    (engine, store)
}

async fn create_test_session(store: &SessionStore, id: &str) -> crate::session::SessionHandle {
    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = id.parse().unwrap();
    store
        .create_with_id_factory(&agent, "system prompt", || session_id.clone(), 1)
        .await
        .unwrap()
}

#[tokio::test]
async fn manual_inbox_rejects_solo_mode_without_changing_session_to_error() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_local_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_1234abcd").await;
    let mut events = Vec::new();

    let error = engine
        .process_inbox_during_session(&session, |event| events.push(event))
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "团队服务未配置，请参考 docs/config_parameters.md 文档配置 maintainer_endpoint/router_endpoint"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::InboxFailed { error }
            if error.contains("团队服务未配置")
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Error
        } | SessionEvent::InboxStarted
    )));
    assert_eq!(
        session.read_metadata().await.unwrap().status,
        SessionStatus::Open
    );
}

#[tokio::test]
async fn solo_mode_session_start_reports_unknown_team_status() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, _) = build_local_test_engine(&dir, provider);
    let mut events = Vec::new();

    let report = engine
        .start_session(1, |event| events.push(event))
        .await
        .unwrap();

    assert_eq!(
        report.inbox_report.team_services,
        crate::agent::TeamServicesConnectionStatus::default()
    );
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::TeamServicesConnectionUpdated { status }
            if *status == crate::agent::TeamServicesConnectionStatus::default()
    )));
    let system_text = tokio::fs::read_to_string(&report.session.paths.system_prompt)
        .await
        .unwrap();
    assert!(system_text.contains("本 session 以单人模式运行"));
    assert!(system_text.contains("docs/config_parameters.md"));
}

#[tokio::test]
async fn resume_inbox_refresh_reports_configured_team_status() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_1234abcd").await;

    let report = engine.process_inbox_for_resume(&session).await.unwrap();

    assert_eq!(
        report.team_services,
        crate::agent::TeamServicesConnectionStatus {
            maintainer: crate::agent::TeamServiceConnectionStatus::Connected,
            router: crate::agent::TeamServiceConnectionStatus::Connected,
        }
    );
}

fn test_message(
    index: usize,
    role: SessionMessageRole,
    content: Vec<SessionContentBlock>,
) -> SessionMessage {
    SessionMessage {
        index,
        role,
        content,
        created_at: Utc::now(),
        model: "test-model".into(),
        provider_replay: None,
    }
}

#[tokio::test]
async fn session_system_prompt_renders_configured_subagent_concurrency_limit() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, _) = build_test_engine(&dir, provider);

    let inbox_report = crate::agent::InboxProcessReport {
        team_services: crate::agent::TeamServicesConnectionStatus {
            maintainer: crate::agent::TeamServiceConnectionStatus::Connected,
            router: crate::agent::TeamServiceConnectionStatus::Connected,
        },
        router_scopes_overview: Some(ScopesOverviewSnapshot::default()),
        ..Default::default()
    };
    let prompt = engine
        .render_session_system_prompt_for_inbox(&inbox_report)
        .await
        .unwrap();

    assert!(prompt.contains("当前同一 session 最多允许 7 个 subagent 同时 running"));
    assert!(!prompt.contains("当前同一 session 最多允许 6 个 subagent 同时 running"));
    assert!(prompt.contains("subagent 进入终态时会被自动清理"));
    assert!(prompt.contains("当前不支持将这类进程转交给你"));
    assert!(prompt.contains("必须从一开始就由你直接调用 `code_run` 创建和管理"));
    assert!(prompt.contains("保持 running、做有界轮询"));
}

#[tokio::test]
async fn run_turn_success_writes_committed_journal_and_canonical_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "assistant done",
        vec![
            ProviderEvent::AssistantTextDelta {
                text: "assistant ".into(),
            },
            ProviderEvent::AssistantTextDelta {
                text: "done".into(),
            },
            ProviderEvent::AssistantMessageCompleted {
                text: "assistant done".into(),
            },
        ],
    )]));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_aaaaaaa1").await;

    engine
        .run_turn(&mut session, "hello user", |_| {})
        .await
        .unwrap();

    let messages = session.read_messages().await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(text_content(&messages[0]), "hello user");
    assert_eq!(text_content(&messages[1]), "assistant done");
    let expected_hash = canonical_user_content_hash(&messages[0].content).unwrap();
    let journal_read = session.read_turn_journal().await;
    assert!(journal_read.events.iter().any(|event| matches!(
        &event.kind,
        TurnJournalEventKind::CanonicalUserMessage {
            content_hash: Some(content_hash),
            content: None,
        } if content_hash == &expected_hash
    )));
    let projection = replay_turn_journal(journal_read);
    assert_eq!(projection.turns.len(), 1);
    assert_eq!(
        projection.turns[0].status,
        Some(TurnJournalStatus::Committed)
    );
    assert_eq!(projection.turns[0].assistant_text, "assistant done");
    assert!(projection.unresolved_tail().is_none());
}

#[tokio::test]
async fn text_attachment_keeps_journal_input_aligned_with_canonical_user_message() {
    let dir = tempfile::tempdir().unwrap();
    let attachment_path = dir.path().join("large.rs");
    tokio::fs::write(&attachment_path, "fn very_long() {}\n".repeat(2_000))
        .await
        .unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "已检查",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_aaaaaaa2").await;
    let user_text = format!("请检查 @{}", attachment_path.display());

    engine
        .run_turn_with_attachments(
            &mut session,
            user_text.clone(),
            vec![crate::api::SessionAttachment::TextFile {
                path: attachment_path,
            }],
            |_| {},
        )
        .await
        .unwrap();

    let messages = session.read_messages().await.unwrap();
    assert!(matches!(
        messages[0].content.first(),
        Some(SessionContentBlock::Text { text }) if text == &user_text
    ));
    assert!(matches!(
        messages[0].content.get(1),
        Some(SessionContentBlock::Text { text }) if text.contains("fn very_long")
    ));

    let journal_read = session.read_turn_journal().await;
    let serialized_journal = serde_json::to_string(&journal_read.events).unwrap();
    assert!(serialized_journal.contains("content_hash"));
    assert!(!serialized_journal.contains("fn very_long"));
    let projection = replay_turn_journal(journal_read);
    assert_eq!(projection.turns.len(), 1);
    assert_eq!(
        projection.turns[0].original_user_request.as_deref(),
        Some(user_text.as_str())
    );
    let expected_hash = canonical_user_content_hash(&messages[0].content).unwrap();
    assert_eq!(
        projection.turns[0].canonical_user_content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(projection.turns[0].canonical_user_first_text, None);
}

#[tokio::test]
async fn explicit_skill_is_snapshotted_before_user_text_and_persisted_in_journal() {
    let dir = tempfile::tempdir().unwrap();
    let skill_dir = dir.path().join("skills").join("review");
    tokio::fs::create_dir_all(&skill_dir).await.unwrap();
    let spec_path = skill_dir.join("SKILL.md");
    tokio::fs::write(
        &spec_path,
        "# Review\n\nRead $ARGUMENTS[0] with $0, then report only P1.",
    )
    .await
    .unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step("done", Vec::new()),
        response_step("second done", Vec::new()),
    ]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools_and_skills(
        &dir,
        provider.clone(),
        tools,
        vec![SkillSummary {
            name: "review".into(),
            description: "review code".into(),
            spec_path,
        }],
    );
    let mut session = create_test_session(&store, "session_51a11a51").await;

    engine
        .run_turn(&mut session, "/review src/auth.rs", |_| {})
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "继续处理当前任务", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let user = requests[0]
        .messages
        .iter()
        .find(|message| message.role == "user")
        .unwrap();
    assert!(matches!(
        user.content.first(),
        Some(SessionTurnContentBlock::SkillInstructions { instruction })
            if instruction.content.contains("Read src/auth.rs with src/auth.rs")
    ));
    assert!(matches!(
        user.content.get(1),
        Some(SessionTurnContentBlock::Text { text })
            if text.contains("/review src/auth.rs")
    ));
    assert!(requests[1].messages.iter().any(|message| {
        matches!(
            message.content.first(),
            Some(SessionTurnContentBlock::SkillInstructions { instruction })
                if instruction.content.contains("Read src/auth.rs with src/auth.rs")
        )
    }));

    let messages = session.read_messages().await.unwrap();
    assert!(matches!(
        messages[0].content.first(),
        Some(SessionContentBlock::SkillInstructions { instruction })
            if instruction.content.contains("Read src/auth.rs with src/auth.rs")
    ));
    assert_eq!(text_content(&messages[0]), "/review src/auth.rs");
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].skill_instructions.len(), 1);
    assert_eq!(projection.turns[0].skill_instructions[0].name, "review");
}

#[tokio::test]
async fn visible_composer_source_does_not_scan_expanded_paste_for_skills() {
    let dir = tempfile::tempdir().unwrap();
    let skill_dir = dir.path().join("skills").join("review");
    tokio::fs::create_dir_all(&skill_dir).await.unwrap();
    let spec_path = skill_dir.join("SKILL.md");
    tokio::fs::write(&spec_path, "# Review\nnever inject from paste")
        .await
        .unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "done",
        Vec::new(),
    )]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools_and_skills(
        &dir,
        provider.clone(),
        tools,
        vec![SkillSummary {
            name: "review".into(),
            description: "review code".into(),
            spec_path,
        }],
    );
    let mut session = create_test_session(&store, "session_51a11a52").await;

    let mut emitted_user_text = None;
    engine
        .run_turn_with_attachments_and_skill_source_controlled(
            &mut session,
            "请看粘贴内容：\n/review hidden".to_string(),
            Vec::new(),
            Some("请看 [Pasted Content #1]".to_string()),
            None,
            |event| {
                if let SessionEvent::UserMessageAccepted { text } = event {
                    emitted_user_text = Some(text);
                }
            },
        )
        .await
        .unwrap();

    assert_eq!(
        emitted_user_text.as_deref(),
        Some("请看 [Pasted Content #1]")
    );

    let requests = provider.requests().await;
    let user = requests[0]
        .messages
        .iter()
        .find(|message| message.role == "user")
        .unwrap();
    assert!(!user
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::SkillInstructions { .. })));
    let messages = session.read_messages().await.unwrap();
    assert_eq!(text_content(&messages[0]), "请看粘贴内容：\n/review hidden");
}

#[tokio::test]
async fn preflight_active_compaction_runs_before_next_provider_request_and_clears_on_commit() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![
                        SessionTurnContentBlock::text("I will write a note."),
                        SessionTurnContentBlock::ToolUse {
                            id: "toolu_1".into(),
                            name: "working_note".into(),
                            input: json!({"action": "add", "note": "remember active compact"}),
                        },
                    ],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "The assistant wrote a working note for the active task."}"#,
            Vec::new(),
        ),
        response_step("final answer after compact", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee01").await;
    let image_path = dir.path().join("active-anchor.png");
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    tokio::fs::write(&image_path, image_bytes).await.unwrap();

    engine
        .run_turn_with_attachments(
            &mut session,
            "please do a long active turn",
            vec![SessionAttachment::LocalImage { path: image_path }],
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[1]
        .messages
        .first()
        .and_then(|message| message.content.first())
        .is_some_and(|block| matches!(
            block,
            SessionTurnContentBlock::Text { text } if text.contains("committed_transcript")
        )));
    let compaction_request = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(compaction_request.contains("active_turn_user_anchor"));
    assert!(compaction_request.contains("please do a long active turn"));
    let raw_image = requests[0]
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|block| match block {
            SessionTurnContentBlock::Image { data, .. } => Some(data.clone()),
            _ => None,
        })
        .unwrap();
    assert!(!compaction_request.contains(&raw_image));
    assert!(compaction_request.contains("image attachment media_type=image/png"));
    assert_eq!(requests[2].system_prompt, "system prompt");
    let final_request = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(final_request.contains("please do a long active turn"));
    assert!(final_request.contains(&raw_image));
    assert!(final_request.contains("compacted_current_turn_progress"));
    assert!(final_request.contains("The assistant wrote a working note"));
    assert!(!final_request.contains("remember active compact"));

    let messages = session.read_messages().await.unwrap();
    assert_eq!(messages.len(), 4);
    assert!(messages.iter().any(|message| {
        message.content.iter().any(|block| {
            matches!(
                block,
                SessionContentBlock::ToolResult { content, .. }
                    if content.contains("working_note")
                        || content.contains("remember active compact")
            )
        })
    }));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    let compaction = metadata.compaction.unwrap();
    assert!(compaction.active_turn_summary.is_none());
    assert!(compaction.frontier.active_turn.is_none());
}

#[tokio::test]
async fn active_turn_compaction_persists_provider_context_high_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "anchor must not persist"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: vec![ProviderEvent::ContextUsageUpdated {
                usage: ContextUsageSnapshot {
                    used_tokens: 900,
                    source: ContextUsageSource::Provider,
                },
            }],
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "The active tool result was summarized."}"#,
            Vec::new(),
        ),
        response_step(
            "final answer after active compact",
            vec![ProviderEvent::ContextUsageUpdated {
                usage: ContextUsageSnapshot {
                    used_tokens: 1_200,
                    source: ContextUsageSource::Provider,
                },
            }],
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee0e").await;

    engine
        .run_turn(&mut session, "please compact active work", |_| {})
        .await
        .unwrap();

    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.message_count, 4);
    let anchor = engine
        .active_context_usage_anchor(&metadata.id, metadata.message_count)
        .unwrap();
    assert_eq!(anchor.used_tokens, 1_200);
}

#[tokio::test]
async fn final_provider_request_without_usage_clears_partial_context_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "partial usage"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: vec![ProviderEvent::ContextUsageUpdated {
                usage: ContextUsageSnapshot {
                    used_tokens: 900,
                    source: ContextUsageSource::Provider,
                },
            }],
        },
        response_step("final answer without usage", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee10").await;
    let mut events = Vec::new();

    engine
        .run_turn(&mut session, "use a tool then answer", |event| {
            events.push(event)
        })
        .await
        .unwrap();

    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.message_count, 4);
    assert!(engine
        .active_context_usage_anchor(&metadata.id, metadata.message_count)
        .is_none());
    assert!(
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::ContextUsageUpdated { .. }))
            .count()
            >= 2
    );
}

#[tokio::test]
async fn preflight_active_compaction_preserves_prior_summary_across_multiple_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "first active compact"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "First active tool round summarized."}"#,
            Vec::new(),
        ),
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_2".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "second active compact"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "First and second active tool rounds summarized."}"#,
            Vec::new(),
        ),
        response_step("final answer after two active compacts", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee07").await;

    engine
        .run_turn(&mut session, "please compact twice in one turn", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5);
    let second_compaction_payload = last_user_text(&requests[3]);
    assert!(second_compaction_payload.contains("First active tool round summarized."));
    assert_eq!(requests[4].system_prompt, "system prompt");
    let final_request = serde_json::to_string(&requests[4].messages).unwrap();
    assert!(final_request.contains("please compact twice in one turn"));
    assert!(final_request.contains("First and second active tool rounds summarized."));
    assert!(!final_request.contains("first active compact"));
    assert!(!final_request.contains("second active compact"));
}

#[tokio::test]
async fn preflight_does_not_reuse_active_summary_from_previous_turn() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee04").await;
    let mut stale = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    stale.active_turn_summary = Some("stale active summary".into());
    stale.frontier.active_turn = Some(ActiveTurnCompactionCursor {
        turn_id: "turn_old".into(),
        base_message_count: 0,
        compacted_until_segment: 1,
        safe_until_event_seq: 0,
        source_hash: "old".into(),
    });
    session.update_compaction(stale).await.unwrap();

    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![
        SessionTurnMessage::user_text(
            "<runtime_context>\ncurrent_date: 2026-06-30 Tuesday\n</runtime_context>\n\nnew task",
        ),
        SessionTurnMessage::assistant_text("new progress"),
    ];
    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_new",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    assert!(plan.prior_active_turn_summary.is_none());
    assert!(plan.prior_active_turn_cursor.is_none());
    assert!(plan.active_turn.is_some());
}

#[tokio::test]
async fn failed_turn_clears_active_compaction_state() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "will be compacted before failure"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "The working note tool round completed before the provider failure."}"#,
            Vec::new(),
        ),
        error_step("provider failed after active compaction", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee05").await;

    let err = engine
        .run_turn(&mut session, "compact then fail", |_| {})
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("provider failed after active compaction"));
    let metadata = session.read_metadata().await.unwrap();
    let compaction = metadata.compaction.unwrap();
    assert!(compaction.active_turn_summary.is_none());
    assert!(compaction.frontier.active_turn.is_none());
}

#[tokio::test]
async fn preflight_compaction_summarizes_oversized_previous_turn_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![json_by_request_kind_step(
        r#"{"committed_summary": "older committed context summarized", "active_turn_summary": null}"#,
        r#"{"new_claims": [], "used_claim_ids": [], "new_disputes": []}"#,
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 10_000;
    engine.compaction.tail_target_ctx_ratio = 0.001;
    engine.compaction.tail_hard_ctx_ratio = 0.03;
    let mut session = create_test_session(&store, "session_c0ffee06").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "older task"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "older answer"),
            NewSessionMessage::text(SessionMessageRole::User, "x ".repeat(1000)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "large tail answer"),
        ])
        .await
        .unwrap();

    let mut events = Vec::new();
    let projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                active_suffix: vec![SessionTurnMessage::user_text("small current request")],
                turn_id: "turn_1",
                base_message_count: 4,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
            },
            &mut |event| events.push(event),
        )
        .await
        .unwrap()
        .unwrap();

    let rendered = serde_json::to_string(&projection.messages).unwrap();
    assert!(rendered.contains("small current request"));
    assert!(!rendered.contains(&"x ".repeat(100)));
    let metadata = session.read_metadata().await.unwrap();
    let compaction = metadata.compaction.unwrap();
    assert_eq!(compaction.committed_message_until(), 4);
    assert_eq!(metadata.recapped_until, 4);
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionTurnEvent::CompactionStarted { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionTurnEvent::CompactionCompleted { .. })));
}

fn oversized_active_suffix(
    original_text: String,
    skill_body: Option<String>,
    attachment_body: Option<String>,
) -> Vec<SessionTurnMessage> {
    let mut content = Vec::new();
    if let Some(skill_body) = skill_body {
        content.push(SessionTurnContentBlock::skill_instructions(
            SkillInstructions {
                name: "large-skill".into(),
                spec_path: PathBuf::from("/tmp/large-skill/SKILL.md"),
                base_dir: PathBuf::from("/tmp/large-skill"),
                arguments: None,
                content: skill_body,
                content_hash: "skill-source-hash".into(),
            },
        ));
    }
    content.push(SessionTurnContentBlock::text(original_text));
    if let Some(attachment_body) = attachment_body {
        content.push(SessionTurnContentBlock::text(format!(
            "Attached file: large.txt\nPath: /tmp/large.txt\n\n{attachment_body}"
        )));
    }
    vec![
        SessionTurnMessage::user_content(content),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_large".into(),
                name: "working_note".into(),
                input: json!({"action": "add", "note": "large active work"}),
            }],
        },
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_large".into(),
                content: "large tool output ".repeat(1_000),
            }],
        },
    ]
}

#[tokio::test]
async fn preflight_externalizes_skill_and_attachment_only_after_full_projection_overflows() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary": null, "active_turn_summary": "active work summarized"}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider);
    // 给 compact wrapper 留出扩展空间，同时仍保证大 Skill/附件正文必须外置。
    engine.context_window = 40_000;
    engine.compaction.tail_target_ctx_ratio = 0.0075;
    engine.compaction.tail_hard_ctx_ratio = 0.01875;
    let mut session = create_test_session(&store, "session_c0ffee11").await;
    let active_suffix = oversized_active_suffix(
        "keep this original request".into(),
        Some("skill body ".repeat(1_000)),
        Some("attachment body ".repeat(1_000)),
    );

    let projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 0,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap()
        .unwrap();

    let rendered = serde_json::to_string(&projection.messages).unwrap();
    assert!(rendered.contains("keep this original request"));
    assert!(rendered.contains("<externalized_compaction_asset>"));
    assert!(!rendered.contains(&"skill body ".repeat(100)));
    assert!(!rendered.contains(&"attachment body ".repeat(100)));
    let mut assets = tokio::fs::read_dir(&session.paths.compaction_assets_dir)
        .await
        .unwrap();
    let mut asset_count = 0;
    while assets.next_entry().await.unwrap().is_some() {
        asset_count += 1;
    }
    assert_eq!(asset_count, 2);
    let journal = tokio::fs::read_to_string(&session.paths.turn_events_jsonl)
        .await
        .unwrap();
    assert!(journal.contains(r#""kind":"compaction_assets_externalized""#));
    assert!(!journal.contains(&"skill body ".repeat(100)));
    assert!(!journal.contains(&"attachment body ".repeat(100)));
    let replayed = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(replayed.turns[0].compaction_assets.len(), 2);
    let audit = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit.contains(r#""kind":"projection_externalized""#));
    assert!(audit.contains(r#""asset_count":2"#));
}

#[tokio::test]
async fn provider_only_externalization_does_not_change_committed_transcript_or_visible_user_event()
{
    let dir = tempfile::tempdir().unwrap();
    let skill_dir = dir.path().join("large-skill");
    tokio::fs::create_dir_all(&skill_dir).await.unwrap();
    let spec_path = skill_dir.join("SKILL.md");
    let skill_body = format!("# Large skill\n{}", "skill canonical ".repeat(800));
    tokio::fs::write(&spec_path, &skill_body).await.unwrap();
    let attachment_path = dir.path().join("large.txt");
    let attachment_body = "attachment canonical ".repeat(800);
    tokio::fs::write(&attachment_path, &attachment_body)
        .await
        .unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_externalize".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "compact this tool round"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "tool round summarized"}"#,
            Vec::new(),
        ),
        response_step("final answer", Vec::new()),
    ]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );
    let (mut engine, store) = build_test_engine_with_tools_and_skills(
        &dir,
        provider.clone(),
        tools,
        vec![SkillSummary {
            name: "large-skill".into(),
            description: "large skill for compaction test".into(),
            spec_path,
        }],
    );
    // Summary 请求允许使用完整 context window；最终 raw tail 只允许占其中 10%。
    // 因此 summary 能安全容纳原始 anchor，而最终 provider projection 仍必须外置重型块。
    engine.context_window = 50_000;
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.10;
    engine.compaction.tail_hard_ctx_ratio = 0.10;
    let mut session = create_test_session(&store, "session_c0ffee14").await;
    let mut events = Vec::new();

    engine
        .run_turn_with_attachments(
            &mut session,
            "/large-skill keep canonical input",
            vec![SessionAttachment::TextFile {
                path: attachment_path,
            }],
            |event| events.push(event),
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let final_projection = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(final_projection.contains("<externalized_compaction_asset>"));
    assert!(!final_projection.contains(&"skill canonical ".repeat(100)));
    assert!(!final_projection.contains(&"attachment canonical ".repeat(100)));
    let canonical = serde_json::to_string(&session.read_messages().await.unwrap()).unwrap();
    assert!(canonical.contains(&"skill canonical ".repeat(100)));
    assert!(canonical.contains(&"attachment canonical ".repeat(100)));
    assert!(!canonical.contains("<externalized_compaction_asset>"));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::UserMessageAccepted { text }
            if text == "/large-skill keep canonical input"
    )));
    assert!(!format!("{events:?}").contains("<externalized_compaction_asset>"));
}

#[tokio::test]
async fn preflight_retries_once_with_half_summary_limit_after_reference_projection_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let first_summary = "A".repeat(4_000);
    let retry_summary = "B".repeat(4_000);
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            &format!(r#"{{"committed_summary": null, "active_turn_summary": "{first_summary}"}}"#),
            Vec::new(),
        ),
        response_step(
            &format!(r#"{{"committed_summary": null, "active_turn_summary": "{retry_summary}"}}"#),
            Vec::new(),
        ),
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "bounded retry summary"}"#,
            Vec::new(),
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        1024,
        1,
        Duration::ZERO,
        Duration::ZERO,
    ));
    engine.context_window = 40_000;
    engine.compaction.tail_target_ctx_ratio = 0.01;
    // 首次 4000 字符摘要仍超投影预算；半长请求先收到 4000 字符的非法输出，
    // 随后通过结构化语义 repair 得到可提交摘要。
    engine.compaction.tail_hard_ctx_ratio = 0.0275;
    engine.compaction.summary_max_chars = 4_000;
    let mut session = create_test_session(&store, "session_c0ffee12").await;
    let active_suffix = oversized_active_suffix(
        "keep original".into(),
        Some("skill body ".repeat(1_000)),
        Some("attachment body ".repeat(1_000)),
    );

    let projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 0,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap()
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(last_user_text(&requests[0]).contains(r#""summary_max_chars": 4000"#));
    assert!(last_user_text(&requests[1]).contains(r#""summary_max_chars": 2000"#));
    assert!(last_user_text(&requests[2]).contains("exceeds summary_max_chars"));
    let rendered = serde_json::to_string(&projection.messages).unwrap();
    assert!(rendered.contains("<externalized_compaction_asset>"));
    assert!(!rendered.contains(&"A".repeat(100)));
    assert!(!rendered.contains(&"B".repeat(100)));
    assert!(rendered.contains("bounded retry summary"));
    let audit = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert_eq!(audit.matches(r#""kind":"started""#).count(), 2);
}

#[tokio::test]
async fn auto_compaction_failure_continues_with_raw_history_when_request_still_fits() {
    let dir = tempfile::tempdir().unwrap();
    let overlong = "X".repeat(11);
    let response = format!(
        r#"{{"committed_summary":{summary},"active_turn_summary":null}}"#,
        summary = serde_json::to_string(&overlong).unwrap()
    );
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(&response, Vec::new()),
        response_step(&response, Vec::new()),
        response_step("continued with full history", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.015;
    engine.compaction.tail_hard_ctx_ratio = 0.0225;
    engine.compaction.tail_previous_real_user_turns = 1;
    engine.compaction.summary_max_chars = 10;
    engine.json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        1024,
        1,
        Duration::ZERO,
        Duration::ZERO,
    ));
    let mut session = create_test_session(&store, "session_c0ffee30").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();
    let mut events = Vec::new();

    engine
        .run_turn(&mut session, "continue safely", |event| events.push(event))
        .await
        .expect("raw request still fits and should continue");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let final_request = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(final_request.contains("old request"));
    assert!(final_request.contains("continue safely"));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::Warning { message }
            if message == "Automatic compaction failed after 2 attempts; continuing with full history."
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::CompactionFailed { .. })));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_none());
    assert!(session
        .read_compaction_checkpoint()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn auto_compaction_provider_failure_continues_with_raw_history_when_request_still_fits() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step("summary provider unavailable", Vec::new()),
        response_step("continued after provider failure", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.015;
    engine.compaction.tail_hard_ctx_ratio = 0.0225;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_c0ffee32").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();
    let mut events = Vec::new();

    engine
        .run_turn(&mut session, "continue after outage", |event| {
            events.push(event)
        })
        .await
        .expect("a recoverable compaction provider failure should not abort a safe raw request");

    assert_eq!(provider.requests().await.len(), 2);
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::Warning { message }
            if message.contains("continuing with full history")
                && message.contains("summary provider unavailable")
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::CompactionFailed { .. })));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_none());
}

#[tokio::test]
async fn auto_compaction_projection_failure_continues_raw_when_full_request_still_fits() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            r#"{"committed_summary":"short summary","active_turn_summary":null}"#,
            Vec::new(),
        ),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
        response_step(
            r#"{"committed_summary":"tiny","active_turn_summary":null}"#,
            Vec::new(),
        ),
        response_step("continued after hard-tail failure", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.005;
    engine.compaction.tail_hard_ctx_ratio = 0.01;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_c0ffee33").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();
    let current_request = "mandatory current plain text ".repeat(100);
    let mut events = Vec::new();

    engine
        .run_turn(&mut session, current_request.clone(), |event| {
            events.push(event)
        })
        .await
        .expect("the full raw request fits even though the compact hard-tail projection does not");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    let final_request = serde_json::to_string(&requests[3].messages).unwrap();
    assert!(final_request.contains("old request"));
    assert!(final_request.contains(&current_request));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::Warning { message }
            if message.contains("continuing with full history")
                && message.contains("mandatory context")
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::CompactionFailed { .. })));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_none());
    assert!(session
        .read_compaction_checkpoint()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn auto_compaction_failure_blocks_when_raw_request_with_output_reserve_does_not_fit() {
    let dir = tempfile::tempdir().unwrap();
    let overlong = "X".repeat(11);
    let response = format!(
        r#"{{"committed_summary":{summary},"active_turn_summary":null}}"#,
        summary = serde_json::to_string(&overlong).unwrap()
    );
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(&response, Vec::new()),
        response_step(&response, Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 6_000;
    engine.compaction.auto_compact_ctx_ratio = 0.00001;
    engine.compaction.tail_target_ctx_ratio = 0.05;
    engine.compaction.tail_hard_ctx_ratio = 0.10;
    engine.compaction.tail_previous_real_user_turns = 1;
    engine.compaction.tool_result_raw_max_chars = 64;
    engine.compaction.summary_max_chars = 10;
    engine.json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        1024,
        1,
        Duration::ZERO,
        Duration::ZERO,
    ));
    let mut session = create_test_session(&store, "session_c0ffee31").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old tool task"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::tool_use(
                    "toolu_large_history",
                    "code_run",
                    json!({"command": "produce output"}),
                )],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "toolu_large_history",
                    "large raw tool output ".repeat(2_000),
                )],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old tool task complete"),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();
    let original_message_count = session.read_messages().await.unwrap().len();
    let mut events = Vec::new();

    let error = engine
        .run_turn(&mut session, "continue but preserve everything", |event| {
            events.push(event)
        })
        .await
        .expect_err("raw request plus output reserve exceeds the context window");

    assert_eq!(
        error.to_string(),
        "Context compaction failed: the generated summary exceeded 10 characters after 2 attempts. Run /compact to retry."
    );
    assert_eq!(provider.requests().await.len(), 2);
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::TurnFailed { error }
            if error == "Context compaction failed: the generated summary exceeded 10 characters after 2 attempts. Run /compact to retry."
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::CompactionFailed { .. })));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.message_count, original_message_count);
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_none());
    assert!(session
        .read_compaction_checkpoint()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn preflight_errors_after_single_retry_when_plain_user_text_remains_over_hard_budget() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "first"}"#,
            Vec::new(),
        ),
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "retry"}"#,
            Vec::new(),
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 2_000;
    engine.compaction.tail_target_ctx_ratio = 0.10;
    engine.compaction.tail_hard_ctx_ratio = 0.25;
    let mut session = create_test_session(&store, "session_c0ffee13").await;
    let active_suffix = oversized_active_suffix("plain user text ".repeat(2_000), None, None);

    let error = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 0,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("remains over budget after omitting all tool results"));
    assert!(provider.requests().await.is_empty());
    assert!(session.read_metadata().await.unwrap().compaction.is_none());
}

#[tokio::test(start_paused = true)]
async fn run_turn_failure_writes_failed_journal_without_canonical_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(exhausted_stream_failure_steps(
        "provider stream failed",
        "partial output",
    )));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_bbbbbbb2").await;

    let err = engine
        .run_turn(&mut session, "first request", |_| {})
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("non-streaming fallback exhausted after 5/5"));
    assert!(session.read_messages().await.unwrap().is_empty());
    let projection = replay_turn_journal(session.read_turn_journal().await);
    let turn = projection.unresolved_tail().unwrap();
    assert_eq!(turn.status, Some(TurnJournalStatus::Failed));
    assert_eq!(turn.original_user_request.as_deref(), Some("first request"));
    assert_eq!(turn.assistant_text, "partial output");
    assert_eq!(turn.non_streaming_fallbacks.len(), 1);
    assert_eq!(turn.non_streaming_fallbacks[0].attempt, 5);
}

#[tokio::test]
async fn committed_turn_keeps_file_read_authority() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_read",
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
        ),
        response_step("read complete", Vec::new()),
    ]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    let mut session = create_test_session(&store, "session_bbbbbbc1").await;

    engine
        .run_turn(&mut session, "read the file", |_| {})
        .await
        .unwrap();

    let write = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(write.output["status"], "success");
    assert_eq!(
        tokio::fs::read_to_string(dir.path().join("note.txt"))
            .await
            .unwrap(),
        "after\n"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_turn_rolls_back_new_file_read_authority() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let mut steps = vec![tool_use_step(
        "toolu_read",
        "file_read",
        json!({"path": "note.txt", "show_linenos": false}),
    )];
    steps.extend(exhausted_stream_failure_steps(
        "provider failed after read",
        "partial after read",
    ));
    let provider = Arc::new(RecordingProvider::new(steps));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    let mut session = create_test_session(&store, "session_bbbbbbc2").await;

    engine
        .run_turn(&mut session, "read then continue", |_| {})
        .await
        .unwrap_err();

    let write = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        write.outcome,
        crate::api::ToolExecutionOutcome::BusinessFailure
    );
    assert_eq!(write.output["status"], "error");
    assert_eq!(
        tokio::fs::read_to_string(dir.path().join("note.txt"))
            .await
            .unwrap(),
        "before\n"
    );
}

#[tokio::test]
async fn aborted_turn_rolls_back_new_file_read_authority() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let provider = Arc::new(BlockingAfterFileReadProvider::new());
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools(&dir, provider.clone(), Arc::clone(&tools));
    let session = create_test_session(&store, "session_bbbbbbc5").await;
    let session_id = session.metadata.id.clone();

    let turn = tokio::spawn(async move {
        let mut session = session;
        engine
            .run_turn(&mut session, "read then wait", |_| {})
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(1),
        provider.second_call_started.notified(),
    )
    .await
    .expect("provider should block after file_read");
    turn.abort();
    assert!(turn.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if tools
                .begin_file_read_state_checkpoint(&session_id, "probe")
                .await
                .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop guard should release the aborted turn checkpoint");
    tools
        .rollback_file_read_state_checkpoint(&session_id, "probe")
        .await
        .unwrap();

    let write = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            ToolDispatchContext {
                current_session_id: Some(session_id),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        write.outcome,
        crate::api::ToolExecutionOutcome::BusinessFailure
    );
    assert_eq!(write.output["status"], "error");
    assert_eq!(
        tokio::fs::read_to_string(dir.path().join("note.txt"))
            .await
            .unwrap(),
        "before\n"
    );
}

#[tokio::test]
async fn late_steer_rolls_back_file_read_authority_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_read",
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
        ),
        ProviderStep::ResponseAndSteer {
            response: provider_response("must not commit"),
            events: Vec::new(),
            control,
        },
    ]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    let mut session = create_test_session(&store, "session_bbbbbbc3").await;

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "read then finish",
            Vec::new(),
            Some(control_rx),
            |_| {},
        )
        .await
        .unwrap();

    assert!(session.read_messages().await.unwrap().is_empty());
    let write = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        write.outcome,
        crate::api::ToolExecutionOutcome::BusinessFailure
    );
    assert_eq!(write.output["status"], "error");
}

#[tokio::test]
async fn late_cancel_rolls_back_file_read_authority_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_read",
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
        ),
        ProviderStep::ResponseAndCancel {
            response: provider_response("must not commit"),
            events: Vec::new(),
            control,
        },
    ]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    let mut session = create_test_session(&store, "session_bbbbbbc4").await;

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "read then finish",
            Vec::new(),
            Some(control_rx),
            |_| {},
        )
        .await
        .unwrap();

    assert!(session.read_messages().await.unwrap().is_empty());
    let write = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        write.outcome,
        crate::api::ToolExecutionOutcome::BusinessFailure
    );
    assert_eq!(write.output["status"], "error");
}

#[tokio::test(start_paused = true)]
async fn run_turn_fallback_success_commits_only_complete_non_streaming_response() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step(
            "provider stream failed",
            vec![ProviderEvent::AssistantTextDelta {
                text: "partial output".into(),
            }],
        ),
        response_step(
            "complete replacement",
            vec![ProviderEvent::AssistantMessageCompleted {
                text: "complete replacement".into(),
            }],
        ),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_bbbbbbb3").await;
    let mut events = Vec::new();

    engine
        .run_turn(&mut session, "first request", |event| events.push(event))
        .await
        .unwrap();

    let messages = session.read_messages().await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(text_content(&messages[1]), "complete replacement");
    assert!(!text_content(&messages[1]).contains("partial output"));
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[0].stream);
    assert!(!requests[1].stream);
    assert_eq!(requests[1].retry_count_override, Some(0));
    let journal = session.read_turn_journal().await;
    assert_eq!(
        journal
            .events
            .iter()
            .filter(|event| matches!(
                event.kind,
                TurnJournalEventKind::NonStreamingFallbackAttemptStarted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        journal
            .events
            .iter()
            .filter(|event| matches!(
                event.kind,
                TurnJournalEventKind::NonStreamingFallbackSucceeded { .. }
            ))
            .count(),
        1
    );
    let projection = replay_turn_journal(journal);
    let turn = &projection.turns[0];
    assert_eq!(turn.status, Some(TurnJournalStatus::Committed));
    assert_eq!(turn.assistant_text, "complete replacement");
    assert_eq!(turn.non_streaming_fallbacks.len(), 1);
    assert_eq!(
        turn.non_streaming_fallbacks[0].state,
        TurnJournalNonStreamingFallbackState::Succeeded
    );
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::NonStreamingFallbackAttemptStarted {
            attempt: 1,
            max_attempts: 5
        }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::NonStreamingFallbackSucceeded { text }
            if text == "complete replacement"
    )));
}

#[tokio::test(start_paused = true)]
async fn fallback_tool_use_cancel_before_dispatch_writes_skipped_journal_without_canonical() {
    let dir = tempfile::tempdir().unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step(
            "provider stream failed",
            vec![ProviderEvent::AssistantTextDelta {
                text: "partial output".into(),
            }],
        ),
        ProviderStep::ResponseAndCancel {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_fallback".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "must not run"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
            control,
        },
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_fa11ba11").await;
    let mut events = Vec::new();

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "first request",
            Vec::new(),
            Some(control_rx),
            |event| events.push(event),
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[0].stream);
    assert!(!requests[1].stream);
    assert_eq!(requests[1].retry_count_override, Some(0));
    assert!(session.read_messages().await.unwrap().is_empty());
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::NonStreamingFallbackSucceeded { text } if text.is_empty()
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::ToolCallSkipped { id, reason, .. }
            if id == "toolu_fallback"
                && *reason == ToolCallSkipReason::TurnCancelledBeforeDispatch
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::ToolCallStarted { id, .. } if id == "toolu_fallback"
    )));

    let journal = session.read_turn_journal().await;
    assert!(journal.events.iter().any(|event| matches!(
        event.kind,
        TurnJournalEventKind::NonStreamingFallbackSucceeded { .. }
    )));
    assert!(journal.events.iter().any(|event| matches!(
        event.kind,
        TurnJournalEventKind::ToolCallSkipped { ref tool_use_id, reason, .. }
            if tool_use_id == "toolu_fallback"
                && reason == ToolCallSkipReason::TurnCancelledBeforeDispatch
    )));
    assert!(!journal.events.iter().any(|event| matches!(
        event.kind,
        TurnJournalEventKind::ToolCallStarted { ref tool_use_id, .. }
            if tool_use_id == "toolu_fallback"
    )));
    let turn = replay_turn_journal(journal)
        .unresolved_tail()
        .cloned()
        .unwrap();
    assert_eq!(turn.status, Some(TurnJournalStatus::Cancelled));
    assert_eq!(
        turn.tool_calls[0].skip_reason,
        Some(ToolCallSkipReason::TurnCancelledBeforeDispatch)
    );
    let recovery = crate::session::turn_journal_recovery_context(
        &turn,
        crate::session::RecoveryContextLimits::default(),
    )
    .unwrap();
    assert!(recovery.contains("tools_skipped"));
    assert!(!recovery.contains("tools_pending_or_skipped"));
}

#[tokio::test]
async fn tool_use_steer_before_dispatch_is_recorded_as_interrupted_skip() {
    let dir = tempfile::tempdir().unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::ResponseAndSteer {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_steer".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "must not run"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
            control,
        },
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_5fee1234").await;
    let mut events = Vec::new();

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "original request",
            Vec::new(),
            Some(control_rx),
            |event| events.push(event),
        )
        .await
        .unwrap();

    assert_eq!(provider.requests().await.len(), 1);
    assert!(session.read_messages().await.unwrap().is_empty());
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::ToolCallSkipped { id, reason, .. }
            if id == "toolu_steer"
                && *reason == ToolCallSkipReason::TurnInterruptedBeforeDispatch
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::ToolCallStarted { id, .. } if id == "toolu_steer"
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::ToolCallCompleted { id, .. } if id == "toolu_steer"
    )));

    let journal = session.read_turn_journal().await;
    let turn = replay_turn_journal(journal)
        .unresolved_tail()
        .cloned()
        .unwrap();
    assert_eq!(turn.status, Some(TurnJournalStatus::InterruptedByUser));
    assert_eq!(
        turn.tool_calls[0].skip_reason,
        Some(ToolCallSkipReason::TurnInterruptedBeforeDispatch)
    );
    assert_eq!(
        turn.user_steers,
        vec!["steer after provider response".to_string()]
    );
    let recovery = crate::session::turn_journal_recovery_context(
        &turn,
        crate::session::RecoveryContextLimits::default(),
    )
    .unwrap();
    assert!(recovery.contains("tools_skipped"));
    assert!(recovery.contains("turn_interrupted_before_dispatch"));
}

#[tokio::test]
async fn journal_only_tail_does_not_feed_memory_review_or_finalize_trace_text() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_abcd1111").await;
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::UserInputAccepted {
                text: "journal only memory/finalize needle".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();

    let messages = session.read_messages().await.unwrap();
    assert!(messages.is_empty());
    assert!(!super::memory_review_should_run(&messages));
    assert_eq!(super::session_trace_text(&messages), "session");
}

#[tokio::test]
async fn finalize_without_unrecapped_messages_does_not_request_success_notification() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0001").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("old request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("old answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    session.advance_recapped_until(2).await.unwrap();

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(!report.advanced_recapped_until);
    assert!(!report.finalized_unrecapped_messages);
    assert!(session
        .read_metadata()
        .await
        .unwrap()
        .finalized_at
        .is_some());
    assert!(provider.requests().await.is_empty());
}

#[tokio::test]
async fn finalize_with_unrecapped_messages_requests_success_notification() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0002").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("new request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("new answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    assert!(report.finalized_unrecapped_messages);
    assert_eq!(provider.requests().await.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, metadata.message_count);
    assert!(metadata.finalized_at.is_some());
}

#[tokio::test]
async fn direct_finalize_marks_session_finalizing_before_recap_request() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(FinalizingStateCheckingProvider {
        session_yaml: dir
            .path()
            .join("agents")
            .join("agent-a")
            .join("sessions")
            .join("session_face0042")
            .join("session.yaml"),
    });
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_face0042").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("new request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("new answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    assert!(session
        .read_metadata()
        .await
        .unwrap()
        .finalized_at
        .is_some());
}

#[tokio::test]
async fn finalize_failure_keeps_unclosed_session_finalizing() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![error_step(
        "recap provider failed",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_face0043").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("new request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("new answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();

    let err = engine
        .finalize_session(&mut session, |_| {})
        .await
        .expect_err("provider failure should fail finalize");

    assert!(err.to_string().contains("recap provider failed"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Finalizing);
    assert!(metadata.finalized_at.is_none());
    assert!(metadata.closed_at.is_none());
}

#[tokio::test]
async fn finalize_recovered_compaction_checkpoint_requests_success_notification() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0003").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("checkpointed request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("checkpointed answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: Vec::new(),
        summary_start_index: 0,
        summary_end_index: 0,
        summary_segment_hash: super::hash_session_segment(&messages[0..0]).unwrap(),
        recap_start_index: 0,
        recap_end_index: messages.len(),
        recap_segment_hash: super::hash_session_segment(&messages).unwrap(),
        summary: String::new(),
        active_turn_summary: Some("stale active summary".into()),
        active_turn: Some(ActiveTurnCompactionCursor {
            turn_id: "turn_1".into(),
            base_message_count: messages.len(),
            compacted_until_segment: 1,
            safe_until_event_seq: 10,
            source_hash: "stale_hash".into(),
        }),
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: "checkpointed request".into(),
        trace_created_at: Utc::now(),
        trace_id: None,
        applied_report: None,
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
        .await
        .unwrap();

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    assert!(report.finalized_unrecapped_messages);
    assert!(provider.requests().await.is_empty());
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, metadata.message_count);
    assert!(metadata.finalized_at.is_some());
}

#[tokio::test]
async fn manual_compact_noop_reports_nothing_new_for_empty_session() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_face0008").await;

    let outcome = engine
        .compact_session_checkpoint(&mut session, |_| {})
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        SessionCompactionResult::Noop(SessionCompactionNoopReason::NothingNew)
    ));
}

#[tokio::test]
async fn manual_compact_noop_reports_raw_tail_budget_when_new_history_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_face0009").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "hihi"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "hello"),
        ])
        .await
        .unwrap();

    let outcome = engine
        .compact_session_checkpoint(&mut session, |_| {})
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        SessionCompactionResult::Noop(SessionCompactionNoopReason::RawTailWithinBudget)
    ));
}

#[tokio::test]
async fn manual_compact_applied_checkpoint_preserves_report_and_clears_file_read_state() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..ToolConfig::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    let (engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    let mut session = create_test_session(&store, "session_face0005").await;
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let tool_context = ToolDispatchContext {
        current_session_id: Some(session.metadata.id.clone()),
        ..ToolDispatchContext::default()
    };
    tools
        .dispatch_with_context(
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
            tool_context.clone(),
        )
        .await
        .unwrap();
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("checkpointed request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("checkpointed answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let summary_hash = super::hash_session_segment(&messages[0..0]).unwrap();
    let recap_hash = super::hash_session_segment(&messages).unwrap();
    let claim_id = ClaimId::random();
    let applied_claim_id = ClaimId::random();
    let applied_dispute_id = DisputeId::random();
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: vec!["compact_report".into()],
        summary_start_index: 0,
        summary_end_index: 0,
        summary_segment_hash: summary_hash,
        recap_start_index: 0,
        recap_end_index: messages.len(),
        recap_segment_hash: recap_hash,
        summary: String::new(),
        active_turn_summary: None,
        active_turn: None,
        prepared_claims: vec![Claim {
            id: claim_id.clone(),
            name: "checkpoint_claim".into(),
            statement: "checkpoint claim statement".into(),
            scope: "test".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: now,
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "checkpoint evidence".into(),
        }],
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: "checkpointed request".into(),
        trace_created_at: now,
        trace_id: None,
        applied_report: Some(CompactionAppliedReport {
            trace_id: None,
            new_claim_ids: vec![applied_claim_id.clone()],
            updated_claim_ids: Vec::new(),
            used_claim_ids: vec![claim_id.clone()],
            new_dispute_ids: vec![applied_dispute_id.clone()],
            warnings: vec!["upload warning kept".into()],
        }),
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
        .await
        .unwrap();

    let outcome = engine
        .compact_session_checkpoint_with_events(&mut session, &mut |_| {})
        .await
        .unwrap();
    let ManualCompactionOutcome::Compacted(outcome) = outcome else {
        panic!("expected recovered applied checkpoint to compact");
    };
    let outcome = *outcome;

    assert_eq!(outcome.report.new_claim_ids, vec![applied_claim_id]);
    assert_eq!(outcome.report.used_claim_ids, vec![claim_id]);
    assert_eq!(outcome.report.new_dispute_ids, vec![applied_dispute_id]);
    assert_eq!(outcome.report.warnings, vec!["upload warning kept"]);
    assert_eq!(outcome.audit_ids, vec!["compact_report"]);
    let write = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            tool_context,
        )
        .await
        .unwrap();
    assert_eq!(write.output["status"], "error");
    assert_eq!(
        tokio::fs::read_to_string(dir.path().join("note.txt"))
            .await
            .unwrap(),
        "before\n"
    );
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit_log.contains(r#""audit_id":"compact_report""#));
    assert!(audit_log.contains(r#""recovered":true"#));
}

#[tokio::test]
async fn manual_compact_post_summary_failure_writes_failed_audit() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            r#"{"committed_summary":"old turn summarized","active_turn_summary":null}"#,
            Vec::new(),
        ),
        response_step("not json", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.tail_target_ctx_ratio = 0.015;
    engine.compaction.tail_hard_ctx_ratio = 0.0225;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_face0006").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();

    let err = engine
        .compact_session_checkpoint_with_events(&mut session, &mut |_| {})
        .await
        .unwrap_err();

    assert!(err.to_string().contains("解析结构化 JSON 响应失败"));
    assert_eq!(provider.requests().await.len(), 2);
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit_log.contains(r#""kind":"started""#));
    assert!(audit_log.contains(r#""kind":"model_attempt""#));
    assert!(audit_log.contains(r#""kind":"failed""#));
    assert!(!audit_log.contains(r#""kind":"completed""#));
}

#[tokio::test]
async fn manual_compact_exhausts_overlong_summary_repairs_without_advancing_state() {
    let dir = tempfile::tempdir().unwrap();
    let overlong = "X".repeat(11);
    let response = format!(
        r#"{{"committed_summary":{summary},"active_turn_summary":null}}"#,
        summary = serde_json::to_string(&overlong).unwrap()
    );
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(&response, Vec::new()),
        response_step(&response, Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.tail_target_ctx_ratio = 0.015;
    engine.compaction.tail_hard_ctx_ratio = 0.0225;
    engine.compaction.tail_previous_real_user_turns = 1;
    engine.compaction.summary_max_chars = 10;
    engine.json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        1024,
        1,
        Duration::ZERO,
        Duration::ZERO,
    ));
    let mut session = create_test_session(&store, "session_face0010").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();
    let original_message_count = session.read_messages().await.unwrap().len();
    let mut events = Vec::new();

    engine
        .compact_session_checkpoint(&mut session, |event| events.push(event))
        .await
        .expect_err("two overlong summaries must fail manual compaction");

    assert_eq!(provider.requests().await.len(), 2);
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.message_count, original_message_count);
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_none());
    assert!(session
        .read_compaction_checkpoint()
        .await
        .unwrap()
        .is_none());
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::CompactionFailed { error }
            if error == "Compaction failed repeatedly. You can run /compact to try again or start a new session."
    )));
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert_eq!(audit_log.matches(r#""kind":"model_attempt""#).count(), 2);
    assert!(audit_log.contains(r#""kind":"failed""#));
    assert!(!audit_log.contains(r#""kind":"completed""#));
}

#[tokio::test]
async fn manual_compact_over_budget_summary_does_not_start_recap_provider() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 1_024;
    engine.compaction.tail_target_ctx_ratio = 0.05;
    engine.compaction.tail_hard_ctx_ratio = 0.075;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_face000a").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer ".repeat(120)),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();

    let error = engine
        .compact_session_checkpoint_with_events(&mut session, &mut |_| {})
        .await
        .expect_err("summary output reserve should exceed the context window");

    assert!(error
        .to_string()
        .contains("remains over budget after omitting all tool results"));
    assert!(provider.requests().await.is_empty());
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_none());
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap_or_default();
    assert!(!audit_log.contains(r#""kind":"started""#));
}

#[tokio::test]
async fn manual_compact_bad_checkpoint_range_writes_failed_audit() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_face0007").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "checkpointed request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "checkpointed answer"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: vec!["compact_bad_range".into()],
        summary_start_index: 0,
        summary_end_index: messages.len() + 10,
        summary_segment_hash: "unused".into(),
        recap_start_index: 0,
        recap_end_index: messages.len(),
        recap_segment_hash: hash_session_segment(&messages).unwrap(),
        summary: "bad range summary".into(),
        active_turn_summary: None,
        active_turn: None,
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: String::new(),
        trace_created_at: Utc::now(),
        trace_id: None,
        applied_report: None,
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
        .await
        .unwrap();

    let err = engine
        .compact_session_checkpoint_with_events(&mut session, &mut |_| {})
        .await
        .unwrap_err();

    assert!(err.to_string().contains("summary_end_index"));
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit_log.contains(r#""audit_id":"compact_bad_range""#));
    assert!(audit_log.contains(r#""kind":"failed""#));
}

#[tokio::test]
async fn finalize_ignores_legacy_compaction_checkpoint_without_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims": [], "used_claim_ids": [], "new_disputes": []}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0004").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("legacy checkpoint request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("legacy checkpoint answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let checkpoint = CompactionCheckpoint {
        schema_version: None,
        audit_ids: Vec::new(),
        summary_start_index: 0,
        summary_end_index: 0,
        summary_segment_hash: super::hash_session_segment(&messages[0..0]).unwrap(),
        recap_start_index: 0,
        recap_end_index: messages.len(),
        recap_segment_hash: super::hash_session_segment(&messages).unwrap(),
        summary: String::new(),
        active_turn_summary: None,
        active_turn: None,
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: "legacy checkpoint request".into(),
        trace_created_at: Utc::now(),
        trace_id: None,
        applied_report: None,
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
        .await
        .unwrap();

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    assert_eq!(provider.requests().await.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, metadata.message_count);
    assert!(metadata.finalized_at.is_some());
}

#[tokio::test]
async fn pre_provider_interrupt_writes_interrupted_journal_without_canonical_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": "remember this"}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        response_step("should not run", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_aaaaaaa9").await;
    let (control, control_rx) = SessionTurnControl::channel();
    let request = tokio::spawn(async move {
        control
            .request_tool_boundary_steer("steer before tools")
            .await
    });
    tokio::task::yield_now().await;
    let mut events = Vec::new();

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "original request",
            Vec::new(),
            Some(control_rx),
            |event| events.push(event),
        )
        .await
        .unwrap();
    assert!(request.await.unwrap());

    assert_eq!(provider.requests().await.len(), 0);
    assert!(session.read_messages().await.unwrap().is_empty());
    let projection = replay_turn_journal(session.read_turn_journal().await);
    let turn = projection.unresolved_tail().unwrap();
    assert_eq!(turn.status, Some(TurnJournalStatus::InterruptedByUser));
    assert_eq!(
        turn.original_user_request.as_deref(),
        Some("original request")
    );
    assert_eq!(turn.user_steers, vec!["steer before tools".to_string()]);
    assert!(turn.tool_calls.is_empty());
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::TurnInterrupted { .. })));
}

#[tokio::test]
async fn pre_provider_cancel_writes_cancelled_journal_without_canonical_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![ProviderStep::Response {
        response: ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "working_note".into(),
                    input: json!({"action": "add", "note": "remember this"}),
                }],
            },
            stop: ProviderStop::ToolUse,
        },
        events: Vec::new(),
    }]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_aaaaaa10").await;
    let (control, control_rx) = SessionTurnControl::channel();
    let request = tokio::spawn(async move {
        control
            .request_tool_boundary_cancel("user cancelled turn")
            .await
    });
    tokio::task::yield_now().await;
    let mut events = Vec::new();

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "original request",
            Vec::new(),
            Some(control_rx),
            |event| events.push(event),
        )
        .await
        .unwrap();
    assert!(request.await.unwrap());

    assert!(session.read_messages().await.unwrap().is_empty());
    let projection = replay_turn_journal(session.read_turn_journal().await);
    let turn = projection.unresolved_tail().unwrap();
    assert_eq!(turn.status, Some(TurnJournalStatus::Cancelled));
    assert!(turn.user_steers.is_empty());
    assert_eq!(provider.requests().await.len(), 0);
    assert!(turn.tool_calls.is_empty());
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::TurnCancelled { .. })));
}

#[tokio::test]
async fn missing_committed_journal_marker_is_reconciled_with_canonical_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "next answer",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "next answer".into(),
        }],
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_aaaaaa11").await;
    let journal_at = Utc::now();
    let committed_at = journal_at + chrono::Duration::milliseconds(10);
    session
        .append_session_turn_messages(
            &[
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage::user_text("already committed"),
                    committed_at,
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage::assistant_text("committed answer"),
                    committed_at,
                ),
            ],
            "test-model",
        )
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            journal_at,
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            journal_at,
            TurnJournalEventKind::UserInputAccepted {
                text: "already committed".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            journal_at,
            TurnJournalEventKind::AssistantCompleted {
                text: "committed answer".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "next request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let next_user_text = last_user_text(&requests[0]);
    assert!(!next_user_text.contains("<interrupted_turn_context>"));
    assert!(next_user_text.contains("next request"));
}

#[tokio::test(start_paused = true)]
async fn continuation_commits_recovery_wrapper_once_after_failed_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut steps =
        exhausted_stream_failure_steps("provider stream failed", "partial before failure");
    steps.extend([
        response_step(
            "continued answer",
            vec![ProviderEvent::AssistantMessageCompleted {
                text: "continued answer".into(),
            }],
        ),
        response_step(
            "fresh answer",
            vec![ProviderEvent::AssistantMessageCompleted {
                text: "fresh answer".into(),
            }],
        ),
    ]);
    let provider = Arc::new(RecordingProvider::new(steps));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_ccccccc3").await;

    let _ = engine.run_turn(&mut session, "first request", |_| {}).await;
    engine
        .run_turn(&mut session, "continue now", |_| {})
        .await
        .unwrap();
    engine
        .run_turn(&mut session, "fresh request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 8);
    let continuation_user_text = last_user_text(&requests[6]);
    assert!(continuation_user_text.contains("<interrupted_turn_context>"));
    assert!(continuation_user_text.contains(r#""previous_turn_status":"failed""#));
    assert!(continuation_user_text.contains(r#""original_user_request":"first request""#));
    assert!(continuation_user_text.contains("partial before failure"));
    assert!(continuation_user_text.contains(r#""text":"continue now""#));

    let fresh_user_text = last_user_text(&requests[7]);
    assert!(fresh_user_text.contains("fresh request"));
    assert!(!fresh_user_text.contains("<interrupted_turn_context>"));

    let messages = session.read_messages().await.unwrap();
    assert_eq!(messages.len(), 4);
    let committed_user_text = text_content(&messages[0]);
    assert!(committed_user_text.contains("<interrupted_turn_context>"));
    assert!(committed_user_text.contains(r#""text":"continue now""#));
    assert_eq!(text_content(&messages[2]), "fresh request");

    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert!(projection.unresolved_tail().is_none());
}

#[tokio::test]
async fn damaged_journal_skips_bad_lines_but_recovers_later_valid_tail() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "clean answer",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "clean answer".into(),
        }],
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_dada0001").await;
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::UserInputAccepted {
                text: "old interrupted request".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::TurnFinished {
                status: TurnJournalStatus::Failed,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    let mut raw = tokio::fs::read_to_string(&session.paths.turn_events_jsonl)
        .await
        .unwrap();
    raw.push_str("not json\n");
    tokio::fs::write(&session.paths.turn_events_jsonl, raw)
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "fresh request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let user_text = last_user_text(&requests[0]);
    assert!(user_text.contains("fresh request"));
    assert!(user_text.contains("<interrupted_turn_context>"));
    assert!(user_text.contains("old interrupted request"));
}

#[tokio::test]
async fn repeated_user_text_unfinished_tail_is_not_mistaken_for_previous_canonical_turn() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "next answer",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "next answer".into(),
        }],
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_ddddddd4").await;
    let previous_at = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("same request")],
                previous_at,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("old answer")],
                previous_at,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let journal_at = previous_at + chrono::Duration::seconds(1);
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            journal_at,
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            journal_at,
            TurnJournalEventKind::UserInputAccepted {
                text: "same request".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "continue", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let next_user_text = last_user_text(&requests[0]);
    assert!(next_user_text.contains("<interrupted_turn_context>"));
    assert!(next_user_text.contains(r#""original_user_request":"same request""#));
    assert!(next_user_text.contains(r#""text":"continue""#));
}

#[tokio::test]
async fn missing_committed_marker_for_continuation_wrapper_is_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "fresh answer",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "fresh answer".into(),
        }],
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_eeeeeee5").await;
    let first_at = Utc::now();
    let continuation_at = first_at + chrono::Duration::seconds(1);
    let committed_at = continuation_at + chrono::Duration::milliseconds(10);
    let recovery_wrapper = r#"<interrupted_turn_context>
{"unresolved_turn_count":1,"unresolved_turns":[{"previous_turn_status":"failed","original_user_request":"first request","assistant_partial_or_completed_summary":"partial"}]}
</interrupted_turn_context>

<current_user_request>
continue now
</current_user_request>"#;
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text(recovery_wrapper)],
                committed_at,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("continued answer")],
                committed_at,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            first_at,
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            first_at,
            TurnJournalEventKind::UserInputAccepted {
                text: "first request".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            first_at,
            TurnJournalEventKind::AssistantDelta {
                text: "partial".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            first_at,
            TurnJournalEventKind::TurnFinished {
                status: TurnJournalStatus::Failed,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_2",
            continuation_at,
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_2",
            continuation_at,
            TurnJournalEventKind::UserInputAccepted {
                text: "continue now".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_2",
            continuation_at,
            TurnJournalEventKind::AssistantCompleted {
                text: "continued answer".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "fresh request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let fresh_user_text = last_user_text(&requests[0]);
    assert!(fresh_user_text.contains("fresh request"));
    assert!(!fresh_user_text.contains("<interrupted_turn_context>"));
}

#[test]
fn recovery_chain_starts_after_canonical_continuation_without_committed_marker() {
    let first_at = Utc::now();
    let continuation_at = first_at + chrono::Duration::seconds(1);
    let third_at = continuation_at + chrono::Duration::seconds(1);
    let committed_at = continuation_at + chrono::Duration::milliseconds(10);
    let messages = vec![
            test_message(
                0,
                SessionMessageRole::User,
                vec![SessionContentBlock::text(
                    "<interrupted_turn_context>\n\
                     {\"unresolved_turn_count\":1,\"unresolved_turns\":[{\"previous_turn_status\":\"failed\",\"original_user_request\":\"first request\",\"assistant_partial_or_completed_summary\":\"first partial\"}]}\n\
                     </interrupted_turn_context>\n\n\
                     <current_user_request>\ncontinue now\n</current_user_request>",
                )],
            ),
            test_message(
                1,
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("continued answer")],
            ),
        ]
        .into_iter()
        .map(|mut message| {
            message.created_at = committed_at;
            message
        })
        .collect::<Vec<_>>();
    let projection = TurnJournalProjection {
        warnings: Vec::new(),
        turns: vec![
            TurnJournalTurn {
                turn_id: "turn_1".into(),
                started_at: Some(first_at),
                accepted_at: Some(first_at),
                finished_at: Some(first_at),
                status: Some(TurnJournalStatus::Failed),
                original_user_request: Some("first request".into()),
                canonical_user_content_hash: None,
                canonical_user_first_text: None,
                skill_instructions: Vec::new(),
                compaction_assets: Vec::new(),
                assistant_text: "first partial".into(),
                assistant_completed: false,
                tool_calls: Vec::new(),
                timeline_items: Vec::new(),
                user_steers: Vec::new(),
                non_streaming_fallbacks: Vec::new(),
            },
            TurnJournalTurn {
                turn_id: "turn_2".into(),
                started_at: Some(continuation_at),
                accepted_at: Some(continuation_at),
                finished_at: None,
                status: None,
                original_user_request: Some("continue now".into()),
                canonical_user_content_hash: None,
                canonical_user_first_text: None,
                skill_instructions: Vec::new(),
                compaction_assets: Vec::new(),
                assistant_text: "continued answer".into(),
                assistant_completed: true,
                tool_calls: Vec::new(),
                timeline_items: Vec::new(),
                user_steers: Vec::new(),
                non_streaming_fallbacks: Vec::new(),
            },
            TurnJournalTurn {
                turn_id: "turn_3".into(),
                started_at: Some(third_at),
                accepted_at: Some(third_at),
                finished_at: Some(third_at),
                status: Some(TurnJournalStatus::Failed),
                original_user_request: Some("third request".into()),
                canonical_user_content_hash: None,
                canonical_user_first_text: None,
                skill_instructions: Vec::new(),
                compaction_assets: Vec::new(),
                assistant_text: "third partial".into(),
                assistant_completed: false,
                tool_calls: Vec::new(),
                timeline_items: Vec::new(),
                user_steers: Vec::new(),
                non_streaming_fallbacks: Vec::new(),
            },
        ],
    };

    let chain = super::recovery_turn_chain(&projection, &messages);
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].turn_id, "turn_3");
}

#[test]
fn recovery_chain_reconciles_tool_loop_canonical_turn_without_committed_marker() {
    let started_at = Utc::now();
    let committed_at = started_at + chrono::Duration::milliseconds(10);
    let mut messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("tool request")],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::tool_use(
                "toolu_1",
                "working_note",
                json!({"action": "add", "note": "remember"}),
            )],
        ),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result(
                "toolu_1",
                r#"{"ok":true}"#,
            )],
        ),
        test_message(
            3,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("final answer")],
        ),
    ];
    for message in &mut messages {
        message.created_at = committed_at;
    }
    let projection = TurnJournalProjection {
        warnings: Vec::new(),
        turns: vec![TurnJournalTurn {
            turn_id: "turn_1".into(),
            started_at: Some(started_at),
            accepted_at: Some(started_at),
            finished_at: None,
            status: None,
            original_user_request: Some("tool request".into()),
            canonical_user_content_hash: None,
            canonical_user_first_text: None,
            skill_instructions: Vec::new(),
            compaction_assets: Vec::new(),
            assistant_text: "final answer".into(),
            assistant_completed: true,
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            non_streaming_fallbacks: Vec::new(),
        }],
    };

    let chain = super::recovery_turn_chain(&projection, &messages);
    assert!(chain.is_empty());
}

#[test]
fn recovery_chain_reconciles_attachment_turn_without_committed_marker() {
    let started_at = Utc::now();
    let committed_at = started_at + chrono::Duration::milliseconds(10);
    let mut messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![
                SessionContentBlock::text("inspect this image"),
                SessionContentBlock::image("image/png", "QUJD"),
            ],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("image inspected")],
        ),
    ];
    for message in &mut messages {
        message.created_at = committed_at;
    }
    let projection = TurnJournalProjection {
        warnings: Vec::new(),
        turns: vec![TurnJournalTurn {
            turn_id: "turn_1".into(),
            started_at: Some(started_at),
            accepted_at: Some(started_at),
            finished_at: None,
            status: None,
            original_user_request: Some("inspect this image".into()),
            canonical_user_content_hash: Some(
                canonical_user_content_hash(&messages[0].content).unwrap(),
            ),
            canonical_user_first_text: None,
            skill_instructions: Vec::new(),
            compaction_assets: Vec::new(),
            assistant_text: "image inspected".into(),
            assistant_completed: true,
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            non_streaming_fallbacks: Vec::new(),
        }],
    };

    let chain = super::recovery_turn_chain(&projection, &messages);

    assert!(chain.is_empty());
}

#[tokio::test(start_paused = true)]
async fn failed_continuation_chain_preserves_earlier_unresolved_context() {
    let dir = tempfile::tempdir().unwrap();
    let mut steps = exhausted_stream_failure_steps("first provider failure", "first partial");
    steps.extend(exhausted_stream_failure_steps(
        "second provider failure",
        "second partial",
    ));
    steps.push(response_step(
        "final answer",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "final answer".into(),
        }],
    ));
    let provider = Arc::new(RecordingProvider::new(steps));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_fffffff6").await;

    let _ = engine.run_turn(&mut session, "first request", |_| {}).await;
    let _ = engine.run_turn(&mut session, "continue once", |_| {}).await;
    engine
        .run_turn(&mut session, "continue twice", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let second_user_text = last_user_text(&requests[6]);
    assert!(second_user_text.contains(r#""original_user_request":"first request""#));
    let third_user_text = last_user_text(&requests[12]);
    assert!(third_user_text.contains(r#""unresolved_turn_count":2"#));
    assert!(third_user_text.contains(r#""original_user_request":"first request""#));
    assert!(third_user_text.contains(r#""assistant_partial_or_completed_summary":"first partial""#));
    assert!(third_user_text.contains(r#""original_user_request":"continue once""#));
    assert!(
        third_user_text.contains(r#""assistant_partial_or_completed_summary":"second partial""#)
    );
}

#[tokio::test]
async fn turn_journal_emitter_flushes_delta_by_timer_without_next_delta() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut emitter = TurnJournalEmitter::new(tx, Duration::from_millis(5), 1024);

    emitter.assistant_delta("partial".into());

    let command = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .unwrap()
        .unwrap();
    match command.kind {
        TurnJournalEventKind::AssistantDelta { text } => assert_eq!(text, "partial"),
        other => panic!("unexpected journal command: {other:?}"),
    }
    emitter.finish(TurnJournalStatus::Failed).await;
}

#[test]
fn append_acn_md_adds_global_markdown_to_prompt_tail() {
    let rendered = append_acn_md(
        "base prompt".to_string(),
        Some("请优先复用团队知识。".into()),
    );

    assert_eq!(
            rendered,
            "base prompt\n\n# ACN.md 用户指令\n\n**注：以下内容来自当前 ACN home 的 `ACN.md`，是用户为本 ACN 环境提供的持久偏好、项目约定或协作指令。请在不违反上文 system prompt、工具边界、数据边界和当前用户明确要求的前提下遵循。**\n\n**如果 ACN.md 与当前用户请求冲突，优先当前用户请求；如果与上文核心系统规则冲突，优先上文系统规则；如果冲突会影响任务执行，应向用户说明并请求确认。**\n\n请优先复用团队知识。"
        );
}

#[test]
fn append_acn_md_keeps_prompt_when_missing() {
    let rendered = append_acn_md("base prompt".to_string(), None);

    assert_eq!(rendered, "base prompt");
}

fn test_compaction_audit_context(
    scope: CompactionAuditScope,
) -> CompactionAuditSummaryContext<'static> {
    CompactionAuditSummaryContext {
        trigger: CompactionAuditTrigger::ManualCheckpoint,
        scope,
        turn_id: None,
        base_message_count: None,
        ranges: CompactionRanges {
            summary_start_index: 0,
            summary_end_index: 0,
            recap_start_index: 0,
            recap_end_index: 0,
        },
    }
}

#[tokio::test]
async fn compaction_summary_prefers_full_tool_results_when_request_fits() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary":"full summary","active_turn_summary":null}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 6_000;
    engine.compaction.tool_result_raw_max_chars = 16;
    let session = create_test_session(&store, "session_c0ffee20").await;
    let raw_marker = format!("FULL_SUMMARY_TOOL_RESULT_{}", "X".repeat(4_000));
    let transcript = compaction_transcript_projection(
        vec![SessionTurnMessage {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_summary".into(),
                content: raw_marker,
            }],
            provider_replay: None,
        }],
        engine.compaction.tool_result_raw_max_chars,
    );
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(1),
        prior_committed_summary: None,
        committed_transcript: Some(&transcript.full),
        committed_transcript_with_large_tool_results_omitted: Some(
            &transcript.large_tool_results_omitted,
        ),
        committed_transcript_with_tool_results_omitted: Some(&transcript.tool_results_omitted),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 6000,
    };

    engine
        .generate_compaction_summary(&session, &inputs, &mut |_| {})
        .await
        .expect("full compaction summary input");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(payload.contains("FULL_SUMMARY_TOOL_RESULT"));
    assert!(!payload.contains("tool_result omitted from compaction summary input"));
}

#[tokio::test]
async fn compaction_summary_omits_media_from_active_turn_user_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary":null,"active_turn_summary":"bounded active summary"}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 6_000;
    let session = create_test_session(&store, "session_c0ffee25").await;
    let active_turn_user_anchor = SessionTurnMessage::user_content(vec![
        SessionTurnContentBlock::image(
            "image/png",
            format!("RAW_ACTIVE_IMAGE_{}", "A".repeat(40_000)),
        ),
        SessionTurnContentBlock::document_named(
            "application/pdf",
            format!("RAW_ACTIVE_DOCUMENT_{}", "B".repeat(40_000)),
            "brief.pdf",
        ),
    ]);
    let active_turn_transcript = vec![TurnMessage {
        role: "assistant".into(),
        content: "analyzed the attachments".into(),
    }];
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::ActiveTurn),
        committed_start_index: None,
        committed_end_index: None,
        prior_committed_summary: None,
        committed_transcript: None,
        committed_transcript_with_large_tool_results_omitted: None,
        committed_transcript_with_tool_results_omitted: None,
        prior_active_turn_summary: None,
        active_turn_user_anchor: Some(&active_turn_user_anchor),
        active_turn_start_segment: Some(0),
        active_turn_end_segment: Some(1),
        active_turn_transcript: Some(&active_turn_transcript),
        active_turn_transcript_with_large_tool_results_omitted: Some(&active_turn_transcript),
        active_turn_transcript_with_tool_results_omitted: Some(&active_turn_transcript),
        summary_max_chars: 6000,
    };

    engine
        .generate_compaction_summary(&session, &inputs, &mut |_| {})
        .await
        .expect("media-projected active compaction summary");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(payload.contains("image omitted from compaction summary input"));
    assert!(payload.contains("document omitted from compaction summary input"));
    assert!(payload.contains("brief.pdf"));
    assert!(!payload.contains("RAW_ACTIVE_IMAGE"));
    assert!(!payload.contains("RAW_ACTIVE_DOCUMENT"));
    let original_anchor = serde_json::to_string(&active_turn_user_anchor).unwrap();
    assert!(original_anchor.contains("RAW_ACTIVE_IMAGE"));
    assert!(original_anchor.contains("RAW_ACTIVE_DOCUMENT"));
}

#[tokio::test]
async fn compaction_summary_omits_only_large_tool_results_when_full_input_is_over_budget() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary":"projected summary","active_turn_summary":null}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 4_000;
    engine.compaction.tool_result_raw_max_chars = 128;
    let session = create_test_session(&store, "session_c0ffee24").await;
    let transcript = compaction_transcript_projection(
        vec![
            SessionTurnMessage {
                role: "user".into(),
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "toolu_large".into(),
                    content: format!("LARGE_SUMMARY_TOOL_RESULT_{}", "X".repeat(40_000)),
                }],
                provider_replay: None,
            },
            SessionTurnMessage {
                role: "user".into(),
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "toolu_small".into(),
                    content: "SMALL_SUMMARY_TOOL_RESULT".into(),
                }],
                provider_replay: None,
            },
        ],
        engine.compaction.tool_result_raw_max_chars,
    );
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(2),
        prior_committed_summary: None,
        committed_transcript: Some(&transcript.full),
        committed_transcript_with_large_tool_results_omitted: Some(
            &transcript.large_tool_results_omitted,
        ),
        committed_transcript_with_tool_results_omitted: Some(&transcript.tool_results_omitted),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 6000,
    };

    engine
        .generate_compaction_summary(&session, &inputs, &mut |_| {})
        .await
        .expect("large-only omission compaction summary");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(payload.contains("tool_result omitted from compaction summary input"));
    assert!(!payload.contains("LARGE_SUMMARY_TOOL_RESULT"));
    assert!(payload.contains("SMALL_SUMMARY_TOOL_RESULT"));
}

#[tokio::test]
async fn compaction_summary_falls_back_to_omitting_all_tool_results_before_provider_call() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary":"bounded summary","active_turn_summary":null}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 6_000;
    engine.compaction.tool_result_raw_max_chars = 100_000;
    let session = create_test_session(&store, "session_c0ffee21").await;
    let raw_marker = format!("RAW_SUMMARY_TOOL_RESULT_{}", "X".repeat(40_000));
    let transcript = compaction_transcript_projection(
        vec![SessionTurnMessage {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_summary".into(),
                content: raw_marker.clone(),
            }],
            provider_replay: None,
        }],
        engine.compaction.tool_result_raw_max_chars,
    );
    assert!(transcript
        .full
        .iter()
        .any(|message| message.content.contains("RAW_SUMMARY_TOOL_RESULT")));

    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(1),
        prior_committed_summary: None,
        committed_transcript: Some(&transcript.full),
        committed_transcript_with_large_tool_results_omitted: Some(
            &transcript.large_tool_results_omitted,
        ),
        committed_transcript_with_tool_results_omitted: Some(&transcript.tool_results_omitted),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 6000,
    };

    engine
        .generate_compaction_summary(&session, &inputs, &mut |_| {})
        .await
        .expect("fallback compaction summary");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].retry_count_override, Some(0));
    let payload = last_user_text(&requests[0]);
    assert!(payload.contains("tool_result omitted from compaction summary input"));
    assert!(!payload.contains("RAW_SUMMARY_TOOL_RESULT"));
    assert!(raw_marker.contains("RAW_SUMMARY_TOOL_RESULT"));
}

#[tokio::test]
async fn compaction_summary_rejects_over_budget_payload_without_provider_call() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 1_024;
    let session = create_test_session(&store, "session_c0ffee22").await;
    let transcript = vec![TurnMessage {
        role: "user".into(),
        content: "plain non-tool content".into(),
    }];
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(1),
        prior_committed_summary: None,
        committed_transcript: Some(&transcript),
        committed_transcript_with_large_tool_results_omitted: Some(&transcript),
        committed_transcript_with_tool_results_omitted: Some(&transcript),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 6000,
    };

    let error = engine
        .generate_compaction_summary(&session, &inputs, &mut |_| {})
        .await
        .expect_err("max output reserve leaves no room for summary input");

    assert!(error
        .to_string()
        .contains("remains over budget after omitting all tool results"));
    assert!(provider.requests().await.is_empty());
    assert!(session.read_metadata().await.unwrap().compaction.is_none());
}

#[tokio::test]
async fn compaction_summary_rechecks_budget_before_json_retry() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(&"界".repeat(4_000), Vec::new()),
        response_step(
            r#"{"committed_summary":"must not be requested","active_turn_summary":null}"#,
            Vec::new(),
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 1_500;
    engine.json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        256,
        1,
        Duration::ZERO,
        Duration::ZERO,
    ));
    let session = create_test_session(&store, "session_c0ffee23").await;
    let transcript = vec![TurnMessage {
        role: "user".into(),
        content: "plain summary input".into(),
    }];
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(1),
        prior_committed_summary: None,
        committed_transcript: Some(&transcript),
        committed_transcript_with_large_tool_results_omitted: Some(&transcript),
        committed_transcript_with_tool_results_omitted: Some(&transcript),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 6000,
    };

    let error = engine
        .generate_compaction_summary(&session, &inputs, &mut |_| {})
        .await
        .expect_err("retry correction should exceed the local compaction budget");

    assert!(error
        .to_string()
        .contains("provider attempt exceeds context window"));
    assert_eq!(provider.requests().await.len(), 1);
}

#[test]
fn compaction_summary_projections_redact_memory_tool_input_and_output() {
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::tool_use(
                "toolu_memory",
                "memory",
                json!({
                    "action": "write",
                    "content": "PRIVATE_MEMORY_INPUT"
                }),
            )],
        ),
        test_message(
            1,
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result(
                "toolu_memory",
                "PRIVATE_MEMORY_OUTPUT",
            )],
        ),
    ];

    let projection = session_compaction_transcript_projection(&messages, 4_096);

    for transcript in [
        projection.full,
        projection.large_tool_results_omitted,
        projection.tool_results_omitted,
    ] {
        let serialized = serde_json::to_string(&transcript).unwrap();
        assert!(serialized.contains("tool_use memory input omitted"));
        assert!(serialized.contains("tool_result memory output omitted"));
        assert!(!serialized.contains("PRIVATE_MEMORY_INPUT"));
        assert!(!serialized.contains("PRIVATE_MEMORY_OUTPUT"));
    }
}

#[test]
fn parse_compaction_summary_outcome_requires_committed_and_active_shape() {
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(2),
        prior_committed_summary: None,
        committed_transcript: Some(&[]),
        committed_transcript_with_large_tool_results_omitted: Some(&[]),
        committed_transcript_with_tool_results_omitted: Some(&[]),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 6000,
    };
    let ok = parse_compaction_summary_outcome(
        json!({"committed_summary": "压缩摘要", "active_turn_summary": null}),
        &inputs,
    )
    .unwrap();
    assert_eq!(ok.committed_summary.as_deref(), Some("压缩摘要"));

    let err = parse_compaction_summary_outcome(json!({"committed_summary": "压缩摘要"}), &inputs)
        .unwrap_err();
    assert!(err.to_string().contains("active_turn_summary key"));

    let err = parse_compaction_summary_outcome(json!({"active_turn_summary": null}), &inputs)
        .unwrap_err();
    assert!(err.to_string().contains("committed_summary key"));

    let err = parse_compaction_summary_outcome(
        json!({"committed_summary": null, "active_turn_summary": null}),
        &inputs,
    )
    .unwrap_err();
    assert!(err.to_string().contains("committed_summary"));

    let err = parse_compaction_summary_outcome(
        json!({"committed_summary": "   ", "active_turn_summary": null}),
        &inputs,
    )
    .unwrap_err();
    assert!(err.to_string().contains("must not be empty"));

    let err = parse_compaction_summary_outcome(
        json!({"committed_summary": "unexpected", "active_turn_summary": "x"}),
        &inputs,
    )
    .unwrap_err();
    assert!(err.to_string().contains("active_turn_summary"));

    let active_transcript = vec![TurnMessage {
        role: "assistant".into(),
        content: "finished active segment".into(),
    }];
    let active_inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::ActiveTurn),
        committed_start_index: None,
        committed_end_index: None,
        prior_committed_summary: None,
        committed_transcript: None,
        committed_transcript_with_large_tool_results_omitted: None,
        committed_transcript_with_tool_results_omitted: None,
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: Some(0),
        active_turn_end_segment: Some(1),
        active_turn_transcript: Some(&active_transcript),
        active_turn_transcript_with_large_tool_results_omitted: Some(&active_transcript),
        active_turn_transcript_with_tool_results_omitted: Some(&active_transcript),
        summary_max_chars: 6000,
    };
    let err = parse_compaction_summary_outcome(
        json!({"committed_summary": null, "active_turn_summary": ""}),
        &active_inputs,
    )
    .unwrap_err();
    assert!(err
        .to_string()
        .contains("active_turn_summary must not be empty"));
}

#[test]
fn parse_compaction_summary_outcome_rejects_summary_over_character_limit() {
    let transcript = vec![TurnMessage {
        role: "user".into(),
        content: "old request".into(),
    }];
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(1),
        prior_committed_summary: None,
        committed_transcript: Some(&transcript),
        committed_transcript_with_large_tool_results_omitted: Some(&transcript),
        committed_transcript_with_tool_results_omitted: Some(&transcript),
        prior_active_turn_summary: None,
        active_turn_user_anchor: None,
        active_turn_start_segment: None,
        active_turn_end_segment: None,
        active_turn_transcript: None,
        active_turn_transcript_with_large_tool_results_omitted: None,
        active_turn_transcript_with_tool_results_omitted: None,
        summary_max_chars: 4,
    };

    let error = parse_compaction_summary_outcome(
        json!({"committed_summary": "五个字符啊", "active_turn_summary": null}),
        &inputs,
    )
    .unwrap_err();

    assert!(error.to_string().contains("actual_chars=5"));
    assert!(error.to_string().contains("max_chars=4"));
}

#[test]
fn auto_compact_trigger_prefers_provider_ctx_usage() {
    let tokens = auto_compact_trigger_tokens(Some(123), Some(999)).unwrap();

    assert_eq!(tokens, 123);
}

#[test]
fn auto_compact_trigger_falls_back_to_estimate() {
    let tokens = auto_compact_trigger_tokens(None, Some(456)).unwrap();

    assert_eq!(tokens, 456);
}

#[test]
fn auto_compact_threshold_uses_context_window_ratio() {
    assert_eq!(auto_compact_trigger_threshold_tokens(200_000, 0.6), 120_000);
    assert_eq!(auto_compact_trigger_threshold_tokens(256_256, 0.5), 128_128);
    assert_eq!(auto_compact_trigger_threshold_tokens(999, 0.5), 500);
    assert_eq!(auto_compact_trigger_threshold_tokens(200_000, 0.0), 0);
}

#[test]
fn auto_compact_zero_threshold_disables_trigger() {
    assert!(!auto_compact_should_trigger(1, 0));
    assert!(!auto_compact_should_trigger(119_999, 120_000));
    assert!(auto_compact_should_trigger(120_000, 120_000));
}

#[test]
fn zero_auto_compact_ratio_keeps_manual_compaction_tail_budget() {
    assert_eq!(compaction_tail_token_limit(200_000, 0.0), 200_000);
    assert_eq!(compaction_tail_token_limit(200_000, 0.6), 120_000);
}

#[test]
fn compaction_tail_keeps_recent_three_real_user_turns_with_tool_results() {
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("old user")],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("old answer")],
        ),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent one")],
        ),
        test_message(
            3,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::tool_use(
                "toolu_1",
                "working_note",
                serde_json::json!({"text": "note"}),
            )],
        ),
        test_message(
            4,
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result("toolu_1", "tool output")],
        ),
        test_message(
            5,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("tool done")],
        ),
        test_message(
            6,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent two")],
        ),
        test_message(
            7,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("answer two")],
        ),
        test_message(
            8,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent three")],
        ),
        test_message(
            9,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("answer three")],
        ),
    ];

    let tail_start = select_compaction_summary_end_index(
        &messages,
        0,
        messages.len(),
        usize::MAX,
        3,
        4096,
        None,
    );

    assert_eq!(tail_start, 2);
}

#[test]
fn compaction_tail_ignores_shell_command_user_records() {
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("old user")],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("old answer")],
        ),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent one")],
        ),
        test_message(
            3,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("answer one")],
        ),
        test_message(
            4,
            SessionMessageRole::User,
            vec![SessionContentBlock::text(
                "<user_shell_command>\n<command>\necho hi\n</command>\n</user_shell_command>",
            )],
        ),
        test_message(
            5,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent two")],
        ),
        test_message(
            6,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("answer two")],
        ),
        test_message(
            7,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent three")],
        ),
    ];

    let tail_start = select_compaction_summary_end_index(
        &messages,
        0,
        messages.len(),
        usize::MAX,
        3,
        4096,
        None,
    );

    assert_eq!(tail_start, 2);
}

#[test]
fn compaction_tail_default_can_keep_four_previous_real_user_turns() {
    let messages = (0..10)
        .map(|index| {
            let role = if index % 2 == 0 {
                SessionMessageRole::User
            } else {
                SessionMessageRole::Assistant
            };
            test_message(
                index,
                role,
                vec![SessionContentBlock::text(format!("message {index}"))],
            )
        })
        .collect::<Vec<_>>();

    let tail_start = select_compaction_summary_end_index(
        &messages,
        0,
        messages.len(),
        usize::MAX,
        4,
        4096,
        None,
    );

    assert_eq!(tail_start, 2);
}

#[test]
fn compaction_tail_budget_estimates_large_tool_results_after_projection() {
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("old user")],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("old answer")],
        ),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("recent task")],
        ),
        test_message(
            3,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::tool_use(
                "toolu_1",
                "file_read",
                json!({"path": "huge.log"}),
            )],
        ),
        test_message(
            4,
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result(
                "toolu_1",
                "A".repeat(20_000),
            )],
        ),
        test_message(
            5,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("done")],
        ),
    ];
    let projected_tail_tokens =
        SessionEngine::estimated_projected_message_tokens(messages[2..].iter(), 128);
    let raw_tail_tokens = SessionEngine::estimated_message_tokens(messages[2..].iter());

    assert!(raw_tail_tokens > projected_tail_tokens);
    let tail_start = select_compaction_summary_end_index(
        &messages,
        0,
        messages.len(),
        projected_tail_tokens,
        1,
        128,
        None,
    );

    assert_eq!(tail_start, 2);
}

#[test]
fn compaction_tail_does_not_noop_when_full_raw_tail_exceeds_budget() {
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("first long task")],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::tool_use(
                "toolu_1",
                "code_run",
                json!({"script": "generate first"}),
            )],
        ),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result(
                "toolu_1",
                "A".repeat(20_000),
            )],
        ),
        test_message(
            3,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("first summarized")],
        ),
        test_message(
            4,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("second long task")],
        ),
        test_message(
            5,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::tool_use(
                "toolu_2",
                "code_run",
                json!({"script": "generate second"}),
            )],
        ),
        test_message(
            6,
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result(
                "toolu_2",
                "B".repeat(20_000),
            )],
        ),
        test_message(
            7,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("second summarized")],
        ),
    ];
    let projected_all_tokens =
        SessionEngine::estimated_projected_message_tokens(messages.iter(), 128);
    let raw_all_tokens = SessionEngine::estimated_message_tokens(messages.iter());

    assert!(raw_all_tokens > projected_all_tokens);
    let tail_start = select_compaction_summary_end_index(
        &messages,
        0,
        messages.len(),
        projected_all_tokens,
        4,
        128,
        None,
    );

    assert_eq!(tail_start, 4);
}

#[test]
fn active_segments_do_not_cut_open_tool_use() {
    let active = vec![
        SessionTurnMessage::user_text(
            "<runtime_context>\ncurrent_date: 2026-06-30 Tuesday\n</runtime_context>\n\nuser task",
        ),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "file_read".into(),
                input: json!({"path": "big.txt"}),
            }],
        },
    ];

    assert!(active_provider_safe_segments(&active).is_empty());

    let mut closed = active;
    closed.push(SessionTurnMessage {
        role: "user".into(),
        provider_replay: None,
        content: vec![SessionTurnContentBlock::ToolResult {
            tool_use_id: "toolu_1".into(),
            content: "tool output".into(),
        }],
    });

    let segments = active_provider_safe_segments(&closed);
    assert_eq!(segments.len(), 1);
    assert_eq!((segments[0].start, segments[0].end), (1, 3));
}

#[test]
fn provider_projection_preserves_current_anchor_and_omits_large_tool_result_raw() {
    let active = vec![
            SessionTurnMessage::user_text("<runtime_context>\ncurrent_date: 2026-06-30 Tuesday\ntimezone: Asia/Shanghai\n</runtime_context>\n\ncontinue the long task"),
            SessionTurnMessage::assistant_text("earlier progress that is now summarized"),
            SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "code_run".into(),
                    input: json!({"cmd": "cat huge.log"}),
                }],
            },
            SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "toolu_1".into(),
                    content: "A".repeat(128),
                }],
            },
        ];
    let segments = active_provider_safe_segments(&active);
    let source_hash = active_segments_hash(&active, &segments[..1]).unwrap();
    let mut state =
        SessionCompactionState::from_committed_summary(0, "earlier summary".into(), Utc::now());
    state.active_turn_summary = Some("active tool output was summarized".into());
    state.frontier.active_turn = Some(ActiveTurnCompactionCursor {
        turn_id: "turn_1".into(),
        base_message_count: 0,
        compacted_until_segment: 1,
        safe_until_event_seq: 0,
        source_hash,
    });

    let projection = project_provider_context(
        "system",
        &state,
        &[],
        active,
        ActiveProjectionContext {
            turn_id: "turn_1",
            base_message_count: 0,
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 4,
            tool_result_raw_max_chars: 16,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();

    assert_eq!(projection.system_prompt, "system");
    assert!(rendered.contains("compacted_session_context"));
    assert!(rendered.contains("Earlier Conversation"));
    assert!(rendered.contains("runtime file-edit authority"));
    assert!(rendered.contains("required_read"));
    assert!(rendered.contains("current_date: 2026-06-30 Tuesday"));
    assert!(rendered.contains("continue the long task"));
    assert!(rendered.contains("compacted_current_turn_progress"));
    assert!(rendered.contains("active tool output was summarized"));
    assert!(!rendered.contains(&"A".repeat(64)));
    assert!(rendered.contains("large tool_result omitted"));
}

#[test]
fn provider_projection_injects_active_progress_note_after_compaction() {
    let active = vec![
        SessionTurnMessage::user_text("create the file, then verify it once"),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "code_run".into(),
                input: json!({"script": "create file"}),
            }],
        },
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "CHAR_COUNT=20058\nLINE_COUNT=252\n".into(),
            }],
        },
    ];
    let segments = active_provider_safe_segments(&active);
    let source_hash = active_segments_hash(&active, &segments[..1]).unwrap();
    let mut state = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    state.active_turn_summary = Some(
            "The file has already been created successfully. Next, verify the existing file; do not recreate it."
                .into(),
        );
    state.frontier.active_turn = Some(ActiveTurnCompactionCursor {
        turn_id: "turn_1".into(),
        base_message_count: 0,
        compacted_until_segment: 1,
        safe_until_event_seq: 0,
        source_hash,
    });

    let projection = project_provider_context(
        "system",
        &state,
        &[],
        active,
        ActiveProjectionContext {
            turn_id: "turn_1",
            base_message_count: 0,
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 4,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );

    assert_eq!(projection.messages.len(), 2);
    assert_eq!(projection.messages[0].role, "user");
    assert_eq!(projection.messages[1].role, "user");
    let rendered = serde_json::to_string(&projection.messages).unwrap();
    assert!(rendered.contains("create the file, then verify it once"));
    assert!(rendered.contains("compacted_current_turn_progress"));
    assert!(rendered.contains("runtime file-edit authority"));
    assert!(rendered.contains("required_read"));
    assert!(rendered.contains("already been created successfully"));
    assert!(rendered.contains("do not recreate it"));
    assert!(!rendered.contains("CHAR_COUNT=20058"));
    assert!(!rendered.contains("create file"));
}

#[test]
fn provider_projection_skips_active_segments_covered_by_cursor() {
    let active = vec![
        SessionTurnMessage::user_text(
            "<runtime_context>\ncurrent_date: 2026-06-30 Tuesday\n</runtime_context>\n\nuser task",
        ),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "file_read".into(),
                input: json!({"path": "old.txt"}),
            }],
        },
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "old output".into(),
            }],
        },
        SessionTurnMessage::assistant_text("latest progress text"),
    ];
    let segments = active_provider_safe_segments(&active);
    let source_hash = active_segments_hash(&active, &segments[..1]).unwrap();
    let mut state = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    state.active_turn_summary = Some("old tool round summarized".into());
    state.frontier.active_turn = Some(ActiveTurnCompactionCursor {
        turn_id: "turn_1".into(),
        base_message_count: 0,
        compacted_until_segment: 1,
        safe_until_event_seq: 0,
        source_hash,
    });

    let projection = project_provider_context(
        "system",
        &state,
        &[],
        active,
        ActiveProjectionContext {
            turn_id: "turn_1",
            base_message_count: 0,
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 4,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();

    assert!(rendered.contains("user task"));
    assert!(rendered.contains("compacted_current_turn_progress"));
    assert!(rendered.contains("old tool round summarized"));
    assert!(rendered.contains("latest progress text"));
    assert!(!rendered.contains("old output"));
    assert!(!rendered.contains("old.txt"));
}

#[test]
fn provider_projection_ignores_active_summary_when_cursor_hash_mismatches() {
    let original_active = vec![
        SessionTurnMessage::user_text("current task"),
        SessionTurnMessage::assistant_text("old summarized progress"),
    ];
    let segments = active_provider_safe_segments(&original_active);
    let source_hash = active_segments_hash(&original_active, &segments[..1]).unwrap();
    let mut state = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    state.active_turn_summary = Some("old progress was summarized".into());
    state.frontier.active_turn = Some(ActiveTurnCompactionCursor {
        turn_id: "turn_1".into(),
        base_message_count: 0,
        compacted_until_segment: 1,
        safe_until_event_seq: 0,
        source_hash,
    });
    let projected_active = vec![
        SessionTurnMessage::user_text("current task"),
        SessionTurnMessage::assistant_text("new raw progress after projection"),
    ];

    let projection = project_provider_context(
        "system",
        &state,
        &[],
        projected_active,
        ActiveProjectionContext {
            turn_id: "turn_1",
            base_message_count: 0,
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 4,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();

    assert_eq!(projection.system_prompt, "system");
    assert!(rendered.contains("current task"));
    assert!(rendered.contains("new raw progress after projection"));
    assert!(!rendered.contains("old progress was summarized"));
}

#[tokio::test]
async fn committed_compacted_context_projects_large_tool_results() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee08").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old summarized request"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::tool_use(
                    "toolu_1",
                    "file_read",
                    json!({"path": "huge.log"}),
                )],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "toolu_1",
                    "A".repeat(20_000),
                )],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "done"),
        ])
        .await
        .unwrap();
    session
        .update_compaction(SessionCompactionState::from_committed_summary(
            1,
            "old request summarized".into(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();

    let (system_prompt, history) = compacted_context_for_turn(
        "system",
        &metadata,
        messages,
        usize::MAX,
        usize::MAX,
        4,
        128,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    )
    .unwrap();
    let rendered = serde_json::to_string(&history).unwrap();

    assert_eq!(system_prompt, "system");
    assert!(rendered.contains("Earlier Conversation"));
    assert!(rendered.contains("old request summarized"));
    assert!(rendered.contains("large tool_result omitted"));
    assert!(!rendered.contains(&"A".repeat(100)));
}

#[tokio::test]
async fn committed_compacted_context_preserves_recent_user_and_turn_end_answer() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0c").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old summarized request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old summarized answer"),
            NewSessionMessage::text(SessionMessageRole::User, "recent user requirement"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::tool_use(
                    "toolu_1",
                    "file_read",
                    json!({"path": "huge.log"}),
                )],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "toolu_1",
                    "A".repeat(16_000),
                )],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "recent final answer"),
        ])
        .await
        .unwrap();
    session
        .update_compaction(SessionCompactionState::from_committed_summary(
            6,
            "committed summary covers all prior messages".into(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();

    let (_system_prompt, history) = compacted_context_for_turn(
        "system",
        &metadata,
        messages,
        usize::MAX,
        usize::MAX,
        1,
        128,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    )
    .unwrap();
    let rendered = serde_json::to_string(&history).unwrap();

    assert!(rendered.contains("recent user requirement"));
    assert!(rendered.contains("recent final answer"));
    assert!(!rendered.contains(&"A".repeat(256)));
    assert!(!rendered.contains("huge.log"));
    assert!(!rendered.contains("old summarized request"));
}

#[test]
fn provider_projection_prunes_committed_preserves_to_respect_global_hard_budget() {
    let session_messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text(
                "previous requirement ".repeat(200),
            )],
        ),
        test_message(
            1,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text(
                "previous final answer ".repeat(200),
            )],
        ),
    ];
    let active = vec![
        SessionTurnMessage::user_text("current request"),
        SessionTurnMessage::assistant_text("latest progress stays raw"),
    ];
    let summary = "summary covers previous turn";
    let summary_tokens = compacted_committed_summary_message(summary)
        .as_ref()
        .map(|message| estimate_session_turn_messages_tokens(std::slice::from_ref(message)))
        .unwrap_or(0);
    let mandatory_tokens =
        summary_tokens.saturating_add(estimate_session_turn_messages_tokens(&active));
    let state = SessionCompactionState::from_committed_summary(2, summary.into(), Utc::now());

    let projection = project_provider_context(
        "system",
        &state,
        &session_messages,
        active,
        ActiveProjectionContext {
            turn_id: "turn_1",
            base_message_count: 2,
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: mandatory_tokens,
            tail_previous_real_user_turns: 1,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();

    assert!(rendered.contains("current request"));
    assert!(rendered.contains("latest progress stays raw"));
    assert!(!rendered.contains("previous requirement"));
    assert!(!rendered.contains("previous final answer"));
    assert!(estimate_session_turn_messages_tokens(&projection.messages) <= mandatory_tokens);
}

#[tokio::test]
async fn preflight_committed_tail_selection_reserves_budget_for_current_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 1_000;
    engine.compaction.tail_target_ctx_ratio = 0.20;
    engine.compaction.tail_hard_ctx_ratio = 0.30;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_c0ffee0d").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "old answer"),
            NewSessionMessage::text(SessionMessageRole::User, "recent requirement ".repeat(16)),
            NewSessionMessage::text(SessionMessageRole::Assistant, "recent final ".repeat(18)),
        ])
        .await
        .unwrap();
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![SessionTurnMessage::user_text("current anchor ".repeat(48))];

    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    assert_eq!(plan.ranges.summary_end_index, messages.len());
    assert!(plan.committed_transcript.is_some());
}

#[tokio::test]
async fn preflight_committed_tail_does_not_noop_when_full_raw_tail_exceeds_budget() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 15_000;
    engine.compaction.tail_target_ctx_ratio = 0.20;
    engine.compaction.tail_hard_ctx_ratio = 0.30;
    engine.compaction.tail_previous_real_user_turns = 4;
    engine.compaction.tool_result_raw_max_chars = 128;
    let mut session = create_test_session(&store, "session_c0ffee11").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "first long task"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::tool_use(
                    "toolu_1",
                    "code_run",
                    json!({"script": "generate first"}),
                )],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "toolu_1",
                    "A".repeat(20_000),
                )],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "first summarized"),
            NewSessionMessage::text(SessionMessageRole::User, "second long task"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::tool_use(
                    "toolu_2",
                    "code_run",
                    json!({"script": "generate second"}),
                )],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "toolu_2",
                    "B".repeat(20_000),
                )],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "second summarized"),
        ])
        .await
        .unwrap();
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![SessionTurnMessage::user_text(
        "Reply exactly AFTER_COMPACT_OK. Do not use tools.",
    )];

    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_2",
                base_message_count: messages.len(),
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    assert_eq!(plan.ranges.summary_end_index, 4);
    let full = plan.committed_transcript.as_ref().unwrap();
    assert!(full
        .iter()
        .any(|message| message.content.contains(&"A".repeat(1_000))));
    let projected = plan
        .committed_transcript_with_large_tool_results_omitted
        .as_ref()
        .unwrap();
    assert!(projected.iter().any(|message| message
        .content
        .contains("tool_result omitted from compaction summary input")));
    assert!(!projected
        .iter()
        .any(|message| message.content.contains(&"A".repeat(1_000))));
    assert!(messages[2].content.iter().any(|block| matches!(
        block,
        SessionContentBlock::ToolResult { content, .. }
            if content.contains(&"A".repeat(1_000))
    )));
}

#[tokio::test]
async fn preflight_committed_tail_selection_reserves_budget_for_committed_summary_message() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.compaction.tail_target_ctx_ratio = 1.0;
    engine.compaction.tail_hard_ctx_ratio = 1.0;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_c0ffee10").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "already summarized request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "already summarized answer"),
            NewSessionMessage::text(SessionMessageRole::User, "recent raw request ".repeat(8)),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "recent raw answer ".repeat(8),
            ),
        ])
        .await
        .unwrap();
    let prior_summary = "prior committed summary ".repeat(20);
    engine.compaction.summary_max_chars = prior_summary.chars().count();
    session
        .update_compaction(SessionCompactionState::from_committed_summary(
            2,
            prior_summary.clone(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![SessionTurnMessage::user_text("current anchor")];
    let active_tokens = estimate_session_turn_messages_tokens(&active);
    let summary_tokens = estimate_compacted_committed_summary_message_tokens(&prior_summary);
    let recent_tail_tokens = SessionEngine::estimated_projected_message_tokens(
        messages[2..].iter(),
        engine.compaction.tool_result_raw_max_chars,
    );
    engine.context_window = active_tokens
        .saturating_add(summary_tokens)
        .saturating_add(recent_tail_tokens)
        .saturating_sub(1);

    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    assert_eq!(plan.ranges.summary_start_index, 2);
    assert_eq!(plan.ranges.summary_end_index, messages.len());
    assert!(plan.committed_transcript.is_some());
}

#[tokio::test]
async fn preflight_recovers_matching_compaction_checkpoint_before_replanning() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0e").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "checkpointed request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "checkpointed answer"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let segment_hash = hash_session_segment(&messages).unwrap();
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: vec!["compact_recovered".into()],
        summary_start_index: 0,
        summary_end_index: messages.len(),
        summary_segment_hash: segment_hash.clone(),
        recap_start_index: 0,
        recap_end_index: messages.len(),
        recap_segment_hash: segment_hash,
        summary: "recovered committed summary".into(),
        active_turn_summary: None,
        active_turn: None,
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: String::new(),
        trace_created_at: Utc::now(),
        trace_id: None,
        applied_report: None,
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
        .await
        .unwrap();

    let mut events = Vec::new();
    let projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                active_suffix: vec![SessionTurnMessage::user_text("continue")],
                turn_id: "turn_1",
                base_message_count: messages.len(),
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
            },
            &mut |event| events.push(event),
        )
        .await
        .unwrap();

    let projection = projection.unwrap();
    assert_eq!(projection.system_prompt, "system");
    let rendered = serde_json::to_string(&projection.messages).unwrap();
    assert!(rendered.contains("recovered committed summary"));
    assert!(rendered.contains("compacted_session_context"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, messages.len());
    let compaction = metadata.compaction.unwrap();
    assert_eq!(compaction.committed_message_until(), messages.len());
    assert_eq!(
        compaction.committed_summary(),
        "recovered committed summary"
    );
    assert!(compaction.active_turn_summary.is_none());
    assert!(compaction.frontier.active_turn.is_none());
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit_log.contains(r#""audit_id":"compact_recovered""#));
    assert!(audit_log.contains(r#""kind":"completed""#));
    assert!(audit_log.contains(r#""recovered":true"#));
}

#[tokio::test]
async fn preflight_checkpoint_recovery_validation_failure_writes_failed_audit() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0f").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "checkpointed request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "checkpointed answer"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let segment_hash = hash_session_segment(&messages).unwrap();
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: vec!["compact_bad_hash".into()],
        summary_start_index: 0,
        summary_end_index: messages.len(),
        summary_segment_hash: "wrong_hash".into(),
        recap_start_index: 0,
        recap_end_index: messages.len(),
        recap_segment_hash: segment_hash,
        summary: "recovered committed summary".into(),
        active_turn_summary: None,
        active_turn: None,
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: String::new(),
        trace_created_at: Utc::now(),
        trace_id: None,
        applied_report: None,
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
        .await
        .unwrap();

    let err = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                active_suffix: vec![SessionTurnMessage::user_text("continue")],
                turn_id: "turn_1",
                base_message_count: messages.len(),
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("summary_segment_hash"));
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit_log.contains(r#""audit_id":"compact_bad_hash""#));
    assert!(audit_log.contains(r#""kind":"failed""#));
}

#[tokio::test]
async fn preflight_plan_does_not_reject_full_anchor_before_projection_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 100;
    engine.compaction.tail_hard_ctx_ratio = 0.01;
    let session = create_test_session(&store, "session_c0ffee02").await;
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![SessionTurnMessage::user_text("x ".repeat(1000))];

    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    assert!(plan.active_turn.is_none());
}

#[tokio::test]
async fn subagent_summary_projection_is_bounded_and_omits_private_context() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_d011e9a1").await;
    let delegation_store = DelegationStore::new(session.paths.dir.clone());
    delegation_store
        .create(DelegationCreateRequest {
            parent_session_id: session.metadata.id.clone(),
            parent_turn_id: "turn_1".into(),
            owner_agent_id: AgentId::new("agent-a").unwrap(),
            title: "scan repo".into(),
            role: "code researcher".into(),
            objective: "private objective that must not be projected".repeat(40),
            constraints: vec!["private constraint that must not be projected".repeat(20)],
        })
        .await
        .unwrap();

    let projection = delegation_summary_projection(&session.paths.dir)
        .await
        .unwrap()
        .unwrap();

    assert!(projection.contains("<subagent_summary_projection>"));
    assert!(projection.contains("scan repo"));
    assert!(projection.contains("code researcher"));
    assert!(projection.contains("queued"));
    assert!(!projection.contains("private objective"));
    assert!(!projection.contains("private constraint"));
    assert!(projection.chars().count() <= DELEGATION_PROJECTION_MAX_CHARS);
    let json = projection
        .trim_start_matches("<subagent_summary_projection>")
        .trim_end_matches("</subagent_summary_projection>")
        .trim();
    serde_json::from_str::<serde_json::Value>(json).expect("projection is valid JSON");
}

#[tokio::test]
async fn subagent_summary_projection_reports_true_omitted_count_beyond_store_default_limit() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_d011e9a2").await;
    let delegation_store = DelegationStore::new(session.paths.dir.clone());
    let total = 70usize;

    for index in 0..total {
        delegation_store
            .create(DelegationCreateRequest {
                parent_session_id: session.metadata.id.clone(),
                parent_turn_id: format!("turn_{index}"),
                owner_agent_id: AgentId::new("agent-a").unwrap(),
                title: format!("d{index}"),
                role: "r".into(),
                objective: "o".into(),
                constraints: Vec::new(),
            })
            .await
            .unwrap();
    }

    let projection = delegation_summary_projection(&session.paths.dir)
        .await
        .unwrap()
        .unwrap();
    let json = projection
        .trim_start_matches("<subagent_summary_projection>")
        .trim_end_matches("</subagent_summary_projection>")
        .trim();
    let value = serde_json::from_str::<serde_json::Value>(json).expect("projection is valid JSON");

    assert_eq!(
        value["subagents"].as_array().unwrap().len(),
        DELEGATION_PROJECTION_MAX_ITEMS
    );
    assert_eq!(
        value["omitted"].as_u64().unwrap(),
        u64::try_from(total - DELEGATION_PROJECTION_MAX_ITEMS).unwrap()
    );
}

#[tokio::test]
async fn subagent_summary_projection_uses_public_name_in_read_errors() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_d011e9a3").await;
    tokio::fs::write(session.paths.dir.join("delegations"), b"not a directory")
        .await
        .unwrap();

    let error = delegation_summary_projection(&session.paths.dir)
        .await
        .expect_err("a non-directory subagent store must fail projection reads");
    let message = format!("{error:#}");

    assert!(message.contains("读取 subagent summary projection 失败"));
    assert!(!message.contains("读取 delegation summary projection 失败"));
}

#[tokio::test]
async fn reopen_existing_session_clears_runtime_file_read_state() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..ToolConfig::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    let (engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    let mut session = create_test_session(&store, "session_d011e9af").await;
    tokio::fs::write(dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();
    let context = ToolDispatchContext {
        current_session_id: Some(session.metadata.id.clone()),
        ..ToolDispatchContext::default()
    };
    tools
        .dispatch_with_context(
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
            context.clone(),
        )
        .await
        .unwrap();
    session.mark_closed(Utc::now()).await.unwrap();

    engine
        .reopen_existing_session(&session.metadata.id)
        .await
        .unwrap();
    let output = tools
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            context,
        )
        .await
        .unwrap();

    assert_eq!(output.output["status"], "error");
    assert_eq!(
        tokio::fs::read_to_string(dir.path().join("note.txt"))
            .await
            .unwrap(),
        "before\n"
    );
}

#[tokio::test]
async fn reopen_existing_session_abandons_unfinished_delegations_for_closed_session() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine_with_delegation_host(&dir, provider);
    let mut session = create_test_session(&store, "session_d011e9a0").await;
    let now = Utc::now();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            now,
            TurnJournalEventKind::UserInputAccepted {
                text: "closed resume request".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    let delegation_store = DelegationStore::new(session.paths.dir.clone());
    let delegation = delegation_store
        .create(DelegationCreateRequest {
            parent_session_id: session.metadata.id.clone(),
            parent_turn_id: "turn_1".into(),
            owner_agent_id: AgentId::new("agent-a").unwrap(),
            title: "restore cleanup".into(),
            role: "verifier".into(),
            objective: "prove unfinished delegation is abandoned on restore".into(),
            constraints: Vec::new(),
        })
        .await
        .unwrap();
    delegation_store.start(&delegation.id).await.unwrap();
    session.mark_closed(now).await.unwrap();

    let reopened = engine
        .reopen_existing_session(&session.metadata.id)
        .await
        .unwrap();

    assert_eq!(reopened.metadata.status, SessionStatus::Open);
    let summaries = delegation_store.list().await.unwrap();
    let summary = summaries
        .iter()
        .find(|summary| summary.id == delegation.id)
        .expect("delegation summary should remain readable after restore cleanup");
    assert_eq!(summary.status, DelegationStatus::Abandoned);
}

#[tokio::test]
async fn preflight_injects_delegation_projection_as_synthetic_user_context() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 10_000;
    engine.compaction.auto_compact_ctx_ratio = 1.0;
    let mut session = create_test_session(&store, "session_d011e9a2").await;
    DelegationStore::new(session.paths.dir.clone())
        .create(DelegationCreateRequest {
            parent_session_id: session.metadata.id.clone(),
            parent_turn_id: "turn_1".into(),
            owner_agent_id: AgentId::new("agent-a").unwrap(),
            title: "verify patch".into(),
            role: "verifier".into(),
            objective: "verify the current patch".into(),
            constraints: Vec::new(),
        })
        .await
        .unwrap();
    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };
    let mut system_prompt = "system".to_string();
    let mut provider_messages = vec![SessionTurnMessage::user_text("hello")];

    preflight
        .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |_event| {})
        .await
        .unwrap();

    assert_eq!(system_prompt, "system");
    assert_eq!(provider_messages.len(), 2);
    assert_eq!(preflight.active_start_index, 1);
    assert!(format!("{:?}", provider_messages[0]).contains("<subagent_summary_projection>"));
    assert!(format!("{:?}", provider_messages[0]).contains("verify patch"));
    assert_eq!(provider_messages[1], SessionTurnMessage::user_text("hello"));
}

#[tokio::test]
async fn preflight_runtime_budget_includes_delegation_and_background_projections() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_d011e9a4").await;
    let delegation_projection =
        "<subagent_summary_projection>delegation state</subagent_summary_projection>";
    let background_projection = "<background_processes>process state</background_processes>";
    let expected = estimate_session_turn_messages_tokens(&[
        SessionTurnMessage::user_text(delegation_projection),
        SessionTurnMessage::user_text(background_projection),
    ]);
    let preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        delegation_projection_loaded: true,
        delegation_projection: Some(delegation_projection.into()),
        delegation_projection_inserted: false,
        background_projection: Some(background_projection.into()),
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };

    assert_eq!(preflight.runtime_projection_tokens(), expected);
}

#[tokio::test]
#[cfg(unix)]
async fn preflight_injects_owner_scoped_background_projection_without_persisting_it() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    let (mut engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    engine.context_window = 10_000;
    engine.compaction.auto_compact_ctx_ratio = 1.0;
    let mut session = create_test_session(&store, "session_d011e9b3").await;
    let started = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 5", "yield_time_ms": 250}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    let process_id = started.output["process_id"].as_str().unwrap().to_string();

    let mut system_prompt = "system".to_string();
    let mut provider_messages = vec![SessionTurnMessage::user_text("hello")];
    {
        let mut preflight = PreflightCompactor {
            engine: &engine,
            session: &mut session,
            active_start_index: 0,
            turn_id: "turn_1".into(),
            base_message_count: 0,
            active_projection_compacted: false,
            provider_context_anchor: None,
            delegation_projection_loaded: false,
            delegation_projection: None,
            delegation_projection_inserted: false,
            background_projection: None,
            background_projection_insert_index: None,
            background_completion_delivery_ids: Vec::new(),
        };
        preflight
            .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |_event| {})
            .await
            .unwrap();
    }

    let projection = serde_json::to_string(&provider_messages[0]).unwrap();
    assert!(projection.contains("<background_processes>"));
    assert!(projection.contains(&process_id));
    assert!(projection.contains("Live processes"));
    let canonical = serde_json::to_string(&session.read_messages().await.unwrap()).unwrap();
    assert!(!canonical.contains("<background_processes>"));

    engine
        .cleanup_processes_for_session(&session.metadata.id)
        .await;
}

#[tokio::test]
#[cfg(unix)]
async fn preflight_does_not_apply_compacted_tail_limit_before_auto_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    let (mut engine, store) = build_test_engine_with_tools(&dir, provider, Arc::clone(&tools));
    engine.context_window = 200_000;
    engine.compaction.auto_compact_ctx_ratio = 1.0;
    engine.compaction.tail_hard_ctx_ratio = 0.30;
    let mut session = create_test_session(&store, "session_d011e9b4").await;
    let started = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 5", "yield_time_ms": 250}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    let process_id = started.output["process_id"].as_str().unwrap().to_string();
    let active_suffix = vec![
        SessionTurnMessage::user_text("continue the current task"),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_large".into(),
                name: "code_run".into(),
                input: json!({"script": "generate a large diagnostic"}),
            }],
        },
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_large".into(),
                content: "A".repeat(245_000),
            }],
        },
    ];
    assert!(
        estimate_session_turn_messages_tokens(&active_suffix)
            > engine.compaction_hard_tail_token_limit()
    );
    assert!(
        estimate_session_turn_messages_tokens(&active_suffix)
            < auto_compact_trigger_threshold_tokens(
                engine.context_window,
                engine.compaction.auto_compact_ctx_ratio,
            )
    );
    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };
    let mut system_prompt = "system".to_string();
    let mut provider_messages = active_suffix;

    preflight
        .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |_event| {})
        .await
        .unwrap();

    let rendered = serde_json::to_string(&provider_messages).unwrap();
    assert!(rendered.contains("<background_processes>"));
    assert!(rendered.contains(&process_id));
    assert!(rendered.contains(&"A".repeat(1_000)));

    engine
        .cleanup_processes_for_session(&session.metadata.id)
        .await;
}

#[test]
fn provider_projection_budget_reserves_background_runtime_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, _store) = build_test_engine(&dir, provider);
    engine.context_window = 200_000;
    engine.compaction.tail_target_ctx_ratio = 0.20;
    engine.compaction.tail_hard_ctx_ratio = 0.30;

    let budget = engine.provider_projection_budget(1_234);

    assert_eq!(budget.tail_token_limit, 40_000 - 1_234);
    assert_eq!(budget.tail_hard_token_limit, 60_000 - 1_234);
    assert_eq!(
        engine
            .provider_projection_budget(usize::MAX)
            .tail_hard_token_limit,
        0
    );
}

#[tokio::test]
async fn active_compaction_plan_uses_runtime_reserved_soft_budget() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 1_000;
    engine.compaction.tail_target_ctx_ratio = 0.20;
    engine.compaction.tail_hard_ctx_ratio = 0.30;
    let session = create_test_session(&store, "session_d011e9b5").await;
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let tool_round = |id: &str, fill: char| {
        vec![
            SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: format!("toolu_{id}"),
                    name: "code_run".into(),
                    input: json!({"script": fill.to_string().repeat(120)}),
                }],
            },
            SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: format!("toolu_{id}"),
                    content: fill.to_string().repeat(120),
                }],
            },
        ]
    };
    let mut active = vec![SessionTurnMessage::user_text("continue")];
    active.extend(tool_round("one", 'A'));
    active.extend(tool_round("two", 'B'));

    let without_reservation = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();
    let with_reservation = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(100),
        )
        .unwrap();

    assert!(without_reservation.active_turn.is_none());
    assert_eq!(
        with_reservation
            .active_turn
            .as_ref()
            .map(|plan| plan.summary_end_segment),
        Some(1)
    );
}

#[test]
fn active_compaction_keeps_delegation_management_io() {
    let messages = vec![
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_deleg".into(),
                name: "create_subagent".into(),
                input: json!({
                    "title": "private title",
                    "objective": "SECRET_OBJECTIVE",
                }),
            }],
        },
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_deleg".into(),
                content: json!({
                    "ok": true,
                    "output": {
                        "result_markdown": "SECRET_RESULT",
                        "summary": {
                            "id": "subagent_secret",
                            "title": "private title",
                        }
                    }
                })
                .to_string(),
            }],
        },
    ];

    let rendered = format!("{messages:?}");

    assert!(rendered.contains("SECRET_OBJECTIVE"));
    assert!(rendered.contains("SECRET_RESULT"));
    assert!(rendered.contains("subagent_secret"));
    assert!(rendered.contains("private title"));
    assert!(!rendered.contains("details_omitted_from_transcript"));
}

#[tokio::test]
async fn preflight_keeps_oversized_anchor_when_auto_compaction_does_not_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 10_000;
    engine.compaction.auto_compact_ctx_ratio = 1.0;
    engine.compaction.tail_hard_ctx_ratio = 0.01;
    let mut session = create_test_session(&store, "session_c0ffee0a").await;
    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };
    let mut system_prompt = "system".to_string();
    let mut provider_messages = vec![SessionTurnMessage::user_text("x ".repeat(1_000))];

    preflight
        .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |_event| {})
        .await
        .unwrap();

    assert_eq!(provider_messages.len(), 1);
    assert!(format!("{:?}", provider_messages[0]).contains(&"x ".repeat(100)));
}

#[tokio::test]
async fn preflight_trigger_uses_session_provider_context_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0c").await;
    engine.set_active_context_usage_anchor(session.metadata.id.clone(), 0, 900);
    let provider_messages = vec![SessionTurnMessage::user_text("new request")];
    let preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };

    let tokens = preflight.trigger_context_tokens("system", &provider_messages);

    assert_eq!(
        tokens,
        (900 + estimate_session_turn_messages_tokens(&provider_messages)).max(
            engine
                .turn_loop
                .estimate_context_tokens("system", &provider_messages)
        )
    );
}

#[tokio::test]
async fn preflight_trigger_uses_in_turn_provider_context_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0d").await;
    let provider_messages = vec![
        SessionTurnMessage::user_text("request"),
        SessionTurnMessage::assistant_text("calling tool"),
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "tool_1".into(),
                content: "tool output".into(),
            }],
        },
    ];
    let preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: Some(ProviderContextUsageAnchor {
            provider_message_count: 2,
            used_tokens: 1_200,
        }),
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };

    let tokens = preflight.trigger_context_tokens("system", &provider_messages);

    assert_eq!(
        tokens,
        (1_200 + estimate_session_turn_messages_tokens(&provider_messages[2..])).max(
            engine
                .turn_loop
                .estimate_context_tokens("system", &provider_messages)
        )
    );
}

#[tokio::test]
async fn removing_background_projection_invalidates_provider_context_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0f").await;
    let mut provider_messages = vec![
        SessionTurnMessage::user_text("<background_processes>old</background_processes>"),
        SessionTurnMessage::user_text("original user request"),
    ];
    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: Some(ProviderContextUsageAnchor {
            provider_message_count: provider_messages.len(),
            used_tokens: 1_200,
        }),
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: Some(0),
        background_completion_delivery_ids: Vec::new(),
    };

    preflight.remove_background_projection(&mut provider_messages);
    provider_messages.push(SessionTurnMessage::user_text(
        "tool result after provider reply",
    ));

    assert!(preflight.provider_context_anchor.is_none());
    assert_eq!(
        preflight.trigger_context_tokens("system", &provider_messages),
        engine
            .turn_loop
            .estimate_context_tokens("system", &provider_messages)
    );
}

#[tokio::test]
async fn preflight_trigger_uses_session_anchor_as_high_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee0f").await;
    engine.set_active_context_usage_anchor(session.metadata.id.clone(), 0, 12_000);
    let provider_messages = vec![SessionTurnMessage::user_text("small request")];
    let preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        delegation_projection_loaded: false,
        delegation_projection: None,
        delegation_projection_inserted: false,
        background_projection: None,
        background_projection_insert_index: None,
        background_completion_delivery_ids: Vec::new(),
    };

    let tokens = preflight.trigger_context_tokens("system", &provider_messages);

    assert_eq!(
        tokens,
        12_000 + estimate_session_turn_messages_tokens(&provider_messages)
    );
}

#[tokio::test]
async fn active_compaction_plan_preserves_latest_assistant_progress_raw() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 1_000;
    engine.compaction.tail_target_ctx_ratio = 0.20;
    engine.compaction.tool_result_raw_max_chars = 16;
    let session = create_test_session(&store, "session_c0ffee0b").await;
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![
        SessionTurnMessage::user_text("current request"),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "file_read".into(),
                input: json!({"path": "huge.log"}),
            }],
        },
        SessionTurnMessage {
            role: "user".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "A".repeat(1_024),
            }],
        },
        SessionTurnMessage::assistant_text("latest progress stays raw"),
    ];

    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    let active_plan = plan.active_turn.unwrap();
    assert_eq!(active_plan.summary_start_segment, 0);
    assert_eq!(active_plan.summary_end_segment, 1);
    assert!(active_plan
        .transcript
        .iter()
        .any(|message| message.content.contains("huge.log")));
    assert!(!active_plan
        .transcript
        .iter()
        .any(|message| message.content.contains("latest progress stays raw")));
}

#[tokio::test]
async fn preflight_plan_noops_when_no_new_safe_segments() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_c0ffee03").await;
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();
    let active = vec![SessionTurnMessage::user_text(
        "<runtime_context>\ncurrent_date: 2026-06-30 Tuesday\n</runtime_context>\n\nuser task",
    )];

    let plan = engine
        .build_preflight_compaction_plan(
            &metadata,
            &messages,
            &active,
            ActiveProjectionContext {
                turn_id: "turn_1",
                base_message_count: 0,
            },
            false,
            engine.preflight_runtime_projection_budget(0),
        )
        .unwrap();

    assert!(plan.committed_transcript.is_none());
    assert!(plan.active_turn.is_none());
}

#[test]
fn media_blocks_use_fixed_estimated_tokens_instead_of_base64_length() {
    let huge_base64 = "A".repeat(1_000_000);
    let media_only = [test_message(
        0,
        SessionMessageRole::User,
        vec![
            SessionContentBlock::image("image/png", huge_base64.clone()),
            SessionContentBlock::Document {
                media_type: "application/pdf".into(),
                data: huge_base64,
                filename: Some("brief.pdf".into()),
            },
        ],
    )];

    assert_eq!(
        SessionEngine::estimated_message_tokens(media_only.iter()),
        (MEDIA_BLOCK_ESTIMATED_TOKENS * 2)
            .saturating_add(estimate_text_tokens(&SessionMessageRole::User.to_string()))
    );
}

#[test]
fn text_blocks_keep_byte_based_estimation() {
    let text_only = [test_message(
        0,
        SessionMessageRole::User,
        vec![SessionContentBlock::text("abcdefgh")],
    )];
    assert_eq!(
        SessionEngine::estimated_message_tokens(text_only.iter()),
        estimate_text_tokens("abcdefgh")
            .saturating_add(estimate_text_tokens(&SessionMessageRole::User.to_string()))
    );
}

#[test]
fn recap_transcript_flattens_media_blocks_to_placeholders_without_base64() {
    let huge_base64 = "QUJD".repeat(10_000);
    let messages = [test_message(
        0,
        SessionMessageRole::User,
        vec![
            SessionContentBlock::text("看下附件"),
            SessionContentBlock::image("image/png", huge_base64.clone()),
            SessionContentBlock::Document {
                media_type: "application/pdf".into(),
                data: huge_base64.clone(),
                filename: Some("brief.pdf".into()),
            },
        ],
    )];

    let transcript = session_messages_to_turn_transcript(&messages);

    assert_eq!(transcript.len(), 1);
    let content = &transcript[0].content;
    assert!(content.contains("看下附件"));
    assert!(content.contains("[image attachment media_type=image/png"));
    assert!(content.contains("[document attachment media_type=application/pdf filename=brief.pdf"));
    assert!(!content.contains(&huge_base64));
}

#[test]
fn historical_provider_context_flattens_media_blocks_without_base64() {
    let huge_base64 = "QUJD".repeat(10_000);
    let messages = vec![test_message(
        0,
        SessionMessageRole::User,
        vec![
            SessionContentBlock::text("看下附件"),
            SessionContentBlock::image("image/png", huge_base64.clone()),
            SessionContentBlock::Document {
                media_type: "application/pdf".into(),
                data: huge_base64.clone(),
                filename: Some("brief.pdf".into()),
            },
        ],
    )];

    let turn_messages = session_messages_to_turn_messages(messages);
    let flattened = turn_messages[0]
        .content
        .iter()
        .map(|block| match block {
            SessionTurnContentBlock::Text { text } => text.as_str(),
            SessionTurnContentBlock::SkillInstructions { .. } => "",
            SessionTurnContentBlock::Image { .. }
            | SessionTurnContentBlock::Document { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => "",
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(turn_messages[0].content.iter().all(|block| !matches!(
        block,
        SessionTurnContentBlock::Image { .. } | SessionTurnContentBlock::Document { .. }
    )));
    assert!(flattened.contains("看下附件"));
    assert!(flattened.contains("[image attachment media_type=image/png"));
    assert!(flattened.contains("filename=brief.pdf"));
    assert!(!flattened.contains(&huge_base64));
}

#[test]
fn post_commit_cleanup_error_keeps_canonical_commit_classification() {
    let committed_error = anyhow::Error::new(SessionTurnCommittedPostCommitError {
        source: anyhow::anyhow!("clear active compaction failed"),
    });

    assert!(is_canonical_messages_committed_error(&committed_error));
    assert!(!is_canonical_messages_committed_error(&anyhow::anyhow!(
        "provider failed before commit"
    )));
}

#[test]
fn responses_history_preserves_uncompacted_media_and_replay() {
    let mut assistant = test_message(
        1,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("done")],
    );
    let replay_items = vec![json!({
        "type": "reasoning",
        "id": "rs_1",
        "encrypted_content": "opaque-value",
        "future_field": 7
    })];
    assistant.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        items: replay_items.clone(),
    });
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![
                SessionContentBlock::image("image/png", "IMAGE_BASE64"),
                SessionContentBlock::Document {
                    media_type: "application/pdf".into(),
                    data: "PDF_BASE64".into(),
                    filename: Some("brief.pdf".into()),
                },
            ],
        ),
        assistant,
    ];

    let history = session_messages_to_provider_turn_messages(
        messages,
        ProviderHistoryMediaPolicy::Preserve,
        Some(ProviderReplayProtocol::OpenAiResponses),
    );

    assert!(matches!(
        history[0].content[0],
        SessionTurnContentBlock::Image { .. }
    ));
    assert!(matches!(
        history[0].content[1],
        SessionTurnContentBlock::Document { .. }
    ));
    assert_eq!(
        history[1].provider_replay,
        Some(ProviderReplayState::OpenAiResponses {
            items: replay_items
        })
    );
}

#[test]
fn cross_protocol_history_drops_replay_before_budgeting_without_rewriting_session() {
    let mut assistant = test_message(
        1,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("visible answer")],
    );
    assistant.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        items: vec![json!({
            "type": "reasoning",
            "encrypted_content": "R".repeat(40_000)
        })],
    });

    let canonical_history = session_messages_to_provider_turn_messages(
        vec![assistant.clone()],
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    let responses_history = session_messages_to_provider_turn_messages(
        vec![assistant.clone()],
        ProviderHistoryMediaPolicy::Preserve,
        Some(ProviderReplayProtocol::OpenAiResponses),
    );

    assert_eq!(canonical_history[0].provider_replay, None);
    assert!(responses_history[0].provider_replay.is_some());
    assert!(assistant.provider_replay.is_some());
    assert!(
        estimate_session_turn_messages_tokens(&responses_history)
            > estimate_session_turn_messages_tokens(&canonical_history)
    );

    let persisted_messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("first request")],
        ),
        assistant.clone(),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("latest request")],
        ),
        test_message(
            3,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("latest answer")],
        ),
    ];
    let canonical_tail_tokens =
        estimated_session_message_tokens_projected(persisted_messages.iter(), None, None);
    let responses_tail_tokens = estimated_session_message_tokens_projected(
        persisted_messages.iter(),
        None,
        Some(ProviderReplayProtocol::OpenAiResponses),
    );
    assert!(responses_tail_tokens > canonical_tail_tokens);
    assert_eq!(
        select_compaction_summary_end_index(
            &persisted_messages,
            0,
            persisted_messages.len(),
            canonical_tail_tokens,
            2,
            4096,
            None,
        ),
        0
    );
    assert_eq!(
        select_compaction_summary_end_index(
            &persisted_messages,
            0,
            persisted_messages.len(),
            canonical_tail_tokens,
            2,
            4096,
            Some(ProviderReplayProtocol::OpenAiResponses),
        ),
        2
    );
    assert!(assistant.provider_replay.is_some());
}

#[test]
fn transcript_projection_drops_provider_replay() {
    let mut message = test_message(
        0,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("visible answer")],
    );
    message.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        items: vec![json!({
            "type": "reasoning",
            "encrypted_content": "must-not-leak"
        })],
    });

    let history = session_messages_to_turn_messages(vec![message.clone()]);
    let transcript = session_messages_to_turn_transcript(&[message]);

    assert_eq!(history[0].provider_replay, None);
    assert_eq!(transcript[0].content, "visible answer");
    assert!(!transcript[0].content.contains("must-not-leak"));
}

#[test]
fn compacted_prefix_drops_media_and_replay_while_suffix_preserves_them() {
    let mut prefix_assistant = test_message(
        1,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("old answer")],
    );
    prefix_assistant.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        items: vec![json!({"type":"reasoning","encrypted_content":"old-replay"})],
    });
    let mut suffix_assistant = test_message(
        3,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("new answer")],
    );
    suffix_assistant.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        items: vec![json!({"type":"reasoning","encrypted_content":"new-replay"})],
    });
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::image("image/png", "OLD_IMAGE")],
        ),
        prefix_assistant,
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::image("image/png", "NEW_IMAGE")],
        ),
        suffix_assistant,
    ];
    let state = SessionCompactionState::from_committed_summary(
        2,
        "old media turn summarized".into(),
        Utc::now(),
    );

    let projection = project_provider_context(
        "system",
        &state,
        &messages,
        Vec::new(),
        ActiveProjectionContext {
            turn_id: "turn_1",
            base_message_count: messages.len(),
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 0,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Preserve,
        Some(ProviderReplayProtocol::OpenAiResponses),
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();

    assert!(rendered.contains("old media turn summarized"));
    assert!(!rendered.contains("OLD_IMAGE"));
    assert!(!rendered.contains("old-replay"));
    assert!(rendered.contains("NEW_IMAGE"));
    assert!(rendered.contains("new-replay"));

    let canonical_projection = project_provider_context(
        "system",
        &state,
        &messages,
        Vec::new(),
        ActiveProjectionContext {
            turn_id: "turn_2",
            base_message_count: messages.len(),
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 0,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    let canonical_rendered = serde_json::to_string(&canonical_projection.messages).unwrap();
    assert!(!canonical_rendered.contains("new-replay"));
    assert!(!canonical_rendered.contains("NEW_IMAGE"));
    assert!(canonical_rendered.contains("image attachment media_type=image/png"));
}

#[test]
fn active_compaction_hash_includes_provider_replay() {
    let active = |encrypted_content: &str| {
        vec![
            SessionTurnMessage::user_text("run tool"),
            SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: Some(ProviderReplayState::OpenAiResponses {
                    items: vec![json!({
                        "type": "reasoning",
                        "encrypted_content": encrypted_content
                    })],
                }),
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "call_1".into(),
                    name: "file_read".into(),
                    input: json!({"path":"README.md"}),
                }],
            },
            SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "call_1".into(),
                    content: "done".into(),
                }],
            },
        ]
    };
    let first = active("first");
    let second = active("second");
    let segments = active_provider_safe_segments(&first);

    assert_ne!(
        active_segments_hash(&first, &segments).unwrap(),
        active_segments_hash(&second, &segments).unwrap()
    );
}

#[test]
fn persisted_compaction_estimate_counts_provider_replay() {
    let canonical = test_message(
        0,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("visible answer")],
    );
    let mut replay = canonical.clone();
    replay.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        items: vec![json!({
            "type": "reasoning",
            "encrypted_content": "R".repeat(4_000)
        })],
    });

    let canonical_tokens = estimated_session_message_tokens_projected(
        [&canonical],
        None,
        Some(ProviderReplayProtocol::OpenAiResponses),
    );
    let replay_tokens = estimated_session_message_tokens_projected(
        [&replay],
        None,
        Some(ProviderReplayProtocol::OpenAiResponses),
    );

    assert!(replay_tokens > canonical_tokens);
}
