//! SessionEngine 的单元测试集合。
//!
//! 这些测试原本内联在 `session_engine.rs`，迁移到独立文件仅为降低
//! facade 文件体积；测试模块路径、断言语义和 helper 可见性保持不变。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio_util::sync::CancellationToken;

use super::super::fs::{
    LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore, LocalFsReportedDisputeClaimSetStore,
};
use super::super::inbox::InboxJsonGenerator;
use super::super::maintainer_upload::{LocalFsMaintainerUploadQueue, PendingMaintainerUploads};
use super::super::runner::AgentRunner;
use super::{
    active_provider_safe_segments, active_segments_hash, append_acn_md,
    assistant_turn_end_text_after, auto_compact_should_trigger,
    auto_compact_trigger_threshold_tokens, auto_compact_trigger_tokens,
    build_memory_review_transcript, compacted_committed_summary_message,
    compacted_context_for_turn, compaction_tail_token_limit, compaction_transcript_projection,
    delegation_summary_projection, estimate_compacted_committed_summary_message_tokens,
    estimated_session_message_tokens_projected, finish_cancelled_turn_journal,
    hash_session_segment, is_canonical_messages_committed_error,
    journal_failure_overrides_turn_result, latest_model_context_matches,
    manual_pending_provider_turn, parse_compaction_summary_outcome,
    persist_main_background_process_completions, project_provider_context,
    provider_recovery_suffix, recovery_turn_chain, select_compaction_summary_end_index,
    session_compaction_transcript_projection,
    session_compaction_transcript_projection_with_memory_mode,
    session_messages_to_provider_turn_messages, session_messages_to_turn_messages,
    session_messages_to_turn_transcript, session_messages_to_turn_transcript_with_memory_mode,
    should_emit_compaction_retry_warning, spawn_turn_control_journal_forwarder,
    write_provider_rejection_recovery, ActiveProjectionContext, CompactionAuditScope,
    CompactionAuditSummaryContext, CompactionAuditTrigger, CompactionRanges,
    CompactionSummaryInputs, DelegationProjectionBaseline, MainModelContextAppender,
    ManualCompactionOutcome, PreflightCompactionRequest, PreflightCompactor,
    ProviderContextUsageAnchor, ProviderProjectionBudget, ProviderRejectionRecoveryRecord,
    SessionCompactionNoopReason, SessionCompactionResult, SessionEngine, SessionEvent,
    SessionFinalizeOnceOutcome, SessionFinalizePreemptionControl,
    SessionRecapBackgroundProcessProjection, SessionRecapPreemptionControl,
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
    InternalizeRequest, MemoryReviewLoop, ModelContextSource, ProviderAdapter, ProviderEvent,
    ProviderHistoryMediaPolicy, ProviderRejectedRequestRecovery, ProviderReplayIdentity,
    ProviderReplayProtocol, ProviderReplayState, ProviderRequest, ProviderRequestObserver,
    ProviderRequestRejected, ProviderResponse, ProviderStop, ProviderStreamFailure,
    SessionAttachment, SessionTurnContentBlock, SessionTurnContextAppender, SessionTurnEvent,
    SessionTurnHooks, SessionTurnMessage, SessionTurnPreflight, SessionTurnRequest,
    StructuredJsonCaller, ToolCallSkipReason, TurnMessage,
};
use crate::claim::{
    AgentId, Claim, ClaimId, ClaimStatus, Confidence, Dispute, DisputeId, InboxId, InboxMessage,
    InboxMessageKind, Policy, PolicyId, PolicyMessageType, PolicyStatus, SessionId,
};
use crate::config::{
    AgentSessionTurnJournalConfig, SessionCompactionConfig, ToolConfig, UserShellConfig,
};
use crate::delegation::{
    DelegationCreateRequest, DelegationExecutionContext, DelegationExecutionError,
    DelegationExecutionOutcome, DelegationExecutor, DelegationProgressSink, DelegationResult,
    DelegationRunnerConfig, DelegationStatus, DelegationStore, DelegationUpdate,
};
use crate::maintainer::traits::MaintainerClient;
use crate::prompt::PromptRegistry;
use crate::router::{AgentQuery, RouterClient, RouterQueryResult, ScopesOverviewSnapshot};
use crate::session::{
    canonical_user_content_hash, replay_turn_journal, ActiveTurnCompactionCursor,
    CompactedProviderHistory, CompactionCheckpoint, CompactionCheckpointStatus, FinalizeCheckpoint,
    FinalizeCheckpointStatus, NewSessionMessage, PendingProviderHistoryTurn,
    SessionCompactionState, SessionContentBlock, SessionMessage, SessionMessageRole,
    SessionMetadata, SessionStatus, SessionStore, TurnJournalEventKind, TurnJournalFlush,
    TurnJournalModelContext, TurnJournalNonStreamingFallbackState, TurnJournalProjection,
    TurnJournalStatus, TurnJournalTurn,
};
use crate::skill::{SkillInstructions, SkillSummary};
use crate::storage::{paths, write_yaml_atomic, FileLockGuard};
use crate::tool::{ProcessCompletion, ToolDispatchContext, ToolRegistry};
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
    ResponseAndSteerThenBreakJournal {
        response: ProviderResponse,
        events: Vec<ProviderEvent>,
        control: SessionTurnControl,
        journal_path: PathBuf,
    },
    ResponseAndPreservedSteer {
        response: ProviderResponse,
        events: Vec<ProviderEvent>,
        control: SessionTurnControl,
    },
    JsonByRequestKind {
        compaction_responses: VecDeque<ProviderResponse>,
        recap_responses: VecDeque<ProviderResponse>,
    },
    Error {
        message: &'static str,
        events: Vec<ProviderEvent>,
    },
    TerminalFailure {
        message: &'static str,
    },
    Rejected {
        message: &'static str,
    },
    ContextWindowRejected,
    MediaRejected,
    StreamFailure(&'static str),
    RequestTooLarge,
}

struct RecordingProvider {
    steps: Mutex<VecDeque<ProviderStep>>,
    requests: Mutex<Vec<ProviderRequest>>,
    history_media_policy: ProviderHistoryMediaPolicy,
}

struct InternalRetryThenRejectedProvider {
    calls: AtomicUsize,
    requests: Mutex<Vec<ProviderRequest>>,
    previous_attempt_ambiguous: bool,
}

struct AmbiguousRetryMediaThenRejectedProvider {
    calls: AtomicUsize,
    requests: Mutex<Vec<ProviderRequest>>,
}

struct AcceptedContinuationThenRejectedProvider {
    requests: Mutex<Vec<ProviderRequest>>,
    reject_media: bool,
}

impl InternalRetryThenRejectedProvider {
    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

impl AmbiguousRetryMediaThenRejectedProvider {
    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

impl AcceptedContinuationThenRejectedProvider {
    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

impl RecordingProvider {
    fn new(steps: Vec<ProviderStep>) -> Self {
        Self {
            steps: Mutex::new(VecDeque::from(steps)),
            requests: Mutex::new(Vec::new()),
            history_media_policy: ProviderHistoryMediaPolicy::Placeholder,
        }
    }

    fn with_history_media_policy(mut self, policy: ProviderHistoryMediaPolicy) -> Self {
        self.history_media_policy = policy;
        self
    }

    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ProviderAdapter for RecordingProvider {
    fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        self.history_media_policy
    }

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
                compaction_responses,
                recap_responses,
            }) = steps.front_mut()
            {
                let selected = if request_for_kind.system_prompt.contains("session 历史压缩")
                    || request_for_kind.system_prompt.contains("committed_summary")
                {
                    compaction_responses.pop_front()
                } else if request_for_kind.system_prompt.contains("复盘阶段")
                    || request_for_kind.system_prompt.contains("new_claims")
                {
                    recap_responses.pop_front()
                } else {
                    anyhow::bail!("recording provider could not classify JSON request")
                };
                if compaction_responses.is_empty() && recap_responses.is_empty() {
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
            Some(ProviderStep::ResponseAndSteerThenBreakJournal {
                response,
                events,
                control,
                journal_path,
            }) => {
                for event in events {
                    emit(event);
                }
                assert!(
                    control
                        .request_tool_boundary_steer("steer before journal failure")
                        .await
                );
                let saved_path = journal_path.with_extension("jsonl.before_terminal_failure");
                std::fs::rename(&journal_path, &saved_path)
                    .expect("fixture should move the journal before terminal append");
                std::fs::create_dir(&journal_path)
                    .expect("fixture should replace the journal with a directory");
                Ok(response)
            }
            Some(ProviderStep::ResponseAndPreservedSteer {
                response,
                events,
                control,
            }) => {
                for event in events {
                    emit(event);
                }
                assert!(
                    control
                        .request_tool_boundary_steer("steer after max-token partial")
                        .await
                );
                request_for_kind
                    .recovery_interrupt
                    .as_ref()
                    .expect("controlled turn must pass a recovery interrupt")
                    .preserve_successful_response();
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
            Some(ProviderStep::TerminalFailure { message }) => {
                Err(crate::api::ProviderTerminalFailure::new(message).into())
            }
            Some(ProviderStep::Rejected { message }) => {
                Err(crate::api::ProviderRequestRejected::new(message).into())
            }
            Some(ProviderStep::ContextWindowRejected) => {
                Err(crate::api::ProviderContextWindowExceeded::new().into())
            }
            Some(ProviderStep::MediaRejected) => {
                Err(crate::api::ProviderMediaRejected::new("provider rejected media input").into())
            }
            Some(ProviderStep::StreamFailure(message)) => {
                Err(ProviderStreamFailure::new(message).into())
            }
            Some(ProviderStep::RequestTooLarge) => {
                Err(crate::api::ProviderRequestTooLarge::new().into())
            }
            None => anyhow::bail!("recording provider response exhausted"),
        }
    }
}

#[async_trait]
impl ProviderAdapter for InternalRetryThenRejectedProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        _request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        anyhow::bail!("unexpected unobserved Provider request")
    }

    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        observer.before_provider_request(&request.messages).await?;
        observer.provider_request_started(&request.messages)?;
        self.requests.lock().await.push(request.clone());
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                observer.provider_request_started_after(
                    &request.messages,
                    self.previous_attempt_ambiguous,
                )?;
                if self.previous_attempt_ambiguous {
                    emit(ProviderEvent::AssistantTextDelta {
                        text: "rejected retry partial".into(),
                    });
                }
                self.requests.lock().await.push(request);
                Err(crate::api::ProviderRequestRejected::new(
                    "retry received deterministic rejection",
                )
                .into())
            }
            1 => Ok(provider_response("recovered ambiguous internal retry")),
            call => anyhow::bail!("unexpected internal retry provider call {call}"),
        }
    }
}

#[async_trait]
impl ProviderAdapter for AcceptedContinuationThenRejectedProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        _request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        anyhow::bail!("unexpected unobserved Provider request")
    }

    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        observer.before_provider_request(&request.messages).await?;
        observer.provider_request_started_after(&request.messages, true)?;
        if self.reject_media && self.requests.lock().await.len() == 2 {
            self.requests.lock().await.push(request);
            return Ok(provider_response("completed clean continuation"));
        }
        self.requests.lock().await.push(request.clone());
        emit(ProviderEvent::AssistantTextDelta {
            text: "kept prefix".into(),
        });
        observer
            .provider_response_accepted(&request.messages)
            .await?;

        let mut continuation_messages = request.messages.clone();
        continuation_messages.push(SessionTurnMessage::assistant_text("kept prefix"));
        continuation_messages.push(SessionTurnMessage::user_text("continue"));
        observer
            .before_provider_request(&continuation_messages)
            .await?;
        observer.provider_request_started(&continuation_messages)?;
        let mut continuation_request = request;
        continuation_request.messages = continuation_messages.clone();
        self.requests.lock().await.push(continuation_request);
        emit(ProviderEvent::AssistantTextDelta {
            text: "ghost suffix".into(),
        });
        observer.provider_request_outcome_resolved(&continuation_messages)?;
        if self.reject_media {
            Err(crate::api::ProviderRequestTooLarge::new().into())
        } else {
            Err(crate::api::ProviderRequestRejected::new("continuation rejected").into())
        }
    }
}

#[async_trait]
impl ProviderAdapter for AmbiguousRetryMediaThenRejectedProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        _request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        anyhow::bail!("unexpected unobserved Provider request")
    }

    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        observer.before_provider_request(&request.messages).await?;
        observer.provider_request_started(&request.messages)?;
        self.requests.lock().await.push(request.clone());
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                observer.provider_request_started_after(&request.messages, true)?;
                Err(crate::api::ProviderMediaRejected::new("retry rejected image input").into())
            }
            1 => Err(crate::api::ProviderRequestRejected::new(
                "cleaned retry received deterministic rejection",
            )
            .into()),
            2 => Ok(provider_response("recovered ambiguous media request")),
            call => anyhow::bail!("unexpected ambiguous media provider call {call}"),
        }
    }
}

struct FailingInternalContinuationProvider {
    calls: AtomicUsize,
    requests: Mutex<Vec<ProviderRequest>>,
    last_internal_request: Mutex<Option<Vec<SessionTurnMessage>>>,
}

struct InternalContinuationContextRecoveryProvider {
    main_calls: AtomicUsize,
    main_requests: Mutex<Vec<ProviderRequest>>,
    compaction_requests: Mutex<Vec<ProviderRequest>>,
    older_tool_payload: String,
}

impl InternalContinuationContextRecoveryProvider {
    fn new(older_tool_payload: String) -> Self {
        Self {
            main_calls: AtomicUsize::new(0),
            main_requests: Mutex::new(Vec::new()),
            compaction_requests: Mutex::new(Vec::new()),
            older_tool_payload,
        }
    }

    async fn main_requests(&self) -> Vec<ProviderRequest> {
        self.main_requests.lock().await.clone()
    }

    async fn compaction_requests(&self) -> Vec<ProviderRequest> {
        self.compaction_requests.lock().await.clone()
    }
}

impl FailingInternalContinuationProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            last_internal_request: Mutex::new(None),
        }
    }

    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ProviderAdapter for FailingInternalContinuationProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        Some(ProviderReplayIdentity {
            protocol: ProviderReplayProtocol::OpenAiResponses,
            model: "test-model".into(),
        })
    }

    async fn send(
        &self,
        _request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        anyhow::bail!("unexpected unobserved Provider request")
    }

    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        observer.before_provider_request(&request.messages).await?;
        observer.provider_request_started(&request.messages)?;
        self.requests.lock().await.push(request.clone());
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let partial = json!({
                    "type":"message",
                    "id":"msg_partial",
                    "role":"assistant",
                    "status":"incomplete",
                    "content":[{"type":"output_text","text":"partial before failure"}],
                });
                let continuation = json!({
                    "type":"message",
                    "role":"user",
                    "content":[{"type":"input_text","text":"continue exactly"}],
                });
                let mut internal_request = request.messages;
                internal_request.push(SessionTurnMessage {
                    role: "assistant".into(),
                    content: vec![SessionTurnContentBlock::text("partial before failure")],
                    provider_replay: Some(ProviderReplayState::OpenAiResponses {
                        model: Some("test-model".into()),
                        items: vec![partial, continuation],
                    }),
                });
                observer.before_provider_request(&internal_request).await?;
                observer.provider_request_started(&internal_request)?;
                *self.last_internal_request.lock().await = Some(internal_request);
                anyhow::bail!("internal continuation failed after request write-ahead")
            }
            1 => Ok(provider_response("recovered internal continuation")),
            call => anyhow::bail!("unexpected internal continuation provider call {call}"),
        }
    }
}

#[async_trait]
impl ProviderAdapter for InternalContinuationContextRecoveryProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        Some(ProviderReplayIdentity {
            protocol: ProviderReplayProtocol::AnthropicMessages,
            model: "test-model".into(),
        })
    }

    async fn send(
        &self,
        request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        if !request.system_prompt.contains("committed_summary") {
            anyhow::bail!("unexpected non-compaction request without observer")
        }
        self.compaction_requests.lock().await.push(request);
        Ok(provider_response(
            r#"{"committed_summary":null,"active_turn_summary":"The earlier working-note round completed before the unfinished response."}"#,
        ))
    }

    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        const TRIGGER: &str = "继续，从上一条回复被截断处继续，不要重复已写内容。";

        observer.before_provider_request(&request.messages).await?;
        self.main_requests.lock().await.push(request.clone());
        match self.main_calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_before_internal_context".into(),
                        name: "working_note".into(),
                        input: json!({
                            "action": "add",
                            "note": self.older_tool_payload.clone(),
                        }),
                    }],
                    provider_replay: None,
                },
                stop: ProviderStop::ToolUse,
            }),
            1 => {
                let max_token_partial = json!({
                    "role": "assistant",
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "private-max-token-partial",
                            "signature": "signature-max-token-partial"
                        },
                        {"type": "text", "text": "MAX-PARTIAL-"}
                    ]
                });
                let internal_trigger = json!({
                    "role": "user",
                    "content": [{"type": "text", "text": TRIGGER}]
                });
                let context_partial = json!({
                    "role": "assistant",
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "private-context-window-partial",
                            "signature": "signature-context-window-partial"
                        },
                        {"type": "text", "text": "CONTEXT-PARTIAL-"}
                    ]
                });
                let mut continued_messages = request.messages.clone();
                continued_messages.push(SessionTurnMessage {
                    role: "assistant".into(),
                    content: vec![SessionTurnContentBlock::text("MAX-PARTIAL-")],
                    provider_replay: Some(ProviderReplayState::AnthropicMessages {
                        model: "test-model".into(),
                        messages: vec![max_token_partial.clone(), internal_trigger.clone()],
                    }),
                });
                observer
                    .before_provider_request(&continued_messages)
                    .await?;
                let mut continued_request = request;
                continued_request.messages = continued_messages;
                self.main_requests.lock().await.push(continued_request);
                Ok(ProviderResponse {
                    assistant_message: SessionTurnMessage {
                        role: "assistant".into(),
                        content: vec![SessionTurnContentBlock::text(
                            "MAX-PARTIAL-CONTEXT-PARTIAL-",
                        )],
                        provider_replay: Some(ProviderReplayState::AnthropicMessages {
                            model: "test-model".into(),
                            messages: vec![max_token_partial, internal_trigger, context_partial],
                        }),
                    },
                    stop: ProviderStop::ContextWindowExceeded,
                })
            }
            2 => Ok(anthropic_response(
                "FINAL",
                ProviderStop::Done,
                "final-after-internal-context",
            )),
            call => anyhow::bail!("unexpected main Provider call {call}"),
        }
    }
}

struct BlockingAfterFileReadProvider {
    calls: AtomicUsize,
    second_call_started: Notify,
}

struct PreemptibleRecapProvider {
    calls: AtomicUsize,
    first_call_started: Notify,
    first_call_dropped: Arc<AtomicBool>,
    requests: Mutex<Vec<ProviderRequest>>,
}

struct FirstRecapCallGuard(Arc<AtomicBool>);

impl Drop for FirstRecapCallGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl PreemptibleRecapProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            first_call_started: Notify::new(),
            first_call_dropped: Arc::new(AtomicBool::new(false)),
            requests: Mutex::new(Vec::new()),
        }
    }

    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ProviderAdapter for PreemptibleRecapProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        self.requests.lock().await.push(request);
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let _guard = FirstRecapCallGuard(Arc::clone(&self.first_call_dropped));
                self.first_call_started.notify_one();
                std::future::pending::<anyhow::Result<ProviderResponse>>().await
            }
            1 => Ok(provider_response(
                r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            )),
            call => anyhow::bail!("unexpected preemptible recap provider call {call}"),
        }
    }
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

struct SummaryFailureWithRecapProvider {
    requests: Mutex<Vec<ProviderRequest>>,
}

impl SummaryFailureWithRecapProvider {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }

    async fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ProviderAdapter for SummaryFailureWithRecapProvider {
    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    async fn send(
        &self,
        request: ProviderRequest,
        _emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        self.requests.lock().await.push(request.clone());
        if request.system_prompt.contains("session 历史压缩")
            || request.system_prompt.contains("committed_summary")
        {
            anyhow::bail!("summary provider unavailable")
        }
        if request.system_prompt.contains("复盘阶段")
            || request.system_prompt.contains("new_claims")
        {
            return Ok(provider_response(
                r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            ));
        }
        Ok(provider_response("continued after provider failure"))
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
        _preferred_transport: Option<crate::api::ProviderTransport>,
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

struct PendingBeforeLedgerReportedDisputeStore {
    pending_path: PathBuf,
    inner: LocalFsReportedDisputeClaimSetStore,
}

impl PendingBeforeLedgerReportedDisputeStore {
    fn new(agent_home: PathBuf) -> Self {
        Self {
            pending_path: paths::agent_home_pending_maintainer_uploads_path(&agent_home),
            inner: LocalFsReportedDisputeClaimSetStore::new(agent_home),
        }
    }
}

#[async_trait]
impl ReportedDisputeClaimSetStore for PendingBeforeLedgerReportedDisputeStore {
    async fn contains_claim_set(&self, claims: &[ClaimId]) -> anyhow::Result<bool> {
        self.inner.contains_claim_set(claims).await
    }

    async fn record_claim_set(
        &self,
        claims: &[ClaimId],
        dispute_id: &DisputeId,
        reported_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let pending: PendingMaintainerUploads =
            crate::storage::read_yaml(&self.pending_path).await?;
        anyhow::ensure!(
            pending
                .disputes
                .iter()
                .any(|dispute| dispute.id == *dispute_id),
            "dispute ledger must be recorded only after durable pending staging"
        );
        self.inner
            .record_claim_set(claims, dispute_id, reported_at)
            .await
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

fn prepared_empty_finalize_checkpoint(
    recap_start_index: usize,
    recap_end_index: usize,
    recap_segment_hash: String,
) -> FinalizeCheckpoint {
    FinalizeCheckpoint {
        recap_start_index,
        recap_end_index,
        recap_segment_hash,
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: "prepared recap trace".into(),
        trace_created_at: Utc::now(),
        trace_id: None,
        status: FinalizeCheckpointStatus::Prepared,
    }
}

fn anthropic_response(text: &str, stop: ProviderStop, replay_marker: &str) -> ProviderResponse {
    let raw_content = vec![
        json!({
            "type": "thinking",
            "thinking": format!("private-{replay_marker}"),
            "signature": format!("signature-{replay_marker}"),
        }),
        json!({"type": "text", "text": text}),
    ];
    ProviderResponse {
        assistant_message: SessionTurnMessage {
            role: "assistant".into(),
            content: if text.is_empty() {
                Vec::new()
            } else {
                vec![SessionTurnContentBlock::text(text)]
            },
            provider_replay: Some(ProviderReplayState::AnthropicMessages {
                model: "test-model".into(),
                messages: vec![json!({"role": "assistant", "content": raw_content})],
            }),
        },
        stop,
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

fn json_by_request_kind_responses(compaction_texts: &[&str], recap_texts: &[&str]) -> ProviderStep {
    ProviderStep::JsonByRequestKind {
        compaction_responses: compaction_texts
            .iter()
            .map(|text| provider_response(text))
            .collect(),
        recap_responses: recap_texts
            .iter()
            .map(|text| provider_response(text))
            .collect(),
    }
}

fn error_step(message: &'static str, events: Vec<ProviderEvent>) -> ProviderStep {
    ProviderStep::Error { message, events }
}

fn request_too_large_step() -> ProviderStep {
    ProviderStep::RequestTooLarge
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
                    SessionTurnContentBlock::SkillInstructions { .. }
                    | SessionTurnContentBlock::ModelContext { .. } => None,
                    SessionTurnContentBlock::Image { .. }
                    | SessionTurnContentBlock::Document { .. }
                    | SessionTurnContentBlock::ToolUse { .. }
                    | SessionTurnContentBlock::InvalidToolUse { .. }
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
            SessionContentBlock::SkillInstructions { .. }
            | SessionContentBlock::ModelContext { .. } => None,
            SessionContentBlock::Image { .. }
            | SessionContentBlock::Document { .. }
            | SessionContentBlock::ToolUse { .. }
            | SessionContentBlock::InvalidToolUse { .. }
            | SessionContentBlock::ToolResult { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn non_context_session_messages(messages: &[SessionMessage]) -> Vec<&SessionMessage> {
    messages
        .iter()
        .filter(|message| {
            !message
                .content
                .iter()
                .any(|block| matches!(block, SessionContentBlock::ModelContext { .. }))
        })
        .collect()
}

fn first_real_provider_user_message(request: &ProviderRequest) -> &SessionTurnMessage {
    request
        .messages
        .iter()
        .find(|message| {
            message.role == "user"
                && message.model_context_snapshot().is_none()
                && !message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionTurnContentBlock::ToolResult { .. }))
        })
        .expect("provider request must contain a real user message")
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

#[tokio::test(start_paused = true)]
async fn cancelled_turn_journal_uses_fixed_durability_timeout_when_writer_stalls() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let emitter = TurnJournalEmitter::new(tx, Duration::from_secs(3600), usize::MAX);
    let writer = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });

    let error = finish_cancelled_turn_journal(emitter, writer, None)
        .await
        .expect_err("stalled cancellation journal must fail the current run");
    assert!(error
        .to_string()
        .contains("cancelled turn journal durability exceeded 10s"));
}

#[tokio::test]
async fn cancelled_turn_waits_past_old_grace_and_records_terminal_marker() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let emitter = TurnJournalEmitter::new(tx, Duration::from_secs(3600), usize::MAX);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut terminal_tx = Some(terminal_tx);
        while let Some(command) = rx.recv().await {
            if matches!(
                command.kind,
                TurnJournalEventKind::TurnFinished {
                    status: TurnJournalStatus::Cancelled
                }
            ) {
                if let Some(terminal_tx) = terminal_tx.take() {
                    let _ = terminal_tx.send(());
                }
            }
        }
        Ok(())
    });

    finish_cancelled_turn_journal(emitter, writer, None)
        .await
        .expect("ordinary writer contention should not drop cancellation journal");
    terminal_rx
        .await
        .expect("cancelled terminal marker should reach the writer");
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
    let reported_store: Arc<dyn ReportedDisputeClaimSetStore> =
        Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home));
    build_test_engine_with_team_mode_and_reported_store(
        dir,
        provider,
        tools,
        available_skills,
        team_services_configured,
        reported_store,
    )
}

fn build_test_engine_with_team_mode_and_reported_store(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
    tools: Arc<ToolRegistry>,
    available_skills: Vec<SkillSummary>,
    team_services_configured: bool,
    reported_store: Arc<dyn ReportedDisputeClaimSetStore>,
) -> (SessionEngine, SessionStore) {
    let agent = AgentId::new("agent-a").unwrap();
    let agents_root = dir.path().join("agents");
    let agent_home = agents_root.join(agent.as_str());
    let claim_store: Arc<dyn LocalClaimStore> =
        Arc::new(LocalFsClaimStore::new(agent_home.clone()));
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
        0,
        Duration::ZERO,
        Duration::ZERO,
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

fn build_test_engine_with_reported_store(
    dir: &tempfile::TempDir,
    provider: Arc<dyn ProviderAdapter>,
    reported_store: Arc<dyn ReportedDisputeClaimSetStore>,
) -> (SessionEngine, SessionStore) {
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    build_test_engine_with_team_mode_and_reported_store(
        dir,
        provider,
        tools,
        Vec::new(),
        true,
        reported_store,
    )
}

async fn create_test_session(store: &SessionStore, id: &str) -> crate::session::SessionHandle {
    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = id.parse().unwrap();
    store
        .create_with_id_factory(&agent, "system prompt", || session_id.clone(), 1)
        .await
        .unwrap()
}

fn active_policy_inbox_message() -> InboxMessage {
    InboxMessage {
        id: InboxId::random(),
        kind: InboxMessageKind::PolicyUpdate {
            policy: Policy {
                id: PolicyId::random(),
                message_type: PolicyMessageType::PolicyUpdate,
                name: "startup-inbox-policy".into(),
                statement: "internalize this policy".into(),
                scope: "tests / startup inbox".into(),
                status: PolicyStatus::Active,
                created_at: Utc::now(),
                updated_at: None,
                target_agents: None,
            },
        },
        handled_at: None,
    }
}

#[tokio::test]
#[cfg(unix)]
async fn finalize_journals_queued_and_live_background_process_completions() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_ba11c001").await;
    let tools = engine.turn_loop.tool_registry();
    let turn_id = "turn_1";
    let user_content = vec![SessionContentBlock::text("run two background jobs")];
    let canonical_hash = canonical_user_content_hash(&user_content).unwrap();
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                user_content,
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("both jobs are running")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();

    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::UserInputAccepted {
                text: "run two background jobs".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::CanonicalUserMessage {
                content_hash: Some(canonical_hash),
                content: None,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    for tool_use_id in ["toolu_finished", "toolu_live"] {
        writer
            .append(
                turn_id,
                Utc::now(),
                TurnJournalEventKind::ToolCallStarted {
                    tool_use_id: tool_use_id.into(),
                    name: "code_run".into(),
                    summary: "tool code_run".into(),
                    input_preview: String::new(),
                    input_truncated: false,
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();
        writer
            .append(
                turn_id,
                Utc::now(),
                TurnJournalEventKind::ToolCallCompleted {
                    tool_use_id: tool_use_id.into(),
                    summary: "tool code_run process_running".into(),
                    outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
                    output_preview: String::new(),
                    output_truncated: false,
                    file_change: None,
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();
    }
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::TurnFinished {
                status: TurnJournalStatus::Committed,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);

    let context = |tool_use_id: &str| ToolDispatchContext {
        current_session_id: Some(session.metadata.id.clone()),
        current_turn_id: Some(turn_id.into()),
        tool_use_id: Some(tool_use_id.into()),
        ..ToolDispatchContext::default()
    };
    let finished = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 1", "yield_time_ms": 50}),
            context("toolu_finished"),
        )
        .await
        .unwrap();
    assert_eq!(
        finished.outcome,
        crate::api::ToolExecutionOutcome::ProcessRunning
    );
    let live = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 30", "yield_time_ms": 50}),
            context("toolu_live"),
        )
        .await
        .unwrap();
    assert_eq!(
        live.outcome,
        crate::api::ToolExecutionOutcome::ProcessRunning
    );

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if tools
                .pending_process_completions_for_root_session(&session.metadata.id)
                .await
                .iter()
                .any(|completion| {
                    completion.originating_tool_use_id.as_deref() == Some("toolu_finished")
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("自然完成的进程必须先进入 root completion 队列");

    let mut completion_events = Vec::new();
    engine
        .finalize_session(&mut session, |event| completion_events.push(event))
        .await
        .unwrap();
    assert_eq!(
        completion_events
            .iter()
            .filter(|event| matches!(event, SessionEvent::BackgroundProcessCompleted { .. }))
            .count(),
        2
    );

    let read = session.read_turn_journal().await;
    let completions = read
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id,
                exit_code,
                signal,
                success,
                ..
            } => Some((tool_use_id.as_str(), *exit_code, *signal, *success)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), 2);
    assert!(completions.contains(&("toolu_finished", Some(0), None, true)));
    assert!(completions.contains(&("toolu_live", None, Some(libc::SIGKILL), false)));
    assert!(tools
        .pending_process_completions_for_root_session(&session.metadata.id)
        .await
        .is_empty());
    assert!(tools
        .process_snapshots_for_root_session(&session.metadata.id)
        .await
        .is_empty());
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let recap_payload = last_user_text(&requests[0]);
    assert!(recap_payload.contains(r#""background_process_completions""#));
    assert!(recap_payload.contains(r#""tool_use_id": "toolu_finished""#));
    assert!(recap_payload.contains(r#""exit_code": 0"#));
    assert!(recap_payload.contains(r#""tool_use_id": "toolu_live""#));
    assert!(recap_payload.contains(r#""signal": 9"#));
}

#[tokio::test]
#[cfg(unix)]
async fn mark_finalizing_emits_durable_completion_before_later_delegation_failure() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine_with_delegation_host(&dir, provider);
    let mut session = create_test_session(&store, "session_ba11c004").await;
    let tools = engine.turn_loop.tool_registry();
    let turn_id = "turn_1";
    let tool_use_id = "toolu_emit_before_failure";

    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::ToolCallStarted {
                tool_use_id: tool_use_id.into(),
                name: "code_run".into(),
                summary: "tool code_run".into(),
                input_preview: String::new(),
                input_truncated: false,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::ToolCallCompleted {
                tool_use_id: tool_use_id.into(),
                summary: "tool code_run process_running".into(),
                outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
                output_preview: String::new(),
                output_truncated: false,
                file_change: None,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);

    let result = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 30", "yield_time_ms": 50}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                current_turn_id: Some(turn_id.into()),
                tool_use_id: Some(tool_use_id.into()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        result.outcome,
        crate::api::ToolExecutionOutcome::ProcessRunning
    );

    let delegation_store =
        DelegationStore::new_for_session(session.paths.dir.clone(), session.metadata.id.clone());
    let corrupt_dir = delegation_store
        .delegations_dir()
        .join("subagent_badbadbad");
    tokio::fs::create_dir_all(&corrupt_dir).await.unwrap();
    tokio::fs::write(corrupt_dir.join("delegation.yaml"), "{not yaml")
        .await
        .unwrap();

    let mut completion_events = Vec::new();
    let mut emit = |event| completion_events.push(event);
    let error = engine
        .mark_session_finalizing(&mut session, &mut emit)
        .await
        .expect_err("delegation cleanup failure should abort finalization");
    assert!(error.to_string().contains("subagent"));
    assert!(completion_events.iter().any(|event| matches!(
        event,
        SessionEvent::BackgroundProcessCompleted {
            originating_tool_use_id: Some(completed_tool_use_id),
            signal: Some(signal),
            ..
        } if completed_tool_use_id == tool_use_id && *signal == libc::SIGKILL
    )));

    let read = session.read_turn_journal().await;
    assert_eq!(
        read.events
            .iter()
            .filter(|event| matches!(
                event.kind,
                TurnJournalEventKind::BackgroundProcessCompleted { .. }
            ))
            .count(),
        1
    );
    assert!(tools
        .pending_process_completions_for_root_session(&session.metadata.id)
        .await
        .is_empty());
}

#[tokio::test]
#[cfg(unix)]
async fn failed_background_completion_journal_append_retries_on_next_drain() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_ba11c002").await;
    let tools = engine.turn_loop.tool_registry();
    let turn_id = "turn_1";
    let tool_use_id = "toolu_retry";

    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::ToolCallStarted {
                tool_use_id: tool_use_id.into(),
                name: "code_run".into(),
                summary: "tool code_run".into(),
                input_preview: String::new(),
                input_truncated: false,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            turn_id,
            Utc::now(),
            TurnJournalEventKind::ToolCallCompleted {
                tool_use_id: tool_use_id.into(),
                summary: "tool code_run process_running".into(),
                outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
                output_preview: String::new(),
                output_truncated: false,
                file_change: None,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);

    let result = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 1", "yield_time_ms": 50}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                current_turn_id: Some(turn_id.into()),
                tool_use_id: Some(tool_use_id.into()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        result.outcome,
        crate::api::ToolExecutionOutcome::ProcessRunning
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !tools
                .pending_process_completions_for_root_session(&session.metadata.id)
                .await
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("watcher 必须登记 completion");

    let journal_path = session.paths.turn_events_jsonl.clone();
    let backup_path = session.paths.dir.join("turn_events.backup.jsonl");
    tokio::fs::rename(&journal_path, &backup_path)
        .await
        .unwrap();
    tokio::fs::create_dir(&journal_path).await.unwrap();

    let first_events = engine.drain_background_process_completions(&session).await;
    assert!(!first_events
        .iter()
        .any(|event| matches!(event, SessionEvent::BackgroundProcessCompleted { .. })));
    assert_eq!(
        tools
            .pending_process_completions_for_root_session(&session.metadata.id)
            .await
            .len(),
        1
    );

    tokio::fs::remove_dir(&journal_path).await.unwrap();
    tokio::fs::rename(&backup_path, &journal_path)
        .await
        .unwrap();
    let second_events = engine.drain_background_process_completions(&session).await;
    assert_eq!(
        second_events
            .iter()
            .filter(|event| matches!(event, SessionEvent::BackgroundProcessCompleted { .. }))
            .count(),
        1
    );
    assert!(tools
        .pending_process_completions_for_root_session(&session.metadata.id)
        .await
        .is_empty());
    let read = session.read_turn_journal().await;
    assert_eq!(
        read.events
            .iter()
            .filter(|event| matches!(
                event.kind,
                TurnJournalEventKind::BackgroundProcessCompleted { .. }
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_completion_persistence_assigns_one_journal_event() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_ba11c006").await;
    let tools = engine.turn_loop.tool_registry();
    let completion = ProcessCompletion {
        root_session_id: session.metadata.id.to_string(),
        owner: tools.process_owner_for_session(&session.metadata.id, None),
        process_id: "process-concurrent".into(),
        originating_turn_id: Some("turn-concurrent".into()),
        originating_tool_use_id: Some("tool-concurrent".into()),
        instance_id: 77,
        status: "finished".into(),
        exit_code: Some(0),
        signal: None,
        success: true,
        finished_at: std::time::SystemTime::now(),
        elapsed_minutes: 0,
    };
    let first_tools = Arc::clone(&tools);
    let second_tools = Arc::clone(&tools);
    let first_session_id = session.metadata.id.clone();
    let second_session_id = session.metadata.id.clone();
    let first_dir = session.paths.dir.clone();
    let second_dir = session.paths.dir.clone();
    let first_completion = completion.clone();
    let second_completion = completion;

    let (first, second) = tokio::join!(
        persist_main_background_process_completions(
            first_tools.as_ref(),
            &first_session_id,
            &first_dir,
            std::slice::from_ref(&first_completion),
        ),
        persist_main_background_process_completions(
            second_tools.as_ref(),
            &second_session_id,
            &second_dir,
            std::slice::from_ref(&second_completion),
        ),
    );
    first.unwrap();
    second.unwrap();

    let journal = session.read_turn_journal().await;
    assert_eq!(
        journal
            .events
            .iter()
            .filter(|event| matches!(
                event.kind,
                TurnJournalEventKind::BackgroundProcessCompleted {
                    instance_id: 77,
                    ..
                }
            ))
            .count(),
        1
    );
}

#[tokio::test]
#[cfg(unix)]
async fn new_turn_persists_pending_background_completion_before_recovery_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "continued",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "continued".into(),
        }],
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_ba11c003").await;
    let tools = engine.turn_loop.tool_registry();
    let turn_id = "turn_1";
    let tool_use_id = "toolu_pending_completion";

    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "start background work".into(),
        },
        TurnJournalEventKind::ToolCallStarted {
            tool_use_id: tool_use_id.into(),
            name: "code_run".into(),
            summary: "tool code_run".into(),
            input_preview: String::new(),
            input_truncated: false,
        },
        TurnJournalEventKind::ToolCallCompleted {
            tool_use_id: tool_use_id.into(),
            summary: "tool code_run process_running".into(),
            outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
            output_preview: String::new(),
            output_truncated: false,
            file_change: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::InterruptedByUser,
        },
    ] {
        writer
            .append(turn_id, Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    let initial = tools
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 1", "yield_time_ms": 50}),
            ToolDispatchContext {
                current_session_id: Some(session.metadata.id.clone()),
                current_turn_id: Some(turn_id.into()),
                tool_use_id: Some(tool_use_id.into()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        initial.outcome,
        crate::api::ToolExecutionOutcome::ProcessRunning
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !tools
                .pending_process_completions_for_root_session(&session.metadata.id)
                .await
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("watcher completion must be pending before the next turn starts");

    let mut events = Vec::new();
    engine
        .run_turn(&mut session, "continue now", |event| events.push(event))
        .await
        .unwrap();

    let completion_index = events
        .iter()
        .position(|event| matches!(event, SessionEvent::BackgroundProcessCompleted { .. }))
        .expect("pending completion must be emitted to the TUI");
    let turn_started_index = events
        .iter()
        .position(
            |event| matches!(event, SessionEvent::TurnStarted { turn_id } if turn_id == "turn_2"),
        )
        .expect("the next turn must start");
    assert!(completion_index < turn_started_index);
    assert!(tools
        .pending_process_completions_for_root_session(&session.metadata.id)
        .await
        .is_empty());

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let recovery_user = last_user_text(&requests[0]);
    assert!(recovery_user.contains("<interrupted_turn_context>"));
    assert!(recovery_user.contains(r#""background_completion":{"exit_code":0"#));
    assert!(recovery_user.contains(r#""status":"finished""#));

    let journal = session.read_turn_journal().await;
    let completion_seq = journal
        .events
        .iter()
        .find(|event| {
            matches!(
                &event.kind,
                TurnJournalEventKind::BackgroundProcessCompleted { tool_use_id: id, .. }
                    if id == tool_use_id
            )
        })
        .map(|event| event.seq)
        .expect("completion must be durable");
    let next_turn_seq = journal
        .events
        .iter()
        .find(|event| {
            event.turn_id == "turn_2" && matches!(event.kind, TurnJournalEventKind::TurnStarted)
        })
        .map(|event| event.seq)
        .expect("next turn start must be durable");
    assert!(completion_seq < next_turn_seq);
}

#[tokio::test]
async fn resumed_runtime_delivers_interrupted_turn_background_completion_once() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step("after resume", Vec::new()),
        response_step("after second turn", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_ba11c005").await;
    let committed_content = vec![SessionContentBlock::text("later committed turn")];
    let canonical_hash = canonical_user_content_hash(&committed_content).unwrap();
    session
        .append_messages(&[
            NewSessionMessage::new(SessionMessageRole::User, committed_content),
            NewSessionMessage::text(SessionMessageRole::Assistant, "later turn completed"),
        ])
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::ToolCallStarted {
            tool_use_id: "toolu_resume_completion".into(),
            name: "code_run".into(),
            summary: "tool code_run".into(),
            input_preview: String::new(),
            input_truncated: false,
        },
        TurnJournalEventKind::ToolCallCompleted {
            tool_use_id: "toolu_resume_completion".into(),
            summary: "tool code_run process_running".into(),
            outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
            output_preview: String::new(),
            output_truncated: false,
            file_change: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::InterruptedByUser,
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::CanonicalUserMessage {
            content_hash: Some(canonical_hash),
            content: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::Committed,
        },
    ] {
        writer
            .append("turn_2", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_resume_completion".into(),
                process_id: "resume12".into(),
                instance_id: 12,
                status: "finished".into(),
                exit_code: Some(0),
                signal: None,
                success: true,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let completion_seq = session
        .read_turn_journal()
        .await
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TurnJournalEventKind::BackgroundProcessCompleted { .. }
            )
        })
        .map(|event| event.seq)
        .max()
        .unwrap();
    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    session.mark_closed(Utc::now()).await.unwrap();
    drop(session);
    drop(engine);

    let (engine, _store) = build_test_engine(&dir, provider.clone());
    let mut session = engine.reopen_existing_session(&session_id).await.unwrap();
    assert_eq!(session.metadata.agent_id, agent_id);
    engine
        .run_turn(&mut session, "continue after restart", |_| {})
        .await
        .unwrap();

    let first_request = &provider.requests().await[0];
    let first_rendered = serde_json::to_string(&first_request.messages).unwrap();
    assert_eq!(first_rendered.matches("resume12").count(), 1);
    assert!(session
        .read_metadata()
        .await
        .unwrap()
        .provider_background_completion_until_seq
        .is_some_and(|seq| seq >= completion_seq));

    engine
        .run_turn(&mut session, "continue once more", |_| {})
        .await
        .unwrap();
    let requests = provider.requests().await;
    let second_rendered = serde_json::to_string(&requests[1].messages).unwrap();
    assert_eq!(second_rendered.matches("resume12").count(), 1);
    assert!(requests[1].messages.starts_with(&requests[0].messages));
}

#[tokio::test]
async fn manual_inbox_processes_local_pending_in_solo_mode() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"updated_claims":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_local_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_1234abcd").await;
    let inbox = LocalFsInboxReader::new(dir.path().join("agents").join("agent-a"));
    inbox
        .accept_pulled(&active_policy_inbox_message())
        .await
        .unwrap();
    let mut events = Vec::new();

    let report = engine
        .process_inbox_during_session(&session, |event| events.push(event))
        .await
        .unwrap();

    assert_eq!(report.total, 1);
    assert_eq!(report.team_services, Default::default());
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::InboxStarted)));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::InboxFailed { .. }
            | SessionEvent::StatusChanged {
                status: SessionRuntimeStatus::Error
            }
    )));
    assert!(matches!(
        events.last(),
        Some(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Open
        })
    ));
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
async fn fresh_start_continues_open_when_inbox_internalization_fails() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "not valid inbox json",
        Vec::new(),
    )]));
    let (engine, _) = build_test_engine(&dir, provider);
    let inbox = LocalFsInboxReader::new(dir.path().join("agents").join("agent-a"));
    let message = active_policy_inbox_message();
    inbox.accept_pulled(&message).await.unwrap();
    let mut events = Vec::new();

    let report = engine
        .start_session(1, |event| events.push(event))
        .await
        .expect("inbox internalization failure must not block session startup");

    assert_eq!(report.inbox_report.total, 0);
    assert!(report
        .inbox_report
        .failures
        .iter()
        .any(|failure| failure.kind == crate::agent::InboxProcessFailureKind::Internalization));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::Warning { message }
            if message == super::events::INBOX_INTERNALIZATION_WARNING
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::InboxFailed { .. }
            | SessionEvent::StatusChanged {
                status: SessionRuntimeStatus::Error
            }
    )));
    assert!(matches!(
        events.last(),
        Some(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Open
        })
    ));
    assert_eq!(
        report.session.read_metadata().await.unwrap().status,
        SessionStatus::Open
    );
    assert_eq!(inbox.list_pending().await.unwrap(), vec![message]);
}

#[tokio::test]
async fn fresh_start_continues_open_when_local_inbox_read_fails() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, _) = build_test_engine(&dir, provider);
    let agent_home = dir.path().join("agents").join("agent-a");
    tokio::fs::create_dir_all(&agent_home).await.unwrap();
    tokio::fs::write(paths::agent_home_inbox_dir(&agent_home), b"not a directory")
        .await
        .unwrap();
    let mut events = Vec::new();

    let report = engine
        .start_session(1, |event| events.push(event))
        .await
        .expect("local inbox failure must not block session startup");

    assert!(report
        .inbox_report
        .failures
        .iter()
        .any(|failure| failure.kind == crate::agent::InboxProcessFailureKind::Local));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::InboxFailed { error }
            if error.contains("Some local changes may already have been applied")
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Error
        }
    )));
    assert!(matches!(
        events.last(),
        Some(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Open
        })
    ));
    assert_eq!(
        report.session.read_metadata().await.unwrap().status,
        SessionStatus::Open
    );
}

#[tokio::test]
async fn manual_inbox_internalization_failure_warns_and_returns_to_open() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "not valid inbox json",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_1234abcd").await;
    let inbox = LocalFsInboxReader::new(dir.path().join("agents").join("agent-a"));
    inbox
        .accept_pulled(&active_policy_inbox_message())
        .await
        .unwrap();
    let mut events = Vec::new();

    let report = engine
        .process_inbox_during_session(&session, |event| events.push(event))
        .await
        .expect("manual inbox internalization failure must be recoverable");

    assert!(report
        .failures
        .iter()
        .any(|failure| failure.kind == crate::agent::InboxProcessFailureKind::Internalization));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::Warning { message }
            if message == super::events::INBOX_INTERNALIZATION_WARNING
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Error
        }
    )));
    assert!(matches!(
        events.last(),
        Some(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Open
        })
    ));
    assert_eq!(
        session.read_metadata().await.unwrap().status,
        SessionStatus::Open
    );
}

#[tokio::test]
async fn resume_inbox_refresh_reports_configured_team_status() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_1234abcd").await;

    let mut events = Vec::new();
    let report = engine
        .process_inbox_for_resume(&session, |event| events.push(event))
        .await;

    assert_eq!(
        report.team_services,
        crate::agent::TeamServicesConnectionStatus {
            maintainer: crate::agent::TeamServiceConnectionStatus::Connected,
            router: crate::agent::TeamServiceConnectionStatus::Connected,
        }
    );
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::TeamServicesConnectionUpdated { status }
            if *status == report.team_services
    )));
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

fn new_model_context_message(
    source: ModelContextSource,
    fingerprint: &str,
    text: impl Into<String>,
) -> NewSessionMessage {
    NewSessionMessage::new(
        SessionMessageRole::User,
        vec![SessionContentBlock::ModelContext {
            source,
            fingerprint: fingerprint.into(),
            text: text.into(),
        }],
    )
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
async fn resume_keeps_frozen_system_prompt_when_file_edit_authority_changes() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "continued",
        Vec::new(),
    )]));
    let mut tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    tool_config.file_edit_authority_enabled = false;
    let tools = Arc::new(ToolRegistry::new(&tool_config).unwrap());
    let (engine, store) = build_test_engine_with_tools(&dir, provider.clone(), tools);
    let inbox_report = crate::agent::InboxProcessReport::default();
    let current_prompt = engine
        .render_session_system_prompt_for_inbox(&inbox_report)
        .await
        .unwrap();
    assert!(!current_prompt.contains("required_read"));

    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = "session_a11ce001".parse().unwrap();
    let frozen_prompt = "frozen system prompt: follow required_read from the original runtime";
    let mut session = store
        .create_with_id_factory(&agent, frozen_prompt, || session_id.clone(), 1)
        .await
        .unwrap();
    session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            "previous request",
        )])
        .await
        .unwrap();
    session.mark_closed(Utc::now()).await.unwrap();
    drop(session);

    let mut resumed = engine.reopen_existing_session(&session_id).await.unwrap();
    engine
        .run_turn(&mut resumed, "continue", |_| {})
        .await
        .unwrap();

    assert_eq!(
        tokio::fs::read_to_string(&resumed.paths.system_prompt)
            .await
            .unwrap(),
        frozen_prompt
    );
    assert_eq!(provider.requests().await[0].system_prompt, frozen_prompt);
}

#[tokio::test]
async fn disabled_memory_omits_new_prompt_and_tools_but_resume_keeps_frozen_system_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "continued",
        Vec::new(),
    )]));
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(
        ToolRegistry::new(&tool_config)
            .unwrap()
            .with_memory_enabled(false),
    );
    let (engine, store) = build_test_engine_with_tools(&dir, provider.clone(), tools);
    let memory_dir = dir.path().join("agents/agent-a/memories");
    tokio::fs::create_dir_all(&memory_dir).await.unwrap();
    tokio::fs::write(memory_dir.join("MEMORY.md"), "PRIVATE_MEMORY_MARKER")
        .await
        .unwrap();
    let current_prompt = engine
        .render_session_system_prompt_for_inbox(&crate::agent::InboxProcessReport::default())
        .await
        .unwrap();
    assert!(!current_prompt.to_ascii_lowercase().contains("memory"));
    assert!(!current_prompt.contains("PRIVATE_MEMORY_MARKER"));

    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = "session_a11ce002".parse().unwrap();
    let frozen_prompt = "frozen system prompt with old memory instructions";
    let mut session = store
        .create_with_id_factory(&agent, frozen_prompt, || session_id.clone(), 1)
        .await
        .unwrap();
    session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            "previous request",
        )])
        .await
        .unwrap();
    session.mark_closed(Utc::now()).await.unwrap();
    drop(session);

    let mut resumed = engine.reopen_existing_session(&session_id).await.unwrap();
    engine
        .run_turn(&mut resumed, "continue", |_| {})
        .await
        .unwrap();

    assert_eq!(
        tokio::fs::read_to_string(&resumed.paths.system_prompt)
            .await
            .unwrap(),
        frozen_prompt
    );
    let requests = provider.requests().await;
    assert_eq!(requests[0].system_prompt, frozen_prompt);
    assert!(!requests[0].tools.iter().any(|tool| tool.name == "memory"));
}

#[test]
fn disabled_memory_is_a_hard_stop_for_background_review_cadence() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let tool_config = ToolConfig {
        workspace_root: dir.path().to_path_buf(),
        ..Default::default()
    };
    let tools = Arc::new(
        ToolRegistry::new(&tool_config)
            .unwrap()
            .with_memory_enabled(false),
    );
    let (engine, _) = build_test_engine_with_tools(&dir, provider, tools);
    let engine = engine
        .with_fork_memory_review(true)
        .with_fork_memory_review_interval_turns(1);

    assert!(!engine.fork_memory_review_cadence_reached());
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
    let conversation = non_context_session_messages(&messages);
    assert_eq!(conversation.len(), 2);
    assert_eq!(text_content(conversation[0]), "hello user");
    assert_eq!(text_content(conversation[1]), "assistant done");
    let expected_hash = canonical_user_content_hash(&conversation[0].content).unwrap();
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
async fn resumed_session_appends_runtime_context_only_after_workspace_change() {
    let dir = tempfile::tempdir().unwrap();
    let first_workspace = dir.path().join("workspace-a");
    let second_workspace = dir.path().join("workspace-b");
    tokio::fs::create_dir_all(&first_workspace).await.unwrap();
    tokio::fs::create_dir_all(&second_workspace).await.unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step("first", Vec::new()),
        response_step("changed", Vec::new()),
        response_step("same", Vec::new()),
    ]));
    let first_tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: first_workspace.clone(),
            ..Default::default()
        })
        .unwrap(),
    );
    let (first_engine, store) = build_test_engine_with_tools(&dir, provider.clone(), first_tools);
    let mut session = create_test_session(&store, "session_c0d0feed").await;

    first_engine
        .run_turn(&mut session, "first workspace", |_| {})
        .await
        .unwrap();
    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);

    let second_tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: second_workspace.clone(),
            ..Default::default()
        })
        .unwrap(),
    );
    let (second_engine, second_store) =
        build_test_engine_with_tools(&dir, provider.clone(), second_tools);
    let mut resumed = second_store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    second_engine
        .run_turn(&mut resumed, "changed workspace", |_| {})
        .await
        .unwrap();
    second_engine
        .run_turn(&mut resumed, "same workspace again", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let runtime_texts = |request: &ProviderRequest| {
        request
            .messages
            .iter()
            .filter_map(SessionTurnMessage::model_context_snapshot)
            .filter(|(source, _, _)| **source == ModelContextSource::Runtime)
            .map(|(_, _, text)| text.to_string())
            .collect::<Vec<_>>()
    };
    let first_runtime = runtime_texts(&requests[0]);
    let changed_runtime = runtime_texts(&requests[1]);
    let same_runtime = runtime_texts(&requests[2]);
    assert_eq!(first_runtime.len(), 1);
    assert_eq!(changed_runtime.len(), 2);
    assert_eq!(same_runtime.len(), 2);
    assert!(first_runtime[0].contains(&format!("cwd: {}", first_workspace.display())));
    assert!(changed_runtime[1].contains(&format!("cwd: {}", second_workspace.display())));
    assert_eq!(changed_runtime, same_runtime);
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
    let conversation = non_context_session_messages(&messages);
    assert!(matches!(
        conversation[0].content.first(),
        Some(SessionContentBlock::Text { text }) if text == &user_text
    ));
    assert!(matches!(
        conversation[0].content.get(1),
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
    let expected_hash = canonical_user_content_hash(&conversation[0].content).unwrap();
    assert_eq!(
        projection.turns[0].canonical_user_content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(projection.turns[0].canonical_user_first_text, None);
}

#[tokio::test]
async fn request_too_large_media_cleanup_persists_provider_history_for_resume() {
    let dir = tempfile::tempdir().unwrap();
    let image_path = dir.path().join("oversized-request.png");
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    tokio::fs::write(&image_path, image_bytes).await.unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        request_too_large_step(),
        response_step("recovered without media", Vec::new()),
        response_step("resume stayed clean", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_4130feed").await;
    let mut warnings = Vec::new();

    engine
        .run_turn_with_attachments(
            &mut session,
            "inspect the image",
            vec![SessionAttachment::LocalImage { path: image_path }],
            |event| {
                if let SessionEvent::Warning { message } = event {
                    warnings.push(message);
                }
            },
        )
        .await
        .unwrap();

    assert_eq!(warnings.len(), 1);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[0].messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
    assert!(!requests[1].messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
    let canonical_messages = session.read_messages().await.unwrap();
    assert!(canonical_messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionContentBlock::Image { .. }))));
    assert!(canonical_messages
        .iter()
        .any(|message| message.content.iter().any(|block| matches!(
            block,
            SessionContentBlock::ModelContext {
                source: ModelContextSource::RequestSizeRecovery,
                text,
                ..
            } if text.contains("few local files necessary to complete the task")
        ))));
    let journal = replay_turn_journal(session.read_turn_journal().await);
    assert!(journal.turns[0].model_context.iter().any(|context| {
        context.source == ModelContextSource::RequestSizeRecovery
            && context.text.contains("<request_size_recovery>")
    }));
    let metadata = session.read_metadata().await.unwrap();
    let stable_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful media recovery must persist provider history");
    assert!(stable_history.pending_turn.is_none());
    let stable_wire = serde_json::to_string(&stable_history.messages).unwrap();
    assert!(stable_wire.contains("image attachment removed after upstream request_too_large"));
    assert!(stable_wire.contains("<request_size_recovery>"));
    assert!(stable_wire.contains("few local files necessary to complete the task"));
    assert!(!stable_history.messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "continue after restart", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[2].messages.starts_with(&stable_history.messages));
    assert!(!requests[2].messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
}

#[tokio::test]
async fn media_rejection_cleanup_persists_provider_history_for_resume() {
    let dir = tempfile::tempdir().unwrap();
    let image_path = dir.path().join("rejected-media.png");
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    tokio::fs::write(&image_path, image_bytes).await.unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::MediaRejected,
        response_step("recovered without rejected media", Vec::new()),
        response_step("resume stayed clean", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_0ed1af11").await;

    engine
        .run_turn_with_attachments(
            &mut session,
            "inspect rejected media",
            vec![SessionAttachment::LocalImage { path: image_path }],
            |_| {},
        )
        .await
        .unwrap();

    let metadata = session.read_metadata().await.unwrap();
    let stable_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("media recovery must persist a clean Provider history")
        .messages
        .clone();
    let stable_wire = serde_json::to_string(&stable_history).unwrap();
    assert!(stable_wire.contains("<media_recovery>"));
    assert!(!stable_history.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "continue after media rejection", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[2].messages.starts_with(&stable_history));
}

#[tokio::test]
async fn ambiguous_fallback_413_discards_partial_without_cleaning_media_wal() {
    let dir = tempfile::tempdir().unwrap();
    let image_path = dir.path().join("partial-before-413.png");
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    tokio::fs::write(&image_path, image_bytes).await.unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step(
            "stream transport failed",
            vec![ProviderEvent::AssistantTextDelta {
                text: "ghost partial".into(),
            }],
        ),
        request_too_large_step(),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_4130beef").await;
    let mut events = Vec::new();

    let error = engine
        .run_turn_with_attachments(
            &mut session,
            "inspect and continue with a tool",
            vec![SessionAttachment::LocalImage { path: image_path }],
            |event| events.push(event),
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("更早的 Provider attempt 结果不明确"));

    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::AssistantOutputDiscarded)));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::Warning { .. })));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::ToolCallStarted { .. })));

    let journal_read = session.read_turn_journal().await;
    assert!(journal_read
        .events
        .iter()
        .any(|event| matches!(event.kind, TurnJournalEventKind::AssistantOutputDiscarded)));
    let projection = replay_turn_journal(journal_read);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    assert!(!projection.turns[0].assistant_text.contains("ghost partial"));
    assert!(!projection.turns[0]
        .timeline_items
        .iter()
        .any(|item| matches!(
            item,
            crate::session::TurnJournalTimelineItem::Assistant { text, .. }
                if text.contains("ghost partial")
        )));
    assert!(
        !serde_json::to_string(&session.read_messages().await.unwrap())
            .unwrap()
            .contains("ghost partial")
    );

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].messages, requests[1].messages);
    let retained = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("ambiguous 413 must retain the original media WAL");
    assert_eq!(retained.messages, requests[0].messages);
    assert!(retained.messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
}

#[tokio::test]
async fn request_too_large_boundary_rebuilds_clean_history_without_provider_wal() {
    let dir = tempfile::tempdir().unwrap();
    let image_path = dir.path().join("canonical-image.png");
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    tokio::fs::write(&image_path, image_bytes).await.unwrap();
    let provider = Arc::new(
        RecordingProvider::new(vec![
            request_too_large_step(),
            response_step("recovered", Vec::new()),
            response_step("continued from canonical", Vec::new()),
        ])
        .with_history_media_policy(ProviderHistoryMediaPolicy::Preserve),
    );
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_4130cafe").await;

    engine
        .run_turn_with_attachments(
            &mut session,
            "inspect canonical image",
            vec![SessionAttachment::LocalImage { path: image_path }],
            |_| {},
        )
        .await
        .unwrap();

    let mut compaction = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .expect("provider WAL should exist after the recovered request");
    compaction.provider_history = None;
    session.update_compaction(compaction).await.unwrap();
    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();

    engine
        .run_turn(&mut resumed, "continue without provider WAL", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let rebuilt_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(rebuilt_wire.contains("image attachment removed after upstream request_too_large"));
    assert!(rebuilt_wire.contains("<request_size_recovery>"));
    assert!(!requests[2].messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
    assert!(resumed
        .read_messages()
        .await
        .unwrap()
        .iter()
        .any(|message| message
            .content
            .iter()
            .any(|block| matches!(block, SessionContentBlock::Image { .. }))));
}

#[tokio::test]
async fn rejected_clean_retry_keeps_historical_media_boundary() {
    for recovery_mode in 0..4 {
        for second_is_413 in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
            let mut bytes = Vec::new();
            image
                .write_to(
                    &mut std::io::Cursor::new(&mut bytes),
                    image::ImageFormat::Png,
                )
                .unwrap();
            let data = BASE64_STANDARD.encode(bytes);
            let provider = Arc::new(
                RecordingProvider::new(vec![
                    response_step("historical image accepted", Vec::new()),
                    request_too_large_step(),
                    if second_is_413 {
                        request_too_large_step()
                    } else {
                        ProviderStep::Rejected {
                            message: "invalid request",
                        }
                    },
                    response_step("new prompt accepted", Vec::new()),
                ])
                .with_history_media_policy(ProviderHistoryMediaPolicy::Preserve),
            );
            let (engine, store) = build_test_engine(&dir, provider.clone());
            let mut session = create_test_session(&store, "session_face0025").await;
            engine
                .run_turn_with_attachments(
                    &mut session,
                    "historical image",
                    vec![SessionAttachment::InlineImage {
                        media_type: "image/png".into(),
                        data: data.clone(),
                    }],
                    |_| {},
                )
                .await
                .unwrap();
            if recovery_mode == 3 {
                let mut compaction = session.read_metadata().await.unwrap().compaction.unwrap();
                let history = compaction.provider_history.as_mut().unwrap();
                history.replay_identity = Some(ProviderReplayIdentity {
                    protocol: ProviderReplayProtocol::OpenAiResponses,
                    model: "previous-model".into(),
                });
                for _ in 0..10 {
                    history
                        .messages
                        .push(SessionTurnMessage::assistant_text("old continuation"));
                    history
                        .messages
                        .push(SessionTurnMessage::user_text("continue"));
                }
                session.update_compaction(compaction).await.unwrap();
            }
            engine
                .run_turn(&mut session, "REJECTED_TEXT_TURN", |_| {})
                .await
                .unwrap_err();
            let mut compaction = session.read_metadata().await.unwrap().compaction.unwrap();
            let history = compaction.provider_history.as_mut().unwrap();
            let cleaned = serde_json::to_string(&history.messages).unwrap();
            assert!(
                !cleaned.contains(&data),
                "mode={recovery_mode}, 413={second_is_413}"
            );
            assert!(!cleaned.contains("REJECTED_TEXT_TURN"));
            assert!(cleaned.contains("<request_size_recovery>"));
            let journal = replay_turn_journal(session.read_turn_journal().await);
            assert_eq!(
                journal.turns[1].status,
                Some(TurnJournalStatus::RejectedByProvider)
            );
            assert_eq!(journal.turns[1].model_context.len(), 1);
            assert_eq!(
                journal.turns[1].model_context[0].source,
                ModelContextSource::RequestSizeRecovery
            );
            match recovery_mode {
                1 => compaction.provider_history = None,
                2 => {
                    history.replay_identity = Some(ProviderReplayIdentity {
                        protocol: ProviderReplayProtocol::OpenAiResponses,
                        model: "other-model".into(),
                    })
                }
                _ => {}
            }
            session.update_compaction(compaction).await.unwrap();
            let agent_id = session.metadata.agent_id.clone();
            let session_id = session.metadata.id.clone();
            drop(session);
            let mut resumed = store
                .load_existing_session(&agent_id, &session_id)
                .await
                .unwrap();
            engine
                .run_turn(&mut resumed, "NEW_TEXT_TURN", |_| {})
                .await
                .unwrap();
            let requests = provider.requests().await;
            assert_eq!(requests.len(), 4);
            let wire = serde_json::to_string(&requests[3].messages).unwrap();
            assert!(!wire.contains(&data));
            assert!(!wire.contains("REJECTED_TEXT_TURN"));
            assert!(wire.contains("NEW_TEXT_TURN"));
            assert!(wire.contains("<request_size_recovery>"));
        }
    }
}

#[tokio::test]
async fn repeated_request_too_large_discards_the_cleaned_rejected_turn() {
    let dir = tempfile::tempdir().unwrap();
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    let inline_data = BASE64_STANDARD.encode(image_bytes);
    let provider = Arc::new(RecordingProvider::new(vec![
        request_too_large_step(),
        request_too_large_step(),
        response_step("continued after failed cleanup", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_4130fade").await;
    let mut warnings = Vec::new();

    let error = engine
        .run_turn_with_attachments(
            &mut session,
            "inspect clipboard image",
            vec![SessionAttachment::InlineImage {
                media_type: "image/png".into(),
                data: inline_data.clone(),
            }],
            |event| {
                if let SessionEvent::Warning { message } = event {
                    warnings.push(message);
                }
            },
        )
        .await
        .expect_err("the second 413 must fail the active turn");

    let rejected = error
        .downcast_ref::<crate::api::ProviderRequestRejected>()
        .expect("the cleaned retry was deterministically rejected");
    assert!(rejected.should_discard_turn());
    assert_eq!(warnings.len(), 1);
    assert_eq!(
        warnings[0],
        "上游拒绝了过大的请求；已从上下文中移除图片 / PDF 并重试。"
    );
    assert!(session.read_messages().await.unwrap().is_empty());
    let failed_journal = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(
        failed_journal.turns[0].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );
    assert!(!failed_journal.turns[0]
        .model_context
        .iter()
        .any(|context| context.source == ModelContextSource::RequestSizeRecovery));
    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 2);
    assert!(session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .is_none());

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "continue without the image", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(!resumed_wire.contains("inspect clipboard image"));
    assert!(!resumed_wire.contains("<request_size_recovery>"));
    assert!(!resumed_wire.contains(&inline_data));
    assert!(resumed_wire.contains("continue without the image"));
    assert!(!requests[2].messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
    assert!(!resumed
        .read_messages()
        .await
        .unwrap()
        .iter()
        .any(|message| {
            message.content.iter().any(|block| {
                matches!(
                    block,
                    SessionContentBlock::ModelContext {
                        source: ModelContextSource::RequestSizeRecovery,
                        ..
                    }
                )
            })
        }));
}

#[tokio::test]
async fn repeated_request_too_large_preserves_completed_tool_with_clean_history() {
    let dir = tempfile::tempdir().unwrap();
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    let inline_data = BASE64_STANDARD.encode(image_bytes);
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_before_repeated_413",
            "working_note",
            json!({"action": "add", "note": "TOOL_COMPLETED_BEFORE_REPEATED_413"}),
        ),
        request_too_large_step(),
        request_too_large_step(),
        response_step("continued from clean journal boundary", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_4130dead").await;

    engine
        .run_turn_with_attachments(
            &mut session,
            "inspect failed clipboard image",
            vec![SessionAttachment::InlineImage {
                media_type: "image/png".into(),
                data: inline_data.clone(),
            }],
            |_| {},
        )
        .await
        .expect_err("the cleaned retry should be rejected without losing tool progress");

    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    assert!(projection.turns[0]
        .tool_calls
        .iter()
        .any(|tool| tool.tool_use_id == "toolu_before_repeated_413"
            && tool.completed_summary.is_some()));
    let clean_history = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("accepted tool request must remain as a clean recovery boundary");
    let clean_wire = serde_json::to_string(&clean_history.messages).unwrap();
    assert!(!clean_wire.contains(&inline_data));
    assert!(!clean_history.messages.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::Image { .. }))));
    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();

    engine
        .run_turn(&mut resumed, "continue after repeated 413", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    let rebuilt_wire = serde_json::to_string(&requests[3].messages).unwrap();
    assert!(rebuilt_wire.contains("<request_size_recovery>"));
    assert!(rebuilt_wire.contains("tools_completed"));
    assert!(rebuilt_wire.contains("toolu_before_repeated_413"));
    assert!(rebuilt_wire.contains("TOOL_COMPLETED_BEFORE_REPEATED_413"));
    assert!(!rebuilt_wire.contains(&inline_data));
    assert!(!requests[3].messages.iter().any(|message| {
        message.content.iter().any(|block| {
            matches!(block, SessionTurnContentBlock::Image { .. })
                || matches!(
                    block,
                    SessionTurnContentBlock::ToolUse { id, .. }
                        if id == "toolu_before_repeated_413"
                )
        })
    }));
    assert!(resumed
        .read_messages()
        .await
        .unwrap()
        .iter()
        .any(|message| {
            message.content.iter().any(|block| {
                matches!(
                    block,
                    SessionContentBlock::ModelContext {
                        source: ModelContextSource::RequestSizeRecovery,
                        ..
                    }
                )
            })
        }));
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
    let user = first_real_provider_user_message(&requests[0]);
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
    let conversation = non_context_session_messages(&messages);
    assert!(matches!(
        conversation[0].content.first(),
        Some(SessionContentBlock::SkillInstructions { instruction })
            if instruction.content.contains("Read src/auth.rs with src/auth.rs")
    ));
    assert_eq!(text_content(conversation[0]), "/review src/auth.rs");
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
    let user = first_real_provider_user_message(&requests[0]);
    assert!(!user
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::SkillInstructions { .. })));
    let messages = session.read_messages().await.unwrap();
    let conversation = non_context_session_messages(&messages);
    assert_eq!(
        text_content(conversation[0]),
        "请看粘贴内容：\n/review hidden"
    );
}

#[tokio::test]
async fn preflight_active_compaction_persists_exact_window_after_commit() {
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
        response_step("answer after the compacted turn", Vec::new()),
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

    let first_turn_requests = provider.requests().await;
    let metadata_after_first_turn = session.read_metadata().await.unwrap();
    let compacted_history = metadata_after_first_turn
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("成功 compact turn 应固化 provider history");
    let mut expected_provider_history = first_turn_requests[2].messages.clone();
    expected_provider_history.push(SessionTurnMessage::assistant_text(
        "final answer after compact",
    ));
    assert_eq!(compacted_history.messages, expected_provider_history);
    assert_eq!(
        compacted_history.canonical_message_until,
        metadata_after_first_turn.message_count
    );

    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine
        .run_turn(&mut session, "continue after compact", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert!(requests[3].messages.starts_with(&requests[2].messages));
    assert_eq!(requests.len(), 4);
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
    let final_context_sources = requests[2]
        .messages
        .iter()
        .rev()
        .take(3)
        .rev()
        .filter_map(|message| {
            message
                .model_context_snapshot()
                .map(|(source, _, _)| *source)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        final_context_sources,
        vec![
            ModelContextSource::Runtime,
            ModelContextSource::BackgroundProcess,
            ModelContextSource::Delegation,
        ],
        "压缩后的第一次主请求必须以完整 authoritative baseline 收尾"
    );

    let messages = session.read_messages().await.unwrap();
    assert_eq!(non_context_session_messages(&messages).len(), 6);
    assert_eq!(
        messages
            .iter()
            .filter(|message| {
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionContentBlock::ModelContext { .. }))
            })
            .count(),
        6,
        "初始 baseline 与 compact-window baseline 都必须进入 canonical history"
    );
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
async fn context_window_recovery_forces_compaction_and_preserves_latest_anthropic_replay_tail() {
    let dir = tempfile::tempdir().unwrap();
    let older_tool_payload = format!("OLDER_TOOL_PAYLOAD-{}", "x".repeat(12_000));
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_context_1".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": older_tool_payload}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        ProviderStep::Response {
            response: anthropic_response(
                "PARTIAL-",
                ProviderStop::ContextWindowExceeded,
                "context-partial",
            ),
            events: Vec::new(),
        },
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "The earlier working-note tool round completed."}"#,
            Vec::new(),
        ),
        ProviderStep::Response {
            response: anthropic_response("FINAL", ProviderStop::Done, "context-final"),
            events: Vec::new(),
        },
        ProviderStep::Response {
            response: anthropic_response("AFTER-RECOVERY", ProviderStop::Done, "after-recovery"),
            events: Vec::new(),
        },
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee14").await;

    engine
        .run_turn(
            &mut session,
            "complete the task after recovering context",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    let compaction_request = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(compaction_request.contains("OLDER_TOOL_PAYLOAD"));
    assert!(!compaction_request.contains("private-context-partial"));
    assert!(!compaction_request.contains("signature-context-partial"));

    let continued_request = serde_json::to_string(&requests[3].messages).unwrap();
    assert!(continued_request.contains("compacted_current_turn_progress"));
    assert!(continued_request.contains("private-context-partial"));
    assert!(continued_request.contains("signature-context-partial"));
    assert!(continued_request.contains("继续，从上一条回复被截断处继续"));
    assert!(!continued_request.contains("OLDER_TOOL_PAYLOAD"));

    let messages = session.read_messages().await.unwrap();
    let conversation = non_context_session_messages(&messages);
    assert_eq!(conversation.len(), 4);
    assert_eq!(text_content(conversation[3]), "PARTIAL-FINAL");
    let Some(ProviderReplayState::AnthropicMessages {
        model,
        messages: replay_messages,
    }) = conversation[3].provider_replay.as_ref()
    else {
        panic!("final assistant should retain Anthropic replay");
    };
    assert_eq!(model, "test-model");
    assert_eq!(replay_messages.len(), 3);
    assert_eq!(replay_messages[1]["role"], "user");
    assert!(replay_messages[1]["content"][0]["text"]
        .as_str()
        .is_some_and(|text| text.contains("继续，从上一条回复被截断处继续")));

    let metadata = session.read_metadata().await.unwrap();
    let compaction = metadata.compaction.as_ref().unwrap();
    assert!(compaction.active_turn_summary.is_none());
    assert!(compaction.frontier.active_turn.is_none());
    let stable_history = compaction
        .provider_history
        .as_ref()
        .expect("context recovery 完成后必须固化实际请求与最终响应");
    assert_eq!(
        stable_history.canonical_message_until,
        metadata.message_count
    );
    assert!(stable_history.messages.starts_with(&requests[3].messages));
    assert_eq!(
        stable_history.messages.len(),
        requests[3].messages.len() + 1
    );
    let stable_wire = serde_json::to_string(&stable_history.messages).unwrap();
    assert_eq!(stable_wire.matches("private-context-partial").count(), 1);
    assert_eq!(stable_wire.matches("private-context-final").count(), 1);

    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine
        .run_turn(
            &mut session,
            "continue after recovered context window",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5);
    assert!(requests[4].messages.starts_with(&stable_history.messages));
    let next_request = serde_json::to_string(&requests[4].messages).unwrap();
    assert_eq!(next_request.matches("private-context-partial").count(), 1);
    assert_eq!(next_request.matches("signature-context-partial").count(), 1);
    assert_eq!(next_request.matches("private-context-final").count(), 1);
    assert_eq!(next_request.matches("signature-context-final").count(), 1);
    assert_eq!(
        next_request
            .matches("继续，从上一条回复被截断处继续，不要重复已写内容。")
            .count(),
        1
    );
}

#[tokio::test]
async fn rejected_context_window_request_cleans_wal_then_compacts_and_retries() {
    let dir = tempfile::tempdir().unwrap();
    let older_tool_payload = format!("REJECTED_CONTEXT_PAYLOAD-{}", "x".repeat(12_000));
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: ProviderResponse {
                assistant_message: SessionTurnMessage {
                    role: "assistant".into(),
                    provider_replay: None,
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_rejected_context".into(),
                        name: "working_note".into(),
                        input: json!({"action": "add", "note": older_tool_payload}),
                    }],
                },
                stop: ProviderStop::ToolUse,
            },
            events: Vec::new(),
        },
        ProviderStep::ContextWindowRejected,
        response_step(
            r#"{"committed_summary": null, "active_turn_summary": "The earlier working-note tool round completed."}"#,
            Vec::new(),
        ),
        response_step("recovered after rejected context request", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c07e1701").await;

    engine
        .run_turn(&mut session, "recover the rejected context request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    assert!(serde_json::to_string(&requests[2].messages)
        .unwrap()
        .contains("REJECTED_CONTEXT_PAYLOAD"));
    let retried_wire = serde_json::to_string(&requests[3].messages).unwrap();
    assert!(retried_wire.contains("compacted_current_turn_progress"));
    assert!(!retried_wire.contains("REJECTED_CONTEXT_PAYLOAD"));
    let metadata = session.read_metadata().await.unwrap();
    let stable_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful retry must promote the compacted Provider history");
    assert!(stable_history.pending_turn.is_none());
    let journal = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(journal.turns[0].status, Some(TurnJournalStatus::Committed));
}

#[tokio::test]
async fn first_request_context_rejection_compacts_committed_history_and_retries() {
    let dir = tempfile::tempdir().unwrap();
    let older_payload = format!("OLDER_COMMITTED_CONTEXT-{}", "x".repeat(12_000));
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(&older_payload, Vec::new()),
        ProviderStep::ContextWindowRejected,
        response_step(
            r#"{"committed_summary":"Older committed context.","active_turn_summary":null}"#,
            Vec::new(),
        ),
        response_step("recovered after first request rejection", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c07e1702").await;

    engine
        .run_turn(&mut session, "create committed history", |_| {})
        .await
        .unwrap();
    engine
        .run_turn(&mut session, "recover on the first request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    let rejected_wire = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(rejected_wire.contains("OLDER_COMMITTED_CONTEXT"));
    let compaction_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(compaction_wire.contains("OLDER_COMMITTED_CONTEXT"));
    let retried_wire = serde_json::to_string(&requests[3].messages).unwrap();
    assert!(retried_wire.contains("Older committed context."));
    assert!(!retried_wire.contains("OLDER_COMMITTED_CONTEXT"));
    assert!(retried_wire.contains("recover on the first request"));

    let journal = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(journal.turns.len(), 2);
    assert!(journal
        .turns
        .iter()
        .all(|turn| turn.status == Some(TurnJournalStatus::Committed)));
}

#[tokio::test]
async fn failed_retry_after_context_rejection_remains_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let older_payload = format!("OLDER_CONTEXT_BEFORE_FAILED_RETRY-{}", "x".repeat(12_000));
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(&older_payload, Vec::new()),
        ProviderStep::ContextWindowRejected,
        response_step(
            r#"{"committed_summary":"Older context.","active_turn_summary":null}"#,
            Vec::new(),
        ),
        error_step("transport failed after context recovery", Vec::new()),
        response_step("recovered on the next turn", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c07e1703").await;

    engine
        .run_turn(&mut session, "create older context", |_| {})
        .await
        .unwrap();
    engine
        .run_turn(&mut session, "recover context then fail", |_| {})
        .await
        .unwrap_err();

    let journal = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(journal.turns[1].status, Some(TurnJournalStatus::Failed));

    engine
        .run_turn(&mut session, "continue after failed retry", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5);
    let resumed_wire = serde_json::to_string(&requests[4].messages).unwrap();
    assert!(resumed_wire.contains("recover context then fail"));
    assert!(resumed_wire.contains("<interrupted_turn_context>"));
    assert!(resumed_wire.contains("continue after failed retry"));
}

#[tokio::test]
async fn internal_max_token_then_context_recovery_preserves_entire_anthropic_replay_chain() {
    let dir = tempfile::tempdir().unwrap();
    let older_tool_payload = format!("OLDER_INTERNAL_CONTEXT_PAYLOAD-{}", "x".repeat(12_000));
    let provider = Arc::new(InternalContinuationContextRecoveryProvider::new(
        older_tool_payload.clone(),
    ));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee1a").await;

    engine
        .run_turn(
            &mut session,
            "finish after both max-token and context-window continuation",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.main_requests().await;
    assert_eq!(requests.len(), 4);
    assert!(requests[2]
        .messages
        .starts_with(requests[1].messages.as_slice()));
    assert_eq!(requests[2].messages.len(), requests[1].messages.len() + 1);

    let compaction_requests = provider.compaction_requests().await;
    assert_eq!(compaction_requests.len(), 1);
    let compaction_wire = serde_json::to_string(&compaction_requests[0].messages).unwrap();
    assert!(compaction_wire.contains("OLDER_INTERNAL_CONTEXT_PAYLOAD"));
    assert!(!compaction_wire.contains("private-max-token-partial"));
    assert!(!compaction_wire.contains("private-context-window-partial"));

    let continued_wire = serde_json::to_string(&requests[3].messages).unwrap();
    assert!(continued_wire.contains("compacted_current_turn_progress"));
    assert!(!continued_wire.contains("OLDER_INTERNAL_CONTEXT_PAYLOAD"));
    assert_eq!(
        continued_wire.matches("private-max-token-partial").count(),
        1
    );
    assert_eq!(
        continued_wire
            .matches("private-context-window-partial")
            .count(),
        1
    );
    assert_eq!(
        continued_wire
            .matches("signature-max-token-partial")
            .count(),
        1
    );
    assert_eq!(
        continued_wire
            .matches("signature-context-window-partial")
            .count(),
        1
    );

    let messages = session.read_messages().await.unwrap();
    let conversation = non_context_session_messages(&messages);
    assert_eq!(conversation.len(), 4);
    assert_eq!(
        text_content(conversation[3]),
        "MAX-PARTIAL-CONTEXT-PARTIAL-FINAL"
    );
    let Some(ProviderReplayState::AnthropicMessages {
        model,
        messages: replay_messages,
    }) = conversation[3].provider_replay.as_ref()
    else {
        panic!("final assistant should retain the complete Anthropic continuation chain");
    };
    assert_eq!(model, "test-model");
    assert_eq!(replay_messages.len(), 5);
    assert_eq!(
        replay_messages
            .iter()
            .map(|message| message["role"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["assistant", "user", "assistant", "user", "assistant"]
    );
    let canonical_replay_wire = serde_json::to_string(replay_messages).unwrap();
    assert_eq!(
        canonical_replay_wire
            .matches("private-max-token-partial")
            .count(),
        1
    );
    assert_eq!(
        canonical_replay_wire
            .matches("private-context-window-partial")
            .count(),
        1
    );
    assert_eq!(
        canonical_replay_wire
            .matches("private-final-after-internal-context")
            .count(),
        1
    );

    let metadata = session.read_metadata().await.unwrap();
    let stable_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful recovery must promote the exact response-inclusive Provider history");
    assert!(stable_history.messages.starts_with(&requests[3].messages));
    assert_eq!(
        stable_history.messages.len(),
        requests[3].messages.len() + 1
    );
    assert_eq!(
        stable_history.canonical_message_until,
        metadata.message_count
    );
    let stable_wire = serde_json::to_string(&stable_history.messages).unwrap();
    assert_eq!(stable_wire.matches("private-max-token-partial").count(), 1);
    assert_eq!(
        stable_wire
            .matches("private-context-window-partial")
            .count(),
        1
    );
    assert_eq!(
        stable_wire
            .matches("private-final-after-internal-context")
            .count(),
        1
    );
}

#[tokio::test]
async fn context_window_recovery_respects_disabled_auto_compaction_without_committing_turn() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![ProviderStep::Response {
        response: anthropic_response("partial", ProviderStop::ContextWindowExceeded, "disabled"),
        events: Vec::new(),
    }]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_c0ffee15").await;

    let error = engine
        .run_turn(&mut session, "do not override disabled compaction", |_| {})
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("模型上下文已满，但自动压缩已关闭"));
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    assert!(session.read_messages().await.unwrap().is_empty());
    let metadata = session.read_metadata().await.unwrap();
    let compaction = metadata
        .compaction
        .as_ref()
        .expect("context-window failure after a real request must retain its ordinary WAL");
    assert!(compaction.committed_summary.is_empty());
    let history = compaction.provider_history.as_ref().unwrap();
    assert!(history.pending_turn.is_some());
    assert_eq!(history.messages, requests[0].messages);
}

#[tokio::test]
async fn context_window_recovery_compacts_and_requests_recap_only_for_committed_history() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Response {
            response: anthropic_response(
                "CURRENT-PARTIAL-",
                ProviderStop::ContextWindowExceeded,
                "committed-history-context",
            ),
            events: Vec::new(),
        },
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":"Older committed work was summarized.","active_turn_summary":null}"#,
            ],
            &[],
        ),
        ProviderStep::Response {
            response: anthropic_response("DONE", ProviderStop::Done, "committed-history-final"),
            events: Vec::new(),
        },
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_c0ffee16").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "OLDER_COMMITTED_USER_ONE"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                format!("OLDER_COMMITTED_ASSISTANT_ONE-{}", "a".repeat(8_000)),
            ),
            NewSessionMessage::text(SessionMessageRole::User, "OLDER_COMMITTED_USER_TWO"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                format!("OLDER_COMMITTED_ASSISTANT_TWO-{}", "b".repeat(8_000)),
            ),
        ])
        .await
        .unwrap();

    let mut events = Vec::new();
    engine
        .run_turn(&mut session, "finish after committed compaction", |event| {
            events.push(event)
        })
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let compaction_request = requests
        .iter()
        .find(|request| request.system_prompt.contains("committed_summary"))
        .expect("committed summary request");
    assert!(serde_json::to_string(&compaction_request.messages)
        .unwrap()
        .contains("OLDER_COMMITTED_USER_ONE"));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::RecapRequested {
            recap_end_index: 4,
            ..
        }
    )));
    assert!(requests
        .iter()
        .all(|request| !request.system_prompt.contains("new_claims")));
    let final_request = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(final_request.contains("Older committed work was summarized."));
    assert!(final_request.contains("CURRENT-PARTIAL"));
    assert!(final_request.contains("private-committed-history-context"));

    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    let compaction = metadata.compaction.unwrap();
    assert_eq!(compaction.committed_message_until(), 4);
    assert!(compaction.active_turn_summary.is_none());
    let messages = session.read_messages().await.unwrap();
    let conversation = non_context_session_messages(&messages);
    assert_eq!(conversation.len(), 6);
    assert_eq!(text_content(conversation[5]), "CURRENT-PARTIAL-DONE");
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
    let messages = session.read_messages().await.unwrap();
    assert_eq!(metadata.message_count, messages.len());
    assert_eq!(non_context_session_messages(&messages).len(), 4);
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
    assert_eq!(metadata.message_count, 7);
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
    assert_eq!(
        final_request
            .matches("please compact twice in one turn")
            .count(),
        1,
        "第二次 active-only compaction 不得把旧 provider window 与重建 active 投影叠加"
    );
}

#[tokio::test]
async fn recovered_turn_can_compact_after_context_request_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_recovered",
            "working_note",
            json!({"action": "add", "note": "recoverable work ".repeat(1_000)}),
        ),
        error_step("interrupted earlier work", Vec::new()),
        ProviderStep::ContextWindowRejected,
        response_step(
            &json!({"committed_summary":null,"active_turn_summary":"Intermediate recovery summary. ".repeat(100)}).to_string(),
            Vec::new(),
        ),
        ProviderStep::ContextWindowRejected,
        response_step(
            r#"{"committed_summary":null,"active_turn_summary":"Recovered work summarized."}"#,
            Vec::new(),
        ),
        response_step("recovered after context rejection", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_face0026").await;
    engine
        .run_turn(&mut session, "earlier task", |_| {})
        .await
        .unwrap_err();
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    let mut compacted = false;
    engine
        .run_turn(&mut session, "resume task", |event| {
            compacted |= matches!(event, SessionEvent::CompactionCompleted { .. });
        })
        .await
        .unwrap();
    assert!(compacted);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 7);
    let final_wire = serde_json::to_string(&requests[6].messages).unwrap();
    assert!(final_wire.contains("Recovered work summarized."));
    assert!(final_wire.contains("resume task"));
}

#[tokio::test]
async fn recovery_turn_preserves_summary_across_multiple_auto_compacts() {
    assert_recovery_turn_can_compact_again(false).await;
}

#[tokio::test]
async fn recovery_turn_forces_context_compaction_after_auto_compact() {
    assert_recovery_turn_can_compact_again(true).await;
}

async fn assert_recovery_turn_can_compact_again(force_context_recovery: bool) {
    const RECOVERY_SUMMARY: &str = "Earlier failed work summarized.";
    const FIRST_SUMMARY: &str = "Failed work and first live tool round summarized.";
    const SECOND_SUMMARY: &str = "Failed work and both live tool rounds summarized.";
    const CURRENT_REQUEST: &str = "finish the recovered task through both tool rounds";
    const NEW_TOOL_MARKER: &str = "SECOND_LIVE_TOOL_PAYLOAD";
    const PARTIAL: &str = "LATEST_CONTEXT_PARTIAL-";

    let dir = tempfile::tempdir().unwrap();
    let summary_step = |summary| {
        response_step(
            &json!({"committed_summary": null, "active_turn_summary": summary}).to_string(),
            Vec::new(),
        )
    };
    let tool_step = |id: &str, note: String, used_tokens| {
        let mut step = tool_use_step(id, "working_note", json!({"action": "add", "note": note}));
        if let ProviderStep::Response { events, .. } = &mut step {
            events.push(ProviderEvent::ContextUsageUpdated {
                usage: ContextUsageSnapshot {
                    used_tokens,
                    source: ContextUsageSource::Provider,
                },
            });
        }
        step
    };
    let mut steps = vec![
        tool_step("toolu_failed", "failed progress ".repeat(1_000), 1),
        error_step("failed after recording tool progress", Vec::new()),
        summary_step(RECOVERY_SUMMARY),
        tool_step(
            "toolu_live_1",
            "first live progress ".repeat(1_000),
            190_000,
        ),
        summary_step(FIRST_SUMMARY),
        tool_step(
            "toolu_live_2",
            format!("{NEW_TOOL_MARKER}{}", " second live progress".repeat(1_000)),
            if force_context_recovery { 1 } else { 190_000 },
        ),
    ];
    if force_context_recovery {
        steps.push(ProviderStep::Response {
            response: anthropic_response(
                PARTIAL,
                ProviderStop::ContextWindowExceeded,
                "recovery-context",
            ),
            events: Vec::new(),
        });
    }
    steps.push(summary_step(SECOND_SUMMARY));
    steps.push(if force_context_recovery {
        ProviderStep::Response {
            response: anthropic_response("DONE", ProviderStop::Done, "recovery-final"),
            events: Vec::new(),
        }
    } else {
        response_step("DONE", Vec::new())
    });
    let provider = Arc::new(RecordingProvider::new(steps));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_face0024").await;

    engine
        .run_turn(&mut session, "record progress before failure", |_| {})
        .await
        .expect_err("the initial turn must fail after a tool round");
    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    engine.compaction.auto_compact_ctx_ratio = 0.9;
    let mut events = Vec::new();
    engine
        .run_turn(&mut session, CURRENT_REQUEST, |event| events.push(event))
        .await
        .unwrap();

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::CompactionCompleted { .. }))
            .count(),
        2,
        "the recovered turn must complete both live compactions"
    );
    let requests = provider.requests().await;
    let summary_requests = requests
        .iter()
        .filter(|request| request.system_prompt.contains("committed_summary"))
        .collect::<Vec<_>>();
    assert_eq!(summary_requests.len(), 3);
    let first_live_summary = serde_json::to_string(&summary_requests[1].messages).unwrap();
    assert!(first_live_summary.contains(RECOVERY_SUMMARY));
    let second_live_summary = serde_json::to_string(&summary_requests[2].messages).unwrap();
    assert!(second_live_summary.contains(FIRST_SUMMARY));
    assert!(second_live_summary.contains(NEW_TOOL_MARKER));
    assert!(!second_live_summary.contains(PARTIAL));

    let final_request = serde_json::to_string(&requests.last().unwrap().messages).unwrap();
    assert!(final_request.contains(SECOND_SUMMARY));
    assert!(!final_request.contains(NEW_TOOL_MARKER));
    assert_eq!(final_request.matches(CURRENT_REQUEST).count(), 1);
    if force_context_recovery {
        assert_eq!(
            requests
                .last()
                .unwrap()
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .filter(|block| matches!(block, SessionTurnContentBlock::Text { text } if text == PARTIAL))
                .count(),
            1
        );
        assert_eq!(final_request.matches("private-recovery-context").count(), 1);
        assert!(final_request.contains("继续，从上一条回复被截断处继续"));
        let messages = session.read_messages().await.unwrap();
        assert_eq!(
            text_content(messages.last().unwrap()),
            format!("{PARTIAL}DONE")
        );
    }
    let state = session.read_metadata().await.unwrap().compaction.unwrap();
    assert!(state.active_turn_summary.is_none());
    assert!(state.frontier.active_turn.is_none());
    let history = state.provider_history.unwrap();
    assert!(history.recovery_turn_id.is_none());
    assert!(history.recovery_base_message_count.is_none());
    assert!(history.pending_turn.is_none());
    assert!(history
        .messages
        .starts_with(&requests.last().unwrap().messages));
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
            0,
        )
        .unwrap();

    assert!(plan.prior_active_turn_summary.is_none());
    assert!(plan.prior_active_turn_cursor.is_none());
    assert!(plan.active_turn.is_some());
}

#[tokio::test]
async fn failed_turn_clears_active_summary_but_replays_write_ahead_provider_window() {
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
        response_step("recovered after compacted provider failure", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
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
    let compaction = metadata.compaction.as_ref().unwrap();
    assert!(compaction.active_turn_summary.is_none());
    assert!(compaction.frontier.active_turn.is_none());
    let pending_history = compaction
        .provider_history
        .as_ref()
        .expect("failed compacted request must retain its write-ahead provider window");
    assert!(pending_history.pending_turn.is_some());
    assert!(pending_history.canonical_message_until > metadata.message_count);
    let requests_after_failure = provider.requests().await;
    assert!(!pending_history.messages.is_empty());
    assert_eq!(requests_after_failure.len(), 3);

    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine
        .run_turn(
            &mut session,
            "recover without rewriting the failed request",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    assert!(requests[3].messages.starts_with(&requests[2].messages));
    let metadata = session.read_metadata().await.unwrap();
    let history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful recovery must promote the pending provider window");
    assert!(history.pending_turn.is_none());
    assert_eq!(history.canonical_message_until, metadata.message_count);
    let mut expected_history = requests[3].messages.clone();
    expected_history.push(SessionTurnMessage::assistant_text(
        "recovered after compacted provider failure",
    ));
    assert_eq!(history.messages, expected_history);
}

#[tokio::test]
async fn failed_turn_after_stable_compaction_replays_latest_write_ahead_provider_window() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step(
            "provider failed after stable compacted baseline",
            Vec::new(),
        ),
        response_step(
            "recovered after stable compacted baseline failure",
            Vec::new(),
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_c0ffee1a").await;
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: None,
        canonical_message_until: 0,
        messages: vec![SessionTurnMessage::user_text("STABLE_COMPACTED_BASELINE")],
    }));
    session.update_compaction(compaction).await.unwrap();

    let err = engine
        .run_turn(&mut session, "fail after the stable baseline", |_| {})
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("provider failed after stable compacted baseline"));

    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    let pending_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("stable compacted generation must write ahead every Provider request");
    assert!(pending_history.pending_turn.is_some());
    assert_eq!(pending_history.messages, failed_requests[0].messages);
    assert_eq!(
        pending_history.messages.last(),
        Some(&SessionTurnMessage::user_text(
            "fail after the stable baseline"
        ))
    );

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut session = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .expect("restart must accept a pending cursor that leads canonical messages");

    engine
        .run_turn(&mut session, "recover the stable baseline request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.starts_with(&requests[0].messages));
    let recovery_suffix = &requests[1].messages[requests[0].messages.len()..];
    assert_eq!(recovery_suffix.len(), 1);
    assert!(serde_json::to_string(recovery_suffix)
        .unwrap()
        .contains("recover the stable baseline request"));
    let metadata = session.read_metadata().await.unwrap();
    let history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful recovery must promote the latest write-ahead window");
    assert!(history.pending_turn.is_none());
    let mut expected_history = requests[1].messages.clone();
    expected_history.push(SessionTurnMessage::assistant_text(
        "recovered after stable compacted baseline failure",
    ));
    assert_eq!(history.messages, expected_history);
    assert_eq!(history.canonical_message_until, metadata.message_count);
}

#[tokio::test]
async fn failed_uncompacted_turn_replays_write_ahead_provider_window_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step("provider failed before any compaction", Vec::new()),
        response_step("recovered uncompacted request", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_c0ffee1e").await;

    let error = engine
        .run_turn(&mut session, "fail before any compaction", |_| {})
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("provider failed before any compaction"));

    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    let compaction = metadata
        .compaction
        .as_ref()
        .expect("first ordinary Provider request must lazily establish the bounded WAL");
    assert!(compaction.committed_summary.is_empty());
    assert_eq!(compaction.committed_message_until(), 0);
    let pending = compaction
        .provider_history
        .as_ref()
        .expect("failed ordinary request must retain its exact provider window");
    assert!(pending.pending_turn.is_some());
    assert!(pending.canonical_message_until > metadata.message_count);
    assert_eq!(pending.messages, failed_requests[0].messages);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut session = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .expect("restart must load an uncompacted pending Provider cursor");

    engine
        .run_turn(&mut session, "recover ordinary failed request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.starts_with(&requests[0].messages));
    let metadata = session.read_metadata().await.unwrap();
    let stable = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful recovery must promote the ordinary request WAL");
    assert!(stable.pending_turn.is_none());
    assert_eq!(stable.canonical_message_until, metadata.message_count);
}

#[tokio::test]
async fn provider_rejected_turn_drops_request_wal_and_is_not_replayed_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Rejected {
            message: "provider rejected historical content",
        },
        response_step("accepted clean retry", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_9e1ec7ed").await;

    let error = engine
        .run_turn(&mut session, "REJECTED_REQUEST_MUST_NOT_REPLAY", |_| {})
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("provider rejected historical content"));

    let metadata = session.read_metadata().await.unwrap();
    assert!(metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .is_none());
    let journal = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(
        journal.turns[0].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "clean replacement request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    let resumed_wire = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(!resumed_wire.contains("REJECTED_REQUEST_MUST_NOT_REPLAY"));
    assert!(!resumed_wire.contains("interrupted_turn_context"));
    assert!(resumed_wire.contains("clean replacement request"));
}

#[tokio::test]
async fn ambiguous_stream_attempt_followed_by_fallback_rejection_keeps_wal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step(
            "stream transport failed",
            vec![ProviderEvent::AssistantTextDelta {
                text: "uncertain partial".into(),
            }],
        ),
        ProviderStep::Rejected {
            message: "fallback rejected the same request",
        },
        response_step("recovered ambiguous request", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c03").await;

    let error = engine
        .run_turn(&mut session, "AMBIGUOUS_STREAM_REQUEST", |_| {})
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("更早的 Provider attempt 结果不明确"));
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    assert!(!projection.turns[0]
        .assistant_text
        .contains("uncertain partial"));

    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 2);
    assert_eq!(failed_requests[0].messages, failed_requests[1].messages);
    let retained = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("an earlier ambiguous attempt must keep its request WAL");
    assert_eq!(retained.messages, failed_requests[0].messages);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "resume ambiguous stream request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[2]
        .messages
        .starts_with(&failed_requests[0].messages));
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(resumed_wire.contains("AMBIGUOUS_STREAM_REQUEST"));
    assert!(resumed_wire.contains("<interrupted_turn_context>"));
}

#[tokio::test]
async fn ambiguous_internal_retry_followed_by_rejection_keeps_wal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(InternalRetryThenRejectedProvider {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        previous_attempt_ambiguous: true,
    });
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c06").await;

    let mut events = Vec::new();
    let error = engine
        .run_turn(&mut session, "AMBIGUOUS_INTERNAL_RETRY", |event| {
            events.push(event)
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("内部重试前已有发送结果不明确"));
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    assert!(projection.turns[0].assistant_text.is_empty());
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::AssistantOutputDiscarded)));

    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 2);
    assert_eq!(failed_requests[0].messages, failed_requests[1].messages);
    let retained = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("ambiguous internal retry must keep its request WAL");
    assert_eq!(retained.messages, failed_requests[0].messages);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "resume ambiguous internal retry", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[2]
        .messages
        .starts_with(&failed_requests[0].messages));
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(resumed_wire.contains("AMBIGUOUS_INTERNAL_RETRY"));
    assert!(resumed_wire.contains("<interrupted_turn_context>"));
}

#[tokio::test]
async fn resolved_internal_retry_followed_by_rejection_clears_wal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(InternalRetryThenRejectedProvider {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        previous_attempt_ambiguous: false,
    });
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c09").await;

    let error = engine
        .run_turn(&mut session, "RESOLVED_INTERNAL_RETRY", |_| {})
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<ProviderRequestRejected>().is_some());
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(
        projection.turns[0].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );

    engine
        .run_turn(&mut session, "continue after resolved retry", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(!resumed_wire.contains("RESOLVED_INTERNAL_RETRY"));
    assert!(!resumed_wire.contains("<interrupted_turn_context>"));
    assert!(resumed_wire.contains("continue after resolved retry"));
}

#[tokio::test]
async fn ambiguous_media_rejection_preserves_the_original_request_wal() {
    let dir = tempfile::tempdir().unwrap();
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut image_bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut image_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    let inline_data = BASE64_STANDARD.encode(image_bytes);
    let provider = Arc::new(AmbiguousRetryMediaThenRejectedProvider {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c07").await;

    let error = engine
        .run_turn_with_attachments(
            &mut session,
            "AMBIGUOUS_MEDIA_RETRY",
            vec![SessionAttachment::InlineImage {
                media_type: "image/png".into(),
                data: inline_data.clone(),
            }],
            |_| {},
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("发送结果不明确"));
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 1);
    let retained_wire = serde_json::to_string(&failed_requests[0].messages).unwrap();
    assert!(!retained_wire.contains("<media_recovery>"));
    assert!(retained_wire.contains(&inline_data));
    let retained = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("ambiguous media rejection must retain the original WAL");
    assert_eq!(retained.messages, failed_requests[0].messages);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    let rejection = engine
        .run_turn(&mut resumed, "resume ambiguous media request", |_| {})
        .await
        .unwrap_err();
    assert!(rejection
        .downcast_ref::<ProviderRequestRejected>()
        .is_some());

    engine
        .run_turn(&mut resumed, "retry after explicit rejection", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[1]
        .messages
        .starts_with(&failed_requests[0].messages));
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(!resumed_wire.contains("resume ambiguous media request"));
    assert!(resumed_wire.contains("retry after explicit rejection"));
}

#[tokio::test]
async fn ambiguous_stream_followed_by_text_only_413_keeps_wal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step(
            "stream transport failed",
            vec![ProviderEvent::AssistantTextDelta {
                text: "uncertain text partial".into(),
            }],
        ),
        request_too_large_step(),
        response_step("recovered text-only 413", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c07").await;

    let error = engine
        .run_turn(&mut session, "AMBIGUOUS_TEXT_ONLY_413", |_| {})
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("更早的 Provider attempt 结果不明确"));
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));

    let failed_requests = provider.requests().await;
    assert_eq!(failed_requests.len(), 2);
    assert_eq!(failed_requests[0].messages, failed_requests[1].messages);
    let retained = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("ambiguous text-only 413 must keep its request WAL");
    assert_eq!(retained.messages, failed_requests[0].messages);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "resume text-only 413", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[2]
        .messages
        .starts_with(&failed_requests[0].messages));
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(resumed_wire.contains("AMBIGUOUS_TEXT_ONLY_413"));
    assert!(resumed_wire.contains("<interrupted_turn_context>"));
}

#[tokio::test]
async fn journaled_rejection_recovers_wal_rollback_after_crash_window() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted after crash recovery",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c04").await;
    let rejected_messages = vec![SessionTurnMessage::user_text(
        "CRASH_WINDOW_REJECTED_REQUEST",
    )];
    let mut rejected_compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    rejected_compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(rejected_messages.len()),
        }),
        canonical_message_until: 1,
        messages: rejected_messages,
    }));
    session
        .update_compaction(rejected_compaction)
        .await
        .unwrap();
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::new(
            "turn_1".into(),
            1,
            ProviderRejectedRequestRecovery::DiscardTurn,
            None,
        ),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "CRASH_WINDOW_REJECTED_REQUEST".into(),
        },
        TurnJournalEventKind::ProviderRequestRejected {
            rejection_id: 1,
            discard_turn: true,
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(&mut session, "clean request after crash", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(!wire.contains("CRASH_WINDOW_REJECTED_REQUEST"));
    assert!(!wire.contains("<interrupted_turn_context>"));
    assert!(wire.contains("clean request after crash"));
    assert!(
        !tokio::fs::try_exists(&session.paths.provider_rejection_recovery_json)
            .await
            .unwrap()
    );
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(
        projection.turns[0].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );
    assert_eq!(
        projection.turns[1].status,
        Some(TurnJournalStatus::Committed)
    );
}

#[tokio::test]
async fn prepared_retry_wal_without_supersede_marker_keeps_rejection_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted after retry preparation crash",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c10").await;
    let retry_messages = vec![SessionTurnMessage::user_text(
        "RETRY_WAL_PREPARED_BEFORE_SUPERSEDE_MARKER",
    )];
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(retry_messages.len()),
        }),
        canonical_message_until: 1,
        messages: retry_messages,
    }));
    session.update_compaction(compaction).await.unwrap();
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::new(
            "turn_1".into(),
            1,
            ProviderRejectedRequestRecovery::DiscardTurn,
            None,
        ),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "original context-rejected request".into(),
        },
        TurnJournalEventKind::ProviderRequestRejected {
            rejection_id: 1,
            discard_turn: true,
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(
            &mut session,
            "continue after retry preparation crash",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(!wire.contains("RETRY_WAL_PREPARED_BEFORE_SUPERSEDE_MARKER"));
    assert!(!wire.contains("original context-rejected request"));
    assert!(!wire.contains("<interrupted_turn_context>"));
    assert!(wire.contains("continue after retry preparation crash"));
}

#[tokio::test]
async fn supersede_marker_keeps_prepared_retry_wal_and_clears_old_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted superseded retry",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c12").await;
    let retry_messages = vec![SessionTurnMessage::user_text(
        "SUPERSEDED_RETRY_WAL_MUST_SURVIVE",
    )];
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(retry_messages.len()),
        }),
        canonical_message_until: 1,
        messages: retry_messages,
    }));
    session.update_compaction(compaction).await.unwrap();
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::new(
            "turn_1".into(),
            1,
            ProviderRejectedRequestRecovery::DiscardTurn,
            None,
        ),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "original context-rejected request".into(),
        },
        TurnJournalEventKind::ProviderRequestRejected {
            rejection_id: 1,
            discard_turn: true,
        },
        TurnJournalEventKind::ProviderRequestRetriedAfterRejection { rejection_id: 1 },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(&mut session, "continue after supersede crash", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(wire.contains("SUPERSEDED_RETRY_WAL_MUST_SURVIVE"));
    assert!(wire.contains("<interrupted_turn_context>"));
    assert!(wire.contains("continue after supersede crash"));
    assert!(
        !tokio::fs::try_exists(&session.paths.provider_rejection_recovery_json)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn cleaned_media_sidecar_replaces_raw_wal_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted cleaned media recovery",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c11").await;
    let raw_messages = vec![SessionTurnMessage {
        role: "user".into(),
        content: vec![SessionTurnContentBlock::Image {
            media_type: "image/png".into(),
            data: "RAW_MEDIA_AFTER_CRASH".into(),
        }],
        provider_replay: None,
    }];
    let cleaned_messages = vec![SessionTurnMessage::user_text(
        "CLEANED_MEDIA_REQUEST_AFTER_CRASH",
    )];
    let history = |messages: Vec<SessionTurnMessage>| CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(messages.len()),
        }),
        canonical_message_until: 1,
        messages,
    };
    let mut raw_compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    raw_compaction.provider_history = Some(Box::new(history(raw_messages)));
    session.update_compaction(raw_compaction).await.unwrap();
    let mut cleaned_compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    cleaned_compaction.provider_history = Some(Box::new(history(cleaned_messages)));
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::cleaned_request(
            "turn_1".into(),
            Some(cleaned_compaction),
        ),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "request with media before crash".into(),
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(&mut session, "continue after media cleanup crash", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(!wire.contains("RAW_MEDIA_AFTER_CRASH"));
    assert!(wire.contains("CLEANED_MEDIA_REQUEST_AFTER_CRASH"));
    assert!(wire.contains("continue after media cleanup crash"));
}

#[tokio::test]
async fn stale_cleaned_media_sidecar_cannot_overwrite_finished_turn_state() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted after stale media sidecar",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c14").await;
    let history = |message: &str| CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(1),
        }),
        canonical_message_until: 0,
        messages: vec![SessionTurnMessage::user_text(message)],
    };
    let mut current = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    current.provider_history = Some(Box::new(history("NEWER_FINISHED_TURN_WAL")));
    session.update_compaction(current).await.unwrap();
    let mut stale = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    stale.provider_history = Some(Box::new(history("STALE_CLEANED_MEDIA_WAL")));
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::cleaned_request("turn_1".into(), Some(stale)),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::Committed,
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(&mut session, "continue after stale media sidecar", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(wire.contains("NEWER_FINISHED_TURN_WAL"));
    assert!(!wire.contains("STALE_CLEANED_MEDIA_WAL"));
    assert!(
        !tokio::fs::try_exists(&session.paths.provider_rejection_recovery_json)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn unjournaled_rejection_sidecar_completes_rejection_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted after ambiguous crash",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c05").await;
    let rejected_messages = vec![SessionTurnMessage::user_text(
        "CRASH_BEFORE_REJECTION_JOURNAL",
    )];
    let mut rejected_compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    rejected_compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(rejected_messages.len()),
        }),
        canonical_message_until: 1,
        messages: rejected_messages,
    }));
    session
        .update_compaction(rejected_compaction)
        .await
        .unwrap();
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::new(
            "turn_1".into(),
            1,
            ProviderRejectedRequestRecovery::DiscardTurn,
            None,
        ),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "CRASH_BEFORE_REJECTION_JOURNAL".into(),
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(&mut session, "continue after rejected request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(!wire.contains("CRASH_BEFORE_REJECTION_JOURNAL"));
    assert!(!wire.contains("<interrupted_turn_context>"));
    assert!(wire.contains("continue after rejected request"));
    assert!(
        !tokio::fs::try_exists(&session.paths.provider_rejection_recovery_json)
            .await
            .unwrap()
    );
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(
        projection.turns[0].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );
}

#[tokio::test]
async fn stale_rejection_event_does_not_replace_latest_sidecar_generation() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "accepted after second rejection crash",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_a11b1c08").await;
    let latest_messages = vec![SessionTurnMessage::user_text(
        "SECOND_REJECTION_WAL_MUST_SURVIVE",
    )];
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(latest_messages.len()),
        }),
        canonical_message_until: 1,
        messages: latest_messages,
    }));
    session.update_compaction(compaction).await.unwrap();
    write_provider_rejection_recovery(
        &session.paths.provider_rejection_recovery_json,
        &ProviderRejectionRecoveryRecord::new(
            "turn_1".into(),
            2,
            ProviderRejectedRequestRecovery::DiscardTurn,
            None,
        ),
    )
    .await
    .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::UserInputAccepted {
            text: "first rejected request".into(),
        },
        TurnJournalEventKind::ProviderRequestRejected {
            rejection_id: 1,
            discard_turn: true,
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    engine
        .run_turn(&mut session, "continue after crash", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let wire = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(!wire.contains("SECOND_REJECTION_WAL_MUST_SURVIVE"));
    assert!(!wire.contains("<interrupted_turn_context>"));
    assert!(wire.contains("continue after crash"));
    assert!(
        !tokio::fs::try_exists(&session.paths.provider_rejection_recovery_json)
            .await
            .unwrap()
    );
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(
        projection.turns[0].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );
}

#[tokio::test]
async fn rejection_after_completed_tool_preserves_progress_without_replaying_rejected_request() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_before_rejection",
            "working_note",
            json!({"action": "add", "note": "SIDE_EFFECT_ALREADY_COMPLETED"}),
        ),
        ProviderStep::Rejected {
            message: "provider rejected tool continuation",
        },
        response_step("continued without repeating the tool", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_9e1ec7ee").await;

    let error = engine
        .run_turn(&mut session, "complete one tool before rejection", |_| {})
        .await
        .unwrap_err();
    let rejected = error
        .downcast_ref::<crate::api::ProviderRequestRejected>()
        .expect("the second request must retain the provider rejection classification");
    assert!(!rejected.should_discard_turn());

    let projection = replay_turn_journal(session.read_turn_journal().await);
    let failed_turn = &projection.turns[0];
    assert_eq!(failed_turn.status, Some(TurnJournalStatus::Failed));
    let completed_tool = failed_turn
        .tool_calls
        .iter()
        .find(|tool| tool.tool_use_id == "toolu_before_rejection")
        .expect("completed tool progress must remain in the failed turn journal");
    assert!(completed_tool.completed_summary.is_some());
    assert_eq!(
        completed_tool.outcome,
        Some(crate::api::ToolExecutionOutcome::Completed)
    );

    let first_requests = provider.requests().await;
    assert_eq!(first_requests.len(), 2);
    let rejected_request = first_requests[1].messages.clone();
    let restored = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|state| state.provider_history)
        .expect("the accepted request boundary must remain available for recovery");
    assert_eq!(restored.messages, first_requests[0].messages);
    assert_ne!(restored.messages, rejected_request);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();
    engine
        .run_turn(&mut resumed, "continue after rejected continuation", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(resumed_wire.contains("<interrupted_turn_context>"));
    assert!(
        resumed_wire.contains("tools_completed"),
        "resumed request omitted completed tool recovery: {resumed_wire}"
    );
    assert!(resumed_wire.contains("toolu_before_rejection"));
    assert!(resumed_wire.contains("SIDE_EFFECT_ALREADY_COMPLETED"));
    assert!(!requests[2]
        .messages
        .iter()
        .any(|message| message.content.iter().any(|block| matches!(
            block,
            SessionTurnContentBlock::ToolUse { id, .. }
                | SessionTurnContentBlock::ToolResult { tool_use_id: id, .. }
                if id == "toolu_before_rejection"
        ))));
}

#[tokio::test]
async fn internal_continuation_size_recovery_uses_the_latest_request() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(AcceptedContinuationThenRejectedProvider {
        requests: Mutex::new(Vec::new()),
        reject_media: true,
    });
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0027").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "adjacent raw user one"),
            NewSessionMessage::text(SessionMessageRole::User, "adjacent raw user two"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "earlier answer"),
        ])
        .await
        .unwrap();
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
    let mut bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    let data = BASE64_STANDARD.encode(bytes);
    engine
        .run_turn_with_attachments(
            &mut session,
            "continue with media",
            vec![SessionAttachment::InlineImage {
                media_type: "image/png".into(),
                data: data.clone(),
            }],
            |_| {},
        )
        .await
        .unwrap();
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    let retry = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(retry.contains("kept prefix"));
    assert!(retry.contains("continue"));
    assert!(!retry.contains("ghost suffix"));
    assert!(!retry.contains(&data));
    assert!(retry.contains("<request_size_recovery>"));
}

#[tokio::test]
async fn rejection_discards_only_output_after_the_last_accepted_response() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(AcceptedContinuationThenRejectedProvider {
        requests: Mutex::new(Vec::new()),
        reject_media: false,
    });
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_9e1ec7ef").await;
    let mut events = Vec::new();

    let error = engine
        .run_turn(&mut session, "preserve accepted provider output", |event| {
            events.push(event)
        })
        .await
        .unwrap_err();
    let rejected = error
        .downcast_ref::<crate::api::ProviderRequestRejected>()
        .expect("continuation rejection must remain machine-readable");
    assert!(!rejected.should_discard_turn());

    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    assert_eq!(projection.turns[0].assistant_text, "kept prefix");
    assert!(!projection.turns[0].assistant_text.contains("ghost suffix"));
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::AssistantOutputDiscarded)));

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    let restored = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|state| state.provider_history)
        .expect("accepted request boundary must remain recoverable");
    assert_eq!(restored.messages, requests[0].messages);
}

#[tokio::test]
async fn rejected_turn_preserves_earlier_ambiguous_wal_and_journal_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step("ambiguous transport failure", Vec::new()),
        ProviderStep::Rejected {
            message: "provider rejected only the later request",
        },
        response_step("recovered the ambiguous request", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_a11b1c02").await;

    engine
        .run_turn(&mut session, "AMBIGUOUS_REQUEST_MUST_REMAIN", |_| {})
        .await
        .unwrap_err();
    let ambiguous_request = provider.requests().await[0].messages.clone();

    engine
        .run_turn(
            &mut session,
            "REJECTED_LATER_REQUEST_MUST_DISAPPEAR",
            |_| {},
        )
        .await
        .unwrap_err();

    let metadata = session.read_metadata().await.unwrap();
    let preserved = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("later rejection must restore the earlier ambiguous WAL");
    assert_eq!(preserved.messages, ambiguous_request);
    assert!(preserved.pending_turn.is_none());
    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert_eq!(projection.turns[0].status, Some(TurnJournalStatus::Failed));
    assert_eq!(
        projection.turns[1].status,
        Some(TurnJournalStatus::RejectedByProvider)
    );
    assert_eq!(
        projection
            .unresolved_tail()
            .map(|turn| turn.turn_id.as_str()),
        Some(projection.turns[0].turn_id.as_str())
    );

    let mut compaction = metadata.compaction.unwrap();
    compaction.provider_history = None;
    session.update_compaction(compaction).await.unwrap();
    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut resumed = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .unwrap();

    engine
        .run_turn(
            &mut resumed,
            "recover after provider identity change",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    let resumed_wire = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(resumed_wire.contains("AMBIGUOUS_REQUEST_MUST_REMAIN"));
    assert!(resumed_wire.contains("interrupted_turn_context"));
    assert!(!resumed_wire.contains("REJECTED_LATER_REQUEST_MUST_DISAPPEAR"));
}

#[tokio::test]
async fn failed_internal_continuation_wal_replays_latest_request_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(FailingInternalContinuationProvider::new());
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_c0ffee20").await;

    let error = engine
        .run_turn(
            &mut session,
            "start an internally continued response",
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("internal continuation failed after request write-ahead"));
    let internal_request = provider
        .last_internal_request
        .lock()
        .await
        .clone()
        .expect("provider must report its second internal request");
    let metadata = session.read_metadata().await.unwrap();
    let pending = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("internal continuation request must reach the same WAL");
    assert_eq!(pending.messages, internal_request);
    assert!(pending.pending_turn.is_some());

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut session = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .expect("restart must load the internal continuation WAL");
    engine
        .run_turn(
            &mut session,
            "recover after internal continuation failure",
            |_| {},
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.starts_with(&internal_request));
    let stable = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("successful recovery must promote internal continuation history");
    assert!(stable.pending_turn.is_none());
}

#[tokio::test]
async fn cancelled_turn_after_stable_compaction_replays_write_ahead_provider_window() {
    let dir = tempfile::tempdir().unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::ResponseAndCancel {
            response: provider_response("must not commit after cancellation"),
            events: Vec::new(),
            control,
        },
        response_step("recovered after compacted cancellation", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_c0ffee1b").await;
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: None,
        canonical_message_until: 0,
        messages: vec![SessionTurnMessage::user_text("STABLE_CANCEL_BASELINE")],
    }));
    session.update_compaction(compaction).await.unwrap();

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "cancel after this request reaches the Provider",
            Vec::new(),
            Some(control_rx),
            |_| {},
        )
        .await
        .unwrap();

    assert!(session.read_messages().await.unwrap().is_empty());
    let requests_after_cancel = provider.requests().await;
    assert_eq!(requests_after_cancel.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    let pending_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("cancelled compacted request must retain write-ahead history");
    assert!(pending_history.pending_turn.is_some());
    assert_eq!(pending_history.messages, requests_after_cancel[0].messages);
    assert_eq!(
        pending_history.messages.last(),
        Some(&SessionTurnMessage::user_text(
            "cancel after this request reaches the Provider"
        ))
    );

    engine
        .run_user_shell_command(
            &mut session,
            "printf PENDING_SHELL_MARKER",
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    let canonical_after_shell = session.read_messages().await.unwrap();
    assert_eq!(canonical_after_shell.len(), 1);
    assert!(serde_json::to_string(&canonical_after_shell)
        .unwrap()
        .contains("PENDING_SHELL_MARKER"));

    engine
        .run_turn(&mut session, "recover after cancellation", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.starts_with(&requests[0].messages));
    let recovery_suffix = &requests[1].messages[requests[0].messages.len()..];
    assert_eq!(recovery_suffix.len(), 1);
    let recovery_suffix = serde_json::to_string(recovery_suffix).unwrap();
    assert!(recovery_suffix.contains("recover after cancellation"));
    assert!(recovery_suffix.contains("PENDING_SHELL_MARKER"));
    assert_eq!(recovery_suffix.matches("<user_shell_command>").count(), 1);
    let metadata = session.read_metadata().await.unwrap();
    let history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("successful recovery must promote cancelled write-ahead history");
    assert!(history.pending_turn.is_none());
}

#[tokio::test]
async fn cancelled_uncompacted_turn_replays_write_ahead_provider_window_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::ResponseAndCancel {
            response: provider_response("must not commit ordinary cancelled response"),
            events: Vec::new(),
            control,
        },
        response_step("recovered ordinary cancellation", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    let mut session = create_test_session(&store, "session_c0ffee1f").await;

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "cancel ordinary request after Provider receives it",
            Vec::new(),
            Some(control_rx),
            |_| {},
        )
        .await
        .unwrap();

    assert!(session.read_messages().await.unwrap().is_empty());
    let cancelled_requests = provider.requests().await;
    assert_eq!(cancelled_requests.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    let pending = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("cancelled ordinary request must retain its exact Provider WAL");
    assert!(pending.pending_turn.is_some());
    assert_eq!(pending.messages, cancelled_requests[0].messages);

    let agent_id = session.metadata.agent_id.clone();
    let session_id = session.metadata.id.clone();
    drop(session);
    let mut session = store
        .load_existing_session(&agent_id, &session_id)
        .await
        .expect("restart must load cancelled ordinary Provider history");
    engine
        .run_turn(&mut session, "recover ordinary cancelled request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.starts_with(&requests[0].messages));
    let stable = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|compaction| compaction.provider_history)
        .expect("successful recovery must promote cancelled ordinary WAL");
    assert!(stable.pending_turn.is_none());
}

#[tokio::test]
async fn preflight_compaction_summarizes_oversized_previous_turn_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary": "older committed context summarized", "active_turn_summary": null}"#,
            ],
            &[],
        ),
    ]));
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
                provider_messages: &[],
                active_suffix: vec![SessionTurnMessage::user_text("small current request")],
                turn_id: "turn_1",
                base_message_count: 4,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
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
    assert_eq!(metadata.recapped_until, 0);
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionTurnEvent::CompactionStarted { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionTurnEvent::CompactionCompleted { .. })));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionTurnEvent::RecapRequested {
            recap_end_index: 4,
            ..
        }
    )));
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
                provider_messages: &[],
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 0,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
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
                provider_messages: &[],
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 0,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
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
        json_by_request_kind_responses(&[&response, &response], &[]),
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
    let compaction = metadata
        .compaction
        .as_ref()
        .expect("successful raw Provider request must retain the ordinary stable window");
    assert!(compaction.committed_summary.is_empty());
    let history = compaction.provider_history.as_ref().unwrap();
    assert!(history.pending_turn.is_none());
    assert_eq!(history.canonical_message_until, metadata.message_count);
    assert!(history.messages.starts_with(&requests[2].messages));
    assert!(session
        .read_compaction_checkpoint()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn auto_compaction_provider_failure_continues_with_raw_history_when_request_still_fits() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(SummaryFailureWithRecapProvider::new());
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

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
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
    let compaction = metadata
        .compaction
        .as_ref()
        .expect("successful raw Provider request must retain the ordinary stable window");
    assert!(compaction.committed_summary.is_empty());
    let history = compaction.provider_history.as_ref().unwrap();
    assert!(history.pending_turn.is_none());
    assert_eq!(history.canonical_message_until, metadata.message_count);
    assert!(history.messages.starts_with(&requests[1].messages));
}

#[tokio::test]
async fn auto_compaction_projection_failure_continues_raw_when_full_request_still_fits() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":"short summary","active_turn_summary":null}"#,
                r#"{"committed_summary":"tiny","active_turn_summary":null}"#,
            ],
            &[],
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
    assert_eq!(requests.len(), 3);
    let final_request = serde_json::to_string(&requests[2].messages).unwrap();
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
    let compaction = metadata
        .compaction
        .as_ref()
        .expect("successful raw Provider request must retain the ordinary stable window");
    assert!(compaction.committed_summary.is_empty());
    let history = compaction.provider_history.as_ref().unwrap();
    assert!(history.pending_turn.is_none());
    assert_eq!(history.canonical_message_until, metadata.message_count);
    assert!(history.messages.starts_with(&requests[2].messages));
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
        json_by_request_kind_responses(&[&response, &response], &[]),
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
                provider_messages: &[],
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 0,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
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

#[tokio::test]
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
async fn provider_terminal_failure_preserves_request_wal_for_resume() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::TerminalFailure {
            message: "unknown upstream terminal",
        },
        response_step("resumed from preserved request", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_bbbbbbb3").await;

    let error = engine
        .run_turn(&mut session, "REQUEST_MUST_REMAIN_IN_WAL", |_| {})
        .await
        .unwrap_err();
    assert!(error
        .downcast_ref::<crate::api::ProviderTerminalFailure>()
        .is_some());
    let first_request = provider.requests().await[0].messages.clone();
    let metadata = session.read_metadata().await.unwrap();
    let persisted = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .expect("terminal failure must preserve Provider request WAL");
    assert_eq!(persisted.messages, first_request);
    assert!(!session
        .read_turn_journal()
        .await
        .events
        .iter()
        .any(|event| matches!(
            event.kind,
            TurnJournalEventKind::ProviderRequestRejected { .. }
        )));

    engine
        .run_turn(&mut session, "continue after upstream recovery", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.starts_with(&first_request));
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

#[tokio::test]
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
        Duration::from_secs(5),
        provider.second_call_started.notified(),
    )
    .await
    .expect("provider should block after file_read");
    turn.abort();
    assert!(turn.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(5), async {
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
async fn preserved_max_token_partial_commits_before_pending_safe_steer() {
    let dir = tempfile::tempdir().unwrap();
    let (control, control_rx) = SessionTurnControl::channel();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::ResponseAndPreservedSteer {
            response: provider_response("successful partial"),
            events: Vec::new(),
            control,
        },
    ]));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_bbbbbbc4").await;

    engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "original request",
            Vec::new(),
            Some(control_rx),
            |_| {},
        )
        .await
        .unwrap();

    let messages = session.read_messages().await.unwrap();
    assert!(messages.iter().any(|message| {
        message.role == SessionMessageRole::Assistant
            && message
                .content
                .iter()
                .any(|block| matches!(block, SessionContentBlock::Text { text } if text == "successful partial"))
    }));
    let projection = replay_turn_journal(session.read_turn_journal().await);
    let turn = projection.turns.last().unwrap();
    assert_eq!(turn.status, Some(TurnJournalStatus::Committed));
    assert_eq!(
        turn.user_steers,
        vec!["steer after max-token partial".to_string()]
    );
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

#[tokio::test]
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
    let conversation = non_context_session_messages(&messages);
    assert_eq!(conversation.len(), 2);
    assert_eq!(text_content(conversation[1]), "complete replacement");
    assert!(!text_content(conversation[1]).contains("partial output"));
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

// 该用例会断言专用 OS journal writer 的真实落盘结果，不能使用会瞬间推进
// cancellation grace 的 Tokio 虚拟时钟。
#[tokio::test]
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
async fn finalize_recaps_background_completion_after_messages_were_already_recapped() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0004").await;
    let user_content = vec![SessionContentBlock::text("start background job")];
    let canonical_hash = canonical_user_content_hash(&user_content).unwrap();
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                user_content,
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("job is running")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    session.advance_recapped_until(2).await.unwrap();

    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::CanonicalUserMessage {
            content_hash: Some(canonical_hash),
            content: None,
        },
        TurnJournalEventKind::ToolCallStarted {
            tool_use_id: "toolu_late_completion".into(),
            name: "code_run".into(),
            summary: "tool code_run".into(),
            input_preview: String::new(),
            input_truncated: false,
        },
        TurnJournalEventKind::ToolCallCompleted {
            tool_use_id: "toolu_late_completion".into(),
            summary: "tool code_run process_running".into(),
            outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
            output_preview: String::new(),
            output_truncated: false,
            file_change: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::Committed,
        },
        TurnJournalEventKind::BackgroundProcessCompleted {
            tool_use_id: "toolu_late_completion".into(),
            process_id: "deadbeef".into(),
            instance_id: 7,
            status: "finished".into(),
            exit_code: Some(7),
            signal: None,
            success: false,
        },
    ] {
        writer
            .append("turn_1", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(report.finalized_unrecapped_messages);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let recap_payload = last_user_text(&requests[0]);
    assert!(recap_payload.contains(r#""tool_use_id": "toolu_late_completion""#));
    assert!(recap_payload.contains(r#""exit_code": 7"#));
    let previous_checkpoint = session.read_finalize_checkpoint().await.unwrap().unwrap();
    let previous_hash = previous_checkpoint.recap_segment_hash.clone();

    session.mark_open(Utc::now()).await.unwrap();
    assert!(session.read_finalize_checkpoint().await.unwrap().is_none());
    // 模拟修复前已经 resume、仍遗留上一生命周期 Applied checkpoint 的 session。
    session
        .write_finalize_checkpoint(&previous_checkpoint)
        .await
        .unwrap();
    let repeated = engine
        .session_recap_background_process_completions(&session)
        .await
        .unwrap();
    assert!(repeated.items.is_empty());

    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_new_completion".into(),
                process_id: "feedface".into(),
                instance_id: 8,
                status: "finished".into(),
                exit_code: Some(0),
                signal: None,
                success: true,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let new_only = engine
        .session_recap_background_process_completions(&session)
        .await
        .unwrap();
    assert_eq!(new_only.items.len(), 1);
    assert_eq!(new_only.items[0].process_id, "feedface");

    let second_report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(second_report.finalized_unrecapped_messages);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    let second_payload = last_user_text(&requests[1]);
    assert!(second_payload.contains(r#""process_id": "feedface""#));
    assert!(!second_payload.contains(r#""process_id": "deadbeef""#));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(
        metadata.recap_background_completion_until_seq,
        Some(session.latest_background_completion_seq().await)
    );
    let current_checkpoint = session.read_finalize_checkpoint().await.unwrap().unwrap();
    assert_eq!(current_checkpoint.status, FinalizeCheckpointStatus::Applied);
    assert_ne!(current_checkpoint.recap_segment_hash, previous_hash);
}

#[tokio::test]
async fn legacy_prepared_finalize_checkpoint_recovers_before_completion_cursor_migration() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0006").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("legacy request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("legacy answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_legacy",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_legacy_prepared".into(),
                process_id: "legacyproc2".into(),
                instance_id: 12,
                status: "finished".into(),
                exit_code: Some(0),
                signal: None,
                success: true,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let completion_tail = session.latest_background_completion_seq().await;
    let messages = session.read_messages().await.unwrap();
    let background = engine
        .legacy_session_recap_background_process_completions(&session)
        .await;
    let checkpoint_hash =
        super::finalize::hash_finalize_recap_input(&messages, &background).unwrap();
    let mut metadata = session.read_metadata().await.unwrap();
    metadata.status = SessionStatus::Finalizing;
    metadata.provider_background_completion_until_seq = None;
    metadata.recap_background_completion_until_seq = None;
    write_yaml_atomic(&session.paths.session_yaml, &metadata)
        .await
        .unwrap();
    session
        .write_finalize_checkpoint(&FinalizeCheckpoint {
            recap_start_index: 0,
            recap_end_index: 2,
            recap_segment_hash: checkpoint_hash,
            prepared_claims: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: Vec::new(),
            trace_text: "legacy frozen trace".into(),
            trace_created_at: Utc::now(),
            trace_id: None,
            status: FinalizeCheckpointStatus::Prepared,
        })
        .await
        .unwrap();

    let mut loaded = store
        .load_existing_session(&metadata.agent_id, &metadata.id)
        .await
        .unwrap();
    assert_eq!(loaded.metadata.recap_background_completion_until_seq, None);
    let report = engine.finalize_session(&mut loaded, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    assert!(provider.requests().await.is_empty());
    let recovered = loaded.read_metadata().await.unwrap();
    assert_eq!(recovered.status, SessionStatus::Closed);
    assert_eq!(recovered.recapped_until, 2);
    assert_eq!(
        recovered.recap_background_completion_until_seq,
        Some(completion_tail)
    );
    assert_eq!(
        loaded
            .read_finalize_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .status,
        FinalizeCheckpointStatus::Applied
    );
}

#[tokio::test]
async fn legacy_applied_completion_only_checkpoint_closes_without_llm_retry() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0007").await;
    let now = Utc::now();
    let user_content = vec![SessionContentBlock::text("already recapped request")];
    let canonical_hash = canonical_user_content_hash(&user_content).unwrap();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                user_content,
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("already recapped answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    session.advance_recapped_until(2).await.unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::CanonicalUserMessage {
            content_hash: Some(canonical_hash),
            content: None,
        },
        TurnJournalEventKind::ToolCallStarted {
            tool_use_id: "toolu_legacy_applied".into(),
            name: "code_run".into(),
            summary: "tool code_run".into(),
            input_preview: String::new(),
            input_truncated: false,
        },
        TurnJournalEventKind::ToolCallCompleted {
            tool_use_id: "toolu_legacy_applied".into(),
            summary: "tool code_run process_running".into(),
            outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
            output_preview: String::new(),
            output_truncated: false,
            file_change: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::Committed,
        },
        TurnJournalEventKind::BackgroundProcessCompleted {
            tool_use_id: "toolu_legacy_applied".into(),
            process_id: "legacyproc3".into(),
            instance_id: 13,
            status: "finished".into(),
            exit_code: Some(0),
            signal: None,
            success: true,
        },
    ] {
        writer
            .append("turn_legacy", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);
    let completion_tail = session.latest_background_completion_seq().await;
    let messages = session.read_messages().await.unwrap();
    let background = engine
        .legacy_session_recap_background_process_completions(&session)
        .await;
    let checkpoint_hash =
        super::finalize::hash_finalize_recap_input(&messages[2..2], &background).unwrap();
    let mut metadata = session.read_metadata().await.unwrap();
    metadata.status = SessionStatus::Finalizing;
    metadata.provider_background_completion_until_seq = None;
    metadata.recap_background_completion_until_seq = None;
    write_yaml_atomic(&session.paths.session_yaml, &metadata)
        .await
        .unwrap();
    session
        .write_finalize_checkpoint(&FinalizeCheckpoint {
            recap_start_index: 2,
            recap_end_index: 2,
            recap_segment_hash: checkpoint_hash,
            prepared_claims: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: Vec::new(),
            trace_text: "legacy completion-only trace".into(),
            trace_created_at: Utc::now(),
            trace_id: None,
            status: FinalizeCheckpointStatus::Applied,
        })
        .await
        .unwrap();

    let mut loaded = store
        .load_existing_session(&metadata.agent_id, &metadata.id)
        .await
        .unwrap();
    let report = engine.finalize_session(&mut loaded, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    assert!(provider.requests().await.is_empty());
    let recovered = loaded.read_metadata().await.unwrap();
    assert_eq!(recovered.status, SessionStatus::Closed);
    assert_eq!(recovered.recapped_until, 2);
    assert_eq!(
        recovered.recap_background_completion_until_seq,
        Some(completion_tail)
    );
}

#[tokio::test]
async fn legacy_stale_finalize_checkpoint_is_discarded_before_recapping_new_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0009").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("old lifecycle request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("old lifecycle answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let old_messages = session.read_messages().await.unwrap();
    let empty_background = SessionRecapBackgroundProcessProjection {
        consumed_through_seq: 0,
        omitted_older_count: 0,
        items: Vec::new(),
    };
    let old_hash =
        super::finalize::hash_finalize_recap_input(&old_messages, &empty_background).unwrap();
    session.advance_recapped_until(2).await.unwrap();
    session
        .write_finalize_checkpoint(&FinalizeCheckpoint {
            recap_start_index: 0,
            recap_end_index: 2,
            recap_segment_hash: old_hash,
            prepared_claims: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: Vec::new(),
            trace_text: "old lifecycle trace".into(),
            trace_created_at: Utc::now(),
            trace_id: None,
            status: FinalizeCheckpointStatus::Applied,
        })
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_old",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_old_completion".into(),
                process_id: "oldproc1".into(),
                instance_id: 15,
                status: "finished".into(),
                exit_code: Some(0),
                signal: None,
                success: true,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let completion_tail = session.latest_background_completion_seq().await;
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("new lifecycle request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("new lifecycle answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    let mut metadata = session.read_metadata().await.unwrap();
    metadata.status = SessionStatus::Finalizing;
    metadata.provider_background_completion_until_seq = None;
    metadata.recap_background_completion_until_seq = None;
    write_yaml_atomic(&session.paths.session_yaml, &metadata)
        .await
        .unwrap();

    let mut loaded = store
        .load_existing_session(&metadata.agent_id, &metadata.id)
        .await
        .unwrap();
    let report = engine.finalize_session(&mut loaded, |_| {}).await.unwrap();

    assert!(report.advanced_recapped_until);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(payload.contains("new lifecycle request"));
    assert!(!payload.contains("old lifecycle request"));
    assert!(!payload.contains("oldproc1"));
    let recovered = loaded.read_metadata().await.unwrap();
    assert_eq!(recovered.status, SessionStatus::Closed);
    assert_eq!(recovered.recapped_until, 4);
    assert_eq!(
        recovered.recap_background_completion_until_seq,
        Some(completion_tail)
    );
    let current_checkpoint = loaded.read_finalize_checkpoint().await.unwrap().unwrap();
    assert_eq!(current_checkpoint.recap_start_index, 2);
    assert_eq!(current_checkpoint.recap_end_index, 4);
    assert_eq!(current_checkpoint.status, FinalizeCheckpointStatus::Applied);
}

#[tokio::test]
async fn legacy_same_range_checkpoint_with_new_completion_is_discarded_without_llm() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0010").await;
    let now = Utc::now();
    let user_content = vec![SessionContentBlock::text("already recapped request")];
    let canonical_hash = canonical_user_content_hash(&user_content).unwrap();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                user_content,
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("already recapped answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    session.advance_recapped_until(2).await.unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::CanonicalUserMessage {
            content_hash: Some(canonical_hash),
            content: None,
        },
        TurnJournalEventKind::ToolCallStarted {
            tool_use_id: "toolu_first_completion".into(),
            name: "code_run".into(),
            summary: "tool code_run".into(),
            input_preview: String::new(),
            input_truncated: false,
        },
        TurnJournalEventKind::ToolCallCompleted {
            tool_use_id: "toolu_first_completion".into(),
            summary: "tool code_run process_running".into(),
            outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
            output_preview: String::new(),
            output_truncated: false,
            file_change: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::Committed,
        },
        TurnJournalEventKind::BackgroundProcessCompleted {
            tool_use_id: "toolu_first_completion".into(),
            process_id: "legacyproc4".into(),
            instance_id: 16,
            status: "finished".into(),
            exit_code: Some(0),
            signal: None,
            success: true,
        },
    ] {
        writer
            .append("turn_old", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);
    let messages = session.read_messages().await.unwrap();
    let first_background = engine
        .legacy_session_recap_background_process_completions(&session)
        .await;
    let old_hash =
        super::finalize::hash_finalize_recap_input(&messages[2..2], &first_background).unwrap();
    session
        .write_finalize_checkpoint(&FinalizeCheckpoint {
            recap_start_index: 2,
            recap_end_index: 2,
            recap_segment_hash: old_hash,
            prepared_claims: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: Vec::new(),
            trace_text: "old completion-only trace".into(),
            trace_created_at: Utc::now(),
            trace_id: None,
            status: FinalizeCheckpointStatus::Applied,
        })
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_old",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_first_completion".into(),
                process_id: "legacyproc5".into(),
                instance_id: 17,
                status: "failed".into(),
                exit_code: Some(7),
                signal: None,
                success: false,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let completion_tail = session.latest_background_completion_seq().await;
    let mut metadata = session.read_metadata().await.unwrap();
    metadata.status = SessionStatus::Finalizing;
    metadata.provider_background_completion_until_seq = None;
    metadata.recap_background_completion_until_seq = None;
    write_yaml_atomic(&session.paths.session_yaml, &metadata)
        .await
        .unwrap();

    let mut loaded = store
        .load_existing_session(&metadata.agent_id, &metadata.id)
        .await
        .unwrap();
    let report = engine.finalize_session(&mut loaded, |_| {}).await.unwrap();

    assert!(!report.finalized_unrecapped_messages);
    assert!(provider.requests().await.is_empty());
    assert!(loaded.read_finalize_checkpoint().await.unwrap().is_none());
    let recovered = loaded.read_metadata().await.unwrap();
    assert_eq!(recovered.status, SessionStatus::Closed);
    assert_eq!(recovered.recapped_until, 2);
    assert_eq!(
        recovered.recap_background_completion_until_seq,
        Some(completion_tail)
    );
}

#[tokio::test]
async fn prepared_finalize_checkpoint_hash_mismatch_is_not_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0008").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("prepared request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("prepared answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    session.advance_recapped_until(2).await.unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_prepared",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_prepared".into(),
                process_id: "preparedproc1".into(),
                instance_id: 14,
                status: "finished".into(),
                exit_code: Some(0),
                signal: None,
                success: true,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let checkpoint = FinalizeCheckpoint {
        recap_start_index: 2,
        recap_end_index: 2,
        recap_segment_hash: "different-prepared-hash".into(),
        prepared_claims: Vec::new(),
        prepared_disputes: Vec::new(),
        used_claim_ids: Vec::new(),
        trace_text: "prepared frozen trace".into(),
        trace_created_at: Utc::now(),
        trace_id: None,
        status: FinalizeCheckpointStatus::Prepared,
    };
    session
        .write_finalize_checkpoint(&checkpoint)
        .await
        .unwrap();

    let error = engine
        .finalize_session(&mut session, |_| {})
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("session finalize checkpoint recap_segment_hash 不匹配"));
    assert_eq!(
        session.read_finalize_checkpoint().await.unwrap(),
        Some(checkpoint)
    );
    assert!(provider.requests().await.is_empty());
    assert_eq!(
        session.read_metadata().await.unwrap().status,
        SessionStatus::Finalizing
    );
}

#[tokio::test]
async fn finalize_recaps_failed_turn_background_completion_without_private_request() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0005").await;
    let now = Utc::now();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("committed request")],
                now,
                "test-model",
            ),
            NewSessionMessage::with_created_at_and_model(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("committed answer")],
                now,
                "test-model",
            ),
        ])
        .await
        .unwrap();
    session.advance_recapped_until(2).await.unwrap();

    let failed_content = vec![SessionContentBlock::text("journal-only private request")];
    let failed_hash = canonical_user_content_hash(&failed_content).unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    for kind in [
        TurnJournalEventKind::TurnStarted,
        TurnJournalEventKind::CanonicalUserMessage {
            content_hash: Some(failed_hash),
            content: None,
        },
        TurnJournalEventKind::ToolCallStarted {
            tool_use_id: "toolu_private".into(),
            name: "code_run".into(),
            summary: "tool code_run".into(),
            input_preview: String::new(),
            input_truncated: false,
        },
        TurnJournalEventKind::ToolCallCompleted {
            tool_use_id: "toolu_private".into(),
            summary: "tool code_run process_running".into(),
            outcome: Some(crate::api::ToolExecutionOutcome::ProcessRunning),
            output_preview: String::new(),
            output_truncated: false,
            file_change: None,
        },
        TurnJournalEventKind::TurnFinished {
            status: TurnJournalStatus::Failed,
        },
        TurnJournalEventKind::BackgroundProcessCompleted {
            tool_use_id: "toolu_private".into(),
            process_id: "private1".into(),
            instance_id: 8,
            status: "finished".into(),
            exit_code: Some(0),
            signal: None,
            success: true,
        },
    ] {
        writer
            .append("turn_failed", Utc::now(), kind, TurnJournalFlush::Immediate)
            .await
            .unwrap();
    }
    drop(writer);

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(report.finalized_unrecapped_messages);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let recap_payload = last_user_text(&requests[0]);
    assert!(recap_payload.contains(r#""process_id": "private1""#));
    assert!(!recap_payload.contains("journal-only private request"));
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
async fn finalize_recovered_summary_checkpoint_still_runs_recap() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims": [], "used_claim_ids": [], "new_disputes": []}"#,
        Vec::new(),
    )]));
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
        summary_end_index: messages.len(),
        summary_segment_hash: super::hash_session_segment(&messages).unwrap(),
        summary: "checkpointed summary".into(),
        active_turn_summary: Some("stale active summary".into()),
        active_turn: Some(ActiveTurnCompactionCursor {
            turn_id: "turn_1".into(),
            base_message_count: messages.len(),
            compacted_until_segment: 1,
            safe_until_event_seq: 10,
            source_hash: "stale_hash".into(),
        }),
        preserve_provider_history: false,
        status: CompactionCheckpointStatus::Applied,
    };
    session
        .write_compaction_checkpoint(&checkpoint)
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
async fn manual_compact_after_rejection_survives_the_next_turn() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::Rejected {
            message: "invalid request",
        },
        json_by_request_kind_responses(
            &[r#"{"committed_summary":"retained manual summary","active_turn_summary":null}"#],
            &[],
        ),
        response_step("continued", Vec::new()),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.context_window = 20_000;
    engine.compaction.tail_target_ctx_ratio = 0.015;
    engine.compaction.tail_hard_ctx_ratio = 0.0225;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_face0028").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "old request ".repeat(120)),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "OLD_UNCOMPACTED_ANSWER ".repeat(120),
            ),
            NewSessionMessage::text(SessionMessageRole::User, "latest request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "latest answer"),
        ])
        .await
        .unwrap();
    engine
        .run_turn(&mut session, "REJECTED_PROMPT", |_| {})
        .await
        .unwrap_err();
    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    assert_eq!(
        session
            .read_metadata()
            .await
            .unwrap()
            .compaction
            .unwrap()
            .committed_summary,
        "retained manual summary"
    );
    engine
        .run_turn(&mut session, "next prompt", |_| {})
        .await
        .unwrap();
    let requests = provider.requests().await;
    let next_request = requests.last().unwrap();
    let next = format!(
        "{}{}",
        next_request.system_prompt,
        serde_json::to_string(&next_request.messages).unwrap()
    );
    assert!(next.contains("retained manual summary"));
    assert!(!next.contains("OLD_UNCOMPACTED_ANSWER"));
    assert!(!next.contains("REJECTED_PROMPT"));
}

#[tokio::test]
async fn rejected_fallback_continuation_preserves_accepted_output_across_adapters() {
    use crate::api::{
        AnthropicProviderAdapter, OpenAiCompatibleChatProviderAdapter,
        OpenAiCompatibleResponsesProviderAdapter,
    };
    use axum::response::IntoResponse;
    const ACCEPTED: &str = "accepted fallback prefix";
    for protocol in 0..3 {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let requests = captured.clone();
        let app = axum::Router::new().fallback(move |axum::Json(body): axum::Json<serde_json::Value>| {
            let requests = requests.clone();
            async move {
                let mut requests = requests.lock().await;
                let index = requests.len();
                requests.push(body);
                if index == 0 {
                    let delta = match protocol {
                        0 => json!({"type":"response.output_text.delta","item_id":"msg_partial","output_index":0,"content_index":0,"delta":"unconfirmed stream fragment"}),
                        1 => json!({"choices":[{"index":0,"delta":{"role":"assistant","content":"unconfirmed stream fragment"},"finish_reason":null}]}),
                        _ => json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"unconfirmed stream fragment"}}),
                    };
                    return ([("content-type", "text/event-stream")], format!("data: {delta}\n\n")).into_response();
                }
                if index == 1 {
                    let partial = match protocol {
                        0 => json!({"id":"resp_partial","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","id":"msg_partial","role":"assistant","status":"completed","content":[{"type":"output_text","text":ACCEPTED}]}]}),
                        1 => json!({"choices":[{"message":{"role":"assistant","content":ACCEPTED},"finish_reason":"length"}]}),
                        _ => json!({"id":"msg_partial","type":"message","role":"assistant","content":[{"type":"text","text":ACCEPTED}],"stop_reason":"max_tokens"}),
                    };
                    return axum::Json(partial).into_response();
                }
                (axum::http::StatusCode::BAD_REQUEST, axum::Json(json!({"error":{"type":"invalid_request_error","message":"continuation rejected"}}))).into_response()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let timeout = Duration::from_secs(5);
        let provider: Arc<dyn ProviderAdapter> = match protocol {
            0 => Arc::new(
                OpenAiCompatibleResponsesProviderAdapter::new(
                    "test-key".into(),
                    endpoint,
                    "test-model".into(),
                    timeout,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                )
                .unwrap(),
            ),
            1 => Arc::new(
                OpenAiCompatibleChatProviderAdapter::new(
                    "test-key".into(),
                    endpoint,
                    "test-model".into(),
                    timeout,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                )
                .unwrap(),
            ),
            _ => Arc::new(
                AnthropicProviderAdapter::new(
                    "test-key".into(),
                    endpoint,
                    "test-model".into(),
                    128,
                    timeout,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                )
                .unwrap(),
            ),
        };
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, store) = build_test_engine(&dir, provider);
        engine.compaction.auto_compact_ctx_ratio = 0.0;
        let mut session = create_test_session(&store, "session_face0029").await;
        let error = engine
            .run_turn(&mut session, "finish the answer", |_| {})
            .await
            .unwrap_err();
        server.abort();
        assert!(
            error.downcast_ref::<ProviderRequestRejected>().is_some(),
            "protocol={protocol}: {error:#}"
        );
        assert_eq!(captured.lock().await.len(), 3, "protocol={protocol}");
        let mut compaction = session.read_metadata().await.unwrap().compaction.unwrap();
        let history = compaction.provider_history.as_ref().unwrap();
        assert_eq!(
            history
                .pending_turn
                .as_ref()
                .unwrap()
                .provider_request_message_count,
            Some(history.messages.len())
        );
        assert!(
            serde_json::to_string(&history.messages)
                .unwrap()
                .contains(ACCEPTED),
            "protocol={protocol}: accepted response missing from rollback WAL"
        );
        let journal = replay_turn_journal(session.read_turn_journal().await);
        assert_eq!(
            journal.turns[0].assistant_text, ACCEPTED,
            "protocol={protocol}"
        );
        assert_eq!(journal.turns[0].status, Some(TurnJournalStatus::Failed));
        // 重建不能依赖当前模型的私有 replay，也不能靠残留 WAL 掩盖 journal 缺口。
        let journal_read = session.read_turn_journal().await;
        engine
            .recover_provider_rejection(&mut session, &journal_read)
            .await
            .unwrap();
        compaction.provider_history = None;
        session.update_compaction(compaction).await.unwrap();
        let resumed_provider = Arc::new(RecordingProvider::new(vec![response_step(
            "continued",
            Vec::new(),
        )]));
        let (resumed_engine, _) = build_test_engine(&dir, resumed_provider.clone());
        resumed_engine
            .run_turn(&mut session, "continue", |_| {})
            .await
            .unwrap();
        assert!(
            serde_json::to_string(&resumed_provider.requests().await[0].messages)
                .unwrap()
                .contains(ACCEPTED)
        );
    }
}

#[tokio::test]
async fn manual_compact_summarizes_pending_failed_turn_without_changing_authorities() {
    const FULL_ONLY_MARKER: &str = "FULL-PENDING-WAL-ONLY-MARKER";
    const RECOVERY_SUMMARY: &str = "failed file read summarized for recovery";
    const LATEST_PARTIAL: &str = "latest provider partial after the compactable request";
    const POST_COMPACT_CANONICAL_MARKER: &str = "POST-COMPACT-CANONICAL-MARKER";

    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("large-note.txt"),
        format!("{}{FULL_ONLY_MARKER}", "x".repeat(12_000)),
    )
    .await
    .unwrap();
    let mut provider_steps = vec![tool_use_step(
        "toolu_manual_recovery",
        "file_read",
        json!({"path": "large-note.txt", "show_linenos": false}),
    )];
    provider_steps.extend(exhausted_stream_failure_steps(
        "provider failed after file read",
        LATEST_PARTIAL,
    ));
    provider_steps.extend([
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":null,"active_turn_summary":"failed file read summarized for recovery"}"#,
            ],
            &[],
        ),
        response_step("continued from compacted recovery", Vec::new()),
    ]);
    let provider = Arc::new(RecordingProvider::new(provider_steps));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (mut engine, store) =
        build_test_engine_with_tools(&dir, provider.clone(), Arc::clone(&tools));
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_face0019").await;

    engine
        .run_turn(&mut session, "read the large note and continue", |_| {})
        .await
        .expect_err("the provider must fail after receiving the full tool result");
    assert!(session.read_messages().await.unwrap().is_empty());
    let before = session.read_metadata().await.unwrap().compaction.unwrap();
    let provider_history_before = before.provider_history.clone().unwrap();
    assert!(provider_history_before.pending_turn.is_some());
    assert!(serde_json::to_string(&provider_history_before.messages)
        .unwrap()
        .contains(FULL_ONLY_MARKER));

    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    let after = session.read_metadata().await.unwrap().compaction.unwrap();
    assert!(after.committed_summary.is_empty());
    assert_eq!(after.frontier.committed_message_until, 0);
    assert_eq!(after.active_turn_summary.as_deref(), Some(RECOVERY_SUMMARY));
    assert!(after.frontier.active_turn.is_some());
    let compacted_wal = serde_json::to_string(&after.provider_history.unwrap()).unwrap();
    assert!(compacted_wal.contains(RECOVERY_SUMMARY));
    assert!(!compacted_wal.contains(FULL_ONLY_MARKER));
    assert!(!compacted_wal.contains(LATEST_PARTIAL));
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "record local follow-up"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                format!("{}{POST_COMPACT_CANONICAL_MARKER}", "z".repeat(12_000)),
            ),
        ])
        .await
        .unwrap();
    let requests_after_first_compact = provider.requests().await.len();
    engine
        .compact_session_checkpoint(&mut session, |_| {})
        .await
        .unwrap();
    assert_eq!(
        provider.requests().await.len(),
        requests_after_first_compact
    );
    let after_second_compact = session.read_metadata().await.unwrap().compaction.unwrap();
    assert_eq!(
        after_second_compact.active_turn_summary.as_deref(),
        Some(RECOVERY_SUMMARY)
    );
    assert!(after_second_compact.frontier.active_turn.is_some());
    assert!(
        serde_json::to_string(&after_second_compact.provider_history)
            .unwrap()
            .contains(RECOVERY_SUMMARY)
    );

    engine
        .run_turn(&mut session, "continue from the recovered state", |_| {})
        .await
        .unwrap();
    let requests = provider.requests().await;
    let continuation = requests.last().unwrap();
    let continuation_json = serde_json::to_string(&continuation.messages).unwrap();
    assert!(continuation_json.contains(RECOVERY_SUMMARY));
    assert!(continuation_json.contains(LATEST_PARTIAL));
    assert!(continuation_json.contains(POST_COMPACT_CANONICAL_MARKER));
    assert!(!continuation_json.contains(FULL_ONLY_MARKER));
    let final_state = session.read_metadata().await.unwrap().compaction.unwrap();
    assert!(final_state.committed_summary.is_empty());
    assert_eq!(final_state.frontier.committed_message_until, 0);
    assert!(final_state.active_turn_summary.is_none());
    assert!(final_state.frontier.active_turn.is_none());
    assert!(final_state
        .provider_history
        .as_ref()
        .is_some_and(|history| history.recovery_turn_id.is_none()));
}

#[tokio::test]
async fn manual_compact_projects_committed_and_failed_turn_summaries_together() {
    const COMMITTED_MARKER: &str = "FULL-COMMITTED-HISTORY-MARKER";
    const ACTIVE_MARKER: &str = "FULL-ACTIVE-RECOVERY-MARKER";
    const COMMITTED_SUMMARY: &str = "earlier committed work summarized";
    const ACTIVE_SUMMARY: &str = "failed active work summarized";
    const POST_FAILURE_MARKER: &str = "POST-FAILURE-CANONICAL-MARKER";
    const NEXT_REQUEST: &str = "continue from the mixed recovered state";

    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("active-note.txt"),
        format!("{}{ACTIVE_MARKER}", "a".repeat(12_000)),
    )
    .await
    .unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "toolu_manual_mixed",
            "file_read",
            json!({"path": "active-note.txt", "show_linenos": false}),
        ),
        error_step("provider failed after mixed history", Vec::new()),
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":"earlier committed work summarized","active_turn_summary":"failed active work summarized"}"#,
            ],
            &[],
        ),
        response_step("continued from both summaries", Vec::new()),
    ]));
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let (mut engine, store) =
        build_test_engine_with_tools(&dir, provider.clone(), Arc::clone(&tools));
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_face0021").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "earlier request"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                format!("{}{COMMITTED_MARKER}", "c".repeat(12_000)),
            ),
        ])
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "read the active note and continue", |_| {})
        .await
        .expect_err("the second main request must fail");
    let provider_history_before = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .as_ref()
        .and_then(|state| state.provider_history.clone())
        .expect("failed turn must retain its exact request WAL");
    let raw_wal = serde_json::to_string(&provider_history_before).unwrap();
    assert!(raw_wal.contains(COMMITTED_MARKER));
    assert!(raw_wal.contains(ACTIVE_MARKER));
    session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            POST_FAILURE_MARKER,
        )])
        .await
        .unwrap();

    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    let after = session.read_metadata().await.unwrap().compaction.unwrap();
    assert_eq!(after.committed_summary, COMMITTED_SUMMARY);
    assert_eq!(after.frontier.committed_message_until, 2);
    assert_eq!(after.active_turn_summary.as_deref(), Some(ACTIVE_SUMMARY));
    assert!(after.frontier.active_turn.is_some());
    let compacted_wal = serde_json::to_string(&after.provider_history.unwrap()).unwrap();
    assert!(compacted_wal.contains(COMMITTED_SUMMARY));
    assert!(compacted_wal.contains(ACTIVE_SUMMARY));
    assert!(!compacted_wal.contains(COMMITTED_MARKER));
    assert!(!compacted_wal.contains(ACTIVE_MARKER));

    let requests = provider.requests().await;
    let summary_request = requests
        .iter()
        .find(|request| request.system_prompt.contains("committed_summary"))
        .expect("manual mixed compact must call the shared summarizer");
    let summary_json = serde_json::to_string(&summary_request.messages).unwrap();
    assert!(summary_json.contains(COMMITTED_MARKER));
    assert!(summary_json.contains(ACTIVE_MARKER));

    engine
        .run_turn(&mut session, NEXT_REQUEST, |_| {})
        .await
        .unwrap();
    let requests = provider.requests().await;
    let continuation_json = serde_json::to_string(&requests.last().unwrap().messages).unwrap();
    assert!(continuation_json.contains(COMMITTED_SUMMARY));
    assert!(continuation_json.contains(ACTIVE_SUMMARY));
    assert!(!continuation_json.contains(COMMITTED_MARKER));
    assert!(!continuation_json.contains(ACTIVE_MARKER));
    let summary_at = continuation_json.find(ACTIVE_SUMMARY).unwrap();
    let post_failure_at = continuation_json.find(POST_FAILURE_MARKER).unwrap();
    let next_request_at = continuation_json.rfind(NEXT_REQUEST).unwrap();
    assert!(summary_at < post_failure_at);
    assert!(post_failure_at < next_request_at);
}

#[tokio::test]
async fn manual_compact_does_not_duplicate_canonical_records_between_failed_turns() {
    const FIRST_REQUEST: &str = "first failed request";
    const SECOND_REQUEST: &str = "second failed request";
    const THIRD_REQUEST: &str = "third failed request";
    const SHELL_MARKER: &str = "INTERLEAVED-SHELL-MARKER";
    const ACTIVE_SUMMARY: &str = "failed turns and interleaved shell record summarized in order";
    const UPDATED_ACTIVE_SUMMARY: &str = "later failure merged into the recovery summary";

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step("first provider failure", Vec::new()),
        error_step("second provider failure", Vec::new()),
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":null,"active_turn_summary":"failed turns and interleaved shell record summarized in order"}"#,
            ],
            &[],
        ),
        error_step("third provider failure", Vec::new()),
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":null,"active_turn_summary":"later failure merged into the recovery summary"}"#,
            ],
            &[],
        ),
        json_by_request_kind_responses(
            &[
                r#"{"committed_summary":null,"active_turn_summary":"live turn merged with recovery"}"#,
            ],
            &[],
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_face0023").await;
    let encoded_shell_marker = SHELL_MARKER
        .bytes()
        .map(|byte| format!("\\{byte:03o}"))
        .collect::<String>();

    engine
        .run_turn(&mut session, FIRST_REQUEST, |_| {})
        .await
        .expect_err("the first turn must fail");
    engine
        .run_user_shell_command(
            &mut session,
            format!("printf '{encoded_shell_marker}'"),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    engine
        .run_turn(&mut session, SECOND_REQUEST, |_| {})
        .await
        .expect_err("the second turn must fail");
    let before_compact = session.read_metadata().await.unwrap().compaction.unwrap();
    let before_compact_history = before_compact.provider_history.unwrap();
    assert_eq!(before_compact_history.recovery_base_message_count, Some(0));

    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    let requests = provider.requests().await;
    let summary_request = requests
        .iter()
        .find(|request| request.system_prompt.contains("committed_summary"))
        .expect("manual recovery compact must call the shared summarizer");
    let summary_json = serde_json::to_string(&summary_request.messages).unwrap();
    assert_eq!(summary_json.matches(SHELL_MARKER).count(), 1);

    let state = session.read_metadata().await.unwrap().compaction.unwrap();
    assert!(state.committed_summary.is_empty());
    assert_eq!(state.frontier.committed_message_until, 0);
    assert_eq!(state.active_turn_summary.as_deref(), Some(ACTIVE_SUMMARY));
    assert_eq!(
        state
            .provider_history
            .as_deref()
            .and_then(|history| history.recovery_base_message_count),
        Some(0)
    );

    engine
        .run_turn(&mut session, THIRD_REQUEST, |_| {})
        .await
        .expect_err("the third turn must fail");
    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    let state = session.read_metadata().await.unwrap().compaction.unwrap();
    assert!(state.committed_summary.is_empty());
    assert_eq!(state.frontier.committed_message_until, 0);
    assert_eq!(
        state.active_turn_summary.as_deref(),
        Some(UPDATED_ACTIVE_SUMMARY)
    );

    let mut live_provider_messages = state
        .provider_history
        .as_deref()
        .expect("manual recovery compact should persist its Provider projection")
        .messages
        .clone();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    let live_started_at = Utc::now();
    writer
        .append(
            "turn_live",
            live_started_at,
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_live",
            live_started_at,
            TurnJournalEventKind::UserInputAccepted {
                text: "live request".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    let live_active_suffix = oversized_active_suffix("live request".into(), None, None);
    live_provider_messages.extend(live_active_suffix.iter().cloned());
    let base_message_count = session.read_metadata().await.unwrap().message_count;
    let live_projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                provider_messages: &live_provider_messages,
                active_suffix: live_active_suffix,
                turn_id: "turn_live",
                base_message_count,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap()
        .expect("live preflight should compact the active tool history");
    let live_projection_json = serde_json::to_string(&live_projection.messages).unwrap();
    assert!(
        live_projection_json.contains("live request"),
        "live preflight must retain the current user request as the raw anchor"
    );
    let live_request_at = live_projection_json.find("live request").unwrap();
    let live_summary_at = live_projection_json
        .find("live turn merged with recovery")
        .unwrap();
    assert!(
        live_request_at < live_summary_at,
        "the replacement WAL must place the current raw anchor before the recovery summary"
    );
    let live_state = session.read_metadata().await.unwrap().compaction.unwrap();
    assert_eq!(
        live_state
            .provider_history
            .as_deref()
            .and_then(|history| history.recovery_turn_id.as_deref()),
        Some("turn_live")
    );
    let journal_projection = replay_turn_journal(session.read_turn_journal().await);
    let canonical_messages = session.read_messages().await.unwrap();
    let recovery_turns = recovery_turn_chain(&journal_projection, &canonical_messages);
    let recovery_suffix = provider_recovery_suffix(&live_projection.messages, &recovery_turns)
        .expect("the replacement WAL must remain recoverable from the current raw anchor");
    let recovery_suffix_json = serde_json::to_string(&recovery_suffix).unwrap();
    assert!(recovery_suffix_json.contains("live request"));
    assert!(recovery_suffix_json.contains("live turn merged with recovery"));
    let requests = provider.requests().await;
    let live_summary_request = requests
        .last()
        .expect("live preflight should call the shared summarizer");
    assert!(
        serde_json::to_string(&live_summary_request.messages)
            .unwrap()
            .contains(UPDATED_ACTIVE_SUMMARY),
        "live preflight must carry the unresolved recovery summary into its replacement"
    );
}

#[test]
fn manual_recovery_keeps_repeated_failed_turns_across_provider_identity_change() {
    fn failed_turn(turn_id: &str, original_user_request: &str) -> TurnJournalTurn {
        TurnJournalTurn {
            turn_id: turn_id.into(),
            started_at: None,
            accepted_at: None,
            finished_at: None,
            status: Some(TurnJournalStatus::Failed),
            original_user_request: Some(original_user_request.into()),
            canonical_user_content_hash: None,
            canonical_user_first_text: None,
            model_context: Vec::new(),
            skill_instructions: Vec::new(),
            compaction_assets: Vec::new(),
            assistant_text: String::new(),
            assistant_completed: false,
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            non_streaming_fallbacks: Vec::new(),
        }
    }

    let first = failed_turn("turn_first", "continue");
    let latest = failed_turn("turn_latest", "continue");
    let recovery_turns = vec![&first, &latest];
    let old_identity = ProviderReplayIdentity {
        protocol: ProviderReplayProtocol::OpenAiResponses,
        model: "old-model".into(),
    };
    let current_identity = ProviderReplayIdentity {
        protocol: ProviderReplayProtocol::AnthropicMessages,
        model: "current-model".into(),
    };
    let history = CompactedProviderHistory {
        replay_identity: Some(old_identity),
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: latest.turn_id.clone(),
            base_message_count: 0,
            provider_request_message_count: Some(4),
        }),
        canonical_message_until: 0,
        messages: vec![
            SessionTurnMessage::user_text("continue"),
            SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_first".into(),
                    name: "working_note".into(),
                    input: json!({"action": "add", "note": "keep neutral tool input"}),
                }],
                provider_replay: Some(ProviderReplayState::OpenAiResponses {
                    model: Some("old-model".into()),
                    items: vec![json!({"opaque": "PRIVATE-REPLAY-MARKER"})],
                }),
            },
            SessionTurnMessage {
                role: "user".into(),
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "toolu_first".into(),
                    content: "FIRST-TOOL-RESULT".into(),
                }],
                provider_replay: None,
            },
            SessionTurnMessage::user_text("continue"),
        ],
    };

    let recovered = manual_pending_provider_turn(
        Some(&history),
        &recovery_turns,
        Some(&current_identity),
        &[],
        ProviderHistoryMediaPolicy::Placeholder,
    )
    .unwrap()
    .expect("the provider-neutral WAL suffix should survive an identity change");

    assert_eq!(recovered.base_message_count, 0);
    assert_eq!(recovered.turn_id, "turn_latest");
    assert_eq!(recovered.protected_tail_segments, 0);
    assert!(recovered
        .active_suffix
        .iter()
        .all(|message| message.provider_replay.is_none()));
    let recovered_json = serde_json::to_string(&recovered.active_suffix).unwrap();
    assert!(recovered_json.contains("toolu_first"));
    assert!(recovered_json.contains("FIRST-TOOL-RESULT"));
    assert_eq!(recovered_json.matches("continue").count(), 2);
    assert!(!recovered_json.contains("PRIVATE-REPLAY-MARKER"));

    let stable_history = CompactedProviderHistory {
        replay_identity: history.replay_identity.clone(),
        recovery_turn_id: Some(first.turn_id.clone()),
        recovery_base_message_count: Some(0),
        pending_turn: None,
        canonical_message_until: 0,
        messages: history.messages[..3].to_vec(),
    };
    let recovered = manual_pending_provider_turn(
        Some(&stable_history),
        &recovery_turns,
        Some(&current_identity),
        &[],
        ProviderHistoryMediaPolicy::Placeholder,
    )
    .unwrap()
    .expect("stable unresolved WAL should remain manually compactable");
    assert_eq!(recovered.turn_id, "turn_first");
    let recovered_json = serde_json::to_string(&recovered.active_suffix).unwrap();
    assert!(recovered_json.contains("toolu_first"));
    assert!(recovered_json.contains("FIRST-TOOL-RESULT"));
    assert_eq!(recovered_json.matches("continue").count(), 1);
}

#[tokio::test]
async fn manual_compact_keeps_short_pending_turn_as_raw_noop() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![error_step(
        "provider failed before any progress",
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.compaction.tail_target_ctx_ratio = 0.6;
    let mut session = create_test_session(&store, "session_face0020").await;

    engine
        .run_turn(&mut session, "short request", |_| {})
        .await
        .unwrap_err();
    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Noop(SessionCompactionNoopReason::NothingNew)
    ));
    assert_eq!(provider.requests().await.len(), 1);
}

#[tokio::test]
async fn manual_compact_preserves_stable_raw_recovery_without_pending_turn() {
    const COMMITTED_MARKER: &str = "COMMITTED-BEFORE-FAILED-TURN";
    const COMMITTED_SUMMARY: &str = "committed history summarized";
    const FAILED_REQUEST: &str = "continue after the committed work";

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        error_step("provider failed before any progress", Vec::new()),
        json_by_request_kind_responses(
            &[r#"{"committed_summary":"committed history summarized","active_turn_summary":null}"#],
            &[],
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.compaction.auto_compact_ctx_ratio = 0.0;
    engine.compaction.tail_target_ctx_ratio = 0.00001;
    let mut session = create_test_session(&store, "session_face0022").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "earlier request"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                format!("{}{COMMITTED_MARKER}", "c".repeat(12_000)),
            ),
        ])
        .await
        .unwrap();

    engine
        .run_turn(&mut session, FAILED_REQUEST, |_| {})
        .await
        .unwrap_err();
    let projection = replay_turn_journal(session.read_turn_journal().await);
    let canonical_messages = session.read_messages().await.unwrap();
    engine
        .reconcile_pending_provider_history(&mut session, &projection, &canonical_messages)
        .await
        .unwrap();
    let stable = session.read_metadata().await.unwrap().compaction.unwrap();
    let stable_history = stable.provider_history.unwrap();
    assert!(stable_history.pending_turn.is_none());
    assert!(stable_history.recovery_turn_id.is_some());

    assert!(matches!(
        engine
            .compact_session_checkpoint(&mut session, |_| {})
            .await
            .unwrap(),
        SessionCompactionResult::Compacted(_)
    ));
    let compacted = session.read_metadata().await.unwrap().compaction.unwrap();
    assert_eq!(compacted.committed_summary, COMMITTED_SUMMARY);
    assert!(compacted.active_turn_summary.is_none());
    let compacted_history = compacted.provider_history.unwrap();
    assert!(compacted_history.pending_turn.is_none());
    assert!(compacted_history.recovery_turn_id.is_some());
    let compacted_wal = serde_json::to_string(&compacted_history.messages).unwrap();
    assert!(compacted_wal.contains(COMMITTED_SUMMARY));
    assert!(compacted_wal.contains(FAILED_REQUEST));
    assert!(!compacted_wal.contains(COMMITTED_MARKER));

    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "later local record"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "l".repeat(12_000)),
        ])
        .await
        .unwrap();
    let requests_before_second_compact = provider.requests().await.len();
    engine
        .compact_session_checkpoint(&mut session, |_| {})
        .await
        .unwrap();
    assert_eq!(
        provider.requests().await.len(),
        requests_before_second_compact
    );
    let after_second = session.read_metadata().await.unwrap().compaction.unwrap();
    let after_second_history = after_second.provider_history.unwrap();
    assert!(after_second_history.recovery_turn_id.is_some());
    let after_second_wal = serde_json::to_string(&after_second_history.messages).unwrap();
    assert!(after_second_wal.contains(FAILED_REQUEST));
}

#[test]
fn no_consumable_compaction_retry_is_hidden_from_tui_but_other_retry_warnings_remain() {
    let no_consumable = crate::api::StructuredJsonNoConsumableOutput::new(
        "Responses 响应没有可消费的 output_text 或 function_call".into(),
        crate::api::ProviderTransport::ResponsesSse,
    );

    assert!(!should_emit_compaction_retry_warning(&no_consumable.into()));
    assert!(should_emit_compaction_retry_warning(&anyhow::anyhow!(
        "compaction summary JSON invalid"
    )));
}

#[tokio::test]
async fn manual_compact_with_only_recap_backlog_requests_background_recap() {
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

    let mut events = Vec::new();
    let outcome = engine
        .compact_session_checkpoint(&mut session, |event| events.push(event))
        .await
        .unwrap();

    assert!(matches!(outcome, SessionCompactionResult::Compacted(_)));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::RecapRequested {
            recap_end_index: 2,
            ..
        }
    )));
}

#[tokio::test]
async fn manual_compact_context_only_summary_range_still_requests_recap_for_canonical_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0010").await;
    session
        .append_messages(&[
            new_model_context_message(
                ModelContextSource::Runtime,
                "runtime-v1",
                "<runtime_context>old</runtime_context>",
            ),
            new_model_context_message(
                ModelContextSource::BackgroundProcess,
                "background-v1",
                "<background_processes>old</background_processes>",
            ),
            new_model_context_message(
                ModelContextSource::Delegation,
                "delegation-v1",
                "<delegation_summary>old</delegation_summary>",
            ),
            NewSessionMessage::text(SessionMessageRole::User, "real request kept in raw tail"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "real answer kept in raw tail",
            ),
        ])
        .await
        .unwrap();

    let mut events = Vec::new();
    let outcome = engine
        .compact_session_checkpoint(&mut session, |event| events.push(event))
        .await
        .unwrap();

    assert!(matches!(outcome, SessionCompactionResult::Compacted(_)));
    assert!(provider.requests().await.is_empty());
    let metadata = session.read_metadata().await.unwrap();
    assert!(metadata.compaction.is_none());
    assert_eq!(metadata.recapped_until, 0);
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::RecapRequested {
            recap_end_index: 5,
            ..
        }
    )));
}

#[tokio::test]
async fn preflight_compact_skips_context_only_committed_projection_without_provider_call() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0011").await;
    session
        .append_messages(&[
            new_model_context_message(
                ModelContextSource::Runtime,
                "runtime-v1",
                "<runtime_context>old</runtime_context>",
            ),
            new_model_context_message(
                ModelContextSource::BackgroundProcess,
                "background-v1",
                "<background_processes>old</background_processes>",
            ),
            NewSessionMessage::text(SessionMessageRole::User, "real request kept in raw tail"),
            NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "real answer kept in raw tail",
            ),
        ])
        .await
        .unwrap();

    let projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                provider_messages: &[],
                active_suffix: vec![SessionTurnMessage::user_text("current request")],
                turn_id: "turn_1",
                base_message_count: 4,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap();

    assert!(projection.is_none());
    assert!(provider.requests().await.is_empty());
    let metadata = session.read_metadata().await.unwrap();
    assert!(metadata.compaction.is_none());
    assert_eq!(metadata.recapped_until, 0);
}

#[tokio::test]
async fn main_forced_context_recovery_errors_when_only_model_context_is_compactable() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.auto_compact_ctx_ratio = 0.5;
    engine.compaction.tail_target_ctx_ratio = 0.001;
    let mut session = create_test_session(&store, "session_face0015").await;
    let marker = SessionTurnMessage::assistant_text("latest partial answer");
    let mut provider_messages = vec![
        SessionTurnMessage::user_text("current objective"),
        SessionTurnMessage::model_context(ModelContextSource::Runtime, "R".repeat(2_000)),
        SessionTurnMessage::model_context(ModelContextSource::BackgroundProcess, "B".repeat(2_000)),
        marker.clone(),
    ];
    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_1".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        context_window_recovery_requested: true,
        context_window_recovery_tail_marker: Some(marker),
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: false,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
    };

    let error = preflight
        .before_provider_request(
            &mut "system".to_string(),
            &mut provider_messages,
            &mut |_| {},
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("没有可安全压缩的历史"));
    assert!(provider.requests().await.is_empty());
}

#[tokio::test]
async fn active_turn_compact_skips_context_only_effective_projection() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_face0012").await;
    let metadata = session.read_metadata().await.unwrap();
    let active = vec![
        SessionTurnMessage::user_text("objective anchor"),
        SessionTurnMessage::model_context(ModelContextSource::Runtime, "R".repeat(2_000)),
        SessionTurnMessage::model_context(ModelContextSource::BackgroundProcess, "B".repeat(2_000)),
        SessionTurnMessage::assistant_text("recent answer kept raw"),
    ];
    let tail_token_limit = estimate_session_turn_messages_tokens(&active[..1])
        .saturating_add(estimate_session_turn_messages_tokens(&active[3..]));

    let plan = engine
        .build_active_turn_plan(&metadata, &active, "turn_1", 0, tail_token_limit, 0)
        .unwrap();

    assert!(plan.is_none());
}

#[tokio::test]
async fn preflight_compact_keeps_active_scope_when_committed_projection_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary": null, "active_turn_summary": "active work summarized"}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.tail_target_ctx_ratio = 0.001;
    let mut session = create_test_session(&store, "session_face0013").await;
    session
        .append_messages(&[
            new_model_context_message(
                ModelContextSource::Runtime,
                "runtime-v1",
                "<runtime_context>old</runtime_context>",
            ),
            new_model_context_message(
                ModelContextSource::BackgroundProcess,
                "background-v1",
                "<background_processes>old</background_processes>",
            ),
        ])
        .await
        .unwrap();
    let active_suffix = vec![
        SessionTurnMessage::user_text("current objective"),
        SessionTurnMessage::assistant_text("older active detail ".repeat(1_000)),
        SessionTurnMessage::assistant_text("recent active answer"),
    ];

    let projection = engine
        .compact_provider_preflight(
            &mut session,
            PreflightCompactionRequest {
                base_system_prompt: "system",
                provider_messages: &[],
                active_suffix,
                turn_id: "turn_1",
                base_message_count: 2,
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
            },
            &mut |_| {},
        )
        .await
        .unwrap()
        .expect("active scope should still compact");

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let request_text = serde_json::to_string(&requests[0].messages).unwrap();
    assert!(request_text.contains(r#"\"committed_transcript\": null"#));
    assert!(request_text.contains("older active detail"));
    let metadata = session.read_metadata().await.unwrap();
    let compaction = metadata.compaction.expect("compaction state");
    assert_eq!(compaction.committed_message_until(), 0);
    assert_eq!(
        compaction.active_turn_summary.as_deref(),
        Some("active work summarized")
    );
    assert!(serde_json::to_string(&projection.messages)
        .unwrap()
        .contains("active work summarized"));
}

#[tokio::test]
async fn finalize_context_only_segment_skips_recap_provider_and_advances_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0014").await;
    session
        .append_messages(&[
            new_model_context_message(
                ModelContextSource::Runtime,
                "runtime-v1",
                "<runtime_context>old</runtime_context>",
            ),
            new_model_context_message(
                ModelContextSource::BackgroundProcess,
                "background-v1",
                "<background_processes>old</background_processes>",
            ),
        ])
        .await
        .unwrap();

    let report = engine.finalize_session(&mut session, |_| {}).await.unwrap();

    assert!(provider.requests().await.is_empty());
    assert!(report.advanced_recapped_until);
    assert!(report.finalized_unrecapped_messages);
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, metadata.message_count);
    assert!(metadata.finalized_at.is_some());
}

#[tokio::test]
async fn background_recap_jobs_process_only_the_remaining_message_range() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0015").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "first request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "first answer"),
            NewSessionMessage::text(SessionMessageRole::User, "second request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "second answer"),
        ])
        .await
        .unwrap();

    let first = engine
        .recap_existing_session_until(&session.metadata.id, 2)
        .await
        .unwrap();
    let second = engine
        .recap_existing_session_until(&session.metadata.id, 4)
        .await
        .unwrap();
    let noop = engine
        .recap_existing_session_until(&session.metadata.id, 4)
        .await
        .unwrap();

    assert!(first.advanced_recapped_until);
    assert!(second.advanced_recapped_until);
    assert!(!noop.advanced_recapped_until);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    let second_payload = last_user_text(&requests[1]);
    assert!(!second_payload.contains("first request"));
    assert!(second_payload.contains("second request"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Open);
    assert_eq!(metadata.recapped_until, 4);
    assert!(metadata.finalized_at.is_none());
}

#[tokio::test]
async fn background_recap_stages_dispute_before_recording_reported_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{
            "new_claims":[
                {
                    "id":"$new_claim_0$",
                    "name":"first recap fact",
                    "statement":"first recap fact",
                    "scope":"tests / recap",
                    "confidence":"high",
                    "evidence_summary":"session evidence",
                    "source_claim_ids":[]
                },
                {
                    "id":"$new_claim_1$",
                    "name":"conflicting recap fact",
                    "statement":"conflicting recap fact",
                    "scope":"tests / recap",
                    "confidence":"high",
                    "evidence_summary":"session evidence",
                    "source_claim_ids":[]
                }
            ],
            "used_claim_ids":[],
            "new_disputes":[{
                "id":"$new_dispute_0$",
                "name":"recap conflict",
                "claims":["$new_claim_0$","$new_claim_1$"],
                "summary":"the recap facts conflict"
            }]
        }"#,
        Vec::new(),
    )]));
    let agent_home = dir.path().join("agents").join("agent-a");
    let reported_store = Arc::new(PendingBeforeLedgerReportedDisputeStore::new(agent_home));
    let (engine, store) = build_test_engine_with_reported_store(&dir, provider, reported_store);
    let mut session = create_test_session(&store, "session_face0023").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
        ])
        .await
        .unwrap();

    let report = engine
        .recap_existing_session_until(&session.metadata.id, 2)
        .await
        .unwrap();

    assert_eq!(report.new_dispute_ids.len(), 1);
    assert!(report.advanced_recapped_until);
}

#[tokio::test]
async fn background_recap_waits_for_agent_knowledge_apply_lock() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0018").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
        ])
        .await
        .unwrap();
    let lock_path =
        paths::agent_home_knowledge_apply_lock_path(&dir.path().join("agents").join("agent-a"));
    let guard = FileLockGuard::lock_exclusive(&lock_path).await.unwrap();
    let session_id = session.metadata.id.clone();
    let task =
        tokio::spawn(async move { engine.recap_existing_session_until(&session_id, 2).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(provider.requests().await.is_empty());

    drop(guard);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(provider.requests().await.len(), 1);
}

#[tokio::test]
async fn same_session_finalize_preempts_recap_before_prepared_and_takes_full_range() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(PreemptibleRecapProvider::new());
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0019").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "first request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "first answer"),
            NewSessionMessage::text(SessionMessageRole::User, "final request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "final answer"),
        ])
        .await
        .unwrap();
    let control = Arc::new(SessionRecapPreemptionControl::new());
    let recap_engine = engine.clone();
    let recap_session_id = session.metadata.id.clone();
    let recap_control = Arc::clone(&control);
    let recap = tokio::spawn(async move {
        recap_engine
            .recap_existing_session_until_with_preemption(&recap_session_id, 2, recap_control)
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        provider.first_call_started.notified(),
    )
    .await
    .unwrap();

    session.mark_finalizing(Utc::now()).await.unwrap();
    assert!(control.request_before_prepared().await);
    let recap_report = tokio::time::timeout(Duration::from_secs(2), recap)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert!(!recap_report.advanced_recapped_until);
    assert!(provider.first_call_dropped.load(Ordering::SeqCst));
    assert!(session.read_finalize_checkpoint().await.unwrap().is_none());
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Finalizing);
    assert_eq!(metadata.recapped_until, 0);

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    let finalize_payload = last_user_text(&requests[1]);
    assert!(finalize_payload.contains("first request"));
    assert!(finalize_payload.contains("final request"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(metadata.recapped_until, metadata.message_count);
}

#[tokio::test]
async fn resume_preempts_running_finalize_before_prepared_without_closing_session() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(PreemptibleRecapProvider::new());
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0023").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "resume target request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "resume target answer"),
        ])
        .await
        .unwrap();
    session.mark_finalizing(Utc::now()).await.unwrap();
    let control = Arc::new(SessionFinalizePreemptionControl::new());
    let finalize_engine = engine.clone();
    let session_id = session.metadata.id.clone();
    let finalize_control = Arc::clone(&control);
    let finalize = tokio::spawn(async move {
        finalize_engine
            .finalize_existing_session_once_with_preemption(&session_id, |_| {}, finalize_control)
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        provider.first_call_started.notified(),
    )
    .await
    .unwrap();

    assert!(control.request_before_prepared().await);
    let outcome = tokio::time::timeout(Duration::from_secs(2), finalize)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert!(matches!(
        outcome,
        SessionFinalizeOnceOutcome::PreemptedBeforePrepared
    ));
    assert!(provider.first_call_dropped.load(Ordering::SeqCst));
    assert!(session.read_finalize_checkpoint().await.unwrap().is_none());
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Finalizing);
    assert_eq!(metadata.recapped_until, 0);
}

#[tokio::test]
async fn finalize_recaps_invalid_tool_use_without_replaying_or_reconstructing_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0024").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "run the safe sibling tool"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::InvalidToolUse {
                    id: "call_invalid".into(),
                    name: "file_read".into(),
                    error: "function_call.arguments was not a JSON object".into(),
                }],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "call_invalid",
                    r#"{"ok":false,"outcome":{"kind":"dispatch_failure"}}"#,
                )],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "continued safely"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    assert_eq!(
        hash_session_segment(&messages).unwrap(),
        hash_session_segment(&messages).unwrap()
    );
    session.mark_finalizing(Utc::now()).await.unwrap();

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(payload.contains("invalid_tool_use file_read"));
    assert!(payload.contains("dispatch_failure"));
    assert!(!payload.contains("raw invalid arguments"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(metadata.recapped_until, metadata.message_count);
}

#[tokio::test]
async fn finalize_recovers_prepared_recap_prefix_before_processing_remaining_messages() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0020").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "prepared request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "prepared answer"),
            NewSessionMessage::text(SessionMessageRole::User, "remaining request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "remaining answer"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let recap_hash = hash_session_segment(&messages[..2]).unwrap();
    session
        .write_finalize_checkpoint(&prepared_empty_finalize_checkpoint(0, 2, recap_hash))
        .await
        .unwrap();
    session.mark_finalizing(Utc::now()).await.unwrap();

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(!payload.contains("prepared request"));
    assert!(payload.contains("remaining request"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(metadata.recapped_until, 4);
    let checkpoint = session.read_finalize_checkpoint().await.unwrap().unwrap();
    assert_eq!(checkpoint.recap_start_index, 2);
    assert_eq!(checkpoint.recap_end_index, 4);
    assert_eq!(checkpoint.status, FinalizeCheckpointStatus::Applied);
}

#[tokio::test]
async fn finalize_applied_checkpoint_recovery_flushes_durable_pending_uploads() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0022").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "applied request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "applied answer"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let recap_hash = hash_session_segment(&messages).unwrap();
    let mut checkpoint = prepared_empty_finalize_checkpoint(0, 2, recap_hash);
    checkpoint.status = FinalizeCheckpointStatus::Applied;
    session
        .write_finalize_checkpoint(&checkpoint)
        .await
        .unwrap();
    let agent_home = dir.path().join("agents").join("agent-a");
    let pending_path = paths::agent_home_pending_maintainer_uploads_path(&agent_home);
    write_yaml_atomic(
        &pending_path,
        &PendingMaintainerUploads {
            claims: vec![Claim {
                id: "claim_11111111".parse().unwrap(),
                name: "pending recap claim".into(),
                statement: "pending recap claim statement".into(),
                scope: "test".into(),
                holder: AgentId::new("agent-a").unwrap(),
                confidence: Confidence::High,
                status: ClaimStatus::Active,
                created_at: Utc::now(),
                updated_at: None,
                source_claim_ids: Vec::new(),
                evidence_summary: "test".into(),
            }],
            durable_claim_ids: Default::default(),
            disputes: Vec::new(),
        },
    )
    .await
    .unwrap();
    session.mark_finalizing(Utc::now()).await.unwrap();

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();

    assert!(!tokio::fs::try_exists(pending_path).await.unwrap());
    assert!(provider.requests().await.is_empty());
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(metadata.recapped_until, 2);
}

#[tokio::test]
async fn finalize_recovers_prepared_recap_before_adding_background_completion() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0021").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "prepared request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "prepared answer"),
        ])
        .await
        .unwrap();
    let messages = session.read_messages().await.unwrap();
    let recap_hash = hash_session_segment(&messages).unwrap();
    session
        .write_finalize_checkpoint(&prepared_empty_finalize_checkpoint(0, 2, recap_hash))
        .await
        .unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_background",
            Utc::now(),
            TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id: "toolu_after_prepared".into(),
                process_id: "process_after_prepared".into(),
                instance_id: 21,
                status: "finished".into(),
                exit_code: Some(0),
                signal: None,
                success: true,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    drop(writer);
    session.mark_finalizing(Utc::now()).await.unwrap();

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    let payload = last_user_text(&requests[0]);
    assert!(!payload.contains("prepared request"));
    assert!(payload.contains("process_after_prepared"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(metadata.recapped_until, 2);
    assert!(metadata
        .recap_background_completion_until_seq
        .is_some_and(|cursor| cursor > 0));
}

#[tokio::test]
async fn finalize_continues_from_background_recap_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0016").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "first request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "first answer"),
            NewSessionMessage::text(SessionMessageRole::User, "final request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "final answer"),
        ])
        .await
        .unwrap();

    engine
        .recap_existing_session_until(&session.metadata.id, 2)
        .await
        .unwrap();
    engine.finalize_session(&mut session, |_| {}).await.unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    let finalize_payload = last_user_text(&requests[1]);
    assert!(!finalize_payload.contains("first request"));
    assert!(finalize_payload.contains("final request"));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
    assert_eq!(metadata.recapped_until, metadata.message_count);
}

#[tokio::test]
async fn supervisor_recap_and_finalize_use_buffered_transport_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        ProviderStep::StreamFailure("recap stream failed"),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
        ProviderStep::StreamFailure("finalize stream failed"),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_face0025").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "recap request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "recap answer"),
        ])
        .await
        .unwrap();

    engine
        .recap_existing_session_until(&session.metadata.id, 2)
        .await
        .unwrap();
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "final request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "final answer"),
        ])
        .await
        .unwrap();
    session.mark_finalizing(Utc::now()).await.unwrap();
    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 4);
    for pair in requests.chunks_exact(2) {
        assert!(pair[0].stream);
        assert!(!pair[1].stream);
        assert!(pair.iter().all(|request| {
            request.stream_output_mode == crate::api::ProviderStreamOutputMode::Buffered
                && request.retry_count_override == Some(0)
                && !request.allow_continuation
        }));
        assert!(pair[0].runtime_chain_id.is_some());
        assert!(pair[0].runtime_fallback_scope.is_some());
        assert!(pair[1].runtime_chain_id.is_none());
        assert!(pair[1].runtime_fallback_scope.is_none());
        assert_eq!(pair[0].messages, pair[1].messages);
    }
}

#[tokio::test]
async fn supervisor_finalize_attempt_uses_one_logical_generation() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step("{not json", Vec::new()),
        response_step(
            r#"{"new_claims":[],"used_claim_ids":[],"new_disputes":[]}"#,
            Vec::new(),
        ),
    ]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        1024,
        4,
        Duration::ZERO,
        Duration::ZERO,
    ));
    let mut session = create_test_session(&store, "session_face0017").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
        ])
        .await
        .unwrap();

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .expect_err("first supervisor attempt should expose invalid JSON to the job retry");
    let first_requests = provider.requests().await;
    assert_eq!(first_requests.len(), 1);
    assert!(first_requests[0].stream);
    assert_eq!(first_requests[0].retry_count_override, Some(0));
    assert!(!first_requests[0].allow_continuation);

    engine
        .finalize_existing_session_once(&session.metadata.id, |_| {})
        .await
        .unwrap();
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request.stream
            && request.stream_output_mode == crate::api::ProviderStreamOutputMode::Buffered
            && request.retry_count_override == Some(0)
            && !request.allow_continuation
    }));
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.status, SessionStatus::Closed);
}

#[tokio::test]
async fn manual_compact_applied_summary_checkpoint_clears_file_read_state() {
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
    let summary_hash = super::hash_session_segment(&messages).unwrap();
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: vec!["compact_report".into()],
        summary_start_index: 0,
        summary_end_index: messages.len(),
        summary_segment_hash: summary_hash,
        summary: "checkpointed summary".into(),
        active_turn_summary: None,
        active_turn: None,
        preserve_provider_history: false,
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
async fn manual_compact_does_not_call_recap_provider_after_summary() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        json_by_request_kind_responses(
            &[r#"{"committed_summary":"old turn summarized","active_turn_summary":null}"#],
            &[],
        ),
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

    let mut events = Vec::new();
    engine
        .compact_session_checkpoint_with_events(&mut session, &mut |event| events.push(event))
        .await
        .unwrap();

    assert_eq!(provider.requests().await.len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::RecapRequested {
            recap_end_index: 4,
            ..
        }
    )));
    let audit_log = tokio::fs::read_to_string(&session.paths.compaction_events_jsonl)
        .await
        .unwrap();
    assert!(audit_log.contains(r#""kind":"started""#));
    assert!(audit_log.contains(r#""kind":"model_attempt""#));
    assert!(!audit_log.contains(r#""kind":"failed""#));
    assert!(audit_log.contains(r#""kind":"completed""#));
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
        json_by_request_kind_responses(&[&response, &response], &[]),
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
async fn manual_compact_requests_background_recap_after_budget_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        r#"{"committed_summary":"old turn summarized","active_turn_summary":null}"#,
        Vec::new(),
    )]));
    let (mut engine, store) = build_test_engine(&dir, provider.clone());
    engine.context_window = 20_000;
    engine.compaction.tail_target_ctx_ratio = 0.015;
    engine.compaction.tail_hard_ctx_ratio = 0.0225;
    engine.compaction.tail_previous_real_user_turns = 1;
    let mut session = create_test_session(&store, "session_face000b").await;
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
    let outcome = engine
        .compact_session_checkpoint_with_events(&mut session, &mut |event| events.push(event))
        .await
        .expect("summary compact should not wait for background recap");

    assert!(matches!(outcome, ManualCompactionOutcome::Compacted(_)));
    assert_eq!(provider.requests().await.len(), 1);
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.recapped_until, 0);
    assert!(metadata.compaction.is_some());
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::RecapRequested {
            session_id,
            recap_end_index,
        } if session_id == &metadata.id && *recap_end_index == metadata.message_count
    )));
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
        summary: "bad range summary".into(),
        active_turn_summary: None,
        active_turn: None,
        preserve_provider_history: false,
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
async fn finalize_ignores_legacy_v2_compaction_checkpoint() {
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
        schema_version: Some(2),
        audit_ids: Vec::new(),
        summary_start_index: 0,
        summary_end_index: 0,
        summary_segment_hash: super::hash_session_segment(&messages[0..0]).unwrap(),
        summary: String::new(),
        active_turn_summary: None,
        active_turn: None,
        preserve_provider_history: false,
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
async fn interrupted_turn_terminal_journal_failure_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_aaaaaa19").await;
    let journal_path = session.paths.turn_events_jsonl.clone();
    let saved_path = journal_path.with_extension("jsonl.before_terminal_failure");
    let (control, control_rx) = SessionTurnControl::channel();
    provider
        .steps
        .lock()
        .await
        .push_back(ProviderStep::ResponseAndSteerThenBreakJournal {
            response: provider_response("must not commit"),
            events: Vec::new(),
            control,
            journal_path: journal_path.clone(),
        });
    let mut events = Vec::new();

    let error = engine
        .run_turn_with_attachments_controlled(
            &mut session,
            "original request",
            Vec::new(),
            Some(control_rx),
            |event| events.push(event),
        )
        .await
        .expect_err("steer journal failure must stop the current run");

    assert!(error.to_string().contains("session 存储 I/O 失败"));
    assert!(events
        .iter()
        .any(|event| matches!(event, SessionEvent::TurnFailed { .. })));
    assert!(events.iter().any(|event| matches!(
        event,
        SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Error
        }
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, SessionEvent::TurnInterrupted { .. })));
    assert!(session.read_messages().await.unwrap().is_empty());

    tokio::fs::remove_dir(&journal_path).await.unwrap();
    tokio::fs::rename(&saved_path, &journal_path).await.unwrap();
}

#[test]
fn interrupted_turn_journal_failure_overrides_successful_interruption_result() {
    assert!(journal_failure_overrides_turn_result(false, true));
    assert!(journal_failure_overrides_turn_result(true, false));
    assert!(!journal_failure_overrides_turn_result(false, false));
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

#[tokio::test]
async fn canonical_retry_after_rejection_is_not_duplicated_when_finish_marker_is_missing() {
    for (discard_turn, session_id) in [(true, "session_aaaaaa12"), (false, "session_aaaaaa13")] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(RecordingProvider::new(vec![response_step(
            "next answer",
            Vec::new(),
        )]));
        let (engine, store) = build_test_engine(&dir, provider.clone());
        let mut session = create_test_session(&store, session_id).await;
        let journal_at = Utc::now();
        let committed_at = journal_at + chrono::Duration::milliseconds(10);
        session
            .append_session_turn_messages(
                &[
                    CompletedSessionTurnMessage::new(
                        SessionTurnMessage::user_text("committed after rejection"),
                        committed_at,
                    ),
                    CompletedSessionTurnMessage::new(
                        SessionTurnMessage::assistant_text("committed retry answer"),
                        committed_at,
                    ),
                ],
                "test-model",
            )
            .await
            .unwrap();
        let mut compaction =
            SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
        compaction.provider_history = Some(Box::new(CompactedProviderHistory {
            replay_identity: None,
            recovery_turn_id: None,
            recovery_base_message_count: None,
            pending_turn: Some(PendingProviderHistoryTurn {
                turn_id: "turn_1".into(),
                base_message_count: 0,
                provider_request_message_count: Some(1),
            }),
            canonical_message_until: 2,
            messages: vec![
                SessionTurnMessage::user_text("committed after rejection"),
                SessionTurnMessage::assistant_text("committed retry answer"),
            ],
        }));
        session.update_compaction(compaction).await.unwrap();

        let mut writer = session.open_turn_journal_writer().await.unwrap();
        for kind in [
            TurnJournalEventKind::TurnStarted,
            TurnJournalEventKind::UserInputAccepted {
                text: "committed after rejection".into(),
            },
            TurnJournalEventKind::ProviderRequestRejected {
                rejection_id: 1,
                discard_turn,
            },
            TurnJournalEventKind::AssistantCompleted {
                text: "committed retry answer".into(),
            },
        ] {
            writer
                .append("turn_1", journal_at, kind, TurnJournalFlush::Immediate)
                .await
                .unwrap();
        }
        drop(writer);

        engine
            .run_turn(&mut session, "next request", |_| {})
            .await
            .unwrap();

        let requests = provider.requests().await;
        let wire = serde_json::to_string(&requests[0].messages).unwrap();
        assert_eq!(wire.matches("committed after rejection").count(), 1);
        assert_eq!(wire.matches("committed retry answer").count(), 1);
        assert!(!wire.contains("<interrupted_turn_context>"));
        assert!(wire.contains("next request"));
    }
}

#[tokio::test]
async fn missing_committed_marker_ignores_model_context_inside_canonical_tool_turn() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "next answer",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_cac4e001").await;
    let journal_at = Utc::now();
    let committed_at = journal_at + chrono::Duration::milliseconds(10);
    session
        .append_session_turn_messages(
            &[
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage::user_text("already committed tool request"),
                    committed_at,
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "assistant".into(),
                        content: vec![SessionTurnContentBlock::ToolUse {
                            id: "toolu_committed".into(),
                            name: "working_note".into(),
                            input: json!({"action": "list"}),
                        }],
                        provider_replay: None,
                    },
                    committed_at,
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "user".into(),
                        content: vec![SessionTurnContentBlock::ToolResult {
                            tool_use_id: "toolu_committed".into(),
                            content: "[]".into(),
                        }],
                        provider_replay: None,
                    },
                    committed_at,
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage::model_context(
                        ModelContextSource::BackgroundProcess,
                        "<background_processes>\nProcesses:\n- none\n</background_processes>",
                    ),
                    committed_at,
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage::assistant_text("committed final answer"),
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
                text: "already committed tool request".into(),
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
                text: "committed final answer".into(),
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

#[test]
fn compaction_turn_end_scan_is_transparent_to_model_context() {
    let now = Utc::now();
    let messages = vec![
        SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::text("request")],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        },
        SessionMessage {
            index: 1,
            role: SessionMessageRole::Assistant,
            content: vec![SessionContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "lookup".into(),
                input: json!({}),
            }],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        },
        SessionMessage {
            index: 2,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "result".into(),
            }],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        },
        SessionMessage {
            index: 3,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::ModelContext {
                source: ModelContextSource::BackgroundProcess,
                fingerprint: "sha256-v1:test".into(),
                text: "<background_processes>changed</background_processes>".into(),
            }],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        },
        SessionMessage {
            index: 4,
            role: SessionMessageRole::Assistant,
            content: vec![SessionContentBlock::text("final answer")],
            created_at: now,
            model: "test-model".into(),
            provider_replay: None,
        },
    ];

    assert_eq!(
        assistant_turn_end_text_after(&messages, 0).as_deref(),
        Some("final answer")
    );
}

#[tokio::test]
async fn consecutive_main_turns_keep_the_previous_provider_request_as_exact_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step(
            "first answer",
            vec![ProviderEvent::AssistantMessageCompleted {
                text: "first answer".into(),
            }],
        ),
        response_step(
            "second answer",
            vec![ProviderEvent::AssistantMessageCompleted {
                text: "second answer".into(),
            }],
        ),
    ]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_cac4e002").await;

    engine
        .run_turn(&mut session, "first request", |_| {})
        .await
        .unwrap();
    engine
        .run_turn(&mut session, "second request", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].system_prompt, requests[1].system_prompt);
    assert_eq!(requests[0].tools, requests[1].tools);
    assert!(requests[1].messages.starts_with(&requests[0].messages));
    assert_eq!(
        requests[1]
            .messages
            .iter()
            .filter(|message| message.model_context_snapshot().is_some())
            .count(),
        3,
        "未发生压缩或语义变化时不得追加 baseline 副本"
    );
    let sources = requests[0]
        .messages
        .iter()
        .filter_map(|message| {
            message
                .model_context_snapshot()
                .map(|(source, _, _)| *source)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sources,
        vec![
            ModelContextSource::Runtime,
            ModelContextSource::BackgroundProcess,
            ModelContextSource::Delegation,
        ]
    );
    let canonical = session_messages_to_provider_turn_messages(
        session.read_messages().await.unwrap(),
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    assert!(canonical.starts_with(&requests[1].messages));
}

#[tokio::test]
async fn unchanged_delegation_revision_skips_projection_store_reload_across_turns() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![
        response_step("first answer", Vec::new()),
        response_step("second answer", Vec::new()),
    ]));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_cac4e003").await;

    engine
        .run_turn(&mut session, "first request", |_| {})
        .await
        .unwrap();
    tokio::fs::write(session.paths.dir.join("delegations"), b"not a directory")
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "second request", |_| {})
        .await
        .expect("unchanged revision must reuse the persisted delegation baseline");
}

#[tokio::test]
async fn cached_delegation_baseline_replaces_stale_snapshot_after_compaction_without_store_reload()
{
    let dir = tempfile::tempdir().unwrap();
    let session_id = "session_cac4e004".parse::<SessionId>().unwrap();
    let session_dir = dir.path().join("sessions").join(session_id.as_str());
    tokio::fs::create_dir_all(&session_dir).await.unwrap();
    tokio::fs::write(session_dir.join("delegations"), b"not a directory")
        .await
        .unwrap();
    let tools = Arc::new(
        ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap(),
    );
    let stale = SessionTurnMessage::model_context(
        ModelContextSource::Delegation,
        "<subagent_summary_projection>stale</subagent_summary_projection>",
    );
    let current = SessionTurnMessage::model_context(
        ModelContextSource::Delegation,
        "<subagent_summary_projection>current</subagent_summary_projection>",
    );
    let baselines = Arc::new(std::sync::Mutex::new(HashMap::from([(
        session_id.clone(),
        DelegationProjectionBaseline {
            activity_revision: None,
            message: current.clone(),
        },
    )])));
    let mut appender = MainModelContextAppender {
        tools,
        session_id,
        session_dir,
        delegation_activity: None,
        delegation_projection_baselines: baselines,
        observed_delegation_baseline: None,
        background_completion_delivery_ids: Vec::new(),
        background_completion_until_seq: 0,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
    };
    let provider_messages = vec![
        stale,
        SessionTurnMessage::model_context(
            ModelContextSource::Runtime,
            "<runtime_context>later source</runtime_context>",
        ),
    ];

    let pending = appender
        .observe_context(&provider_messages)
        .await
        .expect("cached exact baseline must avoid the corrupt store");

    assert!(pending.contains(&current));
}

#[tokio::test]
async fn changed_delegation_revision_with_same_semantics_does_not_append_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "done",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let session = create_test_session(&store, "session_cac4e005").await;
    let delegation_store = DelegationStore::new(session.paths.dir.clone());
    let delegation = delegation_store
        .create(DelegationCreateRequest {
            parent_session_id: session.metadata.id.clone(),
            parent_turn_id: "turn_1".into(),
            owner_agent_id: AgentId::new("agent-a").unwrap(),
            title: "stable semantic state".into(),
            role: "worker".into(),
            objective: "progress must remain pull-only".into(),
            constraints: Vec::new(),
        })
        .await
        .unwrap();
    delegation_store.start(&delegation.id).await.unwrap();
    let baseline = SessionTurnMessage::model_context(
        ModelContextSource::Delegation,
        delegation_summary_projection(&session.paths.dir)
            .await
            .unwrap()
            .unwrap(),
    );
    delegation_store
        .update_progress(
            &delegation.id,
            DelegationUpdate {
                current_step: Some("frequent step".into()),
                summary: "frequent progress only".into(),
                artifacts: Vec::new(),
            },
        )
        .await
        .unwrap();
    let (_activity_tx, activity_rx) = tokio::sync::watch::channel(1_u64);
    let baselines = Arc::new(std::sync::Mutex::new(HashMap::from([(
        session.metadata.id.clone(),
        DelegationProjectionBaseline {
            activity_revision: Some(0),
            message: baseline.clone(),
        },
    )])));
    let mut appender = MainModelContextAppender {
        tools: engine.turn_loop.tool_registry(),
        session_id: session.metadata.id.clone(),
        session_dir: session.paths.dir.clone(),
        delegation_activity: Some(activity_rx),
        delegation_projection_baselines: Arc::clone(&baselines),
        observed_delegation_baseline: None,
        background_completion_delivery_ids: Vec::new(),
        background_completion_until_seq: 0,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
    };

    engine
        .turn_loop
        .run_session_turn_with_context_hooks(
            SessionTurnRequest {
                current_session_id: Some(session.metadata.id.clone()),
                current_turn_id: Some("turn_2".into()),
                system_prompt: "system".into(),
                history: vec![baseline.clone()],
                user_text: "continue".into(),
                user_attachments: Vec::new(),
                skill_instructions: Vec::new(),
            },
            Vec::new(),
            &mut |_| {},
            None,
            SessionTurnHooks::new(None, Some(&mut appender), None),
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    let delegation_snapshots = requests[0]
        .messages
        .iter()
        .filter(|message| {
            message
                .model_context_snapshot()
                .is_some_and(|(source, _, _)| *source == ModelContextSource::Delegation)
        })
        .collect::<Vec<_>>();
    assert_eq!(delegation_snapshots, vec![&baseline]);
    let cached = baselines.lock().unwrap();
    let cached = cached.get(&session.metadata.id).unwrap();
    assert_eq!(cached.activity_revision, Some(1));
    assert!(latest_model_context_matches(
        &requests[0].messages,
        &cached.message
    ));
}

#[tokio::test]
async fn multiple_delegation_changes_coalesce_into_one_next_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "done",
        Vec::new(),
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let session = create_test_session(&store, "session_cac4e006").await;
    let delegation_store = DelegationStore::new(session.paths.dir.clone());
    let mut child_ids = Vec::new();
    for index in 1..=2 {
        let child = delegation_store
            .create(DelegationCreateRequest {
                parent_session_id: session.metadata.id.clone(),
                parent_turn_id: "turn_1".into(),
                owner_agent_id: AgentId::new("agent-a").unwrap(),
                title: format!("child {index}"),
                role: "worker".into(),
                objective: format!("objective {index}"),
                constraints: Vec::new(),
            })
            .await
            .unwrap();
        child_ids.push(child.id.to_string());
    }
    let empty = SessionTurnMessage::model_context(
        ModelContextSource::Delegation,
        super::empty_delegation_summary_projection().unwrap(),
    );
    let (_activity_tx, activity_rx) = tokio::sync::watch::channel(2_u64);
    let baselines = Arc::new(std::sync::Mutex::new(HashMap::from([(
        session.metadata.id.clone(),
        DelegationProjectionBaseline {
            activity_revision: Some(0),
            message: empty.clone(),
        },
    )])));
    let mut appender = MainModelContextAppender {
        tools: engine.turn_loop.tool_registry(),
        session_id: session.metadata.id.clone(),
        session_dir: session.paths.dir.clone(),
        delegation_activity: Some(activity_rx),
        delegation_projection_baselines: baselines,
        observed_delegation_baseline: None,
        background_completion_delivery_ids: Vec::new(),
        background_completion_until_seq: 0,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
    };

    engine
        .turn_loop
        .run_session_turn_with_context_hooks(
            SessionTurnRequest {
                current_session_id: Some(session.metadata.id.clone()),
                current_turn_id: Some("turn_2".into()),
                system_prompt: "system".into(),
                history: vec![empty],
                user_text: "continue".into(),
                user_attachments: Vec::new(),
                skill_instructions: Vec::new(),
            },
            Vec::new(),
            &mut |_| {},
            None,
            SessionTurnHooks::new(None, Some(&mut appender), None),
        )
        .await
        .unwrap();

    let requests = provider.requests().await;
    let snapshots = requests[0]
        .messages
        .iter()
        .filter_map(|message| {
            let (source, _, text) = message.model_context_snapshot()?;
            (*source == ModelContextSource::Delegation).then_some(text)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        snapshots.len(),
        2,
        "one old baseline plus one coalesced delta"
    );
    let latest = snapshots.last().unwrap();
    for child_id in child_ids {
        assert!(latest.contains(&child_id));
    }
}

#[tokio::test]
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
    let conversation = non_context_session_messages(&messages);
    assert_eq!(conversation.len(), 4);
    let committed_user_text = text_content(conversation[0]);
    assert!(committed_user_text.contains("<interrupted_turn_context>"));
    assert!(committed_user_text.contains(r#""text":"continue now""#));
    assert_eq!(text_content(conversation[2]), "fresh request");

    let projection = replay_turn_journal(session.read_turn_journal().await);
    assert!(projection.unresolved_tail().is_none());
}

#[tokio::test]
async fn recovery_replays_exact_journaled_model_context_before_current_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![response_step(
        "continued answer",
        vec![ProviderEvent::AssistantMessageCompleted {
            text: "continued answer".into(),
        }],
    )]));
    let (engine, store) = build_test_engine(&dir, provider.clone());
    let mut session = create_test_session(&store, "session_cac4e001").await;
    let appended_at = Utc::now() - chrono::Duration::days(1);
    let frozen = SessionTurnMessage::model_context(
        ModelContextSource::Runtime,
        "<runtime_context>\ncurrent_date: 2026-06-28 Sunday\ntimezone: Asia/Shanghai\n</runtime_context>",
    );
    let (source, fingerprint, text) = frozen.model_context_snapshot().unwrap();
    let mut writer = session.open_turn_journal_writer().await.unwrap();
    writer
        .append(
            "turn_1",
            appended_at,
            TurnJournalEventKind::TurnStarted,
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            appended_at,
            TurnJournalEventKind::UserInputAccepted {
                text: "interrupted request".into(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            appended_at,
            TurnJournalEventKind::ModelContextAppended {
                source: *source,
                fingerprint: fingerprint.to_string(),
                text: text.to_string(),
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();
    writer
        .append(
            "turn_1",
            appended_at,
            TurnJournalEventKind::TurnFinished {
                status: TurnJournalStatus::Failed,
            },
            TurnJournalFlush::Immediate,
        )
        .await
        .unwrap();

    engine
        .run_turn(&mut session, "continue now", |_| {})
        .await
        .unwrap();

    let requests = provider.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].messages.first(), Some(&frozen));
    let canonical = session_messages_to_provider_turn_messages(
        session.read_messages().await.unwrap(),
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    );
    assert_eq!(canonical.first(), Some(&frozen));
    let projection = replay_turn_journal(session.read_turn_journal().await);
    let committed = projection.turns.last().unwrap();
    assert_eq!(committed.model_context.first().unwrap().source, *source);
    assert_eq!(
        committed.model_context.first().unwrap().fingerprint,
        fingerprint
    );
    assert_eq!(committed.model_context.first().unwrap().text, text);
}

#[test]
fn recovered_model_context_folds_replayed_prefix_across_failed_turns() {
    fn snapshot(
        source: ModelContextSource,
        text: &str,
        appended_at: chrono::DateTime<Utc>,
    ) -> TurnJournalModelContext {
        let message = SessionTurnMessage::model_context(source, text);
        let (source, fingerprint, text) = message.model_context_snapshot().unwrap();
        TurnJournalModelContext {
            source: *source,
            fingerprint: fingerprint.to_string(),
            text: text.to_string(),
            appended_at,
        }
    }

    fn turn(turn_id: &str, model_context: Vec<TurnJournalModelContext>) -> TurnJournalTurn {
        TurnJournalTurn {
            turn_id: turn_id.into(),
            started_at: None,
            accepted_at: None,
            finished_at: None,
            status: Some(TurnJournalStatus::Failed),
            original_user_request: None,
            canonical_user_content_hash: None,
            canonical_user_first_text: None,
            model_context,
            skill_instructions: Vec::new(),
            compaction_assets: Vec::new(),
            assistant_text: String::new(),
            assistant_completed: false,
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            non_streaming_fallbacks: Vec::new(),
        }
    }

    let first_at = Utc::now() - chrono::Duration::minutes(2);
    let second_at = first_at + chrono::Duration::minutes(1);
    let runtime = snapshot(
        ModelContextSource::Runtime,
        "<runtime_context>day one</runtime_context>",
        first_at,
    );
    let background = snapshot(
        ModelContextSource::BackgroundProcess,
        "<background_processes>[]</background_processes>",
        first_at,
    );
    let delegation = snapshot(
        ModelContextSource::Delegation,
        "<delegations>[]</delegations>",
        second_at,
    );
    let first = turn("turn_1", vec![runtime.clone(), background.clone()]);
    let second = turn(
        "turn_2",
        vec![
            TurnJournalModelContext {
                appended_at: second_at,
                ..runtime.clone()
            },
            TurnJournalModelContext {
                appended_at: second_at,
                ..background.clone()
            },
            delegation.clone(),
        ],
    );

    let recovered = super::recovered_model_context(&[&first, &second]);
    assert_eq!(recovered.len(), 3);
    assert_eq!(
        recovered
            .iter()
            .map(|message| message.message.model_context_snapshot().unwrap().0)
            .copied()
            .collect::<Vec<_>>(),
        vec![
            ModelContextSource::Runtime,
            ModelContextSource::BackgroundProcess,
            ModelContextSource::Delegation,
        ]
    );
    assert_eq!(recovered[0].completed_at, first_at);
    assert_eq!(recovered[1].completed_at, first_at);
    assert_eq!(recovered[2].completed_at, second_at);
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
                model_context: Vec::new(),
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
                model_context: Vec::new(),
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
                model_context: Vec::new(),
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
            model_context: Vec::new(),
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
            model_context: Vec::new(),
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

#[tokio::test]
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

#[tokio::test(start_paused = true)]
async fn turn_journal_emitter_flushes_delta_by_timer_without_next_delta() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut emitter = TurnJournalEmitter::new(tx, Duration::from_millis(5), 1024);
    tokio::task::yield_now().await;

    emitter.assistant_delta("partial".into());
    tokio::time::advance(Duration::from_millis(4)).await;
    tokio::task::yield_now().await;
    assert!(
        rx.try_recv().is_err(),
        "assistant delta must remain buffered before the configured interval"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;

    let command = rx.recv().await.unwrap();
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
    assert_eq!(requests[0].retry_count_override, None);
    assert!(requests[0].stream);
    assert_eq!(
        requests[0].stream_output_mode,
        crate::api::ProviderStreamOutputMode::Buffered
    );
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

    let projection =
        session_compaction_transcript_projection_with_memory_mode(&messages, 4_096, false);
    let recap = session_messages_to_turn_transcript_with_memory_mode(&messages, false);
    for serialized in [
        serde_json::to_string(&projection.full).unwrap(),
        serde_json::to_string(&recap).unwrap(),
    ] {
        assert!(!serialized.to_ascii_lowercase().contains("memory"));
        assert!(serialized.contains("private tool input omitted"));
        assert!(serialized.contains("private tool output omitted"));
        assert!(!serialized.contains("PRIVATE_MEMORY_INPUT"));
        assert!(!serialized.contains("PRIVATE_MEMORY_OUTPUT"));
    }
}

#[test]
fn parse_compaction_summary_outcome_requires_committed_and_active_shape() {
    let committed_transcript = vec![TurnMessage {
        role: "user".into(),
        content: "historical request".into(),
    }];
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(2),
        prior_committed_summary: None,
        committed_transcript: Some(&committed_transcript),
        committed_transcript_with_large_tool_results_omitted: Some(&committed_transcript),
        committed_transcript_with_tool_results_omitted: Some(&committed_transcript),
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
fn compaction_request_rejects_empty_transcript_before_provider_call() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, _) = build_test_engine(&dir, provider);
    let empty = Vec::<TurnMessage>::new();
    let inputs = CompactionSummaryInputs {
        audit: test_compaction_audit_context(CompactionAuditScope::Committed),
        committed_start_index: Some(0),
        committed_end_index: Some(2),
        prior_committed_summary: None,
        committed_transcript: Some(&empty),
        committed_transcript_with_large_tool_results_omitted: Some(&empty),
        committed_transcript_with_tool_results_omitted: Some(&empty),
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
        .prepare_compaction_summary_request(&inputs)
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("committed transcript must not be an empty collection"));
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
        0,
        true,
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
        0,
        true,
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
        0,
        true,
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
        0,
        true,
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
        true,
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
async fn persisted_provider_window_replays_new_canonical_tail_without_reprojection() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee18").await;
    let large_result = "EXACT_TOOL_RESULT-".to_string() + &"A".repeat(20_000);
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "already covered"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "covered answer"),
            NewSessionMessage::new(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::tool_use(
                    "toolu_tail",
                    "file_read",
                    json!({"path": "large.log"}),
                )],
            ),
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result(
                    "toolu_tail",
                    large_result.clone(),
                )],
            ),
        ])
        .await
        .unwrap();
    let mut compaction =
        SessionCompactionState::from_committed_summary(2, "covered summary".into(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: None,
        canonical_message_until: 2,
        messages: vec![SessionTurnMessage::user_text("STABLE_COMPACT_WINDOW")],
    }));
    session.update_compaction(compaction).await.unwrap();

    let (system_prompt, history) = compacted_context_for_turn(
        "system",
        &session.read_metadata().await.unwrap(),
        session.read_messages().await.unwrap(),
        16,
        16,
        0,
        8,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
        true,
    )
    .unwrap();
    let rendered = serde_json::to_string(&history).unwrap();

    assert_eq!(system_prompt, "system");
    assert_eq!(
        history[0],
        SessionTurnMessage::user_text("STABLE_COMPACT_WINDOW")
    );
    assert!(rendered.contains(&large_result));
    assert!(!rendered.contains("large tool_result omitted"));
}

#[tokio::test]
async fn recovery_provider_window_projects_safely_across_identity_change() {
    const RECOVERY_MARKER: &str = "RECOVERY-NEUTRAL-MARKER";
    const PRIVATE_REPLAY_MARKER: &str = "PRIVATE-REPLAY-MARKER";
    const CANONICAL_TAIL_MARKER: &str = "CANONICAL-TAIL-MARKER";

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee2b").await;
    session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::Assistant,
            CANONICAL_TAIL_MARKER,
        )])
        .await
        .unwrap();
    let old_identity = ProviderReplayIdentity {
        protocol: ProviderReplayProtocol::OpenAiResponses,
        model: "old-model".into(),
    };
    let current_identity = ProviderReplayIdentity {
        protocol: ProviderReplayProtocol::AnthropicMessages,
        model: "current-model".into(),
    };
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: Some(old_identity),
        recovery_turn_id: Some("turn_failed".into()),
        recovery_base_message_count: Some(0),
        pending_turn: None,
        canonical_message_until: 0,
        messages: vec![SessionTurnMessage {
            role: "assistant".into(),
            content: vec![SessionTurnContentBlock::text(RECOVERY_MARKER)],
            provider_replay: Some(ProviderReplayState::OpenAiResponses {
                model: Some("old-model".into()),
                items: vec![json!({"opaque": PRIVATE_REPLAY_MARKER})],
            }),
        }],
    }));
    session.update_compaction(compaction).await.unwrap();
    let metadata = session.read_metadata().await.unwrap();
    let messages = session.read_messages().await.unwrap();

    let (_, history) = compacted_context_for_turn(
        "system",
        &metadata,
        messages.clone(),
        usize::MAX,
        usize::MAX,
        4,
        4096,
        ProviderHistoryMediaPolicy::Placeholder,
        Some(current_identity.clone()),
        true,
    )
    .unwrap();
    let rendered = serde_json::to_string(&history).unwrap();
    assert!(rendered.contains(RECOVERY_MARKER));
    assert!(rendered.contains(CANONICAL_TAIL_MARKER));
    assert!(!rendered.contains(PRIVATE_REPLAY_MARKER));

    let projection = project_provider_context(
        "system",
        metadata.compaction.as_ref().unwrap(),
        &messages,
        vec![SessionTurnMessage::user_text("next request")],
        ActiveProjectionContext {
            turn_id: "turn_next",
            base_message_count: messages.len(),
        },
        ProviderProjectionBudget {
            tail_token_limit: usize::MAX,
            tail_hard_token_limit: usize::MAX,
            tail_previous_real_user_turns: 4,
            tool_result_raw_max_chars: 4096,
        },
        ProviderHistoryMediaPolicy::Placeholder,
        Some(current_identity),
        0,
        true,
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();
    assert!(rendered.contains(RECOVERY_MARKER));
    assert!(rendered.contains(CANONICAL_TAIL_MARKER));
    assert!(!rendered.contains(PRIVATE_REPLAY_MARKER));
}

#[tokio::test]
async fn disabled_authority_filters_historical_compaction_notice_without_rewriting_wal() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee28").await;
    let historical = compacted_committed_summary_message("historical summary", true).unwrap();
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: None,
        canonical_message_until: 0,
        messages: vec![historical],
    }));
    session.update_compaction(compaction).await.unwrap();
    let metadata = session.read_metadata().await.unwrap();

    let (_, projected) = compacted_context_for_turn(
        "system",
        &metadata,
        Vec::new(),
        usize::MAX,
        usize::MAX,
        0,
        128,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
        false,
    )
    .unwrap();

    let projected = serde_json::to_string(&projected).unwrap();
    assert!(projected.contains("historical summary"));
    assert!(!projected.contains("runtime file-edit authority"));
    assert!(!projected.contains("required_read"));
    let persisted = session.read_metadata().await.unwrap();
    let persisted = serde_json::to_string(
        &persisted
            .compaction
            .as_ref()
            .and_then(|state| state.provider_history.as_ref())
            .unwrap()
            .messages,
    )
    .unwrap();
    assert!(persisted.contains("runtime file-edit authority"));
    assert!(persisted.contains("required_read"));
}

#[tokio::test]
async fn provider_wal_rolls_back_only_when_request_never_started() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee2a").await;
    let candidate = vec![SessionTurnMessage::user_text("definitely unsent")];
    let delivery_seq = Arc::new(AtomicU64::new(0));
    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_unsent".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: true,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: delivery_seq,
        provider_replay_identity: None,
    };

    preflight
        .provider_request_ready(&candidate, 0)
        .await
        .unwrap();
    preflight
        .provider_request_abandoned_before_send()
        .await
        .unwrap();
    drop(preflight);

    assert!(session.read_metadata().await.unwrap().compaction.is_none());
    assert!(!tokio::fs::try_exists(&session.paths.provider_history_json)
        .await
        .unwrap());

    let mut preflight = PreflightCompactor {
        engine: &engine,
        session: &mut session,
        active_start_index: 0,
        turn_id: "turn_started".into(),
        base_message_count: 0,
        active_projection_compacted: false,
        provider_context_anchor: None,
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: true,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
    };
    preflight
        .provider_request_ready(&candidate, 0)
        .await
        .unwrap();
    preflight.provider_request_started(&candidate).unwrap();
    preflight
        .provider_request_abandoned_before_send()
        .await
        .unwrap();
    drop(preflight);

    let retained = session
        .read_metadata()
        .await
        .unwrap()
        .compaction
        .and_then(|state| state.provider_history)
        .unwrap();
    assert_eq!(retained.messages, candidate);
}

#[tokio::test]
async fn pending_provider_window_reconciles_canonical_tail_after_post_commit_failure() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee19").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "covered user"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "covered tool progress"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "final assistant tail"),
        ])
        .await
        .unwrap();
    let mut compaction =
        SessionCompactionState::from_committed_summary(1, "covered summary".into(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(1),
        }),
        canonical_message_until: 2,
        messages: vec![SessionTurnMessage::user_text("EXACT_LAST_PROVIDER_REQUEST")],
    }));
    session.update_compaction(compaction).await.unwrap();

    let (_, history) = compacted_context_for_turn(
        "system",
        &session.read_metadata().await.unwrap(),
        session.read_messages().await.unwrap(),
        16,
        16,
        0,
        8,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
        true,
    )
    .unwrap();

    assert_eq!(
        history,
        vec![
            SessionTurnMessage::user_text("EXACT_LAST_PROVIDER_REQUEST"),
            SessionTurnMessage::assistant_text("final assistant tail"),
        ]
    );
}

#[tokio::test]
async fn uncommitted_pending_provider_window_discards_unaccepted_response_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee20").await;
    let exact_request = SessionTurnMessage::user_text("EXACT_LAST_PROVIDER_REQUEST");
    let unaccepted_response = SessionTurnMessage::assistant_text("UNACCEPTED_LATE_RESPONSE");
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(1),
        }),
        canonical_message_until: 2,
        messages: vec![exact_request.clone(), unaccepted_response],
    }));
    session.update_compaction(compaction).await.unwrap();
    let projection = TurnJournalProjection {
        warnings: Vec::new(),
        turns: vec![TurnJournalTurn {
            turn_id: "turn_1".into(),
            started_at: None,
            accepted_at: None,
            finished_at: None,
            status: Some(TurnJournalStatus::Cancelled),
            original_user_request: None,
            canonical_user_content_hash: None,
            canonical_user_first_text: None,
            model_context: Vec::new(),
            skill_instructions: Vec::new(),
            compaction_assets: Vec::new(),
            assistant_text: "UNACCEPTED_LATE_RESPONSE".into(),
            assistant_completed: true,
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            non_streaming_fallbacks: Vec::new(),
        }],
    };

    engine
        .reconcile_pending_provider_history(&mut session, &projection, &[])
        .await
        .unwrap();

    let metadata = session.read_metadata().await.unwrap();
    let provider_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .unwrap();
    assert!(provider_history.pending_turn.is_none());
    assert_eq!(provider_history.recovery_turn_id.as_deref(), Some("turn_1"));
    assert_eq!(provider_history.canonical_message_until, 0);
    assert_eq!(provider_history.messages, vec![exact_request.clone()]);
    let (_, projected) = compacted_context_for_turn(
        "system",
        &metadata,
        Vec::new(),
        usize::MAX,
        usize::MAX,
        0,
        128,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
        true,
    )
    .unwrap();
    assert_eq!(projected, vec![exact_request]);
}

#[tokio::test]
async fn committed_pending_provider_window_preserves_later_shell_tail() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (engine, store) = build_test_engine(&dir, provider);
    let mut session = create_test_session(&store, "session_c0ffee1c").await;
    session
        .append_messages(&[
            NewSessionMessage::text(SessionMessageRole::User, "committed request"),
            NewSessionMessage::text(SessionMessageRole::Assistant, "committed answer"),
            NewSessionMessage::text(
                SessionMessageRole::User,
                "<user_shell_command>later shell tail</user_shell_command>",
            ),
        ])
        .await
        .unwrap();
    let mut compaction =
        SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());
    compaction.provider_history = Some(Box::new(CompactedProviderHistory {
        replay_identity: None,
        recovery_turn_id: None,
        recovery_base_message_count: None,
        pending_turn: Some(PendingProviderHistoryTurn {
            turn_id: "turn_1".into(),
            base_message_count: 0,
            provider_request_message_count: Some(2),
        }),
        canonical_message_until: 2,
        messages: vec![
            SessionTurnMessage::user_text("committed request"),
            SessionTurnMessage::assistant_text("committed answer"),
        ],
    }));
    session.update_compaction(compaction).await.unwrap();
    let projection = TurnJournalProjection {
        warnings: Vec::new(),
        turns: vec![TurnJournalTurn {
            turn_id: "turn_1".into(),
            started_at: None,
            accepted_at: None,
            finished_at: None,
            status: Some(TurnJournalStatus::Committed),
            original_user_request: None,
            canonical_user_content_hash: None,
            canonical_user_first_text: None,
            model_context: Vec::new(),
            skill_instructions: Vec::new(),
            compaction_assets: Vec::new(),
            assistant_text: String::new(),
            assistant_completed: false,
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            non_streaming_fallbacks: Vec::new(),
        }],
    };
    let canonical_messages = session.read_messages().await.unwrap();

    engine
        .reconcile_pending_provider_history(&mut session, &projection, &canonical_messages)
        .await
        .unwrap();

    let metadata = session.read_metadata().await.unwrap();
    let provider_history = metadata
        .compaction
        .as_ref()
        .and_then(|compaction| compaction.provider_history.as_ref())
        .unwrap();
    assert!(provider_history.pending_turn.is_none());
    assert!(provider_history.recovery_turn_id.is_none());
    assert_eq!(provider_history.canonical_message_until, 2);
    let (_, projected) = compacted_context_for_turn(
        "system",
        &metadata,
        canonical_messages,
        usize::MAX,
        usize::MAX,
        0,
        128,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
        true,
    )
    .unwrap();
    let projected = serde_json::to_string(&projected).unwrap();
    assert_eq!(projected.matches("later shell tail").count(), 1);
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
        true,
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
    let summary_tokens = compacted_committed_summary_message(summary, true)
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
        0,
        true,
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
            0,
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
            0,
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
    let summary_tokens = estimate_compacted_committed_summary_message_tokens(&prior_summary, true);
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
            0,
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
        summary: "recovered committed summary".into(),
        active_turn_summary: None,
        active_turn: None,
        preserve_provider_history: false,
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
                provider_messages: &[],
                active_suffix: vec![SessionTurnMessage::user_text("continue")],
                turn_id: "turn_1",
                base_message_count: messages.len(),
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
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
    assert_eq!(metadata.recapped_until, 0);
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
    let checkpoint = CompactionCheckpoint {
        schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
        audit_ids: vec!["compact_bad_hash".into()],
        summary_start_index: 0,
        summary_end_index: messages.len(),
        summary_segment_hash: "wrong_hash".into(),
        summary: "recovered committed summary".into(),
        active_turn_summary: None,
        active_turn: None,
        preserve_provider_history: false,
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
                provider_messages: &[],
                active_suffix: vec![SessionTurnMessage::user_text("continue")],
                turn_id: "turn_1",
                base_message_count: messages.len(),
                active_projection_compacted: false,
                runtime_projection_tokens: 0,
                protected_active_tail_segments: 0,
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
            0,
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
async fn subagent_projection_ignores_running_progress_but_changes_at_terminal_state() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_d011e9a5").await;
    let delegation_store = DelegationStore::new(session.paths.dir.clone());
    let delegation = delegation_store
        .create(DelegationCreateRequest {
            parent_session_id: session.metadata.id.clone(),
            parent_turn_id: "turn_1".into(),
            owner_agent_id: AgentId::new("agent-a").unwrap(),
            title: "verify cache semantics".into(),
            role: "verifier".into(),
            objective: "private objective".into(),
            constraints: Vec::new(),
        })
        .await
        .unwrap();
    delegation_store.start(&delegation.id).await.unwrap();
    let before_progress = delegation_summary_projection(&session.paths.dir)
        .await
        .unwrap()
        .unwrap();

    delegation_store
        .update_progress(
            &delegation.id,
            DelegationUpdate {
                current_step: Some("frequent internal step".into()),
                summary: "private-pulse-7f31 that must stay pull-only".into(),
                artifacts: Vec::new(),
            },
        )
        .await
        .unwrap();
    let after_progress = delegation_summary_projection(&session.paths.dir)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(after_progress, before_progress);
    assert!(!after_progress.contains("frequent internal step"));
    assert!(!after_progress.contains("private-pulse-7f31"));

    delegation_store
        .complete(
            &delegation.id,
            DelegationResult {
                status: DelegationStatus::Completed,
                summary: "terminal summary is model-relevant".into(),
                changed_files: vec!["src/example.rs".into()],
                artifacts: Vec::new(),
                error_summary: None,
                completed_at: Utc::now(),
            },
        )
        .await
        .unwrap();
    let terminal = delegation_summary_projection(&session.paths.dir)
        .await
        .unwrap()
        .unwrap();

    assert_ne!(terminal, after_progress);
    assert!(terminal.contains("completed"));
    assert!(terminal.contains("terminal summary is model-relevant"));
    assert!(terminal.contains("src/example.rs"));
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
    session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            "previous request",
        )])
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
async fn preflight_preserves_persisted_context_before_auto_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (mut engine, store) = build_test_engine(&dir, provider);
    engine.context_window = 200_000;
    engine.compaction.auto_compact_ctx_ratio = 1.0;
    engine.compaction.tail_hard_ctx_ratio = 0.30;
    let mut session = create_test_session(&store, "session_d011e9b4").await;
    let process_id = "proc_00000001";
    let active_suffix = vec![
        SessionTurnMessage::model_context(
            ModelContextSource::BackgroundProcess,
            format!(
                "<background_processes>process_id={process_id} state=running</background_processes>"
            ),
        ),
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
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: false,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
    };
    let mut system_prompt = "system".to_string();
    let mut provider_messages = active_suffix;

    preflight
        .before_provider_request(&mut system_prompt, &mut provider_messages, &mut |_event| {})
        .await
        .unwrap();

    let rendered = serde_json::to_string(&provider_messages).unwrap();
    assert!(rendered.contains("<background_processes>"));
    assert!(rendered.contains(process_id));
    assert!(rendered.contains(&"A".repeat(1_000)));
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
            0,
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
            0,
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

    let protected_payload = "P".repeat(8_000);
    active.extend([
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_context".into(),
                name: "code_run".into(),
                input: json!({"script":"produce protected output"}),
            }],
        },
        SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
            tool_use_id: "toolu_context".into(),
            content: protected_payload,
        }]),
    ]);
    let segments = active_provider_safe_segments(&active);
    let protected_start = segments[segments.len() - 1].start;
    let exact_mandatory_budget = estimate_session_turn_messages_tokens(&active[..1])
        .saturating_add(estimate_session_turn_messages_tokens(
            &active[protected_start..],
        ));

    let protected_plan = engine
        .build_active_turn_plan(&metadata, &active, "turn_1", 0, exact_mandatory_budget, 1)
        .unwrap()
        .expect("raw protected tail should force both older segments into the summary");
    assert_eq!(protected_plan.summary_end_segment, 2);
}

#[test]
fn provider_projection_keeps_protected_context_tool_result_raw() {
    let older_result = "OLDER_RESULT".repeat(64);
    let protected_result = "PROTECTED_RESULT".repeat(64);
    let active = vec![
        SessionTurnMessage::user_text("current task"),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_old".into(),
                name: "lookup".into(),
                input: json!({}),
            }],
        },
        SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
            tool_use_id: "toolu_old".into(),
            content: older_result.clone(),
        }]),
        SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_context".into(),
                name: "lookup".into(),
                input: json!({}),
            }],
        },
        SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
            tool_use_id: "toolu_context".into(),
            content: protected_result.clone(),
        }]),
    ];
    let state = SessionCompactionState::from_committed_summary(0, String::new(), Utc::now());

    let projection = project_provider_context(
        "system",
        &state,
        &[],
        active,
        ActiveProjectionContext {
            turn_id: "turn_context",
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
        1,
        true,
    );
    let rendered = serde_json::to_string(&projection.messages).unwrap();

    assert!(!rendered.contains(&older_result));
    assert!(rendered.contains("large tool_result omitted"));
    assert!(rendered.contains(&protected_result));
    assert_eq!(projection.protected_tail_start_index, Some(3));
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
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: false,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
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
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: false,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
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
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: false,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
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
        context_window_recovery_requested: false,
        context_window_recovery_tail_marker: None,
        history_replaced_since_last_check: false,
        frozen_provider_history_prefix_len: 0,
        capture_provider_history: false,
        last_compacted_provider_history: None,
        provider_compaction_before_pending_request: None,
        provider_compaction_before_started_request: None,
        provider_compaction_before_turn: None,
        provider_history_before_turn: Vec::new(),
        provider_compaction_for_context_retry: None,
        provider_compaction_before_clean_retry: None,
        provider_response_accepted_in_turn: false,
        background_completion_delivery_seq: Arc::new(AtomicU64::new(0)),
        provider_replay_identity: None,
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
            0,
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
            0,
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

#[tokio::test]
async fn model_context_is_excluded_from_memory_review_and_recap_transcripts() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(Vec::new()));
    let (_engine, store) = build_test_engine(&dir, provider);
    let session = create_test_session(&store, "session_cac4e004").await;
    let mut context = SessionTurnMessage::model_context(
        ModelContextSource::Runtime,
        "<runtime_context>must not enter memory</runtime_context>",
    );
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![context.content.remove(0).into()],
        ),
        test_message(
            1,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("real request")],
        ),
        test_message(
            2,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("real answer")],
        ),
    ];

    let mut metadata = session.read_metadata().await.unwrap();
    metadata.message_count = messages.len();
    let memory = build_memory_review_transcript(&metadata, messages.clone(), 10).unwrap();
    let recap = session_messages_to_turn_transcript(&messages);

    assert_eq!(memory.len(), 2);
    assert_eq!(recap.len(), 2);
    let rendered = format!("{memory:?}{recap:?}");
    assert!(rendered.contains("real request"));
    assert!(!rendered.contains("must not enter memory"));
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
            SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::ModelContext { .. } => "",
            SessionTurnContentBlock::Image { .. }
            | SessionTurnContentBlock::Document { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::InvalidToolUse { .. }
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

#[tokio::test]
async fn preserved_history_media_survives_session_reload() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("agents"));
    let mut session = create_test_session(&store, "session_1a2b3c4d").await;
    session
        .append_messages(&[
            NewSessionMessage::new(
                SessionMessageRole::User,
                vec![
                    SessionContentBlock::text("继续查看上一轮附件"),
                    SessionContentBlock::image("image/png", "IMAGE_BASE64"),
                    SessionContentBlock::Document {
                        media_type: "application/pdf".into(),
                        data: "PDF_BASE64".into(),
                        filename: Some("brief.pdf".into()),
                    },
                ],
            ),
            NewSessionMessage::text(SessionMessageRole::Assistant, "已查看"),
        ])
        .await
        .unwrap();

    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = "session_1a2b3c4d".parse().unwrap();
    let resumed = store
        .load_existing_session(&agent, &session_id)
        .await
        .unwrap();
    let projected = session_messages_to_provider_turn_messages(
        resumed.read_messages().await.unwrap(),
        ProviderHistoryMediaPolicy::Preserve,
        None,
    );
    let rendered = serde_json::to_string(&projected).unwrap();

    assert!(rendered.contains("IMAGE_BASE64"));
    assert!(rendered.contains("PDF_BASE64"));
    assert!(rendered.contains("brief.pdf"));
    assert!(!rendered.contains("image attachment media_type"));
    assert!(!rendered.contains("document attachment media_type"));
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

fn responses_replay_identity() -> ProviderReplayIdentity {
    ProviderReplayIdentity {
        protocol: ProviderReplayProtocol::OpenAiResponses,
        model: "test-model".into(),
    }
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
        model: Some("test-model".into()),
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
        Some(responses_replay_identity()),
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
            model: Some("test-model".into()),
            items: replay_items
        })
    );
}

#[test]
fn replay_generation_does_not_resurrect_after_model_switch_back() {
    let assistant = |index, model: &str, marker: &str| {
        let mut message = test_message(
            index,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text(format!("answer-{marker}"))],
        );
        message.provider_replay = Some(ProviderReplayState::OpenAiResponses {
            model: Some(model.into()),
            items: vec![json!({"type":"reasoning", "marker":marker})],
        });
        message
    };
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("a1")],
        ),
        assistant(1, "model-a", "old-a"),
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("b1")],
        ),
        assistant(3, "model-b", "b"),
        test_message(
            4,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("a2")],
        ),
        assistant(5, "model-a", "new-a"),
    ];

    let projected = session_messages_to_provider_turn_messages(
        messages,
        ProviderHistoryMediaPolicy::Preserve,
        Some(ProviderReplayIdentity {
            protocol: ProviderReplayProtocol::OpenAiResponses,
            model: "model-a".into(),
        }),
    );

    assert_eq!(projected[1].provider_replay, None);
    assert_eq!(projected[3].provider_replay, None);
    assert!(projected[5].provider_replay.is_some());
}

#[test]
fn chat_continuation_replay_survives_later_ordinary_same_model_assistant() {
    let continuation_trigger = "继续，从上一条回复被截断处继续，不要重复已写内容。";
    let mut continued = test_message(
        1,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("partial answer")],
    );
    continued.provider_replay = Some(ProviderReplayState::OpenAiChatCompletions {
        model: "test-model".into(),
        messages: vec![
            json!({"role":"assistant", "content":"partial"}),
            json!({"role":"user", "content":continuation_trigger}),
            json!({"role":"assistant", "content":"answer"}),
        ],
    });
    let ordinary = test_message(
        3,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("ordinary answer")],
    );
    let messages = vec![
        test_message(
            0,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("first request")],
        ),
        continued,
        test_message(
            2,
            SessionMessageRole::User,
            vec![SessionContentBlock::text("second request")],
        ),
        ordinary,
    ];
    let identity = ProviderReplayIdentity {
        protocol: ProviderReplayProtocol::OpenAiChatCompletions,
        model: "test-model".into(),
    };

    let projected = session_messages_to_provider_turn_messages(
        messages.clone(),
        ProviderHistoryMediaPolicy::Preserve,
        Some(identity.clone()),
    );

    assert!(projected[1].provider_replay.is_some());
    assert!(projected[3].provider_replay.is_none());

    let mut switched = messages;
    switched[3].model = "other-model".into();
    let projected_after_switch = session_messages_to_provider_turn_messages(
        switched,
        ProviderHistoryMediaPolicy::Preserve,
        Some(identity),
    );
    assert!(projected_after_switch[1].provider_replay.is_none());
}

#[test]
fn responses_legacy_unbound_and_wrong_model_replay_are_canonical_only() {
    let assistant = |index, model| {
        let mut message = test_message(
            index,
            SessionMessageRole::Assistant,
            vec![SessionContentBlock::text("canonical")],
        );
        message.provider_replay = Some(ProviderReplayState::OpenAiResponses {
            model,
            items: vec![json!({"type":"reasoning", "private":true})],
        });
        message
    };
    for message in [assistant(0, None), assistant(0, Some("other-model".into()))] {
        let projected = session_messages_to_provider_turn_messages(
            vec![message],
            ProviderHistoryMediaPolicy::Preserve,
            Some(responses_replay_identity()),
        );

        assert_eq!(projected[0].provider_replay, None);
        assert!(matches!(
            &projected[0].content[0],
            SessionTurnContentBlock::Text { text } if text == "canonical"
        ));
    }
}

#[test]
fn cross_protocol_history_drops_replay_before_budgeting_without_rewriting_session() {
    let mut assistant = test_message(
        1,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("visible answer")],
    );
    assistant.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        model: Some("test-model".into()),
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
        Some(responses_replay_identity()),
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
        Some(responses_replay_identity()),
    );
    assert_eq!(responses_tail_tokens, canonical_tail_tokens);
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
            Some(responses_replay_identity()),
        ),
        0
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
        model: Some("test-model".into()),
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
        model: Some("test-model".into()),
        items: vec![json!({"type":"reasoning","encrypted_content":"old-replay"})],
    });
    let mut suffix_assistant = test_message(
        3,
        SessionMessageRole::Assistant,
        vec![SessionContentBlock::text("new answer")],
    );
    suffix_assistant.provider_replay = Some(ProviderReplayState::OpenAiResponses {
        model: Some("test-model".into()),
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
        Some(responses_replay_identity()),
        0,
        true,
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
        0,
        true,
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
                    model: Some("test-model".into()),
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
        model: Some("test-model".into()),
        items: vec![json!({
            "type": "reasoning",
            "encrypted_content": "R".repeat(4_000)
        })],
    });

    let canonical_tokens = estimated_session_message_tokens_projected(
        [&canonical],
        None,
        Some(responses_replay_identity()),
    );
    let replay_tokens = estimated_session_message_tokens_projected(
        [&replay],
        None,
        Some(responses_replay_identity()),
    );

    assert!(replay_tokens > canonical_tokens);
}
