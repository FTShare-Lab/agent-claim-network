use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::compaction::{
    transcript_entry_for_message, transcript_source_for_message, DelegationPreflightCompactor,
};
use super::runner::{
    DelegationExecutionContext, DelegationExecutionError, DelegationExecutionOutcome,
    DelegationExecutor, DelegationProgressSink,
};
use super::types::{
    truncate_text, DelegationArtifactRef, DelegationEventKind, DelegationMetadata,
    DelegationSteering, DelegationTranscriptEntry, DelegationTranscriptKind,
    DelegationTranscriptMessageSource,
};
use crate::api::{
    AgentTurnLoop, ModelContextSource, ProviderAdapter, ProviderRuntimeChainId, SessionAttachment,
    SessionTurn, SessionTurnContentBlock, SessionTurnContextAppender, SessionTurnEvent,
    SessionTurnEventRecorder, SessionTurnHooks, SessionTurnMessage, SessionTurnPreflight,
    SessionTurnRequest, StructuredJsonCaller, ToolExecutionOutcome,
};
use crate::attachment::AttachmentLimits;
use crate::claim::SessionId;
use crate::config::{SessionCompactionConfig, DEFAULT_SESSION_DELEGATION_MAX_TOOL_LOOP_TURNS};
use crate::prompt::{PromptError, PromptRegistry};
use crate::tool::ToolRegistry;

const RUNTIME_STEERING_BATCH_LIMIT: usize = 8;
const PROMPT_SUBAGENTS_SYSTEM: &str = "subagents_system";

/// runner 的 abort 会直接 drop `DelegationExecutor::execute` future。这个 guard 在该路径
/// 把 owner cleanup 交给仍存活的 Tokio runtime，避免已登记的 subagent terminal 失去生命周期清理。
struct OwnerProcessCleanupOnDrop {
    tools: Arc<ToolRegistry>,
    session_id: SessionId,
    subagent_id: String,
    armed: bool,
}

impl OwnerProcessCleanupOnDrop {
    fn new(tools: Arc<ToolRegistry>, session_id: SessionId, subagent_id: String) -> Self {
        Self {
            tools,
            session_id,
            subagent_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OwnerProcessCleanupOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            log::warn!(
                target: "delegation",
                "subagent {} dropped without Tokio runtime; process cleanup cannot be scheduled",
                self.subagent_id
            );
            return;
        };
        let tools = Arc::clone(&self.tools);
        let session_id = self.session_id.clone();
        let subagent_id = self.subagent_id.clone();
        runtime.spawn(async move {
            tools
                .cleanup_processes_for_owner(&session_id, Some(&subagent_id))
                .await;
        });
    }
}

#[derive(Clone)]
pub struct LlmDelegationExecutor {
    provider: Arc<dyn ProviderAdapter>,
    tool_registry_template: ToolRegistry,
    prompt_registry: Arc<PromptRegistry>,
    json_caller: Arc<StructuredJsonCaller>,
    max_tokens: u32,
    max_tool_loop_turns: usize,
    compaction: SessionCompactionConfig,
    context_window: usize,
    attachment_limits: AttachmentLimits,
    tool_input_journal_preview_chars: usize,
    tool_output_journal_preview_chars: usize,
}

impl LlmDelegationExecutor {
    pub fn new(
        provider: Arc<dyn ProviderAdapter>,
        tool_registry_template: ToolRegistry,
        prompt_registry: Arc<PromptRegistry>,
        json_caller: Arc<StructuredJsonCaller>,
        max_tokens: u32,
        compaction: SessionCompactionConfig,
        context_window: usize,
    ) -> Self {
        Self {
            provider,
            tool_registry_template,
            prompt_registry,
            json_caller,
            max_tokens,
            max_tool_loop_turns: DEFAULT_SESSION_DELEGATION_MAX_TOOL_LOOP_TURNS,
            compaction,
            context_window,
            attachment_limits: AttachmentLimits::default(),
            tool_input_journal_preview_chars: 2048,
            tool_output_journal_preview_chars: 4096,
        }
    }

    pub fn with_attachment_limits(mut self, limits: AttachmentLimits) -> Self {
        self.attachment_limits = limits;
        self
    }

    pub fn with_max_tool_loop_turns(mut self, max_tool_loop_turns: usize) -> Self {
        self.max_tool_loop_turns = max_tool_loop_turns;
        self
    }

    pub fn with_tool_journal_preview_limits(
        mut self,
        input_max_chars: usize,
        output_max_chars: usize,
    ) -> Self {
        self.tool_input_journal_preview_chars = input_max_chars;
        self.tool_output_journal_preview_chars = output_max_chars;
        self
    }
}

#[async_trait]
impl DelegationExecutor for LlmDelegationExecutor {
    async fn begin_task(
        &self,
        context: &DelegationExecutionContext,
    ) -> Result<(), DelegationExecutionError> {
        let tools = self.tool_registry_template.clone().for_delegation(None);
        tools
            .begin_file_read_state_checkpoint(
                &context.metadata.parent_session_id,
                context.metadata.id.as_str(),
            )
            .await
            .map_err(DelegationExecutionError::new)
    }

    async fn finish_task(
        &self,
        context: &DelegationExecutionContext,
        committed: bool,
    ) -> Result<(), DelegationExecutionError> {
        let tools = self.tool_registry_template.clone().for_delegation(None);
        let result = if committed {
            tools
                .commit_file_read_state_checkpoint(
                    &context.metadata.parent_session_id,
                    context.metadata.id.as_str(),
                )
                .await
        } else {
            tools
                .rollback_file_read_state_checkpoint(
                    &context.metadata.parent_session_id,
                    context.metadata.id.as_str(),
                )
                .await
        };
        if let Err(error) = result {
            tools
                .clear_delegation_file_read_state(
                    &context.metadata.parent_session_id,
                    context.metadata.id.as_str(),
                )
                .await;
            return Err(DelegationExecutionError::new(error));
        }
        Ok(())
    }

    async fn execute(
        &self,
        context: DelegationExecutionContext,
        progress: DelegationProgressSink,
    ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
        let runtime_fallback_scope = context.runtime_fallback_scope();
        let metadata = context.metadata;
        let cleanup_tools = Arc::new(self.tool_registry_template.clone());
        let mut process_cleanup_guard = OwnerProcessCleanupOnDrop::new(
            Arc::clone(&cleanup_tools),
            metadata.parent_session_id.clone(),
            metadata.id.to_string(),
        );
        let last_steering_seq = context
            .initial_steering
            .iter()
            .map(|steering| steering.seq)
            .max()
            .unwrap_or(0);
        progress
            .update(
                Some("started".into()),
                format!("subagent {} started", metadata.id),
                Vec::new(),
            )
            .await
            .map_err(|err| DelegationExecutionError::new(err.to_string()))?;

        let tools = Arc::new(
            self.tool_registry_template
                .clone()
                .for_delegation(Some(progress.clone())),
        );
        let tool_specs = tools
            .definitions()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let turn_loop = AgentTurnLoop::new(self.provider.clone(), tools, self.max_tokens)
            .with_max_tool_loop_turns(self.max_tool_loop_turns)
            .with_attachment_limits(self.attachment_limits)
            .with_tool_journal_preview_limits(
                self.tool_input_journal_preview_chars,
                self.tool_output_journal_preview_chars,
            );
        let runtime_chain_id = ProviderRuntimeChainId::new();
        let runtime_context = self
            .tool_registry_template
            .delegation_runtime_context()
            .await;
        let system_prompt = delegation_system_prompt(
            &self.prompt_registry,
            &metadata,
            &runtime_context,
            self.tool_registry_template.memory_enabled(),
            self.tool_registry_template.file_edit_authority_enabled(),
        )
        .map_err(|err| {
            DelegationExecutionError::new(format!("渲染 subagents system prompt 失败: {err}"))
        })?;
        let request = SessionTurnRequest {
            current_session_id: Some(metadata.parent_session_id.clone()),
            current_turn_id: Some(metadata.id.to_string()),
            system_prompt,
            history: Vec::new(),
            user_text: delegation_user_text(&metadata, &context.initial_steering),
            user_attachments: Vec::<SessionAttachment>::new(),
            skill_instructions: Vec::new(),
        };
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<DelegationEventKind>();
        let event_progress = progress.clone();
        let event_metadata_id = metadata.id.clone();
        let event_recorder = tokio::spawn(async move {
            while let Some(kind) = event_rx.recv().await {
                if let Err(err) = event_progress.record_event(kind).await {
                    log::warn!(
                        target: "delegation",
                        "{event_metadata_id} tool event 落盘失败: {err:#}"
                    );
                }
            }
        });
        let mut transcript_recorder = DelegationTranscriptRecorder {
            progress: progress.clone(),
        };
        let mut compactor = DelegationPreflightCompactor::new(
            metadata.clone(),
            progress.clone(),
            self.prompt_registry.clone(),
            self.json_caller.clone(),
            tool_specs,
            self.compaction.clone(),
            self.context_window,
            runtime_fallback_scope.clone(),
            self.tool_registry_template.file_edit_authority_enabled(),
        );
        let turn_result = {
            let mut tool_names = BTreeMap::<String, String>::new();
            let mut emit = |event: SessionTurnEvent| match event {
                SessionTurnEvent::Warning { message } => {
                    log::warn!(target: "delegation", "{} warning: {message}", metadata.id);
                }
                SessionTurnEvent::ToolCallStarted {
                    id, name, summary, ..
                } => {
                    tool_names.insert(id, name.clone());
                    let _ = event_tx.send(DelegationEventKind::ToolStarted {
                        tool_name: name,
                        summary,
                    });
                }
                SessionTurnEvent::ToolCallCompleted {
                    id,
                    summary,
                    outcome,
                    ..
                } => {
                    let tool_name = tool_names.remove(&id).unwrap_or(id);
                    let _ =
                        event_tx.send(delegation_tool_completed_event(tool_name, summary, outcome));
                }
                SessionTurnEvent::CompactionFailed { error } => {
                    let _ = event_tx.send(DelegationEventKind::CompactionFailed { error });
                }
                SessionTurnEvent::ToolCallProgress { .. }
                | SessionTurnEvent::ToolCallInterrupted { .. }
                | SessionTurnEvent::ToolCallSkipped { .. }
                | SessionTurnEvent::ContextUsageUpdated { .. }
                | SessionTurnEvent::CompactionStarted { .. }
                | SessionTurnEvent::CompactionCompleted { .. }
                | SessionTurnEvent::CompactionSkipped { .. }
                | SessionTurnEvent::AssistantTextDelta { .. }
                | SessionTurnEvent::AssistantMessageCompleted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. } => {}
            };
            let mut steering_preflight = DelegationSteeringPreflight {
                progress: progress.clone(),
                last_seq: last_steering_seq,
            };
            let background_tools = Arc::new(self.tool_registry_template.clone());
            let mut context_appender = DelegationBackgroundContextAppender::new(
                Arc::clone(&background_tools),
                metadata.parent_session_id.clone(),
                metadata.id.to_string(),
            );
            let mut composite_preflight = DelegationCompositePreflight {
                steering: &mut steering_preflight,
                compactor: &mut compactor,
                tools: background_tools,
                session_id: metadata.parent_session_id.clone(),
                subagent_id: metadata.id.to_string(),
                history_replaced_since_last_check: false,
            };
            turn_loop
                .run_session_turn_with_context_and_runtime_chain_hooks(
                    request,
                    Vec::new(),
                    runtime_chain_id,
                    runtime_fallback_scope,
                    &mut emit,
                    None,
                    SessionTurnHooks::new(
                        Some(&mut transcript_recorder),
                        Some(&mut context_appender),
                        Some(&mut composite_preflight),
                    ),
                )
                .await
        };
        turn_loop.discard_runtime_chain(runtime_chain_id).await;
        drop(event_tx);
        if let Err(err) = event_recorder.await {
            log::warn!(target: "delegation", "{} event recorder join 失败: {err:#}", metadata.id);
        }
        cleanup_tools
            .cleanup_processes_for_owner(&metadata.parent_session_id, Some(metadata.id.as_str()))
            .await;
        process_cleanup_guard.disarm();
        let outcome = async {
            let turn = turn_result.map_err(|err| DelegationExecutionError::new(err.to_string()))?;
            let summary = assistant_text_from_turn(&turn);
            let summary = if summary.trim().is_empty() {
                "subagent completed without textual result".to_string()
            } else {
                summary
            };
            let (reported_changed_files, artifacts) = extract_result_references(&summary);
            let changed_files = merge_changed_files(&turn, reported_changed_files);
            progress
                .update(Some("completed".into()), summary.clone(), artifacts.clone())
                .await
                .map_err(|err| DelegationExecutionError::new(err.to_string()))?;
            Ok(DelegationExecutionOutcome {
                summary,
                changed_files,
                artifacts,
            })
        }
        .await;
        outcome
    }
}

fn delegation_tool_completed_event(
    tool_name: String,
    summary: String,
    outcome: ToolExecutionOutcome,
) -> DelegationEventKind {
    DelegationEventKind::ToolCompleted {
        tool_name,
        summary,
        outcome: Some(outcome),
    }
}

fn delegation_system_prompt(
    prompt_registry: &PromptRegistry,
    metadata: &DelegationMetadata,
    runtime_context: &str,
    memory_enabled: bool,
    file_edit_authority_enabled: bool,
) -> Result<String, PromptError> {
    prompt_registry.render(
        PROMPT_SUBAGENTS_SYSTEM,
        minijinja::context! {
            subagent_id => metadata.id.to_string(),
            parent_session_id => metadata.parent_session_id.to_string(),
            parent_turn_id => metadata.parent_turn_id.to_string(),
            owner_agent_id => metadata.owner_agent_id.to_string(),
            title => metadata.title.as_str(),
            role => metadata.role.as_str(),
            runtime_context => runtime_context,
            memory_enabled => memory_enabled,
            file_edit_authority_enabled => file_edit_authority_enabled,
        },
    )
}

fn delegation_user_text(metadata: &DelegationMetadata, steering: &[DelegationSteering]) -> String {
    let mut text = String::new();
    text.push_str("Objective:\n");
    text.push_str(&metadata.objective);
    if !metadata.constraints.is_empty() {
        text.push_str("\n\nConstraints and context:\n");
        for constraint in &metadata.constraints {
            text.push_str("- ");
            text.push_str(constraint);
            text.push('\n');
        }
    }
    if !steering.is_empty() {
        text.push_str("\n\nInitial steering from parent agent:\n");
        append_steering_lines(&mut text, steering);
    }
    text
}

struct DelegationSteeringPreflight {
    progress: DelegationProgressSink,
    last_seq: u64,
}

struct DelegationCompositePreflight<'a> {
    steering: &'a mut DelegationSteeringPreflight,
    compactor: &'a mut DelegationPreflightCompactor,
    tools: Arc<ToolRegistry>,
    session_id: SessionId,
    subagent_id: String,
    history_replaced_since_last_check: bool,
}

/// child 与 main 共用 append-on-semantic-change，只改变 owner scope 与持久化目标。
struct DelegationBackgroundContextAppender {
    tools: Arc<ToolRegistry>,
    session_id: SessionId,
    subagent_id: String,
    completion_delivery_ids: Vec<crate::tool::ProcessCompletionDeliveryReceipt>,
}

impl DelegationBackgroundContextAppender {
    fn new(tools: Arc<ToolRegistry>, session_id: SessionId, subagent_id: String) -> Self {
        Self {
            tools,
            session_id,
            subagent_id,
            completion_delivery_ids: Vec::new(),
        }
    }
}

#[async_trait]
impl SessionTurnContextAppender for DelegationBackgroundContextAppender {
    async fn observe_context(
        &mut self,
        _provider_messages: &[SessionTurnMessage],
    ) -> anyhow::Result<Vec<SessionTurnMessage>> {
        self.tools
            .rollback_process_deliveries_for_owner(&self.session_id, Some(&self.subagent_id))
            .await;
        let (projection, delivery_ids) = self
            .tools
            .begin_background_process_projection_delivery_for_owner(
                &self.session_id,
                Some(&self.subagent_id),
            )
            .await;
        self.completion_delivery_ids = delivery_ids;
        let text = match projection {
            Some(text) => Some(text),
            None => Some(ToolRegistry::empty_background_process_projection()),
        };
        Ok(text
            .map(|text| {
                SessionTurnMessage::model_context(ModelContextSource::BackgroundProcess, text)
            })
            .into_iter()
            .collect())
    }

    async fn after_provider_response_success(&mut self) -> anyhow::Result<()> {
        if !self.completion_delivery_ids.is_empty() {
            self.tools
                .commit_completion_notification_delivery_for_owner(
                    &self.session_id,
                    Some(&self.subagent_id),
                    &self.completion_delivery_ids,
                )
                .await;
            self.completion_delivery_ids.clear();
        }
        Ok(())
    }
}

struct DelegationTranscriptRecorder {
    progress: DelegationProgressSink,
}

#[async_trait]
impl SessionTurnEventRecorder for DelegationTranscriptRecorder {
    async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()> {
        let kind = match event {
            SessionTurnEvent::ToolCallStarted {
                id,
                name,
                summary,
                input_preview,
                input_truncated,
            } => DelegationTranscriptKind::ToolStarted {
                id,
                name,
                summary,
                input_preview,
                input_truncated,
            },
            SessionTurnEvent::ToolCallCompleted {
                id,
                summary,
                outcome,
                output_preview,
                output_truncated,
                file_change,
            } => DelegationTranscriptKind::ToolCompleted {
                id,
                summary,
                outcome: Some(outcome),
                output_preview,
                output_truncated,
                file_change,
            },
            SessionTurnEvent::CompactionFailed { error } => {
                DelegationTranscriptKind::CompactionFailed { error }
            }
            SessionTurnEvent::Warning { .. }
            | SessionTurnEvent::ContextUsageUpdated { .. }
            | SessionTurnEvent::CompactionStarted { .. }
            | SessionTurnEvent::CompactionCompleted { .. }
            | SessionTurnEvent::CompactionSkipped { .. }
            | SessionTurnEvent::AssistantTextDelta { .. }
            | SessionTurnEvent::AssistantMessageCompleted { .. }
            | SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
            | SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
            | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
            | SessionTurnEvent::ToolCallProgress { .. }
            | SessionTurnEvent::ToolCallInterrupted { .. }
            | SessionTurnEvent::ToolCallSkipped { .. } => return Ok(()),
        };
        self.progress
            .append_transcript_entry(DelegationTranscriptEntry {
                at: chrono::Utc::now(),
                kind,
            })
            .await
            .map_err(anyhow::Error::from)
    }

    async fn record_completed_message(
        &mut self,
        message: &crate::api::CompletedSessionTurnMessage,
    ) -> anyhow::Result<()> {
        let source = transcript_source_for_message(&message.message);
        self.progress
            .append_transcript_entry(transcript_entry_for_message(
                source,
                message.message.clone(),
            ))
            .await
            .map_err(anyhow::Error::from)
    }
}

#[async_trait]
impl SessionTurnPreflight for DelegationSteeringPreflight {
    async fn before_provider_request(
        &mut self,
        _system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        let steering = self
            .progress
            .steering_after(self.last_seq, RUNTIME_STEERING_BATCH_LIMIT)
            .await?;
        if steering.is_empty() {
            return Ok(());
        }
        let mut text = String::from("<subagent_steering>\n");
        text.push_str("Parent agent steering received while this subagent was running:\n");
        if let Some(last_delivered) = append_steering_batch(&mut text, &steering, steering.len()) {
            self.last_seq = last_delivered;
        }
        text.push_str("</subagent_steering>");
        let message = SessionTurnMessage::user_text(text);
        self.progress
            .append_transcript_entry(transcript_entry_for_message(
                DelegationTranscriptMessageSource::Steering,
                message.clone(),
            ))
            .await?;
        provider_messages.push(message);
        Ok(())
    }
}

#[async_trait]
impl SessionTurnPreflight for DelegationCompositePreflight<'_> {
    async fn before_context_observation(
        &mut self,
        system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        self.steering
            .before_provider_request(system_prompt, provider_messages, emit)
            .await
    }

    async fn before_provider_request(
        &mut self,
        system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        self.compactor
            .before_provider_request(system_prompt, provider_messages, emit)
            .await?;
        if self.compactor.take_compacted_since_last_check() {
            self.history_replaced_since_last_check = true;
            self.tools
                .clear_delegation_file_read_state(&self.session_id, &self.subagent_id)
                .await;
        }
        Ok(())
    }

    fn history_replacement_expected(
        &self,
        system_prompt: &str,
        provider_messages: &[SessionTurnMessage],
    ) -> bool {
        self.compactor
            .history_replacement_expected(system_prompt, provider_messages)
    }

    fn take_history_replaced_since_last_check(&mut self) -> bool {
        std::mem::take(&mut self.history_replaced_since_last_check)
    }

    fn request_context_window_recovery(
        &mut self,
        assistant_marker: &SessionTurnMessage,
    ) -> anyhow::Result<()> {
        self.compactor
            .request_context_window_recovery(assistant_marker)
    }

    fn observe_provider_context_usage(
        &mut self,
        provider_message_count: usize,
        usage: crate::api::ContextUsageSnapshot,
    ) {
        self.steering
            .observe_provider_context_usage(provider_message_count, usage);
        self.compactor
            .observe_provider_context_usage(provider_message_count, usage);
    }

    fn clear_provider_context_usage(&mut self) {
        self.steering.clear_provider_context_usage();
        self.compactor.clear_provider_context_usage();
    }
}

fn append_steering_lines(out: &mut String, steering: &[DelegationSteering]) {
    for item in steering {
        out.push_str("- ");
        out.push_str(&item.at.to_rfc3339());
        out.push_str(" seq=");
        out.push_str(&item.seq.to_string());
        out.push_str(": ");
        out.push_str(&truncate_text(&item.instruction, 600));
        out.push('\n');
    }
}

fn append_steering_batch(
    out: &mut String,
    steering: &[DelegationSteering],
    max_items: usize,
) -> Option<u64> {
    let mut last_delivered = None;
    for item in steering.iter().take(max_items) {
        out.push_str("- ");
        out.push_str(&item.at.to_rfc3339());
        out.push_str(" seq=");
        out.push_str(&item.seq.to_string());
        out.push_str(": ");
        out.push_str(&truncate_text(&item.instruction, 600));
        out.push('\n');
        last_delivered = Some(item.seq);
    }
    let omitted = steering.len().saturating_sub(max_items);
    if omitted > 0 {
        out.push_str("- ... ");
        out.push_str(&omitted.to_string());
        out.push_str(" pending steering item(s) will be delivered on a later model turn\n");
    }
    last_delivered
}

fn extract_result_references(summary: &str) -> (Vec<String>, Vec<DelegationArtifactRef>) {
    enum Section {
        None,
        ChangedFiles,
        Artifacts,
    }

    let mut section = Section::None;
    let mut changed_files = Vec::new();
    let mut artifacts = Vec::new();
    for raw_line in summary.lines() {
        let line = raw_line.trim();
        let header = line.trim_end_matches(':').to_ascii_lowercase();
        match header.as_str() {
            "changed_files" | "changed files" => {
                section = Section::ChangedFiles;
                continue;
            }
            "artifacts" => {
                section = Section::Artifacts;
                continue;
            }
            _ if line.ends_with(':') => {
                section = Section::None;
                continue;
            }
            _ => {}
        }

        let Some(item) = line.strip_prefix("- ").or_else(|| line.strip_prefix("* ")) else {
            continue;
        };
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        match section {
            Section::ChangedFiles => {
                push_unique_changed_file(&mut changed_files, item);
            }
            Section::Artifacts => {
                if artifacts.len() < 32 {
                    let (path, description) = split_artifact_line(item);
                    artifacts.push(DelegationArtifactRef {
                        path: truncate_text(path, 240),
                        description: description.map(|value| truncate_text(value, 320)),
                    });
                }
            }
            Section::None => {}
        }
    }
    (changed_files, artifacts)
}

fn push_unique_changed_file(changed_files: &mut Vec<String>, path: &str) {
    if changed_files.len() >= 32 {
        return;
    }
    let Some(path) = normalized_changed_file_path(path) else {
        return;
    };
    if !changed_files.iter().any(|existing| existing == &path) {
        changed_files.push(path);
    }
}

fn normalized_changed_file_path(path: &str) -> Option<String> {
    let path = path.trim();
    (!path.is_empty()).then(|| truncate_text(path, 240))
}

#[derive(Debug, Default)]
struct FileMutationEvidence {
    changed_files: Vec<String>,
    successful_paths: BTreeSet<String>,
    non_changed_paths: BTreeSet<String>,
}

fn merge_changed_files(turn: &SessionTurn, reported_paths: Vec<String>) -> Vec<String> {
    let evidence = file_mutation_evidence_from_tool_results(turn);
    let mut changed_files = evidence.changed_files;
    for path in reported_paths {
        let Some(normalized) = normalized_changed_file_path(&path) else {
            continue;
        };
        let known_non_change = evidence.non_changed_paths.contains(&normalized)
            && !evidence.successful_paths.contains(&normalized);
        if !known_non_change {
            push_unique_changed_file(&mut changed_files, &normalized);
        }
    }
    changed_files
}

#[cfg(test)]
fn changed_files_from_tool_results(turn: &SessionTurn) -> Vec<String> {
    file_mutation_evidence_from_tool_results(turn).changed_files
}

fn file_mutation_evidence_from_tool_results(turn: &SessionTurn) -> FileMutationEvidence {
    let mut tools = BTreeMap::<String, (String, Option<String>)>::new();
    let mut evidence = FileMutationEvidence::default();
    for completed in &turn.messages {
        for block in &completed.message.content {
            match block {
                SessionTurnContentBlock::ToolUse { id, name, input } => {
                    if delegation_file_mutation_tool(name) {
                        let input_path = input
                            .get("path")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned);
                        tools.insert(id.clone(), (name.clone(), input_path));
                    }
                }
                SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } => {
                    let Some((name, input_path)) = tools.get(tool_use_id) else {
                        continue;
                    };
                    if !delegation_file_mutation_tool(name) {
                        continue;
                    }
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(content) else {
                        if let Some(path) =
                            input_path.as_deref().and_then(normalized_changed_file_path)
                        {
                            evidence.non_changed_paths.insert(path);
                        }
                        continue;
                    };
                    let output = value.get("output").unwrap_or(&serde_json::Value::Null);
                    let path = output
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .or(input_path.as_deref())
                        .and_then(normalized_changed_file_path);
                    let status_allows_change = match output.get("status") {
                        None => true,
                        Some(status) => status.as_str() == Some("success"),
                    };
                    let succeeded = value.get("ok").and_then(serde_json::Value::as_bool)
                        == Some(true)
                        && status_allows_change;
                    let Some(path) = path else {
                        continue;
                    };
                    if succeeded {
                        evidence.successful_paths.insert(path.clone());
                        push_unique_changed_file(&mut evidence.changed_files, &path);
                    } else {
                        evidence.non_changed_paths.insert(path);
                    }
                }
                SessionTurnContentBlock::Text { .. }
                | SessionTurnContentBlock::ModelContext { .. }
                | SessionTurnContentBlock::SkillInstructions { .. }
                | SessionTurnContentBlock::Image { .. }
                | SessionTurnContentBlock::Document { .. } => {}
            }
        }
    }
    evidence
}

fn delegation_file_mutation_tool(name: &str) -> bool {
    matches!(name, "file_write" | "file_patch")
}

fn split_artifact_line(line: &str) -> (&str, Option<&str>) {
    match line.split_once(" - ").or_else(|| line.split_once(": ")) {
        Some((path, description)) => {
            let description = description.trim();
            (
                path.trim(),
                (!description.is_empty()).then_some(description),
            )
        }
        None => (line.trim(), None),
    }
}

fn assistant_text_from_turn(turn: &SessionTurn) -> String {
    let mut text = String::new();
    for completed in &turn.messages {
        if completed.message.role != "assistant" {
            continue;
        }
        append_text_blocks(&completed.message, &mut text);
    }
    text.trim().to_string()
}

fn append_text_blocks(message: &SessionTurnMessage, out: &mut String) {
    for block in &message.content {
        match block {
            SessionTurnContentBlock::Text { text } => {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(text);
            }
            SessionTurnContentBlock::Image { .. }
            | SessionTurnContentBlock::Document { .. }
            | SessionTurnContentBlock::ModelContext { .. }
            | SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use chrono::Utc;
    use serde_json::json;

    use crate::api::CompletedSessionTurnMessage;
    use crate::claim::{AgentId, SessionId};
    use crate::delegation::{DelegationCreateRequest, DelegationId, DelegationStore};

    #[test]
    fn tool_completed_event_preserves_typed_process_and_http_outcomes() {
        for outcome in [
            ToolExecutionOutcome::ProcessExit {
                exit_code: Some(7),
                success: false,
            },
            ToolExecutionOutcome::ProcessTerminated { signal: Some(9) },
            ToolExecutionOutcome::HttpResponse { http_status: 503 },
        ] {
            let event =
                delegation_tool_completed_event("code_run".into(), "tool finished".into(), outcome);
            assert!(matches!(
                event,
                DelegationEventKind::ToolCompleted {
                    outcome: Some(recorded),
                    ..
                } if recorded == outcome
            ));
        }
    }

    #[tokio::test]
    async fn transcript_recorder_persists_bounded_file_change() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store
            .create_with_id_factory(
                DelegationCreateRequest {
                    parent_session_id: SessionId::from_str("session_aaaaaaaa")
                        .expect("valid session id"),
                    parent_turn_id: "turn-1".into(),
                    owner_agent_id: AgentId::new("agent-a").expect("valid agent id"),
                    title: "diff trace".into(),
                    role: "verifier".into(),
                    objective: "edit one file".into(),
                    constraints: Vec::new(),
                },
                || DelegationId::from_str("subagent_11111111").expect("valid subagent id"),
            )
            .await
            .expect("create subagent");
        let progress = DelegationProgressSink::for_test(store.clone(), metadata.id.clone());
        let mut recorder = DelegationTranscriptRecorder { progress };
        let file_change = crate::tool::diff::compute_file_change(
            "src/lib.rs",
            crate::tool::diff::FileChangeKind::Modified,
            "old\n",
            "new\n",
            20,
        )
        .expect("changed text should produce diff");

        recorder
            .record(SessionTurnEvent::ToolCallCompleted {
                id: "toolu_1".into(),
                summary: "tool file_patch ok".into(),
                outcome: ToolExecutionOutcome::Completed,
                output_preview: "updated src/lib.rs".into(),
                output_truncated: false,
                file_change: Some(file_change.clone()),
            })
            .await
            .expect("record tool completion");

        let entries = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript");
        let [entry] = entries.as_slice() else {
            panic!("expected exactly one transcript entry");
        };
        let DelegationTranscriptKind::ToolCompleted {
            file_change: Some(recorded),
            ..
        } = &entry.kind
        else {
            panic!("expected tool_completed with file_change");
        };
        assert_eq!(recorded, &file_change);
    }

    #[tokio::test]
    async fn transcript_recorder_persists_completed_messages_in_provider_order() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store
            .create_with_id_factory(
                DelegationCreateRequest {
                    parent_session_id: SessionId::from_str("session_aaaaaaaa")
                        .expect("valid session id"),
                    parent_turn_id: "turn-1".into(),
                    owner_agent_id: AgentId::new("agent-a").expect("valid agent id"),
                    title: "ordered transcript".into(),
                    role: "verifier".into(),
                    objective: "verify order".into(),
                    constraints: Vec::new(),
                },
                || DelegationId::from_str("subagent_11111112").expect("valid subagent id"),
            )
            .await
            .expect("create subagent");
        let progress = DelegationProgressSink::for_test(store.clone(), metadata.id.clone());
        let mut recorder = DelegationTranscriptRecorder { progress };
        let messages = vec![
            SessionTurnMessage::model_context(
                ModelContextSource::Runtime,
                "<runtime_context>date</runtime_context>",
            ),
            SessionTurnMessage::model_context(
                ModelContextSource::BackgroundProcess,
                "<background_processes>empty</background_processes>",
            ),
            SessionTurnMessage::user_text("Objective:\nverify order"),
            SessionTurnMessage::assistant_text("done"),
        ];
        for message in &messages {
            recorder
                .record_completed_message(&CompletedSessionTurnMessage::new(
                    message.clone(),
                    Utc::now(),
                ))
                .await
                .unwrap();
        }

        let entries = store
            .read_transcript_entries(&metadata.id)
            .await
            .expect("read transcript");
        let persisted = entries
            .into_iter()
            .map(|entry| match entry.kind {
                DelegationTranscriptKind::Message { source, message } => (source, message),
                other => panic!("unexpected transcript entry: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            persisted
                .iter()
                .map(|(source, _)| *source)
                .collect::<Vec<_>>(),
            vec![
                DelegationTranscriptMessageSource::ModelContext,
                DelegationTranscriptMessageSource::ModelContext,
                DelegationTranscriptMessageSource::Objective,
                DelegationTranscriptMessageSource::Assistant,
            ]
        );
        assert_eq!(
            persisted
                .into_iter()
                .map(|(_, message)| message)
                .collect::<Vec<_>>(),
            messages
        );
    }

    #[test]
    fn extracts_changed_files_and_artifacts_from_markdown_result() {
        let (changed_files, artifacts) = extract_result_references(
            r#"Done.

changed_files:
- src/lib.rs
- docs/example.md

artifacts:
- notes/scan.md: scan notes
- logs/run.txt - command log
"#,
        );

        assert_eq!(
            changed_files,
            vec!["src/lib.rs".to_string(), "docs/example.md".to_string()]
        );
        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].path, "notes/scan.md");
        assert_eq!(artifacts[0].description.as_deref(), Some("scan notes"));
        assert_eq!(artifacts[1].path, "logs/run.txt");
        assert_eq!(artifacts[1].description.as_deref(), Some("command log"));
    }

    #[test]
    fn extracts_changed_files_from_successful_file_tool_results() {
        let turn = SessionTurn {
            messages: vec![
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "assistant".into(),
                        provider_replay: None,
                        content: vec![
                            SessionTurnContentBlock::ToolUse {
                                id: "toolu_write".into(),
                                name: "file_write".into(),
                                input: json!({
                                    "path": "target/smoke.txt",
                                    "content": "ok"
                                }),
                            },
                            SessionTurnContentBlock::ToolUse {
                                id: "toolu_read".into(),
                                name: "file_read".into(),
                                input: json!({ "path": "ignored.txt" }),
                            },
                            SessionTurnContentBlock::ToolUse {
                                id: "toolu_patch_failed".into(),
                                name: "file_patch".into(),
                                input: json!({ "path": "failed.txt" }),
                            },
                            SessionTurnContentBlock::ToolUse {
                                id: "toolu_no_change".into(),
                                name: "file_write".into(),
                                input: json!({ "path": "unchanged.txt" }),
                            },
                        ],
                    },
                    Utc::now(),
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "user".into(),
                        provider_replay: None,
                        content: vec![
                            SessionTurnContentBlock::ToolResult {
                                tool_use_id: "toolu_write".into(),
                                content: json!({
                                    "ok": true,
                                    "outcome": {"kind": "completed"},
                                    "output": {
                                        "path": "target/smoke.txt"
                                    }
                                })
                                .to_string(),
                            },
                            SessionTurnContentBlock::ToolResult {
                                tool_use_id: "toolu_read".into(),
                                content: json!({
                                    "ok": true,
                                    "outcome": {"kind": "completed"},
                                    "output": {
                                        "path": "ignored.txt"
                                    }
                                })
                                .to_string(),
                            },
                            SessionTurnContentBlock::ToolResult {
                                tool_use_id: "toolu_patch_failed".into(),
                                content: json!({
                                    "ok": false,
                                    "outcome": {"kind": "business_failure"},
                                    "output": {"path": "failed.txt"},
                                    "error": "old_content not found"
                                })
                                .to_string(),
                            },
                            SessionTurnContentBlock::ToolResult {
                                tool_use_id: "toolu_no_change".into(),
                                content: json!({
                                    "ok": true,
                                    "output": {
                                        "path": "unchanged.txt",
                                        "status": "no_change"
                                    }
                                })
                                .to_string(),
                            },
                        ],
                    },
                    Utc::now(),
                ),
            ],
        };

        assert_eq!(
            changed_files_from_tool_results(&turn),
            vec!["target/smoke.txt".to_string()]
        );
    }

    #[test]
    fn tool_changed_files_keep_priority_over_reported_changed_files() {
        let turn = SessionTurn {
            messages: vec![
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "assistant".into(),
                        provider_replay: None,
                        content: vec![SessionTurnContentBlock::ToolUse {
                            id: "toolu_write".into(),
                            name: "file_write".into(),
                            input: json!({
                                "path": "target/actual.txt",
                                "content": "ok"
                            }),
                        }],
                    },
                    Utc::now(),
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "user".into(),
                        provider_replay: None,
                        content: vec![SessionTurnContentBlock::ToolResult {
                            tool_use_id: "toolu_write".into(),
                            content: json!({
                                "ok": true,
                                "outcome": {"kind": "completed"},
                                "output": {
                                    "path": "target/actual.txt"
                                }
                            })
                            .to_string(),
                        }],
                    },
                    Utc::now(),
                ),
            ],
        };
        let reported_summary = {
            let mut text = String::from("Changed files:\n");
            for idx in 0..40usize {
                text.push_str(&format!("- target/reported_{idx}.txt\n"));
            }
            text
        };
        let (reported_changed_files, _) = extract_result_references(&reported_summary);
        let changed_files = merge_changed_files(&turn, reported_changed_files);

        assert_eq!(
            changed_files.first().map(String::as_str),
            Some("target/actual.txt")
        );
        assert!(changed_files
            .iter()
            .take(8)
            .any(|path| path == "target/actual.txt"));
    }

    #[test]
    fn reported_changed_files_exclude_known_non_changes_but_keep_untracked_paths() {
        let turn = SessionTurn {
            messages: vec![
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "assistant".into(),
                        provider_replay: None,
                        content: vec![
                            SessionTurnContentBlock::ToolUse {
                                id: "toolu_no_change".into(),
                                name: "file_write".into(),
                                input: json!({ "path": "unchanged.txt" }),
                            },
                            SessionTurnContentBlock::ToolUse {
                                id: "toolu_failed".into(),
                                name: "file_patch".into(),
                                input: json!({ "path": "failed.txt" }),
                            },
                        ],
                    },
                    Utc::now(),
                ),
                CompletedSessionTurnMessage::new(
                    SessionTurnMessage {
                        role: "user".into(),
                        provider_replay: None,
                        content: vec![
                            SessionTurnContentBlock::ToolResult {
                                tool_use_id: "toolu_no_change".into(),
                                content: json!({
                                    "ok": true,
                                    "output": {
                                        "path": "unchanged.txt",
                                        "status": "no_change"
                                    }
                                })
                                .to_string(),
                            },
                            SessionTurnContentBlock::ToolResult {
                                tool_use_id: "toolu_failed".into(),
                                content: json!({
                                    "ok": false,
                                    "error": "old_content not found"
                                })
                                .to_string(),
                            },
                        ],
                    },
                    Utc::now(),
                ),
            ],
        };
        let reported = vec![
            "unchanged.txt".to_string(),
            "failed.txt".to_string(),
            "generated-by-code-run.txt".to_string(),
        ];

        assert_eq!(
            merge_changed_files(&turn, reported),
            vec!["generated-by-code-run.txt".to_string()]
        );
    }
}
