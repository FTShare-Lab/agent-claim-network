//! subagent 工具、等待协议与 delegation runtime 上下文。
//!
//! 实现 delegation 的创建、查询、等待、steer 与进度更新工具。

use super::*;

impl ToolRegistry {
    pub(crate) fn subscribe_delegation_activity_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<tokio::sync::watch::Receiver<u64>>, ToolError> {
        let Some(host) = &self.delegation_host else {
            return Ok(None);
        };
        Ok(Some(host.runner_for(session_id)?.subscribe_activity()))
    }

    pub async fn abandon_delegations_for_session(
        &self,
        session_id: &SessionId,
        reason: &str,
    ) -> Result<usize, ToolError> {
        let Some(host) = &self.delegation_host else {
            return Ok(0);
        };
        let runner = host.runner_for(session_id)?;
        let abandoned = runner
            .abandon_unfinished_for_session(session_id, reason)
            .await
            .map_err(|err| ToolError::Delegation(err.to_string()))?;
        Ok(abandoned.len())
    }

    pub async fn abandon_delegations_for_session_best_effort(
        &self,
        session_id: &SessionId,
        reason: &str,
    ) -> usize {
        let Some(host) = &self.delegation_host else {
            return 0;
        };
        let runner = match host.runner_for(session_id) {
            Ok(runner) => runner,
            Err(error) => {
                log::warn!(
                    target: "tool",
                    "best-effort abandon delegation runner lookup failed for session {}: {error:#}",
                    session_id
                );
                return 0;
            }
        };
        runner
            .abandon_unfinished_for_session_best_effort(session_id, reason)
            .await
            .len()
    }

    pub(super) async fn create_subagent(
        &self,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<Value, ToolError> {
        if context
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(ToolError::Interrupted);
        }
        let host = self.delegation_host()?;
        let session_id = context.current_session_id.ok_or_else(|| {
            ToolError::InvalidArgs("create_subagent 需要当前 parent session".into())
        })?;
        let args: CreateDelegationArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let title = non_empty_string(args.title, "title")?;
        let role = non_empty_string(args.role, "role")?;
        let objective = non_empty_string(args.objective, "objective")?;
        let parent_turn_id = context.current_turn_id.ok_or_else(|| {
            ToolError::InvalidArgs("create_subagent 需要当前 parent turn id".into())
        })?;
        let _session_guard = self
            .lock_parent_session_for_delegation_create(host, &session_id)
            .await?;
        let request = DelegationCreateRequest {
            parent_session_id: session_id.clone(),
            parent_turn_id,
            owner_agent_id: host.owner_agent_id.clone(),
            title,
            role,
            objective,
            constraints: args.constraints.unwrap_or_default(),
        };
        let runner = host.runner_for(&session_id)?;
        let metadata = match runner
            .create_cancellable(request, context.cancellation.clone())
            .await
        {
            Ok(metadata) => metadata,
            Err(crate::delegation::DelegationRunnerError::Interrupted) => {
                return Err(ToolError::Interrupted);
            }
            Err(error) => return Err(ToolError::Delegation(error.to_string())),
        };
        Ok(json!({
            "subagent": metadata.summary(),
        }))
    }

    pub(super) async fn list_subagents(
        &self,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<Value, ToolError> {
        let _args: ListDelegationsArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let host = self.delegation_host()?;
        let session_id = context.current_session_id.ok_or_else(|| {
            ToolError::InvalidArgs("list_subagents 需要当前 parent session".into())
        })?;
        let runner = host.runner_for(&session_id)?;
        let page = runner
            .list_page(DEFAULT_LIST_DELEGATIONS_LIMIT)
            .await
            .map_err(|err| ToolError::Delegation(err.to_string()))?;
        Ok(json!({
            "subagents": page.summaries,
            "omitted": page.omitted,
        }))
    }

    /// 等待当前 session 中已选定的一组 subagent 进入终态，不轮询持久化文件。
    pub(super) async fn wait_subagents(
        &self,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<Value, ToolError> {
        let host = self.delegation_host()?;
        let session_id = context.current_session_id.ok_or_else(|| {
            ToolError::InvalidArgs("wait_subagents 需要当前 parent session".into())
        })?;
        let args: WaitSubagentsArgs =
            serde_json::from_value(input).map_err(|err| ToolError::InvalidArgs(err.to_string()))?;
        let runner = host.runner_for(&session_id)?;
        let wait_config = runner.wait_config();
        let timeout = wait_timeout_from_args(args.timeout_secs, wait_config)?;
        let until = args.until.unwrap_or_default();
        // 先订阅，再读取快照；这样状态恰好在首次读取后变化时也不会漏唤醒。
        let mut activity_rx = runner.subscribe_activity();
        let waited_ids = match args.subagent_ids {
            Some(ids) => parse_explicit_wait_subagent_ids(ids, runner.store()).await?,
            None => {
                let active = runner
                    .store()
                    .list_metadata()
                    .await
                    .map_err(|err| ToolError::Delegation(err.to_string()))?
                    .into_iter()
                    .filter(|metadata| !metadata.status.is_terminal())
                    .map(|metadata| metadata.id)
                    .collect::<Vec<_>>();
                if active.is_empty() {
                    return Ok(wait_subagents_response(
                        WaitSubagentsOutcome::NoActiveSubagents,
                        until,
                        Vec::new(),
                        WaitSubagentsState::default(),
                    ));
                }
                active
            }
        };
        let cancellation = context.cancellation.unwrap_or_default();
        let deadline = time::Instant::now() + timeout;

        loop {
            let state = load_wait_subagents_state(runner.store(), &waited_ids).await?;
            if until.is_satisfied(&state, waited_ids.len()) {
                return Ok(wait_subagents_response(
                    WaitSubagentsOutcome::ConditionMet,
                    until,
                    waited_ids,
                    state,
                ));
            }

            tokio::select! {
                _ = time::sleep_until(deadline) => {
                    let state = load_wait_subagents_state(runner.store(), &waited_ids).await?;
                    let outcome = if until.is_satisfied(&state, waited_ids.len()) {
                        WaitSubagentsOutcome::ConditionMet
                    } else {
                        WaitSubagentsOutcome::Timeout
                    };
                    return Ok(wait_subagents_response(outcome, until, waited_ids, state));
                }
                changed = activity_rx.changed() => {
                    if changed.is_err() {
                        return Err(ToolError::Delegation(
                            "wait_subagents activity channel closed unexpectedly".into(),
                        ));
                    }
                }
                _ = cancellation.cancelled() => {
                    return Err(ToolError::Interrupted);
                }
            }
        }
    }

    pub(super) async fn read_subagent(
        &self,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<Value, ToolError> {
        let host = self.delegation_host()?;
        let session_id = context.current_session_id.ok_or_else(|| {
            ToolError::InvalidArgs("read_subagent 需要当前 parent session".into())
        })?;
        let args: ReadDelegationArgs = serde_json::from_value(input.clone())
            .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let id = args
            .id
            .parse()
            .map_err(|e| ToolError::InvalidArgs(format!("invalid subagent id: {e}")))?;
        let mode =
            read_mode_from_json(&input).map_err(|err| ToolError::InvalidArgs(err.to_string()))?;
        let runner = host.runner_for(&session_id)?;
        let read = runner
            .store()
            .read(&id, mode)
            .await
            .map_err(|err| ToolError::Delegation(err.to_string()))?;
        serde_json::to_value(read)
            .map_err(|e| ToolError::Delegation(format!("read_subagent 序列化失败: {e}")))
    }

    pub(super) async fn steer_subagent(
        &self,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<Value, ToolError> {
        let host = self.delegation_host()?;
        let session_id = context.current_session_id.ok_or_else(|| {
            ToolError::InvalidArgs("steer_subagent 需要当前 parent session".into())
        })?;
        let args: SteerDelegationArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let id = args
            .id
            .parse()
            .map_err(|e| ToolError::InvalidArgs(format!("invalid subagent id: {e}")))?;
        let instruction = non_empty_string(args.instruction, "instruction")?;
        let runner = host.runner_for(&session_id)?;
        let metadata = runner
            .steer(&id, instruction)
            .await
            .map_err(|err| ToolError::Delegation(err.to_string()))?;
        Ok(json!({
            "subagent": metadata.summary(),
        }))
    }

    pub(super) async fn update_subagent_progress(&self, input: Value) -> Result<Value, ToolError> {
        let progress = self
            .delegation_progress
            .as_ref()
            .ok_or_else(|| ToolError::UnknownTool("update_subagent_progress".into()))?;
        let args: UpdateDelegationProgressArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let metadata = progress
            .update(
                args.current_step,
                args.summary,
                args.artifacts.unwrap_or_default(),
            )
            .await
            .map_err(|err| ToolError::Delegation(err.to_string()))?;
        Ok(json!({
            "subagent": metadata.summary(),
        }))
    }

    pub async fn delegation_runtime_context(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("workspace_root: {}", self.workspace_root.display()));
        lines.push("memory_snapshot_mode: read_only_context_no_memory_tool".to_string());
        lines.push("mcp_tools_inherited: same visible MCP tools as parent session".to_string());
        if let Some(memory_store) = &self.memory_store {
            match memory_store.read_snapshot().await {
                Ok(snapshot) => {
                    if !snapshot.user_entries.is_empty() {
                        let joined = snapshot.user_entries.join("; ");
                        lines.push(format!(
                            "user_memory_snapshot: {}",
                            truncate_chars(&joined, 1200).0
                        ));
                    }
                    if !snapshot.memory_entries.is_empty() {
                        let joined = snapshot.memory_entries.join("; ");
                        lines.push(format!(
                            "project_memory_snapshot: {}",
                            truncate_chars(&joined, 1200).0
                        ));
                    }
                }
                Err(err) => {
                    lines.push(format!("memory_snapshot_error: {err:#}"));
                }
            }
        }
        lines.join("\n")
    }

    pub(super) async fn lock_parent_session_for_delegation_create(
        &self,
        host: &DelegationToolHost,
        session_id: &SessionId,
    ) -> Result<FileLockGuard, ToolError> {
        let paths = SessionPaths::new(&host.agent_home, session_id);
        let guard = FileLockGuard::lock_exclusive(&paths.session_lock)
            .await
            .map_err(|err| {
                ToolError::Delegation(format!(
                    "锁定 parent session 状态失败 {}: {err:#}",
                    paths.session_lock.display()
                ))
            })?;
        let metadata: SessionMetadata = crate::storage::read_yaml(&paths.session_yaml)
            .await
            .map_err(|err| {
                ToolError::Delegation(format!(
                    "读取 parent session 状态失败 {}: {err}",
                    paths.session_yaml.display()
                ))
            })?;
        if metadata.status == SessionStatus::Open
            && metadata.closed_at.is_none()
            && metadata.finalized_at.is_none()
        {
            return Ok(guard);
        }
        Err(ToolError::Delegation(format!(
            "parent session {} 当前状态为 {:?}，不能创建 subagent",
            session_id, metadata.status
        )))
    }

    pub(super) fn delegation_host(&self) -> Result<&DelegationToolHost, ToolError> {
        self.delegation_host
            .as_deref()
            .ok_or_else(|| ToolError::UnknownTool("create_subagent".into()))
    }
}

pub(super) fn delegation_tool_definitions(config: DelegationRunnerConfig) -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "create_subagent".into(),
            description: concat!(
                "Create a session-scoped internal subagent for longer background work. ",
                "Users may call a subagent an agent, 代理, 子代理, agent team, or agent团队; ",
                "phrases like “开个agent团队来帮我调研这个问题” refer to creating subagents. ",
                "The user cannot talk to it directly; provide a clear role, objective, and constraints. ",
                "Each subagent has the subagent-only update_subagent_progress tool for one-way progress reporting visible through list/read and the TUI; ",
                "put decision fallbacks in constraints because it cannot ask the parent and wait for a reply. ",
                "Do not use this for tiny immediate tool calls."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Short human-readable title"
                    },
                    "role": {
                        "type": "string",
                        "description": "Internal role, such as code researcher, verifier, or implementation assistant"
                    },
                    "objective": {
                        "type": "string",
                        "description": "Specific task the subagent should complete"
                    },
                    "constraints": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Bounded context, constraints, and expected reporting requirements"
                    }
                },
                "required": ["title", "role", "objective"],
                "additionalProperties": false
            }),
        },
        ToolDefinition {
            name: "list_subagents".into(),
            description: concat!(
                "List bounded summaries of subagents in the current session. ",
                "When the user asks about running agents, subagents, 代理, 子代理, agent teams, or agent团队, ",
                "they are referring to these subagents. Returns status, lifecycle timestamps ",
                "(created_at/updated_at/started_at/completed_at), progress, result_ref, and changed_files; ",
                "use read_subagent for explicit details."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
        },
        ToolDefinition {
            name: "wait_subagents".into(),
            description: format!(
                "Wait without polling for selected subagents to reach terminal states. Omit subagent_ids to snapshot all currently queued/running subagents in this session; subagents created later do not join this wait. until defaults to any_terminal; all_terminal waits for every selected subagent, regardless of whether each completed, failed, or was abandoned. Progress updates only refresh the internal condition check and do not return control. timeout_secs defaults to {} seconds and must be between {} and {} seconds. Do not call this tool more than once in one assistant response.",
                config.wait.default_timeout.as_secs(),
                config.wait.min_timeout.as_secs(),
                config.wait.max_timeout.as_secs(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "subagent_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional current-session subagent IDs. Omit to snapshot every currently queued/running subagent. IDs must be unique."
                    },
                    "until": {
                        "type": "string",
                        "enum": ["any_terminal", "all_terminal"],
                        "description": "Terminal condition. Defaults to any_terminal."
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "minimum": config.wait.min_timeout.as_secs(),
                        "maximum": config.wait.max_timeout.as_secs(),
                        "description": "Optional bounded wait timeout in seconds."
                    }
                },
                "required": [],
                "additionalProperties": false
            }),
        },
        ToolDefinition {
            name: "read_subagent".into(),
            description: concat!(
                "Explicitly read a subagent summary, final result, bounded event tail, or bounded transcript tail. ",
                "If the user asks what an agent, subagent, delegation, 代理, 子代理, agent team, or agent团队 found, ",
                "read the corresponding subagent. Prefer summary first to avoid flooding the main context; ",
                "To determine whether a subagent has compacted its internal context, read summary.compaction_summary ",
                "or transcript_tail compaction_boundary records; result and events_tail are not sufficient for that question. ",
                "Use transcript_tail only for explicit debugging or when the user asks what the subagent actually did internally; ",
                "workspace artifact paths from results should be read with file_read. ",
                "Do not include limit for summary or result mode; limit is only meaningful for events_tail/transcript_tail, ",
                "and max_chars is only meaningful for transcript_tail."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "mode": {
                        "type": "string",
                        "enum": ["summary", "result", "events_tail", "transcript_tail"],
                        "description": "Default summary. Use summary without limit for status, lifecycle timestamps, result_ref, changed_files, and compaction_summary when present. To check whether subagent compaction happened, use summary.compaction_summary or transcript_tail compaction_boundary; result/events_tail are not sufficient. result also does not use limit. events_tail returns product progress events. transcript_tail returns bounded internal subagent transcript for explicit debugging."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "description": "Only for mode=events_tail or transcript_tail. Do not include this field for summary or result."
                    },
                    "max_chars": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 80000,
                        "description": "Only for mode=transcript_tail. Bounds total returned transcript JSON characters."
                    }
                },
                "required": ["id"],
                "additionalProperties": false
            }),
        },
        ToolDefinition {
            name: "steer_subagent".into(),
            description: concat!(
                "Append structured steering to a queued or running subagent. ",
                "If the user gives follow-up instructions for an agent, subagent, delegation, 代理, 子代理, agent team, or agent团队, ",
                "translate them into concise steering for the relevant subagent. ",
                "This is not a direct user chat channel, has no acknowledgement, and is only delivered before a future subagent model request; ",
                "the subagent can reach terminal before consuming it and terminal subagents cannot be modified."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "instruction": {
                        "type": "string",
                        "description": "Concise steering instruction, new constraint, or context summary"
                    }
                },
                "required": ["id", "instruction"],
                "additionalProperties": false
            }),
        },
    ]
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WaitSubagentsUntil {
    AnyTerminal,
    AllTerminal,
}

impl Default for WaitSubagentsUntil {
    fn default() -> Self {
        Self::AnyTerminal
    }
}

impl WaitSubagentsUntil {
    fn as_str(self) -> &'static str {
        match self {
            Self::AnyTerminal => "any_terminal",
            Self::AllTerminal => "all_terminal",
        }
    }

    fn is_satisfied(self, state: &WaitSubagentsState, total: usize) -> bool {
        match self {
            Self::AnyTerminal => !state.terminal.is_empty(),
            Self::AllTerminal => total > 0 && state.pending.is_empty(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum WaitSubagentsOutcome {
    ConditionMet,
    Timeout,
    NoActiveSubagents,
}

impl WaitSubagentsOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::ConditionMet => "condition_met",
            Self::Timeout => "timeout",
            Self::NoActiveSubagents => "no_active_subagents",
        }
    }
}

#[derive(Default)]
struct WaitSubagentsState {
    terminal: Vec<DelegationMetadata>,
    pending: Vec<DelegationId>,
}

fn wait_timeout_from_args(
    timeout_secs: Option<u64>,
    config: DelegationWaitConfig,
) -> Result<Duration, ToolError> {
    let seconds = timeout_secs.unwrap_or(config.default_timeout.as_secs());
    let requested = Duration::from_secs(seconds);
    if requested < config.min_timeout || requested > config.max_timeout {
        return Err(ToolError::InvalidArgs(format!(
            "wait_subagents timeout_secs 必须在 {} 到 {} 秒之间",
            config.min_timeout.as_secs(),
            config.max_timeout.as_secs(),
        )));
    }
    Ok(requested)
}

async fn parse_explicit_wait_subagent_ids(
    raw_ids: Vec<String>,
    store: &DelegationStore,
) -> Result<Vec<DelegationId>, ToolError> {
    if raw_ids.is_empty() {
        return Err(ToolError::InvalidArgs(
            "wait_subagents subagent_ids 不能为空；省略该字段以等待当前活跃 subagent".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut ids = Vec::with_capacity(raw_ids.len());
    for raw_id in raw_ids {
        if !seen.insert(raw_id.clone()) {
            return Err(ToolError::InvalidArgs(format!(
                "wait_subagents subagent_ids 包含重复 id: {raw_id}"
            )));
        }
        let id = raw_id
            .parse::<DelegationId>()
            .map_err(|err| ToolError::InvalidArgs(format!("invalid subagent id: {err}")))?;
        match store.load(&id).await {
            Ok(_) => ids.push(id),
            Err(DelegationStoreError::NotFound(_)) => {
                return Err(ToolError::InvalidArgs(format!(
                    "wait_subagents subagent 不属于当前 session 或不存在: {id}"
                )));
            }
            Err(err) => return Err(ToolError::Delegation(err.to_string())),
        }
    }
    Ok(ids)
}

async fn load_wait_subagents_state(
    store: &DelegationStore,
    ids: &[DelegationId],
) -> Result<WaitSubagentsState, ToolError> {
    let mut state = WaitSubagentsState::default();
    for id in ids {
        match store.load(id).await {
            Ok(metadata) if metadata.status.is_terminal() => state.terminal.push(metadata),
            Ok(_) => state.pending.push(id.clone()),
            Err(DelegationStoreError::NotFound(_)) => {
                return Err(ToolError::InvalidArgs(format!(
                    "wait_subagents subagent 不属于当前 session 或不存在: {id}"
                )));
            }
            Err(err) => return Err(ToolError::Delegation(err.to_string())),
        }
    }
    Ok(state)
}

fn wait_subagents_response(
    outcome: WaitSubagentsOutcome,
    until: WaitSubagentsUntil,
    waited_ids: Vec<DelegationId>,
    state: WaitSubagentsState,
) -> Value {
    let terminal_subagents = state
        .terminal
        .into_iter()
        .map(|metadata| {
            json!({
                "id": metadata.id,
                "status": metadata.status,
                "updated_at": metadata.updated_at,
                "completed_at": metadata.completed_at,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "outcome": outcome.as_str(),
        "until": until.as_str(),
        "waited_subagent_ids": waited_ids,
        "terminal_subagents": terminal_subagents,
        "pending_subagent_ids": state.pending,
    })
}

pub(super) fn update_subagent_progress_definition() -> ToolDefinition {
    ToolDefinition {
        name: "update_subagent_progress".into(),
        description: "Internal subagent-only tool for writing bounded progress visible to the parent agent and TUI. Keep summaries concise.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "current_step": {
                    "type": "string",
                    "description": "Short label for the current step"
                },
                "summary": {
                    "type": "string",
                    "description": "Bounded progress summary"
                },
                "artifacts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string" },
                            "description": { "type": "string" }
                        },
                        "required": ["path", "description"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["summary"],
            "additionalProperties": false
        }),
    }
}

fn non_empty_string(value: String, name: &str) -> Result<String, ToolError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArgs(format!("{name} 不能为空")));
    }
    Ok(trimmed.to_string())
}
