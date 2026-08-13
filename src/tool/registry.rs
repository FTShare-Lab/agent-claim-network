//! Tool registry 的构建、工具定义、权限面与统一派发。
//!
//! 对外入口保持为 `ToolRegistry`，具体工具执行下沉到同级领域模块。

use super::*;

impl ToolRegistry {
    pub fn new(cfg: &ToolConfig) -> Result<Self, ToolError> {
        let api_key_env = cfg.web.api_key_env.trim();
        Self::new_with_web_search(
            cfg,
            cfg.web.endpoint.clone(),
            std::env::var(api_key_env).ok().and_then(|v| {
                let trimmed = v.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }),
        )
    }

    pub fn new_with_web_search(
        cfg: &ToolConfig,
        web_search_endpoint: impl Into<String>,
        web_search_api_key: Option<String>,
    ) -> Result<Self, ToolError> {
        if cfg.code_run_max_output_chars == 0
            || cfg.code_run_max_output_chars > MAX_CODE_RUN_MAX_OUTPUT_CHARS
            || cfg.write_stdin_max_poll_timeout_ms < DEFAULT_CODE_RUN_MAX_YIELD_MS
            || cfg.write_stdin_max_poll_timeout_ms > MAX_WRITE_STDIN_MAX_POLL_TIMEOUT_MS
            || cfg.background_process_output_buffer_bytes == 0
            || cfg.background_process_output_buffer_bytes
                > MAX_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES
            || cfg.background_process_max_entries_per_owner == 0
            || cfg.background_process_max_entries_per_owner
                > MAX_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER
            || cfg.background_process_protected_recent_entries
                > cfg.background_process_max_entries_per_owner
            || cfg.background_process_pty_rows == 0
            || cfg.background_process_pty_cols == 0
            || cfg.background_process_pty_rows > MAX_BACKGROUND_PROCESS_PTY_DIMENSION
            || cfg.background_process_pty_cols > MAX_BACKGROUND_PROCESS_PTY_DIMENSION
            || cfg.background_process_pty_input_buffer_bytes == 0
            || cfg.background_process_pty_input_buffer_bytes
                > MAX_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES
            || cfg.background_process_pty_input_buffer_bytes > Semaphore::MAX_PERMITS
        {
            return Err(ToolError::InvalidArgs(
                "background process tool limits are invalid; load configuration through Config validation"
                    .into(),
            ));
        }
        Ok(Self {
            workspace_root: cfg.workspace_root.clone(),
            http: crate::http_client_builder().build()?,
            direct_http: crate::direct_http_client_builder().build()?,
            notes: Arc::new(Mutex::new(Vec::new())),
            web_search_endpoint: web_search_endpoint.into(),
            web_search_api_key_env: cfg.web.api_key_env.clone(),
            web_search_api_key,
            memory_store: None,
            router_client: None,
            session_search: None,
            mcp_manager: None,
            access: ToolAccessProfile::parent(),
            delegation_host: None,
            delegation_progress: None,
            path_locks: Arc::new(StdMutex::new(BTreeMap::new())),
            file_write_lock_root: None,
            read_state: Arc::new(ReadStateStore::default()),
            limits: ToolLimits::from(cfg),
            process_manager: Arc::new(ProcessManager::new(
                cfg.background_process_output_buffer_bytes,
                default_id_mint_max_attempts(),
                cfg.background_process_max_entries_per_owner,
                cfg.background_process_protected_recent_entries,
            )),
            process_owner_agent_id: "unknown-agent".into(),
            attachment_limits: AttachmentLimits::default(),
        })
    }

    pub fn with_memory_store(mut self, memory_store: Arc<dyn MemoryStore>) -> Self {
        self.memory_store = Some(memory_store);
        self
    }

    /// bootstrap 使用 session 的既有 ID mint 策略覆盖默认值；clone 后所有 profile 共享同一 manager。
    pub fn with_process_id_attempts(mut self, id_attempts: usize) -> Self {
        self.process_manager = Arc::new(ProcessManager::new(
            self.limits.background_process_output_buffer_bytes,
            id_attempts,
            self.limits.background_process_max_entries_per_owner,
            self.limits.background_process_protected_recent_entries,
        ));
        self
    }

    /// 设置受管 terminal 的稳定 owner agent identity。bootstrap 必须在派生
    /// delegation registry 前调用，因此 main 与全部 subagent 的 ProcessOwner 一致完整。
    pub fn with_process_owner_agent_id(mut self, agent_id: AgentId) -> Self {
        self.process_owner_agent_id = agent_id.to_string();
        self
    }

    pub fn with_attachment_limits(mut self, attachment_limits: AttachmentLimits) -> Self {
        self.attachment_limits = attachment_limits;
        self
    }

    /// 设置共享 base ACN home 下的文件写锁目录；不改变工作区或工具公开配置。
    pub(crate) fn with_file_write_lock_root(mut self, lock_root: PathBuf) -> Self {
        self.file_write_lock_root = Some(lock_root);
        self
    }

    pub fn with_router_client(mut self, router_client: Arc<dyn RouterClient>) -> Self {
        self.router_client = Some(router_client);
        self
    }

    pub fn with_session_search(mut self, session_search: Arc<SessionSearchService>) -> Self {
        self.session_search = Some(session_search);
        self
    }

    pub fn with_mcp_manager(mut self, mcp_manager: Arc<McpConnectionManager>) -> Self {
        self.mcp_manager = Some(mcp_manager);
        self
    }

    pub fn with_delegation_executor(
        mut self,
        agent_home: PathBuf,
        owner_agent_id: AgentId,
        executor: Arc<dyn DelegationExecutor>,
        config: DelegationRunnerConfig,
    ) -> Self {
        self.process_owner_agent_id = owner_agent_id.to_string();
        self.delegation_host = Some(Arc::new(DelegationToolHost::new(
            agent_home,
            owner_agent_id,
            executor,
            config,
        )));
        self.access = ToolAccessProfile::parent();
        self
    }

    pub fn for_delegation(mut self, progress: Option<DelegationProgressSink>) -> Self {
        self.access = ToolAccessProfile::delegation();
        self.delegation_host = None;
        self.delegation_progress = progress;
        self
    }

    pub fn for_memory_review(mut self) -> Self {
        self.access = ToolAccessProfile::memory_review();
        self.delegation_host = None;
        self.delegation_progress = None;
        self
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.definitions_with_mcp_routes().0
    }

    /// 为一次逻辑 Provider sampling 同时冻结工具定义与 MCP 路由 generation。
    /// adapter 内部 retry、fallback 与 continuation 必须复用这一份结果。
    pub(crate) fn definitions_with_mcp_routes(
        &self,
    ) -> (
        Vec<ToolDefinition>,
        BTreeMap<String, crate::mcp::tool::McpToolRoute>,
    ) {
        let web_time_guidance = current_year_web_guidance();
        let code_run_process_scope = self.code_run_process_scope_description();
        let write_stdin_description = self.write_stdin_description();
        let process_list_description = self.process_list_description();
        let mut definitions = vec![
            ToolDefinition {
                name: "code_run".into(),
                description: format!(
                    "Execute a local command under the configured workspace. This is high-permission execution, not a sandbox. The call yields after a bounded initial observation window: short commands return their exit result; longer commands return a logical process_id and keep running under ACN management. {code_run_process_scope} process_id is not an OS PID. Prefer bash unless another language is required. tty=true creates a fixed-size interactive PTY; it does not track ACN window resize."
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "script": {
                            "type": "string",
                            "description": "Code to execute"
                        },
                        "type": {
                            "type": "string",
                            "enum": ["bash", "python", "powershell"],
                            "description": "Execution type. Omit for bash."
                        },
                        "cwd": {
                            "type": "string",
                            "description": "Working directory; accepts relative paths (from the configured workspace root), absolute paths, or ~/ paths"
                        },
                        "tty": {
                            "type": "boolean",
                            "description": "Run in an interactive PTY (default false)."
                        },
                        "yield_time_ms": {
                            "type": "integer",
                            "minimum": self.limits.code_run_min_yield_ms,
                            "maximum": self.limits.code_run_max_yield_ms,
                            "description": format!(
                                "Initial observation window in milliseconds. Defaults to {}; clamped to {}..{}.",
                                self.limits.code_run_initial_yield_ms,
                                self.limits.code_run_min_yield_ms,
                                self.limits.code_run_max_yield_ms,
                            )
                        },
                        "max_output_chars": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self.limits.code_run_max_output_chars,
                            "default": self.limits.code_run_max_output_chars,
                            "description": "Maximum returned output characters per stdout/stderr stream for this call (PTY uses stdout only). Usually omit this field; truncated output advances only after provider delivery and the next poll continues from the returned cursor."
                        }
                    },
                    "required": ["script"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "write_stdin".into(),
                description: write_stdin_description,
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "process_id": { "type": "string", "pattern": "^[0-9a-f]{8}$" },
                        "chars": {
                            "type": "string",
                            "description": format!(
                                "Text to write; omit or use empty string to poll. \\u0003 sends terminal Ctrl-C: it may interrupt the current foreground job but does not terminate an interactive shell or SSH session. Non-empty writes are limited to {} UTF-8 bytes at runtime (JSON Schema string length counts Unicode characters, so it cannot express this byte limit exactly).",
                                self.limits.background_process_pty_input_buffer_bytes,
                            )
                        },
                        "terminate": {
                            "type": "boolean",
                            "default": false,
                            "description": "Hard-terminate this managed process group. Reuses the same SIGKILL lifecycle as /ps termination. A completed termination returns ok=true with outcome.kind=process_terminated and the signal; output.success remains false because the child did not exit naturally with status 0. Must not be combined with non-empty chars."
                        },
                        "yield_time_ms": {
                            "type": "integer",
                            "minimum": self.limits.code_run_min_yield_ms,
                            "maximum": self.limits.write_stdin_max_poll_timeout_ms,
                            "description": format!(
                                "Defaults to {}ms after a non-empty write and {}ms for an empty poll; values are clamped to {}..{}ms for writes and {}..{}ms for polls.",
                                self.limits.code_run_write_yield_ms,
                                self.limits.code_run_poll_yield_ms,
                                self.limits.code_run_min_yield_ms,
                                self.limits.code_run_max_yield_ms,
                                self.limits.code_run_min_yield_ms,
                                self.limits.write_stdin_max_poll_timeout_ms,
                            )
                        },
                        "max_output_chars": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self.limits.code_run_max_output_chars,
                            "default": self.limits.code_run_max_output_chars,
                            "description": "Maximum returned output characters per stdout/stderr stream for this call (PTY uses stdout only). Usually omit this field. If truncated=true, the returned cursor identifies the end of the visible prefix; after provider delivery, the next implicit poll continues from there instead of replaying the same prefix."
                        },
                        "stdout_cursor": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Must be supplied together with stderr_cursor."
                        },
                        "stderr_cursor": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Must be supplied together with stdout_cursor."
                        }
                    },
                    "required": ["process_id"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "process_list".into(),
                description: process_list_description,
                input_schema: json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "file_read".into(),
                description: "Read a file by relative or absolute path. UTF-8 text results include page metadata with the exact returned line range, total lines, EOF state, next_start, and stop reason. stop_reason=eof means the file ended; count/max_chars may continue at page.next_start when more content is needed; keyword_not_found/start_after_eof should not repeat the same request. If the effective read or keyword window contains a line that cannot be returned completely, file_read fails without granting read or write authority; use code_run to inspect that line instead. Pages of the same file version accumulate: a unique file_patch needs only its covered target/boundary lines, append needs EOF coverage, while overwrite/prepend/replace_all need complete coverage. Follow page.next_start only when the task needs more content; do not read the whole file merely because truncated=true. Images and PDFs are returned as attached media content.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Relative, absolute, or ~/ file path"
                        },
                        "start": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Start line number, 1-based"
                        },
                        "count": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Maximum number of lines to return"
                        },
                        "keyword": {
                            "type": "string",
                            "description": "Optional case-insensitive keyword to anchor the window"
                        },
                        "show_linenos": {
                            "type": "boolean",
                            "description": "Whether to include line numbers"
                        }
                    },
                    "required": ["path"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "file_patch".into(),
                description: "Replace exact text in an existing UTF-8 file. By default old_content must match exactly once and only the target plus any affected line boundary must have been returned by file_read for the current file version. If multiple matches exist, expand old_content with nearby context until it is unique. replace_all=true intentionally replaces every match and requires complete file coverage. On a read-permission error, follow required_read.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Relative, absolute, or ~/ file path"
                        },
                        "old_content": {
                            "type": "string",
                            "description": "Exact existing text block that must match uniquely"
                        },
                        "new_content": {
                            "type": "string",
                            "description": "Replacement text block"
                        },
                        "replace_all": {
                            "type": "boolean",
                            "description": "Replace every exact match instead of requiring a unique match (default false)"
                        }
                    },
                    "required": ["path", "old_content", "new_content"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "file_write".into(),
                description: "Create or overwrite/append/prepend a UTF-8 text file. New files need no prior read. Existing-file append needs a current file_read page that reaches the real EOF; overwrite and prepend require complete accumulated coverage. A complete text @file attachment is equivalent to complete coverage. On a read-permission error, follow required_read.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Relative, absolute, or ~/ file path"
                        },
                        "content": {
                            "type": "string",
                            "description": "Full content to write"
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["overwrite", "append", "prepend"]
                        }
                    },
                    "required": ["path", "content"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "web_search".into(),
                description: format!("Search the web through Zhipu and return structured search results. No browser automation. {web_time_guidance}"),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query"
                        },
                        "count": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self.limits.web_search_max_count,
                            "description": "Maximum number of results to return"
                        },
                        "search_recency_filter": {
                            "type": "string",
                            "enum": ["oneDay", "oneWeek", "oneMonth", "oneYear", "noLimit"],
                            "description": "Time filter for search results"
                        },
                        "search_domain_filter": {
                            "type": "string",
                            "description": "Optional whitelist domain filter"
                        },
                        "content_size": {
                            "type": "string",
                            "enum": ["medium", "high"],
                            "description": "How much snippet text to return"
                        }
                    },
                    "required": ["query"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "web_fetch".into(),
                description: format!("Fetch a known HTTP/HTTPS URL and return status plus truncated body text. For APIs or API-like services with clear documentation, prefer code_run so you can request, filter, aggregate, and summarize large responses before returning them to the conversation. {web_time_guidance}"),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "url": { "type": "string" },
                        "headers": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": { "type": "string" },
                                    "value": { "type": "string" }
                                },
                                "required": ["name", "value"],
                                "additionalProperties": false
                            }
                        }
                    },
                    "required": ["url"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "web_request".into(),
                description: format!("Send an HTTP request to a known URL and return status plus truncated response body text. For APIs or API-like services with clear documentation, prefer code_run so you can request, filter, aggregate, and summarize large responses before returning them to the conversation. {web_time_guidance}"),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "method": {
                            "type": "string",
                            "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"]
                        },
                        "url": { "type": "string" },
                        "headers": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": { "type": "string" },
                                    "value": { "type": "string" }
                                },
                                "required": ["name", "value"],
                                "additionalProperties": false
                            }
                        },
                        "query": {
                            "type": "object",
                            "additionalProperties": { "type": "string" }
                        },
                        "body": {}
                    },
                    "required": ["method", "url"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "working_note".into(),
                description: "Maintain session-local notes. This does not write long-term memory or claims.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["add", "list", "clear"]
                        },
                        "note": { "type": "string" }
                    },
                    "required": ["action"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "ask_user".into(),
                description: "Request user input when the task is blocked by ambiguity. The CLI returns a structured needs_user_input result.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "question": { "type": "string" },
                        "choices": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    },
                    "required": ["question"],
                    "additionalProperties": false
                }),
            },
        ];
        definitions.retain(|definition| match definition.name.as_str() {
            "code_run" | "write_stdin" | "process_list" => self.access.local_tools,
            "file_read" | "file_patch" | "file_write" => self.access.local_tools,
            "web_search" | "web_fetch" => self.access.web_tools,
            "web_request" => self.access.web_tools,
            "working_note" => self.access.working_note,
            "ask_user" => self.access.ask_user,
            _ => true,
        });
        if self.access.memory && self.memory_store.is_some() {
            definitions.extend(memory::definitions());
        }
        if self.access.router && self.router_client.is_some() {
            definitions.push(ToolDefinition {
                name: "consult_router".into(),
                description: "Consult the team router. Use mode='overview' to list available team claim scopes before choosing a query boundary. Use mode='query' with a non-empty scope to retrieve candidate claims and related disputes. Overview is only a map of available scopes; query returns concrete claim content.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "mode": {
                            "type": "string",
                            "enum": ["overview", "query"],
                            "description": "overview returns router scope overview and must not include scope or semantic_query; query returns candidate claims and requires non-empty scope"
                        },
                        "scope": {
                            "type": "string",
                            "description": "Required when mode='query'. Topic, scope, or knowledge boundary to query"
                        },
                        "semantic_query": {
                            "type": "string",
                            "description": "Optional when mode='query'. Current user task/question to help router ranking"
                        }
                    },
                    "required": ["mode"],
                    "additionalProperties": false
                }),
            });
        }
        if self.access.session_search && self.session_search.is_some() {
            definitions.push(ToolDefinition {
                name: "session_search".into(),
                description: "Search, browse, read, or scroll this agent's other persisted session transcripts. Choose exactly one argument shape: (1) browse recent sessions: omit query, session_id, around_message_index, and window; pass only optional limit/sort/include_tool_results. (2) discovery: pass a non-empty query, optional limit/sort/include_tool_results, and omit session_id/around_message_index/window. (3) read: pass a non-empty session_id only, plus optional include_tool_results. (4) scroll: pass a non-empty session_id and around_message_index, optional window/include_tool_results, and omit query. Never pass empty strings such as query='' or session_id=''; omit the field instead. Never pass placeholder or dummy fields such as '_' or '_=true'; omit unused fields entirely. Discovery returns original evidence windows, snippets, and session bookends; no summary is generated. CJK queries are supported. Tool results are omitted by default because they are noisy historical snapshots; set include_tool_results=true when debugging tool output.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Non-empty discovery query. Supports phrases, OR, NOT, prefix queries such as deploy*, and CJK substring search. Omit entirely to browse recent sessions; do not pass an empty string."
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self.limits.session_search_max_limit.max(1),
                            "description": "Maximum number of sessions to return for discovery or browse"
                        },
                        "sort": {
                            "type": "string",
                            "enum": ["relevance", "newest", "oldest"],
                            "description": "Discovery ordering. Use relevance for topic recall, newest for where work left off, oldest for origin/history questions."
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Non-empty session id returned by discovery/browse. Required for read and scroll; omit entirely for browse/discovery; do not pass an empty string."
                        },
                        "around_message_index": {
                            "type": "integer",
                            "description": "Scroll shape only. Message index to center the window on; requires a non-empty session_id. Omit for browse, discovery, and read."
                        },
                        "window": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 20,
                            "description": "Scroll shape only. Number of messages to return on each side of around_message_index. Default 5. Omit unless using around_message_index."
                        },
                        "include_tool_results": {
                            "type": "boolean",
                            "description": "Include full tool_result blocks in search and returned windows. Defaults to false; omitted tool results are shown as lightweight markers."
                        }
                    },
                    "required": [],
                    "additionalProperties": false
                }),
            });
        }
        if self.access.delegation {
            if let Some(host) = &self.delegation_host {
                definitions.extend(delegation_tool_definitions(host.config));
            }
        }
        if self.access.delegation_progress && self.delegation_progress.is_some() {
            definitions.push(update_subagent_progress_definition());
        }
        let mut mcp_routes = BTreeMap::new();
        if self.access.mcp {
            if let Some(mcp_manager) = &self.mcp_manager {
                let catalog = crate::mcp::tool::tool_catalog(&mcp_manager.snapshot_sync());
                mcp_routes = catalog.routes().clone();
                definitions.extend(catalog.definitions().iter().map(|tool| ToolDefinition {
                    name: tool.visible_name.clone(),
                    description: tool.description.clone(),
                    input_schema: tool.input_schema.clone(),
                }));
            }
        }
        (definitions, mcp_routes)
    }

    pub(super) fn code_run_process_scope_description(&self) -> &'static str {
        if self.access.delegation_child {
            "Use write_stdin and process_list only with processes you own; you cannot inspect or control your parent or sibling subagents' processes."
        } else {
            "Use process_list to inspect all live managed processes in this root session, including direct subagent processes. write_stdin can poll, send Ctrl-C to, or hard-terminate any listed process; a subagent-owned process does not accept text or other control sequences from you."
        }
    }

    pub(super) fn write_stdin_description(&self) -> String {
        let common = format!(
            "For pipe-backed commands, only an empty poll, Ctrl-C (\\u0003), or terminate=true is supported; interactive text requires code_run with tty=true. Ctrl-C is a soft foreground interrupt and does not close an interactive shell or SSH session. terminate=true hard-terminates the managed process group and cannot be combined with non-empty chars; a completed termination returns ok=true with outcome.kind=process_terminated even though output.success=false records that the child did not exit naturally with status 0. state describes the outer managed process, so a running shell/SSH session does not mean its latest command is still running. Usually omit max_output_chars; its default is {} characters per stream. Truncated output is delivered in provider-acknowledged pages, and the returned stdout_cursor/stderr_cursor can also be supplied together for explicit incremental reads. Issue at most one write_stdin call per process in one assistant tool-use response; the next page is available after the provider acknowledges the current result. Any later write_stdin call for the same process in that response, or while a page awaits provider acknowledgement, is rejected before writing text, sending Ctrl-C, or hard-terminating the process.",
            self.limits.code_run_max_output_chars,
        );
        if self.access.delegation_child {
            format!(
                "Write to or poll only your own live code_run processes. You cannot inspect or control parent or sibling subagent processes. Empty chars polls. {common}"
            )
        } else {
            format!(
                "Write to, poll, interrupt, or hard-terminate your own live code_run processes. You may also observe, interrupt, or hard-terminate any live subagent-owned process in this root session: for a subagent-owned process, only empty chars (read-only poll), exactly Ctrl-C (\\u0003), or terminate=true is accepted; text and other control sequences are rejected. Cross-owner reads never advance that subagent's output-delivery state; omit cursors for a retained snapshot, or reuse the returned cursor pair for an incremental read. {common}"
            )
        }
    }

    pub(super) fn process_list_description(&self) -> String {
        if self.access.delegation_child {
            "List only your currently live code_run processes. You cannot see your parent/main agent, sibling subagents, other agents, or other sessions. This does not expose OS PIDs and does not consume output.".into()
        } else {
            "List all currently live code_run processes in this root session, including processes owned by direct subagents. Each result includes owner (`main` or the subagent id). It excludes other agents and other sessions, does not expose OS PIDs, and does not consume output.".into()
        }
    }

    /// 判断一次已完整解析的 tool input 能否加入并发批次。
    ///
    /// 这是调度资格，不改变 `dispatch_with_context` 原有的参数校验或执行语义。任何无法按当前
    /// 工具契约确认的输入都保守地回退为串行。
    pub(crate) fn is_concurrency_safe(&self, name: &str, input: &Value) -> bool {
        if !input.is_object() {
            return false;
        }

        match name {
            "file_read" if self.access.local_tools => {
                serde_json::from_value::<FileReadArgs>(input.clone()).is_ok()
            }
            "code_run" if self.access.local_tools => {
                let Ok(args) = serde_json::from_value::<CodeRunArgs>(input.clone()) else {
                    return false;
                };
                !args.script.trim().is_empty()
                    && matches!(args.r#type.as_deref(), None | Some("bash"))
                    && !args.tty
                    && concurrency::bash_script_is_concurrency_safe(&args.script)
            }
            "web_search" if self.access.web_tools => {
                serde_json::from_value::<WebSearchArgs>(input.clone()).is_ok()
            }
            "web_fetch" if self.access.web_tools => {
                serde_json::from_value::<WebLookupArgs>(input.clone()).is_ok()
            }
            "working_note" if self.access.working_note => {
                matches!(
                    serde_json::from_value::<WorkingNoteArgs>(input.clone()),
                    Ok(WorkingNoteArgs { action, .. }) if action == "list"
                )
            }
            "ask_user" if self.access.ask_user => {
                serde_json::from_value::<AskUserArgs>(input.clone()).is_ok()
            }
            "consult_router" if self.access.router && self.router_client.is_some() => {
                serde_json::from_value::<ConsultRouterArgs>(input.clone()).is_ok()
            }
            "list_subagents" if self.access.delegation && self.delegation_host.is_some() => {
                serde_json::from_value::<ListDelegationsArgs>(input.clone()).is_ok()
            }
            "read_subagent" if self.access.delegation && self.delegation_host.is_some() => {
                serde_json::from_value::<ReadDelegationArgs>(input.clone()).is_ok()
            }
            name if self.access.mcp && is_mcp_visible_tool_name(name) => {
                self.is_read_only_mcp_tool(name)
            }
            _ => false,
        }
    }

    /// 返回当前 agent 每个连续安全批次的最大活跃工具数。
    pub(crate) fn max_parallel_tool_calls(&self) -> usize {
        self.limits.max_parallel_tool_calls.max(1)
    }

    pub async fn dispatch(&self, name: &str, input: Value) -> Result<ToolExecution, ToolError> {
        self.dispatch_with_context(name, input, ToolDispatchContext::default())
            .await
    }

    pub async fn dispatch_with_context(
        &self,
        name: &str,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        self.dispatch_with_context_inner(name, input, context, false)
            .await
    }

    /// 对安全批次中的调用在实际派发边界再次执行 fail-closed 分类。
    pub(crate) async fn dispatch_concurrency_safe_with_context(
        &self,
        name: &str,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        if !self.is_concurrency_safe(name, &input) {
            return Err(ToolError::InvalidArgs(format!(
                "工具 {name} 的并发资格在实际派发前已失效"
            )));
        }
        self.dispatch_with_context_inner(name, input, context, true)
            .await
    }

    pub(super) async fn dispatch_with_context_inner(
        &self,
        name: &str,
        input: Value,
        context: ToolDispatchContext,
        require_mcp_read_only: bool,
    ) -> Result<ToolExecution, ToolError> {
        let write_key = self.file_write_group_key(name, &input).await;
        let failed_file_write_paths = context.failed_file_write_paths.clone();
        if let (Some(failed), Some(key)) = (&failed_file_write_paths, &write_key) {
            if failed.lock().await.contains(key) {
                return Ok(ToolExecution::business_failure(json!({
                    "path": input.get("path").and_then(Value::as_str),
                    "status": "skipped",
                    "msg": "同一 assistant 响应中此前对该文件的写入已失败；为避免基于未知中间状态继续修改，本次调用未执行。",
                })));
            }
        }
        let result = match name {
            "code_run" if self.access.local_tools => self.code_run(input, &context).await,
            "write_stdin" if self.access.local_tools => self.write_stdin(input, &context).await,
            "process_list" if self.access.local_tools => self.process_list(input, &context).await,
            "file_read" if self.access.local_tools => self.file_read(input, &context).await,
            "file_patch" if self.access.local_tools => self.file_patch(input, &context).await,
            "file_write" if self.access.local_tools => self.file_write(input, &context).await,
            "web_search" if self.access.web_tools => self.web_search(input).await,
            "web_fetch" if self.access.web_tools => self.web_fetch(input).await,
            "web_request" if self.access.web_tools => self.web_request(input).await,
            "working_note" if self.access.working_note => {
                self.working_note(input).await.map(ToolExecution::completed)
            }
            "ask_user" if self.access.ask_user => {
                self.ask_user(input).await.map(ToolExecution::completed)
            }
            "memory" if self.access.memory => {
                memory::dispatch(self.memory_store.as_ref(), name, input).await
            }
            "consult_router" if self.access.router => self.consult_router(input).await,
            "session_search" if self.access.session_search => {
                self.session_search(input, context).await
            }
            "create_subagent" if self.access.delegation => self
                .create_subagent(input, context)
                .await
                .map(ToolExecution::completed),
            "list_subagents" if self.access.delegation => self
                .list_subagents(input, context)
                .await
                .map(ToolExecution::completed),
            "wait_subagents" if self.access.delegation => self
                .wait_subagents(input, context)
                .await
                .map(ToolExecution::completed),
            "read_subagent" if self.access.delegation => self
                .read_subagent(input, context)
                .await
                .map(ToolExecution::completed),
            "steer_subagent" if self.access.delegation => self
                .steer_subagent(input, context)
                .await
                .map(ToolExecution::completed),
            "update_subagent_progress" if self.access.delegation_progress => self
                .update_subagent_progress(input)
                .await
                .map(ToolExecution::completed),
            other if self.access.mcp && is_mcp_visible_tool_name(other) => {
                self.mcp_tool(other, input, context, require_mcp_read_only)
                    .await
            }
            other => Err(ToolError::UnknownTool(other.to_owned())),
        };
        if let (Some(failed), Some(key)) = (&failed_file_write_paths, write_key) {
            let failed_write = match &result {
                Ok(execution) => execution.outcome == ToolExecutionOutcome::BusinessFailure,
                Err(_) => true,
            };
            if failed_write {
                failed.lock().await.insert(key);
            }
        }
        result
    }
}
