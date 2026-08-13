//! Tool 模块：agent 可调用工具的 runtime 与统一注册入口。
//!
//! 核心本地工具包括 `file_read / file_write / file_patch / code_run`。
//! `workspace_root` 仅作为相对路径与执行 cwd 的默认基准，并非 sandbox 边界。
//! Web / note / ask_user 仍保留为独立意图工具。

mod command;
mod concurrency;
mod delegation;
pub mod diff;
mod file;
mod file_text;
mod mcp;
pub mod memory;
mod process;
pub mod read_state;
mod registry;
mod session;
mod web;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use chrono::{Datelike, Local};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::agent::MemoryStore;
use crate::api::ToolExecutionOutcome;
use crate::attachment::{AttachmentError, AttachmentKind, AttachmentLimits, FILE_READ_MEDIA_KEY};
use crate::claim::{AgentId, SessionId};
use crate::config::{
    default_id_mint_max_attempts, ToolConfig, DEFAULT_CODE_RUN_MAX_YIELD_MS,
    MAX_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER, MAX_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES,
    MAX_BACKGROUND_PROCESS_PTY_DIMENSION, MAX_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES,
    MAX_CODE_RUN_MAX_OUTPUT_CHARS, MAX_WRITE_STDIN_MAX_POLL_TIMEOUT_MS, WEB_SEARCH_ENGINE,
    WEB_SEARCH_USER_ID,
};
use crate::delegation::{
    read_mode_from_json, DelegationArtifactRef, DelegationCreateRequest, DelegationExecutor,
    DelegationId, DelegationMetadata, DelegationProgressSink, DelegationRunner,
    DelegationRunnerConfig, DelegationStore, DelegationStoreError, DelegationWaitConfig,
};
use crate::mcp::client::McpProgressEvent;
use crate::mcp::connection_manager::{McpConnectionManager, McpToolProgressReporter};
use crate::mcp::name::is_mcp_visible_tool_name;
use crate::mcp::redact::redact_mcp_sensitive_text;
use crate::mcp::tool::McpToolRoute;
use crate::router::http_client::RouterClientError;
use crate::router::{AgentQuery, RouterClient};
use crate::session::{SessionMetadata, SessionPaths, SessionStatus};
use crate::session_search::{SessionSearchRequest, SessionSearchService, SessionSearchSort};
use crate::storage::{paths, FileLockGuard};
use delegation::{
    delegation_tool_definitions, update_subagent_progress_definition, WaitSubagentsUntil,
};
use diff::{attach_file_change, compute_file_change, FileChange, FileChangeKind};
pub(crate) use process::{
    configure_process_group, reap_direct_child_blocking, spawn_direct_child_reaper,
    terminate_process_group, wait_for_child_exit_without_reap, BackgroundProcessEvent,
    ProcessCompletionDeliveryReceipt, ProcessDeliveryReceipt, ProcessOwner,
};
use process::{
    spawn_pty, ManagedProcess, OutputCursor, ProcessCompletion, ProcessManager, ProcessState,
    PtyInput, PtySpawned, PtyWatcherParts, TerminateRequestResult,
};
use read_state::{
    ContentRevision, LineRange, ReadAuthority, ReadEvidence, ReadStateScope, ReadStateStore,
    ReadStateVerdict,
};

const DEFAULT_LIST_DELEGATIONS_LIMIT: usize = 64;
const MAX_MCP_DISPATCH_ERROR_CHARS: usize = 16_000;
const DEFAULT_FILE_READ_LINES: usize = 2_000;

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("非法 url: {0}")]
    InvalidUrl(String),
    #[error("工具不存在: {0}")]
    UnknownTool(String),
    #[error("工具参数非法: {0}")]
    InvalidArgs(String),
    #[error("缺少 {env}，无法执行 web_search")]
    MissingWebSearchApiKey { env: String },
    #[error("命令超时: {0}s")]
    CommandTimeout(u64),
    #[error("memory: {0}")]
    Memory(String),
    #[error("router: {0}")]
    Router(String),
    #[error("attachment: {0}")]
    Attachment(#[from] AttachmentError),
    #[error("subagent: {0}")]
    Delegation(String),
    #[error("工具调用已中断")]
    Interrupted,
    #[error("进程 {process_id} 会继续在后台运行")]
    ProcessContinuesInBackground { process_id: String },
    #[error("mcp: {0}")]
    Mcp(String),
}

/// ToolRegistry dispatch 的类型化返回值。
#[derive(Debug, Clone, PartialEq)]
pub struct ToolExecution {
    pub output: Value,
    pub outcome: ToolExecutionOutcome,
    pub(crate) process_delivery_receipt: Option<ProcessDeliveryReceipt>,
}

impl ToolExecution {
    pub(crate) fn new(output: Value, outcome: ToolExecutionOutcome) -> Self {
        Self {
            output,
            outcome,
            process_delivery_receipt: None,
        }
    }

    pub(crate) fn completed(output: Value) -> Self {
        Self::new(output, ToolExecutionOutcome::Completed)
    }

    pub(crate) fn business_failure(output: Value) -> Self {
        Self::new(output, ToolExecutionOutcome::BusinessFailure)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl From<ToolDefinition> for crate::api::ToolSpec {
    fn from(def: ToolDefinition) -> Self {
        Self {
            name: def.name,
            description: def.description,
            input_schema: def.input_schema,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolLimits {
    file_read_max_chars: usize,
    file_diff_max_changed_lines: usize,
    max_parallel_tool_calls: usize,
    code_run_initial_yield_ms: u64,
    code_run_min_yield_ms: u64,
    code_run_max_yield_ms: u64,
    code_run_write_yield_ms: u64,
    code_run_poll_yield_ms: u64,
    write_stdin_max_poll_timeout_ms: u64,
    code_run_max_output_chars: usize,
    background_process_output_buffer_bytes: usize,
    background_process_max_entries_per_owner: usize,
    background_process_protected_recent_entries: usize,
    background_process_pty_rows: u16,
    background_process_pty_cols: u16,
    background_process_pty_input_buffer_bytes: usize,
    background_process_output_drain_grace_ms: u64,
    web_lookup_max_chars: usize,
    web_search_max_count: usize,
    web_search_max_content_chars: usize,
    web_search_max_total_chars: usize,
    session_search_max_limit: usize,
}

impl From<&ToolConfig> for ToolLimits {
    fn from(cfg: &ToolConfig) -> Self {
        Self {
            file_read_max_chars: cfg.file_read_max_chars,
            file_diff_max_changed_lines: cfg.file_diff_max_changed_lines,
            max_parallel_tool_calls: cfg.max_parallel_tool_calls,
            code_run_initial_yield_ms: cfg.code_run_initial_yield_ms,
            code_run_min_yield_ms: cfg.code_run_min_yield_ms,
            code_run_max_yield_ms: cfg.code_run_max_yield_ms,
            code_run_write_yield_ms: cfg.code_run_write_yield_ms,
            code_run_poll_yield_ms: cfg.code_run_poll_yield_ms,
            write_stdin_max_poll_timeout_ms: cfg.write_stdin_max_poll_timeout_ms,
            code_run_max_output_chars: cfg.code_run_max_output_chars,
            background_process_output_buffer_bytes: cfg.background_process_output_buffer_bytes,
            background_process_max_entries_per_owner: cfg.background_process_max_entries_per_owner,
            background_process_protected_recent_entries: cfg
                .background_process_protected_recent_entries,
            background_process_pty_rows: cfg.background_process_pty_rows,
            background_process_pty_cols: cfg.background_process_pty_cols,
            background_process_pty_input_buffer_bytes: cfg
                .background_process_pty_input_buffer_bytes,
            background_process_output_drain_grace_ms: cfg.background_process_output_drain_grace_ms,
            web_lookup_max_chars: cfg.web.lookup_max_chars,
            web_search_max_count: cfg.web.max_count,
            web_search_max_content_chars: cfg.web.max_content_chars,
            web_search_max_total_chars: cfg.web.max_total_chars,
            session_search_max_limit: cfg.session_search_max_limit,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ToolDispatchContext {
    pub current_session_id: Option<SessionId>,
    pub current_turn_id: Option<String>,
    pub tool_use_id: Option<String>,
    pub progress_tx: Option<mpsc::UnboundedSender<ToolProgressUpdate>>,
    /// 仅用于支持当前 turn 可中断的长时间工具调用。
    pub cancellation: Option<CancellationToken>,
    /// 本次 Provider sampling 实际看到的 MCP 路由。`Some` 表示模型调用必须严格
    /// 受该快照约束；即使当前 catalog 已出现同名 replacement，也不能改投新 generation。
    pub(crate) provider_mcp_routes: Option<Arc<BTreeMap<String, McpToolRoute>>>,
    /// 同一 assistant 响应内，阻止在同路径前序写失败后继续假定中间状态。
    pub(crate) failed_file_write_paths: Option<Arc<Mutex<BTreeSet<PathBuf>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolProgressUpdate {
    pub id: String,
    pub summary: String,
}

/// 给 Session/TUI 的聚合进程快照。它不暴露 OS PID，也不包含 output。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessSnapshot {
    pub(crate) process_id: String,
    /// 仅供 runtime/TUI 将确认操作绑定到本次 allocation，避免旧 logical ID 被回收后误杀重用项。
    pub(crate) instance_id: u64,
    pub(crate) root_session_id: String,
    pub(crate) subagent_id: Option<String>,
    pub(crate) status: String,
    pub(crate) tty: bool,
    pub(crate) command: String,
    pub(crate) code_type: String,
    pub(crate) cwd: String,
    pub(crate) started_at: std::time::SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToolAccessProfile {
    local_tools: bool,
    web_tools: bool,
    working_note: bool,
    ask_user: bool,
    memory: bool,
    router: bool,
    session_search: bool,
    mcp: bool,
    delegation: bool,
    delegation_progress: bool,
    delegation_child: bool,
}

impl ToolAccessProfile {
    fn parent() -> Self {
        Self {
            local_tools: true,
            web_tools: true,
            working_note: true,
            ask_user: true,
            memory: true,
            router: true,
            session_search: true,
            mcp: true,
            delegation: true,
            delegation_progress: false,
            delegation_child: false,
        }
    }

    fn delegation() -> Self {
        Self {
            local_tools: true,
            web_tools: true,
            working_note: false,
            ask_user: false,
            memory: false,
            router: false,
            session_search: false,
            mcp: true,
            delegation: false,
            delegation_progress: true,
            delegation_child: true,
        }
    }

    fn memory_review() -> Self {
        Self {
            local_tools: false,
            web_tools: false,
            working_note: false,
            ask_user: false,
            memory: true,
            router: false,
            session_search: false,
            mcp: false,
            delegation: false,
            delegation_progress: false,
            delegation_child: false,
        }
    }
}

#[derive(Clone)]
struct DelegationToolHost {
    agent_home: PathBuf,
    owner_agent_id: AgentId,
    executor: Arc<dyn DelegationExecutor>,
    config: DelegationRunnerConfig,
    runners: Arc<StdMutex<BTreeMap<SessionId, DelegationRunner>>>,
    #[cfg(test)]
    wait_subagents_snapshot_resolved: Option<Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    wait_subagents_blocking: Option<Arc<tokio::sync::Notify>>,
}

impl DelegationToolHost {
    fn new(
        agent_home: PathBuf,
        owner_agent_id: AgentId,
        executor: Arc<dyn DelegationExecutor>,
        config: DelegationRunnerConfig,
    ) -> Self {
        Self {
            agent_home,
            owner_agent_id,
            executor,
            config,
            runners: Arc::new(StdMutex::new(BTreeMap::new())),
            #[cfg(test)]
            wait_subagents_snapshot_resolved: None,
            #[cfg(test)]
            wait_subagents_blocking: None,
        }
    }

    fn runner_for(&self, session_id: &SessionId) -> Result<DelegationRunner, ToolError> {
        let mut runners = self
            .runners
            .lock()
            .map_err(|_| ToolError::Delegation("subagent runner registry lock poisoned".into()))?;
        if let Some(runner) = runners.get(session_id) {
            return Ok(runner.clone());
        }
        let session_dir = paths::agent_home_session_dir(&self.agent_home, session_id);
        let runner = DelegationRunner::new(
            DelegationStore::new_for_session(session_dir, session_id.clone()),
            self.executor.clone(),
            self.config,
        )
        .map_err(|err| ToolError::Delegation(err.to_string()))?;
        runners.insert(session_id.clone(), runner.clone());
        Ok(runner)
    }
}

#[derive(Clone)]
pub struct ToolRegistry {
    workspace_root: PathBuf,
    http: reqwest::Client,
    direct_http: reqwest::Client,
    notes: Arc<Mutex<Vec<String>>>,
    web_search_endpoint: String,
    web_search_api_key_env: String,
    web_search_api_key: Option<String>,
    memory_store: Option<Arc<dyn MemoryStore>>,
    router_client: Option<Arc<dyn RouterClient>>,
    session_search: Option<Arc<SessionSearchService>>,
    mcp_manager: Option<Arc<McpConnectionManager>>,
    access: ToolAccessProfile,
    delegation_host: Option<Arc<DelegationToolHost>>,
    delegation_progress: Option<DelegationProgressSink>,
    path_locks: Arc<StdMutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>>,
    /// 仅协调共享同一 base ACN home 的 ACN 进程；不阻止普通外部编辑器写入。
    file_write_lock_root: Option<PathBuf>,
    read_state: Arc<ReadStateStore>,
    process_manager: Arc<ProcessManager>,
    /// 一个 registry 及其 delegation clone 都归属于同一个 ACN agent；把这个身份写进
    /// ProcessOwner，避免只靠 root session / subagent id 形成不完整的所有权键。
    process_owner_agent_id: String,
    limits: ToolLimits,
    attachment_limits: AttachmentLimits,
}

fn current_year_web_guidance() -> String {
    // Web 查询的“今年”应跟随用户本地日历年，避免跨年附近误用旧年份。
    let year = Local::now().year();
    format!(
        "Current year is {year}. For latest/current/recent web tasks, include the current year when useful and verify source dates."
    )
}

#[derive(Debug, Deserialize)]
struct FileReadArgs {
    path: String,
    start: Option<usize>,
    count: Option<usize>,
    keyword: Option<String>,
    show_linenos: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct FilePatchArgs {
    path: String,
    old_content: String,
    new_content: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Debug, Deserialize)]
struct FileWriteArgs {
    path: String,
    content: String,
    mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CodeRunArgs {
    script: String,
    r#type: Option<String>,
    cwd: Option<String>,
    #[serde(default)]
    tty: bool,
    yield_time_ms: Option<u64>,
    max_output_chars: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteStdinArgs {
    process_id: String,
    chars: Option<String>,
    #[serde(default)]
    terminate: bool,
    yield_time_ms: Option<u64>,
    max_output_chars: Option<usize>,
    stdout_cursor: Option<u64>,
    stderr_cursor: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessListArgs {}

#[derive(Debug, Deserialize)]
struct WebSearchArgs {
    query: String,
    count: Option<usize>,
    search_recency_filter: Option<String>,
    search_domain_filter: Option<String>,
    content_size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WebLookupHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct WebLookupArgs {
    url: String,
    headers: Option<Vec<WebLookupHeader>>,
}

#[derive(Debug, Deserialize)]
struct WebRequestArgs {
    method: String,
    url: String,
    headers: Option<Vec<WebLookupHeader>>,
    query: Option<std::collections::HashMap<String, String>>,
    body: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct WorkingNoteArgs {
    action: String,
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AskUserArgs {
    question: String,
    choices: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDelegationArgs {
    title: String,
    role: String,
    objective: String,
    constraints: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListDelegationsArgs {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitSubagentsArgs {
    subagent_ids: Option<Vec<String>>,
    until: Option<WaitSubagentsUntil>,
    timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadDelegationArgs {
    id: String,
    #[serde(rename = "mode")]
    _mode: Option<String>,
    #[serde(rename = "limit")]
    _limit: Option<usize>,
    #[serde(rename = "max_chars")]
    _max_chars: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SteerDelegationArgs {
    id: String,
    instruction: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateDelegationProgressArgs {
    current_step: Option<String>,
    summary: String,
    artifacts: Option<Vec<DelegationArtifactRef>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsultRouterArgs {
    mode: ConsultRouterMode,
    scope: Option<String>,
    semantic_query: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConsultRouterMode {
    Overview,
    Query,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionSearchArgs {
    query: Option<String>,
    limit: Option<usize>,
    sort: Option<String>,
    session_id: Option<String>,
    around_message_index: Option<usize>,
    window: Option<usize>,
    include_tool_results: Option<bool>,
}

fn resolve_tool_path(root: &Path, raw: &str) -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    resolve_tool_path_with_home(root, raw, home.as_deref())
}

fn resolve_tool_path_with_home(root: &Path, raw: &str, home: Option<&Path>) -> PathBuf {
    let requested = crate::path_util::expand_current_user_home_with(Path::new(raw), home);
    if requested.is_absolute() {
        requested
    } else {
        root.join(requested)
    }
}

fn bounded_text_byte_limit(max_chars: usize) -> usize {
    max_chars.saturating_mul(4).saturating_add(16).max(16)
}

fn truncate_chars(raw: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (idx, ch) in raw.chars().enumerate() {
        if idx >= max_chars {
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

#[cfg(test)]
mod tests;
