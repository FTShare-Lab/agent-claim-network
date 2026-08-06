//! 配置加载。
//!
//! 默认从 `<acn_home>/config.toml` 加载；LLM endpoint / model 以配置文件为准，
//! 密钥只通过 `[agent.llm].api_key_env` 指定的环境变量读取，避免把密钥写进配置文件。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::claim::AgentId;
use crate::path_util::expand_current_user_home;

/// Maintainer ID 落盘或写 outbox 前查重失败时的默认最大重抽次数。
/// 这是配置缺省值的唯一真理来源：`config.toml` 缺字段时由 serde default 兜底，
/// session id 与 maintainer id mint 默认共用该值，生产路径走配置实例。
pub const DEFAULT_ID_MINT_MAX_RETRIES: u32 = 3;

/// id mint 默认总尝试次数。const fn 让测试 / 集成层也能 const-eval。
pub const fn default_id_mint_max_attempts() -> usize {
    retries_to_attempts(DEFAULT_ID_MINT_MAX_RETRIES)
}

/// 把"重抽次数"换算成"总尝试次数"（首次 + 重抽）。const fn，作为唯一换算入口。
/// u32 → usize 在 32/64-bit 目标上是 widening；16-bit 目标退化到 usize::MAX 也只让循环多跑几次。
pub const fn retries_to_attempts(retries: u32) -> usize {
    retries as usize + 1
}

fn default_id_mint_max_retries() -> u32 {
    DEFAULT_ID_MINT_MAX_RETRIES
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub upstream: String,
    #[serde(default)]
    pub upstreams: BTreeMap<String, UpstreamConfig>,
    pub storage: StorageConfig,
    #[serde(default)]
    pub router: RouterConfig,
    #[serde(default)]
    pub maintainer: MaintainerConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub clients: ClientsConfig,
    /// Prompt 模板仅供测试 / 内部构造覆盖，用户 TOML 一律使用内置模板。
    #[serde(skip)]
    pub prompt: PromptConfig,
    #[serde(default)]
    pub langfuse: LangfuseConfig,
}

pub const DEFAULT_LANGFUSE_SERVICE_NAME: &str = "agent_claim_network";
/// `LangfuseConfig::service_name` 的 serde 缺省值。
fn default_service_name() -> String {
    DEFAULT_LANGFUSE_SERVICE_NAME.to_string()
}

pub const DEFAULT_FILE_READ_MAX_CHARS: usize = 100_000;
pub const DEFAULT_FILE_DIFF_MAX_CHANGED_LINES: usize = 20;
pub const DEFAULT_MAX_PARALLEL_TOOL_CALLS: usize = 5;
pub const DEFAULT_CODE_RUN_INITIAL_YIELD_MS: u64 = 10_000;
pub const DEFAULT_CODE_RUN_MIN_YIELD_MS: u64 = 250;
pub const DEFAULT_CODE_RUN_MAX_YIELD_MS: u64 = 30_000;
pub const DEFAULT_CODE_RUN_WRITE_YIELD_MS: u64 = 250;
pub const DEFAULT_CODE_RUN_POLL_YIELD_MS: u64 = 5_000;
pub const DEFAULT_WRITE_STDIN_MAX_POLL_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_CODE_RUN_MAX_OUTPUT_CHARS: usize = 1_048_576;
pub const DEFAULT_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES: usize = 1_048_576;
pub const DEFAULT_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER: usize = 64;
pub const DEFAULT_BACKGROUND_PROCESS_PROTECTED_RECENT_ENTRIES: usize = 8;
pub const DEFAULT_BACKGROUND_PROCESS_PTY_ROWS: u16 = 24;
pub const DEFAULT_BACKGROUND_PROCESS_PTY_COLS: u16 = 80;
pub const DEFAULT_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES: usize = 65_536;
/// background-shell 的内部资源护栏；不开放 TOML 覆盖。
pub const MAX_CODE_RUN_MAX_YIELD_MS: u64 = 30_000;
pub const MAX_WRITE_STDIN_MAX_POLL_TIMEOUT_MS: u64 = 300_000;
pub const MAX_CODE_RUN_MAX_OUTPUT_CHARS: usize = 2 * 1024 * 1024;
pub const MAX_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER: usize = 64;
pub const MAX_BACKGROUND_PROCESS_PTY_DIMENSION: u16 = 1_000;
pub const MAX_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES: usize = 1024 * 1024;
pub const MAX_BACKGROUND_PROCESS_OUTPUT_DRAIN_GRACE_MS: u64 = 30_000;
/// root 已退出后的输出 drain 仅用于收束 reader，不限制进程正常运行时间。
pub const DEFAULT_BACKGROUND_PROCESS_OUTPUT_DRAIN_GRACE_MS: u64 = 500;
pub const DEFAULT_WEB_LOOKUP_MAX_CHARS: usize = 80_000;
pub const DEFAULT_WEB_SEARCH_MAX_COUNT: usize = 10;
pub const DEFAULT_WEB_SEARCH_MAX_CONTENT_CHARS: usize = 2_500;
pub const DEFAULT_WEB_SEARCH_MAX_TOTAL_CHARS: usize = 200_000;
pub const DEFAULT_SESSION_SEARCH_DEFAULT_LIMIT: usize = 3;
pub const DEFAULT_SESSION_SEARCH_MAX_LIMIT: usize = 5;
pub const DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS: u64 = 500;
/// 智谱 Web Search 请求使用的固定搜索引擎，不开放 TOML 覆盖。
pub(crate) const WEB_SEARCH_ENGINE: &str = "search_pro";
/// 默认智谱 Web Search endpoint 使用的内部终端用户标识，不开放 TOML 覆盖。
pub(crate) const WEB_SEARCH_USER_ID: &str = "agent_claim_network";
pub const DEFAULT_WEB_SEARCH_ENDPOINT: &str = "https://open.bigmodel.cn/api/paas/v4/web_search";
pub const DEFAULT_WEB_SEARCH_API_KEY_ENV: &str = "GLM_API_KEY";
pub const DEFAULT_MAINTAINER_LISTEN: &str = "127.0.0.1:8062";
pub const DEFAULT_ROUTER_LISTEN: &str = "127.0.0.1:8061";
pub const DEFAULT_MAINTAINER_ENDPOINT: &str = "http://127.0.0.1:8062";
pub const DEFAULT_ROUTER_ENDPOINT: &str = "http://127.0.0.1:8061";
pub const DEFAULT_ROUTER_QUERY_TIMEOUT_SECS: u64 = 50;
pub const DEFAULT_SUPERVISOR_IDLE_TIMEOUT_SECS: u64 = 5 * 60;
pub const DEFAULT_SUPERVISOR_STARTUP_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_SUPERVISOR_IPC_TIMEOUT_MS: u64 = 1_500;
pub const DEFAULT_SUPERVISOR_LOCK_TIMEOUT_MS: u64 = 300;
pub const DEFAULT_SUPERVISOR_STOP_WAIT_TIMEOUT_MS: u64 = 1_000;
pub const DEFAULT_SUPERVISOR_UPDATE_SHUTDOWN_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_SUPERVISOR_NOTIFICATION_TIMEOUT_MS: u64 = 5_000;
pub const SUPERVISOR_NOTIFICATION_ICON_FILE_NAME: &str = "acn-notification-icon.png";
pub const DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS: u32 = 5;
pub const AGENT_ID_PLACEHOLDER: &str = "<your_agent_id_here>";
const RESERVED_UPSTREAM_NAMES_RAW: &str = include_str!("../resources/upstream_reserved_names.txt");
pub const DEFAULT_MAINTAINER_SWEEP_TICK_INTERVAL_SECS: u64 = 86_400;
pub const DEFAULT_MAINTAINER_STALE_AFTER_DAYS: u32 = 30;
pub const DEFAULT_MAINTAINER_DEPRECATED_AFTER_DAYS: u32 = 90;
pub const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_LLM_TIMEOUT_SECS: u64 = 300;
pub const DEFAULT_HTTP_RETRY_COUNT: u32 = 1;
pub const DEFAULT_HTTP_RETRY_BASE_DELAY_MS: u64 = 200;
pub const DEFAULT_HTTP_RETRY_MAX_DELAY_MS: u64 = 5_000;
/// MCP 连接建立/显式重连失败后的额外尝试次数；不用于重放 tools/call。
pub const MCP_RECONNECT_MAX_RETRIES: u32 = 2;
/// MCP 重连指数退避的初始等待时间（毫秒）；只由运行时内部使用，不暴露到 TOML。
pub const MCP_RECONNECT_RETRY_BASE_DELAY_MS: u64 = 200;
/// MCP 重连指数退避的等待上限（毫秒）；只由运行时内部使用，不暴露到 TOML。
pub const MCP_RECONNECT_RETRY_MAX_DELAY_MS: u64 = 2_000;
/// MCP 生命周期切换时等待底层 transport/child 收束的上限；不暴露到 TOML。
pub const MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS: u64 = 3;
pub const DEFAULT_LLM_MAX_TOKENS: u32 = 65_536;
pub const DEFAULT_MEMORY_CHAR_LIMIT: usize = 1600;
pub const DEFAULT_USER_CHAR_LIMIT: usize = 1000;
pub const DEFAULT_MEMORY_SAFETY_SCAN: bool = true;
pub const DEFAULT_MAINTAINER_HISTORY_MAX_FILE_BYTES: u64 = 5_000_000;
pub const DEFAULT_MAINTAINER_HISTORY_BACKUP_COUNT: usize = 3;
pub const DEFAULT_MAINTAINER_ADMIN_AUTH_ENABLED: bool = false;
pub const DEFAULT_MAINTAINER_ADMIN_AUTH_USERNAME: &str = "admin";
pub const DEFAULT_MAINTAINER_ADMIN_AUTH_PASSWORD_ENV: &str = "ACN_MAINTAINER_ADMIN_PASSWORD";
pub const DEFAULT_TEAM_AUTH_ENABLED: bool = false;
pub const DEFAULT_SESSION_COMPACTION_SUMMARY_MAX_CHARS: usize = 40_000;
/// 自动 compact 的完整投影仍超 hard tail 时，仅允许一次更紧 summary 重试。
/// 这是内部恢复策略，不开放 TOML 覆盖。
pub(crate) const COMPACTION_RETRY_SUMMARY_DIVISOR: usize = 2;
/// 单个 turn 的 compact 外置资产恢复引用上限；只限制 journal 元数据，不限制
/// provider 当前请求已经获得的引用。
pub(crate) const COMPACTION_ASSET_REFERENCES_PER_TURN_MAX: usize = 64;
pub const DEFAULT_LLM_CONTEXT_WINDOW: usize = 200_000;
pub const DEFAULT_AUTO_COMPACT_CTX_RATIO: f64 = 0.8;
pub const DEFAULT_SESSION_CLEANUP_RETENTION_DAYS: u32 = 30;
pub const MAX_SESSION_CLEANUP_RETENTION_DAYS: u32 = 36_500;
pub const DEFAULT_SESSION_DELEGATION_MAX_CONCURRENT: usize = 6;
pub const DEFAULT_SESSION_DELEGATION_MAX_TOOL_LOOP_TURNS: usize = 256;
pub const DEFAULT_SESSION_DELEGATION_WALL_TIMEOUT_SECS: u64 = 120 * 60;
pub const DEFAULT_SESSION_DELEGATION_WAIT_DEFAULT_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_SESSION_DELEGATION_WAIT_MIN_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_SESSION_DELEGATION_WAIT_MAX_TIMEOUT_SECS: u64 = 60 * 60;
pub const DEFAULT_FORK_MEMORY_REVIEW_INTERVAL_TURNS: usize = 10;
pub const DEFAULT_SESSION_NOTIFY_ON_FINALIZE_COMPLETION: bool = true;
pub const DEFAULT_TURN_JOURNAL_DELTA_SNAPSHOT_INTERVAL_MS: u64 = 500;
pub const DEFAULT_TURN_JOURNAL_DELTA_SNAPSHOT_CHARS: usize = 1024;
pub const DEFAULT_TURN_RECOVERY_ORIGINAL_USER_REQUEST_MAX_CHARS: usize = 8192;
pub const DEFAULT_TURN_RECOVERY_PARTIAL_ASSISTANT_MAX_CHARS: usize = 8192;
pub const DEFAULT_TURN_RECOVERY_TOOL_INPUT_MAX_CHARS: usize = 2048;
pub const DEFAULT_TURN_RECOVERY_TOOL_OUTPUT_MAX_CHARS: usize = 4096;
pub const DEFAULT_TURN_RECOVERY_USER_STEER_MAX_CHARS: usize = 8192;
pub const DEFAULT_INBOX_PROCESSING_STALE_AFTER_SECS: u64 = 30 * 60;
pub const DEFAULT_USER_SHELL_ENABLED: bool = true;
pub const DEFAULT_USER_SHELL_TIMEOUT_SECS: u64 = 180;
pub(crate) const USER_SHELL_DRAIN_GRACE_MS: u64 = 250;
pub(crate) const USER_SHELL_TERMINATION_GRACE_MS: u64 = 250;
pub const DEFAULT_USER_SHELL_MAX_OUTPUT_CHARS: usize = 100_000;
pub const DEFAULT_USER_SHELL_SHELL: &str = "auto";
pub const DEFAULT_USER_SHELL_LOGIN_SHELL: bool = true;
/// `-1` 表示 TUI 虚线框自动占满当前可用高度。
pub const DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES: i64 = -1;
/// `@path` 补全扫描和目录引用上下文共用的一级目录项上限。
pub const DEFAULT_TUI_AT_PATH_DIRECTORY_CONTEXT_MAX_ENTRIES: usize = 1_000;
/// `@path` 自动补全过滤、排序后保留的候选上限。
pub const DEFAULT_TUI_AT_PATH_MAX_CANDIDATES: usize = 50;
const AUTO_LIVE_RESPONSE_PREVIEW_MAX_LINES: i64 = -1;
const MIN_LIVE_RESPONSE_PREVIEW_MAX_LINES: i64 = 5;
pub const DEFAULT_ATTACHMENT_ENABLED: bool = true;
pub const DEFAULT_ATTACHMENT_CLIPBOARD_IMAGE_ENABLED: bool = true;
pub const DEFAULT_ATTACHMENT_MAX_FILE_BYTES: u64 = 5 * 1024 * 1024;
pub const DEFAULT_ATTACHMENT_MAX_FILES_PER_TURN: usize = 5;
pub const DEFAULT_SESSION_SKILL_MAX_BODY_BYTES: usize = 256 * 1024;
pub const DEFAULT_SESSION_SKILL_MAX_PER_TURN: usize = 8;

fn default_acn_home() -> PathBuf {
    PathBuf::from("~/.acn")
}

fn default_workspace_root() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn default_fork_memory_review_interval_turns() -> usize {
    DEFAULT_FORK_MEMORY_REVIEW_INTERVAL_TURNS
}

fn default_session_notify_on_finalize_completion() -> bool {
    DEFAULT_SESSION_NOTIFY_ON_FINALIZE_COMPLETION
}

fn default_session_cleanup_retention_days() -> u32 {
    DEFAULT_SESSION_CLEANUP_RETENTION_DAYS
}

fn default_session_delegation_max_concurrent() -> usize {
    DEFAULT_SESSION_DELEGATION_MAX_CONCURRENT
}

fn default_session_delegation_max_tool_loop_turns() -> usize {
    DEFAULT_SESSION_DELEGATION_MAX_TOOL_LOOP_TURNS
}

fn default_session_delegation_wall_timeout_secs() -> u64 {
    DEFAULT_SESSION_DELEGATION_WALL_TIMEOUT_SECS
}

fn default_session_delegation_wait_default_timeout_secs() -> u64 {
    DEFAULT_SESSION_DELEGATION_WAIT_DEFAULT_TIMEOUT_SECS
}

fn default_session_delegation_wait_min_timeout_secs() -> u64 {
    DEFAULT_SESSION_DELEGATION_WAIT_MIN_TIMEOUT_SECS
}

fn default_session_delegation_wait_max_timeout_secs() -> u64 {
    DEFAULT_SESSION_DELEGATION_WAIT_MAX_TIMEOUT_SECS
}

fn default_turn_journal_delta_snapshot_interval_ms() -> u64 {
    DEFAULT_TURN_JOURNAL_DELTA_SNAPSHOT_INTERVAL_MS
}

fn default_turn_journal_delta_snapshot_chars() -> usize {
    DEFAULT_TURN_JOURNAL_DELTA_SNAPSHOT_CHARS
}

fn default_turn_recovery_partial_assistant_max_chars() -> usize {
    DEFAULT_TURN_RECOVERY_PARTIAL_ASSISTANT_MAX_CHARS
}

fn default_turn_recovery_original_user_request_max_chars() -> usize {
    DEFAULT_TURN_RECOVERY_ORIGINAL_USER_REQUEST_MAX_CHARS
}

fn default_turn_recovery_tool_input_max_chars() -> usize {
    DEFAULT_TURN_RECOVERY_TOOL_INPUT_MAX_CHARS
}

fn default_turn_recovery_tool_output_max_chars() -> usize {
    DEFAULT_TURN_RECOVERY_TOOL_OUTPUT_MAX_CHARS
}

fn default_turn_recovery_user_steer_max_chars() -> usize {
    DEFAULT_TURN_RECOVERY_USER_STEER_MAX_CHARS
}

fn default_user_shell_enabled() -> bool {
    DEFAULT_USER_SHELL_ENABLED
}

fn default_user_shell_timeout_secs() -> u64 {
    DEFAULT_USER_SHELL_TIMEOUT_SECS
}

fn default_user_shell_max_output_chars() -> usize {
    DEFAULT_USER_SHELL_MAX_OUTPUT_CHARS
}

fn default_user_shell_shell() -> String {
    DEFAULT_USER_SHELL_SHELL.to_string()
}

fn default_user_shell_login_shell() -> bool {
    DEFAULT_USER_SHELL_LOGIN_SHELL
}

fn default_live_response_preview_max_lines() -> i64 {
    DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES
}

fn default_session_skill_max_body_bytes() -> usize {
    DEFAULT_SESSION_SKILL_MAX_BODY_BYTES
}

fn default_session_skill_max_per_turn() -> usize {
    DEFAULT_SESSION_SKILL_MAX_PER_TURN
}

fn default_maintainer_history_max_file_bytes() -> u64 {
    DEFAULT_MAINTAINER_HISTORY_MAX_FILE_BYTES
}

fn default_maintainer_history_backup_count() -> usize {
    DEFAULT_MAINTAINER_HISTORY_BACKUP_COUNT
}

fn default_maintainer_admin_auth_enabled() -> bool {
    DEFAULT_MAINTAINER_ADMIN_AUTH_ENABLED
}

fn default_maintainer_admin_auth_username() -> String {
    DEFAULT_MAINTAINER_ADMIN_AUTH_USERNAME.to_string()
}

fn default_maintainer_admin_auth_password_env() -> String {
    DEFAULT_MAINTAINER_ADMIN_AUTH_PASSWORD_ENV.to_string()
}

fn default_team_auth_enabled() -> bool {
    DEFAULT_TEAM_AUTH_ENABLED
}

fn default_session_compaction_summary_max_chars() -> usize {
    DEFAULT_SESSION_COMPACTION_SUMMARY_MAX_CHARS
}

fn default_llm_context_window() -> usize {
    DEFAULT_LLM_CONTEXT_WINDOW
}

fn default_llm_max_tokens() -> u32 {
    DEFAULT_LLM_MAX_TOKENS
}

fn default_auto_compact_ctx_ratio() -> f64 {
    DEFAULT_AUTO_COMPACT_CTX_RATIO
}

fn default_compaction_tail_target_ctx_ratio() -> f64 {
    0.20
}

fn default_compaction_tail_hard_ctx_ratio() -> f64 {
    0.30
}

fn default_compaction_tail_previous_real_user_turns() -> usize {
    4
}

fn default_compaction_tool_result_raw_max_chars() -> usize {
    4096
}

fn default_inbox_processing_stale_after_secs() -> u64 {
    DEFAULT_INBOX_PROCESSING_STALE_AFTER_SECS
}

fn default_attachment_enabled() -> bool {
    DEFAULT_ATTACHMENT_ENABLED
}

fn default_attachment_clipboard_image_enabled() -> bool {
    DEFAULT_ATTACHMENT_CLIPBOARD_IMAGE_ENABLED
}

fn default_attachment_max_file_bytes() -> u64 {
    DEFAULT_ATTACHMENT_MAX_FILE_BYTES
}

fn default_attachment_max_files_per_turn() -> usize {
    DEFAULT_ATTACHMENT_MAX_FILES_PER_TURN
}

fn default_memory_char_limit() -> usize {
    DEFAULT_MEMORY_CHAR_LIMIT
}

fn default_user_char_limit() -> usize {
    DEFAULT_USER_CHAR_LIMIT
}

fn default_memory_safety_scan() -> bool {
    DEFAULT_MEMORY_SAFETY_SCAN
}

fn default_file_read_max_chars() -> usize {
    DEFAULT_FILE_READ_MAX_CHARS
}

fn default_file_diff_max_changed_lines() -> usize {
    DEFAULT_FILE_DIFF_MAX_CHANGED_LINES
}

fn default_max_parallel_tool_calls() -> usize {
    DEFAULT_MAX_PARALLEL_TOOL_CALLS
}

fn default_code_run_initial_yield_ms() -> u64 {
    DEFAULT_CODE_RUN_INITIAL_YIELD_MS
}

fn default_code_run_min_yield_ms() -> u64 {
    DEFAULT_CODE_RUN_MIN_YIELD_MS
}

fn default_code_run_max_yield_ms() -> u64 {
    DEFAULT_CODE_RUN_MAX_YIELD_MS
}

fn default_code_run_write_yield_ms() -> u64 {
    DEFAULT_CODE_RUN_WRITE_YIELD_MS
}

fn default_code_run_poll_yield_ms() -> u64 {
    DEFAULT_CODE_RUN_POLL_YIELD_MS
}

fn default_write_stdin_max_poll_timeout_ms() -> u64 {
    DEFAULT_WRITE_STDIN_MAX_POLL_TIMEOUT_MS
}

fn default_code_run_max_output_chars() -> usize {
    DEFAULT_CODE_RUN_MAX_OUTPUT_CHARS
}

fn default_background_process_output_buffer_bytes() -> usize {
    DEFAULT_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES
}

fn default_background_process_max_entries_per_owner() -> usize {
    DEFAULT_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER
}

fn default_background_process_protected_recent_entries() -> usize {
    DEFAULT_BACKGROUND_PROCESS_PROTECTED_RECENT_ENTRIES
}

fn default_background_process_pty_rows() -> u16 {
    DEFAULT_BACKGROUND_PROCESS_PTY_ROWS
}

fn default_background_process_pty_cols() -> u16 {
    DEFAULT_BACKGROUND_PROCESS_PTY_COLS
}

fn default_background_process_pty_input_buffer_bytes() -> usize {
    DEFAULT_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES
}

fn default_background_process_output_drain_grace_ms() -> u64 {
    DEFAULT_BACKGROUND_PROCESS_OUTPUT_DRAIN_GRACE_MS
}

fn default_web_lookup_max_chars() -> usize {
    DEFAULT_WEB_LOOKUP_MAX_CHARS
}

fn default_web_search_max_count() -> usize {
    DEFAULT_WEB_SEARCH_MAX_COUNT
}

fn default_web_search_max_content_chars() -> usize {
    DEFAULT_WEB_SEARCH_MAX_CONTENT_CHARS
}

fn default_web_search_max_total_chars() -> usize {
    DEFAULT_WEB_SEARCH_MAX_TOTAL_CHARS
}

fn default_session_search_default_limit() -> usize {
    DEFAULT_SESSION_SEARCH_DEFAULT_LIMIT
}

fn default_session_search_max_limit() -> usize {
    DEFAULT_SESSION_SEARCH_MAX_LIMIT
}

fn default_session_search_sqlite_busy_timeout_ms() -> u64 {
    DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS
}

fn default_web_search_endpoint() -> String {
    DEFAULT_WEB_SEARCH_ENDPOINT.to_string()
}

fn default_web_search_api_key_env() -> String {
    DEFAULT_WEB_SEARCH_API_KEY_ENV.to_string()
}

fn default_maintainer_listen() -> String {
    DEFAULT_MAINTAINER_LISTEN.to_string()
}

fn default_router_listen() -> String {
    DEFAULT_ROUTER_LISTEN.to_string()
}

fn default_router_query_timeout_secs() -> u64 {
    DEFAULT_ROUTER_QUERY_TIMEOUT_SECS
}

fn default_maintainer_sweep_tick_interval_secs() -> u64 {
    DEFAULT_MAINTAINER_SWEEP_TICK_INTERVAL_SECS
}

fn default_maintainer_stale_after_days() -> u32 {
    DEFAULT_MAINTAINER_STALE_AFTER_DAYS
}

fn default_maintainer_deprecated_after_days() -> u32 {
    DEFAULT_MAINTAINER_DEPRECATED_AFTER_DAYS
}

fn default_http_timeout_secs() -> u64 {
    DEFAULT_HTTP_TIMEOUT_SECS
}

fn default_llm_timeout_secs() -> u64 {
    DEFAULT_LLM_TIMEOUT_SECS
}

fn default_http_retry_count() -> u32 {
    DEFAULT_HTTP_RETRY_COUNT
}

fn default_http_retry_base_delay_ms() -> u64 {
    DEFAULT_HTTP_RETRY_BASE_DELAY_MS
}

fn default_http_retry_max_delay_ms() -> u64 {
    DEFAULT_HTTP_RETRY_MAX_DELAY_MS
}

/// Langfuse OTLP tracing 配置。
/// `enabled=false`（默认）时不初始化 tracer，零开销。
/// 所有字段均有 serde default，旧 config.toml 缺 `[langfuse]` 段也能正常加载。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LangfuseConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Langfuse OTLP endpoint，如 `http://localhost:3000/api/public/otel`
    #[serde(default = "default_langfuse_endpoint")]
    pub endpoint: String,
    /// tracer / service.name；可被 `OTEL_SERVICE_NAME` 环境变量覆盖。
    #[serde(default = "default_service_name")]
    pub service_name: String,
    #[serde(default)]
    pub public_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
}

impl Default for LangfuseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_langfuse_endpoint(),
            service_name: default_service_name(),
            public_key: None,
            secret_key: None,
        }
    }
}

pub const DEFAULT_LANGFUSE_ENDPOINT: &str = "http://localhost:3000/api/public/otel";
fn default_langfuse_endpoint() -> String {
    DEFAULT_LANGFUSE_ENDPOINT.to_string()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum EmbeddingProvider {
    #[serde(rename = "openai_compatible")]
    OpenAiCompatible,
    #[serde(rename = "ark_multimodal")]
    ArkMultimodal,
}

fn default_embedding_provider() -> EmbeddingProvider {
    EmbeddingProvider::OpenAiCompatible
}

fn default_embedding_endpoint() -> String {
    "https://api.openai.com/v1/embeddings".to_string()
}

fn default_embedding_model() -> String {
    "text-embedding-3-small".to_string()
}

fn default_embedding_timeout_secs() -> u64 {
    60
}

fn default_embedding_max_concurrency() -> usize {
    4
}

fn default_embedding_api_key_env() -> String {
    "EMBEDDING_API_KEY".to_string()
}

/// router 后续用于向量生成的 embedding 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingConfig {
    #[serde(default = "default_embedding_provider")]
    pub provider: EmbeddingProvider,
    #[serde(default = "default_embedding_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_embedding_model")]
    pub model: String,
    #[serde(default = "default_embedding_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_embedding_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_embedding_api_key_env")]
    pub api_key_env: String,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            provider: default_embedding_provider(),
            endpoint: default_embedding_endpoint(),
            model: default_embedding_model(),
            timeout_secs: default_embedding_timeout_secs(),
            max_concurrency: default_embedding_max_concurrency(),
            api_key_env: default_embedding_api_key_env(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum RerankProvider {
    #[serde(rename = "heuristic")]
    Heuristic,
    #[serde(rename = "openai_chat", alias = "openai_compatible_chat")]
    OpenAiChat,
    #[serde(rename = "openai_responses", alias = "openai_compatible_responses")]
    OpenAiResponses,
}

fn default_rerank_provider() -> RerankProvider {
    RerankProvider::OpenAiChat
}

fn default_rerank_endpoint() -> String {
    "https://api.openai.com/v1/chat/completions".to_string()
}

fn default_rerank_model() -> String {
    "gpt-5.6-luna".to_string()
}

fn default_rerank_timeout_secs() -> u64 {
    30
}

fn default_rerank_max_tokens() -> u32 {
    512
}

fn default_rerank_api_key_env() -> String {
    "OPENAI_API_KEY".to_string()
}

fn default_llm_api_key_env() -> String {
    String::new()
}

/// router top-K rerank 配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterRerankConfig {
    #[serde(default = "default_rerank_provider")]
    pub provider: RerankProvider,
    #[serde(default = "default_rerank_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_rerank_model")]
    pub model: String,
    #[serde(default = "default_rerank_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_rerank_max_tokens")]
    pub max_tokens: u32,
    #[serde(default = "default_rerank_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_http_retry_count")]
    pub retry_count: u32,
    #[serde(default = "default_http_retry_base_delay_ms")]
    pub retry_base_delay_ms: u64,
    #[serde(default = "default_http_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

impl Default for RouterRerankConfig {
    fn default() -> Self {
        Self {
            provider: default_rerank_provider(),
            endpoint: default_rerank_endpoint(),
            model: default_rerank_model(),
            timeout_secs: default_rerank_timeout_secs(),
            max_tokens: default_rerank_max_tokens(),
            api_key_env: default_rerank_api_key_env(),
            retry_count: default_http_retry_count(),
            retry_base_delay_ms: default_http_retry_base_delay_ms(),
            retry_max_delay_ms: default_http_retry_max_delay_ms(),
        }
    }
}

fn default_router_hybrid_enabled() -> bool {
    true
}

fn default_router_hybrid_lexical_top_n() -> usize {
    24
}

fn default_router_hybrid_vector_top_m() -> usize {
    24
}

fn default_router_hybrid_top_k() -> usize {
    16
}

fn default_router_hybrid_rerank_enabled() -> bool {
    true
}

fn default_router_hybrid_vector_worker_poll_secs() -> u64 {
    2
}

fn default_router_hybrid_vector_query_timeout_secs() -> u64 {
    5
}

fn default_router_hybrid_vector_retry_base_delay_ms() -> u64 {
    2_000
}

fn default_router_hybrid_vector_retry_max_delay_ms() -> u64 {
    30_000
}

fn default_router_refresh_interval_secs() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LlmChatConfig {
    pub provider: LlmProvider,
    pub endpoint: String,
    pub model: String,
    #[serde(default)]
    pub reasoning_effort: ReasoningEffort,
    #[serde(default = "default_llm_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_llm_max_tokens")]
    pub max_tokens: u32,
    #[serde(default = "default_llm_context_window")]
    pub context_window: usize,
    #[serde(default = "default_llm_timeout_secs")]
    pub timeout_secs: u64,
    /// 失败重试次数。语义：在首次失败后**额外**重试的次数。
    /// `retry_count = 0` 仅尝试 1 次，失败即返回；`retry_count = 1` 共尝试 2 次。
    #[serde(default = "default_http_retry_count")]
    pub retry_count: u32,
    /// 重试退避基础间隔（毫秒）。指数退避：第 N 次等待 = base * 2^(N-1)，再叠加 ±50% 随机抖动。
    #[serde(default = "default_http_retry_base_delay_ms")]
    pub retry_base_delay_ms: u64,
    /// 退避上限（毫秒），超过则截断，避免单次等待过长。
    #[serde(default = "default_http_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
    /// 仅由环境变量注入，配置文件不持久化
    #[serde(skip)]
    pub api_key: Option<String>,
}

impl Default for LlmChatConfig {
    fn default() -> Self {
        Self {
            provider: LlmProvider::Anthropic,
            endpoint: "https://api.anthropic.com".to_string(),
            model: "claude-sonnet-4-20250514".to_string(),
            reasoning_effort: ReasoningEffort::None,
            api_key_env: "ANTHROPIC_API_KEY".to_string(),
            max_tokens: default_llm_max_tokens(),
            context_window: default_llm_context_window(),
            timeout_secs: default_llm_timeout_secs(),
            retry_count: default_http_retry_count(),
            retry_base_delay_ms: default_http_retry_base_delay_ms(),
            retry_max_delay_ms: default_http_retry_max_delay_ms(),
            api_key: None,
        }
    }
}

/// Agent 主 LLM 请求的推理强度。
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    #[default]
    None,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum LlmProvider {
    #[serde(rename = "anthropic")]
    Anthropic,
    #[serde(rename = "openai_chat", alias = "openai_compatible_chat")]
    OpenAiChat,
    #[serde(rename = "openai_responses", alias = "openai_compatible_responses")]
    OpenAiResponses,
}

/// router retrieval 的开关与 top-N 参数。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterRetrievalConfig {
    #[serde(default = "default_router_hybrid_enabled")]
    pub enabled: bool,
    #[serde(default = "default_router_hybrid_lexical_top_n")]
    pub lexical_top_n: usize,
    #[serde(default = "default_router_hybrid_vector_top_m")]
    pub vector_top_m: usize,
    #[serde(default = "default_router_hybrid_top_k")]
    pub top_k: usize,
    #[serde(default = "default_router_hybrid_rerank_enabled")]
    pub rerank_enabled: bool,
    #[serde(default)]
    pub vector: RouterRetrievalVectorConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterRetrievalVectorConfig {
    #[serde(default = "default_router_hybrid_vector_worker_poll_secs")]
    pub worker_poll_secs: u64,
    #[serde(default = "default_router_hybrid_vector_query_timeout_secs")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_router_hybrid_vector_retry_base_delay_ms")]
    pub retry_base_delay_ms: u64,
    #[serde(default = "default_router_hybrid_vector_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

impl Default for RouterRetrievalConfig {
    fn default() -> Self {
        Self {
            enabled: default_router_hybrid_enabled(),
            lexical_top_n: default_router_hybrid_lexical_top_n(),
            vector_top_m: default_router_hybrid_vector_top_m(),
            top_k: default_router_hybrid_top_k(),
            rerank_enabled: default_router_hybrid_rerank_enabled(),
            vector: RouterRetrievalVectorConfig::default(),
        }
    }
}

impl Default for RouterRetrievalVectorConfig {
    fn default() -> Self {
        Self {
            worker_poll_secs: default_router_hybrid_vector_worker_poll_secs(),
            query_timeout_secs: default_router_hybrid_vector_query_timeout_secs(),
            retry_base_delay_ms: default_router_hybrid_vector_retry_base_delay_ms(),
            retry_max_delay_ms: default_router_hybrid_vector_retry_max_delay_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    #[serde(default = "default_router_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
    #[serde(default = "default_router_daemon_config")]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub auth: RouterAuthConfig,
    #[serde(default)]
    pub retrieval: RouterRetrievalConfig,
    #[serde(default)]
    pub embedding: EmbeddingConfig,
    #[serde(default)]
    pub rerank: RouterRerankConfig,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            refresh_interval_secs: default_router_refresh_interval_secs(),
            daemon: default_router_daemon_config(),
            auth: RouterAuthConfig::default(),
            retrieval: RouterRetrievalConfig::default(),
            embedding: EmbeddingConfig::default(),
            rerank: RouterRerankConfig::default(),
        }
    }
}

/// Prompt 模板配置，仅供测试 / 内部构造外部模板目录。
#[derive(Debug, Clone, Default)]
pub struct PromptConfig {
    /// 正常安装后的 binary 不依赖外部 prompts 目录。
    pub root: Option<PathBuf>,
}

impl PromptConfig {
    /// 返回显式外部模板目录；缺省配置走内置模板。
    pub fn external_root(&self) -> Option<&Path> {
        self.root.as_deref()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolConfig {
    /// 本地工具的默认工作目录与相对路径基准。相对路径按进程 CWD 解析。
    #[serde(skip_serializing)]
    pub workspace_root: PathBuf,
    #[serde(default = "default_file_read_max_chars")]
    pub file_read_max_chars: usize,
    #[serde(default = "default_file_diff_max_changed_lines")]
    pub file_diff_max_changed_lines: usize,
    #[serde(default = "default_max_parallel_tool_calls")]
    pub max_parallel_tool_calls: usize,
    /// background-shell 的固定初始观察窗口；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) code_run_initial_yield_ms: u64,
    /// background-shell 的固定 yield 下限；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) code_run_min_yield_ms: u64,
    /// background-shell 的固定 yield 上限；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) code_run_max_yield_ms: u64,
    /// background-shell 写入后的固定观察窗口；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) code_run_write_yield_ms: u64,
    /// background-shell 空轮询的固定观察窗口；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) code_run_poll_yield_ms: u64,
    #[serde(default = "default_code_run_max_output_chars")]
    pub code_run_max_output_chars: usize,
    #[serde(default = "default_write_stdin_max_poll_timeout_ms")]
    pub write_stdin_max_poll_timeout_ms: u64,
    /// background-shell 输出 buffer 的内部资源护栏；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) background_process_output_buffer_bytes: usize,
    /// background-shell owner 容量的内部资源护栏；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) background_process_max_entries_per_owner: usize,
    /// background-shell LRU 保护数量的内部资源护栏；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) background_process_protected_recent_entries: usize,
    /// background-shell PTY 尺寸的内部默认；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) background_process_pty_rows: u16,
    #[serde(skip_serializing)]
    pub(crate) background_process_pty_cols: u16,
    /// background-shell PTY 输入 buffer 的内部资源护栏；不开放 TOML 覆盖。
    #[serde(skip_serializing)]
    pub(crate) background_process_pty_input_buffer_bytes: usize,
    #[serde(skip_serializing)]
    pub(crate) background_process_output_drain_grace_ms: u64,
    #[serde(default = "default_session_search_default_limit")]
    pub session_search_default_limit: usize,
    #[serde(default = "default_session_search_max_limit")]
    pub session_search_max_limit: usize,
    #[serde(default = "default_session_search_sqlite_busy_timeout_ms")]
    pub session_search_sqlite_busy_timeout_ms: u64,
    #[serde(default)]
    pub web: WebConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WebConfig {
    #[serde(default = "default_web_lookup_max_chars")]
    pub lookup_max_chars: usize,
    #[serde(default = "default_web_search_max_count")]
    pub max_count: usize,
    #[serde(default = "default_web_search_max_content_chars")]
    pub max_content_chars: usize,
    #[serde(default = "default_web_search_max_total_chars")]
    pub max_total_chars: usize,
    #[serde(default = "default_web_search_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_web_search_api_key_env")]
    pub api_key_env: String,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            lookup_max_chars: default_web_lookup_max_chars(),
            max_count: default_web_search_max_count(),
            max_content_chars: default_web_search_max_content_chars(),
            max_total_chars: default_web_search_max_total_chars(),
            endpoint: default_web_search_endpoint(),
            api_key_env: default_web_search_api_key_env(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolConfigFile {
    #[serde(default = "default_file_read_max_chars")]
    file_read_max_chars: usize,
    #[serde(default = "default_file_diff_max_changed_lines")]
    file_diff_max_changed_lines: usize,
    #[serde(default = "default_max_parallel_tool_calls")]
    max_parallel_tool_calls: usize,
    #[serde(default = "default_code_run_max_output_chars")]
    code_run_max_output_chars: usize,
    #[serde(default = "default_write_stdin_max_poll_timeout_ms")]
    write_stdin_max_poll_timeout_ms: u64,
    #[serde(default = "default_session_search_default_limit")]
    session_search_default_limit: usize,
    #[serde(default = "default_session_search_max_limit")]
    session_search_max_limit: usize,
    #[serde(default = "default_session_search_sqlite_busy_timeout_ms")]
    session_search_sqlite_busy_timeout_ms: u64,
    #[serde(default)]
    web: WebConfig,
}

impl Default for ToolConfigFile {
    fn default() -> Self {
        Self {
            file_read_max_chars: default_file_read_max_chars(),
            file_diff_max_changed_lines: default_file_diff_max_changed_lines(),
            max_parallel_tool_calls: default_max_parallel_tool_calls(),
            code_run_max_output_chars: default_code_run_max_output_chars(),
            write_stdin_max_poll_timeout_ms: default_write_stdin_max_poll_timeout_ms(),
            session_search_default_limit: default_session_search_default_limit(),
            session_search_max_limit: default_session_search_max_limit(),
            session_search_sqlite_busy_timeout_ms: default_session_search_sqlite_busy_timeout_ms(),
            web: WebConfig::default(),
        }
    }
}

impl From<ToolConfigFile> for ToolConfig {
    fn from(value: ToolConfigFile) -> Self {
        Self {
            workspace_root: default_workspace_root(),
            file_read_max_chars: value.file_read_max_chars,
            file_diff_max_changed_lines: value.file_diff_max_changed_lines,
            max_parallel_tool_calls: value.max_parallel_tool_calls,
            code_run_initial_yield_ms: default_code_run_initial_yield_ms(),
            code_run_min_yield_ms: default_code_run_min_yield_ms(),
            code_run_max_yield_ms: default_code_run_max_yield_ms(),
            code_run_write_yield_ms: default_code_run_write_yield_ms(),
            code_run_poll_yield_ms: default_code_run_poll_yield_ms(),
            code_run_max_output_chars: value.code_run_max_output_chars,
            write_stdin_max_poll_timeout_ms: value.write_stdin_max_poll_timeout_ms,
            background_process_output_buffer_bytes: default_background_process_output_buffer_bytes(
            ),
            background_process_max_entries_per_owner:
                default_background_process_max_entries_per_owner(),
            background_process_protected_recent_entries:
                default_background_process_protected_recent_entries(),
            background_process_pty_rows: default_background_process_pty_rows(),
            background_process_pty_cols: default_background_process_pty_cols(),
            background_process_pty_input_buffer_bytes:
                default_background_process_pty_input_buffer_bytes(),
            background_process_output_drain_grace_ms:
                default_background_process_output_drain_grace_ms(),
            session_search_default_limit: value.session_search_default_limit,
            session_search_max_limit: value.session_search_max_limit,
            session_search_sqlite_busy_timeout_ms: value.session_search_sqlite_busy_timeout_ms,
            web: value.web,
        }
    }
}

impl<'de> Deserialize<'de> for ToolConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        ToolConfigFile::deserialize(deserializer).map(Self::from)
    }
}

impl Default for ToolConfig {
    fn default() -> Self {
        ToolConfigFile::default().into()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "default_acn_home")]
    pub acn_home: PathBuf,
    #[serde(skip)]
    pub(crate) base_acn_home: PathBuf,
    /// Router / Maintainer 的团队数据根，始终从 base `acn_home` 派生。
    #[serde(skip)]
    pub team_root: PathBuf,
    /// Agent 本地数据根，激活 upstream 后切换到对应 runtime。
    #[serde(skip)]
    pub agents_root: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    pub listen: String,
}

fn default_router_daemon_config() -> DaemonConfig {
    DaemonConfig {
        listen: default_router_listen(),
    }
}

fn default_maintainer_daemon_config() -> DaemonConfig {
    DaemonConfig {
        listen: default_maintainer_listen(),
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientsConfig {
    #[serde(default)]
    pub router: RouterClientConfig,
    #[serde(default)]
    pub http: HttpClientConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    pub agent_id: String,
    #[serde(default)]
    pub maintainer_endpoint: String,
    #[serde(default)]
    pub router_endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acn_key_env: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedUpstream {
    pub name: String,
    pub agent_id: AgentId,
    pub maintainer_endpoint: String,
    pub router_endpoint: String,
    pub acn_key: Option<String>,
    pub runtime_acn_home: PathBuf,
}

impl ResolvedUpstream {
    /// 只有 maintainer/router endpoint 同时配置时才启用团队服务。
    pub fn team_services_configured(&self) -> bool {
        !self.maintainer_endpoint.trim().is_empty() && !self.router_endpoint.trim().is_empty()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterClientConfig {
    #[serde(default = "default_router_query_timeout_secs")]
    pub query_timeout_secs: u64,
}

impl Default for RouterClientConfig {
    fn default() -> Self {
        Self {
            query_timeout_secs: default_router_query_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryConfig {
    #[serde(default = "default_memory_char_limit")]
    pub memory_char_limit: usize,
    #[serde(default = "default_user_char_limit")]
    pub user_char_limit: usize,
    #[serde(default = "default_memory_safety_scan")]
    pub memory_safety_scan: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            memory_char_limit: default_memory_char_limit(),
            user_char_limit: default_user_char_limit(),
            memory_safety_scan: default_memory_safety_scan(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCompactionConfig {
    #[serde(default = "default_session_compaction_summary_max_chars")]
    pub summary_max_chars: usize,
    #[serde(default = "default_auto_compact_ctx_ratio")]
    pub auto_compact_ctx_ratio: f64,
    #[serde(default = "default_compaction_tail_target_ctx_ratio")]
    pub tail_target_ctx_ratio: f64,
    #[serde(default = "default_compaction_tail_hard_ctx_ratio")]
    pub tail_hard_ctx_ratio: f64,
    #[serde(default = "default_compaction_tail_previous_real_user_turns")]
    pub tail_previous_real_user_turns: usize,
    #[serde(default = "default_compaction_tool_result_raw_max_chars")]
    pub tool_result_raw_max_chars: usize,
}

impl Default for SessionCompactionConfig {
    fn default() -> Self {
        Self {
            summary_max_chars: default_session_compaction_summary_max_chars(),
            auto_compact_ctx_ratio: default_auto_compact_ctx_ratio(),
            tail_target_ctx_ratio: default_compaction_tail_target_ctx_ratio(),
            tail_hard_ctx_ratio: default_compaction_tail_hard_ctx_ratio(),
            tail_previous_real_user_turns: default_compaction_tail_previous_real_user_turns(),
            tool_result_raw_max_chars: default_compaction_tool_result_raw_max_chars(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMemoryReviewConfig {
    #[serde(default = "default_fork_memory_review_interval_turns")]
    pub interval_turns: usize,
}

impl Default for AgentMemoryReviewConfig {
    fn default() -> Self {
        Self {
            interval_turns: default_fork_memory_review_interval_turns(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionTurnJournalConfig {
    #[serde(default = "default_turn_journal_delta_snapshot_interval_ms")]
    pub delta_snapshot_interval_ms: u64,
    #[serde(default = "default_turn_journal_delta_snapshot_chars")]
    pub delta_snapshot_chars: usize,
    #[serde(default = "default_turn_recovery_original_user_request_max_chars")]
    pub recovery_original_user_request_max_chars: usize,
    #[serde(default = "default_turn_recovery_partial_assistant_max_chars")]
    pub recovery_partial_assistant_max_chars: usize,
    #[serde(default = "default_turn_recovery_tool_input_max_chars")]
    pub recovery_tool_input_max_chars: usize,
    #[serde(default = "default_turn_recovery_tool_output_max_chars")]
    pub recovery_tool_output_max_chars: usize,
    #[serde(default = "default_turn_recovery_user_steer_max_chars")]
    pub recovery_user_steer_max_chars: usize,
}

impl Default for AgentSessionTurnJournalConfig {
    fn default() -> Self {
        Self {
            delta_snapshot_interval_ms: default_turn_journal_delta_snapshot_interval_ms(),
            delta_snapshot_chars: default_turn_journal_delta_snapshot_chars(),
            recovery_original_user_request_max_chars:
                default_turn_recovery_original_user_request_max_chars(),
            recovery_partial_assistant_max_chars: default_turn_recovery_partial_assistant_max_chars(
            ),
            recovery_tool_input_max_chars: default_turn_recovery_tool_input_max_chars(),
            recovery_tool_output_max_chars: default_turn_recovery_tool_output_max_chars(),
            recovery_user_steer_max_chars: default_turn_recovery_user_steer_max_chars(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionTuiConfig {
    #[serde(default = "default_live_response_preview_max_lines")]
    pub live_response_preview_max_lines: i64,
}

impl Default for AgentSessionTuiConfig {
    fn default() -> Self {
        Self {
            live_response_preview_max_lines: default_live_response_preview_max_lines(),
        }
    }
}

/// 显式 `/skill` 注入正文的边界，避免单轮输入意外放大模型上下文。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionSkillConfig {
    #[serde(default = "default_session_skill_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default = "default_session_skill_max_per_turn")]
    pub max_per_turn: usize,
}

impl Default for AgentSessionSkillConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: default_session_skill_max_body_bytes(),
            max_per_turn: default_session_skill_max_per_turn(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionDelegationConfig {
    #[serde(default = "default_session_delegation_max_concurrent")]
    pub max_concurrent: usize,
    #[serde(default = "default_session_delegation_max_tool_loop_turns")]
    pub max_tool_loop_turns: usize,
    #[serde(default = "default_session_delegation_wall_timeout_secs")]
    pub wall_timeout_secs: u64,
    #[serde(default)]
    pub wait: AgentSessionDelegationWaitConfig,
    #[serde(default)]
    pub compaction: Option<SessionCompactionConfig>,
}

impl Default for AgentSessionDelegationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: default_session_delegation_max_concurrent(),
            max_tool_loop_turns: default_session_delegation_max_tool_loop_turns(),
            wall_timeout_secs: default_session_delegation_wall_timeout_secs(),
            wait: AgentSessionDelegationWaitConfig::default(),
            compaction: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionDelegationWaitConfig {
    #[serde(default = "default_session_delegation_wait_default_timeout_secs")]
    pub default_timeout_secs: u64,
    #[serde(default = "default_session_delegation_wait_min_timeout_secs")]
    pub min_timeout_secs: u64,
    #[serde(default = "default_session_delegation_wait_max_timeout_secs")]
    pub max_timeout_secs: u64,
}

impl Default for AgentSessionDelegationWaitConfig {
    fn default() -> Self {
        Self {
            default_timeout_secs: default_session_delegation_wait_default_timeout_secs(),
            min_timeout_secs: default_session_delegation_wait_min_timeout_secs(),
            max_timeout_secs: default_session_delegation_wait_max_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionConfig {
    #[serde(default = "default_id_mint_max_retries")]
    pub id_mint_max_retries: u32,
    #[serde(default = "default_session_notify_on_finalize_completion")]
    pub notify_on_finalize_completion: bool,
    #[serde(default = "default_session_cleanup_retention_days")]
    pub cleanup_retention_days: u32,
    #[serde(default)]
    pub compaction: SessionCompactionConfig,
    #[serde(default)]
    pub memory_review: AgentMemoryReviewConfig,
    #[serde(default)]
    pub turn_journal: AgentSessionTurnJournalConfig,
    #[serde(default)]
    pub user_shell: UserShellConfig,
    #[serde(default)]
    pub tui: AgentSessionTuiConfig,
    #[serde(default)]
    pub skills: AgentSessionSkillConfig,
    #[serde(default)]
    pub subagents: AgentSessionDelegationConfig,
}

impl AgentSessionConfig {
    /// 把"重抽次数"语义换算成"总尝试次数"（首次 + 重抽），session id 创建直接拿来当循环上限。
    pub const fn id_mint_max_attempts(&self) -> usize {
        retries_to_attempts(self.id_mint_max_retries)
    }
}

impl Default for AgentSessionConfig {
    fn default() -> Self {
        Self {
            id_mint_max_retries: default_id_mint_max_retries(),
            notify_on_finalize_completion: default_session_notify_on_finalize_completion(),
            cleanup_retention_days: default_session_cleanup_retention_days(),
            compaction: SessionCompactionConfig::default(),
            memory_review: AgentMemoryReviewConfig::default(),
            turn_journal: AgentSessionTurnJournalConfig::default(),
            user_shell: UserShellConfig::default(),
            tui: AgentSessionTuiConfig::default(),
            skills: AgentSessionSkillConfig::default(),
            subagents: AgentSessionDelegationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserShellConfig {
    #[serde(default = "default_user_shell_enabled")]
    pub enabled: bool,
    #[serde(default = "default_user_shell_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_user_shell_max_output_chars")]
    pub max_output_chars: usize,
    #[serde(default = "default_user_shell_shell")]
    pub shell: String,
    #[serde(default = "default_user_shell_login_shell")]
    pub login_shell: bool,
}

impl Default for UserShellConfig {
    fn default() -> Self {
        Self {
            enabled: default_user_shell_enabled(),
            timeout_secs: default_user_shell_timeout_secs(),
            max_output_chars: default_user_shell_max_output_chars(),
            shell: default_user_shell_shell(),
            login_shell: default_user_shell_login_shell(),
        }
    }
}

/// `[agent.attachment]`：TUI 附件输入（`@path` / 剪贴板图片）与媒体附件读取限制。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentConfig {
    #[serde(default = "default_attachment_enabled")]
    pub enabled: bool,
    #[serde(default = "default_attachment_clipboard_image_enabled")]
    pub clipboard_image_enabled: bool,
    #[serde(default = "default_attachment_max_file_bytes")]
    pub max_file_bytes: u64,
    #[serde(default = "default_attachment_max_files_per_turn")]
    pub max_files_per_turn: usize,
}

impl Default for AttachmentConfig {
    fn default() -> Self {
        Self {
            enabled: default_attachment_enabled(),
            clipboard_image_enabled: default_attachment_clipboard_image_enabled(),
            max_file_bytes: default_attachment_max_file_bytes(),
            max_files_per_turn: default_attachment_max_files_per_turn(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInboxConfig {
    #[serde(default = "default_inbox_processing_stale_after_secs")]
    pub processing_stale_after_secs: u64,
}

impl Default for AgentInboxConfig {
    fn default() -> Self {
        Self {
            processing_stale_after_secs: default_inbox_processing_stale_after_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default)]
    pub llm: LlmChatConfig,
    #[serde(default)]
    pub inbox: AgentInboxConfig,
    #[serde(default)]
    pub session: AgentSessionConfig,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub tool: ToolConfig,
    #[serde(default)]
    pub attachment: AttachmentConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpClientConfig {
    #[serde(default = "default_http_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_http_retry_count")]
    pub retry_count: u32,
    #[serde(default = "default_http_retry_base_delay_ms")]
    pub retry_base_delay_ms: u64,
    #[serde(default = "default_http_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_http_timeout_secs(),
            retry_count: default_http_retry_count(),
            retry_base_delay_ms: default_http_retry_base_delay_ms(),
            retry_max_delay_ms: default_http_retry_max_delay_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerHistoryConfig {
    #[serde(default = "default_maintainer_history_max_file_bytes")]
    pub max_file_bytes: u64,
    #[serde(default = "default_maintainer_history_backup_count")]
    pub backup_count: usize,
}

impl Default for MaintainerHistoryConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: default_maintainer_history_max_file_bytes(),
            backup_count: default_maintainer_history_backup_count(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TeamAuthToggleConfig {
    #[serde(default = "default_team_auth_enabled")]
    pub enabled: bool,
}

impl Default for TeamAuthToggleConfig {
    fn default() -> Self {
        Self {
            enabled: default_team_auth_enabled(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterAuthConfig {
    #[serde(default)]
    pub team: TeamAuthToggleConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerAuthConfig {
    #[serde(default)]
    pub admin: MaintainerAdminAuthConfig,
    #[serde(default)]
    pub team: TeamAuthToggleConfig,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerAdminAuthConfig {
    #[serde(default = "default_maintainer_admin_auth_enabled")]
    pub enabled: bool,
    #[serde(default = "default_maintainer_admin_auth_username")]
    pub username: String,
    #[serde(default = "default_maintainer_admin_auth_password_env")]
    pub password_env: String,
    /// 仅由环境变量注入，配置文件不持久化。
    #[serde(skip)]
    pub password: Option<String>,
}

impl std::fmt::Debug for MaintainerAdminAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintainerAdminAuthConfig")
            .field("enabled", &self.enabled)
            .field("username", &self.username)
            .field("password_env", &self.password_env)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Default for MaintainerAdminAuthConfig {
    fn default() -> Self {
        Self {
            enabled: default_maintainer_admin_auth_enabled(),
            username: default_maintainer_admin_auth_username(),
            password_env: default_maintainer_admin_auth_password_env(),
            password: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerSweepConfig {
    #[serde(default = "default_maintainer_sweep_tick_interval_secs")]
    pub tick_interval_secs: u64,
    #[serde(default = "default_maintainer_stale_after_days")]
    pub stale_after_days: u32,
    #[serde(default = "default_maintainer_deprecated_after_days")]
    pub deprecated_after_days: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerUiConfig {
    #[serde(default = "default_frontend_dist_dir")]
    pub frontend_dist_dir: PathBuf,
}

pub const DEFAULT_MAINTAINER_FRONTEND_DIST_DIR: &str = "./frontend/maintainer-workbench/dist";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerIdConfig {
    /// Maintainer 生成需要查重的 ID 时，发生碰撞后的最大重抽次数。
    /// 当前包括 policy、outbox inbox、action ID；总尝试次数 = 1（首次）+ mint_max_retries。
    #[serde(default = "default_id_mint_max_retries")]
    pub mint_max_retries: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintainerConfig {
    #[serde(default)]
    pub sweep: MaintainerSweepConfig,
    #[serde(default = "default_maintainer_daemon_config")]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub history: MaintainerHistoryConfig,
    #[serde(default)]
    pub ui: MaintainerUiConfig,
    #[serde(default)]
    pub id: MaintainerIdConfig,
    #[serde(default)]
    pub auth: MaintainerAuthConfig,
}

impl Default for MaintainerSweepConfig {
    fn default() -> Self {
        Self {
            tick_interval_secs: default_maintainer_sweep_tick_interval_secs(),
            stale_after_days: default_maintainer_stale_after_days(),
            deprecated_after_days: default_maintainer_deprecated_after_days(),
        }
    }
}

impl Default for MaintainerConfig {
    fn default() -> Self {
        Self {
            sweep: MaintainerSweepConfig::default(),
            daemon: default_maintainer_daemon_config(),
            history: MaintainerHistoryConfig::default(),
            ui: MaintainerUiConfig::default(),
            id: MaintainerIdConfig::default(),
            auth: MaintainerAuthConfig::default(),
        }
    }
}

impl MaintainerConfig {
    /// 把 Maintainer ID 的"重抽次数"换算成"总尝试次数"（首次 + 重抽），下游 mint helper 直接拿来当循环上限。
    pub const fn id_mint_max_attempts(&self) -> usize {
        retries_to_attempts(self.id.mint_max_retries)
    }
}

impl Default for MaintainerUiConfig {
    fn default() -> Self {
        Self {
            frontend_dist_dir: default_frontend_dist_dir(),
        }
    }
}

impl Default for MaintainerIdConfig {
    fn default() -> Self {
        Self {
            mint_max_retries: default_id_mint_max_retries(),
        }
    }
}

fn default_frontend_dist_dir() -> PathBuf {
    PathBuf::from(DEFAULT_MAINTAINER_FRONTEND_DIST_DIR)
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("读取配置文件失败: {0}")]
    Io(#[from] std::io::Error),
    #[error("解析配置文件失败: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("创建默认 config 目录失败: {path} ({source})")]
    CreateDefaultConfigDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("写入默认 config 文件失败: {path} ({source})")]
    WriteDefaultConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("创建 ACN 存储目录失败: {path} ({source})")]
    CreateStorageDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Agent runtime 路径不能包含 symlink: {path}")]
    AgentRuntimeSymlink { path: PathBuf },
    #[error(
        "检测到旧版本遗留的 upstream 团队数据目录: {path}；请停止 Agent、Router 和 Maintainer，手动确认并合并到 {team_root} 后再删除该目录"
    )]
    LegacyUpstreamTeamStorage { path: PathBuf, team_root: PathBuf },
    #[error("清理旧版本遗留的空 upstream 团队目录失败: {path} ({source})")]
    CleanupLegacyUpstreamTeamDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("配置校验失败: {0}")]
    Validation(String),
}

impl Config {
    /// 加载显式 config；未传时支持 `ACN_CONFIG`，否则初始化并加载 `<acn_home>/config.toml`。
    pub fn load_or_init(explicit_path: Option<&Path>) -> Result<(Self, PathBuf), ConfigError> {
        Self::load_or_init_with_options(
            explicit_path,
            ConfigLoadOptions {
                require_agent_llm_api_key: true,
                require_maintainer_admin_auth_password: true,
                validate_upstreams: true,
                ensure_storage_dirs: true,
            },
        )
    }

    /// 为 agent CLI 加载配置；不启动 maintainer，因此不强制读取管理台密码。
    pub fn load_or_init_for_agent(
        explicit_path: Option<&Path>,
    ) -> Result<(Self, PathBuf), ConfigError> {
        Self::load_or_init_with_options(
            explicit_path,
            ConfigLoadOptions {
                require_agent_llm_api_key: true,
                require_maintainer_admin_auth_password: false,
                validate_upstreams: true,
                ensure_storage_dirs: false,
            },
        )
    }

    /// 为 supervisor 管理命令只读加载配置；只需要定位 agent_home，不强制读取 LLM 密钥。
    pub fn load_or_init_for_supervisor_control(
        explicit_path: Option<&Path>,
    ) -> Result<(Self, PathBuf), ConfigError> {
        let options = ConfigLoadOptions {
            require_agent_llm_api_key: false,
            require_maintainer_admin_auth_password: false,
            validate_upstreams: true,
            ensure_storage_dirs: false,
        };
        let explicit_or_env = explicit_path
            .map(Path::to_path_buf)
            .or_else(|| std::env::var_os("ACN_CONFIG").map(PathBuf::from));
        match explicit_or_env {
            Some(path) => Ok((Self::load_with_options(&path, options)?, path)),
            None => {
                let path = default_config_path();
                if path.exists() {
                    Ok((Self::load_with_options(&path, options)?, path))
                } else {
                    let cfg = Self::parse_config_with_options(
                        &default_config_template(),
                        Some(&path),
                        options,
                    )?;
                    Ok((cfg, path))
                }
            }
        }
    }

    /// 为 `acn update` 只读加载配置；允许尚未填写 agent_id 的初始配置。
    pub fn load_or_init_for_update(
        explicit_path: Option<&Path>,
    ) -> Result<(Self, PathBuf), ConfigError> {
        let options = ConfigLoadOptions {
            require_agent_llm_api_key: false,
            require_maintainer_admin_auth_password: false,
            validate_upstreams: false,
            ensure_storage_dirs: false,
        };
        let explicit_or_env = explicit_path
            .map(Path::to_path_buf)
            .or_else(|| std::env::var_os("ACN_CONFIG").map(PathBuf::from));
        match explicit_or_env {
            Some(path) => Ok((Self::load_with_options(&path, options)?, path)),
            None => {
                let path = default_config_path();
                if path.exists() {
                    Ok((Self::load_with_options(&path, options)?, path))
                } else {
                    let cfg = Self::parse_config_with_options(
                        &default_config_template(),
                        Some(&path),
                        options,
                    )?;
                    Ok((cfg, path))
                }
            }
        }
    }

    /// 为 router daemon 加载配置；router 不托管管理台，因此不强制读取管理台密码。
    pub fn load_or_init_for_router(
        explicit_path: Option<&Path>,
    ) -> Result<(Self, PathBuf), ConfigError> {
        Self::load_or_init_with_options(
            explicit_path,
            ConfigLoadOptions {
                require_agent_llm_api_key: false,
                require_maintainer_admin_auth_password: false,
                validate_upstreams: false,
                ensure_storage_dirs: true,
            },
        )
    }

    /// 为 maintainer daemon 加载配置；daemon 使用自身 `storage.acn_home`，不解析 agent upstream。
    pub fn load_or_init_for_maintainer_daemon(
        explicit_path: Option<&Path>,
    ) -> Result<(Self, PathBuf), ConfigError> {
        Self::load_or_init_with_options(
            explicit_path,
            ConfigLoadOptions {
                require_agent_llm_api_key: false,
                require_maintainer_admin_auth_password: true,
                validate_upstreams: false,
                ensure_storage_dirs: true,
            },
        )
    }

    fn load_or_init_with_options(
        explicit_path: Option<&Path>,
        options: ConfigLoadOptions,
    ) -> Result<(Self, PathBuf), ConfigError> {
        let explicit_or_env = explicit_path
            .map(Path::to_path_buf)
            .or_else(|| std::env::var_os("ACN_CONFIG").map(PathBuf::from));
        let path = if let Some(path) = explicit_or_env {
            path
        } else {
            let path = default_config_path();
            if !path.exists() {
                init_default_config(&path)?;
                let cfg = Self::load_with_options(
                    &path,
                    ConfigLoadOptions {
                        require_agent_llm_api_key: false,
                        ensure_storage_dirs: options.ensure_storage_dirs,
                        ..options
                    },
                )?;
                return Ok((cfg, path));
            }
            path
        };
        let cfg = Self::load_with_options(&path, options)?;
        Ok((cfg, path))
    }

    /// 从指定路径加载，并读取配置中声明的环境变量密钥。
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        Self::load_with_options(
            path,
            ConfigLoadOptions {
                require_agent_llm_api_key: true,
                require_maintainer_admin_auth_password: true,
                validate_upstreams: true,
                ensure_storage_dirs: true,
            },
        )
    }

    /// 为 router daemon 从指定路径加载配置；不强制读取 maintainer 管理台密码。
    pub fn load_for_router(path: &Path) -> Result<Self, ConfigError> {
        Self::load_with_options(
            path,
            ConfigLoadOptions {
                require_agent_llm_api_key: false,
                require_maintainer_admin_auth_password: false,
                validate_upstreams: false,
                ensure_storage_dirs: true,
            },
        )
    }

    fn load_with_options(path: &Path, options: ConfigLoadOptions) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        Self::parse_config_with_options(&raw, Some(path), options)
    }

    fn parse_config_with_options(
        raw: &str,
        path: Option<&Path>,
        options: ConfigLoadOptions,
    ) -> Result<Self, ConfigError> {
        let mut cfg: Config = toml::from_str(raw)?;
        cfg.normalize();
        cfg.apply_env_values()?;
        validate_config(&cfg, path, options)?;
        if options.ensure_storage_dirs {
            cfg.ensure_storage_dirs()?;
        }
        Ok(cfg)
    }

    fn normalize(&mut self) {
        self.storage.normalize();
        self.agent.tool.workspace_root = default_workspace_root();
    }

    /// 覆盖 agent 工具的工作目录。该目录只影响工具相对路径，不改变进程 cwd。
    pub fn set_tool_workspace_root(&mut self, workspace_root: PathBuf) {
        self.agent.tool.workspace_root = workspace_root;
    }

    fn ensure_storage_dirs(&self) -> Result<(), ConfigError> {
        for dir in [
            self.storage.acn_home.as_path(),
            self.storage.skills_root().as_path(),
            self.storage.team_root.as_path(),
            self.storage.agents_root.as_path(),
        ] {
            std::fs::create_dir_all(dir).map_err(|source| ConfigError::CreateStorageDir {
                path: dir.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    }

    fn ensure_agent_runtime_dirs(&self, runtime_root: &Path) -> Result<(), ConfigError> {
        for dir in [
            runtime_root.to_path_buf(),
            runtime_root.join("skills"),
            runtime_root.join("data").join("agents"),
        ] {
            self.reject_agent_runtime_symlinks(&dir)?;
            std::fs::create_dir_all(&dir).map_err(|source| ConfigError::CreateStorageDir {
                path: dir.clone(),
                source,
            })?;
            self.reject_agent_runtime_symlinks(&dir)?;
        }
        Ok(())
    }

    fn reject_agent_runtime_symlinks(&self, path: &Path) -> Result<(), ConfigError> {
        let base = self.storage.base_acn_home();
        let relative = path.strip_prefix(base).map_err(|_| {
            ConfigError::Validation(format!(
                "Agent runtime 路径必须位于 acn_home 内: {}",
                path.display()
            ))
        })?;
        let mut current = base.to_path_buf();
        for component in relative.components() {
            current.push(component.as_os_str());
            match current.symlink_metadata() {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(ConfigError::AgentRuntimeSymlink { path: current });
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(ConfigError::Io(err)),
            }
        }
        Ok(())
    }

    fn cleanup_or_reject_legacy_upstream_team_dir(
        &self,
        runtime_root: &Path,
    ) -> Result<(), ConfigError> {
        let path = runtime_root.join("data").join("team");
        let metadata = match path.symlink_metadata() {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(ConfigError::Io(err)),
        };
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(ConfigError::LegacyUpstreamTeamStorage {
                path,
                team_root: self.storage.team_root.clone(),
            });
        }

        let mut entries = std::fs::read_dir(&path)?;
        match entries.next() {
            None => std::fs::remove_dir(&path)
                .map_err(|source| ConfigError::CleanupLegacyUpstreamTeamDir { path, source }),
            Some(Ok(_)) => Err(ConfigError::LegacyUpstreamTeamStorage {
                path,
                team_root: self.storage.team_root.clone(),
            }),
            Some(Err(err)) => Err(ConfigError::Io(err)),
        }
    }

    /// 激活已选择 upstream 的本地运行时目录。
    ///
    /// `storage.acn_home` 在加载配置时代表全局 base；agent CLI 解析 upstream 后
    /// 调用本方法，把 agent 本地状态切换到 `<base>/<upstream>/`。daemon 使用的
    /// `team_root` 始终保留在 base 下，不随 agent upstream 改变。
    pub fn activate_upstream_runtime(
        &mut self,
        upstream: &ResolvedUpstream,
    ) -> Result<(), ConfigError> {
        validate_upstream_name(&upstream.name)?;
        let runtime_root = self.storage.upstream_runtime_root(&upstream.name);
        if upstream.runtime_acn_home != runtime_root {
            return Err(ConfigError::Validation(format!(
                "upstream '{}' 的 runtime 路径与 storage.acn_home 不一致",
                upstream.name
            )));
        }
        self.reject_agent_runtime_symlinks(&runtime_root.join("data").join("team"))?;
        self.cleanup_or_reject_legacy_upstream_team_dir(&runtime_root)?;
        self.ensure_agent_runtime_dirs(&runtime_root)?;
        self.storage
            .activate_agent_runtime_root(runtime_root.as_path());
        Ok(())
    }

    fn apply_env_values(&mut self) -> Result<(), ConfigError> {
        if !self.agent.llm.api_key_env.trim().is_empty() {
            if let Ok(v) = std::env::var(&self.agent.llm.api_key_env) {
                self.agent.llm.api_key = Some(v);
            }
        }
        if let Ok(v) = std::env::var("LANGFUSE_PUBLIC_KEY") {
            self.langfuse.public_key = Some(v);
        }
        if let Ok(v) = std::env::var("LANGFUSE_SECRET_KEY") {
            self.langfuse.secret_key = Some(v);
        }
        if let Ok(v) = std::env::var("OTEL_SERVICE_NAME") {
            self.langfuse.service_name = v;
        }
        let admin_auth = &mut self.maintainer.auth.admin;
        if admin_auth.enabled && !admin_auth.password_env.trim().is_empty() {
            if let Ok(v) = std::env::var(&admin_auth.password_env) {
                admin_auth.password = Some(v);
            }
        }
        Ok(())
    }

    /// agent 本地存储目录：`agents_root/<agent_id>/`
    pub fn agent_home(&self, agent: &AgentId) -> PathBuf {
        self.storage.agents_root.join(agent.as_str())
    }

    /// 解析当前要连接的 upstream。CLI 指定优先，否则使用配置顶层 `upstream`。
    pub fn resolve_upstream(
        &self,
        override_name: Option<&str>,
    ) -> Result<ResolvedUpstream, ConfigError> {
        let name = self.selected_upstream_name(override_name)?;
        let upstream = self.upstream_config_by_name(name)?;
        let raw_agent_id = upstream.agent_id.trim();
        if raw_agent_id == AGENT_ID_PLACEHOLDER {
            return Err(ConfigError::Validation(format!(
                "请在 [upstreams] 中填入你的 agent_id；当前 [upstreams.{name}].agent_id 仍是占位值 {AGENT_ID_PLACEHOLDER}"
            )));
        }
        let agent_id = AgentId::new(raw_agent_id.to_string()).map_err(|err| {
            ConfigError::Validation(format!(
                "[upstreams.{name}].agent_id 不是合法 agent id: {err}"
            ))
        })?;
        let acn_key = resolve_upstream_secret(upstream);
        let runtime_acn_home = self.storage.upstream_runtime_root(name);
        Ok(ResolvedUpstream {
            name: name.to_string(),
            agent_id,
            maintainer_endpoint: upstream.maintainer_endpoint.clone(),
            router_endpoint: upstream.router_endpoint.clone(),
            acn_key,
            runtime_acn_home,
        })
    }

    /// 返回默认 upstream 配置，供 daemon 读取共享 endpoint。
    pub fn default_upstream_config(&self) -> Result<&UpstreamConfig, ConfigError> {
        let name = self.selected_upstream_name(None)?;
        self.upstream_config_by_name(name)
    }

    fn selected_upstream_name<'a>(
        &'a self,
        override_name: Option<&'a str>,
    ) -> Result<&'a str, ConfigError> {
        let name = override_name.unwrap_or(&self.upstream).trim();
        if name.is_empty() {
            return Err(ConfigError::Validation(
                "upstream must not be empty; use --upstream <name> or set top-level upstream"
                    .into(),
            ));
        }
        Ok(name)
    }

    fn upstream_config_by_name(&self, name: &str) -> Result<&UpstreamConfig, ConfigError> {
        self.upstreams.get(name).ok_or_else(|| {
            ConfigError::Validation(format!(
                "upstream '{name}' not found; please define [upstreams.{name}]"
            ))
        })
    }
}

impl StorageConfig {
    fn normalize(&mut self) {
        let base_acn_home = normalize_user_path(&self.acn_home);
        self.base_acn_home = base_acn_home.clone();
        self.team_root = base_acn_home.join("data").join("team");
        self.activate_agent_runtime_root(&base_acn_home);
    }

    fn activate_agent_runtime_root(&mut self, runtime_root: &Path) {
        self.acn_home = runtime_root.to_path_buf();
        self.agents_root = self.acn_home.join("data").join("agents");
    }

    pub fn upstream_runtime_root(&self, upstream: &str) -> PathBuf {
        self.base_acn_home.join(upstream)
    }

    /// 全局 ACN base 目录；各 upstream runtime 从这里派生。
    pub fn base_acn_home(&self) -> &Path {
        &self.base_acn_home
    }

    /// skill 运行时目录：`<acn_home>/<upstream>/skills/`。
    pub fn skills_root(&self) -> PathBuf {
        self.acn_home.join("skills")
    }

    /// ACN Markdown 指令文件：`<acn_home>/<upstream>/ACN.md`。
    pub fn acn_md_path(&self) -> PathBuf {
        self.acn_home.join("ACN.md")
    }

    /// MCP server 配置：`<acn_home>/<upstream>/.mcp.json`。
    pub fn mcp_config_path(&self) -> PathBuf {
        self.acn_home.join(".mcp.json")
    }
}

/// 解析 `--cd` 工作目录，返回真实存在的绝对目录路径。
pub fn resolve_workspace_root(path: Option<&Path>) -> Result<PathBuf, ConfigError> {
    let raw = path.map_or_else(default_workspace_root, normalize_user_path);
    let canonical = std::fs::canonicalize(&raw).map_err(|err| {
        ConfigError::Validation(format!(
            "--cd 指向的目录不存在或不可访问: {} ({err})",
            raw.display()
        ))
    })?;
    let meta = std::fs::metadata(&canonical)?;
    if !meta.is_dir() {
        return Err(ConfigError::Validation(format!(
            "--cd 必须指向已存在目录: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

/// 未显式指定 config 时使用的默认配置文件路径：`<acn_home>/config.toml`。
pub fn default_config_path() -> PathBuf {
    normalize_user_path(&default_acn_home()).join("config.toml")
}

fn init_default_config(path: &Path) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ConfigError::CreateDefaultConfigDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    std::fs::write(path, default_config_template()).map_err(|source| {
        ConfigError::WriteDefaultConfig {
            path: path.to_path_buf(),
            source,
        }
    })
}

fn default_config_template() -> String {
    include_str!("../config.template.toml").to_string()
}

fn normalize_user_path(path: &Path) -> PathBuf {
    let expanded = expand_current_user_home(path);
    if expanded.is_absolute() {
        normalize_path(&expanded)
    } else {
        normalize_path(&default_workspace_root().join(expanded))
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Prefix(_)
            | std::path::Component::RootDir
            | std::path::Component::Normal(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[derive(Clone, Copy)]
struct ConfigLoadOptions {
    require_agent_llm_api_key: bool,
    require_maintainer_admin_auth_password: bool,
    validate_upstreams: bool,
    ensure_storage_dirs: bool,
}

fn validate_config(
    cfg: &Config,
    config_path: Option<&Path>,
    options: ConfigLoadOptions,
) -> Result<(), ConfigError> {
    if options.validate_upstreams {
        if cfg.upstreams.is_empty() {
            return Err(ConfigError::Validation(
                "upstreams must define at least one [upstreams.<name>] table".into(),
            ));
        }
        if !cfg.upstream.trim().is_empty() && !cfg.upstreams.contains_key(cfg.upstream.trim()) {
            return Err(ConfigError::Validation(format!(
                "upstream '{}' not found; please define [upstreams.{}]",
                cfg.upstream.trim(),
                cfg.upstream.trim()
            )));
        }
        for (name, upstream) in &cfg.upstreams {
            validate_upstream_name(name)?;
            if upstream.agent_id.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "upstreams.{name}.agent_id must not be empty"
                )));
            }
            let maintainer_configured = !upstream.maintainer_endpoint.trim().is_empty();
            let router_configured = !upstream.router_endpoint.trim().is_empty();
            if maintainer_configured != router_configured {
                return Err(ConfigError::Validation(format!(
                    "upstreams.{name}.maintainer_endpoint and router_endpoint must both be configured or both be empty"
                )));
            }
        }
    }
    if options.require_agent_llm_api_key {
        let env = cfg.agent.llm.api_key_env.as_str();
        if env.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "config 文件 '{}' 未配置 [agent.llm].api_key_env，请填写用于读取 LLM API key 的环境变量名！",
                config_path_for_error(config_path)
            )));
        }
        if cfg.agent.llm.api_key.as_deref().is_none_or(str::is_empty) {
            return Err(ConfigError::Validation(format!(
                "config 文件 '{}' 中 [agent.llm].api_key_env 指定的环境变量 '{}' 未设置或为空！",
                config_path_for_error(config_path),
                env
            )));
        }
    }
    if cfg.agent.llm.max_tokens == 0 {
        return Err(ConfigError::Validation(
            "agent.llm.max_tokens must be > 0".into(),
        ));
    }
    if cfg.agent.llm.context_window == 0 {
        return Err(ConfigError::Validation(
            "agent.llm.context_window must be > 0".into(),
        ));
    }
    if cfg.agent.llm.timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "agent.llm.timeout_secs must be > 0".into(),
        ));
    }
    if cfg.agent.llm.retry_count > 0 && cfg.agent.llm.retry_base_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "agent.llm.retry_base_delay_ms must be > 0 when retry_count > 0".into(),
        ));
    }
    if cfg.agent.llm.retry_max_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "agent.llm.retry_max_delay_ms must be > 0".into(),
        ));
    }
    if cfg.clients.http.timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "clients.http.timeout_secs must be > 0".into(),
        ));
    }
    if cfg.clients.http.retry_count > 0 && cfg.clients.http.retry_base_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "clients.http.retry_base_delay_ms must be > 0 when retry_count > 0".into(),
        ));
    }
    if cfg.clients.http.retry_max_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "clients.http.retry_max_delay_ms must be > 0".into(),
        ));
    }
    if cfg.clients.router.query_timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "clients.router.query_timeout_secs must be > 0".into(),
        ));
    }
    if cfg.maintainer.auth.admin.enabled {
        if cfg.maintainer.auth.admin.username.trim().is_empty() {
            return Err(ConfigError::Validation(
                "maintainer.auth.admin.username must not be empty when enabled = true".into(),
            ));
        }
        if cfg.maintainer.auth.admin.password_env.trim().is_empty() {
            return Err(ConfigError::Validation(
                "maintainer.auth.admin.password_env must not be empty when enabled = true".into(),
            ));
        }
        if options.require_maintainer_admin_auth_password
            && cfg
                .maintainer
                .auth
                .admin
                .password
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(ConfigError::Validation(format!(
                "[maintainer.auth.admin].password_env 指定的环境变量 '{}' 未设置或为空",
                cfg.maintainer.auth.admin.password_env
            )));
        }
    }
    if cfg.agent.session.memory_review.interval_turns == 0 {
        return Err(ConfigError::Validation(
            "agent.session.memory_review.interval_turns must be > 0".into(),
        ));
    }
    if cfg.agent.session.subagents.max_concurrent == 0 {
        return Err(ConfigError::Validation(
            "agent.session.subagents.max_concurrent must be > 0".into(),
        ));
    }
    if cfg.agent.session.subagents.max_tool_loop_turns == 0 {
        return Err(ConfigError::Validation(
            "agent.session.subagents.max_tool_loop_turns must be > 0".into(),
        ));
    }
    if cfg.agent.session.subagents.wall_timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "agent.session.subagents.wall_timeout_secs must be > 0".into(),
        ));
    }
    let wait = &cfg.agent.session.subagents.wait;
    if wait.min_timeout_secs == 0
        || wait.default_timeout_secs < wait.min_timeout_secs
        || wait.default_timeout_secs > wait.max_timeout_secs
    {
        return Err(ConfigError::Validation(
            "agent.session.subagents.wait must satisfy 0 < min_timeout_secs <= default_timeout_secs <= max_timeout_secs".into(),
        ));
    }
    if cfg.agent.session.turn_journal.delta_snapshot_interval_ms == 0 {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.delta_snapshot_interval_ms must be > 0".into(),
        ));
    }
    if cfg.agent.session.turn_journal.delta_snapshot_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.delta_snapshot_chars must be > 0".into(),
        ));
    }
    if cfg
        .agent
        .session
        .turn_journal
        .recovery_original_user_request_max_chars
        == 0
    {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.recovery_original_user_request_max_chars must be > 0"
                .into(),
        ));
    }
    if cfg
        .agent
        .session
        .turn_journal
        .recovery_partial_assistant_max_chars
        == 0
    {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.recovery_partial_assistant_max_chars must be > 0".into(),
        ));
    }
    if cfg.agent.session.turn_journal.recovery_tool_input_max_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.recovery_tool_input_max_chars must be > 0".into(),
        ));
    }
    if cfg
        .agent
        .session
        .turn_journal
        .recovery_tool_output_max_chars
        == 0
    {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.recovery_tool_output_max_chars must be > 0".into(),
        ));
    }
    if cfg.agent.session.turn_journal.recovery_user_steer_max_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.session.turn_journal.recovery_user_steer_max_chars must be > 0".into(),
        ));
    }
    let live_response_preview_max_lines = cfg.agent.session.tui.live_response_preview_max_lines;
    if live_response_preview_max_lines != AUTO_LIVE_RESPONSE_PREVIEW_MAX_LINES
        && live_response_preview_max_lines < MIN_LIVE_RESPONSE_PREVIEW_MAX_LINES
    {
        return Err(ConfigError::Validation(
            format!(
                "agent.session.tui.live_response_preview_max_lines must be -1 (auto) or >= {MIN_LIVE_RESPONSE_PREVIEW_MAX_LINES}"
            ),
        ));
    }
    if cfg.agent.attachment.max_file_bytes == 0 {
        return Err(ConfigError::Validation(
            "agent.attachment.max_file_bytes must be > 0".into(),
        ));
    }
    if cfg.agent.attachment.max_files_per_turn == 0 {
        return Err(ConfigError::Validation(
            "agent.attachment.max_files_per_turn must be > 0".into(),
        ));
    }
    if cfg.router.embedding.max_concurrency == 0 {
        return Err(ConfigError::Validation(
            "router.embedding.max_concurrency must be > 0".into(),
        ));
    }
    if cfg.router.embedding.timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "router.embedding.timeout_secs must be > 0".into(),
        ));
    }
    if cfg.router.rerank.timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "router.rerank.timeout_secs must be > 0".into(),
        ));
    }
    if cfg.router.rerank.max_tokens == 0 {
        return Err(ConfigError::Validation(
            "router.rerank.max_tokens must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.lexical_top_n == 0 {
        return Err(ConfigError::Validation(
            "router.retrieval.lexical_top_n must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.vector_top_m == 0 {
        return Err(ConfigError::Validation(
            "router.retrieval.vector_top_m must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.top_k == 0 {
        return Err(ConfigError::Validation(
            "router.retrieval.top_k must be > 0".into(),
        ));
    }
    if cfg.router.rerank.retry_count > 0 && cfg.router.rerank.retry_base_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "router.rerank.retry_base_delay_ms must be > 0 when retry_count > 0".into(),
        ));
    }
    if cfg.router.rerank.retry_max_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "router.rerank.retry_max_delay_ms must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.vector.worker_poll_secs == 0 {
        return Err(ConfigError::Validation(
            "router.retrieval.vector.worker_poll_secs must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.vector.query_timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "router.retrieval.vector.query_timeout_secs must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.vector.retry_base_delay_ms == 0 {
        return Err(ConfigError::Validation(
            "router.retrieval.vector.retry_base_delay_ms must be > 0".into(),
        ));
    }
    if cfg.router.retrieval.vector.retry_max_delay_ms
        < cfg.router.retrieval.vector.retry_base_delay_ms
    {
        return Err(ConfigError::Validation(
            "router.retrieval.vector.retry_max_delay_ms must be >= retry_base_delay_ms".into(),
        ));
    }
    if cfg.router.refresh_interval_secs == 0 {
        return Err(ConfigError::Validation(
            "router.refresh_interval_secs must be > 0".into(),
        ));
    }
    if cfg.maintainer.sweep.tick_interval_secs == 0 {
        return Err(ConfigError::Validation(
            "maintainer.sweep.tick_interval_secs must be > 0".into(),
        ));
    }
    if cfg.maintainer.sweep.deprecated_after_days < cfg.maintainer.sweep.stale_after_days {
        return Err(ConfigError::Validation(
            "maintainer.sweep.deprecated_after_days must be >= maintainer.sweep.stale_after_days"
                .into(),
        ));
    }
    if cfg.maintainer.history.max_file_bytes == 0 {
        return Err(ConfigError::Validation(
            "maintainer.history.max_file_bytes must be > 0".into(),
        ));
    }
    if cfg.maintainer.history.backup_count == 0 {
        return Err(ConfigError::Validation(
            "maintainer.history.backup_count must be > 0".into(),
        ));
    }
    if cfg.agent.session.compaction.summary_max_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.session.compaction.summary_max_chars must be > 0".into(),
        ));
    }
    if cfg.agent.session.cleanup_retention_days > MAX_SESSION_CLEANUP_RETENTION_DAYS {
        return Err(ConfigError::Validation(format!(
            "agent.session.cleanup_retention_days must be <= {MAX_SESSION_CLEANUP_RETENTION_DAYS}"
        )));
    }
    validate_session_compaction_config(&cfg.agent.session.compaction, "agent.session.compaction")?;
    if let Some(compaction) = &cfg.agent.session.subagents.compaction {
        validate_session_compaction_config(compaction, "agent.session.subagents.compaction")?;
    }
    if cfg.agent.session.user_shell.timeout_secs == 0 {
        return Err(ConfigError::Validation(
            "agent.session.user_shell.timeout_secs must be > 0".into(),
        ));
    }
    if cfg.agent.session.user_shell.max_output_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.session.user_shell.max_output_chars must be > 0".into(),
        ));
    }
    if !user_shell_shell_is_supported(&cfg.agent.session.user_shell.shell) {
        return Err(ConfigError::Validation(format!(
            "agent.session.user_shell.shell must be auto, sh, bash, zsh, pwsh, powershell, cmd, or an absolute path: {}",
            cfg.agent.session.user_shell.shell
        )));
    }
    if cfg.agent.inbox.processing_stale_after_secs == 0 {
        return Err(ConfigError::Validation(
            "agent.inbox.processing_stale_after_secs must be > 0".into(),
        ));
    }
    if cfg.agent.tool.file_read_max_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.file_read_max_chars must be > 0".into(),
        ));
    }
    if cfg.agent.tool.file_diff_max_changed_lines == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.file_diff_max_changed_lines must be > 0".into(),
        ));
    }
    if cfg.agent.tool.max_parallel_tool_calls == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.max_parallel_tool_calls must be > 0".into(),
        ));
    }
    if cfg.agent.tool.code_run_initial_yield_ms == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.code_run_initial_yield_ms must be > 0".into(),
        ));
    }
    if cfg.agent.tool.code_run_min_yield_ms == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.code_run_min_yield_ms must be > 0".into(),
        ));
    }
    if cfg.agent.tool.code_run_max_yield_ms > MAX_CODE_RUN_MAX_YIELD_MS {
        return Err(ConfigError::Validation(format!(
            "agent.tool.code_run_max_yield_ms must be <= {MAX_CODE_RUN_MAX_YIELD_MS}"
        )));
    }
    if cfg.agent.tool.code_run_max_yield_ms < cfg.agent.tool.code_run_min_yield_ms {
        return Err(ConfigError::Validation(
            "agent.tool.code_run_max_yield_ms must be >= agent.tool.code_run_min_yield_ms".into(),
        ));
    }
    if cfg.agent.tool.code_run_initial_yield_ms < cfg.agent.tool.code_run_min_yield_ms
        || cfg.agent.tool.code_run_initial_yield_ms > cfg.agent.tool.code_run_max_yield_ms
    {
        return Err(ConfigError::Validation(
            "agent.tool.code_run_initial_yield_ms must be within code_run_min_yield_ms..=code_run_max_yield_ms".into(),
        ));
    }
    if cfg.agent.tool.code_run_write_yield_ms < cfg.agent.tool.code_run_min_yield_ms
        || cfg.agent.tool.code_run_write_yield_ms > cfg.agent.tool.code_run_max_yield_ms
    {
        return Err(ConfigError::Validation(
            "agent.tool.code_run_write_yield_ms must be within code_run_min_yield_ms..=code_run_max_yield_ms".into(),
        ));
    }
    if cfg.agent.tool.code_run_poll_yield_ms < cfg.agent.tool.code_run_min_yield_ms
        || cfg.agent.tool.code_run_poll_yield_ms > cfg.agent.tool.write_stdin_max_poll_timeout_ms
    {
        return Err(ConfigError::Validation(
            "internal code_run_poll_yield_ms must be within code_run_min_yield_ms..=write_stdin_max_poll_timeout_ms".into(),
        ));
    }
    if cfg.agent.tool.write_stdin_max_poll_timeout_ms < cfg.agent.tool.code_run_max_yield_ms {
        return Err(ConfigError::Validation(
            "agent.tool.write_stdin_max_poll_timeout_ms must be >= agent.tool.code_run_max_yield_ms"
                .into(),
        ));
    }
    if cfg.agent.tool.write_stdin_max_poll_timeout_ms > MAX_WRITE_STDIN_MAX_POLL_TIMEOUT_MS {
        return Err(ConfigError::Validation(format!(
            "agent.tool.write_stdin_max_poll_timeout_ms must be <= {MAX_WRITE_STDIN_MAX_POLL_TIMEOUT_MS}"
        )));
    }
    if cfg.agent.tool.code_run_max_output_chars == 0
        || cfg.agent.tool.code_run_max_output_chars > MAX_CODE_RUN_MAX_OUTPUT_CHARS
    {
        return Err(ConfigError::Validation(format!(
            "agent.tool.code_run_max_output_chars must be within 1..={MAX_CODE_RUN_MAX_OUTPUT_CHARS}"
        )));
    }
    if cfg.agent.tool.background_process_output_buffer_bytes == 0
        || cfg.agent.tool.background_process_output_buffer_bytes
            > MAX_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES
    {
        return Err(ConfigError::Validation(format!(
            "agent.tool.background_process_output_buffer_bytes must be within 1..={MAX_BACKGROUND_PROCESS_OUTPUT_BUFFER_BYTES}"
        )));
    }
    if cfg.agent.tool.background_process_max_entries_per_owner == 0
        || cfg.agent.tool.background_process_max_entries_per_owner
            > MAX_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER
    {
        return Err(ConfigError::Validation(format!(
            "agent.tool.background_process_max_entries_per_owner must be within 1..={MAX_BACKGROUND_PROCESS_MAX_ENTRIES_PER_OWNER}"
        )));
    }
    if cfg.agent.tool.background_process_protected_recent_entries
        > cfg.agent.tool.background_process_max_entries_per_owner
    {
        return Err(ConfigError::Validation(
            "agent.tool.background_process_protected_recent_entries must be <= agent.tool.background_process_max_entries_per_owner"
                .into(),
        ));
    }
    if cfg.agent.tool.background_process_pty_rows == 0
        || cfg.agent.tool.background_process_pty_cols == 0
        || cfg.agent.tool.background_process_pty_rows > MAX_BACKGROUND_PROCESS_PTY_DIMENSION
        || cfg.agent.tool.background_process_pty_cols > MAX_BACKGROUND_PROCESS_PTY_DIMENSION
    {
        return Err(ConfigError::Validation(format!(
            "agent.tool.background_process_pty_rows and background_process_pty_cols must be within 1..={MAX_BACKGROUND_PROCESS_PTY_DIMENSION}"
        )));
    }
    if cfg.agent.tool.background_process_pty_input_buffer_bytes == 0
        || cfg.agent.tool.background_process_pty_input_buffer_bytes
            > MAX_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES
        || cfg.agent.tool.background_process_pty_input_buffer_bytes
            > tokio::sync::Semaphore::MAX_PERMITS
    {
        return Err(ConfigError::Validation(format!(
            "agent.tool.background_process_pty_input_buffer_bytes must be within 1..={MAX_BACKGROUND_PROCESS_PTY_INPUT_BUFFER_BYTES}",
        )));
    }
    if cfg.agent.tool.background_process_output_drain_grace_ms == 0
        || cfg.agent.tool.background_process_output_drain_grace_ms
            > MAX_BACKGROUND_PROCESS_OUTPUT_DRAIN_GRACE_MS
    {
        return Err(ConfigError::Validation(format!(
            "agent.tool.background_process_output_drain_grace_ms must be within 1..={MAX_BACKGROUND_PROCESS_OUTPUT_DRAIN_GRACE_MS}"
        )));
    }
    if cfg.agent.tool.web.lookup_max_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.web.lookup_max_chars must be > 0".into(),
        ));
    }
    if cfg.agent.tool.web.max_count == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.web.max_count must be > 0".into(),
        ));
    }
    if cfg.agent.tool.web.max_content_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.web.max_content_chars must be > 0".into(),
        ));
    }
    if cfg.agent.tool.web.max_total_chars == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.web.max_total_chars must be > 0".into(),
        ));
    }
    if cfg.agent.tool.web.endpoint.trim().is_empty() {
        return Err(ConfigError::Validation(
            "agent.tool.web.endpoint must not be empty".into(),
        ));
    }
    if cfg.agent.tool.web.api_key_env.trim().is_empty() {
        return Err(ConfigError::Validation(
            "agent.tool.web.api_key_env must not be empty".into(),
        ));
    }
    if cfg.agent.tool.session_search_default_limit == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.session_search_default_limit must be > 0".into(),
        ));
    }
    if cfg.agent.tool.session_search_max_limit == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.session_search_max_limit must be > 0".into(),
        ));
    }
    if cfg.agent.tool.session_search_default_limit > cfg.agent.tool.session_search_max_limit {
        return Err(ConfigError::Validation(
            "agent.tool.session_search_default_limit must be <= agent.tool.session_search_max_limit"
                .into(),
        ));
    }
    if cfg.agent.tool.session_search_sqlite_busy_timeout_ms == 0 {
        return Err(ConfigError::Validation(
            "agent.tool.session_search_sqlite_busy_timeout_ms must be > 0".into(),
        ));
    }
    if cfg.agent.memory.memory_char_limit == 0 {
        return Err(ConfigError::Validation(
            "agent.memory.memory_char_limit must be > 0".into(),
        ));
    }
    if cfg.agent.memory.user_char_limit == 0 {
        return Err(ConfigError::Validation(
            "agent.memory.user_char_limit must be > 0".into(),
        ));
    }
    if cfg.agent.session.skills.max_body_bytes == 0 {
        return Err(ConfigError::Validation(
            "agent.session.skills.max_body_bytes must be > 0".into(),
        ));
    }
    if cfg.agent.session.skills.max_per_turn == 0 {
        return Err(ConfigError::Validation(
            "agent.session.skills.max_per_turn must be > 0".into(),
        ));
    }
    Ok(())
}

fn validate_session_compaction_config(
    compaction: &SessionCompactionConfig,
    prefix: &str,
) -> Result<(), ConfigError> {
    if !compaction.auto_compact_ctx_ratio.is_finite()
        || !(0.0..=1.0).contains(&compaction.auto_compact_ctx_ratio)
    {
        return Err(ConfigError::Validation(format!(
            "{prefix}.auto_compact_ctx_ratio must be between 0.0 and 1.0"
        )));
    }
    if !compaction.tail_target_ctx_ratio.is_finite()
        || !(0.0..=1.0).contains(&compaction.tail_target_ctx_ratio)
        || compaction.tail_target_ctx_ratio == 0.0
    {
        return Err(ConfigError::Validation(format!(
            "{prefix}.tail_target_ctx_ratio must be > 0.0 and <= 1.0"
        )));
    }
    if !compaction.tail_hard_ctx_ratio.is_finite()
        || !(0.0..=1.0).contains(&compaction.tail_hard_ctx_ratio)
        || compaction.tail_hard_ctx_ratio == 0.0
    {
        return Err(ConfigError::Validation(format!(
            "{prefix}.tail_hard_ctx_ratio must be > 0.0 and <= 1.0"
        )));
    }
    if compaction.tail_target_ctx_ratio > compaction.tail_hard_ctx_ratio {
        return Err(ConfigError::Validation(format!(
            "{prefix}.tail_target_ctx_ratio must be <= tail_hard_ctx_ratio"
        )));
    }
    if compaction.tail_previous_real_user_turns == 0 || compaction.tail_previous_real_user_turns > 5
    {
        return Err(ConfigError::Validation(format!(
            "{prefix}.tail_previous_real_user_turns must be between 1 and 5"
        )));
    }
    if compaction.tool_result_raw_max_chars == 0 {
        return Err(ConfigError::Validation(format!(
            "{prefix}.tool_result_raw_max_chars must be > 0"
        )));
    }
    Ok(())
}

pub fn validate_upstream_name(name: &str) -> Result<(), ConfigError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(ConfigError::Validation(
            "upstreams table name must not be empty".into(),
        ));
    }
    if is_reserved_upstream_name(name) {
        return Err(ConfigError::Validation(format!(
            "upstream '{name}' is reserved and cannot be used as a runtime directory"
        )));
    }
    if name == "." || name == ".." {
        return Err(ConfigError::Validation(format!(
            "upstream '{name}' cannot be a relative path segment"
        )));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_')
    {
        return Err(ConfigError::Validation(format!(
            "upstream '{name}' must contain only lowercase ascii letters, digits, '-' or '_'"
        )));
    }
    Ok(())
}

fn is_reserved_upstream_name(name: &str) -> bool {
    RESERVED_UPSTREAM_NAMES_RAW
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .any(|reserved| reserved == name)
}

fn resolve_upstream_secret(upstream: &UpstreamConfig) -> Option<String> {
    let env_name = upstream
        .acn_key_env
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    let env_name = env_name?;
    Some(
        std::env::var(env_name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_default(),
    )
}

fn user_shell_shell_is_supported(shell: &str) -> bool {
    let shell = shell.trim();
    matches!(
        shell,
        "auto" | "sh" | "bash" | "zsh" | "pwsh" | "powershell" | "cmd"
    ) || Path::new(shell).is_absolute()
}

fn config_path_for_error(path: Option<&Path>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_else(|| "<inline>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};
    use tempfile::tempdir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    const LLM_ENV_KEYS: &[&str] = &[
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_ENDPOINT",
        "CUSTOM_ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "EXAMPLE_LLM_API_KEY",
        "LLM_API_KEY",
        "LLM_ENDPOINT",
        "MODEL_NAME",
        "ACN_MAINTAINER_ADMIN_PASSWORD",
    ];
    const CONFIG_BOOTSTRAP_ENV_KEYS: &[&str] = &[
        "ACN_CONFIG",
        "HOME",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_ENDPOINT",
        "CUSTOM_ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "EXAMPLE_LLM_API_KEY",
        "LLM_API_KEY",
        "LLM_ENDPOINT",
        "MODEL_NAME",
        "ACN_LLM_API_KEY",
        "GLM_API_KEY",
        "ACN_MAINTAINER_ADMIN_PASSWORD",
        "DEMO_ACN_AUTH_KEY",
    ];
    const UPSTREAM_ENV_KEYS: &[&str] = &["DEMO_ACN_AUTH_KEY"];

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn clean(keys: &'static [&'static str]) -> Self {
            let lock = ENV_LOCK.lock().unwrap();
            let saved = keys
                .iter()
                .map(|key| (*key, std::env::var(key).ok()))
                .collect::<Vec<_>>();
            for key in keys {
                std::env::remove_var(key);
            }
            Self { saved, _lock: lock }
        }

        fn set(&self, key: &str, value: &str) {
            std::env::set_var(key, value);
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    fn parse_and_validate(raw: &str) -> Result<Config, ConfigError> {
        let mut cfg: Config = toml::from_str(raw).map_err(ConfigError::from)?;
        cfg.agent.llm.api_key = Some("test-api-key".to_string());
        cfg.normalize();
        validate_config(
            &cfg,
            None,
            ConfigLoadOptions {
                require_agent_llm_api_key: true,
                require_maintainer_admin_auth_password: true,
                validate_upstreams: true,
                ensure_storage_dirs: true,
            },
        )?;
        Ok(cfg)
    }

    fn minimal_config_without_optional_defaults() -> &'static str {
        r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

[storage]
acn_home = "./data"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "example-anthropic-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000
"#
    }

    fn minimal_daemon_config_without_upstreams() -> String {
        minimal_config_without_optional_defaults().replace(
            r#"upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

"#,
            "",
        )
    }

    fn expect_parse_err_contains(raw: String, expected: &str) {
        let err = parse_and_validate(&raw).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected {expected} in error: {err}"
        );
    }

    #[test]
    fn attachment_config_defaults_when_section_absent() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        assert_eq!(upstream.name, "dev");
        assert_eq!(upstream.agent_id.as_str(), "agent-a");
        assert!(cfg.agent.attachment.enabled);
        assert!(cfg.agent.attachment.clipboard_image_enabled);
        assert_eq!(
            cfg.agent.attachment.max_file_bytes,
            DEFAULT_ATTACHMENT_MAX_FILE_BYTES
        );
        assert_eq!(
            cfg.agent.attachment.max_files_per_turn,
            DEFAULT_ATTACHMENT_MAX_FILES_PER_TURN
        );
    }

    #[test]
    fn reasoning_effort_defaults_to_none_when_omitted() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();

        assert_eq!(cfg.agent.llm.reasoning_effort, ReasoningEffort::None);
    }

    #[test]
    fn openai_responses_provider_is_accepted() {
        let raw = minimal_config_without_optional_defaults().replace(
            r#"provider = "anthropic""#,
            r#"provider = "openai_responses""#,
        );

        let cfg = parse_and_validate(&raw).unwrap();

        assert_eq!(cfg.agent.llm.provider, LlmProvider::OpenAiResponses);
    }

    #[test]
    fn legacy_agent_provider_names_are_accepted_as_hidden_aliases() {
        for (legacy, expected) in [
            ("openai_compatible_chat", LlmProvider::OpenAiChat),
            ("openai_compatible_responses", LlmProvider::OpenAiResponses),
        ] {
            let raw = minimal_config_without_optional_defaults().replace(
                r#"provider = "anthropic""#,
                &format!(r#"provider = "{legacy}""#),
            );

            let cfg = parse_and_validate(&raw).unwrap();

            assert_eq!(cfg.agent.llm.provider, expected);
        }
    }

    #[test]
    fn agent_provider_names_serialize_canonically() {
        #[derive(Serialize)]
        struct ProviderConfig {
            provider: LlmProvider,
        }

        for (provider, canonical) in [
            (LlmProvider::Anthropic, "anthropic"),
            (LlmProvider::OpenAiChat, "openai_chat"),
            (LlmProvider::OpenAiResponses, "openai_responses"),
        ] {
            let serialized = toml::to_string(&ProviderConfig { provider }).unwrap();
            assert!(serialized.contains(&format!(r#"provider = "{canonical}""#)));
        }
    }

    #[test]
    fn router_rerank_provider_names_and_hidden_aliases_are_accepted() {
        #[derive(Deserialize, Serialize)]
        struct ProviderConfig {
            provider: RerankProvider,
        }

        for (raw, expected, canonical) in [
            ("heuristic", RerankProvider::Heuristic, "heuristic"),
            ("openai_chat", RerankProvider::OpenAiChat, "openai_chat"),
            (
                "openai_responses",
                RerankProvider::OpenAiResponses,
                "openai_responses",
            ),
            (
                "openai_compatible_chat",
                RerankProvider::OpenAiChat,
                "openai_chat",
            ),
            (
                "openai_compatible_responses",
                RerankProvider::OpenAiResponses,
                "openai_responses",
            ),
        ] {
            let parsed: ProviderConfig = toml::from_str(&format!(r#"provider = "{raw}""#)).unwrap();
            assert_eq!(parsed.provider, expected);
            assert!(toml::to_string(&parsed)
                .unwrap()
                .contains(&format!(r#"provider = "{canonical}""#)));
        }
    }

    #[test]
    fn router_rerank_rejects_reasoning_effort_parameter() {
        expect_parse_err_contains(
            format!(
                "{}\n[router.rerank]\nreasoning_effort = \"low\"\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `reasoning_effort`",
        );
    }

    #[test]
    fn reasoning_effort_accepts_supported_values_and_rejects_unknown_value() {
        for (raw, expected) in [
            ("none", ReasoningEffort::None),
            ("low", ReasoningEffort::Low),
            ("medium", ReasoningEffort::Medium),
            ("high", ReasoningEffort::High),
            ("xhigh", ReasoningEffort::Xhigh),
            ("max", ReasoningEffort::Max),
        ] {
            let config = minimal_config_without_optional_defaults().replace(
                r#"model = "example-anthropic-model""#,
                &format!(
                    r#"model = "example-anthropic-model"
reasoning_effort = "{raw}""#
                ),
            );
            let cfg = parse_and_validate(&config).unwrap();
            assert_eq!(cfg.agent.llm.reasoning_effort, expected);
        }

        expect_parse_err_contains(
            minimal_config_without_optional_defaults().replace(
                r#"model = "example-anthropic-model""#,
                r#"model = "example-anthropic-model"
reasoning_effort = "minimal""#,
            ),
            "unknown variant `minimal`",
        );
        expect_parse_err_contains(
            minimal_config_without_optional_defaults().replace(
                r#"model = "example-anthropic-model""#,
                r#"model = "example-anthropic-model"
reasoning_effort = "extreme""#,
            ),
            "unknown variant `extreme`",
        );
    }

    #[test]
    fn tool_parallel_call_limit_defaults_and_accepts_override() {
        let defaults = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            defaults.agent.tool.max_parallel_tool_calls,
            DEFAULT_MAX_PARALLEL_TOOL_CALLS
        );

        let configured = parse_and_validate(&format!(
            "{}\n[agent.tool]\nmax_parallel_tool_calls = 3\n",
            minimal_config_without_optional_defaults()
        ))
        .unwrap();
        assert_eq!(configured.agent.tool.max_parallel_tool_calls, 3);
    }

    #[test]
    fn web_config_defaults_parses_and_validates() {
        let defaults = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            defaults.agent.tool.web.max_count,
            DEFAULT_WEB_SEARCH_MAX_COUNT
        );
        assert_eq!(
            defaults.agent.tool.web.lookup_max_chars,
            DEFAULT_WEB_LOOKUP_MAX_CHARS
        );
        assert_eq!(
            defaults.agent.tool.web.max_content_chars,
            DEFAULT_WEB_SEARCH_MAX_CONTENT_CHARS
        );
        assert_eq!(
            defaults.agent.tool.web.max_total_chars,
            DEFAULT_WEB_SEARCH_MAX_TOTAL_CHARS
        );

        let configured = parse_and_validate(&format!(
            "{}\n[agent.tool.web]\nlookup_max_chars = 400\nmax_count = 4\nmax_content_chars = 300\nmax_total_chars = 1000\n",
            minimal_config_without_optional_defaults()
        ))
        .unwrap();
        assert_eq!(configured.agent.tool.web.lookup_max_chars, 400);
        assert_eq!(configured.agent.tool.web.max_count, 4);
        assert_eq!(configured.agent.tool.web.max_content_chars, 300);
        assert_eq!(configured.agent.tool.web.max_total_chars, 1000);

        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool.web]\nengine = \"search_std\"\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `engine`",
        );

        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool.web]\nmax_count = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.tool.web.max_count must be > 0",
        );

        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool.web_search]\nmax_count = 4\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `web_search`",
        );

        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool]\nweb_lookup_max_chars = 400\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `web_lookup_max_chars`",
        );

        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool.web]\nuser_id = \"example_user\"\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `user_id`",
        );
    }

    #[test]
    fn llm_max_tokens_defaults_when_omitted() {
        let raw = minimal_config_without_optional_defaults().replace("max_tokens = 4096\n", "");
        let cfg = parse_and_validate(&raw).unwrap();

        assert_eq!(cfg.agent.llm.max_tokens, DEFAULT_LLM_MAX_TOKENS);
        assert_eq!(LlmChatConfig::default().max_tokens, DEFAULT_LLM_MAX_TOKENS);
    }

    #[test]
    fn background_shell_config_only_exposes_output_and_write_stdin_poll_timeout() {
        let defaults = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            defaults.agent.tool.code_run_max_output_chars,
            DEFAULT_CODE_RUN_MAX_OUTPUT_CHARS
        );
        assert_eq!(
            defaults.agent.tool.write_stdin_max_poll_timeout_ms,
            DEFAULT_WRITE_STDIN_MAX_POLL_TIMEOUT_MS
        );

        let configured = parse_and_validate(&format!(
            "{}\n[agent.tool]\ncode_run_max_output_chars = 2048\nwrite_stdin_max_poll_timeout_ms = 30000\n",
            minimal_config_without_optional_defaults()
        ))
        .unwrap();
        assert_eq!(configured.agent.tool.code_run_max_output_chars, 2048);
        assert_eq!(
            configured.agent.tool.write_stdin_max_poll_timeout_ms,
            30_000
        );

        let serialized = toml::to_string(&configured.agent.tool).unwrap();
        assert!(serialized.contains("code_run_max_output_chars = 2048"));
        assert!(serialized.contains("write_stdin_max_poll_timeout_ms = 30000"));
        assert!(!serialized.contains("code_run_initial_yield_ms"));
        assert!(!serialized.contains("background_process_output_buffer_bytes"));

        let legacy_hidden_key = ["code_run_max_", "poll_yield_ms"].concat();
        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool]\n{legacy_hidden_key} = 300000\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool]\ncode_run_max_output_chars = {}\n",
                minimal_config_without_optional_defaults(),
                MAX_CODE_RUN_MAX_OUTPUT_CHARS + 1
            ),
            "agent.tool.code_run_max_output_chars must be within 1..=",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.tool]\nwrite_stdin_max_poll_timeout_ms = {}\n",
                minimal_config_without_optional_defaults(),
                MAX_WRITE_STDIN_MAX_POLL_TIMEOUT_MS + 1
            ),
            "agent.tool.write_stdin_max_poll_timeout_ms must be <=",
        );
    }

    #[test]
    fn attachment_config_parses_custom_values() {
        let raw = format!(
            "{}\n[agent.attachment]\nenabled = false\nclipboard_image_enabled = false\nmax_file_bytes = 1024\nmax_files_per_turn = 2\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();
        assert!(!cfg.agent.attachment.enabled);
        assert!(!cfg.agent.attachment.clipboard_image_enabled);
        assert_eq!(cfg.agent.attachment.max_file_bytes, 1024);
        assert_eq!(cfg.agent.attachment.max_files_per_turn, 2);
    }

    #[test]
    fn attachment_config_rejects_zero_limits() {
        expect_parse_err_contains(
            format!(
                "{}\n[agent.attachment]\nmax_file_bytes = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.attachment.max_file_bytes",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.attachment]\nmax_files_per_turn = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.attachment.max_files_per_turn",
        );
    }

    #[test]
    fn delegation_config_defaults_when_section_absent() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();

        assert_eq!(
            cfg.agent.session.subagents.max_concurrent,
            DEFAULT_SESSION_DELEGATION_MAX_CONCURRENT
        );
        assert_eq!(
            cfg.agent.session.subagents.max_tool_loop_turns,
            DEFAULT_SESSION_DELEGATION_MAX_TOOL_LOOP_TURNS
        );
        assert_eq!(
            cfg.agent.session.subagents.wall_timeout_secs,
            DEFAULT_SESSION_DELEGATION_WALL_TIMEOUT_SECS
        );
        assert_eq!(
            cfg.agent.session.subagents.wait.default_timeout_secs,
            DEFAULT_SESSION_DELEGATION_WAIT_DEFAULT_TIMEOUT_SECS
        );
        assert_eq!(
            cfg.agent.session.subagents.wait.min_timeout_secs,
            DEFAULT_SESSION_DELEGATION_WAIT_MIN_TIMEOUT_SECS
        );
        assert_eq!(
            cfg.agent.session.subagents.wait.max_timeout_secs,
            DEFAULT_SESSION_DELEGATION_WAIT_MAX_TIMEOUT_SECS
        );
        assert!(cfg.agent.session.subagents.compaction.is_none());
    }

    #[test]
    fn delegation_config_parses_custom_values() {
        let raw = format!(
            "{}\n[agent.session.subagents]\nmax_concurrent = 8\nmax_tool_loop_turns = 512\nwall_timeout_secs = 9000\n\n[agent.session.subagents.wait]\ndefault_timeout_secs = 40\nmin_timeout_secs = 12\nmax_timeout_secs = 600\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();

        assert_eq!(cfg.agent.session.subagents.max_concurrent, 8);
        assert_eq!(cfg.agent.session.subagents.max_tool_loop_turns, 512);
        assert_eq!(cfg.agent.session.subagents.wall_timeout_secs, 9000);
        assert_eq!(cfg.agent.session.subagents.wait.default_timeout_secs, 40);
        assert_eq!(cfg.agent.session.subagents.wait.min_timeout_secs, 12);
        assert_eq!(cfg.agent.session.subagents.wait.max_timeout_secs, 600);
    }

    #[test]
    fn delegation_config_parses_optional_compaction_override() {
        let raw = format!(
            "{}\n[agent.session.subagents.compaction]\nauto_compact_ctx_ratio = 0.5\ntail_target_ctx_ratio = 0.2\ntail_hard_ctx_ratio = 0.4\ntail_previous_real_user_turns = 2\ntool_result_raw_max_chars = 2048\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();
        let compaction = cfg
            .agent
            .session
            .subagents
            .compaction
            .expect("delegation compaction override");
        assert_eq!(compaction.auto_compact_ctx_ratio, 0.5);
        assert_eq!(compaction.tail_previous_real_user_turns, 2);
        assert_eq!(compaction.tool_result_raw_max_chars, 2048);
    }

    #[test]
    fn delegation_config_rejects_zero_values() {
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session.subagents]\nmax_concurrent = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.session.subagents.max_concurrent",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session.subagents]\nmax_tool_loop_turns = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.session.subagents.max_tool_loop_turns",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session.subagents]\nwall_timeout_secs = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.session.subagents.wall_timeout_secs",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session.subagents.wait]\ndefault_timeout_secs = 9\nmin_timeout_secs = 10\nmax_timeout_secs = 60\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.session.subagents.wait",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session.subagents.compaction]\ntail_previous_real_user_turns = 0\n",
                minimal_config_without_optional_defaults()
            ),
            "agent.session.subagents.compaction.tail_previous_real_user_turns",
        );
    }

    #[test]
    fn delegation_config_rejects_legacy_public_section_name() {
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session.delegation]\nmax_concurrent = 6\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `delegation`",
        );
    }

    #[test]
    fn upstream_config_resolves_default_and_cli_override() {
        let raw = format!(
            r#"{}

[upstreams.agent_hub]
agent_id = "agent-b"
maintainer_endpoint = "http://maintainer.example"
router_endpoint = "http://router.example"
"#,
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();

        let default_upstream = cfg.resolve_upstream(None).unwrap();
        let override_upstream = cfg.resolve_upstream(Some("agent_hub")).unwrap();

        assert_eq!(default_upstream.name, "dev");
        assert_eq!(default_upstream.agent_id.as_str(), "agent-a");
        assert_eq!(override_upstream.name, "agent_hub");
        assert_eq!(override_upstream.agent_id.as_str(), "agent-b");
        assert_eq!(
            override_upstream.maintainer_endpoint,
            "http://maintainer.example"
        );
        assert_eq!(override_upstream.router_endpoint, "http://router.example");
    }

    #[test]
    fn upstream_config_rejects_missing_default_and_placeholder_agent_id() {
        expect_parse_err_contains(
            minimal_config_without_optional_defaults()
                .replace(r#"upstream = "dev""#, r#"upstream = "missing""#),
            "upstream 'missing' not found",
        );

        let cfg = parse_and_validate(&minimal_config_without_optional_defaults().replace(
            r#"agent_id = "agent-a""#,
            r#"agent_id = "<your_agent_id_here>""#,
        ))
        .unwrap();
        let err = cfg.resolve_upstream(None).unwrap_err().to_string();

        assert!(err.contains("请在 [upstreams] 中填入你的 agent_id"));
    }

    #[test]
    fn upstream_config_supports_solo_mode_and_requires_endpoint_pairs() {
        expect_parse_err_contains(
            minimal_config_without_optional_defaults()
                .replace(r#"agent_id = "agent-a""#, r#"agent_id = """#),
            "upstreams.dev.agent_id must not be empty",
        );

        let pair_error =
            "upstreams.dev.maintainer_endpoint and router_endpoint must both be configured or both be empty";
        expect_parse_err_contains(
            minimal_config_without_optional_defaults().replace(
                r#"maintainer_endpoint = "http://127.0.0.1:8062""#,
                r#"maintainer_endpoint = """#,
            ),
            pair_error,
        );
        expect_parse_err_contains(
            minimal_config_without_optional_defaults().replace(
                r#"router_endpoint = "http://127.0.0.1:8061""#,
                r#"router_endpoint = """#,
            ),
            pair_error,
        );

        let raw = minimal_config_without_optional_defaults()
            .replace(
                r#"maintainer_endpoint = "http://127.0.0.1:8062"
"#,
                "",
            )
            .replace(
                r#"router_endpoint = "http://127.0.0.1:8061"
"#,
                "",
            );
        let cfg = parse_and_validate(&raw).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        assert!(!upstream.team_services_configured());
        assert!(upstream.maintainer_endpoint.is_empty());
        assert!(upstream.router_endpoint.is_empty());

        let raw = minimal_config_without_optional_defaults().replace(
            r#"router_endpoint = "http://127.0.0.1:8061""#,
            r#"router_endpoint = "http://127.0.0.1:8061"
acn_key_env = """#,
        );
        let cfg = parse_and_validate(&raw).unwrap();
        assert!(cfg
            .resolve_upstream(None)
            .unwrap()
            .team_services_configured());
        assert!(cfg.resolve_upstream(None).unwrap().acn_key.is_none());
    }

    #[test]
    fn upstream_default_can_be_omitted_when_cli_override_is_used() {
        let cfg = parse_and_validate(
            &minimal_config_without_optional_defaults().replace(r#"upstream = "dev""#, ""),
        )
        .unwrap();

        let err = cfg.resolve_upstream(None).unwrap_err().to_string();
        assert!(err.contains("use --upstream <name>"));

        let upstream = cfg.resolve_upstream(Some("dev")).unwrap();
        assert_eq!(upstream.name, "dev");
        assert_eq!(upstream.agent_id.as_str(), "agent-a");
    }

    #[test]
    fn upstream_acn_key_env_is_the_only_supported_key_source() {
        let env = EnvGuard::clean(UPSTREAM_ENV_KEYS);
        let raw = minimal_config_without_optional_defaults().replace(
            r#"router_endpoint = "http://127.0.0.1:8061""#,
            r#"router_endpoint = "http://127.0.0.1:8061"
acn_key_env = "DEMO_ACN_AUTH_KEY""#,
        );
        let missing_key = parse_and_validate(&raw)
            .unwrap()
            .resolve_upstream(None)
            .unwrap()
            .acn_key;
        assert_eq!(missing_key.as_deref(), Some(""));

        env.set("DEMO_ACN_AUTH_KEY", "team-secret");
        let raw = minimal_config_without_optional_defaults().replace(
            r#"router_endpoint = "http://127.0.0.1:8061""#,
            r#"router_endpoint = "http://127.0.0.1:8061"
acn_key_env = "DEMO_ACN_AUTH_KEY""#,
        );
        let cfg = parse_and_validate(&raw).unwrap();

        assert_eq!(
            cfg.resolve_upstream(None).unwrap().acn_key.as_deref(),
            Some("team-secret")
        );

        expect_parse_err_contains(
            raw.replace(
                r#"acn_key_env = "DEMO_ACN_AUTH_KEY""#,
                r#"api_key_env = "DEMO_ACN_AUTH_KEY""#,
            ),
            "unknown field `api_key_env`",
        );
        expect_parse_err_contains(
            raw.replace(
                r#"acn_key_env = "DEMO_ACN_AUTH_KEY""#,
                r#"api_key = "plain-dev-key""#,
            ),
            "unknown field `api_key`",
        );
        expect_parse_err_contains(
            raw.replace(
                r#"acn_key_env = "DEMO_ACN_AUTH_KEY""#,
                r#"acn_key = "plain-dev-key""#,
            ),
            "unknown field `acn_key`",
        );
    }

    #[test]
    fn upstream_name_rejects_reserved_runtime_directory() {
        expect_parse_err_contains(
            minimal_config_without_optional_defaults()
                .replace(r#"upstream = "dev""#, r#"upstream = "data""#)
                .replace("[upstreams.dev]", "[upstreams.data]"),
            "is reserved",
        );
    }

    fn openai_chat_config_raw() -> String {
        minimal_config_without_optional_defaults()
            .replace(r#"provider = "anthropic""#, r#"provider = "openai_chat""#)
            .replace(
                r#"model = "example-anthropic-model""#,
                r#"model = "example-chat-model""#,
            )
            .replace(
                r#"api_key_env = "PATH""#,
                r#"api_key_env = "EXAMPLE_LLM_API_KEY""#,
            )
    }

    fn openai_responses_config_raw() -> String {
        openai_chat_config_raw()
            .replace(
                r#"provider = "openai_chat""#,
                r#"provider = "openai_responses""#,
            )
            .replace(
                r#"model = "example-chat-model""#,
                r#"model = "example-responses-model""#,
            )
    }

    fn anthropic_config_raw() -> String {
        minimal_config_without_optional_defaults()
            .replace(
                r#"api_key_env = "PATH""#,
                r#"api_key_env = "CUSTOM_ANTHROPIC_API_KEY""#,
            )
            .replace(
                r#"endpoint = "https://api.anthropic.com""#,
                r#"endpoint = "https://llm.example.com/""#,
            )
    }

    fn write_config(raw: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let raw = raw.replace(
            r#"acn_home = "./data""#,
            &format!(r#"acn_home = "{}""#, dir.path().join("acn").display()),
        );
        std::fs::write(&path, raw).unwrap();
        (dir, path)
    }

    #[test]
    fn storage_acn_home_keeps_team_root_at_base_when_agent_runtime_activates() {
        let dir = tempdir().unwrap();
        let acn_home = dir.path().join("custom-acn");
        let raw = minimal_config_without_optional_defaults().replace(
            r#"acn_home = "./data""#,
            &format!(r#"acn_home = "{}""#, acn_home.display()),
        );
        let path = dir.path().join("config.toml");
        std::fs::write(&path, raw).unwrap();

        let mut cfg = Config::load(&path).unwrap();

        assert_eq!(cfg.storage.acn_home, acn_home);
        assert_eq!(cfg.storage.team_root, acn_home.join("data").join("team"));
        assert_eq!(
            cfg.storage.agents_root,
            acn_home.join("data").join("agents")
        );
        assert!(cfg.storage.skills_root().is_dir());
        assert!(cfg.storage.team_root.is_dir());
        assert!(cfg.storage.agents_root.is_dir());
        assert!(!cfg.storage.acn_md_path().exists());

        let upstream = cfg.resolve_upstream(None).unwrap();
        cfg.activate_upstream_runtime(&upstream).unwrap();
        assert_eq!(cfg.storage.acn_home, acn_home.join("dev"));
        assert_eq!(cfg.storage.team_root, acn_home.join("data").join("team"));
        assert_eq!(
            cfg.agent_home(&upstream.agent_id),
            acn_home
                .join("dev")
                .join("data")
                .join("agents")
                .join("agent-a")
        );
        assert!(cfg.storage.skills_root().is_dir());
        assert!(cfg.storage.team_root.is_dir());
        assert!(cfg.storage.agents_root.is_dir());
        assert!(!acn_home.join("dev").join("data").join("team").exists());
    }

    #[test]
    fn upstream_runtime_root_uses_immutable_base_after_activation() {
        let dir = tempdir().unwrap();
        let acn_home = dir.path().join("custom-acn");
        let raw = format!(
            r#"{}

[upstreams.agent_hub]
agent_id = "agent-b"
maintainer_endpoint = "http://maintainer.example"
router_endpoint = "http://router.example"
"#,
            minimal_config_without_optional_defaults()
        )
        .replace(
            r#"acn_home = "./data""#,
            &format!(r#"acn_home = "{}""#, acn_home.display()),
        );
        let path = dir.path().join("config.toml");
        std::fs::write(&path, raw).unwrap();

        let mut cfg = Config::load(&path).unwrap();
        let dev = cfg.resolve_upstream(Some("dev")).unwrap();
        cfg.activate_upstream_runtime(&dev).unwrap();
        assert_eq!(cfg.storage.acn_home, acn_home.join("dev"));

        let agent_hub = cfg.resolve_upstream(Some("agent_hub")).unwrap();
        assert_eq!(agent_hub.runtime_acn_home, acn_home.join("agent_hub"));
        cfg.activate_upstream_runtime(&agent_hub).unwrap();

        assert_eq!(cfg.storage.acn_home, acn_home.join("agent_hub"));
        assert_eq!(
            cfg.agent_home(&agent_hub.agent_id),
            acn_home
                .join("agent_hub")
                .join("data")
                .join("agents")
                .join("agent-b")
        );
        assert!(!acn_home.join("dev").join("agent_hub").exists());
    }

    #[test]
    fn agent_load_defers_storage_dirs_until_upstream_runtime_activation() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (dir, path) = write_config(minimal_config_without_optional_defaults());
        let acn_home = dir.path().join("acn");

        let (mut cfg, used_path) = Config::load_or_init_for_agent(Some(&path)).unwrap();

        assert_eq!(used_path, path);
        assert_eq!(cfg.storage.acn_home, acn_home);
        assert!(!acn_home.join("skills").exists());
        assert!(!acn_home.join("data").exists());

        let upstream = cfg.resolve_upstream(None).unwrap();
        cfg.activate_upstream_runtime(&upstream).unwrap();

        assert!(!acn_home.join("skills").exists());
        assert!(!acn_home.join("data").exists());
        assert!(acn_home.join("dev").join("skills").is_dir());
        assert!(acn_home.join("dev").join("data").join("agents").is_dir());
        assert!(!acn_home.join("dev").join("data").join("team").exists());
    }

    #[test]
    fn agent_activation_removes_empty_legacy_upstream_team_directory() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (dir, path) = write_config(minimal_config_without_optional_defaults());
        let acn_home = dir.path().join("acn");
        let legacy_team_root = acn_home.join("dev").join("data").join("team");
        std::fs::create_dir_all(&legacy_team_root).unwrap();

        let (mut cfg, _) = Config::load_or_init_for_agent(Some(&path)).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        cfg.activate_upstream_runtime(&upstream).unwrap();

        assert!(!legacy_team_root.exists());
        assert!(acn_home.join("dev").join("data").join("agents").is_dir());
    }

    #[test]
    fn agent_activation_rejects_nonempty_legacy_upstream_team_storage() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (dir, path) = write_config(minimal_config_without_optional_defaults());
        let acn_home = dir.path().join("acn");
        let legacy_claim = acn_home
            .join("dev")
            .join("data")
            .join("team")
            .join("claim.yaml");
        std::fs::create_dir_all(legacy_claim.parent().unwrap()).unwrap();
        std::fs::write(&legacy_claim, "id: claim_1234abcd\n").unwrap();

        let (mut cfg, _) = Config::load_or_init_for_agent(Some(&path)).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        let err = cfg.activate_upstream_runtime(&upstream).unwrap_err();

        assert!(matches!(err, ConfigError::LegacyUpstreamTeamStorage { .. }));
        assert_eq!(cfg.storage.acn_home, acn_home);
        assert_eq!(
            std::fs::read_to_string(legacy_claim).unwrap(),
            "id: claim_1234abcd\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn agent_activation_rejects_symlinked_runtime_data_directory() {
        use std::os::unix::fs::symlink;

        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (dir, path) = write_config(minimal_config_without_optional_defaults());
        let acn_home = dir.path().join("acn");
        let daemon_team_root = acn_home.join("data").join("team");
        std::fs::create_dir_all(&daemon_team_root).unwrap();
        std::fs::create_dir_all(acn_home.join("dev")).unwrap();
        symlink(&daemon_team_root, acn_home.join("dev").join("data")).unwrap();

        let (mut cfg, _) = Config::load_or_init_for_agent(Some(&path)).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        let err = cfg.activate_upstream_runtime(&upstream).unwrap_err();

        assert!(matches!(err, ConfigError::AgentRuntimeSymlink { .. }));
        assert!(!daemon_team_root.join("agents").exists());
    }

    #[test]
    fn activate_upstream_runtime_does_not_auto_migrate_legacy_local_state() {
        let (dir, path) = write_config(minimal_config_without_optional_defaults());
        let acn_home = dir.path().join("acn");
        std::fs::create_dir_all(acn_home.join("skills").join("sample")).unwrap();
        std::fs::create_dir_all(
            acn_home
                .join("data")
                .join("agents")
                .join("agent-a")
                .join("sessions"),
        )
        .unwrap();
        std::fs::write(acn_home.join(".mcp.json"), "{\"mcpServers\":{}}\n").unwrap();
        std::fs::write(acn_home.join("ACN.md"), "legacy instructions\n").unwrap();
        std::fs::write(
            acn_home.join("skills").join("sample").join("SKILL.md"),
            "# sample\n",
        )
        .unwrap();
        std::fs::write(
            acn_home
                .join("data")
                .join("agents")
                .join("agent-a")
                .join("sessions")
                .join("session.yaml"),
            "id: session_1234abcd\n",
        )
        .unwrap();

        let (mut cfg, _used_path) = Config::load_or_init_for_agent(Some(&path)).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        cfg.activate_upstream_runtime(&upstream).unwrap();

        assert!(acn_home.join(".mcp.json").is_file());
        assert!(acn_home.join("ACN.md").is_file());
        assert!(acn_home
            .join("skills")
            .join("sample")
            .join("SKILL.md")
            .is_file());
        assert!(acn_home
            .join("data")
            .join("agents")
            .join("agent-a")
            .join("sessions")
            .join("session.yaml")
            .is_file());
        assert!(!acn_home.join("dev").join(".mcp.json").exists());
        assert!(!acn_home.join("dev").join("ACN.md").exists());
    }

    #[test]
    fn router_daemon_uses_base_storage_without_inspecting_agent_runtime() {
        let raw = minimal_daemon_config_without_upstreams();
        let (dir, path) = write_config(&raw);
        let acn_home = dir.path().join("acn");
        let agent_runtime_marker = acn_home
            .join("dev")
            .join("data")
            .join("team")
            .join("agent-runtime-marker");
        std::fs::create_dir_all(agent_runtime_marker.parent().unwrap()).unwrap();
        std::fs::write(&agent_runtime_marker, "untouched\n").unwrap();

        let (cfg, used_path) = Config::load_or_init_for_router(Some(&path)).unwrap();

        assert_eq!(used_path, path);
        assert_eq!(cfg.storage.acn_home, acn_home);
        assert!(acn_home.join("skills").is_dir());
        assert!(acn_home.join("data").join("team").is_dir());
        assert!(acn_home.join("data").join("agents").is_dir());
        assert_eq!(
            std::fs::read_to_string(agent_runtime_marker).unwrap(),
            "untouched\n"
        );
    }

    #[test]
    fn maintainer_daemon_load_uses_storage_acn_home_without_upstream() {
        let raw = minimal_daemon_config_without_upstreams();
        let (dir, path) = write_config(&raw);
        let acn_home = dir.path().join("acn");

        let (cfg, used_path) = Config::load_or_init_for_maintainer_daemon(Some(&path)).unwrap();

        assert_eq!(used_path, path);
        assert_eq!(cfg.storage.acn_home, acn_home);
        assert!(acn_home.join("skills").is_dir());
        assert!(acn_home.join("data").join("team").is_dir());
        assert!(acn_home.join("data").join("agents").is_dir());
    }

    #[test]
    fn team_auth_toggles_default_false_and_parse_true() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert!(!cfg.maintainer.auth.team.enabled);
        assert!(!cfg.router.auth.team.enabled);

        let raw = format!(
            "{}\n[maintainer.auth.team]\nenabled = true\n[router.auth.team]\nenabled = true\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();

        assert!(cfg.maintainer.auth.team.enabled);
        assert!(cfg.router.auth.team.enabled);
    }

    #[test]
    fn clients_rejects_legacy_service_key_fields() {
        expect_parse_err_contains(
            format!(
                "{}\n[clients]\napi_key_env = \"ROUTER_SERVICE_KEY\"\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `api_key_env`",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[clients]\napi_key = \"plain-dev-key\"\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `api_key`",
        );
    }

    #[test]
    fn config_rejects_top_level_auth_table() {
        expect_parse_err_contains(
            format!(
                "{}\n[auth]\nenabled = true\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `auth`",
        );
    }

    #[test]
    fn storage_dir_creation_error_mentions_failing_path() {
        let dir = tempdir().unwrap();
        let acn_home = dir.path().join("acn-file");
        std::fs::write(&acn_home, "not a directory").unwrap();
        let raw = minimal_config_without_optional_defaults().replace(
            r#"acn_home = "./data""#,
            &format!(r#"acn_home = "{}""#, acn_home.display()),
        );
        let path = dir.path().join("config.toml");
        std::fs::write(&path, raw).unwrap();

        let err = Config::load(&path).unwrap_err().to_string();

        assert!(err.contains("创建 ACN 存储目录失败"));
        assert!(err.contains(acn_home.to_string_lossy().as_ref()));
    }

    #[test]
    fn load_or_init_writes_default_config_and_rejects_empty_agent_id() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let home = tempdir().unwrap();
        env.set("HOME", home.path().to_str().unwrap());
        env.set("ACN_LLM_API_KEY", "example-key");
        let default_path = home.path().join(".acn").join("config.toml");

        let err = Config::load_or_init(None).unwrap_err().to_string();

        assert!(default_path.is_file());
        let written = std::fs::read_to_string(&default_path).unwrap();
        assert_eq!(written, include_str!("../config.template.toml"));
        assert!(err.contains("upstreams.default.agent_id must not be empty"));
        assert!(written.contains(r#"acn_home = "~/.acn""#));
        assert!(written.contains("\n[router.daemon]\n"));
        assert!(written.contains(r#"listen = "127.0.0.1:8061""#));
        assert!(written.contains("\n[maintainer.daemon]\n"));
        assert!(written.contains(r#"listen = "127.0.0.1:8062""#));
        assert!(written.contains("\n[maintainer.auth.admin]\n"));
        assert!(written.contains("\n[agent.tool.web]\n"));
        assert!(!written.contains("\n[maintainer.sweep]\n"));
        assert!(!written.contains("\n[clients.http]\n"));
        assert!(!written.contains("\n[agent.session]\n"));
        assert!(!written.contains("\n[agent.tool]\n"));
        assert!(!written.contains("\n[langfuse]\n"));
    }

    #[test]
    fn load_or_init_first_run_reports_empty_agent_id_before_missing_api_key() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let home = tempdir().unwrap();
        env.set("HOME", home.path().to_str().unwrap());
        let default_path = home.path().join(".acn").join("config.toml");

        let err = Config::load_or_init(None).unwrap_err().to_string();

        assert!(default_path.is_file());
        assert!(err.contains("upstreams.default.agent_id must not be empty"));
    }

    #[test]
    fn daemon_first_run_initializes_and_loads_shared_team_template() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let home = tempdir().unwrap();
        env.set("HOME", home.path().to_str().unwrap());
        let default_path = home.path().join(".acn").join("config.toml");

        let (router_cfg, router_path) = Config::load_or_init_for_router(None).unwrap();
        assert_eq!(router_path, default_path);
        assert_eq!(router_cfg.router.daemon.listen, DEFAULT_ROUTER_LISTEN);
        assert_eq!(
            std::fs::read_to_string(&default_path).unwrap(),
            include_str!("../config.template.toml")
        );

        let (maintainer_cfg, maintainer_path) =
            Config::load_or_init_for_maintainer_daemon(None).unwrap();
        assert_eq!(maintainer_path, default_path);
        assert_eq!(
            maintainer_cfg.maintainer.daemon.listen,
            DEFAULT_MAINTAINER_LISTEN
        );
    }

    #[test]
    fn default_config_template_loads_after_required_upstream_values_are_filled() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        env.set("ACN_LLM_API_KEY", "example-key");
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let raw = include_str!("../config.template.toml")
            .replace(r#"agent_id = """#, r#"agent_id = "agent-a""#)
            .replace(
                r#"maintainer_endpoint = """#,
                r#"maintainer_endpoint = "http://127.0.0.1:8062""#,
            )
            .replace(
                r#"router_endpoint = """#,
                r#"router_endpoint = "http://127.0.0.1:8061""#,
            )
            .replace(
                r#"acn_home = "~/.acn""#,
                &format!(r#"acn_home = "{}""#, dir.path().join("acn_home").display()),
            );

        std::fs::write(&config_path, raw).unwrap();
        let cfg = Config::load(&config_path).expect("filled default template should load");
        assert_eq!(cfg.upstream, "default");
        assert_eq!(cfg.agent.tool.web.max_count, DEFAULT_WEB_SEARCH_MAX_COUNT);
    }

    #[test]
    fn load_or_init_uses_acn_config_before_default_path() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let home = tempdir().unwrap();
        env.set("HOME", home.path().to_str().unwrap());
        let (_cfg_dir, cfg_path) = write_config(minimal_config_without_optional_defaults());
        env.set("ACN_CONFIG", cfg_path.to_str().unwrap());

        let (_cfg, used_path) = Config::load_or_init(None).unwrap();

        assert_eq!(used_path, cfg_path);
        assert!(!home.path().join(".acn").join("config.toml").exists());
    }

    #[test]
    fn prompt_table_is_not_accepted_from_toml() {
        expect_parse_err_contains(
            format!(
                "{}\n[prompt]\nroot = \"prompts\"\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `prompt`",
        );
    }

    #[test]
    fn load_or_init_prefers_explicit_path_over_acn_config() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let (_env_cfg_dir, env_cfg_path) = write_config(minimal_config_without_optional_defaults());
        let (_explicit_cfg_dir, explicit_cfg_path) =
            write_config(minimal_config_without_optional_defaults());
        env.set("ACN_CONFIG", env_cfg_path.to_str().unwrap());

        let (_cfg, used_path) = Config::load_or_init(Some(&explicit_cfg_path)).unwrap();

        assert_eq!(used_path, explicit_cfg_path);
    }

    #[test]
    fn resolve_workspace_root_requires_existing_directory() {
        let dir = tempdir().unwrap();
        let resolved = resolve_workspace_root(Some(dir.path())).unwrap();
        assert_eq!(resolved, std::fs::canonicalize(dir.path()).unwrap());

        let err = resolve_workspace_root(Some(&dir.path().join("missing")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("--cd 指向的目录不存在或不可访问"));
    }

    #[test]
    fn supervisor_control_missing_default_config_reports_empty_agent_id_without_writing_files() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let home = tempdir().unwrap();
        env.set("HOME", home.path().to_str().unwrap());
        let default_path = home.path().join(".acn").join("config.toml");

        let err = Config::load_or_init_for_supervisor_control(None)
            .unwrap_err()
            .to_string();

        assert!(!default_path.exists());
        assert!(!home.path().join(".acn").exists());
        assert!(err.contains("upstreams.default.agent_id must not be empty"));
    }

    #[test]
    fn supervisor_control_existing_config_does_not_create_storage_dirs() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (dir, path) = write_config(&openai_chat_config_raw());
        let acn_home = dir.path().join("acn");

        let (cfg, used_path) = Config::load_or_init_for_supervisor_control(Some(&path)).unwrap();

        assert_eq!(used_path, path);
        assert_eq!(cfg.storage.acn_home, acn_home);
        assert!(!cfg.storage.team_root.exists());
        assert!(!cfg.storage.agents_root.exists());
        assert!(!cfg.storage.skills_root().exists());
    }

    #[test]
    fn update_config_prefers_explicit_path_and_does_not_require_llm_key_or_create_dirs() {
        let env = EnvGuard::clean(CONFIG_BOOTSTRAP_ENV_KEYS);
        let (_env_dir, env_path) = write_config(&openai_chat_config_raw());
        let (explicit_dir, explicit_path) = write_config(&openai_chat_config_raw());
        env.set("ACN_CONFIG", env_path.to_str().unwrap());
        let acn_home = explicit_dir.path().join("acn");

        let (cfg, used_path) = Config::load_or_init_for_update(Some(&explicit_path)).unwrap();

        assert_eq!(used_path, explicit_path);
        assert_eq!(cfg.storage.acn_home, acn_home);
        assert!(!cfg.storage.team_root.exists());
        assert!(!cfg.storage.agents_root.exists());
    }

    #[test]
    fn openai_chat_provider_reads_configured_api_key_env() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("EXAMPLE_LLM_API_KEY", "example-key");
        let (_dir, path) = write_config(&openai_chat_config_raw());

        let cfg = Config::load(&path).unwrap();

        assert_eq!(cfg.agent.llm.provider, LlmProvider::OpenAiChat);
        assert_eq!(cfg.agent.llm.api_key_env, "EXAMPLE_LLM_API_KEY");
        assert_eq!(cfg.agent.llm.api_key.as_deref(), Some("example-key"));
    }

    #[test]
    fn openai_responses_provider_reads_configured_api_key_env() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("EXAMPLE_LLM_API_KEY", "example-key");
        let (_dir, path) = write_config(&openai_responses_config_raw());

        let cfg = Config::load(&path).unwrap();

        assert_eq!(cfg.agent.llm.provider, LlmProvider::OpenAiResponses);
        assert_eq!(cfg.agent.llm.api_key_env, "EXAMPLE_LLM_API_KEY");
        assert_eq!(cfg.agent.llm.api_key.as_deref(), Some("example-key"));
    }

    #[test]
    fn anthropic_provider_reads_configured_api_key_env_and_ignores_global_env() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("ANTHROPIC_API_KEY", "anthropic-default-key");
        env.set("CUSTOM_ANTHROPIC_API_KEY", "custom-anthropic-key");
        env.set("ANTHROPIC_ENDPOINT", "https://legacy-env.example");
        env.set("MODEL_NAME", "env-model");
        let (_dir, path) = write_config(&anthropic_config_raw());

        let cfg = Config::load(&path).unwrap();

        assert_eq!(cfg.agent.llm.provider, LlmProvider::Anthropic);
        assert_eq!(cfg.agent.llm.api_key_env, "CUSTOM_ANTHROPIC_API_KEY");
        assert_eq!(
            cfg.agent.llm.api_key.as_deref(),
            Some("custom-anthropic-key")
        );
        assert_eq!(cfg.agent.llm.endpoint, "https://llm.example.com/");
        assert_eq!(cfg.agent.llm.model, "example-anthropic-model");
    }

    #[test]
    fn openai_chat_provider_ignores_global_llm_api_key() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("EXAMPLE_LLM_API_KEY", "example-key");
        env.set("LLM_API_KEY", "llm-key");
        let (_dir, path) = write_config(&openai_chat_config_raw());

        let cfg = Config::load(&path).unwrap();

        assert_eq!(cfg.agent.llm.api_key.as_deref(), Some("example-key"));
    }

    #[test]
    fn openai_chat_provider_ignores_global_llm_endpoint() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("EXAMPLE_LLM_API_KEY", "example-key");
        env.set("LLM_ENDPOINT", "https://llm.example");
        let raw = openai_chat_config_raw().replace(
            r#"endpoint = "https://api.anthropic.com""#,
            r#"endpoint = "https://chat.example.com/v1/chat/completions""#,
        );
        let (_dir, path) = write_config(&raw);

        let cfg = Config::load(&path).unwrap();

        assert_eq!(
            cfg.agent.llm.endpoint,
            "https://chat.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn openai_chat_provider_keeps_config_endpoint_without_llm_endpoint() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("EXAMPLE_LLM_API_KEY", "example-key");
        let raw = openai_chat_config_raw().replace(
            r#"endpoint = "https://api.anthropic.com""#,
            r#"endpoint = "https://chat.example.com/v1/chat/completions""#,
        );
        let (_dir, path) = write_config(&raw);

        let cfg = Config::load(&path).unwrap();

        assert_eq!(
            cfg.agent.llm.endpoint,
            "https://chat.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn web_config_reads_endpoint_and_api_key_env() {
        let raw = format!(
            "{}\n[agent.tool.web]\nendpoint = \"https://search.example.com/v1/web_search\"\napi_key_env = \"EXAMPLE_WEB_SEARCH_KEY\"\nmax_count = 4\n",
            minimal_config_without_optional_defaults()
        );

        let cfg = parse_and_validate(&raw).unwrap();

        assert_eq!(
            cfg.agent.tool.web.endpoint,
            "https://search.example.com/v1/web_search"
        );
        assert_eq!(cfg.agent.tool.web.api_key_env, "EXAMPLE_WEB_SEARCH_KEY");
        assert_eq!(cfg.agent.tool.web.max_count, 4);
    }

    #[test]
    fn maintainer_admin_auth_defaults_disabled() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();

        assert!(!cfg.maintainer.auth.admin.enabled);
        assert_eq!(
            cfg.maintainer.auth.admin.username,
            DEFAULT_MAINTAINER_ADMIN_AUTH_USERNAME
        );
        assert_eq!(
            cfg.maintainer.auth.admin.password_env,
            DEFAULT_MAINTAINER_ADMIN_AUTH_PASSWORD_ENV
        );
        assert_eq!(cfg.maintainer.auth.admin.password, None);
    }

    #[test]
    fn maintainer_admin_auth_requires_configured_password_env_when_enabled() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let raw = format!(
            "{}\n[maintainer.auth.admin]\nenabled = true\nusername = \"admin\"\npassword_env = \"ACN_MAINTAINER_ADMIN_PASSWORD\"\n",
            minimal_config_without_optional_defaults()
        );
        let (_dir, path) = write_config(&raw);

        let err = Config::load(&path).unwrap_err().to_string();

        assert!(err.contains("[maintainer.auth.admin].password_env"));
        assert!(err.contains("ACN_MAINTAINER_ADMIN_PASSWORD"));
    }

    #[test]
    fn maintainer_admin_auth_password_env_is_not_required_for_router_load() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let raw = format!(
            "{}\n[maintainer.auth.admin]\nenabled = true\nusername = \"admin\"\npassword_env = \"ACN_MAINTAINER_ADMIN_PASSWORD\"\n",
            minimal_config_without_optional_defaults()
        );
        let (_dir, path) = write_config(&raw);

        let cfg = Config::load_for_router(&path).unwrap();

        assert!(cfg.maintainer.auth.admin.enabled);
        assert_eq!(cfg.maintainer.auth.admin.password, None);
    }

    #[test]
    fn maintainer_admin_auth_password_env_is_not_required_for_agent_cli_load() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let raw = format!(
            "{}\n[maintainer.auth.admin]\nenabled = true\nusername = \"admin\"\npassword_env = \"ACN_MAINTAINER_ADMIN_PASSWORD\"\n",
            minimal_config_without_optional_defaults()
        );
        let (_dir, path) = write_config(&raw);

        let (cfg, used_path) = Config::load_or_init_for_agent(Some(&path)).unwrap();

        assert_eq!(used_path, path);
        assert!(cfg.maintainer.auth.admin.enabled);
        assert_eq!(cfg.maintainer.auth.admin.password, None);
    }

    #[test]
    fn maintainer_admin_auth_reads_password_from_configured_env() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("ACN_MAINTAINER_ADMIN_PASSWORD", "secret");
        let raw = format!(
            "{}\n[maintainer.auth.admin]\nenabled = true\nusername = \"maintainer-admin\"\npassword_env = \"ACN_MAINTAINER_ADMIN_PASSWORD\"\n",
            minimal_config_without_optional_defaults()
        );
        let (_dir, path) = write_config(&raw);

        let cfg = Config::load(&path).unwrap();

        assert!(cfg.maintainer.auth.admin.enabled);
        assert_eq!(cfg.maintainer.auth.admin.username, "maintainer-admin");
        assert_eq!(
            cfg.maintainer.auth.admin.password.as_deref(),
            Some("secret")
        );
    }

    #[test]
    fn maintainer_admin_auth_debug_redacts_password() {
        let cfg = MaintainerAdminAuthConfig {
            enabled: true,
            username: "admin".to_string(),
            password_env: "ACN_MAINTAINER_ADMIN_PASSWORD".to_string(),
            password: Some("secret".to_string()),
        };

        let debug = format!("{cfg:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn old_config_sections_and_fields_are_rejected() {
        let base = minimal_config_without_optional_defaults();
        expect_parse_err_contains(
            base.replace(
                r#"acn_home = "./data""#,
                r#"team_root = "./data/team"
agents_root = "./data/agents"
workspace_root = ".""#,
            ),
            "unknown field `team_root`",
        );
        let cases = [
            ("[llm]\nprovider = \"anthropic\"\n", "unknown field `llm`"),
            (
                "[router.hybrid]\nenabled = true\n",
                "unknown field `hybrid`",
            ),
            (
                "[router]\nquery_timeout_secs = 60\n",
                "unknown field `query_timeout_secs`",
            ),
            (
                "[agent.client]\nmaintainer_endpoint = \"http://127.0.0.1:8062\"\n",
                "unknown field `client`",
            ),
            (
                "[clients]\nmaintainer_endpoint = \"http://127.0.0.1:8062\"\n",
                "unknown field `maintainer_endpoint`",
            ),
            (
                "[clients]\nrouter_endpoint = \"http://127.0.0.1:8061\"\n",
                "unknown field `router_endpoint`",
            ),
            (
                "[http_client]\ntimeout_secs = 30\n",
                "unknown field `http_client`",
            ),
            (
                "[tool]\nfile_read_max_chars = 100000\n",
                "unknown field `tool`",
            ),
            (
                "[memory]\nmemory_char_limit = 1600\n",
                "unknown field `memory`",
            ),
            (
                "[session_compaction]\nsummary_max_chars = 6000\n",
                "unknown field `session_compaction`",
            ),
            (
                "[maintainer]\nid_mint_max_retries = 3\n",
                "unknown field `id_mint_max_retries`",
            ),
            (
                "[maintainer.id]\npolicy_mint_max_retries = 3\n",
                "unknown field `policy_mint_max_retries`",
            ),
            (
                "[agent.tool]\nworkspace_root = \".\"\n",
                "unknown field `workspace_root`",
            ),
            (
                "[agent.cli]\ntool_input_preview_chars = 120\n",
                "unknown field `cli`",
            ),
            (
                "[agent.tool]\nsession_search_context_max_chars = 24000\n",
                "unknown field `session_search_context_max_chars`",
            ),
            (
                "[agent.tool]\nsession_search_summary_max_chars = 2400\n",
                "unknown field `session_search_summary_max_chars`",
            ),
            (
                "[agent.session.compaction]\nauto_compact_limit_tokens = 120000\n",
                "unknown field `auto_compact_limit_tokens`",
            ),
        ];

        for (legacy_snippet, expected) in cases {
            expect_parse_err_contains(format!("{base}\n{legacy_snippet}"), expected);
        }
    }

    #[test]
    fn maintainer_id_uses_generic_mint_retry_key() {
        let raw = format!(
            "{}\n[maintainer.id]\nmint_max_retries = 5\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();

        assert_eq!(cfg.maintainer.id.mint_max_retries, 5);
        assert_eq!(cfg.maintainer.id_mint_max_attempts(), 6);
    }

    #[test]
    fn key_runtime_limits_are_validated() {
        let cases = [
            (
                minimal_config_without_optional_defaults()
                    .replace("max_tokens = 4096", "max_tokens = 0"),
                "agent.llm.max_tokens",
            ),
            (
                minimal_config_without_optional_defaults()
                    .replace("context_window = 200000", "context_window = 0"),
                "agent.llm.context_window",
            ),
            (
                minimal_config_without_optional_defaults()
                    .replace("timeout_secs = 600", "timeout_secs = 0"),
                "agent.llm.timeout_secs",
            ),
            (
                format!(
                    "{}\n[clients.router]\nquery_timeout_secs = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "clients.router.query_timeout_secs",
            ),
            (
                format!(
                    "{}\n[maintainer.sweep]\ntick_interval_secs = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "maintainer.sweep.tick_interval_secs",
            ),
            (
                format!(
                    "{}\n[clients.http]\ntimeout_secs = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "clients.http.timeout_secs",
            ),
            (
                format!(
                    "{}\n[agent.tool]\nfile_read_max_chars = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.tool.file_read_max_chars",
            ),
            (
                format!(
                    "{}\n[agent.tool]\nfile_diff_max_changed_lines = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.tool.file_diff_max_changed_lines",
            ),
            (
                format!(
                    "{}\n[agent.tool]\nmax_parallel_tool_calls = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.tool.max_parallel_tool_calls",
            ),
            (
                format!(
                    "{}\n[agent.tool.web]\nendpoint = \"\"\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.tool.web.endpoint",
            ),
            (
                format!(
                    "{}\n[agent.tool.web]\napi_key_env = \"\"\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.tool.web.api_key_env",
            ),
            (
                format!(
                    "{}\n[agent.memory]\nmemory_char_limit = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.memory.memory_char_limit",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\nauto_compact_ctx_ratio = -0.1\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.auto_compact_ctx_ratio",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\nauto_compact_ctx_ratio = 1.1\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.auto_compact_ctx_ratio",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\ntail_hard_ctx_ratio = 0.0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.tail_hard_ctx_ratio",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\ntail_target_ctx_ratio = 0.0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.tail_target_ctx_ratio",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\ntail_target_ctx_ratio = 0.4\ntail_hard_ctx_ratio = 0.3\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.tail_target_ctx_ratio",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\ntail_previous_real_user_turns = 6\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.tail_previous_real_user_turns",
            ),
            (
                format!(
                    "{}\n[agent.session.compaction]\ntool_result_raw_max_chars = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.compaction.tool_result_raw_max_chars",
            ),
        ];

        for (raw, expected) in cases {
            expect_parse_err_contains(raw, expected);
        }
    }

    #[test]
    fn maintainer_sweep_deprecated_threshold_cannot_precede_stale_threshold() {
        let raw = format!(
            "{}\n[maintainer.sweep]\nstale_after_days = 30\ndeprecated_after_days = 10\n",
            minimal_config_without_optional_defaults()
        );

        expect_parse_err_contains(
            raw,
            "maintainer.sweep.deprecated_after_days must be >= maintainer.sweep.stale_after_days",
        );
    }

    #[test]
    fn openai_chat_provider_ignores_model_name_env() {
        let env = EnvGuard::clean(LLM_ENV_KEYS);
        env.set("EXAMPLE_LLM_API_KEY", "example-key");
        env.set("MODEL_NAME", "ignored-model");
        let (_dir, path) = write_config(&openai_chat_config_raw());

        let cfg = Config::load(&path).unwrap();

        assert_eq!(cfg.agent.llm.model, "example-chat-model");
    }

    #[test]
    fn openai_chat_provider_without_key_mentions_configured_api_key_env() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (_dir, path) = write_config(&openai_chat_config_raw());

        let err = Config::load(&path).unwrap_err();
        let err = err.to_string();

        assert!(err.contains(path.to_string_lossy().as_ref()));
        assert!(err
            .contains("[agent.llm].api_key_env 指定的环境变量 'EXAMPLE_LLM_API_KEY' 未设置或为空"));
    }

    #[test]
    fn supervisor_control_load_does_not_require_agent_llm_api_key() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let (_dir, path) = write_config(&openai_chat_config_raw());

        let (cfg, used_path) = Config::load_or_init_for_supervisor_control(Some(&path)).unwrap();

        assert_eq!(used_path, path);
        assert_eq!(cfg.agent.llm.provider, LlmProvider::OpenAiChat);
        assert_eq!(cfg.agent.llm.api_key_env, "EXAMPLE_LLM_API_KEY");
        assert_eq!(cfg.agent.llm.api_key, None);
        assert_eq!(
            cfg.resolve_upstream(None).unwrap().agent_id.as_str(),
            "agent-a"
        );
    }

    #[test]
    fn real_provider_requires_configured_api_key_env_name() {
        let _env = EnvGuard::clean(LLM_ENV_KEYS);
        let raw = openai_chat_config_raw().replace(
            r#"api_key_env = "EXAMPLE_LLM_API_KEY""#,
            r#"api_key_env = """#,
        );
        let (_dir, path) = write_config(&raw);

        let err = Config::load(&path).unwrap_err().to_string();

        assert!(err.contains(path.to_string_lossy().as_ref()));
        assert!(err.contains("未配置 [agent.llm].api_key_env"));
    }

    #[test]
    fn fork_memory_review_interval_defaults_and_validates() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            cfg.agent.session.memory_review.interval_turns,
            DEFAULT_FORK_MEMORY_REVIEW_INTERVAL_TURNS
        );
        assert_eq!(
            cfg.clients.router.query_timeout_secs,
            DEFAULT_ROUTER_QUERY_TIMEOUT_SECS
        );
        assert_eq!(
            cfg.agent.session.id_mint_max_attempts(),
            default_id_mint_max_attempts()
        );
        assert_eq!(
            cfg.maintainer.sweep.tick_interval_secs,
            DEFAULT_MAINTAINER_SWEEP_TICK_INTERVAL_SECS
        );

        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let acn_home = dir.path().join("acn");
        let raw = format!(
            r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

[storage]
acn_home = "{}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "example-anthropic-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000

[router]
refresh_interval_secs = 5

[router.daemon]
listen = "127.0.0.1:8061"

[maintainer.sweep]
tick_interval_secs = 86400
stale_after_days = 30
deprecated_after_days = 90

[maintainer.daemon]
listen = "127.0.0.1:8062"

[agent.session.memory_review]
interval_turns = 0
"#,
            acn_home.display()
        );
        std::fs::write(&path, raw).unwrap();

        let err = Config::load(&path).unwrap_err();
        assert!(err
            .to_string()
            .contains("agent.session.memory_review.interval_turns must be > 0"));
    }

    #[test]
    fn session_finalize_notification_config_defaults_and_parses() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            cfg.agent.session.notify_on_finalize_completion,
            DEFAULT_SESSION_NOTIFY_ON_FINALIZE_COMPLETION
        );
        assert_eq!(
            cfg.agent.session.cleanup_retention_days,
            DEFAULT_SESSION_CLEANUP_RETENTION_DAYS
        );

        let raw = format!(
            "{}\n[agent.session]\nnotify_on_finalize_completion = false\ncleanup_retention_days = 0\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();
        assert!(!cfg.agent.session.notify_on_finalize_completion);
        assert_eq!(cfg.agent.session.cleanup_retention_days, 0);

        expect_parse_err_contains(
            format!(
                "{}\n[agent.session]\nmax_tool_loop_turns = 32\n",
                minimal_config_without_optional_defaults()
            ),
            "unknown field `max_tool_loop_turns`",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[agent.session]\ncleanup_retention_days = {}\n",
                minimal_config_without_optional_defaults(),
                MAX_SESSION_CLEANUP_RETENTION_DAYS + 1
            ),
            "agent.session.cleanup_retention_days must be <=",
        );
    }

    #[test]
    fn session_tui_config_defaults_parses_and_validates() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            cfg.agent.session.tui.live_response_preview_max_lines,
            DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES
        );

        let raw = format!(
            "{}\n[agent.session.tui]\nlive_response_preview_max_lines = 5\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();
        assert_eq!(cfg.agent.session.tui.live_response_preview_max_lines, 5);

        let raw = format!(
            "{}\n[agent.session.tui]\nlive_response_preview_max_lines = -1\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();
        assert_eq!(cfg.agent.session.tui.live_response_preview_max_lines, -1);

        for value in ["-2", "0", "4"] {
            expect_parse_err_contains(
                format!(
                    "{}\n[agent.session.tui]\nlive_response_preview_max_lines = {value}\n",
                    minimal_config_without_optional_defaults()
                ),
                "agent.session.tui.live_response_preview_max_lines must be -1 (auto) or >= 5",
            );
        }
    }

    #[test]
    fn user_shell_lifecycle_timings_are_not_user_configurable() {
        for field in ["drain_grace_ms", "termination_grace_ms"] {
            expect_parse_err_contains(
                format!(
                    "{}\n[agent.session.user_shell]\n{field} = 250\n",
                    minimal_config_without_optional_defaults()
                ),
                &format!("unknown field `{field}`"),
            );
        }
    }

    #[test]
    fn turn_journal_config_defaults_parses_and_validates() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            cfg.agent.session.turn_journal.delta_snapshot_interval_ms,
            DEFAULT_TURN_JOURNAL_DELTA_SNAPSHOT_INTERVAL_MS
        );
        assert_eq!(
            cfg.agent.session.turn_journal.delta_snapshot_chars,
            DEFAULT_TURN_JOURNAL_DELTA_SNAPSHOT_CHARS
        );
        assert_eq!(
            cfg.agent
                .session
                .turn_journal
                .recovery_original_user_request_max_chars,
            DEFAULT_TURN_RECOVERY_ORIGINAL_USER_REQUEST_MAX_CHARS
        );
        assert_eq!(
            cfg.agent
                .session
                .turn_journal
                .recovery_partial_assistant_max_chars,
            DEFAULT_TURN_RECOVERY_PARTIAL_ASSISTANT_MAX_CHARS
        );
        assert_eq!(
            cfg.agent.session.turn_journal.recovery_tool_input_max_chars,
            DEFAULT_TURN_RECOVERY_TOOL_INPUT_MAX_CHARS
        );
        assert_eq!(
            cfg.agent
                .session
                .turn_journal
                .recovery_tool_output_max_chars,
            DEFAULT_TURN_RECOVERY_TOOL_OUTPUT_MAX_CHARS
        );
        assert_eq!(
            cfg.agent.session.turn_journal.recovery_user_steer_max_chars,
            DEFAULT_TURN_RECOVERY_USER_STEER_MAX_CHARS
        );

        let raw = format!(
            "{}\n[agent.session.turn_journal]\ndelta_snapshot_interval_ms = 250\ndelta_snapshot_chars = 512\nrecovery_original_user_request_max_chars = 2048\nrecovery_partial_assistant_max_chars = 4096\nrecovery_tool_input_max_chars = 1024\nrecovery_tool_output_max_chars = 2048\nrecovery_user_steer_max_chars = 3072\n",
            minimal_config_without_optional_defaults()
        );
        let cfg = parse_and_validate(&raw).unwrap();
        assert_eq!(
            cfg.agent.session.turn_journal.delta_snapshot_interval_ms,
            250
        );
        assert_eq!(cfg.agent.session.turn_journal.delta_snapshot_chars, 512);
        assert_eq!(
            cfg.agent
                .session
                .turn_journal
                .recovery_original_user_request_max_chars,
            2048
        );
        assert_eq!(
            cfg.agent
                .session
                .turn_journal
                .recovery_partial_assistant_max_chars,
            4096
        );
        assert_eq!(
            cfg.agent.session.turn_journal.recovery_tool_input_max_chars,
            1024
        );
        assert_eq!(
            cfg.agent
                .session
                .turn_journal
                .recovery_tool_output_max_chars,
            2048
        );
        assert_eq!(
            cfg.agent.session.turn_journal.recovery_user_steer_max_chars,
            3072
        );

        for (field, error) in [
            (
                "delta_snapshot_interval_ms",
                "agent.session.turn_journal.delta_snapshot_interval_ms must be > 0",
            ),
            (
                "delta_snapshot_chars",
                "agent.session.turn_journal.delta_snapshot_chars must be > 0",
            ),
            (
                "recovery_original_user_request_max_chars",
                "agent.session.turn_journal.recovery_original_user_request_max_chars must be > 0",
            ),
            (
                "recovery_partial_assistant_max_chars",
                "agent.session.turn_journal.recovery_partial_assistant_max_chars must be > 0",
            ),
            (
                "recovery_tool_input_max_chars",
                "agent.session.turn_journal.recovery_tool_input_max_chars must be > 0",
            ),
            (
                "recovery_tool_output_max_chars",
                "agent.session.turn_journal.recovery_tool_output_max_chars must be > 0",
            ),
            (
                "recovery_user_steer_max_chars",
                "agent.session.turn_journal.recovery_user_steer_max_chars must be > 0",
            ),
        ] {
            expect_parse_err_contains(
                format!(
                    "{}\n[agent.session.turn_journal]\n{field} = 0\n",
                    minimal_config_without_optional_defaults()
                ),
                error,
            );
        }
    }

    #[test]
    fn router_retrieval_config_validation_rejects_zero_values() {
        let cases = [
            ("router.embedding.max_concurrency", 0, 24, 24, 16, 2, 5),
            ("router.retrieval.lexical_top_n", 4, 0, 24, 16, 2, 5),
            ("router.retrieval.vector_top_m", 4, 24, 0, 16, 2, 5),
            ("router.retrieval.top_k", 4, 24, 24, 0, 2, 5),
            (
                "router.retrieval.vector.worker_poll_secs",
                4,
                24,
                24,
                16,
                0,
                5,
            ),
            (
                "router.retrieval.vector.query_timeout_secs",
                4,
                24,
                24,
                16,
                2,
                0,
            ),
        ];

        for (
            expected,
            max_concurrency,
            lexical_top_n,
            vector_top_m,
            top_k,
            vector_worker_poll_secs,
            vector_query_timeout_secs,
        ) in cases
        {
            let dir = tempdir().unwrap();
            let path = dir.path().join("config.toml");
            let acn_home = dir.path().join("acn");
            let raw = format!(
                r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

[storage]
acn_home = "{acn_home}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "example-anthropic-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000

[router.embedding]
provider = "openai_compatible"
endpoint = "https://api.openai.com/v1/embeddings"
model = "text-embedding-3-small"
timeout_secs = 60
max_concurrency = {max_concurrency}

[router]
refresh_interval_secs = 5

[router.daemon]
listen = "{DEFAULT_ROUTER_LISTEN}"

[router.retrieval]
enabled = true
    lexical_top_n = {lexical_top_n}
    vector_top_m = {vector_top_m}
    top_k = {top_k}
rerank_enabled = true

[router.retrieval.vector]
worker_poll_secs = {vector_worker_poll_secs}
query_timeout_secs = {vector_query_timeout_secs}

[maintainer.sweep]
tick_interval_secs = 86400
stale_after_days = 30
deprecated_after_days = 90

[maintainer.daemon]
listen = "{DEFAULT_MAINTAINER_LISTEN}"

[clients.http]
timeout_secs = 30
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000

[langfuse]
enabled = false
endpoint = "http://localhost:3000/api/public/otel/v1/traces"
service_name = "agent_claim_network"
"#,
                acn_home = acn_home.display()
            );
            std::fs::write(&path, raw).unwrap();

            let err = Config::load(&path).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected {expected} in error: {err}"
            );
        }
    }

    #[test]
    fn router_retrieval_defaults_load_from_config() {
        let cfg = parse_and_validate(minimal_config_without_optional_defaults()).unwrap();
        assert_eq!(
            cfg.router.refresh_interval_secs,
            default_router_refresh_interval_secs()
        );
        assert!(cfg.router.retrieval.enabled);
        assert_eq!(
            cfg.router.retrieval.lexical_top_n,
            default_router_hybrid_lexical_top_n()
        );
        assert_eq!(
            cfg.router.retrieval.vector_top_m,
            default_router_hybrid_vector_top_m()
        );
        assert_eq!(cfg.router.retrieval.top_k, default_router_hybrid_top_k());
        assert_eq!(
            cfg.router.retrieval.rerank_enabled,
            default_router_hybrid_rerank_enabled()
        );
        assert_eq!(
            cfg.router.retrieval.vector.worker_poll_secs,
            default_router_hybrid_vector_worker_poll_secs()
        );
        assert_eq!(
            cfg.router.retrieval.vector.query_timeout_secs,
            default_router_hybrid_vector_query_timeout_secs()
        );
        assert_eq!(cfg.router.retrieval.vector.retry_base_delay_ms, 2_000);
        assert_eq!(cfg.router.retrieval.vector.retry_max_delay_ms, 30_000);
        assert!(!cfg.router.embedding.model.is_empty());
        assert!(!cfg.router.embedding.api_key_env.is_empty());
        assert!(cfg.router.embedding.max_concurrency > 0);
        assert_eq!(cfg.router.rerank.provider, RerankProvider::OpenAiChat);
        assert!(!cfg.router.rerank.model.is_empty());
        assert!(!cfg.router.rerank.api_key_env.is_empty());
    }

    #[test]
    fn router_vector_retry_config_rejects_invalid_bounds() {
        expect_parse_err_contains(
            format!(
                "{}\n[router.retrieval.vector]\nretry_base_delay_ms = 0\nretry_max_delay_ms = 100\n",
                minimal_config_without_optional_defaults()
            ),
            "router.retrieval.vector.retry_base_delay_ms",
        );
        expect_parse_err_contains(
            format!(
                "{}\n[router.retrieval.vector]\nretry_base_delay_ms = 100\nretry_max_delay_ms = 99\n",
                minimal_config_without_optional_defaults()
            ),
            "router.retrieval.vector.retry_max_delay_ms",
        );
    }

    #[test]
    fn router_refresh_interval_must_be_positive() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let acn_home = dir.path().join("acn");
        let raw = format!(
            r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

[storage]
acn_home = "{}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "example-anthropic-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000

[router.embedding]
provider = "openai_compatible"
endpoint = "https://api.openai.com/v1/embeddings"
model = "text-embedding-3-small"
timeout_secs = 60
max_concurrency = 4

[router]
refresh_interval_secs = 0

[router.daemon]
listen = "127.0.0.1:8061"

[router.retrieval]
enabled = true
	lexical_top_n = 24
	vector_top_m = 24
	top_k = 16
	rerank_enabled = true

[router.retrieval.vector]
worker_poll_secs = 2
query_timeout_secs = 5

[router.rerank]
provider = "heuristic"
endpoint = "https://api.openai.com/v1/chat/completions"
model = "gpt-5.6-luna"
timeout_secs = 30
max_tokens = 512

[maintainer.sweep]
tick_interval_secs = 86400
stale_after_days = 30
deprecated_after_days = 90

[maintainer.daemon]
listen = "127.0.0.1:8062"

"#,
            acn_home.display()
        );
        std::fs::write(&path, raw).unwrap();

        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("router.refresh_interval_secs"));
    }

    #[test]
    fn memory_config_defaults_enable_safety_scan() {
        let cfg = MemoryConfig::default();
        assert_eq!(cfg.memory_char_limit, DEFAULT_MEMORY_CHAR_LIMIT);
        assert_eq!(cfg.user_char_limit, DEFAULT_USER_CHAR_LIMIT);
        assert!(cfg.memory_safety_scan);
    }

    #[test]
    fn memory_config_missing_safety_scan_defaults_true() {
        let cfg: MemoryConfig = toml::from_str(
            r#"
memory_char_limit = 1600
user_char_limit = 1000
"#,
        )
        .unwrap();
        assert!(cfg.memory_safety_scan);
    }

    #[test]
    fn memory_config_can_disable_safety_scan() {
        let cfg: MemoryConfig = toml::from_str(
            r#"
memory_char_limit = 1600
user_char_limit = 1000
memory_safety_scan = false
"#,
        )
        .unwrap();
        assert!(!cfg.memory_safety_scan);
    }
}
