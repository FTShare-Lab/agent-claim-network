//! MCP connection manager。
//!
//! 负责读取当前 selected-upstream runtime 的 `.mcp.json`、启动 enabled server、
//! 维护 server/tool 状态快照，并为 CLI status、后续 ToolRegistry 与 TUI 提供
//! 统一查询入口。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use rmcp::model::{CallToolResult, ServerInfo, Tool};
use serde_json::Value;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::config::{
    MCP_RECONNECT_MAX_RETRIES, MCP_RECONNECT_RETRY_BASE_DELAY_MS, MCP_RECONNECT_RETRY_MAX_DELAY_MS,
};
use crate::mcp::client::{
    McpClient, McpClientError, McpConnectReleaseFence, McpOAuthRefreshActivity,
    McpOAuthRefreshSupervisor, McpProgressCallback, McpProgressEvent,
};
use crate::mcp::config::{
    lock_mcp_json_config_timeout, read_mcp_json_config, write_mcp_json_config_atomic,
    McpConfigError, McpJsonConfig, McpServerConfig, McpTransportKind,
};

const TOOLS_LIST_PAGE_LIMIT: usize = 100;
const TOOLS_LIST_TOOL_LIMIT: usize = 256;
const MAX_MCP_INPUT_SCHEMA_BYTES: usize = 64 * 1024;
const OAUTH_FINAL_REFRESH_DRAIN_TIMEOUT: Duration = Duration::from_secs(35);
const MCP_CONFIG_WRITE_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

type PendingConnect = (
    String,
    McpServerConfig,
    u64,
    Arc<ConnectAttempt>,
    Option<Arc<ConnectAttempt>>,
);
type RefreshReset = (
    Vec<PendingConnect>,
    BTreeMap<String, Arc<McpClient>>,
    BTreeMap<String, Arc<ConnectAttempt>>,
);
type ConnectStart = (
    u64,
    Option<Arc<McpClient>>,
    Arc<ConnectAttempt>,
    Option<Arc<ConnectAttempt>>,
);

/// 某个 generation 的连接建立任务。replacement 必须等旧 attempt 真正退出，不能只取消 token。
struct ConnectAttempt {
    cancellation: CancellationToken,
    release_fence: Arc<McpConnectReleaseFence>,
}

impl ConnectAttempt {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancellation: CancellationToken::new(),
            release_fence: McpConnectReleaseFence::new(),
        })
    }

    fn cancel(&self) {
        self.release_fence.request_cancellation();
        self.cancellation.cancel();
    }

    fn complete(&self) {
        self.release_fence.finish_connect();
    }

    async fn wait_for_completion(&self) {
        self.release_fence.wait_for_completion().await;
    }

    async fn wait_for_pending_transport_release(&self) {
        self.release_fence
            .wait_for_pending_transport_release()
            .await;
    }

    fn release_failed(&self) -> bool {
        self.release_fence.cleanup_failed()
    }

    fn release_fence(&self) -> Arc<McpConnectReleaseFence> {
        Arc::clone(&self.release_fence)
    }
}

/// 在 connect future 的所有权转移到 outcome 安装前保持 completion fence。
struct ConnectAttemptGuard(Option<Arc<ConnectAttempt>>);

impl ConnectAttemptGuard {
    fn new(attempt: Arc<ConnectAttempt>) -> Self {
        Self(Some(attempt))
    }

    /// 成功/失败 outcome 要先安装当前 snapshot 或清理其 client，才允许 replacement 继续。
    fn hand_off_to_outcome(&mut self) -> Arc<ConnectAttempt> {
        // 此方法只在 connect_server 即将返回 Some(outcome) 时调用一次；所有其他路径由 Drop complete。
        self.0
            .take()
            .expect("ConnectAttemptGuard handoff is called exactly once")
    }
}

impl Drop for ConnectAttemptGuard {
    fn drop(&mut self) {
        if let Some(attempt) = self.0.take() {
            attempt.complete();
        }
    }
}

/// replacement 在旧 attempt 被取消后，必须同时确认其析构式 transport cleanup 成功。
async fn stale_connect_attempt_released(
    release_gates: &TransportReleaseGates,
    server_name: &str,
    stale_attempt: Option<Arc<ConnectAttempt>>,
) -> bool {
    let Some(stale_attempt) = stale_attempt else {
        return true;
    };
    stale_attempt.wait_for_completion().await;
    if stale_attempt.release_failed() {
        release_gates.quarantine(server_name);
        return false;
    }
    true
}

pub struct McpConnectionManager {
    config_path: PathBuf,
    workspace_root: PathBuf,
    progress_router: Arc<McpProgressRouter>,
    config_write_lock: tokio::sync::Mutex<()>,
    state: Arc<Mutex<McpManagerState>>,
    oauth_refresh_activity: McpOAuthRefreshActivity,
    /// `RunningService::close_with_timeout` 超时后会失去 join handle，无法再确认旧 transport
    /// 何时退出。因此同 server 必须持续隔离到进程退出，不能让下一次 Reconnect 绕过该失败。
    release_gates: Arc<TransportReleaseGates>,
}

#[derive(Default)]
struct TransportReleaseGates {
    unreleased_servers: Mutex<BTreeSet<String>>,
}

impl TransportReleaseGates {
    fn quarantine(&self, server_name: &str) {
        let mut servers = match self.unreleased_servers.lock() {
            Ok(servers) => servers,
            Err(poisoned) => poisoned.into_inner(),
        };
        servers.insert(server_name.to_string());
    }

    fn confirm_released(&self, server_name: &str) {
        let mut servers = match self.unreleased_servers.lock() {
            Ok(servers) => servers,
            Err(poisoned) => poisoned.into_inner(),
        };
        servers.remove(server_name);
    }

    fn contains(&self, server_name: &str) -> bool {
        let servers = match self.unreleased_servers.lock() {
            Ok(servers) => servers,
            Err(poisoned) => poisoned.into_inner(),
        };
        servers.contains(server_name)
    }
}

#[derive(Debug, Clone, Default)]
pub struct McpRuntimeState {
    pub servers: BTreeMap<String, McpServerSnapshot>,
    /// 每个 server 当前 lifecycle generation。工具定义与该 generation 一起冻结，
    /// 避免旧请求返回的 tool_use 被派发到 replacement connection。
    pub generations: BTreeMap<String, u64>,
    pub startup_error: Option<String>,
    pub workspace_root: Option<PathBuf>,
}

#[derive(Default)]
struct McpManagerState {
    servers: BTreeMap<String, McpServerSnapshot>,
    clients: McpClientSet,
    /// 尚在建立（包括内部退避）的连接任务。generation 变更会取消并 await 旧 attempt，
    /// 防止 stale task 在退避结束后再启动 transport，或与 replacement 短暂重叠。
    connect_attempts: BTreeMap<String, Arc<ConnectAttempt>>,
    generations: BTreeMap<String, u64>,
    config_revision: u64,
    startup_error: Option<String>,
    shutting_down: bool,
}

#[derive(Debug, Clone)]
pub struct McpServerSnapshot {
    pub name: String,
    pub config: McpServerConfig,
    pub transport: Option<McpTransportKind>,
    pub status: McpServerStatus,
    pub tools: Vec<McpToolSnapshot>,
    pub server_info: Option<ServerInfo>,
    pub last_connected_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub stderr_excerpt: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerStatus {
    Disabled,
    Starting,
    Reconnecting,
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
pub struct McpToolSnapshot {
    pub raw_name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub exposure: McpToolExposure,
    pub raw_tool: Tool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpToolExposure {
    Exposed,
    Filtered { reason: McpToolFilterReason },
    Unsupported { reason: McpToolUnsupportedReason },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpToolFilterReason {
    DisabledTools,
    NotInEnabledTools,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpToolUnsupportedReason {
    InvalidSchema,
}

#[derive(Clone)]
pub struct McpToolProgressReporter {
    on_progress: Arc<dyn Fn(McpProgressEvent) + Send + Sync + 'static>,
}

impl McpToolProgressReporter {
    pub fn new<F>(on_progress: F) -> Self
    where
        F: Fn(McpProgressEvent) + Send + Sync + 'static,
    {
        Self {
            on_progress: Arc::new(on_progress),
        }
    }

    pub(crate) fn emit(&self, event: McpProgressEvent) {
        (self.on_progress)(event);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum McpManagerError {
    #[error(transparent)]
    Config(#[from] McpConfigError),
    #[error(transparent)]
    Client(#[from] McpClientError),
    #[error("MCP server 不存在: {0}")]
    ServerNotFound(String),
    #[error("MCP server '{0}' 当前不是 ready 状态")]
    ServerNotReady(String),
    #[error("MCP server '{server}' 已切换连接 generation；拒绝执行旧 Provider 请求返回的工具调用")]
    StaleToolGeneration { server: String },
    #[error("MCP tool '{server}/{tool}' 当前未明确声明为只读")]
    ReadOnlyRequirementFailed { server: String, tool: String },
    #[error("MCP server '{server}' 的旧 transport 未在关闭窗口内确认释放，已阻止建立 replacement connection")]
    TransportReleaseTimeout { server: String },
    #[error("MCP manager 正在关闭，不能启动新的 lifecycle 操作")]
    ShuttingDown,
}

struct ConnectOutcome {
    name: String,
    generation: u64,
    snapshot: McpServerSnapshot,
    client: Option<Arc<McpClient>>,
    retryable: bool,
    /// 在 outcome 被安装或其 stale client 被关闭之前，replacement 必须持续等待。
    attempt: Option<Arc<ConnectAttempt>>,
}

/// 调用期间固定的 ready client 与 generation；manager 锁只用于构造该快照。
struct ReadyClientLease {
    client: Arc<McpClient>,
    generation: u64,
}

/// UI lifecycle 操作先同步摘除并取消旧 client；新 generation 建连前由调用方等待其 transport 收束。
pub struct McpRuntimeTransition {
    server_name: String,
    generation: u64,
    stale_client: Option<Arc<McpClient>>,
    stale_connect_attempt: Option<Arc<ConnectAttempt>>,
    release_gates: Arc<TransportReleaseGates>,
}

impl McpRuntimeTransition {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub async fn wait_for_transport_release(self) -> Result<(), McpManagerError> {
        if !stale_connect_attempt_released(
            &self.release_gates,
            &self.server_name,
            self.stale_connect_attempt,
        )
        .await
        {
            return Err(McpManagerError::TransportReleaseTimeout {
                server: self.server_name,
            });
        }
        if shutdown_client(&self.release_gates, self.stale_client)
            .await
            .is_some()
        {
            return Err(McpManagerError::TransportReleaseTimeout {
                server: self.server_name,
            });
        }
        Ok(())
    }
}

struct McpProgressRouter {
    routes: Mutex<BTreeMap<(String, String), McpToolProgressReporter>>,
    external_callback: Option<McpProgressCallback>,
}

struct McpProgressRegistration {
    router: Arc<McpProgressRouter>,
    server_name: String,
    token: String,
}

#[derive(Default)]
struct McpClientSet {
    clients: BTreeMap<String, Arc<McpClient>>,
}

impl McpProgressRouter {
    fn new(external_callback: Option<McpProgressCallback>) -> Self {
        Self {
            routes: Mutex::new(BTreeMap::new()),
            external_callback,
        }
    }

    fn callback(self: &Arc<Self>) -> Option<McpProgressCallback> {
        let router = Arc::clone(self);
        Some(Arc::new(move |event| router.dispatch(event)))
    }

    fn register(
        self: &Arc<Self>,
        server_name: String,
        token: String,
        reporter: McpToolProgressReporter,
    ) -> McpProgressRegistration {
        self.lock_routes()
            .insert((server_name.clone(), token.clone()), reporter);
        McpProgressRegistration {
            router: Arc::clone(self),
            server_name,
            token,
        }
    }

    fn dispatch(&self, event: McpProgressEvent) {
        if let Some(callback) = &self.external_callback {
            callback(event.clone());
        }
        let reporter = self
            .lock_routes()
            .get(&(event.server_name.clone(), event.progress_token.clone()))
            .cloned();
        if let Some(reporter) = reporter {
            reporter.emit(event);
        }
    }

    fn unregister(&self, server_name: &str, token: &str) {
        self.lock_routes()
            .remove(&(server_name.to_string(), token.to_string()));
    }

    fn lock_routes(&self) -> MutexGuard<'_, BTreeMap<(String, String), McpToolProgressReporter>> {
        match self.routes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Drop for McpProgressRegistration {
    fn drop(&mut self) {
        self.router.unregister(&self.server_name, &self.token);
    }
}

impl Drop for McpConnectionManager {
    fn drop(&mut self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.cancel_all_connect_attempts();
        let stale_clients = std::mem::take(&mut state.clients.clients)
            .into_values()
            .collect();
        request_shutdown_clients(stale_clients);
    }
}

impl McpConnectionManager {
    pub fn new(
        config_path: PathBuf,
        workspace_root: PathBuf,
        progress_callback: Option<McpProgressCallback>,
    ) -> Self {
        Self {
            config_path,
            workspace_root,
            progress_router: Arc::new(McpProgressRouter::new(progress_callback)),
            config_write_lock: tokio::sync::Mutex::new(()),
            state: Arc::new(Mutex::new(McpManagerState::default())),
            oauth_refresh_activity: McpOAuthRefreshActivity::default(),
            release_gates: Arc::new(TransportReleaseGates::default()),
        }
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// 正常退出时收束连接与其分离的 OAuth refresh，避免服务端已轮换 token、
    /// 本地保存尚未完成时被 Tokio runtime 直接中止。
    pub async fn shutdown(&self) {
        {
            let mut state = self.lock_state();
            state.shutting_down = true;
        }
        // 与已进入 read-modify-write 的 enable/disable 排队；terminal flag 会让随后排队的
        // mutation 在取得本锁后退出，确保下面的资源快照是最终快照。
        let config_guard = self.config_write_lock.lock().await;
        let (connect_attempts, clients) = {
            let mut state = self.lock_state();
            let connect_attempts = state.cancel_all_connect_attempts();
            let clients = std::mem::take(&mut state.clients.clients)
                .into_values()
                .collect::<Vec<_>>();
            (connect_attempts, clients)
        };
        drop(config_guard);
        for client in &clients {
            client.request_shutdown();
        }
        for attempt in connect_attempts.into_values() {
            attempt.wait_for_completion().await;
        }
        for client in clients {
            if let Some(server) = shutdown_client(&self.release_gates, Some(client)).await {
                log::warn!("MCP server '{server}' did not confirm shutdown during ACN exit");
            }
        }
        if time::timeout(
            OAUTH_FINAL_REFRESH_DRAIN_TIMEOUT,
            self.oauth_refresh_activity.wait_for_idle(),
        )
        .await
        .is_err()
        {
            log::warn!(
                "MCP OAuth refresh did not finish within {:?} during ACN exit",
                OAUTH_FINAL_REFRESH_DRAIN_TIMEOUT
            );
        }
    }

    pub async fn refresh_all(&self) -> Result<(), McpManagerError> {
        self.ensure_running()?;
        let (enabled, mut stale_clients, mut stale_connect_attempts) = loop {
            let revision = self.config_revision();
            let cfg = read_mcp_json_config(&self.config_path).await?;
            if let Some(reset) = self.reset_for_refresh_if_current(&cfg, revision) {
                break reset;
            }
        };
        // 每个 server 只等待自己的旧 transport；某个 server 的 3 秒 release gate 不能推迟其他
        // server 重新 ready。完成的 outcome 立刻写回 state，refresh 自身仍等待所有清理任务收束。
        let mut work: futures::stream::FuturesUnordered<
            BoxFuture<'static, Option<ConnectOutcome>>,
        > = futures::stream::FuturesUnordered::new();
        for (name, server, generation, attempt, stale_attempt) in enabled {
            let stale_client = stale_clients.remove(&name);
            let config_path = self.config_path.clone();
            let workspace_root = self.workspace_root.clone();
            let progress_callback = self.progress_router.callback();
            let release_gates = Arc::clone(&self.release_gates);
            let oauth_refresh_activity = self.oauth_refresh_activity.clone();
            work.push(
                async move {
                    if !stale_connect_attempt_released(&release_gates, &name, stale_attempt).await
                        || shutdown_client(&release_gates, stale_client)
                            .await
                            .is_some()
                        || release_gates.contains(&name)
                    {
                        attempt.complete();
                        return Some(transport_release_timeout_outcome(name, server, generation));
                    }
                    connect_server(
                        name,
                        server,
                        generation,
                        config_path,
                        workspace_root,
                        progress_callback,
                        attempt,
                        release_gates,
                        oauth_refresh_activity,
                    )
                    .await
                }
                .boxed(),
            );
        }
        // 已移除或 disabled 的 server 没有 replacement，但仍需受限地收束其旧 client；把它们放进
        // 同一完成队列避免遗留后台 task，同时不影响其他 server outcome 的即时安装。
        for (name, client) in stale_clients {
            let release_gates = Arc::clone(&self.release_gates);
            let stale_attempt = stale_connect_attempts.remove(&name);
            work.push(
                async move {
                    let _ =
                        stale_connect_attempt_released(&release_gates, &name, stale_attempt).await;
                    let _ = shutdown_client(&release_gates, Some(client)).await;
                    None
                }
                .boxed(),
            );
        }
        for (name, stale_attempt) in stale_connect_attempts {
            let release_gates = Arc::clone(&self.release_gates);
            work.push(
                async move {
                    let _ =
                        stale_connect_attempt_released(&release_gates, &name, Some(stale_attempt))
                            .await;
                    None
                }
                .boxed(),
            );
        }
        while let Some(outcome) = work.next().await {
            if let Some(outcome) = outcome {
                self.apply_connect_outcome(outcome).await;
            }
        }
        Ok(())
    }

    pub async fn snapshot(&self) -> McpRuntimeState {
        self.snapshot_sync()
    }

    pub fn snapshot_sync(&self) -> McpRuntimeState {
        let mut snapshot = self.lock_state().snapshot();
        snapshot.workspace_root = Some(self.workspace_root.clone());
        snapshot
    }

    pub fn set_startup_error(&self, error: impl Into<String>) {
        let stale_clients = {
            let mut state = self.lock_state();
            state.startup_error = Some(error.into());
            state.servers.clear();
            state.cancel_all_connect_attempts();
            std::mem::take(&mut state.clients.clients)
                .into_values()
                .collect()
        };
        request_shutdown_clients(stale_clients);
    }

    pub async fn call_tool(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: Option<Value>,
        progress_reporter: Option<McpToolProgressReporter>,
    ) -> Result<CallToolResult, McpManagerError> {
        self.call_tool_cancellable(server_name, tool_name, arguments, progress_reporter, None)
            .await
    }

    /// 调用共享 client，并把当前 turn 的取消限制为这一个 MCP request。
    pub async fn call_tool_cancellable(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: Option<Value>,
        progress_reporter: Option<McpToolProgressReporter>,
        cancellation: Option<CancellationToken>,
    ) -> Result<CallToolResult, McpManagerError> {
        self.call_tool_with_read_only_requirement(
            server_name,
            tool_name,
            arguments,
            progress_reporter,
            false,
            cancellation,
            None,
        )
        .await
    }

    /// 仅在当前 snapshot 与同一常驻 client 的实时 `tools/list` 都确认只读时调用 MCP 工具。
    pub async fn call_read_only_tool(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: Option<Value>,
        progress_reporter: Option<McpToolProgressReporter>,
    ) -> Result<CallToolResult, McpManagerError> {
        self.call_read_only_tool_cancellable(
            server_name,
            tool_name,
            arguments,
            progress_reporter,
            None,
        )
        .await
    }

    /// read-only 实时复核后调用共享 client，并保留调用方的 turn cancellation。
    pub async fn call_read_only_tool_cancellable(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: Option<Value>,
        progress_reporter: Option<McpToolProgressReporter>,
        cancellation: Option<CancellationToken>,
    ) -> Result<CallToolResult, McpManagerError> {
        self.call_tool_with_read_only_requirement(
            server_name,
            tool_name,
            arguments,
            progress_reporter,
            true,
            cancellation,
            None,
        )
        .await
    }

    /// 仅执行由同一 Provider request catalog 暴露的 generation。
    #[allow(
        clippy::too_many_arguments,
        reason = "冻结工具调用需完整保留现有 tools/call 参数并增加 generation admission"
    )]
    pub(crate) async fn call_tool_cancellable_for_generation(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: Option<Value>,
        progress_reporter: Option<McpToolProgressReporter>,
        require_read_only: bool,
        cancellation: Option<CancellationToken>,
        expected_generation: u64,
    ) -> Result<CallToolResult, McpManagerError> {
        self.call_tool_with_read_only_requirement(
            server_name,
            tool_name,
            arguments,
            progress_reporter,
            require_read_only,
            cancellation,
            Some(expected_generation),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "共享执行边界统一承接普通、只读与 generation-fenced tools/call"
    )]
    async fn call_tool_with_read_only_requirement(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: Option<Value>,
        progress_reporter: Option<McpToolProgressReporter>,
        require_read_only: bool,
        cancellation: Option<CancellationToken>,
        expected_generation: Option<u64>,
    ) -> Result<CallToolResult, McpManagerError> {
        let lease = {
            let state = self.lock_state();
            if state.shutting_down {
                return Err(McpManagerError::ShuttingDown);
            }
            let Some(snapshot) = state.servers.get(server_name) else {
                return Err(McpManagerError::ServerNotFound(server_name.to_string()));
            };
            if snapshot.status != McpServerStatus::Ready {
                return Err(McpManagerError::ServerNotReady(server_name.to_string()));
            }
            let generation = state.generation_for(server_name);
            if expected_generation.is_some_and(|expected| expected != generation) {
                return Err(McpManagerError::StaleToolGeneration {
                    server: server_name.to_string(),
                });
            }
            let Some(client) = state.clients.clients.get(server_name).cloned() else {
                return Err(McpManagerError::ServerNotReady(server_name.to_string()));
            };
            if require_read_only
                && !snapshot.tools.iter().any(|tool| {
                    tool.raw_name == tool_name
                        && matches!(tool.exposure, McpToolExposure::Exposed)
                        && raw_tool_is_read_only(&tool.raw_tool)
                })
            {
                return Err(McpManagerError::ReadOnlyRequirementFailed {
                    server: server_name.to_string(),
                    tool: tool_name.to_string(),
                });
            }
            ReadyClientLease { client, generation }
        };
        // 只读实时 tools/list 属于这一次 tools/call 的 admission，不能给后续 call 重置 timeout。
        let deadline = lease.client.next_tool_deadline();
        let mut progress_registration = None;
        let mut register_progress = |token| {
            if let Some(reporter) = &progress_reporter {
                progress_registration = Some(self.progress_router.register(
                    server_name.to_string(),
                    token,
                    reporter.clone(),
                ));
            }
        };
        let mut result = if require_read_only {
            match lease
                .client
                .list_tools_cancellable_until(
                    TOOLS_LIST_PAGE_LIMIT,
                    TOOLS_LIST_TOOL_LIMIT,
                    cancellation.clone(),
                    deadline,
                )
                .await
            {
                Err(err) => Err(McpManagerError::Client(err)),
                Ok(tools) => {
                    if !tools
                        .iter()
                        .any(|tool| tool.name.as_ref() == tool_name && raw_tool_is_read_only(tool))
                    {
                        Err(McpManagerError::ReadOnlyRequirementFailed {
                            server: server_name.to_string(),
                            tool: tool_name.to_string(),
                        })
                    } else if !self.ready_client_matches(
                        server_name,
                        lease.generation,
                        &lease.client,
                    ) {
                        Err(McpManagerError::ServerNotReady(server_name.to_string()))
                    } else {
                        lease
                            .client
                            .call_tool_cancellable_until_with_progress_registration(
                                tool_name,
                                arguments,
                                Some(&mut register_progress),
                                cancellation.clone(),
                                deadline,
                            )
                            .await
                            .map_err(McpManagerError::Client)
                    }
                }
            }
        } else if !self.ready_client_matches(server_name, lease.generation, &lease.client) {
            // lease 取出后，UI 操作或重连可能已替换本 generation；不要向旧 client 发起新请求。
            Err(McpManagerError::ServerNotReady(server_name.to_string()))
        } else {
            lease
                .client
                .call_tool_cancellable_until_with_progress_registration(
                    tool_name,
                    arguments,
                    Some(&mut register_progress),
                    cancellation,
                    deadline,
                )
                .await
                .map_err(McpManagerError::Client)
        };
        if progress_registration.is_some() {
            // rmcp 将 notification 放到独立任务执行；让已经排队的 progress 先完成，
            // 再析构 registration，避免响应先返回时丢失同一 SSE 流中的末尾进度。
            tokio::task::yield_now().await;
        }
        if result.is_ok()
            && !self.ready_client_matches(server_name, lease.generation, &lease.client)
        {
            // lifecycle 在 response 到达前替换了 client 时，旧结果不能回灌到新 generation。
            result = Err(McpManagerError::ServerNotReady(server_name.to_string()));
        }
        if let Err(McpManagerError::Client(err)) = &result {
            if err.is_connection_scoped() {
                self.mark_server_failed_if_generation_current(
                    server_name,
                    lease.generation,
                    &lease.client,
                    err.to_string(),
                )
                .await;
            }
        }
        result
    }

    pub async fn disable_server(&self, server_name: &str) -> Result<(), McpManagerError> {
        let _config_guard = self.config_write_lock.lock().await;
        self.ensure_running()?;
        let file_guard =
            lock_mcp_json_config_timeout(&self.config_path, MCP_CONFIG_WRITE_LOCK_TIMEOUT).await?;
        self.ensure_running()?;
        let mut cfg = read_mcp_json_config(&self.config_path).await?;
        let server = cfg
            .servers
            .get_mut(server_name)
            .ok_or_else(|| McpManagerError::ServerNotFound(server_name.to_string()))?;
        server.enabled = Some(false);
        write_mcp_json_config_atomic(&self.config_path, &cfg).await?;
        drop(file_guard);
        let stale_client = {
            let mut state = self.lock_state();
            state.bump_config_revision();
            state.cancel_connect_attempt(server_name);
            state.bump_generation(server_name);
            let stale_client = state.clients.clients.remove(server_name);
            if let Some(snapshot) = state.servers.get_mut(server_name) {
                snapshot.status = McpServerStatus::Disabled;
                snapshot.config.enabled = Some(false);
                snapshot.tools.clear();
                snapshot.last_error = None;
                snapshot.stderr_excerpt = None;
            }
            stale_client
        };
        if let Some(server) = shutdown_client(&self.release_gates, stale_client).await {
            // Disable 已经持久化并从 runtime 摘除了 client；即使底层 transport 未按时退出，
            // 也应保持 disabled，且 release gate 会拒绝后续 enable/reconnect replacement。
            log::warn!(
                "MCP server '{server}' disabled but its transport did not confirm shutdown; restart ACN before re-enabling"
            );
        }
        Ok(())
    }

    pub async fn enable_server_if_current(
        &self,
        server_name: &str,
        expected_generation: u64,
    ) -> Result<(), McpManagerError> {
        let config_guard = self.config_write_lock.lock().await;
        self.ensure_running()?;
        let file_guard =
            lock_mcp_json_config_timeout(&self.config_path, MCP_CONFIG_WRITE_LOCK_TIMEOUT).await?;
        self.ensure_running()?;
        let mut cfg = read_mcp_json_config(&self.config_path).await?;
        let server = cfg
            .servers
            .get_mut(server_name)
            .ok_or_else(|| McpManagerError::ServerNotFound(server_name.to_string()))?;
        server.enabled = Some(true);
        let server = server.clone();
        if !self.generation_matches(server_name, expected_generation) {
            return Ok(());
        }
        self.ensure_replacement_is_allowed(server_name)?;
        write_mcp_json_config_atomic(&self.config_path, &cfg).await?;
        drop(file_guard);
        drop(config_guard);
        if !self.generation_matches(server_name, expected_generation) {
            return Ok(());
        }
        let Some((generation, stale_client, attempt, stale_attempt)) = self
            .begin_connect_attempt_if_current(
                server_name,
                &server,
                McpServerStatus::Reconnecting,
                true,
                true,
                expected_generation,
            )
        else {
            return Ok(());
        };
        if !stale_connect_attempt_released(&self.release_gates, server_name, stale_attempt).await {
            attempt.complete();
            return Err(McpManagerError::TransportReleaseTimeout {
                server: server_name.to_string(),
            });
        }
        if let Some(server) = shutdown_client(&self.release_gates, stale_client).await {
            attempt.complete();
            return Err(McpManagerError::TransportReleaseTimeout { server });
        }
        let outcome = connect_server(
            server_name.to_string(),
            server,
            generation,
            self.config_path.clone(),
            self.workspace_root.clone(),
            self.progress_router.callback(),
            attempt,
            Arc::clone(&self.release_gates),
            self.oauth_refresh_activity.clone(),
        )
        .await;
        if let Some(outcome) = outcome {
            self.apply_connect_outcome(outcome).await;
        }
        Ok(())
    }

    pub async fn reconnect_server(&self, server_name: &str) -> Result<(), McpManagerError> {
        self.ensure_running()?;
        self.ensure_replacement_is_allowed(server_name)?;
        let cfg = read_mcp_json_config(&self.config_path).await?;
        let server = cfg
            .servers
            .get(server_name)
            .ok_or_else(|| McpManagerError::ServerNotFound(server_name.to_string()))?
            .clone();
        if !server.is_enabled() {
            let stale_client = {
                let mut state = self.lock_state();
                if state.shutting_down {
                    return Err(McpManagerError::ShuttingDown);
                }
                state.cancel_connect_attempt(server_name);
                state.bump_generation(server_name);
                state.servers.insert(
                    server_name.to_string(),
                    disabled_snapshot(server_name.to_string(), server),
                );
                state.clients.clients.remove(server_name)
            };
            if let Some(server) = shutdown_client(&self.release_gates, stale_client).await {
                return Err(McpManagerError::TransportReleaseTimeout { server });
            }
            return Ok(());
        }
        let Some((generation, stale_client, attempt, stale_attempt)) = self.begin_connect_attempt(
            server_name,
            &server,
            McpServerStatus::Reconnecting,
            true,
            false,
        ) else {
            return Err(McpManagerError::ShuttingDown);
        };
        if !stale_connect_attempt_released(&self.release_gates, server_name, stale_attempt).await {
            attempt.complete();
            return Err(McpManagerError::TransportReleaseTimeout {
                server: server_name.to_string(),
            });
        }
        if let Some(server) = shutdown_client(&self.release_gates, stale_client).await {
            attempt.complete();
            return Err(McpManagerError::TransportReleaseTimeout { server });
        }
        let outcome = connect_server(
            server_name.to_string(),
            server,
            generation,
            self.config_path.clone(),
            self.workspace_root.clone(),
            self.progress_router.callback(),
            attempt,
            Arc::clone(&self.release_gates),
            self.oauth_refresh_activity.clone(),
        )
        .await;
        if let Some(outcome) = outcome {
            self.apply_connect_outcome(outcome).await;
        }
        Ok(())
    }

    pub async fn reconnect_server_if_current(
        &self,
        server_name: &str,
        expected_generation: u64,
    ) -> Result<(), McpManagerError> {
        let cfg = read_mcp_json_config(&self.config_path).await?;
        let server = cfg
            .servers
            .get(server_name)
            .ok_or_else(|| McpManagerError::ServerNotFound(server_name.to_string()))?
            .clone();
        if !self.generation_matches(server_name, expected_generation) {
            return Ok(());
        }
        self.ensure_replacement_is_allowed(server_name)?;
        if !server.is_enabled() {
            let stale_client = {
                let mut state = self.lock_state();
                if state.shutting_down || state.generation_for(server_name) != expected_generation {
                    return Ok(());
                }
                state.cancel_connect_attempt(server_name);
                state.bump_generation(server_name);
                state.servers.insert(
                    server_name.to_string(),
                    disabled_snapshot(server_name.to_string(), server),
                );
                state.clients.clients.remove(server_name)
            };
            if let Some(server) = shutdown_client(&self.release_gates, stale_client).await {
                return Err(McpManagerError::TransportReleaseTimeout { server });
            }
            return Ok(());
        }
        let Some((generation, stale_client, attempt, stale_attempt)) = self
            .begin_connect_attempt_if_current(
                server_name,
                &server,
                McpServerStatus::Reconnecting,
                true,
                false,
                expected_generation,
            )
        else {
            return Ok(());
        };
        if !stale_connect_attempt_released(&self.release_gates, server_name, stale_attempt).await {
            attempt.complete();
            return Err(McpManagerError::TransportReleaseTimeout {
                server: server_name.to_string(),
            });
        }
        if let Some(server) = shutdown_client(&self.release_gates, stale_client).await {
            attempt.complete();
            return Err(McpManagerError::TransportReleaseTimeout { server });
        }
        let outcome = connect_server(
            server_name.to_string(),
            server,
            generation,
            self.config_path.clone(),
            self.workspace_root.clone(),
            self.progress_router.callback(),
            attempt,
            Arc::clone(&self.release_gates),
            self.oauth_refresh_activity.clone(),
        )
        .await;
        if let Some(outcome) = outcome {
            self.apply_connect_outcome(outcome).await;
        }
        Ok(())
    }

    pub fn begin_server_reconnecting_runtime(&self, server_name: &str) -> McpRuntimeTransition {
        let (generation, stale_client, stale_connect_attempt) = {
            let mut state = self.lock_state();
            if state.shutting_down {
                return terminal_runtime_transition(
                    server_name,
                    state.generation_for(server_name),
                    &self.release_gates,
                );
            }
            let stale_connect_attempt = state.cancel_connect_attempt(server_name);
            let generation = state.bump_generation(server_name);
            let stale_client = state.clients.clients.remove(server_name);
            if let Some(snapshot) = state.servers.get_mut(server_name) {
                snapshot.status = McpServerStatus::Reconnecting;
                snapshot.tools.clear();
                snapshot.last_error = None;
                snapshot.stderr_excerpt = None;
            }
            (generation, stale_client, stale_connect_attempt)
        };
        if let Some(client) = &stale_client {
            client.request_shutdown();
        }
        McpRuntimeTransition {
            server_name: server_name.to_string(),
            generation,
            stale_client,
            stale_connect_attempt,
            release_gates: Arc::clone(&self.release_gates),
        }
    }

    pub fn begin_server_disabled_runtime(&self, server_name: &str) -> McpRuntimeTransition {
        let (generation, stale_client, stale_connect_attempt) = {
            let mut state = self.lock_state();
            if state.shutting_down {
                return terminal_runtime_transition(
                    server_name,
                    state.generation_for(server_name),
                    &self.release_gates,
                );
            }
            let stale_connect_attempt = state.cancel_connect_attempt(server_name);
            let generation = state.bump_generation(server_name);
            let stale_client = state.clients.clients.remove(server_name);
            if let Some(snapshot) = state.servers.get_mut(server_name) {
                snapshot.status = McpServerStatus::Disabled;
                snapshot.config.enabled = Some(false);
                snapshot.tools.clear();
                snapshot.last_error = None;
                snapshot.stderr_excerpt = None;
            }
            (generation, stale_client, stale_connect_attempt)
        };
        if let Some(client) = &stale_client {
            client.request_shutdown();
        }
        McpRuntimeTransition {
            server_name: server_name.to_string(),
            generation,
            stale_client,
            stale_connect_attempt,
            release_gates: Arc::clone(&self.release_gates),
        }
    }

    pub fn mark_server_failed_runtime(&self, server_name: &str, error: impl Into<String>) {
        let stale_client = {
            let mut state = self.lock_state();
            if state.shutting_down {
                return;
            }
            state.cancel_connect_attempt(server_name);
            state.bump_generation(server_name);
            let stale_client = state.clients.clients.remove(server_name);
            if let Some(snapshot) = state.servers.get_mut(server_name) {
                snapshot.status = McpServerStatus::Failed;
                snapshot.tools.clear();
                snapshot.last_error = Some(error.into());
                snapshot.stderr_excerpt = None;
            }
            stale_client
        };
        request_shutdown_client(stale_client);
    }

    /// 仅当 UI operation 仍对应当前 generation 时才把 server 标记 failed，避免旧操作的 I/O 错误
    /// 覆盖后续已安装的 ready client。
    pub fn mark_server_failed_runtime_if_current(
        &self,
        server_name: &str,
        expected_generation: u64,
        error: impl Into<String>,
    ) {
        let stale_client = {
            let mut state = self.lock_state();
            if state.shutting_down || state.generation_for(server_name) != expected_generation {
                return;
            }
            state.cancel_connect_attempt(server_name);
            state.bump_generation(server_name);
            let stale_client = state.clients.clients.remove(server_name);
            if let Some(snapshot) = state.servers.get_mut(server_name) {
                snapshot.status = McpServerStatus::Failed;
                snapshot.tools.clear();
                snapshot.last_error = Some(error.into());
                snapshot.stderr_excerpt = None;
            }
            stale_client
        };
        request_shutdown_client(stale_client);
    }

    fn reset_for_refresh_if_current(
        &self,
        cfg: &McpJsonConfig,
        revision: u64,
    ) -> Option<RefreshReset> {
        let mut state = self.lock_state();
        if state.shutting_down {
            return Some((Vec::new(), BTreeMap::new(), BTreeMap::new()));
        }
        if state.config_revision != revision {
            return None;
        }
        state.startup_error = None;
        state.servers.clear();
        let mut stale_connect_attempts = state.cancel_all_connect_attempts();
        let stale_clients = std::mem::take(&mut state.clients.clients);
        let mut enabled = Vec::new();
        for (name, server) in &cfg.servers {
            let generation = state.bump_generation(name);
            let snapshot = if server.is_enabled() {
                starting_snapshot(name.clone(), server.clone(), McpServerStatus::Starting)
            } else {
                disabled_snapshot(name.clone(), server.clone())
            };
            state.servers.insert(name.clone(), snapshot);
            if server.is_enabled() {
                let stale_attempt = stale_connect_attempts.remove(name);
                let (attempt, replaced_attempt) = state.start_connect_attempt(name);
                debug_assert!(
                    replaced_attempt.is_none(),
                    "refresh reset 应先摘除所有旧 connect attempt"
                );
                enabled.push((
                    name.clone(),
                    server.clone(),
                    generation,
                    attempt,
                    stale_attempt,
                ));
            }
        }
        Some((enabled, stale_clients, stale_connect_attempts))
    }

    fn begin_connect_attempt(
        &self,
        server_name: &str,
        server: &McpServerConfig,
        status: McpServerStatus,
        enabled: bool,
        config_changed: bool,
    ) -> Option<ConnectStart> {
        let mut state = self.lock_state();
        if state.shutting_down {
            return None;
        }
        if config_changed {
            state.bump_config_revision();
        }
        let generation = state.bump_generation(server_name);
        let (attempt, stale_attempt) = state.start_connect_attempt(server_name);
        let stale_client = state.clients.clients.remove(server_name);
        let mut snapshot = starting_snapshot(server_name.to_string(), server.clone(), status);
        snapshot.config.enabled = Some(enabled);
        state.servers.insert(server_name.to_string(), snapshot);
        Some((generation, stale_client, attempt, stale_attempt))
    }

    fn begin_connect_attempt_if_current(
        &self,
        server_name: &str,
        server: &McpServerConfig,
        status: McpServerStatus,
        enabled: bool,
        config_changed: bool,
        expected_generation: u64,
    ) -> Option<ConnectStart> {
        let mut state = self.lock_state();
        if state.shutting_down || state.generation_for(server_name) != expected_generation {
            return None;
        }
        if config_changed {
            state.bump_config_revision();
        }
        let generation = state.bump_generation(server_name);
        let (attempt, stale_attempt) = state.start_connect_attempt(server_name);
        let stale_client = state.clients.clients.remove(server_name);
        let mut snapshot = starting_snapshot(server_name.to_string(), server.clone(), status);
        snapshot.config.enabled = Some(enabled);
        state.servers.insert(server_name.to_string(), snapshot);
        Some((generation, stale_client, attempt, stale_attempt))
    }

    async fn apply_connect_outcome(&self, mut outcome: ConnectOutcome) {
        // Drop 也会 complete，保证 apply future 在 shutdown await 中被取消时不会遗留永不完成的 fence。
        let _outcome_attempt_guard = outcome.attempt.take().map(ConnectAttemptGuard::new);
        let mut failure_watcher = None;
        let stale_client = {
            let mut state = self.lock_state();
            let outcome_is_current = !state.shutting_down
                && state.generation_for(&outcome.name) == outcome.generation
                && state
                    .servers
                    .get(&outcome.name)
                    .is_some_and(|current| current.config.is_enabled());
            if !outcome_is_current {
                outcome.client
            } else {
                state.connect_attempts.remove(&outcome.name);
                state
                    .servers
                    .insert(outcome.name.clone(), outcome.snapshot.clone());
                if let Some(client) = outcome.client {
                    failure_watcher = Some((
                        outcome.name.clone(),
                        outcome.generation,
                        Arc::downgrade(&client),
                        client.connection_failure_receiver(),
                    ));
                    state.clients.clients.insert(outcome.name, client)
                } else {
                    state.clients.clients.remove(&outcome.name)
                }
            }
        };
        if let Some((server_name, generation, client, failure)) = failure_watcher {
            spawn_connection_failure_watcher(
                Arc::downgrade(&self.state),
                Arc::clone(&self.release_gates),
                server_name,
                generation,
                client,
                failure,
            );
        }
        let _ = shutdown_client(&self.release_gates, stale_client).await;
    }

    async fn mark_server_failed_if_generation_current(
        &self,
        server_name: &str,
        expected_generation: u64,
        expected_client: &Arc<McpClient>,
        error: String,
    ) {
        let stale_client = {
            let mut state = self.lock_state();
            if state.generation_for(server_name) != expected_generation
                || !state
                    .clients
                    .clients
                    .get(server_name)
                    .is_some_and(|client| Arc::ptr_eq(client, expected_client))
            {
                return;
            }
            state.bump_generation(server_name);
            let stale_client = state.clients.clients.remove(server_name);
            if let Some(snapshot) = state.servers.get_mut(server_name) {
                snapshot.status = McpServerStatus::Failed;
                snapshot.tools.clear();
                snapshot.last_error = Some(error);
                snapshot.stderr_excerpt = None;
            }
            stale_client
        };
        let _ = shutdown_client(&self.release_gates, stale_client).await;
    }

    fn config_revision(&self) -> u64 {
        self.lock_state().config_revision
    }

    fn ensure_replacement_is_allowed(&self, server_name: &str) -> Result<(), McpManagerError> {
        self.ensure_running()?;
        if self.release_gates.contains(server_name) {
            return Err(McpManagerError::TransportReleaseTimeout {
                server: server_name.to_string(),
            });
        }
        Ok(())
    }

    fn generation_matches(&self, server_name: &str, expected_generation: u64) -> bool {
        let state = self.lock_state();
        !state.shutting_down && state.generation_for(server_name) == expected_generation
    }

    fn ready_client_matches(
        &self,
        server_name: &str,
        expected_generation: u64,
        expected_client: &Arc<McpClient>,
    ) -> bool {
        let state = self.lock_state();
        !state.shutting_down
            && state.generation_for(server_name) == expected_generation
            && state
                .servers
                .get(server_name)
                .is_some_and(|snapshot| snapshot.status == McpServerStatus::Ready)
            && state
                .clients
                .clients
                .get(server_name)
                .is_some_and(|client| Arc::ptr_eq(client, expected_client))
    }

    fn ensure_running(&self) -> Result<(), McpManagerError> {
        if self.lock_state().shutting_down {
            return Err(McpManagerError::ShuttingDown);
        }
        Ok(())
    }

    fn lock_state(&self) -> MutexGuard<'_, McpManagerState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn spawn_connection_failure_watcher(
    state: Weak<Mutex<McpManagerState>>,
    release_gates: Arc<TransportReleaseGates>,
    server_name: String,
    expected_generation: u64,
    expected_client: Weak<McpClient>,
    mut failure: tokio::sync::watch::Receiver<Option<String>>,
) {
    tokio::spawn(async move {
        let error = loop {
            if let Some(error) = failure.borrow().clone() {
                break error;
            }
            if failure.changed().await.is_err() {
                return;
            }
        };
        let Some(state) = state.upgrade() else {
            return;
        };
        let Some(expected_client) = expected_client.upgrade() else {
            return;
        };
        let stale_client = {
            let mut state = match state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if state.generation_for(&server_name) != expected_generation
                || !state
                    .clients
                    .clients
                    .get(&server_name)
                    .is_some_and(|client| Arc::ptr_eq(client, &expected_client))
            {
                return;
            }
            state.bump_generation(&server_name);
            let stale_client = state.clients.clients.remove(&server_name);
            if let Some(snapshot) = state.servers.get_mut(&server_name) {
                snapshot.status = McpServerStatus::Failed;
                snapshot.tools.clear();
                snapshot.last_error = Some(error);
                snapshot.stderr_excerpt = None;
            }
            stale_client
        };
        let _ = shutdown_client(&release_gates, stale_client).await;
    });
}

fn terminal_runtime_transition(
    server_name: &str,
    generation: u64,
    release_gates: &Arc<TransportReleaseGates>,
) -> McpRuntimeTransition {
    McpRuntimeTransition {
        server_name: server_name.to_string(),
        generation,
        stale_client: None,
        stale_connect_attempt: None,
        release_gates: Arc::clone(release_gates),
    }
}

/// 返回未在关闭窗口内释放的 server 名；调用方若要建立 replacement client 必须把它视为硬失败。
async fn shutdown_client(
    release_gates: &TransportReleaseGates,
    client: Option<Arc<McpClient>>,
) -> Option<String> {
    if let Some(client) = client {
        let server_name = client.server_name().to_string();
        release_gates.quarantine(&server_name);
        if !client.shutdown().await {
            return Some(server_name);
        }
        release_gates.confirm_released(&server_name);
    }
    None
}

fn transport_release_timeout_outcome(
    name: String,
    server: McpServerConfig,
    generation: u64,
) -> ConnectOutcome {
    ConnectOutcome {
        snapshot: failed_snapshot(
            name.clone(),
            server,
            McpManagerError::TransportReleaseTimeout {
                server: name.clone(),
            }
            .to_string(),
            None,
        ),
        name,
        generation,
        client: None,
        retryable: false,
        attempt: None,
    }
}

fn request_shutdown_client(client: Option<Arc<McpClient>>) {
    if let Some(client) = client {
        client.request_shutdown();
    }
}

fn request_shutdown_clients(clients: Vec<Arc<McpClient>>) {
    for client in clients {
        client.request_shutdown();
    }
}

fn raw_tool_is_read_only(tool: &Tool) -> bool {
    tool.annotations
        .as_ref()
        .and_then(|annotations| annotations.read_only_hint)
        == Some(true)
}

impl McpManagerState {
    fn snapshot(&self) -> McpRuntimeState {
        McpRuntimeState {
            servers: self.servers.clone(),
            generations: self.generations.clone(),
            startup_error: self.startup_error.clone(),
            workspace_root: None,
        }
    }

    fn bump_generation(&mut self, server_name: &str) -> u64 {
        let entry = self.generations.entry(server_name.to_string()).or_default();
        *entry = entry.saturating_add(1);
        *entry
    }

    fn generation_for(&self, server_name: &str) -> u64 {
        self.generations.get(server_name).copied().unwrap_or(0)
    }

    /// 取消旧 attempt，并注册当前 generation 对应的新 attempt；调用方必须在建 replacement 前 await 返回值。
    fn start_connect_attempt(
        &mut self,
        server_name: &str,
    ) -> (Arc<ConnectAttempt>, Option<Arc<ConnectAttempt>>) {
        let stale_attempt = self.cancel_connect_attempt(server_name);
        let attempt = ConnectAttempt::new();
        self.connect_attempts
            .insert(server_name.to_string(), Arc::clone(&attempt));
        (attempt, stale_attempt)
    }

    fn cancel_connect_attempt(&mut self, server_name: &str) -> Option<Arc<ConnectAttempt>> {
        // 取消后仍保留该 attempt，直到 replacement 取得它并 await 完成；否则 disable/failed
        // 后紧接 enable/reconnect 会丢失旧任务的 release fence。
        let attempt = self.connect_attempts.get(server_name).cloned();
        if let Some(attempt) = &attempt {
            attempt.cancel();
        }
        attempt
    }

    fn cancel_all_connect_attempts(&mut self) -> BTreeMap<String, Arc<ConnectAttempt>> {
        let attempts = std::mem::take(&mut self.connect_attempts);
        for attempt in attempts.values() {
            attempt.cancel();
        }
        attempts
    }

    fn bump_config_revision(&mut self) {
        self.config_revision = self.config_revision.saturating_add(1);
    }
}

impl Default for McpServerSnapshot {
    fn default() -> Self {
        Self {
            name: String::new(),
            config: McpServerConfig::streamable_http(String::new(), None),
            transport: None,
            status: McpServerStatus::Disabled,
            tools: Vec::new(),
            server_info: None,
            last_connected_at: None,
            last_error: None,
            stderr_excerpt: None,
        }
    }
}

impl McpServerSnapshot {
    pub fn discovered_tool_count(&self) -> usize {
        self.tools.len()
    }

    pub fn exposed_tool_count(&self) -> usize {
        self.tools
            .iter()
            .filter(|tool| tool.exposure == McpToolExposure::Exposed)
            .count()
    }
}

impl McpToolExposure {
    pub fn label(&self) -> &'static str {
        match self {
            McpToolExposure::Exposed => "exposed",
            McpToolExposure::Filtered { .. } => "filtered",
            McpToolExposure::Unsupported { .. } => "unsupported",
        }
    }
}

impl McpToolFilterReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DisabledTools => "disabled_tools",
            Self::NotInEnabledTools => "not_in_enabled_tools",
        }
    }
}

impl McpToolUnsupportedReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidSchema => "invalid_schema",
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_server(
    name: String,
    server: McpServerConfig,
    generation: u64,
    mcp_config_path: PathBuf,
    workspace_root: PathBuf,
    progress_callback: Option<McpProgressCallback>,
    attempt: Arc<ConnectAttempt>,
    release_gates: Arc<TransportReleaseGates>,
    oauth_refresh_activity: McpOAuthRefreshActivity,
) -> Option<ConnectOutcome> {
    let mut attempt_guard = ConnectAttemptGuard::new(Arc::clone(&attempt));
    let cancellation = attempt.cancellation.clone();
    let mut retry_index = 0;
    loop {
        if cancellation.is_cancelled() {
            return None;
        }
        let outcome = tokio::select! {
            biased;
            () = cancellation.cancelled() => return None,
            outcome = connect_server_once(
                name.clone(),
                server.clone(),
                generation,
                mcp_config_path.clone(),
                workspace_root.clone(),
                progress_callback.clone(),
                attempt.release_fence(),
                Arc::clone(&release_gates),
                oauth_refresh_activity.clone(),
            ) => outcome,
        };
        if cancellation.is_cancelled() {
            let _ = shutdown_client(&release_gates, outcome.client).await;
            return None;
        }
        if outcome.client.is_some() {
            let mut outcome = outcome;
            outcome.attempt = Some(attempt_guard.hand_off_to_outcome());
            return Some(outcome);
        }
        // 本轮建立失败时 rmcp 会析构 transport；无论准备 retry、已经达到 retry 上限，还是
        // 错误不可重试，都要先确认 close 已结束。否则后续手动 Reconnect 仍可能与旧 transport 重叠。
        attempt.wait_for_pending_transport_release().await;
        if attempt.release_failed() {
            release_gates.quarantine(&name);
            let mut outcome = outcome;
            outcome.snapshot =
                transport_release_timeout_outcome(name.clone(), server.clone(), generation)
                    .snapshot;
            outcome.retryable = false;
            outcome.attempt = Some(attempt_guard.hand_off_to_outcome());
            return Some(outcome);
        }
        if retry_index == MCP_RECONNECT_MAX_RETRIES || !outcome_is_retryable(&outcome) {
            let mut outcome = outcome;
            outcome.attempt = Some(attempt_guard.hand_off_to_outcome());
            return Some(outcome);
        }
        log::warn!(
            target: "mcp",
            "MCP server '{}' 连接失败，将在 {:?} 后重试（第 {}/{} 次额外重试）: {}",
            name,
            reconnect_backoff(retry_index),
            retry_index.saturating_add(1),
            MCP_RECONNECT_MAX_RETRIES,
            outcome.snapshot.last_error.as_deref().unwrap_or("<unknown>")
        );
        tokio::select! {
            () = cancellation.cancelled() => return None,
            () = time::sleep(reconnect_backoff(retry_index)) => {}
        }
        retry_index = retry_index.saturating_add(1);
    }
}

fn outcome_is_retryable(outcome: &ConnectOutcome) -> bool {
    outcome.retryable
}

fn reconnect_backoff(retry_index: u32) -> Duration {
    let multiplier = 1_u64.checked_shl(retry_index.min(63)).unwrap_or(u64::MAX);
    Duration::from_millis(
        MCP_RECONNECT_RETRY_BASE_DELAY_MS
            .saturating_mul(multiplier)
            .min(MCP_RECONNECT_RETRY_MAX_DELAY_MS),
    )
}

#[allow(clippy::too_many_arguments)]
async fn connect_server_once(
    name: String,
    server: McpServerConfig,
    generation: u64,
    mcp_config_path: PathBuf,
    workspace_root: PathBuf,
    progress_callback: Option<McpProgressCallback>,
    connect_release_fence: Arc<McpConnectReleaseFence>,
    release_gates: Arc<TransportReleaseGates>,
    oauth_refresh_activity: McpOAuthRefreshActivity,
) -> ConnectOutcome {
    match McpClient::connect(
        name.clone(),
        &server,
        &mcp_config_path,
        &workspace_root,
        progress_callback.clone(),
        connect_release_fence,
        McpOAuthRefreshSupervisor::new(oauth_refresh_activity),
    )
    .await
    {
        Ok(client) => {
            let client = Arc::new(client);
            match client
                .list_tools(
                    server.startup_timeout_secs(),
                    TOOLS_LIST_PAGE_LIMIT,
                    TOOLS_LIST_TOOL_LIMIT,
                )
                .await
            {
                Ok(tools) => {
                    let snapshot = ready_snapshot(name.clone(), server, &client, tools).await;
                    ConnectOutcome {
                        name,
                        generation,
                        snapshot,
                        client: Some(client),
                        retryable: false,
                        attempt: None,
                    }
                }
                Err(err) => {
                    let retryable = err.is_retryable_connection_establishment_failure();
                    let stderr_excerpt = client.stderr_excerpt().await;
                    let release_error =
                        shutdown_client(&release_gates, Some(Arc::clone(&client))).await;
                    let (message, retryable) = match release_error {
                        Some(unreleased_server) => (
                            McpManagerError::TransportReleaseTimeout {
                                server: unreleased_server,
                            }
                            .to_string(),
                            false,
                        ),
                        None => (err.to_string(), retryable),
                    };
                    ConnectOutcome {
                        name: name.clone(),
                        generation,
                        snapshot: failed_snapshot(
                            name,
                            server,
                            message,
                            non_empty_string(stderr_excerpt),
                        ),
                        client: None,
                        retryable,
                        attempt: None,
                    }
                }
            }
        }
        Err(err) => {
            let retryable = err.is_retryable_connection_establishment_failure();
            ConnectOutcome {
                name: name.clone(),
                generation,
                snapshot: failed_snapshot(name, server, err.to_string(), None),
                client: None,
                retryable,
                attempt: None,
            }
        }
    }
}

async fn ready_snapshot(
    name: String,
    server: McpServerConfig,
    client: &McpClient,
    tools: Vec<Tool>,
) -> McpServerSnapshot {
    let server_info = client.server_info();
    McpServerSnapshot {
        name: name.clone(),
        transport: server.transport_kind(&name).ok(),
        config: server.clone(),
        status: McpServerStatus::Ready,
        tools: classify_tools(&server, tools),
        server_info,
        last_connected_at: Some(Utc::now()),
        last_error: None,
        stderr_excerpt: non_empty_string(client.stderr_excerpt().await),
    }
}

fn disabled_snapshot(name: String, server: McpServerConfig) -> McpServerSnapshot {
    McpServerSnapshot {
        name: name.clone(),
        transport: server.transport_kind(&name).ok(),
        config: server,
        status: McpServerStatus::Disabled,
        tools: Vec::new(),
        server_info: None,
        last_connected_at: None,
        last_error: None,
        stderr_excerpt: None,
    }
}

fn starting_snapshot(
    name: String,
    server: McpServerConfig,
    status: McpServerStatus,
) -> McpServerSnapshot {
    McpServerSnapshot {
        name: name.clone(),
        transport: server.transport_kind(&name).ok(),
        config: server,
        status,
        tools: Vec::new(),
        server_info: None,
        last_connected_at: None,
        last_error: None,
        stderr_excerpt: None,
    }
}

fn failed_snapshot(
    name: String,
    server: McpServerConfig,
    error: String,
    stderr_excerpt: Option<String>,
) -> McpServerSnapshot {
    McpServerSnapshot {
        name: name.clone(),
        transport: server.transport_kind(&name).ok(),
        config: server,
        status: McpServerStatus::Failed,
        tools: Vec::new(),
        server_info: None,
        last_connected_at: None,
        last_error: Some(error),
        stderr_excerpt,
    }
}

fn classify_tools(server: &McpServerConfig, tools: Vec<Tool>) -> Vec<McpToolSnapshot> {
    let enabled_tools = server
        .enabled_tools
        .as_ref()
        .map(|tools| tools.iter().cloned().collect::<BTreeSet<_>>());
    let disabled_tools = server
        .disabled_tools
        .as_ref()
        .map(|tools| tools.iter().cloned().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    tools
        .into_iter()
        .map(|tool| {
            let raw_name = tool.name.to_string();
            let exposure = if let Some(enabled_tools) = &enabled_tools {
                if !enabled_tools.contains(&raw_name) {
                    McpToolExposure::Filtered {
                        reason: McpToolFilterReason::NotInEnabledTools,
                    }
                } else if disabled_tools.contains(&raw_name) {
                    McpToolExposure::Filtered {
                        reason: McpToolFilterReason::DisabledTools,
                    }
                } else if input_schema_is_invalid(&tool) {
                    McpToolExposure::Unsupported {
                        reason: McpToolUnsupportedReason::InvalidSchema,
                    }
                } else {
                    McpToolExposure::Exposed
                }
            } else if disabled_tools.contains(&raw_name) {
                McpToolExposure::Filtered {
                    reason: McpToolFilterReason::DisabledTools,
                }
            } else if input_schema_is_invalid(&tool) {
                McpToolExposure::Unsupported {
                    reason: McpToolUnsupportedReason::InvalidSchema,
                }
            } else {
                McpToolExposure::Exposed
            };
            McpToolSnapshot {
                title: tool.title.clone(),
                description: tool.description.as_ref().map(ToString::to_string),
                raw_name,
                exposure,
                raw_tool: tool,
            }
        })
        .collect()
}

fn input_schema_is_invalid(tool: &Tool) -> bool {
    let schema = tool.input_schema.as_ref();
    if serde_json::to_vec(schema)
        .map(|bytes| bytes.len() > MAX_MCP_INPUT_SCHEMA_BYTES)
        .unwrap_or(true)
    {
        return true;
    }
    match schema.get("type") {
        Some(Value::String(value)) if value == "object" => {}
        Some(_) => return true,
        None => {}
    }
    if schema
        .get("properties")
        .is_some_and(|value| !value.is_object())
    {
        return true;
    }
    schema
        .get("required")
        .is_some_and(|value| !value.is_array())
}

fn non_empty_string(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

impl McpServerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Starting => "starting",
            Self::Reconnecting => "reconnecting",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use axum::body::{Body, Bytes};
    use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
    use axum::response::sse::{Event, Sse};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use futures::{stream, StreamExt};
    use rmcp::model::JsonObject;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    use super::*;
    use crate::mcp::config::{lock_mcp_json_config, write_mcp_json_config_atomic, McpJsonConfig};

    type SeenProtocolHeaders = Arc<StdMutex<Vec<(String, Option<String>)>>>;

    #[derive(Clone, Default)]
    struct HttpConnectionMetrics {
        initialize_count: Arc<AtomicUsize>,
        list_count: Arc<AtomicUsize>,
        tool_call_count: Arc<AtomicUsize>,
        session_ids: Arc<StdMutex<Vec<String>>>,
    }

    struct ActiveSseGuard(Arc<AtomicUsize>);

    impl ActiveSseGuard {
        fn new(active: Arc<AtomicUsize>) -> Self {
            active.fetch_add(1, Ordering::SeqCst);
            Self(active)
        }
    }

    impl Drop for ActiveSseGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn classify_tools_applies_enabled_and_disabled_filters() {
        let mut server = McpServerConfig::streamable_http("https://example.com/mcp".into(), None);
        server.enabled_tools = Some(vec!["allowed".into(), "blocked".into()]);
        server.disabled_tools = Some(vec!["blocked".into()]);

        let tools = classify_tools(
            &server,
            vec![tool("allowed"), tool("blocked"), tool("other")],
        );

        assert_eq!(tools[0].exposure, McpToolExposure::Exposed);
        assert_eq!(
            tools[1].exposure,
            McpToolExposure::Filtered {
                reason: McpToolFilterReason::DisabledTools
            }
        );
        assert_eq!(
            tools[2].exposure,
            McpToolExposure::Filtered {
                reason: McpToolFilterReason::NotInEnabledTools
            }
        );
    }

    #[test]
    fn classify_tools_marks_invalid_schema_as_unsupported() {
        let server = McpServerConfig::streamable_http("https://example.com/mcp".into(), None);
        let mut invalid = tool("bad_schema");
        Arc::make_mut(&mut invalid.input_schema)
            .insert("type".to_string(), Value::String("string".to_string()));

        let tools = classify_tools(&server, vec![invalid]);

        assert_eq!(
            tools[0].exposure,
            McpToolExposure::Unsupported {
                reason: McpToolUnsupportedReason::InvalidSchema
            }
        );
    }

    #[test]
    fn classify_tools_marks_oversized_schema_as_unsupported() {
        let server = McpServerConfig::streamable_http("https://example.com/mcp".into(), None);
        let mut oversized = tool("huge_schema");
        Arc::make_mut(&mut oversized.input_schema).insert(
            "description".to_string(),
            Value::String("x".repeat(MAX_MCP_INPUT_SCHEMA_BYTES + 1)),
        );

        let tools = classify_tools(&server, vec![oversized]);

        assert_eq!(
            tools[0].exposure,
            McpToolExposure::Unsupported {
                reason: McpToolUnsupportedReason::InvalidSchema
            }
        );
    }

    #[tokio::test]
    async fn manager_connects_stdio_mock_lists_tools_and_calls_tool() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("stdio_mock.sh");
        tokio::fs::write(&script_path, stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let progress_events = Arc::new(StdMutex::new(Vec::new()));
        let captured = Arc::clone(&progress_events);
        let manager = McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            Some(Arc::new(move |event| {
                captured.lock().unwrap().push(event);
            })),
        );

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;
        let server = &snapshot.servers["stdio_server"];
        assert_eq!(
            server.status,
            McpServerStatus::Ready,
            "last_error={:?}",
            server.last_error
        );
        assert_eq!(server.exposed_tool_count(), 1);

        let routed_progress_events = Arc::new(StdMutex::new(Vec::new()));
        let routed = Arc::clone(&routed_progress_events);
        let reporter = McpToolProgressReporter::new(move |event| {
            routed.lock().unwrap().push(event);
        });
        let result = manager
            .call_tool(
                "stdio_server",
                "ping",
                Some(json!({"text": "hi"})),
                Some(reporter),
            )
            .await
            .unwrap();

        let result_json = crate::mcp::client::call_tool_result_to_json(&result);
        assert_eq!(result_json["is_error"], false);
        assert_eq!(result_json["content"][0]["text"], "pong");
        assert_eq!(progress_events.lock().unwrap().len(), 1);
        let routed = routed_progress_events.lock().unwrap();
        assert_eq!(routed.len(), 1);
        assert!(!routed[0].progress_token.is_empty());
        assert_eq!(routed[0].message.as_deref(), Some("half"));
    }

    #[tokio::test]
    async fn stdio_live_read_only_validation_and_tool_call_do_not_send_local_deadline_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("strict-stdio-mock.sh");
        let log_path = dir.path().join("stdio-requests.log");
        tokio::fs::write(&script_path, strict_stdio_no_local_deadline_meta_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_LOG".to_string(),
            log_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let result = manager
            .call_read_only_tool("stdio_server", "ping", Some(json!({"text": "hi"})), None)
            .await
            .unwrap();

        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&result)["content"][0]["text"],
            "pong"
        );
        let requests = tokio::fs::read_to_string(&log_path).await.unwrap();
        assert!(
            !requests.contains("acn.localToolDeadlineMillis"),
            "stdio MCP server must never receive ACN-local HTTP deadline metadata: {requests}"
        );
        assert_eq!(
            requests
                .lines()
                .filter(|line| line.contains("\"method\":\"tools/list\""))
                .count(),
            2,
            "initial discovery and read-only live validation must both run: {requests}"
        );
        assert_eq!(
            requests
                .lines()
                .filter(|line| line.contains("\"method\":\"tools/call\""))
                .count(),
            1,
            "read-only validation must be followed by exactly one tool call: {requests}"
        );
    }

    #[tokio::test]
    async fn stdio_normal_calls_reuse_one_child_process_and_initialize_once() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("counting_stdio_mock.sh");
        let log_path = dir.path().join("stdio-events.log");
        tokio::fs::write(&script_path, counting_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_LOG".to_string(),
            log_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        for text in ["first", "second"] {
            manager
                .call_tool("stdio_server", "ping", Some(json!({"text": text})), None)
                .await
                .unwrap();
        }

        let events = tokio::fs::read_to_string(&log_path).await.unwrap();
        let lines = events.lines().collect::<Vec<_>>();
        let initialize_pids = lines
            .iter()
            .filter_map(|line| line.strip_prefix("initialize "))
            .collect::<BTreeSet<_>>();
        assert_eq!(initialize_pids.len(), 1, "events={events}");
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("initialize "))
                .count(),
            1,
            "events={events}"
        );
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("tools/list "))
                .count(),
            1,
            "events={events}"
        );
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("tools/call "))
                .count(),
            2,
            "events={events}"
        );
    }

    #[tokio::test]
    async fn stdio_reconnect_replaces_the_shared_child_and_releases_the_old_pid() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("counting_stdio_mock.sh");
        let log_path = dir.path().join("stdio-events.log");
        tokio::fs::write(&script_path, counting_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_LOG".to_string(),
            log_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let advertised_generation = manager.snapshot_sync().generations["stdio_server"];
        manager
            .call_tool("stdio_server", "ping", Some(json!({})), None)
            .await
            .unwrap();
        let before = tokio::fs::read_to_string(&log_path).await.unwrap();
        let old_pid = before
            .lines()
            .find_map(|line| line.strip_prefix("initialize "))
            .expect("initial stdio server PID should be recorded")
            .to_string();

        let transition = manager.begin_server_reconnecting_runtime("stdio_server");
        let operation_generation = transition.generation();
        transition.wait_for_transport_release().await.unwrap();
        assert!(
            wait_for_pid_exit(&old_pid).await,
            "UI staged reconnect must release the old stdio child before creating a replacement; events={before}"
        );
        manager
            .reconnect_server_if_current("stdio_server", operation_generation)
            .await
            .unwrap();
        let stale_error = manager
            .call_tool_cancellable_for_generation(
                "stdio_server",
                "ping",
                Some(json!({"call": "stale"})),
                None,
                false,
                None,
                advertised_generation,
            )
            .await
            .expect_err("旧 Provider catalog 不得派发到 replacement generation");
        assert!(matches!(
            stale_error,
            McpManagerError::StaleToolGeneration { server } if server == "stdio_server"
        ));
        manager
            .call_tool("stdio_server", "ping", Some(json!({})), None)
            .await
            .unwrap();
        let events = tokio::fs::read_to_string(&log_path).await.unwrap();
        let pids = events
            .lines()
            .filter_map(|line| line.strip_prefix("initialize "))
            .collect::<BTreeSet<_>>();

        assert_eq!(
            events
                .lines()
                .filter(|line| line.starts_with("initialize "))
                .count(),
            2,
            "reconnect must establish exactly one replacement client; events={events}"
        );
        assert_eq!(
            pids.len(),
            2,
            "reconnect must replace the stdio child; events={events}"
        );
        assert_eq!(
            manager.snapshot().await.servers["stdio_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn reconnect_quarantines_unreleased_stdio_transport_and_settles_old_call() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("reconnect_in_flight_stdio_mock.sh");
        let started_path = dir.path().join("first-call-started");
        tokio::fs::write(&script_path, reconnect_in_flight_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_FIRST_CALL_STARTED".to_string(),
            started_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let old_call_manager = Arc::clone(&manager);
        let old_call = tokio::spawn(async move {
            old_call_manager
                .call_tool("stdio_server", "ping", Some(json!({"call": "old"})), None)
                .await
        });
        wait_for_file(&started_path).await;
        let reconnect_error = manager
            .reconnect_server("stdio_server")
            .await
            .expect_err("a blocked stdio transport must not be replaced before it exits");
        let old_error = time::timeout(Duration::from_secs(2), old_call)
            .await
            .expect("reconnect must settle the old in-flight call")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            old_error.contains("Transport closed") || old_error.contains("取消"),
            "old in-flight call should be cancelled by lifecycle reconnect: {old_error}"
        );
        assert!(matches!(
            reconnect_error,
            McpManagerError::TransportReleaseTimeout { ref server } if server == "stdio_server"
        ));
        assert!(matches!(
            manager.reconnect_server("stdio_server").await,
            Err(McpManagerError::TransportReleaseTimeout { ref server }) if server == "stdio_server"
        ));
        assert!(matches!(
            manager
                .call_tool("stdio_server", "ping", Some(json!({"call": "fresh"})), None)
                .await,
            Err(McpManagerError::ServerNotReady(server)) if server == "stdio_server"
        ));
        assert_eq!(
            manager.snapshot().await.servers["stdio_server"].status,
            McpServerStatus::Reconnecting,
            "quarantined transport must not install a replacement generation"
        );
    }

    #[tokio::test]
    async fn lifecycle_mark_prevents_a_captured_old_client_from_sending_regular_tool_call() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("counting_stdio_mock.sh");
        let log_path = dir.path().join("stdio-events.log");
        tokio::fs::write(&script_path, counting_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_LOG".to_string(),
            log_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let captured_client = manager
            .lock_state()
            .clients
            .clients
            .get("stdio_server")
            .cloned()
            .expect("ready server should install a shared client");
        manager
            .begin_server_reconnecting_runtime("stdio_server")
            .wait_for_transport_release()
            .await
            .unwrap();
        let error = captured_client
            .call_tool("ping", Some(json!({"effect": "must-not-send"})), None)
            .await
            .unwrap_err()
            .to_string();
        let events = tokio::fs::read_to_string(&log_path).await.unwrap();

        assert!(error.contains("lifecycle was replaced or disabled"));
        assert!(
            !events.lines().any(|line| line.starts_with("tools/call ")),
            "a captured pre-lifecycle client must not send a regular tools/call; events={events}"
        );
    }

    #[tokio::test]
    async fn runtime_reconnect_mark_releases_cancelled_http_transport_before_config_work() {
        let initialize_count = Arc::new(AtomicUsize::new(0));
        let handler_initialize_count = Arc::clone(&initialize_count);
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let initialize_count = Arc::clone(&handler_initialize_count);
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    async move {
                        blocking_first_http_tool_mcp(payload, initialize_count, first_tool_started)
                            .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(10);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let old_call_manager = Arc::clone(&manager);
        let old_call = tokio::spawn(async move {
            old_call_manager
                .call_tool("http_server", "ping", Some(json!({"text": "old"})), None)
                .await
        });
        waiting_for_first_request.await;

        let transition = manager.begin_server_reconnecting_runtime("http_server");
        let operation_generation = transition.generation();
        let old_result = time::timeout(Duration::from_secs(2), old_call)
            .await
            .expect("UI reconnect mark must immediately settle the old HTTP call")
            .unwrap();
        assert!(
            old_result.is_err(),
            "old HTTP call must not return a success after the UI lifecycle generation changes"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Reconnecting
        );
        // lifecycle cancellation 现在会中止正在等待 HTTP response headers 的 reqwest request，
        // 因而不应把已经真实释放的 transport 误 quarantine。真正无法释放的 delete-session
        // transport 仍由下一条 `runtime_transition_reports_unreleased_http_transport_as_hard_error`
        // 覆盖。
        transition.wait_for_transport_release().await.unwrap();
        manager
            .reconnect_server_if_current("http_server", operation_generation)
            .await
            .unwrap();
        assert_eq!(initialize_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn runtime_transition_reports_unreleased_http_transport_as_hard_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(http_mcp).get(http_sse).delete(hanging_delete_http_mcp),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let transition = manager.begin_server_reconnecting_runtime("http_server");
        let operation_generation = transition.generation();
        let error = transition
            .wait_for_transport_release()
            .await
            .expect_err("unresponsive DELETE must prevent a replacement session");

        assert!(matches!(
            error,
            McpManagerError::TransportReleaseTimeout { ref server } if server == "http_server"
        ));
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Reconnecting
        );
        assert!(matches!(
            manager
                .reconnect_server_if_current("http_server", operation_generation)
                .await,
            Err(McpManagerError::TransportReleaseTimeout { ref server }) if server == "http_server"
        ));
        assert!(matches!(
            manager
                .call_tool("http_server", "ping", Some(json!({})), None)
                .await,
            Err(McpManagerError::ServerNotReady(server)) if server == "http_server"
        ));
    }

    #[tokio::test]
    async fn refresh_with_unreleased_transport_keeps_other_server_available() {
        let blocked_initializes = Arc::new(AtomicUsize::new(0));
        let blocked_counter = Arc::clone(&blocked_initializes);
        let blocked_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blocked_addr = blocked_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let blocked_counter = Arc::clone(&blocked_counter);
                    async move {
                        if payload.get("method").and_then(Value::as_str) == Some("initialize") {
                            blocked_counter.fetch_add(1, Ordering::SeqCst);
                        }
                        http_mcp(Json(payload)).await.into_response()
                    }
                })
                .get(http_sse)
                .delete(hanging_delete_http_mcp),
            );
            axum::serve(blocked_listener, app).await.unwrap();
        });
        let healthy_initializes = Arc::new(AtomicUsize::new(0));
        let healthy_counter = Arc::clone(&healthy_initializes);
        let healthy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_addr = healthy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let healthy_counter = Arc::clone(&healthy_counter);
                    async move {
                        if payload.get("method").and_then(Value::as_str) == Some("initialize") {
                            healthy_counter.fetch_add(1, Ordering::SeqCst);
                        }
                        http_mcp(Json(payload)).await.into_response()
                    }
                })
                .get(http_sse)
                .delete(clean_delete_http_mcp),
            );
            axum::serve(healthy_listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "blocked".to_string(),
            McpServerConfig::streamable_http(format!("http://{blocked_addr}/mcp"), None),
        );
        cfg.servers.insert(
            "healthy".to_string(),
            McpServerConfig::streamable_http(format!("http://{healthy_addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh_all().await });
        time::timeout(Duration::from_secs(2), async {
            loop {
                if healthy_initializes.load(Ordering::SeqCst) == 2
                    && manager.snapshot().await.servers["healthy"].status == McpServerStatus::Ready
                {
                    return;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("healthy server must become ready before another server's release timeout");
        assert!(
            !refresh.is_finished(),
            "blocked transport should still be inside its bounded shutdown window"
        );
        manager
            .call_tool("healthy", "ping", Some(json!({})), None)
            .await
            .unwrap();
        refresh.await.unwrap().unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(snapshot.servers["blocked"].status, McpServerStatus::Failed);
        assert!(snapshot.servers["blocked"]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("旧 transport 未在关闭窗口内确认释放")));
        assert_eq!(blocked_initializes.load(Ordering::SeqCst), 1);
        assert_eq!(healthy_initializes.load(Ordering::SeqCst), 2);
        assert_eq!(snapshot.servers["healthy"].status, McpServerStatus::Ready);
    }

    #[tokio::test]
    async fn disable_cancels_in_flight_http_call_and_leaves_no_ready_session() {
        let initialize_count = Arc::new(AtomicUsize::new(0));
        let handler_initialize_count = Arc::clone(&initialize_count);
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let initialize_count = Arc::clone(&handler_initialize_count);
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    async move {
                        blocking_first_http_tool_mcp(payload, initialize_count, first_tool_started)
                            .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(10);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let old_call_manager = Arc::clone(&manager);
        let old_call = tokio::spawn(async move {
            old_call_manager
                .call_tool("http_server", "ping", Some(json!({"text": "old"})), None)
                .await
        });
        waiting_for_first_request.await;

        manager.disable_server("http_server").await.unwrap();
        let old_result = time::timeout(Duration::from_secs(2), old_call)
            .await
            .expect("disable must settle the old HTTP call")
            .unwrap();
        assert!(
            old_result.is_err(),
            "old HTTP call must not return a success after disable"
        );
        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Disabled
        );
        assert_eq!(initialize_count.load(Ordering::SeqCst), 1);
        assert!(matches!(
            manager
                .call_tool("http_server", "ping", Some(json!({"text": "after"})), None)
                .await,
            Err(McpManagerError::ServerNotReady(_))
        ));
    }

    #[tokio::test]
    async fn stale_ui_operation_error_does_not_fail_a_new_ready_generation() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("counting_stdio_mock.sh");
        let log_path = dir.path().join("stdio-events.log");
        tokio::fs::write(&script_path, counting_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_LOG".to_string(),
            log_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let transition = manager.begin_server_reconnecting_runtime("stdio_server");
        let stale_operation_generation = transition.generation();
        transition.wait_for_transport_release().await.unwrap();
        manager
            .reconnect_server_if_current("stdio_server", stale_operation_generation)
            .await
            .unwrap();
        assert_eq!(
            manager.snapshot().await.servers["stdio_server"].status,
            McpServerStatus::Ready
        );

        manager.mark_server_failed_runtime_if_current(
            "stdio_server",
            stale_operation_generation,
            "late UI operation error",
        );
        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["stdio_server"].status,
            McpServerStatus::Ready
        );
        assert_eq!(snapshot.servers["stdio_server"].exposed_tool_count(), 1);
    }

    #[tokio::test]
    async fn http_normal_calls_reuse_one_session_without_relisting_tools() {
        let metrics = HttpConnectionMetrics::default();
        let handler_metrics = metrics.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                    let metrics = handler_metrics.clone();
                    async move { tracked_http_mcp(headers, payload, metrics).await }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        for text in ["first", "second"] {
            manager
                .call_tool("http_server", "ping", Some(json!({"text": text})), None)
                .await
                .unwrap();
        }

        assert_eq!(metrics.initialize_count.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.list_count.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.tool_call_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            metrics.session_ids.lock().unwrap().as_slice(),
            ["test-session", "test-session", "test-session"]
        );
    }

    #[tokio::test]
    async fn concurrent_progress_events_route_to_their_own_shared_client_callers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let all_events = Arc::new(StdMutex::new(Vec::new()));
        let captured_all_events = Arc::clone(&all_events);
        let manager = McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            Some(Arc::new(move |event| {
                captured_all_events.lock().unwrap().push(event);
            })),
        );
        manager.refresh_all().await.unwrap();

        let first_events = Arc::new(StdMutex::new(Vec::new()));
        let first_captured = Arc::clone(&first_events);
        let first_reporter = McpToolProgressReporter::new(move |event| {
            first_captured.lock().unwrap().push(event);
        });
        let second_events = Arc::new(StdMutex::new(Vec::new()));
        let second_captured = Arc::clone(&second_events);
        let second_reporter = McpToolProgressReporter::new(move |event| {
            second_captured.lock().unwrap().push(event);
        });

        let (first, second) = tokio::join!(
            manager.call_tool(
                "http_server",
                "ping",
                Some(json!({"request": "first"})),
                Some(first_reporter),
            ),
            manager.call_tool(
                "http_server",
                "ping",
                Some(json!({"request": "second"})),
                Some(second_reporter),
            )
        );

        assert!(first.is_ok());
        assert!(second.is_ok());
        let first_events = first_events.lock().unwrap();
        let second_events = second_events.lock().unwrap();
        assert_eq!(first_events.len(), 1);
        assert_eq!(second_events.len(), 1);
        assert_ne!(
            first_events[0].progress_token,
            second_events[0].progress_token
        );
        assert_eq!(first_events[0].message.as_deref(), Some("half"));
        assert_eq!(second_events[0].message.as_deref(), Some("half"));
        assert_eq!(all_events.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn manager_connects_streamable_http_mock_and_lists_tools() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;
        let server = &snapshot.servers["http_server"];

        assert_eq!(
            server.status,
            McpServerStatus::Ready,
            "last_error={:?}",
            server.last_error
        );
        assert_eq!(server.exposed_tool_count(), 1);
        assert_eq!(server.tools[0].raw_name, "ping");

        let routed_progress_events = Arc::new(StdMutex::new(Vec::new()));
        let routed = Arc::clone(&routed_progress_events);
        let reporter = McpToolProgressReporter::new(move |event| {
            routed.lock().unwrap().push(event);
        });
        let result = manager
            .call_tool(
                "http_server",
                "ping",
                Some(json!({"text": "hi"})),
                Some(reporter),
            )
            .await
            .unwrap();
        let result_json = crate::mcp::client::call_tool_result_to_json(&result);
        assert_eq!(result_json["content"][0]["text"], "pong");
        let routed = routed_progress_events.lock().unwrap();
        assert_eq!(routed.len(), 1);
        assert!(!routed[0].progress_token.is_empty());
        assert_eq!(routed[0].message.as_deref(), Some("half"));
    }

    #[tokio::test]
    async fn streamable_http_falls_back_to_legacy_protocol_when_discover_is_unsupported() {
        let seen_headers = Arc::new(StdMutex::new(Vec::<(String, Option<String>)>::new()));
        let handler_headers = Arc::clone(&seen_headers);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                    let handler_headers = Arc::clone(&handler_headers);
                    async move { negotiated_protocol_http_mcp(headers, payload, handler_headers) }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;

        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready
        );
        let seen = seen_headers.lock().unwrap();
        assert!(seen.iter().any(|(method, header)| {
            method == "server/discover" && header.as_deref() == Some("2026-07-28")
        }));
        assert!(seen.iter().any(|(method, header)| {
            method == "initialize" && header.as_deref() == Some("2025-11-25")
        }));
        assert!(seen.iter().any(|(method, header)| {
            method == "notifications/initialized" && header.as_deref() == Some("2025-06-18")
        }));
        assert!(seen.iter().any(|(method, header)| {
            method == "tools/list" && header.as_deref() == Some("2025-06-18")
        }));
    }

    #[tokio::test]
    async fn streamable_http_falls_back_when_legacy_server_rejects_discover_at_http_layer() {
        let seen_methods = Arc::new(StdMutex::new(Vec::<String>::new()));
        let handler_methods = Arc::clone(&seen_methods);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move |Json(payload): Json<Value>| {
                        let handler_methods = Arc::clone(&handler_methods);
                        async move {
                            let method = payload
                                .get("method")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            handler_methods.lock().unwrap().push(method.clone());
                            if method == "server/discover" {
                                return (
                                    StatusCode::BAD_REQUEST,
                                    "unsupported MCP protocol version",
                                )
                                    .into_response();
                            }
                            http_mcp(Json(payload)).await.into_response()
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready,
            "HTTP 层拒绝 discover 后应回退 initialize，last_error={:?}",
            snapshot.servers["http_server"].last_error
        );
        let seen = seen_methods.lock().unwrap();
        assert_eq!(
            seen.iter()
                .filter(|method| method.as_str() == "server/discover")
                .count(),
            1
        );
        assert!(seen.iter().any(|method| method == "initialize"));
        server.abort();
    }

    #[tokio::test]
    async fn streamable_http_preserves_explicit_discover_json_rpc_error_without_downgrade() {
        let initialize_count = Arc::new(AtomicUsize::new(0));
        let handler_initialize_count = Arc::clone(&initialize_count);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move |Json(payload): Json<Value>| {
                        let initialize_count = Arc::clone(&handler_initialize_count);
                        async move {
                            let id = payload.get("id").cloned().unwrap_or(Value::Null);
                            match payload.get("method").and_then(Value::as_str) {
                                Some("server/discover") => (
                                    StatusCode::BAD_REQUEST,
                                    Json(json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "error": {
                                            "code": -32603,
                                            "message": "discovery failed"
                                        }
                                    })),
                                )
                                    .into_response(),
                                Some("initialize") => {
                                    initialize_count.fetch_add(1, Ordering::SeqCst);
                                    http_mcp(Json(payload)).await.into_response()
                                }
                                _ => http_mcp(Json(payload)).await.into_response(),
                            }
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Failed
        );
        assert_eq!(initialize_count.load(Ordering::SeqCst), 0);
        server.abort();
    }

    #[tokio::test]
    async fn streamable_http_accepts_empty_ok_for_legacy_initialized_notification() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(|Json(payload): Json<Value>| async move {
                        let id = payload.get("id").cloned().unwrap_or(Value::Null);
                        match payload.get("method").and_then(Value::as_str) {
                            Some("server/discover") => {
                                (StatusCode::NOT_FOUND, "legacy endpoint").into_response()
                            }
                            Some("initialize") => {
                                let mut headers = HeaderMap::new();
                                headers.insert(
                                    "Mcp-Session-Id",
                                    HeaderValue::from_static("legacy-session"),
                                );
                                (
                                    headers,
                                    Json(json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "protocolVersion": "2025-11-25",
                                            "capabilities": {"tools": {}},
                                            "serverInfo": {"name": "legacy", "version": "1.0.0"}
                                        }
                                    })),
                                )
                                    .into_response()
                            }
                            Some("notifications/initialized") => StatusCode::OK.into_response(),
                            Some("tools/list") => Json(json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {"tools": []}
                            }))
                            .into_response(),
                            _ => StatusCode::BAD_REQUEST.into_response(),
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready,
            "legacy initialized 返回空 200 时仍应完成启动，last_error={:?}",
            snapshot.servers["http_server"].last_error
        );
        server.abort();
    }

    #[tokio::test]
    async fn streamable_http_reinitializes_after_session_expired_response() {
        let initialize_count = Arc::new(AtomicUsize::new(0));
        let tool_call_count = Arc::new(AtomicUsize::new(0));
        let handler_initialize_count = Arc::clone(&initialize_count);
        let handler_tool_call_count = Arc::clone(&tool_call_count);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                        let initialize_count = Arc::clone(&handler_initialize_count);
                        let tool_call_count = Arc::clone(&handler_tool_call_count);
                        async move {
                            let id = payload.get("id").cloned().unwrap_or(Value::Null);
                            let method = payload
                                .get("method")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if method == "server/discover" {
                                return Json(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "error": {"code": -32601, "message": "Method not found"}
                                }))
                                .into_response();
                            }
                            if method == "initialize" {
                                let generation = initialize_count.fetch_add(1, Ordering::SeqCst) + 1;
                                let session_id = format!("session-{generation}");
                                let mut response_headers = HeaderMap::new();
                                response_headers.insert(
                                    "Mcp-Session-Id",
                                    HeaderValue::from_str(&session_id).unwrap(),
                                );
                                return (
                                    response_headers,
                                    Json(json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "protocolVersion": "2025-11-25",
                                            "capabilities": {"tools": {}},
                                            "serverInfo": {"name": "session-test", "version": "1.0.0"}
                                        }
                                    })),
                                )
                                    .into_response();
                            }
                            if method == "notifications/initialized" {
                                return StatusCode::ACCEPTED.into_response();
                            }
                            if method == "tools/list" {
                                return Json(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": {"tools": [{
                                        "name": "ping",
                                        "description": "Ping tool",
                                        "inputSchema": {"type": "object"}
                                    }]}
                                }))
                                .into_response();
                            }
                            if method == "tools/call" {
                                tool_call_count.fetch_add(1, Ordering::SeqCst);
                                let session_id = headers
                                    .get("mcp-session-id")
                                    .and_then(|value| value.to_str().ok());
                                if session_id == Some("session-1") {
                                    return StatusCode::NOT_FOUND.into_response();
                                }
                                return Json(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": {
                                        "content": [{"type": "text", "text": "pong"}],
                                        "isError": false
                                    }
                                }))
                                .into_response();
                            }
                            StatusCode::BAD_REQUEST.into_response()
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);
        manager.refresh_all().await.unwrap();

        manager
            .call_tool("http_server", "ping", Some(json!({})), None)
            .await
            .unwrap();

        assert_eq!(initialize_count.load(Ordering::SeqCst), 2);
        assert_eq!(tool_call_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
        server.abort();
    }

    #[tokio::test]
    async fn expired_session_reinitialize_timeout_fails_shared_connection() {
        let initialize_count = Arc::new(AtomicUsize::new(0));
        let handler_initialize_count = Arc::clone(&initialize_count);
        let reinitialize_started = Arc::new(Notify::new());
        let handler_reinitialize_started = Arc::clone(&reinitialize_started);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                        let initialize_count = Arc::clone(&handler_initialize_count);
                        let reinitialize_started = Arc::clone(&handler_reinitialize_started);
                        async move {
                            let id = payload.get("id").cloned().unwrap_or(Value::Null);
                            let method = payload
                                .get("method")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if method == "server/discover" {
                                return Json(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "error": {"code": -32601, "message": "Method not found"}
                                }))
                                .into_response();
                            }
                            if method == "initialize" {
                                let generation =
                                    initialize_count.fetch_add(1, Ordering::SeqCst) + 1;
                                if generation > 1 {
                                    reinitialize_started.notify_one();
                                    return Sse::new(stream::pending::<Result<Event, Infallible>>())
                                        .into_response();
                                }
                                let mut response_headers = HeaderMap::new();
                                response_headers.insert(
                                    "Mcp-Session-Id",
                                    HeaderValue::from_static("session-1"),
                                );
                                return (
                                    response_headers,
                                    Json(json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "protocolVersion": "2025-11-25",
                                            "capabilities": {"tools": {}},
                                            "serverInfo": {"name": "session-test", "version": "1.0.0"}
                                        }
                                    })),
                                )
                                    .into_response();
                            }
                            if method == "notifications/initialized" {
                                return StatusCode::ACCEPTED.into_response();
                            }
                            if method == "tools/list" {
                                return Json(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": {"tools": [{
                                        "name": "ping",
                                        "description": "Ping tool",
                                        "inputSchema": {"type": "object"}
                                    }]}
                                }))
                                .into_response();
                            }
                            if method == "tools/call"
                                && headers
                                    .get("mcp-session-id")
                                    .and_then(|value| value.to_str().ok())
                                    == Some("session-1")
                            {
                                return StatusCode::NOT_FOUND.into_response();
                            }
                            StatusCode::BAD_REQUEST.into_response()
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut mcp_server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        mcp_server.startup_timeout_secs = Some(1);
        mcp_server.tool_timeout_secs = Some(5);
        cfg.servers.insert("http_server".to_string(), mcp_server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));
        manager.refresh_all().await.unwrap();

        let cancellation = CancellationToken::new();
        let call = tokio::spawn({
            let manager = Arc::clone(&manager);
            let cancellation = cancellation.clone();
            async move {
                manager
                    .call_tool_cancellable(
                        "http_server",
                        "ping",
                        Some(json!({})),
                        None,
                        Some(cancellation),
                    )
                    .await
            }
        });
        reinitialize_started.notified().await;
        cancellation.cancel();
        let error = time::timeout(Duration::from_millis(500), call)
            .await
            .expect("caller 取消不应等待重建握手超时")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(error.contains("cancelled"), "unexpected error: {error}");

        time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.snapshot().await.servers["http_server"].status == McpServerStatus::Failed
                {
                    break;
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("caller 已取消时，重建 lifecycle timeout 仍必须淘汰共享连接");

        assert_eq!(initialize_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Failed
        );
        server.abort();
    }

    #[tokio::test]
    async fn streamable_http_uses_discovered_protocol_version_without_initialize() {
        let seen_headers = Arc::new(StdMutex::new(Vec::<(String, Option<String>)>::new()));
        let handler_headers = Arc::clone(&seen_headers);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                    let handler_headers = Arc::clone(&handler_headers);
                    async move { discovered_protocol_http_mcp(headers, payload, handler_headers) }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready
        );
        let seen = seen_headers.lock().unwrap();
        assert!(seen.iter().any(|(method, header)| {
            method == "server/discover" && header.as_deref() == Some("2026-07-28")
        }));
        assert!(seen.iter().any(|(method, header)| {
            method == "tools/list" && header.as_deref() == Some("2026-07-28")
        }));
        assert!(!seen.iter().any(|(method, _)| method == "initialize"));
    }

    #[tokio::test]
    async fn streamable_http_auth_challenge_is_reported_clearly_during_initialize() {
        let initialize_count = Arc::new(AtomicUsize::new(0));
        let handler_initialize_count = Arc::clone(&initialize_count);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move || {
                        let initialize_count = Arc::clone(&handler_initialize_count);
                        async move {
                            initialize_count.fetch_add(1, Ordering::SeqCst);
                            auth_required_http_mcp().await
                        }
                    })
                    .get(http_sse),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;
        let server = &snapshot.servers["http_server"];

        assert_eq!(server.status, McpServerStatus::Failed);
        let error = server.last_error.as_deref().unwrap_or_default();
        assert!(error.contains("acn mcp login http_server"));
        assert!(error.contains("Bearer resource_metadata"));
        assert!(!error.contains("secret"));
        assert_eq!(
            initialize_count.load(Ordering::SeqCst),
            1,
            "401 initialize 属于认证/配置问题，不能触发内部连接重试"
        );
    }

    #[tokio::test]
    async fn missing_bearer_token_does_not_enter_backoff_retry() {
        const MISSING_TOKEN_ENV: &str = "ACN_MCP_TEST_MISSING_BEARER_TOKEN_9D916A3B";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(
                "http://127.0.0.1:9/mcp".to_string(),
                Some(MISSING_TOKEN_ENV.to_string()),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        time::timeout(Duration::from_millis(100), manager.refresh_all())
            .await
            .expect("缺 token 应立即失败，不能进入 200ms 的首次退避")
            .unwrap();

        let error = manager.snapshot().await.servers["http_server"]
            .last_error
            .clone()
            .unwrap_or_default();
        assert!(error.contains(MISSING_TOKEN_ENV));
    }

    #[tokio::test]
    async fn streamable_http_auth_challenge_during_tool_call_keeps_shared_client_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(auth_required_tool_call_http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let err = manager
            .call_tool("http_server", "ping", Some(json!({"text": "denied"})), None)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("acn mcp login http_server"));
        assert!(err.contains("Bearer resource_metadata"));
        assert!(!err.contains("secret"));
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready,
            "单个 HTTP POST 的认证失败不能摘除仍可用的共享 session"
        );
        let ping = manager
            .call_tool(
                "http_server",
                "ping",
                Some(json!({"text": "allowed"})),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&ping)["content"][0]["text"],
            "pong"
        );
    }

    #[test]
    fn cancelling_connect_attempt_publishes_release_barrier_before_waking_task() {
        struct CancellationWakeProbe {
            release_fence: Arc<McpConnectReleaseFence>,
            woke_after_barrier: std::sync::atomic::AtomicBool,
        }

        impl std::task::Wake for CancellationWakeProbe {
            fn wake(self: Arc<Self>) {
                self.woke_after_barrier.store(
                    self.release_fence.cancellation_requested_for_test(),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
        }

        let attempt = ConnectAttempt::new();
        let cancellation = attempt.cancellation.clone();
        let mut cancelled = Box::pin(cancellation.cancelled());
        let probe = Arc::new(CancellationWakeProbe {
            release_fence: attempt.release_fence(),
            woke_after_barrier: std::sync::atomic::AtomicBool::new(false),
        });
        let waker = std::task::Waker::from(Arc::clone(&probe));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(matches!(
            std::future::Future::poll(cancelled.as_mut(), &mut context),
            std::task::Poll::Pending
        ));

        attempt.cancel();

        assert!(
            probe
                .woke_after_barrier
                .load(std::sync::atomic::Ordering::SeqCst),
            "connect task was woken before the active transport release barrier was published"
        );
    }

    #[tokio::test]
    async fn shutdown_blocks_a_gated_runtime_reconnect_from_starting_late() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let marker = dir.path().join("late-connect");
        let script = dir.path().join("late-connect.sh");
        tokio::fs::write(&script, "touch \"$1\"\nexit 1\n")
            .await
            .unwrap();
        let server = McpServerConfig::stdio(
            "sh".to_string(),
            vec![script.display().to_string(), marker.display().to_string()],
            BTreeMap::new(),
            Vec::new(),
        );
        let mut cfg = McpJsonConfig::default();
        cfg.servers
            .insert("stdio_server".to_string(), server.clone());
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));
        {
            let mut state = manager.lock_state();
            state.bump_generation("stdio_server");
            state.servers.insert(
                "stdio_server".to_string(),
                starting_snapshot(
                    "stdio_server".to_string(),
                    server,
                    McpServerStatus::Starting,
                ),
            );
        }
        let transition = manager.begin_server_reconnecting_runtime("stdio_server");
        let generation = transition.generation();
        let release = Arc::new(Notify::new());
        let operation_manager = Arc::clone(&manager);
        let operation_release = Arc::clone(&release);
        let operation = tokio::spawn(async move {
            operation_release.notified().await;
            transition.wait_for_transport_release().await.unwrap();
            operation_manager
                .reconnect_server_if_current("stdio_server", generation)
                .await
        });

        manager.shutdown().await;
        release.notify_one();
        operation.await.unwrap().unwrap();

        assert!(!marker.exists());
        let state = manager.lock_state();
        assert!(state.shutting_down);
        assert!(state.clients.clients.is_empty());
        assert!(state.connect_attempts.is_empty());
    }

    #[tokio::test]
    async fn shutdown_finishes_when_lifecycle_operation_waits_for_config_file_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec!["-c".to_string(), "exit 0".to_string()],
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let held_file_lock = lock_mcp_json_config(&path).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));
        let operation_manager = Arc::clone(&manager);
        let operation =
            tokio::spawn(async move { operation_manager.disable_server("stdio_server").await });
        time::sleep(Duration::from_millis(100)).await;
        assert!(
            !operation.is_finished(),
            "disable 必须已进入跨进程配置锁等待"
        );

        time::timeout(Duration::from_secs(3), manager.shutdown())
            .await
            .expect("shutdown 不应被等待跨进程配置锁的 lifecycle 操作永久阻塞");
        let error = operation
            .await
            .unwrap()
            .expect_err("跨进程配置锁等待应在有限时间后失败");
        assert!(matches!(
            error,
            McpManagerError::Config(McpConfigError::WriteLock { .. })
        ));
        drop(held_file_lock);

        let state = manager.lock_state();
        assert!(state.shutting_down);
        assert!(state.clients.clients.is_empty());
        assert!(state.connect_attempts.is_empty());
    }

    #[tokio::test]
    async fn disabled_server_does_not_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::stdio(
            "definitely-not-a-real-mcp-command".to_string(),
            Vec::new(),
            BTreeMap::new(),
            Vec::new(),
        );
        server.enabled = Some(false);
        cfg.servers.insert("disabled".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;

        assert_eq!(
            snapshot.servers["disabled"].status,
            McpServerStatus::Disabled
        );
        assert!(snapshot.servers["disabled"].last_error.is_none());
    }

    #[tokio::test]
    async fn stale_ui_enable_after_disable_does_not_reenable_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http("https://example.test/mcp".into(), None);
        server.enabled = Some(false);
        cfg.servers
            .insert("stdio_server".to_string(), server.clone());
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path.clone(), dir.path().to_path_buf(), None);
        {
            let mut state = manager.lock_state();
            state.servers.insert(
                "stdio_server".to_string(),
                disabled_snapshot("stdio_server".to_string(), server),
            );
        }

        let stale_transition = manager.begin_server_reconnecting_runtime("stdio_server");
        let stale_generation = stale_transition.generation();
        let disabled_transition = manager.begin_server_disabled_runtime("stdio_server");
        stale_transition.wait_for_transport_release().await.unwrap();
        disabled_transition
            .wait_for_transport_release()
            .await
            .unwrap();
        manager
            .enable_server_if_current("stdio_server", stale_generation)
            .await
            .unwrap();

        let cfg = read_mcp_json_config(&path).await.unwrap();
        let snapshot = manager.snapshot().await;
        assert!(!cfg.servers["stdio_server"].is_enabled());
        assert_eq!(
            snapshot.servers["stdio_server"].status,
            McpServerStatus::Disabled
        );
        assert!(snapshot.servers["stdio_server"].tools.is_empty());
    }

    #[tokio::test]
    async fn stale_ui_reconnect_after_disable_does_not_change_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let server = McpServerConfig::streamable_http("https://example.test/mcp".into(), None);
        cfg.servers
            .insert("stdio_server".to_string(), server.clone());
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);
        {
            let mut state = manager.lock_state();
            state.servers.insert(
                "stdio_server".to_string(),
                starting_snapshot("stdio_server".to_string(), server, McpServerStatus::Ready),
            );
        }

        let stale_transition = manager.begin_server_reconnecting_runtime("stdio_server");
        let stale_generation = stale_transition.generation();
        let disabled_transition = manager.begin_server_disabled_runtime("stdio_server");
        stale_transition.wait_for_transport_release().await.unwrap();
        disabled_transition
            .wait_for_transport_release()
            .await
            .unwrap();
        manager
            .reconnect_server_if_current("stdio_server", stale_generation)
            .await
            .unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["stdio_server"].status,
            McpServerStatus::Disabled
        );
        assert!(snapshot.servers["stdio_server"].tools.is_empty());
    }

    #[tokio::test]
    async fn enabled_server_failure_is_recorded_without_failing_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "broken".to_string(),
            McpServerConfig::stdio(
                "definitely-not-a-real-mcp-command".to_string(),
                Vec::new(),
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;

        assert_eq!(snapshot.servers["broken"].status, McpServerStatus::Failed);
        assert_eq!(snapshot.servers["broken"].exposed_tool_count(), 0);
        assert!(snapshot.servers["broken"].last_error.is_some());
    }

    #[tokio::test]
    async fn connection_establishment_retries_with_internal_backoff_without_tool_replay() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("fail_once_stdio_mock.sh");
        let attempts_path = dir.path().join("connection-attempts.txt");
        tokio::fs::write(&script_path, fail_once_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_ATTEMPTS".to_string(),
            attempts_path.display().to_string(),
        );
        cfg.servers.insert(
            "retry_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        assert_eq!(
            tokio::fs::read_to_string(&attempts_path)
                .await
                .unwrap()
                .trim(),
            "2",
            "首次连接失败后应由 config.rs 内部退避参数重试一次"
        );
        assert_eq!(
            manager.snapshot().await.servers["retry_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn http_discovery_connect_failure_retries_connection_establishment() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = first_listener.local_addr().unwrap();
        let close_first_server = CancellationToken::new();
        let first_server_shutdown = close_first_server.clone();
        let app = Router::new().route(
            "/mcp",
            post(move |Json(payload): Json<Value>| {
                let shutdown = first_server_shutdown.clone();
                async move {
                    if payload.get("method").and_then(Value::as_str) == Some("initialize") {
                        let mut response = http_mcp(Json(payload)).await.into_response();
                        response
                            .headers_mut()
                            .insert(header::CONNECTION, HeaderValue::from_static("close"));
                        // initialize 响应仍会由 graceful shutdown 完整写出；随后 listener 不再
                        // 接受 tools/list 的新连接，从而稳定触发 discovery 的连接失败。
                        shutdown.cancel();
                        return response;
                    }
                    http_mcp(Json(payload)).await.into_response()
                }
            })
            .get(http_sse),
        );
        let first_server_wait = close_first_server.clone();
        tokio::spawn(async move {
            axum::serve(first_listener, app)
                .with_graceful_shutdown(async move { first_server_wait.cancelled().await })
                .await
                .unwrap();
        });

        let replacement_wait = close_first_server.clone();
        let replacement_initializes = Arc::new(AtomicUsize::new(0));
        let replacement_initialize_counter = Arc::clone(&replacement_initializes);
        tokio::spawn(async move {
            replacement_wait.cancelled().await;
            loop {
                match TcpListener::bind(addr).await {
                    Ok(listener) => {
                        let initialize_counter = Arc::clone(&replacement_initialize_counter);
                        axum::serve(
                            listener,
                            Router::new().route(
                                "/mcp",
                                post(move |Json(payload): Json<Value>| {
                                    let initialize_counter = Arc::clone(&initialize_counter);
                                    async move {
                                        if payload.get("method").and_then(Value::as_str)
                                            == Some("initialize")
                                        {
                                            initialize_counter.fetch_add(1, Ordering::SeqCst);
                                        }
                                        http_mcp(Json(payload)).await.into_response()
                                    }
                                })
                                .get(http_sse),
                            ),
                        )
                        .await
                        .unwrap();
                        return;
                    }
                    Err(_) => time::sleep(Duration::from_millis(10)).await,
                }
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready,
            "tools/list 的连接拒绝应按内部退避重新 initialize/discover"
        );
        assert_eq!(
            replacement_initializes.load(Ordering::SeqCst),
            1,
            "断开的 discovery 必须触发一次新的 initialize，而非复用首个 server 的成功结果"
        );
    }

    #[tokio::test]
    async fn http_truncated_discovery_response_retries_connection_establishment() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let captured_count = Arc::clone(&request_count);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move |Json(payload): Json<Value>| {
                        let request_count = Arc::clone(&captured_count);
                        async move {
                            if request_count.fetch_add(1, Ordering::SeqCst) == 0 {
                                let mut response =
                                    Body::from(r#"{"jsonrpc":"2.0","id":1"#).into_response();
                                response.headers_mut().insert(
                                    header::CONTENT_TYPE,
                                    HeaderValue::from_static("application/json"),
                                );
                                response.headers_mut().insert(
                                    header::CONTENT_LENGTH,
                                    HeaderValue::from_static("512"),
                                );
                                response
                                    .headers_mut()
                                    .insert(header::CONNECTION, HeaderValue::from_static("close"));
                                return response;
                            }
                            http_mcp(Json(payload)).await.into_response()
                        }
                    })
                    .get(http_sse),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        let snapshot = manager.snapshot().await;
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready,
            "截断响应应触发一次完整重连，last_error={:?}",
            snapshot.servers["http_server"].last_error
        );
        assert!(request_count.load(Ordering::SeqCst) > 1);
        server.abort();
    }

    #[tokio::test]
    async fn http_malformed_json_does_not_retry_connection_establishment() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let captured_count = Arc::clone(&request_count);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move || {
                        let request_count = Arc::clone(&captured_count);
                        async move {
                            request_count.fetch_add(1, Ordering::SeqCst);
                            (
                                [(header::CONTENT_TYPE, "application/json")],
                                Body::from("{"),
                            )
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();

        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Failed
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn disable_cancels_a_failed_connection_before_its_backoff_retry() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("always_fail_stdio_mock.sh");
        let attempts_path = dir.path().join("connection-attempts.txt");
        tokio::fs::write(&script_path, fail_once_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_FIXTURE_ATTEMPTS".to_string(),
            attempts_path.display().to_string(),
        );
        cfg.servers.insert(
            "retry_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh_all().await });
        time::timeout(Duration::from_secs(5), async {
            while !tokio::fs::try_exists(&attempts_path).await.unwrap() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fixture 应先记录首次连接尝试");

        manager.disable_server("retry_server").await.unwrap();
        refresh.await.unwrap().unwrap();
        time::sleep(Duration::from_millis(
            MCP_RECONNECT_RETRY_BASE_DELAY_MS.saturating_add(100),
        ))
        .await;

        assert_eq!(
            tokio::fs::read_to_string(&attempts_path)
                .await
                .unwrap()
                .trim(),
            "1",
            "Disable 必须取消退避中的旧 generation，不能启动第二个 transport"
        );
        assert_eq!(
            manager.snapshot().await.servers["retry_server"].status,
            McpServerStatus::Disabled
        );
    }

    #[tokio::test]
    async fn tool_timeout_keeps_shared_http_client_ready_for_a_peer_and_follow_up_call() {
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let handler_tool_calls = Arc::clone(&tool_calls);
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let tool_calls = Arc::clone(&handler_tool_calls);
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    async move {
                        timeout_once_http_mcp(
                            payload,
                            tool_calls,
                            cancellation_notifications,
                            first_tool_started,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(1);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let timeout_manager = Arc::clone(&manager);
        let timed_out_call = tokio::spawn(async move {
            timeout_manager
                .call_tool("http_server", "ping", Some(json!({"text": "hi"})), None)
                .await
        });
        waiting_for_first_request.await;
        let peer = manager
            .call_tool("http_server", "ping", Some(json!({"text": "peer"})), None)
            .await
            .unwrap();
        let error = timed_out_call.await.unwrap().unwrap_err().to_string();
        let follow_up = manager
            .call_tool("http_server", "ping", Some(json!({"text": "again"})), None)
            .await
            .unwrap();
        let snapshot = manager.snapshot().await;

        assert!(error.contains("调用超时"));
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&follow_up)["content"][0]["text"],
            "pong"
        );
        assert_eq!(tool_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            cancellation_notifications.load(Ordering::SeqCst),
            1,
            "request deadline 应仅取消超时请求"
        );
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready
        );
        assert_eq!(snapshot.servers["http_server"].exposed_tool_count(), 1);
    }

    #[tokio::test]
    async fn turn_cancellation_aborts_slow_http_headers_and_releases_shared_worker() {
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    async move {
                        slow_headers_timeout_http_mcp(
                            payload,
                            first_tool_started,
                            cancellation_notifications,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(30);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let turn_cancellation = CancellationToken::new();
        let waiting_for_first_request = first_tool_started.notified();
        let cancelled_manager = Arc::clone(&manager);
        let cancelled_token = turn_cancellation.clone();
        let cancelled_call = tokio::spawn(async move {
            cancelled_manager
                .call_tool_cancellable(
                    "http_server",
                    "ping",
                    Some(json!({"text": "slow"})),
                    None,
                    Some(cancelled_token),
                )
                .await
        });
        waiting_for_first_request.await;
        turn_cancellation.cancel();
        let cancellation_error = time::timeout(Duration::from_secs(1), cancelled_call)
            .await
            .expect("turn cancellation should not wait for the MCP tool deadline")
            .unwrap()
            .unwrap_err()
            .to_string();

        time::timeout(Duration::from_secs(2), async {
            while cancellation_notifications.load(Ordering::SeqCst) == 0 {
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("turn cancellation should emit an MCP cancellation notification");
        let peer = time::timeout(
            Duration::from_secs(1),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("turn cancellation must abort a slow HTTP headers wait and release the worker")
        .unwrap();
        let follow_up = manager
            .call_tool("http_server", "ping", Some(json!({"text": "again"})), None)
            .await
            .unwrap();

        assert!(cancellation_error.contains("ACN turn cancelled"));
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&follow_up)["content"][0]["text"],
            "pong"
        );
        assert_eq!(cancellation_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn slow_json_http_timeout_releases_shared_worker_for_peer_and_follow_up_call() {
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    async move {
                        slow_json_timeout_http_mcp(
                            payload,
                            first_tool_started,
                            cancellation_notifications,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(1);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let timeout_manager = Arc::clone(&manager);
        let timed_out_call = tokio::spawn(async move {
            timeout_manager
                .call_tool("http_server", "ping", Some(json!({"text": "slow"})), None)
                .await
        });
        waiting_for_first_request.await;
        let error = timed_out_call.await.unwrap().unwrap_err().to_string();
        // 当前 rmcp HTTP worker 对同 session POST 串行；已在队列中的 peer 会共享自己的原始
        // deadline，而不是在 worker 释放后重新起算。这里在释放后再派发独立 follow-up，验证 timeout
        // 没有关闭共享 session。
        let peer = time::timeout(
            Duration::from_secs(3),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("tools/call 的 HTTP response body 超时后应释放 rmcp worker")
        .unwrap();
        let follow_up = manager
            .call_tool("http_server", "ping", Some(json!({"text": "again"})), None)
            .await
            .unwrap();

        assert!(error.contains("调用超时"));
        time::timeout(Duration::from_secs(2), async {
            while cancellation_notifications.load(Ordering::SeqCst) == 0 {
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("HTTP response body 超时也应发送 notifications/cancelled");
        assert_eq!(cancellation_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&follow_up)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn turn_cancellation_aborts_slow_http_json_body_and_releases_shared_worker() {
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    async move {
                        slow_json_timeout_http_mcp(
                            payload,
                            first_tool_started,
                            cancellation_notifications,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        // 远大于 fixture 的慢 body，证明 worker 由取消而非正常 deadline 释放。
        server.tool_timeout_secs = Some(30);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let turn_cancellation = CancellationToken::new();
        let waiting_for_first_request = first_tool_started.notified();
        let cancelled_manager = Arc::clone(&manager);
        let cancelled_token = turn_cancellation.clone();
        let cancelled_call = tokio::spawn(async move {
            cancelled_manager
                .call_tool_cancellable(
                    "http_server",
                    "ping",
                    Some(json!({"text": "slow"})),
                    None,
                    Some(cancelled_token),
                )
                .await
        });
        waiting_for_first_request.await;
        turn_cancellation.cancel();
        let error = time::timeout(Duration::from_millis(500), cancelled_call)
            .await
            .expect("turn cancellation should not wait for the slow HTTP JSON body")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(error.contains("ACN turn cancelled"));

        let peer = time::timeout(
            Duration::from_secs(1),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("turn cancellation must abort a slow HTTP JSON body and release the worker")
        .unwrap();
        time::timeout(Duration::from_secs(2), async {
            while cancellation_notifications.load(Ordering::SeqCst) == 0 {
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("turn cancellation should emit notifications/cancelled after aborting the body");

        assert_eq!(cancellation_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn slow_http_response_headers_timeout_releases_shared_worker_for_peer_and_follow_up_call()
    {
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    async move {
                        slow_headers_timeout_http_mcp(
                            payload,
                            first_tool_started,
                            cancellation_notifications,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(1);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let timeout_manager = Arc::clone(&manager);
        let timed_out_call = tokio::spawn(async move {
            timeout_manager
                .call_tool("http_server", "ping", Some(json!({"text": "slow"})), None)
                .await
        });
        waiting_for_first_request.await;
        let error = timed_out_call.await.unwrap().unwrap_err().to_string();
        let peer = time::timeout(
            Duration::from_secs(3),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("HTTP response headers 超时后应释放 rmcp worker")
        .unwrap();
        let follow_up = manager
            .call_tool("http_server", "ping", Some(json!({"text": "again"})), None)
            .await
            .unwrap();

        assert!(error.contains("调用超时"));
        time::timeout(Duration::from_secs(2), async {
            while cancellation_notifications.load(Ordering::SeqCst) == 0 {
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("HTTP response headers 超时也应发送 notifications/cancelled");
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&follow_up)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn queued_http_tool_call_uses_original_deadline_instead_of_a_fresh_worker_deadline() {
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let observed_tool_texts = Arc::new(StdMutex::new(Vec::new()));
        let handler_observed_tool_texts = Arc::clone(&observed_tool_texts);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    let observed_tool_texts = Arc::clone(&handler_observed_tool_texts);
                    async move {
                        queued_deadline_http_mcp(payload, first_tool_started, observed_tool_texts)
                            .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(1);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let first_manager = Arc::clone(&manager);
        let first = tokio::spawn(async move {
            first_manager
                .call_tool("http_server", "ping", Some(json!({"text": "first"})), None)
                .await
        });
        waiting_for_first_request.await;
        // 让第二个 request 在第一个 HTTP worker 已占用时入队；它的总 deadline 会早于第一个
        // worker 释放后的“重新起算一整段 timeout”。
        time::sleep(Duration::from_millis(300)).await;
        let second_started_at = time::Instant::now();
        let second_manager = Arc::clone(&manager);
        let second = tokio::spawn(async move {
            second_manager
                .call_tool("http_server", "ping", Some(json!({"text": "second"})), None)
                .await
        });

        let first_error = time::timeout(Duration::from_secs(2), first)
            .await
            .expect("first slow headers request should reach its deadline")
            .unwrap()
            .unwrap_err()
            .to_string();
        let second_error = time::timeout(Duration::from_secs(2), second)
            .await
            .expect("queued request should honor its original deadline")
            .unwrap()
            .unwrap_err()
            .to_string();
        let second_elapsed = second_started_at.elapsed();
        let follow_up = time::timeout(
            Duration::from_secs(2),
            manager.call_tool("http_server", "ping", Some(json!({"text": "again"})), None),
        )
        .await
        .expect("expired queued request must not keep the worker occupied")
        .unwrap();

        assert!(first_error.contains("调用超时"));
        assert!(second_error.contains("调用超时"));
        assert!(
            second_elapsed < Duration::from_millis(1_400),
            "queued request received a fresh HTTP worker deadline instead of its original deadline"
        );
        assert_eq!(
            *observed_tool_texts.lock().unwrap(),
            vec![
                "first".to_string(),
                "second".to_string(),
                "again".to_string(),
            ],
            "queued request should reach the server but must not receive a fresh HTTP deadline"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&follow_up)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn aborting_tool_call_future_cancels_its_request_and_keeps_shared_http_session_ready() {
        let first_tool_started = Arc::new(Notify::new());
        let handler_first_tool_started = Arc::clone(&first_tool_started);
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let first_tool_started = Arc::clone(&handler_first_tool_started);
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    async move {
                        slow_headers_timeout_http_mcp(
                            payload,
                            first_tool_started,
                            cancellation_notifications,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        // 远大于 fixture 的慢 headers，确保释放来自 caller abort 而非普通 deadline。
        server.tool_timeout_secs = Some(30);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let waiting_for_first_request = first_tool_started.notified();
        let aborted_manager = Arc::clone(&manager);
        let aborted_call = tokio::spawn(async move {
            aborted_manager
                .call_tool("http_server", "ping", Some(json!({"text": "slow"})), None)
                .await
        });
        waiting_for_first_request.await;
        aborted_call.abort();
        assert!(aborted_call.await.unwrap_err().is_cancelled());
        let peer = time::timeout(
            Duration::from_secs(1),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("aborted caller must immediately release the HTTP worker")
        .unwrap();
        time::timeout(Duration::from_secs(2), async {
            while cancellation_notifications.load(Ordering::SeqCst) == 0 {
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("aborted request should emit notifications/cancelled");
        assert_eq!(cancellation_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn long_sse_tool_call_keeps_full_deadline_and_routes_progress() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(slow_sse_http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.startup_timeout_secs = Some(1);
        server.tool_timeout_secs = Some(2);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let progress_events = Arc::new(StdMutex::new(Vec::new()));
        let captured_progress = Arc::clone(&progress_events);
        let manager = McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            Some(Arc::new(move |event| {
                captured_progress.lock().unwrap().push(event);
            })),
        );

        manager.refresh_all().await.unwrap();
        let started_at = time::Instant::now();
        let result = manager
            .call_tool(
                "http_server",
                "ping",
                Some(json!({"text": "slow-sse"})),
                None,
            )
            .await
            .unwrap();

        assert!(
            started_at.elapsed() >= Duration::from_millis(1_700),
            "SSE result returned before the fixture's delayed progress/response sequence"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&result)["content"][0]["text"],
            "pong"
        );
        assert_eq!(progress_events.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn legacy_tool_sse_is_dropped_when_the_tool_deadline_expires() {
        let active_streams = Arc::new(AtomicUsize::new(0));
        let handler_active_streams = Arc::clone(&active_streams);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/mcp",
                    post(move |Json(payload): Json<Value>| {
                        let active_streams = Arc::clone(&handler_active_streams);
                        async move { pending_tool_sse_http_mcp(payload, active_streams).await }
                    })
                    .get(http_sse),
                ),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(1);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let error = manager
            .call_tool(
                "http_server",
                "ping",
                Some(json!({"text": "pending-sse"})),
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("调用超时"), "{error}");
        time::timeout(Duration::from_secs(1), async {
            while active_streams.load(Ordering::SeqCst) != 0 {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tool timeout must drop the legacy request SSE body");

        let peer = manager
            .call_tool("http_server", "ping", Some(json!({"text": "peer"})), None)
            .await
            .unwrap();
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
    }

    #[tokio::test]
    async fn sse_headers_arriving_near_deadline_are_not_cut_off_by_json_body_protection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(delayed_sse_headers_http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        // 远大于 fixture 的慢 headers，确保释放来自 turn cancellation。
        server.tool_timeout_secs = Some(30);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let started_at = time::Instant::now();
        let result = manager
            .call_tool(
                "http_server",
                "ping",
                Some(json!({"text": "late-sse-headers"})),
                None,
            )
            .await
            .expect("SSE headers before the actual request deadline should be accepted");

        assert!(
            started_at.elapsed() >= Duration::from_millis(750),
            "fixture did not delay SSE response headers"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&result)["content"][0]["text"],
            "pong"
        );
    }

    #[tokio::test]
    async fn read_only_live_list_failure_keeps_shared_client_ready() {
        let list_calls = Arc::new(StdMutex::new(0usize));
        let handler_calls = Arc::clone(&list_calls);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let list_calls = Arc::clone(&handler_calls);
                    async move { flaky_list_http_mcp(payload, list_calls).await }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );

        let error = manager
            .call_read_only_tool("http_server", "ping", Some(json!({"text": "hi"})), None)
            .await
            .unwrap_err()
            .to_string();
        let snapshot = manager.snapshot().await;

        assert!(
            error.contains("tools/list") || error.contains("ListTools"),
            "unexpected error: {error}"
        );
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready
        );
        assert_eq!(snapshot.servers["http_server"].exposed_tool_count(), 1);
        assert!(snapshot.servers["http_server"].last_error.is_none());
    }

    #[tokio::test]
    async fn cancelling_hung_read_only_live_list_keeps_shared_http_session_ready() {
        let list_started = Arc::new(Notify::new());
        let handler_list_started = Arc::clone(&list_started);
        let list_calls = Arc::new(AtomicUsize::new(0));
        let handler_list_calls = Arc::clone(&list_calls);
        let cancellation_notifications = Arc::new(AtomicUsize::new(0));
        let handler_cancellation_notifications = Arc::clone(&cancellation_notifications);
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let handler_tool_calls = Arc::clone(&tool_calls);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let list_started = Arc::clone(&handler_list_started);
                    let list_calls = Arc::clone(&handler_list_calls);
                    let cancellation_notifications =
                        Arc::clone(&handler_cancellation_notifications);
                    let tool_calls = Arc::clone(&handler_tool_calls);
                    async move {
                        hung_live_read_only_list_http_mcp(
                            payload,
                            list_started,
                            list_calls,
                            cancellation_notifications,
                            tool_calls,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        // 远大于 fixture 的慢 headers，确保释放来自 turn cancellation。
        server.tool_timeout_secs = Some(30);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let turn_cancellation = CancellationToken::new();
        let call_manager = Arc::clone(&manager);
        let call_cancellation = turn_cancellation.clone();
        let call = tokio::spawn(async move {
            call_manager
                .call_read_only_tool_cancellable(
                    "http_server",
                    "ping",
                    Some(json!({"text": "cancelled-live-list"})),
                    None,
                    Some(call_cancellation),
                )
                .await
        });
        list_started.notified().await;
        let cancelled_at = time::Instant::now();
        turn_cancellation.cancel();
        let error = time::timeout(Duration::from_millis(500), call)
            .await
            .expect("turn cancellation should not wait for the hung live tools/list")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            cancelled_at.elapsed() < Duration::from_millis(500),
            "live tools/list cancellation did not return promptly"
        );
        assert!(error.contains("cancelled"), "unexpected error: {error}");

        let peer = time::timeout(
            Duration::from_secs(1),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("cancelled live tools/list must immediately release the HTTP worker")
        .unwrap();
        let follow_up = manager
            .call_tool("http_server", "ping", Some(json!({"text": "again"})), None)
            .await
            .unwrap();

        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&follow_up)["content"][0]["text"],
            "pong"
        );
        assert_eq!(tool_calls.load(Ordering::SeqCst), 2);
        assert!(
            cancellation_notifications.load(Ordering::SeqCst) >= 1,
            "hung live tools/list must receive request-scoped cancellation"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn read_only_live_list_and_tool_call_share_one_admission_deadline() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let handler_list_calls = Arc::clone(&list_calls);
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let handler_tool_calls = Arc::clone(&tool_calls);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let list_calls = Arc::clone(&handler_list_calls);
                    let tool_calls = Arc::clone(&handler_tool_calls);
                    async move {
                        slow_live_list_then_slow_tool_http_mcp(payload, list_calls, tool_calls)
                            .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut server = McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None);
        server.tool_timeout_secs = Some(1);
        cfg.servers.insert("http_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let started_at = time::Instant::now();
        let error = manager
            .call_read_only_tool("http_server", "ping", Some(json!({"text": "slow"})), None)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("调用超时"), "unexpected error: {error}");
        assert!(
            started_at.elapsed() < Duration::from_millis(1_400),
            "tools/call received a fresh timeout window after slow live tools/list"
        );
        assert_eq!(list_calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);

        let peer = time::timeout(
            Duration::from_secs(1),
            manager.call_tool("http_server", "ping", Some(json!({"text": "peer"})), None),
        )
        .await
        .expect("expired read-only admission must release the shared HTTP worker")
        .unwrap();
        assert_eq!(
            crate::mcp::client::call_tool_result_to_json(&peer)["content"][0]["text"],
            "pong"
        );
        assert_eq!(
            manager.snapshot().await.servers["http_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn read_only_call_rechecks_live_tool_annotation_before_dispatch() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let handler_list_calls = Arc::clone(&list_calls);
        let handler_tool_calls = Arc::clone(&tool_calls);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let list_calls = Arc::clone(&handler_list_calls);
                    let tool_calls = Arc::clone(&handler_tool_calls);
                    async move {
                        changing_read_only_http_mcp(payload, list_calls, tool_calls).await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        assert!(raw_tool_is_read_only(
            &manager.snapshot().await.servers["http_server"].tools[0].raw_tool
        ));

        let error = manager
            .call_read_only_tool("http_server", "ping", Some(json!({})), None)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            McpManagerError::ReadOnlyRequirementFailed { .. }
        ));
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stale_transient_call_does_not_reach_tools_call_after_disable() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let list_started = Arc::new(Notify::new());
        let release_list = Arc::new(Notify::new());
        let handler_list_calls = Arc::clone(&list_calls);
        let handler_tool_calls = Arc::clone(&tool_calls);
        let handler_list_started = Arc::clone(&list_started);
        let handler_release_list = Arc::clone(&release_list);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/mcp",
                post(move |Json(payload): Json<Value>| {
                    let list_calls = Arc::clone(&handler_list_calls);
                    let tool_calls = Arc::clone(&handler_tool_calls);
                    let list_started = Arc::clone(&handler_list_started);
                    let release_list = Arc::clone(&handler_release_list);
                    async move {
                        gated_list_http_mcp(
                            payload,
                            list_calls,
                            tool_calls,
                            list_started,
                            release_list,
                        )
                        .await
                    }
                })
                .get(http_sse),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        manager.refresh_all().await.unwrap();
        let call_manager = Arc::clone(&manager);
        let call = tokio::spawn(async move {
            call_manager
                .call_read_only_tool("http_server", "ping", Some(json!({"text": "hi"})), None)
                .await
        });
        list_started.notified().await;
        manager.disable_server("http_server").await.unwrap();
        release_list.notify_waiters();

        let error = call.await.unwrap().unwrap_err().to_string();
        let snapshot = manager.snapshot().await;

        assert!(
            error.contains("不是 ready")
                || error.contains("Transport closed")
                || error.contains("lifecycle replaced or disabled"),
            "unexpected error: {error}"
        );
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Disabled
        );
    }

    #[tokio::test]
    async fn transient_call_invalid_params_keeps_server_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(invalid_params_http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let error = manager
            .call_tool(
                "http_server",
                "ping",
                Some(json!({"unexpected": true})),
                None,
            )
            .await
            .unwrap_err()
            .to_string();
        let snapshot = manager.snapshot().await;

        assert!(error.contains("参数非法"), "unexpected error: {error}");
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready
        );
        assert_eq!(snapshot.servers["http_server"].exposed_tool_count(), 1);
        assert!(snapshot.servers["http_server"].last_error.is_none());
    }

    #[tokio::test]
    async fn transient_call_non_object_arguments_keep_server_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let error = manager
            .call_tool("http_server", "ping", Some(json!("bad args")), None)
            .await
            .unwrap_err()
            .to_string();
        let snapshot = manager.snapshot().await;

        assert!(error.contains("JSON object"), "unexpected error: {error}");
        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Ready
        );
        assert_eq!(snapshot.servers["http_server"].exposed_tool_count(), 1);
        assert!(snapshot.servers["http_server"].last_error.is_none());
    }

    #[tokio::test]
    async fn too_many_discovered_tools_marks_server_failed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/mcp", post(too_many_tools_http_mcp).get(http_sse)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "http_server".to_string(),
            McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        let snapshot = manager.snapshot().await;

        assert_eq!(
            snapshot.servers["http_server"].status,
            McpServerStatus::Failed
        );
        assert!(snapshot.servers["http_server"]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("工具数量超过安全上限")));
    }

    #[tokio::test]
    async fn tool_transport_failure_marks_server_failed() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("crashing_stdio_mock.sh");
        tokio::fs::write(&script_path, crashing_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        manager
            .call_tool("stdio_server", "ping", Some(json!({"text": "hi"})), None)
            .await
            .unwrap_err();
        let snapshot = manager.snapshot().await;

        assert_eq!(
            snapshot.servers["stdio_server"].status,
            McpServerStatus::Failed
        );
        assert!(snapshot.servers["stdio_server"].tools.is_empty());
    }

    #[tokio::test]
    async fn disable_during_refresh_prevents_stale_ready_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("slow_stdio_mock.sh");
        tokio::fs::write(&script_path, slow_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh_all().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        manager.disable_server("stdio_server").await.unwrap();
        refresh.await.unwrap().unwrap();
        let snapshot = manager.snapshot().await;

        assert_eq!(
            snapshot.servers["stdio_server"].status,
            McpServerStatus::Disabled
        );
        assert!(snapshot.servers["stdio_server"].tools.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_disable_interrupts_slow_initialize_and_releases_stdio_child() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("slow_initialize_stdio_mock.sh");
        let pid_path = dir.path().join("slow_initialize.pid");
        tokio::fs::write(&script_path, slow_initialize_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_SLOW_INITIALIZE_PID_FILE".to_string(),
            pid_path.display().to_string(),
        );
        let mut server = McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            env,
            Vec::new(),
        );
        server.startup_timeout_secs = Some(30);
        cfg.servers.insert("stdio_server".to_string(), server);
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh_all().await });
        wait_for_file(&pid_path).await;
        let pid = tokio::fs::read_to_string(&pid_path)
            .await
            .unwrap()
            .trim()
            .to_string();

        let cancelled_at = time::Instant::now();
        manager
            .begin_server_disabled_runtime("stdio_server")
            .wait_for_transport_release()
            .await
            .unwrap();
        time::timeout(Duration::from_secs(2), refresh)
            .await
            .expect("disable should interrupt initialize instead of waiting for startup timeout")
            .unwrap()
            .unwrap();

        assert!(
            // 取消后现在会等待底层 child 的 graceful shutdown（其内部有 3 秒 kill 窗口），
            // 不能再把 connect future 已 drop 当成 transport 已释放；但也不能等 30 秒 startup timeout。
            cancelled_at.elapsed() < Duration::from_secs(5),
            "disable unexpectedly waited for the full slow initialize startup timeout"
        );
        assert!(
            wait_for_pid_exit(&pid).await,
            "slow stdio initialize child PID {pid} remained after lifecycle cancellation"
        );
        assert_eq!(
            manager.snapshot().await.servers["stdio_server"].status,
            McpServerStatus::Disabled
        );
        assert!(manager.snapshot().await.servers["stdio_server"]
            .tools
            .is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_disable_releases_same_group_stdio_descendant() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("descendant_stdio_mock.sh");
        let descendant_pid_path = dir.path().join("descendant.pid");
        tokio::fs::write(&script_path, descendant_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_DESCENDANT_PID_FILE".to_string(),
            descendant_pid_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = McpConnectionManager::new(path, dir.path().to_path_buf(), None);

        manager.refresh_all().await.unwrap();
        wait_for_file(&descendant_pid_path).await;
        let descendant_pid = tokio::fs::read_to_string(&descendant_pid_path)
            .await
            .unwrap()
            .trim()
            .to_string();
        assert!(
            !wait_for_pid_exit(&descendant_pid).await,
            "fixture descendant 必须在 disable 前保持存活"
        );

        manager.disable_server("stdio_server").await.unwrap();

        assert!(
            wait_for_pid_exit(&descendant_pid).await,
            "disable 后 stdio 同进程组 descendant {descendant_pid} 仍然存活"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_manager_and_runtime_releases_same_group_stdio_processes() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("descendant_stdio_mock.sh");
        let root_pid_path = dir.path().join("root.pid");
        let descendant_pid_path = dir.path().join("descendant.pid");
        tokio::fs::write(&script_path, descendant_stdio_mock_script())
            .await
            .unwrap();
        let config_path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let mut env = BTreeMap::new();
        env.insert(
            "MCP_ROOT_PID_FILE".to_string(),
            root_pid_path.display().to_string(),
        );
        env.insert(
            "MCP_DESCENDANT_PID_FILE".to_string(),
            descendant_pid_path.display().to_string(),
        );
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&config_path, &cfg)
            .await
            .unwrap();

        let workspace_root = dir.path().to_path_buf();
        let thread_config_path = config_path.clone();
        let thread_root_pid_path = root_pid_path.clone();
        let thread_descendant_pid_path = descendant_pid_path.clone();
        let (root_pid, descendant_pid) = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let pids = runtime.block_on(async move {
                let manager = McpConnectionManager::new(thread_config_path, workspace_root, None);
                manager.refresh_all().await.unwrap();
                wait_for_file(&thread_root_pid_path).await;
                wait_for_file(&thread_descendant_pid_path).await;
                let root_pid = tokio::fs::read_to_string(&thread_root_pid_path)
                    .await
                    .unwrap()
                    .trim()
                    .to_string();
                let descendant_pid = tokio::fs::read_to_string(&thread_descendant_pid_path)
                    .await
                    .unwrap()
                    .trim()
                    .to_string();

                // manager 的 Drop 只能发起取消；紧接着销毁 runtime，覆盖异步 close
                // future 没有机会完成时，process wrapper 的同步 Drop 兜底。
                drop(manager);
                (root_pid, descendant_pid)
            });
            drop(runtime);
            pids
        })
        .join()
        .unwrap();

        assert!(
            wait_for_pid_exit(&root_pid).await,
            "manager/runtime Drop 后 stdio MCP root {root_pid} 仍然存活或未回收"
        );
        assert!(
            wait_for_pid_exit(&descendant_pid).await,
            "manager/runtime Drop 后 stdio MCP descendant {descendant_pid} 仍然存活"
        );
    }

    #[tokio::test]
    async fn reconnect_waits_for_cancelled_pending_stdio_connect_before_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let slow_script_path = dir.path().join("slow_initialize_stdio_mock.sh");
        let replacement_script_path = dir.path().join("replacement_stdio_mock.sh");
        let old_pid_path = dir.path().join("slow-initialize.pid");
        let replacement_events_path = dir.path().join("replacement-events.log");
        tokio::fs::write(&slow_script_path, slow_initialize_stdio_mock_script())
            .await
            .unwrap();
        tokio::fs::write(&replacement_script_path, replacement_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut initial_cfg = McpJsonConfig::default();
        let mut initial_env = BTreeMap::new();
        initial_env.insert(
            "MCP_SLOW_INITIALIZE_PID_FILE".to_string(),
            old_pid_path.display().to_string(),
        );
        let mut initial_server = McpServerConfig::stdio(
            "sh".to_string(),
            vec![slow_script_path.display().to_string()],
            initial_env,
            Vec::new(),
        );
        initial_server.startup_timeout_secs = Some(30);
        initial_cfg
            .servers
            .insert("stdio_server".to_string(), initial_server);
        write_mcp_json_config_atomic(&path, &initial_cfg)
            .await
            .unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path.clone(),
            dir.path().to_path_buf(),
            None,
        ));

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh_all().await });
        wait_for_file(&old_pid_path).await;
        let old_pid = tokio::fs::read_to_string(&old_pid_path)
            .await
            .unwrap()
            .trim()
            .to_string();

        let mut replacement_cfg = McpJsonConfig::default();
        let mut replacement_env = BTreeMap::new();
        replacement_env.insert(
            "MCP_REPLACEMENT_EVENTS_FILE".to_string(),
            replacement_events_path.display().to_string(),
        );
        replacement_env.insert("MCP_REPLACED_PID".to_string(), old_pid.clone());
        replacement_cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![replacement_script_path.display().to_string()],
                replacement_env,
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &replacement_cfg)
            .await
            .unwrap();

        time::timeout(
            // slow child 的 rmcp graceful shutdown 会先等待其内部 3 秒窗口；replacement
            // 必须等到该 close 真正结束，而非提前把 dropped connect future 当作已释放。
            Duration::from_secs(5),
            manager.reconnect_server("stdio_server"),
        )
        .await
        .expect("replacement 应等待 cancelled connect attempt 收束后完成")
        .unwrap();
        time::timeout(Duration::from_secs(2), refresh)
            .await
            .expect("旧 refresh 应随 cancellation 收束")
            .unwrap()
            .unwrap();

        assert!(
            wait_for_pid_exit(&old_pid).await,
            "old pending stdio PID {old_pid} remained after replacement"
        );
        let events = tokio::fs::read_to_string(&replacement_events_path)
            .await
            .unwrap();
        assert!(events.contains("initialize"), "events={events}");
        assert!(
            !events.contains("overlap"),
            "replacement started while old pending stdio PID was still alive; events={events}"
        );
        assert_eq!(
            manager.snapshot().await.servers["stdio_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn reconnect_waits_until_completed_outcome_is_installed_or_disposed() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("stdio_mock.sh");
        tokio::fs::write(&script_path, stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        let server = McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            BTreeMap::new(),
            Vec::new(),
        );
        cfg.servers
            .insert("stdio_server".to_string(), server.clone());
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path,
            dir.path().to_path_buf(),
            None,
        ));

        let (generation, _, attempt, _) = manager
            .begin_connect_attempt(
                "stdio_server",
                &server,
                McpServerStatus::Starting,
                true,
                false,
            )
            .unwrap();
        let outcome = connect_server(
            "stdio_server".to_string(),
            server,
            generation,
            manager.config_path.clone(),
            dir.path().to_path_buf(),
            None,
            Arc::clone(&attempt),
            Arc::clone(&manager.release_gates),
            manager.oauth_refresh_activity.clone(),
        )
        .await
        .expect("first connect should produce a ready outcome");
        assert!(outcome.client.is_some());

        let reconnect_manager = Arc::clone(&manager);
        let reconnect =
            tokio::spawn(async move { reconnect_manager.reconnect_server("stdio_server").await });
        time::timeout(Duration::from_secs(1), async {
            loop {
                if manager.snapshot().await.servers["stdio_server"].status
                    == McpServerStatus::Reconnecting
                {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reconnect should register its replacement generation");
        assert!(
            !reconnect.is_finished(),
            "replacement must wait while the completed old outcome still owns a client"
        );

        manager.apply_connect_outcome(outcome).await;
        time::timeout(Duration::from_secs(2), reconnect)
            .await
            .expect("replacement should continue after stale outcome cleanup")
            .unwrap()
            .unwrap();
        assert_eq!(
            manager.snapshot().await.servers["stdio_server"].status,
            McpServerStatus::Ready
        );
    }

    #[tokio::test]
    async fn removed_server_during_refresh_drops_stale_ready_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("slow_stdio_mock.sh");
        tokio::fs::write(&script_path, slow_stdio_mock_script())
            .await
            .unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "stdio_server".to_string(),
            McpServerConfig::stdio(
                "sh".to_string(),
                vec![script_path.display().to_string()],
                BTreeMap::new(),
                Vec::new(),
            ),
        );
        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let manager = Arc::new(McpConnectionManager::new(
            path.clone(),
            dir.path().to_path_buf(),
            None,
        ));
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh_all().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        write_mcp_json_config_atomic(&path, &McpJsonConfig::default())
            .await
            .unwrap();
        manager.refresh_all().await.unwrap();
        refresh.await.unwrap().unwrap();
        let snapshot = manager.snapshot().await;

        assert!(!snapshot.servers.contains_key("stdio_server"));
    }

    fn tool(name: &'static str) -> Tool {
        Tool::new(
            name,
            "test tool",
            Arc::new(JsonObject::from_iter([(
                "type".to_string(),
                Value::String("object".to_string()),
            )])),
        )
    }

    fn stdio_mock_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
progress_token() {
  printf '%s' "$1" | sed -n 's/.*"progressToken":[[:space:]]*"\{0,1\}\([^",}]*\).*/\1/p'
}
while IFS= read -r line; do
id=$(response_id "$line")
case "$line" in
  *'"method":"server/discover"'*)
    printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
    ;;
  *'"method":"initialize"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"stdio-mock","version":"1.0.0"}}}\n' "$id"
    ;;
  *'"method":"tools/list"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object","properties":{"text":{"type":"string","description":"Input text"}}}}]}}\n' "$id"
    ;;
  *'"method":"tools/call"'*)
    token=$(progress_token "$line")
    printf '{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"%s","progress":1,"total":2,"message":"half"}}\n' "$token"
    printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"pong"}],"isError":false}}\n' "$id"
    ;;
esac
done
"#
    }

    fn strict_stdio_no_local_deadline_meta_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$MCP_FIXTURE_LOG"
  case "$line" in
    *'acn.localToolDeadlineMillis'*)
      printf '%s\n' 'ACN-local HTTP deadline metadata leaked to stdio' >&2
      exit 1
      ;;
  esac
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"strict-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"pong"}],"isError":false}}\n' "$id"
      ;;
  esac
done
"#
    }

    fn counting_stdio_mock_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf 'initialize %s\n' "$$" >> "$MCP_FIXTURE_LOG"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"counting-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf 'tools/list %s\n' "$$" >> "$MCP_FIXTURE_LOG"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      printf 'tools/call %s\n' "$$" >> "$MCP_FIXTURE_LOG"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"pong"}],"isError":false}}\n' "$id"
      ;;
  esac
done
"#
    }

    fn fail_once_stdio_mock_script() -> &'static str {
        r#"attempt=0
if [ -f "$MCP_FIXTURE_ATTEMPTS" ]; then
  attempt=$(cat "$MCP_FIXTURE_ATTEMPTS")
fi
attempt=$((attempt + 1))
printf '%s\n' "$attempt" > "$MCP_FIXTURE_ATTEMPTS"
if [ "$attempt" -eq 1 ]; then
  exit 1
fi
response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"retry-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
  esac
done
"#
    }

    fn reconnect_in_flight_stdio_mock_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"reconnect-in-flight-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      if [ ! -f "$MCP_FIXTURE_FIRST_CALL_STARTED" ]; then
        : > "$MCP_FIXTURE_FIRST_CALL_STARTED"
        sleep 10
      fi
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"pong"}],"isError":false}}\n' "$id"
      ;;
  esac
done
"#
    }

    fn slow_stdio_mock_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
id=$(response_id "$line")
case "$line" in
  *'"method":"server/discover"'*)
    printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
    ;;
  *'"method":"initialize"'*)
    sleep 1
    printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"slow-stdio-mock","version":"1.0.0"}}}\n' "$id"
    ;;
  *'"method":"tools/list"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
    ;;
esac
done
"#
    }

    fn slow_initialize_stdio_mock_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
id=$(response_id "$line")
case "$line" in
  *'"method":"server/discover"'*)
    printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
    ;;
  *'"method":"initialize"'*)
    printf '%s\n' "$$" > "$MCP_SLOW_INITIALIZE_PID_FILE"
    sleep 30
    printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"slow-initialize-mock","version":"1.0.0"}}}\n' "$id"
    ;;
esac
done
"#
    }

    #[cfg(unix)]
    fn descendant_stdio_mock_script() -> &'static str {
        r#"if [ -n "${MCP_ROOT_PID_FILE:-}" ]; then
  printf '%s\n' "$$" > "$MCP_ROOT_PID_FILE"
fi
sh -c 'trap "" HUP TERM; exec sleep 300' &
printf '%s\n' "$!" > "$MCP_DESCENDANT_PID_FILE"
response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"descendant-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
  esac
done
"#
    }

    fn replacement_stdio_mock_script() -> &'static str {
        r#"if kill -0 "$MCP_REPLACED_PID" 2>/dev/null; then
  printf 'overlap old_pid=%s new_pid=%s\n' "$MCP_REPLACED_PID" "$$" >> "$MCP_REPLACEMENT_EVENTS_FILE"
fi
response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
  id="$(response_id "$line")"
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf 'initialize %s\n' "$$" >> "$MCP_REPLACEMENT_EVENTS_FILE"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"replacement-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
  esac
done
"#
    }

    fn crashing_stdio_mock_script() -> &'static str {
        r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
while IFS= read -r line; do
id=$(response_id "$line")
case "$line" in
  *'"method":"server/discover"'*)
    printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
    ;;
  *'"method":"initialize"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"crashing-stdio-mock","version":"1.0.0"}}}\n' "$id"
    ;;
  *'"method":"tools/list"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object"}}]}}\n' "$id"
    ;;
  *'"method":"tools/call"'*)
    exit 1
    ;;
esac
done
"#
    }

    async fn http_sse() -> impl IntoResponse {
        StatusCode::METHOD_NOT_ALLOWED
    }

    /// rmcp 的 session cleanup 有 5 秒上限，用来覆盖 ACN 3 秒 release gate 的失败分支。
    async fn hanging_delete_http_mcp() -> axum::response::Response {
        std::future::pending::<()>().await;
        StatusCode::NO_CONTENT.into_response()
    }

    async fn clean_delete_http_mcp() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    async fn auth_required_http_mcp() -> axum::response::Response {
        auth_required_response()
    }

    async fn auth_required_tool_call_http_mcp(
        Json(payload): Json<Value>,
    ) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("tools/call")
            && payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                == Some("denied")
        {
            return auth_required_response();
        }
        http_mcp(Json(payload)).await.into_response()
    }

    /// 第一项旧 generation call 不返回，直到 client lifecycle 主动关闭 response stream；
    /// 新 generation 的 initialize/list/call 必须仍可独立完成。
    async fn blocking_first_http_tool_mcp(
        payload: Value,
        initialize_count: Arc<AtomicUsize>,
        first_tool_started: Arc<Notify>,
    ) -> axum::response::Response {
        match payload.get("method").and_then(Value::as_str) {
            Some("initialize") => {
                initialize_count.fetch_add(1, Ordering::SeqCst);
            }
            Some("tools/call")
                if payload
                    .pointer("/params/arguments/text")
                    .and_then(Value::as_str)
                    == Some("old") =>
            {
                first_tool_started.notify_waiters();
                std::future::pending::<()>().await;
            }
            _ => {}
        }
        http_mcp(Json(payload)).await.into_response()
    }

    fn auth_required_response() -> axum::response::Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::WWW_AUTHENTICATE,
            HeaderValue::from_static(
                r#"Bearer resource_metadata="https://auth.example.test/.well-known/oauth-protected-resource?token=secret""#,
            ),
        );
        (StatusCode::UNAUTHORIZED, headers, "auth required").into_response()
    }

    async fn http_mcp(Json(payload): Json<Value>) -> impl IntoResponse {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "server/discover" {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": "Method not found"}
            }))
            .into_response();
        }
        if method == "notifications/initialized" {
            return StatusCode::ACCEPTED.into_response();
        }
        if method == "tools/call" {
            let progress_token = payload
                .pointer("/params/_meta/progressToken")
                .cloned()
                .unwrap_or_else(|| Value::String("missing".into()));
            return http_tool_call_sse(id, progress_token);
        }
        let result = match method {
            "initialize" => json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "http-mock", "version": "1.0.0"}
            }),
            "tools/list" => json!({
                "tools": [{
                    "name": "ping",
                    "description": "Ping tool",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "text": {"type": "string", "description": "Input text"}
                        }
                    }
                }]
            }),
            _ => json!({}),
        };
        let mut headers = HeaderMap::new();
        headers.insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
        (
            headers,
            Json(json!({"jsonrpc": "2.0", "id": id, "result": result})),
        )
            .into_response()
    }

    async fn slow_sse_http_mcp(Json(payload): Json<Value>) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("tools/call") {
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            let progress_token = payload
                .pointer("/params/_meta/progressToken")
                .cloned()
                .unwrap_or_else(|| Value::String("missing".into()));
            return slow_http_tool_call_sse(id, progress_token);
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn pending_tool_sse_http_mcp(
        payload: Value,
        active_streams: Arc<AtomicUsize>,
    ) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("tools/call")
            && payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                == Some("pending-sse")
        {
            let guard = ActiveSseGuard::new(active_streams);
            let first = stream::once(async {
                Ok::<Event, Infallible>(Event::default().id("pending-event"))
            });
            let pending = stream::pending::<Result<Event, Infallible>>();
            let body = first.chain(pending).map(move |event| {
                let _guard = &guard;
                event
            });
            let mut response = Sse::new(body).into_response();
            response
                .headers_mut()
                .insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
            return response;
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn delayed_sse_headers_http_mcp(Json(payload): Json<Value>) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("tools/call")
            && payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                == Some("late-sse-headers")
        {
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            let progress_token = payload
                .pointer("/params/_meta/progressToken")
                .cloned()
                .unwrap_or_else(|| Value::String("missing".into()));
            // 该值落在旧的 250ms body 保护窗口内、但仍早于真正 tools/call deadline。
            time::sleep(Duration::from_millis(800)).await;
            return http_tool_call_sse(id, progress_token);
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn tracked_http_mcp(
        headers: HeaderMap,
        payload: Value,
        metrics: HttpConnectionMetrics,
    ) -> axum::response::Response {
        match payload.get("method").and_then(Value::as_str) {
            Some("initialize") => {
                metrics.initialize_count.fetch_add(1, Ordering::SeqCst);
            }
            Some("tools/list") | Some("tools/call") => {
                if payload.get("method").and_then(Value::as_str) == Some("tools/list") {
                    metrics.list_count.fetch_add(1, Ordering::SeqCst);
                } else {
                    metrics.tool_call_count.fetch_add(1, Ordering::SeqCst);
                }
                let session_id = headers
                    .get("mcp-session-id")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("<missing>")
                    .to_string();
                metrics.session_ids.lock().unwrap().push(session_id);
            }
            _ => {}
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn flaky_list_http_mcp(
        payload: Value,
        list_calls: Arc<StdMutex<usize>>,
    ) -> axum::response::Response {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "tools/list" {
            let mut calls = list_calls.lock().unwrap();
            *calls += 1;
            if *calls > 1 {
                let mut headers = HeaderMap::new();
                headers.insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
                return (
                    headers,
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32000, "message": "transient tools/list failure"}
                    })),
                )
                    .into_response();
            }
            return mcp_test_read_only_tools_list_response(id);
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn hung_live_read_only_list_http_mcp(
        payload: Value,
        list_started: Arc<Notify>,
        list_calls: Arc<AtomicUsize>,
        cancellation_notifications: Arc<AtomicUsize>,
        tool_calls: Arc<AtomicUsize>,
    ) -> axum::response::Response {
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        assert!(
            payload
                .pointer("/params/_meta/acn.localToolDeadlineMillis")
                .is_none(),
            "ACN local deadline must not be sent to the MCP server"
        );
        if method == "notifications/cancelled" {
            cancellation_notifications.fetch_add(1, Ordering::SeqCst);
            return StatusCode::ACCEPTED.into_response();
        }
        if method == "tools/list" {
            if list_calls.fetch_add(1, Ordering::SeqCst) > 0 {
                list_started.notify_waiters();
                // 故意不返回 headers，覆盖 rmcp HTTP worker 尚卡在 POST send 的窗口。
                time::sleep(Duration::from_secs(3)).await;
            }
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            return mcp_test_read_only_tools_list_response(id);
        }
        if method == "tools/call" {
            tool_calls.fetch_add(1, Ordering::SeqCst);
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn slow_live_list_then_slow_tool_http_mcp(
        payload: Value,
        list_calls: Arc<AtomicUsize>,
        tool_calls: Arc<AtomicUsize>,
    ) -> axum::response::Response {
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "tools/list" {
            if list_calls.fetch_add(1, Ordering::SeqCst) > 0 {
                // discovery 已完成后，第二次才是只读调用的实时 admission 校验。
                time::sleep(Duration::from_millis(700)).await;
            }
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            return mcp_test_read_only_tools_list_response(id);
        }
        if method == "tools/call" {
            tool_calls.fetch_add(1, Ordering::SeqCst);
            if payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                == Some("slow")
            {
                // 剩余 deadline 不足以等待 headers；旧实现却会给这一步重新完整的一秒窗口。
                time::sleep(Duration::from_secs(3)).await;
            }
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn changing_read_only_http_mcp(
        payload: Value,
        list_calls: Arc<AtomicUsize>,
        tool_calls: Arc<AtomicUsize>,
    ) -> axum::response::Response {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "tools/list" {
            let read_only = list_calls.fetch_add(1, Ordering::SeqCst) == 0;
            let mut headers = HeaderMap::new();
            headers.insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
            return (
                headers,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "tools": [{
                            "name": "ping",
                            "description": "Ping tool",
                            "inputSchema": {"type": "object"},
                            "annotations": {"readOnlyHint": read_only}
                        }]
                    }
                })),
            )
                .into_response();
        }
        if method == "tools/call" {
            tool_calls.fetch_add(1, Ordering::SeqCst);
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn gated_list_http_mcp(
        payload: Value,
        list_calls: Arc<AtomicUsize>,
        tool_calls: Arc<AtomicUsize>,
        list_started: Arc<Notify>,
        release_list: Arc<Notify>,
    ) -> axum::response::Response {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "tools/list" {
            let call_index = list_calls.fetch_add(1, Ordering::SeqCst);
            if call_index > 0 {
                list_started.notify_waiters();
                release_list.notified().await;
            }
            return mcp_test_read_only_tools_list_response(id);
        }
        if method == "tools/call" {
            tool_calls.fetch_add(1, Ordering::SeqCst);
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn invalid_params_http_mcp(Json(payload): Json<Value>) -> impl IntoResponse {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "tools/call" {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32602,
                    "message": "invalid ping arguments"
                }
            }))
            .into_response();
        }
        http_mcp(Json(payload)).await.into_response()
    }

    fn mcp_test_read_only_tools_list_response(id: Value) -> axum::response::Response {
        let mut headers = HeaderMap::new();
        headers.insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
        (
            headers,
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "tools": [{
                        "name": "ping",
                        "description": "Ping tool",
                        "inputSchema": {"type": "object"},
                        "annotations": {"readOnlyHint": true}
                    }]
                }
            })),
        )
            .into_response()
    }

    fn negotiated_protocol_http_mcp(
        headers: HeaderMap,
        payload: Value,
        seen_headers: SeenProtocolHeaders,
    ) -> axum::response::Response {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let protocol_header = headers
            .get("mcp-protocol-version")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        seen_headers
            .lock()
            .unwrap()
            .push((method.clone(), protocol_header.clone()));
        if method == "server/discover" {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": "Method not found"}
            }))
            .into_response();
        }
        if method == "initialize" {
            let mut headers = HeaderMap::new();
            headers.insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
            return (
                headers,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "http-mock", "version": "1.0.0"}
                    }
                })),
            )
                .into_response();
        }
        if protocol_header.as_deref() != Some("2025-06-18") {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "wrong protocol header"})),
            )
                .into_response();
        }
        if method == "notifications/initialized" {
            return StatusCode::ACCEPTED.into_response();
        }
        if method == "tools/list" {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "tools": [{
                        "name": "ping",
                        "description": "Ping tool",
                        "inputSchema": {"type": "object"}
                    }]
                }
            }))
            .into_response();
        }
        Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {}
        }))
        .into_response()
    }

    fn discovered_protocol_http_mcp(
        headers: HeaderMap,
        payload: Value,
        seen_headers: SeenProtocolHeaders,
    ) -> axum::response::Response {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let protocol_header = headers
            .get("mcp-protocol-version")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        seen_headers
            .lock()
            .unwrap()
            .push((method.clone(), protocol_header.clone()));
        if protocol_header.as_deref() != Some("2026-07-28") {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "wrong protocol header"})),
            )
                .into_response();
        }
        if method == "server/discover" {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "resultType": "complete",
                    "supportedVersions": ["2026-07-28"],
                    "capabilities": {"tools": {}},
                    "ttlMs": 0,
                    "cacheScope": "private"
                }
            }))
            .into_response();
        }
        if method == "tools/list" {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "resultType": "complete",
                    "tools": [{
                        "name": "ping",
                        "description": "Ping tool",
                        "inputSchema": {"type": "object"}
                    }]
                }
            }))
            .into_response();
        }
        Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {}
        }))
        .into_response()
    }

    async fn timeout_once_http_mcp(
        payload: Value,
        tool_calls: Arc<AtomicUsize>,
        cancellation_notifications: Arc<AtomicUsize>,
        first_tool_started: Arc<Notify>,
    ) -> axum::response::Response {
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "notifications/cancelled" {
            cancellation_notifications.fetch_add(1, Ordering::SeqCst);
        }
        if method == "tools/call" && tool_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            first_tool_started.notify_waiters();
            return StatusCode::ACCEPTED.into_response();
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn slow_json_timeout_http_mcp(
        payload: Value,
        first_tool_started: Arc<Notify>,
        cancellation_notifications: Arc<AtomicUsize>,
    ) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
            cancellation_notifications.fetch_add(1, Ordering::SeqCst);
        }
        if payload.get("method").and_then(Value::as_str) == Some("tools/call")
            && payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                == Some("slow")
        {
            first_tool_started.notify_waiters();
            // 先回 JSON headers，再延迟 body；这是 rmcp HTTP worker 被 response body 占住的
            // 精确模型，而不是“服务器迟迟没有响应 headers”。
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            let response_json = json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{"type": "text", "text": "late"}],
                    "isError": false
                }
            })
            .to_string();
            let body = stream::once(async move {
                time::sleep(Duration::from_secs(3)).await;
                Ok::<Bytes, Infallible>(Bytes::from(response_json))
            });
            let mut response = axum::response::Response::new(Body::from_stream(body));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            response
                .headers_mut()
                .insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
            return response;
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn slow_headers_timeout_http_mcp(
        payload: Value,
        first_tool_started: Arc<Notify>,
        cancellation_notifications: Arc<AtomicUsize>,
    ) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
            cancellation_notifications.fetch_add(1, Ordering::SeqCst);
        }
        if payload.get("method").and_then(Value::as_str) == Some("tools/call")
            && payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                == Some("slow")
        {
            first_tool_started.notify_waiters();
            // 故意不发送任何 headers，覆盖 reqwest::send() 尚未完成的 worker 堵塞窗口。
            time::sleep(Duration::from_secs(3)).await;
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn queued_deadline_http_mcp(
        payload: Value,
        first_tool_started: Arc<Notify>,
        observed_tool_texts: Arc<StdMutex<Vec<String>>>,
    ) -> axum::response::Response {
        if payload.get("method").and_then(Value::as_str) == Some("tools/call") {
            let text = payload
                .pointer("/params/arguments/text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            observed_tool_texts.lock().unwrap().push(text.clone());
            if matches!(text.as_str(), "first" | "second") {
                if text == "first" {
                    first_tool_started.notify_waiters();
                }
                // 不返回 headers，确保同 session worker 在第一个 request 上被占住。
                time::sleep(Duration::from_secs(3)).await;
            }
        }
        http_mcp(Json(payload)).await.into_response()
    }

    async fn wait_for_pid_exit(pid: &str) -> bool {
        time::timeout(Duration::from_secs(2), async {
            loop {
                let status = tokio::process::Command::new("kill")
                    .args(["-0", pid])
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await;
                if !matches!(status, Ok(status) if status.success()) {
                    return;
                }
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .is_ok()
    }

    async fn wait_for_file(path: &Path) {
        time::timeout(Duration::from_secs(2), async {
            loop {
                if tokio::fs::try_exists(path).await.unwrap_or(false) {
                    return;
                }
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("stdio fixture did not create its expected marker file");
    }

    async fn too_many_tools_http_mcp(Json(payload): Json<Value>) -> impl IntoResponse {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "tools/list" {
            let tools = (0..=TOOLS_LIST_TOOL_LIMIT)
                .map(|index| {
                    json!({
                        "name": format!("tool_{index}"),
                        "description": "tool",
                        "inputSchema": {"type": "object"}
                    })
                })
                .collect::<Vec<_>>();
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"tools": tools}
            }))
            .into_response();
        }
        http_mcp(Json(payload)).await.into_response()
    }

    fn http_tool_call_sse(id: Value, progress_token: Value) -> axum::response::Response {
        let progress = json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {
                "progressToken": progress_token,
                "progress": 1,
                "total": 2,
                "message": "half"
            }
        });
        let result = json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{"type": "text", "text": "pong"}],
                "isError": false
            }
        });
        let events = vec![
            Ok::<Event, Infallible>(Event::default().data(progress.to_string())),
            Ok::<Event, Infallible>(Event::default().data(result.to_string())),
        ];
        let mut response = Sse::new(stream::iter(events)).into_response();
        response
            .headers_mut()
            .insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
        response
    }

    fn slow_http_tool_call_sse(id: Value, progress_token: Value) -> axum::response::Response {
        let progress = json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {
                "progressToken": progress_token,
                "progress": 1,
                "total": 2,
                "message": "slow half"
            }
        });
        let result = json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{"type": "text", "text": "pong"}],
                "isError": false
            }
        });
        let progress_event = stream::once(async move {
            time::sleep(Duration::from_millis(1_000)).await;
            Ok::<Event, Infallible>(Event::default().data(progress.to_string()))
        });
        let result_event = stream::once(async move {
            time::sleep(Duration::from_millis(800)).await;
            Ok::<Event, Infallible>(Event::default().data(result.to_string()))
        });
        let mut response = Sse::new(progress_event.chain(result_event)).into_response();
        response
            .headers_mut()
            .insert("Mcp-Session-Id", HeaderValue::from_static("test-session"));
        response
    }
}
