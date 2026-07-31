//! 单个 MCP server 的 client lifecycle。
//!
//! 基于官方 `rmcp` SDK 建立 stdio / Streamable HTTP 连接，统一处理 initialize、
//! tools/list、tools/call、progress notification 和硬超时。

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::Duration;

use futures::{stream::BoxStream, StreamExt};
use rmcp::handler::client::ClientHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, CancelledNotificationParam, ClientInfo, ClientRequest,
    CreateElicitationRequestParams, CreateElicitationResult, ElicitationAction, ErrorCode,
    GetExtensions, Implementation, ListToolsResult, Meta, NumberOrString, PaginatedRequestParams,
    ProgressNotificationParam, ProgressToken, ProtocolVersion, Request, RequestId, ServerInfo,
    ServerJsonRpcMessage, ServerResult, TaskSupport, Tool,
};
use rmcp::service::{
    NotificationContext, Peer, PeerRequestOptions, RoleClient, RunningService,
    RunningServiceCancellationToken, RxJsonRpcMessage, TxJsonRpcMessage,
};
use rmcp::transport::child_process::TokioChildProcess;
use rmcp::transport::streamable_http_client::{
    SseError, StreamableHttpClient, StreamableHttpClientTransportConfig, StreamableHttpError,
    StreamableHttpPostResponse,
};
use rmcp::transport::{StreamableHttpClientTransport, Transport};
use serde_json::{json, Map, Value};
use sse_stream::{Sse, SseStream};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::config::MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS;
use crate::mcp::config::{McpServerConfig, McpTransportConfig};
use crate::mcp::redact::redact_mcp_sensitive_text;

pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const UNSUPPORTED_INTERACTIVE_AUTH_MESSAGE: &str =
    "MCP server requires OAuth or interactive elicitation, which is not supported yet";
const HEADER_PROTOCOL_VERSION: &str = "MCP-Protocol-Version";
const HEADER_SESSION_ID: &str = "Mcp-Session-Id";
const HEADER_LAST_EVENT_ID: &str = "Last-Event-Id";
const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
const JSON_MIME_TYPE: &str = "application/json";
const STDERR_CAPTURE_MAX_CHARS: usize = 8_000;
/// rmcp 的 stdio graceful shutdown 自身也使用同一秒级窗口；超时后还要向进程组发信号、
/// 等待 root 退出并 reap。额外保留一秒，避免把正在完成强制收束的 transport 误判为泄漏。
const PENDING_CONNECT_CLOSE_GRACE: Duration = Duration::from_secs(1);
/// 仅在 ACN 的 HTTP adapter 内传递绝对单请求 deadline；发出 HTTP 前会从 `_meta` 移除，server 不可见。
const ACN_LOCAL_TOOL_DEADLINE_META_FIELD: &str = "acn.localToolDeadlineMillis";
static MCP_TOOL_DEADLINE_EPOCH: LazyLock<time::Instant> = LazyLock::new(time::Instant::now);
const TURN_CANCELLATION_REASON: &str = "ACN turn cancelled";
const TOOL_TIMEOUT_CANCELLATION_REASON: &str = "request timeout";
const CALLER_ABORT_CANCELLATION_REASON: &str = "caller future dropped";
const LIFECYCLE_CANCELLATION_REASON: &str = "MCP client lifecycle replaced or disabled";

/// 只在 ACN HTTP adapter 内传递的 request cancellation；它保存在 rmcp extensions，永不序列化给 server。
#[derive(Clone)]
struct AcnMcpHttpRequestCancellation(CancellationToken);
const DEFAULT_ENV_VARS_UNIX: &[&str] = &[
    "HOME",
    "LOGNAME",
    "PATH",
    "SHELL",
    "USER",
    "__CF_USER_TEXT_ENCODING",
    "LANG",
    "LC_ALL",
    "TERM",
    "TMPDIR",
    "TZ",
];
const DEFAULT_ENV_VARS_WINDOWS: &[&str] = &[
    "APPDATA",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERDOMAIN",
    "USERNAME",
    "USERPROFILE",
    "WINDIR",
];

pub type McpProgressCallback = Arc<dyn Fn(McpProgressEvent) + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq)]
pub struct McpProgressEvent {
    pub server_name: String,
    pub progress_token: String,
    pub progress: f64,
    pub total: Option<f64>,
    pub message: Option<String>,
}

pub struct McpClient {
    server_name: String,
    service: Mutex<RunningService<RoleClient, AcnMcpClientHandler>>,
    /// 可在同步 lifecycle 边界立刻取消 driver；正常路径仍通过 `shutdown().await` 等待 transport 收束。
    shutdown_token: StdMutex<Option<RunningServiceCancellationToken>>,
    /// 仅 disable/reconnect/shutdown 使用，用于让已等待 response 的旧 generation caller 立即收束。
    lifecycle_cancel: CancellationToken,
    server_info: Option<ServerInfo>,
    stderr: Arc<Mutex<String>>,
    tool_timeout: Duration,
    /// 绝对 HTTP response deadline 仅能通过 Streamable HTTP adapter 的本地 metadata 传递。
    uses_streamable_http: bool,
}

/// 连接尝试被替换时，记录其底层 transport 是否已真正完成关闭。
///
/// `rmcp::serve_client` 在被取消后会析构 transport，但部分 transport 的析构清理是异步的；
/// replacement 只能在该 fence 确认释放后继续，避免同一 server 出现短暂的双连接。
pub(crate) struct McpConnectReleaseFence {
    active_transports: AtomicUsize,
    cancellation_requested: AtomicBool,
    connect_finished: AtomicBool,
    cleanup_failed: AtomicBool,
    completed: AtomicBool,
    changed: tokio::sync::Notify,
}

impl McpConnectReleaseFence {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            active_transports: AtomicUsize::new(0),
            cancellation_requested: AtomicBool::new(false),
            connect_finished: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn register_transport(self: &Arc<Self>) -> PendingConnectTransportRegistration {
        self.active_transports.fetch_add(1, Ordering::AcqRel);
        PendingConnectTransportRegistration {
            fence: Arc::clone(self),
            settled: AtomicBool::new(false),
        }
    }

    pub(crate) fn request_cancellation(&self) {
        self.cancellation_requested.store(true, Ordering::Release);
        self.try_complete();
    }

    /// 连接 task 已不再继续创建 transport。未取消的 ready/failure outcome 可以立刻交接；
    /// 已取消的 attempt 则必须继续等待所有已创建 transport 的 close 结果。
    pub(crate) fn finish_connect(&self) {
        self.connect_finished.store(true, Ordering::Release);
        self.try_complete();
    }

    pub(crate) fn cleanup_failed(&self) -> bool {
        self.cleanup_failed.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_for_completion(&self) {
        self.wait_until(|| self.completed.load(Ordering::Acquire))
            .await;
    }

    /// 初始化失败后再次重试前，也必须等待刚刚丢弃的 transport 实际收束。
    pub(crate) async fn wait_for_pending_transport_release(&self) {
        self.wait_until(|| self.active_transports.load(Ordering::Acquire) == 0)
            .await;
    }

    async fn wait_until(&self, predicate: impl Fn() -> bool) {
        loop {
            let notified = self.changed.notified();
            if predicate() {
                return;
            }
            notified.await;
        }
    }

    fn transport_settled(&self, released: bool) {
        if !released {
            self.cleanup_failed.store(true, Ordering::Release);
        }
        let previous = self.active_transports.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "transport registration must be settled once");
        self.changed.notify_waiters();
        self.try_complete();
    }

    fn try_complete(&self) {
        if !self.connect_finished.load(Ordering::Acquire) {
            return;
        }
        if self.cancellation_requested.load(Ordering::Acquire)
            && self.active_transports.load(Ordering::Acquire) != 0
        {
            return;
        }
        if !self.completed.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }
}

/// `PendingConnectTransport` 的一次性释放登记；显式 close 与析构兜底只允许结算一次。
pub(crate) struct PendingConnectTransportRegistration {
    fence: Arc<McpConnectReleaseFence>,
    settled: AtomicBool,
}

impl PendingConnectTransportRegistration {
    fn settle(&self, released: bool) {
        if !self.settled.swap(true, Ordering::AcqRel) {
            self.fence.transport_settled(released);
        }
    }

    fn is_settled(&self) -> bool {
        self.settled.load(Ordering::Acquire)
    }
}

/// 在 `serve_client` 尚未交给 `McpClient` 前包装 transport，析构时也会等待 close。
struct PendingConnectTransport<T>
where
    T: Transport<RoleClient> + Send + 'static,
{
    inner: Option<T>,
    registration: Option<PendingConnectTransportRegistration>,
}

impl<T> PendingConnectTransport<T>
where
    T: Transport<RoleClient> + Send + 'static,
{
    fn new(inner: T, registration: PendingConnectTransportRegistration) -> Self {
        Self {
            inner: Some(inner),
            registration: Some(registration),
        }
    }

    fn registration(&self) -> &PendingConnectTransportRegistration {
        // `registration` 只会在 Drop 中取走；Transport trait 方法与 Drop 不会并发执行。
        self.registration
            .as_ref()
            .expect("pending transport registration exists until drop")
    }

    fn inner_mut(&mut self) -> &mut T {
        // `inner` 只会在 Drop 中取走；rmcp 不会在 close future 执行期间再次调用 transport 方法。
        self.inner
            .as_mut()
            .expect("pending transport inner exists until drop")
    }
}

impl<T> Transport<RoleClient> for PendingConnectTransport<T>
where
    T: Transport<RoleClient> + Send + 'static,
{
    type Error = T::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.inner_mut().send(item)
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.inner_mut().receive()
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        let result = self.inner_mut().close().await;
        self.registration().settle(result.is_ok());
        result
    }
}

impl<T> Drop for PendingConnectTransport<T>
where
    T: Transport<RoleClient> + Send + 'static,
{
    fn drop(&mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };
        if registration.is_settled() {
            return;
        }
        let Some(mut inner) = self.inner.take() else {
            registration.settle(false);
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            registration.settle(false);
            return;
        };
        std::mem::drop(runtime.spawn(async move {
            let released = matches!(
                time::timeout(
                    Duration::from_secs(MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS)
                        .saturating_add(PENDING_CONNECT_CLOSE_GRACE),
                    inner.close(),
                )
                .await,
                Ok(Ok(()))
            );
            registration.settle(released);
        }));
    }
}

#[derive(Clone)]
struct AcnMcpClientHandler {
    server_name: String,
    client_info: ClientInfo,
    progress_callback: Option<McpProgressCallback>,
}

#[derive(Debug, thiserror::Error)]
pub enum McpClientError {
    #[error("MCP server '{server}' stdio command I/O 失败: {source}")]
    StdioIo {
        server: String,
        #[source]
        source: std::io::Error,
    },
    #[error("MCP server '{server}' streamable_http client 初始化失败: {source}")]
    HttpClient {
        server: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("MCP server '{server}' bearer token 环境变量 '{env_var}' 未设置或为空")]
    MissingBearerToken { server: String, env_var: String },
    #[error("MCP server '{server}' initialize 超时: {timeout_secs}s")]
    StartupTimeout { server: String, timeout_secs: u64 },
    #[error("MCP server '{server}' initialize 连接失败: {message}")]
    InitializeConnection { server: String, message: String },
    #[error("MCP server '{server}' initialize 失败: {message}")]
    Initialize { server: String, message: String },
    #[error("MCP server '{server}' tools/list 超时: {timeout_secs}s")]
    ListToolsTimeout { server: String, timeout_secs: u64 },
    #[error("MCP server '{server}' tools/list 连接失败: {message}")]
    ListToolsConnection { server: String, message: String },
    #[error("MCP server '{server}' tools/list 请求失败: {message}")]
    ListToolsRequest { server: String, message: String },
    #[error("MCP server '{server}' tools/list pagination 超过安全上限 {limit}")]
    ListToolsPaginationLimit { server: String, limit: usize },
    #[error("MCP server '{server}' tools/list 工具数量超过安全上限 {limit}")]
    ListToolsToolLimit { server: String, limit: usize },
    #[error("MCP server '{server}' tool '{tool}' 参数必须是 JSON object")]
    ToolArgumentsNotObject { server: String, tool: String },
    #[error("MCP server '{server}' tool '{tool}' 参数非法: {message}")]
    ToolInvalidParams {
        server: String,
        tool: String,
        message: String,
    },
    #[error("MCP server '{server}' tool '{tool}' 调用超时: {timeout_secs}s")]
    ToolTimeout {
        server: String,
        tool: String,
        timeout_secs: u64,
    },
    #[error("MCP server '{server}' tool '{tool}' 调用被取消: {message}")]
    ToolCancelled {
        server: String,
        tool: String,
        message: String,
    },
    #[error("MCP server '{server}' tool '{tool}' 远端请求失败: {message}")]
    ToolRequest {
        server: String,
        tool: String,
        message: String,
    },
    #[error("MCP server '{server}' tool '{tool}' 连接失败: {message}")]
    ToolConnection {
        server: String,
        tool: String,
        message: String,
    },
}

impl McpClientError {
    /// 仅 transport/driver 已不可继续时才允许 manager 淘汰共享 client。
    pub(crate) fn is_connection_scoped(&self) -> bool {
        matches!(
            self,
            Self::ListToolsConnection { .. } | Self::ToolConnection { .. }
        )
    }

    /// 仅连接建立阶段的短暂 transport 故障才进行内部退避；认证、配置和协议错误直接失败。
    pub(crate) fn is_retryable_connection_establishment_failure(&self) -> bool {
        match self {
            Self::StdioIo { source, .. } => retryable_io_error(source),
            Self::StartupTimeout { .. }
            | Self::InitializeConnection { .. }
            | Self::ListToolsConnection { .. }
            | Self::ListToolsTimeout { .. } => true,
            Self::HttpClient { .. }
            | Self::MissingBearerToken { .. }
            | Self::Initialize { .. }
            | Self::ListToolsRequest { .. }
            | Self::ListToolsPaginationLimit { .. }
            | Self::ListToolsToolLimit { .. }
            | Self::ToolArgumentsNotObject { .. }
            | Self::ToolInvalidParams { .. }
            | Self::ToolTimeout { .. }
            | Self::ToolCancelled { .. }
            | Self::ToolRequest { .. }
            | Self::ToolConnection { .. } => false,
        }
    }
}

impl McpClient {
    pub(crate) async fn connect(
        server_name: String,
        server: &McpServerConfig,
        workspace_root: &Path,
        progress_callback: Option<McpProgressCallback>,
        connect_release_fence: Arc<McpConnectReleaseFence>,
    ) -> Result<Self, McpClientError> {
        let startup_timeout = Duration::from_secs(server.startup_timeout_secs());
        let tool_timeout = Duration::from_secs(server.tool_timeout_secs());
        let stderr = Arc::new(Mutex::new(String::new()));
        let handler = AcnMcpClientHandler {
            server_name: server_name.clone(),
            client_info: client_info(),
            progress_callback,
        };
        let (transport, uses_streamable_http) = match server
            .transport_config(&server_name)
            .map_err(|err| McpClientError::Initialize {
                server: server_name.clone(),
                message: err.to_string(),
            })? {
            McpTransportConfig::Stdio {
                command,
                args,
                env,
                env_vars,
                cwd,
            } => {
                let (transport, child_stderr) = stdio_transport(
                    &server_name,
                    command,
                    args,
                    env,
                    env_vars,
                    cwd,
                    workspace_root,
                    Arc::clone(&connect_release_fence),
                )
                .await?;
                spawn_stderr_capture(child_stderr, Arc::clone(&stderr));
                (PendingTransport::Stdio(transport), false)
            }
            McpTransportConfig::StreamableHttp {
                url,
                bearer_token_env_var,
            } => {
                let transport = streamable_http_transport(
                    &server_name,
                    url,
                    bearer_token_env_var,
                    tool_timeout,
                    Arc::clone(&connect_release_fence),
                )
                .await?;
                (PendingTransport::StreamableHttp(transport), true)
            }
        };
        let service = match transport {
            PendingTransport::Stdio(transport) => time::timeout(
                startup_timeout,
                rmcp::serve_client(handler.clone(), transport),
            )
            .await
            .map_err(|_| McpClientError::StartupTimeout {
                server: server_name.clone(),
                timeout_secs: server.startup_timeout_secs(),
            })?
            .map_err(|err| initialize_service_error(&server_name, err))?,
            PendingTransport::StreamableHttp(transport) => time::timeout(
                startup_timeout,
                rmcp::serve_client(handler.clone(), transport),
            )
            .await
            .map_err(|_| McpClientError::StartupTimeout {
                server: server_name.clone(),
                timeout_secs: server.startup_timeout_secs(),
            })?
            .map_err(|err| initialize_service_error(&server_name, err))?,
        };

        let server_info = service.peer().peer_info().cloned();
        let shutdown_token = service.cancellation_token();
        let lifecycle_cancel = CancellationToken::new();
        Ok(Self {
            server_name,
            service: Mutex::new(service),
            shutdown_token: StdMutex::new(Some(shutdown_token)),
            lifecycle_cancel,
            server_info,
            stderr,
            tool_timeout,
            uses_streamable_http,
        })
    }

    pub fn server_info(&self) -> Option<ServerInfo> {
        self.server_info.clone()
    }

    pub(crate) fn server_name(&self) -> &str {
        &self.server_name
    }

    /// 一个 MCP tool admission 的统一绝对 deadline；只读实时校验与随后 tools/call 必须共用它。
    pub(crate) fn next_tool_deadline(&self) -> time::Instant {
        time::Instant::now() + self.tool_timeout
    }

    pub async fn stderr_excerpt(&self) -> String {
        self.stderr.lock().await.clone()
    }

    pub async fn list_tools(
        &self,
        timeout_secs: u64,
        page_limit: usize,
        tool_limit: usize,
    ) -> Result<Vec<Tool>, McpClientError> {
        time::timeout(
            Duration::from_secs(timeout_secs),
            self.list_tools_inner(page_limit, tool_limit),
        )
        .await
        .map_err(|_| McpClientError::ListToolsTimeout {
            server: self.server_name.clone(),
            timeout_secs,
        })?
    }

    async fn list_tools_inner(
        &self,
        page_limit: usize,
        tool_limit: usize,
    ) -> Result<Vec<Tool>, McpClientError> {
        let peer = self.service.lock().await.peer().clone();
        let mut tools = Vec::new();
        let mut cursor = None;
        for _ in 0..page_limit {
            let params = Some(PaginatedRequestParams { meta: None, cursor });
            let result: ListToolsResult = peer
                .list_tools(params)
                .await
                .map_err(|err| self.list_tools_service_error(err))?;
            tools.extend(result.tools);
            if tools.len() > tool_limit {
                return Err(McpClientError::ListToolsToolLimit {
                    server: self.server_name.clone(),
                    limit: tool_limit,
                });
            }
            cursor = result.next_cursor;
            if cursor.is_none() {
                return Ok(tools);
            }
        }
        Err(McpClientError::ListToolsPaginationLimit {
            server: self.server_name.clone(),
            limit: page_limit,
        })
    }

    /// 只读调用前的实时 tools/list 校验，复用 tools/call 的绝对 deadline 与 request-scoped
    /// cancellation，不能因 HTTP worker 堵塞而拖住整个共享 session。
    pub async fn list_tools_cancellable(
        &self,
        page_limit: usize,
        tool_limit: usize,
        cancellation: Option<CancellationToken>,
    ) -> Result<Vec<Tool>, McpClientError> {
        self.list_tools_cancellable_until(
            page_limit,
            tool_limit,
            cancellation,
            self.next_tool_deadline(),
        )
        .await
    }

    /// 在给定的 tool admission deadline 内执行实时 `tools/list`。
    pub(crate) async fn list_tools_cancellable_until(
        &self,
        page_limit: usize,
        tool_limit: usize,
        cancellation: Option<CancellationToken>,
        deadline: time::Instant,
    ) -> Result<Vec<Tool>, McpClientError> {
        let timeout_secs = self.tool_timeout.as_secs();
        let peer = tokio::select! {
            biased;
            () = self.lifecycle_cancel.cancelled() => {
                return Err(self.list_tools_cancelled_error(LIFECYCLE_CANCELLATION_REASON));
            }
            () = wait_for_optional_cancellation(cancellation.clone()) => {
                return Err(self.list_tools_cancelled_error(TURN_CANCELLATION_REASON));
            }
            () = time::sleep_until(deadline) => {
                return Err(self.list_tools_timeout_error(timeout_secs));
            }
            peer = async { self.service.lock().await.peer().clone() } => peer,
        };
        let mut tools = Vec::new();
        let mut cursor = None;
        for _ in 0..page_limit {
            let params = PaginatedRequestParams { meta: None, cursor };
            let http_request_cancellation = self.uses_streamable_http.then(CancellationToken::new);
            let mut request =
                ClientRequest::ListToolsRequest(rmcp::model::ListToolsRequest::with_param(params));
            if let Some(http_request_cancellation) = &http_request_cancellation {
                request
                    .extensions_mut()
                    .insert(AcnMcpHttpRequestCancellation(
                        http_request_cancellation.clone(),
                    ));
            }
            let options = PeerRequestOptions {
                timeout: Some(deadline.saturating_duration_since(time::Instant::now())),
                meta: self
                    .uses_streamable_http
                    .then(|| local_request_deadline_meta(deadline)),
            };
            let handle = tokio::select! {
                biased;
                () = self.lifecycle_cancel.cancelled() => {
                    return Err(self.list_tools_cancelled_error(LIFECYCLE_CANCELLATION_REASON));
                }
                () = wait_for_optional_cancellation(cancellation.clone()) => {
                    return Err(self.list_tools_cancelled_error(TURN_CANCELLATION_REASON));
                }
                () = time::sleep_until(deadline) => {
                    return Err(self.list_tools_timeout_error(timeout_secs));
                }
                handle = peer.send_cancellable_request(request, options) => handle
                    .map_err(|error| self.list_tools_service_error(error))?,
            };
            let result = self
                .await_list_tools_response_with_cancellation(
                    timeout_secs,
                    deadline,
                    handle,
                    cancellation.clone(),
                    http_request_cancellation,
                )
                .await?;
            let ServerResult::ListToolsResult(result) = result else {
                return Err(McpClientError::ListToolsRequest {
                    server: self.server_name.clone(),
                    message: "unexpected MCP response for tools/list".to_string(),
                });
            };
            tools.extend(result.tools);
            if tools.len() > tool_limit {
                return Err(McpClientError::ListToolsToolLimit {
                    server: self.server_name.clone(),
                    limit: tool_limit,
                });
            }
            cursor = result.next_cursor;
            if cursor.is_none() {
                return Ok(tools);
            }
        }
        Err(McpClientError::ListToolsPaginationLimit {
            server: self.server_name.clone(),
            limit: page_limit,
        })
    }

    pub async fn call_tool(
        &self,
        tool_name: &str,
        arguments: Option<Value>,
        progress_token: Option<String>,
    ) -> Result<CallToolResult, McpClientError> {
        self.call_tool_cancellable(tool_name, arguments, progress_token, None)
            .await
    }

    /// 在不影响共享 session 的前提下，允许当前 turn 取消其自己的 tools/call request。
    pub async fn call_tool_cancellable(
        &self,
        tool_name: &str,
        arguments: Option<Value>,
        progress_token: Option<String>,
        cancellation: Option<CancellationToken>,
    ) -> Result<CallToolResult, McpClientError> {
        self.call_tool_cancellable_until(
            tool_name,
            arguments,
            progress_token,
            cancellation,
            self.next_tool_deadline(),
        )
        .await
    }

    /// 在给定的 tool admission deadline 内执行 tools/call。
    pub(crate) async fn call_tool_cancellable_until(
        &self,
        tool_name: &str,
        arguments: Option<Value>,
        progress_token: Option<String>,
        cancellation: Option<CancellationToken>,
        deadline: time::Instant,
    ) -> Result<CallToolResult, McpClientError> {
        let arguments = match arguments {
            Some(Value::Object(map)) => Some(map),
            None | Some(Value::Null) => None,
            Some(_) => {
                return Err(McpClientError::ToolArgumentsNotObject {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                });
            }
        };
        let mut meta = progress_token
            .map(|token| {
                Meta::with_progress_token(ProgressToken(NumberOrString::String(Arc::from(token))))
            })
            .unwrap_or_default();
        let params = CallToolRequestParams {
            meta: None,
            name: tool_name.to_string().into(),
            arguments,
            task: None,
        };
        let timeout_secs = self.tool_timeout.as_secs();
        // 共享 transport 的 outbound queue、service mutex 与 response 都属于同一个请求生命
        // 周期；deadline 必须从 admission 前开始，不能只包住拿到 RequestHandle 之后的 rx.await。
        if self.uses_streamable_http {
            meta.extend(local_request_deadline_meta(deadline));
        }
        let peer = tokio::select! {
            biased;
            () = self.lifecycle_cancel.cancelled() => {
                return Err(McpClientError::ToolCancelled {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    message: "MCP client lifecycle was replaced or disabled".to_string(),
                });
            }
            () = wait_for_optional_cancellation(cancellation.clone()) => {
                return Err(McpClientError::ToolCancelled {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    message: TURN_CANCELLATION_REASON.to_string(),
                });
            }
            () = time::sleep_until(deadline) => {
                return Err(McpClientError::ToolTimeout {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    timeout_secs,
                });
            }
            peer = async { self.service.lock().await.peer().clone() } => peer,
        };
        let http_request_cancellation = self.uses_streamable_http.then(CancellationToken::new);
        let mut request = ClientRequest::CallToolRequest(Request::new(params));
        if let Some(http_request_cancellation) = &http_request_cancellation {
            request
                .extensions_mut()
                .insert(AcnMcpHttpRequestCancellation(
                    http_request_cancellation.clone(),
                ));
        }
        let options = PeerRequestOptions {
            timeout: Some(deadline.saturating_duration_since(time::Instant::now())),
            meta: Some(meta),
        };
        let handle = tokio::select! {
            biased;
            () = self.lifecycle_cancel.cancelled() => {
                return Err(McpClientError::ToolCancelled {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    message: "MCP client lifecycle was replaced or disabled".to_string(),
                });
            }
            () = wait_for_optional_cancellation(cancellation.clone()) => {
                return Err(McpClientError::ToolCancelled {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    message: TURN_CANCELLATION_REASON.to_string(),
                });
            }
            () = time::sleep_until(deadline) => {
                return Err(McpClientError::ToolTimeout {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    timeout_secs,
                });
            }
            handle = peer.send_cancellable_request(request, options) => handle
                .map_err(|err| self.tool_service_error(tool_name, timeout_secs, err))?,
        };
        let response = self
            .await_tool_response_with_cancellation(
                tool_name,
                timeout_secs,
                deadline,
                handle,
                cancellation,
                http_request_cancellation,
            )
            .await?;
        match response {
            ServerResult::CallToolResult(result) => Ok(result),
            _ => Err(McpClientError::ToolRequest {
                server: self.server_name.clone(),
                tool: tool_name.to_string(),
                message: "unexpected MCP response for tools/call".into(),
            }),
        }
    }

    /// 在 response 等待期以 request-scoped notification 收束当前 turn，不影响共享 session。
    async fn await_tool_response_with_cancellation(
        &self,
        tool_name: &str,
        timeout_secs: u64,
        deadline: time::Instant,
        handle: rmcp::service::RequestHandle<RoleClient>,
        turn_cancellation: Option<CancellationToken>,
        http_request_cancellation: Option<CancellationToken>,
    ) -> Result<ServerResult, McpClientError> {
        let rmcp::service::RequestHandle {
            rx,
            options,
            peer,
            id,
            ..
        } = handle;
        let mut cancellation_on_drop =
            RequestCancellationOnDrop::new(peer.clone(), id.clone(), http_request_cancellation);
        let receive_response = async move {
            rx.await
                .map_err(|_| rmcp::service::ServiceError::TransportClosed)?
        };
        tokio::pin!(receive_response);
        let http_timeout_cancel_peer = peer.clone();
        let http_timeout_cancel_request_id = id.clone();
        let response = match options.timeout {
            Some(_) => {
                tokio::select! {
                    biased;
                    () = self.lifecycle_cancel.cancelled() => {
                        cancellation_on_drop.cancel_and_disarm(LIFECYCLE_CANCELLATION_REASON);
                        return Err(McpClientError::ToolCancelled {
                            server: self.server_name.clone(),
                            tool: tool_name.to_string(),
                            message: "MCP client lifecycle was replaced or disabled".to_string(),
                        });
                    }
                    () = wait_for_optional_cancellation(turn_cancellation) => {
                        cancellation_on_drop.cancel_and_disarm(TURN_CANCELLATION_REASON);
                        return Err(McpClientError::ToolCancelled {
                            server: self.server_name.clone(),
                            tool: tool_name.to_string(),
                            message: TURN_CANCELLATION_REASON.to_string(),
                        });
                    }
                    () = time::sleep_until(deadline) => {
                        cancellation_on_drop
                            .cancel_and_disarm(TOOL_TIMEOUT_CANCELLATION_REASON);
                        return Err(McpClientError::ToolTimeout {
                            server: self.server_name.clone(),
                            tool: tool_name.to_string(),
                            timeout_secs,
                        });
                    }
                    response = &mut receive_response => response,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    () = self.lifecycle_cancel.cancelled() => {
                        cancellation_on_drop.cancel_and_disarm(LIFECYCLE_CANCELLATION_REASON);
                        return Err(McpClientError::ToolCancelled {
                            server: self.server_name.clone(),
                            tool: tool_name.to_string(),
                            message: "MCP client lifecycle was replaced or disabled".to_string(),
                        });
                    }
                    () = wait_for_optional_cancellation(turn_cancellation) => {
                        cancellation_on_drop.cancel_and_disarm(TURN_CANCELLATION_REASON);
                        return Err(McpClientError::ToolCancelled {
                            server: self.server_name.clone(),
                            tool: tool_name.to_string(),
                            message: TURN_CANCELLATION_REASON.to_string(),
                        });
                    }
                    response = &mut receive_response => response,
                }
            }
        };
        cancellation_on_drop.disarm();
        match response {
            Err(error) if service_error_is_http_tool_timeout(&error) => {
                // HTTP response body 提前中断后，服务端仍可能继续执行；补发 request-scoped cancel。
                schedule_request_cancellation(
                    http_timeout_cancel_peer,
                    http_timeout_cancel_request_id,
                    TOOL_TIMEOUT_CANCELLATION_REASON,
                );
                Err(McpClientError::ToolTimeout {
                    server: self.server_name.clone(),
                    tool: tool_name.to_string(),
                    timeout_secs,
                })
            }
            response => {
                response.map_err(|error| self.tool_service_error(tool_name, timeout_secs, error))
            }
        }
    }

    /// 等待实时 tools/list 的单页响应；取消只针对该 request，不能关闭共享 client。
    async fn await_list_tools_response_with_cancellation(
        &self,
        timeout_secs: u64,
        deadline: time::Instant,
        handle: rmcp::service::RequestHandle<RoleClient>,
        turn_cancellation: Option<CancellationToken>,
        http_request_cancellation: Option<CancellationToken>,
    ) -> Result<ServerResult, McpClientError> {
        let rmcp::service::RequestHandle { rx, peer, id, .. } = handle;
        let mut cancellation_on_drop =
            RequestCancellationOnDrop::new(peer.clone(), id.clone(), http_request_cancellation);
        let receive_response = async move {
            rx.await
                .map_err(|_| rmcp::service::ServiceError::TransportClosed)?
        };
        tokio::pin!(receive_response);
        let http_timeout_cancel_peer = peer.clone();
        let http_timeout_cancel_request_id = id.clone();
        let response = tokio::select! {
            biased;
            () = self.lifecycle_cancel.cancelled() => {
                cancellation_on_drop.cancel_and_disarm(LIFECYCLE_CANCELLATION_REASON);
                return Err(self.list_tools_cancelled_error(LIFECYCLE_CANCELLATION_REASON));
            }
            () = wait_for_optional_cancellation(turn_cancellation) => {
                cancellation_on_drop.cancel_and_disarm(TURN_CANCELLATION_REASON);
                return Err(self.list_tools_cancelled_error(TURN_CANCELLATION_REASON));
            }
            () = time::sleep_until(deadline) => {
                cancellation_on_drop.cancel_and_disarm(TOOL_TIMEOUT_CANCELLATION_REASON);
                return Err(self.list_tools_timeout_error(timeout_secs));
            }
            response = &mut receive_response => response,
        };
        cancellation_on_drop.disarm();
        match response {
            Err(error) if service_error_is_http_tool_timeout(&error) => {
                schedule_request_cancellation(
                    http_timeout_cancel_peer,
                    http_timeout_cancel_request_id,
                    TOOL_TIMEOUT_CANCELLATION_REASON,
                );
                Err(self.list_tools_timeout_error(timeout_secs))
            }
            response => response.map_err(|error| self.list_tools_service_error(error)),
        }
    }

    fn list_tools_timeout_error(&self, timeout_secs: u64) -> McpClientError {
        McpClientError::ListToolsTimeout {
            server: self.server_name.clone(),
            timeout_secs,
        }
    }

    fn list_tools_cancelled_error(&self, reason: &str) -> McpClientError {
        McpClientError::ListToolsRequest {
            server: self.server_name.clone(),
            message: format!("tools/list request was cancelled: {reason}"),
        }
    }

    fn list_tools_service_error(&self, error: rmcp::service::ServiceError) -> McpClientError {
        if service_error_is_connection_scoped(&error)
            || service_error_is_retryable_connection_establishment(&error)
        {
            McpClientError::ListToolsConnection {
                server: self.server_name.clone(),
                message: error.to_string(),
            }
        } else {
            McpClientError::ListToolsRequest {
                server: self.server_name.clone(),
                message: error.to_string(),
            }
        }
    }

    fn tool_service_error(
        &self,
        tool_name: &str,
        timeout_secs: u64,
        error: rmcp::service::ServiceError,
    ) -> McpClientError {
        let server = self.server_name.clone();
        let tool = tool_name.to_string();
        match error {
            rmcp::service::ServiceError::Timeout { .. } => McpClientError::ToolTimeout {
                server,
                tool,
                timeout_secs,
            },
            rmcp::service::ServiceError::Cancelled { reason } => McpClientError::ToolCancelled {
                server,
                tool,
                message: reason.unwrap_or_else(|| "<unknown>".to_string()),
            },
            rmcp::service::ServiceError::McpError(data)
                if data.code == ErrorCode::INVALID_PARAMS =>
            {
                McpClientError::ToolInvalidParams {
                    server,
                    tool,
                    message: data.message.to_string(),
                }
            }
            error if service_error_is_http_tool_timeout(&error) => McpClientError::ToolTimeout {
                server,
                tool,
                timeout_secs,
            },
            error if service_error_is_connection_scoped(&error) => McpClientError::ToolConnection {
                server,
                tool,
                message: error.to_string(),
            },
            error => McpClientError::ToolRequest {
                server,
                tool,
                message: error.to_string(),
            },
        }
    }

    /// 关闭 driver 和底层 transport，确保持有旧 Arc 的 in-flight caller 也能被收束。
    /// 返回 `false` 代表 driver 未在受限时间内确认退出；调用方不得在同一 server 上立即新建连接。
    pub async fn shutdown(&self) -> bool {
        self.lifecycle_cancel.cancel();
        let mut service = self.service.lock().await;
        match service
            .close_with_timeout(Duration::from_secs(MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS))
            .await
        {
            Ok(Some(_)) => true,
            Ok(None) => {
                log::warn!(
                    "MCP server '{}' transport shutdown timed out after {}s",
                    self.server_name,
                    MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS
                );
                false
            }
            Err(error) => {
                log::warn!(
                    "MCP server '{}' transport shutdown join failed: {error}",
                    self.server_name
                );
                false
            }
        }
    }

    /// 无法 await 的析构兜底；正常 disable/reconnect 路径必须调用 `shutdown().await`。
    pub fn request_shutdown(&self) {
        self.lifecycle_cancel.cancel();
        let mut shutdown_token = match self.shutdown_token.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(token) = shutdown_token.take() {
            token.cancel();
        }
    }
}

async fn wait_for_optional_cancellation(cancellation: Option<CancellationToken>) {
    match cancellation {
        Some(cancellation) => cancellation.cancelled().await,
        None => std::future::pending::<()>().await,
    }
}

/// 已入队 request 被取消时，立即中止本地 HTTP POST，并尽力向 server 发送 request-scoped cancellation。
struct RequestCancellationOnDrop {
    peer: Peer<RoleClient>,
    request_id: RequestId,
    http_request_cancellation: Option<CancellationToken>,
    armed: bool,
}

impl RequestCancellationOnDrop {
    fn new(
        peer: Peer<RoleClient>,
        request_id: RequestId,
        http_request_cancellation: Option<CancellationToken>,
    ) -> Self {
        Self {
            peer,
            request_id,
            http_request_cancellation,
            armed: true,
        }
    }

    fn cancel_and_disarm(&mut self, reason: &'static str) {
        if self.armed {
            self.armed = false;
            if let Some(cancellation) = &self.http_request_cancellation {
                // rmcp 的同 session HTTP worker 串行；必须先析构正在 await 的 reqwest future，
                // notification 才有机会进入 worker，且不应等服务端的慢响应自行结束。
                cancellation.cancel();
            }
            schedule_request_cancellation(self.peer.clone(), self.request_id.clone(), reason);
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RequestCancellationOnDrop {
    fn drop(&mut self) {
        self.cancel_and_disarm(CALLER_ABORT_CANCELLATION_REASON);
    }
}

/// cancellation notification 不等待 transport worker；慢 HTTP worker 不能拖慢 turn 的本地收束。
fn schedule_request_cancellation(
    peer: Peer<RoleClient>,
    request_id: RequestId,
    reason: &'static str,
) {
    std::mem::drop(tokio::spawn(async move {
        if let Err(error) = peer
            .notify_cancelled(CancelledNotificationParam {
                request_id,
                reason: Some(reason.to_string()),
            })
            .await
        {
            log::debug!(target: "mcp", "MCP cancellation notification failed: {error}");
        }
    }));
}

fn service_error_is_connection_scoped(error: &rmcp::service::ServiceError) -> bool {
    // rmcp 将单个 HTTP POST 的 4xx/5xx、响应体解析失败等也包装成 TransportSend；
    // 这些不会必然终止 session/driver，不能据此淘汰共享 client。
    matches!(error, rmcp::service::ServiceError::TransportClosed)
}

/// discovery 属于连接建立阶段；仅把可明确识别为 connect/timeout/I/O 暂态的 TransportSend
/// 放入有限退避。正常 tools/call 继续按 request-scoped error 处理，不能据此重放调用。
fn service_error_is_retryable_connection_establishment(
    error: &rmcp::service::ServiceError,
) -> bool {
    let rmcp::service::ServiceError::TransportSend(transport_error) = error else {
        return false;
    };
    let source = transport_error.error.as_ref();
    source
        .downcast_ref::<std::io::Error>()
        .is_some_and(retryable_io_error)
        || source
            .downcast_ref::<StreamableHttpError<AcnMcpHttpError>>()
            .is_some_and(retryable_streamable_http_establishment_error)
}

fn initialize_service_error(
    server_name: &str,
    error: rmcp::service::ClientInitializeError,
) -> McpClientError {
    if client_initialize_error_is_retryable(&error) {
        McpClientError::InitializeConnection {
            server: server_name.to_string(),
            message: error.to_string(),
        }
    } else {
        McpClientError::Initialize {
            server: server_name.to_string(),
            message: error.to_string(),
        }
    }
}

fn client_initialize_error_is_retryable(error: &rmcp::service::ClientInitializeError) -> bool {
    match error {
        rmcp::service::ClientInitializeError::ConnectionClosed(_)
        | rmcp::service::ClientInitializeError::Cancelled => true,
        rmcp::service::ClientInitializeError::TransportError {
            error: transport_error,
            ..
        } => {
            let source = transport_error.error.as_ref();
            source
                .downcast_ref::<std::io::Error>()
                .is_some_and(retryable_io_error)
                || source
                    .downcast_ref::<StreamableHttpError<AcnMcpHttpError>>()
                    .is_some_and(retryable_streamable_http_establishment_error)
        }
        rmcp::service::ClientInitializeError::ExpectedInitResponse(_)
        | rmcp::service::ClientInitializeError::ExpectedInitResult(_)
        | rmcp::service::ClientInitializeError::ConflictInitResponseId(_, _)
        | rmcp::service::ClientInitializeError::JsonRpcError(_) => false,
    }
}

fn retryable_streamable_http_establishment_error(
    error: &StreamableHttpError<AcnMcpHttpError>,
) -> bool {
    match error {
        StreamableHttpError::Client(AcnMcpHttpError::Request(error)) => {
            error.is_connect() || error.is_timeout()
        }
        StreamableHttpError::Client(
            AcnMcpHttpError::ToolHttpResponseTimeout | AcnMcpHttpError::RequestCancelled,
        ) => false,
        StreamableHttpError::Io(error) => retryable_io_error(error),
        StreamableHttpError::UnexpectedEndOfStream
        | StreamableHttpError::TransportChannelClosed => true,
        StreamableHttpError::Sse(_)
        | StreamableHttpError::UnexpectedServerResponse(_)
        | StreamableHttpError::UnexpectedContentType(_)
        | StreamableHttpError::ServerDoesNotSupportSse
        | StreamableHttpError::ServerDoesNotSupportDeleteSession
        | StreamableHttpError::TokioJoinError(_)
        | StreamableHttpError::Deserialize(_)
        | StreamableHttpError::MissingSessionIdInResponse
        | StreamableHttpError::AuthRequired(_) => false,
    }
}

fn retryable_io_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::WouldBlock
    )
}

fn service_error_is_http_tool_timeout(error: &rmcp::service::ServiceError) -> bool {
    let rmcp::service::ServiceError::TransportSend(transport_error) = error else {
        return false;
    };
    matches!(
        transport_error
            .error
            .downcast_ref::<StreamableHttpError<AcnMcpHttpError>>(),
        Some(StreamableHttpError::Client(
            AcnMcpHttpError::ToolHttpResponseTimeout
        ))
    )
}

enum PendingTransport {
    Stdio(PendingConnectTransport<TokioChildProcess>),
    StreamableHttp(PendingConnectTransport<StreamableHttpClientTransport<AcnMcpHttpClient>>),
}

/// Streamable HTTP 适配层的错误。JSON body 的本地 deadline 与底层 reqwest 失败必须可区分，
/// 避免把 SSE stream 的整个请求生命周期也错误地提前中断。
#[derive(Debug, thiserror::Error)]
enum AcnMcpHttpError {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("tools/call HTTP response exceeded the local release deadline")]
    ToolHttpResponseTimeout,
    #[error("MCP HTTP request was cancelled locally")]
    RequestCancelled,
}

#[derive(Clone)]
struct AcnMcpHttpClient {
    inner: reqwest::Client,
    protocol_version: Arc<StdMutex<String>>,
    fallback_tool_http_response_timeout: Duration,
}

impl AcnMcpHttpClient {
    fn new(inner: reqwest::Client, fallback_tool_http_response_timeout: Duration) -> Self {
        Self {
            inner,
            protocol_version: Arc::new(StdMutex::new(MCP_PROTOCOL_VERSION.to_string())),
            fallback_tool_http_response_timeout,
        }
    }

    fn protocol_version(&self) -> String {
        match self.protocol_version.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn update_protocol_version(&self, message: &ServerJsonRpcMessage) {
        let ServerJsonRpcMessage::Response(response) = message else {
            return;
        };
        let ServerResult::InitializeResult(result) = &response.result else {
            return;
        };
        let mut guard = match self.protocol_version.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = result.protocol_version.to_string();
    }

    fn request_headers(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header(HEADER_PROTOCOL_VERSION, self.protocol_version())
    }
}

fn local_request_deadline_meta(deadline: time::Instant) -> Meta {
    let deadline_millis = u64::try_from(
        deadline
            .saturating_duration_since(*MCP_TOOL_DEADLINE_EPOCH)
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let mut meta = Meta::default();
    meta.insert(
        ACN_LOCAL_TOOL_DEADLINE_META_FIELD.to_string(),
        Value::Number(deadline_millis.into()),
    );
    meta
}

/// 从即将发出的本地受限 request 移除绝对 deadline。该字段只能留在 adapter 内，不能泄露给 MCP server。
fn take_acn_local_tool_deadline(
    message: &mut rmcp::model::ClientJsonRpcMessage,
) -> Option<time::Instant> {
    let rmcp::model::ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    let meta = match &mut request.request {
        ClientRequest::CallToolRequest(call) => call.extensions.get_mut::<Meta>(),
        ClientRequest::ListToolsRequest(list) => list.extensions.get_mut::<Meta>(),
        _ => None,
    };
    let deadline_millis = meta?.remove(ACN_LOCAL_TOOL_DEADLINE_META_FIELD)?.as_u64()?;
    (*MCP_TOOL_DEADLINE_EPOCH).checked_add(Duration::from_millis(deadline_millis))
}

/// 从 request extensions 取得本地取消 token。extensions 是 rmcp 的进程内载体，不会写入 JSON-RPC。
fn acn_http_request_cancellation(
    message: &rmcp::model::ClientJsonRpcMessage,
) -> Option<CancellationToken> {
    let rmcp::model::ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    request
        .request
        .extensions()
        .get::<AcnMcpHttpRequestCancellation>()
        .map(|cancellation| cancellation.0.clone())
}

impl StreamableHttpClient for AcnMcpHttpClient {
    type Error = AcnMcpHttpError;

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        let mut request_builder = self.request_headers(
            self.inner
                .get(uri.as_ref())
                .header(
                    reqwest::header::ACCEPT,
                    [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "),
                )
                .header(HEADER_SESSION_ID, session_id.as_ref()),
        );
        if let Some(last_event_id) = last_event_id {
            request_builder = request_builder.header(HEADER_LAST_EVENT_ID, last_event_id);
        }
        if let Some(auth_header) = auth_token {
            request_builder = request_builder.bearer_auth(auth_header);
        }
        let response = request_builder
            .send()
            .await
            .map_err(streamable_http_request_error)?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        let response = response
            .error_for_status()
            .map_err(streamable_http_request_error)?;
        ensure_streamable_http_content_type(response.headers().get(reqwest::header::CONTENT_TYPE))?;
        Ok(SseStream::from_bytes_stream(response.bytes_stream()).boxed())
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_token: Option<String>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        let mut request_builder = self.request_headers(self.inner.delete(uri.as_ref()));
        if let Some(auth_header) = auth_token {
            request_builder = request_builder.bearer_auth(auth_header);
        }
        let response = request_builder
            .header(HEADER_SESSION_ID, session_id.as_ref())
            .send()
            .await
            .map_err(streamable_http_request_error)?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Ok(());
        }
        let _response = response
            .error_for_status()
            .map_err(streamable_http_request_error)?;
        Ok(())
    }

    async fn post_message(
        &self,
        uri: Arc<str>,
        mut message: rmcp::model::ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        let mut request = self.request_headers(self.inner.post(uri.as_ref()).header(
            reqwest::header::ACCEPT,
            [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "),
        ));
        let auth_token_present = auth_token.is_some();
        if let Some(auth_header) = auth_token {
            request = request.bearer_auth(auth_header);
        }
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        // rmcp 的同 session HTTP worker 需要先拿到 response headers 才能继续处理下一条 request。
        // ACN 受限 request 从 caller admission 起带绝对 deadline；adapter 在发 HTTP 前去掉本地元数据，
        // 使 queue 等待、headers 与 JSON body 共享同一窗口。SSE 一旦建立不再受此 body 保护截断。
        let request_cancellation = acn_http_request_cancellation(&message);
        let is_tool_call = matches!(
            &message,
            rmcp::model::ClientJsonRpcMessage::Request(request)
                if matches!(&request.request, ClientRequest::CallToolRequest(_))
        );
        let tool_response_deadline = take_acn_local_tool_deadline(&mut message).or_else(|| {
            is_tool_call.then(|| time::Instant::now() + self.fallback_tool_http_response_timeout)
        });
        let send_request = request.json(&message).send();
        let response = match tool_response_deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    () = wait_for_optional_cancellation(request_cancellation.clone()) => {
                        return Err(StreamableHttpError::Client(AcnMcpHttpError::RequestCancelled));
                    }
                    response = time::timeout_at(deadline, send_request) => response
                        .map_err(|_| StreamableHttpError::Client(AcnMcpHttpError::ToolHttpResponseTimeout))?
                        .map_err(streamable_http_request_error)?,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    () = wait_for_optional_cancellation(request_cancellation.clone()) => {
                        return Err(StreamableHttpError::Client(AcnMcpHttpError::RequestCancelled));
                    }
                    response = send_request => response.map_err(streamable_http_request_error)?,
                }
            }
        };
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::from(
                streamable_http_auth_error_message(
                    response.headers().get(reqwest::header::WWW_AUTHENTICATE),
                    auth_token_present,
                ),
            )));
        }
        let status = response.status();
        if matches!(
            status,
            reqwest::StatusCode::ACCEPTED | reqwest::StatusCode::NO_CONTENT
        ) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let content_type = response.headers().get(reqwest::header::CONTENT_TYPE);
        let session_id = response
            .headers()
            .get(HEADER_SESSION_ID)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        match content_type {
            Some(ct) if ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) => {
                let client = self.clone();
                let event_stream = SseStream::from_bytes_stream(response.bytes_stream())
                    .map(move |event| {
                        if let Ok(event) = &event {
                            if let Some(data) = &event.data {
                                if let Ok(message) =
                                    serde_json::from_str::<ServerJsonRpcMessage>(data)
                                {
                                    client.update_protocol_version(&message);
                                }
                            }
                        }
                        event
                    })
                    .boxed();
                Ok(StreamableHttpPostResponse::Sse(event_stream, session_id))
            }
            Some(ct) if ct.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()) => {
                // JSON headers 与 body 共用一个保护窗口，防止“慢 headers + 慢 body”使 rmcp
                // worker 实际占用两倍 deadline。SSE 已在上一个分支返回，不受影响。
                let response_json = response.json();
                let message: ServerJsonRpcMessage = if let Some(deadline) = tool_response_deadline {
                    tokio::select! {
                        biased;
                        () = wait_for_optional_cancellation(request_cancellation) => {
                            return Err(StreamableHttpError::Client(AcnMcpHttpError::RequestCancelled));
                        }
                        message = time::timeout_at(deadline, response_json) => message
                            .map_err(|_| StreamableHttpError::Client(AcnMcpHttpError::ToolHttpResponseTimeout))?
                            .map_err(streamable_http_request_error)?,
                    }
                } else {
                    tokio::select! {
                        biased;
                        () = wait_for_optional_cancellation(request_cancellation) => {
                            return Err(StreamableHttpError::Client(AcnMcpHttpError::RequestCancelled));
                        }
                        message = response_json => message.map_err(streamable_http_request_error)?,
                    }
                };
                self.update_protocol_version(&message);
                Ok(StreamableHttpPostResponse::Json(message, session_id))
            }
            _ => Err(StreamableHttpError::UnexpectedContentType(
                content_type.map(|ct| String::from_utf8_lossy(ct.as_bytes()).to_string()),
            )),
        }
    }
}

fn streamable_http_auth_error_message(
    header: Option<&reqwest::header::HeaderValue>,
    auth_token_present: bool,
) -> String {
    let raw = header.and_then(|value| value.to_str().ok());
    let challenge = raw
        .map(auth_challenge_summary)
        .unwrap_or_else(|| "<missing WWW-Authenticate header>".to_string());
    if auth_token_present {
        return format!(
            "MCP server authentication failed: bearer token was rejected; WWW-Authenticate: {challenge}"
        );
    }
    if raw.is_some_and(is_oauth_or_interactive_auth_challenge) {
        return format!("{UNSUPPORTED_INTERACTIVE_AUTH_MESSAGE}; WWW-Authenticate: {challenge}");
    }
    format!(
        "MCP server requires authentication; configure bearer_token_env_var for bearer-token servers. {UNSUPPORTED_INTERACTIVE_AUTH_MESSAGE}; WWW-Authenticate: {challenge}"
    )
}

fn is_oauth_or_interactive_auth_challenge(value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    lowered.contains("resource_metadata")
        || lowered.contains("authorization_uri")
        || lowered.contains("authorization_url")
        || lowered.contains("oauth")
        || lowered.contains("openid")
}

fn auth_challenge_summary(value: &str) -> String {
    let lowered = value.to_ascii_lowercase();
    let mut parts = Vec::new();
    if let Some(scheme) = value.split_ascii_whitespace().next() {
        parts.push(scheme.trim_matches(',').to_string());
    }
    for marker in [
        "resource_metadata",
        "authorization_uri",
        "authorization_url",
        "oauth",
        "openid",
    ] {
        if lowered.contains(marker) {
            parts.push(marker.to_string());
        }
    }
    let redacted = redact_mcp_sensitive_text(value);
    if redacted == "<redacted>" && !parts.is_empty() {
        return parts.join(" ");
    }
    redacted
}

fn ensure_streamable_http_content_type(
    content_type: Option<&reqwest::header::HeaderValue>,
) -> Result<(), StreamableHttpError<AcnMcpHttpError>> {
    match content_type {
        Some(ct)
            if ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes())
                || ct.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()) =>
        {
            Ok(())
        }
        Some(ct) => Err(StreamableHttpError::UnexpectedContentType(Some(
            String::from_utf8_lossy(ct.as_bytes()).to_string(),
        ))),
        None => Err(StreamableHttpError::UnexpectedContentType(None)),
    }
}

fn streamable_http_request_error(error: reqwest::Error) -> StreamableHttpError<AcnMcpHttpError> {
    StreamableHttpError::Client(AcnMcpHttpError::Request(error))
}

impl ClientHandler for AcnMcpClientHandler {
    fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        let callback = self.progress_callback.clone();
        let server_name = self.server_name.clone();
        async move {
            emit_progress(callback, server_name, params);
        }
    }

    fn create_elicitation(
        &self,
        _request: CreateElicitationRequestParams,
        _context: rmcp::service::RequestContext<RoleClient>,
    ) -> impl Future<Output = Result<CreateElicitationResult, rmcp::ErrorData>> + Send + '_ {
        std::future::ready(Ok(unsupported_elicitation_result()))
    }

    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }
}

fn emit_progress(
    callback: Option<McpProgressCallback>,
    server_name: String,
    params: ProgressNotificationParam,
) {
    if let Some(callback) = callback {
        callback(McpProgressEvent {
            server_name,
            progress_token: progress_token_to_string(&params.progress_token),
            progress: params.progress,
            total: params.total,
            message: params.message,
        });
    }
}

fn unsupported_elicitation_result() -> CreateElicitationResult {
    CreateElicitationResult {
        action: ElicitationAction::Decline,
        content: Some(json!({
            "ok": false,
            "error": "MCP server requires OAuth or interactive elicitation, which is not supported yet"
        })),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "stdio 配置字段来自 McpTransportConfig，拆分会额外复制 command/env；release fence 是同一建连路径的生命周期依赖"
)]
async fn stdio_transport(
    server_name: &str,
    command: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    env_vars: Vec<String>,
    cwd: Option<PathBuf>,
    workspace_root: &Path,
    connect_release_fence: Arc<McpConnectReleaseFence>,
) -> Result<
    (
        PendingConnectTransport<TokioChildProcess>,
        Option<tokio::process::ChildStderr>,
    ),
    McpClientError,
> {
    let mut cmd = Command::new(command);
    let effective_cwd = effective_stdio_cwd(cwd, workspace_root);
    cmd.args(args)
        .current_dir(effective_cwd)
        .env_clear()
        .envs(stdio_env(env, env_vars));
    let cmd = super::process_group::wrap_stdio_command(cmd);
    let (transport, stderr) = TokioChildProcess::builder(cmd)
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| McpClientError::StdioIo {
            server: server_name.to_string(),
            source,
        })?;
    Ok((
        PendingConnectTransport::new(transport, connect_release_fence.register_transport()),
        stderr,
    ))
}

async fn streamable_http_transport(
    server_name: &str,
    url: String,
    bearer_token_env_var: Option<String>,
    fallback_tool_http_response_timeout: Duration,
    connect_release_fence: Arc<McpConnectReleaseFence>,
) -> Result<PendingConnectTransport<StreamableHttpClientTransport<AcnMcpHttpClient>>, McpClientError>
{
    let bearer_token = match bearer_token_env_var {
        Some(env_var) => {
            let value =
                std::env::var(&env_var).map_err(|_| McpClientError::MissingBearerToken {
                    server: server_name.to_string(),
                    env_var: env_var.clone(),
                })?;
            if value.trim().is_empty() {
                return Err(McpClientError::MissingBearerToken {
                    server: server_name.to_string(),
                    env_var,
                });
            }
            Some(value)
        }
        None => None,
    };
    // 初始化由 `McpClient::connect` 的 startup timeout 约束，discovery 由 manager 的 timeout
    // 约束；不能把 startup timeout 设为整个 reqwest client 的默认值，否则合法长 SSE tool call
    // 会早于 `tool_timeout_secs` 被截断。
    let client =
        reqwest::Client::builder()
            .build()
            .map_err(|source| McpClientError::HttpClient {
                server: server_name.to_string(),
                source,
            })?;
    let mut config = StreamableHttpClientTransportConfig::with_uri(url);
    if let Some(token) = bearer_token {
        config = config.auth_header(token);
    }
    Ok(PendingConnectTransport::new(
        StreamableHttpClientTransport::with_client(
            AcnMcpHttpClient::new(client, fallback_tool_http_response_timeout),
            config,
        ),
        connect_release_fence.register_transport(),
    ))
}

fn client_info() -> ClientInfo {
    ClientInfo {
        meta: None,
        protocol_version: protocol_version_2025_11_25(),
        capabilities: Default::default(),
        client_info: Implementation {
            name: "agent_claim_network".to_string(),
            title: Some("Agent Claim Network".to_string()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
    }
}

fn protocol_version_2025_11_25() -> ProtocolVersion {
    serde_json::from_str::<ProtocolVersion>("\"2025-11-25\"")
        .unwrap_or(ProtocolVersion::V_2025_06_18)
}

fn stdio_env(
    literal_env: BTreeMap<String, String>,
    inherited_env_vars: Vec<String>,
) -> BTreeMap<String, String> {
    default_env_var_names()
        .iter()
        .copied()
        .chain(inherited_env_vars.iter().map(String::as_str))
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .chain(literal_env)
        .collect()
}

fn effective_stdio_cwd(cwd: Option<PathBuf>, workspace_root: &Path) -> PathBuf {
    match cwd {
        Some(path) if path.is_relative() => workspace_root.join(path),
        Some(path) => path,
        None => workspace_root.to_path_buf(),
    }
}

fn default_env_var_names() -> &'static [&'static str] {
    if cfg!(windows) {
        DEFAULT_ENV_VARS_WINDOWS
    } else {
        DEFAULT_ENV_VARS_UNIX
    }
}

fn spawn_stderr_capture(stderr: Option<tokio::process::ChildStderr>, buffer: Arc<Mutex<String>>) {
    if let Some(stderr) = stderr {
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(stderr);
            let mut chunk = [0u8; 1024];
            loop {
                match reader.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(read) => {
                        let mut guard = buffer.lock().await;
                        guard.push_str(&String::from_utf8_lossy(&chunk[..read]));
                        truncate_front(&mut guard, STDERR_CAPTURE_MAX_CHARS);
                    }
                    Err(err) => {
                        let mut guard = buffer.lock().await;
                        guard.push_str(&format!("stderr read error: {err}\n"));
                        truncate_front(&mut guard, STDERR_CAPTURE_MAX_CHARS);
                        break;
                    }
                }
            }
        });
    }
}

fn truncate_front(value: &mut String, max_chars: usize) {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return;
    }
    let keep_from = char_count.saturating_sub(max_chars);
    let byte_index = value
        .char_indices()
        .nth(keep_from)
        .map(|(index, _)| index)
        .unwrap_or(0);
    value.drain(..byte_index);
}

fn progress_token_to_string(token: &rmcp::model::ProgressToken) -> String {
    match &token.0 {
        NumberOrString::Number(value) => value.to_string(),
        NumberOrString::String(value) => value.to_string(),
    }
}

pub fn tool_requires_task(tool: &Tool) -> bool {
    tool.task_support() == TaskSupport::Required
}

pub fn call_tool_result_to_json(result: &CallToolResult) -> Value {
    let content = serde_json::to_value(&result.content).unwrap_or_else(|_| Value::Array(vec![]));
    let meta = result
        .meta
        .as_ref()
        .map(|meta| Value::Object(meta.0.clone()))
        .unwrap_or_else(|| Value::Object(Map::new()));
    json!({
        "content": content,
        "structured_content": result.structured_content.clone().unwrap_or(Value::Object(Map::new())),
        "is_error": result.is_error.unwrap_or(false),
        "meta": meta,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn connection_establishment_retries_transient_io_failures() {
        for kind in [
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::NotConnected,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::WouldBlock,
        ] {
            assert!(
                retryable_io_error(&std::io::Error::from(kind)),
                "{kind:?} should be retryable during connection establishment"
            );
        }
    }

    #[test]
    fn connection_establishment_does_not_retry_permanent_io_failures() {
        for kind in [
            std::io::ErrorKind::InvalidData,
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
        ] {
            assert!(
                !retryable_io_error(&std::io::Error::from(kind)),
                "{kind:?} should fail without connection retry"
            );
        }
    }

    #[test]
    fn stdio_env_includes_literal_and_named_inherited_values() {
        std::env::set_var("ACN_MCP_TEST_ENV", "secret");
        let env = stdio_env(
            BTreeMap::from([("DEFAULT_MODEL".to_string(), "auto".to_string())]),
            vec!["ACN_MCP_TEST_ENV".to_string()],
        );

        assert_eq!(env.get("DEFAULT_MODEL").map(String::as_str), Some("auto"));
        assert_eq!(
            env.get("ACN_MCP_TEST_ENV").map(String::as_str),
            Some("secret")
        );
    }

    #[test]
    fn stdio_cwd_resolves_relative_paths_against_workspace_root() {
        let workspace_root = PathBuf::from("/workspace/acn");

        assert_eq!(
            effective_stdio_cwd(Some(PathBuf::from("servers/memory")), &workspace_root),
            PathBuf::from("/workspace/acn/servers/memory")
        );
        assert_eq!(
            effective_stdio_cwd(Some(PathBuf::from("/tmp/mcp")), &workspace_root),
            PathBuf::from("/tmp/mcp")
        );
        assert_eq!(
            effective_stdio_cwd(None, &workspace_root),
            PathBuf::from("/workspace/acn")
        );
    }

    #[test]
    fn elicitation_is_declined_with_clear_unsupported_message() {
        let result = unsupported_elicitation_result();

        assert_eq!(result.action, ElicitationAction::Decline);
        assert_eq!(
            result.content.unwrap()["error"],
            "MCP server requires OAuth or interactive elicitation, which is not supported yet"
        );
    }

    #[test]
    fn streamable_http_auth_challenge_reports_unsupported_oauth() {
        let header = reqwest::header::HeaderValue::from_static(
            r#"Bearer resource_metadata="https://auth.example.test/.well-known/oauth-protected-resource?token=secret""#,
        );

        let message = streamable_http_auth_error_message(Some(&header), false);

        assert!(message.contains(UNSUPPORTED_INTERACTIVE_AUTH_MESSAGE));
        assert!(message.contains("Bearer resource_metadata"));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn streamable_http_auth_challenge_reports_rejected_bearer_token() {
        let header = reqwest::header::HeaderValue::from_static(r#"Bearer realm="mcp""#);

        let message = streamable_http_auth_error_message(Some(&header), true);

        assert!(message.contains("bearer token was rejected"));
        assert!(message.contains("WWW-Authenticate: Bearer"));
    }

    #[test]
    fn progress_callback_receives_progress_notification() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let handler = AcnMcpClientHandler {
            server_name: "pal".to_string(),
            client_info: client_info(),
            progress_callback: Some(Arc::new(move |event| {
                captured.lock().unwrap().push(event);
            })),
        };
        let params = ProgressNotificationParam {
            progress_token: rmcp::model::ProgressToken(rmcp::model::NumberOrString::Number(1)),
            progress: 1.0,
            total: Some(2.0),
            message: Some("half".to_string()),
        };

        emit_progress(
            handler.progress_callback.clone(),
            handler.server_name.clone(),
            params,
        );

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].server_name, "pal");
        assert_eq!(events[0].message.as_deref(), Some("half"));
    }
}
