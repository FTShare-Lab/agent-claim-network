//! Responses WebSocket transport、连接租约与进程内增量链。

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest::StatusCode;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde_json::{Map, Value};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::client::compute_backoff;
use super::protocol::{ReducedResponses, ResponsesRequest};
use super::streaming::ResponsesEventDecoder;
use super::{redact_responses_error_body, ResponsesError, ResponsesStreamEvent};
use crate::api::llm_http::read_llm_error_body;
use crate::api::{ProviderRecoveryInterrupt, ProviderRuntimeChainId, ProviderRuntimeFallbackScope};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONNECTION_AGE: Duration = Duration::from_secs(55 * 60);

#[derive(Debug)]
pub(super) enum WebSocketSendOutcome {
    Response(ReducedResponses),
    FallbackToHttp,
}

pub(super) struct ResponsesWebSocketTransport {
    http: reqwest::Client,
    endpoint: Arc<String>,
    api_key: Arc<String>,
    pool: Arc<WebSocketPool>,
    request_timeout: Duration,
}

impl ResponsesWebSocketTransport {
    pub(super) fn new(
        http_endpoint: &str,
        api_key: Arc<String>,
        pool_capacity: usize,
        request_timeout: Duration,
    ) -> Result<Self, ResponsesError> {
        let endpoint = websocket_endpoint(http_endpoint)?;
        let builder = crate::http_client_builder_for_endpoint(http_endpoint)
            // 当前 WebSocket 实现使用 HTTP/1.1 Upgrade，不支持 HTTP/2 Extended CONNECT。
            .http1_only()
            // Upgrade 必须由配置的 endpoint 直接完成，避免重定向掩盖网关或路径配置错误。
            .redirect(reqwest::redirect::Policy::none());
        let http = builder
            .build()
            .map_err(|error| ResponsesError::StreamFailure {
                reason: format!("Responses WebSocket client 构造失败: {error}"),
            })?;
        Ok(Self {
            http,
            endpoint: Arc::new(endpoint),
            api_key,
            pool: Arc::new(WebSocketPool::new(pool_capacity.max(1))),
            request_timeout,
        })
    }

    pub(super) async fn discard_runtime_chain(&self, chain_id: ProviderRuntimeChainId) {
        self.pool.clear_chain(chain_id).await;
    }

    #[cfg(test)]
    #[allow(
        clippy::too_many_arguments,
        reason = "WebSocket retry 需显式携带 chain、退避、steer 恢复边界与事件 sink"
    )]
    pub(super) async fn send_with_retry_count(
        &self,
        request: &ResponsesRequest,
        chain_id: ProviderRuntimeChainId,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<WebSocketSendOutcome, ResponsesError> {
        self.send_with_retry_count_for_scope(
            request,
            chain_id,
            None,
            retry_count,
            retry_base_delay,
            retry_max_delay,
            recovery_interrupt,
            false,
            emit,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "WebSocket retry 需显式携带 fallback scope 与缓冲语义"
    )]
    pub(super) async fn send_with_retry_count_for_scope(
        &self,
        request: &ResponsesRequest,
        chain_id: ProviderRuntimeChainId,
        fallback_scope: Option<&ProviderRuntimeFallbackScope>,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        retry_after_partial: bool,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<WebSocketSendOutcome, ResponsesError> {
        ensure_recovery_active(recovery_interrupt)?;
        if self.websocket_sticky(chain_id, fallback_scope).await {
            return Ok(WebSocketSendOutcome::FallbackToHttp);
        }
        let snapshot = RequestSnapshot::new(request)?;
        let mut attempt = 0u32;
        let mut force_full_history = false;
        let mut previous_state_recovery_used = false;

        loop {
            ensure_recovery_active(recovery_interrupt)?;
            let attempt_started = Instant::now();
            let waiting_for_pool = AtomicBool::new(false);
            let acquire = tokio::time::timeout(
                self.request_timeout,
                self.acquire_connection(
                    chain_id,
                    fallback_scope,
                    &snapshot,
                    force_full_history,
                    &waiting_for_pool,
                ),
            );
            tokio::pin!(acquire);
            let acquired_result = match recovery_interrupt {
                Some(interrupt) => {
                    tokio::select! {
                        biased;
                        _ = interrupt.cancelled() => {
                            return Err(ResponsesError::RecoveryInterrupted);
                        }
                        acquired = &mut acquire => acquired,
                    }
                }
                None => acquire.await,
            };
            let acquired = match acquired_result {
                Ok(acquired) => acquired,
                Err(_) if waiting_for_pool.load(Ordering::Acquire) => Err(
                    ConnectFailure::PoolTimeout("Responses WebSocket 连接池等待超时".into()),
                ),
                Err(_) => Err(ConnectFailure::Retryable(
                    "Responses WebSocket request 在响应开始前超时".into(),
                )),
            };
            ensure_recovery_active(recovery_interrupt)?;
            let (mut connection, selection) = match acquired {
                Ok(Some(acquired)) => acquired,
                Ok(None) => return Ok(WebSocketSendOutcome::FallbackToHttp),
                Err(ConnectFailure::ImmediateDowngrade) => {
                    self.mark_websocket_sticky(chain_id, fallback_scope).await;
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
                Err(ConnectFailure::PoolTimeout(_)) => {
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
                Err(ConnectFailure::Deterministic(error)) => return Err(error),
                Err(ConnectFailure::Retryable(error)) if attempt < retry_count => {
                    wait_before_retry(
                        attempt,
                        retry_count,
                        retry_base_delay,
                        retry_max_delay,
                        &error,
                        recovery_interrupt,
                    )
                    .await?;
                    attempt = attempt.saturating_add(1);
                    force_full_history = true;
                    continue;
                }
                Err(ConnectFailure::TransientStatus(error)) if attempt < retry_count => {
                    wait_before_retry(
                        attempt,
                        retry_count,
                        retry_base_delay,
                        retry_max_delay,
                        &error,
                        recovery_interrupt,
                    )
                    .await?;
                    attempt = attempt.saturating_add(1);
                    force_full_history = true;
                    continue;
                }
                Err(ConnectFailure::Retryable(_)) => {
                    self.mark_websocket_sticky(chain_id, fallback_scope).await;
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
                Err(ConnectFailure::TransientStatus(_)) => {
                    // 429/5xx 只说明当前握手暂时失败，不能据此断定这个 endpoint
                    // 在后续请求中不支持 WebSocket。
                    self.pool.clear_chain(chain_id).await;
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
            };

            let payload = snapshot.websocket_payload(&selection)?;
            let mut emitted_visible_text = false;
            let remaining = self
                .request_timeout
                .saturating_sub(attempt_started.elapsed());
            let result = tokio::time::timeout(remaining, async {
                let mut tracking_emit = |event| {
                    emitted_visible_text = true;
                    emit(event);
                };
                connection.run_response(payload, &mut tracking_emit).await
            })
            .await
            .unwrap_or_else(|_| {
                Err(stream_failure(
                    "Responses WebSocket request 在完整终态前超时",
                ))
            });
            if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
                self.pool.clear_chain(chain_id).await;
                match result {
                    Ok(response) => {
                        // safe steer 不取消已经正常完成的 response；只清除未提交的
                        // continuation affinity，让外层在工具安全边界收束当前 turn。
                        connection.continuation = None;
                        self.pool.release(connection).await;
                        return Ok(WebSocketSendOutcome::Response(response));
                    }
                    Err(_) => return Err(ResponsesError::RecoveryInterrupted),
                }
            }
            match result {
                Ok(response) => {
                    let response_id = response.response_id.clone().ok_or_else(|| {
                        ResponsesError::StreamFailure {
                            reason: "Responses WebSocket terminal response 缺少合法 id".into(),
                        }
                    })?;
                    connection.continuation = Some(snapshot.continuation_after(
                        chain_id,
                        response_id,
                        &response.output_items,
                    ));
                    self.pool.release(connection).await;
                    return Ok(WebSocketSendOutcome::Response(response));
                }
                Err(WebSocketRequestFailure::PreviousResponseNotFound)
                    if !previous_state_recovery_used =>
                {
                    previous_state_recovery_used = true;
                    force_full_history = true;
                    self.pool.clear_chain(chain_id).await;
                }
                Err(WebSocketRequestFailure::PreviousResponseNotFound) => {
                    self.pool.clear_chain(chain_id).await;
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
                Err(WebSocketRequestFailure::ConnectionLimit) if attempt < retry_count => {
                    wait_before_retry(
                        attempt,
                        retry_count,
                        retry_base_delay,
                        retry_max_delay,
                        &"WebSocket connection limit reached",
                        recovery_interrupt,
                    )
                    .await?;
                    attempt = attempt.saturating_add(1);
                    force_full_history = true;
                }
                Err(WebSocketRequestFailure::ConnectionLimit) => {
                    self.pool.clear_chain(chain_id).await;
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
                Err(WebSocketRequestFailure::Response(error))
                    if emitted_visible_text && !retry_after_partial =>
                {
                    if websocket_error_triggers_sticky_downgrade(&error) {
                        // 当前 response 已经向 TUI 发出 partial，不再从头执行 WS/SSE，
                        // 但连接损坏仍表示这条 runtime chain 后续应直接使用 HTTP。
                        self.mark_websocket_sticky(chain_id, fallback_scope).await;
                    } else {
                        // 429/5xx 与确定性 response 错误只使本次 continuation 失效；
                        // 它们不能被误判成 endpoint 的 WebSocket transport 不可用。
                        self.pool.clear_chain(chain_id).await;
                    }
                    return Err(error);
                }
                Err(WebSocketRequestFailure::Response(error))
                    if websocket_error_is_retryable(&error) && attempt < retry_count =>
                {
                    wait_before_retry(
                        attempt,
                        retry_count,
                        retry_base_delay,
                        retry_max_delay,
                        &error,
                        recovery_interrupt,
                    )
                    .await?;
                    attempt = attempt.saturating_add(1);
                    force_full_history = true;
                }
                Err(WebSocketRequestFailure::Response(error))
                    if websocket_error_is_retryable(&error) =>
                {
                    if websocket_error_triggers_sticky_downgrade(&error) {
                        self.mark_websocket_sticky(chain_id, fallback_scope).await;
                    } else {
                        // 429/5xx 是当前 response 的暂态状态，不代表 endpoint 的
                        // WebSocket transport 不可用；只废弃本次 continuation。
                        self.pool.clear_chain(chain_id).await;
                    }
                    return Ok(WebSocketSendOutcome::FallbackToHttp);
                }
                Err(WebSocketRequestFailure::Response(error)) => {
                    self.pool.clear_chain(chain_id).await;
                    return Err(error);
                }
            }
        }
    }

    async fn acquire_connection(
        &self,
        chain_id: ProviderRuntimeChainId,
        fallback_scope: Option<&ProviderRuntimeFallbackScope>,
        snapshot: &RequestSnapshot,
        force_full_history: bool,
        waiting_for_pool: &AtomicBool,
    ) -> Result<Option<(WebSocketConnection, RequestSelection)>, ConnectFailure> {
        loop {
            if self.websocket_sticky(chain_id, fallback_scope).await {
                return Ok(None);
            }
            if let Some((connection, selection)) = self
                .pool
                .take_preferred_idle(chain_id, snapshot, force_full_history)
                .await
            {
                return Ok(Some((connection, selection)));
            }

            match self.pool.permits.clone().try_acquire_owned() {
                Ok(permit) => {
                    if self.websocket_sticky(chain_id, fallback_scope).await {
                        drop(permit);
                        return Ok(None);
                    }
                    let connection = self.connect(permit).await?;
                    return Ok(Some((connection, RequestSelection::Full)));
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(ConnectFailure::Retryable(
                        "WebSocket connection pool 已关闭".into(),
                    ));
                }
                Err(tokio::sync::TryAcquireError::NoPermits) => {}
            }

            // 只有物理连接池确实满载时才清理其他 chain 的 affinity；容量尚有
            // 空位时新建连接，避免主 session 与 subagent 互相破坏增量链。
            if let Some(connection) = self.pool.take_foreign_idle(chain_id).await {
                return Ok(Some((connection, RequestSelection::Full)));
            }

            // idle 连接自身持有物理连接 permit。池满时必须同时等待 idle
            // 归还与 permit 释放，否则仅等待 semaphore 会在健康连接回池后挂死。
            let idle_available = self.pool.idle_available.notified();
            let permit_available = self.pool.permits.clone().acquire_owned();
            tokio::pin!(idle_available);
            tokio::pin!(permit_available);
            waiting_for_pool.store(true, Ordering::Release);
            tokio::select! {
                biased;
                () = &mut idle_available => {
                    waiting_for_pool.store(false, Ordering::Release);
                }
                permit = &mut permit_available => {
                    waiting_for_pool.store(false, Ordering::Release);
                    let permit = permit.map_err(|_| {
                        ConnectFailure::Retryable("WebSocket connection pool 已关闭".into())
                    })?;
                    if self.websocket_sticky(chain_id, fallback_scope).await {
                        drop(permit);
                        return Ok(None);
                    }
                    let connection = self.connect(permit).await?;
                    return Ok(Some((connection, RequestSelection::Full)));
                }
            }
        }
    }

    async fn connect(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<WebSocketConnection, ConnectFailure> {
        let operation = async {
            let response = self
                .http
                .get(self.endpoint.as_str())
                .bearer_auth(self.api_key.as_str())
                .upgrade()
                .send()
                .await
                .map_err(|_| ConnectFailure::Retryable("WebSocket handshake 请求失败".into()))?;
            let status = response.status();
            if status != StatusCode::SWITCHING_PROTOCOLS {
                let response = response.into_inner();
                return match status {
                    StatusCode::UPGRADE_REQUIRED => Err(ConnectFailure::ImmediateDowngrade),
                    StatusCode::UNAUTHORIZED => {
                        let body = read_llm_error_body(response, CONNECT_TIMEOUT).await;
                        Err(ConnectFailure::Deterministic(ResponsesError::Auth(
                            redact_responses_error_body(&body),
                        )))
                    }
                    StatusCode::TOO_MANY_REQUESTS => Err(ConnectFailure::TransientStatus(format!(
                        "WebSocket handshake 返回 HTTP {status}"
                    ))),
                    status if status.is_server_error() => Err(ConnectFailure::TransientStatus(
                        format!("WebSocket handshake 返回 HTTP {status}"),
                    )),
                    // 握手发生在 response.create 发送前，400/403 等状态无法证明是
                    // 模型请求参数错误，网关也常用它们拒绝 Upgrade。有限重试后转
                    // HTTP；若确为业务错误，HTTP Responses 会返回权威错误内容。
                    _ => Err(ConnectFailure::Retryable(format!(
                        "WebSocket handshake 返回 HTTP {status}"
                    ))),
                };
            }
            let websocket = response
                .into_websocket()
                .await
                .map_err(|_| ConnectFailure::Retryable("WebSocket Upgrade 校验失败".into()))?;
            Ok(websocket)
        };
        let websocket = tokio::time::timeout(CONNECT_TIMEOUT, operation)
            .await
            .map_err(|_| ConnectFailure::Retryable("WebSocket connect timeout".into()))??;
        Ok(WebSocketConnection::new(websocket, permit))
    }

    async fn websocket_sticky(
        &self,
        chain_id: ProviderRuntimeChainId,
        fallback_scope: Option<&ProviderRuntimeFallbackScope>,
    ) -> bool {
        fallback_scope.is_some_and(ProviderRuntimeFallbackScope::websocket_sticky)
            || self.pool.is_sticky(chain_id).await
    }

    async fn mark_websocket_sticky(
        &self,
        chain_id: ProviderRuntimeChainId,
        fallback_scope: Option<&ProviderRuntimeFallbackScope>,
    ) {
        if let Some(scope) = fallback_scope {
            scope.mark_websocket_sticky();
        }
        self.pool.mark_sticky(chain_id).await;
    }
}

fn websocket_endpoint(http_endpoint: &str) -> Result<String, ResponsesError> {
    let mut url = reqwest::Url::parse(http_endpoint)
        .map_err(|error| ResponsesError::InvalidEndpoint(error.to_string()))?;
    let scheme = match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        other => {
            return Err(ResponsesError::InvalidEndpoint(format!(
                "WebSocket 仅支持 http/https endpoint，实际为 {other}"
            )))
        }
    };
    url.set_scheme(scheme).map_err(|_| {
        ResponsesError::InvalidEndpoint("WebSocket endpoint scheme 转换失败".into())
    })?;
    Ok(url.to_string())
}

async fn wait_before_retry(
    attempt: u32,
    retry_count: u32,
    base: Duration,
    max: Duration,
    error: &(dyn std::fmt::Display + Sync),
    recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
) -> Result<(), ResponsesError> {
    let backoff = compute_backoff(attempt, base, max);
    log::warn!(
        target: "api",
        "Responses WebSocket 调用失败，{}ms 后重试 ({}/{}): {}",
        backoff.as_millis(),
        attempt + 1,
        retry_count,
        error
    );
    if backoff.is_zero() {
        return ensure_recovery_active(recovery_interrupt);
    }
    match recovery_interrupt {
        Some(interrupt) => {
            tokio::select! {
                biased;
                _ = interrupt.cancelled() => Err(ResponsesError::RecoveryInterrupted),
                _ = tokio::time::sleep(backoff) => Ok(()),
            }
        }
        None => {
            tokio::time::sleep(backoff).await;
            Ok(())
        }
    }
}

fn ensure_recovery_active(
    recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
) -> Result<(), ResponsesError> {
    if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
        return Err(ResponsesError::RecoveryInterrupted);
    }
    Ok(())
}

fn websocket_error_is_retryable(error: &ResponsesError) -> bool {
    matches!(error, ResponsesError::StreamFailure { .. })
        || matches!(error, ResponsesError::Status { status, .. } if *status == 429 || *status >= 500)
}

fn websocket_error_triggers_sticky_downgrade(error: &ResponsesError) -> bool {
    matches!(error, ResponsesError::StreamFailure { .. })
}

#[derive(Clone)]
struct RequestSnapshot {
    input: Vec<Value>,
    envelope: Value,
    base: Map<String, Value>,
}

impl RequestSnapshot {
    fn new(request: &ResponsesRequest) -> Result<Self, ResponsesError> {
        let value = serde_json::to_value(request)?;
        let mut base = value
            .as_object()
            .cloned()
            .ok_or_else(|| ResponsesError::OutputShape {
                reason: "Responses request 序列化后不是 object".into(),
            })?;
        base.remove("stream");
        base.remove("background");
        let input = base
            .remove("input")
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| ResponsesError::OutputShape {
                reason: "Responses request 缺少 input array".into(),
            })?;
        Ok(Self {
            input,
            envelope: Value::Object(base.clone()),
            base,
        })
    }

    fn websocket_payload(&self, selection: &RequestSelection) -> Result<String, ResponsesError> {
        let mut payload = self.base.clone();
        payload.insert("type".into(), Value::String("response.create".into()));
        match selection {
            RequestSelection::Full => {
                payload.insert("input".into(), Value::Array(self.input.clone()));
            }
            RequestSelection::Incremental {
                previous_response_id,
                prefix_len,
            } => {
                payload.insert(
                    "previous_response_id".into(),
                    Value::String(previous_response_id.clone()),
                );
                payload.insert(
                    "input".into(),
                    Value::Array(self.input[*prefix_len..].to_vec()),
                );
            }
        }
        serde_json::to_string(&Value::Object(payload)).map_err(Into::into)
    }

    fn continuation_after(
        &self,
        chain_id: ProviderRuntimeChainId,
        response_id: String,
        output_items: &[Value],
    ) -> ContinuationState {
        let mut represented_input = self.input.clone();
        represented_input.extend(output_items.iter().cloned());
        ContinuationState {
            chain_id,
            response_id,
            represented_input,
            envelope: self.envelope.clone(),
        }
    }
}

enum RequestSelection {
    Full,
    Incremental {
        previous_response_id: String,
        prefix_len: usize,
    },
}

#[derive(Clone)]
struct ContinuationState {
    chain_id: ProviderRuntimeChainId,
    response_id: String,
    represented_input: Vec<Value>,
    envelope: Value,
}

impl ContinuationState {
    fn selection_for(
        &self,
        chain_id: ProviderRuntimeChainId,
        snapshot: &RequestSnapshot,
    ) -> Option<RequestSelection> {
        if self.chain_id != chain_id
            || self.envelope != snapshot.envelope
            || self.represented_input.is_empty()
            || !snapshot.input.starts_with(&self.represented_input)
            || snapshot.input.len() == self.represented_input.len()
        {
            return None;
        }
        Some(RequestSelection::Incremental {
            previous_response_id: self.response_id.clone(),
            prefix_len: self.represented_input.len(),
        })
    }
}

struct WebSocketPool {
    permits: Arc<Semaphore>,
    idle_available: Notify,
    inner: Mutex<WebSocketPoolState>,
}

struct WebSocketPoolState {
    idle: Vec<WebSocketConnection>,
    sticky_chains: HashSet<ProviderRuntimeChainId>,
}

impl WebSocketPool {
    fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
            idle_available: Notify::new(),
            inner: Mutex::new(WebSocketPoolState {
                idle: Vec::new(),
                sticky_chains: HashSet::new(),
            }),
        }
    }

    async fn is_sticky(&self, chain_id: ProviderRuntimeChainId) -> bool {
        self.inner.lock().await.sticky_chains.contains(&chain_id)
    }

    async fn mark_sticky(&self, chain_id: ProviderRuntimeChainId) {
        let mut inner = self.inner.lock().await;
        inner.sticky_chains.insert(chain_id);
        clear_idle_chain(&mut inner.idle, chain_id);
    }

    async fn clear_chain(&self, chain_id: ProviderRuntimeChainId) {
        let mut inner = self.inner.lock().await;
        clear_idle_chain(&mut inner.idle, chain_id);
    }

    async fn take_preferred_idle(
        &self,
        chain_id: ProviderRuntimeChainId,
        snapshot: &RequestSnapshot,
        force_full_history: bool,
    ) -> Option<(WebSocketConnection, RequestSelection)> {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        inner
            .idle
            .retain(|connection| connection.is_healthy_at(now));
        if !force_full_history {
            if let Some((index, selection)) =
                inner
                    .idle
                    .iter()
                    .enumerate()
                    .find_map(|(index, connection)| {
                        connection
                            .continuation
                            .as_ref()
                            .and_then(|state| state.selection_for(chain_id, snapshot))
                            .map(|selection| (index, selection))
                    })
            {
                let connection = inner.idle.swap_remove(index);
                if !inner.idle.is_empty() {
                    self.idle_available.notify_one();
                }
                return Some((connection, selection));
            }
        }
        let index = inner
            .idle
            .iter()
            .enumerate()
            .filter_map(|(index, connection)| {
                let affinity = match connection.continuation.as_ref() {
                    Some(state) if state.chain_id == chain_id => 0u8,
                    None => 1,
                    Some(_) => return None,
                };
                Some((index, affinity, connection.last_used_at))
            })
            .min_by_key(|(_, affinity, last_used_at)| (*affinity, *last_used_at))
            .map(|(index, _, _)| index)?;
        let mut connection = inner.idle.swap_remove(index);
        connection.continuation = None;
        if !inner.idle.is_empty() {
            self.idle_available.notify_one();
        }
        Some((connection, RequestSelection::Full))
    }

    async fn take_foreign_idle(
        &self,
        chain_id: ProviderRuntimeChainId,
    ) -> Option<WebSocketConnection> {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        inner
            .idle
            .retain(|connection| connection.is_healthy_at(now));
        let index = inner
            .idle
            .iter()
            .enumerate()
            .filter(|(_, connection)| {
                connection
                    .continuation
                    .as_ref()
                    .is_some_and(|state| state.chain_id != chain_id)
            })
            .min_by_key(|(_, connection)| connection.last_used_at)
            .map(|(index, _)| index)?;
        let mut connection = inner.idle.swap_remove(index);
        connection.continuation = None;
        if !inner.idle.is_empty() {
            self.idle_available.notify_one();
        }
        Some(connection)
    }

    async fn release(&self, mut connection: WebSocketConnection) {
        if !connection.is_healthy_at(Instant::now()) {
            return;
        }
        connection.last_used_at = Instant::now();
        self.inner.lock().await.idle.push(connection);
        self.idle_available.notify_one();
    }
}

fn clear_idle_chain(idle: &mut [WebSocketConnection], chain_id: ProviderRuntimeChainId) {
    for connection in idle {
        if connection
            .continuation
            .as_ref()
            .is_some_and(|state| state.chain_id == chain_id)
        {
            connection.continuation = None;
        }
    }
}

struct WebSocketConnection {
    commands: mpsc::UnboundedSender<ConnectionCommand>,
    invalid: Arc<AtomicBool>,
    actor: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
    continuation: Option<ContinuationState>,
    created_at: Instant,
    last_used_at: Instant,
}

impl WebSocketConnection {
    fn new(websocket: WebSocket, permit: OwnedSemaphorePermit) -> Self {
        let (commands, command_rx) = mpsc::unbounded_channel();
        let invalid = Arc::new(AtomicBool::new(false));
        let actor_invalid = Arc::clone(&invalid);
        let actor = tokio::spawn(run_connection_actor(websocket, command_rx, actor_invalid));
        let now = Instant::now();
        Self {
            commands,
            invalid,
            actor,
            _permit: permit,
            continuation: None,
            created_at: now,
            last_used_at: now,
        }
    }

    fn is_healthy_at(&self, now: Instant) -> bool {
        !self.invalid.load(Ordering::Acquire)
            && now.duration_since(self.created_at) < MAX_CONNECTION_AGE
    }

    async fn run_response(
        &mut self,
        payload: String,
        emit: &mut (dyn FnMut(ResponsesStreamEvent) + Send),
    ) -> Result<ReducedResponses, WebSocketRequestFailure> {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (ack_tx, ack_rx) = oneshot::channel();
        self.commands
            .send(ConnectionCommand::Start {
                payload,
                events: events_tx,
                ack: ack_tx,
            })
            .map_err(|_| stream_failure("WebSocket connection actor 已停止"))?;
        ack_rx
            .await
            .map_err(|_| stream_failure("WebSocket request 启动确认丢失"))?
            .map_err(stream_failure)?;

        let mut decoder = ResponsesEventDecoder::default();
        while let Some(event) = events_rx.recv().await {
            match event {
                IncomingMessage::Text(text) => {
                    match websocket_error_code(&text).as_deref() {
                        Some("previous_response_not_found") => {
                            return Err(WebSocketRequestFailure::PreviousResponseNotFound)
                        }
                        Some("websocket_connection_limit_reached") => {
                            return Err(WebSocketRequestFailure::ConnectionLimit)
                        }
                        _ => {}
                    }
                    if decoder.apply_event_text(&text, emit)? {
                        return decoder.finish(true).map_err(Into::into);
                    }
                }
                IncomingMessage::Binary => {
                    return Err(stream_failure(
                        "Responses WebSocket 收到不支持的 binary frame",
                    ))
                }
                IncomingMessage::Closed => {
                    return Err(stream_failure("Responses WebSocket 在完整终态前关闭"))
                }
                IncomingMessage::TransportFailure => {
                    return Err(stream_failure("Responses WebSocket 接收失败"))
                }
            }
        }
        Err(stream_failure("Responses WebSocket 事件通道提前结束"))
    }
}

impl Drop for WebSocketConnection {
    fn drop(&mut self) {
        self.invalid.store(true, Ordering::Release);
        let _ = self.commands.send(ConnectionCommand::Invalidate);
        self.actor.abort();
    }
}

enum ConnectionCommand {
    Start {
        payload: String,
        events: mpsc::UnboundedSender<IncomingMessage>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Invalidate,
}

enum IncomingMessage {
    Text(String),
    Binary,
    Closed,
    TransportFailure,
}

async fn run_connection_actor(
    mut websocket: WebSocket,
    mut commands: mpsc::UnboundedReceiver<ConnectionCommand>,
    invalid: Arc<AtomicBool>,
) {
    let mut active: Option<mpsc::UnboundedSender<IncomingMessage>> = None;
    loop {
        tokio::select! {
            // 已缓冲的服务端 frame 必须先于新请求处理；否则 terminal 后的异常
            // frame 可能被误归到刚租用该连接的下一次 response。
            biased;
            message = websocket.next() => match message {
                Some(Ok(Message::Ping(payload))) => {
                    if websocket.send(Message::Pong(payload)).await.is_err() {
                        notify_active(&active, IncomingMessage::TransportFailure);
                        invalid.store(true, Ordering::Release);
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Text(text))) => {
                    let Some(events) = active.as_ref() else {
                        invalid.store(true, Ordering::Release);
                        break;
                    };
                    let terminal = websocket_event_is_terminal(&text);
                    if events.send(IncomingMessage::Text(text)).is_err() {
                        invalid.store(true, Ordering::Release);
                        break;
                    }
                    if terminal {
                        active = None;
                    }
                }
                Some(Ok(Message::Binary(_))) => {
                    notify_active(&active, IncomingMessage::Binary);
                    invalid.store(true, Ordering::Release);
                    break;
                }
                Some(Ok(Message::Close { .. })) => {
                    notify_active(&active, IncomingMessage::Closed);
                    invalid.store(true, Ordering::Release);
                    break;
                }
                Some(Err(_)) => {
                    notify_active(&active, IncomingMessage::TransportFailure);
                    invalid.store(true, Ordering::Release);
                    break;
                }
                None => {
                    notify_active(&active, IncomingMessage::Closed);
                    invalid.store(true, Ordering::Release);
                    break;
                }
            },
            command = commands.recv() => match command {
                Some(ConnectionCommand::Start { payload, events, ack }) if active.is_none() => {
                    match websocket.send(Message::Text(payload)).await {
                        Ok(()) => {
                            active = Some(events);
                            let _ = ack.send(Ok(()));
                        }
                        Err(_) => {
                            let _ = ack.send(Err("WebSocket request frame 发送失败".into()));
                            invalid.store(true, Ordering::Release);
                            break;
                        }
                    }
                }
                Some(ConnectionCommand::Start { ack, .. }) => {
                    let _ = ack.send(Err("WebSocket connection 已有 in-flight response".into()));
                    invalid.store(true, Ordering::Release);
                    break;
                }
                Some(ConnectionCommand::Invalidate) | None => {
                    invalid.store(true, Ordering::Release);
                    let _ = SinkExt::close(&mut websocket).await;
                    break;
                }
            }
        }
    }
}

fn notify_active(active: &Option<mpsc::UnboundedSender<IncomingMessage>>, event: IncomingMessage) {
    if let Some(active) = active {
        let _ = active.send(event);
    }
}

fn websocket_event_is_terminal(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|kind| {
            matches!(
                kind.as_str(),
                "response.completed" | "response.incomplete" | "response.failed" | "error"
            )
        })
}

fn websocket_error_code(text: &str) -> Option<String> {
    let event = serde_json::from_str::<Value>(text).ok()?;
    let kind = event.get("type").and_then(Value::as_str)?;
    match kind {
        "error" => event
            .get("code")
            .or_else(|| event.get("error").and_then(|error| error.get("code"))),
        "response.failed" => event
            .get("response")
            .and_then(|response| response.get("error"))
            .and_then(|error| error.get("code")),
        _ => None,
    }
    .and_then(Value::as_str)
    .map(str::to_string)
}

enum WebSocketRequestFailure {
    PreviousResponseNotFound,
    ConnectionLimit,
    Response(ResponsesError),
}

impl From<ResponsesError> for WebSocketRequestFailure {
    fn from(error: ResponsesError) -> Self {
        Self::Response(error)
    }
}

fn stream_failure(reason: impl Into<String>) -> WebSocketRequestFailure {
    WebSocketRequestFailure::Response(ResponsesError::StreamFailure {
        reason: reason.into(),
    })
}

enum ConnectFailure {
    ImmediateDowngrade,
    PoolTimeout(String),
    Retryable(String),
    TransientStatus(String),
    Deterministic(ResponsesError),
}

impl std::fmt::Display for ConnectFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ImmediateDowngrade => f.write_str("WebSocket endpoint 要求其他 Upgrade"),
            Self::PoolTimeout(message) => f.write_str(message),
            Self::Retryable(message) => f.write_str(message),
            Self::TransientStatus(message) => f.write_str(message),
            Self::Deterministic(error) => error.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use axum::extract::ws::{Message as AxumMessage, WebSocket as AxumWebSocket, WebSocketUpgrade};
    use axum::extract::State;
    use axum::http::header::{CONTENT_TYPE, LOCATION};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::json;
    use tokio::sync::{Barrier, Notify};

    #[derive(Clone)]
    enum FakeBehavior {
        Success,
        ReasoningAndTool,
        MaxOutputThenSuccess,
        MaxOutputThenRateLimitThenSuccess,
        RateLimitError,
        ServerErrorStatusCodeOnce,
        UnauthorizedError,
        PingBetweenRequests,
        TrailingEventAfterTerminal,
        PreviousNotFoundOnce,
        ConnectionLimitOnce,
        MissingTerminalId,
        CloseBeforeEvents,
        MalformedJson,
        ReasoningThenClose,
        ToolThenClose,
        BinaryFrame,
        HttpStreamTimeoutThenJson,
        HttpMaxOutputThenBadRequest,
        VisibleThenClose,
        VisibleThenRateLimit,
        Concurrent(Arc<Barrier>),
        BlockFirst(Arc<Notify>),
        GateFirst {
            started: Arc<Notify>,
            release: Arc<Notify>,
        },
        CloseAfterSignal {
            started: Arc<Notify>,
            close: Arc<Notify>,
        },
    }

    struct FakeState {
        behavior: FakeBehavior,
        connections: AtomicUsize,
        http_requests: AtomicUsize,
        requests: Mutex<Vec<Value>>,
        special_used: AtomicBool,
    }

    struct TestServer {
        endpoint: String,
        task: JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn start_websocket_server(behavior: FakeBehavior) -> (TestServer, Arc<FakeState>) {
        let state = Arc::new(FakeState {
            behavior,
            connections: AtomicUsize::new(0),
            http_requests: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            special_used: AtomicBool::new(false),
        });
        let app = Router::new()
            .route(
                "/v1/responses",
                get(fake_websocket_handler).post(fake_http_responses_handler),
            )
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (
            TestServer {
                endpoint: format!("http://{address}/v1/responses"),
                task,
            },
            state,
        )
    }

    async fn fake_websocket_handler(
        ws: WebSocketUpgrade,
        State(state): State<Arc<FakeState>>,
    ) -> impl IntoResponse {
        state.connections.fetch_add(1, Ordering::SeqCst);
        ws.on_upgrade(move |socket| fake_websocket_connection(socket, state))
    }

    async fn fake_http_responses_handler(
        State(state): State<Arc<FakeState>>,
        Json(request): Json<Value>,
    ) -> Response {
        let request_number = state.http_requests.fetch_add(1, Ordering::SeqCst) + 1;
        if matches!(&state.behavior, FakeBehavior::HttpStreamTimeoutThenJson) {
            if request.get("stream").and_then(Value::as_bool) == Some(true) {
                tokio::time::sleep(Duration::from_millis(250)).await;
            } else {
                return Json(json!({
                    "status":"completed",
                    "output":[{
                        "type":"message",
                        "id":"msg_json",
                        "role":"assistant",
                        "status":"completed",
                        "content":[{
                            "type":"output_text",
                            "text":"json replacement",
                            "annotations":[]
                        }]
                    }],
                    "usage":{"total_tokens":3}
                }))
                .into_response();
            }
        }
        if matches!(&state.behavior, FakeBehavior::HttpMaxOutputThenBadRequest) {
            if request.get("stream").and_then(Value::as_bool) == Some(true) && request_number == 1 {
                let item = json!({
                    "type":"message",
                    "id":"msg_partial",
                    "role":"assistant",
                    "status":"incomplete",
                    "content":[{
                        "type":"output_text",
                        "text":"first half",
                        "annotations":[]
                    }]
                });
                let frames = format!(
                    "data: {}\n\ndata: {}\n\ndata: {}\n\n",
                    json!({"type":"response.output_text.delta","delta":"first half"}),
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":item
                    }),
                    json!({
                        "type":"response.incomplete",
                        "response":{
                            "id":"resp_partial",
                            "status":"incomplete",
                            "incomplete_details":{"reason":"max_output_tokens"}
                        }
                    }),
                );
                return ([(CONTENT_TYPE, "text/event-stream")], frames).into_response();
            }
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error":{
                        "type":"invalid_request_error",
                        "message":"continuation rejected"
                    }
                })),
            )
                .into_response();
        }
        if matches!(&state.behavior, FakeBehavior::VisibleThenClose)
            && request.get("stream").and_then(Value::as_bool) != Some(true)
        {
            return Json(json!({
                "status":"completed",
                "output":[{
                    "type":"message",
                    "id":"msg_json",
                    "role":"assistant",
                    "status":"completed",
                    "content":[{
                        "type":"output_text",
                        "text":"json replacement",
                        "annotations":[]
                    }]
                }],
                "usage":{"total_tokens":3}
            }))
            .into_response();
        }
        let item = json!({
            "type":"message","id":"msg_http","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"http","annotations":[]}]
        });
        let frames = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":{"status":"completed"}}),
        );
        ([(CONTENT_TYPE, "text/event-stream")], frames).into_response()
    }

    async fn fake_websocket_connection(mut socket: AxumWebSocket, state: Arc<FakeState>) {
        while let Some(Ok(message)) = socket.next().await {
            let AxumMessage::Text(text) = message else {
                continue;
            };
            let Ok(request) = serde_json::from_str::<Value>(&text) else {
                return;
            };
            let request_count = {
                let mut requests = state.requests.lock().await;
                requests.push(request.clone());
                requests.len()
            };

            match &state.behavior {
                FakeBehavior::PreviousNotFoundOnce
                    if request.get("previous_response_id").is_some()
                        && !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "code":"previous_response_not_found",
                            "message":"expired"
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::ConnectionLimitOnce
                    if !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "code":"websocket_connection_limit_reached",
                            "message":"rotate connection"
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::MissingTerminalId => {
                    send_success(&mut socket, None, "missing-id").await;
                }
                FakeBehavior::ReasoningAndTool => {
                    send_reasoning_and_tool(&mut socket).await;
                }
                FakeBehavior::MaxOutputThenSuccess
                    if !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    send_incomplete_max_output(&mut socket).await;
                }
                FakeBehavior::MaxOutputThenRateLimitThenSuccess if request_count == 1 => {
                    send_incomplete_max_output(&mut socket).await;
                }
                FakeBehavior::MaxOutputThenRateLimitThenSuccess if request_count == 2 => {
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "status":429,
                            "error":{
                                "type":"rate_limit_error",
                                "message":"retry later"
                            }
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::RateLimitError => {
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "status":429,
                            "error":{
                                "type":"rate_limit_error",
                                "message":"retry later"
                            }
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::ServerErrorStatusCodeOnce
                    if !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "status_code":503,
                            "error":{
                                "type":"server_error",
                                "message":"temporarily unavailable"
                            }
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::UnauthorizedError => {
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "status":401,
                            "error":{
                                "type":"authentication_error",
                                "message":"invalid credential"
                            }
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::PingBetweenRequests => {
                    let id = format!("resp_{}", state.requests.lock().await.len());
                    send_success(&mut socket, Some(&id), "ok").await;
                    let _ = socket.send(AxumMessage::Ping(vec![1, 2, 3].into())).await;
                }
                FakeBehavior::TrailingEventAfterTerminal => {
                    let id = format!("resp_{}", state.requests.lock().await.len());
                    send_success(&mut socket, Some(&id), "ok").await;
                    send_json_frame(
                        &mut socket,
                        json!({"type":"response.unexpected_after_terminal"}),
                    )
                    .await;
                }
                FakeBehavior::VisibleThenClose => {
                    send_json_frame(
                        &mut socket,
                        json!({"type":"response.output_text.delta","delta":"partial"}),
                    )
                    .await;
                    let _ = socket.send(AxumMessage::Close(None)).await;
                    return;
                }
                FakeBehavior::VisibleThenRateLimit => {
                    send_json_frame(
                        &mut socket,
                        json!({"type":"response.output_text.delta","delta":"partial"}),
                    )
                    .await;
                    send_json_frame(
                        &mut socket,
                        json!({
                            "type":"error",
                            "status":429,
                            "error":{
                                "type":"rate_limit_error",
                                "message":"retry later"
                            }
                        }),
                    )
                    .await;
                    return;
                }
                FakeBehavior::CloseBeforeEvents => {
                    let _ = socket.send(AxumMessage::Close(None)).await;
                    return;
                }
                FakeBehavior::HttpStreamTimeoutThenJson
                | FakeBehavior::HttpMaxOutputThenBadRequest => {
                    let _ = socket.send(AxumMessage::Close(None)).await;
                    return;
                }
                FakeBehavior::MalformedJson => {
                    let _ = socket.send(AxumMessage::Text("{broken".into())).await;
                    return;
                }
                FakeBehavior::ReasoningThenClose => {
                    send_output_item(
                        &mut socket,
                        0,
                        json!({
                            "type":"reasoning",
                            "id":"rs_partial",
                            "encrypted_content":"opaque"
                        }),
                    )
                    .await;
                    let _ = socket.send(AxumMessage::Close(None)).await;
                    return;
                }
                FakeBehavior::ToolThenClose => {
                    send_output_item(
                        &mut socket,
                        0,
                        json!({
                            "type":"function_call",
                            "id":"fc_partial",
                            "call_id":"call_partial",
                            "name":"file_read",
                            "arguments":"{\"path\":\"must-not-run.txt\"}",
                            "status":"completed"
                        }),
                    )
                    .await;
                    let _ = socket.send(AxumMessage::Close(None)).await;
                    return;
                }
                FakeBehavior::BinaryFrame => {
                    let _ = socket
                        .send(AxumMessage::Binary(vec![0xff, 0x00].into()))
                        .await;
                    return;
                }
                FakeBehavior::Concurrent(barrier) => {
                    let request_count = state.requests.lock().await.len();
                    if request_count <= 2 {
                        barrier.wait().await;
                    }
                    let id = format!("resp_{request_count}");
                    send_success(&mut socket, Some(&id), "ok").await;
                }
                FakeBehavior::BlockFirst(notify)
                    if !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    notify.notify_one();
                    futures::future::pending::<()>().await;
                }
                FakeBehavior::GateFirst { started, release }
                    if !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    started.notify_one();
                    release.notified().await;
                    let id = format!("resp_{}", state.requests.lock().await.len());
                    send_success(&mut socket, Some(&id), "ok").await;
                }
                FakeBehavior::CloseAfterSignal { started, close }
                    if !state.special_used.swap(true, Ordering::SeqCst) =>
                {
                    started.notify_one();
                    close.notified().await;
                    let _ = socket.send(AxumMessage::Close(None)).await;
                    return;
                }
                FakeBehavior::Success
                | FakeBehavior::PreviousNotFoundOnce
                | FakeBehavior::ConnectionLimitOnce
                | FakeBehavior::MaxOutputThenSuccess
                | FakeBehavior::MaxOutputThenRateLimitThenSuccess
                | FakeBehavior::ServerErrorStatusCodeOnce
                | FakeBehavior::BlockFirst(_)
                | FakeBehavior::GateFirst { .. }
                | FakeBehavior::CloseAfterSignal { .. } => {
                    let id = format!("resp_{}", state.requests.lock().await.len());
                    send_success(&mut socket, Some(&id), "ok").await;
                }
            }
        }
    }

    async fn send_success(socket: &mut AxumWebSocket, id: Option<&str>, text: &str) {
        let item = json!({
            "type":"message",
            "id":format!("msg_{text}"),
            "role":"assistant",
            "status":"completed",
            "content":[{"type":"output_text","text":text,"annotations":[]}]
        });
        send_json_frame(
            socket,
            json!({"type":"response.output_text.delta","delta":text}),
        )
        .await;
        send_json_frame(
            socket,
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
        )
        .await;
        let mut response = json!({"status":"completed","usage":{"total_tokens":3}});
        if let Some(id) = id {
            response["id"] = Value::String(id.to_string());
        }
        send_json_frame(
            socket,
            json!({"type":"response.completed","response":response}),
        )
        .await;
    }

    async fn send_reasoning_and_tool(socket: &mut AxumWebSocket) {
        let reasoning = json!({
            "type":"reasoning","id":"rs_1","encrypted_content":"opaque"
        });
        let tool = json!({
            "type":"function_call","id":"fc_1","call_id":"call_1",
            "name":"file_read","arguments":"{\"path\":\"example.txt\"}",
            "status":"completed"
        });
        for (output_index, item) in [reasoning, tool].into_iter().enumerate() {
            send_output_item(socket, output_index, item).await;
        }
        send_json_frame(
            socket,
            json!({
                "type":"response.completed",
                "response":{"id":"resp_tool","status":"completed"}
            }),
        )
        .await;
    }

    async fn send_output_item(socket: &mut AxumWebSocket, output_index: usize, item: Value) {
        send_json_frame(
            socket,
            json!({
                "type":"response.output_item.done",
                "output_index":output_index,
                "item":item
            }),
        )
        .await;
    }

    async fn send_incomplete_max_output(socket: &mut AxumWebSocket) {
        let item = json!({
            "type":"message","id":"msg_partial","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"first half","annotations":[]}]
        });
        send_json_frame(
            socket,
            json!({"type":"response.output_text.delta","delta":"first half"}),
        )
        .await;
        send_json_frame(
            socket,
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
        )
        .await;
        send_json_frame(
            socket,
            json!({
                "type":"response.incomplete",
                "response":{
                    "id":"resp_partial",
                    "status":"incomplete",
                    "incomplete_details":{"reason":"max_output_tokens"}
                }
            }),
        )
        .await;
    }

    async fn send_json_frame(socket: &mut AxumWebSocket, value: Value) {
        let _ = socket
            .send(AxumMessage::Text(value.to_string().into()))
            .await;
    }

    fn transport(endpoint: &str, capacity: usize) -> ResponsesWebSocketTransport {
        transport_with_timeout(endpoint, capacity, Duration::from_secs(2))
    }

    fn transport_with_timeout(
        endpoint: &str,
        capacity: usize,
        timeout: Duration,
    ) -> ResponsesWebSocketTransport {
        ResponsesWebSocketTransport::new(endpoint, Arc::new("test-key".into()), capacity, timeout)
            .unwrap()
    }

    async fn send_transport(
        transport: &ResponsesWebSocketTransport,
        request: &ResponsesRequest,
        chain: ProviderRuntimeChainId,
        retry_count: u32,
    ) -> (WebSocketSendOutcome, Vec<ResponsesStreamEvent>) {
        let mut events = Vec::new();
        let response = transport
            .send_with_retry_count(
                request,
                chain,
                retry_count,
                Duration::ZERO,
                Duration::ZERO,
                None,
                &mut |event| events.push(event),
            )
            .await
            .unwrap();
        (response, events)
    }

    fn request(input: Vec<Value>) -> ResponsesRequest {
        ResponsesRequest {
            model: "test-model".into(),
            instructions: "system".into(),
            input,
            tools: Vec::new(),
            max_output_tokens: 32,
            stream: true,
            store: false,
            include: None,
            reasoning: None,
        }
    }

    #[test]
    fn endpoint_conversion_preserves_path_and_query() {
        assert_eq!(
            websocket_endpoint("https://api.example.com/v1/responses?region=test").unwrap(),
            "wss://api.example.com/v1/responses?region=test"
        );
        assert_eq!(
            websocket_endpoint("http://127.0.0.1:8080/responses").unwrap(),
            "ws://127.0.0.1:8080/responses"
        );
        assert!(websocket_endpoint("wss://api.example.com/responses").is_err());
    }

    #[test]
    fn strict_incremental_selection_requires_chain_envelope_and_prefix() {
        let chain = ProviderRuntimeChainId::new();
        let first = RequestSnapshot::new(&request(vec![json!({"type":"message","n":1})])).unwrap();
        let state =
            first.continuation_after(chain, "resp_1".into(), &[json!({"type":"message","n":2})]);
        let next = RequestSnapshot::new(&request(vec![
            json!({"type":"message","n":1}),
            json!({"type":"message","n":2}),
            json!({"type":"message","n":3}),
        ]))
        .unwrap();
        let selection = state.selection_for(chain, &next);
        assert!(matches!(
            selection,
            Some(RequestSelection::Incremental { prefix_len: 2, .. })
        ));

        assert!(state
            .selection_for(ProviderRuntimeChainId::new(), &next)
            .is_none());
        let mut changed = request(next.input.clone());
        changed.instructions = "different".into();
        assert!(state
            .selection_for(chain, &RequestSnapshot::new(&changed).unwrap())
            .is_none());
        let mismatch = RequestSnapshot::new(&request(vec![
            json!({"type":"message","n":9}),
            json!({"type":"message","n":3}),
        ]))
        .unwrap();
        assert!(state.selection_for(chain, &mismatch).is_none());
    }

    #[test]
    fn model_context_participates_in_exact_prefix_and_changes_are_suffixes() {
        let first_context = json!({
            "type":"message","role":"user","content":[{
                "type":"input_text","text":"<runtime_context>first</runtime_context>"
            }]
        });
        let first_user = json!({
            "type":"message","role":"user","content":[{
                "type":"input_text","text":"hello"
            }]
        });
        let first = RequestSnapshot::new(&request(vec![first_context.clone(), first_user.clone()]))
            .unwrap();
        let chain = ProviderRuntimeChainId::new();
        let output = json!({"type":"message","role":"assistant","content":[]});
        let state = first.continuation_after(chain, "resp_1".into(), std::slice::from_ref(&output));
        let next_context = json!({
            "type":"message","role":"user","content":[{
                "type":"input_text","text":"<runtime_context>second</runtime_context>"
            }]
        });
        let next_user = json!({
            "type":"message","role":"user","content":[{
                "type":"input_text","text":"next"
            }]
        });
        let next = RequestSnapshot::new(&request(vec![
            first_context,
            first_user,
            output.clone(),
            next_context.clone(),
            next_user.clone(),
        ]))
        .unwrap();
        let selection = state.selection_for(chain, &next).unwrap();
        assert!(matches!(
            selection,
            RequestSelection::Incremental { prefix_len: 3, .. }
        ));
        let payload: Value =
            serde_json::from_str(&next.websocket_payload(&selection).unwrap()).unwrap();
        assert_eq!(payload["input"], json!([next_context, next_user]));

        let replaced_prefix = RequestSnapshot::new(&request(vec![
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"<runtime_context>changed</runtime_context>"}]}),
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}),
            output,
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"next"}]}),
        ]))
        .unwrap();
        assert!(state.selection_for(chain, &replaced_prefix).is_none());
    }

    #[test]
    fn websocket_payload_omits_http_stream_and_sends_incremental_suffix() {
        let snapshot = RequestSnapshot::new(&request(vec![
            json!({"type":"message","n":1}),
            json!({"type":"message","n":2}),
        ]))
        .unwrap();
        let payload = snapshot
            .websocket_payload(&RequestSelection::Incremental {
                previous_response_id: "resp_1".into(),
                prefix_len: 1,
            })
            .unwrap();
        let payload: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["previous_response_id"], "resp_1");
        assert_eq!(payload["input"], json!([{"type":"message","n":2}]));
        assert!(payload.get("stream").is_none());
    }

    #[tokio::test]
    async fn full_then_incremental_reuses_same_connection_and_sends_only_suffix() {
        let (server, state) = start_websocket_server(FakeBehavior::Success).await;
        let transport = transport(&server.endpoint, 2);
        let chain = ProviderRuntimeChainId::new();
        let first_input = json!({"type":"message","role":"user","content":"one"});
        let first_request = request(vec![first_input.clone()]);

        let (first, events) = send_transport(&transport, &first_request, chain, 0).await;
        let WebSocketSendOutcome::Response(first) = first else {
            panic!("expected websocket response");
        };
        assert_eq!(events.len(), 1);

        let new_input = json!({"type":"message","role":"user","content":"two"});
        let mut second_input = vec![first_input];
        second_input.extend(first.output_items.clone());
        second_input.push(new_input.clone());
        let (second, _) = send_transport(&transport, &request(second_input), chain, 0).await;
        assert!(matches!(second, WebSocketSendOutcome::Response(_)));

        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].get("previous_response_id").is_none());
        assert_eq!(requests[1]["previous_response_id"], "resp_1");
        assert_eq!(requests[1]["input"], json!([new_input]));
    }

    #[tokio::test]
    async fn responses_client_reports_websocket_as_actual_transport() {
        let (server, _) = start_websocket_server(FakeBehavior::Success).await;
        let client = super::super::ResponsesClient::new(
            server.endpoint.clone(),
            "test-key".into(),
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(1)
        .unwrap();
        let chain = ProviderRuntimeChainId::new();

        let (response, transport) = client
            .send_with_retry_count_for_runtime_scope_and_transport(
                &request(vec![json!({"type":"message","n":1})]),
                0,
                Some(chain),
                None,
                false,
                None,
                &mut |_| {},
            )
            .await
            .unwrap();

        assert_eq!(response.output_text, "ok");
        assert_eq!(transport, crate::api::ProviderTransport::ResponsesWebSocket);
    }

    #[tokio::test]
    async fn reasoning_and_tool_items_use_shared_reducer_without_visible_delta() {
        let (server, _) = start_websocket_server(FakeBehavior::ReasoningAndTool).await;
        let transport = transport(&server.endpoint, 1);
        let (outcome, events) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            ProviderRuntimeChainId::new(),
            0,
        )
        .await;
        let WebSocketSendOutcome::Response(response) = outcome else {
            panic!("expected websocket response");
        };
        assert!(events.is_empty());
        assert_eq!(response.output_items[0]["type"], "reasoning");
        assert_eq!(response.function_calls.len(), 1);
        assert_eq!(response.function_calls[0].call_id, "call_1");
    }

    #[tokio::test]
    async fn max_output_continuation_uses_previous_response_and_incremental_marker() {
        use crate::api::{
            OpenAiCompatibleResponsesProviderAdapter, ProviderAdapter, ProviderRequest,
            ProviderStop, SessionTurnContentBlock, SessionTurnMessage,
        };

        let (server, state) = start_websocket_server(FakeBehavior::MaxOutputThenSuccess).await;
        let adapter = OpenAiCompatibleResponsesProviderAdapter::new(
            "test-key".into(),
            server.endpoint.clone(),
            "test-model".into(),
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(true, 1)
        .unwrap();
        let response = adapter
            .send(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 32,
                    stream: true,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: Some(ProviderRuntimeChainId::new()),
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    retry_count_override: Some(0),
                },
                &mut |_| {},
            )
            .await
            .unwrap();

        assert_eq!(response.stop, ProviderStop::Done);
        assert!(matches!(
            &response.assistant_message.content[0],
            SessionTurnContentBlock::Text { text } if text == "first halfok"
        ));
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1]["previous_response_id"], "resp_partial");
        let suffix = requests[1]["input"].as_array().unwrap();
        assert_eq!(suffix.len(), 1);
        assert_eq!(
            suffix[0]["content"][0]["text"],
            crate::api::continuation::CONTINUATION_TRIGGER
        );
    }

    #[tokio::test]
    async fn max_output_continuation_retries_wrapped_rate_limit_error() {
        use crate::api::{
            OpenAiCompatibleResponsesProviderAdapter, ProviderAdapter, ProviderRequest,
            ProviderStop, SessionTurnContentBlock, SessionTurnMessage,
        };

        let (server, state) =
            start_websocket_server(FakeBehavior::MaxOutputThenRateLimitThenSuccess).await;
        let adapter = OpenAiCompatibleResponsesProviderAdapter::new(
            "test-key".into(),
            server.endpoint.clone(),
            "test-model".into(),
            Duration::from_secs(2),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(true, 1)
        .unwrap();
        let response = adapter
            .send(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 32,
                    stream: true,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: Some(ProviderRuntimeChainId::new()),
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    retry_count_override: None,
                },
                &mut |_| {},
            )
            .await
            .unwrap();

        assert_eq!(response.stop, ProviderStop::Done);
        assert!(matches!(
            &response.assistant_message.content[0],
            SessionTurnContentBlock::Text { text } if text == "first halfok"
        ));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 0);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1]["previous_response_id"], "resp_partial");
        assert!(requests[2].get("previous_response_id").is_none());
    }

    #[tokio::test]
    async fn wrapped_status_code_server_error_is_retried() {
        let (server, state) = start_websocket_server(FakeBehavior::ServerErrorStatusCodeOnce).await;
        let transport = transport(&server.endpoint, 1);
        let (outcome, _) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            ProviderRuntimeChainId::new(),
            1,
        )
        .await;

        assert!(matches!(outcome, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wrapped_rate_limit_exhaustion_does_not_stick_runtime_chain() {
        let (server, state) = start_websocket_server(FakeBehavior::RateLimitError).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();

        for n in 1..=2 {
            let (outcome, events) = send_transport(
                &transport,
                &request(vec![json!({"type":"message","n":n})]),
                chain,
                0,
            )
            .await;
            assert!(matches!(outcome, WebSocketSendOutcome::FallbackToHttp));
            assert!(events.is_empty());
            assert!(!transport.pool.is_sticky(chain).await);
        }

        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wrapped_unauthorized_error_is_deterministic() {
        let (server, state) = start_websocket_server(FakeBehavior::UnauthorizedError).await;
        let transport = transport(&server.endpoint, 1);
        let mut events = Vec::new();
        let chain = ProviderRuntimeChainId::new();

        let error = transport
            .send_with_retry_count(
                &request(vec![json!({"type":"message","n":1})]),
                chain,
                2,
                Duration::ZERO,
                Duration::ZERO,
                None,
                &mut |event| events.push(event),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ResponsesError::Status { status: 401, .. }));
        assert!(events.is_empty());
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 0);
        assert!(!transport.pool.is_sticky(chain).await);
    }

    #[tokio::test]
    async fn changed_effective_history_uses_full_input_and_starts_new_chain() {
        let (server, state) = start_websocket_server(FakeBehavior::Success).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();
        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","history":"before-compact"})]),
            chain,
            0,
        )
        .await;
        let compacted = json!({"type":"message","history":"compacted-summary"});
        let _ = send_transport(&transport, &request(vec![compacted.clone()]), chain, 0).await;

        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[1].get("previous_response_id").is_none());
        assert_eq!(requests[1]["input"], json!([compacted]));
    }

    #[tokio::test]
    async fn discarded_chain_keeps_healthy_socket_but_forces_full_history() {
        let (server, state) = start_websocket_server(FakeBehavior::Success).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();
        let first_input = json!({"type":"message","n":1});
        let (first, _) =
            send_transport(&transport, &request(vec![first_input.clone()]), chain, 0).await;
        let WebSocketSendOutcome::Response(first) = first else {
            panic!("expected websocket response");
        };
        transport.discard_runtime_chain(chain).await;

        let mut history = vec![first_input];
        history.extend(first.output_items);
        history.push(json!({"type":"message","n":2}));
        let _ = send_transport(&transport, &request(history.clone()), chain, 0).await;

        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        let requests = state.requests.lock().await;
        assert!(requests[1].get("previous_response_id").is_none());
        assert_eq!(requests[1]["input"], Value::Array(history));
    }

    #[tokio::test]
    async fn idle_pump_handles_server_ping_and_connection_remains_reusable() {
        let (server, state) = start_websocket_server(FakeBehavior::PingBetweenRequests).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();
        let first_input = json!({"type":"message","n":1});
        let (first, _) =
            send_transport(&transport, &request(vec![first_input.clone()]), chain, 0).await;
        let WebSocketSendOutcome::Response(first) = first else {
            panic!("expected websocket response");
        };
        tokio::task::yield_now().await;
        let mut history = vec![first_input];
        history.extend(first.output_items);
        history.push(json!({"type":"message","n":2}));
        let (second, _) = send_transport(&transport, &request(history), chain, 0).await;

        assert!(matches!(second, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn idle_response_data_invalidates_connection_before_next_lease() {
        let (server, state) =
            start_websocket_server(FakeBehavior::TrailingEventAfterTerminal).await;
        let transport = transport(&server.endpoint, 2);
        let chain = ProviderRuntimeChainId::new();
        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            chain,
            0,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            chain,
            0,
        )
        .await;

        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn connection_older_than_internal_limit_is_replaced_before_lease() {
        let (server, state) = start_websocket_server(FakeBehavior::Success).await;
        let transport = transport(&server.endpoint, 2);
        let chain = ProviderRuntimeChainId::new();
        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            chain,
            0,
        )
        .await;
        {
            let mut inner = transport.pool.inner.lock().await;
            inner.idle[0].created_at = Instant::now() - MAX_CONNECTION_AGE;
        }
        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            chain,
            0,
        )
        .await;

        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn previous_response_not_found_reconnects_once_with_full_history() {
        let (server, state) = start_websocket_server(FakeBehavior::PreviousNotFoundOnce).await;
        let transport = transport(&server.endpoint, 2);
        let chain = ProviderRuntimeChainId::new();
        let first_input = json!({"type":"message","n":1});
        let (first, _) =
            send_transport(&transport, &request(vec![first_input.clone()]), chain, 0).await;
        let WebSocketSendOutcome::Response(first) = first else {
            panic!("expected first websocket response");
        };
        let mut history = vec![first_input];
        history.extend(first.output_items);
        history.push(json!({"type":"message","n":2}));

        let (recovered, _) = send_transport(&transport, &request(history.clone()), chain, 0).await;
        assert!(matches!(recovered, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert!(requests[1].get("previous_response_id").is_some());
        assert!(requests[2].get("previous_response_id").is_none());
        assert_eq!(requests[2]["input"], Value::Array(history));
    }

    #[tokio::test]
    async fn connection_limit_rotates_socket_without_sticky_downgrade() {
        let (server, state) = start_websocket_server(FakeBehavior::ConnectionLimitOnce).await;
        let transport = transport(&server.endpoint, 2);
        let chain = ProviderRuntimeChainId::new();
        let input = json!({"type":"message","n":1});
        let (first, _) = send_transport(&transport, &request(vec![input.clone()]), chain, 1).await;
        let WebSocketSendOutcome::Response(first) = first else {
            panic!("expected recovered websocket response");
        };
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
        let mut history = vec![input];
        history.extend(first.output_items);
        history.push(json!({"type":"message","n":2}));
        let (second, _) = send_transport(&transport, &request(history), chain, 0).await;
        assert!(matches!(second, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn independent_chains_can_stream_concurrently_on_distinct_leases() {
        let barrier = Arc::new(Barrier::new(2));
        let (server, state) = start_websocket_server(FakeBehavior::Concurrent(barrier)).await;
        let transport = Arc::new(transport(&server.endpoint, 2));
        let mut tasks = Vec::new();
        for n in 1..=2 {
            let transport = Arc::clone(&transport);
            tasks.push(tokio::spawn(async move {
                send_transport(
                    &transport,
                    &request(vec![json!({"type":"message","n":n})]),
                    ProviderRuntimeChainId::new(),
                    0,
                )
                .await
            }));
        }
        for task in tasks {
            assert!(matches!(
                task.await.unwrap().0,
                WebSocketSendOutcome::Response(_)
            ));
        }
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn full_history_lease_preserves_other_chain_affinity_when_unbound_idle_exists() {
        let barrier = Arc::new(Barrier::new(2));
        let (server, _) = start_websocket_server(FakeBehavior::Concurrent(barrier)).await;
        let transport = Arc::new(transport(&server.endpoint, 2));
        let chain_a = ProviderRuntimeChainId::new();
        let chain_b = ProviderRuntimeChainId::new();
        let first = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                send_transport(
                    &transport,
                    &request(vec![json!({"type":"message","n":"a"})]),
                    chain_a,
                    0,
                )
                .await
            })
        };
        let second = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                send_transport(
                    &transport,
                    &request(vec![json!({"type":"message","n":"b"})]),
                    chain_b,
                    0,
                )
                .await
            })
        };
        let (first, second) = tokio::join!(first, second);
        assert!(matches!(
            first.unwrap().0,
            WebSocketSendOutcome::Response(_)
        ));
        assert!(matches!(
            second.unwrap().0,
            WebSocketSendOutcome::Response(_)
        ));
        {
            let mut inner = transport.pool.inner.lock().await;
            let chain_a_connection = inner
                .idle
                .iter_mut()
                .find(|connection| {
                    connection
                        .continuation
                        .as_ref()
                        .is_some_and(|state| state.chain_id == chain_a)
                })
                .unwrap();
            chain_a_connection.last_used_at = Instant::now() - Duration::from_secs(10);
            let chain_b_connection = inner
                .idle
                .iter_mut()
                .find(|connection| {
                    connection
                        .continuation
                        .as_ref()
                        .is_some_and(|state| state.chain_id == chain_b)
                })
                .unwrap();
            chain_b_connection.continuation = None;
        }

        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":"c"})]),
            ProviderRuntimeChainId::new(),
            0,
        )
        .await;

        let inner = transport.pool.inner.lock().await;
        assert!(inner.idle.iter().any(|connection| {
            connection
                .continuation
                .as_ref()
                .is_some_and(|state| state.chain_id == chain_a)
        }));
    }

    #[tokio::test]
    async fn spare_capacity_opens_new_socket_instead_of_stealing_foreign_affinity() {
        let (server, state) = start_websocket_server(FakeBehavior::Success).await;
        let transport = transport(&server.endpoint, 2);
        let chain_a = ProviderRuntimeChainId::new();
        let chain_b = ProviderRuntimeChainId::new();
        let first_input = json!({"type":"message","role":"user","content":"a1"});

        let (first, _) =
            send_transport(&transport, &request(vec![first_input.clone()]), chain_a, 0).await;
        let WebSocketSendOutcome::Response(first) = first else {
            panic!("expected first websocket response");
        };
        let _ = send_transport(
            &transport,
            &request(vec![json!({"type":"message","role":"user","content":"b1"})]),
            chain_b,
            0,
        )
        .await;

        let mut next_a = vec![first_input];
        next_a.extend(first.output_items);
        let suffix = json!({"type":"message","role":"user","content":"a2"});
        next_a.push(suffix.clone());
        let _ = send_transport(&transport, &request(next_a), chain_a, 0).await;

        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert!(requests[1].get("previous_response_id").is_none());
        assert_eq!(requests[2]["previous_response_id"], "resp_1");
        assert_eq!(requests[2]["input"], json!([suffix]));
    }

    #[tokio::test]
    async fn request_waiting_at_capacity_reuses_connection_returned_to_idle_pool() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (server, state) = start_websocket_server(FakeBehavior::GateFirst {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .await;
        let transport = Arc::new(transport(&server.endpoint, 1));
        let first_transport = Arc::clone(&transport);
        let first = tokio::spawn(async move {
            send_transport(
                &first_transport,
                &request(vec![json!({"type":"message","n":1})]),
                ProviderRuntimeChainId::new(),
                0,
            )
            .await
        });
        started.notified().await;

        let second_transport = Arc::clone(&transport);
        let second = tokio::spawn(async move {
            send_transport(
                &second_transport,
                &request(vec![json!({"type":"message","n":2})]),
                ProviderRuntimeChainId::new(),
                0,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert_eq!(state.requests.lock().await.len(), 1);

        release.notify_one();
        let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(first, second)
        })
        .await
        .expect("waiting request should wake when a connection returns to idle");
        assert!(matches!(
            first.unwrap().0,
            WebSocketSendOutcome::Response(_)
        ));
        assert!(matches!(
            second.unwrap().0,
            WebSocketSendOutcome::Response(_)
        ));
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[1].get("previous_response_id").is_none());
    }

    #[tokio::test]
    async fn cancelling_in_flight_request_discards_socket() {
        let notify = Arc::new(Notify::new());
        let (server, state) =
            start_websocket_server(FakeBehavior::BlockFirst(Arc::clone(&notify))).await;
        let transport = Arc::new(transport(&server.endpoint, 2));
        let running_transport = Arc::clone(&transport);
        let running = tokio::spawn(async move {
            send_transport(
                &running_transport,
                &request(vec![json!({"type":"message","n":1})]),
                ProviderRuntimeChainId::new(),
                0,
            )
            .await
        });
        notify.notified().await;
        running.abort();
        let _ = running.await;

        let (second, _) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            ProviderRuntimeChainId::new(),
            0,
        )
        .await;
        assert!(matches!(second, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn safe_steer_after_success_keeps_socket_but_clears_continuation() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (server, state) = start_websocket_server(FakeBehavior::GateFirst {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .await;
        let transport = Arc::new(transport(&server.endpoint, 1));
        let chain = ProviderRuntimeChainId::new();
        let interrupt = ProviderRecoveryInterrupt::new();
        let running_transport = Arc::clone(&transport);
        let running_interrupt = interrupt.clone();
        let running = tokio::spawn(async move {
            let mut events = Vec::new();
            running_transport
                .send_with_retry_count(
                    &request(vec![json!({"type":"message","n":1})]),
                    chain,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                    Some(&running_interrupt),
                    &mut |event| events.push(event),
                )
                .await
        });
        started.notified().await;
        interrupt.cancel();
        release.notify_one();
        assert!(matches!(
            running.await.unwrap().unwrap(),
            WebSocketSendOutcome::Response(_)
        ));

        let (second, _) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            chain,
            0,
        )
        .await;
        assert!(matches!(second, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[1].get("previous_response_id").is_none());
    }

    #[tokio::test]
    async fn request_timeout_discards_socket_and_falls_back_without_visible_delta() {
        let notify = Arc::new(Notify::new());
        let (server, state) =
            start_websocket_server(FakeBehavior::BlockFirst(Arc::clone(&notify))).await;
        let transport = transport_with_timeout(&server.endpoint, 1, Duration::from_millis(100));
        let chain = ProviderRuntimeChainId::new();

        let (timed_out, events) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            chain,
            0,
        )
        .await;
        assert!(matches!(timed_out, WebSocketSendOutcome::FallbackToHttp));
        assert!(events.is_empty());
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);

        let (next, _) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            ProviderRuntimeChainId::new(),
            0,
        )
        .await;
        assert!(matches!(next, WebSocketSendOutcome::Response(_)));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn agent_timeout_reaches_ws_sticky_then_uses_http_on_following_turn() {
        use crate::api::{
            AgentTurnLoop, OpenAiCompatibleResponsesProviderAdapter, ProviderAdapter,
            SessionTurnContentBlock, SessionTurnRequest,
        };
        use crate::config::ToolConfig;
        use crate::tool::ToolRegistry;

        let notify = Arc::new(Notify::new());
        let (server, state) =
            start_websocket_server(FakeBehavior::BlockFirst(Arc::clone(&notify))).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                server.endpoint.clone(),
                "test-model".into(),
                Duration::from_millis(100),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(true, 1)
            .unwrap(),
        );
        assert_eq!(adapter.request_timeout(), None);
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 32);
        let chain = ProviderRuntimeChainId::new();

        for user_text in ["first", "second"] {
            let turn = turn_loop
                .run_session_turn_with_runtime_chain_hooks(
                    SessionTurnRequest {
                        current_session_id: None,
                        current_turn_id: None,
                        system_prompt: "system".into(),
                        history: Vec::new(),
                        user_text: user_text.into(),
                        user_attachments: Vec::new(),
                        skill_instructions: Vec::new(),
                    },
                    chain,
                    &mut |_| {},
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            assert!(turn.messages.iter().any(|message| {
                message.content.iter().any(|block| {
                    matches!(block, SessionTurnContentBlock::Text { text } if text == "http")
                })
            }));
        }

        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn visible_partial_ws_failure_uses_json_then_sticky_http_next_turn() {
        use crate::api::{
            AgentTurnLoop, OpenAiCompatibleResponsesProviderAdapter, SessionTurnContentBlock,
            SessionTurnEvent, SessionTurnRequest,
        };
        use crate::config::ToolConfig;
        use crate::tool::ToolRegistry;

        let (server, state) = start_websocket_server(FakeBehavior::VisibleThenClose).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                server.endpoint.clone(),
                "test-model".into(),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(true, 1)
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 32);
        let chain = ProviderRuntimeChainId::new();
        let mut first_events = Vec::new();

        let first = turn_loop
            .run_session_turn_with_runtime_chain_hooks(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "first".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                chain,
                &mut |event| first_events.push(event),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(first.messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(block, SessionTurnContentBlock::Text { text } if text == "json replacement")
            })
        }));
        assert!(first_events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::AssistantTextDelta { text } if text == "partial"
        )));
        assert!(first_events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { text, .. }
                if text == "json replacement"
        )));

        let second = turn_loop
            .run_session_turn_with_runtime_chain_hooks(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "second".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                chain,
                &mut |_| {},
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(second.messages.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, SessionTurnContentBlock::Text { text } if text == "http"),
            )
        }));

        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ws_downgrade_then_sse_header_timeout_reaches_json_replacement() {
        use crate::api::{
            AgentTurnLoop, OpenAiCompatibleResponsesProviderAdapter, SessionTurnContentBlock,
            SessionTurnEvent, SessionTurnRequest,
        };
        use crate::config::ToolConfig;
        use crate::tool::ToolRegistry;

        let (server, state) = start_websocket_server(FakeBehavior::HttpStreamTimeoutThenJson).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                server.endpoint.clone(),
                "test-model".into(),
                Duration::from_millis(100),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(true, 1)
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 32);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap();

        assert!(turn.messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(block, SessionTurnContentBlock::Text { text } if text == "json replacement")
            })
        }));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { attempt: 1, .. }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { attempt: 1, text, .. }
                if text == "json replacement"
        )));
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn continuation_bad_request_stops_without_json_replacement() {
        use crate::api::provider::ProviderTerminalFailure;
        use crate::api::{
            AgentTurnLoop, OpenAiCompatibleResponsesProviderAdapter, SessionTurnEvent,
            SessionTurnRequest,
        };
        use crate::config::ToolConfig;
        use crate::tool::ToolRegistry;

        let (server, state) =
            start_websocket_server(FakeBehavior::HttpMaxOutputThenBadRequest).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                server.endpoint.clone(),
                "test-model".into(),
                Duration::from_secs(2),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(true, 1)
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 32);
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<ProviderTerminalFailure>().is_some());
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::AssistantTextDelta { text } if text == "first half"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
        )));
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn accepted_safe_steer_stops_ws_recovery_before_retry_or_http_fallback() {
        use crate::api::{
            AgentTurnLoop, OpenAiCompatibleResponsesProviderAdapter, SessionTurnEvent,
            SessionTurnInterrupted, SessionTurnRequest, ToolBoundaryControl, ToolCallSkipReason,
        };
        use crate::config::ToolConfig;
        use crate::tool::ToolRegistry;

        let started = Arc::new(Notify::new());
        let close = Arc::new(Notify::new());
        let (server, state) = start_websocket_server(FakeBehavior::CloseAfterSignal {
            started: Arc::clone(&started),
            close: Arc::clone(&close),
        })
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                server.endpoint.clone(),
                "test-model".into(),
                Duration::from_secs(2),
                3,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(true, 1)
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 32);
        let control = ToolBoundaryControl::new();
        let mut events = Vec::new();
        let error = {
            let mut emit = |event| events.push(event);
            let mut running = Box::pin(turn_loop.run_session_turn_with_tool_boundary_control(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "first".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut emit,
                Some(control.clone()),
            ));

            tokio::select! {
                () = started.notified() => {}
                result = &mut running => panic!("turn ended before WebSocket request started: {result:?}"),
            }
            control.cancel_if_open(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            close.notify_one();
            tokio::time::timeout(Duration::from_secs(2), running.as_mut())
                .await
                .expect("safe steer should stop recovery after the current request closes")
                .unwrap_err()
        };

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
        assert_eq!(state.requests.lock().await.len(), 1);
        assert_eq!(state.http_requests.load(Ordering::SeqCst), 0);
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
        )));
    }

    #[tokio::test]
    async fn visible_partial_transport_failure_sticks_chain_without_ws_retry() {
        let (server, state) = start_websocket_server(FakeBehavior::VisibleThenClose).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();
        let mut events = Vec::new();
        let error = transport
            .send_with_retry_count(
                &request(vec![json!({"type":"message","n":1})]),
                chain,
                3,
                Duration::ZERO,
                Duration::ZERO,
                None,
                &mut |event| events.push(event),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ResponsesError::StreamFailure { .. }));
        assert_eq!(events.len(), 1);
        assert!(transport.pool.is_sticky(chain).await);
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);

        let (next, next_events) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            chain,
            3,
        )
        .await;
        assert!(matches!(next, WebSocketSendOutcome::FallbackToHttp));
        assert!(next_events.is_empty());
        assert_eq!(state.connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn visible_partial_rate_limit_does_not_stick_runtime_chain() {
        let (server, state) = start_websocket_server(FakeBehavior::VisibleThenRateLimit).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();

        for n in 1..=2 {
            let mut events = Vec::new();
            let error = transport
                .send_with_retry_count(
                    &request(vec![json!({"type":"message","n":n})]),
                    chain,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                    None,
                    &mut |event| events.push(event),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, ResponsesError::Status { status: 429, .. }));
            assert_eq!(events.len(), 1);
            assert!(!transport.pool.is_sticky(chain).await);
        }

        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn zero_delta_transport_failure_retries_then_sticks_to_http() {
        let (server, state) = start_websocket_server(FakeBehavior::CloseBeforeEvents).await;
        let transport = transport(&server.endpoint, 1);
        let chain = ProviderRuntimeChainId::new();
        let (first, events) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            chain,
            1,
        )
        .await;
        assert!(matches!(first, WebSocketSendOutcome::FallbackToHttp));
        assert!(events.is_empty());
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);

        let (second, _) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":2})]),
            chain,
            1,
        )
        .await;
        assert!(matches!(second, WebSocketSendOutcome::FallbackToHttp));
        assert_eq!(state.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn binary_frame_is_transport_failure_and_never_a_success() {
        let (server, _) = start_websocket_server(FakeBehavior::BinaryFrame).await;
        let transport = transport(&server.endpoint, 1);
        let (outcome, events) = send_transport(
            &transport,
            &request(vec![json!({"type":"message","n":1})]),
            ProviderRuntimeChainId::new(),
            0,
        )
        .await;
        assert!(matches!(outcome, WebSocketSendOutcome::FallbackToHttp));
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn malformed_or_nonterminal_internal_items_never_become_ws_success() {
        for behavior in [
            FakeBehavior::MalformedJson,
            FakeBehavior::ReasoningThenClose,
            FakeBehavior::ToolThenClose,
        ] {
            let (server, _) = start_websocket_server(behavior).await;
            let transport = transport(&server.endpoint, 1);
            let (outcome, events) = send_transport(
                &transport,
                &request(vec![json!({"type":"message","n":1})]),
                ProviderRuntimeChainId::new(),
                0,
            )
            .await;
            assert!(matches!(outcome, WebSocketSendOutcome::FallbackToHttp));
            assert!(events.is_empty());
        }
    }

    #[tokio::test]
    async fn terminal_without_response_id_is_not_accepted_as_websocket_success() {
        let (server, _) = start_websocket_server(FakeBehavior::MissingTerminalId).await;
        let transport = transport(&server.endpoint, 1);
        let mut events = Vec::new();
        let error = transport
            .send_with_retry_count(
                &request(vec![json!({"type":"message","n":1})]),
                ProviderRuntimeChainId::new(),
                0,
                Duration::ZERO,
                Duration::ZERO,
                None,
                &mut |event| events.push(event),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ResponsesError::StreamFailure { .. }));
        assert_eq!(events.len(), 1);
    }

    struct HandshakeState {
        status: StatusCode,
        handshakes: AtomicUsize,
        redirects: AtomicUsize,
        posts: AtomicUsize,
    }

    async fn start_handshake_server(status: StatusCode) -> (TestServer, Arc<HandshakeState>) {
        let state = Arc::new(HandshakeState {
            status,
            handshakes: AtomicUsize::new(0),
            redirects: AtomicUsize::new(0),
            posts: AtomicUsize::new(0),
        });
        let app = Router::new()
            .route(
                "/v1/responses",
                get({
                    let state = Arc::clone(&state);
                    move || {
                        let state = Arc::clone(&state);
                        async move {
                            state.handshakes.fetch_add(1, Ordering::SeqCst);
                            if state.status.is_redirection() {
                                (state.status, [(LOCATION, "/redirected")]).into_response()
                            } else {
                                state.status.into_response()
                            }
                        }
                    }
                })
                .post({
                    let state = Arc::clone(&state);
                    move || {
                        let state = Arc::clone(&state);
                        async move {
                            state.posts.fetch_add(1, Ordering::SeqCst);
                            let item = json!({
                                "type":"message","role":"assistant","status":"completed",
                                "content":[{"type":"output_text","text":"http","annotations":[]}]
                            });
                            let frames = format!(
                                "data: {}\n\ndata: {}\n\n",
                                json!({"type":"response.output_item.done","output_index":0,"item":item}),
                                json!({"type":"response.completed","response":{"status":"completed"}}),
                            );
                            ([(CONTENT_TYPE, "text/event-stream")], frames)
                        }
                    }
                }),
            )
            .route(
                "/redirected",
                get({
                    let state = Arc::clone(&state);
                    move || {
                        let state = Arc::clone(&state);
                        async move {
                            state.redirects.fetch_add(1, Ordering::SeqCst);
                            StatusCode::BAD_REQUEST
                        }
                    }
                }),
            )
            .with_state(());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (
            TestServer {
                endpoint: format!("http://{address}/v1/responses"),
                task,
            },
            state,
        )
    }

    #[tokio::test]
    async fn upgrade_required_is_immediately_sticky_for_only_that_chain() {
        let (server, state) = start_handshake_server(StatusCode::UPGRADE_REQUIRED).await;
        let client = super::super::ResponsesClient::new(
            server.endpoint.clone(),
            "test-key".into(),
            Duration::from_secs(2),
            3,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(2)
        .unwrap();
        let chain = ProviderRuntimeChainId::new();
        for n in 1..=2 {
            let response = client
                .send_with_retry_count_for_runtime_chain(
                    &request(vec![json!({"type":"message","n":n})]),
                    3,
                    Some(chain),
                    None,
                    &mut |_| {},
                )
                .await
                .unwrap();
            assert_eq!(response.output_text, "http");
        }
        assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);
        assert_eq!(state.posts.load(Ordering::SeqCst), 2);

        let _ = client
            .send_with_retry_count_for_runtime_chain(
                &request(vec![json!({"type":"message","n":3})]),
                0,
                Some(ProviderRuntimeChainId::new()),
                None,
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(state.handshakes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn not_found_retries_before_becoming_sticky() {
        let (server, state) = start_handshake_server(StatusCode::NOT_FOUND).await;
        let client = super::super::ResponsesClient::new(
            server.endpoint.clone(),
            "test-key".into(),
            Duration::from_secs(2),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(1)
        .unwrap();
        let chain = ProviderRuntimeChainId::new();
        for n in 1..=2 {
            let _ = client
                .send_with_retry_count_for_runtime_chain(
                    &request(vec![json!({"type":"message","n":n})]),
                    1,
                    Some(chain),
                    None,
                    &mut |_| {},
                )
                .await
                .unwrap();
        }
        assert_eq!(state.handshakes.load(Ordering::SeqCst), 2);
        assert_eq!(state.posts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn other_handshake_rejections_retry_then_fallback_and_become_sticky() {
        for status in [StatusCode::BAD_REQUEST, StatusCode::FORBIDDEN] {
            let (server, state) = start_handshake_server(status).await;
            let client = super::super::ResponsesClient::new(
                server.endpoint.clone(),
                "test-key".into(),
                Duration::from_secs(2),
                1,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(1)
            .unwrap();
            let chain = ProviderRuntimeChainId::new();
            for n in 1..=2 {
                let response = client
                    .send_with_retry_count_for_runtime_chain(
                        &request(vec![json!({"type":"message","n":n})]),
                        1,
                        Some(chain),
                        None,
                        &mut |_| {},
                    )
                    .await
                    .unwrap();
                assert_eq!(response.output_text, "http");
            }
            assert_eq!(state.handshakes.load(Ordering::SeqCst), 2, "{status}");
            assert_eq!(state.posts.load(Ordering::SeqCst), 2, "{status}");
        }
    }

    #[tokio::test]
    async fn transient_handshake_statuses_retry_without_sticky_downgrade() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let (server, state) = start_handshake_server(status).await;
            let client = super::super::ResponsesClient::new(
                server.endpoint.clone(),
                "test-key".into(),
                Duration::from_secs(2),
                1,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap()
            .with_websockets(1)
            .unwrap();
            let chain = ProviderRuntimeChainId::new();
            let fallback_scope = ProviderRuntimeFallbackScope::new_root();

            for n in 1..=2 {
                let response = client
                    .send_with_retry_count_for_runtime_scope(
                        &request(vec![json!({"type":"message","n":n})]),
                        1,
                        Some(chain),
                        Some(&fallback_scope),
                        false,
                        None,
                        &mut |_| {},
                    )
                    .await
                    .unwrap();
                assert_eq!(response.output_text, "http");
                assert!(!fallback_scope.websocket_sticky(), "{status}");
            }

            assert_eq!(state.handshakes.load(Ordering::SeqCst), 4, "{status}");
            assert_eq!(state.posts.load(Ordering::SeqCst), 2, "{status}");
        }
    }

    #[tokio::test]
    async fn redirect_is_not_followed_before_http_fallback() {
        let (server, state) = start_handshake_server(StatusCode::TEMPORARY_REDIRECT).await;
        let client = super::super::ResponsesClient::new(
            server.endpoint.clone(),
            "test-key".into(),
            Duration::from_secs(2),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(1)
        .unwrap();
        let response = client
            .send_with_retry_count_for_runtime_chain(
                &request(vec![json!({"type":"message","n":1})]),
                1,
                Some(ProviderRuntimeChainId::new()),
                None,
                &mut |_| {},
            )
            .await
            .unwrap();

        assert_eq!(response.output_text, "http");
        assert_eq!(state.handshakes.load(Ordering::SeqCst), 2);
        assert_eq!(state.redirects.load(Ordering::SeqCst), 0);
        assert_eq!(state.posts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn authentication_failure_is_deterministic_and_does_not_fallback() {
        let (server, state) = start_handshake_server(StatusCode::UNAUTHORIZED).await;
        let client = super::super::ResponsesClient::new(
            server.endpoint.clone(),
            "test-key".into(),
            Duration::from_secs(2),
            3,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap()
        .with_websockets(1)
        .unwrap();
        let error = client
            .send_with_retry_count_for_runtime_chain(
                &request(vec![json!({"type":"message","n":1})]),
                3,
                Some(ProviderRuntimeChainId::new()),
                None,
                &mut |_| {},
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ResponsesError::Auth(_)));
        assert_eq!(state.handshakes.load(Ordering::SeqCst), 1);
        assert_eq!(state.posts.load(Ordering::SeqCst), 0);
    }
}
