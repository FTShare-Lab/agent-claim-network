//! Streamable HTTP MCP 的 OAuth 登录与凭据存储。
//!
//! OAuth token 与 client id 写入 server 配置指定的 keyring 或私有文件；
//! `.mcp.json` 只保存非敏感 OAuth 选项。登录使用 PKCE，并支持本机 loopback
//! callback 或 headless 环境下粘贴完整 redirect URL。

use std::io::{self, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    extract::{Query, State},
    response::Html,
    routing::get,
    Router,
};
use keyring::v1::{Entry as KeyringEntry, Error as KeyringError};
use rmcp::transport::auth::{
    AuthError, AuthorizationManager, AuthorizationMetadata, AuthorizationRequest,
    AuthorizationSession, CredentialStore, StoredCredentials,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex as AsyncMutex},
    time,
};
use tokio_util::sync::CancellationToken;

use crate::auth::sha256_hex;
use crate::mcp::config::{
    read_mcp_json_config, McpOAuthCredentialsStore, McpServerConfig, McpTransportConfig,
};
use crate::storage::FileLockGuard;

const KEYRING_SERVICE: &str = "agent-claim-network.mcp";
const LOGIN_CALLBACK_PATH: &str = "/callback";
const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);
const FILE_CREDENTIALS_DIR: &str = ".mcp-oauth";
const ENROLLMENT_MARKER_CONTENT: &str = "v1\n";
const CREDENTIAL_LOCKS_DIR: &str = ".mcp-oauth-locks";
const PENDING_CLEANUP_DIR: &str = ".mcp-oauth-cleanup";
static KEYRING_OPERATION_LOCK: OnceLock<Arc<AsyncMutex<()>>> = OnceLock::new();
/// `CredentialStore` 只能通过 `AuthError::InternalError` 回传失败原因，凭据存储故障
/// 用这个固定 message 编码，供上层还原成 `McpOAuthError::CredentialStore`。
const CREDENTIAL_STORE_UNAVAILABLE: &str = "OAuth credential store unavailable";

#[derive(Debug, thiserror::Error)]
pub enum McpOAuthError {
    #[error("MCP server '{server}' 不支持 OAuth 登录：仅 streamable_http server 可登录")]
    UnsupportedTransport { server: String },
    #[error("MCP server '{server}' 的 OAuth 配置无效")]
    InvalidConfig { server: String },
    #[error("MCP server '{server}' 的配置在登录期间已删除或变更；请重新执行登录")]
    ConfigurationChanged { server: String },
    #[error("MCP server '{server}' 无法访问 OAuth 凭据存储")]
    CredentialStore { server: String },
    #[error("MCP server '{server}' 的已保存 OAuth 凭据无效")]
    InvalidStoredCredentials { server: String },
    #[error("MCP server '{server}' 的 OAuth 仅允许 HTTPS 或本机 loopback HTTP endpoint")]
    InsecureEndpoint { server: String },
    #[error("MCP server '{server}' 的 OAuth 元数据发现或客户端注册失败：{reason}")]
    AuthorizationSetup {
        server: String,
        reason: &'static str,
    },
    #[error("MCP server '{server}' 的 OAuth 登录在 5 分钟内未完成")]
    LoginTimeout { server: String },
    #[error("MCP server '{server}' 的 OAuth 回调未返回授权码")]
    MissingAuthorizationCode { server: String },
    #[error("MCP server '{server}' 的 OAuth 回调未返回 state")]
    MissingAuthorizationState { server: String },
    #[error("MCP server '{server}' 的 OAuth 授权被拒绝或失败")]
    AuthorizationDenied { server: String },
    #[error("MCP server '{server}' 的 OAuth token 交换失败：{reason}")]
    TokenExchange {
        server: String,
        reason: &'static str,
    },
    #[error("MCP server '{server}' 无法监听 OAuth 回调地址")]
    CallbackListener { server: String },
    #[error("MCP server '{server}' 的 OAuth 回调服务异常结束")]
    CallbackServer { server: String },
    #[error("MCP server '{server}' 无法读取粘贴的 OAuth redirect URL")]
    CallbackInput { server: String },
}

#[derive(Clone)]
pub(crate) struct McpCredentialStore {
    server_name: String,
    account: String,
    backend: CredentialBackend,
    enrollment_path: PathBuf,
    mutation_lease: Arc<Mutex<Option<Arc<FileLockGuard>>>>,
}

#[derive(Clone)]
enum CredentialBackend {
    Keyring { account: String },
    File { path: PathBuf },
}

#[derive(Deserialize, Serialize)]
struct PersistedCredentials {
    credentials: StoredCredentials,
}

#[derive(Deserialize, Serialize)]
struct PendingCredentialCleanup {
    account: String,
    store: McpOAuthCredentialsStore,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpCredentialIdentity {
    client_id: String,
    issuer: Option<String>,
}

pub(crate) struct McpRuntimeAuthorization {
    pub(crate) manager: AuthorizationManager,
    pub(crate) credentials: McpCredentialStore,
    pub(crate) identity: McpCredentialIdentity,
    pub(crate) server_name: String,
    pub(crate) resource_url: String,
}

/// `mcp remove` 从写 cleanup record 起持有同一把 mutation lock，直到凭据清理结束。
#[must_use = "必须持有到 MCP server 配置删除完成"]
pub struct McpCredentialRemovalLease {
    credentials: McpCredentialStore,
    cleanup_path: Option<PathBuf>,
}

impl McpCredentialStore {
    pub(crate) fn new(
        config_path: &Path,
        server_name: &str,
        url: &str,
        store: McpOAuthCredentialsStore,
    ) -> Self {
        let account = credential_account(config_path, server_name, url);
        let backend = match store {
            McpOAuthCredentialsStore::Keyring => CredentialBackend::Keyring {
                account: account.clone(),
            },
            McpOAuthCredentialsStore::File => CredentialBackend::File {
                path: credential_file_path(config_path, &account),
            },
        };
        Self {
            server_name: server_name.to_string(),
            enrollment_path: credential_enrollment_path(config_path, &account),
            account,
            backend,
            mutation_lease: Arc::new(Mutex::new(None)),
        }
    }

    fn from_pending_cleanup(
        config_path: &Path,
        server_name: &str,
        cleanup: PendingCredentialCleanup,
    ) -> Self {
        let backend = match cleanup.store {
            McpOAuthCredentialsStore::Keyring => CredentialBackend::Keyring {
                account: cleanup.account.clone(),
            },
            McpOAuthCredentialsStore::File => CredentialBackend::File {
                path: credential_file_path(config_path, &cleanup.account),
            },
        };
        Self {
            server_name: server_name.to_string(),
            enrollment_path: credential_enrollment_path(config_path, &cleanup.account),
            account: cleanup.account,
            backend,
            mutation_lease: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn set_mutation_lease(&self, guard: FileLockGuard) {
        let mut lease = match self.mutation_lease.lock() {
            Ok(lease) => lease,
            Err(poisoned) => poisoned.into_inner(),
        };
        *lease = Some(Arc::new(guard));
    }

    pub(crate) fn clear_mutation_lease(&self) {
        let mut lease = match self.mutation_lease.lock() {
            Ok(lease) => lease,
            Err(poisoned) => poisoned.into_inner(),
        };
        lease.take();
    }

    fn mutation_lease(&self) -> Option<Arc<FileLockGuard>> {
        let lease = match self.mutation_lease.lock() {
            Ok(lease) => lease,
            Err(poisoned) => poisoned.into_inner(),
        };
        lease.clone()
    }

    async fn read(&self) -> Result<Option<String>, McpOAuthError> {
        if let CredentialBackend::File { path } = &self.backend {
            return match tokio::fs::read_to_string(path).await {
                Ok(value) => Ok(Some(value)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(_) => Err(McpOAuthError::CredentialStore {
                    server: self.server_name.clone(),
                }),
            };
        }
        let server = self.server_name.clone();
        let CredentialBackend::Keyring { account } = self.backend.clone() else {
            unreachable!("file credential store returned above");
        };
        run_keyring_operation(&self.server_name, move || {
            let entry = KeyringEntry::new(KEYRING_SERVICE, &account).map_err(|_| {
                McpOAuthError::CredentialStore {
                    server: server.clone(),
                }
            })?;
            match entry.get_password() {
                Ok(value) => Ok(Some(value)),
                Err(KeyringError::NoEntry) => Ok(None),
                Err(_) => Err(McpOAuthError::CredentialStore { server }),
            }
        })
        .await
    }

    async fn write(&self, value: String) -> Result<(), McpOAuthError> {
        let server = self.server_name.clone();
        let backend = self.backend.clone();
        let mutation_lease = self.mutation_lease();
        match backend {
            CredentialBackend::File { path } => {
                // refresh token 可能在服务端轮换；文件写入一旦开始就放进 blocking pool，
                // request future 被取消时仍会完成，避免磁盘只剩已作废的旧 token。
                tokio::task::spawn_blocking(move || {
                    let _mutation_lease = mutation_lease;
                    write_private_credentials_file(&path, &value, &server)
                })
                .await
                .map_err(|_| McpOAuthError::CredentialStore {
                    server: self.server_name.clone(),
                })?
            }
            CredentialBackend::Keyring { account } => {
                run_keyring_operation(&self.server_name, move || {
                    let _mutation_lease = mutation_lease;
                    KeyringEntry::new(KEYRING_SERVICE, &account)
                        .and_then(|entry| entry.set_password(&value))
                        .map_err(|_| McpOAuthError::CredentialStore { server })
                })
                .await
            }
        }
    }

    async fn delete(&self) -> Result<(), McpOAuthError> {
        let mutation_lease = self.mutation_lease();
        if let CredentialBackend::File { path } = self.backend.clone() {
            let server = self.server_name.clone();
            return tokio::task::spawn_blocking(move || {
                let _mutation_lease = mutation_lease;
                match std::fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                    Err(_) => Err(McpOAuthError::CredentialStore { server }),
                }
            })
            .await
            .map_err(|_| McpOAuthError::CredentialStore {
                server: self.server_name.clone(),
            })?;
        }
        let server = self.server_name.clone();
        let CredentialBackend::Keyring { account } = self.backend.clone() else {
            unreachable!("file credential store returned above");
        };
        run_keyring_operation(&self.server_name, move || {
            let _mutation_lease = mutation_lease;
            let entry = KeyringEntry::new(KEYRING_SERVICE, &account).map_err(|_| {
                McpOAuthError::CredentialStore {
                    server: server.clone(),
                }
            })?;
            match entry.delete_credential() {
                Ok(()) | Err(KeyringError::NoEntry) => Ok(()),
                Err(_) => Err(McpOAuthError::CredentialStore { server }),
            }
        })
        .await
    }

    async fn is_enrolled(&self) -> Result<bool, McpOAuthError> {
        match tokio::fs::metadata(&self.enrollment_path).await {
            Ok(metadata) if metadata.is_file() => Ok(true),
            Ok(_) => Err(McpOAuthError::CredentialStore {
                server: self.server_name.clone(),
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(McpOAuthError::CredentialStore {
                server: self.server_name.clone(),
            }),
        }
    }

    async fn mark_enrolled(&self) -> Result<(), McpOAuthError> {
        if self.is_enrolled().await? {
            return Ok(());
        }
        let path = self.enrollment_path.clone();
        let server = self.server_name.clone();
        let mutation_lease = self.mutation_lease();
        tokio::task::spawn_blocking(move || {
            let _mutation_lease = mutation_lease;
            write_private_credentials_file(&path, ENROLLMENT_MARKER_CONTENT, &server)
        })
        .await
        .map_err(|_| McpOAuthError::CredentialStore {
            server: self.server_name.clone(),
        })?
    }

    async fn clear_enrollment(&self) -> Result<(), McpOAuthError> {
        let path = self.enrollment_path.clone();
        let server = self.server_name.clone();
        let mutation_lease = self.mutation_lease();
        tokio::task::spawn_blocking(move || {
            let _mutation_lease = mutation_lease;
            match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(_) => Err(McpOAuthError::CredentialStore { server }),
            }
        })
        .await
        .map_err(|_| McpOAuthError::CredentialStore {
            server: self.server_name.clone(),
        })?
    }

    async fn delete_credentials_and_enrollment(&self) -> Result<(), McpOAuthError> {
        self.delete().await?;
        self.clear_enrollment().await
    }

    pub(crate) async fn identity(&self) -> Result<Option<McpCredentialIdentity>, AuthError> {
        Ok(self.load().await?.map(|credentials| McpCredentialIdentity {
            client_id: credentials.client_id,
            issuer: credentials.issuer,
        }))
    }
}

/// keyring API 是同步且可能等待系统 UI/Secret Service。独立线程避免 Tokio runtime
/// 在 startup timeout 后仍等待不可取消的 blocking task；进程内串行化则确保重试只等待
/// 已开始的同一次系统访问，不会叠加提示或占用线程。
async fn run_keyring_operation<T, F>(server_name: &str, operation: F) -> Result<T, McpOAuthError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, McpOAuthError> + Send + 'static,
{
    let lock = Arc::clone(KEYRING_OPERATION_LOCK.get_or_init(|| Arc::new(AsyncMutex::new(()))));
    let guard = lock.lock_owned().await;
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name("acn-mcp-keyring".to_string())
        .spawn(move || {
            let _guard = guard;
            let _ = sender.send(operation());
        })
        .map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })?;
    receiver.await.map_err(|_| McpOAuthError::CredentialStore {
        server: server_name.to_string(),
    })?
}

#[async_trait]
impl CredentialStore for McpCredentialStore {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        let Some(raw) = self.read().await.map_err(auth_store_error)? else {
            return Ok(None);
        };
        let stored = serde_json::from_str::<PersistedCredentials>(&raw).map_err(|_| {
            AuthError::InternalError("invalid stored OAuth credentials".to_string())
        })?;
        Ok(Some(stored.credentials))
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        let raw = serde_json::to_string(&PersistedCredentials { credentials })
            .map_err(|_| AuthError::InternalError("encode OAuth credentials failed".to_string()))?;
        self.write(raw).await.map_err(auth_store_error)?;
        if let Err(error) = self.mark_enrolled().await {
            // 没有 marker 时 runtime 不会再查凭据；避免留下无法使用也无法自动清理的 token。
            let _ = self.delete().await;
            return Err(auth_store_error(error));
        }
        Ok(())
    }

    async fn clear(&self) -> Result<(), AuthError> {
        self.delete_credentials_and_enrollment()
            .await
            .map_err(auth_store_error)
    }
}

#[derive(Clone)]
struct PrefetchedCredentialStore {
    inner: McpCredentialStore,
    prefetched: Arc<AsyncMutex<Option<StoredCredentials>>>,
}

impl PrefetchedCredentialStore {
    fn new(inner: McpCredentialStore, credentials: StoredCredentials) -> Self {
        Self {
            inner,
            prefetched: Arc::new(AsyncMutex::new(Some(credentials))),
        }
    }
}

#[async_trait]
impl CredentialStore for PrefetchedCredentialStore {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        if let Some(credentials) = self.prefetched.lock().await.take() {
            return Ok(Some(credentials));
        }
        self.inner.load().await
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        self.prefetched.lock().await.take();
        self.inner.save(credentials).await
    }

    async fn clear(&self) -> Result<(), AuthError> {
        self.prefetched.lock().await.take();
        self.inner.clear().await
    }
}

#[derive(Debug, Deserialize)]
struct OAuthCallback {
    code: Option<String>,
    state: Option<String>,
    iss: Option<String>,
    error: Option<String>,
}

#[derive(Clone)]
struct CallbackState {
    sender: Arc<Mutex<Option<oneshot::Sender<OAuthCallback>>>>,
}

/// 启动授权，等待 loopback callback 或用户粘贴 redirect URL，并保存 OAuth 凭据。
pub async fn login(
    config_path: &Path,
    server_name: &str,
    server: &McpServerConfig,
    no_browser: bool,
) -> Result<(), McpOAuthError> {
    let McpTransportConfig::StreamableHttp {
        url,
        oauth_client_id,
        oauth_callback_port,
        oauth_credentials_store,
        ..
    } = streamable_http_config(server_name, server)?
    else {
        return Err(McpOAuthError::UnsupportedTransport {
            server: server_name.to_string(),
        });
    };
    validate_oauth_resource_url(server_name, &url)?;
    let listener = if no_browser && oauth_callback_port.is_some() {
        None
    } else {
        Some(
            TcpListener::bind(("127.0.0.1", oauth_callback_port.unwrap_or(0)))
                .await
                .map_err(|_| McpOAuthError::CallbackListener {
                    server: server_name.to_string(),
                })?,
        )
    };
    let callback_port = match &listener {
        Some(listener) => listener
            .local_addr()
            .map_err(|_| McpOAuthError::CallbackListener {
                server: server_name.to_string(),
            })?
            .port(),
        None => oauth_callback_port.expect("no-browser fixed callback port was checked above"),
    };
    let callback_url = format!("http://127.0.0.1:{callback_port}{LOGIN_CALLBACK_PATH}");
    let credential_lock_path = credential_refresh_lock_path(config_path, server_name, &url);
    let credentials =
        McpCredentialStore::new(config_path, server_name, &url, oauth_credentials_store);
    let existing_granted_scopes = credentials
        .load()
        .await
        .map_err(|error| {
            credential_store_error_or(server_name, error, |_| {
                McpOAuthError::InvalidStoredCredentials {
                    server: server_name.to_string(),
                }
            })
        })?
        .map(|credentials| credentials.granted_scopes)
        .unwrap_or_default();
    let mut manager = AuthorizationManager::new(url)
        .await
        .map_err(|error| authorization_setup_error(server_name, error))?;
    manager.set_credential_store(credentials.clone());
    let session = authorization_session(
        manager,
        &callback_url,
        oauth_client_id.as_deref(),
        &existing_granted_scopes,
    )
    .await
    .map_err(|error| authorization_setup_error(server_name, error))?;
    let authorization_url = session.get_authorization_url().to_string();

    let callback_result = if no_browser {
        drop(listener);
        println!(
            "请在本地浏览器打开以下 URL 完成 MCP server '{server_name}' 的登录：\n\
             {authorization_url}\n\
             浏览器跳转到 127.0.0.1 后，请复制地址栏中的完整 URL 并粘贴到这里。"
        );
        let callback_url = read_callback_url(server_name).await?;
        let credential_guard = lock_credential_mutation(&credential_lock_path, server_name).await?;
        ensure_login_target_unchanged(config_path, server_name, server).await?;
        credentials.set_mutation_lease(credential_guard);
        let result = session.handle_callback_url(&callback_url).await;
        credentials.clear_mutation_lease();
        result
    } else {
        let listener = listener.expect("browser login always creates a callback listener");
        println!(
            "请在浏览器中完成 MCP server '{server_name}' 的登录。\n若未自动打开，请访问：\n{authorization_url}"
        );
        if let Err(error) = open_browser(&authorization_url) {
            log::debug!("打开 MCP OAuth 浏览器失败: {error}");
        }
        let callback = wait_for_callback(listener, server_name).await?;
        if callback.error.is_some() {
            return Err(McpOAuthError::AuthorizationDenied {
                server: server_name.to_string(),
            });
        }
        let code = callback
            .code
            .ok_or_else(|| McpOAuthError::MissingAuthorizationCode {
                server: server_name.to_string(),
            })?;
        let state = callback
            .state
            .ok_or_else(|| McpOAuthError::MissingAuthorizationState {
                server: server_name.to_string(),
            })?;
        let credential_guard = lock_credential_mutation(&credential_lock_path, server_name).await?;
        ensure_login_target_unchanged(config_path, server_name, server).await?;
        credentials.set_mutation_lease(credential_guard);
        let result = session
            .handle_callback_with_issuer(&code, &state, callback.iss.as_deref())
            .await;
        credentials.clear_mutation_lease();
        result
    };
    callback_result.map_err(|error| token_exchange_error(server_name, error))?;
    Ok(())
}

/// 先按 MCP resource metadata 完成 discovery，再让 rmcp 以其 scope 选择策略创建会话。
///
/// `AuthorizationSession::new` 只消费已发现的 metadata；跳过这一步会使 DCR 缺少
/// registration endpoint，也会丢失 `scopes_supported`。
async fn authorization_session(
    mut manager: AuthorizationManager,
    callback_url: &str,
    client_id: Option<&str>,
    existing_granted_scopes: &[String],
) -> Result<AuthorizationSession, AuthError> {
    let metadata = manager.resolve_metadata().await?;
    validate_authorization_metadata(&metadata.metadata, true)?;
    manager.set_metadata(metadata.metadata);
    let scopes = select_login_scopes(&manager, existing_granted_scopes);
    let mut request = AuthorizationRequest::new(callback_url)
        .with_scopes(scopes)
        .with_client_name("ACN");
    if let Some(client_id) = client_id {
        request = request.with_preregistered_client(client_id);
    }
    AuthorizationSession::new(manager, request)
        .await
        .map_err(|(_, error)| error)
}

/// 已有 grant 作为 scope seed 参与 rmcp 的既有合并策略；非空 seed 会阻止
/// authorization server 的 `scopes_supported` 被误当作 fallback 全量请求。
fn select_login_scopes(
    manager: &AuthorizationManager,
    existing_granted_scopes: &[String],
) -> Vec<String> {
    let existing_scope_seed = existing_granted_scopes.join(" ");
    manager.select_scopes(
        (!existing_scope_seed.is_empty()).then_some(existing_scope_seed.as_str()),
        &[],
    )
}

fn validate_oauth_resource_url(server_name: &str, url: &str) -> Result<(), McpOAuthError> {
    if secure_oauth_url(url) {
        Ok(())
    } else {
        Err(McpOAuthError::InsecureEndpoint {
            server: server_name.to_string(),
        })
    }
}

fn validate_authorization_metadata(
    metadata: &AuthorizationMetadata,
    require_pkce: bool,
) -> Result<(), AuthError> {
    let endpoints = [
        Some(metadata.authorization_endpoint.as_str()),
        Some(metadata.token_endpoint.as_str()),
        metadata.registration_endpoint.as_deref(),
        metadata.issuer.as_deref(),
    ];
    if endpoints
        .into_iter()
        .flatten()
        .any(|url| !secure_oauth_url(url))
    {
        return Err(AuthError::MetadataError(
            "OAuth endpoints require HTTPS or loopback HTTP".to_string(),
        ));
    }
    if require_pkce
        && !metadata
            .code_challenge_methods_supported
            .as_ref()
            .is_some_and(|methods| methods.iter().any(|method| method == "S256"))
    {
        return Err(AuthError::PkceUnsupported);
    }
    Ok(())
}

fn secure_oauth_url(value: &str) -> bool {
    let Ok(url) = reqwest_013::Url::parse(value) else {
        return false;
    };
    if url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
    {
        return false;
    }
    match url.scheme() {
        "https" => true,
        "http" => url.host_str().is_some_and(loopback_host),
        _ => false,
    }
}

fn loopback_host(host: &str) -> bool {
    let host = host.trim_end_matches('.');
    let ip_host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || ip_host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// 清除某个 Streamable HTTP MCP server 的已保存 OAuth 凭据。
pub async fn logout(
    config_path: &Path,
    server_name: &str,
    server: &McpServerConfig,
) -> Result<(), McpOAuthError> {
    let McpTransportConfig::StreamableHttp {
        url,
        oauth_credentials_store,
        ..
    } = streamable_http_config(server_name, server)?
    else {
        return Err(McpOAuthError::UnsupportedTransport {
            server: server_name.to_string(),
        });
    };
    let credential_guard = lock_credential_mutation(
        &credential_refresh_lock_path(config_path, server_name, &url),
        server_name,
    )
    .await?;
    let credentials =
        McpCredentialStore::new(config_path, server_name, &url, oauth_credentials_store);
    credentials.set_mutation_lease(credential_guard);
    let result = credentials.delete_credentials_and_enrollment().await;
    credentials.clear_mutation_lease();
    result
}

/// 写入可重试的 cleanup 记录并持有 credential mutation lock，供 `mcp remove` 删除配置。
///
/// 凭据本身由 [`McpCredentialRemovalLease::finish`] 在配置落盘后清理；并发 login 在整段
/// 操作结束前拿不到锁，之后也会因配置已不存在而拒绝写入。
pub async fn prepare_credentials_for_remove(
    config_path: &Path,
    server_name: &str,
    server: &McpServerConfig,
) -> Result<McpCredentialRemovalLease, McpOAuthError> {
    let McpTransportConfig::StreamableHttp {
        url,
        oauth_credentials_store,
        ..
    } = streamable_http_config(server_name, server)?
    else {
        return Err(McpOAuthError::UnsupportedTransport {
            server: server_name.to_string(),
        });
    };
    let credential_lock_path = credential_refresh_lock_path(config_path, server_name, &url);
    let credential_guard = lock_credential_mutation(&credential_lock_path, server_name).await?;
    let credentials =
        McpCredentialStore::new(config_path, server_name, &url, oauth_credentials_store);
    credentials.set_mutation_lease(credential_guard);
    if !server.has_explicit_oauth_options() && !credentials.is_enrolled().await? {
        return Ok(McpCredentialRemovalLease {
            credentials,
            cleanup_path: None,
        });
    }
    let cleanup_path = pending_cleanup_path(config_path, server_name);
    let cleanup = PendingCredentialCleanup {
        account: credentials.account.clone(),
        store: oauth_credentials_store,
    };
    if let Err(error) = write_pending_cleanup(
        &cleanup_path,
        cleanup,
        server_name,
        credentials.mutation_lease(),
    )
    .await
    {
        credentials.clear_mutation_lease();
        return Err(error);
    }
    Ok(McpCredentialRemovalLease {
        credentials,
        cleanup_path: Some(cleanup_path),
    })
}

impl McpCredentialRemovalLease {
    /// 清理凭据；失败时保留 pending record，让无配置的 `mcp logout <name>` 仍可重试。
    pub async fn finish(self) -> Result<(), McpOAuthError> {
        let Some(cleanup_path) = &self.cleanup_path else {
            return Ok(());
        };
        self.credentials.delete_credentials_and_enrollment().await?;
        remove_pending_cleanup(cleanup_path, &self.credentials.server_name).await
    }

    /// 配置删除未落盘时撤销 cleanup record，并保留原凭据。
    pub async fn cancel(self) -> Result<(), McpOAuthError> {
        let Some(cleanup_path) = &self.cleanup_path else {
            return Ok(());
        };
        remove_pending_cleanup(cleanup_path, &self.credentials.server_name).await
    }
}

/// 配置已由 `mcp remove` 删除时，按 pending cleanup record 重试凭据清理。
pub async fn retry_pending_logout(
    config_path: &Path,
    server_name: &str,
) -> Result<bool, McpOAuthError> {
    let cleanup_path = pending_cleanup_path(config_path, server_name);
    let Some(cleanup) = read_pending_cleanup(&cleanup_path, server_name).await? else {
        return Ok(false);
    };
    let lock_path = credential_lock_path_for_account(config_path, &cleanup.account);
    let credential_guard = lock_credential_mutation(&lock_path, server_name).await?;
    let credentials = McpCredentialStore::from_pending_cleanup(config_path, server_name, cleanup);
    credentials.set_mutation_lease(credential_guard);
    let result = async {
        credentials.delete_credentials_and_enrollment().await?;
        remove_pending_cleanup(&cleanup_path, server_name).await
    }
    .await;
    credentials.clear_mutation_lease();
    result.map(|()| true)
}

pub async fn has_pending_cleanup(
    config_path: &Path,
    server_name: &str,
) -> Result<bool, McpOAuthError> {
    read_pending_cleanup(&pending_cleanup_path(config_path, server_name), server_name)
        .await
        .map(|cleanup| cleanup.is_some())
}

async fn ensure_login_target_unchanged(
    config_path: &Path,
    server_name: &str,
    expected: &McpServerConfig,
) -> Result<(), McpOAuthError> {
    let current = read_mcp_json_config(config_path).await.map_err(|_| {
        McpOAuthError::ConfigurationChanged {
            server: server_name.to_string(),
        }
    })?;
    if current.servers.get(server_name) != Some(expected) {
        return Err(McpOAuthError::ConfigurationChanged {
            server: server_name.to_string(),
        });
    }
    Ok(())
}

/// 返回已登录 server 的 `AuthorizationManager`；没有已保存凭据时返回 `None`。
///
/// 交给 ACN OAuth HTTP client 持有，让每次请求按需取 token 并在过期时用 refresh token 续期；
/// 只在建连时取一次 access token 会让长会话在 token 过期后开始 401。
///
/// 从未登录且没有显式 OAuth 配置时不访问凭据存储，也不做 metadata discovery。
/// 已登记或显式配置 OAuth 后，凭据存储不可用时必须失败，不能在无法确认既有登录
/// 身份的情况下静默改用匿名连接。
pub(crate) async fn authorization_manager(
    config_path: &Path,
    server_name: &str,
    url: &str,
    store: McpOAuthCredentialsStore,
    oauth_explicitly_configured: bool,
) -> Result<Option<McpRuntimeAuthorization>, McpOAuthError> {
    let credentials = McpCredentialStore::new(config_path, server_name, url, store);
    if !oauth_explicitly_configured && !credentials.is_enrolled().await? {
        return Ok(None);
    }
    let credential_guard = lock_credential_mutation(
        &credential_refresh_lock_path(config_path, server_name, url),
        server_name,
    )
    .await?;
    credentials.set_mutation_lease(credential_guard);
    let result = async {
        if !oauth_explicitly_configured && !credentials.is_enrolled().await? {
            return Ok(None);
        }
        authorization_manager_locked(server_name, url, credentials.clone()).await
    }
    .await;
    credentials.clear_mutation_lease();
    result
}

/// 调用方必须持有该 server 的 credential mutation lock，确保 discovery、载入 client
/// identity 与后续 refresh 看到同一份 login/logout 结果。
pub(crate) async fn authorization_manager_locked(
    server_name: &str,
    url: &str,
    credentials: McpCredentialStore,
) -> Result<Option<McpRuntimeAuthorization>, McpOAuthError> {
    let stored = credentials.load().await.map_err(|error| {
        credential_store_error_or(server_name, error, |_| {
            McpOAuthError::InvalidStoredCredentials {
                server: server_name.to_string(),
            }
        })
    })?;
    let Some(stored) = stored else {
        credentials.clear_enrollment().await?;
        return Ok(None);
    };
    validate_oauth_resource_url(server_name, url)?;
    let mut manager = AuthorizationManager::new(url)
        .await
        .map_err(|error| authorization_setup_error(server_name, error))?;
    let metadata = manager
        .resolve_metadata()
        .await
        .map_err(|error| authorization_setup_error(server_name, error))?;
    validate_authorization_metadata(&metadata.metadata, false)
        .map_err(|error| authorization_setup_error(server_name, error))?;
    manager.set_metadata(metadata.metadata);
    manager.set_credential_store(PrefetchedCredentialStore::new(
        credentials.clone(),
        stored.clone(),
    ));
    let loaded = match manager.initialize_from_store().await {
        Ok(loaded) => loaded,
        Err(error) => {
            let error = credential_store_error_or(server_name, error, |_| {
                McpOAuthError::InvalidStoredCredentials {
                    server: server_name.to_string(),
                }
            });
            return Err(error);
        }
    };
    if !loaded {
        credentials.clear_enrollment().await?;
        return Ok(None);
    }
    credentials.mark_enrolled().await?;
    let identity = McpCredentialIdentity {
        client_id: stored.client_id,
        issuer: stored.issuer,
    };
    Ok(Some(McpRuntimeAuthorization {
        manager,
        credentials,
        identity,
        server_name: server_name.to_string(),
        resource_url: url.to_string(),
    }))
}

fn streamable_http_config(
    server_name: &str,
    server: &McpServerConfig,
) -> Result<McpTransportConfig, McpOAuthError> {
    match server.transport_config(server_name) {
        Ok(config @ McpTransportConfig::StreamableHttp { .. }) => Ok(config),
        Ok(McpTransportConfig::Stdio { .. }) => Err(McpOAuthError::UnsupportedTransport {
            server: server_name.to_string(),
        }),
        Err(_) => Err(McpOAuthError::InvalidConfig {
            server: server_name.to_string(),
        }),
    }
}

async fn read_callback_url(server_name: &str) -> Result<String, McpOAuthError> {
    let server = server_name.to_string();
    wait_for_callback_input(server_name, LOGIN_TIMEOUT, move || {
        print!("redirect URL> ");
        io::stdout()
            .flush()
            .map_err(|_| McpOAuthError::CallbackInput {
                server: server.clone(),
            })?;
        let mut value = String::new();
        io::stdin()
            .read_line(&mut value)
            .map_err(|_| McpOAuthError::CallbackInput {
                server: server.clone(),
            })?;
        let value = value.trim().to_string();
        if value.is_empty() {
            return Err(McpOAuthError::CallbackInput { server });
        }
        Ok(value)
    })
    .await
}

async fn wait_for_callback_input<F>(
    server_name: &str,
    timeout: Duration,
    read: F,
) -> Result<String, McpOAuthError>
where
    F: FnOnce() -> Result<String, McpOAuthError> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    // Tokio 无法取消已经开始的 `spawn_blocking`，runtime 析构还会等待它；独立线程在
    // 这个单用途 CLI 超时退出时由进程收束，不会让五分钟 timeout 变成无限等待。
    std::thread::Builder::new()
        .name("acn-mcp-oauth-stdin".to_string())
        .spawn(move || {
            let _ = sender.send(read());
        })
        .map_err(|_| McpOAuthError::CallbackInput {
            server: server_name.to_string(),
        })?;
    match time::timeout(timeout, receiver).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(McpOAuthError::CallbackInput {
            server: server_name.to_string(),
        }),
        Err(_) => Err(McpOAuthError::LoginTimeout {
            server: server_name.to_string(),
        }),
    }
}

fn credential_account(config_path: &Path, server_name: &str, url: &str) -> String {
    let upstream_scope = config_path
        .parent()
        .unwrap_or(config_path)
        .to_string_lossy();
    let identity = format!(
        "{}:{upstream_scope}{}:{server_name}{}:{url}",
        upstream_scope.len(),
        server_name.len(),
        url.len()
    );
    format!("mcp-oauth-{}", sha256_hex(&identity))
}

fn credential_file_path(config_path: &Path, account: &str) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(config_path)
        .join(FILE_CREDENTIALS_DIR)
        .join(format!("{account}.json"))
}

fn credential_enrollment_path(config_path: &Path, account: &str) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(config_path)
        .join(FILE_CREDENTIALS_DIR)
        .join(format!("{account}.enrolled"))
}

fn pending_cleanup_path(config_path: &Path, server_name: &str) -> PathBuf {
    let upstream_scope = config_path
        .parent()
        .unwrap_or(config_path)
        .to_string_lossy();
    let identity = format!(
        "{}:{upstream_scope}{}:{server_name}",
        upstream_scope.len(),
        server_name.len()
    );
    config_path
        .parent()
        .unwrap_or(config_path)
        .join(PENDING_CLEANUP_DIR)
        .join(format!("{}.json", sha256_hex(&identity)))
}

pub(crate) fn credential_refresh_lock_path(
    config_path: &Path,
    server_name: &str,
    url: &str,
) -> PathBuf {
    credential_lock_path_for_account(
        config_path,
        &credential_account(config_path, server_name, url),
    )
}

fn credential_lock_path_for_account(config_path: &Path, account: &str) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(config_path)
        .join(CREDENTIAL_LOCKS_DIR)
        .join(format!("{account}.lock"))
}

async fn lock_credential_mutation(
    path: &Path,
    server_name: &str,
) -> Result<FileLockGuard, McpOAuthError> {
    FileLockGuard::lock_exclusive_timeout(path, LOGIN_TIMEOUT)
        .await
        .map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })
}

async fn write_pending_cleanup(
    path: &Path,
    cleanup: PendingCredentialCleanup,
    server_name: &str,
    mutation_lease: Option<Arc<FileLockGuard>>,
) -> Result<(), McpOAuthError> {
    let encoded = serde_json::to_string(&cleanup).map_err(|_| McpOAuthError::CredentialStore {
        server: server_name.to_string(),
    })?;
    let path = path.to_path_buf();
    let server = server_name.to_string();
    tokio::task::spawn_blocking(move || {
        let _mutation_lease = mutation_lease;
        write_private_credentials_file(&path, &encoded, &server)
    })
    .await
    .map_err(|_| McpOAuthError::CredentialStore {
        server: server_name.to_string(),
    })?
}

async fn read_pending_cleanup(
    path: &Path,
    server_name: &str,
) -> Result<Option<PendingCredentialCleanup>, McpOAuthError> {
    let raw = match tokio::fs::read_to_string(path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(McpOAuthError::CredentialStore {
                server: server_name.to_string(),
            });
        }
    };
    let cleanup = serde_json::from_str::<PendingCredentialCleanup>(&raw).map_err(|_| {
        McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        }
    })?;
    if !valid_credential_account(&cleanup.account) {
        return Err(McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        });
    }
    Ok(Some(cleanup))
}

fn valid_credential_account(account: &str) -> bool {
    account.strip_prefix("mcp-oauth-").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

async fn remove_pending_cleanup(path: &Path, server_name: &str) -> Result<(), McpOAuthError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        }),
    }
}

fn write_private_credentials_file(
    path: &Path,
    value: &str,
    server_name: &str,
) -> Result<(), McpOAuthError> {
    let Some(parent) = path.parent() else {
        return Err(McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        });
    };
    std::fs::create_dir_all(parent).map_err(|_| McpOAuthError::CredentialStore {
        server: server_name.to_string(),
    })?;
    set_private_directory_permissions(parent, server_name)?;

    let mut file =
        tempfile::NamedTempFile::new_in(parent).map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })?;
    file.write_all(value.as_bytes())
        .and_then(|_| file.flush())
        .and_then(|_| file.as_file().sync_all())
        .map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })?;
    file.persist(path)
        .map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })?;
    set_private_file_permissions_blocking(path).map_err(|_| McpOAuthError::CredentialStore {
        server: server_name.to_string(),
    })
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path, server_name: &str) -> Result<(), McpOAuthError> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(|_| {
        McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        }
    })
}

#[cfg(not(unix))]
fn set_private_directory_permissions(
    _path: &Path,
    _server_name: &str,
) -> Result<(), McpOAuthError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions_blocking(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file_permissions_blocking(_path: &Path) -> io::Result<()> {
    Ok(())
}

async fn wait_for_callback(
    listener: TcpListener,
    server_name: &str,
) -> Result<OAuthCallback, McpOAuthError> {
    let (sender, receiver) = oneshot::channel();
    let callback_state = CallbackState {
        sender: Arc::new(Mutex::new(Some(sender))),
    };
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(LOGIN_CALLBACK_PATH, get(capture_callback))
                .with_state(callback_state),
        )
        .with_graceful_shutdown(server_shutdown.cancelled_owned())
        .await
    });

    let callback = match time::timeout(LOGIN_TIMEOUT, receiver).await {
        Ok(Ok(callback)) => Ok(callback),
        Ok(Err(_)) => Err(McpOAuthError::CallbackServer {
            server: server_name.to_string(),
        }),
        Err(_) => Err(McpOAuthError::LoginTimeout {
            server: server_name.to_string(),
        }),
    };
    shutdown.cancel();
    let _ = server.await;
    callback
}

async fn capture_callback(
    State(state): State<CallbackState>,
    Query(callback): Query<OAuthCallback>,
) -> Html<&'static str> {
    let sender = match state.sender.lock() {
        Ok(mut sender) => sender.take(),
        Err(_) => None,
    };
    if let Some(sender) = sender {
        let _ = sender.send(callback);
        Html("<h1>登录已完成</h1><p>可以关闭此页面并返回终端。</p>")
    } else {
        Html("<h1>登录回调已处理</h1><p>可以关闭此页面并返回终端。</p>")
    }
}

fn open_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = std::process::Command::new("open");
    #[cfg(target_os = "linux")]
    let mut command = std::process::Command::new("xdg-open");
    #[cfg(target_os = "windows")]
    let mut command = windows_browser_command(url);
    #[cfg(not(target_os = "windows"))]
    {
        command.arg(url).spawn().map(|_| ())
    }

    #[cfg(target_os = "windows")]
    {
        command.spawn().map(|_| ())
    }
}

#[cfg(any(test, target_os = "windows"))]
fn windows_browser_command(url: &str) -> std::process::Command {
    // 直接调用 URL handler，避免 OAuth URL 中的 `&` 被 cmd.exe 当作命令分隔符。
    let mut command = std::process::Command::new("rundll32.exe");
    command.args(["url.dll,FileProtocolHandler", url]);
    command
}

/// `McpCredentialStore` 只会因凭据库访问失败而出错，统一编码成固定 message。
fn auth_store_error(_: McpOAuthError) -> AuthError {
    AuthError::InternalError(CREDENTIAL_STORE_UNAVAILABLE.to_string())
}

/// 还原 `auth_store_error` 编码的凭据库故障，其余错误使用调用点给出的语义。
fn credential_store_error_or<F>(server_name: &str, error: AuthError, fallback: F) -> McpOAuthError
where
    F: FnOnce(AuthError) -> McpOAuthError,
{
    match error {
        AuthError::InternalError(message) if message == CREDENTIAL_STORE_UNAVAILABLE => {
            McpOAuthError::CredentialStore {
                server: server_name.to_string(),
            }
        }
        error => fallback(error),
    }
}

fn authorization_setup_error(server_name: &str, error: AuthError) -> McpOAuthError {
    credential_store_error_or(server_name, error, |error| {
        McpOAuthError::AuthorizationSetup {
            server: server_name.to_string(),
            reason: auth_error_reason(&error),
        }
    })
}

fn token_exchange_error(server_name: &str, error: AuthError) -> McpOAuthError {
    credential_store_error_or(server_name, error, |error| McpOAuthError::TokenExchange {
        server: server_name.to_string(),
        reason: auth_error_reason(&error),
    })
}

fn auth_error_reason(error: &AuthError) -> &'static str {
    match error {
        AuthError::AuthorizationRequired => "authorization server 要求重新授权",
        AuthError::AuthorizationFailed(message)
            if message == "Authorization callback missing code" =>
        {
            "回调缺少 authorization code"
        }
        AuthError::AuthorizationFailed(message)
            if message == "Authorization callback missing state" =>
        {
            "回调缺少 state"
        }
        AuthError::AuthorizationFailed(_) => "授权回调校验失败",
        AuthError::TokenExchangeFailed(_) => "token endpoint 拒绝授权码或返回了无效响应",
        AuthError::HttpError(_) => "OAuth endpoint HTTP 请求失败",
        AuthError::OAuthError(_) => "authorization server 返回 OAuth 错误",
        AuthError::MetadataError(_) => "OAuth metadata 无效或不完整",
        AuthError::PkceUnsupported => "authorization server 不支持 PKCE S256",
        AuthError::UrlError(_) => "OAuth endpoint URL 无效",
        AuthError::NoAuthorizationSupport => "server 未发布可用的 OAuth metadata",
        AuthError::InternalError(message) if message == "Authorization state not found" => {
            "回调 state 无效或已失效，请重新执行 login"
        }
        AuthError::InternalError(_) => "OAuth session 内部状态无效",
        AuthError::RegistrationFailed(_) => "动态客户端注册被拒绝或响应无效",
        AuthError::AuthorizationServerMismatch { .. } => "回调 issuer 不匹配 discovery metadata",
        AuthError::AuthorizationServerMissingIssuer { .. } => {
            "authorization server 声明支持 issuer，但回调缺少 iss"
        }
        _ => "OAuth 协议处理失败",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::client::{McpClient, McpClientError, McpConnectReleaseFence};
    use crate::mcp::config::McpOAuthCredentialsStore;
    use axum::{
        body::Bytes,
        extract::State,
        http::{header, StatusCode},
        routing::post,
        Json,
    };
    use oauth2::TokenResponse;
    use serde_json::{json, Value};
    use std::{collections::BTreeMap, future::IntoFuture};

    #[derive(Clone)]
    struct OAuthDiscoveryState {
        authorization_server: String,
        registration: Arc<Mutex<Option<Value>>>,
        token_requests: Arc<Mutex<Vec<String>>>,
    }

    async fn oauth_test_mcp_endpoint() -> (StatusCode, [(header::HeaderName, &'static str); 1]) {
        (
            StatusCode::UNAUTHORIZED,
            [(
                header::WWW_AUTHENTICATE,
                r#"Bearer resource_metadata="/.well-known/oauth-protected-resource", scope="challenge:read""#,
            )],
        )
    }

    async fn oauth_test_resource_metadata(State(state): State<OAuthDiscoveryState>) -> Json<Value> {
        Json(json!({
            "resource": format!("{}/mcp", state.authorization_server),
            "authorization_servers": [state.authorization_server],
            "scopes_supported": ["files:read"],
        }))
    }

    async fn oauth_test_authorization_metadata(
        State(state): State<OAuthDiscoveryState>,
    ) -> Json<Value> {
        Json(json!({
            "issuer": format!("{}/", state.authorization_server),
            "authorization_endpoint": format!("{}/authorize", state.authorization_server),
            "token_endpoint": format!("{}/token", state.authorization_server),
            "registration_endpoint": format!("{}/register", state.authorization_server),
            "response_types_supported": ["code"],
            "code_challenge_methods_supported": ["S256"],
            "authorization_response_iss_parameter_supported": true,
        }))
    }

    async fn oauth_test_register(
        State(state): State<OAuthDiscoveryState>,
        Json(registration): Json<Value>,
    ) -> Json<Value> {
        *state.registration.lock().unwrap() = Some(registration);
        Json(json!({
            "client_id": "acn-test-client",
            "client_secret": null,
            "client_name": "ACN",
            "redirect_uris": ["http://127.0.0.1:9876/callback"],
        }))
    }

    async fn oauth_test_token(
        State(state): State<OAuthDiscoveryState>,
        body: Bytes,
    ) -> Json<Value> {
        state
            .token_requests
            .lock()
            .unwrap()
            .push(String::from_utf8(body.to_vec()).unwrap());
        Json(json!({
            "access_token": "access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "refresh-token",
        }))
    }

    #[test]
    fn credential_account_isolates_upstream_server_and_url_without_exposing_them() {
        let config_path = Path::new("/srv/acn/upstream-a/.mcp.json");
        let url = "https://user:password@example.test/mcp?token=secret";
        let account = credential_account(config_path, "docs", url);

        assert!(account.starts_with("mcp-oauth-"));
        assert_eq!(account, credential_account(config_path, "docs", url));
        assert_ne!(
            account,
            credential_account(Path::new("/srv/acn/upstream-b/.mcp.json"), "docs", url)
        );
        assert_ne!(account, credential_account(config_path, "issues", url));
        assert_ne!(
            account,
            credential_account(config_path, "docs", "https://example.test/mcp")
        );
        assert!(!account.contains("example"));
        assert!(!account.contains("secret"));
    }

    #[test]
    fn oauth_urls_require_https_except_for_loopback_development() {
        for url in [
            "https://auth.example.test/token",
            "http://127.0.0.1:8080/token",
            "http://[::1]:8080/token",
            "http://login.localhost/token",
        ] {
            assert!(secure_oauth_url(url), "应允许 {url}");
        }
        for url in [
            "http://auth.example.test/token",
            "ftp://auth.example.test/token",
            "https://user:password@auth.example.test/token",
            "https://auth.example.test/token#fragment",
            "not-a-url",
        ] {
            assert!(!secure_oauth_url(url), "应拒绝 {url}");
        }
    }

    #[test]
    fn login_requires_authorization_server_to_advertise_pkce_s256() {
        let mut metadata = AuthorizationMetadata::default();
        metadata.authorization_endpoint = "https://auth.example.test/authorize".to_string();
        metadata.token_endpoint = "https://auth.example.test/token".to_string();
        metadata.issuer = Some("https://auth.example.test".to_string());

        assert!(matches!(
            validate_authorization_metadata(&metadata, true),
            Err(AuthError::PkceUnsupported)
        ));
        metadata.code_challenge_methods_supported = Some(vec!["S256".to_string()]);
        validate_authorization_metadata(&metadata, true).unwrap();

        metadata.token_endpoint = "http://auth.example.test/token".to_string();
        assert!(matches!(
            validate_authorization_metadata(&metadata, false),
            Err(AuthError::MetadataError(_))
        ));
    }

    #[tokio::test]
    async fn runtime_oauth_metadata_discovery_honors_server_startup_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(|| async {
                    std::future::pending::<axum::response::Response>().await
                }),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join(".mcp.json");
        let url = format!("http://{addr}/mcp");
        let store =
            McpCredentialStore::new(&config_path, "remote", &url, McpOAuthCredentialsStore::File);
        store
            .save(
                serde_json::from_value(json!({
                    "client_id": "test-client",
                    "token_response": {
                        "access_token": "access-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "refresh_token": "refresh-token"
                    }
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let mut config = McpServerConfig::streamable_http(url, None);
        config.startup_timeout_secs = Some(1);
        config.oauth_credentials_store = Some(McpOAuthCredentialsStore::File);

        let result = time::timeout(
            Duration::from_secs(2),
            McpClient::connect(
                "remote".to_string(),
                &config,
                &config_path,
                dir.path(),
                None,
                McpConnectReleaseFence::new(),
                crate::mcp::client::McpOAuthRefreshSupervisor::default(),
            ),
        )
        .await
        .expect("OAuth metadata discovery 应由 startup_timeout_secs 收束");

        assert!(matches!(result, Err(McpClientError::StartupTimeout { .. })));
        server_task.abort();
    }

    #[test]
    fn only_streamable_http_servers_can_log_in() {
        let server = McpServerConfig::stdio(
            "server".to_string(),
            Vec::new(),
            Default::default(),
            Vec::new(),
        );

        assert!(matches!(
            streamable_http_config("local", &server),
            Err(McpOAuthError::UnsupportedTransport { .. })
        ));
    }

    #[test]
    fn windows_browser_launcher_does_not_use_command_interpreter() {
        let url = "https://auth.example.test/authorize?client_id=client&state=state";
        let command = windows_browser_command(url);

        assert_eq!(command.get_program(), std::ffi::OsStr::new("rundll32.exe"));
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![
                std::ffi::OsStr::new("url.dll,FileProtocolHandler"),
                std::ffi::OsStr::new(url),
            ]
        );
    }

    #[test]
    fn oauth_auth_errors_keep_actionable_issuer_state_and_setup_reasons() {
        let issuer = token_exchange_error(
            "docs",
            AuthError::AuthorizationServerMismatch {
                expected_issuer: "https://auth.example.test".to_string(),
                received_issuer: "https://evil.example.test".to_string(),
            },
        )
        .to_string();
        let state = token_exchange_error(
            "docs",
            AuthError::InternalError("Authorization state not found".to_string()),
        )
        .to_string();
        let setup = authorization_setup_error("docs", AuthError::PkceUnsupported).to_string();

        assert!(issuer.contains("issuer 不匹配"));
        assert!(!issuer.contains("evil.example.test"));
        assert!(state.contains("state 无效或已失效"));
        assert!(setup.contains("PKCE S256"));
    }

    /// ACN OAuth HTTP client 每次请求都从凭据库重新加载并按需刷新，client_id 和 refresh_token
    /// 必须能在持久化格式里往返，否则 token 过期后无法续期，只能重新登录。
    #[test]
    fn persisted_credentials_round_trip_keeps_client_id_and_refresh_token() {
        let credentials = serde_json::from_str::<StoredCredentials>(
            r#"{
                "client_id": "registered-client",
                "token_response": {
                    "access_token": "access",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "refresh_token": "refresh"
                }
            }"#,
        )
        .unwrap();
        let encoded = serde_json::to_string(&PersistedCredentials { credentials }).unwrap();
        let decoded = serde_json::from_str::<PersistedCredentials>(&encoded).unwrap();

        assert_eq!(decoded.credentials.client_id, "registered-client");
        let token = decoded.credentials.token_response.as_ref().unwrap();
        assert_eq!(token.access_token().secret(), "access");
        assert_eq!(
            token.refresh_token().map(|token| token.secret().as_str()),
            Some("refresh")
        );
    }

    #[test]
    fn persisted_credentials_keep_rmcp_token_received_at() {
        let credentials = serde_json::from_str::<StoredCredentials>(
            r#"{
                "client_id": "client",
                "token_response": {
                    "access_token": "access",
                    "token_type": "Bearer",
                    "expires_in": 10,
                    "refresh_token": "refresh"
                },
                "token_received_at": 123456
            }"#,
        )
        .unwrap();
        let encoded = serde_json::to_string(&PersistedCredentials { credentials }).unwrap();
        let decoded = serde_json::from_str::<PersistedCredentials>(&encoded).unwrap();

        assert_eq!(decoded.credentials.token_received_at, Some(123456));
        assert!(!encoded.contains("saved_at"));
    }

    #[tokio::test]
    async fn existing_grant_prevents_authorization_metadata_scope_fallback() {
        let mut manager = AuthorizationManager::new("https://resource.example.test/mcp")
            .await
            .unwrap();
        let mut metadata = rmcp::transport::auth::AuthorizationMetadata::default();
        metadata.scopes_supported = Some(vec![
            "metadata:read".to_string(),
            "metadata:write".to_string(),
            "offline_access".to_string(),
        ]);
        manager.set_metadata(metadata);

        let scopes = select_login_scopes(&manager, &["existing:grant".to_string()]);

        assert_eq!(scopes, vec!["existing:grant", "offline_access"]);
    }

    #[test]
    fn callback_input_timeout_does_not_delay_runtime_shutdown() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let started = std::time::Instant::now();
        let result = runtime.block_on(wait_for_callback_input(
            "docs",
            Duration::from_millis(20),
            || {
                std::thread::sleep(Duration::from_secs(2));
                Ok("http://127.0.0.1:9876/callback?code=late".to_string())
            },
        ));
        drop(runtime);

        assert!(matches!(result, Err(McpOAuthError::LoginTimeout { .. })));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "runtime 析构不应等待已超时的 stdin reader"
        );
    }

    #[test]
    fn timed_out_keyring_operation_does_not_block_runtime_or_start_duplicate_access() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Condvar, Mutex as StdMutex};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((StdMutex::new(false), Condvar::new()));
        let first_calls = Arc::clone(&calls);
        let first_gate = Arc::clone(&gate);

        let first = runtime.block_on(async {
            time::timeout(
                Duration::from_millis(20),
                run_keyring_operation("docs", move || {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    let (lock, ready) = &*first_gate;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = ready.wait(released).unwrap();
                    }
                    Ok(())
                }),
            )
            .await
        });
        assert!(first.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let retry_calls = Arc::clone(&calls);
        let retry = runtime.block_on(async {
            time::timeout(
                Duration::from_millis(20),
                run_keyring_operation("docs", move || {
                    retry_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .await
        });
        assert!(retry.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "前一次系统访问未完成时不得启动重复 keyring 调用"
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_one();
        runtime.block_on(async {
            time::timeout(Duration::from_secs(1), async {
                while KEYRING_OPERATION_LOCK
                    .get()
                    .expect("keyring lock initialized")
                    .try_lock()
                    .is_err()
                {
                    time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        });

        let started = std::time::Instant::now();
        drop(runtime);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "runtime 析构不应等待已超时的 keyring 系统调用"
        );
    }

    #[tokio::test]
    async fn file_credentials_store_round_trips_with_private_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("upstream-a").join(".mcp.json");
        let store = McpCredentialStore::new(
            &config_path,
            "docs",
            "https://example.test/mcp",
            McpOAuthCredentialsStore::File,
        );
        let credentials = serde_json::from_str::<StoredCredentials>(
            r#"{"client_id":"client","token_received_at":123456}"#,
        )
        .unwrap();

        store.save(credentials).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();

        assert_eq!(loaded.client_id, "client");
        assert_eq!(loaded.token_received_at, Some(123456));
        let CredentialBackend::File { path } = &store.backend else {
            panic!("expected file credential store");
        };
        assert!(path.is_absolute());
        assert!(store.enrollment_path.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }

        store.clear().await.unwrap();
        assert!(!path.exists());
        assert!(!store.enrollment_path.exists());
    }

    #[tokio::test]
    async fn anonymous_http_server_does_not_touch_unavailable_credentials_store() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join(".mcp.json");
        let store = McpCredentialStore::new(
            &config_path,
            "docs",
            "https://example.test/mcp",
            McpOAuthCredentialsStore::File,
        );
        let CredentialBackend::File { path } = &store.backend else {
            panic!("expected file credential store");
        };
        tokio::fs::create_dir_all(path).await.unwrap();

        let authorization = authorization_manager(
            &config_path,
            "docs",
            "https://example.test/mcp",
            McpOAuthCredentialsStore::File,
            false,
        )
        .await
        .unwrap();

        assert!(authorization.is_none());
        assert!(!dir.path().join(CREDENTIAL_LOCKS_DIR).exists());
    }

    #[tokio::test]
    async fn anonymous_remove_keeps_lock_without_creating_oauth_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join(".mcp.json");
        let server = McpServerConfig::streamable_http("https://example.test/mcp".to_string(), None);
        let store = McpCredentialStore::new(
            &config_path,
            "docs",
            "https://example.test/mcp",
            McpOAuthCredentialsStore::File,
        );
        let CredentialBackend::File { path } = &store.backend else {
            panic!("expected file credential store");
        };
        tokio::fs::create_dir_all(path).await.unwrap();

        let lease = prepare_credentials_for_remove(&config_path, "docs", &server)
            .await
            .unwrap();

        assert!(!has_pending_cleanup(&config_path, "docs").await.unwrap());
        lease.finish().await.unwrap();
    }

    #[tokio::test]
    async fn enrolled_oauth_rejects_plaintext_remote_resource_url_before_discovery() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join(".mcp.json");
        let url = "http://resource.example.test/mcp";
        let store =
            McpCredentialStore::new(&config_path, "docs", url, McpOAuthCredentialsStore::File);
        store
            .save(
                serde_json::from_value(json!({
                    "client_id": "client",
                    "token_response": {
                        "access_token": "access-token",
                        "token_type": "Bearer"
                    }
                }))
                .unwrap(),
            )
            .await
            .unwrap();

        let result = authorization_manager(
            &config_path,
            "docs",
            url,
            McpOAuthCredentialsStore::File,
            false,
        )
        .await;

        assert!(matches!(
            result,
            Err(McpOAuthError::InsecureEndpoint { .. })
        ));
    }

    #[tokio::test]
    async fn logout_waits_for_in_flight_refresh_before_deleting_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("upstream-a").join(".mcp.json");
        let url = "https://example.test/mcp".to_string();
        let store =
            McpCredentialStore::new(&config_path, "docs", &url, McpOAuthCredentialsStore::File);
        store
            .save(
                serde_json::from_value(json!({
                    "client_id": "client",
                    "token_response": {
                        "access_token": "old-access",
                        "token_type": "Bearer",
                        "refresh_token": "old-refresh"
                    }
                }))
                .unwrap(),
            )
            .await
            .unwrap();

        let refresh_guard = lock_credential_mutation(
            &credential_refresh_lock_path(&config_path, "docs", &url),
            "docs",
        )
        .await
        .unwrap();
        let mut server = McpServerConfig::streamable_http(url.clone(), None);
        server.oauth_credentials_store = Some(McpOAuthCredentialsStore::File);
        let logout_config_path = config_path.clone();
        let logout_task =
            tokio::spawn(async move { logout(&logout_config_path, "docs", &server).await });

        time::sleep(Duration::from_millis(20)).await;
        assert!(
            !logout_task.is_finished(),
            "logout 必须等待正在持锁保存凭据的 refresh"
        );
        store
            .save(
                serde_json::from_value(json!({
                    "client_id": "client",
                    "token_response": {
                        "access_token": "rotated-access",
                        "token_type": "Bearer",
                        "refresh_token": "rotated-refresh"
                    }
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        drop(refresh_guard);

        time::timeout(Duration::from_secs(1), logout_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(store.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pending_login_rejects_a_removed_or_replaced_server() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join(".mcp.json");
        let server =
            McpServerConfig::streamable_http("https://resource.example.test/mcp".to_string(), None);
        let mut config = crate::mcp::config::McpJsonConfig::default();
        config.servers.insert("docs".to_string(), server.clone());
        crate::mcp::config::write_mcp_json_config_atomic(&config_path, &config)
            .await
            .unwrap();
        ensure_login_target_unchanged(&config_path, "docs", &server)
            .await
            .unwrap();

        config.servers.remove("docs");
        crate::mcp::config::write_mcp_json_config_atomic(&config_path, &config)
            .await
            .unwrap();
        assert!(matches!(
            ensure_login_target_unchanged(&config_path, "docs", &server).await,
            Err(McpOAuthError::ConfigurationChanged { .. })
        ));

        let replacement = McpServerConfig::streamable_http(
            "https://replacement.example.test/mcp".to_string(),
            None,
        );
        config.servers.insert("docs".to_string(), replacement);
        crate::mcp::config::write_mcp_json_config_atomic(&config_path, &config)
            .await
            .unwrap();
        assert!(matches!(
            ensure_login_target_unchanged(&config_path, "docs", &server).await,
            Err(McpOAuthError::ConfigurationChanged { .. })
        ));
    }

    #[tokio::test]
    async fn runtime_initialization_waits_for_concurrent_login_before_loading_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let authorization_server = format!("http://{}", listener.local_addr().unwrap());
        let state = OAuthDiscoveryState {
            authorization_server: authorization_server.clone(),
            registration: Arc::new(Mutex::new(None)),
            token_requests: Arc::new(Mutex::new(Vec::new())),
        };
        let server_task = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/mcp", get(oauth_test_mcp_endpoint))
                    .route(
                        "/.well-known/oauth-protected-resource",
                        get(oauth_test_resource_metadata),
                    )
                    .route(
                        "/.well-known/oauth-authorization-server",
                        get(oauth_test_authorization_metadata),
                    )
                    .with_state(state),
            )
            .into_future(),
        );
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("upstream-a").join(".mcp.json");
        let url = format!("{authorization_server}/mcp");
        let store =
            McpCredentialStore::new(&config_path, "docs", &url, McpOAuthCredentialsStore::File);
        store
            .save(
                serde_json::from_value(json!({
                    "client_id": "old-client",
                    "token_response": {
                        "access_token": "old-access",
                        "token_type": "Bearer"
                    },
                    "issuer": "https://old.example.test"
                }))
                .unwrap(),
            )
            .await
            .unwrap();

        let login_guard = lock_credential_mutation(
            &credential_refresh_lock_path(&config_path, "docs", &url),
            "docs",
        )
        .await
        .unwrap();
        let runtime_config_path = config_path.clone();
        let runtime_url = url.clone();
        let manager_task = tokio::spawn(async move {
            authorization_manager(
                &runtime_config_path,
                "docs",
                &runtime_url,
                McpOAuthCredentialsStore::File,
                false,
            )
            .await
        });

        time::sleep(Duration::from_millis(20)).await;
        assert!(
            !manager_task.is_finished(),
            "runtime 初始化必须等待正在持锁保存新 grant 的 login"
        );
        store
            .save(
                serde_json::from_value(json!({
                    "client_id": "new-client",
                    "token_response": {
                        "access_token": "new-access",
                        "token_type": "Bearer"
                    },
                    "issuer": format!("{authorization_server}/")
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        drop(login_guard);

        let manager = time::timeout(Duration::from_secs(1), manager_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(manager.is_some());
        assert_eq!(store.load().await.unwrap().unwrap().client_id, "new-client");
        server_task.abort();
    }

    #[tokio::test]
    async fn runtime_initialization_fails_closed_when_credentials_store_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join(FILE_CREDENTIALS_DIR), b"not a directory")
            .await
            .unwrap();
        let result = authorization_manager(
            &dir.path().join(".mcp.json"),
            "docs",
            "https://example.test/mcp",
            McpOAuthCredentialsStore::File,
            true,
        )
        .await;

        assert!(matches!(result, Err(McpOAuthError::CredentialStore { .. })));
    }

    #[tokio::test]
    async fn authorization_session_uses_discovered_scope_and_resource() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let authorization_server = format!("http://{}", listener.local_addr().unwrap());
        let registration = Arc::new(Mutex::new(None));
        let token_requests = Arc::new(Mutex::new(Vec::new()));
        let state = OAuthDiscoveryState {
            authorization_server: authorization_server.clone(),
            registration: Arc::clone(&registration),
            token_requests: Arc::clone(&token_requests),
        };
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/mcp", get(oauth_test_mcp_endpoint))
                    .route(
                        "/.well-known/oauth-protected-resource",
                        get(oauth_test_resource_metadata),
                    )
                    .route(
                        "/.well-known/oauth-authorization-server",
                        get(oauth_test_authorization_metadata),
                    )
                    .route("/register", post(oauth_test_register))
                    .route("/token", post(oauth_test_token))
                    .with_state(state),
            )
            .into_future(),
        );
        let resource = format!("{authorization_server}/mcp");
        let manager = AuthorizationManager::new(resource.clone()).await.unwrap();

        let session = authorization_session(
            manager,
            "http://127.0.0.1:9876/callback",
            None,
            &["existing:grant".to_string()],
        )
        .await
        .unwrap();
        let authorization_url = reqwest::Url::parse(session.get_authorization_url()).unwrap();
        let params = authorization_url.query_pairs().collect::<BTreeMap<_, _>>();

        let scopes = params
            .get("scope")
            .unwrap()
            .split_whitespace()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            scopes,
            std::collections::BTreeSet::from(["challenge:read", "existing:grant", "files:read"])
        );
        assert_eq!(
            params.get("resource").map(|value| value.as_ref()),
            Some(resource.as_str())
        );
        assert_eq!(
            registration.lock().unwrap().as_ref().unwrap()["redirect_uris"][0],
            "http://127.0.0.1:9876/callback"
        );
        session
            .handle_callback_with_issuer(
                "authorization-code",
                params.get("state").unwrap(),
                Some(&format!("{authorization_server}/")),
            )
            .await
            .unwrap();
        session.auth_manager.refresh_token().await.unwrap();
        let token_resources = token_requests
            .lock()
            .unwrap()
            .iter()
            .map(|body| {
                reqwest::Url::parse(&format!("http://localhost/?{body}"))
                    .unwrap()
                    .query_pairs()
                    .find_map(|(key, value)| (key == "resource").then(|| value.into_owned()))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            token_resources,
            vec![Some(resource.clone()), Some(resource)]
        );

        *registration.lock().unwrap() = None;
        let manager = AuthorizationManager::new(format!("{authorization_server}/mcp"))
            .await
            .unwrap();
        let preregistered = authorization_session(
            manager,
            "http://127.0.0.1:9876/callback",
            Some("pre-registered-client"),
            &[],
        )
        .await
        .unwrap();
        let url = reqwest::Url::parse(preregistered.get_authorization_url()).unwrap();
        assert_eq!(
            url.query_pairs()
                .find_map(|(key, value)| (key == "client_id").then(|| value.into_owned()))
                .as_deref(),
            Some("pre-registered-client")
        );
        assert!(registration.lock().unwrap().is_none());

        let manager = AuthorizationManager::new(format!("{authorization_server}/mcp"))
            .await
            .unwrap();
        let issuer_checked =
            authorization_session(manager, "http://127.0.0.1:9876/callback", None, &[])
                .await
                .unwrap();
        let state = reqwest::Url::parse(issuer_checked.get_authorization_url())
            .unwrap()
            .query_pairs()
            .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
            .unwrap();
        assert!(matches!(
            issuer_checked
                .handle_callback_with_issuer(
                    "authorization-code",
                    &state,
                    Some("https://evil.example.test")
                )
                .await,
            Err(AuthError::AuthorizationServerMismatch { .. })
        ));

        server.abort();
    }

    #[tokio::test]
    async fn detached_credential_mutation_keeps_cross_process_lease_until_completion() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("upstream-a").join(".mcp.json");
        let url = "https://resource.example.test/mcp";
        let lock_path = credential_refresh_lock_path(&config_path, "docs", url);
        let store =
            McpCredentialStore::new(&config_path, "docs", url, McpOAuthCredentialsStore::File);
        let guard = lock_credential_mutation(&lock_path, "docs").await.unwrap();
        store.set_mutation_lease(guard);

        // file fsync 或 keyring 调用已经进入不可取消 work item 时，它会克隆这份 lease。
        let detached_mutation_lease = store.mutation_lease().unwrap();
        store.clear_mutation_lease();
        drop(store);

        assert!(
            FileLockGuard::try_lock_exclusive(&lock_path)
                .await
                .unwrap()
                .is_none(),
            "父 future 被取消后，detached mutation 完成前不能让并发 login 取得锁"
        );
        drop(detached_mutation_lease);
        assert!(
            FileLockGuard::try_lock_exclusive(&lock_path)
                .await
                .unwrap()
                .is_some(),
            "detached mutation 完成后必须释放 credential lock"
        );
    }
}
