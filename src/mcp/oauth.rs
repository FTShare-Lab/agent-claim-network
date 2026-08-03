//! Streamable HTTP MCP 的 OAuth 登录与凭据存储。
//!
//! OAuth token 与 client id 写入 server 配置指定的 keyring 或私有文件；
//! `.mcp.json` 只保存非敏感 OAuth 选项。登录使用 PKCE，并支持本机 loopback
//! callback 或 headless 环境下粘贴完整 redirect URL。

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
    AuthError, AuthorizationManager, AuthorizationRequest, AuthorizationSession, CredentialStore,
    StoredCredentials,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::oneshot, time};
use tokio_util::sync::CancellationToken;

use crate::auth::sha256_hex;
use crate::mcp::config::{McpOAuthCredentialsStore, McpServerConfig, McpTransportConfig};

const KEYRING_SERVICE: &str = "agent-claim-network.mcp";
const LOGIN_CALLBACK_PATH: &str = "/callback";
const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);
const FILE_CREDENTIALS_DIR: &str = ".mcp-oauth";
/// `CredentialStore` 只能通过 `AuthError::InternalError` 回传失败原因，凭据存储故障
/// 用这个固定 message 编码，供上层还原成 `McpOAuthError::CredentialStore`。
const CREDENTIAL_STORE_UNAVAILABLE: &str = "OAuth credential store unavailable";

#[derive(Debug, thiserror::Error)]
pub enum McpOAuthError {
    #[error("MCP server '{server}' 不支持 OAuth 登录：仅 streamable_http server 可登录")]
    UnsupportedTransport { server: String },
    #[error("MCP server '{server}' 的 OAuth 配置无效")]
    InvalidConfig { server: String },
    #[error("MCP server '{server}' 无法访问 OAuth 凭据存储")]
    CredentialStore { server: String },
    #[error("MCP server '{server}' 的已保存 OAuth 凭据无效")]
    InvalidStoredCredentials { server: String },
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
struct McpCredentialStore {
    server_name: String,
    backend: CredentialBackend,
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

impl McpCredentialStore {
    fn new(
        config_path: &Path,
        server_name: &str,
        url: &str,
        store: McpOAuthCredentialsStore,
    ) -> Self {
        let account = credential_account(config_path, server_name, url);
        let backend = match store {
            McpOAuthCredentialsStore::Keyring => CredentialBackend::Keyring { account },
            McpOAuthCredentialsStore::File => CredentialBackend::File {
                path: credential_file_path(config_path, &account),
            },
        };
        Self {
            server_name: server_name.to_string(),
            backend,
        }
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
        tokio::task::spawn_blocking(move || {
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
        .map_err(|_| McpOAuthError::CredentialStore {
            server: self.server_name.clone(),
        })?
    }

    async fn write(&self, value: String) -> Result<(), McpOAuthError> {
        if let CredentialBackend::File { path } = &self.backend {
            return write_private_credentials_file(path, value, &self.server_name).await;
        }
        let server = self.server_name.clone();
        let CredentialBackend::Keyring { account } = self.backend.clone() else {
            unreachable!("file credential store returned above");
        };
        tokio::task::spawn_blocking(move || {
            KeyringEntry::new(KEYRING_SERVICE, &account)
                .and_then(|entry| entry.set_password(&value))
                .map_err(|_| McpOAuthError::CredentialStore { server })
        })
        .await
        .map_err(|_| McpOAuthError::CredentialStore {
            server: self.server_name.clone(),
        })?
    }

    async fn delete(&self) -> Result<(), McpOAuthError> {
        if let CredentialBackend::File { path } = &self.backend {
            return match tokio::fs::remove_file(path).await {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(_) => Err(McpOAuthError::CredentialStore {
                    server: self.server_name.clone(),
                }),
            };
        }
        let server = self.server_name.clone();
        let CredentialBackend::Keyring { account } = self.backend.clone() else {
            unreachable!("file credential store returned above");
        };
        tokio::task::spawn_blocking(move || {
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
        .map_err(|_| McpOAuthError::CredentialStore {
            server: self.server_name.clone(),
        })?
    }
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
        self.write(raw).await.map_err(auth_store_error)
    }

    async fn clear(&self) -> Result<(), AuthError> {
        self.delete().await.map_err(auth_store_error)
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
    let credentials =
        McpCredentialStore::new(config_path, server_name, &url, oauth_credentials_store);
    let mut manager = AuthorizationManager::new(url)
        .await
        .map_err(|error| authorization_setup_error(server_name, error))?;
    manager.set_credential_store(credentials);
    let session = authorization_session(manager, &callback_url, oauth_client_id.as_deref())
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
        session.handle_callback_url(&callback_url).await
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
        session
            .handle_callback_with_issuer(&code, &state, callback.iss.as_deref())
            .await
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
) -> Result<AuthorizationSession, AuthError> {
    let metadata = manager.resolve_metadata().await?;
    manager.set_metadata(metadata.metadata);
    let scopes = manager.select_scopes(None, &[]);
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
    McpCredentialStore::new(config_path, server_name, &url, oauth_credentials_store)
        .delete()
        .await
}

/// 返回已登录 server 的 `AuthorizationManager`；没有已保存凭据时返回 `None`。
///
/// 交给 `AuthClient` 持有，让每次请求按需取 token 并在过期时用 refresh token 续期；
/// 只在建连时取一次 access token 会让长会话在 token 过期后开始 401。
///
/// 未登录时不做 OAuth metadata discovery，避免给普通 HTTP server 增加一次网络请求。
/// 凭据存储不可用时按未登录处理并记 warning：多数 Streamable HTTP server 不需要
/// OAuth，不应因此连不上；真实原因会在用户执行 `acn mcp login` 时明确报出。
pub async fn authorization_manager(
    config_path: &Path,
    server_name: &str,
    url: &str,
    store: McpOAuthCredentialsStore,
) -> Result<Option<AuthorizationManager>, McpOAuthError> {
    let credentials = McpCredentialStore::new(config_path, server_name, url, store);
    match credentials.read().await {
        Ok(Some(_)) => {}
        Ok(None) => return Ok(None),
        Err(error) => {
            log::warn!("{error}；按未登录方式连接");
            return Ok(None);
        }
    }
    let mut manager = AuthorizationManager::new(url)
        .await
        .map_err(|error| authorization_setup_error(server_name, error))?;
    manager.set_credential_store(credentials);
    let loaded = manager.initialize_from_store().await.map_err(|error| {
        credential_store_error_or(server_name, error, |_| {
            McpOAuthError::InvalidStoredCredentials {
                server: server_name.to_string(),
            }
        })
    })?;
    Ok(loaded.then_some(manager))
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
    let read = tokio::task::spawn_blocking(move || -> Result<String, McpOAuthError> {
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
    });
    time::timeout(LOGIN_TIMEOUT, read)
        .await
        .map_err(|_| McpOAuthError::LoginTimeout {
            server: server_name.to_string(),
        })?
        .map_err(|_| McpOAuthError::CallbackInput {
            server: server_name.to_string(),
        })?
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

async fn write_private_credentials_file(
    path: &Path,
    value: String,
    server_name: &str,
) -> Result<(), McpOAuthError> {
    let Some(parent) = path.parent() else {
        return Err(McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        });
    };
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })?;
    set_private_directory_permissions(parent, server_name).await?;

    let path = path.to_path_buf();
    let parent = parent.to_path_buf();
    let server = server_name.to_string();
    tokio::task::spawn_blocking(move || {
        let mut file = tempfile::NamedTempFile::new_in(parent).map_err(|_| {
            McpOAuthError::CredentialStore {
                server: server.clone(),
            }
        })?;
        file.write_all(value.as_bytes())
            .and_then(|_| file.flush())
            .and_then(|_| file.as_file().sync_all())
            .map_err(|_| McpOAuthError::CredentialStore {
                server: server.clone(),
            })?;
        file.persist(&path)
            .map_err(|_| McpOAuthError::CredentialStore {
                server: server.clone(),
            })?;
        set_private_file_permissions_blocking(&path)
            .map_err(|_| McpOAuthError::CredentialStore { server })
    })
    .await
    .map_err(|_| McpOAuthError::CredentialStore {
        server: server_name.to_string(),
    })?
}

#[cfg(unix)]
async fn set_private_directory_permissions(
    path: &Path,
    server_name: &str,
) -> Result<(), McpOAuthError> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .await
        .map_err(|_| McpOAuthError::CredentialStore {
            server: server_name.to_string(),
        })
}

#[cfg(not(unix))]
async fn set_private_directory_permissions(
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
    let mut command = {
        let mut command = std::process::Command::new("cmd");
        command.args(["/C", "start", ""]);
        command
    };
    command.arg(url).spawn().map(|_| ())
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

    /// `AuthClient` 每次请求都从凭据库重新加载并按需刷新，client_id 和 refresh_token
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

        let session = authorization_session(manager, "http://127.0.0.1:9876/callback", None)
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
            std::collections::BTreeSet::from(["challenge:read", "files:read"])
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
        let issuer_checked = authorization_session(manager, "http://127.0.0.1:9876/callback", None)
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
}
