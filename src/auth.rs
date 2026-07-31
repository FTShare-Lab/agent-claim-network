//! 团队侧 API key 鉴权。
//!
//! 本模块集中维护 `{auth, data}` 信封协议、团队 key 台账读写、
//! key hash 生成与常量时间校验。Maintainer / Router 只在 handler
//! 入口解包信封并做对象级绑定，不在业务存储层感知明文 key。

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use rand::RngCore;
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::claim::AgentId;
use crate::storage::{read_yaml, write_yaml_atomic, StorageError};
use crate::time::now_seconds;

pub const ROUTER_SERVICE_AGENT_ID: &str = "router-service";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthEnvelope {
    pub agent_id: AgentId,
    pub acn_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthRequest<T> {
    pub auth: AuthEnvelope,
    pub data: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthKeyStatus {
    Active,
    Revoked,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthApiKeyConfig {
    pub key_id: String,
    pub agent_id: AgentId,
    /// `sha256:<64 hex>`；服务端不保存明文 key。
    pub key_hash: String,
    pub generated_time: DateTime<Utc>,
    pub status: AuthKeyStatus,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_keys: Vec<AuthApiKeyConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthKeyLedger {
    #[serde(default)]
    pub auth: AuthConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthPrincipal {
    pub agent_id: AgentId,
    pub key_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("auth key key_id={key_id} 的 key_hash 格式非法: {reason}")]
    InvalidKeyHash { key_id: String, reason: String },
    #[error("auth key key_id={key_id} 的 agent_id 为空")]
    EmptyAgentId { key_id: String },
    #[error("agent_id={agent_id} 同时存在多条 active key")]
    DuplicateActiveAgent { agent_id: AgentId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthHttpError {
    Unauthorized,
    Forbidden,
}

impl AuthHttpError {
    pub fn into_http_response(self) -> (StatusCode, String) {
        match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden".to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthVerifier {
    inner: Arc<Mutex<AuthVerifierState>>,
}

#[derive(Debug, Clone)]
struct AuthVerifierState {
    enabled: bool,
    keys: Vec<VerifiedAuthKey>,
}

impl Default for AuthVerifier {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone)]
struct VerifiedAuthKey {
    key_hash: [u8; 32],
    principal: AuthPrincipal,
}

impl AuthVerifier {
    pub fn disabled() -> Self {
        Self::from_state(AuthVerifierState {
            enabled: false,
            keys: Vec::new(),
        })
    }

    pub fn from_config(config: &AuthConfig) -> Result<Self, AuthConfigError> {
        compile_auth_config(config).map(Self::from_state)
    }

    pub async fn from_key_store_path(
        path: &Path,
        enabled: bool,
    ) -> Result<Self, TeamAuthStoreError> {
        let verifier = Self::disabled();
        let store = TeamAuthStore::new(path.to_path_buf());
        verifier
            .replace_active_keys_from_store(&store, enabled)
            .await?;
        Ok(verifier)
    }

    pub async fn replace_active_keys_from_store(
        &self,
        store: &TeamAuthStore,
        enabled: bool,
    ) -> Result<(), TeamAuthStoreError> {
        if !enabled {
            let mut state = self.state_guard();
            *state = AuthVerifierState {
                enabled: false,
                keys: Vec::new(),
            };
            return Ok(());
        }

        let mut ledger = store.load_for_verifier().await?;
        ledger.auth.enabled = true;
        let snapshot = compile_auth_config(&ledger.auth).map_err(TeamAuthStoreError::Config)?;
        let mut state = self.state_guard();
        *state = snapshot;
        Ok(())
    }

    fn from_state(state: AuthVerifierState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    fn state_guard(&self) -> MutexGuard<'_, AuthVerifierState> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.state_guard().enabled
    }

    /// 校验 `{auth, data}` 信封中的凭据。auth 关闭时返回 `Ok(None)`。
    pub fn verify_envelope(
        &self,
        auth: Option<&AuthEnvelope>,
    ) -> Result<Option<AuthPrincipal>, AuthHttpError> {
        let state = self.state_guard();
        if !state.enabled {
            return Ok(None);
        }
        let auth = auth.ok_or(AuthHttpError::Unauthorized)?;
        let incoming_hash = sha256_bytes(auth.acn_key.as_bytes());
        let mut matched: Option<AuthPrincipal> = None;
        for key in &state.keys {
            let hash_matches = bool::from(key.key_hash.ct_eq(&incoming_hash));
            if hash_matches && key.principal.agent_id == auth.agent_id {
                matched = Some(key.principal.clone());
            }
        }
        matched.map(Some).ok_or(AuthHttpError::Unauthorized)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicAuthKeyRecord {
    pub key_id: String,
    pub agent_id: AgentId,
    pub generated_time: DateTime<Utc>,
    pub status: AuthKeyStatus,
}

impl From<&AuthApiKeyConfig> for PublicAuthKeyRecord {
    fn from(value: &AuthApiKeyConfig) -> Self {
        Self {
            key_id: value.key_id.clone(),
            agent_id: value.agent_id.clone(),
            generated_time: value.generated_time,
            status: value.status,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateAuthKeyResponse {
    pub key: PublicAuthKeyRecord,
    pub acn_key: String,
}

#[derive(Debug, Clone)]
pub struct CreatedAuthKey {
    pub response: CreateAuthKeyResponse,
}

#[derive(Debug, Clone)]
pub struct ServiceAuthKey {
    pub agent_id: AgentId,
    pub acn_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum TeamAuthStoreError {
    #[error("读取或写入团队 auth key store 失败: {0}")]
    Storage(#[from] StorageError),
    #[error("团队 auth key store 配置非法: {0}")]
    Config(#[from] AuthConfigError),
    #[error("agent_id={agent_id} 已存在 active key {key_id}")]
    ActiveKeyConflict { agent_id: AgentId, key_id: String },
    #[error("未找到 key_id={key_id}")]
    KeyNotFound { key_id: String },
    #[error("agent_id 非法: {0}")]
    InvalidAgentId(String),
    #[error("agent_id={agent_id} 是系统保留身份")]
    ReservedAgentId { agent_id: AgentId },
    #[error("生成 key_id 多次碰撞，请重试")]
    KeyIdCollision,
}

#[derive(Debug, Clone)]
pub struct TeamAuthStore {
    path: PathBuf,
    write_lock: Arc<AsyncMutex<()>>,
}

impl TeamAuthStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn load_or_default(&self) -> Result<AuthKeyLedger, TeamAuthStoreError> {
        match read_yaml::<AuthKeyLedger>(&self.path).await {
            Ok(ledger) => Ok(ledger),
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(AuthKeyLedger::default())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn load_for_verifier(&self) -> Result<AuthKeyLedger, TeamAuthStoreError> {
        match read_yaml::<AuthKeyLedger>(&self.path).await {
            Ok(ledger) => Ok(ledger),
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(AuthKeyLedger {
                    auth: AuthConfig {
                        enabled: true,
                        api_keys: Vec::new(),
                    },
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn list_public(&self) -> Result<Vec<PublicAuthKeyRecord>, TeamAuthStoreError> {
        let mut rows = self
            .load_or_default()
            .await?
            .auth
            .api_keys
            .iter()
            .filter(|row| !is_router_service_agent(&row.agent_id))
            .map(PublicAuthKeyRecord::from)
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            right
                .generated_time
                .cmp(&left.generated_time)
                .then_with(|| left.agent_id.as_str().cmp(right.agent_id.as_str()))
                .then_with(|| left.key_id.cmp(&right.key_id))
        });
        Ok(rows)
    }

    pub async fn create_key(&self, agent_id: &str) -> Result<CreatedAuthKey, TeamAuthStoreError> {
        let _guard = self.write_lock.lock().await;
        let agent_id = AgentId::new(agent_id.trim().to_string())
            .map_err(|err| TeamAuthStoreError::InvalidAgentId(err.to_string()))?;
        if is_router_service_agent(&agent_id) {
            return Err(TeamAuthStoreError::ReservedAgentId { agent_id });
        }
        let mut ledger = self.load_or_default().await?;
        if let Some(active) = ledger
            .auth
            .api_keys
            .iter()
            .find(|row| row.agent_id == agent_id && row.status == AuthKeyStatus::Active)
        {
            return Err(TeamAuthStoreError::ActiveKeyConflict {
                agent_id,
                key_id: active.key_id.clone(),
            });
        }

        ledger.auth.enabled = true;
        let key_id = unique_key_id(&ledger.auth.api_keys)?;
        let acn_key = generate_acn_key();
        let row = AuthApiKeyConfig {
            key_id,
            agent_id,
            key_hash: format!("sha256:{}", sha256_hex(&acn_key)),
            generated_time: now_seconds(),
            status: AuthKeyStatus::Active,
        };
        let public = PublicAuthKeyRecord::from(&row);
        ledger.auth.api_keys.push(row.clone());
        compile_auth_config(&ledger.auth).map_err(TeamAuthStoreError::Config)?;
        write_yaml_atomic(&self.path, &ledger).await?;
        Ok(CreatedAuthKey {
            response: CreateAuthKeyResponse {
                key: public,
                acn_key,
            },
        })
    }

    pub async fn revoke_key(
        &self,
        key_id: &str,
    ) -> Result<PublicAuthKeyRecord, TeamAuthStoreError> {
        let _guard = self.write_lock.lock().await;
        let key_id = key_id.trim();
        let mut ledger = self.load_or_default().await?;
        let Some(row) = ledger
            .auth
            .api_keys
            .iter_mut()
            .find(|row| row.key_id == key_id)
        else {
            return Err(TeamAuthStoreError::KeyNotFound {
                key_id: key_id.to_string(),
            });
        };
        if is_router_service_agent(&row.agent_id) {
            return Err(TeamAuthStoreError::ReservedAgentId {
                agent_id: row.agent_id.clone(),
            });
        }
        row.status = AuthKeyStatus::Revoked;
        let public = PublicAuthKeyRecord::from(&*row);
        write_yaml_atomic(&self.path, &ledger).await?;
        Ok(public)
    }

    pub async fn ensure_router_service_key(
        &self,
        private_key_path: &Path,
    ) -> Result<ServiceAuthKey, TeamAuthStoreError> {
        let _guard = self.write_lock.lock().await;
        let service_agent = AgentId::new(ROUTER_SERVICE_AGENT_ID)
            .map_err(|err| TeamAuthStoreError::InvalidAgentId(err.to_string()))?;
        let mut ledger = self.load_or_default().await?;
        let private_key = read_optional_plain_key(private_key_path).await?;

        let active_service_rows = ledger
            .auth
            .api_keys
            .iter()
            .filter(|row| row.agent_id == service_agent && row.status == AuthKeyStatus::Active)
            .collect::<Vec<_>>();
        if let Some(acn_key) = private_key.as_deref() {
            let expected_hash = format!("sha256:{}", sha256_hex(acn_key));
            if active_service_rows.len() == 1 && active_service_rows[0].key_hash == expected_hash {
                set_private_file_permissions(private_key_path).await?;
                return Ok(ServiceAuthKey {
                    agent_id: service_agent,
                    acn_key: acn_key.to_string(),
                });
            }
        }

        for row in ledger
            .auth
            .api_keys
            .iter_mut()
            .filter(|row| row.agent_id == service_agent && row.status == AuthKeyStatus::Active)
        {
            row.status = AuthKeyStatus::Revoked;
        }

        ledger.auth.enabled = true;
        let acn_key = generate_acn_key();
        let row = AuthApiKeyConfig {
            key_id: unique_key_id(&ledger.auth.api_keys)?,
            agent_id: service_agent.clone(),
            key_hash: format!("sha256:{}", sha256_hex(&acn_key)),
            generated_time: now_seconds(),
            status: AuthKeyStatus::Active,
        };
        ledger.auth.api_keys.push(row.clone());
        write_yaml_atomic(&self.path, &ledger).await?;
        write_private_service_key(private_key_path, &acn_key).await?;
        Ok(ServiceAuthKey {
            agent_id: service_agent,
            acn_key,
        })
    }
}

pub fn sha256_hex(value: &str) -> String {
    hex::encode(sha256_bytes(value.as_bytes()))
}

pub fn is_router_service_agent(agent_id: &AgentId) -> bool {
    agent_id.as_str() == ROUTER_SERVICE_AGENT_ID
}

fn compile_auth_config(config: &AuthConfig) -> Result<AuthVerifierState, AuthConfigError> {
    if !config.enabled {
        return Ok(AuthVerifierState {
            enabled: false,
            keys: Vec::new(),
        });
    }

    let mut seen_agents = HashSet::new();
    let mut keys = Vec::new();
    for item in &config.api_keys {
        let Some(key) = verified_key_from_row(item)? else {
            continue;
        };
        if !seen_agents.insert(key.principal.agent_id.clone()) {
            return Err(AuthConfigError::DuplicateActiveAgent {
                agent_id: key.principal.agent_id,
            });
        }
        keys.push(key);
    }

    Ok(AuthVerifierState {
        enabled: true,
        keys,
    })
}

fn verified_key_from_row(
    item: &AuthApiKeyConfig,
) -> Result<Option<VerifiedAuthKey>, AuthConfigError> {
    if item.status != AuthKeyStatus::Active {
        return Ok(None);
    }
    if item.agent_id.as_str().trim().is_empty() {
        return Err(AuthConfigError::EmptyAgentId {
            key_id: item.key_id.clone(),
        });
    }
    let key_hash =
        parse_sha256_hash(&item.key_hash).map_err(|reason| AuthConfigError::InvalidKeyHash {
            key_id: item.key_id.clone(),
            reason,
        })?;
    Ok(Some(VerifiedAuthKey {
        key_hash,
        principal: AuthPrincipal {
            agent_id: item.agent_id.clone(),
            key_id: item.key_id.clone(),
        },
    }))
}

fn sha256_bytes(value: &[u8]) -> [u8; 32] {
    let hash = digest(&SHA256, value);
    let mut out = [0u8; 32];
    out.copy_from_slice(hash.as_ref());
    out
}

fn parse_sha256_hash(raw: &str) -> Result<[u8; 32], String> {
    let trimmed = raw.trim();
    let Some(hex_value) = trimmed.strip_prefix("sha256:") else {
        return Err("key_hash 必须使用 sha256:<64 hex> 格式".to_string());
    };
    let bytes = hex::decode(hex_value).map_err(|err| err.to_string())?;
    let len = bytes.len();
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("sha256 hash 必须是 32 bytes，实际 {len} bytes"))?;
    Ok(arr)
}

fn unique_key_id(rows: &[AuthApiKeyConfig]) -> Result<String, TeamAuthStoreError> {
    for _ in 0..32 {
        let key_id = format!("key_{}", random_hex(4));
        if !rows.iter().any(|row| row.key_id == key_id) {
            return Ok(key_id);
        }
    }
    Err(TeamAuthStoreError::KeyIdCollision)
}

fn generate_acn_key() -> String {
    format!("acn_{}", random_hex(32))
}

async fn read_optional_plain_key(path: &Path) -> Result<Option<String>, TeamAuthStoreError> {
    match tokio::fs::read_to_string(path).await {
        Ok(raw) => {
            let key = raw.trim().to_string();
            Ok((!key.is_empty()).then_some(key))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(TeamAuthStoreError::Storage(StorageError::Io {
            path: path.to_path_buf(),
            source,
        })),
    }
}

async fn write_private_service_key(path: &Path, acn_key: &str) -> Result<(), TeamAuthStoreError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: parent.to_path_buf(),
                source,
            })
        })?;
    }

    let tmp_path = private_tmp_sibling(path);
    let write_result = async {
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options.open(&tmp_path).await.map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: tmp_path.clone(),
                source,
            })
        })?;
        file.write_all(acn_key.as_bytes()).await.map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: tmp_path.clone(),
                source,
            })
        })?;
        file.flush().await.map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: tmp_path.clone(),
                source,
            })
        })?;
        file.sync_all().await.map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: tmp_path.clone(),
                source,
            })
        })?;
        tokio::fs::rename(&tmp_path, path).await.map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: path.to_path_buf(),
                source,
            })
        })
    }
    .await;
    if write_result.is_err() {
        let _ = tokio::fs::remove_file(&tmp_path).await;
    }
    write_result?;
    set_private_file_permissions(path).await?;
    Ok(())
}

fn private_tmp_sibling(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|file_name| file_name.to_os_string())
        .unwrap_or_default();
    name.push(format!(".tmp.{}", random_hex(4)));
    path.with_file_name(name)
}

#[cfg(unix)]
async fn set_private_file_permissions(path: &Path) -> Result<(), TeamAuthStoreError> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = std::fs::Permissions::from_mode(0o600);
    tokio::fs::set_permissions(path, permissions)
        .await
        .map_err(|source| {
            TeamAuthStoreError::Storage(StorageError::Io {
                path: path.to_path_buf(),
                source,
            })
        })
}

#[cfg(not(unix))]
async fn set_private_file_permissions(_path: &Path) -> Result<(), TeamAuthStoreError> {
    Ok(())
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    hex::encode(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent_id(raw: &str) -> AgentId {
        AgentId::new(raw).unwrap()
    }

    fn active_row(agent: &str, key: &str) -> AuthApiKeyConfig {
        AuthApiKeyConfig {
            key_id: format!("key_{agent}"),
            agent_id: agent_id(agent),
            key_hash: format!("sha256:{}", sha256_hex(key)),
            generated_time: "2026-06-26T12:00:00Z".parse().unwrap(),
            status: AuthKeyStatus::Active,
        }
    }

    #[test]
    fn verifier_accepts_matching_envelope() {
        let verifier = AuthVerifier::from_config(&AuthConfig {
            enabled: true,
            api_keys: vec![active_row("agent-a", "secret")],
        })
        .unwrap();

        let principal = verifier
            .verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "secret".into(),
            }))
            .unwrap()
            .unwrap();

        assert_eq!(principal.agent_id, agent_id("agent-a"));
        assert_eq!(principal.key_id, "key_agent-a");
    }

    #[test]
    fn verifier_rejects_missing_wrong_or_mismatched_key() {
        let verifier = AuthVerifier::from_config(&AuthConfig {
            enabled: true,
            api_keys: vec![active_row("agent-a", "secret")],
        })
        .unwrap();

        assert_eq!(
            verifier.verify_envelope(None),
            Err(AuthHttpError::Unauthorized)
        );
        assert_eq!(
            verifier.verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "wrong".into(),
            })),
            Err(AuthHttpError::Unauthorized)
        );
        assert_eq!(
            verifier.verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-b"),
                acn_key: "secret".into(),
            })),
            Err(AuthHttpError::Unauthorized)
        );
    }

    #[test]
    fn verifier_ignores_revoked_key() {
        let mut row = active_row("agent-a", "secret");
        row.status = AuthKeyStatus::Revoked;
        let verifier = AuthVerifier::from_config(&AuthConfig {
            enabled: true,
            api_keys: vec![row],
        })
        .unwrap();

        assert_eq!(
            verifier.verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "secret".into(),
            })),
            Err(AuthHttpError::Unauthorized)
        );
    }

    #[test]
    fn verifier_rejects_duplicate_active_agent() {
        let mut second = active_row("agent-a", "other-secret");
        second.key_id = "key_agent-a_2".into();

        let err = AuthVerifier::from_config(&AuthConfig {
            enabled: true,
            api_keys: vec![active_row("agent-a", "secret"), second],
        })
        .unwrap_err()
        .to_string();

        assert!(err.contains("多条 active key"));
    }

    #[test]
    fn verifier_requires_sha256_prefix() {
        let mut row = active_row("agent-a", "secret");
        row.key_hash = sha256_hex("secret");

        let err = AuthVerifier::from_config(&AuthConfig {
            enabled: true,
            api_keys: vec![row],
        })
        .unwrap_err()
        .to_string();

        assert!(err.contains("sha256:<64 hex>"));
    }

    #[tokio::test]
    async fn missing_key_store_starts_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let verifier = AuthVerifier::from_key_store_path(
            &dir.path().join("maintainer").join("auth_keys.yaml"),
            true,
        )
        .await
        .unwrap();

        assert!(verifier.is_enabled());
        assert_eq!(
            verifier.verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "secret".into(),
            })),
            Err(AuthHttpError::Unauthorized)
        );
    }

    #[tokio::test]
    async fn disabled_key_store_starts_fail_closed_for_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("maintainer").join("auth_keys.yaml");
        write_yaml_atomic(&path, &AuthKeyLedger::default())
            .await
            .unwrap();

        let verifier = AuthVerifier::from_key_store_path(&path, true)
            .await
            .unwrap();

        assert!(verifier.is_enabled());
        assert_eq!(
            verifier.verify_envelope(None),
            Err(AuthHttpError::Unauthorized)
        );
    }

    #[tokio::test]
    async fn disabled_team_auth_config_skips_key_store_verification() {
        let dir = tempfile::tempdir().unwrap();
        let verifier = AuthVerifier::from_key_store_path(
            &dir.path().join("maintainer").join("auth_keys.yaml"),
            false,
        )
        .await
        .unwrap();

        assert!(!verifier.is_enabled());
        assert_eq!(verifier.verify_envelope(None), Ok(None));
    }

    #[tokio::test]
    async fn replace_active_keys_from_store_drops_old_snapshot_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("maintainer").join("auth_keys.yaml");
        let store = TeamAuthStore::new(path.clone());
        write_yaml_atomic(
            &path,
            &AuthKeyLedger {
                auth: AuthConfig {
                    enabled: true,
                    api_keys: vec![active_row("agent-a", "old-secret")],
                },
            },
        )
        .await
        .unwrap();
        let verifier = AuthVerifier::from_key_store_path(&path, true)
            .await
            .unwrap();
        assert!(verifier
            .verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "old-secret".into(),
            }))
            .is_ok());

        let mut old = active_row("agent-a", "old-secret");
        old.status = AuthKeyStatus::Revoked;
        let replacement = active_row("agent-a", "new-secret");
        write_yaml_atomic(
            &path,
            &AuthKeyLedger {
                auth: AuthConfig {
                    enabled: true,
                    api_keys: vec![old, replacement],
                },
            },
        )
        .await
        .unwrap();

        verifier
            .replace_active_keys_from_store(&store, true)
            .await
            .unwrap();
        assert_eq!(
            verifier.verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "old-secret".into(),
            })),
            Err(AuthHttpError::Unauthorized)
        );
        assert!(verifier
            .verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: "new-secret".into(),
            }))
            .is_ok());
    }

    #[tokio::test]
    async fn team_auth_store_creates_lists_and_revokes_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));

        let created = store.create_key("agent-a").await.unwrap();
        assert!(created.response.acn_key.starts_with("acn_"));
        assert_eq!(created.response.key.agent_id, agent_id("agent-a"));
        assert_eq!(created.response.key.status, AuthKeyStatus::Active);
        assert_eq!(store.list_public().await.unwrap().len(), 1);

        let revoked = store
            .revoke_key(&created.response.key.key_id)
            .await
            .unwrap();
        assert_eq!(revoked.status, AuthKeyStatus::Revoked);
        let verifier = AuthVerifier::from_key_store_path(store.path(), true)
            .await
            .unwrap();
        assert_eq!(
            verifier.verify_envelope(Some(&AuthEnvelope {
                agent_id: agent_id("agent-a"),
                acn_key: created.response.acn_key,
            })),
            Err(AuthHttpError::Unauthorized)
        );
    }

    #[tokio::test]
    async fn team_auth_store_rejects_duplicate_active_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));

        let created = store.create_key("agent-a").await.unwrap();
        let err = store.create_key("agent-a").await.unwrap_err();

        match err {
            TeamAuthStoreError::ActiveKeyConflict {
                agent_id: existing_agent,
                key_id,
            } => {
                assert_eq!(existing_agent, agent_id("agent-a"));
                assert_eq!(key_id, created.response.key.key_id);
            }
            other => panic!("expected active key conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn team_auth_store_validates_candidate_ledger_before_writing_new_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("maintainer").join("auth_keys.yaml");
        let store = TeamAuthStore::new(path.clone());
        let mut bad_row = active_row("agent-a", "secret");
        bad_row.key_hash = "sha256:not-hex".to_string();
        write_yaml_atomic(
            &path,
            &AuthKeyLedger {
                auth: AuthConfig {
                    enabled: true,
                    api_keys: vec![bad_row],
                },
            },
        )
        .await
        .unwrap();

        let err = store.create_key("agent-b").await.unwrap_err().to_string();

        assert!(err.contains("key_hash"));
        let raw = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(!raw.contains("agent-b"));
    }

    #[tokio::test]
    async fn team_auth_store_reserves_router_service_agent_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));

        let err = store
            .create_key(ROUTER_SERVICE_AGENT_ID)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("系统保留身份"));
    }

    #[tokio::test]
    async fn ensure_router_service_key_creates_private_key_and_hides_public_row() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));
        let private_key_path = dir
            .path()
            .join("maintainer")
            .join("service_keys")
            .join("router_service_acn_key");

        let service_key = store
            .ensure_router_service_key(&private_key_path)
            .await
            .unwrap();

        assert_eq!(service_key.agent_id, agent_id(ROUTER_SERVICE_AGENT_ID));
        assert!(service_key.acn_key.starts_with("acn_"));
        assert_eq!(
            tokio::fs::read_to_string(&private_key_path).await.unwrap(),
            service_key.acn_key
        );
        assert!(store.list_public().await.unwrap().is_empty());

        let ledger = store.load_or_default().await.unwrap();
        let service_rows = ledger
            .auth
            .api_keys
            .iter()
            .filter(|row| row.agent_id == agent_id(ROUTER_SERVICE_AGENT_ID))
            .collect::<Vec<_>>();
        assert_eq!(service_rows.len(), 1);
        assert_eq!(service_rows[0].status, AuthKeyStatus::Active);
        assert_eq!(
            service_rows[0].key_hash,
            format!("sha256:{}", sha256_hex(&service_key.acn_key))
        );
    }

    #[tokio::test]
    async fn private_service_key_tmp_is_removed_when_rename_fails() {
        let dir = tempfile::tempdir().unwrap();
        let private_key_path = dir.path().join("router_service_acn_key");
        tokio::fs::create_dir(&private_key_path).await.unwrap();

        let err = write_private_service_key(&private_key_path, "acn_secret")
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("router_service_acn_key"));
        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.contains(".tmp."),
                "private service key tmp was left behind: {name}"
            );
        }
    }

    #[tokio::test]
    async fn ensure_router_service_key_reuses_matching_private_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));
        let private_key_path = dir
            .path()
            .join("maintainer")
            .join("service_keys")
            .join("router_service_acn_key");

        let first = store
            .ensure_router_service_key(&private_key_path)
            .await
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            tokio::fs::set_permissions(&private_key_path, std::fs::Permissions::from_mode(0o644))
                .await
                .unwrap();
        }
        let second = store
            .ensure_router_service_key(&private_key_path)
            .await
            .unwrap();

        assert_eq!(first.acn_key, second.acn_key);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = tokio::fs::metadata(&private_key_path)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        let ledger = store.load_or_default().await.unwrap();
        let active_service_rows = ledger
            .auth
            .api_keys
            .iter()
            .filter(|row| {
                row.agent_id == agent_id(ROUTER_SERVICE_AGENT_ID)
                    && row.status == AuthKeyStatus::Active
            })
            .count();
        assert_eq!(active_service_rows, 1);
    }

    #[tokio::test]
    async fn ensure_router_service_key_replaces_mismatched_private_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));
        let private_key_path = dir
            .path()
            .join("maintainer")
            .join("service_keys")
            .join("router_service_acn_key");
        let first = store
            .ensure_router_service_key(&private_key_path)
            .await
            .unwrap();
        tokio::fs::write(&private_key_path, b"wrong-key")
            .await
            .unwrap();

        let second = store
            .ensure_router_service_key(&private_key_path)
            .await
            .unwrap();

        assert_ne!(first.acn_key, second.acn_key);
        let ledger = store.load_or_default().await.unwrap();
        let active_service_rows = ledger
            .auth
            .api_keys
            .iter()
            .filter(|row| {
                row.agent_id == agent_id(ROUTER_SERVICE_AGENT_ID)
                    && row.status == AuthKeyStatus::Active
            })
            .collect::<Vec<_>>();
        assert_eq!(active_service_rows.len(), 1);
        assert_eq!(
            active_service_rows[0].key_hash,
            format!("sha256:{}", sha256_hex(&second.acn_key))
        );
        assert_eq!(
            tokio::fs::read_to_string(&private_key_path).await.unwrap(),
            second.acn_key
        );
    }

    #[tokio::test]
    async fn ensure_router_service_key_replaces_duplicate_active_service_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = TeamAuthStore::new(dir.path().join("maintainer").join("auth_keys.yaml"));
        let private_key_path = dir
            .path()
            .join("maintainer")
            .join("service_keys")
            .join("router_service_acn_key");
        tokio::fs::create_dir_all(private_key_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&private_key_path, b"existing-key")
            .await
            .unwrap();
        let mut matching = active_row(ROUTER_SERVICE_AGENT_ID, "existing-key");
        matching.key_id = "key_router_service_matching".into();
        let mut duplicate = active_row(ROUTER_SERVICE_AGENT_ID, "other-key");
        duplicate.key_id = "key_router_service_duplicate".into();
        write_yaml_atomic(
            store.path(),
            &AuthKeyLedger {
                auth: AuthConfig {
                    enabled: true,
                    api_keys: vec![matching, duplicate],
                },
            },
        )
        .await
        .unwrap();

        let service_key = store
            .ensure_router_service_key(&private_key_path)
            .await
            .unwrap();

        assert_ne!(service_key.acn_key, "existing-key");
        let ledger = store.load_or_default().await.unwrap();
        let service_rows = ledger
            .auth
            .api_keys
            .iter()
            .filter(|row| row.agent_id == agent_id(ROUTER_SERVICE_AGENT_ID))
            .collect::<Vec<_>>();
        assert_eq!(
            service_rows
                .iter()
                .filter(|row| row.status == AuthKeyStatus::Active)
                .count(),
            1
        );
        assert_eq!(
            service_rows
                .iter()
                .filter(|row| row.status == AuthKeyStatus::Revoked)
                .count(),
            2
        );
        assert_eq!(
            tokio::fs::read_to_string(&private_key_path).await.unwrap(),
            service_key.acn_key
        );
    }
}
