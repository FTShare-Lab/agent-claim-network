// 管理员 Basic 入口与仅供管理台使用的限时 Cookie 会话；不参与团队鉴权。
use rand::RngCore;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use serde::Serialize;

use crate::config::MaintainerAdminAuthConfig;

use super::state::AppState;

const BASIC_REALM: &str = "Basic realm=\"ACN Maintainer\"";
const WORKBENCH_FETCH_HEADER: &str = "x-acn-workbench";

#[derive(Serialize)]
pub struct AdminAuthStatus {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<AdminSession>,
}

#[derive(Clone)]
pub struct AdminAuth {
    username: String,
    password: String,
    session_ttl_secs: u64,
    cookie_secure: bool,
    sessions: Arc<Mutex<HashMap<String, AdminSession>>>,
}

const COOKIE_NAME: &str = "acn_admin_session";
const SESSION_HEADER: &str = "x-acn-admin-session";

#[derive(Clone, Serialize)]
pub struct AdminSession {
    id: String,
    username: String,
    expires_at: i64,
}

fn random_id() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn cookie_token(request: &Request) -> Option<&str> {
    request
        .headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == COOKIE_NAME).then_some(value))
}

// 自定义头不允许跨源表单构造；不信任来自其他源（含同站子域）的浏览器请求。
fn safe_workbench_request(request: &Request) -> bool {
    is_workbench_fetch(request)
        && request
            .headers()
            .get("sec-fetch-site")
            .is_none_or(|value| value == "same-origin" || value == "none")
}

impl AdminAuth {
    async fn session(&self, token: Option<String>) -> Option<AdminSession> {
        let token = token?;
        let mut sessions = self.sessions.lock().await;
        sessions.retain(|_, session| session.expires_at > chrono::Utc::now().timestamp());
        sessions.get(&token).cloned()
    }

    fn cookie(&self, token: &str, max_age: u64) -> String {
        format!(
            "{COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}{}",
            if self.cookie_secure { "; Secure" } else { "" }
        )
    }
}

impl AdminAuth {
    pub fn from_config(cfg: &MaintainerAdminAuthConfig) -> anyhow::Result<Option<Self>> {
        if !cfg.enabled {
            return Ok(None);
        }
        let Some(password) = cfg.password.clone().filter(|password| !password.is_empty()) else {
            anyhow::bail!("maintainer admin auth enabled but password is missing or empty");
        };
        if cfg.session_ttl_secs == 0 || cfg.session_ttl_secs > 365 * 24 * 60 * 60 {
            anyhow::bail!("admin session_ttl_secs must be between 1 and 31536000");
        }
        Ok(Some(Self {
            username: cfg.username.clone(),
            password,
            session_ttl_secs: cfg.session_ttl_secs,
            cookie_secure: cfg.cookie_secure,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }))
    }
}

pub async fn admin_auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let Some(auth) = state.admin_auth.as_ref() else {
        return next.run(request).await;
    };
    if !requires_admin_auth(path) {
        return next.run(request).await;
    }
    let cookie_authorized = if safe_workbench_request(&request) {
        auth.session(cookie_token(&request).map(str::to_owned))
            .await
            .is_some_and(|session| {
                request
                    .headers()
                    .get(SESSION_HEADER)
                    .and_then(|value| value.to_str().ok())
                    == Some(session.id.as_str())
            })
    } else {
        false
    };
    if cookie_authorized || (!is_workbench_fetch(&request) && is_authorized(&request, auth)) {
        let mut response = next.run(request).await;
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        );
        return response;
    }
    unauthorized_response(&request)
}

fn requires_admin_auth(path: &str) -> bool {
    if matches!(
        path,
        "/health"
            | "/inbox/pull"
            | "/inbox/ack"
            | "/claims/upload"
            | "/disputes/report"
            | "/"
            | "/login"
            | "/favicon.svg"
    ) || path.starts_with("/assets/")
        || path.starts_with("/app/")
        || path.starts_with("/docs/")
        || ["/app", "/favicon.svg"].contains(&path)
    {
        return false;
    }

    matches!(
        path,
        "/status" | "/actions" | "/send_log" | "/outbox" | "/maintenance/sweep"
    ) || (path.starts_with("/api/")
        && path != "/api/admin-auth/check"
        && path != "/api/admin-auth/status"
        && path != "/api/admin-auth/login"
        && path != "/api/admin-auth/logout")
        || path.starts_with("/policies/")
        || (path.starts_with("/disputes/") && path.ends_with("/resolve"))
}

fn is_authorized(request: &Request, auth: &AdminAuth) -> bool {
    let Some(value) = request.headers().get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(encoded) = value
        .split_once(' ')
        .and_then(|(scheme, encoded)| scheme.eq_ignore_ascii_case("Basic").then_some(encoded))
    else {
        return false;
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(credentials) = String::from_utf8(decoded) else {
        return false;
    };
    credentials == format!("{}:{}", auth.username, auth.password)
}

pub async fn check_admin_auth(State(state): State<AppState>, request: Request) -> Response {
    let Some(auth) = state.admin_auth.as_ref() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    if is_authorized(&request, auth) {
        return StatusCode::NO_CONTENT.into_response();
    }
    (StatusCode::UNAUTHORIZED, "invalid admin credentials").into_response()
}

pub async fn admin_auth_status(State(state): State<AppState>, request: Request) -> Response {
    let session = if safe_workbench_request(&request) {
        match state.admin_auth.as_ref() {
            Some(auth) => {
                auth.session(cookie_token(&request).map(str::to_owned))
                    .await
            }
            None => None,
        }
    } else {
        None
    };
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(AdminAuthStatus {
            enabled: state.admin_auth.is_some(),
            session,
        }),
    )
        .into_response()
}

pub async fn login_admin(State(state): State<AppState>, request: Request) -> Response {
    if !safe_workbench_request(&request) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(auth) = state.admin_auth.as_ref() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    if !is_authorized(&request, auth) {
        return unauthorized_response(&request);
    }
    let now = chrono::Utc::now().timestamp();
    // 配置已限制为一年以内，可安全转换并加到当前秒数。
    let Ok(ttl) = i64::try_from(auth.session_ttl_secs) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let session = AdminSession {
        id: random_id(),
        username: auth.username.clone(),
        expires_at: now + ttl,
    };
    let token = random_id();
    let mut sessions = auth.sessions.lock().await;
    sessions.retain(|_, session| session.expires_at > now);
    if let Some(old) = cookie_token(&request) {
        sessions.remove(old);
    }
    if sessions.len() >= 1024 {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    sessions.insert(token.clone(), session.clone());
    (
        [
            (
                header::SET_COOKIE,
                auth.cookie(&token, auth.session_ttl_secs),
            ),
            (header::CACHE_CONTROL, "no-store".to_string()),
        ],
        Json(session),
    )
        .into_response()
}

pub async fn logout_admin(State(state): State<AppState>, request: Request) -> Response {
    if !safe_workbench_request(&request) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(auth) = state.admin_auth.as_ref() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    let mut sessions = auth.sessions.lock().await;
    if let Some(token) = cookie_token(&request) {
        if let Some(session) = sessions.get(token) {
            if request
                .headers()
                .get(SESSION_HEADER)
                .and_then(|value| value.to_str().ok())
                != Some(session.id.as_str())
            {
                return StatusCode::CONFLICT.into_response();
            }
        }
        sessions.remove(token);
    }
    (
        StatusCode::NO_CONTENT,
        [
            (header::SET_COOKIE, auth.cookie("", 0)),
            (header::CACHE_CONTROL, "no-store".to_string()),
        ],
    )
        .into_response()
}

fn unauthorized_response(request: &Request) -> Response {
    if is_workbench_fetch(request) {
        return (StatusCode::UNAUTHORIZED, "admin auth required").into_response();
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, BASIC_REALM)],
        "admin auth required",
    )
        .into_response()
}

fn is_workbench_fetch(request: &Request) -> bool {
    request
        .headers()
        .get(WORKBENCH_FETCH_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some("1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_secure_can_be_enabled_explicitly_for_https_only_deployments() {
        assert!(!MaintainerAdminAuthConfig::default().cookie_secure);
        for secure in [false, true] {
            let auth = AdminAuth::from_config(&MaintainerAdminAuthConfig {
                enabled: true,
                password: Some("test-password".to_string()),
                cookie_secure: secure,
                ..Default::default()
            })
            .unwrap()
            .unwrap();
            let cookie = auth.cookie("test-token", 60);
            assert_eq!(cookie.contains("; Secure"), secure);
            assert!(cookie.contains("HttpOnly; SameSite=Strict; Max-Age=60"));
        }
    }

    #[test]
    fn admin_auth_skips_agent_and_health_paths() {
        for path in [
            "/health",
            "/inbox/pull",
            "/inbox/ack",
            "/claims/upload",
            "/disputes/report",
        ] {
            assert!(!requires_admin_auth(path));
        }
    }

    #[test]
    fn admin_auth_leaves_spa_shell_public_but_guards_management_endpoints() {
        for path in [
            "/",
            "/login",
            "/assets/index.js",
            "/api/admin-auth/status",
            "/policies",
            "/claims",
        ] {
            assert!(!requires_admin_auth(path));
        }
        for path in [
            "/api/overview",
            "/status",
            "/actions",
            "/send_log",
            "/outbox",
            "/policies/policy-update",
            "/maintenance/sweep",
            "/disputes/dispute-a/resolve",
        ] {
            assert!(requires_admin_auth(path));
        }
    }

    #[test]
    fn admin_auth_config_rejects_missing_password_when_enabled() {
        let result = AdminAuth::from_config(&MaintainerAdminAuthConfig {
            enabled: true,
            username: "admin".to_string(),
            password_env: "TEST_ADMIN_PASSWORD".to_string(),
            password: None,
            ..Default::default()
        });
        let err = match result {
            Ok(_) => panic!("enabled admin auth without password should fail"),
            Err(err) => err.to_string(),
        };

        assert!(err.contains("password is missing or empty"));
    }
}
