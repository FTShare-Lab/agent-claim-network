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
}

#[derive(Clone)]
pub struct AdminAuth {
    username: String,
    password: String,
}

impl AdminAuth {
    pub fn from_config(cfg: &MaintainerAdminAuthConfig) -> anyhow::Result<Option<Self>> {
        if !cfg.enabled {
            return Ok(None);
        }
        let Some(password) = cfg.password.clone().filter(|password| !password.is_empty()) else {
            anyhow::bail!("maintainer admin auth enabled but password is missing or empty");
        };
        Ok(Some(Self {
            username: cfg.username.clone(),
            password,
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
    if is_authorized(&request, auth) {
        return next.run(request).await;
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
        && path != "/api/admin-auth/status")
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

pub async fn admin_auth_status(State(state): State<AppState>) -> Json<AdminAuthStatus> {
    Json(AdminAuthStatus {
        enabled: state.admin_auth.is_some(),
    })
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
        });
        let err = match result {
            Ok(_) => panic!("enabled admin auth without password should fail"),
            Err(err) => err.to_string(),
        };

        assert!(err.contains("password is missing or empty"));
    }
}
