use axum::body::{to_bytes, Body};
use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use std::net::SocketAddr;

use crate::maintainer::history::{fresh_record_id, HttpAuditRecord};

use super::state::AppState;

const MAX_AUDIT_BODY_BYTES: usize = 200_000;
const MAX_AUDIT_REQUEST_READ_BYTES: usize = 1_000_000;
const MAX_AUDIT_RESPONSE_READ_BYTES: usize = 5_000_000;
const AUDIT_BODY_TRUNCATED_SUFFIX: &str = "\n...<truncated for audit>";

pub async fn audit_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    if !should_audit(&method, &path) {
        return next.run(request).await;
    }

    let started_at = Utc::now();
    let source_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip().to_string());

    let (parts, body) = request.into_parts();
    let request_body_bytes = match to_bytes(body, MAX_AUDIT_REQUEST_READ_BYTES).await {
        Ok(bytes) => bytes,
        Err(err) => {
            log::warn!(target: "maintainer_http_server", "读取 HTTP request body 失败: {err:#}");
            return Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(Body::from("request body too large for audit"))
                .unwrap_or_else(|_| Response::new(Body::empty()));
        }
    };
    // 持久化前抹掉团队凭据：审计日志会在 dashboard 上可见，acn_key 不应落盘。
    // rebuilt_request 仍带原始 bytes，handler 照常拿到真实 key 做校验。
    let request_body = redact_audit_body(&request_body_bytes);
    let rebuilt_request = Request::from_parts(parts, Body::from(request_body_bytes.clone()));

    let response = next.run(rebuilt_request).await;
    let status_code = response.status().as_u16();
    let (parts, body) = response.into_parts();
    let response_body_bytes = match to_bytes(body, MAX_AUDIT_RESPONSE_READ_BYTES).await {
        Ok(bytes) => bytes,
        Err(err) => {
            log::warn!(target: "maintainer_http_server", "读取 HTTP response body 失败: {err:#}");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("failed to read response body for audit"))
                .unwrap_or_else(|_| Response::new(Body::empty()));
        }
    };
    let response_body = redact_audit_body(&response_body_bytes);

    let duration_ms = Utc::now()
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64;
    let record = HttpAuditRecord {
        audit_id: fresh_record_id("http_audit"),
        occurred_at: started_at,
        method: method.clone(),
        path: path.clone(),
        status_code,
        duration_ms,
        source_ip,
        request_body: request_body.clone(),
        response_body: response_body.clone(),
        resource_id: infer_resource_id(&path, &request_body, &response_body),
        summary: format!("{method} {path} -> {status_code}"),
    };
    if let Err(err) = state.history_store.write_http_audit_log(&record).await {
        log::warn!(target: "maintainer_http_server", "写 HTTP 审计失败: {err:#}");
    }

    Response::from_parts(parts, Body::from(response_body_bytes))
}

fn should_audit(method: &str, path: &str) -> bool {
    if method != "POST" {
        return false;
    }
    matches!(
        path,
        "/inbox/pull"
            | "/inbox/ack"
            | "/claims/upload"
            | "/disputes/report"
            | "/policies/policy-update"
            | "/policies/claim-update-suggestion"
            | "/policies/policy-deprecation"
            | "/maintenance/sweep"
            | "/api/router-query"
            | "/api/team-auth/keys"
    ) || (path.starts_with("/disputes/") && path.ends_with("/resolve"))
        || (path.starts_with("/api/team-auth/keys/") && path.ends_with("/revoke"))
}

fn infer_resource_id(path: &str, request_body: &str, response_body: &str) -> Option<String> {
    if let Some(id) = path
        .strip_prefix("/disputes/")
        .and_then(|rest| rest.strip_suffix("/resolve"))
    {
        return Some(id.to_string());
    }
    extract_json_string(request_body, "policy_id")
        .or_else(|| extract_json_string(request_body, "id"))
        .or_else(|| extract_json_string(response_body, "id"))
}

/// 持久化审计 body 前递归抹掉 `acn_key` 字段；handler / client 仍拿原始 body。
/// JSON 解析失败时继续做文本兜底脱敏，避免畸形请求把 body 内 key 原样落盘。
fn redact_audit_body(bytes: &[u8]) -> String {
    if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        if redact_acn_key_fields(&mut value) {
            if let Ok(serialized) = serde_json::to_string(&value) {
                return audit_body_text(serialized.as_bytes());
            }
        }
    }
    let text = String::from_utf8_lossy(bytes);
    if let Some(redacted) = redact_acn_key_text(&text) {
        return audit_body_text(redacted.as_bytes());
    }
    audit_body_text(bytes)
}

fn redact_acn_key_fields(value: &mut serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            let mut changed = false;
            for (key, value) in map.iter_mut() {
                if key == "acn_key" {
                    *value = serde_json::Value::String("<redacted>".to_string());
                    changed = true;
                } else if redact_acn_key_fields(value) {
                    changed = true;
                }
            }
            changed
        }
        serde_json::Value::Array(items) => {
            let mut changed = false;
            for item in items {
                if redact_acn_key_fields(item) {
                    changed = true;
                }
            }
            changed
        }
        _ => false,
    }
}

fn audit_body_text(bytes: &[u8]) -> String {
    if bytes.len() <= MAX_AUDIT_BODY_BYTES {
        return String::from_utf8_lossy(bytes).into_owned();
    }

    let mut text = String::from_utf8_lossy(&bytes[..MAX_AUDIT_BODY_BYTES]).into_owned();
    text.push_str(AUDIT_BODY_TRUNCATED_SUFFIX);
    text
}

fn redact_acn_key_text(text: &str) -> Option<String> {
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0usize;
    let mut changed = false;
    while let Some(relative_key_start) = text[cursor..].find("\"acn_key\"") {
        let key_start = cursor + relative_key_start;
        let after_key = key_start + "\"acn_key\"".len();
        let Some(relative_colon) = text[after_key..].find(':') else {
            break;
        };
        let mut value_start = after_key + relative_colon + 1;
        let bytes = text.as_bytes();
        while value_start < bytes.len() && bytes[value_start].is_ascii_whitespace() {
            value_start += 1;
        }
        if value_start >= bytes.len() {
            break;
        }

        output.push_str(&text[cursor..value_start]);
        if bytes[value_start] == b'"' {
            output.push('"');
            output.push_str("<redacted>");
            let mut index = value_start + 1;
            let mut escaped = false;
            while index < bytes.len() {
                let byte = bytes[index];
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    output.push('"');
                    cursor = index + 1;
                    changed = true;
                    break;
                }
                index += 1;
            }
            if cursor != index + 1 {
                cursor = bytes.len();
                changed = true;
            }
        } else {
            output.push_str("\"<redacted>\"");
            let mut index = value_start;
            while index < bytes.len() && !matches!(bytes[index], b',' | b'}' | b']') {
                index += 1;
            }
            cursor = index;
            changed = true;
        }
    }
    if !changed {
        return None;
    }
    output.push_str(&text[cursor..]);
    Some(output)
}

fn extract_json_string(body: &str, key: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value.get(key)?.as_str().map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::extract::State as AxumState;
    use axum::middleware;
    use axum::routing::post;
    use axum::Router as AxumRouter;
    use tower::ServiceExt;

    use super::*;
    use crate::maintainer::Maintainer;
    use crate::router::Router;

    fn build_state() -> (AppState, tempfile::TempDir) {
        let team = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            chrono::Duration::days(7),
            chrono::Duration::days(30),
            4,
        ));
        (
            AppState {
                history_store: maintainer.history_store().clone(),
                maintainer,
                router_client: Arc::new(Router::new(team.path().to_path_buf())),
                auth: crate::auth::AuthVerifier::disabled(),
                auth_store: crate::auth::TeamAuthStore::new(
                    crate::storage::paths::team_store_auth_keys_path(team.path()),
                ),
                maintainer_team_auth_enabled: true,
                router_team_auth_enabled: false,
                frontend_dist_dir: PathBuf::from("frontend/maintainer-workbench/dist"),
                sweep_scheduler: crate::maintainer::server::SweepScheduler::new(86_400),
                admin_auth: None,
            },
            team,
        )
    }

    async fn echo_body(AxumState(_state): AxumState<AppState>, request: Request) -> Response {
        let body = request.into_body();
        let body = to_bytes(body, usize::MAX).await.unwrap();
        Response::new(Body::from(body))
    }

    async fn large_response(AxumState(_state): AxumState<AppState>) -> Response {
        Response::new(Body::from("y".repeat(MAX_AUDIT_BODY_BYTES + 32)))
    }

    async fn response_above_request_cap(AxumState(_state): AxumState<AppState>) -> Response {
        Response::new(Body::from("z".repeat(MAX_AUDIT_REQUEST_READ_BYTES + 1)))
    }

    async fn team_auth_key_response(AxumState(_state): AxumState<AppState>) -> Response {
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":{"key_id":"key_test","agent_id":"agent-a","generated_time":"2026-06-26T12:00:00Z","status":"active"},"acn_key":"acn_response_secret"}"#,
            ))
            .unwrap()
    }

    #[test]
    fn should_audit_only_mutating_or_operator_routes() {
        assert!(should_audit("POST", "/inbox/pull"));
        assert!(should_audit("POST", "/inbox/ack"));
        assert!(should_audit("POST", "/maintenance/sweep"));
        assert!(should_audit("POST", "/api/router-query"));
        assert!(should_audit("POST", "/api/team-auth/keys"));
        assert!(should_audit(
            "POST",
            "/api/team-auth/keys/key_abcd1234/revoke"
        ));
        assert!(should_audit("POST", "/disputes/dispute_abcd1234/resolve"));
        assert!(!should_audit("GET", "/api/audits"));
        assert!(!should_audit("GET", "/assets/index.js"));
        assert!(!should_audit("GET", "/api/overview"));
    }

    #[tokio::test]
    async fn audit_keeps_full_request_body_but_truncates_log() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/claims/upload", post(echo_body))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let body = "x".repeat(MAX_AUDIT_BODY_BYTES + 64);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/upload")
                    .body(Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let echoed = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&echoed), body);

        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert!(audits[0]
            .request_body
            .ends_with(AUDIT_BODY_TRUNCATED_SUFFIX));
        assert!(audits[0].request_body.len() < body.len());
    }

    #[tokio::test]
    async fn audit_rejects_request_body_above_read_cap() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/claims/upload", post(echo_body))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/upload")
                    .body(Body::from("x".repeat(MAX_AUDIT_REQUEST_READ_BYTES + 1)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert!(audits.is_empty());
    }

    #[tokio::test]
    async fn audit_never_persists_authorization_header() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/claims/upload", post(echo_body))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/upload")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        "Bearer super-secret-key-value",
                    )
                    .body(Body::from(r#"{"holder":"agent-a"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        // 审计只记 body / path / status，不读 Authorization header——这里做反向断言锁死该保证。
        let serialized = serde_json::to_string(&audits[0]).unwrap();
        assert!(
            !serialized.contains("super-secret-key-value"),
            "审计记录泄漏了 bearer key: {serialized}"
        );
        assert!(!serialized.to_lowercase().contains("authorization"));
    }

    #[tokio::test]
    async fn audit_redacts_team_acn_key_from_request_body() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/claims/upload", post(echo_body))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/upload")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"auth":{"agent_id":"agent-a","acn_key":"acn_supersecretvalue"},"data":{"holder":"agent-a"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        // handler 仍拿到原始 body（含真实 key）——echo 把原始 body 回放出来。
        let echoed = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&echoed).contains("acn_supersecretvalue"));

        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert!(
            !audits[0].request_body.contains("acn_supersecretvalue"),
            "审计落盘了明文 acn_key: {}",
            audits[0].request_body
        );
        assert!(audits[0].request_body.contains("<redacted>"));
        // agent_id 与 data 仍保留，审计可读性不受影响。
        assert!(audits[0].request_body.contains("agent-a"));
    }

    #[test]
    fn audit_redacts_acn_key_from_malformed_json_text() {
        let body = br#"{"auth":{"agent_id":"agent-a","acn_key":"broken_secret"},"data": "#;
        let redacted = redact_audit_body(body);

        assert!(!redacted.contains("broken_secret"));
        assert!(redacted.contains("\"acn_key\":\"<redacted>\""));
    }

    #[test]
    fn audit_redacts_acn_key_field_regardless_of_json_value_type() {
        let body = br#"{"auth":{"agent_id":"agent-a","acn_key":["array_secret"]},"nested":{"acn_key":{"v":"object_secret"}}}"#;
        let redacted = redact_audit_body(body);

        assert!(!redacted.contains("array_secret"));
        assert!(!redacted.contains("object_secret"));
        assert_eq!(redacted.matches("\"acn_key\":\"<redacted>\"").count(), 2);
    }

    #[tokio::test]
    async fn audit_redacts_team_acn_key_from_response_body() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/api/team-auth/keys", post(team_auth_key_response))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/team-auth/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"agent_id":"agent-a"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let echoed = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&echoed).contains("acn_response_secret"));

        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert!(
            !audits[0].response_body.contains("acn_response_secret"),
            "审计落盘了响应里的明文 acn_key: {}",
            audits[0].response_body
        );
        assert!(audits[0].response_body.contains("<redacted>"));
    }

    #[tokio::test]
    async fn audit_keeps_full_response_body_but_truncates_log() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/maintenance/sweep", post(large_response))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/maintenance/sweep")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let echoed = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(echoed.len(), MAX_AUDIT_BODY_BYTES + 32);

        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert!(audits[0]
            .response_body
            .ends_with(AUDIT_BODY_TRUNCATED_SUFFIX));
        assert!(audits[0].response_body.len() < echoed.len());
    }

    #[tokio::test]
    async fn audit_allows_response_above_unchanged_request_cap() {
        let (state, _team) = build_state();
        let app = AxumRouter::new()
            .route("/maintenance/sweep", post(response_above_request_cap))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/maintenance/sweep")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let echoed = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(echoed.len(), MAX_AUDIT_REQUEST_READ_BYTES + 1);
        assert!(echoed.len() < MAX_AUDIT_RESPONSE_READ_BYTES);
        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert!(audits[0]
            .response_body
            .ends_with(AUDIT_BODY_TRUNCATED_SUFFIX));
    }
}
