//! M/R 共用的公开存活探针，只报告团队鉴权状态，不执行受保护业务。

use axum::body::Bytes;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::{AuthEnvelope, AuthVerifier, TeamAuthStore};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthRequest {
    #[serde(default)]
    auth: Option<AuthEnvelope>,
    #[serde(default)]
    data: HealthData,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthData {}

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    team_auth_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_passed: Option<bool>,
}

pub async fn respond(team_auth_enabled: bool, store: &TeamAuthStore, body: Bytes) -> Response {
    let request = if body.is_empty() {
        HealthRequest::default()
    } else {
        match serde_json::from_slice::<HealthRequest>(&body) {
            Ok(request) => request,
            Err(_) => {
                // 不回显解析错误或请求内容，避免凭据进入响应与日志。
                return (StatusCode::BAD_REQUEST, "invalid health request envelope")
                    .into_response();
            }
        }
    };
    let HealthRequest { auth, data: _data } = request;
    let auth_passed = match auth {
        None => None,
        Some(_) if !team_auth_enabled => Some(true),
        Some(auth) => {
            // 读取当前团队 key 台账，确保探测能观察到撤销；不替换业务 verifier 的状态。
            let passed = match AuthVerifier::from_key_store_path(store.path(), true).await {
                Ok(verifier) => verifier.verify_envelope(Some(&auth)).is_ok(),
                Err(_) => {
                    log::warn!(target: "health", "health 团队鉴权检查无法读取 key 台账");
                    false
                }
            };
            Some(passed)
        }
    };
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(HealthResponse {
            status: "ok",
            team_auth_enabled,
            auth_passed,
        }),
    )
        .into_response()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    async fn request(app: &axum::Router, method: &str, body: String) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/health")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        if status == StatusCode::OK {
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        }
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    pub async fn assert_health_routes(app: axum::Router, store: &TeamAuthStore, enabled: bool) {
        let key = store.create_key("agent-a").await.unwrap();
        let base = json!({"status":"ok", "team_auth_enabled":enabled});
        for (method, body) in [
            ("GET", ""),
            ("POST", ""),
            ("POST", "{}"),
            ("POST", r#"{"data":{}}"#),
        ] {
            let (status, response) = request(&app, method, body.to_string()).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(response, base);
        }
        for method in ["GET", "POST"] {
            for (agent, supplied_key, passed) in [
                ("agent-a", key.response.acn_key.as_str(), true),
                ("agent-a", "incorrect", !enabled),
                ("agent-b", key.response.acn_key.as_str(), !enabled),
            ] {
                let (status, response) = request(
                    &app,
                    method,
                    json!({"auth":{"agent_id":agent,"acn_key":supplied_key},"data":{}}).to_string(),
                )
                .await;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(
                    response,
                    json!({"status":"ok","team_auth_enabled":enabled,"auth_passed":passed})
                );
                assert!(!response.to_string().contains(&key.response.acn_key));
            }
        }
        store.revoke_key(&key.response.key.key_id).await.unwrap();
        let probe = json!({"auth":{"agent_id":"agent-a","acn_key":key.response.acn_key},"data":{}})
            .to_string();
        let (status, response) = request(&app, "POST", probe.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["auth_passed"], !enabled);
        for body in ["not-json", r#"{"auth":{"agent_id":"agent-a"},"data":{}}"#] {
            assert_eq!(
                request(&app, "POST", body.to_string()).await.0,
                StatusCode::BAD_REQUEST
            );
        }
        // key 台账故障不改变存活结果，也不能使用旧凭据缓存误报成功。
        tokio::fs::write(store.path(), "invalid: [").await.unwrap();
        let (status, response) = request(&app, "GET", String::new()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response, base);
        let (status, response) = request(&app, "POST", probe).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["auth_passed"], !enabled);
    }
}
