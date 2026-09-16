// 覆盖 Cookie 生命周期以及管理员会话与 Agent 团队鉴权的隔离。
use super::tests::{admin_auth_state, basic_auth};
use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use serde_json::{json, Value};
use tower::ServiceExt;

fn app(state: AppState) -> AxumRouter {
    build_app()
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::admin_auth_middleware,
        ))
        .with_state(state)
}

async fn login(app: &AxumRouter, old: &str) -> (String, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin-auth/login")
                .header("x-acn-workbench", "1")
                .header(header::COOKIE, old)
                .header(header::AUTHORIZATION, basic_auth("admin", "secret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .to_owned();
    for attribute in ["HttpOnly", "SameSite=Strict", "Max-Age=", "Path=/"] {
        assert!(cookie.contains(attribute));
    }
    assert!(!cookie.contains("; Secure"));
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.get("authorization").is_none());
    (
        cookie.split(';').next().unwrap().to_owned(),
        body["id"].as_str().unwrap().to_owned(),
    )
}

async fn get(app: &AxumRouter, path: &str, cookie: &str, id: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header(header::COOKIE, cookie)
                .header("x-acn-workbench", "1")
                .header("x-acn-admin-session", id)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn cookie_session_restores_rotates_revokes_and_preserves_new_login() {
    let team = tempfile::tempdir().unwrap();
    let (state, _) = admin_auth_state(&team);
    let app = app(state);
    let (old_cookie, old_id) = login(&app, "").await;
    assert_eq!(
        get(&app, "/api/overview", &old_cookie, &old_id)
            .await
            .status(),
        StatusCode::OK
    );
    // 新标签或浏览器重启后只有 Cookie 也能恢复界面元数据。
    let status = get(&app, "/api/admin-auth/status", &old_cookie, "").await;
    let restored: Value =
        serde_json::from_slice(&to_bytes(status.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(restored["session"]["id"], old_id);
    let (cookie, id) = login(&app, &old_cookie).await;
    assert_ne!(old_id, id);
    let rejected = get(&app, "/api/overview", &old_cookie, &old_id).await;
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    assert!(!rejected.headers().contains_key(header::SET_COOKIE));
    assert_eq!(
        get(&app, "/api/overview", &cookie, &old_id).await.status(),
        StatusCode::UNAUTHORIZED
    );
    // 迟到注销带旧标识时不能撤销浏览器已经替换的新 Cookie。
    let stale_logout = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin-auth/logout")
                .header(header::COOKIE, &cookie)
                .header("x-acn-workbench", "1")
                .header("x-acn-admin-session", &old_id)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale_logout.status(), StatusCode::CONFLICT);
    assert!(!stale_logout.headers().contains_key(header::SET_COOKIE));
    assert_eq!(
        get(&app, "/api/overview", &cookie, &id).await.status(),
        StatusCode::OK
    );
    let logout = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin-auth/logout")
                .header(header::COOKIE, &cookie)
                .header("x-acn-workbench", "1")
                .header("x-acn-admin-session", &id)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert!(logout.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
    assert_eq!(
        get(&app, "/api/overview", &cookie, &id).await.status(),
        StatusCode::UNAUTHORIZED
    );
    let (fresh, fresh_id) = login(&app, "").await;
    assert_eq!(
        get(&app, "/api/overview", &fresh, &fresh_id).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn cookie_session_rejects_cross_origin_and_missing_workbench_header() {
    let team = tempfile::tempdir().unwrap();
    let (state, _) = admin_auth_state(&team);
    let app = app(state);
    let (cookie, id) = login(&app, "").await;
    for site in ["cross-site", "same-site"] {
        for path in ["/api/admin-auth/login", "/api/admin-auth/logout"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header(header::COOKIE, &cookie)
                        .header("x-acn-workbench", "1")
                        .header("sec-fetch-site", site)
                        .header("x-acn-admin-session", &id)
                        .header(header::AUTHORIZATION, basic_auth("admin", "secret"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/overview")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cookie_session_cannot_replace_agent_team_key_or_authorize_another_agent() {
    let team = tempfile::tempdir().unwrap();
    let (mut state, _) = admin_auth_state(&team);
    let key = state.auth_store.create_key("agent-a").await.unwrap();
    state.auth = AuthVerifier::from_key_store_path(state.auth_store.path(), true)
        .await
        .unwrap();
    let app = app(state);
    let (cookie, _) = login(&app, "").await;
    for (agent, supplied_key, expected) in [
        ("agent-a", key.response.acn_key.as_str(), StatusCode::OK),
        ("agent-a", "invalid", StatusCode::UNAUTHORIZED),
        (
            "agent-b",
            key.response.acn_key.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let claim_id = crate::claim::ClaimId::random();
        let dispute_id = crate::claim::DisputeId::random();
        let now = crate::time::now_seconds();
        let payloads = [
            ("/inbox/pull", json!({"agent_id":agent})),
            ("/inbox/ack", json!({"agent_id":agent, "inbox_ids":[]})),
            (
                "/claims/upload",
                json!({"id":claim_id,"name":"example","statement":"example",
                "scope":"example","holder":agent,"confidence":"medium","status":"active",
                "created_at":now,"evidence_summary":"example"}),
            ),
            (
                "/disputes/report",
                json!({"id":dispute_id,"name":"example","reporter_agent_id":agent,
                "claims":[claim_id],"summary":"example","status":"open","created_at":now}),
            ),
        ];
        for cookie in ["", cookie.as_str()] {
            for (path, data) in &payloads {
                let response = app.clone().oneshot(Request::builder().method("POST").uri(*path)
                    .header(header::CONTENT_TYPE, "application/json").header(header::COOKIE, cookie)
                    .body(Body::from(json!({"auth":{"agent_id":agent,"acn_key":supplied_key},"data":data}).to_string())).unwrap()).await.unwrap();
                assert_eq!(response.status(), expected, "{path}");
            }
        }
    }
    // 团队 key 无法授权管理 API。
    assert_eq!(
        get(&app, "/api/overview", "", &key.response.acn_key)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn cookie_session_expires_and_accepts_relogin() {
    let team = tempfile::tempdir().unwrap();
    let (mut state, _) = admin_auth_state(&team);
    state.admin_auth = auth::AdminAuth::from_config(&crate::config::MaintainerAdminAuthConfig {
        enabled: true,
        password: Some("secret".to_string()),
        session_ttl_secs: 1,
        ..Default::default()
    })
    .unwrap();
    let app = app(state);
    let (cookie, id) = login(&app, "").await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let rejected = get(&app, "/api/overview", &cookie, &id).await;
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    assert!(!rejected.headers().contains_key(header::SET_COOKIE));
    let status = get(&app, "/api/admin-auth/status", &cookie, "").await;
    let body: Value =
        serde_json::from_slice(&to_bytes(status.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.get("session").is_none());
    let (fresh, fresh_id) = login(&app, &cookie).await;
    assert_eq!(
        get(&app, "/api/overview", &fresh, &fresh_id).await.status(),
        StatusCode::OK
    );
}
