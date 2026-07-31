//! Router 的 HTTP server。
//!
//! daemon 只暴露查询与健康检查；业务逻辑仍委托给 `Router` 本体。

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router as AxumRouter};
use serde::{Deserialize, Serialize};

use crate::auth::{AuthEnvelope, AuthHttpError, AuthRequest, AuthVerifier, TeamAuthStore};
use crate::storage::paths;

use super::service::Router;
use super::traits::{AgentQuery, RouterClient, RouterQueryResult};
use super::ScopesOverviewSnapshot;

#[derive(Clone)]
struct RouterState {
    router: Arc<Router>,
    auth: AuthVerifier,
    auth_store: TeamAuthStore,
}

pub async fn serve(router: Arc<Router>, listen: &str, auth: AuthVerifier) -> anyhow::Result<()> {
    let app = build_app(router, auth);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    log::info!(target: "router_http_server", "router daemon 监听 {}", listen);
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_app(router: Arc<Router>, auth: AuthVerifier) -> AxumRouter {
    let auth_store = TeamAuthStore::new(paths::team_store_auth_keys_path(router.team_root()));
    AxumRouter::new()
        .route("/health", get(health))
        .route("/claims/query", post(query_claims))
        .route("/claims/scopes/overview", post(scopes_overview))
        .with_state(RouterState {
            router,
            auth,
            auth_store,
        })
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn query_claims(
    State(state): State<RouterState>,
    Json(request): Json<AuthRequest<AgentQuery>>,
) -> Result<Json<RouterQueryResult>, (StatusCode, String)> {
    let AuthRequest { auth, data: query } = request;
    require_router_reader(&state, &auth).await?;
    log::info!(
        target: "router_http_server",
        "收到 claim query 请求 scope={:?} semantic_query={:?}",
        query.scope,
        query.semantic_query.as_deref().unwrap_or("")
    );

    match state.router.query(&query).await {
        Ok(result) => {
            log::info!(
                target: "router_http_server",
                "claim query 返回 scope={:?} semantic_query={:?}: candidate_claims={} disputes={}",
                query.scope,
                query.semantic_query.as_deref().unwrap_or(""),
                result.candidate_claims.len(),
                result.disputes.len()
            );
            Ok(Json(result))
        }
        Err(err) => {
            log::warn!(
                target: "router_http_server",
                "claim query 失败 scope={} err={err:#}",
                query.scope
            );
            Err(internal_error(err))
        }
    }
}

async fn scopes_overview(
    State(state): State<RouterState>,
    Json(request): Json<AuthRequest<EmptyData>>,
) -> Result<Json<ScopesOverviewSnapshot>, (StatusCode, String)> {
    let AuthRequest { auth, data: _data } = request;
    require_router_reader(&state, &auth).await?;
    log::info!(target: "router_http_server", "收到 scope overview 请求");

    match state.router.load_scopes_overview().await {
        Ok(result) => {
            let active_claims: usize = result.scopes.iter().map(|scope| scope.active_claims).sum();
            let stale_claims: usize = result.scopes.iter().map(|scope| scope.stale_claims).sum();
            let open_disputes: usize = result.scopes.iter().map(|scope| scope.open_disputes).sum();
            let resolved_disputes: usize = result
                .scopes
                .iter()
                .map(|scope| scope.resolved_disputes)
                .sum();
            log::info!(
                target: "router_http_server",
                "scope overview 返回 scopes={} active_claims={} stale_claims={} open_disputes={} resolved_disputes={}",
                result.scopes.len(),
                active_claims,
                stale_claims,
                open_disputes,
                resolved_disputes
            );
            Ok(Json(result))
        }
        Err(err) => {
            log::warn!(
                target: "router_http_server",
                "scope overview 查询失败 err={err:#}"
            );
            Err(internal_error(err))
        }
    }
}

async fn require_router_reader(
    state: &RouterState,
    auth: &AuthEnvelope,
) -> Result<(), (StatusCode, String)> {
    if state.auth.is_enabled() {
        state
            .auth
            .replace_active_keys_from_store(&state.auth_store, true)
            .await
            .map_err(|err| {
                log::warn!(target: "router_http_server", "刷新团队 auth key store 失败: {err:#}");
                internal_error(err.into())
            })?;
    }
    match state.auth.verify_envelope(Some(auth)) {
        Ok(_) => Ok(()),
        Err(AuthHttpError::Unauthorized) => Err(AuthHttpError::Unauthorized.into_http_response()),
        Err(err) => Err(err.into_http_response()),
    }
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyData {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::claim::{AgentId, Claim, ClaimId, ClaimStatus, Confidence};
    use crate::router::derived_views::RouterDerivedViewsSnapshot;
    use crate::storage::{paths, read_yaml, write_yaml_atomic};

    fn sample_claim(agent: &AgentId) -> Claim {
        Claim {
            id: ClaimId::random(),
            name: "scope_overview".into(),
            statement: "s".into(),
            scope: "agent/session/recap".into(),
            holder: agent.clone(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-05-16T12:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "e".into(),
        }
    }

    #[tokio::test]
    async fn scopes_overview_route_returns_snapshot_and_refreshes_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        let claim = sample_claim(&agent);
        let claim_path = paths::team_store_agent_claims_dir(&team_root, &agent)
            .join(format!("{}.yaml", claim.id));
        write_yaml_atomic(&claim_path, &claim).await.unwrap();

        let app = build_app(
            Arc::new(Router::new(team_root.clone())),
            AuthVerifier::disabled(),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/scopes/overview")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"auth":{"agent_id":"agent-a","acn_key":""},"data":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let snapshot: RouterDerivedViewsSnapshot =
            read_yaml(&paths::team_store_router_derived_views_path(&team_root))
                .await
                .unwrap();
        assert_eq!(snapshot.scopes_overview().scopes.len(), 1);
        assert_eq!(
            snapshot.scopes_overview().scopes[0].latest_claim_created_at,
            claim.created_at
        );
    }

    fn auth_verifier(agent: &str) -> AuthVerifier {
        AuthVerifier::from_config(&crate::auth::AuthConfig {
            enabled: true,
            api_keys: vec![crate::auth::AuthApiKeyConfig {
                key_id: "key_test".into(),
                agent_id: AgentId::new(agent).unwrap(),
                key_hash: format!("sha256:{}", crate::auth::sha256_hex("secret")),
                generated_time: "2026-06-26T12:00:00Z".parse().unwrap(),
                status: crate::auth::AuthKeyStatus::Active,
            }],
        })
        .unwrap()
    }

    async fn write_auth_key(team_root: &std::path::Path, agent: &str, key: &str) {
        write_yaml_atomic(
            &paths::team_store_auth_keys_path(team_root),
            &crate::auth::AuthKeyLedger {
                auth: crate::auth::AuthConfig {
                    enabled: true,
                    api_keys: vec![crate::auth::AuthApiKeyConfig {
                        key_id: "key_test".into(),
                        agent_id: AgentId::new(agent).unwrap(),
                        key_hash: format!("sha256:{}", crate::auth::sha256_hex(key)),
                        generated_time: "2026-06-26T12:00:00Z".parse().unwrap(),
                        status: crate::auth::AuthKeyStatus::Active,
                    }],
                },
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn query_rejects_legacy_body_when_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_app(
            Arc::new(Router::new(dir.path().to_path_buf())),
            auth_verifier("agent-a"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"scope":"scope"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn query_rejects_legacy_body_when_auth_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_app(
            Arc::new(Router::new(dir.path().to_path_buf())),
            AuthVerifier::disabled(),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"scope":"scope"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn query_accepts_agent_envelope_key() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        write_auth_key(&team_root, "agent-a", "secret").await;
        let verifier =
            AuthVerifier::from_key_store_path(&paths::team_store_auth_keys_path(&team_root), true)
                .await
                .unwrap();
        let app = build_app(Arc::new(Router::new(team_root)), verifier);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"auth":{"agent_id":"agent-a","acn_key":"secret"},"data":{"scope":"scope"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn query_lazily_adds_new_active_key_from_store() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let auth_path = paths::team_store_auth_keys_path(&team_root);
        let verifier = AuthVerifier::from_key_store_path(&auth_path, true)
            .await
            .unwrap();
        let app = build_app(Arc::new(Router::new(team_root)), verifier);
        let created = crate::auth::TeamAuthStore::new(auth_path)
            .create_key("agent-a")
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/query")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"auth":{{"agent_id":"agent-a","acn_key":"{}"}},"data":{{"scope":"scope"}}}}"#,
                        created.response.acn_key
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn query_reloads_replacement_key_for_same_agent_and_drops_old_key() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let auth_path = paths::team_store_auth_keys_path(&team_root);
        let store = crate::auth::TeamAuthStore::new(auth_path.clone());
        let old = store.create_key("agent-a").await.unwrap();
        let verifier = AuthVerifier::from_key_store_path(&auth_path, true)
            .await
            .unwrap();
        let app = build_app(Arc::new(Router::new(team_root)), verifier);
        store.revoke_key(&old.response.key.key_id).await.unwrap();
        let new_key = store.create_key("agent-a").await.unwrap();

        let old_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/query")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"auth":{{"agent_id":"agent-a","acn_key":"{}"}},"data":{{"scope":"scope"}}}}"#,
                        old.response.acn_key
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(old_response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/claims/query")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"auth":{{"agent_id":"agent-a","acn_key":"{}"}},"data":{{"scope":"scope"}}}}"#,
                        new_key.response.acn_key
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
