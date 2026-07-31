//! Maintainer 的 HTTP server。
//!
//! 把 agent push 面、admin JSON API 与 localhost 前端统一挂在同一个 daemon 上。

#[path = "server/api.rs"]
mod api;
#[path = "server/audit.rs"]
mod audit;
#[path = "server/auth.rs"]
mod auth;
#[path = "server/state.rs"]
mod state;
#[path = "server/ui.rs"]
mod ui;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::middleware;
use axum::routing::{get, post};
use axum::Router as AxumRouter;

use crate::auth::{AuthVerifier, TeamAuthStore};
use crate::config::{Config, DEFAULT_MAINTAINER_FRONTEND_DIST_DIR};
use crate::maintainer::Maintainer;
use crate::router::http_client::HttpRouterClient;
use crate::storage::paths;

use self::state::AppState;
pub use self::state::{SweepScheduleStatus, SweepScheduler};

pub async fn serve(
    maintainer: Arc<Maintainer>,
    cfg: &Config,
    sweep_scheduler: SweepScheduler,
) -> anyhow::Result<()> {
    let frontend_dist_dir = resolve_frontend_dist_dir(&cfg.maintainer.ui.frontend_dist_dir).await;
    let auth_store = TeamAuthStore::new(paths::team_store_auth_keys_path(&cfg.storage.team_root));
    let router_service_key = auth_store
        .ensure_router_service_key(&paths::team_store_router_service_key_path(
            &cfg.storage.team_root,
        ))
        .await?;
    let router_endpoint = router_endpoint_from_listen(&cfg.router.daemon.listen)?;
    let router_client = Arc::new(HttpRouterClient::new_with_auth(
        router_endpoint,
        &cfg.clients.http,
        router_service_key.agent_id,
        Some(router_service_key.acn_key),
    )?);
    let team_auth =
        AuthVerifier::from_key_store_path(auth_store.path(), cfg.maintainer.auth.team.enabled)
            .await?;
    let state = AppState {
        history_store: maintainer.history_store().clone(),
        maintainer,
        router_client,
        auth: team_auth,
        auth_store,
        maintainer_team_auth_enabled: cfg.maintainer.auth.team.enabled,
        router_team_auth_enabled: cfg.router.auth.team.enabled,
        frontend_dist_dir,
        sweep_scheduler,
        admin_auth: auth::AdminAuth::from_config(&cfg.maintainer.auth.admin)?,
    };
    let app = build_app()
        .layer(middleware::from_fn_with_state(
            state.clone(),
            audit::audit_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::admin_auth_middleware,
        ))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.maintainer.daemon.listen).await?;
    log::info!(
        target: "maintainer_http_server",
        "maintainer daemon 监听 {}",
        cfg.maintainer.daemon.listen
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn resolve_frontend_dist_dir(configured: &Path) -> PathBuf {
    let executable = match std::env::current_exe() {
        Ok(path) => Some(tokio::fs::canonicalize(&path).await.unwrap_or(path)),
        Err(_) => None,
    };
    let candidates = frontend_dist_candidates(configured, executable.as_deref());
    for candidate in candidates {
        if tokio::fs::metadata(&candidate)
            .await
            .is_ok_and(|metadata| metadata.is_dir())
        {
            if candidate != configured {
                log::info!(
                    target: "maintainer_http_server",
                    "默认 Workbench 目录不存在，改用随 ACN 安装的静态资源: {}",
                    candidate.display()
                );
            }
            return candidate;
        }
    }
    configured.to_path_buf()
}

fn frontend_dist_candidates(configured: &Path, executable: Option<&Path>) -> Vec<PathBuf> {
    let mut candidates = vec![configured.to_path_buf()];
    if configured != Path::new(DEFAULT_MAINTAINER_FRONTEND_DIST_DIR) {
        return candidates;
    }
    if let Some(packaged) = executable.and_then(packaged_frontend_dist_dir) {
        if packaged != configured {
            candidates.push(packaged);
        }
    }
    candidates
}

fn packaged_frontend_dist_dir(executable: &Path) -> Option<PathBuf> {
    let bin_dir = executable.parent()?;
    let install_prefix = bin_dir.parent()?;
    Some(
        install_prefix
            .join("share")
            .join("acn")
            .join("maintainer-workbench"),
    )
}

pub(crate) fn build_app() -> AxumRouter<AppState> {
    AxumRouter::new()
        .route("/", get(ui::landing))
        .route("/docs/{*path}", get(ui::docs_asset))
        .route("/app", get(ui::spa_entry))
        .route("/app/", get(ui::spa_entry))
        .route("/assets/{*path}", get(ui::asset))
        .route("/app/assets/{*path}", get(ui::asset))
        .route("/favicon.svg", get(ui::favicon))
        .route("/app/favicon.svg", get(ui::favicon))
        .route("/health", get(ui::health))
        .route("/status", get(api::status_snapshot))
        .route("/actions", get(api::actions))
        .route("/send_log", get(api::send_log))
        .route("/outbox", get(api::outbox))
        .route("/inbox/pull", post(api::pull_inbox))
        .route("/inbox/ack", post(api::ack_inbox))
        .route("/claims/upload", post(api::upload_claim))
        .route("/disputes/report", post(api::report_dispute))
        .route("/policies/policy-update", post(api::create_policy))
        .route(
            "/policies/claim-update-suggestion",
            post(api::claim_update_suggestion),
        )
        .route("/policies/policy-deprecation", post(api::deprecate_policy))
        .route("/maintenance/sweep", post(api::run_sweep))
        .route("/disputes/{id}/resolve", post(api::resolve_dispute))
        .route("/api/admin-auth/check", post(auth::check_admin_auth))
        .route("/api/admin-auth/status", get(auth::admin_auth_status))
        .route("/api/overview", get(api::overview))
        .route("/api/disputes", get(api::list_disputes))
        .route("/api/disputes/{id}", get(api::get_dispute))
        .route("/api/claims", get(api::list_claims))
        .route("/api/claims/{id}", get(api::get_claim))
        .route("/api/policies", get(api::list_policies))
        .route("/api/agents", get(api::list_agents))
        .route("/api/sweeps", get(api::list_sweeps))
        .route("/api/audits", get(api::list_http_audits))
        .route("/api/audits/{id}", get(api::get_http_audit))
        .route("/api/team-auth/status", get(api::team_auth_status))
        .route("/api/team-auth/keys", get(api::list_team_auth_keys))
        .route("/api/team-auth/keys", post(api::create_team_auth_key))
        .route(
            "/api/team-auth/keys/{key_id}/revoke",
            post(api::revoke_team_auth_key),
        )
        .route("/api/router-query", post(api::router_query))
        .fallback(get(ui::spa_entry))
}

fn router_endpoint_from_listen(listen: &str) -> anyhow::Result<String> {
    let trimmed = listen.trim();
    let Some((host_raw, port_raw)) = trimmed.rsplit_once(':') else {
        anyhow::bail!("router.daemon.listen 缺少端口: {listen}");
    };
    let port = port_raw.trim();
    if port.is_empty() {
        anyhow::bail!("router.daemon.listen 端口为空: {listen}");
    }
    let host = host_raw
        .trim()
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or_else(|| host_raw.trim());
    let endpoint_host = match host {
        "" | "0.0.0.0" | "::" => "127.0.0.1",
        other => other,
    };
    let formatted_host = if endpoint_host.contains(':') {
        format!("[{endpoint_host}]")
    } else {
        endpoint_host.to_string()
    };
    Ok(format!("http://{formatted_host}:{port}"))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::extract::ConnectInfo;
    use axum::http::{header, Request};
    use base64::Engine;
    use tower::ServiceExt;

    use super::*;
    use crate::config::MaintainerAdminAuthConfig;
    use crate::router::Router;

    #[test]
    fn default_frontend_path_falls_back_beside_install_prefix() {
        let executable = Path::new("/opt/homebrew/Cellar/acn/0.2.0/bin/acn-maintainer");

        assert_eq!(
            frontend_dist_candidates(
                Path::new(DEFAULT_MAINTAINER_FRONTEND_DIST_DIR),
                Some(executable)
            ),
            vec![
                PathBuf::from(DEFAULT_MAINTAINER_FRONTEND_DIST_DIR),
                PathBuf::from("/opt/homebrew/Cellar/acn/0.2.0/share/acn/maintainer-workbench"),
            ]
        );
    }

    #[test]
    fn custom_frontend_path_never_uses_packaged_fallback() {
        let configured = Path::new("/srv/acn/workbench");

        assert_eq!(
            frontend_dist_candidates(
                configured,
                Some(Path::new(
                    "/opt/homebrew/Cellar/acn/0.2.0/bin/acn-maintainer"
                ))
            ),
            vec![configured.to_path_buf()]
        );
    }

    fn admin_auth_state(team: &tempfile::TempDir) -> (AppState, Arc<Maintainer>) {
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            chrono::Duration::days(7),
            chrono::Duration::days(30),
            4,
        ));
        let state = AppState {
            history_store: maintainer.history_store().clone(),
            maintainer: maintainer.clone(),
            router_client: Arc::new(Router::new(team.path().to_path_buf())),
            auth: AuthVerifier::disabled(),
            auth_store: TeamAuthStore::new(paths::team_store_auth_keys_path(team.path())),
            maintainer_team_auth_enabled: true,
            router_team_auth_enabled: false,
            frontend_dist_dir: PathBuf::from("frontend/maintainer-workbench/dist"),
            sweep_scheduler: SweepScheduler::new(86_400),
            admin_auth: auth::AdminAuth::from_config(&MaintainerAdminAuthConfig {
                enabled: true,
                username: "admin".to_string(),
                password_env: "TEST_ADMIN_PASSWORD".to_string(),
                password: Some("secret".to_string()),
            })
            .unwrap(),
        };
        (state, maintainer)
    }

    fn basic_auth(username: &str, password: &str) -> String {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        format!("Basic {encoded}")
    }

    #[test]
    fn router_endpoint_from_listen_maps_wildcard_to_localhost() {
        assert_eq!(
            router_endpoint_from_listen("0.0.0.0:8061").unwrap(),
            "http://127.0.0.1:8061"
        );
    }

    #[test]
    fn router_endpoint_from_listen_keeps_explicit_loopback() {
        assert_eq!(
            router_endpoint_from_listen("127.0.0.1:8061").unwrap(),
            "http://127.0.0.1:8061"
        );
    }

    #[tokio::test]
    async fn middleware_writes_http_audit_record_for_post_route() {
        let team = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            chrono::Duration::days(7),
            chrono::Duration::days(30),
            4,
        ));
        let state = AppState {
            history_store: maintainer.history_store().clone(),
            maintainer,
            router_client: Arc::new(Router::new(team.path().to_path_buf())),
            auth: AuthVerifier::disabled(),
            auth_store: TeamAuthStore::new(paths::team_store_auth_keys_path(team.path())),
            maintainer_team_auth_enabled: true,
            router_team_auth_enabled: false,
            frontend_dist_dir: PathBuf::from("frontend/maintainer-workbench/dist"),
            sweep_scheduler: SweepScheduler::new(86_400),
            admin_auth: None,
        };
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/maintenance/sweep")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].path, "/maintenance/sweep");
        assert_eq!(audits[0].status_code, 200);
    }

    #[tokio::test]
    async fn admin_auth_rejects_management_api_without_audit_record() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/maintenance/sweep")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"ACN Maintainer\""
        );
        let audits = state.history_store.list_http_audit_logs().await.unwrap();
        assert!(audits.is_empty());
    }

    #[tokio::test]
    async fn admin_auth_allows_management_api_with_correct_basic_credentials() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/overview")
                    .header(header::AUTHORIZATION, basic_auth("admin", "secret"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_auth_rejects_wrong_basic_credentials() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/overview")
                    .header(header::AUTHORIZATION, basic_auth("admin", "wrong"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_auth_rejects_malformed_authorization_header() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/overview")
                    .header(header::AUTHORIZATION, "Bearer nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_auth_does_not_guard_agent_pull_endpoint() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/inbox/pull")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"auth":{"agent_id":"agent-a","acn_key":""},"data":{"agent_id":"agent-a"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_auth_does_not_guard_health_claim_upload_or_dispute_report() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), axum::http::StatusCode::OK);

        for path in ["/claims/upload", "/disputes/report"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_ne!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn admin_auth_allows_spa_fallback_paths_for_custom_login() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/policies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_auth_check_endpoint_accepts_correct_credentials() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin-auth/check")
                    .header(header::AUTHORIZATION, basic_auth("admin", "secret"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn admin_auth_check_endpoint_rejects_wrong_credentials_without_browser_challenge() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin-auth/check")
                    .header(header::AUTHORIZATION, basic_auth("admin", "wrong"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn admin_auth_status_endpoint_reports_disabled_without_credentials() {
        let team = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            chrono::Duration::days(7),
            chrono::Duration::days(30),
            4,
        ));
        let state = AppState {
            history_store: maintainer.history_store().clone(),
            maintainer,
            router_client: Arc::new(Router::new(team.path().to_path_buf())),
            auth: AuthVerifier::disabled(),
            auth_store: TeamAuthStore::new(paths::team_store_auth_keys_path(team.path())),
            maintainer_team_auth_enabled: true,
            router_team_auth_enabled: false,
            frontend_dist_dir: PathBuf::from("frontend/maintainer-workbench/dist"),
            sweep_scheduler: SweepScheduler::new(86_400),
            admin_auth: None,
        };
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/admin-auth/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), br#"{"enabled":false}"#);
    }

    #[tokio::test]
    async fn admin_auth_omits_browser_challenge_for_workbench_fetches() {
        let team = tempfile::tempdir().unwrap();
        let (state, _maintainer) = admin_auth_state(&team);
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth::admin_auth_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/overview")
                    .header("x-acn-workbench", "1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn inbox_pull_audit_updates_agent_last_source_ip() {
        let team = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            chrono::Duration::days(7),
            chrono::Duration::days(30),
            4,
        ));
        let state = AppState {
            history_store: maintainer.history_store().clone(),
            maintainer,
            router_client: Arc::new(Router::new(team.path().to_path_buf())),
            auth: AuthVerifier::disabled(),
            auth_store: TeamAuthStore::new(paths::team_store_auth_keys_path(team.path())),
            maintainer_team_auth_enabled: true,
            router_team_auth_enabled: false,
            frontend_dist_dir: PathBuf::from("frontend/maintainer-workbench/dist"),
            sweep_scheduler: SweepScheduler::new(86_400),
            admin_auth: None,
        };
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .with_state(state.clone());

        let mut request = Request::builder()
            .method("POST")
            .uri("/inbox/pull")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"auth":{"agent_id":"agent-a","acn_key":""},"data":{"agent_id":"agent-a"}}"#,
            ))
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            34567,
        )));

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let response = api::list_agents(
            axum::extract::State(state),
            axum::extract::Query(api::AgentListQuery { agent: None }),
        )
        .await
        .unwrap();
        let agents = response.0;
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].agent_id.as_str(), "agent-a");
        assert_eq!(agents[0].last_source_ip.as_deref(), Some("127.0.0.1"));
    }

    #[tokio::test]
    async fn spa_routes_fall_back_to_frontend_index() {
        let team = tempfile::tempdir().unwrap();
        let dist = tempfile::tempdir().unwrap();
        std::fs::write(
            dist.path().join("app.html"),
            "<!doctype html><title>workbench</title>",
        )
        .unwrap();
        std::fs::create_dir_all(dist.path().join("assets")).unwrap();
        std::fs::write(
            dist.path().join("assets").join("app.js"),
            "console.log('ok');",
        )
        .unwrap();
        std::fs::create_dir_all(dist.path().join("assets").join("fonts")).unwrap();
        std::fs::write(
            dist.path()
                .join("assets")
                .join("fonts")
                .join("landing.woff2"),
            b"test-font",
        )
        .unwrap();
        std::fs::write(
            dist.path().join("favicon.svg"),
            r#"<svg xmlns="http://www.w3.org/2000/svg"></svg>"#,
        )
        // favicon 夹具写入失败时无法继续验证路由，测试应立即终止。
        .unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            chrono::Duration::days(7),
            chrono::Duration::days(30),
            4,
        ));
        let state = AppState {
            history_store: maintainer.history_store().clone(),
            maintainer,
            router_client: Arc::new(Router::new(team.path().to_path_buf())),
            auth: AuthVerifier::disabled(),
            auth_store: TeamAuthStore::new(paths::team_store_auth_keys_path(team.path())),
            maintainer_team_auth_enabled: true,
            router_team_auth_enabled: false,
            frontend_dist_dir: dist.path().to_path_buf(),
            sweep_scheduler: SweepScheduler::new(86_400),
            admin_auth: None,
        };
        let app = build_app()
            .layer(middleware::from_fn_with_state(
                state.clone(),
                audit::audit_middleware,
            ))
            .with_state(state.clone());

        let claims_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app/claims")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(claims_response.status(), axum::http::StatusCode::OK);

        let asset_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app/assets/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(asset_response.status(), axum::http::StatusCode::OK);

        let landing_font_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/assets/fonts/landing.woff2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(landing_font_response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            landing_font_response
                .headers()
                .get(axum::http::header::CONTENT_TYPE),
            Some(&axum::http::HeaderValue::from_static("font/woff2"))
        );

        for path in ["/favicon.svg", "/app/favicon.svg"] {
            let favicon_response = app
                .clone()
                // 路径来自上方静态常量，构造请求不会包含非法 URI。
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                // Axum Router 的 oneshot 错误类型为 Infallible，测试可直接解包。
                .unwrap();
            assert_eq!(favicon_response.status(), axum::http::StatusCode::OK);
            assert_eq!(
                favicon_response
                    .headers()
                    .get(axum::http::header::CONTENT_TYPE),
                Some(&axum::http::HeaderValue::from_static("image/svg+xml"))
            );
        }
    }
}
