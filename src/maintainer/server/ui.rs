use std::path::{Component, Path as StdPath, PathBuf};

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, Response, StatusCode};
use tokio::fs;

use super::state::AppState;

/// Serve the ACN landing page as the root entry point (`/`).
pub async fn landing(
    State(state): State<AppState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let path = state.frontend_dist_dir.join("acn_landing.html");
    serve_file(path, "text/html; charset=utf-8").await
}

pub async fn spa_entry(
    State(state): State<AppState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let path = state.frontend_dist_dir.join("app.html");
    serve_file(path, "text/html; charset=utf-8").await
}

/// Serve app assets (JS, CSS, fonts, etc.).
pub async fn asset(
    State(state): State<AppState>,
    Path(path): Path<String>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let safe_relative = sanitize_relative_path(&path)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "非法静态资源路径".to_string()))?;
    let asset_path = state.frontend_dist_dir.join("assets").join(safe_relative);
    let content_type = guess_content_type(&asset_path);
    serve_file(asset_path, content_type).await
}

/// Serve the workbench favicon from the dist root.
pub async fn favicon(
    State(state): State<AppState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    serve_file(state.frontend_dist_dir.join("favicon.svg"), "image/svg+xml").await
}

/// Serve docs/ pages (acn_roles_interaction.html, etc.) from dist/docs/.
pub async fn docs_asset(
    State(state): State<AppState>,
    Path(path): Path<String>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let safe_relative = sanitize_relative_path(&path)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "非法静态资源路径".to_string()))?;
    let asset_path = state.frontend_dist_dir.join("docs").join(safe_relative);
    let content_type = guess_content_type(&asset_path);
    serve_file(asset_path, content_type).await
}

pub async fn health() -> StatusCode {
    StatusCode::OK
}

async fn serve_file(
    path: PathBuf,
    content_type: &'static str,
) -> Result<Response<Body>, (StatusCode, String)> {
    let body = fs::read(&path).await.map_err(|err| {
        let status = if err.kind() == std::io::ErrorKind::NotFound {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        (status, format!("读取静态资源失败 {:?}: {err}", path))
    })?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, HeaderValue::from_static(content_type))
        .body(Body::from(body))
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("构建静态资源响应失败: {err}"),
            )
        })
}

fn sanitize_relative_path(raw: &str) -> Option<PathBuf> {
    let path = StdPath::new(raw);
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            _ => return None,
        }
    }
    Some(out)
}

fn guess_content_type(path: &StdPath) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("json") => "application/json; charset=utf-8",
        Some("woff2") => "font/woff2",
        Some("html") => "text/html; charset=utf-8",
        _ => "application/octet-stream",
    }
}
