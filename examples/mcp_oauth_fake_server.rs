//! 本地 MCP/OAuth 验收 fixture；只允许监听 loopback 地址。

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Form, OriginalUri, Query, State};
use axum::http::header::{AUTHORIZATION, LOCATION};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use ring::digest::{digest, SHA256};
use serde_json::{json, Map, Value};
use subtle::ConstantTimeEq;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const NEW_PROTOCOL_VERSION: &str = "2026-07-28";
const LEGACY_PROTOCOL_VERSION: &str = "2025-11-25";
const STATIC_BEARER_TOKEN: &str = "fixture-static-token";

#[derive(Debug)]
struct Args {
    host: String,
    port: u16,
    log_file: Option<PathBuf>,
    token_expires_in: u64,
    omit_pkce: bool,
    insecure_metadata: bool,
    mismatched_callback_issuer: bool,
}

impl Args {
    fn parse() -> Result<Option<Self>, String> {
        let mut host = "127.0.0.1".to_string();
        let mut port = 8765_u16;
        let mut log_file = None;
        let mut token_expires_in = 5_u64;
        let mut omit_pkce = false;
        let mut insecure_metadata = false;
        let mut mismatched_callback_issuer = false;
        let mut values = std::env::args().skip(1);

        while let Some(argument) = values.next() {
            match argument.as_str() {
                "--host" => host = take_arg_value(&mut values, "--host")?,
                "--port" => port = parse_positive(&take_arg_value(&mut values, "--port")?, "port")?,
                "--log-file" => {
                    log_file = Some(PathBuf::from(take_arg_value(&mut values, "--log-file")?));
                }
                "--token-expires-in" => {
                    token_expires_in = parse_positive(
                        &take_arg_value(&mut values, "--token-expires-in")?,
                        "token-expires-in",
                    )?;
                }
                "--omit-pkce" => omit_pkce = true,
                "--insecure-metadata" => insecure_metadata = true,
                "--mismatched-callback-issuer" => mismatched_callback_issuer = true,
                "--help" | "-h" => {
                    print_usage();
                    return Ok(None);
                }
                _ => return Err(format!("未知参数: {argument}")),
            }
        }

        if !loopback_host(&host) {
            return Err("fixture 只允许监听 loopback host".to_string());
        }
        if port == 0 {
            return Err("port 必须在 1..65535".to_string());
        }
        if token_expires_in == 0 {
            return Err("token-expires-in 必须大于 0".to_string());
        }

        Ok(Some(Self {
            host,
            port,
            log_file,
            token_expires_in,
            omit_pkce,
            insecure_metadata,
            mismatched_callback_issuer,
        }))
    }
}

fn take_arg_value(
    values: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    values
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{option} 缺少值"))
}

fn parse_positive<T>(value: &str, label: &str) -> Result<T, String>
where
    T: std::str::FromStr + PartialEq + Default,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| format!("{label} 不是有效正整数: {value}"))?;
    if parsed == T::default() {
        return Err(format!("{label} 必须大于 0"));
    }
    Ok(parsed)
}

fn print_usage() {
    println!(
        "用法: cargo run --example mcp_oauth_fake_server -- [选项]\n\
         \n\
         选项:\n\
           --host HOST                    监听地址，必须是 loopback；默认 127.0.0.1\n\
           --port PORT                    监听端口；默认 8765\n\
           --log-file PATH                追加写入 JSONL 请求轨迹\n\
           --token-expires-in SECONDS     access token 有效期；默认 5 秒\n\
           --omit-pkce                    metadata 不声明 PKCE S256\n\
           --insecure-metadata            metadata 返回远端明文 HTTP endpoint\n\
           --mismatched-callback-issuer   callback 返回不匹配的 iss"
    );
}

#[derive(Debug, Clone)]
struct Grant {
    challenge: String,
    client_id: String,
    redirect_uri: String,
}

#[derive(Debug)]
struct Secrets {
    authorization_codes: HashMap<String, Grant>,
    access_tokens: HashSet<String>,
    refresh_tokens: HashSet<String>,
}

#[derive(Debug)]
struct FixtureState {
    base_url: String,
    issuer: String,
    log_file: Option<PathBuf>,
    token_expires_in: u64,
    omit_pkce: bool,
    insecure_metadata: bool,
    mismatched_callback_issuer: bool,
    secrets: Mutex<Secrets>,
    log_lock: Mutex<()>,
}

impl FixtureState {
    fn new(args: &Args, base_url: String) -> Self {
        let mut access_tokens = HashSet::new();
        access_tokens.insert(STATIC_BEARER_TOKEN.to_string());
        Self {
            issuer: format!("{base_url}/"),
            base_url,
            log_file: args.log_file.clone(),
            token_expires_in: args.token_expires_in,
            omit_pkce: args.omit_pkce,
            insecure_metadata: args.insecure_metadata,
            mismatched_callback_issuer: args.mismatched_callback_issuer,
            secrets: Mutex::new(Secrets {
                authorization_codes: HashMap::new(),
                access_tokens,
                refresh_tokens: HashSet::new(),
            }),
            log_lock: Mutex::new(()),
        }
    }

    async fn record(&self, fields: Value) {
        let mut row = match fields {
            Value::Object(row) => row,
            _ => Map::new(),
        };
        row.insert("at".to_string(), json!(unix_timestamp_millis()));
        let Ok(line) = serde_json::to_string(&Value::Object(row)) else {
            eprintln!("fixture request log serialization failed");
            return;
        };

        let _guard = self.log_lock.lock().await;
        eprintln!("{line}");
        let Some(path) = &self.log_file else {
            return;
        };
        if let Some(parent) = path.parent() {
            if let Err(error) = tokio::fs::create_dir_all(parent).await {
                eprintln!("fixture request log directory failed: {error}");
                return;
            }
        }
        match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            Ok(mut stream) => {
                if let Err(error) = stream.write_all(format!("{line}\n").as_bytes()).await {
                    eprintln!("fixture request log write failed: {error}");
                }
            }
            Err(error) => eprintln!("fixture request log open failed: {error}"),
        }
    }

    async fn is_authorized(&self, headers: &HeaderMap) -> bool {
        let Some(token) = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        self.secrets.lock().await.access_tokens.contains(token)
    }

    async fn issue_tokens(&self) -> Value {
        let access_token = fixture_token("fixture-access-", 16);
        let refresh_token = fixture_token("fixture-refresh-", 16);
        let mut secrets = self.secrets.lock().await;
        secrets.access_tokens.insert(access_token.clone());
        secrets.refresh_tokens.insert(refresh_token.clone());
        json!({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": self.token_expires_in,
            "refresh_token": refresh_token,
            "scope": "fixture:read"
        })
    }
}

fn unix_timestamp_millis() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| (duration.as_secs_f64() * 1000.0).round() / 1000.0)
        .unwrap_or_default()
}

fn fixture_token(prefix: &str, length: usize) -> String {
    let random = thread_rng()
        .sample_iter(Alphanumeric)
        .take(length)
        .map(char::from)
        .collect::<String>();
    format!("{prefix}{random}")
}

async fn protected_resource_metadata(State(state): State<Arc<FixtureState>>) -> Response {
    state
        .record(json!({
            "method": "GET",
            "path": "/.well-known/oauth-protected-resource"
        }))
        .await;
    json_response(
        StatusCode::OK,
        json!({
            "resource": format!("{}/mcp", state.base_url),
            "authorization_servers": [&state.base_url],
            "scopes_supported": ["fixture:read"]
        }),
        &[],
    )
}

async fn authorization_metadata(
    State(state): State<Arc<FixtureState>>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    state
        .record(json!({"method": "GET", "path": uri.path()}))
        .await;
    let endpoint_base = if state.insecure_metadata {
        "http://auth.example.test"
    } else {
        &state.base_url
    };
    let mut metadata = json!({
        "issuer": state.issuer,
        "authorization_endpoint": format!("{endpoint_base}/authorize"),
        "token_endpoint": format!("{endpoint_base}/token"),
        "registration_endpoint": format!("{endpoint_base}/register"),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_methods_supported": ["none"],
        "authorization_response_iss_parameter_supported": true
    });
    if !state.omit_pkce {
        if let Some(object) = metadata.as_object_mut() {
            object.insert(
                "code_challenge_methods_supported".to_string(),
                json!(["S256"]),
            );
        }
    }
    json_response(StatusCode::OK, metadata, &[])
}

async fn register_client(
    State(state): State<Arc<FixtureState>>,
    Json(body): Json<Value>,
) -> Response {
    let client_id = fixture_token("fixture-client-", 12);
    let redirect_uris = body
        .get("redirect_uris")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let client_name = body
        .get("client_name")
        .and_then(Value::as_str)
        .unwrap_or("ACN");
    state
        .record(json!({
            "method": "POST",
            "path": "/register",
            "client_id": client_id,
            "redirect_uris": redirect_uris
        }))
        .await;
    json_response(
        StatusCode::CREATED,
        json!({
            "client_id": client_id,
            "client_name": client_name,
            "redirect_uris": redirect_uris,
            "token_endpoint_auth_method": "none"
        }),
        &[],
    )
}

async fn authorize(
    State(state): State<Arc<FixtureState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let required = ["client_id", "redirect_uri", "state", "code_challenge"];
    let missing = required
        .into_iter()
        .filter(|name| params.get(*name).is_none_or(String::is_empty))
        .collect::<Vec<_>>();
    if !missing.is_empty()
        || params.get("code_challenge_method").map(String::as_str) != Some("S256")
    {
        state
            .record(json!({
                "method": "GET",
                "path": "/authorize",
                "accepted": false,
                "missing": missing
            }))
            .await;
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error": "invalid_request", "missing": missing}),
            &[],
        );
    }

    let grant = Grant {
        challenge: params["code_challenge"].clone(),
        client_id: params["client_id"].clone(),
        redirect_uri: params["redirect_uri"].clone(),
    };
    let code = fixture_token("fixture-code-", 16);
    state
        .secrets
        .lock()
        .await
        .authorization_codes
        .insert(code.clone(), grant.clone());
    let callback_issuer = if state.mismatched_callback_issuer {
        "https://issuer.example.test/"
    } else {
        &state.issuer
    };
    let mut redirect = match reqwest_013::Url::parse(&grant.redirect_uri) {
        Ok(redirect) => redirect,
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json!({"error": "invalid_redirect_uri"}),
                &[],
            );
        }
    };
    redirect
        .query_pairs_mut()
        .append_pair("code", &code)
        .append_pair("state", &params["state"])
        .append_pair("iss", callback_issuer);
    state
        .record(json!({
            "method": "GET",
            "path": "/authorize",
            "accepted": true,
            "client_id": grant.client_id,
            "scope": params.get("scope").cloned().unwrap_or_default(),
            "resource": params.get("resource").cloned().unwrap_or_default()
        }))
        .await;

    redirect_response(redirect.as_str())
}

async fn exchange_token(
    State(state): State<Arc<FixtureState>>,
    Form(params): Form<HashMap<String, String>>,
) -> Response {
    let grant_type = params.get("grant_type").cloned().unwrap_or_default();
    let accepted = match grant_type.as_str() {
        "authorization_code" => verify_authorization_code(&state, &params).await,
        "refresh_token" => {
            let refresh_token = params.get("refresh_token").cloned().unwrap_or_default();
            state
                .secrets
                .lock()
                .await
                .refresh_tokens
                .remove(&refresh_token)
        }
        _ => false,
    };
    state
        .record(json!({
            "method": "POST",
            "path": "/token",
            "grant_type": grant_type,
            "accepted": accepted
        }))
        .await;
    if !accepted {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error": "invalid_grant"}),
            &[],
        );
    }
    let tokens = state.issue_tokens().await;
    json_response(
        StatusCode::OK,
        tokens,
        &[("cache-control", "no-store"), ("pragma", "no-cache")],
    )
}

async fn verify_authorization_code(state: &FixtureState, params: &HashMap<String, String>) -> bool {
    let code = params.get("code").cloned().unwrap_or_default();
    let verifier = params.get("code_verifier").cloned().unwrap_or_default();
    let grant = state.secrets.lock().await.authorization_codes.remove(&code);
    let Some(grant) = grant else {
        return false;
    };
    let challenge = URL_SAFE_NO_PAD.encode(digest(&SHA256, verifier.as_bytes()).as_ref());
    bool::from(challenge.as_bytes().ct_eq(grant.challenge.as_bytes()))
        && params.get("client_id") == Some(&grant.client_id)
        && params.get("redirect_uri") == Some(&grant.redirect_uri)
}

async fn oauth_mcp_get(State(state): State<Arc<FixtureState>>, headers: HeaderMap) -> Response {
    if state.is_authorized(&headers).await {
        StatusCode::METHOD_NOT_ALLOWED.into_response()
    } else {
        auth_required(&state, &headers, "GET").await
    }
}

async fn anonymous_mcp_get() -> Response {
    StatusCode::METHOD_NOT_ALLOWED.into_response()
}

async fn oauth_mcp_post(
    State(state): State<Arc<FixtureState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    if !state.is_authorized(&headers).await {
        return auth_required(&state, &headers, "POST").await;
    }
    handle_mcp(state, headers, payload, true, "oauth").await
}

async fn anonymous_mcp_post(
    State(state): State<Arc<FixtureState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    handle_mcp(state, headers, payload, true, "anonymous").await
}

async fn legacy_mcp_post(
    State(state): State<Arc<FixtureState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    handle_mcp(state, headers, payload, false, "legacy").await
}

async fn handle_mcp(
    state: Arc<FixtureState>,
    headers: HeaderMap,
    payload: Value,
    new_protocol: bool,
    endpoint: &'static str,
) -> Response {
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let request_id = payload.get("id").cloned().unwrap_or(Value::Null);
    let protocol_version = headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    state
        .record(json!({
            "method": "POST",
            "path": if endpoint == "anonymous" { "/anonymous-mcp" } else if endpoint == "legacy" { "/legacy-mcp" } else { "/mcp" },
            "endpoint": endpoint,
            "rpc_method": method,
            "protocol_version": protocol_version,
            "authorized": if endpoint == "oauth" { Some(true) } else { None }
        }))
        .await;

    if new_protocol && protocol_version.as_deref() != Some(NEW_PROTOCOL_VERSION) {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error": "wrong_protocol_version"}),
            &[],
        );
    }
    if !new_protocol && method == "server/discover" {
        return rpc_error(request_id, -32601, "Method not found");
    }
    if !new_protocol && method == "initialize" {
        return rpc_result(
            request_id,
            json!({
                "protocolVersion": LEGACY_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "acn-fake-legacy", "version": "1.0.0"}
            }),
            Some("fixture-session"),
        );
    }
    match method {
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "server/discover" => rpc_result(
            request_id,
            json!({
                "resultType": "complete",
                "supportedVersions": [NEW_PROTOCOL_VERSION],
                "capabilities": {"tools": {}},
                "ttlMs": 0,
                "cacheScope": "private"
            }),
            None,
        ),
        "tools/list" => {
            let mut result = json!({
                "tools": [{
                    "name": "ping",
                    "description": "Return pong from the local fixture",
                    "inputSchema": {"type": "object", "properties": {}}
                }]
            });
            if new_protocol {
                if let Some(object) = result.as_object_mut() {
                    object.insert("resultType".to_string(), json!("complete"));
                }
            }
            rpc_result(request_id, result, None)
        }
        "tools/call" => rpc_result(
            request_id,
            json!({
                "content": [{"type": "text", "text": "pong"}],
                "isError": false
            }),
            None,
        ),
        _ => rpc_error(request_id, -32601, "Method not found"),
    }
}

async fn auth_required(state: &FixtureState, headers: &HeaderMap, method: &str) -> Response {
    let protocol_version = headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok());
    state
        .record(json!({
            "method": method,
            "path": "/mcp",
            "authorized": false,
            "protocol_version": protocol_version
        }))
        .await;
    json_response(
        StatusCode::UNAUTHORIZED,
        json!({"error": "authorization_required"}),
        &[(
            "www-authenticate",
            "Bearer resource_metadata=\"/.well-known/oauth-protected-resource\", scope=\"fixture:read\"",
        )],
    )
}

async fn delete_request(
    State(state): State<Arc<FixtureState>>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    state
        .record(json!({"method": "DELETE", "path": uri.path()}))
        .await;
    StatusCode::NO_CONTENT.into_response()
}

fn rpc_result(request_id: Value, result: Value, session_id: Option<&str>) -> Response {
    let headers = session_id
        .map(|value| vec![("mcp-session-id", value)])
        .unwrap_or_default();
    json_response(
        StatusCode::OK,
        json!({"jsonrpc": "2.0", "id": request_id, "result": result}),
        &headers,
    )
}

fn rpc_error(request_id: Value, code: i64, message: &str) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message}
        }),
        &[],
    )
}

fn redirect_response(location: &str) -> Response {
    let mut response = StatusCode::FOUND.into_response();
    if let Ok(location) = HeaderValue::from_str(location) {
        response.headers_mut().insert(LOCATION, location);
    }
    response
}

fn json_response(status: StatusCode, body: Value, headers: &[(&str, &str)]) -> Response {
    let mut response = (status, Json(body)).into_response();
    for (name, value) in headers {
        let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_str(value) else {
            continue;
        };
        response.headers_mut().insert(name, value);
    }
    response
}

fn loopback_host(value: &str) -> bool {
    let normalized = value.trim_end_matches('.');
    normalized.eq_ignore_ascii_case("localhost")
        || normalized
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn host_for_url(host: &str) -> String {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]"),
        _ => host.to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(args) = Args::parse().map_err(std::io::Error::other)? else {
        return Ok(());
    };
    let base_url = format!("http://{}:{}", host_for_url(&args.host), args.port);
    let state = Arc::new(FixtureState::new(&args, base_url.clone()));
    let app = Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource_metadata),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_metadata),
        )
        .route(
            "/.well-known/openid-configuration",
            get(authorization_metadata),
        )
        .route("/register", post(register_client))
        .route("/authorize", get(authorize))
        .route("/token", post(exchange_token))
        .route(
            "/mcp",
            get(oauth_mcp_get)
                .post(oauth_mcp_post)
                .delete(delete_request),
        )
        .route(
            "/anonymous-mcp",
            get(anonymous_mcp_get)
                .post(anonymous_mcp_post)
                .delete(delete_request),
        )
        .route(
            "/legacy-mcp",
            get(anonymous_mcp_get)
                .post(legacy_mcp_post)
                .delete(delete_request),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port)).await?;

    println!("fixture: {base_url}");
    println!("OAuth MCP: {base_url}/mcp");
    println!("anonymous MCP {NEW_PROTOCOL_VERSION}: {base_url}/anonymous-mcp");
    println!("legacy MCP {LEGACY_PROTOCOL_VERSION}: {base_url}/legacy-mcp");
    println!("static bearer: {STATIC_BEARER_TOKEN}");
    if let Some(log_file) = &args.log_file {
        println!("request log: {}", log_file.display());
    }

    axum::serve(listener, app).await?;
    Ok(())
}
