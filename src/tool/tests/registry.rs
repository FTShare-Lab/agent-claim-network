//! Registry 权限面、并发分类、MCP 与 router 派发测试。
//!
//! 验证工具可见性、fail-closed 分类和动态外部工具的统一派发边界。

use super::*;

#[test]
fn concurrency_classifier_matches_native_tool_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    for (name, input) in [
        ("file_read", json!({"path": "src/tool/mod.rs"})),
        ("code_run", json!({"script": "pwd"})),
        (
            "code_run",
            json!({
                "script": "cd src && rg -n 'ToolRegistry' . | head -n 20",
                "cwd": ".",
                "yield_time_ms": 300,
            }),
        ),
        ("web_search", json!({"query": "ACN parallel tools"})),
        ("web_fetch", json!({"url": "https://example.com"})),
        ("working_note", json!({"action": "list"})),
        ("ask_user", json!({"question": "continue?"})),
    ] {
        assert!(
            registry.is_concurrency_safe(name, &input),
            "expected {name} to be concurrency-safe for input {input}"
        );
    }

    for (name, input) in [
        (
            "file_patch",
            json!({"path": "src/tool/mod.rs", "old_content": "old", "new_content": "new"}),
        ),
        ("file_write", json!({"path": "note.md", "content": "new"})),
        ("code_run", json!({"script": "git status"})),
        ("code_run", json!({"script": "pwd", "type": "python"})),
        ("code_run", json!({"script": "pwd", "type": "powershell"})),
        ("code_run", json!({"script": "pwd", "type": "zsh"})),
        (
            "web_request",
            json!({"method": "GET", "url": "https://example.com"}),
        ),
        (
            "web_request",
            json!({"method": "POST", "url": "https://example.com"}),
        ),
        ("working_note", json!({"action": "add", "note": "state"})),
        ("working_note", json!({"action": "clear"})),
        ("working_note", json!({"action": "unknown"})),
        (
            "memory",
            json!({"action": "add", "target": "memory", "content": "state"}),
        ),
        ("session_search", json!({"query": "previous work"})),
        (
            "create_subagent",
            json!({"title": "scan", "role": "reader", "objective": "scan files"}),
        ),
        ("wait_subagents", json!({})),
        (
            "steer_subagent",
            json!({"id": "subagent_12345678", "instruction": "continue"}),
        ),
        ("update_subagent_progress", json!({"summary": "working"})),
    ] {
        assert!(
            !registry.is_concurrency_safe(name, &input),
            "expected {name} to be a serial barrier for input {input}"
        );
    }

    let registry = registry
        .with_router_client(Arc::new(TestRouter {
            result: sample_router_result(),
            overview: sample_scopes_overview(),
        }))
        .with_delegation_executor(
            dir.path().join("agents/agent-a"),
            AgentId::new("agent-a").unwrap(),
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig::default(),
        );
    assert!(registry.is_concurrency_safe("consult_router", &json!({"mode": "overview"})));
    assert!(registry.is_concurrency_safe(
        "consult_router",
        &json!({"mode": "query", "scope": "parallel/tools"}),
    ));
    assert!(registry.is_concurrency_safe("list_subagents", &json!({})));
    assert!(registry.is_concurrency_safe("read_subagent", &json!({"id": "subagent_12345678"}),));
}

#[test]
fn concurrency_classifier_fails_closed_for_missing_access_or_unparseable_input() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    // parent profile 有 router/delegation 权限位，但未注入相应 host/client 时不得推测为安全。
    assert!(!registry.is_concurrency_safe("consult_router", &json!({"mode": "overview"})));
    assert!(!registry.is_concurrency_safe("list_subagents", &json!({})));
    assert!(!registry.is_concurrency_safe("read_subagent", &json!({"id": "subagent_12345678"}),));

    for (name, input) in [
        ("unknown_tool", json!({})),
        ("file_read", json!("src/tool/mod.rs")),
        ("file_read", json!({})),
        ("web_search", json!({"query": 3})),
        ("web_fetch", json!({"url": 3})),
        ("working_note", json!({"action": "list", "note": 3})),
        ("ask_user", json!({"choices": ["yes", "no"]})),
        ("code_run", json!({"script": ""})),
        ("code_run", json!({"script": 3})),
        ("consult_router", json!({"mode": "invalid"})),
        ("list_subagents", json!({"unexpected": true})),
        ("read_subagent", json!({})),
        ("mcp__server__unknown", json!({})),
    ] {
        assert!(
            !registry.is_concurrency_safe(name, &input),
            "expected malformed or unavailable {name} input {input} to fail closed"
        );
    }

    let restricted = registry.for_memory_review();
    for (name, input) in [
        ("file_read", json!({"path": "src/tool/mod.rs"})),
        ("code_run", json!({"script": "pwd"})),
        ("web_search", json!({"query": "parallel tools"})),
        ("web_fetch", json!({"url": "https://example.com"})),
        ("working_note", json!({"action": "list"})),
        ("ask_user", json!({"question": "continue?"})),
    ] {
        assert!(
            !restricted.is_concurrency_safe(name, &input),
            "inaccessible {name} must fail closed"
        );
    }
}

#[tokio::test]
async fn consult_router_tool_is_exposed_only_when_router_is_configured() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let names = registry
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "consult_router"));

    let router = Arc::new(TestRouter {
        result: sample_router_result(),
        overview: ScopesOverviewSnapshot::default(),
    });
    let registry = registry.with_router_client(router);
    let tools = registry.definitions();
    let names = tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "consult_router"));
    let consult_router = tools
        .iter()
        .find(|tool| tool.name == "consult_router")
        .unwrap();
    assert_eq!(
        consult_router.input_schema["required"],
        serde_json::json!(["mode"])
    );
}

#[tokio::test]
async fn evaluation_registry_limits_tools_and_keeps_router() {
    let dir = tempfile::tempdir().unwrap();
    let mcp_config_path = dir.path().join(".mcp.json");
    let script_path = dir.path().join("stdio_mcp_tool.sh");
    tokio::fs::write(&script_path, stdio_mcp_tool_script())
        .await
        .unwrap();
    let mut mcp_cfg = crate::mcp::config::McpJsonConfig::default();
    mcp_cfg.servers.insert(
        "local".to_string(),
        crate::mcp::config::McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            BTreeMap::new(),
            Vec::new(),
        ),
    );
    crate::mcp::config::write_mcp_json_config_atomic(&mcp_config_path, &mcp_cfg)
        .await
        .unwrap();
    let mcp_manager = Arc::new(crate::mcp::connection_manager::McpConnectionManager::new(
        mcp_config_path,
        dir.path().to_path_buf(),
        None,
    ));
    mcp_manager.refresh_all().await.unwrap();
    let session_search = Arc::new(SessionSearchService::new(
        AgentId::new("agent-a").unwrap(),
        dir.path().join("agent-a"),
        "test-model".into(),
        Arc::new(UnusedSessionSearchSummarizer),
        SessionSearchConfig::default(),
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_memory_store(Arc::new(LocalFsMemoryStore::new(
            dir.path().to_path_buf(),
            100,
            100,
            true,
        )))
        .with_session_search(session_search)
        .with_mcp_manager(mcp_manager)
        .with_delegation_executor(
            dir.path().join("agent-a"),
            AgentId::new("agent-a").unwrap(),
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig::default(),
        )
        .with_router_client(Arc::new(TestRouter {
            result: sample_router_result(),
            overview: sample_scopes_overview(),
        }))
        .for_evaluation("ACN_TEST_EVALUATION_SECRET".into());

    let names = registry
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    for name in [
        "web_search",
        "web_fetch",
        "web_request",
        "ask_user",
        "memory",
        "session_search",
        "mcp__local__ping",
        "create_subagent",
        "list_subagents",
        "wait_subagents",
        "read_subagent",
        "steer_subagent",
        "update_subagent_progress",
    ] {
        assert!(!names.iter().any(|visible| visible == name));
    }
    assert!(names.iter().any(|name| name == "working_note"));
    assert!(names.iter().any(|name| name == "consult_router"));
    assert!(registry.memory_store.is_none());
    assert!(registry.session_search.is_none());
    assert!(registry.mcp_manager.is_none());
    assert!(registry.delegation_host.is_none());

    let result = registry
        .dispatch("consult_router", json!({"mode": "overview"}))
        .await
        .unwrap();
    assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
}

#[tokio::test]
async fn evaluation_code_run_removes_configured_secret_from_pipe_environment() {
    let _secret = EnvVarGuard::set("ACN_TEST_EVALUATION_SECRET", "secret-value");
    let _visible = EnvVarGuard::set("ACN_TEST_EVALUATION_VISIBLE", "visible-value");
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_evaluation("ACN_TEST_EVALUATION_SECRET".into());

    let result = registry
        .dispatch(
            "code_run",
            json!({
                "script": "printf 'secret=%s\\nvisible=%s\\n' \"${ACN_TEST_EVALUATION_SECRET-unset}\" \"$ACN_TEST_EVALUATION_VISIBLE\"",
            }),
        )
        .await
        .unwrap();
    let stdout = result.output["stdout"].as_str().unwrap();
    assert!(stdout.contains("secret=unset"));
    assert!(stdout.contains("visible=visible-value"));
}

#[cfg(unix)]
#[tokio::test]
async fn evaluation_code_run_removes_configured_secret_from_pty_environment() {
    let _secret = EnvVarGuard::set("ACN_TEST_EVALUATION_SECRET", "secret-value");
    let _visible = EnvVarGuard::set("ACN_TEST_EVALUATION_VISIBLE", "visible-value");
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_evaluation("ACN_TEST_EVALUATION_SECRET".into());

    let result = registry
        .dispatch(
            "code_run",
            json!({
                "script": "printf 'secret=%s\\nvisible=%s\\n' \"${ACN_TEST_EVALUATION_SECRET-unset}\" \"$ACN_TEST_EVALUATION_VISIBLE\"",
                "tty": true,
            }),
        )
        .await
        .unwrap();
    let stdout = result.output["stdout"].as_str().unwrap();
    assert!(stdout.contains("secret=unset"));
    assert!(stdout.contains("visible=visible-value"));
}

#[tokio::test]
async fn mcp_tool_is_exposed_and_dispatched_through_registry() {
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("stdio_mcp_tool.sh");
    tokio::fs::write(&script_path, stdio_mcp_tool_script())
        .await
        .unwrap();
    let mcp_config_path = dir.path().join(".mcp.json");
    let mut mcp_cfg = crate::mcp::config::McpJsonConfig::default();
    mcp_cfg.servers.insert(
        "local".to_string(),
        crate::mcp::config::McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            BTreeMap::new(),
            Vec::new(),
        ),
    );
    crate::mcp::config::write_mcp_json_config_atomic(&mcp_config_path, &mcp_cfg)
        .await
        .unwrap();
    let mcp_manager = Arc::new(crate::mcp::connection_manager::McpConnectionManager::new(
        mcp_config_path,
        dir.path().to_path_buf(),
        None,
    ));
    mcp_manager.refresh_all().await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_mcp_manager(mcp_manager);

    let names = registry
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "mcp__local__ping"));

    let result = registry
        .dispatch("mcp__local__ping", serde_json::json!({"text": "hi"}))
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
    assert_eq!(result.output["content"][0]["text"], "pong");

    let business_failure = registry
        .dispatch(
            "mcp__local__ping",
            serde_json::json!({"text": "hi", "fail": true}),
        )
        .await
        .unwrap();
    assert_eq!(
        business_failure.outcome,
        ToolExecutionOutcome::BusinessFailure
    );
    assert_eq!(business_failure.output["is_error"], true);
    assert_eq!(business_failure.output["content"][0]["text"], "failed");
}

#[tokio::test]
async fn mcp_transport_or_protocol_error_is_dispatch_failure() {
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("stdio_mcp_tool.sh");
    tokio::fs::write(&script_path, stdio_mcp_tool_script())
        .await
        .unwrap();
    let mcp_config_path = dir.path().join(".mcp.json");
    let mut mcp_cfg = crate::mcp::config::McpJsonConfig::default();
    mcp_cfg.servers.insert(
        "local".to_string(),
        crate::mcp::config::McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            BTreeMap::new(),
            Vec::new(),
        ),
    );
    crate::mcp::config::write_mcp_json_config_atomic(&mcp_config_path, &mcp_cfg)
        .await
        .unwrap();
    let mcp_manager = Arc::new(crate::mcp::connection_manager::McpConnectionManager::new(
        mcp_config_path,
        dir.path().to_path_buf(),
        None,
    ));
    mcp_manager.refresh_all().await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_mcp_manager(mcp_manager);

    let error = registry
        .dispatch("mcp__local__ping", serde_json::json!("not an object"))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("参数必须是 JSON object"));
}

pub(super) fn stdio_mcp_tool_script() -> &'static str {
    r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}

while IFS= read -r line; do
id="$(response_id "$line")"
case "$line" in
  *'"method":"server/discover"'*)
    printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
    ;;
  *'"method":"initialize"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"stdio-tool-mock","version":"1.0.0"}}}\n' "$id"
    ;;
  *'"method":"tools/list"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"ping","description":"Ping tool","inputSchema":{"type":"object","properties":{"text":{"type":"string","description":"Input text"},"fail":{"type":"boolean"}}}}]}}\n' "$id"
    ;;
  *'"method":"tools/call"'*'"fail":true'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"failed"}],"isError":true}}\n' "$id"
    ;;
  *'"method":"tools/call"'*)
    printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"pong"}],"isError":false}}\n' "$id"
    ;;
esac
done
"#
}

pub(super) fn parallel_stdio_mcp_tool_script() -> &'static str {
    r#"response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}
timestamp() {
  perl -MTime::HiRes=time -e 'printf "%.6f", time'
}
while IFS= read -r line; do
  id=$(response_id "$line")
  case "$line" in
    *'"method":"server/discover"'*)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id"
      ;;
    *'"method":"initialize"'*)
      printf '{"event":"initialize","pid":%s,"ts":%s}\n' "$$" "$(timestamp)" >> "$MCP_FIXTURE_LOG"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"parallel-stdio-mock","version":"1.0.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"slow","description":"Slow tool","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
    *'"method":"tools/call"'*)
      (
        printf '{"event":"start","id":"%s","pid":%s,"ts":%s}\n' "$id" "$$" "$(timestamp)" >> "$MCP_FIXTURE_LOG"
        sleep 0.2
        printf '{"event":"end","id":"%s","pid":%s,"ts":%s}\n' "$id" "$$" "$(timestamp)" >> "$MCP_FIXTURE_LOG"
        printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"slow-done"}],"isError":false}}\n' "$id"
      ) &
      ;;
  esac
done
"#
}

fn mcp_test_json_response(id: Value, result: Value) -> axum::response::Response {
    use axum::http::{HeaderMap, HeaderValue};
    use axum::response::IntoResponse;

    let mut headers = HeaderMap::new();
    headers.insert(
        "Mcp-Session-Id",
        HeaderValue::from_static("parallel-test-session"),
    );
    (
        headers,
        axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        })),
    )
        .into_response()
}

pub(super) async fn parallel_mcp_test_response(
    payload: Value,
    active_calls: Arc<AtomicUsize>,
    max_active_calls: Arc<AtomicUsize>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let id = payload.get("id").cloned().unwrap_or(Value::Null);
    let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
    match method {
        "server/discover" => axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "Method not found"},
        }))
        .into_response(),
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "initialize" => mcp_test_json_response(
            id,
            json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "parallel-test", "version": "1.0.0"},
            }),
        ),
        "tools/list" => mcp_test_json_response(
            id,
            json!({
                "tools": [{
                    "name": "slow",
                    "description": "Slow tool for concurrency regression coverage",
                    "inputSchema": {"type": "object"},
                }],
            }),
        ),
        "tools/call" => {
            let now_active = active_calls.fetch_add(1, Ordering::SeqCst) + 1;
            max_active_calls.fetch_max(now_active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            active_calls.fetch_sub(1, Ordering::SeqCst);
            mcp_test_json_response(
                id,
                json!({
                    "content": [{"type": "text", "text": "slow-done"}],
                    "isError": false,
                }),
            )
        }
        _ => mcp_test_json_response(id, json!({})),
    }
}

#[tokio::test]
async fn consult_router_filters_retrieval_debug_from_tool_result() {
    let dir = tempfile::tempdir().unwrap();
    let claim = sample_claim();
    let dispute_id = DisputeId::random();
    let result = RouterQueryResult {
        candidate_claims: vec![CandidateClaim {
            claim: claim.clone(),
            open_dispute_ids: vec![dispute_id.clone()],
            resolved_dispute_ids: vec![],
        }],
        disputes: vec![DisputeRef {
            id: dispute_id,
            name: "router_tool_dispute".into(),
            claim_ids: vec![claim.id.clone()],
            summary: "test dispute".into(),
            status: DisputeStatus::Open,
        }],
        retrieval_debug: Some(RetrievalDebug {
            mode: "hybrid".into(),
            lexical_hits: 1,
            ..Default::default()
        }),
    };
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_router_client(Arc::new(TestRouter {
            result,
            overview: ScopesOverviewSnapshot::default(),
        }));

    let output = registry
        .dispatch(
            "consult_router",
            serde_json::json!({
                "mode": "query",
                "scope": "router/tool",
                "semantic_query": "check shared router claims"
            }),
        )
        .await
        .unwrap();

    assert_eq!(output.outcome, ToolExecutionOutcome::Completed);
    assert_eq!(output.output["mode"], "query");
    assert_eq!(
        output.output["candidate_claims"][0]["id"],
        claim.id.as_str()
    );
    assert_eq!(output.output["disputes"][0]["status"], "open");
    assert!(output.output.get("retrieval_debug").is_none());
}

#[tokio::test]
async fn consult_router_overview_mode_returns_scope_map() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_router_client(Arc::new(TestRouter {
            result: sample_router_result(),
            overview: sample_scopes_overview(),
        }));

    let output = registry
        .dispatch(
            "consult_router",
            serde_json::json!({
                "mode": "overview"
            }),
        )
        .await
        .unwrap();

    assert_eq!(output.outcome, ToolExecutionOutcome::Completed);
    assert_eq!(output.output["mode"], "overview");
    assert_eq!(output.output["scopes"][0]["scope"], "router/tool");
    assert_eq!(output.output["scopes"][0]["active_claims"], 2);
}

struct UnavailableRouter;

#[async_trait]
impl RouterClient for UnavailableRouter {
    async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        Err(anyhow::anyhow!("router unreachable"))
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        Err(anyhow::anyhow!("router unreachable"))
    }
}

struct AuthFailingRouter;

#[async_trait]
impl RouterClient for AuthFailingRouter {
    async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        Err(RouterClientError::Auth {
            operation: "POST /claims/query".into(),
            status: 401,
        }
        .into())
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        Err(RouterClientError::Auth {
            operation: "POST /claims/scopes/overview".into(),
            status: 403,
        }
        .into())
    }
}

#[tokio::test]
async fn consult_router_degrades_to_unavailable_result_when_router_errors() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_router_client(Arc::new(UnavailableRouter));

    // router 报错时两种模式都必须降级为结构化不可用结果，而不是把错误抛给调用方中断会话。
    let overview = registry
        .dispatch("consult_router", serde_json::json!({ "mode": "overview" }))
        .await
        .unwrap();
    assert_eq!(overview.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(overview.output["mode"], "overview");
    assert_eq!(overview.output["available"], false);
    assert_eq!(overview.output["reason"], "router_unavailable");
    assert_eq!(overview.output["scopes"], serde_json::json!([]));

    let query = registry
        .dispatch(
            "consult_router",
            serde_json::json!({ "mode": "query", "scope": "router/tool" }),
        )
        .await
        .unwrap();
    assert_eq!(query.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(query.output["mode"], "query");
    assert_eq!(query.output["available"], false);
    assert_eq!(query.output["reason"], "router_unavailable");
}

#[tokio::test]
async fn consult_router_exposes_auth_failure_reason_to_agent() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_router_client(Arc::new(AuthFailingRouter));

    let overview = registry
        .dispatch("consult_router", serde_json::json!({ "mode": "overview" }))
        .await
        .unwrap();
    assert_eq!(overview.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(overview.output["available"], false);
    assert_eq!(overview.output["reason"], "router_auth_failed");
    assert_eq!(overview.output["http_status"], 403);
    assert_eq!(overview.output["operation"], "POST /claims/scopes/overview");
    assert!(overview.output["error"]
        .as_str()
        .unwrap()
        .contains("鉴权失败: status=403"));

    let query = registry
        .dispatch(
            "consult_router",
            serde_json::json!({ "mode": "query", "scope": "router/tool" }),
        )
        .await
        .unwrap();
    assert_eq!(query.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(query.output["available"], false);
    assert_eq!(query.output["reason"], "router_auth_failed");
    assert_eq!(query.output["http_status"], 401);
    assert_eq!(query.output["operation"], "POST /claims/query");
    assert!(query.output["message"]
        .as_str()
        .unwrap()
        .contains("acn_key_env"));
}

#[tokio::test]
async fn consult_router_rejects_invalid_mode_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_router_client(Arc::new(TestRouter {
            result: sample_router_result(),
            overview: sample_scopes_overview(),
        }));

    let overview_err = registry
        .dispatch(
            "consult_router",
            serde_json::json!({
                "mode": "overview",
                "scope": "router/tool"
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(overview_err.contains("mode=overview 不允许"));

    let query_err = registry
        .dispatch(
            "consult_router",
            serde_json::json!({
                "mode": "query",
                "semantic_query": "semantic only"
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(query_err.contains("mode=query 必须提供非空 scope"));
}
