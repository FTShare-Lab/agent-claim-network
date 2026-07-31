//! Memory 与 MCP 跨 registry 集成测试。
//!
//! 覆盖父子 registry 共享外部客户端与 memory profile 隔离行为。

use super::*;

#[tokio::test]
async fn file_read_rejects_memory_files_when_memory_store_is_configured() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(dir.path().join("agent-a/memories"))
        .await
        .unwrap();
    tokio::fs::write(dir.path().join("agent-a/memories/MEMORY.md"), "frozen")
        .await
        .unwrap();
    let store = Arc::new(LocalFsMemoryStore::new(
        dir.path().to_path_buf(),
        100,
        100,
        true,
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_memory_store(store);

    let err = registry
        .dispatch(
            "file_read",
            serde_json::json!({ "path": "agent-a/memories/MEMORY.md" }),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("memory 工具"));
}

#[tokio::test]
async fn file_read_rejects_media_when_attachments_disabled() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("shot.png"), tiny_png_bytes())
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_attachment_limits(AttachmentLimits {
            enabled: false,
            ..AttachmentLimits::default()
        });

    let err = registry
        .dispatch("file_read", serde_json::json!({ "path": "shot.png" }))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("附件功能已禁用"));
}

#[tokio::test]
async fn memory_tools_are_exposed_only_when_store_is_configured() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let names = registry
        .definitions()
        .into_iter()
        .map(|t| t.name)
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "memory"));

    let store = Arc::new(LocalFsMemoryStore::new(
        dir.path().to_path_buf(),
        100,
        100,
        true,
    ));
    let registry = registry.with_memory_store(store);
    let names = registry
        .definitions()
        .into_iter()
        .map(|t| t.name)
        .collect::<Vec<_>>();
    assert_eq!(
        names
            .iter()
            .filter(|name| name.as_str() == "memory")
            .count(),
        1
    );
    assert!(names.iter().any(|name| name == "code_run"));
}

#[tokio::test]
async fn memory_capacity_failure_is_typed_and_structured_at_registry_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFsMemoryStore::new(
        dir.path().to_path_buf(),
        10,
        100,
        true,
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_memory_store(store);

    let result = registry
        .dispatch(
            "memory",
            serde_json::json!({
                "action": "add",
                "target": "memory",
                "content": "safe but too long",
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(result.output["success"], false);
    assert_eq!(result.output["cap"], 10);
    assert!(result.output["need_free"].is_number());
    assert!(result.output["current_entries"].is_array());
}

#[tokio::test]
async fn memory_review_registry_exposes_only_memory_tool_surface() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFsMemoryStore::new(
        dir.path().to_path_buf(),
        100,
        100,
        true,
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_memory_store(store)
        .for_memory_review();
    let names = registry
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "memory"));
    for forbidden in [
        "code_run",
        "file_read",
        "file_patch",
        "file_write",
        "web_search",
        "web_fetch",
        "web_request",
        "ask_user",
        "create_subagent",
    ] {
        assert!(!names.iter().any(|name| name == forbidden));
        let err = registry
            .dispatch(forbidden, serde_json::json!({}))
            .await
            .expect_err("memory review should reject non-memory tools");
        assert!(matches!(err, ToolError::UnknownTool(_)));
    }
}

#[tokio::test]
async fn delegation_registry_inherits_parent_visible_mcp_tools() {
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("stdio_mcp_tool.sh");
    tokio::fs::write(&script_path, super::registry::stdio_mcp_tool_script())
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
        .with_mcp_manager(mcp_manager)
        .for_delegation(None);

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
}

#[tokio::test]
async fn delegation_mcp_calls_same_http_server_accept_transport_serialization() {
    use axum::routing::post;
    use axum::Router;
    use tokio::net::TcpListener;

    let active_calls = Arc::new(AtomicUsize::new(0));
    let max_active_calls = Arc::new(AtomicUsize::new(0));
    let handler_active = Arc::clone(&active_calls);
    let handler_max = Arc::clone(&max_active_calls);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/mcp",
        post(move |axum::Json(payload): axum::Json<Value>| {
            let active_calls = Arc::clone(&handler_active);
            let max_active_calls = Arc::clone(&handler_max);
            async move {
                super::registry::parallel_mcp_test_response(payload, active_calls, max_active_calls)
                    .await
            }
        })
        .get(|| async { axum::http::StatusCode::METHOD_NOT_ALLOWED }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let mcp_config_path = dir.path().join(".mcp.json");
    let mut mcp_cfg = crate::mcp::config::McpJsonConfig::default();
    mcp_cfg.servers.insert(
        "parallel".to_string(),
        crate::mcp::config::McpServerConfig::streamable_http(format!("http://{addr}/mcp"), None),
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
    let registry = Arc::new(
        ToolRegistry::new(&test_tool_config(dir.path()))
            .unwrap()
            .with_mcp_manager(mcp_manager)
            .for_delegation(None),
    );

    let first_registry = Arc::clone(&registry);
    let second_registry = Arc::clone(&registry);
    let (first, second) = tokio::join!(
        async move {
            first_registry
                .dispatch("mcp__parallel__slow", json!({"request": "first"}))
                .await
        },
        async move {
            second_registry
                .dispatch("mcp__parallel__slow", json!({"request": "second"}))
                .await
        }
    );

    for result in [first.unwrap(), second.unwrap()] {
        assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
        assert_eq!(result.output["content"][0]["text"], "slow-done");
    }
    // 已拍板：同一 Streamable HTTP session 的慢 JSON response 会在 rmcp transport worker
    // 内串行；这里验证 ACN 不额外建立短连接绕开该语义。
    assert_eq!(max_active_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parent_and_two_children_share_one_stdio_client_without_cross_agent_lock() {
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("parallel_stdio_mcp_tool.sh");
    let log_path = dir.path().join("parallel-stdio-events.log");
    tokio::fs::write(
        &script_path,
        super::registry::parallel_stdio_mcp_tool_script(),
    )
    .await
    .unwrap();
    let mcp_config_path = dir.path().join(".mcp.json");
    let mut mcp_cfg = crate::mcp::config::McpJsonConfig::default();
    let mut env = BTreeMap::new();
    env.insert(
        "MCP_FIXTURE_LOG".to_string(),
        log_path.display().to_string(),
    );
    mcp_cfg.servers.insert(
        "parallel".to_string(),
        crate::mcp::config::McpServerConfig::stdio(
            "sh".to_string(),
            vec![script_path.display().to_string()],
            env,
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
    let parent = Arc::new(
        ToolRegistry::new(&test_tool_config(dir.path()))
            .unwrap()
            .with_mcp_manager(Arc::clone(&mcp_manager)),
    );
    let child_a = Arc::new(((*parent).clone()).for_delegation(None));
    let child_b = Arc::new(((*parent).clone()).for_delegation(None));

    let parent_call = Arc::clone(&parent);
    let child_a_call = Arc::clone(&child_a);
    let child_b_call = Arc::clone(&child_b);
    let (parent_result, child_a_result, child_b_result) = tokio::join!(
        async move {
            parent_call
                .dispatch("mcp__parallel__slow", json!({"request": "parent"}))
                .await
        },
        async move {
            child_a_call
                .dispatch("mcp__parallel__slow", json!({"request": "child-a"}))
                .await
        },
        async move {
            child_b_call
                .dispatch("mcp__parallel__slow", json!({"request": "child-b"}))
                .await
        }
    );

    for result in [
        parent_result.unwrap(),
        child_a_result.unwrap(),
        child_b_result.unwrap(),
    ] {
        assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
        assert_eq!(result.output["content"][0]["text"], "slow-done");
    }
    let events = tokio::fs::read_to_string(&log_path).await.unwrap();
    let event_json = events
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        event_json
            .iter()
            .filter(|event| event["event"] == "initialize")
            .count(),
        1,
        "events={events}"
    );
    assert_eq!(
        event_json
            .iter()
            .filter(|event| event["event"] == "start")
            .count(),
        3,
        "events={events}"
    );
    let starts = event_json
        .iter()
        .filter(|event| event["event"] == "start")
        .map(|event| {
            (
                event["id"].as_str().unwrap(),
                event["pid"].as_u64().unwrap(),
                event["ts"].as_f64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let ends = event_json
        .iter()
        .filter(|event| event["event"] == "end")
        .map(|event| {
            (
                event["id"].as_str().unwrap(),
                event["pid"].as_u64().unwrap(),
                event["ts"].as_f64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(ends.len(), 3, "events={events}");
    let pids = starts
        .iter()
        .map(|(_, pid, _)| *pid)
        .collect::<BTreeSet<_>>();
    assert_eq!(pids.len(), 1, "events={events}");
    assert!(
        starts
            .iter()
            .all(|(id, _, _)| ends.iter().any(|(end_id, _, _)| end_id == id)),
        "every request must have a matching server-side end event; events={events}"
    );
    for (index, (first_id, _, first_start)) in starts.iter().enumerate() {
        let first_end = ends
            .iter()
            .find(|(id, _, _)| id == first_id)
            .map(|(_, _, timestamp)| *timestamp)
            .unwrap();
        for (second_id, _, second_start) in starts.iter().skip(index + 1) {
            let second_end = ends
                .iter()
                .find(|(id, _, _)| id == second_id)
                .map(|(_, _, timestamp)| *timestamp)
                .unwrap();
            assert!(
                first_start < &second_end && second_start < &first_end,
                "parent/child or child/child requests were serialized by ACN; events={events}"
            );
        }
    }
}
