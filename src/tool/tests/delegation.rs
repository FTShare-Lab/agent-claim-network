//! Delegation 工具定义、执行、等待与子 agent 边界测试。
//!
//! 验证 subagent 生命周期、权限面、文件路径语义与等待协议。

use super::*;

#[test]
fn registry_exposes_core_file_tools() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let tools = registry.definitions();

    let names = tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "code_run",
            "write_stdin",
            "process_list",
            "file_read",
            "file_patch",
            "file_write",
            "web_search",
            "web_fetch",
            "web_request",
            "working_note",
            "ask_user"
        ]
    );
    let read = tools.iter().find(|t| t.name == "file_read").unwrap();
    assert!(read
        .input_schema
        .get("properties")
        .and_then(|v| v.get("path"))
        .is_some());
}

#[tokio::test]
async fn parent_registry_exposes_delegation_tools_and_can_create_list() {
    let dir = tempfile::tempdir().unwrap();
    let agents_root = dir.path().join("agents");
    let agent_home = agents_root.join("agent-a");
    let agent_id = AgentId::new("agent-a").unwrap();
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let store = SessionStore::new(agents_root);
    store
        .create_with_id_factory(&agent_id, "system", || session_id.clone(), 1)
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_delegation_executor(
            agent_home.clone(),
            agent_id,
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig::default(),
        );
    let definitions = registry.definitions();
    let create_definition = definitions
        .iter()
        .find(|tool| tool.name == "create_subagent")
        .unwrap();
    assert!(create_definition
        .description
        .contains("update_subagent_progress"));
    assert!(create_definition
        .description
        .contains("cannot ask the parent and wait for a reply"));
    let steer_definition = definitions
        .iter()
        .find(|tool| tool.name == "steer_subagent")
        .unwrap();
    assert!(steer_definition.description.contains("no acknowledgement"));
    assert!(steer_definition
        .description
        .contains("before a future subagent model request"));
    let read_definition = definitions
        .iter()
        .find(|tool| tool.name == "read_subagent")
        .unwrap();
    assert!(read_definition.input_schema["properties"]
        .get("path")
        .is_none());
    assert!(!read_definition.input_schema["properties"]["mode"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .any(|mode| mode == "artifact"));
    let names = definitions
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "create_subagent"));
    assert!(names.iter().any(|name| name == "list_subagents"));
    assert!(names.iter().any(|name| name == "read_subagent"));
    assert!(names.iter().any(|name| name == "steer_subagent"));
    assert!(names.iter().any(|name| name == "wait_subagents"));

    let created = registry
        .dispatch_with_context(
            "create_subagent",
            json!({
                "title": "scan files",
                "role": "researcher",
                "objective": "scan the files",
                "constraints": ["keep it short"]
            }),
            ToolDispatchContext {
                current_session_id: Some(session_id.clone()),
                current_turn_id: Some("turn_1".into()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(created.output["subagent"]["title"], "scan files");

    tokio::time::sleep(Duration::from_millis(20)).await;
    let listed = registry
        .dispatch_with_context(
            "list_subagents",
            json!({}),
            ToolDispatchContext {
                current_session_id: Some(session_id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.output["subagents"].as_array().unwrap().len(), 1);
    let listed_summary = &listed.output["subagents"][0];
    assert!(listed_summary["created_at"].is_string());
    assert!(listed_summary["updated_at"].is_string());
    assert!(listed_summary["started_at"].is_string() || listed_summary["started_at"].is_null());
    assert!(listed_summary["completed_at"].is_string() || listed_summary["completed_at"].is_null());
    let list_err = registry
        .dispatch_with_context(
            "list_subagents",
            json!({"id": "subagent_11111111"}),
            ToolDispatchContext {
                current_session_id: Some(session_id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .expect_err("list_subagents should reject unexpected args");
    assert!(matches!(list_err, ToolError::InvalidArgs(_)));
    let id = created.output["subagent"]["id"].as_str().unwrap();
    assert!(id.starts_with("subagent_"));
    let read_summary = registry
        .dispatch_with_context(
            "read_subagent",
            json!({
                "id": id,
                "mode": "summary",
            }),
            ToolDispatchContext {
                current_session_id: Some(session_id.clone()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert!(read_summary.output["summary"]["created_at"].is_string());
    assert!(read_summary.output["summary"]["updated_at"].is_string());
    assert!(
        read_summary.output["summary"]["started_at"].is_string()
            || read_summary.output["summary"]["started_at"].is_null()
    );
    assert!(
        read_summary.output["summary"]["completed_at"].is_string()
            || read_summary.output["summary"]["completed_at"].is_null()
    );
    assert!(agent_home
        .join("sessions")
        .join(session_id.as_str())
        .join("delegations")
        .exists());
}

#[test]
fn wait_subagents_and_code_run_descriptions_use_runtime_limits() {
    let dir = tempfile::tempdir().unwrap();
    let mut tool_config = test_tool_config(dir.path());
    tool_config.code_run_initial_yield_ms = 1_230;
    tool_config.code_run_min_yield_ms = 300;
    tool_config.code_run_max_yield_ms = 3_210;
    tool_config.write_stdin_max_poll_timeout_ms = 90_000;
    let registry = ToolRegistry::new(&tool_config)
        .unwrap()
        .with_delegation_executor(
            dir.path().join("agents/agent-a"),
            AgentId::new("agent-a").unwrap(),
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig {
                max_concurrent: 1,
                wall_timeout: Duration::from_secs(5),
                wait: DelegationWaitConfig {
                    default_timeout: Duration::from_secs(27),
                    min_timeout: Duration::from_secs(11),
                    max_timeout: Duration::from_secs(123),
                },
            },
        );
    let tools = registry.definitions();
    let code_run = tools.iter().find(|tool| tool.name == "code_run").unwrap();
    assert!(code_run.description.contains("logical process_id"));
    assert_eq!(
        code_run.input_schema["properties"]["yield_time_ms"]["maximum"],
        3210
    );
    assert!(
        code_run.input_schema["properties"]["yield_time_ms"]["description"]
            .as_str()
            .unwrap()
            .contains("Defaults to 1230; clamped to 300..3210.")
    );
    let write_stdin = tools
        .iter()
        .find(|tool| tool.name == "write_stdin")
        .unwrap();
    assert!(write_stdin.input_schema["properties"]["chars"]
        .get("maxLength")
        .is_none());
    assert_eq!(
        write_stdin.input_schema["properties"]["yield_time_ms"]["maximum"],
        90_000
    );
    assert_eq!(
        write_stdin.input_schema["properties"]["max_output_chars"]["default"],
        tool_config.code_run_max_output_chars
    );
    assert_eq!(
        write_stdin.input_schema["properties"]["terminate"]["default"],
        false
    );
    assert!(
        write_stdin.input_schema["properties"]["chars"]["description"]
            .as_str()
            .unwrap()
            .contains("UTF-8 bytes at runtime")
    );
    assert!(write_stdin
        .description
        .contains("does not close an interactive shell or SSH session"));
    assert!(write_stdin
        .description
        .contains("terminate=true hard-terminates the managed process group"));
    assert!(write_stdin
        .description
        .contains("state describes the outer managed process"));

    let process_list = tools
        .iter()
        .find(|tool| tool.name == "process_list")
        .unwrap();
    assert!(process_list
        .description
        .contains("including processes owned by direct subagents"));
    assert!(process_list
        .description
        .contains("Each result includes owner"));
    assert!(write_stdin
        .description
        .contains("only empty chars (read-only poll), exactly Ctrl-C"));
    assert!(write_stdin
        .description
        .contains("or terminate=true is accepted"));

    let child_tools = registry.clone().for_delegation(None).definitions();
    let child_process_list = child_tools
        .iter()
        .find(|tool| tool.name == "process_list")
        .unwrap();
    assert!(child_process_list
        .description
        .contains("List only your currently live code_run processes"));
    assert!(child_process_list
        .description
        .contains("cannot see your parent/main agent, sibling subagents"));
    let child_write_stdin = child_tools
        .iter()
        .find(|tool| tool.name == "write_stdin")
        .unwrap();
    assert!(child_write_stdin
        .description
        .contains("only your own live code_run processes"));

    let wait = tools
        .iter()
        .find(|tool| tool.name == "wait_subagents")
        .unwrap();
    assert!(wait.description.contains("27 seconds"));
    assert!(wait.description.contains("11 and 123 seconds"));
    assert_eq!(
        wait.input_schema["properties"]["timeout_secs"]["minimum"],
        11
    );
    assert_eq!(
        wait.input_schema["properties"]["timeout_secs"]["maximum"],
        123
    );
}

#[test]
fn code_run_yield_uses_configured_default_and_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let mut tool_config = test_tool_config(dir.path());
    tool_config.code_run_initial_yield_ms = 450;
    tool_config.code_run_min_yield_ms = 250;
    tool_config.code_run_max_yield_ms = 900;
    tool_config.write_stdin_max_poll_timeout_ms = 60_000;
    let registry = ToolRegistry::new(&tool_config).unwrap();

    assert_eq!(registry.clamp_yield_time(None), Duration::from_millis(450));
    assert_eq!(
        registry.clamp_yield_time(Some(30)),
        Duration::from_millis(250)
    );
    assert_eq!(
        registry.clamp_yield_time(Some(1200)),
        Duration::from_millis(900)
    );
    assert_eq!(
        registry.clamp_write_yield_time(Some(120_000), true),
        Duration::from_secs(60)
    );
}

#[tokio::test]
async fn wait_subagents_returns_no_active_and_rejects_invalid_explicit_ids() {
    let (dir, registry, session_id, _executor) = wait_test_registry().await;
    let no_active = registry
        .dispatch_with_context("wait_subagents", json!({}), wait_test_context(&session_id))
        .await
        .unwrap();
    assert_eq!(no_active.output["outcome"], "no_active_subagents");
    assert_eq!(no_active.output["waited_subagent_ids"], json!([]));

    let duplicate = registry
        .dispatch_with_context(
            "wait_subagents",
            json!({"subagent_ids": ["subagent_11111111", "subagent_11111111"]}),
            wait_test_context(&session_id),
        )
        .await
        .expect_err("duplicate IDs must fail before store lookup");
    assert!(matches!(duplicate, ToolError::InvalidArgs(_)));

    let unknown = registry
        .dispatch_with_context(
            "wait_subagents",
            json!({"subagent_ids": ["subagent_11111111"]}),
            wait_test_context(&session_id),
        )
        .await
        .expect_err("unknown current-session ID must fail");
    assert!(matches!(unknown, ToolError::InvalidArgs(_)));
    assert!(unknown.to_string().contains("不属于当前 session 或不存在"));

    let foreign_session_id = SessionId::from_str("session_87654321").unwrap();
    let agent_id = AgentId::new("agent-a").unwrap();
    SessionStore::new(dir.path().join("agents"))
        .create_with_id_factory(&agent_id, "system", || foreign_session_id.clone(), 1)
        .await
        .unwrap();
    let foreign_id = create_wait_test_subagent(&registry, &foreign_session_id, "slow").await;
    let cross_session = registry
        .dispatch_with_context(
            "wait_subagents",
            json!({"subagent_ids": [foreign_id]}),
            wait_test_context(&session_id),
        )
        .await
        .expect_err("foreign-session ID must fail");
    assert!(matches!(cross_session, ToolError::InvalidArgs(_)));
    assert!(cross_session
        .to_string()
        .contains("不属于当前 session 或不存在"));
    registry
        .abandon_delegations_for_session(&foreign_session_id, "test cleanup")
        .await
        .unwrap();
}

#[tokio::test]
async fn wait_subagents_any_terminal_reports_pending_and_all_terminal_accepts_abandoned() {
    let (_dir, registry, session_id, executor) = wait_test_registry().await;
    let slow_started = executor.slow_started.notified();
    let slow_id = create_wait_test_subagent(&registry, &session_id, "slow").await;
    tokio::time::timeout(Duration::from_millis(200), slow_started)
        .await
        .expect("slow subagent should start");
    let fast_id = create_wait_test_subagent(&registry, &session_id, "fast").await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    let any = registry
        .dispatch_with_context(
            "wait_subagents",
            json!({"subagent_ids": [fast_id, slow_id], "until": "any_terminal", "timeout_secs": 1}),
            wait_test_context(&session_id),
        )
        .await
        .unwrap();
    assert_eq!(any.output["outcome"], "condition_met");
    assert_eq!(
        any.output["terminal_subagents"].as_array().unwrap().len(),
        1
    );
    assert_eq!(any.output["pending_subagent_ids"], json!([slow_id]));

    let abandoned = registry
        .abandon_delegations_for_session(&session_id, "test abandon")
        .await
        .unwrap();
    assert_eq!(abandoned, 1);
    let all = registry
            .dispatch_with_context(
                "wait_subagents",
                json!({"subagent_ids": any.output["waited_subagent_ids"], "until": "all_terminal", "timeout_secs": 1}),
                wait_test_context(&session_id),
            )
            .await
            .unwrap();
    assert_eq!(all.output["outcome"], "condition_met");
    assert_eq!(all.output["pending_subagent_ids"], json!([]));
    assert_eq!(
        all.output["terminal_subagents"].as_array().unwrap().len(),
        2
    );
    assert!(all.output["terminal_subagents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["status"] == "abandoned"));
}

#[tokio::test]
async fn wait_subagents_progress_wakes_check_without_early_return() {
    let (_dir, registry, session_id, executor) = wait_test_registry().await;
    let slow_started = executor.slow_started.notified();
    let slow_id = create_wait_test_subagent(&registry, &session_id, "slow").await;
    tokio::time::timeout(Duration::from_millis(200), slow_started)
        .await
        .expect("slow subagent should start");

    let wait_registry = Arc::clone(&registry);
    let wait_session_id = session_id.clone();
    let wait_task = tokio::spawn(async move {
        wait_registry
            .dispatch_with_context(
                "wait_subagents",
                json!({"subagent_ids": [slow_id], "until": "all_terminal", "timeout_secs": 1}),
                wait_test_context(&wait_session_id),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let progress_recorded = executor.progress_recorded.notified();
    executor.progress_gate.notify_one();
    tokio::time::timeout(Duration::from_millis(200), progress_recorded)
        .await
        .expect("progress should persist");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !wait_task.is_finished(),
        "ordinary progress must not resolve terminal wait"
    );

    executor.release.notify_one();
    let waited = tokio::time::timeout(Duration::from_millis(500), wait_task)
        .await
        .expect("terminal state should wake wait")
        .unwrap()
        .unwrap();
    assert_eq!(waited.output["outcome"], "condition_met");
    assert_eq!(waited.output["pending_subagent_ids"], json!([]));
}

#[tokio::test]
async fn wait_subagents_omitted_ids_are_fixed_at_call_start_and_support_cancellation() {
    let (_dir, registry, session_id, executor) = wait_test_registry().await;
    let slow_started = executor.slow_started.notified();
    let _slow_id = create_wait_test_subagent(&registry, &session_id, "slow").await;
    tokio::time::timeout(Duration::from_millis(200), slow_started)
        .await
        .expect("slow subagent should start");

    let cancellation = CancellationToken::new();
    let wait_registry = Arc::clone(&registry);
    let wait_session_id = session_id.clone();
    let wait_cancellation = cancellation.clone();
    let wait_task = tokio::spawn(async move {
        wait_registry
            .dispatch_with_context(
                "wait_subagents",
                json!({"until": "all_terminal", "timeout_secs": 1}),
                ToolDispatchContext {
                    current_session_id: Some(wait_session_id),
                    cancellation: Some(wait_cancellation),
                    ..ToolDispatchContext::default()
                },
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let _fast_id = create_wait_test_subagent(&registry, &session_id, "fast").await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !wait_task.is_finished(),
        "a subagent created after the call must not satisfy the fixed snapshot"
    );
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_millis(500), wait_task)
        .await
        .expect("cancellation should wake wait")
        .unwrap()
        .expect_err("cancelled wait must not return a normal tool result");
    assert!(matches!(error, ToolError::Interrupted));

    let progress_recorded = executor.progress_recorded.notified();
    executor.progress_gate.notify_one();
    tokio::time::timeout(Duration::from_millis(200), progress_recorded)
        .await
        .expect("progress should persist before cleanup");
    executor.release.notify_one();
}

#[tokio::test]
async fn wait_subagents_times_out_with_bounded_status_only() {
    let (_dir, registry, session_id, executor) = wait_test_registry().await;
    let slow_started = executor.slow_started.notified();
    let slow_id = create_wait_test_subagent(&registry, &session_id, "slow").await;
    tokio::time::timeout(Duration::from_millis(200), slow_started)
        .await
        .expect("slow subagent should start");

    let timed_out = registry
        .dispatch_with_context(
            "wait_subagents",
            json!({"subagent_ids": [slow_id], "timeout_secs": 1}),
            wait_test_context(&session_id),
        )
        .await
        .unwrap();
    assert_eq!(timed_out.output["outcome"], "timeout");
    assert!(timed_out.output.get("result").is_none());
    assert!(timed_out.output.get("transcript").is_none());
    assert!(timed_out.output["terminal_subagents"]
        .as_array()
        .unwrap()
        .is_empty());

    let progress_recorded = executor.progress_recorded.notified();
    executor.progress_gate.notify_one();
    tokio::time::timeout(Duration::from_millis(200), progress_recorded)
        .await
        .expect("progress should persist before cleanup");
    executor.release.notify_one();
}

#[tokio::test]
async fn read_subagent_rejects_removed_artifact_inputs_before_store_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_delegation_executor(
            dir.path().join("agents/agent-a"),
            AgentId::new("agent-a").unwrap(),
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig::default(),
        );
    let context = ToolDispatchContext {
        current_session_id: Some(session_id),
        ..ToolDispatchContext::default()
    };

    let summary_with_path = registry
        .dispatch_with_context(
            "read_subagent",
            json!({
                "id": "subagent_12345678",
                "mode": "summary",
                "path": "result.md"
            }),
            context.clone(),
        )
        .await
        .expect_err("removed path argument must be rejected by the read DTO");
    let ToolError::InvalidArgs(message) = summary_with_path else {
        panic!("path must fail before Store lookup: {summary_with_path}");
    };
    assert!(message.contains("unknown field `path`"));

    let artifact_mode = registry
        .dispatch_with_context(
            "read_subagent",
            json!({
                "id": "subagent_12345678",
                "mode": "artifact"
            }),
            context,
        )
        .await
        .expect_err("removed artifact mode must be rejected before Store lookup");
    let ToolError::InvalidArgs(message) = artifact_mode else {
        panic!("artifact mode must fail before Store lookup: {artifact_mode}");
    };
    assert!(message.contains("未知 mode: artifact"));
}

#[tokio::test]
async fn create_subagent_requires_parent_turn_id() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_delegation_executor(
            dir.path().join("agents/agent-a"),
            AgentId::new("agent-a").unwrap(),
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig::default(),
        );

    let err = registry
        .dispatch_with_context(
            "create_subagent",
            json!({
                "title": "scan files",
                "role": "researcher",
                "objective": "scan the files"
            }),
            ToolDispatchContext {
                current_session_id: Some(SessionId::from_str("session_1234abcd").unwrap()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("parent turn id"));
}

#[tokio::test]
async fn create_subagent_rejects_non_open_parent_session() {
    let dir = tempfile::tempdir().unwrap();
    let agents_root = dir.path().join("agents");
    let agent_home = agents_root.join("agent-a");
    let agent_id = AgentId::new("agent-a").unwrap();
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let store = SessionStore::new(agents_root);
    let mut session = store
        .create_with_id_factory(&agent_id, "system", || session_id.clone(), 1)
        .await
        .unwrap();
    session.mark_finalizing(chrono::Utc::now()).await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_delegation_executor(
            agent_home,
            agent_id,
            Arc::new(ImmediateDelegationExecutor),
            DelegationRunnerConfig::default(),
        );

    let err = registry
        .dispatch_with_context(
            "create_subagent",
            json!({
                "title": "scan files",
                "role": "researcher",
                "objective": "scan the files"
            }),
            ToolDispatchContext {
                current_session_id: Some(session_id),
                current_turn_id: Some("turn_1".into()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .expect_err("finalizing session should reject delegation creation");

    assert!(err.to_string().contains("不能创建 subagent"));
}

#[tokio::test]
async fn delegation_registry_exposes_execution_web_and_hides_acn_stateful_tools() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFsMemoryStore::new(
        dir.path().join("memories"),
        100,
        100,
        true,
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_memory_store(store)
        .with_router_client(Arc::new(TestRouter {
            result: RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            },
            overview: ScopesOverviewSnapshot { scopes: Vec::new() },
        }))
        .for_delegation(None);
    let names = registry
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "code_run"));
    assert!(names.iter().any(|name| name == "file_write"));
    assert!(names.iter().any(|name| name == "web_search"));
    assert!(names.iter().any(|name| name == "web_fetch"));
    assert!(ToolAccessProfile::delegation().mcp);
    assert!(names.iter().any(|name| name == "web_request"));
    assert!(!names.iter().any(|name| name == "ask_user"));
    assert!(!names.iter().any(|name| name == "working_note"));
    assert!(!names.iter().any(|name| name == "memory"));
    assert!(!names.iter().any(|name| name == "consult_router"));
    assert!(!names.iter().any(|name| name == "session_search"));
    assert!(!names.iter().any(|name| name == "create_subagent"));
    assert!(!names.iter().any(|name| name == "list_subagents"));
    assert!(!names.iter().any(|name| name == "wait_subagents"));
    assert!(!names.iter().any(|name| name == "read_subagent"));
    assert!(!names.iter().any(|name| name == "steer_subagent"));

    let output = registry
        .dispatch("code_run", json!({"script": "echo nope"}))
        .await
        .unwrap();
    assert_eq!(output.output["success"], true);
    assert!(output.output["stdout"].as_str().unwrap().contains("nope"));
}

#[tokio::test]
async fn delegation_progress_tool_uses_only_the_public_subagent_contract() {
    let dir = tempfile::tempdir().unwrap();
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let store = DelegationStore::new_for_session(
        dir.path().join("sessions/session_1234abcd"),
        session_id.clone(),
    );
    let metadata = store
        .create(DelegationCreateRequest {
            parent_session_id: session_id,
            parent_turn_id: "turn_1".into(),
            owner_agent_id: AgentId::new("agent-a").unwrap(),
            title: "progress test".into(),
            role: "verifier".into(),
            objective: "verify the public progress tool contract".into(),
            constraints: Vec::new(),
        })
        .await
        .unwrap();
    let metadata = store.start(&metadata.id).await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_delegation(Some(DelegationProgressSink::for_test(
            store,
            metadata.id.clone(),
        )));
    let names = registry
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "update_subagent_progress"));

    let updated = registry
        .dispatch(
            "update_subagent_progress",
            json!({
                "current_step": "checking",
                "summary": "checking the public subagent contract"
            }),
        )
        .await
        .unwrap();
    assert_eq!(updated.output["subagent"]["id"], metadata.id.as_str());
    assert!(updated.output.get("delegation").is_none());
}

#[tokio::test]
async fn delegation_child_code_run_injects_identity_env() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_delegation(None);
    let session_id = SessionId::from_str("session_1234abcd").unwrap();

    let output = registry
            .dispatch_with_context(
                "code_run",
                json!({
                    "script": "printf '%s\\n%s\\nlegacy=%s\\n' \"$ACN_SUBAGENT_ID\" \"$ACN_PARENT_SESSION_ID\" \"${ACN_DELEGATION_ID-unset}\""
                }),
                ToolDispatchContext {
                    current_session_id: Some(session_id),
                    current_turn_id: Some("subagent_87654321".into()),
                    ..ToolDispatchContext::default()
                },
            )
            .await
            .unwrap();

    assert_eq!(output.output["success"], true);
    let stdout = output.output["stdout"].as_str().unwrap();
    assert!(stdout.contains("subagent_87654321"));
    assert!(stdout.contains("session_1234abcd"));
    assert!(stdout.contains("legacy=unset"));
}

#[tokio::test]
async fn delegation_child_file_tools_follow_parent_path_semantics() {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let external = outside.path().join("external.txt");
    tokio::fs::write(&external, "secret").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(workspace.path()))
        .unwrap()
        .for_delegation(None);
    let context = ToolDispatchContext {
        current_session_id: Some(SessionId::from_str("session_1234abcd").unwrap()),
        current_turn_id: Some("subagent_12345678".into()),
        ..ToolDispatchContext::default()
    };

    let read = registry
        .dispatch_with_context(
            "file_read",
            json!({ "path": external.display().to_string() }),
            context.clone(),
        )
        .await
        .unwrap();
    assert!(read.output["content"].as_str().unwrap().contains("secret"));

    let new_file = outside.path().join("new.txt");
    registry
        .dispatch_with_context(
            "file_write",
            json!({
                "path": new_file.display().to_string(),
                "content": "created"
            }),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read_to_string(&new_file).await.unwrap(),
        "created"
    );

    registry
        .dispatch_with_context(
            "file_patch",
            json!({
                "path": external.display().to_string(),
                "old_content": "secret",
                "new_content": "changed"
            }),
            context,
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read_to_string(&external).await.unwrap(),
        "changed"
    );
}

#[tokio::test]
async fn delegation_child_file_tools_can_read_workspace_secret_like_files() {
    let dir = tempfile::tempdir().unwrap();
    for path in [
        "export_env.sh",
        ".env",
        ".mcp.json",
        ".cargo/credentials.toml",
        ".docker/config.json",
        ".kube/config",
        ".config/gcloud/application_default_credentials.json",
    ] {
        let full_path = dir.path().join(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&full_path, "SECRET=value").await.unwrap();
    }
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_delegation(None);

    for path in [
        "export_env.sh",
        ".env",
        ".mcp.json",
        ".cargo/credentials.toml",
        ".docker/config.json",
        ".kube/config",
        ".config/gcloud/application_default_credentials.json",
    ] {
        let output = registry
            .dispatch("file_read", json!({ "path": path }))
            .await
            .unwrap();
        assert!(output.output["content"]
            .as_str()
            .unwrap()
            .contains("SECRET=value"));
    }
}

#[tokio::test]
async fn delegation_child_web_request_can_access_localhost() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let body = "delegation localhost";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_delegation(None);
    let result = registry
        .dispatch(
            "web_request",
            json!({
                "method": "GET",
                "url": format!("http://127.0.0.1:{port}/status")
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.output["http_status"], 200);
    assert_eq!(result.output["body"]["_raw"], "delegation localhost");
}

#[tokio::test]
async fn delegation_child_file_read_still_returns_bounded_tool_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.txt");
    tokio::fs::write(&path, "a".repeat(16_000)).await.unwrap();
    let mut config = test_tool_config(dir.path());
    config.file_read_max_chars = 128;
    let registry = ToolRegistry::new(&config).unwrap().for_delegation(None);

    let output = registry
        .dispatch("file_read", json!({ "path": path.display().to_string() }))
        .await
        .unwrap();

    assert_eq!(output.output["truncated"], true);
    assert!(
        output.output["content"].as_str().unwrap().chars().count()
            <= config.file_read_max_chars + 3
    );
}
