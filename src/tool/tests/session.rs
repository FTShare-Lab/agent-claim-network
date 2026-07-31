//! Session search 工具测试。
//!
//! 验证搜索结果投影、当前 session 排除与业务失败返回。

use super::*;

#[tokio::test]
async fn session_search_tool_returns_json_and_excludes_current_session() {
    let dir = tempfile::tempdir().unwrap();
    let agent = AgentId::new("agent-a").unwrap();
    let store = SessionStore::new(dir.path().to_path_buf());
    let mut old_session = store
        .create_with_id_factory(
            &agent,
            "system",
            || SessionId::from_str("session_66666666").unwrap(),
            1,
        )
        .await
        .unwrap();
    old_session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            "docker networking old session",
        )])
        .await
        .unwrap();
    let mut current_session = store
        .create_with_id_factory(
            &agent,
            "system",
            || SessionId::from_str("session_77777777").unwrap(),
            1,
        )
        .await
        .unwrap();
    current_session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            "docker networking current session",
        )])
        .await
        .unwrap();
    let service = Arc::new(SessionSearchService::new(
        agent.clone(),
        dir.path().join(agent.as_str()),
        "test-model".into(),
        Arc::new(UnusedSessionSearchSummarizer),
        SessionSearchConfig::default(),
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_session_search(service);

    let result = registry
        .dispatch_with_context(
            "session_search",
            serde_json::json!({"query": "docker", "limit": 3}),
            ToolDispatchContext {
                current_session_id: Some(SessionId::from_str("session_77777777").unwrap()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
    assert_eq!(result.output["success"], true);
    assert_eq!(result.output["count"], 1);
    assert_eq!(
        result.output["results"][0]["session_id"],
        "session_66666666"
    );

    let placeholder_arg = registry
        .dispatch_with_context(
            "session_search",
            serde_json::json!({"_": true, "limit": 3, "sort": "newest"}),
            ToolDispatchContext {
                current_session_id: Some(SessionId::from_str("session_77777777").unwrap()),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(placeholder_arg.output["success"], true);

    let invalid_sort = registry
        .dispatch(
            "session_search",
            serde_json::json!({"query": "docker", "sort": "new"}),
        )
        .await
        .unwrap_err();
    assert!(invalid_sort.to_string().contains("invalid sort"));

    let unknown_arg = registry
        .dispatch(
            "session_search",
            serde_json::json!({"query": "docker", "summary": true}),
        )
        .await
        .unwrap_err();
    assert!(unknown_arg.to_string().contains("unknown field"));
}

#[tokio::test]
async fn session_search_unsuccessful_response_is_typed_business_failure() {
    let dir = tempfile::tempdir().unwrap();
    let blocked_agent_home = dir.path().join("blocked-agent-home");
    tokio::fs::write(&blocked_agent_home, "not a directory")
        .await
        .unwrap();
    let service = Arc::new(SessionSearchService::new(
        AgentId::new("agent-a").unwrap(),
        blocked_agent_home,
        "test-model".into(),
        Arc::new(UnusedSessionSearchSummarizer),
        SessionSearchConfig::default(),
    ));
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .with_session_search(service);

    let result = registry
        .dispatch("session_search", serde_json::json!({"query": "docker"}))
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(result.output["success"], false);
    assert!(result.output["warnings"]
        .as_array()
        .is_some_and(|warnings| !warnings.is_empty()));
}

#[test]
fn memory_path_guard_collapses_dot_segments() {
    assert!(crate::tool::file::ensure_not_memory_path(Path::new(
        "agent-a/memories/sub/../MEMORY.md",
    ))
    .is_err());
    assert!(
        crate::tool::file::ensure_not_memory_path(Path::new("../agent-a/memories/USER.md",))
            .is_err()
    );
    assert!(
        crate::tool::file::ensure_not_memory_path(Path::new("agent-a/notes/MEMORY.md")).is_ok()
    );
}
