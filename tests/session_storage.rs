use agent_claim_network::claim::{AgentId, SessionId};
use agent_claim_network::session::{
    NewSessionMessage, SessionContentBlock, SessionMessageRole, SessionStatus, SessionStore,
};
use agent_claim_network::storage::paths;
use chrono::Utc;
use std::cell::Cell;

#[tokio::test]
async fn t_session_create_writes_agent_home_files_without_embedded_transcript() {
    let tmp = tempfile::tempdir().unwrap();
    let agents_root = tmp.path().join("agents");
    let team_root = tmp.path().join("team");
    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = "session_aaaaaaaa".parse().unwrap();
    let store = SessionStore::new(agents_root.clone());

    let created = store
        .create_with_id_factory(&agent, "agent system prompt", || session_id.clone(), 1)
        .await
        .unwrap();

    let expected_dir = agents_root
        .join(agent.as_str())
        .join("sessions")
        .join(session_id.as_str());
    assert_eq!(created.paths.dir, expected_dir);
    assert!(!created.paths.dir.starts_with(&team_root));
    assert!(created.paths.session_yaml.exists());
    assert!(created.paths.system_prompt.exists());
    assert!(created.paths.messages_jsonl.exists());
    assert_eq!(
        tokio::fs::read_to_string(&created.paths.system_prompt)
            .await
            .unwrap(),
        "agent system prompt"
    );
    assert_eq!(
        tokio::fs::read_to_string(&created.paths.messages_jsonl)
            .await
            .unwrap(),
        ""
    );

    let metadata = created.read_metadata().await.unwrap();
    assert_eq!(metadata.id, session_id);
    assert_eq!(metadata.agent_id, agent);
    assert_eq!(metadata.status, SessionStatus::Open);
    assert_eq!(metadata.system_prompt_path, "system_prompt.md");
    assert_eq!(metadata.message_count, 0);
    assert!(metadata.closed_at.is_none());
    assert!(metadata.finalized_at.is_none());

    let raw_yaml = tokio::fs::read_to_string(&created.paths.session_yaml)
        .await
        .unwrap();
    assert!(!raw_yaml.contains("agent system prompt"));
    assert!(!raw_yaml.contains("messages"));
}

#[tokio::test]
async fn t_session_id_retries_when_existing_session_dir_collides() {
    let tmp = tempfile::tempdir().unwrap();
    let agents_root = tmp.path().join("agents");
    let agent = AgentId::new("agent-a").unwrap();
    let collide: SessionId = "session_11111111".parse().unwrap();
    let unique: SessionId = "session_22222222".parse().unwrap();
    let existing_dir = paths::agent_home_session_dir(&agents_root.join(agent.as_str()), &collide);
    tokio::fs::create_dir_all(&existing_dir).await.unwrap();
    let calls = Cell::new(0usize);
    let store = SessionStore::new(agents_root);

    let created = store
        .create_with_id_factory(
            &agent,
            "",
            || {
                let n = calls.get();
                calls.set(n + 1);
                if n == 0 {
                    collide.clone()
                } else {
                    unique.clone()
                }
            },
            2,
        )
        .await
        .unwrap();

    assert_eq!(created.metadata.id, unique);
    assert_eq!(calls.get(), 2);
}

#[tokio::test]
async fn t_session_id_bails_after_max_attempts_collide() {
    let tmp = tempfile::tempdir().unwrap();
    let agents_root = tmp.path().join("agents");
    let agent = AgentId::new("agent-a").unwrap();
    let collide: SessionId = "session_33333333".parse().unwrap();
    let existing_dir = paths::agent_home_session_dir(&agents_root.join(agent.as_str()), &collide);
    tokio::fs::create_dir_all(&existing_dir).await.unwrap();
    let store = SessionStore::new(agents_root);

    let err = store
        .create_with_id_factory(&agent, "", || collide.clone(), 2)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("尝试 2 次仍碰撞"));
}

#[tokio::test]
async fn t_session_messages_append_with_contiguous_indexes_and_read_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let agents_root = tmp.path().join("agents");
    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = "session_44444444".parse().unwrap();
    let store = SessionStore::new(agents_root);
    let mut session = store
        .create_with_id_factory(&agent, "prompt", || session_id.clone(), 1)
        .await
        .unwrap();

    session
        .append_messages(&[
            NewSessionMessage::text_with_model(SessionMessageRole::User, "第一轮问题", "model-a"),
            NewSessionMessage::with_model(
                SessionMessageRole::Assistant,
                vec![
                    SessionContentBlock::text("我先检查。"),
                    SessionContentBlock::tool_use(
                        "toolu_1",
                        "file_read",
                        serde_json::json!({"path": "src/payment.rs"}),
                    ),
                ],
                "model-a",
            ),
        ])
        .await
        .unwrap();
    session
        .append_messages(&[NewSessionMessage::with_model(
            SessionMessageRole::User,
            vec![SessionContentBlock::tool_result(
                "toolu_1",
                "payment retry code",
            )],
            "model-b",
        )])
        .await
        .unwrap();

    let messages = session.read_messages().await.unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].index, 0);
    assert_eq!(messages[1].index, 1);
    assert_eq!(messages[2].index, 2);
    assert_eq!(messages[0].role, SessionMessageRole::User);
    assert_eq!(messages[1].role, SessionMessageRole::Assistant);
    assert_eq!(messages[2].role, SessionMessageRole::User);
    assert_eq!(messages[0].model, "model-a");
    assert_eq!(messages[1].model, "model-a");
    assert_eq!(messages[2].model, "model-b");
    assert_eq!(
        messages[1].content[1],
        SessionContentBlock::tool_use(
            "toolu_1",
            "file_read",
            serde_json::json!({"path": "src/payment.rs"})
        )
    );
    assert_eq!(
        messages[2].content[0],
        SessionContentBlock::tool_result("toolu_1", "payment retry code")
    );

    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.message_count, 3);
    assert_eq!(metadata.model, "model-b");
    assert!(metadata.updated_at >= metadata.created_at);

    let raw_jsonl = tokio::fs::read_to_string(&session.paths.messages_jsonl)
        .await
        .unwrap();
    assert_eq!(raw_jsonl.lines().count(), 3);
    assert!(raw_jsonl.lines().all(|line| line.contains(r#""model":"#)));
}

#[tokio::test]
async fn t_session_closed_rejects_later_message_append() {
    let tmp = tempfile::tempdir().unwrap();
    let agents_root = tmp.path().join("agents");
    let agent = AgentId::new("agent-a").unwrap();
    let session_id: SessionId = "session_66666666".parse().unwrap();
    let store = SessionStore::new(agents_root);
    let mut session = store
        .create_with_id_factory(&agent, "prompt", || session_id.clone(), 1)
        .await
        .unwrap();

    session.mark_closed(Utc::now()).await.unwrap();
    let err = session
        .append_messages(&[NewSessionMessage::text(
            SessionMessageRole::User,
            "关闭后不应写入",
        )])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("已关闭"));
    assert!(session.read_messages().await.unwrap().is_empty());
    let metadata = session.read_metadata().await.unwrap();
    assert_eq!(metadata.message_count, 0);
    assert_eq!(metadata.status, SessionStatus::Closed);
}
