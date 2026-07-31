use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use agent_claim_network::claim::{AgentId, SessionId};
use agent_claim_network::session::{
    NewSessionMessage, SessionContentBlock, SessionMessageRole, SessionStore,
};
use agent_claim_network::session_search::best_effort_index_session_from_files;
use agent_claim_network::storage::paths;
use chrono::Utc;

#[tokio::test]
async fn acn_session_cleanup_dry_run_then_apply_removes_session_and_search_index() {
    let tmp = tempfile::tempdir().unwrap();
    let acn_home = tmp.path().join("acn_home");
    let config_path = tmp.path().join("config.toml");
    write_test_config(&config_path, &acn_home);

    let agent = AgentId::new("agent-a").unwrap();
    let session_id = SessionId::from_str("session_1234abcd").unwrap();
    let base_agent_home = acn_home.join("data").join("agents").join(agent.as_str());
    let upstream_home = acn_home.join("dev");
    let agents_root = upstream_home.join("data").join("agents");
    let agent_home = agents_root.join(agent.as_str());
    let store = SessionStore::new(agents_root);
    let old = Utc::now() - chrono::Duration::days(45);
    let mut session = store
        .create_with_id_factory(&agent, "system", || session_id.clone(), 1)
        .await
        .unwrap();
    session
        .append_messages(&[
            NewSessionMessage::with_created_at(
                SessionMessageRole::User,
                vec![SessionContentBlock::text("旧中文 cleanup needle")],
                old,
            ),
            NewSessionMessage::with_created_at(
                SessionMessageRole::Assistant,
                vec![SessionContentBlock::text("old assistant answer")],
                old,
            ),
        ])
        .await
        .unwrap();
    session.mark_closed(old).await.unwrap();
    best_effort_index_session_from_files(
        agent_home.clone(),
        session_id.clone(),
        Duration::from_millis(500),
        0,
    )
    .await;

    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages", &session_id),
        2
    );
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages_fts", &session_id),
        2
    );
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages_fts_trigram", &session_id),
        2
    );
    assert!(
        !base_agent_home.exists(),
        "fixture must use upstream runtime, not base acn_home"
    );

    let dry_run = run_acn_session_cleanup(&config_path, false);
    assert!(
        dry_run.status.success(),
        "dry-run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&dry_run.stdout),
        String::from_utf8_lossy(&dry_run.stderr)
    );
    let dry_run_stdout = String::from_utf8_lossy(&dry_run.stdout);
    assert!(dry_run_stdout.contains("This is a dry run. Use --apply to delete."));
    assert!(dry_run_stdout.contains("Outcome"));
    assert!(dry_run_stdout.contains("Last Activity At"));
    assert!(dry_run_stdout.contains("Reason"));
    assert!(dry_run_stdout.contains("eligible       1"));
    assert!(session.paths.dir.exists());
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages", &session_id),
        2
    );

    let applied = run_acn_session_cleanup(&config_path, true);
    assert!(
        applied.status.success(),
        "apply failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&applied.stdout),
        String::from_utf8_lossy(&applied.stderr)
    );
    let apply_stdout = String::from_utf8_lossy(&applied.stdout);
    assert!(!apply_stdout.contains("This is a dry run. Use --apply to delete."));
    assert!(apply_stdout.contains("deleted        1"));
    assert!(apply_stdout.contains("sqlite_purged  1"));
    assert!(!session.paths.dir.exists());
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages", &session_id),
        0
    );
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages_fts", &session_id),
        0
    );
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages_fts_trigram", &session_id),
        0
    );
    assert_eq!(
        sqlite_session_row_count(&agent_home, "indexed_sessions", &session_id),
        0
    );
    assert_eq!(
        sqlite_session_row_count(&agent_home, "sessions", &session_id),
        0
    );

    let orphan_id = SessionId::from_str("session_deadbeef").unwrap();
    let mut orphan = store
        .create_with_id_factory(&agent, "system", || orphan_id.clone(), 1)
        .await
        .unwrap();
    orphan
        .append_messages(&[NewSessionMessage::with_created_at(
            SessionMessageRole::User,
            vec![SessionContentBlock::text("orphan cleanup needle")],
            old,
        )])
        .await
        .unwrap();
    orphan.mark_closed(old).await.unwrap();
    best_effort_index_session_from_files(
        agent_home.clone(),
        orphan_id.clone(),
        Duration::from_millis(500),
        0,
    )
    .await;
    tokio::fs::remove_dir_all(&orphan.paths.dir).await.unwrap();

    let orphan_dry_run = run_acn_session_cleanup(&config_path, false);
    assert!(
        orphan_dry_run.status.success(),
        "orphan dry-run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&orphan_dry_run.stdout),
        String::from_utf8_lossy(&orphan_dry_run.stderr)
    );
    let orphan_dry_run_stdout = String::from_utf8_lossy(&orphan_dry_run.stdout);
    assert!(orphan_dry_run_stdout.contains("This is a dry run. Use --apply to delete."));
    assert!(orphan_dry_run_stdout.contains(orphan_id.as_str()));
    assert!(orphan_dry_run_stdout.contains("Orphan sqlite rows eligible for purge"));
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages", &orphan_id),
        1
    );

    let orphan_cleanup = run_acn_session_cleanup(&config_path, true);
    assert!(
        orphan_cleanup.status.success(),
        "orphan cleanup failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&orphan_cleanup.stdout),
        String::from_utf8_lossy(&orphan_cleanup.stderr)
    );
    let orphan_stdout = String::from_utf8_lossy(&orphan_cleanup.stdout);
    assert!(orphan_stdout.contains("eligible       1"));
    assert!(orphan_stdout.contains("sqlite_purged  1"));
    assert!(orphan_stdout.contains("index_purged"));
    assert!(orphan_stdout.contains(orphan_id.as_str()));
    assert_eq!(
        sqlite_session_row_count(&agent_home, "messages", &orphan_id),
        0
    );
}

fn write_test_config(config_path: &std::path::Path, acn_home: &std::path::Path) {
    let acn_home = toml_string(&acn_home.display().to_string());
    let raw = format!(
        r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

[storage]
acn_home = "{acn_home}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "test-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000
"#
    );
    std::fs::write(config_path, raw).unwrap();
}

fn toml_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn run_acn_session_cleanup(config_path: &std::path::Path, apply: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_acn"));
    command
        .arg("session")
        .arg("cleanup")
        .arg("--config")
        .arg(config_path)
        .arg("--upstream")
        .arg("dev");
    if apply {
        command.arg("--apply");
    }
    command.output().unwrap()
}

fn sqlite_session_row_count(
    agent_home: &std::path::Path,
    table: &str,
    session_id: &SessionId,
) -> i64 {
    let db_path = paths::agent_home_session_search_index_path(agent_home);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    let sql = format!("SELECT count(*) FROM {table} WHERE session_id = ?1;");
    conn.query_row(&sql, [session_id.as_str()], |row| row.get(0))
        .unwrap()
}
