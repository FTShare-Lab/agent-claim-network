//! 文件读写、diff、read state 与路径语义测试。
//!
//! 覆盖读后写授权、并发写入、symlink、附件与路径归一化边界。

use super::*;

#[tokio::test]
async fn file_read_reads_relative_file_with_line_numbers() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "alpha\nbeta\n")
        .await
        .unwrap();

    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "file_read",
            serde_json::json!({
                "path": "note.txt",
                "start": 2,
                "count": 1
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
    assert_eq!(result.output["path"], "note.txt");
    assert_eq!(result.output["content"], "2|beta\n");
    assert_eq!(result.output["truncated"], false);
}

#[tokio::test]
async fn file_read_default_count_does_not_cap_explicit_larger_reads() {
    let dir = tempfile::tempdir().unwrap();
    let content = (1..=2_501)
        .map(|line| format!("line-{line}\n"))
        .collect::<String>();
    tokio::fs::write(dir.path().join("many.txt"), content)
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    let default_page = registry
        .dispatch(
            "file_read",
            json!({"path": "many.txt", "show_linenos": false}),
        )
        .await
        .unwrap();
    assert_eq!(default_page.output["page"]["returned_end"], 2_000);
    assert_eq!(default_page.output["page"]["next_start"], 2_001);
    assert_eq!(default_page.output["page"]["stop_reason"], "count");

    let explicit_page = registry
        .dispatch(
            "file_read",
            json!({"path": "many.txt", "count": 2_501, "show_linenos": false}),
        )
        .await
        .unwrap();
    assert_eq!(explicit_page.output["page"]["returned_end"], 2_501);
    assert_eq!(explicit_page.output["page"]["reaches_eof"], true);
    assert_eq!(explicit_page.output["page"]["stop_reason"], "eof");
}

#[tokio::test]
async fn file_read_supports_keyword_window_without_line_numbers() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "alpha\nbeta\ncharlie\n")
        .await
        .unwrap();

    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "file_read",
            serde_json::json!({
                "path": "note.txt",
                "keyword": "BETA",
                "count": 2,
                "show_linenos": false
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.output["path"], "note.txt");
    assert_eq!(result.output["content"], "beta\ncharlie\n");
    assert_eq!(result.output["truncated"], false);
}

#[tokio::test]
async fn file_read_treats_blank_keyword_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "one\ntwo\nthree\nfour\nfive\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    for keyword in ["", " \t "] {
        let result = registry
            .dispatch(
                "file_read",
                serde_json::json!({
                    "path": "note.txt",
                    "start": 3,
                    "count": 2,
                    "keyword": keyword,
                    "show_linenos": false
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.output["content"], "three\nfour\n");
        assert_eq!(result.output["page"]["returned_start"], 3);
        assert_eq!(result.output["page"]["returned_end"], 4);
        assert_eq!(result.output["page"]["keyword_match_line"], Value::Null);
    }
}

#[tokio::test]
async fn file_read_accepts_absolute_path() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(outside.path(), "secret\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "file_read",
            serde_json::json!({ "path": outside.path().display().to_string() }),
        )
        .await
        .unwrap();
    assert_eq!(result.output["content"], "1|secret\n");
}

#[tokio::test]
async fn tilde_home_paths_execute_all_file_tools_and_code_run_cwd() {
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        // `HOME` 缺失时的解析行为由 path_util 单测覆盖；此处无法构造真实 ~/ 路径。
        return;
    };
    let home_dir = tempfile::Builder::new()
        .prefix("acn-tilde-tool-test-")
        .tempdir_in(&home)
        .unwrap();
    let relative_dir = home_dir
        .path()
        .strip_prefix(&home)
        .expect("tempdir_in must create the fixture below HOME");
    let relative_dir = relative_dir.to_string_lossy();
    let tilde_dir = format!("~/{relative_dir}");
    let doubled_tilde_dir = format!("~//{relative_dir}");
    let tilde_path = format!("{tilde_dir}/note.txt");
    let workspace = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(workspace.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    tokio::fs::write(home_dir.path().join("note.txt"), "before\n")
        .await
        .unwrap();

    let read = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": tilde_path, "count": 10_000, "show_linenos": false}),
    )
    .await;
    assert_eq!(read["content"], "before\n");
    assert_eq!(read["truncated"], false);

    let write = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": tilde_path, "content": "written\n"}),
    )
    .await;
    assert_eq!(write["status"], "success");

    let patch = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({
            "path": tilde_path,
            "old_content": "written",
            "new_content": "patched",
        }),
    )
    .await;
    assert_eq!(patch["status"], "success");
    assert_eq!(
        tokio::fs::read_to_string(home_dir.path().join("note.txt"))
            .await
            .unwrap(),
        "patched\n"
    );

    let command = registry
        .dispatch(
            "code_run",
            json!({"script": "pwd; cat note.txt", "cwd": doubled_tilde_dir}),
        )
        .await
        .unwrap();
    assert_eq!(command.output["success"], true);
    let stdout = command.output["stdout"].as_str().unwrap();
    let reported_cwd = PathBuf::from(stdout.lines().next().unwrap());
    assert_eq!(
        tokio::fs::canonicalize(reported_cwd).await.unwrap(),
        tokio::fs::canonicalize(home_dir.path()).await.unwrap()
    );
    assert!(stdout.ends_with("patched\n"));
}

#[tokio::test]
async fn other_user_tilde_path_remains_workspace_relative() {
    let workspace = tempfile::tempdir().unwrap();
    let nested = workspace.path().join("~other");
    tokio::fs::create_dir_all(&nested).await.unwrap();
    tokio::fs::write(nested.join("note.txt"), "workspace-owned\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(workspace.path())).unwrap();

    let result = registry
        .dispatch(
            "file_read",
            json!({"path": "~other/note.txt", "show_linenos": false}),
        )
        .await
        .unwrap();

    assert_eq!(result.output["content"], "workspace-owned\n");
    assert_eq!(result.output["truncated"], false);
}

#[tokio::test]
async fn shell_syntax_in_file_paths_is_not_expanded() {
    let workspace = tempfile::tempdir().unwrap();
    let literal_home = workspace.path().join("$HOME");
    tokio::fs::create_dir_all(&literal_home).await.unwrap();
    tokio::fs::write(literal_home.join("note.txt"), "literal-home\n")
        .await
        .unwrap();
    tokio::fs::write(workspace.path().join("*.txt"), "literal-glob\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(workspace.path())).unwrap();

    for (raw_path, expected) in [
        ("$HOME/note.txt", "literal-home"),
        ("*.txt", "literal-glob"),
    ] {
        let result = registry
            .dispatch(
                "file_read",
                json!({"path": raw_path, "show_linenos": false}),
            )
            .await
            .unwrap();
        assert_eq!(
            result.output["content"],
            format!("{expected}\n"),
            "{raw_path} must stay literal"
        );
        assert_eq!(result.output["truncated"], false);
    }
}

#[test]
fn tool_paths_expand_current_user_home_before_workspace_resolution() {
    let workspace = Path::new("/tmp/acn-workspace");
    let home = Path::new("/tmp/acn-home");

    assert_eq!(
        resolve_tool_path_with_home(workspace, "~/notes/todo.md", Some(home)),
        home.join("notes/todo.md")
    );
    assert_eq!(
        resolve_tool_path_with_home(workspace, "~//notes/todo.md", Some(home)),
        home.join("notes/todo.md")
    );
    assert_eq!(
        resolve_tool_path_with_home(workspace, "relative/todo.md", Some(home)),
        workspace.join("relative/todo.md")
    );
    assert_eq!(
        resolve_tool_path_with_home(workspace, "/tmp/absolute.md", Some(home)),
        PathBuf::from("/tmp/absolute.md")
    );
    assert_eq!(
        resolve_tool_path_with_home(workspace, "~other/todo.md", Some(home)),
        workspace.join("~other/todo.md")
    );
}

#[tokio::test]
async fn file_patch_replaces_unique_block() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "alpha\nbeta\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let mut output = registry
        .dispatch_with_context(
            "file_patch",
            serde_json::json!({
                "path": "note.txt",
                "old_content": "beta",
                "new_content": "gamma"
            }),
            file_tool_context(&session),
        )
        .await
        .unwrap();

    let updated = tokio::fs::read_to_string(dir.path().join("note.txt"))
        .await
        .unwrap();
    assert_eq!(updated, "alpha\ngamma\n");
    let change = crate::tool::diff::take_file_change(&mut output.output).expect("应携带 diff");
    assert_eq!(change.added_lines, 1);
    assert_eq!(change.removed_lines, 1);
}

#[tokio::test]
async fn file_patch_uniqueness_uses_non_overlapping_exact_matches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "aaa").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let output = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({
            "path": "note.txt",
            "old_content": "aa",
            "new_content": "X",
        }),
    )
    .await;

    assert_eq!(output["status"], "success");
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "Xa");
}

#[tokio::test]
async fn file_patch_zero_and_multiple_matches_are_typed_business_failures() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "alpha\nalpha\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    for (old_content, expected_message) in [
        ("missing", "未找到匹配"),
        ("alpha", "old_content 至少匹配两处"),
    ] {
        let result = registry
            .dispatch_with_context(
                "file_patch",
                serde_json::json!({
                    "path": "note.txt",
                    "old_content": old_content,
                    "new_content": "beta",
                }),
                file_tool_context(&session),
            )
            .await
            .unwrap();
        assert_eq!(result.outcome, ToolExecutionOutcome::BusinessFailure);
        assert!(result.output["msg"]
            .as_str()
            .is_some_and(|message| message.contains(expected_message)));
    }
    assert_eq!(
        tokio::fs::read_to_string(path).await.unwrap(),
        "alpha\nalpha\n"
    );
}

#[tokio::test]
async fn file_write_supports_prepend() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "beta\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;
    registry
        .dispatch_with_context(
            "file_write",
            serde_json::json!({
                "path": "note.txt",
                "content": "alpha\n",
                "mode": "prepend"
            }),
            file_tool_context(&session),
        )
        .await
        .unwrap();
    let updated = tokio::fs::read_to_string(dir.path().join("note.txt"))
        .await
        .unwrap();
    assert_eq!(updated, "alpha\nbeta\n");
}

#[tokio::test]
async fn cancelled_file_write_does_not_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "before\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = registry
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "after\n"}),
            ToolDispatchContext {
                current_session_id: Some(session),
                cancellation: Some(cancellation),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ToolError::Interrupted));
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "before\n");
}

#[tokio::test]
async fn cancelled_file_patch_does_not_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "before\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = registry
        .dispatch_with_context(
            "file_patch",
            json!({
                "path": "note.txt",
                "old_content": "before",
                "new_content": "after",
            }),
            ToolDispatchContext {
                current_session_id: Some(session),
                cancellation: Some(cancellation),
                ..ToolDispatchContext::default()
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ToolError::Interrupted));
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "before\n");
}

#[tokio::test]
async fn read_state_is_session_scoped_and_same_session_stays_authorized() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "original\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session_a = SessionId::from_str("session_aaaaaaaa").unwrap();
    let session_b = SessionId::from_str("session_bbbbbbbb").unwrap();

    let read = full_file_read(&registry, &session_a, "note.txt").await;
    assert_eq!(read["truncated"], false);

    let cross_session = dispatch_file_tool(
        &registry,
        &session_b,
        "file_write",
        json!({ "path": "note.txt", "content": "from-b\n" }),
    )
    .await;
    let same_session = dispatch_file_tool(
        &registry,
        &session_a,
        "file_write",
        json!({ "path": "note.txt", "content": "from-a\n" }),
    )
    .await;

    assert_eq!(cross_session["status"], "error");
    assert_eq!(same_session["status"], "success");
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "from-a\n");
}

#[tokio::test]
async fn checkpoint_rollback_keeps_file_write_but_restores_stale_read_revision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "alpha\nbeta\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);

    registry
        .dispatch_with_context(
            "file_read",
            json!({
                "path": "note.txt",
                "start": 1,
                "count": 1,
                "show_linenos": false,
            }),
            context.clone(),
        )
        .await
        .unwrap();
    registry
        .begin_file_read_state_checkpoint(&session, "turn_1")
        .await
        .unwrap();
    registry
        .dispatch_with_context(
            "file_read",
            json!({
                "path": "note.txt",
                "start": 2,
                "count": 1,
                "show_linenos": false,
            }),
            context.clone(),
        )
        .await
        .unwrap();
    let patch = registry
        .dispatch_with_context(
            "file_patch",
            json!({
                "path": "note.txt",
                "old_content": "beta",
                "new_content": "BETA",
            }),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(patch.output["status"], "success");

    registry
        .rollback_file_read_state_checkpoint(&session, "turn_1")
        .await
        .unwrap();

    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "alpha\nBETA\n"
    );
    let stale = registry
        .dispatch_with_context(
            "file_patch",
            json!({
                "path": "note.txt",
                "old_content": "alpha",
                "new_content": "ALPHA",
            }),
            context,
        )
        .await
        .unwrap();
    assert_eq!(stale.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(stale.output["stale"], true);
    assert_eq!(
        tokio::fs::read_to_string(path).await.unwrap(),
        "alpha\nBETA\n"
    );
}

#[tokio::test]
async fn delegation_children_do_not_share_file_read_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "original\n").await.unwrap();
    let template = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let child_a = template.clone().for_delegation(None);
    let child_b = template.for_delegation(None);
    let session_id = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = |delegation_id: &str| ToolDispatchContext {
        current_session_id: Some(session_id.clone()),
        current_turn_id: Some(delegation_id.into()),
        ..ToolDispatchContext::default()
    };

    let read = child_a
        .dispatch_with_context(
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
            context("subagent_aaaaaaaa"),
        )
        .await
        .unwrap();
    let child_b_write = child_b
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "from-b\n"}),
            context("subagent_bbbbbbbb"),
        )
        .await
        .unwrap();
    let child_a_write = child_a
        .dispatch_with_context(
            "file_write",
            json!({"path": "note.txt", "content": "from-a\n"}),
            context("subagent_aaaaaaaa"),
        )
        .await
        .unwrap();

    assert_eq!(read.output["truncated"], false);
    assert_eq!(child_b_write.output["status"], "error");
    assert_eq!(child_a_write.output["status"], "success");
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "from-a\n");
}

#[tokio::test]
async fn partial_keyword_and_truncated_reads_do_not_authorize_write() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let cases = [
        (
            "partial.txt",
            json!({
                "path": "partial.txt",
                "start": 2,
                "count": 2,
                "show_linenos": false,
            }),
        ),
        (
            "keyword.txt",
            json!({
                "path": "keyword.txt",
                "keyword": "beta",
                "count": 1,
                "show_linenos": false,
            }),
        ),
        (
            "truncated.txt",
            json!({
                "path": "truncated.txt",
                "count": 1,
                "show_linenos": false,
            }),
        ),
    ];
    let mut outcomes = Vec::new();

    for (name, read_input) in cases {
        tokio::fs::write(dir.path().join(name), "alpha\nbeta\ngamma\n")
            .await
            .unwrap();
        let _ = dispatch_file_tool(&registry, &session, "file_read", read_input).await;
        let output = dispatch_file_tool(
            &registry,
            &session,
            "file_write",
            json!({ "path": name, "content": "changed\n" }),
        )
        .await;
        let disk = tokio::fs::read_to_string(dir.path().join(name))
            .await
            .unwrap();
        outcomes.push((name, output, disk));
    }

    for (name, output, disk) in outcomes {
        assert_eq!(output["status"], "error", "{name} 不应获得写权限");
        assert_eq!(disk, "alpha\nbeta\ngamma\n", "{name} 不应被改写");
    }
}

#[tokio::test]
async fn max_chars_truncation_can_page_and_then_authorize_complete_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.file_read_max_chars = 4;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "abc\ndef\n").await.unwrap();

    let read = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "note.txt", "count": 100, "show_linenos": false}),
    )
    .await;
    let blocked_write = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "changed\n"}),
    )
    .await;
    let local_patch = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "note.txt", "old_content": "abc", "new_content": "ABC"}),
    )
    .await;
    assert_eq!(read["truncated"], true);
    assert_eq!(read["page"]["next_start"], 2);
    assert_eq!(read["page"]["stop_reason"], "max_chars");
    assert_eq!(blocked_write["status"], "error");
    assert!(blocked_write.get("requires_user_config_change").is_none());
    assert_eq!(local_patch["status"], "success");

    let second = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "note.txt", "start": 2, "show_linenos": false}),
    )
    .await;
    assert_eq!(second["content"], "def\n");
    assert_eq!(second["page"]["reaches_eof"], true);
    let write = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "changed\n"}),
    )
    .await;
    assert_eq!(write["status"], "success");
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "changed\n");
}

#[tokio::test]
async fn paged_reads_merge_out_of_order_but_gap_is_not_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.txt");
    let content = (1..=264)
        .map(|line| format!("line-{line:03}\n"))
        .collect::<String>();
    tokio::fs::write(&path, content).await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    let first = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "large.txt", "start": 1, "count": 130, "show_linenos": false}),
    )
    .await;
    let tail = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "large.txt", "start": 201, "count": 64, "show_linenos": false}),
    )
    .await;
    let blocked = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "large.txt", "content": "blocked\n"}),
    )
    .await;

    assert_eq!(first["page"]["returned_end"], 130);
    assert_eq!(first["page"]["next_start"], 131);
    assert_eq!(tail["page"]["returned_start"], 201);
    assert_eq!(tail["page"]["reaches_eof"], true);
    assert_eq!(blocked["status"], "error");
    assert_eq!(blocked["required_read"]["kind"], "complete");

    let middle = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "large.txt", "start": 131, "count": 70, "show_linenos": false}),
    )
    .await;
    assert_eq!(middle["page"]["returned_end"], 200);
    let allowed = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "large.txt", "content": "complete\n"}),
    )
    .await;
    assert_eq!(allowed["status"], "success");
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "complete\n");
}

#[tokio::test]
async fn unique_patch_needs_only_target_line_and_preserves_partial_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.txt");
    tokio::fs::write(&path, "unread-before\nTARGET\nunread-after\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let read = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "partial.txt", "start": 2, "count": 1, "show_linenos": false}),
    )
    .await;
    assert_eq!(read["content"], "TARGET\n");

    let patch = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "partial.txt", "old_content": "TARGET", "new_content": "DONE"}),
    )
    .await;
    assert_eq!(patch["status"], "success");
    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "unread-before\nDONE\nunread-after\n"
    );

    let overwrite = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "partial.txt", "content": "must-not-overwrite\n"}),
    )
    .await;
    assert_eq!(overwrite["status"], "error");
}

#[tokio::test]
async fn eof_page_authorizes_append_but_not_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tail.txt");
    tokio::fs::write(&path, "unread\nlast-line").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let tail = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "tail.txt", "start": 2, "count": 1, "show_linenos": false}),
    )
    .await;
    assert_eq!(tail["page"]["reaches_eof"], true);
    assert_eq!(tail["page"]["ends_with_newline"], false);

    let append = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "tail.txt", "mode": "append", "content": "\n成功审核\n"}),
    )
    .await;
    assert_eq!(append["status"], "success");
    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "unread\nlast-line\n成功审核\n"
    );

    let overwrite = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "tail.txt", "content": "blocked\n"}),
    )
    .await;
    assert_eq!(overwrite["status"], "error");
}

#[tokio::test]
async fn patch_that_joins_an_unread_neighbor_requires_more_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("boundary.txt");
    tokio::fs::write(&path, "first\nsecond\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "boundary.txt", "start": 1, "count": 1, "show_linenos": false}),
    )
    .await;

    let rejected = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "boundary.txt", "old_content": "first\n", "new_content": "first"}),
    )
    .await;
    assert_eq!(rejected["status"], "error");
    assert_eq!(rejected["required_read"]["kind"], "range");
    assert_eq!(rejected["required_read"]["start"], 1);
    assert_eq!(rejected["required_read"]["count"], 2);
    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "first\nsecond\n"
    );

    let _ = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "boundary.txt", "start": 2, "count": 1, "show_linenos": false}),
    )
    .await;
    let allowed = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "boundary.txt", "old_content": "first\n", "new_content": "first"}),
    )
    .await;
    assert_eq!(allowed["status"], "success");
    assert_eq!(
        tokio::fs::read_to_string(path).await.unwrap(),
        "firstsecond\n"
    );
}

#[tokio::test]
async fn replace_all_requires_complete_coverage_even_for_one_match() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("replace.txt"), "target\nunread\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "replace.txt", "count": 1, "show_linenos": false}),
    )
    .await;
    let result = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({
            "path": "replace.txt",
            "old_content": "target",
            "new_content": "done",
            "replace_all": true
        }),
    )
    .await;
    assert_eq!(result["status"], "error");
    assert_eq!(result["required_read"]["kind"], "complete");
}

#[tokio::test]
async fn pdf_attachment_read_does_not_authorize_text_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("doc.pdf"), b"%PDF-1.7 fake")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    let read =
        dispatch_file_tool(&registry, &session, "file_read", json!({"path": "doc.pdf"})).await;
    let write = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "doc.pdf", "content": "replacement"}),
    )
    .await;

    assert_eq!(read["kind"], "pdf");
    assert_eq!(write["status"], "error");
    assert_eq!(
        tokio::fs::read(dir.path().join("doc.pdf")).await.unwrap(),
        b"%PDF-1.7 fake"
    );
}

#[tokio::test]
async fn existing_file_write_modes_require_full_read() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let mut outcomes = Vec::new();

    for mode in ["overwrite", "append", "prepend"] {
        let name = format!("{mode}.txt");
        tokio::fs::write(dir.path().join(&name), "base\n")
            .await
            .unwrap();
        let output = dispatch_file_tool(
            &registry,
            &session,
            "file_write",
            json!({ "path": name, "content": "next\n", "mode": mode }),
        )
        .await;
        outcomes.push((mode, output));
    }

    for (mode, output) in outcomes {
        assert_eq!(
            output["status"], "error",
            "已有文件的 {mode} 必须先取得对应读取许可"
        );
        assert!(output.get("stale").is_none());
    }
}

#[tokio::test]
async fn existing_file_write_modes_emit_modified_diff_after_full_read() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let mut outputs = Vec::new();

    for mode in ["overwrite", "append", "prepend"] {
        let name = format!("{mode}.txt");
        tokio::fs::write(dir.path().join(&name), "base\n")
            .await
            .unwrap();
        let read = full_file_read(&registry, &session, &name).await;
        assert_eq!(read["truncated"], false);
        let output = dispatch_file_tool(
            &registry,
            &session,
            "file_write",
            json!({ "path": name, "content": "next\n", "mode": mode }),
        )
        .await;
        outputs.push((mode, name, output));
    }

    for (mode, name, mut output) in outputs {
        assert_eq!(output["status"], "success", "{mode} 应写入成功");
        let change = crate::tool::diff::take_file_change(&mut output)
            .unwrap_or_else(|| panic!("{mode} 应携带 FileChange"));
        assert_eq!(change.path, name);
        assert_eq!(change.kind, crate::tool::diff::FileChangeKind::Modified);
        let expected_removed = usize::from(mode == "overwrite");
        assert_eq!(change.added_lines, 1, "{mode} 的新增行统计应准确");
        assert_eq!(
            change.removed_lines, expected_removed,
            "{mode} 的删除行统计应准确"
        );
        assert_eq!(change.truncated_changed_lines, 0);
    }
}

#[tokio::test]
async fn unchanged_file_write_does_not_emit_diff() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "same\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let mut output = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({ "path": "note.txt", "content": "same\n" }),
    )
    .await;

    assert_ne!(output["status"], "error");
    assert!(crate::tool::diff::take_file_change(&mut output).is_none());
}

#[tokio::test]
async fn file_diff_changed_line_limit_propagates_from_config() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "a\nb\nc\nd\n")
        .await
        .unwrap();
    let mut config = test_tool_config(dir.path());
    config.file_diff_max_changed_lines = 2;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let mut output = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "A\nB\nC\nD\n"}),
    )
    .await;
    let change = crate::tool::diff::take_file_change(&mut output).expect("应携带 diff");

    assert_eq!(change.added_lines + change.removed_lines, 8);
    assert_eq!(change.truncated_changed_lines, 6);
}

#[tokio::test]
async fn utf8_file_read_limit_counts_chars_and_authorizes_write() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "你好")
        .await
        .unwrap();
    let mut config = test_tool_config(dir.path());
    config.file_read_max_chars = 2;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    let read = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({ "path": "note.txt", "show_linenos": false }),
    )
    .await;
    let write = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({ "path": "note.txt", "content": "再见" }),
    )
    .await;

    assert_eq!(read["content"], "你好");
    assert_eq!(read["truncated"], false);
    assert_eq!(write["status"], "success");
}

#[tokio::test]
async fn file_patch_many_matches_returns_bounded_ambiguity_error() {
    let dir = tempfile::tempdir().unwrap();
    let line_count = 512usize;
    tokio::fs::write(dir.path().join("many.txt"), "needle\n".repeat(line_count))
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "many.txt").await;

    let output = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({
            "path": "many.txt",
            "old_content": "needle",
            "new_content": "replacement",
        }),
    )
    .await;

    assert_eq!(output["status"], "error");
    let msg = output["msg"].as_str().unwrap();
    assert_eq!(
        msg,
        "old_content 至少匹配两处，必须全局唯一。请扩大文本块并加入目标附近上下文，或在确认全部替换时使用 replace_all=true。"
    );
    assert!(!msg.contains(&line_count.to_string()));
}

#[tokio::test]
async fn file_patch_replace_all_updates_every_match_and_diff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "needle\nkeep\nneedle\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let mut output = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({
            "path": "note.txt",
            "old_content": "needle",
            "new_content": "done",
            "replace_all": true,
        }),
    )
    .await;

    assert_eq!(output["status"], "success");
    assert_eq!(output["replacements"], 2);
    assert_eq!(
        tokio::fs::read_to_string(path).await.unwrap(),
        "done\nkeep\ndone\n"
    );
    let change = crate::tool::diff::take_file_change(&mut output).expect("应携带 diff");
    assert_eq!(change.added_lines, 2);
    assert_eq!(change.removed_lines, 2);
}

#[tokio::test]
async fn file_patch_rejects_missing_stale_and_noop_targets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "old\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    let missing = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "missing.txt", "old_content": "a", "new_content": "b"}),
    )
    .await;
    let unread = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "note.txt", "old_content": "old", "new_content": "new"}),
    )
    .await;
    let noop = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "note.txt", "old_content": "old", "new_content": "old"}),
    )
    .await;
    let _ = full_file_read(&registry, &session, "note.txt").await;
    let zero_match = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "note.txt", "old_content": "absent", "new_content": "new"}),
    )
    .await;
    tokio::fs::write(&path, "old\nexternal\n").await.unwrap();
    let stale = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "note.txt", "old_content": "old", "new_content": "new"}),
    )
    .await;

    assert_eq!(missing["status"], "error");
    assert_eq!(unread["status"], "error");
    assert_eq!(noop["status"], "error");
    assert_eq!(zero_match["status"], "error");
    assert!(zero_match["msg"]
        .as_str()
        .is_some_and(|msg| msg.contains("重新 file_read")));
    assert_eq!(stale["status"], "error");
    assert_eq!(stale["stale"], true);
    assert_eq!(
        tokio::fs::read_to_string(path).await.unwrap(),
        "old\nexternal\n"
    );
}

#[tokio::test]
async fn new_file_write_needs_no_read_and_emits_created_diff() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    let mut output = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "new.txt", "content": "first\nsecond\n"}),
    )
    .await;

    assert_eq!(output["status"], "success");
    let change = crate::tool::diff::take_file_change(&mut output).expect("新建文件应携带 diff");
    assert_eq!(change.kind, FileChangeKind::Created);
    assert_eq!(change.added_lines, 2);
    assert_eq!(change.removed_lines, 0);
}

#[tokio::test]
async fn new_file_append_and_prepend_are_created_changes() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();

    for mode in ["append", "prepend"] {
        let path = format!("{mode}.txt");
        let mut output = dispatch_file_tool(
            &registry,
            &session,
            "file_write",
            json!({"path": path, "content": "created\n", "mode": mode}),
        )
        .await;
        let change = crate::tool::diff::take_file_change(&mut output).expect("新建文件应携带 diff");

        assert_eq!(output["status"], "success");
        assert_eq!(change.kind, FileChangeKind::Created);
        assert_eq!(change.added_lines, 1);
    }
}

#[tokio::test]
async fn external_change_after_read_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "original\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;
    tokio::fs::write(&path, "external\n").await.unwrap();

    let output = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "tool\n"}),
    )
    .await;

    assert_eq!(output["status"], "error");
    assert_eq!(output["stale"], true);
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "external\n");
}

#[tokio::test]
async fn missing_session_context_cannot_establish_file_write_authorization() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "original\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    let read = registry
        .dispatch(
            "file_read",
            json!({"path": "note.txt", "show_linenos": false}),
        )
        .await
        .unwrap();
    let write = registry
        .clone()
        .dispatch(
            "file_write",
            json!({"path": "note.txt", "content": "changed\n"}),
        )
        .await
        .unwrap();

    assert_eq!(read.output["content"], "original\n");
    assert_eq!(write.output["status"], "error");
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "original\n");
}

#[tokio::test]
async fn successful_write_refreshes_read_state_until_resume_clear() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "v1\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let first = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "v2\n"}),
    )
    .await;
    let second = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "v3\n"}),
    )
    .await;
    registry.clear_file_read_state(&session).await;
    let after_resume = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "v4\n"}),
    )
    .await;

    assert_eq!(first["status"], "success");
    assert_eq!(second["status"], "success");
    assert_eq!(after_resume["status"], "error");
}

#[tokio::test]
async fn concurrent_appends_do_not_lose_updates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "base\n").await.unwrap();
    let registry = Arc::new(ToolRegistry::new(&test_tool_config(dir.path())).unwrap());
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    let append = |content: &'static str| {
        let registry = Arc::clone(&registry);
        let session = session.clone();
        tokio::spawn(async move {
            dispatch_file_tool(
                &registry,
                &session,
                "file_write",
                json!({"path": "note.txt", "content": content, "mode": "append"}),
            )
            .await
        })
    };
    let (first, second) = tokio::join!(append("one\n"), append("two\n"));

    assert_eq!(first.unwrap()["status"], "success");
    assert_eq!(second.unwrap()["status"], "success");
    let content = tokio::fs::read_to_string(path).await.unwrap();
    assert!(content.starts_with("base\n"));
    assert!(content.contains("one\n"));
    assert!(content.contains("two\n"));
}

#[tokio::test]
async fn independent_registries_share_file_write_lock_and_reject_stale_second_write() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let base_acn_home = dir.path().join("acn");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let path = workspace.join("note.txt");
    tokio::fs::write(&path, "base\n").await.unwrap();
    let lock_root = paths::base_acn_home_file_write_locks_dir(&base_acn_home);
    let first_registry = Arc::new(
        ToolRegistry::new(&test_tool_config(&workspace))
            .unwrap()
            .with_file_write_lock_root(lock_root.clone()),
    );
    let second_registry = Arc::new(
        ToolRegistry::new(&test_tool_config(&workspace))
            .unwrap()
            .with_file_write_lock_root(lock_root.clone()),
    );
    let first_session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let second_session = SessionId::from_str("session_bbbbbbbb").unwrap();
    let _ = full_file_read(&first_registry, &first_session, "note.txt").await;
    let _ = full_file_read(&second_registry, &second_session, "note.txt").await;

    let stable_key = tokio::fs::canonicalize(&path).await.unwrap();
    let lock_path = paths::file_write_lock_path(&lock_root, &stable_key);
    let blocker = FileLockGuard::lock_exclusive(&lock_path).await.unwrap();
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let spawn_write = |registry: Arc<ToolRegistry>, session: SessionId, content: &'static str| {
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            dispatch_file_tool(
                &registry,
                &session,
                "file_write",
                json!({"path": "note.txt", "content": content}),
            )
            .await
        })
    };
    let first = spawn_write(first_registry, first_session, "first\n");
    let second = spawn_write(second_registry, second_session, "second\n");
    start.wait().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    drop(blocker);

    let first = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap()
        .unwrap();
    let successes = [&first, &second]
        .into_iter()
        .filter(|output| output["status"] == "success")
        .count();
    assert_eq!(successes, 1);
    assert!(first["status"] == "error" || second["status"] == "error");
    let content = tokio::fs::read_to_string(path).await.unwrap();
    assert!(content == "first\n" || content == "second\n");
    assert!(lock_path.is_file(), "lock 文件不得在释放时删除");
}

#[tokio::test]
async fn independent_registries_share_file_write_lock_for_concurrent_create() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let lock_root = paths::base_acn_home_file_write_locks_dir(&dir.path().join("acn"));
    let first_registry = Arc::new(
        ToolRegistry::new(&test_tool_config(&workspace))
            .unwrap()
            .with_file_write_lock_root(lock_root.clone()),
    );
    let second_registry = Arc::new(
        ToolRegistry::new(&test_tool_config(&workspace))
            .unwrap()
            .with_file_write_lock_root(lock_root.clone()),
    );
    let path = workspace.join("created.txt");
    let stable_key = tokio::fs::canonicalize(&workspace)
        .await
        .unwrap()
        .join("created.txt");
    let lock_path = paths::file_write_lock_path(&lock_root, &stable_key);
    let blocker = FileLockGuard::lock_exclusive(&lock_path).await.unwrap();
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let spawn_create =
        |registry: Arc<ToolRegistry>, session: &'static str, content: &'static str| {
            let start = Arc::clone(&start);
            tokio::spawn(async move {
                start.wait().await;
                dispatch_file_tool(
                    &registry,
                    &SessionId::from_str(session).unwrap(),
                    "file_write",
                    json!({"path": "created.txt", "content": content}),
                )
                .await
            })
        };
    let first = spawn_create(first_registry, "session_aaaaaaaa", "first\n");
    let second = spawn_create(second_registry, "session_bbbbbbbb", "second\n");
    start.wait().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    drop(blocker);

    let first = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        [&first, &second]
            .into_iter()
            .filter(|output| output["status"] == "success")
            .count(),
        1
    );
    assert!(first["status"] == "error" || second["status"] == "error");
    let content = tokio::fs::read_to_string(path).await.unwrap();
    assert!(content == "first\n" || content == "second\n");
}

#[tokio::test]
async fn waiting_for_file_write_lock_respects_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let base_acn_home = dir.path().join("acn");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    let path = workspace.join("note.txt");
    tokio::fs::write(&path, "base\n").await.unwrap();
    let lock_root = paths::base_acn_home_file_write_locks_dir(&base_acn_home);
    let registry = Arc::new(
        ToolRegistry::new(&test_tool_config(&workspace))
            .unwrap()
            .with_file_write_lock_root(lock_root.clone()),
    );
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;
    let stable_key = tokio::fs::canonicalize(&path).await.unwrap();
    let lock_path = paths::file_write_lock_path(&lock_root, &stable_key);
    let blocker = FileLockGuard::lock_exclusive(&lock_path).await.unwrap();
    let cancellation = CancellationToken::new();
    let task = {
        let registry = Arc::clone(&registry);
        let cancellation = cancellation.clone();
        tokio::spawn(async move {
            registry
                .dispatch_with_context(
                    "file_write",
                    json!({"path": "note.txt", "content": "changed\n"}),
                    ToolDispatchContext {
                        current_session_id: Some(session),
                        cancellation: Some(cancellation),
                        ..ToolDispatchContext::default()
                    },
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    cancellation.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(ToolError::Interrupted)));
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "base\n");
    drop(blocker);
}

#[tokio::test]
async fn path_lock_registry_prunes_unused_paths() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    let first = registry
        .path_lock(&dir.path().join("first.txt"))
        .await
        .unwrap();
    assert_eq!(registry.path_locks.lock().unwrap().len(), 1);
    drop(first);
    let _second = registry
        .path_lock(&dir.path().join("second.txt"))
        .await
        .unwrap();

    assert_eq!(registry.path_locks.lock().unwrap().len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn file_write_preserves_leaf_symlink_and_updates_target() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target.txt");
    let link = dir.path().join("link.txt");
    tokio::fs::write(&target, "old\n").await.unwrap();
    tokio::fs::symlink(&target, &link).await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "link.txt").await;

    let output = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "link.txt", "content": "new\n"}),
    )
    .await;

    assert_eq!(output["status"], "success");
    assert!(tokio::fs::symlink_metadata(link)
        .await
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(tokio::fs::read_to_string(target).await.unwrap(), "new\n");
}

#[cfg(unix)]
#[tokio::test]
async fn path_lock_follows_symlink_before_parent_segment() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let target_parent = dir.path().join("target");
    let linked_dir = target_parent.join("linked");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::create_dir_all(&linked_dir).await.unwrap();
    tokio::fs::symlink(&linked_dir, workspace.join("alias"))
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(&workspace)).unwrap();

    let through_symlink = registry
        .path_lock(&workspace.join("alias/../shared.txt"))
        .await
        .unwrap();
    let canonical_target = registry
        .path_lock(&target_parent.join("shared.txt"))
        .await
        .unwrap();

    assert!(Arc::ptr_eq(&through_symlink, &canonical_target));
}

#[cfg(unix)]
#[tokio::test]
async fn file_write_rejects_new_memory_file_through_symlinked_parent() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let memories = dir.path().join("agent-a/memories");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::create_dir_all(&memories).await.unwrap();
    tokio::fs::symlink(&memories, workspace.join("private_alias"))
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(&workspace)).unwrap();

    let error = registry
        .dispatch(
            "file_write",
            json!({"path": "private_alias/MEMORY.md", "content": "bypass"}),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("memory 工具"));
    assert!(!memories.join("MEMORY.md").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn file_write_rejects_new_memory_files_through_symlink_and_parent_segments() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let memories = dir.path().join("agent-a/memories");
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::create_dir_all(&memories).await.unwrap();
    tokio::fs::symlink(&memories, workspace.join("private_alias"))
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(&workspace)).unwrap();

    for file_name in ["MEMORY.md", "USER.md"] {
        let error = registry
            .dispatch(
                "file_write",
                json!({
                    "path": format!("private_alias/newdir/../{file_name}"),
                    "content": "bypass",
                }),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("memory 工具"));
        assert!(!memories.join(file_name).exists());
    }
    assert!(!memories.join("newdir").exists());
}

#[tokio::test]
async fn direct_file_tools_reject_memory_files() {
    let dir = tempfile::tempdir().unwrap();
    let memories = dir.path().join("agent-a").join("memories").join("sub");
    tokio::fs::create_dir_all(&memories).await.unwrap();
    tokio::fs::write(dir.path().join("agent-a/memories/MEMORY.md"), "keep me")
        .await
        .unwrap();
    tokio::fs::write(dir.path().join("agent-a/memories/USER.md"), "keep me")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();

    let write_err = registry
        .dispatch(
            "file_write",
            serde_json::json!({
                "path": "agent-a/memories/sub/../MEMORY.md",
                "content": "bypass"
            }),
        )
        .await
        .unwrap_err();
    assert!(write_err.to_string().contains("memory 工具"));

    let patch_err = registry
        .dispatch(
            "file_patch",
            serde_json::json!({
                "path": dir.path().join("agent-a/memories/./USER.md").display().to_string(),
                "old_content": "x",
                "new_content": "y"
            }),
        )
        .await
        .unwrap_err();
    assert!(patch_err.to_string().contains("memory 工具"));
}

#[tokio::test]
async fn file_write_append_prepend_preserves_invalid_utf8_existing_file_on_read_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("binary.dat");
    let original = vec![0xff, 0xfe, b'a'];
    tokio::fs::write(&path, &original).await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path()))
        .unwrap()
        .for_delegation(None);

    for mode in ["append", "prepend"] {
        let err = registry
            .dispatch(
                "file_write",
                json!({
                    "path": "binary.dat",
                    "content": "text",
                    "mode": mode
                }),
            )
            .await
            .expect_err("append/prepend must fail when existing content is not UTF-8");
        assert!(matches!(err, ToolError::Io(_)));
        assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    }
}
