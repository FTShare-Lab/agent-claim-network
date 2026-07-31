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
    assert_eq!(result.output["content"], "2|beta");
    assert_eq!(result.output["truncated"], false);
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
    assert_eq!(result.output["content"], "beta\ncharlie");
    assert_eq!(result.output["truncated"], false);
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
    assert_eq!(result.output["content"], "1|secret");
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
    assert_eq!(read["content"], "before");
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

    assert_eq!(result.output["content"], "workspace-owned");
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
            result.output["content"], expected,
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
async fn file_patch_zero_and_multiple_matches_are_typed_business_failures() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "alpha\nalpha\n").await.unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let _ = full_file_read(&registry, &session, "note.txt").await;

    for (old_content, expected_message) in [("missing", "未找到匹配"), ("alpha", "找到 2 处匹配")]
    {
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
                "count": 3,
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
async fn config_truncated_read_reports_limit_and_requires_user_config_change_before_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.file_read_max_chars = 5;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let path = dir.path().join("note.txt");
    tokio::fs::write(&path, "abcdef\n").await.unwrap();

    let read = dispatch_file_tool(
        &registry,
        &session,
        "file_read",
        json!({"path": "note.txt", "count": 100, "show_linenos": false}),
    )
    .await;
    let write = dispatch_file_tool(
        &registry,
        &session,
        "file_write",
        json!({"path": "note.txt", "content": "changed\n"}),
    )
    .await;
    let patch = dispatch_file_tool(
        &registry,
        &session,
        "file_patch",
        json!({"path": "note.txt", "old_content": "abcdef", "new_content": "changed"}),
    )
    .await;

    assert_eq!(read["truncated"], true);
    for output in [&write, &patch] {
        assert_eq!(output["status"], "error");
        assert_eq!(output["file_read_max_chars"], 5);
        assert_eq!(output["requires_user_config_change"], true);
        let message = output["msg"].as_str().unwrap();
        assert!(message.contains("分页读取只能查看局部内容"));
        assert!(message.contains("[agent.tool].file_read_max_chars=5"));
        assert!(message.contains("重启 ACN 后重新完整 file_read 并重试"));
    }
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "abcdef\n");
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
            "已有文件的 {mode} 必须先完整 file_read"
        );
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
async fn file_patch_many_matches_reports_every_start_line() {
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
    let expected = (1..=line_count)
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("、");
    let msg = output["msg"].as_str().unwrap();
    assert!(
        msg.contains(&format!("第 {expected} 行")),
        "多匹配错误必须包含全部起始行号: {msg}"
    );
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
    tokio::fs::write(&path, "external\n").await.unwrap();
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
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "external\n");
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

    assert_eq!(read.output["content"], "original");
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
