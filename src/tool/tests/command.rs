//! code_run、PTY 与后台进程生命周期测试。
//!
//! 覆盖命令执行、进程交接、输出 cursor、终止与退出清理行为。

use super::*;

async fn acknowledge_process_output(registry: &ToolRegistry, execution: &ToolExecution) {
    let Some(receipt) = execution.process_delivery_receipt.clone() else {
        return;
    };
    registry
        .begin_process_deliveries(std::slice::from_ref(&receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&receipt))
        .await;
}

#[tokio::test]
async fn code_run_executes_python_in_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "code_run",
            serde_json::json!({
                "script": "print('hello')",
                "type": "python",
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        result.outcome,
        ToolExecutionOutcome::ProcessExit {
            exit_code: Some(0),
            success: true,
        }
    );
    assert_eq!(result.output["success"], true);
    assert_eq!(result.output["exit_code"], 0);
    assert_eq!(result.output["stdout"], "hello\n");
}

#[tokio::test]
async fn code_run_defaults_to_bash_in_workspace() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("note.txt"), "hello\n")
        .await
        .unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "code_run",
            serde_json::json!({
                "script": "pwd; cat note.txt",
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.output["type"], "bash");
    assert_eq!(result.output["success"], true);
    assert!(result.output["stdout"]
        .as_str()
        .unwrap()
        .contains(dir.path().to_string_lossy().as_ref()));
    assert!(result.output["stdout"]
        .as_str()
        .unwrap()
        .contains("hello\n"));
}

#[tokio::test]
#[cfg(unix)]
async fn code_run_tty_uses_fixed_size_and_accepts_interactive_input() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.background_process_pty_rows = 31;
    config.background_process_pty_cols = 97;
    let registry = ToolRegistry::new(&config).unwrap();
    let running = registry
        .dispatch(
            "code_run",
            json!({
                "script": "stty size; read line; printf 'reply=%s\\n' \"$line\"",
                "tty": true,
                "yield_time_ms": 250
            }),
        )
        .await
        .unwrap();
    assert_eq!(running.outcome, ToolExecutionOutcome::ProcessRunning);
    assert_eq!(running.output["tty"], true);
    assert!(running.output["stdout"]
        .as_str()
        .is_some_and(|stdout| stdout.contains("31 97")));
    let process_id = running.output["process_id"].as_str().unwrap();
    acknowledge_process_output(&registry, &running).await;
    let completed = registry
        .dispatch(
            "write_stdin",
            json!({
                "process_id": process_id,
                "chars": "hello\n",
                "yield_time_ms": 500
            }),
        )
        .await
        .unwrap();
    assert_eq!(completed.output["success"], true);
    assert!(completed.output["stdout"]
        .as_str()
        .is_some_and(|stdout| stdout.contains("reply=hello")));
}

#[tokio::test]
#[cfg(unix)]
async fn pty_ctrl_c_interrupts_the_foreground_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let child_pid_path = dir.path().join("pty-interrupt-child.pid");
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let running = registry
        .dispatch(
            "code_run",
            json!({
                "script": format!(
                    "sleep 30 & child=$!; echo $child > {}; wait $child",
                    shell_quote_path(&child_pid_path),
                ),
                "tty": true,
                "yield_time_ms": 250,
            }),
        )
        .await
        .unwrap();
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;
    let child_pid = wait_for_test_pid(&child_pid_path).await;

    let interrupted = registry
        .dispatch(
            "write_stdin",
            json!({
                "process_id": process_id,
                "chars": "\u{0003}",
                "yield_time_ms": 1_000,
            }),
        )
        .await
        .unwrap();
    assert_ne!(interrupted.output["state"], "running");
    assert_eq!(interrupted.output["success"], false);
    for _ in 0..40 {
        // SAFETY: the PID is emitted only by this test's same-process-group child fixture.
        if unsafe { libc::kill(child_pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("PTY Ctrl-C left foreground child {child_pid} alive");
}

#[tokio::test]
#[cfg(unix)]
async fn running_pty_output_pages_forward_and_terminate_reuses_managed_kill_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 64;
    config.background_process_output_buffer_bytes = 256;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let initial = registry
        .dispatch_with_context(
            "code_run",
            json!({
                "script": "printf '0123456789PROMPT>'; sleep 30",
                "tty": true,
                "yield_time_ms": 250,
                "max_output_chars": 5,
            }),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(initial.outcome, ToolExecutionOutcome::ProcessRunning);
    assert_eq!(initial.output["stdout"], "01234");
    assert_eq!(initial.output["stdout_cursor"], 5);
    assert_eq!(initial.output["truncated"], true);
    let process_id = initial.output["process_id"].as_str().unwrap().to_string();
    let initial_receipt = initial
        .process_delivery_receipt
        .expect("running PTY prefix must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;

    let tail = registry
        .dispatch_with_context(
            "write_stdin",
            json!({
                "process_id": process_id,
                "max_output_chars": 64,
                "yield_time_ms": 250,
            }),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(tail.output["state"], "running");
    assert_eq!(tail.output["stdout"], "56789PROMPT>");
    assert_eq!(tail.output["stdout_cursor"], 17);
    assert_eq!(tail.output["truncated"], false);
    let tail_receipt = tail
        .process_delivery_receipt
        .expect("running PTY tail must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&tail_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&tail_receipt))
        .await;

    let terminated = registry
        .dispatch_with_context(
            "write_stdin",
            json!({
                "process_id": process_id,
                "terminate": true,
                "yield_time_ms": 3_000,
            }),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        terminated.outcome,
        ToolExecutionOutcome::ProcessTerminated {
            signal: Some(libc::SIGKILL),
        }
    );
    assert!(terminated.outcome.is_success());
    assert_eq!(terminated.output["state"], "terminated");
    assert_eq!(terminated.output["signal"], libc::SIGKILL);
    assert_eq!(terminated.output["success"], false);
    let terminated_receipt = terminated
        .process_delivery_receipt
        .expect("terminated PTY result must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&terminated_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&terminated_receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_none());
}

#[tokio::test]
#[cfg(unix)]
async fn write_stdin_terminate_rejects_nonempty_input() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let running = registry
        .dispatch(
            "code_run",
            json!({
                "script": "sleep 30",
                "tty": true,
                "yield_time_ms": 250,
            }),
        )
        .await
        .unwrap();
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;
    let error = registry
        .dispatch(
            "write_stdin",
            json!({
                "process_id": process_id,
                "chars": "exit\n",
                "terminate": true,
            }),
        )
        .await
        .expect_err("hard terminate and terminal input must be mutually exclusive");
    assert!(error
        .to_string()
        .contains("terminate=true must not be combined with non-empty chars"));
    registry
        .dispatch(
            "write_stdin",
            json!({
                "process_id": process_id,
                "terminate": true,
                "yield_time_ms": 3_000,
            }),
        )
        .await
        .expect("cleanup terminate should succeed");
}

#[tokio::test]
#[cfg(unix)]
async fn delegation_code_runs_execute_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let first = registry.clone().for_delegation(None);
    let second = registry.for_delegation(None);
    let started = std::time::Instant::now();

    let (first_result, second_result) = tokio::join!(
        first.dispatch(
            "code_run",
            serde_json::json!({
                "script": "sleep 1; printf first",
            }),
        ),
        second.dispatch(
            "code_run",
            serde_json::json!({
                "script": "sleep 1; printf second",
            }),
        ),
    );

    assert_eq!(first_result.unwrap().output["stdout"], "first");
    assert_eq!(second_result.unwrap().output["stdout"], "second");
    assert!(
        started.elapsed() < Duration::from_millis(1700),
        "delegation code_run must not be serialized behind a workspace-wide lock"
    );
}

#[tokio::test]
async fn code_run_nonzero_exit_preserves_structured_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "code_run",
            serde_json::json!({
                "script": "printf 'out'; printf 'diagnostic' >&2; exit 7",
            }),
        )
        .await
        .unwrap();

    assert_eq!(
        result.outcome,
        ToolExecutionOutcome::ProcessExit {
            exit_code: Some(7),
            success: false,
        }
    );
    assert_eq!(result.output["exit_code"], 7);
    assert_eq!(result.output["success"], false);
    assert_eq!(result.output["stdout"], "out");
    assert_eq!(result.output["stderr"], "diagnostic");
}

#[tokio::test]
#[cfg(unix)]
async fn long_code_run_yields_process_id_without_timeout_kill() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let running = registry
        .dispatch(
            "code_run",
            serde_json::json!({
                "script": "sleep 1; printf complete",
                "yield_time_ms": 250
            }),
        )
        .await
        .expect("long command should yield");
    assert_eq!(running.outcome, ToolExecutionOutcome::ProcessRunning);
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let completed = registry
        .dispatch("write_stdin", json!({"process_id": process_id}))
        .await
        .unwrap();
    assert_eq!(completed.output["success"], true);
    assert_eq!(completed.output["stdout"], "complete");
}

#[tokio::test]
#[cfg(unix)]
async fn terminal_code_run_waits_for_provider_delivery_before_entry_removal() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let execution = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "printf delivered"}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(execution.output["stdout"], "delivered");
    let receipt = execution
        .process_delivery_receipt
        .expect("terminal code_run must create a provider delivery receipt");
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &receipt.process_id)
        .await
        .is_some());

    registry
        .begin_process_deliveries(std::slice::from_ref(&receipt))
        .await;
    registry
        .rollback_process_deliveries_for_owner(&session, None)
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &receipt.process_id)
        .await
        .is_some());

    let retry = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": receipt.process_id, "chars": ""}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(retry.output["stdout"], "delivered");
    let retry_receipt = retry
        .process_delivery_receipt
        .expect("retry must prepare a fresh delivery receipt");
    registry
        .begin_process_deliveries(std::slice::from_ref(&retry_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&retry_receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &receipt.process_id)
        .await
        .is_none());
}

#[tokio::test]
#[cfg(unix)]
async fn process_list_is_owner_scoped_and_write_stdin_reads_terminal_result() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let owner_session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let other_session = SessionId::from_str("session_bbbbbbbb").unwrap();
    let owner_context = file_tool_context(&owner_session);
    let running = registry
        .dispatch_with_context(
            "code_run",
            json!({
                "script": "sleep 1; printf complete",
                "yield_time_ms": 50,
            }),
            owner_context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(running.outcome, ToolExecutionOutcome::ProcessRunning);
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;

    let owner_list = registry
        .dispatch_with_context("process_list", json!({}), owner_context.clone())
        .await
        .unwrap();
    assert_eq!(owner_list.output["processes"].as_array().unwrap().len(), 1);
    assert_eq!(owner_list.output["processes"][0]["process_id"], process_id);
    assert_eq!(owner_list.output["processes"][0]["status"], "running");
    assert!(owner_list.output["processes"][0]["started_at"].is_number());
    assert!(owner_list.output["processes"][0]["elapsed_ms"].is_number());
    assert!(owner_list.output["processes"][0]["command"].is_string());
    assert!(owner_list.output["processes"][0].get("pid").is_none());

    let other_list = registry
        .dispatch_with_context("process_list", json!({}), file_tool_context(&other_session))
        .await
        .unwrap();
    assert!(other_list.output["processes"]
        .as_array()
        .unwrap()
        .is_empty());

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let completed = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "chars": "", "yield_time_ms": 50}),
            owner_context,
        )
        .await
        .unwrap();
    assert_eq!(completed.output["state"], "finished");
    assert_eq!(completed.output["stdout"], "complete");
    assert_eq!(completed.output["stdin_open"], false);
}

#[tokio::test]
#[cfg(unix)]
async fn main_agent_can_terminate_subagent_process_without_taking_input_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let main_registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let child_registry = main_registry.clone().for_delegation(None);
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let subagent_id = "subagent_12345678";
    let child_context = ToolDispatchContext {
        current_session_id: Some(session.clone()),
        current_turn_id: Some(subagent_id.into()),
        ..ToolDispatchContext::default()
    };
    let emit_tail_marker = dir.path().join("emit-child-tail");

    let running = child_registry
        .dispatch_with_context(
            "code_run",
            json!({
                "script": "trap '' INT; printf child-initial; while [ ! -f emit-child-tail ]; do sleep 0.01; done; printf child-tail; while :; do sleep 1; done",
                "yield_time_ms": 50,
            }),
            child_context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(running.outcome, ToolExecutionOutcome::ProcessRunning);
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&child_registry, &running).await;
    std::fs::write(emit_tail_marker, b"ready").unwrap();

    let child_list = child_registry
        .dispatch_with_context("process_list", json!({}), child_context.clone())
        .await
        .unwrap();
    assert_eq!(child_list.output["processes"].as_array().unwrap().len(), 1);
    assert_eq!(child_list.output["processes"][0]["process_id"], process_id);

    let main_list = main_registry
        .dispatch_with_context("process_list", json!({}), file_tool_context(&session))
        .await
        .unwrap();
    let processes = main_list.output["processes"].as_array().unwrap();
    assert_eq!(processes.len(), 1);
    assert_eq!(processes[0]["process_id"], process_id);
    assert_eq!(processes[0]["owner"], subagent_id);

    let root_poll = main_registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "chars": "", "yield_time_ms": 300}),
            file_tool_context(&session),
        )
        .await
        .unwrap();
    assert_eq!(root_poll.outcome, ToolExecutionOutcome::ProcessRunning);
    assert!(root_poll.output["stdout"]
        .as_str()
        .is_some_and(|stdout| stdout.contains("child-tail")));
    assert!(
        root_poll.process_delivery_receipt.is_none(),
        "main observation must not advance the subagent output-delivery cursor"
    );

    let cross_owner_input = main_registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "chars": "q"}),
            file_tool_context(&session),
        )
        .await
        .expect_err("main must not send arbitrary input to a subagent process");
    assert!(cross_owner_input
        .to_string()
        .contains("may only poll, send Ctrl-C"));

    let snapshots = main_registry
        .process_snapshots_for_root_session(&session)
        .await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].process_id, process_id);
    assert_eq!(snapshots[0].subagent_id.as_deref(), Some(subagent_id));

    let interrupted = main_registry
        .dispatch_with_context(
            "write_stdin",
            // 模拟 provider 将 JSON escape 再转义一次的实际 tool input。
            json!({"process_id": process_id, "chars": r"\u0003", "yield_time_ms": 50}),
            file_tool_context(&session),
        )
        .await
        .unwrap();
    assert_eq!(interrupted.outcome, ToolExecutionOutcome::ProcessRunning);
    assert!(
        interrupted.process_delivery_receipt.is_none(),
        "main interrupt must not advance the subagent output-delivery cursor"
    );

    let terminated = main_registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "terminate": true, "yield_time_ms": 500}),
            file_tool_context(&session),
        )
        .await;
    let terminated = terminated.expect("main must hard-terminate a subagent process");
    assert_eq!(
        terminated.outcome,
        ToolExecutionOutcome::ProcessTerminated {
            signal: Some(libc::SIGKILL),
        }
    );
    assert_eq!(terminated.output["state"], "terminated");
    assert!(
        terminated.process_delivery_receipt.is_none(),
        "main termination must not consume the subagent terminal result"
    );

    let child_final = child_registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "chars": "", "yield_time_ms": 50}),
            child_context,
        )
        .await
        .expect("subagent must still receive its terminal result");
    assert_eq!(child_final.output["state"], "terminated");
    assert_eq!(child_final.output["signal"], libc::SIGKILL);
    assert!(child_final.output["stdout"]
        .as_str()
        .is_some_and(|stdout| stdout.contains("child-tail")));
    assert!(child_final.process_delivery_receipt.is_some());

    main_registry
        .cleanup_processes_for_owner(&session, Some(subagent_id))
        .await;
}

#[tokio::test]
async fn ps_snapshots_hide_starting_entries_until_their_controls_are_attached() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let entry = registry
        .process_manager
        .reserve(
            ProcessOwner::main(session.as_str()),
            "sleep 1".into(),
            "bash".into(),
            dir.path().display().to_string(),
            false,
        )
        .await
        .unwrap();

    assert!(registry
        .process_snapshots_for_root_session(&session)
        .await
        .is_empty());
    entry.mark_running().await;
    let snapshots = registry.process_snapshots_for_root_session(&session).await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].status, "running");
}

#[tokio::test]
#[cfg(unix)]
async fn pipe_process_rejects_text_but_accepts_ctrl_c() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let running = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "trap '' INT; while :; do sleep 1; done", "yield_time_ms": 50}),
            context.clone(),
        )
        .await
        .unwrap();
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;
    let text_error = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "chars": "hello"}),
            context.clone(),
        )
        .await
        .expect_err("pipe backend must not offer text stdin");
    assert!(text_error.to_string().contains("pipe-backed process"));

    let interrupted = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "chars": "\u{0003}", "yield_time_ms": 100}),
            context.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        interrupted.outcome,
        ToolExecutionOutcome::ProcessExit { .. } | ToolExecutionOutcome::ProcessRunning
    ));
    let live = registry
        .dispatch_with_context("process_list", json!({}), context)
        .await
        .unwrap();
    assert_eq!(live.output["processes"][0]["status"], "running");
    let instance_id = registry
        .process_snapshots_for_root_session(&session)
        .await
        .into_iter()
        .find(|entry| entry.process_id == process_id)
        .expect("running process must have a /ps snapshot")
        .instance_id;
    registry
        .terminate_process_for_root_session(&session, &process_id, None, instance_id)
        .await
        .expect("hard terminate must still work after ignored SIGINT");
}

#[tokio::test]
#[cfg(unix)]
async fn runtime_terminate_reports_already_exited_after_a_stale_ps_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let running = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 0.5", "yield_time_ms": 250}),
            context,
        )
        .await
        .unwrap();
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    let stale_snapshot = registry.process_snapshots_for_root_session(&session).await;
    let stale_instance_id = stale_snapshot
        .iter()
        .find(|entry| entry.process_id == process_id)
        .expect("running process must have a /ps snapshot")
        .instance_id;

    tokio::time::sleep(Duration::from_millis(700)).await;
    let error = registry
        .terminate_process_for_root_session(&session, &process_id, None, stale_instance_id)
        .await
        .expect_err("a process that exited after /ps snapshot must not be terminated");
    assert!(error.to_string().contains("already exited"));
}

#[tokio::test]
#[cfg(unix)]
async fn truncated_final_output_advances_by_provider_acknowledged_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 64;
    config.background_process_output_buffer_bytes = 64;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let running = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "sleep 1; printf 0123456789", "yield_time_ms": 50}),
            context.clone(),
        )
        .await
        .unwrap();
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let partial = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 3}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(partial.output["truncated"], true);
    assert_eq!(partial.output["stdout"], "012");
    assert_eq!(partial.output["stdout_cursor"], 3);
    let partial_receipt = partial
        .process_delivery_receipt
        .expect("visible prefix must prepare a partial delivery receipt");
    registry
        .begin_process_deliveries(std::slice::from_ref(&partial_receipt))
        .await;
    registry
        .rollback_process_deliveries_for_owner(&session, None)
        .await;

    let retry = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 3}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(retry.output["stdout"], "012");
    assert_eq!(retry.output["stdout_cursor"], 3);
    let retry_receipt = retry
        .process_delivery_receipt
        .expect("rolled-back page must prepare a fresh receipt");
    registry
        .begin_process_deliveries(std::slice::from_ref(&retry_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&retry_receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_some());

    let second_page = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 3}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.output["stdout"], "345");
    assert_eq!(second_page.output["stdout_cursor"], 6);
    let second_receipt = second_page
        .process_delivery_receipt
        .expect("second page must prepare a partial delivery receipt");
    registry
        .begin_process_deliveries(std::slice::from_ref(&second_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&second_receipt))
        .await;

    let final_page = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 64}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(final_page.output["stdout"], "6789");
    assert_eq!(final_page.output["stdout_cursor"], 10);
    assert_eq!(final_page.output["truncated"], false);
    let final_receipt = final_page
        .process_delivery_receipt
        .expect("last page must prepare the final delivery receipt");
    registry
        .begin_process_deliveries(std::slice::from_ref(&final_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&final_receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_none());
}

#[tokio::test]
#[cfg(unix)]
async fn initially_finished_truncated_code_run_is_retained_until_a_full_result_is_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 64;
    config.background_process_output_buffer_bytes = 64;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);

    let initial = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "printf 0123456789", "max_output_chars": 3}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(initial.output["truncated"], true);
    assert_eq!(initial.output["stdout"], "012");
    assert_eq!(initial.output["stdout_cursor"], 3);
    let process_id = initial.output["process_id"]
        .as_str()
        .expect("truncated terminal result keeps a logical process id")
        .to_string();
    let initial_receipt = initial
        .process_delivery_receipt
        .expect("initial visible prefix must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_some());

    let completed = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 64}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(completed.output["stdout"], "3456789");
    let receipt = completed
        .process_delivery_receipt
        .expect("complete result must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_none());
}

#[tokio::test]
#[cfg(unix)]
async fn partial_delivery_cursor_counts_utf8_scalars() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 64;
    config.background_process_output_buffer_bytes = 64;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let initial = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "printf '甲乙丙丁'", "max_output_chars": 2}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(initial.output["stdout"], "甲乙");
    assert_eq!(initial.output["stdout_cursor"], 2);
    let initial_receipt = initial
        .process_delivery_receipt
        .expect("UTF-8 prefix must prepare a delivery receipt");
    let process_id = initial_receipt.process_id.clone();
    registry
        .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;

    let final_page = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 64}),
            context,
        )
        .await
        .unwrap();
    assert_eq!(final_page.output["stdout"], "丙丁");
    assert_eq!(final_page.output["stdout_cursor"], 4);
    let final_receipt = final_page
        .process_delivery_receipt
        .expect("UTF-8 final page must prepare a delivery receipt");
    registry
        .begin_process_deliveries(std::slice::from_ref(&final_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&final_receipt))
        .await;
}

#[tokio::test]
#[cfg(unix)]
async fn configured_small_output_limit_pages_across_retained_head_tail_gap() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 2;
    // pipe backend splits the total retained budget evenly between stdout/stderr.
    config.background_process_output_buffer_bytes = 12;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let initial = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "printf abcdefghi"}),
            context.clone(),
        )
        .await
        .unwrap();
    let process_id = initial.output["process_id"].as_str().unwrap().to_string();

    let mut pages = vec![initial];
    for _ in 0..8 {
        let receipt = pages
            .last()
            .and_then(|page| page.process_delivery_receipt.clone())
            .expect("every page must prepare a delivery receipt");
        registry
            .begin_process_deliveries(std::slice::from_ref(&receipt))
            .await;
        registry
            .commit_process_deliveries(std::slice::from_ref(&receipt))
            .await;
        if receipt.final_result {
            break;
        }
        pages.push(
            registry
                .dispatch_with_context(
                    "write_stdin",
                    json!({"process_id": process_id}),
                    context.clone(),
                )
                .await
                .unwrap(),
        );
    }

    assert_eq!(pages.len(), 5, "unexpected page sequence: {pages:#?}");
    assert_eq!(pages[0].output["stdout"], "ab");
    assert_eq!(pages[0].output["stdout_cursor"], 2);
    assert_eq!(pages[1].output["stdout"], "c");
    assert_eq!(pages[1].output["stdout_cursor"], 3);
    assert_eq!(pages[2].output["stdout"], "");
    assert_eq!(pages[2].output["stdout_cursor"], 6);
    assert_eq!(pages[2].output["omitted_bytes"], 3);
    assert_eq!(pages[3].output["stdout"], "gh");
    assert_eq!(pages[3].output["stdout_cursor"], 8);
    assert_eq!(pages[4].output["stdout"], "i");
    assert_eq!(pages[4].output["stdout_cursor"], 9);
    let final_receipt = pages[4]
        .process_delivery_receipt
        .clone()
        .expect("retained tail's final page must await provider delivery");
    assert!(final_receipt.final_result);
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_none());
}

#[tokio::test]
#[cfg(unix)]
async fn partial_explicit_cursor_pair_is_rejected_without_consuming_final_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 64;
    config.background_process_output_buffer_bytes = 64;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let initial = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "printf 0123456789", "max_output_chars": 3}),
            context.clone(),
        )
        .await
        .unwrap();
    let process_id = initial.output["process_id"].as_str().unwrap().to_string();
    let initial_receipt = initial
        .process_delivery_receipt
        .expect("initial visible prefix must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;

    let stdout_only = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "stdout_cursor": 0}),
            context.clone(),
        )
        .await
        .expect_err("stdout cursor without stderr cursor must be rejected");
    assert!(stdout_only
        .to_string()
        .contains("stdout_cursor and stderr_cursor must be supplied together"));

    let stderr_only = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "stderr_cursor": 0}),
            context.clone(),
        )
        .await
        .expect_err("stderr cursor without stdout cursor must be rejected");
    assert!(stderr_only
        .to_string()
        .contains("stdout_cursor and stderr_cursor must be supplied together"));

    let complete = registry
        .dispatch_with_context(
            "write_stdin",
            json!({"process_id": process_id, "max_output_chars": 64}),
            context.clone(),
        )
        .await
        .unwrap();
    assert_eq!(complete.output["stdout"], "3456789");
    assert!(complete.process_delivery_receipt.is_some());
}

#[tokio::test]
#[cfg(unix)]
async fn terminal_explicit_cursor_uses_committed_cursor_and_cannot_skip_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_tool_config(dir.path());
    config.code_run_max_output_chars = 64;
    config.background_process_output_buffer_bytes = 64;
    let registry = ToolRegistry::new(&config).unwrap();
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let context = file_tool_context(&session);
    let initial = registry
        .dispatch_with_context(
            "code_run",
            json!({"script": "printf 0123456789", "max_output_chars": 3}),
            context.clone(),
        )
        .await
        .unwrap();
    let process_id = initial.output["process_id"].as_str().unwrap().to_string();
    let initial_receipt = initial
        .process_delivery_receipt
        .expect("initial visible prefix must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
        .await;

    let final_snapshot = registry
        .dispatch_with_context(
            "write_stdin",
            json!({
                "process_id": process_id,
                "stdout_cursor": 5,
                "stderr_cursor": 0,
                "max_output_chars": 64,
            }),
            context.clone(),
        )
        .await
        .unwrap();
    // 即使调用方传入更靠后的显式 cursor，终态结果仍必须从已经确认交付的 cursor=3
    // 开始；否则 provider 成功后会删除 entry 并永久跳过 3..4。
    assert_eq!(final_snapshot.output["stdout"], "3456789");
    assert_eq!(final_snapshot.output["truncated"], false);
    let receipt = final_snapshot
        .process_delivery_receipt
        .expect("terminal snapshot must await provider delivery");
    registry
        .begin_process_deliveries(std::slice::from_ref(&receipt))
        .await;
    registry
        .commit_process_deliveries(std::slice::from_ref(&receipt))
        .await;
    assert!(registry
        .process_manager
        .find_for_owner(&registry.process_owner(&context), &process_id)
        .await
        .is_none());
}

#[tokio::test]
#[cfg(unix)]
async fn signal_exit_is_exposed_without_fabricating_an_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "code_run",
            json!({"script": "kill -TERM $$", "yield_time_ms": 500}),
        )
        .await
        .unwrap();
    assert_eq!(result.output["exit_code"], Value::Null);
    assert_eq!(result.output["signal"], libc::SIGTERM);
    assert_eq!(result.output["success"], false);
}

#[tokio::test]
#[cfg(unix)]
async fn code_run_cleans_background_process_group_after_parent_exits() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let started = std::time::Instant::now();
    let result = registry
        .dispatch(
            "code_run",
            serde_json::json!({
                "script": "(sleep 10) & echo parent-done",
                "yield_time_ms": 250
            }),
        )
        .await
        .expect("command should return after parent exits");

    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(result.output["success"], true);
    assert!(result.output["stdout"]
        .as_str()
        .unwrap()
        .contains("parent-done"));
}

#[tokio::test]
#[cfg(unix)]
async fn dropping_unregistered_pipe_guard_kills_and_reaps_its_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = Command::new("bash");
    command
        .args(["-lc", "sleep 300"])
        .current_dir(dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_process_group(&mut command);
    let child = command.spawn().expect("pipe fixture should spawn");
    let process_group_id = child
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .expect("Unix child PID should fit the managed PGID type");

    drop(crate::tool::command::SpawnedProcessKillGuard::new(
        child,
        Some(process_group_id),
    ));

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            // SAFETY: this test generated the isolated process group and the drop guard
            // owns its only root child; probe only observes whether cleanup has completed.
            let result = unsafe { libc::kill(-process_group_id, 0) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("unregistered pipe guard must explicitly kill and reap its process group");
}

#[tokio::test]
#[cfg(unix)]
async fn capacity_eviction_kills_the_evicted_live_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let pid_path = dir.path().join("evicted-root.pid");
    let mut config = test_tool_config(dir.path());
    config.background_process_max_entries_per_owner = 1;
    config.background_process_protected_recent_entries = 0;
    let registry = ToolRegistry::new(&config).unwrap();
    let first = registry
        .dispatch(
            "code_run",
            json!({
                "script": format!("echo $$ > {}; sleep 30", shell_quote_path(&pid_path)),
                "yield_time_ms": 50,
            }),
        )
        .await
        .unwrap();
    assert_eq!(first.outcome, ToolExecutionOutcome::ProcessRunning);
    let pid = wait_for_test_pid(&pid_path).await;

    let replacement = registry
        .dispatch(
            "code_run",
            json!({"script": "printf replacement", "yield_time_ms": 500}),
        )
        .await
        .unwrap();
    assert_eq!(replacement.output["stdout"], "replacement");
    for _ in 0..20 {
        // SAFETY: kill(pid, 0) only probes the PID obtained from this test fixture.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("capacity eviction left process group leader {pid} alive");
}

#[tokio::test]
#[cfg(unix)]
async fn runtime_shutdown_kills_all_registered_process_groups() {
    let dir = tempfile::tempdir().unwrap();
    let pid_path = dir.path().join("shutdown-root.pid");
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let started = registry
        .dispatch(
            "code_run",
            json!({
                "script": format!("echo $$ > {}; sleep 30", shell_quote_path(&pid_path)),
                "yield_time_ms": 50,
            }),
        )
        .await
        .unwrap();
    assert_eq!(started.outcome, ToolExecutionOutcome::ProcessRunning);
    let pid = wait_for_test_pid(&pid_path).await;

    registry.shutdown_background_processes().await;

    for _ in 0..40 {
        // SAFETY: kill(pid, 0) only probes the PID recorded by this test fixture.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("runtime shutdown left process group leader {pid} alive");
}

#[tokio::test]
#[cfg(unix)]
async fn runtime_shutdown_during_pipe_handoff_kills_registered_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let root_pid_path = dir.path().join("handoff-shutdown-root.pid");
    let child_pid_path = dir.path().join("handoff-shutdown-child.pid");
    let registry = Arc::new(ToolRegistry::new(&test_tool_config(dir.path())).unwrap());
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let gate = registry.process_manager.pause_next_handoff_for_test().await;
    let dispatch_registry = Arc::clone(&registry);
    let dispatch_session = session.clone();
    let script = format!(
        "echo $$ > {}; sleep 30 & echo $! > {}; wait",
        shell_quote_path(&root_pid_path),
        shell_quote_path(&child_pid_path),
    );
    let dispatch = tokio::spawn(async move {
        dispatch_registry
            .dispatch_with_context(
                "code_run",
                json!({"script": script, "yield_time_ms": 10_000}),
                file_tool_context(&dispatch_session),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), gate.wait_until_entered())
        .await
        .expect("pipe handoff must pause after reservation");
    let root_pid = wait_for_test_pid(&root_pid_path).await;
    let child_pid = wait_for_test_pid(&child_pid_path).await;

    // 此时 handoff 尚未 attach Pipe，但 reserve 已把 PGID 线性化写入 manager；
    // runtime shutdown 必须仍能杀掉 root 及其同组 child。
    registry.shutdown_background_processes().await;
    gate.release();
    let _ = tokio::time::timeout(Duration::from_secs(2), dispatch).await;

    for _ in 0..80 {
        // SAFETY: both PIDs are produced solely by this test's shell fixture.
        let root_gone = unsafe { libc::kill(root_pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        // SAFETY: both PIDs are produced solely by this test's shell fixture.
        let child_gone = unsafe { libc::kill(child_pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        if root_gone && child_gone {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("runtime shutdown during handoff left root {root_pid} or child {child_pid} alive");
}

#[tokio::test]
#[cfg(unix)]
async fn hard_abort_after_pipe_reservation_keeps_registered_process_running() {
    assert_hard_abort_after_registered_code_run_preserves_process(false).await;
}

#[tokio::test]
#[cfg(unix)]
async fn hard_abort_after_pty_reservation_keeps_registered_process_running() {
    assert_hard_abort_after_registered_code_run_preserves_process(true).await;
}

#[tokio::test]
#[cfg(unix)]
async fn cancellation_during_pipe_registration_reaps_unmanaged_process_group() {
    assert_cancellation_during_registration_reaps_unmanaged_process_group(false).await;
}

#[tokio::test]
#[cfg(unix)]
async fn cancellation_during_pty_registration_reaps_unmanaged_process_group() {
    assert_cancellation_during_registration_reaps_unmanaged_process_group(true).await;
}

#[cfg(unix)]
async fn assert_cancellation_during_registration_reaps_unmanaged_process_group(tty: bool) {
    let dir = tempfile::tempdir().unwrap();
    let pid_path = dir.path().join(if tty {
        "cancel-registration-pty.pid"
    } else {
        "cancel-registration-pipe.pid"
    });
    let registry = Arc::new(ToolRegistry::new(&test_tool_config(dir.path())).unwrap());
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let gate = registry
        .process_manager
        .pause_next_reservation_for_test()
        .await;
    let cancellation = CancellationToken::new();
    let dispatch_registry = Arc::clone(&registry);
    let dispatch_session = session.clone();
    let dispatch_cancellation = cancellation.clone();
    let script = format!("echo $$ > {}; sleep 30", shell_quote_path(&pid_path));
    let dispatch = tokio::spawn(async move {
        dispatch_registry
            .dispatch_with_context(
                "code_run",
                json!({"script": script, "tty": tty, "yield_time_ms": 10_000}),
                ToolDispatchContext {
                    current_session_id: Some(dispatch_session),
                    cancellation: Some(dispatch_cancellation),
                    ..ToolDispatchContext::default()
                },
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), gate.wait_until_entered())
        .await
        .expect("spawned process must pause immediately before registration");
    let pid = wait_for_test_pid(&pid_path).await;
    cancellation.cancel();
    gate.release();
    let error = tokio::time::timeout(Duration::from_secs(3), dispatch)
        .await
        .expect("cancelled registration should settle")
        .expect("tool task should not panic")
        .expect_err("cancellation before handoff must not create a background session");
    assert!(matches!(error, ToolError::Interrupted));
    assert!(registry
        .process_snapshots_for_root_session(&session)
        .await
        .is_empty());

    for _ in 0..80 {
        // SAFETY: this PID is written solely by the fixture process owned by this test.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "cancellation during registration left {} process group leader {pid} alive",
        if tty { "PTY" } else { "pipe" }
    );
}

#[cfg(unix)]
async fn assert_hard_abort_after_registered_code_run_preserves_process(tty: bool) {
    let dir = tempfile::tempdir().unwrap();
    let pid_path = dir.path().join(if tty {
        "hard-abort-pty.pid"
    } else {
        "hard-abort-pipe.pid"
    });
    let registry = Arc::new(ToolRegistry::new(&test_tool_config(dir.path())).unwrap());
    let session = SessionId::from_str("session_aaaaaaaa").unwrap();
    let gate = registry.process_manager.pause_next_handoff_for_test().await;
    let dispatch_registry = Arc::clone(&registry);
    let dispatch_session = session.clone();
    let script = format!("echo $$ > {}; sleep 30", shell_quote_path(&pid_path));
    let dispatch = tokio::spawn(async move {
        dispatch_registry
            .dispatch_with_context(
                "code_run",
                json!({"script": script, "tty": tty, "yield_time_ms": 10_000}),
                file_tool_context(&dispatch_session),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), gate.wait_until_entered())
        .await
        .expect("handoff must own the registered process before hard abort");
    dispatch.abort();
    assert!(dispatch
        .await
        .expect_err("hard abort must cancel tool future")
        .is_cancelled());
    gate.release();

    let pid = wait_for_test_pid(&pid_path).await;
    for _ in 0..40 {
        let snapshots = registry.process_snapshots_for_root_session(&session).await;
        if snapshots.iter().any(|entry| entry.status == "running") {
            // SAFETY: this PID comes only from the test fixture process group leader.
            assert_eq!(unsafe { libc::kill(pid, 0) }, 0);
            registry.shutdown_background_processes().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    registry.shutdown_background_processes().await;
    panic!("hard-aborted code_run did not leave its registered process managed");
}

#[tokio::test]
#[cfg(unix)]
async fn pty_fast_root_exit_still_kills_same_group_descendant() {
    let dir = tempfile::tempdir().unwrap();
    let descendant_pid_path = dir.path().join("pty-same-group.pid");
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let result = registry
        .dispatch(
            "code_run",
            json!({
                "script": format!(
                    "sleep 30 & echo $! > {}; exit 0",
                    shell_quote_path(&descendant_pid_path)
                ),
                "tty": true,
                "yield_time_ms": 250,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.output["success"], true);
    let descendant_pid = wait_for_test_pid(&descendant_pid_path).await;
    for _ in 0..40 {
        // SAFETY: this PID is produced only by the test fixture above.
        if unsafe { libc::kill(descendant_pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("PTY root exit left same-process-group descendant {descendant_pid} alive");
}

#[tokio::test]
#[cfg(unix)]
async fn pty_escape_descendant_does_not_block_terminal_cleanup_workers() {
    let dir = tempfile::tempdir().unwrap();
    let escaped_pid_path = dir.path().join("escaped-pty.pid");
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let running = registry
        .dispatch(
            "code_run",
            json!({
                "script": format!(
                    "setsid sleep 30 & echo $! > {}; sleep 1; printf root-exit",
                    shell_quote_path(&escaped_pid_path)
                ),
                "tty": true,
                "yield_time_ms": 50,
            }),
        )
        .await
        .unwrap();
    let process_id = running.output["process_id"].as_str().unwrap().to_string();
    acknowledge_process_output(&registry, &running).await;
    let escaped_pid = wait_for_test_pid(&escaped_pid_path).await;
    let settled = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let polled = registry
                .dispatch(
                    "write_stdin",
                    json!({"process_id": process_id, "yield_time_ms": 50}),
                )
                .await
                .unwrap();
            acknowledge_process_output(&registry, &polled).await;
            if polled.output["state"] != "running" {
                return polled;
            }
        }
    })
    .await
    .expect("escaped PTY child must not hold watcher completion forever");
    assert_eq!(settled.output["state"], "finished");
    // SAFETY: cleanup targets only the escaped fixture PID recorded by this test.
    let _ = unsafe { libc::kill(escaped_pid, libc::SIGKILL) };
}

#[cfg(unix)]
async fn wait_for_test_pid(path: &Path) -> i32 {
    for _ in 0..80 {
        if let Ok(raw) = tokio::fs::read_to_string(path).await {
            if let Ok(pid) = raw.trim().parse::<i32>() {
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("fixture did not write pid: {}", path.display());
}

#[cfg(unix)]
fn shell_quote_path(path: &Path) -> String {
    format!(
        "'{}'",
        path.display().to_string().replace('\'', "'\\\"'\\\"'")
    )
}

#[tokio::test]
#[cfg(unix)]
async fn escaped_descendant_holding_pipe_fd_cannot_block_root_terminal_completion() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("escaped.pid");
    let pid_file_shell = format!(
        "'{}'",
        pid_file.display().to_string().replace('\'', "'\\\"'\\\"'")
    );
    let mut config = test_tool_config(dir.path());
    config.background_process_output_drain_grace_ms = 1;
    let registry = ToolRegistry::new(&config).unwrap();
    let started = std::time::Instant::now();
    let result = registry
            .dispatch(
                "code_run",
                serde_json::json!({
                    "script": format!(
                        "python3 -c 'import os,sys,time; os.setsid(); open(sys.argv[1], \"w\").write(str(os.getpid())); sys.stdout.write(\"escaped-ready\\n\"); sys.stdout.flush(); time.sleep(30)' {pid_file_shell} & while [ ! -s {pid_file_shell} ]; do sleep 0.01; done; echo parent-done"
                    ),
                    "yield_time_ms": 1000
                }),
            )
            .await
            .expect("root terminal should finish even when an escaped descendant holds stdout");

    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(result.output["success"], true);
    assert_eq!(result.output["truncated"], true);
    assert!(result.output["stdout"]
        .as_str()
        .is_some_and(|stdout| stdout.contains("parent-done")));

    let mut escaped_pid = None;
    for _ in 0..50 {
        if let Ok(text) = tokio::fs::read_to_string(&pid_file).await {
            if let Ok(pid) = text.trim().parse::<i32>() {
                escaped_pid = Some(pid);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(pid) = escaped_pid {
        // 测试主动制造了逃离 ProcessManager 所有权边界的后代，必须自行回收。
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}
