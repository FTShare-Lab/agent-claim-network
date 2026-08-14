//! 本地命令工具及受管后台进程在 registry 层的生命周期编排。
//!
//! 核心实现是 `ToolRegistry` 的 code_run/write_stdin 方法与进程交接辅助逻辑。

use super::*;

#[derive(Clone, Copy)]
struct ProcessExecutionDelivery {
    track: bool,
    allow_terminal: bool,
    termination_requested: bool,
}

impl ToolRegistry {
    pub(crate) fn empty_background_process_projection() -> String {
        concat!(
            "<background_processes>\n",
            "Authoritative runtime state, not a user request. process_id is not an OS PID.\n",
            "Processes:\n",
            "- none\n",
            "Use process_list for full live-process details and write_stdin with empty chars to read final output.\n",
            "</background_processes>"
        )
        .to_string()
    }

    pub(crate) async fn process_snapshots_for_root_session(
        &self,
        session_id: &SessionId,
    ) -> Vec<ProcessSnapshot> {
        let entries = self
            .process_manager
            .live_for_root(session_id.as_str())
            .await;
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            let state = entry.state().await;
            if !matches!(state, ProcessState::Running | ProcessState::Terminating) {
                continue;
            }
            snapshots.push(ProcessSnapshot {
                process_id: entry.id.as_str().to_string(),
                instance_id: entry.instance_id,
                root_session_id: entry.owner.root_session_id.clone(),
                subagent_id: entry.owner.subagent_id.clone(),
                status: state.label().to_string(),
                tty: entry.tty,
                command: entry.command.clone(),
                code_type: entry.code_type.clone(),
                cwd: entry.cwd.clone(),
                started_at: entry.started_at,
            });
        }
        snapshots.sort_by(|left, right| {
            let status_rank = |status: &str| match status {
                "running" => 0_u8,
                "terminating" => 1,
                _ => 2,
            };
            status_rank(&left.status)
                .cmp(&status_rank(&right.status))
                .then_with(|| right.started_at.cmp(&left.started_at))
                .then_with(|| left.process_id.cmp(&right.process_id))
        });
        snapshots
    }

    /// 供 SessionEngine/TUI durable 消费的后台完成事件；成功写 journal 后再 ack。
    pub(crate) async fn pending_process_completions_for_root_session(
        &self,
        session_id: &SessionId,
    ) -> Vec<ProcessCompletion> {
        self.process_manager
            .pending_completions_for_root(session_id.as_str())
            .await
    }

    pub(crate) async fn acknowledge_process_completion_for_root_session(
        &self,
        session_id: &SessionId,
        instance_id: u64,
    ) {
        self.process_manager
            .acknowledge_completion_for_root(session_id.as_str(), instance_id)
            .await;
    }

    /// 供 SessionEngine 消费的后台 terminal 生命周期事件。它们仅用于 runtime / TUI
    /// 投影，不会作为额外的模型 tool result 回填。
    pub(crate) async fn take_background_events_for_root_session(
        &self,
        session_id: &SessionId,
    ) -> Vec<BackgroundProcessEvent> {
        self.process_manager
            .take_background_events_for_root(session_id.as_str())
            .await
    }

    /// 为一个 provider request 同时冻结 completion notification 与其 runtime projection。
    /// notification 在之后才完成不能被本 request 成功提交，避免“已提交但未展示”的竞态。
    pub(crate) async fn begin_background_process_projection_delivery_for_owner(
        &self,
        session_id: &SessionId,
        subagent_id: Option<&str>,
    ) -> (Option<String>, Vec<ProcessCompletionDeliveryReceipt>) {
        let owner = self.process_owner_for_session(session_id, subagent_id);
        let (delivery_ids, completion_notifications) = self
            .process_manager
            .begin_completion_notification_delivery_snapshot(&owner)
            .await;
        let projection = self
            .background_process_projection_for_owner_with_notifications(
                &owner,
                completion_notifications,
            )
            .await;
        (projection, delivery_ids)
    }

    pub(super) async fn background_process_projection_for_owner_with_notifications(
        &self,
        owner: &ProcessOwner,
        completion_notifications: Vec<ProcessCompletion>,
    ) -> Option<String> {
        let entries = self.process_manager.retained_for_owner(owner).await;
        if entries.is_empty() && completion_notifications.is_empty() {
            return None;
        }
        let mut rows = Vec::new();
        for entry in entries {
            let state = entry.state().await;
            let final_output_available = !state.is_live();
            let command = truncate_chars(&entry.command, 400).0;
            let cwd = truncate_chars(&entry.cwd, 200).0;
            let (exit_code, signal) = match state {
                ProcessState::Finished {
                    exit_code, signal, ..
                }
                | ProcessState::Terminated { exit_code, signal } => (exit_code, signal),
                ProcessState::Starting
                | ProcessState::Running
                | ProcessState::Terminating
                | ProcessState::Error => (None, None),
            };
            rows.push((
                entry.id.as_str().to_string(),
                entry.instance_id,
                format!(
                    "- process_id={} instance_id={} state={} exit_code={} signal={} final_output_available={} tty={} command={:?} cwd={:?}",
                    entry.id.as_str(),
                    entry.instance_id,
                    semantic_process_state_label(state),
                    exit_code.map_or_else(|| "null".into(), |code| code.to_string()),
                    signal.map_or_else(|| "null".into(), |value| value.to_string()),
                    final_output_available,
                    entry.tty,
                    command,
                    cwd,
                ),
            ));
        }
        let retained_instances = rows
            .iter()
            .map(|entry| (entry.0.clone(), entry.1))
            .collect::<BTreeSet<_>>();
        for completion in completion_notifications {
            if retained_instances.contains(&(completion.process_id.clone(), completion.instance_id))
            {
                continue;
            }
            rows.push((
                completion.process_id.clone(),
                completion.instance_id,
                format!(
                    "- process_id={} instance_id={} state={} exit_code={} signal={} final_output_available=false",
                    completion.process_id,
                    completion.instance_id,
                    completion.status,
                    completion
                        .exit_code
                        .map_or_else(|| "null".into(), |code| code.to_string()),
                    completion
                        .signal
                        .map_or_else(|| "null".into(), |value| value.to_string()),
                ),
            ));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

        let mut projection = String::from(
            "<background_processes>\nAuthoritative runtime state, not a user request. process_id is not an OS PID.\n",
        );
        projection.push_str("Processes:\n");
        for (_, _, row) in rows {
            projection.push_str(&row);
            projection.push('\n');
        }
        projection.push_str(
            "Use process_list for full live-process details and write_stdin with empty chars to read final output.\n</background_processes>",
        );
        Some(projection)
    }

    pub(crate) async fn rollback_process_deliveries_for_owner(
        &self,
        session_id: &SessionId,
        subagent_id: Option<&str>,
    ) {
        let owner = self.process_owner_for_session(session_id, subagent_id);
        self.process_manager
            .rollback_inflight_deliveries_for_owner(&owner)
            .await;
        self.process_manager
            .rollback_completion_notification_delivery(&owner)
            .await;
    }

    /// 当前 turn 未能得到后续 provider 成功响应时，撤销该 owner 全部未提交输出 cursor。
    /// 这与下一 request 开始时只回滚 inflight 的路径不同：这里的 pending 从未进入
    /// provider request，必须让下一次 poll 从已提交 cursor 重新交付。
    pub(crate) async fn rollback_uncommitted_process_deliveries_for_context(
        &self,
        context: &ToolDispatchContext,
    ) {
        let owner = self.process_owner(context);
        self.process_manager
            .rollback_uncommitted_deliveries_for_owner(&owner)
            .await;
    }

    pub(crate) async fn commit_completion_notification_delivery_for_owner(
        &self,
        session_id: &SessionId,
        subagent_id: Option<&str>,
        receipts: &[ProcessCompletionDeliveryReceipt],
    ) {
        let owner = self.process_owner_for_session(session_id, subagent_id);
        self.process_manager
            .commit_completion_notification_delivery(&owner, receipts)
            .await;
    }

    pub(crate) async fn begin_process_deliveries(&self, receipts: &[ProcessDeliveryReceipt]) {
        self.process_manager.begin_deliveries(receipts).await;
    }

    pub(crate) async fn rollback_process_deliveries(&self, receipts: &[ProcessDeliveryReceipt]) {
        self.process_manager
            .rollback_uncommitted_deliveries(receipts)
            .await;
    }

    pub(crate) async fn commit_process_deliveries(&self, receipts: &[ProcessDeliveryReceipt]) {
        self.process_manager.commit_deliveries(receipts).await;
    }

    pub(crate) async fn terminate_process_for_root_session(
        &self,
        session_id: &SessionId,
        process_id: &str,
        subagent_id: Option<&str>,
        instance_id: u64,
    ) -> Result<(), ToolError> {
        let owner = self.process_owner_for_session(session_id, subagent_id);
        match self
            .process_manager
            .terminate_live_for_root_matching(
                session_id.as_str(),
                process_id,
                &owner,
                instance_id,
                libc::SIGKILL,
            )
            .await
            .map_err(ToolError::InvalidArgs)?
        {
            TerminateRequestResult::Requested | TerminateRequestResult::AlreadyTerminating => {
                Ok(())
            }
            TerminateRequestResult::AlreadyExited => {
                Err(ToolError::InvalidArgs("process has already exited".into()))
            }
        }
    }

    pub(crate) async fn cleanup_processes_for_owner(
        &self,
        root_session_id: &SessionId,
        subagent_id: Option<&str>,
    ) {
        let owner = self.process_owner_for_session(root_session_id, subagent_id);
        self.process_manager.cleanup_owner(&owner).await;
    }

    pub(crate) async fn cleanup_processes_for_session(&self, session_id: &SessionId) {
        self.process_manager
            .cleanup_root_session(session_id.as_str())
            .await;
    }

    pub(crate) async fn settle_processes_for_session(
        &self,
        session_id: &SessionId,
        wait: Duration,
    ) {
        self.process_manager
            .settle_root_session(session_id.as_str(), wait)
            .await;
    }

    /// 当前 ACN runtime 正常退出时收束其注册的所有后台 terminal。
    pub(crate) async fn shutdown_background_processes(&self) {
        self.process_manager.shutdown_all().await;
    }

    pub(super) async fn code_run(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        #[cfg(not(unix))]
        {
            let _ = (input, context);
            return Err(ToolError::InvalidArgs(
                "managed background code_run is only supported on Unix in this release".into(),
            ));
        }
        let args: CodeRunArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        if args.script.trim().is_empty() {
            return Err(ToolError::InvalidArgs("script 不能为空".into()));
        }
        if context
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(ToolError::Interrupted);
        }
        let code_type = args.r#type.unwrap_or_else(|| "bash".into());
        let cwd = resolve_tool_path(&self.workspace_root, args.cwd.as_deref().unwrap_or("."));
        let cwd_display = cwd.display().to_string();
        let (program, command_args) = self.code_run_spec(&code_type, &args.script)?;
        let mut environment = Vec::<(String, String)>::new();
        let mut removed_environment = Vec::<String>::new();
        if self.access.delegation_child {
            removed_environment.push("ACN_DELEGATION_ID".into());
            if let Some(delegation_id) = context.current_turn_id.as_deref() {
                environment.push(("ACN_SUBAGENT_ID".into(), delegation_id.to_string()));
            }
            if let Some(parent_session_id) = context.current_session_id.as_ref() {
                environment.push((
                    "ACN_PARENT_SESSION_ID".into(),
                    parent_session_id.as_str().to_string(),
                ));
            }
        }
        let owner = self.process_owner(context);
        let process = if args.tty {
            let pty_program = program.clone();
            let pty_args = command_args.clone();
            let pty_cwd = cwd.clone();
            let pty_environment = environment.clone();
            let pty_removed_environment = removed_environment.clone();
            let pty_rows = self.limits.background_process_pty_rows;
            let pty_cols = self.limits.background_process_pty_cols;
            let spawned = tokio::task::spawn_blocking(move || {
                spawn_pty(
                    &pty_program,
                    &pty_args,
                    &pty_cwd,
                    &pty_environment,
                    &pty_removed_environment,
                    pty_rows,
                    pty_cols,
                )
            })
            .await
            .map_err(|error| ToolError::InvalidArgs(format!("PTY spawn task failed: {error}")))?
            .map_err(ToolError::InvalidArgs)?;
            if context
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                terminate_and_reap_unregistered_pty(spawned).await;
                return Err(ToolError::Interrupted);
            }
            let process = match self
                .process_manager
                .reserve_with_process_group(
                    owner,
                    args.script,
                    code_type.clone(),
                    cwd_display,
                    true,
                    spawned.process_group_id,
                    context.current_turn_id.clone(),
                    context.tool_use_id.clone(),
                )
                .await
            {
                Ok(process) => process,
                Err(error) => {
                    terminate_and_reap_unregistered_pty(spawned).await;
                    return Err(ToolError::InvalidArgs(error));
                }
            };
            // cancellation 与 reservation 的最终线性化检查必须发生在 reserve 成功后、把
            // child 交给 handoff 前。若 Esc 恰好落在 reserve 等锁期间，撤回短暂 entry，
            // 仍由未登记 PTY guard 完整 kill/reap，不能误升级为后台 session。
            if context
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                self.process_manager.abort_reservation(process).await;
                terminate_and_reap_unregistered_pty(spawned).await;
                return Err(ToolError::Interrupted);
            }
            let (sender, receiver) = mpsc::channel(128);
            let input_byte_budget = Arc::new(Semaphore::new(
                self.limits.background_process_pty_input_buffer_bytes,
            ));
            // reserve 成功就是所有权交给 manager 的线性化点。handoff task 现在持有
            // PTY/child；之后本 tool future 被 Esc/Ctrl-C hard abort 也不会触发 Drop 杀掉
            // 已登记进程，只会放弃等待这个调用。
            let ready = spawn_pty_handoff(
                Arc::clone(&process),
                spawned,
                Arc::clone(&self.process_manager),
                Duration::from_millis(self.limits.background_process_output_drain_grace_ms),
                PtyHandoffIo {
                    sender,
                    input_byte_budget,
                    max_input_bytes: self.limits.background_process_pty_input_buffer_bytes,
                    receiver,
                },
            );
            ready.await.map_err(|_| {
                ToolError::InvalidArgs(
                    "PTY background process handoff task stopped unexpectedly".into(),
                )
            })?;
            process
        } else {
            let mut cmd = Command::new(&program);
            cmd.args(&command_args).current_dir(&cwd);
            if self.access.delegation_child {
                cmd.env_remove("ACN_DELEGATION_ID");
            }
            for (key, value) in &environment {
                if key != "ACN_DELEGATION_ID" {
                    cmd.env(key, value);
                }
            }
            configure_process_group(&mut cmd);
            cmd.kill_on_drop(true)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                // pipe backend 不提供文本 stdin；立即 EOF 能避免 `cat` 等命令被错误地
                // 挂成后台常驻任务，Ctrl-C 仍通过受管 PGID 发送。
                .stdin(Stdio::null());
            let child = cmd.spawn()?;
            let process_group_id = child.id().and_then(|pid| i32::try_from(pid).ok());
            let mut spawn_guard = SpawnedProcessKillGuard::new(child, process_group_id);
            if context
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(ToolError::Interrupted);
            }
            let process = self
                .process_manager
                .reserve_with_process_group(
                    owner,
                    args.script,
                    code_type.clone(),
                    cwd_display,
                    false,
                    process_group_id,
                    context.current_turn_id.clone(),
                    context.tool_use_id.clone(),
                )
                .await
                .map_err(ToolError::InvalidArgs)?;
            // 与 PTY 相同：cancellation 若在线性化登记之前发生，不能因 manager 锁竞争
            // 而把本应由 spawn guard 清理的 child 变成后台 process。
            if context
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                self.process_manager.abort_reservation(process).await;
                return Err(ToolError::Interrupted);
            }
            // 与 PTY 一样，reserve 后马上把 Child 移入独立 handoff task。不能再让
            // code_run future 持有 kill-on-drop 的 child 跨 await，否则 100ms hard abort
            // 会误杀已登记的后台任务。
            let child = spawn_guard.take_handoff_child().ok_or_else(|| {
                ToolError::InvalidArgs(
                    "pipe background process cleanup guard lost its child".into(),
                )
            })?;
            let ready = spawn_pipe_handoff(
                Arc::clone(&process),
                child,
                process_group_id,
                Arc::clone(&self.process_manager),
                Duration::from_millis(self.limits.background_process_output_drain_grace_ms),
            );
            ready.await.map_err(|_| {
                ToolError::InvalidArgs(
                    "pipe background process handoff task stopped unexpectedly".into(),
                )
            })?;
            process
        };

        let yield_time = self.clamp_yield_time(args.yield_time_ms);
        let finished = match wait_for_initial_result(
            &process,
            yield_time,
            context.cancellation.as_ref(),
        )
        .await
        {
            Ok(finished) => finished,
            Err(ToolError::Interrupted) => {
                return Err(ToolError::ProcessContinuesInBackground {
                    process_id: process.id.as_str().to_string(),
                });
            }
            Err(error) => return Err(error),
        };
        // initial yield 正常返回与 Esc/Ctrl-C 可以同一时刻 ready。登记后的进程寿命已
        // 独立于 tool call；取消优先让 turn 收束为 Interrupted，不能再构造 Completed/Running。
        if context
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(ToolError::ProcessContinuesInBackground {
                process_id: process.id.as_str().to_string(),
            });
        }
        let max_output_chars = self.clamp_output_chars(args.max_output_chars);
        let execution = self
            .process_execution(Arc::clone(&process), &code_type, max_output_chars, finished)
            .await?;
        if context
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(ToolError::ProcessContinuesInBackground {
                process_id: process.id.as_str().to_string(),
            });
        }
        Ok(execution)
    }

    pub(super) async fn write_stdin(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let args: WriteStdinArgs = serde_json::from_value(input)
            .map_err(|error| ToolError::InvalidArgs(error.to_string()))?;
        if args.stdout_cursor.is_some() != args.stderr_cursor.is_some() {
            return Err(ToolError::InvalidArgs(
                "stdout_cursor and stderr_cursor must be supplied together".into(),
            ));
        }
        let owner = self.process_owner(context);
        let own_process = self
            .process_manager
            .find_for_owner(&owner, &args.process_id)
            .await;
        let (process, cross_owner) = match own_process {
            Some(process) => (process, false),
            None if !self.access.delegation_child => self
                .process_manager
                .find_live_for_root(&owner, &args.process_id)
                .await
                .map(|process| (process, true))
                .ok_or_else(|| {
                    ToolError::InvalidArgs(
                        "process_id is not a live process owned by this agent or in this root session"
                            .into(),
                    )
                })?,
            None => {
                return Err(ToolError::InvalidArgs(
                    "process_id is not a live process owned by this subagent".into(),
                ));
            }
        };
        let chars = match args.chars.unwrap_or_default().as_str() {
            // 有些 provider 会把 tool JSON 中的 `\u0003` 再转义一次，最终传来六个
            // 可见字符而非 U+0003。它仍是 Ctrl-C 的同一 JSON 表示，统一成控制字符后
            // 再走下面的所有权与 pipe/PTY 校验；不能借此放开其他控制序列。
            r"\u0003" => "\u{3}".to_string(),
            other => other.to_string(),
        };
        if args.terminate && !chars.is_empty() {
            return Err(ToolError::InvalidArgs(
                "terminate=true must not be combined with non-empty chars".into(),
            ));
        }
        if !cross_owner && process.has_uncommitted_output_delivery().await {
            return Err(ToolError::InvalidArgs(
                "process output already has a page awaiting provider delivery; retry write_stdin after the next provider response".into(),
            ));
        }
        let is_poll = chars.is_empty() && !args.terminate;
        if !process.state().await.is_live() && (!chars.is_empty() || args.terminate) {
            return Err(ToolError::InvalidArgs(
                "process has already exited; only an empty poll can read its final output".into(),
            ));
        }
        if args.terminate {
            let _ = self
                .process_manager
                .terminate_live_for_root_matching(
                    &process.owner.root_session_id,
                    process.id.as_str(),
                    &process.owner,
                    process.instance_id,
                    libc::SIGKILL,
                )
                .await
                .map_err(ToolError::InvalidArgs)?;
        } else if !chars.is_empty() {
            if cross_owner && chars != "\u{3}" {
                return Err(ToolError::InvalidArgs(
                    "a main agent may only poll, send Ctrl-C (\\u0003), or use terminate=true on a subagent-owned process"
                        .into(),
                ));
            }
            if !process.tty && chars != "\u{3}" {
                return Err(ToolError::InvalidArgs(
                    "pipe-backed process only accepts an empty poll or Ctrl-C (\\u0003)".into(),
                ));
            }
            if chars == "\u{3}" && !process.tty {
                process
                    .request_interrupt(libc::SIGINT)
                    .await
                    .map_err(ToolError::InvalidArgs)?;
            } else {
                process
                    .write(chars.into_bytes())
                    .await
                    .map_err(ToolError::InvalidArgs)?;
            }
        }
        let yield_time = self.clamp_write_yield_time(args.yield_time_ms, is_poll);
        let _finished =
            wait_for_initial_result(&process, yield_time, context.cancellation.as_ref()).await?;
        let max_output_chars = self.clamp_output_chars(args.max_output_chars);
        // parent 对 subagent 的观察不能推进 child 交付给其自身 provider 的 cursor。未提供
        // cursor 时从 retained buffer 起点返回快照；模型若只需增量可带回本次返回的 cursor pair。
        let (track_delivery, stdout_cursor, stderr_cursor) = if cross_owner {
            (
                false,
                Some(OutputCursor(args.stdout_cursor.unwrap_or_default())),
                Some(OutputCursor(args.stderr_cursor.unwrap_or_default())),
            )
        } else {
            (
                true,
                args.stdout_cursor.map(OutputCursor),
                args.stderr_cursor.map(OutputCursor),
            )
        };
        let execution = self
            .process_execution_with_cursors(
                process,
                max_output_chars,
                ProcessExecutionDelivery {
                    track: track_delivery,
                    allow_terminal: !cross_owner,
                    termination_requested: args.terminate,
                },
                stdout_cursor,
                stderr_cursor,
            )
            .await?;
        Ok(execution)
    }

    pub(super) async fn process_list(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let _: ProcessListArgs = serde_json::from_value(input)
            .map_err(|error| ToolError::InvalidArgs(error.to_string()))?;
        let owner = self.process_owner(context);
        let entries = if self.access.delegation_child {
            self.process_manager.live_for_owner(&owner).await
        } else {
            self.process_manager
                .live_for_root(&owner.root_session_id)
                .await
                .into_iter()
                .filter(|entry| entry.owner.owner_agent_id == owner.owner_agent_id)
                .collect()
        };
        let mut processes = Vec::with_capacity(entries.len());
        for entry in entries {
            let state = entry.state().await;
            processes.push(json!({
                "process_id": entry.id.as_str(),
                "owner": entry.owner.subagent_id.as_deref().unwrap_or("main"),
                "state": state.label(),
                "status": state.label(),
                "tty": entry.tty,
                "started_at": entry.started_at.duration_since(std::time::UNIX_EPOCH).ok().map(|duration| duration.as_secs()),
                "elapsed_ms": entry.started_at.elapsed().map(|duration| duration.as_millis()).unwrap_or_default(),
                "command": entry.command,
                "cwd": entry.cwd,
            }));
        }
        Ok(ToolExecution::completed(json!({ "processes": processes })))
    }

    pub(super) fn process_owner(&self, context: &ToolDispatchContext) -> ProcessOwner {
        let root_session_id = context
            .current_session_id
            .as_ref()
            .map(|session_id| session_id.as_str().to_string())
            .unwrap_or_else(|| "ephemeral-tool-session".into());
        if self.access.delegation_child {
            let subagent_id = context
                .current_turn_id
                .clone()
                .unwrap_or_else(|| "ephemeral-subagent".into());
            ProcessOwner::subagent_for_agent(
                self.process_owner_agent_id.clone(),
                root_session_id,
                subagent_id,
            )
        } else {
            ProcessOwner::main_for_agent(self.process_owner_agent_id.clone(), root_session_id)
        }
    }

    pub(super) fn process_owner_for_session(
        &self,
        session_id: &SessionId,
        subagent_id: Option<&str>,
    ) -> ProcessOwner {
        match subagent_id {
            Some(subagent_id) => ProcessOwner::subagent_for_agent(
                self.process_owner_agent_id.clone(),
                session_id.as_str(),
                subagent_id,
            ),
            None => ProcessOwner::main_for_agent(
                self.process_owner_agent_id.clone(),
                session_id.as_str(),
            ),
        }
    }

    /// explicit cancel 的强制 abort 发生在 tool future 被 drop 之后；通过创建时记录的
    /// tool_use 精确找回仍受管的后台进程，供 turn loop 生成 continuation 提示。
    pub(crate) async fn live_process_ids_for_tool_use(
        &self,
        context: &ToolDispatchContext,
    ) -> Vec<String> {
        let Some(tool_use_id) = context.tool_use_id.as_deref() else {
            return Vec::new();
        };
        let owner = self.process_owner(context);
        self.process_manager
            .live_ids_for_owner_and_tool_use(
                &owner,
                context.current_turn_id.as_deref(),
                tool_use_id,
            )
            .await
    }

    pub(super) fn clamp_yield_time(&self, requested: Option<u64>) -> Duration {
        let milliseconds = requested
            .unwrap_or(self.limits.code_run_initial_yield_ms)
            .clamp(
                self.limits.code_run_min_yield_ms,
                self.limits.code_run_max_yield_ms,
            );
        Duration::from_millis(milliseconds)
    }

    pub(super) fn clamp_write_yield_time(&self, requested: Option<u64>, is_poll: bool) -> Duration {
        let default = if is_poll {
            self.limits.code_run_poll_yield_ms
        } else {
            self.limits.code_run_write_yield_ms
        };
        let max = if is_poll {
            self.limits.write_stdin_max_poll_timeout_ms
        } else {
            self.limits.code_run_max_yield_ms
        };
        Duration::from_millis(
            requested
                .unwrap_or(default)
                .clamp(self.limits.code_run_min_yield_ms, max),
        )
    }

    pub(super) fn clamp_output_chars(&self, requested: Option<usize>) -> usize {
        requested
            .unwrap_or(self.limits.code_run_max_output_chars)
            .clamp(1, self.limits.code_run_max_output_chars)
    }

    pub(super) async fn process_execution(
        &self,
        process: Arc<ManagedProcess>,
        code_type: &str,
        max_output_chars: usize,
        finished: bool,
    ) -> Result<ToolExecution, ToolError> {
        self.process_execution_with_cursors(
            process,
            max_output_chars,
            ProcessExecutionDelivery {
                track: !finished,
                allow_terminal: true,
                termination_requested: false,
            },
            None,
            None,
        )
        .await
        .map(|mut execution| {
            if let Some(object) = execution.output.as_object_mut() {
                object.insert("type".into(), Value::String(code_type.to_string()));
            }
            execution
        })
    }

    async fn process_execution_with_cursors(
        &self,
        process: Arc<ManagedProcess>,
        max_output_chars: usize,
        delivery: ProcessExecutionDelivery,
        stdout_cursor: Option<OutputCursor>,
        stderr_cursor: Option<OutputCursor>,
    ) -> Result<ToolExecution, ToolError> {
        let implicit_cursor = stdout_cursor.is_none() && stderr_cursor.is_none();
        let requested_stdout_cursor = stdout_cursor;
        let requested_stderr_cursor = stderr_cursor;
        let state_before_snapshot = process.state().await;
        // 已退出 entry 的最终输出必须从 committed delivery cursor 开始，不允许调用方用
        // 过去的显式 cursor 跳过模型尚未确认看到的字节后再提交删除 entry。
        let mut snapshot_start = if state_before_snapshot.is_live() {
            match (stdout_cursor, stderr_cursor) {
                (Some(stdout_cursor), Some(stderr_cursor)) => (stdout_cursor, stderr_cursor),
                _ if implicit_cursor => process.output_delivery_cursors().await,
                _ => {
                    return Err(ToolError::InvalidArgs(
                        "stdout_cursor and stderr_cursor must be supplied together".into(),
                    ));
                }
            }
        } else {
            process.output_delivery_cursors().await
        };
        let (mut stdout_snapshot, mut stderr_snapshot) = process
            .output_since(snapshot_start.0, snapshot_start.1)
            .await;
        let mut state = process.state().await;
        // watcher 在 mark_finished 前保证 output drain 已结束。若刚好跨越 live →
        // terminal，重新从 committed delivery cursor 取快照，避免用显式 cursor 跳过
        // 未交付尾部后提交删除 entry。
        if state_before_snapshot.is_live() && !state.is_live() {
            snapshot_start = process.output_delivery_cursors().await;
            (stdout_snapshot, stderr_snapshot) = process
                .output_since(snapshot_start.0, snapshot_start.1)
                .await;
            state = process.state().await;
        }
        let stdout = truncate_chars(
            &String::from_utf8_lossy(&stdout_snapshot.bytes),
            max_output_chars,
        );
        let stderr = truncate_chars(
            &String::from_utf8_lossy(&stderr_snapshot.bytes),
            max_output_chars,
        );
        let stdin_open = process.stdin_open().await;
        let elapsed_ms = process
            .started_at
            .elapsed()
            .map(|duration| duration.as_millis())
            .unwrap_or_default();
        let (exit_code, signal, success) = match state {
            ProcessState::Finished {
                exit_code,
                signal,
                success,
            } => (exit_code, signal, success),
            ProcessState::Terminated { exit_code, signal } => (exit_code, signal, false),
            ProcessState::Error => (None, None, false),
            ProcessState::Starting | ProcessState::Running | ProcessState::Terminating => {
                (None, None, true)
            }
        };
        let running = state.is_live();
        // future cursor 的请求并不对应任何已交付的数据；它和本次长度截断一样，必须
        // 明确反馈给模型且绝不能推进 delivery cursor。
        let explicit_cursor_out_of_range = state.is_live()
            && (requested_stdout_cursor.is_some_and(|cursor| cursor > stdout_snapshot.cursor)
                || requested_stderr_cursor.is_some_and(|cursor| cursor > stderr_snapshot.cursor));
        let stdout_cursor_out_of_range = state.is_live()
            && requested_stdout_cursor.is_some_and(|cursor| cursor > stdout_snapshot.cursor);
        let stderr_cursor_out_of_range = state.is_live()
            && requested_stderr_cursor.is_some_and(|cursor| cursor > stderr_snapshot.cursor);
        // tool-result 层只展示 snapshot 的前缀时，游标推进到“已经展示的前缀末尾”。
        // 该游标仍通过 provider-success receipt 两阶段提交；provider 失败会回滚，因此
        // 下一轮既不会跳过未见内容，也不会在成功交付后无限重放同一前缀。
        let (stdout_snapshot_start, stderr_snapshot_start) = snapshot_start;
        let stdout_visible_chars = u64::try_from(stdout.0.chars().count()).unwrap_or(u64::MAX);
        let stderr_visible_chars = u64::try_from(stderr.0.chars().count()).unwrap_or(u64::MAX);
        let stdout_page_is_contiguous = stdout_snapshot.page_contiguous;
        let stderr_page_is_contiguous = stderr_snapshot.page_contiguous;
        let reported_stdout_cursor = if stdout.1 && stdout_page_is_contiguous {
            OutputCursor(
                stdout_snapshot_start
                    .0
                    .saturating_add(stdout_visible_chars)
                    .min(stdout_snapshot.cursor.0),
            )
        } else if stdout.1 {
            stdout_snapshot_start
        } else if stdout_cursor_out_of_range {
            OutputCursor(0)
        } else {
            stdout_snapshot.cursor
        };
        let reported_stderr_cursor = if stderr.1 && stderr_page_is_contiguous {
            OutputCursor(
                stderr_snapshot_start
                    .0
                    .saturating_add(stderr_visible_chars)
                    .min(stderr_snapshot.cursor.0),
            )
        } else if stderr.1 {
            stderr_snapshot_start
        } else if stderr_cursor_out_of_range {
            OutputCursor(0)
        } else {
            stderr_snapshot.cursor
        };
        let mut output = json!({
            "state": state.label(),
            "stdin_open": stdin_open,
            "tty": process.tty,
            "exit_code": exit_code,
            "signal": signal,
            "success": success,
            "stdout": stdout.0,
            "stderr": stderr.0,
            "stdout_cursor": reported_stdout_cursor.0,
            "stderr_cursor": reported_stderr_cursor.0,
            "chunk_id": format!("{}:{}", process.id.as_str(), elapsed_ms),
            "wall_time_ms": elapsed_ms,
            "truncated": stdout.1 || stderr.1 || stdout_snapshot.truncated || stderr_snapshot.truncated || explicit_cursor_out_of_range,
            "omitted_bytes": stdout_snapshot.omitted_bytes.saturating_add(stderr_snapshot.omitted_bytes),
        });
        // 终态但截断的初始结果也必须给出 logical id：它不出现在 live-only
        // process_list 中，模型只能用这个 id 通过 write_stdin 取完整结果。
        if running || output["truncated"].as_bool().unwrap_or(true) {
            if let Some(object) = output.as_object_mut() {
                object.insert(
                    "process_id".into(),
                    Value::String(process.id.as_str().to_string()),
                );
            }
        }
        let outcome = if running {
            ToolExecutionOutcome::ProcessRunning
        } else if delivery.termination_requested && matches!(state, ProcessState::Terminated { .. })
        {
            ToolExecutionOutcome::ProcessTerminated { signal }
        } else {
            ToolExecutionOutcome::ProcessExit { exit_code, success }
        };
        let mut execution = ToolExecution::new(output, outcome);
        // snapshot_since 会把 retained head、不可恢复 gap 与 retained tail 拆成独立连续页；
        // 局部截断只提交当前页已经展示的前缀。非连续页或 future cursor 绝不能提交。
        let locally_truncated = stdout.1 || stderr.1 || explicit_cursor_out_of_range;
        let unsafe_local_truncation =
            (stdout.1 && !stdout_page_is_contiguous) || (stderr.1 && !stderr_page_is_contiguous);
        // owner 的终态读取必须生成 provider-success receipt，否则模型虽已收到 final result 却永远
        // 无法完成消费。main 对 child 的跨 owner 观察显式关闭该 receipt，不能改动 child delivery。
        if ((delivery.track && implicit_cursor) || (delivery.allow_terminal && !running))
            && !explicit_cursor_out_of_range
            && !unsafe_local_truncation
        {
            let final_result = !running
                && !locally_truncated
                && !stdout_snapshot.has_more_retained
                && !stderr_snapshot.has_more_retained;
            execution.process_delivery_receipt = Some(
                process
                    .prepare_output_delivery(
                        reported_stdout_cursor,
                        reported_stderr_cursor,
                        final_result,
                    )
                    .await
                    .ok_or_else(|| {
                        ToolError::InvalidArgs(
                            "process output already has a page awaiting provider delivery; retry write_stdin after the next provider response".into(),
                        )
                    })?,
            );
        }
        Ok(execution)
    }

    pub(super) async fn response_text_bounded(
        &self,
        mut resp: reqwest::Response,
        max_chars: usize,
    ) -> Result<(String, bool), ToolError> {
        let byte_limit = bounded_text_byte_limit(max_chars);
        let mut bytes = Vec::new();
        let mut response_truncated = false;
        while let Some(chunk) = resp.chunk().await? {
            let remaining = byte_limit.saturating_sub(bytes.len());
            if chunk.len() > remaining {
                bytes.extend_from_slice(&chunk[..remaining]);
                response_truncated = true;
                break;
            }
            bytes.extend_from_slice(&chunk);
            if bytes.len() >= byte_limit {
                response_truncated = true;
                break;
            }
        }
        let text = String::from_utf8_lossy(&bytes);
        let (text, text_truncated) = truncate_chars(&text, max_chars);
        Ok((text, response_truncated || text_truncated))
    }

    pub(super) fn code_run_spec(
        &self,
        code_type: &str,
        script: &str,
    ) -> Result<(String, Vec<String>), ToolError> {
        let (program, args): (&str, Vec<String>) = match code_type {
            "bash" => ("bash", vec!["-lc".into(), script.into()]),
            "python" => ("python3", vec!["-c".into(), script.into()]),
            "powershell" => ("pwsh", vec!["-Command".into(), script.into()]),
            other => {
                return Err(ToolError::InvalidArgs(format!(
                    "不支持的 code_run type: {other}"
                )));
            }
        };
        Ok((program.into(), args))
    }
}

fn semantic_process_state_label(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Starting => "starting",
        ProcessState::Running => "running",
        ProcessState::Terminating => "terminating",
        ProcessState::Finished { .. } => "finished",
        ProcessState::Terminated { .. } => "terminated",
        ProcessState::Error => "error",
    }
}

/// Spawn 前注册还没有完成时的本地 cleanup guard。登记成功后 watcher 接管 child 生命周期。
pub(super) struct SpawnedProcessKillGuard {
    process_group_id: Option<i32>,
    child: Option<tokio::process::Child>,
}

impl SpawnedProcessKillGuard {
    pub(super) fn new(child: tokio::process::Child, process_group_id: Option<i32>) -> Self {
        Self {
            process_group_id,
            child: Some(child),
        }
    }

    /// reserve 成功后把唯一的 `Child` 交给 detached watcher；此后 guard 不再负责清理。
    fn take_handoff_child(&mut self) -> Option<tokio::process::Child> {
        self.process_group_id = None;
        self.child.take()
    }
}

impl Drop for SpawnedProcessKillGuard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let process_group_id = self.process_group_id;
        // runtime shutdown 时 `Handle::spawn` 接收的 future 可能永远没有机会 poll；先同步
        // killpg，保证独立组的全部后代在 Drop 返回前已经收到终止请求。
        if let Some(process_group_id) = process_group_id {
            let _ = terminate_process_group(process_group_id, libc::SIGKILL);
        }
        let child_process_id = child.id().and_then(|pid| i32::try_from(pid).ok());
        if let Some(child_process_id) = child_process_id {
            if let Err(error) = spawn_direct_child_reaper(child_process_id) {
                log::warn!(
                    target: "tool",
                    "failed to start detached pipe child reaper for {child_process_id}: {error}; reaping inline"
                );
                // thread 已确认创建失败，且 killpg 已同步发送。不能把回收交给可能正在
                // shutdown 的 Tokio runtime；inline waitpid 是避免 zombie 的最后兜底。
                reap_direct_child_blocking(child_process_id);
                drop(child);
                return;
            } else {
                // waitpid worker 已成为唯一 reaper；drop Child 不会重复 wait。
                drop(child);
                return;
            }
        }
        // 无法取得 PID 或创建独立 worker 的极少数退化路径，仍尽力在当前 runtime 回收。
        let cleanup = async move {
            let _ = child.wait().await;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(cleanup);
        }
    }
}

/// PTY 已经 spawn 但尚未登记给 ProcessManager 时的失败收束。
///
/// 此时不能只向 PGID 发信号后 drop `Child`：portable-pty 的 child handle 不会自动 reap，
/// 需要在 blocking worker 中等待，避免高频取消留下 zombie。
async fn terminate_and_reap_unregistered_pty(spawned: PtySpawned) {
    let Some((mut child, process_group_id)) = spawned.into_unregistered_child() else {
        return;
    };
    let result = tokio::task::spawn_blocking(move || {
        if let Some(process_group_id) = process_group_id {
            let _ = terminate_process_group(process_group_id, libc::SIGKILL);
        }
        let _ = child.wait();
    })
    .await;
    if let Err(error) = result {
        log::warn!(
            target: "tool",
            "unregistered PTY cleanup worker join failed: {error}"
        );
    }
}

/// root 退出后必须在真正 reap 前清理同组后代。WNOWAIT 让 root 保持 zombie，从而保留
/// 原 PGID，避免 `kill(-old_pgid)` 落到极端 PID reuse 后的无关进程组。
async fn observe_root_exit_without_reap(root_pid: Option<i32>) -> bool {
    let Some(root_pid) = root_pid else {
        log::warn!(
            target: "tool",
            "managed process child did not expose a root PID; skipping residual process-group cleanup"
        );
        return false;
    };
    match tokio::task::spawn_blocking(move || wait_for_child_exit_without_reap(root_pid)).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            log::warn!(
                target: "tool",
                "failed to observe root child {root_pid} before reap; skipping residual process-group cleanup: {error}"
            );
            false
        }
        Err(error) => {
            log::warn!(
                target: "tool",
                "root exit observer worker for child {root_pid} failed; skipping residual process-group cleanup: {error}"
            );
            false
        }
    }
}

fn spawn_pipe_watcher(
    process: Arc<ManagedProcess>,
    mut child: tokio::process::Child,
    manager: Arc<ProcessManager>,
    output_drain_grace: Duration,
) {
    let root_pid = child.id().and_then(|pid| i32::try_from(pid).ok());
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_process = Arc::clone(&process);
    let stdout_manager = Arc::clone(&manager);
    let stdout_task = tokio::spawn(async move {
        drain_pipe_to_process(stdout, stdout_process, stdout_manager, true).await;
    });
    let stderr_process = Arc::clone(&process);
    let stderr_manager = Arc::clone(&manager);
    let stderr_task = tokio::spawn(async move {
        drain_pipe_to_process(stderr, stderr_process, stderr_manager, false).await;
    });
    tokio::spawn(async move {
        let root_exit_observed = observe_root_exit_without_reap(root_pid).await;
        if root_exit_observed {
            let _ = process.kill_remaining_process_group().await;
        }
        let root_reap_gate = process.acquire_root_reap_gate().await;
        let child_status = child.wait().await;
        if let Ok(status) = &child_status {
            process
                .record_observed_root_exit(status.code(), exit_signal(status), status.success())
                .await;
        }
        process.retire_process_group_after_root_reap().await;
        drop(root_reap_gate);
        match child_status {
            Ok(status) => {
                let mut stdout_task = stdout_task;
                let mut stderr_task = stderr_task;
                let drained = tokio::time::timeout(output_drain_grace, async {
                    let _ = (&mut stdout_task).await;
                    let _ = (&mut stderr_task).await;
                })
                .await;
                if drained.is_err() {
                    // `setsid` 等 escape 后代可能继续持有 pipe fd；它已不属于受管 PGID，
                    // 但不能因此阻塞 root terminal 的终态推进。
                    stdout_task.abort();
                    stderr_task.abort();
                    process.mark_output_incomplete().await;
                } else {
                    process.finish_output().await;
                }
                process
                    .mark_finished(status.code(), exit_signal(&status), status.success())
                    .await;
                manager.record_state_changed(&process).await;
                manager.record_completion(&process).await;
            }
            Err(error) => {
                let mut stdout_task = stdout_task;
                let mut stderr_task = stderr_task;
                let drained = tokio::time::timeout(output_drain_grace, async {
                    let _ = (&mut stdout_task).await;
                    let _ = (&mut stderr_task).await;
                })
                .await;
                if drained.is_err() {
                    stdout_task.abort();
                    stderr_task.abort();
                    process.mark_output_incomplete().await;
                } else {
                    process.finish_output().await;
                }
                process
                    .mark_error(&format!("process wait failed: {error}\n"))
                    .await;
                manager.record_state_changed(&process).await;
                manager.record_completion(&process).await;
            }
        }
    });
}

/// reserve 成功后，将 pipe child 的完整生命周期移出可被 hard-abort 的 tool future。
/// ready 仅用于维持原先“返回 process_id 前已可管理”的语义；caller 被取消不会取消这里。
fn spawn_pipe_handoff(
    process: Arc<ManagedProcess>,
    child: tokio::process::Child,
    process_group_id: Option<i32>,
    manager: Arc<ProcessManager>,
    output_drain_grace: Duration,
) -> oneshot::Receiver<()> {
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(async move {
        #[cfg(test)]
        manager.wait_for_test_handoff_gate().await;
        process.attach_pipe(process_group_id).await;
        process.mark_running().await;
        manager.record_started(&process).await;
        manager.record_state_changed(&process).await;
        spawn_pipe_watcher(process, child, manager, output_drain_grace);
        let _ = ready_tx.send(());
    });
    ready_rx
}

/// PTY 的对应 handoff；PTY 资源在这个 detached task 中持有，避免 hard abort 的 future
/// drop 触发 `PtySpawned` cleanup。
struct PtyHandoffIo {
    sender: mpsc::Sender<PtyInput>,
    input_byte_budget: Arc<Semaphore>,
    max_input_bytes: usize,
    receiver: mpsc::Receiver<PtyInput>,
}

fn spawn_pty_handoff(
    process: Arc<ManagedProcess>,
    spawned: PtySpawned,
    manager: Arc<ProcessManager>,
    output_drain_grace: Duration,
    io: PtyHandoffIo,
) -> oneshot::Receiver<()> {
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(async move {
        #[cfg(test)]
        manager.wait_for_test_handoff_gate().await;
        let process_group_id = spawned.process_group_id;
        // PGID 必须先挂到 entry，再启动 watcher：极快退出的 root 也能由 watcher
        // 对同组后代执行 cleanup，不能观察到 None 后错过这次清理。
        process
            .attach_pty(
                io.sender,
                io.input_byte_budget,
                io.max_input_bytes,
                process_group_id,
            )
            .await;
        process.mark_running().await;
        manager.record_started(&process).await;
        manager.record_state_changed(&process).await;
        spawn_pty_watcher(process, spawned, manager, output_drain_grace, io.receiver);
        let _ = ready_tx.send(());
    });
    ready_rx
}

/// PTY 的 reader、writer 和 wait 都在 blocking worker 中运行；Tokio 侧只做 channel 转发和状态更新。
fn spawn_pty_watcher(
    process: Arc<ManagedProcess>,
    spawned: PtySpawned,
    manager: Arc<ProcessManager>,
    output_drain_grace: Duration,
    mut writer_rx: mpsc::Receiver<PtyInput>,
) {
    let Some(PtyWatcherParts {
        mut child,
        master,
        reader,
        writer,
        io_stop,
        process_group_id,
    }) = spawned.into_watcher_parts()
    else {
        // spawn_pty 成功返回的对象始终完整；若未来底层实现违背该假设，保留 process
        // reservation 反而会泄漏 Starting entry，因此立即交给 manager 收束。
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            manager.abort_reservation(process).await;
        });
        return;
    };
    let root_pid = child.process_id().and_then(|pid| i32::try_from(pid).ok());
    let mut writer = writer;
    let writer_stop = Arc::clone(&io_stop);
    let (io_failure_tx, mut io_failure_rx) = mpsc::unbounded_channel::<String>();
    let writer_failure_tx = io_failure_tx.clone();
    let writer_task = tokio::task::spawn_blocking(move || {
        use std::io::Write;

        while let Some(input) = writer_rx.blocking_recv() {
            let bytes = input.bytes;
            let mut written = 0usize;
            while written < bytes.len() {
                if writer_stop.load(Ordering::Acquire) {
                    return;
                }
                match writer.write(&bytes[written..]) {
                    Ok(0) => {
                        let _ = writer_failure_tx
                            .send("PTY writer closed before accepting input".into());
                        return;
                    }
                    Ok(count) => written = written.saturating_add(count),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => {
                        let _ = writer_failure_tx.send(format!("PTY writer failed: {error}"));
                        return;
                    }
                }
            }
            loop {
                if writer_stop.load(Ordering::Acquire) {
                    return;
                }
                match writer.flush() {
                    Ok(()) => break,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => {
                        let _ = writer_failure_tx.send(format!("PTY writer flush failed: {error}"));
                        return;
                    }
                }
            }
        }
    });

    let (output_tx, mut output_rx) = mpsc::channel::<Vec<u8>>(128);
    let mut reader = reader;
    let reader_stop = Arc::clone(&io_stop);
    let reader_failure_tx = io_failure_tx.clone();
    let reader_task = tokio::task::spawn_blocking(move || {
        use std::io::Read;

        let mut bytes = [0_u8; 8192];
        while !reader_stop.load(Ordering::Acquire) {
            match reader.read(&mut bytes) {
                Ok(0) => break,
                Ok(read) => {
                    let mut pending = bytes[..read].to_vec();
                    loop {
                        if reader_stop.load(Ordering::Acquire) {
                            return;
                        }
                        match output_tx.try_send(pending) {
                            Ok(()) => break,
                            Err(mpsc::error::TrySendError::Full(bytes)) => {
                                pending = bytes;
                                std::thread::sleep(Duration::from_millis(5));
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                // macOS/Linux PTY master returns EIO when the slave side closes; that is EOF.
                Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                Err(error) => {
                    let _ = reader_failure_tx.send(format!("PTY reader failed: {error}"));
                    break;
                }
            }
        }
    });
    drop(io_failure_tx);
    let io_failure_process = Arc::clone(&process);
    let io_failure_stop = Arc::clone(&io_stop);
    tokio::spawn(async move {
        if let Some(message) = io_failure_rx.recv().await {
            io_failure_stop.store(true, Ordering::Release);
            io_failure_process.handle_io_failure(&message).await;
        }
    });
    let output_process = Arc::clone(&process);
    let output_manager = Arc::clone(&manager);
    let output_task = tokio::spawn(async move {
        while let Some(bytes) = output_rx.recv().await {
            output_process.append_stdout(&bytes).await;
            output_manager.record_output(&output_process).await;
        }
    });
    tokio::spawn(async move {
        // master 必须一直持有到 root terminal 完成，不能在创建 reader/writer 后提前 drop。
        let master = master;
        let root_exit_observed = observe_root_exit_without_reap(root_pid).await;
        if root_exit_observed {
            let _ = process.kill_remaining_process_group().await;
        }
        let root_reap_gate = process.acquire_root_reap_gate().await;
        let waited = tokio::task::spawn_blocking(move || child.wait()).await;
        if let Ok(Ok(status)) = &waited {
            let signal = status.signal().and_then(pty_signal_number);
            let exit_code = if signal.is_some() {
                None
            } else {
                i32::try_from(status.exit_code()).ok()
            };
            process
                .record_observed_root_exit(exit_code, signal, status.success())
                .await;
        }
        process.retire_process_group_after_root_reap().await;
        drop(root_reap_gate);
        // root 已终态后主动关 master，确保 reader 不会被逃离 terminal 的后代无限挂住。
        drop(master);
        match waited {
            Ok(Ok(status)) => {
                process.close_stdin().await;
                if settle_pty_io_tasks(
                    &io_stop,
                    reader_task,
                    output_task,
                    writer_task,
                    output_drain_grace,
                )
                .await
                {
                    process.mark_output_incomplete().await;
                } else {
                    process.finish_output().await;
                }
                let signal = status.signal().and_then(pty_signal_number);
                let exit_code = if signal.is_some() {
                    None
                } else {
                    i32::try_from(status.exit_code()).ok()
                };
                process
                    .mark_finished(exit_code, signal, status.success())
                    .await;
                manager.record_state_changed(&process).await;
                manager.record_completion(&process).await;
            }
            Ok(Err(error)) => {
                process.close_stdin().await;
                if settle_pty_io_tasks(
                    &io_stop,
                    reader_task,
                    output_task,
                    writer_task,
                    output_drain_grace,
                )
                .await
                {
                    process.mark_output_incomplete().await;
                } else {
                    process.finish_output().await;
                }
                process
                    .mark_error(&format!("PTY process wait failed: {error}\n"))
                    .await;
                manager.record_state_changed(&process).await;
                manager.record_completion(&process).await;
            }
            Err(error) => {
                if let Some(process_group_id) = process_group_id {
                    let _ = process.request_terminate(libc::SIGKILL).await;
                    log::warn!(
                        target: "tool",
                        "PTY watcher join failed for process group {process_group_id}: {error}"
                    );
                }
                process.close_stdin().await;
                if settle_pty_io_tasks(
                    &io_stop,
                    reader_task,
                    output_task,
                    writer_task,
                    output_drain_grace,
                )
                .await
                {
                    process.mark_output_incomplete().await;
                } else {
                    process.finish_output().await;
                }
                process
                    .mark_error(&format!("PTY watcher join failed: {error}\n"))
                    .await;
                manager.record_state_changed(&process).await;
                manager.record_completion(&process).await;
            }
        }
    });
}

/// PTY child 退出后先给 reader 留出 drain window；若 escape 后代仍持有 slave，则通过
/// 非阻塞 fd 的 stop flag 让 blocking workers 自行退出，而不是 abort 无法中断的 worker。
async fn settle_pty_io_tasks(
    io_stop: &std::sync::atomic::AtomicBool,
    mut reader_task: tokio::task::JoinHandle<()>,
    mut output_task: tokio::task::JoinHandle<()>,
    mut writer_task: tokio::task::JoinHandle<()>,
    output_drain_grace: Duration,
) -> bool {
    let drained = tokio::time::timeout(output_drain_grace, async {
        let _ = (&mut reader_task).await;
        let _ = (&mut output_task).await;
        let _ = (&mut writer_task).await;
    })
    .await;
    if drained.is_ok() {
        return false;
    }
    io_stop.store(true, Ordering::Release);
    let stopped = tokio::time::timeout(Duration::from_millis(250), async {
        let _ = (&mut reader_task).await;
        let _ = (&mut output_task).await;
        let _ = (&mut writer_task).await;
    })
    .await;
    if stopped.is_err() {
        log::warn!(
            target: "tool",
            "PTY I/O workers did not settle after stop request; workers were detached"
        );
    }
    true
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;

    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

/// portable-pty 将 Unix signal 序列化为本地描述字符串；只接受我们实际会发送的
/// 标准信号，未知值保守地不伪造数字。
fn pty_signal_number(signal: &str) -> Option<i32> {
    let normalized = signal.to_ascii_lowercase();
    if normalized.contains("interrupt") {
        Some(libc::SIGINT)
    } else if normalized.contains("terminated") {
        Some(libc::SIGTERM)
    } else if normalized.contains("killed") {
        Some(libc::SIGKILL)
    } else if normalized.contains("hangup") {
        Some(libc::SIGHUP)
    } else if normalized.contains("quit") {
        Some(libc::SIGQUIT)
    } else {
        None
    }
}

async fn drain_pipe_to_process<R>(
    pipe: Option<R>,
    process: Arc<ManagedProcess>,
    manager: Arc<ProcessManager>,
    stdout: bool,
) where
    R: AsyncRead + Unpin,
{
    let Some(mut pipe) = pipe else {
        return;
    };
    let mut chunk = [0_u8; 8192];
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) if stdout => {
                process.append_stdout(&chunk[..read]).await;
                manager.record_output(&process).await;
            }
            Ok(read) => {
                process.append_stderr(&chunk[..read]).await;
                manager.record_output(&process).await;
            }
            Err(error) => {
                let stream = if stdout { "stdout" } else { "stderr" };
                process
                    .handle_io_failure(&format!("pipe {stream} reader failed: {error}"))
                    .await;
                break;
            }
        }
    }
}

async fn wait_for_initial_result(
    process: &ManagedProcess,
    yield_time: Duration,
    cancellation: Option<&CancellationToken>,
) -> Result<bool, ToolError> {
    if let Some(cancellation) = cancellation {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ToolError::Interrupted),
            finished = process.wait_for_terminal(yield_time) => Ok(finished),
        }
    } else {
        Ok(process.wait_for_terminal(yield_time).await)
    }
}
