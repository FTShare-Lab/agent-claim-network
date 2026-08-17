//! 文件分页读取、分级修改许可与路径级并发控制。

use super::*;
use crate::tool::file_text::{
    migrate_edit_coverage, read_text_page, suggested_read_range, CoverageMigration,
    TextPageOutcome, TextPageRequest,
};

impl ToolRegistry {
    /// resume 同一 session 时撤销其旧进程内文件修改许可。
    pub async fn clear_file_read_state(&self, session_id: &SessionId) {
        if !self.file_edit_authority_enabled() {
            return;
        }
        self.read_state.clear_session(session_id).await;
    }

    /// 主会话 compact 只撤销 parent scope，不影响仍在运行的 delegation child。
    pub(crate) async fn clear_parent_file_read_state(&self, session_id: &SessionId) {
        if !self.file_edit_authority_enabled() {
            return;
        }
        self.read_state
            .clear_scope(&ReadStateScope::new(Some(session_id.clone()), None))
            .await;
    }

    pub(crate) async fn begin_file_read_state_checkpoint(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> Result<(), String> {
        if !self.file_edit_authority_enabled() {
            return Ok(());
        }
        let scope = self.file_read_state_scope_for_turn(session_id, turn_id);
        self.read_state.begin_checkpoint(&scope, turn_id).await
    }

    pub(crate) async fn commit_file_read_state_checkpoint(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> Result<(), String> {
        if !self.file_edit_authority_enabled() {
            return Ok(());
        }
        let scope = self.file_read_state_scope_for_turn(session_id, turn_id);
        self.read_state.commit_checkpoint(&scope, turn_id).await
    }

    pub(crate) async fn rollback_file_read_state_checkpoint(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> Result<(), String> {
        if !self.file_edit_authority_enabled() {
            return Ok(());
        }
        let scope = self.file_read_state_scope_for_turn(session_id, turn_id);
        self.read_state.rollback_checkpoint(&scope, turn_id).await
    }

    /// delegation compact 只撤销该 child 的许可，不影响同 session 的 parent / sibling。
    pub(crate) async fn clear_delegation_file_read_state(
        &self,
        session_id: &SessionId,
        caller_id: &str,
    ) {
        if !self.file_edit_authority_enabled() {
            return;
        }
        self.read_state
            .clear_scope(&ReadStateScope::new(
                Some(session_id.clone()),
                Some(caller_id.to_owned()),
            ))
            .await;
    }

    fn file_read_state_scope_for_turn(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> ReadStateScope {
        let caller_id = self.access.delegation_child.then(|| turn_id.to_owned());
        ReadStateScope::new(Some(session_id.clone()), caller_id)
    }

    pub(super) fn file_read_state_scope(
        &self,
        context: &ToolDispatchContext,
    ) -> Option<ReadStateScope> {
        if !self.file_edit_authority_enabled() {
            return None;
        }
        let session_id = context.current_session_id.clone()?;
        let caller_id = if self.access.delegation_child {
            Some(context.current_turn_id.clone()?)
        } else {
            None
        };
        Some(ReadStateScope::new(Some(session_id), caller_id))
    }

    async fn evaluate_file_read_state(
        &self,
        context: &ToolDispatchContext,
        path: &Path,
        content: &str,
    ) -> ReadStateVerdict {
        let Some(scope) = self.file_read_state_scope(context) else {
            return ReadStateVerdict::Missing;
        };
        self.read_state
            .evaluate(&scope, path, &ContentRevision::from_text(content))
            .await
    }

    async fn activate_file_read_evidence(
        &self,
        context: &ToolDispatchContext,
        evidence: ReadEvidence,
    ) {
        let Some(scope) = self.file_read_state_scope(context) else {
            return;
        };
        self.read_state.record(&scope, evidence).await;
    }

    /// 完整文本附件成功构造进模型用户消息后，登记等价的完整读取许可。
    pub(crate) async fn record_text_attachment_read(
        &self,
        context: &ToolDispatchContext,
        canonical_path: PathBuf,
        content: &str,
    ) {
        self.activate_file_read_evidence(
            context,
            ReadEvidence::complete_text(canonical_path, content),
        )
        .await;
    }

    async fn clear_file_path_state(&self, context: &ToolDispatchContext, path: &Path) {
        if let Some(scope) = self.file_read_state_scope(context) {
            self.read_state.clear_path(&scope, path).await;
        }
    }

    pub(super) async fn file_read(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let args: FileReadArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let start = args.start.unwrap_or(1);
        let count = args.count.unwrap_or(DEFAULT_FILE_READ_LINES);
        if start == 0 {
            return Err(ToolError::InvalidArgs("start 必须大于 0".into()));
        }
        if count == 0 {
            return Err(ToolError::InvalidArgs("count 必须大于 0".into()));
        }
        let path = resolve_tool_path(&self.workspace_root, &args.path);
        if self.memory_store.is_some() {
            ensure_not_memory_path(&path)?;
        }
        match crate::attachment::attachment_kind_for_path(&path) {
            AttachmentKind::Image => {
                let media =
                    crate::attachment::read_image_attachment(&path, &self.attachment_limits)
                        .await?;
                return Ok(ToolExecution::completed(json!({
                    "path": args.path,
                    "kind": "image",
                    "media_type": media.media_type,
                    "note": "图片内容已作为附件块附加在 tool_result 之后",
                    (FILE_READ_MEDIA_KEY): media.to_json(),
                })));
            }
            AttachmentKind::Pdf => {
                let media =
                    crate::attachment::read_document_attachment(&path, &self.attachment_limits)
                        .await?;
                return Ok(ToolExecution::completed(json!({
                    "path": args.path,
                    "kind": "pdf",
                    "media_type": media.media_type,
                    "note": "PDF 内容已作为附件块附加在 tool_result 之后",
                    (FILE_READ_MEDIA_KEY): media.to_json(),
                })));
            }
            AttachmentKind::Text => {}
        }

        let path_lock = self.path_lock(&path).await?;
        let _guard = path_lock.lock().await;
        let read_path = fs::canonicalize(&path).await?;
        if self.memory_store.is_some() {
            ensure_not_memory_path(&read_path)?;
        }
        let canonical = lexical_normalize_path(&read_path);
        let keyword = args
            .keyword
            .as_deref()
            .filter(|keyword| !keyword.trim().is_empty());
        let result = read_text_page(
            &read_path,
            TextPageRequest {
                display_path: &args.path,
                canonical_path: canonical,
                start,
                count,
                keyword,
                show_linenos: args.show_linenos.unwrap_or(true),
                max_chars: self.limits.file_read_max_chars,
            },
        )
        .await?;
        match result {
            TextPageOutcome::Page(result) => {
                if let Some(evidence) = result.evidence {
                    self.activate_file_read_evidence(context, evidence).await;
                }
                Ok(ToolExecution::completed(result.output))
            }
            TextPageOutcome::LineTooLong { line } => Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "line": line,
                "msg": if self.file_edit_authority_enabled() {
                    format!(
                        "第 {line} 行无法在本次 file_read 单行上限内完整返回，因而无法安全生成读取证据或修改许可；请改用 code_run 定向读取该行。"
                    )
                } else {
                    format!(
                        "第 {line} 行无法在本次 file_read 单行上限内完整返回；请改用 code_run 定向读取该行。"
                    )
                },
            }))),
        }
    }

    pub(super) async fn file_patch(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let args: FilePatchArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        if args.old_content.is_empty() {
            return Err(ToolError::InvalidArgs("old_content 不能为空".into()));
        }
        let path = resolve_tool_path(&self.workspace_root, &args.path);
        ensure_not_memory_path(&path)?;
        let path_lock = self.path_lock(&path).await?;
        let _guard = path_lock.lock().await;
        let (write_path, existed) = resolve_write_target(&path).await?;
        if !existed {
            return Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "msg": "file_patch 的目标文件不存在，请先用 file_read 确认路径。",
            })));
        }
        ensure_not_memory_path(&write_path)?;
        let key = lexical_normalize_path(&write_path);
        let _file_write_lock = self.acquire_file_write_lock(&key, context).await?;
        let before = Arc::new(match fs::read_to_string(&write_path).await {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ToolExecution::business_failure(json!({
                    "path": args.path,
                    "status": "error",
                    "msg": "file_patch 的目标文件不存在，请先用 file_read 确认路径。",
                })));
            }
            Err(error) => return Err(ToolError::Io(error)),
        });
        let mut match_starts = before
            .match_indices(&args.old_content)
            .map(|(start, _)| start);
        let Some(match_start) = match_starts.next() else {
            return Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "msg": "未找到匹配的 old_content，请先重新 file_read 确认当前内容。",
            })));
        };
        if !args.replace_all && match_starts.next().is_some() {
            return Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "msg": "old_content 至少匹配两处，必须全局唯一。请扩大文本块并加入目标附近上下文，或在确认全部替换时使用 replace_all=true。",
            })));
        }

        let match_end = match_start.saturating_add(args.old_content.len());
        let authority = if self.file_edit_authority_enabled() {
            let suggested = suggested_read_range(&before, match_start, match_end);
            Some(
                match self.evaluate_file_read_state(context, &key, &before).await {
                    ReadStateVerdict::Fresh(authority) => authority,
                    ReadStateVerdict::Missing => {
                        return Ok(ToolExecution::business_failure(patch_permission_error(
                            &args.path,
                            if args.replace_all {
                                None
                            } else {
                                Some(suggested)
                            },
                            false,
                        )));
                    }
                    ReadStateVerdict::Stale => {
                        return Ok(ToolExecution::business_failure(patch_permission_error(
                            &args.path,
                            if args.replace_all {
                                None
                            } else {
                                Some(suggested)
                            },
                            true,
                        )));
                    }
                },
            )
        } else {
            None
        };
        if args.replace_all && authority.as_ref().is_some_and(|value| !value.complete) {
            return Ok(ToolExecution::business_failure(patch_permission_error(
                &args.path, None, false,
            )));
        }

        let replacements = if args.replace_all {
            1usize.saturating_add(match_starts.count())
        } else {
            1
        };
        let after = Arc::new(if args.replace_all {
            before.replace(&args.old_content, &args.new_content)
        } else {
            before.replacen(&args.old_content, &args.new_content, 1)
        });
        let migration = if !self.file_edit_authority_enabled() {
            None
        } else if args.replace_all {
            Some(CoverageMigration {
                ranges: LineRange::new(1, read_state::logical_line_count(&after))
                    .into_iter()
                    .collect(),
                complete: true,
            })
        } else {
            let Some(authority) = authority.as_ref() else {
                return Err(ToolError::InvalidArgs(
                    "file edit authority state missing while enforcement is enabled".into(),
                ));
            };
            match migrate_edit_coverage(
                &before,
                &after,
                authority,
                match_start,
                match_end,
                args.new_content.len(),
            ) {
                Ok(migration) => Some(migration),
                Err(required) => {
                    return Ok(ToolExecution::business_failure(patch_permission_error(
                        &args.path,
                        Some(required),
                        false,
                    )));
                }
            }
        };
        let evidence = migration.map(|migration| {
            ReadEvidence::known_ranges(key.clone(), &after, migration.ranges, migration.complete)
        });
        ensure_file_commit_not_cancelled(context)?;
        let change = compute_file_change_async(
            args.path.clone(),
            FileChangeKind::Modified,
            Arc::clone(&before),
            Arc::clone(&after),
            self.limits.file_diff_max_changed_lines,
        )
        .await;
        ensure_file_commit_not_cancelled(context)?;
        let written = crate::storage::write_text_atomic_if_unchanged(
            &write_path,
            after.as_bytes(),
            Some(before.as_bytes()),
        )
        .await
        .map_err(|e| ToolError::Io(e.into_io_error()))?;
        if !written {
            self.clear_file_path_state(context, &key).await;
            return Ok(ToolExecution::business_failure(stale_write_error(
                &args.path,
            )));
        }
        if let Some(evidence) = evidence {
            self.activate_file_read_evidence(context, evidence).await;
        }
        let mut output = json!({
            "path": args.path,
            "status": "success",
            "msg": "文件局部修改成功",
            "replacements": replacements,
        });
        drop(_guard);
        if let Some(change) = change {
            attach_file_change(&mut output, &change);
        }
        Ok(ToolExecution::completed(output))
    }

    pub(super) async fn file_write(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let args: FileWriteArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let mode = args.mode.unwrap_or_else(|| "overwrite".into());
        if !matches!(mode.as_str(), "overwrite" | "append" | "prepend") {
            return Err(ToolError::InvalidArgs(format!("未知 mode: {mode}")));
        }
        let path = resolve_tool_path(&self.workspace_root, &args.path);
        ensure_not_memory_path(&path)?;
        let path_lock = self.path_lock(&path).await?;
        let _guard = path_lock.lock().await;
        let (write_path, existed) = resolve_write_target(&path).await?;
        if existed {
            ensure_not_memory_path(&write_path)?;
        }
        let key = if existed {
            lexical_normalize_path(&write_path)
        } else {
            tool_path_lock_key(&write_path).await
        };
        if !existed {
            ensure_not_memory_path(&key)?;
        }
        let _file_write_lock = self.acquire_file_write_lock(&key, context).await?;
        let before = if existed {
            fs::read_to_string(&write_path).await?
        } else {
            String::new()
        };

        let authority = if existed && self.file_edit_authority_enabled() {
            match self.evaluate_file_read_state(context, &key, &before).await {
                ReadStateVerdict::Fresh(authority) => Some(authority),
                ReadStateVerdict::Missing => {
                    return Ok(ToolExecution::business_failure(write_permission_error(
                        &args.path,
                        &mode,
                        false,
                        read_state::logical_line_count(&before),
                    )));
                }
                ReadStateVerdict::Stale => {
                    return Ok(ToolExecution::business_failure(write_permission_error(
                        &args.path,
                        &mode,
                        true,
                        read_state::logical_line_count(&before),
                    )));
                }
            }
        } else {
            None
        };
        if let Some(authority) = &authority {
            let authorized = match mode.as_str() {
                "append" => authority.has_eof(),
                "overwrite" | "prepend" => authority.complete,
                _ => false,
            };
            if !authorized {
                return Ok(ToolExecution::business_failure(write_permission_error(
                    &args.path,
                    &mode,
                    false,
                    authority.total_lines,
                )));
            }
        }

        let before = Arc::new(before);
        let after = Arc::new(match mode.as_str() {
            "overwrite" => args.content,
            "append" => format!("{before}{}", args.content),
            "prepend" => format!("{}{before}", args.content),
            _ => unreachable!("mode 已校验"),
        });
        if existed && before == after {
            return Ok(ToolExecution::completed(json!({
                "path": args.path,
                "status": "no_change",
                "bytes_written": before.len(),
            })));
        }
        let migration = if !self.file_edit_authority_enabled() {
            None
        } else if let Some(authority) = &authority {
            if authority.complete || mode != "append" {
                Some(CoverageMigration {
                    ranges: LineRange::new(1, read_state::logical_line_count(&after))
                        .into_iter()
                        .collect(),
                    complete: true,
                })
            } else {
                match migrate_edit_coverage(
                    &before,
                    &after,
                    authority,
                    before.len(),
                    before.len(),
                    after.len().saturating_sub(before.len()),
                ) {
                    Ok(migration) => Some(migration),
                    Err(_) => {
                        self.clear_file_path_state(context, &key).await;
                        return Ok(ToolExecution::business_failure(write_permission_error(
                            &args.path,
                            &mode,
                            false,
                            authority.total_lines,
                        )));
                    }
                }
            }
        } else {
            Some(CoverageMigration {
                ranges: LineRange::new(1, read_state::logical_line_count(&after))
                    .into_iter()
                    .collect(),
                complete: true,
            })
        };
        let evidence = migration.map(|migration| {
            ReadEvidence::known_ranges(key.clone(), &after, migration.ranges, migration.complete)
        });
        ensure_file_commit_not_cancelled(context)?;
        let change = compute_file_change_async(
            args.path.clone(),
            if existed {
                FileChangeKind::Modified
            } else {
                FileChangeKind::Created
            },
            Arc::clone(&before),
            Arc::clone(&after),
            self.limits.file_diff_max_changed_lines,
        )
        .await;
        ensure_file_commit_not_cancelled(context)?;
        let written = crate::storage::write_text_atomic_if_unchanged(
            &write_path,
            after.as_bytes(),
            existed.then_some(before.as_bytes()),
        )
        .await
        .map_err(|e| ToolError::Io(e.into_io_error()))?;
        if !written {
            self.clear_file_path_state(context, &key).await;
            return Ok(ToolExecution::business_failure(stale_write_error(
                &args.path,
            )));
        }
        if let Some(evidence) = evidence {
            self.activate_file_read_evidence(context, evidence).await;
        }
        let mut output = json!({
            "path": args.path,
            "status": "success",
            "bytes_written": after.len(),
        });
        drop(_guard);
        if let Some(change) = change {
            attach_file_change(&mut output, &change);
        }
        Ok(ToolExecution::completed(output))
    }

    pub(super) async fn path_lock(&self, path: &Path) -> Result<Arc<Mutex<()>>, ToolError> {
        let key = tool_path_lock_key(path).await;
        let mut locks = self
            .path_locks
            .lock()
            .map_err(|_| ToolError::InvalidArgs("tool path lock registry poisoned".to_string()))?;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        Ok(lock)
    }

    async fn acquire_file_write_lock(
        &self,
        stable_path_key: &Path,
        context: &ToolDispatchContext,
    ) -> Result<Option<FileLockGuard>, ToolError> {
        let Some(lock_root) = self.file_write_lock_root.as_deref() else {
            return Ok(None);
        };
        let lock_path = paths::file_write_lock_path(lock_root, stable_path_key);
        loop {
            ensure_file_commit_not_cancelled(context)?;
            match FileLockGuard::try_lock_exclusive(&lock_path)
                .await
                .map_err(|error| {
                    ToolError::Io(std::io::Error::other(format!(
                        "获取文件写锁失败 ({}): {error}",
                        lock_path.display()
                    )))
                })? {
                Some(guard) => {
                    ensure_file_commit_not_cancelled(context)?;
                    return Ok(Some(guard));
                }
                None => {
                    let retry = time::sleep(Duration::from_millis(50));
                    if let Some(cancellation) = context.cancellation.as_ref() {
                        tokio::select! {
                            _ = cancellation.cancelled() => return Err(ToolError::Interrupted),
                            _ = retry => {}
                        }
                    } else {
                        retry.await;
                    }
                }
            }
        }
    }
}

async fn tool_path_lock_key(path: &Path) -> PathBuf {
    if let Ok(canonical) = tokio::fs::canonicalize(path).await {
        return canonical;
    }
    let mut resolved = PathBuf::new();
    let mut unresolved = Vec::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if unresolved.pop().is_none() {
                    resolved.pop();
                }
            }
            Component::Normal(part) => {
                if unresolved.is_empty() {
                    let candidate = resolved.join(part);
                    if let Ok(canonical) = tokio::fs::canonicalize(&candidate).await {
                        // 已存在前缀必须先解析 symlink，再处理后续 `..`。
                        resolved = canonical;
                        continue;
                    }
                }
                unresolved.push(part.to_os_string());
            }
        }
    }
    resolved.extend(unresolved);
    resolved
}

fn lexical_normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
            Component::RootDir | Component::Prefix(_) => out.push(component.as_os_str()),
        }
    }
    out
}

async fn compute_file_change_async(
    path: String,
    kind: FileChangeKind,
    before: Arc<String>,
    after: Arc<String>,
    max_changed_lines: usize,
) -> Option<FileChange> {
    match tokio::task::spawn_blocking(move || {
        compute_file_change(path, kind, &before, &after, max_changed_lines)
    })
    .await
    {
        Ok(change) => change,
        Err(error) => {
            log::error!(target: "tool_diff", "file diff 采集任务异常结束: {error}");
            None
        }
    }
}

fn ensure_file_commit_not_cancelled(context: &ToolDispatchContext) -> Result<(), ToolError> {
    if context
        .cancellation
        .as_ref()
        .is_some_and(|cancellation| cancellation.is_cancelled())
    {
        return Err(ToolError::Interrupted);
    }
    Ok(())
}

/// 对已有文件固定 canonical 目标，保留叶子 symlink 本身；新文件保留请求路径。
async fn resolve_write_target(path: &Path) -> Result<(PathBuf, bool), ToolError> {
    match fs::canonicalize(path).await {
        Ok(target) => Ok((target, true)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.file_type().is_symlink() => Err(ToolError::InvalidArgs(
                    format!("拒绝写入悬空 symlink: {}", path.display()),
                )),
                Ok(_) => Err(ToolError::Io(error)),
                Err(metadata_error) if metadata_error.kind() == std::io::ErrorKind::NotFound => {
                    Ok((path.to_path_buf(), false))
                }
                Err(metadata_error) => Err(ToolError::Io(metadata_error)),
            }
        }
        Err(error) => Err(ToolError::Io(error)),
    }
}

fn patch_permission_error(
    display_path: &str,
    required_range: Option<LineRange>,
    stale: bool,
) -> Value {
    let required_read = if let Some(range) = required_range {
        json!({
            "kind": "range",
            "start": range.start,
            "count": range.end.saturating_sub(range.start).saturating_add(1),
        })
    } else {
        json!({"kind": "complete", "start": 1})
    };
    let mut output = json!({
        "path": display_path,
        "status": "error",
        "required_read": required_read,
        "msg": if stale {
            format!("文件 {display_path} 在上次读取后已变化；请按 required_read 重新读取后再试。")
        } else if required_range.is_some() {
            "file_patch 的目标及必要换行边界尚未完整读取；请按 required_read 补读后再试。"
                .to_string()
        } else {
            format!("file_patch(replace_all=true) 需要先完整读取已有文件 {display_path}。")
        },
    });
    if stale {
        output["stale"] = json!(true);
    }
    output
}

fn write_permission_error(
    display_path: &str,
    mode: &str,
    stale: bool,
    total_lines: usize,
) -> Value {
    let required_read = if mode == "append" {
        json!({
            "kind": "eof",
            "start": total_lines.max(1),
        })
    } else {
        json!({"kind": "complete", "start": 1})
    };
    let requirement = if mode == "append" {
        "真实文件末尾"
    } else {
        "完整文件"
    };
    let mut output = json!({
        "path": display_path,
        "status": "error",
        "required_read": required_read,
        "msg": if stale {
            format!("文件 {display_path} 在上次读取后已变化；请按 required_read 重新读取{requirement}后再试。")
        } else {
            format!("已有文件 {display_path} 的 {mode} 操作需要先读取{requirement}；请按 required_read 读取后再试。")
        },
    });
    if stale {
        output["stale"] = json!(true);
    }
    output
}

fn stale_write_error(display_path: &str) -> Value {
    json!({
        "path": display_path,
        "status": "error",
        "stale": true,
        "msg": format!(
            "文件 {display_path} 在写入前被其他进程修改，原子写入已拒绝；请重新读取所需区域后再试。"
        ),
    })
}

pub(super) fn ensure_not_memory_path(path: &Path) -> Result<(), ToolError> {
    if crate::attachment::is_protected_memory_path(path) {
        return Err(ToolError::InvalidArgs(
            "目标是 agent 私有受保护文件，普通文件工具不可访问".into(),
        ));
    }
    Ok(())
}
