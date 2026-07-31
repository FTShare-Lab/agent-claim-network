//! 文件工具、read-before-write 校验与路径级并发控制。
//!
//! 实现 file_read/file_write/file_patch，并维护文件读取授权与 diff 采集。

use super::*;

impl ToolRegistry {
    /// resume 同一 session 时撤销其旧进程内 file_read 写入许可。
    pub async fn clear_file_read_state(&self, session_id: &SessionId) {
        self.read_state.clear_session(session_id).await;
    }

    pub(super) fn file_read_state_scope(
        &self,
        context: &ToolDispatchContext,
    ) -> Option<ReadStateScope> {
        let session_id = context.current_session_id.clone()?;
        let caller_id = if self.access.delegation_child {
            Some(context.current_turn_id.clone()?)
        } else {
            None
        };
        Some(ReadStateScope::new(Some(session_id), caller_id))
    }

    pub(super) async fn evaluate_file_read_state(
        &self,
        context: &ToolDispatchContext,
        path: &Path,
        content: &str,
        mtime: Option<std::time::SystemTime>,
    ) -> ReadStateVerdict {
        let Some(scope) = self.file_read_state_scope(context) else {
            return ReadStateVerdict::Missing;
        };
        self.read_state.evaluate(&scope, path, content, mtime).await
    }

    pub(super) async fn record_file_read_state(
        &self,
        context: &ToolDispatchContext,
        path: PathBuf,
        content: String,
        mtime: Option<std::time::SystemTime>,
    ) {
        let Some(scope) = self.file_read_state_scope(context) else {
            return;
        };
        self.read_state.record(&scope, path, content, mtime).await;
    }

    pub(super) async fn record_file_read_config_truncated_state(
        &self,
        context: &ToolDispatchContext,
        path: PathBuf,
    ) {
        let Some(scope) = self.file_read_state_scope(context) else {
            return;
        };
        self.read_state
            .record_config_truncated(&scope, path, self.limits.file_read_max_chars)
            .await;
    }

    pub(super) async fn file_read(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<Value, ToolError> {
        let args: FileReadArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let path = resolve_tool_path(&self.workspace_root, &args.path);
        if self.memory_store.is_some() {
            ensure_not_memory_path(&path)?;
        }
        // 图片 / PDF 走附件公共层：内容校验 + 规格化，媒体放保留键 media，
        // 由 turn loop 剥离成独立内容块，避免 base64 进入 tool_result 文本。
        match crate::attachment::attachment_kind_for_path(&path) {
            AttachmentKind::Image => {
                let media =
                    crate::attachment::read_image_attachment(&path, &self.attachment_limits)
                        .await?;
                return Ok(json!({
                    "path": args.path,
                    "kind": "image",
                    "media_type": media.media_type,
                    "note": "图片内容已作为附件块附加在 tool_result 之后",
                    (FILE_READ_MEDIA_KEY): media.to_json(),
                }));
            }
            AttachmentKind::Pdf => {
                let media =
                    crate::attachment::read_document_attachment(&path, &self.attachment_limits)
                        .await?;
                return Ok(json!({
                    "path": args.path,
                    "kind": "pdf",
                    "media_type": media.media_type,
                    "note": "PDF 内容已作为附件块附加在 tool_result 之后",
                    (FILE_READ_MEDIA_KEY): media.to_json(),
                }));
            }
            AttachmentKind::Text => {}
        }
        let path_lock = self.path_lock(&path).await?;
        let _guard = path_lock.lock().await;
        let read_path = fs::canonicalize(&path).await?;
        let (raw, file_truncated) = if self.access.delegation_child {
            read_text_file_bounded(&read_path, self.limits.file_read_max_chars).await?
        } else {
            (tokio::fs::read_to_string(&read_path).await?, false)
        };
        let selection = select_lines_with_keyword(
            &raw,
            args.start.unwrap_or(1),
            args.count.unwrap_or(200),
            args.keyword.as_deref(),
            args.show_linenos.unwrap_or(true),
            self.limits.file_read_max_chars,
        );
        let truncated = file_truncated || selection.truncated;
        let is_complete_read = !truncated && args.start.unwrap_or(1) == 1 && args.keyword.is_none();
        if is_complete_read {
            let key = lexical_normalize_path(&read_path);
            let mtime = file_mtime(&read_path).await?;
            self.record_file_read_state(context, key, raw, mtime).await;
        } else if file_truncated || selection.max_chars_reached {
            self.record_file_read_config_truncated_state(
                context,
                lexical_normalize_path(&read_path),
            )
            .await;
        }
        Ok(json!({
            "path": args.path,
            "content": selection.content,
            "truncated": truncated,
        }))
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
        if args.old_content == args.new_content {
            return Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "msg": "old_content 与 new_content 相同，拒绝 no-op patch。",
            })));
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
        let before = match fs::read_to_string(&write_path).await {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ToolExecution::business_failure(json!({
                    "path": args.path,
                    "status": "error",
                    "msg": "file_patch 的目标文件不存在，请先用 file_read 确认路径。",
                })));
            }
            Err(error) => return Err(ToolError::Io(error)),
        };
        let key = lexical_normalize_path(&write_path);
        let mtime = file_mtime(&write_path).await?;
        if let Some(error) = read_state_guard_error(
            self.evaluate_file_read_state(context, &key, &before, mtime)
                .await,
            &args.path,
        ) {
            return Ok(ToolExecution::business_failure(error));
        }
        let match_lines = match_start_line_numbers(&before, &args.old_content);
        let count = match_lines.len();
        if count == 0 {
            return Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "msg": "未找到匹配的 old_content，请先重新 file_read 确认当前内容。",
            })));
        }
        if count > 1 && !args.replace_all {
            let line_list = match_lines
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join("、");
            return Ok(ToolExecution::business_failure(json!({
                "path": args.path,
                "status": "error",
                "msg": format!("找到 {count} 处匹配（第 {line_list} 行），old_content 必须全局唯一。请扩大文本块，或在确认全部替换时使用 replace_all=true。"),
            })));
        }
        let replacements = if args.replace_all { count } else { 1 };
        let after = if args.replace_all {
            before.replace(&args.old_content, &args.new_content)
        } else {
            before.replacen(&args.old_content, &args.new_content, 1)
        };
        let written = crate::storage::write_text_atomic_if_unchanged(
            &write_path,
            after.as_bytes(),
            Some(before.as_bytes()),
        )
        .await
        .map_err(|e| ToolError::Io(e.into_io_error()))?;
        if !written {
            return Ok(ToolExecution::business_failure(stale_write_error(
                &args.path,
            )));
        }
        let new_mtime = file_mtime(&write_path).await?;
        self.record_file_read_state(context, key, after.clone(), new_mtime)
            .await;
        let mut output = json!({
            "path": args.path,
            "status": "success",
            "msg": "文件局部修改成功",
            "replacements": replacements,
        });
        drop(_guard);
        if let Some(change) = compute_file_change_async(
            args.path,
            FileChangeKind::Modified,
            before,
            after,
            self.limits.file_diff_max_changed_lines,
        )
        .await
        {
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
        let path = resolve_tool_path(&self.workspace_root, &args.path);
        ensure_not_memory_path(&path)?;
        let path_lock = self.path_lock(&path).await?;
        let _guard = path_lock.lock().await;
        let mode = args.mode.unwrap_or_else(|| "overwrite".into());
        let (write_path, existed) = resolve_write_target(&path).await?;
        let before = if existed {
            fs::read_to_string(&write_path).await?
        } else {
            String::new()
        };
        let key = if existed {
            lexical_normalize_path(&write_path)
        } else {
            tool_path_lock_key(&write_path).await
        };
        if existed {
            let mtime = file_mtime(&write_path).await?;
            if let Some(error) = read_state_guard_error(
                self.evaluate_file_read_state(context, &key, &before, mtime)
                    .await,
                &args.path,
            ) {
                return Ok(ToolExecution::business_failure(error));
            }
        }
        let after = match mode.as_str() {
            "overwrite" => args.content,
            "append" => format!("{before}{}", args.content),
            "prepend" => format!("{}{before}", args.content),
            other => return Err(ToolError::InvalidArgs(format!("未知 mode: {other}"))),
        };
        if existed && before == after {
            return Ok(ToolExecution::completed(json!({
                "path": args.path,
                "status": "no_change",
                "bytes_written": before.len(),
            })));
        }
        let written = crate::storage::write_text_atomic_if_unchanged(
            &write_path,
            after.as_bytes(),
            existed.then_some(before.as_bytes()),
        )
        .await
        .map_err(|e| ToolError::Io(e.into_io_error()))?;
        if !written {
            return Ok(ToolExecution::business_failure(stale_write_error(
                &args.path,
            )));
        }
        let new_mtime = file_mtime(&write_path).await?;
        self.record_file_read_state(context, key, after.clone(), new_mtime)
            .await;
        let mut output = json!({
            "path": args.path,
            "status": "success",
            "bytes_written": after.len(),
        });
        drop(_guard);
        if let Some(change) = compute_file_change_async(
            args.path,
            if existed {
                FileChangeKind::Modified
            } else {
                FileChangeKind::Created
            },
            before,
            after,
            self.limits.file_diff_max_changed_lines,
        )
        .await
        {
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
}

async fn tool_path_lock_key(path: &Path) -> PathBuf {
    if let Ok(canonical) = tokio::fs::canonicalize(path).await {
        return canonical;
    }
    let mut current = path.to_path_buf();
    let mut suffix = Vec::<OsString>::new();
    while let Some(file_name) = current.file_name().map(|value| value.to_os_string()) {
        suffix.push(file_name);
        if !current.pop() {
            break;
        }
        if let Ok(canonical_parent) = tokio::fs::canonicalize(&current).await {
            let mut resolved = canonical_parent;
            for component in suffix.iter().rev() {
                resolved.push(component);
            }
            return lexical_normalize_path(&resolved);
        }
    }
    lexical_normalize_path(path)
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

async fn file_mtime(path: &Path) -> Result<Option<std::time::SystemTime>, ToolError> {
    let metadata = fs::metadata(path).await?;
    Ok(metadata.modified().ok())
}

async fn compute_file_change_async(
    path: String,
    kind: FileChangeKind,
    before: String,
    after: String,
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

fn read_state_guard_error(verdict: ReadStateVerdict, display_path: &str) -> Option<Value> {
    let msg = match verdict {
        ReadStateVerdict::Fresh => return None,
        ReadStateVerdict::Missing => format!(
            "已有文件 {display_path} 必须先在当前 session 中完整 file_read；窗口、搜索或截断读取不授权写入。"
        ),
        ReadStateVerdict::Stale => return Some(stale_write_error(display_path)),
        ReadStateVerdict::ConfigTruncated { max_chars } => {
            return Some(json!({
                "path": display_path,
                "status": "error",
                "file_read_max_chars": max_chars,
                "requires_user_config_change": true,
                "msg": format!(
                    "文件 {display_path} 的 file_read 被当前 [agent.tool].file_read_max_chars={max_chars} 截断。分页读取只能查看局部内容，因读取不完整不能授予写权限。请告知用户提高 ACN 的 [agent.tool].file_read_max_chars 配置，重启 ACN 后重新完整 file_read 并重试。"
                ),
            }));
        }
    };
    Some(json!({
        "path": display_path,
        "status": "error",
        "msg": msg,
    }))
}

fn stale_write_error(display_path: &str) -> Value {
    json!({
        "path": display_path,
        "status": "error",
        "msg": format!(
            "文件 {display_path} 在上次 file_read 后已变化，已拒绝写入；请重新完整 file_read 后再试。"
        ),
    })
}

/// 线性扫描所有非重叠精确匹配的起始行号，与 `str::replace` 语义一致。
fn match_start_line_numbers(content: &str, needle: &str) -> Vec<usize> {
    let mut line = 1usize;
    let mut scanned_until = 0usize;
    let mut result = Vec::new();
    for (start, _) in content.match_indices(needle) {
        line = line.saturating_add(
            content[scanned_until..start]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count(),
        );
        result.push(line);
        scanned_until = start;
    }
    result
}

pub(super) fn ensure_not_memory_path(path: &Path) -> Result<(), ToolError> {
    if crate::attachment::is_protected_memory_path(path) {
        return Err(ToolError::InvalidArgs(
            "MEMORY.md / USER.md 必须通过 memory 工具访问".into(),
        ));
    }
    Ok(())
}

async fn read_text_file_bounded(
    path: &Path,
    max_chars: usize,
) -> Result<(String, bool), ToolError> {
    let max_bytes = u64::try_from(bounded_text_byte_limit(max_chars)).unwrap_or(u64::MAX);
    let mut bytes = Vec::new();
    let mut reader = fs::File::open(path)
        .await?
        .take(max_bytes.saturating_add(1));
    reader.read_to_end(&mut bytes).await?;
    let truncated = u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes;
    if truncated {
        let limit = usize::try_from(max_bytes).unwrap_or(usize::MAX);
        bytes.truncate(limit.min(bytes.len()));
        while std::str::from_utf8(&bytes).is_err() {
            if bytes.pop().is_none() {
                break;
            }
        }
    }
    let text = String::from_utf8(bytes)
        .map_err(|err| ToolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, err)))?;
    Ok((text, truncated))
}

struct FileReadSelection {
    content: String,
    truncated: bool,
    max_chars_reached: bool,
}

fn select_lines_with_keyword(
    raw: &str,
    start: usize,
    count: usize,
    keyword: Option<&str>,
    show_linenos: bool,
    max_chars: usize,
) -> FileReadSelection {
    let lines = raw.lines().collect::<Vec<_>>();
    let start_idx = start.saturating_sub(1).min(lines.len());
    let mut window_start = start_idx;
    if let Some(keyword) = keyword {
        let keyword = keyword.to_ascii_lowercase();
        if let Some(found_idx) = lines
            .iter()
            .enumerate()
            .skip(start_idx)
            .find(|(_, line)| line.to_ascii_lowercase().contains(&keyword))
            .map(|(idx, _)| idx)
        {
            window_start = found_idx.saturating_sub(count / 3);
        }
    }
    let window_end = window_start.saturating_add(count).min(lines.len());
    let mut content = String::new();
    let mut content_chars = 0usize;
    let mut truncated = false;
    let mut max_chars_reached = false;
    for (idx, line) in lines[window_start..window_end].iter().enumerate() {
        let actual_line = window_start + idx + 1;
        let rendered = if show_linenos {
            format!("{actual_line}|{line}")
        } else {
            (*line).to_string()
        };
        let rendered = if content.is_empty() {
            rendered
        } else {
            format!("\n{rendered}")
        };
        let rendered_chars = rendered.chars().count();
        if content_chars.saturating_add(rendered_chars) > max_chars {
            truncated = true;
            max_chars_reached = true;
            break;
        }
        content.push_str(&rendered);
        content_chars = content_chars.saturating_add(rendered_chars);
    }
    if window_end < lines.len() {
        truncated = true;
    }
    FileReadSelection {
        content,
        truncated,
        max_chars_reached,
    }
}
