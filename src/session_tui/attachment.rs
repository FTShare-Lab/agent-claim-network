//! TUI `@path` 输入：文件附件预检、目录上下文展开与 macOS 剪贴板图片。
//!
//! Composer 只保存轻量路径与可见占位符；文件的重量级内容校验 / 重采样 / base64
//! 发生在发送链路的 `crate::attachment` 公共层，目录则在提交前展开为一级名称列表。

use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::Command;

use rand::Rng;

use crate::api::SessionAttachment;
use crate::attachment::{
    attachment_kind_for_path, AttachmentKind, AttachmentLimits, NormalizedMedia,
};
use crate::path_util::expand_current_user_home;

use super::at_path::scan_at_path_tokens;

/// 单个 `@目录` 最多注入的一级名称数，与路径补全共用资源上限。
const DIRECTORY_CONTEXT_MAX_ENTRIES: usize =
    crate::config::DEFAULT_TUI_AT_PATH_DIRECTORY_CONTEXT_MAX_ENTRIES;

/// 提交前解析出的文件附件及目录文本上下文。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ResolvedAtPaths {
    pub(super) attachments: Vec<SessionAttachment>,
    pub(super) directory_context: String,
}

/// 剪贴板图片附件：占位符 + 已规格化的内联图片数据。
/// 删除输入框中的占位符即等于撤销该附件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InputAttachment {
    pub(super) placeholder: String,
    media_type: String,
    data: String,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AttachmentError {
    #[error("`{token}` 解析失败: {reason}")]
    AtPathParse { token: String, reason: String },
    #[error("附件路径不存在或不可读取: {path}: {source}")]
    Metadata {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("附件路径是目录，不能作为文件预览: {0}")]
    IsDirectory(String),
    #[error("读取目录列表失败: {path}: {source}")]
    ReadDirectory {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("附件功能已禁用，不能引用路径: {0}")]
    Disabled(String),
    #[error("附件不是常规文件: {0}")]
    NotFile(String),
    #[error("附件过大: {path} 为 {actual} bytes，超过上限 {limit} bytes")]
    TooLarge {
        path: String,
        actual: u64,
        limit: u64,
    },
    #[error("单轮附件数量超限: {actual} 个，最多 {limit} 个")]
    TooManyFiles { actual: usize, limit: usize },
    #[error("agent 私有受保护文件不能作为附件: {0}")]
    ProtectedMemoryPath(String),
    #[cfg(target_os = "macos")]
    #[error("系统剪贴板暂不可用: {0}")]
    Clipboard(String),
    #[error("剪贴板图片内容校验失败: {0}")]
    ClipboardImageInvalid(String),
}

impl InputAttachment {
    pub(super) fn clipboard_image(placeholder: String, media: NormalizedMedia) -> Self {
        Self {
            placeholder,
            media_type: media.media_type,
            data: media.data,
        }
    }

    pub(super) fn to_session_attachment(&self) -> SessionAttachment {
        SessionAttachment::InlineImage {
            media_type: self.media_type.clone(),
            data: self.data.clone(),
        }
    }

    pub(super) fn to_preview_target(&self) -> PreviewTarget {
        PreviewTarget::InlineImage {
            // 占位符（如 [Image #2]）随目标传递，预览提示里据此区分多张图
            name: self.placeholder.clone(),
            media_type: self.media_type.clone(),
            data: self.data.clone(),
        }
    }
}

/// Ctrl+O 预览的目标：`@path` 引用的磁盘文件，或剪贴板图片的内联数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PreviewTarget {
    AtPath {
        raw_path: String,
    },
    InlineImage {
        name: String,
        media_type: String,
        data: String,
    },
}

/// 预览前准备好的本地文件。`temporary` 为 true 表示是为预览临时写出的
/// 文件（剪贴板图片），由 App 在退出时统一清理。`label` 用于 transcript
/// 提示，让用户确认打开的究竟是哪个附件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreviewFile {
    pub(super) path: PathBuf,
    pub(super) temporary: bool,
    pub(super) label: String,
}

/// 一批预览文件准备失败时，保留已经落盘的临时文件路径，交给 App 统一清理。
#[derive(Debug)]
pub(super) struct PreviewPreparationError {
    pub(super) source: AttachmentError,
    pub(super) temporary_paths: Vec<PathBuf>,
}

/// Ctrl+O 准备或拉起失败；临时路径即使跨 session 变为 stale 也必须登记清理。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreviewFailure {
    pub(super) message: String,
    pub(super) temporary_paths: Vec<PathBuf>,
}

/// 把一组预览目标落成可供 Quick Look 打开的本地文件（任一失败整体报错）。
/// 含文件系统访问与 base64 解码，**必须**在 `spawn_blocking` 中执行。
pub(super) fn prepare_preview_files(
    targets: Vec<PreviewTarget>,
    workspace_root: &std::path::Path,
) -> Result<Vec<PreviewFile>, PreviewPreparationError> {
    let mut files = Vec::with_capacity(targets.len());
    for target in targets {
        match prepare_preview_file(target, workspace_root) {
            Ok(file) => files.push(file),
            Err(source) => {
                let temporary_paths = files
                    .iter()
                    .filter(|file| file.temporary)
                    .map(|file| file.path.clone())
                    .collect();
                return Err(PreviewPreparationError {
                    source,
                    temporary_paths,
                });
            }
        }
    }
    Ok(files)
}

fn prepare_preview_file(
    target: PreviewTarget,
    workspace_root: &std::path::Path,
) -> Result<PreviewFile, AttachmentError> {
    match target {
        PreviewTarget::AtPath { raw_path } => {
            let path = absolutize_path(&raw_path, workspace_root);
            let display = path.display().to_string();
            let metadata =
                std::fs::metadata(&path).map_err(|source| AttachmentError::Metadata {
                    path: display.clone(),
                    source,
                })?;
            if metadata.is_dir() {
                return Err(AttachmentError::IsDirectory(display));
            }
            if !metadata.is_file() {
                return Err(AttachmentError::NotFile(display));
            }
            let label = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("attachment")
                .to_string();
            Ok(PreviewFile {
                path,
                temporary: false,
                label,
            })
        }
        PreviewTarget::InlineImage {
            name,
            media_type,
            data,
        } => {
            use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
            use base64::Engine as _;
            let bytes = BASE64_STANDARD.decode(data.as_bytes()).map_err(|error| {
                AttachmentError::ClipboardImageInvalid(format!("base64 解码失败: {error}"))
            })?;
            let extension = match media_type.as_str() {
                "image/jpeg" => "jpg",
                "image/gif" => "gif",
                "image/webp" => "webp",
                _ => "png",
            };
            let label = format!("{name} 剪贴板图片 {}KB ({media_type})", bytes.len() / 1024);
            let path = std::env::temp_dir().join(format!(
                "acn-preview-{:016x}.{extension}",
                preview_file_nonce()
            ));
            std::fs::write(&path, bytes).map_err(|source| AttachmentError::Metadata {
                path: path.display().to_string(),
                source,
            })?;
            Ok(PreviewFile {
                path,
                temporary: true,
                label,
            })
        }
    }
}

fn preview_file_nonce() -> u64 {
    rand::thread_rng().gen::<u64>()
}

/// 解析提交文本中的全部 `@path`：普通文件转为附件，目录展开为一级名称列表文本。
/// `existing_attachments` 为草稿里已有的剪贴板附件数，只与文件附件共同参与数量上限。
pub(super) fn resolve_at_paths(
    text: &str,
    workspace_root: &Path,
    limits: &AttachmentLimits,
    existing_attachments: usize,
) -> Result<ResolvedAtPaths, AttachmentError> {
    let tokens = scan_at_path_tokens(text);
    if tokens.is_empty() {
        return Ok(ResolvedAtPaths::default());
    }
    if !limits.enabled {
        let token = tokens
            .first()
            .and_then(|token| text.get(token.range.clone()))
            .unwrap_or("@path");
        return Err(AttachmentError::Disabled(token.to_string()));
    }
    let mut attachments = Vec::with_capacity(tokens.len());
    let mut directory_sections = Vec::new();
    for token in &tokens {
        let raw_path = match &token.parsed {
            Ok(raw_path) => raw_path,
            Err(reason) => {
                return Err(AttachmentError::AtPathParse {
                    token: text.get(token.range.clone()).unwrap_or("@").to_string(),
                    reason: reason.to_string(),
                });
            }
        };
        let path = absolutize_path(raw_path, workspace_root);
        let display = path.display().to_string();
        if crate::attachment::is_protected_memory_path(&path) {
            return Err(AttachmentError::ProtectedMemoryPath(display));
        }
        let metadata = std::fs::metadata(&path).map_err(|source| AttachmentError::Metadata {
            path: display.clone(),
            source,
        })?;
        if metadata.is_dir() {
            directory_sections.push(directory_context_section(raw_path, &path)?);
            continue;
        }
        if !metadata.is_file() {
            return Err(AttachmentError::NotFile(display));
        }
        if metadata.len() > limits.max_file_bytes {
            return Err(AttachmentError::TooLarge {
                path: display,
                actual: metadata.len(),
                limit: limits.max_file_bytes,
            });
        }
        attachments.push(match attachment_kind_for_path(&path) {
            AttachmentKind::Image => SessionAttachment::LocalImage { path },
            AttachmentKind::Pdf => SessionAttachment::DocumentFile {
                path,
                media_type: "application/pdf".into(),
            },
            AttachmentKind::Text => SessionAttachment::TextFile { path },
        });
    }
    let total = existing_attachments.saturating_add(attachments.len());
    if total > limits.max_files_per_turn {
        return Err(AttachmentError::TooManyFiles {
            actual: total,
            limit: limits.max_files_per_turn,
        });
    }
    Ok(ResolvedAtPaths {
        attachments,
        directory_context: directory_sections.join("\n\n"),
    })
}

fn directory_context_section(raw_path: &str, path: &Path) -> Result<String, AttachmentError> {
    let display = path.display().to_string();
    let reader = std::fs::read_dir(path).map_err(|source| AttachmentError::ReadDirectory {
        path: display.clone(),
        source,
    })?;
    // 只多读一项来确认截断，避免 @node_modules 等目录仍被全量读入和排序。
    let mut names = Vec::with_capacity(DIRECTORY_CONTEXT_MAX_ENTRIES.saturating_add(1));
    for entry in reader.take(DIRECTORY_CONTEXT_MAX_ENTRIES.saturating_add(1)) {
        let entry = entry.map_err(|source| AttachmentError::ReadDirectory {
            path: display.clone(),
            source,
        })?;
        let name = entry.file_name();
        if name != "." && name != ".." {
            names.push(name);
        }
    }
    let truncated = names.len() > DIRECTORY_CONTEXT_MAX_ENTRIES;
    if truncated {
        names.truncate(DIRECTORY_CONTEXT_MAX_ENTRIES);
    }
    // macOS `ls -A` 默认按 locale 排序；这里采用稳定的 Unicode 字典序，非 UTF-8
    // 名称使用 lossless-unavailable 的可见替代，仅作为模型上下文，不用于再次访问文件。
    names.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    let entries_summary = if truncated {
        format!("showing first {DIRECTORY_CONTEXT_MAX_ENTRIES}; more entries omitted")
    } else {
        format!("showing {}", names.len())
    };

    let mut lines = vec![
        format!("[Referenced directory: {raw_path}]"),
        format!("Resolved path: {display}"),
        format!("First-level entries (ls -A, {entries_summary}):"),
    ];
    lines.extend(
        names
            .into_iter()
            .map(|name| name.to_string_lossy().into_owned()),
    );
    Ok(lines.join("\n"))
}

/// Ctrl+V：读取剪贴板图片并完成校验 / 重采样，返回内联媒体数据。
/// 剪贴板没有图片时返回 `Ok(None)`（调用方给轻提示，不报错）。
///
/// 含子进程与文件 I/O 等阻塞调用，**必须**在 `spawn_blocking` 中执行；
/// 临时 PNG 读取完毕立即删除，不在磁盘残留剪贴板内容。
pub(super) fn read_clipboard_image_blocking(
    limits: &AttachmentLimits,
) -> Result<Option<NormalizedMedia>, AttachmentError> {
    let Some(path) = read_platform_clipboard_image_file()? else {
        return Ok(None);
    };
    let read = std::fs::read(&path);
    let _ = std::fs::remove_file(&path);
    let bytes = read.map_err(|source| AttachmentError::Metadata {
        path: path.display().to_string(),
        source,
    })?;
    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual > limits.max_file_bytes {
        return Err(AttachmentError::TooLarge {
            path: "剪贴板图片".into(),
            actual,
            limit: limits.max_file_bytes,
        });
    }
    let media = crate::attachment::normalize_image_attachment_sync_with_limit(
        bytes,
        "clipboard image".into(),
        limits.max_file_bytes,
    )
    .map_err(|error| AttachmentError::ClipboardImageInvalid(error.to_string()))?;
    Ok(Some(media))
}

fn absolutize_path(path: &str, workspace_root: &std::path::Path) -> PathBuf {
    let path = expand_current_user_home(std::path::Path::new(path));
    if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    }
}

#[cfg(target_os = "macos")]
fn read_platform_clipboard_image_file() -> Result<Option<PathBuf>, AttachmentError> {
    let path = std::env::temp_dir().join(format!(
        "acn-clipboard-{:016x}.png",
        rand::thread_rng().gen::<u64>()
    ));
    let escaped_path = applescript_string(&path.to_string_lossy());
    let script = format!(
        r#"set outPath to "{escaped_path}"
try
    set pngData to the clipboard as «class PNGf»
    set outFile to POSIX file outPath
    set fileRef to open for access outFile with write permission
    set eof of fileRef to 0
    write pngData to fileRef
    close access fileRef
    return "ok"
on error errMsg
    try
        close access fileRef
    end try
    return "missing:" & errMsg
end try"#
    );
    let output = Command::new("osascript")
        .arg("-e")
        .arg(script)
        .output()
        .map_err(|error| AttachmentError::Clipboard(error.to_string()))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if output.status.success() && stdout.trim().starts_with("ok") && path.is_file() {
        return Ok(Some(path));
    }
    let _ = std::fs::remove_file(path);
    Ok(None)
}

#[cfg(not(target_os = "macos"))]
fn read_platform_clipboard_image_file() -> Result<Option<PathBuf>, AttachmentError> {
    Ok(None)
}

#[cfg(target_os = "macos")]
fn applescript_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn limits() -> AttachmentLimits {
        AttachmentLimits::default()
    }

    #[test]
    fn no_at_token_resolves_to_no_attachments() {
        assert_eq!(
            resolve_at_paths("看下 src/lib.rs 这个文件", Path::new("."), &limits(), 0).unwrap(),
            ResolvedAtPaths::default()
        );
    }

    #[test]
    fn at_path_resolves_by_kind() {
        let dir = tempfile::tempdir().unwrap();
        let text_path = dir.path().join("note.md");
        let pdf_path = dir.path().join("brief.pdf");
        let image_path = dir.path().join("shot.png");
        std::fs::write(&text_path, "hello").unwrap();
        std::fs::write(&pdf_path, "%PDF-1.7").unwrap();
        std::fs::write(&image_path, "fake png bytes").unwrap();

        let text = format!(
            "对比 @{} 和 @{} 还有 @{}",
            text_path.display(),
            pdf_path.display(),
            image_path.display()
        );
        let resolved = resolve_at_paths(&text, Path::new("."), &limits(), 0).unwrap();
        assert_eq!(
            resolved.attachments,
            vec![
                SessionAttachment::TextFile { path: text_path },
                SessionAttachment::DocumentFile {
                    path: pdf_path,
                    media_type: "application/pdf".into(),
                },
                SessionAttachment::LocalImage { path: image_path },
            ]
        );
        assert!(resolved.directory_context.is_empty());
    }

    #[test]
    fn relative_at_path_uses_effective_acn_workspace_root() {
        let process_dir = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("note.md");
        std::fs::write(&path, "workspace file").unwrap();
        assert!(!process_dir.path().join("note.md").exists());

        let resolved = resolve_at_paths("@note.md", workspace.path(), &limits(), 0).unwrap();
        assert_eq!(
            resolved.attachments,
            vec![SessionAttachment::TextFile { path }]
        );
        assert!(resolved.directory_context.is_empty());
    }

    #[test]
    fn quoted_at_path_with_space_resolves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a b.txt");
        std::fs::write(&path, "x").unwrap();
        let text = format!("@\"{}\"", path.display());
        let resolved = resolve_at_paths(&text, Path::new("."), &limits(), 0).unwrap();
        assert_eq!(
            resolved.attachments,
            vec![SessionAttachment::TextFile { path }]
        );
        assert!(resolved.directory_context.is_empty());
    }

    #[test]
    fn missing_path_is_an_error_and_directory_becomes_context() {
        let dir = tempfile::tempdir().unwrap();
        let missing = format!("@{}/missing.txt", dir.path().display());
        assert!(matches!(
            resolve_at_paths(&missing, Path::new("."), &limits(), 0),
            Err(AttachmentError::Metadata { .. })
        ));
        std::fs::write(dir.path().join("visible.txt"), "x").unwrap();
        std::fs::write(dir.path().join(".hidden"), "x").unwrap();
        let directory = format!("@{}", dir.path().display());
        let resolved = resolve_at_paths(&directory, Path::new("."), &limits(), 0).unwrap();
        assert!(resolved.attachments.is_empty());
        assert!(resolved
            .directory_context
            .contains("[Referenced directory:"));
        assert!(resolved.directory_context.contains(".hidden"));
        assert!(resolved.directory_context.contains("visible.txt"));
    }

    #[test]
    fn directory_context_is_sorted_non_recursive_and_caps_retained_entries_at_thousand() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("0-nested")).unwrap();
        std::fs::write(dir.path().join("0-nested").join("inside.txt"), "x").unwrap();
        for index in (0..1_005).rev() {
            std::fs::write(dir.path().join(format!("entry-{index:04}.txt")), "x").unwrap();
        }
        std::fs::write(dir.path().join(".dotfile"), "x").unwrap();

        let input = format!("@{}", dir.path().display());
        let resolved = resolve_at_paths(&input, Path::new("."), &limits(), 0).unwrap();
        assert!(resolved.attachments.is_empty());
        assert!(resolved
            .directory_context
            .contains("showing first 1000; more entries omitted"));
        assert!(!resolved.directory_context.contains("inside.txt"));
        let entries = resolved
            .directory_context
            .lines()
            .skip(3)
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), DIRECTORY_CONTEXT_MAX_ENTRIES);
        assert!(entries.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn mixed_directory_and_file_reference_keeps_only_file_as_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("folder");
        std::fs::create_dir(&folder).unwrap();
        std::fs::write(folder.join("child.txt"), "x").unwrap();
        let file = dir.path().join("note.md");
        std::fs::write(&file, "note").unwrap();
        let input = format!("@{} @{}", folder.display(), file.display());

        let resolved = resolve_at_paths(&input, Path::new("."), &limits(), 0).unwrap();
        assert_eq!(
            resolved.attachments,
            vec![SessionAttachment::TextFile { path: file }]
        );
        assert!(resolved.directory_context.contains("child.txt"));
    }

    #[test]
    fn disabled_attachments_reject_directory_reference_too() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("child.txt"), "x").unwrap();
        let disabled = AttachmentLimits {
            enabled: false,
            ..AttachmentLimits::default()
        };
        let input = format!("@{}", dir.path().display());

        assert!(matches!(
            resolve_at_paths(&input, Path::new("."), &disabled, 0),
            Err(AttachmentError::Disabled(_))
        ));
    }

    #[test]
    fn parse_errors_are_not_silently_ignored() {
        assert!(matches!(
            resolve_at_paths("结尾 @", Path::new("."), &limits(), 0),
            Err(AttachmentError::AtPathParse { .. })
        ));
        assert!(matches!(
            resolve_at_paths(r#"@"未闭合"#, Path::new("."), &limits(), 0),
            Err(AttachmentError::AtPathParse { .. })
        ));
    }

    #[test]
    fn at_path_respects_max_files_per_turn() {
        let dir = tempfile::tempdir().unwrap();
        let mut text = String::new();
        for index in 0..3 {
            let path = dir.path().join(format!("f{index}.txt"));
            std::fs::write(&path, "x").unwrap();
            text.push_str(&format!("@{} ", path.display()));
        }
        let tight = AttachmentLimits {
            enabled: true,
            max_file_bytes: 1024,
            max_files_per_turn: 2,
            ..AttachmentLimits::default()
        };
        assert!(matches!(
            resolve_at_paths(&text, Path::new("."), &tight, 0),
            Err(AttachmentError::TooManyFiles {
                actual: 3,
                limit: 2
            })
        ));
        // 已有剪贴板附件也计入总数
        let path = dir.path().join("f0.txt");
        let single = format!("@{}", path.display());
        assert!(matches!(
            resolve_at_paths(&single, Path::new("."), &tight, 2),
            Err(AttachmentError::TooManyFiles {
                actual: 3,
                limit: 2
            })
        ));
    }

    #[test]
    fn at_path_rejects_protected_memory_files() {
        let dir = tempfile::tempdir().unwrap();
        let memories = dir.path().join("memories");
        std::fs::create_dir_all(&memories).unwrap();
        let path = memories.join("MEMORY.md");
        std::fs::write(&path, "secret").unwrap();

        let text = format!("@{}", path.display());
        assert!(matches!(
            resolve_at_paths(&text, Path::new("."), &limits(), 0),
            Err(AttachmentError::ProtectedMemoryPath(_))
        ));
    }

    #[test]
    fn at_path_respects_max_file_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.txt");
        std::fs::write(&path, vec![b'a'; 64]).unwrap();
        let tight = AttachmentLimits {
            enabled: true,
            max_file_bytes: 16,
            max_files_per_turn: 5,
            ..AttachmentLimits::default()
        };
        let text = format!("@{}", path.display());
        assert!(matches!(
            resolve_at_paths(&text, Path::new("."), &tight, 0),
            Err(AttachmentError::TooLarge { .. })
        ));
    }

    #[test]
    fn prepare_preview_resolves_at_path_file_without_temp_copy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shot.png");
        std::fs::write(&path, "fake").unwrap();

        let file = prepare_preview_file(
            PreviewTarget::AtPath {
                raw_path: path.display().to_string(),
            },
            Path::new("."),
        )
        .unwrap();
        assert_eq!(file.path, path);
        assert!(!file.temporary);
        assert_eq!(file.label, "shot.png");

        // 任一目标失败时整批报错
        assert!(matches!(
            prepare_preview_files(
                vec![PreviewTarget::AtPath {
                    raw_path: dir.path().join("missing.txt").display().to_string(),
                }],
                Path::new("."),
            ),
            Err(PreviewPreparationError {
                source: AttachmentError::Metadata { .. },
                ..
            })
        ));
    }

    #[test]
    fn preview_relative_path_uses_effective_workspace_root() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("shot.png");
        std::fs::write(&path, "fake").unwrap();

        let file = prepare_preview_file(
            PreviewTarget::AtPath {
                raw_path: "shot.png".into(),
            },
            workspace.path(),
        )
        .unwrap();
        assert_eq!(file.path, path);
    }

    #[test]
    fn prepare_preview_writes_inline_image_to_temp_file() {
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
        use base64::Engine as _;

        let file = prepare_preview_file(
            PreviewTarget::InlineImage {
                name: "[Image #2]".into(),
                media_type: "image/png".into(),
                data: BASE64_STANDARD.encode(b"png bytes"),
            },
            Path::new("."),
        )
        .unwrap();
        assert!(file.temporary);
        assert_eq!(file.path.extension().and_then(|e| e.to_str()), Some("png"));
        assert_eq!(std::fs::read(&file.path).unwrap(), b"png bytes");
        // 提示里带占位符编号，便于区分多张剪贴板图片
        assert!(file.label.contains("[Image #2]"));
        assert!(file.label.contains("剪贴板图片"));
        let _ = std::fs::remove_file(&file.path);

        assert!(matches!(
            prepare_preview_file(
                PreviewTarget::InlineImage {
                    name: "[Image #1]".into(),
                    media_type: "image/png".into(),
                    data: "不是 base64!!".into(),
                },
                Path::new("."),
            ),
            Err(AttachmentError::ClipboardImageInvalid(_))
        ));
    }

    #[test]
    fn preview_batch_failure_preserves_prior_temp_path_for_cleanup() {
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
        use base64::Engine as _;

        let error = prepare_preview_files(
            vec![
                PreviewTarget::InlineImage {
                    name: "[Image #1]".into(),
                    media_type: "image/png".into(),
                    data: BASE64_STANDARD.encode(b"temporary image"),
                },
                PreviewTarget::AtPath {
                    raw_path: "/definitely/missing/acn-preview-file".into(),
                },
            ],
            Path::new("."),
        )
        .unwrap_err();

        assert!(matches!(error.source, AttachmentError::Metadata { .. }));
        assert_eq!(error.temporary_paths.len(), 1);
        assert!(error.temporary_paths[0].exists());
        std::fs::remove_file(&error.temporary_paths[0]).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn applescript_strings_escape_quotes_and_backslashes() {
        assert_eq!(applescript_string(r#"/tmp/a"b\c"#), r#"/tmp/a\"b\\c"#);
    }
}
