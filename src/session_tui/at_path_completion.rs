//! `@path` 文件候选：活动 token 解析、一级目录扫描、过滤与插入文本编码。
//!
//! 候选只读取当前路径所属目录的直接子项，不递归、也不按扩展名或附件支持类型过滤；
//! 相对路径以 ACN 的有效 workspace（`acn --cd`，未指定时为启动 cwd）为基准，
//! 绝对路径保持原语义。

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use tokio::fs;

use crate::path_util::expand_current_user_home;

use super::at_path::ActiveAtPathToken;
use super::completion_menu::CompletionMenuEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AtPathCompletionLimits {
    pub(super) max_directory_entries: usize,
    pub(super) max_candidates: usize,
}

impl Default for AtPathCompletionLimits {
    fn default() -> Self {
        Self {
            max_directory_entries: crate::config::DEFAULT_TUI_AT_PATH_DIRECTORY_CONTEXT_MAX_ENTRIES,
            max_candidates: crate::config::DEFAULT_TUI_AT_PATH_MAX_CANDIDATES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum AtPathCandidateKind {
    Directory,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AtPathCandidate {
    pub(super) display_path: String,
    pub(super) raw_path: String,
    pub(super) kind: AtPathCandidateKind,
}

impl CompletionMenuEntry for AtPathCandidate {
    fn label(&self) -> &str {
        &self.display_path
    }

    fn description(&self) -> &str {
        match self.kind {
            AtPathCandidateKind::Directory => "directory",
            AtPathCandidateKind::File => "file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AtPathCompletionContext {
    pub(super) token: ActiveAtPathToken,
    pub(super) scan_dir: PathBuf,
    pub(super) typed_parent: String,
    pub(super) query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AtPathDirectoryEntry {
    pub(super) file_name: OsString,
    pub(super) kind: AtPathCandidateKind,
    pub(super) protected: bool,
}

/// 把活动 `@path` 拆成扫描目录、已输入父路径与当前文件名查询。
pub(super) fn completion_context(
    token: ActiveAtPathToken,
    workspace_root: &Path,
) -> AtPathCompletionContext {
    let (typed_parent, query) = split_typed_path(&token.raw_path);
    let parent_path = if typed_parent.is_empty() {
        PathBuf::new()
    } else if typed_parent == std::path::MAIN_SEPARATOR.to_string() {
        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
    } else if typed_parent == format!("~{}", std::path::MAIN_SEPARATOR) {
        expand_current_user_home(Path::new("~"))
    } else {
        let trimmed = typed_parent.trim_end_matches(std::path::MAIN_SEPARATOR);
        expand_current_user_home(Path::new(trimmed))
    };
    let scan_dir = if parent_path.is_absolute() {
        parent_path
    } else {
        workspace_root.join(parent_path)
    };
    AtPathCompletionContext {
        token,
        scan_dir,
        typed_parent,
        query,
    }
}

/// 异步读取目录的直接子项；不读取文件内容，也不递归进入子目录。
pub(super) async fn read_directory_entries(
    directory: &Path,
    max_entries: usize,
) -> Result<Vec<AtPathDirectoryEntry>, String> {
    let mut reader = fs::read_dir(directory)
        .await
        .map_err(|error| format!("无法读取目录 {}: {error}", directory.display()))?;
    let mut entries = Vec::new();
    while entries.len() < max_entries {
        let Some(entry) = reader
            .next_entry()
            .await
            .map_err(|error| format!("读取目录 {} 失败: {error}", directory.display()))?
        else {
            break;
        };
        let file_type = match entry.file_type().await {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        let kind = if file_type.is_dir() {
            AtPathCandidateKind::Directory
        } else if file_type.is_file() {
            AtPathCandidateKind::File
        } else if file_type.is_symlink() {
            match fs::metadata(entry.path()).await {
                Ok(metadata) if metadata.is_dir() => AtPathCandidateKind::Directory,
                Ok(metadata) if metadata.is_file() => AtPathCandidateKind::File,
                _ => continue,
            }
        } else {
            continue;
        };
        entries.push(AtPathDirectoryEntry {
            file_name: entry.file_name(),
            kind,
            protected: crate::attachment::is_protected_memory_path(&entry.path()),
        });
    }
    Ok(entries)
}

/// 按当前 component 实时过滤候选。目录优先，同类按名称排序。
pub(super) fn matching_candidates(
    entries: &[AtPathDirectoryEntry],
    context: &AtPathCompletionContext,
    max_candidates: usize,
) -> Vec<AtPathCandidate> {
    let query_lower = context.query.to_lowercase();
    let mut candidates = entries
        .iter()
        .filter_map(|entry| {
            if entry.protected {
                return None;
            }
            let file_name = entry.file_name.to_str()?;
            let exact_case_prefix = file_name.starts_with(&context.query);
            let insensitive_prefix = file_name.to_lowercase().starts_with(&query_lower);
            if !exact_case_prefix && !insensitive_prefix {
                return None;
            }
            let raw_path = candidate_raw_path(&context.typed_parent, file_name, entry.kind);
            Some((
                !exact_case_prefix,
                AtPathCandidate {
                    display_path: raw_path.clone(),
                    raw_path,
                    kind: entry.kind,
                },
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(case_rank_a, a), (case_rank_b, b)| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| case_rank_a.cmp(case_rank_b))
            .then_with(|| a.display_path.cmp(&b.display_path))
    });
    candidates
        .into_iter()
        .map(|(_, candidate)| candidate)
        .take(max_candidates)
        .collect()
}

/// 为 composer 生成完整 `@path` 文本；空白字符使用现有 bare-path 反斜杠语法编码。
pub(super) fn encoded_at_path(raw_path: &str) -> String {
    let mut encoded = String::from("@");
    for ch in raw_path.chars() {
        if ch.is_whitespace() {
            encoded.push('\\');
        }
        encoded.push(ch);
    }
    encoded
}

fn split_typed_path(raw_path: &str) -> (String, String) {
    let Some(separator_index) = raw_path.rfind(std::path::MAIN_SEPARATOR) else {
        return (String::new(), raw_path.to_string());
    };
    let parent_end = separator_index.saturating_add(1);
    (
        raw_path[..parent_end].to_string(),
        raw_path[parent_end..].to_string(),
    )
}

fn candidate_raw_path(typed_parent: &str, file_name: &str, kind: AtPathCandidateKind) -> String {
    let mut path = format!("{typed_parent}{file_name}");
    if kind == AtPathCandidateKind::Directory {
        path.push(std::path::MAIN_SEPARATOR);
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, kind: AtPathCandidateKind) -> AtPathDirectoryEntry {
        AtPathDirectoryEntry {
            file_name: OsString::from(name),
            kind,
            protected: false,
        }
    }

    #[test]
    fn default_limits_share_directory_context_cap_and_bound_candidates() {
        assert_eq!(
            AtPathCompletionLimits::default().max_directory_entries,
            crate::config::DEFAULT_TUI_AT_PATH_DIRECTORY_CONTEXT_MAX_ENTRIES
        );
        assert_eq!(
            AtPathCompletionLimits::default().max_candidates,
            crate::config::DEFAULT_TUI_AT_PATH_MAX_CANDIDATES
        );
        assert_eq!(crate::config::DEFAULT_TUI_AT_PATH_MAX_CANDIDATES, 50);
    }

    #[test]
    fn relative_and_absolute_contexts_choose_expected_scan_directory() {
        let workspace = Path::new("/workspace");
        let relative = completion_context(
            ActiveAtPathToken {
                range: 0..7,
                raw_path: "src/se".into(),
            },
            workspace,
        );
        assert_eq!(relative.scan_dir, PathBuf::from("/workspace/src"));
        assert_eq!(relative.typed_parent, "src/");
        assert_eq!(relative.query, "se");

        let absolute = completion_context(
            ActiveAtPathToken {
                range: 0..8,
                raw_path: "/tmp/lo".into(),
            },
            workspace,
        );
        assert_eq!(absolute.scan_dir, PathBuf::from("/tmp/"));
        assert_eq!(absolute.typed_parent, "/tmp/");
        assert_eq!(absolute.query, "lo");
    }

    #[test]
    fn home_relative_context_scans_current_user_home() {
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            return;
        };
        let context = completion_context(
            ActiveAtPathToken {
                range: 0..5,
                raw_path: format!("~{}Do", std::path::MAIN_SEPARATOR),
            },
            Path::new("/workspace"),
        );
        assert_eq!(context.scan_dir, home);
        assert_eq!(
            context.typed_parent,
            format!("~{}", std::path::MAIN_SEPARATOR)
        );
        assert_eq!(context.query, "Do");
    }

    #[test]
    fn candidates_filter_live_and_put_directories_first() {
        let entries = vec![
            entry("session_tui", AtPathCandidateKind::Directory),
            entry("state.rs", AtPathCandidateKind::File),
            entry("README.md", AtPathCandidateKind::File),
            entry(".secret", AtPathCandidateKind::File),
        ];
        let context = AtPathCompletionContext {
            token: ActiveAtPathToken {
                range: 0..6,
                raw_path: "src/s".into(),
            },
            scan_dir: PathBuf::from("/workspace/src"),
            typed_parent: "src/".into(),
            query: "s".into(),
        };
        assert_eq!(
            matching_candidates(&entries, &context, usize::MAX),
            vec![
                AtPathCandidate {
                    display_path: format!("src/session_tui{}", std::path::MAIN_SEPARATOR),
                    raw_path: format!("src/session_tui{}", std::path::MAIN_SEPARATOR),
                    kind: AtPathCandidateKind::Directory,
                },
                AtPathCandidate {
                    display_path: "src/state.rs".into(),
                    raw_path: "src/state.rs".into(),
                    kind: AtPathCandidateKind::File,
                },
            ]
        );
    }

    #[test]
    fn hidden_entries_are_visible_without_explicit_dot_query() {
        let entries = vec![
            entry(".env", AtPathCandidateKind::File),
            entry(".git", AtPathCandidateKind::Directory),
            entry("src", AtPathCandidateKind::Directory),
        ];
        let context = AtPathCompletionContext {
            token: ActiveAtPathToken {
                range: 0..1,
                raw_path: String::new(),
            },
            scan_dir: PathBuf::from("/workspace"),
            typed_parent: String::new(),
            query: String::new(),
        };
        assert_eq!(
            matching_candidates(&entries, &context, usize::MAX)
                .into_iter()
                .map(|candidate| candidate.raw_path)
                .collect::<Vec<_>>(),
            vec![
                format!(".git{}", std::path::MAIN_SEPARATOR),
                format!("src{}", std::path::MAIN_SEPARATOR),
                ".env".to_string(),
            ]
        );
    }

    #[test]
    fn candidates_do_not_filter_by_attachment_file_type() {
        let entries = vec![
            entry("archive.zip", AtPathCandidateKind::File),
            entry("slides.pptx", AtPathCandidateKind::File),
            entry("video.mp4", AtPathCandidateKind::File),
            entry("custom.unknown", AtPathCandidateKind::File),
        ];
        let context = AtPathCompletionContext {
            token: ActiveAtPathToken {
                range: 0..1,
                raw_path: String::new(),
            },
            scan_dir: PathBuf::from("/workspace"),
            typed_parent: String::new(),
            query: String::new(),
        };
        assert_eq!(
            matching_candidates(&entries, &context, usize::MAX)
                .into_iter()
                .map(|candidate| candidate.raw_path)
                .collect::<Vec<_>>(),
            vec!["archive.zip", "custom.unknown", "slides.pptx", "video.mp4"]
        );
    }

    #[test]
    fn protected_memory_files_are_hidden_from_candidates() {
        let mut protected = entry("MEMORY.md", AtPathCandidateKind::File);
        protected.protected = true;
        let context = AtPathCompletionContext {
            token: ActiveAtPathToken {
                range: 0..1,
                raw_path: String::new(),
            },
            scan_dir: PathBuf::from("/workspace/memories"),
            typed_parent: "memories/".into(),
            query: String::new(),
        };
        assert!(matching_candidates(&[protected], &context, usize::MAX).is_empty());
    }

    #[test]
    fn paths_with_spaces_are_escaped_for_insertion() {
        assert_eq!(encoded_at_path("docs/a b.md"), r#"@docs/a\ b.md"#);
        assert_eq!(encoded_at_path("src/lib.rs"), "@src/lib.rs");
        assert_eq!(encoded_at_path(r"dir\file.rs"), r"@dir\file.rs");
    }
}
