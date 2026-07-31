//! Markdown 双文件 memory 存储的领域类型与纯操作。
//!
//! MEMORY.md 与 USER.md 使用相同的 op 语义，但容量上限不同。
//! 本模块只处理文本层面的 add/replace/remove、条目去重、容量校验与 prompt 渲染；具体文件 I/O 由 agent 侧实现。

use serde::{Deserialize, Serialize};

const ENTRY_DELIMITER: &str = "\n§\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryTarget {
    Memory,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MemoryOp {
    Add {
        target: MemoryTarget,
        entry: String,
    },
    Replace {
        target: MemoryTarget,
        old_text: String,
        new_text: String,
    },
    Remove {
        target: MemoryTarget,
        old_text: String,
    },
}

impl MemoryOp {
    pub fn target(&self) -> MemoryTarget {
        match self {
            Self::Add { target, .. }
            | Self::Replace { target, .. }
            | Self::Remove { target, .. } => *target,
        }
    }

    fn new_text(&self) -> Option<&str> {
        match self {
            Self::Add { entry, .. } => Some(entry),
            Self::Replace { new_text, .. } => Some(new_text),
            Self::Remove { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryUsage {
    pub chars: usize,
    pub cap_chars: usize,
    pub percent: usize,
    pub entry_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryApplyReport {
    pub memory_chars: usize,
    pub memory_cap_chars: usize,
    pub memory_percent: usize,
    pub memory_entry_count: usize,
    pub user_chars: usize,
    pub user_cap_chars: usize,
    pub user_percent: usize,
    pub user_entry_count: usize,
    #[serde(default)]
    pub target: Option<MemoryTarget>,
    #[serde(default)]
    pub target_entries: Vec<String>,
    #[serde(default)]
    pub no_op: bool,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySnapshot {
    pub memory_text: String,
    pub user_text: String,
    pub memory_entries: Vec<String>,
    pub user_entries: Vec<String>,
    pub memory_usage: MemoryUsage,
    pub user_usage: MemoryUsage,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum MemoryError {
    #[error(
        "{target} memory 容量超限: current/cap chars = {current}/{cap}, need free {need_free}"
    )]
    CapacityExceeded {
        target: &'static str,
        current: usize,
        cap: usize,
        need_free: usize,
        current_entries: Vec<String>,
    },
    #[error("{target} memory substring 匹配失败: matches={matches}, needle={needle:?}")]
    AmbiguousSubstring {
        target: &'static str,
        matches: usize,
        needle: String,
    },
    #[error("{target} memory entry 包含保留分隔符 §，请改写为不含该字符的长期记忆")]
    ReservedDelimiter { target: &'static str },
    #[error("{target} memory entry 不能为空")]
    EmptyEntry { target: &'static str },
}

pub fn snapshot_texts(
    memory: &str,
    user: &str,
    memory_cap_chars: usize,
    user_cap_chars: usize,
) -> MemorySnapshot {
    let memory_entries = parse_entries(memory);
    let user_entries = parse_entries(user);
    let memory_text = render_entries(&memory_entries);
    let user_text = render_entries(&user_entries);
    let memory_usage = usage_for_entries(&memory_entries, memory_cap_chars);
    let user_usage = usage_for_entries(&user_entries, user_cap_chars);
    MemorySnapshot {
        memory_text,
        user_text,
        memory_entries,
        user_entries,
        memory_usage,
        user_usage,
    }
}

pub fn render_prompt_block(target: MemoryTarget, entries: &[String], cap_chars: usize) -> String {
    if entries.is_empty() {
        return String::new();
    }
    let usage = usage_for_entries(entries, cap_chars);
    let header = match target {
        MemoryTarget::Memory => format!(
            "MEMORY (your personal notes) [{}% — {}/{} chars]",
            usage.percent,
            format_count(usage.chars),
            format_count(usage.cap_chars)
        ),
        MemoryTarget::User => format!(
            "USER PROFILE (who the user is) [{}% — {}/{} chars]",
            usage.percent,
            format_count(usage.chars),
            format_count(usage.cap_chars)
        ),
    };
    let separator = "═".repeat(46);
    format!(
        "{separator}\n{header}\n{separator}\n{}",
        render_entries(entries)
    )
}

pub fn apply_ops_to_texts(
    memory: String,
    user: String,
    memory_cap_chars: usize,
    user_cap_chars: usize,
    ops: &[MemoryOp],
) -> Result<(String, String, MemoryApplyReport), MemoryError> {
    let mut memory_entries = parse_entries(&memory);
    let mut user_entries = parse_entries(&user);
    let original_memory = render_entries(&memory_entries);
    let original_user = render_entries(&user_entries);
    let mut target = None;
    let mut message = None;

    for op in ops {
        ensure_no_reserved_delimiter(op.target(), op.new_text())?;
        target = Some(op.target());
        match op {
            MemoryOp::Add { target, entry } => {
                let trimmed = trim_entry(*target, entry)?;
                let entries = target_entries_mut(*target, &mut memory_entries, &mut user_entries);
                if entries.iter().any(|existing| existing == trimmed) {
                    message = Some("Entry already exists (no duplicate added).".to_string());
                } else {
                    entries.push(trimmed.to_string());
                    message = Some("Entry added.".to_string());
                }
            }
            MemoryOp::Replace {
                target,
                old_text,
                new_text,
            } => {
                let trimmed = trim_entry(*target, new_text)?;
                let entries = target_entries_mut(*target, &mut memory_entries, &mut user_entries);
                replace_unique(entries, *target, old_text, trimmed)?;
                message = Some("Entry replaced.".to_string());
            }
            MemoryOp::Remove { target, old_text } => {
                let entries = target_entries_mut(*target, &mut memory_entries, &mut user_entries);
                remove_unique(entries, *target, old_text)?;
                message = Some("Entry removed.".to_string());
            }
        }
    }

    dedupe_entries(&mut memory_entries);
    dedupe_entries(&mut user_entries);

    ensure_capacity(MemoryTarget::Memory, &memory_entries, memory_cap_chars)?;
    ensure_capacity(MemoryTarget::User, &user_entries, user_cap_chars)?;

    let next_memory = render_entries(&memory_entries);
    let next_user = render_entries(&user_entries);
    let no_op = next_memory == original_memory && next_user == original_user;
    let target_entries = target
        .map(|target| target_entries(target, &memory_entries, &user_entries).to_vec())
        .unwrap_or_default();
    let memory_usage = usage_for_entries(&memory_entries, memory_cap_chars);
    let user_usage = usage_for_entries(&user_entries, user_cap_chars);
    let report = MemoryApplyReport {
        memory_chars: memory_usage.chars,
        memory_cap_chars: memory_usage.cap_chars,
        memory_percent: memory_usage.percent,
        memory_entry_count: memory_usage.entry_count,
        user_chars: user_usage.chars,
        user_cap_chars: user_usage.cap_chars,
        user_percent: user_usage.percent,
        user_entry_count: user_usage.entry_count,
        target,
        target_entries,
        no_op,
        message,
    };
    Ok((next_memory, next_user, report))
}

fn parse_entries(text: &str) -> Vec<String> {
    let mut entries = Vec::new();
    for raw in text.split('§') {
        let trimmed = raw.trim();
        if !trimmed.is_empty() && !entries.iter().any(|entry| entry == trimmed) {
            entries.push(trimmed.to_string());
        }
    }
    entries
}

fn render_entries(entries: &[String]) -> String {
    entries.join(ENTRY_DELIMITER)
}

fn dedupe_entries(entries: &mut Vec<String>) {
    let mut deduped = Vec::with_capacity(entries.len());
    for entry in entries.drain(..) {
        if !deduped.iter().any(|existing| existing == &entry) {
            deduped.push(entry);
        }
    }
    *entries = deduped;
}

fn usage_for_entries(entries: &[String], cap_chars: usize) -> MemoryUsage {
    let chars = char_count(&render_entries(entries));
    let percent = if cap_chars > 0 {
        ((chars.saturating_mul(100)) / cap_chars).min(100)
    } else {
        0
    };
    MemoryUsage {
        chars,
        cap_chars,
        percent,
        entry_count: entries.len(),
    }
}

fn format_count(value: usize) -> String {
    let raw = value.to_string();
    let mut out = String::with_capacity(raw.len() + raw.len() / 3);
    let first_group = raw.len() % 3;
    for (idx, ch) in raw.chars().enumerate() {
        if idx > 0 && (idx % 3) == first_group {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn ensure_no_reserved_delimiter(
    target: MemoryTarget,
    text: Option<&str>,
) -> Result<(), MemoryError> {
    if text.is_some_and(|text| text.contains('§')) {
        return Err(MemoryError::ReservedDelimiter {
            target: target_name(target),
        });
    }
    Ok(())
}

fn trim_entry(target: MemoryTarget, entry: &str) -> Result<&str, MemoryError> {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return Err(MemoryError::EmptyEntry {
            target: target_name(target),
        });
    }
    Ok(trimmed)
}

fn target_entries_mut<'a>(
    target: MemoryTarget,
    memory: &'a mut Vec<String>,
    user: &'a mut Vec<String>,
) -> &'a mut Vec<String> {
    match target {
        MemoryTarget::Memory => memory,
        MemoryTarget::User => user,
    }
}

fn target_entries<'a>(
    target: MemoryTarget,
    memory: &'a [String],
    user: &'a [String],
) -> &'a [String] {
    match target {
        MemoryTarget::Memory => memory,
        MemoryTarget::User => user,
    }
}

fn replace_unique(
    entries: &mut [String],
    target: MemoryTarget,
    old_text: &str,
    new_text: &str,
) -> Result<(), MemoryError> {
    let idx = unique_match_index(entries, target, old_text)?;
    entries[idx] = new_text.to_string();
    Ok(())
}

fn remove_unique(
    entries: &mut Vec<String>,
    target: MemoryTarget,
    old_text: &str,
) -> Result<(), MemoryError> {
    let idx = unique_match_index(entries, target, old_text)?;
    entries.remove(idx);
    Ok(())
}

fn unique_match_index(
    entries: &[String],
    target: MemoryTarget,
    needle: &str,
) -> Result<usize, MemoryError> {
    let needle = needle.trim();
    if needle.is_empty() {
        return Err(MemoryError::AmbiguousSubstring {
            target: target_name(target),
            matches: 0,
            needle: needle.to_string(),
        });
    }
    let matches: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.contains(needle))
        .collect();
    match matches.as_slice() {
        [] => Err(MemoryError::AmbiguousSubstring {
            target: target_name(target),
            matches: 0,
            needle: needle.to_string(),
        }),
        [(idx, _)] => Ok(*idx),
        many => {
            let first = many[0].1;
            if many.iter().all(|(_, entry)| entry == &first) {
                Ok(many[0].0)
            } else {
                Err(MemoryError::AmbiguousSubstring {
                    target: target_name(target),
                    matches: many.len(),
                    needle: needle.to_string(),
                })
            }
        }
    }
}

fn ensure_capacity(
    target: MemoryTarget,
    entries: &[String],
    cap: usize,
) -> Result<(), MemoryError> {
    let current = char_count(&render_entries(entries));
    if current > cap {
        return Err(MemoryError::CapacityExceeded {
            target: target_name(target),
            current,
            cap,
            need_free: current - cap,
            current_entries: entries.to_vec(),
        });
    }
    Ok(())
}

fn char_count(text: &str) -> usize {
    text.chars().count()
}

fn target_name(target: MemoryTarget) -> &'static str {
    match target {
        MemoryTarget::Memory => "MEMORY",
        MemoryTarget::User => "USER",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_appends_delimited_entry() {
        let (memory, user, report) = apply_ops_to_texts(
            "old".into(),
            String::new(),
            100,
            100,
            &[MemoryOp::Add {
                target: MemoryTarget::Memory,
                entry: "new".into(),
            }],
        )
        .unwrap();
        assert_eq!(memory, "old\n§\nnew");
        assert!(user.is_empty());
        assert_eq!(report.memory_chars, 9);
        assert_eq!(report.target_entries, vec!["old", "new"]);
    }

    #[test]
    fn snapshot_dedupes_entries_without_requiring_writeback() {
        let snapshot = snapshot_texts("old\n§\nold\n§\nnew", "u\n§\nu", 100, 100);
        assert_eq!(snapshot.memory_text, "old\n§\nnew");
        assert_eq!(snapshot.user_text, "u");
        assert_eq!(snapshot.memory_entries, vec!["old", "new"]);
        assert_eq!(snapshot.user_entries, vec!["u"]);
    }

    #[test]
    fn add_duplicate_is_success_noop() {
        let (memory, _user, report) = apply_ops_to_texts(
            "old\n§\nold".into(),
            String::new(),
            100,
            100,
            &[MemoryOp::Add {
                target: MemoryTarget::Memory,
                entry: "old".into(),
            }],
        )
        .unwrap();
        assert_eq!(memory, "old");
        assert!(report.no_op);
        assert_eq!(
            report.message.as_deref(),
            Some("Entry already exists (no duplicate added).")
        );
    }

    #[test]
    fn duplicate_add_in_batch_does_not_hide_later_change() {
        let (memory, _user, report) = apply_ops_to_texts(
            "old".into(),
            String::new(),
            100,
            100,
            &[
                MemoryOp::Add {
                    target: MemoryTarget::Memory,
                    entry: "old".into(),
                },
                MemoryOp::Add {
                    target: MemoryTarget::Memory,
                    entry: "new".into(),
                },
            ],
        )
        .unwrap();
        assert_eq!(memory, "old\n§\nnew");
        assert!(!report.no_op);
    }

    #[test]
    fn replace_to_existing_entry_dedupes_result() {
        let (memory, _user, report) = apply_ops_to_texts(
            "a\n§\nb".into(),
            String::new(),
            100,
            100,
            &[MemoryOp::Replace {
                target: MemoryTarget::Memory,
                old_text: "b".into(),
                new_text: "a".into(),
            }],
        )
        .unwrap();
        assert_eq!(memory, "a");
        assert_eq!(report.memory_entry_count, 1);
    }

    #[test]
    fn replace_requires_unique_substring() {
        let err = apply_ops_to_texts(
            "same one\n§\nsame two".into(),
            String::new(),
            100,
            100,
            &[MemoryOp::Replace {
                target: MemoryTarget::Memory,
                old_text: "same".into(),
                new_text: "x".into(),
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            MemoryError::AmbiguousSubstring { matches: 2, .. }
        ));
    }

    #[test]
    fn remove_requires_existing_substring() {
        let err = apply_ops_to_texts(
            "old".into(),
            String::new(),
            100,
            100,
            &[MemoryOp::Remove {
                target: MemoryTarget::Memory,
                old_text: "missing".into(),
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            MemoryError::AmbiguousSubstring { matches: 0, .. }
        ));
    }

    #[test]
    fn capacity_error_returns_current_entries() {
        let err = apply_ops_to_texts(
            String::new(),
            String::new(),
            5,
            100,
            &[MemoryOp::Add {
                target: MemoryTarget::Memory,
                entry: "abcdef".into(),
            }],
        )
        .unwrap_err();
        match err {
            MemoryError::CapacityExceeded {
                current_entries, ..
            } => assert_eq!(current_entries, vec!["abcdef"]),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn add_rejects_reserved_delimiter_inside_entry() {
        let err = apply_ops_to_texts(
            String::new(),
            String::new(),
            100,
            100,
            &[MemoryOp::Add {
                target: MemoryTarget::Memory,
                entry: "bad § split".into(),
            }],
        )
        .unwrap_err();
        assert!(matches!(err, MemoryError::ReservedDelimiter { .. }));
    }

    #[test]
    fn replace_rejects_reserved_delimiter_inside_new_text() {
        let err = apply_ops_to_texts(
            "old".into(),
            String::new(),
            100,
            100,
            &[MemoryOp::Replace {
                target: MemoryTarget::Memory,
                old_text: "old".into(),
                new_text: "new § split".into(),
            }],
        )
        .unwrap_err();
        assert!(matches!(err, MemoryError::ReservedDelimiter { .. }));
    }

    #[test]
    fn prompt_block_renders_capacity_and_entries() {
        let block = render_prompt_block(MemoryTarget::Memory, &["one".into(), "two".into()], 100);
        assert_eq!(
            block,
            "══════════════════════════════════════════════\nMEMORY (your personal notes) [9% — 9/100 chars]\n══════════════════════════════════════════════\none\n§\ntwo"
        );
    }
}
