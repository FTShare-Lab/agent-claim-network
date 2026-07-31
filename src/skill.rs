//! Skill 模块：扫描 `<acn_home>/skills/` 风格的子目录，加载每个 skill 的元数据。
//!
//! 约定：每个 skill 是 `<skills_root>/<skill_name>/` 下的一个目录，包含一份
//! `SKILL.md`。除了给模型展示的摘要外，本模块还负责解析用户显式输入的
//! `/skill` 引用，并在发送模型前制作不可变的正文快照。
//!
//! 不在本模块范围：skill 调度（agent 决定何时使用）、skill 内部逻辑执行。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use tokio::fs;

/// 与 TUI 原生命令同名的 skill 不会被视为显式 Skill 调用。
///
/// 这里是注入层的安全边界；TUI 菜单还会以自身的原生命令表拦截这些名字。
pub const NATIVE_SLASH_COMMAND_NAMES: &[&str] = &[
    "compact", "copy", "exit", "help", "inbox", "mcp", "ps", "resume", "skills",
];

pub const DEFAULT_SKILL_INJECTION_MAX_BODY_BYTES: usize = 256 * 1024;
pub const DEFAULT_SKILL_INJECTION_MAX_PER_TURN: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub dir: PathBuf,
    pub spec_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSummary {
    pub name: String,
    pub description: String,
    pub spec_path: PathBuf,
}

/// 已从当前可见输入中解析出的显式 Skill 调用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillInvocationReference {
    pub name: String,
    pub spec_path: PathBuf,
    pub arguments: Option<String>,
}

/// 随当前 turn 一起持久化、发送给模型的 Skill 正文快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInstructions {
    pub name: String,
    pub spec_path: PathBuf,
    pub base_dir: PathBuf,
    pub arguments: Option<String>,
    pub content: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillInjectionLimits {
    pub max_body_bytes: usize,
    pub max_per_turn: usize,
}

impl Default for SkillInjectionLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_SKILL_INJECTION_MAX_BODY_BYTES,
            max_per_turn: DEFAULT_SKILL_INJECTION_MAX_PER_TURN,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("skill {name} 的 SKILL.md 不是 UTF-8 文本")]
    NotUtf8 { name: String },
    #[error("skill {name} 正文为 {actual_bytes} bytes，超过单个 skill 上限 {max_bytes} bytes")]
    BodyTooLarge {
        name: String,
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("本次最多显式调用 {max} 个 skill，实际为 {actual}")]
    TooManyInvocations { actual: usize, max: usize },
    #[error("skill {name} 缺少 SKILL.md 所在目录")]
    MissingBaseDir { name: String },
    #[error("skill {name} 的参数无法按 shell 语义解析：{reason}")]
    InvalidArguments { name: String, reason: String },
}

pub struct SkillRegistry {
    root: PathBuf,
}

impl SkillRegistry {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 扫一遍 root，返回所有合法 skill。缺失 root 或缺 SKILL.md 的子目录被忽略，不视为错误。
    pub async fn list(&self) -> Result<Vec<Skill>, SkillError> {
        list_skills_async(&self.root).await
    }

    pub async fn summaries(&self) -> Result<Vec<SkillSummary>, SkillError> {
        let skills = self.list().await?;
        Ok(to_summaries(skills))
    }

    pub fn summaries_sync(&self) -> Result<Vec<SkillSummary>, SkillError> {
        let skills = self.list_sync()?;
        Ok(to_summaries(skills))
    }

    pub fn list_sync(&self) -> Result<Vec<Skill>, SkillError> {
        list_skills_sync(&self.root)
    }
}

fn to_summaries(skills: Vec<Skill>) -> Vec<SkillSummary> {
    skills
        .into_iter()
        .map(|skill| SkillSummary {
            name: skill.name,
            description: skill.description,
            spec_path: skill.spec_path,
        })
        .collect()
}

pub fn is_native_slash_command_name(name: &str) -> bool {
    NATIVE_SLASH_COMMAND_NAMES.contains(&name)
}

/// 从可见 composer 文本找出明确的 `/skill` 引用。
///
/// 只接受行首或空白后的 slash；inline 的名字末尾必须接输入结束、空白或标点。
/// 返回顺序与文本中首次出现的顺序一致，并按 `SKILL.md` 路径去重。
pub fn find_explicit_skill_invocations(
    input: &str,
    skills: &[SkillSummary],
) -> Vec<SkillInvocationReference> {
    #[derive(Debug)]
    struct Candidate<'a> {
        name: &'a str,
        start: usize,
        end: usize,
    }

    let mut candidates = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((slash_index, ch)) = chars.next() {
        if ch != '/' {
            continue;
        }
        let predecessor_is_valid = input[..slash_index]
            .chars()
            .next_back()
            .is_none_or(char::is_whitespace);
        if !predecessor_is_valid {
            continue;
        }
        let name_start = slash_index + ch.len_utf8();
        let mut name_end = name_start;
        while let Some(&(index, next)) = chars.peek() {
            if !is_skill_name_char(next) {
                break;
            }
            name_end = index + next.len_utf8();
            let _ = chars.next();
        }
        if name_start == name_end {
            continue;
        }
        let name = &input[name_start..name_end];
        let valid_token_end = input[name_end..]
            .chars()
            .next()
            .is_none_or(|next| next.is_whitespace() || is_punctuation(next));
        if valid_token_end {
            candidates.push(Candidate {
                name,
                start: slash_index,
                end: name_end,
            });
        }
    }

    let mut seen_paths = HashSet::new();
    let mut references = Vec::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        if is_native_slash_command_name(candidate.name) {
            continue;
        }
        let Some(skill) = skills.iter().find(|skill| skill.name == candidate.name) else {
            continue;
        };
        if !seen_paths.insert(skill.spec_path.clone()) {
            continue;
        }
        let is_leading = input[..candidate.start].trim().is_empty();
        let arguments = if is_leading {
            let next_skill_start = candidates
                .iter()
                .skip(candidate_index + 1)
                .find(|next| skills.iter().any(|skill| skill.name == next.name))
                .map(|next| next.start)
                .unwrap_or(input.len());
            let suffix = input[candidate.end..next_skill_start].trim();
            (!suffix.is_empty()).then(|| suffix.to_owned())
        } else {
            None
        };
        references.push(SkillInvocationReference {
            name: skill.name.clone(),
            spec_path: skill.spec_path.clone(),
            arguments,
        });
    }
    references
}

/// 按 turn 配置读取、展开并冻结当前显式调用的 Skill 正文。
pub async fn resolve_explicit_skill_instructions(
    input: &str,
    skills: &[SkillSummary],
    limits: SkillInjectionLimits,
) -> Result<Vec<SkillInstructions>, SkillError> {
    let references = find_explicit_skill_invocations(input, skills);
    if references.len() > limits.max_per_turn {
        return Err(SkillError::TooManyInvocations {
            actual: references.len(),
            max: limits.max_per_turn,
        });
    }

    let mut resolved = Vec::with_capacity(references.len());
    for reference in references {
        let spec_path = fs::canonicalize(&reference.spec_path).await?;
        let bytes = fs::read(&spec_path).await?;
        if bytes.len() > limits.max_body_bytes {
            return Err(SkillError::BodyTooLarge {
                name: reference.name,
                actual_bytes: bytes.len(),
                max_bytes: limits.max_body_bytes,
            });
        }
        let raw = String::from_utf8(bytes).map_err(|_| SkillError::NotUtf8 {
            name: reference.name.clone(),
        })?;
        let Some(base_dir) = spec_path.parent().map(Path::to_path_buf) else {
            return Err(SkillError::MissingBaseDir {
                name: reference.name,
            });
        };
        let content = render_skill_template(
            &reference.name,
            &raw,
            reference.arguments.as_deref(),
            &base_dir,
        )?;
        let content_hash = hex::encode(digest(&SHA256, content.as_bytes()).as_ref());
        resolved.push(SkillInstructions {
            name: reference.name,
            spec_path,
            base_dir,
            arguments: reference.arguments,
            content,
            content_hash,
        });
    }
    Ok(resolved)
}

/// 把 Skill 正文编码为发给模型的结构化文本块。正文原样保留，只有属性会 XML 转义。
pub fn render_skill_instructions(instructions: &SkillInstructions) -> String {
    format!(
        "<acn_skill scope=\"current_user_turn\" name=\"{}\" spec_path=\"{}\" base_dir=\"{}\" arguments=\"{}\" content_sha256=\"{}\">\n{}\n</acn_skill>",
        escape_xml_metadata(&instructions.name),
        escape_xml_metadata(&instructions.spec_path.display().to_string()),
        escape_xml_metadata(&instructions.base_dir.display().to_string()),
        escape_xml_metadata(instructions.arguments.as_deref().unwrap_or_default()),
        instructions.content_hash,
        instructions.content,
    )
}

fn render_skill_template(
    name: &str,
    raw: &str,
    arguments: Option<&str>,
    base_dir: &Path,
) -> Result<String, SkillError> {
    let Some(arguments) = arguments else {
        return Ok(raw.to_owned());
    };
    let tokens = shlex::split(arguments).ok_or_else(|| SkillError::InvalidArguments {
        name: name.to_owned(),
        reason: "存在未闭合的引号或转义".to_owned(),
    })?;
    let has_argument_placeholder = raw.contains("$ARGUMENTS")
        || raw.contains("$0")
        || raw.contains("$1")
        || (0..tokens.len()).any(|index| raw.contains(&format!("$ARGUMENTS[{index}]")));
    let mut rendered = raw.to_owned();
    for (index, token) in tokens.iter().enumerate() {
        rendered = rendered.replace(&format!("$ARGUMENTS[{index}]"), token);
        rendered = rendered.replace(&format!("${index}"), token);
    }
    rendered = rendered.replace("$ARGUMENTS", arguments);
    rendered = rendered.replace("${ACN_SKILL_DIR}", &base_dir.display().to_string());
    if !has_argument_placeholder && !arguments.is_empty() {
        if !rendered.ends_with('\n') {
            rendered.push('\n');
        }
        rendered.push_str("\nARGUMENTS: ");
        rendered.push_str(arguments);
    }
    Ok(rendered)
}

fn is_skill_name_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')
}

fn is_punctuation(ch: char) -> bool {
    ch.is_ascii_punctuation()
        || matches!(
            ch,
            '，' | '。' | '！' | '？' | '；' | '：' | '、' | '）' | '】' | '》'
        )
}

fn escape_xml_metadata(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

async fn list_skills_async(root: &Path) -> Result<Vec<Skill>, SkillError> {
    if !fs::try_exists(root).await.unwrap_or(false) {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    let mut rd = fs::read_dir(root).await?;
    while let Some(entry) = rd.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let dir = entry.path();
        let spec_path = dir.join("SKILL.md");
        if !fs::try_exists(&spec_path).await.unwrap_or(false) {
            continue;
        }
        let raw = fs::read_to_string(&spec_path).await?;
        let description = first_meaningful_line(&raw).unwrap_or_default();
        out.push(Skill {
            name,
            description,
            dir,
            spec_path,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn list_skills_sync(root: &Path) -> Result<Vec<Skill>, SkillError> {
    if !root.exists() {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let dir = entry.path();
        let spec_path = dir.join("SKILL.md");
        if !spec_path.exists() {
            continue;
        }
        let raw = std::fs::read_to_string(&spec_path)?;
        let description = first_meaningful_line(&raw).unwrap_or_default();
        out.push(Skill {
            name,
            description,
            dir,
            spec_path,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

#[derive(Debug, Default, Deserialize)]
struct SkillFrontmatter {
    description: Option<String>,
}

fn frontmatter_description(s: &str) -> Option<String> {
    let mut lines = s.lines();
    if lines.next()?.trim() != "---" {
        return None;
    }
    let mut yaml = String::new();
    for line in lines {
        if line.trim() == "---" {
            let frontmatter: SkillFrontmatter = serde_yaml_ng::from_str(&yaml).ok()?;
            return frontmatter
                .description
                .map(|description| description.trim().to_owned())
                .filter(|description| !description.is_empty());
        }
        yaml.push_str(line);
        yaml.push('\n');
    }
    None
}

/// 优先读取 frontmatter.description；否则取第一段非空、非纯 `#` 标题前缀的行。
fn first_meaningful_line(s: &str) -> Option<String> {
    if let Some(description) = frontmatter_description(s) {
        return Some(description);
    }
    let mut in_frontmatter = s.lines().next().map(str::trim) == Some("---");
    for line in s.lines().skip(usize::from(in_frontmatter)) {
        if in_frontmatter {
            if line.trim() == "---" {
                in_frontmatter = false;
            }
            continue;
        }
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let cleaned = t.trim_start_matches('#').trim();
        if !cleaned.is_empty() {
            return Some(cleaned.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn write(p: &Path, content: &str) {
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(p, content).await.unwrap();
    }

    #[tokio::test]
    async fn missing_root_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let r = SkillRegistry::new(dir.path().join("nonexistent"));
        assert!(r.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn lists_skills_with_description() {
        let dir = tempfile::tempdir().unwrap();
        write(
            &dir.path().join("alpha").join("SKILL.md"),
            "# alpha skill\n\nbody",
        )
        .await;
        write(&dir.path().join("beta").join("SKILL.md"), "beta one-liner").await;
        // 没有 SKILL.md 的目录，应被忽略
        fs::create_dir_all(dir.path().join("noskill"))
            .await
            .unwrap();

        let r = SkillRegistry::new(dir.path().to_path_buf());
        let skills = r.list().await.unwrap();
        assert_eq!(skills.len(), 2);
        assert_eq!(skills[0].name, "alpha");
        assert_eq!(skills[0].description, "alpha skill");
        assert_eq!(skills[1].name, "beta");
        assert_eq!(skills[1].description, "beta one-liner");
    }

    #[tokio::test]
    async fn summaries_keep_skill_path_and_description() {
        let dir = tempfile::tempdir().unwrap();
        write(
            &dir.path().join("verify").join("SKILL.md"),
            "# verify skill\n\nrun checks",
        )
        .await;

        let r = SkillRegistry::new(dir.path().to_path_buf());
        let summaries = r.summaries().await.unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].name, "verify");
        assert_eq!(summaries[0].description, "verify skill");
        assert_eq!(
            summaries[0].spec_path,
            dir.path().join("verify").join("SKILL.md")
        );
    }

    #[tokio::test]
    async fn frontmatter_description_is_used_as_summary() {
        let dir = tempfile::tempdir().unwrap();
        write(
            &dir.path().join("verify").join("SKILL.md"),
            r#"---
name: verify
description: Run full project verification
---

# Verify

body
"#,
        )
        .await;

        let r = SkillRegistry::new(dir.path().to_path_buf());
        let summaries = r.summaries().await.unwrap();
        assert_eq!(summaries[0].description, "Run full project verification");
    }

    #[test]
    fn explicit_invocations_only_match_registered_skills_at_token_boundaries() {
        let skills = vec![
            SkillSummary {
                name: "review".into(),
                description: String::new(),
                spec_path: PathBuf::from("/skills/review/SKILL.md"),
            },
            SkillSummary {
                name: "verify".into(),
                description: String::new(),
                spec_path: PathBuf::from("/skills/verify/SKILL.md"),
            },
            SkillSummary {
                name: "compact".into(),
                description: String::new(),
                spec_path: PathBuf::from("/skills/compact/SKILL.md"),
            },
        ];

        let invocations = find_explicit_skill_invocations(
            "先 /review，再 /verify。/review 不重复；hihi/review /compact /unknown /reviewing",
            &skills,
        );

        assert_eq!(
            invocations
                .iter()
                .map(|invocation| invocation.name.as_str())
                .collect::<Vec<_>>(),
            vec!["review", "verify"]
        );
        assert!(invocations
            .iter()
            .all(|invocation| invocation.arguments.is_none()));
    }

    #[tokio::test]
    async fn leading_arguments_expand_templates_and_snapshot_the_body() {
        let dir = tempfile::tempdir().unwrap();
        let spec_path = dir.path().join("review").join("SKILL.md");
        write(
            &spec_path,
            "all=$ARGUMENTS\nfirst=$ARGUMENTS[0]\nzero=$0\none=$1\ndir=${ACN_SKILL_DIR}",
        )
        .await;
        let skills = vec![SkillSummary {
            name: "review".into(),
            description: String::new(),
            spec_path,
        }];

        let resolved = resolve_explicit_skill_instructions(
            "/review 'src/auth rs' --strict",
            &skills,
            SkillInjectionLimits::default(),
        )
        .await
        .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved[0].arguments.as_deref(),
            Some("'src/auth rs' --strict")
        );
        assert!(resolved[0].content.contains("all='src/auth rs' --strict"));
        assert!(resolved[0].content.contains("first=src/auth rs"));
        assert!(resolved[0].content.contains("zero=src/auth rs"));
        assert!(resolved[0].content.contains("one=--strict"));
        let canonical_skill_dir = fs::canonicalize(dir.path().join("review")).await.unwrap();
        assert!(resolved[0]
            .content
            .contains(&format!("dir={}", canonical_skill_dir.display())));
        assert!(render_skill_instructions(&resolved[0]).contains("scope=\"current_user_turn\""));
    }

    #[tokio::test]
    async fn visible_input_deduplicates_and_inline_invocations_do_not_receive_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let review_path = dir.path().join("review").join("SKILL.md");
        let verify_path = dir.path().join("verify").join("SKILL.md");
        write(&review_path, "review").await;
        write(&verify_path, "verify").await;
        let skills = vec![
            SkillSummary {
                name: "review".into(),
                description: String::new(),
                spec_path: review_path,
            },
            SkillSummary {
                name: "verify".into(),
                description: String::new(),
                spec_path: verify_path,
            },
        ];

        let resolved = resolve_explicit_skill_instructions(
            "请先 /review strict，再 /verify；最后 /review",
            &skills,
            SkillInjectionLimits::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|instruction| instruction.name.as_str())
                .collect::<Vec<_>>(),
            vec!["review", "verify"]
        );
        assert!(resolved
            .iter()
            .all(|instruction| instruction.arguments.is_none()));
    }
}
