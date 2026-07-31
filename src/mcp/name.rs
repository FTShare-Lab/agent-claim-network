//! MCP 工具可见名生成。
//!
//! 模型看到的 MCP 工具名统一是 `mcp__server__tool`，真实 server/tool 名通过
//! 独立映射保存。这里集中处理归一化、冲突消解和前缀判断，避免路由层暗猜名称。

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

pub const MCP_TOOL_PREFIX: &str = "mcp__";
pub const MAX_VISIBLE_TOOL_NAME_CHARS: usize = 64;
const SHORT_HASH_CHARS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpVisibleToolName {
    pub visible_name: String,
    pub server_name: String,
    pub raw_tool_name: String,
}

pub fn is_mcp_visible_tool_name(name: &str) -> bool {
    name.starts_with(MCP_TOOL_PREFIX)
}

pub fn parse_mcp_visible_tool_name(name: &str) -> Option<(&str, &str)> {
    let remainder = name.strip_prefix(MCP_TOOL_PREFIX)?;
    let (server, tool) = remainder.split_once("__")?;
    if server.is_empty() || tool.is_empty() {
        return None;
    }
    Some((server, tool))
}

pub fn normalize_visible_component(raw: &str) -> String {
    let normalized = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        "_".to_string()
    } else {
        normalized
    }
}

pub fn visible_name_base(server_name: &str, raw_tool_name: &str) -> String {
    let server = normalize_visible_component(server_name);
    let tool = normalize_visible_component(raw_tool_name);
    let base = format!("{MCP_TOOL_PREFIX}{server}__{tool}");
    if base.chars().count() <= MAX_VISIBLE_TOOL_NAME_CHARS {
        return base;
    }
    bounded_hashed_name(&server, &tool, &short_hash(server_name, raw_tool_name))
}

fn bounded_hashed_name(server: &str, tool: &str, hash: &str) -> String {
    let reserved = MCP_TOOL_PREFIX
        .len()
        .saturating_add("__".len())
        .saturating_add("__".len())
        .saturating_add(hash.len());
    let budget = MAX_VISIBLE_TOOL_NAME_CHARS.saturating_sub(reserved).max(2);
    let server_budget = server.chars().count().min((budget / 2).max(1));
    let tool_budget = budget.saturating_sub(server_budget).max(1);
    format!(
        "{MCP_TOOL_PREFIX}{}__{}",
        truncate_chars(server, server_budget),
        truncate_chars(tool, tool_budget)
    ) + "__"
        + hash
}

fn append_suffix_bounded(base: &str, suffix: &str) -> String {
    let reserved = "__".len().saturating_add(suffix.len());
    let base_budget = MAX_VISIBLE_TOOL_NAME_CHARS.saturating_sub(reserved).max(1);
    format!("{}__{suffix}", truncate_chars(base, base_budget))
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn visible_name_len(value: &str) -> usize {
    value.chars().count()
}

fn debug_assert_visible_name_len(value: &str) {
    debug_assert!(
        visible_name_len(value) <= MAX_VISIBLE_TOOL_NAME_CHARS,
        "MCP visible tool name exceeds provider-safe length: {value}"
    )
}

pub fn build_visible_tool_names(pairs: &[(String, String)]) -> Vec<McpVisibleToolName> {
    let mut base_counts = BTreeMap::<String, usize>::new();
    for (server_name, raw_tool_name) in pairs {
        let base = visible_name_base(server_name, raw_tool_name);
        *base_counts.entry(base).or_default() += 1;
    }

    let mut used = BTreeSet::<String>::new();
    pairs
        .iter()
        .map(|(server_name, raw_tool_name)| {
            let base = visible_name_base(server_name, raw_tool_name);
            let visible_name =
                if base_counts.get(&base).copied().unwrap_or(0) <= 1 && !used.contains(&base) {
                    base
                } else {
                    unique_hashed_name(&base, server_name, raw_tool_name, &used)
                };
            used.insert(visible_name.clone());
            McpVisibleToolName {
                visible_name,
                server_name: server_name.clone(),
                raw_tool_name: raw_tool_name.clone(),
            }
        })
        .collect()
}

fn unique_hashed_name(
    base: &str,
    server_name: &str,
    raw_tool_name: &str,
    used: &BTreeSet<String>,
) -> String {
    let hash = short_hash(server_name, raw_tool_name);
    let mut candidate = append_suffix_bounded(base, &hash);
    debug_assert_visible_name_len(&candidate);
    if !used.contains(&candidate) {
        return candidate;
    }
    let mut suffix = 2usize;
    while used.contains(&candidate) {
        candidate = append_suffix_bounded(base, &format!("{hash}_{suffix}"));
        debug_assert_visible_name_len(&candidate);
        suffix = suffix.saturating_add(1);
    }
    candidate
}

fn short_hash(server_name: &str, raw_tool_name: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    server_name.hash(&mut hasher);
    "\0".hash(&mut hasher);
    raw_tool_name.hash(&mut hasher);
    let full = format!("{:016x}", hasher.finish());
    full.chars().take(SHORT_HASH_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_visible_component_replaces_invalid_chars() {
        assert_eq!(
            normalize_visible_component("list.issues/v2"),
            "list_issues_v2"
        );
        assert_eq!(normalize_visible_component(""), "_");
    }

    #[test]
    fn build_visible_tool_names_adds_hash_for_collisions() {
        let names = build_visible_tool_names(&[
            ("linear".to_string(), "list.issues".to_string()),
            ("linear".to_string(), "list/issues".to_string()),
        ]);

        assert_ne!(names[0].visible_name, names[1].visible_name);
        assert!(names[0]
            .visible_name
            .starts_with("mcp__linear__list_issues__"));
        assert!(names[1]
            .visible_name
            .starts_with("mcp__linear__list_issues__"));
    }

    #[test]
    fn visible_name_base_caps_long_names_with_hash() {
        let server = "server_".repeat(20);
        let tool = "tool_".repeat(30);

        let name = visible_name_base(&server, &tool);

        assert!(name.starts_with(MCP_TOOL_PREFIX));
        assert!(name.chars().count() <= MAX_VISIBLE_TOOL_NAME_CHARS);
        assert_eq!(name.rsplit("__").next().unwrap().len(), SHORT_HASH_CHARS);
    }

    #[test]
    fn collision_suffix_keeps_name_under_limit() {
        let server = "linear".to_string();
        let long_a = format!("{}a.b", "tool_".repeat(20));
        let long_b = format!("{}a/b", "tool_".repeat(20));

        let names = build_visible_tool_names(&[(server.clone(), long_a), (server, long_b)]);

        assert_ne!(names[0].visible_name, names[1].visible_name);
        assert!(names
            .iter()
            .all(|name| name.visible_name.chars().count() <= MAX_VISIBLE_TOOL_NAME_CHARS));
    }

    #[test]
    fn parse_mcp_visible_tool_name_extracts_normalized_parts() {
        assert_eq!(
            parse_mcp_visible_tool_name("mcp__pal__ask"),
            Some(("pal", "ask"))
        );
        assert!(parse_mcp_visible_tool_name("file_read").is_none());
    }
}
