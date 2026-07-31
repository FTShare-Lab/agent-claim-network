//! `code_run` 的 Bash 并发资格分类。
//!
//! 本模块只回答“这次脚本能否与相邻只读工具并发”，不改变 `bash -lc` 的实际 runner，也不把
//! 静态检查当作沙箱。解析失败或任何未明确允许的 AST 结构都 fail-closed。

use tree_sitter::{Node, Parser};

#[derive(Debug, Clone)]
struct BashLiteral {
    value: String,
}

#[derive(Debug, Clone, Copy)]
enum OptionValueKind {
    Literal,
    Decimal,
}

/// 以 tree-sitter Bash AST 和 PRD 白名单判断脚本是否可并发执行。
pub(crate) fn bash_script_is_concurrency_safe(script: &str) -> bool {
    let mut parser = Parser::new();
    let language = tree_sitter_bash::LANGUAGE;
    if parser.set_language(&language.into()).is_err() {
        return false;
    }
    let Some(tree) = parser.parse(script, None) else {
        return false;
    };
    let root = tree.root_node();
    !root.has_error() && !root.is_missing() && is_safe_script_node(root, script)
}

fn is_safe_script_node(node: Node<'_>, source: &str) -> bool {
    match node.kind() {
        "program" => is_safe_program(node, source),
        "list" => is_safe_list(node, source),
        "pipeline" => is_safe_pipeline(node, source),
        "command" => is_safe_command(node, source),
        "redirected_statement" => is_safe_redirected_statement(node, source),
        _ => false,
    }
}

fn is_safe_program(node: Node<'_>, source: &str) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.is_named() {
            if !is_safe_script_node(child, source) {
                return false;
            }
        } else if child.kind() != ";" {
            // 换行属于 Bash grammar 的 extra，不会作为 child 出现；`&` 等其他终止符必须拒绝。
            return false;
        }
    }
    true
}

fn is_safe_list(node: Node<'_>, source: &str) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.is_named() {
            if !is_safe_script_node(child, source) {
                return false;
            }
        } else if !matches!(child.kind(), "&&" | "||") {
            return false;
        }
    }
    true
}

fn is_safe_pipeline(node: Node<'_>, source: &str) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.is_named() {
            // 管道的每一段都必须是直接的 simple command；嵌套 list/subshell 均拒绝。
            if child.kind() != "command" || !is_safe_command(child, source) {
                return false;
            }
        } else if child.kind() != "|" {
            // `|&` 会把 stderr 管入下游，暂时不在允许集合。
            return false;
        }
    }
    true
}

fn is_safe_redirected_statement(node: Node<'_>, source: &str) -> bool {
    let mut has_command = false;
    let mut has_redirect = false;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if !child.is_named() {
            return false;
        }
        match child.kind() {
            "command" if !has_command => {
                if !is_safe_command(child, source) {
                    return false;
                }
                has_command = true;
            }
            "file_redirect" => {
                if !is_safe_redirect(child, source) {
                    return false;
                }
                has_redirect = true;
            }
            _ => return false,
        }
    }
    has_command && has_redirect
}

fn is_safe_command(node: Node<'_>, source: &str) -> bool {
    let mut command_name = None;
    let mut arguments = Vec::new();
    let mut cursor = node.walk();

    for child in node.children(&mut cursor) {
        if !child.is_named() {
            // 正常 command 的 name、argument 与 redirect 都是 named node；裸 `$` 等会走这里。
            return false;
        }
        match child.kind() {
            "command_name" => {
                if command_name.is_some() {
                    return false;
                }
                command_name = literal_from_node(child, source);
                if command_name.is_none() {
                    return false;
                }
            }
            "file_redirect" => {
                if !is_safe_redirect(child, source) {
                    return false;
                }
            }
            _ => {
                let Some(argument) = literal_from_node(child, source) else {
                    return false;
                };
                arguments.push(argument);
            }
        }
    }

    let Some(command_name) = command_name else {
        return false;
    };
    command_arguments_are_allowed(&command_name.value, &arguments)
}

fn is_safe_redirect(node: Node<'_>, source: &str) -> bool {
    let Some(text) = node_text(node, source) else {
        return false;
    };
    let compact = text
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    if compact == "2>&1" {
        return true;
    }
    if !compact.starts_with('<') || compact.contains('&') || compact.contains('>') {
        return false;
    }

    let mut cursor = node.walk();
    let named_children = node.named_children(&mut cursor).collect::<Vec<Node<'_>>>();
    if named_children.len() != 1 {
        return false;
    }
    literal_from_node(named_children[0], source).is_some()
}

fn literal_from_node(node: Node<'_>, source: &str) -> Option<BashLiteral> {
    match node.kind() {
        "command_name" => {
            let mut cursor = node.walk();
            let mut named_children = node.named_children(&mut cursor);
            let child = named_children.next()?;
            if named_children.next().is_some() {
                return None;
            }
            literal_from_node(child, source)
        }
        "word" => {
            if node.named_child_count() != 0 {
                return None;
            }
            let text = node_text(node, source)?;
            if text.is_empty() || text.chars().any(is_unquoted_expansion_character) {
                return None;
            }
            Some(BashLiteral {
                value: text.to_string(),
            })
        }
        "raw_string" => quoted_literal(node, source, '\''),
        "string" => {
            let mut cursor = node.walk();
            if node
                .named_children(&mut cursor)
                .any(|child| child.kind() != "string_content")
            {
                return None;
            }
            quoted_literal(node, source, '"')
        }
        "number" => {
            if node.named_child_count() != 0 {
                return None;
            }
            let text = node_text(node, source)?;
            if text.is_empty() {
                return None;
            }
            Some(BashLiteral {
                value: text.to_string(),
            })
        }
        _ => None,
    }
}

fn quoted_literal(node: Node<'_>, source: &str, quote: char) -> Option<BashLiteral> {
    let text = node_text(node, source)?;
    if text.len() < 2 || !text.starts_with(quote) || !text.ends_with(quote) {
        return None;
    }
    let value = text.get(1..text.len().saturating_sub(1))?;
    Some(BashLiteral {
        value: value.to_string(),
    })
}

fn node_text<'source>(node: Node<'_>, source: &'source str) -> Option<&'source str> {
    node.utf8_text(source.as_bytes()).ok()
}

fn is_unquoted_expansion_character(character: char) -> bool {
    matches!(
        character,
        '$' | '~' | '*' | '?' | '[' | ']' | '{' | '}' | '`' | '\\'
    )
}

fn command_arguments_are_allowed(command: &str, arguments: &[BashLiteral]) -> bool {
    match command {
        "pwd" | "true" | "false" => arguments.is_empty(),
        "ls" => arguments_allow_options(
            arguments,
            &['a', 'A', 'l', 'h', '1', 'd', 'R'],
            &[
                "--all",
                "--almost-all",
                "--long",
                "--human-readable",
                "--one-per-line",
                "--directory",
                "--recursive",
            ],
            &[],
            &[],
        ),
        "rg" => arguments_allow_options(
            arguments,
            &['n', 'i', 'S', 'F', 'w', 'x', 'l', 'c'],
            &["--files", "--hidden", "--no-heading", "--no-ignore"],
            &[
                ("-A", OptionValueKind::Decimal),
                ("-B", OptionValueKind::Decimal),
                ("-C", OptionValueKind::Decimal),
                ("-g", OptionValueKind::Literal),
            ],
            &[
                ("--glob", OptionValueKind::Literal),
                ("--type", OptionValueKind::Literal),
                ("--type-not", OptionValueKind::Literal),
                ("--max-count", OptionValueKind::Decimal),
            ],
        ),
        "grep" => arguments_allow_options(
            arguments,
            &['n', 'i', 'E', 'F', 'w', 'x', 'v', 'l', 'c', 'r', 'R'],
            &[],
            &[("-m", OptionValueKind::Decimal)],
            &[
                ("--include", OptionValueKind::Literal),
                ("--exclude", OptionValueKind::Literal),
                ("--exclude-dir", OptionValueKind::Literal),
            ],
        ),
        "cat" => arguments_allow_options(arguments, &['n', 'b', 's', 'E', 'T'], &[], &[], &[]),
        "head" | "tail" => arguments_allow_options(
            arguments,
            &[],
            &[],
            &[
                ("-n", OptionValueKind::Decimal),
                ("-c", OptionValueKind::Decimal),
            ],
            &[("--lines", OptionValueKind::Decimal)],
        ),
        "wc" => arguments_allow_options(arguments, &['l', 'w', 'c', 'm', 'L'], &[], &[], &[]),
        "stat" => arguments_allow_options(arguments, &[], &[], &[], &[]),
        "file" => arguments_allow_options(
            arguments,
            &['b'],
            &["--brief", "--mime", "--mime-type"],
            &[],
            &[],
        ),
        "cd" => {
            arguments.len() == 1
                && !arguments[0].value.is_empty()
                && !arguments[0].value.starts_with('-')
                && !arguments[0].value.starts_with('~')
        }
        _ => false,
    }
}

fn arguments_allow_options(
    arguments: &[BashLiteral],
    short_without_value: &[char],
    long_without_value: &[&str],
    short_with_value: &[(&str, OptionValueKind)],
    long_with_value: &[(&str, OptionValueKind)],
) -> bool {
    let mut index = 0usize;
    while let Some(argument) = arguments.get(index) {
        let value = argument.value.as_str();
        if value == "-" {
            return false;
        }
        if let Some(option) = value.strip_prefix("--") {
            let full_option = format!("--{option}");
            if long_without_value
                .iter()
                .any(|allowed| *allowed == full_option)
            {
                index = index.saturating_add(1);
                continue;
            }
            let Some((_, value_kind)) = long_with_value
                .iter()
                .find(|(allowed, _)| *allowed == full_option)
            else {
                return false;
            };
            let Some(value_argument) = arguments.get(index.saturating_add(1)) else {
                return false;
            };
            if !option_value_is_allowed(&value_argument.value, *value_kind) {
                return false;
            }
            index = index.saturating_add(2);
            continue;
        }
        if value.starts_with('-') {
            if let Some((_, value_kind)) = short_with_value
                .iter()
                .find(|(allowed, _)| *allowed == value)
            {
                let Some(value_argument) = arguments.get(index.saturating_add(1)) else {
                    return false;
                };
                if !option_value_is_allowed(&value_argument.value, *value_kind) {
                    return false;
                }
                index = index.saturating_add(2);
                continue;
            }
            let Some(short_cluster) = value.strip_prefix('-') else {
                return false;
            };
            if short_cluster.is_empty()
                || !short_cluster
                    .chars()
                    .all(|flag| short_without_value.contains(&flag))
            {
                return false;
            }
        }
        index = index.saturating_add(1);
    }
    true
}

fn option_value_is_allowed(value: &str, kind: OptionValueKind) -> bool {
    match kind {
        OptionValueKind::Literal => !value.is_empty() && value != "-",
        OptionValueKind::Decimal => {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::bash_script_is_concurrency_safe;

    #[test]
    fn bash_classifier_accepts_prd_allowlist_examples() {
        for script in [
            "pwd",
            "ls -la",
            "pwd; rg -n TODO src",
            "pwd\nrg -n TODO src",
            "rg -n foo src",
            "cd src && rg -n 'ToolRegistry' . | head -n 20",
            "rg -n TODO src && wc -l src/tool/mod.rs",
            "grep -R -n --include '*.md' parallel docs || true",
            "rg foo < input.txt",
            "rg foo 2>&1",
            "cat 'file with spaces.md'",
        ] {
            assert!(
                bash_script_is_concurrency_safe(script),
                "expected safe: {script}"
            );
        }
    }

    #[test]
    fn bash_classifier_rejects_prd_forbidden_forms() {
        for script in [
            "git status",
            "echo hi > output.txt",
            "rg foo |& head",
            "rg foo &",
            "rg $PATTERN src",
            "rg $(pwd) src",
            "rg `pwd` src",
            "rg foo *.rs",
            "source .env",
            "eval 'rg foo src'",
            "function readit() { rg foo src; }",
            "if true; then rg foo src; fi",
            "for file in src/*; do cat \"$file\"; done",
            "(rg foo src)",
            "cat <<'EOF'\nhello\nEOF",
            "cat <<< 'hello'",
            "FOO=bar rg foo src",
            "cd -- && pwd",
            "cd -L && pwd",
            "cd -P && pwd",
            "cd ~ && pwd",
            "tail -f log.txt",
        ] {
            assert!(
                !bash_script_is_concurrency_safe(script),
                "expected unsafe: {script}"
            );
        }
    }
}
