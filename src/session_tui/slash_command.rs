//! Slash command 目录与补全渲染。
//!
//! 本模块维护 TUI 可识别的命令清单（原生命令 + workspace skills）、前缀匹配、
//! 5 行窗口渲染与行中唯一前缀补全。实际命令执行由 `app` 根据 `InputAction` 分发。

use ratatui::text::Line;

use super::completion_menu::{render_completion_menu, CompletionMenuEntry, CompletionMenuState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlashCommandAction {
    Compact,
    Copy,
    Exit,
    Help,
    Inbox,
    Mcp,
    Ps,
    Resume,
    Skills,
    Subagents,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlashCommandSpec {
    command: &'static str,
    description: &'static str,
    action: SlashCommandAction,
}

const SLASH_COMMANDS: &[SlashCommandSpec] = &[
    SlashCommandSpec {
        command: "/compact",
        description: "压缩当前 session 历史",
        action: SlashCommandAction::Compact,
    },
    SlashCommandSpec {
        command: "/copy",
        description: "复制最后一条 Assistant 回复",
        action: SlashCommandAction::Copy,
    },
    SlashCommandSpec {
        command: "/exit",
        description: "finalize 并退出",
        action: SlashCommandAction::Exit,
    },
    SlashCommandSpec {
        command: "/help",
        description: "显示 TUI 命令",
        action: SlashCommandAction::Help,
    },
    SlashCommandSpec {
        command: "/inbox",
        description: "同步 maintainer inbox",
        action: SlashCommandAction::Inbox,
    },
    SlashCommandSpec {
        command: "/mcp",
        description: "查看 MCP servers 与 tools",
        action: SlashCommandAction::Mcp,
    },
    SlashCommandSpec {
        command: "/ps",
        description: "查看和管理受管后台进程",
        action: SlashCommandAction::Ps,
    },
    SlashCommandSpec {
        command: "/resume",
        description: "恢复历史 session",
        action: SlashCommandAction::Resume,
    },
    SlashCommandSpec {
        command: "/skills",
        description: "列出可用 skills",
        action: SlashCommandAction::Skills,
    },
    SlashCommandSpec {
        command: "/subagents",
        description: "查看当前 session 的 subagents",
        action: SlashCommandAction::Subagents,
    },
];

/// 菜单条目：原生命令或 workspace skill，`command` 均带前导 `/`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SlashCommandEntry {
    pub(super) command: String,
    pub(super) description: String,
    pub(super) kind: SlashCommandEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlashCommandEntryKind {
    Native(SlashCommandAction),
    Skill,
}

impl CompletionMenuEntry for SlashCommandEntry {
    fn label(&self) -> &str {
        &self.command
    }

    fn description(&self) -> &str {
        &self.description
    }
}

/// slash 命令目录：skills 按字母序排在原生命令（字母序）之前。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SlashCommandCatalog {
    skill_entries: Vec<SlashCommandEntry>,
}

impl SlashCommandCatalog {
    /// 用 workspace skills 构建目录。名字含 `/` 补全字符集之外字符或与原生命令
    /// 撞名的 skill 被跳过（原生命令优先，避免同名双条目）。
    pub(super) fn with_skills<'a>(skills: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut skill_entries = skills
            .into_iter()
            .filter(|(name, _)| is_completable_command_name(name))
            .filter(|(name, _)| {
                SLASH_COMMANDS
                    .iter()
                    .all(|spec| spec.command[1..] != **name)
            })
            .map(|(name, description)| SlashCommandEntry {
                command: format!("/{name}"),
                // skill 的 frontmatter 支持多行 YAML 字符串；菜单每个候选只能占一行，
                // 且不能把控制字符交给终端解释。
                description: normalized_menu_description(description),
                kind: SlashCommandEntryKind::Skill,
            })
            .collect::<Vec<_>>();
        skill_entries.sort_by(|a, b| a.command.cmp(&b.command));
        skill_entries.dedup_by(|a, b| a.command == b.command);
        Self { skill_entries }
    }

    /// 全量条目：skills（字母序）在前，原生命令（字母序）在后。
    fn entries(&self) -> impl Iterator<Item = SlashCommandEntry> + '_ {
        self.skill_entries
            .iter()
            .cloned()
            .chain(SLASH_COMMANDS.iter().map(|spec| SlashCommandEntry {
                command: spec.command.to_string(),
                description: spec.description.to_string(),
                kind: SlashCommandEntryKind::Native(spec.action),
            }))
    }

    pub(super) fn matching(&self, input: &str) -> Vec<SlashCommandEntry> {
        let Some(prefix) = valid_slash_prefix(input) else {
            return Vec::new();
        };
        self.entries()
            .filter(|entry| entry.command.starts_with(prefix))
            .collect()
    }

    pub(super) fn exact_entry(&self, input: &str) -> Option<SlashCommandEntry> {
        let command = valid_slash_prefix(input)?;
        self.entries().find(|entry| entry.command == command)
    }

    /// 行首 `/skill args` 的 skill 判定。原生命令带参数继续作为普通文本发送，
    /// 以保持既有 `/compact now` 等输入的语义；未知的行首 slash 由调用方报错。
    pub(super) fn has_leading_skill_invocation(&self, input: &str) -> bool {
        let Some(command) = leading_slash_command(input) else {
            return false;
        };
        self.skill_entries
            .iter()
            .any(|entry| entry.command == command)
    }

    /// 原生命令的带参数形式保持普通文本语义，避免破坏已有交互和 Ctrl+Enter steer。
    pub(super) fn has_leading_native_invocation(&self, input: &str) -> bool {
        let Some(command) = leading_slash_command(input) else {
            return false;
        };
        SLASH_COMMANDS.iter().any(|spec| spec.command == command)
    }

    pub(super) fn should_show_menu(&self, input: &str) -> bool {
        self.exact_entry(input).is_none() && !self.matching(input).is_empty()
    }

    /// 行中 `空格+/前缀` 的唯一 skill 补全：候选中恰有一个真前缀匹配时返回缺失后缀。
    ///
    /// 原生命令仅在行首作为 TUI 操作执行；句中 slash 只表示用户显式提及 skill，
    /// 因而不能让当前或将来新增的原生命令参与补全。
    pub(super) fn unique_skill_completion_suffix(&self, token: &str) -> Option<String> {
        valid_slash_prefix(token)?;
        let mut candidates = self
            .skill_entries
            .iter()
            .filter(|entry| entry.command.starts_with(token));
        let first = candidates.next()?;
        if candidates.next().is_some() {
            return None;
        }
        let suffix = first.command.strip_prefix(token)?;
        (!suffix.is_empty()).then(|| suffix.to_string())
    }
}

fn is_completable_command_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

/// 把外部 skill 描述规整成可安全渲染的一行菜单文本。
fn normalized_menu_description(description: &str) -> String {
    description
        .split(|ch: char| ch.is_whitespace() || ch.is_control())
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn render_slash_menu(
    catalog: &SlashCommandCatalog,
    input: &str,
    state: &CompletionMenuState,
    width: u16,
) -> Vec<Line<'static>> {
    let matches = catalog.matching(input);
    render_completion_menu(&matches, state, width)
}

fn valid_slash_prefix(input: &str) -> Option<&str> {
    if !input.starts_with('/') || input.contains('\n') {
        return None;
    }
    if input.chars().any(char::is_whitespace) {
        return None;
    }
    if !input
        .chars()
        .skip(1)
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return None;
    }
    Some(input)
}

pub(super) fn leading_slash_command(input: &str) -> Option<&str> {
    if !input.starts_with('/') || input.contains('\n') {
        return None;
    }
    let end = input
        .char_indices()
        .find_map(|(index, ch)| (index > 0 && ch.is_whitespace()).then_some(index))
        .unwrap_or(input.len());
    let command = &input[..end];
    valid_slash_prefix(command)?;
    input[end..]
        .chars()
        .next()
        .is_none_or(char::is_whitespace)
        .then_some(command)
}

pub(super) fn is_slash_command_like(input: &str) -> bool {
    valid_slash_prefix(input).is_some()
}

#[cfg(test)]
mod tests {
    use ratatui::style::Modifier;

    use super::*;

    fn catalog_with_two_skills() -> SlashCommandCatalog {
        SlashCommandCatalog::with_skills([
            ("verify", "运行完整验证"),
            ("tui-smoke-test-with-tmux", "tmux 冒烟测试"),
        ])
    }

    #[test]
    fn slash_commands_match_prefix_in_alphabetical_order() {
        let commands = SlashCommandCatalog::default()
            .matching("/")
            .into_iter()
            .map(|entry| entry.command)
            .collect::<Vec<_>>();

        assert_eq!(
            commands,
            vec![
                "/compact",
                "/copy",
                "/exit",
                "/help",
                "/inbox",
                "/mcp",
                "/ps",
                "/resume",
                "/skills",
                "/subagents"
            ]
        );
    }

    #[test]
    fn skills_sort_before_native_commands() {
        let commands = catalog_with_two_skills()
            .matching("/")
            .into_iter()
            .map(|entry| entry.command)
            .collect::<Vec<_>>();

        assert_eq!(
            commands,
            vec![
                "/tui-smoke-test-with-tmux",
                "/verify",
                "/compact",
                "/copy",
                "/exit",
                "/help",
                "/inbox",
                "/mcp",
                "/ps",
                "/resume",
                "/skills",
                "/subagents",
            ]
        );
    }

    #[test]
    fn skill_name_prefix_matches_partial_input() {
        let matches = catalog_with_two_skills()
            .matching("/tui-")
            .into_iter()
            .map(|entry| entry.command)
            .collect::<Vec<_>>();
        assert_eq!(matches, vec!["/tui-smoke-test-with-tmux"]);
    }

    #[test]
    fn skill_colliding_with_native_command_is_skipped() {
        let catalog = SlashCommandCatalog::with_skills([
            ("resume", "撞名 skill"),
            ("ps", "撞名 skill"),
            ("subagents", "撞名 skill"),
        ]);
        for (command, action) in [
            ("/resume", SlashCommandAction::Resume),
            ("/ps", SlashCommandAction::Ps),
            ("/subagents", SlashCommandAction::Subagents),
        ] {
            let matches = catalog.matching(command);
            assert_eq!(matches.len(), 1);
            assert!(matches!(
                matches[0].kind,
                SlashCommandEntryKind::Native(actual) if actual == action
            ));
        }
    }

    #[test]
    fn skill_with_invalid_name_chars_is_skipped() {
        let catalog = SlashCommandCatalog::with_skills([("有中文", "x"), ("a.b", "y")]);
        assert!(catalog.matching("/").len() == SLASH_COMMANDS.len());
    }

    #[test]
    fn skill_description_is_rendered_as_a_single_safe_line() {
        let catalog =
            SlashCommandCatalog::with_skills([("multiline", "第一行\n第二行\t\u{1b}[2J\r第三行")]);

        let entry = catalog
            .matching("/multiline")
            .into_iter()
            .next()
            .expect("应匹配 seeded skill");
        assert_eq!(entry.description, "第一行 第二行 [2J 第三行");

        let state = CompletionMenuState::default();
        let lines = render_slash_menu(&catalog, "/multiline", &state, 96);
        assert_eq!(lines.len(), 1);
        let rendered = lines[0].to_string();
        assert!(rendered.contains("第一行 第二行 [2J 第三行"));
        assert!(!rendered.chars().any(char::is_control));
    }

    #[test]
    fn exact_command_hides_completion_menu() {
        let catalog = catalog_with_two_skills();
        assert!(catalog.should_show_menu("/re"));
        assert!(!catalog.should_show_menu("/resume"));
        assert!(!catalog.should_show_menu("/unknown"));
        assert!(catalog.should_show_menu("/tui-"));
        assert!(!catalog.should_show_menu("/tui-smoke-test-with-tmux"));
    }

    #[test]
    fn slash_command_like_rejects_path_like_input() {
        assert!(is_slash_command_like("/refresh"));
        assert!(!is_slash_command_like("/tmp/foo"));
    }

    #[test]
    fn unique_skill_completion_suffix_requires_single_skill_candidate() {
        let catalog = catalog_with_two_skills();
        assert_eq!(
            catalog.unique_skill_completion_suffix("/tui-s"),
            Some("moke-test-with-tmux".to_string())
        );
        assert_eq!(
            catalog.unique_skill_completion_suffix("/v"),
            Some("erify".to_string())
        );
        // 句中原生命令永远不参与补全；即使唯一匹配 /compact 也不能出提示。
        assert_eq!(catalog.unique_skill_completion_suffix("/compa"), None);
        assert_eq!(catalog.unique_skill_completion_suffix("/skills"), None);
        // 完整 skill 没有缺失后缀。
        assert_eq!(catalog.unique_skill_completion_suffix("/verify"), None);
        // 非法 token 不补全
        assert_eq!(catalog.unique_skill_completion_suffix("/a b"), None);
        assert_eq!(catalog.unique_skill_completion_suffix("verify"), None);
    }

    #[test]
    fn render_caps_visible_rows_and_keeps_selection_visible() {
        let catalog = catalog_with_two_skills();
        let mut state = CompletionMenuState::default();
        let lines = render_slash_menu(&catalog, "/", &state, 96);
        assert_eq!(
            lines.len(),
            super::super::completion_menu::COMPLETION_MENU_MAX_VISIBLE
        );
        let text = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("/tui-smoke-test-with-tmux"));
        assert!(text.contains("/verify"));
        assert!(!text.contains("/resume"));

        // 选中第 7 项（/inbox）时窗口滚动，选中行加粗可见
        for _ in 0..6 {
            assert!(state.select_next(catalog.matching("/").len()));
        }
        let lines = render_slash_menu(&catalog, "/", &state, 96);
        assert_eq!(
            lines.len(),
            super::super::completion_menu::COMPLETION_MENU_MAX_VISIBLE
        );
        let bold = lines
            .iter()
            .flat_map(|line| line.spans.iter())
            .find(|span| span.style.add_modifier.contains(Modifier::BOLD))
            .expect("选中行应加粗");
        assert!(bold.content.starts_with("/inbox"));
    }

    #[test]
    fn render_truncates_long_skill_name_to_terminal_width() {
        let long_name = "long".repeat(48);
        let catalog = SlashCommandCatalog::with_skills([(long_name.as_str(), "long skill")]);

        let state = CompletionMenuState::default();
        let lines = render_slash_menu(&catalog, "/", &state, 12);

        assert_eq!(
            lines.len(),
            super::super::completion_menu::COMPLETION_MENU_MAX_VISIBLE
        );
        assert!(lines.iter().all(|line| line.width() <= 12));
        let command = lines[0]
            .spans
            .iter()
            .find(|span| span.style.add_modifier.contains(Modifier::BOLD))
            .expect("选中 skill 应渲染为加粗命令");
        assert!(command.content.ends_with('…'));
    }
}
