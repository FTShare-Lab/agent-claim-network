//! Prompt 模板管理：MiniJinja 内置模板 + 测试/调试用目录模板。
//!
//! 用户安装后的 binary 默认使用编译进程序的 `prompts/*.j2`，不依赖启动 cwd。
//! 测试或调试需要临时模板时，可显式传入非默认目录。调用方按模板名
//! `render("agent_system", ctx)` 渲染，内部约定文件名为 `<name>.j2`。

use std::path::{Path, PathBuf};

use minijinja::{path_loader, Environment};
use serde::Serialize;

use crate::config::PromptConfig;

const BUNDLED_PROMPT_ROOT_LABEL: &str = "<bundled-prompts>";
const BUNDLED_TEMPLATES: &[(&str, &str)] = &[
    (
        "agent_system.j2",
        include_str!("../../prompts/agent_system.j2"),
    ),
    (
        "inbox_claim_attribute_update_internalize.j2",
        include_str!("../../prompts/inbox_claim_attribute_update_internalize.j2"),
    ),
    (
        "inbox_policy_update_internalize.j2",
        include_str!("../../prompts/inbox_policy_update_internalize.j2"),
    ),
    (
        "maintainer_arbitration_proposal.j2",
        include_str!("../../prompts/maintainer_arbitration_proposal.j2"),
    ),
    (
        "maintainer_arbitration_verification.j2",
        include_str!("../../prompts/maintainer_arbitration_verification.j2"),
    ),
    (
        "memory_review_system.j2",
        include_str!("../../prompts/memory_review_system.j2"),
    ),
    (
        "memory_review.j2",
        include_str!("../../prompts/memory_review.j2"),
    ),
    (
        "session_compaction.j2",
        include_str!("../../prompts/session_compaction.j2"),
    ),
    (
        "session_recap.j2",
        include_str!("../../prompts/session_recap.j2"),
    ),
    (
        "session_search_summary.j2",
        include_str!("../../prompts/session_search_summary.j2"),
    ),
    (
        "subagents_system.j2",
        include_str!("../../prompts/subagents_system.j2"),
    ),
    (
        "subagents_compaction.j2",
        include_str!("../../prompts/subagents_compaction.j2"),
    ),
    (
        "what_is_claim.j2",
        include_str!("../../prompts/what_is_claim.j2"),
    ),
    (
        "what_is_dispute.j2",
        include_str!("../../prompts/what_is_dispute.j2"),
    ),
    (
        "what_is_memory.j2",
        include_str!("../../prompts/what_is_memory.j2"),
    ),
    (
        "what_is_policy.j2",
        include_str!("../../prompts/what_is_policy.j2"),
    ),
];

#[derive(Debug, thiserror::Error)]
pub enum PromptError {
    #[error("prompt 根目录不存在或不是目录: {0}")]
    InvalidRoot(PathBuf),
    #[error("加载模板失败: {name} ({source})")]
    Load {
        name: String,
        #[source]
        source: minijinja::Error,
    },
    #[error("渲染模板失败: {name} ({source})")]
    Render {
        name: String,
        #[source]
        source: minijinja::Error,
    },
}

pub struct PromptRegistry {
    env: Environment<'static>,
    root: PathBuf,
}

impl std::fmt::Debug for PromptRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // minijinja::Environment 不实现 Debug，这里只露关键路径即可
        f.debug_struct("PromptRegistry")
            .field("root", &self.root)
            .finish()
    }
}

impl PromptRegistry {
    /// 从配置构造 prompt registry。缺省配置使用内置模板，显式 root 使用外部目录。
    pub fn from_config(config: &PromptConfig) -> Result<Self, PromptError> {
        match config.external_root() {
            Some(root) => Self::new(root),
            None => Self::bundled(),
        }
    }

    /// 使用编译进 binary 的模板。用户通过 `cargo install` 后在任意 cwd 启动都走这里。
    pub fn bundled() -> Result<Self, PromptError> {
        let mut env = Environment::new();
        for (name, source) in BUNDLED_TEMPLATES {
            env.add_template(name, source)
                .map_err(|source| PromptError::Load {
                    name: (*name).to_string(),
                    source,
                })?;
        }
        Ok(Self {
            env,
            root: PathBuf::from(BUNDLED_PROMPT_ROOT_LABEL),
        })
    }

    /// 构造时验证 root 是已存在的目录；模板本身走 minijinja 的惰性加载，
    /// 缺失模板要等到 `render` 才暴露，便于热替换文件而不必重启。
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, PromptError> {
        let root = root.into();
        if !root.is_dir() {
            return Err(PromptError::InvalidRoot(root));
        }
        let mut env = Environment::new();
        env.set_loader(path_loader(&root));
        Ok(Self { env, root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 按 name 渲染。约定文件名 = `<name>.j2`，调用方只传 `"agent_system"`。
    pub fn render<S: Serialize>(&self, name: &str, ctx: S) -> Result<String, PromptError> {
        let file = format!("{name}.j2");
        let tmpl = self
            .env
            .get_template(&file)
            .map_err(|source| PromptError::Load {
                name: name.to_string(),
                source,
            })?;
        tmpl.render(ctx).map_err(|source| PromptError::Render {
            name: name.to_string(),
            source,
        })
    }

    /// 启动期渲染入口模板，确保 include 依赖也能展开。
    pub fn validate_renderable(&self, names: &[&str]) -> Result<(), PromptError> {
        for name in names {
            match *name {
                "session_compaction" => self.render(
                    name,
                    minijinja::context! {
                        agent_id => "agent-a",
                        start_index => 0usize,
                        end_index => 1usize,
                        prior_summary => Option::<String>::None,
                        summary_max_chars => 6000usize,
                    },
                )?,
                "memory_review_system" => self.render(
                    name,
                    minijinja::context! {
                        agent_id => "agent-a",
                        memory_md => "agent memory",
                        user_md => "user profile",
                    },
                )?,
                "subagents_system" => self.render(
                    name,
                    minijinja::context! {
                        subagent_id => "subagent_1234abcd",
                        parent_session_id => "session_parent",
                        parent_turn_id => "turn_parent",
                        owner_agent_id => "agent-a",
                        title => "test delegation",
                        role => "test verifier",
                        runtime_context => "runtime context",
                    },
                )?,
                "subagents_compaction" => self.render(
                    name,
                    minijinja::context! {
                        summary_max_chars => 6000usize,
                    },
                )?,
                _ => self.render(name, ())?,
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[derive(Serialize)]
    struct AgentSystemTestContext<'a> {
        agent_id: &'a str,
        memory_md: &'a str,
        user_md: &'a str,
        local_claims_snapshot: &'a str,
        available_skills: Vec<AgentSystemSkillContext<'a>>,
        subagent_max_concurrent: usize,
    }

    #[derive(Serialize)]
    struct AgentSystemSkillContext<'a> {
        name: &'a str,
        description: &'a str,
        spec_path: &'a str,
    }

    fn agent_system_test_context() -> AgentSystemTestContext<'static> {
        AgentSystemTestContext {
            agent_id: "agent-a",
            memory_md: "agent memory",
            user_md: "user profile",
            local_claims_snapshot: "以下是当前 agent 已内化且仍有效的 self claims 快照，不是团队真理；当任务涉及团队共享知识、复用或冲突时，仍应 consult_router。\n\n```jsonl\n{\"id\":\"claim_1234abcd\",\"name\":\"claim name\",\"scope\":\"scope/a\",\"statement\":\"statement\",\"status\":\"active\",\"confidence\":\"high\",\"created_at\":\"2026-05-20T00:00:00Z\"}\n```",
            available_skills: vec![AgentSystemSkillContext {
                name: "consult_router",
                description: "根据 claim 候选判断是否继续查询 router",
                spec_path: "<acn_home>/skills/consult_router/SKILL.md",
            }],
            subagent_max_concurrent: 7,
        }
    }

    #[test]
    fn new_rejects_missing_root() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = dir.path().join("does_not_exist");
        let err = PromptRegistry::new(&bogus).expect_err("不存在的目录应当报错");
        assert!(matches!(err, PromptError::InvalidRoot(p) if p == bogus));
    }

    #[test]
    fn bundled_registry_renders_without_prompt_directory() {
        let reg = PromptRegistry::from_config(&PromptConfig { root: None }).unwrap();

        assert_eq!(reg.root(), Path::new(BUNDLED_PROMPT_ROOT_LABEL));
        assert!(!reg.render("what_is_claim", ()).unwrap().is_empty());
    }

    #[test]
    fn render_static_template() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("agent_system.j2"), "hello, world!").unwrap();
        let reg = PromptRegistry::new(dir.path()).unwrap();
        let out = reg.render("agent_system", ()).unwrap();
        assert_eq!(out, "hello, world!");
    }

    #[test]
    fn render_with_context_interpolates() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("hello.j2"), "{{ greeting }}, {{ who }}!").unwrap();
        let reg = PromptRegistry::new(dir.path()).unwrap();
        let out = reg
            .render(
                "hello",
                minijinja::context! { greeting => "hi", who => "agent" },
            )
            .unwrap();
        assert_eq!(out, "hi, agent!");
    }

    #[test]
    fn render_missing_template_errors() {
        let dir = tempfile::tempdir().unwrap();
        let reg = PromptRegistry::new(dir.path()).unwrap();
        let err = reg.render("not_there", ()).expect_err("缺失模板应当报错");
        assert!(matches!(err, PromptError::Load { .. }));
    }

    #[test]
    fn validate_renderable_reports_missing_include() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("agent_system.j2"),
            "{% include \"missing.j2\" %}",
        )
        .unwrap();
        let reg = PromptRegistry::new(dir.path()).unwrap();
        let err = reg
            .validate_renderable(&["agent_system"])
            .expect_err("缺失 include 应当在渲染入口模板时报错");
        assert!(matches!(err, PromptError::Render { ref name, .. } if name == "agent_system"));
    }

    #[test]
    fn repository_prompts_render_includes() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let claim_doc = fs::read_to_string(root.join("what_is_claim.j2")).unwrap();
        let dispute_doc = fs::read_to_string(root.join("what_is_dispute.j2")).unwrap();
        let reg = PromptRegistry::new(&root).unwrap();

        for name in [
            "agent_system",
            "inbox_policy_update_internalize",
            "inbox_claim_attribute_update_internalize",
            "session_recap",
        ] {
            let source = fs::read_to_string(root.join(format!("{name}.j2"))).unwrap();
            assert!(
                source.contains("{% include \"what_is_claim.j2\" %}"),
                "{name} 应显式 include what_is_claim.j2"
            );
            assert!(
                source.contains("{% include \"what_is_dispute.j2\" %}"),
                "{name} 应显式 include what_is_dispute.j2"
            );

            let out = if name == "agent_system" {
                reg.render(name, agent_system_test_context()).unwrap()
            } else {
                reg.render(name, ()).unwrap()
            };
            assert!(
                out.contains(claim_doc.trim()),
                "{name} 应包含 what_is_claim.j2 渲染后的完整内容"
            );
            assert!(
                out.contains(dispute_doc.trim()),
                "{name} 应包含 what_is_dispute.j2 渲染后的完整内容"
            );
            assert!(!out.contains("{% include"), "{name} 不应残留 include 标记");
        }
    }

    #[test]
    fn repository_agent_system_mentions_skills_as_protocol() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let out = reg
            .render("agent_system", agent_system_test_context())
            .unwrap();
        assert!(out.contains("agent-a"));
        assert!(out.contains("agent memory"));
        assert!(out.contains("user profile"));
        assert!(out.contains("# available skills"));
        assert!(out.contains("consult_router"));
        assert!(out.contains("调用 `consult_router` tool 查询 router"));
        assert!(out.contains("不是 claim"));
        assert!(out.contains("SKILL.md"));
        assert!(out.contains("file_read"));
        assert!(out.contains("create_subagent"));
        assert!(out.contains("list_subagents"));
        assert!(out.contains("read_subagent"));
        assert!(out.contains("steer_subagent"));
        assert!(out.contains("wait_subagents"));
        assert!(out.contains("subagent 不是独立 ACN agent"));
        assert!(out.contains("update_subagent_progress"));
        assert!(out.contains("单向进度上报"));
        assert!(out.contains("不能向你提问、等待你的回复"));
        assert!(out.contains("不要频繁轮询"));
        assert!(out.contains("先继续所有不依赖其结果的主线工作"));
        assert!(out.contains("只有下一步确实被该结果阻塞时，才调用 `wait_subagents`"));
        assert!(out.contains("在下一轮显式调用 `read_subagent` 读取结果"));
        assert!(out.contains("只用于 queued/running 的 subagent"));
        assert!(out.contains("completed/failed/abandoned 都是终态"));
        assert!(out.contains("当前同一 session 最多允许 7 个 subagent 同时 running"));
        assert!(out.contains("不要提前创建下游 subagent"));
        assert!(out.contains("同一 assistant 回合最多调用一次 `wait_subagents`"));
        assert!(out.contains("不是可靠消息通道或同步屏障"));
        assert!(out.contains("没有 ack"));
        assert!(out.contains("先用 `read_subagent` 读取旧 summary/result/changed files"));
        assert!(out.contains("再用 `create_subagent` 创建新的 subagent"));
        assert!(!out.contains("create_delegation"));
        assert!(!out.contains("# 你看到的输入"));
        assert!(!out.contains("available_skills：当前工作区内可用"));
        assert!(out.contains("router scope overview 快照"));
        assert!(out.contains("# 你的自有 claims 快照"));
        assert!(out.contains("self claims 快照"));
        assert!(out.contains("```jsonl"));
        assert!(out.contains("\"id\":\"claim_1234abcd\""));
        assert!(out.contains("candidate_claims"));
        assert!(out.contains("disputes"));
        assert!(out.contains("mode=\"overview\""));
        assert!(out.contains("mode=\"query\""));
        assert!(out.contains("scope 总览"));
        assert!(out.contains("关键 tool_use"));
        assert!(out.contains("简短、具体、与当前执行相关的自然语言进度说明"));
        assert!(out.contains("同一阶段连续多个工具调用可以共用一句说明"));
        assert!(out.contains("当用户只是要求你产出长文档、方案、总结、报告或说明时"));
        assert!(out.contains("不要为了“写文档”而调用 `file_write`"));
        assert!(out.contains("2000 个中文字符或 120 行以内"));
        assert!(out.contains("更长内容按章节分段 append"));
        assert!(out.contains("避免把大段正文一次性塞进 `file_write.content`"));
    }

    #[test]
    fn repository_memory_review_system_uses_own_prompt_and_memory_snapshot() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let out = reg
            .render(
                "memory_review_system",
                minijinja::context! {
                    agent_id => "agent-a",
                    memory_md => "agent memory",
                    user_md => "user profile",
                },
            )
            .unwrap();

        assert!(out.contains("persistent memory review agent"));
        assert!(out.contains("agent-a"));
        assert!(out.contains("agent memory"));
        assert!(out.contains("user profile"));
        assert!(out.contains("只能使用 `memory` 工具"));
        assert!(out.contains("后台 review 不暴露 skills"));
        assert!(!out.contains("# available skills"));
        assert!(!out.contains("思考并**用自然语言**回答用户的问题"));
        assert!(!out.contains("应访问 router"));
    }

    #[test]
    fn repository_subagents_system_renders_subagent_identity_and_boundaries() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let out = reg
            .render(
                "subagents_system",
                minijinja::context! {
                    subagent_id => "subagent_1234abcd",
                    parent_session_id => "session_parent",
                    parent_turn_id => "turn_parent",
                    owner_agent_id => "agent-a",
                    title => "manual-happy-1",
                    role => "tui verifier",
                    runtime_context => "workspace=/tmp/acn",
                },
            )
            .unwrap();

        assert!(out.contains("当前 session 内部的 subagent"));
        assert!(out.contains("subagent_1234abcd"));
        assert!(out.contains("session_parent"));
        assert!(out.contains("manual-happy-1"));
        assert!(out.contains("tui verifier"));
        assert!(out.contains("workspace=/tmp/acn"));
        assert!(out.contains("不要创建新的 subagent"));
        assert!(out.contains("update_subagent_progress"));
        assert!(out.contains("不要等待 parent 的确认或 steering 回复"));
        assert!(out.contains("`completed`、`failed` 和 `abandoned` 都是你的终态"));
        assert!(out.contains("进入任何终态后"));
        assert!(out.contains("`code_run` 后台任务是工具调用闭合规则的例外"));
        assert!(out.contains("不表示对应命令已经结束"));
        assert!(out.contains("不得提前给出最终回答"));
        assert!(out.contains("write_stdin 做有界等待或轮询"));
        assert!(out.contains("共同依赖或长期进程应由 main agent 创建和管理"));
        assert!(out.contains("blocker、可选方案、你的推荐及其理由和已验证事实"));
        assert!(!out.contains("delegation_id"));
        assert!(out.contains("parent session 可见的同一组 MCP 工具"));
        assert!(out.contains("不得作为绕过这些文件工具进行文件读取或修改的通道"));
        assert!(out.contains("不是发给用户的话"));
    }

    #[test]
    fn repository_subagents_compaction_renders_summary_limit() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let out = reg
            .render(
                "subagents_compaction",
                minijinja::context! {
                    summary_max_chars => 1234usize,
                },
            )
            .unwrap();

        assert!(out.contains("session subagent"));
        assert!(out.contains("\"summary\""));
        assert!(out.contains("1234"));
        assert!(out.contains("不要编造"));
        assert!(out.contains("运行期修改许可"));
        assert!(out.contains("required_read"));
    }

    #[test]
    fn repository_session_recap_reads_router_results_from_transcript() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let out = reg.render("session_recap", ()).unwrap();

        assert!(out.contains("transcript 中可能包含 router 工具调用结果"));
        assert!(out.contains("candidate_claims"));
        assert!(out.contains("disputes"));
        assert!(out.contains("transcript 中真实出现过的 claim id"));
        assert!(out.contains("\"updated_claims\""));
        assert!(out.contains("必须输出完整属性和 `status`"));
        assert!(out.contains("仍相关的来源 id 需要一并返回"));
        assert!(out.contains("后端也会忽略并把新 claim 初始化为 `active`"));
        assert!(!out.contains("`router_context`："));
        assert!(!out.contains("router_context.candidate_claims"));
        assert!(!out.contains("transcript、router_context 或 local_claims"));
    }

    #[test]
    fn repository_inbox_prompts_require_status_only_for_claim_updates() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();

        for name in [
            "inbox_policy_update_internalize",
            "inbox_claim_attribute_update_internalize",
        ] {
            let out = reg.render(name, ()).unwrap();
            assert!(out.contains("`updated_claims` 必须返回 `status`"));
            assert!(out.contains("后端也会忽略并把新 claim 初始化为 `active`"));
            assert!(out.contains("\"status\": \"active\" | \"stale\" | \"deprecated\""));
        }
    }

    #[test]
    fn repository_arbitration_inbox_prompt_defines_simple_single_call_context() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let prompt = reg
            .render("inbox_claim_attribute_update_internalize", ())
            .unwrap();

        for field in ["arbitration_message", "local_claims", "direct_claims"] {
            assert!(prompt.contains(field), "missing arbitration field {field}");
        }
        assert!(prompt.contains("不读取 Memory、USER、session transcript 或工具上下文"));
        assert!(prompt.contains("全部非 deprecated 本地 Claims"));
        assert!(prompt.contains("全部 direct Claim 冻结快照"));
        assert!(prompt.contains("只能更新 `local_claims`"));
        assert!(prompt.contains("非直接 deprecated Claim 不可见"));
        assert!(prompt.contains("仅仅选择不采纳 Resolution"));
        assert!(prompt.contains("新的实质证据"));
        assert!(prompt.contains("语义输入完全相同，不要重复创建"));
    }

    #[test]
    fn repository_arbitration_prompts_define_governance_and_confidence_boundary() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let prompt_context = minijinja::context! { confidence_threshold => 0.91 };
        let proposal = reg
            .render("maintainer_arbitration_proposal", &prompt_context)
            .unwrap();
        let verification = reg
            .render("maintainer_arbitration_verification", &prompt_context)
            .unwrap();

        for prompt in [&proposal, &verification] {
            for input in [
                "direct_claims",
                "source_claims",
                "router_candidate_claims",
                "message_type=policy_update",
            ] {
                assert!(prompt.contains(input), "missing input boundary {input}");
            }
            for kind in [
                "`coexist`",
                "`lifecycle_update`",
                "`conflict_resolved`",
                "`unresolved`",
            ] {
                assert!(prompt.contains(kind), "missing resolution type {kind}");
            }
            for basis in [
                "`direct_analysis`",
                "`prior_resolution`",
                "`policy`",
                "`evidence`",
                "`insufficient_evidence`",
            ] {
                assert!(prompt.contains(basis), "missing resolution basis {basis}");
            }
            for status in ["`active`", "`stale`", "`deprecated`"] {
                assert!(prompt.contains(status), "missing claim status {status}");
            }
            assert!(prompt.contains("团队当前基线"));
            assert!(prompt.contains("Node 18"));
            assert!(prompt.contains("Node 22"));
            assert!(prompt.contains("query v4"));
            assert!(prompt.contains("硬编码"));
            assert!(prompt.contains("0.91"));
            assert!(prompt.contains("最弱环节原则"));
            assert!(prompt.contains("0.00–0.49"));
            assert!(prompt.contains("0.50–0.74"));
            assert!(prompt.contains("0.75–门槛以下"));
            assert!(prompt.contains("门槛–0.97"));
            assert!(prompt.contains("0.98–1.00"));
            assert!(prompt.contains("自动关闭"));
            assert!(prompt.contains("missing evidence"));
            for check in 1..=10 {
                assert!(
                    prompt.contains(&format!("{check}.")),
                    "missing confidence check {check}"
                );
            }
        }
        assert!(proposal.contains("`human_review_reason` 只是可选的人工交接说明"));
        assert!(proposal.contains("缺失不能让正确的 unresolved 变成技术失败"));
        assert!(proposal.contains("`unresolved` 必须输出空的 `claim_assessments`"));
        assert!(proposal.contains("不以 `stale` 或其他状态代替“等待人工”"));
        assert!(proposal.contains("必须包含每个 direct Claim ID"));
        assert!(proposal.contains("Policy 是主要依据时也不能省略 direct Claim"));
        assert!(proposal.contains("unresolved 也必须引用承载证据缺口的 direct Claim"));
        assert!(verification.contains("Proposal 是待审查对象，不是既定事实"));
        assert!(verification.contains("不得照抄它的结论或 confidence"));
        assert!(verification.contains("任一核心项目不同意"));
        assert!(verification.contains("门槛+0.01"));
        assert!(verification.contains("verdict 为 unresolved 时 `claim_assessments` 必须为 `[]`"));
        assert!(verification.contains("evidence_refs 必须无重复并覆盖全部 direct Claim"));
    }

    #[test]
    fn repository_arbitration_prompts_include_shared_domain_definitions() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let claim_doc = fs::read_to_string(root.join("what_is_claim.j2")).unwrap();
        let dispute_doc = fs::read_to_string(root.join("what_is_dispute.j2")).unwrap();
        let policy_doc = fs::read_to_string(root.join("what_is_policy.j2")).unwrap();
        let reg = PromptRegistry::new(&root).unwrap();
        let bundled = PromptRegistry::bundled().unwrap();
        let prompt_context = minijinja::context! { confidence_threshold => 0.90 };

        for name in [
            "maintainer_arbitration_proposal",
            "maintainer_arbitration_verification",
        ] {
            let source = fs::read_to_string(root.join(format!("{name}.j2"))).unwrap();
            for include in [
                "what_is_claim.j2",
                "what_is_dispute.j2",
                "what_is_policy.j2",
            ] {
                assert!(
                    source.contains(&format!("{{% include \"{include}\" %}}")),
                    "{name} 应显式 include {include}"
                );
            }

            let rendered = reg.render(name, &prompt_context).unwrap();
            for definition in [&claim_doc, &dispute_doc, &policy_doc] {
                assert!(
                    rendered.contains(definition.trim()),
                    "{name} 应包含统一领域定义"
                );
            }
            assert!(!rendered.contains("{% include"));

            let bundled_rendered = bundled.render(name, &prompt_context).unwrap();
            for definition in [&claim_doc, &dispute_doc, &policy_doc] {
                assert!(
                    bundled_rendered.contains(definition.trim()),
                    "bundled {name} 应包含统一领域定义"
                );
            }
            assert!(!bundled_rendered.contains("{% include"));
        }
    }

    #[test]
    fn repository_session_compaction_renders_summary_limit() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let reg = PromptRegistry::new(&root).unwrap();
        let out = reg
            .render(
                "session_compaction",
                serde_json::json!({
                    "agent_id": "agent-a",
                    "start_index": 0,
                    "end_index": 2,
                    "prior_summary": null,
                    "summary_max_chars": 1234,
                }),
            )
            .unwrap();

        assert!(out.contains("不超过 1234 个字符"));
        assert!(out.contains("选择性保留"));
        assert!(out.contains("运行期修改许可"));
        assert!(out.contains("required_read"));
        assert!(!out.contains("summary_max_chars"));
    }
}
