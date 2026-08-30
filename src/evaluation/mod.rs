//! 非交互 coding benchmark 入口。
//!
//! 本模块只接受外部 attempt 配置，构造隔离 runtime root 中的单个 session，
//! 并把不含 credential 的版本化 JSONL 事件和最终 result JSON 落到 attempt output 目录。
//! 模型 API key 由容器环境变量提供（`[agent.llm].api_key_env`）。

mod bundle_router;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{mpsc as std_mpsc, Arc};

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::agent::fs::LocalFsClaimStore;
use crate::agent::{LocalClaimStore, SessionEvent, SessionFinalizeReport};
use crate::api::{with_evaluation_usage_recording, EvaluationUsage, EvaluationUsageRecorder};
use crate::bootstrap::build_evaluation_session_engine;
use crate::claim::Claim;
use crate::claim::{AgentId, ClaimId, SessionId};
use crate::config::{resolve_workspace_root, Config, LlmProvider};
use crate::tool::EvaluationSubmission;

pub const EVALUATION_SCHEMA_VERSION: u32 = 1;
pub const EVALUATION_MODEL_KEY_ENV: &str = "ACN_EVAL_MODEL_KEY";

/// 评测 harness 的模型可见面。默认保持既有 profile，minimal 只用于显式上限对照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationHarnessMode {
    #[default]
    Standard,
    Minimal,
}

/// 评测 runtime 的 ACN.md 只承载 claim 交付契约；通用工程流程由冻结 skill 单一维护。
const EVALUATION_ACN_MD_GUIDANCE: &str = r#"不要在会话中创建、更新或删除 claim 或 dispute；会话结束后的 finalize 会另线提取可复用判断。

system prompt 中的 router scope overview 已是本 attempt 的冻结快照，不要再调用 overview。若当前 user turn 含 `<frozen-claims>`，这些内容就是本 attempt 的完整强制交付，不要调用 `consult_router`。否则，仅当 scope overview 非空时，在形成具体检索问题后选择最相关的一个 scope，调用一次 `consult_router` query；后续直接使用该次返回，不要重复查询。

候选 claim 只是此前探索的经验和线索，不代表其中的方案已经成功，也不保证在当前任务或当前代码状态下仍成立。始终以当前任务契约、工作区事实以及可复现的工具和测试结果独立判断；证据不足、条件不符或与当前证据冲突时，不要沿用其中的解法。

使用候选 claim 时，应区分其中已验证的核心机制与尚未覆盖的边界；采用前主动检查适用条件、例外、反例和组合交互，并独立验证异常路径、空值或极端输入、状态生命周期、兼容性及错误优先级。"#;

pub use bundle_router::{
    FrozenClaimBundle, FrozenClaimBundleRouter, FrozenClaimDeliveryPolicy, RouterEvidence,
};

/// 单个 benchmark attempt 的外部配置；不含任何 credential。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationAttemptConfig {
    pub schema_version: u32,
    pub attempt_id: String,
    pub task_prompt: String,
    pub workspace_root: PathBuf,
    pub runtime_root: PathBuf,
    pub acn_config: PathBuf,
    pub output_dir: PathBuf,
    pub upstream: String,
    pub variant: String,
    /// 早于 Pier 墙钟的自有截止时间；到点后干净收尾并写出证据，
    /// 避免被 SIGKILL 后没有 result.json 而被误判成基础设施故障。
    pub attempt_deadline_secs: u64,
    /// 冻结的模型出口模式；由宿主 runner 与 Formal Gate 共同消费。
    pub model_egress_mode: String,
    #[serde(default)]
    pub harness_mode: EvaluationHarnessMode,
    #[serde(default)]
    pub claim_bundle: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationRunPaths {
    pub event_ledger: PathBuf,
    pub result: PathBuf,
}

impl EvaluationRunPaths {
    pub fn from_config(config: &EvaluationAttemptConfig) -> anyhow::Result<Self> {
        for (field, path) in [
            ("workspace_root", &config.workspace_root),
            ("runtime_root", &config.runtime_root),
            ("acn_config", &config.acn_config),
            ("output_dir", &config.output_dir),
        ] {
            if !path.is_absolute() {
                anyhow::bail!(
                    "stage=config field={field} 必须是绝对路径: {}",
                    path.display()
                );
            }
        }
        if let Some(path) = &config.claim_bundle {
            if !path.is_absolute() {
                anyhow::bail!(
                    "stage=config field=claim_bundle 必须是绝对路径: {}",
                    path.display()
                );
            }
        }
        if config.schema_version != EVALUATION_SCHEMA_VERSION {
            anyhow::bail!(
                "stage=config schema_version 不支持: expected={} actual={}",
                EVALUATION_SCHEMA_VERSION,
                config.schema_version
            );
        }
        if config.attempt_id.trim().is_empty() || config.task_prompt.trim().is_empty() {
            anyhow::bail!("stage=config attempt_id 和 task_prompt 均不能为空");
        }
        if config.attempt_deadline_secs == 0 {
            anyhow::bail!("stage=config attempt_deadline_secs 必须为正数");
        }
        if !matches!(config.model_egress_mode.as_str(), "pier" | "direct") {
            anyhow::bail!("stage=config model_egress_mode 必须是 pier 或 direct");
        }
        match config.variant.as_str() {
            "A" | "B_empty" if config.claim_bundle.is_none() => {}
            "B_claim" | "B_forced_claim" if config.claim_bundle.is_some() => {}
            "A" | "B_empty" => {
                anyhow::bail!(
                    "stage=config variant={} 不得设置 claim_bundle",
                    config.variant
                )
            }
            "B_claim" | "B_forced_claim" => {
                anyhow::bail!("stage=config {} 必须设置 claim_bundle", config.variant)
            }
            _ => anyhow::bail!("stage=config variant 必须是 A、B_empty、B_claim 或 B_forced_claim"),
        }
        Ok(Self {
            event_ledger: config.output_dir.join("events.jsonl"),
            result: config.output_dir.join("result.json"),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EvaluationEvent {
    pub schema_version: u32,
    pub attempt_id: String,
    pub seq: usize,
    pub event_type: String,
    pub timestamp_utc: DateTime<Utc>,
    pub payload: Value,
}

enum BufferedEvaluationEvent {
    Direct {
        event_type: String,
        payload: Value,
        timestamp_utc: DateTime<Utc>,
    },
    Session {
        event: SessionEvent,
        timestamp_utc: DateTime<Utc>,
    },
}

impl EvaluationEvent {
    pub fn new(
        attempt_id: impl Into<String>,
        seq: usize,
        event_type: impl Into<String>,
        payload: Value,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: EVALUATION_SCHEMA_VERSION,
            attempt_id: attempt_id.into(),
            seq,
            event_type: event_type.into(),
            timestamp_utc: timestamp,
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EvaluationResult {
    pub schema_version: u32,
    pub attempt_id: String,
    pub exit_type: String,
    pub agent_steps: usize,
    pub claim_new_ids: Vec<String>,
    pub claim_updated_ids: Vec<String>,
    pub claim_used_ids: Vec<String>,
    pub router_evidence: Vec<RouterEvidence>,
    pub router_evidence_incomplete: bool,
    pub usage: EvaluationUsageTotals,
    pub event_ledger_path: PathBuf,
    /// 可由宿主可靠分类的外部基础设施失败；普通 agent 失败保持为 None。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<EvaluationFailureKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationFailureKind {
    /// 上游明确拒绝新的模型请求，无法归因于 agent、claim 或任务实现。
    UpstreamConcurrencyExhausted,
}

/// 一个 attempt 内全部模型请求（含 finalize）的 token 计量；失败请求保留不完整记录。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct EvaluationUsageTotals {
    pub model_requests: usize,
    /// 同时具备 model、prompt_tokens 与 completion_tokens 的响应数。
    pub complete_model_responses: usize,
    pub incomplete_model_responses: usize,
    /// recorder mutex 损坏等本地审计失败；此类结果不得通过评测 gate。
    pub audit_incomplete: bool,
    pub response_models: Vec<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub reasoning_tokens: u64,
}

impl EvaluationUsageTotals {
    fn accumulate(records: &[EvaluationUsage]) -> Self {
        let mut totals = records.iter().fold(Self::default(), |mut totals, record| {
            totals.model_requests += 1;
            if record.is_complete {
                totals.complete_model_responses += 1;
            } else {
                totals.incomplete_model_responses += 1;
            }
            totals.input_tokens = totals.input_tokens.saturating_add(record.input_tokens);
            totals.output_tokens = totals.output_tokens.saturating_add(record.output_tokens);
            totals.cache_read_tokens = totals
                .cache_read_tokens
                .saturating_add(record.cache_read_tokens);
            totals.reasoning_tokens = totals
                .reasoning_tokens
                .saturating_add(record.reasoning_tokens);
            totals
        });
        totals.response_models = records
            .iter()
            .filter_map(|record| record.model.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        totals
    }
}

impl EvaluationResult {
    pub fn empty(
        attempt_id: impl Into<String>,
        exit_type: impl Into<String>,
        event_ledger_path: PathBuf,
    ) -> Self {
        Self {
            schema_version: EVALUATION_SCHEMA_VERSION,
            attempt_id: attempt_id.into(),
            exit_type: exit_type.into(),
            agent_steps: 0,
            claim_new_ids: Vec::new(),
            claim_updated_ids: Vec::new(),
            claim_used_ids: Vec::new(),
            router_evidence: Vec::new(),
            router_evidence_incomplete: false,
            usage: EvaluationUsageTotals::default(),
            event_ledger_path,
            failure_kind: None,
            error: None,
        }
    }
}

/// 读取 attempt TOML；路径只能由 binary 的 `--config` 指定。
pub async fn load_attempt_config(path: &Path) -> anyhow::Result<EvaluationAttemptConfig> {
    if !path.is_absolute() {
        anyhow::bail!(
            "stage=config config path 必须是绝对路径: {}",
            path.display()
        );
    }
    let raw = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("stage=config 读取 attempt 配置失败: {}", path.display()))?;
    let config = toml::from_str(&raw)
        .with_context(|| format!("stage=config 解析 attempt 配置失败: {}", path.display()))?;
    EvaluationRunPaths::from_config(&config)?;
    Ok(config)
}

/// 执行一次评测。成功与失败都尽量写出 result JSON；调用方以 exit_type 决定进程退出码。
pub async fn run_attempt(config: EvaluationAttemptConfig) -> anyhow::Result<EvaluationResult> {
    let paths = EvaluationRunPaths::from_config(&config)?;
    prepare_output_dir(&config, &paths).await.with_context(|| {
        format!(
            "stage=output attempt_id={} workspace={}",
            config.attempt_id,
            config.workspace_root.display()
        )
    })?;
    // 被测命令可能耗尽容器 PID，届时 Tokio 无法再创建 blocking worker。writer 必须在
    // attempt 启动前预留自己的线程，保证内部 deadline 后仍能写出完整判定产物。
    let artifact_writer = EvaluationArtifactWriter::start(paths.clone())?;
    let mut events = Vec::new();
    record_event(
        &mut events,
        &config.attempt_id,
        "attempt_started",
        json!({}),
    );

    let usage_recorder = Arc::new(EvaluationUsageRecorder::default());
    // agent_steps 与 router 都在外层持有：agent 失败或超时同样是有效实验结果，
    // 其 step 数、token 与 claim 注入证据都必须落盘，不能随被丢弃的 future 一起消失。
    let router = match load_bundle_router(&config).await {
        Ok(router) => Arc::new(router),
        Err(error) => {
            let anchored = format!(
                "stage=run attempt_id={} workspace={}: {error:#}",
                config.attempt_id,
                config.workspace_root.display()
            );
            record_event(
                &mut events,
                &config.attempt_id,
                "attempt_failed",
                json!({ "error": anchored }),
            );
            let mut result = EvaluationResult::empty(
                config.attempt_id.clone(),
                "failed",
                paths.event_ledger.clone(),
            );
            result.error = Some(anchored);
            record_event(
                &mut events,
                &config.attempt_id,
                "attempt_finished",
                json!({"exit_type": result.exit_type, "agent_steps": result.agent_steps}),
            );
            artifact_writer.persist(&events, &result).await?;
            return Ok(result);
        }
    };
    let deadline = std::time::Duration::from_secs(config.attempt_deadline_secs);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let mut compaction_report = SessionFinalizeReport::default();
    let task_config = config.clone();
    let task_router = router.clone();
    let task_usage_recorder = usage_recorder.clone();
    let mut attempt_task = tokio::spawn(with_evaluation_usage_recording(
        task_usage_recorder,
        async move { run_attempt_inner(&task_config, event_tx, task_router).await },
    ));
    let attempt_outcome = match tokio::time::timeout(deadline, &mut attempt_task).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => Err(anyhow::anyhow!("stage=run attempt task 异常退出: {error}")),
        Err(_) => {
            attempt_task.abort();
            // 给 runtime 一个有界机会处理 abort 并释放 sender；不让清理再次吃掉 Pier reserve。
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut attempt_task).await;
            Err(anyhow::anyhow!(
                "stage=deadline attempt 超过自有截止时间 {}s，已在 Pier 墙钟前收尾",
                config.attempt_deadline_secs
            ))
        }
    };
    drain_buffered_events(
        &mut event_rx,
        &mut events,
        &config.attempt_id,
        &mut compaction_report,
    );
    let agent_steps = usage_recorder.response_count();
    let router_evidence = router.take_evidence();
    let usage = record_model_request_events(&mut events, &config.attempt_id, &usage_recorder);
    let router_evidence_incomplete = router.audit_is_incomplete();
    let (report, run_error) = match attempt_outcome {
        Ok(finalize_report) => (
            merge_attempt_claim_reports(compaction_report, finalize_report),
            None,
        ),
        Err(error) => (compaction_report, Some(error)),
    };
    let snapshot_error = append_attempt_claim_snapshots(&config, &mut events, &report)
        .await
        .err();
    let run_error = match (run_error, snapshot_error) {
        (Some(error), Some(snapshot_error)) => Some(anyhow::anyhow!(
            "{error:#}; stage=claim_snapshot 部分归因快照失败: {snapshot_error:#}"
        )),
        (Some(error), None) => Some(error),
        (None, Some(error)) => Some(error),
        (None, None) => None,
    };
    let result = match run_error {
        None => result_from_finalize(
            &config,
            &paths,
            agent_steps,
            report,
            router_evidence,
            router_evidence_incomplete,
            usage,
        ),
        Some(error) => {
            let anchored = format!(
                "stage=run attempt_id={} workspace={}: {error:#}",
                config.attempt_id,
                config.workspace_root.display()
            );
            record_event(
                &mut events,
                &config.attempt_id,
                "attempt_failed",
                json!({ "error": anchored }),
            );
            let mut result = EvaluationResult::empty(
                config.attempt_id.clone(),
                "failed",
                paths.event_ledger.clone(),
            );
            result.error = Some(anchored);
            result.failure_kind = classify_evaluation_failure(&error);
            result.usage = usage;
            result.agent_steps = agent_steps;
            result.router_evidence = router_evidence;
            result.router_evidence_incomplete = router_evidence_incomplete;
            apply_claim_attribution(&mut result, &report);
            result
        }
    };
    record_event(
        &mut events,
        &config.attempt_id,
        "attempt_finished",
        json!({"exit_type": result.exit_type, "agent_steps": result.agent_steps}),
    );
    artifact_writer.persist(&events, &result).await?;
    Ok(result)
}

async fn run_attempt_inner(
    config: &EvaluationAttemptConfig,
    event_tx: mpsc::UnboundedSender<BufferedEvaluationEvent>,
    router: Arc<FrozenClaimBundleRouter>,
) -> anyhow::Result<SessionFinalizeReport> {
    let workspace_root = resolve_workspace_root(Some(&config.workspace_root))
        .context("stage=workspace 校验 workspace_root 失败")?;
    let mut cfg = Config::load_for_evaluation(&config.acn_config)
        .context("stage=config 加载 ACN 配置失败")?;
    if cfg.agent.llm.api_key_env != EVALUATION_MODEL_KEY_ENV {
        anyhow::bail!(
            "stage=config evaluation [agent.llm].api_key_env 必须为 {EVALUATION_MODEL_KEY_ENV}: {}",
            cfg.agent.llm.api_key_env
        );
    }
    cfg.set_tool_workspace_root(workspace_root);
    validate_evaluation_llm_provider(cfg.agent.llm.provider)?;
    // key 仅从容器环境读取，不写入评测配置或结果产物。
    if cfg.agent.llm.api_key.is_none() {
        anyhow::bail!(
            "stage=credential 容器环境缺少 [agent.llm].api_key_env 指定的模型 key: {}",
            cfg.agent.llm.api_key_env
        );
    }
    let mut upstream = cfg
        .resolve_upstream(Some(&config.upstream))
        .context("stage=config 解析评测 upstream 失败")?;
    upstream.agent_id = evaluation_agent_id(&config.attempt_id)?;
    let existing_agent_home = config
        .runtime_root
        .join("data")
        .join("agents")
        .join(upstream.agent_id.as_str());
    if existing_agent_home.exists() {
        anyhow::bail!(
            "stage=runtime 独立 runtime_root 已含本 attempt 的 agent 状态: {}",
            existing_agent_home.display()
        );
    }
    cfg.activate_evaluation_runtime(&config.runtime_root)
        .context("stage=runtime 激活独立 runtime_root 失败")?;
    if config.harness_mode == EvaluationHarnessMode::Standard {
        let acn_md_path = cfg.storage.acn_md_path();
        write_evaluation_acn_md(&acn_md_path)
            .await
            .context("stage=runtime 写入 evaluation ACN.md 引导失败")?;
    }
    let (turn_prompt, forced_claim_ids) =
        task_prompt_with_forced_claims(config, router.as_ref()).await?;
    if !forced_claim_ids.is_empty() {
        buffer_direct_event(
            &event_tx,
            "forced_claim_context",
            json!({ "claim_ids": forced_claim_ids }),
        );
    }
    let submission = EvaluationSubmission::new();
    let engine = build_evaluation_session_engine(
        &cfg,
        &upstream,
        router.clone(),
        submission.clone(),
        config.harness_mode,
    )
    .context("stage=engine 构造 evaluation session engine 失败")?;
    let start = engine
        .start_session_with_id_factory(
            SessionId::random,
            cfg.agent.session.id_mint_max_attempts(),
            |event| buffer_session_event(&event_tx, event),
        )
        .await
        .context("stage=session 创建单次 session 失败")?;
    let mut session = start.session;
    let turn_result = engine
        .run_turn(&mut session, turn_prompt, |event| {
            buffer_session_event(&event_tx, event)
        })
        .await;
    turn_result.context("stage=turn 执行任务失败")?;
    buffer_evaluation_completion(&event_tx, &submission);
    let report = engine
        .finalize_session(&mut session, |event| buffer_session_event(&event_tx, event))
        .await
        .context("stage=finalize 收尾 session 失败")?;
    Ok(report)
}

async fn write_evaluation_acn_md(path: &Path) -> anyhow::Result<()> {
    tokio::fs::write(path, EVALUATION_ACN_MD_GUIDANCE)
        .await
        .with_context(|| format!("写入 evaluation ACN.md 失败: {}", path.display()))
}

/// B_forced_claim 在首轮 user turn 一次性附上冻结 router 的完整 claim bundle。
///
/// 专用交付入口会记录唯一 router evidence 并立即封闭模型侧查询；B_claim 仍由模型
/// 自主调用一次 `consult_router`，从而把强制交付和按需检索保持为两个清晰实验契约。
async fn task_prompt_with_forced_claims(
    config: &EvaluationAttemptConfig,
    router: &FrozenClaimBundleRouter,
) -> anyhow::Result<(String, Vec<String>)> {
    if config.variant != "B_forced_claim" {
        return Ok((config.task_prompt.clone(), Vec::new()));
    }
    let candidates = router
        .deliver_forced_claims_once(&config.task_prompt)
        .context("stage=router B_forced_claim 一次性交付冻结 claim bundle 失败")?;
    // 四臂评测允许 A 未产生 eligible claim。此时 bundle 仍经同一冻结与审计链路
    // 传入，但 B_forced_claim 没有可注入的内容，应退化为原任务 prompt 而非在请求
    // 模型前失败；Gate 会以 EMPTY_CLAIM_BUNDLE 保留该事实。
    if candidates.is_empty() {
        return Ok((config.task_prompt.clone(), Vec::new()));
    }
    let claim_ids = candidates
        .iter()
        .map(|candidate| candidate.claim.id.to_string())
        .collect::<Vec<_>>();
    let claims = candidates
        .into_iter()
        .map(|candidate| candidate.claim)
        .collect::<Vec<_>>();
    let rendered_claims = serde_json::to_string_pretty(&claims)
        .context("stage=router 序列化 B_forced_claim 上下文失败")?;
    let prompt = format!(
        "{task_prompt}\n\n以下是通过冻结 claim router 检索到的前序实验信息。它们可能不完整或不适用；\
         请按当前任务契约独立验证，只在确有帮助时使用。\n<frozen-claims>\n{rendered_claims}\n</frozen-claims>",
        task_prompt = config.task_prompt,
    );
    Ok((prompt, claim_ids))
}

/// 评测 turn 已正常结束后的 completion 归因。
///
/// `run_turn` 会把 provider 截断、无可消费输出和中断转成错误，只有正常完成的 assistant
/// 最终响应才会到达这里。因此遗漏 `submit_task` 时可安全进入同一条 finalize 路径，同时保留
/// 对显式提交与隐式结束的实验审计。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvaluationCompletionMode {
    ExplicitSubmitTask,
    ImplicitAssistantDone,
}

impl EvaluationCompletionMode {
    fn from_submission(submission: &EvaluationSubmission) -> Self {
        if submission.was_submitted() {
            Self::ExplicitSubmitTask
        } else {
            Self::ImplicitAssistantDone
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitSubmitTask => "explicit_submit_task",
            Self::ImplicitAssistantDone => "implicit_assistant_done",
        }
    }
}

fn buffer_evaluation_completion(
    event_tx: &mpsc::UnboundedSender<BufferedEvaluationEvent>,
    submission: &EvaluationSubmission,
) {
    let mode = EvaluationCompletionMode::from_submission(submission);
    buffer_direct_event(
        event_tx,
        "evaluation_completion",
        json!({"mode": mode.as_str()}),
    );
    if mode == EvaluationCompletionMode::ExplicitSubmitTask {
        buffer_direct_event(
            event_tx,
            "evaluation_submitted",
            json!({"tool": "submit_task"}),
        );
    }
}

fn buffer_direct_event(
    event_tx: &mpsc::UnboundedSender<BufferedEvaluationEvent>,
    event_type: &str,
    payload: Value,
) {
    let _ = event_tx.send(BufferedEvaluationEvent::Direct {
        event_type: event_type.to_string(),
        payload,
        timestamp_utc: Utc::now(),
    });
}

fn buffer_session_event(
    event_tx: &mpsc::UnboundedSender<BufferedEvaluationEvent>,
    event: SessionEvent,
) {
    let _ = event_tx.send(BufferedEvaluationEvent::Session {
        event,
        timestamp_utc: Utc::now(),
    });
}

fn drain_buffered_events(
    event_rx: &mut mpsc::UnboundedReceiver<BufferedEvaluationEvent>,
    events: &mut Vec<EvaluationEvent>,
    attempt_id: &str,
    compaction_report: &mut SessionFinalizeReport,
) {
    while let Ok(buffered) = event_rx.try_recv() {
        match buffered {
            BufferedEvaluationEvent::Direct {
                event_type,
                payload,
                timestamp_utc,
            } => record_event_at(events, attempt_id, &event_type, payload, timestamp_utc),
            BufferedEvaluationEvent::Session {
                event,
                timestamp_utc,
            } => {
                record_session_event_at(events, attempt_id, compaction_report, event, timestamp_utc)
            }
        }
    }
}

fn validate_evaluation_llm_provider(provider: LlmProvider) -> anyhow::Result<()> {
    match provider {
        LlmProvider::OpenAiChat | LlmProvider::OpenAiResponses => Ok(()),
        LlmProvider::Anthropic => anyhow::bail!(
            "stage=config 评测只支持 openai_chat 或 openai_responses provider: {provider:?}"
        ),
    }
}

async fn append_attempt_claim_snapshots(
    config: &EvaluationAttemptConfig,
    events: &mut Vec<EvaluationEvent>,
    report: &SessionFinalizeReport,
) -> anyhow::Result<()> {
    if report.new_claim_ids.is_empty() && report.updated_claim_ids.is_empty() {
        return Ok(());
    }
    let agent_id = evaluation_agent_id(&config.attempt_id)?;
    let claim_store = LocalFsClaimStore::new(
        config
            .runtime_root
            .join("data")
            .join("agents")
            .join(agent_id.as_str()),
    );
    let local_claims = claim_store
        .list_local_claims()
        .await
        .context("stage=claim_snapshot 读取本 attempt LocalFsClaimStore 失败")?;
    append_claim_snapshot_events(events, &config.attempt_id, report, local_claims)
}

fn append_claim_snapshot_events(
    events: &mut Vec<EvaluationEvent>,
    attempt_id: &str,
    report: &SessionFinalizeReport,
    local_claims: Vec<Claim>,
) -> anyhow::Result<()> {
    let expected_ids = report
        .new_claim_ids
        .iter()
        .chain(report.updated_claim_ids.iter())
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    let local_by_id = local_claims
        .into_iter()
        .map(|claim| (claim.id.to_string(), claim))
        .collect::<BTreeMap<_, _>>();
    for claim_id in expected_ids {
        let claim = local_by_id.get(&claim_id).ok_or_else(|| {
            anyhow::anyhow!(
                "stage=claim_snapshot report claim_id 未在本 attempt LocalFsClaimStore 中找到: {claim_id}"
            )
        })?;
        record_event(
            events,
            attempt_id,
            "claim_snapshot",
            json!({ "claim": claim }),
        );
    }
    Ok(())
}

async fn load_bundle_router(
    config: &EvaluationAttemptConfig,
) -> anyhow::Result<FrozenClaimBundleRouter> {
    let (bundle, bundle_hash) = match &config.claim_bundle {
        Some(path) => consume_frozen_claim_bundle(path).await?,
        None => (
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: Vec::new(),
            },
            None,
        ),
    };
    let policy = match config.variant.as_str() {
        "B_claim" => FrozenClaimDeliveryPolicy::OnDemandOnce,
        "B_forced_claim" => FrozenClaimDeliveryPolicy::ForcedOnce,
        "A" | "B_empty" => FrozenClaimDeliveryPolicy::Disabled,
        _ => anyhow::bail!("stage=router 未知评测 variant: {}", config.variant),
    };
    FrozenClaimBundleRouter::new(bundle, config.attempt_id.clone(), bundle_hash)
        .map(|router| router.with_delivery_policy(policy))
        .context("stage=router 构造冻结 bundle router 失败")
}

/// 读取后立即删除冻结 bundle，避免模型绕过 router 直接读取原始 claim 文件。
async fn consume_frozen_claim_bundle(
    path: &Path,
) -> anyhow::Result<(FrozenClaimBundle, Option<String>)> {
    let metadata = tokio::fs::symlink_metadata(path).await.with_context(|| {
        format!(
            "stage=router 读取冻结 claim bundle 元数据失败: {}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_file() {
        anyhow::bail!(
            "stage=router 冻结 claim bundle 必须是普通文件: {}",
            path.display()
        );
    }
    let bytes = tokio::fs::read(path).await.with_context(|| {
        format!(
            "stage=router 读取冻结 claim bundle 失败: {}",
            path.display()
        )
    })?;
    tokio::fs::remove_file(path).await.with_context(|| {
        format!(
            "stage=router 删除已消费的冻结 claim bundle 失败: {}",
            path.display()
        )
    })?;
    let bundle = serde_yaml_ng::from_slice::<FrozenClaimBundle>(&bytes).map_err(|error| {
        anyhow::anyhow!(
            "stage=router 解析已消费的冻结 claim bundle 失败: {} ({error})",
            path.display()
        )
    })?;
    Ok((bundle, Some(sha256_hex(&bytes))))
}

fn evaluation_agent_id(attempt_id: &str) -> anyhow::Result<AgentId> {
    let digest = sha256_hex(attempt_id.as_bytes());
    AgentId::new(format!("eval_{}", &digest[..16]))
        .map_err(|error| anyhow::anyhow!("stage=identity 构造唯一评测 agent_id 失败: {error}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

fn record_model_request_events(
    events: &mut Vec<EvaluationEvent>,
    attempt_id: &str,
    recorder: &EvaluationUsageRecorder,
) -> EvaluationUsageTotals {
    let records = recorder.take_records();
    for record in &records {
        record_event(events, attempt_id, "model_request", json!(record));
    }
    let mut totals = EvaluationUsageTotals::accumulate(&records);
    totals.audit_incomplete = recorder.audit_is_incomplete();
    totals
}

fn classify_evaluation_failure(error: &anyhow::Error) -> Option<EvaluationFailureKind> {
    let message = format!("{error:#}");
    let is_http_429 = message.contains("LLM provider returned HTTP 429");
    let concurrency_exhausted = message.contains("channel:concurrency_full");
    if is_http_429 && concurrency_exhausted {
        return Some(EvaluationFailureKind::UpstreamConcurrencyExhausted);
    }
    None
}

fn result_from_finalize(
    config: &EvaluationAttemptConfig,
    paths: &EvaluationRunPaths,
    agent_steps: usize,
    report: SessionFinalizeReport,
    router_evidence: Vec<RouterEvidence>,
    router_evidence_incomplete: bool,
    usage: EvaluationUsageTotals,
) -> EvaluationResult {
    let claim_used_ids = report
        .used_claim_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    EvaluationResult {
        schema_version: EVALUATION_SCHEMA_VERSION,
        attempt_id: config.attempt_id.clone(),
        exit_type: "completed".into(),
        agent_steps,
        claim_new_ids: report
            .new_claim_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        claim_updated_ids: report
            .updated_claim_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        router_evidence,
        router_evidence_incomplete,
        usage,
        claim_used_ids,
        event_ledger_path: paths.event_ledger.clone(),
        failure_kind: None,
        error: None,
    }
}

fn apply_claim_attribution(result: &mut EvaluationResult, report: &SessionFinalizeReport) {
    result.claim_new_ids = report
        .new_claim_ids
        .iter()
        .map(ToString::to_string)
        .collect();
    result.claim_updated_ids = report
        .updated_claim_ids
        .iter()
        .map(ToString::to_string)
        .collect();
    result.claim_used_ids = report
        .used_claim_ids
        .iter()
        .map(ToString::to_string)
        .collect();
}

fn record_session_event_at(
    events: &mut Vec<EvaluationEvent>,
    attempt_id: &str,
    compaction_report: &mut SessionFinalizeReport,
    event: SessionEvent,
    timestamp_utc: DateTime<Utc>,
) {
    let (kind, payload) = match event {
        SessionEvent::AssistantMessageCompleted { .. }
        | SessionEvent::NonStreamingFallbackSucceeded { .. } => ("assistant_completed", json!({})),
        SessionEvent::ToolCallStarted { id, name, .. } => {
            ("tool_started", json!({"id": id, "name": name}))
        }
        SessionEvent::ToolCallCompleted { id, outcome, .. } => (
            "tool_completed",
            json!({"id": id, "outcome": format!("{outcome:?}")}),
        ),
        SessionEvent::TurnCommitted { message_count } => {
            ("turn_committed", json!({ "message_count": message_count }))
        }
        SessionEvent::FinalizeCompleted {
            new_claim_ids,
            updated_claim_ids,
            ..
        } => (
            "finalize_completed",
            json!({"new_claim_ids": new_claim_ids, "updated_claim_ids": updated_claim_ids}),
        ),
        SessionEvent::CompactionCompleted {
            new_claim_ids,
            updated_claim_ids,
            used_claim_ids,
            ..
        } => {
            add_claim_attribution(
                compaction_report,
                &new_claim_ids,
                &updated_claim_ids,
                &used_claim_ids,
            );
            (
                "compaction_completed",
                json!({
                    "new_claim_ids": new_claim_ids,
                    "updated_claim_ids": updated_claim_ids,
                    "used_claim_ids": used_claim_ids,
                }),
            )
        }
        SessionEvent::TurnFailed { error } | SessionEvent::FinalizeFailed { error } => {
            ("session_error", json!({ "error": error }))
        }
        _ => return,
    };
    record_event_at(events, attempt_id, kind, payload, timestamp_utc);
}

/// 评测只汇总本 attempt 的 claim 归因字段，不携带 report 的 trace、warning 或 recap 状态。
fn merge_attempt_claim_reports(
    compaction_report: SessionFinalizeReport,
    finalize_report: SessionFinalizeReport,
) -> SessionFinalizeReport {
    let mut merged = SessionFinalizeReport::default();
    add_claim_attribution(
        &mut merged,
        &compaction_report.new_claim_ids,
        &compaction_report.updated_claim_ids,
        &compaction_report.used_claim_ids,
    );
    add_claim_attribution(
        &mut merged,
        &finalize_report.new_claim_ids,
        &finalize_report.updated_claim_ids,
        &finalize_report.used_claim_ids,
    );
    merged
}

fn add_claim_attribution(
    report: &mut SessionFinalizeReport,
    new_claim_ids: &[ClaimId],
    updated_claim_ids: &[ClaimId],
    used_claim_ids: &[ClaimId],
) {
    extend_unique_claim_ids(&mut report.new_claim_ids, new_claim_ids);
    for claim_id in updated_claim_ids {
        if !report.new_claim_ids.contains(claim_id) && !report.updated_claim_ids.contains(claim_id)
        {
            report.updated_claim_ids.push(claim_id.clone());
        }
    }
    extend_unique_claim_ids(&mut report.used_claim_ids, used_claim_ids);
}

fn extend_unique_claim_ids(target: &mut Vec<ClaimId>, source: &[ClaimId]) {
    for claim_id in source {
        if !target.contains(claim_id) {
            target.push(claim_id.clone());
        }
    }
}

fn record_event(events: &mut Vec<EvaluationEvent>, attempt_id: &str, kind: &str, payload: Value) {
    record_event_at(events, attempt_id, kind, payload, Utc::now());
}

fn record_event_at(
    events: &mut Vec<EvaluationEvent>,
    attempt_id: &str,
    event_type: &str,
    payload: Value,
    timestamp_utc: DateTime<Utc>,
) {
    // Vec 的最大长度受 isize::MAX 限制，故 len + 1 不会耗尽 usize。
    let seq = events.len() + 1;
    events.push(EvaluationEvent::new(
        attempt_id,
        seq,
        event_type,
        payload,
        timestamp_utc,
    ));
}

async fn prepare_output_dir(
    config: &EvaluationAttemptConfig,
    paths: &EvaluationRunPaths,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&config.output_dir)
        .await
        .with_context(|| {
            format!(
                "stage=output 创建输出目录失败: {}",
                config.output_dir.display()
            )
        })?;
    for path in [&paths.event_ledger, &paths.result] {
        if path.exists() {
            anyhow::bail!("stage=output 拒绝覆盖已有评测输出: {}", path.display());
        }
    }
    Ok(())
}

struct EvaluationArtifactWrite {
    events: Vec<u8>,
    result: Vec<u8>,
    ack: oneshot::Sender<anyhow::Result<()>>,
}

struct EvaluationArtifactWriter {
    tx: std_mpsc::Sender<EvaluationArtifactWrite>,
}

impl EvaluationArtifactWriter {
    fn start(paths: EvaluationRunPaths) -> anyhow::Result<Self> {
        let (tx, rx) = std_mpsc::channel::<EvaluationArtifactWrite>();
        std::thread::Builder::new()
            .name("acn-eval-artifacts".into())
            .spawn(move || {
                if let Ok(write) = rx.recv() {
                    let result =
                        write_evaluation_artifacts_blocking(&paths, &write.events, &write.result);
                    let _ = write.ack.send(result);
                }
            })
            .map_err(|error| anyhow::anyhow!("stage=output 启动评测产物持久化线程失败: {error}"))?;
        Ok(Self { tx })
    }

    async fn persist(
        self,
        events: &[EvaluationEvent],
        result: &EvaluationResult,
    ) -> anyhow::Result<()> {
        let mut encoded_events = Vec::new();
        for event in events {
            serde_json::to_writer(&mut encoded_events, event)
                .context("stage=output 序列化 event 失败")?;
            encoded_events.push(b'\n');
        }
        let mut encoded_result =
            serde_json::to_vec_pretty(result).context("stage=output 序列化 result 失败")?;
        encoded_result.push(b'\n');
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(EvaluationArtifactWrite {
                events: encoded_events,
                result: encoded_result,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("stage=output 评测产物持久化线程已停止"))?;
        match ack_rx.await {
            Ok(result) => result,
            Err(_) => anyhow::bail!("stage=output 评测产物持久化线程未返回结果"),
        }
    }
}

fn write_evaluation_artifacts_blocking(
    paths: &EvaluationRunPaths,
    events: &[u8],
    result: &[u8],
) -> anyhow::Result<()> {
    write_new_file_blocking(&paths.event_ledger, events, "event ledger")?;
    write_new_file_blocking(&paths.result, result, "result")
}

fn write_new_file_blocking(path: &Path, content: &[u8], label: &str) -> anyhow::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("stage=output 创建 {label} 失败: {}", path.display()))?;
    file.write_all(content)
        .with_context(|| format!("stage=output 写入 {label} 失败: {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("stage=output 同步 {label} 失败: {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::evaluation_usage::{
        record_evaluation_request_started, record_evaluation_usage,
    };
    use serde_json::json;

    #[test]
    fn evaluation_agent_identity_is_deterministic_and_attempt_specific() {
        let first = evaluation_agent_id("task-1-A").unwrap();
        assert_eq!(first, evaluation_agent_id("task-1-A").unwrap());
        assert_ne!(first, evaluation_agent_id("task-1-B_empty").unwrap());
        assert!(first.as_str().starts_with("eval_"));
    }

    #[test]
    fn evaluation_accepts_both_openai_wire_protocols() {
        assert!(validate_evaluation_llm_provider(LlmProvider::OpenAiChat).is_ok());
        assert!(validate_evaluation_llm_provider(LlmProvider::OpenAiResponses).is_ok());
        assert!(validate_evaluation_llm_provider(LlmProvider::Anthropic).is_err());
    }

    #[test]
    fn evaluation_attempt_config_accepts_frozen_model_egress_mode() {
        let config: EvaluationAttemptConfig = toml::from_str(
            r#"
schema_version = 1
attempt_id = "attempt-001"
task_prompt = "repair the fixture"
workspace_root = "/tmp/acn-eval/workspace"
runtime_root = "/tmp/acn-eval/runtime"
acn_config = "/tmp/acn-eval/acn.toml"
output_dir = "/tmp/acn-eval/output"
upstream = "eval"
variant = "A"
attempt_deadline_secs = 1
model_egress_mode = "pier"
"#,
        )
        .unwrap();

        assert!(EvaluationRunPaths::from_config(&config).is_ok());
        assert_eq!(config.model_egress_mode, "pier");
        assert_eq!(config.harness_mode, EvaluationHarnessMode::Standard);
    }

    #[tokio::test]
    async fn evaluation_acn_md_contains_only_the_single_delivery_claim_contract() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ACN.md");

        write_evaluation_acn_md(&path).await.unwrap();

        assert_eq!(
            tokio::fs::read_to_string(path).await.unwrap(),
            EVALUATION_ACN_MD_GUIDANCE
        );
        assert!(EVALUATION_ACN_MD_GUIDANCE.contains("不要再调用 overview"));
        assert!(EVALUATION_ACN_MD_GUIDANCE.contains("调用一次 `consult_router` query"));
        assert!(EVALUATION_ACN_MD_GUIDANCE.contains("<frozen-claims>"));
        assert!(EVALUATION_ACN_MD_GUIDANCE.contains("consult_router"));
        assert!(EVALUATION_ACN_MD_GUIDANCE.contains("候选 claim 只是此前探索的经验和线索"));
        assert!(!EVALUATION_ACN_MD_GUIDANCE.contains("submit_task"));
        assert!(!EVALUATION_ACN_MD_GUIDANCE.contains("git commit"));
    }

    #[test]
    fn evaluation_completion_distinguishes_explicit_submission_from_normal_assistant_done() {
        let submission = EvaluationSubmission::new();
        let mut events = Vec::new();
        let mut compaction_report = SessionFinalizeReport::default();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        buffer_evaluation_completion(&event_tx, &submission);
        drain_buffered_events(
            &mut event_rx,
            &mut events,
            "attempt-001",
            &mut compaction_report,
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "evaluation_completion");
        assert_eq!(
            events[0].payload,
            json!({"mode": "implicit_assistant_done"})
        );

        submission.mark_submitted();
        buffer_evaluation_completion(&event_tx, &submission);
        drain_buffered_events(
            &mut event_rx,
            &mut events,
            "attempt-001",
            &mut compaction_report,
        );
        assert_eq!(events.len(), 3);
        assert_eq!(events[1].event_type, "evaluation_completion");
        assert_eq!(events[1].payload, json!({"mode": "explicit_submit_task"}));
        assert_eq!(events[2].event_type, "evaluation_submitted");
        assert_eq!(events[2].payload, json!({"tool": "submit_task"}));
    }

    #[test]
    fn outer_ledger_assigns_one_strictly_increasing_sequence_to_buffered_events() {
        let mut events = Vec::new();
        let mut compaction_report = SessionFinalizeReport::default();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        record_event(&mut events, "attempt-001", "attempt_started", json!({}));
        buffer_direct_event(&event_tx, "forced_claim_context", json!({"claim_ids": []}));
        buffer_session_event(&event_tx, SessionEvent::TurnCommitted { message_count: 2 });

        drain_buffered_events(
            &mut event_rx,
            &mut events,
            "attempt-001",
            &mut compaction_report,
        );
        record_event(&mut events, "attempt-001", "attempt_finished", json!({}));

        assert_eq!(
            events.iter().map(|event| event.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(events[2].event_type, "turn_committed");
    }

    #[test]
    fn artifact_persistence_does_not_depend_on_the_tokio_blocking_pool() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("test runtime should build");
        runtime.block_on(async {
            let directory = tempfile::tempdir().expect("tempdir should be created");
            let paths = EvaluationRunPaths {
                event_ledger: directory.path().join("events.jsonl"),
                result: directory.path().join("result.json"),
            };
            let artifact_writer = EvaluationArtifactWriter::start(paths.clone())
                .expect("artifact writer should start before PID exhaustion");
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (blocking_started_tx, blocking_started_rx) = oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = blocking_started_tx.send(());
                let _ = release_rx.recv();
            });
            blocking_started_rx
                .await
                .expect("blocking worker should start");
            let events = vec![EvaluationEvent::new(
                "attempt-001",
                1,
                "attempt_finished",
                json!({"exit_type": "failed"}),
                Utc::now(),
            )];
            let result =
                EvaluationResult::empty("attempt-001", "failed", paths.event_ledger.clone());

            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                artifact_writer.persist(&events, &result),
            )
            .await
            .expect("artifact persistence must not wait for Tokio blocking capacity")
            .expect("artifact persistence should succeed");
            assert!(paths.event_ledger.is_file());
            assert!(paths.result.is_file());

            let _ = release_tx.send(());
            blocker.await.expect("blocking probe should finish");
        });
    }

    #[test]
    fn upstream_concurrency_exhaustion_is_classified_without_matching_other_429_errors() {
        let exhausted = anyhow::anyhow!(
            "LLM provider returned HTTP 429: {{\"error\":{{\"code\":\"channel:concurrency_full\"}}}}"
        );
        assert_eq!(
            classify_evaluation_failure(&exhausted),
            Some(EvaluationFailureKind::UpstreamConcurrencyExhausted)
        );

        let rate_limited = anyhow::anyhow!("LLM provider returned HTTP 429: rate limited");
        assert_eq!(classify_evaluation_failure(&rate_limited), None);
    }

    #[tokio::test]
    async fn forced_claim_prompt_comes_from_router_and_records_evidence() {
        let claim = snapshot_claim("claim_11111111", "frozen");
        let router = FrozenClaimBundleRouter::new(
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: vec![claim.clone()],
            },
            "attempt-001".into(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
        )
        .unwrap()
        .with_delivery_policy(FrozenClaimDeliveryPolicy::ForcedOnce);
        let config = EvaluationAttemptConfig {
            schema_version: EVALUATION_SCHEMA_VERSION,
            attempt_id: "attempt-001".into(),
            task_prompt: "repair the fixture".into(),
            workspace_root: PathBuf::from("/tmp/acn-eval/workspace"),
            runtime_root: PathBuf::from("/tmp/acn-eval/runtime"),
            acn_config: PathBuf::from("/tmp/acn-eval/acn.toml"),
            output_dir: PathBuf::from("/tmp/acn-eval/output"),
            upstream: "eval".into(),
            variant: "B_forced_claim".into(),
            attempt_deadline_secs: 1,
            model_egress_mode: "pier".into(),
            harness_mode: EvaluationHarnessMode::Standard,
            claim_bundle: Some(PathBuf::from("/tmp/acn-eval/claims.json")),
        };

        let (prompt, claim_ids) = task_prompt_with_forced_claims(&config, &router)
            .await
            .unwrap();

        assert!(prompt.contains("repair the fixture"));
        assert!(prompt.contains("<frozen-claims>"));
        assert!(prompt.contains(&claim.statement));
        assert_eq!(claim_ids, vec![claim.id.to_string()]);
        assert_eq!(router.take_evidence()[0].injected_claim_ids, claim_ids);
    }

    #[tokio::test]
    async fn forced_claim_prompt_allows_an_empty_frozen_bundle() {
        let router = FrozenClaimBundleRouter::new(
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: Vec::new(),
            },
            "attempt-001".into(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
        )
        .unwrap()
        .with_delivery_policy(FrozenClaimDeliveryPolicy::ForcedOnce);
        let config = EvaluationAttemptConfig {
            schema_version: EVALUATION_SCHEMA_VERSION,
            attempt_id: "attempt-001".into(),
            task_prompt: "repair the fixture".into(),
            workspace_root: PathBuf::from("/tmp/acn-eval/workspace"),
            runtime_root: PathBuf::from("/tmp/acn-eval/runtime"),
            acn_config: PathBuf::from("/tmp/acn-eval/acn.toml"),
            output_dir: PathBuf::from("/tmp/acn-eval/output"),
            upstream: "eval".into(),
            variant: "B_forced_claim".into(),
            attempt_deadline_secs: 1,
            model_egress_mode: "pier".into(),
            harness_mode: EvaluationHarnessMode::Standard,
            claim_bundle: Some(PathBuf::from("/tmp/acn-eval/claims.json")),
        };

        let (prompt, claim_ids) = task_prompt_with_forced_claims(&config, &router)
            .await
            .unwrap();

        assert_eq!(prompt, config.task_prompt);
        assert!(claim_ids.is_empty());
        let evidence = router.take_evidence();
        assert_eq!(evidence.len(), 1);
        assert!(evidence[0].injected_claim_ids.is_empty());
    }

    #[tokio::test]
    async fn ordinary_claim_variant_does_not_force_router_query() {
        let router = FrozenClaimBundleRouter::new(
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: vec![snapshot_claim("claim_11111111", "frozen")],
            },
            "attempt-001".into(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
        )
        .unwrap();
        let config = EvaluationAttemptConfig {
            schema_version: EVALUATION_SCHEMA_VERSION,
            attempt_id: "attempt-001".into(),
            task_prompt: "repair the fixture".into(),
            workspace_root: PathBuf::from("/tmp/acn-eval/workspace"),
            runtime_root: PathBuf::from("/tmp/acn-eval/runtime"),
            acn_config: PathBuf::from("/tmp/acn-eval/acn.toml"),
            output_dir: PathBuf::from("/tmp/acn-eval/output"),
            upstream: "eval".into(),
            variant: "B_claim".into(),
            attempt_deadline_secs: 1,
            model_egress_mode: "pier".into(),
            harness_mode: EvaluationHarnessMode::Standard,
            claim_bundle: Some(PathBuf::from("/tmp/acn-eval/claims.json")),
        };

        let (prompt, claim_ids) = task_prompt_with_forced_claims(&config, &router)
            .await
            .unwrap();

        assert_eq!(prompt, config.task_prompt);
        assert!(claim_ids.is_empty());
        assert!(router.take_evidence().is_empty());
    }

    #[test]
    fn claim_snapshot_events_contain_only_reported_new_or_updated_full_claims() {
        let new_claim = snapshot_claim("claim_11111111", "new");
        let updated_claim = snapshot_claim("claim_22222222", "updated");
        let unrelated = snapshot_claim("claim_33333333", "unrelated");
        let report = SessionFinalizeReport {
            new_claim_ids: vec![new_claim.id.clone()],
            updated_claim_ids: vec![updated_claim.id.clone()],
            ..Default::default()
        };
        let mut events = Vec::new();

        append_claim_snapshot_events(
            &mut events,
            "attempt-001",
            &report,
            vec![unrelated, updated_claim.clone(), new_claim.clone()],
        )
        .unwrap();

        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|event| event.event_type == "claim_snapshot"));
        assert_eq!(
            events[0].payload["claim"],
            serde_json::to_value(new_claim).unwrap()
        );
        assert_eq!(
            events[1].payload["claim"],
            serde_json::to_value(updated_claim).unwrap()
        );
    }

    #[test]
    fn compaction_claim_attribution_is_merged_into_result_and_snapshots() {
        let compaction_new = snapshot_claim("claim_11111111", "compaction-new");
        let compaction_updated = snapshot_claim("claim_22222222", "compaction-updated");
        let compaction_used: ClaimId = "claim_33333333".parse().unwrap();
        let finalize_updated = snapshot_claim("claim_44444444", "finalize-updated");
        let finalize_used: ClaimId = "claim_55555555".parse().unwrap();
        let mut compaction_report = SessionFinalizeReport::default();
        let mut events = Vec::new();

        record_session_event_at(
            &mut events,
            "attempt-001",
            &mut compaction_report,
            SessionEvent::CompactionCompleted {
                compacted_until: 2,
                recapped_until: 4,
                new_claim_ids: vec![compaction_new.id.clone()],
                updated_claim_ids: vec![compaction_updated.id.clone()],
                used_claim_ids: vec![compaction_used.clone()],
                new_dispute_ids: Vec::new(),
            },
            Utc::now(),
        );
        let merged = merge_attempt_claim_reports(
            compaction_report,
            SessionFinalizeReport {
                updated_claim_ids: vec![compaction_new.id.clone(), finalize_updated.id.clone()],
                used_claim_ids: vec![compaction_used.clone(), finalize_used.clone()],
                ..Default::default()
            },
        );

        assert_eq!(merged.new_claim_ids, vec![compaction_new.id.clone()]);
        assert_eq!(
            merged.updated_claim_ids,
            vec![compaction_updated.id.clone(), finalize_updated.id.clone()]
        );
        assert_eq!(merged.used_claim_ids, vec![compaction_used, finalize_used]);
        assert_eq!(events[0].event_type, "compaction_completed");
        append_claim_snapshot_events(
            &mut events,
            "attempt-001",
            &merged,
            vec![
                compaction_new.clone(),
                compaction_updated.clone(),
                finalize_updated.clone(),
                snapshot_claim("claim_66666666", "unrelated"),
            ],
        )
        .unwrap();
        let snapshot_ids = events
            .iter()
            .filter(|event| event.event_type == "claim_snapshot")
            .map(|event| event.payload["claim"]["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            snapshot_ids,
            vec![
                compaction_new.id.to_string(),
                compaction_updated.id.to_string(),
                finalize_updated.id.to_string(),
            ]
        );
    }

    #[test]
    fn failed_attempt_result_keeps_partial_compaction_claim_attribution() {
        let new_claim: ClaimId = "claim_11111111".parse().unwrap();
        let updated_claim: ClaimId = "claim_22222222".parse().unwrap();
        let used_claim: ClaimId = "claim_33333333".parse().unwrap();
        let report = SessionFinalizeReport {
            new_claim_ids: vec![new_claim.clone()],
            updated_claim_ids: vec![updated_claim.clone()],
            used_claim_ids: vec![used_claim.clone()],
            ..Default::default()
        };
        let mut result = EvaluationResult::empty(
            "attempt-001",
            "failed",
            PathBuf::from("/tmp/acn-eval/events.jsonl"),
        );

        apply_claim_attribution(&mut result, &report);

        assert_eq!(result.claim_new_ids, vec![new_claim.to_string()]);
        assert_eq!(result.claim_updated_ids, vec![updated_claim.to_string()]);
        assert_eq!(result.claim_used_ids, vec![used_claim.to_string()]);
    }

    #[tokio::test]
    async fn timeout_cancellation_leaves_outer_compaction_attribution_available() {
        let claim_id: ClaimId = "claim_11111111".parse().unwrap();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let mut outer_report = SessionFinalizeReport::default();
        let buffered_claim_id = claim_id.clone();
        let attempt_task = tokio::spawn(async move {
            buffer_session_event(
                &event_tx,
                SessionEvent::CompactionCompleted {
                    compacted_until: 1,
                    recapped_until: 1,
                    new_claim_ids: vec![buffered_claim_id],
                    updated_claim_ids: Vec::new(),
                    used_claim_ids: Vec::new(),
                    new_dispute_ids: Vec::new(),
                },
            );
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        attempt_task.abort();
        let _ = attempt_task.await;
        let mut events = Vec::new();
        drain_buffered_events(&mut event_rx, &mut events, "attempt-001", &mut outer_report);

        assert_eq!(outer_report.new_claim_ids, vec![claim_id]);
        assert_eq!(events[0].event_type, "compaction_completed");
    }

    fn snapshot_claim(id: &str, name: &str) -> Claim {
        Claim {
            id: id.parse().unwrap(),
            name: name.into(),
            statement: "full claim content".into(),
            scope: "snapshot/test".into(),
            holder: AgentId::new("eval_test").unwrap(),
            confidence: crate::claim::Confidence::High,
            status: crate::claim::ClaimStatus::Active,
            created_at: "2026-07-26T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "fixture".into(),
        }
    }

    #[tokio::test]
    async fn frozen_bundle_is_consumed_deleted_and_remains_queryable_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("claims.yaml");
        let claim = snapshot_claim("claim_44444444", "frozen");
        let source = serde_yaml_ng::to_string(&FrozenClaimBundle {
            schema_version: EVALUATION_SCHEMA_VERSION,
            claims: vec![claim.clone()],
        })
        .unwrap();
        tokio::fs::write(&path, source).await.unwrap();

        let (bundle, _) = consume_frozen_claim_bundle(&path).await.unwrap();
        assert!(!path.exists());
        let router = FrozenClaimBundleRouter::new(
            bundle,
            "test-attempt".into(),
            Some("0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a".into()),
        )
        .unwrap();
        let result = crate::router::RouterClient::query(
            &router,
            &crate::router::AgentQuery::from_scope("snapshot/test"),
        )
        .await
        .unwrap();
        assert_eq!(result.candidate_claims[0].claim, claim);
    }

    #[tokio::test]
    async fn frozen_bundle_rejects_non_file_without_echoing_claim_content() {
        let dir = tempfile::tempdir().unwrap();
        let error = consume_frozen_claim_bundle(dir.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("stage=router"));
        assert!(!error.contains("full claim content"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn frozen_bundle_delete_failure_is_router_stage_and_does_not_echo_claim() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("claims.yaml");
        tokio::fs::write(
            &path,
            "schema_version: 1\nclaims:\n  - statement: secret claim content\n",
        )
        .await
        .unwrap();
        tokio::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
            .await
            .unwrap();
        let error = consume_frozen_claim_bundle(&path)
            .await
            .unwrap_err()
            .to_string();
        tokio::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .await
            .unwrap();

        assert!(error.contains("stage=router"));
        assert!(error.contains("删除已消费"));
        assert!(!error.contains("secret claim content"));
    }

    #[test]
    fn usage_totals_sum_every_model_request() {
        let totals = EvaluationUsageTotals::accumulate(&[
            EvaluationUsage {
                request_sequence: 1,
                response_received: true,
                model: Some("model-b".into()),
                is_complete: true,
                input_tokens: 8462,
                output_tokens: 53,
                cache_read_tokens: 0,
                reasoning_tokens: 41,
            },
            EvaluationUsage {
                request_sequence: 2,
                response_received: true,
                model: Some("model-a".into()),
                is_complete: false,
                input_tokens: 8636,
                output_tokens: 164,
                cache_read_tokens: 8448,
                reasoning_tokens: 120,
            },
        ]);

        assert_eq!(
            totals,
            EvaluationUsageTotals {
                model_requests: 2,
                complete_model_responses: 1,
                incomplete_model_responses: 1,
                audit_incomplete: false,
                response_models: vec!["model-a".into(), "model-b".into()],
                input_tokens: 17098,
                output_tokens: 217,
                cache_read_tokens: 8448,
                reasoning_tokens: 161,
            }
        );
        assert_eq!(
            EvaluationUsageTotals::accumulate(&[]),
            EvaluationUsageTotals::default()
        );
    }

    #[tokio::test]
    async fn agent_steps_include_response_emitted_during_finalize() {
        let recorder = Arc::new(EvaluationUsageRecorder::default());
        with_evaluation_usage_recording(recorder.clone(), async {
            let turn_response = record_evaluation_request_started().unwrap();
            record_evaluation_usage(
                Some(turn_response),
                Some(&json!({"prompt_tokens": 3, "completion_tokens": 2})),
                Some("eval-model"),
            );
            // finalize 期间的模型调用与 turn 调用使用同一个计步口径。
            let finalize_response = record_evaluation_request_started().unwrap();
            record_evaluation_usage(
                Some(finalize_response),
                Some(&json!({"prompt_tokens": 4, "completion_tokens": 1})),
                Some("eval-model"),
            );
        })
        .await;

        let agent_steps = recorder.response_count();
        let totals = EvaluationUsageTotals::accumulate(&recorder.take_records());
        assert_eq!(agent_steps, 2);
        assert_eq!(totals.model_requests, 2);
        assert_eq!(totals.complete_model_responses, 2);
    }
}
