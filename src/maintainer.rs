//! Maintainer：发布 policy / 关闭 dispute / 跑 stale sweep / 维护 outbox 投递台账。
//!
//! 当前实现直接基于本地文件系统：
//! - 写 `team_root/maintainer/{policies,disputes}/`（policy 发布、dispute 关闭）
//! - 扫 `team_root/agents/*/claims/`（评估 stale；按 updated_at 优先、created_at 兜底）
//! - 写 `team_root/maintainer/outbox/<inbox_id>.yaml`（每条投递对一条 entry）
//! - 处理 agent 主动 `pull_inbox(agent_id)`：lazy register + 持久 offer；本地落盘后显式 ACK
//!
//! 当前刻意不做：
//! - 基于 trace 的“内化建议”自动推送
//! - dispute 自动检测；dispute 由 Agent 报告、Maintainer 解决，Router 只反映派生状态

pub mod arbitration;
pub mod history;
pub mod http_client;
pub mod outbox_io;
pub mod server;
pub mod traits;

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::Mutex;

#[cfg(test)]
use crate::claim::DisputeId;
use crate::claim::{
    AgentId, Claim, ClaimId, ClaimStatus, Dispute, DisputeStatus, InboxId, InboxMessage,
    InboxMessageKind, MaintainerActionId, OutboxEntry, OutboxTarget, Policy, PolicyId,
    PolicyMessageType, PolicyStatus,
};
use crate::storage::{mint_unique_id_in_dir, paths, read_yaml, write_yaml_atomic, FileLockGuard};
use crate::time::serde_utc;
use history::HistoryStore;

pub type DeliveryMessageType = PolicyMessageType;

const CLAIM_ATTRIBUTE_POLICY_NAME: &str = "claim_attribute_update_suggestion";
const CLAIM_ATTRIBUTE_POLICY_SCOPE: &str = "maintainer / claim-attribute-update";

struct SweepAgentClaims {
    agent_id: AgentId,
    stale_claims: Vec<ClaimId>,
    deprecated_claims: Vec<ClaimId>,
}

/// 一次 claim sweep 的产出，便于上层日志、前端详情或测试断言。
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimSweepReport {
    pub stale_claims: Vec<(AgentId, ClaimId)>,
    pub deprecated_claims: Vec<(AgentId, ClaimId)>,
    #[serde(default)]
    pub notifications: Vec<SweepNotification>,
    #[serde(default)]
    pub notification_errors: Vec<SweepNotificationError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepNotification {
    pub agent_id: AgentId,
    pub stale_claims: Vec<ClaimId>,
    pub deprecated_claims: Vec<ClaimId>,
    pub policy_id: PolicyId,
    pub pushed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepNotificationError {
    pub agent_id: AgentId,
    pub stale_claims: Vec<ClaimId>,
    pub deprecated_claims: Vec<ClaimId>,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStatusSummary {
    pub agent_id: AgentId,
    pub mirror_claims: usize,
    pub active_claims: usize,
    pub stale_claims: usize,
    pub deprecated_claims: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintainerStatusCounts {
    pub agents: usize,
    pub claims: usize,
    pub active_claims: usize,
    pub stale_claims: usize,
    pub deprecated_claims: usize,
    pub active_policies: usize,
    pub deprecated_policies: usize,
    pub open_disputes: usize,
    pub resolved_disputes: usize,
    /// outbox 中尚有 broadcast / targeted entry 没投递给某些 agent 的 entry 总数
    pub outbox_entries: usize,
    /// outbox 投递事件总数 = 所有 entry delivered_to 的总长度
    pub send_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintainerStatusSnapshot {
    #[serde(with = "serde_utc")]
    pub generated_at: DateTime<Utc>,
    pub counts: MaintainerStatusCounts,
    pub agents: Vec<AgentStatusSummary>,
    pub policies: Vec<Policy>,
    pub disputes: Vec<Dispute>,
    pub actions: Vec<MaintainerActionRow>,
    /// 按 sent_at 降序，dashboard 用最近 N 行渲染表格。
    pub send_log: Vec<SendLogRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintainerActionRow {
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    pub maintainer_action_id: MaintainerActionId,
    pub message_type: PolicyMessageType,
    pub policy_id: PolicyId,
    pub policy_name: String,
    pub policy_scope: String,
    pub policy_status: PolicyStatus,
    pub target_kind: String,
    pub inbox_ids: Vec<InboxId>,
    pub target_agents: Vec<AgentId>,
    pub delivered_agents: Vec<AgentId>,
    pub outbox_entries: usize,
    pub send_events: usize,
}

/// 派生于 outbox 的"按时间序展开的发送日志"一行；同一 broadcast outbox entry
/// 会展开成 N 行（每个收件 agent 一行）。dashboard 与对外 audit 接口共用此结构。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendLogRow {
    #[serde(with = "serde_utc")]
    pub sent_at: DateTime<Utc>,
    pub agent_id: AgentId,
    pub inbox_id: InboxId,
    pub maintainer_action_id: MaintainerActionId,
    pub policy_id: PolicyId,
    pub message_type: PolicyMessageType,
}

/// Maintainer 拒绝 Inbox 收件 ACK 的原因。
#[derive(Debug, thiserror::Error)]
pub enum InboxAckError {
    #[error("未知 inbox_id: {inbox_id}")]
    UnknownInbox { inbox_id: InboxId },
    #[error("agent={agent_id} 无权 ACK targeted inbox_id={inbox_id}")]
    TargetMismatch {
        agent_id: AgentId,
        inbox_id: InboxId,
    },
    #[error("agent={agent_id} 尚未获 offer，不能 ACK inbox_id={inbox_id}")]
    NotOffered {
        agent_id: AgentId,
        inbox_id: InboxId,
    },
    #[error("读取 outbox 失败: {0}")]
    ReadOutbox(#[source] anyhow::Error),
    #[error("获取 outbox 跨进程锁失败: {0}")]
    LockOutbox(#[source] anyhow::Error),
    #[error("持久化 inbox_id={inbox_id} 的 ACK 失败: {source}")]
    PersistAck {
        inbox_id: InboxId,
        #[source]
        source: anyhow::Error,
    },
}

pub struct Maintainer {
    team_root: PathBuf,
    /// active → stale 的最小年龄（基于 claim 最近语义更新时间）
    stale_after: Duration,
    /// stale → deprecated 的最小年龄（同样基于 claim 最近语义更新时间）
    deprecate_after: Duration,
    /// Maintainer 生成 policy、inbox、action ID 的最大尝试次数（首次 + 重抽），由配置注入避免硬编码。
    id_mint_max_attempts: usize,
    history_store: HistoryStore,
    /// 串行化当前进程内的 publish + pull + ACK + outbox 任意写操作。
    /// 跨进程边界由 `maintainer/outbox.lock` 继续保护，避免同一 team store 的复合读改写交错。
    outbox_lock: Mutex<()>,
    #[cfg(test)]
    outbox_lock_attempt_notify: Option<std::sync::Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    action_id_candidates:
        Option<std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<MaintainerActionId>>>>,
}

impl Maintainer {
    pub fn new(
        team_root: PathBuf,
        stale_after: Duration,
        deprecate_after: Duration,
        id_mint_max_attempts: usize,
    ) -> Self {
        let history_store = HistoryStore::with_defaults(team_root.clone());
        Self::with_history_store(
            team_root,
            stale_after,
            deprecate_after,
            id_mint_max_attempts,
            history_store,
        )
    }

    pub fn with_history_store(
        team_root: PathBuf,
        stale_after: Duration,
        deprecate_after: Duration,
        id_mint_max_attempts: usize,
        history_store: HistoryStore,
    ) -> Self {
        Self {
            team_root,
            stale_after,
            deprecate_after,
            id_mint_max_attempts,
            history_store,
            outbox_lock: Mutex::new(()),
            #[cfg(test)]
            outbox_lock_attempt_notify: None,
            #[cfg(test)]
            action_id_candidates: None,
        }
    }

    pub fn history_store(&self) -> &HistoryStore {
        &self.history_store
    }

    pub fn team_root(&self) -> &Path {
        &self.team_root
    }

    #[cfg(test)]
    fn with_outbox_lock_attempt_notifier(
        mut self,
        notifier: std::sync::Arc<tokio::sync::Notify>,
    ) -> Self {
        self.outbox_lock_attempt_notify = Some(notifier);
        self
    }

    #[cfg(test)]
    fn with_action_id_candidates(
        mut self,
        candidates: impl IntoIterator<Item = MaintainerActionId>,
    ) -> Self {
        self.action_id_candidates = Some(std::sync::Arc::new(std::sync::Mutex::new(
            candidates.into_iter().collect(),
        )));
        self
    }

    /// 获取跨 Maintainer 进程共享的 outbox 锚定锁。
    async fn lock_outbox_file(&self) -> anyhow::Result<FileLockGuard> {
        #[cfg(test)]
        if let Some(notifier) = &self.outbox_lock_attempt_notify {
            notifier.notify_one();
        }
        let path = paths::team_store_outbox_lock_path(&self.team_root);
        let guard = FileLockGuard::lock_exclusive(&path)
            .await
            .with_context(|| format!("获取 maintainer outbox 锁失败: {path:?}"))?;
        #[cfg(test)]
        self.pause_after_outbox_lock_for_subprocess_test().await?;
        Ok(guard)
    }

    /// 子进程回归测试专用：确认真实 Maintainer 已拿锁后暂停，供另一进程验证等待语义。
    #[cfg(test)]
    async fn pause_after_outbox_lock_for_subprocess_test(&self) -> anyhow::Result<()> {
        let Some(ready_path) = std::env::var_os("ACN_TEST_OUTBOX_LOCK_READY") else {
            return Ok(());
        };
        let release_path = std::env::var_os("ACN_TEST_OUTBOX_LOCK_RELEASE")
            .context("子进程缺少 outbox 锁释放标记路径")?;
        tokio::fs::write(&ready_path, b"ready").await?;

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if tokio::fs::try_exists(&release_path).await? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("子进程等待父测试释放 outbox 锁超时");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// 发布一条新 policy：内部 mint id、写 `maintainer/policies/`、按 target_agents 推送 PolicyUpdate。
    /// None 和空列表统一表示全员广播、非空列表表示只发给指定 agent。
    /// 唯一对外发布入口；不再暴露独立的 `mint_policy_id`，避免调用方拿到 id 后绕开 mint+publish 配对。
    /// 返回 (新 policy id, 成功推送的 agent 数)。
    pub async fn publish_new_policy(
        &self,
        name: String,
        statement: String,
        scope: String,
        now: DateTime<Utc>,
        target_agents: Option<Vec<AgentId>>,
    ) -> anyhow::Result<(crate::claim::PolicyId, usize)> {
        let _guard = self.outbox_lock.lock().await;
        let _file_guard = self.lock_outbox_file().await?;
        let action_id = self.mint_action_id_for_outbox().await?;
        let id = self.mint_policy_id().await?;
        let policy = Policy {
            id: id.clone(),
            message_type: PolicyMessageType::PolicyUpdate,
            name,
            statement,
            scope,
            status: PolicyStatus::Active,
            created_at: now,
            updated_at: None,
            target_agents: normalize_target_agents(target_agents),
        };
        self.write_policy(&policy).await?;
        let pushed = self.create_outbox_entries(&policy, &action_id, now).await?;
        log::info!(
            target: "maintainer",
            "publish_new_policy id={} name={} → 写文件 + 落 outbox {} 条",
            policy.id, policy.name, pushed
        );
        Ok((id, pushed))
    }

    /// 申请一个唯一 PolicyId：在 `maintainer/policies/` 目录里查重，最多尝试 `id_mint_max_attempts` 次。
    /// 私有：仅由 `publish_new_policy` 内部调用，避免外部"先 mint 再随手 publish"的反模式。
    async fn mint_policy_id(&self) -> anyhow::Result<PolicyId> {
        let dir = paths::team_store_policies_dir(&self.team_root);
        mint_unique_id_in_dir(&dir, PolicyId::random, self.id_mint_max_attempts).await
    }

    /// 废弃 policy：更新 maintainer/policies 中的状态，并沿用原发布范围下发撤销消息。
    pub async fn deprecate_policy(
        &self,
        policy_id: &crate::claim::PolicyId,
        now: DateTime<Utc>,
    ) -> anyhow::Result<usize> {
        let _guard = self.outbox_lock.lock().await;
        let _file_guard = self.lock_outbox_file().await?;
        let p = paths::team_store_policies_dir(&self.team_root).join(format!("{policy_id}.yaml"));
        let mut policy: Policy = read_yaml(&p).await?;
        policy.status = PolicyStatus::Deprecated;
        policy.updated_at = Some(now);
        policy.target_agents = normalize_target_agents(policy.target_agents);
        let action_id = self.mint_action_id_for_outbox().await?;
        write_yaml_atomic(&p, &policy).await?;
        let pushed = self.create_outbox_entries(&policy, &action_id, now).await?;
        log::info!(
            target: "maintainer",
            "deprecate_policy id={} → 更新文件 + 落 outbox {} 条",
            policy_id, pushed
        );
        Ok(pushed)
    }

    /// 测试底层 replay 兼容时使用；生产关闭入口必须经 ResolutionService，确保
    /// resolved Dispute 始终同时拥有结构化 Resolution。
    #[cfg(test)]
    async fn mark_dispute_resolved_for_test(
        &self,
        dispute_id: &DisputeId,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let store = arbitration::ArbitrationStore::new(self.team_root.clone());
        let _guard = store.lock_dispute(dispute_id).await?;
        let mut record = store.read_dispute(dispute_id).await?;
        if record.dispute.status != DisputeStatus::Open {
            anyhow::bail!("dispute={dispute_id} 已 resolved");
        }
        record.dispute.status = DisputeStatus::Resolved;
        record.dispute.resolved_at = Some(now);
        store.write_dispute(&record).await?;
        log::info!(
            target: "maintainer",
            "mark_dispute_resolved_for_test id={} → status=resolved",
            dispute_id
        );
        Ok(())
    }

    /// 接收 agent 主动上传的 claim mirror。
    pub async fn upload_claim(&self, claim: &Claim) -> anyhow::Result<()> {
        let lock_path = paths::team_store_agent_claim_mirror_lock_path(
            &self.team_root,
            &claim.holder,
            &claim.id,
        );
        // Router 会在同一锁下复核 Claim 后发布派生 target；镜像写入不能插入该临界区。
        let _mirror_guard = FileLockGuard::lock_exclusive(&lock_path)
            .await
            .with_context(|| format!("获取 Claim 镜像写入锁失败: {lock_path:?}"))?;
        let dir = paths::team_store_agent_claims_dir(&self.team_root, &claim.holder);
        let path = dir.join(format!("{}.yaml", claim.id));
        write_yaml_atomic(&path, claim).await?;
        Ok(())
    }

    /// 接收 agent 主动上报的 dispute 文件。
    pub async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()> {
        dispute.validate_agent_report()?;
        let store = arbitration::ArbitrationStore::new(self.team_root.clone());
        let _guard = store.lock_dispute(&dispute.id).await?;
        match store.read_dispute(&dispute.id).await {
            Ok(existing) => {
                if !same_report_payload(&existing.dispute, dispute) {
                    return Err(arbitration::AnalysisConflict(format!(
                        "dispute id={} 已存在但原始字段不同",
                        dispute.id
                    ))
                    .into());
                }
                return Ok(());
            }
            Err(error) if arbitration_not_found(&error) => {}
            Err(error) => return Err(error),
        }
        validate_new_dispute_direct_claims(&self.team_root, dispute).await?;
        store
            .write_dispute(&arbitration::MaintainerDisputeRecord::from(dispute.clone()))
            .await?;
        Ok(())
    }

    /// 跑一遍 claim sweep：检测过期 claim，按 agent 发 ClaimAttributeUpdate 建议，不改写 mirror。
    /// 判定只看 mirror 中 claim 的最近语义更新时间，不读取 trace 引用频次。
    pub async fn run_stale_sweep(&self, now: DateTime<Utc>) -> anyhow::Result<ClaimSweepReport> {
        let mut report = ClaimSweepReport::default();
        for (agent, claim) in self.list_all_claims().await? {
            let age = now.signed_duration_since(claim.effective_updated_at());
            match claim.status {
                ClaimStatus::Active if age >= self.stale_after => {
                    let claim_id = claim.id.clone();
                    report.stale_claims.push((agent, claim_id));
                }
                ClaimStatus::Stale if age >= self.deprecate_after => {
                    let claim_id = claim.id.clone();
                    report.deprecated_claims.push((agent, claim_id));
                }
                _ => {}
            }
        }
        self.send_sweep_notifications(&mut report, now).await;
        log::info!(
            target: "maintainer",
            "run_stale_sweep: stale_claims={} deprecated_claims={} notifications={} notification_errors={}",
            report.stale_claims.len(),
            report.deprecated_claims.len(),
            report.notifications.len(),
            report.notification_errors.len()
        );
        Ok(report)
    }

    /// 跑 stale sweep 并记录触发来源，供 admin 工作台展示历史。
    pub async fn run_stale_sweep_with_trigger(
        &self,
        now: DateTime<Utc>,
        trigger: &str,
    ) -> anyhow::Result<ClaimSweepReport> {
        let report = self.run_stale_sweep(now).await?;
        let record = history::SweepRunRecord {
            run_id: history::fresh_record_id("sweep_run"),
            triggered_at: now,
            trigger: trigger.to_string(),
            report: report.clone(),
        };
        self.history_store.write_sweep_run(&record).await?;
        Ok(report)
    }

    async fn send_sweep_notifications(&self, report: &mut ClaimSweepReport, now: DateTime<Utc>) {
        let groups = group_sweep_claims_by_agent(report);
        for group in groups {
            let statement = claim_sweep_notification_statement(
                &group.agent_id,
                &group.stale_claims,
                &group.deprecated_claims,
            );
            match self
                .claim_update_suggestion(statement, now, Some(vec![group.agent_id.clone()]))
                .await
            {
                Ok((policy_id, pushed)) => {
                    report.notifications.push(SweepNotification {
                        agent_id: group.agent_id,
                        stale_claims: group.stale_claims,
                        deprecated_claims: group.deprecated_claims,
                        policy_id,
                        pushed,
                    });
                }
                Err(err) => {
                    log::warn!(
                        target: "maintainer",
                        "claim sweep 通知 agent={} 失败，等待下次 sweep 重试: {err:#}",
                        group.agent_id
                    );
                    report.notification_errors.push(SweepNotificationError {
                        agent_id: group.agent_id,
                        stale_claims: group.stale_claims,
                        deprecated_claims: group.deprecated_claims,
                        error: format!("{err:#}"),
                    });
                }
            }
        }
    }

    /// 创建一条 claim 属性更新建议 policy，并按目标范围下发 ClaimAttributeUpdate。
    /// statement 承载具体业务语义；name/scope 固定为 maintainer 协议用途。
    pub async fn claim_update_suggestion(
        &self,
        statement: String,
        now: DateTime<Utc>,
        target_agents: Option<Vec<AgentId>>,
    ) -> anyhow::Result<(PolicyId, usize)> {
        let _guard = self.outbox_lock.lock().await;
        let _file_guard = self.lock_outbox_file().await?;
        let action_id = self.mint_action_id_for_outbox().await?;
        let id = self.mint_policy_id().await?;
        let policy = Policy {
            id: id.clone(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: CLAIM_ATTRIBUTE_POLICY_NAME.into(),
            statement,
            scope: CLAIM_ATTRIBUTE_POLICY_SCOPE.into(),
            status: PolicyStatus::Active,
            created_at: now,
            updated_at: None,
            target_agents: normalize_target_agents(target_agents),
        };
        self.write_policy(&policy).await?;
        let pushed = self.create_outbox_entries(&policy, &action_id, now).await?;
        log::info!(
            target: "maintainer",
            "claim_update_suggestion id={} → 写文件 + 落 outbox {} 条",
            policy.id, pushed
        );
        Ok((id, pushed))
    }

    /// Agent session 启动前主动拉取自己应收的 inbox 消息。
    ///
    /// 行为：
    /// 1. lazy register：若该 agent 在 maintainer 视角尚未注册（`team/agents/<id>/claims/`
    ///    目录不存在）则 mkdir，免去单独注册接口
    /// 2. 扫 outbox 并按规则筛：
    ///    - delivered_to 已含 agent_id → 跳过（已确认持久收件）
    ///    - targeted entry：仅当 target_agent == agent_id 时命中
    ///    - broadcast entry：active 快照命中；若已经 offered，则 Policy 随后退役也继续重投
    ///    - deprecated 快照仅当该 agent 曾被 offer 或已经 ACK 同 policy 历史 entry 时命中
    /// 3. 按事件时间、outbox 创建时间、inbox_id 排序
    /// 4. 对每条命中 entry 写入/更新 offered_to；ACK 前后续 pull 会重投同一 inbox_id
    /// 5. 返回剥离 OutboxEntry 包装的 InboxMessage 列表，供 agent 落本地 inbox 文件
    pub async fn pull_inbox(&self, agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
        let _guard = self.outbox_lock.lock().await;
        let _file_guard = self.lock_outbox_file().await?;
        self.ensure_agent_registered(agent_id).await?;

        let active_policies = self.active_policy_id_set().await?;
        let entries = outbox_io::list(&self.team_root).await?;
        let seen_policy_ids: FxHashSet<PolicyId> = entries
            .iter()
            .filter(|entry| {
                entry
                    .offered_to
                    .iter()
                    .any(|mark| &mark.agent_id == agent_id)
                    || entry
                        .delivered_to
                        .iter()
                        .any(|mark| &mark.agent_id == agent_id)
            })
            .map(|entry| entry.inbox_message.policy_id().clone())
            .collect();

        let mut chosen: Vec<OutboxEntry> = Vec::new();
        for entry in entries {
            if entry.delivered_to.iter().any(|d| &d.agent_id == agent_id) {
                continue;
            }
            match &entry.target {
                OutboxTarget::Targeted { target_agent } => {
                    if target_agent != agent_id {
                        continue;
                    }
                }
                OutboxTarget::Broadcast => {
                    let snapshot_policy = entry.inbox_message.policy();
                    let was_offered = entry
                        .offered_to
                        .iter()
                        .any(|mark| &mark.agent_id == agent_id);
                    if snapshot_policy.status == PolicyStatus::Deprecated {
                        if !seen_policy_ids.contains(&snapshot_policy.id) {
                            continue;
                        }
                    } else if !active_policies.contains(&snapshot_policy.id) && !was_offered {
                        continue;
                    }
                }
            }
            chosen.push(entry);
        }

        chosen.sort_by(|left, right| {
            left.inbox_message
                .event_at()
                .cmp(&right.inbox_message.event_at())
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.inbox_id.as_str().cmp(right.inbox_id.as_str()))
        });

        let now = Utc::now();
        let mut out = Vec::with_capacity(chosen.len());
        for entry in chosen {
            // offer 不是终态：响应丢失或 Agent 本地落盘失败时，下一次 pull 会继续返回。
            outbox_io::record_offered(&self.team_root, &entry.inbox_id, agent_id, now).await?;
            out.push(entry.inbox_message);
        }

        log::info!(
            target: "maintainer",
            "pull_inbox agent={} → 提供 {} 条（等待持久收件 ACK）",
            agent_id,
            out.len()
        );
        Ok(out)
    }

    /// 确认 Agent 已把指定消息持久写入本地 Inbox。
    ///
    /// 先在锁内校验整批 ID 与 targeted 所有权，再逐条追加 delivered_to。重复 ID 和重复
    /// ACK 均幂等；未知 ID 与越权 ID 显式失败，不能伪装成成功。若磁盘故障发生在逐条
    /// 写入期间，可能留下已确认前缀；客户端重试原批次会跳过该前缀并继续收敛。
    pub async fn ack_inbox(
        &self,
        agent_id: &AgentId,
        inbox_ids: &[InboxId],
    ) -> Result<(), InboxAckError> {
        let _guard = self.outbox_lock.lock().await;
        let _file_guard = self
            .lock_outbox_file()
            .await
            .map_err(InboxAckError::LockOutbox)?;
        let entries = outbox_io::list(&self.team_root)
            .await
            .map_err(InboxAckError::ReadOutbox)?;

        let mut unique_ids = Vec::with_capacity(inbox_ids.len());
        let mut seen = FxHashSet::default();
        for inbox_id in inbox_ids {
            if seen.insert(inbox_id.clone()) {
                unique_ids.push(inbox_id.clone());
            }
        }

        for inbox_id in &unique_ids {
            let entry = entries
                .iter()
                .find(|entry| &entry.inbox_id == inbox_id)
                .ok_or_else(|| InboxAckError::UnknownInbox {
                    inbox_id: inbox_id.clone(),
                })?;
            if let OutboxTarget::Targeted { target_agent } = &entry.target {
                if target_agent != agent_id {
                    return Err(InboxAckError::TargetMismatch {
                        agent_id: agent_id.clone(),
                        inbox_id: inbox_id.clone(),
                    });
                }
            }
            let was_offered = entry
                .offered_to
                .iter()
                .any(|mark| &mark.agent_id == agent_id);
            let was_delivered = entry
                .delivered_to
                .iter()
                .any(|mark| &mark.agent_id == agent_id);
            if !was_offered && !was_delivered {
                return Err(InboxAckError::NotOffered {
                    agent_id: agent_id.clone(),
                    inbox_id: inbox_id.clone(),
                });
            }
        }

        let now = Utc::now();
        for inbox_id in unique_ids {
            outbox_io::append_delivered(&self.team_root, &inbox_id, agent_id, now)
                .await
                .map_err(|source| InboxAckError::PersistAck {
                    inbox_id: inbox_id.clone(),
                    source,
                })?;
        }
        log::info!(
            target: "maintainer",
            "ack_inbox agent={} → 确认持久收件 {} 条",
            agent_id,
            inbox_ids.len()
        );
        Ok(())
    }

    /// 在 maintainer 视角"注册"一个 agent，幂等。
    /// 注册的物化形式 = 创建该 agent 的 claims 镜像目录；后续 list_agent_ids /
    /// is_registered_agent / upload_claim 都依赖这个目录存在。
    async fn ensure_agent_registered(&self, agent_id: &AgentId) -> anyhow::Result<()> {
        let dir = paths::team_store_agent_claims_dir(&self.team_root, agent_id);
        fs::create_dir_all(&dir).await?;
        Ok(())
    }

    /// 当前 active policy id 集合，供 pull_inbox 过滤"已 deprecated 广播"使用。
    async fn active_policy_id_set(&self) -> anyhow::Result<FxHashSet<PolicyId>> {
        Ok(self
            .list_policies()
            .await?
            .into_iter()
            .filter(|p| p.status == PolicyStatus::Active)
            .map(|p| p.id)
            .collect())
    }

    pub async fn list_actions(&self) -> anyhow::Result<Vec<MaintainerActionRow>> {
        let entries = outbox_io::list(&self.team_root).await?;
        Ok(build_action_rows_from_entries(&entries))
    }

    pub async fn list_outbox_entries(
        &self,
        limit: Option<usize>,
        open: Option<bool>,
    ) -> anyhow::Result<Vec<OutboxEntry>> {
        let active_policies = self.active_policy_id_set().await?;
        let mut entries = outbox_io::list(&self.team_root).await?;
        let active_broadcast_deliveries = active_broadcast_deliveries_by_policy(&entries);
        entries.retain(|entry| {
            open.is_none_or(|expected| {
                outbox_entry_is_open(entry, &active_policies, &active_broadcast_deliveries)
                    == expected
            })
        });
        entries.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| b.inbox_id.as_str().cmp(a.inbox_id.as_str()))
        });
        if let Some(limit) = limit {
            entries.truncate(limit);
        }
        Ok(entries)
    }

    /// 派生 send log：把 outbox entries 的 delivered_to 全部 flatten 成
    /// (sent_at, agent, inbox_id, action, policy, message_type) 行，按 sent_at 升序。
    /// 同 sent_at 时按 agent_id 兜底排序保证稳定。
    pub async fn list_send_log(&self) -> anyhow::Result<Vec<SendLogRow>> {
        let entries = outbox_io::list(&self.team_root).await?;
        Ok(build_send_log_from_entries(&entries))
    }

    /// 聚合 maintainer 当前可视状态，供轻量 Web UI 和 JSON 接口复用。
    /// 这里只读现有文件，不推进任何后台流程。
    pub async fn status_snapshot(
        &self,
        now: DateTime<Utc>,
    ) -> anyhow::Result<MaintainerStatusSnapshot> {
        let mut policies = self.list_policies().await?;
        policies.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        let mut disputes = self.list_disputes().await?;
        disputes.sort_by(|a, b| {
            dispute_status_rank(b.status)
                .cmp(&dispute_status_rank(a.status))
                .then_with(|| b.created_at.cmp(&a.created_at))
        });

        let outbox_entries = outbox_io::list(&self.team_root).await?;
        let outbox_entry_count = outbox_entries.len();
        let send_event_count: usize = outbox_entries.iter().map(|e| e.delivered_to.len()).sum();
        let actions = build_action_rows_from_entries(&outbox_entries);
        let mut send_log = build_send_log_from_entries(&outbox_entries);
        // dashboard 想看最近的，倒序
        send_log.sort_by(|a, b| b.sent_at.cmp(&a.sent_at));

        let claims = self.list_all_claims().await?;
        let agents = self.build_agent_statuses(&claims).await?;
        let counts = MaintainerStatusCounts {
            agents: agents.len(),
            claims: claims.len(),
            active_claims: claims
                .iter()
                .filter(|(_, claim)| claim.status == ClaimStatus::Active)
                .count(),
            stale_claims: claims
                .iter()
                .filter(|(_, claim)| claim.status == ClaimStatus::Stale)
                .count(),
            deprecated_claims: claims
                .iter()
                .filter(|(_, claim)| claim.status == ClaimStatus::Deprecated)
                .count(),
            active_policies: policies
                .iter()
                .filter(|policy| policy.status == PolicyStatus::Active)
                .count(),
            deprecated_policies: policies
                .iter()
                .filter(|policy| policy.status == PolicyStatus::Deprecated)
                .count(),
            open_disputes: disputes
                .iter()
                .filter(|dispute| dispute.status == DisputeStatus::Open)
                .count(),
            resolved_disputes: disputes
                .iter()
                .filter(|dispute| dispute.status == DisputeStatus::Resolved)
                .count(),
            outbox_entries: outbox_entry_count,
            send_events: send_event_count,
        };
        Ok(MaintainerStatusSnapshot {
            generated_at: now,
            counts,
            agents,
            policies,
            disputes,
            actions,
            send_log,
        })
    }

    // ---- 内部辅助 ----

    /// 扫 `team_root/agents/*/claims/`，只把 claims 目录存在的 agent 视为已注册。
    async fn list_agent_ids(&self) -> anyhow::Result<Vec<AgentId>> {
        let agents_root = paths::team_store_agents_root(&self.team_root);
        match fs::try_exists(&agents_root).await {
            Ok(true) => {}
            Ok(false) => return Ok(vec![]),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e).with_context(|| format!("检查 agents 目录: {agents_root:?}")),
        }
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&agents_root).await?;
        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            if !ft.is_dir() {
                continue;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Ok(agent) = AgentId::new(name) else {
                continue;
            };
            if self.is_registered_agent(&agent).await? {
                out.push(agent);
            }
        }
        out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(out)
    }

    async fn list_policies(&self) -> anyhow::Result<Vec<Policy>> {
        let dir = paths::team_store_policies_dir(&self.team_root);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            return Ok(vec![]);
        }
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let p = entry.path();
            let Some(file_name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !file_name.ends_with(".yaml") || file_name.contains(".tmp.") {
                continue;
            }
            let metadata = entry.metadata().await?;
            let is_stale_policy_reservation = file_name
                .strip_suffix(".yaml")
                .is_some_and(|id| id.parse::<PolicyId>().is_ok())
                && metadata.is_file()
                && metadata.len() == 0;
            if is_stale_policy_reservation {
                // `mint_unique_id_in_dir` 崩溃后可能留下有效 policy 名的空占位；它仍占住
                // ID，但不是可读取 policy。不能在扫描时删除，以免撞上正在发布的 writer。
                log::warn!(target: "maintainer", "跳过遗留的 policy 占位文件: {p:?}");
                continue;
            }
            out.push(read_yaml(&p).await?);
        }
        Ok(out)
    }

    async fn list_all_claims(&self) -> anyhow::Result<Vec<(AgentId, Claim)>> {
        let agents = self.list_agent_ids().await?;
        let mut out = Vec::new();
        for agent in agents {
            let claims_dir = paths::team_store_agent_claims_dir(&self.team_root, &agent);
            match fs::try_exists(&claims_dir).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("检查 agent claims 目录: {claims_dir:?}"))
                }
            }
            let mut rd = fs::read_dir(&claims_dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let p = entry.path();
                let Some(file_name) = p.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !file_name.ends_with(".yaml") || file_name.contains(".tmp.") {
                    continue;
                }
                let claim: Claim = read_yaml(&p).await?;
                out.push((agent.clone(), claim));
            }
        }
        Ok(out)
    }

    async fn list_disputes(&self) -> anyhow::Result<Vec<Dispute>> {
        let dir = paths::team_store_disputes_dir(&self.team_root);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            return Ok(vec![]);
        }
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let p = entry.path();
            let Some(file_name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !file_name.ends_with(".yaml") || file_name.contains(".tmp.") {
                continue;
            }
            out.push(read_yaml(&p).await?);
        }
        Ok(out)
    }

    async fn build_agent_statuses(
        &self,
        claims: &[(AgentId, Claim)],
    ) -> anyhow::Result<Vec<AgentStatusSummary>> {
        let agents = self.list_agent_ids().await?;
        let mut out = Vec::with_capacity(agents.len());
        for agent in agents {
            let mut mirror_claims = 0;
            let mut active_claims = 0;
            let mut stale_claims = 0;
            let mut deprecated_claims = 0;
            for (holder, claim) in claims {
                if holder != &agent {
                    continue;
                }
                mirror_claims += 1;
                match claim.status {
                    ClaimStatus::Active => active_claims += 1,
                    ClaimStatus::Stale => stale_claims += 1,
                    ClaimStatus::Deprecated => deprecated_claims += 1,
                }
            }
            out.push(AgentStatusSummary {
                agent_id: agent,
                mirror_claims,
                active_claims,
                stale_claims,
                deprecated_claims,
            });
        }
        Ok(out)
    }

    async fn is_registered_agent(&self, agent: &AgentId) -> anyhow::Result<bool> {
        let dir = paths::team_store_agent_claims_dir(&self.team_root, agent);
        match fs::metadata(&dir).await {
            Ok(meta) => Ok(meta.is_dir()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).with_context(|| format!("检查 agent 注册目录: {dir:?}")),
        }
    }

    async fn write_policy(&self, policy: &Policy) -> anyhow::Result<()> {
        let p = paths::team_store_policies_dir(&self.team_root).join(format!("{}.yaml", policy.id));
        write_yaml_atomic(&p, policy).await?;
        Ok(())
    }

    /// 给一条新 outbox entry mint 消息 id；查重目录是 `team/maintainer/outbox/`。
    async fn mint_inbox_id_for_outbox(&self) -> anyhow::Result<InboxId> {
        let dir = paths::team_store_outbox_dir(&self.team_root);
        mint_unique_id_in_dir(&dir, InboxId::random, self.id_mint_max_attempts).await
    }

    /// 为一次 maintainer 对外动作生成未被现有 outbox 使用的 action ID。
    ///
    /// action ID 是多条 outbox entry 共用的审计分组键，不能像 inbox ID 那样借文件名占位。
    /// 调用方通过 Maintainer 入口按固定顺序持有进程内锁和 outbox 文件锁后，先扫描已有台账，
    /// 候选撞名即重抽；该单写边界使扫描与随后写入保持串行。
    async fn mint_action_id_for_outbox(&self) -> anyhow::Result<MaintainerActionId> {
        #[cfg(test)]
        if let Some(candidates) = &self.action_id_candidates {
            let candidates = std::sync::Arc::clone(candidates);
            return self
                .mint_action_id_for_outbox_with_factory(move || {
                    candidates
                        .lock()
                        .map_err(|_| anyhow::anyhow!("测试 action ID 候选锁已 poisoned"))?
                        .pop_front()
                        .ok_or_else(|| anyhow::anyhow!("测试 action ID 候选已耗尽"))
                })
                .await;
        }
        self.mint_action_id_for_outbox_with_factory(|| Ok(MaintainerActionId::random()))
            .await
    }

    async fn mint_action_id_for_outbox_with_factory<F>(
        &self,
        mut factory: F,
    ) -> anyhow::Result<MaintainerActionId>
    where
        F: FnMut() -> anyhow::Result<MaintainerActionId>,
    {
        if self.id_mint_max_attempts == 0 {
            anyhow::bail!("mint maintainer action id: max_attempts 必须 >= 1");
        }

        let used_ids: FxHashSet<MaintainerActionId> = outbox_io::list(&self.team_root)
            .await?
            .into_iter()
            .map(|entry| entry.maintainer_action_id)
            .collect();
        let mut last_collision = None;
        for _ in 0..self.id_mint_max_attempts {
            let candidate = factory()?;
            if !used_ids.contains(&candidate) {
                return Ok(candidate);
            }
            last_collision = Some(candidate.to_string());
        }

        anyhow::bail!(
            "mint maintainer action id: 尝试 {} 次仍与现有 outbox 动作撞名（最近候选 id={}）",
            self.id_mint_max_attempts,
            last_collision.as_deref().unwrap_or("?")
        );
    }

    /// 把 policy 的一次发布动作展开成 outbox entry：
    /// - target_agents == None 或空 → 1 条 broadcast entry
    /// - target_agents == Some(non_empty) → N 条 targeted entry，按 agent 各一条
    ///
    /// 同一次发布共享一个 maintainer_action_id，便于审计回溯。
    /// 调用方必须通过 Maintainer 入口持有进程内锁和 outbox 文件锁，与同时进行的 pull 串行。
    /// 返回创建的 entry 数量。
    async fn create_outbox_entries(
        &self,
        policy: &Policy,
        action_id: &MaintainerActionId,
        now: DateTime<Utc>,
    ) -> anyhow::Result<usize> {
        let kind = policy_inbox_kind_for_policy(policy);
        let targets: Vec<OutboxTarget> = match &policy.target_agents {
            Some(list) if !list.is_empty() => list
                .iter()
                .cloned()
                .map(|agent| OutboxTarget::Targeted {
                    target_agent: agent,
                })
                .collect(),
            _ => vec![OutboxTarget::Broadcast],
        };

        let mut count = 0;
        for target in targets {
            let inbox_id = self.mint_inbox_id_for_outbox().await?;
            let inbox_message = InboxMessage {
                id: inbox_id.clone(),
                kind: kind.clone(),
                handled_at: None,
            };
            let entry = OutboxEntry {
                inbox_id,
                maintainer_action_id: action_id.clone(),
                target,
                created_at: now,
                offered_to: vec![],
                delivered_to: vec![],
                inbox_message,
            };
            outbox_io::write(&self.team_root, &entry).await?;
            count += 1;
        }
        Ok(count)
    }
}

pub(crate) async fn validate_new_dispute_direct_claims(
    team_root: &Path,
    dispute: &Dispute,
) -> anyhow::Result<()> {
    let mirrors = arbitration::load_team_claims(team_root).await?;
    let deprecated: Vec<String> = mirrors
        .iter()
        .filter(|(_, claim)| {
            claim.status == ClaimStatus::Deprecated && dispute.claims.contains(&claim.id)
        })
        .map(|(_, claim)| claim.id.to_string())
        .collect();
    if deprecated.is_empty() {
        return Ok(());
    }
    Err(arbitration::AnalysisConflict(format!(
        "dispute id={} 包含 deprecated direct claim: {}",
        dispute.id,
        deprecated.join(", ")
    ))
    .into())
}

fn build_action_rows_from_entries(entries: &[OutboxEntry]) -> Vec<MaintainerActionRow> {
    let mut groups: BTreeMap<MaintainerActionId, Vec<&OutboxEntry>> = BTreeMap::default();
    for entry in entries {
        groups
            .entry(entry.maintainer_action_id.clone())
            .or_default()
            .push(entry);
    }

    let mut rows = Vec::with_capacity(groups.len());
    for (action_id, mut entries) in groups {
        entries.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.inbox_id.as_str().cmp(b.inbox_id.as_str()))
        });
        let Some(first) = entries.first() else {
            continue;
        };
        let first_policy = first.inbox_message.policy();
        let mut inbox_ids: Vec<InboxId> =
            entries.iter().map(|entry| entry.inbox_id.clone()).collect();
        inbox_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        inbox_ids.dedup();

        let mut target_agents: Vec<AgentId> = entries
            .iter()
            .filter_map(|entry| match &entry.target {
                OutboxTarget::Targeted { target_agent } => Some(target_agent.clone()),
                OutboxTarget::Broadcast => None,
            })
            .collect();
        target_agents.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        target_agents.dedup();

        let mut delivered_agents: Vec<AgentId> = entries
            .iter()
            .flat_map(|entry| entry.delivered_to.iter().map(|mark| mark.agent_id.clone()))
            .collect();
        delivered_agents.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        delivered_agents.dedup();

        let has_broadcast = entries
            .iter()
            .any(|entry| matches!(entry.target, OutboxTarget::Broadcast));
        let has_targeted = entries
            .iter()
            .any(|entry| matches!(entry.target, OutboxTarget::Targeted { .. }));
        let target_kind = match (has_broadcast, has_targeted) {
            (true, false) => "broadcast",
            (false, true) => "targeted",
            (true, true) => "mixed",
            (false, false) => "unknown",
        }
        .to_string();

        rows.push(MaintainerActionRow {
            created_at: first.created_at,
            maintainer_action_id: action_id,
            message_type: first.inbox_message.message_type(),
            policy_id: first_policy.id.clone(),
            policy_name: first_policy.name.clone(),
            policy_scope: first_policy.scope.clone(),
            policy_status: first_policy.status,
            target_kind,
            inbox_ids,
            target_agents,
            delivered_agents,
            outbox_entries: entries.len(),
            send_events: entries.iter().map(|entry| entry.delivered_to.len()).sum(),
        });
    }

    rows.sort_by(|a, b| {
        b.created_at.cmp(&a.created_at).then_with(|| {
            a.maintainer_action_id
                .as_str()
                .cmp(b.maintainer_action_id.as_str())
        })
    });
    rows
}

fn active_broadcast_deliveries_by_policy(
    entries: &[OutboxEntry],
) -> BTreeMap<PolicyId, FxHashSet<AgentId>> {
    let mut deliveries: BTreeMap<PolicyId, FxHashSet<AgentId>> = BTreeMap::new();
    for entry in entries {
        if !matches!(entry.target, OutboxTarget::Broadcast)
            || entry.inbox_message.policy().status != PolicyStatus::Active
        {
            continue;
        }
        deliveries
            .entry(entry.inbox_message.policy_id().clone())
            .or_default()
            .extend(
                entry
                    .offered_to
                    .iter()
                    .map(|mark| mark.agent_id.clone())
                    .chain(entry.delivered_to.iter().map(|mark| mark.agent_id.clone())),
            );
    }
    deliveries
}

fn outbox_entry_is_open(
    entry: &OutboxEntry,
    active_policies: &FxHashSet<PolicyId>,
    active_broadcast_deliveries: &BTreeMap<PolicyId, FxHashSet<AgentId>>,
) -> bool {
    match &entry.target {
        OutboxTarget::Targeted { target_agent } => !entry
            .delivered_to
            .iter()
            .any(|mark| &mark.agent_id == target_agent),
        OutboxTarget::Broadcast => {
            let policy = entry.inbox_message.policy();
            if policy.status == PolicyStatus::Active {
                return active_policies.contains(&policy.id);
            }
            let Some(eligible_agents) = active_broadcast_deliveries.get(&policy.id) else {
                return false;
            };
            eligible_agents.iter().any(|agent| {
                !entry
                    .delivered_to
                    .iter()
                    .any(|mark| &mark.agent_id == agent)
            })
        }
    }
}

fn build_send_log_from_entries(entries: &[OutboxEntry]) -> Vec<SendLogRow> {
    let mut rows = Vec::new();
    for entry in entries {
        let policy_id = entry.inbox_message.policy_id().clone();
        let message_type = entry.inbox_message.message_type();
        for d in &entry.delivered_to {
            rows.push(SendLogRow {
                sent_at: d.sent_at,
                agent_id: d.agent_id.clone(),
                inbox_id: entry.inbox_id.clone(),
                maintainer_action_id: entry.maintainer_action_id.clone(),
                policy_id: policy_id.clone(),
                message_type,
            });
        }
    }
    rows.sort_by(|a, b| {
        a.sent_at
            .cmp(&b.sent_at)
            .then_with(|| a.agent_id.as_str().cmp(b.agent_id.as_str()))
    });
    rows
}

fn normalize_target_agents(target_agents: Option<Vec<AgentId>>) -> Option<Vec<AgentId>> {
    let mut unique = Vec::new();
    for agent in target_agents.unwrap_or_default() {
        if !unique.contains(&agent) {
            unique.push(agent);
        }
    }
    if unique.is_empty() {
        None
    } else {
        Some(unique)
    }
}

pub(crate) fn same_report_payload(existing: &Dispute, incoming: &Dispute) -> bool {
    existing.id == incoming.id
        && existing.name == incoming.name
        && existing.reporter_agent_id == incoming.reporter_agent_id
        && existing.claims == incoming.claims
        && existing.summary == incoming.summary
        && existing.created_at == incoming.created_at
}

fn arbitration_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == ErrorKind::NotFound)
            || cause
                .downcast_ref::<crate::storage::StorageError>()
                .is_some_and(|storage| {
                    matches!(storage, crate::storage::StorageError::Io { source, .. } if source.kind() == ErrorKind::NotFound)
                })
    })
}

/// 直接根据 policy.message_type 派生 InboxMessageKind，新 outbox 路径用这个。
fn policy_inbox_kind_for_policy(policy: &Policy) -> InboxMessageKind {
    match policy.message_type {
        PolicyMessageType::PolicyUpdate => InboxMessageKind::PolicyUpdate {
            policy: policy.clone(),
        },
        PolicyMessageType::ClaimAttributeUpdate => InboxMessageKind::ClaimAttributeUpdate {
            policy: policy.clone(),
            arbitration_resolution: None,
        },
    }
}

fn group_sweep_claims_by_agent(report: &ClaimSweepReport) -> Vec<SweepAgentClaims> {
    let mut groups: Vec<SweepAgentClaims> = Vec::new();
    for (agent, claim_id) in &report.stale_claims {
        push_sweep_group_claim(&mut groups, agent, claim_id, false);
    }
    for (agent, claim_id) in &report.deprecated_claims {
        push_sweep_group_claim(&mut groups, agent, claim_id, true);
    }
    groups.sort_by(|left, right| left.agent_id.as_str().cmp(right.agent_id.as_str()));
    groups
}

fn push_sweep_group_claim(
    groups: &mut Vec<SweepAgentClaims>,
    agent: &AgentId,
    claim_id: &ClaimId,
    deprecated: bool,
) {
    let idx = groups
        .iter()
        .position(|candidate| &candidate.agent_id == agent)
        .unwrap_or_else(|| {
            groups.push(SweepAgentClaims {
                agent_id: agent.clone(),
                stale_claims: Vec::new(),
                deprecated_claims: Vec::new(),
            });
            groups.len() - 1
        });
    let group = &mut groups[idx];
    if deprecated {
        group.deprecated_claims.push(claim_id.clone());
    } else {
        group.stale_claims.push(claim_id.clone());
    }
}

fn claim_sweep_notification_statement(
    agent_id: &AgentId,
    stale_claims: &[ClaimId],
    deprecated_claims: &[ClaimId],
) -> String {
    format!(
        "来自 ACN 团队 Maintainer 的通知：\nagent：{agent_id}\n\n根据 maintainer 的定期 claim sweep 机制，您有如下 local claims 建议调整 status 字段。\n\n建议调整 status 为 stale 的 claim：{}\n建议调整 status 为 deprecated 的 claim：{}\n\n注：本次调整为团队建议，具体处理办法请结合本 agent 的自身情况决定。",
        format_claim_id_list(stale_claims),
        format_claim_id_list(deprecated_claims)
    )
}

fn format_claim_id_list(ids: &[ClaimId]) -> String {
    if ids.is_empty() {
        return "[]".into();
    }
    let joined = ids
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{joined}]")
}

fn dispute_status_rank(status: DisputeStatus) -> usize {
    match status {
        DisputeStatus::Open => 2,
        DisputeStatus::Resolved => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{Confidence, PolicyStatus};
    use crate::time::now_seconds;

    async fn list_outbox(m: &Maintainer) -> Vec<OutboxEntry> {
        outbox_io::list(m.team_root()).await.unwrap()
    }

    fn sample_policy_fields() -> (String, String, String) {
        (
            "batch_order_chunking_limit_50".into(),
            "批量订单提交时必须按每批不超过50条分片".into(),
            "order-system / batch-order-submit".into(),
        )
    }

    async fn publish_sample_policy(
        m: &Maintainer,
        now: DateTime<Utc>,
    ) -> (crate::claim::PolicyId, usize) {
        let (name, statement, scope) = sample_policy_fields();
        m.publish_new_policy(name, statement, scope, now, None)
            .await
            .unwrap()
    }

    async fn pull_and_ack(m: &Maintainer, agent: &AgentId) -> Vec<InboxMessage> {
        let messages = m.pull_inbox(agent).await.unwrap();
        let inbox_ids = messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        m.ack_inbox(agent, &inbox_ids).await.unwrap();
        messages
    }

    fn sample_claim_at(holder: &AgentId, created: DateTime<Utc>, status: ClaimStatus) -> Claim {
        Claim {
            id: ClaimId::random(),
            name: "n".into(),
            statement: "s".into(),
            scope: "scope".into(),
            holder: holder.clone(),
            confidence: Confidence::Medium,
            status,
            created_at: created,
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "e".into(),
        }
    }

    fn build(stale_days: i64, deprecate_days: i64) -> (Maintainer, tempfile::TempDir) {
        let team = tempfile::tempdir().unwrap();
        let m = Maintainer::new(
            team.path().to_path_buf(),
            Duration::days(stale_days),
            Duration::days(deprecate_days),
            crate::config::default_id_mint_max_attempts(),
        );
        (m, team)
    }

    /// 工具：把 claim 直接写入 team store 镜像（模拟 agent 已经上送过）
    async fn seed_claim(team_root: &Path, c: &Claim) {
        let dir = paths::team_store_agent_claims_dir(team_root, &c.holder);
        let p = dir.join(format!("{}.yaml", c.id));
        write_yaml_atomic(&p, c).await.unwrap();
    }

    async fn count_outbox_for_policy(m: &Maintainer, policy_id: &PolicyId) -> usize {
        list_outbox(m)
            .await
            .into_iter()
            .filter(|e| e.inbox_message.policy_id() == policy_id)
            .count()
    }

    /// 发布 policy → 写 policy 文件 + 落 outbox（不再直写 agent inbox）
    #[tokio::test]
    async fn publish_policy_writes_file_and_outbox_entry() {
        let (m, _team) = build(7, 30);

        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let (policy_id, pushed) = publish_sample_policy(&m, now).await;
        assert_eq!(pushed, 1, "broadcast publish 应只创建 1 条 outbox entry");

        // policy 文件落地
        let pf = paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        assert!(pf.exists());

        // outbox 中有 1 条 broadcast entry，delivered_to 为空（未 pull）
        let entries = list_outbox(&m).await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0].target, OutboxTarget::Broadcast));
        assert!(entries[0].delivered_to.is_empty());
        assert_eq!(entries[0].inbox_message.policy_id(), &policy_id);
    }

    #[tokio::test]
    async fn action_id_mint_retries_when_existing_outbox_uses_candidate() {
        let (m, _team) = build(7, 30);
        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let collision = list_outbox(&m).await[0].maintainer_action_id.clone();
        let unique: MaintainerActionId = if collision.as_str() == "intent_aaaaaaaa" {
            "intent_bbbbbbbb".parse().unwrap()
        } else {
            "intent_aaaaaaaa".parse().unwrap()
        };
        let calls = std::cell::Cell::new(0);

        let _guard = m.outbox_lock.lock().await;
        let _file_guard = m.lock_outbox_file().await.unwrap();
        let minted = m
            .mint_action_id_for_outbox_with_factory(|| {
                let call = calls.get();
                calls.set(call + 1);
                if call == 0 {
                    Ok(collision.clone())
                } else {
                    Ok(unique.clone())
                }
            })
            .await
            .unwrap();

        assert_eq!(minted, unique);
        assert_eq!(calls.get(), 2);
    }

    #[tokio::test]
    async fn action_id_mint_errors_after_collision_budget_is_exhausted() {
        let (m, _team) = build(7, 30);
        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let collision = list_outbox(&m).await[0].maintainer_action_id.clone();
        let calls = std::cell::Cell::new(0);

        let _guard = m.outbox_lock.lock().await;
        let _file_guard = m.lock_outbox_file().await.unwrap();
        let error = m
            .mint_action_id_for_outbox_with_factory(|| {
                calls.set(calls.get() + 1);
                Ok(collision.clone())
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("仍与现有 outbox 动作撞名"));
        assert_eq!(calls.get(), m.id_mint_max_attempts);
    }

    #[tokio::test]
    async fn action_id_mint_propagates_outbox_scan_error() {
        let team = tempfile::tempdir().unwrap();
        let root_file = team.path().join("not-a-directory");
        tokio::fs::write(&root_file, b"not a directory")
            .await
            .unwrap();
        let m = Maintainer::new(
            root_file,
            Duration::days(7),
            Duration::days(30),
            crate::config::default_id_mint_max_attempts(),
        );

        let error = m.mint_action_id_for_outbox().await.unwrap_err();

        assert!(error.to_string().contains("检查 outbox 目录失败"));
    }

    #[tokio::test]
    async fn publish_recovers_from_stale_inbox_reservation_before_action_id_scan() {
        let (m, _team) = build(7, 30);
        let reserved = InboxId::random();
        let outbox_dir = paths::team_store_outbox_dir(m.team_root());
        tokio::fs::create_dir_all(&outbox_dir).await.unwrap();
        let reservation = tokio::fs::File::create(outbox_dir.join(format!("{reserved}.yaml")))
            .await
            .unwrap();
        drop(reservation);

        let (_, pushed) = publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;

        assert_eq!(pushed, 1);
        assert_eq!(list_outbox(&m).await.len(), 1);
    }

    #[tokio::test]
    async fn policy_scan_skips_stale_zero_byte_reservation_without_deleting_it() {
        let (m, _team) = build(7, 30);
        let (published_id, _) =
            publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let reserved_id = PolicyId::random();
        let policies_dir = paths::team_store_policies_dir(m.team_root());
        let reservation_path = policies_dir.join(format!("{reserved_id}.yaml"));
        let reservation = tokio::fs::File::create(&reservation_path).await.unwrap();
        drop(reservation);

        let policies = m.list_policies().await.unwrap();

        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, published_id);
        assert!(
            reservation_path.exists(),
            "扫描不能删除可能仍由发布流程持有的占位"
        );
    }

    #[tokio::test]
    async fn deprecate_keeps_policy_active_when_action_id_mint_fails() {
        let (m, team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let (policy_id, _) = publish_sample_policy(&m, now).await;
        let failing = Maintainer::new(
            team.path().to_path_buf(),
            Duration::days(7),
            Duration::days(30),
            0,
        );

        let error = failing
            .deprecate_policy(&policy_id, "2026-05-15T10:00:00Z".parse().unwrap())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("max_attempts 必须 >= 1"));
        let policy_path =
            paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        let policy: Policy = read_yaml(&policy_path).await.unwrap();
        assert_eq!(policy.status, PolicyStatus::Active);
        assert_eq!(list_outbox(&m).await.len(), 1);
    }

    /// 子进程辅助测试：走不涉及仲裁语义输入锁的真实 outbox 发布路径，
    /// 并在拿到 outbox 锁后等待父测试放行。
    #[tokio::test]
    async fn outbox_file_lock_subprocess_holder() -> anyhow::Result<()> {
        let Some(team_root) = std::env::var_os("ACN_TEST_OUTBOX_LOCK_TEAM_ROOT") else {
            return Ok(());
        };
        let action_id: MaintainerActionId = std::env::var("ACN_TEST_OUTBOX_ACTION_ID")
            .context("子进程缺少固定 action ID")?
            .parse()
            .context("子进程固定 action ID 不合法")?;
        let holder = Maintainer::new(
            std::path::PathBuf::from(team_root),
            Duration::days(7),
            Duration::days(30),
            crate::config::default_id_mint_max_attempts(),
        )
        .with_action_id_candidates([action_id]);
        holder
            .claim_update_suggestion(
                "child must hold outbox lock from the real maintainer publish path".into(),
                "2026-05-14T10:00:00Z".parse().unwrap(),
                None,
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn outbox_file_lock_blocks_a_separate_maintainer_process() -> anyhow::Result<()> {
        let (m, team) = build(7, 30);
        let lock_path = paths::team_store_outbox_lock_path(m.team_root());
        let ready_path = team.path().join("outbox-lock-ready");
        let release_path = team.path().join("outbox-lock-release");
        let child_action_id: MaintainerActionId = "intent_aaaaaaaa".parse().unwrap();
        let parent_action_id: MaintainerActionId = "intent_bbbbbbbb".parse().unwrap();
        let test_executable =
            std::env::current_exe().context("定位 maintainer 测试可执行文件失败")?;
        let mut holder = tokio::process::Command::new(test_executable)
            .arg("--exact")
            .arg("maintainer::tests::outbox_file_lock_subprocess_holder")
            .arg("--nocapture")
            .env("ACN_TEST_OUTBOX_LOCK_TEAM_ROOT", team.path())
            .env("ACN_TEST_OUTBOX_ACTION_ID", child_action_id.as_str())
            .env("ACN_TEST_OUTBOX_LOCK_READY", &ready_path)
            .env("ACN_TEST_OUTBOX_LOCK_RELEASE", &release_path)
            .kill_on_drop(true)
            .spawn()
            .context("启动 outbox 锁子进程失败")?;

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !tokio::fs::try_exists(&ready_path).await? {
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("等待 outbox 锁子进程就绪超时");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let parent_cannot_acquire = FileLockGuard::try_lock_exclusive(&lock_path)
            .await?
            .is_none();
        let lock_attempted = std::sync::Arc::new(tokio::sync::Notify::new());
        let wait_for_lock_attempt = lock_attempted.notified();
        let other = Maintainer::new(
            team.path().to_path_buf(),
            Duration::days(7),
            Duration::days(30),
            crate::config::default_id_mint_max_attempts(),
        )
        .with_outbox_lock_attempt_notifier(std::sync::Arc::clone(&lock_attempted))
        .with_action_id_candidates([child_action_id.clone(), parent_action_id.clone()]);
        let mut publish = tokio::spawn(async move {
            other
                .claim_update_suggestion(
                    "outbox lock must serialize action ID minting".into(),
                    "2026-05-14T10:00:00Z".parse().unwrap(),
                    None,
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), wait_for_lock_attempt)
            .await
            .context("等待第二个 Maintainer 进入 outbox 锁获取点超时")?;
        tokio::task::yield_now().await;
        let publish_is_blocked = !publish.is_finished();

        tokio::fs::write(&release_path, b"release").await?;
        let holder_status = tokio::time::timeout(std::time::Duration::from_secs(5), holder.wait())
            .await
            .context("等待 outbox 锁子进程退出超时")??;
        let (_, pushed) = tokio::time::timeout(std::time::Duration::from_secs(5), &mut publish)
            .await
            .context("等待被锁住的 Maintainer 发布完成超时")???;

        assert!(
            parent_cannot_acquire,
            "独立子进程持锁时父进程不应取得 outbox 锁"
        );
        assert!(
            publish_is_blocked,
            "第二个 Maintainer 必须在 outbox 文件锁上等待"
        );
        assert!(
            holder_status.success(),
            "outbox 锁子进程应正常退出: {holder_status}"
        );
        assert_eq!(pushed, 1);
        let mut action_ids = list_outbox(&m)
            .await
            .into_iter()
            .map(|entry| entry.maintainer_action_id)
            .collect::<Vec<_>>();
        action_ids.sort();
        assert_eq!(action_ids, vec![child_action_id, parent_action_id]);
        Ok(())
    }

    #[tokio::test]
    async fn publish_broadcast_writes_outbox_entry_even_without_registered_agents() {
        let (m, _team) = build(7, 30);
        let (name, statement, scope) = sample_policy_fields();
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();

        let (policy_id, pushed) = m
            .publish_new_policy(name, statement, scope, now, None)
            .await
            .unwrap();

        assert_eq!(pushed, 1, "broadcast 永远落 1 条 outbox entry");
        let pf = paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        assert!(pf.exists());
        assert_eq!(count_outbox_for_policy(&m, &policy_id).await, 1);
    }

    #[tokio::test]
    async fn publish_policy_with_targets_creates_one_entry_per_agent() {
        let (m, _team) = build(7, 30);
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_c = AgentId::new("agent-c").unwrap();

        let (name, statement, scope) = sample_policy_fields();
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let (policy_id, pushed) = m
            .publish_new_policy(
                name,
                statement,
                scope,
                now,
                Some(vec![agent_a.clone(), agent_c.clone(), agent_a.clone()]),
            )
            .await
            .unwrap();
        assert_eq!(pushed, 2, "重复 target_agents 应去重后投递");

        let entries: Vec<_> = list_outbox(&m)
            .await
            .into_iter()
            .filter(|e| e.inbox_message.policy_id() == &policy_id)
            .collect();
        assert_eq!(entries.len(), 2);
        let target_agents: std::collections::HashSet<_> = entries
            .iter()
            .filter_map(|e| match &e.target {
                OutboxTarget::Targeted { target_agent } => Some(target_agent.as_str()),
                OutboxTarget::Broadcast => None,
            })
            .collect();
        assert!(target_agents.contains("agent-a"));
        assert!(target_agents.contains("agent-c"));
        assert!(!target_agents.contains("agent-b"));

        let pf = paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        let saved: Policy = read_yaml(&pf).await.unwrap();
        assert_eq!(saved.target_agents, Some(vec![agent_a, agent_c]));
    }

    #[tokio::test]
    async fn empty_target_agents_is_broadcast() {
        let (m, _team) = build(7, 30);

        let (name, statement, scope) = sample_policy_fields();
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let (policy_id, pushed) = m
            .publish_new_policy(name, statement, scope, now, Some(vec![]))
            .await
            .unwrap();
        assert_eq!(pushed, 1);

        let pf = paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        let saved: Policy = read_yaml(&pf).await.unwrap();
        assert_eq!(saved.target_agents, None);
        let entries = list_outbox(&m).await;
        assert!(matches!(entries[0].target, OutboxTarget::Broadcast));
    }

    /// 底层状态写入会保留原文件。
    #[tokio::test]
    async fn resolve_dispute_updates_status_and_keeps_file() {
        let (m, _team) = build(7, 30);
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "n".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "原始 summary".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let p = paths::team_store_disputes_dir(m.team_root()).join(format!("{}.yaml", dispute.id));
        write_yaml_atomic(&p, &dispute).await.unwrap();
        let now: DateTime<Utc> = "2026-04-22T10:00:00Z".parse().unwrap();
        m.mark_dispute_resolved_for_test(&dispute.id, now)
            .await
            .unwrap();

        let after: Dispute = read_yaml(&p).await.unwrap();
        assert_eq!(after.status, DisputeStatus::Resolved);
        assert_eq!(after.resolved_at, Some(now));
        assert_eq!(after.summary, "原始 summary");
        assert!(p.exists(), "不应物理删除 dispute 文件");
    }

    #[tokio::test]
    async fn report_dispute_rejects_maintainer_owned_resolution_fields() {
        let (m, _team) = build(7, 30);
        let mut dispute = Dispute {
            id: DisputeId::random(),
            name: "invalid_agent_report".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random()],
            summary: "invalid".into(),
            status: DisputeStatus::Resolved,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };

        let path =
            paths::team_store_disputes_dir(m.team_root()).join(format!("{}.yaml", dispute.id));
        assert!(m.report_dispute(&dispute).await.is_err());
        assert!(!path.exists());

        dispute.status = DisputeStatus::Open;
        dispute.resolved_at = Some("2026-04-22T00:00:00Z".parse().unwrap());
        assert!(m.report_dispute(&dispute).await.is_err());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn new_dispute_rejects_deprecated_direct_claim_but_exact_replay_stays_idempotent() {
        let (m, _team) = build(7, 30);
        let holder = AgentId::new("agent-a").unwrap();
        let created_at: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let mut first = sample_claim_at(&holder, created_at, ClaimStatus::Active);
        let second = sample_claim_at(&holder, created_at, ClaimStatus::Active);
        seed_claim(m.team_root(), &first).await;
        seed_claim(m.team_root(), &second).await;
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "valid_before_claim_deprecation".into(),
            reporter_agent_id: holder,
            claims: vec![first.id.clone(), second.id.clone()],
            summary: "same-scope conflict".into(),
            status: DisputeStatus::Open,
            created_at,
            resolved_at: None,
        };
        m.report_dispute(&dispute).await.unwrap();

        first.status = ClaimStatus::Deprecated;
        first.updated_at = Some("2026-04-22T00:00:00Z".parse().unwrap());
        m.upload_claim(&first).await.unwrap();

        // 已持久化上报的网络重放仍按原 payload 幂等，不因之后的 Claim 生命周期变化失败。
        m.report_dispute(&dispute).await.unwrap();

        let mut new_dispute = dispute.clone();
        new_dispute.id = DisputeId::random();
        let error = m.report_dispute(&new_dispute).await.unwrap_err();
        assert!(error.to_string().contains("deprecated direct claim"));
        let path =
            paths::team_store_disputes_dir(m.team_root()).join(format!("{}.yaml", new_dispute.id));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn late_open_replay_does_not_overwrite_resolved_dispute() {
        let (m, _team) = build(7, 30);
        let original = Dispute {
            id: DisputeId::random(),
            name: "replayed".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "original payload".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        m.report_dispute(&original).await.unwrap();
        let resolved_at: DateTime<Utc> = "2026-04-22T00:00:00Z".parse().unwrap();
        m.mark_dispute_resolved_for_test(&original.id, resolved_at)
            .await
            .unwrap();

        m.report_dispute(&original).await.unwrap();

        let path =
            paths::team_store_disputes_dir(m.team_root()).join(format!("{}.yaml", original.id));
        let stored: Dispute = read_yaml(&path).await.unwrap();
        assert_eq!(stored.status, DisputeStatus::Resolved);
        assert_eq!(stored.resolved_at, Some(resolved_at));

        let mut conflicting = original;
        conflicting.summary = "different payload".into();
        assert!(m
            .report_dispute(&conflicting)
            .await
            .unwrap_err()
            .to_string()
            .contains("原始字段不同"));
    }

    /// claim sweep 检测旧 active claim，并通过 ClaimAttributeUpdate 提醒对应 agent
    #[tokio::test]
    async fn stale_sweep_detects_old_active_as_stale() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-d").unwrap();
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let old = sample_claim_at(&agent, now - Duration::days(10), ClaimStatus::Active);
        let fresh = sample_claim_at(&agent, now - Duration::days(1), ClaimStatus::Active);
        seed_claim(m.team_root(), &old).await;
        seed_claim(m.team_root(), &fresh).await;

        let report = m.run_stale_sweep(now).await.unwrap();
        assert_eq!(report.stale_claims.len(), 1);
        assert_eq!(report.stale_claims[0].1, old.id);
        assert!(report.deprecated_claims.is_empty());
        assert_eq!(report.notifications.len(), 1);
        assert!(report.notification_errors.is_empty());
        let notification = &report.notifications[0];
        assert_eq!(notification.agent_id, agent);
        assert_eq!(notification.stale_claims, vec![old.id.clone()]);
        assert!(notification.deprecated_claims.is_empty());
        assert_eq!(notification.pushed, 1);

        let p = paths::team_store_agent_claims_dir(m.team_root(), &agent)
            .join(format!("{}.yaml", old.id));
        let after: Claim = read_yaml(&p).await.unwrap();
        assert_eq!(after.status, ClaimStatus::Active);
        let policy_path = paths::team_store_policies_dir(m.team_root())
            .join(format!("{}.yaml", notification.policy_id));
        let policy: Policy = read_yaml(&policy_path).await.unwrap();
        assert_eq!(policy.message_type, PolicyMessageType::ClaimAttributeUpdate);
        assert!(policy.statement.contains(old.id.as_str()));
        assert!(policy
            .statement
            .contains("建议调整 status 为 deprecated 的 claim：[]"));
        assert_eq!(policy.target_agents, Some(vec![agent.clone()]));
        let entries = list_outbox(&m).await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            &entries[0].target,
            OutboxTarget::Targeted { target_agent } if target_agent == &agent
        ));
    }

    #[tokio::test]
    async fn stale_sweep_uses_updated_at_before_created_at() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-d").unwrap();
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let mut refreshed_active =
            sample_claim_at(&agent, now - Duration::days(60), ClaimStatus::Active);
        refreshed_active.updated_at = Some(now - Duration::days(1));
        let mut refreshed_stale =
            sample_claim_at(&agent, now - Duration::days(120), ClaimStatus::Stale);
        refreshed_stale.updated_at = Some(now - Duration::days(1));
        seed_claim(m.team_root(), &refreshed_active).await;
        seed_claim(m.team_root(), &refreshed_stale).await;

        let report = m.run_stale_sweep(now).await.unwrap();

        assert!(report.stale_claims.is_empty());
        assert!(report.deprecated_claims.is_empty());
        assert!(report.notifications.is_empty());
    }

    #[tokio::test]
    async fn claim_update_suggestion_writes_policy_and_outbox_entry() {
        let (m, _team) = build(7, 30);
        let agent_b = AgentId::new("agent-b").unwrap();

        let now: DateTime<Utc> = "2026-04-23T00:00:00Z".parse().unwrap();
        let statement = "建议把支付超时阈值更新为 45s".to_string();
        let (policy_id, pushed) = m
            .claim_update_suggestion(statement.clone(), now, Some(vec![agent_b.clone()]))
            .await
            .unwrap();
        assert_eq!(pushed, 1);

        let pf = paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        let policy: Policy = read_yaml(&pf).await.unwrap();
        assert_eq!(policy.statement, statement);
        assert_eq!(policy.message_type, PolicyMessageType::ClaimAttributeUpdate);
        assert_eq!(policy.status, PolicyStatus::Active);

        let entries = list_outbox(&m).await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            &entries[0].target,
            OutboxTarget::Targeted { target_agent } if target_agent == &agent_b
        ));
        assert!(matches!(
            &entries[0].inbox_message.kind,
            InboxMessageKind::ClaimAttributeUpdate { policy, .. } if policy.id == policy_id
        ));
    }

    /// stale sweep 只检测旧 stale claim
    #[tokio::test]
    async fn stale_sweep_detects_old_stale_as_deprecated() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-d").unwrap();
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let very_old = sample_claim_at(&agent, now - Duration::days(60), ClaimStatus::Stale);
        seed_claim(m.team_root(), &very_old).await;

        let report = m.run_stale_sweep(now).await.unwrap();
        assert_eq!(report.deprecated_claims.len(), 1);
        assert_eq!(report.deprecated_claims[0].1, very_old.id);
        assert!(report.stale_claims.is_empty());
        assert_eq!(report.notifications.len(), 1);
        assert!(report.notification_errors.is_empty());
        let notification = &report.notifications[0];
        assert_eq!(notification.agent_id, agent);
        assert!(notification.stale_claims.is_empty());
        assert_eq!(notification.deprecated_claims, vec![very_old.id.clone()]);

        let p = paths::team_store_agent_claims_dir(m.team_root(), &agent)
            .join(format!("{}.yaml", very_old.id));
        let after: Claim = read_yaml(&p).await.unwrap();
        assert_eq!(after.status, ClaimStatus::Stale);
        let policy_path = paths::team_store_policies_dir(m.team_root())
            .join(format!("{}.yaml", notification.policy_id));
        let policy: Policy = read_yaml(&policy_path).await.unwrap();
        assert_eq!(policy.message_type, PolicyMessageType::ClaimAttributeUpdate);
        assert!(policy
            .statement
            .contains("建议调整 status 为 stale 的 claim：[]"));
        assert!(policy.statement.contains(very_old.id.as_str()));
    }

    #[tokio::test]
    async fn deprecate_policy_updates_file_and_creates_outbox_entry() {
        let (m, _team) = build(7, 30);

        let created_at: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let (policy_id, _) = publish_sample_policy(&m, created_at).await;
        let now: DateTime<Utc> = "2026-04-22T10:00:00Z".parse().unwrap();
        let pushed = m.deprecate_policy(&policy_id, now).await.unwrap();
        assert_eq!(pushed, 1, "broadcast deprecate 应再落 1 条 outbox entry");

        let pf = paths::team_store_policies_dir(m.team_root()).join(format!("{policy_id}.yaml"));
        let after: Policy = read_yaml(&pf).await.unwrap();
        assert_eq!(after.status, PolicyStatus::Deprecated);
        assert_eq!(after.message_type, PolicyMessageType::PolicyUpdate);
        assert_eq!(after.created_at, created_at);
        assert_eq!(after.updated_at, Some(now));

        // outbox 现在有两条：原 active publish + 新 deprecate
        let entries: Vec<_> = list_outbox(&m)
            .await
            .into_iter()
            .filter(|e| e.inbox_message.policy_id() == &policy_id)
            .collect();
        assert_eq!(entries.len(), 2);
        let deprecated_entry = entries
            .iter()
            .find(|e| e.inbox_message.policy().status == PolicyStatus::Deprecated)
            .expect("应有 deprecated 状态的 entry");
        assert!(matches!(deprecated_entry.target, OutboxTarget::Broadcast));
    }

    /// 空 team_root：所有方法都不应 panic
    #[tokio::test]
    async fn empty_team_root_no_panic() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-04-21T00:00:00Z".parse().unwrap();
        let (_, pushed) = publish_sample_policy(&m, now).await;
        assert_eq!(pushed, 1, "broadcast 总产生 1 条 outbox entry");

        let report = m.run_stale_sweep(now_seconds()).await.unwrap();
        assert!(report.stale_claims.is_empty());
        assert!(report.deprecated_claims.is_empty());
    }

    #[tokio::test]
    async fn status_snapshot_collects_agents_claims_and_outbox_counts() {
        let (m, team) = build(7, 30);
        let agent = AgentId::new("agent-a").unwrap();
        fs::create_dir_all(paths::team_store_agent_claims_dir(team.path(), &agent))
            .await
            .unwrap();
        let active = sample_claim_at(&agent, now_seconds(), ClaimStatus::Active);
        let stale = sample_claim_at(&agent, now_seconds(), ClaimStatus::Stale);
        seed_claim(team.path(), &active).await;
        seed_claim(team.path(), &stale).await;

        let now: DateTime<Utc> = "2026-04-22T00:00:00Z".parse().unwrap();
        publish_sample_policy(&m, now).await;
        pull_and_ack(&m, &agent).await;

        let snapshot = m.status_snapshot(now_seconds()).await.unwrap();
        assert_eq!(snapshot.counts.agents, 1);
        assert_eq!(snapshot.counts.claims, 2);
        assert_eq!(snapshot.counts.active_claims, 1);
        assert_eq!(snapshot.counts.stale_claims, 1);
        assert_eq!(snapshot.counts.outbox_entries, 1);
        assert_eq!(snapshot.counts.send_events, 1);
        assert_eq!(snapshot.agents.len(), 1);
        assert_eq!(snapshot.agents[0].agent_id, agent);
        assert_eq!(snapshot.agents[0].mirror_claims, 2);
        assert_eq!(snapshot.send_log.len(), 1);
        assert_eq!(snapshot.send_log[0].agent_id, agent);
    }

    // ---- pull_inbox 行为 ----

    #[tokio::test]
    async fn pull_inbox_lazy_registers_unknown_agent() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-new").unwrap();
        let claims_dir = paths::team_store_agent_claims_dir(m.team_root(), &agent);
        assert!(!claims_dir.exists(), "前置：注册目录不应存在");

        let pulled = m.pull_inbox(&agent).await.unwrap();
        assert!(pulled.is_empty(), "无 outbox 时应返回空");
        assert!(claims_dir.exists(), "首次 pull 应 lazy register");
    }

    #[tokio::test]
    async fn pull_inbox_returns_active_broadcast_for_new_agent() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        publish_sample_policy(&m, now).await;

        let agent = AgentId::new("agent-c").unwrap();
        let pulled = m.pull_inbox(&agent).await.unwrap();
        assert_eq!(pulled.len(), 1, "新 agent 应收到 active 广播");

        // ACK 前重复 pull 必须返回同一稳定 ID，并累计 offer 尝试。
        let pulled_again = m.pull_inbox(&agent).await.unwrap();
        assert_eq!(pulled_again.len(), 1);
        assert_eq!(pulled_again[0].id, pulled[0].id);
        let outbox = list_outbox(&m).await;
        assert_eq!(outbox[0].offered_to[0].attempts, 2);
        assert!(outbox[0].delivered_to.is_empty());

        m.ack_inbox(&agent, &[pulled[0].id.clone()]).await.unwrap();
        // 重复 ACK 幂等，确认后不再重投。
        m.ack_inbox(&agent, &[pulled[0].id.clone()]).await.unwrap();
        assert!(m.pull_inbox(&agent).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pull_inbox_skips_targeted_for_other_agent() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let agent_a = AgentId::new("agent-a").unwrap();
        let (name, statement, scope) = sample_policy_fields();
        m.publish_new_policy(name, statement, scope, now, Some(vec![agent_a.clone()]))
            .await
            .unwrap();

        // agent-b 不是 targeted 目标
        let agent_b = AgentId::new("agent-b").unwrap();
        let pulled_b = m.pull_inbox(&agent_b).await.unwrap();
        assert!(pulled_b.is_empty(), "targeted entry 不应投给非目标 agent");

        let pulled_a = m.pull_inbox(&agent_a).await.unwrap();
        assert_eq!(pulled_a.len(), 1);
    }

    #[tokio::test]
    async fn pull_inbox_skips_deprecated_broadcast_for_new_agent() {
        let (m, _team) = build(7, 30);
        let publish_at: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let deprecate_at: DateTime<Utc> = "2026-05-14T11:00:00Z".parse().unwrap();
        let (policy_id, _) = publish_sample_policy(&m, publish_at).await;
        m.deprecate_policy(&policy_id, deprecate_at).await.unwrap();

        // 已 deprecated 的广播 policy → 新 agent 不应收到任何广播 entry
        let agent = AgentId::new("agent-new").unwrap();
        let pulled = m.pull_inbox(&agent).await.unwrap();
        assert!(
            pulled.is_empty(),
            "新 agent 不应补发已 deprecated 广播：{pulled:?}"
        );
    }

    #[tokio::test]
    async fn pull_inbox_delivers_broadcast_deprecation_to_existing_agent() {
        let (m, _team) = build(7, 30);
        let publish_at: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let deprecate_at: DateTime<Utc> = "2026-05-14T11:00:00Z".parse().unwrap();
        let (policy_id, _) = publish_sample_policy(&m, publish_at).await;
        let agent = AgentId::new("agent-a").unwrap();

        let first = m.pull_inbox(&agent).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].policy().status, PolicyStatus::Active);

        m.deprecate_policy(&policy_id, deprecate_at).await.unwrap();
        let second = m.pull_inbox(&agent).await.unwrap();
        assert_eq!(
            second.len(),
            2,
            "未 ACK 的 active 与 deprecation 都必须可见"
        );
        assert_eq!(second[0].policy().status, PolicyStatus::Active);
        assert_eq!(second[1].policy().status, PolicyStatus::Deprecated);
        assert_eq!(second[1].event_at(), deprecate_at);
        let ids = second
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        m.ack_inbox(&agent, &ids).await.unwrap();
        assert!(m.pull_inbox(&agent).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pull_inbox_orders_by_event_time() {
        let (m, _team) = build(7, 30);
        let early: DateTime<Utc> = "2026-05-14T09:00:00Z".parse().unwrap();
        let late: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let (name1, statement1, scope1) = sample_policy_fields();
        m.publish_new_policy(name1, statement1, scope1, late, None)
            .await
            .unwrap();
        let (name2, statement2, scope2) = sample_policy_fields();
        m.publish_new_policy(name2, statement2, scope2, early, None)
            .await
            .unwrap();

        let agent = AgentId::new("agent-a").unwrap();
        let pulled = m.pull_inbox(&agent).await.unwrap();
        assert_eq!(pulled.len(), 2);
        assert_eq!(pulled[0].event_at(), early, "先发的事件时间应在前");
        assert_eq!(pulled[1].event_at(), late);
    }

    #[tokio::test]
    async fn pull_inbox_records_offer_without_confirming_delivery() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        publish_sample_policy(&m, now).await;
        let agent = AgentId::new("agent-a").unwrap();

        let _pulled = m.pull_inbox(&agent).await.unwrap();
        let outbox = list_outbox(&m).await;
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].delivered_to.is_empty());
        assert_eq!(outbox[0].offered_to.len(), 1);
        assert_eq!(outbox[0].offered_to[0].agent_id, agent);
        assert_eq!(outbox[0].offered_to[0].attempts, 1);
    }

    #[tokio::test]
    async fn pull_inbox_targeted_active_filter_does_not_apply() {
        // targeted 投递不受"当前 policy 是否 active"过滤；deprecate 后再 pull 仍能拿到该 entry
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-a").unwrap();
        let publish_at: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let deprecate_at: DateTime<Utc> = "2026-05-14T11:00:00Z".parse().unwrap();
        let (name, statement, scope) = sample_policy_fields();
        let (policy_id, _) = m
            .publish_new_policy(
                name,
                statement,
                scope,
                publish_at,
                Some(vec![agent.clone()]),
            )
            .await
            .unwrap();
        m.deprecate_policy(&policy_id, deprecate_at).await.unwrap();

        // 现在有两条 outbox entry：发布的 targeted active + deprecate 的 targeted deprecated
        // agent-a 第一次 pull 应拿到两条（targeted 不过滤 policy.status）
        let pulled = m.pull_inbox(&agent).await.unwrap();
        assert_eq!(pulled.len(), 2);
    }

    #[tokio::test]
    async fn ack_inbox_rejects_unknown_unoffered_and_wrong_target() {
        let (m, _team) = build(7, 30);
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();

        let unknown = InboxId::random();
        assert!(matches!(
            m.ack_inbox(&agent_a, std::slice::from_ref(&unknown)).await,
            Err(InboxAckError::UnknownInbox { inbox_id }) if inbox_id == unknown
        ));

        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let broadcast_id = list_outbox(&m).await[0].inbox_id.clone();
        assert!(matches!(
            m.ack_inbox(&agent_a, std::slice::from_ref(&broadcast_id))
                .await,
            Err(InboxAckError::NotOffered { inbox_id, .. }) if inbox_id == broadcast_id
        ));

        let (name, statement, scope) = sample_policy_fields();
        m.publish_new_policy(
            name,
            statement,
            scope,
            "2026-05-14T11:00:00Z".parse().unwrap(),
            Some(vec![agent_a.clone()]),
        )
        .await
        .unwrap();
        let targeted_id = list_outbox(&m)
            .await
            .into_iter()
            .find(|entry| matches!(entry.target, OutboxTarget::Targeted { .. }))
            .unwrap()
            .inbox_id;
        assert!(matches!(
            m.ack_inbox(&agent_a, std::slice::from_ref(&targeted_id))
                .await,
            Err(InboxAckError::NotOffered { inbox_id, .. }) if inbox_id == targeted_id
        ));
        assert!(matches!(
            m.ack_inbox(&agent_b, std::slice::from_ref(&targeted_id))
                .await,
            Err(InboxAckError::TargetMismatch { inbox_id, .. }) if inbox_id == targeted_id
        ));
    }

    #[tokio::test]
    async fn ack_inbox_prevalidation_failure_does_not_partially_deliver() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-a").unwrap();
        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        publish_sample_policy(&m, "2026-05-14T11:00:00Z".parse().unwrap()).await;
        let entries = list_outbox(&m).await;
        let first_id = entries[0].inbox_id.clone();
        let second_id = entries[1].inbox_id.clone();
        outbox_io::record_offered(m.team_root(), &first_id, &agent, now_seconds())
            .await
            .unwrap();

        assert!(matches!(
            m.ack_inbox(&agent, &[first_id.clone(), second_id.clone()])
                .await,
            Err(InboxAckError::NotOffered { inbox_id, .. }) if inbox_id == second_id
        ));
        let after = list_outbox(&m).await;
        assert!(after.iter().all(|entry| entry.delivered_to.is_empty()));
    }

    #[tokio::test]
    async fn legacy_delivered_entry_does_not_replay_and_accepts_duplicate_ack() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-a").unwrap();
        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let inbox_id = list_outbox(&m).await[0].inbox_id.clone();
        outbox_io::append_delivered(m.team_root(), &inbox_id, &agent, now_seconds())
            .await
            .unwrap();

        assert!(m.pull_inbox(&agent).await.unwrap().is_empty());
        m.ack_inbox(&agent, &[inbox_id]).await.unwrap();
    }

    // ---- list_actions 行为 ----

    #[tokio::test]
    async fn list_actions_empty_when_no_outbox() {
        let (m, _team) = build(7, 30);
        assert!(m.list_actions().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_actions_groups_broadcast_and_delivered_agents() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let (policy_id, _) = publish_sample_policy(&m, now).await;
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        pull_and_ack(&m, &agent_b).await;
        pull_and_ack(&m, &agent_a).await;

        let rows = m.list_actions().await.unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.created_at, now);
        assert_eq!(row.policy_id, policy_id);
        assert_eq!(row.target_kind, "broadcast");
        assert_eq!(row.outbox_entries, 1);
        assert_eq!(row.send_events, 2);
        assert!(row.target_agents.is_empty());
        assert_eq!(
            row.delivered_agents
                .iter()
                .map(AgentId::as_str)
                .collect::<Vec<_>>(),
            vec!["agent-a", "agent-b"]
        );
    }

    #[tokio::test]
    async fn list_actions_groups_targeted_entries_by_action() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let (name, statement, scope) = sample_policy_fields();
        m.publish_new_policy(
            name,
            statement,
            scope,
            now,
            Some(vec![
                AgentId::new("agent-b").unwrap(),
                AgentId::new("agent-a").unwrap(),
            ]),
        )
        .await
        .unwrap();

        let rows = m.list_actions().await.unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.target_kind, "targeted");
        assert_eq!(row.outbox_entries, 2);
        assert_eq!(row.inbox_ids.len(), 2);
        assert_eq!(
            row.target_agents
                .iter()
                .map(AgentId::as_str)
                .collect::<Vec<_>>(),
            vec!["agent-a", "agent-b"]
        );

        let entries = list_outbox(&m).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].maintainer_action_id, entries[1].maintainer_action_id,
            "同一次定向发布必须共享 action id"
        );
    }

    #[tokio::test]
    async fn list_actions_orders_newest_first() {
        let (m, _team) = build(7, 30);
        let older: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let newer: DateTime<Utc> = "2026-05-14T11:00:00Z".parse().unwrap();
        publish_sample_policy(&m, older).await;
        let (name, statement, scope) = sample_policy_fields();
        let (newer_policy_id, _) = m
            .publish_new_policy(name, statement, scope, newer, None)
            .await
            .unwrap();

        let rows = m.list_actions().await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].policy_id, newer_policy_id);
        assert!(rows[0].created_at > rows[1].created_at);
    }

    // ---- list_outbox_entries 行为 ----

    #[tokio::test]
    async fn list_outbox_entries_returns_all_newest_first_by_default() {
        let (m, _team) = build(7, 30);
        let older: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        let newer: DateTime<Utc> = "2026-05-14T11:00:00Z".parse().unwrap();
        let (older_policy_id, _) = publish_sample_policy(&m, older).await;
        let (name, statement, scope) = sample_policy_fields();
        let (newer_policy_id, _) = m
            .publish_new_policy(name, statement, scope, newer, None)
            .await
            .unwrap();

        let rows = m.list_outbox_entries(None, None).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].inbox_message.policy_id(), &newer_policy_id);
        assert_eq!(rows[1].inbox_message.policy_id(), &older_policy_id);
    }

    #[tokio::test]
    async fn list_outbox_entries_applies_limit() {
        let (m, _team) = build(7, 30);
        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let (name, statement, scope) = sample_policy_fields();
        let (newer_policy_id, _) = m
            .publish_new_policy(
                name,
                statement,
                scope,
                "2026-05-14T11:00:00Z".parse().unwrap(),
                None,
            )
            .await
            .unwrap();

        let rows = m.list_outbox_entries(Some(1), None).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].inbox_message.policy_id(), &newer_policy_id);
    }

    #[tokio::test]
    async fn list_outbox_entries_filters_targeted_open_and_closed() {
        let (m, _team) = build(7, 30);
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let (name, statement, scope) = sample_policy_fields();
        m.publish_new_policy(
            name,
            statement,
            scope,
            "2026-05-14T10:00:00Z".parse().unwrap(),
            Some(vec![agent_a.clone(), agent_b]),
        )
        .await
        .unwrap();
        pull_and_ack(&m, &agent_a).await;

        let open_rows = m.list_outbox_entries(None, Some(true)).await.unwrap();
        let closed_rows = m.list_outbox_entries(None, Some(false)).await.unwrap();
        assert_eq!(open_rows.len(), 1);
        assert_eq!(closed_rows.len(), 1);
        assert_eq!(open_rows[0].delivered_to.len(), 0);
        assert_eq!(closed_rows[0].delivered_to.len(), 1);
    }

    #[tokio::test]
    async fn list_outbox_entries_keeps_active_broadcast_open_after_delivery() {
        let (m, _team) = build(7, 30);
        publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        let agent = AgentId::new("agent-a").unwrap();
        pull_and_ack(&m, &agent).await;

        assert_eq!(
            m.list_outbox_entries(None, Some(true)).await.unwrap().len(),
            1
        );
        assert!(m
            .list_outbox_entries(None, Some(false))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn list_outbox_entries_closes_deprecated_broadcast_when_active_recipients_received_it() {
        let (m, _team) = build(7, 30);
        let agent = AgentId::new("agent-a").unwrap();
        let new_agent = AgentId::new("agent-new").unwrap();
        let (policy_id, _) =
            publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        pull_and_ack(&m, &agent).await;
        m.deprecate_policy(&policy_id, "2026-05-14T11:00:00Z".parse().unwrap())
            .await
            .unwrap();
        m.ensure_agent_registered(&new_agent).await.unwrap();
        pull_and_ack(&m, &agent).await;

        let open_rows = m.list_outbox_entries(None, Some(true)).await.unwrap();
        let closed_rows = m.list_outbox_entries(None, Some(false)).await.unwrap();
        assert!(open_rows.is_empty());
        assert_eq!(closed_rows.len(), 2);
        assert!(closed_rows
            .iter()
            .any(|row| row.inbox_message.policy().status == PolicyStatus::Active));
        assert!(closed_rows
            .iter()
            .any(|row| row.inbox_message.policy().status == PolicyStatus::Deprecated));
    }

    #[tokio::test]
    async fn list_outbox_entries_keeps_deprecated_broadcast_open_until_active_recipients_receive_it(
    ) {
        let (m, _team) = build(7, 30);
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let (policy_id, _) =
            publish_sample_policy(&m, "2026-05-14T10:00:00Z".parse().unwrap()).await;
        pull_and_ack(&m, &agent_a).await;
        pull_and_ack(&m, &agent_b).await;
        m.deprecate_policy(&policy_id, "2026-05-14T11:00:00Z".parse().unwrap())
            .await
            .unwrap();
        pull_and_ack(&m, &agent_a).await;

        let open_rows = m.list_outbox_entries(None, Some(true)).await.unwrap();
        assert_eq!(open_rows.len(), 1);
        assert_eq!(
            open_rows[0].inbox_message.policy().status,
            PolicyStatus::Deprecated
        );
    }

    // ---- list_send_log 行为 ----

    #[tokio::test]
    async fn list_send_log_empty_when_no_outbox() {
        let (m, _team) = build(7, 30);
        assert!(m.list_send_log().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_send_log_contains_both_pulled_agents() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        publish_sample_policy(&m, now).await;

        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        pull_and_ack(&m, &agent_b).await;
        pull_and_ack(&m, &agent_a).await;

        let rows = m.list_send_log().await.unwrap();
        assert_eq!(rows.len(), 2, "两个 agent 各一行");
        for row in &rows {
            assert_eq!(row.message_type, PolicyMessageType::PolicyUpdate);
        }
        let agents: std::collections::HashSet<_> =
            rows.iter().map(|r| r.agent_id.as_str()).collect();
        assert!(agents.contains("agent-a"));
        assert!(agents.contains("agent-b"));
    }

    #[tokio::test]
    async fn list_send_log_flattens_broadcast_entries() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        publish_sample_policy(&m, now).await;
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        pull_and_ack(&m, &agent_a).await;
        pull_and_ack(&m, &agent_b).await;

        let rows = m.list_send_log().await.unwrap();
        // 同一 broadcast outbox entry → 2 行（每个 agent 一行），共享 inbox_id 与 action_id
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].inbox_id, rows[1].inbox_id);
        assert_eq!(rows[0].maintainer_action_id, rows[1].maintainer_action_id);
    }

    #[tokio::test]
    async fn list_send_log_orders_by_sent_at_ascending() {
        let (m, _team) = build(7, 30);
        let now: DateTime<Utc> = "2026-05-14T10:00:00Z".parse().unwrap();
        // 两次独立 publish → 两条 outbox entry → 先 pull 的 sent_at 在前
        publish_sample_policy(&m, now).await;
        let agent = AgentId::new("agent-a").unwrap();
        pull_and_ack(&m, &agent).await;
        // 让 sent_at 拉开
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let (name, statement, scope) = sample_policy_fields();
        m.publish_new_policy(name, statement, scope, now, None)
            .await
            .unwrap();
        pull_and_ack(&m, &agent).await;

        let rows = m.list_send_log().await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].sent_at <= rows[1].sent_at, "应按 sent_at 升序");
    }
}
