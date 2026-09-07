//! Claim 老化扫描与通知去重；outbox 同时保存投递快照和每条建议的轮次。

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rustc_hash::FxHashSet;

use super::{
    history, outbox_io, ClaimSweepReport, Maintainer, SweepNotification, SweepNotificationError,
};
use crate::claim::{AgentId, ClaimId, ClaimStatus, OutboxTarget, SweepNotificationItem};

impl Maintainer {
    /// 按 mirror 的最近更新时间检测候选，只向 holder 发送属性建议。
    pub async fn run_stale_sweep(&self, now: DateTime<Utc>) -> anyhow::Result<ClaimSweepReport> {
        let mut report = ClaimSweepReport::default();
        let mut groups: BTreeMap<AgentId, Vec<SweepNotificationItem>> = BTreeMap::new();
        for (agent, claim) in self.list_all_claims().await? {
            let effective_updated_at = claim.effective_updated_at();
            let age = now.signed_duration_since(effective_updated_at);
            let suggested_status = match claim.status {
                ClaimStatus::Active if age >= self.stale_after => {
                    report.stale_claims.push((agent.clone(), claim.id.clone()));
                    ClaimStatus::Stale
                }
                ClaimStatus::Stale if age >= self.deprecate_after => {
                    report
                        .deprecated_claims
                        .push((agent.clone(), claim.id.clone()));
                    ClaimStatus::Deprecated
                }
                _ => continue,
            };
            groups
                .entry(agent)
                .or_default()
                .push(SweepNotificationItem {
                    claim_id: claim.id,
                    effective_updated_at,
                    suggested_status,
                });
        }
        if !groups.is_empty() {
            self.send_sweep_notifications(groups, &mut report, now)
                .await?;
        }
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

    /// 每次扫描保留全部候选，notifications 只记录该次实际创建的消息。
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

    async fn send_sweep_notifications(
        &self,
        groups: BTreeMap<AgentId, Vec<SweepNotificationItem>>,
        report: &mut ClaimSweepReport,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let _guard = self.outbox_lock.lock().await;
        let _file_guard = self.lock_outbox_file().await?;
        let mut scheduled = FxHashSet::default();
        for entry in outbox_io::list(&self.team_root).await? {
            if let OutboxTarget::Targeted { target_agent } = entry.target {
                for item in entry.sweep_items {
                    scheduled.insert((target_agent.clone(), item));
                }
            }
        }

        for (agent_id, mut items) in groups {
            // 未 ACK 的建议仍由原消息重投；已 ACK 的建议由 Agent 本地负责内化重试。
            items.retain(|item| !scheduled.contains(&(agent_id.clone(), item.clone())));
            if items.is_empty() {
                continue;
            }
            items.sort_by(|left, right| left.claim_id.cmp(&right.claim_id));
            let stale_claims = suggested_claim_ids(&items, ClaimStatus::Stale);
            let deprecated_claims = suggested_claim_ids(&items, ClaimStatus::Deprecated);
            let statement =
                claim_sweep_notification_statement(&agent_id, &stale_claims, &deprecated_claims);
            match self
                .claim_update_suggestion_locked(
                    statement,
                    now,
                    Some(vec![agent_id.clone()]),
                    &items,
                )
                .await
            {
                Ok((policy_id, pushed)) => report.notifications.push(SweepNotification {
                    agent_id,
                    stale_claims,
                    deprecated_claims,
                    policy_id,
                    pushed,
                }),
                Err(err) => {
                    log::warn!(
                        target: "maintainer",
                        "claim sweep 通知 agent={} 失败，等待下次 sweep 重试: {err:#}",
                        agent_id
                    );
                    report.notification_errors.push(SweepNotificationError {
                        agent_id,
                        stale_claims,
                        deprecated_claims,
                        error: format!("{err:#}"),
                    });
                }
            }
        }
        Ok(())
    }
}

fn suggested_claim_ids(items: &[SweepNotificationItem], status: ClaimStatus) -> Vec<ClaimId> {
    items
        .iter()
        .filter(|item| item.suggested_status == status)
        .map(|item| item.claim_id.clone())
        .collect()
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
