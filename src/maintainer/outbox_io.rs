//! Outbox 文件读写。
//!
//! 提供 maintainer outbox 目录的扫描、单条写入、offer 更新与 delivered_to 追加。
//! 仅做文件 I/O，不维护任何业务规则（例如"该不该投给某 agent"），由 caller 决定。
//!
//! 并发说明：所有写操作要求 caller 经由 Maintainer 入口按固定顺序持有进程内
//! `Maintainer::outbox_lock` 与跨进程的 `maintainer/outbox.lock`，避免复合读改写互相覆盖。

use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, Utc};
use tokio::fs;

use crate::claim::{AgentId, DeliveredMark, InboxId, OfferedMark, OutboxEntry};
use crate::storage::{paths, read_yaml, write_yaml_atomic};

/// 扫描 outbox 目录，返回所有 entry，不保证顺序。
pub async fn list(team_root: &Path) -> anyhow::Result<Vec<OutboxEntry>> {
    let dir = paths::team_store_outbox_dir(team_root);
    if !fs::try_exists(&dir)
        .await
        .with_context(|| format!("检查 outbox 目录失败: {dir:?}"))?
    {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut rd = fs::read_dir(&dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".yaml") || name.contains(".tmp.") {
            continue;
        }
        let metadata = entry.metadata().await?;
        let is_stale_inbox_reservation = name
            .strip_suffix(".yaml")
            .is_some_and(|id| id.parse::<InboxId>().is_ok())
            && metadata.is_file()
            && metadata.len() == 0;
        if is_stale_inbox_reservation {
            // `mint_unique_id_in_dir` 崩溃后可能留下有效 inbox 名的空占位；它仍占住 ID，
            // 但不是可投递 entry，读取方跳过才能让下一次动作自行恢复。
            log::warn!(target: "maintainer", "跳过遗留的 outbox inbox 占位文件: {path:?}");
            continue;
        }
        out.push(read_yaml(&path).await?);
    }
    Ok(out)
}

/// 原子写一条 outbox entry。文件名固定 `<inbox_id>.yaml`。
pub async fn write(team_root: &Path, entry: &OutboxEntry) -> anyhow::Result<()> {
    let dir = paths::team_store_outbox_dir(team_root);
    let path = dir.join(format!("{}.yaml", entry.inbox_id));
    write_yaml_atomic(&path, entry).await?;
    Ok(())
}

/// 记录一次向指定 Agent 提供消息的尝试。
///
/// 首次调用创建 mark；后续调用保留首次时间、更新最近时间并递增 attempts。
/// 调用方必须经由 Maintainer 入口同时持有进程内锁与 outbox 文件锁。
pub async fn record_offered(
    team_root: &Path,
    inbox_id: &InboxId,
    agent_id: &AgentId,
    offered_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let dir = paths::team_store_outbox_dir(team_root);
    let path = dir.join(format!("{inbox_id}.yaml"));
    let mut entry: OutboxEntry = read_yaml(&path).await?;
    if let Some(mark) = entry
        .offered_to
        .iter_mut()
        .find(|mark| &mark.agent_id == agent_id)
    {
        mark.last_offered_at = offered_at;
        mark.attempts = mark.attempts.saturating_add(1);
    } else {
        entry.offered_to.push(OfferedMark {
            agent_id: agent_id.clone(),
            first_offered_at: offered_at,
            last_offered_at: offered_at,
            attempts: 1,
        });
    }
    write_yaml_atomic(&path, &entry).await?;
    Ok(())
}

/// 给指定 inbox_id 对应的 outbox entry 追加一条 DeliveredMark。
///
/// 行为：读 → 追加 → atomic rewrite。如果 (inbox_id, agent_id) 已存在则跳过（幂等）。
/// 调用方必须经由 Maintainer 入口同时持有进程内锁与 outbox 文件锁，否则并发可能丢失更新。
pub async fn append_delivered(
    team_root: &Path,
    inbox_id: &InboxId,
    agent_id: &AgentId,
    sent_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let dir = paths::team_store_outbox_dir(team_root);
    let path = dir.join(format!("{inbox_id}.yaml"));
    let mut entry: OutboxEntry = read_yaml(&path).await?;
    if entry.delivered_to.iter().any(|d| &d.agent_id == agent_id) {
        return Ok(());
    }
    entry.delivered_to.push(DeliveredMark {
        agent_id: agent_id.clone(),
        sent_at,
    });
    write_yaml_atomic(&path, &entry).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{
        InboxMessage, InboxMessageKind, MaintainerActionId, OutboxTarget, Policy, PolicyId,
        PolicyMessageType, PolicyStatus,
    };

    fn sample_entry(inbox_id: InboxId) -> OutboxEntry {
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "p".into(),
            statement: "stmt".into(),
            scope: "sc".into(),
            status: PolicyStatus::Active,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: None,
        };
        OutboxEntry {
            inbox_id: inbox_id.clone(),
            maintainer_action_id: MaintainerActionId::random(),
            target: OutboxTarget::Broadcast,
            created_at: "2026-05-14T10:00:00Z".parse().unwrap(),
            offered_to: vec![],
            delivered_to: vec![],
            inbox_message: InboxMessage {
                id: inbox_id,
                kind: InboxMessageKind::PolicyUpdate { policy },
                handled_at: None,
            },
        }
    }

    #[tokio::test]
    async fn list_missing_dir_returns_empty() {
        let team = tempfile::tempdir().unwrap();
        let listed = list(team.path()).await.unwrap();
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn list_skips_stale_zero_byte_inbox_reservation() {
        let team = tempfile::tempdir().unwrap();
        let reserved = InboxId::random();
        let dir = paths::team_store_outbox_dir(team.path());
        fs::create_dir_all(&dir).await.unwrap();
        let reservation = fs::File::create(dir.join(format!("{reserved}.yaml")))
            .await
            .unwrap();
        drop(reservation);

        let entry = sample_entry(InboxId::random());
        write(team.path(), &entry).await.unwrap();

        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed, vec![entry]);
    }

    #[tokio::test]
    async fn list_propagates_invalid_team_root_error() {
        let team = tempfile::tempdir().unwrap();
        let root_file = team.path().join("not-a-directory");
        tokio::fs::write(&root_file, b"not a directory")
            .await
            .unwrap();

        let error = list(&root_file).await.unwrap_err();

        assert!(error.to_string().contains("检查 outbox 目录失败"));
    }

    #[tokio::test]
    async fn write_then_list_round_trip() {
        let team = tempfile::tempdir().unwrap();
        let entry = sample_entry(InboxId::random());
        write(team.path(), &entry).await.unwrap();
        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], entry);
    }

    #[tokio::test]
    async fn list_skips_tmp_files() {
        let team = tempfile::tempdir().unwrap();
        let entry = sample_entry(InboxId::random());
        write(team.path(), &entry).await.unwrap();
        let dir = paths::team_store_outbox_dir(team.path());
        // 模拟 atomic write 中途残留的临时文件
        fs::write(dir.join("inbox_aaaa1111.tmp.bad"), b"junk")
            .await
            .unwrap();
        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed.len(), 1);
    }

    #[tokio::test]
    async fn append_delivered_adds_mark() {
        let team = tempfile::tempdir().unwrap();
        let mid = InboxId::random();
        write(team.path(), &sample_entry(mid.clone()))
            .await
            .unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let now: DateTime<Utc> = "2026-05-14T10:01:00Z".parse().unwrap();
        append_delivered(team.path(), &mid, &agent, now)
            .await
            .unwrap();
        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed[0].delivered_to.len(), 1);
        assert_eq!(listed[0].delivered_to[0].agent_id, agent);
        assert_eq!(listed[0].delivered_to[0].sent_at, now);
    }

    #[tokio::test]
    async fn record_offered_tracks_first_last_and_attempts() {
        let team = tempfile::tempdir().unwrap();
        let mid = InboxId::random();
        write(team.path(), &sample_entry(mid.clone()))
            .await
            .unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let first: DateTime<Utc> = "2026-05-14T10:01:00Z".parse().unwrap();
        let second: DateTime<Utc> = "2026-05-14T10:02:00Z".parse().unwrap();

        record_offered(team.path(), &mid, &agent, first)
            .await
            .unwrap();
        record_offered(team.path(), &mid, &agent, second)
            .await
            .unwrap();

        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed[0].offered_to.len(), 1);
        assert_eq!(listed[0].offered_to[0].agent_id, agent);
        assert_eq!(listed[0].offered_to[0].first_offered_at, first);
        assert_eq!(listed[0].offered_to[0].last_offered_at, second);
        assert_eq!(listed[0].offered_to[0].attempts, 2);
        assert!(listed[0].delivered_to.is_empty());
    }

    #[tokio::test]
    async fn append_delivered_is_idempotent_for_same_agent() {
        let team = tempfile::tempdir().unwrap();
        let mid = InboxId::random();
        write(team.path(), &sample_entry(mid.clone()))
            .await
            .unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let t1: DateTime<Utc> = "2026-05-14T10:01:00Z".parse().unwrap();
        let t2: DateTime<Utc> = "2026-05-14T10:02:00Z".parse().unwrap();
        append_delivered(team.path(), &mid, &agent, t1)
            .await
            .unwrap();
        append_delivered(team.path(), &mid, &agent, t2)
            .await
            .unwrap();
        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed[0].delivered_to.len(), 1, "重复 agent 不应追加");
        assert_eq!(listed[0].delivered_to[0].sent_at, t1);
    }

    #[tokio::test]
    async fn append_delivered_supports_multiple_agents() {
        let team = tempfile::tempdir().unwrap();
        let mid = InboxId::random();
        write(team.path(), &sample_entry(mid.clone()))
            .await
            .unwrap();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let now: DateTime<Utc> = "2026-05-14T10:01:00Z".parse().unwrap();
        append_delivered(team.path(), &mid, &agent_a, now)
            .await
            .unwrap();
        append_delivered(team.path(), &mid, &agent_b, now)
            .await
            .unwrap();
        let listed = list(team.path()).await.unwrap();
        assert_eq!(listed[0].delivered_to.len(), 2);
    }
}
