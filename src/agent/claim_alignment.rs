//! 知识修改方案的状态核对；恢复时放弃失效的剩余操作，不再调用模型。

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::claim::Claim;

pub(crate) const STALE_PLAN_WARNING: &str =
    "恢复时 Claim 已变化或缺少分析快照，已放弃本批尚未执行的 Claim 修改及 Dispute；已执行结果保留。";

#[derive(Debug, thiserror::Error)]
#[error("知识库忙碌：Dream 关联提交正在执行或等待恢复，请稍后使用相同参数重试")]
pub(crate) struct KnowledgeBusy;

/// 调用者必须持有知识锁，并在这次检查后保持锁直到本地提交结束。
pub(crate) async fn ensure_knowledge_ready(home: &std::path::Path) -> anyhow::Result<()> {
    if tokio::fs::try_exists(home.join("runtime/supervisor/dream_pending.yaml")).await? {
        return Err(KnowledgeBusy.into());
    }
    Ok(())
}

/// 普通读取不等待分析锁；提交结束会先更新持久状态再清除 pending，因此前后核对能识别
/// 整个 Dream 提交恰好发生在读取期间的窗口。专用 Dream 读取仍使用知识锁。
pub(crate) struct DreamReadBoundary(Option<Vec<u8>>);

impl DreamReadBoundary {
    pub(crate) async fn begin(home: &std::path::Path) -> anyhow::Result<Self> {
        let state = Self::state(home).await?;
        ensure_knowledge_ready(home).await?;
        Ok(Self(state))
    }

    pub(crate) async fn finish(self, home: &std::path::Path) -> anyhow::Result<()> {
        ensure_knowledge_ready(home).await?;
        if self.0 != Self::state(home).await? {
            return Err(KnowledgeBusy.into());
        }
        Ok(())
    }

    async fn state(home: &std::path::Path) -> anyhow::Result<Option<Vec<u8>>> {
        match tokio::fs::read(home.join("runtime/supervisor/dream_state.yaml")).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// 精确目标匹配补齐写盘后、进度保存前的中断；其他版本绝不推定为执行成功。
pub(crate) fn observe_applied(targets: &[Claim], current: &[Claim], applied: &mut Vec<Claim>) {
    for target in targets {
        if !applied.iter().any(|c| c.id == target.id) && current.contains(target) {
            applied.push(target.clone());
        }
    }
}

pub(crate) fn changes(
    analysis: Option<&[Claim]>,
    targets: &[Claim],
    applied: &[Claim],
    current: &[Claim],
) -> Value {
    let current: BTreeMap<_, _> = current.iter().map(|c| (&c.id, c)).collect();
    let mut expected: BTreeMap<_, Option<&Claim>> = analysis
        .unwrap_or_default()
        .iter()
        .map(|c| (&c.id, Some(c)))
        .collect();
    for target in targets {
        expected.entry(&target.id).or_insert(None);
    }
    for claim in applied {
        if targets.contains(claim) {
            expected.insert(&claim.id, Some(claim));
        }
    }
    json!(expected
        .into_iter()
        .filter_map(|(id, before)| {
            let actual = current.get(id).copied();
            (actual != before).then(|| {
                let before = json!(before);
                let current = json!(actual);
                let changed_fields = if before.is_null() || current.is_null() {
                    vec!["existence"]
                } else {
                    [
                        "name",
                        "statement",
                        "scope",
                        "holder",
                        "confidence",
                        "status",
                        "created_at",
                        "updated_at",
                        "source_claim_ids",
                        "evidence_summary",
                    ]
                    .into_iter()
                    .filter(|field| before[*field] != current[*field])
                    .collect()
                };
                json!({"id":id,"before":before,"current":current,"changed_fields":changed_fields})
            })
        })
        .collect::<Vec<_>>())
}

pub(crate) fn analysis_is_stale(analysis: Option<&[Claim]>, changes: &Value) -> bool {
    analysis.is_none() || changes.as_array().is_some_and(|c| !c.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{AgentId, ClaimStatus, Confidence};

    #[tokio::test]
    async fn ordinary_read_detects_pending_and_a_commit_completed_during_read() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let home = dir.path();
        let before = DreamReadBoundary::begin(home).await?;
        let state = home.join("runtime/supervisor/dream_state.yaml");
        crate::storage::write_yaml_atomic(
            &state,
            &json!({"known_versions": {"claim_11111111": "new"}}),
        )
        .await?;
        assert!(before.finish(home).await.unwrap_err().is::<KnowledgeBusy>());
        DreamReadBoundary::begin(home).await?.finish(home).await?;
        let before = DreamReadBoundary::begin(home).await?;
        let pending = home.join("runtime/supervisor/dream_pending.yaml");
        tokio::fs::write(&pending, "pending").await?;
        assert!(DreamReadBoundary::begin(home)
            .await
            .err()
            .unwrap()
            .is::<KnowledgeBusy>());
        assert!(before.finish(home).await.unwrap_err().is::<KnowledgeBusy>());
        Ok(())
    }

    fn claim() -> Claim {
        Claim {
            id: "claim_11111111".parse().unwrap(),
            name: "retry_rule".into(),
            statement: "Retry only idempotent operations".into(),
            scope: "service / imports".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "integration contract".into(),
        }
    }

    #[test]
    fn alignment_compares_content_and_recognizes_partial_writes_without_hiding_external_edits() {
        let before = claim();
        let mut target = before.clone();
        target.statement = "Retry idempotent operations within the budget".into();
        let mut applied = vec![];
        observe_applied(
            std::slice::from_ref(&target),
            std::slice::from_ref(&target),
            &mut applied,
        );
        assert!(!analysis_is_stale(
            Some(std::slice::from_ref(&before)),
            &changes(
                Some(std::slice::from_ref(&before)),
                std::slice::from_ref(&target),
                &applied,
                std::slice::from_ref(&target)
            )
        ));
        let mut external = target.clone();
        external.scope = "service / imports / tenant-isolated".into();
        let difference = changes(
            Some(std::slice::from_ref(&before)),
            std::slice::from_ref(&target),
            &applied,
            std::slice::from_ref(&external),
        );
        assert!(analysis_is_stale(
            Some(std::slice::from_ref(&before)),
            &difference
        ));
        // 已执行目标不在待执行方案中时，以保存的快照为准。
        assert_eq!(
            changes(
                Some(std::slice::from_ref(&external)),
                &[],
                &applied,
                std::slice::from_ref(&external)
            ),
            json!([])
        );
        assert!(analysis_is_stale(None, &json!([])));
    }
}
