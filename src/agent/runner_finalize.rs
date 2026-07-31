//! session finalize 阶段的复盘占位符解析和落地前校验。
//!
//! 本模块只承接 `finalize_session` 后到 claim/dispute prepared 产物之间的逻辑，
//! 不做文件写入，避免半成品状态。

use std::str::FromStr;

use chrono::{DateTime, Utc};
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::Value;

use super::prepare::{
    prepare_claim_updates, prepare_claims, prepare_disputes, validate_claim_ids,
    validate_visible_policy_sources,
};
use crate::api::{resolve_placeholders, RecapOutcome};
use crate::claim::{AgentId, Claim, ClaimId, Dispute, SourceId};

pub(super) fn prepare_recap_value(
    raw: Value,
    agent_id: &AgentId,
    allowed_claim_ids: &FxHashSet<ClaimId>,
    local_by_id: &FxHashMap<ClaimId, Claim>,
    now: DateTime<Utc>,
) -> anyhow::Result<(Vec<ClaimId>, Vec<Claim>, Vec<Dispute>)> {
    let resolved = resolve_placeholders(raw, now)?;
    let outcome: RecapOutcome = serde_json::from_value(resolved)
        .map_err(|e| anyhow::anyhow!("finalize_session 输出无法解析为 RecapOutcome: {e}"))?;

    let mut used_claim_ids =
        validate_claim_ids("used_claim_ids", outcome.used_claim_ids, allowed_claim_ids)?;

    let mut allowed_source_claim_ids = allowed_claim_ids.clone();
    let mut allowed_policy_ids = FxHashSet::default();
    for claim in local_by_id.values() {
        for source in &claim.source_claim_ids {
            match source {
                SourceId::Claim(claim_id) => {
                    allowed_source_claim_ids.insert(claim_id.clone());
                }
                SourceId::Policy(policy_id) => {
                    allowed_policy_ids.insert(policy_id.clone());
                }
            }
        }
    }
    validate_visible_policy_sources("new_claims", &outcome.new_claims, &allowed_policy_ids)?;
    validate_visible_policy_sources(
        "updated_claims",
        &outcome.updated_claims,
        &allowed_policy_ids,
    )?;

    // dispute 引用仍只允许本次上下文对象与本批新 claim；历史来源仅供 provenance 复用。
    let mut allowed_dispute_claim_ids = allowed_claim_ids.clone();
    for c in &outcome.new_claims {
        let id = ClaimId::from_str(&c.id)
            .map_err(|e| anyhow::anyhow!("finalize new_claims[*].id 不是合法 ClaimId: {e}"))?;
        allowed_source_claim_ids.insert(id.clone());
        allowed_dispute_claim_ids.insert(id);
    }

    // —— 校验前置：先把 outcome 内所有可能 bail 的检查跑完，再做任何 I/O，
    // 避免出现"已经落了部分 claim 但 dispute 校验失败"的半成品状态。
    let mut prepared_claims =
        prepare_claims(outcome.new_claims, &allowed_source_claim_ids, agent_id, now)?;
    let prepared_updates = prepare_claim_updates(
        outcome.updated_claims,
        local_by_id,
        &allowed_source_claim_ids,
        now,
    )?;
    for claim in &prepared_updates {
        if !used_claim_ids.contains(&claim.id) {
            used_claim_ids.push(claim.id.clone());
        }
    }
    prepared_claims.extend(prepared_updates);
    let prepared_disputes = prepare_disputes(
        outcome.new_disputes,
        &allowed_dispute_claim_ids,
        agent_id,
        now,
    )?;
    Ok((used_claim_ids, prepared_claims, prepared_disputes))
}

#[cfg(test)]
mod tests {
    use rustc_hash::{FxHashMap, FxHashSet};
    use serde_json::json;

    use super::*;
    use crate::claim::{ClaimStatus, Confidence, PolicyId, SourceId};

    fn local_claim() -> Claim {
        Claim {
            id: "claim_1234abcd".parse().unwrap(),
            name: "old_rule".into(),
            statement: "旧结论".into(),
            scope: "service / prod".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Stale,
            created_at: "2026-04-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "旧证据".into(),
        }
    }

    #[test]
    fn recap_can_update_existing_claim_status_and_refresh_timestamp() {
        let mut existing = local_claim();
        existing.source_claim_ids = vec![SourceId::Policy(PolicyId::random())];
        let local_by_id = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let allowed = FxHashSet::from_iter([existing.id.clone()]);
        let now: DateTime<Utc> = "2026-05-20T12:34:56.789Z".parse().unwrap();
        let raw = json!({
            "new_claims": [],
            "updated_claims": [{
                "id": existing.id.as_str(),
                "name": "current_rule",
                "statement": "新证据确认后的最新结论",
                "scope": "service / prod",
                "confidence": "high",
                "status": "active",
                "evidence_summary": "本 session 的新证据",
                "source_claim_ids": []
            }],
            "used_claim_ids": [],
            "new_disputes": []
        });

        let (used, claims, disputes) =
            prepare_recap_value(raw, &existing.holder, &allowed, &local_by_id, now).unwrap();

        assert_eq!(used, vec![existing.id.clone()]);
        assert_eq!(claims.len(), 1);
        assert!(disputes.is_empty());
        assert_eq!(claims[0].id, existing.id);
        assert_eq!(claims[0].status, ClaimStatus::Active);
        assert_eq!(claims[0].created_at, existing.created_at);
        assert!(claims[0].source_claim_ids.is_empty());
        assert_eq!(
            claims[0].updated_at,
            Some("2026-05-20T12:34:56Z".parse().unwrap())
        );
    }

    #[test]
    fn recap_can_preserve_visible_historical_sources() {
        let mut existing = local_claim();
        let historical_policy = PolicyId::random();
        let historical_claim = ClaimId::random();
        existing.source_claim_ids = vec![
            SourceId::Policy(historical_policy.clone()),
            SourceId::Claim(historical_claim.clone()),
        ];
        let local_by_id = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let allowed = FxHashSet::from_iter([existing.id.clone()]);
        let raw = json!({
            "new_claims": [],
            "updated_claims": [{
                "id": existing.id.as_str(),
                "name": "current_rule",
                "statement": "更新后仍沿用历史来源",
                "scope": "service / prod",
                "confidence": "high",
                "status": "active",
                "evidence_summary": "历史来源仍与当前结论相关",
                "source_claim_ids": [
                    historical_policy.as_str(),
                    historical_claim.as_str()
                ]
            }],
            "used_claim_ids": [],
            "new_disputes": []
        });

        let (_, claims, _) = prepare_recap_value(
            raw,
            &existing.holder,
            &allowed,
            &local_by_id,
            "2026-05-20T12:34:56Z".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(
            claims[0].source_claim_ids,
            vec![
                SourceId::Policy(historical_policy),
                SourceId::Claim(historical_claim)
            ]
        );
    }

    #[test]
    fn recap_rejects_invented_policy_source() {
        let existing = local_claim();
        let invented_policy = PolicyId::random();
        let local_by_id = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let allowed = FxHashSet::from_iter([existing.id.clone()]);
        let raw = json!({
            "new_claims": [],
            "updated_claims": [{
                "id": existing.id.as_str(),
                "name": "current_rule",
                "statement": "引用了不存在的 policy",
                "scope": "service / prod",
                "confidence": "high",
                "status": "active",
                "evidence_summary": "不应通过校验",
                "source_claim_ids": [invented_policy.as_str()]
            }],
            "used_claim_ids": [],
            "new_disputes": []
        });

        let err = prepare_recap_value(
            raw,
            &existing.holder,
            &allowed,
            &local_by_id,
            "2026-05-20T12:34:56Z".parse().unwrap(),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("不是本次 LLM 输入中可见的 PolicyId"));
    }

    #[test]
    fn recap_rejects_existing_claim_update_without_status() {
        let existing = local_claim();
        let local_by_id = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let allowed = FxHashSet::from_iter([existing.id.clone()]);
        let raw = json!({
            "new_claims": [],
            "updated_claims": [{
                "id": existing.id.as_str(),
                "name": "current_rule",
                "statement": "新结论",
                "scope": "service / prod",
                "confidence": "high",
                "evidence_summary": "新证据",
                "source_claim_ids": []
            }],
            "used_claim_ids": [],
            "new_disputes": []
        });

        let err = prepare_recap_value(
            raw,
            &existing.holder,
            &allowed,
            &local_by_id,
            "2026-05-20T12:34:56Z".parse().unwrap(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("status 缺失"));
    }
}
