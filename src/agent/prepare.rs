//! runner 的 DTO 预处理与协议校验辅助函数。
//!
//! 这里集中处理 LLM 输出到本地实体的转换、claim id 白名单校验、source 去重等纯逻辑，
//! 让 `runner.rs` 和 `inbox.rs` 只保留流程编排。

use std::str::FromStr;

use chrono::{DateTime, Utc};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::api::TurnMessage;
use crate::claim::{
    AgentId, Claim, ClaimId, ClaimStatus, Dispute, DisputeId, DisputeStatus, PolicyId, SourceId,
};
use crate::time::truncate_to_second;

pub(super) fn llm_visible_claims(claims: Vec<Claim>) -> Vec<Claim> {
    claims
        .into_iter()
        .filter(|claim| claim.status != ClaimStatus::Deprecated)
        .collect()
}

pub(super) fn allowed_claim_ids_for_recap(
    local: &[Claim],
    transcript: &[TurnMessage],
) -> FxHashSet<ClaimId> {
    let mut allowed: FxHashSet<ClaimId> = local.iter().map(|claim| claim.id.clone()).collect();
    // recap 刻意采用扁平可见语义：transcript 任意位置出现的合法 ClaimId 都可继续引用，
    // 包括 router 候选的 source_claim_ids；这里不按 JSON 字段区分用途。
    for message in transcript {
        allowed.extend(extract_claim_ids_from_text(&message.content));
    }
    allowed
}

fn extract_claim_ids_from_text(text: &str) -> FxHashSet<ClaimId> {
    let mut ids = FxHashSet::default();
    let bytes = text.as_bytes();
    let mut start = 0;
    while let Some(relative) = text[start..].find("claim_") {
        let idx = start + relative;
        let end = idx + 14;
        if end <= bytes.len()
            && bytes[idx + 6..end].iter().all(u8::is_ascii_hexdigit)
            && bytes[idx + 6..end]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
            && bytes.get(end).is_none_or(|b| !is_claim_id_token_byte(*b))
            && bytes
                .get(idx.wrapping_sub(1))
                .is_none_or(|b| !is_claim_id_token_byte(*b))
        {
            if let Ok(id) = ClaimId::from_str(&text[idx..end]) {
                ids.insert(id);
            }
        }
        start = idx + 6;
    }
    ids
}

fn is_claim_id_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

pub(super) fn validate_claim_ids(
    field: &str,
    ids: Vec<ClaimId>,
    allowed: &FxHashSet<ClaimId>,
) -> anyhow::Result<Vec<ClaimId>> {
    let invalid: Vec<_> = ids
        .iter()
        .filter(|id| !allowed.contains(*id))
        .map(ToString::to_string)
        .collect();
    if !invalid.is_empty() {
        anyhow::bail!(
            "{field} 包含不在本次上下文中的 claim id: {}",
            invalid.join(", ")
        );
    }
    Ok(dedup_claim_ids(ids))
}

pub(super) fn dedup_claim_ids(ids: Vec<ClaimId>) -> Vec<ClaimId> {
    let mut seen = FxHashSet::default();
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }
    out
}

pub(super) fn push_source_id_once(sources: &mut Vec<SourceId>, source: SourceId) {
    if !sources.contains(&source) {
        sources.push(source);
    }
}

pub(super) fn sorted_source_ids(ids: FxHashSet<SourceId>) -> Vec<SourceId> {
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    ids
}

/// 校验 LLM 新写入的 policy provenance。
///
/// 模型可以不输出 PolicyId；一旦输出，就必须来自调用方提供的本次 LLM 可见集合，
/// 不能凭空构造不可见的历史来源。
pub(super) fn validate_visible_policy_sources(
    field: &str,
    drafts: &[crate::api::ClaimDraft],
    allowed_policy_ids: &FxHashSet<PolicyId>,
) -> anyhow::Result<()> {
    for (idx, draft) in drafts.iter().enumerate() {
        for (source_idx, raw) in draft.source_claim_ids.iter().enumerate() {
            let source = SourceId::from_str(raw).map_err(|e| {
                anyhow::anyhow!(
                    "{field}[{idx}].source_claim_ids[{source_idx}]={raw:?} 解析失败 \
                     (期望 claim_/policy_ 前缀): {e}"
                )
            })?;
            let SourceId::Policy(policy_id) = source else {
                continue;
            };
            if !allowed_policy_ids.contains(&policy_id) {
                anyhow::bail!(
                    "{field}[{idx}].source_claim_ids[{source_idx}]={raw:?} \
                     不是本次 LLM 输入中可见的 PolicyId"
                );
            }
        }
    }
    Ok(())
}

/// 把 `ClaimDraft` 列表转成可直接落盘的 `Claim`（id 已由 `resolve_placeholders` 派生）。
///
/// `now` 用作所有 claim 的 `created_at`（截到秒），与 placeholder resolver 派生 id
/// 时使用的纳秒时间共享同一时刻，保证 id 与时间字段语义一致。
///
/// `source_claim_ids` 在此边界把 DTO 层的 `Vec<String>` 解析为 `Vec<SourceId>`：
/// 每个元素必须以 `claim_` 或 `policy_` 开头并通过后缀格式校验，否则整批 prepare
/// 失败（validate-before-I/O 不变量保证不留半写状态）。
pub(super) fn prepare_claims(
    drafts: Vec<crate::api::ClaimDraft>,
    allowed_source_claim_ids: &FxHashSet<ClaimId>,
    holder: &AgentId,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<Claim>> {
    let created_at = truncate_to_second(now);
    let mut out = Vec::with_capacity(drafts.len());
    for (idx, draft) in drafts.into_iter().enumerate() {
        let id = ClaimId::from_str(&draft.id).map_err(|e| {
            anyhow::anyhow!("new_claims[{idx}].id 解析失败 (期望真实 ClaimId): {e}")
        })?;
        let mut sources: Vec<SourceId> = Vec::with_capacity(draft.source_claim_ids.len());
        for (j, raw) in draft.source_claim_ids.iter().enumerate() {
            let s = SourceId::from_str(raw).map_err(|e| {
                anyhow::anyhow!(
                    "new_claims[{idx}].source_claim_ids[{j}]={raw:?} 解析失败 (期望 claim_/policy_ 前缀): {e}"
                )
            })?;
            if let SourceId::Claim(id) = &s {
                if !allowed_source_claim_ids.contains(id) {
                    anyhow::bail!(
                        "new_claims[{idx}].source_claim_ids[{j}]={raw:?} 不在本次上下文/本批新生成中"
                    );
                }
            }
            push_source_id_once(&mut sources, s);
        }
        out.push(Claim {
            id,
            name: draft.name,
            statement: draft.statement,
            scope: draft.scope,
            holder: holder.clone(),
            confidence: draft.confidence,
            status: ClaimStatus::Active,
            created_at,
            updated_at: None,
            source_claim_ids: sources,
            evidence_summary: draft.evidence_summary,
        });
    }
    Ok(out)
}

pub(super) fn prepare_claim_updates(
    drafts: Vec<crate::api::ClaimDraft>,
    local_by_id: &FxHashMap<ClaimId, Claim>,
    allowed_source_claim_ids: &FxHashSet<ClaimId>,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<Claim>> {
    let updated_at = truncate_to_second(now);
    let mut out = Vec::with_capacity(drafts.len());
    let mut seen_ids = FxHashSet::default();
    for (idx, draft) in drafts.into_iter().enumerate() {
        let id = ClaimId::from_str(&draft.id).map_err(|e| {
            anyhow::anyhow!("updated_claims[{idx}].id 解析失败 (期望已有 ClaimId): {e}")
        })?;
        let Some(existing) = local_by_id.get(&id) else {
            anyhow::bail!(
                "updated_claims[{idx}].id={} 不是当前 agent 本地已有 claim",
                id
            );
        };
        if !seen_ids.insert(id.clone()) {
            anyhow::bail!("updated_claims 含重复 id={id}");
        }
        let status_raw = draft.status.ok_or_else(|| {
            anyhow::anyhow!("updated_claims[{idx}].status 缺失（必须是 active/stale/deprecated）")
        })?;
        let status = match status_raw.as_str() {
            "active" => ClaimStatus::Active,
            "stale" => ClaimStatus::Stale,
            "deprecated" => ClaimStatus::Deprecated,
            _ => anyhow::bail!(
                "updated_claims[{idx}].status={status_raw:?} 非法（必须是 active/stale/deprecated）"
            ),
        };
        // 更新结果以 LLM 本轮显式返回的完整来源列表为准；调用方已基于本次可见输入
        // 校验 PolicyId，下面继续校验 ClaimId，并统一解析、去重后整体替换旧值。
        let mut sources = Vec::with_capacity(draft.source_claim_ids.len());
        for (j, raw) in draft.source_claim_ids.iter().enumerate() {
            let source = SourceId::from_str(raw).map_err(|e| {
                anyhow::anyhow!(
                    "updated_claims[{idx}].source_claim_ids[{j}]={raw:?} 解析失败 (期望 claim_/policy_ 前缀): {e}"
                )
            })?;
            if let SourceId::Claim(id) = &source {
                if !allowed_source_claim_ids.contains(id) {
                    anyhow::bail!(
                        "updated_claims[{idx}].source_claim_ids[{j}]={raw:?} 不在本次上下文/本批新生成中"
                    );
                }
            }
            push_source_id_once(&mut sources, source);
        }
        out.push(Claim {
            id,
            name: draft.name,
            statement: draft.statement,
            scope: draft.scope,
            holder: existing.holder.clone(),
            confidence: draft.confidence,
            status,
            created_at: existing.created_at,
            updated_at: Some(updated_at),
            source_claim_ids: sources,
            evidence_summary: draft.evidence_summary,
        });
    }
    Ok(out)
}

/// 把 `DisputeDraft` 列表转成可直接落盘的 `Dispute`，并执行：
/// - claims 必须 ≥2
/// - claims 不能有重复
/// - claims 内每条要么是真实已存在的 ClaimId（在 `allowed_for_refs` 里），要么是本批新
///   生成的 ClaimId（同样已经被加入 `allowed_for_refs`）
pub(super) fn prepare_disputes(
    drafts: Vec<crate::api::DisputeDraft>,
    allowed_for_refs: &FxHashSet<ClaimId>,
    reporter_agent_id: &AgentId,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<Dispute>> {
    let created_at = truncate_to_second(now);
    let mut out = Vec::with_capacity(drafts.len());
    for (idx, d) in drafts.into_iter().enumerate() {
        let mut parsed: Vec<ClaimId> = Vec::with_capacity(d.claims.len());
        for (j, raw) in d.claims.iter().enumerate() {
            let id = ClaimId::from_str(raw).map_err(|e| {
                anyhow::anyhow!("new_disputes[{idx}].claims[{j}]={raw:?} 不是合法 ClaimId: {e}")
            })?;
            parsed.push(id);
        }
        let unique: FxHashSet<&ClaimId> = parsed.iter().collect();
        if unique.len() != parsed.len() {
            anyhow::bail!(
                "new_disputes[{idx}].claims 含重复 ClaimId（dispute 应是不同 claim 之间的冲突）"
            );
        }
        if parsed.len() < 2 {
            anyhow::bail!("new_disputes[{idx}].claims 至少需要 2 条不同的 claim 引用");
        }
        let invalid: Vec<String> = parsed
            .iter()
            .filter(|id| !allowed_for_refs.contains(*id))
            .map(ToString::to_string)
            .collect();
        if !invalid.is_empty() {
            anyhow::bail!(
                "new_disputes[{idx}].claims 包含不在本次上下文/本批新生成中的 claim id: {}",
                invalid.join(", ")
            );
        }
        let dispute_id = DisputeId::from_str(&d.id).map_err(|e| {
            anyhow::anyhow!("new_disputes[{idx}].id 解析失败 (期望真实 DisputeId): {e}")
        })?;
        out.push(Dispute {
            id: dispute_id,
            name: d.name,
            reporter_agent_id: reporter_agent_id.clone(),
            claims: parsed,
            summary: d.summary,
            status: DisputeStatus::Open,
            created_at,
            resolved_at: None,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ClaimDraft, DisputeDraft};

    fn sample_claim(id: ClaimId) -> Claim {
        Claim {
            id,
            name: "sample".into(),
            statement: "sample statement".into(),
            scope: "sample scope".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: crate::claim::Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: "2026-05-18T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "sample evidence".into(),
        }
    }

    fn sample_claim_draft(id: String, status: Option<&str>) -> ClaimDraft {
        ClaimDraft {
            id,
            name: "updated_sample".into(),
            statement: "updated statement".into(),
            scope: "updated scope".into(),
            confidence: crate::claim::Confidence::High,
            status: status.map(str::to_string),
            evidence_summary: "updated evidence".into(),
            source_claim_ids: Vec::new(),
        }
    }

    #[test]
    fn prepare_new_claim_ignores_returned_status_and_forces_active() {
        let id = ClaimId::random();
        let holder = AgentId::new("agent-a").unwrap();
        let now: DateTime<Utc> = "2026-05-19T10:00:00.456Z".parse().unwrap();
        let claims = prepare_claims(
            vec![sample_claim_draft(
                id.into_string(),
                Some("not-a-valid-status"),
            )],
            &FxHashSet::default(),
            &holder,
            now,
        )
        .unwrap();

        assert_eq!(claims[0].status, ClaimStatus::Active);
        assert_eq!(claims[0].updated_at, None);
        assert_eq!(claims[0].created_at.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn inbox_policy_sources_are_optional_but_reject_invisible_policy() {
        let current_policy = PolicyId::random();
        let visible_historical_policy = PolicyId::random();
        let unknown_policy = PolicyId::random();
        let mut draft = sample_claim_draft(ClaimId::random().into_string(), None);
        let allowed =
            FxHashSet::from_iter([current_policy.clone(), visible_historical_policy.clone()]);

        validate_visible_policy_sources("new_claims", std::slice::from_ref(&draft), &allowed)
            .unwrap();
        draft.source_claim_ids = vec![current_policy.to_string()];
        validate_visible_policy_sources("new_claims", std::slice::from_ref(&draft), &allowed)
            .unwrap();
        draft.source_claim_ids = vec![visible_historical_policy.to_string()];
        validate_visible_policy_sources("updated_claims", std::slice::from_ref(&draft), &allowed)
            .unwrap();

        draft.source_claim_ids = vec![unknown_policy.to_string()];
        let unknown =
            validate_visible_policy_sources("new_claims", std::slice::from_ref(&draft), &allowed)
                .unwrap_err();
        assert!(unknown.to_string().contains("不是本次 LLM 输入中可见"));
    }

    #[test]
    fn prepare_claim_update_requires_status() {
        let existing = sample_claim(ClaimId::random());
        let local = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let err = prepare_claim_updates(
            vec![sample_claim_draft(existing.id.into_string(), None)],
            &local,
            &FxHashSet::default(),
            "2026-05-19T10:00:00Z".parse().unwrap(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("status 缺失"));
    }

    #[test]
    fn prepare_claim_update_rejects_unknown_status() {
        let existing = sample_claim(ClaimId::random());
        let local = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let err = prepare_claim_updates(
            vec![sample_claim_draft(
                existing.id.into_string(),
                Some("unknown"),
            )],
            &local,
            &FxHashSet::default(),
            "2026-05-19T10:00:00Z".parse().unwrap(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("status=\"unknown\" 非法"));
    }

    #[test]
    fn prepare_claim_update_rejects_invisible_claim_source() {
        let existing = sample_claim(ClaimId::random());
        let local = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let invisible_source = ClaimId::random();
        let mut draft = sample_claim_draft(existing.id.clone().into_string(), Some("active"));
        draft.source_claim_ids = vec![invisible_source.to_string()];

        let err = prepare_claim_updates(
            vec![draft],
            &local,
            &FxHashSet::from_iter([existing.id]),
            "2026-05-19T10:00:00Z".parse().unwrap(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("不在本次上下文/本批新生成中"));
    }

    #[test]
    fn prepare_claim_update_applies_status_and_backend_timestamp() {
        let mut existing = sample_claim(ClaimId::random());
        existing.status = ClaimStatus::Stale;
        existing.source_claim_ids = vec![
            SourceId::Policy(PolicyId::random()),
            SourceId::Claim(ClaimId::random()),
        ];
        let local = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);
        let now: DateTime<Utc> = "2026-05-19T10:00:00.789Z".parse().unwrap();
        let replacement_claim = ClaimId::random();
        let mut draft = sample_claim_draft(existing.id.clone().into_string(), Some("active"));
        draft.source_claim_ids = vec![replacement_claim.to_string(), replacement_claim.to_string()];
        let updates = prepare_claim_updates(
            vec![draft],
            &local,
            &FxHashSet::from_iter([replacement_claim.clone()]),
            now,
        )
        .unwrap();

        assert_eq!(updates[0].id, existing.id);
        assert_eq!(updates[0].holder, existing.holder);
        assert_eq!(updates[0].created_at, existing.created_at);
        assert_eq!(updates[0].status, ClaimStatus::Active);
        assert_eq!(
            updates[0].source_claim_ids,
            vec![SourceId::Claim(replacement_claim)]
        );
        assert_eq!(
            updates[0].updated_at,
            Some("2026-05-19T10:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn allowed_claim_ids_for_recap_includes_local_and_transcript_ids() {
        let local_id = ClaimId::random();
        let transcript_id = ClaimId::from_str("claim_abcd1234").unwrap();
        let local = vec![sample_claim(local_id.clone())];
        let transcript = vec![TurnMessage {
            role: "assistant".into(),
            content: format!("router result includes {transcript_id}"),
        }];

        let allowed = allowed_claim_ids_for_recap(&local, &transcript);

        assert!(allowed.contains(&local_id));
        assert!(allowed.contains(&transcript_id));
        assert_eq!(allowed.len(), 2);
    }

    #[test]
    fn allowed_claim_ids_for_recap_rejects_invalid_or_partial_tokens() {
        let transcript = vec![TurnMessage {
            role: "assistant".into(),
            content: "claim_abcg1234 claim_abcd123 claim_abcd12345 xclaim_abcd1234 claim_abcd1234g claim_abcd1234_foo claim_abcd1234Z".into(),
        }];

        let allowed = allowed_claim_ids_for_recap(&[], &transcript);

        assert!(allowed.is_empty());
    }

    #[test]
    fn allowed_claim_ids_for_recap_deduplicates_repeated_ids() {
        let id = ClaimId::from_str("claim_abcd1234").unwrap();
        let local = vec![sample_claim(id.clone())];
        let transcript = vec![TurnMessage {
            role: "assistant".into(),
            content: format!("{id} {id}"),
        }];

        let allowed = allowed_claim_ids_for_recap(&local, &transcript);

        assert_eq!(allowed.len(), 1);
        assert!(allowed.contains(&id));
    }

    #[test]
    fn allowed_claim_ids_for_recap_accepts_standalone_id_with_punctuation() {
        let id = ClaimId::from_str("claim_abcd1234").unwrap();
        let transcript = vec![TurnMessage {
            role: "assistant".into(),
            content: "see claim_abcd1234, then claim_abcd1234.".into(),
        }];

        let allowed = allowed_claim_ids_for_recap(&[], &transcript);

        assert_eq!(allowed.len(), 1);
        assert!(allowed.contains(&id));
    }

    #[test]
    fn allowed_claim_ids_for_recap_includes_router_source_ids_by_design() {
        let candidate_id = ClaimId::from_str("claim_abcd1234").unwrap();
        let source_id = ClaimId::from_str("claim_1234abcd").unwrap();
        let transcript = vec![TurnMessage {
            role: "user".into(),
            content: serde_json::json!({
                "ok": true,
                "output": {
                    "mode": "query",
                    "candidate_claims": [{
                        "id": candidate_id.as_str(),
                        "source_claim_ids": [source_id.as_str()]
                    }]
                }
            })
            .to_string(),
        }];

        let allowed = allowed_claim_ids_for_recap(&[], &transcript);

        assert_eq!(allowed, FxHashSet::from_iter([candidate_id, source_id]));
    }

    #[test]
    fn prepare_disputes_sets_reporter_to_current_agent() {
        let reporter = AgentId::new("agent-a").unwrap();
        let claim_a = ClaimId::random();
        let claim_b = ClaimId::random();
        let allowed_for_refs = FxHashSet::from_iter([claim_a.clone(), claim_b.clone()]);
        let drafts = vec![DisputeDraft {
            id: DisputeId::random().into_string(),
            name: "claim_conflict".into(),
            claims: vec![claim_a.into_string(), claim_b.into_string()],
            summary: "conflict".into(),
        }];

        let disputes = prepare_disputes(
            drafts,
            &allowed_for_refs,
            &reporter,
            "2026-05-18T00:00:00Z".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(disputes.len(), 1);
        assert_eq!(disputes[0].reporter_agent_id, reporter);
    }
}
