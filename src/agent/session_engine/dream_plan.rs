//! Dream 修改计划的结构与语义边界校验；不执行 I/O 或持久化。
use crate::claim::{Claim, ClaimId, ClaimStatus, Confidence, SourceId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Review {
    pub(super) quality: String,
    pub(super) evidence: String,
    pub(super) consolidation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
// 持久化旧计划中的退役字段读取后丢弃；模型提交工具仍拒绝未知字段。
pub(super) struct Plan {
    pub(super) review: Review,
    pub(super) groups: Vec<OperationGroup>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Kind {
    Quality,
    Evidence,
    Consolidation,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperationGroup {
    pub(super) kind: Kind,
    pub(super) reason: String,
    pub(super) evidence_ids: Vec<String>,
    pub(super) coverage: Vec<Coverage>,
    // 兼容已保存的旧计划；未提交的旧草稿仍须补齐依据并通过内容复核。
    #[serde(default)]
    pub(super) change_basis: Vec<ChangeBasis>,
    pub(super) updates: Vec<Update>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangeBasis {
    pub(super) claim_id: ClaimId,
    pub(super) removed_or_changed: String,
    pub(super) added: String,
    pub(super) justification: String,
    pub(super) evidence_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Coverage {
    pub(super) input_id: ClaimId,
    pub(super) output_ids: Vec<ClaimId>,
    pub(super) note: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Update {
    pub(super) id: ClaimId,
    pub(super) name: String,
    pub(super) statement: String,
    pub(super) scope: String,
    pub(super) confidence: Confidence,
    pub(super) status: ClaimStatus,
    pub(super) source_claim_ids: Vec<SourceId>,
    pub(super) evidence_summary: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PreparedGroup {
    pub(super) before: Vec<Claim>,
    pub(super) after: Vec<Claim>,
    #[serde(default)]
    pub(super) completed: bool,
    #[serde(default)]
    pub(super) skipped: bool,
}

fn confidence_rank(value: Confidence) -> u8 {
    match value {
        Confidence::Low => 0,
        Confidence::Medium => 1,
        Confidence::High => 2,
    }
}
pub(super) fn validate_plan(
    plan: &Plan,
    readable: &BTreeMap<ClaimId, Claim>,
    owner: &crate::claim::AgentId,
    receipts: &BTreeMap<String, Value>,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<PreparedGroup>> {
    anyhow::ensure!(
        !plan.review.quality.trim().is_empty()
            && !plan.review.evidence.trim().is_empty()
            && !plan.review.consolidation.trim().is_empty(),
        "Dream must report A/B/C review coverage"
    );
    let mut used = BTreeSet::new();
    let mut prepared = Vec::new();
    for group in &plan.groups {
        anyhow::ensure!(
            !group.reason.trim().is_empty() && !group.updates.is_empty(),
            "Dream group requires a reason and updates"
        );
        anyhow::ensure!(
            group
                .evidence_ids
                .iter()
                .all(|id| receipts.contains_key(id)),
            "Dream references evidence never read"
        );
        let independent = group.evidence_ids.iter().any(|id| {
            receipts.get(id).is_some_and(|r| {
                r.get("tool")
                    .and_then(Value::as_str)
                    .is_some_and(|tool| tool == "file_read")
                    && r.pointer("/output/file_version").is_some()
            })
        });
        let command_evidence = group.evidence_ids.iter().any(|id| {
            receipts
                .get(id)
                .and_then(|r| r.get("tool"))
                .and_then(Value::as_str)
                == Some("code_run")
        });
        anyhow::ensure!(!command_evidence || independent, "Command exploration must be confirmed with versioned file_read evidence before changing claims");
        let mut before = Vec::new();
        let mut after = Vec::new();
        for update in &group.updates {
            anyhow::ensure!(
                used.insert(update.id.clone()),
                "Dream claim {} appears in multiple operations; combine edits in one group",
                update.id
            );
            let original = readable.get(&update.id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Dream can only modify completely read claims: {}",
                    update.id
                )
            })?;
            anyhow::ensure!(
                &original.holder == owner && original.status != ClaimStatus::Deprecated,
                "Dream cannot modify foreign or deprecated claims: {}",
                update.id
            );
            anyhow::ensure!(
                [
                    &update.name,
                    &update.statement,
                    &update.scope,
                    &update.evidence_summary
                ]
                .iter()
                .all(|s| !s.trim().is_empty()),
                "Dream claim {} fields name/statement/scope/evidence_summary must be non-empty",
                update.id
            );
            let direct = group.change_basis.iter().any(|basis| {
                basis.claim_id == update.id
                    && basis.evidence_ids.iter().any(|id| {
                        group.evidence_ids.contains(id)
                            && receipts.get(id).is_some_and(|r| {
                                r["tool"] == "file_read"
                                    && r.pointer("/output/file_version").is_some()
                            })
                    })
            });
            let raised = confidence_rank(update.confidence) > confidence_rank(original.confidence);
            anyhow::ensure!(!raised || (direct && update.evidence_summary.trim() != original.evidence_summary.trim()),
                "Dream claim {} confidence increase requires its own versioned file_read evidence in change_basis.evidence_ids and a new evidence_summary in any A/B/C group", update.id);
            // 来源只能从本次已读输入的既有来源中继承，不能凭空创建来源或反向边。
            let sources: Vec<_> = group
                .updates
                .iter()
                .filter_map(|u| readable.get(&u.id))
                .flat_map(|c| c.source_claim_ids.iter().cloned())
                .collect();
            anyhow::ensure!(
                update
                    .source_claim_ids
                    .iter()
                    .all(|id| sources.contains(id) && id != &SourceId::Claim(update.id.clone())),
                "Dream claim {} source is unsupported or self-referential; inherit existing sources, put merge relationships in coverage", update.id
            );
            let mut target = original.clone();
            target.name = update.name.clone();
            target.statement = update.statement.clone();
            target.scope = update.scope.clone();
            target.confidence = update.confidence;
            target.status = update.status;
            target.source_claim_ids = update.source_claim_ids.clone();
            target.evidence_summary = update.evidence_summary.clone();
            if target != *original {
                // B 的资格看磁盘原文，不允许先在草稿里降低 high 再绕过门槛；A/C 仍可整理 high。
                anyhow::ensure!(
                    group.kind != Kind::Evidence || original.confidence != Confidence::High,
                    "Dream B evidence calibration only accepts original medium/low claims: {}; keep the high claim unchanged, do not lower confidence to bypass the gate",
                    update.id
                );
                if group.kind == Kind::Evidence {
                    anyhow::ensure!(
                        update.evidence_summary.trim() != original.evidence_summary.trim(),
                        "Dream B change for {} requires a new evidence_summary describing what was observed and why it supports this change; otherwise keep the claim unchanged",
                        update.id
                    );
                    anyhow::ensure!(
                        direct,
                        "Dream B change for {} must cite its own versioned file_read evidence in change_basis.evidence_ids and group.evidence_ids; a group-level receipt or Claim/Trace alone is insufficient. Obtain direct evidence or keep the claim unchanged",
                        update.id
                    );
                }
                target.updated_at = Some(now);
            }
            before.push(original.clone());
            after.push(target);
        }
        if group.kind == Kind::Consolidation {
            let inputs: BTreeSet<_> = before.iter().map(|c| c.id.clone()).collect();
            let outputs: BTreeSet<_> = after
                .iter()
                .filter(|c| c.status != ClaimStatus::Deprecated)
                .map(|c| c.id.clone())
                .collect();
            anyhow::ensure!(
                inputs.len() >= 2 && !outputs.is_empty(),
                "consolidation requires multiple inputs and surviving outputs"
            );
            let covered: BTreeSet<_> = group.coverage.iter().map(|c| c.input_id.clone()).collect();
            anyhow::ensure!(
                group.coverage.len() == inputs.len() && covered == inputs,
                "consolidation must explain every input's information destination: add explicit keep/update/deprecate operations for {:?}; add coverage for {:?}; each input must appear exactly once in both lists",
                covered.difference(&inputs).collect::<Vec<_>>(), inputs.difference(&covered).collect::<Vec<_>>()
            );
            anyhow::ensure!(
                group.coverage.iter().all(|c| !c.note.trim().is_empty()
                    && !c.output_ids.is_empty()
                    && c.output_ids.iter().all(|id| outputs.contains(id))),
                "consolidation has incomplete information coverage"
            );
            for coverage in &group.coverage {
                let input = before
                    .iter()
                    .find(|c| c.id == coverage.input_id)
                    .ok_or_else(|| anyhow::anyhow!("Missing consolidation input"))?;
                for source in &input.source_claim_ids {
                    // 合并回既有来源本身时不制造自引用。before/coverage 随执行计划落盘，
                    // 保留原始输入到来源的关系；其他来源仍必须显式继承到承接项。
                    anyhow::ensure!(after.iter().any(|c| coverage.output_ids.contains(&c.id)
                        && (c.source_claim_ids.contains(source) || source == &SourceId::Claim(c.id.clone()))),
                        "Consolidation must retain input source references in a surviving destination: input {}, missing source {:?}, destinations {:?}. Inherit this existing source in a destination's source_claim_ids; when the source IS that surviving destination, omit the self-reference and preserve the relationship in coverage and the execution record",
                        input.id, source, coverage.output_ids);
                }
                for output in after.iter().filter(|c| coverage.output_ids.contains(&c.id)) {
                    // 较弱知识合入强 Claim 不能自动继承强置信度；需要该输出自己的新证据。
                    if confidence_rank(output.confidence) > confidence_rank(input.confidence) {
                        let original = readable
                            .get(&output.id)
                            .ok_or_else(|| anyhow::anyhow!("Missing consolidation output"))?;
                        let direct = group.change_basis.iter().any(|b| {
                            b.claim_id == output.id
                                && b.evidence_ids.iter().any(|id| {
                                    group.evidence_ids.contains(id)
                                        && receipts.get(id).is_some_and(|r| {
                                            r["tool"] == "file_read"
                                                && r.pointer("/output/file_version").is_some()
                                        })
                                })
                        });
                        anyhow::ensure!(direct && output.evidence_summary != original.evidence_summary, "Consolidation output {} cannot inherit higher confidence than contributing {}; split outputs, retain the lower certainty, or cite direct new evidence", output.id, input.id);
                    }
                }
            }
            anyhow::ensure!(
                before
                    .iter()
                    .zip(&after)
                    .any(|(a, b)| b.status != ClaimStatus::Deprecated
                        && a.statement != b.statement),
                "consolidation must rewrite surviving content"
            );
        }
        prepared.push(PreparedGroup {
            before,
            after,
            completed: false,
            skipped: false,
        });
    }
    let mut graph = readable.clone();
    for group in &prepared {
        for claim in &group.after {
            graph.insert(claim.id.clone(), claim.clone());
        }
    }
    for group in &prepared {
        for (before, after) in group.before.iter().zip(&group.after) {
            for source in &after.source_claim_ids {
                if before.source_claim_ids.contains(source) {
                    continue;
                }
                if let SourceId::Claim(id) = source {
                    anyhow::ensure!(
                        !reaches(id, &after.id, &graph, &mut BTreeSet::new()),
                        "Dream would introduce a source cycle"
                    );
                }
            }
        }
    }
    Ok(prepared)
}
pub(super) fn reaches(
    id: &ClaimId,
    target: &ClaimId,
    graph: &BTreeMap<ClaimId, Claim>,
    seen: &mut BTreeSet<ClaimId>,
) -> bool {
    if id == target {
        return true;
    }
    if !seen.insert(id.clone()) {
        return false;
    }
    graph.get(id).is_some_and(|c| {
        c.source_claim_ids.iter().any(|s| match s {
            SourceId::Claim(next) => reaches(next, target, graph, seen),
            _ => false,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn fixture() -> (crate::claim::AgentId, Claim) {
        let owner = crate::claim::AgentId::new("agent-test").unwrap();
        let claim = Claim {
            id: "claim_11111111".parse().unwrap(),
            name: "bounded assertion".into(),
            statement: "Only the observed input size is supported.".into(),
            scope: "example".into(),
            holder: owner.clone(),
            confidence: Confidence::Low,
            status: ClaimStatus::Active,
            created_at: crate::time::now_seconds(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "Task completion has no independent result.".into(),
        };
        (owner, claim)
    }
    fn plan(claim: &Claim) -> Plan {
        Plan {
            review: Review {
                quality: "reviewed".into(),
                evidence: "unknown".into(),
                consolidation: "none".into(),
            },
            groups: vec![OperationGroup {
                kind: Kind::Quality,
                reason: "episodic content".into(),
                evidence_ids: vec![],
                coverage: vec![],
                change_basis: vec![],
                updates: vec![Update {
                    id: claim.id.clone(),
                    name: claim.name.clone(),
                    statement: claim.statement.clone(),
                    scope: claim.scope.clone(),
                    confidence: claim.confidence,
                    status: ClaimStatus::Deprecated,
                    source_claim_ids: vec![],
                    evidence_summary: claim.evidence_summary.clone(),
                }],
            }],
        }
    }
    #[test]
    fn dream_rejects_foreign_unseen_and_deprecated_mutations() {
        let (owner, claim) = fixture();
        let p = plan(&claim);
        let now = Utc::now();
        assert!(validate_plan(&p, &BTreeMap::new(), &owner, &BTreeMap::new(), now).is_err());
        let mut foreign = claim.clone();
        foreign.holder = crate::claim::AgentId::new("agent-other").unwrap();
        assert!(validate_plan(
            &p,
            &BTreeMap::from([(foreign.id.clone(), foreign)]),
            &owner,
            &BTreeMap::new(),
            now
        )
        .is_err());
        let mut old = claim.clone();
        old.status = ClaimStatus::Deprecated;
        assert!(validate_plan(
            &p,
            &BTreeMap::from([(old.id.clone(), old)]),
            &owner,
            &BTreeMap::new(),
            now
        )
        .is_err());
    }
    #[test]
    fn dream_rejects_unread_evidence_and_self_report_confidence_inflation() {
        let (owner, claim) = fixture();
        let mut p = plan(&claim);
        p.groups[0].updates[0].confidence = Confidence::High;
        let claims = BTreeMap::from([(claim.id.clone(), claim)]);
        assert!(validate_plan(&p, &claims, &owner, &BTreeMap::new(), Utc::now()).is_err());
        p.groups[0].evidence_ids.push("fabricated".into());
        assert!(validate_plan(&p, &claims, &owner, &BTreeMap::new(), Utc::now()).is_err());
        let receipts = BTreeMap::from([(
            "fabricated".into(),
            json!({"tool":"read_claim","output":{"file_version":{}}}),
        )]);
        assert!(validate_plan(&p, &claims, &owner, &receipts, Utc::now()).is_err());
    }
    #[test]
    fn dream_reports_and_unversioned_commands_do_not_certify_changes() {
        let (owner, claim) = fixture();
        let mut p = plan(&claim);
        p.groups[0].updates[0].confidence = Confidence::High;
        p.groups[0].evidence_ids = vec!["receipt".into()];
        let claims = BTreeMap::from([(claim.id.clone(), claim)]);
        let report = BTreeMap::from([(
            "receipt".into(),
            json!({"tool":"read_trace","output":{"file_version":{},"content":"Dream report says high confidence"}}),
        )]);
        assert!(validate_plan(&p, &claims, &owner, &report, Utc::now()).is_err());
        p.groups[0].updates[0].confidence = Confidence::Low;
        let command = BTreeMap::from([(
            "receipt".into(),
            json!({"tool":"code_run","output":{"stdout":"old result"}}),
        )]);
        assert!(validate_plan(&p, &claims, &owner, &command, Utc::now()).is_err());
    }

    #[test]
    fn dream_evidence_gate_uses_original_confidence_even_with_direct_evidence() {
        for confidence in [Confidence::Low, Confidence::Medium, Confidence::High] {
            let (owner, mut claim) = fixture();
            claim.confidence = confidence;
            let mut p = plan(&claim);
            p.groups[0].kind = Kind::Evidence;
            p.groups[0].evidence_ids = vec!["file".into()];
            p.groups[0].change_basis = vec![ChangeBasis {
                claim_id: claim.id.clone(),
                removed_or_changed: "Revise the evidence boundary.".into(),
                added: String::new(),
                justification: "Direct file observation supports the change.".into(),
                evidence_ids: vec!["file".into()],
            }];
            let update = &mut p.groups[0].updates[0];
            update.status = ClaimStatus::Active;
            update.confidence = Confidence::Low;
            update.evidence_summary =
                "Direct evidence supports only the observed condition.".into();
            let claims = BTreeMap::from([(claim.id.clone(), claim)]);
            let receipts = BTreeMap::from([(
                "file".into(),
                json!({"tool":"file_read","output":{"file_version":{}}}),
            )]);
            let result = validate_plan(&p, &claims, &owner, &receipts, Utc::now());
            if confidence == Confidence::High {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("original medium/low"));
            } else {
                assert!(result.is_ok(), "{result:?}");
            }
        }
    }

    #[test]
    fn dream_evidence_gate_allows_unchanged_high_claims() {
        let (owner, mut claim) = fixture();
        claim.confidence = Confidence::High;
        let mut p = plan(&claim);
        p.groups[0].kind = Kind::Evidence;
        p.groups[0].updates[0].status = ClaimStatus::Active;
        let claims = BTreeMap::from([(claim.id.clone(), claim.clone())]);
        let groups = validate_plan(&p, &claims, &owner, &BTreeMap::new(), Utc::now()).unwrap();
        assert_eq!(groups[0].after, vec![claim]);
    }

    #[test]
    fn dream_b_changes_need_new_summary_and_claim_specific_file_evidence() {
        let mutations: [fn(&mut Update); 7] = [
            |u| u.name = "bounded retry".into(),
            |u| u.statement = "Persisted items can be retried.".into(),
            |u| u.scope = "example / persisted".into(),
            |u| u.confidence = Confidence::High,
            |u| u.confidence = Confidence::Low,
            |u| u.status = ClaimStatus::Stale,
            |u| u.status = ClaimStatus::Deprecated,
        ];
        for mutate in mutations {
            let (owner, mut claim) = fixture();
            claim.confidence = Confidence::Medium;
            let mut p = plan(&claim);
            let group = &mut p.groups[0];
            group.kind = Kind::Evidence;
            group.evidence_ids = vec!["file".into()];
            group.updates[0].status = ClaimStatus::Active;
            mutate(&mut group.updates[0]);
            group.updates[0].evidence_summary =
                "Observed persisted input recovery in result.txt.".into();
            let claims = BTreeMap::from([(claim.id.clone(), claim.clone())]);
            let receipts = BTreeMap::from([(
                "file".into(),
                json!({"tool":"file_read","output":{"file_version":{}}}),
            )]);
            // 其他修改已有组级证据，也不能代替这一条的证据绑定。
            assert!(validate_plan(&p, &claims, &owner, &receipts, Utc::now())
                .unwrap_err()
                .to_string()
                .contains("change_basis.evidence_ids"));
            p.groups[0].change_basis = vec![ChangeBasis {
                claim_id: claim.id.clone(),
                removed_or_changed: "Original boundary".into(),
                added: String::new(),
                justification: "The observed file result supports this change.".into(),
                evidence_ids: vec!["file".into()],
            }];
            assert!(validate_plan(&p, &claims, &owner, &receipts, Utc::now()).is_ok());
            p.groups[0].updates[0].evidence_summary = format!(" {} ", claim.evidence_summary);
            assert!(validate_plan(&p, &claims, &owner, &receipts, Utc::now())
                .unwrap_err()
                .to_string()
                .contains("new evidence_summary"));
        }
    }

    #[test]
    fn dream_b_cannot_borrow_another_claims_receipt_or_self_report() {
        let (owner, claim) = fixture();
        let mut p = plan(&claim);
        let group = &mut p.groups[0];
        group.kind = Kind::Evidence;
        group.updates[0].status = ClaimStatus::Active;
        group.updates[0].evidence_summary = "New observation.".into();
        group.evidence_ids = vec!["file".into(), "source".into()];
        group.change_basis = vec![ChangeBasis {
            claim_id: "claim_22222222".parse().unwrap(),
            removed_or_changed: "Other claim".into(),
            added: String::new(),
            justification: "Other claim evidence.".into(),
            evidence_ids: vec!["file".into()],
        }];
        let claims = BTreeMap::from([(claim.id.clone(), claim.clone())]);
        let mut receipts = BTreeMap::from([
            (
                "file".into(),
                json!({"tool":"file_read","output":{"file_version":{}}}),
            ),
            (
                "source".into(),
                json!({"tool":"read_trace","output":{"file_version":{}}}),
            ),
        ]);
        assert!(validate_plan(&p, &claims, &owner, &receipts, Utc::now()).is_err());
        p.groups[0].change_basis[0].claim_id = claim.id;
        p.groups[0].change_basis[0].evidence_ids = vec!["source".into()];
        for tool in ["read_claim", "read_trace", "code_run", "file_read"] {
            receipts.insert("source".into(), json!({"tool":tool,"output":{}}));
            assert!(
                validate_plan(&p, &claims, &owner, &receipts, Utc::now()).is_err(),
                "{tool}"
            );
        }
        receipts.insert(
            "source".into(),
            json!({"tool":"file_read","output":{"file_version":{}}}),
        );
        assert!(validate_plan(&p, &claims, &owner, &receipts, Utc::now()).is_ok());
    }

    #[test]
    fn dream_legacy_plan_discards_retired_top_level_fields() {
        let legacy = json!({"review":{"quality":"kept", "evidence":"no change", "consolidation":"none"},"groups":[],"unresolved":["old note"]});
        let parsed: Plan = serde_json::from_value(legacy).unwrap();
        let saved = serde_json::to_value(parsed).unwrap();
        assert_eq!(saved.as_object().unwrap().len(), 2);
        assert!(saved.get("unresolved").is_none());
    }

    #[test]
    fn dream_high_claims_can_still_participate_in_quality_and_consolidation() {
        let (owner, mut first) = fixture();
        first.confidence = Confidence::High;
        let mut second = first.clone();
        second.id = "claim_22222222".parse().unwrap();
        second.statement = "Larger inputs have no verification result.".into();
        let claims = BTreeMap::from([
            (first.id.clone(), first.clone()),
            (second.id.clone(), second.clone()),
        ]);
        let mut p = plan(&first);
        assert!(validate_plan(&p, &claims, &owner, &BTreeMap::new(), Utc::now()).is_ok());
        p.groups[0].kind = Kind::Consolidation;
        p.groups[0].updates[0].status = ClaimStatus::Active;
        p.groups[0].updates[0].statement =
            "Only the observed input size is supported; larger inputs remain unverified.".into();
        p.groups[0]
            .updates
            .push(plan(&second).groups[0].updates[0].clone());
        p.groups[0].coverage = [&first, &second]
            .into_iter()
            .map(|claim| Coverage {
                input_id: claim.id.clone(),
                output_ids: vec![first.id.clone()],
                note: "Preserve observed and unverified size boundaries.".into(),
            })
            .collect();
        let groups = validate_plan(&p, &claims, &owner, &BTreeMap::new(), Utc::now()).unwrap();
        assert_eq!(groups[0].after[0].status, ClaimStatus::Active);
        assert_eq!(groups[0].after[1].status, ClaimStatus::Deprecated);
    }
    #[test]
    fn dream_confidence_increase_cannot_use_another_claims_receipt_in_a_quality_group() {
        let (owner, original) = fixture();
        let mut p = plan(&original);
        p.groups[0].updates[0].status = ClaimStatus::Active;
        p.groups[0].updates[0].confidence = Confidence::High;
        p.groups[0].updates[0].evidence_summary = "Observed independent restart test".into();
        p.groups[0].evidence_ids = vec!["file".into()];
        p.groups[0].change_basis = vec![ChangeBasis {
            claim_id: "claim_22222222".parse().unwrap(),
            removed_or_changed: "confidence".into(),
            added: String::new(),
            justification: "other claim".into(),
            evidence_ids: vec!["file".into()],
        }];
        let receipts = BTreeMap::from([(
            "file".into(),
            json!({"tool":"file_read","output":{"file_version":{}}}),
        )]);
        let readable = BTreeMap::from([(original.id.clone(), original.clone())]);
        assert!(validate_plan(&p, &readable, &owner, &receipts, Utc::now())
            .unwrap_err()
            .to_string()
            .contains("confidence increase"));
        p.groups[0].change_basis[0].claim_id = original.id;
        validate_plan(&p, &readable, &owner, &receipts, Utc::now()).unwrap();
    }
    #[test]
    fn dream_merge_back_to_source_preserves_lineage_without_self_edge() {
        let (owner, first) = fixture();
        let mut second = first.clone();
        second.id = "claim_22222222".parse().unwrap();
        let policy: SourceId = "policy_33333333".parse().unwrap();
        second.source_claim_ids = vec![SourceId::Claim(first.id.clone()), policy.clone()];
        let mut p = plan(&first);
        let group = &mut p.groups[0];
        group.kind = Kind::Consolidation;
        group.updates[0].status = ClaimStatus::Active;
        group.updates[0]
            .statement
            .push_str(" Preserve the second rule and its boundary.");
        group.updates[0].source_claim_ids = vec![policy.clone()];
        let mut deprecated = plan(&second).groups.remove(0).updates.remove(0);
        deprecated.source_claim_ids = second.source_claim_ids.clone();
        group.updates.push(deprecated);
        group.coverage = [&first, &second]
            .iter()
            .map(|c| Coverage {
                input_id: c.id.clone(),
                output_ids: vec![first.id.clone()],
                note: "Rule and evidence boundary retained in source claim".into(),
            })
            .collect();
        let readable = BTreeMap::from([
            (first.id.clone(), first.clone()),
            (second.id.clone(), second.clone()),
        ]);
        let prepared = validate_plan(&p, &readable, &owner, &BTreeMap::new(), Utc::now()).unwrap();
        assert_eq!(
            prepared[0].before[1].source_claim_ids,
            second.source_claim_ids
        );
        assert_eq!(prepared[0].after[0].source_claim_ids, vec![policy]);
        p.groups[0].updates[0].source_claim_ids.clear();
        assert!(
            validate_plan(&p, &readable, &owner, &BTreeMap::new(), Utc::now())
                .unwrap_err()
                .to_string()
                .contains("retain input source")
        );
        p.groups[0].updates[0].source_claim_ids = vec![SourceId::Claim(first.id)];
        assert!(
            validate_plan(&p, &readable, &owner, &BTreeMap::new(), Utc::now())
                .unwrap_err()
                .to_string()
                .contains("self-referential")
        );
    }

    #[test]
    fn dream_merge_cannot_lose_lineage_or_promote_weaker_input_without_evidence() {
        let (owner, mut first) = fixture();
        first.confidence = Confidence::High;
        let mut second = first.clone();
        second.id = "claim_22222222".parse().unwrap();
        second.confidence = Confidence::Low;
        second.source_claim_ids = vec![SourceId::Claim("claim_33333333".parse().unwrap())];
        let mut p = plan(&first);
        let group = &mut p.groups[0];
        group.kind = Kind::Consolidation;
        group.updates[0].status = ClaimStatus::Active;
        group.updates[0]
            .statement
            .push_str(" Include the weaker conditional observation.");
        group.updates.extend(plan(&second).groups.remove(0).updates);
        group.coverage = [&first, &second]
            .iter()
            .map(|c| Coverage {
                input_id: c.id.clone(),
                output_ids: vec![first.id.clone()],
                note: "retained".into(),
            })
            .collect();
        let readable = BTreeMap::from([
            (first.id.clone(), first),
            (second.id.clone(), second.clone()),
        ]);
        assert!(
            validate_plan(&p, &readable, &owner, &BTreeMap::new(), Utc::now())
                .unwrap_err()
                .to_string()
                .contains("retain input source")
        );
        p.groups[0].updates[0].source_claim_ids = second.source_claim_ids;
        assert!(
            validate_plan(&p, &readable, &owner, &BTreeMap::new(), Utc::now())
                .unwrap_err()
                .to_string()
                .contains("cannot inherit higher confidence")
        );
        p.groups[0].updates[0].confidence = Confidence::Low;
        validate_plan(&p, &readable, &owner, &BTreeMap::new(), Utc::now()).unwrap();
    }
}
