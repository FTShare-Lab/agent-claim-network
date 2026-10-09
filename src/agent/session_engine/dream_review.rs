//! 同一 Dream 上下文的双向内容复核协议；校验引用和版本，不声称证明语义正确。
use super::claim_context::content_hash;
use super::dream_draft_feedback;
use super::dream_plan::{check_factual_correction_eligible, Kind, OperationGroup, Plan};
use crate::claim::{Claim, ClaimId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const REVIEW_FIELDS: [&str; 4] = ["statement", "scope", "evidence_summary", "name"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Quote {
    pub claim_id: ClaimId,
    pub field: String,
    pub quote: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EvidenceQuote {
    pub evidence_id: String,
    pub quote: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Preservation {
    pub before: Quote,
    pub after: Vec<Quote>,
    // preserved / episodic / corrected。后两项必须明确解释为何可以移除。
    pub disposition: Disposition,
    pub reason: String,
    pub evidence: Vec<EvidenceQuote>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Disposition {
    Preserved,
    Episodic,
    Corrected,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Support {
    pub after: Quote,
    pub before: Vec<Quote>,
    pub evidence: Vec<EvidenceQuote>,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelfReview {
    pub group_id: String,
    pub validation_id: String,
    pub preservation: Vec<Preservation>,
    pub support: Vec<Support>,
    pub scope_and_certainty: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Validation {
    pub validation_id: String,
    pub input: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ReviewRecord {
    pub validation: Validation,
    pub review: SelfReview,
}

impl Validation {
    pub(super) fn new(input: Value) -> anyhow::Result<Self> {
        Ok(Self {
            validation_id: content_hash(&input)?,
            input,
        })
    }
}
pub(super) fn review_input(
    id: &str,
    group: &OperationGroup,
    readable: &BTreeMap<ClaimId, Claim>,
    receipts: &BTreeMap<String, Value>,
) -> Value {
    let operations = dream_draft_feedback::group_input(id, group, readable);
    json!({"group_id":id,"kind":group.kind,"reason":group.reason,
        "before":group.updates.iter().filter_map(|u| readable.get(&u.id)).collect::<Vec<_>>(),
        "after":group.updates,"changes":operations["group"]["operations"],
        "coverage":group.coverage,"change_basis":group.change_basis,
        "unversioned_evidence_omitted":group.evidence_ids.iter().filter(|id| receipts.get(*id).is_some_and(|r| r.pointer("/output/file_version").is_none())).collect::<Vec<_>>(),
        "evidence":group.evidence_ids.iter().filter_map(|id| receipts.get(id).filter(|r| r.pointer("/output/file_version").is_some()).map(|r| (id, r))).collect::<BTreeMap<_,_>>()})
}

fn changed_ids(input: &Value) -> BTreeSet<ClaimId> {
    input["changes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|op| op["action"] != "keep")
        .filter_map(|op| serde_json::from_value(op["id"].clone()).ok())
        .collect()
}

pub(super) fn basis_error(group: &OperationGroup, input: &Value) -> Option<String> {
    let changed = changed_ids(input);
    let explained: BTreeSet<_> = group
        .change_basis
        .iter()
        .map(|b| b.claim_id.clone())
        .collect();
    if explained != changed || group.change_basis.len() != changed.len() {
        return Some(format!("change_basis must describe every changed/deprecated claim exactly once, excluding keep. Changed IDs: {changed:?}; missing entries: {:?}; unexpected/keep entries: {:?}; duplicate entries: {}. Repair parameters without dropping intended knowledge: each entry needs claim_id, removed_or_changed, added (empty if none), justification and evidence_ids.", changed.difference(&explained).collect::<Vec<_>>(), explained.difference(&changed).collect::<Vec<_>>(), group.change_basis.len() != explained.len()));
    }
    for basis in &group.change_basis {
        if basis.removed_or_changed.trim().is_empty() || basis.justification.trim().is_empty() {
            return Some(format!(
                "change_basis for {} requires non-empty removed_or_changed and justification",
                basis.claim_id
            ));
        }
        let missing: Vec<_> = basis
            .evidence_ids
            .iter()
            .filter(|id| !group.evidence_ids.contains(id))
            .collect();
        if !missing.is_empty() {
            return Some(format!("change_basis for {} references evidence {missing:?} outside group.evidence_ids {:?}; cite an existing group receipt or add the actual receipt to the group", basis.claim_id, group.evidence_ids));
        }
    }
    None
}

fn quoted<'a>(input: &'a Value, side: &str, quote: &Quote) -> anyhow::Result<&'a str> {
    anyhow::ensure!(
        REVIEW_FIELDS.contains(&quote.field.as_str()),
        "Review quotes must identify name, statement, scope or evidence_summary"
    );
    let claim = input[side]
        .as_array()
        .and_then(|claims| claims.iter().find(|c| c["id"] == json!(quote.claim_id)))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Review quote references unknown {side} Claim {}",
                quote.claim_id
            )
        })?;
    anyhow::ensure!(
        side != "after" || claim["status"] != "deprecated",
        "A deprecated Claim cannot receive preserved information or support output assertions"
    );
    let text = claim[&quote.field].as_str().unwrap_or_default();
    anyhow::ensure!(!quote.quote.trim().is_empty() && text.contains(&quote.quote), "Review quote for {side} {}.{} must be a verbatim non-empty substring; actual field: {text}", quote.claim_id, quote.field);
    Ok(text)
}

fn evidence_text(receipt: &Value) -> &str {
    receipt["output"]["content"]
        .as_str()
        .or_else(|| receipt["output"]["text"].as_str())
        .unwrap_or_default()
}

fn evidence_body(receipt: &Value) -> String {
    let text = evidence_text(receipt);
    // 仅移除 file_read 确实添加的连续行号；正文自身的数字竖线和缩进保持原样。
    let page = &receipt["output"]["page"];
    let start = page["returned_start"].as_u64();
    let end = page["returned_end"].as_u64();
    if receipt["input"]["show_linenos"] == false || start.is_none() || end.is_none() {
        return text.into();
    }
    let mut next = start;
    let mut last = None;
    let mut body = String::new();
    for line in text.split_inclusive('\n') {
        let Some(number) = next else {
            return text.into();
        };
        let Some(raw) = line.strip_prefix(&format!("{number}|")) else {
            return text.into();
        };
        body.push_str(raw);
        last = Some(number);
        next = number.checked_add(1);
    }
    if last == end {
        body
    } else {
        text.into()
    }
}

pub(super) fn evidence_feedback(input: &Value, review: &SelfReview) -> Value {
    let mut errors = Vec::new();
    let mut previews = BTreeMap::new();
    let items = review
        .preservation
        .iter()
        .enumerate()
        .map(|(i, p)| ("preservation", i, &p.evidence))
        .chain(
            review
                .support
                .iter()
                .enumerate()
                .map(|(i, s)| ("support", i, &s.evidence)),
        );
    for (section, item_index, evidence) in items {
        for (evidence_index, cited) in evidence.iter().enumerate() {
            let receipt = &input["evidence"][&cited.evidence_id];
            let body = evidence_body(receipt);
            let reason = if receipt["tool"] != "file_read"
                || receipt.pointer("/output/file_version").is_none()
            {
                "Must cite a versioned file_read receipt in this group. If needed, add the actual receipt via dream_stage_group, then validate again."
            } else if cited.quote.trim().is_empty()
                || !(evidence_text(receipt).contains(&cited.quote)
                    || body.contains(cited.quote.trim()))
            {
                "Quote is absent from this file page. Copy a contiguous body excerpt; do not change internal whitespace, join separate spans, or use another file's text."
            } else {
                continue;
            };
            errors.push(
                json!({"section":section,"item_index":item_index,"evidence_index":evidence_index,
                "evidence_id":cited.evidence_id,"quote":cited.quote,"reason":reason}),
            );
            if receipt.is_object() {
                // 每份错误来源只展示一次有限正文；完整原文仍由 file_read 分页读取。
                previews
                    .entry(cited.evidence_id.clone())
                    .or_insert_with(|| {
                        json!({
                            "path":receipt["input"]["path"],"page":receipt["output"]["page"],
                            "body_preview":body.chars().take(1600).collect::<String>(),
                            "preview_truncated":body.chars().count() > 1600
                        })
                    });
            }
        }
    }
    json!({"evidence_errors":errors,"evidence_previews":previews})
}

// 只忽略句间分隔符；保留否定、数字、比较/算术符号等可能改变语义的字符。
fn separator(c: char) -> bool {
    c.is_whitespace() || matches!(c, '，' | '。' | '；' | '：' | '、' | ',' | ';' | ':')
}

#[derive(Debug, Serialize)]
pub(super) struct CoverageGap {
    side: String,
    claim_id: Value,
    field: String,
    missing_fragments: Vec<String>,
}

pub(super) fn coverage_gaps(validation: &Validation, review: &SelfReview) -> Vec<CoverageGap> {
    let mut gaps = Vec::new();
    for (side, quotes) in [
        (
            "before",
            review
                .preservation
                .iter()
                .map(|i| &i.before)
                .collect::<Vec<_>>(),
        ),
        (
            "after",
            review.support.iter().map(|i| &i.after).collect::<Vec<_>>(),
        ),
    ] {
        for claim in validation.input[side].as_array().into_iter().flatten() {
            if side == "after" && claim["status"] == "deprecated" {
                continue;
            }
            for field in REVIEW_FIELDS {
                let text = claim[field].as_str().unwrap_or_default();
                let mut covered = vec![false; text.len()];
                for quote in quotes.iter().filter(|q| {
                    json!(q.claim_id) == claim["id"] && q.field == field && !q.quote.is_empty()
                }) {
                    for (offset, matched) in text.match_indices(&quote.quote) {
                        covered[offset..offset + matched.len()].fill(true);
                    }
                }
                let mut missing = Vec::new();
                let mut fragment = String::new();
                for (offset, c) in text.char_indices() {
                    if !covered[offset] && !separator(c) {
                        fragment.push(c);
                    } else if !fragment.is_empty() {
                        missing.push(std::mem::take(&mut fragment));
                    }
                }
                if !fragment.is_empty() {
                    missing.push(fragment);
                }
                if !missing.is_empty() {
                    gaps.push(CoverageGap {
                        side: side.into(),
                        claim_id: claim["id"].clone(),
                        field: field.into(),
                        missing_fragments: missing,
                    });
                }
            }
        }
    }
    gaps
}

pub(super) fn validate_review(validation: &Validation, review: &SelfReview) -> anyhow::Result<()> {
    let input = &validation.input;
    anyhow::ensure!(review.validation_id == validation.validation_id && input["group_id"] == review.group_id, "Self-review is for a different draft version; call dream_validate and review the current group");
    anyhow::ensure!(
        !review.scope_and_certainty.trim().is_empty(),
        "Explain name/body consistency, scope, certainty and source boundaries in self-review"
    );
    let feedback = evidence_feedback(input, review);
    anyhow::ensure!(feedback["evidence_errors"].as_array().is_some_and(Vec::is_empty),
        "Review contains invalid evidence quotes; inspect all evidence_errors and evidence_previews in review_feedback");
    for item in &review.preservation {
        quoted(input, "before", &item.before)?;
        anyhow::ensure!(
            !item.reason.trim().is_empty(),
            "Explain each information destination or removal"
        );
        for quote in &item.after {
            quoted(input, "after", quote)?;
        }
        match item.disposition {
            Disposition::Preserved => anyhow::ensure!(!item.after.is_empty(), "Preserved information needs a surviving output quote"),
            Disposition::Episodic => anyhow::ensure!(input["kind"] != json!(Kind::Evidence) && item.after.is_empty() && item.evidence.is_empty(), "Only A cleanup (including episodic fragments within C) may remove purely episodic material without new evidence; reusable rules must have an output"),
            Disposition::Corrected => {
                anyhow::ensure!(!item.evidence.is_empty(), "Removing/changing a factual rule requires new direct evidence; absent verification is not counterevidence");
                let original = input["before"].as_array().and_then(|v| v.iter().find(|c| c["id"] == json!(item.before.claim_id)));
                anyhow::ensure!(original.is_some(), "Missing original Claim {} for factual correction", item.before.claim_id);
                check_factual_correction_eligible(&item.before.claim_id, original.is_some_and(|c| c["confidence"] == "high"))?;
                let basis = input["change_basis"].as_array().and_then(|v| v.iter().find(|b| b["claim_id"] == json!(item.before.claim_id)));
                anyhow::ensure!(basis.is_some_and(|b| item.evidence.iter().all(|e| b["evidence_ids"].as_array().is_some_and(|ids| ids.contains(&json!(e.evidence_id))))), "Corrected information must cite this Claim's change_basis evidence");
                let target = input["after"].as_array().and_then(|v| v.iter().find(|c| c["id"] == json!(item.before.claim_id)));
                anyhow::ensure!(target.zip(original).is_some_and(|(a,b)| a["evidence_summary"] != b["evidence_summary"]), "Factual correction requires a new evidence_summary on the corrected Claim");
            }
        }
    }
    for item in &review.support {
        quoted(input, "after", &item.after)?;
        anyhow::ensure!(!item.reason.trim().is_empty() && (!item.before.is_empty() || !item.evidence.is_empty()), "Every output assertion needs a concrete original quote or direct evidence and explanation");
        for quote in &item.before {
            quoted(input, "before", quote)?;
        }
    }
    let gaps = coverage_gaps(validation, review);
    if let Some(first) = gaps.first() {
        anyhow::bail!("Review must cover all {} fields, including conditions, alternatives and evidence limits; missing fragments: {}", first.side, serde_json::to_string(&gaps)?);
    }
    Ok(())
}

pub(super) fn validate_approvals(
    plan: &Plan,
    readable: &BTreeMap<ClaimId, Claim>,
    receipts: &BTreeMap<String, Value>,
    approvals: &[ReviewRecord],
) -> anyhow::Result<()> {
    for group in &plan.groups {
        let input = review_input("", group, readable, receipts);
        if changed_ids(&input).is_empty() {
            continue;
        }
        let record = approvals.iter().find(|record| {
            let mut reviewed = record.validation.input.clone();
            reviewed["group_id"] = json!("");
            reviewed == input
        }).ok_or_else(|| anyhow::anyhow!("Dream changes or inputs no longer match a validated self-review; no changes committed"))?;
        validate_review(&record.validation, &record.review)?;
    }
    Ok(())
}

pub(super) fn review_schema() -> Value {
    let s = json!({"type":"string"});
    let quote = json!({"type":"object","properties":{"claim_id":s,"field":{"type":"string","enum":REVIEW_FIELDS},"quote":s},"required":["claim_id","field","quote"],"additionalProperties":false});
    let quotes = json!({"type":"array","items":quote});
    let evidence = json!({"type":"array","items":{"type":"object","properties":{"evidence_id":s,"quote":s},"required":["evidence_id","quote"],"additionalProperties":false}});
    json!({"type":"array","items":{"type":"object","properties":{
        "group_id":s,"validation_id":s,"scope_and_certainty":{"type":"string","description":"Explain name/body consistency, applicability, inherited sources and certainty limits."},
        "preservation":{"type":"array","items":{"type":"object","properties":{"before":quote,"after":quotes,"disposition":{"type":"string","enum":["preserved","episodic","corrected"]},"reason":s,"evidence":evidence},"required":["before","after","disposition","reason","evidence"],"additionalProperties":false}},
        "support":{"type":"array","items":{"type":"object","properties":{"after":quote,"before":quotes,"evidence":evidence,"reason":s},"required":["after","before","evidence","reason"],"additionalProperties":false}}
    },"required":["group_id","validation_id","preservation","support","scope_and_certainty"],"additionalProperties":false}})
}

#[cfg(test)]
#[path = "dream_review_tests.rs"]
mod tests;

// 测试 adapter 的结构化回包生成器，仅用于协议/恢复测试，不评估语义等价。
#[cfg(test)]
pub(super) fn fixture_review(validation: &Validation) -> SelfReview {
    let input = &validation.input;
    let mut preservation = Vec::new();
    let mut support = Vec::new();
    let before = input["before"].as_array().unwrap();
    let after = input["after"].as_array().unwrap();
    let quote = |claim: &Value, field: &str| Quote {
        claim_id: serde_json::from_value(claim["id"].clone()).unwrap(),
        field: field.into(),
        quote: claim[field].as_str().unwrap().into(),
    };
    for original in before {
        let target = after
            .iter()
            .find(|c| c["id"] == original["id"] && c["status"] != "deprecated")
            .or_else(|| after.iter().find(|c| c["status"] != "deprecated"));
        for field in REVIEW_FIELDS {
            preservation.push(Preservation {
                before: quote(original, field),
                after: target.map(|c| vec![quote(c, field)]).unwrap_or_default(),
                disposition: if target.is_some() {
                    Disposition::Preserved
                } else {
                    Disposition::Episodic
                },
                reason: "Fixture mapping for protocol tests".into(),
                evidence: vec![],
            });
        }
    }
    for target in after.iter().filter(|c| c["status"] != "deprecated") {
        for field in REVIEW_FIELDS {
            support.push(Support {
                after: quote(target, field),
                before: before.iter().map(|c| quote(c, field)).collect(),
                evidence: vec![],
                reason: "Fixture source for protocol tests".into(),
            });
        }
    }
    SelfReview {
        group_id: input["group_id"].as_str().unwrap().into(),
        validation_id: validation.validation_id.clone(),
        preservation,
        support,
        scope_and_certainty: "Fixture keeps the asserted boundary".into(),
    }
}
