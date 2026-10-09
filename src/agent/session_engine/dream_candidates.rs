//! 本轮 Dream 已发现候选的顺序与处理结果；不修改 Claim，也不判断语义真伪。
use super::dream_plan::{Kind, OperationGroup};
use crate::claim::{AgentId, Claim, ClaimId, ClaimStatus, Confidence};
use crate::tool::ToolDefinition;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Candidate {
    pub(super) group_id: String,
    pub(super) claim_ids: Vec<ClaimId>,
    pub(super) kind: Kind,
    pub(super) reason: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Outcome {
    #[default]
    Pending,
    Executed {
        validation_id: String,
    },
    Kept {
        reason_kind: KeepReason,
        reason: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct Entry {
    candidate: Candidate,
    outcome: Outcome,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct Candidates {
    entries: Vec<Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Register {
    pub(super) candidates: Vec<Candidate>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum KeepReason {
    InsufficientEvidence,
    UsefulKnowledge,
    UncertainPreservation,
    NoConsolidationBenefit,
    ExternalChange,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Keep {
    pub(super) group_id: String,
    pub(super) reason_kind: KeepReason,
    pub(super) reason: String,
}

impl Candidates {
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn current(&self) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|e| matches!(e.outcome, Outcome::Pending))
    }

    pub(super) fn pending(&self) -> bool {
        self.current().is_some()
    }

    pub(super) fn current_id(&self) -> Option<&str> {
        self.current().map(|e| e.candidate.group_id.as_str())
    }

    pub(super) fn require_current(&self, id: &str) -> anyhow::Result<()> {
        let current = self
            .current()
            .ok_or_else(|| anyhow::anyhow!("No pending candidate {id}"))?;
        anyhow::ensure!(current.candidate.group_id == id,
            "Current candidate is {}; finish it with dream_apply_group or dream_keep_candidate before handling {id}. Other tasks are not a reason to abandon it.", current.candidate.group_id);
        Ok(())
    }

    /// 一批候选全部检查通过后才接纳，避免错误输入留下半批登记。
    pub(super) fn register(
        &mut self,
        candidates: Vec<Candidate>,
        readable: &BTreeMap<ClaimId, Claim>,
        owner: &AgentId,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!candidates.is_empty(), "candidates must not be empty");
        let mut next = self.clone();
        let mut ids = BTreeSet::new();
        for candidate in candidates {
            anyhow::ensure!(
                !candidate.group_id.trim().is_empty() && !candidate.reason.trim().is_empty(),
                "Candidate requires a group_id and specific reason"
            );
            anyhow::ensure!(
                ids.insert(candidate.group_id.clone()),
                "Duplicate candidate group_id in one request"
            );
            let claims = candidate.claim_ids.iter().cloned().collect::<BTreeSet<_>>();
            anyhow::ensure!(
                !claims.is_empty() && claims.len() == candidate.claim_ids.len(),
                "Candidate needs distinct claim_ids"
            );
            for id in &claims {
                let claim = readable.get(id).ok_or_else(|| {
                    anyhow::anyhow!("Candidate claim {id} has not been fully read")
                })?;
                anyhow::ensure!(
                    &claim.holder == owner && claim.status != ClaimStatus::Deprecated,
                    "Candidate cannot target foreign or deprecated claim {id}"
                );
                anyhow::ensure!(
                    candidate.kind != Kind::Evidence || claim.confidence != Confidence::High,
                    "B candidates require original medium/low confidence: {id}"
                );
            }
            for entry in next
                .entries
                .iter()
                .filter(|e| matches!(e.outcome, Outcome::Pending))
            {
                if entry.candidate.group_id == candidate.group_id {
                    anyhow::ensure!(entry.candidate.claim_ids.iter().all(|id| claims.contains(id)),
                        "Do not silently remove candidate claims; handle the existing candidate first");
                } else {
                    anyhow::ensure!(
                        !entry
                            .candidate
                            .claim_ids
                            .iter()
                            .any(|id| claims.contains(id)),
                        "Claim already belongs to pending candidate {}; combine its work there",
                        entry.candidate.group_id
                    );
                }
            }
            if let Some(entry) = next.entries.iter_mut().find(|e| {
                e.candidate.group_id == candidate.group_id && matches!(e.outcome, Outcome::Pending)
            }) {
                entry.candidate = candidate;
            } else {
                next.entries.push(Entry {
                    candidate,
                    outcome: Outcome::Pending,
                });
            }
        }
        *self = next;
        Ok(())
    }

    /// 兼容旧检查点及直接提交的完整草案；历史结果保留，同名新组可继续登记。
    pub(super) fn remember_group(&mut self, id: &str, group: &OperationGroup) {
        if !self
            .entries
            .iter()
            .any(|e| e.candidate.group_id == id && matches!(e.outcome, Outcome::Pending))
        {
            self.entries.push(Entry {
                candidate: Candidate {
                    group_id: id.into(),
                    claim_ids: group.updates.iter().map(|u| u.id.clone()).collect(),
                    kind: group.kind,
                    reason: group.reason.clone(),
                },
                outcome: Outcome::Pending,
            });
        }
    }

    pub(super) fn stage(&mut self, id: &str, group: &OperationGroup) -> anyhow::Result<()> {
        if self.pending() {
            self.require_current(id)?;
            let entry = self
                .current()
                .ok_or_else(|| anyhow::anyhow!("Current candidate disappeared"))?;
            let expected = entry.candidate.claim_ids.iter().collect::<BTreeSet<_>>();
            let actual = group.updates.iter().map(|u| &u.id).collect::<BTreeSet<_>>();
            anyhow::ensure!(expected == actual && entry.candidate.kind == group.kind,
                "Stage must handle the current candidate's exact claim_ids and kind; revise its registration explicitly without dropping claims, or keep it with a reason");
        } else {
            self.remember_group(id, group);
        }
        Ok(())
    }

    pub(super) fn applied(&mut self, id: &str, validation_id: &str) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|e| e.candidate.group_id == id && matches!(e.outcome, Outcome::Pending))
        {
            entry.outcome = Outcome::Executed {
                validation_id: validation_id.into(),
            };
        }
    }

    pub(super) fn keep(&mut self, keep: Keep) -> anyhow::Result<()> {
        self.require_current(&keep.group_id)?;
        anyhow::ensure!(!keep.reason.trim().is_empty(), "Explain the specific knowledge, evidence gap or safety reason for keeping this candidate; other tasks or priority are not reasons");
        let entry = self
            .entries
            .iter_mut()
            .find(|e| {
                e.candidate.group_id == keep.group_id && matches!(e.outcome, Outcome::Pending)
            })
            .ok_or_else(|| anyhow::anyhow!("Current candidate disappeared"))?;
        anyhow::ensure!(
            !matches!(keep.reason_kind, KeepReason::NoConsolidationBenefit)
                || entry.candidate.kind == Kind::Consolidation,
            "no_consolidation_benefit only applies to consolidation. Keeping means every Claim field stays unchanged. For a pure record requiring A cleanup, stage quality deprecate; otherwise explain its actual knowledge or safety reason. Candidate remains pending."
        );
        entry.outcome = Outcome::Kept {
            reason_kind: keep.reason_kind,
            reason: keep.reason,
        };
        Ok(())
    }

    pub(super) fn context(&self) -> Value {
        json!({
            "current":self.current().map(|e| &e.candidate),
            "pending":self.entries.iter().filter(|e| matches!(e.outcome, Outcome::Pending)).map(|e| &e.candidate).collect::<Vec<_>>(),
            "handled":self.entries.iter().filter(|e| !matches!(e.outcome, Outcome::Pending)).collect::<Vec<_>>(),
            "next":"Complete the current candidate with stage/validate/apply, or dream_keep_candidate with a concrete safety or no-change reason. group:null only removes a draft, not its pending candidate. Then handle the next candidate. No modification quota."
        })
    }
}

pub(super) fn definitions() -> Vec<ToolDefinition> {
    let string = json!({"type":"string","minLength":1});
    vec![
        ToolDefinition {
            name: "dream_record_candidates".into(),
            description: "Record already identified changes or concrete investigation questions immediately, in the order you want to handle them. First pending candidate is current; finish it before the next. Use the SAME group_id when staging its plan. Can add candidates without preparing the whole run. A ready full stage also registers itself. Re-registering a pending group refines it but cannot silently remove claim_ids. No Claim writes; no obligation to modify if review finds it unsafe. Do not create a candidate for every unchanged claim.".into(),
            input_schema: json!({"type":"object","properties":{"candidates":{"type":"array","minItems":1,"items":{"type":"object","properties":{"group_id":string,"claim_ids":{"type":"array","minItems":1,"uniqueItems":true,"items":string},"kind":{"type":"string","enum":["quality","evidence","consolidation"]},"reason":string},"required":["group_id","claim_ids","kind","reason"],"additionalProperties":false}}},"required":["candidates"],"additionalProperties":false}),
        },
        ToolDefinition {
            name: "dream_keep_candidate".into(),
            description: "Keep ALL fields of the current candidate's claims unchanged after a specific safety or no-change judgment. no_consolidation_benefit is only for consolidation. A pure record judged suitable for deprecation needs stage/validate/apply, not keep. Explain useful knowledge, evidence gap, uncertainty or external change; tool argument errors and other tasks are not safety reasons. Removes the unexecuted draft and records the decision, then exposes the next candidate. Cannot undo applied changes or bypass pending execution recovery.".into(),
            input_schema: json!({"type":"object","properties":{"group_id":string,"reason_kind":{"type":"string","enum":["insufficient_evidence","useful_knowledge","uncertain_preservation","no_consolidation_benefit","external_change"]},"reason":string},"required":["group_id","reason_kind","reason"],"additionalProperties":false}),
        },
    ]
}
