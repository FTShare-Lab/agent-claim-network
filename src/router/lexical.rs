//! router lexical retrieval helpers。

use rustc_hash::FxHashSet;

use super::{traits::AgentQuery, RetrievalDocument};
use crate::claim::ClaimStatus;

pub(crate) fn query_match_score(
    agent_query: &AgentQuery,
    doc: &RetrievalDocument,
    claim_status: ClaimStatus,
) -> Option<usize> {
    let query_scope_segments = scope_word_segments(&agent_query.scope);
    let claim_scope_segments = scope_word_segments(&doc.scope_text);
    let scope_overlap = query_scope_segments
        .iter()
        .filter(|segment| claim_scope_segments.contains(*segment))
        .count();

    let task_term_hits = task_term_hits(agent_query.semantic_query.as_deref(), &doc.search_text);
    if scope_overlap == 0 && task_term_hits == 0 {
        return None;
    }

    let active_bonus = usize::from(matches!(claim_status, ClaimStatus::Active));
    Some(scope_overlap * 100 + task_term_hits * 10 + active_bonus)
}

fn scope_word_segments(scope: &str) -> FxHashSet<String> {
    scope
        .split(|c: char| !c.is_alphanumeric())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
        .collect()
}

fn task_term_hits(task_text: Option<&str>, search_text: &str) -> usize {
    let Some(task_text) = task_text else {
        return 0;
    };
    let task_terms = text_fragments(task_text);
    if task_terms.is_empty() {
        return 0;
    }
    let haystack = search_text.to_ascii_lowercase();
    task_terms
        .iter()
        .filter(|term| haystack.contains(term.as_str()))
        .count()
}

fn text_fragments(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if is_search_char(ch) {
            current.extend(ch.to_lowercase());
            continue;
        }
        push_fragment(&mut out, &mut current);
    }
    push_fragment(&mut out, &mut current);
    out
}

fn push_fragment(out: &mut Vec<String>, current: &mut String) {
    if current.chars().count() >= 2 && !out.contains(current) {
        out.push(current.clone());
    }
    current.clear();
}

fn is_search_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '\u{4E00}'..='\u{9FFF}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{AgentId, Claim, ClaimId, Confidence, DisputeId};

    fn sample_doc() -> RetrievalDocument {
        let claim = Claim {
            id: ClaimId::random(),
            name: "payment_timeout_root_cause".into(),
            statement: "payment timeout is caused by connection pool exhaustion".into(),
            scope: "order-system / payment-service / prod".into(),
            holder: AgentId::new("agent-b").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "timeout logs point to pool exhaustion".into(),
        };
        RetrievalDocument::from_claim(&claim, vec![DisputeId::random()], vec![])
    }

    #[test]
    fn query_match_score_hits_scope_and_task_terms() {
        let doc = sample_doc();
        let score = query_match_score(
            &AgentQuery::from_task("order-system", "investigate timeout"),
            &doc,
            ClaimStatus::Active,
        );
        assert!(score.is_some());
    }
}
