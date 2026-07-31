//! Trace 实体：记录一次任务中 input source → output claim 的产出关系。
//! Trace 不区分 borrowed / internalized。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::id::{AgentId, ClaimId, SourceId, TraceId};
use crate::time::serde_utc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    pub id: TraceId,
    pub name: String,
    pub task: String,
    pub agent: AgentId,
    #[serde(default)]
    pub input_claims: Vec<SourceId>,
    #[serde(default)]
    pub output_claims: Vec<ClaimId>,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::PolicyId;

    #[test]
    fn trace_yaml_round_trip() {
        let policy_id = PolicyId::random();
        let t = Trace {
            id: TraceId::random(),
            name: "batch_retry_design".into(),
            task: "设计批量订单重试策略".into(),
            agent: AgentId::new("agent-c").unwrap(),
            input_claims: vec![
                SourceId::Claim(ClaimId::random()),
                SourceId::Policy(policy_id.clone()),
            ],
            output_claims: vec![ClaimId::random()],
            created_at: "2026-04-12T08:00:00Z".parse().unwrap(),
        };
        let yaml = serde_yaml_ng::to_string(&t).unwrap();
        assert!(!yaml.contains("input_policies:"));
        assert!(yaml.contains(policy_id.as_str()));
        let back: Trace = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn trace_yaml_input_claims_accept_policy_source() {
        let policy_id = PolicyId::random();
        let yaml = format!(
            "\
id: {}
name: legacy
task: legacy task
agent: agent-c
input_claims:
  - {}
output_claims: []
created_at: 2026-04-12T08:00:00Z
",
            TraceId::random(),
            policy_id
        );
        let back: Trace = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(back.input_claims, vec![SourceId::Policy(policy_id)]);
    }
}
