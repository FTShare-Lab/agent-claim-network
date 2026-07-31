//! AgentRunner 的 trace 写入和日志辅助。
//!
//! 本模块集中放置 trace id 派生、trace 文件写入和短文本日志截断，
//! 供 session finalize / compact recap 与 inbox 内化流程复用。

use chrono::{DateTime, Utc};

use super::runner::AgentRunner;
use crate::claim::{ClaimId, SourceId, Trace, TraceId};
use crate::time::truncate_to_second;

impl AgentRunner {
    pub(super) async fn write_trace(
        &self,
        name: String,
        task: String,
        input_claims: Vec<SourceId>,
        output_claims: Vec<ClaimId>,
        trace_id_time: DateTime<Utc>,
    ) -> anyhow::Result<TraceId> {
        let id = TraceId::from_trace_parts(trace_id_time, &name, &input_claims, &output_claims);
        self.write_trace_with_id(id, name, task, input_claims, output_claims, trace_id_time)
            .await
    }

    pub(super) async fn write_trace_with_id(
        &self,
        id: TraceId,
        name: String,
        task: String,
        input_claims: Vec<SourceId>,
        output_claims: Vec<ClaimId>,
        trace_id_time: DateTime<Utc>,
    ) -> anyhow::Result<TraceId> {
        let trace = Trace {
            id,
            name,
            task,
            agent: self.agent_id.clone(),
            input_claims,
            output_claims,
            created_at: truncate_to_second(trace_id_time),
        };
        let id = trace.id.clone();
        self.claim_store.write_trace(&trace).await?;
        Ok(id)
    }
}

pub(super) fn trace_name_from_task(task: &str) -> String {
    let line = task.lines().next().unwrap_or("").trim();
    let head: String = line.chars().take(32).collect();
    if head.is_empty() {
        "trace".into()
    } else {
        head
    }
}
