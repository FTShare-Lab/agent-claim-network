//! router 查询时的调试视图。
//!
//! 这里放只用于开发和观测的结构，不参与 claim_index 的持久化，也不应该被原样塞进
//! LLM prompt。正式结果仍由 `RouterQueryResult` 承载，debug 信息作为附加层存在。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievalDebug {
    pub mode: String,
    #[serde(default)]
    pub failed_paths: Vec<String>,
    #[serde(default)]
    pub error_summaries: Vec<String>,
    #[serde(default)]
    pub lexical_hits: usize,
    #[serde(default)]
    pub vector_hits: usize,
    #[serde(default)]
    pub rerank_fallback: bool,
    #[serde(default)]
    pub candidates: Vec<ClaimRetrievalDebug>,
}

/// 单条候选 claim 的检索调试信息。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRetrievalDebug {
    pub claim_id: String,
    pub hit_sources: String,
    #[serde(default)]
    pub lexical_score: usize,
    #[serde(default)]
    pub vector_score: usize,
    #[serde(default)]
    pub rank_before_rerank: usize,
    #[serde(default)]
    pub rank_after_rerank: usize,
    pub vector_status: String,
}
