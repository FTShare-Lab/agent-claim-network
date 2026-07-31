//! router 模块。
//!
//! 对外暴露：
//! - `RouterClient` trait + 响应类型（`RouterQueryResult` / `CandidateClaim` / `DisputeRef`）
//! - `Router`：基于本地文件系统的实现，维护单文件 `derived_views.yaml` 并按 scope word segment 相关性查询
//! - `ClaimIndex` / `ClaimIndexEntry`：索引文件的内存表示
//!
//! 设计纪律：router 只读 / 派生写 `claim_index`，**不**修改 claim 文件本体或 dispute 文件本体；
//! dispute 状态唯一真相来自 `maintainer/disputes/`，Router 刷新时永远整体重算。

mod debug;
pub(crate) mod derived_views;
pub mod http_client;
mod index;
mod lexical;
mod overview;
mod rerank;
mod retrieval_doc;
pub mod server;
mod service;
pub mod traits;
mod vector;

pub use debug::{ClaimRetrievalDebug, RetrievalDebug};
pub use index::{ClaimIndex, ClaimIndexEntry};
pub use overview::{ScopeOverviewItem, ScopesOverviewSnapshot};
pub use rerank::{
    apply_rerank_order, build_reranker, default_reranker, RerankCandidate, RouterReranker,
};
pub use retrieval_doc::RetrievalDocument;
pub use service::{run_refresh_worker, RefreshOnQueryRouterClient, Router};
pub use traits::{
    AgentQuery, CandidateClaim, DisputeRef, RouterClient, RouterQueryResult, TimeoutRouterClient,
};
pub use vector::{
    ensure_vector_pending, load_vector_state, load_vector_state_opt, process_pending_queue,
    run_vector_worker, search_ready_vectors, search_text_hash, store_failed_vector_state,
    store_ready_vector_state, VectorHit, VectorProcessReport, VectorRetryPolicy, VectorState,
    VectorStatus,
};
