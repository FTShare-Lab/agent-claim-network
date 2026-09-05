//! `Router`：单文件派生快照维护 + scope 查询。
//!
//! - **派生快照刷新**：一次刷新构造并原子替换 `derived_views.yaml`
//!   - dispute 关联整体重算，永远不做增量 patch
//! - **query**（`RouterClient` 实现）：默认读最近派生快照，缺失时兜底刷新
//!   → 附带每条 claim 的 dispute id 列表
//!   - 默认不召回 deprecated
//!   - 仅当 dispute 至少 2 条 claim 都被本次查询命中时，才把 dispute 摘要塞进结果
//!     （避免把对侧 claim 隐式拽进上下文制造噪声）

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::fs;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::derived_views::{self, DerivedViewsRead, RouterDerivedViewsSnapshot};
use super::index::{ClaimIndex, ClaimIndexEntry};
use super::lexical;
use super::overview::{ScopeOverviewItem, ScopesOverviewSnapshot};
use super::rerank;
use super::traits::{AgentQuery, CandidateClaim, DisputeRef, RouterClient, RouterQueryResult};
use super::vector;
use super::RetrievalDocument;
use crate::api::EmbeddingClient;
use crate::claim::{Claim, ClaimId, ClaimStatus, Dispute, DisputeId, DisputeStatus};
use crate::config::RouterRetrievalConfig;
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard};

pub struct Router {
    team_root: PathBuf,
    hybrid: RouterRetrievalConfig,
    embedding_client: Option<Arc<dyn EmbeddingClient>>,
    reranker: Arc<dyn rerank::RouterReranker>,
    refresh_lock: Mutex<()>,
}

/// Claim 镜像连续换代时，最多重读一次；超过后宁可跳过本次候选，也不能混用正文与 Vector。
const RETRIEVAL_SOURCE_SNAPSHOT_ATTEMPTS: usize = 2;

#[derive(Debug, Clone)]
struct ScannedClaim {
    claim: Claim,
    rel_path: String,
}

#[derive(Debug, Default)]
struct ScopeAccumulator {
    active_claims: usize,
    stale_claims: usize,
    latest_claim_created_at: Option<DateTime<Utc>>,
    open_dispute_ids: FxHashSet<DisputeId>,
    resolved_dispute_ids: FxHashSet<DisputeId>,
}

pub struct RefreshOnQueryRouterClient {
    inner: Arc<Router>,
}

impl RefreshOnQueryRouterClient {
    pub fn new(inner: Arc<Router>) -> Self {
        Self { inner }
    }
}

impl Router {
    pub fn new(team_root: PathBuf) -> Self {
        Self::with_dependencies(
            team_root,
            RouterRetrievalConfig::default(),
            None,
            rerank::default_reranker(),
        )
    }

    pub fn with_hybrid_config(team_root: PathBuf, hybrid: RouterRetrievalConfig) -> Self {
        Self::with_dependencies(team_root, hybrid, None, rerank::default_reranker())
    }

    pub fn with_dependencies(
        team_root: PathBuf,
        hybrid: RouterRetrievalConfig,
        embedding_client: Option<Arc<dyn EmbeddingClient>>,
        reranker: Arc<dyn rerank::RouterReranker>,
    ) -> Self {
        Self {
            team_root,
            hybrid,
            embedding_client,
            reranker,
            refresh_lock: Mutex::new(()),
        }
    }

    pub fn team_root(&self) -> &std::path::Path {
        &self.team_root
    }

    /// 全量刷新 Router 派生快照，并以一个文件作为唯一发布点。
    pub async fn refresh_derived_views(&self) -> anyhow::Result<()> {
        let _guard = self.refresh_lock.lock().await;
        let lock_path = paths::team_store_router_derived_views_lock_path(&self.team_root);
        let _file_guard = FileLockGuard::lock_exclusive(&lock_path).await?;
        // 定时 worker 也会直接调用本函数，因此必须在发布前拒绝覆盖未来 schema。
        // Missing / 已知损坏的派生数据仍可由下面的权威扫描安全重建。
        let path = paths::team_store_router_derived_views_path(&self.team_root);
        let _ = derived_views::read_derived_views(&path).await?;
        let scanned_claims = self.scan_claims().await?;
        let dispute_map = self.scan_disputes_grouped_by_claim().await?;
        let overview = build_scopes_overview(&scanned_claims, &dispute_map);

        let mut claim_paths = Vec::with_capacity(scanned_claims.len());
        for scanned in &scanned_claims {
            claim_paths.push((scanned.claim.id.clone(), scanned.rel_path.clone()));
        }
        let mut entries = Vec::with_capacity(claim_paths.len());
        for (id, rel_path) in claim_paths {
            let (open, resolved) = dispute_map.get(&id).cloned().unwrap_or_default();
            entries.push(ClaimIndexEntry {
                id,
                path: rel_path,
                open_dispute_ids: open,
                resolved_dispute_ids: resolved,
            });
        }
        entries.sort_by(|lhs, rhs| lhs.path.cmp(&rhs.path));
        let snapshot = RouterDerivedViewsSnapshot::new(ClaimIndex(entries), overview);
        write_yaml_atomic(&path, &snapshot).await?;
        Ok(())
    }

    /// 读取完整 bundle；缺失或可重建损坏时，先从权威数据全量刷新一次。
    async fn load_derived_views(&self) -> anyhow::Result<RouterDerivedViewsSnapshot> {
        let path = paths::team_store_router_derived_views_path(&self.team_root);
        match derived_views::read_derived_views(&path).await? {
            DerivedViewsRead::Current(snapshot) => Ok(snapshot),
            DerivedViewsRead::Missing | DerivedViewsRead::RecoverableCorrupt => {
                self.refresh_derived_views().await?;
                match derived_views::read_derived_views(&path).await? {
                    DerivedViewsRead::Current(snapshot) => Ok(snapshot),
                    DerivedViewsRead::Missing => {
                        anyhow::bail!("Router 派生快照刷新后仍不存在: {path:?}")
                    }
                    DerivedViewsRead::RecoverableCorrupt => {
                        anyhow::bail!("Router 派生快照刷新后仍无法解析: {path:?}")
                    }
                }
            }
        }
    }

    pub async fn load_scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        Ok(self.load_derived_views().await?.scopes_overview().clone())
    }

    /// 扫所有 agent 镜像目录，返回 claim + team_root 相对路径。
    async fn scan_claims(&self) -> anyhow::Result<Vec<ScannedClaim>> {
        let agents_root = paths::team_store_agents_root(&self.team_root);
        if !fs::try_exists(&agents_root).await.unwrap_or(false) {
            return Ok(vec![]);
        }
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&agents_root).await?;
        while let Some(agent_entry) = rd.next_entry().await? {
            let agent_dir = agent_entry.path();
            let ft = agent_entry.file_type().await?;
            if !ft.is_dir() {
                continue;
            }
            let claims_dir = agent_dir.join("claims");
            if !fs::try_exists(&claims_dir).await.unwrap_or(false) {
                continue;
            }
            let mut claim_rd = fs::read_dir(&claims_dir).await?;
            while let Some(claim_entry) = claim_rd.next_entry().await? {
                let claim_path = claim_entry.path();
                let Some(name) = claim_path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.ends_with(".yaml") || name.contains(".tmp.") {
                    continue;
                }
                let claim: Claim = read_yaml(&claim_path).await?;
                let rel = claim_path
                    .strip_prefix(&self.team_root)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| claim_path.to_string_lossy().into_owned());
                out.push(ScannedClaim {
                    claim,
                    rel_path: rel,
                });
            }
        }
        Ok(out)
    }

    /// 扫 `maintainer/disputes/`，按 claim_id 反向汇总：claim_id → (open_ids, resolved_ids)
    async fn scan_disputes_grouped_by_claim(
        &self,
    ) -> anyhow::Result<FxHashMap<ClaimId, (Vec<DisputeId>, Vec<DisputeId>)>> {
        let dir = paths::team_store_disputes_dir(&self.team_root);
        let mut map: FxHashMap<ClaimId, (Vec<DisputeId>, Vec<DisputeId>)> = FxHashMap::default();
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            return Ok(map);
        }
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let p = entry.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.ends_with(".yaml") || name.contains(".tmp.") {
                continue;
            }
            let dispute: Dispute = read_yaml(&p).await?;
            for cid in &dispute.claims {
                let bucket = map.entry(cid.clone()).or_default();
                match dispute.status {
                    DisputeStatus::Open => bucket.0.push(dispute.id.clone()),
                    DisputeStatus::Resolved => bucket.1.push(dispute.id.clone()),
                }
            }
        }
        for (open, resolved) in map.values_mut() {
            open.sort();
            resolved.sort();
        }
        Ok(map)
    }

    /// 读取所有 dispute 文件（用于查询时附带 disputes 摘要）。
    async fn load_all_disputes(&self) -> anyhow::Result<Vec<Dispute>> {
        let dir = paths::team_store_disputes_dir(&self.team_root);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            return Ok(vec![]);
        }
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let p = entry.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.ends_with(".yaml") || name.contains(".tmp.") {
                continue;
            }
            let d: Dispute = read_yaml(&p).await?;
            out.push(d);
        }
        Ok(out)
    }

    /// 确认权威 Claim 快照后，在单个 claim 协调边界内发布检索 target。
    async fn ensure_retrieval_target(
        &self,
        entry: &ClaimIndexEntry,
        embedding_fingerprint: Option<&crate::api::EmbeddingCacheFingerprint>,
    ) -> anyhow::Result<Option<(Claim, RetrievalDocument, vector::RetrievalTargetEnsure)>> {
        let path = self.team_root.join(&entry.path);
        for _ in 0..RETRIEVAL_SOURCE_SNAPSHOT_ATTEMPTS {
            let claim: Claim = read_yaml(&path).await?;
            if claim.id != entry.id {
                anyhow::bail!(
                    "Router index 与 Claim 镜像 id 不一致: index={} mirror={} path={path:?}",
                    entry.id,
                    claim.id
                );
            }
            // 默认 active 查询不返回 deprecated（active + stale 仍可见，stale 在 status 上自暴）
            if claim.status == ClaimStatus::Deprecated {
                return Ok(None);
            }
            let retrieval_doc = RetrievalDocument::from_claim(
                &claim,
                entry.open_dispute_ids.clone(),
                entry.resolved_dispute_ids.clone(),
            );
            if let Some(target) = vector::ensure_retrieval_target_for_claim_snapshot(
                &self.team_root,
                &path,
                &claim,
                &retrieval_doc,
                embedding_fingerprint,
            )
            .await?
            {
                return Ok(Some((claim, retrieval_doc, target)));
            }
        }
        log::debug!(
            target: "router",
            "Claim 镜像在建立 retrieval target 期间连续换代，跳过本次候选 claim_id={}",
            entry.id
        );
        Ok(None)
    }
}

#[async_trait]
impl RouterClient for Router {
    async fn query(&self, agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        #[derive(Clone)]
        struct CandidateEnvelope {
            candidate: CandidateClaim,
            lexical_score: usize,
            vector_score: usize,
            hit_sources: String,
            vector_status: String,
        }

        let idx = self.load_derived_views().await?.claim_index().clone();

        let mut candidate_ids: FxHashSet<ClaimId> = FxHashSet::default();
        let mut retrieval_debug = crate::router::RetrievalDebug {
            mode: "lexical_only".into(),
            ..Default::default()
        };
        let mut candidates_by_id: FxHashMap<ClaimId, CandidateClaim> = FxHashMap::default();
        let mut lexical_scores: FxHashMap<ClaimId, usize> = FxHashMap::default();
        let mut vector_states: FxHashMap<ClaimId, vector::VectorState> = FxHashMap::default();
        let mut expected_vector_content_hashes: FxHashMap<ClaimId, String> = FxHashMap::default();
        let embedding_fingerprint = self
            .embedding_client
            .as_ref()
            .map(|client| client.cache_fingerprint());
        for entry in idx.entries() {
            let target_fingerprint = self
                .hybrid
                .enabled
                .then_some(embedding_fingerprint.as_ref())
                .flatten();
            let Some((claim, retrieval_doc, vector_target)) = self
                .ensure_retrieval_target(entry, target_fingerprint)
                .await?
            else {
                continue;
            };
            if let Some(error) = vector_target.vector_error {
                retrieval_debug.failed_paths.push("vector".into());
                retrieval_debug.error_summaries.push(error.to_string());
            }
            if let Some(state) = vector_target.vector_state {
                // ensure 已用这份 retrieval_doc 计算并核对 target；复用结果避免正文二次 hash。
                expected_vector_content_hashes.insert(claim.id.clone(), state.content_hash.clone());
                if state.status == vector::VectorStatus::Failed {
                    retrieval_debug.failed_paths.push("vector".into());
                    if let Some(summary) = state.error_summary.as_ref() {
                        retrieval_debug.error_summaries.push(summary.clone());
                    }
                }
                vector_states.insert(claim.id.clone(), state);
            }
            if let Some(score) =
                lexical::query_match_score(agent_query, &retrieval_doc, claim.status)
            {
                lexical_scores.insert(claim.id.clone(), score);
            }
            candidates_by_id.insert(
                claim.id.clone(),
                CandidateClaim {
                    claim,
                    open_dispute_ids: retrieval_doc.open_dispute_ids,
                    resolved_dispute_ids: retrieval_doc.resolved_dispute_ids,
                },
            );
        }

        let mut lexical_hits = lexical_scores.into_iter().collect::<Vec<_>>();
        lexical_hits.sort_by(|(id_a, score_a), (id_b, score_b)| {
            score_b
                .cmp(score_a)
                .then_with(|| id_a.as_str().cmp(id_b.as_str()))
        });
        lexical_hits.truncate(self.hybrid.lexical_top_n);
        retrieval_debug.lexical_hits = lexical_hits.len();

        let mut vector_hits = Vec::new();
        if self.hybrid.enabled {
            if let (Some(embedding_client), Some(embedding_fingerprint)) = (
                self.embedding_client.as_ref(),
                embedding_fingerprint.as_ref(),
            ) {
                let retrieval_query = agent_query
                    .semantic_query
                    .as_deref()
                    .filter(|text| !text.trim().is_empty())
                    .unwrap_or(agent_query.scope.as_str());
                match tokio::time::timeout(
                    Duration::from_secs(self.hybrid.vector.query_timeout_secs),
                    embedding_client.embed(retrieval_query),
                )
                .await
                {
                    Err(_) => {
                        retrieval_debug.failed_paths.push("vector".into());
                        retrieval_debug.error_summaries.push(format!(
                            "vector query embedding timed out after {}s",
                            self.hybrid.vector.query_timeout_secs
                        ));
                    }
                    Ok(embedding_result) => match embedding_result {
                        Ok(query_vector) => match vector::search_ready_vectors_for_claims(
                            &self.team_root,
                            &query_vector,
                            embedding_fingerprint,
                            &expected_vector_content_hashes,
                            self.hybrid.vector_top_m,
                        )
                        .await
                        {
                            Ok(hits) => vector_hits = hits,
                            Err(err) => {
                                retrieval_debug.failed_paths.push("vector".into());
                                retrieval_debug.error_summaries.push(err.to_string());
                            }
                        },
                        Err(err) => {
                            retrieval_debug.failed_paths.push("vector".into());
                            retrieval_debug.error_summaries.push(err.to_string());
                        }
                    },
                }
            }
        }
        retrieval_debug.vector_hits = vector_hits.len();

        let lexical_lookup: FxHashMap<ClaimId, usize> = lexical_hits.iter().cloned().collect();
        let vector_lookup: FxHashMap<ClaimId, usize> = vector_hits
            .iter()
            .map(|hit| (hit.claim_id.clone(), hit.score))
            .collect();

        retrieval_debug.mode = match (
            retrieval_debug.lexical_hits > 0,
            retrieval_debug.vector_hits > 0,
        ) {
            (true, true) => "hybrid".into(),
            (false, true) => "vector_only".into(),
            _ => "lexical_only".into(),
        };

        let fallback_ids = interleave_hit_ids(&lexical_hits, &vector_hits);
        let mut fallback_order = Vec::new();
        for claim_id in fallback_ids {
            let Some(candidate) = candidates_by_id.get(&claim_id).cloned() else {
                continue;
            };
            let lexical_score = lexical_lookup.get(&claim_id).copied().unwrap_or(0);
            let vector_score = vector_lookup.get(&claim_id).copied().unwrap_or(0);
            fallback_order.push(CandidateEnvelope {
                vector_status: vector_states
                    .get(&claim_id)
                    .map(|state| state.status.as_str().to_string())
                    .unwrap_or_else(|| "not_requested".into()),
                hit_sources: hit_sources(lexical_score, vector_score),
                candidate,
                lexical_score,
                vector_score,
            });
        }

        let fallback_candidates = fallback_order
            .iter()
            .map(|envelope| envelope.candidate.clone())
            .collect::<Vec<_>>();
        let reranked_candidates = if self.hybrid.enabled && self.hybrid.rerank_enabled {
            match rerank::apply_rerank_order(
                fallback_candidates.clone(),
                agent_query,
                self.reranker.clone(),
            )
            .await
            {
                Ok(candidates) => candidates,
                Err(err) => {
                    retrieval_debug.rerank_fallback = true;
                    retrieval_debug.error_summaries.push(err.to_string());
                    fallback_candidates
                }
            }
        } else {
            fallback_candidates
        };
        let mut truncated_candidates = reranked_candidates;
        truncated_candidates.truncate(self.hybrid.top_k);
        let rank_after_rerank = truncated_candidates
            .iter()
            .enumerate()
            .map(|(idx, candidate)| (candidate.claim.id.clone(), idx + 1))
            .collect::<FxHashMap<_, _>>();
        let mut candidate_claims = Vec::new();
        for candidate in truncated_candidates {
            candidate_ids.insert(candidate.claim.id.clone());
            candidate_claims.push(candidate);
        }
        for (idx, envelope) in fallback_order.into_iter().enumerate() {
            retrieval_debug
                .candidates
                .push(crate::router::ClaimRetrievalDebug {
                    claim_id: envelope.candidate.claim.id.to_string(),
                    hit_sources: envelope.hit_sources,
                    lexical_score: envelope.lexical_score,
                    vector_score: envelope.vector_score,
                    rank_before_rerank: idx + 1,
                    rank_after_rerank: rank_after_rerank
                        .get(&envelope.candidate.claim.id)
                        .copied()
                        .unwrap_or(idx + 1),
                    vector_status: envelope.vector_status,
                });
        }

        // 仅附带"双方都在本次候选里"的 dispute 摘要
        let mut disputes = Vec::new();
        for d in self.load_all_disputes().await? {
            let hits = d
                .claims
                .iter()
                .filter(|c| candidate_ids.contains(*c))
                .count();
            if hits >= 2 {
                disputes.push(DisputeRef::from_dispute(&d));
            }
        }

        Ok(RouterQueryResult {
            candidate_claims,
            disputes,
            retrieval_debug: self.hybrid.enabled.then_some(retrieval_debug),
        })
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        self.load_scopes_overview().await
    }
}

fn build_scopes_overview(
    scanned_claims: &[ScannedClaim],
    dispute_map: &FxHashMap<ClaimId, (Vec<DisputeId>, Vec<DisputeId>)>,
) -> ScopesOverviewSnapshot {
    let mut by_scope: FxHashMap<String, ScopeAccumulator> = FxHashMap::default();
    for scanned in scanned_claims {
        if scanned.claim.status == ClaimStatus::Deprecated {
            continue;
        }
        let acc = by_scope.entry(scanned.claim.scope.clone()).or_default();
        match scanned.claim.status {
            ClaimStatus::Active => acc.active_claims += 1,
            ClaimStatus::Stale => acc.stale_claims += 1,
            ClaimStatus::Deprecated => {}
        }
        acc.latest_claim_created_at = Some(
            acc.latest_claim_created_at
                .map(|current| current.max(scanned.claim.created_at))
                .unwrap_or(scanned.claim.created_at),
        );
        if let Some((open_ids, resolved_ids)) = dispute_map.get(&scanned.claim.id) {
            acc.open_dispute_ids.extend(open_ids.iter().cloned());
            acc.resolved_dispute_ids
                .extend(resolved_ids.iter().cloned());
        }
    }

    let mut scopes = by_scope
        .into_iter()
        .filter_map(|(scope, acc)| {
            Some(ScopeOverviewItem {
                scope,
                active_claims: acc.active_claims,
                stale_claims: acc.stale_claims,
                open_disputes: acc.open_dispute_ids.len(),
                resolved_disputes: acc.resolved_dispute_ids.len(),
                latest_claim_created_at: acc.latest_claim_created_at?,
            })
        })
        .collect::<Vec<_>>();
    scopes.sort_by(|lhs, rhs| {
        rhs.active_claims
            .cmp(&lhs.active_claims)
            .then_with(|| rhs.stale_claims.cmp(&lhs.stale_claims))
            .then_with(|| lhs.scope.cmp(&rhs.scope))
    });
    ScopesOverviewSnapshot {
        scopes,
        claim_summaries: None,
    }
}

pub async fn run_refresh_worker(
    router: Arc<Router>,
    interval: Duration,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = ticker.tick() => {
                if let Err(err) = router.refresh_derived_views().await {
                    log::warn!(target: "router_refresh_worker", "router 派生快照刷新失败: {err:#}");
                }
            }
        }
    }
}

#[async_trait]
impl RouterClient for RefreshOnQueryRouterClient {
    async fn query(&self, agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        self.inner.refresh_derived_views().await?;
        self.inner.query(agent_query).await
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        self.inner.refresh_derived_views().await?;
        self.inner.load_scopes_overview().await
    }
}

fn hit_sources(lexical_score: usize, vector_score: usize) -> String {
    match (lexical_score > 0, vector_score > 0) {
        (true, true) => "both".into(),
        (true, false) => "lexical".into(),
        (false, true) => "vector".into(),
        (false, false) => "none".into(),
    }
}

fn interleave_hit_ids(
    lexical_hits: &[(ClaimId, usize)],
    vector_hits: &[vector::VectorHit],
) -> Vec<ClaimId> {
    let mut seen = FxHashSet::default();
    let mut out = Vec::new();
    let max_len = lexical_hits.len().max(vector_hits.len());
    for idx in 0..max_len {
        if let Some((claim_id, _)) = lexical_hits.get(idx) {
            if seen.insert(claim_id.clone()) {
                out.push(claim_id.clone());
            }
        }
        if let Some(hit) = vector_hits.get(idx) {
            if seen.insert(hit.claim_id.clone()) {
                out.push(hit.claim_id.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use crate::api::{EmbeddingCacheFingerprint, EmbeddingClient};
    use crate::claim::{AgentId, Confidence};
    use crate::config::EmbeddingProvider;
    use crate::storage::write_text_atomic;

    fn sample_claim(id: ClaimId, holder: &AgentId, scope: &str, status: ClaimStatus) -> Claim {
        Claim {
            id,
            name: "n".into(),
            statement: "s".into(),
            scope: scope.into(),
            holder: holder.clone(),
            confidence: Confidence::High,
            status,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "e".into(),
        }
    }

    async fn write_claim_to_mirror(team_root: &std::path::Path, c: &Claim) {
        let dir = paths::team_store_agent_claims_dir(team_root, &c.holder);
        let p = dir.join(format!("{}.yaml", c.id));
        write_yaml_atomic(&p, c).await.unwrap();
    }

    async fn write_dispute(team_root: &std::path::Path, d: &Dispute) {
        let p = paths::team_store_disputes_dir(team_root).join(format!("{}.yaml", d.id));
        write_yaml_atomic(&p, d).await.unwrap();
    }

    async fn read_router_snapshot(team_root: &std::path::Path) -> RouterDerivedViewsSnapshot {
        read_yaml(&paths::team_store_router_derived_views_path(team_root))
            .await
            .unwrap()
    }

    async fn seed_ready_vector_state(team_root: &std::path::Path, claim: &Claim, vector: Vec<f32>) {
        let retrieval_doc = RetrievalDocument::from_claim(claim, vec![], vec![]);
        let retrieval_doc_path = paths::team_store_router_retrieval_doc_path(team_root, &claim.id);
        write_yaml_atomic(&retrieval_doc_path, &retrieval_doc)
            .await
            .unwrap();
        vector::store_ready_vector_state(
            team_root,
            &claim.id,
            vector::search_text_hash(&retrieval_doc.search_text),
            test_embedding_fingerprint(),
            vector,
        )
        .await
        .unwrap();
    }

    struct TestEmbeddingClient;

    fn test_embedding_fingerprint() -> EmbeddingCacheFingerprint {
        EmbeddingCacheFingerprint {
            schema_version: 1,
            provider: EmbeddingProvider::OpenAiCompatible,
            endpoint: "http://router-service.test/v1/embeddings".into(),
            model: "two-dimensional-test-vector".into(),
            dimension_policy: "fixed:2".into(),
            normalization: "none".into(),
        }
    }

    #[async_trait]
    impl EmbeddingClient for TestEmbeddingClient {
        fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
            test_embedding_fingerprint()
        }

        async fn embed(&self, input: &str) -> anyhow::Result<Vec<f32>> {
            if input.contains("semantic-query") {
                return Ok(vec![1.0, 0.0]);
            }
            Ok(vec![0.0, 1.0])
        }
    }

    struct BlockingQueryEmbeddingClient {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl EmbeddingClient for BlockingQueryEmbeddingClient {
        fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
            test_embedding_fingerprint()
        }

        async fn embed(&self, input: &str) -> anyhow::Result<Vec<f32>> {
            if input.contains("semantic-query") {
                self.started.notify_one();
                self.release.notified().await;
                return Ok(vec![1.0, 0.0]);
            }
            Ok(vec![0.0, 1.0])
        }
    }

    struct FailingReranker;

    #[async_trait]
    impl rerank::RouterReranker for FailingReranker {
        async fn rerank(
            &self,
            _query: &AgentQuery,
            _candidates: &[rerank::RerankCandidate],
        ) -> anyhow::Result<Vec<ClaimId>> {
            anyhow::bail!("rerank exploded");
        }
    }

    struct SlowEmbeddingClient;

    #[async_trait]
    impl EmbeddingClient for SlowEmbeddingClient {
        fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
            test_embedding_fingerprint()
        }

        async fn embed(&self, _input: &str) -> anyhow::Result<Vec<f32>> {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(vec![1.0, 0.0])
        }
    }

    /// 索引基础：刷新后 bundle 中的 claim index 应带 id + 相对 path。
    #[tokio::test]
    async fn refresh_derived_views_collects_all_agent_claims() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        let c = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &c).await;

        let r = Router::new(team_root.clone());
        r.refresh_derived_views().await.unwrap();

        let snapshot = read_router_snapshot(&team_root).await;
        let idx = snapshot.claim_index();
        assert_eq!(idx.entries().len(), 1);
        assert_eq!(idx.entries()[0].id, c.id);
        assert!(idx.entries()[0].path.ends_with(&format!("{}.yaml", c.id)));
    }

    #[tokio::test]
    async fn refresh_derived_views_writes_scopes_overview_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let mut active = sample_claim(
            ClaimId::random(),
            &agent_a,
            "agent/session/recap",
            ClaimStatus::Active,
        );
        active.created_at = "2026-05-16T12:00:00Z".parse().unwrap();
        let stale = sample_claim(
            ClaimId::random(),
            &agent_b,
            "agent/session/recap",
            ClaimStatus::Stale,
        );
        let deprecated = sample_claim(
            ClaimId::random(),
            &agent_b,
            "agent/session/recap",
            ClaimStatus::Deprecated,
        );
        let other = sample_claim(
            ClaimId::random(),
            &agent_b,
            "router/retrieval/prompt-boundary",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &active).await;
        write_claim_to_mirror(&team_root, &stale).await;
        write_claim_to_mirror(&team_root, &deprecated).await;
        write_claim_to_mirror(&team_root, &other).await;

        write_dispute(
            &team_root,
            &Dispute {
                id: DisputeId::random(),
                name: "open".into(),
                reporter_agent_id: agent_a.clone(),
                claims: vec![active.id.clone(), stale.id.clone()],
                summary: "open".into(),
                status: DisputeStatus::Open,
                created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
                resolved_at: None,
            },
        )
        .await;
        write_dispute(
            &team_root,
            &Dispute {
                id: DisputeId::random(),
                name: "resolved".into(),
                reporter_agent_id: agent_b,
                claims: vec![active.id.clone()],
                summary: "resolved".into(),
                status: DisputeStatus::Resolved,
                created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
                resolved_at: Some("2026-04-22T00:00:00Z".parse().unwrap()),
            },
        )
        .await;

        let r = Router::new(team_root.clone());
        r.refresh_derived_views().await.unwrap();

        let snapshot = read_router_snapshot(&team_root).await;
        let overview = snapshot.scopes_overview();
        assert_eq!(overview.scopes.len(), 2);
        let recap = overview
            .scopes
            .iter()
            .find(|item| item.scope == "agent/session/recap")
            .unwrap();
        assert_eq!(recap.active_claims, 1);
        assert_eq!(recap.stale_claims, 1);
        assert_eq!(recap.open_disputes, 1);
        assert_eq!(recap.resolved_disputes, 1);
        assert_eq!(
            recap.latest_claim_created_at,
            "2026-05-16T12:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn corrupt_bundle_rebuilds_from_authoritative_claims() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        write_claim_to_mirror(
            &team_root,
            &sample_claim(ClaimId::random(), &agent, "scope/a", ClaimStatus::Active),
        )
        .await;
        let bundle_path = paths::team_store_router_derived_views_path(&team_root);
        write_text_atomic(&bundle_path, b"not: [valid")
            .await
            .unwrap();

        let overview = Router::new(team_root.clone())
            .load_scopes_overview()
            .await
            .unwrap();

        assert_eq!(overview.scopes.len(), 1);
        assert_eq!(
            read_router_snapshot(&team_root).await.scopes_overview(),
            &overview
        );
    }

    #[tokio::test]
    async fn failed_scan_keeps_last_complete_bundle_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        write_claim_to_mirror(
            &team_root,
            &sample_claim(ClaimId::random(), &agent, "scope/a", ClaimStatus::Active),
        )
        .await;
        let router = Router::new(team_root.clone());
        router.refresh_derived_views().await.unwrap();
        let bundle_path = paths::team_store_router_derived_views_path(&team_root);
        let before = tokio::fs::read(&bundle_path).await.unwrap();

        let invalid_claim =
            paths::team_store_agent_claims_dir(&team_root, &agent).join("claim_invalid.yaml");
        write_text_atomic(&invalid_claim, b"not: [valid")
            .await
            .unwrap();

        assert!(router.refresh_derived_views().await.is_err());
        assert_eq!(tokio::fs::read(&bundle_path).await.unwrap(), before);
        assert_eq!(
            read_router_snapshot(&team_root)
                .await
                .claim_index()
                .entries()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn future_bundle_schema_blocks_refresh_without_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        write_claim_to_mirror(
            &team_root,
            &sample_claim(ClaimId::random(), &agent, "scope/a", ClaimStatus::Active),
        )
        .await;
        let bundle_path = paths::team_store_router_derived_views_path(&team_root);
        let future = b"schema_version: 2\nfuture_payload:\n  preserves: unknown-fields\n";
        write_text_atomic(&bundle_path, future).await.unwrap();

        let error = Router::new(team_root)
            .refresh_derived_views()
            .await
            .unwrap_err();

        assert!(error.to_string().contains("schema_version=2"));
        assert_eq!(tokio::fs::read(&bundle_path).await.unwrap(), future);
    }

    #[tokio::test]
    async fn deleting_router_directory_rebuilds_snapshot_from_authoritative_data() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        write_claim_to_mirror(
            &team_root,
            &sample_claim(ClaimId::random(), &agent, "scope/a", ClaimStatus::Active),
        )
        .await;

        let r = Router::new(team_root.clone());
        r.refresh_derived_views().await.unwrap();
        tokio::fs::remove_dir_all(paths::team_store_router_dir(&team_root))
            .await
            .unwrap();

        let snapshot = r.load_scopes_overview().await.unwrap();
        assert_eq!(snapshot.scopes.len(), 1);
        assert!(paths::team_store_router_derived_views_path(&team_root).exists());
    }

    #[tokio::test]
    async fn refresh_derived_views_parallel_calls_keep_snapshot_readable() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        write_claim_to_mirror(
            &team_root,
            &sample_claim(ClaimId::random(), &agent, "scope/a", ClaimStatus::Active),
        )
        .await;

        let first = Router::new(team_root.clone());
        let second = Router::new(team_root.clone());
        let (a, b) = tokio::join!(
            first.refresh_derived_views(),
            second.refresh_derived_views()
        );
        a.unwrap();
        b.unwrap();

        let snapshot = read_router_snapshot(&team_root).await;
        assert_eq!(snapshot.claim_index().entries().len(), 1);
        assert_eq!(snapshot.scopes_overview().scopes.len(), 1);
        assert!(paths::team_store_router_derived_views_lock_path(&team_root).exists());
    }

    #[tokio::test]
    async fn refresh_on_query_router_client_sees_new_claims_after_snapshot_exists() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent = AgentId::new("agent-a").unwrap();
        let first = sample_claim(
            ClaimId::random(),
            &agent,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &first).await;

        let router = Arc::new(Router::new(team_root.clone()));
        router.refresh_derived_views().await.unwrap();

        let second = sample_claim(
            ClaimId::random(),
            &agent,
            "billing / invoice",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &second).await;

        let client = RefreshOnQueryRouterClient::new(router);
        let result = client
            .query(&AgentQuery::from_scope("billing"))
            .await
            .unwrap();
        assert_eq!(result.candidate_claims.len(), 1);
        assert_eq!(result.candidate_claims[0].claim.id, second.id);
    }

    /// scope word segment 相关性匹配 + 无匹配返回空
    #[tokio::test]
    async fn query_filters_by_scope_word_segments() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        let c1 = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        let c2 = sample_claim(
            ClaimId::random(),
            &a,
            "billing / invoice",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &c1).await;
        write_claim_to_mirror(&team_root, &c2).await;

        let r = Router::new(team_root);
        let hit = r
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(hit.candidate_claims.len(), 1);
        assert_eq!(hit.candidate_claims[0].claim.id, c1.id);

        let miss = r
            .query(&AgentQuery::from_scope("nonexistent"))
            .await
            .unwrap();
        assert!(miss.candidate_claims.is_empty());
    }

    #[tokio::test]
    async fn query_truncates_final_candidates_to_top_k() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        for _ in 0..3 {
            write_claim_to_mirror(
                &team_root,
                &sample_claim(
                    ClaimId::random(),
                    &a,
                    "order-system / payment-service / prod",
                    ClaimStatus::Active,
                ),
            )
            .await;
        }

        let r = Router::with_hybrid_config(
            team_root,
            RouterRetrievalConfig {
                top_k: 2,
                rerank_enabled: false,
                ..RouterRetrievalConfig::default()
            },
        );
        let res = r
            .query(&AgentQuery::from_scope("order-system / payment-service"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 2);
    }

    /// batch-order-submit 任务应能召回同一 order-system 下 payment-service 的 claim
    #[tokio::test]
    async fn query_matches_scope_by_word_segment_relevance() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        let c = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &c).await;

        let r = Router::new(team_root);
        let res = r
            .query(&AgentQuery::from_scope("order-system / batch-order-submit"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        assert_eq!(res.candidate_claims[0].claim.id, c.id);
    }

    #[tokio::test]
    async fn query_prefers_claims_whose_text_matches_task_terms() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        let mut stronger = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        stronger.name = "payment_timeout_root_cause".into();
        stronger.statement = "payment timeout is caused by connection pool exhaustion".into();
        stronger.evidence_summary = "timeout logs point to pool exhaustion".into();
        let weaker = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &weaker).await;
        write_claim_to_mirror(&team_root, &stronger).await;

        let r = Router::new(team_root);
        let res = r
            .query(&AgentQuery::from_task(
                "order-system / payment-service",
                "investigate payment timeout root cause",
            ))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 2);
        assert_eq!(res.candidate_claims[0].claim.id, stronger.id);
    }

    #[tokio::test]
    async fn query_refreshes_missing_lexical_doc_before_recall() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        let mut claim = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        claim.name = "payment_timeout_root_cause".into();
        claim.statement = "payment timeout is caused by connection pool exhaustion".into();
        claim.evidence_summary = "timeout logs point to pool exhaustion".into();
        write_claim_to_mirror(&team_root, &claim).await;

        let retrieval_doc_path = paths::team_store_router_retrieval_docs_dir(&team_root)
            .join(format!("{}.yaml", claim.id));

        let r = Router::new(team_root);
        let res = r
            .query(&AgentQuery::from_task(
                "order-system",
                "investigate timeout",
            ))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        assert!(retrieval_doc_path.exists());
    }

    #[tokio::test]
    async fn query_recovers_corrupted_lexical_doc_without_breaking_recall() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-b").unwrap();
        let mut claim = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        claim.name = "payment_timeout_root_cause".into();
        claim.statement = "payment timeout is caused by connection pool exhaustion".into();
        claim.evidence_summary = "timeout logs point to pool exhaustion".into();
        write_claim_to_mirror(&team_root, &claim).await;

        let retrieval_doc_path = paths::team_store_router_retrieval_doc_path(&team_root, &claim.id);
        write_text_atomic(&retrieval_doc_path, b"not: [valid yaml")
            .await
            .unwrap();

        let r = Router::new(team_root.clone());
        let res = r
            .query(&AgentQuery::from_task(
                "order-system",
                "investigate timeout",
            ))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);

        let recovered: RetrievalDocument = read_yaml(&retrieval_doc_path).await.unwrap();
        assert_eq!(recovered.claim_id, claim.id);
        assert!(recovered.search_text.contains("connection pool exhaustion"));
    }

    #[tokio::test]
    async fn query_refreshes_stale_lexical_doc_when_dispute_fields_change() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let claim_a = sample_claim(
            ClaimId::random(),
            &agent_a,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        let mut claim_b = sample_claim(
            ClaimId::random(),
            &agent_b,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        claim_b.name = "payment_timeout_root_cause".into();
        claim_b.statement = "payment timeout is caused by connection pool exhaustion".into();
        claim_b.evidence_summary = "timeout logs point to pool exhaustion".into();
        write_claim_to_mirror(&team_root, &claim_a).await;
        write_claim_to_mirror(&team_root, &claim_b).await;

        let retrieval_doc_path =
            paths::team_store_router_retrieval_doc_path(&team_root, &claim_b.id);
        let stale_doc = RetrievalDocument::from_claim(&claim_b, vec![], vec![]);
        write_yaml_atomic(&retrieval_doc_path, &stale_doc)
            .await
            .unwrap();

        let dispute = Dispute {
            id: DisputeId::random(),
            name: "payment_batch_timeout_vs_success".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![claim_a.id.clone(), claim_b.id.clone()],
            summary: "...".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        write_dispute(&team_root, &dispute).await;

        let r = Router::new(team_root.clone());
        let res = r
            .query(&AgentQuery::from_task(
                "order-system",
                "investigate timeout",
            ))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 2);

        let refreshed: RetrievalDocument = read_yaml(&retrieval_doc_path).await.unwrap();
        assert_eq!(refreshed.open_dispute_ids, vec![dispute.id.clone()]);
        assert!(refreshed.search_text.contains("connection pool exhaustion"));
    }

    /// deprecated claim 不出现在默认查询结果中
    #[tokio::test]
    async fn query_excludes_deprecated_claims() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let a = AgentId::new("agent-d").unwrap();
        let active = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / api",
            ClaimStatus::Active,
        );
        let deprecated = sample_claim(
            ClaimId::random(),
            &a,
            "order-system / api",
            ClaimStatus::Deprecated,
        );
        write_claim_to_mirror(&team_root, &active).await;
        write_claim_to_mirror(&team_root, &deprecated).await;

        let r = Router::new(team_root);
        let res = r
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        assert_eq!(res.candidate_claims[0].claim.id, active.id);
    }

    /// dispute 状态会在 rebuild 后同步进 claim_index。
    #[tokio::test]
    async fn query_syncs_dispute_state_into_index() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let ca = sample_claim(
            ClaimId::random(),
            &agent_a,
            "order-system / payment-service / staging",
            ClaimStatus::Active,
        );
        let cb = sample_claim(
            ClaimId::random(),
            &agent_b,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &ca).await;
        write_claim_to_mirror(&team_root, &cb).await;

        let dispute = Dispute {
            id: DisputeId::random(),
            name: "payment_batch_timeout_vs_success".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ca.id.clone(), cb.id.clone()],
            summary: "...".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        write_dispute(&team_root, &dispute).await;

        let r = Router::new(team_root.clone());
        let res = r
            .query(&AgentQuery::from_scope("order-system / payment-service"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 2);
        // 两条候选都应该带上 open dispute id
        for candidate_claim in &res.candidate_claims {
            assert_eq!(candidate_claim.open_dispute_ids, vec![dispute.id.clone()]);
        }
        // disputes 摘要里有这条 dispute（双方命中）
        assert_eq!(res.disputes.len(), 1);
        assert_eq!(res.disputes[0].id, dispute.id);

        // bundle 中的 claim index 已被刷新
        let snapshot = read_router_snapshot(&team_root).await;
        let idx = snapshot.claim_index();
        for entry in idx.entries() {
            assert_eq!(entry.open_dispute_ids, vec![dispute.id.clone()]);
        }
    }

    /// dispute 双方未都被命中时，不附带 disputes 摘要
    #[tokio::test]
    async fn dispute_summary_only_when_both_sides_hit() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let ca = sample_claim(
            ClaimId::random(),
            &agent_a,
            "order-system / payment-service",
            ClaimStatus::Active,
        );
        let cb = sample_claim(
            ClaimId::random(),
            &agent_b,
            "billing / refund",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &ca).await;
        write_claim_to_mirror(&team_root, &cb).await;

        let dispute = Dispute {
            id: DisputeId::random(),
            name: "x".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ca.id.clone(), cb.id.clone()],
            summary: "x".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        write_dispute(&team_root, &dispute).await;

        let r = Router::new(team_root);
        // 只命中 ca
        let res = r
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        // ca 仍带有 open_dispute_ids（来自 index），但 disputes 摘要不应附带
        assert_eq!(res.candidate_claims[0].open_dispute_ids.len(), 1);
        assert!(
            res.disputes.is_empty(),
            "对侧未命中时不应在 disputes 中带回该 dispute"
        );
    }

    /// 空目录场景：什么都没有时 query 不应 panic
    #[tokio::test]
    async fn query_on_empty_team_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let r = Router::new(dir.path().to_path_buf());
        let res = r.query(&AgentQuery::from_scope("anything")).await.unwrap();
        assert!(res.candidate_claims.is_empty());
        assert!(res.disputes.is_empty());
    }

    #[tokio::test]
    async fn query_returns_lexical_results_when_vector_not_ready() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let claim = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &claim).await;

        let router = Router::with_dependencies(
            team_root.clone(),
            RouterRetrievalConfig::default(),
            Some(Arc::new(TestEmbeddingClient)),
            rerank::default_reranker(),
        );
        let res = router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        let debug = res.retrieval_debug.unwrap();
        assert_eq!(debug.mode, "lexical_only");
        assert_eq!(debug.lexical_hits, 1);
        assert_eq!(debug.candidates[0].vector_status, "pending");

        let state = vector::load_vector_state(&team_root, &claim.id)
            .await
            .unwrap();
        assert_eq!(state.status, vector::VectorStatus::Pending);
    }

    #[tokio::test]
    async fn query_honors_failed_vector_backoff_and_keeps_lexical_results() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let claim = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &claim).await;

        let retrieval_doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let retrieval_doc_path = paths::team_store_router_retrieval_doc_path(&team_root, &claim.id);
        write_yaml_atomic(&retrieval_doc_path, &retrieval_doc)
            .await
            .unwrap();
        let _ = vector::store_failed_vector_state(
            &team_root,
            &claim.id,
            vector::search_text_hash(&retrieval_doc.search_text),
            test_embedding_fingerprint(),
            "embedding worker failed".into(),
            vector::VectorRetryPolicy::new(Duration::from_secs(60), Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();

        let router = Router::with_dependencies(
            team_root.clone(),
            RouterRetrievalConfig::default(),
            Some(Arc::new(TestEmbeddingClient)),
            rerank::default_reranker(),
        );
        let res = router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        let debug = res.retrieval_debug.unwrap();
        assert_eq!(debug.candidates[0].vector_status, "failed");

        let state = vector::load_vector_state(&team_root, &claim.id)
            .await
            .unwrap();
        assert_eq!(state.status, vector::VectorStatus::Failed);
    }

    #[tokio::test]
    async fn query_keeps_lexical_results_when_vector_target_setup_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let claim = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &claim).await;

        let broken_state_path = paths::team_store_router_vector_state_path(&team_root, &claim.id);
        write_text_atomic(&broken_state_path, b"not valid vector state")
            .await
            .unwrap();

        let router = Router::with_dependencies(
            team_root,
            RouterRetrievalConfig::default(),
            Some(Arc::new(TestEmbeddingClient)),
            rerank::default_reranker(),
        );
        let res = router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();

        assert_eq!(res.candidate_claims.len(), 1);
        let debug = res.retrieval_debug.unwrap();
        assert_eq!(debug.mode, "lexical_only");
        assert_eq!(debug.lexical_hits, 1);
        assert!(debug.failed_paths.iter().any(|path| path == "vector"));
        assert!(debug
            .error_summaries
            .iter()
            .any(|summary| summary.contains("建立 claim Vector target 失败")));
    }

    #[tokio::test]
    async fn lexical_only_query_keeps_pending_vector_target_intent_for_worker_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let claim = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &claim).await;
        let retrieval_doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);

        let queue_path = paths::team_store_router_vector_queue_path(&team_root);
        tokio::fs::create_dir_all(&queue_path).await.unwrap();
        let failed_target = vector::ensure_retrieval_target(
            &team_root,
            &retrieval_doc,
            Some(&test_embedding_fingerprint()),
        )
        .await
        .unwrap();
        assert!(failed_target.vector_error.is_some());
        let intent_path = paths::team_store_router_vector_intent_path(&team_root, &claim.id);
        assert!(tokio::fs::try_exists(&intent_path).await.unwrap());

        // 默认 Router 没有 embedding client，只做 lexical；它不能把 worker 唯一的恢复锚点删掉。
        let lexical_router = Router::new(team_root.clone());
        let result = lexical_router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(result.candidate_claims.len(), 1);
        assert!(tokio::fs::try_exists(&intent_path).await.unwrap());

        tokio::fs::remove_dir(&queue_path).await.unwrap();
        let report = vector::process_pending_queue(
            team_root.clone(),
            Arc::new(TestEmbeddingClient),
            1,
            vector::VectorRetryPolicy::new(Duration::from_millis(10), Duration::from_millis(100))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(report.processed, 1);
        let state = vector::load_vector_state(&team_root, &claim.id)
            .await
            .unwrap();
        assert_eq!(state.status, vector::VectorStatus::Ready);
        assert!(!tokio::fs::try_exists(&intent_path).await.unwrap());
    }

    #[tokio::test]
    async fn lexical_only_claim_update_keeps_vector_worker_recovery_target() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let claim_a = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &claim_a).await;

        // 先让有 embedding 的 Router 为 A 建立已知 target；正常建队成功后不会残留 intent。
        let vector_router = Router::with_dependencies(
            team_root.clone(),
            RouterRetrievalConfig::default(),
            Some(Arc::new(TestEmbeddingClient)),
            rerank::default_reranker(),
        );
        vector_router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        let state_a = vector::load_vector_state(&team_root, &claim_a.id)
            .await
            .unwrap();
        assert_eq!(state_a.status, vector::VectorStatus::Pending);
        let intent_path = paths::team_store_router_vector_intent_path(&team_root, &claim_a.id);
        assert!(!tokio::fs::try_exists(&intent_path).await.unwrap());

        let mut claim_b = claim_a.clone();
        claim_b.statement = "payment-service 的超时阈值已调整为 45 秒".into();
        claim_b.updated_at = Some("2026-07-13T12:00:00Z".parse().unwrap());
        write_claim_to_mirror(&team_root, &claim_b).await;

        // B 换代时这个 Router 没有 embedding client；它仍须从 A 的已知 target 继承恢复 intent。
        let lexical_router = Router::new(team_root.clone());
        let result = lexical_router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(result.candidate_claims.len(), 1);
        assert!(tokio::fs::try_exists(&intent_path).await.unwrap());

        // worker 恢复时会丢弃 A 的过期队列项、重建并完成 B，不需要第二次带 embedding 的查询。
        vector::process_pending_queue(
            team_root.clone(),
            Arc::new(TestEmbeddingClient),
            1,
            vector::VectorRetryPolicy::new(Duration::from_millis(10), Duration::from_millis(100))
                .unwrap(),
        )
        .await
        .unwrap();
        let state_b = vector::load_vector_state(&team_root, &claim_a.id)
            .await
            .unwrap();
        let retrieval_doc_b = RetrievalDocument::from_claim(&claim_b, vec![], vec![]);
        assert_eq!(state_b.status, vector::VectorStatus::Ready);
        assert_eq!(
            state_b.content_hash,
            vector::search_text_hash(&retrieval_doc_b.search_text)
        );
        assert!(!tokio::fs::try_exists(&intent_path).await.unwrap());
    }

    #[tokio::test]
    async fn hybrid_query_merges_lexical_and_vector_hits_before_final_ordering() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let mut lexical_and_vector = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        lexical_and_vector.name = "semantic bridge claim".into();
        let mut vector_only = sample_claim(
            ClaimId::random(),
            &holder,
            "inventory / stock-sync",
            ClaimStatus::Active,
        );
        vector_only.name = "warehouse sync note".into();
        write_claim_to_mirror(&team_root, &lexical_and_vector).await;
        write_claim_to_mirror(&team_root, &vector_only).await;
        seed_ready_vector_state(&team_root, &lexical_and_vector, vec![1.0, 0.0]).await;
        seed_ready_vector_state(&team_root, &vector_only, vec![1.0, 0.0]).await;

        let router = Router::with_dependencies(
            team_root,
            RouterRetrievalConfig::default(),
            Some(Arc::new(TestEmbeddingClient)),
            rerank::default_reranker(),
        );
        let res = router
            .query(&AgentQuery::from_task("order-system", "semantic-query"))
            .await
            .unwrap();
        let debug = res.retrieval_debug.unwrap();
        assert_eq!(debug.mode, "hybrid");
        assert!(debug
            .candidates
            .iter()
            .any(|candidate| candidate.hit_sources == "both"));
        assert!(debug
            .candidates
            .iter()
            .any(|candidate| candidate.hit_sources == "vector"));
    }

    #[tokio::test]
    async fn query_does_not_score_claim_a_with_concurrent_ready_vector_b() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let mut claim_a = sample_claim(
            ClaimId::random(),
            &holder,
            "catalog / alpha",
            ClaimStatus::Active,
        );
        claim_a.name = "claim-version-a".into();
        claim_a.statement = "the original retrieval text".into();
        write_claim_to_mirror(&team_root, &claim_a).await;
        seed_ready_vector_state(&team_root, &claim_a, vec![1.0, 0.0]).await;

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let router = Arc::new(Router::with_dependencies(
            team_root.clone(),
            RouterRetrievalConfig::default(),
            Some(Arc::new(BlockingQueryEmbeddingClient {
                started: started.clone(),
                release: release.clone(),
            })),
            rerank::default_reranker(),
        ));
        let query = AgentQuery::from_task("unmatched-context", "semantic-query");
        let query_task = tokio::spawn({
            let router = router.clone();
            async move { router.query(&query).await }
        });
        started.notified().await;

        let mut claim_b = claim_a.clone();
        claim_b.statement = "the replacement retrieval text".into();
        write_claim_to_mirror(&team_root, &claim_b).await;
        let retrieval_doc_b = RetrievalDocument::from_claim(&claim_b, vec![], vec![]);
        let pending_b = vector::ensure_retrieval_target(
            &team_root,
            &retrieval_doc_b,
            Some(&test_embedding_fingerprint()),
        )
        .await
        .unwrap()
        .vector_state
        .expect("启用 embedding 时必须建立 Vector target");
        assert_eq!(pending_b.status, vector::VectorStatus::Pending);
        vector::store_ready_vector_state(
            &team_root,
            &claim_b.id,
            vector::search_text_hash(&retrieval_doc_b.search_text),
            test_embedding_fingerprint(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();

        release.notify_one();
        let result = query_task.await.unwrap().unwrap();
        let debug = result
            .retrieval_debug
            .expect("hybrid 查询应返回检索调试信息");
        assert!(result.candidate_claims.is_empty());
        assert_eq!(debug.vector_hits, 0);
        assert_eq!(debug.mode, "lexical_only");

        let state_b = vector::load_vector_state(&team_root, &claim_b.id)
            .await
            .unwrap();
        assert_eq!(state_b.status, vector::VectorStatus::Ready);
        assert_eq!(
            state_b.content_hash,
            vector::search_text_hash(&retrieval_doc_b.search_text)
        );
    }

    #[tokio::test]
    async fn stale_claim_snapshot_cannot_replace_a_published_newer_target() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let mut claim_a = sample_claim(
            ClaimId::random(),
            &holder,
            "catalog / alpha",
            ClaimStatus::Active,
        );
        // 两次合法更新可落在同一秒；这个 fence 必须比较完整快照，而不是依赖时间排序。
        claim_a.updated_at = Some("2026-06-01T00:00:00Z".parse().unwrap());
        claim_a.statement = "the original retrieval text".into();
        write_claim_to_mirror(&team_root, &claim_a).await;
        let source_path = paths::team_store_agent_claims_dir(&team_root, &holder)
            .join(format!("{}.yaml", claim_a.id));
        let stale_snapshot: Claim = read_yaml(&source_path).await.unwrap();
        let retrieval_doc_a = RetrievalDocument::from_claim(&stale_snapshot, vec![], vec![]);

        let mut claim_b = claim_a.clone();
        claim_b.statement = "the replacement retrieval text".into();
        write_claim_to_mirror(&team_root, &claim_b).await;
        let retrieval_doc_b = RetrievalDocument::from_claim(&claim_b, vec![], vec![]);
        let pending_b = vector::ensure_retrieval_target_for_claim_snapshot(
            &team_root,
            &source_path,
            &claim_b,
            &retrieval_doc_b,
            Some(&test_embedding_fingerprint()),
        )
        .await
        .unwrap()
        .expect("当前 B 快照必须能发布 target")
        .vector_state
        .expect("启用 embedding 时必须建立 Vector target");
        assert_eq!(pending_b.status, vector::VectorStatus::Pending);
        vector::store_ready_vector_state(
            &team_root,
            &claim_b.id,
            vector::search_text_hash(&retrieval_doc_b.search_text),
            test_embedding_fingerprint(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();

        // 模拟已经读取 A 的旧请求在 B 完成发布后才取得 target lock。
        let stale_publish = vector::ensure_retrieval_target_for_claim_snapshot(
            &team_root,
            &source_path,
            &stale_snapshot,
            &retrieval_doc_a,
            Some(&test_embedding_fingerprint()),
        )
        .await
        .unwrap();
        assert!(stale_publish.is_none());

        let persisted_doc: RetrievalDocument = read_yaml(
            &paths::team_store_router_retrieval_doc_path(&team_root, &claim_b.id),
        )
        .await
        .unwrap();
        assert_eq!(persisted_doc, retrieval_doc_b);
        let persisted_state = vector::load_vector_state(&team_root, &claim_b.id)
            .await
            .unwrap();
        assert_eq!(persisted_state.status, vector::VectorStatus::Ready);
        assert_eq!(
            persisted_state.content_hash,
            vector::search_text_hash(&retrieval_doc_b.search_text)
        );
    }

    #[tokio::test]
    async fn rerank_failure_falls_back_to_interleaved_channel_order() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let lexical = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        let vector = sample_claim(
            ClaimId::random(),
            &holder,
            "inventory / stock-sync",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &lexical).await;
        write_claim_to_mirror(&team_root, &vector).await;
        seed_ready_vector_state(&team_root, &lexical, vec![1.0, 0.0]).await;
        seed_ready_vector_state(&team_root, &vector, vec![1.0, 0.0]).await;

        let router = Router::with_dependencies(
            team_root,
            RouterRetrievalConfig {
                rerank_enabled: true,
                ..RouterRetrievalConfig::default()
            },
            Some(Arc::new(TestEmbeddingClient)),
            Arc::new(FailingReranker),
        );
        let res = router
            .query(&AgentQuery::from_task("order-system", "semantic-query"))
            .await
            .unwrap();
        let debug = res.retrieval_debug.unwrap();
        assert!(debug.rerank_fallback);
        assert!(debug
            .error_summaries
            .iter()
            .any(|summary| summary.contains("rerank exploded")));
        assert_eq!(res.candidate_claims[0].claim.id, lexical.id);
    }

    #[tokio::test(start_paused = true)]
    async fn query_embedding_timeout_keeps_lexical_results() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let holder = AgentId::new("agent-b").unwrap();
        let claim = sample_claim(
            ClaimId::random(),
            &holder,
            "order-system / payment-service / prod",
            ClaimStatus::Active,
        );
        write_claim_to_mirror(&team_root, &claim).await;

        let router = Router::with_dependencies(
            team_root,
            RouterRetrievalConfig {
                vector: crate::config::RouterRetrievalVectorConfig {
                    query_timeout_secs: 1,
                    ..Default::default()
                },
                ..RouterRetrievalConfig::default()
            },
            Some(Arc::new(SlowEmbeddingClient)),
            rerank::default_reranker(),
        );
        let res = router
            .query(&AgentQuery::from_scope("order-system"))
            .await
            .unwrap();
        assert_eq!(res.candidate_claims.len(), 1);
        let debug = res.retrieval_debug.unwrap();
        assert!(debug.failed_paths.iter().any(|path| path == "vector"));
        assert!(debug
            .error_summaries
            .iter()
            .any(|summary| summary.contains("timed out")));
    }
}
