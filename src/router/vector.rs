//! router 向量派生状态与本地队列。
//!
//! 该模块只维护 router 自己的派生层：
//! - query 期确保 claim 至少进入 pending，不阻塞 lexical recall
//! - 后台 worker 消费 `pending.jsonl` 并更新单 claim 的向量状态
//! - doc 与 queue/state 跨文件发布时以 target intent 留下可重放锚点

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use ring::digest::{digest, SHA256};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::api::{EmbeddingCacheFingerprint, EmbeddingClient};
use crate::claim::{Claim, ClaimId};
use crate::router::RetrievalDocument;
use crate::storage::{paths, read_yaml, write_text_atomic, write_yaml_atomic, FileLockGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorStatus {
    Pending,
    Ready,
    Failed,
}

impl VectorStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorState {
    pub claim_id: ClaimId,
    pub status: VectorStatus,
    pub updated_at: DateTime<Utc>,
    pub content_hash: String,
    /// 同一 claim 的持久化 generation 单调序号；旧版 JSON 缺失时按 0 处理。
    #[serde(default)]
    pub generation_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_fingerprint: Option<EmbeddingCacheFingerprint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_dimensions: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_dimensions: Option<usize>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct VectorAttemptMetadata {
    attempts: u32,
    last_attempt_at: DateTime<Utc>,
    next_retry_at: Option<DateTime<Utc>>,
}

impl VectorAttemptMetadata {
    fn completed(attempts: u32, last_attempt_at: DateTime<Utc>) -> Self {
        Self {
            attempts,
            last_attempt_at,
            next_retry_at: None,
        }
    }

    fn failed(attempts: u32, last_attempt_at: DateTime<Utc>, next_retry_at: DateTime<Utc>) -> Self {
        Self {
            attempts,
            last_attempt_at,
            next_retry_at: Some(next_retry_at),
        }
    }
}

impl VectorState {
    fn pending(
        claim_id: ClaimId,
        content_hash: String,
        embedding_fingerprint: EmbeddingCacheFingerprint,
        expected_dimensions: Option<usize>,
        generation_seq: u64,
    ) -> Self {
        Self {
            claim_id,
            status: VectorStatus::Pending,
            updated_at: Utc::now(),
            content_hash,
            generation_seq,
            embedding_fingerprint: Some(embedding_fingerprint),
            vector_dimensions: None,
            expected_dimensions,
            attempts: 0,
            last_attempt_at: None,
            next_retry_at: None,
            vector: None,
            error_summary: None,
        }
    }

    fn ready(
        claim_id: ClaimId,
        content_hash: String,
        embedding_fingerprint: EmbeddingCacheFingerprint,
        expected_dimensions: Option<usize>,
        generation_seq: u64,
        attempt: VectorAttemptMetadata,
        vector: Vec<f32>,
    ) -> Self {
        let vector_dimensions = vector.len();
        Self {
            claim_id,
            status: VectorStatus::Ready,
            updated_at: Utc::now(),
            content_hash,
            generation_seq,
            embedding_fingerprint: Some(embedding_fingerprint),
            vector_dimensions: Some(vector_dimensions),
            expected_dimensions,
            attempts: attempt.attempts,
            last_attempt_at: Some(attempt.last_attempt_at),
            next_retry_at: attempt.next_retry_at,
            vector: Some(vector),
            error_summary: None,
        }
    }

    fn failed(
        claim_id: ClaimId,
        content_hash: String,
        embedding_fingerprint: EmbeddingCacheFingerprint,
        expected_dimensions: Option<usize>,
        generation_seq: u64,
        attempt: VectorAttemptMetadata,
        error_summary: String,
    ) -> Self {
        Self {
            claim_id,
            status: VectorStatus::Failed,
            updated_at: Utc::now(),
            content_hash,
            generation_seq,
            embedding_fingerprint: Some(embedding_fingerprint),
            vector_dimensions: None,
            expected_dimensions,
            attempts: attempt.attempts,
            last_attempt_at: Some(attempt.last_attempt_at),
            next_retry_at: attempt.next_retry_at,
            vector: None,
            error_summary: Some(error_summary),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct VectorQueueEntry {
    claim_id: ClaimId,
    content_hash: String,
    #[serde(default)]
    generation_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    embedding_fingerprint: Option<EmbeddingCacheFingerprint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_dimensions: Option<usize>,
    enqueued_at: DateTime<Utc>,
}

/// 即将或已发布检索文档、但尚未完成 Vector state/queue 建立时的可恢复意图。
///
/// 这个文件是跨多个原子文件写的提交日志：仅在 doc 与内容 hash 都吻合时才允许 worker
/// 重放；若进程停在 doc 前则清理意图，避免旧任务从新正文凭空推导 successor。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorTargetIntent {
    claim_id: ClaimId,
    content_hash: String,
    embedding_fingerprint: EmbeddingCacheFingerprint,
}

impl VectorTargetIntent {
    fn from_retrieval_doc(
        retrieval_doc: &RetrievalDocument,
        embedding_fingerprint: EmbeddingCacheFingerprint,
    ) -> Self {
        Self {
            claim_id: retrieval_doc.claim_id.clone(),
            content_hash: search_text_hash(&retrieval_doc.search_text),
            embedding_fingerprint,
        }
    }
}

type VectorQueueEntryKey = (
    ClaimId,
    String,
    u64,
    Option<EmbeddingCacheFingerprint>,
    Option<usize>,
);

struct DrainedQueue {
    entries: Vec<VectorQueueEntry>,
    in_flight_path: PathBuf,
    lease_path: PathBuf,
    lease: FileLockGuard,
}

/// claim embedding 失败后的持久化退避策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorRetryPolicy {
    base_delay: Duration,
    max_delay: Duration,
}

impl VectorRetryPolicy {
    pub fn new(base_delay: Duration, max_delay: Duration) -> anyhow::Result<Self> {
        if base_delay.is_zero() {
            anyhow::bail!("vector retry base delay must be > 0");
        }
        if max_delay < base_delay {
            anyhow::bail!("vector retry max delay must be >= base delay");
        }
        Ok(Self {
            base_delay,
            max_delay,
        })
    }

    fn retry_at(self, attempts: u32, now: DateTime<Utc>) -> DateTime<Utc> {
        let shift = attempts.saturating_sub(1).min(63);
        let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
        let delay_ms = u64::try_from(self.base_delay.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_mul(multiplier)
            .min(u64::try_from(self.max_delay.as_millis()).unwrap_or(u64::MAX));
        let chrono_delay =
            chrono::Duration::try_milliseconds(i64::try_from(delay_ms).unwrap_or(i64::MAX))
                .unwrap_or(chrono::Duration::MAX);
        now.checked_add_signed(chrono_delay)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorProcessReport {
    pub processed: usize,
    pub failures: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub claim_id: ClaimId,
    pub score: usize,
}

/// 一次 retrieval target 发布的可降级结果。
///
/// retrieval document 的读写错误仍通过外层 `Result` 返回；Vector target 是可选召回层，
/// 其部分发布错误交给 Router 记录后回退 lexical，避免丢失本可用的 Claim 结果。
pub(crate) struct RetrievalTargetEnsure {
    pub(crate) vector_state: Option<VectorState>,
    pub(crate) vector_error: Option<anyhow::Error>,
}

enum QueueEntryOutcome {
    Complete,
    RequeueForMatchingWorker,
    EmbeddingFailureRecorded(String),
}

pub fn search_text_hash(text: &str) -> String {
    // 此摘要是跨进程、跨重启的内容代围栏，不能使用 DefaultHasher 这类非稳定、非抗碰撞 hash。
    format!(
        "sha256-v1:{}",
        hex::encode(digest(&SHA256, text.as_bytes()).as_ref())
    )
}

const SEARCH_TEXT_HASH_V1_PREFIX: &str = "sha256-v1:";
const SEARCH_TEXT_HASH_V1_HEX_LEN: usize = 64;

/// 只接受当前实现可证明的内容代摘要；旧持久化摘要一律安全失效，不复用未知正文的向量。
fn is_current_search_text_hash(value: &str) -> bool {
    let Some(hex) = value.strip_prefix(SEARCH_TEXT_HASH_V1_PREFIX) else {
        return false;
    };
    hex.len() == SEARCH_TEXT_HASH_V1_HEX_LEN
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// 读取单条 claim 的当前 Vector state。
///
/// 这是保留的兼容读取入口；生产写入必须使用本模块的 target 协调入口。
pub async fn load_vector_state(
    team_root: &Path,
    claim_id: &ClaimId,
) -> anyhow::Result<VectorState> {
    load_vector_state_inner(team_root, claim_id).await
}

async fn load_vector_state_inner(
    team_root: &Path,
    claim_id: &ClaimId,
) -> anyhow::Result<VectorState> {
    let path = paths::team_store_router_vector_state_path(team_root, claim_id);
    let raw = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("读取向量状态失败: {path:?}"))?;
    serde_json::from_str(&raw).with_context(|| format!("解析向量状态失败: {path:?}"))
}

pub async fn load_vector_state_opt(
    team_root: &Path,
    claim_id: &ClaimId,
) -> anyhow::Result<Option<VectorState>> {
    let path = paths::team_store_router_vector_state_path(team_root, claim_id);
    if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return Ok(None);
    }
    load_vector_state_inner(team_root, claim_id).await.map(Some)
}

/// 为回归测试构造已确认权威的 retrieval target。
///
/// 正式 Router 只能使用 `ensure_retrieval_target_for_claim_snapshot`，避免无 source snapshot
/// fence 的调用倒写较新的检索正文。
#[cfg(test)]
pub(crate) async fn ensure_retrieval_target(
    team_root: &Path,
    retrieval_doc: &RetrievalDocument,
    embedding_fingerprint: Option<&EmbeddingCacheFingerprint>,
) -> anyhow::Result<RetrievalTargetEnsure> {
    let _target_guard = lock_retrieval_target(team_root, &retrieval_doc.claim_id).await?;
    ensure_retrieval_target_locked(team_root, retrieval_doc, embedding_fingerprint).await
}

/// 仅当 Router 最初读取的镜像 Claim 在同一协调边界内仍是权威快照时，才发布 target。
///
/// `None` 表示镜像已换代；调用方必须重读 Claim 后再试，不能让旧请求倒写较新的
/// retrieval document / queue / VectorState。
pub(crate) async fn ensure_retrieval_target_for_claim_snapshot(
    team_root: &Path,
    source_claim_path: &Path,
    source_claim: &Claim,
    retrieval_doc: &RetrievalDocument,
    embedding_fingerprint: Option<&EmbeddingCacheFingerprint>,
) -> anyhow::Result<Option<RetrievalTargetEnsure>> {
    if source_claim.id != retrieval_doc.claim_id {
        anyhow::bail!(
            "Claim 镜像与 retrieval document 的 id 不一致: claim={} document={}",
            source_claim.id,
            retrieval_doc.claim_id
        );
    }
    let mirror_lock_path = paths::team_store_agent_claim_mirror_lock_path(
        team_root,
        &source_claim.holder,
        &source_claim.id,
    );
    // 所有 Claim 镜像写入都持有这把锁；因此从下方复核到 target 发布的整段
    // 临界区内，不会有新 Claim 在同一路径上插入并被旧快照反向覆盖。
    let _mirror_guard = FileLockGuard::lock_exclusive(&mirror_lock_path)
        .await
        .with_context(|| format!("获取 Claim 镜像 source snapshot 锁失败: {mirror_lock_path:?}"))?;
    let _target_guard = lock_retrieval_target(team_root, &retrieval_doc.claim_id).await?;
    let current_claim: Claim = read_yaml(source_claim_path)
        .await
        .with_context(|| format!("持锁读取 claim 权威镜像失败: {source_claim_path:?}"))?;
    if current_claim.id != source_claim.id {
        anyhow::bail!(
            "Claim 镜像路径对应的 id 在读取期间变化: expected={} actual={}",
            source_claim.id,
            current_claim.id
        );
    }
    if current_claim != *source_claim {
        return Ok(None);
    }
    ensure_retrieval_target_locked(team_root, retrieval_doc, embedding_fingerprint)
        .await
        .map(Some)
}

/// 调用方已持有对应 claim 的 retrieval target lock。
async fn ensure_retrieval_target_locked(
    team_root: &Path,
    retrieval_doc: &RetrievalDocument,
    embedding_fingerprint: Option<&EmbeddingCacheFingerprint>,
) -> anyhow::Result<RetrievalTargetEnsure> {
    let path = paths::team_store_router_retrieval_doc_path(team_root, &retrieval_doc.claim_id);
    let existing_retrieval_doc = if tokio::fs::try_exists(&path).await.unwrap_or(false) {
        // 损坏的派生文档可由当前权威 Claim 快照安全重建，沿用原有刷新语义。
        (read_yaml::<RetrievalDocument>(&path).await).ok()
    } else {
        None
    };
    let needs_refresh = existing_retrieval_doc
        .as_ref()
        .is_none_or(|existing| existing != retrieval_doc);
    let target_intent = match embedding_fingerprint {
        Some(fingerprint) => Some(VectorTargetIntent::from_retrieval_doc(
            retrieval_doc,
            fingerprint.clone(),
        )),
        None if needs_refresh => {
            inherit_vector_target_intent_for_lexical_refresh(
                team_root,
                retrieval_doc,
                existing_retrieval_doc.as_ref(),
            )
            .await
        }
        None => None,
    };
    if needs_refresh {
        if let Some(intent) = target_intent.as_ref() {
            if let Err(error) = write_vector_target_intent(team_root, intent).await {
                return Ok(RetrievalTargetEnsure {
                    vector_state: None,
                    vector_error: Some(error.context("持久化 Vector target 恢复意图失败")),
                });
            }
        }
        write_yaml_atomic(&path, retrieval_doc)
            .await
            .with_context(|| format!("持锁刷新 claim 检索文档失败: {path:?}"))?;
    }

    let Some(embedding_fingerprint) = embedding_fingerprint else {
        // 没有 embedding client 可能只是本次 lexical-only 查询，不能删除尚待 worker 重放的
        // intent；永久停用 Vector 的清理必须由显式迁移/运维操作承担。
        return Ok(RetrievalTargetEnsure {
            vector_state: None,
            vector_error: None,
        });
    };
    match ensure_vector_pending_locked(
        team_root,
        retrieval_doc,
        embedding_fingerprint,
        Utc::now(),
        target_intent.as_ref(),
    )
    .await
    {
        Ok(state) => {
            if let Err(error) =
                remove_vector_target_intent(team_root, &retrieval_doc.claim_id).await
            {
                // state 与 queue 已经各自落盘，残留 intent 只会触发一次幂等清理，不能反过来
                // 把已可用的 Vector 召回降级为失败。
                log::warn!(
                    target: "router_vector",
                    "建立 Vector target 后清理恢复意图失败 claim_id={}: {error:#}",
                    retrieval_doc.claim_id
                );
            }
            Ok(RetrievalTargetEnsure {
                vector_state: Some(state),
                vector_error: None,
            })
        }
        Err(error) => {
            let vector_error = match target_intent.as_ref() {
                Some(intent) => match write_vector_target_intent(team_root, intent).await {
                    Ok(()) => error.context("建立 claim Vector target 失败；已保留恢复意图"),
                    Err(intent_error) => anyhow::anyhow!(
                        "建立 claim Vector target 失败: {error:#}; 同时持久化恢复意图失败: {intent_error:#}"
                    ),
                },
                None => error.context("建立 claim Vector target 失败"),
            };
            Ok(RetrievalTargetEnsure {
                vector_state: None,
                vector_error: Some(vector_error),
            })
        }
    }
}

/// 在仅 lexical 的文档换代中，继承已经可证明属于旧文档的 embedding 指纹。
///
/// Router 本身可能没有 embedding client，但同一团队目录的 worker 仍会消费队列。此时若
/// 直接把 A 文档改为 B，原本 A 的 intent 会被 worker 当作过期内容清理，B 就永远失去
/// 恢复锚点。只接受与**当前旧文档**精确匹配的 intent/state，才把该指纹迁移到 B；不能从
/// 不匹配的旧记录猜测 B 的新 target。
async fn inherit_vector_target_intent_for_lexical_refresh(
    team_root: &Path,
    retrieval_doc: &RetrievalDocument,
    existing_retrieval_doc: Option<&RetrievalDocument>,
) -> Option<VectorTargetIntent> {
    let existing_retrieval_doc = existing_retrieval_doc?;
    if existing_retrieval_doc.claim_id != retrieval_doc.claim_id {
        return None;
    }
    let existing_content_hash = search_text_hash(&existing_retrieval_doc.search_text);

    match load_vector_target_intent_opt(team_root, &retrieval_doc.claim_id).await {
        Ok(Some(intent))
            if intent.claim_id == retrieval_doc.claim_id
                && intent.content_hash == existing_content_hash =>
        {
            return Some(VectorTargetIntent::from_retrieval_doc(
                retrieval_doc,
                intent.embedding_fingerprint,
            ));
        }
        Ok(_) => {}
        Err(error) => {
            // 这只是 lexical 召回的可选恢复线索，坏掉的旧 intent 不能阻断当前 Claim 的可见性；
            // 若下面 state 也无法证明 target，则交给下一次带 embedding 的 query 显式重建。
            log::warn!(
                target: "router_vector",
                "读取既有 Vector target intent 失败，无法从 intent 继承 lexical 文档换代 claim_id={}: {error:#}",
                retrieval_doc.claim_id
            );
        }
    }

    let state = match load_vector_state_opt(team_root, &retrieval_doc.claim_id).await {
        Ok(state) => state,
        Err(error) => {
            log::warn!(
                target: "router_vector",
                "读取既有 Vector state 失败，无法从 state 继承 lexical 文档换代 claim_id={}: {error:#}",
                retrieval_doc.claim_id
            );
            return None;
        }
    }?;
    if state.claim_id != retrieval_doc.claim_id || state.content_hash != existing_content_hash {
        return None;
    }
    state
        .embedding_fingerprint
        .map(|fingerprint| VectorTargetIntent::from_retrieval_doc(retrieval_doc, fingerprint))
}

/// 为给定检索文档建立或复用待处理的 Vector target。
///
/// 兼容既有调用方的低层 state/queue 建立入口。
///
/// 它不会发布或覆盖 retrieval document；调用方若需要更新文档，必须走带 Claim 快照校验的
/// Router 路径。没有 retrieval document 时保留既有兼容写入；一旦已有 document，传入内容
/// 必须精确匹配，避免过时调用把当前 target 的 state/queue 覆盖成旧内容代。
pub async fn ensure_vector_pending(
    team_root: &Path,
    retrieval_doc: &RetrievalDocument,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
) -> anyhow::Result<VectorState> {
    ensure_vector_pending_at(team_root, retrieval_doc, embedding_fingerprint, Utc::now()).await
}

async fn ensure_vector_pending_at(
    team_root: &Path,
    retrieval_doc: &RetrievalDocument,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    now: DateTime<Utc>,
) -> anyhow::Result<VectorState> {
    let _target_guard = lock_retrieval_target(team_root, &retrieval_doc.claim_id).await?;
    let content_hash = search_text_hash(&retrieval_doc.search_text);
    ensure_compatible_target_matches_current_document(
        team_root,
        &retrieval_doc.claim_id,
        &content_hash,
    )
    .await?;
    ensure_vector_pending_locked(team_root, retrieval_doc, embedding_fingerprint, now, None).await
}

/// 在保留的公开 Vector helper 写入前，防止旧调用越过当前 retrieval document 的内容代。
///
/// 没有 document 的旧用法仍可创建 state；但已有 document 时，兼容 helper 不能自行决定换代，
/// 只能由 Router 的 Claim snapshot 发布路径完成。
async fn ensure_compatible_target_matches_current_document(
    team_root: &Path,
    claim_id: &ClaimId,
    content_hash: &str,
) -> anyhow::Result<()> {
    let path = paths::team_store_router_retrieval_doc_path(team_root, claim_id);
    match tokio::fs::try_exists(&path).await {
        Ok(false) => return Ok(()),
        Ok(true) => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("检查当前 retrieval document 失败: {path:?}"));
        }
    }
    let current: RetrievalDocument = read_yaml(&path)
        .await
        .with_context(|| format!("读取当前 retrieval document 失败: {path:?}"))?;
    let current_hash = search_text_hash(&current.search_text);
    if current.claim_id != *claim_id || current_hash != content_hash {
        anyhow::bail!(
            "兼容 Vector 写入与当前 retrieval document 不一致: claim_id={} document_claim_id={} expected_content_hash={} actual_content_hash={}",
            claim_id,
            current.claim_id,
            content_hash,
            current_hash
        );
    }
    Ok(())
}

/// 调用方已持有对应 claim 的 retrieval target lock。
async fn ensure_vector_pending_locked(
    team_root: &Path,
    retrieval_doc: &RetrievalDocument,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    now: DateTime<Utc>,
    target_intent: Option<&VectorTargetIntent>,
) -> anyhow::Result<VectorState> {
    let content_hash = search_text_hash(&retrieval_doc.search_text);
    let previous = load_vector_state_opt(team_root, &retrieval_doc.claim_id).await?;
    if let Some(state) = previous.as_ref() {
        if vector_state_matches(state, &content_hash, embedding_fingerprint) {
            match state.status {
                VectorStatus::Ready => return Ok(state.clone()),
                VectorStatus::Pending => {
                    if !has_live_queue_entry(
                        team_root,
                        &retrieval_doc.claim_id,
                        &content_hash,
                        embedding_fingerprint,
                        state.expected_dimensions,
                        state.generation_seq,
                    )
                    .await?
                    {
                        persist_vector_target_intent(team_root, target_intent).await?;
                        enqueue_vector_pending_copy_with_dimensions(
                            team_root,
                            &retrieval_doc.claim_id,
                            &content_hash,
                            embedding_fingerprint,
                            state.expected_dimensions,
                            state.generation_seq,
                        )
                        .await?;
                    }
                    return Ok(state.clone());
                }
                VectorStatus::Failed
                    if state.next_retry_at.is_some_and(|retry_at| retry_at > now) =>
                {
                    return Ok(state.clone());
                }
                VectorStatus::Failed => {
                    let mut pending = state.clone();
                    pending.status = VectorStatus::Pending;
                    pending.updated_at = now;
                    pending.next_retry_at = None;
                    pending.error_summary = None;
                    persist_vector_target_intent(team_root, target_intent).await?;
                    enqueue_vector_pending_copy_with_dimensions(
                        team_root,
                        &retrieval_doc.claim_id,
                        &content_hash,
                        embedding_fingerprint,
                        pending.expected_dimensions,
                        pending.generation_seq,
                    )
                    .await?;
                    write_vector_state(team_root, &pending).await?;
                    return Ok(pending);
                }
            }
        }
    }

    let current_generation_seq = previous.as_ref().map_or(0, |state| state.generation_seq);
    persist_vector_target_intent(team_root, target_intent).await?;
    let generation_seq = enqueue_new_vector_generation(
        team_root,
        &retrieval_doc.claim_id,
        &content_hash,
        embedding_fingerprint,
        None,
        current_generation_seq,
    )
    .await?;
    let state = VectorState::pending(
        retrieval_doc.claim_id.clone(),
        content_hash,
        embedding_fingerprint.clone(),
        None,
        generation_seq,
    );
    write_vector_state(team_root, &state).await?;
    Ok(state)
}

/// 写入失败 Vector state 的兼容入口。
///
/// 该低层入口不发布 retrieval document；已有 document 时只允许写入同一内容代，避免旧调用
/// 覆盖当前 target。document 缺失时仍保留旧调用方可直接落 state 的兼容性。
pub async fn store_failed_vector_state(
    team_root: &Path,
    claim_id: &ClaimId,
    content_hash: String,
    embedding_fingerprint: EmbeddingCacheFingerprint,
    error_summary: String,
    retry_policy: VectorRetryPolicy,
) -> anyhow::Result<VectorState> {
    let _target_guard = lock_retrieval_target(team_root, claim_id).await?;
    ensure_compatible_target_matches_current_document(team_root, claim_id, &content_hash).await?;
    let now = Utc::now();
    let previous = load_vector_state_opt(team_root, claim_id).await?;
    let matching = previous
        .as_ref()
        .filter(|state| vector_state_matches(state, &content_hash, &embedding_fingerprint));
    let attempts = matching.map_or(1, |state| state.attempts.saturating_add(1));
    let expected_dimensions = matching.and_then(|state| state.expected_dimensions);
    let generation_seq = if let Some(state) = matching {
        state.generation_seq
    } else {
        reserve_new_vector_generation(
            team_root,
            claim_id,
            previous.as_ref().map_or(0, |state| state.generation_seq),
        )
        .await?
    };
    let state = VectorState::failed(
        claim_id.clone(),
        content_hash,
        embedding_fingerprint,
        expected_dimensions,
        generation_seq,
        VectorAttemptMetadata::failed(attempts, now, retry_policy.retry_at(attempts, now)),
        error_summary,
    );
    write_vector_state(team_root, &state).await?;
    Ok(state)
}

/// 写入就绪 Vector state 的兼容入口。
///
/// 该低层入口不发布 retrieval document；已有 document 时只允许写入同一内容代，避免旧调用
/// 覆盖当前 target。document 缺失时仍保留旧调用方可直接落 state 的兼容性。
pub async fn store_ready_vector_state(
    team_root: &Path,
    claim_id: &ClaimId,
    content_hash: String,
    embedding_fingerprint: EmbeddingCacheFingerprint,
    vector: Vec<f32>,
) -> anyhow::Result<VectorState> {
    validate_embedding_vector(&vector)?;
    let _target_guard = lock_retrieval_target(team_root, claim_id).await?;
    ensure_compatible_target_matches_current_document(team_root, claim_id, &content_hash).await?;
    let now = Utc::now();
    let previous = load_vector_state_opt(team_root, claim_id).await?;
    let matching_content = previous
        .as_ref()
        .filter(|state| vector_state_matches(state, &content_hash, &embedding_fingerprint));
    let expected_dimensions = matching_content
        .and_then(|state| state.expected_dimensions)
        .or(Some(vector.len()));
    if expected_dimensions != Some(vector.len()) {
        anyhow::bail!(
            "待写入 embedding 维度与当前 generation 不一致: expected={} actual={}",
            expected_dimensions.unwrap_or_default(),
            vector.len()
        );
    }
    let matching_target =
        matching_content.filter(|state| state.expected_dimensions == expected_dimensions);
    let attempts = matching_target.map_or(1, |state| state.attempts.saturating_add(1));
    let generation_seq = if let Some(state) = matching_target {
        state.generation_seq
    } else {
        reserve_new_vector_generation(
            team_root,
            claim_id,
            previous.as_ref().map_or(0, |state| state.generation_seq),
        )
        .await?
    };
    let state = VectorState::ready(
        claim_id.clone(),
        content_hash,
        embedding_fingerprint,
        expected_dimensions,
        generation_seq,
        VectorAttemptMetadata::completed(attempts, now),
        vector,
    );
    write_vector_state(team_root, &state).await?;
    Ok(state)
}

/// 搜索 fingerprint 与 query 向量匹配的就绪 Vector state。
///
/// 保留既有签名，但会逐条重读当前 retrieval document 做内容 fence；Router 查询使用内部
/// snapshot fence，以避免查询期间 Claim 换代时把“当前文档”错误地替换为另一版快照。
pub async fn search_ready_vectors(
    team_root: &Path,
    query_vector: &[f32],
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    limit: usize,
) -> anyhow::Result<Vec<VectorHit>> {
    search_ready_vectors_inner(team_root, query_vector, embedding_fingerprint, None, limit).await
}

/// 只搜索本次 query 快照的确切内容，其他内容代不会占 top_m 或触发修复副作用。
pub(crate) async fn search_ready_vectors_for_claims(
    team_root: &Path,
    query_vector: &[f32],
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_content_hashes: &FxHashMap<ClaimId, String>,
    limit: usize,
) -> anyhow::Result<Vec<VectorHit>> {
    search_ready_vectors_inner(
        team_root,
        query_vector,
        embedding_fingerprint,
        Some(expected_content_hashes),
        limit,
    )
    .await
}

async fn search_ready_vectors_inner(
    team_root: &Path,
    query_vector: &[f32],
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_content_hashes: Option<&FxHashMap<ClaimId, String>>,
    limit: usize,
) -> anyhow::Result<Vec<VectorHit>> {
    validate_embedding_vector(query_vector).context("查询 embedding 无效")?;
    let dir = paths::team_store_router_vector_state_dir(team_root);
    if !tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(Vec::new());
    }

    let mut hits = Vec::new();
    let mut rd = tokio::fs::read_dir(&dir)
        .await
        .with_context(|| format!("读取向量状态目录失败: {dir:?}"))?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(".json") || name.contains(".tmp.") {
            continue;
        }
        let raw = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("读取向量状态文件失败: {path:?}"))?;
        let state: VectorState =
            serde_json::from_str(&raw).with_context(|| format!("解析向量状态失败: {path:?}"))?;
        if !is_current_search_text_hash(&state.content_hash) {
            // 旧 DefaultHasher state 没有可验证的内容代关系；既不能返回，也不能借维度修复重写它。
            continue;
        }
        let expected_content_hash = match expected_content_hashes {
            Some(expected_content_hashes) => {
                let Some(expected_content_hash) = expected_content_hashes.get(&state.claim_id)
                else {
                    continue;
                };
                if state.content_hash != *expected_content_hash {
                    // 旧 query 只能把另一内容代视为 miss，不能据自身维度反向修复它。
                    continue;
                }
                expected_content_hash.as_str()
            }
            None => {
                let retrieval_doc_path =
                    paths::team_store_router_retrieval_doc_path(team_root, &state.claim_id);
                let retrieval_doc: RetrievalDocument = match read_yaml(&retrieval_doc_path).await {
                    Ok(document) => document,
                    Err(error) => {
                        log::debug!(
                            target: "router_vector",
                            "跳过无法验证当前 retrieval document 的公开 Vector 搜索 state claim_id={}: {error:#}",
                            state.claim_id
                        );
                        continue;
                    }
                };
                if retrieval_doc.claim_id != state.claim_id
                    || search_text_hash(&retrieval_doc.search_text) != state.content_hash
                {
                    // 公开入口没有调用方持有的 Claim 快照，必须以当前镜像派生的 document 为准。
                    continue;
                }
                state.content_hash.as_str()
            }
        };
        if state.embedding_fingerprint.as_ref() != Some(embedding_fingerprint) {
            continue;
        }
        if state.status != VectorStatus::Ready {
            if state.expected_dimensions != Some(query_vector.len()) {
                requeue_expected_dimension_change(
                    team_root,
                    &state,
                    embedding_fingerprint,
                    expected_content_hash,
                    query_vector.len(),
                )
                .await?;
            }
            continue;
        }
        let Some(vector) = state.vector.as_ref() else {
            requeue_dimension_mismatch(
                team_root,
                &state,
                embedding_fingerprint,
                expected_content_hash,
                query_vector.len(),
            )
            .await?;
            continue;
        };
        if state.vector_dimensions != Some(vector.len())
            || state.vector_dimensions != Some(query_vector.len())
        {
            requeue_dimension_mismatch(
                team_root,
                &state,
                embedding_fingerprint,
                expected_content_hash,
                query_vector.len(),
            )
            .await?;
            continue;
        }
        let Some(score) = cosine_similarity(query_vector, vector) else {
            continue;
        };
        if score == 0 {
            continue;
        }
        hits.push(VectorHit {
            claim_id: state.claim_id,
            score,
        });
    }

    hits.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.claim_id.as_str().cmp(b.claim_id.as_str()))
    });
    hits.truncate(limit);
    Ok(hits)
}

async fn requeue_expected_dimension_change(
    team_root: &Path,
    snapshot: &VectorState,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_content_hash: &str,
    query_dimensions: usize,
) -> anyhow::Result<()> {
    let _target_guard = lock_retrieval_target(team_root, &snapshot.claim_id).await?;
    let retrieval_doc_path =
        paths::team_store_router_retrieval_doc_path(team_root, &snapshot.claim_id);
    let retrieval_doc: RetrievalDocument = read_yaml(&retrieval_doc_path)
        .await
        .with_context(|| format!("读取维度变化 claim 检索文档失败: {retrieval_doc_path:?}"))?;
    if search_text_hash(&retrieval_doc.search_text) != expected_content_hash {
        return Ok(());
    }
    let Some(current) = load_vector_state_opt(team_root, &snapshot.claim_id).await? else {
        return Ok(());
    };
    if current.status == VectorStatus::Ready
        || current.content_hash != expected_content_hash
        || current.embedding_fingerprint.as_ref() != Some(embedding_fingerprint)
        || current.expected_dimensions == Some(query_dimensions)
    {
        return Ok(());
    }

    let now = Utc::now();
    let had_known_dimensions = current.expected_dimensions.is_some();
    let should_enqueue = had_known_dimensions
        || match current.status {
            VectorStatus::Pending => true,
            VectorStatus::Failed => current.next_retry_at.is_none_or(|retry_at| retry_at <= now),
            VectorStatus::Ready => false,
        };
    let generation_seq = if should_enqueue {
        enqueue_new_vector_generation(
            team_root,
            &current.claim_id,
            expected_content_hash,
            embedding_fingerprint,
            Some(query_dimensions),
            current.generation_seq,
        )
        .await?
    } else {
        reserve_new_vector_generation(team_root, &current.claim_id, current.generation_seq).await?
    };

    let mut aligned = if had_known_dimensions {
        // 已知维度发生变化代表新的 generation；沿用既有语义重置旧维度的失败退避。
        VectorState::pending(
            current.claim_id.clone(),
            expected_content_hash.to_owned(),
            embedding_fingerprint.clone(),
            Some(query_dimensions),
            generation_seq,
        )
    } else {
        current
    };
    aligned.generation_seq = generation_seq;
    aligned.expected_dimensions = Some(query_dimensions);
    aligned.updated_at = now;
    if should_enqueue {
        aligned.status = VectorStatus::Pending;
        aligned.next_retry_at = None;
        aligned.error_summary = None;
    }
    write_vector_state(team_root, &aligned).await
}

async fn requeue_dimension_mismatch(
    team_root: &Path,
    snapshot: &VectorState,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_content_hash: &str,
    query_dimensions: usize,
) -> anyhow::Result<()> {
    let _target_guard = lock_retrieval_target(team_root, &snapshot.claim_id).await?;
    let retrieval_doc_path =
        paths::team_store_router_retrieval_doc_path(team_root, &snapshot.claim_id);
    let retrieval_doc: RetrievalDocument = read_yaml(&retrieval_doc_path)
        .await
        .with_context(|| format!("读取维度失配 claim 检索文档失败: {retrieval_doc_path:?}"))?;
    if search_text_hash(&retrieval_doc.search_text) != expected_content_hash {
        return Ok(());
    }
    let Some(current) = load_vector_state_opt(team_root, &snapshot.claim_id).await? else {
        return Ok(());
    };
    if current.status != VectorStatus::Ready
        || current.content_hash != expected_content_hash
        || current.embedding_fingerprint.as_ref() != Some(embedding_fingerprint)
    {
        return Ok(());
    }
    let actual_dimensions = current.vector.as_ref().map(Vec::len);
    if current.vector_dimensions == actual_dimensions
        && current.vector_dimensions == Some(query_dimensions)
    {
        return Ok(());
    }

    let generation_seq = enqueue_new_vector_generation(
        team_root,
        &current.claim_id,
        expected_content_hash,
        embedding_fingerprint,
        Some(query_dimensions),
        current.generation_seq,
    )
    .await?;
    let pending = VectorState::pending(
        current.claim_id,
        expected_content_hash.to_owned(),
        embedding_fingerprint.clone(),
        Some(query_dimensions),
        generation_seq,
    );
    write_vector_state(team_root, &pending).await
}

/// 重放已落盘但尚未完成 state/queue 建立的 Vector target 意图。
///
/// 只接受与当前 retrieval document 完全吻合的 intent；因此旧内容代的残留文件会被
/// 清理，而不会由 worker 从新正文猜测 successor。
async fn recover_vector_target_intents(
    team_root: &Path,
    worker_fingerprint: &EmbeddingCacheFingerprint,
) -> anyhow::Result<usize> {
    let intents_dir = paths::team_store_router_vector_intents_dir(team_root);
    match tokio::fs::try_exists(&intents_dir).await {
        Ok(true) => {}
        Ok(false) => return Ok(0),
        Err(error) => {
            log::warn!(
                target: "router_vector",
                "检查 Vector target 恢复意图目录失败，将在下轮重试 path={intents_dir:?}: {error:#}"
            );
            return Ok(0);
        }
    }

    let mut intent_paths = Vec::new();
    let mut directory = tokio::fs::read_dir(&intents_dir)
        .await
        .with_context(|| format!("读取 Vector target 恢复意图目录失败: {intents_dir:?}"))?;
    while let Some(entry) = directory.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".json") && !name.contains(".tmp.") {
            intent_paths.push(path);
        }
    }
    intent_paths.sort();

    let mut recovered = 0;
    for intent_path in intent_paths {
        let initial_intent = match read_vector_target_intent(&intent_path).await {
            Ok(intent) => intent,
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "跳过无法读取的 Vector target 恢复意图 path={intent_path:?}: {error:#}"
                );
                continue;
            }
        };
        let expected_intent_path =
            paths::team_store_router_vector_intent_path(team_root, &initial_intent.claim_id);
        if expected_intent_path != intent_path {
            log::warn!(
                target: "router_vector",
                "丢弃路径与 claim_id 不一致的 Vector target 恢复意图 path={intent_path:?} claim_id={}",
                initial_intent.claim_id
            );
            if let Err(error) = remove_vector_target_intent_path(&intent_path).await {
                log::warn!(
                    target: "router_vector",
                    "清理异常 Vector target 恢复意图失败 path={intent_path:?}: {error:#}"
                );
            }
            continue;
        }

        let _target_guard = match lock_retrieval_target(team_root, &initial_intent.claim_id).await {
            Ok(guard) => guard,
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "获取 intent recovery claim 锁失败 claim_id={}: {error:#}",
                    initial_intent.claim_id
                );
                continue;
            }
        };
        match tokio::fs::try_exists(&expected_intent_path).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "检查持锁后的 Vector target 恢复意图失败，将在下轮重试 path={expected_intent_path:?}: {error:#}"
                );
                continue;
            }
        }
        let intent = match read_vector_target_intent(&expected_intent_path).await {
            Ok(intent) => intent,
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "持锁重读 Vector target 恢复意图失败 claim_id={}: {error:#}",
                    initial_intent.claim_id
                );
                continue;
            }
        };
        if intent.claim_id != initial_intent.claim_id {
            log::warn!(
                target: "router_vector",
                "丢弃持锁期间 claim_id 被替换的 Vector target 恢复意图 path={expected_intent_path:?} locked_claim_id={} actual_claim_id={}",
                initial_intent.claim_id,
                intent.claim_id
            );
            if let Err(error) = remove_vector_target_intent_path(&expected_intent_path).await {
                log::warn!(
                    target: "router_vector",
                    "清理 claim_id 异常的 Vector target 恢复意图失败 path={expected_intent_path:?}: {error:#}"
                );
            }
            continue;
        }
        let retrieval_doc_path =
            paths::team_store_router_retrieval_doc_path(team_root, &intent.claim_id);
        match tokio::fs::try_exists(&retrieval_doc_path).await {
            Ok(true) => {}
            Ok(false) => {
                if let Err(error) = remove_vector_target_intent(team_root, &intent.claim_id).await {
                    log::warn!(
                        target: "router_vector",
                        "清理没有检索文档的 Vector target 恢复意图失败 claim_id={}: {error:#}",
                        intent.claim_id
                    );
                }
                continue;
            }
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "检查 intent 对应的检索文档失败，保留以便重试 path={retrieval_doc_path:?}: {error:#}"
                );
                continue;
            }
        }
        let retrieval_doc: RetrievalDocument = match read_yaml(&retrieval_doc_path).await {
            Ok(document) => document,
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "读取 intent 对应的检索文档失败，保留以便重试 claim_id={}: {error:#}",
                    intent.claim_id
                );
                continue;
            }
        };
        if retrieval_doc.claim_id != intent.claim_id
            || search_text_hash(&retrieval_doc.search_text) != intent.content_hash
        {
            log::debug!(
                target: "router_vector",
                "丢弃与当前检索文档不匹配的 Vector target 恢复意图 claim_id={}",
                intent.claim_id
            );
            if let Err(error) = remove_vector_target_intent(team_root, &intent.claim_id).await {
                log::warn!(
                    target: "router_vector",
                    "清理过期 Vector target 恢复意图失败 claim_id={}: {error:#}",
                    intent.claim_id
                );
            }
            continue;
        }
        if intent.embedding_fingerprint != *worker_fingerprint {
            // 当前 worker 不能把一份明确归属其他 embedding 配置的 intent 重写为自身配置。
            continue;
        }
        match ensure_vector_pending_locked(
            team_root,
            &retrieval_doc,
            worker_fingerprint,
            Utc::now(),
            Some(&intent),
        )
        .await
        {
            Ok(_) => {
                if let Err(error) = remove_vector_target_intent(team_root, &intent.claim_id).await {
                    log::warn!(
                        target: "router_vector",
                        "重放 Vector target 后清理恢复意图失败 claim_id={}: {error:#}",
                        intent.claim_id
                    );
                } else {
                    recovered += 1;
                }
            }
            Err(error) => {
                log::warn!(
                    target: "router_vector",
                    "重放 Vector target 恢复意图失败，将在下轮重试 claim_id={}: {error:#}",
                    intent.claim_id
                );
            }
        }
    }
    Ok(recovered)
}

pub async fn process_pending_queue(
    team_root: PathBuf,
    embedding_client: Arc<dyn EmbeddingClient>,
    max_concurrency: usize,
    retry_policy: VectorRetryPolicy,
) -> anyhow::Result<VectorProcessReport> {
    let recovered_targets =
        recover_vector_target_intents(&team_root, &embedding_client.cache_fingerprint()).await?;
    if recovered_targets > 0 {
        log::info!(
            target: "router_vector",
            "已重放未完成的 Vector target intents={recovered_targets}"
        );
    }
    let Some(drained) = drain_queue_entries(&team_root).await? else {
        return Ok(VectorProcessReport::default());
    };
    let entries = drained.entries.clone();
    if entries.is_empty() {
        finish_in_flight_queue(&team_root, drained, &[]).await?;
        return Ok(VectorProcessReport::default());
    }

    let entries_len = entries.len();
    let results = stream::iter(entries.into_iter().map(|entry| {
        let team_root = team_root.clone();
        let embedding_client = embedding_client.clone();
        let retry_entry = entry.clone();
        async move {
            (
                retry_entry,
                process_queue_entry(&team_root, embedding_client, entry, retry_policy).await,
            )
        }
    }))
    .buffer_unordered(max_concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut report = VectorProcessReport {
        processed: entries_len,
        failures: 0,
    };
    let mut retry_entries = Vec::new();
    for (entry, result) in results {
        match result {
            Ok(QueueEntryOutcome::Complete) => {}
            Ok(QueueEntryOutcome::RequeueForMatchingWorker) => retry_entries.push(entry),
            Ok(QueueEntryOutcome::EmbeddingFailureRecorded(summary)) => {
                report.failures += 1;
                log::warn!(target: "router_vector", "生成 claim embedding 失败: {summary}");
            }
            Err(err) => {
                report.failures += 1;
                log::warn!(target: "router_vector", "处理向量队列失败，将重新入队: {err:#}");
                retry_entries.push(entry);
            }
        }
    }
    finish_in_flight_queue(&team_root, drained, &retry_entries).await?;
    Ok(report)
}

pub async fn run_vector_worker(
    team_root: PathBuf,
    embedding_client: Arc<dyn EmbeddingClient>,
    max_concurrency: usize,
    poll_interval: Duration,
    retry_policy: VectorRetryPolicy,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        match process_pending_queue(
            team_root.clone(),
            embedding_client.clone(),
            max_concurrency,
            retry_policy,
        )
        .await
        {
            Ok(report) if report.processed > 0 => {
                log::info!(
                    target: "router_vector",
                    "router 向量队列处理完成 processed={} failures={}",
                    report.processed,
                    report.failures
                );
            }
            Ok(_) => {}
            Err(error) => {
                // 队列或 intent 存储可能只是短暂不可用；退出会让已落盘 intent 永远无人重放。
                log::warn!(target: "router_vector", "消费 router 向量队列失败，将在下轮重试: {error:#}");
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
}

async fn process_queue_entry(
    team_root: &Path,
    embedding_client: Arc<dyn EmbeddingClient>,
    entry: VectorQueueEntry,
    retry_policy: VectorRetryPolicy,
) -> anyhow::Result<QueueEntryOutcome> {
    let embedding_fingerprint = embedding_client.cache_fingerprint();
    let (retrieval_doc, actual_hash) = {
        let _target_guard = lock_retrieval_target(team_root, &entry.claim_id).await?;
        let retrieval_doc_path =
            paths::team_store_router_retrieval_doc_path(team_root, &entry.claim_id);
        let retrieval_doc: RetrievalDocument = read_yaml(&retrieval_doc_path)
            .await
            .with_context(|| format!("持锁读取检索文档失败: {retrieval_doc_path:?}"))?;
        let actual_hash = search_text_hash(&retrieval_doc.search_text);
        if actual_hash != entry.content_hash {
            // 旧 entry 不能从当前 document 猜测新的 content target；新 generation
            // 只能由 query 的 document + Vector 组合 publication 建立。
            return Ok(QueueEntryOutcome::Complete);
        }

        let state = load_vector_state_opt(team_root, &entry.claim_id).await?;
        if entry.embedding_fingerprint.as_ref() != Some(&embedding_fingerprint) {
            if let Some(state) = state.as_ref() {
                if state.generation_seq > entry.generation_seq
                    || (state.generation_seq == entry.generation_seq
                        && !vector_state_target_matches_queue_entry(state, &entry))
                {
                    return Ok(QueueEntryOutcome::Complete);
                }
            }
            // 队列项属于另一组 embedding 配置时，当前 worker 不能把权威 generation
            // 翻写成自身配置；保留任务，交给 fingerprint 匹配的 worker 或后续 query 失效它。
            return Ok(QueueEntryOutcome::RequeueForMatchingWorker);
        }

        let mut state = match state {
            Some(state) => state,
            None => {
                let pending = VectorState::pending(
                    entry.claim_id.clone(),
                    actual_hash.clone(),
                    embedding_fingerprint.clone(),
                    entry.expected_dimensions,
                    entry.generation_seq,
                );
                write_vector_state(team_root, &pending).await?;
                pending
            }
        };
        if state.generation_seq > entry.generation_seq {
            return Ok(QueueEntryOutcome::Complete);
        }
        if state.generation_seq == entry.generation_seq
            && !vector_state_target_matches_queue_entry(&state, &entry)
        {
            // generation=0 的旧记录若 target 冲突已无法安全判序；宁可等待 query
            // 建立新 generation，也不能用时间戳猜测并回滚当前状态。
            return Ok(QueueEntryOutcome::Complete);
        }
        if state.generation_seq < entry.generation_seq {
            let binds_unknown_dimension = state.status != VectorStatus::Ready
                && state.content_hash == entry.content_hash
                && state.embedding_fingerprint == entry.embedding_fingerprint
                && state.expected_dimensions.is_none()
                && entry.expected_dimensions.is_some();
            if binds_unknown_dimension {
                // None -> Some 只补全同一派生目标的维度约束；保留失败次数与
                // 最近尝试时间，但新 generation 必须 fence 仍在运行的旧 worker。
                let now = Utc::now();
                state.updated_at = now;
                state.generation_seq = entry.generation_seq;
                state.expected_dimensions = entry.expected_dimensions;
                if state.status == VectorStatus::Failed
                    && state.next_retry_at.is_some_and(|retry_at| retry_at > now)
                {
                    write_vector_state(team_root, &state).await?;
                    return Ok(QueueEntryOutcome::Complete);
                }
                state.status = VectorStatus::Pending;
                state.vector_dimensions = None;
                state.next_retry_at = None;
                state.vector = None;
                state.error_summary = None;
            } else {
                state = VectorState::pending(
                    entry.claim_id.clone(),
                    actual_hash.clone(),
                    embedding_fingerprint.clone(),
                    entry.expected_dimensions,
                    entry.generation_seq,
                );
            }
            write_vector_state(team_root, &state).await?;
        }
        if state.status == VectorStatus::Ready {
            let ready_dimensions = state.vector.as_ref().map(Vec::len);
            if entry.expected_dimensions.is_some_and(|expected| {
                state.vector_dimensions != Some(expected) || ready_dimensions != Some(expected)
            }) {
                let pending = VectorState::pending(
                    entry.claim_id.clone(),
                    actual_hash.clone(),
                    embedding_fingerprint.clone(),
                    entry.expected_dimensions,
                    entry.generation_seq,
                );
                write_vector_state(team_root, &pending).await?;
            } else {
                return Ok(QueueEntryOutcome::Complete);
            }
        }
        if state.status == VectorStatus::Failed
            && state
                .next_retry_at
                .is_some_and(|retry_at| retry_at > Utc::now())
        {
            return Ok(QueueEntryOutcome::Complete);
        }
        (retrieval_doc, actual_hash)
    };

    let embedding_result = embedding_client.embed(&retrieval_doc.search_text).await;
    let completed_at = Utc::now();
    let _target_guard = lock_retrieval_target(team_root, &entry.claim_id).await?;
    let retrieval_doc_path =
        paths::team_store_router_retrieval_doc_path(team_root, &entry.claim_id);
    let current_doc: RetrievalDocument = read_yaml(&retrieval_doc_path)
        .await
        .with_context(|| format!("embedding 完成后持锁重读检索文档失败: {retrieval_doc_path:?}"))?;
    if search_text_hash(&current_doc.search_text) != entry.content_hash {
        return Ok(QueueEntryOutcome::Complete);
    }
    let Some(current_state) = load_vector_state_opt(team_root, &entry.claim_id).await? else {
        anyhow::bail!("embedding 完成后向量状态缺失: {}", entry.claim_id);
    };
    if !vector_state_matches_queue_entry(&current_state, &entry)
        || current_state.status == VectorStatus::Ready
    {
        // claim 内容或 embedding 配置已更新时，旧 worker 的迟到结果必须直接丢弃。
        return Ok(QueueEntryOutcome::Complete);
    }
    let attempts = current_state.attempts.saturating_add(1);
    let expected_dimensions = current_state.expected_dimensions;

    let vector = match embedding_result.and_then(|vector| {
        validate_embedding_vector(&vector)?;
        if expected_dimensions.is_some_and(|expected| vector.len() != expected) {
            anyhow::bail!(
                "embedding 维度与查询向量不一致: expected={} actual={}",
                expected_dimensions.unwrap_or_default(),
                vector.len()
            );
        }
        Ok(vector)
    }) {
        Ok(vector) => vector,
        Err(err) => {
            let summary = err.to_string();
            let state = VectorState::failed(
                entry.claim_id.clone(),
                actual_hash,
                embedding_fingerprint,
                expected_dimensions,
                entry.generation_seq,
                VectorAttemptMetadata::failed(
                    attempts,
                    completed_at,
                    retry_policy.retry_at(attempts, completed_at),
                ),
                summary.clone(),
            );
            write_vector_state(team_root, &state).await?;
            return Ok(QueueEntryOutcome::EmbeddingFailureRecorded(summary));
        }
    };
    let state = VectorState::ready(
        entry.claim_id,
        actual_hash,
        embedding_fingerprint,
        expected_dimensions,
        entry.generation_seq,
        VectorAttemptMetadata::completed(attempts, completed_at),
        vector,
    );
    write_vector_state(team_root, &state).await?;
    Ok(QueueEntryOutcome::Complete)
}

async fn write_vector_state(team_root: &Path, state: &VectorState) -> anyhow::Result<()> {
    let path = paths::team_store_router_vector_state_path(team_root, &state.claim_id);
    let raw = serde_json::to_vec_pretty(state)?;
    write_text_atomic(&path, &raw)
        .await
        .with_context(|| format!("写入向量状态失败: {path:?}"))
}

async fn write_vector_target_intent(
    team_root: &Path,
    intent: &VectorTargetIntent,
) -> anyhow::Result<()> {
    let path = paths::team_store_router_vector_intent_path(team_root, &intent.claim_id);
    let raw = serde_json::to_vec_pretty(intent)?;
    write_text_atomic(&path, &raw)
        .await
        .with_context(|| format!("写入 Vector target 恢复意图失败: {path:?}"))
}

/// 在即将修改 queue/state 前留下恢复锚点；无意图的旧兼容入口保持原有纯 state 语义。
async fn persist_vector_target_intent(
    team_root: &Path,
    intent: Option<&VectorTargetIntent>,
) -> anyhow::Result<()> {
    if let Some(intent) = intent {
        write_vector_target_intent(team_root, intent).await?;
    }
    Ok(())
}

async fn read_vector_target_intent(path: &Path) -> anyhow::Result<VectorTargetIntent> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("读取 Vector target 恢复意图失败: {path:?}"))?;
    serde_json::from_str(&raw).with_context(|| format!("解析 Vector target 恢复意图失败: {path:?}"))
}

async fn load_vector_target_intent_opt(
    team_root: &Path,
    claim_id: &ClaimId,
) -> anyhow::Result<Option<VectorTargetIntent>> {
    let path = paths::team_store_router_vector_intent_path(team_root, claim_id);
    match tokio::fs::try_exists(&path).await {
        Ok(true) => read_vector_target_intent(&path).await.map(Some),
        Ok(false) => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("检查 Vector target 恢复意图失败: {path:?}"))
        }
    }
}

async fn remove_vector_target_intent(team_root: &Path, claim_id: &ClaimId) -> anyhow::Result<()> {
    let path = paths::team_store_router_vector_intent_path(team_root, claim_id);
    remove_vector_target_intent_path(&path).await
}

async fn remove_vector_target_intent_path(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("删除 Vector target 恢复意图失败: {path:?}"))
        }
    }
}

#[cfg(test)]
async fn append_queue_entry(team_root: &Path, entry: &VectorQueueEntry) -> anyhow::Result<()> {
    let _queue_guard = lock_vector_queue(team_root).await?;
    recover_stale_in_flight_locked(team_root).await?;
    if queue_contains_entry_locked(team_root, entry).await? {
        return Ok(());
    }
    append_queue_entry_locked(team_root, entry).await
}

async fn append_queue_entry_locked(
    team_root: &Path,
    entry: &VectorQueueEntry,
) -> anyhow::Result<()> {
    let path = paths::team_store_router_vector_queue_path(team_root);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("创建向量队列目录失败: {parent:?}"))?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("打开向量队列失败: {path:?}"))?;
    let mut line = serde_json::to_vec(entry)?;
    line.push(b'\n');
    file.write_all(&line)
        .await
        .with_context(|| format!("写入向量队列失败: {path:?}"))?;
    file.flush()
        .await
        .with_context(|| format!("flush 向量队列失败: {path:?}"))?;
    file.sync_data()
        .await
        .with_context(|| format!("fsync 向量队列失败: {path:?}"))?;
    Ok(())
}

async fn enqueue_vector_pending_copy_with_dimensions(
    team_root: &Path,
    claim_id: &ClaimId,
    content_hash: &str,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_dimensions: Option<usize>,
    generation_seq: u64,
) -> anyhow::Result<()> {
    let entry = VectorQueueEntry {
        claim_id: claim_id.clone(),
        content_hash: content_hash.to_string(),
        generation_seq,
        embedding_fingerprint: Some(embedding_fingerprint.clone()),
        expected_dimensions,
        enqueued_at: Utc::now(),
    };
    let _queue_guard = lock_vector_queue(team_root).await?;
    recover_stale_in_flight_locked(team_root).await?;
    let pending_path = paths::team_store_router_vector_queue_path(team_root);
    if tokio::fs::try_exists(&pending_path).await.unwrap_or(false)
        && read_queue_entries(&pending_path)
            .await?
            .iter()
            .any(|queued| queue_entry_key(queued) == queue_entry_key(&entry))
    {
        return Ok(());
    }
    // 任何将 state 切为 Pending 的路径都不能信任 active inflight：
    // worker 可能已根据旧 state 决定删除它，因此 pending 必须留下独立持久副本。
    append_queue_entry_locked(team_root, &entry).await
}

/// 在 queue lock 内分配并先持久化一个严格更新的 generation。
///
/// 调用方必须已持有对应 claim lock，统一保持 claim -> queue 的加锁顺序。
async fn enqueue_new_vector_generation(
    team_root: &Path,
    claim_id: &ClaimId,
    content_hash: &str,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_dimensions: Option<usize>,
    current_generation_seq: u64,
) -> anyhow::Result<u64> {
    let _queue_guard = lock_vector_queue(team_root).await?;
    recover_stale_in_flight_locked(team_root).await?;
    let generation_seq =
        next_generation_seq_locked(team_root, claim_id, current_generation_seq).await?;
    let entry = VectorQueueEntry {
        claim_id: claim_id.clone(),
        content_hash: content_hash.to_string(),
        generation_seq,
        embedding_fingerprint: Some(embedding_fingerprint.clone()),
        expected_dimensions,
        enqueued_at: Utc::now(),
    };
    append_queue_entry_locked(team_root, &entry).await?;
    Ok(generation_seq)
}

/// 为无需立即入队的状态变化预留下一个 generation。
///
/// 若进程在 state 落盘前退出，该序号没有可见写入，后续安全重用即可。
async fn reserve_new_vector_generation(
    team_root: &Path,
    claim_id: &ClaimId,
    current_generation_seq: u64,
) -> anyhow::Result<u64> {
    let _queue_guard = lock_vector_queue(team_root).await?;
    recover_stale_in_flight_locked(team_root).await?;
    next_generation_seq_locked(team_root, claim_id, current_generation_seq).await
}

async fn next_generation_seq_locked(
    team_root: &Path,
    claim_id: &ClaimId,
    current_generation_seq: u64,
) -> anyhow::Result<u64> {
    let max_live = max_live_queue_generation_locked(team_root, claim_id).await?;
    current_generation_seq
        .max(max_live)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("claim {} 的向量 generation 序号已耗尽", claim_id))
}

async fn max_live_queue_generation_locked(
    team_root: &Path,
    claim_id: &ClaimId,
) -> anyhow::Result<u64> {
    let dir = paths::team_store_router_vector_queue_dir(team_root);
    if !tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(0);
    }
    let mut max_generation = 0;
    let mut rd = tokio::fs::read_dir(&dir)
        .await
        .with_context(|| format!("读取向量队列目录失败: {dir:?}"))?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !(file_name == "pending.jsonl" || file_name.ends_with(".inflight.jsonl")) {
            continue;
        }
        for entry in read_queue_entries(&path).await? {
            if &entry.claim_id == claim_id {
                max_generation = max_generation.max(entry.generation_seq);
            }
        }
    }
    Ok(max_generation)
}

async fn drain_queue_entries(team_root: &Path) -> anyhow::Result<Option<DrainedQueue>> {
    let _queue_guard = lock_vector_queue(team_root).await?;
    recover_stale_in_flight_locked(team_root).await?;
    let path = paths::team_store_router_vector_queue_path(team_root);
    if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return Ok(None);
    }
    let metadata = tokio::fs::metadata(&path)
        .await
        .with_context(|| format!("读取向量队列元数据失败: {path:?}"))?;
    if !metadata.is_file() {
        anyhow::bail!("向量队列不是普通文件: {path:?}");
    }
    let in_flight_path = next_in_flight_queue_path(&path).await?;
    tokio::fs::rename(&path, &in_flight_path)
        .await
        .with_context(|| format!("切换向量队列到处理中失败: {path:?} -> {in_flight_path:?}"))?;
    // lease 必须在 queue lock 内建立，避免其他 worker 把刚 rename 的批次误判为 stale。
    let lease_path = in_flight_lease_path(&in_flight_path);
    let lease = FileLockGuard::lock_exclusive(&lease_path)
        .await
        .with_context(|| format!("建立向量 inflight lease 失败: {lease_path:?}"))?;
    let entries = read_queue_entries(&in_flight_path).await?;
    Ok(Some(DrainedQueue {
        entries,
        in_flight_path,
        lease_path,
        lease,
    }))
}

async fn read_queue_entries(path: &Path) -> anyhow::Result<Vec<VectorQueueEntry>> {
    let raw = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("读取向量队列失败: {path:?}"))?;
    let mut deduped: FxHashMap<VectorQueueEntryKey, VectorQueueEntry> = FxHashMap::default();
    for (index, line) in raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let entry: VectorQueueEntry = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(err) => {
                log::warn!(
                    target: "router_vector",
                    "跳过损坏的向量队列行 path={:?} line_no={} error={err:#}",
                    path,
                    index + 1
                );
                continue;
            }
        };
        let key = queue_entry_key(&entry);
        match deduped.get(&key) {
            Some(existing) if existing.enqueued_at >= entry.enqueued_at => {}
            _ => {
                deduped.insert(key, entry);
            }
        }
    }
    Ok(deduped.into_values().collect())
}

async fn finish_in_flight_queue(
    team_root: &Path,
    drained: DrainedQueue,
    retry_entries: &[VectorQueueEntry],
) -> anyhow::Result<()> {
    let _queue_guard = lock_vector_queue(team_root).await?;
    if !retry_entries.is_empty() {
        let pending_path = paths::team_store_router_vector_queue_path(team_root);
        let mut merged = if tokio::fs::try_exists(&pending_path).await.unwrap_or(false) {
            read_queue_entries(&pending_path).await?
        } else {
            Vec::new()
        };
        merged.extend(retry_entries.iter().cloned());
        write_queue_entries_atomic(&pending_path, &dedupe_queue_entries(merged)).await?;
    }
    if tokio::fs::try_exists(&drained.in_flight_path)
        .await
        .unwrap_or(false)
    {
        tokio::fs::remove_file(&drained.in_flight_path)
            .await
            .with_context(|| format!("删除处理中向量队列失败: {:?}", drained.in_flight_path))?;
    }
    // data 先删，再在同一 queue lock 临界区内释放并清理 lease，避免 split-brain。
    drop(drained.lease);
    remove_lease_file(&drained.lease_path).await?;
    Ok(())
}

async fn next_in_flight_queue_path(path: &Path) -> anyhow::Result<PathBuf> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let suffix = Utc::now()
        .timestamp_nanos_opt()
        .map(|nanos| nanos.to_string())
        .unwrap_or_else(|| "unknown".into());
    loop {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = path.with_file_name(format!(
            "pending.{}.{suffix}.{sequence}.inflight.jsonl",
            std::process::id()
        ));
        if !tokio::fs::try_exists(&candidate).await.unwrap_or(false) {
            return Ok(candidate);
        }
        if sequence == u64::MAX {
            anyhow::bail!("无法分配唯一的向量 inflight 队列文件名");
        }
    }
}

fn in_flight_lease_path(in_flight_path: &Path) -> PathBuf {
    let file_name = in_flight_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown.inflight.jsonl");
    in_flight_path.with_file_name(format!("{file_name}.lease.lock"))
}

async fn lock_vector_queue(team_root: &Path) -> anyhow::Result<FileLockGuard> {
    let path = paths::team_store_router_vector_queue_lock_path(team_root);
    FileLockGuard::lock_exclusive(&path)
        .await
        .with_context(|| format!("获取向量队列锁失败: {path:?}"))
}

/// 取得 retrieval document 与 Vector target 共用的 per-claim 协调锁。
async fn lock_retrieval_target(
    team_root: &Path,
    claim_id: &ClaimId,
) -> anyhow::Result<FileLockGuard> {
    let path = paths::team_store_router_vector_state_lock_path(team_root, claim_id);
    FileLockGuard::lock_exclusive(&path)
        .await
        .with_context(|| format!("获取 claim retrieval target 协调锁失败: {path:?}"))
}

async fn has_live_queue_entry(
    team_root: &Path,
    claim_id: &ClaimId,
    content_hash: &str,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
    expected_dimensions: Option<usize>,
    generation_seq: u64,
) -> anyhow::Result<bool> {
    let _queue_guard = lock_vector_queue(team_root).await?;
    recover_stale_in_flight_locked(team_root).await?;
    let dir = paths::team_store_router_vector_queue_dir(team_root);
    if !tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(false);
    }
    let mut rd = tokio::fs::read_dir(&dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !(file_name == "pending.jsonl" || file_name.ends_with(".inflight.jsonl")) {
            continue;
        }
        if read_queue_entries(&path).await?.iter().any(|entry| {
            &entry.claim_id == claim_id
                && entry.content_hash == content_hash
                && entry.generation_seq == generation_seq
                && entry.embedding_fingerprint.as_ref() == Some(embedding_fingerprint)
                && entry.expected_dimensions == expected_dimensions
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
async fn queue_contains_entry_locked(
    team_root: &Path,
    target: &VectorQueueEntry,
) -> anyhow::Result<bool> {
    let dir = paths::team_store_router_vector_queue_dir(team_root);
    if !tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(false);
    }

    let mut rd = tokio::fs::read_dir(&dir)
        .await
        .with_context(|| format!("读取向量队列目录失败: {dir:?}"))?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !(file_name == "pending.jsonl" || file_name.ends_with(".inflight.jsonl")) {
            continue;
        }
        let entries = read_queue_entries(&path).await?;
        if entries
            .iter()
            .any(|entry| queue_entry_key(entry) == queue_entry_key(target))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn queue_entry_key(entry: &VectorQueueEntry) -> VectorQueueEntryKey {
    (
        entry.claim_id.clone(),
        entry.content_hash.clone(),
        entry.generation_seq,
        entry.embedding_fingerprint.clone(),
        entry.expected_dimensions,
    )
}

fn vector_state_matches(
    state: &VectorState,
    content_hash: &str,
    embedding_fingerprint: &EmbeddingCacheFingerprint,
) -> bool {
    state.content_hash == content_hash
        && state.embedding_fingerprint.as_ref() == Some(embedding_fingerprint)
}

fn vector_state_matches_queue_entry(state: &VectorState, entry: &VectorQueueEntry) -> bool {
    state.generation_seq == entry.generation_seq
        && vector_state_target_matches_queue_entry(state, entry)
}

fn vector_state_target_matches_queue_entry(state: &VectorState, entry: &VectorQueueEntry) -> bool {
    state.content_hash == entry.content_hash
        && state.embedding_fingerprint == entry.embedding_fingerprint
        && state.expected_dimensions == entry.expected_dimensions
}

fn validate_embedding_vector(vector: &[f32]) -> anyhow::Result<()> {
    if vector.is_empty() {
        anyhow::bail!("embedding 返回空向量");
    }
    if vector.iter().any(|value| !value.is_finite()) {
        anyhow::bail!("embedding 返回非有限数值");
    }
    if vector.iter().all(|value| value.abs() <= f32::EPSILON) {
        anyhow::bail!("embedding 返回全零向量");
    }
    Ok(())
}

async fn recover_stale_in_flight_locked(team_root: &Path) -> anyhow::Result<usize> {
    let dir = paths::team_store_router_vector_queue_dir(team_root);
    if !tokio::fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(0);
    }

    let mut stale = Vec::new();
    let mut rd = tokio::fs::read_dir(&dir)
        .await
        .with_context(|| format!("读取向量队列目录失败: {dir:?}"))?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".inflight.jsonl") {
            continue;
        }
        let lease_path = in_flight_lease_path(&path);
        let Some(lease) = FileLockGuard::try_lock_exclusive(&lease_path)
            .await
            .with_context(|| format!("检查向量 inflight lease 失败: {lease_path:?}"))?
        else {
            continue;
        };
        let entries = read_queue_entries(&path).await?;
        stale.push((path, lease_path, lease, entries));
    }

    if stale.is_empty() {
        cleanup_orphan_lease_files_locked(&dir).await?;
        return Ok(0);
    }

    let pending_path = paths::team_store_router_vector_queue_path(team_root);
    let mut merged = if tokio::fs::try_exists(&pending_path).await.unwrap_or(false) {
        read_queue_entries(&pending_path).await?
    } else {
        Vec::new()
    };
    for (_, _, _, entries) in &stale {
        merged.extend(entries.iter().cloned());
    }
    let merged = dedupe_queue_entries(merged);
    write_queue_entries_atomic(&pending_path, &merged).await?;

    let recovered = stale.len();
    for (path, lease_path, lease, _) in stale {
        tokio::fs::remove_file(&path)
            .await
            .with_context(|| format!("删除已恢复向量 inflight 队列失败: {path:?}"))?;
        drop(lease);
        remove_lease_file(&lease_path).await?;
    }
    log::info!(
        target: "router_vector",
        "恢复 stale router 向量 inflight 队列 batches={recovered} entries={}",
        merged.len()
    );
    cleanup_orphan_lease_files_locked(&dir).await?;
    Ok(recovered)
}

async fn cleanup_orphan_lease_files_locked(dir: &Path) -> anyhow::Result<()> {
    let mut rd = tokio::fs::read_dir(dir)
        .await
        .with_context(|| format!("读取向量 lease 目录失败: {dir:?}"))?;
    while let Some(entry) = rd.next_entry().await? {
        let lease_path = entry.path();
        let Some(file_name) = lease_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(data_file_name) = file_name.strip_suffix(".lease.lock") else {
            continue;
        };
        if !data_file_name.ends_with(".inflight.jsonl") {
            continue;
        }
        let data_path = lease_path.with_file_name(data_file_name);
        if tokio::fs::try_exists(&data_path).await.unwrap_or(false) {
            continue;
        }
        let Some(lease) = FileLockGuard::try_lock_exclusive(&lease_path).await? else {
            continue;
        };
        drop(lease);
        remove_lease_file(&lease_path).await?;
    }
    Ok(())
}

fn dedupe_queue_entries(entries: Vec<VectorQueueEntry>) -> Vec<VectorQueueEntry> {
    let mut deduped: FxHashMap<VectorQueueEntryKey, VectorQueueEntry> = FxHashMap::default();
    for entry in entries {
        let key = queue_entry_key(&entry);
        match deduped.get(&key) {
            Some(existing) if existing.enqueued_at >= entry.enqueued_at => {}
            _ => {
                deduped.insert(key, entry);
            }
        }
    }
    deduped.into_values().collect()
}

async fn write_queue_entries_atomic(
    path: &Path,
    entries: &[VectorQueueEntry],
) -> anyhow::Result<()> {
    let mut raw = Vec::new();
    for entry in entries {
        raw.extend(serde_json::to_vec(entry)?);
        raw.push(b'\n');
    }
    write_text_atomic(path, &raw)
        .await
        .with_context(|| format!("合并恢复向量队列失败: {path:?}"))
}

async fn remove_lease_file(path: &Path) -> anyhow::Result<()> {
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("删除向量 inflight lease 文件失败: {path:?}"))?;
    }
    Ok(())
}

fn cosine_similarity(query: &[f32], vector: &[f32]) -> Option<usize> {
    if query.is_empty() || vector.is_empty() || query.len() != vector.len() {
        return None;
    }

    let mut dot = 0.0_f32;
    let mut query_norm = 0.0_f32;
    let mut vector_norm = 0.0_f32;
    for (lhs, rhs) in query.iter().zip(vector.iter()) {
        dot += lhs * rhs;
        query_norm += lhs * lhs;
        vector_norm += rhs * rhs;
    }

    if query_norm <= f32::EPSILON || vector_norm <= f32::EPSILON {
        return None;
    }
    let cosine = dot / (query_norm.sqrt() * vector_norm.sqrt());
    let clamped = cosine.clamp(0.0_f32, 1.0_f32);
    let millis = (clamped * 1000.0_f32).round();
    // 0..=1000 的有限实数转整型，理论上不会越界；这里走字符串解析避免 `as` 降位。
    millis.to_string().parse::<usize>().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::*;
    use crate::api::EmbeddingClient;
    use crate::claim::{AgentId, Claim, ClaimStatus, Confidence};
    use crate::config::EmbeddingProvider;
    use crate::maintainer::Maintainer;
    use crate::storage::write_yaml_atomic;

    fn sample_claim() -> Claim {
        Claim {
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
        }
    }

    fn fingerprint(name: &str, dimension_policy: &str) -> EmbeddingCacheFingerprint {
        EmbeddingCacheFingerprint {
            schema_version: 1,
            provider: EmbeddingProvider::OpenAiCompatible,
            endpoint: format!("http://{name}.test/v1/embeddings"),
            model: name.into(),
            dimension_policy: dimension_policy.into(),
            normalization: "none".into(),
        }
    }

    fn retry_policy(base_ms: u64, max_ms: u64) -> VectorRetryPolicy {
        VectorRetryPolicy::new(
            Duration::from_millis(base_ms),
            Duration::from_millis(max_ms),
        )
        .unwrap()
    }

    #[test]
    fn search_text_hash_is_versioned_sha256_and_rejects_legacy_formats() {
        assert_eq!(
            search_text_hash("abc"),
            "sha256-v1:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(is_current_search_text_hash(&search_text_hash("abc")));
        assert!(!is_current_search_text_hash("0123456789abcdef"));
        assert!(!is_current_search_text_hash("sha256-v1:ABCDEF"));
        assert!(!is_current_search_text_hash("sha256-v1:abcdef"));
    }

    async fn write_retrieval_doc(team_root: &Path, doc: &RetrievalDocument) {
        write_yaml_atomic(
            &paths::team_store_router_retrieval_doc_path(team_root, &doc.claim_id),
            doc,
        )
        .await
        .unwrap();
    }

    fn queue_entry(
        doc: &RetrievalDocument,
        embedding_fingerprint: Option<EmbeddingCacheFingerprint>,
    ) -> VectorQueueEntry {
        VectorQueueEntry {
            claim_id: doc.claim_id.clone(),
            content_hash: search_text_hash(&doc.search_text),
            generation_seq: 0,
            embedding_fingerprint,
            expected_dimensions: None,
            enqueued_at: Utc::now(),
        }
    }

    #[derive(Clone)]
    struct FakeEmbeddingClient {
        fingerprint: EmbeddingCacheFingerprint,
        result: Result<Vec<f32>, String>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeEmbeddingClient {
        fn success(fingerprint: EmbeddingCacheFingerprint, vector: Vec<f32>) -> Self {
            Self {
                fingerprint,
                result: Ok(vector),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn failure(fingerprint: EmbeddingCacheFingerprint, message: &str) -> Self {
            Self {
                fingerprint,
                result: Err(message.into()),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl EmbeddingClient for FakeEmbeddingClient {
        fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
            self.fingerprint.clone()
        }

        async fn embed(&self, _input: &str) -> anyhow::Result<Vec<f32>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result
                .clone()
                .map_err(|message| anyhow::anyhow!(message))
        }
    }

    struct BlockingEmbeddingClient {
        fingerprint: EmbeddingCacheFingerprint,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl EmbeddingClient for BlockingEmbeddingClient {
        fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
            self.fingerprint.clone()
        }

        async fn embed(&self, _input: &str) -> anyhow::Result<Vec<f32>> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(vec![1.0, 0.0])
        }
    }

    struct BlockingFailureEmbeddingClient {
        fingerprint: EmbeddingCacheFingerprint,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl EmbeddingClient for BlockingFailureEmbeddingClient {
        fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
            self.fingerprint.clone()
        }

        async fn embed(&self, _input: &str) -> anyhow::Result<Vec<f32>> {
            self.started.notify_one();
            self.release.notified().await;
            anyhow::bail!("delayed provider failure")
        }
    }

    #[tokio::test]
    async fn enqueue_vector_work_marks_claim_pending() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let retrieval_doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &retrieval_doc).await;

        let state = ensure_vector_pending(&team_root, &retrieval_doc, &fp)
            .await
            .unwrap();
        assert_eq!(state.status, VectorStatus::Pending);

        let stored = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(stored.status, VectorStatus::Pending);
        assert_eq!(
            stored.content_hash,
            search_text_hash(&retrieval_doc.search_text)
        );
        assert_eq!(stored.embedding_fingerprint.as_ref(), Some(&fp));
        assert_eq!(stored.attempts, 0);
    }

    #[tokio::test]
    async fn append_opens_pending_only_after_queue_lock_allows_drain_switch() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let first = sample_claim();
        let mut second = sample_claim();
        second.id = ClaimId::random();
        second.name = "second_claim".into();

        let fp = fingerprint("model-a", "fixed:2");
        let first_doc = RetrievalDocument::from_claim(&first, vec![], vec![]);
        let second_doc = RetrievalDocument::from_claim(&second, vec![], vec![]);
        let first_entry = queue_entry(&first_doc, Some(fp.clone()));
        let second_entry = queue_entry(&second_doc, Some(fp));
        append_queue_entry(&team_root, &first_entry).await.unwrap();

        let queue_guard = lock_vector_queue(&team_root).await.unwrap();
        let append_task = tokio::spawn({
            let team_root = team_root.clone();
            let second_entry = second_entry.clone();
            async move { append_queue_entry(&team_root, &second_entry).await }
        });
        tokio::task::yield_now().await;
        assert!(!append_task.is_finished());

        let pending_path = paths::team_store_router_vector_queue_path(&team_root);
        let in_flight_path = pending_path.with_file_name("pending.test.inflight.jsonl");
        tokio::fs::rename(&pending_path, &in_flight_path)
            .await
            .unwrap();
        let lease_path = in_flight_lease_path(&in_flight_path);
        let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();
        drop(queue_guard);
        append_task.await.unwrap().unwrap();

        let in_flight_entries = read_queue_entries(&in_flight_path).await.unwrap();
        let pending_entries = read_queue_entries(&pending_path).await.unwrap();
        assert_eq!(in_flight_entries, vec![first_entry]);
        assert_eq!(pending_entries, vec![second_entry]);
        finish_in_flight_queue(
            &team_root,
            DrainedQueue {
                entries: in_flight_entries,
                in_flight_path,
                lease_path,
                lease,
            },
            &[],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn drain_waits_for_locked_append_and_reads_only_complete_synced_line() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let entry = queue_entry(&doc, Some(fingerprint("model-a", "fixed:2")));
        let queue_guard = lock_vector_queue(&team_root).await.unwrap();
        let pending_path = paths::team_store_router_vector_queue_path(&team_root);
        tokio::fs::create_dir_all(pending_path.parent().unwrap())
            .await
            .unwrap();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&pending_path)
            .await
            .unwrap();
        let drain_task = tokio::spawn({
            let team_root = team_root.clone();
            async move { drain_queue_entries(&team_root).await }
        });
        tokio::task::yield_now().await;
        assert!(!drain_task.is_finished());

        let mut line = serde_json::to_vec(&entry).unwrap();
        line.push(b'\n');
        let midpoint = line.len() / 2;
        file.write_all(&line[..midpoint]).await.unwrap();
        tokio::task::yield_now().await;
        assert!(!drain_task.is_finished());
        file.write_all(&line[midpoint..]).await.unwrap();
        file.flush().await.unwrap();
        file.sync_data().await.unwrap();
        drop(file);
        drop(queue_guard);

        let drained = drain_task.await.unwrap().unwrap().unwrap();
        assert_eq!(drained.entries, vec![entry]);
        finish_in_flight_queue(&team_root, drained, &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn active_inflight_is_not_recovered_then_is_recovered_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let entry = queue_entry(&doc, Some(fingerprint("model-a", "fixed:2")));
        let queue_dir = paths::team_store_router_vector_queue_dir(&team_root);
        tokio::fs::create_dir_all(&queue_dir).await.unwrap();
        let in_flight_path = queue_dir.join("pending.orphan.inflight.jsonl");
        write_queue_entries_atomic(&in_flight_path, std::slice::from_ref(&entry))
            .await
            .unwrap();
        let lease_path = in_flight_lease_path(&in_flight_path);
        let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();

        assert!(drain_queue_entries(&team_root).await.unwrap().is_none());
        assert!(tokio::fs::try_exists(&in_flight_path).await.unwrap());

        drop(lease);
        let drained = drain_queue_entries(&team_root).await.unwrap().unwrap();
        assert_eq!(drained.entries, vec![entry]);
        assert!(!tokio::fs::try_exists(&in_flight_path).await.unwrap());
        finish_in_flight_queue(&team_root, drained, &[])
            .await
            .unwrap();
        assert!(drain_queue_entries(&team_root).await.unwrap().is_none());
        assert!(!tokio::fs::try_exists(&lease_path).await.unwrap());
    }

    #[tokio::test]
    async fn stale_recovery_deduplicates_pending_and_inflight_entries() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let entry = queue_entry(&doc, Some(fingerprint("model-a", "fixed:2")));
        let pending_path = paths::team_store_router_vector_queue_path(&team_root);
        write_queue_entries_atomic(&pending_path, std::slice::from_ref(&entry))
            .await
            .unwrap();
        let stale_path = pending_path.with_file_name("pending.stale.inflight.jsonl");
        write_queue_entries_atomic(&stale_path, std::slice::from_ref(&entry))
            .await
            .unwrap();

        let drained = drain_queue_entries(&team_root).await.unwrap().unwrap();
        assert_eq!(drained.entries, vec![entry]);
        finish_in_flight_queue(&team_root, drained, &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn orphan_lease_without_data_is_cleaned_during_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let queue_dir = paths::team_store_router_vector_queue_dir(&team_root);
        let lease_path = queue_dir.join("pending.gone.inflight.jsonl.lease.lock");
        let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();
        drop(lease);

        assert!(drain_queue_entries(&team_root).await.unwrap().is_none());
        assert!(!tokio::fs::try_exists(&lease_path).await.unwrap());
    }

    #[tokio::test]
    async fn worker_recovers_persisted_queue_entry_when_state_write_never_happened() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        append_queue_entry(&team_root, &queue_entry(&doc, Some(fp.clone())))
            .await
            .unwrap();
        let client = FakeEmbeddingClient::success(fp.clone(), vec![1.0, 0.0]);

        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(report.processed, 1);
        assert_eq!(client.calls(), 1);
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert_eq!(state.embedding_fingerprint.as_ref(), Some(&fp));
    }

    #[tokio::test]
    async fn recovered_ready_entry_skips_duplicate_embedding_call() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();
        let stale_path = paths::team_store_router_vector_queue_dir(&team_root)
            .join("pending.crashed.inflight.jsonl");
        write_queue_entries_atomic(&stale_path, &[queue_entry(&doc, Some(fp.clone()))])
            .await
            .unwrap();
        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);

        process_pending_queue(
            team_root,
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 0);
    }

    #[tokio::test]
    async fn processing_io_error_requeues_entry_instead_of_deleting_it() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        ensure_vector_pending_at(&team_root, &doc, &fp, Utc::now())
            .await
            .unwrap();
        let client = FakeEmbeddingClient::success(fp.clone(), vec![1.0, 0.0]);

        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(report.failures, 1);
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        let mut expected = queue_entry(&doc, Some(fp));
        expected.generation_seq = 1;
        assert_eq!(queue_entry_key(&pending[0]), queue_entry_key(&expected));
    }

    #[tokio::test]
    async fn failed_state_waits_until_backoff_deadline_and_preserves_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        let now: DateTime<Utc> = "2026-07-10T00:00:00Z".parse().unwrap();
        let retry_at = now + chrono::Duration::seconds(30);
        let state = VectorState::failed(
            claim.id.clone(),
            search_text_hash(&doc.search_text),
            fp.clone(),
            None,
            0,
            VectorAttemptMetadata::failed(3, now, retry_at),
            "outage".into(),
        );
        write_vector_state(&team_root, &state).await.unwrap();

        let waiting = ensure_vector_pending_at(
            &team_root,
            &doc,
            &fp,
            retry_at - chrono::Duration::milliseconds(1),
        )
        .await
        .unwrap();
        assert_eq!(waiting.status, VectorStatus::Failed);
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );

        let due = ensure_vector_pending_at(&team_root, &doc, &fp, retry_at)
            .await
            .unwrap();
        assert_eq!(due.status, VectorStatus::Pending);
        assert_eq!(due.attempts, 3);
        assert_eq!(
            read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn failed_retry_keeps_pending_copy_when_old_inflight_is_still_active() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        let entry = queue_entry(&doc, Some(fp.clone()));
        let now = Utc::now();
        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                entry.content_hash.clone(),
                fp.clone(),
                None,
                0,
                VectorAttemptMetadata::failed(1, now, now),
                "outage".into(),
            ),
        )
        .await
        .unwrap();
        let in_flight_path = paths::team_store_router_vector_queue_dir(&team_root)
            .join("pending.active.inflight.jsonl");
        write_queue_entries_atomic(&in_flight_path, std::slice::from_ref(&entry))
            .await
            .unwrap();
        let lease_path = in_flight_lease_path(&in_flight_path);
        let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();

        let state = ensure_vector_pending_at(&team_root, &doc, &fp, now)
            .await
            .unwrap();
        assert_eq!(state.status, VectorStatus::Pending);
        finish_in_flight_queue(
            &team_root,
            DrainedQueue {
                entries: vec![entry.clone()],
                in_flight_path,
                lease_path,
                lease,
            },
            &[],
        )
        .await
        .unwrap();
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(queue_entry_key(&pending[0]), queue_entry_key(&entry));
    }

    #[tokio::test]
    async fn pending_generation_ignores_live_entry_with_old_dimension_expectation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_vector_state(
            &team_root,
            &VectorState::pending(
                claim.id.clone(),
                content_hash.clone(),
                fp.clone(),
                Some(3),
                0,
            ),
        )
        .await
        .unwrap();

        let old_entry = queue_entry(&doc, Some(fp.clone()));
        let in_flight_path = paths::team_store_router_vector_queue_dir(&team_root)
            .join("pending.old-dimension.inflight.jsonl");
        write_queue_entries_atomic(&in_flight_path, std::slice::from_ref(&old_entry))
            .await
            .unwrap();
        let lease_path = in_flight_lease_path(&in_flight_path);
        let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();

        ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].expected_dimensions, Some(3));

        finish_in_flight_queue(
            &team_root,
            DrainedQueue {
                entries: vec![old_entry],
                in_flight_path,
                lease_path,
                lease,
            },
            &[],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn content_or_fingerprint_change_resets_failed_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-a", "fixed:2");
        let new_fp = fingerprint("model-b", "fixed:2");
        let now = Utc::now();
        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                search_text_hash(&doc.search_text),
                old_fp,
                Some(3),
                0,
                VectorAttemptMetadata::failed(7, now, now + chrono::Duration::hours(1)),
                "outage".into(),
            ),
        )
        .await
        .unwrap();

        let changed = ensure_vector_pending(&team_root, &doc, &new_fp)
            .await
            .unwrap();
        assert_eq!(changed.status, VectorStatus::Pending);
        assert_eq!(changed.attempts, 0);
        assert_eq!(changed.embedding_fingerprint.as_ref(), Some(&new_fp));
        assert_eq!(changed.expected_dimensions, None);

        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                search_text_hash(&doc.search_text),
                new_fp.clone(),
                Some(3),
                0,
                VectorAttemptMetadata::failed(7, now, now + chrono::Duration::hours(1)),
                "outage".into(),
            ),
        )
        .await
        .unwrap();

        let mut changed_doc = doc;
        changed_doc.search_text.push_str(" changed");
        let content_changed = ensure_vector_pending(&team_root, &changed_doc, &new_fp)
            .await
            .unwrap();
        assert_eq!(content_changed.attempts, 0);
        assert_eq!(content_changed.expected_dimensions, None);
        assert_eq!(
            content_changed.content_hash,
            search_text_hash(&changed_doc.search_text)
        );
    }

    #[tokio::test]
    async fn embedding_failures_persist_capped_backoff_and_invalid_vectors_fail() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        let client = FakeEmbeddingClient::failure(fp.clone(), "provider down");
        let policy = retry_policy(10, 20);

        for (attempt, expected_delay_ms) in [(1_u32, 10_i64), (2, 20), (3, 20)] {
            let report =
                process_pending_queue(team_root.clone(), Arc::new(client.clone()), 1, policy)
                    .await
                    .unwrap();
            assert_eq!(report.failures, 1);
            let state = load_vector_state(&team_root, &claim.id).await.unwrap();
            assert_eq!(state.status, VectorStatus::Failed);
            assert_eq!(state.attempts, attempt);
            assert_eq!(
                state.next_retry_at.unwrap() - state.last_attempt_at.unwrap(),
                chrono::Duration::milliseconds(expected_delay_ms)
            );
            if attempt < 3 {
                ensure_vector_pending_at(&team_root, &doc, &fp, state.next_retry_at.unwrap())
                    .await
                    .unwrap();
            }
        }

        let mut invalid_claim = sample_claim();
        invalid_claim.id = ClaimId::random();
        let invalid_doc = RetrievalDocument::from_claim(&invalid_claim, vec![], vec![]);
        write_retrieval_doc(&team_root, &invalid_doc).await;
        ensure_vector_pending(&team_root, &invalid_doc, &fp)
            .await
            .unwrap();
        let invalid_client = FakeEmbeddingClient::success(fp, vec![0.0, 0.0]);
        process_pending_queue(team_root.clone(), Arc::new(invalid_client), 1, policy)
            .await
            .unwrap();
        let invalid_state = load_vector_state(&team_root, &invalid_claim.id)
            .await
            .unwrap();
        assert_eq!(invalid_state.status, VectorStatus::Failed);
        assert!(invalid_state
            .error_summary
            .as_deref()
            .unwrap()
            .contains("全零"));
    }

    #[tokio::test]
    async fn retry_deadline_starts_when_slow_failure_completes() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingFailureEmbeddingClient {
                fingerprint: fp,
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(1_000, 1_000))
                    .await
            }
        });
        started.notified().await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        let failure_not_before = Utc::now();
        release.notify_one();
        worker.await.unwrap().unwrap();

        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Failed);
        let completed_at = state.last_attempt_at.unwrap();
        assert!(completed_at >= failure_not_before);
        assert_eq!(
            state.next_retry_at.unwrap() - completed_at,
            chrono::Duration::seconds(1)
        );
    }

    #[tokio::test]
    async fn same_fingerprint_dimension_change_requeues_ready_vector() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        write_retrieval_doc(&team_root, &doc).await;
        store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();
        let active_entry = queue_entry(&doc, Some(fp.clone()));
        let in_flight_path = paths::team_store_router_vector_queue_dir(&team_root)
            .join("pending.ready.inflight.jsonl");
        write_queue_entries_atomic(&in_flight_path, std::slice::from_ref(&active_entry))
            .await
            .unwrap();
        let lease_path = in_flight_lease_path(&in_flight_path);
        let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();

        let hits = search_ready_vectors(&team_root, &[1.0, 0.0, 0.0], &fp, 5)
            .await
            .unwrap();
        assert!(hits.is_empty());
        finish_in_flight_queue(
            &team_root,
            DrainedQueue {
                entries: vec![active_entry],
                in_flight_path,
                lease_path,
                lease,
            },
            &[],
        )
        .await
        .unwrap();
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Pending);
        assert_eq!(state.vector_dimensions, None);
        assert_eq!(
            read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn query_content_fence_skips_mismatched_state_before_dimension_repair() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc_b).await;
        let state_b = VectorState::ready(
            claim.id.clone(),
            search_text_hash(&doc_b.search_text),
            fp.clone(),
            Some(3),
            9,
            VectorAttemptMetadata::completed(1, Utc::now()),
            vec![1.0, 0.0, 0.0],
        );
        write_vector_state(&team_root, &state_b).await.unwrap();
        let mut expected = FxHashMap::default();
        expected.insert(claim.id.clone(), search_text_hash(&doc_a.search_text));

        let hits = search_ready_vectors_for_claims(&team_root, &[1.0, 0.0], &fp, &expected, 5)
            .await
            .unwrap();
        assert!(hits.is_empty());
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            state_b
        );
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn query_content_fence_accepts_same_content_with_higher_generation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        let ready = VectorState::ready(
            claim.id.clone(),
            content_hash.clone(),
            fp.clone(),
            Some(2),
            42,
            VectorAttemptMetadata::completed(1, Utc::now()),
            vec![1.0, 0.0],
        );
        write_vector_state(&team_root, &ready).await.unwrap();
        let mut expected = FxHashMap::default();
        expected.insert(claim.id.clone(), content_hash);

        let hits = search_ready_vectors_for_claims(&team_root, &[1.0, 0.0], &fp, &expected, 5)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].claim_id, claim.id);
    }

    #[tokio::test]
    async fn stale_query_repair_helpers_do_not_mutate_replaced_generation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        let hash_a = search_text_hash(&doc_a.search_text);
        let hash_b = search_text_hash(&doc_b.search_text);
        write_retrieval_doc(&team_root, &doc_b).await;

        let pending_a = VectorState::pending(claim.id.clone(), hash_a.clone(), fp.clone(), None, 1);
        let pending_b = VectorState::pending(claim.id.clone(), hash_b.clone(), fp.clone(), None, 2);
        write_vector_state(&team_root, &pending_b).await.unwrap();
        requeue_expected_dimension_change(&team_root, &pending_a, &fp, &hash_a, 3)
            .await
            .unwrap();
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            pending_b
        );
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );

        let ready_a = VectorState::ready(
            claim.id.clone(),
            hash_a,
            fp.clone(),
            Some(2),
            3,
            VectorAttemptMetadata::completed(1, Utc::now()),
            vec![1.0, 0.0],
        );
        let ready_b = VectorState::ready(
            claim.id.clone(),
            hash_b,
            fp.clone(),
            Some(2),
            4,
            VectorAttemptMetadata::completed(1, Utc::now()),
            vec![1.0, 0.0],
        );
        write_vector_state(&team_root, &ready_b).await.unwrap();
        requeue_dimension_mismatch(&team_root, &ready_a, &fp, &ready_a.content_hash, 3)
            .await
            .unwrap();
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            ready_b
        );
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn delayed_unknown_dimension_result_cannot_overwrite_new_expectation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        write_retrieval_doc(&team_root, &doc).await;
        ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingEmbeddingClient {
                fingerprint: fp.clone(),
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(10, 100)).await
            }
        });
        started.notified().await;

        assert!(search_ready_vectors(&team_root, &[1.0, 0.0, 0.0], &fp, 5)
            .await
            .unwrap()
            .is_empty());
        let aligned = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(aligned.status, VectorStatus::Pending);
        assert_eq!(aligned.expected_dimensions, Some(3));
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].expected_dimensions, Some(3));

        release.notify_one();
        worker.await.unwrap().unwrap();

        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Pending);
        assert_eq!(state.expected_dimensions, Some(3));
        assert_eq!(state.error_summary, None);
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].expected_dimensions, Some(3));

        process_pending_queue(
            team_root.clone(),
            Arc::new(FakeEmbeddingClient::success(fp, vec![1.0, 0.0, 0.0])),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.status, VectorStatus::Ready);
        assert_eq!(ready.expected_dimensions, Some(3));
        assert_eq!(ready.vector_dimensions, Some(3));
    }

    #[tokio::test]
    async fn delayed_unknown_dimension_failure_cannot_overwrite_new_expectation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        write_retrieval_doc(&team_root, &doc).await;
        let initial = ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let old_worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingFailureEmbeddingClient {
                fingerprint: fp.clone(),
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(10, 100)).await
            }
        });
        started.notified().await;

        assert!(search_ready_vectors(&team_root, &[1.0, 0.0, 0.0], &fp, 5)
            .await
            .unwrap()
            .is_empty());
        let aligned = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(aligned.status, VectorStatus::Pending);
        assert!(aligned.generation_seq > initial.generation_seq);
        assert_eq!(aligned.expected_dimensions, Some(3));

        release.notify_one();
        let report = old_worker.await.unwrap().unwrap();
        assert_eq!(report.failures, 0);
        let after = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(after, aligned);
        assert_eq!(after.attempts, 0);
        assert_eq!(after.last_attempt_at, None);
        assert_eq!(after.next_retry_at, None);
        assert_eq!(after.error_summary, None);
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].generation_seq, aligned.generation_seq);
        assert_eq!(pending[0].expected_dimensions, Some(3));
    }

    #[tokio::test]
    async fn queue_first_dimension_generation_survives_late_old_completion() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        let initial = ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        assert_eq!(initial.generation_seq, 1);

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let old_worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingEmbeddingClient {
                fingerprint: fp.clone(),
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(10, 100)).await
            }
        });
        started.notified().await;

        let claim_guard = lock_retrieval_target(&team_root, &claim.id).await.unwrap();
        let generation_seq = enqueue_new_vector_generation(
            &team_root,
            &claim.id,
            &content_hash,
            &fp,
            Some(3),
            initial.generation_seq,
        )
        .await
        .unwrap();
        drop(claim_guard);
        assert_eq!(generation_seq, 2);
        let queued = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(queued.len(), 1);
        let queued_at = queued[0].enqueued_at;

        release.notify_one();
        old_worker.await.unwrap().unwrap();
        let late_old = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(late_old.status, VectorStatus::Ready);
        assert_eq!(late_old.generation_seq, 1);
        assert!(late_old.updated_at >= queued_at);

        process_pending_queue(
            team_root.clone(),
            Arc::new(FakeEmbeddingClient::success(fp, vec![1.0, 0.0, 0.0])),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.status, VectorStatus::Ready);
        assert_eq!(ready.generation_seq, 2);
        assert_eq!(ready.expected_dimensions, Some(3));
        assert_eq!(ready.vector_dimensions, Some(3));
    }

    #[tokio::test]
    async fn queue_first_content_and_fingerprint_generation_recovers_old_state() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-old", "fixed:2");
        let new_fp = fingerprint("model-new", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        let old = store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            old_fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();
        assert_eq!(old.generation_seq, 1);

        doc.search_text.push_str(" authoritative update");
        write_retrieval_doc(&team_root, &doc).await;
        let new_hash = search_text_hash(&doc.search_text);
        let claim_guard = lock_retrieval_target(&team_root, &claim.id).await.unwrap();
        let generation_seq = enqueue_new_vector_generation(
            &team_root,
            &claim.id,
            &new_hash,
            &new_fp,
            None,
            old.generation_seq,
        )
        .await
        .unwrap();
        drop(claim_guard);
        assert_eq!(generation_seq, 2);
        assert_eq!(
            load_vector_state(&team_root, &claim.id)
                .await
                .unwrap()
                .embedding_fingerprint,
            Some(old_fp)
        );

        process_pending_queue(
            team_root.clone(),
            Arc::new(FakeEmbeddingClient::success(new_fp.clone(), vec![1.0, 0.0])),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.status, VectorStatus::Ready);
        assert_eq!(ready.generation_seq, 2);
        assert_eq!(ready.content_hash, new_hash);
        assert_eq!(ready.embedding_fingerprint, Some(new_fp));
    }

    #[tokio::test]
    async fn higher_sibling_generation_wins_out_of_order_processing() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        let state = store_ready_vector_state(
            &team_root,
            &claim.id,
            content_hash.clone(),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();

        let claim_guard = lock_retrieval_target(&team_root, &claim.id).await.unwrap();
        let lower_seq = enqueue_new_vector_generation(
            &team_root,
            &claim.id,
            &content_hash,
            &fp,
            Some(3),
            state.generation_seq,
        )
        .await
        .unwrap();
        let lower_batch = drain_queue_entries(&team_root).await.unwrap().unwrap();
        let higher_seq = enqueue_new_vector_generation(
            &team_root,
            &claim.id,
            &content_hash,
            &fp,
            Some(4),
            state.generation_seq,
        )
        .await
        .unwrap();
        drop(claim_guard);
        assert_eq!((lower_seq, higher_seq), (2, 3));

        let higher_entry =
            read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
                .pop()
                .unwrap();
        let higher_client = FakeEmbeddingClient::success(fp.clone(), vec![1.0, 0.0, 0.0, 0.0]);
        process_queue_entry(
            &team_root,
            Arc::new(higher_client.clone()),
            higher_entry,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(higher_client.calls(), 1);

        let lower_client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0, 0.0]);
        process_queue_entry(
            &team_root,
            Arc::new(lower_client.clone()),
            lower_batch.entries[0].clone(),
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(lower_client.calls(), 0);
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.generation_seq, 3);
        assert_eq!(ready.expected_dimensions, Some(4));
        assert_eq!(ready.vector_dimensions, Some(4));
        finish_in_flight_queue(&team_root, lower_batch, &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ready_state_recovers_enqueue_before_dimension_state_write() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        write_retrieval_doc(&team_root, &doc).await;
        store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash: search_text_hash(&doc.search_text),
                generation_seq: 2,
                embedding_fingerprint: Some(fp.clone()),
                expected_dimensions: Some(3),
                enqueued_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        process_pending_queue(
            team_root.clone(),
            Arc::new(FakeEmbeddingClient::success(fp, vec![1.0, 0.0, 0.0])),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert_eq!(state.vector_dimensions, Some(3));
        assert_eq!(state.expected_dimensions, Some(3));
    }

    #[tokio::test]
    async fn pending_state_recovers_enqueue_before_dimension_state_write() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        write_vector_state(
            &team_root,
            &VectorState::pending(claim.id.clone(), content_hash.clone(), fp.clone(), None, 1),
        )
        .await
        .unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash,
                generation_seq: 2,
                embedding_fingerprint: Some(fp.clone()),
                expected_dimensions: Some(3),
                enqueued_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0, 0.0]);
        process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 1);
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert_eq!(state.vector_dimensions, Some(3));
        assert_eq!(state.expected_dimensions, Some(3));
    }

    #[tokio::test]
    async fn failed_unknown_dimension_keeps_backoff_when_query_binds_dimension() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let now = Utc::now();
        let retry_at = now + chrono::Duration::hours(1);
        write_retrieval_doc(&team_root, &doc).await;
        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                search_text_hash(&doc.search_text),
                fp.clone(),
                None,
                1,
                VectorAttemptMetadata::failed(4, now, retry_at),
                "provider outage".into(),
            ),
        )
        .await
        .unwrap();

        assert!(search_ready_vectors(&team_root, &[1.0, 0.0, 0.0], &fp, 5)
            .await
            .unwrap()
            .is_empty());
        let bound = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(bound.status, VectorStatus::Failed);
        assert_eq!(bound.expected_dimensions, Some(3));
        assert_eq!(bound.attempts, 4);
        assert_eq!(bound.last_attempt_at, Some(now));
        assert_eq!(bound.next_retry_at, Some(retry_at));
        assert_eq!(bound.error_summary.as_deref(), Some("provider outage"));
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );

        let due = ensure_vector_pending_at(&team_root, &doc, &fp, retry_at)
            .await
            .unwrap();
        assert_eq!(due.status, VectorStatus::Pending);
        assert_eq!(due.expected_dimensions, Some(3));
        assert_eq!(due.attempts, 4);
        let client = FakeEmbeddingClient::failure(fp, "still unavailable");
        process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 1);
        let failed_again = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(failed_again.status, VectorStatus::Failed);
        assert_eq!(failed_again.expected_dimensions, Some(3));
        assert_eq!(failed_again.attempts, 5);
    }

    #[tokio::test]
    async fn store_ready_binds_unknown_dimension_as_fresh_generation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        let attempted_at = Utc::now();
        let mut pending =
            VectorState::pending(claim.id.clone(), content_hash.clone(), fp.clone(), None, 7);
        pending.attempts = 4;
        pending.last_attempt_at = Some(attempted_at);
        write_vector_state(&team_root, &pending).await.unwrap();

        let ready =
            store_ready_vector_state(&team_root, &claim.id, content_hash, fp, vec![1.0, 0.0])
                .await
                .unwrap();
        assert_eq!(ready.generation_seq, 8);
        assert_eq!(ready.expected_dimensions, Some(2));
        assert_eq!(ready.attempts, 1);
    }

    #[tokio::test]
    async fn queue_first_dimension_heal_preserves_due_failure_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        let attempted_at = Utc::now() - chrono::Duration::minutes(1);
        write_retrieval_doc(&team_root, &doc).await;
        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                content_hash.clone(),
                fp.clone(),
                None,
                5,
                VectorAttemptMetadata::failed(4, attempted_at, attempted_at),
                "old outage".into(),
            ),
        )
        .await
        .unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash,
                generation_seq: 6,
                embedding_fingerprint: Some(fp.clone()),
                expected_dimensions: Some(2),
                enqueued_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingEmbeddingClient {
                fingerprint: fp,
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(10, 100)).await
            }
        });
        started.notified().await;
        let healed = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(healed.status, VectorStatus::Pending);
        assert_eq!(healed.generation_seq, 6);
        assert_eq!(healed.expected_dimensions, Some(2));
        assert_eq!(healed.attempts, 4);
        assert_eq!(healed.last_attempt_at, Some(attempted_at));

        release.notify_one();
        worker.await.unwrap().unwrap();
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.status, VectorStatus::Ready);
        assert_eq!(ready.generation_seq, 6);
        assert_eq!(ready.attempts, 5);
    }

    #[tokio::test]
    async fn queue_first_dimension_heal_keeps_future_failure_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        let attempted_at = Utc::now();
        let retry_at = attempted_at + chrono::Duration::hours(1);
        write_retrieval_doc(&team_root, &doc).await;
        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                content_hash.clone(),
                fp.clone(),
                None,
                5,
                VectorAttemptMetadata::failed(4, attempted_at, retry_at),
                "provider outage".into(),
            ),
        )
        .await
        .unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash,
                generation_seq: 6,
                embedding_fingerprint: Some(fp.clone()),
                expected_dimensions: Some(2),
                enqueued_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);

        process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 0);
        let failed = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(failed.status, VectorStatus::Failed);
        assert_eq!(failed.generation_seq, 6);
        assert_eq!(failed.expected_dimensions, Some(2));
        assert_eq!(failed.attempts, 4);
        assert_eq!(failed.last_attempt_at, Some(attempted_at));
        assert_eq!(failed.next_retry_at, Some(retry_at));
        assert_eq!(failed.error_summary.as_deref(), Some("provider outage"));
    }

    #[tokio::test]
    async fn stale_dimension_entry_cannot_replace_newer_ready_generation() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        let ready_at = Utc::now();
        write_vector_state(
            &team_root,
            &VectorState::ready(
                claim.id.clone(),
                content_hash.clone(),
                fp.clone(),
                Some(3),
                2,
                VectorAttemptMetadata::completed(1, ready_at),
                vec![1.0, 0.0, 0.0],
            ),
        )
        .await
        .unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash,
                generation_seq: 1,
                embedding_fingerprint: Some(fp.clone()),
                expected_dimensions: Some(2),
                enqueued_at: ready_at + chrono::Duration::hours(1),
            },
        )
        .await
        .unwrap();

        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);
        process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 0);
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert_eq!(state.expected_dimensions, Some(3));
        assert_eq!(state.vector_dimensions, Some(3));
    }

    #[tokio::test]
    async fn direct_state_helpers_never_drop_known_dimensions() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let content_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        write_vector_state(
            &team_root,
            &VectorState::pending(
                claim.id.clone(),
                content_hash.clone(),
                fp.clone(),
                Some(3),
                1,
            ),
        )
        .await
        .unwrap();

        let failed = store_failed_vector_state(
            &team_root,
            &claim.id,
            content_hash.clone(),
            fp.clone(),
            "provider outage".into(),
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(failed.expected_dimensions, Some(3));

        let ready = store_ready_vector_state(
            &team_root,
            &claim.id,
            content_hash.clone(),
            fp.clone(),
            vec![1.0, 0.0, 0.0],
        )
        .await
        .unwrap();
        assert_eq!(ready.expected_dimensions, Some(3));

        let err = store_ready_vector_state(&team_root, &claim.id, content_hash, fp, vec![1.0, 0.0])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("generation 不一致"));
        assert_eq!(
            load_vector_state(&team_root, &claim.id)
                .await
                .unwrap()
                .expected_dimensions,
            Some(3)
        );
    }

    #[tokio::test]
    async fn compatibility_helpers_reject_stale_document_targets_without_clobbering_current_state()
    {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc_b).await;
        let current_state = store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc_b.search_text),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();

        let pending_error = ensure_vector_pending(&team_root, &doc_a, &fp)
            .await
            .unwrap_err();
        assert!(pending_error
            .to_string()
            .contains("兼容 Vector 写入与当前 retrieval document 不一致"));
        let failed_error = store_failed_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc_a.search_text),
            fp.clone(),
            "stale helper failure".into(),
            retry_policy(10, 100),
        )
        .await
        .unwrap_err();
        assert!(failed_error
            .to_string()
            .contains("兼容 Vector 写入与当前 retrieval document 不一致"));
        let ready_error = store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc_a.search_text),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap_err();
        assert!(ready_error
            .to_string()
            .contains("兼容 Vector 写入与当前 retrieval document 不一致"));
        let current_doc: RetrievalDocument = read_yaml(
            &paths::team_store_router_retrieval_doc_path(&team_root, &claim.id),
        )
        .await
        .unwrap();
        assert_eq!(
            current_doc, doc_b,
            "兼容 helper 不得倒写当前 retrieval document"
        );
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            current_state,
            "过时兼容 helper 不得覆盖 B 的当前 Vector state"
        );
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap(),
            "过时 pending helper 不得为 A 留下队列项"
        );
    }

    #[tokio::test]
    async fn legacy_ready_state_is_not_returned_by_public_vector_search() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let fp = fingerprint("model-a", "fixed:2");
        // 公开兼容写入仍允许旧格式，便于旧调用方继续落盘；但搜索必须安全失效它。
        let failed = store_failed_vector_state(
            &team_root,
            &claim.id,
            "0123456789abcdef".into(),
            fp.clone(),
            "legacy worker failure".into(),
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(failed.status, VectorStatus::Failed);
        let state = store_ready_vector_state(
            &team_root,
            &claim.id,
            "0123456789abcdef".into(),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();
        assert_eq!(state.content_hash, "0123456789abcdef");

        let hits = search_ready_vectors(&team_root, &[1.0, 0.0], &fp, 5)
            .await
            .unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn public_vector_search_fences_against_current_retrieval_document() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc_b).await;
        // 模拟旧版本或手工损坏留下的 A state；公开兼容 helper 现在会在写入时拒绝它。
        write_vector_state(
            &team_root,
            &VectorState::ready(
                claim.id.clone(),
                search_text_hash(&doc_a.search_text),
                fp.clone(),
                Some(2),
                1,
                VectorAttemptMetadata::completed(1, Utc::now()),
                vec![1.0, 0.0],
            ),
        )
        .await
        .unwrap();

        let hits = search_ready_vectors(&team_root, &[1.0, 0.0], &fp, 5)
            .await
            .unwrap();
        assert!(hits.is_empty(), "公开入口不能拿 A 向量给当前 B 文档排序");
    }

    #[tokio::test]
    async fn claim_upload_cannot_interleave_source_snapshot_fence() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim_a = sample_claim();
        let source_path = paths::team_store_agent_claims_dir(&team_root, &claim_a.holder)
            .join(format!("{}.yaml", claim_a.id));
        write_yaml_atomic(&source_path, &claim_a).await.unwrap();
        let doc_a = RetrievalDocument::from_claim(&claim_a, vec![], vec![]);

        // 先占住 target 锁，使 Router 已取得 mirror 锁并复读 A 后停在发布前的精确窗口。
        let target_lock_path =
            paths::team_store_router_vector_state_lock_path(&team_root, &claim_a.id);
        let target_guard = FileLockGuard::lock_exclusive(&target_lock_path)
            .await
            .unwrap();
        let source_task = tokio::spawn({
            let team_root = team_root.clone();
            let source_path = source_path.clone();
            let claim_a = claim_a.clone();
            let doc_a = doc_a.clone();
            async move {
                ensure_retrieval_target_for_claim_snapshot(
                    &team_root,
                    &source_path,
                    &claim_a,
                    &doc_a,
                    None,
                )
                .await
            }
        });

        let mirror_lock_path = paths::team_store_agent_claim_mirror_lock_path(
            &team_root,
            &claim_a.holder,
            &claim_a.id,
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(guard) = FileLockGuard::try_lock_exclusive(&mirror_lock_path)
                .await
                .unwrap()
            {
                drop(guard);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Router 应先持有 mirror 锁再等待 target 锁");

        let mut claim_b = claim_a.clone();
        claim_b.statement = "replacement claim body".into();
        let mut upload_task = tokio::spawn({
            let claim_b = claim_b.clone();
            let maintainer = Maintainer::new(
                team_root.clone(),
                chrono::Duration::days(7),
                chrono::Duration::days(30),
                3,
            );
            async move { maintainer.upload_claim(&claim_b).await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut upload_task)
                .await
                .is_err(),
            "Maintainer 上传不能插入 Router 已复核 A、尚未发布 target 的临界窗口"
        );

        drop(target_guard);
        let published_a = source_task.await.unwrap().unwrap();
        assert!(published_a.is_some());
        upload_task.await.unwrap().unwrap();

        let current_claim: Claim = read_yaml(&source_path).await.unwrap();
        assert_eq!(current_claim, claim_b);
        let current_doc: RetrievalDocument = read_yaml(
            &paths::team_store_router_retrieval_doc_path(&team_root, &claim_a.id),
        )
        .await
        .unwrap();
        assert_eq!(current_doc, doc_a, "B 只能在 A 发布完成后写入镜像");

        let doc_b = RetrievalDocument::from_claim(&claim_b, vec![], vec![]);
        let published_b = ensure_retrieval_target_for_claim_snapshot(
            &team_root,
            &source_path,
            &claim_b,
            &doc_b,
            None,
        )
        .await
        .unwrap();
        assert!(published_b.is_some());
        let current_doc: RetrievalDocument = read_yaml(
            &paths::team_store_router_retrieval_doc_path(&team_root, &claim_a.id),
        )
        .await
        .unwrap();
        assert_eq!(current_doc, doc_b);
    }

    #[tokio::test]
    async fn dimension_expectation_survives_failure_and_backoff_retry() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        write_retrieval_doc(&team_root, &doc).await;
        store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            fp.clone(),
            vec![1.0, 0.0],
        )
        .await
        .unwrap();

        assert!(search_ready_vectors(&team_root, &[1.0, 0.0, 0.0], &fp, 5)
            .await
            .unwrap()
            .is_empty());
        let pending = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(pending.status, VectorStatus::Pending);
        assert_eq!(pending.expected_dimensions, Some(3));

        let client = FakeEmbeddingClient::success(fp.clone(), vec![1.0, 0.0]);
        let policy = retry_policy(10, 100);
        process_pending_queue(team_root.clone(), Arc::new(client.clone()), 1, policy)
            .await
            .unwrap();
        let failed = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(failed.status, VectorStatus::Failed);
        assert_eq!(failed.expected_dimensions, Some(3));
        assert_eq!(failed.attempts, 1);

        ensure_vector_pending_at(&team_root, &doc, &fp, failed.next_retry_at.unwrap())
            .await
            .unwrap();
        let retry_entries =
            read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap();
        assert_eq!(retry_entries.len(), 1);
        assert_eq!(retry_entries[0].expected_dimensions, Some(3));

        process_pending_queue(team_root.clone(), Arc::new(client.clone()), 1, policy)
            .await
            .unwrap();
        let failed_again = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(failed_again.status, VectorStatus::Failed);
        assert_eq!(failed_again.expected_dimensions, Some(3));
        assert_eq!(failed_again.attempts, 2);
        assert_eq!(client.calls(), 2);
    }

    #[tokio::test]
    async fn changed_query_dimension_resets_failed_dimension_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("dynamic-model", "response_length");
        let now = Utc::now();
        write_retrieval_doc(&team_root, &doc).await;
        write_vector_state(
            &team_root,
            &VectorState::failed(
                claim.id.clone(),
                search_text_hash(&doc.search_text),
                fp.clone(),
                Some(3),
                1,
                VectorAttemptMetadata::failed(4, now, now + chrono::Duration::hours(1)),
                "old dimension".into(),
            ),
        )
        .await
        .unwrap();

        assert!(
            search_ready_vectors(&team_root, &[1.0, 0.0, 0.0, 0.0], &fp, 5)
                .await
                .unwrap()
                .is_empty()
        );
        let pending = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(pending.status, VectorStatus::Pending);
        assert_eq!(pending.expected_dimensions, Some(4));
        assert_eq!(pending.attempts, 0);
        let queued = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].expected_dimensions, Some(4));

        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0, 0.0, 0.0]);
        process_pending_queue(
            team_root.clone(),
            Arc::new(client),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.status, VectorStatus::Ready);
        assert_eq!(ready.vector_dimensions, Some(4));
        assert_eq!(ready.expected_dimensions, Some(4));
    }

    #[tokio::test]
    async fn legacy_state_without_fingerprint_is_invalidated() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        let legacy: VectorState = serde_json::from_value(serde_json::json!({
            "claim_id": claim.id,
            "status": "ready",
            "updated_at": "2026-07-10T00:00:00Z",
            "content_hash": search_text_hash(&doc.search_text),
            "vector": [1.0, 0.0]
        }))
        .unwrap();
        assert_eq!(legacy.generation_seq, 0);
        assert_eq!(legacy.embedding_fingerprint, None);
        assert_eq!(legacy.vector_dimensions, None);
        assert_eq!(legacy.expected_dimensions, None);
        assert_eq!(legacy.attempts, 0);
        assert_eq!(legacy.last_attempt_at, None);
        assert_eq!(legacy.next_retry_at, None);
        let legacy_entry: VectorQueueEntry = serde_json::from_value(serde_json::json!({
            "claim_id": claim.id,
            "content_hash": search_text_hash(&doc.search_text),
            "embedding_fingerprint": fp.clone(),
            "enqueued_at": "2026-07-10T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(legacy_entry.generation_seq, 0);
        write_vector_state(&team_root, &legacy).await.unwrap();

        let state = ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        assert_eq!(state.status, VectorStatus::Pending);
        assert_eq!(state.embedding_fingerprint.as_ref(), Some(&fp));
    }

    #[tokio::test]
    async fn delayed_old_worker_result_cannot_overwrite_new_fingerprint_state() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-a", "fixed:2");
        let new_fp = fingerprint("model-b", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        ensure_vector_pending(&team_root, &doc, &old_fp)
            .await
            .unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingEmbeddingClient {
                fingerprint: old_fp,
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(10, 100)).await
            }
        });
        started.notified().await;

        ensure_vector_pending(&team_root, &doc, &new_fp)
            .await
            .unwrap();
        release.notify_one();
        worker.await.unwrap().unwrap();

        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Pending);
        assert_eq!(state.embedding_fingerprint.as_ref(), Some(&new_fp));
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert!(pending
            .iter()
            .any(|entry| { entry.embedding_fingerprint.as_ref() == Some(&new_fp) }));
    }

    #[tokio::test]
    async fn stale_queue_entry_for_replaced_document_is_consumed_without_successor() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc_a).await;
        let state_a = ensure_vector_pending(&team_root, &doc_a, &fp)
            .await
            .unwrap();

        // 模拟旧版本遗留的“只有新 document、没有恢复 intent”状态；旧 A worker 不得猜测 B。
        write_retrieval_doc(&team_root, &doc_b).await;
        let client = FakeEmbeddingClient::success(fp.clone(), vec![1.0, 0.0]);
        process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 0);
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            state_a
        );
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );

        let state_b = ensure_retrieval_target(&team_root, &doc_b, Some(&fp))
            .await
            .unwrap()
            .vector_state
            .expect("正常 query 应为 B 建立新的 Vector target");
        assert_eq!(state_b.status, VectorStatus::Pending);
        assert_eq!(state_b.content_hash, search_text_hash(&doc_b.search_text));
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending[0].content_hash,
            search_text_hash(&doc_b.search_text)
        );
    }

    #[tokio::test]
    async fn failed_target_publication_is_recovered_from_intent_without_second_query() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_b = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");

        // 让 queue 创建失败，模拟 intent + document 已提交、state/queue 尚未完成的窗口。
        let queue_path = paths::team_store_router_vector_queue_path(&team_root);
        tokio::fs::create_dir_all(&queue_path).await.unwrap();
        let failed_target = ensure_retrieval_target(&team_root, &doc_b, Some(&fp))
            .await
            .unwrap();
        assert!(failed_target.vector_state.is_none());
        assert!(failed_target.vector_error.is_some());
        assert!(
            tokio::fs::try_exists(paths::team_store_router_vector_intent_path(
                &team_root, &claim.id
            ))
            .await
            .unwrap()
        );
        let persisted_doc: RetrievalDocument = read_yaml(
            &paths::team_store_router_retrieval_doc_path(&team_root, &claim.id),
        )
        .await
        .unwrap();
        assert_eq!(persisted_doc, doc_b);
        assert!(load_vector_state_opt(&team_root, &claim.id)
            .await
            .unwrap()
            .is_none());

        tokio::fs::remove_dir(&queue_path).await.unwrap();
        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);
        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();

        assert_eq!(report.processed, 1);
        assert_eq!(client.calls(), 1);
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert_eq!(state.content_hash, search_text_hash(&doc_b.search_text));
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_intent_path(
                &team_root, &claim.id
            ))
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn fingerprint_rotation_target_failure_is_recovered_from_intent() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-a", "fixed:2");
        let new_fp = fingerprint("model-b", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        let _ = store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            old_fp,
            vec![1.0, 0.0],
        )
        .await
        .unwrap();

        // 文档未变化、只有 embedding 指纹换代时，queue 失败前也必须先持久化 intent。
        let queue_path = paths::team_store_router_vector_queue_path(&team_root);
        tokio::fs::create_dir_all(&queue_path).await.unwrap();
        let failed_target = ensure_retrieval_target(&team_root, &doc, Some(&new_fp))
            .await
            .unwrap();
        assert!(failed_target.vector_state.is_none());
        assert!(failed_target.vector_error.is_some());
        let intent_path = paths::team_store_router_vector_intent_path(&team_root, &claim.id);
        let intent = read_vector_target_intent(&intent_path).await.unwrap();
        assert_eq!(intent.embedding_fingerprint, new_fp);
        let persisted_doc: RetrievalDocument = read_yaml(
            &paths::team_store_router_retrieval_doc_path(&team_root, &claim.id),
        )
        .await
        .unwrap();
        assert_eq!(persisted_doc, doc);

        tokio::fs::remove_dir(&queue_path).await.unwrap();
        let client = FakeEmbeddingClient::success(new_fp.clone(), vec![1.0, 0.0]);
        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();

        assert_eq!(report.processed, 1);
        assert_eq!(client.calls(), 1);
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert_eq!(state.embedding_fingerprint.as_ref(), Some(&new_fp));
        assert!(
            !tokio::fs::try_exists(intent_path).await.unwrap(),
            "成功重放后应清理 fingerprint 换代 intent"
        );
    }

    #[tokio::test]
    async fn intent_before_document_commit_is_discarded_without_guessing_successor() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc_a).await;
        write_vector_target_intent(
            &team_root,
            &VectorTargetIntent::from_retrieval_doc(&doc_b, fp.clone()),
        )
        .await
        .unwrap();

        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);
        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();

        assert_eq!(report.processed, 0);
        assert_eq!(client.calls(), 0);
        assert!(load_vector_state_opt(&team_root, &claim.id)
            .await
            .unwrap()
            .is_none());
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_intent_path(
                &team_root, &claim.id
            ))
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn legacy_hash_queue_and_intent_are_safely_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;

        let legacy_hash = "0123456789abcdef".to_owned();
        write_vector_target_intent(
            &team_root,
            &VectorTargetIntent {
                claim_id: claim.id.clone(),
                content_hash: legacy_hash.clone(),
                embedding_fingerprint: fp.clone(),
            },
        )
        .await
        .unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash: legacy_hash,
                generation_seq: 0,
                embedding_fingerprint: Some(fp.clone()),
                expected_dimensions: None,
                enqueued_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);
        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();

        assert_eq!(report.processed, 1);
        assert_eq!(client.calls(), 0);
        assert!(load_vector_state_opt(&team_root, &claim.id)
            .await
            .unwrap()
            .is_none());
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_intent_path(
                &team_root, &claim.id
            ))
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn stale_worker_completion_does_not_publish_old_vector_after_document_changes() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let mut doc_a = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        doc_a.search_text = "content-generation-a".into();
        let mut doc_b = doc_a.clone();
        doc_b.search_text = "content-generation-b".into();
        let fp = fingerprint("model-a", "fixed:2");
        let target_a = ensure_retrieval_target(&team_root, &doc_a, Some(&fp))
            .await
            .unwrap();
        assert!(target_a.vector_error.is_none());
        target_a.vector_state.expect("A 应建立 Vector target");

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let worker = tokio::spawn({
            let team_root = team_root.clone();
            let client = BlockingEmbeddingClient {
                fingerprint: fp.clone(),
                started: started.clone(),
                release: release.clone(),
            };
            async move {
                process_pending_queue(team_root, Arc::new(client), 1, retry_policy(10, 100)).await
            }
        });
        started.notified().await;

        let pending_b = ensure_retrieval_target(&team_root, &doc_b, Some(&fp))
            .await
            .unwrap()
            .vector_state
            .expect("B 应建立新的 Vector target");
        release.notify_one();
        worker.await.unwrap().unwrap();

        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            pending_b
        );
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending[0].content_hash,
            search_text_hash(&doc_b.search_text)
        );

        process_pending_queue(
            team_root.clone(),
            Arc::new(FakeEmbeddingClient::success(fp, vec![1.0, 0.0])),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        let ready_b = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready_b.status, VectorStatus::Ready);
        assert_eq!(ready_b.content_hash, search_text_hash(&doc_b.search_text));
    }

    #[tokio::test]
    async fn mismatched_worker_requeues_without_overwriting_current_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-a", "fixed:2");
        let new_fp = fingerprint("model-b", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        ensure_vector_pending(&team_root, &doc, &new_fp)
            .await
            .unwrap();
        let client = FakeEmbeddingClient::success(old_fp, vec![1.0, 0.0]);

        let report = process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(report.failures, 0);
        assert_eq!(client.calls(), 0);
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Pending);
        assert_eq!(state.embedding_fingerprint.as_ref(), Some(&new_fp));
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].embedding_fingerprint.as_ref(), Some(&new_fp));
    }

    #[tokio::test]
    async fn old_fingerprint_entry_cannot_use_content_change_to_replace_newer_state() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-old", "fixed:2");
        let new_fp = fingerprint("model-new", "fixed:2");
        let actual_hash = search_text_hash(&doc.search_text);
        let ready_at = Utc::now();
        write_retrieval_doc(&team_root, &doc).await;
        let newer = VectorState::ready(
            claim.id.clone(),
            actual_hash,
            new_fp,
            Some(2),
            5,
            VectorAttemptMetadata::completed(1, ready_at),
            vec![1.0, 0.0],
        );
        write_vector_state(&team_root, &newer).await.unwrap();
        append_queue_entry(
            &team_root,
            &VectorQueueEntry {
                claim_id: claim.id.clone(),
                content_hash: search_text_hash("obsolete content"),
                generation_seq: 4,
                embedding_fingerprint: Some(old_fp.clone()),
                expected_dimensions: Some(2),
                enqueued_at: ready_at + chrono::Duration::hours(1),
            },
        )
        .await
        .unwrap();
        let client = FakeEmbeddingClient::success(old_fp, vec![1.0, 0.0]);

        process_pending_queue(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(client.calls(), 0);
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            newer
        );
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_queue_path(&team_root))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn lower_content_repair_cannot_overtake_higher_live_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let old_fp = fingerprint("model-old", "fixed:2");
        let new_fp = fingerprint("model-new", "fixed:2");
        let old_hash = search_text_hash("obsolete content");
        let actual_hash = search_text_hash(&doc.search_text);
        write_retrieval_doc(&team_root, &doc).await;
        let initial =
            VectorState::pending(claim.id.clone(), old_hash.clone(), old_fp.clone(), None, 1);
        write_vector_state(&team_root, &initial).await.unwrap();

        let lower = VectorQueueEntry {
            claim_id: claim.id.clone(),
            content_hash: old_hash,
            generation_seq: 2,
            embedding_fingerprint: Some(old_fp.clone()),
            expected_dimensions: None,
            enqueued_at: Utc::now(),
        };
        let lower_path = paths::team_store_router_vector_queue_dir(&team_root)
            .join("pending.lower.inflight.jsonl");
        write_queue_entries_atomic(&lower_path, std::slice::from_ref(&lower))
            .await
            .unwrap();
        let lower_lease_path = in_flight_lease_path(&lower_path);
        let lower_lease = FileLockGuard::lock_exclusive(&lower_lease_path)
            .await
            .unwrap();
        let higher = VectorQueueEntry {
            claim_id: claim.id.clone(),
            content_hash: actual_hash,
            generation_seq: 3,
            embedding_fingerprint: Some(new_fp.clone()),
            expected_dimensions: None,
            enqueued_at: Utc::now(),
        };
        append_queue_entry(&team_root, &higher).await.unwrap();

        let old_client = FakeEmbeddingClient::success(old_fp, vec![1.0, 0.0]);
        process_queue_entry(
            &team_root,
            Arc::new(old_client.clone()),
            lower.clone(),
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(old_client.calls(), 0);
        assert_eq!(
            load_vector_state(&team_root, &claim.id).await.unwrap(),
            initial
        );
        let pending = read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();
        assert_eq!(pending, vec![higher]);

        let new_client = FakeEmbeddingClient::success(new_fp.clone(), vec![1.0, 0.0]);
        process_pending_queue(
            team_root.clone(),
            Arc::new(new_client.clone()),
            1,
            retry_policy(10, 100),
        )
        .await
        .unwrap();
        assert_eq!(new_client.calls(), 1);
        let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(ready.status, VectorStatus::Ready);
        assert_eq!(ready.generation_seq, 3);
        assert_eq!(ready.embedding_fingerprint, Some(new_fp));

        finish_in_flight_queue(
            &team_root,
            DrainedQueue {
                entries: vec![lower],
                in_flight_path: lower_path,
                lease_path: lower_lease_path,
                lease: lower_lease,
            },
            &[],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn new_fingerprint_or_content_generation_survives_completed_inflight_cleanup() {
        for changed_axis in ["fingerprint", "content"] {
            let dir = tempfile::tempdir().unwrap();
            let team_root = dir.path().to_path_buf();
            let claim = sample_claim();
            let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
            let target_fp = fingerprint("model-target", "fixed:2");
            let current_fp = if changed_axis == "fingerprint" {
                fingerprint("model-current", "fixed:2")
            } else {
                target_fp.clone()
            };
            let target_hash = search_text_hash(&doc.search_text);
            let current_hash = if changed_axis == "content" {
                "superseded-content-generation".into()
            } else {
                target_hash.clone()
            };
            write_retrieval_doc(&team_root, &doc).await;
            write_vector_state(
                &team_root,
                &VectorState::pending(claim.id.clone(), current_hash, current_fp, None, 0),
            )
            .await
            .unwrap();

            let target_entry = queue_entry(&doc, Some(target_fp.clone()));
            let in_flight_path = paths::team_store_router_vector_queue_dir(&team_root)
                .join(format!("pending.{changed_axis}.inflight.jsonl"));
            write_queue_entries_atomic(&in_flight_path, std::slice::from_ref(&target_entry))
                .await
                .unwrap();
            let lease_path = in_flight_lease_path(&in_flight_path);
            let lease = FileLockGuard::lock_exclusive(&lease_path).await.unwrap();

            let mismatched_client = FakeEmbeddingClient::success(
                fingerprint("model-mismatched-worker", "fixed:2"),
                vec![1.0, 0.0],
            );
            let outcome = process_queue_entry(
                &team_root,
                Arc::new(mismatched_client.clone()),
                target_entry.clone(),
                retry_policy(10, 100),
            )
            .await
            .unwrap();
            assert!(matches!(outcome, QueueEntryOutcome::Complete));
            assert_eq!(mismatched_client.calls(), 0);

            let state = ensure_vector_pending(&team_root, &doc, &target_fp)
                .await
                .unwrap();
            assert_eq!(state.status, VectorStatus::Pending);
            assert_eq!(state.content_hash, target_hash);
            assert_eq!(state.embedding_fingerprint.as_ref(), Some(&target_fp));

            // 复现旧 worker 已经决定 Complete 后的批次清理；pending 独立副本必须保留。
            finish_in_flight_queue(
                &team_root,
                DrainedQueue {
                    entries: vec![target_entry.clone()],
                    in_flight_path,
                    lease_path,
                    lease,
                },
                &[],
            )
            .await
            .unwrap();
            let pending =
                read_queue_entries(&paths::team_store_router_vector_queue_path(&team_root))
                    .await
                    .unwrap();
            assert_eq!(pending.len(), 1);
            let mut expected_pending = target_entry.clone();
            expected_pending.generation_seq = 1;
            assert_eq!(
                queue_entry_key(&pending[0]),
                queue_entry_key(&expected_pending)
            );

            let matching_client = FakeEmbeddingClient::success(target_fp.clone(), vec![1.0, 0.0]);
            process_pending_queue(
                team_root.clone(),
                Arc::new(matching_client.clone()),
                1,
                retry_policy(10, 100),
            )
            .await
            .unwrap();
            assert_eq!(matching_client.calls(), 1);
            let ready = load_vector_state(&team_root, &claim.id).await.unwrap();
            assert_eq!(ready.status, VectorStatus::Ready);
            assert_eq!(ready.embedding_fingerprint.as_ref(), Some(&target_fp));
        }
    }

    #[tokio::test]
    async fn worker_processes_queue_before_first_poll_sleep() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        write_retrieval_doc(&team_root, &doc).await;
        ensure_vector_pending(&team_root, &doc, &fp).await.unwrap();
        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(run_vector_worker(
            team_root,
            Arc::new(client.clone()),
            1,
            Duration::from_secs(3_600),
            retry_policy(10, 100),
            cancel.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while client.calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn worker_retries_target_intent_after_temporary_queue_storage_error() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        let fp = fingerprint("model-a", "fixed:2");
        let queue_path = paths::team_store_router_vector_queue_path(&team_root);
        tokio::fs::create_dir_all(&queue_path).await.unwrap();
        let failed_target = ensure_retrieval_target(&team_root, &doc, Some(&fp))
            .await
            .unwrap();
        assert!(failed_target.vector_error.is_some());

        let client = FakeEmbeddingClient::success(fp, vec![1.0, 0.0]);
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(run_vector_worker(
            team_root.clone(),
            Arc::new(client.clone()),
            1,
            Duration::from_millis(5),
            retry_policy(10, 100),
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !worker.is_finished(),
            "临时 queue 存储错误不能让 worker 直接退出"
        );
        tokio::fs::remove_dir(&queue_path).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        cancel.cancel();
        worker.await.unwrap().unwrap();
        let state = load_vector_state(&team_root, &claim.id).await.unwrap();
        assert_eq!(state.status, VectorStatus::Ready);
        assert!(
            !tokio::fs::try_exists(paths::team_store_router_vector_intent_path(
                &team_root, &claim.id
            ))
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn queue_append_failure_does_not_leave_pending_state() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let retrieval_doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        tokio::fs::create_dir_all(paths::team_store_router_vector_queue_dir(&team_root))
            .await
            .unwrap();
        tokio::fs::create_dir(paths::team_store_router_vector_queue_path(&team_root))
            .await
            .unwrap();

        let err = ensure_vector_pending_at(
            &team_root,
            &retrieval_doc,
            &fingerprint("model-a", "fixed:2"),
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("向量队列"));
        assert!(load_vector_state_opt(&team_root, &claim.id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn malformed_queue_line_is_skipped_without_failing_drain() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let valid = serde_json::to_string(&VectorQueueEntry {
            claim_id: claim.id.clone(),
            content_hash: "hash".into(),
            generation_seq: 0,
            embedding_fingerprint: Some(fingerprint("model-a", "fixed:2")),
            expected_dimensions: None,
            enqueued_at: Utc::now(),
        })
        .unwrap();
        let queue_path = paths::team_store_router_vector_queue_path(&team_root);
        tokio::fs::create_dir_all(queue_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&queue_path, format!("{{bad json}}\n{valid}\n"))
            .await
            .unwrap();

        let drained = drain_queue_entries(&team_root).await.unwrap().unwrap();
        assert_eq!(drained.entries.len(), 1);
        assert_eq!(drained.entries[0].claim_id, claim.id);
        finish_in_flight_queue(&team_root, drained, &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn search_ready_vectors_filters_zero_similarity_hits() {
        let dir = tempfile::tempdir().unwrap();
        let team_root = dir.path().to_path_buf();
        let claim = sample_claim();
        let fp = fingerprint("model-a", "fixed:2");
        let doc = RetrievalDocument::from_claim(&claim, vec![], vec![]);
        write_retrieval_doc(&team_root, &doc).await;
        let _ = store_ready_vector_state(
            &team_root,
            &claim.id,
            search_text_hash(&doc.search_text),
            fp.clone(),
            vec![-1.0_f32, 0.0_f32],
        )
        .await
        .unwrap();

        let hits = search_ready_vectors(&team_root, &[1.0_f32, 0.0_f32], &fp, 5)
            .await
            .unwrap();
        assert!(hits.is_empty());
        let error = search_ready_vectors(&team_root, &[0.0_f32, 0.0_f32], &fp, 5)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("查询 embedding 无效"));
        assert_eq!(
            load_vector_state(&team_root, &claim.id)
                .await
                .unwrap()
                .status,
            VectorStatus::Ready
        );
    }
}
