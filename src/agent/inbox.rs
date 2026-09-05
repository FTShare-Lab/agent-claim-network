//! inbox 处理流程。
//!
//! 本模块承接 `AgentRunner::process_inbox` 及其内部 provider-neutral 内化和 inbox trace 写入逻辑，
//! 保持 runner 只承担 agent 本地资源和 inbox 边界。

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use opentelemetry::trace::{Span, Tracer};
use opentelemetry::KeyValue;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use super::fs::reported_dispute_claim_set_key;
use super::prepare::{
    llm_visible_claims, prepare_claim_updates, prepare_claims, prepare_disputes, sorted_source_ids,
    validate_visible_policy_sources,
};
use super::runner::{AgentRunner, InboxProcessReport, TeamServiceConnectionStatus};
use super::traits::ClaimedInboxMessage;
use crate::api::{
    resolve_placeholders, BufferedProviderRuntime, ClaimAttributeUpdateInternalizeItem,
    ClaimAttributeUpdateInternalizeRequest, InboxInternalizeKind, InternalizeOutcome,
    InternalizeRequest, ProviderRuntimeFallbackScope, ProviderTransport, SessionTurnMessage,
    StructuredJsonAttemptRequest, StructuredJsonCaller,
};
use crate::claim::{
    ArbitrationResolutionContext, ArbitrationResolutionId, Claim, ClaimId, ClaimStatus, Dispute,
    DisputeId, InboxId, InboxMessage, InboxMessageKind, Policy, PolicyId, PolicyStatus, SourceId,
    Trace, TraceId,
};
use crate::maintainer::traits::MaintainerClientError;
use crate::prompt::PromptRegistry;
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard, StorageError};
use crate::tracing::tracer;

pub(crate) type PreparedClaimAttributeUpdate =
    (DateTime<Utc>, Vec<Claim>, Vec<Claim>, Vec<Dispute>);
pub(crate) type ClaimAttributeUpdateJsonValidator<'a> =
    dyn FnMut(serde_json::Value) -> anyhow::Result<PreparedClaimAttributeUpdate> + Send + 'a;
type PreparedInternalization = (DateTime<Utc>, Vec<Claim>, Vec<Claim>, Vec<Dispute>);

#[derive(Default)]
struct PendingInboxUpload {
    claims: Vec<Claim>,
    disputes: Vec<Dispute>,
}

struct AppliedInboxEffect {
    summary: InternalizeSummary,
}

#[async_trait]
pub(crate) trait InboxJsonGenerator: Send + Sync {
    async fn generate_json(
        &self,
        kind: InboxInternalizeKind,
        request: InternalizeRequest,
        preferred_transport: Option<ProviderTransport>,
    ) -> anyhow::Result<serde_json::Value>;

    async fn generate_claim_attribute_update_json(
        &self,
        _request: ClaimAttributeUpdateInternalizeRequest,
    ) -> anyhow::Result<serde_json::Value> {
        anyhow::bail!("当前 inbox generator 未实现 ClaimAttributeUpdate 批量内化输入")
    }

    async fn generate_validated_claim_attribute_update_json(
        &self,
        request: ClaimAttributeUpdateInternalizeRequest,
        validator: &mut ClaimAttributeUpdateJsonValidator<'_>,
    ) -> anyhow::Result<PreparedClaimAttributeUpdate> {
        let value = self.generate_claim_attribute_update_json(request).await?;
        validator(value)
    }
}

pub(crate) struct PromptInboxJsonGenerator {
    prompt_registry: Arc<PromptRegistry>,
    json_caller: Arc<StructuredJsonCaller>,
    fallback_scope: ProviderRuntimeFallbackScope,
}

impl PromptInboxJsonGenerator {
    pub(crate) fn new(
        prompt_registry: Arc<PromptRegistry>,
        json_caller: Arc<StructuredJsonCaller>,
    ) -> Self {
        Self {
            prompt_registry,
            json_caller,
            fallback_scope: ProviderRuntimeFallbackScope::new_root(),
        }
    }
}

#[async_trait]
impl InboxJsonGenerator for PromptInboxJsonGenerator {
    async fn generate_json(
        &self,
        kind: InboxInternalizeKind,
        request: InternalizeRequest,
        preferred_transport: Option<ProviderTransport>,
    ) -> anyhow::Result<serde_json::Value> {
        let prompt_name = match kind {
            InboxInternalizeKind::PolicyUpdate => "inbox_policy_update_internalize",
            InboxInternalizeKind::ClaimAttributeUpdate => {
                "inbox_claim_attribute_update_internalize"
            }
        };
        let system_prompt = self
            .prompt_registry
            .render(prompt_name, ())
            .map_err(anyhow::Error::from)?;
        let user_text = serde_json::to_string_pretty(&request)?;
        self.json_caller
            .generate_json_streaming_once(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
                BufferedProviderRuntime::new(self.fallback_scope.clone()),
                preferred_transport,
            )
            .await
    }

    async fn generate_claim_attribute_update_json(
        &self,
        request: ClaimAttributeUpdateInternalizeRequest,
    ) -> anyhow::Result<serde_json::Value> {
        let system_prompt = self
            .prompt_registry
            .render("inbox_claim_attribute_update_internalize", ())
            .map_err(anyhow::Error::from)?;
        let user_text = serde_json::to_string_pretty(&request)?;
        self.json_caller
            .generate_json(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
            )
            .await
    }

    async fn generate_validated_claim_attribute_update_json(
        &self,
        request: ClaimAttributeUpdateInternalizeRequest,
        validator: &mut ClaimAttributeUpdateJsonValidator<'_>,
    ) -> anyhow::Result<PreparedClaimAttributeUpdate> {
        let agent_id = request.agent_id.clone();
        let system_prompt = self
            .prompt_registry
            .render("inbox_claim_attribute_update_internalize", ())
            .map_err(anyhow::Error::from)?;
        let user_text = serde_json::to_string_pretty(&request)?;
        self.json_caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::claim_attribute_update(
                    system_prompt,
                    vec![SessionTurnMessage::user_text(user_text)],
                ),
                validator,
                |retry, total, error| {
                    log::warn!(
                        target: "agent",
                        "agent {} ClaimAttributeUpdate 输出校验失败，重试 ({retry}/{total}): {error:#}",
                        agent_id
                    );
                },
                |_| std::future::ready(()),
                |_, _| Ok(()),
            )
            .await
    }
}

#[derive(Debug, Default)]
struct InboxSyncReport {
    pull_succeeded: bool,
    accepted: usize,
    warnings: Vec<String>,
}

const INBOX_EFFECT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InboxEffectState {
    Prepared,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PlannedClaimUpdate {
    target: Claim,
    preimage_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InboxEffectPlan {
    schema_version: u32,
    inbox_id: InboxId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolution_id: Option<ArbitrationResolutionId>,
    message_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    batch_members: Vec<InboxEffectMember>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    batch_hash: Option<String>,
    state: InboxEffectState,
    #[serde(with = "crate::time::serde_utc")]
    prepared_at: DateTime<Utc>,
    new_claims: Vec<Claim>,
    updated_claims: Vec<PlannedClaimUpdate>,
    #[serde(default)]
    deprecated_claim_ids: Vec<ClaimId>,
    new_disputes: Vec<Dispute>,
    trace: Option<Trace>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InboxEffectMember {
    inbox_id: InboxId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolution_id: Option<ArbitrationResolutionId>,
    message_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InboxEffectRef {
    schema_version: u32,
    canonical_inbox_id: InboxId,
    batch_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum InboxEffectRecord {
    Plan(Box<InboxEffectPlan>),
    Ref(InboxEffectRef),
}

struct LoadedInboxEffectBatch {
    plan: InboxEffectPlan,
    canonical_path: std::path::PathBuf,
    members: FxHashMap<InboxId, InboxEffectMember>,
}

impl AgentRunner {
    /// 排空 inbox 中所有 pending 消息。
    ///
    /// 处理纪律：
    /// - 连续同类型消息收集成 batch 后交给 LLM；ClaimAttributeUpdate 以批量 Effect
    ///   Journal 保留逐消息幂等与恢复边界
    /// - 单次最多处理 1024 条，避免极端 inbox 堆积让一次 session 卡太久
    pub async fn process_inbox(&self) -> anyhow::Result<InboxProcessReport> {
        self.process_inbox_with(self.inbox_generator.as_ref()).await
    }

    pub(super) async fn process_inbox_with(
        &self,
        generator: &dyn InboxJsonGenerator,
    ) -> anyhow::Result<InboxProcessReport> {
        let _guard = self.inbox_process_lock.lock().await;
        self.recover_pending_claim_edit().await?;
        let mut report = InboxProcessReport::default();
        if self.team_services_configured() {
            let sync_report = self.sync_inbox_to_local().await?;
            report.team_services.maintainer = if sync_report.pull_succeeded {
                TeamServiceConnectionStatus::Connected
            } else {
                TeamServiceConnectionStatus::Failed
            };
            report.warnings.extend(sync_report.warnings);
            if sync_report.pull_succeeded {
                // 收件持久化和 receipt ACK 已完成后，才允许历史 pending upload 重试。
                push_upload_warning(
                    &mut report.warnings,
                    self.upload_maintainer_batch(Vec::new(), Vec::new()).await?,
                );
            }
            if let Some(router) = self.context.router.as_ref() {
                match router.scopes_overview().await {
                    Ok(snapshot) => {
                        report.team_services.router = TeamServiceConnectionStatus::Connected;
                        report.router_scopes_overview = Some(snapshot);
                    }
                    Err(err) => {
                        report.team_services.router = TeamServiceConnectionStatus::Failed;
                        let warning = format!("Router scope 概览获取失败：{err}");
                        log::warn!(target: "agent", "agent {} {warning}", self.agent_id);
                        report.warnings.push(warning);
                    }
                }
            }
        }
        self.process_local_inbox(generator, &mut report).await?;
        Ok(report)
    }

    /// 从 Maintainer 拉取消息，逐条持久化到本地后批量发送 receipt ACK。
    ///
    /// ACK 只确认本地持久收件；失败会记录 warning，但不会阻止本地 pending 消息继续处理。
    async fn sync_inbox_to_local(&self) -> anyhow::Result<InboxSyncReport> {
        let mut report = InboxSyncReport::default();
        let Some(maintainer_client) = self.maintainer_client.as_ref() else {
            return Ok(report);
        };
        let pulled = match maintainer_client.pull_inbox(&self.agent_id).await {
            Ok(msgs) => {
                report.pull_succeeded = true;
                msgs
            }
            Err(err) => {
                let warning = inbox_pull_warning(&err);
                log::warn!(
                    target: "agent",
                    "agent {} {warning}",
                    self.agent_id,
                );
                report.warnings.push(warning);
                return Ok(report);
            }
        };

        let mut persisted_ids = Vec::with_capacity(pulled.len());
        let mut persisted_id_set = FxHashSet::default();
        for msg in &pulled {
            if let Err(err) = self.inbox.accept_pulled(msg).await {
                // 即便本批后续落盘失败，也要尽力确认已经持久化的前缀，避免永久重投。
                let _ack_warning = self.ack_persisted_inbox(&persisted_ids).await;
                return Err(err.context(format!(
                    "agent {} 持久化 maintainer inbox 失败 msg_id={}；此前已落盘并尝试 ACK {} 条",
                    self.agent_id,
                    msg.id,
                    persisted_ids.len()
                )));
            }
            if persisted_id_set.insert(msg.id.clone()) {
                persisted_ids.push(msg.id.clone());
            }
        }
        report.accepted = persisted_ids.len();
        if let Some(warning) = self.ack_persisted_inbox(&persisted_ids).await {
            report.warnings.push(warning);
        }
        if report.accepted > 0 {
            log::info!(
                target: "agent",
                "agent {} pull_inbox 拉到并持久收件 {} 条消息，已尝试 receipt ACK",
                self.agent_id,
                report.accepted
            );
        }
        Ok(report)
    }

    async fn ack_persisted_inbox(&self, inbox_ids: &[InboxId]) -> Option<String> {
        if inbox_ids.is_empty() {
            return None;
        }
        let maintainer_client = self.maintainer_client.as_ref()?;
        match maintainer_client.ack_inbox(&self.agent_id, inbox_ids).await {
            Ok(()) => None,
            Err(err) => {
                let warning = inbox_ack_warning(&err);
                log::warn!(target: "agent", "agent {} {warning}", self.agent_id);
                Some(warning)
            }
        }
    }

    /// 只处理已经进入本地 Inbox 生命周期的消息，不访问 Maintainer 远端 outbox。
    async fn process_local_inbox(
        &self,
        generator: &dyn InboxJsonGenerator,
        report: &mut InboxProcessReport,
    ) -> anyhow::Result<()> {
        let mut batch_kind: Option<InboxInternalizeKind> = None;
        let mut llm_msgs: Vec<ClaimedInboxMessage> = Vec::new();

        let claimed = self.inbox.claim_pending().await?;
        let result = self
            .process_claimed_inbox(
                generator,
                claimed.clone(),
                report,
                &mut batch_kind,
                &mut llm_msgs,
            )
            .await;
        if let Err(err) = result {
            for claimed_msg in &claimed {
                if let Err(release_err) = self.inbox.release_claimed(claimed_msg).await {
                    log::warn!(
                        target: "agent",
                        "agent {} release inbox lease 失败 msg_id={}: {release_err:#}",
                        self.agent_id,
                        claimed_msg.message.id
                    );
                }
            }
            return Err(err);
        }
        for claimed_msg in claimed.iter().skip(report.total) {
            self.inbox.release_claimed(claimed_msg).await?;
        }

        if report.total > 0 {
            log::info!(
                target: "agent",
                "agent {} 处理 inbox 消息 {} 条 (PolicyUpdate {} 条, ClaimAttributeUpdate {} 条 → 新 claim {} / 更新 claim {} / dispute {})",
                self.agent_id,
                report.total,
                report.policy_count,
                report.claim_attribute_count,
                report.new_claim_ids.len(),
                report.updated_claim_ids.len(),
                report.new_dispute_ids.len()
            );
        }
        Ok(())
    }

    async fn process_claimed_inbox(
        &self,
        generator: &dyn InboxJsonGenerator,
        claimed: Vec<ClaimedInboxMessage>,
        report: &mut InboxProcessReport,
        batch_kind: &mut Option<InboxInternalizeKind>,
        llm_msgs: &mut Vec<ClaimedInboxMessage>,
    ) -> anyhow::Result<()> {
        const MAX_DRAIN: usize = 1024;
        for claimed_msg in claimed.into_iter().take(MAX_DRAIN) {
            match claimed_msg.message.kind.clone() {
                InboxMessageKind::PolicyUpdate { policy }
                    if policy.status == PolicyStatus::Deprecated =>
                {
                    self.flush_internalize_updates(generator, batch_kind, llm_msgs, report)
                        .await?;
                    let summary = self.apply_policy_deprecation(&policy).await?;
                    self.inbox.ack_claimed(&claimed_msg).await?;
                    report.policy_deprecation_count += 1;
                    report
                        .deprecated_claim_ids
                        .extend(summary.deprecated_claim_ids);
                    report.warnings.extend(summary.warnings);
                    if let Some(trace_id) = summary.trace_id {
                        report.trace_ids.push(trace_id);
                    }
                }
                InboxMessageKind::PolicyUpdate { .. } => {
                    // 不在这里 ack：等批量交给 LLM 内化后，落地成功才 ack
                    self.push_internalize_message(
                        generator,
                        InboxInternalizeKind::PolicyUpdate,
                        claimed_msg,
                        batch_kind,
                        llm_msgs,
                        report,
                    )
                    .await?;
                }
                InboxMessageKind::ClaimAttributeUpdate { .. } => {
                    self.push_internalize_message(
                        generator,
                        InboxInternalizeKind::ClaimAttributeUpdate,
                        claimed_msg,
                        batch_kind,
                        llm_msgs,
                        report,
                    )
                    .await?;
                }
            }
            report.total += 1;
        }

        self.flush_internalize_updates(generator, batch_kind, llm_msgs, report)
            .await?;
        Ok(())
    }

    async fn push_internalize_message(
        &self,
        generator: &dyn InboxJsonGenerator,
        kind: InboxInternalizeKind,
        msg: ClaimedInboxMessage,
        batch_kind: &mut Option<InboxInternalizeKind>,
        llm_msgs: &mut Vec<ClaimedInboxMessage>,
        report: &mut InboxProcessReport,
    ) -> anyhow::Result<()> {
        if matches!(batch_kind, Some(current) if *current != kind) {
            self.flush_internalize_updates(generator, batch_kind, llm_msgs, report)
                .await?;
        }
        *batch_kind = Some(kind);
        llm_msgs.push(msg);
        Ok(())
    }

    async fn flush_internalize_updates(
        &self,
        generator: &dyn InboxJsonGenerator,
        batch_kind: &mut Option<InboxInternalizeKind>,
        llm_msgs: &mut Vec<ClaimedInboxMessage>,
        report: &mut InboxProcessReport,
    ) -> anyhow::Result<()> {
        if llm_msgs.is_empty() {
            *batch_kind = None;
            return Ok(());
        }
        let kind = batch_kind
            .take()
            .expect("llm_msgs 非空时 batch_kind 必须存在");
        let batch = std::mem::take(llm_msgs);
        let msg_ids: Vec<InboxId> = batch.iter().map(|m| m.message.id.clone()).collect();
        let inbox_messages = batch
            .iter()
            .map(|claimed| claimed.message.clone())
            .collect::<Vec<_>>();
        let summary = match kind {
            InboxInternalizeKind::PolicyUpdate => {
                self.internalize_inbox_updates(generator, kind, inbox_messages)
                    .await?
            }
            InboxInternalizeKind::ClaimAttributeUpdate => {
                self.internalize_claim_attribute_update_messages(generator, &inbox_messages)
                    .await?
            }
        };
        // 内化产出全部落地后再 ack 这批消息
        for claimed in &batch {
            self.inbox.ack_claimed(claimed).await?;
        }
        match kind {
            InboxInternalizeKind::PolicyUpdate => report.policy_count += msg_ids.len(),
            InboxInternalizeKind::ClaimAttributeUpdate => {
                report.claim_attribute_count += msg_ids.len();
            }
        }
        if let Some(trace_id) = summary.trace_id {
            report.trace_ids.push(trace_id);
        }
        report.new_claim_ids.extend(summary.new_claim_ids);
        report.updated_claim_ids.extend(summary.updated_claim_ids);
        report
            .deprecated_claim_ids
            .extend(summary.deprecated_claim_ids);
        report.new_dispute_ids.extend(summary.new_dispute_ids);
        report.warnings.extend(summary.warnings);
        Ok(())
    }

    async fn apply_policy_deprecation(
        &self,
        policy: &Policy,
    ) -> anyhow::Result<PolicyDeprecationSummary> {
        let knowledge_guard = FileLockGuard::lock_exclusive(
            paths::agent_home_knowledge_apply_lock_path(self.maintainer_upload_queue.agent_home()),
        )
        .await?;
        let now = Utc::now();
        let updated_at = crate::time::truncate_to_second(now);
        let policy_source = SourceId::Policy(policy.id.clone());
        let mut deprecated_claim_ids = Vec::new();
        let mut claims_to_upload = Vec::new();
        for mut claim in self.claim_store.list_local_claims().await? {
            if !claim.source_claim_ids.contains(&policy_source) {
                continue;
            }
            if claim.status != ClaimStatus::Deprecated {
                claim.status = ClaimStatus::Deprecated;
                claim.updated_at = Some(updated_at);
                deprecated_claim_ids.push(claim.id.clone());
            }
            // trace 是本地审计线索；这里优先保证 claim 状态和 maintainer mirror 可重试收敛。
            self.claim_store.write_claim(&claim).await?;
            claims_to_upload.push(claim);
        }
        let trace_id = if deprecated_claim_ids.is_empty() {
            None
        } else {
            deprecated_claim_ids.sort();
            Some(
                self.write_trace(
                    "policy_deprecation_internalization".into(),
                    format!("policy {} deprecated", policy.id),
                    vec![policy_source],
                    deprecated_claim_ids.clone(),
                    now,
                )
                .await?,
            )
        };
        self.stage_maintainer_batch(claims_to_upload, Vec::new())
            .await?;
        drop(knowledge_guard);
        let upload_report = self.upload_maintainer_batch(Vec::new(), Vec::new()).await?;
        let mut warnings = Vec::new();
        push_upload_warning(&mut warnings, upload_report);

        if deprecated_claim_ids.is_empty() {
            return Ok(PolicyDeprecationSummary {
                warnings,
                ..PolicyDeprecationSummary::default()
            });
        }

        log::info!(
            target: "agent",
            "agent {} 处理 deprecated policy id={} → deprecated claims={:?}",
            self.agent_id,
            policy.id,
            deprecated_claim_ids
        );

        Ok(PolicyDeprecationSummary {
            trace_id,
            deprecated_claim_ids,
            warnings,
        })
    }

    #[cfg(test)]
    async fn internalize_claim_attribute_update_message(
        &self,
        generator: &dyn InboxJsonGenerator,
        message: &InboxMessage,
    ) -> anyhow::Result<InternalizeSummary> {
        self.internalize_claim_attribute_update_messages(generator, std::slice::from_ref(message))
            .await
    }

    async fn internalize_claim_attribute_update_messages(
        &self,
        generator: &dyn InboxJsonGenerator,
        messages: &[InboxMessage],
    ) -> anyhow::Result<InternalizeSummary> {
        if messages.is_empty() {
            return Ok(InternalizeSummary::default());
        }
        for message in messages {
            let InboxMessageKind::ClaimAttributeUpdate {
                arbitration_resolution,
                ..
            } = &message.kind
            else {
                anyhow::bail!("期望 ClaimAttributeUpdate inbox 消息");
            };
            if let Some(context) = arbitration_resolution.as_deref() {
                validate_arbitration_message(message, context)?;
            }
        }

        // 先发现 ref 指向的完整 batch，再按路径排序统一加锁。若无锁发现期间记录变化，
        // 释放后重新收集，避免跨进程恢复时违反固定锁顺序。
        let _guards = loop {
            let effect_paths = self
                .discover_claim_attribute_update_effect_paths(messages)
                .await?;
            let mut guards = Vec::with_capacity(effect_paths.len());
            for path in &effect_paths {
                guards.push(FileLockGuard::lock_exclusive(path.with_extension("lock")).await?);
            }
            let stable_paths = self
                .discover_claim_attribute_update_effect_paths(messages)
                .await?;
            if stable_paths
                .iter()
                .all(|path| effect_paths.binary_search(path).is_ok())
            {
                break guards;
            }
        };
        let knowledge_guard = FileLockGuard::lock_exclusive(
            paths::agent_home_knowledge_apply_lock_path(self.maintainer_upload_queue.agent_home()),
        )
        .await?;

        let mut records = Vec::with_capacity(messages.len());
        for message in messages {
            records.push(read_inbox_effect_record(&self.inbox_effect_path(&message.id)).await?);
        }

        let mut batches = FxHashMap::<InboxId, LoadedInboxEffectBatch>::default();
        // 在产生任何副作用前，先校验本轮已经存在的 journal。后续因恢复旧 plan
        // 而补齐的 ref，会在顺序循环中按同一规则即时校验。
        for (message, record) in messages.iter().zip(&records) {
            if let Some(record) = record.as_ref() {
                self.load_claim_attribute_update_effect_batch(message, record, &mut batches)
                    .await?;
            }
        }

        let mut summary = InternalizeSummary::default();
        let mut applied_batches = FxHashSet::default();
        let mut index = 0;
        while index < messages.len() {
            if records[index].is_none() {
                // 前一个 Prepared plan 可能刚补齐本消息的 ref；重新读取后必须复用
                // 已有联合 effect，不能把它当成新的 CAU 再次规划。
                records[index] =
                    read_inbox_effect_record(&self.inbox_effect_path(&messages[index].id)).await?;
            }
            if let Some(record) = records[index].as_ref() {
                let canonical_id = self
                    .load_claim_attribute_update_effect_batch(
                        &messages[index],
                        record,
                        &mut batches,
                    )
                    .await?;
                if applied_batches.insert(canonical_id.clone()) {
                    let batch = batches
                        .get_mut(&canonical_id)
                        .ok_or_else(|| anyhow::anyhow!("canonical batch cache 缺失"))?;
                    let canonical_path = batch.canonical_path.clone();
                    let applied = self
                        .apply_persisted_claim_attribute_update_plan(
                            &mut batch.plan,
                            &canonical_path,
                        )
                        .await?;
                    summary.extend(applied.summary);
                }
                index += 1;
                continue;
            }

            // 只合并当前位置之后、尚无 journal 的连续消息。遇到既有 plan/ref 就先
            // 应用它；下一段新 CAU 因而总能读取此前 effect 落地后的最新本地 Claim。
            let mut end = index + 1;
            while end < messages.len() {
                if records[end].is_none() {
                    records[end] =
                        read_inbox_effect_record(&self.inbox_effect_path(&messages[end].id))
                            .await?;
                }
                if records[end].is_some() {
                    break;
                }
                end += 1;
            }
            let mut prepared = self
                .prepare_claim_attribute_update_effect(generator, &messages[index..end])
                .await?;
            self.persist_prepared_claim_attribute_update_plan(&prepared)
                .await?;
            let canonical_path = self.inbox_effect_path(&prepared.inbox_id);
            let applied = self
                .apply_persisted_claim_attribute_update_plan(&mut prepared, &canonical_path)
                .await?;
            summary.extend(applied.summary);
            index = end;
        }
        drop(knowledge_guard);
        let upload = self
            .upload_maintainer_batch_with_durable_claims(Vec::new(), Vec::new())
            .await?;
        push_upload_warning(&mut summary.warnings, upload);
        Ok(summary)
    }

    async fn load_claim_attribute_update_effect_batch(
        &self,
        message: &InboxMessage,
        record: &InboxEffectRecord,
        batches: &mut FxHashMap<InboxId, LoadedInboxEffectBatch>,
    ) -> anyhow::Result<InboxId> {
        let canonical_id = match record {
            InboxEffectRecord::Plan(plan) => plan.inbox_id.clone(),
            InboxEffectRecord::Ref(reference) => reference.canonical_inbox_id.clone(),
        };
        if !batches.contains_key(&canonical_id) {
            let (plan, canonical_path) = match record {
                InboxEffectRecord::Plan(plan) => (
                    plan.as_ref().clone(),
                    self.inbox_effect_path(&plan.inbox_id),
                ),
                InboxEffectRecord::Ref(reference) => {
                    let path = self.inbox_effect_path(&reference.canonical_inbox_id);
                    let Some(InboxEffectRecord::Plan(plan)) =
                        read_inbox_effect_record(&path).await?
                    else {
                        anyhow::bail!(
                            "inbox effect ref 缺少 canonical plan: inbox_id={} canonical_inbox_id={}",
                            message.id,
                            reference.canonical_inbox_id
                        );
                    };
                    (*plan, path)
                }
            };
            validate_effect_plan_integrity(&plan)?;
            let members = effect_plan_members(&plan)
                .into_iter()
                .map(|member| (member.inbox_id.clone(), member))
                .collect();
            batches.insert(
                canonical_id.clone(),
                LoadedInboxEffectBatch {
                    plan,
                    canonical_path,
                    members,
                },
            );
        }

        let batch = batches
            .get(&canonical_id)
            .ok_or_else(|| anyhow::anyhow!("canonical batch cache 缺失"))?;
        match record {
            InboxEffectRecord::Plan(plan) if **plan != batch.plan => anyhow::bail!(
                "inbox effect 冲突: canonical inbox_id={} 存在不同 batch plan",
                canonical_id
            ),
            InboxEffectRecord::Plan(_) => {}
            InboxEffectRecord::Ref(reference) => {
                validate_effect_ref(reference, &batch.plan, message)?;
            }
        }
        validate_effect_plan_message_member(&batch.plan.inbox_id, &batch.members, message)?;
        Ok(canonical_id)
    }

    async fn apply_persisted_claim_attribute_update_plan(
        &self,
        plan: &mut InboxEffectPlan,
        canonical_path: &std::path::Path,
    ) -> anyhow::Result<AppliedInboxEffect> {
        self.repair_claim_attribute_update_refs(plan).await?;
        if plan.state == InboxEffectState::Prepared {
            let pending_upload = self.apply_claim_attribute_update_effect(plan).await?;
            self.stage_maintainer_batch_with_durable_claims(
                pending_upload.claims,
                pending_upload.disputes.clone(),
            )
            .await?;
            if self.team_services_configured() {
                for dispute in &pending_upload.disputes {
                    self.record_dispute_if_new(dispute).await?;
                }
            }
            plan.state = InboxEffectState::Applied;
            write_yaml_atomic(
                canonical_path,
                &InboxEffectRecord::Plan(Box::new(plan.clone())),
            )
            .await?;
        }
        Ok(AppliedInboxEffect {
            summary: effect_summary(plan),
        })
    }

    fn inbox_effect_path(&self, inbox_id: &InboxId) -> std::path::PathBuf {
        paths::agent_home_inbox_effect_path(self.maintainer_upload_queue.agent_home(), inbox_id)
    }

    async fn discover_claim_attribute_update_effect_paths(
        &self,
        messages: &[InboxMessage],
    ) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let mut paths = messages
            .iter()
            .map(|message| self.inbox_effect_path(&message.id))
            .collect::<Vec<_>>();
        let mut records = FxHashMap::default();
        let mut canonical_paths = FxHashSet::default();
        for message in messages {
            let message_path = self.inbox_effect_path(&message.id);
            if !records.contains_key(&message_path) {
                records.insert(
                    message_path.clone(),
                    read_inbox_effect_record(&message_path).await?,
                );
            }
            match records.get(&message_path).and_then(Option::as_ref) {
                Some(InboxEffectRecord::Plan(_)) => {
                    canonical_paths.insert(message_path);
                }
                Some(InboxEffectRecord::Ref(reference)) => {
                    let canonical_path = self.inbox_effect_path(&reference.canonical_inbox_id);
                    paths.push(canonical_path.clone());
                    canonical_paths.insert(canonical_path);
                }
                None => {}
            }
        }
        for canonical_path in canonical_paths {
            if !records.contains_key(&canonical_path) {
                records.insert(
                    canonical_path.clone(),
                    read_inbox_effect_record(&canonical_path).await?,
                );
            }
            if let Some(InboxEffectRecord::Plan(plan)) =
                records.get(&canonical_path).and_then(Option::as_ref)
            {
                paths.extend(
                    effect_plan_members(plan)
                        .into_iter()
                        .map(|member| self.inbox_effect_path(&member.inbox_id)),
                );
            }
        }
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    async fn persist_prepared_claim_attribute_update_plan(
        &self,
        plan: &InboxEffectPlan,
    ) -> anyhow::Result<()> {
        let canonical_path = self.inbox_effect_path(&plan.inbox_id);
        write_yaml_atomic(
            &canonical_path,
            &InboxEffectRecord::Plan(Box::new(plan.clone())),
        )
        .await?;
        self.repair_claim_attribute_update_refs(plan).await
    }

    async fn repair_claim_attribute_update_refs(
        &self,
        plan: &InboxEffectPlan,
    ) -> anyhow::Result<()> {
        let Some(batch_hash) = plan.batch_hash.as_ref() else {
            return Ok(());
        };
        for member in effect_plan_members(plan).iter().skip(1) {
            let path = self.inbox_effect_path(&member.inbox_id);
            let expected = InboxEffectRecord::Ref(InboxEffectRef {
                schema_version: INBOX_EFFECT_SCHEMA_VERSION,
                canonical_inbox_id: plan.inbox_id.clone(),
                batch_hash: batch_hash.clone(),
            });
            match read_inbox_effect_record(&path).await? {
                Some(existing) if existing == expected => {}
                Some(_) => anyhow::bail!(
                    "inbox effect 冲突: inbox_id={} 已存在不同 batch plan",
                    member.inbox_id
                ),
                None => write_yaml_atomic(&path, &expected).await?,
            }
        }
        Ok(())
    }

    async fn prepare_claim_attribute_update_effect(
        &self,
        generator: &dyn InboxJsonGenerator,
        messages: &[InboxMessage],
    ) -> anyhow::Result<InboxEffectPlan> {
        let members = effect_members_from_messages(messages)?;
        let canonical = members
            .first()
            .ok_or_else(|| anyhow::anyhow!("ClaimAttributeUpdate batch 不能为空"))?;
        let all_local = self.claim_store.list_local_claims().await?;
        let direct_ids: FxHashSet<ClaimId> = messages
            .iter()
            .filter_map(cau_resolution_context)
            .flat_map(|context| &context.direct_claim_snapshots)
            .map(|claim| claim.id.clone())
            .collect();
        let mut local_claims = Vec::new();
        let mut seen_local_ids = FxHashSet::default();
        for claim in all_local {
            if claim.holder != self.agent_id {
                anyhow::bail!(
                    "本地 claim={} holder={} 不是当前 agent={}",
                    claim.id,
                    claim.holder,
                    self.agent_id
                );
            }
            if !seen_local_ids.insert(claim.id.clone()) {
                anyhow::bail!("当前 agent 存在重复本地 claim={}", claim.id);
            }
            if claim.status != ClaimStatus::Deprecated || direct_ids.contains(&claim.id) {
                local_claims.push(claim);
            }
        }
        local_claims.sort_by(|left, right| left.id.cmp(&right.id));
        let local_by_id: FxHashMap<ClaimId, Claim> = local_claims
            .iter()
            .map(|claim| (claim.id.clone(), claim.clone()))
            .collect();
        let mut items = Vec::with_capacity(messages.len());
        for message in messages {
            let InboxMessageKind::ClaimAttributeUpdate {
                policy,
                arbitration_resolution,
            } = &message.kind
            else {
                anyhow::bail!("期望 ClaimAttributeUpdate inbox 消息");
            };
            let mut direct_claims = arbitration_resolution
                .as_deref()
                .map(|context| context.direct_claim_snapshots.clone())
                .unwrap_or_default();
            direct_claims.sort_by(|left, right| left.id.cmp(&right.id));
            items.push(ClaimAttributeUpdateInternalizeItem {
                claim_attribute_update: message.clone(),
                conclusion: arbitration_resolution
                    .as_deref()
                    .map(|context| context.resolution.conclusion.clone())
                    .unwrap_or_else(|| policy.statement.clone()),
                resolution: arbitration_resolution
                    .as_deref()
                    .map(|context| context.resolution.clone()),
                dispute: arbitration_resolution
                    .as_deref()
                    .map(|context| context.dispute_snapshot.clone()),
                direct_claims,
            });
        }
        let request = ClaimAttributeUpdateInternalizeRequest {
            agent_id: self.agent_id.clone(),
            claim_attribute_updates: items,
            local_claims,
        };
        let (now, new_claims, updated_claims, mut new_disputes) = self
            .claim_attribute_update_internalize_and_prepare_once(generator, request, &local_by_id)
            .await?;
        let before_repeat_filter = new_disputes.len();
        new_disputes.retain(|dispute| {
            !messages
                .iter()
                .filter_map(cau_resolution_context)
                .any(|context| {
                    repeats_resolved_arbitration_input(
                        dispute,
                        context,
                        &local_by_id,
                        &updated_claims,
                    )
                })
        });
        if new_disputes.len() != before_repeat_filter {
            log::info!(
                target: "agent",
                "agent {} 跳过与本批已 resolved dispute 语义输入相同的重复 dispute",
                self.agent_id
            );
        }
        let mut deprecated_claim_ids = Vec::new();
        let updated_claims = updated_claims
            .into_iter()
            .map(|target| {
                let preimage = local_by_id.get(&target.id).ok_or_else(|| {
                    anyhow::anyhow!("prepared update claim={} 不在本地输入中", target.id)
                })?;
                if preimage.status != ClaimStatus::Deprecated
                    && target.status == ClaimStatus::Deprecated
                {
                    deprecated_claim_ids.push(target.id.clone());
                }
                Ok(PlannedClaimUpdate {
                    target,
                    preimage_hash: inbox_effect_hash(preimage)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        deprecated_claim_ids.sort();
        deprecated_claim_ids.dedup();
        let output_claims: Vec<ClaimId> = new_claims
            .iter()
            .map(|claim| claim.id.clone())
            .chain(updated_claims.iter().map(|update| update.target.id.clone()))
            .collect();
        let trace = if output_claims.is_empty() && new_disputes.is_empty() {
            None
        } else {
            let mut inputs = FxHashSet::default();
            inputs.extend(inbox_policy_ids(messages).into_iter().map(SourceId::Policy));
            inputs.extend(direct_ids.iter().cloned().map(SourceId::Claim));
            inputs.extend(
                new_claims
                    .iter()
                    .flat_map(|claim| claim.source_claim_ids.iter().cloned()),
            );
            inputs.extend(
                updated_claims
                    .iter()
                    .flat_map(|update| update.target.source_claim_ids.iter().cloned()),
            );
            let input_claims = sorted_source_ids(inputs);
            let name = "inbox_claim_attribute_update".to_string();
            Some(Trace {
                id: TraceId::from_trace_parts(now, &name, &input_claims, &output_claims),
                name,
                task: if messages.len() == 1 {
                    cau_resolution_context(&messages[0]).map_or_else(
                        || "内化 ClaimAttributeUpdate".to_string(),
                        |context| {
                            format!(
                                "内化 ClaimAttributeUpdate resolution {}",
                                context.resolution.resolution_id
                            )
                        },
                    )
                } else {
                    format!("批量内化 {} 条 ClaimAttributeUpdate", messages.len())
                },
                agent: self.agent_id.clone(),
                input_claims,
                output_claims,
                created_at: crate::time::truncate_to_second(now),
            })
        };
        let (batch_members, batch_hash) = if members.len() == 1 {
            (Vec::new(), None)
        } else {
            let hash = inbox_effect_hash(&members)?;
            (members.clone(), Some(hash))
        };
        Ok(InboxEffectPlan {
            schema_version: INBOX_EFFECT_SCHEMA_VERSION,
            inbox_id: canonical.inbox_id.clone(),
            resolution_id: canonical.resolution_id.clone(),
            message_hash: canonical.message_hash.clone(),
            batch_members,
            batch_hash,
            state: InboxEffectState::Prepared,
            prepared_at: now,
            new_claims,
            updated_claims,
            deprecated_claim_ids,
            new_disputes,
            trace,
            warnings: Vec::new(),
        })
    }

    async fn claim_attribute_update_internalize_and_prepare_once(
        &self,
        generator: &dyn InboxJsonGenerator,
        request: ClaimAttributeUpdateInternalizeRequest,
        editable_local_by_id: &FxHashMap<ClaimId, Claim>,
    ) -> anyhow::Result<(DateTime<Utc>, Vec<Claim>, Vec<Claim>, Vec<Dispute>)> {
        let agent_id = self.agent_id.clone();
        let mut visible_claims_by_id: FxHashMap<ClaimId, Claim> = request
            .claim_attribute_updates
            .iter()
            .flat_map(|item| &item.direct_claims)
            .map(|claim| (claim.id.clone(), claim.clone()))
            .collect();
        // 当前 holder 的本地状态比 resolution 中的历史快照更新，应覆盖同 ID 快照。
        visible_claims_by_id.extend(
            request
                .local_claims
                .iter()
                .map(|claim| (claim.id.clone(), claim.clone())),
        );
        let inbox_messages = request
            .claim_attribute_updates
            .iter()
            .map(|item| item.claim_attribute_update.clone())
            .collect::<Vec<_>>();
        let batch_policy_ids = inbox_policy_ids(&inbox_messages)
            .into_iter()
            .collect::<FxHashSet<_>>();
        let mut allowed_policy_ids = batch_policy_ids.clone();
        let mut allowed_source_claim_ids = FxHashSet::default();
        let mut allowed_dispute_claim_ids = FxHashSet::default();
        for claim in request
            .claim_attribute_updates
            .iter()
            .flat_map(|item| &item.direct_claims)
            .chain(&request.local_claims)
        {
            allowed_source_claim_ids.insert(claim.id.clone());
            allowed_dispute_claim_ids.insert(claim.id.clone());
            for source in &claim.source_claim_ids {
                match source {
                    SourceId::Claim(claim_id) => {
                        allowed_source_claim_ids.insert(claim_id.clone());
                    }
                    SourceId::Policy(policy_id) => {
                        allowed_policy_ids.insert(policy_id.clone());
                    }
                }
            }
        }
        let mut validator = move |raw| {
            // 每次 structured retry 都从同一份只读输入权限开始，失败输出不能扩张白名单。
            let mut attempt_source_claim_ids = allowed_source_claim_ids.clone();
            let mut attempt_dispute_claim_ids = allowed_dispute_claim_ids.clone();
            let now = Utc::now();
            let resolved = resolve_placeholders(raw, now)?;
            let mut outcome: InternalizeOutcome = serde_json::from_value(resolved)
                .map_err(|error| anyhow::anyhow!("ClaimAttributeUpdate 输出无法解析: {error}"))?;
            ensure_cau_policy_provenance(&mut outcome, &batch_policy_ids)?;
            validate_visible_policy_sources(
                "new_claims",
                &outcome.new_claims,
                &allowed_policy_ids,
            )?;
            validate_visible_policy_sources(
                "updated_claims",
                &outcome.updated_claims,
                &allowed_policy_ids,
            )?;
            for (index, draft) in outcome.new_claims.iter().enumerate() {
                let claim_id = ClaimId::from_str(&draft.id).map_err(|error| {
                    anyhow::anyhow!(
                        "ClaimAttributeUpdate new_claims[{index}].id 不是合法 ClaimId: {error}"
                    )
                })?;
                attempt_source_claim_ids.insert(claim_id.clone());
                attempt_dispute_claim_ids.insert(claim_id);
            }
            let new_claims = prepare_claims(
                outcome.new_claims,
                Some(&attempt_source_claim_ids),
                &agent_id,
                now,
            )?;
            let updated_claims = prepare_claim_updates(
                outcome.updated_claims,
                editable_local_by_id,
                Some(&attempt_source_claim_ids),
                now,
            )?;
            if updated_claims.iter().any(|claim| claim.holder != agent_id) {
                anyhow::bail!("ClaimAttributeUpdate updated_claims 只能由当前 holder 修改");
            }
            let new_disputes = prepare_disputes(
                outcome.new_disputes,
                Some(&attempt_dispute_claim_ids),
                &agent_id,
                now,
            )?;
            validate_new_disputes_against_final_claim_status(
                &visible_claims_by_id,
                &new_claims,
                &updated_claims,
                &new_disputes,
            )?;
            Ok((now, new_claims, updated_claims, new_disputes))
        };
        generator
            .generate_validated_claim_attribute_update_json(request, &mut validator)
            .await
    }

    async fn apply_claim_attribute_update_effect(
        &self,
        plan: &mut InboxEffectPlan,
    ) -> anyhow::Result<PendingInboxUpload> {
        if self.team_services_configured() {
            let mut unreported = Vec::with_capacity(plan.new_disputes.len());
            let mut accepted_claim_sets = FxHashSet::default();
            for dispute in &plan.new_disputes {
                if self.dispute_claim_set_reported(dispute).await? {
                    plan.warnings.push(format!(
                        "dispute={} 的 claim-set 已报告，未再次上报",
                        dispute.id
                    ));
                } else if !accepted_claim_sets
                    .insert(reported_dispute_claim_set_key(&dispute.claims))
                {
                    plan.warnings.push(format!(
                        "dispute={} 的 claim-set 在当前 CAU 中重复，未再次上报",
                        dispute.id
                    ));
                } else {
                    unreported.push(dispute.clone());
                }
            }
            plan.new_disputes = unreported;
        }
        let mut current: FxHashMap<ClaimId, Claim> = self
            .claim_store
            .list_local_claims()
            .await?
            .into_iter()
            .map(|claim| (claim.id.clone(), claim))
            .collect();
        let mut claims_to_upload = Vec::new();
        for target in &plan.new_claims {
            match current.get(&target.id) {
                Some(existing) if existing == target => claims_to_upload.push(target.clone()),
                Some(_) => plan
                    .warnings
                    .push(format!("claim={} 已被其他本地操作占用，未覆盖", target.id)),
                None => {
                    self.claim_store.write_claim(target).await?;
                    current.insert(target.id.clone(), target.clone());
                    claims_to_upload.push(target.clone());
                }
            }
        }
        let mut applied_updated_claim_ids = FxHashSet::default();
        for update in &plan.updated_claims {
            match current.get(&update.target.id) {
                Some(existing) if existing == &update.target => {
                    applied_updated_claim_ids.insert(update.target.id.clone());
                    claims_to_upload.push(update.target.clone());
                }
                Some(existing) if inbox_effect_hash(existing)? == update.preimage_hash => {
                    self.claim_store.write_claim(&update.target).await?;
                    current.insert(update.target.id.clone(), update.target.clone());
                    applied_updated_claim_ids.insert(update.target.id.clone());
                    claims_to_upload.push(update.target.clone());
                }
                Some(_) => plan.warnings.push(format!(
                    "claim={} 在 effect prepared 后已变更，CAU 更新已 superseded",
                    update.target.id
                )),
                None => plan.warnings.push(format!(
                    "claim={} 在 effect prepared 后已缺失，CAU 更新已 superseded",
                    update.target.id
                )),
            }
        }
        plan.deprecated_claim_ids
            .retain(|claim_id| applied_updated_claim_ids.contains(claim_id));
        if let Some(trace) = plan.trace.as_ref() {
            let path = paths::agent_home_traces_dir(self.maintainer_upload_queue.agent_home())
                .join(format!("{}.yaml", trace.id));
            match read_yaml::<Trace>(&path).await {
                Ok(existing) if existing == *trace => {}
                Ok(_) => anyhow::bail!("trace id={} 已存在但内容不同", trace.id),
                Err(StorageError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    self.claim_store.write_trace(trace).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        plan.warnings.sort();
        plan.warnings.dedup();
        Ok(PendingInboxUpload {
            claims: claims_to_upload,
            disputes: plan.new_disputes.clone(),
        })
    }

    /// 把一批同类型 inbox 更新消息交给 LLM 内化。
    ///
    /// 校验前置：所有 prepared_claims / prepared_updates / prepared_disputes 都校验通过后，
    /// 再开始落地，避免出现"已经写了部分 claim 但 dispute 失败"的半成品状态。
    async fn internalize_inbox_updates(
        &self,
        generator: &dyn InboxJsonGenerator,
        kind: InboxInternalizeKind,
        inbox_messages: Vec<InboxMessage>,
    ) -> anyhow::Result<InternalizeSummary> {
        let tracer = tracer();
        let mut span = tracer.start("agent.internalize_inbox");
        span.set_attribute(KeyValue::new(
            "agent.id",
            self.agent_id.as_str().to_string(),
        ));
        span.set_attribute(KeyValue::new(
            "inbox.message_count",
            i64::try_from(inbox_messages.len()).unwrap_or(i64::MAX),
        ));

        let knowledge_guard = FileLockGuard::lock_exclusive(
            paths::agent_home_knowledge_apply_lock_path(self.maintainer_upload_queue.agent_home()),
        )
        .await?;
        let local = llm_visible_claims(self.claim_store.list_local_claims().await?);
        let source_policy_ids = inbox_policy_ids(&inbox_messages);
        let request = InternalizeRequest {
            agent_id: self.agent_id.clone(),
            inbox_messages,
            local_claims: local.clone(),
        };

        let local_by_id: FxHashMap<ClaimId, Claim> = local
            .iter()
            .map(|claim| (claim.id.clone(), claim.clone()))
            .collect();
        let mut last_err = None;
        let mut preferred_transport = None;
        let (now, prepared_claims, prepared_updates, prepared_disputes) = {
            let mut prepared = None;
            for attempt in 0..=self.llm_retry_count {
                match self
                    .internalize_and_prepare_once(
                        generator,
                        kind,
                        request.clone(),
                        &local_by_id,
                        preferred_transport,
                    )
                    .await
                {
                    Ok(value) => {
                        prepared = Some(value);
                        break;
                    }
                    Err(e)
                        if crate::api::structured_json_business_retryable(&e)
                            && attempt < self.llm_retry_count =>
                    {
                        if let Some(transport) =
                            crate::api::structured_json_no_consumable_transport(&e)
                        {
                            preferred_transport = Some(transport);
                            log::warn!(
                                target: "agent",
                                "agent {} internalize_inbox 没有可消费输出，使用相同 transport 原样重试 ({}/{}): transport={}; {e:#}",
                                self.agent_id,
                                attempt + 1,
                                self.llm_retry_count,
                                transport
                            );
                        } else {
                            log::warn!(
                                target: "agent",
                                "agent {} internalize_inbox 输出未通过协议校验，重试 ({}/{}): {e:#}",
                                self.agent_id,
                                attempt + 1,
                                self.llm_retry_count
                            );
                        }
                        last_err = Some(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            prepared.ok_or_else(|| {
                last_err
                    .unwrap_or_else(|| anyhow::anyhow!("internalize_inbox retry loop 未返回结果"))
            })?
        };
        let should_write_inbox_trace = !prepared_claims.is_empty()
            || !prepared_updates.is_empty()
            || !prepared_disputes.is_empty();
        let kind_label = match kind {
            InboxInternalizeKind::PolicyUpdate => "PolicyUpdate",
            InboxInternalizeKind::ClaimAttributeUpdate => "ClaimAttributeUpdate",
        };

        let mut written_claim_ids = Vec::with_capacity(prepared_claims.len());
        let mut claims_to_upload =
            Vec::with_capacity(prepared_claims.len() + prepared_updates.len());
        let mut trace_input_sources: FxHashSet<SourceId> = source_policy_ids
            .into_iter()
            .map(SourceId::Policy)
            .collect();
        for claim in prepared_claims {
            trace_input_sources.extend(claim.source_claim_ids.iter().cloned());
            self.claim_store.write_claim(&claim).await?;
            log::info!(
                target: "agent",
                "agent {} 内化 {} → claim id={} name={} scope={}",
                self.agent_id, kind_label, claim.id, claim.name, claim.scope
            );
            span.add_event(
                "claim_created",
                vec![
                    KeyValue::new("claim.id", claim.id.to_string()),
                    KeyValue::new("claim.name", claim.name.clone()),
                    KeyValue::new("claim.scope", claim.scope.clone()),
                ],
            );
            written_claim_ids.push(claim.id.clone());
            claims_to_upload.push(claim);
        }

        let mut trace_id = None;

        let mut updated_claim_ids = Vec::with_capacity(prepared_updates.len());
        let mut deprecated_claim_ids = Vec::new();
        for claim in prepared_updates {
            trace_input_sources.extend(claim.source_claim_ids.iter().cloned());
            trace_input_sources.insert(SourceId::Claim(claim.id.clone()));
            if claim.status == ClaimStatus::Deprecated
                && local_by_id
                    .get(&claim.id)
                    .is_some_and(|preimage| preimage.status != ClaimStatus::Deprecated)
            {
                deprecated_claim_ids.push(claim.id.clone());
            }
            self.claim_store.write_claim(&claim).await?;
            log::info!(
                target: "agent",
                "agent {} 内化 {} → 更新 claim id={} name={} scope={}",
                self.agent_id, kind_label, claim.id, claim.name, claim.scope
            );
            updated_claim_ids.push(claim.id.clone());
            claims_to_upload.push(claim);
        }
        deprecated_claim_ids.sort();
        deprecated_claim_ids.dedup();

        if should_write_inbox_trace {
            let output_claims: Vec<ClaimId> = written_claim_ids
                .iter()
                .chain(updated_claim_ids.iter())
                .cloned()
                .collect();
            let (trace_name, trace_task) = match kind {
                InboxInternalizeKind::PolicyUpdate => (
                    "inbox_policy_internalization",
                    "处理 inbox PolicyUpdate 并内化或更新本地 claim",
                ),
                InboxInternalizeKind::ClaimAttributeUpdate => {
                    ("inbox_claim_attribute_update", "claim_attribute_update")
                }
            };
            trace_id = Some(
                self.write_trace(
                    trace_name.into(),
                    trace_task.into(),
                    sorted_source_ids(trace_input_sources),
                    output_claims,
                    now,
                )
                .await?,
            );
        }

        let mut written_dispute_ids = Vec::new();
        let mut disputes_to_upload = Vec::new();
        if self.team_services_configured() {
            written_dispute_ids.reserve(prepared_disputes.len());
            disputes_to_upload.reserve(prepared_disputes.len());
            let mut accepted_claim_sets = FxHashSet::default();
            for dispute in prepared_disputes {
                if self.dispute_claim_set_reported(&dispute).await? {
                    continue;
                }
                if !accepted_claim_sets.insert(reported_dispute_claim_set_key(&dispute.claims)) {
                    continue;
                }
                log::warn!(
                    target: "agent",
                    "agent {} 内化 {:?} → dispute id={} 涉及 claims={:?}",
                    self.agent_id, kind, dispute.id, dispute.claims
                );
                span.add_event(
                    "dispute_created",
                    vec![KeyValue::new("dispute.id", dispute.id.to_string())],
                );
                disputes_to_upload.push(dispute);
            }
        }
        self.stage_maintainer_batch(claims_to_upload, disputes_to_upload.clone())
            .await?;
        for dispute in &disputes_to_upload {
            if self.record_dispute_if_new(dispute).await? {
                written_dispute_ids.push(dispute.id.clone());
            }
        }
        drop(knowledge_guard);
        let upload_report = self.upload_maintainer_batch(Vec::new(), Vec::new()).await?;
        let mut warnings = Vec::new();
        push_upload_warning(&mut warnings, upload_report);

        span.end();
        Ok(InternalizeSummary {
            trace_id,
            new_claim_ids: written_claim_ids,
            updated_claim_ids,
            deprecated_claim_ids,
            new_dispute_ids: written_dispute_ids,
            warnings,
        })
    }

    async fn internalize_and_prepare_once(
        &self,
        generator: &dyn InboxJsonGenerator,
        kind: InboxInternalizeKind,
        request: InternalizeRequest,
        local_by_id: &FxHashMap<ClaimId, Claim>,
        preferred_transport: Option<ProviderTransport>,
    ) -> anyhow::Result<PreparedInternalization> {
        let mut allowed_policy_ids = inbox_policy_ids(&request.inbox_messages)
            .into_iter()
            .collect::<FxHashSet<_>>();
        let mut allowed_source_claim_ids: FxHashSet<ClaimId> = request
            .local_claims
            .iter()
            .map(|claim| claim.id.clone())
            .collect();
        for claim in &request.local_claims {
            for source in &claim.source_claim_ids {
                match source {
                    SourceId::Claim(claim_id) => {
                        allowed_source_claim_ids.insert(claim_id.clone());
                    }
                    SourceId::Policy(policy_id) => {
                        allowed_policy_ids.insert(policy_id.clone());
                    }
                }
            }
        }
        // Dispute 仍只能指向当前本地 claim 或本批新 claim；这里只扩展 ClaimDraft 的
        // source_claim_ids 白名单，不把仅作为历史来源可见的 claim 升格为 dispute 对象。
        let allowed_dispute_claim_ids: FxHashSet<ClaimId> = request
            .local_claims
            .iter()
            .map(|claim| claim.id.clone())
            .collect();
        let raw = generator
            .generate_json(kind, request, preferred_transport)
            .await?;
        self.prepare_internalized_output(
            raw,
            local_by_id,
            allowed_policy_ids,
            allowed_source_claim_ids,
            allowed_dispute_claim_ids,
        )
    }

    fn prepare_internalized_output(
        &self,
        raw: serde_json::Value,
        local_by_id: &FxHashMap<ClaimId, Claim>,
        allowed_policy_ids: FxHashSet<PolicyId>,
        mut allowed_source_claim_ids: FxHashSet<ClaimId>,
        mut allowed_dispute_claim_ids: FxHashSet<ClaimId>,
    ) -> anyhow::Result<PreparedInternalization> {
        let now = Utc::now();
        let resolved = resolve_placeholders(raw, now)?;
        let outcome: InternalizeOutcome = serde_json::from_value(resolved).map_err(|e| {
            anyhow::anyhow!("internalize_inbox 输出无法解析为 InternalizeOutcome: {e}")
        })?;
        validate_visible_policy_sources("new_claims", &outcome.new_claims, &allowed_policy_ids)?;
        validate_visible_policy_sources(
            "updated_claims",
            &outcome.updated_claims,
            &allowed_policy_ids,
        )?;

        for (idx, draft) in outcome.new_claims.iter().enumerate() {
            let id = ClaimId::from_str(&draft.id).map_err(|e| {
                anyhow::anyhow!("internalize new_claims[{idx}].id 不是合法 ClaimId: {e}")
            })?;
            allowed_source_claim_ids.insert(id.clone());
            allowed_dispute_claim_ids.insert(id);
        }

        let prepared_claims = prepare_claims(
            outcome.new_claims,
            Some(&allowed_source_claim_ids),
            &self.agent_id,
            now,
        )?;
        let prepared_updates = prepare_claim_updates(
            outcome.updated_claims,
            local_by_id,
            Some(&allowed_source_claim_ids),
            now,
        )?;
        let prepared_disputes = prepare_disputes(
            outcome.new_disputes,
            Some(&allowed_dispute_claim_ids),
            &self.agent_id,
            now,
        )?;
        validate_new_disputes_against_final_claim_status(
            local_by_id,
            &prepared_claims,
            &prepared_updates,
            &prepared_disputes,
        )?;
        Ok((now, prepared_claims, prepared_updates, prepared_disputes))
    }
}

/// 以整批内化完成后的状态校验新 Dispute，避免 Claim 更新与 Dispute 并发上传时，
/// Maintainer 在旧 mirror 仍为 active 的短暂窗口内接受已失效的冲突。
fn validate_new_disputes_against_final_claim_status(
    existing_claims: &FxHashMap<ClaimId, Claim>,
    new_claims: &[Claim],
    updated_claims: &[Claim],
    new_disputes: &[Dispute],
) -> anyhow::Result<()> {
    let mut final_statuses: FxHashMap<ClaimId, ClaimStatus> = existing_claims
        .iter()
        .map(|(id, claim)| (id.clone(), claim.status))
        .collect();
    final_statuses.extend(
        new_claims
            .iter()
            .chain(updated_claims)
            .map(|claim| (claim.id.clone(), claim.status)),
    );

    for dispute in new_disputes {
        let mut deprecated: Vec<_> = dispute
            .claims
            .iter()
            .filter(|claim_id| final_statuses.get(*claim_id) == Some(&ClaimStatus::Deprecated))
            .map(ToString::to_string)
            .collect();
        if !deprecated.is_empty() {
            deprecated.sort();
            anyhow::bail!(
                "new_disputes 不得引用本批内化后最终状态为 deprecated 的 Claim: dispute={} claims={}",
                dispute.id,
                deprecated.join(", ")
            );
        }
    }
    Ok(())
}

fn validate_arbitration_message(
    message: &InboxMessage,
    resolution_context: &ArbitrationResolutionContext,
) -> anyhow::Result<()> {
    if resolution_context.dispute_id != resolution_context.dispute_snapshot.id {
        anyhow::bail!("arbitration dispute_id 与 dispute snapshot 不一致");
    }
    if resolution_context
        .resolution
        .resolution_type
        .is_some_and(|kind| !kind.is_resolved())
    {
        anyhow::bail!("arbitration inbox 不能内化 unresolved resolution");
    }
    if resolution_context.resolution.resolution_basis
        == Some(crate::claim::ResolutionBasis::InsufficientEvidence)
    {
        anyhow::bail!("arbitration inbox 不能内化 insufficient_evidence resolution");
    }
    let direct: FxHashSet<ClaimId> = resolution_context
        .direct_claim_snapshots
        .iter()
        .map(|claim| claim.id.clone())
        .collect();
    let disputed: FxHashSet<ClaimId> = resolution_context
        .dispute_snapshot
        .claims
        .iter()
        .cloned()
        .collect();
    if direct.len() != resolution_context.direct_claim_snapshots.len()
        || disputed.len() != resolution_context.dispute_snapshot.claims.len()
        || direct != disputed
    {
        anyhow::bail!("arbitration 直接 Claim 快照必须唯一且完整覆盖 Dispute");
    }
    let assessments: FxHashSet<ClaimId> = resolution_context
        .resolution
        .claim_assessments
        .iter()
        .map(|assessment| assessment.claim_id.clone())
        .collect();
    if !resolution_context.resolution.claim_assessments.is_empty()
        && (assessments.len() != resolution_context.resolution.claim_assessments.len()
            || assessments != direct)
    {
        anyhow::bail!("arbitration assessments 必须完整且唯一覆盖直接 Claim");
    }
    if message.id.as_str().is_empty() {
        anyhow::bail!("arbitration inbox id 不能为空");
    }
    Ok(())
}

fn repeats_resolved_arbitration_input(
    candidate: &Dispute,
    resolution_context: &ArbitrationResolutionContext,
    editable_local: &FxHashMap<ClaimId, Claim>,
    prepared_updates: &[Claim],
) -> bool {
    let candidate_ids = candidate.claims.iter().collect::<FxHashSet<_>>();
    let original_ids = resolution_context
        .dispute_snapshot
        .claims
        .iter()
        .collect::<FxHashSet<_>>();
    if candidate_ids.len() != candidate.claims.len()
        || original_ids.len() != resolution_context.dispute_snapshot.claims.len()
        || candidate_ids != original_ids
    {
        return false;
    }

    let updates = prepared_updates
        .iter()
        .map(|claim| (&claim.id, claim))
        .collect::<FxHashMap<_, _>>();
    resolution_context
        .direct_claim_snapshots
        .iter()
        .filter(|snapshot| editable_local.contains_key(&snapshot.id))
        .all(|snapshot| {
            let effective = updates
                .get(&snapshot.id)
                .copied()
                .or_else(|| editable_local.get(&snapshot.id));
            effective.is_none_or(|claim| claim_semantics_equal(claim, snapshot))
        })
}

fn claim_semantics_equal(left: &Claim, right: &Claim) -> bool {
    left.id == right.id
        && left.name == right.name
        && left.statement == right.statement
        && left.scope == right.scope
        && left.holder == right.holder
        && left.confidence == right.confidence
        && left.status == right.status
        && left.evidence_summary == right.evidence_summary
        && left.source_claim_ids.iter().collect::<FxHashSet<_>>()
            == right.source_claim_ids.iter().collect::<FxHashSet<_>>()
}

fn inbox_effect_hash(value: &impl Serialize) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(value)?;
    Ok(format!(
        "sha256-v1:{}",
        hex::encode(ring::digest::digest(&ring::digest::SHA256, &encoded).as_ref())
    ))
}

async fn read_inbox_effect_record(
    path: &std::path::Path,
) -> anyhow::Result<Option<InboxEffectRecord>> {
    match read_yaml(path).await {
        Ok(record) => Ok(Some(record)),
        Err(StorageError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn cau_resolution_context(message: &InboxMessage) -> Option<&ArbitrationResolutionContext> {
    match &message.kind {
        InboxMessageKind::ClaimAttributeUpdate {
            arbitration_resolution,
            ..
        } => arbitration_resolution.as_deref(),
        InboxMessageKind::PolicyUpdate { .. } => None,
    }
}

fn effect_members_from_messages(
    messages: &[InboxMessage],
) -> anyhow::Result<Vec<InboxEffectMember>> {
    let mut seen = FxHashSet::default();
    let mut members = Vec::with_capacity(messages.len());
    for message in messages {
        if !seen.insert(message.id.clone()) {
            anyhow::bail!("ClaimAttributeUpdate batch 含重复 inbox_id={}", message.id);
        }
        members.push(InboxEffectMember {
            inbox_id: message.id.clone(),
            resolution_id: cau_resolution_context(message)
                .map(|context| context.resolution.resolution_id.clone()),
            message_hash: inbox_effect_hash(message)?,
        });
    }
    Ok(members)
}

fn effect_plan_members(plan: &InboxEffectPlan) -> Vec<InboxEffectMember> {
    if plan.batch_members.is_empty() {
        vec![InboxEffectMember {
            inbox_id: plan.inbox_id.clone(),
            resolution_id: plan.resolution_id.clone(),
            message_hash: plan.message_hash.clone(),
        }]
    } else {
        plan.batch_members.clone()
    }
}

fn validate_effect_plan_integrity(plan: &InboxEffectPlan) -> anyhow::Result<()> {
    if plan.schema_version != INBOX_EFFECT_SCHEMA_VERSION {
        anyhow::bail!(
            "inbox effect schema 不兼容: inbox_id={} schema_version={}",
            plan.inbox_id,
            plan.schema_version
        );
    }
    let members = effect_plan_members(plan);
    let Some(canonical) = members.first() else {
        anyhow::bail!("inbox effect batch 不能为空");
    };
    if canonical.inbox_id != plan.inbox_id
        || canonical.resolution_id != plan.resolution_id
        || canonical.message_hash != plan.message_hash
    {
        anyhow::bail!("inbox effect canonical member 与 plan 顶层字段不一致");
    }
    let mut seen = FxHashSet::default();
    if members
        .iter()
        .any(|member| !seen.insert(member.inbox_id.clone()))
    {
        anyhow::bail!("inbox effect batch 含重复 inbox_id");
    }
    match (&plan.batch_hash, plan.batch_members.is_empty()) {
        (None, true) => Ok(()),
        (Some(expected), false) if inbox_effect_hash(&members)? == *expected => Ok(()),
        _ => anyhow::bail!("inbox effect batch hash 缺失或不匹配"),
    }
}

fn validate_effect_ref(
    reference: &InboxEffectRef,
    plan: &InboxEffectPlan,
    message: &InboxMessage,
) -> anyhow::Result<()> {
    if reference.schema_version != INBOX_EFFECT_SCHEMA_VERSION {
        anyhow::bail!(
            "inbox effect ref schema 不兼容: inbox_id={} schema_version={}",
            message.id,
            reference.schema_version
        );
    }
    if reference.canonical_inbox_id != plan.inbox_id
        || plan.batch_hash.as_deref() != Some(reference.batch_hash.as_str())
    {
        anyhow::bail!(
            "inbox effect ref 与 canonical plan 不一致: inbox_id={}",
            message.id
        );
    }
    Ok(())
}

fn validate_effect_plan_message_member(
    canonical_inbox_id: &InboxId,
    members: &FxHashMap<InboxId, InboxEffectMember>,
    message: &InboxMessage,
) -> anyhow::Result<()> {
    let expected_resolution_id =
        cau_resolution_context(message).map(|context| context.resolution.resolution_id.clone());
    let message_hash = inbox_effect_hash(message)?;
    let Some(member) = members.get(&message.id) else {
        anyhow::bail!(
            "inbox effect 冲突: inbox_id={} 不属于 canonical batch={}",
            message.id,
            canonical_inbox_id
        );
    };
    if member.resolution_id != expected_resolution_id || member.message_hash != message_hash {
        anyhow::bail!(
            "inbox effect 冲突: inbox_id={} 已存在不同 CAU context 或 message payload",
            message.id
        );
    }
    Ok(())
}

fn ensure_cau_policy_provenance(
    outcome: &mut InternalizeOutcome,
    batch_policy_ids: &FxHashSet<PolicyId>,
) -> anyhow::Result<()> {
    let sole_policy = if batch_policy_ids.len() == 1 {
        batch_policy_ids.iter().next().map(ToString::to_string)
    } else {
        None
    };
    for (field, drafts) in [
        ("new_claims", &mut outcome.new_claims),
        ("updated_claims", &mut outcome.updated_claims),
    ] {
        for (index, draft) in drafts.iter_mut().enumerate() {
            let has_batch_policy = draft.source_claim_ids.iter().any(|source| {
                batch_policy_ids
                    .iter()
                    .any(|policy_id| source == &policy_id.to_string())
            });
            if has_batch_policy {
                continue;
            }
            if let Some(policy_id) = sole_policy.as_ref() {
                // 单一可归因 Policy 时沿用旧单条 CAU 的自动 provenance 行为。
                draft.source_claim_ids.push(policy_id.clone());
            } else {
                anyhow::bail!(
                    "ClaimAttributeUpdate {field}[{index}] 必须引用至少一个真正相关的本批 CAU PolicyId"
                );
            }
        }
    }
    Ok(())
}

fn effect_summary(plan: &InboxEffectPlan) -> InternalizeSummary {
    InternalizeSummary {
        trace_id: plan.trace.as_ref().map(|trace| trace.id.clone()),
        new_claim_ids: plan
            .new_claims
            .iter()
            .map(|claim| claim.id.clone())
            .collect(),
        updated_claim_ids: plan
            .updated_claims
            .iter()
            .map(|update| update.target.id.clone())
            .collect(),
        deprecated_claim_ids: plan.deprecated_claim_ids.clone(),
        new_dispute_ids: plan
            .new_disputes
            .iter()
            .map(|dispute| dispute.id.clone())
            .collect(),
        warnings: plan.warnings.clone(),
    }
}

fn inbox_pull_warning(err: &anyhow::Error) -> String {
    match err.downcast_ref::<MaintainerClientError>() {
        Some(MaintainerClientError::Auth { operation, status }) => format!(
            "Maintainer inbox 拉取鉴权失败，已跳过远端拉取并继续处理本地 inbox：operation={operation} status={status}。请检查当前 upstream 的 acn_key_env。"
        ),
        Some(MaintainerClientError::Client {
            operation,
            status: 403,
            ..
        }) => format!(
            "Maintainer inbox 拉取被拒绝，已跳过远端拉取并继续处理本地 inbox：operation={operation} status=403。请检查当前 upstream 身份和对象绑定。"
        ),
        _ => format!(
            "Maintainer inbox 拉取失败，已跳过远端拉取并继续处理本地 inbox：{err:#}"
        ),
    }
}

fn inbox_ack_warning(err: &anyhow::Error) -> String {
    match err.downcast_ref::<MaintainerClientError>() {
        Some(MaintainerClientError::LegacyServer { operation, status }) => format!(
            "Inbox receipt ACK warning: maintainer is a legacy server without ACK support; local inbox remains durable and will continue processing: operation={operation} status={status}."
        ),
        Some(MaintainerClientError::Auth { operation, status }) => format!(
            "Inbox receipt ACK warning: maintainer auth failed after local persistence; local inbox will continue processing: operation={operation} status={status}."
        ),
        Some(MaintainerClientError::Client {
            operation,
            status: 403,
            ..
        }) => format!(
            "Inbox receipt ACK warning: maintainer rejected the current identity after local persistence; local inbox will continue processing: operation={operation} status=403."
        ),
        _ => format!(
            "Inbox receipt ACK warning: maintainer unavailable after local persistence; local inbox will continue processing and ACK can converge after redelivery: {err:#}"
        ),
    }
}

fn inbox_policy_ids(messages: &[InboxMessage]) -> Vec<PolicyId> {
    let mut out = Vec::new();
    for msg in messages {
        let policy_id = msg.policy_id();
        if !out.contains(policy_id) {
            out.push(policy_id.clone());
        }
    }
    out
}

#[derive(Default)]
struct PolicyDeprecationSummary {
    trace_id: Option<TraceId>,
    deprecated_claim_ids: Vec<ClaimId>,
    warnings: Vec<String>,
}

#[derive(Default)]
struct InternalizeSummary {
    trace_id: Option<TraceId>,
    new_claim_ids: Vec<ClaimId>,
    updated_claim_ids: Vec<ClaimId>,
    deprecated_claim_ids: Vec<ClaimId>,
    new_dispute_ids: Vec<DisputeId>,
    warnings: Vec<String>,
}

impl InternalizeSummary {
    fn extend(&mut self, other: Self) {
        if self.trace_id.is_none() {
            self.trace_id = other.trace_id;
        }
        self.new_claim_ids.extend(other.new_claim_ids);
        self.updated_claim_ids.extend(other.updated_claim_ids);
        self.deprecated_claim_ids.extend(other.deprecated_claim_ids);
        self.new_dispute_ids.extend(other.new_dispute_ids);
        self.warnings.extend(other.warnings);
    }
}

fn push_upload_warning(
    warnings: &mut Vec<String>,
    report: super::maintainer_upload::MaintainerUploadReport,
) {
    if let Some(warning) = report.warning {
        warnings.push(warning);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::{json, Value};

    use super::*;
    use crate::agent::fs::{
        LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore,
        LocalFsReportedDisputeClaimSetStore,
    };
    use crate::agent::maintainer_upload::{LocalFsMaintainerUploadQueue, PendingMaintainerUploads};
    use crate::agent::traits::{
        InboxReader, LocalClaimStore, MemoryStore, ReportedDisputeClaimSetStore,
    };
    use crate::api::{
        ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
    };
    use crate::claim::{
        AgentId, ClaimStatus, Confidence, InboxId, InboxMessageKind, Policy, PolicyId,
        PolicyMessageType, PolicyStatus, SourceId, Trace,
    };
    use crate::maintainer::traits::MaintainerClient;
    use crate::memory::{MemoryOp, MemoryTarget};
    use crate::router::{AgentQuery, RouterClient, RouterQueryResult, ScopesOverviewSnapshot};
    use crate::skill::SkillSummary;
    use crate::storage::{paths, read_yaml};

    #[test]
    fn retryable_502_inbox_warning_matches_router_error_format() {
        let error: anyhow::Error = MaintainerClientError::Retryable {
            operation: "POST /inbox/pull".into(),
            timeout_secs: 30,
            timed_out: false,
            message: "status=502 body=None".into(),
        }
        .into();

        assert_eq!(
            inbox_pull_warning(&error),
            "Maintainer inbox 拉取失败，已跳过远端拉取并继续处理本地 inbox：maintainer POST /inbox/pull 暂时不可用: status=502 body=None"
        );
    }

    struct StaticInboxGenerator {
        expected_kind: InboxInternalizeKind,
        response: Value,
    }

    struct PendingBeforeLedgerReportedDisputeStore {
        pending_path: std::path::PathBuf,
        inner: LocalFsReportedDisputeClaimSetStore,
    }

    impl PendingBeforeLedgerReportedDisputeStore {
        fn new(agent_home: std::path::PathBuf) -> Self {
            Self {
                pending_path: paths::agent_home_pending_maintainer_uploads_path(&agent_home),
                inner: LocalFsReportedDisputeClaimSetStore::new(agent_home),
            }
        }
    }

    #[async_trait]
    impl ReportedDisputeClaimSetStore for PendingBeforeLedgerReportedDisputeStore {
        async fn contains_claim_set(&self, claims: &[ClaimId]) -> anyhow::Result<bool> {
            self.inner.contains_claim_set(claims).await
        }

        async fn record_claim_set(
            &self,
            claims: &[ClaimId],
            dispute_id: &DisputeId,
            reported_at: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            let pending: PendingMaintainerUploads = read_yaml(&self.pending_path).await?;
            anyhow::ensure!(
                pending
                    .disputes
                    .iter()
                    .any(|dispute| dispute.id == *dispute_id),
                "dispute ledger must be recorded only after durable pending staging"
            );
            self.inner
                .record_claim_set(claims, dispute_id, reported_at)
                .await
        }
    }

    #[async_trait]
    impl InboxJsonGenerator for StaticInboxGenerator {
        async fn generate_json(
            &self,
            kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<ProviderTransport>,
        ) -> anyhow::Result<Value> {
            assert_eq!(kind, self.expected_kind);
            Ok(self.response.clone())
        }

        async fn generate_claim_attribute_update_json(
            &self,
            _request: ClaimAttributeUpdateInternalizeRequest,
        ) -> anyhow::Result<Value> {
            assert_eq!(
                self.expected_kind,
                InboxInternalizeKind::ClaimAttributeUpdate
            );
            Ok(self.response.clone())
        }
    }

    struct CountingInboxGenerator {
        response: Value,
        calls: AtomicUsize,
    }

    struct LockObservingInboxGenerator {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl InboxJsonGenerator for LockObservingInboxGenerator {
        async fn generate_json(
            &self,
            kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<ProviderTransport>,
        ) -> anyhow::Result<Value> {
            assert_eq!(kind, InboxInternalizeKind::PolicyUpdate);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [],
            }))
        }
    }

    struct CountingArbitrationProvider {
        responses: Mutex<VecDeque<anyhow::Result<ProviderResponse>>>,
        requests: Mutex<Vec<ProviderRequest>>,
    }

    impl CountingArbitrationProvider {
        fn new(responses: Vec<anyhow::Result<ProviderResponse>>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ProviderAdapter for CountingArbitrationProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("arbitration provider response exhausted"))?
        }
    }

    fn arbitration_provider_response(value: impl Into<String>) -> anyhow::Result<ProviderResponse> {
        Ok(ProviderResponse {
            assistant_message: SessionTurnMessage::assistant_text(value.into()),
            stop: ProviderStop::Done,
        })
    }

    struct RecordingClaimAttributeUpdateGenerator {
        response: Value,
        requests: Mutex<Vec<ClaimAttributeUpdateInternalizeRequest>>,
    }

    #[async_trait]
    impl InboxJsonGenerator for RecordingClaimAttributeUpdateGenerator {
        async fn generate_json(
            &self,
            _kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<ProviderTransport>,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("CAU fixture must use the dedicated request")
        }

        async fn generate_claim_attribute_update_json(
            &self,
            request: ClaimAttributeUpdateInternalizeRequest,
        ) -> anyhow::Result<Value> {
            self.requests.lock().unwrap().push(request);
            Ok(self.response.clone())
        }
    }

    #[async_trait]
    impl InboxJsonGenerator for CountingInboxGenerator {
        async fn generate_json(
            &self,
            kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<ProviderTransport>,
        ) -> anyhow::Result<Value> {
            assert_eq!(kind, InboxInternalizeKind::ClaimAttributeUpdate);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }

        async fn generate_claim_attribute_update_json(
            &self,
            _request: ClaimAttributeUpdateInternalizeRequest,
        ) -> anyhow::Result<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    struct RetryRecordingInboxGenerator {
        responses: Mutex<VecDeque<anyhow::Result<Value>>>,
        requests: Mutex<Vec<InternalizeRequest>>,
        preferred_transports: Mutex<Vec<Option<ProviderTransport>>>,
    }

    #[async_trait]
    impl InboxJsonGenerator for RetryRecordingInboxGenerator {
        async fn generate_json(
            &self,
            _kind: InboxInternalizeKind,
            request: InternalizeRequest,
            preferred_transport: Option<ProviderTransport>,
        ) -> anyhow::Result<Value> {
            self.requests.lock().unwrap().push(request);
            self.preferred_transports
                .lock()
                .unwrap()
                .push(preferred_transport);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake inbox response"))?
        }
    }

    struct NoopMaintainerClient;

    #[async_trait]
    impl MaintainerClient for NoopMaintainerClient {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            _inbox_ids: &[InboxId],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn upload_claim(&self, _claim: &Claim) -> anyhow::Result<()> {
            Ok(())
        }

        async fn report_dispute(&self, _dispute: &Dispute) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct PayloadRecordingMaintainerClient {
        claims: Mutex<Vec<Claim>>,
        disputes: Mutex<Vec<Dispute>>,
    }

    #[async_trait]
    impl MaintainerClient for PayloadRecordingMaintainerClient {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            _inbox_ids: &[InboxId],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn upload_claim(&self, claim: &Claim) -> anyhow::Result<()> {
            self.claims.lock().unwrap().push(claim.clone());
            Ok(())
        }

        async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()> {
            self.disputes.lock().unwrap().push(dispute.clone());
            Ok(())
        }
    }

    struct RecoveringAuthMaintainerClient {
        reject_claim_uploads: AtomicBool,
        uploaded_claim_ids: Mutex<Vec<ClaimId>>,
    }

    #[async_trait]
    impl MaintainerClient for RecoveringAuthMaintainerClient {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            _inbox_ids: &[InboxId],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn upload_claim(&self, claim: &Claim) -> anyhow::Result<()> {
            if self.reject_claim_uploads.load(Ordering::SeqCst) {
                return Err(MaintainerClientError::Auth {
                    operation: "claims/upload".into(),
                    status: 401,
                }
                .into());
            }
            self.uploaded_claim_ids
                .lock()
                .unwrap()
                .push(claim.id.clone());
            Ok(())
        }

        async fn report_dispute(&self, _dispute: &Dispute) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct PullFailingMaintainerClient;

    #[async_trait]
    impl MaintainerClient for PullFailingMaintainerClient {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            anyhow::bail!("simulated maintainer timeout")
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            _inbox_ids: &[InboxId],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn upload_claim(&self, _claim: &Claim) -> anyhow::Result<()> {
            Ok(())
        }

        async fn report_dispute(&self, _dispute: &Dispute) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct RecordingAckMaintainerClient {
        pulled: Vec<InboxMessage>,
        fail_ack: bool,
        acked_batches: Mutex<Vec<Vec<InboxId>>>,
        upload_calls: AtomicUsize,
    }

    impl RecordingAckMaintainerClient {
        fn new(pulled: Vec<InboxMessage>, fail_ack: bool) -> Self {
            Self {
                pulled,
                fail_ack,
                acked_batches: Mutex::new(Vec::new()),
                upload_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl MaintainerClient for RecordingAckMaintainerClient {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(self.pulled.clone())
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            inbox_ids: &[InboxId],
        ) -> anyhow::Result<()> {
            self.acked_batches.lock().unwrap().push(inbox_ids.to_vec());
            if self.fail_ack {
                anyhow::bail!("simulated receipt ACK failure");
            }
            Ok(())
        }

        async fn upload_claim(&self, _claim: &Claim) -> anyhow::Result<()> {
            self.upload_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn report_dispute(&self, _dispute: &Dispute) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct PrefixFailingInbox {
        fail_at: usize,
        attempts: AtomicUsize,
        persisted: Mutex<Vec<InboxId>>,
    }

    impl PrefixFailingInbox {
        fn new(fail_at: usize) -> Self {
            Self {
                fail_at,
                attempts: AtomicUsize::new(0),
                persisted: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl InboxReader for PrefixFailingInbox {
        async fn list_pending(&self) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }

        async fn ack(&self, _msg_id: &InboxId) -> anyhow::Result<()> {
            Ok(())
        }

        async fn accept_pulled(&self, msg: &InboxMessage) -> anyhow::Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == self.fail_at {
                anyhow::bail!("simulated local persistence failure");
            }
            self.persisted.lock().unwrap().push(msg.id.clone());
            Ok(())
        }
    }

    struct EmptyRouterClient;

    #[async_trait]
    impl RouterClient for EmptyRouterClient {
        async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    struct ScopesFailingRouterClient;

    #[async_trait]
    impl RouterClient for ScopesFailingRouterClient {
        async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            anyhow::bail!("simulated router timeout")
        }
    }

    fn receipt_test_message(status: PolicyStatus) -> InboxMessage {
        let id = InboxId::random();
        InboxMessage {
            id,
            kind: InboxMessageKind::PolicyUpdate {
                policy: Policy {
                    id: PolicyId::random(),
                    message_type: PolicyMessageType::PolicyUpdate,
                    name: "receipt-test".into(),
                    statement: "receipt test policy".into(),
                    scope: "tests / inbox".into(),
                    status,
                    created_at: Utc::now(),
                    updated_at: None,
                    target_agents: None,
                },
            },
            handled_at: None,
        }
    }

    fn receipt_test_runner(
        dir: &tempfile::TempDir,
        inbox: Arc<dyn InboxReader>,
        maintainer: Arc<dyn MaintainerClient>,
        generator: Arc<dyn InboxJsonGenerator>,
    ) -> AgentRunner {
        receipt_test_runner_with_router(
            dir,
            inbox,
            maintainer,
            generator,
            Arc::new(EmptyRouterClient),
        )
    }

    fn receipt_test_runner_with_router(
        dir: &tempfile::TempDir,
        inbox: Arc<dyn InboxReader>,
        maintainer: Arc<dyn MaintainerClient>,
        generator: Arc<dyn InboxJsonGenerator>,
        router: Arc<dyn RouterClient>,
    ) -> AgentRunner {
        let agent_home = dir.path().to_path_buf();
        AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            generator,
            Arc::new(LocalFsClaimStore::new(agent_home.clone())),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox,
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            router,
            maintainer,
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            0,
            Vec::<SkillSummary>::new(),
        )
    }

    fn empty_receipt_generator() -> Arc<dyn InboxJsonGenerator> {
        Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::PolicyUpdate,
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [],
            }),
        })
    }

    #[tokio::test]
    async fn inbox_internalization_waits_for_agent_knowledge_apply_lock() {
        let dir = tempfile::tempdir().unwrap();
        let generator = Arc::new(LockObservingInboxGenerator {
            calls: AtomicUsize::new(0),
        });
        let runner = Arc::new(receipt_test_runner(
            &dir,
            Arc::new(LocalFsInboxReader::new(dir.path().to_path_buf())),
            Arc::new(NoopMaintainerClient),
            generator.clone(),
        ));
        let guard =
            FileLockGuard::lock_exclusive(paths::agent_home_knowledge_apply_lock_path(dir.path()))
                .await
                .unwrap();
        let task_runner = runner.clone();
        let task_generator = generator.clone();
        let messages = vec![receipt_test_message(PolicyStatus::Active)];
        let task = tokio::spawn(async move {
            task_runner
                .internalize_inbox_updates(
                    task_generator.as_ref(),
                    InboxInternalizeKind::PolicyUpdate,
                    messages,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(generator.calls.load(Ordering::SeqCst), 0);

        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn policy_deprecation_stages_upload_before_releasing_knowledge_lock() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "deprecated-policy".into(),
            statement: "deprecated policy".into(),
            scope: "tests / inbox".into(),
            status: PolicyStatus::Deprecated,
            created_at: Utc::now(),
            updated_at: None,
            target_agents: None,
        };
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        let claim = Claim {
            id: ClaimId::random(),
            name: "policy-backed-claim".into(),
            statement: "policy-backed claim".into(),
            scope: "tests / inbox".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: crate::time::now_seconds(),
            updated_at: None,
            source_claim_ids: vec![SourceId::Policy(policy.id.clone())],
            evidence_summary: "policy source".into(),
        };
        claim_store.write_claim(&claim).await.unwrap();
        let runner = Arc::new(AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            empty_receipt_generator(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        ));
        let pending_guard = FileLockGuard::lock_exclusive(
            paths::agent_home_pending_maintainer_uploads_lock_path(&agent_home),
        )
        .await
        .unwrap();

        let task_runner = runner.clone();
        let task_policy = policy.clone();
        let task =
            tokio::spawn(async move { task_runner.apply_policy_deprecation(&task_policy).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let stored = claim_store.list_local_claims().await.unwrap();
                if stored
                    .iter()
                    .any(|stored| stored.id == claim.id && stored.status == ClaimStatus::Deprecated)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(
            FileLockGuard::try_lock_exclusive(paths::agent_home_knowledge_apply_lock_path(
                &agent_home
            ))
            .await
            .unwrap()
            .is_none(),
            "knowledge lock must cover local apply and durable upload staging"
        );

        drop(pending_guard);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn policy_update_stages_upload_before_releasing_knowledge_lock() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "active-policy".into(),
            statement: "active policy".into(),
            scope: "tests / inbox".into(),
            status: PolicyStatus::Active,
            created_at: Utc::now(),
            updated_at: None,
            target_agents: None,
        };
        let generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::PolicyUpdate,
            response: json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "generated-policy-claim",
                    "statement": "generated policy claim",
                    "scope": "tests / inbox",
                    "confidence": "high",
                    "evidence_summary": "active policy source",
                    "source_claim_ids": [policy.id.as_str()],
                }],
                "updated_claims": [],
                "new_disputes": [],
            }),
        });
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        let runner = Arc::new(AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        ));
        let pending_guard = FileLockGuard::lock_exclusive(
            paths::agent_home_pending_maintainer_uploads_lock_path(&agent_home),
        )
        .await
        .unwrap();
        let message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::PolicyUpdate { policy },
            handled_at: None,
        };

        let task_runner = runner.clone();
        let task_generator = generator.clone();
        let task = tokio::spawn(async move {
            task_runner
                .internalize_inbox_updates(
                    task_generator.as_ref(),
                    InboxInternalizeKind::PolicyUpdate,
                    vec![message],
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if claim_store
                    .list_local_claims()
                    .await
                    .unwrap()
                    .iter()
                    .any(|claim| claim.name == "generated-policy-claim")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(
            FileLockGuard::try_lock_exclusive(paths::agent_home_knowledge_apply_lock_path(
                &agent_home
            ))
            .await
            .unwrap()
            .is_none(),
            "knowledge lock must cover local apply and durable upload staging"
        );

        drop(pending_guard);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn arbitration_message(local: &Claim, remote: &Claim, policy: Policy) -> InboxMessage {
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "shared knowledge conflict".into(),
            reporter_agent_id: local.holder.clone(),
            claims: vec![local.id.clone(), remote.id.clone()],
            summary: "two holders disagree".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let resolution_id = ArbitrationResolutionId::random();
        InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy,
                arbitration_resolution: Some(Box::new(ArbitrationResolutionContext {
                    dispute_id: dispute.id.clone(),
                    resolution: crate::claim::DisputeResolution {
                        resolution_id,
                        resolved_by: crate::claim::ResolvedBy::Automatic,
                        resolved_at: "2026-08-02T00:00:00Z".parse().unwrap(),
                        resolution_type: Some(crate::claim::ResolutionType::ConflictResolved),
                        resolution_basis: Some(crate::claim::ResolutionBasis::Evidence),
                        conclusion: "keep current local facts where evidence supports them".into(),
                        claim_assessments: vec![
                            crate::claim::ClaimAssessment {
                                claim_id: local.id.clone(),
                                recommended_status: ClaimStatus::Deprecated,
                                assessment: "outdated".into(),
                                recommended_scope: None,
                                recommended_statement: None,
                                reason: "newer evidence".into(),
                            },
                            crate::claim::ClaimAssessment {
                                claim_id: remote.id.clone(),
                                recommended_status: ClaimStatus::Active,
                                assessment: "supported".into(),
                                recommended_scope: None,
                                recommended_statement: None,
                                reason: "newer evidence".into(),
                            },
                        ],
                        rejection_reason: None,
                    },
                    context_snapshot_hash: Some("sha256-v1:test".into()),
                    dispute_snapshot: dispute,
                    direct_claim_snapshots: vec![local.clone(), remote.clone()],
                    snapshot_source_resolution_id: None,
                })),
            },
            handled_at: None,
        }
    }

    fn arbitration_claim(holder: &str, name: &str, status: ClaimStatus) -> Claim {
        Claim {
            id: ClaimId::random(),
            name: name.into(),
            statement: format!("{name} statement"),
            scope: "knowledge / shared".into(),
            holder: AgentId::new(holder).unwrap(),
            confidence: Confidence::High,
            status,
            created_at: "2026-07-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "evidence".into(),
        }
    }

    fn prompt_arbitration_generator(
        provider: Arc<CountingArbitrationProvider>,
        retry_count: u32,
    ) -> Arc<PromptInboxJsonGenerator> {
        let caller = Arc::new(StructuredJsonCaller::new(
            provider,
            1024,
            retry_count,
            Duration::ZERO,
            Duration::ZERO,
        ));
        Arc::new(PromptInboxJsonGenerator::new(
            Arc::new(PromptRegistry::bundled().unwrap()),
            caller,
        ))
    }

    async fn arbitration_retry_budget_runner(
        dir: &tempfile::TempDir,
        local: &Claim,
        generator: Arc<dyn InboxJsonGenerator>,
        retry_count: u32,
    ) -> AgentRunner {
        let agent_home = dir.path().to_path_buf();
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(local).await.unwrap();
        AgentRunner::new_local(
            local.holder.clone(),
            generator,
            claim_store,
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            retry_count,
            Vec::<SkillSummary>::new(),
        )
    }

    #[tokio::test]
    async fn arbitration_retry_budget_business_validation_can_recover_within_total_budget() {
        let dir = tempfile::tempdir().unwrap();
        let local = arbitration_claim("agent-a", "local", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "remote", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let invalid_remote_update = json!({
            "new_claims": [],
            "updated_claims": [{
                "id": remote.id.as_str(),
                "name": remote.name,
                "statement": remote.statement,
                "scope": remote.scope,
                "confidence": "high",
                "status": "deprecated",
                "evidence_summary": "invalid remote edit",
                "source_claim_ids": []
            }],
            "new_disputes": []
        });
        let accepted = json!({
            "new_claims": [],
            "updated_claims": [],
            "new_disputes": []
        });
        let provider = Arc::new(CountingArbitrationProvider::new(vec![
            arbitration_provider_response(invalid_remote_update.to_string()),
            arbitration_provider_response(accepted.to_string()),
        ]));
        let generator = prompt_arbitration_generator(provider.clone(), 1);
        let runner = arbitration_retry_budget_runner(&dir, &local, generator.clone(), 1).await;

        runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();

        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.retry_count_override == Some(0)));
    }

    #[tokio::test]
    async fn arbitration_retry_budget_never_exceeds_configured_total_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let local = arbitration_claim("agent-a", "local", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "remote", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let invalid_remote_update = json!({
            "new_claims": [],
            "updated_claims": [{
                "id": remote.id.as_str(),
                "name": remote.name,
                "statement": remote.statement,
                "scope": remote.scope,
                "confidence": "high",
                "status": "deprecated",
                "evidence_summary": "invalid remote edit",
                "source_claim_ids": []
            }],
            "new_disputes": []
        });
        let provider = Arc::new(CountingArbitrationProvider::new(vec![
            arbitration_provider_response(invalid_remote_update.to_string()),
            arbitration_provider_response(invalid_remote_update.to_string()),
            arbitration_provider_response("not-json"),
            arbitration_provider_response("still-not-json"),
        ]));
        let generator = prompt_arbitration_generator(provider.clone(), 2);
        let runner = arbitration_retry_budget_runner(&dir, &local, generator.clone(), 2).await;

        let error = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("JSON"));
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests
            .iter()
            .all(|request| request.retry_count_override == Some(0)));
    }

    #[tokio::test]
    async fn arbitration_request_restores_holder_deprecated_direct_claim() {
        const SAFE_MEMORY_FACT: &str =
            "Operational finding: staged rollout requires an independently verified rollback path.";
        const SHARED_DERIVED_FACT: &str =
            "Before a staged release, a separate reviewer must confirm that rollback is workable.";
        const PRIVATE_MEMORY_SENTINEL: &str = "[private] account note PRIVATE_MEMORY_ONLY_SENTINEL";
        const USER_SENTINEL: &str = "PRIVATE_USER_MUST_NOT_BE_INCLUDED";

        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let historical_source_id = ClaimId::random();
        let mut local_direct =
            arbitration_claim("agent-a", "local_direct", ClaimStatus::Deprecated);
        local_direct.source_claim_ids = vec![SourceId::Claim(historical_source_id.clone())];
        let remote_direct = arbitration_claim("agent-b", "remote_direct", ClaimStatus::Active);
        let editable_active = arbitration_claim("agent-a", "editable_active", ClaimStatus::Active);
        let editable_stale = arbitration_claim("agent-a", "editable_stale", ClaimStatus::Stale);
        let hidden_deprecated =
            arbitration_claim("agent-a", "hidden_deprecated", ClaimStatus::Deprecated);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local_direct.holder.clone()]),
        };
        let message = arbitration_message(&local_direct, &remote_direct, policy.clone());
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        for claim in [
            &local_direct,
            &editable_active,
            &editable_stale,
            &hidden_deprecated,
        ] {
            claim_store.write_claim(claim).await.unwrap();
        }
        let memory_store = Arc::new(LocalFsMemoryStore::new(
            agent_home.clone(),
            1600,
            1000,
            false,
        ));
        memory_store
            .apply_ops(&[
                MemoryOp::Add {
                    target: MemoryTarget::Memory,
                    entry: SAFE_MEMORY_FACT.into(),
                },
                MemoryOp::Add {
                    target: MemoryTarget::Memory,
                    entry: PRIVATE_MEMORY_SENTINEL.into(),
                },
                MemoryOp::Add {
                    target: MemoryTarget::User,
                    entry: USER_SENTINEL.into(),
                },
            ])
            .await
            .unwrap();
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "follow_up_knowledge",
                    "statement": SHARED_DERIVED_FACT,
                    "scope": "knowledge / shared",
                    "confidence": "high",
                    "evidence_summary": "based on visible local and remote claims",
                    "source_claim_ids": [
                        editable_active.id.as_str(),
                        remote_direct.id.as_str(),
                        policy.id.as_str()
                    ]
                }],
                "updated_claims": [{
                    "id": local_direct.id.as_str(),
                    "name": local_direct.name,
                    "statement": local_direct.statement,
                    "scope": local_direct.scope,
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "the holder accepts the replacement after local review",
                    "source_claim_ids": [historical_source_id.as_str(), remote_direct.id.as_str()]
                }],
                "new_disputes": [{
                    "id": "$new_dispute_0$",
                    "name": "follow_up_conflict",
                    "claims": [editable_active.id.as_str(), remote_direct.id.as_str(), "$new_claim_0$"],
                    "summary": "newly created knowledge conflicts with visible local and remote evidence"
                }]
            }),
            requests: Mutex::new(Vec::new()),
        });
        let maintainer = Arc::new(PayloadRecordingMaintainerClient::default());
        let runner = AgentRunner::new(
            local_direct.holder.clone(),
            generator.clone(),
            claim_store,
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            memory_store,
            Arc::new(EmptyRouterClient),
            maintainer.clone(),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let summary = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();

        {
            let requests = generator.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let request = &requests[0];
            assert_eq!(request.claim_attribute_updates.len(), 1);
            let item = &request.claim_attribute_updates[0];
            assert_eq!(item.claim_attribute_update, message);
            assert_eq!(
                item.conclusion,
                "keep current local facts where evidence supports them"
            );
            assert!(item.resolution.is_some());
            assert!(item.dispute.is_some());
            assert_eq!(
                request
                    .local_claims
                    .iter()
                    .map(|claim| claim.id.clone())
                    .collect::<FxHashSet<_>>(),
                FxHashSet::from_iter([
                    local_direct.id.clone(),
                    editable_active.id.clone(),
                    editable_stale.id.clone(),
                ])
            );
            assert!(request
                .local_claims
                .windows(2)
                .all(|pair| pair[0].id < pair[1].id));
            assert_eq!(
                item.direct_claims
                    .iter()
                    .map(|claim| claim.id.clone())
                    .collect::<FxHashSet<_>>(),
                FxHashSet::from_iter([local_direct.id.clone(), remote_direct.id.clone()])
            );
            assert!(!request
                .local_claims
                .iter()
                .any(|claim| claim.id == hidden_deprecated.id));
            let serialized = serde_json::to_string(request).unwrap();
            assert!(!serialized.contains(SAFE_MEMORY_FACT));
            assert!(!serialized.contains(PRIVATE_MEMORY_SENTINEL));
            assert!(!serialized.contains(USER_SENTINEL));
        }

        assert_eq!(summary.updated_claim_ids, vec![local_direct.id.clone()]);
        assert_eq!(summary.new_claim_ids.len(), 1);
        assert_eq!(summary.new_dispute_ids.len(), 1);
        let uploaded_claims = maintainer.claims.lock().unwrap().clone();
        assert_eq!(uploaded_claims.len(), 2);
        assert!(uploaded_claims
            .iter()
            .any(|claim| claim.statement == SHARED_DERIVED_FACT));
        let restored = uploaded_claims
            .iter()
            .find(|claim| claim.id == local_direct.id)
            .unwrap();
        assert_eq!(restored.status, ClaimStatus::Active);
        assert!(restored
            .source_claim_ids
            .contains(&SourceId::Claim(historical_source_id)));
        assert!(restored
            .source_claim_ids
            .contains(&SourceId::Policy(policy.id)));
        let uploaded_disputes = maintainer.disputes.lock().unwrap().clone();
        assert_eq!(uploaded_disputes.len(), 1);
        assert!(uploaded_disputes[0].claims.contains(&editable_active.id));
        assert!(uploaded_disputes[0].claims.contains(&remote_direct.id));

        let effect_text = tokio::fs::read_to_string(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap();
        let trace_path = paths::agent_home_traces_dir(&agent_home)
            .join(format!("{}.yaml", summary.trace_id.unwrap()));
        let trace: Trace = read_yaml(&trace_path).await.unwrap();
        assert!(trace
            .input_claims
            .contains(&SourceId::Claim(editable_active.id.clone())));
        assert!(trace
            .input_claims
            .contains(&SourceId::Claim(remote_direct.id.clone())));
        let trace_text = tokio::fs::read_to_string(trace_path).await.unwrap();
        let shared_payload = serde_json::to_string(&(uploaded_claims, uploaded_disputes)).unwrap();
        for serialized in [effect_text, trace_text, shared_payload] {
            assert!(!serialized.contains(PRIVATE_MEMORY_SENTINEL));
            assert!(!serialized.contains(USER_SENTINEL));
            assert!(!serialized.contains("private_memory"));
        }
    }

    #[tokio::test]
    async fn arbitration_rejects_history_only_source_as_dispute_reference() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let historical_source_id = ClaimId::random();
        let mut local_direct = arbitration_claim("agent-a", "local_direct", ClaimStatus::Active);
        local_direct.source_claim_ids = vec![SourceId::Claim(historical_source_id.clone())];
        let remote_direct = arbitration_claim("agent-b", "remote_direct", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local_direct.holder.clone()]),
        };
        let message = arbitration_message(&local_direct, &remote_direct, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local_direct).await.unwrap();
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [{
                    "id": "$new_dispute_0$",
                    "name": "hidden_source_conflict",
                    "claims": [local_direct.id.as_str(), historical_source_id.as_str()],
                    "summary": "a historical source id is not itself visible dispute context"
                }]
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new_local(
            local_direct.holder.clone(),
            generator.clone(),
            claim_store,
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let error = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .err()
            .unwrap();

        assert!(error
            .to_string()
            .contains("不在本次上下文/本批新生成中的 claim id"));
        assert!(!tokio::fs::try_exists(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn arbitration_does_not_derive_source_permissions_from_memory() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local_direct = arbitration_claim("agent-a", "local_direct", ClaimStatus::Active);
        let remote_direct = arbitration_claim("agent-b", "remote_direct", ClaimStatus::Active);
        let memory_only_id = ClaimId::random();
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local_direct.holder.clone()]),
        };
        let message = arbitration_message(&local_direct, &remote_direct, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local_direct).await.unwrap();
        let memory_store = Arc::new(LocalFsMemoryStore::new(
            agent_home.clone(),
            1600,
            1000,
            false,
        ));
        memory_store
            .apply_ops(&[MemoryOp::Add {
                target: MemoryTarget::Memory,
                entry: format!("private observation references {memory_only_id}"),
            }])
            .await
            .unwrap();
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "memory_only_source",
                    "statement": "shareable conclusion",
                    "scope": "knowledge / shared",
                    "confidence": "medium",
                    "evidence_summary": "attempted to cite private memory",
                    "source_claim_ids": [memory_only_id.as_str()]
                }],
                "updated_claims": [],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new_local(
            local_direct.holder.clone(),
            generator.clone(),
            claim_store,
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            memory_store,
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let error = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("不在本次上下文/本批新生成中"));
        assert_eq!(generator.requests.lock().unwrap().len(), 1);
        assert!(!tokio::fs::try_exists(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn arbitration_rejects_invented_policy_source_before_side_effects() {
        let dir = tempfile::tempdir().unwrap();
        let local = arbitration_claim("agent-a", "local_direct", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "remote_direct", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let invented_policy = PolicyId::random();
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "invented_policy_source",
                    "statement": "must not be persisted",
                    "scope": "knowledge / shared",
                    "confidence": "high",
                    "evidence_summary": "invented provenance",
                    "source_claim_ids": [invented_policy.as_str()]
                }],
                "updated_claims": [],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = arbitration_retry_budget_runner(&dir, &local, generator.clone(), 0).await;

        let error = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .err()
            .unwrap();

        assert!(error
            .to_string()
            .contains("不是本次 LLM 输入中可见的 PolicyId"));
        assert!(!tokio::fs::try_exists(paths::agent_home_inbox_effect_path(
            dir.path(),
            &message.id,
        ))
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn claim_attribute_update_can_edit_non_deprecated_non_direct_local_claims() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local_direct = arbitration_claim("agent-a", "local_direct", ClaimStatus::Active);
        let editable_local = arbitration_claim("agent-a", "editable_local", ClaimStatus::Stale);
        let remote_direct = arbitration_claim("agent-b", "remote_direct", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local_direct.holder.clone()]),
        };
        let message = arbitration_message(&local_direct, &remote_direct, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local_direct).await.unwrap();
        claim_store.write_claim(&editable_local).await.unwrap();
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": editable_local.id.as_str(),
                    "name": editable_local.name,
                    "statement": editable_local.statement,
                    "scope": editable_local.scope,
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "valid non-direct edit",
                    "source_claim_ids": []
                }],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new_local(
            local_direct.holder.clone(),
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let summary = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();

        assert_eq!(summary.updated_claim_ids, vec![editable_local.id.clone()]);
        let updated = claim_store
            .list_local_claims()
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.id == editable_local.id)
            .unwrap();
        assert_eq!(updated.status, ClaimStatus::Active);
        assert!(tokio::fs::try_exists(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn claim_attribute_update_rejects_deprecated_non_direct_local_claims() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local_direct = arbitration_claim("agent-a", "local_direct", ClaimStatus::Active);
        let deprecated_local =
            arbitration_claim("agent-a", "deprecated_local", ClaimStatus::Deprecated);
        let remote_direct = arbitration_claim("agent-b", "remote_direct", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_resolution".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local_direct.holder.clone()]),
        };
        let message = arbitration_message(&local_direct, &remote_direct, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local_direct).await.unwrap();
        claim_store.write_claim(&deprecated_local).await.unwrap();
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": deprecated_local.id.as_str(),
                    "name": deprecated_local.name,
                    "statement": deprecated_local.statement,
                    "scope": deprecated_local.scope,
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "must remain outside the editable set",
                    "source_claim_ids": []
                }],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new_local(
            local_direct.holder.clone(),
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let error = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("不是当前 agent 本地已有 claim"));
        assert!(!tokio::fs::try_exists(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap());
    }

    #[test]
    fn human_arbitration_without_type_is_valid_but_unresolved_is_rejected() {
        let local = arbitration_claim("agent-a", "local", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "remote", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "human replacement".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let mut message = arbitration_message(&local, &remote, policy);
        let resolution_context = match &mut message.kind {
            InboxMessageKind::ClaimAttributeUpdate {
                arbitration_resolution: Some(resolution_context),
                ..
            } => resolution_context,
            _ => panic!("fixture must contain an arbitration resolution"),
        };
        resolution_context.resolution.resolved_by = crate::claim::ResolvedBy::Human;
        resolution_context.resolution.resolution_type = None;
        let resolution_context = resolution_context.as_ref().clone();

        validate_arbitration_message(&message, &resolution_context).unwrap();

        let mut unresolved = resolution_context.clone();
        unresolved.resolution.resolution_type = Some(crate::claim::ResolutionType::Unresolved);
        assert!(validate_arbitration_message(&message, &unresolved)
            .unwrap_err()
            .to_string()
            .contains("unresolved"));

        let mut insufficient = resolution_context;
        insufficient.resolution.resolution_basis =
            Some(crate::claim::ResolutionBasis::InsufficientEvidence);
        assert!(validate_arbitration_message(&message, &insufficient)
            .unwrap_err()
            .to_string()
            .contains("insufficient_evidence"));
    }

    #[test]
    fn unchanged_follow_up_dispute_is_suppressed_but_new_claim_semantics_are_allowed() {
        let local = arbitration_claim("agent-a", "local", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "remote", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let resolution_context = match &message.kind {
            InboxMessageKind::ClaimAttributeUpdate {
                arbitration_resolution: Some(resolution_context),
                ..
            } => resolution_context.as_ref(),
            _ => panic!("fixture must contain arbitration resolution"),
        };
        let repeated = Dispute {
            id: DisputeId::random(),
            name: "same conflict".into(),
            reporter_agent_id: local.holder.clone(),
            claims: vec![remote.id.clone(), local.id.clone()],
            summary: "same evidence in different words".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: Utc::now(),
            resolved_at: None,
        };
        let editable = FxHashMap::from_iter([(local.id.clone(), local.clone())]);

        assert!(repeats_resolved_arbitration_input(
            &repeated,
            resolution_context,
            &editable,
            &[]
        ));

        let mut changed = local;
        changed.status = ClaimStatus::Stale;
        assert!(!repeats_resolved_arbitration_input(
            &repeated,
            resolution_context,
            &editable,
            &[changed]
        ));
    }

    #[tokio::test]
    async fn business_retry_resends_unchanged_request() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        let message = receipt_test_message(PolicyStatus::Active);
        inbox.accept_pulled(&message).await.unwrap();
        let generator = Arc::new(RetryRecordingInboxGenerator {
            responses: Mutex::new(VecDeque::from([
                Ok(json!({
                    "new_claims": [{"id":"$new_claim_0$"}],
                    "updated_claims": [],
                    "new_disputes": [],
                })),
                Ok(json!({
                    "new_claims": [],
                    "updated_claims": [],
                    "new_disputes": [],
                })),
            ])),
            requests: Mutex::new(Vec::new()),
            preferred_transports: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            generator.clone(),
            Arc::new(LocalFsClaimStore::new(agent_home.clone())),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox,
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            1,
            Vec::<SkillSummary>::new(),
        );

        let report = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(report.policy_count, 1);
        let requests = generator.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert_eq!(
            generator.preferred_transports.lock().unwrap().as_slice(),
            &[None, None]
        );
    }

    #[tokio::test]
    async fn no_consumable_inbox_retry_preserves_actual_transport() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        let message = receipt_test_message(PolicyStatus::Active);
        inbox.accept_pulled(&message).await.unwrap();
        let generator = Arc::new(RetryRecordingInboxGenerator {
            responses: Mutex::new(VecDeque::from([
                Err(crate::api::StructuredJsonNoConsumableOutput::new(
                    "Responses 响应没有可消费的 output_text 或 function_call".into(),
                    ProviderTransport::ResponsesNonStreaming,
                )
                .into()),
                Ok(json!({
                    "new_claims": [],
                    "updated_claims": [],
                    "new_disputes": [],
                })),
            ])),
            requests: Mutex::new(Vec::new()),
            preferred_transports: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            generator.clone(),
            Arc::new(LocalFsClaimStore::new(agent_home.clone())),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox,
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            1,
            Vec::<SkillSummary>::new(),
        );

        let report = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(report.policy_count, 1);
        assert_eq!(
            generator.preferred_transports.lock().unwrap().as_slice(),
            &[None, Some(ProviderTransport::ResponsesNonStreaming)]
        );
    }

    async fn assert_invalid_inbox_output_has_no_side_effects(
        response: Value,
        expected_error: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        let original = Claim {
            id: ClaimId::random(),
            name: "stable_rule".into(),
            statement: "原始结论".into(),
            scope: "tests / inbox".into(),
            holder: agent_id.clone(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: crate::time::now_seconds() - chrono::Duration::days(1),
            updated_at: None,
            source_claim_ids: vec![SourceId::Policy(PolicyId::random())],
            evidence_summary: "原始证据".into(),
        };
        claim_store.write_claim(&original).await.unwrap();

        let message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::PolicyUpdate {
                policy: Policy {
                    id: PolicyId::random(),
                    message_type: PolicyMessageType::PolicyUpdate,
                    name: "validation_boundary".into(),
                    statement: "用于验证 inbox 输出边界".into(),
                    scope: "tests / inbox".into(),
                    status: PolicyStatus::Active,
                    created_at: Utc::now(),
                    updated_at: None,
                    target_agents: None,
                },
            },
            handled_at: None,
        };
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        inbox.accept_pulled(&message).await.unwrap();
        let maintainer = Arc::new(RecordingAckMaintainerClient::new(Vec::new(), false));
        let generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::PolicyUpdate,
            response,
        });
        let runner = AgentRunner::new(
            agent_id,
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox.clone(),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            maintainer.clone(),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let err = runner
            .process_inbox_with(generator.as_ref())
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains(expected_error),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            claim_store.list_local_claims().await.unwrap(),
            vec![original]
        );
        assert_eq!(maintainer.upload_calls.load(Ordering::SeqCst), 0);
        assert_eq!(inbox.list_pending().await.unwrap(), vec![message.clone()]);
        let done_path =
            paths::agent_home_inbox_dir(&agent_home).join(format!("{}.done.yaml", message.id));
        assert!(!tokio::fs::try_exists(done_path).await.unwrap());
        assert!(
            !tokio::fs::try_exists(paths::agent_home_traces_dir(&agent_home))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn large_consecutive_claim_attribute_updates_share_one_call_and_batch_journal() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        let messages = (0..64)
            .map(|index| InboxMessage {
                id: InboxId::random(),
                kind: InboxMessageKind::ClaimAttributeUpdate {
                    policy: Policy {
                        id: PolicyId::random(),
                        message_type: PolicyMessageType::ClaimAttributeUpdate,
                        name: format!("cau_{index}"),
                        statement: format!("conclusion {index}"),
                        scope: "tests / cau".into(),
                        status: PolicyStatus::Active,
                        created_at: Utc::now(),
                        updated_at: None,
                        target_agents: Some(vec![agent_id.clone()]),
                    },
                    arbitration_resolution: None,
                },
                handled_at: None,
            })
            .collect::<Vec<_>>();
        for message in &messages {
            inbox.accept_pulled(message).await.unwrap();
        }
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new_local(
            agent_id,
            generator.clone(),
            Arc::new(LocalFsClaimStore::new(agent_home.clone())),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox,
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let report = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(report.total, messages.len());
        assert_eq!(report.claim_attribute_count, messages.len());
        {
            let requests = generator.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0]
                    .claim_attribute_updates
                    .iter()
                    .map(|item| item.claim_attribute_update.id.clone())
                    .collect::<Vec<_>>(),
                messages
                    .iter()
                    .map(|message| message.id.clone())
                    .collect::<Vec<_>>()
            );
        }
        let canonical: InboxEffectPlan = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &messages[0].id,
        ))
        .await
        .unwrap();
        assert_eq!(canonical.state, InboxEffectState::Applied);
        assert_eq!(canonical.batch_members.len(), messages.len());
        assert!(canonical.batch_hash.is_some());
        let reference: InboxEffectRef = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &messages[1].id,
        ))
        .await
        .unwrap();
        assert_eq!(reference.canonical_inbox_id, messages[0].id);
        assert_eq!(Some(reference.batch_hash), canonical.batch_hash);
    }

    #[tokio::test]
    async fn batch_effect_ref_replays_remaining_cau_without_second_model_call() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let messages = ["first", "second", "third"].map(|name| InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: Policy {
                    id: PolicyId::random(),
                    message_type: PolicyMessageType::ClaimAttributeUpdate,
                    name: format!("{name}_cau"),
                    statement: format!("{name} conclusion"),
                    scope: "tests / cau".into(),
                    status: PolicyStatus::Active,
                    created_at: Utc::now(),
                    updated_at: None,
                    target_agents: Some(vec![agent_id.clone()]),
                },
                arbitration_resolution: None,
            },
            handled_at: None,
        });
        let generator = Arc::new(CountingInboxGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": []
            }),
            calls: AtomicUsize::new(0),
        });
        let runner = AgentRunner::new_local(
            agent_id,
            generator.clone(),
            Arc::new(LocalFsClaimStore::new(agent_home.clone())),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        runner
            .internalize_claim_attribute_update_messages(generator.as_ref(), &messages)
            .await
            .unwrap();
        let canonical_path = paths::agent_home_inbox_effect_path(&agent_home, &messages[0].id);
        let mut interrupted: InboxEffectPlan = read_yaml(&canonical_path).await.unwrap();
        interrupted.state = InboxEffectState::Prepared;
        write_yaml_atomic(
            &canonical_path,
            &InboxEffectRecord::Plan(Box::new(interrupted)),
        )
        .await
        .unwrap();

        for remaining in &messages[1..] {
            runner
                .internalize_claim_attribute_update_messages(
                    generator.as_ref(),
                    std::slice::from_ref(remaining),
                )
                .await
                .unwrap();
        }

        assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
        let recovered: InboxEffectPlan = read_yaml(&canonical_path).await.unwrap();
        assert_eq!(recovered.state, InboxEffectState::Applied);
    }

    #[tokio::test]
    async fn prepared_prefix_is_applied_before_planning_later_cau() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first_policy = PolicyId::random();
        let second_policy = PolicyId::random();
        let messages = [
            ("first", first_policy.clone()),
            ("second", second_policy.clone()),
        ]
        .map(|(name, policy_id)| InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: Policy {
                    id: policy_id,
                    message_type: PolicyMessageType::ClaimAttributeUpdate,
                    name: format!("{name}_cau"),
                    statement: format!("{name} conclusion"),
                    scope: "tests / ordered-recovery".into(),
                    status: PolicyStatus::Active,
                    created_at: Utc::now(),
                    updated_at: None,
                    target_agents: Some(vec![agent_id.clone()]),
                },
                arbitration_resolution: None,
            },
            handled_at: None,
        });
        let mut local = arbitration_claim("agent-a", "shared", ClaimStatus::Active);
        local.statement = "before recovery".into();
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local).await.unwrap();
        let first_generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::ClaimAttributeUpdate,
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": local.id.as_str(),
                    "name": local.name,
                    "statement": "after first CAU",
                    "scope": local.scope,
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "first CAU applied",
                    "source_claim_ids": [first_policy.as_str()]
                }],
                "new_disputes": []
            }),
        });
        let runner = AgentRunner::new_local(
            agent_id,
            first_generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let first_plan = runner
            .prepare_claim_attribute_update_effect(
                first_generator.as_ref(),
                std::slice::from_ref(&messages[0]),
            )
            .await
            .unwrap();
        write_yaml_atomic(
            &paths::agent_home_inbox_effect_path(&agent_home, &messages[0].id),
            &InboxEffectRecord::Plan(Box::new(first_plan)),
        )
        .await
        .unwrap();

        let second_generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": local.id.as_str(),
                    "name": local.name,
                    "statement": "after second CAU",
                    "scope": local.scope,
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "second CAU applied after recovery",
                    "source_claim_ids": [second_policy.as_str()]
                }],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });

        runner
            .internalize_claim_attribute_update_messages(second_generator.as_ref(), &messages)
            .await
            .unwrap();

        {
            let requests = second_generator.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].claim_attribute_updates.len(), 1);
            assert_eq!(
                requests[0].claim_attribute_updates[0]
                    .claim_attribute_update
                    .id,
                messages[1].id
            );
            assert_eq!(requests[0].local_claims[0].statement, "after first CAU");
        }

        let final_claim = claim_store
            .list_local_claims()
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.id == local.id)
            .unwrap();
        assert_eq!(final_claim.statement, "after second CAU");
        let second_plan: InboxEffectPlan = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &messages[1].id,
        ))
        .await
        .unwrap();
        assert_eq!(second_plan.state, InboxEffectState::Applied);
        assert!(!second_plan
            .warnings
            .iter()
            .any(|warning| warning.contains("superseded")));
    }

    #[test]
    fn cau_batch_requires_specific_policy_provenance_but_keeps_single_compatibility() {
        let first = PolicyId::random();
        let second = PolicyId::random();
        let mut missing: InternalizeOutcome = serde_json::from_value(json!({
            "new_claims": [{
                "id": ClaimId::random().as_str(),
                "name": "derived_fact",
                "statement": "derived fact",
                "scope": "tests / cau",
                "confidence": "high",
                "evidence_summary": "batch conclusion",
                "source_claim_ids": []
            }],
            "updated_claims": [],
            "new_disputes": []
        }))
        .unwrap();
        let error = ensure_cau_policy_provenance(
            &mut missing,
            &FxHashSet::from_iter([first.clone(), second.clone()]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("真正相关的本批 CAU PolicyId"));

        missing.new_claims[0].source_claim_ids = vec![second.to_string()];
        ensure_cau_policy_provenance(
            &mut missing,
            &FxHashSet::from_iter([first.clone(), second.clone()]),
        )
        .unwrap();
        assert_eq!(
            missing.new_claims[0].source_claim_ids,
            vec![second.to_string()]
        );

        missing.new_claims[0].source_claim_ids.clear();
        ensure_cau_policy_provenance(&mut missing, &FxHashSet::from_iter([first.clone()])).unwrap();
        assert_eq!(
            missing.new_claims[0].source_claim_ids,
            vec![first.to_string()]
        );
    }

    #[tokio::test]
    async fn cau_batch_persists_only_model_attributed_policy_on_changed_claim() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first_policy = PolicyId::random();
        let second_policy = PolicyId::random();
        let messages =
            [first_policy.clone(), second_policy.clone()].map(|policy_id| InboxMessage {
                id: InboxId::random(),
                kind: InboxMessageKind::ClaimAttributeUpdate {
                    policy: Policy {
                        id: policy_id,
                        message_type: PolicyMessageType::ClaimAttributeUpdate,
                        name: "batch_cau".into(),
                        statement: "batch conclusion".into(),
                        scope: "tests / cau".into(),
                        status: PolicyStatus::Active,
                        created_at: Utc::now(),
                        updated_at: None,
                        target_agents: Some(vec![agent_id.clone()]),
                    },
                    arbitration_resolution: None,
                },
                handled_at: None,
            });
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "second_policy_fact",
                    "statement": "only the second CAU caused this knowledge",
                    "scope": "tests / cau",
                    "confidence": "high",
                    "evidence_summary": "second conclusion",
                    "source_claim_ids": [second_policy.as_str()]
                }],
                "updated_claims": [],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        let runner = AgentRunner::new_local(
            agent_id,
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        runner
            .internalize_claim_attribute_update_messages(generator.as_ref(), &messages)
            .await
            .unwrap();

        assert_eq!(generator.requests.lock().unwrap().len(), 1);
        let claims = claim_store.list_local_claims().await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(
            claims[0].source_claim_ids,
            vec![SourceId::Policy(second_policy)]
        );
        assert!(!claims[0]
            .source_claim_ids
            .contains(&SourceId::Policy(first_policy)));
    }

    #[tokio::test]
    async fn claim_attribute_update_effect_journal_replays_without_second_model_call() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local = arbitration_claim("agent-a", "legacy", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "current", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy.clone());
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local).await.unwrap();
        let generator = Arc::new(CountingInboxGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": local.id.as_str(),
                    "name": local.name,
                    "statement": local.statement,
                    "scope": local.scope,
                    "confidence": "high",
                    "status": "deprecated",
                    "evidence_summary": "holder accepted the replacement resolution",
                    "source_claim_ids": [remote.id.as_str()]
                }],
                "new_disputes": []
            }),
            calls: AtomicUsize::new(0),
        });
        let runner = AgentRunner::new(
            local.holder.clone(),
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let first = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();
        let effect_path = paths::agent_home_inbox_effect_path(&agent_home, &message.id);
        let mut interrupted: InboxEffectPlan = read_yaml(&effect_path).await.unwrap();
        interrupted.state = InboxEffectState::Prepared;
        write_yaml_atomic(&effect_path, &interrupted).await.unwrap();
        let replay = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();

        assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.deprecated_claim_ids, vec![local.id.clone()]);
        assert_eq!(replay.deprecated_claim_ids, vec![local.id.clone()]);
        let updated = claim_store
            .list_local_claims()
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.id == local.id)
            .unwrap();
        assert_eq!(updated.status, ClaimStatus::Deprecated);
        assert!(updated
            .source_claim_ids
            .contains(&SourceId::Claim(remote.id.clone())));
        assert!(updated
            .source_claim_ids
            .contains(&SourceId::Policy(policy.id)));
        let effect: InboxEffectPlan = read_yaml(&effect_path).await.unwrap();
        assert_eq!(effect.state, InboxEffectState::Applied);
        assert_eq!(effect.deprecated_claim_ids, vec![local.id.clone()]);
        assert!(effect.new_disputes.is_empty());
        let serialized = tokio::fs::read_to_string(&effect_path).await.unwrap();
        assert!(!serialized.contains("batch_members"));
        assert!(!serialized.contains("batch_hash"));
    }

    #[tokio::test]
    async fn arbitration_process_report_counts_deprecated_claims() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local = arbitration_claim("agent-a", "legacy", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "current", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local).await.unwrap();
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        inbox.accept_pulled(&message).await.unwrap();
        let generator = Arc::new(CountingInboxGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": local.id.as_str(),
                    "name": local.name,
                    "statement": local.statement,
                    "scope": local.scope,
                    "confidence": "high",
                    "status": "deprecated",
                    "evidence_summary": "holder accepted the replacement resolution",
                    "source_claim_ids": [remote.id.as_str()]
                }],
                "new_disputes": []
            }),
            calls: AtomicUsize::new(0),
        });
        let runner = AgentRunner::new_local(
            local.holder.clone(),
            generator.clone(),
            claim_store,
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox,
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            0,
            Vec::<SkillSummary>::new(),
        );

        let report = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(report.total, 1);
        assert_eq!(report.claim_attribute_count, 1);
        assert_eq!(report.updated_claim_ids, vec![local.id.clone()]);
        assert_eq!(report.deprecated_claim_ids, vec![local.id]);
    }

    #[tokio::test]
    async fn arbitration_auth_failure_retries_upload_without_replaying_effect() {
        const PRIVATE_MEMORY_SENTINEL: &str = "[private] account PRIVATE_UPLOAD_SENTINEL";
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local = arbitration_claim("agent-a", "legacy", ClaimStatus::Active);
        let remote = arbitration_claim("agent-b", "current", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local).await.unwrap();
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        inbox.accept_pulled(&message).await.unwrap();
        let generator = Arc::new(CountingInboxGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": local.id.as_str(),
                    "name": local.name,
                    "statement": local.statement,
                    "scope": local.scope,
                    "confidence": "high",
                    "status": "deprecated",
                    "evidence_summary": "holder accepted the replacement resolution",
                    "source_claim_ids": [remote.id.as_str()]
                }],
                "new_disputes": []
            }),
            calls: AtomicUsize::new(0),
        });
        let maintainer = Arc::new(RecoveringAuthMaintainerClient {
            reject_claim_uploads: AtomicBool::new(true),
            uploaded_claim_ids: Mutex::new(Vec::new()),
        });
        let memory_store = Arc::new(LocalFsMemoryStore::new(
            agent_home.clone(),
            1600,
            1000,
            false,
        ));
        memory_store
            .apply_ops(&[MemoryOp::Add {
                target: MemoryTarget::Memory,
                entry: PRIVATE_MEMORY_SENTINEL.into(),
            }])
            .await
            .unwrap();
        let runner = AgentRunner::new(
            local.holder.clone(),
            generator.clone(),
            claim_store,
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox,
            memory_store,
            Arc::new(EmptyRouterClient),
            maintainer.clone(),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let first = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(first.total, 1);
        assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
        let effect: InboxEffectPlan = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap();
        assert_eq!(effect.state, InboxEffectState::Applied);
        let pending_path = paths::agent_home_pending_maintainer_uploads_path(&agent_home);
        let pending: PendingMaintainerUploads = read_yaml(&pending_path).await.unwrap();
        assert_eq!(pending.claims.len(), 1);
        assert_eq!(pending.claims[0].id, local.id);
        let effect_text = tokio::fs::read_to_string(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap();
        let pending_text = tokio::fs::read_to_string(&pending_path).await.unwrap();
        assert!(!effect_text.contains(PRIVATE_MEMORY_SENTINEL));
        assert!(!effect_text.contains("private_memory"));
        assert!(!pending_text.contains(PRIVATE_MEMORY_SENTINEL));
        assert!(!pending_text.contains("private_memory"));
        assert_eq!(
            pending.durable_claim_ids,
            std::collections::BTreeSet::from([local.id.clone()])
        );

        let still_unauthorized = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(still_unauthorized.total, 0);
        assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
        let pending: PendingMaintainerUploads = read_yaml(&pending_path).await.unwrap();
        assert_eq!(pending.claims.len(), 1);
        assert!(pending.durable_claim_ids.contains(&local.id));

        maintainer
            .reject_claim_uploads
            .store(false, Ordering::SeqCst);
        let recovered = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(recovered.total, 0);
        assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            maintainer.uploaded_claim_ids.lock().unwrap().as_slice(),
            std::slice::from_ref(&local.id)
        );
        assert!(!tokio::fs::try_exists(pending_path).await.unwrap());
    }

    #[tokio::test]
    async fn arbitration_cannot_update_another_holder_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let local = arbitration_claim("agent-a", "local", ClaimStatus::Deprecated);
        let remote = arbitration_claim("agent-b", "remote", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "structured resolution".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![local.holder.clone()]),
        };
        let message = arbitration_message(&local, &remote, policy);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&local).await.unwrap();
        let generator = Arc::new(CountingInboxGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": remote.id.as_str(),
                    "name": remote.name,
                    "statement": remote.statement,
                    "scope": remote.scope,
                    "confidence": "high",
                    "status": "deprecated",
                    "evidence_summary": "invalid remote edit",
                    "source_claim_ids": []
                }],
                "new_disputes": []
            }),
            calls: AtomicUsize::new(0),
        });
        let runner = AgentRunner::new(
            local.holder.clone(),
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let error = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("不是当前 agent 本地已有 claim"));
        assert_eq!(claim_store.list_local_claims().await.unwrap(), vec![local]);
        assert!(!tokio::fs::try_exists(paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id
        ))
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn unknown_policy_source_is_rejected_before_inbox_side_effects() {
        let unknown_policy = PolicyId::random();
        assert_invalid_inbox_output_has_no_side_effects(
            json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "invented_policy_source",
                    "statement": "不应落盘",
                    "scope": "tests / inbox",
                    "confidence": "high",
                    "evidence_summary": "引用了不可见 policy",
                    "source_claim_ids": [unknown_policy.as_str()],
                }],
                "updated_claims": [],
                "new_disputes": [],
            }),
            "不是本次 LLM 输入中可见的 PolicyId",
        )
        .await;
    }

    #[tokio::test]
    async fn unknown_claim_source_is_rejected_before_inbox_side_effects() {
        let unknown_claim = ClaimId::random();
        assert_invalid_inbox_output_has_no_side_effects(
            json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "invented_claim_source",
                    "statement": "不应落盘",
                    "scope": "tests / inbox",
                    "confidence": "high",
                    "evidence_summary": "引用了不可见 claim",
                    "source_claim_ids": [unknown_claim.as_str()],
                }],
                "updated_claims": [],
                "new_disputes": [],
            }),
            "不在本次上下文/本批新生成中",
        )
        .await;
    }

    #[tokio::test]
    async fn invented_updated_claim_id_is_rejected_before_inbox_side_effects() {
        let invented_claim = ClaimId::random();
        assert_invalid_inbox_output_has_no_side_effects(
            json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": invented_claim.as_str(),
                    "name": "invented_update",
                    "statement": "不应落盘",
                    "scope": "tests / inbox",
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "更新了不存在的本地 claim",
                    "source_claim_ids": [],
                }],
                "new_disputes": [],
            }),
            "不是当前 agent 本地已有 claim",
        )
        .await;
    }

    #[tokio::test]
    async fn local_persistence_failure_still_acks_successful_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let first = receipt_test_message(PolicyStatus::Active);
        let second = receipt_test_message(PolicyStatus::Active);
        let third = receipt_test_message(PolicyStatus::Active);
        let maintainer = Arc::new(RecordingAckMaintainerClient::new(
            vec![first.clone(), second.clone(), third],
            false,
        ));
        let inbox = Arc::new(PrefixFailingInbox::new(1));
        let runner = receipt_test_runner(
            &dir,
            inbox.clone(),
            maintainer.clone(),
            empty_receipt_generator(),
        );

        let err = runner.sync_inbox_to_local().await.unwrap_err();

        assert!(err.to_string().contains("此前已落盘并尝试 ACK 1 条"));
        assert_eq!(
            inbox.persisted.lock().unwrap().as_slice(),
            std::slice::from_ref(&first.id)
        );
        assert_eq!(inbox.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            maintainer.acked_batches.lock().unwrap().as_slice(),
            &[vec![first.id]]
        );
        assert_eq!(maintainer.upload_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn receipt_ack_failure_does_not_block_local_processing() {
        let dir = tempfile::tempdir().unwrap();
        let message = receipt_test_message(PolicyStatus::Deprecated);
        let maintainer = Arc::new(RecordingAckMaintainerClient::new(
            vec![message.clone()],
            true,
        ));
        let inbox = Arc::new(LocalFsInboxReader::new(dir.path().to_path_buf()));
        let runner = receipt_test_runner(
            &dir,
            inbox.clone(),
            maintainer.clone(),
            empty_receipt_generator(),
        );

        let report = runner.process_inbox().await.unwrap();

        assert_eq!(report.total, 1);
        assert_eq!(
            report.team_services,
            crate::agent::TeamServicesConnectionStatus {
                maintainer: TeamServiceConnectionStatus::Connected,
                router: TeamServiceConnectionStatus::Connected,
            }
        );
        assert_eq!(report.policy_deprecation_count, 1);
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("Inbox receipt ACK warning")
                && warning.contains("local inbox will continue processing")
        }));
        assert_eq!(
            maintainer.acked_batches.lock().unwrap().as_slice(),
            &[vec![message.id.clone()]]
        );
        assert!(inbox.list_pending().await.unwrap().is_empty());
        let done_path =
            paths::agent_home_inbox_dir(dir.path()).join(format!("{}.done.yaml", message.id));
        assert!(tokio::fs::try_exists(done_path).await.unwrap());
    }

    #[tokio::test]
    async fn inbox_reports_maintainer_and_router_failures_independently() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Arc::new(LocalFsInboxReader::new(dir.path().to_path_buf()));
        let maintainer_failed = receipt_test_runner(
            &dir,
            inbox.clone(),
            Arc::new(PullFailingMaintainerClient),
            empty_receipt_generator(),
        )
        .process_inbox()
        .await
        .unwrap();
        assert_eq!(
            maintainer_failed.team_services.maintainer,
            TeamServiceConnectionStatus::Failed
        );
        assert_eq!(
            maintainer_failed.team_services.router,
            TeamServiceConnectionStatus::Connected
        );
        assert!(maintainer_failed.warnings.iter().any(|warning| {
            warning == "Maintainer inbox 拉取失败，已跳过远端拉取并继续处理本地 inbox：simulated maintainer timeout"
        }));

        let router_failed = receipt_test_runner_with_router(
            &dir,
            inbox,
            Arc::new(NoopMaintainerClient),
            empty_receipt_generator(),
            Arc::new(ScopesFailingRouterClient),
        )
        .process_inbox()
        .await
        .unwrap();
        assert_eq!(
            router_failed.team_services.maintainer,
            TeamServiceConnectionStatus::Connected
        );
        assert_eq!(
            router_failed.team_services.router,
            TeamServiceConnectionStatus::Failed
        );
        assert!(router_failed.warnings.iter().any(|warning| {
            warning == "Router scope 概览获取失败：simulated router timeout"
        }));
    }

    #[tokio::test]
    async fn policy_update_rejects_dispute_that_references_same_batch_deprecation() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let existing = arbitration_claim("agent-a", "legacy_default", ClaimStatus::Active);
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "current_baseline".into(),
            statement: "use the current supported baseline".into(),
            scope: "runtime / current".into(),
            status: PolicyStatus::Active,
            created_at: Utc::now(),
            updated_at: None,
            target_agents: None,
        };
        let generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::PolicyUpdate,
            response: json!({
                "new_claims": [{
                    "id": "$new_claim_0$",
                    "name": "supported_default",
                    "statement": "use the current supported runtime",
                    "scope": "runtime / current",
                    "confidence": "high",
                    "evidence_summary": "team policy",
                    "source_claim_ids": []
                }],
                "updated_claims": [{
                    "id": existing.id.as_str(),
                    "name": existing.name,
                    "statement": existing.statement,
                    "scope": existing.scope,
                    "confidence": "high",
                    "status": "deprecated",
                    "evidence_summary": "superseded by the current baseline",
                    "source_claim_ids": []
                }],
                "new_disputes": [{
                    "id": "$new_dispute_0$",
                    "name": "redundant_baseline_conflict",
                    "claims": [existing.id.as_str(), "$new_claim_0$"],
                    "summary": "the old and current baselines disagree"
                }]
            }),
        });
        let runner = AgentRunner::new_local(
            agent_id.clone(),
            generator.clone(),
            Arc::new(LocalFsClaimStore::new(agent_home.clone())),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            0,
            Vec::<SkillSummary>::new(),
        );
        let request = InternalizeRequest {
            agent_id,
            inbox_messages: vec![InboxMessage {
                id: InboxId::random(),
                kind: InboxMessageKind::PolicyUpdate { policy },
                handled_at: None,
            }],
            local_claims: vec![existing.clone()],
        };
        let local_by_id = FxHashMap::from_iter([(existing.id.clone(), existing)]);

        let error = runner
            .internalize_and_prepare_once(
                generator.as_ref(),
                InboxInternalizeKind::PolicyUpdate,
                request,
                &local_by_id,
                None,
            )
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("最终状态为 deprecated 的 Claim"));
    }

    #[tokio::test]
    async fn policy_update_can_clear_existing_sources_and_write_trace() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        let original = Claim {
            id: ClaimId::random(),
            name: "batch_limit".into(),
            statement: "批处理每批最多 100 条".into(),
            scope: "orders / batch".into(),
            holder: agent_id.clone(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Stale,
            created_at: crate::time::now_seconds() - chrono::Duration::days(10),
            updated_at: None,
            source_claim_ids: vec![
                SourceId::Policy(PolicyId::random()),
                SourceId::Claim(ClaimId::random()),
            ],
            evidence_summary: "旧规则".into(),
        };
        claim_store.write_claim(&original).await.unwrap();

        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "batch_limit_update".into(),
            statement: "批处理每批最多 50 条".into(),
            scope: "orders / batch".into(),
            status: PolicyStatus::Active,
            created_at: Utc::now(),
            updated_at: None,
            target_agents: None,
        };
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        inbox
            .accept_pulled(&InboxMessage {
                id: InboxId::random(),
                kind: InboxMessageKind::PolicyUpdate {
                    policy: policy.clone(),
                },
                handled_at: None,
            })
            .await
            .unwrap();

        let generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::PolicyUpdate,
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": original.id.as_str(),
                    "name": "batch_limit",
                    "statement": "批处理每批最多 50 条",
                    "scope": "orders / batch",
                    "confidence": "high",
                    "status": "active",
                    "evidence_summary": "依据新的团队 policy 收紧批处理上限",
                    "source_claim_ids": [],
                }],
                "new_disputes": [],
            }),
        });
        let runner = AgentRunner::new(
            agent_id,
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            inbox.clone(),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );

        let report = runner.process_inbox_with(generator.as_ref()).await.unwrap();

        assert_eq!(report.updated_claim_ids, vec![original.id.clone()]);
        assert!(report.new_claim_ids.is_empty());
        assert_eq!(report.trace_ids.len(), 1);

        let updated = claim_store
            .list_local_claims()
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.id == original.id)
            .unwrap();
        assert_eq!(updated.statement, "批处理每批最多 50 条");
        assert_eq!(updated.confidence, Confidence::High);
        assert_eq!(updated.status, ClaimStatus::Active);
        assert_eq!(updated.created_at, original.created_at);
        assert!(updated.updated_at.is_some());
        assert!(updated.updated_at.unwrap() > updated.created_at);
        assert_eq!(updated.holder, original.holder);
        assert!(updated.source_claim_ids.is_empty());

        let trace_path =
            paths::agent_home_traces_dir(&agent_home).join(format!("{}.yaml", report.trace_ids[0]));
        let trace: Trace = read_yaml(&trace_path).await.unwrap();
        assert_eq!(trace.name, "inbox_policy_internalization");
        assert!(trace
            .input_claims
            .contains(&SourceId::Policy(policy.id.clone())));
        assert!(trace
            .input_claims
            .contains(&SourceId::Claim(original.id.clone())));
        assert_eq!(trace.output_claims, vec![original.id]);
    }

    #[tokio::test]
    async fn claim_attribute_update_accepts_visible_historical_sources_without_current_policy() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let historical_policy_id = PolicyId::random();
        let historical_claim_id = ClaimId::random();
        let existing = Claim {
            id: ClaimId::random(),
            name: "legacy_rule".into(),
            statement: "旧规则".into(),
            scope: "orders / prod".into(),
            holder: agent_id.clone(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: "2026-04-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![
                SourceId::Policy(historical_policy_id.clone()),
                SourceId::Claim(historical_claim_id.clone()),
            ],
            evidence_summary: "旧证据".into(),
        };
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "deprecate_legacy_rule".into(),
            statement: "建议将旧规则标记为 deprecated".into(),
            scope: "orders / prod".into(),
            status: PolicyStatus::Active,
            created_at: "2026-05-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![agent_id.clone()]),
        };
        let policy_id = policy.id.clone();
        let message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy,
                arbitration_resolution: None,
            },
            handled_at: None,
        };
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [{
                    "id": existing.id.as_str(),
                    "name": "legacy_rule",
                    "statement": "旧规则已不再适用",
                    "scope": "orders / prod",
                    "confidence": "high",
                    "status": "deprecated",
                    "evidence_summary": "依据 maintainer 建议和当前证据",
                    "source_claim_ids": [
                        historical_policy_id.as_str(),
                        historical_claim_id.as_str()
                    ]
                }],
                "new_disputes": []
            }),
            requests: Mutex::new(Vec::new()),
        });
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&existing).await.unwrap();
        let runner = AgentRunner::new(
            agent_id.clone(),
            generator.clone(),
            claim_store.clone(),
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone())),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );
        let summary = runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();

        assert!(summary.new_claim_ids.is_empty());
        assert!(summary.new_dispute_ids.is_empty());
        assert_eq!(summary.updated_claim_ids, vec![existing.id.clone()]);
        let request = generator.requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.agent_id, agent_id);
        assert_eq!(request.claim_attribute_updates.len(), 1);
        let item = &request.claim_attribute_updates[0];
        assert_eq!(item.claim_attribute_update, message);
        assert_eq!(item.conclusion, "建议将旧规则标记为 deprecated");
        assert!(item.resolution.is_none());
        assert!(item.dispute.is_none());
        assert_eq!(request.local_claims, vec![existing.clone()]);
        assert!(item.direct_claims.is_empty());

        let updated = claim_store
            .list_local_claims()
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.id == existing.id)
            .unwrap();
        assert_eq!(updated.status, ClaimStatus::Deprecated);
        assert!(updated.updated_at.is_some());
        assert_eq!(
            updated.source_claim_ids,
            vec![
                SourceId::Policy(historical_policy_id),
                SourceId::Claim(historical_claim_id),
                SourceId::Policy(policy_id),
            ]
        );
        let plan: InboxEffectPlan = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &message.id,
        ))
        .await
        .unwrap();
        assert_eq!(plan.state, InboxEffectState::Applied);
        assert_eq!(plan.resolution_id, None);
    }

    #[tokio::test]
    async fn claim_attribute_update_filters_duplicate_claim_sets_before_upload() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first_claim = arbitration_claim("agent-a", "first", ClaimStatus::Active);
        let second_claim = arbitration_claim("agent-a", "second", ClaimStatus::Active);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&first_claim).await.unwrap();
        claim_store.write_claim(&second_claim).await.unwrap();
        let maintainer = Arc::new(PayloadRecordingMaintainerClient::default());
        let runner = AgentRunner::new(
            agent_id.clone(),
            empty_receipt_generator(),
            claim_store,
            Arc::new(PendingBeforeLedgerReportedDisputeStore::new(
                agent_home.clone(),
            )),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            maintainer.clone(),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "review_claim_conflict".into(),
            statement: "review the conflicting local knowledge".into(),
            scope: "knowledge / shared".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![agent_id]),
        };
        let first_message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: policy.clone(),
                arbitration_resolution: None,
            },
            handled_at: None,
        };
        let second_message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy,
                arbitration_resolution: None,
            },
            handled_at: None,
        };
        let third_message = InboxMessage {
            id: InboxId::random(),
            kind: second_message.kind.clone(),
            handled_at: None,
        };
        let first_generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [
                    {
                        "id": "$new_dispute_0$",
                        "name": "first_report",
                        "claims": [first_claim.id.as_str(), second_claim.id.as_str()],
                        "summary": "the local claims conflict"
                    },
                    {
                        "id": "$new_dispute_1$",
                        "name": "same_effect_duplicate",
                        "claims": [second_claim.id.as_str(), first_claim.id.as_str()],
                        "summary": "the same output repeated the claim set"
                    }
                ]
            }),
            requests: Mutex::new(Vec::new()),
        });
        let second_generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [{
                    "id": "$new_dispute_0$",
                    "name": "duplicate_report",
                    "claims": [second_claim.id.as_str(), first_claim.id.as_str()],
                    "summary": "the same local claims still conflict"
                }]
            }),
            requests: Mutex::new(Vec::new()),
        });

        let first = runner
            .internalize_claim_attribute_update_messages(
                first_generator.as_ref(),
                &[first_message.clone(), second_message.clone()],
            )
            .await
            .unwrap();
        let second = runner
            .internalize_claim_attribute_update_message(second_generator.as_ref(), &third_message)
            .await
            .unwrap();

        assert_eq!(first.new_dispute_ids.len(), 1);
        assert!(second.new_dispute_ids.is_empty());
        {
            let uploaded = maintainer.disputes.lock().unwrap();
            assert_eq!(uploaded.len(), 1);
            assert_eq!(uploaded[0].id, first.new_dispute_ids[0]);
        }
        let batch_ref: InboxEffectRef = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &second_message.id,
        ))
        .await
        .unwrap();
        assert_eq!(batch_ref.canonical_inbox_id, first_message.id);
        let second_plan: InboxEffectPlan = read_yaml(&paths::agent_home_inbox_effect_path(
            &agent_home,
            &third_message.id,
        ))
        .await
        .unwrap();
        assert_eq!(second_plan.state, InboxEffectState::Applied);
        assert!(second_plan.new_disputes.is_empty());
    }

    #[tokio::test]
    async fn solo_cau_does_not_record_unstaged_dispute_as_reported() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first_claim = arbitration_claim("agent-a", "first", ClaimStatus::Active);
        let second_claim = arbitration_claim("agent-a", "second", ClaimStatus::Active);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&first_claim).await.unwrap();
        claim_store.write_claim(&second_claim).await.unwrap();
        let reported_store = Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone()));
        let generator = Arc::new(RecordingClaimAttributeUpdateGenerator {
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [{
                    "id": "$new_dispute_0$",
                    "name": "solo_conflict",
                    "claims": [first_claim.id.as_str(), second_claim.id.as_str()],
                    "summary": "the local claims conflict"
                }]
            }),
            requests: Mutex::new(Vec::new()),
        });
        let runner = AgentRunner::new_local(
            agent_id.clone(),
            generator.clone(),
            claim_store,
            reported_store.clone(),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone())),
            0,
            Vec::<SkillSummary>::new(),
        );
        let message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: Policy {
                    id: PolicyId::random(),
                    message_type: PolicyMessageType::ClaimAttributeUpdate,
                    name: "review_claim_conflict".into(),
                    statement: "review the conflicting local knowledge".into(),
                    scope: "knowledge / shared".into(),
                    status: PolicyStatus::Active,
                    created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
                    updated_at: None,
                    target_agents: Some(vec![agent_id]),
                },
                arbitration_resolution: None,
            },
            handled_at: None,
        };

        runner
            .internalize_claim_attribute_update_message(generator.as_ref(), &message)
            .await
            .unwrap();

        assert!(!reported_store
            .contains_claim_set(&[first_claim.id, second_claim.id])
            .await
            .unwrap());
        assert!(
            !tokio::fs::try_exists(paths::agent_home_pending_maintainer_uploads_path(
                &agent_home
            ))
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn policy_update_filters_duplicate_claim_sets_before_upload() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first_claim = arbitration_claim("agent-a", "first", ClaimStatus::Active);
        let second_claim = arbitration_claim("agent-a", "second", ClaimStatus::Active);
        let claim_store = Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        claim_store.write_claim(&first_claim).await.unwrap();
        claim_store.write_claim(&second_claim).await.unwrap();
        let maintainer = Arc::new(PayloadRecordingMaintainerClient::default());
        let generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::PolicyUpdate,
            response: json!({
                "new_claims": [],
                "updated_claims": [],
                "new_disputes": [
                    {
                        "id": "$new_dispute_0$",
                        "name": "first_report",
                        "claims": [first_claim.id.as_str(), second_claim.id.as_str()],
                        "summary": "the local claims conflict"
                    },
                    {
                        "id": "$new_dispute_1$",
                        "name": "same_batch_duplicate",
                        "claims": [second_claim.id.as_str(), first_claim.id.as_str()],
                        "summary": "the same batch repeated the claim set"
                    }
                ]
            }),
        });
        let runner = AgentRunner::new(
            agent_id.clone(),
            generator.clone(),
            claim_store,
            Arc::new(PendingBeforeLedgerReportedDisputeStore::new(
                agent_home.clone(),
            )),
            Arc::new(LocalFsInboxReader::new(agent_home.clone())),
            Arc::new(LocalFsMemoryStore::new(
                agent_home.clone(),
                1600,
                1000,
                false,
            )),
            Arc::new(EmptyRouterClient),
            maintainer.clone(),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            0,
            Vec::<SkillSummary>::new(),
        );
        let message = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::PolicyUpdate {
                policy: Policy {
                    id: PolicyId::random(),
                    message_type: PolicyMessageType::PolicyUpdate,
                    name: "review_claim_conflict".into(),
                    statement: "review the conflicting local knowledge".into(),
                    scope: "knowledge / shared".into(),
                    status: PolicyStatus::Active,
                    created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
                    updated_at: None,
                    target_agents: Some(vec![agent_id]),
                },
            },
            handled_at: None,
        };

        let summary = runner
            .internalize_inbox_updates(
                generator.as_ref(),
                InboxInternalizeKind::PolicyUpdate,
                vec![message],
            )
            .await
            .unwrap();

        assert_eq!(summary.new_dispute_ids.len(), 1);
        assert_eq!(maintainer.disputes.lock().unwrap().len(), 1);
    }
}
