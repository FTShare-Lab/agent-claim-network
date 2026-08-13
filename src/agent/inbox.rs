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

use super::prepare::{
    llm_visible_claims, prepare_claim_updates, prepare_claims, prepare_disputes, sorted_source_ids,
    validate_visible_policy_sources,
};
use super::runner::{AgentRunner, InboxProcessReport, TeamServiceConnectionStatus};
use super::traits::ClaimedInboxMessage;
use crate::api::{
    resolve_placeholders, BufferedProviderRuntime, InboxInternalizeKind, InternalizeOutcome,
    InternalizeRequest, ProviderRuntimeFallbackScope, SessionTurnMessage, StructuredJsonCaller,
};
use crate::claim::{
    Claim, ClaimId, ClaimStatus, Dispute, DisputeId, InboxId, InboxMessage, InboxMessageKind,
    Policy, PolicyId, PolicyStatus, SourceId, TraceId,
};
use crate::maintainer::traits::MaintainerClientError;
use crate::prompt::PromptRegistry;
use crate::tracing::tracer;

type PreparedInternalization = (DateTime<Utc>, Vec<Claim>, Vec<Claim>, Vec<Dispute>);

#[async_trait]
pub(crate) trait InboxJsonGenerator: Send + Sync {
    async fn generate_json(
        &self,
        kind: InboxInternalizeKind,
        request: InternalizeRequest,
    ) -> anyhow::Result<serde_json::Value>;
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

impl AgentRunner {
    /// 排空 inbox 中所有 pending 消息。
    ///
    /// 处理纪律：
    /// - 连续同类型的 `PolicyUpdate` / `ClaimAttributeUpdate` 收集成 batch 后交给 LLM；
    ///   一旦遇到其它消息类型，先 flush 已缓冲 batch，保证 inbox 事件顺序不被重排
    /// - 单次最多处理 1024 条，避免极端 inbox 堆积让一次 session 卡太久
    pub async fn process_inbox(&self) -> anyhow::Result<InboxProcessReport> {
        self.process_inbox_with(self.inbox_generator.as_ref()).await
    }

    pub(super) async fn process_inbox_with(
        &self,
        generator: &dyn InboxJsonGenerator,
    ) -> anyhow::Result<InboxProcessReport> {
        let _guard = self.inbox_process_lock.lock().await;
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
        let summary = self
            .internalize_inbox_updates(generator, kind, inbox_messages)
            .await?;
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
        report.new_dispute_ids.extend(summary.new_dispute_ids);
        report.warnings.extend(summary.warnings);
        Ok(())
    }

    async fn apply_policy_deprecation(
        &self,
        policy: &Policy,
    ) -> anyhow::Result<PolicyDeprecationSummary> {
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
        let upload_report = self
            .upload_maintainer_batch(claims_to_upload, Vec::new())
            .await?;
        let mut warnings = Vec::new();
        push_upload_warning(&mut warnings, upload_report);

        if deprecated_claim_ids.is_empty() {
            return Ok(PolicyDeprecationSummary {
                warnings,
                ..PolicyDeprecationSummary::default()
            });
        }

        deprecated_claim_ids.sort();
        let trace_id = self
            .write_trace(
                "policy_deprecation_internalization".into(),
                format!("policy {} deprecated", policy.id),
                vec![policy_source],
                deprecated_claim_ids.clone(),
                now,
            )
            .await?;
        log::info!(
            target: "agent",
            "agent {} 处理 deprecated policy id={} → deprecated claims={:?}",
            self.agent_id,
            policy.id,
            deprecated_claim_ids
        );

        Ok(PolicyDeprecationSummary {
            trace_id: Some(trace_id),
            deprecated_claim_ids,
            warnings,
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
        let (now, prepared_claims, prepared_updates, prepared_disputes) = {
            let mut prepared = None;
            for attempt in 0..=self.llm_retry_count {
                match self
                    .internalize_and_prepare_once(generator, kind, request.clone(), &local_by_id)
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
                        log::warn!(
                            target: "agent",
                            "agent {} internalize_inbox 输出未通过协议校验，重试 ({}/{}): {e:#}",
                            self.agent_id,
                            attempt + 1,
                            self.llm_retry_count
                        );
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
        for claim in prepared_updates {
            trace_input_sources.extend(claim.source_claim_ids.iter().cloned());
            trace_input_sources.insert(SourceId::Claim(claim.id.clone()));
            self.claim_store.write_claim(&claim).await?;
            log::info!(
                target: "agent",
                "agent {} 内化 {} → 更新 claim id={} name={} scope={}",
                self.agent_id, kind_label, claim.id, claim.name, claim.scope
            );
            updated_claim_ids.push(claim.id.clone());
            claims_to_upload.push(claim);
        }

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
            for dispute in prepared_disputes {
                if self.dispute_claim_set_reported(&dispute).await? {
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
        let upload_report = self
            .upload_maintainer_batch(claims_to_upload, disputes_to_upload.clone())
            .await?;
        for dispute in disputes_to_upload {
            if self.record_dispute_if_new(&dispute).await? {
                written_dispute_ids.push(dispute.id.clone());
            }
        }
        let mut warnings = Vec::new();
        push_upload_warning(&mut warnings, upload_report);

        span.end();
        Ok(InternalizeSummary {
            trace_id,
            new_claim_ids: written_claim_ids,
            updated_claim_ids,
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
        let raw = generator.generate_json(kind, request).await?;
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
            &allowed_source_claim_ids,
            &self.agent_id,
            now,
        )?;
        let prepared_updates = prepare_claim_updates(
            outcome.updated_claims,
            local_by_id,
            &allowed_source_claim_ids,
            now,
        )?;
        let prepared_disputes = prepare_disputes(
            outcome.new_disputes,
            &allowed_dispute_claim_ids,
            &self.agent_id,
            now,
        )?;
        Ok((now, prepared_claims, prepared_updates, prepared_disputes))
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

struct InternalizeSummary {
    trace_id: Option<TraceId>,
    new_claim_ids: Vec<ClaimId>,
    updated_claim_ids: Vec<ClaimId>,
    new_dispute_ids: Vec<DisputeId>,
    warnings: Vec<String>,
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::{json, Value};

    use super::*;
    use crate::agent::fs::{
        LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore,
        LocalFsReportedDisputeClaimSetStore,
    };
    use crate::agent::maintainer_upload::LocalFsMaintainerUploadQueue;
    use crate::agent::traits::{InboxReader, LocalClaimStore};
    use crate::claim::{
        AgentId, ClaimStatus, Confidence, InboxId, InboxMessageKind, Policy, PolicyId,
        PolicyMessageType, PolicyStatus, SourceId, Trace,
    };
    use crate::maintainer::traits::MaintainerClient;
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

    #[async_trait]
    impl InboxJsonGenerator for StaticInboxGenerator {
        async fn generate_json(
            &self,
            kind: InboxInternalizeKind,
            _request: InternalizeRequest,
        ) -> anyhow::Result<Value> {
            assert_eq!(kind, self.expected_kind);
            Ok(self.response.clone())
        }
    }

    struct RetryRecordingInboxGenerator {
        responses: Mutex<VecDeque<Value>>,
        requests: Mutex<Vec<InternalizeRequest>>,
    }

    #[async_trait]
    impl InboxJsonGenerator for RetryRecordingInboxGenerator {
        async fn generate_json(
            &self,
            _kind: InboxInternalizeKind,
            request: InternalizeRequest,
        ) -> anyhow::Result<Value> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake inbox response"))
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
    async fn business_retry_resends_unchanged_request() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().to_path_buf();
        let inbox = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        let message = receipt_test_message(PolicyStatus::Active);
        inbox.accept_pulled(&message).await.unwrap();
        let generator = Arc::new(RetryRecordingInboxGenerator {
            responses: Mutex::new(VecDeque::from([
                json!({
                    "new_claims": [{"id":"$new_claim_0$"}],
                    "updated_claims": [],
                    "new_disputes": [],
                }),
                json!({
                    "new_claims": [],
                    "updated_claims": [],
                    "new_disputes": [],
                }),
            ])),
            requests: Mutex::new(Vec::new()),
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
        let generator = Arc::new(StaticInboxGenerator {
            expected_kind: InboxInternalizeKind::ClaimAttributeUpdate,
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
        });
        let runner = AgentRunner::new(
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
            Arc::new(EmptyRouterClient),
            Arc::new(NoopMaintainerClient),
            Arc::new(LocalFsMaintainerUploadQueue::new(agent_home)),
            0,
            Vec::<SkillSummary>::new(),
        );
        let request = InternalizeRequest {
            agent_id,
            inbox_messages: vec![InboxMessage {
                id: InboxId::random(),
                kind: InboxMessageKind::ClaimAttributeUpdate { policy },
                handled_at: None,
            }],
            local_claims: vec![existing.clone()],
        };
        let local_by_id = FxHashMap::from_iter([(existing.id.clone(), existing.clone())]);

        let (_, new_claims, updates, disputes) = runner
            .internalize_and_prepare_once(
                generator.as_ref(),
                InboxInternalizeKind::ClaimAttributeUpdate,
                request,
                &local_by_id,
            )
            .await
            .unwrap();

        assert!(new_claims.is_empty());
        assert!(disputes.is_empty());
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].id, existing.id);
        assert_eq!(updates[0].status, ClaimStatus::Deprecated);
        assert!(updates[0].updated_at.is_some());
        assert_eq!(
            updates[0].source_claim_ids,
            vec![
                SourceId::Policy(historical_policy_id),
                SourceId::Claim(historical_claim_id)
            ]
        );
    }
}
