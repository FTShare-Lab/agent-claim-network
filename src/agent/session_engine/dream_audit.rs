//! Dream 人工审计视图；只投影探索和 Prepared 状态，不参与 Claim 提交决策。
use crate::storage::{write_text_atomic, write_text_atomic_if_unchanged, FileLockGuard};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(super) struct Audit {
    root: PathBuf,
    fingerprint: Option<String>,
}
impl Audit {
    pub(super) fn new(home: &Path, job_id: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !job_id.is_empty()
                && job_id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_')),
            "Invalid Dream job id"
        );
        Ok(Self {
            root: home.join("dream").join(job_id),
            fingerprint: None,
        })
    }
    pub(super) fn with_fingerprint(mut self, fingerprint: String) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }
    pub(super) fn history_root(&self) -> PathBuf {
        self.root
            .join("history")
            .join(self.fingerprint.as_deref().unwrap_or("legacy"))
    }
    async fn json(&self, path: &Path, value: &Value) -> anyhow::Result<()> {
        write_text_atomic(path, &serde_json::to_vec_pretty(value)?).await?;
        Ok(())
    }
    pub(super) async fn input(&self, fingerprint: &str, input: &Value) -> anyhow::Result<()> {
        let value = json!({"fingerprint":fingerprint,"input":input});
        let bytes = serde_json::to_vec_pretty(&value)?;
        // 首次输入不覆盖；重试因输入或证据变化重启时保留每次输入。
        write_text_atomic_if_unchanged(&self.root.join("input.json"), &bytes, None).await?;
        write_text_atomic_if_unchanged(
            &self.root.join("inputs").join(format!("{fingerprint}.json")),
            &bytes,
            None,
        )
        .await?;
        Ok(())
    }
    pub(super) async fn event(&self, value: Value) -> anyhow::Result<()> {
        let _guard = FileLockGuard::lock_exclusive(&self.root.join("audit.lock")).await?;
        let revisions = self.root.join("revisions");
        tokio::fs::create_dir_all(&revisions).await?;
        let mut entries = tokio::fs::read_dir(&revisions).await?;
        let mut next = 1u64;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(n) = entry
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                next = next.max(n.saturating_add(1));
            }
        }
        let event = json!({"at":chrono::Utc::now(),"revision":next,"input_fingerprint":self.fingerprint,"event":value});
        let written = write_text_atomic_if_unchanged(
            &revisions.join(format!("{next:06}.json")),
            &serde_json::to_vec_pretty(&event)?,
            None,
        )
        .await?;
        anyhow::ensure!(written, "Dream audit revision already exists");
        let mut note = format!("# Dream 修订 {next}\n\n动作：{}；组：{}。\n\n宿主接受：{}（接受草稿不代表已写 Claim）。\n\n", display(&value["tool"]), display(&value["input"]["group_id"]), display(&value["feedback"]["accepted"]));
        if value["kind"] == "context_compacted" {
            note = format!("# Dream 上下文压缩 {next}\n\n消息数量：{} → {}。\n\n完整旧历史：`history/{}/{}.json`（JSON 快照）。旧工具历史未删除；摘要不构成新的证据或复核批准。\n\n",
                display(&value["before_messages"]), display(&value["after_messages"]),
                self.fingerprint.as_deref().unwrap_or("legacy"), display(&value["archive_id"]));
            for (field, label) in [
                ("checked", "已检查"),
                ("findings", "探索发现"),
                ("uncertainties", "不确定性"),
                ("next_steps", "下一步"),
            ] {
                note.push_str(&format!(
                    "## {label}\n\n{}\n\n",
                    display(&value["summary"][field])
                ));
            }
        }
        if let Some(error) = value["feedback"]["error"]
            .as_str()
            .or_else(|| value["error"].as_str())
            .or_else(|| value["feedback"]["note"].as_str())
        {
            note.push_str(&format!("反馈：{error}\n\n"));
        }
        if value["tool"] == "dream_stage_group" {
            if value["input"]["group"].is_null() {
                note.push_str(
                    "本次操作撤回该组；原 Claim 保留。是否实际移除组见 JSON 的 removed。\n",
                );
            } else {
                note.push_str(&format!(
                    "修改理由：{}\n\n",
                    display(&value["input"]["group"]["reason"])
                ));
                for operation in value["input"]["group"]["operations"]
                    .as_array()
                    .into_iter()
                    .flatten()
                {
                    note.push_str(&format!(
                        "## {}：{}\n\n",
                        display(&operation["id"]),
                        display(&operation["action"])
                    ));
                    for (field, changed) in operation["changes"]
                        .as_object()
                        .into_iter()
                        .flat_map(|m| m.iter())
                    {
                        note.push_str(&format!("- **{field}**：{}\n", display(changed)));
                    }
                    note.push('\n');
                }
            }
        }
        if let Some(progress) = value.pointer("/feedback/draft_state/candidate_progress") {
            note.push_str("\n## 本轮候选处理进度\n\n");
            for candidate in progress["pending"].as_array().into_iter().flatten() {
                note.push_str(&format!(
                    "- 待处理 `{}`：{}；涉及 {}。\n",
                    display(&candidate["group_id"]),
                    display(&candidate["reason"]),
                    candidate["claim_ids"]
                ));
            }
            for entry in progress["handled"].as_array().into_iter().flatten() {
                note.push_str(&format!(
                    "- 已处理 `{}`：{}；{}。\n",
                    display(&entry["candidate"]["group_id"]),
                    display(&entry["outcome"]["status"]),
                    display(&entry["outcome"]["reason"])
                ));
            }
            note.push_str("\n候选记录仅跟踪本轮工作；保留理由由模型给出，不构成宿主的语义批准。\n");
        }
        note.push_str(&format!("\n[完整输入、反馈、草稿与证据]({next:06}.json)\n"));
        anyhow::ensure!(
            write_text_atomic_if_unchanged(
                &revisions.join(format!("{next:06}.md")),
                note.as_bytes(),
                None
            )
            .await?,
            "Dream audit text revision already exists"
        );
        self.json(&self.root.join("latest.json"), &event).await?;
        if let Some(receipts) = value.get("receipts") {
            self.json(&self.root.join("evidence.json"), receipts)
                .await?;
        }
        if !tokio::fs::try_exists(self.root.join("result.json")).await?
            && !tokio::fs::try_exists(self.root.join("executions")).await?
        {
            let report = format!("# Dream 运行记录\n\n当前仍为草稿，尚未进入 Claim 提交。最新记录：[修订 {next}](revisions/{next:06}.md)。\n\n每份修订记录保留工具输入、宿主反馈、当时草稿及证据。input.json 是首次输入；inputs/ 保存重启时的新输入。\n\n审计保留的是提交依据和简要复核结论，不是模型内部思考。后端校验不能证明语义正确。\n");
            write_text_atomic(&self.root.join("report.md"), report.as_bytes()).await?;
        }
        Ok(())
    }
    pub(super) async fn result(&self, checkpoint: Value) -> anyhow::Result<()> {
        self.json(&self.root.join("result.json"), &checkpoint)
            .await?;
        let applied = checkpoint["applied"].as_bool().unwrap_or(false);
        let mut report = format!("# Dream 运行记录\n\n状态：{}。实际写入版本见 result.json 的 self_versions。\n\n## 修改前后\n", if applied {"提交阶段完成（冲突组可能跳过）"} else {"已进入 Prepared，提交可能尚未完成"});
        if let Some(count) = checkpoint["exploration_receipts"].as_u64() {
            let changed = checkpoint["self_versions"]
                .as_object()
                .map_or(0, |v| v.len());
            report.push_str(&format!("\n实际修改 {changed} 条；当前上下文保留 {count} 份工具读取/运行记录。记录数量不代表查证通过；0 份也可能是工具未成功返回。\n"));
        }
        for (field, label) in [
            ("quality", "A 质量清理"),
            ("evidence", "B 证据校准"),
            ("consolidation", "C 主题整合"),
        ] {
            report.push_str(&format!(
                "\n{label}（模型总结）：{}\n",
                display(&checkpoint["plan"]["review"][field])
            ));
        }
        for (index, group) in checkpoint["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            report.push_str(&format!(
                "\n### 组 {}：{}\n\n",
                index + 1,
                if group["skipped"] == true {
                    "版本冲突，停止本组剩余写入"
                } else if group["completed"] == true {
                    "已提交"
                } else {
                    "待提交或恢复"
                }
            ));
            if let Some(plan) = checkpoint["plan"]["groups"].get(index) {
                report.push_str(&format!(
                    "原因：{}\n\n",
                    plan["reason"].as_str().unwrap_or_default()
                ));
            }
            for (before, after) in group["before"]
                .as_array()
                .into_iter()
                .flatten()
                .zip(group["after"].as_array().into_iter().flatten())
            {
                report.push_str(&format!(
                    "#### {} — {}\n\n",
                    before["id"].as_str().unwrap_or_default(),
                    before["name"].as_str().unwrap_or_default()
                ));
                for field in [
                    "name",
                    "statement",
                    "scope",
                    "confidence",
                    "status",
                    "source_claim_ids",
                    "evidence_summary",
                ] {
                    if before[field] != after[field] {
                        report.push_str(&format!(
                            "- **{field}**\n  - 原文：{}\n  - 计划：{}\n",
                            display(&before[field]),
                            display(&after[field])
                        ));
                    }
                }
                let written = checkpoint["self_versions"]
                    .get(before["id"].as_str().unwrap_or_default())
                    .is_some();
                report.push_str(if written {
                    "\n实际结果：目标版本已在本地保存，已执行现有同步暂存流程（不表示远端上传成功）。\n"
                } else {
                    "\n实际结果：未记录目标版本写入；以完成状态及恢复后的 result.json 为准。\n"
                });
            }
        }
        report.push_str("\n## 复核与证据\n\n逐组执行的 executions/ 保存每次修改前后、简要语义复核、版本化证据和实际结果；旧任务的 self_reviews 保存全文映射。revisions/ 保留每次暂存、校验、提交尝试和撤回，包括失败反馈。没有写入的计划不代表 Claim 已改变。后端检查引用、覆盖和版本，不能证明语义等价或事实正确。\n");
        write_text_atomic(&self.root.join("report.md"), report.as_bytes()).await?;
        Ok(())
    }

    pub(super) async fn checkpoint(&self, value: Value) -> anyhow::Result<()> {
        if value["operation"].is_null() {
            return self.result(value).await;
        }
        let id = value["operation"]["validation_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Execution has no validation id"))?;
        let root = self.root.join("executions");
        self.json(&root.join(format!("{id}.json")), &value).await?;
        let mut report = format!("# Dream 单组执行记录\n\n组：{}\n\n提交阶段完成：{}\n\n实际写入版本：`{}`\n\n## 修改与依据\n\n", display(&value["operation"]["group_id"]), display(&value["applied"]),display(&value["self_versions"]));
        for group in value["groups"].as_array().into_iter().flatten() {
            report.push_str(&format!("版本冲突：{}\n\n", display(&group["skipped"])));
            for (before, after) in group["before"]
                .as_array()
                .into_iter()
                .flatten()
                .zip(group["after"].as_array().into_iter().flatten())
            {
                report.push_str(&format!(
                    "### {}\n\n原文：{}\n\n目标：{}\n\n状态：{} → {}\n\n",
                    display(&before["id"]),
                    display(&before["statement"]),
                    display(&after["statement"]),
                    display(&before["status"]),
                    display(&after["status"])
                ));
            }
        }
        report.push_str(&format!("## 语义复核\n\n```json\n{}\n```\n\n完整 before/after、证据回执、时间和实际结果见 [{id}.json]({id}.json)。本地写入不表示远端同步成功。\n",serde_json::to_string_pretty(&value["operation"]["review"])?));
        write_text_atomic(&root.join(format!("{id}.md")), report.as_bytes()).await?;
        write_text_atomic(&self.root.join("report.md"), "# Dream 运行记录\n\n正在逐组执行。executions/ 保存每次修改前后、语义复核和实际结果。即使本轮尚未结束或中途失败，也可能已有修改生效。\n".as_bytes()).await?;
        Ok(())
    }
}
fn display(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
        .replace('\n', " ")
}
