//! Agent 级 Dream 入队、定时检查和恢复，不创建伪会话。
use super::*;

const CHECK_INTERVAL: Duration = Duration::from_secs(4 * 3600);
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Registration {
    workspace: PathBuf,
    last_check_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DreamCheckReport {
    pub job_id: Option<String>,
    pub message: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DreamCompletionSummary {
    pub changed_claims: usize,
    pub exploration_receipts: usize,
}

/// 从完成后的审计记录读取实际统计；旧记录或跳过的任务不伪报为零。
pub(crate) async fn dream_completion_summary(
    home: &Path,
    job_id: &str,
) -> anyhow::Result<Option<DreamCompletionSummary>> {
    anyhow::ensure!(
        !job_id.is_empty()
            && job_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_')),
        "Invalid Dream job id"
    );
    let bytes = match tokio::fs::read(home.join("dream").join(job_id).join("result.json")).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    #[derive(Deserialize)]
    struct Record {
        applied: bool,
        self_versions: std::collections::BTreeMap<String, String>,
        exploration_receipts: Option<usize>,
    }
    let record: Record = serde_json::from_slice(&bytes)?;
    Ok(record
        .exploration_receipts
        .filter(|_| record.applied)
        .map(|count| DreamCompletionSummary {
            changed_claims: record.self_versions.len(),
            exploration_receipts: count,
        }))
}

pub async fn check_dream(
    config: &SupervisorLaunchConfig,
    workspace: PathBuf,
    manual: bool,
) -> anyhow::Result<DreamCheckReport> {
    ensure_supervisor_running(config).await?;
    match send_request(
        &config.paths(),
        SupervisorRequest::CheckDream { workspace, manual },
    )
    .await?
    {
        SupervisorResponse::DreamChecked { job_id, message } => {
            Ok(DreamCheckReport { job_id, message })
        }
        SupervisorResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("unexpected Dream response: {other:?}"),
    }
}

pub(crate) async fn dream_has_priority_work(jobs_dir: &Path) -> anyhow::Result<bool> {
    let mut entries = match fs::read_dir(jobs_dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension().is_none_or(|x| x != "yaml") {
            continue;
        }
        // Enqueue 先原子申领空占位，发布窗口内下一边界再检查。
        let Ok(job) = read_yaml::<SupervisorJob>(&entry.path()).await else {
            continue;
        };
        if entry.path().file_stem().and_then(|s| s.to_str()) != Some(job.id.as_str()) {
            continue;
        }
        if job.status == SupervisorJobStatus::Queued
            && !matches!(job.kind, SupervisorJobKind::Dream { .. })
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn enqueue_check(
    paths: &SupervisorPaths,
    shared: &SupervisorSharedState,
    workspace: PathBuf,
    manual: bool,
) -> anyhow::Result<DreamCheckReport> {
    anyhow::ensure!(
        !shared.stopping.load(Ordering::Acquire) && !shared.stop_requested.is_cancelled(),
        SUPERVISOR_STOPPING_MESSAGE
    );
    let engine = shared
        .dream_engine
        .as_ref()
        .context("Dream runtime unavailable")?;
    anyhow::ensure!(workspace.is_absolute(), "Dream workspace must be absolute");
    let workspace = match fs::canonicalize(&workspace).await {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => workspace,
        Err(error) => return Err(error.into()),
    };
    // 本地恢复可能等待知识锁或文件落盘，不能占用 Stop / 入队共用的生命周期锁。
    {
        let _recovery = shared.dream_recovery_gate.lock().await;
        anyhow::ensure!(
            !shared.stopping.load(Ordering::Acquire) && !shared.stop_requested.is_cancelled(),
            SUPERVISOR_STOPPING_MESSAGE
        );
        engine.recover_pending_dream_execution().await?;
    }
    let _guard = shared.lifecycle_gate.lock().await;
    anyhow::ensure!(
        !shared.stopping.load(Ordering::Acquire) && !shared.stop_requested.is_cancelled(),
        SUPERVISOR_STOPPING_MESSAGE
    );
    write_yaml_atomic(
        &paths.supervisor_dir.join("dream_registration.yaml"),
        &Registration {
            workspace: workspace.clone(),
            last_check_at: Utc::now(),
        },
    )
    .await?;
    let jobs = read_jobs(paths).await?;
    if let Some(existing) = jobs.iter().find(|j| {
        matches!(j.kind, SupervisorJobKind::Dream { .. })
            && matches!(
                j.status,
                SupervisorJobStatus::Queued | SupervisorJobStatus::Running
            )
    }) {
        let mut job = existing.clone();
        if manual && job.status == SupervisorJobStatus::Queued {
            if let SupervisorJobKind::Dream {
                manual: requested, ..
            } = &mut job.kind
            {
                *requested = true;
            }
            job.notify_on_completion = true;
            write_job(paths, &job).await?;
        }
        return Ok(DreamCheckReport {
            job_id: Some(job.id),
            message: "Dream 已在队列中或运行中".into(),
        });
    }
    for failed in jobs
        .iter()
        .filter(|job| job.status == SupervisorJobStatus::Failed)
    {
        if has_checkpoint(paths, failed).await? {
            let job = recover_prepared(paths, failed).await?;
            let _ = shared.notify_tx.send(());
            return Ok(DreamCheckReport {
                job_id: Some(job.id),
                message: "Dream 正在恢复已持久化的提交，不重复调用模型".into(),
            });
        }
    }
    if !engine.dream_enabled() {
        return Ok(DreamCheckReport {
            job_id: None,
            message: "Dream 已关闭（agent.dream.enable=false）".into(),
        });
    }
    if !engine.dream_eligible(manual).await? {
        return Ok(DreamCheckReport {
            job_id: None,
            message: "Dream 未触发：没有可整理 Claim，或未满足自动触发门槛".into(),
        });
    }
    let fingerprint = engine.dream_input_fingerprint().await?;
    if !manual && jobs.iter().any(|j| j.status == SupervisorJobStatus::Failed && matches!(&j.kind,SupervisorJobKind::Dream{input_fingerprint,..} if input_fingerprint==&fingerprint)) {
        return Ok(DreamCheckReport {job_id:None,message:"Dream 相同输入已耗尽重试，等待新变化或手动触发".into()});
    }
    fs::create_dir_all(&paths.jobs_dir).await?;
    let id =
        mint_unique_id_in_dir(&paths.jobs_dir, next_job_id, default_id_mint_max_attempts()).await?;
    let now = Utc::now();
    let job = SupervisorJob {
        id,
        agent_id: Some(shared.agent_id.clone()),
        kind: SupervisorJobKind::Dream {
            workspace,
            manual,
            input_fingerprint: fingerprint,
        },
        status: SupervisorJobStatus::Queued,
        attempts: 0,
        manual_retries: 0,
        created_at: now,
        updated_at: now,
        started_at: None,
        finished_at: None,
        last_error: None,
        notify_on_completion: manual,
    };
    write_reserved_job(paths, &job).await?;
    let _ = shared.notify_tx.send(());
    Ok(DreamCheckReport {
        job_id: Some(job.id),
        message: "Dream 已入队（Finalize > Recap > Dream）".into(),
    })
}

pub(super) async fn tick(
    paths: &SupervisorPaths,
    shared: &SupervisorSharedState,
) -> anyhow::Result<()> {
    if shared
        .dream_engine
        .as_ref()
        .is_none_or(|e| !e.dream_enabled())
    {
        return Ok(());
    }
    let registration = match read_yaml::<Registration>(
        &paths.supervisor_dir.join("dream_registration.yaml"),
    )
    .await
    {
        Ok(r) => r,
        Err(crate::storage::StorageError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            // Supervisor 自己的工具根是 Agent 数据目录，不能当作代码证据工作区。
            // 等待前台启动/恢复会话注册真实工作区后，才开始定时检查。
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    if (Utc::now() - registration.last_check_at)
        .to_std()
        .unwrap_or_default()
        >= CHECK_INTERVAL
    {
        enqueue_check(paths, shared, registration.workspace, false).await?;
    }
    Ok(())
}

pub(super) async fn has_checkpoint(
    paths: &SupervisorPaths,
    job: &SupervisorJob,
) -> anyhow::Result<bool> {
    Ok(matches!(job.kind, SupervisorJobKind::Dream { .. })
        && fs::try_exists(
            paths
                .supervisor_dir
                .join("dream")
                .join(format!("{}.yaml", job.id)),
        )
        .await?)
}

pub(super) async fn recover_prepared(
    paths: &SupervisorPaths,
    job: &SupervisorJob,
) -> anyhow::Result<SupervisorJob> {
    let mut recovered = job.clone();
    recovered.status = SupervisorJobStatus::Queued;
    recovered.finished_at = None;
    recovered.updated_at = Utc::now();
    recovered.last_error =
        Some("Recovering durable Dream checkpoint without another model attempt".into());
    write_job(paths, &recovered).await?;
    Ok(recovered)
}

pub(super) async fn recover_sessionless(
    paths: &SupervisorPaths,
    job: &SupervisorJob,
    message: String,
) -> anyhow::Result<SupervisorJob> {
    let mut recovered = job.clone();
    apply_job_attempt_failure(&mut recovered, message);
    write_job(paths, &recovered).await?;
    Ok(recovered)
}

pub(super) async fn retry_dream_job(
    paths: &SupervisorPaths,
    agent_id: &AgentId,
    jobs: &[SupervisorJob],
    original: &SupervisorJob,
) -> anyhow::Result<SupervisorRetryReport> {
    validate_job_agent(original, agent_id)?;
    anyhow::ensure!(
        original.status == SupervisorJobStatus::Failed,
        "only failed Dream jobs can be retried"
    );
    anyhow::ensure!(
        !jobs
            .iter()
            .any(|j| matches!(j.kind, SupervisorJobKind::Dream { .. })
                && matches!(
                    j.status,
                    SupervisorJobStatus::Queued | SupervisorJobStatus::Running
                )),
        "another Dream job is pending"
    );
    let mut job = original.clone();
    let previous_attempts = job.attempts;
    job.attempts = 0;
    job.manual_retries = job.manual_retries.saturating_add(1);
    job.status = SupervisorJobStatus::Queued;
    job.finished_at = None;
    job.started_at = None;
    job.updated_at = Utc::now();
    if let SupervisorJobKind::Dream { manual, .. } = &mut job.kind {
        *manual = true;
    }
    write_job(paths, &job).await?;
    Ok(SupervisorRetryReport {
        session_id: None,
        job_id: job.id,
        previous_attempts,
        manual_retries: job.manual_retries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dream_completion_statistics_use_written_versions_and_preserve_unknown_legacy_data() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("dream/job_example");
        tokio::fs::create_dir_all(&root).await.unwrap();
        assert!(dream_completion_summary(dir.path(), "job_example")
            .await
            .unwrap()
            .is_none());
        let mut record = serde_json::json!({"applied":true,"self_versions":{"claim_11111111":"version"},"plan":{"review":{"quality":"claims changed: 99"}}});
        tokio::fs::write(root.join("result.json"), record.to_string())
            .await
            .unwrap();
        assert!(dream_completion_summary(dir.path(), "job_example")
            .await
            .unwrap()
            .is_none());
        record["exploration_receipts"] = serde_json::json!(0);
        tokio::fs::write(root.join("result.json"), record.to_string())
            .await
            .unwrap();
        assert_eq!(
            dream_completion_summary(dir.path(), "job_example")
                .await
                .unwrap(),
            Some(DreamCompletionSummary {
                changed_claims: 1,
                exploration_receipts: 0
            })
        );
        record["applied"] = serde_json::json!(false);
        tokio::fs::write(root.join("result.json"), record.to_string())
            .await
            .unwrap();
        assert!(dream_completion_summary(dir.path(), "job_example")
            .await
            .unwrap()
            .is_none());
        assert!(dream_completion_summary(dir.path(), "../job_example")
            .await
            .is_err());
    }
    fn job(kind: SupervisorJobKind, id: &str) -> SupervisorJob {
        SupervisorJob {
            id: id.into(),
            agent_id: Some(AgentId::new("agent-test").unwrap()),
            kind,
            status: SupervisorJobStatus::Queued,
            attempts: 0,
            manual_retries: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            started_at: None,
            finished_at: None,
            last_error: None,
            notify_on_completion: false,
        }
    }
    #[tokio::test]
    async fn dream_queue_priority_and_sessionless_restart_retry() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SupervisorPaths::new(dir.path());
        let session: SessionId = "session_11111111".parse().unwrap();
        let mut dream = job(
            SupervisorJobKind::Dream {
                workspace: dir.path().into(),
                manual: false,
                input_fingerprint: "version".into(),
            },
            "job_dream",
        );
        let recap = job(
            SupervisorJobKind::Recap {
                session_id: session.clone(),
                recap_end_index: 1,
            },
            "job_recap",
        );
        let finalize = job(
            SupervisorJobKind::Finalize {
                session_id: session,
            },
            "job_finalize",
        );
        for item in [&dream, &recap, &finalize] {
            write_yaml_atomic(&job_path(&paths, &item.id), item)
                .await
                .unwrap();
        }
        assert_eq!(
            next_queued_job(&paths).await.unwrap().unwrap().id,
            "job_finalize"
        );
        assert!(dream_has_priority_work(&paths.jobs_dir).await.unwrap());
        assert!(job_to_view(&dream).session_id.is_none());
        dream.status = SupervisorJobStatus::Running;
        dream.attempts = 4;
        write_job(&paths, &dream).await.unwrap();
        reconcile_stale_running_jobs(&paths).await.unwrap();
        let jobs = read_jobs(&paths).await.unwrap();
        let recovered = jobs.iter().find(|j| j.id == dream.id).unwrap();
        assert_eq!(recovered.status, SupervisorJobStatus::Queued);
        assert_eq!(recovered.attempts, 4);
        dream.attempts = 5;
        write_job(&paths, &dream).await.unwrap();
        reconcile_stale_running_jobs(&paths).await.unwrap();
        let jobs = read_jobs(&paths).await.unwrap();
        let failed = jobs.iter().find(|j| j.id == dream.id).unwrap();
        assert_eq!(failed.status, SupervisorJobStatus::Failed);
        let report = retry_dream_job(&paths, &AgentId::new("agent-test").unwrap(), &jobs, failed)
            .await
            .unwrap();
        assert_eq!(report.previous_attempts, 5);
        assert!(report.session_id.is_none());
    }

    #[tokio::test]
    async fn exhausted_dream_with_checkpoint_recovers_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SupervisorPaths::new(dir.path());
        let mut item = job(
            SupervisorJobKind::Dream {
                workspace: dir.path().into(),
                manual: false,
                input_fingerprint: "version".into(),
            },
            "job_prepared",
        );
        item.status = SupervisorJobStatus::Failed;
        item.attempts = 5;
        write_yaml_atomic(&job_path(&paths, &item.id), &item)
            .await
            .unwrap();
        write_yaml_atomic(
            &paths.supervisor_dir.join("dream").join("job_prepared.yaml"),
            &serde_json::json!({"applied":false}),
        )
        .await
        .unwrap();
        reconcile_stale_running_jobs(&paths).await.unwrap();
        let jobs = read_jobs(&paths).await.unwrap();
        assert_eq!(jobs[0].status, SupervisorJobStatus::Queued);
        assert_eq!(jobs[0].attempts, 5);
        assert!(jobs[0]
            .last_error
            .as_ref()
            .unwrap()
            .contains("without another model attempt"));
    }
}
