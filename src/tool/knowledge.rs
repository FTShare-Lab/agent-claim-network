//! 本地 Claim/Trace 读取与 Dream 的只读工具准入、证据收据。
use super::*;
use crate::agent::LocalClaimStore;
use crate::claim::{Claim, ClaimId, ClaimStatus, Trace, TraceId};
use crate::storage::read_yaml;

/// 计划属于 Agent；工具层只提供受限派发，不获得 Claim 写权限。
#[async_trait::async_trait]
pub(crate) trait DreamPlanTools: Send + Sync {
    fn definitions(&self) -> Vec<ToolDefinition>;
    async fn call(
        &self,
        name: &str,
        input: Value,
        receipts: BTreeMap<String, Value>,
        observed: BTreeMap<ClaimId, Claim>,
    ) -> Value;
}

pub(super) const DREAM_COMMAND_GUIDANCE: &str = "Read-only Bash exploration inside the declared workspace, with an OS sandbox and a strict argument allowlist. Consult evidence_access.command_environment first; unavailable commands are rejected before execution. If lookup fails, use another available command or file_read/read_trace; do not treat tool failure as absent project evidence. Use simple literal commands: ls -la; ls -R; grep -n 'pattern' path; rg --files; rg -n 'pattern' path; cat path; head -n 80 path; wc -l path. These are separate examples, not a script to run together. rg requires ripgrep installed in the sandbox system PATH; if unavailable use ls/grep or file_read. find, sort, echo, sed, scripts, tests/builds, shell expansion, globs, Python, PowerShell and interactive PTYs are unavailable. Pipes and && work only when every command and argument is allowed; prefer separate simple calls. On rejection use a listed example or file_read instead of guessing flags. Use file_read for versioned evidence before changing a claim. Commands complete here; no process polling tool is exposed.";

#[derive(Clone)]
pub(super) struct KnowledgeAccess {
    store: Arc<dyn LocalClaimStore>,
    home: PathBuf,
    agent_id: AgentId,
    receipts: Arc<Mutex<BTreeMap<String, Value>>>,
    observed: Arc<Mutex<BTreeMap<ClaimId, Claim>>>,
    partial_reads: Arc<Mutex<BTreeMap<ClaimId, (Claim, usize)>>>,
    trace_links: Arc<tokio::sync::OnceCell<Vec<TraceLink>>>,
}

impl KnowledgeAccess {
    pub(super) fn private_root(&self) -> PathBuf {
        self.home
            .parent()
            .filter(|p| p.file_name().is_some_and(|n| n == "agents"))
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.home.clone())
    }

    async fn read_guard(&self) -> Result<crate::storage::FileLockGuard, ToolError> {
        let path = crate::storage::paths::agent_home_knowledge_apply_lock_path(&self.home);
        let guard = crate::storage::FileLockGuard::try_lock_exclusive(&path)
            .await
            .map_err(|e| ToolError::InvalidArgs(e.to_string()))?
            .ok_or_else(|| {
                ToolError::InvalidArgs(
                    "Claim store is being updated; retry this read shortly".into(),
                )
            })?;
        if fs::try_exists(
            self.home
                .join("runtime")
                .join("supervisor")
                .join("dream_pending.yaml"),
        )
        .await?
        {
            return Err(ToolError::InvalidArgs("A Dream group is being recovered; retry after recovery, do not read a partially applied group".into()));
        }
        Ok(guard)
    }
}

#[derive(Clone)]
struct TraceLink {
    id: TraceId,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
    inputs: Vec<crate::claim::SourceId>,
    outputs: Vec<ClaimId>,
}

async fn scan_trace_links(access: &KnowledgeAccess) -> Result<Vec<TraceLink>, ToolError> {
    let mut links = Vec::new();
    let mut entries = match fs::read_dir(access.home.join("traces")).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(links),
        Err(e) => return Err(e.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension().is_none_or(|e| e != "yaml") {
            continue;
        }
        let Ok(trace) = read_yaml::<Trace>(&entry.path()).await else {
            continue;
        };
        if trace.agent == access.agent_id
            && entry.path().file_stem().and_then(|x| x.to_str()) == Some(trace.id.as_str())
        {
            links.push(TraceLink {
                id: trace.id,
                name: trace.name,
                created_at: trace.created_at,
                inputs: trace.input_claims,
                outputs: trace.output_claims,
            });
        }
    }
    links.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(links)
}

pub(super) fn definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_claim".into(),
            description: "Read a local claim by id (including deprecated history), or omit id to page through the non-deprecated local index. start/count paginate lines for a claim and entries for the index. Follow page.next_start only if needed. For runtime change notices, assess relevance from name/scope and read current content only when the task needs to cite or rely on that Claim. Do not read every changed Claim merely because it is listed. Deprecated/removed notices already mean stop relying on those entries; no content read is needed for that.".into(),
            input_schema: json!({"type":"object","properties":{"id":{"type":"string"},"start":{"type":"integer","minimum":1},"count":{"type":"integer","minimum":1}},"additionalProperties":false}),
        },
        ToolDefinition {
            name: "read_trace".into(),
            description: "Find this agent's local traces related to claim_id. Omit trace_id to list related trace identities; provide it to read that related trace with file_read pagination. A trace records provenance, not proof of task success. Missing traces or evaluator results mean unknown.".into(),
            input_schema: json!({"type":"object","properties":{"claim_id":{"type":"string"},"trace_id":{"type":"string"},"keyword":{"type":"string"},"start":{"type":"integer","minimum":1},"count":{"type":"integer","minimum":1}},"required":["claim_id"],"additionalProperties":false}),
        },
    ]
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimReadArgs {
    id: Option<ClaimId>,
    start: Option<usize>,
    count: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TraceReadArgs {
    claim_id: ClaimId,
    trace_id: Option<TraceId>,
    keyword: Option<String>,
    start: Option<usize>,
    count: Option<usize>,
}

impl ToolRegistry {
    pub(crate) fn with_dream_plan_tools(mut self, handler: Arc<dyn DreamPlanTools>) -> Self {
        self.dream_plan_tools = Some(handler);
        self
    }
    pub fn with_local_knowledge(
        mut self,
        store: Arc<dyn LocalClaimStore>,
        home: PathBuf,
        agent_id: AgentId,
    ) -> Self {
        self.knowledge = Some(KnowledgeAccess {
            store,
            home,
            agent_id,
            receipts: Arc::default(),
            observed: Arc::default(),
            partial_reads: Arc::default(),
            trace_links: Arc::default(),
        });
        self
    }

    pub(crate) fn for_dream(mut self, workspace: PathBuf) -> Self {
        self.workspace_root = workspace;
        self.access = ToolAccessProfile::memory_review();
        self.access.local_tools = true;
        self.access.memory = false;
        self.dream_read_only = true;
        self.delegation_host = None;
        self.delegation_progress = None;
        if let Some(access) = &mut self.knowledge {
            access.receipts = Arc::default();
            access.observed = Arc::default();
            access.partial_reads = Arc::default();
            access.trace_links = Arc::default();
        }
        // 每次 attempt 独享进程管理器，退出时清理，不占用前台进程所有权。
        self.process_manager = Arc::new(ProcessManager::new(
            self.limits.background_process_output_buffer_bytes,
            default_id_mint_max_attempts(),
            self.limits.background_process_max_entries_per_owner,
            self.limits.background_process_protected_recent_entries,
        ));
        self
    }

    pub(crate) async fn dream_evidence(
        &self,
    ) -> (BTreeMap<String, Value>, BTreeMap<ClaimId, Claim>) {
        match &self.knowledge {
            Some(access) => (
                access.receipts.lock().await.clone(),
                access.observed.lock().await.clone(),
            ),
            None => (BTreeMap::new(), BTreeMap::new()),
        }
    }

    /// 仅恢复宿主已校验版本的读取记录；不恢复进程或尚未完整读完的 Claim。
    pub(crate) async fn restore_dream_evidence(
        &self,
        receipts: BTreeMap<String, Value>,
        observed: BTreeMap<ClaimId, Claim>,
    ) {
        if let Some(access) = &self.knowledge {
            *access.receipts.lock().await = receipts;
            *access.observed.lock().await = observed;
        }
    }

    pub(crate) async fn cleanup_dream_processes(&self) {
        let owner = self.process_owner(&ToolDispatchContext::default());
        self.process_manager.cleanup_owner(&owner).await;
    }

    pub(super) async fn validate_dream_tool(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<(), ToolError> {
        match name {
            "dream_stage_group"
            | "dream_finish"
            | "dream_read_draft"
            | "dream_validate"
            | "dream_review_group"
            | "dream_apply_group"
            | "dream_read_history"
            | "dream_record_candidates"
            | "dream_keep_candidate"
                if self.dream_plan_tools.is_some() =>
            {
                Ok(())
            }
            "read_claim" | "read_trace" => Ok(()),
            "code_run" => self.validate_dream_command(input).await,
            "file_read" => {
                let args: FileReadArgs = serde_json::from_value(input.clone())
                    .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
                self.validate_dream_path(&resolve_tool_path(&self.workspace_root, &args.path))
                    .await?;
                Ok(())
            }
            _ => Err(ToolError::InvalidArgs(
                "tool is outside Dream read-only allowlist".into(),
            )),
        }
    }

    pub(super) async fn dispatch_dream(
        &self,
        name: &str,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        self.validate_dream_tool(name, &input).await?;
        if matches!(
            name,
            "dream_stage_group"
                | "dream_finish"
                | "dream_read_draft"
                | "dream_validate"
                | "dream_review_group"
                | "dream_apply_group"
                | "dream_read_history"
                | "dream_record_candidates"
                | "dream_keep_candidate"
        ) {
            let handler = self
                .dream_plan_tools
                .as_ref()
                .ok_or_else(|| ToolError::InvalidArgs("Dream plan handler unavailable".into()))?;
            let (receipts, observed) = self.dream_evidence().await;
            return Ok(ToolExecution::completed(
                handler.call(name, input, receipts, observed).await,
            ));
        }
        let mut execution = match name {
            "read_claim" => self.read_claim(input.clone(), context).await?,
            "read_trace" => self.read_trace(input.clone(), context).await?,
            "file_read" => self.file_read(input.clone(), context).await?,
            "code_run" => {
                let mut result = self.code_run(input.clone(), context).await?;
                let mut stdout = String::new();
                let mut stderr = String::new();
                let mut truncated = false;
                collect_command_output(
                    &result.output,
                    &mut stdout,
                    &mut stderr,
                    &mut truncated,
                    self.limits.code_run_max_output_chars,
                );
                // Dream 不把子进程交给下一次 job；只允许内部空轮询，不暴露写入 stdin。
                while let Some(process_id) = result
                    .output
                    .get("process_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .filter(|_| {
                        result
                            .output
                            .get("state")
                            .and_then(Value::as_str)
                            .is_some_and(|s| matches!(s, "running" | "terminating"))
                    })
                {
                    result = self
                        .write_stdin(
                            json!({"process_id":process_id,"chars":"","yield_time_ms":1000}),
                            context,
                        )
                        .await?;
                    collect_command_output(
                        &result.output,
                        &mut stdout,
                        &mut stderr,
                        &mut truncated,
                        self.limits.code_run_max_output_chars,
                    );
                }
                result.output["stdout"] = json!(stdout);
                result.output["stderr"] = json!(stderr);
                result.output["truncated"] = json!(truncated);
                result
            }
            _ => return Err(ToolError::InvalidArgs("unsupported Dream tool".into())),
        };
        // 必须在返回模型（含附件块）之前检查，而非事后丢弃审批依赖。
        // 这也覆盖媒体、索引以及将来新增的无法版本化读取分支。
        if let Some(access) = &self.knowledge {
            let mut receipts = access.receipts.lock().await;
            let id = format!("evidence_{}", receipts.len() + 1);
            receipts.insert(
                id.clone(),
                json!({"tool":name,"input":input,"output":execution.output}),
            );
            if let Some(object) = execution.output.as_object_mut() {
                object.insert("evidence_id".into(), json!(id));
            }
        }
        Ok(execution)
    }

    pub(super) async fn read_claim(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let access = self
            .knowledge
            .as_ref()
            .ok_or_else(|| ToolError::InvalidArgs("local knowledge unavailable".into()))?;
        let args: ClaimReadArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let _guard = access.read_guard().await?;
        let mut claims = access
            .store
            .list_local_claims()
            .await
            .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        claims.retain(|claim| claim.holder == access.agent_id);
        if let Some(id) = args.id {
            let claim = claims.into_iter().find(|c| c.id == id).ok_or_else(|| {
                ToolError::InvalidArgs("claim not found in this agent's local store".into())
            })?;
            let path = access.home.join("claims").join(format!("{id}.yaml"));
            let result = self
                .file_read(
                    json!({"path":path,"start":args.start,"count":args.count}),
                    context,
                )
                .await?;
            // 分页只累计同一版本的连续正文；中途发生变化必须从第一页重读。
            let after: Claim = read_yaml(&path)
                .await
                .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
            let mut partial = access.partial_reads.lock().await;
            if after != claim {
                partial.remove(&id);
                access.observed.lock().await.remove(&id);
            } else if let Some(start) = result
                .output
                .pointer("/page/returned_start")
                .and_then(Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
            {
                if start == 1 {
                    partial.insert(id.clone(), (claim.clone(), 1));
                }
                if let Some((previous, next)) = partial.get_mut(&id) {
                    if *previous == claim && *next == start {
                        if result
                            .output
                            .pointer("/page/reaches_eof")
                            .and_then(Value::as_bool)
                            == Some(true)
                        {
                            access.observed.lock().await.insert(id.clone(), claim);
                        } else if let Some(next_start) = result
                            .output
                            .pointer("/page/next_start")
                            .and_then(Value::as_u64)
                            .and_then(|n| usize::try_from(n).ok())
                        {
                            *next = next_start;
                        }
                    }
                }
            }
            Ok(result)
        } else {
            claims.retain(|claim| claim.status != ClaimStatus::Deprecated);
            claims.sort_by(|a, b| {
                b.effective_updated_at()
                    .cmp(&a.effective_updated_at())
                    .then_with(|| a.id.cmp(&b.id))
            });
            page_items(
                claims
                    .iter()
                    .map(|c| json!({"id":c.id,"name":c.name,"scope":c.scope}))
                    .collect(),
                args.start,
                args.count,
            )
        }
    }

    pub(super) async fn read_trace(
        &self,
        input: Value,
        context: &ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let access = self
            .knowledge
            .as_ref()
            .ok_or_else(|| ToolError::InvalidArgs("local knowledge unavailable".into()))?;
        let args: TraceReadArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let _guard = access.read_guard().await?;
        let claims = access
            .store
            .list_local_claims()
            .await
            .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        if !claims
            .iter()
            .any(|c| c.id == args.claim_id && c.holder == access.agent_id)
        {
            return Err(ToolError::InvalidArgs(
                "claim not found in this agent's local store".into(),
            ));
        }
        let dir = access.home.join("traces");
        let links = if self.dream_read_only {
            access
                .trace_links
                .get_or_try_init(|| scan_trace_links(access))
                .await?
                .clone()
        } else {
            scan_trace_links(access).await?
        };
        let traces: Vec<_> = links
            .into_iter()
            .filter(|trace| {
                trace.outputs.contains(&args.claim_id)
                    || trace
                        .inputs
                        .contains(&crate::claim::SourceId::Claim(args.claim_id.clone()))
            })
            .collect();
        if let Some(id) = args.trace_id {
            if !traces.iter().any(|t| t.id == id) {
                return Err(ToolError::InvalidArgs(
                    "trace is not related to the requested claim".into(),
                ));
            }
            let current: Trace = read_yaml(&dir.join(format!("{id}.yaml")))
                .await
                .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
            if current.agent != access.agent_id
                || !(current.output_claims.contains(&args.claim_id)
                    || current
                        .input_claims
                        .contains(&crate::claim::SourceId::Claim(args.claim_id.clone())))
            {
                return Err(ToolError::InvalidArgs(
                    "trace relation changed; read its index again".into(),
                ));
            }
            self.file_read(json!({"path":dir.join(format!("{id}.yaml")),"start":args.start,"count":args.count,"keyword":args.keyword}), context).await
        } else {
            page_items(
                traces
                    .iter()
                    .map(|t| json!({"id":t.id,"name":t.name,"created_at":t.created_at,"input_claims":t.inputs,"output_claims":t.outputs}))
                    .collect(),
                args.start,
                args.count,
            )
        }
    }
}

fn collect_command_output(
    output: &Value,
    stdout: &mut String,
    stderr: &mut String,
    truncated: &mut bool,
    limit: usize,
) {
    *truncated |= output
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    for (key, target) in [("stdout", stdout), ("stderr", stderr)] {
        let chunk = output.get(key).and_then(Value::as_str).unwrap_or("");
        let remaining = limit.saturating_sub(target.chars().count());
        *truncated |= chunk.chars().count() > remaining;
        target.extend(chunk.chars().take(remaining));
    }
}

fn page_items(
    items: Vec<Value>,
    start: Option<usize>,
    count: Option<usize>,
) -> Result<ToolExecution, ToolError> {
    let start = start.unwrap_or(1);
    let count = count.unwrap_or(50).min(100);
    if start == 0 || count == 0 {
        return Err(ToolError::InvalidArgs(
            "start and count must be positive".into(),
        ));
    }
    let end = start
        .saturating_sub(1)
        .saturating_add(count)
        .min(items.len());
    let page = items.iter().skip(start - 1).take(count).collect::<Vec<_>>();
    Ok(ToolExecution::completed(
        json!({"items":page,"total":items.len(),"next_start":if end < items.len() { Some(end+1) } else {None}}),
    ))
}

/// 使用流式摘要核对本次实际读到的文件版本，防止探索后工作区变化被当作旧证据。
pub(crate) async fn dream_evidence_is_current(receipt: &Value) -> anyhow::Result<bool> {
    let Some(version) = receipt.pointer("/output/file_version") else {
        return Ok(true);
    };
    let path = version
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing evidence path"))?;
    let expected: ContentRevision = serde_json::from_value(version["revision"].clone())?;
    let mut file = match fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    let mut bytes = 0u64;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
        bytes = bytes.saturating_add(u64::try_from(n)?);
    }
    Ok(expected == ContentRevision::from_sha256(hex::encode(digest.finish().as_ref()), bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{fs::LocalFsClaimStore, LocalClaimStore};
    use crate::claim::{Claim, Confidence};

    fn claim(owner: AgentId, id: &str) -> Claim {
        Claim {
            id: id.parse().unwrap(),
            name: "queue".into(),
            statement: "Persisted items survive restart.".into(),
            scope: "example".into(),
            holder: owner,
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: crate::time::now_seconds(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "Recovery test.".into(),
        }
    }

    #[test]
    fn dream_command_examples_match_existing_allowlist() {
        for script in [
            "ls -la",
            "ls -R",
            "grep -n 'pattern' path",
            "rg --files",
            "rg -n 'pattern' path",
            "cat path",
            "head -n 80 path",
            "wc -l path",
        ] {
            assert!(
                super::super::concurrency::bash_script_is_concurrency_safe(script),
                "{script}"
            );
        }
        for script in [
            "find .",
            "head -100 path",
            "echo done",
            "python script.py",
            "cat *.rs",
        ] {
            assert!(
                !super::super::concurrency::bash_script_is_concurrency_safe(script),
                "{script}"
            );
        }
    }
    #[tokio::test]
    async fn dream_tools_reject_write_and_foreign_claim_and_resolve_trace_relationship() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("agent");
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(&workspace).await.unwrap();
        let owner = AgentId::new("agent-test").unwrap();
        let store = Arc::new(LocalFsClaimStore::new(home.clone()));
        let own = claim(owner.clone(), "claim_11111111");
        let foreign = claim(AgentId::new("agent-other").unwrap(), "claim_22222222");
        store.write_claim(&own).await.unwrap();
        store.write_claim(&foreign).await.unwrap();
        let trace = Trace {
            id: "trace_11111111".parse().unwrap(),
            name: "verification".into(),
            task: "Task ended without evaluator feedback.".into(),
            agent: owner.clone(),
            input_claims: vec![],
            output_claims: vec![own.id.clone()],
            created_at: chrono::Utc::now(),
        };
        store.write_trace(&trace).await.unwrap();
        let registry = ToolRegistry::new(&ToolConfig {
            workspace_root: workspace.clone(),
            ..Default::default()
        })
        .unwrap()
        .with_local_knowledge(store, home, owner)
        .for_dream(workspace.clone());
        let names = registry
            .definitions()
            .into_iter()
            .map(|d| d.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            BTreeSet::from([
                "file_read".into(),
                "code_run".into(),
                "read_claim".into(),
                "read_trace".into()
            ])
        );
        let command = registry
            .definitions()
            .into_iter()
            .find(|d| d.name == "code_run")
            .unwrap();
        assert!(command.description.contains("Read-only Bash"));
        assert!(!command.description.contains("high-permission"));
        assert_eq!(
            command.input_schema["properties"]["type"]["enum"],
            json!(["bash"])
        );
        assert!(registry
            .dispatch("file_write", json!({"path":"x","content":"bad"}))
            .await
            .is_err());
        assert!(registry
            .dispatch(
                "code_run",
                json!({"script":"printf bad > x","description":"write"})
            )
            .await
            .is_err());
        assert!(registry
            .dispatch("read_claim", json!({"id":foreign.id}))
            .await
            .is_err());
        let index = registry
            .dispatch("read_trace", json!({"claim_id":own.id}))
            .await
            .unwrap();
        assert_eq!(index.output["items"][0]["id"], json!(trace.id));
        assert!(registry
            .dispatch(
                "read_trace",
                json!({"claim_id":foreign.id,"trace_id":trace.id})
            )
            .await
            .is_err());
        let result = registry
            .dispatch("read_trace", json!({"claim_id":own.id,"trace_id":trace.id}))
            .await
            .unwrap();
        assert!(result.output["content"]
            .as_str()
            .unwrap()
            .contains("without evaluator feedback"));
        fs::write(workspace.join("evidence.txt"), "before\n")
            .await
            .unwrap();
        let read = registry
            .dispatch("file_read", json!({"path":"evidence.txt"}))
            .await
            .unwrap();
        let (receipts, _) = registry.dream_evidence().await;
        let receipt = &receipts[read.output["evidence_id"].as_str().unwrap()];
        assert!(dream_evidence_is_current(receipt).await.unwrap());
        fs::write(workspace.join("evidence.txt"), "after\n")
            .await
            .unwrap();
        assert!(!dream_evidence_is_current(receipt).await.unwrap());
        assert!(registry
            .dispatch(
                "code_run",
                json!({"script":"cat ../agent/claims/claim_11111111.yaml","description":"outside"})
            )
            .await
            .is_err());
        assert!(registry
            .dispatch(
                "code_run",
                json!({"script":"pwd","cwd":dir.path(),"description":"outside cwd"})
            )
            .await
            .is_err());
        #[cfg(target_os = "macos")]
        {
            fs::write(workspace.join("USER.md"), "PRIVATE_USER_SAMPLE")
                .await
                .unwrap();
            let command = registry
                .dispatch(
                    "code_run",
                    json!({"script":"cat evidence.txt","description":"read evidence"}),
                )
                .await
                .unwrap();
            assert_eq!(command.output["success"], true, "{:?}", command.output);
            assert!(command.output["stdout"].as_str().unwrap().contains("after"));
            let search = registry
                .dispatch(
                    "code_run",
                    json!({"script":"grep -R SAMPLE .","description":"restricted recursive read"}),
                )
                .await
                .unwrap();
            assert!(!search.output["stdout"]
                .as_str()
                .unwrap()
                .contains("PRIVATE_USER_SAMPLE"));
            let outside = dir.path().join("outside");
            fs::create_dir_all(&outside).await.unwrap();
            fs::write(outside.join("note.txt"), "OUTSIDE_SAMPLE")
                .await
                .unwrap();
            std::os::unix::fs::symlink(&outside, workspace.join("linked")).unwrap();
            let search = registry
                .dispatch(
                    "code_run",
                    json!({"script":"grep -R SAMPLE .","description":"symlink boundary"}),
                )
                .await
                .unwrap();
            assert!(!search.output["stdout"]
                .as_str()
                .unwrap()
                .contains("OUTSIDE_SAMPLE"));
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path(), workspace.join("escape")).unwrap();
            assert!(registry
                .dispatch(
                    "file_read",
                    json!({"path":"escape/agent/claims/claim_11111111.yaml"})
                )
                .await
                .is_err());
        }
    }
}
