//! session 搜索、working note、ask_user 与 router 查询工具。
//!
//! 聚合仅依赖 session/agent 上下文的轻量工具及其业务失败映射。

use super::*;

impl ToolRegistry {
    pub(super) async fn session_search(
        &self,
        input: Value,
        context: ToolDispatchContext,
    ) -> Result<ToolExecution, ToolError> {
        let mut input = input;
        if let Value::Object(map) = &mut input {
            map.remove("_");
        }
        let args: SessionSearchArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let Some(service) = self.session_search.as_ref() else {
            return Err(ToolError::UnknownTool("session_search".into()));
        };
        let request = SessionSearchRequest {
            query: args.query,
            limit: args.limit,
            sort: SessionSearchSort::parse(args.sort.as_deref()).map_err(ToolError::InvalidArgs)?,
            session_id: args
                .session_id
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|e| ToolError::InvalidArgs(format!("invalid session_id: {e}")))?,
            around_message_index: args.around_message_index,
            window: args.window,
            include_tool_results: args.include_tool_results.unwrap_or(false),
        };
        let response = service.run(request, context.current_session_id).await;
        let success = response.success;
        let output = serde_json::to_value(response)
            .map_err(|e| ToolError::InvalidArgs(format!("session_search 序列化失败: {e}")))?;
        if success {
            Ok(ToolExecution::completed(output))
        } else {
            Ok(ToolExecution::business_failure(output))
        }
    }

    pub(super) async fn working_note(&self, input: Value) -> Result<Value, ToolError> {
        let args: WorkingNoteArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let mut notes = self.notes.lock().await;
        match args.action.as_str() {
            "add" => {
                let note = args
                    .note
                    .ok_or_else(|| ToolError::InvalidArgs("action=add 需要 note".into()))?;
                notes.push(note);
            }
            "list" => {}
            "clear" => notes.clear(),
            other => return Err(ToolError::InvalidArgs(format!("未知 action: {other}"))),
        }
        Ok(json!({
            "notes": notes.clone(),
        }))
    }

    /// 当前只通知模型任务需要用户输入，不真正挂起等待回答。
    /// tool_result 写回后模型只看到"需要用户输入"标记，拿不到实际回答。
    /// 后续版本需在 CLI 模式下从 stdin 读取用户回答并注入 tool_result。
    pub(super) async fn ask_user(&self, input: Value) -> Result<Value, ToolError> {
        let args: AskUserArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        Ok(json!({
            "needs_user_input": true,
            "question": args.question,
            "choices": args.choices.unwrap_or_default(),
        }))
    }

    pub(super) async fn consult_router(&self, input: Value) -> Result<ToolExecution, ToolError> {
        let router = self
            .router_client
            .as_ref()
            .ok_or_else(|| ToolError::UnknownTool("consult_router".into()))?;
        let args: ConsultRouterArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        match args.mode {
            ConsultRouterMode::Overview => {
                if non_empty_trimmed(args.scope.as_deref()).is_some()
                    || non_empty_trimmed(args.semantic_query.as_deref()).is_some()
                {
                    return Err(ToolError::InvalidArgs(
                        "consult_router mode=overview 不允许提供 scope 或 semantic_query".into(),
                    ));
                }
                let overview = router.scopes_overview().await.map_err(|err| {
                    log::warn!(target: "tool", "consult_router overview 不可用: {err:#}");
                    err
                });
                let overview = match overview {
                    Ok(overview) => overview,
                    Err(err) => {
                        let mut payload = consult_router_failure_payload("overview", &err);
                        payload.insert("scopes".into(), json!([]));
                        return Ok(ToolExecution::business_failure(Value::Object(payload)));
                    }
                };
                Ok(ToolExecution::completed(json!({
                    "mode": "overview",
                    "available": true,
                    "scopes": overview.scopes,
                })))
            }
            ConsultRouterMode::Query => {
                let scope = non_empty_trimmed(args.scope.as_deref()).ok_or_else(|| {
                    ToolError::InvalidArgs("consult_router mode=query 必须提供非空 scope".into())
                })?;
                let semantic_query =
                    non_empty_trimmed(args.semantic_query.as_deref()).map(str::to_string);
                let result = router
                    .query(&AgentQuery {
                        scope: scope.to_string(),
                        semantic_query,
                    })
                    .await
                    .map_err(|err| {
                        log::warn!(target: "tool", "consult_router query 不可用: {err:#}");
                        err
                    });
                let result = match result {
                    Ok(result) => result,
                    Err(err) => {
                        let mut payload = consult_router_failure_payload("query", &err);
                        payload.insert("candidate_claims".into(), json!([]));
                        payload.insert("disputes".into(), json!([]));
                        return Ok(ToolExecution::business_failure(Value::Object(payload)));
                    }
                };
                Ok(ToolExecution::completed(json!({
                    "mode": "query",
                    "available": true,
                    "candidate_claims": result.candidate_claims,
                    "disputes": result.disputes,
                })))
            }
        }
    }
}

fn consult_router_failure_payload(mode: &str, err: &anyhow::Error) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert("mode".into(), json!(mode));
    payload.insert("available".into(), json!(false));
    payload.insert("error".into(), json!(err.to_string()));

    if let Some(RouterClientError::Auth { operation, status }) =
        err.downcast_ref::<RouterClientError>()
    {
        payload.insert("reason".into(), json!("router_auth_failed"));
        payload.insert("http_status".into(), json!(status));
        payload.insert("operation".into(), json!(operation));
        payload.insert(
            "message".into(),
            json!("团队 router 鉴权失败，请检查当前 upstream 的 acn_key_env 是否有效。"),
        );
    } else {
        payload.insert("reason".into(), json!("router_unavailable"));
        payload.insert(
            "message".into(),
            json!("团队 router 当前不可用，本轮请仅基于本地上下文继续。"),
        );
    }

    payload
}

fn non_empty_trimmed(value: Option<&str>) -> Option<&str> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}
