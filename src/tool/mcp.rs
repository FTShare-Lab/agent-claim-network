//! MCP 工具的可见性判断、派发与进度转发。
//!
//! 负责将动态 MCP catalog 接入 `ToolRegistry`，并约束错误与进度输出。

use super::*;

impl ToolRegistry {
    pub(super) async fn mcp_tool(
        &self,
        visible_name: &str,
        input: Value,
        context: ToolDispatchContext,
        require_read_only: bool,
    ) -> Result<ToolExecution, ToolError> {
        let Some(mcp_manager) = &self.mcp_manager else {
            return Err(ToolError::UnknownTool(visible_name.to_string()));
        };
        let frozen_route = match &context.provider_mcp_routes {
            Some(routes) => Some(
                routes
                    .get(visible_name)
                    .cloned()
                    .ok_or_else(|| ToolError::UnknownTool(visible_name.to_string()))?,
            ),
            None => None,
        };
        let route = frozen_route.clone().or_else(|| {
            crate::mcp::tool::tool_catalog(&mcp_manager.snapshot_sync())
                .route(visible_name)
                .cloned()
        });
        let Some(route) = route else {
            return Err(ToolError::UnknownTool(visible_name.to_string()));
        };
        let progress_reporter = mcp_progress_reporter(visible_name, &context);
        log::debug!(
            target: "mcp",
            "mcp tool call visible={} server={} tool={} caller={}",
            visible_name,
            route.server_name,
            route.raw_tool_name,
            context.current_turn_id.as_deref().unwrap_or("unknown")
        );
        let result = if frozen_route.is_some() {
            mcp_manager
                .call_tool_cancellable_for_generation(
                    &route.server_name,
                    &route.raw_tool_name,
                    Some(input),
                    progress_reporter,
                    require_read_only,
                    context.cancellation,
                    route.generation,
                )
                .await
        } else if require_read_only {
            mcp_manager
                .call_read_only_tool_cancellable(
                    &route.server_name,
                    &route.raw_tool_name,
                    Some(input),
                    progress_reporter,
                    context.cancellation,
                )
                .await
        } else {
            mcp_manager
                .call_tool_cancellable(
                    &route.server_name,
                    &route.raw_tool_name,
                    Some(input),
                    progress_reporter,
                    context.cancellation,
                )
                .await
        };
        match result {
            Ok(result) => {
                let output = crate::mcp::tool::mcp_tool_result_to_value(&result);
                if result.is_error.unwrap_or(false) {
                    Ok(ToolExecution::business_failure(output))
                } else {
                    Ok(ToolExecution::completed(output))
                }
            }
            Err(err) => Err(ToolError::Mcp(bounded_mcp_dispatch_error(&err.to_string()))),
        }
    }

    pub(super) fn is_read_only_mcp_tool(&self, visible_name: &str) -> bool {
        self.mcp_manager.as_ref().is_some_and(|mcp_manager| {
            crate::mcp::tool::visible_tool_is_read_only(&mcp_manager.snapshot_sync(), visible_name)
        })
    }
}

fn mcp_progress_reporter(
    visible_name: &str,
    context: &ToolDispatchContext,
) -> Option<McpToolProgressReporter> {
    let tool_use_id = context.tool_use_id.clone()?;
    let progress_tx = context.progress_tx.clone()?;
    let visible_name = visible_name.to_string();
    Some(McpToolProgressReporter::new(move |event| {
        let _ = progress_tx.send(ToolProgressUpdate {
            id: tool_use_id.clone(),
            // ToolCell 以标准 `tool <name> progress ...` 前缀识别进度；turn id 属于
            // 调度上下文而非用户可见摘要，写入这里会使真实 MCP progress 无法渲染。
            summary: format_mcp_progress_summary(&visible_name, &event),
        });
    }))
}

fn format_mcp_progress_summary(visible_name: &str, event: &McpProgressEvent) -> String {
    let progress = match event.total {
        Some(total) => format!(
            "{}/{}",
            format_progress_number(event.progress),
            format_progress_number(total)
        ),
        None => format_progress_number(event.progress),
    };
    match event
        .message
        .as_deref()
        .map(str::trim)
        .filter(|message| !message.is_empty())
    {
        Some(message) => {
            let message = redact_mcp_sensitive_text(message);
            format!("tool {visible_name} progress {progress} {message}")
        }
        None => format!("tool {visible_name} progress {progress}"),
    }
}

fn format_progress_number(value: f64) -> String {
    if value.is_finite() && value.fract().abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        let formatted = format!("{value:.2}");
        formatted
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn bounded_mcp_dispatch_error(error: &str) -> String {
    let redacted = redact_mcp_sensitive_text(error);
    let (bounded, _) = truncate_chars(&redacted, MAX_MCP_DISPATCH_ERROR_CHARS);
    bounded
}

#[cfg(test)]
mod mcp_progress_tests {
    use super::*;

    #[test]
    fn mcp_progress_summary_redacts_sensitive_message() {
        let event = McpProgressEvent {
            server_name: "pal".to_string(),
            progress_token: "progress-1".to_string(),
            progress: 1.0,
            total: Some(2.0),
            message: Some(
                "Authorization: Bearer secret-token url=HTTPS://user:pass@example.test/mcp?token=abc"
                    .to_string(),
            ),
        };

        let summary = format_mcp_progress_summary("mcp__pal__ask", &event);

        assert!(summary.contains("<redacted>"));
        assert!(!summary.contains("secret-token"));
        assert!(!summary.contains("user:pass"));
        assert!(!summary.contains("token=abc"));
    }

    #[test]
    fn mcp_progress_reporter_keeps_summary_renderable_with_turn_context() {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let reporter = mcp_progress_reporter(
            "mcp__shared__slow_read",
            &ToolDispatchContext {
                current_turn_id: Some("turn_1".to_string()),
                tool_use_id: Some("toolu_1".to_string()),
                progress_tx: Some(progress_tx),
                ..ToolDispatchContext::default()
            },
        )
        .expect("完整的 MCP 工具上下文应创建 progress reporter");

        reporter.emit(McpProgressEvent {
            server_name: "shared".to_string(),
            progress_token: "acn-mcp-1".to_string(),
            progress: 1.0,
            total: Some(2.0),
            message: Some("fixture running".to_string()),
        });

        let progress = progress_rx
            .try_recv()
            .expect("reporter 应发送 progress update");
        assert_eq!(progress.id, "toolu_1");
        assert_eq!(
            progress.summary,
            "tool mcp__shared__slow_read progress 1/2 fixture running"
        );
    }

    #[test]
    fn mcp_dispatch_error_is_redacted_and_bounded() {
        let error = format!(
            "Authorization: Bearer secret-token url=https://user:pass@example.test/mcp?token=abc {}",
            "x".repeat(MAX_MCP_DISPATCH_ERROR_CHARS + 100)
        );

        let bounded = bounded_mcp_dispatch_error(&error);

        assert!(bounded.chars().count() <= MAX_MCP_DISPATCH_ERROR_CHARS);
        assert!(bounded.contains("<redacted>"));
        assert!(!bounded.contains("secret-token"));
        assert!(!bounded.contains("user:pass"));
        assert!(!bounded.contains("token=abc"));
    }
}
