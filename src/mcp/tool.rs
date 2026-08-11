//! MCP tool 与 ACN tool registry 之间的转换层。
//!
//! 本模块只处理模型可见工具定义、visible name 路由表和 `tools/call` 结果包装；
//! 连接生命周期仍由 `connection_manager` 负责。

use std::collections::BTreeMap;

use rmcp::model::CallToolResult;
use serde_json::{json, Map, Value};

use crate::mcp::client::call_tool_result_to_json;
use crate::mcp::connection_manager::{
    McpRuntimeState, McpServerStatus, McpToolExposure, McpToolSnapshot,
};
use crate::mcp::name::{build_visible_tool_names, McpVisibleToolName};
use crate::mcp::redact::redact_mcp_sensitive_text;

const MAX_MCP_TOOL_DESCRIPTION_CHARS: usize = 2_000;
const MAX_MCP_TOOL_RESULT_JSON_CHARS: usize = 64_000;
const MAX_MCP_TOOL_RESULT_PREVIEW_CHARS: usize = 16_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpToolRoute {
    pub visible_name: String,
    pub server_name: String,
    pub raw_tool_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpToolDefinitionSnapshot {
    pub visible_name: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub title: Option<String>,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct McpToolCatalog {
    definitions: Vec<McpToolDefinitionSnapshot>,
    routes: BTreeMap<String, McpToolRoute>,
    visible_names: BTreeMap<(String, String), String>,
}

impl McpToolCatalog {
    pub fn definitions(&self) -> &[McpToolDefinitionSnapshot] {
        &self.definitions
    }

    pub fn route(&self, visible_name: &str) -> Option<&McpToolRoute> {
        self.routes.get(visible_name)
    }

    pub fn visible_name_for(&self, server_name: &str, raw_tool_name: &str) -> Option<&str> {
        self.visible_names
            .get(&(server_name.to_string(), raw_tool_name.to_string()))
            .map(String::as_str)
    }
}

/// 判断当前可见 MCP 工具是否由 server 明确标记为只读。
///
/// 先经 catalog 回查可见名，既避免重名 hash 的字符串猜测，也确保输入 schema 与 exposure 已通过
/// 当前 catalog 的 fail-closed 过滤。
pub fn visible_tool_is_read_only(snapshot: &McpRuntimeState, visible_name: &str) -> bool {
    let catalog = tool_catalog(snapshot);
    let Some(route) = catalog.route(visible_name) else {
        return false;
    };
    let Some(server) = snapshot.servers.get(&route.server_name) else {
        return false;
    };
    if server.status != McpServerStatus::Ready {
        return false;
    }
    let Some(tool) = server
        .tools
        .iter()
        .find(|tool| tool.raw_name == route.raw_tool_name)
    else {
        return false;
    };
    matches!(tool.exposure, McpToolExposure::Exposed)
        && tool
            .raw_tool
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.read_only_hint)
            == Some(true)
}

pub fn tool_catalog(snapshot: &McpRuntimeState) -> McpToolCatalog {
    let mut ready_tools = Vec::<(&str, &McpToolSnapshot)>::new();
    let mut name_pairs = Vec::<(String, String)>::new();
    for (server_name, server) in &snapshot.servers {
        if server.status != McpServerStatus::Ready {
            continue;
        }
        for tool in &server.tools {
            ready_tools.push((server_name.as_str(), tool));
            name_pairs.push((server_name.clone(), tool.raw_name.clone()));
        }
    }

    let visible_names = build_visible_tool_names(&name_pairs);
    let mut definitions = Vec::new();
    let mut routes = BTreeMap::new();
    let mut visible_name_map = BTreeMap::new();
    for ((server_name, tool), visible) in ready_tools.into_iter().zip(visible_names) {
        visible_name_map.insert(
            (server_name.to_string(), tool.raw_name.clone()),
            visible.visible_name.clone(),
        );
        if tool.exposure != McpToolExposure::Exposed {
            continue;
        }
        let Some(input_schema) = normalized_input_schema(tool) else {
            continue;
        };
        let description = tool_description(server_name, tool);
        routes.insert(
            visible.visible_name.clone(),
            route_from_visible_name(&visible),
        );
        definitions.push(McpToolDefinitionSnapshot {
            visible_name: visible.visible_name,
            server_name: server_name.to_string(),
            raw_tool_name: tool.raw_name.clone(),
            title: tool.title.clone(),
            description,
            input_schema,
        });
    }

    McpToolCatalog {
        definitions,
        routes,
        visible_names: visible_name_map,
    }
}

pub fn mcp_tool_result_to_value(result: &CallToolResult) -> Value {
    let mut value = call_tool_result_to_json(result);
    let is_error = result.is_error.unwrap_or(false);
    if is_error {
        redact_value_strings(&mut value);
    }
    bounded_tool_result_value(value, is_error)
}

fn route_from_visible_name(visible: &McpVisibleToolName) -> McpToolRoute {
    McpToolRoute {
        visible_name: visible.visible_name.clone(),
        server_name: visible.server_name.clone(),
        raw_tool_name: visible.raw_tool_name.clone(),
    }
}

fn normalized_input_schema(tool: &McpToolSnapshot) -> Option<Value> {
    let mut schema = tool.raw_tool.input_schema.as_ref().clone();
    match schema.get("type") {
        Some(Value::String(value)) if value == "object" => {}
        Some(_) => return None,
        None => {
            schema.insert("type".to_string(), Value::String("object".to_string()));
        }
    }

    match schema.get("properties") {
        Some(Value::Object(_)) => {}
        Some(_) => return None,
        None => {
            schema.insert("properties".to_string(), Value::Object(Map::new()));
        }
    }

    match schema.get("required") {
        Some(Value::Array(_)) => {}
        Some(_) => return None,
        None => {
            schema.insert("required".to_string(), Value::Array(Vec::new()));
        }
    }

    Some(Value::Object(schema))
}

fn tool_description(server_name: &str, tool: &McpToolSnapshot) -> String {
    let body = tool
        .description
        .as_deref()
        .or(tool.title.as_deref())
        .unwrap_or("MCP tool");
    let raw_name = truncate_chars(&tool.raw_name, 256).0;
    let server_name = truncate_chars(server_name, 256).0;
    let body = truncate_chars(body, MAX_MCP_TOOL_DESCRIPTION_CHARS).0;
    format!(
        "MCP tool from server '{server_name}' raw tool '{}'. Users may refer to this tool by the raw tool name without the MCP prefix. {body}",
        raw_name
    )
}

fn bounded_tool_result_value(value: Value, is_error: bool) -> Value {
    let Ok(raw) = serde_json::to_string(&value) else {
        return value;
    };
    if raw.chars().count() <= MAX_MCP_TOOL_RESULT_JSON_CHARS {
        return value;
    }
    let (preview, _) = truncate_chars(&raw, MAX_MCP_TOOL_RESULT_PREVIEW_CHARS);
    json!({
        "is_error": is_error,
        "truncated": true,
        "content": [{
            "type": "text",
            "text": format!("MCP tool result exceeded {MAX_MCP_TOOL_RESULT_JSON_CHARS} JSON characters and was truncated. JSON prefix: {preview}")
        }],
        "structured_content": {},
        "meta": {
            "acn_truncated": true,
            "max_json_chars": MAX_MCP_TOOL_RESULT_JSON_CHARS
        }
    })
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    if value.chars().count() <= max_chars {
        return (value.to_string(), false);
    }
    let mut out = value.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    (out, true)
}

fn redact_value_strings(value: &mut Value) {
    match value {
        Value::String(text) => {
            *text = redact_mcp_sensitive_text(text);
        }
        Value::Array(items) => {
            for item in items {
                redact_value_strings(item);
            }
        }
        Value::Object(map) => {
            let original = std::mem::take(map);
            for (key, mut item) in original {
                redact_value_strings(&mut item);
                map.insert(redact_mcp_sensitive_text(&key), item);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::{Tool, ToolAnnotations};

    use super::*;
    use crate::mcp::config::McpServerConfig;
    use crate::mcp::connection_manager::{McpServerSnapshot, McpToolExposure};

    #[test]
    fn catalog_exposes_ready_tools_with_visible_names() {
        let snapshot = runtime_snapshot(vec![tool_snapshot(
            "ask",
            json!({"type": "object", "properties": {"q": {"type": "string"}}}),
            McpToolExposure::Exposed,
        )]);

        let catalog = tool_catalog(&snapshot);

        assert_eq!(catalog.definitions().len(), 1);
        assert_eq!(catalog.definitions()[0].visible_name, "mcp__pal__ask");
        assert!(catalog.route("mcp__pal__ask").is_some());
    }

    #[test]
    fn catalog_skips_non_ready_and_non_exposed_tools() {
        let mut snapshot = runtime_snapshot(vec![
            tool_snapshot("ask", json!({"type": "object"}), McpToolExposure::Exposed),
            tool_snapshot(
                "hidden",
                json!({"type": "object"}),
                McpToolExposure::Filtered {
                    reason: crate::mcp::connection_manager::McpToolFilterReason::DisabledTools,
                },
            ),
        ]);
        snapshot.servers.get_mut("pal").unwrap().status = McpServerStatus::Failed;

        let catalog = tool_catalog(&snapshot);

        assert!(catalog.definitions().is_empty());
    }

    #[test]
    fn visible_tool_read_only_requires_an_explicit_true_hint() {
        let snapshot = runtime_snapshot(vec![
            tool_snapshot_with_read_only_hint(
                "read",
                json!({"type": "object"}),
                McpToolExposure::Exposed,
                Some(true),
            ),
            tool_snapshot_with_read_only_hint(
                "write",
                json!({"type": "object"}),
                McpToolExposure::Exposed,
                Some(false),
            ),
            tool_snapshot_with_read_only_hint(
                "unknown",
                json!({"type": "object"}),
                McpToolExposure::Exposed,
                None,
            ),
        ]);

        assert!(visible_tool_is_read_only(&snapshot, "mcp__pal__read"));
        assert!(!visible_tool_is_read_only(&snapshot, "mcp__pal__write"));
        assert!(!visible_tool_is_read_only(&snapshot, "mcp__pal__unknown"));
    }

    #[test]
    fn malformed_read_only_hint_cannot_enter_the_runtime_catalog() {
        let malformed = serde_json::from_value::<Tool>(json!({
            "name": "read",
            "inputSchema": {"type": "object"},
            "annotations": {"readOnlyHint": "true"}
        }));

        assert!(malformed.is_err());
    }

    #[test]
    fn visible_tool_read_only_fails_closed_when_no_visible_route_exists() {
        let snapshot = runtime_snapshot(vec![
            tool_snapshot_with_read_only_hint(
                "hidden",
                json!({"type": "object"}),
                McpToolExposure::Filtered {
                    reason: crate::mcp::connection_manager::McpToolFilterReason::DisabledTools,
                },
                Some(true),
            ),
            tool_snapshot_with_read_only_hint(
                "invalid-schema",
                json!({"type": "array"}),
                McpToolExposure::Exposed,
                Some(true),
            ),
        ]);

        assert!(!visible_tool_is_read_only(&snapshot, "mcp__pal__hidden"));
        assert!(!visible_tool_is_read_only(
            &snapshot,
            "mcp__pal__invalid-schema"
        ));
        assert!(!visible_tool_is_read_only(
            &snapshot,
            "mcp__pal__not-present"
        ));
    }

    #[test]
    fn visible_tool_read_only_uses_catalog_route_for_colliding_visible_names() {
        let snapshot = runtime_snapshot(vec![
            tool_snapshot_with_read_only_hint(
                "list.issues",
                json!({"type": "object"}),
                McpToolExposure::Exposed,
                Some(true),
            ),
            tool_snapshot_with_read_only_hint(
                "list/issues",
                json!({"type": "object"}),
                McpToolExposure::Exposed,
                Some(false),
            ),
        ]);
        let catalog = tool_catalog(&snapshot);
        let read_name = catalog
            .visible_name_for("pal", "list.issues")
            .expect("visible read tool")
            .to_string();
        let write_name = catalog
            .visible_name_for("pal", "list/issues")
            .expect("visible write tool")
            .to_string();

        assert_ne!(read_name, write_name);
        assert!(read_name.starts_with("mcp__pal__list_issues__"));
        assert!(write_name.starts_with("mcp__pal__list_issues__"));
        assert!(visible_tool_is_read_only(&snapshot, &read_name));
        assert!(!visible_tool_is_read_only(&snapshot, &write_name));
    }

    #[test]
    fn catalog_normalizes_no_param_schema_to_object_schema() {
        let snapshot = runtime_snapshot(vec![tool_snapshot(
            "ping",
            json!({}),
            McpToolExposure::Exposed,
        )]);

        let catalog = tool_catalog(&snapshot);
        let schema = &catalog.definitions()[0].input_schema;

        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"], json!({}));
        assert_eq!(schema["required"], json!([]));
    }

    #[test]
    fn catalog_truncates_long_descriptions() {
        let snapshot = runtime_snapshot(vec![tool_snapshot(
            "ask",
            json!({"type": "object"}),
            McpToolExposure::Exposed,
        )]);
        let mut snapshot = snapshot;
        snapshot.servers.get_mut("pal").unwrap().tools[0].description =
            Some("x".repeat(MAX_MCP_TOOL_DESCRIPTION_CHARS + 100));

        let catalog = tool_catalog(&snapshot);

        assert!(catalog.definitions()[0].description.len() < MAX_MCP_TOOL_DESCRIPTION_CHARS + 400);
        assert!(catalog.definitions()[0].description.ends_with("..."));
    }

    #[test]
    fn mcp_tool_result_is_bounded() {
        let result = CallToolResult::success(vec![rmcp::model::ContentBlock::text(
            "x".repeat(MAX_MCP_TOOL_RESULT_JSON_CHARS + 100),
        )]);

        let value = mcp_tool_result_to_value(&result);
        let raw = serde_json::to_string(&value).unwrap();

        assert_eq!(value["truncated"], true);
        assert!(raw.chars().count() < MAX_MCP_TOOL_RESULT_JSON_CHARS);
    }

    #[test]
    fn mcp_error_tool_result_is_redacted_before_model_visibility() {
        let mut result = CallToolResult::error(vec![rmcp::model::ContentBlock::text(
            "Authorization: Bearer secret-token url=https://user:pass@example.test/mcp?token=abc",
        )]);
        result.structured_content = Some(json!({
            "SERVICE_API_KEY=fixture-secret": "SERVICE_API_KEY=fixture-secret"
        }));

        let value = mcp_tool_result_to_value(&result);
        let raw = serde_json::to_string(&value).unwrap();

        assert_eq!(value["is_error"], true);
        assert!(raw.contains("<redacted>"));
        assert!(!raw.contains("secret-token"));
        assert!(!raw.contains("user:pass"));
        assert!(!raw.contains("token=abc"));
        assert!(!raw.contains("SERVICE_API_KEY=fixture-secret"));
    }

    fn runtime_snapshot(tools: Vec<McpToolSnapshot>) -> McpRuntimeState {
        let server = McpServerSnapshot {
            name: "pal".into(),
            config: McpServerConfig::streamable_http("https://example.test/mcp".into(), None),
            transport: None,
            status: McpServerStatus::Ready,
            tools,
            server_info: None,
            last_connected_at: None,
            last_error: None,
            stderr_excerpt: None,
        };
        McpRuntimeState {
            servers: BTreeMap::from([("pal".to_string(), server)]),
            startup_error: None,
            workspace_root: None,
        }
    }

    fn tool_snapshot(
        name: &'static str,
        schema: Value,
        exposure: McpToolExposure,
    ) -> McpToolSnapshot {
        McpToolSnapshot {
            raw_name: name.to_string(),
            title: None,
            description: Some("Test tool".to_string()),
            exposure,
            raw_tool: Tool::new(
                name,
                "Test tool",
                Arc::new(schema.as_object().cloned().unwrap_or_default()),
            ),
        }
    }

    fn tool_snapshot_with_read_only_hint(
        name: &'static str,
        schema: Value,
        exposure: McpToolExposure,
        read_only_hint: Option<bool>,
    ) -> McpToolSnapshot {
        let mut snapshot = tool_snapshot(name, schema, exposure);
        snapshot.raw_tool.annotations =
            read_only_hint.map(|value| ToolAnnotations::new().read_only(value));
        snapshot
    }
}
