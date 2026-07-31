//! LLM 调用 span 的辅助函数：从 Anthropic 响应 JSON 中提取 usage 信息。

use opentelemetry::trace::Span;
use opentelemetry::KeyValue;
use serde_json::Value;

/// 从 Anthropic Messages API 的 JSON 响应中提取 usage 并设置到当前 span。
///
/// 响应格式示例：
/// ```json
/// { "usage": { "input_tokens": 123, "output_tokens": 456 }, "stop_reason": "end_turn" }
/// ```
///
/// 提取失败时静默跳过（不影响业务）。
pub fn record_llm_response<S: Span>(span: &mut S, response: &Value) {
    if let Some(usage) = response.get("usage") {
        if let Some(v) = usage.get("input_tokens").and_then(Value::as_i64) {
            span.set_attribute(KeyValue::new("llm.input_tokens", v));
        }
        if let Some(v) = usage.get("output_tokens").and_then(Value::as_i64) {
            span.set_attribute(KeyValue::new("llm.output_tokens", v));
        }
    }
    if let Some(v) = response.get("stop_reason").and_then(Value::as_str) {
        span.set_attribute(KeyValue::new("llm.stop_reason", v.to_string()));
    }
}
