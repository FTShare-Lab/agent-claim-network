//! provider-neutral 结构化 JSON 调用辅助。
//!
//! 本模块用于 internalize/finalize 这类“只要 JSON、不要工具”的 LLM 调用。
//! 它复用 `ProviderAdapter::send`，并集中处理纯文本提取、fence 剥离与 JSON parse retry。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use serde_json::Value;

use crate::api::{
    ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
    SessionTurnContentBlock, SessionTurnMessage,
};

const STRUCTURED_JSON_RETRY_RAW_MAX_CHARS: usize = 4000;

/// 通过 provider-neutral 接口生成结构化 JSON。
pub struct StructuredJsonCaller {
    provider: Arc<dyn ProviderAdapter>,
    max_tokens: u32,
    retry_count: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
}

pub(crate) struct StructuredJsonAttemptRequest {
    system_prompt: String,
    messages: Vec<SessionTurnMessage>,
    provider_retry_count_override: Option<u32>,
}

impl StructuredJsonAttemptRequest {
    fn standard(system_prompt: String, messages: Vec<SessionTurnMessage>) -> Self {
        Self {
            system_prompt,
            messages,
            provider_retry_count_override: None,
        }
    }

    pub(crate) fn compaction(system_prompt: String, messages: Vec<SessionTurnMessage>) -> Self {
        Self {
            system_prompt,
            messages,
            provider_retry_count_override: Some(0),
        }
    }
}

/// 单次结构化 JSON provider attempt 的观测信息。
#[derive(Debug, Clone)]
pub struct StructuredJsonAttemptReport {
    pub attempt: u32,
    pub retry_total: u32,
    pub raw_text: Option<String>,
    pub parsed_json: Option<Value>,
    pub error: Option<String>,
    pub will_retry: bool,
}

impl StructuredJsonCaller {
    /// 创建结构化 JSON 调用器。
    pub fn new(
        provider: Arc<dyn ProviderAdapter>,
        max_tokens: u32,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Self {
        Self {
            provider,
            max_tokens,
            retry_count,
            retry_base_delay,
            retry_max_delay,
        }
    }

    pub(crate) fn max_tokens(&self) -> u32 {
        self.max_tokens
    }

    pub(crate) fn max_attempts(&self) -> u32 {
        self.retry_count.saturating_add(1)
    }

    /// 请求模型生成 JSON，并在 JSON 解析失败时按配置重试。
    pub async fn generate_json(
        &self,
        system_prompt: String,
        messages: Vec<SessionTurnMessage>,
    ) -> anyhow::Result<Value> {
        self.generate_json_with_retry_notice(system_prompt, messages, |_, _, _| {})
            .await
    }

    /// 请求模型生成 JSON，并在每次 retry 前通知调用方。
    pub async fn generate_json_with_retry_notice<F>(
        &self,
        system_prompt: String,
        messages: Vec<SessionTurnMessage>,
        mut on_retry: F,
    ) -> anyhow::Result<Value>
    where
        F: FnMut(u32, u32, &anyhow::Error),
    {
        let mut attempt = 0;
        let base_messages = messages;
        let mut attempt_messages = base_messages.clone();
        loop {
            match self
                .generate_json_once(system_prompt.clone(), attempt_messages.clone())
                .await
            {
                Ok(value) => return Ok(value),
                Err(JsonCallError::Provider(error) | JsonCallError::Terminal(error)) => {
                    return Err(error);
                }
                Err(JsonCallError::Parse(error) | JsonCallError::RetryableShape(error))
                    if attempt >= self.retry_count =>
                {
                    return Err(error);
                }
                Err(JsonCallError::Parse(error) | JsonCallError::RetryableShape(error)) => {
                    let error_text = error.to_string();
                    on_retry(attempt + 1, self.retry_count, &error);
                    attempt_messages =
                        structured_json_retry_messages(&base_messages, None, &error_text);
                    let delay = self.retry_delay(attempt);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    attempt += 1;
                }
            }
        }
    }

    /// 请求模型生成 JSON，并对解析后的业务 shape 做同一 retry budget 下的校验。
    pub async fn generate_json_validated_with_retry_notice<T, V, F>(
        &self,
        system_prompt: String,
        messages: Vec<SessionTurnMessage>,
        validate: V,
        on_retry: F,
    ) -> anyhow::Result<T>
    where
        V: FnMut(Value) -> anyhow::Result<T>,
        F: FnMut(u32, u32, &anyhow::Error),
    {
        self.generate_json_validated_with_attempt_notice(
            system_prompt,
            messages,
            validate,
            on_retry,
            |_| std::future::ready(()),
        )
        .await
    }

    /// 请求模型生成 JSON，同时回传每次 provider attempt 的 raw/parsed/error 观测信息。
    pub async fn generate_json_validated_with_attempt_notice<T, V, F, A, AFut>(
        &self,
        system_prompt: String,
        messages: Vec<SessionTurnMessage>,
        validate: V,
        on_retry: F,
        on_attempt: A,
    ) -> anyhow::Result<T>
    where
        V: FnMut(Value) -> anyhow::Result<T>,
        F: FnMut(u32, u32, &anyhow::Error),
        A: FnMut(StructuredJsonAttemptReport) -> AFut,
        AFut: std::future::Future<Output = ()>,
    {
        self.generate_json_validated_with_guarded_attempts(
            StructuredJsonAttemptRequest::standard(system_prompt, messages),
            validate,
            on_retry,
            on_attempt,
            |_, _| Ok(()),
        )
        .await
    }

    /// 与普通结构化调用相同，但在每次真实 provider attempt 前检查最终请求。
    /// compaction 使用它覆盖 retry 追加纠错消息后的预算，其他调用保持原行为。
    pub(crate) async fn generate_json_validated_with_guarded_attempts<T, V, F, A, AFut, G>(
        &self,
        request: StructuredJsonAttemptRequest,
        mut validate: V,
        mut on_retry: F,
        mut on_attempt: A,
        mut before_attempt: G,
    ) -> anyhow::Result<T>
    where
        V: FnMut(Value) -> anyhow::Result<T>,
        F: FnMut(u32, u32, &anyhow::Error),
        A: FnMut(StructuredJsonAttemptReport) -> AFut,
        AFut: std::future::Future<Output = ()>,
        G: FnMut(&str, &[SessionTurnMessage]) -> anyhow::Result<()>,
    {
        let StructuredJsonAttemptRequest {
            system_prompt,
            messages,
            provider_retry_count_override,
        } = request;
        let mut attempt = 0;
        let base_messages = messages;
        let mut attempt_messages = base_messages.clone();
        loop {
            before_attempt(&system_prompt, &attempt_messages)?;
            match self
                .generate_json_once_observed(
                    system_prompt.clone(),
                    attempt_messages.clone(),
                    provider_retry_count_override,
                )
                .await
            {
                Ok(parsed) => match validate(parsed.value.clone()) {
                    Ok(outcome) => {
                        on_attempt(StructuredJsonAttemptReport {
                            attempt: attempt + 1,
                            retry_total: self.retry_count,
                            raw_text: parsed.raw_text,
                            parsed_json: Some(parsed.value),
                            error: None,
                            will_retry: false,
                        })
                        .await;
                        return Ok(outcome);
                    }
                    Err(error) => {
                        let will_retry = attempt < self.retry_count;
                        let raw_text = parsed.raw_text.clone();
                        let error_text = error.to_string();
                        on_attempt(StructuredJsonAttemptReport {
                            attempt: attempt + 1,
                            retry_total: self.retry_count,
                            raw_text,
                            parsed_json: Some(parsed.value),
                            error: Some(error_text.clone()),
                            will_retry,
                        })
                        .await;
                        if !will_retry {
                            return Err(error);
                        }
                        on_retry(attempt + 1, self.retry_count, &error);
                        attempt_messages = structured_json_retry_messages(
                            &base_messages,
                            parsed.raw_text.as_deref(),
                            &error_text,
                        );
                        let delay = self.retry_delay(attempt);
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        attempt += 1;
                    }
                },
                Err(failure) => {
                    let retryable = matches!(
                        failure.error,
                        JsonCallError::Parse(_) | JsonCallError::RetryableShape(_)
                    ) || (provider_retry_count_override.is_some()
                        && matches!(failure.error, JsonCallError::Provider(_)));
                    let will_retry = retryable && attempt < self.retry_count;
                    let raw_text = failure.raw_text.clone();
                    let parsed_json = failure.parsed_json.clone();
                    let error_text = failure.error.to_string();
                    on_attempt(StructuredJsonAttemptReport {
                        attempt: attempt + 1,
                        retry_total: self.retry_count,
                        raw_text: raw_text.clone(),
                        parsed_json,
                        error: Some(error_text.clone()),
                        will_retry,
                    })
                    .await;
                    match failure.error {
                        JsonCallError::Provider(error) if will_retry => {
                            on_retry(attempt + 1, self.retry_count, &error);
                            // Transport/provider 失败没有模型输出，重试同一份最终请求；
                            // parse/shape 失败才追加纠错消息。
                            let delay = self.retry_delay(attempt);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            attempt += 1;
                        }
                        JsonCallError::Provider(error) | JsonCallError::Terminal(error) => {
                            return Err(error);
                        }
                        JsonCallError::Parse(error) | JsonCallError::RetryableShape(error)
                            if !will_retry =>
                        {
                            return Err(error);
                        }
                        JsonCallError::Parse(error) | JsonCallError::RetryableShape(error) => {
                            on_retry(attempt + 1, self.retry_count, &error);
                            attempt_messages = structured_json_retry_messages(
                                &base_messages,
                                raw_text.as_deref(),
                                &error_text,
                            );
                            let delay = self.retry_delay(attempt);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            attempt += 1;
                        }
                    }
                }
            }
        }
    }

    async fn generate_json_once(
        &self,
        system_prompt: String,
        messages: Vec<SessionTurnMessage>,
    ) -> Result<Value, JsonCallError> {
        let request = ProviderRequest {
            system_prompt,
            messages,
            tools: Vec::new(),
            max_tokens: self.max_tokens,
            stream: false,
            retry_count_override: None,
        };

        let mut emit = |_event: ProviderEvent| {};
        let response = self
            .provider
            .send(request, &mut emit)
            .await
            .map_err(JsonCallError::Provider)?;

        parse_structured_response(response)
    }

    async fn generate_json_once_observed(
        &self,
        system_prompt: String,
        messages: Vec<SessionTurnMessage>,
        retry_count_override: Option<u32>,
    ) -> Result<JsonCallParsed, JsonCallFailure> {
        let request = ProviderRequest {
            system_prompt,
            messages,
            tools: Vec::new(),
            max_tokens: self.max_tokens,
            stream: false,
            retry_count_override,
        };

        let mut emit = |_event: ProviderEvent| {};
        let response = self
            .provider
            .send(request, &mut emit)
            .await
            .map_err(|error| JsonCallFailure::new(JsonCallError::Provider(error), None, None))?;

        parse_structured_response_observed(response)
    }

    fn retry_delay(&self, attempt: u32) -> Duration {
        let multiplier = if attempt >= u32::BITS {
            u32::MAX
        } else {
            1_u32 << attempt
        };
        self.retry_base_delay
            .saturating_mul(multiplier)
            .min(self.retry_max_delay)
    }
}

enum JsonCallError {
    Provider(anyhow::Error),
    Terminal(anyhow::Error),
    RetryableShape(anyhow::Error),
    Parse(anyhow::Error),
}

impl std::fmt::Display for JsonCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(error)
            | Self::Terminal(error)
            | Self::RetryableShape(error)
            | Self::Parse(error) => write!(f, "{error}"),
        }
    }
}

struct JsonCallParsed {
    raw_text: Option<String>,
    value: Value,
}

struct JsonCallFailure {
    error: JsonCallError,
    raw_text: Option<String>,
    parsed_json: Option<Value>,
}

impl JsonCallFailure {
    fn new(error: JsonCallError, raw_text: Option<String>, parsed_json: Option<Value>) -> Self {
        Self {
            error,
            raw_text,
            parsed_json,
        }
    }
}

fn parse_structured_response(response: ProviderResponse) -> Result<Value, JsonCallError> {
    parse_structured_response_observed(response)
        .map(|parsed| parsed.value)
        .map_err(|failure| failure.error)
}

fn parse_structured_response_observed(
    response: ProviderResponse,
) -> Result<JsonCallParsed, JsonCallFailure> {
    let raw_text = raw_text_for_audit(&response.assistant_message);
    if response.stop == ProviderStop::MaxTokens {
        return Err(JsonCallFailure::new(
            JsonCallError::Terminal(anyhow::anyhow!("结构化 JSON 调用命中 MaxTokens")),
            raw_text,
            None,
        ));
    }
    if response.stop == ProviderStop::ToolUse {
        return Err(JsonCallFailure::new(
            JsonCallError::Terminal(terminal_text_shape_error(
                &response,
                "结构化 JSON 调用收到 ToolUse response",
            )),
            raw_text,
            None,
        ));
    }
    if response
        .assistant_message
        .content
        .iter()
        .any(is_terminal_structured_json_block)
    {
        return Err(JsonCallFailure::new(
            JsonCallError::Terminal(terminal_text_shape_error(
                &response,
                "结构化 JSON 调用收到非 Text block",
            )),
            raw_text,
            None,
        ));
    }

    let text = match structured_text_from_message(&response.assistant_message) {
        Ok(text) => text,
        Err(error) => {
            return Err(JsonCallFailure::new(
                JsonCallError::RetryableShape(error),
                raw_text,
                None,
            ));
        }
    };

    if response.stop != ProviderStop::Done {
        return Err(JsonCallFailure::new(
            JsonCallError::RetryableShape(anyhow::anyhow!(
                "结构化 JSON 调用返回非 Done stop: {:?}",
                response.stop
            )),
            Some(text),
            None,
        ));
    }

    match serde_json::from_str(strip_code_fence(&text)).context("解析结构化 JSON 响应失败")
    {
        Ok(value) => Ok(JsonCallParsed {
            raw_text: Some(text),
            value,
        }),
        Err(error) => Err(JsonCallFailure::new(
            JsonCallError::Parse(error),
            Some(text),
            None,
        )),
    }
}

fn terminal_text_shape_error(response: &ProviderResponse, fallback: &'static str) -> anyhow::Error {
    structured_text_from_message(&response.assistant_message)
        .map(|_| anyhow::anyhow!(fallback))
        .unwrap_or_else(|error| error)
}

fn is_terminal_structured_json_block(block: &SessionTurnContentBlock) -> bool {
    matches!(
        block,
        SessionTurnContentBlock::Image { .. }
            | SessionTurnContentBlock::Document { .. }
            | SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. }
    )
}

fn structured_text_from_message(message: &SessionTurnMessage) -> anyhow::Result<String> {
    if message.role != "assistant" {
        anyhow::bail!("provider response role 必须是 assistant: {}", message.role);
    }

    let mut text = String::new();
    for block in &message.content {
        match block {
            SessionTurnContentBlock::Text { text: part } => text.push_str(part),
            SessionTurnContentBlock::SkillInstructions { .. } => {
                anyhow::bail!("结构化文本响应不能包含 SkillInstructions block");
            }
            SessionTurnContentBlock::Image { .. } | SessionTurnContentBlock::Document { .. } => {
                anyhow::bail!("结构化文本响应不能包含附件 block");
            }
            SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => {
                anyhow::bail!("结构化文本响应只能包含 Text block");
            }
        }
    }
    Ok(text)
}

fn raw_text_for_audit(message: &SessionTurnMessage) -> Option<String> {
    if message.role != "assistant" {
        return None;
    }
    let mut text = String::new();
    for block in &message.content {
        if let SessionTurnContentBlock::Text { text: part } = block {
            text.push_str(part);
        }
    }
    Some(text)
}

fn strip_code_fence(text: &str) -> &str {
    let trimmed = text.trim();
    if let Some(rest) = strip_json_fence_prefix(trimmed) {
        rest.trim_start().trim_end().trim_end_matches("```").trim()
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        rest.trim_start().trim_end().trim_end_matches("```").trim()
    } else {
        trimmed
    }
}

fn strip_json_fence_prefix(trimmed: &str) -> Option<&str> {
    let rest = trimmed.strip_prefix("```")?;
    let tag = rest.get(..4)?;
    if tag.eq_ignore_ascii_case("json") {
        Some(&rest[4..])
    } else {
        None
    }
}

fn structured_json_retry_messages(
    base_messages: &[SessionTurnMessage],
    raw_text: Option<&str>,
    error_text: &str,
) -> Vec<SessionTurnMessage> {
    let mut messages = base_messages.to_vec();
    let mut correction = String::from(
        "Your previous response could not be accepted as strict JSON.\n\n\
Error:\n",
    );
    correction.push_str(error_text);
    if let Some(raw_text) = raw_text.map(str::trim).filter(|text| !text.is_empty()) {
        correction.push_str("\n\nPrevious response preview:\n<previous_response>\n");
        correction.push_str(&truncate_retry_raw_text(
            raw_text,
            STRUCTURED_JSON_RETRY_RAW_MAX_CHARS,
        ));
        correction.push_str("\n</previous_response>");
    }
    correction.push_str(
        "\n\nReturn the corrected answer now as exactly one valid JSON object. \
Do not use markdown or code fences. Escape every double quote inside string values as \\\". \
Keep the required schema and omit any extra explanation.",
    );
    messages.push(SessionTurnMessage::user_text(correction));
    messages
}

fn truncate_retry_raw_text(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index >= max_chars {
            out.push_str("\n[truncated]");
            return out;
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use serde_json::json;
    use tokio::sync::Mutex;

    use crate::api::{
        ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
        SessionTurnContentBlock, SessionTurnMessage, StructuredJsonAttemptRequest,
        StructuredJsonCaller,
    };

    struct FakeProvider {
        responses: Mutex<VecDeque<anyhow::Result<ProviderResponse>>>,
        requests: Mutex<Vec<ProviderRequest>>,
    }

    impl FakeProvider {
        fn new(responses: Vec<anyhow::Result<ProviderResponse>>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ProviderAdapter for FakeProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            self.responses
                .lock()
                .await
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("fake provider response exhausted"))?
        }
    }

    fn caller(provider: Arc<dyn ProviderAdapter>) -> StructuredJsonCaller {
        StructuredJsonCaller::new(provider, 512, 1, Duration::ZERO, Duration::from_millis(1))
    }

    fn text_response(text: &str) -> anyhow::Result<ProviderResponse> {
        Ok(ProviderResponse {
            assistant_message: SessionTurnMessage::assistant_text(text),
            stop: ProviderStop::Done,
        })
    }

    fn block_response(
        content: Vec<SessionTurnContentBlock>,
        stop: ProviderStop,
    ) -> anyhow::Result<ProviderResponse> {
        Ok(ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: "assistant".into(),
                content,
            },
            stop,
        })
    }

    fn role_response(role: &str, text: &str) -> anyhow::Result<ProviderResponse> {
        Ok(ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: role.into(),
                content: vec![SessionTurnContentBlock::text(text)],
            },
            stop: ProviderStop::Done,
        })
    }

    #[tokio::test]
    async fn generate_json_parses_plain_json_text() {
        let provider = Arc::new(FakeProvider::new(vec![text_response(r#"{"ok":true}"#)]));
        let caller = caller(provider.clone());

        let value = caller
            .generate_json(
                "system".into(),
                vec![SessionTurnMessage::user_text("payload")],
            )
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].system_prompt, "system");
        assert_eq!(
            requests[0].messages,
            vec![SessionTurnMessage::user_text("payload")]
        );
        assert!(requests[0].tools.is_empty());
        assert_eq!(requests[0].max_tokens, 512);
        assert!(!requests[0].stream);
    }

    #[tokio::test]
    async fn generate_json_strips_json_code_fence() {
        let provider = Arc::new(FakeProvider::new(vec![text_response(
            "```json\n{\"ok\":true}\n```",
        )]));
        let caller = caller(provider);

        let value = caller.generate_json("system".into(), vec![]).await.unwrap();

        assert_eq!(value, json!({"ok": true}));
    }

    #[tokio::test]
    async fn generate_json_retries_parse_failure() {
        let provider = Arc::new(FakeProvider::new(vec![
            text_response("{not json"),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller = caller(provider.clone());

        let value = caller.generate_json("system".into(), vec![]).await.unwrap();

        assert_eq!(value, json!({"ok": true}));
        assert_eq!(provider.requests.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn generate_json_retry_request_includes_correction_context() {
        let provider = Arc::new(FakeProvider::new(vec![
            text_response(r#"{"summary":"quoted "bad" text"}"#),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller = caller(provider.clone());

        let value = caller
            .generate_json_validated_with_attempt_notice(
                "system".into(),
                vec![SessionTurnMessage::user_text("payload")],
                Ok,
                |_, _, _| {},
                |_| std::future::ready(()),
            )
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].messages.len(), 2);
        let retry_text = match &requests[1].messages[1].content[0] {
            SessionTurnContentBlock::Text { text } => text,
            _ => panic!("retry correction must be text"),
        };
        assert!(retry_text.contains("could not be accepted as strict JSON"));
        assert!(retry_text.contains("Previous response preview"));
        assert!(retry_text.contains(r#"quoted "bad" text"#));
        assert!(retry_text.contains("Escape every double quote"));
    }

    #[tokio::test]
    async fn guarded_attempts_recheck_retry_messages_before_provider_call() {
        let provider = Arc::new(FakeProvider::new(vec![
            text_response("{not json"),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller = caller(provider.clone());
        let mut checked_message_counts = Vec::new();

        let error = caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::compaction(
                    "system".into(),
                    vec![SessionTurnMessage::user_text("payload")],
                ),
                Ok,
                |_, _, _| {},
                |_| std::future::ready(()),
                |_, messages| {
                    checked_message_counts.push(messages.len());
                    if messages.len() > 1 {
                        anyhow::bail!("retry request exceeds local budget");
                    }
                    Ok(())
                },
            )
            .await
            .expect_err("retry guard should reject the expanded correction request");

        assert!(error
            .to_string()
            .contains("retry request exceeds local budget"));
        assert_eq!(checked_message_counts, vec![1, 2]);
        assert_eq!(provider.requests.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn guarded_attempts_share_one_budget_across_transport_and_parse_failures() {
        let provider = Arc::new(FakeProvider::new(vec![
            Err(anyhow::anyhow!("transport failed")),
            text_response("{not json"),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller =
            StructuredJsonCaller::new(provider.clone(), 512, 2, Duration::ZERO, Duration::ZERO);

        let value = caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::compaction(
                    "system".into(),
                    vec![SessionTurnMessage::user_text("payload")],
                ),
                Ok,
                |_, _, _| {},
                |_| std::future::ready(()),
                |_, _| Ok(()),
            )
            .await
            .expect("third and final shared-budget attempt should succeed");

        assert_eq!(value, json!({"ok": true}));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].messages.len(), 1);
        assert_eq!(requests[1].messages.len(), 1);
        assert_eq!(requests[2].messages.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.retry_count_override == Some(0)));
    }

    #[tokio::test]
    async fn guarded_transport_retry_preserves_parse_correction_messages() {
        let provider = Arc::new(FakeProvider::new(vec![
            text_response("{not json"),
            Err(anyhow::anyhow!("transport failed")),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller =
            StructuredJsonCaller::new(provider.clone(), 512, 2, Duration::ZERO, Duration::ZERO);

        caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::compaction(
                    "system".into(),
                    vec![SessionTurnMessage::user_text("payload")],
                ),
                Ok,
                |_, _, _| {},
                |_| std::future::ready(()),
                |_, _| Ok(()),
            )
            .await
            .expect("transport retry of the correction request should succeed");

        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].messages.len(), 1);
        assert_eq!(requests[1].messages.len(), 2);
        assert_eq!(requests[2].messages, requests[1].messages);
    }

    #[tokio::test]
    async fn generate_json_notifies_before_retry() {
        let provider = Arc::new(FakeProvider::new(vec![
            text_response("{not json"),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller = caller(provider);
        let mut notices = Vec::new();

        let value = caller
            .generate_json_with_retry_notice("system".into(), vec![], |retry, total, error| {
                notices.push((retry, total, error.to_string()));
            })
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].0, 1);
        assert_eq!(notices[0].1, 1);
        assert!(notices[0].2.contains("解析结构化 JSON 响应失败"));
    }

    #[tokio::test]
    async fn generate_json_attempt_notice_includes_raw_text_and_retry_state() {
        let provider = Arc::new(FakeProvider::new(vec![
            text_response("{not json"),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller = caller(provider);
        let mut attempts = Vec::new();

        let value = caller
            .generate_json_validated_with_attempt_notice(
                "system".into(),
                vec![],
                Ok,
                |_, _, _| {},
                |attempt| {
                    attempts.push(attempt);
                    std::future::ready(())
                },
            )
            .await
            .unwrap();

        assert_eq!(value, json!({"ok": true}));
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].attempt, 1);
        assert_eq!(attempts[0].raw_text.as_deref(), Some("{not json"));
        assert!(attempts[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("解析结构化 JSON 响应失败")));
        assert!(attempts[0].will_retry);
        assert_eq!(attempts[1].attempt, 2);
        assert_eq!(attempts[1].parsed_json, Some(json!({"ok": true})));
        assert!(attempts[1].error.is_none());
        assert!(!attempts[1].will_retry);
    }

    #[tokio::test]
    async fn generate_json_retries_output_shape_failure() {
        let provider = Arc::new(FakeProvider::new(vec![
            role_response("user", r#"{"ok":false}"#),
            text_response(r#"{"ok":true}"#),
        ]));
        let caller = caller(provider.clone());

        let value = caller.generate_json("system".into(), vec![]).await.unwrap();

        assert_eq!(value, json!({"ok": true}));
        assert_eq!(provider.requests.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn generate_json_rejects_tool_use() {
        let provider = Arc::new(FakeProvider::new(vec![block_response(
            vec![SessionTurnContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "working_note".into(),
                input: json!({}),
            }],
            ProviderStop::ToolUse,
        )]));
        let caller = caller(provider);

        let err = caller
            .generate_json("system".into(), vec![])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("只能包含 Text block"));
    }

    #[tokio::test]
    async fn generate_json_rejects_max_tokens() {
        let provider = Arc::new(FakeProvider::new(vec![block_response(
            vec![SessionTurnContentBlock::text("{}")],
            ProviderStop::MaxTokens,
        )]));
        let caller = caller(provider);

        let err = caller
            .generate_json("system".into(), vec![])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("MaxTokens"));
    }
}
