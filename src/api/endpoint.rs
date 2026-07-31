//! LLM HTTP endpoint 的统一解析与请求路径补全。

use reqwest::Url;

#[derive(Debug, Clone, Copy)]
pub(super) enum LlmEndpointKind {
    OpenAiChatCompletions,
    AnthropicMessages,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum LlmEndpointError {
    #[error("LLM endpoint 不能为空")]
    Empty,
    #[error("LLM endpoint 不是有效的绝对 URL: {0}")]
    InvalidUrl(String),
    #[error("LLM endpoint 只支持 http/https，实际 scheme 为 '{0}'")]
    UnsupportedScheme(String),
    #[error("LLM endpoint 不能包含 URL fragment")]
    FragmentNotAllowed,
    #[error("LLM endpoint 无法追加请求路径")]
    CannotAppendPath,
}

pub(super) fn resolve_llm_endpoint(
    endpoint: &str,
    kind: LlmEndpointKind,
) -> Result<String, LlmEndpointError> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(LlmEndpointError::Empty);
    }

    let mut url =
        Url::parse(endpoint).map_err(|error| LlmEndpointError::InvalidUrl(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(LlmEndpointError::UnsupportedScheme(
            url.scheme().to_string(),
        ));
    }
    if url.fragment().is_some() {
        return Err(LlmEndpointError::FragmentNotAllowed);
    }

    trim_trailing_path_slashes(&mut url)?;
    let path = url.path();
    let segments = match kind {
        LlmEndpointKind::OpenAiChatCompletions if path.ends_with("/chat/completions") => &[][..],
        LlmEndpointKind::OpenAiChatCompletions if path == "/" => &["v1", "chat", "completions"][..],
        LlmEndpointKind::OpenAiChatCompletions => &["chat", "completions"][..],
        LlmEndpointKind::AnthropicMessages if path.ends_with("/v1/messages") => &[][..],
        LlmEndpointKind::AnthropicMessages if path.ends_with("/v1") => &["messages"][..],
        LlmEndpointKind::AnthropicMessages => &["v1", "messages"][..],
    };
    append_path_segments(&mut url, segments)?;
    Ok(url.to_string())
}

fn trim_trailing_path_slashes(url: &mut Url) -> Result<(), LlmEndpointError> {
    while url.path().len() > 1 && url.path().ends_with('/') {
        url.path_segments_mut()
            .map_err(|_| LlmEndpointError::CannotAppendPath)?
            .pop_if_empty();
    }
    Ok(())
}

fn append_path_segments(url: &mut Url, segments: &[&str]) -> Result<(), LlmEndpointError> {
    if segments.is_empty() {
        return Ok(());
    }
    let mut path = url
        .path_segments_mut()
        .map_err(|_| LlmEndpointError::CannotAppendPath)?;
    path.pop_if_empty();
    for segment in segments {
        path.push(segment);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_openai_chat_completions_endpoint() {
        let cases = [
            (
                "https://llm.example.com",
                "https://llm.example.com/v1/chat/completions",
            ),
            (
                "https://llm.example.com/",
                "https://llm.example.com/v1/chat/completions",
            ),
            (
                "https://llm.example.com/v1",
                "https://llm.example.com/v1/chat/completions",
            ),
            (
                "https://llm.example.com/proxy/v2/",
                "https://llm.example.com/proxy/v2/chat/completions",
            ),
            (
                "https://llm.example.com/v1/chat/completions/",
                "https://llm.example.com/v1/chat/completions",
            ),
            (
                "https://llm.example.com/v1/chat/completions?api-version=2026-01-01",
                "https://llm.example.com/v1/chat/completions?api-version=2026-01-01",
            ),
            (
                " https://llm.example.com/v1/chat/completions/?api-version=2026-01-01 ",
                "https://llm.example.com/v1/chat/completions?api-version=2026-01-01",
            ),
            (
                "https://llm.example.com?api-version=2026-01-01",
                "https://llm.example.com/v1/chat/completions?api-version=2026-01-01",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                resolve_llm_endpoint(input, LlmEndpointKind::OpenAiChatCompletions).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn resolves_anthropic_messages_endpoint() {
        let cases = [
            (
                "https://llm.example.com",
                "https://llm.example.com/v1/messages",
            ),
            (
                "https://llm.example.com/v1",
                "https://llm.example.com/v1/messages",
            ),
            (
                "https://llm.example.com/proxy/v1/",
                "https://llm.example.com/proxy/v1/messages",
            ),
            (
                "https://llm.example.com/v1/messages/",
                "https://llm.example.com/v1/messages",
            ),
            (
                "https://llm.example.com/v1/messages?beta=true",
                "https://llm.example.com/v1/messages?beta=true",
            ),
            (
                "https://llm.example.com/proxy?beta=true",
                "https://llm.example.com/proxy/v1/messages?beta=true",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                resolve_llm_endpoint(input, LlmEndpointKind::AnthropicMessages).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_invalid_llm_endpoints() {
        for input in [
            "",
            "   ",
            "/v1",
            "ftp://llm.example.com/v1",
            "https://llm.example.com/v1#fragment",
        ] {
            assert!(
                resolve_llm_endpoint(input, LlmEndpointKind::OpenAiChatCompletions).is_err(),
                "endpoint should be rejected: {input:?}"
            );
        }
    }
}
