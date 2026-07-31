//! LLM provider HTTP 错误的展示与分类。
//!
//! reqwest 的底层错误会把 timeout、连接失败、body 解码失败等都压成一句英文。
//! 这里统一补上“这是 LLM 请求的哪个阶段失败”，让 TUI 与日志里的错误更可读。

use std::error::Error;
use std::fmt;
use std::time::Duration;

const MAX_LLM_ERROR_BODY_CHARS: usize = 2_000;

#[derive(Debug, Clone, Copy)]
pub(crate) enum LlmHttpPhase {
    BuildClient,
    SendRequest,
    ReadResponseBody,
    ReadStreamBody,
    DecodeJsonBody,
}

impl LlmHttpPhase {
    fn label(self) -> &'static str {
        match self {
            Self::BuildClient => "building LLM HTTP client",
            Self::SendRequest => "sending LLM request",
            Self::ReadResponseBody => "reading LLM response body",
            Self::ReadStreamBody => "reading LLM stream response body",
            Self::DecodeJsonBody => "decoding LLM JSON response body",
        }
    }
}

#[derive(Debug)]
pub struct LlmHttpError {
    phase: LlmHttpPhase,
    timeout: Option<Duration>,
    source: reqwest::Error,
}

impl LlmHttpError {
    pub(crate) fn new(
        source: reqwest::Error,
        phase: LlmHttpPhase,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            phase,
            timeout,
            source,
        }
    }

    pub(crate) fn is_retryable(&self) -> bool {
        true
    }

    fn timeout_label(&self) -> Option<String> {
        self.timeout.map(|timeout| {
            if timeout.as_secs() > 0 {
                format!("{}s", timeout.as_secs())
            } else {
                format!("{}ms", timeout.as_millis())
            }
        })
    }
}

impl fmt::Display for LlmHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = self.phase.label();
        if self.source.is_timeout() {
            if let Some(timeout_label) = self.timeout_label() {
                return write!(
                    f,
                    "LLM request timeout after {timeout_label} while {phase}: {}",
                    self.source
                );
            }
            return write!(f, "LLM request timeout while {phase}: {}", self.source);
        }
        if self.source.is_connect() {
            return write!(f, "LLM connection failed while {phase}: {}", self.source);
        }
        if self.source.is_decode() {
            return write!(
                f,
                "LLM response body decode failed while {phase}: {}",
                self.source
            );
        }
        if self.source.is_body() {
            return write!(
                f,
                "LLM response body read failed while {phase}: {}",
                self.source
            );
        }
        if self.source.is_request() {
            return write!(f, "LLM request build failed while {phase}: {}", self.source);
        }
        write!(f, "LLM HTTP error while {phase}: {}", self.source)
    }
}

impl Error for LlmHttpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

pub(crate) async fn read_llm_error_body(resp: reqwest::Response, timeout: Duration) -> String {
    match resp.text().await {
        Ok(body) => truncate_llm_error_body(&body),
        Err(error) => format!(
            "<failed to read provider error body: {}>",
            LlmHttpError::new(error, LlmHttpPhase::ReadResponseBody, Some(timeout))
        ),
    }
}

fn truncate_llm_error_body(body: &str) -> String {
    let mut iter = body.chars();
    let truncated = iter
        .by_ref()
        .take(MAX_LLM_ERROR_BODY_CHARS)
        .collect::<String>();
    if iter.next().is_some() {
        format!("{truncated}...[truncated]")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_llm_error_body_truncates_long_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = "x".repeat(MAX_LLM_ERROR_BODY_CHARS + 8);
            let response = format!(
                "HTTP/1.1 500 Internal Server Error\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            use tokio::io::AsyncWriteExt as _;
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let resp = reqwest::Client::new()
            .get(format!("http://{addr}"))
            .send()
            .await
            .unwrap();
        let body = read_llm_error_body(resp, Duration::from_secs(30)).await;

        assert_eq!(
            body.chars().filter(|ch| *ch == 'x').count(),
            MAX_LLM_ERROR_BODY_CHARS
        );
        assert!(body.ends_with("...[truncated]"));
    }
}
