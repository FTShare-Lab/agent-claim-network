//! Router 的 HTTP client 实现。
//!
//! agent 通过该 client 调用 router daemon，协议面只暴露 `RouterClient::query`。

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Serialize;

use super::traits::{AgentQuery, RouterClient, RouterQueryResult};
use crate::auth::{AuthEnvelope, AuthRequest};
use crate::claim::AgentId;
use crate::config::HttpClientConfig;
use crate::router::ScopesOverviewSnapshot;

pub struct HttpRouterClient {
    endpoint: String,
    http: reqwest::Client,
    auth: ClientAuth,
    retry_count: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
}

#[derive(Debug, Clone)]
struct ClientAuth {
    agent_id: AgentId,
    acn_key: String,
}

#[derive(Debug, Clone, Serialize)]
struct EmptyData {}

#[derive(Debug, thiserror::Error)]
pub enum RouterClientError {
    #[error("router {operation} 鉴权失败: status={status}")]
    Auth { operation: String, status: u16 },
    #[error("router {operation} 暂时不可用: status={status} body={body}")]
    Retryable {
        operation: String,
        status: u16,
        body: String,
    },
    #[error("router {operation} 客户端错误: status={status} body={body}")]
    Client {
        operation: String,
        status: u16,
        body: String,
    },
}

impl HttpRouterClient {
    pub fn new_with_auth(
        endpoint: String,
        cfg: &HttpClientConfig,
        agent_id: AgentId,
        api_key: Option<String>,
    ) -> anyhow::Result<Self> {
        let http = crate::http_client_builder_for_endpoint(&endpoint)
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build()?;
        let auth = ClientAuth {
            agent_id,
            acn_key: api_key.unwrap_or_default(),
        };
        Ok(Self {
            endpoint,
            http,
            auth,
            retry_count: cfg.retry_count,
            retry_base_delay: Duration::from_millis(cfg.retry_base_delay_ms),
            retry_max_delay: Duration::from_millis(cfg.retry_max_delay_ms),
        })
    }

    async fn get_scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        let url = format!(
            "{}/claims/scopes/overview",
            self.endpoint.trim_end_matches('/')
        );
        let mut attempt: u32 = 0;
        loop {
            let body = EmptyData {};
            let res = self
                .http
                .post(&url)
                .json(&self.wrap_body(&body))
                .send()
                .await;
            match res {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(resp.json::<ScopesOverviewSnapshot>().await?)
                }
                Ok(resp) if is_auth_status(resp.status()) => {
                    let status = resp.status();
                    return Err(RouterClientError::Auth {
                        operation: "POST /claims/scopes/overview".to_string(),
                        status: status.as_u16(),
                    }
                    .into());
                }
                Ok(resp) if should_not_retry(resp.status()) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(RouterClientError::Client {
                        operation: "POST /claims/scopes/overview".to_string(),
                        status: status.as_u16(),
                        body: response_body_label(body),
                    }
                    .into());
                }
                Ok(resp) => {
                    if attempt >= self.retry_count {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        return Err(RouterClientError::Retryable {
                            operation: "POST /claims/scopes/overview".to_string(),
                            status: status.as_u16(),
                            body: response_body_label(body),
                        }
                        .into());
                    }
                    retry_sleep(
                        attempt,
                        self.retry_base_delay,
                        self.retry_max_delay,
                        Some(resp.status()),
                    )
                    .await;
                }
                Err(err) => {
                    if attempt >= self.retry_count {
                        return Err(err.into());
                    }
                    retry_sleep(attempt, self.retry_base_delay, self.retry_max_delay, None).await;
                }
            }
            attempt += 1;
        }
    }

    fn wrap_body<'a, B>(&'a self, data: &'a B) -> AuthRequest<&'a B>
    where
        B: Serialize + ?Sized,
    {
        AuthRequest {
            auth: AuthEnvelope {
                agent_id: self.auth.agent_id.clone(),
                acn_key: self.auth.acn_key.clone(),
            },
            data,
        }
    }
}

#[async_trait]
impl RouterClient for HttpRouterClient {
    async fn query(&self, agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        let url = format!("{}/claims/query", self.endpoint.trim_end_matches('/'));
        let mut attempt: u32 = 0;
        loop {
            let res = self
                .http
                .post(&url)
                .json(&self.wrap_body(agent_query))
                .send()
                .await;
            match res {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(resp.json::<RouterQueryResult>().await?)
                }
                Ok(resp) if is_auth_status(resp.status()) => {
                    let status = resp.status();
                    return Err(RouterClientError::Auth {
                        operation: "POST /claims/query".to_string(),
                        status: status.as_u16(),
                    }
                    .into());
                }
                Ok(resp) if should_not_retry(resp.status()) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(RouterClientError::Client {
                        operation: "POST /claims/query".to_string(),
                        status: status.as_u16(),
                        body: response_body_label(body),
                    }
                    .into());
                }
                Ok(resp) => {
                    if attempt >= self.retry_count {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        return Err(RouterClientError::Retryable {
                            operation: "POST /claims/query".to_string(),
                            status: status.as_u16(),
                            body: response_body_label(body),
                        }
                        .into());
                    }
                    retry_sleep(
                        attempt,
                        self.retry_base_delay,
                        self.retry_max_delay,
                        Some(resp.status()),
                    )
                    .await;
                }
                Err(err) => {
                    if attempt >= self.retry_count {
                        return Err(err.into());
                    }
                    retry_sleep(attempt, self.retry_base_delay, self.retry_max_delay, None).await;
                }
            }
            attempt += 1;
        }
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        self.get_scopes_overview().await
    }
}

fn response_body_label(body: String) -> String {
    if body.is_empty() {
        "None".into()
    } else {
        body
    }
}

fn is_auth_status(status: StatusCode) -> bool {
    matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
}

fn should_not_retry(status: StatusCode) -> bool {
    status.is_client_error()
        && !matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
}

async fn retry_sleep(attempt: u32, base: Duration, max: Duration, status: Option<StatusCode>) {
    let shift = attempt.min(16);
    let factor = 1u32 << shift;
    let delay = base.saturating_mul(factor).min(max);
    log::warn!(
        target: "router_http_client",
        "router HTTP 请求失败，准备重试 attempt={} status={:?} delay_ms={}",
        attempt + 1,
        status.map(|s| s.as_u16()),
        delay.as_millis()
    );
    tokio::time::sleep(delay).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_error_formats_status_without_option_wrapper_and_empty_body_as_none() {
        let error = RouterClientError::Retryable {
            operation: "POST /claims/scopes/overview".into(),
            status: StatusCode::BAD_GATEWAY.as_u16(),
            body: response_body_label(String::new()),
        };

        assert_eq!(
            error.to_string(),
            "router POST /claims/scopes/overview 暂时不可用: status=502 body=None"
        );
    }

    #[test]
    fn response_body_label_preserves_non_empty_body() {
        assert_eq!(
            response_body_label("upstream unavailable".into()),
            "upstream unavailable"
        );
    }
}
