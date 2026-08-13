//! Maintainer 的 HTTP client 实现。
//!
//! agent 侧只通过 `MaintainerClient` 访问 inbox pull / receipt ACK、claim upload 与
//! dispute report。

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Serialize;

use super::traits::{MaintainerClient, MaintainerClientError};
use crate::auth::{AuthEnvelope, AuthRequest};
use crate::claim::{AgentId, Claim, Dispute, InboxAckRequest, InboxId, InboxMessage};
use crate::config::HttpClientConfig;

pub struct HttpMaintainerClient {
    endpoint: String,
    http: reqwest::Client,
    auth: ClientAuth,
    retry_count: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
struct PullInboxRequest<'a> {
    agent_id: &'a AgentId,
}

#[derive(Debug, Clone)]
struct ClientAuth {
    agent_id: AgentId,
    acn_key: String,
}

impl HttpMaintainerClient {
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
            timeout_secs: cfg.timeout_secs,
        })
    }

    async fn post_json<B, T>(&self, path: &str, body: B) -> anyhow::Result<T>
    where
        B: Serialize,
        T: serde::de::DeserializeOwned,
    {
        let url = format!("{}/{}", self.endpoint.trim_end_matches('/'), path);
        let mut attempt: u32 = 0;
        loop {
            let req = self.http.post(&url).json(&self.wrap_body(&body));
            let res = req.send().await;
            match res {
                Ok(resp) if resp.status().is_success() => return Ok(resp.json::<T>().await?),
                Ok(resp) if is_auth_status(resp.status()) => {
                    let status = resp.status();
                    return Err(MaintainerClientError::Auth {
                        operation: post_operation(path),
                        status: status.as_u16(),
                    }
                    .into());
                }
                Ok(resp) if is_retryable_http_status(resp.status()) => {
                    if attempt >= self.retry_count {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        return Err(
                            retryable_status_error(path, status, body, self.timeout_secs).into(),
                        );
                    }
                    retry_sleep(
                        attempt,
                        self.retry_base_delay,
                        self.retry_max_delay,
                        Some(resp.status()),
                    )
                    .await;
                }
                Ok(resp) if resp.status().is_client_error() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(MaintainerClientError::Client {
                        operation: post_operation(path),
                        status: status.as_u16(),
                        body: response_body_label(&body).to_string(),
                    }
                    .into());
                }
                Ok(resp) => {
                    if attempt >= self.retry_count {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        return Err(
                            retryable_status_error(path, status, body, self.timeout_secs).into(),
                        );
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
                        return Err(MaintainerClientError::Retryable {
                            operation: post_operation(path),
                            timeout_secs: self.timeout_secs,
                            timed_out: err.is_timeout(),
                            message: err.to_string(),
                        }
                        .into());
                    }
                    retry_sleep(attempt, self.retry_base_delay, self.retry_max_delay, None).await;
                }
            }
            attempt += 1;
        }
    }

    async fn post_no_response<B>(&self, path: &str, body: B) -> anyhow::Result<()>
    where
        B: Serialize,
    {
        let url = format!("{}/{}", self.endpoint.trim_end_matches('/'), path);
        let mut attempt: u32 = 0;
        loop {
            let req = self.http.post(&url).json(&self.wrap_body(&body));
            let res = req.send().await;
            match res {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) if is_auth_status(resp.status()) => {
                    let status = resp.status();
                    return Err(MaintainerClientError::Auth {
                        operation: post_operation(path),
                        status: status.as_u16(),
                    }
                    .into());
                }
                Ok(resp) if is_retryable_http_status(resp.status()) => {
                    if attempt >= self.retry_count {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        return Err(
                            retryable_status_error(path, status, body, self.timeout_secs).into(),
                        );
                    }
                    retry_sleep(
                        attempt,
                        self.retry_base_delay,
                        self.retry_max_delay,
                        Some(resp.status()),
                    )
                    .await;
                }
                Ok(resp) if resp.status().is_client_error() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(MaintainerClientError::Client {
                        operation: post_operation(path),
                        status: status.as_u16(),
                        body: response_body_label(&body).to_string(),
                    }
                    .into());
                }
                Ok(resp) => {
                    if attempt >= self.retry_count {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        return Err(
                            retryable_status_error(path, status, body, self.timeout_secs).into(),
                        );
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
                        return Err(MaintainerClientError::Retryable {
                            operation: post_operation(path),
                            timeout_secs: self.timeout_secs,
                            timed_out: err.is_timeout(),
                            message: err.to_string(),
                        }
                        .into());
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
impl MaintainerClient for HttpMaintainerClient {
    async fn pull_inbox(&self, agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
        self.post_json("inbox/pull", PullInboxRequest { agent_id })
            .await
    }

    async fn ack_inbox(&self, agent_id: &AgentId, inbox_ids: &[InboxId]) -> anyhow::Result<()> {
        let result = self
            .post_no_response(
                "inbox/ack",
                InboxAckRequest {
                    agent_id: agent_id.clone(),
                    inbox_ids: inbox_ids.to_vec(),
                },
            )
            .await;
        result.map_err(recognize_legacy_ack_route)
    }

    async fn upload_claim(&self, claim: &Claim) -> anyhow::Result<()> {
        self.post_no_response("claims/upload", claim).await
    }

    async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()> {
        self.post_no_response("disputes/report", dispute).await
    }
}

fn recognize_legacy_ack_route(err: anyhow::Error) -> anyhow::Error {
    let Some(MaintainerClientError::Client { status, .. }) =
        err.downcast_ref::<MaintainerClientError>()
    else {
        return err;
    };
    if *status != StatusCode::NOT_FOUND.as_u16()
        && *status != StatusCode::METHOD_NOT_ALLOWED.as_u16()
    {
        return err;
    }
    MaintainerClientError::LegacyServer {
        operation: "POST /inbox/ack".into(),
        status: *status,
    }
    .into()
}

async fn retry_sleep(attempt: u32, base: Duration, max: Duration, status: Option<StatusCode>) {
    let shift = attempt.min(16);
    let factor = 1u32 << shift;
    let delay = base.saturating_mul(factor).min(max);
    log::warn!(
        target: "maintainer_http_client",
        "maintainer HTTP 请求失败，准备重试 attempt={} status={:?} delay_ms={}",
        attempt + 1,
        status.map(|s| s.as_u16()),
        delay.as_millis()
    );
    tokio::time::sleep(delay).await;
}

fn is_retryable_http_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn is_auth_status(status: StatusCode) -> bool {
    status == StatusCode::UNAUTHORIZED
}

fn post_operation(path: &str) -> String {
    format!("POST /{}", path.trim_start_matches('/'))
}

fn retryable_status_error(
    path: &str,
    status: StatusCode,
    body: String,
    timeout_secs: u64,
) -> MaintainerClientError {
    MaintainerClientError::Retryable {
        operation: post_operation(path),
        timeout_secs,
        timed_out: status == StatusCode::REQUEST_TIMEOUT,
        message: format!(
            "status={} body={}",
            status.as_u16(),
            response_body_label(&body)
        ),
    }
}

fn response_body_label(body: &str) -> &str {
    if body.is_empty() {
        "None"
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpClientConfig;

    #[test]
    fn retryable_http_status_includes_timeout_rate_limit_and_server_errors() {
        assert!(is_retryable_http_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_retryable_http_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_http_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!is_retryable_http_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_http_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn auth_status_only_treats_401_as_key_failure() {
        assert!(is_auth_status(StatusCode::UNAUTHORIZED));
        assert!(!is_auth_status(StatusCode::FORBIDDEN));
    }

    #[test]
    fn retryable_status_error_marks_408_as_timed_out() {
        let err =
            retryable_status_error("claims/upload", StatusCode::REQUEST_TIMEOUT, "".into(), 20);
        assert!(err.timed_out());

        let err = retryable_status_error(
            "claims/upload",
            StatusCode::TOO_MANY_REQUESTS,
            "".into(),
            20,
        );
        assert!(!err.timed_out());
    }

    #[test]
    fn retryable_status_error_formats_empty_body_as_none() {
        let err = retryable_status_error("inbox/pull", StatusCode::BAD_GATEWAY, "".into(), 20);
        assert_eq!(
            err.to_string(),
            "maintainer POST /inbox/pull 暂时不可用: status=502 body=None"
        );
    }

    #[test]
    fn new_with_auth_wraps_requests_in_auth_envelope() {
        let agent = AgentId::new("agent-a").unwrap();
        let client = HttpMaintainerClient::new_with_auth(
            "http://127.0.0.1:8062".into(),
            &HttpClientConfig {
                retry_count: 0,
                ..HttpClientConfig::default()
            },
            agent.clone(),
            Some("team-secret".into()),
        )
        .unwrap();

        let body = PullInboxRequest { agent_id: &agent };
        let value = serde_json::to_value(client.wrap_body(&body)).unwrap();

        assert_eq!(value["auth"]["agent_id"], "agent-a");
        assert_eq!(value["auth"]["acn_key"], "team-secret");
        assert_eq!(value["data"]["agent_id"], "agent-a");
    }

    #[test]
    fn new_with_auth_wraps_empty_key_in_auth_envelope() {
        let agent = AgentId::new("agent-a").unwrap();
        let client = HttpMaintainerClient::new_with_auth(
            "http://127.0.0.1:8062".into(),
            &HttpClientConfig {
                retry_count: 0,
                ..HttpClientConfig::default()
            },
            agent.clone(),
            None,
        )
        .unwrap();

        let body = PullInboxRequest { agent_id: &agent };
        let value = serde_json::to_value(client.wrap_body(&body)).unwrap();

        assert_eq!(value["auth"]["agent_id"], "agent-a");
        assert_eq!(value["auth"]["acn_key"], "");
        assert_eq!(value["data"]["agent_id"], "agent-a");
    }

    #[test]
    fn ack_404_and_405_are_recognized_as_legacy_server() {
        for status in [StatusCode::NOT_FOUND, StatusCode::METHOD_NOT_ALLOWED] {
            let err: anyhow::Error = MaintainerClientError::Client {
                operation: "POST /inbox/ack".into(),
                status: status.as_u16(),
                body: String::new(),
            }
            .into();
            let mapped = recognize_legacy_ack_route(err);
            assert!(matches!(
                mapped.downcast_ref::<MaintainerClientError>(),
                Some(MaintainerClientError::LegacyServer {
                    operation,
                    status: mapped_status,
                }) if operation == "POST /inbox/ack" && *mapped_status == status.as_u16()
            ));
        }
    }

    #[test]
    fn ack_other_client_errors_keep_original_classification() {
        let err: anyhow::Error = MaintainerClientError::Client {
            operation: "POST /inbox/ack".into(),
            status: StatusCode::BAD_REQUEST.as_u16(),
            body: "invalid ids".into(),
        }
        .into();
        let mapped = recognize_legacy_ack_route(err);
        assert!(matches!(
            mapped.downcast_ref::<MaintainerClientError>(),
            Some(MaintainerClientError::Client { status: 400, .. })
        ));
    }
}
