//! MCP OAuth HTTP 请求的 client 注入与代理选择。

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rmcp::transport::auth::{
    AuthError, AuthorizationManager, OAuthHttpClient, OAuthHttpClientError, OAuthHttpClientFuture,
    OAuthHttpRedirectPolicy, OAuthHttpRequest,
};

const OAUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OAUTH_HTTP_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("OAuth HTTP response body exceeds {0} bytes")]
struct OAuthHttpResponseTooLarge(usize);

/// rmcp 内置 reqwest client 不读取 macOS 系统代理的 ExceptionsList。这里保留其
/// redirect 与响应上限语义，同时按每个 OAuth 请求的真实目标选择代理策略。
struct AcnOAuthHttpClient {
    proxied_follow: reqwest_013::Client,
    proxied_stop: reqwest_013::Client,
    direct_follow: reqwest_013::Client,
    direct_stop: reqwest_013::Client,
}

impl AcnOAuthHttpClient {
    fn new() -> Result<Self, AuthError> {
        let build = |builder: reqwest_013::ClientBuilder, follow_redirects: bool| {
            let builder = builder.timeout(OAUTH_HTTP_TIMEOUT);
            let builder = if follow_redirects {
                builder
            } else {
                builder.redirect(reqwest_013::redirect::Policy::none())
            };
            builder
                .build()
                .map_err(|error| AuthError::InternalError(error.to_string()))
        };
        Ok(Self {
            proxied_follow: build(crate::http_client_013_builder(), true)?,
            proxied_stop: build(crate::http_client_013_builder(), false)?,
            direct_follow: build(crate::direct_http_client_013_builder(), true)?,
            direct_stop: build(crate::direct_http_client_013_builder(), false)?,
        })
    }

    fn client_for(
        &self,
        endpoint: &str,
        redirect_policy: OAuthHttpRedirectPolicy,
    ) -> &reqwest_013::Client {
        match (crate::is_loopback_endpoint(endpoint), redirect_policy) {
            (true, OAuthHttpRedirectPolicy::Stop) => &self.direct_stop,
            (true, _) => &self.direct_follow,
            (false, OAuthHttpRedirectPolicy::Stop) => &self.proxied_stop,
            (false, _) => &self.proxied_follow,
        }
    }
}

impl OAuthHttpClient for AcnOAuthHttpClient {
    fn execute(&self, request: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            let OAuthHttpRequest {
                request,
                redirect_policy,
                ..
            } = request;
            let endpoint = request.uri().to_string();
            let client = self.client_for(&endpoint, redirect_policy);
            let request = reqwest_013::Request::try_from(request)
                .map_err(|error| Box::new(error) as OAuthHttpClientError)?;
            let response = client
                .execute(request)
                .await
                .map_err(|error| Box::new(error) as OAuthHttpClientError)?;

            let mut builder = oauth2::http::Response::builder()
                .status(response.status())
                .version(response.version());
            for (name, value) in response.headers() {
                builder = builder.header(name, value);
            }
            let mut body = Vec::new();
            let mut body_stream = response.bytes_stream();
            while let Some(chunk) = body_stream.next().await {
                let chunk = chunk.map_err(|error| Box::new(error) as OAuthHttpClientError)?;
                if chunk.len() > MAX_OAUTH_HTTP_RESPONSE_BODY_BYTES - body.len() {
                    return Err(Box::new(OAuthHttpResponseTooLarge(
                        MAX_OAUTH_HTTP_RESPONSE_BODY_BYTES,
                    )) as OAuthHttpClientError);
                }
                body.extend_from_slice(&chunk);
            }
            builder
                .body(body)
                .map_err(|error| Box::new(error) as OAuthHttpClientError)
        })
    }
}

pub(crate) async fn new_authorization_manager(
    url: &str,
) -> Result<AuthorizationManager, AuthError> {
    AuthorizationManager::new_with_oauth_http_client(url, Arc::new(AcnOAuthHttpClient::new()?))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_client_selects_proxy_per_request_target_and_redirect_policy() {
        let client = AcnOAuthHttpClient::new().unwrap();

        assert!(std::ptr::eq(
            client.client_for(
                "http://127.0.0.1:8000/token",
                OAuthHttpRedirectPolicy::Follow
            ),
            &client.direct_follow
        ));
        assert!(std::ptr::eq(
            client.client_for("http://[::1]:8000/token", OAuthHttpRedirectPolicy::Stop),
            &client.direct_stop
        ));
        assert!(std::ptr::eq(
            client.client_for(
                "https://auth.example.test/token",
                OAuthHttpRedirectPolicy::Follow
            ),
            &client.proxied_follow
        ));
        assert!(std::ptr::eq(
            client.client_for("https://10.0.0.1/token", OAuthHttpRedirectPolicy::Stop),
            &client.proxied_stop
        ));
    }
}
