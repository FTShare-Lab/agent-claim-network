//! router embedding 客户端抽象。
//!
//! 当前只服务 router 自管的向量派生链路：
//! - `OpenAiCompatibleEmbeddingClient`：最小 OpenAI-compatible `/embeddings` 调用
//! - `ArkMultimodalEmbeddingClient`：火山 Ark `/embeddings/multimodal` 文本 embedding 调用

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::{EmbeddingConfig, EmbeddingProvider};

const EMBEDDING_CACHE_SCHEMA_VERSION: u32 = 1;
const EMBEDDING_NORMALIZATION: &str = "none";

/// 标识一组可安全复用的 embedding 配置；API key 不属于缓存身份。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EmbeddingCacheFingerprint {
    pub schema_version: u32,
    pub provider: EmbeddingProvider,
    pub endpoint: String,
    pub model: String,
    pub dimension_policy: String,
    pub normalization: String,
}

impl EmbeddingCacheFingerprint {
    fn from_config(cfg: &EmbeddingConfig, dimension_policy: &str) -> Self {
        Self {
            schema_version: EMBEDDING_CACHE_SCHEMA_VERSION,
            provider: cfg.provider,
            endpoint: cfg.endpoint.clone(),
            model: cfg.model.clone(),
            dimension_policy: dimension_policy.into(),
            normalization: EMBEDDING_NORMALIZATION.into(),
        }
    }
}

#[async_trait]
pub trait EmbeddingClient: Send + Sync {
    /// 返回用于隔离本地向量缓存的稳定配置身份。
    fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint;

    /// 为输入文本生成单向量。
    async fn embed(&self, input: &str) -> anyhow::Result<Vec<f32>>;
}

pub fn build_embedding_client(cfg: &EmbeddingConfig) -> anyhow::Result<Arc<dyn EmbeddingClient>> {
    match cfg.provider {
        EmbeddingProvider::OpenAiCompatible => Ok(Arc::new(
            OpenAiCompatibleEmbeddingClient::new(cfg)
                .context("构造 openai-compatible embedding client 失败")?,
        )),
        EmbeddingProvider::ArkMultimodal => Ok(Arc::new(
            ArkMultimodalEmbeddingClient::new(cfg)
                .context("构造 ark multimodal embedding client 失败")?,
        )),
    }
}

pub struct OpenAiCompatibleEmbeddingClient {
    endpoint: String,
    model: String,
    cache_fingerprint: EmbeddingCacheFingerprint,
    http: reqwest::Client,
}

impl OpenAiCompatibleEmbeddingClient {
    pub fn new(cfg: &EmbeddingConfig) -> anyhow::Result<Self> {
        let http = build_http_client(&cfg.endpoint, cfg.timeout_secs, &cfg.api_key_env)?;
        Ok(Self {
            endpoint: cfg.endpoint.clone(),
            model: cfg.model.clone(),
            cache_fingerprint: EmbeddingCacheFingerprint::from_config(cfg, "response_length"),
            http,
        })
    }
}

#[async_trait]
impl EmbeddingClient for OpenAiCompatibleEmbeddingClient {
    fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
        self.cache_fingerprint.clone()
    }

    async fn embed(&self, input: &str) -> anyhow::Result<Vec<f32>> {
        let req = EmbeddingRequest {
            model: self.model.clone(),
            input: input.to_string(),
        };
        let resp = self
            .http
            .post(&self.endpoint)
            .json(&req)
            .send()
            .await
            .with_context(|| format!("调用 embedding endpoint 失败: {}", self.endpoint))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "embedding endpoint 返回非成功状态: status={} body={body}",
                status.as_u16()
            );
        }
        let body: EmbeddingResponse = resp.json().await.context("解析 embedding 响应失败")?;
        let vector = body
            .data
            .into_iter()
            .next()
            .map(|item| item.embedding)
            .context("embedding 响应缺少 data[0].embedding")?;
        Ok(vector)
    }
}

pub struct ArkMultimodalEmbeddingClient {
    endpoint: String,
    model: String,
    cache_fingerprint: EmbeddingCacheFingerprint,
    http: reqwest::Client,
}

impl ArkMultimodalEmbeddingClient {
    pub fn new(cfg: &EmbeddingConfig) -> anyhow::Result<Self> {
        let http = build_http_client(&cfg.endpoint, cfg.timeout_secs, &cfg.api_key_env)?;
        Ok(Self {
            endpoint: cfg.endpoint.clone(),
            model: cfg.model.clone(),
            cache_fingerprint: EmbeddingCacheFingerprint::from_config(cfg, "response_length"),
            http,
        })
    }
}

#[async_trait]
impl EmbeddingClient for ArkMultimodalEmbeddingClient {
    fn cache_fingerprint(&self) -> EmbeddingCacheFingerprint {
        self.cache_fingerprint.clone()
    }

    async fn embed(&self, input: &str) -> anyhow::Result<Vec<f32>> {
        let req = ArkMultimodalEmbeddingRequest {
            model: self.model.clone(),
            input: vec![ArkMultimodalInput {
                input_type: "text",
                text: input.to_string(),
            }],
            encoding_format: "float",
        };
        let resp = self
            .http
            .post(&self.endpoint)
            .json(&req)
            .send()
            .await
            .with_context(|| {
                format!(
                    "调用 ark multimodal embedding endpoint 失败: {}",
                    self.endpoint
                )
            })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "ark multimodal embedding endpoint 返回非成功状态: status={} body={body}",
                status.as_u16()
            );
        }
        let body: ArkMultimodalEmbeddingResponse = resp
            .json()
            .await
            .context("解析 ark multimodal embedding 响应失败")?;
        Ok(body.data.embedding)
    }
}

fn build_http_client(
    endpoint: &str,
    timeout_secs: u64,
    api_key_env: &str,
) -> anyhow::Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let api_key = resolve_api_key_with(api_key_env, |name| std::env::var(name))?;
    let auth = format!("Bearer {api_key}");
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&auth).context("embedding API key header 非法")?,
    );

    crate::http_client_builder_for_endpoint(endpoint)
        .default_headers(headers)
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .context("构造 embedding HTTP client 失败")
}

fn resolve_api_key_with<F>(api_key_env: &str, read_env: F) -> anyhow::Result<String>
where
    F: FnOnce(&str) -> Result<String, std::env::VarError>,
{
    let api_key = read_env(api_key_env)
        .with_context(|| format!("{api_key_env} 未设置，无法调用 embedding API"))?;
    if api_key.trim().is_empty() {
        anyhow::bail!("{api_key_env} 为空，无法调用 embedding API");
    }
    Ok(api_key)
}

#[derive(Debug, Serialize)]
struct EmbeddingRequest {
    model: String,
    input: String,
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingItem>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingItem {
    embedding: Vec<f32>,
}

#[derive(Debug, Serialize)]
struct ArkMultimodalEmbeddingRequest {
    model: String,
    input: Vec<ArkMultimodalInput>,
    encoding_format: &'static str,
}

#[derive(Debug, Serialize)]
struct ArkMultimodalInput {
    #[serde(rename = "type")]
    input_type: &'static str,
    text: String,
}

#[derive(Debug, Deserialize)]
struct ArkMultimodalEmbeddingResponse {
    data: ArkMultimodalEmbeddingData,
}

#[derive(Debug, Deserialize)]
struct ArkMultimodalEmbeddingData {
    embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    use super::*;

    async fn spawn_embedding_server(response: Value) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/embeddings",
            post(move |Json(_request): Json<Value>| {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}/embeddings")
    }

    #[tokio::test]
    async fn openai_compatible_client_reads_embedding_from_local_server() {
        let endpoint = spawn_embedding_server(json!({
            "data": [{"embedding": [0.25, 0.75]}]
        }))
        .await;
        let cfg = EmbeddingConfig {
            provider: EmbeddingProvider::OpenAiCompatible,
            endpoint: endpoint.clone(),
            model: "test-model".into(),
            ..EmbeddingConfig::default()
        };
        let client = OpenAiCompatibleEmbeddingClient {
            http: crate::http_client_builder_for_endpoint(&endpoint)
                .build()
                .unwrap(),
            endpoint,
            model: cfg.model.clone(),
            cache_fingerprint: EmbeddingCacheFingerprint::from_config(&cfg, "response_length"),
        };

        assert_eq!(client.embed("hello").await.unwrap(), vec![0.25, 0.75]);
    }

    #[tokio::test]
    async fn ark_multimodal_client_reads_embedding_from_local_server() {
        let endpoint = spawn_embedding_server(json!({
            "data": {"embedding": [0.5, 0.125]}
        }))
        .await;
        let cfg = EmbeddingConfig {
            provider: EmbeddingProvider::ArkMultimodal,
            endpoint: endpoint.clone(),
            model: "test-model".into(),
            ..EmbeddingConfig::default()
        };
        let client = ArkMultimodalEmbeddingClient {
            http: crate::http_client_builder_for_endpoint(&endpoint)
                .build()
                .unwrap(),
            endpoint,
            model: cfg.model.clone(),
            cache_fingerprint: EmbeddingCacheFingerprint::from_config(&cfg, "response_length"),
        };

        assert_eq!(client.embed("hello").await.unwrap(), vec![0.5, 0.125]);
    }

    #[test]
    fn api_key_resolver_reads_only_the_configured_environment_variable() {
        let values = BTreeMap::from([
            ("CUSTOM_EMBEDDING_KEY", "embedding-secret"),
            ("OPENAI_API_KEY", "must-not-be-used"),
        ]);

        let key = resolve_api_key_with("CUSTOM_EMBEDDING_KEY", |name| {
            values
                .get(name)
                .map(|value| (*value).to_string())
                .ok_or(std::env::VarError::NotPresent)
        })
        .unwrap();

        assert_eq!(key, "embedding-secret");
    }

    #[test]
    fn missing_embedding_key_does_not_fall_back_to_openai_key() {
        let values = BTreeMap::from([("OPENAI_API_KEY", "must-not-be-used")]);

        let error = resolve_api_key_with("EMBEDDING_API_KEY", |name| {
            values
                .get(name)
                .map(|value| (*value).to_string())
                .ok_or(std::env::VarError::NotPresent)
        })
        .unwrap_err();

        assert!(error.to_string().contains("EMBEDDING_API_KEY 未设置"));
    }

    #[test]
    fn empty_configured_embedding_key_is_rejected() {
        let error =
            resolve_api_key_with("EMBEDDING_API_KEY", |_| Ok("  \t".to_string())).unwrap_err();

        assert!(error.to_string().contains("EMBEDDING_API_KEY 为空"));
    }

    #[test]
    fn cache_fingerprint_covers_config_identity_but_never_key_source() {
        let mut cfg = EmbeddingConfig {
            provider: EmbeddingProvider::OpenAiCompatible,
            endpoint: "https://embedding.example/v1/embeddings".into(),
            model: "model-a".into(),
            api_key_env: "KEY_A".into(),
            ..EmbeddingConfig::default()
        };
        let fingerprint = EmbeddingCacheFingerprint::from_config(&cfg, "response_length");
        assert_eq!(fingerprint.schema_version, EMBEDDING_CACHE_SCHEMA_VERSION);
        assert_eq!(fingerprint.normalization, EMBEDDING_NORMALIZATION);
        assert_eq!(fingerprint.dimension_policy, "response_length");

        cfg.api_key_env = "KEY_B".into();
        assert_eq!(
            fingerprint,
            EmbeddingCacheFingerprint::from_config(&cfg, "response_length")
        );
        cfg.endpoint = "https://other.example/v1/embeddings".into();
        assert_ne!(
            fingerprint,
            EmbeddingCacheFingerprint::from_config(&cfg, "response_length")
        );
        cfg.endpoint = "https://embedding.example/v1/embeddings".into();
        cfg.model = "model-b".into();
        assert_ne!(
            fingerprint,
            EmbeddingCacheFingerprint::from_config(&cfg, "response_length")
        );
    }
}
