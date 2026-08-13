//! Web 搜索、抓取与通用 HTTP 请求工具。
//!
//! 实现 web_search/web_fetch/web_request 及其有界响应模型。

use super::*;

impl ToolRegistry {
    fn http_for_url(&self, url: &str) -> &reqwest::Client {
        if crate::is_loopback_endpoint(url) {
            &self.direct_http
        } else {
            &self.http
        }
    }

    pub(super) async fn web_search(&self, input: Value) -> Result<ToolExecution, ToolError> {
        let args: WebSearchArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        self.web_search_zhipu(args).await
    }

    pub(super) async fn web_fetch(&self, input: Value) -> Result<ToolExecution, ToolError> {
        let args: WebLookupArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        if !(args.url.starts_with("http://") || args.url.starts_with("https://")) {
            return Err(ToolError::InvalidUrl(args.url));
        }
        let mut req = self.http_for_url(&args.url).get(&args.url);
        for header in args.headers.unwrap_or_default() {
            req = req.header(header.name, header.value);
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let (body, truncated) = self
            .response_text_bounded(resp, self.limits.web_lookup_max_chars)
            .await?;
        Ok(ToolExecution::new(
            json!({
                "url": args.url,
                "http_status": status,
                "body": body,
                "truncated": truncated,
            }),
            ToolExecutionOutcome::HttpResponse {
                http_status: status,
            },
        ))
    }

    pub(super) async fn web_request(&self, input: Value) -> Result<ToolExecution, ToolError> {
        let args: WebRequestArgs =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        if !(args.url.starts_with("http://") || args.url.starts_with("https://")) {
            return Err(ToolError::InvalidUrl(args.url));
        }
        let method = match args.method.as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "PATCH" => reqwest::Method::PATCH,
            "DELETE" => reqwest::Method::DELETE,
            other => return Err(ToolError::InvalidArgs(format!("不支持的 method: {other}"))),
        };
        let mut req = self.http_for_url(&args.url).request(method, &args.url);
        for header in args.headers.unwrap_or_default() {
            req = req.header(header.name, header.value);
        }
        if let Some(query) = args.query {
            let pairs: Vec<(String, String)> = query.into_iter().collect();
            req = req.query(&pairs);
        }
        if let Some(body) = args.body {
            req = req.json(&body);
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let (body, truncated) = self
            .response_text_bounded(resp, self.limits.web_lookup_max_chars)
            .await?;
        let body_json =
            serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({ "_raw": body }));
        Ok(ToolExecution::new(
            json!({
                "method": args.method,
                "url": args.url,
                "http_status": status,
                "body": body_json,
                "truncated": truncated,
            }),
            ToolExecutionOutcome::HttpResponse {
                http_status: status,
            },
        ))
    }

    pub(super) async fn web_search_zhipu(
        &self,
        args: WebSearchArgs,
    ) -> Result<ToolExecution, ToolError> {
        let api_key = self
            .web_search_api_key
            .as_ref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ToolError::MissingWebSearchApiKey {
                env: self.web_search_api_key_env.clone(),
            })?;

        let count = args
            .count
            .unwrap_or(10)
            .clamp(1, self.limits.web_search_max_count);
        let search_recency_filter = args
            .search_recency_filter
            .unwrap_or_else(|| "noLimit".into());
        let content_size = args.content_size.unwrap_or_else(|| "medium".into());

        let mut body = json!({
            "search_query": args.query,
            "search_engine": WEB_SEARCH_ENGINE,
            "search_intent": true,
            "count": count,
            "search_recency_filter": search_recency_filter,
            "content_size": content_size,
            "user_id": WEB_SEARCH_USER_ID,
        });
        if let Some(domain_filter) = args.search_domain_filter {
            if !domain_filter.trim().is_empty() {
                body["search_domain_filter"] = Value::String(domain_filter);
            }
        }

        let resp = self
            .http_for_url(&self.web_search_endpoint)
            .post(&self.web_search_endpoint)
            .bearer_auth(api_key)
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let text = resp.text().await?;
        if !(200..300).contains(&status) {
            let (body, truncated) = truncate_chars(&text, 1_000);
            return Ok(ToolExecution::new(
                json!({
                    "provider": "zhipu",
                    "http_status": status,
                    "body": body,
                    "truncated": truncated,
                }),
                ToolExecutionOutcome::HttpResponse {
                    http_status: status,
                },
            ));
        }

        let parsed: WebSearchResponse = match serde_json::from_str(&text) {
            Ok(parsed) => parsed,
            Err(err) => {
                let (body, truncated) = truncate_chars(&text, 1_000);
                return Ok(ToolExecution::business_failure(json!({
                    "provider": "zhipu",
                    "http_status": status,
                    "body": body,
                    "truncated": truncated,
                    "error": format!("web_search 响应解析失败: {err}"),
                })));
            }
        };
        let (search_result, truncated) = trim_web_search_results(
            parsed.search_result.unwrap_or_default(),
            count,
            &self.limits,
        );
        Ok(ToolExecution::new(
            json!({
                "provider": "zhipu",
                "http_status": status,
                "search_engine": WEB_SEARCH_ENGINE,
                "query": parsed
                    .search_intent
                    .as_ref()
                    .and_then(|items| items.first())
                    .map(|item| item.query.clone())
                    .unwrap_or_else(|| body["search_query"].as_str().unwrap_or("").to_string()),
                "request_id": parsed.request_id,
                "search_intent": parsed.search_intent,
                "search_result": search_result,
                "truncated": truncated,
            }),
            ToolExecutionOutcome::HttpResponse {
                http_status: status,
            },
        ))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct WebSearchIntent {
    query: String,
    intent: String,
    keywords: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct WebSearchResult {
    title: String,
    content: String,
    link: String,
    media: String,
    icon: String,
    refer: String,
    publish_date: String,
}

#[derive(Debug, Deserialize)]
struct WebSearchResponse {
    request_id: Option<String>,
    search_intent: Option<Vec<WebSearchIntent>>,
    search_result: Option<Vec<WebSearchResult>>,
}

fn trim_web_search_results(
    results: Vec<WebSearchResult>,
    max_results: usize,
    limits: &ToolLimits,
) -> (Vec<WebSearchResult>, bool) {
    let mut trimmed = Vec::new();
    let mut total_chars = 0usize;
    let total_results = results.len();
    let mut truncated = false;
    for item in results.into_iter().take(max_results) {
        let mut item = item;
        item.content = truncate_chars(&item.content, limits.web_search_max_content_chars).0;
        let item_chars = item.title.len()
            + item.content.len()
            + item.link.len()
            + item.media.len()
            + item.icon.len()
            + item.refer.len()
            + item.publish_date.len();
        if total_chars + item_chars > limits.web_search_max_total_chars {
            truncated = true;
            break;
        }
        total_chars += item_chars;
        if item.content.len() >= limits.web_search_max_content_chars {
            truncated = true;
        }
        trimmed.push(item);
    }
    if trimmed.len() < total_results {
        truncated = true;
    }
    (trimmed, truncated)
}
