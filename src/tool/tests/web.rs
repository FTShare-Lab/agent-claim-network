//! Web、working note、ask_user 与动态定义测试。
//!
//! 覆盖 HTTP 成败响应、有界输出、配置化搜索与辅助交互工具。

use super::*;

#[tokio::test]
async fn web_fetch_fetches_http_url() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let body = "web body";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let registry =
        ToolRegistry::new(&test_tool_config(tempfile::tempdir().unwrap().path())).unwrap();
    let result = registry
        .dispatch(
            "web_fetch",
            serde_json::json!({ "url": format!("http://127.0.0.1:{port}/") }),
        )
        .await
        .unwrap();

    assert_eq!(
        result.outcome,
        ToolExecutionOutcome::HttpResponse { http_status: 200 }
    );
    assert_eq!(result.output["http_status"], 200);
    assert_eq!(result.output["body"], "web body");
}

#[tokio::test]
async fn web_fetch_non_success_http_response_is_typed_and_keeps_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await;
        let body = "missing resource";
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let registry =
        ToolRegistry::new(&test_tool_config(tempfile::tempdir().unwrap().path())).unwrap();
    let result = registry
        .dispatch(
            "web_fetch",
            serde_json::json!({ "url": format!("http://127.0.0.1:{port}/missing") }),
        )
        .await
        .unwrap();

    assert_eq!(
        result.outcome,
        ToolExecutionOutcome::HttpResponse { http_status: 404 }
    );
    assert!(!result.outcome.is_success());
    assert_eq!(result.output["http_status"], 404);
    assert_eq!(result.output["body"], "missing resource");
}

#[tokio::test]
async fn web_request_posts_to_endpoint_and_parses_body() {
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_clone = captured.clone();

    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = match sock.read(&mut tmp).await {
                Ok(n) => n,
                Err(_) => return,
            };
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let header_text = String::from_utf8_lossy(&buf[..pos + 4]).to_string();
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                while buf.len() < pos + 4 + content_length {
                    let n = match sock.read(&mut tmp).await {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                break;
            }
        }
        *captured_clone.lock().await = String::from_utf8_lossy(&buf).to_string();
        let body = r#"{"ok":true,"message":"hello"}"#;
        let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n{}",
                body.len(),
                body
            );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let registry = ToolRegistry::new_with_web_search(
        &test_tool_config(tempfile::tempdir().unwrap().path()),
        format!("http://127.0.0.1:{port}/"),
        Some("example_test_key".into()),
    )
    .unwrap();
    let result = registry
        .dispatch(
            "web_request",
            serde_json::json!({
                "method": "POST",
                "url": format!("http://127.0.0.1:{port}/api"),
                "headers": [{"name":"X-Test","value":"abc"}],
                "query": {"page":"1"},
                "body": {"q":"rust async"}
            }),
        )
        .await
        .unwrap();

    let request = captured.lock().await.clone();
    let request_lower = request.to_ascii_lowercase();
    assert!(request_lower.contains("post /api?page=1 http/1.1"));
    assert!(request_lower.contains("x-test: abc"));
    assert!(request.contains(r#""q":"rust async""#));
    assert_eq!(result.output["method"], "POST");
    assert_eq!(result.output["http_status"], 200);
    assert_eq!(result.output["body"]["ok"], true);
    assert_eq!(result.output["body"]["message"], "hello");
}

#[tokio::test]
async fn web_request_reads_response_with_bounded_output() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let body = "x".repeat(16_000);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
    });
    let mut cfg = test_tool_config(tempfile::tempdir().unwrap().path());
    cfg.web.lookup_max_chars = 64;
    let registry = ToolRegistry::new(&cfg).unwrap();

    let result = registry
        .dispatch(
            "web_request",
            serde_json::json!({
                "method": "GET",
                "url": format!("http://127.0.0.1:{port}/large")
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.output["http_status"], 200);
    assert_eq!(result.output["truncated"], true);
    assert!(
        result.output["body"]["_raw"]
            .as_str()
            .unwrap()
            .chars()
            .count()
            <= 64
    );
}

#[tokio::test]
async fn web_search_missing_key_mentions_configured_env() {
    let mut cfg = test_tool_config(tempfile::tempdir().unwrap().path());
    cfg.web.api_key_env = "EXAMPLE_WEB_SEARCH_KEY".into();
    let registry = ToolRegistry::new(&cfg).unwrap();

    let err = registry
        .dispatch(
            "web_search",
            serde_json::json!({
                "query": "agent claim network",
            }),
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("EXAMPLE_WEB_SEARCH_KEY"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn web_search_uses_configured_endpoint_and_api_key_env() {
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_clone = captured.clone();

    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = match sock.read(&mut tmp).await {
                Ok(n) => n,
                Err(_) => return,
            };
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let header_text = String::from_utf8_lossy(&buf[..pos + 4]).to_string();
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                while buf.len() < pos + 4 + content_length {
                    let n = match sock.read(&mut tmp).await {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                break;
            }
        }
        *captured_clone.lock().await = String::from_utf8_lossy(&buf).to_string();
        let body = r#"{"request_id":"req-test","search_intent":[{"query":"rust async","intent":"search","keywords":"rust async"}],"search_result":[{"title":"Rust","link":"https://example.com/rust","content":"async result","media":"","icon":"","refer":"","publish_date":""}]}"#;
        let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n{}",
                body.len(),
                body
            );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let _env = EnvVarGuard::set("ACN_TEST_WEB_SEARCH_KEY", "configured-search-key");
    let mut cfg = test_tool_config(tempfile::tempdir().unwrap().path());
    cfg.web.endpoint = format!("http://127.0.0.1:{port}/custom-search");
    cfg.web.api_key_env = "ACN_TEST_WEB_SEARCH_KEY".into();
    let registry = ToolRegistry::new(&cfg).unwrap();

    let result = registry
        .dispatch(
            "web_search",
            serde_json::json!({
                "query": "rust async",
                "count": 1
            }),
        )
        .await
        .unwrap();

    let request = captured.lock().await.clone();
    let request_lower = request.to_ascii_lowercase();
    assert!(request_lower.contains("post /custom-search http/1.1"));
    assert!(request_lower.contains("authorization: bearer configured-search-key"));
    assert!(request.contains(r#""search_query":"rust async""#));
    assert!(request.contains(r#""search_engine":"search_pro""#));
    assert!(request.contains(r#""user_id":"agent_claim_network""#));
    assert_eq!(
        result.outcome,
        ToolExecutionOutcome::HttpResponse { http_status: 200 }
    );
    assert_eq!(result.output["http_status"], 200);
    assert_eq!(result.output["request_id"], "req-test");
    assert_eq!(result.output["search_result"][0]["title"], "Rust");
}

#[tokio::test]
async fn web_search_non_success_http_response_is_typed_and_keeps_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let body = "search upstream unavailable";
        let response = format!(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let _env = EnvVarGuard::set("ACN_TEST_WEB_SEARCH_ERROR_KEY", "configured-search-key");
    let mut cfg = test_tool_config(tempfile::tempdir().unwrap().path());
    cfg.web.endpoint = format!("http://127.0.0.1:{port}/custom-search");
    cfg.web.api_key_env = "ACN_TEST_WEB_SEARCH_ERROR_KEY".into();
    let registry = ToolRegistry::new(&cfg).unwrap();

    let result = registry
        .dispatch(
            "web_search",
            serde_json::json!({
                "query": "rust async",
                "count": 1
            }),
        )
        .await
        .unwrap();

    assert_eq!(
        result.outcome,
        ToolExecutionOutcome::HttpResponse { http_status: 503 }
    );
    assert!(!result.outcome.is_success());
    assert_eq!(result.output["http_status"], 503);
    assert_eq!(result.output["body"], "search upstream unavailable");
    assert_eq!(result.output["truncated"], false);
}

#[tokio::test]
async fn web_search_invalid_success_body_is_structured_business_failure() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let body = "not-json";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
    });

    let _env = EnvVarGuard::set("ACN_TEST_WEB_SEARCH_INVALID_KEY", "configured-search-key");
    let mut cfg = test_tool_config(tempfile::tempdir().unwrap().path());
    cfg.web.endpoint = format!("http://127.0.0.1:{port}/custom-search");
    cfg.web.api_key_env = "ACN_TEST_WEB_SEARCH_INVALID_KEY".into();
    let registry = ToolRegistry::new(&cfg).unwrap();

    let result = registry
        .dispatch("web_search", serde_json::json!({"query": "rust async"}))
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::BusinessFailure);
    assert_eq!(result.output["http_status"], 200);
    assert_eq!(result.output["body"], "not-json");
    assert!(result.output["error"]
        .as_str()
        .unwrap()
        .contains("web_search 响应解析失败"));
}

#[tokio::test]
async fn working_note_is_session_local() {
    let registry =
        ToolRegistry::new(&test_tool_config(tempfile::tempdir().unwrap().path())).unwrap();
    registry
        .dispatch(
            "working_note",
            serde_json::json!({ "action": "add", "note": "check config" }),
        )
        .await
        .unwrap();
    let result = registry
        .dispatch("working_note", serde_json::json!({ "action": "list" }))
        .await
        .unwrap();

    assert_eq!(result.output["notes"][0], "check config");
}

#[tokio::test]
async fn ask_user_returns_structured_blocker() {
    let registry =
        ToolRegistry::new(&test_tool_config(tempfile::tempdir().unwrap().path())).unwrap();
    let result = registry
        .dispatch(
            "ask_user",
            serde_json::json!({
                "question": "选哪个文件？",
                "choices": ["a", "b"]
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.outcome, ToolExecutionOutcome::Completed);
    assert_eq!(result.output["needs_user_input"], true);
    assert_eq!(result.output["choices"][1], "b");
}

#[test]
fn web_tool_descriptions_include_dynamic_current_year_guidance() {
    let dir = tempfile::tempdir().unwrap();
    let registry = ToolRegistry::new(&test_tool_config(dir.path())).unwrap();
    let tools = registry.definitions();
    let current_year = Local::now().year().to_string();

    for name in ["web_search", "web_fetch", "web_request"] {
        let description = tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap()
            .description
            .as_str();
        assert!(description.contains(&format!("Current year is {current_year}")));
        assert!(description.contains("latest/current/recent"));
    }
}
