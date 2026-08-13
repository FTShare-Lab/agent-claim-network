//! HTTP client 的代理选择策略。
//!
//! 系统代理实现不一定会同步操作系统的 bypass list，因此发往本机回环地址的请求
//! 由 ACN 明确直连；其他地址继续遵从 reqwest 的环境与系统代理配置。

use std::net::IpAddr;

pub(crate) fn is_loopback_endpoint(endpoint: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let ip_literal = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(address) = ip_literal.parse::<IpAddr>() {
        return address.is_loopback();
    }
    let domain = host.trim_end_matches('.');
    domain.eq_ignore_ascii_case("localhost")
        || domain
            .to_ascii_lowercase()
            .strip_suffix(".localhost")
            .is_some_and(|prefix| !prefix.is_empty())
}

pub(crate) fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
}

pub(crate) fn direct_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().no_proxy()
}

pub(crate) fn http_client_builder_for_endpoint(endpoint: &str) -> reqwest::ClientBuilder {
    apply_endpoint_proxy_policy(reqwest::Client::builder(), endpoint)
}

fn apply_endpoint_proxy_policy(
    builder: reqwest::ClientBuilder,
    endpoint: &str,
) -> reqwest::ClientBuilder {
    if is_loopback_endpoint(endpoint) {
        builder.no_proxy()
    } else {
        builder
    }
}

pub(crate) fn http_client_013_builder() -> reqwest_013::ClientBuilder {
    reqwest_013::Client::builder()
}

pub(crate) fn direct_http_client_013_builder() -> reqwest_013::ClientBuilder {
    reqwest_013::Client::builder().no_proxy()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn loopback_detection_does_not_include_private_or_similar_hosts() {
        for endpoint in [
            "http://localhost:8000/v1",
            "http://localhost.:8000/v1",
            "http://gateway.localhost/v1",
            "http://127.0.0.1:8000/v1",
            "http://127.42.0.9/v1",
            "http://[::1]:8000/v1",
        ] {
            assert!(is_loopback_endpoint(endpoint), "{endpoint}");
        }
        for endpoint in [
            "https://llm.example.test/v1",
            "http://notlocalhost/v1",
            "http://10.0.0.1/v1",
            "http://192.168.0.1/v1",
            "not a url",
        ] {
            assert!(!is_loopback_endpoint(endpoint), "{endpoint}");
        }
    }

    async fn spawn_http_server(body: &'static str) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        address
    }

    #[tokio::test]
    async fn loopback_endpoint_clears_an_explicit_proxy() {
        let destination = spawn_http_server("direct").await;
        let proxy = spawn_http_server("proxy").await;
        let endpoint = format!("http://{destination}/v1");
        let client = apply_endpoint_proxy_policy(
            reqwest::Client::builder().proxy(
                reqwest::Proxy::all(format!("http://{proxy}")).expect("测试代理地址应当有效"),
            ),
            &endpoint,
        )
        .build()
        .unwrap();

        let body = client
            .get(endpoint)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "direct");
    }

    #[tokio::test]
    async fn remote_endpoint_retains_an_explicit_proxy() {
        let proxy = spawn_http_server("proxy").await;
        let endpoint = "http://llm.example.test/v1";
        let client = apply_endpoint_proxy_policy(
            reqwest::Client::builder().proxy(
                reqwest::Proxy::all(format!("http://{proxy}")).expect("测试代理地址应当有效"),
            ),
            endpoint,
        )
        .build()
        .unwrap();

        let body = client
            .get(endpoint)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "proxy");
    }

    #[test]
    fn standard_library_loopback_semantics_cover_both_ip_families() {
        assert!("127.0.0.2".parse::<IpAddr>().unwrap().is_loopback());
        assert!("::1".parse::<IpAddr>().unwrap().is_loopback());
    }
}
