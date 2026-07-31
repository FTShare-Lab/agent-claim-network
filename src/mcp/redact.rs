//! MCP 诊断输出脱敏。
//!
//! MCP server 的 URL、stderr 和错误文本都可能来自外部配置或外部进程。
//! 这里提供 CLI/TUI 共用的保守脱敏，避免把 token、bearer header 或带凭证 URL 打到界面。

pub fn redact_mcp_sensitive_text(value: &str) -> String {
    let value = redact_urls(value);
    redact_sensitive_tokens(&value)
}

fn redact_urls(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut cursor = 0usize;
    while let Some((start, _scheme_len)) = find_next_url(&value[cursor..]) {
        let start = cursor + start;
        out.push_str(&value[cursor..start]);
        let end = url_end(value, start);
        let (body, suffix) = split_trailing_punctuation(&value[start..end]);
        out.push_str(&redact_url_segment(body));
        out.push_str(suffix);
        cursor = end;
    }
    out.push_str(&value[cursor..]);
    out
}

fn find_next_url(value: &str) -> Option<(usize, usize)> {
    let lowered = value.to_ascii_lowercase();
    let http = lowered
        .find("http://")
        .map(|index| (index, "http://".len()));
    let https = lowered
        .find("https://")
        .map(|index| (index, "https://".len()));
    match (http, https) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(found), None) | (None, Some(found)) => Some(found),
        (None, None) => None,
    }
}

fn url_end(value: &str, start: usize) -> usize {
    value[start..]
        .char_indices()
        .find(|(_, ch)| ch.is_ascii_whitespace() || matches!(ch, '"' | '\'' | '<' | '>'))
        .map(|(index, _)| start + index)
        .unwrap_or(value.len())
}

fn split_trailing_punctuation(value: &str) -> (&str, &str) {
    let split = value
        .char_indices()
        .rev()
        .find(|(_, ch)| !matches!(ch, ')' | ']' | '}' | ',' | ';' | '.'))
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0);
    value.split_at(split)
}

fn redact_url_segment(url: &str) -> String {
    let mut redacted = redact_url_userinfo(url);
    redacted = redact_url_tail(&redacted, '?', "?<redacted>");
    redact_url_tail(&redacted, '#', "#<redacted>")
}

fn redact_url_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let authority_start = scheme_end + "://".len();
    let authority_end = url[authority_start..]
        .find(|ch| ['/', '?', '#'].contains(&ch))
        .map(|index| authority_start + index)
        .unwrap_or(url.len());
    let authority = &url[authority_start..authority_end];
    let Some(at_index) = authority.rfind('@') else {
        return url.to_string();
    };
    format!(
        "{}<redacted>@{}{}",
        &url[..authority_start],
        &authority[at_index + 1..],
        &url[authority_end..]
    )
}

fn redact_url_tail(url: &str, marker: char, replacement: &str) -> String {
    let Some(index) = url.find(marker) else {
        return url.to_string();
    };
    format!("{}{}", &url[..index], replacement)
}

fn redact_sensitive_tokens(value: &str) -> String {
    let lowered = value.to_ascii_lowercase();
    if lowered.contains("authorization:") || lowered.contains("bearer ") {
        return "<redacted>".to_string();
    }
    let mut redact_next = false;
    let mut out = Vec::new();
    for token in value.split_whitespace() {
        if redact_next {
            out.push("<redacted>".to_string());
            redact_next = false;
            continue;
        }
        if let Some((key, _)) = token.split_once('=') {
            if is_sensitive_name(clean_key(key)) {
                out.push(format!("{key}=<redacted>"));
                continue;
            }
        }
        if let Some((key, value)) = token.split_once(':') {
            if !value.is_empty() && is_sensitive_name(clean_key(key)) {
                out.push(format!("{key}:<redacted>"));
                continue;
            }
        }
        let key = clean_key(token.trim_end_matches(':'));
        if is_sensitive_name(key) {
            out.push(token.to_string());
            redact_next = true;
        } else {
            out.push(token.to_string());
        }
    }
    out.join(" ")
}

fn clean_key(value: &str) -> &str {
    value
        .trim_matches(|ch: char| {
            !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
        })
        .trim_start_matches('-')
}

fn is_sensitive_name(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase().replace(['-', '.'], "_");
    lowered.contains("token")
        || lowered.contains("api_key")
        || lowered.contains("apikey")
        || lowered.contains("secret")
        || lowered.contains("password")
        || lowered == "key"
        || lowered.ends_with("_key")
        || lowered.contains("bearer")
        || lowered.contains("auth")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_urls_inside_wrappers() {
        assert_eq!(
            redact_mcp_sensitive_text(r#"url="https://user:pass@example.test/mcp?token=abc"."#),
            r#"url="https://<redacted>@example.test/mcp?<redacted>"."#
        );
        assert_eq!(
            redact_mcp_sensitive_text("HTTPS://user:pass@example.test/mcp?token=abc"),
            "HTTPS://<redacted>@example.test/mcp?<redacted>"
        );
    }

    #[test]
    fn redacts_sensitive_assignments_and_headers() {
        assert_eq!(
            redact_mcp_sensitive_text("OPENAI_API_KEY=secret"),
            "OPENAI_API_KEY=<redacted>"
        );
        assert_eq!(
            redact_mcp_sensitive_text("X-API-Key: sk-test"),
            "X-API-Key: <redacted>"
        );
        assert_eq!(
            redact_mcp_sensitive_text("X-API-Key:sk-test"),
            "X-API-Key:<redacted>"
        );
        assert_eq!(
            redact_mcp_sensitive_text("token:sk-test"),
            "token:<redacted>"
        );
        assert_eq!(
            redact_mcp_sensitive_text("password:abc"),
            "password:<redacted>"
        );
        assert_eq!(redact_mcp_sensitive_text("secret abc"), "secret <redacted>");
        assert_eq!(
            redact_mcp_sensitive_text("Authorization: Bearer abc"),
            "<redacted>"
        );
    }
}
