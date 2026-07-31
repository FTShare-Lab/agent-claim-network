//! session_search 的 FTS5 query 轻量规范化。
//!
//! 主模型可以构造 FTS5 query，但自然语言里常见的 `-`、`.`、悬空布尔操作符等
//! 容易触发 FTS5 syntax error。这里只做轻量 sanitize：保留短语、布尔和前缀，
//! 同时把易碎 token quote 起来。

pub(crate) fn sanitize_fts5_query(query: &str) -> String {
    let mut tokens = Vec::new();
    let mut chars = query.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '"' {
            let mut phrase = String::new();
            let mut closed = false;
            for next in chars.by_ref() {
                if next == '"' {
                    closed = true;
                    break;
                }
                phrase.push(next);
            }
            if closed {
                let phrase = phrase.trim();
                if !phrase.is_empty() {
                    tokens.push(format!("\"{}\"", phrase.replace('"', " ")));
                }
            } else {
                tokens.extend(sanitize_unquoted_token(&phrase));
            }
            continue;
        }

        let mut raw = String::from(ch);
        while let Some(next) = chars.peek() {
            if next.is_whitespace() {
                break;
            }
            raw.push(*next);
            chars.next();
        }
        tokens.extend(sanitize_unquoted_token(&raw));
    }

    let mut cleaned = Vec::new();
    let mut iter = tokens.into_iter().peekable();
    while let Some(token) = iter.next() {
        let token = if is_boolean_operator(&token) {
            token.to_ascii_uppercase()
        } else {
            token
        };
        if token == "NOT" {
            match cleaned.last().map(String::as_str) {
                Some("AND") => {
                    cleaned.pop();
                    cleaned.push(token);
                }
                Some("OR") => {
                    cleaned.pop();
                    let _ = iter.next();
                }
                Some(previous) if !is_boolean_operator(previous) => cleaned.push(token),
                _ => {
                    let _ = iter.next();
                }
            }
            continue;
        }
        if matches!(token.as_str(), "AND" | "OR")
            && (cleaned.is_empty()
                || cleaned
                    .last()
                    .is_some_and(|previous: &String| is_boolean_operator(previous)))
        {
            continue;
        }
        cleaned.push(token);
    }
    while cleaned
        .last()
        .is_some_and(|token| is_boolean_operator(token))
    {
        cleaned.pop();
    }
    cleaned.join(" ")
}

fn sanitize_unquoted_token(raw: &str) -> Vec<String> {
    let token = raw
        .chars()
        .filter(|ch| !matches!(ch, '+' | '{' | '}' | '(' | ')' | '"' | '^'))
        .collect::<String>();
    let token = token.trim_matches(|ch: char| matches!(ch, ',' | ';' | ':' | '[' | ']'));
    let token = token.trim_start_matches('*');
    let token = collapse_repeated_stars(token);
    let token = token.trim();
    if token.is_empty() {
        return Vec::new();
    }
    if is_boolean_operator(token) {
        return vec![token.to_ascii_uppercase()];
    }
    let needs_quote = token.chars().any(|ch| !(ch.is_alphanumeric() || ch == '*'));
    if needs_quote {
        let quoted = token.trim_matches('*');
        if quoted.is_empty() {
            return Vec::new();
        }
        return vec![format!("\"{}\"", quoted.replace('"', " "))];
    }
    vec![token.to_string()]
}

fn collapse_repeated_stars(token: &str) -> String {
    let mut out = String::new();
    let mut previous_was_star = false;
    for ch in token.chars() {
        if ch == '*' {
            if previous_was_star {
                continue;
            }
            previous_was_star = true;
        } else {
            previous_was_star = false;
        }
        out.push(ch);
    }
    out
}

fn is_boolean_operator(token: &str) -> bool {
    matches!(token.to_ascii_uppercase().as_str(), "AND" | "OR" | "NOT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_fts5_query_keeps_supported_forms_and_quotes_fragile_terms() {
        assert_eq!(
            sanitize_fts5_query("docker OR kubernetes"),
            "docker OR kubernetes"
        );
        assert_eq!(
            sanitize_fts5_query("\"docker networking\""),
            "\"docker networking\""
        );
        assert_eq!(sanitize_fts5_query("python NOT java"), "python NOT java");
        assert_eq!(sanitize_fts5_query("deploy*"), "deploy*");
        assert_eq!(sanitize_fts5_query("docker OR"), "docker");
        assert_eq!(
            sanitize_fts5_query("docker AND NOT java"),
            "docker NOT java"
        );
        assert_eq!(sanitize_fts5_query("docker OR NOT java"), "docker");
        assert_eq!(
            sanitize_fts5_query("chat-send P2.2"),
            "\"chat-send\" \"P2.2\""
        );
    }
}
