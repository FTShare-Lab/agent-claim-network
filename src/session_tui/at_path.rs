//! 输入框内联 `@path` 附件标记的词法解析。
//!
//! 只负责词法扫描（触发边界、引号、反斜杠转义）与高亮分段；
//! 文件系统校验（存在性 / 类型 / 大小 / 数量）由提交侧通过 attachment 层完成。

use std::fmt;
use std::ops::Range;

/// 光标所在的、仍可继续补全的 `@path` 标记。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveAtPathToken {
    /// 含 `@` 前缀的替换区间。
    pub(super) range: Range<usize>,
    /// 已按现有词法规则反引号/反斜杠解析的路径文本。
    pub(super) raw_path: String,
}

/// 一次扫描出的 `@path` 标记。`range` 是含 `@` 前缀的字节区间，用于高亮。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AtPathToken {
    pub(super) range: Range<usize>,
    pub(super) parsed: Result<String, AtPathParseError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AtPathParseError {
    EmptyPath,
    UnclosedQuote,
}

impl fmt::Display for AtPathParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPath => f.write_str("@ 后缺少路径"),
            Self::UnclosedQuote => f.write_str("@ 路径引号未闭合"),
        }
    }
}

/// 扫描文本中的全部 `@path` 标记。`@` 仅在行首 / 空白后触发，
/// 支持 `@"a b"`、`@'a b'` 与 `@a\ b` 三种带空格写法。
pub(super) fn scan_at_path_tokens(text: &str) -> Vec<AtPathToken> {
    let mut tokens = Vec::new();
    let mut prev_char: Option<char> = None;
    let mut iter = text.char_indices().peekable();
    while let Some((index, ch)) = iter.next() {
        if ch == '@' && prev_char.is_none_or(char::is_whitespace) {
            let token = lex_token(text, index);
            let end = token.range.end;
            tokens.push(token);
            while iter.peek().is_some_and(|&(j, _)| j < end) {
                iter.next();
            }
            // 标记结束后紧贴的字符不构成新边界（如 @"a"@b 中第二个 @）
            prev_char = text[..end].chars().last();
            continue;
        }
        prev_char = Some(ch);
    }
    tokens
}

/// 查找光标所在（或紧贴末尾）的 `@path` 标记，供实时候选菜单使用。
/// 已闭合引号后光标移出 token 时不再激活；未闭合引号仍可继续补全。
pub(super) fn active_at_path_token(text: &str, cursor: usize) -> Option<ActiveAtPathToken> {
    if cursor > text.len() || !text.is_char_boundary(cursor) {
        return None;
    }
    let token = scan_at_path_tokens(text)
        .into_iter()
        .find(|token| token.range.start < cursor && cursor <= token.range.end)?;
    if cursor != token.range.end {
        return None;
    }
    let raw_path = match token.parsed {
        Ok(path) => path,
        Err(AtPathParseError::EmptyPath) => String::new(),
        Err(AtPathParseError::UnclosedQuote) => parse_unclosed_quoted_path(text, &token.range)?,
    };
    Some(ActiveAtPathToken {
        range: token.range,
        raw_path,
    })
}

fn parse_unclosed_quoted_path(text: &str, range: &Range<usize>) -> Option<String> {
    let token = text.get(range.clone())?;
    let mut chars = token.chars();
    (chars.next()? == '@').then_some(())?;
    matches!(chars.next()?, '"' | '\'').then_some(())?;
    Some(chars.collect())
}

fn lex_token(text: &str, at_index: usize) -> AtPathToken {
    let after = &text[at_index + 1..];
    let mut chars = after.char_indices().peekable();
    match chars.peek().copied() {
        None => AtPathToken {
            range: at_index..at_index + 1,
            parsed: Err(AtPathParseError::EmptyPath),
        },
        Some((_, c)) if c.is_whitespace() => AtPathToken {
            range: at_index..at_index + 1,
            parsed: Err(AtPathParseError::EmptyPath),
        },
        Some((_, quote)) if quote == '"' || quote == '\'' => {
            chars.next();
            let mut path = String::new();
            for (j, c) in chars {
                if c == quote {
                    let end = at_index + 1 + j + c.len_utf8();
                    let parsed = if path.is_empty() {
                        Err(AtPathParseError::EmptyPath)
                    } else {
                        Ok(path)
                    };
                    return AtPathToken {
                        range: at_index..end,
                        parsed,
                    };
                }
                path.push(c);
            }
            AtPathToken {
                range: at_index..text.len(),
                parsed: Err(AtPathParseError::UnclosedQuote),
            }
        }
        Some(_) => {
            let mut path = String::new();
            let mut escaped = false;
            let mut end = text.len();
            while let Some((j, c)) = chars.next() {
                if escaped {
                    path.push(c);
                    escaped = false;
                    continue;
                }
                if c == '\\' {
                    let escaped_char = chars.peek().map(|(_, next)| *next);
                    if escaped_char.is_some_and(|next| next.is_whitespace() || next == '\\') {
                        escaped = true;
                        continue;
                    }
                }
                if c.is_whitespace() {
                    end = at_index + 1 + j;
                    break;
                }
                path.push(c);
            }
            if escaped {
                path.push('\\');
            }
            AtPathToken {
                range: at_index..end,
                parsed: Ok(path),
            }
        }
    }
}

/// 把一段可见文本（`line_start` 为其在全文中的字节偏移）按标记区间切段，
/// 返回 `(segment, 命中的区间下标)`，供输入框 / transcript 渲染上色，
/// 下标让调用方能对特定标记（如光标所在的那个）叠加额外样式。
pub(super) fn split_at_path_segments(
    line_text: &str,
    line_start: usize,
    token_ranges: &[Range<usize>],
) -> Vec<(String, Option<usize>)> {
    let line_end = line_start + line_text.len();
    let mut segments = Vec::new();
    let mut cursor = line_start;
    for (index, range) in token_ranges.iter().enumerate() {
        if range.end <= cursor || range.start >= line_end {
            continue;
        }
        let seg_start = range.start.max(cursor);
        let seg_end = range.end.min(line_end);
        if seg_start > cursor {
            if let Some(plain) = line_text.get(cursor - line_start..seg_start - line_start) {
                segments.push((plain.to_string(), None));
            }
        }
        if let Some(hit) = line_text.get(seg_start - line_start..seg_end - line_start) {
            segments.push((hit.to_string(), Some(index)));
        }
        cursor = seg_end;
    }
    if cursor < line_end {
        if let Some(rest) = line_text.get(cursor - line_start..) {
            segments.push((rest.to_string(), None));
        }
    }
    if segments.is_empty() {
        segments.push((line_text.to_string(), None));
    }
    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_paths(text: &str) -> Vec<String> {
        scan_at_path_tokens(text)
            .into_iter()
            .filter_map(|token| token.parsed.ok())
            .collect()
    }

    #[test]
    fn at_triggers_only_at_start_or_after_whitespace() {
        assert_eq!(ok_paths("@a.txt"), vec!["a.txt"]);
        assert_eq!(ok_paths("看下 @docs/b.md 的内容"), vec!["docs/b.md"]);
        assert!(scan_at_path_tokens("user@example.com").is_empty());
        assert!(scan_at_path_tokens("路径a@b不触发").is_empty());
    }

    #[test]
    fn bare_path_ends_at_first_unescaped_whitespace() {
        assert_eq!(ok_paths("@a.txt 后面"), vec!["a.txt"]);
        assert_eq!(ok_paths("@a\\ b.pdf x"), vec!["a b.pdf"]);
        assert_eq!(ok_paths(r"@dir\file.txt"), vec![r"dir\file.txt"]);
    }

    #[test]
    fn quoted_paths_keep_spaces() {
        assert_eq!(ok_paths(r#"@"a b.png" 之后"#), vec!["a b.png"]);
        assert_eq!(ok_paths("@'c d.txt'"), vec!["c d.txt"]);
    }

    #[test]
    fn multiple_tokens_in_one_line() {
        assert_eq!(ok_paths("@a.txt 和 @b.pdf"), vec!["a.txt", "b.pdf"]);
    }

    #[test]
    fn empty_after_at_is_a_parse_error() {
        let tokens = scan_at_path_tokens("@ 空的");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].parsed, Err(AtPathParseError::EmptyPath));
        let tokens = scan_at_path_tokens("结尾 @");
        assert_eq!(tokens[0].parsed, Err(AtPathParseError::EmptyPath));
    }

    #[test]
    fn unclosed_quote_is_a_parse_error() {
        let tokens = scan_at_path_tokens(r#"@"a b.png"#);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].parsed, Err(AtPathParseError::UnclosedQuote));
        assert_eq!(tokens[0].range, 0..r#"@"a b.png"#.len());
    }

    #[test]
    fn token_range_covers_at_prefix() {
        let text = "看 @a.txt 内容";
        let tokens = scan_at_path_tokens(text);
        assert_eq!(&text[tokens[0].range.clone()], "@a.txt");
    }

    #[test]
    fn split_segments_marks_at_ranges() {
        let text = "看 @a.txt 内容";
        let ranges = scan_at_path_tokens(text)
            .into_iter()
            .map(|token| token.range)
            .collect::<Vec<_>>();
        let segments = split_at_path_segments(text, 0, &ranges);
        assert_eq!(
            segments,
            vec![
                ("看 ".to_string(), None),
                ("@a.txt".to_string(), Some(0)),
                (" 内容".to_string(), None),
            ]
        );
    }

    #[test]
    fn active_token_tracks_cursor_at_open_token_end() {
        let text = "请看 @src/sess 后面";
        let cursor = text.find(" 后面").unwrap();
        assert_eq!(
            active_at_path_token(text, cursor),
            Some(ActiveAtPathToken {
                range: "请看 ".len()..cursor,
                raw_path: "src/sess".into(),
            })
        );
        assert_eq!(active_at_path_token(text, text.len()), None);
    }

    #[test]
    fn active_token_supports_empty_and_unclosed_quoted_paths() {
        assert_eq!(
            active_at_path_token("@", 1),
            Some(ActiveAtPathToken {
                range: 0..1,
                raw_path: String::new(),
            })
        );
        let text = r#"请看 @"docs/a b"#;
        assert_eq!(
            active_at_path_token(text, text.len()),
            Some(ActiveAtPathToken {
                range: "请看 ".len()..text.len(),
                raw_path: "docs/a b".into(),
            })
        );
    }

    #[test]
    fn split_segments_handles_partial_overlap_across_wrapped_lines() {
        // token 区间 3..9，行片段只覆盖 0..6 —— 高亮只到行尾
        let range = 3..9;
        let segments = split_at_path_segments("ab @a.", 0, std::slice::from_ref(&range));
        assert_eq!(
            segments,
            vec![("ab ".to_string(), None), ("@a.".to_string(), Some(0))]
        );
    }
}
