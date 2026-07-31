//! 输入框 Option+←/→ 词跳转边界计算。
//!
//! 两个方向都落在"词首"：空白与标点（含 `/` `@` `-`）是分隔符，`_` 连词，
//! 数字间的 `.`/`,` 连词，字母间的 `'` 连词。连续 CJK 段视作一个词整段跳过，
//! 只停在空白或标点边界。

use std::ops::Range;

use unicode_segmentation::UnicodeSegmentation;

/// Option+Left 目标：光标左侧最近的词首；没有词则回到 0。
pub(super) fn prev_word_boundary(text: &str, cursor: usize) -> usize {
    let placeholders = generated_placeholder_ranges(text);
    let cursor = placeholder_containing(&placeholders, cursor).map_or(cursor, |range| range.start);
    word_ranges(text, &placeholders)
        .into_iter()
        .rev()
        .find(|range| range.start < cursor)
        .map(|range| range.start)
        .unwrap_or(0)
}

/// Option+Right 目标：光标右侧最近的词首；没有下一个词则到文本末尾。
pub(super) fn next_word_boundary(text: &str, cursor: usize) -> usize {
    let placeholders = generated_placeholder_ranges(text);
    let cursor = placeholder_containing(&placeholders, cursor).map_or(cursor, |range| range.end);
    word_ranges(text, &placeholders)
        .into_iter()
        .find(|range| range.start > cursor)
        .map(|range| range.start)
        .unwrap_or(text.len())
}

/// 扫描全文，返回所有可停留的"词"字节区间（词 = 连续词字符段，含连接符规则）。
///
/// ACN 自己生成的大粘贴和剪贴板图片占位符是编辑器实体，不是普通文本词；
/// Option 跳转必须越过它们，避免进入占位符内部的说明文字或编号。
fn word_ranges(text: &str, placeholders: &[Range<usize>]) -> Vec<Range<usize>> {
    let graphemes: Vec<(usize, &str)> = text.grapheme_indices(true).collect();
    let mut ranges = Vec::new();
    let mut index = 0;
    let mut placeholder_index = 0;
    while index < graphemes.len() {
        let grapheme_start = graphemes[index].0;
        while placeholders
            .get(placeholder_index)
            .is_some_and(|range| range.end <= grapheme_start)
        {
            placeholder_index += 1;
        }
        if placeholders
            .get(placeholder_index)
            .is_some_and(|range| range.start <= grapheme_start && grapheme_start < range.end)
        {
            index += 1;
            continue;
        }
        if !is_word_grapheme(graphemes[index].1) {
            index += 1;
            continue;
        }
        let start = graphemes[index].0;
        let mut last = index;
        loop {
            let next = last + 1;
            if next < graphemes.len() && is_word_grapheme(graphemes[next].1) {
                last = next;
                continue;
            }
            // 连接符：数字间的 . / , 与字母间的 ' 并入当前词，
            // 例如 "3.14"、"1,000"、"don't"。
            if next + 1 < graphemes.len()
                && is_word_joiner(graphemes[last].1, graphemes[next].1, graphemes[next + 1].1)
            {
                last = next + 1;
                continue;
            }
            break;
        }
        let (last_start, last_grapheme) = graphemes[last];
        ranges.push(start..last_start + last_grapheme.len());
        index = last + 1;
    }
    ranges
}

/// 返回 ACN 生成的 `[Pasted Content …]` 与 `[Image #N]` 占位符范围。
fn generated_placeholder_ranges(text: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut search_start = 0;
    while let Some(relative_start) = text[search_start..].find('[') {
        let start = search_start + relative_start;
        if let Some(length) = generated_placeholder_length(&text[start..]) {
            let end = start + length;
            ranges.push(start..end);
            search_start = end;
        } else {
            // '[' 为单字节 ASCII 字符，向后一字节仍是有效 UTF-8 边界。
            search_start = start + 1;
        }
    }
    ranges
}

/// 解析当前位置是否为 ACN 的可见占位符，并返回其 UTF-8 字节长度。
fn generated_placeholder_length(text: &str) -> Option<usize> {
    const IMAGE_PREFIX: &str = "[Image #";
    const PASTED_PREFIX: &str = "[Pasted Content ";

    if let Some(rest) = text.strip_prefix(IMAGE_PREFIX) {
        let digits = leading_ascii_digit_count(rest);
        let suffix = rest.get(digits..)?;
        return (digits > 0 && suffix.starts_with(']')).then_some(IMAGE_PREFIX.len() + digits + 1);
    }

    let mut consumed = PASTED_PREFIX.len();
    let rest = text.strip_prefix(PASTED_PREFIX)?;
    let char_count_digits = leading_ascii_digit_count(rest);
    if char_count_digits == 0 {
        return None;
    }
    consumed += char_count_digits;
    let rest = text.get(consumed..)?.strip_prefix(" chars")?;
    consumed += " chars".len();

    if rest.starts_with(']') {
        return Some(consumed + 1);
    }

    let rest = rest.strip_prefix(" #")?;
    let sequence_digits = leading_ascii_digit_count(rest);
    let suffix = rest.get(sequence_digits..)?;
    (sequence_digits > 0 && suffix.starts_with(']')).then_some(consumed + 2 + sequence_digits + 1)
}

fn leading_ascii_digit_count(text: &str) -> usize {
    text.bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count()
}

fn placeholder_containing(placeholders: &[Range<usize>], cursor: usize) -> Option<&Range<usize>> {
    placeholders
        .iter()
        .find(|range| range.start < cursor && cursor < range.end)
}

/// 词字符：字母 / 数字 / 下划线（含 CJK；组合标记跟随基字符成一个 grapheme）。
fn is_word_grapheme(grapheme: &str) -> bool {
    grapheme
        .chars()
        .next()
        .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
}

fn is_word_joiner(prev: &str, joiner: &str, next: &str) -> bool {
    let prev_last = prev.chars().last();
    let next_first = next.chars().next();
    match joiner {
        "." | "," => {
            prev_last.is_some_and(char::is_numeric) && next_first.is_some_and(char::is_numeric)
        }
        "'" | "\u{2019}" => {
            prev_last.is_some_and(char::is_alphabetic)
                && next_first.is_some_and(char::is_alphabetic)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prev_word_skips_run_to_word_start() {
        let text = "hello world";
        assert_eq!(prev_word_boundary(text, text.len()), 6);
        assert_eq!(prev_word_boundary(text, 6), 0);
        // 词内任意位置 → 本词词首
        assert_eq!(prev_word_boundary(text, 8), 6);
        assert_eq!(prev_word_boundary(text, 3), 0);
        assert_eq!(prev_word_boundary(text, 0), 0);
    }

    #[test]
    fn next_word_lands_on_next_word_start() {
        let text = "hello world";
        assert_eq!(next_word_boundary(text, 0), 6);
        assert_eq!(next_word_boundary(text, 3), 6);
        // 已无下一个词首 → 文本末尾
        assert_eq!(next_word_boundary(text, 6), text.len());
        assert_eq!(next_word_boundary(text, text.len()), text.len());
    }

    #[test]
    fn slash_and_at_are_separators() {
        // 对齐用户截图场景：hiahi/home/whate@ver/this
        let text = "hiahi/home/whate@ver/this";
        let starts = [0, 6, 11, 17, 21];
        assert_eq!(prev_word_boundary(text, text.len()), 21);
        assert_eq!(prev_word_boundary(text, 21), 17);
        assert_eq!(prev_word_boundary(text, 17), 11);
        for window in starts.windows(2) {
            assert_eq!(next_word_boundary(text, window[0]), window[1]);
        }
    }

    #[test]
    fn cjk_run_jumps_as_one_word() {
        let text = "今天几月几 what are words";
        let cjk_end = "今天几月几".len();
        assert_eq!(prev_word_boundary(text, cjk_end), 0);
        assert_eq!(
            prev_word_boundary(text, text.len()),
            text.len() - "words".len()
        );
        assert_eq!(next_word_boundary(text, 0), cjk_end + 1);
        // CJK 与 latin 紧邻（无分隔符）时合成一个词，整段跳过
        let glued = "今天几月几what are";
        assert_eq!(next_word_boundary(glued, 0), "今天几月几what ".len());
        assert_eq!(prev_word_boundary(glued, "今天几月几what".len()), 0);
    }

    #[test]
    fn underscore_joins_and_hyphen_splits() {
        let text = "snake_case kebab-case";
        assert_eq!(next_word_boundary(text, 0), 11);
        assert_eq!(prev_word_boundary(text, 10), 0);
        // kebab-case 中 '-' 是分隔符
        assert_eq!(next_word_boundary(text, 11), 17);
        assert_eq!(prev_word_boundary(text, text.len()), 17);
    }

    #[test]
    fn digits_join_across_decimal_and_letters_join_across_apostrophe() {
        let text = "pi 3.14 x 1,000 don't stop";
        assert_eq!(prev_word_boundary(text, 7), 3); // 3.14 是一个词
        assert_eq!(next_word_boundary(text, 3), 8); // 跳过 3.14 → x
        assert_eq!(prev_word_boundary(text, 15), 10); // 1,000 是一个词
        assert_eq!(prev_word_boundary(text, 21), 16); // don't 是一个词
                                                      // 词尾/词首非数字时 . 不连接
        assert_eq!(next_word_boundary("a.b", 0), 2);
    }

    #[test]
    fn leading_and_trailing_separators_fall_back_to_edges() {
        assert_eq!(prev_word_boundary("   abc", 2), 0);
        assert_eq!(next_word_boundary("abc   ", 1), 6);
        assert_eq!(prev_word_boundary("", 0), 0);
        assert_eq!(next_word_boundary("", 0), 0);
        assert_eq!(prev_word_boundary("///", 3), 0);
        assert_eq!(next_word_boundary("///", 0), 3);
    }

    #[test]
    fn multiline_text_treats_newline_as_separator() {
        let text = "abc\ndef";
        assert_eq!(next_word_boundary(text, 0), 4);
        assert_eq!(prev_word_boundary(text, 4), 0);
        assert_eq!(prev_word_boundary(text, text.len()), 4);
    }

    #[test]
    fn generated_paste_and_image_placeholders_are_skipped_as_a_whole() {
        let pasted = "[Pasted Content 1200 chars]";
        let image = "[Image #2]";
        let text = format!("before {pasted} after {image} end");
        let after_start = format!("before {pasted} ").len();
        let end_start = format!("before {pasted} after {image} ").len();
        let pasted_interior = "before [Pasted".len();

        // 从普通词跨过占位符时，不应落到其内部的 Content / chars / #1 等片段。
        assert_eq!(next_word_boundary(&text, 0), after_start);
        assert_eq!(prev_word_boundary(&text, after_start), 0);
        assert_eq!(next_word_boundary(&text, after_start), end_start);
        assert_eq!(prev_word_boundary(&text, end_start), after_start);
        // 即使光标已经由单字符移动进入占位符，Option 跳转也应直接离开整个占位符。
        assert_eq!(prev_word_boundary(&text, pasted_interior), 0);
        assert_eq!(next_word_boundary(&text, pasted_interior), after_start);

        let numbered_paste = "[Pasted Content 1200 chars #1]";
        assert_eq!(
            generated_placeholder_length(numbered_paste),
            Some(numbered_paste.len())
        );
    }
}
