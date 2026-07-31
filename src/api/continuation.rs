//! LLM 输出截断续写的共享策略。
//!
//! 本模块只放 provider-neutral 的策略常量和纯文本拼接 helper。具体如何追加
//! continuation message 由各 provider adapter 按自己的协议格式实现。

pub(crate) const MAX_CONTINUATION_TURNS: usize = 8;
pub(crate) const CONTINUATION_TRIGGER: &str = "继续，从上一条回复被截断处继续，不要重复已写内容。";

/// 把续写片段拼接到累积文本中，并去掉首尾重叠的重复片段。
pub(crate) fn append_with_overlap_dedupe(acc: &mut String, chunk: &str) {
    if chunk.is_empty() {
        return;
    }
    if acc.is_empty() {
        acc.push_str(chunk);
        return;
    }
    if acc.ends_with(chunk) {
        return;
    }

    let start = acc
        .char_indices()
        .map(|(idx, _)| idx)
        .filter(|idx| acc.len() - *idx <= chunk.len())
        .find(|idx| chunk.starts_with(&acc[*idx..]))
        .unwrap_or(acc.len());
    acc.push_str(&chunk[acc.len() - start..]);
}

#[cfg(test)]
mod tests {
    use super::append_with_overlap_dedupe;

    #[test]
    fn append_with_overlap_dedupe_skips_exact_duplicate_chunk() {
        let mut acc = "hello world".to_string();
        append_with_overlap_dedupe(&mut acc, "world");
        assert_eq!(acc, "hello world");
    }

    #[test]
    fn append_with_overlap_dedupe_merges_partial_overlap() {
        let mut acc = "abcde".to_string();
        append_with_overlap_dedupe(&mut acc, "defg");
        assert_eq!(acc, "abcdefg");
    }

    #[test]
    fn append_with_overlap_dedupe_appends_without_overlap() {
        let mut acc = "abc".to_string();
        append_with_overlap_dedupe(&mut acc, "xyz");
        assert_eq!(acc, "abcxyz");
    }
}
