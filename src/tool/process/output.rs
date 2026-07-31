use std::collections::VecDeque;

/// 一条输出流的绝对读取游标；值只在进程 entry 内有效。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OutputCursor(pub(crate) u64);

/// 进程输出的有界快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessOutput {
    pub(crate) bytes: Vec<u8>,
    pub(crate) cursor: OutputCursor,
    pub(crate) truncated: bool,
    pub(crate) omitted_bytes: u64,
    /// 当前 bytes 是否从调用方请求的 cursor 起连续。完整 snapshot 在 head/tail
    /// 之间存在 gap 时为 false；`snapshot_since` 会把 gap 拆成可提交的独立页面。
    pub(crate) page_contiguous: bool,
    /// 当前页之后是否还有 retained 输出需要继续读取；终态 entry 只有最后一页才能移除。
    pub(crate) has_more_retained: bool,
}

/// 按原始 UTF-8 字节数保留稳定 head 与滚动 tail，确保 child 永远持续被 drain。
#[derive(Debug)]
pub(crate) struct BoundedOutput {
    max_bytes: usize,
    head_limit: usize,
    head: Vec<u8>,
    head_sealed: bool,
    tail: VecDeque<u8>,
    head_chars: usize,
    tail_chars: usize,
    /// reader 的 chunk 可以恰好切开 UTF-8 scalar。只在凑齐完整 scalar 后才推进
    /// cursor/容量，以免 head/tail 拼接时制造 replacement character。
    pending_utf8: Vec<u8>,
    cursor: u64,
    omitted_bytes: u64,
    /// drain worker 被强制停止时，内核缓冲区内还可能存在无法计数的尾部字节。该标记
    /// 让调用方看到明确的非完整快照，而不是把已有 cursor 误认为连续完整输出。
    incomplete: bool,
}

impl BoundedOutput {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            head_limit: max_bytes / 2,
            head: Vec::with_capacity(max_bytes / 2),
            head_sealed: false,
            tail: VecDeque::with_capacity(max_bytes.saturating_sub(max_bytes / 2)),
            head_chars: 0,
            tail_chars: 0,
            pending_utf8: Vec::new(),
            cursor: 0,
            omitted_bytes: 0,
            incomplete: false,
        }
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) {
        self.pending_utf8.extend_from_slice(bytes);
        loop {
            let decoded = std::str::from_utf8(&self.pending_utf8);
            match decoded {
                Ok(text) => {
                    let decoded_chars = text.chars().collect::<Vec<_>>();
                    self.pending_utf8.clear();
                    for value in decoded_chars {
                        self.append_char(value);
                    }
                    break;
                }
                Err(error) => {
                    let valid_up_to = error.valid_up_to();
                    if valid_up_to > 0 {
                        let decoded_chars =
                            match std::str::from_utf8(&self.pending_utf8[..valid_up_to]) {
                                // Utf8Error 的 valid_up_to 契约保证这个 prefix 合法；保留错误分支
                                // 只是避免输出 reader 的错误路径通过 panic 破坏 manager 收束。
                                Ok(text) => text.chars().collect::<Vec<_>>(),
                                Err(_) => {
                                    self.incomplete = true;
                                    break;
                                }
                            };
                        self.pending_utf8.drain(..valid_up_to);
                        for value in decoded_chars {
                            self.append_char(value);
                        }
                        continue;
                    }
                    let Some(invalid_bytes) = error.error_len() else {
                        // 这是 reader chunk 末尾的半个 UTF-8 scalar，等待下一次 append。
                        break;
                    };
                    self.pending_utf8.drain(..invalid_bytes);
                    self.append_char(char::REPLACEMENT_CHARACTER);
                }
            }
        }
    }

    /// stream 已确实 EOF 后，把最后一个不完整 scalar 以 Rust lossy UTF-8 的同一规则
    /// 表示为 replacement character。drain 被中断时不应调用本方法，因为后续字节未知。
    pub(crate) fn finish(&mut self) {
        if self.pending_utf8.is_empty() {
            return;
        }
        self.pending_utf8.clear();
        self.append_char(char::REPLACEMENT_CHARACTER);
    }

    fn append_char(&mut self, value: char) {
        self.cursor = self.cursor.saturating_add(1);
        let mut encoded = [0_u8; 4];
        let bytes = value.encode_utf8(&mut encoded).as_bytes();
        if !self.head_sealed && self.head.len().saturating_add(bytes.len()) <= self.head_limit {
            self.head.extend_from_slice(bytes);
            self.head_chars = self.head_chars.saturating_add(1);
            return;
        }
        self.head_sealed = true;
        let tail_limit = self.max_bytes.saturating_sub(self.head.len());
        if bytes.len() > tail_limit {
            self.omitted_bytes = self.omitted_bytes.saturating_add(
                u64::try_from(self.tail.len().saturating_add(bytes.len())).unwrap_or(u64::MAX),
            );
            self.tail.clear();
            self.tail_chars = 0;
            return;
        }
        while self.tail.len().saturating_add(bytes.len()) > tail_limit {
            let removed = pop_front_utf8_scalar(&mut self.tail);
            self.omitted_bytes = self
                .omitted_bytes
                .saturating_add(u64::try_from(removed).unwrap_or(u64::MAX));
            self.tail_chars = self.tail_chars.saturating_sub(1);
        }
        self.tail.extend(bytes.iter().copied());
        self.tail_chars = self.tail_chars.saturating_add(1);
    }

    pub(crate) fn snapshot(&self) -> ProcessOutput {
        let bytes = render_bytes(&self.head, &self.tail);
        ProcessOutput {
            bytes,
            cursor: OutputCursor(self.cursor),
            truncated: self.omitted_bytes > 0 || self.incomplete,
            omitted_bytes: self.omitted_bytes,
            page_contiguous: self.omitted_bytes == 0,
            has_more_retained: false,
        }
    }

    /// 标记无法继续 drain 的输出流。没有可靠的内核字节数时不能伪造 omitted_bytes，
    /// 但必须使外层协议把结果视为 truncated，禁止将 cursor 当成完整交付。
    pub(crate) fn mark_incomplete(&mut self) {
        self.incomplete = true;
    }

    /// 返回从调用方 cursor 之后可可靠提供的增量。
    ///
    /// head/tail 策略不会保留被截掉的中段；cursor 落在该区间时退回当前快照并明确
    /// 标记截断，避免悄悄伪造连续输出。
    pub(crate) fn snapshot_since(&self, cursor: OutputCursor) -> ProcessOutput {
        let current = self.snapshot();
        if cursor.0 > self.cursor {
            // 调用方不能用伪造的 future cursor 把尚未见过的输出标成已消费。
            return ProcessOutput {
                bytes: Vec::new(),
                cursor: current.cursor,
                truncated: true,
                omitted_bytes: current.omitted_bytes,
                page_contiguous: false,
                has_more_retained: false,
            };
        }
        if cursor.0 == self.cursor {
            return ProcessOutput {
                bytes: Vec::new(),
                cursor: current.cursor,
                truncated: current.truncated,
                omitted_bytes: current.omitted_bytes,
                page_contiguous: true,
                has_more_retained: false,
            };
        }

        if self.omitted_bytes == 0 {
            let skip = usize::try_from(cursor.0).unwrap_or(usize::MAX);
            return ProcessOutput {
                bytes: match std::str::from_utf8(&current.bytes) {
                    // head/tail 始终在 scalar 边界拼接；保留此分支避免不变量被未来调用者
                    // 破坏时把 process result 变成 panic。
                    Ok(text) => text.chars().skip(skip).collect::<String>().into_bytes(),
                    Err(_) => Vec::new(),
                },
                cursor: current.cursor,
                // 即使没有可计数的 head/tail 淘汰，drain worker 也可能在后代持有 fd
                // 时被有界收束；这时后续字节未知，不能把增量伪装成可安全提交的最终输出。
                truncated: current.truncated,
                omitted_bytes: current.omitted_bytes,
                page_contiguous: true,
                has_more_retained: false,
            };
        }

        let head_len = u64::try_from(self.head_chars).unwrap_or(u64::MAX);
        if cursor.0 < head_len {
            let skip = usize::try_from(cursor.0).unwrap_or(usize::MAX);
            return ProcessOutput {
                bytes: match std::str::from_utf8(&self.head) {
                    Ok(text) => text.chars().skip(skip).collect::<String>().into_bytes(),
                    Err(_) => Vec::new(),
                },
                cursor: OutputCursor(head_len),
                truncated: true,
                omitted_bytes: current.omitted_bytes,
                page_contiguous: true,
                has_more_retained: true,
            };
        }

        let tail_start = self
            .cursor
            .saturating_sub(u64::try_from(self.tail_chars).unwrap_or(0));
        if cursor.0 < tail_start {
            // head 与 tail 之间的字节已经不可恢复。用一个空页面把 cursor 推到
            // tail 起点，tool result 仍携带 truncated/omitted_bytes，provider 成功
            // 确认该事实后才能继续交付 tail。
            return ProcessOutput {
                bytes: Vec::new(),
                cursor: OutputCursor(tail_start),
                truncated: true,
                omitted_bytes: current.omitted_bytes,
                page_contiguous: true,
                has_more_retained: !self.tail.is_empty(),
            };
        }

        let start = usize::try_from(cursor.0.saturating_sub(tail_start)).unwrap_or(usize::MAX);
        ProcessOutput {
            bytes: tail_from_char_offset(&self.tail, start),
            cursor: current.cursor,
            truncated: current.truncated,
            omitted_bytes: current.omitted_bytes,
            page_contiguous: true,
            has_more_retained: false,
        }
    }
}

fn render_bytes(head: &[u8], tail: &VecDeque<u8>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(head.len().saturating_add(tail.len()));
    bytes.extend_from_slice(head);
    bytes.extend(tail.iter().copied());
    bytes
}

fn pop_front_utf8_scalar(tail: &mut VecDeque<u8>) -> usize {
    let Some(first) = tail.front().copied() else {
        return 0;
    };
    let scalar_bytes = match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        // append_char 只写入 Rust `char` 的 UTF-8；若以后有破坏该不变量的调用，
        // 保守移除一个字节以保证 buffer 不会卡死。
        _ => 1,
    };
    for _ in 0..scalar_bytes {
        let _ = tail.pop_front();
    }
    scalar_bytes
}

fn tail_from_char_offset(tail: &VecDeque<u8>, offset: usize) -> Vec<u8> {
    let mut byte_offset = 0usize;
    for _ in 0..offset {
        let Some(first) = tail.get(byte_offset).copied() else {
            return Vec::new();
        };
        byte_offset = byte_offset.saturating_add(match first {
            0x00..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 1,
        });
    }
    tail.iter().skip(byte_offset).copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_head_and_latest_tail_when_output_exceeds_limit() {
        let mut output = BoundedOutput::new(6);
        output.append(b"abcdefghi");
        let snapshot = output.snapshot();

        assert_eq!(snapshot.bytes, b"abcghi");
        assert_eq!(snapshot.cursor, OutputCursor(9));
        assert!(snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 3);
    }

    #[test]
    fn snapshot_since_pages_retained_head_gap_and_tail_separately() {
        let mut output = BoundedOutput::new(6);
        output.append(b"abcdefghi");

        let head = output.snapshot_since(OutputCursor(0));
        assert_eq!(head.bytes, b"abc");
        assert_eq!(head.cursor, OutputCursor(3));
        assert!(head.page_contiguous);
        assert!(head.has_more_retained);

        let gap = output.snapshot_since(head.cursor);
        assert!(gap.bytes.is_empty());
        assert_eq!(gap.cursor, OutputCursor(6));
        assert!(gap.truncated);
        assert_eq!(gap.omitted_bytes, 3);
        assert!(gap.page_contiguous);
        assert!(gap.has_more_retained);

        let tail = output.snapshot_since(gap.cursor);
        assert_eq!(tail.bytes, b"ghi");
        assert_eq!(tail.cursor, OutputCursor(9));
        assert!(tail.truncated);
        assert!(tail.page_contiguous);
        assert!(!tail.has_more_retained);
    }

    #[test]
    fn exact_limit_is_not_truncated() {
        let mut output = BoundedOutput::new(4);
        output.append(b"abcd");
        let snapshot = output.snapshot();

        assert_eq!(snapshot.bytes, b"abcd");
        assert!(!snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 0);
    }

    #[test]
    fn cursor_at_current_returns_no_duplicate_bytes() {
        let mut output = BoundedOutput::new(32);
        output.append(b"hello");
        let cursor = output.snapshot().cursor;
        output.append(b" world");

        let result = output.snapshot_since(cursor);
        assert_eq!(result.bytes, b" world");
        assert_eq!(result.cursor, OutputCursor(11));
    }

    #[test]
    fn future_cursor_is_rejected_without_advancing_the_snapshot() {
        let mut output = BoundedOutput::new(32);
        output.append(b"hello");

        let result = output.snapshot_since(OutputCursor(99));
        assert!(result.bytes.is_empty());
        assert_eq!(result.cursor, OutputCursor(5));
        assert!(result.truncated);
    }

    #[test]
    fn zero_capacity_still_advances_cursor_without_retaining_bytes() {
        let mut output = BoundedOutput::new(0);
        let bytes = b"stderr is multiplexed into PTY stdout";
        output.append(bytes);

        let snapshot = output.snapshot();
        assert!(snapshot.bytes.is_empty());
        assert_eq!(snapshot.cursor, OutputCursor(37));
        assert!(snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 37);
    }

    #[test]
    fn incomplete_drain_marks_snapshot_truncated_without_fabricating_omitted_bytes() {
        let mut output = BoundedOutput::new(32);
        output.append(b"already drained");
        output.mark_incomplete();

        let snapshot = output.snapshot();
        assert!(snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 0);
        assert_eq!(snapshot.bytes, b"already drained");
    }

    #[test]
    fn incomplete_drain_keeps_incremental_snapshot_truncated() {
        let mut output = BoundedOutput::new(32);
        output.append(b"before");
        let cursor = output.snapshot().cursor;
        output.append(b" after");
        output.mark_incomplete();

        let snapshot = output.snapshot_since(cursor);
        assert_eq!(snapshot.bytes, b" after");
        assert!(snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 0);
    }

    #[test]
    fn head_tail_capacity_preserves_complete_utf8_scalars() {
        let mut output = BoundedOutput::new(9);
        output.append("你好世界".as_bytes());

        let snapshot = output.snapshot();
        assert_eq!(
            String::from_utf8(snapshot.bytes).ok().as_deref(),
            Some("你世界")
        );
        assert_eq!(snapshot.cursor, OutputCursor(4));
        assert!(snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 3);
    }

    #[test]
    fn utf8_retention_budget_is_measured_in_bytes_not_scalars() {
        let mut output = BoundedOutput::new(8);
        output.append("😀😃😄😁".as_bytes());

        let snapshot = output.snapshot();
        assert_eq!(
            String::from_utf8(snapshot.bytes.clone()).ok().as_deref(),
            Some("😀😁")
        );
        assert!(snapshot.bytes.len() <= 8);
        assert_eq!(snapshot.cursor, OutputCursor(4));
        assert!(snapshot.truncated);
        assert_eq!(snapshot.omitted_bytes, 8);
    }

    #[test]
    fn split_utf8_reader_chunks_are_joined_before_retention() {
        let mut output = BoundedOutput::new(8);
        let character = "你".as_bytes();
        output.append(&character[..2]);
        assert!(output.snapshot().bytes.is_empty());

        output.append(&character[2..]);
        let snapshot = output.snapshot();
        assert_eq!(
            String::from_utf8(snapshot.bytes).ok().as_deref(),
            Some("你")
        );
        assert_eq!(snapshot.cursor, OutputCursor(1));
    }

    #[test]
    fn eof_flushes_incomplete_utf8_as_one_lossy_scalar() {
        let mut output = BoundedOutput::new(8);
        output.append(&"你".as_bytes()[..2]);
        output.finish();

        let snapshot = output.snapshot();
        assert_eq!(String::from_utf8(snapshot.bytes).ok().as_deref(), Some("�"));
        assert_eq!(snapshot.cursor, OutputCursor(1));
    }
}
