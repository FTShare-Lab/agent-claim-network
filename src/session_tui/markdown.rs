//! TUI transcript 的轻量 Markdown 渲染。
//!
//! 本模块负责把 assistant 文本转换为 `ratatui::Line`，供上层 cell 直接展示。
//! 渲染器支持增量追加，并保留 source-backed markdown 便于 resize 后重渲染。

use super::theme::CODE_CONTENT_FG;
use super::wrapping::hard_wrap_styled_lines;
use pulldown_cmark::{CodeBlockKind, Event as MarkdownEvent, Options, Parser, Tag, TagEnd};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

#[cfg(test)]
pub(super) fn render_markdown_lines(markdown: &str) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    append_markdown_agent(markdown, None, &mut lines);
    lines
}

pub(super) fn append_markdown_agent(
    markdown: &str,
    width: Option<usize>,
    lines: &mut Vec<Line<'static>>,
) {
    let mut renderer = MarkdownLineRenderer::new(width);
    let mut options = Options::empty();
    options.insert(Options::ENABLE_GFM);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_TASKLISTS);
    let normalized = normalize_quoted_strong_markers(&unwrap_markdown_fences(markdown));
    for event in Parser::new_ext(&normalized, options) {
        renderer.apply(event);
    }
    let rendered = renderer.finish();
    if let Some(width) = width {
        lines.extend(hard_wrap_styled_lines(rendered, width));
    } else {
        lines.extend(rendered);
    }
}

fn unwrap_markdown_fences(markdown: &str) -> String {
    let mut output = String::with_capacity(markdown.len());
    let mut lines = markdown.lines();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        let Some(info) = trimmed
            .strip_prefix("```")
            .or_else(|| trimmed.strip_prefix("~~~"))
        else {
            output.push_str(line);
            output.push('\n');
            continue;
        };
        let info = info.trim();
        if info != "md" && info != "markdown" {
            output.push_str(line);
            output.push('\n');
            continue;
        }

        let mut body = Vec::new();
        let mut closing_line: Option<&str> = None;
        for body_line in lines.by_ref() {
            let body_trimmed = body_line.trim_start();
            if body_trimmed.starts_with("```") || body_trimmed.starts_with("~~~") {
                closing_line = Some(body_line);
                break;
            }
            body.push(body_line);
        }

        if closing_line.is_some() && markdown_lines_contain_table(&body) {
            for body_line in body {
                output.push_str(body_line);
                output.push('\n');
            }
        } else {
            output.push_str(line);
            output.push('\n');
            for body_line in body {
                output.push_str(body_line);
                output.push('\n');
            }
            if let Some(closing_line) = closing_line {
                output.push_str(closing_line);
                output.push('\n');
            }
        }
    }
    output
}

fn markdown_lines_contain_table(lines: &[&str]) -> bool {
    lines.windows(2).any(|pair| {
        let header = pair[0].trim();
        let delimiter = pair[1].trim();
        header.contains('|')
            && delimiter.contains('|')
            && delimiter
                .chars()
                .all(|ch| matches!(ch, '|' | '-' | ':' | ' '))
            && delimiter.contains('-')
    })
}

#[derive(Debug, Clone, Copy)]
struct QuotedStrongFallback {
    opening_quote: char,
    inner_start: usize,
    inner_end: usize,
    closing_quote: char,
    close_end: usize,
}

fn normalize_quoted_strong_markers(markdown: &str) -> String {
    let mut output = String::with_capacity(markdown.len());
    let mut in_fence = false;
    for line in markdown.split_inclusive('\n') {
        let (body, newline) = line
            .strip_suffix('\n')
            .map_or((line, ""), |body| (body, "\n"));
        let trimmed = body.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            output.push_str(body);
            output.push_str(newline);
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            output.push_str(body);
        } else {
            output.push_str(&normalize_quoted_strong_line(body));
        }
        output.push_str(newline);
    }
    output
}

fn normalize_quoted_strong_line(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut cursor = 0usize;
    while cursor < line.len() {
        let Some(rest) = line.get(cursor..) else {
            break;
        };
        if rest.starts_with('`') {
            let (code_span, next_cursor) = take_inline_code_span(line, cursor);
            output.push_str(code_span);
            cursor = next_cursor;
            continue;
        }
        if rest.starts_with("**") {
            if let Some(match_range) = find_quoted_strong_fallback(line, cursor) {
                output.push(match_range.opening_quote);
                output.push_str("**");
                output.push_str(&line[match_range.inner_start..match_range.inner_end]);
                output.push_str("**");
                output.push(match_range.closing_quote);
                cursor = match_range.close_end;
                continue;
            }
        }

        let Some(ch) = rest.chars().next() else {
            break;
        };
        output.push(ch);
        cursor = cursor.saturating_add(ch.len_utf8());
    }
    output
}

fn take_inline_code_span(line: &str, start: usize) -> (&str, usize) {
    let Some(rest) = line.get(start..) else {
        return ("", start);
    };
    let tick_count = rest.chars().take_while(|ch| *ch == '`').count();
    if tick_count == 0 {
        return ("", start);
    }
    let marker = "`".repeat(tick_count);
    let content_start = start.saturating_add(tick_count);
    let Some(after_open) = line.get(content_start..) else {
        return (&line[start..], line.len());
    };
    let Some(relative_close) = after_open.find(&marker) else {
        return (&line[start..], line.len());
    };
    let close_end = content_start
        .saturating_add(relative_close)
        .saturating_add(tick_count);
    (&line[start..close_end], close_end)
}

fn find_quoted_strong_fallback(text: &str, marker_start: usize) -> Option<QuotedStrongFallback> {
    let after_open = marker_start.checked_add(2)?;
    let opening_quote = text.get(after_open..)?.chars().next()?;
    let closing_quote = matching_closing_quote(opening_quote)?;
    let inner_start = after_open.saturating_add(opening_quote.len_utf8());
    let mut search = inner_start;

    while let Some(relative_close) = text.get(search..)?.find("**") {
        let close_start = search.saturating_add(relative_close);
        let (closing_quote_start, found_closing_quote) =
            text.get(..close_start)?.char_indices().next_back()?;
        if found_closing_quote == closing_quote {
            return Some(QuotedStrongFallback {
                opening_quote,
                inner_start,
                inner_end: closing_quote_start,
                closing_quote,
                close_end: close_start.saturating_add(2),
            });
        }
        search = close_start.saturating_add(2);
    }

    None
}

fn matching_closing_quote(opening_quote: char) -> Option<char> {
    match opening_quote {
        '"' => Some('"'),
        '\'' => Some('\''),
        '“' => Some('”'),
        '‘' => Some('’'),
        '「' => Some('」'),
        '『' => Some('』'),
        '《' => Some('》'),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct ListState {
    next_ordered: Option<u64>,
}

#[derive(Debug, Default)]
struct TableRenderState {
    rows: Vec<Vec<String>>,
    current_row: Vec<String>,
    current_cell: String,
    in_cell: bool,
}

#[derive(Debug)]
struct CodeBlockRenderState {
    label: Option<String>,
    lines: Vec<String>,
    current_line: String,
}

#[derive(Debug)]
struct MarkdownLineRenderer {
    lines: Vec<Line<'static>>,
    spans: Vec<Span<'static>>,
    width: Option<usize>,
    style_stack: Vec<Style>,
    list_stack: Vec<ListState>,
    quote_depth: usize,
    pending_list_marker: Option<String>,
    in_heading: bool,
    in_code_block: bool,
    code_block: Option<CodeBlockRenderState>,
    table: Option<TableRenderState>,
}

impl MarkdownLineRenderer {
    fn new(width: Option<usize>) -> Self {
        Self {
            lines: Vec::new(),
            spans: Vec::new(),
            width,
            style_stack: Vec::new(),
            list_stack: Vec::new(),
            quote_depth: 0,
            pending_list_marker: None,
            in_heading: false,
            in_code_block: false,
            code_block: None,
            table: None,
        }
    }

    fn apply(&mut self, event: MarkdownEvent<'_>) {
        match event {
            MarkdownEvent::Start(tag) => self.start_tag(tag),
            MarkdownEvent::End(tag) => self.end_tag(tag),
            MarkdownEvent::Text(text) => self.push_text(&text),
            MarkdownEvent::Code(text) => self.push_span(
                text.into_string(),
                self.current_style().add_modifier(Modifier::BOLD),
            ),
            MarkdownEvent::SoftBreak | MarkdownEvent::HardBreak => {
                if self.in_code_block {
                    self.push_code_newline();
                } else if self.in_table_cell() {
                    self.push_table_text(" ");
                } else {
                    self.flush_line();
                }
            }
            MarkdownEvent::Rule => {
                self.flush_line();
                self.lines.push(Line::styled(
                    "─".repeat(24),
                    Style::default().fg(Color::DarkGray),
                ));
            }
            _ => {}
        }
    }

    fn start_tag(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => {}
            Tag::Heading { .. } => {
                self.in_heading = true;
                self.style_stack.push(
                    self.current_style()
                        .fg(Color::Blue)
                        .add_modifier(Modifier::BOLD),
                );
            }
            Tag::Emphasis => self
                .style_stack
                .push(self.current_style().add_modifier(Modifier::ITALIC)),
            Tag::Strong => self
                .style_stack
                .push(self.current_style().add_modifier(Modifier::BOLD)),
            Tag::CodeBlock(kind) => {
                self.flush_line();
                self.in_code_block = true;
                let label = match kind {
                    CodeBlockKind::Fenced(label) if !label.trim().is_empty() => {
                        Some(label.trim().to_string())
                    }
                    _ => None,
                };
                self.code_block = Some(CodeBlockRenderState {
                    label,
                    lines: Vec::new(),
                    current_line: String::new(),
                });
            }
            Tag::BlockQuote(_) => {
                self.flush_line();
                self.quote_depth = self.quote_depth.saturating_add(1);
            }
            Tag::List(start) => self.list_stack.push(ListState {
                next_ordered: start,
            }),
            Tag::Item => {
                self.flush_line();
                let indent = "  ".repeat(self.list_stack.len().saturating_sub(1));
                let marker = match self.list_stack.last_mut().and_then(|state| {
                    state.next_ordered.as_mut().map(|next| {
                        let current = *next;
                        *next = next.saturating_add(1);
                        current
                    })
                }) {
                    Some(number) => format!("{indent}{number}. "),
                    None => format!("{indent}• "),
                };
                self.pending_list_marker = Some(marker);
            }
            Tag::Table(_) => {
                self.flush_line();
                self.table = Some(TableRenderState::default());
            }
            Tag::TableHead | Tag::TableRow => {
                if let Some(table) = self.table.as_mut() {
                    table.current_row.clear();
                }
            }
            Tag::TableCell => {
                if let Some(table) = self.table.as_mut() {
                    table.current_cell.clear();
                    table.in_cell = true;
                }
            }
            _ => {}
        }
    }

    fn end_tag(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph => self.flush_line(),
            TagEnd::Heading(_) => {
                self.flush_line();
                self.in_heading = false;
                self.style_stack.pop();
            }
            TagEnd::Emphasis | TagEnd::Strong => {
                self.style_stack.pop();
            }
            TagEnd::CodeBlock => {
                self.render_current_code_block();
                self.in_code_block = false;
            }
            TagEnd::BlockQuote(_) => {
                self.flush_line();
                self.quote_depth = self.quote_depth.saturating_sub(1);
            }
            TagEnd::List(_) => {
                self.flush_line();
                self.list_stack.pop();
            }
            TagEnd::Item => self.flush_line(),
            TagEnd::TableCell => {
                if let Some(table) = self.table.as_mut() {
                    table
                        .current_row
                        .push(table.current_cell.trim().to_string());
                    table.current_cell.clear();
                    table.in_cell = false;
                }
            }
            TagEnd::TableHead | TagEnd::TableRow => {
                if let Some(table) = self.table.as_mut() {
                    if !table.current_row.is_empty() {
                        table.rows.push(std::mem::take(&mut table.current_row));
                    }
                }
            }
            TagEnd::Table => {
                self.flush_line();
                if let Some(table) = self.table.take() {
                    self.render_table(table);
                }
            }
            _ => {}
        }
    }

    fn push_text(&mut self, text: &str) {
        if self.in_code_block {
            self.push_code_text(text);
            return;
        }

        for (idx, segment) in text.split('\n').enumerate() {
            if idx > 0 {
                self.flush_line();
            }
            if !segment.is_empty() {
                if self.in_table_cell() {
                    self.push_table_text(segment);
                    continue;
                }
                self.push_span(segment.to_string(), self.current_style());
            }
        }
    }

    fn push_span(&mut self, text: String, style: Style) {
        if self.in_table_cell() {
            self.push_table_text(&text);
            return;
        }
        if let Some(marker) = self.pending_list_marker.take() {
            self.spans
                .push(Span::styled(marker, Style::default().fg(Color::DarkGray)));
        }
        if self.quote_depth > 0 && self.spans.is_empty() {
            let prefix = "> ".repeat(self.quote_depth);
            self.spans
                .push(Span::styled(prefix, Style::default().fg(Color::DarkGray)));
        }
        self.spans.push(Span::styled(text, style));
    }

    fn flush_line(&mut self) {
        if self.spans.is_empty() {
            return;
        }
        self.lines.push(Line::from(std::mem::take(&mut self.spans)));
    }

    fn finish(mut self) -> Vec<Line<'static>> {
        if self.in_code_block {
            self.render_current_code_block();
            self.in_code_block = false;
        }
        self.flush_line();
        if self.lines.is_empty() {
            vec![Line::default()]
        } else {
            self.lines
        }
    }

    fn current_style(&self) -> Style {
        self.style_stack
            .last()
            .copied()
            .unwrap_or_else(Style::default)
    }

    fn in_table_cell(&self) -> bool {
        self.table
            .as_ref()
            .map(|table| table.in_cell)
            .unwrap_or(false)
    }

    fn push_table_text(&mut self, text: &str) {
        if let Some(table) = self.table.as_mut() {
            table.current_cell.push_str(text);
        }
    }

    fn push_code_text(&mut self, text: &str) {
        let Some(block) = self.code_block.as_mut() else {
            return;
        };
        for ch in text.chars() {
            if ch == '\n' {
                block.lines.push(std::mem::take(&mut block.current_line));
            } else {
                block.current_line.push(ch);
            }
        }
    }

    fn push_code_newline(&mut self) {
        if let Some(block) = self.code_block.as_mut() {
            block.lines.push(std::mem::take(&mut block.current_line));
        }
    }

    fn render_current_code_block(&mut self) {
        let Some(mut block) = self.code_block.take() else {
            return;
        };
        if !block.current_line.is_empty() || block.lines.is_empty() {
            block.lines.push(std::mem::take(&mut block.current_line));
        }
        self.lines.extend(render_code_block_lines(
            block.label.as_deref(),
            &block.lines,
            self.width,
        ));
    }

    fn render_table(&mut self, table: TableRenderState) {
        if table.rows.is_empty() {
            return;
        }
        let column_count = table.rows.iter().map(Vec::len).max().unwrap_or(0);
        if column_count == 0 {
            return;
        }

        // 1. 计算每列最大宽度
        let mut widths = vec![0usize; column_count];
        for row in &table.rows {
            for (index, cell) in row.iter().enumerate() {
                widths[index] = widths[index].max(UnicodeWidthStr::width(cell.as_str()));
            }
        }

        // 2. 逐行渲染
        for (row_index, row) in table.rows.iter().enumerate() {
            let mut rendered = String::new();

            // 左边框
            rendered.push_str("| ");

            for (column, width) in widths.iter().enumerate() {
                let cell = row.get(column).map(String::as_str).unwrap_or("");
                rendered.push_str(cell);

                // 用空格补齐剩余宽度
                let pad = width.saturating_sub(UnicodeWidthStr::width(cell));
                rendered.push_str(&" ".repeat(pad));

                // 列分割线和右边框
                rendered.push_str(" | ");
            }

            // 渲染当前数据行
            let style = if row_index == 0 {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            // trim_end 去掉最右边多余的空格，保持整洁
            self.lines
                .push(Line::styled(rendered.trim_end().to_string(), style));

            // 3. 如果是第一行（表头），在它下面画一条横向分割线
            if row_index == 0 {
                let mut separator = String::new();
                separator.push('|');
                for width in &widths {
                    // width + 2 是为了覆盖文字两边的空格
                    separator.push_str(&"-".repeat(*width + 2));
                    separator.push('|');
                }
                // 分割线通常用暗灰色，避免视觉干扰
                self.lines.push(Line::styled(
                    separator,
                    Style::default().fg(Color::DarkGray),
                ));
            }
        }
    }
}

pub(super) fn render_code_block_lines(
    label: Option<&str>,
    code_lines: &[String],
    width: Option<usize>,
) -> Vec<Line<'static>> {
    let natural_width = code_block_natural_width(label, code_lines);
    let frame_width = width.unwrap_or(natural_width).max(1);
    if frame_width < 12 {
        return render_minimal_code_block(label, code_lines, frame_width);
    }

    let inner_width = frame_width.saturating_sub(4).max(1);
    let mut lines = vec![Line::styled(
        code_block_top_border(label, frame_width),
        code_border_style(),
    )];
    for line in code_lines {
        let wrapped = hard_wrap_styled_lines(vec![Line::raw(line.clone())], inner_width);
        for visual_line in wrapped {
            lines.push(code_content_line(&visual_line.to_string(), inner_width));
        }
    }
    lines.push(Line::styled(
        format!("╰{}╯", "─".repeat(frame_width.saturating_sub(2))),
        code_border_style(),
    ));
    lines
}

fn render_minimal_code_block(
    label: Option<&str>,
    code_lines: &[String],
    width: usize,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::styled(
        fit_text_to_width(&format!("{}:", label.unwrap_or("code")), width),
        code_border_style(),
    )];
    let content_width = width.max(1);
    for line in code_lines {
        let wrapped = hard_wrap_styled_lines(vec![Line::raw(line.clone())], content_width);
        for visual_line in wrapped {
            lines.push(Line::styled(visual_line.to_string(), code_content_style()));
        }
    }
    lines
}

fn code_block_natural_width(label: Option<&str>, code_lines: &[String]) -> usize {
    let label_width = label.map(UnicodeWidthStr::width).unwrap_or(4);
    let code_width = code_lines
        .iter()
        .map(|line| UnicodeWidthStr::width(line.as_str()))
        .max()
        .unwrap_or(0);
    label_width.max(code_width).saturating_add(4).max(12)
}

fn code_block_top_border(label: Option<&str>, width: usize) -> String {
    let raw_title = label.unwrap_or("code");
    let title = format!(" {raw_title} ");
    let prefix = "╭─";
    let suffix = "╮";
    let available = width
        .saturating_sub(UnicodeWidthStr::width(prefix))
        .saturating_sub(UnicodeWidthStr::width(suffix));
    let fitted_title = fit_text_to_width(&title, available);
    let fill = available.saturating_sub(UnicodeWidthStr::width(fitted_title.as_str()));
    format!("{prefix}{fitted_title}{}{suffix}", "─".repeat(fill))
}

fn code_content_line(content: &str, inner_width: usize) -> Line<'static> {
    let content_width = UnicodeWidthStr::width(content);
    let padding = " ".repeat(inner_width.saturating_sub(content_width));
    Line::from(vec![
        Span::styled("│ ", code_border_style()),
        Span::styled(content.to_string(), code_content_style()),
        Span::styled(padding, code_content_style()),
        Span::styled(" │", code_border_style()),
    ])
}

fn code_border_style() -> Style {
    Style::default().fg(Color::DarkGray)
}

fn code_content_style() -> Style {
    // code block text color
    Style::default().fg(CODE_CONTENT_FG)
}

fn fit_text_to_width(text: &str, width: usize) -> String {
    if UnicodeWidthStr::width(text) <= width {
        return text.to_string();
    }
    if width == 0 {
        return String::new();
    }

    let ellipsis = "…";
    let ellipsis_width = UnicodeWidthStr::width(ellipsis);
    let target_width = width.saturating_sub(ellipsis_width);
    let mut out = String::new();
    let mut used = 0usize;
    for ch in text.chars() {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if used.saturating_add(ch_width) > target_width {
            break;
        }
        out.push(ch);
        used = used.saturating_add(ch_width);
    }
    if ellipsis_width <= width {
        out.push_str(ellipsis);
    }
    out
}

#[cfg(test)]
mod tests {
    use ratatui::style::{Color, Modifier};

    #[test]
    fn heading_renders_as_title_without_literal_hash_marker() {
        let lines = super::render_markdown_lines("# 音乐/演出类");

        assert_eq!(lines[0].to_string(), "音乐/演出类");
    }

    #[test]
    fn structural_markdown_markers_avoid_green_and_user_input_colors() {
        let lines = super::render_markdown_lines("- 项目\n\n> 引用");
        let forbidden = [Some(Color::Green), Some(Color::LightBlue)];

        for line in lines {
            for span in line.spans {
                assert!(
                    !forbidden.contains(&span.style.fg),
                    "Unexpected color {:?} in span {:?}",
                    span.style.fg,
                    span.content
                );
            }
        }
    }

    #[test]
    fn markdown_table_inside_markdown_fence_renders_as_table_content() {
        let lines =
            super::render_markdown_lines("```markdown\n| A | B |\n| - | - |\n| x | y |\n```");
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("A"));
        assert!(rendered.contains("B"));
        assert!(!rendered.contains("```markdown"));
    }

    #[test]
    fn inline_code_inside_table_does_not_render_literal_backticks() {
        let lines = super::render_markdown_lines("| symbol |\n| - |\n| `derive_id_string` |");
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("derive_id_string"));
        assert!(!rendered.contains("`derive_id_string`"));
    }

    #[test]
    fn quoted_strong_followed_by_cjk_renders_without_literal_markers() {
        let lines = super::render_markdown_lines(r#"**"耳机是一个很私密的东西"**登上热搜"#);
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert_eq!(rendered, r#""耳机是一个很私密的东西"登上热搜"#);
        assert!(!rendered.contains("**"));
        assert!(lines[0].spans.iter().any(|span| {
            span.content.as_ref() == "耳机是一个很私密的东西"
                && span.style.add_modifier.contains(Modifier::BOLD)
        }));
    }

    #[test]
    fn quoted_strong_inside_code_block_keeps_literal_markers() {
        let lines = super::render_markdown_lines("```text\n**\"x\"**y\n```");
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains(r#"**"x"**y"#));
    }

    #[test]
    fn quoted_strong_inside_inline_code_keeps_literal_markers() {
        let lines = super::render_markdown_lines("`**\"x\"**y`");
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert_eq!(rendered, r#"**"x"**y"#);
    }

    #[test]
    fn fenced_code_block_renders_as_width_bounded_box() {
        let mut lines = Vec::new();
        super::append_markdown_agent(
            "```rust\nfn very_long_function_name_that_wraps() {\n    println!(\"hi\");\n}\n```",
            Some(28),
            &mut lines,
        );
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("╭─ rust"));
        assert!(rendered.contains("very_long_function"));
        assert!(rendered.contains("╰"));
        assert!(!rendered.contains("```rust"));
        assert!(lines.iter().all(|line| line.width() <= 28));
    }

    #[test]
    fn narrow_fenced_code_block_falls_back_to_plain_lines() {
        let mut lines = Vec::new();
        super::append_markdown_agent("```rs\nab\ncd\n```", Some(4), &mut lines);
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("rs:"));
        assert!(rendered.contains("ab"));
        assert!(rendered.contains("cd"));
        assert!(!rendered.contains("```rs"));
        assert!(!rendered.contains("╭"));
        assert!(!rendered.contains("│"));
        assert!(!rendered.contains("╰"));
        assert!(lines.iter().all(|line| line.width() <= 4));
    }

    #[test]
    fn unterminated_fenced_code_block_still_renders_as_box() {
        let mut lines = Vec::new();
        super::append_markdown_agent("```text\nstill streaming", Some(32), &mut lines);
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("╭─ text"));
        assert!(rendered.contains("still streaming"));
        assert!(rendered.contains("╰"));
        assert!(!rendered.contains("```text"));
        assert!(lines.iter().all(|line| line.width() <= 32));
    }
}
