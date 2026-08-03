# TUI File Diff 展示与 file 类工具优化需求设计

> 状态：已实现。本文保留 diff 事件与 TUI 展示决策；当前分页读取和分级写入许可统一以 `PRD_file_read_write_capability.md` 为准。

## 背景

本需求解决 file 类工具（`file_write` / `file_patch`）修改文件后缺少可见 diff，以及既有文件写入缺少一致预读边界的问题。实现后，成功修改会在历史区展示红绿 diff，并由 read state 防止基于过期内容覆盖文件。

设计依据：

- `file_patch` 默认要求 `old_content` 全局唯一；`replace_all` 是显式选择，不能把多匹配静默解释为全量替换。
- `file_write` 对既有文件的 overwrite、append 与 prepend 都必须先完整读取；读取失败或 read state 过期时拒绝写入。
- 事后不能仅凭短 tool preview 重建 diff，因此 `FileChange` 必须在工具执行现场采集并随事件持久化。

## 目标

- `file_write` / `file_patch` 执行成功且文件内容实际发生变化的调用，在 TUI history 区对应 ToolCell 下追加红绿 diff 块。
- streaming 虚线框内不展示 diff；turn 落定进 scrollback 后才展示。
- 超长修改截断：只展示前 N 行修改（按 changed lines 计数，可配），尾部提示剩余修改行数。
- resume 会话后，历史消息里的 diff 仍然可见。
- `file_patch` 精确性优化：多匹配使用有界歧义错误；`replace_all` 为显式可选参数。
- 既有文件写入前必须有完整 `file_read` 记录，并检查 stale，避免模型基于旧内容覆盖用户或 formatter 的改动。
- 本期不新增 `file_delete`；删除继续通过 `code_run` / shell 完成，删除造成的文件变化不展示 file diff。

## File 工具语义优化

### file_read 状态

- `file_read` 成功完整读取 UTF-8 文本文件后，记录该文件的 read state：规范化后的绝对路径、读取内容、mtime / metadata 时间戳、是否完整读取。
- 如果 `file_read` 是窗口读取、命中 `max_chars` 截断，或读取的是图片 / PDF / 非文本附件，不视为完整 read state。
- read state 用于后续 `file_write` / `file_patch` 的写前校验；写入成功后更新为新内容和新 mtime。
- read state 只作为当前运行期的写入安全状态，不随 journal 持久化恢复；resume 后如需继续修改已有文件，必须重新 `file_read`。

### 写前校验

- `file_patch`：目标文件必须存在，且必须有完整 read state；如果文件在 read 之后发生变化，拒绝写入并提示重新 `file_read`。
- `file_write overwrite`：
  - 文件不存在：允许创建，不要求 read state。
  - 文件存在：必须有完整 read state；stale 时拒绝写入。
- `file_write append` / `prepend`：
  - 文件不存在：允许按空文件创建。
  - 文件存在：必须有完整 read state；stale 时拒绝写入。
- stale 判定优先用 mtime / metadata；如时间戳变化但完整 read state 内容与当前内容一致，可视为未 stale，减少 formatter / 平台时间戳误报。
- `old_content == new_content` 时 `file_patch` 直接失败，不做 no-op 写入。

### file_patch 精确性

- 保留当前默认行为：`replace_all = false` 时，`old_content` 必须全局唯一匹配。
- 默认按大小写敏感、逐字节精确、非重叠语义扫描；只保存第一处，发现第二处立即停止并返回固定长度错误，提示扩大文本块并加入目标附近上下文。不得静默选择第一处，也不输出随匹配数量增长的位置列表。
- `replace_all: bool`（默认 false）：true 时替换全部非重叠精确匹配，result 中返回替换处数；统计使用常量额外内存，不保存位置列表，diff 采集覆盖全部替换位置。
- 0 匹配行为保持失败，但错误信息应提示先 `file_read` 确认当前内容。
- 工具 description 与实际 system prompt/tool 使用说明同步补充：read-before-write、唯一匹配约束、扩大上下文的消歧方式与 `replace_all` 用法。

### file_write 行为

- overwrite / append / prepend 都统一计算 before / after；除文件不存在可视为 before 为空外，读旧内容失败必须失败，不再静默当空字符串。
- before == after 时不生成 `FileChange`；tool result 可返回成功或 no change 状态，但 TUI 不展示 diff。
- 新建文件 kind = created；覆盖、追加、前插已有文件 kind = modified。

## Diff 采集

- 引入 diff crate 计算行级 diff。若选择存 unified diff，可优先评估 `diffy`；若直接生成结构化 hunks，可评估 `similar`。
- `file_patch`：在替换前保留 before，替换后对 before / after 求 diff。
- `file_write`：按上文规则在写前取得 before，写入后用 before / after 求 diff。
- 产出结构化 `FileChange`，不要只存预渲染字符串：

```
FileChange {
  path,
  kind: created | modified,
  added_lines, removed_lines,        # 全量统计
  hunks,                             # 已按 file_diff_max_changed_lines 截断的结构化行级 hunk
  truncated_changed_lines,           # 被截断的修改行数，0 表示完整
}
```

- 只在工具执行成功且 before != after 时生成；截断发生在采集时，避免超大文件撑爆事件与 journal。
- 截断按 changed lines 计数（`+` / `-` 行），上下文行固定小窗口（建议 3 行），并保留内部总渲染行数兜底。
- 只覆盖 UTF-8 文本 file tool；`code_run` / shell 导致的文件删除或修改本期不生成 file diff。
- diff 详情不回灌给 LLM；tool result 只保留短状态信息。因写前校验、`replace_all`、no-op 等工具语义变化产生的成功 / 失败状态和少量计数字段允许对模型可见。

## 事件透传与持久化

- `ExecutedToolUse` 增加 `file_change: Option<FileChange>`。
- `SessionTurnEvent::ToolCallCompleted` 与 `SessionEvent::ToolCallCompleted` 透传该字段，降级转发也不能丢弃。
- `TurnJournalToolCall` 持久化截断后的 `FileChange`；resume 回放把它灌回 ToolCell，保证 resume 后历史 diff 可见。
- serde 字段使用 `#[serde(default, skip_serializing_if = "Option::is_none")]`，保证旧 journal 兼容。
- 本期采用 per-tool diff：哪个 `file_write` / `file_patch` 成功并生成 `FileChange`，就在对应 ToolCell下展示；不做 turn-level net diff 聚合。
- journal 只恢复历史展示信息，不恢复 read state；历史 diff 可见不代表对应文件仍具备写入许可。
- subagent 的 `transcript.jsonl` 在 `tool_completed` 记录中持久化同一份有界`FileChange`。该字段仅供显式读取和排障，不自动投影到主 agent 上下文，也不在`/subagents` 列表展开。

## 渲染形态（src/session_tui/cell.rs / transcript.rs）

`ToolCell` 增加 `file_change` 字段；completed 且成功且有 `file_change` 时，在输入预览行之后渲染：

```
• Called file_patch
  └ {"path":"src/foo.rs",...}
    Edited src/foo.rs (+3 -1)
    41    fn resolve() {          ← 上下文行，MUTED_FG
    42  -     let x = old();      ← 删除行，红
    42  +     let x = new();      ← 新增行，绿
    43  +     retry(x);
    ⋮ 其余 12 行修改未展示        ← truncated_changed_lines > 0 时
```

- 行号 + 标记 + 内容；折行复用 `wrapping.rs`。
- 新增行使用淡绿整行背景，删除行使用淡红整行背景；背景覆盖当前 diff 可用宽度，折行续行与窄屏 marker-only fallback 必须保持相同语义背景。上下文行、hunk 间隔与截断提示继续使用普通 surface，避免整块 diff 形成高饱和色墙。
- 展示位置约束：live 投影（`transcript.rs` 的 `live_status_lines` / `active_assistant_lines`）走不带 diff 的渲染路径，只有 `scrollback_lines` 路径渲染 diff。实现上给 ToolCell 渲染入口加渲染上下文参数（live / scrollback）区分。
- kind = created 时头部展示 `Added src/foo.rs (+N -0)`，内容全为绿行，仍受同一截断上限约束。
- kind = modified 时头部展示 `Edited src/foo.rs (+A -D)`。

## 配置

`config.toml` 的 `[agent.tool]` 下新增采集截断上限，TUI 直接渲染已截断结果，不另设展示侧配置：

```
[agent.tool]
file_diff_max_changed_lines = 20
```

## 测试与验收

- 采集：patch 唯一匹配 / patch replace_all / write overwrite / write append / write prepend / 新建文件，`FileChange` 的 kind、增删统计、截断行数正确；before == after 不生成 diff。
- 写前校验：已有文件未完整 `file_read` 时拒绝写入；文件 stale 时拒绝写入；新建文件允许无 read state；append / prepend 读旧文件的非 NotFound 错误会失败。
- `file_patch`：两处及大量重复匹配都返回固定长度歧义错误；`replace_all = true` 全量替换并返回处数；0 匹配行为不变。
- TUI：turn 进行中虚线框内已完成的 file 工具不出现 diff 行；`TurnCommitted` 落 scrollback 后出现；超限截断提示正确；失败调用不渲染 diff。
- resume：journal 回放后历史 ToolCell 带 diff。

验收标准：

- 修改成功的 `file_write` / `file_patch` 在 history 区可见红绿 diff。
- `+` / `-` 行不仅文字有语义色，整条物理终端行也分别使用淡绿 / 淡红背景；文字末尾后的空白区域、折行续行和极窄终端均不得漏回 surface 背景。
- streaming 虚线框内始终无 diff。
- 超长修改按 `file_diff_max_changed_lines` 截断，默认 20 行修改。
- `file_patch` 多匹配不会误改或产生无界错误，扩大上下文可精确定位，`replace_all` 生效。
- 已有文件修改必须基于完整 `file_read` 且通过 stale guard。

## 本阶段不做

- diff 按语言语法高亮（syntect）。
- `code_run` / shell 的文件修改检测（执行前后快照比对）。
- 修改后片段回灌 tool result，帮助模型自校验。
- turn-level net diff 聚合，合并一个 turn 内对同一文件的多次修改。
