# PRD: Session Search

> 状态：已实现。本文描述 `session_search` 的当前产品语义、数据边界与验收标准。

## 背景

长期使用 ACN 后，同一个 agent 会积累多个 session。主模型需要一种只读方式，在处理当前任务时查找自己过去讨论过的内容，并按需回读原始上下文。

`session_search` 是主模型可调用的 agent tool。它通过 agent 级 SQLite 派生索引完成召回，再从 session 权威 transcript 回读有界原文 evidence。该能力没有独立的 slash command、TUI 面板或 CLI 命令。

## 目标

- 浏览本 agent 最近的历史 session。
- 按关键词、短语、布尔表达式、前缀或 CJK 文本检索历史 session。
- 回读指定 session 的头尾内容，或围绕指定 message 定位上下文。
- 返回足够判断来源和边界的 metadata、snippet 与原文 evidence。
- 保持严格只读，不改变当前 session 或历史 session。
- 索引故障不得影响 canonical session 写入和正常对话。

## 非目标

- 不做 embedding 或语义向量检索。
- 不跨 agent 搜索。
- 不搜索尚未提交的 turn journal 尾部。
- 不依赖 compaction summary 或运行时 projection。
- 不生成 LLM summary，也不为搜索额外调用一次 LLM。
- 不生成 claim、dispute、trace 或 Memory。
- 不提供独立后台 indexer、搜索管理 UI 或跨 agent 权限模型。

## 使用入口

主模型通过 `session_search` tool 使用四种只读模式。规范调用应只选择一种参数形态，不传空字符串或无关占位字段。

| 模式 | 参数形态 | 返回内容 |
|---|---|---|
| `browse` | 省略 `query`、`session_id`、`around_message_index` 和 `window`；可传 `limit` | 最近的历史 session、message 数和首条真实 user message preview |
| `discover` | 非空 `query`；可传 `limit`、`sort`、`include_tool_results` | 每个 session 一个检索命中、snippet、anchor 附近原文和 session 头尾 bookend |
| `read` | 非空 `session_id`；可传 `include_tool_results` | 短 session 的全文，或长 session 的头部与尾部 |
| `scroll` | 非空 `session_id` 与 `around_message_index`；可传 `window`、`include_tool_results` | 以指定 message 为 anchor 的连续原文窗口 |

`session_id` 应来自 browse 或 discover 返回的其他历史 session。browse 和 discover 会排除当前 session；read 和 scroll 按调用方提供的 `session_id` 回读，不额外承担当前 session 访问控制。

### 参数

- `query`：discover 使用的非空查询文本；省略时进入 browse。
- `limit`：browse/discover 最多返回多少个 session，默认 `3`，最大 `5`。
- `sort`：discover 排序，可选 `relevance`、`newest`、`oldest`，默认 `relevance`。
- `session_id`：read/scroll 使用的历史 session ID。
- `around_message_index`：scroll 的 anchor message index，必须与 `session_id` 一起使用。
- `window`：scroll 在 anchor 两侧各返回多少条 message，默认 `5`，范围 `1`～`20`。
- `include_tool_results`：是否允许 tool result 成为检索命中并返回其内容，默认 `false`。

browse 固定按 session 的 `updated_at`、`created_at` 从新到旧排列，不使用 `sort` 改变顺序。

## 查询语义

普通查询支持：

- 关键词：`docker deployment`
- 短语：`"docker networking"`
- 布尔组合：`docker OR kubernetes`
- 排除：`python NOT java`
- 前缀：`deploy*`

系统会对常见易碎标点、重复通配符和悬空布尔操作符做轻量规范化，避免把自然语言直接变成无效 FTS5 表达式。

CJK 查询采用两级召回：

- 较长 CJK 查询使用 trigram FTS5 索引。
- 短 CJK 词和不适合 trigram 的组合使用 `LIKE` 回退。

CJK 回退保留 `AND`、`OR`、`NOT` 的基本筛选语义，但不承诺与 BM25 relevance 完全相同的排序。

## 检索范围

权威范围固定为当前 upstream 下、本 agent 已持久化的其他 session：

```text
<acn_home>/<upstream>/data/agents/<agent_id>/sessions/<session_id>/session.yaml
<acn_home>/<upstream>/data/agents/<agent_id>/sessions/<session_id>/messages.jsonl
```

边界如下：

- browse/discover 排除当前 session。
- 不读取其他 agent 的 session。
- 只消费 canonical `messages.jsonl`。
- unresolved `turn_events.jsonl` tail 不进入搜索。
- compaction summary、provider projection 和 delegation 内部 transcript 不作为搜索来源。
- SQLite 只负责召回和定位；返回的 evidence 从权威 `messages.jsonl` 回读。

## Evidence 视图

### Discover

- 每个 session 最多返回一个命中。
- anchor 默认保留前后各 `5` 条 message。
- 若 session 更长，额外返回头部和尾部各最多 `3` 条非 tool-result bookend。
- 返回 `matched_role`、`match_message_index` 和 snippet，便于继续 scroll。

### Read

- message 总数不超过 `30` 时返回完整 session。
- 更长的 session 返回头 `20` 条和尾 `10` 条，并设置 `truncated = true`。

### Scroll

- 以 `around_message_index` 为 anchor。
- `window` 表示 anchor 两侧各取多少条 message。
- 返回 `messages_before`、`messages_after` 和 anchor 标记。

### 内容边界

- 普通 text block 最多返回 `4,000` 字符。
- 显式包含的单个 tool result 最多返回 `4,000` 字符。
- `include_tool_results = false` 时，纯 tool-result message 不作为 discover 命中；evidence 中只保留轻量 omitted marker 和省略数量。
- 图片、文档和显式 skill instruction 只返回轻量 metadata，不返回 base64 内容。
- `truncated`、`tool_results_omitted`、`messages_before` 和 `messages_after` 用于提醒调用方当前结果不是完整 transcript。

## 返回协议

browse/discover 使用 `results`：

```json
{
  "success": true,
  "mode": "discover",
  "query": "docker networking",
  "results": [
    {
      "session_id": "session_xxxxxxxx",
      "when": "2026-05-20T12:00:00Z",
      "source": "tui",
      "model": "example-model",
      "message_count": 42,
      "matched_role": "user",
      "match_message_index": 17,
      "snippet": "...docker networking...",
      "bookend_start": [],
      "messages": [],
      "bookend_end": [],
      "messages_before": 5,
      "messages_after": 5
    }
  ],
  "count": 1,
  "sessions_searched": 1,
  "index_incomplete": false,
  "warnings": []
}
```

read/scroll 使用顶层 `session_id`、`session_meta`、`messages` 和窗口 metadata，`results` 为空且 `count = 0`。

公共字段：

- `success`：本次 tool 是否完成其业务操作。
- `mode`：`browse`、`discover`、`read` 或 `scroll`。
- `query`：规范化后的实际查询；非 discover 模式为空字符串。
- `index_incomplete`：索引可能落后、部分 repair 失败或查询不可用时为 `true`。
- `warnings`：可供模型判断降级原因的诊断信息。

非法字段、非法 `sort` 或无法解析的 `session_id` 属于 tool 参数错误。索引不可用、session/message 不存在等可恢复业务失败返回结构化 JSON：

```json
{
  "success": false,
  "mode": "discover",
  "query": "docker networking",
  "results": [],
  "count": 0,
  "sessions_searched": 0,
  "index_incomplete": true,
  "warnings": ["session search index is temporarily unavailable"]
}
```

## 索引与一致性

每个 agent 使用一个派生 SQLite 索引：

```text
<acn_home>/<upstream>/data/agents/<agent_id>/session_search_index.sqlite
```

索引包含 session metadata、逐 message 可搜索文本、普通 FTS5 与 CJK trigram FTS5 数据。它不是权威存储，删除后可从 `session.yaml` 和 `messages.jsonl` 重建。

### 更新策略

- canonical message 成功提交后，best-effort 增量写入本 session 的新增 message。
- 索引失败只记录 warning，不回滚已经成功的 session turn。
- 每次调用 session_search 前扫描本 agent 的其他 session，并按需 repair。
- 缺失或落后的索引补写新增 message。
- message 数倒退、索引版本变化或普通表/FTS 表不完整时，按 session 重建。
- 已删除 session 的孤儿索引会在 repair 或 session cleanup 中清理。

### 并发策略

同一个 agent 的多个进程共享该 SQLite：

- 优先使用 WAL；不可用时回退到 DELETE journal。
- 写入使用短事务、busy timeout 和有限重试。
- 默认 busy timeout 为 `500ms`。
- 锁竞争或部分 repair 失败时允许索引暂时落后，并通过 `index_incomplete` 与 `warnings` 暴露。
- 查询只承诺读取已提交的索引快照，不承诺立即看见其他进程刚提交但尚未索引的 message。

## 配置

`[agent.tool]` 下提供：

- `session_search_default_limit`：默认返回 session 数，默认 `3`。
- `session_search_max_limit`：允许请求的最大 session 数，默认 `5`。
- `session_search_sqlite_busy_timeout_ms`：SQLite busy timeout，默认 `500`。

三个值都必须大于 `0`，且 default limit 不能大于 max limit。Session search 不提供 summary/context 字符预算配置。

## 验收标准

- browse 能列出最近历史 session 和稳定 preview。
- discover 能按普通查询与 CJK 查询召回相关历史 session，并排除当前 session。
- discover 的 `limit` 在 session 聚合后生效，同一 session 不重复占用结果。
- read/scroll 从权威 transcript 返回原文 evidence 和明确窗口边界。
- 默认不返回 tool result 正文或媒体 base64；显式开启 tool result 后仍受单 block 上限约束。
- unresolved turn journal tail 不进入索引。
- 索引可幂等增量更新、按 session 重建并清理孤儿数据。
- 索引失败不会破坏 canonical transcript，也不会生成 claim、dispute、trace 或 Memory。
- 业务失败保持结构化返回，主模型可以根据 `warnings` 选择缩小查询、改用 browse 或停止检索。

## 后续方向

- 后台预热大型历史索引。
- 更完整的 FTS5 query sanitizer。
- 显式索引检查、重建与迁移命令。
- session 浏览和检索 UI。
- 经独立权限设计后的跨 agent session search。
- 如未来需要 LLM summary，重新定义独立的产品入口、预算、错误语义与测试边界。
