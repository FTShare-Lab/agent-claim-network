# ACN Turn Journal 与 Mid-Turn 恢复 PRD

> 状态：已实现。本文保留 turn journal、steer、interrupt 与中断恢复语义。

本文档记录 ACN 对话结构从“turn 成功后一次性落 canonical transcript”升级为“turn 中间行为可持久记录、可恢复、可被用户中途引导”的产品语义与设计决策。

当前文档优先保存已经拍板的产品语义与实现边界；实现时如发现更细的工程参数，再按本文约束补充。

---

## 背景

ACN 当前 session 持久化以 `session.yaml` + `messages.jsonl` 为权威存储。`messages.jsonl` 只保存已经成功提交的 provider-valid transcript。`AgentTurnLoop` 在内存中收集当前 turn 的 user / assistant / tool_result，`SessionEngine` 等整个 turn 成功返回后才调用 `append_session_turn_messages` 写入 `messages.jsonl`。

这导致一个明确问题：如果 turn 中途 provider stream 失败、工具后续失败、用户主动中断、进程退出，当前 turn 中已经发生的用户输入、assistant partial 输出、工具执行进展等不会作为可恢复的结构化事实落盘。TUI 运行时能看到这些事件，但 resume 后会丢失。

为解决这一问题，ACN 将 committed transcript 与 turn 中间事实分层存储：streaming delta 主要用于UI 恢复，完整模型响应单元、turn lifecycle 和关键 tool 终态进入结构化 journal；中途用户输入可以进入后续输入队列，interrupt 则形成模型可理解、但不会污染 canonical transcript 的恢复上下文。

---

## 目标

- 用户中断退出、进程失败或 provider/tool 中途失败后，ACN 能恢复“刚才发生了什么”。
- 用户在 turn 运行中可以继续输入。
- 普通 Enter 在运行中表示排队下一 turn。
- Ctrl+Enter 在运行中表示打断当前 turn 并注入引导。
- `messages.jsonl` 保持 committed canonical transcript，不被半截 assistant/tool_use 污染。
- 中断/失败 turn 可在 resume UI 中展示。
- session search V1 保持只搜索 canonical `messages.jsonl`；若后续 continuation committed，吸收后的 recovery wrapper 可被搜索到。
- compact/finalize/memory_review 不因为中断半截内容产生未经用户确认的业务后果。
- 避免文件数量膨胀；优先每个 session 一个结构化 journal。

---

## 非目标

- 不把 raw token delta 直接作为 provider replay 历史。
- 不修复未闭合或半截 tool_use JSON 后继续执行。
- 不让 partial tool_use 进入 `messages.jsonl`。
- 暂时不实现任意工具的强制 kill/cancel 语义；工具执行默认等当前工具完成后再处理中断。
- 暂时不让 interrupted/failed journal 直接生成 claim、memory、dispute 或 finalize trace。
- 暂时不把 `messages.jsonl` 升级成 event-sourced 格式。

---

## 已拍板决策

### 1. 持久化模型

采用新增结构化 journal 的方案：

```text
session.yaml
messages.jsonl
turn_events.jsonl
session_events.log
```

`messages.jsonl` 继续保持 committed canonical transcript。`turn_events.jsonl` 记录 turn 中间行为、失败/中断状态、用户中途引导和恢复所需事实。

对每个已通过 provider 处理并到达 transcript commit gate 的 user turn，journal 还会在
canonical user message 已构造、写入 `messages.jsonl` 之前追加一个
`canonical_user_message` 事件。该事件只写经稳定 JSON 序列化后计算的
`sha256-v1` 内容哈希；哈希对应 `messages.jsonl` 同一条 user message 的完整内容块
（包括文本附件、图片/PDF 媒体块与 skill 块），不重复保存附件正文或媒体数据。它只服务于
resume 的两份数据对齐与崩溃恢复校验；它不改变
`messages.jsonl` 的 canonical 权威地位，也不进入 session_search、compact、finalize 或
memory_review。TUI 用户气泡仍只显示这条 message 的原始用户正文，不能展示附件展开内容。

旧 journal 没有该事件时，resume 保持兼容：回退使用 `user_input_accepted` 的文本和
`messages.jsonl` 的简化历史；已写入旧版完整 `content` 的事件在读取时即时计算同一哈希，
不要求迁移或重写 journal。

原因：

- `messages.jsonl` 当前影响 resume、session_search、compact、finalize、memory_review。
- 直接把 partial content 写入 `messages.jsonl` 会破坏 provider-valid transcript 约束。
- 单独 journal 的影响面小，V1 先通过 projection/view 层接入 UI/recovery；未来如需 journal search，再单独接入派生搜索视图。

### 2. Mid-Turn 内容进入模型上下文

采用“journal 事实 + recovery projection”的方案。

中断或失败后，journal 不会原样进入模型，也不会直接 materialize 到 `messages.jsonl`。下一次用户继续或注入引导时，由 recovery projection 从 journal 构造一段模型可见上下文，例如：

```text
<interrupted_turn_context>
{"unresolved_turn_count":1,"unresolved_turns":[{"previous_turn_status":"interrupted_by_user","original_user_request":"...","assistant_partial_or_completed_summary":"...","tools_completed":[...],"tools_interrupted":[...],"tools_skipped":[...],"tools_pending_or_skipped":[...],"user_steer":"..."}]}
</interrupted_turn_context>

<current_user_request>
...
</current_user_request>
```

raw journal 不直接成为 canonical transcript。若 recovery projection 实际参与了一次成功 committed 的 continuation turn，则以受控的 contextual wrapper 写入这次 canonical user message，保证后续 resume/replay 能看到当时影响模型回答的上下文。只有经过 projection 的摘要/片段进入 canonical，raw delta、半截 tool_use、未确认的大段 tool_result 不进入 canonical。

原因：

- 用户说“继续”或“刚才不对，改成...”时，模型需要知道上一轮中断点。
- 直接 canonical 化半截 turn 风险大。
- interrupted turn 信息需要转换为后续模型可理解的上下文，同时过滤或避免 unresolved tool_use。

### 3. 中途用户输入语义

运行中普通 Enter：排队下一 turn。

运行中 Ctrl+Enter：打断当前 turn 并注入引导。

原因：

- ACN TUI 现有体验已经有 running turn 期间输入排队的雏形。
- Enter 保持排队能避免用户误把普通后续问题变成当前 turn steer。
- Ctrl+Enter 作为显式 interrupt-and-steer，语义更强。

### 4. Interrupt 硬语义

分层处理：

- provider streaming 阶段可以硬中断当前 provider request。
- tool 执行阶段默认软中断：必须等当前工具执行结束，随后在安全边界处理中断/注入。
- 后续若某些工具明确支持安全 cancel，再单独设计 cancel token 语义。

原因：

- provider streaming 中断不会制造外部副作用。
- tool 执行可能已经产生文件/网络/命令副作用，强杀最容易制造不可重放状态。
- ACN 暂时采用保守策略：没有明确安全取消契约的工具一律等待当前调用结束。

### 5. Journal 记录粒度

采用“delta 也记，但 canonical 只认完整块”的方案。

journal 记录 assistant delta，用于恢复 UI 展示“写到哪里了”。V1 对 journal 选择完整保留可重建的 assistant partial text；但 provider replay、compact、finalize、memory_review 不直接消费 raw delta。

完整 assistant message、tool completed、turn status 等结构化事件会作为 recovery 与 TUI replay projection 的主要来源。

原因：

- 崩在 streaming 中间时，用户仍能看到 partial 输出。
- raw delta 噪音大，不适合直接进入 LLM 上下文、长期 search 或业务派生。

### 6. Tool Use 半截处理

partial tool_use 永不 canonical，只作为 UI/journal 事实。

不尝试修复半截 JSON，不猜测工具参数，不执行未完整闭合的 tool_use。

完整 tool_use 已结束但 tool_result 未执行的恢复执行语义，后续另行设计；暂时不自动恢复执行。

若已完整接纳 provider 的 tool_use、但在真正 dispatch 前收到 cancel 或 steer，则每个尚未启动的调用记为 `tool_call_skipped`。它是明确的终态，带 `turn_cancelled_before_dispatch` 或 `turn_interrupted_before_dispatch` 原因；不会伪装成 `tool_call_started`，也不会产生 `tool_result`。

`tool_call_started` 只表示该调用已通过取消 gate、已承诺进入 dispatch。已启动调用若在协作取消中停止，仍使用 `tool_call_interrupted`；这与从未启动的 skipped 严格区分。

tool_use / tool_result 的持久化分层：

- journal 记录结构化事实，包括 `tool_use_id`、工具名、输入预览或 size-limited 输入、完成状态、输出预览、truncated 标记。
- recovery context 使用摘要/预览，不把大段 tool_result 原样塞入 canonical。
- recovery context 将已知未 dispatch 的调用放入 `tools_skipped`（保留工具名、输入预览和 skip reason）；`tools_pending_or_skipped` 只保留旧 journal 或异常中断留下的 started-only 不确定记录，不据此自动恢复执行。
- `messages.jsonl` 只在普通成功 turn 中保存 provider-valid 的 assistant tool_use 与对应 user tool_result。
- interrupted/failed/cancelled turn 不把 tool_use/tool_result 写入 `messages.jsonl`；后续 continuation committed 时，只把受控的 interrupted context wrapper 写入新的 canonical user message。

### 9. 失败 Turn 状态定义

turn 终态至少区分：

- `committed`
- `failed`
- `cancelled`
- `interrupted_by_user`

原因：

- 用户主动引导中断、用户取消、系统失败不是同一语义。
- resume、UI、recovery context 以及未来可能的 journal search projection 都需要区分这些状态。

### 10. 恢复 UI 展示

resume 时展示 committed 历史 + interrupted/failed turn 灰色状态。

interrupted/failed turn 不应伪装成已 committed 对话，但也不能消失。

---

## 补充已拍板设计

### Delta 写入与 Flush 策略

assistant delta 不逐 chunk 写入 journal，而是合并为 snapshot 写入：

- 按时间或字符阈值批量写，阈值从配置读取，默认约 500ms 或累计约 1KB 写一次。
- assistant 完整结束时写 `assistant_completed`。
- TUI 当前运行时仍可实时显示内存中的 streaming delta；journal snapshot 用于 crash/resume 恢复。

这里的 assistant delta snapshot 主要是给 TUI timeline / crash resume 看的事实日志，不是直接塞给 LLM 的模型上下文。LLM 只消费从 journal 派生出的 recovery projection；projection 可以使用这些 delta 还原 partial assistant text，再按 recovery context 上限截断或摘要。

如果把“全部保留”理解为 journal 是否保留可重建的完整 partial assistant text，V1 选择全部保留，不做 journal 瘦身；如果理解为 LLM recovery context 是否完整吃掉这些内容，则不选择全部进入模型，而是使用配置上限做 bounded projection。

为避免文件膨胀，snapshot 语义是“合并后的增量文本片段”，不是每次都重复写完整累计全文。TUI replay 时按顺序拼接这些片段；`assistant_completed` 到达后以完整 assistant message 作为该 turn 的最终展示事实。

journal flush 分级：

- 关键事件必须及时 flush：`turn_started`、`user_input_accepted`、`user_steer_submitted`、`interrupt_requested`、`interrupt_pending`、`tool_call_started`、`tool_call_completed`、`tool_call_interrupted`、`tool_call_skipped`、`turn_finished`。
- 高频事件可以批量 flush：`assistant_delta` snapshot、`tool_call_progress`。

原因：

- 保证关键生命周期和用户输入不丢。
- 避免每个 token/chunk 都触发文件写入和 flush。
- 时间与字符阈值属于实现超参数，放入配置便于后续按真实 TUI/磁盘表现调整。

### 7. Session Search 是否搜索 Journal

已确认：V1 只搜索 canonical `messages.jsonl`，不搜索 journal。

原因：

- 后续 continuation turn 成功 committed 后，recovery context 会以受控 wrapper 进入 canonical user message，重要上下文最终会被 `messages.jsonl` 搜到。
- SQLite schema 当前已围绕 `messages.message_index == SessionMessage.index` 建模，V1 不为了 journal projection 改动这个边界。
- unresolved interrupted tail 的主要入口是 `/resume`，不是跨 session search。

未来若要让 session_search 搜 failed/interrupted journal，应通过新增派生搜索表（如 `search_entries`）承载 canonical 与 journal projection，而不是把 journal rows 塞进现有 `messages` 表。现有 `messages` 表必须继续只表示 canonical `SessionMessage.index`。

### 8. Compact / Finalize / Memory Review 是否消费 Journal

已确认：暂时只消费 canonical `messages.jsonl`。

规则：

- compact/finalize/memory_review 不吃 unresolved interrupted tail。
- 用户后续正常 turn 如果吸收了 interrupted context，并成功 committed，则业务派生只消费这个新的 committed turn。
- interrupted journal tail 在补充执行成功落到 `messages.jsonl` 后视为 resolved。
- unresolved journal tail 保留用于 resume/recovery，不参与 session_search、claim、memory/dispute。
- V1 不做 journal compact，因此 resolved tail 也保留 raw journal 细节，保证 `/resume` 的 TUI timeline 尽量与退出前一致。

### 11. 文件命名和范围

已确认：每个 session 一个 `turn_events.jsonl`，不采用每个 turn 一个文件。

原因：

- 每 session 一个文件即可顺序记录完整生命周期。
- 每 turn 文件会增加目录和清理复杂度。
- 用户明确不希望文件太多。

V1 不做 journal 瘦身，不删除历史 turn 的 raw journal 事件。`/resume` 的 TUI 展示优先通过 `turn_events.jsonl` 重建 timeline，从而尽量保持退出前后看到的 interrupted/resolved turn 细节一致。

`messages.jsonl` 仍然是 canonical model transcript 和业务派生的权威来源，但 TUI timeline 不直接从 `messages.jsonl` 渲染 recovery wrapper。若 `turn_events.jsonl` 缺失、不完整或来自旧 session，则退回到 `messages.jsonl` 生成简化历史视图。

后续如果 `turn_events.jsonl` 过大，再单独设计 journal compaction；在没有明确视觉等价策略前，不自动删除 raw delta 或 resolved interrupted turn 细节。

### Recovery Context 长度上限

recovery context 是给 LLM 的模型上下文，不是给 TUI 展示的内容。

- TUI resume 展示优先 replay 完整 `turn_events.jsonl`，不受 recovery context 截断限制。
- LLM recovery context 从 journal projection 构造，需要按字段截断，避免把超长 partial 输出或 tool result 塞爆上下文。

V1 上限从配置读取，默认值如下；具体配置文件字段名实现时按现有 config 风格落地：

- original user request：默认最多 8192 字符。
- partial assistant text：默认最多 8192 字符。
- tool input preview：默认最多 2048 字符。
- tool output preview：默认最多 4096 字符。
- user steer text：默认最多 8192 字符。

完整细节仍保留在 `turn_events.jsonl`，截断只影响下一次模型请求中的 recovery wrapper。

recovery wrapper 的 `<interrupted_turn_context>` 标签内使用单个 JSON payload 表示结构化字段；用户文本、assistant 文本和 tool preview 必须作为 JSON string/array/object value 写入，不允许裸行拼接，避免换行、冒号或闭合标签伪造字段。

### Ctrl+Enter 与附件

V1 Ctrl+Enter interrupt-and-steer 只支持纯文本 steer。

附件类输入只支持排队，不支持立刻插入当前 turn：

- 图片、PDF、`@path` 附件不能作为 Ctrl+Enter steer 注入。
- 用户在含附件输入上按 Ctrl+Enter 时，TUI 应提示：附件输入已排队，不能打断注入当前 turn。
- 用户可用普通 Enter 排队该输入，或等待当前 turn 结束后再提交。
- 这类输入不写 `user_steer_submitted`；按 queued user input 处理，避免 recovery wrapper 与 provider attachment payload 状态不一致。

原因：

- 附件会显著复杂化 journal、recovery wrapper 与 provider request 的一致性。
- V1 优先保证 interrupt-and-steer 的文本语义稳定。

### Pending Steer 合并

当当前 turn 正在执行工具，Ctrl+Enter 已记录为 pending steer 但尚未处理时，如果用户再次 Ctrl+Enter：

- 按时间顺序合并多次 steer，形成一个 pending steer block。
- 工具结束后的安全边界只处理一次合并后的 steer。
- 合并后的 block 保留每次 steer 的提交顺序与时间信息；模型侧看到的是一次连续的用户引导。

这个规则只覆盖一种场景：当前工具还没结束，上一条 Ctrl+Enter steer 还处于 pending 状态，此时用户又提交新的 Ctrl+Enter steer。普通 Enter 排队输入不参与合并；provider streaming 阶段若已能立即中断，也不需要走 pending merge。

原因：

- 避免丢失用户连续补充的引导。
- 避免排成多个 steer turn 导致语义碎裂。
- 与“工具执行必须等当前工具结束”保持一致。

### Ctrl+C 取消与普通队列

运行中普通 Enter 提交的输入是 queued next turn，不是 steer。

如果用户随后用 Ctrl+C 取消当前正在进行的 turn：

- 当前 turn 标记为 `cancelled` 或 `interrupted_by_user`，取决于实际触发语义；纯取消默认是 `cancelled`。
- 已排队的普通输入不自动进入下一 turn。
- TUI 应将 queued input 按原顺序恢复到 composer 中，作为草稿等待用户再次确认。
- 用户再次按普通 Enter 时，才会把恢复后的内容作为新的 turn 发送。

原因：

- Ctrl+C 的核心语义是“停下当前行为”，不是“停下当前并立即执行队列”。
- 用户可能是因为发现方向不对才取消；自动发送旧队列会制造意外的后续请求。
- 当前 ACN TUI 已是这个方向：running turn 取消或失败时，queued inputs 会恢复到输入栏，而不是自动 dispatch。

### Journal 损坏或缺失的 Resume 策略

如果 `turn_events.jsonl` 缺失、不完整、损坏或来自旧 session：

- 不阻塞打开 session。
- 降级为从 `messages.jsonl` 渲染简化历史。
- TUI 给出轻量提示，说明 journal 不完整，已使用 canonical transcript 恢复。
- 该降级只影响 TUI 历史完整度，不改变 canonical transcript、session_search、compact、finalize、memory_review 的语义。

`messages.jsonl` 仍是 canonical 权威；journal 损坏不能让 session 不可用。

---

## Journal 事件草案

`turn_events.jsonl` 使用 append-mostly JSONL。每行带全局递增 `seq`、`turn_id`、`kind`、`created_at`。

示例：

```json
{"seq":1,"turn_id":"turn_1","kind":"turn_started","created_at":"..."}
{"seq":2,"turn_id":"turn_1","kind":"user_input_accepted","text":"...","created_at":"..."}
{"seq":3,"turn_id":"turn_1","kind":"assistant_delta","text":"...","created_at":"..."}
{"seq":4,"turn_id":"turn_1","kind":"assistant_completed","text":"...","created_at":"..."}
{"seq":5,"turn_id":"turn_1","kind":"tool_call_started","tool_use_id":"toolu_1","name":"web_search","summary":"...","input_preview":"...","input_truncated":false,"created_at":"..."}
{"seq":6,"turn_id":"turn_1","kind":"tool_call_completed","tool_use_id":"toolu_1","summary":"...","output_preview":"...","output_truncated":false,"created_at":"..."}
{"seq":7,"turn_id":"turn_1","kind":"user_steer_submitted","text":"...","created_at":"..."}
{"seq":8,"turn_id":"turn_1","kind":"interrupt_requested","reason":"user steer pending","created_at":"..."}
{"seq":9,"turn_id":"turn_1","kind":"interrupt_pending","reason":"user steer pending","created_at":"..."}
{"seq":10,"turn_id":"turn_1","kind":"tool_call_skipped","tool_use_id":"toolu_2","name":"working_note","summary":"...","input_preview":"...","input_truncated":false,"reason":"turn_interrupted_before_dispatch","created_at":"..."}
{"seq":11,"turn_id":"turn_1","kind":"turn_finished","status":"interrupted_by_user","created_at":"..."}
```

当前确定的候选事件：

- `turn_started`
- `user_input_accepted`
- `canonical_user_message`（成功构造的 canonical user content 的 `sha256-v1` 哈希，用于与 `messages.jsonl` 对齐）
- `user_steer_submitted`
- `interrupt_requested`
- `interrupt_pending`
- `assistant_delta`
- `assistant_completed`
- `tool_call_started`
- `tool_call_progress`
- `tool_call_completed`
- `tool_call_interrupted`
- `tool_call_skipped`
- `turn_finished`

---

## `messages.jsonl` 语义

`messages.jsonl` 只在 turn committed 后写入完整 provider-valid session messages。

中断/失败/cancelled turn 不写入 `messages.jsonl`。

下一次 turn 如果通过 recovery context 成功继续并 committed，`messages.jsonl` 记录的是这一次真实请求的 canonical user message 与 assistant/tool_result 序列。其中 user message 可以包含受控的 `<interrupted_turn_context>` wrapper 与当前用户请求；它不记录上一轮 raw delta、半截 assistant/tool_use，也不把未确认的大段 tool_result 原样塞入 canonical。

---

## Search Projection 草案

V1 不实现 journal search projection。session_search 继续只从 `messages.jsonl` 派生索引。

规则：

- `messages` 表继续只表示 canonical `SessionMessage.index`。
- continuation turn 成功 committed 后，recovery context wrapper 作为新 user message 的一部分进入 canonical，重要上下文后续可被 session_search 搜到。
- unresolved interrupted tail 不进入 session_search；它通过 `/resume` 恢复。

未来若要支持 journal search，应新增派生视图或派生表，例如 `search_entries`，并保持现有 `messages.message_index` 语义不变。

---

## Compact / Cleanup 草案

Journal cleanup 不应破坏 `/resume`。

当前规则：

- V1 保留完整 journal，不做瘦身。
- unresolved failed/interrupted/cancelled turn 使用 journal 做恢复。
- resolved interrupted turn 仍保留原始 journal 细节，用于 `/resume` 重建与退出前一致的 TUI timeline。
- 后续如需 journal compact，必须先定义 compact 前后 UI 等价策略。

---

## TUI Resume 草案

`/resume` 分两层恢复：

- canonical 状态：读取 `session.yaml` + `messages.jsonl`，用于后续模型请求、compact、finalize、memory_review、session_search。
- TUI timeline：优先 replay `turn_events.jsonl`，还原普通 committed turn、interrupted/failed/cancelled turn、partial assistant、tool 状态与 continuation 关系。

TUI 不直接从 `messages.jsonl` 渲染 `<interrupted_turn_context>` wrapper。若 `turn_events.jsonl` 缺失、损坏或旧 session 没有 journal，则 fallback 到 `messages.jsonl` 渲染简化历史。

---

## 实现阶段 TODO / Planning

实现时按阶段推进；每次进入下一阶段前必须重新阅读本文档，确保实现没有偏离已拍板语义。

### 阶段 0：PRD 与现状确认

Planning：

- 确认本文档没有待拍板问题。
- 确认当前代码里 `messages.jsonl`、session_search、memory_review、TUI queue/cancel 的现状。

验证：

- `rg` 确认本文档无 `待确认` / `待回答` 残留。

### 阶段 1：Turn Journal 存储基础

Planning：

- 新增每 session 一个 `turn_events.jsonl` 的路径、事件类型、append、read、replay/projection 基础能力。
- 事件至少覆盖：`turn_started`、`user_input_accepted`、`user_steer_submitted`、`interrupt_requested`、`interrupt_pending`、`assistant_delta`、`assistant_completed`、`tool_call_started`、`tool_call_progress`、`tool_call_completed`、`tool_call_interrupted`、`tool_call_skipped`、`turn_finished`。
- 增加 turn status：`committed`、`failed`、`cancelled`、`interrupted_by_user`。
- append 使用 JSONL；关键事件及时 flush；assistant delta snapshot 支持按配置阈值合并写入。
- journal 损坏/缺失读取不得让 session 不可用，projection 层返回降级信息供 TUI 使用。

验证：

- 单元测试：事件 serde roundtrip、append/read 顺序、seq 单调、缺失文件读取为空、损坏行降级。
- 单元测试：assistant delta snapshot 可按顺序重建完整 partial assistant text。
- 单元测试：replay 能识别 committed / failed / cancelled / interrupted turn。

### 阶段 2：SessionEngine / AgentTurnLoop 接入

Planning：

- 在 turn 开始、用户输入接受、assistant delta snapshot、assistant completed、tool start/progress/completed、turn committed/failed/cancelled/interrupted 等关键节点写 journal。
- `messages.jsonl` 仍只在 turn committed 后写入完整 provider-valid session messages。
- 中断/失败/cancelled turn 不写 `messages.jsonl`。
- 下一次 turn 如存在 unresolved interrupted/failed tail，构造 recovery projection，作为受控 `<interrupted_turn_context>` wrapper 注入当前 canonical user message；成功 committed 后该 wrapper 随新的 user message 写入 `messages.jsonl`。
- recovery context 字段按配置上限截断；raw delta、半截 tool_use、未确认大段 tool_result 不进入 canonical。
- session_search、compact、finalize、memory_review 继续只消费 canonical `messages.jsonl`。

验证：

- 单元测试：成功 turn 同时写 journal committed 与 canonical messages。
- 单元测试：带文本/媒体附件的成功 turn 的 `canonical_user_message` 哈希与 `messages.jsonl` 完整 user content 一致，且 journal 不重复保存附件正文；resume 不重复渲染该 turn，且用户气泡只显示原始 `@path` 输入。
- 单元测试：failed/cancelled/interrupted turn 只写 journal，不增加 `messages.jsonl`。
- 单元测试：continuation committed 后，`messages.jsonl` 只包含受控 recovery wrapper 和当前请求，不包含 raw journal。
- 单元测试：session_search index 仍从 `messages.jsonl` 派生，未读 `turn_events.jsonl`。
- 单元测试：memory_review transcript 仍只来自 committed transcript。

### 阶段 3：TUI 行为与恢复

Planning：

- running turn + 普通 Enter 继续排队下一 turn。
- running turn + Ctrl+Enter 执行 interrupt-and-steer；V1 只支持纯文本 steer。
- Ctrl+Enter 输入含附件时，不注入当前 turn；输入按普通 queued user input 处理，并在 TUI 提示用户附件不能打断注入。
- provider streaming 阶段可硬中断；tool 执行阶段软中断，必须等当前工具结束后在安全边界处理 steer。
- 工具执行期间多次 pending steer 按时间顺序合并成一个 pending steer block；普通 Enter 队列不参与合并。
- Ctrl+C 纯取消当前 turn 后，普通 queued input 不自动 dispatch，恢复到 composer 等用户再次确认。
- `@路径` 预检可异步完成，但每个 `QueuedInput` 必须持有自身的 `InputDraft`。按提交 sequence flush 后，输入历史只记录该输入自己的草稿，禁止用跨提交的“最后一次取走 composer 草稿”单槽推断归属。
- `/resume` 的 canonical 状态仍来自 `session.yaml` + `messages.jsonl`；TUI timeline 优先 replay `turn_events.jsonl`，缺失/损坏时 fallback 到 `messages.jsonl` 简化历史并提示。
- TUI 不直接渲染 `<interrupted_turn_context>` wrapper。

验证：

- 单元测试：Ctrl+Enter 文本 steer 记录为 steer，不进入普通 queue。
- 单元测试：Ctrl+Enter 含附件时进入 queue 并生成提示。
- 单元测试：pending steer 合并只发生在工具 pending steer 场景，普通 queued input 不合并。
- 单元测试：Ctrl+C 取消恢复 queued input 到 composer，不自动 dispatch。
- 单元测试：两个 `@路径` 输入 A、B 的预检完成顺序与提交顺序不同，取消后按 Up 依次恢复 B、A 各自的原始草稿，不混入对方文本、粘贴映射或附件占位。
- 单元测试：resume timeline 优先 journal；journal 损坏 fallback canonical。
- tmux TUI smoke：默认启动与 `/help` 正常。
- tmux TUI 场景：运行中排队输入后取消，确认 queued input 回到 composer 且不自动发送。

### 阶段 4：配置、文档与回归收口

Planning：

- 在现有 config 体系加入 turn journal / recovery 参数，默认值：
  - assistant delta snapshot interval：500ms。
  - assistant delta snapshot chars：1024。
  - original user request recovery chars：8192。
  - partial assistant recovery chars：8192。
  - tool input preview chars：2048。
  - tool output preview chars：4096。
  - user steer recovery chars：8192。
- 更新 `docs/config_parameters.md` 或现有配置文档，说明新参数。
- 确认不引入每 turn 文件，不实现 journal compaction，不改变 session_search SQLite `messages.message_index` 语义。

验证：

- 单元测试：配置默认值与非法值校验。
- `rg` 检查没有新代码路径让 session_search / memory_review 读取 `turn_events.jsonl`。
- `cargo fmt` 后检查 diff。

### 阶段 5：整体验证与 Code Review

整体验证：

- `cargo clippy -- -D warnings`
- `cargo test`
- `cargo check`
- TUI smoke test：使用 tmux 运行默认 smoke。
- TUI 专项 smoke test：覆盖 queue + cancel + composer restore；如果 Ctrl+Enter 难以稳定模拟，至少用单元测试覆盖精确状态机。

code-review skill：

- 使用 code-review skill 检查 journal/storage、engine/recovery、TUI、config/docs/tests。

完成标准：

- PRD 中 V1 目标全部实现，非目标没有越界实现。
- `messages.jsonl` canonical 语义保持不变。
- `turn_events.jsonl` 能恢复中断/失败/取消 turn 的 TUI timeline。
- recovery projection 只在后续 committed turn 中以受控 wrapper 进入 canonical。
- session_search / compact / finalize / memory_review 不直接消费 journal。
- 所有验证通过，code-review skill 无未处理的高风险问题。

---

## 开工前拍板记录

当前已确认：

- recovery context 字段上限使用配置项，默认值采用本文推荐值。
- assistant delta snapshot 的时间/字符阈值使用配置项，默认值采用约 500ms / 1KB。
- assistant delta snapshot 主要服务 TUI timeline / crash resume；journal 保留可重建的完整 partial assistant text，LLM 只消费有上限的 recovery projection。
- Ctrl+Enter 的 interrupt-and-steer V1 只支持纯文本；附件类输入只能排队，并由 TUI 提示。
- 工具执行期间如果出现多次 pending steer，按时间顺序合并成一个 pending steer block。
- Ctrl+C 取消当前 turn 后，普通 queued input 不自动 dispatch，而是恢复到 composer 等用户再次确认。
- journal 缺失或损坏时不阻塞 resume，降级为 `messages.jsonl` 简化渲染并提示用户。
