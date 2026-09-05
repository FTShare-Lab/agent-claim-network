# Provider 确定性拒绝恢复

> 状态：实现中（2026-09-05）。本文补齐 `fix/session-compaction-recovery` 分支中 provider 确定性拒绝分类、Provider WAL 回滚与失败 turn 恢复的已拍板语义。失败 turn 的手动 / 自动压缩语义见 [PRD_compact_in_turn.md](PRD_compact_in_turn.md)；异常流（损坏 SSE、缺终态）恢复见 [PRD_provider_stream_recovery.md](PRD_provider_stream_recovery.md)，本文不重复。

## 1. 背景

一个 turn 的每次 provider request 在发送前先写入 Provider WAL（`provider_history.json`）。此前任何失败都保留 WAL，并由 turn journal 在下一 turn 前置注入失败 turn 的上下文。这在两类失败上是错的：

- 上下文窗口溢出、请求过大、媒体被拒、非法 tool schema 等**确定性请求错误**，重放同一份内容只会再次失败；保留 WAL 让下一 turn 继续撞同一堵墙。
- 一次网络级失败后 adapter 会重发同一请求；此时无法判断上游是否已接受第一次发送。若在这种"发送结果不明确"的状态下改写 WAL，可能丢掉上游已计费、已产出的响应。

TUI 侧的全局提示 `Edit the prompt, retry, or /exit to finalize` 对所有失败一视同仁，用户拿不到"该做什么"的信息；`/compact` 也只能压缩 `messages.jsonl`。

## 2. 目标

1. 三种主对话 provider（`anthropic`、`openai_chat`、`openai_responses`，含 Responses WebSocket）对确定性请求错误给出统一的 provider-neutral 分类。
2. 确定性拒绝后按本 turn 是否已有已接受响应决定回滚粒度：没有则丢弃整个 turn，有则回滚到最后一次已接受边界。
3. 回滚在崩溃窗口内可恢复，且恢复完成后不留下会影响后续写入的残留状态。
4. 上下文窗口拒绝自动触发一次压缩并重发；重试上限内仍失败才交给用户。
5. 发送结果不明确时保守保留 WAL，但不因此关闭本可安全进行的恢复。
6. 错误体脱敏不回显请求内容。

## 3. 非目标

- 不修改异常流 fallback 的重试次数、退避与分类。
- 不新增 TOML 配置项。
- 不在本 PRD 内处理 TUI 失败阶段提示与"删除失败 turn / 丢弃 WAL"命令入口（见第 8 节遗留）。
- 不把上游原始错误体写入日志（沿用 PRD_provider_stream_recovery 第 3 节的拍板）。

## 4. 已拍板语义

### 4.1 错误分类

adapter 在 HTTP 状态、流式 `error` 事件、非流式响应和 WebSocket close frame 四个入口把上游错误映射为下列 provider-neutral 类型：

| 类型 | 含义 | turn loop 处理 |
| --- | --- | --- |
| `ProviderContextWindowExceeded` | 请求超过模型上下文窗口 | 回滚 WAL，压缩后重发，最多 `MAX_CONTEXT_WINDOW_RECOVERIES = 2` 次 |
| `ProviderRequestTooLarge` | HTTP 413 或 WebSocket 1009 尺寸错误 | 回滚 WAL；若请求含图片 / PDF 则剥离后重发一次 |
| `ProviderMediaRejected` | 明确指向图片 / PDF 的请求错误 | 同上 |
| `ProviderRequestRejected` | 其他确定性请求错误（含内容策略拒绝） | 回滚 WAL，turn 以 `RejectedByProvider` 结束 |
| `ProviderTerminalFailure` | 确定性但不改写 WAL 的失败 | 保留 WAL，turn 以 `Failed` 结束 |

分类规则：结构化 error code / type 优先于 HTTP status；已知的非请求错误 code（限流、过载、鉴权等）不会被当作请求错误；未知 code 一律按非请求错误处理（保守保留 WAL）；只有完全没有 code 时才回落到 `400 | 415 | 422` 判定。每种类型都带 `after_visible_output` 变体，用于通知 TUI 丢弃已展示但未被接受的流式输出。

### 4.2 回滚粒度

- **本 turn 尚无已接受响应** → `DiscardTurn`：恢复到 turn 开始前的 compaction 状态，turn journal 记 `RejectedByProvider`，该 turn 不进入 `recovery_turn_chain`，下一 turn 不再前置注入它。
- **本 turn 已有已接受响应**（工具循环中的中间响应、`max_tokens` 续写前的中间响应）→ `PreserveTurnProgress`：恢复到最后一次已接受响应之后、本次失败请求写入之前的 WAL 快照，turn 以 `Failed` 结束并进入既有失败 turn 恢复链。

"已接受"指 turn loop 已把响应写入 journal 并确认可消费。内部续写前，已接受的响应独立形成恢复快照，不包含下一次续写的触发消息。

### 4.3 崩溃窗口

回滚分三步：写 sidecar 记录 `provider_rejection_recovery.json`（含回滚目标快照）→ journal 追加 `ProviderRequestRejected { rejection_id, discard_turn }` → 应用回滚。

- 每个 turn 开始前和手动 `/compact` 生成新摘要前，先调用 `recover_provider_rejection`：
  - 记录对应的 turn 已在 journal 写下 `TurnFinished` → 回滚早已应用，直接删除记录，不重放。
  - journal 中已有该 rejection 之后的 `ProviderRequestRetriedAfterRejection` → 重试已成功推进 WAL，删除记录。
  - 其余情况视为崩溃窗口，补写缺失的 journal 事件并重放回滚，然后删除记录。
- 压缩重试或媒体清理重试成功推进到下一代请求后，立即删除记录。

sidecar 只在崩溃窗口内是事实源；一旦 turn 终态落盘，journal 与 `provider_history.json` 是唯一事实源。

### 4.4 发送歧义

adapter 每次物理发送都上报 `provider_request_started_after(messages, previous_attempt_ambiguous)`。上一次发送没有拿到明确终态（网络错误、超时）就再发时，turn 级标志 `ambiguous_provider_send_seen` 置真；收到并接受任何完整响应后复位。

标志为真时，会**改写 WAL 内容**的拒绝（媒体剥离、尺寸、普通请求拒绝）降级为 `ProviderTerminalFailure` 并保留 WAL，由用户决定是否 `/compact` 或 `/new`。

**例外：上下文窗口拒绝不受该标志影响。** 该拒绝只取决于请求内容，结果不明确的那次发送携带同一份内容，上游对它的裁决必然相同，不存在"上游已接受并产出"的可能，因此仍按 4.2 回滚并进入压缩重试。判定集中在 `rejection_would_mutate_request_wal`，三处调用点共用。

### 4.5 错误体脱敏

上游 4xx 的 `message` 可能回显请求内容（字段路径、被拒的输入值、非 JSON 文本）。用户可见的错误体统一替换为 `{"error":{"message":"[redacted ... payload]","type|code":"<白名单内的分类 code，或 redacted>"}}`，分类在脱敏前完成，脱敏后仍可被 `is_*` 判别器识别。

原始 message 不写入日志。这条与"用户看不到根因"直接冲突，属于待拍板项（第 8 节）。

## 5. 设计约束

- 不新增平行恢复入口：所有恢复挂在既有 `start_turn_journal` 与 `compact_session_checkpoint_with_events` 上。
- `PreflightCompactor` 持有的四个 compaction 快照（`before_turn` / `before_started_request` / `before_pending_request` / `before_clean_retry`）在构造时全部初始化，代码中不得再对它们做 `is_none()` 兜底。
- 分类判别器（`is_context_window_error_body`、`is_content_policy_error_body`、`is_provider_non_request_error_code` 等）位于 `src/api/mod.rs`，adapter 只引用不复制。

## 6. 验收矩阵

| 场景 | 期望 | 覆盖 |
| --- | --- | --- |
| 首次请求即被上下文窗口拒绝 | 丢弃 turn WAL，压缩后重发成功，journal 记 rejected + retried | `rejected_context_window_request_is_discarded_before_compaction_retry` |
| 网络级失败重发后收到上下文窗口拒绝 | 不被歧义标志冻结，仍压缩重发成功；已展示的 partial 被丢弃 | `ambiguous_send_does_not_block_context_window_recovery` |
| 网络级失败重发后收到 413 | 降级为 TerminalFailure，保留 WAL，不做媒体剥离 | `ambiguous_fallback_request_too_large_does_not_replace_request_history` |
| 续写请求被拒，之前已有已接受响应 | 三种 adapter 都保留已接受正文，turn 记 Failed，下一 turn 能重建 | `rejected_fallback_continuation_preserves_accepted_output_across_adapters` |
| sidecar 已写、journal 已写、回滚未应用时崩溃 | 下一 turn 重放回滚并删记录 | `journaled_rejection_recovers_wal_rollback_after_crash_window` |
| 拒绝 turn 已终态，用户随后 `/compact` | 下一 turn 开始时只删记录，不覆盖新 compaction 状态 | `finished_rejected_turn_clears_stale_recovery_record_without_rollback` |
| 手动 `/compact` 前存在残留记录 | 先完成恢复再规划摘要 | `manual_compact_*` 系列（PRD_compact_in_turn） |

## 7. 完成定义

- `cargo fmt --check`、`cargo clippy --all-targets`、`cargo test --lib` 通过。
- 第 6 节矩阵全部有对应测试且通过。
- `docs/core_behavior.md` 与本文第 4 节一致。

## 8. 遗留与待拍板

1. **TUI 失败阶段提示**：`chat_widget.rs` 对任意 `SessionRuntimeStatus::Error` 显示同一句 `Edit the prompt, retry, or /exit to finalize`。上下文窗口耗尽、内容策略拒绝、413 三种情况下正确动作分别是 `/compact`、改内容、缩短输入。需要 `SessionTuiState` 携带最后一次失败的分类后才能区分，同时缺少"删除失败 turn / 丢弃 WAL"的命令入口。属于新能力，需负责人拍板承载点后再做。
2. **错误体 message 全部丢弃**：现有测试明确锁定"即使 message 看起来无害也不保留"，PRD_provider_stream_recovery 又禁止把原始体写日志。若要恢复可行动性，可选方案是保留不含请求回显特征的白名单 message，或在 debug 级日志保留原始体；两者都推翻既有拍板，需负责人决定。
3. **sidecar 是第三个持久事实源**：与 journal 的 `ProviderRequestRejected` 事件重复记录"拒绝已发生"，回滚快照只在 sidecar 内。第 4.3 节已把它限定为崩溃窗口保护；长期可考虑把快照并入 journal 事件以消除该文件。
4. **分类白名单重复维护**：`safe_anthropic_error_type` / `safe_chat_error_code` / `safe_responses_error_code` 三份人工白名单与 transient code 列表共四份拷贝；`anthropic` 侧还反向依赖 `responses::is_media_rejection_error_code`。应上收至 `src/api/mod.rs`。
