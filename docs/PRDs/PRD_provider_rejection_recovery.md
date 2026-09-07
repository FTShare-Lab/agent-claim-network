# Provider 确定性拒绝恢复

> 状态：实现中（2026-09-05）。本文补齐 `fix/session-compaction-recovery` 分支中 provider 确定性拒绝分类、Provider WAL 回滚与失败 turn 恢复的已拍板语义。失败 turn 的手动 / 自动压缩语义见 [PRD_compact_in_turn.md](PRD_compact_in_turn.md)；异常流（损坏 SSE、缺终态）恢复见 [PRD_provider_stream_recovery.md](PRD_provider_stream_recovery.md)，本文不重复。

## 1. 背景

一个 turn 的每次 provider request 在发送前先写入 Provider WAL（`provider_history.json`）。此前任何失败都保留 WAL，并由 turn journal 在下一 turn 前置注入失败 turn 的上下文。这在两类失败上是错的：

- 上下文窗口溢出、请求过大、媒体被拒、非法 tool schema 等**确定性请求错误**，重放同一份内容只会再次失败；保留 WAL 让下一 turn 继续撞同一堵墙。
- 一次网络级失败后 adapter 会重发同一请求；此时无法判断上游是否已接受第一次发送。若在这种"发送结果不明确"的状态下改写 WAL，可能丢掉上游已计费、已产出的响应。

历史 TUI 兜底分支显示通用的编辑 prompt 建议，无法适用于所有失败；历史 `/compact` 也只能压缩 `messages.jsonl`。当前已修复失败窗口压缩，并移除兜底分支的通用操作建议。

## 2. 目标

1. 三种主对话 provider（`anthropic`、`openai_chat`、`openai_responses`，含 Responses WebSocket）对确定性请求错误给出统一的 provider-neutral 分类。
2. 确定性拒绝后按本 turn 是否已有已接受响应决定回滚粒度：没有则丢弃整个 turn，有则回滚到最后一次已接受边界。
3. 回滚在崩溃窗口内可恢复，且恢复完成后不留下会影响后续写入的残留状态。
4. 上下文窗口拒绝自动触发一次压缩并重发；重试上限内仍失败才交给用户。
5. 发送结果不明确时保守保留 WAL，但不因此关闭本可安全进行的恢复。
6. 展示上游错误原因，不主动附加请求正文或鉴权头；展示内容不扩大错误恢复分类范围。

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

媒体剥离分类收窄（2026-09-07，已实现）：

- 仅结构化 `code` / `type` 为 `invalid_image`、`invalid_image_url`、`image_too_large`、`unsupported_image` 时识别为媒体拒绝；不从上游 `message` 或请求内容推断。
- `unsupported_media_type` 保留为普通确定性请求错误，即使 message 提到图片 / PDF，也不升级为附件剥离。该错误码可能表示 HTTP Content-Type 不受支持。
- 三种 adapter 的错误归一化只接受明确媒体错误码，不将正文中的图片 / PDF 描述转换为媒体错误码。仅有通用错误码的媒体故障走既有普通拒绝恢复，历史附件保持原样；这类故障不再自动剥离重试。
- HTTP 413、已支持的 WebSocket 1009 尺寸判断，以及上下文超限等其他分类不变。不新增配置、持久化字段或恢复状态机。

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

### 4.5 错误原因展示（2026-09-07 拍板）

三种协议优先展示结构化错误的 `message`，没有非空消息时展示错误正文，正文兜底按 UTF-8 边界截断至 1000 字节并附加省略号；提取出的 message 不做该截断。保留状态码、既有分类白名单和特殊错误提示。错误归一化仍保留分类 code/type，恢复逻辑只消费原有分类结果，不能因为正文恢复展示而扩大附件剥离、重试或 compact 的触发范围。

不根据字符串特征猜测敏感内容，不主动附加请求正文或鉴权头，不另存完整原始错误体。展示后的错误沿用现有日志与 session 事件链路，可能落盘；用户接受上游 message 或非 JSON 错误正文可能回显输入片段、URL 等信息的边界。此决定替代历史的 message 整段丢弃策略；损坏响应帧及应用自行附加的私有 payload 仍不得写入错误日志。

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

## 8. 收口与长期整理

1. **TUI 兜底提示（已完成）**：移除 `Edit the prompt, retry, or /exit to finalize`，保留既有 Attention 标题、虚线框和启动失败提示。不增加失败分类状态；删除失败 turn / 丢弃 WAL 的新命令暂缓。
2. **错误原因展示（已完成）**：按第 4.5 节展示上游消息，取消 message 整段丢弃。无需新增正文敏感性白名单或原始体 debug 日志。
3. **sidecar 与 journal（非阻塞长期整理）**：两者记录有重叠，sidecar 还保存崩溃恢复所需的回滚快照。保留当前实现；未来可独立评估把快照并入 journal，不纳入本次改动。
4. **分类白名单集中维护（2026-09-07，已实现）**：展示白名单、明确请求错误与 transient 分类集中在 `src/api/mod.rs`，各协议只保留转调入口；媒体判断同样使用公共函数。保留既有协议差异：Messages 不新增四个兼容 transient 别名，Chat Completions 不新增上下文文本匹配，WebSocket 大消息错误码只在 Responses 展示。此整理不改变重试、剥离或脱敏语义，后续新增错误码统一在公共模块维护。
