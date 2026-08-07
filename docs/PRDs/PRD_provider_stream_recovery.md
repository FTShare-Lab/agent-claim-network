# Provider 异常流恢复

> 状态：已完成（2026-08-07）。本文定义 `openai_chat`、`openai_responses` 与 `anthropic` 在 HTTP SSE 损坏、未完整结束及完整但无可消费输出时的统一恢复语义。已经拍板的产品语义不得在实现过程中静默修改。

## 1. 背景

ACN 的三种主对话 provider 都先请求 HTTP SSE streaming；streaming 失败后，provider-neutral turn loop 可以切换到 non-streaming，最多尝试 5 次。当前恢复门槛只观察是否已经输出可见 assistant text，导致三条协议行为不一致：

- Chat 的非法 UTF-8、损坏 SSE JSON、缺 `finish_reason` 等结构错误不参加内部 streaming retry；零可见输出时，外层也不会进入 non-streaming fallback。
- Responses 的损坏帧、缺终态、未完成 output item 同样在零可见输出时直接失败。
- Anthropic 会在没有可见 text delta 时内部重试结构错误，但重试耗尽后仍不会进入 non-streaming fallback。
- Chat 的完整 `finish_reason = stop` 响应如果只有被 adapter 丢弃的 reasoning 字段，可能被投影为空 assistant 并当作成功提交；Responses 与 Anthropic 已经拒绝没有 text/tool 的完整响应。

异常流内累积的 tool call、Responses output item 和 Anthropic thinking/tool block 都不会在 provider response 完整返回并通过校验前执行或落盘，因此可以安全放弃当前 attempt 并重放同一请求。

## 2. 目标

1. 为三种 provider 建立统一、明确的“流损坏或未完整结束”错误边界。
2. 零可见输出的异常流先按现有 `retry_count` 重试 streaming，耗尽后进入现有 non-streaming fallback。
3. 已输出可见文本的异常流不重复 streaming，直接进入 non-streaming fallback。
4. 完整但没有可消费 text/tool 的响应不提交空 assistant，而是进入 non-streaming fallback。
5. 失败 attempt 的 partial text、reasoning、tool draft 与 provider replay 全部作废；只允许最终成功 attempt 进入 canonical session 与私有 replay。
6. 工具只执行最终成功 attempt 的完整调用，而且只执行一次。
7. 保持确定性 provider 终态、max-token continuation、上下文窗口恢复、cancel/steer 的现有语义。

## 3. 非目标

- 不实现 OpenAI Chat Reasoning 保存或回传；Chat 仍然明确不支持 Reasoning replay。
- 不放松三种协议对完整终态、block 生命周期、tool arguments 和 replay 的校验。
- 不把所有 `OutputShape`、所有 JSON 错误或 non-streaming 格式错误都标记为异常流。
- 不修改 WebSocket、TUI 布局、timeline cell 或 Reasoning 展示。
- 不增加 TOML 参数，不改变 `retry_count` 和 non-streaming fallback 最多 5 次的现有配置语义。
- 不保存原始损坏 SSE frame、请求体、Reasoning 或附件内容到错误日志。

## 4. 已拍板语义

### 4.1 适用范围

- `openai_chat`、`openai_responses`、`anthropic` 一起修复并遵循同一 provider-neutral 恢复契约。
- adapter 可以保留各协议自己的 reducer 和结构校验，但必须把可恢复的 streaming 失败映射为统一错误类型，供 turn loop 判断。

### 4.2 可恢复的异常流

仅下列发生在 streaming 解码、读取或完成校验阶段的问题属于可恢复异常流：

- streaming response body 读取中断；
- SSE frame 非法 UTF-8；
- SSE data JSON 损坏；
- EOF 前缺少协议要求的完整终态；
- Chat 缺 `finish_reason`；
- Responses 缺 terminal event、output item 未完成或 streaming item 生命周期损坏；
- Anthropic 缺 `message_stop`，或 text/tool/thinking block 未完整结束、类型错配或结构损坏。

完整协议终态是吸收边界：Chat 在 `finish_reason` 后只接受不含 choice 的 usage 元数据与 `[DONE]`，Responses 在 terminal event 后只接受 `[DONE]`。终态后出现新的 text/tool item 或 delta 说明该 streaming attempt 的生命周期损坏，必须整轮作废并按异常流恢复，不能执行尾随工具。

下列不因本 PRD 自动变成异常流：

- non-streaming JSON、字段或输出结构错误；
- HTTP 鉴权、确定性 4xx 与多媒体拒收；
- Responses `failed`、非 `max_output_tokens` 的 `incomplete`；
- Anthropic `refusal`、`pause_turn` 与未知 stop reason；
- provider 返回的完整但不受支持的协议语义。
- Anthropic 合法的 SSE `error` event；它是显式 provider 错误，不伪装成损坏流。

### 4.3 恢复顺序

- 尚未输出可见 assistant text：先按 provider 已配置的 `retry_count` 和退避策略重试 streaming；耗尽后进入 non-streaming fallback。
- 已经输出可见 assistant text：不再重试 streaming，直接进入 non-streaming fallback。
- non-streaming fallback 沿用现有最多 5 次和退避策略，不新增配置。
- fallback 成功后，以完整结果替换失败 streaming attempt 的 TUI partial；tool-only 成功响应可以清空 partial 后进入工具循环。
- 全部 fallback 失败后，当前 turn 返回错误，不提交 canonical assistant 或 provider replay。

### 4.4 完整但无可消费输出

- 三种 provider 的完整响应如果既没有非空 assistant text，也没有完整 tool call，则不得提交空 assistant。
- 该情况使用独立的“无可消费输出”错误分类，不伪装成 SSE 损坏。
- 因为没有可见输出、工具副作用或 session commit，它直接进入 non-streaming fallback，最多 5 次；不重新请求同一 streaming 路径。
- fallback 仍然无可消费输出时继续当前 fallback 尝试；5 次均失败后明确报错且不提交。
- Chat reasoning-only、Responses reasoning-only、Anthropic thinking-only 都遵循本规则；失败 attempt 的 reasoning/thinking 不保存、不回传。
- Chat `tool_calls`/`function_call`、Anthropic `tool_use` 等工具终态如果没有实际完整工具块，也属于无可消费输出；`max_tokens` 的空 partial 继续走原有 continuation/结构错误边界。
- 显式拒绝或失败终态优先于“无可消费输出”，不得借此触发 fallback。

### 4.5 Partial、工具与持久化

- 每次 provider attempt 是独立事实。失败 streaming attempt 的 text accumulator、reasoning/thinking、tool draft、raw output item 和 replay 全部丢弃，不与 fallback 结果拼接。
- streaming delta 可以作为当前运行中 TUI 的临时展示；只有成功 fallback 的完整文本会通过既有 replacement event 替换它。
- 未完整或已放弃 attempt 的 tool call 永不执行，也不生成 tool result。
- 只有最终成功 attempt 的工具调用进入工具执行，而且每个调用只执行一次。
- 最终失败不向 `messages.jsonl` 写入 canonical assistant 或 provider replay；resume 只恢复此前 committed 历史。诊断性 `turn_events.jsonl` 仍可记录既有失败/fallback 事件，但不能包含原始私有 payload。

### 4.6 不进入 fallback 的路径

以下继续遵循已有独立状态机：

- 用户 cancel/steer：立即中断，不 fallback；
- `max_tokens` / `max_output_tokens`：走既有 continuation；
- `model_context_window_exceeded`：走既有强制 compact recovery；
- Responses `failed`、非 token-limit incomplete：确定性失败；
- Anthropic `refusal`、`pause_turn`、未知 stop reason：确定性失败；
- 鉴权失败、确定性 4xx、多媒体拒收：按现有错误语义返回。

## 5. 设计约束

### 5.1 Provider-neutral 错误

- 新增只供 adapter 与 turn loop 共享的 streaming failure 标记；它只表达“本次 streaming attempt 不完整或损坏，可以换路径安全重放”。
- 新增独立的 no-consumable-output 标记；它表达“provider 正常结束，但 ACN 没有可提交的 text/tool”。
- `ProviderTerminalFailure` 继续拥有最高优先级，不能因已经输出 text 或命中其他形状错误而 fallback。
- `SessionTurnInterrupted` 继续禁止 retry/fallback。

### 5.2 Adapter 内部 retry

- 三个 adapter 的 streaming client 都只在没有 replay-blocking visible text event 时重试异常流。
- 已有 HTTP 429/5xx 和 retryable transport 错误继续沿用现有内部 retry。
- Chat/Responses 不能通过把全部 `ResponseJson` 或 `OutputShape` 改成 retryable 来实现本需求；streaming parser 必须产生可区分的错误。
- Anthropic 的 streaming retry 应从当前宽泛的 `ResponseJson`/`OutputShape` 判断收紧到 streaming failure 与原有 HTTP retryable 错误；既有 non-streaming/结构化 JSON 调用的重试策略保持不变，避免本期顺带改变其他调用链。
- streaming retry 耗尽时，adapter 必须保留统一 streaming failure 类型到 turn loop，不能退化成普通 anyhow 文本。

### 5.3 完整响应校验

- Chat adapter 补齐非空 text 或完整 tool call 校验，消除空成功。
- Responses 与 Anthropic 复用现有 no-consumable 检查，但把错误映射到统一 no-consumable-output 标记。
- `ProviderStop::Done` 不再单独足以证明响应可提交；完成响应必须同时有可消费输出。
- context-window partial 的空 canonical content 是已有恢复状态，不属于 no-consumable-output，不能破坏其 replay 与强制 compact continuation。
- streaming reducer 必须把 `finish_reason`、Responses terminal event 与 Anthropic `message_stop` 作为吸收边界；终态后的内容或工具事件不得进入最终 response。

## 6. 分阶段实施

### 阶段 0：基线与 PRD

- 核对三种 client、streaming reducer、adapter 和 provider-neutral fallback 的实际路径。
- 固化本 PRD 及已拍板语义。

验收：PRD 与当前实现证据一致，没有把已存在的 terminal/continuation 状态误归为异常流。

### 阶段 1：共享错误契约

- 增加 provider-neutral streaming failure 与 no-consumable-output 类型。
- 调整 turn loop fallback 门槛和错误优先级。
- 补充零文本异常流、空输出、terminal failure、cancel 的 provider-neutral 测试。

验收：只有明确分类的异常流或空输出能在零可见文本时进入 fallback；普通错误仍直接返回。

### 阶段 2：Chat

- streaming decoder 区分流错误和普通响应错误。
- 异常流参加现有内部 retry，并在耗尽后保留共享标记。
- 完整空响应映射为 no-consumable-output。
- 覆盖非法 UTF-8、损坏 JSON、缺 finish reason、未完成 tool draft、reasoning-only/空完成响应。

验收：不再提交空 assistant；异常 tool draft 不执行；streaming retry 与 fallback 顺序符合 4.3。

### 阶段 3：Responses

- streaming decoder 区分流错误，内部 retry 耗尽后保留共享标记。
- 现有 no-consumable 检查映射到共享标记。
- 保持 `failed`、incomplete、output item done、reasoning replay 与 max-output continuation 语义。

验收：损坏或缺终态流可恢复；确定性终态不 fallback；失败 reasoning/output item 不落盘。

### 阶段 4：Anthropic

- streaming reducer 的损坏/未完整结束错误映射为共享标记。
- 收紧当前宽泛的 retryable 判断。
- thinking-only/空完成响应映射到共享 no-consumable 标记。
- 保持 context-window、max-token、thinking replay、工具块严格校验与确定性 stop reason。

验收：内部 retry 耗尽后可以进入 non-streaming fallback；损坏 tool/thinking block 不执行、不保存；context recovery 不被改写。

### 阶段 5：定向与完整验证

- 使用本地 fake server 对三种协议分别验证：非法 UTF-8、损坏 JSON、缺终态、零文本 tool/reasoning partial、可见文本后损坏、空完整响应、fallback 成功、fallback 耗尽、显式 terminal failure。
- 断言失败 attempt 不提交 replay、不执行工具，成功 fallback 的工具仅执行一次。
- 执行 `cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check`。本期不修改版本号，不运行版本一致性脚本。
- 使用真实 LLM TUI 对三种 provider 的正常 text、工具回环和连续会话做回归；真实 endpoint 无法稳定制造损坏 SSE，因此异常分支以 fake server 的确定性测试为验收依据。

验收：所有命令通过，真实正常流无新增 fallback，stderr 无新增协议错误。

### 阶段 6：Code review 闭环

- 按 `code-review` skill 做本地针对性审查和一次独立只读 review。
- 修复真实可触发且值得修复的 P0/P1，不扩大到极端假设或无关重构。
- 修复后重新执行受影响测试与完整验证。
- 再做全量 diff review；没有 P0/P1 后结束。

验收：review 没有未解决的 P0/P1，所有修复没有改变第 4 节已拍板语义。

## 7. 最终验收矩阵

| 场景 | Chat | Responses | Anthropic | 预期 |
|---|---:|---:|---:|---|
| 零文本时非法 UTF-8 / 损坏 JSON | ✓ | ✓ | ✓ | streaming retry 后进入 non-streaming fallback |
| 缺协议终态 | ✓ | ✓ | ✓ | 不提交 partial，按异常流恢复 |
| 可见文本后流损坏 | ✓ | ✓ | ✓ | 直接 fallback，成功结果替换 partial |
| tool-only 流中断 | ✓ | ✓ | ✓ | 草稿不执行，成功 attempt 的工具只执行一次 |
| reasoning/thinking-only 流中断 | ✓ | ✓ | ✓ | partial 私有状态不落盘，安全恢复 |
| 完整但无 text/tool | ✓ | ✓ | ✓ | 直接 non-streaming fallback；最终失败不提交 |
| 显式拒绝/失败终态 | 按既有 Chat 终态 | ✓ | ✓ | 不 fallback |
| max-token / context-window | ✓ | ✓ | ✓ | 沿用 continuation / compact recovery |
| cancel/steer | ✓ | ✓ | ✓ | 不 retry、不 fallback、不提交 |
| resume | ✓ | ✓ | ✓ | 只包含最终 committed 历史，无失败 attempt replay |

## 8. 完成定义

只有同时满足以下条件，本文状态才能改为“已完成”：

1. 阶段 1–4 的实现和定向测试全部完成。
2. 第 7 节矩阵由自动化测试或明确说明的真实 TUI 回归覆盖。
3. Rust 完整验证通过。
4. 针对性与全量 code review 均无未解决 P0/P1。
5. 最终逐项对照本 PRD，确认没有遗漏、冲突或静默改变拍板语义。

## 9. 实施与验收记录

### 9.1 实现结果

- provider-neutral turn loop 已使用独立的 streaming-failure 与 no-consumable-output 分类；普通零文本错误、确定性终态和用户中断不会借此进入 fallback。
- Chat、Responses 与 Anthropic streaming decoder 已把读取中断、非法 UTF-8、损坏 JSON、缺终态和 block/item 生命周期损坏映射到 streaming failure；没有可见输出时先走各 client 的既有 streaming retry，耗尽后由 turn loop 进入 non-streaming fallback。
- provider-neutral streaming 总 deadline 超时也会保留 streaming-failure 分类；首个 delta 前超时可以进入 non-streaming fallback，non-streaming timeout 仍是当前 fallback attempt 的普通失败。
- 完整但没有非空 text 或完整工具调用的响应已统一进入 no-consumable fallback；工具终态缺少实际工具块也遵循该规则，max-token/context-window 边界保持不变。
- Chat `finish_reason`、Responses terminal event 与 Anthropic `message_stop` 已作为 streaming 生命周期的吸收边界；终态后的内容或工具事件会使当前 attempt 整体失败，不能进入工具执行层。
- Chat `content_filter` / 未知 finish reason 与 Anthropic 合法 `error` event 会映射为确定性 provider 失败；即使此前已有可见 delta，也不会触发 non-streaming fallback。
- 失败 attempt 的 text、reasoning/thinking、tool draft 与 replay 不进入最终 `ProviderResponse`；成功 fallback 沿用既有 replacement event，最终成功工具只执行一次。

### 9.2 自动化验证

- `cargo fmt --check`：通过。
- `cargo clippy -- -D warnings`：通过。
- `cargo test`：通过；lib 2104 项、`acn` 53 项、Maintainer 2 项、Router 2 项、CLI integration 1 项、storage integration 5 项，全部 0 失败；doc tests 通过。
- `cargo check`：通过。
- 本期没有修改版本号，按拍板不运行版本一致性脚本。
- 第 7 节异常矩阵由 decoder/reducer 单元测试、client fake-server retry 测试、adapter 分类测试与 provider-neutral turn-loop/fallback/工具副作用测试组合覆盖；Responses 另有从损坏 SSE 到 fallback、replay 丢弃和工具不执行的端到端 fake-server 测试。

### 9.3 真实 LLM TUI

| Provider | Session | 结果 |
|---|---|---|
| `openai_chat` | `session_b2b80ff4` | 2 个 committed turn、5 个 streaming delta、fallback 0、`file_read` 启动/完成各 1 次、stderr 为空 |
| `openai_responses` | `session_3e4937ce` | 2 个 committed turn、6 个 streaming delta、fallback 0、`file_read` 启动/完成各 1 次、stderr 为空 |
| `anthropic` | `session_c2402203` | 2 个 committed turn、5 个 streaming delta、fallback 0、`file_read` 启动/完成各 1 次、stderr 为空 |
| `openai_responses` 异常流实测 | `session_6f6c202a` | 上游 SSE 缺 terminal event，3 次 non-streaming fallback 均成功；2 个 turn 全部 committed、工具只执行 1 次、失败 turn 0、stderr 为空 |

真实 endpoint 不用于伪造非法 UTF-8 或损坏 JSON；这些确定性异常由本地 fake server 验收。TUI 会话验证的是正常 streaming、连续历史、工具结果回环，以及真实缺终态 SSE 的恢复。

### 9.4 Review 闭环

- 本地针对性审查发现 Anthropic 合法 SSE `error` event 被误归为损坏流，已改为普通显式 provider 错误并补测试。
- 一次可用的独立只读 review 未发现 P0，发现 2 个 P1：空工具终态漏过 no-consumable fallback、Chat/Responses 终态后仍可接收工具事件。两项均已按第 4 节语义修复并补回归测试。
- 后续复审又确认 1 个 P1：工具终态已有开场文本、但没有实际工具块时仍会硬失败。Chat 与 Anthropic 已改为按 canonical ToolUse 是否真实存在判断，并补充 text-only tool terminal 回归测试。
- 提交前复审又确认 2 个 P1：首个 delta 前触发 provider-neutral 总 deadline 时丢失 streaming-failure 分类，以及 Chat / Anthropic 的显式失败终态在已有可见文本后仍可能 fallback。两项均已收紧错误映射并补回归测试。
- 上述修复后重新执行定向测试与完整 Rust 验证；新的独立只读复审和最终全量 diff review 均没有未解决 P0/P1，也没有发现第 4 节语义被修改。

### 9.5 最终 PRD 对照

- 阶段 1–4 的共享契约和三 adapter 实现完成。
- 阶段 5 的异常、fallback、持久化、工具副作用、完整 Rust 验证与真实 TUI 验收完成。
- 阶段 6 的本地审查、独立只读审查、P1 修复、修复后复验和最终全量 diff review 完成。
- 第 7 节各场景均有组合自动化证据或真实 TUI 证据；没有依赖真实 endpoint 制造协议损坏。
- 没有新增 TOML 参数，没有修改 TUI 布局，没有改变 terminal、max-token、context-window、cancel/steer 与 Reasoning replay 的既有语义。
