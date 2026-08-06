# OpenAI-compatible Responses API 支持

> 状态：已完成（2026-08-04，15A 于 2026-08-06 修订）；阶段 0–7 与整体验收全部通过。本文定义 HTTP JSON/SSE Responses 协议、provider replay、session 恢复、多媒体历史与验收边界；WebSocket 和统一 Reasoning 展示不在本期范围内。

## 1. 背景

ACN 当前主对话 provider 支持 `openai_compatible_chat` 与 `anthropic`。两种 adapter 都接入统一的 provider-neutral turn loop：首次请求使用 streaming；若已经向 TUI 发出部分 assistant 文本后流式失败，则使用同一份 provider request 进入既有非流式 fallback；只有完整响应通过校验后才会执行工具和提交 canonical session。

OpenAI Responses API 与 Chat Completions 不是同一套响应字段的简单改名。Responses 使用有类型的 input/output item，包括：

- `message`；
- `reasoning`；
- `function_call`；
- `function_call_output`；
- 未来可能增加的其他 item。

在 `store = false` 的本地状态模式下，下一次请求需要携带仍在有效上下文中的历史 input，以及上一次响应返回的完整可回放 output items。若只把 `output_text` 和 tool call 投影成 ACN 现有 canonical content，会丢失 reasoning、phase 和未来未知 item，导致工具回环、resume 或后续 Reasoning 展示无法保持协议连续性。

本需求新增独立的 `openai_compatible_responses` adapter，在不改变现有 Chat/Anthropic 行为的前提下，让 Responses 的 HTTP streaming、HTTP non-streaming、工具循环、session resume、compaction 与历史多媒体形成一条完整链路。

相关官方协议说明：

- [迁移到 Responses API](https://developers.openai.com/api/docs/guides/migrate-to-responses)
- [Conversation state](https://developers.openai.com/api/docs/guides/conversation-state)
- [Streaming Responses](https://developers.openai.com/api/docs/guides/streaming-responses)
- [Function calling](https://developers.openai.com/api/docs/guides/function-calling)

## 2. 目标

1. 新增独立的 `openai_compatible_responses` provider，不复用或隐式切换到 Chat Completions wire protocol。
2. 同时支持 `/v1/responses` 的 SSE streaming 与 JSON non-streaming。
3. 复用现有 provider-neutral turn loop、工具执行、max-token continuation、非流式 fallback、TUI 文本事件和 canonical commit gate。
4. 使用 `store = false`，由 ACN 本地 session 保存并回放 Responses 所需的协议私有 items。
5. 第一阶段支持 reasoning 请求参数，并完整保存、原样回传 reasoning item；暂不把 Reasoning 显示到 TUI。
6. 支持从旧 session 和其他协议的 canonical content 合成 Responses input。
7. 对未 compact 的历史图片和 PDF 保留真实媒体 block；当前轮无需重复附加，历史消息本身携带原始媒体。
8. 保持旧 `messages.jsonl` 可读，不做批量迁移，不让协议私有内容进入 session search、Memory、claim 或普通 transcript。

## 3. 非目标

- 不实现 Responses WebSocket transport，也不增加 `supports_websockets`；该能力在 HTTP Responses 稳定后另立需求。
- 不实现 OpenAI Chat 的 `reasoning_content` 或其他 Chat Reasoning 兼容字段。
- 不实现 Anthropic thinking/reasoning 的保存、回传或展示；后续可以复用本期的协议私有 replay 容器。
- 不实现统一 Reasoning TUI cell、Reasoning delta、折叠展示或 session search。
- 不主动请求 `reasoning.summary`，不保证本期一定得到可展示的 Reasoning 明文。
- 不依赖 `previous_response_id`，不实现 `store = true` 模式。
- 不实现 Responses 厂商 A 到厂商 B 的状态迁移、endpoint/model 指纹或加密 reasoning 兼容判断。
- 不实现模型生成图片、文件、音频等输出的 TUI 展示。
- 不增加厂商专属自动删字段、自动改路径或 Responses 失败后自动切 Chat 的启发式兼容逻辑。
- 不改变 Chat/Anthropic 当前把历史附件投影为文本占位符的行为。

## 4. 已拍板产品与协议决策

### 4.1 独立 provider 与配置名（1A）

新增：

```toml
[agent.llm]
provider = "openai_compatible_responses"
endpoint = "https://llm.example.com/v1"
model = "example-model"
api_key_env = "LLM_API_KEY"
```

- `openai_compatible_responses` 是独立 adapter，不在 `openai_compatible_chat` 内增加 `api_mode`。
- endpoint resolver 沿用现有完整 URL/base URL 兼容规则，并为 base URL 补充 `/responses`。
- provider 名表示选择 ACN adapter；replay 中的 protocol tag 表示落盘数据所遵循的 wire protocol，两者不是厂商身份。

### 4.2 本地状态管理（2A）

- 每次 Responses 请求固定发送 `store = false`。
- ACN 不使用 `previous_response_id` 作为 session 的权威状态。
- 当前请求由有效 canonical input history、匹配当前协议的 provider replay items 和当前用户输入共同构造。
- 不保存 endpoint/model/provider 指纹，也不为假设不存在的跨 Responses 厂商迁移增加分支。
- 若用户意外更换 Responses endpoint，而新 endpoint 拒绝旧 replay items，保留上游明确错误；不静默删除 reasoning 或自动降级重试。

### 4.3 Session 顶层可选 replay（3A）

在 provider-neutral runtime message 与 canonical `SessionMessage` 上增加可选的协议私有 replay 字段。目标序列化形态为：

```json
{
  "role": "assistant",
  "content": [
    {"type": "text", "text": "最终回答"}
  ],
  "provider_replay": {
    "protocol": "openai_responses",
    "items": []
  }
}
```

Rust 侧采用带 serde tag 的通用枚举，首版只实现 Responses 变体；未来 Anthropic Reasoning 可以新增自己的变体，不复用 Responses item：

```rust
enum ProviderReplayState {
    OpenAiResponses { items: Vec<serde_json::Value> },
}
```

约束：

- `provider_replay` 是 message 顶层可选字段，不是新的 `SessionContentBlock`。
- 旧 session 缺少该字段时按 `None` 读取，不要求迁移或重写。
- 旧 reader 默认忽略该顶层未知字段；相比新增 content enum variant，降级读取更安全。
- replay 与 message 在同一条 canonical JSONL 记录中提交，不建立 sidecar 文件。
- `turn_events.jsonl` 不保存完整 opaque replay；失败或 partial response 不进入 canonical replay。

### 4.4 保存完整可回放 items（4A）

- 保存 Responses 返回的完整、已完成 output items，保持原始顺序和未知字段。
- parser 使用 typed view 校验并投影 text、tool call、stop 和 usage；持久化边界保留原始 JSON，避免兼容厂商字段被丢弃。
- 不保存整个 HTTP response envelope、header、request id 或无需回放的 usage/status 元数据。
- 普通用户 input、图片和 PDF 从 canonical content 重建，不复制进 assistant replay。
- max-token continuation 若插入协议私有 continuation input，该 input 与后续 output items 必须进入同一段有序 replay，确保下一轮能重建实际发生过的协议序列。

### 4.5 Reasoning 第一阶段边界（5A、10A）

- `reasoning_effort = none` 或未配置时，不发送 `reasoning` 请求字段。
- 其他值映射为 Responses `reasoning.effort`；ACN 不静默改写或降级不受上游支持的 effort 值。
- 本期不主动发送 `reasoning.summary`。
- 返回的 reasoning item 无论包含 summary、明文、加密内容或兼容厂商扩展字段，都完整保留并在下一次 Responses 请求原样回传。
- Reasoning 不投影为 assistant Text，不进入 TUI、普通 transcript、session search、Memory、claim 或团队服务。
- 本期不新增 Reasoning streaming event；以后统一展示时在本基础上增加投影，不再次修改 session replay 基础模型。

### 4.6 旧 session 与跨协议 resume（6A）

- 历史消息没有匹配的 replay 时，从 canonical Text、Image、Document、ToolUse、ToolResult 合成当前协议的 input。
- Chat/Anthropic 切换到 Responses 时，已存在的 canonical 文本、工具和未 compact 媒体继续使用；旧协议已丢弃的 reasoning 无法恢复。
- Responses 切换到 Chat/Anthropic 时，忽略 Responses replay，只使用 canonical content；不把 Responses reasoning 转换为其他协议的 thinking。
- 同一 session 可以包含不同协议产生的 message。构造请求时只使用匹配当前协议的 replay；不匹配的 message 走 canonical 投影。
- 协议切换不重写或删除历史 JSONL。以后切回 Responses 时，仍在有效上下文内的 Responses replay 可以继续使用；切换期间新增的其他协议消息从 canonical 合成。
- 这是语义级连续，不承诺协议私有 reasoning 在跨协议后仍然无损。

### 4.7 Streaming、non-streaming 与 fallback（7A）

- Responses adapter 必须同时实现 streaming 和 non-streaming；两条 transport 最终归一到同一个完整响应 reducer。
- 首次 provider call 继续使用 `stream = true`。
- 尚未产生可见 assistant 文本前的 retry 沿用底层 client 规则。
- 已经向 TUI 发出部分 assistant 文本后流式失败，沿用 [流式响应失败后回退非流式重试](PRD_retry_non_streaming.md) 的现有语义。
- 不把 streaming partial item、partial reasoning 或未完成 function call 写入 canonical。
- 只有完整响应成功、通过 item/tool/stop 校验后，才执行工具并提交 session。
- 用户 cancel/steer 不得被当作 provider 错误进行 fallback。

### 4.8 未 compact 历史多媒体真实重放（修正版 8B）

当前 ACN 会把已经落盘的历史 Image/Document 在下一用户 turn 的 provider projection 中转换为文本占位符。本 provider 改为：

- 当前用户上传的图片/PDF 继续按现有 canonical content 落盘一次。
- 同一逻辑 turn 的工具回环继续携带当前输入附件。
- turn 已完成后进入下一用户 turn，或进程退出后 resume，只要原消息尚未进入 compacted prefix，Responses history 就从 canonical content 重建真实 `input_image`/`input_file`。
- 当前轮不需要把上一轮附件当作新附件重复添加；原始媒体位于上一轮 user input item 中。
- `provider_replay` 不复制用户图片/PDF base64。
- compacted prefix 使用摘要，不再发送该 prefix 的原始附件和旧 replay；未 compact tail 继续发送原始媒体与对应 replay。
- Chat/Anthropic 保持现状，本期不顺带改变其历史附件策略。

### 4.9 Function tool strict 策略（9A）

- Responses function tool 使用扁平定义：`type`、`name`、`description`、`parameters`、`strict`。
- 首版显式发送 `strict = false`，保持 ACN 现有非严格工具 schema 与调用行为。
- 不在本需求中补齐全工具 strict schema，也不增加 TOML strict 开关。
- function call 与 function call output 使用 `call_id` 关联，并保持 provider 输出顺序与现有 tool loop source order。

### 4.10 输出 item 支持范围（11A）

- `message.output_text` 投影为 canonical assistant Text。
- `function_call` 投影为 canonical ToolUse。
- `reasoning` 和其他不需要本地动作的未知 output item 保存在 replay 中，暂不展示。
- 响应同时包含可处理 text/tool 与未知 item 时，未知 item 不阻塞成功。
- 若整个成功响应只有 ACN 无法消费的输出媒体或未知 actionable item，且没有 text/tool，则返回明确的“不支持该输出类型”错误，不静默提交空回答。
- 模型生成图片、文件、音频的 TUI/canonical 展示另立需求。

### 4.11 Compatible endpoint 边界（12A）

- `openai_compatible_responses` 要求 endpoint 实现 Responses 基本协议。
- 不因 400/404 自动改发 Chat Completions。
- 不猜测删除 `store`、`reasoning`、`instructions` 或其他字段后重试。
- 不增加厂商名分支或一组未经真实接口验证的兼容开关。
- streaming 失败后的 non-streaming fallback 是同一 Responses 协议内的 transport 恢复，不属于协议降级。

### 4.12 实现期追加协议边界（13A、14A、15A）

13A：Responses 上游错误保留状态码、错误码和不含 payload 的说明，但递归脱敏回显的 `request`、`request_body`、`instructions`、`input`、`output`、`reasoning` 与 `encrypted_content`。选项包括完整展示错误体、完全隐藏错误体或仅脱敏协议私有/请求字段；选择第三项，因为它既保留可诊断性，也避免 reasoning/replay、system prompt 与用户输入进入 retry 日志、fallback journal 或 TUI 错误。

14A：顶层 `status = completed` 时，可消费的 `message`/`function_call` 若显式携带 item `status`，必须也是 `completed`；显式 `incomplete`/`in_progress` 视为协议终态冲突并拒绝提交或执行工具。兼容实现省略 item `status` 时继续接受。选项包括忽略 item status、强制所有实现必须提供 status 或“有则校验”；选择“有则校验”，因为它能阻止半截工具调用，同时不破坏已验证的兼容 endpoint。

15A：SSE 以索引连续、无重复且结构合法的 `response.output_item.done` 作为完整 output 与 replay 的唯一权威；terminal event 只提供顶层 status、usage 等终态元数据，不读取或校验 terminal `response.output` 与 done items 的数量、身份或字段一致性。done 中的 reasoning、message 与 function call 按原始顺序保留和处理；terminal output 省略 item、额外列出 item 或使用不同可选字段形状均不改变结果，未经过合法 done event 的 terminal tool item 不执行。选项包括 terminal 与 done 完全相等、terminal 为 done 子集或完全不比较；选择第三项，因为单一权威能兼容 terminal envelope 的表示差异，并确保只有明确完成的 done item 能进入 replay 或触发工具。重复/不连续/结构非法的 done event、terminal event 与顶层 status 冲突、显式未完成的可消费 item，以及 terminal event 前 EOF 仍按协议错误处理。

## 5. 请求、响应与状态归一

### 5.1 请求字段

首版请求至少覆盖：

- `model`；
- `instructions`；
- `input`；
- `tools`；
- `max_output_tokens`；
- `stream`；
- `store = false`；
- 非 `none` 时的 `reasoning.effort`。

系统提示映射为 `instructions`，每次请求都按 ACN 当前 system prompt/compaction 结果重新发送，不依赖服务端 response state。

### 5.2 Canonical input 映射

| ACN canonical block | Responses input |
| --- | --- |
| 用户 Text | user message `input_text` |
| SkillInstructions | 渲染后的 user `input_text` |
| Image | user message `input_image`，使用现有 media type/base64 |
| PDF Document | user message `input_file`，保留可用 filename |
| assistant Text（无 replay） | assistant message |
| ToolUse（无 replay） | `function_call` |
| ToolResult | `function_call_output` |
| 匹配的 provider replay | 按保存顺序使用原始 Responses items |

若 canonical 与 raw replay 都描述同一 assistant 响应，Responses adapter 必须优先使用 raw replay，不能同时发送 canonical 投影导致重复 assistant text/tool call。

旧 session 中没有 filename 的 PDF 使用中性文件名 `attachment.pdf` 构造 `input_file`；该名称不包含本地路径或用户信息，不改变有原始 filename 时的保留行为。

### 5.3 Non-streaming

- 解析完整 Response JSON 的 status、output、usage、error/incomplete detail。
- 按原始顺序保留完整 output items。
- 从 output items 投影 assistant text、function calls 和 provider stop。
- token usage 优先使用上游返回的 `usage.total_tokens` 更新现有 context usage 事件。
- response 缺少必须终态、item 结构非法或 call id 不能形成合法工具回环时返回协议错误，不提交 partial state。

### 5.4 Streaming

至少处理并测试：

- `response.created`；
- `response.output_item.added`；
- `response.output_text.delta`；
- `response.output_item.done`；
- `response.completed`；
- `response.incomplete`；
- `response.failed`；
- error event。

约束：

- `response.output_text.delta` 只用于即时 TUI 文本事件。
- `response.output_item.done` 是完整 item 的权威来源；不能依赖 delta 反推完整 reasoning 或 function call JSON。
- terminal `response.output` 不作为第二份 output 来源，也不与 done items 做一致性比较。
- 收到合法 `response.completed` 后才算 transport 成功。
- SSE 正常 EOF 但没有完成终态必须报错。
- event 可以跨任意网络 chunk/frame 边界，parser 不依赖单次 read 对齐。
- 未识别事件可以保留 debug 诊断并安全忽略，但不能掩盖缺失 terminal event 或已知失败状态。

### 5.5 Stop 与 max-token continuation

- 完整响应含 function call 时映射为现有 `ProviderStop::ToolUse`。
- 正常完成且无待执行工具时映射为 `ProviderStop::Done`。
- `response.incomplete` 且原因是 `max_output_tokens` 时，复用当前 adapter 的 max-token continuation 上限、继续提示、文本合并和逻辑 deadline。
- continuation 不能被计作 provider retry 或 non-streaming fallback attempt。
- continuation 过程中产生的 output items 和内部 continuation input 形成有序 replay，不能只保存最终一段 text。
- 其他 incomplete 原因返回明确错误，不假装正常完成。

## 6. Session、compaction 与隐私边界

### 6.1 Canonical 与 replay 的职责

```text
SessionMessage.content
  ├─ TUI / transcript
  ├─ tool loop
  ├─ session search
  ├─ recap / Memory
  └─ 跨协议语义投影

SessionMessage.provider_replay
  └─ 匹配协议的下一次 provider request
```

- replay 不参与用户可见文本拼接。
- replay 不进入 claim、Memory、Router、Maintainer 或团队上传。
- 日志与错误使用现有 media/base64 脱敏边界，不打印 opaque item payload、API key 或完整附件。
- reasoning/encrypted content 仅保存在 agent 私有 session 目录中。

### 6.2 Compaction

- compaction cursor/hash 必须考虑 replay 对消息身份的影响，避免 canonical 相同但协议状态不同的 suffix 被误判为同一历史。
- compacted prefix 的 canonical raw message 仍可保留在本地 JSONL 作为历史事实，但 provider projection 不再发送其附件或 replay。
- compaction summary 只进入 canonical/provider-visible summary，不生成伪造的 Responses reasoning item。
- 未 compact suffix 保留匹配的 replay、真实 Image/Document 与合法 ToolUse/ToolResult 边界。
- token estimation 不按 base64 字符数直接估算视觉 token；沿用媒体固定估算与上游 usage 优先原则。opaque reasoning 不作为可见文本重复计数。
- 同一 message 同时存在 canonical 与 replay 时，本地 token estimate 取两种投影估算的较大值，避免 Responses reasoning 被漏算，也避免把同一 assistant 语义简单双算。
- hard-tail provider-only 外置仍可处理 Skill 与文本附件；当 adapter 的历史媒体策略为 `Preserve` 时，不得把 Image/Document 替换为文件引用。保留真实媒体后仍超预算则返回明确错误，不静默破坏 8B。
- session/delegation 的摘要输入、compaction anchor、审计预览和可读 transcript 统一使用 safe canonical projection：去掉 replay，并把 Image/Document 转为不可逆占位；该规则不影响真正 provider history 的未 compact 媒体/replay。

### 6.3 Journal 与失败提交

- `turn_events.jsonl` 继续记录可恢复的 TUI/turn 事实，不复制完整 Responses raw items。
- streaming partial、失败 non-streaming response、失败 continuation 和解析失败的 response 都不写 `provider_replay`。
- 一次逻辑 turn 成功提交时，canonical content 与 replay 一起进入 `messages.jsonl`。
- fallback 成功时只提交最终完整成功 response 的 replay；失败 streaming 中收到的 partial items 不混入。

## 7. 分阶段实施与阶段验收

实施者进入每一阶段前应重新核对本文已拍板边界；发现协议文档、真实 endpoint 或现有 ACN 架构与本文冲突时，先记录证据并请求新的产品决策，不在实现中悄然改变语义。

### 阶段 0：基线与测试夹具

实施：

1. 确认 worktree、分支、现有 dirty changes 和 provider/session/TUI 基线。
2. 固化当前 Chat/Anthropic、流式 fallback、max-token continuation、tool loop 和历史附件占位行为的现有回归测试。
3. 建立 Responses fake server/SSE fixture，覆盖可控 chunk、失败终态、工具调用、usage 与 non-streaming JSON。
4. 明确新增模块与现有所有权，避免把 Responses DTO 塞入 provider-neutral turn loop。

阶段验收：

- 现有相关测试可以运行且基线明确。
- fixture 不访问真实外部服务，不含 secret、真实用户名或内部域名。
- 实施落点能够映射到本文每一项已拍板决策。

### 阶段 1：Provider replay 与 session 基础

实施：

1. 增加通用 `ProviderReplayState` 与 runtime/session message 可选字段。
2. 打通 CompletedSessionTurnMessage、canonical commit、JSONL 读写和 session reload。
3. 更新 provider projection、compaction suffix/hash、token estimate 与 transcript/search/Memory 忽略边界。
4. 为 Responses 增加未 compact 历史多媒体真实投影能力，同时保持 Chat/Anthropic 现状。
5. 明确协议不匹配时 canonical fallback、匹配时 raw replay 优先且不重复发送。

阶段验收：

- 旧 JSONL fixture 无迁移可读。
- 新 replay 完整 round-trip，未知 item 字段不丢失。
- TUI/transcript/search/Memory 不展示 opaque reasoning、raw JSON 或 base64。
- Responses resume 能恢复未 compact Image/PDF；compacted prefix 不再进入 provider request。
- Chat/Anthropic 的历史附件和请求快照测试不发生意外变化。
- 跨协议切换测试覆盖 Chat/Anthropic → Responses 与 Responses → Chat/Anthropic。

### 阶段 2：Responses protocol、JSON client 与 SSE client

实施：

1. 新增独立 Responses protocol/client/streaming 模块与 endpoint kind。
2. 实现 request DTO、flattened function tools、`store = false` 和 reasoning effort。
3. 实现 non-streaming Response JSON 解析与统一 output reducer。
4. 实现 SSE decoder、event accumulator、terminal validation 与 raw item 收集。
5. 使用项目现有 timeout、retry、错误脱敏和 API key header 管线。

阶段验收：

- JSON text/tool/reasoning/usage/failed/incomplete fixture 全覆盖。
- SSE 覆盖任意 chunk/frame 切分、多 text delta、多 function call、未知附带 item、terminal output 缺失/额外/字段形状不同、completed/failed/incomplete/error 和无终态 EOF。
- streaming 与 non-streaming 对同一逻辑 response 产生相同 canonical projection 与 replay。
- 解析错误不泄露 API key、base64 或完整 opaque payload。

### 阶段 3：Adapter、turn loop 与配置装配

实施：

1. 新增 `openai_compatible_responses` adapter，接入 provider abstraction。
2. 更新 config enum/校验、bootstrap、endpoint resolver、API module exports 与配置文档。
3. 实现 canonical input、raw replay、tool specs、assistant output 的双向映射。
4. 接入现有 context usage、ToolUse/ToolResult、并发工具、max-token continuation 和 streaming fallback。
5. 保持 `strict = false`、不请求 reasoning summary、不实现协议降级。

阶段验收：

- TOML provider、base URL、完整 `/responses` URL、query/path 保留和 API key env 有自动化测试。
- simple text、历史消息、多媒体、工具定义、并行 function calls、tool results、reasoning replay 均有请求快照测试。
- tool call → 本地执行 → `function_call_output` → 后续回答形成完整集成测试。
- streaming partial 后 non-streaming fallback 只提交最终成功结果，不重复执行工具。
- max-token continuation 合并可见 text，并保存完整有序 replay。
- 仅未知输出媒体时返回明确错误；附带未知 item 时仍可完成 text/tool 响应。

### 阶段 4：定向自动化回归与文档

实施：

1. 补齐 config template、README、user guide、config parameters、architecture/provider 文档和 PRD 索引。
2. 增加 session resume、compaction、跨协议、附件、fallback、journal commit gate 的跨模块回归。
3. 检查所有新增 enum/field 的 serde 兼容、错误日志脱敏和测试 fixture 中性化。

阶段验收：

- 用户可以只根据稳定文档完成 Responses TOML 配置。
- 文档明确 Chat、Responses、Anthropic、HTTP SSE、non-streaming 与未来 WebSocket 的边界。
- 自动化测试覆盖本文第 9 节验收矩阵中的所有非真实网络项目。

### 阶段 5：真实 LLM TUI 定向 smoke test

按照 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md` 在真实 tty 中验收。测试使用 `export_env.sh` 或用户已有环境变量，但不得打印、落盘或回显 secret。真实测试使用明确支持 Responses 的实际模型与 endpoint，不用 fake server 代替以下核心行为。

场景 A：直接 Responses streaming。

1. 使用 `openai_compatible_responses` 启动 ACN TUI。
2. 发送稳定的文本请求，确认文本逐步出现、最终完成且 stderr 为空。
3. 检查 canonical assistant text 与 provider replay 同时落盘，TUI 不展示 raw reasoning。

场景 B：真实工具回环。

1. 要求模型读取仓库中一个明确、无副作用的文件。
2. 确认模型实际发出 function call、TUI 展示现有 ToolCell、本地工具执行一次。
3. 确认下一次 Responses 请求包含匹配 `call_id` 的 `function_call_output`，最终回答正常提交。

场景 C：历史图片与 resume。

1. 在 `target/` 下准备一个不含敏感信息、具有多个可区分细节的确定性测试图片。
2. 第一用户 turn 附加该图并只询问其中一个细节。
3. 第二用户 turn 不重新附图，询问另一个未在上一回答中出现的细节，确认模型仍能查看历史原图。
4. 正常退出并 resume 同一 session，再询问第三个独立细节，确认未 compact 历史图片仍被发送。
5. 检查 session 中图片只在 canonical user content 保存一份，没有复制进 assistant replay。

场景 D：真实 streaming 断流后的 non-streaming fallback。

1. 使用受控本地代理转发到真实 Responses endpoint。
2. 代理在首个真实 `output_text.delta` 后只切断第一次 SSE，不伪造模型输出；后续 non-streaming 请求正常转发到同一真实模型。
3. 确认 TUI partial 只出现一份、fallback activity 正确、成功结果原位替换并只提交一次。
4. 确认失败 stream 中的 partial items 不进入 replay，工具不重复执行，stderr 为空。

场景 E：max-token continuation。

1. 使用测试专用低 `max_tokens` 配置和需要较长输出的稳定请求，触发真实 `max_output_tokens` incomplete。
2. 确认沿用现有 continuation 上限和合并逻辑，TUI 最终只显示一条完整 assistant 输出。
3. 再发送一轮普通问题，确认 continuation 后的 replay history 可继续使用。

真实 LLM smoke 的屏幕 capture、脱敏后的请求结构断言、stderr 和 session 检查结果保存在 `target/`；不得把真实响应全文、API key、endpoint 私有信息或附件 base64 提交到仓库。

阶段验收：场景 A–E 全部通过；若真实模型行为存在非确定性，使用更明确的正常业务提示或受控 transport 故障重试，不通过伪造成功、扩大生产逻辑或为极低概率情况增加防御来让测试变绿。

### 阶段 6：模块级 code review 与修复

按 `.agents/skills/code-review/SKILL.md` 对下列风险域分别进行完整的“本地 review + 外部独立只读 review”，而不是只看 PRD 或测试结果：

1. Responses protocol/client/streaming：SSE 状态机、terminal、错误、usage、JSON/SSE 一致性。
2. replay/session/compaction/media：落盘兼容、resume、跨协议、附件、隐私和 canonical/replay 去重。
3. adapter/turn loop/tools/fallback/config：tool call、continuation、retry/fallback、装配与用户配置。

review 处理规则：

- 合并、去重本地与独立 reviewer 结论，并记录现实触发条件。
- 修复所有有现实触发条件、会造成数据/协议错误、安全问题、工具重复执行、session 无法恢复或明显用户错误的 P0/P1。
- P2/P3 不自动扩 scope；只有它直接影响已拍板语义或会阻塞维护时才单独说明并请求决定。
- 不修复仅基于极低概率、不可构造业务路径、重复防御、纯风格偏好或“也许某厂商会这样”的虚空问题。
- 每个修复后运行对应模块测试，再对该风险域重新执行 code-review skill；直到没有可行动的现实 P0/P1。

阶段验收：三个风险域都取得可用的本地和外部 review 结果，且不存在未处理的现实 P0/P1；所有接受或拒绝的 finding 都有简短、可核查的理由。

### 阶段 7：全量验证、全量 review 与收敛

1. 按 `.agents/skills/verify/SKILL.md` 执行：

   ```bash
   if [[ -f export_env.sh ]]; then
     source export_env.sh
   fi
   cargo fmt --check
   cargo clippy -- -D warnings
   cargo test
   cargo check
   ```

   本需求不修改 crate、CLI 或配置版本号，因此不运行 `scripts/check_version_consistency.sh`。只有后续实现实际触碰版本字段或发布元数据时，才把该检查重新加入验收。

2. 运行项目默认 tmux TUI smoke，并检查所有 capture、`stderr.log` 与 tmux cleanup。
3. 再执行一次覆盖整个 feature diff 和周边运行代码的全量 code-review skill。
4. 修复全量 review 中有现实触发条件且值得修复的 P0/P1，运行受影响测试与完整 verify。
5. 修复后再次执行全量 code-review skill；重复“review → 现实 P0/P1 修复 → 验证 → re-review”，直到全量 review 明确没有可行动的现实 P0/P1。
6. 最后重新运行阶段 5 中受修复影响的真实 LLM TUI 场景；若修复触及 transport、turn loop、session 或媒体路径，则 A–E 全量复验。

最终 review 的“没问题”定义为：本地与独立 reviewer 均没有仍未处理的、具有现实触发条件和实质影响的 P0/P1。纯风格、低价值重构、P2/P3 和极端假设不作为无限循环修复门槛，但需要在最终报告中说明是否存在与为什么不处理。

阶段验收：完整 verify、默认 TUI smoke、真实 LLM 定向 smoke、全量 review 和必要的 re-review 全部通过；工作树只包含本需求范围内的代码、测试和文档。

## 8. 模块级改动范围

预计新增或修改：

- `src/api/responses/`：protocol、client、streaming、统一 reducer；
- `src/api/openai_compatible_responses.rs`：canonical/replay adapter；
- `src/api/provider.rs`、`src/api/types.rs`、`src/api/endpoint.rs`、`src/api/mod.rs`：provider abstraction 与 endpoint；
- `src/config.rs`、`src/bootstrap.rs`：TOML enum、校验与 adapter 装配；
- `src/session/mod.rs`：optional provider replay 与 canonical commit；
- `src/agent/session_engine/`：provider projection、resume、compaction、多媒体与 transcript 隔离；
- `src/api/turn_loop.rs`：只做现有 provider-neutral 接缝所需改动，不下沉 Responses DTO；
- `README.md`、`config.template.toml`、`docs/config_parameters.md`、`docs/user_guide.md`、`docs/architecture.md` 与本 PRD；
- 相应单元、集成、fake server、session fixture 与 tmux 验收脚本。

若实现发现必须修改 TUI 渲染或事件类型，应先证明现有 provider-neutral Text/Tool 事件无法承载；Reasoning TUI 或输出多媒体不能借此进入本期。

## 9. 整体验收矩阵

| 范畴 | 必须通过的行为 |
| --- | --- |
| 配置 | 新 provider 可解析；旧配置行为不变；endpoint 和 key env 规则正确 |
| Text | JSON 与 SSE 都得到一致 canonical text 和 replay |
| Streaming | delta 实时展示；done/completed 后才提交；断流按现有 fallback 恢复 |
| Tools | function call、并行 calls、call_id、tool output、后续回答完整闭环 |
| Reasoning | effort 正确发送；raw item 保存并回传；TUI/search/Memory 不展示 |
| Session | 新字段 round-trip；旧 JSONL 可读；失败 response 不污染 canonical |
| Resume | 同协议使用 replay；跨协议 canonical 投影；不重复 text/tool |
| 多媒体 | 当前 turn、下一 turn、resume 的未 compact 图片/PDF真实发送且只落盘一份 |
| Compaction | prefix 的附件/replay 退出 provider context；suffix 保留；摘要有效 |
| Max tokens | continuation 沿用现有边界，合并文本并保留有序协议历史 |
| 错误 | failed/incomplete/error/EOF/非法 item 有清晰错误且不泄露敏感 payload |
| 兼容 | 不自动切 Chat、不猜字段、不增加无证据厂商分支 |
| 回归 | Chat、Anthropic、fallback、并发工具、session search、Memory 行为不退化 |
| 终端 | 默认 tmux smoke 与真实 LLM 场景 A–E 通过，stderr 为空 |
| 工程 | fmt、clippy、tests、check 全部通过 |
| 审查 | 模块级与全量 code-review skill 均无未处理的现实 P0/P1 |

## 10. 最终交付报告

实现完成后的交付报告必须包含：

1. 实际改动模块和最终数据流概述。
2. 与本文 1A–12A/8B 决策逐项对应的实现结果。
3. session schema 兼容、跨协议和 compaction 的实际验证证据。
4. JSON/SSE/fallback/tool/max-token/附件/reasoning 的自动化测试结果。
5. 真实 LLM TUI 场景 A–E 的结果、使用的模型类别和脱敏后的观察，不泄露 key 或私有 endpoint。
6. `verify` 每条命令和默认 tmux smoke 的结果。
7. 各模块 code review、全量 code review、修复与 re-review 的最终结论。
8. 未处理 finding 及其等级和理由；不得把现实 P0/P1 留作“后续优化”。
9. 明确列出仍不在本期范围内的 WebSocket、统一 Reasoning 展示、Anthropic Reasoning 和输出多媒体。

## 11. 当前完成状态

- 产品与协议决策：已拍板。
- PRD：已建立并作为本次实现与验收基线；实现期新增的 13A–15A 均为既有语义下的协议安全边界，没有修改 1A–12A/8B。
- 阶段 0–4：已完成；provider replay、Responses JSON/SSE、adapter/config、工具/fallback/continuation、session/compaction/media 与稳定文档均已落地。
- 阶段 5：真实 LLM TUI 场景 A–E 全部通过。实际观察覆盖文本 streaming 与下一轮历史、真实 function call/tool output 回环、未 compact 历史图片在下一轮与正常退出/resume 后继续可见、首个可见 delta 后断流并按同一 Responses 协议 non-streaming fallback、低 max-token 下的多请求 continuation 与后续历史。TUI stderr 均为空；结构断言与脱敏 capture 仅保存在忽略的 `target/`。
- 阶段 6：三个风险域均完成本地与独立只读 review；已修复并复审通过的现实 P1 包括 SSE retry 边界测试缺口、replay token 漏估、Preserve 媒体被 hard-tail 外置、摘要/审计/委托路径泄露 replay 或 raw media、Responses 错误体回显私有 payload、显式未完成 item 被消费，以及 terminal output 被误作第二份 output 权威导致的兼容流失败。各风险域最终均无未处理 P0/P1。
- 阶段 7：`cargo fmt --all -- --check`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets --all-features`、`cargo check --all-targets --all-features` 全部通过；完整测试结果为 1920 个库测试以及全部二进制/集成测试通过。项目默认 tmux `/help` → `/exit` smoke 通过且无残留测试进程。按本 PRD约定未运行版本一致性脚本。
- 15A 修订回归：重新通过格式、Clippy、1920 个库测试、全部二进制/集成测试与 check；真实 LLM TUI 依次覆盖短文本、同会话 replay、40 行长文本和 `file_read` 工具回环，共观察到 24 个 streaming delta、4 个成功 turn、1 组完整 tool use/result，未出现 non-streaming fallback，TUI stderr 为空。该兼容 endpoint 未按本次低 `max_output_tokens` 配置返回 incomplete，因此 continuation 仍由自动化测试覆盖，不把长输出误报为真实 continuation 验收。
- 最终全量 review：独立 reviewer 覆盖相对 `origin/main` 的 30 个已跟踪修改文件与 6 个新增文件，并复跑 44 个 Responses 定向测试和完整测试集；结论为“未发现 P0/P1 级别缺陷”。没有遗留 finding。
- TUI：未新增或修改用户可见渲染语义；Responses 文本、工具与 fallback 继续复用既有 Text/Tool/activity 展示，reasoning 仍只保存和回传、不展示。
