# Anthropic Reasoning 保存与原样回传

> 状态：阶段 0–15 已完成（2026-08-07）。本文定义 Anthropic Messages thinking/reasoning 的请求、解析、私有落盘、resume、工具回环、compaction 与原样回传边界，并同时把 OpenAI Responses replay 从“仅协议”收紧为“协议 + model + 连续 replay 世代”。Reasoning 的 TUI 展示与 OpenAI Chat reasoning 不在本期范围内。

> 2026-08-27 补充：本文的 Reasoning/replay 与 provider-triggered compaction 边界继续有效；compact recap 已由 [PRD_recap_in_supervisor.md](PRD_recap_in_supervisor.md) 改为异步 Supervisor job。Summary 失败仍阻断当前 context recovery，Recap 失败不再阻断或回滚 summary。

## 1. 背景与现状

ACN 的主对话 provider 当前支持：

- `anthropic`；
- `openai_chat`；
- `openai_responses`。

三种 adapter 都接入同一套 provider-neutral turn loop、工具执行、HTTP streaming、HTTP non-streaming fallback、canonical session 与 compaction。当前行为存在以下差异：

1. OpenAI Responses 已经能够把完整 reasoning/output item 保存在 `provider_replay`，并在后续请求、resume 与未 compact history 中回传，但 replay 尚未记录生成它的 model。
2. Anthropic streaming reducer 已经能够接收 `thinking_delta` 与 `signature_delta`，并拼装完整 content block；non-streaming 也能取得完整 `content` 数组，但完成响应投影会主动跳过 `thinking` 与 `redacted_thinking`，且没有 Anthropic replay，因此它们不会进入 session 或下一次请求。
3. OpenAI Chat 不承载 ACN 的 Reasoning replay。部分要求回传 `reasoning_content` 的 Chat-compatible thinking 模型可能在工具调用或后续轮次报错；用户应改用 `openai_responses` 或 `anthropic`。
4. 当前 provider history 只按协议筛选 replay，不能阻止某个 model 生成的 opaque reasoning 被另一个 model 使用。

Anthropic Messages 的 thinking 不是普通 assistant 文本：

- 普通 `thinking` block 可能包含可读 `thinking` 与 opaque `signature`；
- `redacted_thinking` 使用 opaque `data`；
- 工具回环要求相关 assistant content blocks 保持原始顺序并完整回传；
- 模型切换时应移除旧 model 生成的 thinking blocks；
- 不同模型和兼容厂商支持的 `thinking.type`、`budget_tokens`、`output_config.effort` 与默认开关并不完全一致。

因此本期不把不同协议的原始字段强行归一为一种通用 JSON，也不把 reasoning 投影成可见文本。ACN 继续使用 canonical content 表达用户可见语义，同时为每种协议保存各自可验证、可回放的私有状态。

相关协议资料：

- [Anthropic Thinking](https://platform.claude.com/docs/en/build-with-claude/thinking)
- [Anthropic Thinking in tool and multi-turn workflows](https://platform.claude.com/docs/en/build-with-claude/thinking-tool-workflows)
- [Anthropic Extended thinking](https://platform.claude.com/docs/en/build-with-claude/extended-thinking)
- [DeepSeek Anthropic API compatibility](https://api-docs.deepseek.com/guides/anthropic_api/)
- [智谱 Claude API 兼容](https://docs.bigmodel.cn/cn/guide/develop/claude/introduction)
- [阿里云百炼 Token Plan Anthropic 兼容说明](https://help.aliyun.com/zh/model-studio/token-plan-team-faq)
- [OpenAI Responses statefulness](https://developers.openai.com/api/docs/guides/migrate-to-responses#4-decide-when-to-use-statefulness)
- [OpenAI encrypted reasoning items](https://developers.openai.com/cookbook/examples/responses_api/reasoning_items#encrypted-reasoning-items)

## 2. 目标

1. Anthropic streaming 与 non-streaming 都完整接收、校验、保存 `thinking`、`redacted_thinking` 以及未来未知但可安全回放的 assistant content block。
2. 在工具回环、普通下一轮、进程退出后的 resume 与未 compact history 中，按 Anthropic Messages 协议原样回传完整 raw blocks。
3. 保留 canonical Text、ToolUse、ToolResult、Image、Document 作为跨协议、TUI、search、Memory 与 compaction summary 的语义来源。
4. OpenAI Responses 与 Anthropic replay 统一绑定到“wire protocol + 配置中的精确 model 字符串”。
5. model 或 protocol 切换后开始新的连续 replay 世代，不能在 A → B → A 后复活第一次 A 世代的 opaque reasoning。
6. Reasoning 只保存在 agent 私有 session 中，不进入 TUI、普通 transcript、session search、Memory、claim、Router、Maintainer、recap 或团队服务。
7. 保持现有 HTTP streaming → non-streaming fallback、工具执行、max-token continuation、cancel/steer 与 canonical commit gate。
8. 对 Responses 已完成链路做同步回归，避免 identity 重构破坏已有 reasoning、附件、工具与 resume 行为。
9. `openai_responses`、`anthropic`、`openai_chat` 对未 compact 的历史 Image/Document 统一保留真实媒体；compacted prefix 仍只发送摘要。

## 3. 非目标

- 不实现 Reasoning TUI cell、thinking delta 展示、折叠面板、颜色、耗时或 token 明细。
- 不把可读 thinking 拼入 assistant Text，也不在最终回答前后插入“思考过程”文本。
- 不实现 OpenAI Chat 的 `reasoning_content`、`reasoning`、`reasoning_details` 或厂商专属 Chat 字段。
- 不在 Chat streaming 异常恢复问题上顺带扩大改造；tool-only、缺终态、损坏 SSE 等作为独立 Chat P2 hardening 后续处理。
- 不把 Anthropic raw block 转换成 Responses reasoning item，也不反向转换。
- 不实现跨厂商、跨 endpoint 的加密 reasoning 转换、解密、重新签名或兼容性探测。
- 不新增 Router Reasoning。Router rerank 仍是单次调用，不请求、不保存、不回传 reasoning。
- 不改变 TUI 的用户可见渲染、composer、timeline 或 turn event 类型。
- 不实现 WebSocket transport 或 `supports_websockets`。
- 不实现模型生成图片、文件、音频等输出的 TUI/canonical 展示。
- 不根据 model 名称自动猜测 thinking 模式，不在 400 后自动切换 `enabled`/`adaptive`、删除 budget 或降级协议。
- 不在本期引入 provider endpoint fingerprint。相同协议、相同 model 字符串但不同 endpoint 仍视为同一 identity；调用方应避免把同名 model 映射到不兼容实现。

## 4. 已拍板决策

以下决策来自需求讨论。实现过程中不得静默修改；若真实协议、真实 endpoint 或现有架构证明某项不可行，必须补充证据并由用户重新拍板。

### 4.1 Reasoning 的产品边界

- OpenAI Chat 明确视为不支持 Reasoning replay；文档需要提示部分 thinking 模型可能因此在后续轮次或工具回环报错，并建议切换到 `openai_responses` 或 `anthropic`。
- OpenAI Responses 与 Anthropic 都接受、保存、回传各自协议的 Reasoning，但当前都不展示。
- 明文、摘要、空 thinking + signature、加密或脱敏内容都按上游原样保存；ACN 不要求能读懂或解密。
- Reasoning 不进入 canonical Text。用户看到的仍只有最终 text、工具活动和既有错误/fallback 状态。
- 不新增跨协议通用 raw reasoning DTO；统一的是生命周期和安全边界，不是 wire JSON 字段。

### 4.2 Anthropic 请求侧配置语义（原 2A）

- 保留现有 `reasoning_effort`，Anthropic adapter 继续把非 `none` 值映射为 `output_config.effort`；`none` 时省略该字段。
- Anthropic 的 `thinking.type` 必须允许显式配置，也必须允许完全省略，让 endpoint 使用自身默认行为。
- `budget_tokens` 是独立、可选配置，不能从 `reasoning_effort` 或 `max_tokens` 自动推导。
- 选择 `enabled` 时，只有用户配置了 budget 才发送 `budget_tokens`；不因为 Anthropic 官方某些 model 要求 budget，就强迫所有兼容 endpoint 都携带它。
- 选择 `adaptive` 或 `disabled` 时不发送 `budget_tokens`。
- ACN 不按 model 名称判断应该使用 `enabled` 还是 `adaptive`，也不在上游拒绝后自动改写请求。
- 不主动设置 `display = summarized/omitted`；ACN 接受 endpoint 实际返回的 block，并且无论是否有可读 thinking 都不在 TUI 展示。

背景原因：Anthropic 当前不同代际 model 对 manual `enabled + budget_tokens` 与 `adaptive + effort` 的支持不同；DeepSeek 等兼容 endpoint 支持 `thinking`，但可能忽略 `budget_tokens`；其他兼容厂商也可能使用默认思考模式。因此 type、effort 与 budget 不能被 ACN 合并成一个猜测性开关。

### 4.3 Anthropic replay 保存完整协议消息

- `ProviderReplayState` 新增独立 Anthropic 变体，不复用 Responses item。
- Anthropic replay 保存完成并通过校验的有序协议私有 message/content blocks，而不只抽取 `thinking` 字符串。
- raw assistant message 必须保留 `thinking`、`redacted_thinking`、`text`、`tool_use`、signature/data、block 顺序和未知扩展字段。
- 正常用户输入、附件、tool result 继续从 canonical content 重建，不在 replay 中复制一份。
- max-token continuation 由 ACN 插入的内部 assistant partial 与 user continuation 必须保存在同一段有序 replay，避免下一轮只看到合并文本却丢失真实 provider 历史。
- matching replay 进入 Anthropic 请求时替代该 canonical assistant message 的协议投影，不能把 raw 与 canonical 重复发送。
- replay 与 canonical message 在同一条 `messages.jsonl` 记录中原子提交；不建立 sidecar，也不把 raw replay 复制到 `turn_events.jsonl`。

建议目标形态：

```json
{
  "role": "assistant",
  "content": [
    {"type": "text", "text": "最终回答"}
  ],
  "provider_replay": {
    "protocol": "anthropic_messages",
    "model": "example-model",
    "messages": [
      {
        "role": "assistant",
        "content": [
          {"type": "thinking", "thinking": "...", "signature": "..."},
          {"type": "text", "text": "最终回答"}
        ]
      }
    ]
  }
}
```

字段名称允许在实现阶段根据现有 serde 结构做等价微调，但 wire protocol tag、model、原始顺序与“不重复发送”的语义不能改变。

### 4.4 Streaming 与 non-streaming 同等支持

- non-streaming 直接从完整 `content` 数组取得 raw blocks，并与 canonical text/tool 投影共同产出。
- streaming reducer 从 `content_block_start`、各类 delta、`content_block_stop` 与 `message_stop` 组装同样的 raw blocks。
- `thinking_delta` 只写入 raw block，不产生 TUI text delta；`signature_delta` 必须附着到对应 block。
- `redacted_thinking` 即使没有可读文本也必须原样保留。
- block index 不连续、重复 start/stop、非法 tool JSON、缺 `message_stop`、损坏 JSON/UTF-8 或未知的结构性 delta 继续作为协议错误，不提交 partial replay。
- streaming 与 non-streaming 最终进入同一完成响应校验与 canonical/replay reducer，避免两条路径字段漂移。
- streaming 已经输出可见文本后失败，继续走现有 non-streaming fallback；fallback 成功只提交最终完整 response 的 replay，失败 streaming 的 partial thinking/text/tool 不落盘。
- cancel/steer 不触发 provider fallback，也不提交 partial replay。

实现后的兼容性复核进一步明确字段校验边界：`usage` 等统计字段不作为提交门槛，`ping` 不参与 reducer；`message_start`、完整 block 生命周期、有效 `stop_reason` 与 `message_stop` 仍是完整终态的必要条件。结构性 delta 必须附着到已开始且未结束的 block，并严格满足 `text_delta → text`、`input_json_delta → tool_use`、`thinking_delta/signature_delta → thinking`。不能为了减少 fallback 而接受缺终态或类型错配的 partial，因为这可能把截断 text、损坏 tool input 或错误归类的私有 reasoning 提交到 canonical/replay。真实调用中的 fallback 频率单独以 `turn_events.jsonl` 验收，不以放松完整性校验换取表面上的 streaming 成功率。

独立复审后补充两条完成边界：已识别 delta 的对应载荷字段必须存在且为字符串，空字符串合法，但缺字段、`null` 或其他类型均是损坏流；尤其不能把缺失 `partial_json` 静默退化成 `{}` 后执行工具。`stop_reason` 按语义处理：`end_turn`/`stop_sequence` 是正常完成，`tool_use` 进入工具循环，`max_tokens` 沿用既有 continuation；`model_context_window_exceeded`、`pause_turn`、`refusal` 以及未知值都是明确的非成功终态，不提交 canonical/replay、不执行工具。此类终态不是 transport streaming 故障，已出现可见 text delta 时也不自动改用 non-streaming 重放，避免对确定性的拒绝、暂停或上下文截断连续发起五次相同请求；如果先因独立的 transport 故障进入 fallback，而某次 non-streaming attempt 才返回上述确定性终态，则记录该次失败后立即停止，不再继续剩余 attempts。当前 turn 返回清晰错误，之后 resume 仍只恢复此前 committed 历史。

上述段落描述阶段 0–10 已实现的当前行为。阶段 11–14 完成后，只有 `model_context_window_exceeded` 会按 4.15 提升为可恢复的 provider stop；`pause_turn`、`refusal` 与未知值仍保持确定性终态错误，并继续禁止 non-streaming fallback。

### 4.5 工具调用与原样回传

- assistant `thinking`/`redacted_thinking`、`text` 和 `tool_use` 的顺序按上游原样保存。
- 当 assistant turn 包含 tool use 时，下一请求必须先放回完整、未修改的 assistant blocks，再发送对应 canonical `tool_result`。
- 不能只保留 `thinking` 文本、只保留 signature、重新构造 block 或按类型排序。
- 一个 assistant turn 内多次工具调用和 interleaved thinking 使用同一 replay 链；只有完整 block 才能触发工具执行。
- 未完成、损坏或 fallback 失败的 raw tool block 不执行工具，也不进入 replay。
- Reasoning 本身不作为新的 tool 或 canonical content 类型。

### 4.6 统一 replay identity：协议 + model

本项覆盖 `PRD_openai_responses.md` 中“Responses replay 只按协议、不保存 model”的旧决策。发生冲突时以本文为准；实现完成时同步更新旧 PRD 的当前行为说明。

- Responses 与 Anthropic replay 都保存生成它的配置 model 字符串。
- runtime replay identity 为 `(wire protocol, exact model string)`。
- model 使用配置原值精确比较，不做大小写、版本号、日期、厂商前缀、别名或 model family 归一。
- endpoint 不进入 identity，不增加 endpoint hash/fingerprint。
- 当前 adapter 只消费 identity 匹配且位于当前连续 replay 世代中的 replay；其余 message 使用 canonical 投影。
- identity 不匹配不会重写或删除本地 JSONL，也不会自动重试或向用户伪装成同协议 replay 成功。

OpenAI 官方要求在 `store = false` 时保存并回传 reasoning item，但没有承诺 encrypted reasoning 跨 model 可移植；对 Responses 采用 model 绑定是保守工程边界，不冒充 OpenAI 的明文协议要求。Anthropic 则明确说明 thinking block 与生成它的 model 绑定，并建议模型切换时移除旧 thinking。

### 4.7 连续 replay 世代与 A → B → A

仅逐条比较 model 会让第一次 A 生成的 replay 在 A → B → A 后重新匹配。为落实“切换即清理 provider reasoning 上下文”，本期定义连续 replay 世代：

- provider history 从最近历史向前寻找；assistant message 的 replay identity 不匹配，或 assistant message 没有当前 identity replay 时，构成当前世代边界。
- 边界之前的 replay 一律不进入本次 provider 请求，即使更早存在相同 model 的 replay。
- A → B 的第一次 B 请求不带 A replay；B 成功后建立新的 B 世代。
- B → A 的第一次 A 请求不复活第一次 A 世代；A 成功后建立新的 A 世代。
- user message、tool result 本身没有 assistant replay，不单独切断世代；它们仍按 canonical 协议位置参与请求。
- 原始 replay 保留在私有 session 中用于历史事实与审计，但不再进入 active provider projection。
- compacted prefix 天然终止旧 replay 世代；summary 之后只从未 compact tail 建立当前世代。

### 4.8 旧 session 与持久化兼容

- 旧 session 没有 `provider_replay` 时继续按 canonical content 读取。
- 当前分支已经产生但没有 model 的 Responses replay 不做迁移、不猜测生成 model；session 仍可读取，该 replay 视为 unbound，只走 canonical 投影。
- 新写入的 Responses 与 Anthropic replay 必须携带 model。
- serde 兼容只服务于“旧数据可读并安全降级”，不反向补写或修改历史 JSONL。
- protocol/model/generation 过滤只影响 provider request 和对应 token estimate，不影响普通 transcript 与 TUI history。

### 4.9 Compaction、token 与 Reasoning 边界

Reasoning 不进入 compaction summary 的可读输入，也不会被压缩成摘要内容：

- compaction、recap、delegation summary、Memory、session search、claim 与审计预览统一使用 safe canonical projection，始终去掉 provider replay。
- active provider history 在未 compact tail 中仍携带 matching replay，确保真实模型上下文连续。
- 选择 compact prefix 后，该 prefix 的 replay 不再发送；compaction summary 不生成伪造的 thinking/reasoning item。
- 本地 context/token 预算必须考虑当前 adapter 实际会发送的 matching replay，不能只数 canonical text。
- 同一 assistant message 同时有 canonical 与 replay 时，估算不得把同一 text/tool 语义简单双算；沿用“实际协议投影与 canonical 估算取较大值”的保守策略。
- unbound、identity mismatch、旧世代或其他协议 replay 不进入当前 provider request，也不计入当前 provider replay 预算。
- compaction cursor/hash 必须包含 replay identity 与 raw 状态对消息身份的影响，避免 canonical 相同但 provider 状态不同的 suffix 被误判相同。

### 4.10 隐私、日志与错误

- raw thinking、signature、redacted `data`、Responses `encrypted_content` 只允许出现在 agent 私有 `messages.jsonl` 的 replay 中。
- TUI 错误、retry/fallback journal、普通日志、panic/context、测试 snapshot 与 code-review 输出不能打印完整 reasoning、signature、data、附件 base64、system prompt 或用户请求体。
- Anthropic 协议错误保留状态码、错误码与不含 payload 的诊断信息；若上游错误体回显 request/content/thinking/signature/data，必须递归脱敏。
- partial、失败、取消、超时或校验失败的 reasoning 不落盘。
- 不把可读 thinking 当作用户最终回答，因此它不会被 Memory/claim/Router 消费。

### 4.11 跨协议 resume

- `openai_responses`、`anthropic`、`openai_chat` 之间切换时，canonical Text/ToolUse/ToolResult 继续提供语义级历史。
- 只允许当前 `(protocol, model)` 连续世代的 replay 进入请求；其他 replay 不转换、不发送。
- 跨协议后切回原协议会建立新的 replay 世代，不复活切换前的 opaque reasoning。
- `openai_responses`、`anthropic`、`openai_chat` 都将未 compact 的历史 Image/Document 原样投影为各自协议的真实媒体块；下一轮和 resume 不要求用户重新附加。
- compacted prefix 仍只发送摘要，不重新发送该前缀的原始附件；未 compact suffix 继续保留真实媒体。
- 协议切换不删除 session 数据，但不承诺 opaque reasoning 跨协议连续。

### 4.12 TUI、Router 与 Chat 边界

- 本期不修改 TUI event、cell、timeline 或渲染。thinking delta 不显示，最终 text/tool/fallback 继续走现有展示。
- Router rerank 维持单次无状态调用：不请求、不保存、不回传 Reasoning。
- OpenAI Chat 继续丢弃 Reasoning 兼容字段；只补用户文档说明，不修改 adapter。
- Chat SSE 的异常恢复盲区在本需求完成并稳定后另立 P2 hardening；不与 Anthropic replay 绑定实施。

### 4.13 实施补充：Anthropic 工具 schema 顶层兼容

真实 Anthropic-compatible TUI 验收发现，目标 endpoint 会拒绝工具 `input_schema` 顶层的 `oneOf`、`anyOf` 或 `allOf`。ACN 内置 `write_stdin` 原先用顶层 `allOf` 重复表达 stdout/stderr cursor 必须成对提供，而执行层已经对该约束做严格校验。

- 移除 `write_stdin` schema 顶层的组合关键字，保留字段说明、`type = object`、required 与 additionalProperties 约束。
- cursor 成对约束继续由执行层强制，不改变工具权限、调用语义或失败行为。
- 不为 endpoint 猜测或递归改写任意第三方 MCP schema；本次只消除已确认且有执行期等价校验的内置冗余约束。
- 增加回归测试，确保该内置 schema 不再引入顶层 `oneOf`、`anyOf`、`allOf`。

该补充只解除真实 Anthropic TUI 请求的 schema 兼容阻塞，不修改 4.1–4.12 已拍板的 Reasoning 语义。

### 4.14 实施补充：Responses 无状态加密 reasoning 请求

全分支 review 对照官方 Responses 无状态流程发现：Agent 固定使用 `store = false`，若要确保上游返回可供下一轮、工具回环和 resume 使用的加密 reasoning 状态，请求必须显式包含 `include = ["reasoning.encrypted_content"]`。

- Agent Responses 的 streaming、non-streaming fallback、工具回环与 max-token continuation 统一携带该 `include`。
- 返回的 reasoning item 仍按 4.1、4.6–4.11 的边界原样保存、按 identity/连续世代回传且不展示。
- Router Responses rerank 是单次无状态排序，不保存或回传 reasoning，因此继续省略 `include` 与 `reasoning`。
- 不增加用户 TOML 开关，避免允许配置出 `store = false` 却拿不到可重放 reasoning 的不完整 Agent 语义。

该补充修复已拍板“Responses Reasoning 保存并在下轮原样回传”的请求侧必要条件，不改变 replay identity、隐私或 TUI 边界。

### 4.15 后续增量：`model_context_window_exceeded` 自动恢复（阶段 11–14）

Anthropic 将 `model_context_window_exceeded` 定义为成功响应中的有效但被模型上下文窗口截断的内容，而不是 HTTP、transport 或 SSE 损坏。它与 `max_tokens` 都需要保留 partial，但不能共用同一恢复动作：`max_tokens` 通常可以直接续写；上下文窗口已满时，追加 assistant partial 和内部 continuation 只会让下一请求更大，必须先释放上下文空间。

已确认的目标语义：

1. 新增独立的 `ProviderStop::ContextWindowExceeded`，不再把该 stop reason 包装为 `ProviderTerminalFailure`，也不映射为普通 `MaxTokens`。
2. streaming 与 non-streaming 都把已经完整组装、校验通过的 assistant blocks 作为有效 partial 返回；SSE 缺终态、损坏 block、非法 tool JSON 等协议错误仍不进入此恢复路径。
3. 该 stop 是完整 provider 响应，不触发 streaming → non-streaming fallback；如果它出现在已经开始的 non-streaming fallback attempt 中，记录该 attempt 后立即切换到上下文恢复或按恢复条件报错，不继续剩余 fallback attempts。
4. partial canonical text、完整 Anthropic replay、内部 continuation 与恢复计数只存在于当前 turn 的 in-flight 状态；最终成功前不写入 canonical `messages.jsonl`。
5. 下一次 provider request 必须以显式 `ContextWindowExceeded` preflight 原因进入现有 active-turn compactor，绕过普通 ratio 触发判断，但继续使用既有 `tail_target_ctx_ratio`、`tail_hard_ctx_ratio`、安全投影、Reasoning 排除和 checkpoint 机制。
6. 最近一次被截断的 assistant raw blocks 及紧随其后的内部 continuation 必须作为未 compact tail 原样保留；不能把需要回传的 thinking/signature/redacted data 改写进 summary，也不能生成伪造 reasoning。
7. forced preflight 没有可压缩范围、压缩后请求没有实际缩小、投影仍超 hard budget、summary 失败或恢复次数耗尽时，本轮返回清晰错误，不以原请求继续碰撞上游；异步 Recap 失败不阻断 context recovery。
8. 最终获得 `Done` 后才合并 canonical assistant text、完整 provider replay 与本轮工具历史，并通过既有 commit gate 一次性提交；恢复失败、取消或进程退出后，resume 仍只恢复此前 committed 历史。
9. forced compaction 若覆盖此前 committed history，summary 独立提交并异步投递 Supervisor Recap；recap 只读取已经落盘的 committed `session_messages`。若只压缩当前 active turn，则只生成 active-turn summary，不投递 recap。
10. 当前 user、partial assistant、内部 continuation 与 Reasoning 均不进入 recap。即使后续恢复失败，已经成功完成的 committed-history compaction/recap 可以保留；失败 turn 的 active compaction 必须按现有收束逻辑清理。
11. 不新增 Reasoning TUI 展示、不把 partial thinking 作为 text delta；用户可见 text、工具和既有 compaction 状态事件继续复用当前 TUI 管线。

该增量优先复用现有 `PreflightCompactor`、active-turn summary、provider replay 和整轮 commit gate，不新建第二套 session schema、手工 `/compact` 路径或 adapter 内部 compaction。P4B、P5B、P6A 已完成拍板，阶段 11–14 按本文语义实施。

实现补充（不改变既有拍板）：

- turn loop 收到第一次 context-stop 的完整响应时，立即把该 assistant 交给 preflight 建立稳定恢复起点，早于 delegation steering、background projection 或其他 runtime message 的下一轮注入。普通截断从 `assistant partial + internal continuation` 的 assistant 起始，带工具截断从不可拆分的 `assistant tool_use + tool_result` 起始。该起点建立后，同一 logical turn 后续的普通 ratio-triggered 与再次 forced compaction 都持续保护从该 assistant 到当前尾部的完整恢复链，避免较早 partial 被压缩后又由最终 provider replay 重新带回。恢复开始前的普通 compaction 不启用该额外保护。
- 受保护恢复链按 raw provider/canonical 形态参与下一请求与 token/hard-budget 计算，不经过 `tool_result_raw_max_chars`、媒体/附件 externalization 或其他有损投影；选取更早保留 segment 时先把这段 raw mandatory tail 完整扣除。若 raw 链本身放不下，则按“无安全恢复空间”失败，不先发送省略内容、再在最终 session 中恢复全文。
- context continuation 的完成事件和 non-streaming replacement 使用“此前已接受 partial + 当前响应”的去重合并文本，保证 TUI 最终完成时不会把已经显示的前半段覆盖掉；Reasoning 仍不显示。
- Agent 与 delegation preflight 都实现相同的显式 recovery 请求、禁用语义、无安全范围失败和无缩减失败；Memory review、Router 等没有 session compactor 的单次调用路径返回清晰错误，不伪造恢复。
- 不新增 TOML 字段；恢复上限固定为每个逻辑 user turn 2 次，与普通 max-token continuation 独立。
- 全分支 review 同步明确 Responses 确定性终态：`response.failed` 与非 `max_output_tokens` 的 `response.incomplete` 包装为 provider terminal failure；即使此前已有可见 delta，也不触发或继续 non-streaming fallback。`max_output_tokens` 仍沿用既有 continuation。

## 5. 开工前拍板（均已完成）

P1A、P2A、P3A 是主需求开工前完成的拍板；阶段 11–14 的 `model_context_window_exceeded` 自动恢复使用 P4B、P5B、P6A。以下选择均已完成，不再存在待拍板项。

### P1A（已拍板）. 用户可见 TOML 字段命名

公开字段选择 Anthropic 前缀的扁平命名：

```toml
anthropic_thinking = "auto" # auto | enabled | adaptive | disabled
anthropic_thinking_budget_tokens = 4096
```

- `auto` 表示不发送 `thinking`，由 endpoint 决定默认行为。
- 字段明确只作用于 Anthropic，不让 Responses/Chat 用户误以为共用同一 wire 字段。
- 保持当前扁平 `LlmChatConfig`，不为两个字段增加 provider 专属子表。

### P2A（已拍板）. 不支持旧式 interleaved-thinking beta header

部分旧一代 Anthropic manual extended-thinking model 需要版本化 beta header 才能在工具调用之间产生 interleaved thinking；当前 adaptive thinking model 不需要，DeepSeek 明确忽略 `anthropic-beta`，其他兼容 endpoint 的行为也不一致。

本期不新增 beta header 配置，也不自动发送：

- 支持 endpoint 正常返回的 thinking/tool blocks，并保证原样 replay。
- 旧模型即使不产生工具间 thinking，普通 thinking 与 tool replay 仍可工作。
- 避免把短生命周期 beta 名称写死在通用配置和请求层。
- 若以后真实目标模型必须依赖该 header，以真实 endpoint 证据单独增加精确配置，不在本期预埋。

### P3A（已拍板）. 完整终态只有 Reasoning、没有 text/tool

可能出现一个结构完整、`stop_reason = end_turn`，但 content 只有 `thinking`/`redacted_thinking` 的 response。由于本期不展示 Reasoning，这种 response 对用户没有可见结果。

返回明确的“没有可消费输出”错误，不提交 canonical session：

- 防止 TUI 出现空成功。
- raw replay 也不落盘，用户可重试或调整 model/config。
- 失败 turn 仍按既有 turn journal 与 resume 恢复边界保留，不等于从 session 审计事实中彻底删除。

`thinking + tool_use` 属于可消费工具响应，不受此规则影响；`stop_reason = max_tokens` 继续先走既有 continuation，只有最终完整终态仍无 text/tool 才报错。

### P4B（已拍板）. 用户显式关闭 auto compact 时是否允许 provider 强制恢复

- A：即使 `auto_compact_ctx_ratio = 0.0`，收到 `model_context_window_exceeded` 也强制 compact。
- B：尊重 `0.0` 的禁用语义，返回“上下文窗口耗尽且自动压缩已关闭”；只有配置大于 `0.0` 时才允许 provider stop 绕过 ratio 阈值。
- C：新增独立 TOML 开关控制 provider-triggered compaction。

推荐 B。`0.0` 是用户对自动 summary/recap 的明确关闭，不应被 provider 终态静默推翻；新增开关会扩大配置面，而启用 auto compact 后绕过估算阈值已经足够表达恢复意图。

选择 B。

### P5B（已拍板）. 单个逻辑 turn 的上下文恢复次数上限

- A：最多恢复 1 次；再次耗尽立即失败。
- B：使用独立固定上限 2 次，不与 `MAX_CONTINUATION_TURNS` 共用。
- C：复用普通 max-token continuation 的 8 次上限。
- D：新增 TOML 配置。

推荐 B。一次 compact 通常足以从 soft target 重新获得大段窗口；第二次为异常长输出保留合理恢复空间。8 次可能产生高延迟和多次有损 summary，新增用户配置则没有足够现实需求。计数按单个 ACN 逻辑 user turn 累计，工具回环和普通 max-token continuation 使用各自现有边界。

选择 B。

### P6A（已拍板）. 截断响应中已经存在完整 `tool_use` 时如何处理

- A：完整且通过校验的 `tool_use` 继续进入既有工具循环；assistant raw blocks 与 tool result 保存在当前 in-flight turn，下一次 provider request 先执行强制 preflight compact。残缺或非法 tool block 仍是协议错误。
- B：不执行工具，忽略 tool block 并追加普通 continuation。
- C：只要该 stop reason 同时包含 `tool_use` 就立即失败。

推荐 A。Anthropic 消息协议要求完整 `tool_use` 后紧随对应 `tool_result`，不能插入普通 continuation；该行为也与现有 `max_tokens + 完整 tool_use` 的处理一致。工具和结果仍受整轮 canonical commit gate 约束，不会把 partial session 当作成功提交。

选择 A。

## 6. 数据与请求投影

### 6.1 Provider replay 结构

目标 Rust 语义：

```rust
enum ProviderReplayState {
    OpenAiResponses {
        model: Option<String>,
        items: Vec<serde_json::Value>,
    },
    AnthropicMessages {
        model: String,
        messages: Vec<serde_json::Value>,
    },
}
```

- Responses 的 `Option<String>` 仅用于读取当前分支已有的 unbound 本地 session；新写入必须是 `Some`。实现也可使用等价的 serde default 结构，只要不会把缺 model 数据误绑定到当前 model。
- Anthropic `messages` 保存一次逻辑 assistant 产出的 provider-private 有序消息；普通 user/tool result 不重复保存。
- protocol tag 使用稳定 wire 名称，不跟随 Rust 模块名或公开 provider alias 改动。

### 6.2 Canonical 与 replay 双投影

```text
messages.jsonl
  └─ SessionMessage
      ├─ canonical content
      │   ├─ TUI / transcript / search
      │   ├─ Memory / recap / claim safe projection
      │   ├─ compaction summary input
      │   └─ identity mismatch 时的跨协议语义历史
      └─ provider_replay
          ├─ protocol + model
          ├─ 当前连续世代的 provider request
          └─ 当前 provider context/token 估算
```

matching replay 与 canonical 不能同时发给同一个 provider 形成重复 assistant 内容。canonical 始终保留，确保跨协议、旧数据、安全摘要与用户可读历史不依赖 opaque block。

### 6.3 Anthropic 请求顺序

对于含 thinking 的工具回环，逻辑顺序必须保持：

```text
user canonical message
assistant raw replay: thinking/redacted_thinking + text? + tool_use
user canonical tool_result
assistant raw replay: thinking? + text/tool_use
...
```

若某条 assistant replay identity 不匹配或位于旧世代，则该条 assistant 从 canonical Text/ToolUse 重建，并省略其旧 thinking；后续 user/tool result 仍保持合法 Anthropic 消息顺序。

## 7. 分阶段实施与阶段验收

进入每一阶段前重新核对本文决策。实现过程中允许追加不冲突的协议细节，但不得用追加项推翻或架空已有拍板。

### 阶段 0：基线、协议 fixture 与影响面（已完成）

实施：

1. 记录 worktree、branch、`origin/main` 基线和已有 dirty changes，保留用户改动。
2. 固化 Anthropic non-streaming、SSE、工具回环、max-token continuation、fallback、cancel 与 Responses replay 的现有回归。
3. 增加最小真实协议 fixture：thinking + text、redacted + text、thinking + tool_use、interleaved tool、thinking-only、signature/data 不同字段形状、非法 delta/index/终态。
4. 列出 session、transcript projection、compaction/token、bootstrap/config、文档与测试影响面。

阶段验收：

- 基线定向测试通过。
- fixture 不包含真实 key、私有 endpoint、真实用户名、附件 base64 或真实 reasoning payload。
- 明确区分官方协议规则、兼容 endpoint 差异与本地工程策略。

### 阶段 1：统一 replay identity 与连续世代（已完成）

实施：

1. 扩展 provider replay protocol/identity，让 adapter 暴露 protocol + exact model。
2. 为 Responses 新写入 replay 增加 model；旧缺 model 数据安全降级为 canonical。
3. 实现连续 replay 世代投影，覆盖 A → B、B → A、A → B → A、协议切换与无 replay assistant 边界。
4. 更新 session provider history 与 turn loop 传递接口。
5. 同步 token/compaction projection，只计算当前 identity、当前世代实际会发送的 replay。

阶段验收：

- Responses 同 model replay 行为不变。
- Responses/Anthropic model mismatch 都不发送 raw replay。
- A → B → A 不复活第一次 A replay。
- 旧 unbound Responses session 可读、canonical 可用、raw replay 不发送。
- identity mismatch 不删除或重写 JSONL。

### 阶段 2：Anthropic 请求配置（已完成）

实施：

1. 按 P1 最终拍板增加 thinking type 与可选 budget 配置、serde 校验、默认值和文档。
2. 保留 `reasoning_effort -> output_config.effort`。
3. streaming 与 non-streaming 共用同一 request DTO。
4. 不做 model-name heuristic、400 自动改字段或 endpoint 专属分支。
5. 按 P2 最终拍板处理或明确不处理 beta header。

阶段验收：

- auto/省略、enabled、adaptive、disabled、可选 budget 与 effort 有完整 request snapshot。
- `budget_tokens` 不从其他字段推导；不适用时不发送。
- stream true/false 除 transport 字段外请求语义一致。
- 公开配置文档明确哪些字段只作用于 Anthropic。

### 阶段 3：Anthropic 完成响应与 raw replay（已完成）

实施：

1. non-streaming 完整保存 assistant content blocks。
2. canonical reducer 继续投影 text/tool，不把 thinking 投影为 Text。
3. 新增 Anthropic replay，并附带当前 model。
4. 未知非 actionable block 与已知 thinking block保留 raw；无法安全消费的 actionable-only 输出返回明确错误。
5. 按 P3 最终拍板处理 thinking-only 完整终态。

阶段验收：

- thinking/signature、redacted data、text、tool_use 顺序和未知字段 round-trip 不变。
- canonical text/tool 与 replay 同时原子落盘。
- TUI/transcript/search/Memory 不出现 thinking、signature 或 raw JSON。
- 失败、partial 与非法 response 不写 replay、不执行工具。

### 阶段 4：Anthropic SSE reducer（已完成）

实施：

1. 把现有 thinking/signature delta 组装纳入完成 replay。
2. 校验 block 生命周期、index、delta 类型、tool input JSON、message terminal 与 stop reason。
3. streaming/non-streaming 归一到阶段 3 的完成 reducer。
4. 保持可见 text delta、fallback、cancel/steer 与 commit gate。

阶段验收：

- 合法 SSE 与等价 non-streaming JSON 生成相同 canonical/replay 语义。
- thinking delta 从不进入 TUI；text delta 保持原行为。
- 缺终态、损坏帧、非法 UTF-8、重复/不连续 block 与残缺 tool input 不提交。
- fallback 成功只保存 fallback 完整 replay；失败 streaming raw 不混入。

### 阶段 5：工具、continuation、resume 与落盘（已完成）

实施：

1. 工具结果请求原样回放完整 assistant blocks。
2. 支持多工具与 interleaved thinking 的有序 replay。
3. 把 Anthropic max-token continuation 的 provider-private assistant/user 序列保存到最终 replay。
4. 验证普通下一轮、退出后 resume、supervisor job 与 delegation 继承主 provider/model 时的 session 行为。
5. 确保 protocol/model 切换使用 canonical，并建立新世代。

阶段验收：

- thinking + tool_use 缺失/修改/重排会被测试捕获。
- max-token continuation 后下一轮看到完整真实 provider 序列，不只看到合并文本。
- resume 前后请求 JSON 等价。
- model/protocol 切换无 opaque reasoning 串线。

### 阶段 6：Compaction、隐私与安全投影（已完成）

实施：

1. 更新 compaction cursor/hash 与 token estimate。
2. 保证 compaction summary、recap、Memory、search、claim、Router/Maintainer 都只消费 safe canonical projection。
3. compacted prefix 停止 replay raw block；未 compact tail 保留当前世代 replay。
4. 扩展 Anthropic error/body/log redaction，覆盖 thinking、signature、redacted data 与请求回显。

阶段验收：

- compaction prompt、journal、TUI error、普通日志中没有 raw reasoning/signature/data。
- 当前世代 replay 会计入 provider context 预算，mismatch/旧世代不计入。
- compact 后请求只发送 summary + 未 compact tail，不生成伪造 thinking。
- canonical 与 replay 不双算同一可见输出。

### 阶段 7：文档与自动化验证（已完成）

实施：

1. 更新 README、用户指南、配置参数与示例，明确 Chat/Responses/Anthropic Reasoning 边界。
2. 同步修订 `PRD_openai_responses.md` 中已被本文覆盖的 model/replay 行为。
3. 运行格式化、定向测试、相关集成测试、Clippy、全量 tests 与 type check。
4. 本期不修改版本号，因此不执行 `scripts/check_version_consistency.sh`。

阶段验收：

- 文档不存在“Responses 只按协议 replay”与“协议 + model 世代”相互矛盾。
- 配置示例不含真实 endpoint、key、用户路径或内部域名。
- 所有自动化验证通过；失败项必须分类为本次回归、基线问题或环境问题并给出证据。

### 阶段 8：真实 LLM TUI smoke test（已完成）

必须按 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md` 使用真实 `acn` TUI、真实 LLM endpoint 与新建 session，不允许用 fake provider、预录响应、直接调用 manager 或 smoke 脚本冒充 TUI 验收。

至少覆盖：

1. Anthropic streaming 短文本，确认 TUI 只显示最终 text、不显示 thinking。
2. 同 session 下一轮，检查上游请求或私有落盘证据证明上一轮 raw thinking 原样回传。
3. 真实工具调用与 tool result 回环，确认 thinking/signature 与 tool_use 顺序完整、工具只执行一次。
4. 退出后 resume 同 session，再发一轮并验证 replay 连续。
5. 新建 session 验证较长文本/多 delta，不发生无故 non-streaming fallback。
6. 使用可控配置或另一个真实模型验证 model 切换后不回传旧 reasoning；切回时不复活旧世代。
7. 对已有 Responses 能力做真实 TUI 回归：streaming、下一轮 replay、resume，确认 identity 重构没有退化。
8. 检查 `messages.jsonl`、`turn_events.jsonl`、stderr 与 supervisor/job 状态；不在报告中粘贴真实 reasoning、signature、key、私有 endpoint 或附件 base64。

真实 endpoint 无法稳定触发 fallback、redacted block 或 max-token continuation 时，不能伪造成功；这些边界由本地 deterministic fixture 验收，并在最终报告中说明真实 smoke 覆盖与未覆盖项。

阶段验收：

- 新建 session 的 Anthropic streaming、multi-turn、tool、resume 全部成功。
- TUI fallback 次数符合真实事件，不把终态成功误判为失败。
- raw reasoning 只出现在私有 session replay，用户可见区域和 journal 不泄漏。
- Responses 真实回归通过。

实施证据：Anthropic-compatible 隔离新会话 `session_2139344f` 使用支持 thinking 的模型完成 4 个 committed 逻辑 turn，覆盖需要推理的 streaming、多轮 replay、一次真实 `file_read` 工具回环、关闭后 resume；共落盘 5 条绑定 `anthropic_messages + 精确 model` 的 assistant replay，结构检查得到 5 个 `thinking` block 且全部携带 signature，包含 `thinking + tool_use` 与随后 `thinking + text`，fallback 为 0，两个 TUI stderr 均为空。检查只统计 block 类型和签名存在性，不输出 raw thinking/signature；TUI 未渲染 raw thinking。另一个 Anthropic-compatible 模型完成 4 个 committed turn，验证未返回 thinking 时的普通兼容路径。thinking/signature/redacted、JSON/SSE、continuation 与脱敏同时由 deterministic HTTP/SSE fixture 验收。Responses 隔离新会话完成 6 个 committed turn，真实产生 reasoning item、下一轮 replay 成功，并完成 model A → B → A，fallback 为 0、stderr 为空。相关 tmux 与 supervisor 均已清理。

### 阶段 9：针对性 code-review 与修复复验（已完成）

完成真实 LLM TUI smoke 后，使用仓库 `code-review` skill 对以下范围做针对性 review：

- Anthropic request/JSON/SSE reducer；
- replay schema、identity、连续世代；
- tool/max-token continuation；
- session resume、compaction/token projection；
- redaction、日志与持久化；
- Responses 回归与配置文档。

review 必须包含本地多轮审查和独立只读 reviewer。对真实存在、可触发、与本需求相关的 P0/P1 全部修复；不得为了理论上基本不存在的极端假设引入虚空防御、厂商猜测或大范围架构改造。P2/P3 记录影响与是否建议后续处理。

修复后必须重新运行：

1. 对应定向测试与新增回归；
2. 格式化、Clippy、相关集成测试；
3. 受修复影响的真实 LLM TUI 场景；
4. 同一范围再次 targeted review。

只有 targeted review 不再存在未处理 P0/P1，且复验通过，才能进入全量 review。

实施证据：本地审查先修复 `thinking + 空白 text` 被误作空成功的完成边界。独立 targeted reviewer 随后发现 Anthropic 错误体可能通过请求型 `input/system/content` 回显用户输入、system prompt 或工具参数；修复结构化 JSON、嵌套 JSON 字符串和非 JSON 脱敏后，复审继续发现无引号 `key: value`/`key = value` 及带引号混合大小写 key 的确定性旁路。最终实现让结构化与字符串扫描共用敏感键集合，覆盖 quoted/escaped/unquoted、冒号/等号与 ASCII 大小写，同时用词边界保留 `invalid_input` 等非字段诊断；status/code/message/parameter 等安全信息仍可保留。相关回归全部通过。review 同时发现旧 Responses PRD 仍保留“只按协议、切回后复活 replay”的旧说明，已同步为协议 + 精确 model + 连续世代。最终 targeted 独立复审结论为“没有发现未处理的 P0/P1”，JSON/SSE、工具、continuation、resume、compaction/token、Responses 回归和内置工具 schema 均无新增本期问题。

### 阶段 10：全量 code-review 与最终验收（已完成）

使用 `code-review` skill 审查当前分支相对 `origin/main` 的全部改动，而不是只审 Anthropic 新文件。覆盖已有 Responses、Router Responses 与本期 Reasoning/identity 改造的组合行为。

执行门槛：

1. 本地全量 review 与独立只读 reviewer 都完成。
2. 修复所有真实、值得修复的 P0/P1；不做无证据的兼容分支和虚空防御。
3. 修复后重新运行全量自动化验证。
4. 重新运行受影响的真实 LLM TUI smoke；若修改触及 replay/stream/tool/compaction，则重跑对应完整场景。
5. 再次执行全量 review。
6. 重复“修复 → 验收 → review”，直到全量 review 没有未处理 P0/P1、没有仍影响本需求正确性的 actionable finding，且所有验收通过。P2/P3 若不修复，必须有可复核的非问题、低现实概率或明确不在本期范围的证据，不能用降级定级代替处理。

最终验收报告必须包含：

- 各阶段完成状态与主要证据；
- 最终配置字段和请求映射；
- Anthropic/Responses replay 落盘示例的脱敏结构；
- streaming/non-streaming/tool/continuation/resume/compaction/model-switch 自动化结果；
- 真实 LLM TUI 使用的 provider/model 类型、session ID、场景和结果，但不披露 key、私有 endpoint 或 reasoning payload；
- targeted review、全量 review 的发现、修复与复验结论；
- 全量 fmt/Clippy/test/check 结果；
- 明确列出仍不在本期范围内的 TUI Reasoning 展示、OpenAI Chat Reasoning、Chat SSE hardening、WebSocket 与 Router Reasoning。

实施证据：首次全分支独立 review 覆盖相对 `origin/main` 的两个 Responses/Router Responses 提交、全部 Anthropic Reasoning 工作区改动与未跟踪 PRD，发现 1 个现实 P1：Agent 固定 `store = false`，但没有请求 `include = ["reasoning.encrypted_content"]`，因此官方无状态 reasoning endpoint 不保证返回可 replay 的加密状态。对照官方文档修复共享 request DTO 后，Agent 所有 transport、fallback、工具与 continuation 请求固定携带 include，Router 保持省略；请求快照、max-token 双轮请求与 Router fake server 回归通过。真实 Responses 新会话 `session_f3c63026` 完成 2 个 committed turn，两个 reasoning item 都具有非空 `encrypted_content`，第二轮 replay 被上游接受，fallback 为 0，stderr 为空。修复后完整验证再次通过 fmt、Clippy `-D warnings`、2045 个库测试、全部二进制/集成测试、doc tests、check 与 diff check；按约定未运行版本一致性脚本。最终全分支独立复审结论为“没有发现未处理的 P0/P1”。

完成后的 Anthropic streaming 兼容性复核继续对照成熟客户端的 block/delta reducer 边界，并用真实 TUI 统计自动 fallback。保留了 block 必须存在、delta 与 block 类型必须匹配、完整终态必须可证明等必要校验；没有照搬只适用于特定 SDK 对象变更或会接受缺失终态的宽松行为。独立 reviewer 在这一轮发现并修复 3 个现实 P1：已知 delta 的必需载荷不能用空默认值掩盖损坏帧；拒绝、暂停、上下文截断和未知 `stop_reason` 不能提交为空成功，也不能因已有可见文本自动 fallback；若确定性终态出现在已经开始的 non-streaming fallback 中，必须记录当前 attempt 失败并立即停止剩余 attempts。两次全新真实会话合计完成 8 个 committed turn、37 个 streaming delta 和 2 次真实工具回环，覆盖长推理、多轮 replay、关闭后 resume 与历史读取，fallback 为 0、stderr 为空；thinking block 均只检查类型与签名存在性，不输出原文。最终完整验证通过 fmt、Clippy `-D warnings`、2051 个库测试、全部二进制/集成测试、doc tests、check 与 diff check；最终独立 targeted 复审结论为“未发现 P0/P1”。

保留 1 个不阻塞本期的 P2：共享 `LlmHttpError` 会保留 `reqwest::Error`，当用户把敏感 query/userinfo 直接写入 endpoint 且发生传输错误时，完整 URL 可能进入本地日志、journal 或 TUI。该边界相对 `origin/main` 未修改，Chat/Anthropic 已共同使用，本分支只让 Responses 复用；它不涉及认证 header、请求体或 reasoning 内容。后续应作为跨 provider HTTP error hardening 独立使用 `without_url()` 修复，不在本期扩大共享错误架构。

### 阶段 11：终态建模与 Anthropic partial 响应（已完成）

实施：

1. 在 provider-neutral stop 中增加独立 `ContextWindowExceeded`，同步所有 exhaustiveness 检查，但只由 Anthropic adapter 映射该 wire stop reason。
2. 统一 Anthropic JSON/SSE 完成 reducer，使该终态保留完整、已校验的 text/thinking/redacted/tool blocks 和 usage；协议损坏仍返回原错误。
3. 保证 streaming 已有可见 text 时不进入 non-streaming fallback，fallback attempt 收到该终态时停止剩余 attempts。
4. 增加 deterministic JSON/SSE fixture，覆盖 text partial、thinking + text、thinking-only partial、完整 tool_use、损坏 tool block 与未知 stop reason。

阶段验收：

- streaming/non-streaming 产生等价 partial canonical/replay；Reasoning 不显示、不泄漏到错误与 journal。
- `pause_turn`、`refusal` 和未知值行为不变。
- 该终态不提交空成功、不执行残缺工具、不触发五次 fallback。

### 阶段 12：forced preflight compaction、continuation 与 commit gate（已完成）

实施：

1. 为 turn-loop preflight 增加显式 provider context recovery 原因；普通自动 compact 继续按 ratio，恢复模式按 P4 最终拍板处理。
2. 建立独立 in-flight accumulator，保存 partial canonical text、Anthropic replay messages、内部 continuation、工具回环和 P5 恢复计数。
3. 复用 active-turn compactor，从第一次 context-stop assistant 建立稳定边界，持续保护其后的完整 partial/continuation/tool raw tail；无进展、超预算、重复耗尽和 compaction 失败均停止恢复。
4. 按已确认边界处理 committed summary 与异步 Recap、active-only summary；Reasoning 和未提交 turn 不进入 recap。
5. 按 P6 最终拍板处理完整 tool use；最终成功才把合并 canonical/replay 原子提交。

阶段验收：

- context stop → forced compact → continuation → Done 的完整路径只提交一轮 canonical user/assistant，replay 顺序完整。
- compaction 禁用、无可压缩历史、summary 失败、重复耗尽、取消和进程退出均不提交 partial turn；后台 Recap 失败不影响 summary 或 partial-turn gate。
- committed-history compaction/recap 可在后续 turn 失败时保留；active compaction 被清理，resume 只恢复旧 committed 历史。
- max-token continuation、普通工具循环和正常 preflight auto compact 不回归。

### 阶段 13：自动化、真实 LLM TUI 与回归验收（已完成）

1. 运行格式化、Clippy `-D warnings`、受影响单元/集成测试、全量 tests、doc tests 与 check；不修改版本号时不执行版本一致性脚本。
2. 使用本地 deterministic HTTP/SSE provider 稳定覆盖两次 context recovery、compaction 无进展、non-streaming 与工具边界，不以无法控制的真实模型输出冒充成功。
3. 按 `tui-smoke-test-with-tmux` skill 使用全新真实 session 验证 Anthropic streaming、thinking replay、多轮、工具、resume、正常 auto compact 与 fallback 计数；真实 endpoint 能稳定产生该 stop reason 时额外覆盖完整恢复，否则如实记录未覆盖，并以 deterministic fixture 验收该终态。
4. 检查 `messages.jsonl`、turn journal、compaction checkpoint/audit、recap、stderr 与 TUI，禁止输出 raw thinking/signature/data、key、私有 endpoint 或附件 base64。

阶段 13 实施证据：

- deterministic 覆盖 JSON/SSE context partial、thinking-only、完整工具、两次独立恢复上限、fallback attempt 中途转 recovery、`auto_compact_ctx_ratio = 0.0`、active-only summary、committed summary 与 recap 输入边界、最近 raw replay 尾段保护、无安全范围和最终可见文本合并。
- 完整验证通过：fmt check、all-targets/all-features Clippy `-D warnings`、2068 个 lib tests、全部 bin/integration/example tests 与 all-targets/all-features check；按约定未执行版本一致性脚本。
- 真实 LLM TUI 使用 Anthropic `claude-haiku-4-5` 与全新 `session_81bd9a92`，连续覆盖计算型 streaming、同会话 replay、真实 `file_read` 工具回环、工具后下一轮、关闭后 resume。结果为 5 个 committed turn、6 个 streaming delta、1 次完整工具调用、0 次 fallback；6 条 assistant replay 都有 Anthropic thinking block 与非空 signature，两份 stderr 均为空。只统计 block 类型和 signature 存在性，不输出 reasoning 原文。
- 真实模型没有稳定产生 `model_context_window_exceeded`，因此该确定性终态不冒充真实覆盖，继续由上述本地 JSON/SSE 与 SessionEngine fixture 验收。

### 阶段 14：针对性 review、全量 review 与最终复验（已完成）

1. 使用 `code-review` skill 对 Anthropic JSON/SSE、turn-loop state machine、forced compaction、tool/continuation、session commit/resume 和 recap 边界做 targeted review。
2. 修复全部真实、可触发且值得修复的 P0/P1，不做厂商猜测、无证据降级或虚空防御；修复后重跑定向测试与受影响真实 TUI，并再次 targeted review。
3. targeted review 通过后，对当前分支相对 `origin/main` 的全部改动执行全量 review；修复 P0/P1 后重复全量自动化、真实 TUI 与 review，直到没有未处理 P0/P1 或影响本需求正确性的 actionable finding。
4. 最终报告列出拍板、实现范围、deterministic/真实验证证据、fallback 次数、session/compaction/replay 结果和仍未真实触发的边界，不披露私有 payload。

阶段 14 实施证据：

- 本地 targeted review 首先发现连续两次恢复或中间工具轮次可能让较早 partial 被 active summary 覆盖、最终 replay 又重新带回；修复为从第一次 context-stop assistant 开始持续保护完整恢复链。独立 targeted reviewer 随后发现 2 个现实 P1：受保护的大工具结果仍可能被有损投影，以及 delegation steering 可能在下一次 preflight 建 marker 前成为最新尾段。修复为 stop 当下直接传递稳定 marker，且受保护链完全跳过 tool-result 截断和 heavy-block externalization，并按 raw 预算；新增 Agent、delegation、steering、media 与大工具结果回归。修复后的独立 targeted re-review 结论为没有剩余 P0/P1。
- 全分支 review 覆盖 Responses、Router Responses、Anthropic Reasoning/replay 与 context recovery 的组合改动，发现 2 个现实 P1：Responses 显式 failed/非 max-output incomplete 会在可见 delta 后误触发最多 5 次 fallback；Agent/delegation 选取旧 tail 时没有预先完整扣除 raw protected tail，可能多保留旧 segment 后在 hard-budget gate 错误失败。前者改为 provider terminal failure，后者在 planner 选段前把 raw protected tail 作为 mandatory budget；定向测试覆盖 failed stream、fallback 中的 content filter 与两条 compactor 的选段边界。修复后的独立全分支 re-review 结论为没有剩余现实 P0/P1。
- 最终完整验证通过：fmt check、all-targets/all-features Clippy `-D warnings`、2076 个 lib tests、全部 bin/integration/example tests、all-targets/all-features check 与 diff check；按约定未执行版本一致性脚本。
- 完整真实 TUI 回归使用全新 `session_b4bf78db`，覆盖 Anthropic streaming、多轮 reasoning replay、真实 `file_read`、退出后 resume；结果为 4 个 committed turn、4 个 streaming delta、1 次完整工具调用、0 次 fallback、0 个失败 turn，5 条 assistant replay 都有 thinking block 与非空 signature，两份 stderr 均为空。最终两项 P1 修复后重新构建源码，并以另一个全新 `session_3a9baf63` 完成 2 个 streaming/replay turn；3 个 delta、0 fallback、0 失败 turn、2 条 thinking/signature replay，stderr 为空。两轮都只统计 block/signature 存在性，不输出 reasoning 原文；supervisor 均已停止。
- 真实 endpoint 仍没有稳定产生 `model_context_window_exceeded`，因此不宣称真实覆盖该终态；JSON/SSE、两次恢复、工具、禁用、无进展、raw budget 与 commit/resume 继续由 deterministic fixture 验收。

### 阶段 15：历史图片/PDF策略统一（已完成）

实施：

1. `anthropic` 与 `openai_chat` 和现有 `openai_responses` 一样，对未 compact 历史使用 `Preserve` media policy。
2. 复用各 adapter 已有的 Image/Document wire 转换，不复制附件到 assistant replay，不改变 canonical/session schema。
3. 补充 adapter policy、session reload、未 compact suffix 与 compacted prefix 回归，确保真实媒体只进入 provider history，不进入摘要、Memory 或普通 transcript。
4. 只用真实 LLM TUI 验收本阶段：分别验证 Anthropic 与 Chat 的图片下一轮追问、退出后 resume；PDF 由支持文档输入的已知可用 endpoint 验证，若 endpoint 不支持则保留协议转换与 deterministic 回归证据，不把上游能力限制误报为 ACN 失败。
5. 完成针对性 code-review；修复现实 P0/P1 后重新验证。本阶段不改 TUI、Reasoning、fallback 或 compaction 其他语义。

阶段验收：

- 三个主对话 adapter 都声明 `Preserve`，未 compact Image/Document 在下一轮和 resume 后仍是原始媒体块。
- compacted prefix 只发送摘要；未 compact suffix 保留真实媒体。
- 本地 canonical session 仍只保存一份原始附件，assistant replay 不复制 base64。
- 真实 TUI 无附件重复显示、无无故 fallback，stderr 为空。
- targeted review 没有未处理的现实 P0/P1。

阶段 15 实施证据：

- `anthropic`、`openai_chat` 与 `openai_responses` 都声明 `Preserve`；adapter policy、Image/Document wire 转换、session JSONL reload、未 compact suffix 与 compacted prefix 均有 deterministic 回归。完整验证通过 fmt check、all-targets/all-features Clippy `-D warnings`、2080 个 lib tests、全部 bin/integration/example tests与 all-targets/all-features check；按约定未执行版本一致性脚本。
- Anthropic 使用全新 `session_61270f62` 完成“首轮图片、次轮无附件追问、关闭后 resume 无附件追问”；Chat 使用 `gpt-5.6-luna` 与全新 `session_a1f9f613` 完成同一流程。两条会话都得到 3 个 committed turn、0 fallback、0 failed turn，初始与 resume stderr 均为空；各自 canonical JSONL 只有 1 个 Image block，证明历史回传没有复制为新的用户附件。
- PDF 真实 endpoint 使用包含 TOP/MIDDLE/BOTTOM 三个互不重复值的单页 PDF，分别以 `openai_responses` GPT 模型的全新 `session_88446f13`、`anthropic` `claude-sonnet-5` 的全新 `session_9bda385c`，以及 OpenRouter `openai_chat` `qwen/qwen3.8-max` 的全新 `session_eee9a210` 完成“首轮附件、次轮无附件追问、关闭后 resume 无附件追问”。三个值都在对应回答前未出现在对话文本中，三条会话都依次正确返回；各自得到 3 个 committed turn、0 次工具调用、0 fallback、0 failed turn，两份 stderr 均为空，canonical JSONL 都只有 1 个 Document block。Responses 与 Anthropic 各有 6 个 streaming delta，Chat 有 7 个；Anthropic 会话另有 3 条匹配 model 的 replay。另一个 Chat-compatible 代理下 `gpt-5.6-luna`、重新请求后的 `gpt-5.6-sol` 与 `gpt-5.5` 都无法读取同一 PDF，`qwen3.6-plus` 则明确以 400 拒绝 `file` content part；这些上游限制均未计作 ACN 失败或通过。
- 本地审查与独立 targeted review 均未发现现实 P0/P1。独立 reviewer 记录了一个非阻断测试增强项：尚未为三个 adapter 各自捕获完整 resume HTTP body；现有运行路径、adapter 转换、reload/compaction 测试与真实 Anthropic/Chat TUI 已覆盖本阶段产品语义，因此没有继续扩大实现。

## 8. 总体验收矩阵

| 范围 | 通过标准 |
| --- | --- |
| Anthropic 请求 | thinking type/budget/effort 按拍板精确发送，streaming/non-streaming 一致，不做 model 猜测 |
| Non-streaming | 完整 raw blocks 保存；canonical text/tool 正确；thinking 不展示 |
| Streaming | thinking/signature/tool delta 完整组装；缺终态与损坏流不提交 |
| Tool loop | assistant raw blocks 原样、原序回传；tool result 紧随对应 tool_use；工具不重复执行 |
| Max tokens | continuation 的 provider-private 序列完整保存，下轮不丢失 reasoning 上下文 |
| Context window（阶段 11–14） | 有效 partial 原样保留；不 fallback；forced compact 后续写；最终成功才提交，失败 resume 不恢复 partial |
| Session | canonical + replay 原子落盘；resume 前后请求语义一致 |
| Identity | protocol + exact model；A → B → A 不复活旧世代；endpoint 不参与 |
| Responses 回归 | 原有 reasoning/item、附件、tool、resume 行为不退化；旧 unbound replay 安全降级 |
| 历史媒体（阶段 15） | 三个主对话 adapter 的未 compact Image/Document 在下一轮与 resume 中真实发送；compacted prefix 只发送摘要 |
| Compaction | raw reasoning 不进 summary/Memory/search；当前世代 tail 可 replay；预算不漏算/双算 |
| Provider-triggered compaction（阶段 11–14） | committed history 独立 summary 并异步投递 Recap；active-only 不 recap；最近 partial replay tail 不被 compact；无进展立即失败 |
| 隐私 | TUI/log/journal/error/review 输出不含 raw reasoning、signature、data、key、base64 |
| TUI | 无新 Reasoning 展示；最终 text/tool/fallback/cancel 行为保持现状 |
| Router/Chat | Router 仍无 Reasoning；Chat 只补文档、不实现 reasoning_content |
| 真实 LLM | 新 session 覆盖 streaming、多轮、tool、resume、model switch 与 Responses 回归 |
| Review | targeted 与全量 review 均无未处理 P0/P1、无影响本需求正确性的 actionable finding；修复后自动化与真实 TUI 复验通过 |

## 9. 开工门槛

正式编码前必须满足：

1. P1A、P2A、P3A 均已完成拍板。
2. 阶段 11–14 开工前，P4、P5、P6 必须完成拍板。
3. 本文状态中的后续增量从“规划中，尚未开工”更新为“实施中”；阶段 0–10 的已完成证据保持不变。
4. 确认仍在本需求对应的 feature worktree 与 `feature/oai_response` 分支工作，不误改主 worktree。
5. 确认现有工作区变更归属并保留用户修改。
6. 任何后续追加决策不得与本文已拍板项冲突或使其失效。
