# OpenAI Responses WebSocket Transport

> 状态：已完成并验收（2026-08-11；1A–21A、22D、23A、24B、25C、26B、27C、28C、29B、30B、31B、32B、33B、34A、35A 产品与协议语义均已实现）。
>
> 本文只定义 `openai_responses` 的 WebSocket transport。Responses 的 canonical message、provider replay、Reasoning、历史附件、compaction、session resume 与 HTTP SSE/JSON 基线仍以 `PRD_openai_responses.md` 和 `PRD_anthropic_reasoning.md` 的已完成语义为准；本文仅覆盖其中“WebSocket 不在范围内”的旧边界。

## 1. 背景

ACN 当前的 `openai_responses` adapter 支持两种 HTTP 调用形态：

- 主对话和需要流式事件的调用优先使用 HTTP SSE；
- 非流式调用与流式失败恢复使用 HTTP JSON。

Responses WebSocket mode 在同一个 `/responses` 协议上增加持久连接。客户端通过 `response.create` 发起响应；在连接仍然有效时，可以使用上一条 `response.id` 作为 `previous_response_id`，只发送新增 input items，减少长工具链反复上传完整历史造成的延迟。

WebSocket 不是 Chat Completions，也不是 Realtime API。它返回的服务端事件与 Responses streaming 事件模型一致，因此应作为 `openai_responses` 的第三种 transport 接入，而不是增加第四个 provider，也不能复制一套独立的 Responses canonical、Reasoning 或工具协议。

相关协议资料：

- [Responses WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode)
- [Responses WebSocket events](https://developers.openai.com/api/reference/resources/responses/websocket-events)
- [Responses conversation state](https://developers.openai.com/api/docs/guides/conversation-state)
- [Responses streaming](https://developers.openai.com/api/docs/guides/streaming-responses)
- [WebSocket mode errors](https://developers.openai.com/api/docs/guides/error-codes#websocket-mode-errors)

## 2. 目标

1. 为 `openai_responses` 增加可选 WebSocket streaming transport，保留现有 HTTP SSE 与 HTTP JSON。
2. 支持持久连接复用、`previous_response_id` 和增量 input，降低连续对话与工具回环的重复传输成本。
3. 保持 ACN 的 `store = false` 本地会话设计；上游 response ID 只作为当前连接内的性能优化，不成为 session 权威状态。
4. 使用 connection lease 支持主对话、并发子代理等并行调用，不以一个全局锁串行化所有 response。
5. 复用现有 Responses event reducer、工具安全门、canonical commit、Reasoning replay、历史附件、compaction 和 TUI 行为。
6. WebSocket 不可用或发生可恢复 transport 故障时，安全回到现有 HTTP SSE/JSON 路径。
7. 对 fresh session、连续对话、工具回环、并发、resume、compact、取消、故障恢复进行可重复验收。

## 3. 非目标

- 不新增 `openai_websocket`、`openai_realtime` 或其他 provider 名称。
- 不为 `openai_chat`、`anthropic` 或 Router rerank 增加 WebSocket。
- 不实现 Realtime API 的音频、会话或事件协议。
- 不根据 model 名是否包含 `gpt` 自动开启或拒绝 WebSocket。
- 不根据 endpoint hostname 建立厂商白名单或黑名单。
- 不改变 `store = false`，不依赖上游持久化 response 来恢复 ACN session。
- 不将 response ID 写入 `messages.jsonl`、`turn_events.jsonl` 或其他 session 文件。
- 不把 `supports_websockets` 加入 `config.template.toml`；默认模板继续保持最小配置。
- 不增加用户可配置的 WebSocket connect timeout、连接池大小或 response ID 有效期。
- 不实现 `generate = false` 预热，不发送非标准 beta header 或实现来源专属 metadata。
- 不改变 Reasoning 的当前产品边界：接收、保存、原样回传，但不在 TUI 展示。
- 不改变 Router 的单次、非流式、不请求 Reasoning 的 rerank 语义。
- 不承诺所有提供 `/responses` HTTP 接口的第三方 endpoint 都同时支持 WebSocket；能力由用户显式配置。

## 4. 已拍板产品与协议决策

### 4.1 WebSocket 是第三种 transport（1A）

配置仍选择现有 provider：

```toml
[agent.llm]
provider = "openai_responses"
endpoint = "https://api.example.com/v1"
model = "example-model"
api_key_env = "LLM_API_KEY"
supports_websockets = true
```

- `supports_websockets = false` 为默认值，现有配置行为不变。
- 只有 `provider = "openai_responses"` 时允许设为 `true`。
- `openai_chat` 或 `anthropic` 配置 `supports_websockets = true` 时，在配置加载阶段返回清晰错误，不静默忽略。
- 该字段是用户对 endpoint 能力的明确声明，不是 model 能力探测结果。
- `config.template.toml` 不展示该高级字段；稳定参数文档中以简短的可选项说明。

### 4.2 不做 model 或 endpoint 启发式限制（2A）

- 运行时不检查 model 名是否包含 `gpt`。
- 运行时不检查 endpoint 是否属于特定域名。
- 用户文档可以建议仅在明确实现 OpenAI Responses WebSocket 语义的 GPT 系列 endpoint 上开启。
- 兼容 endpoint 只有经过真实验证后才能被描述为“已验证”；不能仅凭 HTTP Responses 可用就推断 WebSocket 可用。

理由：WebSocket 是 endpoint 的 transport 能力。model 名可能被别名化，endpoint 也可能由网关、云服务或自建兼容层提供；硬编码名称只会制造误判，且不能验证 wire protocol 是否真的兼容。

### 4.3 Endpoint 与 wire 请求（3A）

- 沿用现有 Responses endpoint resolver 得到 `/responses` 路径。
- `https` 转为 `wss`，`http` 转为 `ws`；路径和安全的既有 query 保持现有 endpoint 解析语义。
- 握手沿用当前 HTTP adapter 的认证 header 与必要公共 header，不新增专属 beta header。
- 每次调用发送一个文本 JSON frame，顶层 `type = "response.create"`。
- body 与当前 Responses create request 共用构造逻辑，但 WebSocket 不发送 `stream` 和 `background`。
- `store` 固定为 `false`；Agent 继续请求 `include = ["reasoning.encrypted_content"]`。
- 首版不发送 `generate = false` 预热请求。
- 服务端文本事件进入现有 Responses streaming event reducer；不得复制一套 canonical、Reasoning、工具调用或终态判断。
- binary frame、非法 UTF-8、损坏 JSON、连接在完整终态前关闭、缺少终态或请求超时均视为流式 transport 失败，不能跳过后继续提交。

### 4.4 Response ID 只是连接内短期状态（4A）

ACN 继续固定使用 `store = false`。在这种模式下：

- 服务端只在当前 WebSocket 连接内保留最近一个 previous-response state；
- 该状态没有供 ACN 依赖的独立、固定 TTL；
- WebSocket 连接最长持续 60 分钟，但“60 分钟”是连接上限，不是 response ID 保证有效 60 分钟；
- 连接关闭或重建后，原 ID 通常不可用于 `store = false` 的新连接；
- ID 不是当前连接缓存的最近一项时可能不可用；
- 某次 continuation 返回 `4xx` 或 `5xx` 后，被引用的 previous state 会被服务端淘汰；
- 服务端返回 `previous_response_not_found` 时，ACN 必须按缓存失效处理。

因此，response ID 只保存在拥有它的 connection lease 运行时状态中：

- 不落盘；
- 不进入 canonical message；
- 不进入 provider replay；
- 不进入 trace、recap、Memory、Claim 或 session search；
- 不允许脱离原连接交给另一条连接使用。

### 4.5 Fresh session 与 resume（5A）

Fresh session 的第一条 WebSocket 请求：

1. 建立新连接；
2. 不发送 `previous_response_id`；
3. 发送当前完整有效 input history；
4. 完整成功后，在该连接内记录最新 response ID 和它所代表的逻辑上下文；
5. 后续满足严格匹配时进入增量链。

任何 session resume 都开始一条新的连接内增量链：

1. 从 `messages.jsonl` 的 canonical messages、匹配当前协议与精确 model 的 provider replay、有效 compaction summary，以及未 compact 的历史附件重建完整有效 history；
2. 建立或租用一条没有旧 session continuation 状态的新连接；
3. 首次 resumed 请求省略 `previous_response_id`，发送完整 history 加本轮新输入；
4. 该请求完整成功后，才在新连接内保存新的 response ID；
5. 同一次 resumed 进程中的后续轮次可继续使用新的 ID 和增量 input。

即使未来出现“进程未退出且旧 socket 仍在”的特殊 resume 入口，也不把旧 connection-local state 与 resumed session 隐式绑定。resume 的正确性只依赖本地 session；response ID 只是重新建立链之后的优化。

该决策保证：

- 退出后 resume 不会依赖已经消失的 socket；
- 60 分钟连接上限不会变成 session 生命周期上限；
- 用户切换 session、协议或 model 时不会串用上游缓存；
- 本地 session 与上游短期状态发生分歧时，以已提交的本地 session 为准。

### 4.6 完整 history 与增量 input（6A）

第一条请求、resume、重连、compact、配置变化或匹配失败时发送完整有效 history。只有满足下列全部条件，才可发送 `previous_response_id + 增量 input`：

1. lease 持有的仍是产生该 response ID 的同一条活动连接；
2. response ID 是该连接已知的最新完整成功 response；
3. 当前请求的有效 input 严格以前一次 input 加前一次 output items 为前缀；
4. 除 input 和 WebSocket transport 专属字段外，规范化后的 Responses 请求属性与连接缓存完全一致；
5. 当前会话未发生 compact、replay 世代切换、协议切换、精确 model 切换或 adapter 重建；
6. 上一轮没有未知终态、失败、取消或可能改变上游缓存一致性的事件。

实现应比较规范化请求 envelope 与精确 input 前缀，而不是维护一份容易随字段增加而漏比较的手写白名单。比较至少覆盖 instructions、model、tools、tool choice、并行工具设置、Reasoning、include、store、输出格式、service tier 及后续新增的有语义字段。

若任一条件不满足：

- 不报错；
- 不发送可能错误的增量请求；
- 使用完整 history 开始新的连接内链。

当服务端返回 `previous_response_not_found`：

- 丢弃当前连接及其 continuation state；
- 建立新连接；
- `previous_response_id` 设为 `null` 或省略；
- 使用同一逻辑请求的完整 history 重试一次恢复路径；
- 该恢复不会丢弃 Reasoning replay、工具结果或历史附件。

### 4.7 Connection lease 与并发（7A）

一条 Responses WebSocket 连接同时只允许一个 in-flight response，协议不提供 multiplex。因此不能用一个全局连接加跨完整响应生命周期的异步锁，否则主对话和并发子代理会被无意串行化。Memory review 与 Structured JSON 按 8A 固定走 HTTP JSON，不参与 WebSocket 连接竞争。

采用 connection lease：

```text
短锁查找并取出可复用的空闲连接
              ↓
调用方独占连接完成一个 response，不持有共享锁
              ↓
完整成功且连接状态明确时归还空闲池
              ↓
未知状态时丢连接；仅 chain 失效时清状态后归还
```

- 优先租用 continuation state 与当前请求严格匹配的连接。
- 没有匹配连接时，可租用无 continuation state 的健康空闲连接并发送完整 history。
- 没有空闲连接时，并发调用各自建立新连接，不等待另一条长响应释放全局单例。
- 连接池总容量固定为 `1 + agent.session.subagents.max_concurrent`，默认最多 7 条；优先保留具有 runtime chain 亲和性的连接，满载时只淘汰最久未使用的 idle 连接，绝不淘汰 leased 连接，也不新增用户配置项。
- connection-local response ID、请求快照和 output items 随 lease 移动，不能与连接分离或全局共享。
- 握手失败、frame 错误、超时、服务端错误、hard cancel 或接收状态不确定时直接丢弃连接；safe-boundary steering 等仅使 chain 失效、但 socket 健康且无 in-flight response 的情况，清空 continuation state 后可以归还。

### 4.8 调用路径边界（8A）

- 主 Agent 与子代理中请求 streaming 的 `openai_responses` 调用可以使用 WebSocket。
- Memory review、Structured JSON、普通 non-streaming 调用继续使用 HTTP JSON。
- Router rerank 继续使用 HTTP JSON，不读取 Agent 的 `supports_websockets`，也不创建连接池。
- HTTP SSE 与 HTTP JSON 保留为完整、独立、可直接工作的路径；WebSocket 不能成为 adapter 唯一可用 transport。

### 4.9 失败恢复与 fallback（9A）

WebSocket 正常路径必须以既有 Responses 完整终态校验为 commit gate。故障恢复按是否已经向用户输出可见 assistant 文本分流。

尚未输出可见文本：

1. 对符合现有 transport retry 分类的错误，使用现有 `retry_count` 和退避策略重试 WebSocket；
2. WebSocket 重试耗尽后，使用同一逻辑请求的完整 history 尝试 HTTP SSE；
3. HTTP SSE 若满足既有 fallback 条件，再进入 HTTP JSON fallback；
4. 确定性认证、参数和 response 终态错误不伪装成 transport 不兼容反复计费；握手 HTTP 状态按 24B 单独处理。

已经输出可见文本：

1. 不再从头启动一条 HTTP SSE，把第二份 delta 追加到同一个 TUI 回答；
2. 直接进入现有 HTTP JSON fallback；
3. JSON 成功后，用完整结果替换本轮 partial，继续既有 canonical commit；
4. JSON 失败则本轮失败，不提交 partial。

理由：SSE 从头生成会让用户看到重复或互相矛盾的两份文本；现有 non-streaming replacement 已经提供原子替换边界。由于完整响应校验前不会执行工具，使用完整 history 重放不会重复执行本地工具副作用。

### 4.10 取消、steering 与工具安全（10A）

- 用户 cancel 或 steering 结束当前逻辑 turn 时，不触发 SSE/JSON fallback。
- 未 committed 的 cancel/steering 一律废弃当前 continuation state；下一次请求不使用该 `previous_response_id`。
- 显式 cancel 真正打断 in-flight WebSocket，或终态/收帧状态不确定时，立即停止接收并丢弃连接；未知状态连接不得放回池中。
- safe-boundary steering 或发生在本地工具阶段、当前没有 in-flight WebSocket 的 cancel/steering，可以保留已知健康的 socket，但必须清空其 continuation state；下一次使用完整 history 开始新链。
- steering 太晚而未被当前 turn 接纳、仅成为普通排队输入时，前一 turn 正常 committed，连接和 continuation state 均可正常复用。
- 沿用当前 failed/cancelled turn 的 session 语义，不提交未完成 assistant。
- 在 output item、参数和完整终态全部校验通过前，不执行 tool call。
- tool-only 与 reasoning-only 流即使没有可见文本，也必须按完整终态判断；不得提交“空成功”。

### 4.11 连接不可用的进程内降级（11A）

- 若握手明确不支持 WebSocket，或按配置完成 WebSocket transport retry 后仍失败，则把当前 runtime chain 标记为 WebSocket unavailable。
- 该 runtime chain 后续请求直接走现有 HTTP SSE → HTTP JSON，不在每轮重复慢握手；主 session 与各 subagent/delegation chain 相互独立。
- 新 subagent、新 session、resume、进程重启或 adapter 重建后，若配置仍为 `supports_websockets = true`，重新尝试 WebSocket。
- 临时 response 失败不等同于 endpoint 永久不支持；只有连接/握手层失败达到既定重试上限才触发 sticky downgrade。
- 降级状态只在内存中，不写回 TOML，也不进入 session。

### 4.12 Timeout（12A）

- 首版内部使用固定 15 秒 WebSocket connect timeout，不增加 TOML 参数。
- connect timeout 只覆盖 DNS、TCP、TLS 与 HTTP Upgrade 完成，不是模型整轮生成时限，也不是任意两帧之间的 idle timeout。
- 现有 `timeout_secs` 继续约束每一次实际 WS/HTTP provider request 从开始到完整终态的生命周期；启用 WebSocket 时的 deadline 所有权按 27C 处理。
- 到达 60 分钟服务端连接上限或收到 `websocket_connection_limit_reached` 时，丢弃旧连接；当前逻辑请求按完整 history 在新连接恢复。

### 4.13 Session、Reasoning、附件与 TUI（13A）

- 不修改 canonical message 或 `provider_replay` 的落盘 schema。
- Responses reasoning item 继续原样落盘、resume 和下轮回传；只有 connection-local 增量请求在严格匹配时省略已被 previous state 表示的旧 items。
- 完整 history 请求继续携带未 compact 的历史图片和 PDF；增量请求只发送新增 items。
- compact 后以当前 ACN 有效 history 为准，开始新链，不尝试延续 compact 前的 response ID。
- TUI 继续只显示最终可读回答，不显示 Reasoning，不增加 WebSocket 专属布局。
- 正常 streaming delta 继续进入现有 assistant streaming cell；fallback 沿用现有简短状态提示和 partial replacement。
- response ID 不得出现在用户可见错误、普通日志或 session 文件中；诊断仅记录中性错误分类。

### 4.14 实现与验收声明边界（14A）

- 实现目标是公开的 OpenAI Responses WebSocket wire semantics。
- 本地 fake server 是精确协议、并发、取消和故障恢复的确定性验收依据。
- 真实 LLM TUI 验收使用一个明确支持相同 wire semantics 的 GPT 系列 endpoint。
- 没有实际凭据和真实测试证据时，不声称某个云服务或第三方 endpoint 已经通过 ACN 验收。
- 仓库、PRD、fixture、日志与测试产物不写入真实 endpoint、API key、私有响应、真实用户名或附件 base64。

### 4.15 增量链绑定 runtime chain（15A）

- 增加只存在于进程内、不落盘且不发送上游的 runtime chain ID。
- 主对话按当前运行期 session generation 绑定；每个 subagent/delegation 使用独立 chain。
- resume 即使沿用相同持久化 session ID，也创建新的 runtime generation，不能复活旧 response ID 或旧 sticky downgrade 状态。
- connection lease 只有在 runtime chain ID、精确请求 envelope 与 input prefix 全部匹配时，才能使用 `previous_response_id`。
- 禁止两个碰巧具有相同 history 的不同 session 或 delegation 共用 continuation state。

理由：ACN 的 provider adapter 被多个调用方共享，而 response ID 表示某条连接上的短期上游状态；仅比较 history 不能建立可靠的会话所有权。

### 4.16 连接池容量（16A）

- 总容量固定为 `1 + agent.session.subagents.max_concurrent`，默认最多 7 条。
- 主 Agent 占一个潜在并发槽，其余槽位对应 ACN 已允许同时 running 的 subagent。
- Memory review、Structured JSON、session search summarizer 与 Router 走 HTTP，不计入 WebSocket 容量。
- 优先保留有 runtime chain 亲和性的 idle 连接；需要腾位时按 LRU 淘汰 idle 连接，不等待或终止 leased 连接。
- 不增加连接池容量 TOML 参数，也不使用任意较大的固定默认值。

### 4.17 Endpoint scheme（17A）

- 用户配置继续只接受 `http://` 与 `https://` endpoint。
- 先沿用现有 Responses endpoint 规则得到 HTTP `/responses` URL，再内部转换为对应的 `ws://` 或 `wss://` URL，并保留合法 path 与 query。
- 不接受用户直接填写 WS scheme，不新增独立 `websocket_endpoint`；同一个配置必须始终能支持 HTTP SSE/JSON fallback。

### 4.18 WebSocket Rust client（18A）

- 首版使用与当前 reqwest 0.12 对应的 `reqwest-websocket 0.5`。
- WebSocket 单独建立 HTTP/1.1 upgrade 连接，沿用现有 rustls、系统证书、system proxy 与认证 header 语义；HTTP SSE/JSON client 保持不变。
- 不自行实现 handshake/frame 层，也不引入一套与现有 HTTP client 脱离的 TLS/代理配置。

### 4.19 完整终态必须包含 response ID（19B）

- `response.completed.response.id` 是完整成功终态的必需字段。
- 缺失、空值或类型错误均视为损坏流，不能因为已经收到文本、Reasoning 或工具 item 就提交当前响应。
- 该错误按 9A 根据是否已有可见文本进入相应 retry/fallback；不得从 `response.created` 猜测补齐，也不得复用旧 ID 或生成本地 ID。

理由：顶层 response ID 是 WebSocket 完整终态和后续 continuation 的协议身份。接受缺 ID 的终态会让“响应已完整”与“增量链可证明”使用两套标准。

### 4.20 HTTP fallback 后废弃旧 WS chain（20A）

- 任何 HTTP SSE/JSON fallback 成功后，都废弃触发 fallback 前的 WebSocket continuation state。
- HTTP 结果按既有逻辑正常提交；若当前 runtime chain 后续仍允许 WebSocket，则下一次必须使用 full history 建立新链。
- 不把 HTTP output 在本地拼接到旧 WS ID 后面，也不把 HTTP response ID 带到旧 WebSocket 连接继续。

理由：`store=false` 下，旧连接的 connection-local cache 不包含 HTTP 生成的结果，强行拼接会产生分叉历史。

### 4.21 Idle connection pump（21A）

- 每条连接由轻量后台 reader/pump 持续处理服务端 ping/pong、close 与连接状态，不在 idle 期间停止读取 socket。
- 客户端首版不主动周期发送 ping，避免额外后台流量与兼容 endpoint 差异。
- 连接接近官方 60 分钟上限时，在下一次租用前主动淘汰；首版使用 55 分钟内部阈值，不新增 TOML 参数。
- idle 阶段收到不应出现的 response 数据或无法确认协议状态时，将连接标记 invalid 并丢弃。

### 4.22 Runtime-chain sticky HTTP downgrade（22D）

- sticky downgrade 的作用域是 runtime chain，不是共享 adapter，也不是已经失败并将被销毁的单条 socket。
- 主 session 触发降级后，其后续 turn 继续使用 HTTP；已有或新建 subagent/delegation chain 仍可独立尝试 WebSocket。
- 某个 subagent 降级只影响该 subagent；其他 subagent 与主 session 不受影响。
- HTTP 426 属于明确的 Upgrade 不兼容，在当前 chain 直接触发 sticky HTTP；404、405、未成功完成 101 Upgrade、connect/request timeout、网络或 WebSocket transport 错误按 24B 与既有有限 `retry_count` 处理，不照搬无限等待网络的特殊重连路径。
- `previous_response_not_found`、60 分钟连接上限和单次 response 终态错误只按既定规则重建/失败，不触发 sticky。
- 精确 model 变化会废弃 continuation chain，但不清除同一 runtime chain 已触发的 HTTP downgrade；新 session、resume、新 subagent、进程重启或 adapter 重建会得到新的运行期状态。
- downgrade 时清理属于该 runtime chain 的 idle continuation state；状态不落盘、不写回 TOML。

### 4.23 Cancel/steering 的连接与 chain 边界（23A）

- cancel/steering 只要使当前 turn 未 committed，就不能继续使用该 turn 对应的 response ID。
- 真正打断 in-flight WebSocket 或无法证明完整终态时，同时废弃 continuation state 与 socket。
- ACN 现有 safe-boundary steering 不取消正在运行的 provider future；若 response 已完整结束、socket 健康，但 turn 因 steering 未提交，则只清空 continuation state，socket 可以回池供 full-history 请求复用。
- cancel/steering 发生在本地工具阶段且没有 in-flight WebSocket 时遵循同一规则：废弃 chain，健康 socket 可保留。
- 已经完成 canonical commit 后才到达、未被当前 turn 接纳的输入不属于中断，正常沿用前一 response ID。

理由：本地没有落盘“半截 assistant”并不代表上游 connection-local state 与 committed history 仍一致；connection 健康性和 continuation 可复用性必须分别判断。

### 4.24 WebSocket 握手 HTTP 状态的 retry 与 downgrade（24B）

- 握手返回 HTTP 426 `Upgrade Required` 时，不消耗 WebSocket retry 次数，立即将当前 runtime chain 标记为 sticky HTTP，并以完整 history 通过现有 HTTP 路径重放当前逻辑请求。
- 握手返回 HTTP 404 或 405 时，不立即判定 endpoint 确定性不支持 WebSocket；将其归入可重试的握手失败，使用现有 `retry_count` 与退避策略进行有限 WebSocket 重试。
- 404/405 重试耗尽后，才将当前 runtime chain 标记为 sticky HTTP，并以完整 history 通过现有 HTTP 路径重放当前逻辑请求。
- 其他未成功完成 101 Upgrade 的可恢复握手错误沿用既有 transport retry 分类；重试耗尽后触发当前 runtime chain 的 sticky HTTP。
- 认证失败、明确的请求参数错误等现有确定性错误保持直接报错，不通过 HTTP fallback 掩盖真实配置问题，也不把 runtime chain 标记为 WebSocket unavailable。
- sticky 状态仍只属于当前 runtime chain：主 Agent、各 subagent/delegation 独立判断；Memory、Structured JSON 与 Router 固定走 HTTP，不参与此状态。

理由：426 是明确的协议升级信号，可以立即降级；404/405 也可能来自临时路由、网关或部署状态，应获得有限恢复机会。有限重试失败后再 sticky，可以避免每个后续 turn 重复慢握手，同时不把一次 chain 的连接问题扩散到其他并发 chain。

### 4.25 Runtime context 与严格增量前缀（25C，已由 34A 取代）

背景：ACN 会在每个逻辑 user turn 发给 provider 的当前 user item 前注入日期与时区 `<runtime_context>`，但 canonical session 只保存原始用户内容。若直接比较原始 wire input，下一轮历史中的旧 user item不再含该运行期前缀，导致严格前缀永远不匹配。

选项：

- A：保持原始 wire input 比较，接受所有跨 turn 请求都发送 full history；这会使 `previous_response_id` 只能用于同一 turn 的工具或 token continuation，未达到本期连续对话目标。
- B：把 runtime context 写入 canonical session/provider replay；这会改变既有持久化与 resume 语义。
- C：首个 full request 正常发送 runtime context；connection-local state 额外保存其精确值，并用去掉该运行期前缀后的逻辑 input 做严格前缀比较。context 值相同的后续 incremental request 只发送同样规范化后的 suffix，不重复发送 context；context 值变化时强制 full history 开新链。
- D：每轮 incremental 都重复发送 runtime context；这会让 connection-local history 累积多份运行期元数据，与现有 HTTP full-history 语义偏离。

选择 C：

- runtime context 仍不落盘，canonical、resume、recap、Memory 与 session search 语义不变；
- 同一链上游始终保留首个 full request 已携带的一份当前 runtime context，不在每轮累积重复项；
- 日期或时区变化会使精确 context fingerprint 不匹配，下一次使用 full history 和新 context 重建链；
- 规范化只作用于 connection-local strict match 与 incremental suffix。full history、HTTP SSE/JSON 和默认关闭 WebSocket 的路径继续使用原始 provider request；
- 无法无歧义识别 ACN 生成前缀时，安全地不做规范化并使用 full history。

25C 记录了 WebSocket 首次实现时的旧前提。随后 Provider 缓存前缀改造已将 runtime、
background 和 delegation context 统一为独立、只追加、可持久化的 `ModelContext`
message；因此 25C 不再是当前运行语义，以 34A 为准。

### 4.26 其他 WebSocket 握手拒绝状态（26B，针对性 review 后追加）

背景：`response.create` 尚未发送时，HTTP 400、403 等握手状态不能证明模型请求参数有误；网关、WAF 或反向代理也可能用这些状态拒绝 Upgrade，而同一 endpoint 的 HTTP Responses 仍然可用。

选项：

- A：仅 404、405、429 与 5xx 视为可重试，400、403 直接终止当前 turn；这会让可用的 HTTP 路径失去恢复机会。
- B：426 仍立即降级，401 仍作为明确认证失败直接返回；其余未完成 101 Upgrade 的 HTTP 状态统一按握手 transport failure 有限重试，耗尽后对当前 runtime chain sticky HTTP。若它实际也是业务错误，随后的 HTTP Responses 会返回权威错误。
- C：解析各厂商握手 body 的错误码或文案再分类；没有统一 schema，容易形成脆弱的厂商字符串启发式。

选择 B：不掩盖 401，也不凭握手阶段的非标准状态推断模型参数错误；400、403 等只多消耗有限的无推理握手请求，不会执行工具或提交 session。

### 4.27 WebSocket 开启时的 request deadline 所有权（27C，全量 review 后追加）

背景：provider-neutral turn loop 原本会以 `timeout_secs` 包裹整个 adapter future，WebSocket transport 内部也使用同样长度判断单次 WS request 超时。两个同长 timer 竞争时，外层可能先取消 future，使内层来不及把超时归类为 WebSocket retry、runtime-chain sticky HTTP 和 SSE fallback；同一 chain 的下一轮还可能再次等待一遍 WebSocket 超时。

选项：

- A：保留两个同长 timer；实现简单，但会稳定留下上述恢复竞态。
- B：给外层任意增加固定宽限时间；宽限难以覆盖 retry、退避和后续 HTTP 请求，且会形成脆弱的双重 deadline。
- C：启用 WebSocket 的 Responses adapter 由 transport/client 独占 request deadline：每一次实际 WS、HTTP SSE 或 HTTP JSON 请求仍使用同一个 `timeout_secs`；provider-neutral 外层不再叠加同长 timer。未开启 WebSocket 的 Responses、Chat 与 Anthropic 继续保持原行为。

选择 C：

- WebSocket timeout 能先完成 socket 丢弃、有限 retry、sticky downgrade 与 HTTP fallback 分类，不被外层抢先取消；
- fallback 后同一 runtime chain 的下一轮可直接走 HTTP，不重复慢握手；
- 用户 cancel/steering 仍由 provider-neutral cancellation token 立即打断，不依赖 deadline；
- 不新增配置，也不改变默认关闭 WebSocket 时的超时语义；
- `timeout_secs` 仍限制每一次真实网络请求，而不是允许某个 socket 无限等待。

### 4.28 Safe steering 的恢复中断信号（28C，针对性复验后追加）

背景：safe steering 不能像 Esc/Ctrl-C 一样取消已经正常运行的 provider request 或工具，否则会破坏既有“运行单元自然收束到安全边界”的语义；但若当前 request 随后失败，仍继续 WebSocket retry、HTTP SSE、max-output continuation 或 HTTP JSON fallback，会延迟 steering 并为必定丢弃的 turn 产生无效调用。

选项：

- A：复用 hard-cancel token；能够及时停止恢复，但也会错误中断当前正常 response 和已派发工具。
- B：只在 provider-neutral JSON fallback 前检查 steering；无法阻止 adapter 内部的 WebSocket retry、WS → SSE、HTTP SSE retry 和 max-output continuation。
- C：增加只存在于当前 turn 内存中的 recovery interrupt。steer 与 hard cancel 都会触发它；adapter/turn loop 只在尚未开始的 retry、continuation 或 transport fallback 边界检查，绝不用它取消当前正常 request 或工具。

选择 C：

- 当前 request 正常完成时，仍自然收束；本轮因 steer 不提交，健康 socket 清除 continuation 后可回池；
- 当前 request 在 steer 后失败时，立即返回 interrupted，不再发起第二次 WS handshake、HTTP SSE 或 HTTP JSON；
- Responses 的内部 retry、`previous_response_id` 恢复、max-output continuation 与 WS → SSE 都服从同一信号；
- provider-neutral non-streaming fallback 在记录 attempt、退避、发请求和处理失败结果的边界重复检查，避免竞态启动下一次 attempt；
- 信号不落盘、不发给上游、不改变 hard cancel、canonical session 或 TUI 的既有语义。

### 4.29 WS 降级后 SSE transport 错误的分类（29B，针对性复验后追加）

背景：启用 WebSocket 时，单次 request deadline 由 Responses client 持有。WS 降级到 HTTP SSE 后，若请求在响应头到达前超时或发生其他可重试网络错误，reqwest 会返回 `Http` 错误；adapter 内部 retry 耗尽后若把它当普通业务错误，provider-neutral JSON replacement 将被绕过。

选项：

- A：保持普通错误并直接结束 turn；会使 WS 模式下的 SSE pre-header timeout 与既有损坏/中断 stream 恢复语义不一致。
- B：仅把流式请求在内部 retry 耗尽后的可重试 `Http` transport 错误，与损坏/未终结 stream 一起映射为 `ProviderStreamFailure`；认证、参数、终态和普通 HTTP 状态仍保持原分类。
- C：把所有 HTTP 状态和响应错误都映射为 stream failure；会掩盖认证、参数等确定性错误并扩大重复计费。

选择 B：

- WS → SSE 后的连接失败或 pre-header timeout 可以进入现有 HTTP JSON replacement；
- adapter 自身的 `retry_count` 仍先执行，只有耗尽后才交给 provider-neutral fallback；
- 非流式 fallback 继续关闭 adapter 嵌套 retry，TUI 的 `N/5` 与真实请求次数一致；
- 401、请求参数、确定性终态和不可重试 HTTP 错误不降级为 transport failure。

### 4.30 Continuation 确定性错误优先于 partial fallback（30B，针对性复验后追加）

背景：Responses 的 `max_output_tokens` continuation 可能已经向 TUI 输出第一段文字。若下一次内部 continuation 收到 400、401 或其他确定性终态，不能仅因“本轮已有可见文本”就进入最多 5 次 HTTP JSON replacement；重放相同请求不会恢复，反而会重复计费并掩盖权威错误。

选项：

- A：只看是否已有可见文本，所有 continuation 错误都进入 JSON replacement；会让确定性错误反复请求。
- B：认证失败、无效 endpoint、非 429 的 4xx、明确 `failed` / 非 `max_output_tokens` 的 `incomplete`，以及媒体拒绝等确定性错误优先映射为 terminal failure；429、5xx、可重试网络错误和损坏 stream 仍保留既有 retry/fallback。
- C：所有 continuation 错误都直接终止；会丢失 429、5xx 和网络抖动的安全恢复能力。

选择 B：

- 确定性终态分类覆盖首个 response 和所有内部 continuation，不被此前的 partial text 覆盖；
- provider-neutral turn loop 立即结束当前 turn，不启动 non-streaming fallback，也不提交 partial；
- 429、5xx 与 transport/stream 故障继续按 9A、29B 的有限恢复路径处理；
- 不改变完整成功、`max_output_tokens` 正常续写或默认 HTTP 模式的行为。

### 4.31 WebSocket wrapped error 保留状态分类（31B，针对性复验后追加）

背景：Responses WebSocket 的通用 `type = "error"` 事件可以在顶层携带 `status`，兼容实现也可能使用 `status_code`。若 reducer 只保留 message 并统一映射成 `Failed`，429 与 5xx 会被误判成确定性终态，无法执行有限 WS retry 或 HTTP 恢复。

选项：

- A：所有通用 error frame 都按确定性 `Failed`；实现简单，但会丢失限流和服务端暂态状态。
- B：识别数值型 `status` / `status_code` 并保留为现有 `Status` 错误；429、5xx 按可重试状态处理，其他 4xx 仍为确定性终态。没有合法状态码时保持 `Failed`，不根据错误文案猜测。
- C：所有通用 error frame 都按 transport failure 重试；会让 400/401 等确定性错误重复请求并掩盖配置问题。

选择 B：

- 复用 HTTP Responses 已有的状态分类，不创造厂商错误文案启发式；
- 429、5xx 在未输出当前 response 文本时执行有限 WebSocket retry，耗尽后按既定路径回到 HTTP；
- 400、401 等仍直接终止，不触发 non-streaming fallback；
- wrapped error 的嵌套 message 经过既有脱敏后才进入错误文本，不记录完整 frame、headers 或请求回显；
- SSE 与 WebSocket 共用 reducer，因此相同事件形状在两种 streaming transport 上得到一致分类。

### 4.32 暂态 response 错误不触发 sticky downgrade（32B，针对性复验后追加）

背景：429/5xx wrapped error 需要有限 WS retry，但“可重试”不等于“endpoint 的 WebSocket transport 不可用”。若重试耗尽后与连接损坏共用 `mark_sticky`，一次限流或短暂服务端故障会让当前 runtime chain 的所有后续 turn 永久绕过 WebSocket，直到 session/resume 边界。

选项：

- A：所有可重试 WS 错误耗尽后都 sticky HTTP；恢复最保守，但把常见暂态 response 状态误判成 transport 能力缺失。
- B：拆分 retry 与 sticky 分类：429/5xx 耗尽后只清除当前 continuation 并用 HTTP 恢复本次请求，下一 turn 仍可重新尝试 WS；连接损坏、timeout、握手/transport 失败耗尽后才 sticky。
- C：所有失败都不 sticky；会让不支持 WebSocket 或持续断流的 endpoint 每轮重复慢握手。

选择 B：

- 与 22D“单次 response 终态错误不触发 sticky”的既有拍板一致；
- 当前请求仍能通过完整 history 的 HTTP 路径恢复，不保留失败 response 的 ID 或 continuation；
- 同一 runtime chain 的下一 turn 会重新探测 WS，限流窗口结束后可自动恢复低延迟路径；
- 真正的握手、连接、timeout 与损坏流仍维持 sticky，避免每轮重复慢失败。

### 4.33 已输出 partial 后仍保留 transport sticky 分类（33B，全量 review 后追加）

背景：WebSocket 已经向 TUI 输出 partial 后，当前 turn 按 9A 不再从头执行 WS 或 SSE，而是把错误交给现有 HTTP JSON replacement。这个“当前请求的恢复方式”不能覆盖“后续 turn 是否继续尝试 WebSocket”的独立判断。若所有 partial 后错误都只清除 chain，一条持续损坏的 WS 连接会在同一 session 的每个后续 turn 重复生成 partial、断流并回到 JSON。

选项：

- A：已有 partial 后一律清除 chain、不 sticky；当前 turn 可恢复，但持续断流 endpoint 会在每轮重复付出 WS 生成与 fallback 成本。
- B：已有 partial 后仍按错误性质分类：连接损坏、timeout 与损坏 stream 对当前 runtime chain sticky HTTP；429/5xx 只清除当前 continuation，下一 turn 仍可重新尝试 WS；确定性错误清除 chain 并直接返回。
- C：已有 partial 后所有错误一律 sticky；会把限流、短暂 5xx 和确定性业务错误误判成 WebSocket transport 不可用。

选择 B：

- 当前 turn 仍严格遵循 9A：不再发起 WS/SSE，只有可恢复 transport/暂态错误才由外层 JSON replacement 完整替换 partial；
- 连接损坏的 runtime chain 在下一 turn 直接使用 HTTP，避免重复 partial 与重复计费；
- 429/5xx 延续 32B 的非 sticky 语义，限流或服务端短暂恢复后仍可重新使用 WS；
- sticky 仍只属于当前 runtime chain，不影响主 Agent、其他 subagent 或新 session。

### 4.34 持久化 ModelContext 参与精确前缀（34A，合并缓存语义时追加）

背景：Provider 缓存前缀改造已将 runtime、background 和 delegation context 改为独立的
`ModelContext` message，并与 canonical/provider history 一起持久化。25C 中“runtime context
临时塞入当前 user message、不落盘”的前提已不存在。

选择 A：

- WebSocket 使用与 HTTP Provider 完全相同的逻辑 input，不删除、抽取或重写
  `ModelContext`；
- 稳定 Provider 窗口与当前请求之间进行精确前缀比较。context 未变时自然命中，新
  context snapshot 按 append-only 语义作为 suffix 发送；
- compaction 是显式 history replacement：清除当前 WebSocket continuation/response ID，下一次
  使用完整 compacted history；
- compaction 不清除同一 runtime chain 已经确立的 sticky HTTP downgrade，健康空闲 socket
  仍可供 full-history 请求重用。

理由：这使 WebSocket 只优化物理传输，不再维护一套与 HTTP、WAL、resume 不同的
runtime context 规范化规则。

### 4.35 Provider WAL 与网络 deadline 分层（35A，合并缓存语义时追加）

选择 A：

- WebSocket、SSE 与 JSON 的每一次实际网络请求继续使用 `timeout_secs`；
- WebSocket adapter 不再被一个同时长的 provider-neutral 外层 timer 竞态取消；
- 首次 request WAL、adapter 内 continuation request WAL 和最终 response-inclusive WAL 均使用
  内部固定 10 秒的准备上限，不增加 TOML 参数；
- request WAL 超时时不发起后续请求，也不通过 fallback 绕过旧前缀；response WAL
  超时时当前 turn 不提交，并清除 WebSocket continuation。

理由：WAL 是网络 I/O 前的本地一致性准备步骤，不应占用模型请求的几分钟 timeout；
它又必须有界，避免开启 WebSocket 后因外层 timer 取消而无限等待。

## 5. 运行时状态模型

每条可复用连接携带自己的私有状态：

```text
WebSocketConnectionState
├── socket
├── lifecycle: idle | leased | invalid
├── runtime_chain_id
├── last_response_id
├── represented_input
├── last_output_items
├── normalized_request_envelope
├── created_at
└── last_used_at
```

adapter 另有不落盘的运行期协调状态：

```text
WebSocketRuntimeState
├── pool_capacity = 1 + subagents.max_concurrent
├── idle/leased connection registry
├── runtime chain generation registry
└── sticky_http_runtime_chains
```

逻辑请求流程：

```text
构造完整有效 Responses request
              ↓
在空闲池中查找严格匹配的 continuation lease
        ┌─────┴─────┐
      匹配          不匹配
        ↓              ↓
previous_response_id   新/空闲连接
+ input suffix         + full input
        └─────┬─────┘
              ↓
复用 Responses event reducer
              ↓
完整终态校验与工具安全门
        ┌─────┴─────┐
      成功          失败/取消/steer
        ↓              ↓
更新连接状态并归还    废弃 chain；仅状态未知时丢连接
```

持久化边界：

```text
本地 session（权威、可 resume）
  canonical messages + provider replay + compaction state

WebSocket connection（短期、不可 resume）
  response ID + previous-state request snapshot
```

## 6. 分阶段实施计划

### 阶段 0：基线与影响面确认

实施：

1. 对照当前 `openai_responses` 的 request DTO、endpoint resolver、SSE reducer、JSON parser、turn loop、fallback、bootstrap 和 session replay。
2. 确认主对话、子代理、Memory review、Structured JSON 与 Router 的实际调用边界。
3. 记录 HTTP SSE/JSON 基线测试，避免 WebSocket 改造改变默认关闭时的行为。

验收：

- `supports_websockets` 默认关闭时，现有测试和请求序列完全不变。
- 明确列出共享 reducer 与不能复制的 canonical/session 逻辑。

### 阶段 1：配置、endpoint 与共享事件入口

实施：

1. 增加 `supports_websockets`，默认 `false`，并完成 provider 配置校验。
2. 增加 HTTP(S) 到 WS(S) endpoint 转换与 connect timeout。
3. 将现有 Responses streaming event 处理整理为 SSE 与 WebSocket 共用入口。
4. 保持 HTTP 请求 DTO 与 WebSocket `response.create` envelope 的字段来源一致。

验收：

- 配置默认值、合法/非法 provider 组合、endpoint 转换均有单元测试。
- SSE 既有事件 fixture 在重构后结果不变。
- WebSocket request 不发送 `stream`、`background` 或预热字段。

### 阶段 2：完整 history WebSocket streaming

实施：

1. 建立 WebSocket，发送完整 history 的 `response.create`。
2. 接收文本 frames，经共享 reducer 输出 text、reasoning、tool call 与终态。
3. 完成正常关闭、服务端错误、非法 frame、缺终态和 timeout 的分类。
4. 复用现有完整终态校验、tool execution gate 与 canonical commit。

验收：

- fake server 覆盖 text、reasoning、tool call、max-output continuation 与完整终态。
- 损坏 JSON、binary frame、非法 UTF-8、提前 close、缺终态均不提交 session。
- tool call 只在完整成功后执行一次。

### 阶段 3：Connection lease 与并发

实施：

1. 增加容量为 `1 + subagents.max_concurrent` 的连接池与 connection lease。
2. lease 独占期间不持有共享池锁。
3. 增加 idle reader/pump、55 分钟租用前淘汰与 LRU idle 清理。
4. 成功归还健康连接；未知状态丢弃；仅 chain 失效但 socket 健康时清空 continuation 后归还。
5. 并发请求没有匹配空闲连接时在容量允许范围内建立独立连接。
6. 池满时同时等待健康连接归还 idle 池或物理连接槽释放，不能只等待仍被 idle 连接持有的 semaphore permit。

验收：

- 两个并发 streaming response 不被单连接串行化。
- 同一连接绝不出现两个 in-flight response。
- 取消其中一个请求不污染另一个请求或空闲池。
- idle pump 正确响应 ping/pong/close，不主动周期发 ping。
- 连接池尚有空位时，新 chain 建立新连接，不清除其他 idle connection 的 continuation affinity。
- 容量为 1 时，第二个独立 chain 会在首个 response 完成后复用归还的连接，不会永久等待。

### 阶段 4：`previous_response_id` 与增量 input

实施：

1. 为主 session generation 和各 subagent/delegation 注入不落盘的 runtime chain ID。
2. 在 connection-local state 保存 runtime chain ID、最新成功 response ID、请求 envelope 与其表示的 input/output 前缀。
3. 实现完整 envelope 和精确 input prefix 比较。
4. 匹配时只发送 suffix；不匹配时发送完整 history 并开始新链。
5. 实现 `previous_response_not_found` 的新连接 + 完整 history 恢复。
6. 将缺失 `response.completed.response.id` 作为损坏流处理。
7. compact、model/协议切换、adapter 重建和 resume 强制新链。
8. runtime、background 与 delegation `ModelContext` 与普通 input 一样参与精确前缀；
   新 snapshot 按 append-only suffix 发送，不做 WebSocket 私有规范化。

验收：

- 连续第二轮与工具回环只发送新增 items。
- 修改任一有语义请求字段都会阻止错误增量复用。
- 旧 response ID 不会被另一连接、另一 session 或另一 model 使用。
- `previous_response_not_found` 恢复后只提交一份完整结果。
- 缺失 terminal response ID 不提交，即使此前已经收到 text/tool/reasoning item。
- 持久化 ModelContext 未变的连续 turn 可增量复用；新 snapshot 只作为 suffix 发送；
  只有精确前缀被改写时才发送 full history。

### 阶段 5：Fallback、取消与 sticky downgrade

实施：

1. 将 WebSocket transport failure 接入现有 retry/fallback 分类。
2. 无可见输出时按 WebSocket → SSE → JSON 恢复。
3. 有可见输出时直接进入 JSON replacement。
4. HTTP fallback 成功后废弃旧 WS continuation state。
5. cancel/steering 不 fallback；未 committed 时废弃 chain，只有 in-flight/未知终态才丢弃 socket。
6. HTTP 426 握手响应立即对当前 runtime chain 启用进程内 HTTP sticky downgrade；404/405 与其他可恢复握手/连接错误在有限重试耗尽后再启用。
7. WS 降级后的 SSE 在响应头前出现可重试 transport 错误时，adapter 内部 retry 耗尽后仍进入 JSON replacement。
8. 已有 partial 的内部 continuation 若返回确定性认证、参数或终态错误，terminal 分类优先，不进入 JSON replacement；429、5xx 和 transport 故障仍可恢复。
9. WebSocket 通用 error frame 保留 `status` / `status_code`；429、5xx 可重试，其他 4xx 确定性失败，缺少合法状态码时不猜测。
10. 429/5xx 的 WS retry 耗尽后只恢复当前请求，不将 runtime chain 标记为 sticky；transport/握手错误耗尽后仍 sticky。
11. 已有可见 partial 后若发生连接损坏或损坏 stream，当前 turn 直接交给 JSON replacement，且当前 runtime chain 后续 turn sticky HTTP；429/5xx 仍只清除本次 continuation。

验收：

- fake server 分别覆盖零 delta、已有 text delta、reasoning-only、tool-only、取消和握手失败。
- TUI 不出现重复拼接文本，不执行半截工具，不提交空 assistant。
- 426 不消耗 retry 次数并立即使用 HTTP；404/405 严格执行配置的有限 retry，耗尽后才使用 HTTP。
- 400/403 等其他非 101 握手状态有限 retry 后使用 HTTP；401 仍直接报认证失败。
- 主 session sticky downgrade 后其下一轮不重复 WebSocket 握手；独立 subagent chain 仍可使用 WS。
- `timeout_secs` 到达时由 WS transport 完成丢 socket与 sticky/SSE 分类；同一 chain 下一轮不再重复 WS 超时。
- safe-boundary steer 可以保留健康 socket，但下一次请求必须使用 full history；hard cancel 不把未知状态连接放回池。
- safe-boundary steer 不取消当前健康 response，但若 response 随后失败，不得再启动 WS retry、SSE、max-output continuation 或 JSON fallback。
- WS 降级后的 SSE 响应头超时不会绕过 JSON replacement；认证、参数和确定性终态仍直接失败。
- `max_output_tokens` 已输出 partial 后，下一次 continuation 返回 400/401 等确定性错误时立即失败，不启动非流式 fallback；429、5xx 仍保留恢复能力。
- wrapped WS error 的 429 与 `status_code` 5xx 会有限 retry 后恢复；wrapped 401 不重试、不 sticky、不进入 HTTP fallback。
- wrapped 429/5xx 重试耗尽后当前请求回到 HTTP，但同一 runtime chain 的下一 turn 仍重新尝试 WS；连接损坏重试耗尽后仍 sticky。
- 已输出 partial 后连接损坏不会再发起 WS/SSE；JSON replacement 恢复当前 turn，同一 runtime chain 的下一 turn 不再重复 WS。已有 partial 后的 429/5xx 仍不 sticky。
- 新 session、resume、新 subagent、进程重启或重建 adapter 后可再次尝试。

### 阶段 6：Session、resume、compaction 与媒体回归

实施：

1. 验证 response ID 没有进入任何持久化文件。
2. 验证 fresh process resume 首轮发送完整有效 history。
3. 验证 resumed 首轮成功后重新建立增量链。
4. 验证 compact 后发送 compacted effective history 并开始新链。
5. 验证完整 history 保留未 compact 的图片、PDF 与 Responses provider replay。

验收：

- 退出并 resume 后不需要旧 socket 或旧 response ID。
- canonical transcript 与 provider replay 没有重复消息。
- Reasoning、工具结果、历史媒体在完整恢复请求中保持现有语义。
- session search、Memory、recap 与 TUI 不出现 response ID 或 Reasoning 泄漏。

### 阶段 7：文档与真实 LLM TUI 验收

实施：

1. 在参数文档中简短说明 `supports_websockets` 的作用、默认值、适用 provider 和推荐范围。
2. 不修改 `config.template.toml`。
3. 使用 fresh session 完成真实 LLM TUI smoke test，覆盖多轮 streaming、工具调用、并发子代理和退出后 resume。
4. 检查 `turn_events.jsonl`、`messages.jsonl` 与 stderr；不提交真实响应或私有配置。

验收：

- 正常路径有 streaming delta、成功 turn 和完整工具回环。
- 正常路径没有非预期 HTTP fallback。
- 退出再 resume 能读取历史并继续对话；首次 resume 后的下一轮仍可正常 streaming。
- TUI 没有新增 Reasoning 展示、重复文本或错误的 session 回放。

### 阶段 8：针对性 review、全量 review 与最终验收

实施：

1. 阶段 0–7 的实现与真实 LLM TUI smoke test 通过后，使用项目 code-review skill 对 WebSocket transport、连接生命周期、增量前缀匹配、fallback 和持久化边界做针对性 review。
2. 修复针对性 review 发现的全部真实 P0、P1，以及有充分证据且值得本期处理的问题；不为基本不存在的极端假设增加虚空防御。
3. 修复后重新执行受影响的确定性测试、完整项目验证、真实 LLM TUI smoke test 和针对性 code-review；循环到无未解决 P0/P1。
4. 只有针对性复验通过后，才使用 code-review skill 对当前分支相对基线的全部改动做全量 review，并执行整体验收。
5. 若全量 review 发现 P0/P1，完成修复后重新执行完整项目验证、受影响的真实 LLM TUI smoke test 和全量 review；循环到无未解决 P0/P1。
6. 最后逐项核对本文目标、非目标、35 项拍板、分阶段验收和验收矩阵，确认实现没有遗漏或静默改变既有语义。

验收：

- format、Clippy、test、type check 与项目规定验证全部通过。
- 针对性 review 和全量 review 均无未解决 P0/P1。
- 真实 LLM TUI smoke test 在最终相关修复后重新通过，正常路径没有频繁或非预期 fallback。
- 最终实现逐项对照本文目标、非目标、拍板和验收矩阵，无静默语义漂移。

## 7. 验收矩阵

| 场景 | 预期 transport | 关键断言 |
|---|---|---|
| 默认配置 | HTTP SSE → JSON | 不建立 WebSocket |
| fresh session 首轮 | WebSocket full input | 无 `previous_response_id` |
| 连续第二轮 | WebSocket incremental | 同连接、最新 ID、只发送 suffix |
| 未变的持久化 ModelContext | WebSocket incremental | 稳定 context 已在精确前缀，只发送新 user suffix |
| 日期、background 或 delegation context 变化 | WebSocket incremental | 新 ModelContext snapshot 与 user input 作为 suffix |
| 工具调用回环 | WebSocket incremental | tool output 为新增 item，工具只执行一次 |
| 并发子代理 | 多条 WebSocket lease | 不被一个全局锁串行化 |
| 池有空位且仅有其他 chain 的 idle 连接 | 新建 WebSocket | 不清除其他 chain 的 continuation affinity |
| 连接池满载后归还 idle | 等待后复用连接 | 不因 idle 连接持有 permit 而永久阻塞 |
| Memory/Structured JSON | HTTP JSON | 不占用 WebSocket pool |
| Router rerank | HTTP JSON | 行为不变 |
| 进程退出后 resume | WebSocket full input | 不依赖旧 ID，完整历史重建 |
| resumed 后下一轮 | WebSocket incremental | 使用新链产生的新 ID |
| compact 后继续 | WebSocket full input | 旧链失效，以有效 compact history 开新链 |
| model/协议切换 | full input / 其他 adapter | 不跨 identity 复用 |
| `previous_response_not_found` | 新连接 full input | 恢复一次，不丢 replay |
| WS 无 delta 断开 | WS retry → SSE → JSON | 不提交空结果 |
| WS 已输出文本后断开 | HTTP JSON replacement | 不重复追加 SSE 文本 |
| WS 已输出文本后断开后的下一 turn | runtime-chain sticky HTTP | 当前 turn JSON 完整替换；后续不再重复 WS partial/断流 |
| reasoning-only/tool-only 中断 | 失败恢复 | 不提交空成功、不执行半截工具 |
| `response.completed` 缺 ID | 失败恢复 | 不提交已收到的 partial，不猜测或生成 ID |
| hard cancel/in-flight 中断 | 无 fallback | chain 与未知状态连接均废弃 |
| safe-boundary steering | 无 fallback | chain 废弃；健康 socket 可供 full history 复用 |
| safe steering 后当前 WS 断流 | interrupted，无恢复请求 | 不二次握手、不发 HTTP POST、不记录 fallback attempt |
| WS 降级后 SSE 响应头超时 | HTTP JSON replacement | SSE 内部 retry 耗尽后进入既有 fallback，不直接结束 turn |
| `max_output_tokens` partial 后 continuation 返回确定性 4xx | terminal failure | 不启动 JSON replacement，不提交 partial |
| wrapped WS error 返回 429/5xx | WS 有限 retry → HTTP | 保留数值状态分类，不误判为确定性 `Failed` |
| wrapped WS error 返回 400/401 | terminal failure | 不 retry、不 sticky、不通过 HTTP 掩盖权威错误 |
| wrapped 429/5xx 重试耗尽后的下一 turn | 重新尝试 WebSocket | 暂态 response 错误不污染 runtime-chain sticky 状态 |
| WS 握手返回 426 | 立即 HTTP；runtime-chain sticky | 不消耗 WS retry，当前请求使用 full history 重放 |
| WS 握手返回 404/405 | WS 有限 retry → HTTP；runtime-chain sticky | 达到 `retry_count` 前不降级，耗尽后后续 turn 不再握手 |
| WS 握手返回 400/403 等其他状态 | WS 有限 retry → HTTP；runtime-chain sticky | HTTP 可用时恢复；401 除外 |
| 主 session 握手持续失败 | runtime-chain sticky HTTP | 主 session 后续 turn 不重复慢握手，subagent 可继续 WS |
| WS request 达到 `timeout_secs` | 丢 socket后 HTTP fallback；runtime-chain sticky | 外层不抢先取消恢复分类，同一 chain 下一轮不再握手 |
| Provider WAL 超过 10 秒 | terminal preparation failure | request WAL 不发起网络 I/O；response WAL 不提交 turn；不 fallback |
| 单个 subagent 握手持续失败 | runtime-chain sticky HTTP | 只影响该 subagent |
| 连接达到 60 分钟 | 新连接 full input | 不把 60 分钟当 session/ID TTL |
| 未 compact 历史媒体 | full input 保留媒体 | 图片/PDF 不变成占位符 |

## 8. 整体验收标准

实现只有同时满足以下条件才算完成：

1. `openai_responses` 在显式开启时支持 WebSocket，默认关闭时行为完全兼容。
2. connection lease 支持并发，不存在跨完整 response 的全局串行锁。
3. first/full、incremental、reconnect、resume、compact 和 model/协议切换的边界都有确定性测试。
4. response ID 仅存在于连接内存，不落盘、不跨连接、不成为 session 权威状态。
5. `store = false`、Reasoning replay、历史媒体、工具安全门和 canonical commit 语义不变。
6. WebSocket 故障不会制造重复 TUI 文本、重复工具副作用、空成功或 partial commit。
7. HTTP SSE/JSON 始终可独立工作；WebSocket 不可用时能按拍板路径恢复，sticky downgrade 不跨 runtime chain 扩散。
8. 真实 LLM TUI fresh session、多轮、工具、并发和 resume smoke test 验收通过，正常路径无频繁非预期 fallback；相关修复后必须重跑受影响场景。
9. 不在 `config.template.toml` 增加 `supports_websockets`。
10. 仓库没有真实 secret、私有 endpoint、私有响应、真实用户名或附件 base64。
11. 项目完整验证通过；针对性 code-review 通过后才进入全量 review，最终两轮 review 均无未解决 P0/P1。

### 8.1 完成与验收证据（2026-08-10）

确定性协议与状态机验收：

- WebSocket fake server 测试覆盖 full/incremental、严格 envelope 与 input 前缀、持久化
  ModelContext suffix、Reasoning、工具、连接 lease、并发、池容量、idle pump、连接淘汰、
  `previous_response_not_found`、终态/stream 损坏、timeout、cancel/steer、握手状态、
  SSE/JSON fallback、partial replacement 与 sticky 分类。
- session 与 adapter 测试分别覆盖 resume 创建新 runtime chain、同一 handle clone 保持 chain、model/协议 replay 世代、未 compact 图片/PDF与 Reasoning replay、session reload、compacted prefix 丢弃媒体/replay、HTTP 默认路径及 Memory/Structured JSON/Router 非 WS 边界。
- 持久化验收不依赖一条把所有组件耦合在一起的大型测试，而由上述 session round-trip/compaction 确定性测试、真实 TUI resume，以及真实 session 文件扫描共同闭环；三组真实 session 的 `messages.jsonl` 与 `turn_events.jsonl` 均不存在 `response_id` 或 `previous_response_id`。

项目完整验证：

- `cargo fmt --all -- --check` 通过。
- `cargo clippy -- -D warnings` 与 `cargo clippy --locked --all-targets -- -D warnings` 通过。
- `cargo test` 的 library、binary、integration 与 doc tests 全部通过。
- `cargo check` 与 `cargo check --locked --all-targets` 通过。
- `git diff --check` 通过；改动扫描未发现真实 secret、私有 endpoint、真实用户名、私有响应或附件 base64；`config.template.toml` 未增加 `supports_websockets`。

真实 LLM TUI 验收：

- 最终代码 fresh session 连续完成两轮 streaming：2 个 committed turn、12 个 assistant delta、0 个 failed turn、0 次 non-streaming fallback、stderr 为空；第二轮正确读取上一轮上下文，退出后 session 正常关闭。
- 工具与 resume 场景连续完成 4 个 committed turn、13 个 assistant delta和一次完整 `file_read` 工具回环；退出 finalize 后 resume 成功，resume 后继续完成两轮，0 次 fallback。
- 并发子代理场景在一个 committed turn 内创建两名子代理并完成 wait/read 回环，共 5 次完整 delegation 工具调用、8 个 assistant delta、0 次 fallback。
- 最后一项全量 review 修复仅影响“已有 partial 后 transport 损坏”的故障路径；修复后已重跑该路径的 fake turn-loop 回归和 fresh 两轮真实 TUI。真实 endpoint、凭据和响应正文均未写入仓库。

Review 门禁：

- 针对性 code-review 反复复验到无未解决 P0/P1。
- 全量 code-review 首轮发现的“已有 partial 后连接损坏没有 sticky 当前 runtime chain”已按 33B 修复，并新增 transport、429 非 sticky 和 turn-loop JSON replacement 后下一轮 sticky 三层回归。
- 修复后的最终独立全量 review 明确结论为“无 P0/P1 发现”，并逐项确认 35 项拍板都有对应实现路径，没有发现 PRD 产品语义或协议语义的遗漏实现。

最终审计结论：目标、非目标、35 项拍板、阶段 0–8、验收矩阵与整体验收标准均已对照实现和证据；未发现未实现项，也未发现静默改变既有 HTTP、session、Reasoning、媒体、工具或 TUI 语义的情况。

## 9. 风险与回滚

### 9.1 主要风险

- 兼容 endpoint 声称支持 WebSocket，但事件、错误或 previous-state 缓存语义不完整。
- 请求字段漏入 strict match，导致错误地对变化后的请求使用增量 input。
- 连接在 hard cancel 或未知终态后被错误归还，污染下一次 response；或 safe-boundary steer 后错误保留了已与 committed history 分叉的 continuation state。
- 单连接限制被误实现成 adapter 全局串行，拖慢并发子代理。
- sticky downgrade 被误实现成 adapter 全局状态，导致一个 session 的故障关闭所有 subagent 的 WebSocket。
- WebSocket partial 与 HTTP fallback 结果同时写入 TUI 或 session。

### 9.2 控制措施

- endpoint 能力由显式配置声明；正常默认仍走已验证的 HTTP 路径。
- 增量匹配使用完整 envelope 与精确 input prefix，持久化 `ModelContext` 不被特别删改；匹配失败安全地发送 full input。
- 只有状态明确的健康连接才能归还空闲池；未 committed turn 的 continuation state 必须清空。
- fake server 对 frame、并发、取消、缓存失效和 fallback 做确定性验证。
- response ID 完全不持久化，resume 永远可从本地权威状态重建。

### 9.3 回滚

- 用户将 `supports_websockets` 设为 `false` 即恢复现有 HTTP SSE/JSON 行为。
- runtime-chain sticky downgrade 可在 WebSocket 暂时不可用时保持当前 session/delegation 可用，同时不关闭其他独立 chain 的 WebSocket。
- 由于不修改 session schema，回滚到不支持 WebSocket 的版本仍能读取本期产生的 session。

## 10. 后续新增拍板规则

实施中若出现本文未覆盖且会改变用户可见语义、持久化格式、并发边界或 fallback 顺序的新问题，必须先追加为新的编号拍板项，再实施。新增项不得与本文已拍板语义冲突，也不得使已有验收标准失效。
