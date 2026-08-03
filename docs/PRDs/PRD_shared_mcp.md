# PRD：MCP 常驻连接复用与跨 Agent 并发语义

> 状态：已实现。共享连接、请求级取消、并发和 generation fencing 已完成；完整验证、
> 基础 TUI smoke 与四个真实 LLM TUI smoke 均通过。
>
> 关联文档：`docs/PRDs/PRD_support_mcp.md`、`docs/PRDs/PRD_subagents.md`、
> `docs/PRDs/PRD_parallel_tools.md`

## 1. 背景与当前基线

ACN 当前已经具备以下基础：

- `McpConnectionManager` 在启动、discovery 和状态展示阶段持有每个 ready server 的`Arc<McpClient>`。
- `ToolRegistry` 通过 `Arc<McpConnectionManager>` 接入 MCP；parent registry clone 后生成delegation child registry，因此主 agent 与所有子 agent 已经共享同一个 manager。
- 每个 `AgentTurnLoop` 已经有本地工具并发调度：只有原始 MCP `annotations.readOnlyHint == true` 的工具可以加入该 agent 当前 turn 的并发批次；annotation缺失、为 `false` 或异常时 fail-closed，按串行 barrier 执行。
- `call_read_only_tool` 会先检查当前工具 snapshot，再通过实时 `tools/list` 复核`readOnlyHint`，避免 server 在 discovery 后把工具从只读降级为可写。
- generation 已用于隔离 disable、enable、reconnect 与旧连接结果。

但实际 `tools/call` 仍然走短生命周期连接：每次调用重新执行`connect -> initialize -> tools/list -> tools/call -> shutdown`。这带来以下问题：

1. stdio server 每次调用都会新建进程，无法保留 server 侧会话和缓存。
2. Streamable HTTP 每次调用会建立新的逻辑 MCP session，无法稳定复用`MCP-Session-Id`。
3. 高频调用重复支付 initialize 和 discovery 成本。
4. 单次 tool timeout 通过 `shutdown` 关闭 client 的行为只适合一次性 client；改成长连接后会误伤同一连接上的其他 in-flight request。
5. `docs/PRDs/PRD_subagents.md` 的 S/W 拍板仍把短生命周期 client 写成“保证并发”的实现约束，与本 RPD 的共享常驻连接方案冲突。

本需求只改变 MCP client 的生命周期与错误隔离方式，不重做现有工具批次调度器。

## 2. 目标

- 在一个 ACN 进程运行期内，同一个 configured MCP server 只维护一个 ready 的常驻`McpClient` 和一条逻辑 MCP session。
- 主 agent 与其所有子 agent 复用同一个 `McpConnectionManager`、同一个 server client 和同一条 MCP session。
- 不在 ACN 增加跨 agent 的 server lock；不同 agent 的调用是否在 server 侧真实重叠由 transport与 server 的响应形态决定。
- 保留每个 agent 内现有的 `readOnlyHint` 并发分类与 fail-closed 二次校验。
- 单个请求的参数错误、业务失败、取消或超时不关闭共享连接，不影响同连接的其他请求和后续请求。
- disable、reconnect、transport 断开等连接级事件能够安全替换旧 client，旧请求的迟到结果或错误不能污染新 generation。
- stdio 与 Streamable HTTP 都有自动化证据和真实 LLM TUI 证据证明连接确实被复用。

## 3. 非目标

本期明确不做：

- 不增加跨 agent、跨 session 或跨进程的 MCP 全局锁。
- 不增加 per-server `max_in_flight`、全局限流器或 server 并发能力猜测。
- 不根据工具名称、参数或业务语义推断跨 agent 调用是否安全。
- 不改变 `docs/PRDs/PRD_parallel_tools.md` 已拍板的 agent 内工具分类矩阵。
- 不把 `readOnlyHint = false` 解释为“整个 ACN 进程内必须互斥”；它只影响当前`AgentTurnLoop` 内的批次调度。
- 不自动重试 `tools/call`。请求可能已在 server 侧执行，自动重试会造成重复副作用。
- 不实现跨 ACN 进程共享连接；两个独立 ACN 进程各自拥有自己的 MCP client/session。
- 不在本需求中升级 MCP SDK、实现 MCP Tasks、elicitation、OAuth 或动态 tool-list notification，除非现有 SDK 缺陷直接阻塞本需求且另行确认。
- 不新增“短连接/长连接”用户配置开关；常驻复用是统一运行时语义。

## 4. 已拍板决策

### 4.1 连接所有权与复用范围

复用单位是：

```text
一个 ACN 进程
  -> 一个 McpConnectionManager
    -> 每个 configured server 一个 ready Arc<McpClient>
      -> 一条 MCP logical session
        -> 多个可同时 in-flight 的 JSON-RPC request
```

- stdio：一个 ready server 对应一个常驻 child process。主 agent、所有 delegation child 和后续 turn 都复用该进程。macOS/Linux 下 child 位于 ACN 独占的进程组；disable、reconnect、transport failure 或进程 shutdown 收束连接时，同组后代也必须被清理。异步关闭被取消或 runtime 正在退出时，由 child wrapper 的同步 Drop guard 终止进程组，并交给独立 reaper 回收直属 child。主动创建新 session/进程组逃逸的 daemon 不属于该保证。
- Streamable HTTP：一个 ready server 对应一个初始化后的逻辑 session；server 返回`MCP-Session-Id` 时，后续请求继续携带同一 session id。当前锁定的 `rmcp 3.0.1` 对“server 延迟后返回 JSON”的同一 session POST 在 transport worker 内串行，这是已接受的 SDK/transport 边界，不用短连接、连接池或 ACN 锁绕开。
- `McpConnectionManager` 是连接生命周期的唯一 owner；`ToolRegistry`、parent 和 child 只持有同一个 manager 的 `Arc`，不单独创建 client。
- CLI `mcp status` 等独立命令如果启动的是另一个 ACN 进程，仍有自己的短暂进程级 manager；这不属于“同一运行中 TUI session”的复用范围。

### 4.2 并发边界

| 范围 | 调度规则 | 本期是否新增锁 |
| --- | --- | --- |
| 同一个 agent、同一个 assistant response | 保留现有规则：连续且 `is_concurrency_safe = true` 的调用并发；其他调用形成串行 barrier | 否 |
| MCP `readOnlyHint == true` | 可进入该 agent 的本地并发批次，实际派发前仍二次复核 | 否 |
| MCP annotation 缺失、`false` 或异常 | 在该 agent 内串行执行 | 否 |
| parent 与 child | 各自的 `AgentTurnLoop` 独立推进，请求可以重叠 | 否 |
| child 与 child | 各自的 `AgentTurnLoop` 独立推进，请求可以重叠 | 否 |
| 不同 ACN 进程 | 互不感知，各自连接 server | 否 |

因此，两个 agent 即使同时调用同一个未声明只读的 MCP tool，也允许在共享 client 上并发in-flight。ACN 不主动判断这类跨 agent 调用应串行还是并行；MCP server 必须负责自身的并发、事务、幂等和资源互斥。如果特定 server 不耐并发，后续应通过显式 server 配置或 capability契约单独设计，不能由 ACN 猜测。

现有 `agent.tool.max_parallel_tool_calls` 仍只限制一个 agent 当前 turn 的连续安全批次，不是跨 agent 的连接池上限。

### 4.3 共享连接必须支持请求复用，且接受 HTTP transport 差异

MCP/JSON-RPC 使用 request id 匹配 response。ACN 不得因为共用一个 client 在外围加 mutex，把所有 `tools/call` 退化成串行；但 transport 自身的排队不由 ACN 绕开。

实施前需要用当前 `rmcp` 版本的实现和定向测试确认：

- peer 可以安全并发发起多个 request；
- response 按 id 路由到各自 waiter；
- request-scoped progress/cancellation 不串线；
- stdio 单一读写 transport 支持多个 in-flight request；定向测试必须证明 parent/child 调用真实重叠。
- Streamable HTTP 必须复用同一 logical session，但对慢 JSON response 接受 `rmcp` worker 的串行POST 行为；`202 Accepted` 或尽早建立 SSE response stream 的 server 仍可在 server 侧重叠执行。

若 stdio 的单 client 无法支持多 in-flight request，这是阻塞性技术事实，必须回到本文追加拍板，不能静默恢复为每次调用重连。HTTP 的上述已验证差异不构成阻塞。

### 4.4 `readOnlyHint` 保留两层 fail-closed 校验

`readOnlyHint` 只决定 agent 内并发资格，但仍要防止 discovery 后 annotation 变化：

1. 调度分类时读取 ready snapshot，只有明确 `true` 才进入安全批次。
2. 实际派发 `call_read_only_tool` 时再次检查 snapshot。
3. 在同一个常驻 client/session 上执行实时 `tools/list`，确认目标 tool 仍暴露且`readOnlyHint == true`。
4. 实时 `tools/list` 返回后再次检查 generation 与 ready 状态，确认期间没有发生disable/reconnect。
5. 任一步失败都不发送 `tools/call`，仅让该 call fail-closed。

普通串行 MCP call 不需要每次重新 `tools/list`；它直接使用当前 ready client。这样既保留只读并发的安全边界，也消除所有调用都重复 discovery 的成本。

### 4.5 超时和取消是 request-scoped

`tool_timeout_secs` 约束单个 `tools/call`，不是共享连接的 idle timeout 或 session timeout。只读调用的实时 `tools/list` 复核属于该 `tools/call` 的 admission：它也必须使用 request-scoped cancellation 与从 admission 开始计算的绝对 deadline，避免卡住 rmcp 的同 session HTTP worker；这不改变它不触发`tools/call` 重放的语义。

- 每个 request 使用 SDK 的 request timeout/cancellable request 能力。
- 超时尽量发送对应 request 的 MCP cancellation notification。
- 对 Streamable HTTP 的慢响应，受限的 `tools/call` 或实时 `tools/list` 的 HTTP response headers 与确认 JSON 后的 body 共用同一个 request deadline 保护窗口，以释放 rmcp 单 worker 供同 session 的peer request 继续；本地 deadline 元数据必须在 HTTP 发包前剥离，server 不可见。SSE response 一旦确认，不再施加 JSON body 保护，仍保留完整 `tool_timeout_secs` 以持续发送 progress。若远端已经开始执行，该中断不推断远端副作用，更不会重放 call。
- 对 turn、lifecycle 或 caller-abort 的本地取消，若该 request 正在 Streamable HTTP adapter 内等待`reqwest` 的 response headers 或 JSON body，必须先取消该 HTTP future 以立即释放 rmcp 同 session worker，再尽力异步发送对应 MCP `notifications/cancelled`；notification 不能成为释放 worker 的前提。
- 单个 request timeout、caller future 被取消或用户取消 turn 时，不调用共享 client 的`shutdown()`。
- 同连接上其他 in-flight request 继续运行；超时后发起的后续 request 仍应成功。
- 移除当前“SDK request timeout + 外层相同 `tokio::time::timeout`”的重复竞态，只保留一层权威request deadline；除非测试证明 SDK 无法覆盖某 transport，并在文档追加原因。

只有连接级事件可以关闭 client，例如 transport 已关闭、发送通道失效、client driver 终止、disable、reconnect 或 ACN shutdown。

### 4.6 错误分类与 server 状态

| 错误类别 | 示例 | 是否把 server 标成 failed / 移除工具 |
| --- | --- | --- |
| 参数/应用级 | arguments 非 object、JSON-RPC invalid params、MCP tool 返回 `isError` | 否 |
| request 生命周期 | 单项 timeout、request cancellation、caller/turn cancellation | 否 |
| 远端单项失败 | 有效 JSON-RPC error response，且 transport/session 仍可用 | 否 |
| 连接级 | transport closed、send channel closed、driver task 退出、协议状态已不可继续 | 是 |
| 管理级 | disable、reconnect、ACN shutdown | 主动移除/替换，不作为普通 tool 业务失败 |

当前 `McpClientError::ToolCall { message }` 过于宽泛，实施时必须保留足够的结构化错误来源，使manager 能区分 request-scoped 与 connection-scoped；不能继续用“除 invalid params 外所有错误都 mark failed”的粗粒度规则。

特别地，rmcp 的 `TransportSend` 也可能只是单个 Streamable HTTP POST 的 401/5xx、坏 content type 或 response-body 失败，并不等价于 driver 已关闭；这类错误保持 request-scoped。当前实现仅在`TransportClosed` 等明确终止信号下淘汰 ready client。

连接级失败后不自动重放当前 tool call。建立连接和显式 Reconnect 使用 `src/config.rs` 内部常量的有限指数退避（额外重试次数、base delay、max delay），这些运行时常量不暴露到 TOML；这是本需求明确拍板的产品决定，优先于仓库一般的“可调超参数配置外置”约定。即使重连成功，也只能恢复连接，不能自动重试语义未知的 tool call。

### 4.7 generation fencing 与生命周期切换

每次调用从 manager state 获取以下同一份快照：

- server ready 状态；
- `Arc<McpClient>`；
- 当前 generation；
- 调用所需的 timeout/tool metadata。

持有 state mutex 的临界区只做校验和 clone，不跨任何 `.await`。调用、实时 `tools/list`、取消和progress 均在释放锁后执行。

disable/reconnect/连接级失败必须：

1. 在 state 内 bump generation，并摘除旧 client/工具状态。
2. 释放 state lock。
3. 取消并等待旧 pending connect attempt 的底层 transport 真正完成 `close()`，再 shutdown 旧 client，使其 in-flight request 收束为取消或连接关闭；不得只取消 token、drop connect future 或只看到`ConnectAttempt` future 返回后便建立 replacement transport。
4. reconnect 成功后只把新 client 安装到对应的新 generation。

旧调用完成后，只有在“server name + captured generation + 必要时 client identity”仍匹配时，才能修改当前 server 状态。旧 generation 的迟到错误绝不能把新 client 标成 failed；迟到成功也不能重新暴露旧工具。

disable/reconnect 会取消旧 client 上所有 in-flight request，这是用户显式生命周期操作的既定语义；它不同于单个 tool timeout。

关闭旧 transport 受 `MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS` 限制（transport 自身 graceful shutdown完成时允许极小的内部调度余量）。若在该窗口内未确认退出，manager 将该 server 标记为本进程内不可替换（quarantine），并拒绝后续 refresh、enable 或 reconnect建立 replacement client。由于当前 `rmcp::RunningService::close_with_timeout` 超时后不再保留可 await的 join handle，ACN 无法在进程存活期间安全地重新确认旧 transport 已退出；恢复该 server 的方式是重启 ACN 进程，而不是冒险建立并行的同名连接。这个保守状态不影响其他 server。

### 4.8 progress 路由

- 继续由 manager 的全局原子计数器生成唯一 progress token。
- progress route 至少以 `server_name + progress_token` 区分，不因共享连接而合并。
- parent/child 身份继续通过本地 `ToolDispatchContext.current_turn_id`、reporter 和事件链归属，不向第三方 MCP tool arguments 注入额外字段。
- 测试必须覆盖并发 progress 乱序到达时仍更新各自的 TUI ToolCell/subagent 事件。

### 4.9 共享连接的选择依据

本 PRD 明确选择“同一 ACN 进程内共享一个 manager/client”。当前 parent/child 已经共享`ToolRegistry` 中的 manager，这一拓扑既能保留 stdio server 状态、减少 initialize 开销，也让连接生命周期、generation fencing 和 progress 路由只有一个权威 owner。调用能否并行仍由各agent 自身调度与 MCP server 处理，不增加跨 agent 串行锁。

## 5. 目标调用流程

```text
AgentTurnLoop(parent 或 child)
  -> ToolRegistry 现有本地并发分类
  -> McpConnectionManager::call_tool / call_read_only_tool
     1. 锁内校验 ready，clone Arc<McpClient> + generation
     2. 释放锁，注册唯一 progress token
     3. read-only 路径在同一 client 上实时 tools/list 并做 generation fence
     4. 在共享 client 上发送 tools/call
     5. 仅收束当前 request 的 result/timeout/cancel
     6. 只有连接级错误且 generation 仍匹配时才摘除 client
  -> 当前 AgentTurnLoop 按既有 source order 回灌 tool_result
```

这个流程不要求 manager 知道调用来自主 agent 还是子 agent，也不要求 manager 决定两个 agent之间的先后顺序。

## 6. 分阶段 TODO 与验收门槛

每个阶段开始前重新阅读本文和三个关联 PRD。遇到本文未覆盖、会改变产品语义的分歧时，先在“追加拍板记录”中列出选项并确认，不得在代码里暗自选择。

### Phase 0：文档与基线锁定

Todo：

- [x] 记录当前 discovery client 常驻、`tools/call` 短连接的真实基线。
- [x] 记录 parent/child 已通过 cloned `ToolRegistry` 共享同一 manager。
- [x] 固化 agent 内 `readOnlyHint` 调度、跨 agent 无锁的并发语义。
- [x] 列出阶段化实施、自动化验证、真实 LLM TUI smoke 与最终 code-review skill gate。

验收：

- 本文不存在未拍板的产品阻塞项。
- 本阶段只产生文档变更，没有 Rust 源码或测试代码变更。

### Phase 1：共享 client 调用路径

Todo：

- [x] 在 `McpConnectionManager` 中增加锁内获取 ready client lease/snapshot 的窄逻辑，返回`Arc<McpClient> + generation`；不得持锁跨 `.await`。
- [x] `call_tool` 改为直接调用该常驻 client，删除每次`connect/initialize/list_tools/shutdown`。
- [x] `call_read_only_tool` 保留 snapshot 检查，并把实时 `tools/list` 改到同一常驻 client。
- [x] 在实时 `tools/list` 与 `tools/call` 之间保留 ready/generation fence。
- [x] 确认 `ToolRegistry::clone/for_delegation` 继续保留同一个 manager `Arc`，不为 child创建 manager/client。
- [x] 清理已经失效的“短生命周期 call client”注释和 helper，但不做无关重构。

验收：

- 连续多个普通调用只 initialize 一次，且不为每次调用重复 `tools/list`。
- read-only 调用仍执行实时 `tools/list`，但 initialize、session 与 stdio PID 不变。
- parent 与 child 观测到相同的 manager/client identity。
- manager state mutex 没有跨 `.await` 持有。

### Phase 2：request-scoped timeout、取消与错误分类

Todo：

- [x] 在 `McpClient` 中保留唯一权威 request deadline，移除会关闭整个 client 的单项 timeout分支和重复 timeout race。
- [x] 验证/保留 SDK RequestHandle 的 cancellation notification 行为。
- [x] 将 `McpClientError` 拆分到足以判断 request-scoped 与 connection-scoped 的粒度。
- [x] 修改 manager 的失败判定：参数、业务、timeout、cancel 和有效 JSON-RPC error 不摘除ready client。
- [x] 连接级错误只在 captured generation/client 仍为当前值时 mark failed。
- [x] disable/reconnect/shutdown 摘除旧 client 后在锁外关闭，并保证旧错误无法污染新 generation。

验收：

- 一个 timeout request 失败时，同连接另一个 in-flight request 和后续 `ping` 均成功。
- timeout 后 server 仍为 ready，工具仍暴露。
- transport 真正断开时 server 进入 failed、工具被移除，错误可诊断。
- reconnect 后旧 generation 的迟到错误不影响新 ready client。

### Phase 3：并发、progress 与生命周期集成测试

Todo：

- [x] 为 stdio fixture 记录 PID、initialize/list/call/cancel/shutdown 事件（可由事件数计数）、request id、progress token 和单调时钟区间。
- [x] 为 Streamable HTTP fixture 记录 initialize 次数、`MCP-Session-Id`、request / cancellation事件；慢 JSON response 的 rmcp worker 串行现象作为已接受的 transport 事实记录。
- [x] 覆盖同一 agent 内 read-only 并发以及 non-read-only barrier，确认现有调度语义未变。
- [x] 用 stdio fixture 覆盖 parent + child、child + child 对同 server 的请求重叠，不添加跨 agent gate；HTTP 慢 JSON response 只验证同 session 复用和没有 ACN 额外短连接。
- [x] 覆盖并发 progress 的 caller/ToolCell 归属。
- [x] 覆盖 read-only annotation 在 discovery 后降级时 fail-closed，且未发送 tool call。
- [x] 覆盖 disable/reconnect 时 in-flight call 的收束、旧进程清理和新 generation 安装。

验收：

- stdio 在 reconnect 前始终只有一个 server PID、一次 initialize。
- HTTP 在 reconnect 前使用同一个逻辑 session id。
- stdio 下两个跨 agent 非只读调用的服务端执行区间真实重叠；Streamable HTTP 慢 JSON response场景验证同 session 串行且没有额外短连接。
- 同一 agent 内非只读调用仍按 source order 串行。
- 每个 progress token 只更新对应调用，未发生串线或 route 泄漏。
- manager shutdown 后 stdio child 不残留，HTTP session/driver 正常收束。

### Phase 4：文档一致性与回归清理

Todo：

- [x] 修订 `docs/PRDs/PRD_subagents.md` 的 S/W 实现约束：删除“每次调用短连接”，保留 S1/W3的 server-side concurrency 拍板。
- [x] 修订 `docs/PRDs/PRD_support_mcp.md` 的 timeout 文案：单项 timeout 取消 request，不默认清理整条ready session。
- [x] 核对 `docs/PRDs/PRD_parallel_tools.md`，确保 agent 内 `readOnlyHint` 语义没有被扩大到跨 agent。
- [x] 如用户文档涉及连接生命周期，说明同一 ACN 运行期复用连接、Reconnect 会替换连接。
- [x] 删除或更新与短连接假设绑定的测试名称、注释和日志文案。

验收：

- 三份关联 PRD 与实际实现不再互相矛盾。
- 没有文档暗示 `readOnlyHint = false` 会产生跨 agent 全局锁。
- 没有文档承诺失败 tool call 自动重试。

### Phase 5：完整自动化验证

Todo：

- [x] 先 `source export_env.sh`。
- [x] 运行 `cargo fmt --check`。
- [x] 运行 `cargo clippy -- -D warnings`。
- [x] 运行 `cargo test`，包括 Phase 1～3 新增的定向测试。
- [x] 运行 `cargo check`。
- [x] 对失败用例先修实现或测试前提，不跳过、不降低断言。

验收：

- 上述命令全部通过且 clippy 无 warning。
- 新增测试同时覆盖 stdio 与 Streamable HTTP，不只验证其中一种 transport。
- 测试断言使用 server 侧结构化日志、PID/session id、request 区间和计数，不只检查 ACN 返回了“success”。

### Phase 6：针对性真实 LLM TUI smoke test

本阶段是代码完成后的硬门槛，必须使用项目`.agents/skills/tui-smoke-test-with-tmux/SKILL.md`。测试必须运行真实 `acn` TUI、真实配置的LLM provider 和真实 MCP transport；不得用 fake provider、预录 assistant response 或直接绕过TUI 调用 manager 来冒充本验收。

Todo：

- [x] `source export_env.sh`，确认真实 provider/model 环境可用。
- [x] 基于 skill 的 scenario template 建立受版本控制的`.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_shared_mcp_real_llm.sh` 与`shared_mcp_real_llm_fixture.sh`。
- [x] 启动可控但协议真实的本地 **stdio** MCP fixture，运行真实 LLM/TUI 的复用场景；Streamable HTTP 的 session 复用、慢 JSON 响应串行接受性和 lifecycle 隔离由 integration tests覆盖。fixture 至少提供：
  - `slow_read`：`readOnlyHint = true`，可配置耗时并发送 progress；
  - `slow_write`：annotation 缺失或为 `false`，可配置耗时；
  - `timeout_once`：第一次执行超过 tool timeout；
  - `ping`：快速返回当前 PID/session id。
- [x] 场景 A：要求主 agent 在一个 assistant response 中相邻调用两次 `slow_read`，确认同 agent read-only 调用重叠、TUI 同时出现两个 MCP ToolCell、最终回灌顺序仍符合现有规则。
- [x] 场景 B（stdio fixture）：要求主 agent 创建至少两个子 agent，每个 child 调用同一 server 的`slow_write`；确认 child/child 请求重叠。可再让 parent 同时调用一次，验证 parent/child 也不互锁。Streamable HTTP fixture 的慢 JSON response 只验同 session 复用和没有额外 initialize，不把重叠作为通过条件。
- [x] 场景 C：让一个 agent 触发 `timeout_once`，同时让另一 agent 执行可在 deadline 内完成的调用；timeout 收束后再调用 `ping`，并第二次调用 `timeout_once`，确认首次 timeout、其他调用和后续调用均不改变共享 PID/session id，且 fixture 确实只让第一次调用超时。
- [x] 场景 D：在 idle 状态通过 `/mcp` Reconnect，确认旧 stdio child 被清理、initialize count精确增加一次、新 PID/session 安装成功，随后 `ping` 成功。
- [x] 每个 checkpoint 用 `tui_capture` 保存 pane 文本；同时保存 MCP fixture 的 JSONL 结构化日志、ACN 日志和 `stderr.log`。
- [x] 使用稳定 UI marker 做 tmux 断言；并发与复用结论必须由 fixture 日志断言，不能依赖模型在最终回答里自述“已经并发/复用”。
- [x] 断言 `stderr.log` 为空并清理 tmux session、MCP child 和临时配置。

硬通过条件：

- Reconnect 前 stdio initialize count 为 `1` 且只有一个 PID；HTTP session 复用由 integration tests 以同一个 `MCP-Session-Id` 断言。
- stdio fixture 的并发区间满足 `start_a < end_b && start_b < end_a`，不能只比较日志行顺序；HTTP慢 JSON response 的 transport 串行记录为可接受结果。
- stdio agent 内两次 `slow_read` 重叠；agent 内未声明只读的 barrier 回归由自动化测试保证串行。
- stdio child/child 至少一组 `slow_write` 真实重叠，证明 ACN 没有跨 agent server lock。
- 首次 `timeout_once` 失败不改变同连接其他成功调用；之后 `ping` 和第二次 `timeout_once` 使用相同PID/session id 成功，证明 fixture 的“只首次超时”语义真实生效。
- Reconnect 后旧连接确实退出，且只建立一条新的 ready 连接。
- TUI 没有残留 Calling 状态，progress 没有串到错误 ToolCell，`stderr.log` 为空。

真实 LLM 可能没有按提示生成所需的 tool-call 形态。此时该次尝试记为“场景未形成”，不算功能失败也不算通过；可以调整提示后最多重试 3 次，并保留每次 capture。3 次仍未形成时，本阶段阻塞，需要人工确认提示或 provider 能力，不能改用 fake response 替代。

### Phase 7：code-review skill 验收

代码、自动化验证和真实 LLM TUI smoke 全部完成后，必须使用`.agents/skills/code-review/SKILL.md` 检查以下风险域：

- manager 共享连接、generation fencing 与 lock 范围；
- client timeout/cancellation、错误分类与 transport 生命周期；
- parent/child 并发、progress 路由与 ToolRegistry 接入；
- stdio/HTTP fixtures、TUI smoke 证据和文档一致性。

最终验收：

- 无未处理的 P0/P1 或其他确认需修复的问题。
- code-review skill、完整验证和受影响的 TUI 场景均通过。

## 7. 验证矩阵

| 编号 | 场景 | 层级 | 关键证据 |
| --- | --- | --- | --- |
| V01 | 同 server 连续普通调用 | stdio integration | initialize=1、PID 唯一、call>1 |
| V02 | 同 server 连续普通调用 | HTTP integration | initialize=1、session id 唯一 |
| V03 | read-only 实时复核 | manager integration | 同 client `tools/list`；降级后无 `tools/call` |
| V04 | 同 agent read-only 并发 | stdio turn-loop integration | 区间重叠、source-order result |
| V05 | 同 agent non-read-only barrier | turn-loop integration | 区间不重叠、顺序稳定 |
| V06 | parent + child 同 server | stdio delegation integration | 同 PID、区间重叠 |
| V07 | child + child 同 server | stdio delegation integration | 同 PID、区间重叠 |
| V08 | 单项 timeout 隔离 | client/manager integration | peer call 成功、后续 ping 成功、仍 ready |
| V09 | 参数/业务错误隔离 | manager integration | call 失败但 client/session 不变 |
| V10 | transport 断开 | client/manager integration | 当前 generation failed、工具移除 |
| V11 | reconnect generation fence | manager integration | 旧迟到错误不污染新 ready client |
| V12 | disable/reconnect 收束 | stdio + HTTP integration | 旧 in-flight 取消、旧资源释放、新连接唯一 |
| V13 | 用户/生命周期取消或 caller abort 的 tools/call | client/manager integration | 慢 HTTP headers/JSON body 立即中止，随后发对应 request cancellation，peer/follow-up 成功、仍 ready |
| V14 | lifecycle 取消慢 initialize | stdio integration | refresh 立即收束、stdio child PID 退出、不等 startup timeout |
| V15 | 并发 progress 路由 | integration + TUI state | token/caller/ToolCell 一一对应 |
| V16 | 真实 LLM 主 agent MCP 并发 | tmux TUI | pane capture + fixture JSONL |
| V17 | 真实 LLM 多子 agent MCP 并发 | tmux TUI | pane capture + fixture JSONL |
| V18 | 真实 LLM timeout 后连接可用 | tmux TUI | 首次 timeout、peer success、同 PID/session 的 ping 与第二次成功调用 |
| V19 | transport 关闭窗口超时 | HTTP manager integration | server quarantine、再次 reconnect 被拒绝、不建立 replacement |
| V20 | HTTP discovery 暂态连接失败 | HTTP manager integration | `tools/list` 失败后重新 initialize/discover，有限退避生效 |
| V21 | quarantine refresh 隔离 | HTTP manager integration | 同一轮 refresh 内被隔离 server 为 failed，健康 server 先变 ready 且可调用 |
| V22 | pending connect replacement fence | stdio manager integration | 旧慢 initialize 已退出后才启动 replacement PID |

## 8. 完成定义

只有同时满足以下条件，本需求才算完成：

- 同一 ACN 进程内每个 ready MCP server 只有一条共享常驻 client/session。
- 主 agent 与所有子 agent 复用该 client，跨 agent 不加隐式锁。
- agent 内现有 `readOnlyHint` 并发分类、barrier 和实时复核没有退化。
- 单项 timeout/cancel/参数或业务错误不关闭共享 client。
- 连接级错误和 reconnect 具备 generation fencing，不受旧请求迟到结果污染。
- stdio、Streamable HTTP、parent/child、progress、timeout、reconnect 均有自动化覆盖。
- 针对性真实 LLM TUI smoke 的所有硬通过条件成立。
- 三份关联 PRD 已同步，不再保留短连接实现约束。
- 完整 Rust 验证通过。
- code-review skill 通过且没有未处理高风险问题。

## 9. 追加拍板记录

当前没有阻塞实施的额外产品拍板项。以下仅属于实现阶段需要用测试确认的技术事实，不改变本文语义：

1. 当前 `rmcp` 版本对同一 client 多 in-flight request、request cancellation 和 HTTP session header 的具体行为。
2. 结构化连接错误在现有 SDK error chain 中可保留到什么粒度；如信息不足，优先在 ACN adapter层做明确映射，不按 error message 文本猜测。
3. 获取 client snapshot 的内部 helper/类型命名；应复用现有 manager 结构，不为此引入新的公共API。

如果上述技术事实证明既定语义无法实现，必须在此追加选项、影响与建议并请求拍板，不能退回短连接或新增跨 agent 锁作为隐式降级。

## 10. 实现与完成状态

实现落点：

- `src/mcp/connection_manager.rs`：ready client lease、generation/client identity fence、连接建立有限退避、`ConnectAttempt` completion fence、异步关闭旧 driver/transport，以及 parent/child 共享manager。
- `src/mcp/client.rs`：常驻 `RunningService`、request-scoped `tools/call` deadline/cancellation、turn cancellation 到 MCP cancellation notification 的映射、HTTP 慢 headers/JSON body 的有界中断、`McpConnectReleaseFence`/`PendingConnectTransport` 对取消中 connect 的实际 close 确认，以及结构化request/connection error 分类。
- `src/config.rs`：`MCP_RECONNECT_*` 与 `MCP_CONNECTION_SHUTDOWN_TIMEOUT_SECS` 都是仅运行时内部常量，未新增 TOML 字段。

自动化关键证据：

- stdio 单 PID/一次 initialize、HTTP 单 session、read-only 降级 fail-closed、progress route、timeout 后 peer/follow-up、401 后同 session `ping`、传输断开、generation fence 均由 manager integration tests 覆盖。
- `tool::tests::parent_and_two_children_share_one_stdio_client_without_cross_agent_lock` 使用 fixture记录的 request id、PID、单调时间 start/end 区间，证明共享单一 stdio client 下 parent/child 与child/child 都真正重叠。
- `stdio_reconnect_replaces_the_shared_child_and_releases_the_old_pid`、`reconnect_quarantines_unreleased_stdio_transport_and_settles_old_call` 与`reconnect_waits_for_cancelled_pending_stdio_connect_before_replacement`、`reconnect_waits_until_completed_outcome_is_installed_or_disposed` 覆盖旧 PID 释放、in-flight、尚未完成/已完成但未安装的 connect outcome 收束，以及旧 generation 不污染新 ready client。
- `runtime_reconnect_mark_releases_cancelled_http_transport_before_config_work` 证明 TUI 生命周期generation 变化时会同步摘除旧 HTTP client、取消旧慢 POST 并在确认 transport 已释放后再连接 replacement；`runtime_transition_reports_unreleased_http_transport_as_hard_error` 单独覆盖真正未释放时的 quarantine；`disable_cancels_in_flight_http_call_and_leaves_no_ready_session` 覆盖 HTTP disable 的旧调用收束与ready session 移除。
- `turn_cancellation_aborts_slow_http_headers_and_releases_shared_worker` 与`turn_cancellation_aborts_slow_http_json_body_and_releases_shared_worker` 覆盖用户取消 turn 时先中止对应的慢 HTTP POST（headers/body 两个窗口）、再发送 MCP cancellation，peer 在旧 30 秒 deadline 前成功；`lifecycle_disable_interrupts_slow_initialize_and_releases_stdio_child` 覆盖取消慢 initialize 后refresh 不等 startup timeout 且 stdio child 已退出；`slow_json_http_timeout_releases_shared_worker_for_peer_and_follow_up_call` 与`slow_http_response_headers_timeout_releases_shared_worker_for_peer_and_follow_up_call` 进一步断言HTTP JSON body 或 response headers 到期都会发送对应 `notifications/cancelled`，再释放同 session worker；rmcp 已入队 HTTP POST 保留其原始 deadline，不在 worker 释放时重新起算。
- `queued_http_tool_call_uses_original_deadline_instead_of_a_fresh_worker_deadline` 覆盖上述 HTTP queue 边界；`aborting_tool_call_future_cancels_its_request_and_keeps_shared_http_session_ready` 覆盖caller future 直接 abort 时立即中止慢 headers POST 并发出 request-scoped cancellation；`long_sse_tool_call_keeps_full_deadline_and_routes_progress` 与`sse_headers_arriving_near_deadline_are_not_cut_off_by_json_body_protection` 断言 delayed SSE headers/progress 不受 JSON body 保护窗口截断；`stdio_reconnect_replaces_the_shared_child_and_releases_the_old_pid` 以 UI staged lifecycle 路径断言旧 PID 已退出后才建立 replacement client。
- `cancelling_hung_read_only_live_list_keeps_shared_http_session_ready` 覆盖只读实时 `tools/list`在未返回 headers 时的 turn cancellation：取消立即释放 HTTP worker、本地 deadline 元数据不泄露到server、随后发送对应 `notifications/cancelled`，同一 session 的 peer/follow-up 继续成功；这一路径保持server ready，不把单项 list 取消误判为连接失败。
- `read_only_live_list_and_tool_call_share_one_admission_deadline` 覆盖实时 `tools/list` 已消耗 700ms 后，随后的慢 `tools/call` 只能使用同一秒级绝对 deadline 的剩余窗口，不会重新获得完整 timeout；超时仍会释放 worker 供 peer 调用，并保持 server ready。
- `runtime_transition_reports_unreleased_http_transport_as_hard_error` 覆盖 HTTP 旧 transport 在关闭窗口内不退出时的 quarantine：同一 server 的后续 reconnect 被拒绝且不会建立 replacement；`refresh_with_unreleased_transport_keeps_other_server_available` 同时断言同一轮 refresh 中，该隔离不会阻塞其他 server 先完成 refresh 并接受 tool call；`http_discovery_connect_failure_retries_connection_establishment` 覆盖 initialize 成功后 tools/list遭遇连接失败，仍按 `config.rs` 内部退避重新 initialize/discover。
- `tool::mcp_progress_tests::mcp_progress_reporter_keeps_summary_renderable_with_turn_context` 防止把`turn_id` 混入 MCP ToolCell 的标准 progress 摘要，保证真实 TUI 能显示每个调用各自的进度。
- `session_tui::app::tests::tui_disable_persists_config_when_transport_release_times_out` 覆盖 TUI Disable 的 transport quarantine 路径：面板仍显示关闭超时，但 `.mcp.json` 必须持久化`enabled=false`，重启后不会重新启用。
- 完整命令：`source export_env.sh && cargo fmt --check && cargo clippy -- -D warnings && cargo test && cargo check`。

真实 LLM TUI runner 与协议真实 fixture 源码受版本控制，位于`.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_shared_mcp_real_llm.sh` 与`shared_mcp_real_llm_fixture.sh`。分别以 `SHARED_MCP_SCENARIO=reads|children|timeout|reconnect`执行该 runner，覆盖：

- `reads`：同一 response 中两个 `slow_read` 调用、独立 progress 和 ToolCell 收束。
- `children`：两个真实 session child 共享同一 stdio client，并产生重叠的 `slow_write`。
- `timeout`：单项 timeout/cancellation 不影响同连接 peer 和后续调用。
- `reconnect`：旧连接释放后建立唯一 replacement，并恢复调用。

上述四个场景、基础 TUI smoke、自动化验证和 code-review skill 均已通过，没有未处理的高风险问题。
