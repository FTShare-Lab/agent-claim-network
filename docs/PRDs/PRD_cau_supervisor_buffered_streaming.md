# Inbox CAU 与 Supervisor Recap 缓冲流式化

> 状态：已完成（2026-09-03）

## 背景

ACN 的主对话、Inbox PolicyUpdate、Session/Subagent compact summary、前台 recap/finalize、Memory review、Router rerank 与 Maintainer 仲裁已经采用 streaming-first。结构化内部任务使用 Buffered streaming：网络层通过 WS/SSE 增量接收，partial 只在进程内缓冲，完整终态返回后才解析、校验和提交。

当前仍有两条活跃路径默认使用 non-streaming：

- Inbox ClaimAttributeUpdate（下称 CAU）的结构化内化。
- Supervisor Recap job，以及 Supervisor Finalize job 内部尚未完成的 recap。

本需求把这两条路径补齐为 Buffered streaming，同时保留既有知识提交、checkpoint、Effect Journal、队列、抢占和用户展示边界。

## 目标

- CAU 的 transport、retry、fallback、WebSocket sticky 与 timeout 语义对齐 Inbox PolicyUpdate。
- Supervisor recap 以一次逻辑生成作为一个 job attempt，优先使用流式 transport，并允许有限 transport fallback。
- 流式 partial 永不进入 JSON/业务校验、知识提交、checkpoint、Effect Journal、通知或 TUI transcript。
- 完整但非法的模型输出不得被误判为 transport failure，也不得触发 WS → SSE → non-streaming。
- 不新增用户配置，不改变 Supervisor 外层最多五次 job attempt 的语义。

## 非目标

- 不在 TUI 中展示 CAU 或 Supervisor recap 的 token delta。
- 不修改 CAU 的输入、合批、权限、provenance、Claim/Dispute 校验与 Effect Journal 数据形状。
- 不修改 Recap/Finalize 的输入范围、共享 `finalize_checkpoint.yaml`、Prepared/Applied 提交顺序、通知和 session 生命周期。
- 不让 Supervisor 使用 `[agent.llm].retry_count`，也不恢复 Supervisor 的 max-token continuation。
- 不为 transport 选择新增持久化状态、job 字段、checkpoint 或恢复目录。
- 不修改主对话、普通 Inbox PolicyUpdate、compact、前台 fallback finalize 等已经流式化路径的业务语义。

## 已拍板语义

### D1：CAU 直接对齐 Inbox PolicyUpdate

- CAU 使用现有 `BufferedProviderRuntime` 和公共 Buffered streaming/fallback 基础设施，不另造 CAU 专用 transport 状态机。
- 初始请求优先使用配置允许的 WS/SSE；partial 仅缓冲，完整终态后才进入结构化解析和 CAU validator。
- 流式失败按既有 provider transport retry 处理；耗尽后允许 WS → SSE → non-streaming 或 SSE → non-streaming。
- CAU 接受与 PolicyUpdate 相同的分层 retry：transport retry/fallback 与结构化/业务输出 retry 分开计算，不再保留“所有 provider、解析和业务失败严格共享最多 `retry_count + 1` 次 provider-level attempt”的特殊限制。
- 完整响应的 JSON 解析、shape、holder 权限、Policy provenance、Claim 编辑权限、Dispute 引用等校验失败，只进入结构化/业务 retry；不得因此切换 transport。
- 请求正常完成但没有可消费输出时，沿用既有 Inbox 语义：丢弃未提交 chain，使用相同实际 transport 原样重试，不追加 transport fallback。
- CAU 继续允许既有 max-token continuation，并沿用 provider adapter 的 timeout 处理。
- CAU 使用当前 session Inbox fallback root；既有 WS sticky 的传播与 resume 后重置语义不变。
- CAU 仍在完整业务校验通过后写 Prepared Effect Journal，应用成功后才 ACK inbox；崩溃恢复不得因流式化重复应用知识。

### D2：Supervisor job attempt 表示一次逻辑 recap 生成

- Supervisor Recap job 与 Supervisor Finalize job 内部的 recap 都改为 Buffered streaming。
- 一次 Supervisor job attempt 表示一次逻辑 recap 生成，不再严格等同于一次 wire 模型请求。
- 一个 job attempt 内，每种 transport 最多发起一次：Responses 可依次尝试 WS、SSE、non-streaming；其他流式 provider 可依次尝试 streaming、non-streaming。
- 任一 transport 返回完整结果后立即停止 transport fallback，进入 JSON、shape、引用和业务校验。
- 全部允许的 transport 均失败，或者完整结果未通过校验时，当前 job attempt 失败；由 Supervisor 外层重新排队。
- Supervisor 仍最多执行五个持久化 job attempt。该上限、attempt 计数、退避、`acn supervisor retry` 和 Failed 状态语义不变。
- 不叠加 `[agent.llm].retry_count`：`retry_count_override` 保持为 0，同一种 transport 不在一个 job attempt 内重复。
- max-token continuation 保持关闭；MaxTokens 不能在同一 job attempt 内自动产生额外 continuation 请求。
- 理论 wire 请求上限为：SSE 类 provider 每个 job 最多 10 次；启用 Responses WS 时每个 job 最多 15 次。该数字是五个 job attempt 全部走完整 transport fallback 链时的上限，不是正常成功路径。

本决策只替代 `PRD_recap_in_supervisor.md` D6 中“单个 job attempt 只允许一次真实模型请求”的表述；以下旧语义继续有效：外层最多五次、不叠加 `[agent.llm].retry_count`、checkpoint 恢复优先、前台 fallback finalize 使用配置 retry、compact summary 不受影响。

### D3：只有 transport 分类允许降级

- Provider-neutral streaming → non-streaming 只接受既有 `ProviderStreamFailure` 分类。
- 连接失败、握手/协议错误、流损坏、完整终态前断开、请求/流超时等可恢复 transport failure 可以触发降级。
- 认证失败、非法请求、请求过大、确定性拒绝、上下文耗尽、取消、完整但无可消费输出，以及 JSON/shape/引用/业务校验失败均不得触发 transport 降级。
- Buffered partial 在任何失败、抢占或 fallback 前全部丢弃，不得进入下一 transport 的输入，也不得形成 checkpoint 或 Effect Journal。

### D4：Supervisor 抢占与持久化边界不变

- Supervisor recap/finalize 继续在等待 session/knowledge lock、等待 provider future 和 Prepared 原子写之前响应既有抢占控制。
- Prepared 前抢占获胜时，取消整个逻辑生成并丢弃所有 buffered partial；不得继续启动尚未开始的 transport fallback。
- Prepared 原子写一旦获胜，继续按现有 checkpoint 恢复、应用和上传语义完成，不允许 Resume 抹掉 checkpoint。
- 流式化不改变 Finalize > Recap 全局优先级、同级 FIFO、重叠 Recap 不合并、Finalize subsume 或 Resume 原地转换语义。

### D5：展示、日志与通知不变

- CAU 仍只展示现有 Inbox activity 与最终 Inbox 结果，不新增 token、attempt 或 transport 提示。
- Supervisor recap 不向 TUI 输出 token，不发送 recap 系统通知。
- Finalize 是否通知继续取决于既有“确实处理了尚未 recap 内容”的规则；transport fallback 不改变通知条件。
- 日志和测试可以记录实际 transport 与 fallback，用于诊断；不得把模型 partial 或敏感请求正文写入日志。

### D6：最小实现边界

- 复用 `StructuredJsonCaller`、`BufferedProviderRuntime`、`ProviderStreamFailure` 与 `send_buffered_with_fallback` 的现有职责。
- 为 Supervisor 增加的调用形状只表达“Buffered streaming、结构化/业务单次、provider retry 0、continuation false”，不创建第二套 provider loop。
- CAU 的 `PromptInboxJsonGenerator` 与 `SessionInboxJsonGenerator` 两个生产实现必须同步；可以抽取已有小 helper，但不为消除少量重复做额外架构重构。
- Supervisor 各 job attempt 继续使用进程内 runtime scope；不把 WS/SSE/non-streaming 选择写入 job 或 session 持久状态。下一 job attempt 重新按既有 runtime 能力选择 transport。

## 实施中追加拍板

### P1：429/5xx 继续属于流式恢复分类

现有 OpenAI Chat、OpenAI Responses 与 Anthropic adapter 都把 retryable HTTP/network failure，以及 HTTP 429/5xx，归入流式恢复分类。Responses WS 的 429/5xx 在 retry 耗尽后可以用 HTTP/SSE 恢复当前请求，但不会写入 WS sticky。它们不是 JSON 或业务输出错误。

- A：保持现有公共 provider 分类。Supervisor 与 CAU 和其他 streaming-first 路径一致，429/5xx 可以触发有限 transport recovery/fallback。
- B：本需求单独把 429/5xx 排除在 Supervisor/CAU transport fallback 之外，直接结束当前业务/job attempt。

选择 A（2026-09-03）。原因：它复用已经验证的 provider 分类，不给 Supervisor/CAU 制造特例；同时仍严格保证任何完整模型输出的格式或业务错误都不会触发 transport 降级。

## 修改后的完整流程

### CAU

```text
连续 CAU 合批
→ 构造既有 CAU 输入与 validator
→ Buffered streaming（WS/SSE）
→ transport retry/fallback（与 PolicyUpdate 相同）
→ 完整响应
→ JSON/shape/权限/provenance/引用校验
→ 失败则走既有结构化/业务 retry，不因格式错误切 transport
→ 成功后写 Prepared Effect Journal
→ 应用本地知识与 durable upload
→ 标记 Applied
→ ACK inbox
```

### Supervisor Recap/Finalize 内部 recap

```text
Supervisor 选择 job，attempt + 1
→ 读取剩余 recap 范围并恢复可用 checkpoint
→ 无可恢复 checkpoint 时开始一次逻辑 recap 生成
→ Buffered WS（若配置且可用）
   → transport failure 才尝试 SSE
   → SSE transport failure 才尝试 non-streaming
→ 完整响应后执行 recap JSON/业务校验
→ 成功：提交 Prepared checkpoint 并继续应用
→ 失败：当前 job attempt 失败并由外层重新排队
→ 最多五个 job attempt
```

## 分阶段实施计划

执行期间每次切换阶段前必须完整重读本 PRD，核对已拍板语义、待拍板结论和非目标；实施中如出现新的业务选择，只能追加“问题、选项、选择、原因”，不得改写既有拍板的语义。

### 阶段一：锁定公共 Buffered transport 边界

- 为公共 Buffered helper/StructuredJsonCaller 补足请求策略测试：Buffered、runtime chain/scope、provider retry override、continuation 与 fallback 分类。
- 明确并测试完整非法 JSON、业务 validator 失败、无可消费输出和 terminal failure 均不被当作 `ProviderStreamFailure`。
- 若现有 API 不能同时表达 CAU 与 Supervisor 的策略，只做最小参数化或增加一个命名明确的构造/入口。

验收：

- 流式 partial 失败后被丢弃，fallback 只接收原始完整输入。
- 非 transport 错误不启动 non-streaming replacement。
- Supervisor 策略能够做到每种 transport 最多一次、continuation false、adapter retry 0。

### 阶段二：CAU 对齐 PolicyUpdate

- 将两个生产 Inbox generator 的 CAU 路径接入 Buffered streaming，并使用对应 Inbox fallback scope。
- 保留 CAU validator、纠错/业务 retry、合批、Effect Journal、ACK 与恢复流程。
- 删除或收敛仅服务于 CAU non-streaming 的特殊调用形状，但不影响仍有合法用途的标准 non-streaming API。
- 更新原有 CAU retry-budget 测试，使其验证与 PolicyUpdate 一致的分层 retry/fallback，而不是旧的统一 provider-level attempt 上限。

验收：

- 首次请求为 `stream=true`、`stream_output_mode=Buffered`，runtime chain/scope 存在。
- transport failure 按公共路径降级；完整非法 JSON/业务校验失败重新执行结构化业务 attempt，但不因该错误降级 transport。
- CAU 完整响应前不写 Effect Journal、不修改 Claim/Dispute、不 ACK。
- Prepared/Applied journal 恢复不重复调用模型或重复应用知识。

### 阶段三：Supervisor Recap/Finalize 内部 recap 流式化

- 增加或复用“Buffered streaming validated once”调用入口，固定 provider retry 0、continuation false、结构化业务单次。
- 让 Supervisor 的 `RecapRetryMode::SingleAttempt` 使用该入口；前台 `Configured` 路径保持原样。
- 保留 job attempt 计数、外层五次、checkpoint 恢复与 Prepared 前抢占。
- 补充 Recap job 与 Finalize job 内 recap 的同等覆盖。

验收：

- 单个 job attempt 中，同一 transport 不重复；允许有限 WS → SSE → non-streaming。
- 完整非法 JSON/业务校验失败时不启动 transport fallback，直接使当前 job attempt 失败。
- 五次外层 attempt 耗尽后仍进入既有 Failed/finalizing 状态；`acn supervisor retry` 行为不变。
- 可恢复 Prepared/Applied checkpoint 时不调用模型。
- provider future 尚未 Prepared 时被 Resume/Finalize 抢占，不写 checkpoint、不提交 buffered partial，也不继续启动新 fallback。

### 阶段四：整体回归、真实 LLM 验收与复审

- 更新 `PRD_internal_llm_streaming.md` 中已经漂移的当前行为说明；除非出现用户可见稳定行为变化，不向 README、顶层 architecture 或 user guide 堆叠 transport 实现细节。
- 按仓库 `verify` skill 完整执行格式化、Clippy、测试、类型检查与必要的 canonical tmux smoke。
- 使用真实 LLM 分别执行一次 CAU Inbox 内化、Supervisor Recap 和 Supervisor Finalize，确认流式 transport 成功、完整后提交、TUI/通知无新增噪音。
- 使用可控 fake provider 覆盖真实服务难以稳定制造的 partial 后断流、WS/SSE fallback、非法 JSON、五次外层失败和 Prepared 前抢占。
- 使用 `code-review` skill 做针对性本地与独立外部复审；修复非过度防御的 P0/P1 后重跑受影响测试和完整 verify。最后一次代码修改后必须再次复审，确认没有待修复的非过度防御 P0/P1。
- 最终逐条对照本 PRD 的目标、非目标、已拍板语义和验收项，确认整体实现对齐后才能标记完成。

## 总体验收标准

- CAU、Supervisor Recap、Supervisor Finalize 内 recap 默认均以 Buffered streaming 开始。
- 只有既有 transport failure 分类能触发 transport fallback；格式和业务校验失败不能切 transport。
- CAU 与 PolicyUpdate 的 transport/retry/fallback/sticky/timeout 语义一致。
- Supervisor 每个 job attempt 是一次逻辑生成，每种 transport 最多一次，最多五个 job attempt，不使用 `[agent.llm].retry_count`，不启用 continuation。
- 所有 partial 在完整终态前不可见、不可校验、不可提交；失败、fallback 或抢占时完整丢弃。
- CAU Effect Journal、Inbox ACK、Recap checkpoint、知识提交、session 生命周期、队列优先级、Resume 抢占、TUI 与通知语义均保持既有边界。
- 定向测试、完整 verify、真实 LLM smoke 与最终外部 code review 全部通过。

## 实施与验收结果

完成日期：2026-09-03。

### 实现对齐

- `StructuredJsonCaller` 增加 Supervisor 专用的 Buffered streaming validated-once 入口：结构化业务 attempt 为一次，adapter retry 固定为 0，continuation 关闭；transport failure 继续复用公共 Buffered fallback。
- `PromptInboxJsonGenerator` 与 `SessionInboxJsonGenerator` 的 CAU 普通生成和带业务 validator 生成均改用当前 Inbox fallback scope 的 Buffered streaming；删除 CAU 专用 non-streaming/request-timeout 包装，timeout 回归 provider adapter 所有。
- Supervisor `RecapRetryMode::SingleAttempt` 同时覆盖普通 Recap job 与 Finalize job 内部 recap；前台 `Configured` recap/finalize、compact summary 和其他结构化任务未改。
- CAU Effect Journal/ACK、共享 `finalize_checkpoint.yaml`、Prepared/Applied、Supervisor 队列/五次 attempt、Resume/Finalize 抢占、通知与 TUI 展示代码均未改变。
- `PRD_recap_in_supervisor.md` 已追加 D6 的后续替换说明；`PRD_internal_llm_streaming.md` 已补充 CAU 与 Supervisor 的当前分层 retry 语义。未修改 README、顶层 architecture 或 user guide。

### 定向与完整验证

- 公共结构化调用测试确认：stream failure 仅产生一次 non-streaming replacement；非法 JSON 不触发 transport fallback；fallback 使用原始消息并丢弃旧 runtime chain。
- CAU 测试确认：完整业务校验失败仍在 Buffered streaming 上按结构化预算重试；transport failure 使用公共 fallback；请求为 Buffered、允许 continuation、adapter retry 使用配置；既有 Effect Journal、恢复与权限测试保持通过。
- Supervisor 测试确认：Recap 与 Finalize 内 recap 都从 Buffered streaming 开始，fallback request 固定 provider retry 0、continuation false；非法 JSON 直接结束当前 logical attempt；Prepared 前 Finalize/Resume 抢占测试保持通过。
- `scripts/check_version_consistency.sh`、`cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check` 全部通过。完整测试结果：lib 2680、`acn` 57、maintainer 2、router 2、cleanup CLI 1、session storage 5，doc tests 0 个失败。
- 本次未修改 TUI 渲染、输入或状态机，canonical tmux smoke 不属于必要项；仍额外执行了覆盖真实 TUI 的真实 LLM smoke。

### 真实 LLM 验收

- 隔离团队模式运行保存在 `target/tui-real-smoke/cau-supervisor-streaming/`。真实 TUI 完成七个 turn、两次 compact、resume 和两次退出；两条 Recap job 与两条 Finalize job 均在 attempt 1 成功，最终 `session_eab7a3dd` 为 Closed，`recapped_until=message_count=34`，agent/resume stderr 均为空。
- 在同一隔离 Maintainer 中额外投递 ClaimAttributeUpdate `policy_c0f0907b`；真实启动 Inbox 拉取并处理唯一消息 `inbox_2a8d7e36`，形成 Applied Effect Journal、`.done` 本地收件记录和 Maintainer delivered 记录，未产生非预期 Claim/Dispute。
- Recap enqueue/成功未增加 TUI 通知或 token 文案；Finalize 与 session 生命周期保持既有展示。

### Code Review

- 按 `code-review` skill 完成本地 diff/周边调用链审计，并在完整验证与真实 LLM 后运行一次独立只读外部 review。
- 本地与外部 reviewer 均未发现具有现实触发条件、需要修复的 P0/P1；外部 reviewer 明确核对了 Inbox、Buffered provider、StructuredJson、Supervisor Recap/Finalize、checkpoint 与抢占链路，且未修改工作区。
- 外部 review 发生在最后一次代码修改及完整验证之后；其后仅补记本验收结果，没有需要修复或再次复审的代码变化。
