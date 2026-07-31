# PRD: wait_subagents 子代理等待

> 状态：已实现。本文保留等待工具、通知和主 Agent 使用纪律。

本文定义主 agent 等待当前 session 内 subagent 的产品语义、工具协议和运行时边界。

它是 `docs/PRDs/PRD_subagents.md` 的配套需求，只解决“主 agent 如何在不轮询、不污染上下文的前提下等待子代理”的问题。本文中的“终态”表示状态机已经结束、不会继续推进的状态，不是命令行终端。

---

## 背景与目标

subagent 可以跨 user turn 在后台推进。主 agent 目前可以用 `list_subagents` 和`read_subagent` 主动读取状态，但如果结果是当前工作的硬依赖，反复轮询既浪费工具调用，也会把不必要的进度和工具结果带入主上下文。

本需求增加 `wait_subagents`。它让主 agent 在确有必要等待时，以一次有界工具调用等待当前 session 中的一组 subagent 进入结束状态；等待期间不轮询磁盘，也不会因普通进度变化反复把控制权交还模型。

目标是：

- 让主 agent 能等到需要的 subagent 结果，而非猜测完成时机；
- 保持主 agent 对子代理内部过程的克制感知；
- 不改变现有“用户只与主 agent 对话”的星型协作结构；
- 不要求当前 ACN 实现通用工具并发。

---

## 范围与非目标

本需求包含：

- `wait_subagents` 工具及其主 agent prompt 指引；
- session runtime 的内存活动通知与取消等待；
- 正常工具历史中的 `Called wait_subagents` / tool result 展示；
- `code_run` description 动态展示当前有效的最长执行时间。

本需求不包含：

- 通用工具并发调度；
- 面向用户的子代理消息、取消或聊天入口；
- 子代理之间通信或二级子代理；
- 通用受管后台 shell job；
- 通过 `nohup`、`tmux`、`setsid` 等命令绕过 `code_run` 生命周期。

最后一项必须明确：当前 `code_run` 会清理它启动进程组中残留的后代进程，不能被产品 prompt描述成可靠的后台任务启动器。真正的后台命令能力需要独立的 job id、PID/进程组、日志、状态、停止和 session 退出回收设计，后续另立 PRD。

---

## 主 agent 使用纪律

### 适合派发的工作

主 agent 只应派发能够与自己的有效工作并行推进、职责和访问边界明确的实质任务。

如果下一步关键决策立刻依赖某项工作结果，而主 agent 没有其他独立且有价值的工作可推进，也没有用户明确要求后台处理，不应把该工作作为阻塞性交接派给 subagent；应由主 agent直接完成。

若多项工作可以真正并行，主 agent 应在创建时划清目标、文件范围、产物和完成标准。多个subagent 默认不得并发写同一文件或重叠范围，具体规则仍以 `PRD_subagents.md` 为准。

### 何时等待

主 agent 不能在刚创建 subagent 后，仅报告“已排队”便机械结束当前工作。

准备结束当前 turn 时，如果仍有 queued / running subagent，且当前用户请求需要其结果才能得到完整、可靠的答复，或主 agent 尚无独立可交付的有效结果，应先调用`wait_subagents`。

下列情况不应为了等待而延迟用户：用户明确要求后台推进或稍后再看；当前用户提出的新问题可以独立、完整地回答；或者主 agent 已经有足以回答当前请求的经过核实的结果。

等待超时是正常控制流，不得在同一 turn 中无条件循环调用 `wait_subagents` 直到全部完成。只有当前请求明确要求等齐全部依赖结果时，才应使用 `until = "all_terminal"` 并视情况继续等待。

### 同一 assistant 回合的约束

当前 ACN 工具调用串行执行。主 agent 在同一个 assistant 回合中不得发出多条`wait_subagents`；需要等待多项指定任务时，必须在一次调用的 `subagent_ids` 数组中一起传入。

这是一项 prompt 使用纪律，不以当前需求实现通用工具并发。未来即使支持工具并发，也应由当时的调度协议重新决定多条 wait 的并行语义。

---

## 工具协议

工具名固定为 `wait_subagents`。

### 入参

```json
{
  "subagent_ids": ["subagent_a1b2c3d4", "subagent_e5f60718"],
  "until": "any_terminal",
  "timeout_secs": 30
}
```

`subagent_ids` 是可选数组。

- 传入时，所有 ID 必须属于当前 session；未知、属于其他 session 或重复的 ID 应返回参数错误。
- 不传时，在**工具调用开始时**快照当前 session 中全部 `queued` / `running` subagent 作为本次固定等待集合。调用开始后新建的 subagent 不加入本次等待。
- 显式传入已经终态的 ID 是合法的；它在首次状态检查时就计入结束对象。
- 不传且当时没有 queued / running subagent 时，工具立即返回 `no_active_subagents`，不把空集合误报为条件满足。

`until` 可选，取值为：

- `any_terminal`：等待集合中任一 subagent 进入终态；这是默认值。
- `all_terminal`：等待集合中所有 subagent 都进入终态。

终态包含完成、失败和 abandoned 等已经结束、不会继续执行的状态。`all_terminal` 只表示“全部结束”，不表示“全部成功”。

`timeout_secs` 可选。它由 `agent.session.subagents.wait` 配置约束：默认 30 秒、最小 10 秒、最大 3600 秒。工具参数不能越过当前进程已加载配置的边界。

### 出参

返回必须保持有界，不直接嵌入 `result.md`、完整 transcript、完整事件或工具输出。

```json
{
  "outcome": "condition_met",
  "until": "any_terminal",
  "waited_subagent_ids": ["subagent_a1b2c3d4", "subagent_e5f60718"],
  "terminal_subagents": [
    {
      "id": "subagent_a1b2c3d4",
      "status": "completed",
      "updated_at": "2026-07-14T08:00:00Z",
      "completed_at": "2026-07-14T08:00:00Z"
    }
  ],
  "pending_subagent_ids": ["subagent_e5f60718"]
}
```

`outcome` 取值：

- `condition_met`：已满足 `until`；
- `timeout`：到达 timeout，但等待集合尚未满足条件；
- `no_active_subagents`：省略 `subagent_ids` 且调用开始时没有可等待对象。

主 agent 需要阅读详细结果时，仍须显式调用 `read_subagent`，默认先读 `summary`，仅在确有采纳或诊断需要时读取 `result` 或有限 transcript。

---

## 运行时通知与等待

`wait_subagents` 不轮询文件。每个 session runtime 持有一个只在内存中存在的活动通知通道，其职责只是唤醒等待器，不保存事实状态。

事实来源仍是每个 subagent 已持久化的 `delegation.yaml`、`progress.json` 与事件日志。所有状态变更都遵循“先成功落盘，再通知”的顺序：

```text
子代理调用 update_subagent_progress
  -> 写入进度与事件
  -> runtime 发布进度活动通知

runner 写入 queued -> running、completed / failed / abandoned
  -> 写入元数据与事件
  -> runtime 发布状态活动通知
```

通知不是子代理 LLM 的自主行为，也不是新增工具调用。`update_subagent_progress` 是产生进度通知的一个来源；runner 的启动、结束、失败和收束同样会产生通知。

等待器的流程为：先读取一次持久化状态并判断；未满足时同时等待活动通知、timeout deadline和当前 turn 的取消信号。收到普通进度通知后重新读取持久化状态；若未满足 `until` 则继续等。只有等待对象满足结束条件时才返回工具结果。

内存通知允许合并多次连续更新；因为每次被唤醒后都会重新读取持久化状态，不会因此丢失正确性。

用户以 Ctrl+Enter 注入新输入时，现有 turn 应取消，正在执行的 `wait_subagents` 随之结束，旧 turn 不再继续消费普通 tool result。普通 Enter 的排队输入不打断当前等待，也不作为活动通知的返回条件。

协作式取消的 `wait_subagents` 必须发出 `ToolCallInterrupted` 运行时与 turn journal 事件，用于闭合 TUI 中已经开始的工具项并记录中断事实；该事件不是 canonical `tool_result`，旧 turn中的 `tool_use` 与中断事件都不提交到正式 LLM transcript。普通工具若不支持协作式取消，Ctrl+Enter 到达后仍须执行到安全边界，先发出并持久化正常 `ToolCallCompleted`，再结束旧 turn，不得因为 cancellation 已挂起而丢弃已经真实发生的工具结果。

`wait_subagents` 使用普通工具展示和 canonical transcript 规则；本需求不新增专用 TUI 折叠、静默等待条或替代渲染。

---

## code_run 初始观察窗口说明

`code_run` 的 tool description 应展示当前进程已加载的默认 `yield_time_ms` 以及允许范围，例如：

> 未传 `yield_time_ms` 时默认观察 10 秒；可传范围为 250 至 30000 毫秒。窗口结束后仍在运行的命令会返回受管 `process_id`，不设置进程运行时 timeout。

`yield_time_ms` 只决定本次 tool call 返回前的初始观察窗口；`code_run` 的初始值、最小值和最大值分别是`config.rs` 内部固定的 `10000`ms、`250`ms 和 `30000`ms，不开放部署 TOML 覆盖，也不支持热更新。进程会持续由 ProcessManager 管理，直到自然退出或 owner/session 清理。

---

## 实现清单

1. 在配置中加入 `agent.session.subagents.wait.default_timeout_secs`、`min_timeout_secs`、`max_timeout_secs`，完成默认值和区间校验。
2. 新增 session-scoped subagent activity hub，提供进度和状态变更后的内存通知。
3. 在 subagent progress 写入、runner 启动、终态写入与 abandon 收束成功落盘后发布通知。
4. 注册 `wait_subagents` 的 schema、参数校验、快照选择、等待与有界结果序列化。
5. 将当前 turn 的取消 token 接入等待器，保持 Ctrl+Enter 与普通 Enter 的既有语义。
6. 更新主 agent system prompt：派发纪律、何时等待、单回合只调用一次 wait 的规则。
7. 更新 `code_run` description，格式化当前有效 `yield_time_ms` 默认值与范围，并说明不设置进程运行时 timeout。
8. 为工具定义、等待语义、活动通知、取消与 prompt 渲染补充单元/集成测试；对 TUI 做一次工具历史与 Ctrl+Enter 中断的定向 smoke test。

---

## 验收策略

至少覆盖下列场景：

- 省略 `subagent_ids` 时，只等待调用开始时的 queued / running 快照；之后创建的 subagent不影响本次等待。
- `any_terminal` 在指定集合任一对象结束后返回，并准确列出 pending ID。
- `all_terminal` 在所有指定对象结束后才返回；其中失败或 abandoned 仍是结束状态。
- 指定已终态对象立即满足相应条件；未知、跨 session、重复 ID 明确报参数错误。
- 无活动对象时返回 `no_active_subagents`。
- 普通 `update_subagent_progress` 能唤醒内部检查，但不会让等待工具提前返回。
- 终态写入能立即唤醒等待并返回；timeout 返回有界状态而不读取完整结果。
- Ctrl+Enter 取消正在等待的 turn；普通 Enter 继续按现有语义排队。
- TUI 中 `wait_subagents` 以普通工具调用进入历史，不出现多余空行或专用错误状态。
- `code_run` definition 的自然语言 description 反映内部固定的 `yield_time_ms` 默认值和范围，schema minimum / maximum 分别为 `250` / `30000`，且不再暴露 `timeout`。
