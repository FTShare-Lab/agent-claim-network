# PRD: Session Delegation 子代理

> 状态：已实现。本文保留 session subagent 的生命周期、工具、上下文与 TUI 决策。

本文记录 ACN 子代理能力截至当前已经拍板的产品语义与实现边界。

这里的“子代理”是公开产品说法；主 agent 工具、工具返回协议、ID、配置、环境变量、模型提示和 TUI 等公开可见界面统一使用 `subagent`。实现层继续称为`SessionDelegation`：它不是 Agent Claim Network 中的独立 agent 身份，而是主 agent在一个交互式 session 内发起的内部委托任务。

---

## 背景

ACN 当前已经有成熟的 turn/tool loop、TUI 事件投影、turn journal、session finalize supervisor、后台 memory review 与 session cleanup。

现有普通 tool call 适合一个 turn 内的即时工具调用；子代理需求的核心价值不在于“再包装一个 tool call”，而在于让主 agent 能把较长、可并行、可跨 turn 观察的工作委托出去，同时保持 ACN 的责任边界清晰。

本需求暂不实现定时任务。定时任务与子代理在后台执行、上下文继承、生命周期管理上有重合，因此设计时需要为未来兼容留空间，但本 PRD 只拍板子代理语义。

---

## 核心定位

子代理不是一个新的 ACN agent。

ACN 的 claim、trace、dispute、inbox、finalize 责任仍然只属于主 agent。子代理只为主 agent 提供内部分析、搜索、验证、草稿、局部执行结果或阶段性观察。主 agent 决定是否采纳这些结果，以及如何把它们写入最终回复、trace 或 claim。

子代理的输出是主 agent 的素材，不是网络中的知识事实。

---

## 已拍板决策

### 1. 公开命名与实现层命名

公开产品与协议统一使用 `subagent`。用户、主模型和子模型不需要同时学习 `subagent` 与`delegation` 两套词汇；公开工具名、JSON 包裹字段、ID 前缀、配置键、身份环境变量、模型提示和 TUI 文案都使用 `subagent`。

实现层使用 `SessionDelegation` 表示一次 session 内部委托。

推荐命名：

- `DelegationId`
- `SessionDelegation`
- `DelegationStatus`
- `DelegationEvent`
- `DelegationResult`
- `DelegationRunner`

不建议把实现实体命名为 `SubAgent` / `Subagent`。当前仓库里 `Agent` 已经是很重的领域身份，绑定 claim store、inbox、memory、router、maintainer client 与 trace/finalize职责。使用 `SubAgent` 容易误导后续实现复用完整 agent 能力。

不建议把领域实体直接命名为 `Worker`。仓库中 `worker` 主要表示 TUI / router / runtime里的执行循环或后台任务，不足以表达主 agent 对内部认知任务的委托关系。

### 2. 生命周期

`SessionDelegation` 属于一个 parent `SessionId`。

它可以跨多个 user turn 存活，因此不是一个 turn 内的即时 tool call。主 agent 在后续turn 中可以看到未完成 delegation 的状态、阶段性输出和最终结果。

它不跨 parent session 存活。parent session 进入 `Finalizing` 后开始拒绝新的 delegation创建，并强收束已有 queued / running delegation；只有这些未完成 delegation 已进入终态后，parent session 才能进入 `Closed`。已完成的结果可以作为该 session 内的历史材料保留，但不再作为活体继续运行。

### 3. 用户交互边界

用户始终只与主 agent 对话。

用户不能直接给子代理发消息，不能 resume 子代理，不能把子代理当作独立会话打开。主agent 可以在自己的推理与工具调用中决定如何向 delegation 传递信息；这个内部传递过程不暴露为用户可寻址接口。

主 agent 可以向 queued / running delegation 追加结构化指令，但不把该能力建模为`send_message_to_subagent`。推荐使用 `steer_subagent` 或 `update_subagent_task`一类语义：它表示主 agent 对内部委托任务的目标、约束、上下文摘要或结果引用进行 steering，而不是向一个独立聊天对象发消息。

completed / failed / abandoned 的 delegation 不再接收追加指令；如果需要新的工作，应创建新的 delegation。

TUI 可以展示 delegation 的运行状态、简短标题、进度或结果摘要，但不提供面向用户的子代理聊天入口。

### 4. 主体责任

子代理不拥有 inbox。

子代理不执行 session finalize。

子代理不直接写 claim、dispute、trace 或 maintainer upload。需要进入 ACN 知识网络的内容，必须由主 agent 在自己的 session/finalize 路径中采纳并落盘。

子代理不触发 memory review、compact 或 inbox sync 的主流程副作用，除非后续单独拍板某类内部工具可以安全参与。

### 5. 禁止嵌套

子代理不能再创建二级子代理。

实现上应从子代理可用工具集中移除任何 `delegate_*` / `spawn_*` 类能力，并在服务端执行路径上再次校验，避免仅靠 prompt 约束。

按 P1 / X4 拍板，子代理会完整继承 parent session 已暴露的第三方 MCP tools。ACN 原生`create_subagent` 等 delegation 工具仍然不暴露给 child，服务端也不允许 child 走原生路径创建二级 delegation；但 ACN 暂不按名称或 schema 猜测第三方 MCP 是否“间接创建agent”。如果某个 parent-visible MCP server 自身提供类似 spawn 的能力，暂时视为 parent已经选择暴露的 MCP 能力，后续再通过显式 capability / 权限系统处理。

### 6. 上下文继承

子代理继承的不是整个进程环境，而是显式的运行上下文快照。

快照至少应考虑：

- parent session id 与触发 turn id
- agent id、agent home、upstream、config path
- workspace root、cwd
- model/provider/reasoning 等 LLM 运行参数
- system/developer prompt 与必要的 session 历史投影
- 可用 skill/tool 子集
- MCP 配置与允许继承的 env var 名称
- 权限模式、sandbox、审批策略
- tracing / telemetry 上下文

密钥类信息应优先继承环境变量名或配置引用，而不是复制明文值。

### 7. 工具边界

子代理默认使用比主 agent 更窄的工具集。

默认不应提供：

- `ask_user`
- inbox / finalize / claim 写入相关能力
- 再创建子代理的能力

已拍板允许文件读写、`code_run`、完整 web/search（含 `web_request`）以及 parent session可见的全量 MCP tools。当前项目仍处于产品能力实现阶段，暂不做 delegation 专属的文件敏感路径 denylist、MCP denylist、schema 过滤或 localhost/private network 拦截。细粒度权限、审批和 hardening 后续单独拍板补充。

### 8. 定时任务兼容性

定时任务不属于本 PRD 的实现范围。

未来定时任务如果触发 agent 行为，应优先触发主 agent 的 scheduled turn 或等价主会话事件，而不是直接唤醒某个子代理成为独立主体。子代理仍然应是主 agent 执行过程中的内部委托任务。

### 9. 托管形态

`SessionDelegation` 不采用 daemon-first 设计。

子代理活体由 parent session runtime 托管。它可以跨 user turn 存活，并通过持久化状态让主 agent 在后续 turn 中读取进度和结果；但它不脱离 parent session 独立运行。parent session 进入 `Finalizing` 时开始拒绝新的 delegation 并强收束未完成 delegation；进入`Closed` 前，所有未完成 delegation 必须停止并进入终态。

该决策不否定未来引入 daemon。daemon 更适合无人值守的 durable scheduler、远程控制、多客户端连接或显式后台常驻 agent host。定时任务如果未来要求“用户不打开 ACN 也按时执行”，应另行设计 daemon / launchd / systemd / 远程服务托管；它不应反向要求`SessionDelegation` daemon 化。

### 10. 主 agent 克制感知

主 agent 对 delegation 的感知必须克制。

默认情况下，delegation 不应把完整 transcript、内部工具流或高频进度刷入主 agent 的canonical transcript，也不应因为子代理频繁产生日志而触发主 session compact。这里的“内部工具流”指 delegation child 自己执行任务时产生的细节，不包括主 agent 显式调用`read_subagent` 等 delegation 管理工具得到的结果；后者属于主 agent 的正式工具读取，应按普通 tool result 进入 canonical transcript。

主 agent 每个真实 user turn 默认只看到有界的结构化状态摘要，例如 delegation 标题、状态、当前步骤、最近更新时间、错误摘要、阶段性结果摘要和可读取的持久化引用。

主 agent 可以按需读取 delegation 详情，但读取应是显式动作，并且默认读取有界摘要而不是暴力读取完整日志。bounded result / events tail 只在主 agent 明确调用 `read_subagent` 诊断或采纳细节时进入上下文。

delegation 的执行进度必须落盘。即使 delegation 失败或被中断，主 agent 也应该能读取到已经完成的阶段、最近错误和阶段性结果，而不是只能看到一个空的失败状态。

默认注入主 agent 上下文的 delegation 状态摘要采用有界结构化 schema。字段先按以下形态设计：

- `id`
- `title`
- `role`
- `status`
- `current_step`
- `updated_at`
- `error_summary`
- `progress_summary`
- `result_ref`

文本字段必须设置长度上限，整体摘要也必须设置总长度上限，避免 delegation 状态投影触发主 session compact。完整事件日志、完整工具输出和完整 transcript 只能通过显式读取进入上下文。

每个 running subagent 还拥有仅自己可调用的 `update_subagent_progress`：它写入有界`current_step`、`summary` 与 artifact，并通过上述 runtime projection、`list_subagents` / `read_subagent(summary)` 和 TUI 供主 agent 查看。主 agent 不暴露也不调用该工具。

这是一条单向进度上报通道，不是子代理向主 agent 发问、等待回复或收到 ack 的协议。子代理不提供`ask_user`，`steer_subagent` 也只是主 agent 向 queued/running 子代理的异步追加指令：仅在下一次子代理模型请求前尝试递送，可能在递送前已终态。创建子代理时，主 agent 应把关键决策的 fallback写入 objective / constraints；如果子代理遇到没有 fallback、必须由主 agent 决定的 blocker，它应先上报 blocker、备选方案与已验证事实，再用最终结果形成可交接终态。主 agent 读取后决定是否创建新的子代理续办；本期不引入 `waiting_for_parent` 生命周期。

### 11. 持久化归属

delegation 持久化归属于 parent session。

目录形态拍板为 parent session 目录下的附属子域，例如：

```text
sessions/<session_id>/delegations/<subagent_id>/
```

具体文件字段可在实现设计中细化，但至少需要覆盖：

- `delegation.yaml`：元数据、状态、身份、角色、父 session / turn 关联、时间戳
- `events.jsonl`：结构化事件日志
- `progress.md` 或 `progress.json`：最新有界进度摘要
- `result.md`：最终结果或失败摘要
- `artifacts/`：较大的阶段性材料或工具产物引用

这些文件不是 agent home 级公共资源，也不是 team store 资源。它们只为 parent session恢复、TUI 展示和主 agent 按需读取服务。

### 12. 并发与默认工具边界

同一个 parent session 内默认最多允许 6 个 delegation 并行 running，具体由`agent.session.subagents.max_concurrent` 配置。超过上限的 delegation 应进入排队状态，而不是无限制并发启动。

delegation 默认禁止以下能力：

- `ask_user`
- session finalize
- inbox sync / inbox internalize
- claim / dispute / trace / maintainer upload 直接写入
- 再创建 delegation

delegation 可以使用文件读写、`code_run`、完整 web/search 和 parent 可见 MCP。文件修改或命令执行结果仍然只是主 agent 的内部素材；是否采纳、如何解释、是否进入最终回复或 finalize产物，仍由主 agent 负责。

当前项目尚未实现交互式权限审批功能，因此 delegation 暂不设计“把审批请求转交给主agent / 用户”的流程。后续如果引入权限审批，再单独拍板子代理遇到需审批工具动作时的失败、暂停或转交流程。

### 13. 异常退出恢复

parent session runtime 异常退出后，未完成 delegation 在下次恢复时默认标记为`abandoned`。

因为 `SessionDelegation` 不采用 daemon-first 设计，runtime 退出后 delegation 活体已经停止。恢复时不自动重启 delegation，避免在用户不知情的情况下重新执行文件修改、shell command、MCP 调用或外部请求。主 agent 可以读取 abandoned delegation 已落盘的进度、阶段性结果和错误摘要，再决定是否重新发起新的 delegation。

### 14. Finalize 收束

parent session 进入 finalize 前，未完成 delegation 不等待继续执行，直接停止并标记为`abandoned`。

finalize 可以读取 delegation 已落盘的最新有界进度摘要和阶段性结果引用，但不会等待delegation 完成，也不会把完整 delegation transcript 纳入 finalize 输入。这样 session退出路径不会被内部委托任务拖住。

### 15. TUI 展示

TUI 使用原生 slash 命令 `/subagents` 打开当前 session 的 delegation 只读页面。该命令与`/ps`、`/mcp` 一样属于即时 live panel 操作：即使 parent turn 正在运行也立即响应，不进入queued input，不写入 canonical transcript，也不发送给模型。

该页面与 `/ps`、`/mcp` 统一占满 live region 的可用高度，底部只保留两行：`↑/↓ to navigate  · Esc to back` 和原有 footer 状态行。页面支持 `↑` / `↓` 键滚动查看较长subagent 列表或详情，`Esc` 返回主会话交互页面。三个 live panel 同一时刻只展示一个，新面板打开时关闭旧面板。页面标题使用 `Session Subagents  read-only`。列表表头使用`Hash / Status / Title / Role / Update_time / Latest`，其中 `Hash` 显示 subagent id 去掉`subagent_` 前缀后的短 hash，`Update_time` 使用带日期的本地时间，表示该 subagent最近一条状态、进度或结果落盘的更新时间。

该面板展示 delegation 的运行情况，例如标题、状态、当前步骤、最近更新时间、错误摘要、阶段性结果摘要和变更文件。用户不能在面板中向 delegation 发送消息，也不能取消 delegation。

主消息流中不高频刷 delegation 进度。完成、失败或 abandoned 等终态是否插入克制的系统提示，可在 TUI 细化设计中继续拍板。

### 16. 搜索、MCP 与 Web

delegation 可以使用完整 web/search 能力，包括 `web_request` 的 GET / POST / PUT / PATCH / DELETE。

delegation 可以使用 MCP，且继承 parent session 已注册、已暴露的全量 MCP tools。实现层不做delegation 专属 MCP denylist、schema filtering 或安全能力分类。

delegation 不允许自己声明或新增 MCP server，也不为单个 delegation 单独复制一套 MCP配置。实现上优先复用 parent session 的 `McpConnectionManager` 与既有 server 连接；MCP stdio server 需要的 env 只来自 `.mcp.json` 中已配置的 `env` / `env_vars` / `bearer_token_env_var`，不额外继承整包进程环境。

MCP 调用需要带上 delegation 身份用于事件归属、进度路由和审计。默认情况下，同一 MCP server 的并发调用由 MCP server 自己处理，ACN 不为 delegation 额外加同 server 串行锁。

如果 MCP server 触发需要用户输入的 elicitation，delegation 不能直接向用户弹交互或阻塞等待用户。该请求应转为结构化状态 / 错误，由主 agent 决定是否在主会话中处理。

### 17. Memory、Session Search 与 Claim 上下文

delegation 内部 transcript、事件日志、工具输出和进度日志不进入 session_search 索引。

delegation 内部 transcript 不触发 memory_review，也不作为 memory_review 的输入。只有主 agent 显式采纳并写入主 session canonical transcript 的内容，才可能进入session_search、memory_review、compact 或 finalize 的既有路径。

delegation 启动时可以注入当前 memory 快照，但不提供 memory tool。该快照是只读上下文，用于让子代理理解用户偏好、项目约定和环境事实，不能由子代理直接修改。

delegation 启动时可以注入 local claims 快照或有界 claims projection。该内容同样是只读上下文，不代表子代理可以写 claim、dispute 或 trace。

### 18. 身份与角色注入

主 agent 创建 delegation 时，必须为 delegation 提供明确身份与角色信息。

启动上下文至少包含：

- `subagent_id`
- `parent_session_id`
- `parent_turn_id`
- `owner_agent_id`
- `title`
- `role`
- `objective`
- `constraints`
- `created_at`

子代理 system prompt 必须明确说明：它是主 agent 当前 session 内部的 subagent，不是 ACN中的独立 agent；用户不会直接与它对话；它没有 inbox、finalize、memory tool、claim / dispute / trace 写入能力；它不能再创建 subagent；它的输出只是主 agent 的内部素材。

`role` 用于限定 delegation 的工作姿态，例如代码检索、实现审查、测试验证、文档梳理等。`objective` 表示本次委托要完成的具体目标。`constraints` 用于承载工具限制、输出格式、进度落盘要求和禁止事项。

### 19. 启动上下文边界

delegation 启动时默认不注入完整 parent session transcript。

默认启动上下文只包含主 agent 明确写给 delegation 的 `objective` / `constraints`、有界recent context、只读 memory 快照、只读 local claims projection、workspace / cwd 和必要附件引用。主 agent 如果认为某段历史对 delegation 必要，应在创建 delegation 时显式摘录给它，而不是让 delegation 自动复制完整主会话历史。

### 20. 结果采纳

delegation 完成后只写入自己的 `result.md`、`progress` 和状态摘要，不自动进入 parent session 的 canonical transcript。

delegation 结果不会自动进入 session_search、memory_review、compact 或 finalize。只有主agent 显式读取并在自己的回复、工具结果或后续 committed turn 中采纳后，相关内容才进入主 session 的既有链路。

### 21. 文件修改并发

delegation 允许文件修改，但同一路径写入必须串行化。

文件读可以并行；不同路径的文件写可以并行；同一路径的文件写需要加轻量 path lock，避免多个 delegation 同时覆盖同一文件。文件修改仍需通过主 agent 解释、采纳和承担责任。

### 22. ID 与状态机

`DelegationId` 前缀使用 `subagent_`。

状态机先收敛为：

- `queued`
- `running`
- `completed`
- `failed`
- `abandoned`

用户不能取消 delegation，因此暂不引入用户语义的 `cancelled` 状态。异常退出恢复、finalize 收束或 runtime 主动停止未完成 delegation 时，统一进入 `abandoned`。执行错误进入`failed`。

### 23. 进度写入契约

delegation 进度由 runtime 自动事件与子代理主动摘要共同维护。

runtime 自动记录工具开始、工具完成、错误、状态转换等结构化事件到 `events.jsonl`。同时提供一个窄内部工具，例如 `update_subagent_progress(current_step, summary, artifacts)`，让子代理主动维护 `progress.md` 或 `progress.json`。这样即使 delegation 失败，也至少有runtime 事件；正常执行时则有模型主动整理过的阶段性摘要。

### 24. TUI 常驻状态提示

即使用户没有打开 `/subagents` 面板，TUI 也应在克制位置显示 delegation 后台运行状态。

例如底部状态区或 live area 可以显示类似 `3 subagents running in background` 的一行提示。该提示不写入 `messages.jsonl`，也不进入主 agent 上下文，只作为 UI 状态投影。

### 25. 主 agent 可用的 subagent 工具 API

主 agent 默认只获得四类 delegation 管理能力：

- `create_subagent`
- `list_subagents`
- `read_subagent`
- `steer_subagent`

`create_subagent` 用于创建新的内部委托任务。`list_subagents` 用于读取当前 session 内delegation 的有界状态列表。`read_subagent` 用于显式读取某个 delegation 的进度摘要、阶段性结果、最终结果或有限事件窗口。`steer_subagent` 用于向 queued / running delegation 追加结构化任务修正。

默认不提供 `wait_subagent`。等待型工具容易诱导主 agent 反复阻塞等待或频繁读取，违背“主 agent 克制感知、子代理后台推进”的设计方向。主 agent 需要结果时，应通过`read_subagent` 读取有界摘要或结果引用。

### 26. 执行架构

实现上引入专用 `DelegationRunner`。

`DelegationRunner` 负责 delegation 的排队、启动、状态转换、并发限制、落盘、进度事件、异常终止和 abandoned 收束。它可以复用现有 `SessionEngine` 的模型调用、tool registry、事件类型、transcript 工具函数和配置解析，但不直接塞进主 session 的 turn loop。

主 session turn loop 仍然只负责主 agent 的对话回合。delegation 的后台执行通过`DelegationRunner` 向持久化文件和 TUI 状态投影写入进展，主 agent 需要时再显式读取。

### 27. 结果进入主上下文的形式

delegation 的状态默认以 `delegation summary projection` 进入主 agent turn context。

主 agent 的 session system prompt 可以包含静态 delegation 使用纪律，例如什么时候创建delegation、何时避免创建、如何用 `list_subagents` / `read_subagent` 克制读取、如何把用户针对子代理的后续要求转成 `steer_subagent`，以及 delegation 输出只作为主 agent 内部素材。这类静态能力说明随新 session 的 `system_prompt.md` 冻结，不包含具体 delegation 运行状态。

该 projection 以 runtime synthetic user context 的形式，注入到每个真实用户触发的 agent turn前。它不拼入 session system prompt，不注入 tool_result 协议轮次，不为 `!pwd` 这类本地 shell命令注入，也不写入 canonical transcript。这样可以保持 session 创建后的 system prompt 前缀稳定，同时让主 agent 在切换 turn 时低频感知后台 delegation 状态。

该 projection 只包含有界字段，例如 id、title、role、status、current_step、updated_at、error_summary、progress_summary、result_ref、changed_files。它不是普通 tool result，不携带完整 transcript 或大段工具输出。

只有主 agent 显式调用 `read_subagent` 时，更长的进度、阶段性结果、最终结果或有限事件窗口才作为本 turn 的工具结果进入上下文。

### 28. 文件修改采纳语义

delegation 可以直接修改同一个 workspace，不先引入 shadow workspace。

为了控制复杂度，暂时不做隔离工作区、patch staging 或主 agent 手动 apply diff。delegation如果完成了文件修改，必须在 result 中列出 `changed_files` 和简短修改摘要。主 agent 在最终回复用户前，应显式检查相关 diff 或文件内容，并由主 agent 对最终采纳负责。

该策略保留未来升级空间：如果后续引入权限审批或多 delegation 大规模并行写入，再考虑shadow workspace、patch bundle 或主 agent 显式 apply/merge 流程。

### 29. 超时、失败与重试

delegation 默认不自动重试。

每个 delegation 应有 wall-clock timeout 和 token budget。wall-clock timeout 默认`7200` 秒，由 `agent.session.subagents.wall_timeout_secs` 配置；queued 等待时间不计入，从 delegation 进入 running 并开始 executor 后计时，running 期间收到 steering 不重置计时。超时、模型错误、工具错误或 runtime 错误导致任务无法继续时，状态进入 `failed`，同时保留已经落盘的 progress、events、result 草稿和 artifacts。

如果主 agent 认为需要继续，应创建新的 delegation，或在原 delegation 仍为 queued/running时使用 `steer_subagent` 修正任务；runtime 不应在用户和主 agent 不知情的情况下悄悄重跑。

---

## 非目标

- 不实现 turn 内 one-shot 版本作为第一目标。
- 不把子代理做成可被用户直接对话的独立会话。
- 不把子代理接入 ACN inbox/finalize/claim 主体生命周期。
- 不支持子代理嵌套。
- 不在本 PRD 中拍板定时任务。

---

## 分阶段待拍板问题

### 阶段 A：产品语义与模型上下文

1. 主 agent 什么时候应该创建 delegation：完全由模型工具调用决定，还是由某些命令 / skill / prompt 触发。
2. delegation 的状态摘要字段长度上限和整体 token / 字符预算。
3. `read_subagent` 的读取模式、默认窗口大小和最大输出预算。

### 阶段 B：持久化与生命周期

1. delegation 目录下各文件的具体 YAML/JSONL/Markdown 字段 schema。
2. `DelegationId` 的生成位置。
3. running delegation 在 finalize / abnormal restore 时标记 abandoned 前是否写入最后一条synthetic event。

### 阶段 C：执行与资源控制

1. delegation 的排队顺序、默认超时数值和失败展示策略。
2. delegation 的 token budget、上下文窗口和模型选择策略。

### 阶段 D：工具与权限

1. 后续是否补 delegation 专属权限审批、MCP capability manifest、文件敏感路径 denylist 或web SSRF hardening。
2. `code_run`、web_request 与 MCP 的命令超时、输出截断、取消语义和审计展示是否需要更细的产品约束。
3. 如何在服务端硬性禁止 delegation 再创建 delegation。

### 阶段 E：TUI 展示

1. `/subagents` 只读面板的具体布局、字段顺序和摘要长度。
2. delegation 常驻状态提示在 live box、底部状态区还是其他位置显示。
3. running turn 期间 delegation 状态更新如何避免刷屏。
4. 完成 / 失败 / abandoned 时是否向主对话插入可见通知。
5. 多个 delegation 并行时的排序、命名和进度摘要。

### 阶段 F：未来定时任务兼容

1. durable scheduled turn 是否需要 daemon / 系统托管进程。
2. 定时任务触发后创建的是新主 session、恢复已有主 session，还是写入某个 agent 级任务队列。
3. 定时任务与 delegation 是否共享底层 task store，还是只共享运行上下文快照与权限模型。

---

## 实施规划与验收策略

### 实施纪律

每次切换实现阶段前，必须重新阅读本 PRD，确认当前阶段仍然符合已拍板决策。阶段内如果遇到本文没有覆盖的设计分歧，由实现者在本 PRD 末尾“追加拍板记录”中记录可选方案、最终选择和选择理由，再继续编码。

### 阶段 0：PRD 与基线确认

Todo：

- 重新阅读 `docs/PRDs/PRD_subagents.md`。
- 阅读当前仓库 `AGENTS.md`、`SessionEngine`、`ToolRegistry`、session store、TUI runtime和 MCP manager 的相关代码。
- 将实施阶段、验收策略和追加拍板记录补入 PRD。

验收：

- PRD 中存在阶段化 todo、每阶段验收策略和追加拍板记录。

### 阶段 1：Delegation 数据模型与持久化

Todo：

- 新增 `SessionDelegation` 相关类型：id、状态、元数据、事件、进度、结果、读取模式。
- 新增 parent session 下的 delegation store，目录为`sessions/<session_id>/delegations/<subagent_id>/`。
- 实现原子写 `delegation.yaml`、append `events.jsonl`、更新 `progress`、写入 `result`。
- 实现 list/read/abandon 等纯存储操作与单元测试。

验收：

- 能创建 delegation 目录并落盘 metadata、events、progress、result。
- list 按状态和更新时间返回有界摘要。
- failed / abandoned 状态下仍可读取阶段性进度。
- 单元测试覆盖 id 生成、状态转换、读写 roundtrip、events tail 截断。

### 阶段 2：DelegationRunner 与后台生命周期

Todo：

- 引入 `DelegationRunner`，负责 delegation 的排队、并发上限、启动、状态转换、超时、失败和 abandoned。
- runner 复用主 session 的模型配置、tool registry 能力和 MCP manager，但不进入主`SessionEngine` turn loop。
- 实现 `steer_subagent` 对 queued/running delegation 的结构化追加指令。
- 实现 session finalize / runtime drop 前的 abandoned 收束入口。

验收：

- 同 session 默认最多 6 个 running delegation，超出进入 queued；上限可由`agent.session.subagents.max_concurrent` 配置。
- completed / failed / abandoned 不接受 steering。
- runtime 异常恢复时未完成 delegation 可标记 abandoned。
- 超时或执行错误进入 failed，并保留 progress/events/result 草稿。

### 阶段 3：主 agent 工具 API 与上下文投影

Todo：

- 将 `create_subagent`、`list_subagents`、`read_subagent`、`steer_subagent` 暴露给主 agent。
- 子代理可用工具集中移除 `ask_user`、memory、session_search、claim/dispute/trace 写入、finalize、inbox、再创建 delegation。
- 在真实 user agent turn 前注入有界 `delegation summary projection`，但不污染 system prompt。
- 确保 delegation child 内部 transcript、自动 projection 和高频 events 不进入 session_search / memory_review / canonical transcript；主 agent 显式调用 delegation 管理工具得到的tool_use / tool_result 按普通工具语义落 canonical transcript。

验收：

- 主 agent 工具定义中出现四类 delegation 工具，且没有 `wait_subagent`。
- delegation runner 调用工具时无法再创建 delegation，也无法使用禁止工具。
- 主 agent 默认只看到有界状态摘要；显式 `read_subagent` 才返回更长内容。
- 相关测试覆盖工具定义、工具分发、projection 截断和禁止嵌套。

### 阶段 4：TUI 状态提示与 `/subagents` 只读面板

Todo：

- TUI 状态中保存当前 session delegation snapshot。
- 常驻区域显示类似 `3 subagents running in background` 的 UI-only 提示。
- `/subagents` 打开 delegation 只读面板，展示列表、状态、当前步骤、更新时间、错误摘要、result ref 和 changed files。
- completed / failed / abandoned 插入克制 UI-only 终态通知，不写入 canonical transcript。

验收：

- 未打开 `/subagents` 时，running delegation 数量能在 live/status 区显示。
- `/subagents` 面板只读，不能发消息也不能取消。
- 面板在窄宽度下不溢出，列表排序和状态文案一致。
- TUI 单元测试和 tmux smoke test 覆盖状态提示与面板打开。

### 阶段 5：验证与 code-review skill

Todo：

- 运行格式化、clippy、测试、check。
- 使用 code-review skill 检查存储/runner、工具/API、TUI、上下文隔离。
- 运行 TUI smoke test。

验收：

- `cargo fmt`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check` 通过。
- code-review skill 无未处理高风险问题。
- TUI smoke test 无阻塞问题。

---

## 追加拍板记录

### A. delegation 触发入口

选项：

- A1：只允许主 agent 通过 `create_subagent` 创建。
- A2：额外提供用户可直接调用的 `/delegate` 命令。
- A3：允许 skill / prompt 绕过主 agent 直接创建 delegation。

选择：A1。

理由：用户始终只与主 agent 对话。自然语言里用户可以要求“派几个子代理查一下”，但最终是否创建、创建几个、给什么上下文，都由主 agent 决定。skill / prompt 可以建议模型使用delegation，但不绕过主 agent 判断。

### B. `read_subagent` 读取模式

选项：

- B1：单一读取接口，自动返回所有可用内容。
- B2：区分 `summary`、`result`、`events_tail`、`artifact` 读取模式。
- B3：默认允许读取完整 transcript。

选择：B2。

理由：读取模式显式化可以保护主 agent 上下文。默认模式为 `summary`；`result` 返回最终结果，`events_tail` 只返回有限事件窗口，`artifact` 读取明确引用的产物。完整 transcript不作为常规读取模式，只保留给后续 debug 设计。

### C. 模型与预算继承

选项：

- C1：delegation 默认完全继承主 agent 当前 provider/model/reasoning。
- C2：delegation 默认使用固定轻量模型。
- C3：每次创建 delegation 都让主 agent 显式选择模型。

选择：C1。

理由：默认继承最符合用户预期，也减少 provider 兼容复杂度。delegation 仍有独立 token budget、wall-clock timeout 和工具输出预算。后续如果需要不同模型，可通过配置里的 role profile 或用户显式要求开放，不在当前引入模型自由切换。

### D. shell command 环境

选项：

- D1：复用主 agent 当前工具 runner 的 cwd/env/sandbox，并注入 delegation 只读标识。
- D2：为 delegation 设计全新的 env allowlist。
- D3：禁止 delegation 执行 shell command。

选择：D1。

理由：当前项目尚未实现交互式权限审批，也没有完整 env 权限系统。复用既有 tool runner可以减少行为分裂；额外注入 `ACN_SUBAGENT_ID`、`ACN_PARENT_SESSION_ID` 等标识，便于日志和脚本识别来源。

后续修订：V 已改选 V1，delegation child 继续复用主 agent 的 `code_run` 执行语义。当前不引入额外 OS sandbox；后续如补权限系统，再单独拍板更细的命令审批与写保护。

### E. TUI 终态通知

选项：

- E1：终态完全不提示，只在 `/subagents` 面板可见。
- E2：running 只显示常驻状态；completed / failed / abandoned 插入短 UI-only 通知。
- E3：所有进度都刷入主消息流。

选择：E2。

理由：running 期间不刷屏，符合克制感知；终态给一个短提示能避免用户错过后台结果。该通知不写入 canonical transcript，不进入模型上下文，真正内容仍通过 `/subagents` 或`read_subagent` 读取。

### F. runner 托管接入点

选项：

- F1：把 `DelegationRunner` 直接放入 `SessionEngine`，由主 turn loop 管理创建与调度。
- F2：把 `DelegationRunner` 作为 `ToolRegistry` 的父会话能力，按 parent session id 懒加载runner；子执行器使用裁剪后的 `ToolRegistry`。
- F3：先做独立 daemon / background runtime，再由主 session 远程调用。

选择：F2。

理由：delegation 的创建入口本来就是主 agent 工具 API，放在 `ToolRegistry` 可以在服务端硬性区分父会话工具面和子执行器工具面。它也避免把后台任务调度塞进主`SessionEngine` turn loop，同时符合当前“非 daemon-first”的产品形态。后续如果定时任务需要 durable daemon，可以在这个边界之外新增托管层，而不反向污染 `SessionDelegation`。

### G. TUI delegation 状态来源

选项：

- G1：`DelegationRunner` 主动向 TUI 推送高频进度事件。
- G2：TUI 低频轮询当前 session 的 `delegations/` store，生成只读 UI snapshot。
- G3：把 delegation 进度写成普通 `SessionEvent`，复用主 transcript 渲染流。

选择：G2。

理由：TUI 只需要展示状态，不应把 delegation 的高频事件并入主 session 事件流。低频轮询session 本地 store 简单、可恢复，也天然适配失败后读取阶段性结果。轮询结果只进入`ChatWidgetState`，不写 `messages.jsonl`，不进入 canonical transcript，也不触发session_search / memory_review。

### H. steering 交付语义

选项：

- H1：`steer_subagent` 只记录事件，执行器不保证消费。
- H2：queued delegation 启动前注入历史 steering；running delegation 在每次模型请求前拉取新增 steering，并作为内部 steering 摘要追加到 delegation 自己的上下文。
- H3：为每个 delegation 建立长期双向 mailbox / inbox。

选择：H2。

理由：H1 会让工具返回成功但不影响执行，语义不诚实；H3 容易把 delegation 做成可对话主体，偏离用户始终只和主 agent 对话的边界。H2 把 steering 当作主 agent 对内部任务的目标修正，既可跨 turn 生效，又不引入用户可寻址 inbox。

### I. terminal 状态不可变

选项：

- I1：runtime 后写入可以覆盖 `completed` / `failed` / `abandoned`。
- I2：store 层强制 terminal 状态不可再被 start、progress、complete 或 transition 覆盖。

选择：I2。

理由：finalize / abnormal restore 要求 unfinished delegation 必须落成 `abandoned`。如果store 允许迟到的后台 task 再写回 completed/failed，就会破坏生命周期收束语义。terminal不可变应放在 store 层，而不是只靠 runner 约束。

### J. 读取输出预算

选项：

- J1：`read_subagent` 返回原始 metadata、完整 result 和完整 artifact。
- J2：默认 summary 只返回有界 `DelegationSummary`；result / artifact / events_tail 均在服务端强制截断并返回 truncated 标记。

选择：J2。

理由：主 agent 对 delegation 的感知应克制。显式读取可以拿到更多内容，但也不能绕过上下文预算。完整大产物应通过 artifact 引用或文件路径由主 agent 再分段读取。

### K. MCP 身份透传边界

选项：

- K1：把 delegation id 注入 MCP tool arguments。
- K2：不污染远端 MCP tool schema，在本地 tool dispatch / progress / log 上携带`current_turn_id` 中的 delegation id；后续如需远端审计再扩展 MCP manager context。

选择：K2。

理由：MCP tool arguments 属于第三方 server schema，擅自注入字段会破坏兼容性。暂时先保证本地事件归属和 TUI 进度归属明确；远端 server 级审计作为 MCP manager 的独立扩展处理。

### L. TUI 面板入口与优先级

选项：

- L1：为 delegation 单独保留全局键盘快捷键。
- L2：使用与 `/ps`、`/mcp` 一致的 `/subagents` 原生命令；已有 panel 继续优先处理面板内按键，用户用 `Esc` 返回后再输入其他 panel 命令。

选择：L2。

理由：三个管理视图统一通过可发现的 slash command 打开，避免为单一 panel 引入额外全局按键语义。`/subagents` 在 turn 运行中仍即时分发，不会被当作模型输入或进入队列；该选择不改变用户不能直接操作 delegation 的产品边界。

### M. 自动上下文快照范围

选项：

- M1：delegation 启动时自动复制完整 parent session transcript。
- M2：delegation 启动时只注入身份、workspace/MCP/tool 运行信息、只读 memory 摘要和主agent 显式写入的 objective/constraints；必要历史由主 agent 摘录进 objective/constraints。
- M3：完全不注入 memory / workspace 等上下文，只靠 objective。

选择：M2。

理由：M1 会污染 delegation 上下文并增加 compact 压力，也违背“主 agent 克制感知”的方向。M3 又太弱，容易让 delegation 缺少项目偏好和运行边界。M2 保留必要只读快照，同时要求主agent 对委托上下文负责，不自动复制完整主会话历史。

### N. delegation event 序号与 steering 读取

选项：

- N1：每次追加事件前读取完整 `events.jsonl`，用最后一条事件计算 seq。
- N2：维护 `events.seq` 轻量 sidecar 文件；旧数据缺少 sidecar 时才流式扫描最后事件兜底。
- N3：引入 sqlite / sled 等嵌入式事件库。

选择：N2。

理由：N1 在长时间运行和高频进度下会退化成 O(n²)，违背有界读取原则。N3 对当前 session本地文件存储来说过重。N2 保持 YAML/JSONL 文件形态，同时让事件追加接近 O(1)。steering读取也采用有界流式读取，queued/running delegation 分批消费，不一次性把大量 steering塞进子上下文。

### O. shell command 超时收束

选项：

- O1：超时后只把 delegation 标记 failed，不处理正在运行的子进程。
- O2：只依赖 `kill_on_drop` 杀直接子进程。
- O3：`code_run` 自己管理 spawn/wait；Unix 下创建独立 process group，超时时杀整个 group。

选择：O3。

理由：delegation 允许文件修改，超时或 abandoned 后继续留下 shell 副作用是不可接受的。O1 只改状态不收束真实执行。O2 对 shell 拉起的后台孙进程不可靠。O3 的实现复杂度仍可控，并且对普通 `code_run` 的成功输出形态保持不变。

后续修订：本项同时适用于 delegation child 的 `code_run`。子代理可以使用 `code_run`，但仍复用普通 command runner 的超时、输出截断和进程组收束策略。

### P. delegation MCP 继承过滤

选项：

- P1：继承 parent session 暴露的所有 MCP tools。
- P2：完全禁止 delegation 使用 MCP。
- P3：继承 parent MCP，但按 server/raw/visible tool name 做 forbidden capability denylist。

选择：P1。

理由：当前产品阶段优先把 delegation 做成真正有用的后台协作能力。子代理继承 parent session 已暴露的 MCP tool catalog，不额外猜测第三方 MCP tool 的语义，不做名称或 schema过滤。ACN 原生 `ask_user`、finalize、inbox、claim/dispute/trace、memory、session_search和再创建 delegation 等能力仍通过本地 tool profile 禁掉；MCP 侧的细粒度权限后续再以capability manifest 或权限系统单独设计。

### Q. memory-review tool profile

选项：

- Q1：memory review fork 只在 prompt 上要求使用 memory tool，但 registry 仍保留 file / shell / web 工具。
- Q2：memory review fork 的 registry 服务端硬性只开放 memory tool。

选择：Q2。

理由：delegation 接入后需要明确 tool profile 边界。memory review 的文档语义是只能通过原生`memory` 工具写持久记忆，服务端也应保持一致，避免后台 fork 获得不必要的 shell、文件或 web权限。

### R. command output 与取消边界

选项：

- R1：进程结束后完整收集 stdout/stderr，再按 tool result 字符上限截断。
- R2：pipe reader 在读取时只保留有界前缀，超出部分继续 drain 但不入内存；timeout 或future drop 时清理进程组并 abort pipe reader。

选择：R2。

理由：R1 虽然能保护模型上下文，但不能保护 ACN 进程内存。delegation 允许并发 shell，高输出命令必须在 pipe 层就有内存上限。timeout / abandoned 还需要覆盖 future 被 abort 的路径，因此 command runner 持有进程组清理 guard，避免 finalize 或 runtime 收束时只改状态不停止真实副作用。

### S. MCP server 并发

选项：

- S1：delegation 并发调用同一个 MCP server，由 server 自己处理竞态。
- S2：delegation child profile 对同一 MCP server 默认串行化；主 agent 的 MCP 调用不额外受这个 delegation lock 影响。

选择：S1。

理由：delegation 继承完整 parent MCP 后，ACN 不再尝试在本地推断某个 MCP server 是否有状态或是否需要串行。并发语义交给 MCP server 自身处理；如果后续出现具体 server 竞态，再在 MCP 配置或权限层做显式约束。

实现约束：MCP discovery / status / `tools/call` 都复用本次 ACN 运行期内同一条常驻 client/session；parent 与 child 不创建短生命周期 call client，也不维护本地 server lock。stdio transport 支持同连接多 in-flight request；Streamable HTTP 的慢 JSON response 可能被 SDK worker 串行化，这是已接受的transport 差异，不以短连接、client pool 或 ACN 锁规避。并发限制由 delegation runner 的全局并发和MCP server 自身承担。

### T. finalized session 与 delegation 恢复

选项：

- T1：`Closed` session 都允许 resume，resume 时清掉 `finalized_at`。
- T2：finalized session 是终态，不进入 resumable 列表，也不能被 reopen。

选择：T1。

理由：finalize 的职责是为当前会话阶段产出 claim / trace 等结果，不是把 session 永久封存。`Closed` session 都可以 resume；如果它之前 finalized 过，resume 时清掉 `finalized_at` 与`closed_at`，但保留 `recapped_until`、compaction pointer 和既有 claim/trace 结果。这样恢复对话不会丢掉已经完成的 finalize 产物，也不会把 finalize 错误理解成不可继续聊天的终态。

### U. parent session 写入一致性锁

选项：

- U1：只靠每个方法开头读取 `session.yaml` 判断状态，不额外加锁。
- U2：复用 `finalize.lock` 同时保护 finalize 与普通 session 写入。
- U3：新增 per-session `session.lock`，保护 `messages.jsonl` 与 `session.yaml` 的读改写临界区；`finalize.lock` 继续只表示 finalize 作业互斥。

选择：U3。

理由：`append_messages` 可能在读到 `Open` 后，与 `mark_finalizing` / finalize 状态写入交错，导致迟到 append 覆盖 metadata。单靠开头状态检查不够；复用`finalize.lock` 又会混淆 finalize 作业互斥与 session 文件一致性的语义。独立`session.lock` 范围更清楚，只包住本地文件读改写，不覆盖 LLM recap 或后台长任务。

### V. delegation child 的 shell / code_run 边界

选项：

- V1：继续暴露 `code_run`，只靠 prompt 要求不要写受保护路径。
- V2：暴露 `code_run`，在 macOS sandbox / Linux namespace 等 OS 级机制可用时为 child 加写保护；不支持的平台降级为失败。
- V3：暂时不向 delegation child 暴露 `code_run` / shell，只保留 file、web/search 和过滤后的 MCP；未来需要 shell 时重新拍板。

选择：V1。

理由：子代理需要能完成真实调研、验证和局部实现任务，只给 file/web 会显著削弱产品价值。当前工具体系尚未实现权限审批，主 agent 本身也已拥有高权限 `code_run`。delegation child先复用主 agent 的 command runner、超时和输出截断语义；更细的 shell 审批、OS sandbox 或写保护后续按实际风险补。

### W. MCP server 串行范围

选项：

- W1：只串行 delegation child 之间的同 server MCP 调用，parent 主 agent 不受影响。
- W2：parent 与 delegation child 对同一 MCP server 的调用共享同一把本地 server lock。
- W3：完全不串行 MCP，依赖 server 自己处理并发。

选择：W3。

理由：W3 与 S1 保持一致。ACN 不在 delegation 层维护 MCP server lock，parent 和 child调用同一 server 时也不额外串行；如果某个 server 需要互斥，应由 server 或显式 MCP 配置声明，而不是由 ACN 猜测。

实现约束同 S1：本地不维护 server lock，也不因复用同一个 MCP client 而主动把调用串行化。ACN 不推断或补偿 transport/server 的并发行为；SDK 对 Streamable HTTP 慢 JSON response 的 worker串行化是可接受差异，不切回短连接。

### X. MCP denylist 形态

选项：

- X1：只按 MCP tool 的原始 name 做精确 denylist。
- X2：按 raw name、visible name 和 server name 做归一化匹配，移除大小写、下划线、连字符等差异，并覆盖 write/edit/create/update/delete/exec/shell/filesystem 等高风险词根。
- X3：完全禁止 delegation 使用 MCP。
- X4：不启用 delegation 专属 MCP denylist，完整继承 parent visible MCP tools。

选择：X4。

理由：名称 denylist 会把 ACN 带回“猜第三方工具语义”的不稳定状态，也容易误伤正常 MCP。当前拍板选择产品能力优先：child 看到 parent 已经暴露的 MCP tool，就可以调用。后续如果要做 MCP 权限，应基于显式 capability 或用户审批，而不是词根猜测。

### Y. delegation child 的 web 工具边界

选项：

- Y1：继承完整 web 工具，包括 `web_request` 的 POST / PUT / PATCH / DELETE。
- Y2：只暴露 `web_search` 和 `web_fetch`，且禁止访问 localhost、loopback、private / link-local 等本机或局域网地址。
- Y3：完全禁止 delegation 使用 web。

选择：Y1。

理由：主 agent 可用完整 web 工具，delegation 作为主 agent 的内部执行体也需要完整网络能力才能完成真实调研、API 验证和自动化任务。暂时不做 localhost/private network 拦截，也不移除 `web_request`。更细的 SSRF / side-effecting HTTP 策略后续单独设计。

### Z. file 读写边界

选项：

- Z0：不做 delegation 专属 protected roots / secret denylist，文件工具沿用主 agent 语义。
- Z1：只禁止 delegation 写入 ACN runtime protected roots，允许读取。
- Z2：delegation 的 file_read / file_write / file_patch 都禁止触达 protected roots；路径判断需要解析已存在祖先 symlink。
- Z3：完全禁止 delegation 文件工具。

选择：Z0。

理由：当前项目主 agent 的 `workspace_root` 本来不是 sandbox，文件工具支持相对路径和绝对路径。delegation 先沿用同一语义，避免过早引入一套和主流程不一致的保护边界。memory 路径这类主 agent 已有的工具保护继续保留；delegation 专属 secret denylist / protected roots后续遇到实际风险再补。

### AA. `list_subagents` 输出上限

选项：

- AA1：返回当前 session 下所有 delegation 摘要。
- AA2：默认返回固定上限的排序摘要，并返回 omitted count；未来需要时再加分页参数。
- AA3：只返回 running / queued，不返回历史终态 delegation。

选择：AA2。

理由：主 agent 对 delegation 的感知必须克制。AA1 会让长 session 中累积的历史 delegation持续进入上下文；AA3 会让 completed / failed / abandoned 的阶段性结果不易发现。AA2 保留按需可见性，又给输出和 I/O 一个明确上限。

### AB. MCP schema 级过滤

选项：

- AB1：只按 MCP server/tool 名称过滤。
- AB2：名称过滤之外，再检查 input schema；只要参数暴露 path/file/url/command/body/method等能力，就不向 delegation child 暴露。
- AB3：完全禁止 delegation 使用 MCP。
- AB4：不做 delegation 专属 MCP schema 过滤，完整继承 parent visible MCP tools。

选择：AB4。

理由：本项由 P1 / X4 覆盖。当前不按 schema 猜测 MCP 能力；否则通用参数、动态 schema 或温和命名都会造成误判。后续 MCP 权限应依赖显式 capability，而不是 schema 词根过滤。

### AC. delegation child 的 MCP 暴露策略

选项：

- AC1：继续用名称 denylist + input schema denylist 暴露一部分 MCP tools。
- AC2：不向 delegation child 暴露任何 MCP tool；保留 parent MCP manager 复用能力和过滤 helper，等后续有显式 `delegation_safe` allowlist / capability manifest 后再开放。
- AC3：完全删除 delegation 与 MCP 的兼容设计。
- AC4：直接向 delegation child 暴露 parent visible MCP tools 的完整集合。

选择：AC4。

理由：本次修订明确选择产品能力优先：子代理可以访问主代理可访问的全量 MCP。ACN 不为delegation 额外维护 MCP 过滤 helper 或 allowlist；后续如果要补权限模型，再按显式 capability和审批流程设计。

### AD. finalize 与 restore 的 delegation 收束强度

选项：

- AD1：finalize 和 restore 都 best-effort abandoned，失败只记日志。
- AD2：finalize 使用 hard abandoned，失败则 finalize 报错；restore 保持 best-effort，避免坏 delegation 文件阻断主 session 恢复。
- AD3：finalize 和 restore 都 hard abandoned，任何 delegation 收束失败都阻断主流程。

选择：AD2。

理由：finalize 是 session 终态收束，产品契约要求不能在 `Closed` session 中留下queued/running delegation，因此应强制失败可见，并按 AF 语义让 session 留在 `Finalizing`等待重试或排障。restore 场景不同：runtime 已异常退出，delegation 活体事实上已经不在；如果某个 delegation metadata 损坏，阻断主会话恢复反而伤害用户。restore 继续 best-effort 记录warning，由主 agent 后续读取进度/错误并决定是否重建 delegation。

### AE. delegation tool 结果进入 canonical transcript 的粒度

选项：

- AE1：delegation 管理工具的 `tool_use` / `tool_result` 按普通工具语义完整进入 canonical transcript，不做 delegation 专属 opaque 替换。
- AE2：canonical transcript 中只保留 delegation 管理工具的 opaque stub，不保存 id、title、status、progress、result_ref、changed_files 或错误细节。
- AE3：完全不提交 delegation 管理工具的 tool_use / tool_result。

选择：AE1。

理由：主 agent 显式调用 delegation 管理工具时，这个读取动作已经是主 agent 对子代理结果的正式采纳或诊断，应当像其他工具结果一样进入 canonical transcript。真正需要克制的是自动projection、delegation child 内部 transcript、高频 events 和内部工具流，而不是把主 agent主动读取到的 `read_subagent` / `list_subagents` 结果从正式 transcript 中替换掉。AE2 会让主 agent 当轮看过的正式工具结果在落盘后消失，不利于追踪问题；AE3 会破坏provider-neutral transcript 中 tool_use / tool_result 成对的基本结构。

### AF. finalize abandon 失败后的 session 状态

选项：

- AF1：先把 parent session 标成 `Finalizing`，abandon 或 finalize 失败也保持该状态；supervisor job 记录错误并按 retry 策略重试，只有全部收束成功后才进入 `Closed`。
- AF2：进入 `Finalizing` 前先 hard abandon；进入后再 hard abandon 一次。如果第二次失败，将未关闭、未 finalized 的 session 回滚为 `Open`。
- AF3：abandon 失败时直接标记 `Closed`，把错误写入日志。

选择：AF1。

理由：AO 已拍板只有 `Closed` session 可以 resume，因此失败时回滚 `Open` 会把用户已经退出的session 重新打开，也会重新允许输入和 delegation 创建，不符合 finalize 的收束语义。AF3 会在delegation 未收束时关闭 session，破坏 finalize 的硬收束契约。AF1 让失败可见且状态诚实：这个 session 还没有完成关闭，不能 resume；用户通过 supervisor jobs 和 session event log定位失败。当前版本不提供显式 retry / rescue 入口，后续单独拍板。

### AG. 空 session 删除与 delegation 目录

选项：

- AG1：空 session 只看 `messages.jsonl` 和 turn journal，哪怕存在 `delegations/` 也删除。
- AG2：只要 parent session 目录下存在非空 `delegations/`，`delete_empty_session` 就不删除该 session。
- AG3：删除 session 前把 delegation 结果移动到 agent 级归档目录。

选择：AG2。

理由：delegation 进度和阶段性结果归属于 parent session。第一 turn 里可能先创建 delegation再遇到 finalize / interrupt，此时 messages 仍为空，但 delegation 已有持久化结果。AG1 会造成数据丢失；AG3 引入新的归档语义，超出本需求。AG2 最小且符合“失败也能读到阶段性结果”。

### AH. child web / file 的前置 I/O 上限

选项：

- AH1：先完整读取文件或响应体，再按 tool result 字符上限截断。
- AH2：delegation child 的 `file_read` 先按字节上限读取；`web_fetch` 对响应体流式读取并按字节上限停止；child 收到 30x redirect 直接失败。
- AH3：完全禁止 child 读取大文件和 web 响应。
- AH4：不做 delegation 专属前置 I/O / redirect 规则，沿用主 agent 文件与 web 工具语义。
- AH5：不做 delegation 专属权限边界或 redirect 拦截；但 delegation child 的 `file_read`先按 tool 输出预算做有界读取，避免后台子任务为超大文本整包占用内存。

选择：AH5。

理由：Y1 / Z0 取消的是权限与访问边界，不代表可以取消资源上限。delegation 会后台并发运行，`file_read` 如果先把超大文件整包读入内存再截断，容易造成稳定性问题。AH5 只保留有界读取这种资源保护：不恢复 secret denylist、不限制绝对路径、不拦截 localhost/private web，也不对 `web_request` 做 child-only 策略。

### AI. create delegation 与 finalize 的本地串行化

选项：

- AI1：`create_subagent` 只读取 `session.yaml` 判断 parent 是否 `Open`。
- AI2：`create_subagent` 在读取 parent 状态并创建 delegation metadata 时持有同一个`session.lock`，与 `mark_finalizing` 的状态切换串行。
- AI3：引入新的 daemon / sqlite transaction 统一管理 session 与 delegation 生命周期。

选择：AI2。

理由：AI1 存在 create 与 finalize 并发时的读写窗口。AI3 对当前非 daemon-first 的产品形态过重。AI2 复用已有 session 写入一致性锁，临界区只覆盖状态检查与 delegation 创建，不覆盖子代理后台执行，因此能收住竞态而不拖慢长期任务。

### AJ. turn journal 与 active compaction 的 delegation redaction

选项：

- AJ1：delegation 管理工具不做专属 redaction；canonical transcript、turn journal preview 和active-turn compaction 都按普通工具消息处理，只使用既有截断 / 压缩策略。
- AJ2：canonical transcript、turn journal preview、active-turn compaction transcript 都只保留opaque stub，不保存 delegation id、title、objective、result、events 或错误细节。
- AJ3：完全不记录 delegation 管理工具事件。

选择：AJ1。

理由：AE 已拍板主 agent 显式读取 delegation 的工具结果应正常进入 canonical transcript。turn journal 与 active compaction 如果继续替换成 opaque stub，会造成“模型当轮看到的正式工具结果”和“恢复/压缩后可追踪的内容”不一致。AJ1 保持 delegation 管理工具与普通工具一致：该截断就截断，该压缩就压缩，但不做 delegation 专属屏蔽。自动 runtime projection 仍然不写入canonical transcript、turn journal 或 active compaction。

### AK. restore best-effort 的扫描策略

选项：

- AK1：restore best-effort 复用 finalize 的 strict abandon；遇到坏 metadata 就整次失败并由上层吞掉错误。
- AK2：restore best-effort 使用非严格扫描：坏 metadata 只记录 warning，仍继续 abandon 其余可读 queued/running delegation。
- AK3：restore 时不处理 delegation，完全交给主 agent 后续读取。

选择：AK2。

理由：AD 已拍板 restore 不阻断主 session 恢复，但这不等于可以留下健康 delegation 继续显示为 running/queued。AK1 会被一个坏 YAML 短路，导致其他可读 delegation 没有进入 abandoned。AK3 又放弃了恢复收束。AK2 最符合 best-effort 的含义：尽最大努力收束可处理对象，对坏数据留日志而不阻塞恢复。

### AL. finalize 成功后的最后收束

选项：

- AL1：finalize 成功、session 已 marked finalized 后，再做一次 hard abandon；失败则让finalize 返回错误。
- AL2：所有 hard abandon 都必须发生在 session marked finalized 之前；成功关闭后只做best-effort 补扫。
- AL3：finalize 成功后不再检查 delegation。

选择：AL2。

理由：session 一旦 marked finalized/closed，就已经对外承诺本轮关闭成功；此后再让 hard abandon 失败会出现“文件已经关闭但 API 返回 finalize 失败”的矛盾状态。AL3 少一道防御。AL2让硬收束发生在写入 `Closed` 之前；如果失败则 session 保持 `Finalizing`，不关闭、不 resume。关闭后的补扫只用于处理理论上的迟到残留，不改变 finalize 成功语义。

### AM. delegation child 文件工具的 workspace 边界

选项：

- AM1：沿用主 agent 文件工具语义，允许相对路径和任意绝对路径，不叠加 delegation 专属workspace / protected roots 边界。
- AM2：delegation child 的 `file_read` / `file_write` / `file_patch` 只能触达 canonical workspace root 内路径，再叠加 ACN runtime protected roots 禁止；路径判断解析已存在祖先symlink。
- AM3：完全禁止 delegation child 文件工具。

选择：AM1。

理由：与 Z0 一致。`workspace_root` 是相对路径和默认 cwd 的基准，不是 sandbox。delegation文件工具先沿用主 agent 语义，后续如需更强隔离，再和权限审批、shadow workspace 或 artifact采纳流程一起设计。

### AN. delegation child 修改类文件工具的预读上限

选项：

- AN1：`file_patch` / `file_write append` / `file_write prepend` 先完整读取目标文件，再按输出上限截断。
- AN2：delegation child 在这些操作读取现有文件前，先按 child `file_read` 字节预算检查目标文件大小；超过预算直接失败，由主 agent 决定是否拆分或改用其他方案。
- AN3：为 append 实现纯流式追加，为 prepend/patch 引入临时流式重写算法。
- AN4：不做 delegation 专属修改类文件工具预读上限，沿用主 agent 文件修改语义。

选择：AN4。

理由：与 Z0 / AH5 一致。delegation child 不再维护和主 agent 分叉的 file_patch / append/prepend 策略；大文件编辑能力和内存上限后续作为通用工具 hardening 处理。

### AO. direct resume 与异常退出恢复入口

选项：

- AO1：`acn --resume <session_id>` 只允许 `Closed` session，`Open` session 一律早拒绝；picker 与 direct resume 使用同一条可恢复边界。
- AO2：direct resume 早期 preflight 只拒绝 wrong-agent 和 `Finalizing`；允许 `Closed` 与 crash-open `Open` session 进入 `SessionEngine::reopen_existing_session`，由 engine 统一执行 delegation restore best-effort abandon。
- AO3：把 delegation restore cleanup 挪到 CLI preflight，在构建 engine 前直接操作 store。

选择：AO1。

理由：当前 ACN 没有长生命周期 owner lease / heartbeat / stale 判定。允许 direct `--resume <session_id>` 打开 `Open` session，会留下绕过 picker 的并发占用口子：用户可以在另一个进程中显式恢复一个正在运行的 session，导致 transcript、metadata、delegation restore cleanup 产生竞态。delegation restore cleanup 仍保留在 engine 的 closed resume 路径里，确保正常恢复时 queued/running delegation 会被 best-effort 标成 `abandoned`。crash-open 恢复不在当前拍板内；如果以后要支持，需要先引入 active owner 记录、心跳、stale 判定与显式救援语义。按 T1 修订，finalized `Closed` session 也可以 resume；resume 会清掉 `finalized_at` / `closed_at`，但保留 `recapped_until` 和既有 claim/trace 结果。

### AP. append/prepend 读取既有文件失败时的语义

选项：

- AP1：`file_write append/prepend` 读取既有文件失败时一律当作空文件继续写入。
- AP2：只有目标文件不存在时按空文件处理；目标存在但读取失败、不可读或不是 UTF-8 时直接返回错误，不继续写入。
- AP3：对 delegation child 使用 AP2，主 agent 继续沿用 AP1。

选择：AP2。

理由：append/prepend 的语义是基于既有文本追加或前插。如果把非 UTF-8、小文件读取错误或权限错误吞成空字符串，就会把已有文件静默覆盖成新内容，尤其对子代理这种可后台修改 workspace的执行体风险更高。AP2 对主 agent 和 delegation child 保持一致的文件安全语义：只有`NotFound` 表示“从空文件开始”，其他读取错误必须失败并保留原文件。

### AQ. delegation web 工具 IP denylist 范围

选项：

- AQ1：只禁止 localhost、private、link-local、unspecified 与 IPv4 broadcast 地址。
- AQ2：在 AQ1 基础上同时禁止 multicast、documentation、benchmarking、shared、reserved 等所有非 global 地址段。
- AQ3：完全禁止 delegation web 工具，只保留主 agent web 工具。
- AQ4：不做 delegation 专属 IP denylist，web 工具沿用主 agent 语义。

选择：AQ4。

理由：本项被 Y1 覆盖。delegation child 当前可使用完整 web 工具，包括 localhost/private network 目标和 `web_request`。后续如果需要 SSRF hardening，应作为通用 web 工具或权限模型能力补充，而不是这次产品功能的默认限制。

### AR. 真实 LLM TUI smoke 验收批次

选项：

- AR1：只跑单元测试、集成测试和本地 fake provider TUI smoke。
- AR2：保留单元 / 集成 / fake provider 验证，同时新增真实 LLM TUI smoke suite，按多批次重复运行。
- AR3：真实 LLM 只做人工手动探索，不固化为脚本。

选择：AR2。

理由：delegation 是跨 turn、后台执行、TUI 展示、真实模型工具调用共同作用的功能，只靠fake provider 很难暴露模型是否会漏写 changed files、是否能理解 `/subagents` 面板语义、是否能正确使用`code_run`、`web_request` 和 MCP。AR3 不利于回归验证。新增`.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_delegation_real_llm_suite.sh`，使用真实 LLM provider 和隔离的临时 config / `acn_home` 重复运行以下批次：

- happy：创建 1 个 delegation，验证常驻提示、`/subagents` 只读面板、progress、result、changed files、文件写入和终态展示。
- boundary：验证 delegation 可读取 workspace 外绝对路径、可读取 selected upstream runtime `<acn_home>/<upstream>/.mcp.json`、可访问 localhost `web_request` GET、可执行 `code_run`、可调用本地 stdio MCP tool，并通过 `code_run` stdout 检查 `ACN_SUBAGENT_ID` / `ACN_PARENT_SESSION_ID` 身份env 已注入；相关结果有界落盘、TUI 显示易读终态。`web_request` 的 POST / body / header语义由工具层单元测试覆盖，真实 LLM smoke 不把所有 HTTP method 都跑一遍。
- lock：创建 7 个 delegation 写同一路径，验证并发上限、queued/running/completed 展示、path lock、列表表头对齐和无取消入口。
- diff：让 delegation 修改 fixture，再由主 agent 用 `code_run diff -u` 展示 diff，验证工具调用、文件路径、行数语义、diff 位置和原有消息渲染不被 delegation 面板破坏。

脚本默认重复 happy 3 次、boundary 2 次、lock 1 次、diff 1 次。每次运行都捕获关键阶段screen text / ANSI、检查 stderr 为空、检查 session store 中 metadata / events / progress / result 与 UI 展示一致。真实模型输出允许有轻微措辞差异，但验收断言必须围绕产品契约：状态、工具能力、落盘结果、UI 可读性、changed files 和上下文有界性，而不是要求模型逐字照抄。

### AS. finalize 关闭 create 窗口的顺序

选项：

- AS1：finalize 先 hard abandon，再把 parent session 标成 `Finalizing`。
- AS2：finalize 先通过 `session.lock` 把 parent session 标成 `Finalizing`，阻断新的`create_subagent`；随后 hard abandon 所有 unfinished delegation。若 abandon 失败，session 保持 `Finalizing`，等待 retry 或排障。
- AS3：`create_subagent` 不持有 parent `session.lock`，只在 runner/store 层自行处理finalize 竞态。

选择：AS2。

理由：AS1 在 `create_subagent` 正持有 `session.lock` 并创建 delegation 目录、但尚未写完metadata 时，strict abandon 可能看到半创建目录并失败，导致 finalize 被一个短暂中间态误伤。AS3 不能从源头阻断新 delegation。AS2 让 `mark_finalizing` 与 `create_subagent` 在同一把parent `session.lock` 上串行：先关闭新的 create 入口，再执行 hard abandon；如果 abandon发现真实问题，仍按 AF1 语义保持 `Finalizing`，不重新打开用户输入，也不允许新的 delegation继续进入这个正在关闭的 session。

### AT. delegation child 对 workspace secret 文件的读取边界

选项：

- AT1：只限制 workspace 外路径与 ACN runtime protected roots，不额外识别 secret 文件。
- AT2：在 AT1 基础上增加一组保守的 workspace secret 文件 / 目录 denylist，例如`export_env.sh`、`.env*`、`.netrc`、`.npmrc`、私钥后缀、`.ssh/`、`.aws/` 等。
- AT3：禁止 delegation child 读取任何 gitignored 文件。
- AT4：不启用 delegation 专属 workspace secret denylist。

选择：AT4。

理由：本项被 Z0 / AM1 覆盖。delegation child 文件工具先沿用主 agent 语义，不维护额外secret denylist。后续若项目引入正式权限模型，再把 secret 路径策略提升为配置化能力。

### AU. `read_subagent artifact` 的对外语义

选项：

- AU1：保留对外 artifact mode，即使当前 artifacts 目录没有可靠写入来源。
- AU2：实现 artifact materialization，把子代理报告的文件复制到 delegation artifacts 目录。
- AU3：对外不开放 artifact mode；result / progress 中的 workspace 路径由主 agent用普通 `file_read` 显式读取。

选择：AU3。

理由：当前 delegation child 的产物主要是 workspace 文件修改、progress/result 和事件日志；`read_subagent` 本身仍开放 `summary` / `result` / `events_tail`，但`delegations/<id>/artifacts/` 仍没有稳定 materialization 机制。AU1 会误导主 agent 调用一个大概率读不到内容的 artifact mode。AU2 会引入产物复制、路径归属、大小上限和敏感内容二次落盘等额外设计。AU3 保留主 agent 的显式读取责任：先通过 `read_subagent result` 拿到changed_files / artifacts 中报告的 workspace 路径，再用普通 `file_read` 分段读取实际文件。

### AV. events.jsonl 尾部修复成本

选项：

- AV1：每次 append 前整读 `events.jsonl`，修复缺换行或半截 JSONL。
- AV2：每次 append 前只扫描尾部，找到最后换行后修复最后一行；异常过长尾行直接截断。
- AV3：不修复 tail，读 events 时忽略尾部半截。

选择：AV2。

理由：AV1 语义简单，但长 delegation 高频写 event 时会退化成 O(n²)，还会产生不必要内存尖峰。AV3 会让下一次 append 把新事件接在半截 JSON 后面，污染后续读取。AV2 保留 crash-tail 修复语义，同时把常规成本限制在尾部窗口内；若最后一行超过修复窗口，按损坏尾行处理并截断。

### AW. 前台 / 降级 finalize 失败后的 TUI 状态

选项：

- AW1：前台 / 降级 finalize worker 返回错误时只恢复输入队列，不改 UI runtime status。
- AW2：前台 / 降级 finalize worker 返回错误时把 UI runtime status 恢复为 `Open`，并显示明确错误。
- AW3：前台 / 降级 finalize worker 返回错误时把 UI runtime status 置为 `Error`，输入继续不可用，并显示明确错误和排障 / retry 提示。
- AW4：finalize worker 返回错误时保持 `Finalizing`，输入继续不可用，并显示明确错误、job id和 supervisor/session log 查看或 retry 提示。

选择：AW3。

理由：正常 supervisor 后台 finalize 路径中，enqueue 成功后 TUI 已经退出；此后 supervisor job 失败不属于 AW，而由 BK 的 job retry / failed job 可见性处理。AW 只覆盖 TUI 仍活着的前台 finalize 或 supervisor enqueue 失败后的降级 finalize 路径。

在这些路径里，如果 finalize worker 已经返回错误，UI 继续显示 `Finalizing` 会让用户误以为后台仍在推进；恢复成 `Open` 又会和 AF / AS 的“不回滚 Open”语义冲突，并错误暗示用户还能继续输入这个正在关闭的 session。AW3 用 `Error` 明确表达“当前前台关闭失败，已经不是等待中”，输入不可用；用户通过错误信息、session event log 和 supervisor 状态定位问题。显式 retry / rescue当前版本不提供该入口，后续单独拍板。只有实际存在 job id 的 enqueue 场景才展示 job id，否则展示 finalize / enqueue 错误和排障提示。

### AX. 真实 LLM smoke 断言强度

选项：

- AX1：真实 LLM smoke 只断言脚本最终退出码。
- AX2：除退出码外，锚定真实 provider config、`/subagents` 面板头、只读标识、终态状态、delegation工具能力结果、diff 文本、并发队列和同路径写入 marker 次数。
- AX3：要求真实 LLM 每次逐字输出固定文本。

选择：AX2。

理由：AX1 容易被过宽正则或配置漂移掩盖问题。AX3 会把模型措辞差异变成无意义的 flaky。AX2 只钉住产品契约：是否接入真实 provider，UI 是否出现正确结构，`code_run` / `web_request` / 文件路径能力是否按拍板可用，同路径写入是否没有重复 append，diff 是否展示在主消息流中。

### AY. TUI delegation 入口与其他 panel 的统一修订

选项：

- AY1：保留 delegation 专属键盘快捷键。
- AY2：只保留 `/subagents`，并与 `/ps`、`/mcp` 共享即时命令和互斥 panel 语义。
- AY3：同时保留 slash command 与键盘快捷键。

选择：AY2。

理由：`/subagents` 更容易通过 slash 菜单与 `/help` 发现，也与现有管理面板入口一致。AY2保持一个时刻只有一个 live panel，三个面板统一占满 live region 可用高度；这修订并覆盖此前的 delegation 键盘快捷键设计。

### AZ. workspace secret denylist 的保守范围

选项：

- AZ1：AT2 只覆盖 `.env*`、`export_env.sh`、私钥和 `.ssh/.aws`。
- AZ2：在 AT2 基础上扩展 Git/Cargo/Docker/Kube/GCloud 等常见凭据位置，例如 `.git/`、`.git-credentials`、`.cargo/credentials.toml`、`.docker/config.json`、`.kube/config`、`.config/gcloud/`。
- AZ3：禁止 delegation child 读取所有隐藏文件和隐藏目录。
- AZ4：不启用 delegation 专属 workspace secret denylist。

选择：AZ4。

理由：本项被 Z0 / AM1 覆盖。delegation 文件工具当前沿用主 agent 语义，不维护额外 secret denylist。后续如果要补 secret hardening，需要和权限审批、日志脱敏、result 截断一起设计。

### BA. hard abandon 失败时 runner 内存一致性

选项：

- BA1：runner 先 abort / 清空 running 与 queued，再调用 store hard abandon。
- BA2：runner 持有 pump lock 后先让 store hard abandon；成功后清空内存，失败时只清理已经确认落盘为 terminal 的 delegation。
- BA3：hard abandon 失败时强制把所有内存任务都保留，无论磁盘状态是否已经 terminal。

选择：BA2。

理由：BA1 会在 store 因坏 metadata 或 I/O 失败时丢失内存任务，留下磁盘 `running/queued`假状态。BA3 又会让已经落盘 abandoned 的任务继续运行。BA2 让 store 成为状态真相来源：能落盘终态的任务就停止，未落盘成功的任务仍保持可恢复状态，并把失败上抛给 finalize。

### BB. delegation create 的半创建清理

选项：

- BB1：`delegation.yaml` 写完后如果 `events.jsonl` / `events.seq` 初始化失败，保留目录。
- BB2：create 过程中任何 metadata 后续 sidecar 初始化失败，都 best-effort 删除刚创建的delegation 目录并返回错误。
- BB3：改用 sqlite transaction 管理 metadata 与 events 初始化。

选择：BB2。

理由：BB1 会留下 runner 不知道、TUI 却能看到的永远 queued delegation。BB3 超出当前文件存储架构。BB2 保持文件存储形态，同时避免半创建对象进入 session delegation 列表。

### BC. 真实 LLM boundary smoke 的本地能力覆盖

选项：

- BC1：boundary 只验证普通 delegation 创建与终态展示。
- BC2：boundary 构造 workspace 外文件、selected upstream runtime `<acn_home>/<upstream>/.mcp.json`、localhost web server 和本地 stdio MCP server，验证delegation 可以按新拍板访问这些资源，且结果有界落盘、TUI 易读。
- BC3：只用单元测试覆盖 secret denylist，不放入真实 LLM TUI smoke。

选择：BC2。

理由：本次 boundary smoke 的职责从“验证拒绝”改为“验证新能力真的可用”。它应覆盖真实模型调用链路里的绝对路径文件读取、ACN runtime MCP 配置文件读取、localhost `web_request` GET、`code_run`、parent-visible MCP tool 调用和清晰终态展示，同时仍坚持输出有界，不把大段工具结果刷进主上下文。`web_request` 的非 GET method 由工具层测试保证；smoke 只验证真实模型能在 delegation 内触达该工具链路。

### BD. TUI delegation snapshot 的坏 metadata 处理

选项：

- BD1：TUI 使用宽松 list，跳过无法读取的 delegation metadata。
- BD2：TUI 使用 strict list；只要存在坏 metadata，就显示 delegation 状态不可用。
- BD3：TUI 同时展示可读 delegation，并额外展示坏 metadata 数量。

选择：BD2。

理由：finalize hard abandon 会把坏 metadata 作为失败暴露。如果 TUI 宽松跳过坏 metadata，用户会以为后台子代理状态正常或为空，直到 finalize 才突然失败。BD3 展示体验更细，但需要新的 partial snapshot schema。BD2 直接复用现有 snapshot error UI，让问题早暴露且实现面小。

### BE. 真实 LLM smoke 中 progress 的验收职责

选项：

- BE1：要求 `progress.json` 的 latest summary 逐字包含所有边界分类和原始错误。
- BE2：`progress.json` 只验证已经写入非空阶段性摘要；完整工具能力结果、路径、状态变化和终态摘要由 `events.jsonl` 与 `result.md` 严格验收。
- BE3：不检查 progress。

选择：BE2。

理由：真实模型完成时会把 latest progress 覆盖为最终摘要，且 store 会对长文本截断；它不是审计日志。BE1 会把模型措辞和截断差异变成 flaky。BE3 又无法证明运行中仍有阶段性进度落盘。BE2 与设计一致：progress 是最新阶段摘要，events/result 才承担完整诊断与采纳依据。

### BF. MCP 配置文件读取边界

选项：

- BF1：允许 delegation child 按主 agent 文件工具语义读取 selected upstream runtime 的`<acn_home>/<upstream>/.mcp.json`。
- BF2：把 `<acn_home>/<upstream>/.mcp.json` 归入 delegation 专属 secret denylist。
- BF3：只在 `<acn_home>/<upstream>/.mcp.json` 中出现 env 字段时阻止读取。

选择：BF1。

理由：当前 ACN 使用的 MCP 配置文件位于 selected upstream runtime 的`<acn_home>/<upstream>/.mcp.json`，暂不引入 workspace/project-local `.mcp.json` 语义。本项被Z0 / AM1 / P1 覆盖：delegation child 可以使用 parent visible MCP tools，也可以按主 agent文件工具语义读取该 runtime MCP 配置文件。

### BG. lock smoke 对 queued / running 的断言方式

选项：

- BG1：真实 LLM lock smoke 必须观察到 queued>=1，否则失败。
- BG2：queued 只做机会性观测；脚本持续采样 running 数不得超过 6，并严格验证最终 7 个 marker各出现一次、`/subagents` 列表和 changed files。
- BG3：完全删除 lock smoke，只保留单元测试。

选择：BG2。

理由：真实模型和子任务可能完成很快，queued 是瞬态 UI 状态，硬等待会制造 flaky。并发上限的确定性语义由 runner 单元测试覆盖；真实 LLM smoke 更适合验证端到端运行中没有超过 running上限、同路径写入串行、TUI 展示和最终文件结果正确。

### BH. boundary smoke 的假阳性防护

选项：

- BH1：把所有期望 marker 明文写进 prompt，让模型最终复述。
- BH2：marker 只写入本地文件、HTTP 响应、可执行脚本或 MCP server 返回值；prompt 只告诉delegation 去调用对应工具并报告“观察到的 token”。
- BH3：不检查 marker，只检查 events.jsonl 中出现过相关 tool name。

选择：BH2。

理由：BH1 可能在工具失败时靠模型复读通过；BH3 又只能证明模型发起过调用，不能证明工具结果被正确读取和落盘。BH2 让结果 token 只有通过 `file_read`、`web_request`、`code_run` 和 MCP调用后才能获得，再用 completed tool event/result 双重检查，能更严格地验证真实能力链路。

### BI. `code_run` 修改文件的 changed-files 归因

选项：

- BI1：只从 `file_write` / `file_patch` 的结构化工具结果自动提取 changed files；`code_run`修改文件时依赖 delegation final answer 的 `Changed files` section。
- BI2：每个 delegation 启动前后对 workspace 做快照或 git diff，自动推断 `code_run` 改动。
- BI3：禁止 delegation 通过 `code_run` 修改文件，只允许 file tools 修改。

选择：BI1。

理由：V1 已允许 child 使用高权限 `code_run`。自动 diff/snapshot 会引入新的性能成本、ignore规则、并发写归因和二进制文件边界，超出本轮需求；BI3 又和当前产品拍板冲突。暂时保持轻量：结构化 file tools 自动归因，shell 写入由 prompt 要求在 final answer 中列出，TUI 和主 agent 再按 result 采纳。后续如果 shell 写文件成为高频路径，再单独设计 workspace diff或 artifact 采纳机制。

### BJ. delegation summary projection 的注入位置

选项：

- BJ1：每次 provider request 前把 delegation summary projection 动态拼到 system prompt。
- BJ2：只在真实用户触发的 agent turn 前，把 delegation summary projection 作为 runtime synthetic user context 插入 provider messages；不拼 system prompt、不注入 tool_result 轮次、不为 `!pwd` 等本地 shell command 注入、不写 canonical transcript。
- BJ3：完全取消自动 projection，主 agent 只能通过 `list_subagents` / `read_subagent`获取子代理状态。

选择：BJ2。

理由：BJ1 虽然实现简单，但会让 session 创建后的 system prompt 前缀随 delegation 状态变化，影响 prompt cache、审计和长期一致性；delegation 运行状态也不是系统规则，语义上不应放在system prompt。BJ3 又会让主 agent 对后台任务完全失去低频态势感知。BJ2 把 projection 定位为运行时上下文：真实 user turn 切换时自动给主 agent 一份有界快照；同一 turn 内如果主 agent想知道最新结果，应显式调用 `list_subagents` / `read_subagent`，并让这些正式工具结果按AE/AJ 进入 canonical transcript。

### BK. Finalizing session 的 finalize job retry 语义

选项：

- BK1：supervisor 每次启动都自动重试所有 failed finalize job，直到成功。
- BK2：supervisor 自动重试 queued / requeued / stale running finalize job，并设置有限attempts；attempts 耗尽后 job 标为 failed，session 仍保持 `Finalizing`，用户通过`acn supervisor jobs` 和 session event log 定位问题；显式 retry / rescue 入口不在本轮实现范围，后续单独拍板。
- BK3：finalize job 失败后立即把 session 标成 `Closed`，只在 job 里记录失败。

选择：BK2。

理由：BK1 会在磁盘损坏、权限错误、坏 metadata 等不可自愈问题上无限循环，静默消耗模型和后台资源；BK3 会破坏 `Closed` 的产品契约。BK2 保持状态诚实：queued / stale running 这类可恢复后台任务由 supervisor 自动接续；真正失败且耗尽 attempts 后不再悄悄重试，而是让用户看到明确的 Finalizing 卡点和错误原因。当前实现范围只要求失败可见与状态诚实，不提供自动无限重试，也不提供用户级 retry/re-enqueue 命令。后续若要解卡，需要单独设计 retry / rescue / force-close 等运维入口；只有 finalize 真正成功后，session 才能从 `Finalizing` 进入 `Closed`，随后出现在 resume 边界内。

### BL. 公开层统一使用 subagent 命名

选项：

- BL1：维持现状，公开工具使用 `delegation`，TUI 和用户文案使用 `subagent`。
- BL2：只把工具名改成 `subagent`，返回字段、ID、配置和环境变量继续使用 `delegation`。
- BL3：公开层完整统一为 `subagent`；内部 Rust 领域模型和持久化目录继续使用`delegation`。
- BL4：连内部 Rust 类型和持久化目录也全部改成 `subagent`。

选择：BL3。

理由：ACN 会在 TUI 中直接展示 `Called <tool_name>`，所以主 agent 工具不仅是内部协议，也是用户可见产品界面。BL1 会让用户在 `Session Subagents` 与 `create_delegation` 之间来回切换；BL2 又会形成 `subagent.id = deleg_xxx` 这类半套协议。BL4 会把当前受限的 session 内委托任务误导成完整 ACN agent 身份，并造成无意义的大范围领域重命名。

公开契约统一为：

- 父工具：`create_subagent`、`list_subagents`、`read_subagent`、`steer_subagent`。
- 子代理进度工具：`update_subagent_progress`。
- JSON 包裹字段：`subagent` / `subagents`。
- 新 ID：`subagent_<8 位小写 hex>`；不接受旧 `deleg_` ID。
- 配置：`agent.session.subagents`；不接受旧 `agent.session.delegation`。
- code runner 身份变量：`ACN_SUBAGENT_ID`；不注入旧 `ACN_DELEGATION_ID`。
- 主/子模型 prompt、runtime projection、可见错误和 TUI 工具轨迹统一使用 `subagent`。

内部继续保留 `DelegationId`、`DelegationMetadata`、`DelegationRunner`、`DelegationStore`、`delegations/`、`delegation.yaml` 等实现和持久化命名。这里的内部命名不进入常规用户或模型协议，也不改变本 PRD 已拍板的生命周期、托管、权限、上下文、并发、收束和结果采纳语义。本次明确不兼容旧工具名、旧 ID、旧配置键或旧环境变量。
