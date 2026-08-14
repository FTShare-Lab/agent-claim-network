# PRD: `code_run` 后台长命令与受管终端会话

> 状态：已实现。本文保留受管进程、PTY、后台交付与 live panel 的产品决策。

本文记录 ACN `code_run` 后台长命令、PTY、交互式 stdin 与进程管理能力截至当前已经拍板的产品语义、现状问题和实现边界。

目标不是在现有一次性 `code_run` 外再拼接一套后台任务协议，而是把 `code_run` 改造成同时覆盖短命令和长生命周期终端会话的统一执行入口。

---

## 背景与当前问题

### 改造前的 `code_run` 是一次性前台执行器

改造前的主要流程为：

1. 按 `bash` / `python` / `powershell` 构造 `tokio::process::Command`。
2. 为 Unix 子进程创建独立进程组。
3. 管道捕获 stdout / stderr。
4. 在同一个 tool call 内等待 root process 退出。
5. 超时后 kill 整个进程组并返回 `CommandTimeout`。
6. root process 正常退出后仍清理同进程组残留后代，再返回 exit code 和截断输出。

该行为已经由测试明确约束：

- `code_run_timeout_kills_background_process_group`
- `code_run_cleans_background_process_group_after_parent_exits`

因此当前 `code_run` 的资源所有权是完整且安全的，但它无法表达“命令仍在运行，而启动操作已经成功完成”。

### 当前超时同时承担了两个不同概念

现有配置：

- `code_run_default_timeout_secs = 60`
- `code_run_max_timeout_secs = 180`

当前 `timeout` 同时被当作：

- tool call 最多等待多久；
- 子进程最多允许运行多久。

这导致长命令无法在不阻塞 tool call 的前提下继续运行。把超时值调大只能延长阻塞时间，不能提供后台会话。

### `nohup` 无法解决工具生命周期问题

`nohup` 只是 shell / OS 层的信号与输出处理工具，不会自动向 ACN 返回一个可管理的进程句柄。

当前非交互执行中使用：

```bash
nohup server >server.log 2>&1 &
```

父 shell 会快速退出；如果后台进程仍在父 shell 的进程组中，当前 `code_run` 会按既有资源清理语义将它一起杀掉。

交互式 shell 开启 job control 后，`nohup ... &` 也可能进入新的进程组并在 shell 退出后继续存活，但这种进程已经逃离原终端 session，不能继续通过原句柄读取状态、写 stdin 或停止。它属于普通 Unix 进程行为，不是 ACN 的受管后台任务。

### 当前 tool result 只能表达最终退出

`code_run` 当前只返回 `ToolExecutionOutcome::ProcessExit`。如果一直等待最终退出，agent 无法在命令运行期间继续执行其他工具；如果仅仅放开子进程又提前返回，则会失去输出、退出状态、取消和 shutdown 清理能力。

后台能力必须把“启动/首次等待完成”和“进程最终退出”拆成两个生命周期，同时保证每个模型`tool_use` 仍然只对应一个 `tool_result`。

---

## 目标

- 同一个 `code_run` 同时支持短命令和长命令。
- 长命令超过初始等待窗口后返回 `process_id`，原 tool call 正常闭合。
- agent 可以继续执行其他工作，之后按 `process_id` 观察、写入、软中断或硬终止其有权管理的进程。
- 支持 PTY、交互式 stdin、Ctrl-C 和长时间轮询。
- turn interrupt 不自动杀掉已经登记的后台终端。
- session / subagent / ACN 正常收束时清理其拥有的全部受管进程。
- 对输出、进程数量、等待时间和内存占用设置上限。
- 不要求后台进程跨 ACN 重启恢复。

---

## 非目标

- 不实现独立 `acn-execd` daemon。
- 不实现 ACN 重启后的进程重连、句柄恢复或输出续读。
- 不承诺管理调用 `setsid`、双重 fork 或其他方式主动逃离受管进程组的 daemon。
- 不把逃离 root terminal session 的任意后代进程重新发现并登记为`detached_running`。
- 不实现附着/进入某个后台终端的交互页面，也不实现运行中 PTY 动态 resize。PTY 只在创建时使用配置的初始行列数；ACN TUI 窗口变化只重排 ACN 自身界面，不改变子进程的逻辑终端尺寸。
- 不为本需求新增 sandbox、命令审批或权限分类；继续沿用当前 `code_run` 的高权限本地执行边界。
- 不改变 TUI 用户直接输入的 `!` shell command；其现有一次性非 PTY 语义继续由`docs/PRDs/PRD_shell_command.md` 定义。
- 暂时不支持 Windows，不实现 ConPTY、Job Object 或 Windows 后台进程树管理。

---

## 核心模型选择

后台命令建模为可持续交互的 terminal session：

- 同一个 session 支持 PTY、stdin、短暂 yield 和输出轮询。
- TUI/runtime 控制面提供进程列表与终止能力；模型侧只负责命令启动、持续输入和状态查询。
- 活进程必须在初始等待前登记到进程表，避免 turn interrupt 造成句柄丢失。
- turn interrupt 与后台终端清理是两个独立生命周期。
- 本需求不增加 sandbox approval、network proxy 或 remote exec-server。

---

## 已拍板决策

### D1. 改造现有 `code_run`，不新增第二套命令启动工具

`code_run` 继续作为模型执行本地命令或脚本的唯一启动入口。

不新增 `code_start` / `background_run` 等重复工具。cwd、脚本类型、环境变量、delegation 身份、权限边界和工具审计继续复用 `code_run`。

### D2. 采用有界 yield，不增加 `background: true`

`code_run` 新增 `yield_time_ms`：

- 命令在 yield 窗口内退出：返回最终 exit code，不返回 `process_id`。
- yield 到期时仍运行：返回当前输出和 `process_id`，原 tool call 完成。
- 需要尽快后台化时，模型传入最小 `yield-time_ms`。

默认与边界：

- 默认 `10_000ms`
- 最小 `250ms`
- 最大 `30_000ms`

这些值必须通过 config 下发，不在业务源码中散落硬编码。

### D3. 删除进程运行时 `timeout`，只保留 yield

`yield_time_ms` 只控制当前工具调用等待多久，不限制进程寿命。新的 `code_run` schema 不再提供进程运行时 `timeout`：

- 删除现有 `code_run_default_timeout_secs` / `code_run_max_timeout_secs`。
- 不新增全局 `max_runtime_secs`。
- 进程运行到自然退出、`write_stdin` 发送 Ctrl-C、`write_stdin(terminate=true)` 硬终止、模型通过普通 shell 命令停止 OS 进程、runtime 显式终止、LRU 淘汰 live entry，或 owner 生命周期结束。
- 长驻 server、watcher、REPL 等在 owner session 内无限运行是正式支持的行为。

资源安全由进程数量上限、单进程输出上限、显式管理入口和 owner shutdown 清理共同保证，而不是由粗粒度运行时限保证。ACN 被强制杀死或异常崩溃时不承诺完成清理。

### D4. `code_run` 返回统一进程协议

运行中返回示例：

```json
{
  "process_id": "8f31ab20",
  "state": "running",
  "stdin_open": true,
  "tty": true,
  "exit_code": null,
  "stdout": "...",
  "stderr": "",
  "chunk_id": "32ac91",
  "wall_time_ms": 10002,
  "truncated": false,
  "omitted_bytes": 0
}
```

完整终态结果返回时：

- 不再返回 live `process_id`。
- 如果终态输出因单次工具回传上限而只交付了一部分，仍返回可用于继续分页读取的`process_id`；最后一页成功交付 provider 后再移除终态 entry。
- 返回 `exit_code` 和最终状态。
- PTY 模式的 stdout / stderr 天然合流，统一放入 `stdout`，`stderr` 为空，并通过`tty: true` 明示。
- pipe 模式继续区分 stdout / stderr。

外部 ID 使用 ACN 既有 8 位 hex 随机 ID 工具。结构化进程工具只返回和接受 ACN `process_id`，OS PID 不作为 ProcessManager 的控制标识：一个 entry 表示 root process、PTY 和受管进程组组成的终端 session，OS PID 会复用，也不能独立表达 owner 权限和整个 session 的所有权。该约束不是 PID保密边界；模型仍可通过普通 shell 执行 `ps`、`echo $$` 等命令做 OS 级诊断。

随机工具只负责生成候选，唯一性由 root-session `ProcessManager` 保证：每次登记时必须在同一临界区内跨全部 owner 分区检查候选并插入；候选与任一仍保留的 live 或 exited entry 冲突时立即重抽，不能先检查、释放锁后再插入。重抽复用 `[agent.session].id_mint_max_retries`，默认最多重抽3 次、连同首次候选共尝试 4 次；全部冲突则 `code_run` 返回 ID allocation error。若此时子进程已经 spawn，局部 kill guard 负责清理，不得留下未登记进程。entry 从 ProcessManager 移除后无需为防止未来复用而保留无界 tombstone；本需求只保证同一 root session 当前仍保留的 entry 之间不重复。

### D5. 新增 `write_stdin`

工具输入：

```json
{
  "process_id": "8f31ab20",
  "chars": "yes\n",
  "terminate": false,
  "yield_time_ms": 250,
  "max_output_chars": 1048576
}
```

语义：

- `chars` 非空：向活跃 PTY 写入，并短暂等待响应输出。
- `chars` 为空：不写入，只轮询自上次读取后的新增输出。
- `\u0003`：发送 Ctrl-C 软中断。PTY 下它作用于当前前台进程组；它可能中断当前命令，但不会关闭仍在运行的交互 shell 或 SSH session。pipe 下由 ProcessManager 请求 SIGINT。
- `\u0004`：PTY 下发送 Ctrl-D。
- `terminate=true`：复用 `/ps` 的 ProcessManager 管理路径，对调用方有权管理的完整受管进程组发送 SIGKILL。subagent 只能终止自己的 entry；main 还可终止同一 Agent、同一 root session 中的 subagent entry。它不能与非空 `chars` 同时使用。终止完成后返回成功的 `process_terminated` outcome 和信号；输出内的 `success=false` 仍表示子进程不是自然零码退出，不得据此把终止操作解释为失败。
- `tty = false` 时只允许空轮询、Ctrl-C interrupt 或 `terminate=true`；其他输入返回 stdin 不可用错误。
- 子 agent 只能访问自己 owner 的 entry。主 agent 对同一 root session 的子 agent entry 可以空轮询、发送精确的 `\u0003` 或使用 `terminate=true`；不能写入文本、Ctrl-D、Esc、Ctrl-Z 或其他控制序列。对子 agent entry 的读取是观察操作，不推进其 owner 的 output-delivery cursor；主 agent 如需增量读取，使用返回的`stdout_cursor` / `stderr_cursor` 成对继续轮询。
- 返回的 `state` 描述 ACN 管理的外层进程。交互 shell 或 SSH session 仍为`running`，不代表它最近执行的内部命令仍在运行。
- `max_output_chars` 通常省略，默认每个 stdout/stderr stream 最多返回 `1048576` 个字符。连续输出超过本次上限时只返回可见前缀，并把 cursor 指向该前缀末尾；包含该 tool result 的 provider request 成功后，下一次隐式轮询从新 cursor 继续。provider 失败或 turn 取消则不提交该页，后续可重新取得相同前缀。
- 如果 retained buffer 已经因容量上限形成 head/tail gap，则先按当前单次上限分页交付 retained head；随后用一个携带 `truncated` / `omitted_bytes` 的空页面把 cursor 推进到 tail 起点，明确确认中间内容已不可恢复；最后继续分页交付 retained tail。每一步都等待 provider 成功确认，不要求突破用户配置的单次回传上限。
- 同一进程同一时刻只允许一页输出等待 provider 确认。同一条模型回复（同一批 `tool_use`）内若重复调用同一进程的 `write_stdin`，无论是否显式传入 cursor、是否读到新输出，后续调用都在产生输入、interrupt 或 terminate 副作用前明确拒绝；不能用后一页覆盖尚未确认的前一页。不同 `process_id` 的调用不受该去重规则影响。
- 每次 `write_stdin` 都是新的 tool call，并产生自己的唯一 tool result。

非空写入默认短等待；空轮询允许更长的等待窗口：

- 非空写入默认 `250ms`，最大 `30_000ms`。
- 空轮询默认 `5_000ms`，最大 `300_000ms`。

### D6. 模型工具面固定为 `code_run`、`write_stdin`、`process_list`

模型侧不提供 `process_signal`、`process_stop` 或 `process_resize`；runtime 同样不实现`process_resize`。除启动和交互工具外，只增加一个只读恢复入口：

- `process_list`：列出当前 agent 可见的 live process，帮助模型在上下文压缩或较长对话后恢复内部`process_id`。主 agent 看到本 root session 的 main 与全部直接子 agent entry；子 agent 只看到自己的 entry。
- 结果至少包含 `process_id`、`owner`、`command`、`cwd`、`tty`、`state`、`started_at`。`owner` 对main 显示 `main`，对子 agent 显示其 `subagent_id`。
- 不暴露 OS PID，不读取或消费输出，不列出已退出 entry；主 agent 的可见范围严格限于同一`owner_agent_id + root_session_id`，子 agent 不能查看 parent、sibling、其他 Agent 或其他 session。

Ctrl-C 只用于软中断当前前台任务；关闭交互 shell/SSH 应发送其正常退出命令。模型需要终止异常受管进程时，使用现有`write_stdin(terminate=true)`，不新增 `process_signal` / `process_stop` 工具。主 agent 可以按`process_list` 返回的 ACN `process_id` 对同一 root session 的子 agent 进程发送 Ctrl-C 或硬终止，但仍不能跨 owner 写入任意文本。ProcessManager 内部继续提供 list、terminate one、terminate all 和 PTY进程组清理，供 `write_stdin(terminate=true)`、owner shutdown、容量淘汰以及 TUI/runtime 管理面复用。

### D7. PTY 与 pipe 双后端

`code_run` 新增 `tty`，默认 `false`：

- `tty = false`：普通 pipe，适合构建、测试、脚本和结构化 stdout / stderr。
- `tty = true`：分配 PTY，支持交互式 shell、REPL、提示输入和控制字符。

PTY 层要求：

- 使用 `portable-pty`。
- 阻塞 reader / wait 放入 `spawn_blocking`。
- writer 通过有界 async channel 串行写入。
- Unix child 成为新的 session / process group leader。
- interrupt、runtime terminate 和容量/shutdown 清理作用于受管进程组。
- manager drop / owner shutdown 时清理进程树。
- PTY 创建时使用配置的 `pty_rows` / `pty_cols` 作为固定逻辑尺寸，运行期间不动态 resize。
- ACN TUI 的 resize 事件只触发聊天、`/ps`、`/mcp` 和确认页面重新布局，不向 PTY 传播。

### D8. root terminal process 是受管 session 的所有权边界

ACN 管理的是 `code_run` 启动并登记的 root terminal process 及其受管进程组，不重新发现已经逃离该边界的任意后代。

root process 退出后，受管 terminal session 进入终态；同一受管进程组内的残留后代按既有资源清理语义终止。

不实现“root 已退出但 PGID 仍有成员就继续登记”的 `detached_running` 扩展。

### D9. 允许 `nohup`，但后台管理由 `process_id` 提供

不禁止、不重写、不按命令文本特殊拦截 `nohup`、`&` 或 shell job control。

推荐的受管长命令：

```bash
nohup ./server >server.log 2>&1
```

或直接：

```bash
./server
```

命令超过 yield 后由 ACN 返回 `process_id`，无需依赖末尾 `&`。

以下命令语法允许执行，但不承诺在父 shell 退出后仍是 ACN 受管 session：

```bash
nohup ./server >server.log 2>&1 &
```

具体边界：

- 非交互 shell 的后台 job 若仍在受管 PGID，会随 root session 清理。
- 交互 shell job control、`setsid` 或 daemon 自身 fork 可能让后代逃离受管 PGID。
- 逃离后的进程可能按 OS 语义继续存活，但 `process_list`、`write_stdin`、状态查询和 runtime管理接口不再保证可用。
- ACN 不把这种逃离行为宣传为正式后台能力。

### D10. 工具调用闭合与进程生命周期分离

流程：

```text
code_run tool_use
  -> spawn process
  -> 在初始 yield 前登记 ProcessManager
  -> 进程仍运行时返回 process_id
  -> 原 tool_result 闭合
  -> watcher 继续读取输出和观察退出
  -> 后续 write_stdin / process_list 使用新的 tool_use
```

禁止在进程最终退出后向原 `tool_use_id` 再补第二个 tool result。

`ToolExecutionOutcome` 新增运行中语义，例如 `ProcessRunning`；运行中结果不能伪装成`ProcessExit`，也不能要求事件/TUI 从任意 JSON `status` 字段反推执行语义。

### D11. ProcessManager 先登记、后等待

活进程必须在初始 yield 前进入 process store，再由 `code_run` 等待输出或退出。

这样即使 turn 被 interrupt、tool future 被取消，最后一个进程句柄也仍由 manager 持有。

进程启动与登记之间需要显式所有权交接：

- 登记成功前由局部 kill guard 负责清理。
- 登记成功后把清理责任转交给 manager。
- 任一步失败都不能留下无主子进程。

### D12. turn interrupt 不杀后台 terminal

- 尚未登记成功的 foreground spawn 随 tool future 取消清理。
- 已登记且已经成为后台 terminal 的进程不随当前 turn interrupt 自动终止。
- `write_stdin("\u0003")` 可以请求当前前台进程组软中断；`write_stdin(terminate=true)` 可以硬终止调用方有权管理的完整受管进程组，runtime 仍可强制终止单个或全部受管进程。
- owner session finalize / close、subagent 收束和 ACN 正常 shutdown 会清理对应进程。

不支持跨 ACN 重启，因此 runtime 异常退出后的重连和状态恢复不在范围内。

### D13. 输出采用有界 head/tail buffer

- manager 持续 drain stdout / stderr，不能让子进程因 pipe 堵塞而挂起。
- 单个进程默认最多在内存保留 `1MiB` 输出。
- 超限后保留稳定 head 和最新 tail，中间内容丢弃并记录 `omitted_bytes`。
- 每次 `code_run` / `write_stdin` 返回本次收集到的有界输出和 `chunk_id`。
- 单次 tool result 的字符上限与 manager 的 retained buffer 上限是两个边界。连续输出即使超过单次回传量，也按 provider 确认成功的页逐步推进 cursor；不能在模型尚未收到某页时跳过，也不能在某页成功交付后无限重放。
- 不自动为所有命令保存无上限完整日志；需要完整日志时由命令显式重定向到文件，再使用`file_read` 或 shell 工具读取。
- 高频 output delta 可以投影到 TUI，但 journal 不应逐字节持久化无界事件。

### D14. 进程数量与清理上限

初始上限：

- 每个逻辑 owner 最多登记 `64` 个 process entry；main 和每个 subagent 分别计算，不设置root-session 聚合总上限。
- 每个 owner 分别保护其最近使用的 `8` 个 entry，无论它们仍在运行还是已经退出。
- owner 达到上限时只在自己的分区内优先清理已退出 entry，再按 LRU 清理未受保护 entry。
- 淘汰 live entry 时必须终止其受管进程树。
- 一个 owner 的容量压力不得淘汰或终止其他 owner 的 entry。
- owner shutdown 时 drain store，再逐个终止进程；不能持有全局 store mutex 跨异步终止。

数量、buffer 和等待上限统一通过 config 提供。

### D15. 后台完成使用独立事件，不复用工具完成事件

新增或等价表达以下领域事件：

- `BackgroundProcessStarted`
- `BackgroundProcessOutput`
- `BackgroundProcessStateChanged`
- `BackgroundProcessCompleted`

所有事件都必须携带 `process_id` 和完整 `ProcessOwner`，使模型侧按 owner 过滤、TUI 按 root session 聚合，并避免不同 subagent 的事件相互污染。

原 `code_run` 在返回 `process_id` 时仍产生自己的 `ToolCallCompleted`，其 outcome 为运行中。后台进程最终完成是独立生命周期事件。

main-owned completion 同时保留创建进程的原始 `turn_id + tool_use_id`，但仍不得伪造第二个工具结果。该终态作为 durable obligation 保留到写入对应 turn journal 并确认，不能按 TUI fanout 容量淘汰；新 turn 在冻结 recovery context 前也必须先完成这一步，因此正常 `ProcessRunning` 与强制中断后继续运行的进程都能在 recovery 中携带后续终态。TUI 每秒独立抽取 watcher 事件，并用组合键把原 `code_run` cell 的 `Process running in background` 展示投影替换为 exit code 或 signal 终态：活动 turn 的虚线框走普通重绘，已提交到终端 scrollback 的历史 cell 走完整历史 reflow；`/exit` 收束 live process 时也必须在退出前发送相同事件并完成 reflow。completion 不在 transcript 末尾追加独立的 `Background process ID=...` 通知行；找不到原 tool cell 时保持静默。subagent-owned completion 不改写 main transcript 中不存在的 child tool cell，也不向 main transcript 插入通知行。finalize recap 额外接收有界的 main completion 投影，避免 canonical transcript 中旧的 running tool result 掩盖实际 exit / signal；该投影同时进入 finalize checkpoint hash 与 trace 证据。

事件至少服务于：

- TUI 状态更新；
- turn journal / session 恢复诊断；
- 下一次模型调用时的有界状态通知。

进程完成只写入事件、TUI 状态和有界 pending notification，不主动创建 scheduled/system turn，也不为已经结束的 turn 发起新的模型请求。通知只在当前模型循环的下一个安全边界或下一次真实用户 turn 中注入。

### D16. 与现有 `code_run` 权限和 delegation 上下文保持一致

- 继续使用 workspace root / cwd 解析逻辑。
- 继续支持 `bash` / `python` / `powershell`。
- 继续注入现有 delegation 身份环境变量。
- `write_stdin` 和 `process_list` 沿用 `code_run` 的访问 profile，不向 memory review 等无本地执行权限的 profile 暴露。
- 本需求不顺手引入命令 sandbox 或审批流程。

### D17. `/ps` 使用可交互进程面板，`/mcp` 同步补齐列表滚动

TUI 新增且只新增 `/ps` 进程管理入口，不新增 `/stop` slash command。`/ps` 不写 transcript，而是在当前 TUI live region 中打开一个与 `/mcp` 风格一致的列表面板：

- `↑` / `↓` 移动光标；列表超过可用高度时维护 `list_offset`，viewport 自动跟随当前选择。
- `Esc` 退出面板。
- 光标位于 `running` process 时，按 `t` / `T` 进入 `/ps` 内部的 `TerminateConfirm` 全页面子视图；光标位于 `terminating` 行时按 `t` / `T` 不响应，不重复发 terminate。
- 确认视图完全替换进程列表，不渲染列表背景，也不做 overlay、半透明或局部挖空；agent、subagent 和后台进程仍在后台继续运行，process snapshot 继续更新。
- 确认视图固定显示标题、`Process ID`、`Owner`、带状态色的 `Status`、`Started` 和 `Elapsed`。
- `Command:` 单独占一行，其后直接复用 TUI 现有 code-block renderer，按原始 `code_run.type`标注 `bash` / `python` / `powershell`。必须使用 entry 保存的完整 command，不做 head/tail、字符数或行数截断；超宽内容只做视觉换行，不丢字符。
- 页面布局分为“固定 header/metadata、可滚动 Command body、固定 footer”三段。Command 按当前宽度重新 wrap，只滚动 body viewport；`↑` / `↓` 每次滚动一条视觉行。
- footer `[y] Yes                         [n/Esc] No` 始终占据页面最后一行，不参与滚动。
- 确认视图只响应 `↑`、`↓`、`y` / `Y`、`n` / `N` 和 `Esc`：`y` 确认，`n` 与 `Esc` 取消；其他按键全部忽略。
- 确认状态必须保存目标 `process_id`，不能只保存行号。若确认前目标已经退出，返回列表并显示`already exited`，不得误操作刷新后占据同一行的其他进程。
- 确认后异步硬终止整个受管进程组，行暂时显示 `terminating`；完成后回到列表并刷新内容。
- `n` / `Esc` 取消时同样回到列表并刷新内容，不执行 terminate。
- 刷新时按 `process_id` 保持选中对象；目标消失后优先选择原位置的下一行，没有下一行时选择上一行。

列表只聚合当前 root session 内 main 与全部 subagent 的 live entry，不显示 `error`、`finished` 或其他终态 entry。可见状态固定为 `running` / `terminating`，排序键依次为状态优先级`running → terminating`、`started_at` 倒序、`process_id` 升序；即所有 running 行在前，同状态下开始时间越近越靠前，同一开始时间再按 ID 保证稳定顺序。每个 entry 固定占三条视觉行；宽屏第一行的主表列为：

```text
PROCESS ID | OWNER | STATUS | TTY | STARTED | ELAPSED
› 91759f8b   subagent_f22e… running  no    07-20 16:15  1m0s
    cwd: /Users/example/agent-claim-network
    command: printf 'ACN_ALPHA_INITIAL\n'; sleep 600
```

- `OWNER` 对 main 显示 `main`，对子代理显示其 `subagent_id`；真正的 ACN `AgentId` 与当前`SessionId` 显示在面板标题，不在每行重复。
- `/ps` 列表与 `TerminateConfirm` 页面必须共用同一个 process status style helper：`running`使用绿色粗体（对应 `/mcp` 的 ready），`terminating` 使用黄色粗体（对应 `/mcp` 的 starting）。即使当前行被选中，`STATUS` 单元格也不能被蓝色 selection style 覆盖；确认页的 `Status` 值使用完全相同的颜色映射。
- `STARTED` 使用 TUI 所在机器的本地时区，精确到分钟。
- `ELAPSED` 精确到秒，从最大时间单位开始连续展示，最多显示三个单位：不足一分钟使用 `10s`，不足一小时使用 `12m34s`，不足一天使用 `12h34m56s`；达到一天后使用 `2d8h50m`，秒作为第四个单位省略。已进入展示范围的低位单位即使为零也保留，例如 `1m0s`、`1h0m0s`、`1d0h0m`。
- `cwd:` 与 `command:` 是所属 entry 的第二、第三行，采用相对主行内容额外缩进两格的 muted 灰色detail style；两者各自只渲染一条 preview，到可用宽度后截断，不在列表中换行。完整多行 command只在 `TerminateConfirm` 页面以现有 code renderer 展开。
- 选中态覆盖一个 entry 的全部三行，但 `STATUS` 单元格仍保留其 running/terminating 状态色；`↑` / `↓`、`list_offset` 与 viewport 均按 process entry（而非 detail 行）移动和计算。
- 进入主表本身不能容纳的 compact 模式时，隐藏整条 `cwd:` detail 行，第二行改为 `owner:`，第三行仍为 `command:`，保持每个 entry 固定三行；此模式同时隐藏 `TTY`。`PROCESS ID`、`OWNER`、`STATUS`、`STARTED` 和 `ELAPSED` 必须保留。
- 只要 root session 存在 live entry，普通 TUI 底栏在 subagent 摘要的上一行显示`Processes: {running} running · {terminating} terminating · /ps`；省略数量为零的状态段，数量必须与 `/ps` 当前行数一致。该行只代表受管进程，不与 subagent 数相加。
- subagent 状态固定为一行 `Subagents: {status summary} · {latest terminal notice} · /subagents`：最新`subagent 'title' completed/failed/abandoned` notice 内联在该行，不再额外占一行。两者同时存在时`Processes:` 在上、`Subagents:` 在下；其他通用 status notice 仍排在它们之后。
- 当完整状态行任一条放不进当前可用宽度（或宽度小于 64 cells）时，进入紧凑布局：进程状态使用`run` / `stopping`，subagent 计数使用 `done` / `failed` / `run` / `queued`；最新 subagent 终态notice 作为紧随计数行的 `↳ ...` 独立语义行，而不是让整条状态在单词中间硬折行。进程行仍在subagent 行之前；若 notice 自身仍超宽，才由通用 grapheme wrap 继续折行。
- 为保持底栏与 `/ps` 一致，process snapshot 每秒刷新，即使 `/ps` 面板当前关闭。TUI 同时保持1 秒一次的低频 heartbeat 重绘；即使 snapshot 内容和状态没有变化，打开的 `/ps` 也必须持续更新精确到秒的 `ELAPSED`。该 heartbeat 与 turn 动画的高频 tick 分离，重绘请求统一合并并串行执行。
- `/ps` 与 `/mcp` 的 live panel 补齐到 live region 可用高度，panel 内帮助行固定在全局 footer上方；与 `/subagents` 保持一致，避免三个管理视图出现不同的覆盖范围。

`/mcp` server/tool 列表同步补齐同样的 `list_offset` 与“选择始终位于可见 viewport”语义，但不改变其现有操作键和视图层级。

`/mcp` 与 `/ps` 都允许在 agent turn 运行期间立即打开，不进入 queued input。面板打开期间 agent、subagent 和后台进程继续运行，事件继续更新底层状态；面板关闭后恢复普通 TUI live region。terminate 与模型侧 `write_stdin` 并发时，由 ProcessManager 保证竞态安全。

active turn 中的 `/mcp` 保留全部现有操作键，不降级为只读面板。用户主动执行 Reconnect 或 Disable属于明确的连接级管理操作：允许它取消旧共享 client 上该 server 的全部 in-flight MCP request，包括 main 与各 subagent 正在执行的调用；这些调用以明确的取消或连接切换错误收束，不自动重试。该行为不取消当前 turn：失败调用作为 `dispatch_failure` tool result 回灌模型，当前 turn 继续推进。尚未派发的同 server 调用按派发时的新 generation 状态决定成功或失败。该行为不影响其他 MCP server，也不终止任何 `code_run` 受管进程。

### D18. subagent 保持 owner 隔离；main 是 root-session 模型监督者

每个 subagent 与 main 一样获得以下模型工具：

```text
code_run
write_stdin
process_list
```

模型工具始终绑定一个逻辑 `ProcessOwner`：

- main owner：`owner_agent_id + session_id + main`。
- subagent owner：`owner_agent_id + parent_session_id + subagent_id`。
- main 的 `process_list` 返回同一 `owner_agent_id + root_session_id` 下 main 与全部直接 subagent 的live entry，并在每一项中返回 `owner`，让用户面对的 main 能回答当前后台任务及其归属。
- subagent 的 `process_list` 只返回该 subagent-owned live entry。
- subagent 的 `write_stdin` 只能操作自己的 entry；不能访问 parent 或 sibling。
- main 对自己的 entry 保留完整既有 `write_stdin` 语义。对同一 root session 的 subagent entry，可以空轮询、发送精确 Ctrl-C (`\u0003`) 或使用 `terminate=true`；不允许文本、Ctrl-D、Esc、Ctrl-Z 或其他控制序列。跨 owner 读取不推进 subagent 的 output-delivery cursor，避免 main 的观察吞掉 child 后续应读取的输出。
- main 的跨 owner Ctrl-C 是受管 session 的软中断；`terminate=true` 是 SIGINT 被忽略或无法完整收束进程组时的硬终止兜底。它只适用于同一 Agent、同一 root session 内的 live entry，不改变 subagent owner、输出交付或生命周期归属。模型工具面不新增 `process_signal` / `process_stop`。

用户 TUI 同样是 root session 的控制面。`/ps` 聚合当前 root session 中 main 与全部 subagent 的live entry，用户可以选择并 terminate 任意一行；它不显示其他历史 session 或其他 ACN Agent 的进程。

用户 terminate subagent-owned process 时只终止被选中的受管进程组，不取消、不 abandon 对应subagent。subagent 如果正在等待或随后访问该进程，会得到终止后的状态或 unknown process结果，并自行继续当前任务。

subagent 到达 completed / failed / abandoned、执行 future 被取消，或 parent session 收束时，必须清理该 subagent 的全部 owned process，不向 main 转交。main 的可见性与受限 interrupt 权不改变子 agent 的 owner、容量、delivery cursor 或生命周期归属。实现上使用 root-session `ProcessManager` 维护按 owner 分区的 store，再向每个模型工具注册表提供不同权限的 handle；不能让 parent 和各 subagent 维护 TUI 无法聚合的孤立 store。

`process_id` 在一个 root-session ProcessManager 内全局唯一。TUI terminate 以 `process_id` 定位 entry，确认状态同时保存 owner 信息用于展示和竞态校验。

### D19. steer 保持现有 tool-boundary 收束语义

用户在 turn 运行期间通过现有 steer 交互提交纯文本时，继续复用当前`request_tool_boundary_steer` 流程，本需求不改变它的调度语义：

- steer 先 durable 记录，再关闭当前 turn 的 tool-dispatch gate。
- 尚未实际派发的 tool call 以 `turn_interrupted_before_dispatch` 跳过，不再启动。
- 已经发出 `ToolCallStarted` 的调用继续等待真实终态；支持协作取消的工具可以`ToolCallInterrupted` 收束，其他工具允许正常 `ToolCallCompleted`，不能伪造终态。
- 当前安全边界是全部已启动 tool call 都产生终态；后台进程的最终退出不属于该边界。
- 已经返回 `ProcessRunning` / `process_id` 的 `code_run` tool call 已经闭合，其后台进程不参与steer 等待，也不因 steer 被终止。
- 仍处于 initial yield、尚未返回 tool result 的 `code_run` 仍是当前活跃 tool call；steer 等待它自然退出或 yield 到期并完成本次 tool call，然后才到达安全边界。
- 安全边界到达后，旧 turn 以 `InterruptedByUser` 收束，不构造不完整批次的 tool result，也不继续旧 turn 的 provider 回环。
- TUI 随后把已经保存的 steer 文本自动提交为一个新的真实用户 turn；它不作为当前 turn 内的待处理输入，也不在旧 turn 内追加一轮 provider 请求。
- 多次 pending steer 沿用当前合并顺序；若用户随后显式 cancel，则恢复这些 steer draft，不把它们误提交为新 turn。

因此 steer 的核心调度、journal 和 TUI 转交流程无需因后台 shell 重写；实现只需保证新的`ProcessRunning` outcome 和后台领域事件不会重新把进程寿命纳入 tool boundary，并补充回归测试。

### D20. Esc / Ctrl-C 采用显式取消，已登记进程转为受管后台进程

用户在 turn 运行期间按 Esc / Ctrl-C 时，不再无限等待全部活跃工具自然结束，而采用显式取消语义；该路径与 D19 的 steer 严格区分：

- 立即取消当前 provider 请求，并关闭 tool-dispatch gate；尚未派发的调用保持 skipped。
- 已启动的普通工具先收到协作取消信号，最多给予 `100ms` grace period；仍未收束的工具执行future 随后强制 abort，TUI 不再被不响应取消的工具长期阻塞。
- 强制 abort 只停止尚未完成的本地执行和等待，不回滚工具已经产生的文件、网络或其他外部副作用。需要资源 teardown 的工具必须实现自身的有界清理协议。
- abort 后，当前 async tool future 停止被 poll 并被 drop，不能再产生 tool result；但它此前启动的 OS 进程、独立异步任务、线程或已经发出的远端请求可能继续执行。继续执行不等于无人管理：每个仍在运行的本地资源都必须已经把所有权转交给 ProcessManager、subagent runtime 等明确 owner，或者由 cancel/drop guard 清理。无人持有和无法发现的本地 detached work 属于实现缺陷，不是允许的取消结果。
- 已经到达外部系统的请求或副作用只提供 best-effort cancellation；本地 future 被 abort 不代表远端操作已停止。协议支持远端取消时应主动传播，但不提供事务回滚保证。
- MCP tool 必须继续使用 `docs/PRDs/PRD_shared_mcp.md` 已实现的 request-scoped cancellation：协作取消先取消当前 request；若在 `100ms` 后强制 abort caller future，其 drop guard 仍只取消对应 request；对 Streamable HTTP 先中止对应 HTTP future，再尽力发送 MCP `notifications/cancelled`。这两条路径都不能调用共享 `McpClient::shutdown()`、不能把 server 标成 failed，也不能影响同一常驻 client上其他 agent 的 in-flight request 或后续调用。远端 MCP 副作用是否已经停止仍按上一条的best-effort 语义处理。
- `code_run` 在 spawn 后、ProcessManager 登记前由本地 guard 持有；此阶段取消必须清理已经生成但尚未登记的进程，不留下 manager 无法追踪的孤儿进程。
- ProcessManager 登记是进程所有权转移的线性化点。登记成功后，即使 `code_run` 仍处于 initial yield，显式取消也立即放弃本次 tool call 的等待，但不终止进程；进程继续由 manager 管理，可以通过 `process_list`、`write_stdin` 和用户 `/ps` 观察或终止。
- 已经返回 `process_id` 的进程本来就不属于当前 turn，显式取消不影响它。
- 普通写入或轮询形式的 `write_stdin` 被取消时只停止本次写入后的等待，不终止目标进程；取消前已经写入的字节不回滚。`terminate=true` 的 SIGKILL 请求一旦由 ProcessManager 接受就是已经发生的副作用，后续取消等待不会撤销。`process_list` 可以直接结束，不持有需要额外清理的资源。
- 已成功登记并启动的 subagent 是独立受管任务；取消 parent turn 不隐式停止它。尚未完成登记的spawn 调用随 tool future 一起取消，不得留下不可发现的半登记 subagent。
- tool call 的 Completed 与 Interrupted 终态必须由同一个原子状态迁移决定，先到者获胜，禁止重复发终态；旧 turn 最终以 `InterruptedByUser` 收束，不向 provider 发送伪造或不完整的结果。
- 取消期间 TUI 显示 `Turn cancel pending: settling active tool calls`。协作收束、最长 `100ms` grace period、必要的有界 teardown 与 journal flush 完成后恢复 idle；后台进程和 subagent 继续运行。

若取消时有已登记的 `code_run` 进程继续运行，取消收束展示使用以下精确文案：

- 单个：`Interrupted · process 8f31ab20 continues in background`
- 多个：`Interrupted · processes 8f31ab20 / 8f31abe1 / 8f31ab29 continue in background`

多个 `process_id` 使用 ` / ` 分隔并保持稳定顺序。该文案表达的是 tool call 被中断、进程寿命已
与 turn 分离，不能把对应 ToolCell 标记为普通 Completed。

### D21. 每次 provider request 注入 owner-scoped 后台进程动态上下文

后台进程是独立于 transcript 变化的 runtime world state，不能只依赖 compaction summary 或期待模型主动调用 `process_list`。每次构造 main 或 subagent 的 provider request 时，如果当前 owner存在 live entry、仍保留的终态 entry 或尚未成功投递的最小 completion notification，则在完成compaction projection 后追加一份有界动态上下文；没有任何 entry / notification 时不注入。该上下文：

- 不写入 `messages.jsonl`，不推进 compaction frontier，也不再次交给 compaction summarizer；它只表示本次 provider request 构造时的权威 runtime snapshot，并明确标注不是新用户请求。
- 动态上下文参与 provider request 的自动 compact 触发估算；尚未发生 compact 时，不得把“compact 后 raw tail”的 hard limit 提前应用到完整 active turn。实际执行 compact 时，planner 必须从 soft target 与 hard limit 中预留本次动态上下文的 token 预算，完成 compact 后再校验合并投影，避免 runtime state 被追加后越过 hard limit。
- 动态上下文严格按 `ProcessOwner` 过滤：main 自动注入时只看到 main-owned entry，各 subagent 只看到自己的 entry。main 的跨 owner `process_list` / 受限 `write_stdin` 是用户面对的显式监督能力，不等同于每次 provider request 自动注入全部 child runtime state；子 agent 间绝不泄漏。
- `Live processes` 列出全部 live entry，至少包含完整 `process_id`、`running` / `terminating`、TTY、`started_at`、向下取整的 elapsed、command preview 和 CWD preview；按 `running → terminating`、`started_at` 倒序、`process_id` 升序排列。模型需要完整权威字段时调用 `process_list`。
- `Recently completed` 不按固定分钟、小时或天数设置 TTL，也不表示当前 Agent 历史上完成过的全部任务。它只列出当前 owner 的 ProcessManager store 中仍保留的终态 entry，状态至少区分`finished`、`error` 和 `terminated`，并包含完整 `process_id`、exit code 或终止信号、`finished_at`、固定运行时长、command preview，以及最终输出当前是否仍可读取；按`finished_at` 倒序、`process_id` 升序排列。
- 动态上下文不内联 stdout / stderr。只要终态 entry 尚未被移除，模型就可以使用`write_stdin(process_id, "")` 获取有界剩余输出与最终状态。
- 输出分页与最终结果统一采用两阶段交付，不能在 `code_run` / `write_stdin` runtime 读取完成时立即破坏性消费。第一阶段生成有界 snapshot，并把本次实际展示前缀末尾的 cursor 作为待提交 receipt 与 tool result 一起加入当前 active suffix；只有包含该 tool result 的后续 provider request 成功完成，才能确认模型已经读到该页并提交 cursor。provider request 失败、被取消，或 turn 在成功响应前中断时，必须回滚/保留该页，使 provider retry 或新的 `write_stdin` 仍可获得相同内容。终态 entry 只有在最后一页成功交付后才从 ProcessManager 移除。
- 模型仅看到 `Recently completed` 元数据不算已经读取最终结果，也不能触发 entry 移除。
- `Recently completed` 的容量和寿命直接复用现有 entry 规则：最终结果成功交付 provider 后移除；尚未确认交付的 entry 受每 owner `64` 个总 entry 上限、最近使用 `8` 个 entry 保护、LRU 淘汰和owner shutdown 约束。因此它有明确容量上限但没有独立时间范围或额外无界历史。
- `BackgroundProcessCompleted` 产生的最小 completion notification 至少保留到它被包含在一次成功完成的 provider request 中；provider 失败或 turn 在响应前中断时不能视为已投递。若完整 entry在首次成功投递前已因容量压力被淘汰，notification 仍提供 ID、终态、exit code / signal、完成时间和 `final_output_available=false`，但不虚构已丢失的最终输出。
- compaction summary 仍负责保留“为什么启动该任务、预期如何使用结果”的会话意图；动态上下文负责提供当前 ID 和状态。两者职责不能互相替代。
- 最终结果成功交付 provider 后，该终态 entry 无论之后是否发生 compact，都不再重新出现在runtime context。此后它属于历史会话信息：compaction prompt 必须保留仍会影响后续工作的任务目的、最终成功/失败状态、关键输出、生成或修改的文件以及剩余动作；已经失效的 `process_id`除非仍有审计价值，否则不要求继续保留。

动态上下文的逻辑形态为：

```text
<background_processes>
This is authoritative runtime state, not a new user request.

Live processes:
- 8f31ab20 | Running | tty | started 10:20 | elapsed 1h20m | cargo test ... | /workspace

Recently completed:
- 4a092f13 | Finished | exit 0 | finished 8m ago | elapsed 42m | ./build.sh

Use process_list for full live-process details and write_stdin with empty chars to read final output.
</background_processes>
```

### D22. ProcessManager 与共享 MCP 连接使用两套独立所有权域

合并后的 `docs/PRDs/PRD_shared_mcp.md` 已确定：一个 ACN 进程只有一个 `McpConnectionManager`，parent、全部 subagent 和后续 turn 共享每个 server 的常驻 client/session；stdio server 对应的 child process也由该 manager 独占管理。后台 shell 实现必须保持这条边界：

- MCP stdio child 是 ACN 连接基础设施，不是模型通过 `code_run` 启动的受管 terminal。它不登记到`ProcessManager`，不分配 `process_id`，不出现在模型 `process_list`、用户 `/ps`、`<background_processes>` 或 `Recently completed` 中。
- `/ps` terminate、owner cleanup、subagent 收束和 root session shutdown 只能清理对应`ProcessManager` entry，不能关闭 MCP stdio child。MCP child 只由 disable、reconnect、明确transport failure 或 ACN process shutdown 经 `McpConnectionManager` 收束。
- `ToolRegistry` 可以同时持有共享的 `Arc<McpConnectionManager>` 与后台进程依赖，但两者的复用范围不同：registry clone / `for_delegation` 必须原样复用同一个 MCP manager；后台进程侧则由当前 root session 的 ProcessManager 派生不同 `ProcessOwner` handle。不能为了注入进程工具重建 MCP manager，也不能把跨 root session 的进程 entry 放进进程级 MCP 生命周期。
- MCP 的 progress token、request id、server generation、stdio OS PID 和后台 shell `process_id`属于不同命名空间，不得互相复用或在路由、取消、TUI 展示中混为一谈。
- 本需求不改变 `docs/PRDs/PRD_parallel_tools.md` 与共享 MCP 已有的调度矩阵：MCP 仍只按原始`readOnlyHint` 决定同 agent 并发资格，parent / subagent 之间不新增 server lock；后台 shell 的owner 隔离也不能被实现成 MCP 跨 agent 锁。
- 显式 turn cancel 通过现有 `ToolDispatchContext.cancellation` 传播到该 turn 的 MCP request；ProcessManager 登记点只决定 `code_run` 进程的去留，不参与 MCP request 或连接 generation 的判定。反过来，MCP disable / reconnect 是连接级操作，会收束旧 client 的全部 in-flight request，但不能终止任何 `code_run` 受管进程。

---

## 推荐实现结构

工具定义、调度、文件、Web、delegation 与进程执行原本集中在单文件中。后台终端是内聚且规模较大的子域，因此拆到：

```text
src/tool/process/
  mod.rs
  manager.rs
  manager.rs
  session.rs
  state.rs
  output.rs
  watcher.rs
  pty.rs
  pipe.rs
  process_group.rs
```

职责：

- `manager.rs`：owner-scoped handle、启动、轮询、写入、list、terminate、LRU、owner cleanup。
- `manager.rs`：root-session process store、全局 ID、owner 分区、TUI 聚合查询与 session cleanup。
- `session.rs`：本地 PTY / pipe 句柄的统一抽象。
- `state.rs`：starting / running / exited / failed / killed 与 stdin 状态。
- `output.rs`：有界 head/tail buffer、chunk、截断与遗漏计数。
- `watcher.rs`：持续输出 drain、退出观察和领域事件。
- `pty.rs`：PTY spawn、writer、reader、wait 和固定初始行列数。
- `pipe.rs`：非 TTY spawn 与 stdout / stderr 分流。
- `process_group.rs`：Unix PGID / interrupt / terminate / kill；暂时不提供 Windows 实现。

`ProcessManager` 应以 `Arc` 形式由 bootstrap / session runtime 创建并注入 `ToolRegistry` clone。main registry 与每个 delegation registry 获得不同的 owner-scoped handle，但共享同一个root-session ProcessManager；memory review registry 不获得进程工具访问能力。

该注入必须与现有 `Arc<McpConnectionManager>` 并存：clone / delegation 继续复用进程级共享 MCP manager，同时只替换或收窄后台进程的 owner-scoped handle。两类 manager 不互相持有对方的 child process，也不互相执行 shutdown。

每个 entry 至少包含：

```text
process_id
owner_session_id
owner_agent_id
owner_subagent_id
command / type / cwd / tty
root_pid / process_group_handle
stdin / output / exit handles
state / exit_code / failure
started_at / last_used_at
```

---

## 配置方向

用户 TOML 的 background-shell 配置面固定为：

```toml
[agent.tool]
code_run_max_output_chars = 1048576
write_stdin_max_poll_timeout_ms = 300000
```

`write_stdin_max_poll_timeout_ms` 的最大值为 `300000`ms，且不能小于内部 `code_run` 最大观察窗口`30000`ms。`code_run_max_output_chars` 限制每个 stdout/stderr stream 的单次工具回传量，并作为 schema 中显式可见的默认值。当前默认 `1048576` 已是每个 stream 约一百万字符，不再为少数超长输出把默认值提高到硬上限；超出单次上限的连续输出使用分页读取。

其余 background-shell 参数仅是 `config.rs` 的内部默认值和资源护栏，不可由部署 TOML 下调或覆盖：`code_run` 的初始 / 最小 / 最大观察窗口为 `10000`ms / `250`ms / `30000`ms，非空写入与空轮询默认窗口为`250`ms / `5000`ms；每 owner entry 容量、输出 buffer、PTY 初始行列、PTY stdin budget 与输出 drain grace 也使用内部值。PTY 初始尺寸不跟随 ACN TUI 窗口变化，也不构成动态 resize API。

这些内部护栏防止配置绕过 D13/D14 的内存、容量和工具等待边界。

---

## 验收测试

至少覆盖：

1. 短命令在 yield 内退出，不留下 process entry。
2. 长命令超过 yield 后返回 `process_id`，tool call 正常闭合。
3. 进程在初始 yield 前登记，turn interrupt 不会丢失或杀掉已登记进程。
4. `tty = false` 保持 stdout / stderr 分流。
5. `tty = true` 支持 bash / Python REPL 或等待输入的程序。
6. `write_stdin` 能写入文本并读取响应。
7. 空 `write_stdin` 只轮询；连续输出超过单次上限时，按已展示前缀分页推进，provider 成功后下一页不重复，provider retry 或 turn 中断后仍可重新取得尚未确认的同一页。
8. Ctrl-C 能软中断 foreground process group，但不会误关闭仍在运行的交互 shell / SSH session；返回的`state` 始终表示外层受管进程。
9. `write_stdin(terminate=true)` 与 runtime terminate one / terminate all 复用同一管理路径，能停止完整受管进程树；`terminate=true` 与非空 `chars` 互斥。
10. PTY 使用配置的初始行列数且运行期间保持不变；ACN TUI resize 不改变它，也不存在模型或 runtime `process_resize` 入口。
11. root process 正常退出后清理同 PGID 后代。
12. 长命令不会因旧 60/180 秒配置或隐式 runtime timeout 被终止。
13. turn interrupt 不杀已登记后台 terminal。
14. owner session / subagent / ACN 正常 shutdown 清理全部 owned process。
15. `nohup command` 可以作为受管长命令返回 `process_id`。
16. `nohup command &` 不被语法拦截，但不承诺 root 退出后的管理能力。
17. `setsid` / daemon escape 边界有明确测试和文档。
18. root 退出但后代继承 stdout fd 时不会导致 manager 永久挂起。
19. 输出超过上限后 head/tail、`truncated`、`omitted_bytes` 正确；UTF-8 分页 cursor 按字符而非字节推进；retained buffer 已出现 gap 时不错误跳过内容。
20. 并发 write / poll / runtime terminate 不产生死锁、重复消费或进程句柄竞态。
21. 达到 process 上限时按策略淘汰，并清理被淘汰 live process。
22. `process_id` 不复用活跃 ID，不直接等于 OS PID。
23. 旧 `code_run` 非零 exit code 的结构化诊断保持可用。
24. delegation 身份环境变量在后台 terminal 中保持正确。
25. main 的 `process_list` 返回本 root session 全部 live entry（带 `owner`），subagent 的`process_list` 只返回自己的 live entry；两者均不消费输出，且能恢复可用于 `write_stdin` 的内部 `process_id`。
26. subagent 与 parent / sibling 的模型视图保持隔离；main 对 child 可空轮询、发送 Ctrl-C 或`terminate=true`，但不能写入任意文本，subagent 收束时仍清理自己的全部进程。
27. `/ps` 与 `/mcp` 在 turn 运行期间立即打开，不进入 queued input，关闭面板后 turn 输出继续正常显示。
28. `/ps` 超过 viewport 高度后，`↑` / `↓` 移动会同步调整 `list_offset`，选中行始终可见；`/mcp` server/tool 列表具备同等滚动行为。
29. `/ps` `TerminateConfirm` 只响应 `↑`、`↓`、`y` / `Y`、`n` / `N`、`Esc`；Command body滚动时 footer 始终固定在页面最后一行，其他按键不改变状态。
30. terminate 确认绑定 `process_id` 而不是行号；确认前目标自然退出不会误杀刷新后同位置的其他进程。
31. 确认 terminate 后硬终止完整受管进程组，异步状态从 `terminating` 刷新到行消失，选择落点符合相邻行规则。
32. `/ps` 只显示 live entry，按 `running → terminating`、开始时间倒序、`process_id` 升序稳定排列；终态 entry 不出现，`STARTED` 与精确到秒、最多三个单位的 `ELAPSED` 在不同终端宽度下正确显示；turn idle 时保持面板打开，`ELAPSED` 仍按秒持续增加。
33. main 与 subagent 模型均注册 `code_run`、`write_stdin`、`process_list`：child 的 list / write严格 owner 隔离；main 在本 root session 跨 owner list、空轮询、Ctrl-C 和 `terminate=true` 的受控监督能力正确，文案按角色明确说明边界。
34. `/ps` 聚合当前 root session 的 main 与全部 subagent entry，`OWNER` 列、面板标题和确认页显示正确，不混入其他 session 或 Agent。
35. TUI terminate subagent-owned process 只停止目标进程组，不取消 subagent；subagent 后续读取能观察到终止结果。
36. 每个 owner 独立应用 64-entry 与最近 8-entry 保护策略；一个 owner 达到容量上限不会淘汰其他 owner 的 live entry。
37. subagent completed / failed / abandoned、执行 future 取消和 parent session shutdown 都会清理正确 owner 分区，且不向 main handoff。
38. `TerminateConfirm` 完全替换 `/ps` 列表；完整 Command 复用 code-block renderer，在不同宽度下只 wrap、不截断，resize 后滚动位置正确 clamp。
39. `/ps` 列表与 `TerminateConfirm` 的 `running` / `terminating` 使用同一状态样式映射；选中行不覆盖 STATUS 颜色，分别保持绿色粗体与黄色粗体；选中 `terminating` 行时按 `t` 不产生动作。
40. steer 到达时，尚未派发调用被 skipped，已启动调用全部真实收束后旧 turn 才以`InterruptedByUser` 结束，随后 steer 文本自动作为新 turn 提交。
41. steer 发生在 `code_run` initial yield 期间时等待该 tool call 退出或 yield；发生在`code_run` 已返回 `process_id` 后时不等待、不终止后台进程。
42. Esc / Ctrl-C 立即取消 provider、跳过未派发工具，并在协作取消后最多等待 `100ms` 就强制abort 未收束的普通 tool future，随后恢复 idle。
43. `code_run` 在 ProcessManager 登记前被取消时由 guard 清理进程；登记后在 initial yield 期间被取消时 tool call 以 Interrupted 收束，但进程保持 live 且可由三个进程工具和 `/ps` 管理。
44. 单个及多个后台进程的 Interrupted 文案、单复数、` / ` 分隔与稳定排序正确，且不会同时发出
    Completed 和 Interrupted 两个终态。
45. 显式取消普通写入/轮询形式的 `write_stdin` 不终止目标进程、不回滚已写入字节；`terminate=true` 已接受的硬终止也不回滚，其他工具已经产生的副作用同样不回滚。
46. parent turn 在 subagent 成功登记后被显式取消时 subagent 继续运行；登记前取消不会留下半登记或不可发现的 subagent。
47. steer 仍按 D19 等待 tool boundary，不会误用显式取消的 `100ms` 强制 abort 路径。
48. 强制 abort 后原 tool future 不再产生结果；它此前创建的每个仍在运行的本地资源都能映射到ProcessManager、subagent runtime 等明确 owner，或已被 guard 清理，不存在不可发现的 detached task、线程或进程。
49. 对已经发出的远端请求执行 best-effort cancellation；即使远端无法取消，turn 仍能本地收束，且不会错误宣称外部副作用已停止或回滚。
50. `process_id` 候选发生碰撞时在 ProcessManager 原子检查并重抽；并发登记不会获得相同 ID，重试耗尽时返回明确错误，已经 spawn 的未登记进程由 guard 清理。
51. compact 把原始 `ProcessRunning` tool result 移出 raw tail 后，下一次 provider request 仍注入当前 owner 的完整 live process ID 与状态；main 和 subagent 之间不串数据。
52. `Recently completed` 不使用时间 TTL，只包含仍保留的 owner-scoped 终态 entry，并随最终结果成功交付 provider、LRU、entry 上限或 owner shutdown 消失；不内联最终输出。
53. completion notification 在 provider 失败或响应前 turn 中断时不会被错误确认；entry 先被淘汰时仍至少成功投递一次最小终态信息，并明确最终输出不可读取。
54. 后台动态上下文只进入当次 provider projection，不写 canonical transcript、不推进 compaction frontier；没有相关 entry / notification 时完全省略。
55. `code_run` / `write_stdin` 生成 partial 或 final snapshot 后，provider 成功响应才提交该页cursor；provider 失败、取消或 turn 提前中断时保留同一页。终态最后一页成功交付后才移除entry；仅看到 `Recently completed` 不算读取，成功交付后即使再次 compact 也不重新注入该 entry，历史摘要保留其关键语义结果。
56. MCP stdio child 不登记为后台 process，不出现在 `process_list`、`/ps` 或动态后台上下文；`/ps` terminate、owner cleanup 和 session cleanup 均不会误杀共享 MCP child。
57. Esc / Ctrl-C 取消正在执行的 MCP tool 时只取消对应 request；协作取消及 caller future 强制abort 都不会 shutdown 共享 client、把 server 标成 failed 或打断 peer / follow-up request。
58. registry clone / delegation 同时保持“同一个进程级 `McpConnectionManager`”与“同一个root-session `ProcessManager` 的不同 owner handle”；不同 root session 的后台进程不因 MCP manager 共享而串入同一 `/ps` 或模型视图。
59. MCP disable / reconnect 只按共享 MCP generation 语义收束旧连接及其 in-flight request，不会终止任何 `code_run` 进程；后台进程 terminate 也不会改变 MCP server generation 或 ready 状态。
60. turn 运行期间通过 `/mcp` 主动 Reconnect / Disable 时，该 server 上 main 与 subagent 的全部in-flight MCP 调用都以 `dispatch_failure` 收束且不自动重试；当前 turn 不取消并继续推进，其他 MCP server、后台进程和 agent runtime 继续运行，生命周期切换后的新 generation 不受旧调用迟到结果污染。
61. active turn 尚未达到自动 compact 触发阈值时，即使其原始工具轨迹大于 compact 后 raw-tail hard limit，也不会因存在后台 runtime projection 而提前失败；实际 compact 时为 runtime projection 预留 soft/hard tail 预算，合并后的投影不越界。
62. main-owned `code_run` 返回 `ProcessRunning` 后，无论进程自然退出、被 `/ps` 终止或从外部收到信号，TUI heartbeat 都把原 cell 更新为对应 exit / signal 终态；活动虚线框与已提交历史都不残留 `Process running in background`。subagent 进程不改写 main transcript 的 tool cell。
63. session finalize 会在 recap 前收束并持久化全部 main-owned 后台终态；`/exit` 的最后一帧先更新原 tool cell，再退出 TUI。recap 与 trace 使用至多 64 条最新 completion 事实，并显式记录更早事实的省略数量。

---

## P1～P7 已拍板结论

### P1. TUI `!` shell command 保持现状

本 PRD 只改模型工具 `code_run`。`!` 继续按 `PRD_shell_command.md` 的一次性非 PTY 语义执行，不复用 ProcessManager，也不新增 terminal input mode、焦点切换或交互式后台 cell。

### P2. 模型侧只提供三个进程工具

模型工具固定为：

```text
code_run
write_stdin
process_list
```

不向模型暴露 `process_signal`、`process_stop` 或 `process_resize`，runtime 也不实现动态`process_resize`。`process_list` 是受控的只读恢复入口，用于恢复上下文压缩后丢失的内部`process_id`；`write_stdin(terminate=true)` 在不增加第四个工具的前提下提供 owner-aware 硬终止，其中 main 可终止本 root session 的 child entry，TUI/runtime 控制面继续拥有 root-session 聚合强制管理能力。

### P3. 后台完成不主动唤醒 agent

完成事件只更新 journal、TUI 和 pending notification。它不会自动创建 scheduled/system turn，也不会在用户没有新动作时发起额外模型请求。

### P4. subagent 所有权隔离与 main 受控监督

subagent 创建的进程仍归该 subagent 所有，subagent 只在自己的 `process_list` / `write_stdin` 视图中操作它；subagent 可以 `terminate=true` 硬终止自己的 entry，收束时清理其全部受管进程，不发生 handoff。parent/main agent 可以通过 root-session 聚合 `process_list` 查看 main 与全部直接subagent 的 live entry，并对 child 执行空轮询、Ctrl-C 或 `terminate=true`，但不能发送任意终端输入。用户 TUI `/ps` 可以查看和 terminate main 与全部 subagent 的进程。

### P5. 暂时只支持 macOS/Linux

暂时完整实现 Unix PTY、session、PGID 与进程树清理。Windows 不在范围内，不要求 ConPTY、Job Object、Windows 后台功能或相关兼容验证。

### P6. 无进程运行时 timeout

删除旧 `code_run_default_timeout_secs` / `code_run_max_timeout_secs`，不保留兼容字段，也不新增全局硬上限。长命令可以在 owner session 内持续运行；owner finalize/close、subagent 收束和ACN 正常 shutdown 时统一清理。

### P7. 已退出 entry 惰性回收、无 TTL

退出 entry 保留到输出的最后一页成功交付 provider 后才移除；每个 partial/final snapshot 都只在包含该 tool result 的 provider request 成功后提交 cursor，工具本地读取、provider 失败/取消或 turn 提前中断都不提交消费。尚未成功交付的 entry 在 LRU 淘汰或 owner shutdown 时也可清理，但必须遵守 D21 的最小 completion notification 投递规则。容量淘汰时保护最近使用的 8 个 entry，再优先删除较旧的已退出 entry；没有可淘汰的已退出 entry 时才终止并移除较旧的 live entry。容量和淘汰按 owner 独立计算，不跨 owner 回收，也不设置时间 TTL。


---

## 当前结论

- 现有 `code_run` 改造成统一的短命令与受管终端执行入口。
- 长命令通过 yield 返回 `process_id`，不依赖 shell `&`。
- 模型工具面只有 `code_run`、`write_stdin`、`process_list`。
- main 和每个 subagent 都获得这三个工具；child 模型视图严格按 owner 隔离，main 对当前 root session 具有带 `owner` 的聚合查询、空轮询、Ctrl-C 和 `terminate=true` 监督能力；subagent 只能硬终止自己的 entry，用户 `/ps` 聚合全部 owner 并可 hard terminate。
- PTY、stdin、分页 poll 和进程管理属于同一受管 terminal session；Ctrl-C 是软中断，`terminate=true`是 owner-aware 硬终止。
- tool call 生命周期与进程生命周期分离。
- root terminal process 是管理边界，不实现 `detached_running`。
- `nohup` 正常允许执行，但不会得到超出 ProcessManager 所有权边界的特殊保证。
- Esc / Ctrl-C 采用协作取消、`100ms` grace period 和强制 abort；已登记的进程与subagent 继续运行，已发生的工具副作用不回滚。
- MCP stdio child 仍由进程级共享 `McpConnectionManager` 独占管理，不进入 ProcessManager；turn cancel 只取消对应 MCP request，不关闭共享 client。
- 不设置进程运行时 timeout，也不跨 ACN 重启存活。
- 暂时仅支持 macOS/Linux。

---

## 分阶段实施计划

本需求同时包含进程运行时、模型工具协议、provider context、turn cancellation、subagent ownership和 TUI 六类改造。实施时按下列阶段推进；每一阶段都必须保持可编译、通过该阶段定向测试，并且不能用临时的第二套后台工具或永久兼容分支绕开最终协议。阶段内可以继续细分提交，但不得跨过当前阶段的验收 gate 后再补核心资源所有权或取消安全。Phase 0～7 是同一功能分支上的工程 gate，不是可单独发布的残缺产品；只有 Phase 8 和整体验收全部完成后才交付。

### Phase 0：基线冻结与契约测试

目标：先把当前一次性 `code_run`、tool boundary、MCP request cancellation、delegation registry和 TUI 输入路由的行为固化，避免后续把既有能力退化误认为新需求问题。

TODO：

- [x] 记录当前 `code_run` schema、短命令成功/非零退出、stdout/stderr、cwd、三种 script type、delegation 身份环境变量和输出截断基线。
- [x] 为当前 steer、Esc/Ctrl-C、并行 tool batch、MCP turn cancellation 与 caller-abort 增加或确认回归测试；测试必须区分 `ToolCallCompleted`、`ToolCallInterrupted` 和 skipped。
- [x] 盘点 `ToolRegistry` 的 parent、delegation、memory review 构造/clone 路径，形成注入`McpConnectionManager` 与 root-session `ProcessManager` 的唯一接线图。
- [x] 盘点 session start/resume/finalize、subagent completed/failed/abandoned、ACN shutdown 的清理入口。
- [x] 把本文 60 条验收项映射到测试层级和负责阶段；若实现中发现产品语义缺口，先回到 PRD 拍板，不在代码中暗设行为。

阶段验收：

- 当前主分支基线测试通过，新增契约测试在未改生产语义时稳定通过。
- 接线图覆盖 main、resume 后的 main、每个 subagent 和 memory review，不存在未知 registry clone 路径。
- 本阶段不引入后台进程生产代码，不改变用户可见行为。

### Phase 1：Unix process core 与 root-session ProcessManager

目标：建立唯一的进程所有权内核，并让现有 pipe `code_run` 先纵向接入；本阶段可以仍等待命令退出，但 spawn 后的每一个进程必须立即进入“guard 持有或 ProcessManager 已登记”二选一状态。

TODO：

- [x] 创建 `src/tool/process/` 内聚模块，落实 manager、session、state、output、watcher、pipe 和 process_group 职责；避免继续把实现堆入工具聚合模块。
- [x] 实现 `ProcessOwner`、root-session `ProcessManager`、owner-scoped handle、8 位 hex `process_id` 原子碰撞重抽和明确的重试耗尽错误。
- [x] 实现 spawn guard、登记线性化点、root PID/PGID 句柄、异步 wait、pipe drain、退出状态迁移和owner/session shutdown cleanup；所有 I/O 使用 Tokio 异步路径。
- [x] 实现有界 head/tail output buffer、绝对 cursor、`truncated`、`omitted_bytes` 和 stdout/stderr分流；读取期间不得无界积累完整输出。
- [x] 把现有非 PTY `code_run` 接到新内核，先保持短命令的原有 tool result 与错误诊断，证明新模块已真实使用而不是未接线抽象。
- [x] 增加不含进程运行时 timeout 的 background process 配置结构和启动校验；旧 `code_run` timeout 只允许在这个不可发布的迁移阶段维持基线入口，必须与 Phase 2 的工具协议切换一起删除，不能进入 ProcessManager 或最终配置。
- [x] 保证 MCP stdio child 不经过 ProcessManager，两个 manager 的 child ownership 完全隔离。

阶段验收：

- 覆盖验收项 1、4、19、22、23、50、56 的底层部分。
- 单元测试覆盖 ID 并发碰撞、状态机非法迁移、buffer 边界、UTF-8 分块、cursor、截断和 LRU 基础逻辑。
- 集成测试覆盖 spawn 后 future drop、登记失败、非零退出、超大 stdout/stderr、owner cleanup 和完整PGID 清理，不留下孤儿进程或 reader task。
- `cargo check`、定向 `cargo test process` 和改动文件相关 clippy 通过。

### Phase 2：统一执行工具协议与长命令 yield

目标：完成模型侧 `code_run`、`write_stdin`、`process_list` 三工具协议，使 tool call 与进程寿命真正分离；先覆盖 pipe backend，PTY 在下一阶段接入同一 session abstraction。

TODO：

- [x] 将 `code_run` 输入改为 `script/type/cwd/tty/yield_time_ms/max_output_chars`，删除旧运行时`timeout`；实现 yield clamp、初始等待和短命令/`ProcessRunning` 双返回。
- [x] 保证 ProcessManager 先登记、`code_run` 后等待；yield 返回后 tool call 正常闭合，watcher继续 drain output 和观察退出。
- [x] 实现 `write_stdin(process_id, chars, terminate, yield_time_ms, max_output_chars)` 的写入、空字符 poll、owner-aware 硬终止、chunk cursor、stdin_open、exit/failure 返回和分页读取语义。
- [x] 实现只列 live entry 且不消费 output 的角色化 `process_list`：subagent 只看 owner，main 看同一root session 全部 owner；字段、排序和状态符合 D6。
- [x] 在 parent 与 delegation 工具定义中注册三个进程工具，在 memory review 等 profile 中保持不可见。
- [x] 实现 `BackgroundProcessCompleted` 领域事件与最小 completion notification，但本阶段不主动发起provider request。
- [x] 实现 per-owner 64-entry、最近使用 8-entry 保护和 live/terminal 淘汰顺序；淘汰 live entry必须先清理真实进程。

阶段验收：

- 覆盖验收项 1～4、6～7、12～15、19～25、50 的 pipe/tool-protocol 部分。
- 使用短 yield 的异步测试证明长命令快速返回 `process_id`，旧 tool future 结束后进程与 watcher仍存活；短命令不留下 entry。
- 并发 start/list/poll/write/terminate 与容量压力测试不死锁、不重复消费、不跨 owner 淘汰。
- schema snapshot 测试证明模型只看到三个进程工具且不再看到 `timeout`。
- 定向测试、`cargo check` 和 clippy 通过；不得等到 PTY 阶段才修复 pipe 资源泄漏。

### Phase 3：PTY、交互输入与 Unix 进程组边界

目标：在同一 process session abstraction 上补齐 PTY 和完整 Unix 终端控制，不建立另一套 PTY专用 manager。

TODO：

- [x] 在确认所选 PTY crate/API 的 spawn、固定初始行列数、reader/writer 和 Unix 支持边界后接入 PTY backend；PTY master 的读写和 wait 不得阻塞 Tokio runtime。
- [x] 支持 bash、Python REPL、等待 stdin 的程序、文本写入与 `\u0003` Ctrl-C；不实现 runtime resize。
- [x] 实现 PGID/session 创建、interrupt、terminate、kill 和有界升级策略，保证终止完整受管进程树。
- [x] 以 root terminal process 为 session 完成边界：root 退出后清理同 PGID 后代，不实现`detached_running`；处理继承 stdout fd 导致 reader 不 EOF 的情况。
- [x] 验证 `nohup command`、`nohup command &`、`setsid` 和 daemon escape 边界；不禁止 shell 语法，也不虚构 escape 后仍可管理。
- [x] 明确 macOS/Linux 条件编译和测试；Windows 不实现 ConPTY、Job Object 或降级后端。

阶段验收：

- 覆盖验收项 5、8～11、15～18，以及 9/14 的 Unix process-tree 部分。
- 真实 PTY 集成测试运行 bash/Python 交互程序，写入多轮输入、发送 Ctrl-C，并验证子进程读到配置的初始行列数；运行期间触发 ACN TUI resize 后该尺寸保持不变。
- 进程树 fixture 记录 PID/PGID；interrupt、terminate、root exit 和 owner cleanup 后逐个确认成员消失。
- 至少一个真实 wall-clock 长命令运行超过旧最大 timeout 后仍存活，再由 owner cleanup 收束；普通单元测试可用短 yield，但不能只靠暂停时间证明 OS 进程存活。
- macOS 本地定向测试与 Linux CI/等价环境测试均通过；没有 Windows 验收要求。

### Phase 4：终态结果两阶段交付与 provider runtime context

目标：让 compact、provider 失败和长对话都不会使模型忘记后台任务，也不会提前消费尚未真正投递给模型的最终输出。

TODO：

- [x] 实现 terminal entry、partial/final snapshot、delivery token/游标和 provider-success commit；`code_run` / `write_stdin` 本地返回不等于已经成功交付模型。
- [x] provider 失败、turn cancel、steer 中断或 retry 时保留/回滚同一页；成功响应后原子提交该页 cursor，并在终态最后一页交付后移除对应 entry。
- [x] 每次 provider request 在 compaction projection 后注入 owner-scoped `<background_processes>`，包含有界 `Live processes`、`Recently completed` 和最小 notification。
- [x] 动态上下文不写 canonical transcript、不推进 compaction frontier、不交给 summarizer；无相关entry 时完全省略。
- [x] completion notification 至少保留到一次成功 provider response；entry 先被 LRU 淘汰时保留`final_output_available=false` 的最小事实。
- [x] 更新 compaction prompt，使已成功交付并移除的任务只保留长期仍有价值的目的、结果、文件和剩余动作，不重新注入失效 `process_id`。

阶段验收：

- 覆盖验收项 7、51～55。
- provider fixture 分别模拟成功、错误、取消、响应前中断和 retry，证明只有成功 response 提交消费。
- compact 前后 request capture 证明 runtime context owner-scoped、位置正确且不进入 transcript；main/subagent fixture 之间不泄漏 ID。
- LRU 淘汰 terminal entry 后仍投递最小 notification，不声称丢失输出可读。
- resume/compact/多轮 provider integration tests、`cargo check` 与 clippy 通过。

### Phase 5：subagent ownership、聚合控制面与生命周期清理

目标：让 subagent 保持 owner 隔离，同时让 main 与 root-session TUI 都有受控的跨 owner 监督视图；subagent 结束时不 handoff 进程。

TODO：

- [x] bootstrap/session runtime 创建一个 root-session ProcessManager；main 与每个 delegation registry获得不同 owner handle，同时继续 clone 同一个进程级 `McpConnectionManager`。
- [x] owner identity 包含 session/agent/subagent 维度，`process_id` 在 ProcessManager 内全局唯一；child list/write 严格 owner scoped，main 可按 root session 聚合 list、只读 poll、Ctrl-C 和 `terminate=true`，runtime 聚合API 同样按 root session scoped。
- [x] subagent completed、failed、abandoned、future cancel 和 parent session shutdown 时清理对应owner 全部进程，不转交 main，也不影响其他 owner。
- [x] TUI 聚合 snapshot 保存 owner 信息，terminate 仍只按稳定 `process_id` 定位目标进程组。
- [x] 保持 delegation 身份环境变量、并发 runner 和 MCP parent/child 共享连接语义。

阶段验收：

- 覆盖验收项 24～26、33～37、56、58。
- 至少两个并发 subagent 与 main 同时启动进程；subagent 模型视图互不可见，main 的受控聚合视图与root TUI 聚合视图完整可见，main 的文本输入不会跨 owner 写入。
- 分别触发 completed/failed/abandoned/future abort/parent shutdown，使用 PID/PGID fixture 验证只清理正确 owner。
- 同时调用共享 MCP 与后台 shell，证明 MCP stdio PID 不出现在 process store，任一侧 cleanup 不误杀另一侧资源。
- delegation、session resume/finalize 集成测试、`cargo check` 与 clippy 通过。

### Phase 6：steer 与显式 turn cancel 收束

目标：在已有 ProcessManager 线性化点上实现 D19/D20，严格区分 steer 的安全边界等待与Esc/Ctrl-C 的快速显式取消。

TODO：

- [x] 保持 steer durable record、dispatch gate、skipped、已启动工具真实终态和新用户 turn 提交流程；只适配 `ProcessRunning` outcome，不把后台进程退出重新纳入 tool boundary。
- [x] 为 explicit cancel 增加独立 hard-cancel 路径：立即取消 provider/关闭 gate，向已启动工具发cooperative token，等待最多 `100ms`，随后 abort 未收束 future。
- [x] 以原子 terminal state 防止 Completed/Interrupted 双终态；完成必要的有界 teardown 与 journal flush 后恢复 idle。
- [x] `code_run` 登记前取消由 spawn guard 清理，登记后 initial yield 被取消则 tool call Interrupted、进程继续；`write_stdin` 取消不终止目标或回滚已写字节。
- [x] 已登记 subagent 在 parent cancel 后继续；未登记完成的 create 不留下半登记任务。
- [x] 复用共享 MCP 的 request-scoped cancellation/drop guard；caller abort 不 shutdown shared client，peer/follow-up request 保持可用。
- [x] TUI 使用精确 cancel-pending、单/多 process background continuation 文案和稳定 ID 顺序。

阶段验收：

- 覆盖验收项 3、13、40～49、57。
- 对每个取消竞态做确定性 barrier 测试：spawn 前、spawn 后登记前、登记后 yield 中、yield 已返回、tool 正好完成、100ms grace 边缘和 journal flush 失败。
- steer 测试证明 initial yield 会等待真实 tool terminal，explicit cancel 测试证明不响应取消的普通future 在有界时间内被 abort；两条路径不能串用。
- MCP 慢 headers/body、stdio、普通 HTTP/file side effect 和 subagent fixture 分别验证本地收束、best-effort 远端取消和资源 owner 不丢失。
- TUI state/unit integration 测试验证通知文案与 ToolCell 终态；`cargo check` 与 clippy 通过。

### Phase 7：`/ps`、`/mcp` active-turn 面板与终端交互

目标：完成用户控制面，并同步修复 `/mcp` viewport；面板是 TUI live state，不写 transcript 或 queued input。

TODO：

- [x] 新增 `/ps` slash command、聚合列表、稳定排序、列宽降级、started/elapsed 格式和 owner 标题。
- [x] 实现真正的 `list_offset`、选择跟随 viewport 和 snapshot refresh；把相同滚动能力补给 `/mcp` server/tool list。
- [x] 实现全页面 `TerminateConfirm`：固定 metadata/footer、完整 Command code renderer、视觉行滚动、resize clamp、`y/n/Esc` 和其他键忽略。
- [x] 共用 process status style helper；ANSI 下 running 绿色粗体、terminating 黄色粗体，selection 不覆盖状态色。
- [x] `/ps` 与 `/mcp` 在 turn 中立即打开，面板事件优先消费按键；面板内 Esc 只关闭/返回面板，普通 turn live view 中 Esc 才触发 D20 explicit cancel。
- [x] `/ps` terminate 与 `write_stdin` 竞态安全，terminating 行不重复 terminate，目标自然退出时不误杀新行。
- [x] active-turn `/mcp` 保留 Reconnect/Disable：旧 server request 以 `dispatch_failure` 回灌、当前turn 继续、新 generation 不受迟到结果污染，其他 server 与后台进程不受影响。
- [x] 保持 MCP stdio child 对 `/ps` 不可见，MCP lifecycle 与 process terminate 双向隔离。

阶段验收：

- 覆盖验收项 27～32、38～39、56、59～60。
- ratatui state/render 单元测试覆盖 0/1/超 viewport 行、刷新后选中落点、窄屏、resize、多行 command、状态色和所有按键矩阵。
- 使用 `tui-smoke-test-with-tmux` skill 建立可重复的非 LLM 场景脚本：固定终端尺寸、逐 checkpoint文本 capture、颜色场景 ANSI capture、稳定 marker 断言、空 `stderr.log` 和 tmux cleanup。
- active-turn 脚本必须证明 agent output 在面板背后继续推进，关闭后 live region 恢复；`/mcp` Reconnect 场景由 MCP fixture 的 generation/PID/request 日志判定，不能只看模型自述。
- TUI 定向测试、`cargo check`、clippy 和默认 tmux smoke 通过。

### Phase 8：容量、shutdown 与跨阶段故障注入

目标：在最终 UI/协议已经接通后完成跨模块压力与资源泄漏审计。

TODO：

- [x] 并发混合 main/subagent start、write、poll、process_list、TUI terminate、turn cancel、compact 和MCP reconnect，覆盖锁顺序与竞态。
- [x] 注入 spawn、PTY open、register、pipe read/write、wait、journal、provider、MCP lifecycle 和shutdown 失败，确认状态、错误和资源 owner 可恢复。
- [x] 验证 per-owner 容量、最近 8 个保护、terminal 优先淘汰、live cleanup 和最小 notification。
- [x] 验证 session finalize/close、resume 切换、subagent 所有终态、正常 ACN shutdown 与异常 future abort 后无受管进程、reader/watcher、PTY fd 或 tmux session 泄漏。
- [x] 审核日志脱敏和级别；command/output 不因新增 debug 日志无界打印，MCP secret 处理不退化。
- [x] 删除旧 timeout 字段、旧一次性 runner 死代码和临时兼容路径；更新用户可见工具描述与相关 PRD。

阶段验收：

- 60 条验收测试全部有自动化证据或明确的最终 TUI/平台证据，不能只在 checklist 上勾选。
- 使用结构化 fixture 日志核对 PID、PGID、process_id、owner、generation 和事件次数；测试结束用只读进程检查确认没有 child 泄漏。
- 重复运行高竞态测试和 TUI scenario，排除只在单次时序下通过；发现 flaky 必须修复根因。
- macOS 与 Linux gate、完整项目 verification 和默认 TUI smoke 全部通过后才进入最终验收。

---

## 整体验收策略

### 1. 自动化测试金字塔

- 单元测试：状态机、ID、排序、时间格式、buffer/cursor、LRU、schema、viewport、按键路由和纯函数。
- 模块集成测试：真实文件 I/O、真实 Unix process/PTY/PGID、真实 Tokio cancellation、provider fixture、session journal/compaction 和 MCP stdio/HTTP fixture；除 LLM provider 外不 mock 业务流程。
- 跨模块回归：main/subagent ownership、provider 两阶段交付、steer/explicit cancel、TUI runtime snapshot、MCP generation 与 ProcessManager 隔离。
- 平台测试：macOS 与 Linux 都运行 process group/PTY/cleanup 核心矩阵；Windows 明确跳过。

每个阶段先跑定向测试，最终从仓库根目录执行：

```bash
source export_env.sh
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo check
```

任何 clippy warning、失败测试、非预期 stderr、资源泄漏或 flaky 都是阻塞项。

### 2. 针对性真实 LLM TUI smoke test

最终验收必须使用 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md`，运行真实 `acn` TUI、真实provider/model 和真实 tool loop。允许使用受控本地 shell、PTY 和 MCP fixture 产生确定性副作用与结构化日志，但不得使用 fake provider、预录 assistant response、直接调用 manager 或修改生产代码来冒充真实 LLM/TUI 验收。

在 skill 的 `scripts/` 下提交可复用 scenario runner，并运行以下场景：

1. **统一执行 / PTY**：真实模型按明确提示启动短命令、超过 yield 的长命令和交互 PTY；验证`process_id`、`process_list`、多轮 `write_stdin`、最终输出与 ToolCell。
2. **explicit cancel / `/ps`**：在 initial yield 中按 Esc/Ctrl-C，验证 cooperative grace 为`100ms` 且本地收束不再无限等待、精确 background continuation 文案、进程继续、`/ps` 可见以及确认页 terminate 完整进程组；不把有界 teardown/journal flush 误判为必须在 100ms 内完成。
3. **steer / compact context**：长命令期间提交 steer，验证安全边界后新 turn；随后 compact 并发起新请求，验证模型收到权威 live/recent context，成功读取最终结果后不再注入。
4. **subagent ownership**：main 与至少两个真实 subagent 各自启动进程；模型工具视图隔离，root `/ps`聚合可见，terminate 一个 child process 不取消 child 或影响其他 owner。
5. **active `/mcp` lifecycle**：后台 `code_run` 与慢 MCP call 同时存在时打开 `/mcp` Reconnect；验证旧 MCP ToolCell 失败但当前 turn 继续、新 generation 可调用、后台进程和其他 server 不受影响。
6. **viewport / colors / confirmation**：制造超过 viewport 的 process/MCP 行，验证自动滚动、窄屏、Command body、固定 footer、y/n/Esc、running/terminating ANSI 状态色与 resize clamp。

每个场景必须：

- 定义 `initial`、操作中、取消/重连后、最终状态等 checkpoint，并用 `tui_capture` 保存。
- 对稳定 UI marker 使用 `rg` 断言；进程寿命、并发、终止和 reconnect 结论以 fixture 的 PID/PGID、generation、request interval 和退出记录为权威证据。
- 检查 `stderr.log` 为空，失败也清理 tmux session、fixture child 和临时配置。
- 不以模型最终文字“声称成功”作为通过条件。

### 3. code-review skill

代码、测试和 TUI scenario 通过后，使用 `.agents/skills/code-review/SKILL.md` 检查以下风险域：

- A：`src/tool/process/**`、工具注册与 config、Unix/PTY/resource tests。
- B：turn loop、tool boundary、provider runtime context、journal/compaction、delegation ownership。
- C：TUI `/ps`/`/mcp`、runtime wiring、tmux scripts 与端到端测试。

验收要求：三个风险域均已覆盖，不存在未处理的 correctness、resource lifecycle、取消竞态、死锁或数据丢失问题。

### 4. 最终完成门槛

只有同时满足以下条件才可交付：

- D1～D22 与 60 条验收项均有可追溯实现和证据，没有待拍板产品项。
- 完整 fmt/clippy/test/check 在最终代码上通过，macOS/Linux 核心矩阵通过。
- 所有 deterministic tmux 和针对性真实 LLM TUI smoke 通过，且无残留 session/process。
- code-review skill 完成，没有未处理的高风险结论。
- 最终实现不包含旧 runner、未使用兼容代码或测试专用生产后门。
