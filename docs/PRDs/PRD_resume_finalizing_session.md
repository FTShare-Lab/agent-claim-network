# PRD：Resume 接管 Finalizing Session

> 状态：已完成（2026-09-02，追加 ND-1、ND-2、ND-3、ND-4 并完成复验与外部复审）。

> 后续范围说明（2026-09-03）：Resume inbox 的失败分类、用户提示和统一恢复 `Open`
> 语义由 `docs/PRDs/PRD_inbox_failure_and_startup_recovery.md` 扩展；本文的 Finalizing
> 接管顺序、历史恢复、输入归属及 notice 前后空行语义继续有效。

## 背景

ACN 当前允许恢复未被占用的一致 `Open` session 和已经完成收尾的 `Closed`
session，但会拒绝所有 `Finalizing` session：

- `queued` / `running` Finalize 只能等待 Supervisor 完成；
- `failed` / `orphaned` Finalize 要求用户先运行
  `acn supervisor retry <session_id>`；
- Resume picker 不展示 `Finalizing` session，direct resume 也会在进入 TUI 前拒绝。

Recap 已经可以作为独立的 Supervisor job 在 Open session 中异步推进
`recapped_until`，并与 Finalize 共用 `finalize.lock`、checkpoint 和五次 job
attempt。基于这套能力，Finalize 在尚未到达不可逆 Prepared/Applied 边界时，不必继续阻塞
Resume：可以把同一个 Finalize job 原地降为 Recap，冻结当时的消息上界后重新打开 session。

本需求让正常、有效且包含真实用户输入的 `Finalizing` session 也能通过
`acn --resume [session_id]`、启动 picker 或会话内 `/resume` 恢复；用户不再需要先手动执行
Supervisor retry。真正由另一个前台进程执行的 Finalize 仍拒绝 Resume，并提示等待完成。

## 与既有 PRD 的关系

本文是后续覆盖型 PRD，不改写历史文档中的旧拍板文字：

- 覆盖 `PRD_interrupted_session_resume.md` 中“Finalizing session 不可 resume”的旧限制；
- 覆盖 `PRD_finalize_supervisor.md` 中 Finalizing direct resume 只提示等待或手动 retry 的旧语义；
- 扩展 `PRD_in_session_new_resume.md` 的 Resume 候选、轻量预检和 queued-input 失败处理；
- 保留 `PRD_recap_in_supervisor.md` 的 Recap 输入、动态起点、五次 job attempt、无通知、
  checkpoint 与全局 `Finalize > Recap` 优先级。

未被本文明确覆盖的既有语义继续有效，尤其包括：runtime lease 排他、Resume 历史只读、
handoff 后 history → inbox → Open、旧 session Finalizing 输入锁、interaction generation
隔离、Resume inbox warning、Finalize 通知 predicate 和 canonical messages 不重写。

## Main 合并后的协议基线（2026-09-01，非新增拍板）

本 PRD 固化后，工作分支 fast-forward 合入 main 的 `c930dc8` 与 `4b31a01`。两项提交新增
provider-neutral `InvalidToolUse { id, name, error }` canonical block：模型返回无法解析为 JSON
object 的工具参数时，不派发该工具，而是生成同 ID 的 `dispatch_failure` ToolResult，让合法
sibling tool 和当前 turn 继续推进。协议 replay 按各 provider 的安全形状保留或归一化，原始
非法参数不进入 `InvalidToolUse` canonical 字段。

该合并没有修改 Supervisor、Finalize engine、TUI、Resume CLI、session status 或 checkpoint
结构，因此不改变 D1–D14，也不产生新的产品拍板。它对本需求只有以下测试影响：

- Resume/Finalize/Recap 必须把 `InvalidToolUse` 当作普通已提交 canonical 内容，不能过滤、
  重写或再次派发；
- 冻结的 T、segment hash 和 checkpoint 校验自然包含该 block 及配对 ToolResult；
- Recap transcript 使用既有脱敏错误描述，不恢复或猜测原始非法参数；
- Finalizing Resume 后的历史加载、provider-safe replay、compaction projection 和真实后续 turn
  必须能够跨过这类历史继续工作。

## 目标

- 把符合现有 agent、真实输入和 metadata 一致性条件的 `Finalizing` session 纳入 Resume。
- Finalize 尚未 Prepared 时，由 Resume 抢占并把同一个 Supervisor job 原地转换为 Recap。
- Finalize 已 Prepared/Applied 或已经进入最终关闭提交时，不丢 checkpoint，完成关闭后自动
  reopen。
- Failed/Orphaned Finalizing 由 Resume 自动改投或恢复，不要求用户先执行
  `acn supervisor retry`。
- 转换失败时保持原 Finalize 可继续完成，目标不进入 Open，当前 TUI session 不开始
  handoff。
- 保持 Supervisor 现有全局串行队列、Finalize 优先级、Recap FIFO 和动态剩余区间语义。
- 对 direct resume、启动 picker 和会话内 `/resume` 使用同一套状态判断与接管逻辑。

## 非目标

- 不抢占或终止另一个前台 ACN 进程正在执行的 Finalize。
- 不允许恢复被活跃进程占用的 `Open` session。
- 不放宽 wrong-agent、空 session、无真实用户输入、metadata 不一致或损坏数据的校验。
- 不增加新的持久化 SessionStatus、job status、session generation、事务目录、多代
  checkpoint 或跨文件 CAS。
- 不合并多个 Recap job，不改变重叠 Recap 的动态剩余区间语义。
- 不改变 Recap 的消息输入，不让 Recap 消费 Finalize 专属 background-process
  completion。
- 不给转换后的 Recap 增加系统通知、成功 TUI 消息或完成订阅。
- 不移除 `acn supervisor retry`；该命令继续作为显式运维入口，只是不再是 Resume 的前置步骤。
- 不为掉电恰好发生在两次原子文件提交之间增加复杂事务恢复协议；正常错误返回路径必须保持
  可收敛。

## 术语与边界

- **目标 session A**：用户希望 Resume 的 session。
- **当前 session B**：会话内 `/resume` 发起时，当前 TUI 正在使用的 session；direct
  resume 没有 B。
- **T**：Resume 接管决定时冻结的 A 的 `metadata.message_count`。
- **Supervisor Finalize**：存在唯一未成功 Finalize job，由 Supervisor queued、running
  或标记 failed。
- **前台 Finalize**：没有可接管 Supervisor job，同时 `finalize.lock` 被真实执行者持有；
  现有诊断为 `RunningWithoutJob`。
- **Orphaned Finalizing**：metadata 为 `Finalizing`，没有唯一未成功 Finalize job，且
  `finalize.lock` 当前空闲。
- **不可抢占边界**：有效共享 checkpoint 已进入 `Prepared` / `Applied`，或者无 recap 工作的
  Finalize 已经赢得最终 Closed 提交权。边界后必须完成本轮关闭再 reopen。

## 已拍板语义（不可静默修改）

以下决策是本文的固定基线。实施中发现新的业务或用户可见选择时，必须先在本文末尾追加
“新增拍板记录”，写明原因、选项、选择和影响，再继续实现；不得删除、重写或弱化已有拍板。

### D1：Resume 资格与 picker

- Resume 继续要求 session 属于当前 agent、包含 canonical 真实用户消息或 journal 已接受的
  真实用户输入，并通过现有 metadata 一致性校验。
- 一致的 `Closed`、未占用的异常 `Open` 和 `Finalizing` 都进入 Resume 候选。
- Picker 中 `Finalizing` 使用既有状态名称展示，不在列表中展开 Supervisor job ID、attempt
  或错误详情；最终状态必须在选择后重新读取和校验。
- 被活跃进程占用的 `Open` session 继续不出现在 picker，direct resume 继续拒绝。
- `Finalizing` 可以出现在 picker；选择后若确认是前台 Finalize，则按 D7 拒绝本次 Resume。
- 空 session、wrong-agent、无真实输入和不一致 metadata 继续过滤或报错，不因本需求放宽。
- 现有按最新 activity 排序保持不变。

### D2：Resume Finalizing 的状态矩阵

| 目标状态 | Resume 行为 |
| --- | --- |
| `Closed` | 沿用现有 reopen |
| 一致 `Open`，runtime lease 可获取 | 沿用 Interrupted Resume |
| `Open`，runtime lease 被占用 | 拒绝 |
| Finalize `Queued`，无 checkpoint | 原 job 转成 Recap(T)，然后 Open |
| Finalize `Running`，尚未到不可抢占边界 | 请求抢占；worker 确认无副作用后原地转成 Recap(T)，然后 Open |
| Finalize 已 `Prepared` / `Applied` 或 Closed 提交已经获胜 | 不转换；完成关闭后 reopen |
| Finalize `Failed`，无 checkpoint | 原 failed job 转成 Recap(T)，然后 Open |
| Finalize `Failed`，有 checkpoint | 自动恢复同一 Finalize，完成关闭后 reopen |
| Orphaned，无 checkpoint | 有 recap backlog 时新建 Recap(T)，持久化后 Open；无 backlog 时直接 Open |
| Orphaned，有 checkpoint | 新建 Finalize recovery job，完成关闭后 reopen |
| 前台 Finalize | 报错并拒绝；用户等待完成后再次 Resume |

- Resume Finalizing 成功后的目标仍使用同一个 session ID、既有 messages、journal、system
  prompt、compaction state、local claims 和 background-process journal。
- Resume 不重写已有 canonical history 或 system prompt，只有后续正常 turn 才追加消息。
- 如果 Finalize 已经提交 `Closed`、但 job 尚未提交 `Succeeded`，按现有 stale job 对账语义把
  旧 job 视为成功，再从 `Closed` reopen；旧 job 不得关闭新的 Open 周期。

### D3：同一个 Finalize job 原地转换

- 存在唯一未成功 Finalize job 时，不删除旧 job、不创建 replacement job；在同一个 job
  YAML 中原地改变 kind。
- 转换后的 job 保持：
  - 相同 `id`；
  - 相同 `agent_id`、`session_id`；
  - 相同 `created_at`。
- 转换后的字段为：
  - `kind = Recap { session_id, recap_end_index: T }`；
  - `status = Queued`；
  - `attempts = 0`；
  - `manual_retries = 0`；
  - `started_at = None`、`finished_at = None`、`last_error = None`；
  - `notify_on_completion = false`；
  - `updated_at` 使用转换时间。
- Supervisor log 记录 job ID、session ID、原状态、T 和 `converted by resume before
  Prepared`；不增加 provenance 字段或新 job status。
- Failed Finalize 改成 Recap 后获得独立的五次 Recap job attempt；不继承已经耗尽的
  Finalize attempts，也不套用 `[agent.llm].retry_count`。

### D4：转换后的队列顺序与 Recap 范围

- 继续使用现有全局两级优先级：`Finalize > Recap`。
- Finalize job 转成 Recap 后立即失去 Finalize 优先级；此后到达的任意 session Finalize
  都排在它前面。
- 没有 Finalize 时，转换后的 job 以保留的 `created_at + job_id` 进入现有 Recap FIFO；
  更早的 Recap 仍先执行，不增加 Resume 专属优先级。
- Recap 的冻结终点为 T，执行起点始终读取当时最新 `recapped_until`：

  ```text
  [latest recapped_until, T)
  ```

- cursor 已到达 T 时成功 no-op；多个重叠 job 不合并，前一个完成后后一个只处理剩余区间。
- 转换后的 Recap 仍只读取 canonical messages；不消费 background-process completion。
  未消费 completion 保持原 cursor，等 session 之后真正 Finalize 时处理。

### D5：Running Finalize 的抢占边界与所有权

- Supervisor 为当前 Running Finalize 增加与现有 Running Recap 对称的进程内登记和抢占控制。
- Resume 请求只能在同 session 内抢占该 Finalize；不同 session 之间不取消 Running job。
- Running Finalize 的当前 worker 是抢占确认和 job 转换的唯一写入者：
  - 收到请求后，在 `finalize.lock` 等待、知识锁等待或模型准备期间，可以在 checkpoint
    `Prepared` 前停止；
  - worker 确认尚无持久副作用后，亲自把当前 job 原地转换为 Recap；
  - Resume 调用方不能与 worker 并发覆写同一个 Running job。
- 如果 checkpoint 已存在、Prepared/Applied 提交已经获胜，或者最终 Closed 提交已经获胜，
  抢占返回“不可抢占”；本次 Resume 自动等待原 Finalize 完成，不把它视为转换失败。
- Resume 触发的抢占不额外消耗 Finalize job attempt。转换失败后，worker 继续或重新执行同一
  logical attempt；不得因为抢占请求把原 Finalize 记为一次业务失败。

### D6：原地转换的原子性与失败语义

- Queued/Failed job 的转换在 Supervisor `lifecycle_gate` 内完成；Running job 由 worker 在
  同一生命周期边界内完成。
- 先在内存构造完整目标 job，再通过现有原子 YAML 替换提交。原子写返回失败时，磁盘上的
  job 仍是完整的原 Finalize，不允许出现半个 Finalize/半个 Recap。
- 只有 Recap job 已经持久化后，才允许把目标 session 从 `Finalizing` 改回 `Open`。
- job 转换成功但目标 Open 提交失败时，正常错误路径必须把同一 job 恢复为原 Finalize，
  再返回失败；不得留下 `Finalizing + Recap job` 的普通可复现状态。
- 上述补偿只复用现有原子 job/session 写和生命周期锁，不增加持久事务日志、CAS 或恢复目录。
- 改投动作失败时：
  - 目标保持 `Finalizing`；
  - 原 Finalize 完整保持转换前状态；queued/running 继续执行或重新排队，failed 仍可由之后的
    Resume 或显式 retry 处理；
  - 当前 session B 保持 `Open`，不开始 handoff；
  - TUI 显示：

    ```text
    Error: This session is still finalizing; wait for finalization to complete before resuming.
    ```

  - 不发送系统通知。
- 改投已经成功、之后 Recap 执行失败属于普通后台 Recap 失败：目标 session 保持 Open 且可
  继续使用；Recap 使用 Supervisor 五次 attempt；后续 compact 或 Finalize 继续覆盖 backlog；
  不显示 TUI 完成结果、不发送系统通知。

### D7：前台 Finalize 不抢占

- 如果目标 `Finalizing` 没有可接管 Supervisor job，且 `finalize.lock` 被真实前台执行者
  持有，本次 Resume 直接失败，不等待、不发送取消请求、不终止另一个进程。
- TUI 使用与 D6 相同的短错误，提示等待 Finalize 完成后重新 Resume；详细诊断只写日志。
- Direct resume 同样进入正常 TUI Resume 流程并显示该错误，不再要求用户运行
  `supervisor retry`。
- 前台 Finalize 之后成功 Closed 时，用户下次按普通 Closed Resume；如果执行者退出并形成
  orphaned，则下次按 D2 的 Orphaned 分支处理。

### D8：Failed/Orphaned checkpoint 自动恢复

- 存在有效 Prepared/Applied checkpoint 时不得改成普通 Recap、删除 checkpoint 或直接
  Open；必须兑现 checkpoint，完成本轮 Finalize 并关闭，再按现有 Closed Resume reopen。
- 一次 Resume 为 Failed/Orphaned checkpoint 自动提供一轮最多五次的 Supervisor Finalize
  recovery。Failed job 存在时复用同一 job；Orphaned 没有 job 时创建一个 recovery job。
- 自动 recovery 的单个 attempt 继续优先恢复 checkpoint，不重新调用 recap 模型。
- 这轮五次 attempt 再次耗尽时，本次 Resume 失败，session 保持 Finalizing；用户之后再次
  Resume 可以主动发起新一轮，不需要先运行 `acn supervisor retry`。
- 自动 Resume recovery 的触发来源、reset 和最终结果写入 Supervisor log；不增加新的
  resume-retry 持久字段。
- Recovery Finalize 保留原 job 的通知设置；Orphaned 新建 recovery job 时使用当前配置的
  Finalize 通知设置，并继续受 D13 的真实完成 predicate 约束。
- 不做无限重试，不让一次 Resume 永久等待不可恢复的磁盘、配置或上传错误。

### D9：Orphaned 无 checkpoint

- Orphaned 且没有 checkpoint 时，没有 job 可以原地转换。
- 如果 `recapped_until < T`，先创建一个新的 Recap(T) job；只有 job 成功持久化后才把
  session 改回 Open。
- 如果 `recapped_until >= T`，不创建空 Recap job，直接把 session 改回 Open。
- background-process completion cursor 保持不变；下一次真正 Finalize 继续消费。
- 新 Recap job 使用普通五次 attempt、`notify_on_completion = false` 和普通 Recap FIFO。
- 新 job 创建失败时不修改 session；新 Recap 已创建但 Open 提交失败时，把该 job 原地改成
  queued Finalize recovery，再返回失败。两种情况都保持可恢复的 Finalizing 语义，不把
  session 暴露给 TUI。

### D10：目标等待发生在当前 session handoff 之前

- 会话内 `/resume` 选择 Finalizing 目标后，先建立既有 interaction generation 早边界，
  再取得目标 runtime lease、重读 metadata 并执行接管/等待。
- 目标尚未成功变为可用 Open 前，当前 session B 不进入 Finalizing、不投递自己的
  Finalize，也不释放 runtime lease。
- 目标已经 Prepared/Applied 或正在 checkpoint recovery 时，B 保持 Open，页面显示：

  ```text
  Resuming · Waiting for target finalization
  ```

- 等待成功后才进入既有 B handoff：B 的旧页面显示正常
  `Finalizing · Committing contribution` 并禁用输入；handoff 成功后安装 A。
- 这样 A 恢复失败时不会同时失去 B；不增加跨 session 回滚或自动切回。

### D11：等待期间 queued input 的归属与失败清除

- A 的接管/等待期间，B metadata 虽保持 Open，但新输入只进入现有进程内 queued input，
  不追加到 B 的 canonical messages，也不派发模型或工具。
- A 恢复成功后，queued input 保留原顺序，在完成 B handoff、A 历史恢复和 A inbox 后，
  等 A 进入 Open 再派发给 A。
- A 的 Finalizing 接管/等待失败时，期间积累的 queued input 全部清除：
  - 不恢复到 B composer；
  - 不发送给 B 或 A；
  - 不持久化为任何 session 消息；
  - TUI 错误明确说明 queued input 已被丢弃，避免用户误以为仍在队列。
- 本条只覆盖选择 Finalizing 目标后进入接管/等待的失败。普通 Closed/Open Resume 的轻量
  metadata 预检失败继续沿用既有“恢复到当前 composer”语义。
- 不增加等待取消协议；Ctrl+C/退出请求沿用现有“session switch 正在运行，请等待”的处理。

### D12：成功后的 Resume startup 保持现有语义

- Direct resume、启动 picker 和会话内 `/resume` 最终都复用同一个 Finalizing 接管结果，
  不维护三套状态判断。
- 目标成功 Open 后，继续按现有 Resume 顺序：

  ```text
  欢迎页
  → 只读加载并显示目标历史、context usage、local claims
  → 可见执行目标 inbox
  → Open
  → 派发 queued input
  ```

- Resume 不重建或替换目标 system prompt、messages、journal 或 provider history。
- Resume inbox 失败继续显示既有带前后空行的固定 warning，目标仍进入 Open，可使用
  `/inbox` 重试。
- 安装 A 后继续使用当前 ACN 进程已经解析的 cwd、配置、upstream、agent、model、provider、
  MCP 和工具运行时。
- B 的晚到 Recap enqueue、copy、附件预览、剪贴板图片等结果继续由现有 session ID / interaction
  generation 边界隔离，不能进入 A 的 transcript。

### D13：通知与用户可见结果

- 原地转换成 Recap 的 job 不发送成功或失败系统通知。
- Finalize 已越过不可抢占边界、实际完成关闭时，继续使用现有
  `finalized_unrecapped_messages` 成功通知 predicate；Resume 正在等待不抑制真实 Finalize
  通知。
- Failed/Orphaned checkpoint recovery 最终实际完成 Finalize 时也沿用同一通知 predicate。
- 已完全 recap、只快速关闭的 Finalize 继续不通知。
- 接管/等待错误只显示在发起 Resume 的 TUI，并写详细日志；不额外发送系统通知。
- 旧 Finalize job 在目标 Open 后不得再把成功/失败文本写入该 session 的 transcript。

### D14：Supervisor retry 命令兼容

- `acn supervisor retry <session_id|job_id>` 保持现有显式运维语义和参数兼容。
- 未发起 Resume 时，Failed Finalize 仍可由用户主动 retry。
- Resume 自动接管 Failed/Orphaned 后，不再向用户输出必须执行该命令的提示。
- Finalize job 已原地转成 Recap 后，旧 job ID 的 kind 已是 Recap；它不再接受
  Finalize-only retry。后续失败按普通 Recap backlog 语义由 compact/Finalize 覆盖。

## 状态与时序

### Supervisor Finalize 在 Prepared 前被 Resume

```text
A: Finalizing + Finalize job queued/running
  → Resume 冻结 T = message_count
  → 取得 A runtime lease
  → Supervisor/worker 确认尚未 Prepared
  → 同一 job 原子改成 queued Recap(T)
  → A metadata: Finalizing → Open
  → Resume 正常恢复 A 历史与 inbox
  → Recap 按全局队列异步执行 [latest recapped_until, T)
```

### Supervisor Finalize 已 Prepared/Applied

```text
A: Finalizing + checkpoint Prepared/Applied
  → Resume 取得 A runtime lease
  → 不抢占、不删除 checkpoint
  → 等待原 Finalize 或一次自动 recovery 完成
  → A: Closed
  → 沿用 Closed reopen，A: Open
  → Resume 正常恢复历史与 inbox
```

### 会话内从 B Resume A

```text
B: Open
  → picker 选择 Finalizing A，建立 interaction generation 早边界
  → B 保持 Open；A 接管/等待；新输入仅进入 queued input
  → A 失败：B 保持 Open，清空本次等待期 queued input，显示错误
  → A 成功：B 进入正常 Finalizing，输入禁用
  → B handoff 成功
  → 清空旧页面并安装 A
  → A history → inbox → Open
  → 派发等待期 queued input
```

### 转换后的队列示例

```text
原队列：A Finalize(running), C Recap(older), D Recap(newer)
Resume A 成功转换：A Recap(created_at 保持不变)

如果随后 B Finalize 到达：
  B Finalize → Recap FIFO

没有新的 Finalize 时：
  C/A/D 按各自原 created_at + job_id 排序
```

## 实现边界

- Session store 负责候选过滤、runtime lease 和 metadata 状态提交；增加能够“保留
  Finalizing、先取得 lease”的薄 reservation，不把 Finalizing 直接当 Closed reopen。
- Supervisor 提供单一 Resume takeover 请求/入口，负责诊断唯一 job、转换、running worker
  抢占、orphan recovery 和结果分类；TUI 不直接改 Supervisor job YAML。
- Finalize engine 暴露明确的“Prepared 前被 Resume 抢占”结果，不能把抢占伪装成普通成功
  report，也不能发送 Finalize 成功通知。
- TUI runtime worker 编排 Finalizing target 的 reservation、takeover/wait、Open 和错误；App
  继续负责 B handoff、页面切换、queued input 与 interaction generation。
- Direct resume 的旧前置拒绝逻辑改为允许有效 Finalizing 进入同一 TUI worker 流程；CLI 不在
  TUI 之外复制接管状态机。
- 所有 job mutation 继续走 Supervisor 生命周期锁和既有原子 YAML helper。
- 不让 TUI 轮询任意旧 job；只等待本次明确选中的 A 的 takeover/recovery 结果。

## 分阶段实施 Planning 与验收

### 阶段切换硬约束

- 每次从 PRD 固化进入实现、从一个实现阶段进入下一个阶段、从实现进入验证、从验证进入
  真实 TUI smoke、从 smoke 进入 code review，以及 review 修复后进入复验前，主执行者必须
  完整重读本 PRD。
- 重读后逐条检查当前代码和下一步是否仍符合 D1–D14；不得凭记忆继续。
- 实施中产生新的业务或用户可见选择时，必须先在本文“新增拍板记录”末尾追加原因、选项、
  最终选择和影响，再继续修改代码。
- 新记录只能追加；不得删除、重写或改变 D1–D14 及此前新增拍板的语义。

### Phase 0：基线与可测试边界

计划：

- [x] 重读本 PRD并核对当前 Session store、Resume picker/direct preflight、Supervisor job、
  Finalize checkpoint/preemption 和 TUI switch continuation。
- [x] 固化状态矩阵、转换字段和用户可见文案的纯函数/DTO 测试入口。
- [x] 把已合入 main 的 `InvalidToolUse + dispatch_failure ToolResult` 作为 canonical 基线加入
  Resume/Recap/checkpoint 测试数据，不复制协议 adapter 的解析逻辑。
- [x] 确认工作区基线与既有用户修改，避免覆盖无关变更。

验收：

- [x] 当前代码与旧行为的差异被定向测试明确捕获：Finalizing 当前仍被过滤和拒绝。
- [x] 实施范围只涉及 Resume/Session/Supervisor/Finalize/TUI 及对应文档测试。

### Phase 1：Session 候选与 Finalizing reservation

计划：

- [x] Resume list 纳入符合 D1 的 Finalizing session，保持空 session、wrong-agent、不一致
  metadata 和活跃 Open 过滤。
- [x] 增加薄的 Finalizing target reservation：取得 runtime lease 后重读 metadata，但不提前
  mark Open、不删除 checkpoint。
- [x] 保持 Closed/Interrupted 的既有恢复实现和历史只读语义。
- [x] 让 direct resume、picker 和会话内 `/resume` 复用同一目标资格判断。

验收：

- [x] 有真实输入的 Queued/Running/Failed/Orphaned Finalizing 出现在 picker。
- [x] 前台 Finalizing 可以显示，但选择和 direct resume 均在最终预检时明确拒绝。
- [x] 活跃 Open、空 session、wrong-agent、metadata 异常仍不能 Resume。
- [x] 两个进程竞争同一目标 runtime lease 时只有一个进入接管流程。

### Phase 2：Supervisor 原地转换与 Running Finalize 抢占

计划：

- [x] 增加单一 Resume takeover IPC/内部入口和结构化结果，不让 TUI 猜 job 状态。
- [x] Queued/Failed Finalize 在 lifecycle gate 内按 D3 原地转换。
- [x] 登记 Running Finalize，并让当前 worker 在 Prepared/Closed commit 前响应同 session Resume
  抢占；Prepared/Applied/Closed commit 获胜时返回必须等待。
- [x] 保证转换保留 ID/created_at、重置 retry 字段、关闭通知，并保持现有优先级排序。
- [x] 实现正常错误路径的原 job 保持/恢复，不增加持久事务协议。
- [x] 增加转换、抢占、竞态和故障注入测试。

验收：

- [x] Queued、Running-before-Prepared、Failed-without-checkpoint 都转换同一个 job，不创建
  replacement。
- [x] 转换后到达的 Finalize 全局优先执行；没有 Finalize 时按原 Recap FIFO 执行。
- [x] 原子 job 写失败时旧 Finalize YAML 字节语义完整，session 仍 Finalizing。
- [x] Running 转换失败后原 Finalize 继续/重排，Resume 抢占不额外吃掉 attempt。
- [x] Prepared/Applied 或 final close commit 获胜时不转换、不丢 checkpoint。
- [x] 转换成功后的 Recap 使用动态起点和冻结 T，五次失败不影响 Open session。
- [x] 含 `InvalidToolUse` 与配对失败 ToolResult 的 segment 可以稳定 hash、转换、Recap 和恢复；
  不重新派发非法工具，也不在 canonical/recap 中补造原始非法参数。

### Phase 3：Failed/Orphaned recovery 与 Open 提交

计划：

- [x] Failed + checkpoint 自动重置同一 Finalize job并提供一轮五次 recovery。
- [x] Orphaned + checkpoint 创建 recovery Finalize job并等待 Closed。
- [x] Orphaned + no checkpoint 根据 backlog 创建 Recap(T) 或直接 Open。
- [x] 转换/新 Recap 持久化成功后再提交 `Finalizing → Open`；Open 写失败执行最小 job 回滚。
- [x] 保留 background completion cursor，确保未来 Finalize 继续消费。
- [x] 保留显式 `supervisor retry` 的既有兼容行为。

验收：

- [x] Failed/Orphaned 无 checkpoint 不再要求 CLI retry，Resume 后可立即继续对话。
- [x] Prepared/Applied checkpoint 被恢复而不是重算或丢弃，完成 Closed 后正常 reopen。
- [x] 一轮五次 recovery 耗尽时 Resume 报错、session 仍 Finalizing；再次 Resume 可以新开一轮。
- [x] 日志能够区分 resume-triggered recovery，且没有新增无必要的 retry 持久字段。
- [x] Open 提交注入失败时不会稳定留下 `Finalizing + Recap`，原 Finalize 仍可推进。

### Phase 4：TUI 与 direct resume 编排

计划：

- [x] Finalizing target 接管/等待期间让 B 保持 Open，显示固定 Resuming activity，并把输入放入
  既有 queued input。
- [x] 成功后沿用 B Finalize handoff，再按 A history → inbox → Open 安装目标并派发队列。
- [x] 接管/等待失败时清空本次队列，不恢复 composer，并用短错误明确提示丢弃。
- [x] 普通 Closed/Open Resume 预检失败继续恢复 composer，不受 D11 影响。
- [x] Direct resume 进入同一 TUI 接管流程，删除旧的 Finalizing 专用 CLI retry 提示。
- [x] 保持旧 session 晚到事件的 session/generation 隔离和现有 Resume inbox warning 排列。

验收：

- [x] B 在 A 接管/等待成功前保持 Open，未进入 Finalizing、未释放 lease。
- [x] 等待页准确显示 `Resuming · Waiting for target finalization`。
- [x] 等待输入成功时只进入 A；失败时 B/A messages 均没有该输入且 composer/queue 已清空。
- [x] A 成功后 B 才显示正常 Finalizing 输入锁，随后 A 历史先于 inbox 展示。
- [x] Direct resume、picker 和会话内 `/resume` 对每种 Finalizing 状态得到一致结果。
- [x] 前台 Finalize 只显示短错误，不被取消、不要求 Supervisor retry。
- [x] A 的历史包含 `InvalidToolUse` 时仍能完整渲染并继续真实 turn，Resume 不改写该 block 或
  既有配对 ToolResult。

### Phase 5：稳定文档、完整验证与 TUI 自动 smoke

计划：

- [x] 更新 README、用户指南、架构及受覆盖 PRD 的后续范围说明；保留历史拍板原文。
- [x] 按 `.agents/skills/verify/SKILL.md` 完整执行版本一致性、fmt、Clippy、全部测试和 check。
- [x] 按 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md` 执行 canonical tmux smoke，并补
  Finalizing Resume 的确定性 tmux 场景。
- [x] 检查 stderr、tmux/Supervisor 清理、job YAML、session metadata 和日志。

验收：

- [x] 所有新增与既有测试通过，Clippy 无 warning，格式和版本一致性通过。
- [x] Canonical TUI 启动/help/退出 smoke 无回归，stderr 为空。
- [x] 确定性 tmux capture 覆盖等待 activity、queued input 成功归属、失败清除和前台 Finalize
  拒绝。
- [x] 稳定文档不再把 Finalizing Resume 描述为一律禁止或必须手动 retry。

### Phase 6：真实 LLM TUI Smoke Test

整体实现和自动验证完成后，必须使用真实 LLM、真实 ACN TUI、隔离的真实 Supervisor 和独占
临时 `acn_home` 完成针对性 smoke；fake provider 只能补确定性测试，不能替代本阶段。

计划：

- [x] 建立包含可验证临时代号和多轮上下文的真实 A，再构造 Finalizing job；从真实 B 的
  `/resume` picker 选择 A。
- [x] 覆盖至少一次 Prepared 前原地转换，证明 A 在 Recap 完成前已经 Open、历史可续接、真实
  turn 能引用 A 的既有上下文。
- [x] 使用真实已提交或受控构造的 `InvalidToolUse + dispatch_failure ToolResult` 历史恢复 A，
  随后的 provider 请求和回答必须使用真实 LLM，证明该 canonical 历史不会阻断 Resume、Recap
  或后续 turn；不要求真实模型在 smoke 中随机再次生成非法参数。
- [x] 在等待窗口提交 queued input，证明它不进入 B，最终只由 A 的真实模型处理。
- [x] 构造一次目标恢复失败，证明 queued input 被清空、B 保持 Open、错误文案准确。
- [x] 覆盖 Prepared/Applied 等待或 checkpoint recovery，证明 Finalize 先 Closed、再 reopen，
  不重复生成 recap 结果。
- [x] 覆盖前台 Finalize 拒绝，证明目标执行者不被取消，完成后可以再次 Resume。
- [x] 在转换后再投递另一个 session 的 Finalize，结合 Supervisor log/job 文件证明后者优先于
  Recap；无 Finalize 时证明 Recap 按原 FIFO。
- [x] 检查系统通知条件、job kind/ID/created_at/attempt、recapped_until、session status、
  messages/journal、captures、stderr 和全部进程/锁清理。

验收：

- [x] 真实 LLM 场景逐条符合 D1–D14，未依赖 fake provider 冒充真实行为。
- [x] 转换后的 Recap 失败或仍在排队时，A 的真实对话保持可用。
- [x] A/B canonical messages 没有跨 session 输入或晚到结果污染。
- [x] 所有 capture 可读、stderr 为空，tmux、隔离 Supervisor 和临时 runtime 均清理。

### Phase 7：Code Review、修复、复验与最终 PRD 对齐

完整实现、自动验证和真实 LLM TUI smoke 完成后，必须使用
`.agents/skills/code-review/SKILL.md`：先做本地多轮 review，再执行独立只读外部 review。

Review 聚焦：

- [x] Running Finalize 抢占与 Prepared/Closed commit 竞态；
- [x] job 原地转换的唯一写入者、原子失败和 Open 失败回滚；
- [x] Finalize/Recap 优先级、attempt、通知和 stale recovery；
- [x] runtime lease、finalize.lock、lifecycle gate 的顺序与 async 锁边界；
- [x] Failed/Orphaned checkpoint 恢复与 background completion cursor；
- [x] direct/picker/in-session 三入口一致性；
- [x] B handoff 时点、queued input 成功归属/失败清除和旧事件隔离；
- [x] 是否出现为了极小概率而引入的过度防御、复杂事务或无现实收益的恢复层。

修复与复验要求：

- [x] 修复所有具有现实触发条件、实质影响且不属于过度防御的 P0/P1。
- [x] 不为低概率理论问题扩展为多代 checkpoint、持久事务、通用 CAS 或跨进程取消框架。
- [x] 每次 review 后只要修改了实现，就重新运行受影响定向测试、完整 verify、canonical tmux
  smoke 和相关真实 LLM TUI smoke。
- [x] 每次修复后再次执行本地 review 和独立只读外部 review；若仍有非过度防御 P0/P1，继续
  修复、复验和复审，直到闭合。
- [x] 最后完整重读本 PRD，逐条核对 D1–D14、所有新增拍板、代码、测试、真实 TUI 证据和稳定
  文档；只有全部对齐后才能把 PRD 状态改为“已完成”。

最终验收：

- [x] 无遗留的非过度防御 P0/P1。
- [x] 最终一次代码修改之后已有新的验证和外部 review 结论，而不是复用修改前结论。
- [x] 实现未静默改变旧拍板，新增拍板记录完整，稳定文档与最终行为一致。
- [x] 最终交付说明能够完整描述每种 Resume 状态、队列顺序、失败处理、TUI 输入归属和通知
  语义。

本轮外部复审待闭环项（不改变 D1–D14）：

- [x] Resume 成功先于输入预处理结果返回时，等待期 submission 仍按 D11 进入 A，不因 B 已
  Finalizing 而丢失。
- [x] 持久 job 为 Running 但已无匹配进程内 worker 时，Supervisor 在 lifecycle gate 内按既有
  attempt 语义收敛后继续接管，不让 Resume 无限等待。
- [x] 目标 A 已接管、当前 B 的 handoff Finalize 随后失败时，仍按 D11 清除等待期已排队和晚到
  异步输入并明确提示，不让输入滞留或被静默吞掉。
- [x] Running Finalize 在 Prepared 前恰好结束为失败时，等待中的 Resume 在 worker 持久化
  Queued/Failed 并释放运行登记后重新执行同一 takeover 判定；无 checkpoint 时原 job 转为
  Recap，不错误停留在 Wait/五次重试耗尽。

## 新增拍板记录

当前无新增拍板。实施中如出现本文未覆盖的选择，只能按以下模板追加：

```text
### ND-N：标题（日期）

原因：

问题：

选项：

- A：...
- B：...

选择：...

原因：...

影响：...
```

### ND-1：Picker 内联显示 Finalizing Resume 错误（2026-09-02）

原因：

会话内 `/resume` 或启动 Resume picker 中，用户已经明确选择了某个 session；当前实现却在
失败后关闭 picker，并把错误写进当前 session transcript，错误与目标行失去对应关系，而且会
在退出 picker 后继续留在主页面。

问题：

Finalizing 目标无法 Resume 时，错误应显示在主 transcript，还是回到 picker 并锚定失败行？

选项：

- A：保持现状，关闭 picker，把错误写入当前 session transcript。
- B：恢复本次 picker，在失败 session 行正下方插入临时错误行。

选择：B。

原因：

错误属于被选择的目标 session，列表内锚定能直接说明哪一行失败，也允许用户立即选择其他
session，不污染当前 session transcript。

影响：

- 错误文本固定为
  `Error: This session is still finalizing; wait for finalization to complete before resuming.`；
- 错误行紧跟失败 session，不加空行，使用明显缩进和红色样式；后续 session 行自然下移；
- picker 选择索引仍只包含 session，错误行不可选；失败后光标保留在原 session；
- picker 关闭后错误销毁，重新进入 `/resume` 或启动 picker 时不再显示；再次选择目标时先清除
  旧错误；
- 接管/等待期间仍沿用 D10–D11 的主页面 Resuming activity 和 queued-input 数据归属；失败时仍
  清空等待期输入，但按用户最新拍板，不显示
  `Queued input entered while resuming was discarded.`，本条仅覆盖 D11 的用户提示要求，不改变
  输入不写入 B/A、不恢复 composer 的数据语义；
- 当前 session B 保持 Open；该错误不写入 B transcript。

### ND-2：Direct Resume 的 Finalizing 拒绝直接报错退出（2026-09-02）

原因：

`acn --resume <session_id>` 已经给出唯一目标；目标正由另一个前台进程 Finalize 或以同一短错误
拒绝接管时，当前实现会留下一个没有可用 session 的 Error TUI 页面，用户只能再手动退出。

问题：

Direct Resume 遇到该 Finalizing 错误后，应停留在 TUI，还是恢复终端并退出？

选项：

- A：保持现状，在 TUI 中显示错误并等待用户退出。
- B：恢复终端，向 stderr 输出错误并以非零状态退出。

选择：B。

原因：

direct 命令没有其他 picker 目标可选，继续停留没有可操作收益；终端错误和非零退出也更适合
脚本与人工判断。

影响：

- 仅覆盖 D7 中 direct resume 失败后的承载方式；Finalizing 的接管判断、错误文本和不抢占前台
  Finalize 的语义不变；
- direct resume 仍复用同一 TUI worker/Supervisor 接管状态机，不在 CLI 复制判断逻辑；
- 错误返回后先恢复终端，再由 `acn` 向 stderr 输出
  `Error: This session is still finalizing; wait for finalization to complete before resuming.` 并非零退出；
- direct resume 成功、需要等待 Finalize 后成功恢复，以及无 session ID 的启动 picker 均保持
  原语义。

### ND-1/ND-2 实施与验收

- [x] Picker 保存本次选择器状态；Finalizing 接管失败时恢复同一列表并设置 session-scoped
  临时错误，不写当前 transcript。
- [x] Picker 渲染红色缩进错误行，错误行不参与上下移动或 Enter，Esc/重新打开后清除。
- [x] Direct `--resume <session_id>` 在相同错误上恢复终端、stderr 报错并非零退出。
- [x] 保持 Resuming activity 的现有位置、等待期输入归属/清除和成功 Resume 顺序。
- [x] 增加单元测试，并更新确定性及真实 LLM tmux smoke 对 picker 行位移、光标、Esc 清除、
  direct 退出和 waiting activity 做断言。
- [x] 完整执行 verify、canonical TUI smoke、针对性真实 LLM smoke 和 code-review skill；修复并
  复验所有非过度防御的 P0/P1。

### ND-3：Resume 失败不显示 queued-input 丢弃提示（2026-09-02）

原因：

ND-1 已取消目标接管失败时的
`Queued input entered while resuming was discarded.`，本地 review 发现当前 session handoff
Finalize 随后失败时仍会从另一个出口显示同一句话。用户明确要求不显示该提示，不应因失败发生
在接管前后而产生两套用户文案。

问题：

等待期 queued input 按既定语义清除后，是否在其他 Resume 失败出口继续显示单独的丢弃提示？

选项：

- A：仅目标接管失败不显示，B handoff Finalize 失败仍显示。
- B：所有 Resume 失败出口都不显示该句，保留输入清除的数据语义。

选择：B。

原因：

用户已明确该句不需要；失败本身已有对应错误，重复增加一条系统消息没有额外操作价值。

影响：

- 目标接管/等待失败以及 B handoff Finalize 失败都不显示该句；
- 等待期输入仍不恢复 composer、不写入 B/A、不继续派发；
- Finalize 的主错误、session 状态和恢复边界保持不变。

### ND-4：前台 Finalize 拒绝与等待提示携带目标 session（2026-09-02）

原因：

Direct `--resume <session_id>` 在目标正由另一个前台进程 Finalize 时虽然会恢复终端并非零退出，
但首帧欢迎页已经写入终端 scrollback，最终看起来像是启动过一个 TUI；同时 direct stderr、picker
行内错误和 Resuming 框正文都没有指出具体目标 session。

问题：

前台 Finalize 的立即拒绝和等待界面是否应保留现有通用文案与欢迎页？

选项：

- A：保持现状，先渲染欢迎页，并使用不含目标 ID 的通用文案。
- B：Direct 立即拒绝不把欢迎页写入终端，前台拒绝错误和等待框正文携带目标 session ID。

选择：B。

原因：

Direct 命令在这种失败下没有可继续操作的页面；只留下带目标 ID 的 stderr 更符合命令行预期。
等待界面明确目标 ID，也能避免用户把当前 Open session 与正在等待的 Resume 目标混淆。

影响：

- 前台 Finalize 专用错误为
  `Error: <session_id> is still finalizing foreground; Try again after its completion.`；其中
  `<session_id>` 使用实际目标 ID；
- picker 在目标 session 行下方显示同一条红色缩进错误；Direct `--resume` 则先恢复终端，向
  stderr 输出同一错误并非零退出；
- Direct `--resume` 在前台拒绝前不把欢迎页写入原生 scrollback；等待期间仍可显示临时 live
  状态，恢复终端时清除，最终可见输出只保留错误；
- Direct Resume 成功时仍在安装目标 session 前恢复正常欢迎页、历史、Inbox 与 Open 顺序；
- Resuming 框标题仍为 `Resuming · Waiting for target finalization`，框内 activity 改为
  `Target resume <session_id> finalizing foreground...`；底部当前 session 状态与 queued-input
  提示语义不变；
- 只有 Supervisor 明确返回“无 job、目标被前台 Finalize 占用”时使用 foreground 专用错误；
  checkpoint 恢复、转换和其他 Finalizing Resume 失败继续使用既有通用错误，避免误报；
- 本拍板只修改错误承载和 TUI 文案，不改变 Finalize/Recap job、checkpoint、状态机、等待与输入
  归属语义。

### ND-4 实施与验收

- [x] 区分 Supervisor job 等待与前台 Finalize 占用，向 TUI 返回目标 ID 和明确失败类型。
- [x] Direct 前台拒绝抑制欢迎页落入 scrollback；成功或其他可继续 TUI 的路径恢复原显示。
- [x] Picker 前台拒绝与 Direct stderr 使用动态目标 ID；其他失败保持原通用文案。
- [x] Resuming live activity 使用动态目标 ID，保留标题、local claims 与底栏既有语义。
- [x] 增加单元测试并更新确定性及真实 LLM tmux 场景；执行完整 verify、针对性 TUI smoke 和
  code-review skill，修复并复验所有非过度防御的 P0/P1。

### ND-5：等待 activity 不标记 foreground（2026-09-02）

原因：

`ResumeFinalizingStarted` 在用户选择元数据为 `Finalizing` 的目标后、Supervisor 完成具体状态
诊断前发出。该 activity 既可能短暂覆盖尚未 Prepared、随后可原地转换的 Finalize，也会在
Prepared/Applied 或 checkpoint recovery 必须等待时持续显示；它不能证明目标正由不可抢占的
前台 Finalize 占用。

问题：

Resuming 框内 activity 是否继续显示 `foreground`？

选项：

- A：保留 `Target resume <session_id> finalizing foreground...`。
- B：改为 `Target resume <session_id> finalizing...`，只表达目标仍处于 Finalizing。

选择：B。

原因：

等待 activity 应描述当前可确认的状态，不应提前展示尚未完成的诊断结论。真正不可抢占的前台
Finalize 仍由 Supervisor job 与锁诊断决定，并继续使用已有的 foreground 专用拒绝错误。

影响：

- Resuming 框内 activity 为 `Target resume <session_id> finalizing...`；
- activity 的触发时机、框标题、底栏、queued input、接管、等待和拒绝语义均不变；
- 前台 Finalize 的红色 picker 错误与 Direct `--resume` stderr 文案保持不变。

### ND-6：Direct Resume 成功后保留本次 MCP 启动 warning（2026-09-02）

原因：

Direct `--resume <session_id>` 是新的 ACN 进程，本次进程会重新初始化 MCP。为保证前台
Finalize 立即拒绝时终端只留下 Resume 错误，启动 scrollback 会暂时隐藏；但成功接管目标时，
目标页面的 session reset 会同时清除尚未展示的 MCP 启动 warning。结果是 MCP 仍为 Failed、
工具仍不可用，而用户没有看到本次进程的失败提示。

问题：

Direct Resume 成功后是否补回本次进程生成的 MCP 启动 warning，以及它与 queued input 的
显示顺序如何定义？

选项：

- A：不补回；用户通过 MCP 状态页自行发现失败。
- B：只为 Direct Resume 暂存启动 warning；目标历史和 Inbox 完成后显示 warning，再派发等待期
  queued input。会话内 `/resume` 不重复显示进程启动时已经展示过的 warning。

选择：B。

原因：

MCP 初始化失败属于当前进程，而不是持久 session 历史。Direct Resume 成功时本次 warning 尚未
展示，应与普通启动保持一致；会话内 `/resume` 仍是同一进程，不需要在每次切换后重复提示。

影响：

- Direct Resume 成功时，在恢复流程最后一条状态之后空一行显示 MCP startup warning，再空一行
  进入输入区；多个 MCP warning 作为同一个 warning block 连续显示；
- 如有等待期 queued input，先同步绘制 warning block，再从队列取出并派发到目标 session；
- Direct Resume 的前台 Finalize 立即拒绝仍只向 stderr 输出 Resume 错误，不显示欢迎页或 MCP
  warning；
- 会话内 `/resume`、MCP 生命周期、工具暴露状态、session 历史和消息持久化语义均不变。
