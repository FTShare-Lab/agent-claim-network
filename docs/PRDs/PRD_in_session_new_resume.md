# PRD：TUI 会话内 `/new` 与 `/resume` 切换

> 状态：已完成（2026-08-31；ND-1 至 ND-8 已实现并完成验证与外部复审）。

> 后续范围说明（2026-09-01）：Finalizing 目标的候选、接管/等待和等待期 queued input 失败语义，由 `docs/PRDs/PRD_resume_finalizing_session.md` 扩展；本文其余 `/new`、`/resume` handoff 与目标启动顺序继续有效。

## 背景与问题

当前 TUI 不支持 `/new`；`/resume` 只允许在启动后尚无真实内容的临时空 session 中使用。非空 session 中执行 `/resume` 会要求用户先 `/exit`，而 `/exit` 的 Finalize 成功投递 supervisor 后会直接结束整个 TUI。

现有代码已经具备本需求所需的主要能力：

- session 持久状态只有 `Open`、`Finalizing`、`Closed`；
- Resume 会重新获取目标 session 的 `runtime.lock`，并支持 Closed 与异常中断的 Open session；
- Finalize 可以在 TUI 中完成快速关闭，也可以投递 supervisor 后台处理未 recap 内容；
- supervisor 已经处理同 session Recap/Finalize 的优先级、抢占、checkpoint 与重试；
- TUI 已有 resume 历史恢复、输入队列、session 视图 reset 和空临时 session 删除逻辑。

本需求在同一个 TUI 进程中增加 session 切换编排：`/new` 创建并切换到新 session，`/resume` 在任意空闲 session 中选择并切换到历史 session；被切走的当前 session 沿用正常 Finalize 语义收尾。

本 PRD 替换 `docs/PRDs/PRD_interrupted_session_resume.md` 中“会话内非空 session 的 `/resume` 限制保持不变；本期不实现会话内切换，也不实现 `/new`”的旧范围限制。旧 PRD 的异常退出恢复、runtime lease 与候选筛选语义继续有效。

## 目标

- 增加原生 `/new`，无需退出 TUI 即可进入新的 session。
- 允许非空当前 session 使用 `/resume` picker 并切换到目标 session。
- 当前 session 按 `Open → Finalizing → Closed` 收尾；需要 recap/background completion 时由 supervisor 后台完成。
- Finalize job 成功持久化入队后即可切换，不等待后台执行完成。
- `/new` 只有在新 session 已成功准备并取得 handle/runtime lease 后才刷新 TUI；准备失败时旧页面和旧 session 保持原样。
- `/new` 切换时重新渲染正常欢迎页，并应用新 session 的正常 inbox/startup 结果。
- `/resume` 保持现有 picker、历史恢复、context/local claims 刷新与 Interrupted/Closed 语义。
- 复用现有 Finalize、Supervisor、session store、runtime lease、输入队列和 TUI state，不创建第二套生命周期协议。

## 非目标

- 不增加新的持久化 session 状态、切换事务、CAS、恢复目录或 reservation 文件。
- 不改变 supervisor Recap/Finalize 的优先级、同 session 抢占、checkpoint 或五次 job retry 语义。
- 不为了 session 切换取消正在执行的 turn、compact、inbox、shell、MCP 操作或其他前台任务。
- 不强制把没有后台工作的 Finalize 包装成 supervisor job。
- 不改变 Finalize 成功通知条件，不为已完全 recap 的快速关闭补发通知。
- 不在新 session transcript 中展示旧 session 的 Finalize job ID 或完成状态。
- 不让新 TUI 持续订阅、轮询或展示旧 session 的后台 Finalize 状态。
- 不为已经打开的 Resume 目标实现状态回滚；失败后允许它按现有语义成为可再次恢复的 Interrupted session。
- 不把旧 session 的 transcript、contribution、context usage、delegation/process live state 带入目标 session。
- 不改变新 session 启动时现有 inbox、system prompt 与持久化数据之间的业务含义。

## 已拍板语义（不可静默修改）

以下决策是本 PRD 的固定基线。实施中如果发现真实冲突，必须先在“新增拍板记录”中追加原因、选项、选择和影响，再继续实现；不得以重构、兼容或恢复为由删除、重写或弱化既有决策。

### D1：`/new` 与会话内 `/resume`

- `/new` 是原生 TUI 命令，创建并切换到不同 session ID 的新 session。
- `/resume` 不再以当前 session 非空为由拒绝；空闲时均可打开 picker。
- `/resume` 只是打开 picker 时不改变当前 session。用户取消 picker 后回到原页面，当前 session 不进入 Finalizing。
- 只有选定 Resume 目标或 `/new` 目标成功准备后，才开始当前 session 的切换收尾。
- 当前正在运行前台任务时，`/new`、`/resume` 沿用现有输入队列，在任务收束后执行；不隐式取消当前任务。

### D2：切换期间的输入队列

- 目标准备、当前 Finalize 投递或前台 fallback 期间，用户后续输入继续进入既有 queued input。
- 切换成功后，队列属于目标 session，并按现有顺序派发。
- 目标准备失败且当前 session 尚未进入 Finalizing 时，排队内容恢复到当前 session composer。
- 当前 session 已进入 Finalizing 后若前台 Finalize 最终失败，不把排队内容发送给任何 session；沿用现有 finalize-failed 输入禁用与恢复边界。
- 不新增切换专用输入队列或跨 session 输入持久化文件。

### D3：Resume 目标先准备，当前 session 后收尾

- 用户在 picker 中选中目标后，先复用现有 Resume 路径重新非阻塞获取目标 `runtime.lock`，再读取 metadata 校验所属 agent、真实用户输入和 `Open`/`Closed` 可恢复状态。
- 目标准备继续执行正常 reopen、inbox、历史、context usage 与 local claims 加载。
- 目标准备成功后，TUI 在内存中暂存目标 session、runtime lease 与恢复结果；此时尚未替换当前 session。
- 目标准备失败时，当前 session 保持 `Open`，页面与输入可恢复，不进入 Finalizing。
- 准备期间允许同一 TUI 同时持有当前 session 与目标 session 的两把独立 runtime lease。

### D4：Resume reservation 保持薄实现

- 不新增持久化 reservation 状态、prepare/commit 协议、CAS 或目标回滚。
- 直接复用现有 `resume_runtime_session`：Closed 目标成功取得 lease 后可以立即变为 `Open`；异常 Open 目标保持 `Open`。
- 如果目标已经打开，但随后当前 session 的 Finalize 失败或 TUI 异常退出，释放目标 lease 后目标保留为未占用的 `Open`，由现有 picker 派生显示为 Interrupted，可在之后再次 resume。
- 不把这一小窗口扩展成复杂两阶段恢复；目标没有新的 canonical turn 就不存在对话内容丢失。

### D5：`/new` 先准备目标 session

- `/new` 先执行新 session 的正常准备，包括既有 inbox、system prompt、session 创建与 runtime lease 获取。
- 当前底层启动仍可保持既有 `inbox → prompt → session 持久化` 依赖；本需求不为显示顺序创建 provisional session。
- 只有准备成功并取得新的 session handle/runtime lease，才开始当前 session 的 Finalize。
- 准备失败时当前 session 仍为 `Open`，不执行 Finalize，也不留下可见的半切换页面。
- 如果新 session 已准备成功，但当前 session 的前台 Finalize 最终失败，则释放新 session lease，并使用现有空 session 删除逻辑清理这个尚无真实输入的新 session；不保留不可见的空目标。

### D6：`/new` 的 TUI 刷新时点与欢迎页

- 用户提交 `/new` 后，旧 session 的当前页面暂时保持不变；不能立即清空 transcript 或切换为初始化页。
- 新 session 准备成功并确认进入属于新 session 的正常 startup/inbox 展示边界后，才清空旧 session 视图并重新绘制欢迎页。
- 欢迎页沿用正常新 session 展示：Runtime Metadata、Agent、Model、Cwd、Branch、Maintainer/Router 状态、local claims、inbox warning 及初始化状态。
- 新欢迎页应用本次新 session 正常 inbox/startup 的真实结果，不伪造或跳过 inbox，也不沿用旧 session 的 network/contribution 快照。
- 新 session 在旧 session Finalize 成功投递或前台关闭完成前不接收 queued turn；切换完成后状态为 `Open` 并正常派发队列。
- 如果新 session 准备失败，TUI 保持或恢复旧 session 的完整页面，在旧页面显示本次 `/new` 失败；切换期间输入恢复到 composer。

### D7：复用现有 Finalize 工作判断

- 当前 session 进入切换收尾时持久状态按正常路径从 `Open` 变为 `Finalizing`。
- 继续复用现有后台工作判断：
  - `message_count > recapped_until`，或存在未消费的 background-process completion 时，创建 supervisor Finalize job；
  - 已完全 recap 且没有待消费 background completion 时，在 TUI worker 中快速 Finalize 并写入 `Closed`。
- 不因为当前 session 非空就强制创建一个没有实际后台工作的 Finalize job。
- 快速 Finalize 不调用 recap 模型，完成后再切换目标 session。

### D8：Finalize continuation

- `/exit`、`/new`、`/resume` 共用同一套 mark-finalizing、enqueue 和前台 fallback，不复制 Finalize 实现。
- Finalize 完成后的 continuation 分为：
  - `/exit`：保持现有行为，绘制最后一帧后退出 TUI；
  - `/new`：安装已准备的新 session；
  - `/resume`：安装已准备的 Resume 目标。
- Supervisor Finalize job 成功持久化入队后即可执行 session switch，不等待 job Running 或 Succeeded。
- Supervisor 不可用或 enqueue 失败时沿用现有前台 Finalize fallback；前台成功后继续切换。
- 前台 Finalize 也失败时不切换，当前 session 保持现有 Finalizing/Error 语义。

### D9：Finalize 通知保持当前语义

- `notify_on_finalize_completion` 继续控制系统通知总开关。
- Finalize 成功通知仍只在 `SessionFinalizeReport.finalized_unrecapped_messages == true` 时发送。
- 已完全 recap、仅执行快速关闭的非空 session 不发送成功通知。
- Supervisor Finalize 最终失败继续沿用现有失败通知与 `acn supervisor retry <session_id>` 处理方式。
- 不因 `/new` 或 `/resume` 改变 `/exit` 的通知条件。

### D10：旧 Finalize 后续失败不影响目标 session

- Finalize job 成功入队并完成切换后，目标 session 独立保持可用。
- 旧 session 的 job 后来失败时，旧 session 继续保持 `Finalizing`，用户按既有方式查看 supervisor jobs 并 retry。
- 新 TUI 不轮询旧 job、不展示跨 session warning banner、不自动切回旧 session。
- 旧 session 最终成功后正常变为 `Closed`。

### D11：空当前 session 直接替换

- 当前 session 没有真实用户输入、canonical messages、非空 turn journal 或 delegation 时，视为临时空 session。
- `/new` 或 `/resume` 目标准备成功后，直接替换并使用现有 `delete_empty_session` 删除旧空 session。
- 空 session 不进入 supervisor Finalize、不发送通知。
- journal 已接受真实用户输入时，即使 `message_count == 0` 也不按纯空 session 删除，继续走正常 Finalize 收尾。

### D12：目标安装与 TUI session 隔离

- 切换成功时一次性替换当前 `SessionHandle` 与 `SessionRuntimeLease`，释放旧 session 的 TUI 所有权。
- `/new` 清空旧 transcript；`/resume` 清空旧 transcript 后恢复目标 session 的历史 timeline。
- 两种切换都重置旧 context usage、turn 状态、contribution、status notice、delegation/process snapshot 和 session 级 background UI 投影。
- Workspace、branch、模型配置、MCP runtime、slash/skill 目录等进程或 workspace 级状态继续保留。
- 新 transcript 不写入旧 Finalize job ID、`Previous session finalizing` 或旧 session 的完成统计。

### D13：晚到 Recap enqueue 结果必须按 session 隔离

- Turn commit 触发的 Recap enqueue worker 继续独立运行，不阻塞 turn 完成或后续 session switch。
- `RecapEnqueueFinished` 必须携带来源 `session_id`。
- 结果返回时来源仍是当前 session，enqueue 失败继续显示既有固定 warning。
- 结果返回时已经切换到其他 session，忽略旧 session 的成功或失败结果，不能把旧 warning 显示到新 transcript。
- 这只修正一个已有异步结果的归属，不新增通用跨 session 事件总线；已有 process snapshot 的 session/generation 隔离继续复用。
- 旧 Recap enqueue 失败不阻止切换：只要后续 Finalize enqueue 成功，Finalize 会覆盖尚未 recap 的剩余区间。

### D14：用户可见状态

- 被切走的非空当前 session 持久状态保持 `Open → Finalizing → Closed`。
- Resume 目标按现有语义从 Closed 变为 Open，或以 Interrupted Open 继续保持 Open。
- 新 session 创建后为 Open，但在旧 session handoff 完成前，TUI 不派发目标 queued turn。
- Picker 取消、Resume 目标准备失败或 `/new` 目标准备失败时，当前 session 保持 Open。
- 不增加 `Switching`、`Reserved`、`Interrupted` 等持久状态；TUI 可以使用内存中的 startup/activity 文案表达短暂切换过程。

## 状态与数据流

### 非空 session 执行 `/new`

```text
current session Open + current TUI
  → 后台准备 new session（正常 inbox / prompt / create / runtime lease）
  → 准备失败：current 保持 Open，TUI 不切换
  → 准备成功：刷新为 new session 欢迎页，暂存 queued input
  → current mark Finalizing
      → 有后台工作：enqueue Finalize
      → 无后台工作：前台快速 Finalize
      → enqueue 失败：前台 Finalize fallback
  → handoff 成功：安装 new handle/lease，new session Open，派发 queued input
  → old supervisor Finalize 成功：old session Closed
```

### 非空 session 执行 `/resume`

```text
current session Open
  → 打开 picker
      → cancel：返回 current
      → select target：获取 target runtime lease 并正常 reopen/load
  → target 失败：current 保持 Open
  → target 成功：内存暂存 target
  → current mark Finalizing
      → enqueue Finalize / 前台快速 Finalize / 前台 fallback
  → handoff 成功：清理 current UI，安装 target history/handle/lease
  → target Open，派发 queued input
  → old supervisor Finalize 成功：old session Closed
```

### 临时空 session 切换

```text
准备 new/resume target
  → target 成功
  → 替换 current handle/lease
  → 删除旧空 session
  → 不创建 Finalize job，不通知
```

## 实现边界

### TUI 应用状态

- 增加只存在于进程内的 session-switch intent/prepared target，例如：

  ```text
  SessionSwitchIntent = New | Resume(session_id)
  PreparedSessionTarget = New(start_report) | Resume(resume_outcome)
  FinalizeContinuation = Exit | Switch(prepared_target)
  ```

- 数据结构命名可按当前模块职责调整，但不能把 continuation 写入 session metadata 或 supervisor job。
- `/new` 准备期间必须保留当前 session handle/lease；目标成功后直到 handoff 完成，允许同时持有两把不同 session 的 lease。
- Finalize worker 仍只操作旧 session；目标 session 在 continuation 安装前不运行 turn。

### TUI state reset

- 将现有 resume-only reset 泛化为 session-switch reset，并区分：
  - `/new`：正常欢迎页和新 session startup/inbox 状态；
  - `/resume`：目标历史 timeline、context usage、local claims 与 warning。
- `/new` 的旧页面必须保留到目标准备成功；不得在用户提交命令时立即清屏。
- 若需要暂存旧 session view，只保存内存态，并保持 queued input/async input sequence 的现有顺序；不新增磁盘 snapshot。

### Runtime 与 Supervisor

- Finalize enqueue outcome 与 foreground finish 由 TUI continuation 决定退出还是切换。
- Background enqueue 成功时先确认 job 已持久化，再释放旧 TUI runtime lease并安装目标。
- 不修改 supervisor job DTO 来保存 TUI continuation。
- 不修改 Finalize success notification predicate。

### 异步事件

- 给 Recap enqueue completion 增加来源 session ID，并在 App 层校验当前 session。
- 对已有携带 session/generation 的 process snapshot 继续沿用现有过滤。
- 实施时审计其他可能跨 switch 晚到且会写 transcript/session UI 的现实事件；只有发现真实可触发路径时才做同级最小来源校验，不扩展成推测性的全事件框架。

## 文档同步

实现完成时至少同步：

- `README.md`：TUI 命令表增加 `/new`，更新 `/resume` 为会话内切换语义。
- `docs/user_guide.md`：补充 `/new`、非空 `/resume`、Finalize 后台与失败 retry 说明。
- `docs/PRDs/PRD_interrupted_session_resume.md`：仅追加本 PRD 的替代关系，保留原历史范围。
- TUI `/help`、slash completion 和相关稳定用户文案。
- 如果实现实际改变其他当前架构文档，再按真实代码同步；不为了 PRD 扩写无关文档。

## 分阶段 Planning 与验收

### 阶段切换硬约束

- 每次进入下一阶段前，主执行者必须完整重读本 PRD。
- 重读后逐项确认下一阶段实现没有偏离 D1–D14。
- 工作中出现新的业务、失败处理或用户可见语义选择时，必须先在“新增拍板记录”末尾追加：原因、选项、选择、原因与影响，再继续实现。
- 新记录只能追加，不能删除、改写或弱化 D1–D14。
- 如果新发现与既有拍板冲突，暂停冲突部分并向用户确认，不能自行用实现便利替换旧语义。

### Phase 0：PRD 固化与现状基线

Todo：

- [x] 写入全部已拍板语义、非目标、状态流和失败边界。
- [x] 对照当前 main 的 `/resume`、start、Finalize、runtime lease、Supervisor、TUI reset 与输入队列确认可实施性。
- [x] 明确当前底层 inbox/prompt/session 创建依赖不因 UI 展示顺序被改写。
- [x] 在异常退出恢复 PRD 中追加本 PRD 的替代指向。

验收：

- 本 PRD 与用户逐项拍板一致。
- 未引入新持久状态、复杂回滚或跨 session job 监控。
- 当前无未决的阻断性业务拍板。

### Phase 1：命令入口与 session-switch 状态机

进入本阶段前完整重读本 PRD。

Todo：

- [x] 增加 `/new` 的 slash catalog、输入分类、help 与命令分发。
- [x] 移除非空当前 session 对 `/resume` 的产品限制，保留任务运行与 picker 并发边界。
- [x] 增加内存中的 switch intent、prepared target 与 Finalize continuation。
- [x] 泛化当前 task/handle busy 判断，使 prepare、Finalize 与 queued input 顺序清晰且互斥。
- [x] 保持 `/exit` 现有用户语义不变。

验收：

- `/new` 被识别为原生命令，不作为模型消息。
- 非空空闲 session 可以打开 `/resume` picker。
- Picker 取消完全不改变当前 session。
- 前台任务运行时 `/new`、`/resume` 排队而不是取消任务。
- 同一时刻不能启动两条 session switch。

### Phase 2：目标准备、欢迎页与 Resume 安装

进入本阶段前完整重读本 PRD。

Todo：

- [x] 复用正常 start 路径准备 `/new` 目标，成功前保留旧页面和旧 session 所有权。
- [x] `/new` 目标成功后重新绘制欢迎页，应用本次正常 inbox/startup 的 team status、warning 与 local claims。
- [x] `/new` 目标失败时保持/恢复旧页面、旧 session 与输入草稿。
- [x] 复用现有 Resume reopen/inbox/history 路径准备目标，并在内存中暂存目标 lease。
- [x] Resume 目标失败时保持当前 Open；成功后继续沿用目标历史、context usage、local claims 与 journal warning。
- [x] 泛化 session UI reset，隔离 session 级状态并保留 workspace/MCP/skills 等进程级状态。
- [x] 空当前 session 成功切换后使用现有删除逻辑清理。

验收：

- `/new` 命令提交后不会立刻清空旧 transcript。
- 新目标未准备成功时旧 session 仍可继续使用。
- 新目标成功时看到正常欢迎页，旧 transcript/contribution/context 不出现。
- Resume picker 列表后的锁竞争仍会明确失败，当前 session 不受影响。
- Closed 目标可以在暂存期变为 Open；异常中止后按 Interrupted 再次出现，不做回滚。
- 新 session 准备成功但旧 Finalize 失败时，准备出的空 session 被安全清理。

### Phase 3：Finalize continuation 与晚到事件隔离

进入本阶段前完整重读本 PRD。

Todo：

- [x] 将现有 Finalize enqueue/foreground finish 的固定退出行为泛化为 Exit/New/Resume continuation。
- [x] 保留现有 background-work gate、supervisor enqueue 与前台 fallback。
- [x] Enqueue 成功或前台快速 Finalize 成功后替换 handle/lease，并开始派发目标队列。
- [x] 前台 Finalize 失败时不安装目标，保持当前 Finalizing/Error 语义。
- [x] 给 Recap enqueue completion 增加来源 session ID，过滤旧 session 晚到结果。
- [x] 审计切换期间真实存在的其他晚到 UI 事件，只修复有现实触发路径的 session 归属问题。
- [x] 保持通知 predicate、supervisor retry 和旧 job 后续失败语义不变。

验收：

- 有未 recap 内容时，Finalize job 成功持久化后立即切换，旧 session 后台最终 Closed。
- 已完全 recap 时前台快速关闭后切换，不创建空 supervisor job、不通知。
- Supervisor enqueue 失败后前台 Finalize 成功，仍能完成切换。
- 前台 Finalize 失败不误装目标、不发送 queued turn。
- 旧 Recap enqueue 失败结果不会显示到新 session。
- 新 session 不显示旧 Finalize job ID，不订阅旧 job 状态。

### Phase 4：定向测试、文档与完整 Verify

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/verify/SKILL.md` 与 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md`。

Todo：

- [x] 补齐 slash command、输入队列、picker cancel、target prepare、Finalize continuation 和 session UI reset 单元测试。
- [x] 增加 runtime lease、Closed/Interrupted target、空 session 删除、enqueue fallback 和晚到 Recap result 的定向测试。
- [x] 更新 README、用户指南、help 与受影响的当前行为文档。
- [x] 运行 `scripts/check_version_consistency.sh`。
- [x] 运行 `cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test` 与 `cargo check`。
- [x] 运行 canonical tmux TUI smoke，并检查 captures、空 stderr 与 tmux 清理。

验收：

- 自动化覆盖 D1–D14 的主要成功、常见失败和状态转换。
- 完整 verify 与 canonical TUI smoke 全部通过。
- 文档中不再把会话内非空 `/resume` 或 `/new` 描述为不支持。
- 未修改或清理无关工作区内容。

### Phase 5：针对性真实 LLM TUI Smoke Test

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md`。

Todo：

- [x] `source export_env.sh` 后使用真实 LLM、真实 ACN TUI 和真实 supervisor，不用 fake provider 冒充本验收。
- [x] 使用独占临时 `acn_home`、测试配置和 supervisor，避免污染用户正式 session、claims 或 jobs。
- [x] 在 `target/` 下编写聚焦 tmux flow 并保存文本 captures、stderr、session metadata 与 supervisor jobs 输出。
- [x] `/new` 场景至少覆盖：
  - [x] 当前 session 完成真实模型 turn；
  - [x] `/new` 提交后旧页面在目标成功前不被提前清空；
  - [x] 目标成功后重新出现正常欢迎页与新 session ID，旧 transcript 不存在；
  - [x] 新 session 能完成真实模型 turn；
  - [x] 旧 session 经 Finalizing 最终 Closed。
- [x] 会话内 `/resume` 场景至少覆盖：
  - [x] 在非空当前 session 打开 picker并取消，页面和 session 不变；
  - [x] 再次打开 picker并选择一个有真实历史的 Closed/Interrupted 目标；
  - [x] 目标历史正确恢复，后续真实模型 turn 能继续目标上下文；
  - [x] 被切走 session 的 Finalize 成功入队并最终 Closed；
  - [x] 当前 TUI 不退出。
- [x] 检查两条 flow 的 `stderr.log` 为空，tmux session 与独占 supervisor 被清理。

验收：

- 真实模型确实完成 `/new` 后的新 session turn 和 `/resume` 后的续接 turn。
- TUI captures 能证明欢迎页刷新时点、旧 transcript 隔离、目标历史恢复和进程未退出。
- Session metadata/jobs 能证明旧 session 的 `Open → Finalizing → Closed`。
- 不通过破坏共享 supervisor、伪造系统通知或依赖不稳定模型措辞制造验收结果。

### Phase 6：针对性 Code Review、P0/P1 修复与复验

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/code-review/SKILL.md`。

Todo：

- [x] 先检查实际 `git status`、diff 与周边运行代码，完成本地 review。
- [x] 本地 review 聚焦 session 状态机、双 runtime lease、Finalize handoff、输入归属、失败恢复、异步晚到事件、TUI 误导与高价值测试缺口。
- [x] 运行一次独立、只读的 `codex exec --json` review；禁止外部 reviewer 修改文件、调用 code-review skill、运行嵌套 Codex 或使用 delegation。
- [x] 合并并去重本地与外部 findings。
- [x] 修复所有具有现实触发条件、实质影响且不属于过度防御的 P0/P1。
- [x] 不为极小概率 P2/P3、纯样式意见、推测性 crash 或复杂回滚扩大实现。
- [x] 修复后重跑受影响定向测试、完整 verify、canonical tmux smoke 和针对性真实 LLM TUI smoke。
- [x] 如果 review 后发生代码修复，再对修复后的最终 diff 做本地复审和一次独立只读外部复审，确认没有遗留或新引入的现实 P0/P1。

验收：

- 本地与外部 findings 有清晰结论和现实触发条件。
- 所有符合范围的 P0/P1 均已修复并复验；若无发现则明确记录无发现。
- 修复后的最终代码再次通过必要 review，不以第一次 review 代替修复后的检查。
- 没有以“防御性”为名引入本 PRD 排除的持久化事务、复杂回滚或跨 session 监控。

### Phase 7：最终 PRD 对齐审计

进入本阶段前完整重读本 PRD。

Todo：

- [x] 逐条对照 D1–D14、各 Phase 验收与最终代码、测试、文档和真实 TUI 证据。
- [x] 检查新增拍板记录是否完整，确认旧拍板未被静默改变。
- [x] 汇总完整 verify、canonical smoke、真实 LLM TUI smoke、code review、修复和修复后复验结果。
- [x] 只有全部要求完成后才把 PRD 状态更新为“已完成”并勾销 Todo；未完成项必须明确保留。

验收：

- 整体实现与本 PRD 对齐。
- 所有必须验证项都有可复核证据。
- 没有未解释语义偏差、未处理现实 P0/P1、旧文档冲突或伪装完成的验收项。

## 新增拍板记录（只追加）

当前没有新增未决拍板。实施期间如需新增，必须使用以下格式追加在本节末尾，禁止回写 D1–D14：

```text
### ND-N：标题（YYYY-MM-DD）

原因：

问题：

选项：
- A：...
- B：...

选择：A/B

原因：

影响：
```

### ND-1：旧 session 先完成 handoff，再启动目标（2026-08-31）

原因：

现有实现先完整准备 `/new` 或 `/resume` 目标，再收尾旧 session，导致目标 inbox、历史加载和欢迎页与旧 session Finalize 交错，也使旧 session 仍可输入的边界不清晰。用户要求切换体验与独立启动 ACN 一致，并让旧 session 的 Finalize 明确发生在旧页面。

问题：

目标 startup 应发生在旧 session Finalize handoff 之前还是之后？

选项：

- A：保持先完整准备目标，再 Finalize 旧 session。
- B：只完成必要的切换前校验，先让旧 session 完成 Finalize handoff，成功后再清屏并启动目标。

选择：B。

原因：

这样旧页面只表达旧 session 的正常 `Finalizing`，handoff 成功后目标页面才开始自己的 startup，不需要跨 session 的临时展示或提前执行目标 inbox。Supervisor job 持久化入队仍视为 handoff 成功，不等待后台 job 实际完成。

影响：

- 本记录仅在冲突处覆盖 D1、D3、D5、D6、D8、D11、D12、D14 以及旧“状态与数据流”“实现边界”“实施结果”中“先完整准备目标”的描述；未冲突部分继续有效。
- `/new` 不再预创建目标：旧 session handoff 成功后，清空旧页面，显示正常启动欢迎页，可见地执行现有 `inbox → prompt → create session/runtime lease → Open`。
- `/resume` 只在切换前取得目标 runtime lease 并做 metadata/所属 agent/可恢复状态校验；旧 session handoff 成功后才加载目标历史、context 与 local claims，再在目标历史页面可见地执行 inbox。
- 当前 session 为空时仍使用现有空 session 删除语义跳过 Finalize，然后开始目标 startup。
- `/new` 继续复用当前进程已经解析的 SessionEngine、配置、`--cd`、upstream、agent、模型、provider 与工具，不重新加载启动配置；`/resume` 后的新 turn 也使用本进程工作目录和运行配置。

### ND-2：handoff Finalize 使用早输入锁（2026-08-31）

原因：

先前实现为让切换等待期可输入，过滤了旧 session 的 Finalize 展示；在新的 finalize-first 顺序下，这会让用户误以为旧 session 仍可交互，也可能模糊输入究竟属于旧 session 还是目标 session。

问题：

旧 session handoff 期间是否允许继续编辑或提交输入？

选项：

- A：旧 session 一进入 handoff Finalize 就使用正常 `Finalizing` 页面并禁用输入；handoff 成功进入目标 startup 后恢复输入，输入在目标 Open 前按现有队列等待。
- B：handoff 期间继续允许输入并排队到目标。

选择：A。

原因：

Finalizing 页面与持久状态一致，用户不会在旧页面输入将来属于另一个 session 的内容。目标 startup、历史加载和 inbox 期间仍可输入，沿用现有 queued input，在目标 `Open` 后按顺序派发。

影响：

- 本记录在冲突处覆盖 D2、D6、D8、D14 和旧 review 中“switch continuation 保持 composer”的结论。
- `/new` 真正执行命令、或 `/resume` 选中目标并完成轻量预检后，旧页面进入现有 `Finalizing · Committing contribution`，状态栏显示旧 session finalizing，composer 禁用。
- Supervisor enqueue ack、本地快速关闭或前台 Finalize fallback 成功后即解除旧 session 输入锁并进入目标 startup；后台 Supervisor 实际执行不继续阻塞目标。
- `/exit` 同样继续在 Finalizing 期间禁用输入；前台 Finalize 失败继续沿用 finalize-failed 锁定。
- handoff 之前已经存在的 queued input 不丢弃，只在目标最终进入 `Open` 后派发。

### ND-3：Resume 的轻量预检与 handoff 后恢复顺序（2026-08-31）

原因：

Resume 必须在收尾旧 session 前发现真实的目标锁竞争和 metadata 错误，但不应在旧 session 仍活跃时执行目标 inbox 或加载完整历史。

问题：

Resume 的目标校验、历史恢复和 inbox 分别放在哪个边界？

选项：

- A：选中目标后只取得 runtime lease 并校验 metadata；handoff 成功后按“欢迎页 → 历史/context/local claims → 显示历史 → inbox → Open”执行。
- B：handoff 前完成全部 reopen、历史加载和 inbox。

选择：A。

原因：

锁和 metadata 是决定能否开始切换的必要条件；历史和 inbox 是目标 session startup 的一部分，应在 handoff 后发生。实现保持薄，不增加持久 reservation 或两阶段事务。

影响：

- 选中目标后立即建立早隔离边界，再做 runtime lease 与 metadata 轻量校验；校验失败时当前 session 保持 `Open`，但早隔离 generation 不回滚。
- 预检成功后旧页面进入 Finalizing；handoff 失败时不安装目标，释放目标 lease，目标按既有 Interrupted/Open 语义可再次恢复。
- handoff 成功后清空旧页面，显示目标启动欢迎页，读取并显示目标既有历史、context、local claims；随后在保留历史的页面上可见地进入 inbox，成功后进入 `Open` 并派发 queued input。
- 目标历史加载和 inbox 期间允许输入，但只进入既有 queued input，不在 `Open` 前派发。

### ND-4：Resume 不改写既有历史与 system prompt（2026-08-31）

原因：

Resume 的 startup 顺序调整后，需要明确加载历史和执行 inbox 对 canonical 对话数据的写边界。

问题：

Resume startup 是否可以重建或替换已有 system prompt、历史消息？

选项：

- A：只读并渲染既有 messages/journal/history 与 system prompt；inbox 不替换或重建它们，只有后续正常 turn 追加消息。
- B：按新 session startup 重建 system prompt 或重写历史。

选择：A。

原因：

Resume 的核心语义是续接原会话。重建 prompt 或改写历史会改变既有上下文，不属于本需求。

影响：

- Resume 历史、ctx 和 local claims 加载为只读恢复；inbox 仅按既有 inbox 语义处理团队输入，不修改已有 canonical messages 和 system prompt。
- Resume 后未来用户/assistant turn 继续按正常路径追加。

### ND-5：Resume inbox 失败降级为可恢复 warning（2026-08-31）

原因：

handoff 成功后旧 session 已交给 Supervisor 或完成关闭，若目标 inbox 失败，不能再安全回退到旧 session；但 inbox 失败也不应阻断已成功恢复的目标历史对话。

问题：

Resume handoff 后 inbox 失败时，目标 session 是否仍可使用？

选项：

- A：显示短 warning，建议 `/inbox` 重试，随后进入 `Open` 并允许继续交互。
- B：进入 startup Error，禁止使用目标 session。

选择：A。

原因：

目标 session 的 lease、metadata 和历史已经成功恢复，inbox 是可由 `/inbox` 重试的独立同步动作。降级可避免一次团队同步失败使本地会话不可用。

影响：

- TUI warning 固定为 `Warning: Inbox sync failed; run /inbox to retry.`，详细错误只写日志，避免长错误污染 transcript。
- warning 前后各保留一个可见空行；不得紧贴前一条历史或后一条内容。
- warning 后目标状态进入 `Open`，用户可以正常继续交互或稍后手动执行 `/inbox`；queued input 正常派发。
- `/new` 的 inbox 失败不采用本降级，完全保持直接启动 ACN 的现有失败语义：新 session 尚未创建、startup Error、无自动恢复或专用重试。

### ND-6：会话切换使用早 interaction generation 隔离现实晚到事件（2026-08-31）

原因：

除已按 `session_id` 隔离的 Recap 结果外，`/copy`、Ctrl+O 附件预览和 Ctrl+V 剪贴板图片读取也会异步完成；它们在旧 session 发起、切换后完成时，当前实现会把结果文本或附件状态写到新 session transcript。

问题：

何时建立隔离边界，如何处理边界前已启动但边界后才完成的交互？

选项：

- A：在 `/new` 真正执行、或 `/resume` picker 选中目标时递增进程内 interaction generation；结果 generation 不匹配时静默丢弃 TUI 投影，但不撤销外部副作用。
- B：等目标安装后才隔离，或逐个取消异步操作。

选择：A。

原因：

早边界从用户作出切换决定时就阻止旧操作污染后续页面；单个 generation 足以覆盖已确认的现实事件，无需引入通用事件总线、任务取消或回滚协议。

影响：

- `/new` 若排队到当前 turn 后执行，只在命令真正开始执行时递增 generation；仅输入命令但尚未执行不提前隔离。
- `/resume` 打开或取消 picker 不递增；用户选中目标时立即递增。之后即使预检或 startup 失败也不回滚 generation，旧操作继续视为 stale。
- stale `/copy` 成功/失败提示、Ctrl+O 预览结果/附件名/错误、Ctrl+V 读取失败/未发现图片/丢弃提示均不进入当前 transcript。
- 不取消已经发生的 `pbcopy` 或外部预览应用；stale 预览成功仍登记临时文件供退出时清理；stale 剪贴板读取仍结算 pending 计数但不附加图片或写提示。
- 边界后在目标 startup 期间新发起的交互属于目标 generation，可以跨 handoff 正常完成。
- MCP 继续是进程/workspace 级运行时，不纳入本次 session 事件隔离。

### ND-7：不改变 Supervisor、通知与现有 New startup 失败语义（2026-08-31）

原因：

新的编排只改变 TUI 何时启动目标，不应扩大为 Supervisor 协议、job/checkpoint、通知或通用 startup 恢复改造。

问题：

是否借本次需求同时增加空 Finalize job、跨 session job 订阅、新 session inbox 失败恢复或通知变化？

选项：

- A：不增加；继续复用现有 work gate、Supervisor enqueue/fallback、checkpoint、五次 job retry 与通知 predicate。
- B：同时扩展后台协议和 startup 恢复。

选择：A。

原因：

这些能力不是会话内切换的必要条件，会显著扩大改动和恢复边界。

影响：

- 没有真实 background work 时继续本地快速关闭，不投递空 job。
- Supervisor 启动或 enqueue 失败时继续前台 Finalize fallback；成功后才进入目标，失败则不切换。
- 旧 session 后台 Finalize 的通知与 job retry 完全保持现有语义，目标 TUI 不订阅旧 job。
- `/new` inbox 失败保持直接启动 ACN 的现状，留给后续独立需求处理。

## 追加实施 Planning 与验收（ND-1 至 ND-7）

以下清单只覆盖追加拍板导致的重构和修复。上方已完成清单保留为历史证据；凡与 ND-1 至 ND-7 冲突的旧验收结果，不再作为当前实现完成依据。

### 状态切换硬约束

- 每次从 PRD 固化进入实现、从实现进入验证、从验证进入 code review、以及 review 修复后再次进入复验前，主执行者必须完整重读本 PRD。
- 发现新的业务选择时，先继续追加 ND 记录，再修改代码；不得回写或删除 D1–D14、ND-1 至 ND-7。

### 编排与隔离实现

- [x] 将 `/new` 改为旧 session handoff 成功后才调用现有正常 start 流程；旧空 session 继续直接删除。
- [x] 将 `/resume` 拆为切换前轻量 lease/metadata 预检，以及 handoff 后历史恢复、可见 inbox、Open。
- [x] Resume 恢复不改写已有 canonical history/messages/system prompt。
- [x] handoff Finalize 使用正常旧页面和输入锁；目标 startup/inbox 期间输入进入现有队列。
- [x] Resume inbox 失败降级为带前后空行的固定 warning，目标进入 Open 并允许 `/inbox` 重试。
- [x] 增加 interaction generation 早边界，隔离 Recap 之外已确认的 `/copy`、Ctrl+O、Ctrl+V 晚到 UI 结果，并保留必要资源清理/计数结算。
- [x] 不修改 Supervisor DTO、优先级、checkpoint、retry、通知和 background-work gate。

验收：

- `/new` 的旧页面先正常 Finalizing，handoff 成功后才出现正常欢迎页和可见 inbox；新 startup 失败与直接启动行为一致。
- `/resume` 在预检失败时保留旧 Open；成功时旧页面先 Finalizing，handoff 后按“欢迎页 → 历史 → inbox → Open”执行。
- Resume inbox 失败不会进入 Error；warning 与相邻内容之间均有空行，用户可继续 turn 或手动 `/inbox`。
- Finalize 期间不能输入；目标 startup 期间可以输入但不会在 Open 前派发。
- 三类旧 interaction completion 不污染新 transcript，且 stale preview 临时文件和 stale clipboard pending 状态没有泄漏。

### 定向测试、文档与完整验证

- [x] 增加/更新状态机、输入锁、空旧 session、preflight failure、history-before-inbox、Resume inbox success/failure、queued input 与早 generation 隔离测试。
- [x] 为 `/copy`、Ctrl+O、Ctrl+V 的 stale/current generation 结果补现实回归测试。
- [x] 同步 README、用户指南、架构与 help 中受新顺序影响的稳定语义，不扩写无关设计。
- [x] 按 `.agents/skills/verify/SKILL.md` 运行版本一致性、fmt、Clippy、全部测试和 check。
- [x] 按 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md` 运行 canonical tmux smoke，检查 captures、空 stderr 与 session 清理。

### 针对性真实 LLM TUI Smoke Test

- [x] 使用真实 LLM、真实 TUI、真实隔离 Supervisor 与独占临时 `acn_home`，不得用 fake provider 代替。
- [x] `/new` 证明旧页面正常 Finalizing 且输入禁用，handoff 后才出现新欢迎页/inbox，新 session 可完成真实 turn。
- [x] `/resume` 证明 picker cancel 不建立边界，选择目标后旧页面 Finalizing；handoff 后先恢复历史再显示 inbox，成功后可完成续接 turn。
- [x] 注入或构造 Resume inbox 失败，证明固定 warning 前后空行、目标仍 Open、`/inbox` 可重试且后续真实 turn 可用。
- [x] 至少覆盖一种边界前异步交互在切换后完成的场景，证明结果不会写入目标 transcript。
- [x] 检查 session metadata、Supervisor jobs、captures、空 stderr，以及 tmux/隔离 Supervisor 清理。

### 针对性 Code Review、修复与最终对齐

- [x] 按 `.agents/skills/code-review/SKILL.md` 先完成本地 review，再执行一次独立只读 `codex exec --json` 外部 review。
- [x] review 聚焦 finalize-first 顺序、双 lease 窗口、Resume 历史只读、输入归属、generation stale 资源结算、warning 展示与常见失败路径。
- [x] 修复所有具有现实触发条件、实质影响且不属于过度防御的 P0/P1；不为极小概率恢复引入复杂事务。
- [x] 修复后重跑受影响测试、完整 verify、canonical tmux smoke 和针对性真实 LLM smoke。
- [x] 如果 review 后修改过代码，对最终 diff 再做本地复审和一次独立只读外部复审；有新 P0/P1 则继续修复、复验并再次 review。
- [x] 最后完整重读本 PRD，逐条对照 D1–D14 与 ND-1 至 ND-7；只有全部实现、验证和 review 证据齐全后才恢复“已完成”状态并勾选追加清单。

## 实施与验收结果

### 实现结果

- 已增加原生 `/new`，并允许非空空闲 session 使用 `/resume` picker；任务忙时继续使用既有输入队列。
- 新增的 switch intent、prepared target 与 Finalize continuation 全部只存在于 TUI 进程内；未增加持久状态、切换事务、CAS、reservation、恢复目录或 supervisor DTO 字段。
- `/new` 和 `/resume` 都先准备目标，再复用 `/exit` 的 mark-finalizing、background-work gate、Finalize enqueue、前台 fallback 与通知语义收尾旧 session。
- `/new` 在目标正常 startup/inbox 完成前保留旧页面；准备成功后绘制目标欢迎页，handoff 成功后才派发等待输入。`/resume` 保留 picker cancel、Closed/Interrupted reopen、历史/context/local claims 恢复语义。
- 切换成功时替换 handle/runtime lease，并清理旧 transcript、context、contribution、process/delegation 等 session 投影；workspace、MCP、skills 与输入交互顺序继续保留。
- `RecapEnqueueFinished` 已携带来源 session ID，旧 session 的晚到结果不会污染新 transcript。
- 空当前 session 继续使用既有删除语义；非空旧 session 仍保持 `Open → Finalizing → Closed`。
- 为支持同进程切换，在前台 Finalize 成功或 supervisor job 已持久化入队后释放旧 session 的空闲 delegation runner lease；失败与 fallback 期间不提前释放，仍保持 runtime 排他性。

### 自动化验证

- `scripts/check_version_consistency.sh` 通过，版本保持 `0.2.5`。
- `cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check` 全部通过；补充的 `cargo clippy --all-targets --all-features -- -D warnings` 也通过。
- 测试结果包含 2616 个 library tests、59 个 `acn` binary tests，以及 maintainer、router、storage/cleanup 与 doc tests，全部通过。
- 定向测试覆盖 `/new` 命令与 help、忙时排队、picker cancel、Finalize continuation、journal-only 非空判断、switch state reset、`/new` 失败恢复、晚到 Recap 归属、Resume Finalize 等待输入和 delegation runtime lease 释放。
- `git diff --check` 通过；tracked diff 未发现真实用户名、绝对用户路径、明文密钥、内网域名或内部账号等不适合开源的内容。

### TUI 验收

- Canonical tmux smoke 通过：初始页正常，`/help` 展示 `/new`、`/resume` 与 `/skills`，`target/tui-smoke/stderr.log` 为空，tmux session 已清理。
- 使用真实 `deepseek-v4-flash`、真实 ACN TUI、真实独占 supervisor 和隔离 `acn_home` 完成 `target/tui-scenarios/in-session-switch-real-llm/` 场景。
- `/new` 场景完成旧 session 真实 turn，证明提交后旧页面未提前清空；目标成功后出现不同 session ID 的正常欢迎页且旧 transcript 不存在，新 session 随后完成真实 turn。
- `/resume` 场景先取消 picker，证明当前 session 和页面不变；再次选择已有真实历史的 Closed session 后恢复旧 transcript，并在 picker 消失后立即提交等待输入。目标安装后该输入直接进入真实模型 turn，模型从目标上下文返回验收代号，证明输入归属和历史续接正确，TUI 全程未退出。
- `session-state-summary.txt` 证明两个被切走 session 都经历 `open → finalizing → closed`；`supervisor-jobs-final.txt` 中三个 Finalize job 均为 `succeeded`；真实场景 `stderr.log` 为空，tmux 与独占 supervisor 均已清理。

### Review 与修复

- 本地 review 与第一次真实 LLM smoke 发现：旧 session 的 delegation runner 仍持有 runtime lease，导致同进程 `/resume` 无法重新选择已 Closed 的目标。已按既有 runner registry 所有权最小修复，并增加 idle lease 释放测试。
- 第一次独立只读外部 review 发现一个现实 P1：Resume 切换的旧 Finalize 生命周期事件会把 TUI 置为 `Finalizing`，从而禁止等待期输入。已只对 switch continuation 保留 composer、过滤旧 session 的 `Finalizing`/`Closed` 展示事件，同时继续由运行中的 Finalize task 阻止输入发往旧 session；Finalize 失败时仍沿用 `finalize_failed` 锁定。
- 修复后重新运行定向测试、完整 verify、canonical tmux smoke 与真实 LLM TUI smoke，全部通过。
- 对修复后的最终 diff 再做本地复审和独立只读外部复审；结论为先前 P1 已完整修复，没有符合范围的遗留或新引入 P0/P1，也没有新的高价值 P0/P1 测试缺口。

### 最终对齐结论

- 最终代码、测试、稳定文档和真实 TUI 证据逐条符合 D1–D14 与各 Phase 验收要求。
- 实施中没有产生需要新增业务拍板的语义分歧；发现的问题均属于既有拍板下的实现缺陷修正，旧拍板未被删除、改写或弱化。
- 未引入非目标中的复杂持久化恢复、跨 session job 监控、通知变化或 supervisor 调度变化。

## ND-1 至 ND-7 追加实施最终结果（2026-08-31）

本节是追加拍板后的最终实现与验收结论；上方“实施与验收结果”保留为此前实现历史，其中与 ND-1 至 ND-7 冲突的旧顺序和旧 review 结论不再代表最终代码。

### 最终实现语义

- `/new` 真正开始执行时建立 interaction generation 早边界。非空旧 session 立即进入正常 `Finalizing · Committing contribution` 并禁用输入；Supervisor Finalize job 持久化入队、本地快速关闭或前台 fallback 成功后，才清空旧页面并进入目标的正常欢迎页与既有 `inbox → prompt → create session/runtime lease → Open` 启动流程。新 session 启动失败保持直接启动 ACN 的既有语义。
- `/resume` 打开或取消 picker 不改变 generation 和当前 session；选中目标时建立早边界，只取得目标 runtime lease 并做 metadata、agent 和可恢复状态预检。预检成功后旧页面先正常 Finalizing；handoff 成功后才清屏，按“欢迎页 → 只读加载并显示已有历史/context/local claims → 可见 inbox → Open”恢复目标。
- Resume startup 不重建或替换已有 system prompt、canonical messages、journal 或历史；只有恢复后的正常新 turn 才追加消息。Resume inbox 失败固定显示 `Warning: Inbox sync failed; run /inbox to retry.`，warning 前后各有一个可见空行，随后目标保持 `Open`，queued input 可派发，用户可继续交互或手动 `/inbox`。
- Finalizing 期间包括 Esc 在内均不能编辑、提交或取回 queued input；handoff 前已有队列完整保留。目标 startup/history/inbox 期间可以继续输入，但只进入既有队列，目标 `Open` 后才按顺序派发。
- `RecapEnqueueFinished` 按来源 session ID 隔离；`/copy`、Ctrl+O 与 Ctrl+V 按 interaction generation 隔离。旧 session 的晚到成功、失败或丢弃提示不进入新 transcript，外部剪贴板/预览副作用不回滚，clipboard pending 计数和 preview 临时文件仍正常结算。
- Ctrl+O 批次准备失败会返回已生成的临时路径，`open` 启动/非零退出失败也携带全部临时路径；预览任务额外由 App 的 `JoinSet` 保留清理路径，因此即使 completion event 在 TUI 退出前尚未消费，shutdown 仍会等待任务、汇总并删除临时文件。
- 空旧 session 继续使用现有删除语义，不投递空 Finalize job、不通知。Supervisor enqueue/fallback、同 session Recap/Finalize 优先级与抢占、共用 checkpoint、五次 job retry、Finalize 通知 predicate 和后台 job 后续失败/retry 语义均未改变；目标 TUI 不订阅旧 session job。

### 修复后的最终验证

- `scripts/check_version_consistency.sh` 通过，版本保持 `0.2.5`。
- `cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test` 与 `cargo check` 全部通过；最终测试包含 2622 个 library tests、59 个 `acn` binary tests，以及 maintainer、router、storage/cleanup 与 doc tests，0 失败。
- 定向回归覆盖：重复 Esc 不改变两个目标 queued inputs；部分 Ctrl+O 准备失败返回先前临时路径；completion event 保持排队且未消费时，TUI shutdown 仍删除临时文件。
- Canonical tmux smoke 通过：欢迎页与 `/help` 中 `/new`、`/resume`、`/skills` 正常，`target/tui-smoke/stderr.log` 为空，tmux session 已清理。
- `target/tui-scenarios/in-session-switch-real-llm/` 使用真实 `deepseek-v4-flash`、真实 ACN TUI、隔离真实 Supervisor 和独占临时 `acn_home` 再次通过。captures 证明 `/new` 与 `/resume` 的旧页面 Finalizing、目标欢迎页/history-before-inbox、固定 warning 空行、真实上下文续接、手动 `/inbox` 重试及 stale `/copy` 隔离；两个被切走 session 均为 `open → finalizing → closed`，三个 Finalize jobs 均 `succeeded`，全部 stderr 为空，tmux 与隔离 Supervisor 已清理。

### 最终 Code Review

- 本轮第一次独立只读外部 review 发现两个现实 P1：Finalizing 时 Esc 仍可取回并覆盖目标队列；Ctrl+O 在批次后续准备失败或 `open` 失败时会丢失已创建临时路径。两项均按既有所有权做最小修复并增加回归测试。
- 修复与复验后的第二次独立只读外部 review 发现一个现实 P1：预览任务已生成临时文件、但 completion event 尚未被事件循环消费时退出，旧 shutdown 路径仍可能漏删文件。已用 App 自有 `JoinSet` 保留任务级清理路径，并增加未消费事件的 shutdown 回归。
- 第二次修复后重新完成定向测试、完整 verify、canonical tmux smoke 与真实 LLM TUI smoke；最终本地复审和第三次独立只读外部 review 均为 P0 无、P1 无。最终外部 reviewer 明确确认两个既有 P1 已闭合，且未发现 finalize-first、Resume 历史/inbox、输入归属、generation 隔离、Supervisor、通知或 retry 边界中的新现实 P0/P1。

### 最终对齐

- 最终实现逐条符合 ND-1 至 ND-7，并在冲突处按追加记录覆盖 D1–D14 的旧顺序；未改变任何旧拍板本身的文字或未冲突语义。
- review 期间新增内容均是已拍板语义下的现实实现缺陷修复，不构成新的业务选项，因此没有新增 ND 拍板。
- 未增加新持久状态、切换事务、CAS、目标回滚、跨 session job 监控或 Supervisor 协议；追加 Planning 与验收项已全部完成。

### ND-8：Inbox live box 首行与 network snapshot 排列（2026-08-31）

原因：

真实 TUI capture 显示，`Inbox started` 作为历史 Status 产生的间隔被带入了 live box，导致框内第一内容行为空，第二行才显示 `syncing inbox...`，而 `local claims` 又紧随其后。用户确认该空行位置不符合预期。

问题：

Inbox 同步期间，activity 与 network snapshot 在虚线框内应如何排列？

选项：

- A：保持“空行 → `syncing inbox...` → `local claims`”。
- B：删除框内前导空行，直接显示“`syncing inbox...` → `local claims`”，两者之间不增加空行。

选择：B。

原因：

live box 标题已经提供了与 scrollback 的视觉边界，框内第一行应直接表达当前活动；`local claims` 是同一实时状态区的紧邻快照，不需要额外分段。

影响：

- 普通启动和 `/resume` handoff 后的 Inbox 共用这一排列。
- 只调整 `SyncingInbox` live box 的可见行序，不改变 transcript、Inbox 执行、输入队列、session 状态或 network snapshot 数据。
- 增加精确渲染回归，要求框内内容行依次为 `syncing inbox...`、`local claims 0`。

## ND-8 实施与验收结果（2026-08-31）

### 实现

- 在 live box 组装边界仅对 `SyncingInbox` 去除 activity 首个空行；未修改 transcript、scrollback 或通用 entry gap 规则。
- 新增精确渲染测试，要求框内内容严格依次为 `syncing inbox...`、`local claims 0`，中间及之前均无空行。

### 验证

- `scripts/check_version_consistency.sh`、`cargo fmt --check`、`cargo clippy -- -D warnings` 与 `cargo check` 通过。
- 新增定向测试和既有 Inbox 状态摘要测试通过；完整 `cargo test` 最终包含 2623 个 library tests、59 个 `acn` binary tests，以及 maintainer、router、storage/cleanup 与 doc tests，0 失败。
- 完整测试首次运行时，既有 maintainer 时序用例 `context_waits_in_same_analysis_without_calling_model` 单次返回了错误状态；该用例随后连续独立复跑三次均通过，完整 `cargo test` 再次运行也全部通过，未修改无关 maintainer 代码。
- Canonical tmux smoke 通过，captures 正常、`stderr.log` 为空且 tmux session 已清理。
- 真实 `deepseek-v4-flash`、真实 TUI、隔离真实 Supervisor 的 `/new` 与 `/resume` smoke 再次通过；`resume_history_then_inbox.txt` 实际捕获的 Inbox 框内第 1 行是 `syncing inbox...`，第 2 行紧接 `local claims 0`，无前导或中间空行。场景 `stderr.log` 为空，tmux 与隔离 Supervisor 已清理。

### Review 与最终对齐

- 本地复审确认改动只作用于 `SyncingInbox` live box 投影，不改变 `Inbox started` 的 scrollback、其他状态的 timeline 间隔或 session-switch 语义。
- 独立只读外部 code review 未发现现实可触发的 P0/P1，也未发现 P0/P1 级高价值测试缺口；外部 reviewer 未修改文件。
- 本次没有产生新的业务选择；ND-8 只固化用户确认的可见排列，D1–D14 与 ND-1 至 ND-7 的既有语义均未改变。
