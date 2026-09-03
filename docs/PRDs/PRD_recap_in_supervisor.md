# PRD：将 compact recap 移交 Supervisor

状态：已完成

## 背景与问题

当前 compact 在同一次前台操作中并行生成 compaction summary 与 session recap，并在二者都成功后同时推进 `frontier.committed_message_until` 与 `recapped_until`。模型偶发返回非法 JSON、缺失字段或错误字段类型时，即使 summary 已经成功，recap 失败仍会让自动 compact 失败并阻断后续主模型请求。

本需求把 recap 从 compact 的成功条件中移除，交给现有 supervisor 异步执行。Compaction summary 仍是继续主请求所必需的前台步骤，继续使用严格结构化校验和 `[agent.llm].retry_count`；recap 失败只形成后台 job 失败，不再回滚或否定已经成功的 summary。

本 PRD 替换 `docs/PRDs/PRD_compact_in_turn.md` 中“summary 与 recap 原子成功后同时推进两个 cursor”的旧语义。实施时必须同步修正该文档、`docs/architecture.md` 及相关用户文档，不能留下互相矛盾的当前行为描述。

## 目标

- Compact 只负责生成、验证和提交 provider context 所需的 summary。
- Compact 触发 recap 后立即 enqueue supervisor Recap job，不等待 recap 完成。
- Recap 独立推进 `recapped_until`，失败不影响当前 turn 或已提交 summary。
- Finalize 仍覆盖全部未 recap 消息和未消费的后台进程 completion，并优先于排队 Recap。
- 复用现有 supervisor、`finalize.lock` 与 `finalize_checkpoint.yaml`，不引入第二个 daemon、多代 checkpoint、CAS 或复杂恢复目录。
- 保持 canonical `messages.jsonl`、claim/dispute/trace 与团队上传边界不变。

## 非目标

- 不把 compaction summary 移入 supervisor。
- 不清洗、猜测修复或宽松解析非法模型 JSON。
- 不为 recap 增加系统通知、TUI 成功提示或实时完成订阅。
- 不合并重叠 Recap job，不增加 job 抢占或取消正在进行的 provider 请求。
- 不为极小概率恢复场景增加 checkpoint 多代保留、逐 claim CAS 或逐文件锁。
- 不改变 canonical message、background completion 或 claim 的业务含义。

## 已拍板语义（不可静默修改）

以下决策是本 PRD 的语义基线。实施中发现冲突时必须暂停对应实现，记录原因、选项与建议并取得新拍板；不得以重构或兼容为由改写既有决策。

### D1：Summary 与 Recap 解耦

- Compaction summary 成功后独立推进 `frontier.committed_message_until`。
- Recap 是否 enqueue、执行或重试成功，不影响 summary 成功语义。
- Summary 失败仍使 compact 失败，不推进 summary frontier。
- `recapped_until` 只由成功的后台 Recap 或 Finalize 推进。

### D2：Recap 输入与覆盖范围保持原语义

- Compact recap 的 canonical 消息输入始终来自 `messages.jsonl`。
- 每个请求的目标区间是 compact 触发时的：

  ```text
  [metadata.recapped_until, metadata.message_count)
  ```

- target 使用 compact 触发时冻结的 `message_count`，不改成 `compacted_until`。
- payload 继续携带当时读取的 `local_claims`，用于 claim 引用、新增与更新校验。
- Compact recap 不消费 background-process completion；该输入仍只由 Finalize 处理。
- Active-turn-only compact 不 enqueue Recap。

### D3：Enqueue 时点与并发

- Compact plan 和 summary 本地预算预检成功后，立即请求 enqueue Recap。
- Enqueue 发生在 summary provider 调用之前或与其同时调度；summary 与 supervisor Recap 可以并行。
- Enqueue 必须异步，不等待 job 执行完成，也不能阻塞 summary provider 调用。
- 即使 summary 随后失败，已 enqueue 的 Recap 仍可独立完成并推进 `recapped_until`。
- Summary 本地预算预检失败时不 enqueue Recap，保持“无效 summary 请求不额外消耗 recap 调用”的现有边界。

### D4：重叠 Recap job 不合并

- 每次 enqueue 创建带不可变 `recap_end_index` 的 Recap job。
- 同一 session 允许存在多个重叠 Recap job。
- Job 执行时以最新 `recapped_until` 为起点；cursor 已达到 target 时成功 no-op。
- 历史 Failed Recap job 不阻止后续 compact 创建新 Recap job。

### D5：Finalize 是全局高优先级、非抢占式任务

- Supervisor 使用两级优先级：`Finalize > Recap`。
- 同一优先级按 `created_at + job_id` 保持 FIFO。
- 优先级是全局的，不只作用于同一 session。
- 已经 Running 且已开始 provider 请求的 Recap 不被取消，完成当前 job attempt 后再选 Finalize。
- 尚未真正开始处理、仍在等待锁的 Recap 发现 session 已进入 `Finalizing` 时成功 no-op。
- Finalize 从最新 `recapped_until` 覆盖到最终 `message_count` 并关闭 session。
- Finalize 成功后，剩余排队或重试 Recap 读取到 `Closed`，标记 `Succeeded` no-op，并记录 `subsumed by finalize` 日志；不新增 `Skipped` job 状态。

### D6：Supervisor Recap 使用外层五次重试

- 所有由 supervisor 驱动的 recap，包括普通 Recap job 和 Finalize job 内部的最终 recap，单个 job attempt 只允许一次真实模型请求。
- 非法 JSON、JSON shape 错误、provider/transport 最终失败均使当前 job attempt 失败，并由 supervisor 重新排队。
- Supervisor 复用现有 `DEFAULT_SUPERVISOR_JOB_MAX_ATTEMPTS = 5`，因此单个 job 最多发起五次 recap 模型请求，不再叠加 `[agent.llm].retry_count`。
- 若存在可恢复 Prepared/Applied checkpoint，后续 attempt 优先恢复 checkpoint，不为恢复而重复调用模型。
- TUI 前台 fallback finalize 不属于 supervisor job，继续使用配置的结构化业务 retry。
- Compaction summary 继续使用 `[agent.llm].retry_count`，不受本决策影响。

> 后续语义更新（2026-09-03）：[`PRD_cau_supervisor_buffered_streaming.md`](PRD_cau_supervisor_buffered_streaming.md)
> 将“单个 job attempt 只允许一次真实模型请求”替换为“单个 job attempt 表示一次逻辑 recap
> 生成”。Supervisor 仍最多五个外层 attempt、不叠加 `[agent.llm].retry_count`、不启用
> max-token continuation；但每个 attempt 改为 Buffered streaming，并允许 transport failure
> 触发一次有限的流式 transport 恢复及 non-streaming fallback。非法 JSON、shape、引用和业务
> 校验失败仍直接结束当前 attempt。D6 的 checkpoint 恢复、前台 fallback finalize 与 compact
> summary 语义不变。

### D7：复用共享 Recap/Finalize checkpoint

- 继续使用物理文件 `finalize_checkpoint.yaml` 和现有 Prepared/Applied 数据形状。
- 将其语义与注释泛化为 Open-session Recap 和 Finalize 共用 checkpoint。
- Recap 与 Finalize 继续共用 session 级 `finalize.lock`，同一时刻只允许一个执行者读写共享 checkpoint。
- Recap 成功后只推进 `recapped_until`，不修改 session status、不 settle process、不 abandon delegation、不消费 background completion cursor。
- Finalize 可以用新的剩余区间覆盖上一条已完成 Recap 的 Applied checkpoint。
- 不新增 checkpoint 多代文件、目录或 CAS。

### D8：Compaction checkpoint 升级

- 新 compaction checkpoint 只保存 summary/frontier 提交所需数据，不再保存 recap prepared batch、trace 或 recap cursor 提交状态。
- 提升 compaction checkpoint schema。
- 遗留 v2 `compaction_checkpoint.yaml` 不兼容恢复；忽略后根据 canonical messages 重新生成 summary。
- 不为 v2 保留旧 summary+recap 原子恢复分支。

### D9：Agent 级知识写锁

- 新增一把 agent 级逻辑文件锁，例如 `<agent_home>/data/knowledge_apply.lock`。
- 该锁不是操作系统目录锁，只约束遵守 ACN 协议的进程。
- Inbox 内化与 supervisor Recap/Finalize 在“读取 local claims → 模型生成 → 本地 claim/trace/dispute 应用”期间互斥，因此二者串行。
- 普通主模型 turn 不获取此锁。
- 同一 agent 的 supervisor Recap 本来已由单 worker 串行；该锁主要防止后台 recap 与 TUI inbox 或其他 ACN 进程互相覆盖 claim。
- 团队上传可在本地知识应用完成并释放锁后继续；不得为了网络上传长期持有知识锁。

### D10：手动 `/compact` 的 recap-only 语义

- 当 summary frontier 已经追上、但 `recapped_until < message_count` 时，手动 `/compact` 仍 enqueue Recap 并立即返回，不在前台调用 recap 模型。
- Enqueue 成功不显示额外 TUI 文案。
- Enqueue 失败不把 session 或 `/compact` 置为 Error，显示 warning 后保持 Open；后续 `/compact`、自动 compact 或 Finalize 继续处理 backlog。

### D11：Recap 通知与 warning

- Recap enqueue 成功、job 成功和 job 最终失败都不发送系统通知。
- Recap enqueue 成功不写 TUI 成功提示。
- Enqueue 失败只显示：

  ```text
  Background recap could not be queued and will retry later.
  ```

- 已 enqueue job 的状态与错误保存在 supervisor jobs 和日志中。

### D12：TUI Compacting 与 Idle 语义

- `CompactionStarted` 清除旧 `last_contribution`，包括此前 inbox contribution。
- Compact 不再生成 `ContributionKind::Compact`，也不显示 claim/dispute 增删改统计。
- Compacting live box 保留现有 `local claims` 与最近 router consult；不显示 recap enqueue、recap 进度或 recap 结果。
- Compacting 标题继续为 `Compacting · Session history · Ns`。
- 自动 compact 完成后恢复当前 turn 的 `Working · Streaming response · Ns`。
- 手动 `/compact` 完成后进入 `Idle`。
- Idle 不显示 compact contribution。
- 每个 turn 收束时读取一次 `local_claim_count` 并发送 `LocalClaimsUpdated`；这不是周期轮询。
- 如果手动 `/compact` 后一直没有新 turn，后台 recap 产生的 claim 数量允许在 TUI 中暂时滞后，到下一次 turn、inbox 或 resume 时刷新。

### D13：Prompt JSON 转义与严格校验

- `prompts/session_recap.j2` 与 `prompts/session_compaction.j2` 都补齐 JSON 字符串转义约束。
- 明确要求双引号、反斜杠、换行、回车、制表符使用 JSON escape，禁止在字符串中直接输出 U+0000–U+001F 控制字符。
- 不增加 JSON 清洗、字段猜测、缺失字段默认值或宽松 parser。
- 非法输出按各自调用边界进入既定 retry：summary 使用 `[agent.llm].retry_count`，supervisor recap 使用 job 外层五次。

## 数据与状态流

### 自动 compact

```text
provider request preflight
  → 构造 compact plan
  → summary 本地预算预检
  → 发出异步 Recap enqueue 请求 ──────────────┐
  → 调用并严格校验 summary                    │
  → 写 summary-only compaction checkpoint     │ supervisor Recap job
  → 提交 compaction frontier                   │   → finalize.lock
  → 继续主 provider request                    │   → knowledge_apply.lock
                                                │   → 读取 messages/local claims
                                                │   → 单次 recap 模型请求
                                                │   → checkpoint + 本地应用
                                                └─  → 推进 recapped_until
```

### Finalize 到达时

```text
running Recap：完成当前 attempt
queued/requeued Recap：保持排队
Finalize：全局优先执行
  → 从最新 recapped_until 处理到 message_count
  → 消费 Finalize 专属 background completion
  → 关闭 session
剩余 Recap：Succeeded no-op (subsumed by finalize)
```

## Supervisor 协议与可见性

- `SupervisorJobKind` 增加 `Recap { session_id, recap_end_index }`。
- `SupervisorRequest` 增加 enqueue recap 请求；请求必须携带冻结 target。
- `SupervisorJobView` 与 `acn supervisor jobs` 显示 job kind，Recap 显示 target；不改变现有 Finalize retry 命令语义，除非实施中发现真实冲突并追加拍板。
- Recap job 固定 `notify_on_completion = false`。
- stale Running job 恢复必须按 kind 区分：
  - Finalize 保持现有 Finalizing 生命周期恢复语义。
  - Recap + Open 重新排队；Recap + Finalizing/Closed 视为被 Finalize 接管并成功 no-op。

## 审计与日志

- Compaction audit 只记录 summary 的 started/completed/failed，不再把 recap report 作为 compact 完成条件。
- Recap job 在 supervisor log 记录 session、target、attempt、成功推进后的 cursor、claim/dispute 数量以及失败错误。
- 被 Finalize 接管的 Recap 记录 `subsumed by finalize`。
- 不把 recap job 成功/失败写入 TUI transcript。

## 文档同步

实现阶段必须同步更新：

- `docs/PRDs/PRD_compact_in_turn.md`：删除 summary/recap 原子提交语义，改为本 PRD 的独立 cursor 语义。
- `docs/architecture.md`：将 Finalize Supervisor 更新为同时承载 Recap 与 Finalize 的 supervisor。
- 相关配置、用户指南、help 或日志文本（如果实际行为发生变化）。

## 分阶段 Planning 与验收

### 阶段切换硬约束

- 每次进入下一阶段前，主执行者必须完整重读本 PRD。
- 重读后确认当前代码、测试和下一阶段计划没有偏离 D1–D13。
- 工作中产生新的业务或用户可见语义选择时，必须先在“新增拍板记录”中追加：原因、可选方案、最终选择与影响，再继续实现。
- 新记录只能追加，不能删除、重写或弱化 D1–D13。

### Phase 0：PRD 固化与现状基线

Todo：

- [x] 写入全部已拍板语义、非目标、数据流和阶段验收。
- [x] 对照当前 main 的 compact、finalize、supervisor、TUI、prompt 与持久化实现复核可实施性。
- [x] 标出需要被本 PRD 替换的旧文档语义。

验收：

- 本 PRD 内容与用户逐项拍板一致。
- 没有把未拍板的过度恢复机制写入实施范围。
- 工作区既有无关改动已识别并保持不动。

### Phase 1：Summary-only Compact 与共享 Recap 引擎

进入本阶段前完整重读本 PRD。

Todo：

- [x] 将 committed compact 的 checkpoint 与 commit 路径收窄为 summary-only。
- [x] 自动与手动 compact 在 summary 本地预算预检后发出异步 Recap enqueue 请求，不等待 recap。
- [x] 增加 `recap_existing_session_until`，复用 finalize 区间校验、checkpoint 和 prepared batch 应用。
- [x] Recap 成功只推进 `recapped_until`；Finalize 继续关闭 session并消费 background completion。
- [x] 泛化 `finalize_checkpoint.yaml` 注释和恢复语义。
- [x] v2 compaction checkpoint 不恢复。

验收：

- Summary 成功、Recap enqueue/执行失败时 compact 仍成功并推进 summary frontier。
- Summary 失败时不推进 summary frontier；已 enqueue Recap 可独立推进 recap cursor。
- Active-only compact 不 enqueue Recap。
- Recap-only 手动 `/compact` 能请求 enqueue。
- Finalize 可以从后台 Recap 推进后的 cursor 继续处理剩余区间。

### Phase 2：Supervisor Recap Job、优先级与重试

进入本阶段前完整重读本 PRD。

Todo：

- [x] 增加 Recap request/job/view 与持久化序列化。
- [x] 实现不可变 target、重叠 job、cursor no-op 与 Failed job 不阻塞新 enqueue。
- [x] 实现全局 `Finalize > Recap`、同优先级 FIFO 的非抢占式选择。
- [x] 实现 Recap/Finalize supervisor recap 的“外层五次、内层单次”。
- [x] 实现 Open/Finalizing/Closed 与 stale Running 的 kind-specific 收敛。
- [x] Recap 禁止系统通知；补齐 supervisor jobs 可见 kind/target。

验收：

- 同 session 两个重叠 Recap 只处理剩余范围。
- Running Recap + queued Recap + Finalize 时，当前 attempt 后先执行 Finalize，其余 Recap no-op。
- 其他 session 的 Finalize 也能全局优先于排队 Recap。
- 非法 recap JSON 恰好按 job attempt 重试，单个 attempt 不叠加 LLM retry。
- 第五次失败后 job 为 Failed，后续新 compact 仍可创建 Recap job。
- Recap 成功/失败均不发送通知。

### Phase 3：知识锁、Prompt 与 TUI 语义

进入本阶段前完整重读本 PRD。

Todo：

- [x] 增加 agent 级 `knowledge_apply.lock`，让 inbox 与 recap/finalize 的知识快照及本地应用串行。
- [x] 不在团队上传期间长期持锁。
- [x] 强化 recap 与 compaction prompt 的 JSON escape 约束。
- [x] `CompactionStarted` 清除旧 contribution。
- [x] 删除 compact contribution 生成和 claim/dispute 增删改显示。
- [x] 保留 Compacting/Working/Idle 的 local claims 与 router 行。
- [x] 每个 turn 收束时刷新一次 local claim count。
- [x] Enqueue 成功静默；失败使用唯一固定 warning，且不进入 Error。

验收：

- Inbox 与 recap 并发启动时只有一个进入“知识快照 → 模型 → 本地应用”区间。
- 普通 turn 不获取知识锁。
- Compacting 框显示标题、local claims、router；不显示旧 contribution、recap 或 compact claim 统计。
- 自动 compact 后恢复 Working；手动 compact 后恢复 Idle。
- Turn 收束后 local claims 数量刷新。
- 两个 prompt 都包含完整 JSON 控制字符转义要求。

### Phase 4：定向测试、文档同步与完整 Verify

进入本阶段前完整重读本 PRD。

Todo：

- [x] 补齐 SessionEngine、supervisor、session store、TUI state/render 和 prompt 单元/集成测试。
- [x] 更新 `PRD_compact_in_turn.md`、`architecture.md` 及实际受影响文档。
- [x] 按 `.agents/skills/verify/SKILL.md` 运行：
  - [x] `scripts/check_version_consistency.sh`
  - [x] `cargo fmt --check`
  - [x] `cargo clippy -- -D warnings`
  - [x] `cargo test`
  - [x] `cargo check`
- [x] TUI 有改动，额外运行技能规定的 canonical tmux smoke。

验收：

- 所有定向测试与完整 verify 通过。
- 文档不再保留 compact/recap 原子提交的旧当前语义。
- 无关工作区文件未被修改或删除。

### Phase 5：真实 LLM TUI Smoke Test

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md`。

Todo：

- [x] 使用 `source export_env.sh` 后的真实 LLM 配置和真实 ACN TUI，不使用 fake provider 冒充本验收。
- [x] 使用独占临时 `acn_home` / 测试 config，避免污染用户正式 session、claims、supervisor jobs 或终止共享 supervisor。
- [x] 编写位于 `target/` 的聚焦 tmux flow，至少覆盖：
  - [x] 真实 TUI 启动并进入 Open。
  - [x] 产生可压缩的真实会话内容并执行手动 `/compact`。
  - [x] Compacting live box 标题可见。
  - [x] Compacting/Idle 不出现 `compact · claims`、`recap queued` 或 recap 成功提示。
  - [x] enqueue 成功后 supervisor jobs 可观察到 Recap kind/target。
  - [x] TUI 可继续完成真实主模型 turn。
  - [x] `stderr.log` 为空且 tmux session 清理完成。
- [x] 在可控条件下补充 enqueue 失败 warning 的非真实网络单测；真实 smoke 不破坏 supervisor 来制造失败。

验收：

- 保存关键 tmux capture、supervisor jobs 输出和 stderr 供复核。
- 真实模型调用成功，compact 与后台 recap 均实际运行。
- TUI 用户可见语义与 D10–D13 一致。

### Phase 6：针对性 Code Review 与 P0/P1 修复

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/code-review/SKILL.md`。

Todo：

- [x] 先对实际 diff 与运行路径做本地 review。
- [x] 再运行一次独立、只读的 `codex exec --json` review；明确禁止外部 reviewer 修改文件、调用 code-review skill、运行嵌套 codex 或使用 delegation。
- [x] Review 聚焦真实可触发的业务状态机、持久化、锁顺序、异步进程生命周期、TUI 误导和高价值测试缺口。
- [x] 修复所有非过度防御、具有真实触发条件和实质影响的 P0/P1。
- [x] 不为极小概率 P2/P3、纯样式意见或推测性恢复场景扩大实现。
- [x] 修复后重跑受影响定向测试、完整 verify 和必要的真实 TUI smoke。

验收：

- 本地与外部 review 结论合并去重。
- 所有符合边界的 P0/P1 已修复并验证；若无发现，明确记录无发现。
- 不以“防御性”名义引入本 PRD 非目标中的复杂机制。

### Phase 7：最终 PRD 对齐审计

进入本阶段前完整重读本 PRD。

Todo：

- [x] 逐条对照 D1–D13、各 Phase 验收与实际代码/测试/文档。
- [x] 检查新增拍板记录是否完整、是否有旧语义被静默改变。
- [x] 汇总 verify、真实 LLM TUI smoke、code review 与修复后的复验结果。
- [x] 将本 PRD 状态更新为“已完成”，勾销已完成 Todo，并记录任何明确延期项；不得把未完成要求伪装为完成。

验收：

- 整体实现与本 PRD 一致。
- 所有必须验证项有可复核结果。
- 没有未解释的语义偏差、P0/P1 或文档冲突。

## 实施与验收结果

完成日期：2026-08-27。

### D1–D13 对齐审计

- D1–D3：Compact 已收窄为 summary-only checkpoint/frontier；committed history 在 summary 本地预算预检通过后异步发出冻结 `message_count` target 的 Recap 请求，summary 与 recap 独立完成，active-only compact 不投递。
- D4–D6：Supervisor 持久化不可变 Recap target，允许重叠 job；按全局 `Finalize > Recap`、同级 FIFO 非抢占执行；supervisor 每个 attempt 只发一次 recap 模型请求，stale Running 在第 5 次后直接 Failed，不会产生第 6 次自动请求。
- D7–D9：Open Recap 与 Finalize 共用 `finalize.lock`、`finalize_checkpoint.yaml` 和 Prepared/Applied 形状；compaction checkpoint 已升级为 summary-only schema 3；inbox 与 recap/finalize 共用 `knowledge_apply.lock`，本地应用与 checkpoint 提交在锁内，团队网络上传在锁外。
- D10–D13：recap-only `/compact` 异步投递并返回；成功静默、失败只显示约定 warning；Compact 不再生成 contribution，Compacting/Idle 保留 local claims/router；turn 收束刷新 claim 数；两个 prompt 均保留严格 JSON parser 并补齐控制字符转义要求。
- 文档已同步更新 `PRD_compact_in_turn.md`、`architecture.md`、README、用户指南、核心行为、配置说明、发布说明和相关历史 PRD；检索到的旧 summary/recap 原子语义仅保留在本 PRD 的历史问题背景与明确替换说明中。

### 完整 Verify

- `scripts/check_version_consistency.sh`：通过，版本 `0.2.5`。
- `cargo fmt --all -- --check`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过。
- `cargo test --all-targets --all-features`：通过；lib 2558、`acn` 59、maintainer 2、router 2、cleanup CLI 集成 1、session storage 集成 5，示例测试目标无失败。
- `cargo check --all-targets --all-features`：通过。
- canonical tmux smoke：通过，capture 保存于 `target/tui-smoke/`。

### 真实 LLM TUI Smoke

- 在 code review 修复完成后，使用真实 `deepseek-v4-flash`、真实 ACN TUI 和独占测试 `acn_home` 再次执行通过。
- 最终场景 session 为 `session_b75fdb51`；手动 compact 显示 `Compacting · Session history · 0s` 与 `local claims 0`，完成后回到 Idle，未显示 compact contribution、recap queued 或 recap 成功提示。
- Supervisor Recap job `job_1787806277578_173f41e4` 冻结 target 7，在 attempt 1 成功；session 最终 `recapped_until=7`，summary frontier `committed_message_until=5`。
- Compact 后真实主模型 turn 成功返回 `REAL-AFTER-COMPACT`；`stderr.log` 为 0 bytes，tmux 与 supervisor 已清理。
- 可复核 capture、job、log、session metadata 和脚本保存在 `target/tui-scenarios/recap-supervisor-real-llm/`。

### Code Review 与修复

- 本地 review 修复了 CAU 多消息批次中“前缀已应用、后续失败时可能遗漏前缀团队上传”的 P1：每条本地 effect 标记 Applied 前先持久化 durable pending upload；同时修正 Running Recap 与 Finalize 交叠时已真实推进游标却误记为 `subsumed` 的审计错误。
- 按 code-review skill 运行了恰好一次独立只读 `codex exec --json` review；外部 reviewer 报告 2 个 P1、无 P0，未修改文件。
- 外部 P1 一：Prepared checkpoint 本地应用后曾先释放知识锁、网络上传后才写 Applied，可能由后续 inbox 写入较新 claim 后在恢复时被旧 Prepared 覆盖。现已改为锁内完成“本地应用 → pending upload 持久化 → checkpoint Applied”，仅网络发送在锁外。
- 外部 P1 二：第 5 次 Running job 崩溃后 stale recovery 曾可重新排队并执行 attempt 6。现已在 stale recovery 按保留的 attempt 计数直接置 Failed，并补回归测试。
- 修复后受影响定向测试、完整 verify、canonical tmux smoke 与真实 LLM TUI smoke 均通过；最终无未处理 P0/P1。

### 延期项

无。

## 新增拍板记录（只追加）

本次实施未产生新的业务或用户可见语义拍板；code review 修复均用于落实既有 D5、D6、D7、D9 与原有 inbox 提交语义，没有改写 D1–D13。

后续若出现新决策，按以下格式追加，禁止回写 D1–D13：

```text
### ND-N：标题（YYYY-MM-DD）

原因：

选项：
- A：...
- B：...

选择：A/B

影响：
```

### ND-1：同 session Finalize 可在 Prepared 前抢占 Running Recap（2026-08-27）

原因：

原 D5 的全局优先级只影响下一次 job 选择。若同一 session 的 Recap 正在等待锁或等待模型响应，Finalize 即使已经到达，也必须等待整个 Recap attempt 完成。由于 Finalize 能从最新 `recapped_until` 覆盖该 Recap 尚未提交的完整范围，在 Recap 尚无 durable Prepared checkpoint 时继续等待没有必要；但一旦 Prepared 已持久化，丢弃它会浪费已经校验成功的模型结果，并破坏共享 checkpoint 的恢复边界。

选项：

- A：保留 D5 的完全非抢占语义，任何 Running Recap 都完成当前 attempt 后再选择 Finalize。
- B：只允许同 session Finalize 在 Running Recap 持久化 Prepared 前取消它；Prepared 后继续完成。其他 session 的 Finalize 不取消该 Recap。
- C：任何 session 的 Finalize 都可以取消当前 Running Recap，以获得绝对全局抢占。

选择：B。

影响：

- 本决策只覆盖原 D5 中“已经 Running 的 Recap 不被取消”这一条；D5 的全局 `Finalize > Recap`、Finalize 同级 `created_at + job_id` FIFO、非并发执行和其余语义保持不变。
- 只有同 session 的 Finalize job 已成功持久化入队后，才允许向当前 Running Recap 请求抢占；Finalize enqueue 失败不得取消 Recap。
- 抢占边界是匹配当前区间的 `FinalizeCheckpointStatus::Prepared` 已成功原子写入 `finalize_checkpoint.yaml`，而不是“模型已经返回”或“内存中已经解析成功”。Applied checkpoint 同样属于不可抢占侧。
- Finalize 在 Prepared 前先取得抢占判定权时，Running Recap 取消等待锁或 provider future，丢弃尚未提交的模型结果，不写 claim/trace/dispute/checkpoint，不推进 `recapped_until`。该 Recap attempt 已消费，job 标记 `Succeeded` no-op、记录 `preempted before Prepared` 与 `subsumed by finalize`，不重试、不通知。
- Running Recap 先取得 Prepared 提交权时，Finalize 不取消它；Recap 完成本地应用、Applied checkpoint、cursor 和既有锁外团队上传，然后 Supervisor 重新选队列。
- 抢占只让当前 Recap 尽快结束，不给触发抢占的 Finalize 插队权。若更早到达的其他 session Finalize 已排队，当前 Recap 结束后仍先执行更早的 Finalize，再执行同 session Finalize。
- 同 session Finalize 最终从当时最新的 `recapped_until` 覆盖到最终 `message_count`。如果它耗尽五次 attempt，session 继续保持 Finalizing，沿用既有手动 retry 语义；被抢占 Recap 不恢复重试。
- Queued Recap 仍按 D4 保持不可变 target、互不合并；本决策不引入 Recap 合并、抢占其他 session Recap、新 job 状态或 checkpoint CAS/多代目录。

## 增量 Planning 与验收

### Phase 8：同 session Prepared 前抢占

进入本阶段前完整重读本 PRD。

Todo：

- [x] 在 Supervisor worker 与 enqueue handler 之间登记当前 Running Recap 的 session、job 与抢占控制器。
- [x] Finalize job 成功持久化后，只向同 session Running Recap 请求取消。
- [x] 在锁等待、单次 provider 请求与 Prepared 原子写之间建立明确线性化边界；Prepared 提交与抢占请求不得同时获胜。
- [x] Prepared 前取消时不产生本地知识副作用、不推进 cursor，并将 Recap 收敛为 `Succeeded` no-op。
- [x] Prepared 或 Applied 已存在时忽略抢占，让 Recap 完成既有恢复与提交路径。
- [x] 保持全局 Finalize FIFO、五次 retry、Queued Recap 不合并以及 TUI/通知语义不变。

验收：

- 同 session Finalize 能取消一个尚未 Prepared、正在等待 provider 的 Running Recap；Recap 无 checkpoint/cursor/claim 副作用，Finalize 随后覆盖完整范围。
- Finalize 与 Prepared 写入竞争时只能有一个边界获胜；Prepared 成功后 Recap 必须完成，不能留下 Prepared 未应用。
- 其他 session Finalize 不取消当前 Recap。
- 更早排队的其他 session Finalize 仍先于触发抢占的同 session Finalize。
- 多个 Queued Recap 仍按 FIFO 逐个处理剩余区间，不合并。

### Phase 9：Verify、Code Review 与修复

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/verify/SKILL.md` 与 `.agents/skills/code-review/SKILL.md`。

Todo：

- [x] 运行抢占、Prepared 竞争、跨 session 隔离、全局 FIFO、stale recovery 与五次 retry 的定向测试。
- [x] 运行版本一致性、fmt、Clippy、全量 test 和 check；仅在实际修改 TUI/交互行为时追加 canonical tmux smoke。
- [x] 先做本地 review，再运行恰好一次独立只读 `codex exec --json` review。
- [x] 修复所有具有真实触发条件、非过度防御的 P0/P1，不扩大到极小概率 P2/P3 或复杂恢复机制。

验收：

- 所有定向测试与完整 verify 通过。
- 本地与外部 review 合并去重，所有符合范围的 P0/P1 已修复。
- 没有改变 D4、不相关的 D1–D13、TUI 成功/失败提示或通知语义。

### Phase 10：最终增量对齐

进入本阶段前完整重读本 PRD。

Todo：

- [x] 对照 ND-1、D1–D13、实现、测试、日志和文档逐项审计。
- [x] 将增量验收、review 与修复结果追加到本 PRD，勾销增量 Todo，并恢复状态为“已完成”。

验收：

- ND-1 的 Prepared 线性化、同 session 限定与全局 FIFO 均有可复核测试。
- 最终无未解释语义偏差、未处理 P0/P1 或未记录的新拍板。

## ND-1 增量实施与验收结果

完成日期：2026-08-27。

### 语义与实现对齐

- Supervisor 只维护一个当前 Running Recap 登记，包含 job、session 与抢占控制器；worker 的选队和 Running 登记与 Finalize enqueue 共用既有 `lifecycle_gate`，避免 Finalize 插入选队与登记之间而错过抢占。
- Finalize enqueue 只有在 job 成功持久化后才请求取消；请求严格匹配同 session。其他 session Finalize 不触发取消，触发抢占的 Finalize 也不获得插队权。
- 抢占控制器在等待 `finalize.lock`、等待 `knowledge_apply.lock` 和单次 provider future 时可取消；Prepared checkpoint 的原子写与取消请求共用 phase 锁，只允许一方先取得边界。
- Prepared 前取消返回无副作用 report；Supervisor 将该 attempt 收敛为 `Succeeded` no-op，记录 `preempted before Prepared` 与 `subsumed by finalize`，不重试、不通知。
- Prepared/Applied 获胜后继续完成既有本地应用、pending upload、Applied checkpoint 与 cursor 路径。若 attempt 在 Prepared 后失败或进程退出，高优先级 Finalize 会先恢复并推进该 message-only checkpoint 前缀，再重新读取 cursor，处理剩余消息与 Finalize 专属 background completion；不会覆盖或绕过 durable Prepared。
- 全局 `Finalize > Recap`、Finalize 同级 `created_at + job_id` FIFO、五次外层 retry、Queued Recap 不合并、TUI 与通知语义均未改变。
- ND-1 是“非目标”中“不增加 job 抢占”、D5 中 Running Recap 非抢占条目以及旧 Finalize 状态流的唯一后续例外：只允许同 session、只发生在 Prepared 前。其余非抢占边界保持原义；该解释通过追加 ND-1 生效，不回写旧拍板。

### 定向测试

- Prepared 前取消：真实阻塞 provider future 被 drop，无 checkpoint、claim 或 cursor 副作用；随后 Finalize 使用完整消息范围并关闭 session。
- Prepared 竞争：取消先赢时禁止写 checkpoint；Prepared 原子写先赢时取消请求等待并最终失败；Recap 完成后不再接受迟到取消。
- Prepared 后恢复：覆盖短 Recap 前缀后继续剩余消息，以及 Recap 已覆盖全部消息后再合入 Finalize 专属 background completion，两种路径均不重复 recap 已 Prepared 的消息。
- Session 隔离与顺序：其他 session Finalize 不取消 Running Recap；更早的其他 session Finalize 仍先于触发抢占的同 session Finalize。
- 既有 stale Running、第五次失败和重叠 Recap 不合并测试继续通过。

### 完整 Verify

- `scripts/check_version_consistency.sh`：通过，版本 `0.2.5`。
- `cargo fmt --all -- --check`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过。
- `cargo test --all-targets --all-features`：通过；lib 2566、`acn` 59、maintainer 2、router 2、cleanup CLI 集成 1、session storage 集成 5，示例测试目标无失败。
- `cargo check --all-targets --all-features`：通过。
- 本增量未修改 TUI 或交互行为，按 Phase 9 约束不重复 canonical tmux smoke；此前 Phase 4–6 的 canonical 与真实 LLM TUI smoke 结果保持有效。

### Code Review 与修复

- 本地 review 修复了 worker 在“选中 Recap、尚未登记 Running”期间可能插入 Finalize 并错过优先级/抢占的竞态，并用 `Finished` phase 封住 Recap 已结束后的迟到取消。
- 按 code-review skill 运行了本增量恰好一次独立只读 `codex exec --json` review；reviewer 未修改文件，报告 1 个 P1、无 P0。
- 外部 P1：Prepared 已落盘后若 Recap 在 Applied/cursor 前失败，Recap 会重排队而 Finalize 先执行，可能覆盖 message-only Prepared，或因新增 background completion 导致 hash 不同而稳定失败。现已让 Finalize 在生成最终范围前恢复匹配当前 cursor 的共享 recap checkpoint 前缀，并补两条端到端回归测试。
- P1 修复后重跑全部定向测试与完整 verify，最终无未处理 P0/P1。

### 新拍板与延期

- 本增量没有产生 ND-1 之外的新业务或用户可见语义选择；Prepared 后 checkpoint 前缀恢复用于落实 D6、D7 与 ND-1，不新增 checkpoint 格式、job 状态或恢复目录。
- 延期项：无。

## 补充 Code Review 与真实 LLM TUI 验收（2026-08-27）

### Phase 11：补充审查、修复与运行态复验

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/code-review/SKILL.md`、`.agents/skills/verify/SKILL.md` 与 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md`。

Todo：

- [x] 重跑真实 LLM 的 compact、后台 Recap 与同 session Finalize Prepared 前抢占流程。
- [x] 对完整实际 diff 做本地 review，并运行本阶段恰好一次独立只读 `codex exec --json` review。
- [x] 复核并修复所有具有真实触发条件、非过度防御的 P0/P1。
- [x] 为修复补针对性回归测试，并重跑完整 verify、canonical tmux smoke 与真实 LLM TUI smoke。
- [x] 对照 D1–D13、ND-1 与实际结果做最终审计。

验收：

- 本地与独立 review 结论合并去重，最终无未处理 P0/P1。
- Supervisor 单个 job attempt 对所有 provider adapter 都只发一次真实模型请求。
- Applied checkpoint 恢复不会遗漏已经 durable staged 的 Maintainer 上传。
- 同一 Claim 的并发旧/新版本上传不会以旧版本覆盖较新 mirror。
- 真实 TUI 能复现并通过 ND-1 抢占，stderr 为空；完整 verify 全绿。

### Code Review 结果与修复

- 本地 review 未发现额外 P0/P1；本阶段按 skill 运行了恰好一次独立只读 `codex exec --json` review，reviewer 未修改文件，报告 3 个 P1、无 P0。
- P1 一：Applied checkpoint 表示本地知识已经提交，但进程可能在锁外 Maintainer 上传前退出；原 Applied 恢复分支只推进 cursor 或关闭 session，未补传 durable pending。现已让 current recap prefix、legacy finalize 与同范围 Applied 三条恢复路径都先执行既有 pending upload flush，再推进状态，并补端到端恢复测试。
- P1 二：TUI inbox 与 supervisor recap/finalize 可并发发起 Maintainer 上传；旧 Claim 请求若晚于新请求完成，会把 mirror 回退为旧版本。现已增加 agent 级网络 delivery 单飞锁；新批次仍可在旧请求进行时并发进入 durable pending，网络交付与 reconcile 按序执行，且 `knowledge_apply.lock` 不跨网络等待。两套 runner 共享目录的并发回归测试验证上传顺序为旧版本后新版本，最终 pending 清空。
- P1 三：`retry_count_override = 0` 只关闭 HTTP retry，三个 adapter 原本仍可能因 max-token 自动发起 continuation，违反 D6 的“一次真实模型请求”。现已在 `ProviderRequest` 显式携带 continuation 策略，supervisor 的 `generate_json_validated_once` 将其关闭；Anthropic、OpenAI Chat 与 OpenAI Responses 都有回归测试验证 max-token 时只收到一个 HTTP 请求并返回 `ProviderStop::MaxTokens`，随后由 supervisor 外层 job retry 处理。
- 三项修复均落实 D6、D7、D9 与既有 durable pending 语义，没有新增业务或用户可见拍板，没有改变 D1–D13 或 ND-1。

### 完整 Verify 与 TUI 结果

- `scripts/check_version_consistency.sh`：通过，版本 `0.2.5`。
- `cargo fmt --all -- --check`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过。
- `cargo test --all-targets --all-features`：通过；lib 2571、`acn` 59、maintainer 2、router 2、cleanup CLI 集成 1、session storage 集成 5，示例测试目标无失败。
- `cargo check --all-targets --all-features`：通过。
- canonical tmux smoke：通过，capture 保存在 `target/tui-smoke/`。
- 修复后的真实 LLM TUI smoke 使用 `deepseek-v4-flash` 与独占测试 `acn_home`，session 为 `session_bfdd911e`。首次 Recap `job_1787814270611_c863d8b0` 正常成功；第二次 Recap `job_1787814271516_c9755b72` 在 Prepared 前被同 session Finalize `job_1787814271583_2f101988` 抢占并 `Succeeded` no-op，Finalize 随后成功关闭 session。
- smoke 结果为 `preempted-before-prepared`；TUI 显示 `Compacting · Session history`、`local claims 0` 与 `Background finalize enqueued`，没有 recap 成功提示或 compact contribution；TUI、jobs 与首次 jobs 三个 stderr 均为 0 bytes。
- 可复核脚本、capture、job 列表、supervisor log 与 metadata 路径保存在 `target/tui-scenarios/recap-finalize-preemption-real-llm/`。

### 最终对齐

- D1–D13、ND-1、TUI 文案、通知边界、五次外层 retry、Prepared 抢占线性化及队列优先级均未改变。
- 本阶段没有新增拍板或延期项；最终无未解释语义偏差、未处理 P0/P1 或未完成验收。

## 最终修复后复审与验收（2026-08-27）

### Phase 12：Inbox staging 原子边界修复与再次 Review

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/code-review/SKILL.md` 与 `.agents/skills/verify/SKILL.md`。

Todo：

- [x] 对 Phase 11 修复后的完整 diff 做本地 review，并运行本阶段恰好一次独立只读 `codex exec --json` review。
- [x] 核实 reviewer 报告的 inbox Claim 版本回退竞态具有真实触发条件和实质影响，不属于过度防御。
- [x] 以最小改动修复 deprecated Policy 与普通 PolicyUpdate 的本地应用/pending staging 原子边界。
- [x] 补两条确定性并发回归测试，并重跑相关上传顺序测试与完整 verify。
- [x] 对最终修改后的锁序、失败语义和测试覆盖再次做本地 code review。

验收：

- Inbox 与 Recap/Finalize 对同一 Claim 的提交顺序同时约束本地 Claim 和 durable pending；较旧 inbox 结果不能在较新 supervisor 结果之后进入上传队列。
- `knowledge_apply.lock` 只覆盖本地知识应用和 pending staging，不跨 Maintainer 网络请求。
- 最终修改后的本地 review 无未处理 P0/P1，完整 verify 全绿。

### Review 发现、修复与复验

- 本阶段独立 reviewer 报告 1 个 P1、无 P0：deprecated Policy 和普通 PolicyUpdate 原先在写完本地 Claim 后先释放 `knowledge_apply.lock`，再进入 pending staging。另一个进程可在该间隙应用并上传较新 Claim，随后旧 inbox 结果再上传；Maintainer mirror 当前按 Claim ID 无条件覆盖，因此镜像可能稳定回退到旧版本。
- 该问题不是推测性恢复防御：TUI inbox 与 supervisor 是可并发的不同进程，Maintainer 没有版本拒绝；一旦旧上传最后成功且 pending 清空，Router 与其他 Agent 可持续读取旧镜像，直到未来恰好再次上传该 Claim。
- 修复保持现有机制：两个 inbox 路径都在持有 `knowledge_apply.lock` 时调用既有 `stage_maintainer_batch`，完成本地 Claim/trace/dispute 与 pending 的同序提交；随后释放知识锁，只用空 batch 触发现有 delivery。没有新增锁、队列、checkpoint 或恢复目录。
- 回归测试分别冻结 pending 文件锁，等待 deprecated Policy 或普通 PolicyUpdate 已写入本地 Claim，再验证知识锁仍不可获取；旧实现会在此处暴露锁间隙，新实现稳定阻塞到 pending staging 完成。
- 相关定向测试通过：两条新增 staging 测试、同 Claim 新旧版本 delivery 顺序、pending 先于网络写入、PolicyUpdate trace/source 与 inbox receipt 路径均通过。
- 完整 verify 通过：版本 `0.2.5`；fmt、Clippy、`cargo check` 全绿；`cargo test --all-targets --all-features` 中 lib 2573、`acn` 59、maintainer 2、router 2、cleanup CLI 1、session storage 5，示例目标无失败。
- 最终代码修改后再次执行本地 review：确认统一锁序为 `knowledge_apply → pending stage`，网络 delivery 在知识锁外；未发现反向持锁路径或新的 P0/P1。
- 本阶段没有修改 TUI、交互文案或 recap/finalize 状态机，因此不重复 canonical tmux 或真实 LLM TUI smoke；Phase 11 的修复后真实 smoke 结论继续有效。
- 本修复落实 D9 与既有 durable pending 语义，不改变 D1–D13、ND-1 或任何用户可见行为；没有新增拍板与延期项。

## Dispute durable staging 修复与最终外部复审（2026-08-27）

### Phase 13：台账提交顺序、Solo 边界与外部复审闭环

进入本阶段前完整重读本 PRD，并完整遵循 `.agents/skills/code-review/SKILL.md` 与 `.agents/skills/verify/SKILL.md`。

Todo：

- [x] 对 Phase 12 修复后的完整 diff 做本地 review，并运行独立只读外部 review。
- [x] 修复 CAU、普通 PolicyUpdate 与 Recap/Finalize 在 durable pending 之前记录 dispute 台账的 P1。
- [x] 为三条路径补确定性的 `pending → reported ledger` 顺序回归测试。
- [x] 修复外部复审发现的 Solo CAU staging no-op 后仍记录 reported ledger 的 P1。
- [x] 补 Solo CAU 回归测试，并重跑定向测试与完整 verify。
- [x] 对最终修改再次运行独立只读外部 review，确认无剩余 P0/P1。

验收：

- Team 模式的 CAU、普通 PolicyUpdate 与 Recap/Finalize 都先持久化 dispute pending，再记录 reported-claim-set ledger。
- Solo 模式不创建 Maintainer pending，也不记录未实际暂存 dispute 的 reported ledger。
- Maintainer 网络 delivery 保持在 `knowledge_apply.lock` 外。
- 最终本地与独立外部 review 无未处理 P0/P1；完整 verify 全绿。

### Review 发现与修复

- Phase 12 后的独立 reviewer 报告 1 个 P1、无 P0：三个 dispute 生成路径曾先写“已报告”台账、后写 durable pending。若台账原子写成功后 pending staging 因普通 I/O 错误失败，重放会过滤该 dispute，造成永久漏报。
- 修复统一为“本地知识应用 → durable pending staging → reported ledger → Applied effect/checkpoint”，网络 delivery 仍在知识锁外。CAU Prepared journal 与 Recap/Finalize Prepared checkpoint 的重放继续复用既有文件和幂等队列，没有新增 checkpoint、CAS、锁或恢复目录。
- 三条确定性回归测试在 `record_claim_set` 时直接读取 pending 文件并确认对应 dispute 已存在，分别覆盖 CAU、普通 PolicyUpdate 和 Recap/Finalize 共用提交 helper。
- 修复后的下一轮独立 reviewer 报告 1 个 P1、无 P0：CAU 移动台账写入位置时漏掉原有 `team_services_configured()` 守卫。Solo 模式下 staging 合法 no-op，但仍会写 reported ledger；以后恢复 Team 模式时同 claim-set 会被永久过滤。
- 该发现不是过度防御：Solo 模式会继续处理已有本地 inbox/effect，且模式切换后继续使用同一 agent runtime。修复只恢复原有 Team guard；新增 `AgentRunner::new_local` 回归测试确认 pending 文件和 reported ledger 都不会创建。

### 验证与最终外部复审

- 四条定向测试通过：Team CAU、Team PolicyUpdate、Recap/Finalize 的 staging 顺序，以及 Solo CAU 不写 pending/ledger。
- 完整 verify 通过：版本 `0.2.5`；`cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo check` 全绿；`cargo test` 中 lib 2575、`acn` 59、maintainer 2、router 2、cleanup CLI 1、session storage 5，doc tests 无失败。
- 最终修复后再次按 code-review skill 运行独立只读 `codex exec --json` review。Reviewer 未修改文件，明确报告：没有可现实触发的 P0/P1，也没有达到 P1 的高价值测试缺口。
- Reviewer 复核确认：Solo guard 有效；三条 Team 路径均保持 durable staging 在 ledger 之前；CAU/Finalize checkpoint 可重放；Applied 恢复先 flush durable pending；Maintainer 网络交付保持在知识锁外并由既有 delivery 单飞锁串行。
- 本阶段没有修改 TUI、交互文案或 recap/finalize 状态机，因此不重复 canonical tmux 或真实 LLM TUI smoke；Phase 11 的修复后真实 smoke 结论继续有效。
- 本阶段没有产生新的业务或用户可见拍板，不改变 D1–D13、ND-1；没有延期项。
