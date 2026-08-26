# PRD: Maintainer Dispute 自裁决

> 状态：已实现。本文保留 Maintainer Dispute 自裁决的产品决策与验收边界。

## 背景与目标

Agent 会在团队 Claim 互相冲突、适用范围不清或发生生命周期演进时向 Maintainer 上报 Dispute。Maintainer 需要结合直接 Claim、来源图、团队治理 Policy 与 Router 补充知识形成一致结论，同时避免把 Claim 的实际修改权从 holder 手中拿走。

本功能提供两阶段 Analysis、正式 Resolution、可恢复投递和 holder adoption observation，以降低团队知识治理成本：

- `manual`、`shadow`、`auto` 三种启用模式适配不同发布阶段。
- Proposal 与 Verification 分别判断裁决结论和可靠性。
- `unresolved` 始终保持 Dispute open，等待管理者处理。
- `auto` 在分析输入稳定且双阶段通过时自动形成 Resolution。
- 管理者可以手动 Analyze、Adopt approved Analysis，或直接 Human Resolve。
- Maintainer 只向 holder 发送结构化 Resolution；Claim 是否更新以及如何更新仍由 holder Agent 内化决定。

## 产品边界

1. 分析服务默认关闭；启用后默认 `shadow`。
2. `coexist`、`lifecycle_update`、`conflict_resolved` 均可形成正式 Resolution；`unresolved` 不能关闭 Dispute。
3. `human_review_reason` 是可选交接说明。缺失时，合法的 unresolved 仍是正常 unresolved。
4. Claim 的 `active`、`stale`、`deprecated` 建议由模型结合证据判断；Rust 只校验协议，不增加关键词风险门、单调状态门或固定 type/status 组合。
5. 原始 Dispute `summary` 保持不变；当前 Resolution 单独存储。
6. Maintainer 使用独立 `[maintainer.llm]`，不回退 Agent LLM。
7. 治理上下文包含全部处于 `active` 状态的真正 `policy_update` Policy，排除 `deprecated` Policy 与 `claim_attribute_update` 通知 Policy。
8. Maintainer 分析与 Agent 仲裁 inbox 都不读取 `MEMORY.md`、`USER.md`、session transcript、Trace 或工具上下文。
9. Router 补充上下文通过现有 `RouterClient` 获取。
10. Receipt ACK 表示 Agent 已安全持久化消息，不表示 Claim 已完成内化。
11. 新 Dispute 首次上报时，任何 direct Claim 已在团队 mirror 中标记为 deprecated 都会被拒绝；相同 ID、相同原始内容的既有 Dispute 网络重放仍保持幂等。

## 术语

- `direct_claims`：由 `Dispute.claims` 显式列出并唯一解析的全部 Claim。
- `source_claims`：从 direct Claim 的 Claim 来源开始，按 Claim ID 稳定 BFS 展开的来源图，受 `max_source_claims` 限制。
- `router_candidate_claims`：按 direct Claim scope 查询并合并去重的 Router 候选 Claim。
- `Current Analysis`：一个 Dispute 当前唯一的分析记录。它可以由 Dispute 上报触发，也可以由管理者点击 Analyze 创建；新的 Analyze 原子替换旧记录。
- `Analysis round`：Current Analysis 的一次 Proposal + Verification。初始分析为第 1 轮。
- `context_changed`：Analysis 完成后，采用前发现稳定分析输入已经发生实质变化。
- `Resolution`：关闭 Dispute 的当前正式治理记录，包含结论、依据、逐 Claim assessment、来源 Analysis 与稳定投递意图。
- `Adopt`：不再次调用模型，把仍适用于当前输入的 approved Analysis 转换为 Resolution。

## Resolution 协议

### Resolution Type

| 类型 | 含义 | 正式行为 |
| --- | --- | --- |
| `coexist` | 多条 Claim 当前仍有效，且明确分属不同 scope、版本、环境或路径 | 可关闭 |
| `lifecycle_update` | 同一能力或路径发生演进，当前基线已迁移，旧默认或旧路径退出 | 可关闭 |
| `conflict_resolved` | 可比较条件下不能同时成立，且证据足以选择方向 | 可关闭 |
| `unresolved` | 材料不足、存在核心分歧或缺少会改变结论的证据 | 保持 open |

### Resolution Basis

| 依据 | 含义 |
| --- | --- |
| `direct_analysis` | direct Claim 的逻辑、scope 与条件关系足以判断 |
| `prior_resolution` | 相同 direct Claim 集合的已有 Resolution 对当前输入适用 |
| `policy` | 团队治理 Policy 是决定性依据 |
| `evidence` | Claim evidence、source Claim 或 Router 补充知识是决定性依据 |
| `insufficient_evidence` | 不能形成正式结论，只能 unresolved |

只有 `coexist`、`lifecycle_update`、`conflict_resolved` 才要求每条 direct Claim 必须且只能有一条 assessment，包含 Claim ID、recommended status、判断、理由和可选 scope/statement 建议。每个 Proposal 的 `evidence_refs` 必须唯一覆盖全部 direct Claim，并可继续引用支撑结论的 Policy、source Claim、Router candidate Claim、真实 Router Dispute 或历史 Resolution；Policy 是主要依据或结果为 `unresolved` 时也不能省略 direct Claim。`unresolved` 不输出 Claim assessment，也不建议 status、scope 或 statement 修改；它只说明缺失材料并等待人类管理者处理。Proposal 与 Verification 的 confidence 都表示当前冻结上下文是否足以支持正式关闭 Dispute。任一阶段低于门槛、Verification 不同意核心字段或 assessment、或任一阶段选择 unresolved，都使 Analysis 进入 `unresolved`。

resolved assessment 遵循最小知识变更原则：`coexist` 默认保持当前有效 Claim，仅在边界含混时澄清 scope/statement；`lifecycle_update` 区分当前基线与被替代知识，同一知识单元可准确修正时优先建议原地更新；`conflict_resolved` 保持有依据的正确 Claim，错误 Claim可在同一主题内纠正时优先建议原地更新。只有 Claim 已无当前价值且正确知识已有明确承载对象时才建议 deprecated。Maintainer 不假设 holder 的完整本地知识，也不在 assessment 中要求创建新 Claim；是否已有本地替代、是否需要新增独立知识单元，由 holder Agent 内化时判断。Verification 独立检查这些建议是否与 type、证据和知识边界一致，但不把推荐策略变成固定 type/status 规则。

## 模式与 Analysis 生命周期

### `enabled=false`

模型分析入口不可用。管理者仍可 Human Resolve，Resolution 投递与 observation 事件仍可恢复；若已有 Analysis 在关闭前已经固定采用意图，启动恢复仍会复用该意图完成提交与投递，不需要 LLM。

### `manual`

Dispute 上报只持久化 open Dispute。管理者可以：

- 点击 Analyze 写入或替换 Current Analysis；
- Adopt approved Current Analysis；
- 忽略 Analysis，直接 Human Resolve。

### `shadow`

新 Dispute 创建 Current Analysis并执行双阶段分析。approved 结果只供审阅，可由管理者显式 Adopt。`unresolved` 与 `failed` 保持 open。

### `auto`

新 Dispute 创建 Current Analysis。双阶段通过、采用前输入稳定且 Maintainer 仍运行在 `auto` 时形成 automatic Resolution。管理者点击 Analyze 会替换 Current Analysis，新记录仍遵循 `auto` 的自动采用规则。`unresolved`、`failed`、低置信或 Verification 不通过时保持 open。

Analysis 由有界、单 consumer 的持久事件调度器处理。Analysis 先落盘，再进入队列；请求在两步之间取消时会唤醒持久恢复扫描。队列容量只限制内存工作集，持久状态是恢复依据。每个执行阶段使用 lease token fencing，完成的 Proposal 与 Verification 在恢复时复用。

Current Analysis 被新的 Analyze 覆盖，或其 Dispute 被 Resolution 关闭时，执行中的 provider 调用会在短周期状态检查后被丢弃，不继续占用唯一 consumer；持久化的上下文等待与重分析等待同时转为审计终态并清除调度时间。启动恢复也会在 Router 或模型调用前完成该检查。保存 Proposal、进入 Verification 和最终采用前都会再次确认 Analysis ID 与 open 状态。

## 冻结上下文与稳定输入判定

模型上下文按以下顺序构建：

Proposal 与 Verification 的 system prompt 都先注入项目统一的 Claim、Dispute、Policy 领域定义；两阶段使用同一领域语义，但各自独立判断。随后提供以下冻结数据上下文：

1. 原始目标 Dispute。
2. 唯一解析的全部 direct Claim。
3. 稳定 BFS 展开的 source Claim。
4. 全部处于 `active` 状态的治理 `policy_update` Policy。
5. 按 direct Claim scope 查询、合并并去重的 Router candidate Claim。
6. Router 返回的真实相关 Dispute 内容与 status。
7. direct Claim 集合相同的已有 Resolution。
8. UTC 分析时间与规范化 warning。

稳定语义投影用于判断采用时输入是否仍等价。它跟踪：

- 目标 Dispute；
- direct/source Claim 的稳定知识字段；
- 治理 Policy；
- Router candidate Claim 的实际内容；
- Router 返回的真实 Dispute 内容与 status；
- 已有 Resolution 的稳定结论；
- 影响分析语义的 warning code 与模型配置。

Router candidate 以 Claim 的稳定内容参与上下文与 fingerprint；其检索索引关联的 Dispute ID 列表属于派生检索元数据，不参与二者。`updated_at`、运行时间、lease、provider 错误和随机 ID 等运行噪声也不进入 semantic fingerprint。完整实际快照另由 context snapshot hash 标识。

direct mirror 暂未到齐时，Analysis 进入 `waiting_context` 并按有限预算重试上下文准备；该阶段不调用 LLM。Router 补充来源失败形成稳定 warning；完整 active 治理 Policy 超过 context window 时 Analysis 进入 `failed`。

## `auto` 模式的输入变化重分析

只有创建时模式为 `auto` 且 Maintainer 当前仍运行在 `auto` 的 Current Analysis，才会在采用检查中新增输入变化重分析计划。已经持久化为 `waiting_reanalysis` 的轮次按原 `next_retry_at` 恢复，但在非 `auto` 模式下完成后不会自动采用。切换到 `shadow` 或 `manual` 会暂停尚未固定 Resolution intent 的自动采用；切回 `auto` 后可从同一持久记录恢复。创建于其他模式的 approved Analysis 不会因后来切到 `auto` 而被自动采用。

从持久记录恢复时，原本要求管理者显式 Adopt 的 Analysis 继续保持该采用边界；当前运行模式不会把它改为自动采用。之后由管理者新建的 Current Analysis 按创建时模式运行。

1. 第 1 轮 Analysis approved 后，采用前第一次发现 `context_changed`：保留第 1 轮，进入 `waiting_reanalysis`，在 5 分钟后开始第 2 轮。
2. 第 2 轮 approved 后再次发现 `context_changed`：保留前两轮，在 15 分钟后开始第 3 轮。
3. 第 3 轮 approved 后仍发现 `context_changed`：停止自动处理，Dispute 保持 open，状态说明为“分析输入连续变化，已停止自动处理，等待人工”。

等待时间写入 `next_retry_at`，由持久调度器延迟队列管理；其他 Dispute 的 Analysis 可以继续执行。Maintainer 重启后按同一个时间点恢复等待。一个 Current Analysis 最多运行三轮，不创建额外 Analysis，也不重复创建 Resolution、Policy 或 inbox。

每轮审计保存：

- round number；
- started/completed time；
- 输入 fingerprint 与 context snapshot hash；
- Proposal 与 Verification；
- 导致下一轮的简短 `context_change_reason`。

`manual`、`shadow`、`enabled=false` 不使用自动输入变化重分析。`unresolved`、`failed`、低置信和 Verification 不通过也不会进入该流程。

## Analyze 与 Adopt

`POST /api/disputes/{id}/analyses` 为 open Dispute 创建 Current Analysis，并原子覆盖原记录。覆盖前尚未提交的旧任务会被 Analysis ID fencing，不能回写。Analyze 本身不修改 Dispute、Policy、outbox 或当前 Resolution；新记录是否自动采用由创建时模式决定。

管理 API：

- `POST /api/disputes/{id}/analyses`
- `GET /api/disputes/{id}/analyses`
- `GET /api/disputes/{id}/analyses/{analysis_id}`
- `POST /api/disputes/{id}/analyses/{analysis_id}/adopt`

`GET /analyses` 只返回可选的 `current_analysis`。被覆盖的记录不形成产品历史或 chain，也不能继续 Adopt。

Adopt 只接受双阶段通过、resolution type 非 unresolved、状态为 approved 的当前 Analysis：

1. 在 per-dispute 临界区确认 open Dispute、当前 Analysis 和当前 Resolution。
2. 锁外通过 RouterClient 构建当前上下文。
3. 重新进入 per-dispute 临界区，复核 Analysis ID、当前 Resolution 和稳定输入 fingerprint。
4. 输入一致时使用固定 Analysis 输出形成 Resolution 与稳定 delivery intent。
5. 输入变化或 Resolution 已由其他操作提交时返回 409，并刷新 Workbench 当前数据。

同一 Analysis 在 adopting/adopted 期间的重复或并发 Adopt 复用固定 Resolution ID 与投递 intent，不重新调用模型或生成新 ID。

## Resolution、并发与恢复

每个 Dispute 只保存当前 Resolution。Human Resolve、Adopt 或 Reject & Replace 写入当前 Resolution；不提供 Resolution chain 产品视图。

Resolution 在提交前固定以下内容：

- Dispute 与 direct Claim 快照；
- resolution type、basis、conclusion 与 assessments；
- 可选来源 Analysis ID 与 semantic/context hash；
- Policy、Maintainer action ID；
- 每个 holder 的 inbox ID、target 与完整消息快照。

固定锁顺序：

```text
per-dispute lock → outbox 进程锁 → outbox 文件锁
```

Resolution、Policy 和 outbox entry 都使用 create-or-verify 语义。相同 ID 和 immutable payload 幂等成功，不同 payload 报冲突。已经固定的人类 Resolution 优先于尚未提交的 Analysis。

## Agent Claim Attribute Update 内化

普通建议、自动 Resolution、人工 Resolve 与 Reject & Replace 统一为 Claim Attribute Update。连续 CAU 按 inbox 顺序组成一次结构化模型调用，输入：

```text
agent_id
claim_attribute_updates[] {
  claim_attribute_update
  conclusion
  resolution?
  dispute?
  direct_claims
}
local_claims
```

- `claim_attribute_updates` 保留每条完整 inbox 消息和 Policy，顺序与 inbox 一致。
- `conclusion` 始终存在；普通 CAU 取自 `policy.statement`，结构化 CAU 取自 Resolution conclusion。
- `resolution` 与 `dispute` 按消息是否包含结构化治理结果提供；Resolution 保留 type、basis、assessment 等信息。
- `local_claims` 是唯一可编辑集合：当前 Agent 全部非 deprecated 本地 Claim，加上由它持有的任意 status direct Claim。
- `direct_claims` 是可选 Dispute 的全部 direct Claim 快照，保留任意 status 与 holder；其他 holder 的快照只读。

该调用不读取 Memory、USER、session transcript 或工具上下文。模型可以保持不变、更新 `local_claims` 中的对象、创建新 Claim，或在发现新的实质冲突时报告新 Dispute。存在 Resolution 时，模型先判断当前 Claim 是否已经符合结论；已有等价、正确的本地 Claim 时不创建重复知识，同一知识单元优先原地更新，只有明确存在正确承载对象时才将错误 direct Claim deprecated。不能为了记录 Resolution 或提高可观察性而制造更新。

后端重新按 holder、status 与 direct Claim 集合构造编辑白名单，不信任模型对权限的理解：更新目标必须属于当前 Agent，且必须是非 deprecated 本地 Claim或当前 Dispute 的本地 direct Claim。Claim source 只允许输入中可见 Claim、它们已有的 Claim 来源和本批新 Claim；Policy source 只允许本批 CAU 及可见 Claim 中出现的 Policy；每个 new/updated Claim 必须至少引用一个真正影响它的本批 CAU Policy。Dispute 只允许引用实际可见 Claim和本批新 Claim。仅作为历史来源出现的 Claim不能升格为 Dispute 对象，其他 holder 的 direct Claim不能更新。

每批连续 CAU 在 `<agent_home>/inbox/effects/` 保存稳定联合 Effect Journal，并为批内各 inbox 消息保存可恢复引用：

- `Prepared` 保存有序消息身份与 hash、已校验 effect、preimage hash 与固定 Claim/Dispute/Trace ID。
- `Applied` 表示 Claim、Trace 与 durable Maintainer upload 已进入幂等恢复边界。
- 崩溃或部分 ACK 后优先重放同一 Prepared plan，不再次调用模型。
- 当前 Claim 等于 target 时视为已应用；等于 preimage 时应用；已被后续本地操作改变时记录 superseded warning，不覆盖新内容。

## Resolution 投递与 Holder Observation

Resolution 在提交 Dispute 前先写入 durable pending commit/delivery 记录；即使无需通知 holder，该记录也保存固定 Resolution intent。Maintainer 内部有界事件调度器立即接管任务；失败时按上限退避恢复，启动时只恢复这些持久任务。恢复过程以同一 ID 幂等补齐当前 Resolution、Dispute、可选 Policy/inbox 与治理历史事件；全部完成后才消费任务。

Observation 由以下事件定向刷新当前 Resolution：

- 相关 inbox receipt ACK；
- 相关 Claim mirror 上传；
- 当前 Resolution 切换；
- Dispute 详情按需读取。

索引把 inbox ID、direct Claim ID 和通知 Policy ID 映射到受影响的当前 Resolution；Policy 索引使 CAU 新建 Claim 的首次 mirror 上传也能定向刷新。被替换 Resolution 的 observation cache 保留，后续事件只更新当前 Resolution。

Observation 以 Resolution 中冻结的 direct Claim 快照为基线，逐 Claim 对比 holder mirror 的 status、scope 和 statement，并以通知 Policy provenance 归因 CAU 修改。具有对应 provenance、但不属于 direct 快照的 Claim 作为额外 Policy-linked Claim 纳入观察，其中包含本次真正新建的 Claim，也可能包含被 CAU 修改的既有非 direct Claim；仅凭 provenance 不伪造两者的区别。Maintainer 在首次收到携带对应 provenance 的 Claim 上传时，直接从该请求 create-once 冻结 adoption 快照；异步刷新和后续 mirror 版本不能改写它。assessment 只提供可选的建议元数据，不决定比较对象，因此不含 assessment 的人工 Resolution 仍能展示完整快照对比。receipt 独立表示是否送达。状态为：

- `not_delivered`
- `no_update_observed`
- `update_observed`
- `unknown`

updated、additional、unchanged 和 unavailable 以 Claim 为单位汇总，notified 与 delivered 以 holder 为单位汇总。Observation 不评价 holder 是否逐字段服从 assessment，只用于治理可见性，不触发 Analysis、Resolution、通知或 Claim 修改。

## Workbench

- Dispute 列表把 open 放在 resolved 前，组内按时间倒序。
- 详情展示原始 Dispute、可选 Current Analysis、当前 Resolution 与 holder adoption。
- open Dispute 提供 Analyze 与 Human Resolve；approved Analysis 提供 Adopt。
- `auto` 模式的 Current Analysis 等待重分析时展示轮次、5/15 分钟等待、下次重试时间与原因。
- 第三次输入变化后展示“分析输入连续变化，已停止自动处理，等待人工”。
- open Dispute 直接展示 Analysis，供管理者参考 unresolved 分析；resolved Dispute 优先展示当前 Resolution，并默认收起 Analysis 过程，按需展开审阅。
- Direct Claim、Analysis 与 Resolution 使用明确的视觉分区；Resolution assessment 标为治理建议，不与 Claim 当前状态混淆。
- Delivery & Holder Adoption 默认折叠；展开后按 holder 展示 Resolution 时的 Claim 快照与当前 mirror，重点呈现 Agent 的实际内化结果，不以是否逐字段遵循建议作为界面评价。
- 页面只展示一个 Current Analysis 与当前 Resolution。

## 配置

```toml
[maintainer.arbitration]
enabled = false
# manual | shadow | auto
mode = "shadow"
confidence_threshold = 0.90
max_source_claims = 20

[maintainer.llm]
# 独立 LlmChatConfig；enabled=true 时必填
retry_count = 2
```

Proposal 与 Verification 使用同一份 Maintainer LLM 配置，但执行两次独立调用和两套 prompt。Analysis 调度器使用单 consumer。

## 验收标准

- 三种启用模式与 `enabled=false` 的上报、Analyze、Adopt、Human Resolve 行为符合上述定义。
- 稳定输入判定对真实知识变化敏感，对 Router candidate lifecycle 派生元数据和运行噪声保持稳定。
- `auto` 模式的 Current Analysis 最多三轮；5/15 分钟等待持久、可恢复且不阻塞其他 job。
- 三轮 fingerprint、Proposal、Verification、时间与变化原因完整保留。
- Analyze 覆盖 Current Analysis，不产生 history/chain；Adopt 不重新调用模型。
- Resolution 与投递 ID 在并发、请求取消、进程重启和部分 outbox 写入后保持幂等。
- pending delivery 退避恢复；ACK、Claim upload 与 Resolution switch 只刷新相关当前 Resolution；旧 observation cache 保持冻结。
- 所有 CAU 使用统一的连续批量输入与联合 Effect Journal，不读取 Memory；当前 Agent 可修改全部非 deprecated 本地 Claim和自己持有的任意状态 direct Claim，其他 holder 快照只读，崩溃或部分 ACK 恢复不重复调用模型。
- Policy 内化已消除的冲突不生成 Dispute；Agent 在本地落盘和上传前按整批最终 Claim 状态校验，Maintainer 再对含 deprecated direct Claim 的新上报返回确定性冲突，Agent 单次发送后不自动重试。
- Workbench 在 Resolution 提交后立即刷新当前 Dispute、Analysis 与 Resolution 视图。
- Rust 与 Workbench 的格式、静态检查、测试、构建及独立 code review 通过。
