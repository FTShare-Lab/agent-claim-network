# ACN 核心行为与数据边界

本文定义 ACN 中 Claim、Policy、Trace、Dispute、Inbox 的稳定语义，并说明各角色可以做什么。

## 通用标识与时间

业务实体使用带类型前缀的随机 ID，例如 `claim_`、`policy_`、`trace_`、`dispute_`、`inbox_` 与 `session_`。反序列化会校验前缀，避免把不同实体的 ID 混用。

持久时间统一使用 UTC。`created_at` 表示首次创建时间；只有实体发生后续语义或状态变化时才写 `updated_at` 或 `resolved_at`。

## Claim

Claim 是某个 Agent 愿意作为 holder 维护、可被团队检索和引用的稳定判断。

核心字段：

- `id`、`name`
- `statement`：可复用的具体判断
- `scope`：适用系统、环境或问题域
- `holder`：对该 claim 负责的 Agent
- `confidence`：`high`、`medium` 或 `low`
- `status`：`active`、`stale` 或 `deprecated`
- `created_at`、可选 `updated_at`
- `source_claim_ids`：形成该 claim 时使用的 Claim 或 Policy
- `evidence_summary`：足以解释判断依据的摘要

规则：

- Agent 只能创建 holder 为自己的 claim。
- 借用外部 claim 不等于复制。没有形成自己的稳定判断时，不创建本地 claim。
- confidence 来自 holder；查询和借用方不在原 claim 上追加“接收方置信度”。
- `USER.md` 内容永不进入 claim。私有 Memory 也不能作为可反查的条目身份上传。
- stale 表示需要复核，不等于自动失效；deprecated 表示 holder 已不再推荐使用。

## Policy

Policy 是 Maintainer 发布的行动约束或 claim 属性更新建议，不是客观事实，也不自动覆盖 Agent 的私有判断。

`message_type` 支持：

- `policy_update`：发布或废弃团队行动约束
- `claim_attribute_update`：建议 holder 更新 claim 属性，例如 stale/deprecated

Policy 可通过 `target_agents` 定向投递；未指定时为广播。首次发布只写 `created_at`，状态变化时保留原创建时间并写 `updated_at`。

Agent 在 inbox 内化 policy 后可以形成 claim、更新自己的 claim、报告 dispute，或不做知识变更。Maintainer 不越权直接写 Agent 本地存储。

## Trace

Trace 记录一次任务使用了哪些来源、产出了哪些本地 claim：

- `task` 与 `agent`
- `input_claims`：Claim 或 Policy 类型的 `SourceId`
- `output_claims`：当前 Agent 产出的 Claim
- `created_at`

Trace 不区分“借用”与“内化”状态，也不替代 session transcript。它用于解释知识产出链路，而不是把每个工具调用或推理步骤永久化。

Trace 保存在 holder Agent 本地，不上传 Maintainer，也不进入 Router 派生视图。

## Dispute

Dispute 表示多个 claim 之间可能存在冲突、不兼容或适用范围不清：

- 至少引用一组 direct Claim；`Dispute.claims` 只记录这组直接冲突对象
- 记录 reporter Agent 和自然语言 summary
- Agent 上报时只能是 `open`，且不得预填 `resolved_at`
- Maintainer 可以通过人工 Resolution，或在显式启用 `auto` 后通过双阶段 Analysis 形成 Resolution，将其改为 `resolved`

冲突不会阻止当前任务继续。Agent 应把矛盾暴露给用户或在上下文中作出有依据的选择，并在确有必要时报告 dispute。

已经解决的 dispute 仍保留为历史事实。Router 查询候选 claim 时同时返回相关 dispute，使借用方看见已知争议。

新 Resolution 不改写 Dispute 的原始 `summary`。自动模型可选择 `coexist`、`lifecycle_update`、`conflict_resolved` 或 `unresolved`；前三种在双阶段高置信且一致时可关闭 Dispute，`unresolved` 本身表示等待人类管理者处理并始终保持 open，不输出 Claim assessment 或 status、scope、statement 修改建议，可选交接说明缺失也不会把它变成技术失败。`lifecycle_update` 用于团队当前基线已迁移、旧默认失效或旧路径被替代的演进；只有新旧路径当前仍受支持且 scope 明确不同时才使用 `coexist`。Reject & Replace 通过 `expected_resolution_id` 替换当前 automatic Resolution。

Proposal 与独立 Verification 都使用仲裁专用 system prompt，并包含项目统一的 Claim、Dispute、Policy 定义，确保两阶段按同一领域语义解释输入对象；实际团队 Policy 仍只取上下文中的 `policy_update` 记录。Proposal 的 `evidence_refs` 唯一覆盖全部 direct Claim，也可引用上下文中其他决定性对象，使 resolved 与 unresolved 结论都能追溯到争议主体和依据。

resolved assessment 描述 direct Claim 的最小目标变更：`coexist` 通常保持当前有效 Claim，只在边界含混时澄清 scope/statement；`lifecycle_update` 与 `conflict_resolved` 在同一知识单元可以准确修正时优先建议原地更新，只有 Claim 已无当前价值且正确知识已有明确承载对象时才建议 deprecated。Maintainer 不假设 holder 的完整本地知识，也不要求创建新 Claim；Verification 独立检查建议是否有证据且符合 Resolution Type。

`shadow`/`auto` 下，新 Dispute create-once 写入 Current Analysis并进入有界串行调度器；`manual` 只保存 Dispute。显式 Analyze 原子替换 Current Analysis，也可直接 Human Resolve。direct Claim 暂未准备好时，同一 Analysis 先等待上下文，最终仍不完整才标为 failed。

被新的 Analyze 覆盖或已由 Resolution 关闭的 Analysis 会停止当前模型等待。Resolution 的固定提交意图先持久化，再幂等补齐 Dispute、投递与治理历史；进程重启沿用原 ID 恢复。

稳定语义投影对 direct/source Claim、治理 Policy、目标 Dispute、Router candidate Claim 内容及真实 Router Dispute 内容/status 的变化敏感，并忽略 Router candidate 的派生 lifecycle ID 列表。创建于 `auto` 且当前配置仍为 `auto` 的 Current Analysis 在采用前发现输入变化时，会新增 5 分钟和 15 分钟的重分析计划；第三次仍变化就停止自动处理、保持 open。已持久化的等待轮次按原时间恢复。切换到其他模式会暂停尚未固定 intent 的自动采用；创建于 `shadow` 或 `manual` 的 Analysis 不会因后来切到 `auto` 而自动采用。每轮 Proposal、Verification、fingerprint、时间和变化原因都保留在同一 Analysis。unresolved、failed、低置信和 Verification 不通过不使用该自动重分析流程。

Dispute 属于团队治理流：只有配置团队服务时，Agent 才把 finalize 或 inbox 内化形成的 dispute 报告给 Maintainer。单人模式不创建待日后补传的 dispute 队列。

Maintainer 对相同 ID、相同原始内容的 Dispute 重放做幂等接收；相同 ID 对应不同原始内容时保留团队中已有记录并返回冲突。Agent 会显示一次 warning，并将该冲突从待上传队列移除，不再自动重试。

## Inbox

Inbox 是 Maintainer 到 Agent 的下行通道。当前支持 `PolicyUpdate` 和 `ClaimAttributeUpdate`，两类消息都内嵌完整 Policy。

带 `arbitration_resolution` 的 ClaimAttributeUpdate 会单条内化，并只发起一次专用结构化模型调用；配置内的纠错重试共享同一个总预算。输入包含完整仲裁消息、当前 Agent 全部非 deprecated 本地 Claim、由当前 Agent 持有的任意 status direct Claim，以及原 Dispute 全部 direct Claim 快照；不读取 Memory、USER、session transcript 或工具上下文。当前 holder 可以修改自己实际持有的 direct Claim，不受其当前 status 限制；其他 holder 的 direct Claim 只读。Agent 优先保持已符合 Resolution 的 Claim；已有等价正确本地 Claim 时避免重复创建，并可将错误 direct Claim deprecated；没有等价本地替代且仍属同一知识单元时优先原地更新 direct Claim；只有独立、可复用的新知识单元才创建新 Claim。后端校验输出协议、holder、更新目标与基本引用格式。每条消息在 `<agent_home>/inbox/effects/` 保存已校验 Effect Journal，崩溃后直接重放 plan，不再次调用模型；若本地 Claim 已被后续操作修改则记录 superseded warning，不覆盖新内容。仲裁造成的 Claim 更新在 pending 上传中保留 durable 标记，鉴权恢复后继续补传。

Maintainer 以 receipt ACK、通知 Policy provenance、mirror 更新时间和 assessment 字段派生 `not_delivered`、`delivered_unobserved`、`observed_converged`、`observed_diverged` 或 `unknown`。ACK、相关 Claim 上传与 Resolution 切换定向刷新当前 Resolution；详情读取可按需刷新。旧 Resolution 的 cache 保留且不再更新。Observation 只用于治理可见性。

Workbench 将 Observation 展示为 Resolution 时的 Claim 快照与当前 holder mirror 的前后对照。底层状态仍用于判断送达、是否观察到更新和数据是否可用；界面不把 Agent 是否逐字段采用 Resolution 建议作为评分。

消息的本地生命周期是 pending、claimed、handled：

- pending 可以被处理器领取
- claimed 使用 lease 防止同一 Agent 内重复处理
- 成功后写 `handled_at`
- 失败时释放 lease，后续可以重试

远端 receipt ACK 只表示 Agent 已经把消息持久化到本地，不表示 LLM 内化已经成功。这个区分保证网络重投与本地业务重试互不混淆。

## Router 查询

Agent 在这些情形应考虑查询 Router：

- 本地没有足够信息，需要发现团队已有判断
- 用户问题属于 scope overview 展示的团队知识范围
- 需要核对已有 claim 是否冲突或过时
- 即使本地已有较高置信度，任务仍明确要求团队视角或冲突检查

Router 返回完整候选 claim，而不是服务端文件路径。Agent 只能引用本次上下文中已经出现的候选 ID；模型凭空生成的 ID 会被校验拒绝。

查询无结果不是错误，Agent 可以继续使用本地知识和工具完成任务。

## Finalize 与知识形成

Session finalize 对尚未 recap 的消息段做结构化复盘：

- 识别本次使用的来源
- 形成新 claim 或更新已有本地 claim
- 在团队模式下识别需要报告的 dispute
- 为有知识输入或产出的任务写 trace

Finalize 只处理可验证的 session 内容，不把 system prompt、Memory 更新本身或未出现的 Router claim 当作证据。Checkpoint 使中断后的重试不会重复提交已完成段。

## Stale Sweep

Maintainer 根据团队 mirror 中 claim 的最近语义更新时间判断 stale 候选。Sweep 只产生 `claim_attribute_update` 建议：

- 不依据 trace 引用频率自动裁决
- 不直接改写 Agent 本地 claim
- 不自动归档或删除历史文件

最终是否更新由 holder Agent 在 inbox 内化时决定。

## 必须保持的边界

- Memory / USER 与团队 claim 网络分离
- Agent 本地权威数据与团队 mirror 分离
- Router 派生视图与权威 claim 分离
- 远端投递 ACK 与本地内化完成分离
- Policy 行动约束与事实 claim 分离
- Trace 产出关系与完整 session transcript 分离
- 团队服务失败与本地 Agent 可用性分离
