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

普通 session 的启动上下文只包含有界本地 claim 目录。主 Agent 通过 `claim` 工具搜索目录、读取正文，再按当前任务的 scope、证据与时间判断是否采用。目录在 session 内冻结，工具读取的是最新本地内容；打开条目不等于验证或采纳它。

主 Agent 与用户的 `/claim` 面板可以修订 holder 为当前 Agent 的已有 claim，包括名称、判断、范围、证据摘要、置信度和状态；不能修改 id、holder、创建时间或来源链，也不能直接新建或删除。修订要求读取时的完整内容 revision，在 `knowledge_apply.lock` 内重新校验，冲突时保留较新的版本并要求重新读取。团队同步沿用既有 Maintainer 上传队列；单人模式不发团队请求、不累积待补传队列。

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

`claim` 工具与 `/claim` 面板可按 claim ID 回查关联 trace，再分页展开任务正文。历史 trace 没有对应的 claim 版本快照，不能证明后来修订的 statement 已获验证；人工或工具编辑本身不额外生成任务 trace，也不按引用次数自动改变 confidence。

## Dispute

Dispute 表示多个 claim 之间可能存在冲突、不兼容或适用范围不清：

- 至少引用相关 claim 集合
- 记录 reporter Agent 和自然语言 summary
- Agent 上报时只能是 `open`，且不得预填 `resolved_at`
- Maintainer 可以将其改为 `resolved` 并写入解决说明与时间

冲突不会阻止当前任务继续。Agent 应把矛盾暴露给用户或在上下文中作出有依据的选择，并在确有必要时报告 dispute。

已经解决的 dispute 仍保留为历史事实。Router 查询候选 claim 时同时返回相关 dispute，使借用方看见已知争议。

Dispute 属于团队治理流：只有配置团队服务时，Agent 才把 finalize 或 inbox 内化形成的 dispute 报告给 Maintainer。单人模式不创建待日后补传的 dispute 队列。

## Inbox

Inbox 是 Maintainer 到 Agent 的下行通道。当前支持 `PolicyUpdate` 和 `ClaimAttributeUpdate`，两类消息都内嵌完整 Policy。

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

## Session 的 provider 私有 replay

Session 的 canonical content 只保存用户可见文本、附件与工具语义。`openai_responses` 和 `anthropic` 还可以在 assistant message 上保存 provider 私有 replay，用于满足同一 wire protocol 的多轮连续性要求。Replay 绑定生成它的精确配置 model；协议或 model 变化会开始新代际，切回时不复活早先代际。

Provider 私有 replay 不属于用户可见 transcript，不进入 TUI、session search、Memory、recap、Claim、Router 或 Maintainer；compaction summary 也不消费它。只有未 compact、身份匹配且属于当前连续代际的 replay 会进入下一次 provider 请求和对应 token 预算。失败、取消或结构不完整的 turn 不提交 replay。

`openai_chat` 当前没有 provider 私有 Reasoning replay；厂商扩展的 Reasoning 字段会被丢弃。

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
