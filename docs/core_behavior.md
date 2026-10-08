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
- Maintainer 可以通过人工 Resolution，或在显式启用 `auto` 后通过双阶段 Analysis 形成 Resolution，将其改为 `resolved`

冲突不会阻止当前任务继续。Agent 应把矛盾暴露给用户或在上下文中作出有依据的选择，并在确有必要时报告 dispute。

已经解决的 dispute 仍保留为历史事实。Router 查询候选 claim 时同时返回相关 dispute，使借用方看见已知争议。

Maintainer 可选使用独立 LLM 生成 Analysis。Analysis 是可审阅的建议，只有分析被采用或完成人工裁决后，才形成正式 Resolution 并解决 Dispute。新 Resolution 保留原始 `summary`，类型包括 `coexist`、`lifecycle_update` 和 `conflict_resolved`；无法可靠判断时，Analysis 记为 `unresolved`，Dispute 继续保持 open。

Resolution 中的 Claim 调整属于治理建议。Maintainer 不要求创建新 Claim，也不直接修改 holder 的本地知识；holder Agent 根据自己的完整上下文决定保持、原地修正、创建或废弃哪些 Claim。

自裁决的模式、分析与采用流程，以及投递后的 Claim 变化观察，见 [Maintainer 自裁决说明](maintainer_auto_arbitration.md)。

Dispute 属于团队治理流：只有配置团队服务时，Agent 才把 finalize 或 inbox 内化形成的 dispute 报告给 Maintainer。单人模式不创建待日后补传的 dispute 队列。

新 Dispute 只能引用仍未废弃的 direct Claim；如果同一轮内化已经消除冲突，就不再上报对应 Dispute。相同 ID、相同原始内容的网络重放按幂等处理；相同 ID 对应不同内容时拒绝覆盖既有记录。

## Inbox

Inbox 是 Maintainer 到 Agent 的下行通道。当前支持 `PolicyUpdate` 和 `ClaimAttributeUpdate`，两类消息都内嵌完整 Policy。

普通建议与 Resolution 使用同一种 ClaimAttributeUpdate 内化流程。Agent 可以更新自己的本地 Claim；该过程不读取 Memory、USER、session transcript 或工具上下文。Agent 可以接受、调整或不采用建议，也可以在确有必要时形成新的 Claim 或 Dispute。

消息的本地生命周期是 pending、claimed、handled：

- pending 可以被处理器领取
- claimed 使用 lease 防止同一 Agent 内重复处理
- 成功后写 `handled_at`
- 失败时释放 lease，后续可以重试

远端 receipt ACK 只表示 Agent 已经把消息持久化到本地，不表示 LLM 内化已经成功。这个区分保证网络重投与本地业务重试互不混淆。

Inbox 失败按副作用边界降级：远端请求与 ACK 失败显示 warning；LLM/provider/解析/校验在 prepared 结果产生前失败时不应用该 batch，保留 pending 并提示 `/inbox` 重试；本地持久化、effect 应用或 done ACK 失败显示具体 error，并明确可能已有部分本地副作用。

## Router 查询

Agent 在这些情形应考虑查询 Router：

- 本地没有足够信息，需要发现团队已有判断
- 用户问题属于 scope overview 展示的团队知识范围
- 需要核对已有 claim 是否冲突或过时
- 即使本地已有较高置信度，任务仍明确要求团队视角或冲突检查

Router 返回完整候选 claim，而不是服务端文件路径。Agent 的引用与使用必须来自已展示完整正文的 claim；目录摘要中的 ID 仅用于发现，不能仅据摘要计为使用或来源。模型凭空生成的 ID 会被校验拒绝。

查询无结果不是错误，Agent 可以继续使用本地知识和工具完成任务。

## 压缩后的文件工作集

SessionEngine 在模型摘要之外保留成功 `file_read/file_write/file_patch` 的文件工作集，从已压缩的 canonical 消息和 active 消息确定性重算。失败或尚未返回的工具调用不计入，shell 操作不做路径猜测；修改过的路径不再重复列为只读路径。工作集有数量、路径长度及总字符边界，超量明确报告 omitted，并计入 provider 上下文预算。

这些路径记录历史操作，不表示当前文件仍存在、内容正确或已取得修改许可。模型仍需遵守现有文件读取与编辑授权；工作集不新增持久化文件，恢复时复用 session 与 turn journal。

## Session 的 provider 私有 replay

Session 的 canonical content 只保存用户可见文本、附件与工具语义。`openai_responses` 和 `anthropic` 还可以在 assistant message 上保存 provider 私有 replay，用于满足同一 wire protocol 的多轮连续性要求。Replay 绑定生成它的精确配置 model；协议或 model 变化会开始新代际，切回时不复活早先代际。

Provider 私有 replay 不属于用户可见 transcript，不进入 TUI、session search、Memory、recap、Claim、Router 或 Maintainer；compaction summary 也不消费它。只有未 compact、身份匹配且属于当前连续代际的 replay 会进入下一次 provider 请求和对应 token 预算。失败、取消或结构不完整的 turn 不提交 replay。

`openai_chat` 当前没有 provider 私有 Reasoning replay；厂商扩展的 Reasoning 字段会被丢弃。

请求发送结果不明确时，Agent 保留 Provider WAL 供恢复；收到并接受完整响应（包括内部续写前的 `max_tokens` 中间响应）后，该次发送的歧义结束。上下文窗口拒绝只取决于请求内容，不受该歧义影响，仍进入压缩重试。后续确定性拒绝按自身结果回滚：本轮尚无已确认响应时丢弃失败 turn，已有已确认进度时保留此前进度。拒绝回滚的 sidecar 记录只在崩溃窗口内有效，对应 turn 写下终态后下一 turn 只删除记录，不再重放回滚。HTTP、流式和非流式错误分类都利用已知外层错误类别，未知细粒度 code 不会遮蔽它，也不会仅因未知 code 就清除 WAL。

`anthropic` 在 `max_tokens` 打断最后一个 `tool_use` 时，把该调用视为参数无法解析的调用：工具循环返回错误 tool_result 让模型重试，结构化输出调用按可重试的形状错误处理，不把残缺参数当作完整结果。

内部续写前，已接受的响应独立形成恢复快照，不包含下一次续写触发消息；非流式 fallback 的已接受正文同时进入 journal，即使续写被拒绝或 Provider WAL 丢失也能恢复。手动 `/compact` 在生成新摘要前先完成残留的拒绝恢复，后续输入不会再用旧拒绝快照覆盖新摘要。

HTTP 413 或明确的 WebSocket 1009 尺寸错误可触发媒体清理；其他请求只有明确指向图片 / PDF 的错误才进入媒体恢复。上下文窗口错误和内容策略拒绝优先走各自路径，非法 tool schema 等普通请求错误不会剥离媒体。清理后的紧邻重试再次被确定性拒绝时，已提交历史的媒体占位符和恢复边界继续有效，失败 turn 的输入和动态上下文被丢弃。正常 resume、Provider WAL 缺失或 replay identity 改变时都会遵循该边界；仅当前失败 turn 带媒体时不保留无意义的清理边界。

## Recap、Finalize 与知识形成

Compact 的 summary 只负责 provider context，并独立推进 compaction frontier。对于已提交且尚未 recap 的 canonical messages，compact 会异步投递 Supervisor Recap job；Recap 从最新 `recapped_until` 处理到冻结 target，不影响前台 summary 的成功结果。Session Finalize 则从最新 cursor 覆盖到最终 `message_count`，并处理 Finalize 专属的后台进程终态。

Recap/Finalize 对目标消息段做结构化复盘：

- 识别本次使用的来源
- 形成新 claim 或更新已有本地 claim
- 在团队模式下识别需要报告的 dispute
- 为有知识输入或产出的任务写 trace

Recap/Finalize 只处理可验证的 session 内容，不把 system prompt、Memory 更新本身或未出现的 Router claim 当作证据。两者共用 session 级锁与 `finalize_checkpoint.yaml`；Recap 只推进 cursor，Finalize 负责关闭 session。Inbox 与 Recap/Finalize 的本地知识应用还通过 agent 级锁串行，团队上传在释放知识锁后进行。

### Claim 修改的分析依据与恢复

正常 Inbox、Recap/Finalize 继续在知识锁内读取、分析和提交。持久方案保留分析时的本地 Claim 快照、已分配的 ID 和逐项执行进度；执行前核对整个关联方案的内容，不只比较时间戳。

- 当前等于分析版本时按原方案执行；等于目标版本时识别为已经完成，不重复写入。
- 中断恢复时发现 Claim 已变化，或旧检查点缺少分析快照无法确认一致性，放弃该批尚未执行的 Claim 修改及 Dispute，保留已经生效的结果。整批放弃，避免承接更新未执行却继续弃用来源。
- 放弃原因写入原执行记录并作为 warning 返回；原批次按现有流程结束、推进 recap cursor，不再重放。恢复不使用专用提示词、不追加模型调用，也不重新分配对象 ID。持久化等实际操作失败仍走原重试流程。
- 旧 Maintainer 裁决到达本地后仍作为建议：若尚未进行 Inbox 分析，模型直接结合当前本地 Claim 自主内化，不因裁决快照较旧而拒绝。

未完成的 Dream 本地提交在会话入口优先恢复。恢复失败时知识读取和 Inbox 暂缓；会话仍可使用空 Claim 基线启动，后续由 runtime_context 补齐。恢复不等待远端服务。上述保护针对 ACN 管理的持锁修改路径，不能替代对模型语义判断的评估。

## Dream 与会话内 Claim 变化

Dream 在已有 Claim 上执行质量清理、证据校准和主题整合，只修改当前 Agent 自己的非 deprecated Claim，不处理 dispute。缺少评审反馈表示未知；Trace 自述和历史 Dream 报告不能单独作为提高置信度的依据。整合必须承接有效信息，先写存活结果，再废弃被覆盖项。修改计划经宿主校验后使用知识应用锁和独立 checkpoint 提交，遇到并发版本变化跳过关联组。

B 只从修改前 confidence 为 medium / low 的 Claim 中按需选择具体疑点，不要求逐条验证；high 仍参与 A / C，但不做 B 的主动查证或修订。取得明确适用证据且能确定修订方向才修改；证据缺失、冲突或仍不确定时保持全部字段原样，不因此降置信度、标 stale / deprecated 或刷新依据；Dream 不建立问题裁决状态或待办清单。每项 B 修改都必须提供新的 evidence_summary，并在该 Claim 的 change_basis.evidence_ids 中引用本轮版本化 file_read 直接证据，说明观察结果如何支持这项修改；仅有组级引用或“未找到证据”不够。宿主拒绝 evidence 组修改原 high Claim，不能先降低置信度绕过门槛；免查不代表已验证为真。

B 的探索门槛与修改门槛分开：有具体疑点和合理查证入口时，提示模型主动获取证据，不要求初始上下文已有直接证据或确切文件路径。入口可来自组件、scope、来源 Trace 或结果记录；明显依赖不可获取的历史/外部材料时可以跳过。探索后仍不确定就不修改，没有最低调用或修改数量。 不同组件或证据需求按独立疑点处理；共享证据和判断条件时才成组。提示词要求保留理由对应具体 Claim，并区分未探索、探索后不确定和确认无需修改；组级 reason 协议保持不变，宿主不能证明理由充分。

Dream 结束前由同一模型检查实际 ABC 覆盖：A 对全部已装入正文使用一致的记忆层级标准；B 对选中的疑点判断是否仍有值得利用的具体线索，必要时从概述或 Trace 继续到对应组件、版本和条件下的实现、契约或结果；C 可以选择分开保留。完成一组或读取一份文件不代表整轮完成。模型可在没有有价值的入口、无法获得适用材料或继续查证无助于判断时停止，并简述代表性理由和未覆盖范围。上述为提示词引导，不新增后端覆盖率门槛、固定流程或调用配额；安全优先，不能为显得完整而强行修改。

B 仅确认原判断、补证据或调整置信度时，默认只修改 confidence / evidence_summary，正文及其他字段保持原样。新证据明确指出原判断或范围需修订时，才改动相应片段，说明必要性并保留其余有效前提、步骤、例外和恢复路径。执行前自复核须对照真实差异，不能声明“仅补证据”却顺手改写正文。这是 prompt 与自复核约束，后端结构校验不证明修订必要性或语义保留。

Claim 的 name 可以随正文一起修改，id 不变。Dream 在改写、纠正或整合正文时，若旧名称已不准确，在同一 update 中同步修改 changes.name；仍准确则保留。自复核逐条填写 `final_name` 与 `name_reason`，检查未改的旧名称是否与新正文一致；后端要求名称与当前草稿一致、说明非空，语义准确性仍由模型判断。历史执行记录可继续恢复。

证据复核逐项核对新增/纠正判断与实际提交的版本化来源，不能以一个有效回执代替完整支持关系。正文纠错与置信度提升独立判断，默认保留原 confidence；提升时在现有复核文本中说明完整声明、scope 与关键条件的依据。纠正局部错误不会自动使整条 Claim 达到 high。缺证据的拟新增内容应补证据或撤回，不能只降低置信度后写入；后端仍只检查结构与来源边界，语义充分性由同一模型复核。

Dream 使用同一 supervisor，优先级低于 Finalize、Recap。自动门槛、手动 `/dream`、后台生命周期和命令隔离要求见[使用说明](user_guide.md#dream后台整理-claim)。它不读取 USER.md 或私有 Memory，也不代替 Memory Review。

已识别的修改或具体查证疑点立即登记到本轮草稿，复用 `group_id`。`dream_record_candidates` 按模型选择的顺序登记，第一项为当前候选；已有完整方案时直接暂存也会自动登记。当前项先探索、校验并执行，或通过 `dream_keep_candidate` 说明具体保留理由，再处理下一项。有效知识、证据不足、信息保留不确定、整合无收益或外部版本变化均可支持保留；“整合无收益”仅适用于 C；其他任务、优先级或参数错误不能作为放弃理由。初始上下文明示自有权限及已装入 Claim 的 B 资格；候选被原始 high 门槛拒绝时列出具体 ID，整批不登记，模型修正后可重新提交。后端在登记项尚未处理时拒绝结束，即使已有其他修改成功。撤销草稿不清除候选，候选状态随审计、压缩和重试恢复。这只约束已经登记的本轮工作，不增加 Claim 裁决状态，也不保证模型发现全部问题。

Dream 按独立修改组执行：`dream_stage_group` 暂存、`dream_validate` 返回真实前后对照和版本；模型完成简要语义复核后调用 `dream_apply_group`，成功即生效，然后继续探索。`dream_finish` 只结束本轮并汇总已经执行的结果，不统一写入。纯 A 废弃可逐条提供 `id/action/reason`，省略 `changes` 与空数组，后端补齐规范化 `change_basis`，仅改变状态；其他修改仍需完整依据，B 的新证据和 C 的信息去向约束不变。复核逐 Claim 说明保留的规则、条件、例外、备选行动和证据限制、删除或纠正的依据、存活去向和证据，不再强制重复抄写所有字段。硬规则检查权限、证据归属、版本、信息去向和置信度边界；语义是否充分仍由模型判断。没有独立复核模型，不确定时保留原文。

存在输入 Claim，但尚无候选登记、探索回执、修改提案或已执行组时，首次 `dream_finish` 返回一次事实反馈，提醒 A/C 不受 B 的 high 门槛限制，并要求简述代表候选的保留理由。模型可继续探索，也可确认后零修改结束；没有最低调用或修改数量。提醒随草稿持久化，恢复同一草稿后不重复。完成审计记录实际写入条目数和当前上下文的工具读取/运行记录数；记录数不等于验证通过数，零记录也可能表示工具没有成功返回。

执行工具先把 before/after、证据、语义复核和硬校验结果保存到 `<agent_home>/dream/<job_id>/executions/`，再复用 Prepared 提交；成功后记录实际写入和同步暂存结果。记录失败则不开始写入。中断后从检查点恢复；已经生效的组不因后续 API 错误撤销。同一 validation_id 重试返回已有执行回执。组内先写整合结果再 deprecated 来源，知识读取和运行时通知避开未恢复的中间状态。整轮失败可能已有部分修改，应查看逐组记录。

执行冲突通过工具回执返回分析快照、当前 Claim 和实际已执行结果。被拒绝的候选保留原草稿并使旧校验失效，同一 Dream Agent 调整后重新校验，或明确保持原样；正常调用、回执重放和中断恢复均不把拒绝当成完成。

C 类整合在持久上传队列记录承接依赖。远端确认承接 Claim 的具体版本后，才上传依赖它的来源 deprecated 结果；失败时保留队列，无关上传继续。后续普通修改不自动证明知识仍被承接，也不回传旧版本覆盖新内容；明确的后续整合关系可继续形成承接链。来源恢复为有效 Claim 时取消旧弃用请求。明确 Policy 撤销会在暂存弃用结果时一并取消这些 Claim 的旧整合依赖，避免等待已经失效的承接知识；普通 deprecated 更新不能解除此保护。

后续 Dream 可读取 `pending_consolidation_sync`，通过 `dream_review_sync` 获取版本绑定的原来源与当前承接内容，并提交与 `dream_apply_group` 相同格式的简洁语义复核，明确原来源的条件、例外和来源如何被当前承接项完整保留。复核绑定 `validation_id`，版本变化后须重新读取判断，后端复用 C 整合的来源继承检查。此工具仅更新交付依赖，不能修改或重新激活 Claim；所有来源信息必须保留，不确定时继续暂缓。复核先保存到 `<agent_home>/dream/<job_id>/sync_reviews/`，交付仍需远端确认。单人模式不创建这些队列，也不发起团队请求。

正文改写必须同步核对 name、scope、confidence、条件、例外和替代路径。B 的所有事实纠正都需要该 Claim 的新直接文件证据；先经 A/C 降低 high 再进行 B 仍拒绝。C 保留各输入有效信息和来源，不能把较弱判断无证据合并为 high。简短复核不是语义正确性的证明，也不引入裁决状态或仲裁待办。

Dream 长探索的请求输入估算超过 160k token 时自动压缩；较小模型窗口按输出预留提前触发。完整旧工具历史保存在本次 Dream 审计目录，可由 Dream 分页查询。压缩摘要不是新的证据，准确草稿、已执行操作、最新 Claim 和证据版本由后端保留；初始快照及历史 Dream 输出不成为新的独立证据。

主会话的 system prompt 仍然冻结。每个 turn 观察本地 Claim 变化，通过 `<runtime_context>` 提供 ID、name、scope 和变化种类。模型先根据 name、scope 判断相关性，只有当前任务需要引用或依赖该 Claim 时才使用 `claim` 的 `action="read"` 读取最新正文，不因收到通知就逐条读取所有变化；deprecated / removed 直接停止使用，无须为此读取正文。此能力覆盖 Inbox、Recap、Finalize、Dream 等全部变更来源，各 session 使用自己的初始快照及持久化通知，不依赖 Dream 开关。

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
