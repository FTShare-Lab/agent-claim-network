# Maintainer 自裁决说明

Maintainer 自裁决用于处理团队 Claim 之间的争议（Dispute）。它会整理相关证据，生成分析记录（Analysis）并复核；分析通过并被采用后，形成正式裁决（Resolution）。

Maintainer 向 Claim 的持有者发送处理建议，由 Agent 决定如何修改本地 Claim。本文介绍功能行为、使用方式和查看结果的入口，完整配置见[配置参数](config_parameters.md#maintainerarbitration)。

## 模式

启用时需设置 `[maintainer.arbitration].enabled = true`，独立配置 `[maintainer.llm]`，并设置 `api_key_env` 指定的密钥环境变量。Maintainer 不会沿用 Agent 的模型配置。修改配置后需重启 Maintainer。

`enabled` 控制分析和采用功能是否可用；启用后，`mode` 控制新 Dispute 的处理方式。

| 配置 | 新 Dispute 上报后 | 如何形成正式裁决 |
| --- | --- | --- |
| `enabled = false` | 只保存 Dispute，不调用模型 | 管理员人工处理 |
| `mode = "manual"` | 只保存 Dispute | 管理员发起分析并采用，或直接人工处理 |
| `mode = "shadow"` | 自动生成并复核分析 | 管理员确认后采用，或直接人工处理 |
| `mode = "auto"` | 自动生成并复核分析 | 分析通过、上下文稳定时自动采用 |

建议新团队先使用 `shadow`，确认分析质量和证据完整性后再决定是否切换到 `auto`。争议风险较高或需要逐项审批时，使用 `manual` 更合适。

在 `manual` 或 `shadow` 下创建的分析，切换到 `auto` 后仍需人工采用。切出 `auto` 后，尚未开始的自动采用会暂停；切回后可继续处理。

关闭自裁决（`enabled = false`）后，自动分析、`Analyze` 和 `Adopt` 均不可用，仍可通过 `Resolve Dispute` 人工处理争议。

## 裁决使用哪些信息

每轮分析会保存一份用于判断的材料，包括：

- 原始 Dispute 和其中直接引用的 Claim；
- 这些 Claim 的来源 Claim，数量受 `max_source_claims` 限制；
- 当前有效、类型为 `policy_update` 的团队 Policy；
- Router 返回的候选 Claim 和相关 Dispute；
- 直接 Claim 集合相同的既有正式裁决；
- 材料缺失或检索异常等提示。

Maintainer 不读取 Agent 的 `MEMORY.md`、`USER.md`、会话记录或工具执行记录。争议涉及的 Claim 尚未同步完整时，系统会短暂重试。若分析失败，可稍后重新点击 `Analyze`，也可直接人工处理争议。

## 自裁决流程

1. **接收并保存 Dispute**

   Maintainer 校验并保存原始 Dispute。`manual` 到此等待管理员操作；`shadow` 和 `auto` 自动开始分析。

2. **生成提案（Proposal）**

   第一阶段根据保存的材料提出结论，给出裁决类型、依据、每条 Claim 的建议和置信度。证据引用必须覆盖争议中的全部直接 Claim，且限于本轮材料。

3. **独立复核（Verification）**

   第二阶段使用相同材料，独立检查提案的类型、依据、结论和每条 Claim 建议。

4. **判定分析结果**

   提案和复核的置信度都达到 `confidence_threshold`，且复核同意结论及全部 Claim 建议，分析才通过（`approved`）。证据不足、置信度不足或复核不通过时，记为未解决（`unresolved`）。模型调用或输出校验失败时，会按 `[maintainer.llm].retry_count` 重试，耗尽后记为失败（`failed`）。

   `unresolved` 和 `failed` 都保留争议，不会自动重新分析。管理员可稍后重新 `Analyze`，或直接人工裁决。

5. **采用分析**

   `manual` 和 `shadow` 需要管理员点击 `Adopt`。所有采用操作都会检查材料是否变化。`auto` 遇到实质变化时会等待后重新分析，包含首次分析在内最多三轮；仍不稳定时交由管理员重新 `Analyze` 或人工裁决。

6. **提交、投递和观察**

   正式裁决提交后，Dispute 变为已解决（`resolved`）。采用分析后会向相关持有者发送 Claim 更新建议；直接人工裁决时可选择是否通知。页面会显示通知是否送达，以及 Agent 后续同步的 Claim 变化。重启后，未完成的裁决提交和通知会继续处理。

<p align="center">
  <img alt="Maintainer 自裁决演示：分析、裁决与 Claim 变化" src="assets/maintainer-auto-arbitration.gif" width="728">
</p>

演示依次展示分析、复核、正式裁决及 Claim 的后续变化。

## 裁决结果

分析可以给出四类判断：

| 类型 | 含义 | 何时关闭 Dispute |
| --- | --- | --- |
| `coexist` | Claim 适用的范围或条件不同，可以并存 | 分析通过并采用后 |
| `lifecycle_update` | 当前情况已改变，旧知识需要更新或停用 | 分析通过并采用后 |
| `conflict_resolved` | Claim 在相同条件下冲突，证据足以作出判断 | 分析通过并采用后 |
| `unresolved` | 缺少关键证据，或两阶段分析存在分歧 | 保持未解决 |

裁决依据分别记录为直接 Claim 分析（`direct_analysis`）、既有裁决（`prior_resolution`）、团队 Policy（`policy`）或其他证据（`evidence`）；证据不足（`insufficient_evidence`）只会得到 `unresolved`，不会形成正式裁决。

前三类提案必须覆盖每条直接 Claim，给出建议状态、判断和理由，也可建议调整适用范围或内容。`unresolved` 提案不提供修改建议。分析通过本身不会关闭争议，还需要采用并提交正式裁决。

## 管理员操作

启动 Maintainer 后，打开 `http://<maintainer-listen>/app`，进入 `Disputes`。

- `Analyze`：分析尚未解决的争议。重新点击会替换当前分析，不保留被替换的历史版本。
- `Adopt`：采用当前已通过的分析，无需模型再次生成结论。材料变化时会阻止采用，请根据页面提示等待自动重分析或重新 `Analyze`。
- `Resolve Dispute`：直接填写结论并提交人工裁决。默认通知持有者；取消勾选 `Notify Affected Agents` 时不通知对应持有者。
- `Reject & Replace`：替换自动裁决，需填写驳回原因和替代结论，并通知持有者。

持有者收到裁决后，可以自行决定保持现状、修改已有 Claim 或创建新 Claim。

## 审计方法

Workbench 中可以按以下顺序检查一条 Dispute：

| 检查内容 | 页面位置 | 重点 |
| --- | --- | --- |
| 原始争议 | `Direct Claims`、`Summary` | 直接 Claim 是否完整，范围和证据是否可比较 |
| 两阶段分析 | `Current Analysis` | 提案、独立复核、置信度、证据引用、警告和分析轮次 |
| 正式裁决 | `Current Resolution` | 自动或人工、裁决类型、依据、结论和逐 Claim 建议 |
| 投递与后续变化 | `Delivery & Holder Adoption` | 是否送达，裁决快照、首次内化快照与当前 Claim 镜像的差异 |
| 管理操作 | `HTTP Audits` | Analyze、Adopt、人工处理和驳回替换的请求时间、来源地址与结果 |

`Delivery & Holder Adoption` 中的“已送达”表示 Agent 已保存消息，后续才会处理建议。查看 Claim 变化时，应区分裁决时的内容、Agent 首次处理后的结果和当前内容；后续修改也可能来自其他任务。

<details>
<summary>排障时查看本地记录</summary>

需要离线核对或排障时，可检查 `<acn_home>/data/team/maintainer/`：

- `disputes/`：原始 Dispute 和当前状态；
- `arbitrations/<dispute-id>/analysis.yaml`：Current Analysis、冻结上下文、两阶段输出和轮次；
- `arbitrations/<dispute-id>/resolution.yaml`：当前正式裁决及投递意图；
- `arbitrations/<dispute-id>/observations/`：持有者投递与 Claim 变化观察；
- `outbox/`：发给持有者的投递台账；
- `history/dispute_resolution_events/current.jsonl`：裁决事件；
- `history/resolution_observation_events/current.jsonl`：观察事件；
- `history/http_audit_logs/current.jsonl`：HTTP 管理操作记录。

审计时可用 `dispute_id`、`analysis_id`、`resolution_id`、`policy_id` 和 `inbox_id` 串联各页面与文件。持久文件用于审计和恢复，不应在 Maintainer 运行时手工修改。

</details>
