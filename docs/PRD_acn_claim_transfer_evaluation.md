# PRD: ACN Claim 协作量化评测

本文定义 Agent Claim Network（ACN）在**团队知识传递**场景下的量化评测。它回答的不是“哪一个
agent 的绝对能力最高”，而是：一名成员完成工程任务后留下的 claim，是否能让一名没有该任务
上下文和私有 memory 的新成员，以不降低交付质量的方式更少地完成后续工作。

本 PRD 采用可复现实验而非主观体验作为证据。首期优先评估用户输入 turn；provider token 只在
计费口径相同的配对实验中作为辅助指标。任务耗时会记录为诊断信息，不作为主要结论，因为它会受到
上游 LLM provider 负载、网络和人工操作等待的影响。

---

## 目标与非目标

### 目标

1. 在同一 ACN 版本、同一模型和同一任务快照下，量化 claim 对新 agent（consumer）的完成质量、
   用户输入 turn 和 token 消耗的影响。
2. 使每次实验具备固定的任务输入、可执行的验收、可审计的运行记录和可复算的统计口径。
3. 提供 Codex、Claude Code 等非共享上下文产品的 fresh-session 参照，说明其“成员 A 已完成任务”
   不会自动给成员 B 带来知识传递；不把不同产品的绝对 token 数直接当作能力排名。
4. 选择有真实工程摩擦、但可在授权环境中验收的任务，优先覆盖动态页面采集、调试修复和小型工具
   开发等场景。

### 本期不做

- 不以单个成功案例、单一模型或单次运行宣称 ACN 整体优于任意产品。
- 不比较不同 system prompt、工具集、模型版本或 provider 计费口径下的绝对 token 值。
- 不把 wall-clock 时间、人工扫码等待或 provider 排队时间解释为 agent 效率。
- 不绕过验证码、登录保护、robots 规则、平台访问限制或其他反自动化机制；涉及登录的任务只能使用
  获授权的测试账号、测试环境或由评测者完成的正常人工登录。
- 不把 producer A 的源码、git 历史、终端记录或 private memory 暴露给 consumer B；B 能获得的
  额外信息只能是实验指定的 claim 集合。

---

## 核心假设与实验对象

### 核心假设

若 A 在任务中形成了准确、具体、可检索的 claim，则 B 在全新身份和全新工作区中完成相同任务时，
应减少探索和试错；因此在不降低验收通过率的前提下，B 的用户输入 turn 中位数预计更低。

这是一条**可证伪**假设：claim 没有被检索、内容错误、任务本身没有可复用的坑，或 claim 的注入
成本高于节省的探索成本时，实验应如实呈现零收益或负收益。

### 角色与术语

| 术语 | 定义 |
| --- | --- |
| producer（A） | 首次执行任务、形成并上报 claim 的 ACN agent。 |
| consumer（B） | 使用新的 agent id 执行任务的 ACN agent；没有 A 的 session、私有 memory、工作区或 git 历史。 |
| `B_empty` | consumer 的空知识对照：与实验组使用相同 ACN 版本、模型和团队协议，但运行于独立的空 team store。 |
| `B_claim` | consumer 的 claim 实验组：只能经 router 获得本次 A 产生且通过资格检查的 claim。 |
| fresh-session 参照 | Codex 或 Claude Code 在新窗口、新工作区中执行同一任务。A 的工作不通过 memory、上下文、文件或人工提示传给该窗口。 |
| task card | 一份不可变的任务卡，包含任务 prompt、初始文件快照、验收命令、允许的人工操作和预算。 |
| 配对运行 | 对同一 task card、同一 seed、同一模型配置完成的 `B_empty` 与 `B_claim`。 |

---

## 公平性与隔离规则

### 固定项

每个配对运行必须固定并写入 run record：

- ACN git revision、配置 hash、provider、模型名和模型版本（若 provider 可提供）；
- task card 版本、初始 workspace 的 commit 或压缩包 SHA-256、依赖锁文件；
- 完整的首条用户 prompt、允许工具与网络策略；
- 用户输入预算、token 预算、验收命令和 timeout；
- 运行端的 OS、终端、关键依赖版本，以及执行日期和时区。

`B_empty` 与 `B_claim` 使用不同的 agent id、session id、`acn_home` 和工作副本，均从相同的初始
workspace 快照创建。两组使用相同的 team-service 配置；前者的 router 数据源必须为空，后者只导入
本次 A 的合格 claim。这样隔离的是“可获得的团队知识”，而不是把 ACN 的其他运行成本一并移除。

### 禁止信息泄漏

- 每一轮 B 都从 pristine workspace 启动；不得看见 A 的 commit、未提交改动、日志、trace 原文或
  本地运行目录。
- 关闭或隔离 ACN 的 agent 私有 memory；Codex/Claude Code 的参照实验也关闭其可关闭的 memory /
  project instruction 持久化能力。若某产品无法关闭，必须在报告中标注，且不能和已关闭的一组做
  因果比较。
- 评测者只能发送 task card 预先允许的用户消息。不得根据前一组的解法给另一组追加提示。
- 若外部页面、接口或依赖版本会变化，主评测使用固定 fixture、测试站或镜像；真实站点运行只能作为
  单列的探索性 case study。

### 人工参与

人工发送给 agent 的每条自然语言消息都计入用户输入 turn。扫码登录、点击授权、输入已批准测试账号
等非文本操作单独记录为 `human_operations`，包含操作类型和是否为任务先决条件；它们不计入 turn，
也不纳入时长比较。不同组的人工操作要求不一致时，该对运行不得进入效率指标，只能报告可用性差异。

---

## 实验分组

### 主实验：ACN 的 claim transfer

每个 task card / seed 按下列顺序运行：

1. **A：生产知识。** A 在干净环境完成任务；记录其所有用户输入、LLM usage、验收结果以及产出的
   claim / trace。
2. **claim 资格检查。** 从 A 的 claim 中选出与该 task card scope 匹配的候选，执行本节的资格
   规则。选择过程和最终 payload 固化为 run artifact，不允许为 B 人工改写内容。
3. **`B_empty`：空知识对照。** 新 agent id 在空 team store 中从同一初始快照执行任务。
4. **`B_claim`：团队知识实验组。** 另一新 agent id 从同一初始快照执行任务，只能读取步骤 2 固化
   的 claim。记录 router 返回、实际使用的 claim id 和最终 trace。

`B_empty` 与 `B_claim` 的先后顺序按预先生成的随机表交替，避免评测者熟悉任务后无意改变操作。
A 必须先完成，因为 B 的实验组依赖其 claim；A 的结果独立报告，不替代 B 的对照。

若 A 未生成合格 claim，必须记录 `no_eligible_claim`。该运行仍保留在样本清单中，不能通过重跑 A
直到生成“好看”的 claim 来替换；它可用于分析 claim 生成率，但不构成 `B_empty` / `B_claim` 的
有效转移配对。

### 外部 fresh-session 参照

对每个 task card 另用 Codex 与 Claude Code（条件允许时）执行 fresh session：新窗口、新 workspace、
相同 task prompt 与验收。它们不接收 A 的 claim，也不接收评测者的解法提示。

该组仅回答“没有共享团队知识时，成员 A 的既往工作不会自动改变成员 B 的起点”。对外报告时：

- ACN 的主要证据是同产品的 `B_empty` vs `B_claim` 配对差异；
- Codex / Claude Code 只展示各自的通过率、turn 分布和环境说明；
- 不横向比较不同产品的绝对 token 或把不同模型的结果合并成同一排名；
- 若需要比较“claim 文本本身”的价值，可另立实验：将**同一冻结 claim**作为用户显式 handoff 给各
  产品。该实验评估 handoff 文本，不得表述为 ACN 产品能力比较。

---

## claim 资格与可观测性

进入 `B_claim` 的 claim 必须同时满足：

1. 由 A 的真实任务运行产生，且 scope 能匹配当前 task card；
2. 包含可操作的判断、条件或验证线索，不能只是“任务已完成”一类状态消息；
3. 没有 open dispute，且不包含密钥、cookie、个人数据、A 的完整工作区路径或不应转交的凭据；
4. payload、claim id、创建时间和选取理由均写入 artifact；
5. B 运行后能从 router 记录 claim 是否被返回，并从 trace / finalize 记录其是否被实际使用。

资格检查只审查安全性、任务相关性和可审计性，不得按“是否能提升 B 的指标”筛选。候选 claim 有多个
时，采用预先写入 task card 的规则（例如同 scope、最高 confidence、最多 3 条）确定集合，并保留
排序结果。

claim 被 router 返回不等于被 B 采纳；报告必须分别展示 `retrieved`、`used_in_trace` 和 `accepted`。
若 B 未使用 claim，仍按原始实验组保留，不事后把该运行移出分析。

---

## 任务池

### 入选标准

每张 task card 必须：

- 要求至少两个有验收的工程步骤，且存在需要探索、调试或选择实现路径的真实摩擦；
- 可在干净 workspace 中独立完成，结果可由脚本、fixture 或明确的人工 rubric 验收；
- 不需要评测者在中途告诉 agent 隐含答案；
- 在合规、授权的网络与账号条件下可执行；
- 预计 claim 能表达“已验证的限制、失败原因、有效路径或验收要点”，而不是仅复述题目。

首批任务建议覆盖三类以上情形：

| 类别 | 建议任务 | 自动验收 | claim 可能复用的知识 |
| --- | --- | --- | --- |
| 动态数据采集 | 面向自建或获授权测试站，完成带动态渲染、分页和去重的数据采集工具。 | fixture 数据集、记录数、去重和重试测试。 | 渲染等待条件、有效采集路径、常见失败及验证方式。 |
| 认证后采集（探索性） | 在 OAuth sandbox 或获授权测试账号下，完成登录后页面的读取；人工完成正常登录。 | session 有效、最小字段集、无凭据落盘。 | 登录状态边界、受限页面的合法访问路径。 |
| 代码修复 | 在给定项目中定位并修复一个带已知回归测试的 bug。 | 指定测试、全量相关测试和静态检查。 | 根因、受影响模块、最小修复和回归命令。 |
| 小型工具开发 | 实现本地图像浏览 / 元数据筛选等小程序，并处理一个预置的边界问题。 | 黑盒测试、启动检查和 fixture 输出。 | 依赖选择、边界输入和验收命令。 |

直接爬取生产站点、扫码登录或带反自动化限制的网站不作为主量化样本；它们容易因站点变化和账号状态
破坏可重复性。确有业务授权时，可作为单独 case study，明确说明目标、授权范围和人工操作。

### task card 最小字段

```yaml
id: dynamic-collector-v1
revision: 1
prompt_file: prompt.md
workspace_sha256: "..."
setup: "./scripts/benchmark_setup.sh"
acceptance:
  - "cargo test -p collector"
  - "./scripts/assert_fixture_output.sh"
allowed_human_operations: []
network_policy: fixture_only
budgets:
  max_user_input_turns: 12
  max_provider_total_tokens: 120000
  max_wall_clock_minutes: 45
claim_selection:
  max_claims: 3
  scope: "benchmark / dynamic-collector-v1"
```

预算是停止和故障归类的边界，必须在首轮运行前冻结；示例值不是所有任务的默认值。达到任何预算而未
通过验收时，标记为 `not_accepted_within_budget`，不得把超预算的后续尝试拼接到同一次运行中。

---

## 指标与计算口径

### 质量门槛

效率指标只在交付质量达标后解释。每次运行先执行 task card 的自动验收；必要的人工 rubric 至少由
两名评测者盲审，分歧按预先定义的规则裁决。每轮记录：

- `accepted`：所有必需验收通过；
- `failure_kind`：例如编译失败、功能缺失、超 turn 预算、超 token 预算、环境失败；
- `human_operations`：人工先决操作及其结果；
- 产物 hash 与验收日志路径。

### 主指标：用户输入 turn

`user_input_turns` 是从 task card 的首条任务 prompt 起，到最终验收前，评测者向该 agent 发送的
自然语言消息数量；首条 prompt 计为 1。仅回复确认、补充需求、提供登录后状态或纠正错误也各计 1。
agent 的内部 tool loop、assistant message、网络重试和 provider request 不计为用户 turn。

对同时通过验收的一对运行，计算：

```text
turn_saving_ratio = (turns(B_empty) - turns(B_claim)) / turns(B_empty)
```

正值表示 claim 组需要更少用户输入。若一侧未通过，不能用任意填充值伪造节省比例；将它体现在
通过率与失败类型中，并保留原始日志。

报告使用每个 task card 的配对中位数、IQR、样本数和单对胜/平/负（`B_claim` turn 更少 / 相同 /
更多）。均值可作为补充，不能代替稳健统计量。

### 辅助指标：provider token

每次 provider response 记录可获得的 `input_tokens`、`output_tokens`、`cache_read_input_tokens`、
`cache_creation_input_tokens` 和 provider 的总计字段。报告同时给出原始字段和定义明确的：

```text
provider_total_tokens = input_tokens + output_tokens
token_saving_ratio = (tokens(B_empty) - tokens(B_claim)) / tokens(B_empty)
```

只有 `B_empty` / `B_claim` 使用相同 provider、模型、usage 口径和 cache 策略时，才计算
`token_saving_ratio`。claim 注入会增加 consumer 的初始输入 token，因此 token 结果可以为负，即使
turn 减少；这是应报告的成本，而非应隐藏的异常。当前 ACN 已在 provider / tracing 路径暴露 usage
信息，但本期需要由 benchmark recorder 汇总每次 response，不能把 TUI 的单次 context 使用量当作
整场总消耗。

### claim 机制指标

为避免只看到“结果变好”而不知道是否由 claim 带来，额外记录：

- `eligible_claim_count`、注入的 claim id / 字符数 / token 估算；
- `router_retrieved_claim_ids`；
- `trace_used_claim_ids` 与 `used_in_trace`；
- A 的 claim 生成率（有合格 claim 的 A 运行占比）；
- B 的最终产物是否复现了 claim 中声称的约束或验证步骤。

### 不作为主要指标的时间

记录 setup、人工等待、首 token、验收和总 wall-clock 时长，用来诊断环境问题；不用于比较产品或作为
“更快”的核心结论。若以后需要时间指标，应在稳定、隔离的 provider 和硬件环境中另立实验。

---

## 运行记录与产物

建议每次运行保存为不可覆盖目录：

```text
benchmarks/runs/<task-id>/<seed>/<condition>/<run-id>/
  manifest.yaml            # 固定配置、版本、hash、预算与环境
  prompts.jsonl            # 按序记录用户输入；敏感字段脱敏
  provider_usage.jsonl     # 每次 response 的原始 usage 与归一化字段
  claims.yaml              # B_claim 可见的冻结 claim payload
  router.jsonl             # claim 检索结果
  outcome.yaml             # turn、预算、验收和失败分类
  acceptance.log
  artifact_hashes.txt
```

`manifest.yaml` 至少应包括 `task_id`、task revision、condition、agent id、ACN revision、model、
provider、config hash、workspace hash、随机顺序编号和启动时间。日志不得提交 API key、cookie、完整
认证 header、私有页面内容或用户个人数据；必要的敏感内容以 hash 或脱敏摘要替代。

任务、配置和 recorder 发生任何影响输入或验收的改动时，必须增加 revision，不得与旧 revision 混算。

---

## 分析与结论规则

### 分阶段样本量

1. **可行性 pilot**：至少 3 张 task card，每张至少 3 组有效配对。目标是验证隔离、记录器和验收，
   不发布产品优越性结论。
2. **方向性评测**：至少 4 张 task card、每张至少 5 组有效配对；报告每类任务及总体的中位数和 IQR。
3. **正式结论**：任务池与样本量在运行前冻结；建议至少 8 张任务卡、每张 10 组有效配对，并使用
   配对 bootstrap 置信区间。成本不足时应明确标为探索性结果。

### 对外表述门槛

只有同时满足下列条件，才可表述“在该评测范围内，ACN claim transfer 降低了后续成员的协作成本”：

1. `B_claim` 的验收通过率不低于 `B_empty`，且没有新增严重质量或安全问题；
2. 预先冻结的主分析中，`turn_saving_ratio` 的中位数为正，且结果覆盖多个任务类别；
3. claim 的 retrieval / trace 数据表明消费方确实获得并使用了指定团队知识；
4. 报告完整披露任务、模型、样本量、失败运行、token 口径和限制。

如果只在少量案例中观察到收益，应表述为“在这些任务和配置下观察到正向 transfer signal”，不能外推为
所有工程任务、所有模型或所有团队均有提升。token 指标与 turn 指标方向不一致时，必须并列展示，不得
只挑选有利的一项。

---

## 实施阶段与验收

### 阶段 0：冻结评测协议

- 基于本文选定首批 task card、模型 / provider、预算、memory 关闭方式和外部参照范围。
- 为每张任务卡写出 setup、pristine snapshot、验收命令、合法网络范围和人工操作脚本。
- 评审安全边界，特别是登录、采集和敏感日志处理。

验收：每张 task card 能由独立评测者在不询问隐含业务规则的情况下启动；所有固定项已写入 manifest。

### 阶段 1：记录器与一条端到端 dry run

- 实现或接入 run recorder，至少输出本文定义的 manifest、用户输入、usage、claim/router/trace 与
  acceptance artifact。
- 以一个本地 fixture 任务完整跑通 A、`B_empty`、`B_claim`，人工检查无 workspace 或 memory 泄漏。

验收：三组运行均可复现；B 的工作区和 `acn_home` 相互独立；能够从日志复算 turn、验收和 token。

### 阶段 2：pilot 与协议修订

- 运行可行性 pilot；不因结果方向修改 task prompt、验收或分析公式。
- 只允许修复记录器 bug、环境 bug 或任务卡中明确的不可执行问题；所有修订必须提高 revision。

验收：输出包含全部运行（含失败）的 pilot 报告，并明确哪些任务适合进入方向性评测。

### 阶段 3：方向性评测与报告

- 按冻结协议完成样本，生成配对汇总、任务维度明细、fresh-session 参照和限制说明。
- 逐条对照本文“对外表述门槛”，由至少一名未参与运行的评审者复核数据与结论。

验收：报告中的每个汇总值均可追溯至 run artifact；没有将跨产品 token、失败运行或人工等待混入主结论。

---

## 待确认决策

以下事项会影响首轮成本或外部可比性，必须在阶段 0 写入 manifest 后再开始正式评测：

1. 首批任务卡的具体来源、授权范围及是否包含认证后采集 case study；
2. ACN、Codex、Claude Code 分别使用的模型、版本和关闭 memory 的具体方式；
3. 方向性评测采用的每任务配对数与总 token / 预算上限；
4. 是否为 benchmark recorder 单独实现结构化导出，还是先以现有 session、tracing 和 trace 文件组装；
5. 外部 fresh-session 参照是否作为首轮必做项，还是在 ACN 主实验 protocol 稳定后加入。

在这些决定未冻结前，可以运行 dry run，但不得把结果发布为产品比较结论。
