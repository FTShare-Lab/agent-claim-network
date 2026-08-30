# PRD: ACN DeepSWE 评测打样

> 状态：已实现 Pre-smoke 评测基础设施；Smoke 与 Full 的实际执行按冻结配置单独启动并保留 provenance。

## 结论

首期**不复跑 Claude Code、Codex、Cursor CLI、OpenCode**。这些产品已有公开 DeepSWE 数据，
直接摘取作为外部参照。

我们只跑 ACN，回答两个问题：

1. ACN 在无 claim 状态下，DeepSWE 得分、token、成本和 agent step 大致处于什么位置；
2. 同一模型、同一批任务下，第二个全新 agent 通过 router 获得 claim 后，是否优于无 claim 的全新 agent。

重点是第二个问题。ACN 若接近公开产品是额外收益，不是本期成败标准。

## 1. 数据集

使用 **DeepSWE v1.1**：113 个真实软件工程任务，覆盖 91 个仓库、5 种语言。
每题提供仓库快照和需求，最终由 program verifier 判断 patch 是否通过。

选择 DeepSWE 的原因：

- 任务就是我们原本想设计的工程场景，不再重复自造数据集；
- 每题都有明确判卷，结果比主观体验更容易对齐；
- 官方和 Artificial Analysis 已有模型、harness、成本和 step 数据。

当前官方对齐组从 DeepSWE revision
`435ee89ec2f2e2289f33b0da4f992f0b7b7266b9` 与 Pier revision
`0daf53d3599e58c4506cf0bcff5e12c77dc282d2` 新建冻结 manifest。任务清单、verifier、
`[[verifier.collect]]` 钩子和容器镜像 digest 随该 manifest 一起冻结，避免不同版本混算。仓库内
旧 revision 的 manifest 仅为历史 run artifact，不得混入此对齐组。

## 2. 公开数据直接复用

以下数据截至 2026-07-23，均直接引用，不安排复跑：

| 来源 | Harness / 模型 | DeepSWE |
| --- | --- | ---: |
| Artificial Analysis | OpenCode / Opus 4.7 medium | 40% |
| Artificial Analysis | Cursor CLI / Opus 4.7 medium | 32% |
| Artificial Analysis | Claude Code / Opus 4.7 medium | 27% |
| Artificial Analysis | Codex / GPT-5.6 Sol max | 69% |
| Artificial Analysis | Codex / GPT-5.6 Terra max | 67% |
| Artificial Analysis | Codex / GPT-5.6 Luna max | 63% |
| DeepSWE 官方，统一 mini-swe-agent | GLM-5.2 max | 44%±2% |
| Artificial Analysis | Claude Code / GLM-5.2 | 29% |

这些数字只用于建立坐标系，不作为严格控制变量实验。Artificial Analysis 页面中的默认组合经常同时
更换 harness 和模型；其 Coding Agent Index 当前为 v1.3，Index 成本也是多个 benchmark 的综合口径。

因此首期图表可以回答“ACN 大概在哪里”，不能据此断言分差全部来自 harness。

## 3. 基准配置

### 3.1 DeepSWE 官方运行条件

以下配置来自当前冻结的 DeepSWE 113 个 `task.toml` 及 Pier 的
`mini-swe-agent` adapter。正式评测需重新冻结实际使用的 revision，不得只记录“DeepSWE v1.1”。

| 项目 | 官方配置 | 含义 |
| --- | --- | --- |
| agent 网络 | `network_mode = "no-network"` | 禁止访问 GitHub、搜索引擎、包仓库等普通公网 |
| verifier 网络 | `network_mode = "no-network"` | 判卷过程也不联网 |
| 网络例外 | Pier 的隔离代理仅转发到宿主模型 broker | task 容器不持有长期模型密钥 |
| agent 工具 | 仅 Bash | 通过 Bash 读写文件、检索代码和运行测试 |
| MCP | `mcp_servers = []` | 不加载 MCP server |
| 跨任务状态 | 无 | 每题使用隔离环境，不继承上一题的 workspace、session 或 memory |
| 题内上下文 | 线性保留 | 本题之前的模型消息和命令结果会继续进入上下文 |
| 命令确认 | `--yolo` | 命令直接执行，不等待人工确认 |
| 交互收尾 | `--exit-immediately` | agent 结束时不等待人工输入 |
| step / cost limit | `0` | adapter 不按步数或累计费用提前停止，由 wall timeout 控制 |
| agent timeout | 5400 秒 | 单题最多运行 90 分钟 |
| verifier timeout | 1800 秒 | 判卷最多运行 30 分钟 |
| verifier 环境 | `environment_mode = "separate"` | 只提取 patch，在全新容器中应用并判卷 |
| 资源 | 2 CPU / 8 GiB / 20 GiB / 0 GPU | agent 与 verifier 使用固定资源 |

`mini-swe-agent` 默认只向模型暴露 Bash；命令结果超过 10,000 字符时只保留前 5,000 和后
5,000 字符。提示词中即使允许“安装缺失工具”，任务期普通公网仍被拦截，因此只能使用镜像内已有
依赖。

DeepSWE 的 `network_mode` 与 Pier 的 `allow_internet` 表达不同。runner 必须在运行前做
fail-closed 转换：仅当 agent / verifier 两者均为 `"no-network"` 时，生成 Pier 兼容副本，并显式写入
`environment.allow_internet = false` 与 `verifier.environment.allow_internet = false`。
原始和转换后配置的 SHA-256 都写入 manifest；转换必须保留 `[[verifier.collect]]`，未经转换的任务
禁止运行。

### 3.2 ACN 对齐配置

ACN 不要求伪装成 `mini-swe-agent`，其原生文件、命令和 router 工具属于待测 harness；但环境边界
必须对齐，且 A、`B_empty`、`B_claim`、`B_forced_claim` 除明确的 claim 交付方式外完全相同。

| 项目 | ACN 评测配置 |
| --- | --- |
| 普通公网 | 关闭；`web_search`、`web_fetch`、`web_request` 不注册，Shell 出网由 sandbox 硬拦截 |
| 网络白名单 | task 仅能经 Pier 隔离代理访问宿主模型 broker；router 使用进程内冻结 bundle，maintainer 关闭 |
| 依赖 | 预装进镜像；运行中禁止从 GitHub、PyPI、npm 等下载 |
| MCP | 关闭，`.mcp.json` 为空 |
| Memory | 关闭注入、读取、写入和后台 memory review；每个 attempt 使用全新 `acn_home` |
| Session | 不 resume；每题、每组使用新的 agent id、session id 和 session 目录 |
| Workspace | 从同一 base commit / image digest 创建 pristine 副本；组间不共享 git 对象外的运行产物 |
| Claim | 按 3.3 的组别矩阵配置；只要 A 在 freeze barrier 前产生 claim，就同时运行两个干净 B 臂；B 不能看到 A 的 patch、session、trace、日志或私有文件 |
| Skill | 四臂注入完全相同的 `coding-benchmark` skill，记录全文 SHA-256 |
| Prompt | 相同 system prompt、首条任务 prompt 和注入顺序；每个 runtime 的 `ACN.md` 只包含相同的 Claim 使用引导：有相关 scope 时查询 Router，候选 Claim 仅是历史经验线索，须以当前证据独立验证；禁止中途人工补充提示 |
| 工具 | 四臂工具 schema、权限、并发上限和输出截断相同；除 router 返回内容外不得因组别变化 |
| 人工交互 / 终止 | 关闭 `ask_user` 和 TUI user shell；完成后应调用无参数 `submit_task`；只有正常、可消费的 assistant 最终回复可在遗漏该调用时作为隐式完成 |
| Subagent | 首期关闭，避免引入额外模型实例和未计量的共享状态 |
| 超时 / 资源 | 官方对齐组：agent 5400 秒、verifier 1800 秒，ACN 工作 deadline 5280 秒（收尾预留 120 秒）；均为 2 CPU / 8 GiB / 20 GiB / 0 GPU |
| step / cost limit | 不设置早于所属 agent timeout 的停止线；若实现要求正数上限，设为不会在 Smoke 中触发的固定值 |
| verifier | 与 agent 分离；只应用最终 patch，在 pristine 容器中离线判卷 |

网络必须由 runner/sandbox 强制执行，不能只在 prompt 中要求模型“不要联网”。模型 broker
记录每条请求；白名单外请求写入审计日志并拒绝。

`submit_task` 只在 evaluation profile 注册，且必须是一个 assistant 响应中的唯一工具调用。其成功执行后
turn loop 立即结束，不再把 tool result 回灌给模型或发起下一次 provider request；之后才开始 session
finalize 与 verifier。它是首选的明确终止信号，但不是 claim 收尾的硬门槛：`run_turn` 正常结束而未提交时，
评测记录 `evaluation_completion.mode=implicit_assistant_done`，再进入相同的 finalize 与 verifier 路径；显式
提交记录 `mode=explicit_submit_task`，并保留 `evaluation_submitted` 事件。provider 截断、无可消费输出、
中断、请求错误和 deadline 都不能走隐式完成，仍记为 agent failure。该设计保留与官方 mini-swe-agent
sentinel 相近的明确边界，同时适配部分模型仅以最终文本报告完成的行为，且不把 ACN 伪装为 Bash-only
baseline。

任何扩展预算仅用于后续诊断，不与官方榜单或本 PRD 的官方对齐组混算。`agent_seconds` 同时驱动
Pier 的 `override_timeout_sec`、ACN 单次请求 timeout 与 attempt 自有 deadline；后者必须预留至少
120 秒给 session finalize、事件账本与 result 写入。配置、hash 与 timeout 均写入每次运行的
provenance。

宿主在 attempt 运行期间只读轮询 session `turn_events.jsonl`，把最后活动时间、最近事件和
疑似停滞状态写入 attempt 的 `progress.json`。该监控不提前停止 agent：模型长推理、上游排队或
工具运行都必须继续到原有 deadline。运行被人为中止时，manifest 和 progress 记录
`INTERRUPTED_BY_OPERATOR`；缺少最终 result 不能单独作为“模型无响应”或 claim 逻辑失败的证据。

### 3.3 Claim 可见性矩阵

| 状态 | A：producer | `B_empty` | `B_claim` | `B_forced_claim` |
| --- | --- | --- | --- |
| 历史 Memory / Session | 空 | 空 | 空 | 空 |
| 初始本地 Claim | 空 | 空 | 空 | 空 |
| Router | 进程内空 bundle | 进程内空 bundle | 进程内只读 bundle，仅含配置选定的 A 或 B_empty 本题 freeze barrier 前 claim | 同 `B_claim` |
| 首轮任务上下文 | 无 claim | 无 claim | 无 claim（模型自主调用 `consult_router`） | 同一冻结 router 查询所得完整 claim；明确标为需独立验证的前序信息 |
| 另一个 producer 的 workspace / patch / log / trace | 不可见 | 不可见 | 不可见 | 不可见 |
| 运行中团队数据变化 | 不读取历史 claim | 禁止 | 禁止；开始前生成只读快照 | 禁止；开始前生成只读快照 |

A 与 B_empty 从 pristine workspace 并行完成后，宿主分别写入不可变 freeze barrier；claim 资格检查只采信
各自 barrier 前的宿主事件账本，不采信 claim 文件自报的时间或 attempt id。冻结配置通过
`claim_producer_variant=A|B_empty` 预先绑定两个带 claim B 臂的唯一来源。只要选定快照非空，两个 consumer
就使用通过检查的只读快照；producer 是否通过 verifier 不影响 claim 是否可被注入。consumer 运行期间不得
继续接收 producer 的新 claim、policy 或 dispute 更新。

自动化全量可选用自适应两阶段。此时物理 A / B_empty 仅作为对称候选 S1 / S2；两者在同一
冻结 task 集合上全部结束后，按预注册的全量 verifier 通过数、F2P micro、完全平局选 S1
依次排序。胜者整体重标为逻辑 A，败者整体重标为逻辑 B_empty，再让两个 claim consumer
读取胜者的逐题 bundle。禁止逐题挑选、按 claim 是否非空挑选或让 consumer 结果反向影响选择。
该模式的逻辑 A 是两次 producer 的最大值，因此报告必须保留物理臂与选择分数，且不能把它
替代固定 producer 实验来宣称无选择偏差的因果效果。

### 3.3.1 成功效率与失败恢复的分层

每题先并行运行 A 与 B_empty，再从 pristine workspace 并行启动两个 claim consumer。配置选定 producer
的 verifier 结果只决定统计分层，不决定两个带 claim 的 B 臂是否启动：

| 分层 | 入组条件 | B 臂 | 主要问题 |
| --- | --- | --- | --- |
| `success_efficiency` | 选定 producer verifier 通过，且 freeze snapshot 非空 | `B_claim`、`B_forced_claim` | 自主检索与强制交付的 claim 能否维持质量并降低 agent step、请求、观测 token 或耗时？ |
| `failure_recovery` | 选定 producer verifier 未通过，且 freeze snapshot 非空 | `B_claim`、`B_forced_claim` | 失败中的观察、已排除路径和测试结果能否让 claim consumer 比 producer 更常通过 verifier？ |
| `unpaired_no_claim` | 选定 producer freeze snapshot 为空 | 无 claim consumer（除非显式开启空 bundle 对照） | 记录 claim 产出覆盖率，不进入 claim 对照统计。 |

失败 claim 不被当作已验证事实：它们只能作为带 provenance 的冻结观察供 consumer 自主判断。consumer
不得获得 producer 的 patch、workspace、session、trace 或日志，因此失败恢复衡量的是 ACN 外化 claim
的价值，而非续跑 producer 的工作区。

### 3.4 每次运行必须落盘

每个 attempt 生成一个不可修改的 `run_manifest`，至少包含：

- DeepSWE、Pier、ACN、skill、system prompt 和 task prompt 的 revision / SHA-256；
- task id、base commit、agent/verifier image digest、OS 和资源限制；
- provider、实际模型名、checkpoint、reasoning effort、采样参数、context window、单次
  `max_tokens`、retry 和 cache 策略；
- enabled tools、MCP、subagent、Memory、Claim、网络白名单和 sandbox 配置；
- `max_tool_loop_turns`、并发工具数、file/code-run 输出上限、compact 阈值及实际是否触发；
- agent/verifier timeout、退出原因、runner/proxy/network 异常；
- input/output/cache token、完整与不完整模型请求数、agent step、tool call 及标准化费用；
- workspace 初始/最终 hash、patch hash、verifier 结果、router 返回和实际使用的 claim id。

缺少 manifest、配置 hash 对不上或发生白名单外联网的 attempt 不进入正式统计，修复后重跑。
单次可重试请求若在收到响应前中断，会保留为 `incomplete_model_responses` 审计告警，但不因而
否定已完成的 agent/verifier 结果；成功响应的 usage 必须完整。token 与标准化费用在这种情况下
标为“成功响应观测值下界”，并按 arm 报告不完整请求数，不把未知的中断请求成本补零或伪造。

## 4. ACN 怎么跑

每道题先运行 A；当且仅当 A freeze 后存在 claim 时，再运行三个彼此隔离的 B agent：

| 组别 | 作用 | 可见信息 |
| --- | --- | --- |
| A：producer | 第一次正常运行，同时产出 claim | 无历史 claim |
| B_empty | 全新 agent，对照组 | router 可用，但 team store 为空 |
| B_claim | 另一个全新 agent，实验组 | 只能通过 router 获取 A 产出的 freeze claim；A 成功和失败两种情况均可注入 |
| B_forced_claim | 另一个全新 agent，受控交付组 | 首轮任务上下文附有同一冻结 router 检索的完整 claim；仍不能看到 A 的 patch、session、日志或 private memory |

三个 B agent 都从同一仓库快照开始，使用相同模型、system prompt、skill、工具、预算和 verifier。
`B_claim` 与 `B_forced_claim` 的唯一差别是 claim 是否由模型自主检索；两者均必须由同一冻结 router
产生可校验的 content hash 归因。

B_claim 不得获得 A 的 patch、工作区、session、日志或 private memory；这些内容如果被带过去，
测到的就不是 claim 价值。A 通过 verifier 的题归入效率分层；A 未通过但有 claim 的题归入失败恢复
分层，两者绝不混合计算 uplift。

## 5. 统一 Skill

四组使用同一份 `coding-benchmark` skill，主要补足轻量 harness 缺少的通用工作流：

> 阅读任务和仓库 → 复现问题 → 定位原因 → 实现修复 → 跑针对性测试 → 按错误返工 → 检查 diff → 最终验证。

skill 不能包含题目答案或仓库专属提示。评测期间 skill 原文、模型配置和预算保持不变。

## 6. 模型和调用渠道

第一轮使用本地部署的 **deepseek-v4-flash-local**，由独立模型服务提供 endpoint。

这样做的原因是：

- 成本低，适合先验证 ACN 流水线和 claim 信号；
- 公开数据可用于建立外部参照，但不替代本次受控实验；
- 如果结果有区分度，再申请更强模型，不必一开始投入大量 token。

运行时冻结具体 checkpoint、采样参数、上下文长度和每题限额。模型服务需要返回实际模型名和
input/output/cache token，避免路由换模或缓存差异无法解释。

## 7. 记录什么

| 指标 | 用途 | 来源 |
| --- | --- | --- |
| 成功效率 | `B_claim`、`B_forced_claim` 分别相对 `B_empty` 的 agent step、成功响应观测 token、耗时差 | `success_efficiency` 的按题配对结果 |
| 失败恢复 uplift | `pass(B_claim) - pass(B_empty)` 与 `pass(B_forced_claim) - pass(B_empty)` | `failure_recovery` 的按题配对结果 |
| token | 成功响应返回的原始 usage；中断请求单列计数，不补零 | 模型服务原始 usage |
| 标准化费用 | 按冻结官方费率换算，缓存单独计价 | token usage + 价目表 |
| agent step | 看完成任务需要多少轮决策 | ACN session JSONL |
| claim funnel | B_claim / B_forced_claim 的 bundle 可用、router 检索、内容注入、模型报告使用及 claim 数 | attempt 记录 + aggregate manifest |

`agent step` 统一定义为一次完整的模型响应；tool call 数另记。实际本地 GPU 成本如果模型服务
能够提供则单列，不能和按官方费率换算的费用混成一个数。

出现 `incomplete_model_responses` 时，attempt 仍可进入 verifier 和 claim 分层；报告必须同时给出
每个 arm 的不完整请求数及受影响 attempt 比例。涉及 token/费用的结论仅使用“成功响应观测值下界”
表述，不将其解释为完整账单。

## 8. 运行规模

| 阶段 | 规模 | 目的 |
| --- | ---: | --- |
| Pre-smoke | 5 题 × 4 组 = 20 attempts | 先验证端到端协议、隔离、计量和 claim 归因 |
| Smoke | 30 题 × 4 组 = 120 attempts | 验证预算、无 claim 基线、自主检索和强制交付方向 |
| Full | 113 题 × 4 组 = 452 attempts | 形成全量结果 |

5 题和 30 题都从完整任务清单按固定 seed 无放回抽取并冻结。Pre-smoke 的第 1 题必须先完成
A、`B_empty`、`B_claim`、`B_forced_claim` 四臂硬门禁，确认 schema、verifier、artifact hash、broker
request/step nonce、usage、claim/router 证据和隔离检查全部闭合，才允许运行余下 4 题。
硬门禁通过后，余下 4 题可按平台限流并发；每题内部仍保持 A → freeze → 三个 B 臂串行。若
freeze snapshot 为空，两个带 claim 的 B 臂记为 `NO_ELIGIBLE_CLAIM` 而不运行。broker 必须使用独立随机端口，
避免并发 task 共享连接或抢占固定端口。

每个 task/arm 只允许一次解题运行；verifier 0 分是有效结果，不得重跑刷分。单次可重试模型请求的
中断由 agent 内部 retry 处理，并作为非阻断审计告警留存；只有明确的 runner、容器、网络或 proxy
故障可以原配置重试一次，并保留失败 attempt。若 Pre-smoke 后模型、skill、
预算和执行协议不变，结果可并入 Smoke。

上游若以 HTTP 429 并明确返回“并发容量耗尽”的机器可读代码，Rust result 必须写入稳定的
`failure_kind`。宿主将其记为基础设施失败，保留 result、event ledger 和运行进度，但不经过
Gate、freeze 或后续 B 臂，也不混入 agent、claim、verifier 指标；不能只凭 HTTP 429 泛化该归因，
以免把普通限流或 agent 侧失败错误排除。

若 Smoke 后模型、skill、预算和执行协议不变，**保留这 30 题结果，
Full 只补剩余 83 题，即新增 83 × 4 = 332 attempts**，不重复花钱。

只有 Smoke 暴露出协议错误并导致配置修改时，受影响的 30 题才需要重跑。

## 9. 何时继续跑 Full

Smoke 完成后检查：

- verifier、router、session JSONL 和成功模型响应 usage 均能稳定落盘；
- 无 claim 基线没有出现明显的全失败或全通过；
- B_claim 能实际检索到 claim；
- token 和费用的成功响应观测值可以复算，并按 arm 报告中断请求比例；全量预算可接受；
- 成功效率分层或失败恢复分层至少出现值得继续验证的信号。

30 题只用于做投入判断，不发布强结论。Full 报告按题配对的得分差，并附 95% 置信区间。
agent 自身失败按未通过计分；runner、网络或 proxy 故障修复后重跑，不混入模型失败。

## 10. 最终产出

### 图一：复刻公开 Harness 对比图

柱状图展示公开 Claude Code、Cursor CLI、OpenCode、Codex 数据，并增加 ACN 的 **B_empty 无 claim**
结果。公开数据直接引用，不复跑；图下注明模型和协议并未完全控制。

### 图二：ACN Claim 增益

分两栏展示 `B_empty`、`B_claim` 与 `B_forced_claim`：成功效率栏只比较 A 已通过题的
step/token/耗时；失败恢复栏比较 A 未通过题的 verifier pass rate。两栏都报告不完整请求比例，
并把自主检索与强制交付分开呈现。

成功效率栏展示：

- 平均 token / task；
- 标准化费用 / task；
- 平均 agent step / task。

失败恢复栏分别展示 `pass(B_claim) - pass(B_empty)`、
`pass(B_forced_claim) - pass(B_empty)` 及其置信区间。另画“横轴成功响应观测费用、纵轴通过率”的
散点图，并明确费用为下界；任一 claim 臂相比 B_empty 越向左上移动，说明 claim 带来的净收益越好。

## 11. 组会待拍板

1. deepseek-v4-flash-local 的具体 checkpoint、采样参数和每题 token/时间上限；
2. 模型服务能否提供 input、output、cache token、实际模型名和费用数据；
3. 统一 `coding-benchmark` skill 的最终内容；
4. 30 题固定抽样结果和全量预算；
5. 什么信号触发更强模型的第二轮 Smoke。

## 参考

- [DeepSWE 官方页面](https://deepswe.datacurve.ai/)
- [DeepSWE GitHub](https://github.com/datacurve-ai/deep-swe)
- [DeepSWE task.toml 配置示例](https://github.com/datacurve-ai/deep-swe/blob/435ee89ec2f2e2289f33b0da4f992f0b7b7266b9/tasks/true-myth-iterable-collection-combinators/task.toml)
- [Pier mini-swe-agent adapter](https://github.com/datacurve-ai/pier/blob/0daf53d3599e58c4506cf0bcff5e12c77dc282d2/src/pier/agents/installed/mini_swe_agent.py)
- [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent)
- [mini-swe-agent mini.yaml](https://github.com/SWE-agent/mini-swe-agent/blob/a83fcae82d2a08f0ee0c688f9d137b3566c097f8/src/minisweagent/config/mini.yaml)
- [Codex vs OpenCode](https://artificialanalysis.ai/agents/coding-agents/comparisons/codex-vs-opencode)
- [Claude Code vs Cursor CLI](https://artificialanalysis.ai/agents/coding-agents/comparisons/claude-code-vs-cursor-cli)
- [Artificial Analysis 方法](https://artificialanalysis.ai/methodology/coding-agents-benchmarking)
