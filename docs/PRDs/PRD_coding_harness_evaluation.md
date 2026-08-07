# PRD: ACN DeepSWE 评测打样

> 状态：已实现 Pre-smoke 评测基础设施；Smoke 与 Full 的实际执行仍需按本文冻结配置后另行启动。

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

首轮固定使用 DeepSWE revision
`e016041a6ccf8da29906afc9a3f5a8df940a1f78` 与 Pier revision
`fefa7475a32bb05271abdea378e8083c83eb5c35`。任务清单、verifier 和容器镜像 digest
随实验 manifest 一起冻结，避免不同版本混算。

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

以下配置来自 2026-07-23 检查的 DeepSWE 113 个 `task.toml` 及 Pier 的
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

Pinned Pier 不识别 DeepSWE `task.toml` 中的 `agent.network_mode` /
`verifier.network_mode`。runner 必须在运行前做 fail-closed 转换：仅当两者均为
`"no-network"` 时，生成 Pier 兼容副本，并显式写入
`environment.allow_internet = false` 与 `verifier.environment.allow_internet = false`。
原始和转换后配置的 SHA-256 都写入 manifest；未经转换的任务禁止运行。

### 3.2 ACN 对齐配置

ACN 不要求伪装成 `mini-swe-agent`，其原生文件、命令和 router 工具属于待测 harness；但环境边界
必须对齐，且 A、`B_empty`、`B_claim` 除 claim 可见性外完全相同。

| 项目 | ACN 评测配置 |
| --- | --- |
| 普通公网 | 关闭；`web_search`、`web_fetch`、`web_request` 不注册，Shell 出网由 sandbox 硬拦截 |
| 网络白名单 | task 仅能经 Pier 隔离代理访问宿主模型 broker；router 使用进程内冻结 bundle，maintainer 关闭 |
| 依赖 | 预装进镜像；运行中禁止从 GitHub、PyPI、npm 等下载 |
| MCP | 关闭，`.mcp.json` 为空 |
| Memory | 关闭注入、读取、写入和后台 memory review；每个 attempt 使用全新 `acn_home` |
| Session | 不 resume；每题、每组使用新的 agent id、session id 和 session 目录 |
| Workspace | 从同一 base commit / image digest 创建 pristine 副本；组间不共享 git 对象外的运行产物 |
| Claim | 按 3.3 的组别矩阵配置；B 不能看到 A 的 patch、session、trace、日志或私有文件 |
| Skill | 三组注入完全相同的 `coding-benchmark` skill，记录全文 SHA-256 |
| Prompt | 相同 system prompt、首条任务 prompt 和注入顺序；禁止中途人工补充提示 |
| 工具 | 三组工具 schema、权限、并发上限和输出截断相同；除 router 返回内容外不得因组别变化 |
| 人工交互 | 关闭 `ask_user` 和 TUI user shell；一次性提交任务，agent 自主结束 |
| Subagent | 首期关闭，避免引入额外模型实例和未计量的共享状态 |
| 超时 / 资源 | agent 5400 秒，verifier 1800 秒；2 CPU / 8 GiB / 20 GiB / 0 GPU |
| step / cost limit | 不设置早于 5400 秒 timeout 的停止线；若实现要求正数上限，设为不会在 Smoke 中触发的固定值 |
| verifier | 与 agent 分离；只应用最终 patch，在 pristine 容器中离线判卷 |

网络必须由 runner/sandbox 强制执行，不能只在 prompt 中要求模型“不要联网”。模型 broker
记录每条请求；白名单外请求写入审计日志并拒绝。

### 3.3 Claim 可见性矩阵

| 状态 | A：producer | `B_empty` | `B_claim` |
| --- | --- | --- | --- |
| 历史 Memory / Session | 空 | 空 | 空 |
| 初始本地 Claim | 空 | 空 | 空 |
| Router | 进程内空 bundle | 进程内空 bundle | 进程内只读 bundle，仅含 A 本题 freeze barrier 前的合格 claim |
| A 的 workspace / patch / log / trace | 自身运行可见 | 不可见 | 不可见 |
| 运行中团队数据变化 | 不读取历史 claim | 禁止 | 禁止；开始前生成只读快照 |

A 完成并退出后，由宿主写入不可变 freeze barrier；claim 资格检查只采信 barrier 前的宿主事件
账本，不采信 claim 文件自报的时间或 attempt id。`B_claim` 使用通过检查的只读快照。B 运行期间
不得继续接收 A 的新 claim、policy 或 dispute 更新。

### 3.4 每次运行必须落盘

每个 attempt 生成一个不可修改的 `run_manifest`，至少包含：

- DeepSWE、Pier、ACN、skill、system prompt 和 task prompt 的 revision / SHA-256；
- task id、base commit、agent/verifier image digest、OS 和资源限制；
- provider、实际模型名、checkpoint、reasoning effort、采样参数、context window、单次
  `max_tokens`、retry 和 cache 策略；
- enabled tools、MCP、subagent、Memory、Claim、网络白名单和 sandbox 配置；
- `max_tool_loop_turns`、并发工具数、file/code-run 输出上限、compact 阈值及实际是否触发；
- agent/verifier timeout、退出原因、runner/proxy/network 异常；
- input/output/cache token、模型请求数、agent step、tool call 及标准化费用；
- workspace 初始/最终 hash、patch hash、verifier 结果、router 返回和实际使用的 claim id。

缺少 manifest、配置 hash 对不上或发生白名单外联网的 attempt 不进入正式统计，修复后重跑。

## 4. ACN 怎么跑

每道题运行三个彼此隔离的 agent：

| 组别 | 作用 | 可见信息 |
| --- | --- | --- |
| A：producer | 第一次正常运行，同时产出 claim | 无历史 claim |
| B_empty | 全新 agent，对照组 | router 可用，但 team store 为空 |
| B_claim | 另一个全新 agent，实验组 | 只能通过 router 获取 A 产出的 claim |

三个 agent 都从同一仓库快照开始，使用相同模型、system prompt、skill、工具、预算和 verifier。
`B_empty` 与 `B_claim` 的唯一差别是 router 中有没有 A 的 claim。

B_claim 不得获得 A 的 patch、工作区、session、日志或 private memory；这些内容如果被带过去，
测到的就不是 claim 价值。

## 5. 统一 Skill

三组使用同一份 `coding-benchmark` skill，主要补足轻量 harness 缺少的通用工作流：

> 阅读任务和仓库 → 复现问题 → 定位原因 → 实现修复 → 跑针对性测试 → 按错误返工 → 检查 diff → 最终验证。

skill 不能包含题目答案或仓库专属提示。评测期间 skill 原文、模型配置和预算保持不变。

## 6. 模型和调用渠道

第一轮倾向使用本地部署的 **GLM-5.2**，由 llm-proxy 提供独立 endpoint。

这样做的原因是：

- 成本低，适合先验证 ACN 流水线和 claim 信号；
- 官方已有 mini-swe-agent + GLM-5.2 的 44%±2% 结果，可作外部参照；
- 如果结果有区分度，再申请更强模型，不必一开始投入大量 token。

运行时冻结具体 checkpoint、采样参数、上下文长度和每题限额。llm-proxy 需要返回实际模型名和
input/output/cache token，避免路由换模或缓存差异无法解释。

## 7. 记录什么

| 指标 | 用途 | 来源 |
| --- | --- | --- |
| pass rate | 看任务完成质量 | DeepSWE verifier |
| claim uplift | `pass(B_claim) - pass(B_empty)`，本期主指标 | 按题配对结果 |
| token | 看 claim 是否减少或增加模型消耗 | llm-proxy 原始 usage |
| 标准化费用 | 按冻结官方费率换算，缓存单独计价 | token usage + 价目表 |
| agent step | 看完成任务需要多少轮决策 | ACN session JSONL |
| claim retrieved / used | 确认 B 是否真的检索和引用 claim | router 记录 + trace |

`agent step` 统一定义为一次完整的模型响应；tool call 数另记。实际本地 GPU 成本如果 llm-proxy
能够提供则单列，不能和按官方费率换算的费用混成一个数。

## 8. 运行规模

| 阶段 | 规模 | 目的 |
| --- | ---: | --- |
| Pre-smoke | 5 题 × 3 组 = 15 attempts | 先验证端到端协议、隔离、计量和 claim 归因 |
| Smoke | 30 题 × 3 组 = 90 attempts | 验证预算、无 claim 基线和 claim 方向 |
| Full | 113 题 × 3 组 = 339 attempts | 形成全量结果 |

5 题和 30 题都从完整任务清单按固定 seed 无放回抽取并冻结。Pre-smoke 的第 1 题必须先完成
A、`B_empty`、`B_claim` 三臂硬门禁，确认 schema、verifier、artifact hash、broker
request/step nonce、usage、claim/router 证据和隔离检查全部闭合，才允许运行余下 4 题。
硬门禁通过后，余下 4 题可按平台限流并发；每题内部仍保持 A → freeze → 两个 B 臂串行。
broker 必须使用独立随机端口，避免并发 task 共享连接或抢占固定端口。

每个 task/arm 只允许一次解题运行；verifier 0 分是有效结果，不得重跑刷分。只有明确的 runner、
容器、网络或 proxy 故障可以原配置重试一次，并保留失败 attempt。若 Pre-smoke 后模型、skill、
预算和执行协议不变，结果可并入 Smoke。

若 Smoke 后模型、skill、预算和执行协议不变，**保留这 30 题结果，
Full 只补剩余 83 题，即新增 83 × 3 = 249 attempts**，不重复花钱。

只有 Smoke 暴露出协议错误并导致配置修改时，受影响的 30 题才需要重跑。

## 9. 何时继续跑 Full

Smoke 完成后检查：

- verifier、router、session JSONL 和 llm-proxy usage 均能稳定落盘；
- 无 claim 基线没有出现明显的全失败或全通过；
- B_claim 能实际检索到 claim；
- token 和费用可以复算，全量预算可接受；
- claim 组至少在得分、token、费用或 step 中出现值得继续验证的信号。

30 题只用于做投入判断，不发布强结论。Full 报告按题配对的得分差，并附 95% 置信区间。
agent 自身失败按未通过计分；runner、网络或 proxy 故障修复后重跑，不混入模型失败。

## 10. 最终产出

### 图一：复刻公开 Harness 对比图

柱状图展示公开 Claude Code、Cursor CLI、OpenCode、Codex 数据，并增加 ACN 的 **B_empty 无 claim**
结果。公开数据直接引用，不复跑；图下注明模型和协议并未完全控制。

### 图二：ACN Claim 增益

展示 `B_empty` 与 `B_claim` 的：

- DeepSWE pass rate；
- 平均 token / task；
- 标准化费用 / task；
- 平均 agent step / task。

另画“横轴费用、纵轴得分”的散点图。B_claim 相比 B_empty 越向左上移动，说明 claim 带来的净收益越好。

## 11. 组会待拍板

1. GLM-5.2 的具体 checkpoint、采样参数和每题 token/时间上限；
2. llm-proxy 能否提供 input、output、cache token、实际模型名和费用数据；
3. 统一 `coding-benchmark` skill 的最终内容；
4. 30 题固定抽样结果和全量预算；
5. 什么信号触发更强模型的第二轮 Smoke。

## 参考

- [DeepSWE 官方页面](https://deepswe.datacurve.ai/)
- [DeepSWE GitHub](https://github.com/datacurve-ai/deep-swe)
- [DeepSWE task.toml 配置示例](https://github.com/datacurve-ai/deep-swe/blob/e016041a6ccf8da29906afc9a3f5a8df940a1f78/tasks/abs-module-cache-flags/task.toml)
- [Pier mini-swe-agent adapter](https://github.com/datacurve-ai/pier/blob/fefa7475a32bb05271abdea378e8083c83eb5c35/src/pier/agents/installed/mini_swe_agent.py)
- [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent)
- [mini-swe-agent mini.yaml](https://github.com/SWE-agent/mini-swe-agent/blob/a83fcae82d2a08f0ee0c688f9d137b3566c097f8/src/minisweagent/config/mini.yaml)
- [Codex vs OpenCode](https://artificialanalysis.ai/agents/coding-agents/comparisons/codex-vs-opencode)
- [Claude Code vs Cursor CLI](https://artificialanalysis.ai/agents/coding-agents/comparisons/claude-code-vs-cursor-cli)
- [Artificial Analysis 方法](https://artificialanalysis.ai/methodology/coding-agents-benchmarking)
