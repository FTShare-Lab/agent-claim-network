# DeepSWE Full-113 四臂评测归档

## 结论

本归档冻结 2026-08-15 的 `deepseek-v4-flash-local-exp` Full-113 四臂运行。按完整的 113 题分母，
`B_forced_claim` 为最高的 verifier 成功率：50/113（44.2%）；`B_empty` 为 48/113（42.5%）；A 与
`B_claim` 均为 46/113（40.7%）。这是 ACN 四臂、单次 rollout 的结果，不是官方模型榜单的可直接复现值；
本次历史运行没有冻结模型出口模式，不能作为当前加固版 formal Gate 下的严格正式得分。

相对用户提供的 54.4 外部公布分数，最高的本地 arm 低 10.2 个百分点。现有证据不能把差距归因于模型
能力下降：agent harness、提示、工具、completion 契约、计分 / 重跑策略、模型服务 endpoint 的精确
checkpoint 和评测 task revision 都没有被证明一致。特别是请求别名回显为 `deepseek-v4-flash` 只能证明
路由目标名称，不足以证明其与外部公布分数所用的具体服务构建完全相同。

## 冻结运行配置

| 项目 | 值 |
| --- | --- |
| ACN `main` 基线 | `70473898468c1cfbd036d350a70cae4de12afdce` |
| 评测工作树 | `992b039c0805b4032d33a513f6959cca703d8d1a+evaluation-worktree` |
| ACN 配置 hash | `05586a7ca5ae50d3d5ae668b3de9bbf526895df02dee8a9e0994e8fe613c9625` |
| DeepSWE revision | `435ee89ec2f2e2289f33b0da4f992f0b7b7266b9` |
| Pier revision | `0daf53d3599e58c4506cf0bcff5e12c77dc282d2` |
| 请求模型 | `deepseek-v4-flash-local-exp` |
| 要求的响应 checkpoint | `deepseek-v4-flash` |
| provider / effort | `openai_responses` / `max` |
| Full 规模 | 113 题，A / B_empty / B_claim / B_forced_claim，各一次 rollout |
| 调度 | 30 个 task worker；每 task 2 CPU、8 GiB 内存、20 GiB 存储 |
| 上下文与预算 | 1,000,000 context tokens；65,536 max tokens；recap compact ratio 0.80 |
| 时限 | agent 5,400 秒；deadline reserve 120 秒；task 原生 verifier timeout 1,800 秒 |
| 网络 | agent 和 verifier 都为 no-network；模型出口模式当时未冻结到 attempt 配置 |
| 重试 | LLM retry 2 次（1,000–30,000 ms）；Pier trial `max_retries=0` |
| 监控 | 每 30 秒采样 progress，600 秒无活动仅标记、绝不自动终止 |

运行跳过 smoke（`smoke_size=0`），以固定种子直接启动 Full-113。`B_claim` 在模型主动调用
`consult_router` 时才获取冻结 claim；`B_forced_claim` 将同一冻结 bundle 的检索结果放入首轮上下文。
两条 recap / claim 指引分别要求忽略通用环境噪音、并在使用 claim 时独立检查适用条件、反例、边界和异常路径。

## 计分口径与完整性

正式分数只由 `verifier_passed=true` 计为成功；formal gate 是产物、隔离、usage 与 verifier 证据的完整性
检查，不等同于解题成功。主运行计划 452 个 task-arm，实际启动 451 个：

- 两个已启动 arm 的 verifier 均在 task 自己的 1,800 秒时限后无结果，原始 gate 因而为
  `VERIFIER_DID_NOT_RUN`；它们不是模型请求、并发或模型名称问题。
- `pwntools-tube-multiplexing` 的一个 `B_forced_claim` arm 未被实例化。
- `resume-001` 以原配置重跑两个完整 task group（8 个 arm），并用其中 3 个 arm 补上上述两个 formal
  failure 与一个缺失 arm。其余 5 个是为 task-level resume 必然产生的重复执行，保留在资源账本中，
  不替换原始已通过 formal gate 的结果。

因此，按**当时 Gate 定义**，最终计分集为 452 个 arm，historical formal gate 452/452 通过。实际执行账本
为 459 个 arm，包含 2 个原始 timeout failure 与 8 个 resume arm。当前代码把模型出口模式写入冻结 attempt
配置，只有 `model_egress_mode="pier"` 可通过正式隔离；由于本次历史产物没有该字段，不能在新版 Gate 下被
重新认证为严格 formal score。这个选择规则在同名 JSON 的 `scoring_selection` 字段固定，不按 verifier 成功
与否挑选 rollout。

## 结果

| Arm | verifier 成功 | 成功率 | 平均 steps | 平均输入 token | cache 命中率 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 46 / 113 | 40.7% | 124.22 | 19.34M | 98.33% |
| B_empty | 48 / 113 | 42.5% | 123.79 | 19.02M | 98.46% |
| B_claim | 46 / 113 | 40.7% | 117.62 | 17.51M | 98.32% |
| B_forced_claim | 50 / 113 | 44.2% | 116.68 | 17.27M | 98.37% |

跨 arm 的 113 题分布为：四臂全通过 15（13.3%）、至少一臂通过 79（69.9%）、四臂全失败 34（30.1%）；
成功 arm 数为 0 / 1 / 2 / 3 / 4 的 task 数依次为 34 / 20 / 22 / 22 / 15。

相对 A 的配对结果：

| 对比 | 均通过 | 仅 A 通过 | 仅该 arm 通过 | 均失败 |
| --- | ---: | ---: | ---: | ---: |
| B_empty vs A | 27 | 19 | 21 | 46 |
| B_claim vs A | 29 | 17 | 17 | 50 |
| B_forced_claim vs A | 31 | 15 | 19 | 48 |

`B_forced_claim` 的单臂成功数最高，同时平均输入 token 和平均 steps 最低；但单次 rollout、113 题规模和
claim bundle 的 task 依赖性意味着这只能视为本次配置下的观测，不能宣称为稳定的因果提升。

## 资源账本

| 账本 | input tokens | cache read tokens | output tokens | reasoning tokens | 模型请求 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 452 个计分 arm | 8,264,911,061 | 8,130,206,208 | 54,644,158 | 37,346,875 | 54,502 |
| 全部 459 个实际执行 arm | 8,453,917,008 | 8,316,997,888 | 55,607,317 | 37,978,981 | 55,480 |

实际执行账本的 cache read / input 为 98.38%。资源账本含 timeout 与 resume 的真实消耗；计分账本只含每个
task-arm 的既定代表性 rollout，二者不可混用。

## 为什么不能把 44.2 与 54.4 直接对齐

外部 54.4 的具体配置、agent harness、task snapshot、采样次数与统计定义没有随本地运行一同冻结。即使均为
“DeepSWE 分数”，以下差异足以造成数个百分点的变化：

1. 本次运行使用 ACN 的多工具 turn loop、session recap、claim 网络和显式 `submit_task` 契约；外部公布
   分数可能采用不同的 agent harness 与 prompt。长程软件工程 benchmark 对 harness 非常敏感。
2. 本次是四个相关但不同提示条件下的单次 rollout，且主要可比候选是 `B_forced_claim`，而非一个官方标准
   baseline。外部数字的 pass@k、重试和失败处置若不同，分母与成功事件均会变化。
3. 本次请求别名是 `deepseek-v4-flash-local-exp`，响应名称为 `deepseek-v4-flash`。该映射通过当时的
   Gate，但不携带可审计的服务构建 / 权重版本；需由服务提供方或一次直连的对照 run 才能确认等价。
4. 本次 DeepSWE 与 Pier revision 已冻结，外部公布时的 task / verifier revision 未证明相同。两个原始 verifier
   timeout 经原配置 resume 后补齐，但这说明 verifier 时限也会影响报告的有效样本与资源成本。
5. 推理 effort 已设为 `max`。不要把未记录的 `temperature` / `top_p` 当作本运行的主要解释：在 DeepSeek 的
   Thinking Mode 文档中，这两个采样参数不生效；若外部运行不在该模式，才需要另作对照。

要定位差距，下一步应先拿到外部 54.4 的可执行 scorecard（task revision、agent harness、提示、采样和
retry 规则、endpoint checkpoint），然后在同一冻结任务与资源下做最小 harness A/B。没有这些控制变量，
把 10.2 个百分点归因到模型、claim 或一个采样参数都不可靠。

## 可复现性与安全

启动时把模型 endpoint 和 key 仅作为环境变量提供；不要将真实值写进 JSON、README、shell history 或 run
archive。可从 `manifests/presmoke-run.example.json` / `manifests/automated-run.example.json` 复制配置，并将
`model`、`response_model`、`reasoning_effort`、`model_egress_mode`、资源和 timeout 改为本表冻结值。运行结束后保留
`frozen-manifest.json`、`attempt-plan.json`、每 arm 的 `attempt-result.json` / `gate.json` / `progress.json` 及
resume manifest，才能重建本报告的两个账本。
