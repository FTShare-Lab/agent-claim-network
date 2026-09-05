# Claim harness 异机全量配对实验

本文给出在另一台评测机上比较 `ff12e50c0bb16eb114dfd8b45353f49ef4c341b6`
与本次 claim harness 改动的可执行流程。实验使用现有 DeepSWE runner、冻结 manifest、attempt
plan 和 Gate，不修改 evaluator、verifier、计分或 claim 质量门禁。

本实验只能记为 `diagnostic`。runner 的 `formal` 模式固定锚定历史产品 revision；为了比较
`ff12e50` 与它的后继提交，不得改动或绕过该锚点，也不得把 diagnostic 结果改称 formal score。

## 研究问题与固定对照

每个版本、每个 rollout 都完整执行 113 题的四臂，共 452 个 task-arm：

| Arm | 作用 |
| --- | --- |
| `A` | 运行 producer，并产生该题的冻结 claim bundle |
| `B_empty` | 不提供 claim 的通用 harness 基线 |
| `B_claim` | bundle 可用，但由模型自主调用 `consult_router` 检索 |
| `B_forced_claim` | 启动时强制提供与 `B_claim` 相同的冻结 claim |

预注册的主比较为：

1. 新版 `B_empty` 对旧版 `B_empty`：估计通用 harness 行为变化；
2. 各版本内部 `B_claim - B_empty`：估计自主发现路径的知识增益；
3. 各版本内部 `B_forced_claim - B_empty`：估计 claim 内容在首轮已暴露时的知识增益。

不要只挑 3–5 题做结论，也不要只报告成功题。固定分母始终是每个版本、每个 rollout 的
113 题；基础设施失败、Gate 失败、空 bundle 和缺失 arm 单独报告原因，不能从分母静默删除。

## 必须冻结的变量

旧版与新版使用相同的 DeepSWE revision、Pier revision、113 个 task 目录及其 tree hash、模型请求名、
响应 checkpoint、`reasoning_effort`、资源与时间预算、dataset seed、plan seed、coding skill 目录及
其 hash。attempt plan 的 seed 与 arm 顺序相同，但 plan 内含绝对 `output_path`，所以必须为两版本
分别生成，不能共用同一个 plan 文件。两边都固定：

```json
{
  "run_class": "diagnostic",
  "harness_mode": "standard",
  "claim_producer_variant": "A",
  "run_all_variants_without_claims": true,
  "claim_quality_gate": "verified_producer_only"
}
```

不要启用 `adaptive` producer 选择。`A` 未通过 verifier 时，`verified_producer_only` 会隔离其
claim；`run_all_variants_without_claims=true` 仍会运行两个 claim consumer，并将空 bundle 作为
可审计结果。模型 checkpoint、effort、token/context 上限、超时或 skill hash 任一变化都需要另开
cohort，不能混入主比较。

两版本必须使用独立的干净 checkout、独立构建的 Linux `acn_eval`、独立 Python runner、独立
attempt plan 和独立 `output_dir`。共享只读的冻结 dataset 与 normalized task trees；不要让一个版本
写入另一个版本的 checkout、binary 或输出目录。`acn_main_revision` 在两份 diagnostic 配置中都写
`ff12e50c0bb16eb114dfd8b45353f49ef4c341b6`，`acn_version` 写 `0.2.5`，而 `acn_revision`
分别写旧版 commit 和最终新版 commit。新版 commit 必须是 `ff12e50` 的后代。

本次新版同时包含以下已确定行为：

- `B_claim` 首次 system context 自动注入有界 claim 目录，只含 `id`、`name`、`scope`、`holder`、
  `confidence`；正文和证据不在目录中；
- 模型仍通过既有 `scope + semantic_query` 的一次 query 获取全文；目录中的 ID 在 query 发生前不得
  计为 `used`，Gate、`RouterEvidence`、`AgentQuery` 和 runner 契约不变；
- 完整、连续的长工具输出在原预算内增加 tail preview，但消费 cursor 只推进已消费的 prefix；
- compaction 确定性保留真实成功 file tools 形成的 read/modified 工作集；
- producer prompt 澄清可迁移判断、适用条件、证据与未验证边界，不修改 claim DTO。

因此本次提交既改变 claim producer 内容，也改变 consumer 的发现路径和共享 harness 行为。本文不预言
成功率、成本或退步方向。

## 准备冻结输入

以下路径均是中性绝对占位路径。配置不得包含 endpoint 或 credential；模型 endpoint 只通过
`ACN_EVAL_UPSTREAM_BASE_URL` 提供，credential 只通过 `ACN_EVAL_UPSTREAM_KEY` 提供。

```sh
export OLD_ACN=/opt/acn-checkouts/claim-harness-old
export NEW_ACN=/opt/acn-checkouts/claim-harness-new
export DEEPSWE_CHECKOUT=/opt/benchmarks/DeepSWE
export PIER_CHECKOUT=/opt/benchmarks/pier
export SHARED_RUN=/var/lib/acn-eval/claim-harness-paired/rollout-01

git -C "$OLD_ACN" rev-parse HEAD
git -C "$NEW_ACN" rev-parse HEAD
git -C "$NEW_ACN" merge-base --is-ancestor \
  ff12e50c0bb16eb114dfd8b45353f49ef4c341b6 HEAD
git -C "$DEEPSWE_CHECKOUT" status --porcelain
git -C "$PIER_CHECKOUT" status --porcelain
```

为 rollout 冻结完整 113 题；`freeze-execution-dataset` 只准备输入，不运行模型。相同 rollout 的
新旧版本复用这份 manifest 和 normalized tree。已有正式 pipeline 产出的等价冻结产物时，直接复用并
记录其 hash，不要重新抽样。

```sh
PYTHONPATH="$OLD_ACN/benchmarks/deepswe/src" python -m acn_deepswe.cli \
  freeze-execution-dataset "$DEEPSWE_CHECKOUT/tasks" \
  "$SHARED_RUN/frozen-manifest.json" "$SHARED_RUN/normalized" \
  --deepswe-checkout "$DEEPSWE_CHECKOUT" --pier-checkout "$PIER_CHECKOUT" \
  --seed 20260901 --sample-size 113

PYTHONPATH="$OLD_ACN/benchmarks/deepswe/src" python -m acn_deepswe.cli \
  plan "$SHARED_RUN/frozen-manifest.json" "$SHARED_RUN/old-output" \
  --seed 20260902

PYTHONPATH="$NEW_ACN/benchmarks/deepswe/src" python -m acn_deepswe.cli \
  plan "$SHARED_RUN/frozen-manifest.json" "$SHARED_RUN/new-output" \
  --seed 20260902
```

复制 [示例配置](manifests/claim-harness-paired.example.json) 两次。两份配置的
`frozen_manifest`、DeepSWE/Pier 路径、model、checkpoint、effort、预算、plan seed 和 skill hash 必须
相同。两份 `attempt_plan` 的 arm 身份与顺序应相同，绝对输出路径分别指向自己的目录。只修改以下
版本专属值：

| 配置 | `acn_eval` / runner 来源 | `attempt_plan` / `output_dir` | `acn_revision` |
| --- | --- | --- | --- |
| old | `$OLD_ACN` 构建和 checkout 内容 | `$SHARED_RUN/old-output/...` | `ff12e50c...` |
| new | `$NEW_ACN` 构建和 checkout 内容 | `$SHARED_RUN/new-output/...` | 最终新版 commit |

示例中的模型别名、checkpoint、全零 revision / image digest 和路径都是占位值，须先替换为评测机核验过的冻结值；新版 revision 用该 checkout 的 `git rev-parse HEAD`。这些占位值不能用于实际评测。

配置中的 `task_workers` 必须按评测机实测容量填写，不能让 runner 静默缩减资源。两版本资源相同，
调度顺序可交错以减少时间漂移，但不要并发到共享 Docker 容量不足。

## 离线校验

先在各自 checkout 运行 runner 单元测试。随后对两份真实配置执行 `--dry-run`；它会核对 checkout
revision、冻结 task tree hash 和四臂计划，不调用 Docker、Pier 或模型。`auto prepare` 会检查/准备
Docker 相关状态，不能称为离线校验，本步骤不使用它。

```sh
cd "$OLD_ACN"
PYTHONPATH=benchmarks/deepswe/src python -m unittest discover \
  -s benchmarks/deepswe/tests -p 'test_*.py'

cd "$NEW_ACN"
PYTHONPATH=benchmarks/deepswe/src python -m unittest discover \
  -s benchmarks/deepswe/tests -p 'test_*.py'

ACN_EVAL_UPSTREAM_BASE_URL=https://model-gateway.example \
PYTHONPATH="$OLD_ACN/benchmarks/deepswe/src" python -m acn_deepswe.presmoke_cli \
  --config /etc/acn-eval/claim-harness-old-rollout-01.json --dry-run \
  > "$SHARED_RUN/old-dry-run.json"

ACN_EVAL_UPSTREAM_BASE_URL=https://model-gateway.example \
PYTHONPATH="$NEW_ACN/benchmarks/deepswe/src" python -m acn_deepswe.presmoke_cli \
  --config /etc/acn-eval/claim-harness-new-rollout-01.json --dry-run \
  > "$SHARED_RUN/new-dry-run.json"
```

验收两个 dry-run JSON：`task_order` 完全一致且长度为 113；每题 `arms` 都恰好包含 `A`、
`B_empty`、`B_claim`、`B_forced_claim`，并且新旧对应题的 arm 顺序一致（runner 会平衡两个 claim
consumer 的先后顺序）；`phase_mode=full`；producer、quality gate、预算与版本绑定符合上文。
dry-run 不验证 Linux binary 身份或 Docker image digest，这些由真实启动前的现有 preflight 验证。

## 异机执行

本轮开发机禁止执行下面的命令。评测机在 dry-run、容量检查和预登记完成后，分别对旧版和新版运行
现有入口：

```sh
ACN_EVAL_UPSTREAM_BASE_URL="$ACN_EVAL_UPSTREAM_BASE_URL" \
ACN_EVAL_UPSTREAM_KEY="$ACN_EVAL_UPSTREAM_KEY" \
PYTHONPATH="$OLD_ACN/benchmarks/deepswe/src" python -m acn_deepswe.presmoke_cli \
  --config /etc/acn-eval/claim-harness-old-rollout-01.json

ACN_EVAL_UPSTREAM_BASE_URL="$ACN_EVAL_UPSTREAM_BASE_URL" \
ACN_EVAL_UPSTREAM_KEY="$ACN_EVAL_UPSTREAM_KEY" \
PYTHONPATH="$NEW_ACN/benchmarks/deepswe/src" python -m acn_deepswe.presmoke_cli \
  --config /etc/acn-eval/claim-harness-new-rollout-01.json
```

不要因 verifier 0 分或 agent failure 重跑；它们是有效结果。只按 runner 的既有规则续跑无终态的中断
task，并保留原始证据。不得用重复 attempt 替换较差结果。

至少预先固定多个完整 rollout（建议 3 个），所有 rollout 都报告。每个 rollout 仍覆盖同一 113 题；
rollout 间使用预先登记的不同 plan seed，某个 seed 在旧版与新版之间保持相同。不要在看到结果后选
最好的一轮，也不要把多个 rollout 当成更多独立 task；汇总时同时给出逐 rollout 配对结果和按 task
聚类的总体不确定性。

## 汇总指标

先保留每个版本原生 `presmoke-aggregate.json`、task manifest、Gate、attempt result、events 和 claim
bundle，再按 `(rollout, task_id)` 对齐。至少报告：

- 每臂通过数 / 113、成功率和完整 coverage；缺失、Gate/基础设施失败按原因计数；
- 配对新增成功（subject 通过、reference 未通过）、配对退步（subject 未通过、reference 通过）、
  净成功变化及 `exact_mcnemar_p`；跨版本 `B_empty` 和版本内两组 claim 对照分别计算；
- 总/均值 `model_requests`、input/output/cache-read/reasoning tokens、steps、墙钟时间，以及
  `cache_hit_rate`；成本同时给绝对量和每个新增成功的增量；没有净新增成功时该比率记为未定义，
  不能只报成功 attempt 的成本；
- `empty_claim_bundle_attempts` 与 claim 使用漏斗：bundle 可用、router 检索、内容注入、模型报告使用，
  每层同时给 task 数和 claim 数；“bundle 已挂载”不能替代发现或使用证据；
- 按 producer 是否通过与是否有 eligible claim 分层，保留 quarantined claim 和空 bundle cohort，
  不把它们混入有效 claim 注入效果。

原生 aggregate 已提供版本内 `paired_against_no_claim_baseline`、`claim_funnel`、
`cohort_coverage` 和 usage 汇总。跨版本 `B_empty` 需要基于两个独立 aggregate 的 task 级结果配对，
并验证 task、rollout、checkpoint、skill hash 和预算身份后再计算。

## 解释边界

四臂中的 claim 都由该版本自己的 `A` producer 产生。本次 producer prompt 已改变，同时 consumer 增加
自动摘要目录，共享 harness 还改变长输出与 compaction 行为。因此新版与旧版的
`B_claim - B_empty` 或 `B_forced_claim - B_empty` 差异同时包含 producer 内容、consumer 发现路径及
共享 harness 的变化，不能解释为纯检索因果效应。应并列报告 bundle 产出率、eligible / quarantined
claim 数、内容长度或其他预登记的内容特征。对 `B_claim` 的漏斗还应核对：不得把目录曝光直接当成
全文 `retrieved` 或 claim `used`；只有既有 query 实际取回正文后，才按当前证据契约判断后续阶段，
query 前看到目录 ID 不得计为使用。

现有 `b_only_from_a_output_dir` 只支持在同一 ACN revision 下，从完整 A-only 输出读取每题
`manifest.json` 与 `claims.json`，再运行其余三个 B 臂；它要求 source/output 完全隔离，且启动时仍会
绑定当前 revision。它不提供“把旧 revision 的 frozen bundle 直接喂给新 revision”的跨版本接口。
因此本实验不能用该字段消除 producer 变化；若未来确需固定 producer 内容，必须先单独设计并审查新的
可审计协议，不能在本次配置中伪造路径或绕过 revision 绑定。

## 后续研究

当前可执行四臂能比较无 claim、自主发现与强制曝光，却不足以证明 claim 相对普通文本 memory 的优势，
也不足以证明跨题或跨 repository 泛化。后续应另行实现并预注册：等 token 且计入相同 producer 成本的
自由文本 memory 强基线；注入错误 claim 或相互冲突 claim 的鲁棒性实验；以及以多个预注册 seed 持续
运行的配对 CI。它们目前都不是 runner 选项或已有能力，不能用本实验配置字段代替。

本实验测得的是指定 DeepSWE/Pier/task/model/checkpoint/预算下的 diagnostic 差异，不是官方单 agent
排行榜分数，也不能据此声称其他模型、任务分布或正式锚点必然提分。
