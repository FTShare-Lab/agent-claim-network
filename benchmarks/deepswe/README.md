# ACN DeepSWE runner

比较 `ff12e50` 与 claim harness 改动的异机完整 113 题配对流程见
[CLAIM_HARNESS_EXPERIMENT.md](CLAIM_HARNESS_EXPERIMENT.md)。

在冻结的 DeepSWE / Pier revision 上跑 ACN 的 DeepSWE 四臂实验：每题先执行 `A` / `B_empty`，分别
freeze，再执行 `B_claim` / `B_forced_claim`。`B_claim` 评估模型自主检索 claim 的真实端到端路径，
`B_forced_claim` 是强制提供同一冻结 claim 的受控对照，主比较是两者分别相对 `B_empty` 的差异。

## 与官方口径的关系

模型访问完全沿用 Pier 官方 adapter（`mini_swe_agent` / `codex` / `opencode`）的做法，不自建
代理或 broker：

- 模型 key 从宿主环境变量 `ACN_EVAL_UPSTREAM_KEY` 读取，仅以容器变量
  `ACN_EVAL_MODEL_KEY` 交给 `acn_eval`；进程启动后通过匿名 pipe 原位 re-exec 清除初始环境，
  不进 argv、配置文件、manifest 或 JSONL；
- 出网由 Pier 自己的 Squid 域名 allowlist 限死，只允许 `upstream_base_url` 的主机名，其余
  一律拒绝；agent 与 verifier 的 `allow_internet` 均为 `false`；
- `code_run` 子进程会剥掉 `api_key_env` 指定的变量，题目工作区里的命令看不到 key。

`model_egress_mode` 是冻结启动配置的一部分，默认且唯一可作为正式结果的值是 `"pier"`。它会保留
Pier 的域名 allowlist；环境变量 `ACN_EVAL_MODEL_EGRESS` 不再具有覆盖能力，若存在会使运行直接失败，
防止宿主残留环境绕过 Gate。诊断模型连通性时可在配置中显式写 `"model_egress_mode": "direct"`；这会被
execution manifest 记录，并使 formal Gate 失败，不能计入正式得分。该模式下可额外设置
`ACN_EVAL_CONTAINER_MODEL_PROXY=http://<container-reachable-host>:<port>`，值必须是无凭据的 HTTP URL。
此代理变量仅影响执行 `acn_eval` 的容器进程，不传给后续提交代码的容器进程。macOS Docker Desktop 上，
宿主 loopback 代理通常应写为 `http://host.docker.internal:<port>`，不要把容器内不可达的 `127.0.0.1`
直接传入。

token 计量由 `acn_eval` 自己从上游响应的 `usage` 累计，写进 `result.json` 的 `usage`
（`model_requests` / `incomplete_model_responses` / `response_models` / `input_tokens` /
`output_tokens` / `cache_read_tokens` / `reasoning_tokens`）；宿主结果另计算 `cache_hit_rate`。
**reasoning token 计入 `max_tokens`**——不同模型的推理 token 开销不同，`max_tokens` 设小会让
模型在发出 tool call 前被截断，attempt 直接失败。
官方 `mini-swe-agent` 不设 output cap，本 runner 默认给 65536。

四臂注入同一份冻结 `assets/coding-benchmark/SKILL.md`（hash 写入 manifest），路径为
`/logs/agent/runtime/skills/coding-benchmark`。评测生成的 `acn.toml` 将
`max_parallel_tool_calls` 设为 `5`、`file_diff_max_changed_lines` 设为 `200`；
`code_run` 观察窗口仍用产品内部护栏，不开放 TOML。A-only 短评测 prompt 与提交纪律的决策记录见
[reports/a-only-minimal-harness-adjustments-20260820.md](reports/a-only-minimal-harness-adjustments-20260820.md)。
51 分轮的 PID 耗尽、持久化阻塞与修正见
[reports/a-only-minimal-timeout-and-gap-fixes-20260820.md](reports/a-only-minimal-timeout-and-gap-fixes-20260820.md)。
容器 attempt TOML 固定
`workspace_root=/app`、`runtime_root=/logs/agent/runtime`、`output_dir=/logs/agent/evaluation`、
`acn_config=/opt/acn-eval/acn.toml`；`B_claim` 和 `B_forced_claim` 设置
`claim_bundle=/opt/acn-eval/claims.json`，A / B_empty 容器中不存在该文件。前者只在模型调用
`consult_router` 时取得 claim；后者启动时通过相同 router 检索，并把返回的完整 claim 放入首轮任务上下文。

ACN 不把任意“模型不再调用工具”当作完成。evaluation profile 额外暴露无参数的 `submit_task`；模型完成
实现、测试与 diff 检查后应把它作为唯一工具调用。提交后 turn loop 不再请求模型，随后才运行 session
finalize 和 Pier verifier。若一个可消费的正常最终回复遗漏了该调用，attempt 会记录
`evaluation_completion.mode=implicit_assistant_done`，再走同一条 finalize 与 verifier 路径；截断、
异常、无可消费输出和 deadline 仍为 agent failure。事件账本区分显式与隐式完成，便于评估不同模型的
提交遵从率。这保留了与官方 mini-swe-agent sentinel 相近的明确边界，而不把 ACN 的工具面伪装成
Bash-only。

## 常用命令

```sh
sh benchmarks/deepswe/docker/build-acn-eval-amd64.sh
acn-deepswe validate-config /absolute/tasks/<task> /absolute/checked
acn-deepswe freeze-dataset /absolute/tasks /absolute/dataset.json --seed 17 --sample-size 5
acn-deepswe freeze-execution-dataset /absolute/tasks /absolute/frozen.json /absolute/normalized \
  --deepswe-checkout /absolute/DeepSWE --pier-checkout /absolute/pier --seed 17 --sample-size 30
acn-deepswe plan /absolute/dataset.json /absolute/run --seed 99
acn-deepswe append-freeze-barrier /absolute/host-events.jsonl attempt-1 barrier-1
acn-deepswe freeze-claims /absolute/host-events.jsonl attempt-1 /absolute/claims.json
acn-deepswe scan-sentinels /absolute/run --sentinel <sentinel>
```

`build-acn-eval-amd64.sh` 支持 Darwin / Linux 的 arm64 与 x86_64 宿主。脚本会读取宿主
kernel 与 CPU 架构，并从 Docker daemon 获取实际 Linux 容器架构，据此选择原生 builder；
最终产物固定为 Linux x86_64 ELF。缺少 Docker、平台不受支持或产物架构不符时会直接失败。
脚本默认从 DaoCloud 拉取 `ubuntu:24.04` 和 `debian:bookworm-slim`，校验平台后重新打回
官方 tag，供 Pier 的 Squid builder 和 ACN builder 使用。可通过
`--mirror-prefix <registry-prefix>` 或 `ACN_DOCKER_MIRROR_PREFIX` 更换镜像源；能直连
Docker Hub 时传 `--direct`。

`freeze-dataset` 默认抽取 5 题，适用于 Pre-smoke；Smoke 固定抽 30 题时传
`--sample-size 30`。抽样始终基于稳定排序、固定 seed 的无放回选择，并在执行前写入 manifest。
真实运行使用 `freeze-execution-dataset`：它在写入前确认两个 checkout 的精确 revision 与干净
工作树，批量生成 `allow_internet = false` 的任务副本，并把每题 source/normalized TOML 与目录
tree hash 一同冻结。已有 manifest 或 normalized 目录时拒绝覆盖。

`validate-config` 做 fail-closed 网络转换：只有 `agent.network_mode` 与
`verifier.network_mode` 均为 `"no-network"` 时才生成 Pier 兼容副本，并对两个环境写入
`allow_internet = false`；转换保留 DeepSWE 当前的 `[[verifier.collect]]` patch 收集钩子，源与
转换结果的 SHA-256 一并输出，未经转换的任务禁止运行。

`append-freeze-barrier` 仅在同 attempt 已记录 `attempt_finished`、无旧 barrier 且 seq 严格
递增时 fsync 追加。`freeze-claims` 输出 Rust 可读的 `{schema_version, claims}` bundle，排除
stale、deprecated、disputed、跨 attempt 与 barrier 之后的 snapshot。

## 冻结多题 pre-smoke

复制 `manifests/presmoke-run.example.json` 为本机绝对路径配置，填入已 checkout 的
DeepSWE / Pier checkout、该 checkout 的 editable `pier`、Linux `acn_eval`、冻结模型名和资源预算。**配置中不得放
credential**；它只从宿主环境读取。`frozen_skill` 必须指向含 `SKILL.md` 的完整 skill 目录。
示例刻意指向待生成的 `target/deepswe-runs/deepswe-current/`；不要复用仓库内仅作历史证据的
`presmoke-v1.json` 或 Luna cohort manifest 作为当前官方对齐 run 的输入。

先 dry-run 静态校验两份 checkout revision、source/normalized 完整 task 目录 tree hash
和全部四臂计划；二进制身份在真实启动前使用冻结任务镜像中的无网络 Linux 容器验证，
dry-run 不执行 Linux 二进制，也不调用 Docker：

```sh
ACN_EVAL_UPSTREAM_BASE_URL=<https-url> \
PYTHONPATH=benchmarks/deepswe/src python -m acn_deepswe.presmoke_cli \
  --config /absolute/path/to/presmoke-run.json --dry-run
```

真实执行时再注入 `ACN_EVAL_UPSTREAM_KEY` 并移除 `--dry-run`。不希望把 key 写进 shell 历史
或环境时用 `--read-key-stdin` 按提示隐藏输入；它只在真实执行且环境无该变量时读取，dry-run
不读，已有环境值优先，进程退出时清除。

冻结 manifest 中的每题按 producer / consumer 两波执行，波内可并行；全部题目和 arm 共用
`task_workers` 个 attempt 许可，`max_retries=0`。启动配置的
`run_all_variants_without_claims=true` 会要求每题实际执行四臂：若 A 没有 eligible claim，freeze barrier
仍产出可审计的空 bundle，两个带 claim 的 B 臂照常执行并记录 `EMPTY_CLAIM_BUNDLE`，绝不伪造或借用其他题的
claim。未开启该开关时才保留历史的“两个带 claim B 臂不适用”行为。`claim_quality_gate` 默认为
`verified_producer_only`：producer 未通过 verifier 时其 claim 全部隔离（bundle manifest 记录
`quarantined_claim_ids`），两个带 claim 的 B 臂拿到的是空 bundle；显式写 `"none"` 才会把失败 producer 的 claim
交付给 consumer，只用于隔离研究，不作为正式产品路径。基础设施或 Gate 失败会以非零退出，但不会自动重试
solve。输出 aggregate manifest 在
`output_dir/presmoke-aggregate.json`，各题 manifest、jobs 与 claim bundle 在
`output_dir/tasks/<task>/`。

真实执行在创建任何 attempt 目录前硬性检查：`pier` 必须是其 venv 的 `bin/pier`，且
该 venv 的 PEP 610 `direct_url` 必须以 editable 方式明确指向 frozen Pier checkout；两份
checkout 均须是冻结 revision 且工作树干净。随后 `pier --help` 必须成功、
Docker daemon 可用、每个 task 的镜像引用能解析为本地 content digest，且 Docker 的 `NCPU`
与 `MemTotal` 足以容纳 `task_workers * cpus` 与 `task_workers * memory_mb`。资源不足直接
失败，不静默降低并发。

preflight 通过后，runner 会把 `acn_deepswe`、Pier package、console script 与 coding skill
一次性复制到 `output_dir/frozen-python/`，四臂只从该目录 import，并在每臂前复核二进制、
skill、task 与两份 Python source tree hash。`acn_revision` 必须等于当前 ACN `HEAD`，工作树必须干净；
历史 `<HEAD>+evaluation-worktree` 标记不能用于当前启动入口。真实运行验证的是冻结副本的二进制身份。

16 GiB Mac 的 Docker VM 约 7.65 GiB，本机 pre-smoke 用 `task_workers=1`、`memory_mb=6144`、
`cpus=2`；该降配结果不能与官方 8 GiB 资源口径直接横向比较。

## 每个 attempt 的产物

位于 attempt 的 `output_path`：`host-config/`（attempt TOML、ACN config、Pier job 与 trial）、
`gate.json`、`attempt-result.json`。Pier trial 下的 `agent/evaluation/{result.json,events.jsonl}`、
`artifacts/model.patch` 与 pinned `TrialResult` 会被引用并解析。
`Task1ExecutionConfig.manifest_path` 写入实验与每个已执行 attempt 的结果/失败证据。

每个 attempt 还会在 `output_path/progress.json` 原子写入运行中观测：session 的
`turn_events.jsonl` 路径、事件数、最后活动时间和最近事件类型。它是只读监控，绝不因
`possibly_stalled` 自动终止任务；仅在连续 `stall_after_secs` 未见事件时提示人工排查。
默认每 30 秒刷新一次、10 分钟标记为疑似停滞，可通过启动配置的 `progress.poll_secs` 和
`progress.stall_after_secs` 冻结并记录。若人为中止，记录会写入
`INTERRUPTED_BY_OPERATOR`，避免把尚未落盘的最终 result 误判为模型无响应。

若 Rust `result.json` 明确标记 `failure_kind=upstream_concurrency_exhausted`（HTTP 429 且上游
给出并发容量耗尽代码），宿主将该 attempt 记录为基础设施失败，保留 Rust result、event ledger
和 `progress.json`，但不执行 Gate、freeze 或后续 B 臂。它不计入 agent、claim 或 verifier 的
得分；一般 429 若没有这个精确标记，仍按原始 agent 结果处理，避免把普通限流误归因。

Pier 固定 `force_build=false`，使用冻结 `task.toml` 指向的官方预构建镜像，避免本机联网重建
和架构漂移；同时固定 `delete=false`，使每个 trial 结束时拆掉 Compose 容器，但保留已在本地的
官方题面镜像。Pier 的 `delete=true` 会执行 `down --rmi all`，把这些镜像删掉后再次拉取，
既浪费磁盘也不再能复用预构建环境。同时固定 `n_attempts=1`、`n_concurrent_trials=1`、`max_retries=0`。

`EvaluationProvenance.model` 是发送给模型服务的请求模型名，`expected_response_model` 是
上游实际返回的 checkpoint 名，二者都写入 execution manifest。当前示例使用
`deepseek-v4-flash-local-exp` 作为请求别名、`deepseek-v4-flash` 作为冻结响应值；若预探针发现
响应 checkpoint 不同，必须以实际值更新 `response_model`，不能静默忽略别名映射。`reasoning_effort` 是必填的 ACN
推理强度配置，官方可比的 Flash 组使用 `max`（`high` 或扩展预算均须在 provenance 中明确标为
非官方对齐配置）。`resources` 必须记录 `cpus`、
`memory_mb`、`storage_mb`、`max_tokens`、`context_window`；`timeouts` 必须记录
`agent_seconds`、`deadline_reserve_seconds`，`llm_retry` 必须记录三项重试参数。
官方 task 的 agent timeout 为 5400 秒，示例为 verifier、事件和 result 写入预留 120 秒，故 ACN
工作 deadline 为 5280 秒。任何更长的 `agent_seconds` 都必须标记为扩展预算，不能与官方 90 分钟
口径直接比较。该值会同时覆盖 Pier 墙钟、ACN 请求 timeout 与 attempt deadline（扣除
`deadline_reserve_seconds`）。

`manifests/luna-smoke-v1.json` 从 DeepSWE v1.1 官方 trial artifact 中冻结
`gpt-5-6-luna / mini_swe_agent_gpt_5_6_luna_max` 的两个极端历史 cohort：5 题在四次
rollout 中 4/4 通过，5 题 0/4 通过。manifest 同时记录 artifact SHA-256、筛选口径、
历史 token/step 合计和本地 DeepSWE/Pier revision；它只用于任务抽样，真实请求使用上述冻结模型。

## Full-113 归档（2026-08-15）

本仓库随代码保留 `deepseek-v4-flash-local-exp` 的一次完整四臂运行摘要；可复现的结构化数据与
方法说明见 [reports/full-113-v4-flash-local-exp-20260815-directfull30-defaultcompact-r1.md](reports/full-113-v4-flash-local-exp-20260815-directfull30-defaultcompact-r1.md)
和同名 JSON。归档不包含 key、endpoint、容器日志或任务源码副本。

运行使用的基线与配置如下：

| 项目 | 冻结值 |
| --- | --- |
| ACN `main` 基线 | `70473898468c1cfbd036d350a70cae4de12afdce` |
| 评测工作树 | `992b039c0805b4032d33a513f6959cca703d8d1a+evaluation-worktree` |
| DeepSWE / Pier | `435ee89ec2f2e2289f33b0da4f992f0b7b7266b9` / `0daf53d3599e58c4506cf0bcff5e12c77dc282d2` |
| 请求模型 / 响应 checkpoint | `deepseek-v4-flash-local-exp` / `deepseek-v4-flash` |
| provider / 上下文压缩 | `openai_responses` / `auto_compact_ctx_ratio=0.80` |
| 推理与上下文 | `reasoning_effort=max`；`context_window=1_000_000`；`max_tokens=65_536` |
| 调度与隔离 | 113 题、四臂、`task_workers=30`；agent 与 verifier 均无网络；模型出口模式当时未冻结 |
| 单任务资源 | 2 CPU、8 GiB 内存、20 GiB 存储；agent timeout 5,400 秒，deadline reserve 120 秒 |
| 重试与监控 | LLM retry 2 次（1–30 秒退避）；Pier trial 不重试；30 秒轮询、600 秒仅告警不自动终止 |

最终计分集为 452 个 task-arm（113 题 × 4）：原始运行中 verifier 超时的两个 arm，以及缺失的一个
`B_forced_claim` arm 由同配置的 `resume-001` 补齐；其余 5 个 resume 中的重复 arm 只计入实际资源消耗，
不替换原始结果。按**当时**的 Gate 定义，452 个计分 arm 均通过；但该 run 没有把模型出口模式写入冻结
attempt 配置，不能满足本版加固后的正式隔离证据，以下保留为可审计的诊断结果而非新版严格 formal score。
verifier 成功率如下：

| Arm | 成功 / 113 | 成功率 | 平均 steps | 平均输入 token | cache 命中率 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 46 | 40.7% | 124.22 | 19.34M | 98.33% |
| B_empty | 48 | 42.5% | 123.79 | 19.02M | 98.46% |
| B_claim | 46 | 40.7% | 117.62 | 17.51M | 98.32% |
| B_forced_claim | 50 | 44.2% | 116.68 | 17.27M | 98.37% |

计分集累计输入 / 输出 / reasoning token 为 8.265B / 54.644M / 37.347M，共 54,502 次模型请求；包含
失败与全部补跑的实际消耗为 8.454B / 55.607M / 37.979M，共 55,480 次请求。四臂并非官方单 agent
score 的直接复刻，因而不得把本表与外部公布分数当作同口径排行榜比较；完整的差异分析与限制在归档报告中。

## Gate 判什么

Gate 只验证基础设施、claim 归因与隔离：artifact hash、verifier 是否真的跑过、usage 是否
完整上报、Pier task checksum/trial 隔离，以及 `B_empty` 不得见到任何 claim、带 claim 的两个 B 臂
只能使用冻结 bundle 内的 claim。producer 的有效得分决定统计分层，默认质量门控还会隔离失败 producer 的 claim；
在 `run_all_variants_without_claims=true` 的四臂模式下，没有 claim 的任务会执行四臂并显式标为
`EMPTY_CLAIM_BUNDLE`，不可与成功注入 claim 的结果混同。`presmoke-aggregate.json` 会单列 `claim_funnel`：每臂的
bundle 可用、router 检索、内容注入、模型报告使用及对应 claim 数，不能用“挂载成功”代替这些证据。
`cohort_metrics` 按 producer 结果分层；`failed_producer_quarantine` 保留被隔离题目的四臂结果，
与 `unpaired_no_claim` 一样不计算 claim 效果配对。每层给出各臂通过数、用量总和 / 均值、`empty_claim_bundle_attempts`，
以及两组同题配对：`paired_against_producer`（consumer 减 producer）和 `paired_against_no_claim_baseline`
（claim 臂减同题未拿到 claim 的非 producer 臂，含不一致配对的 `wins` / `losses` 与双侧精确二项检验
`exact_mcnemar_p`）。分母固定为冻结 task 集：`cohort_coverage` 记录 planned / included / excluded 数量与每个被排除
task 的原因，不得把缺失或失败的 task 从分母里静默删掉。attempt 级 `attempt-result.json` 的 `agent_error` 保留
Rust 侧带 `stage=` 前缀的失败摘要，用于区分 turn 阶段与 finalize 阶段的 agent 失败。

**verifier 判 0 分与 agent 自身失败都是有效实验结果，不是 Gate 失败**，按未通过计分，不得
重跑刷分。`verifier_passed` 只有 agent 正常完成且 verifier 通过才为 true；原始 patch 判卷保留在
`pier_trial` / `verifier_regrade`。checkpoint 会持久化所有 task 终态（含 Gate、协议与基础设施失败）；普通 `--resume` 遇到任何
失败终态即拒绝。只有无终态且已有半成品的中断 task，才可由操作者显式传
`--resume --retry-interrupted` 重跑一次；此前的产物和 retry 计数都会保留。
