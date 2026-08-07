# ACN DeepSWE runner

在 pinned Pier 0.3 上跑 ACN 的 DeepSWE 三臂实验：每题 `A`（产 claim）→ freeze barrier →
`B_empty` / `B_claim`，主指标是 `pass(B_claim) - pass(B_empty)`。

## 与官方口径的关系

模型访问完全沿用 Pier 官方 adapter（`mini_swe_agent` / `codex` / `opencode`）的做法，不自建
代理或 broker：

- 模型 key 从宿主环境变量 `ACN_EVAL_UPSTREAM_KEY` 读取，仅以容器变量
  `ACN_EVAL_MODEL_KEY` 交给 `acn_eval`；进程启动后通过匿名 pipe 原位 re-exec 清除初始环境，
  不进 argv、配置文件、manifest 或 JSONL；
- 出网由 Pier 自己的 Squid 域名 allowlist 限死，只允许 `upstream_base_url` 的主机名，其余
  一律拒绝；agent 与 verifier 的 `allow_internet` 均为 `false`；
- `code_run` 子进程会剥掉 `api_key_env` 指定的变量，题目工作区里的命令看不到 key。

token 计量由 `acn_eval` 自己从上游响应的 `usage` 累计，写进 `result.json` 的 `usage`
（`model_requests` / `incomplete_model_responses` / `response_models` / `input_tokens` /
`output_tokens` / `cache_read_tokens` / `reasoning_tokens`）；宿主结果另计算 `cache_hit_rate`。
**reasoning token 计入 `max_tokens`**——不同模型的推理 token 开销不同，`max_tokens` 设小会让
模型在发出 tool call 前被截断，attempt 直接失败。
官方 `mini-swe-agent` 不设 output cap，本 runner 默认给 65536。

三臂注入同一份冻结 `assets/coding-benchmark/SKILL.md`（hash 写入 manifest），路径为
`/logs/agent/runtime/skills/coding-benchmark`。容器 attempt TOML 固定
`workspace_root=/app`、`runtime_root=/logs/agent/runtime`、`output_dir=/logs/agent/evaluation`、
`acn_config=/opt/acn-eval/acn.toml`；仅 `B_claim` 设置 `claim_bundle=/opt/acn-eval/claims.json`，
A / B_empty 容器中不存在该文件。

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
`allow_internet = false`；源与转换结果的 SHA-256 一并输出，未经转换的任务禁止运行。

`append-freeze-barrier` 仅在同 attempt 已记录 `attempt_finished`、无旧 barrier 且 seq 严格
递增时 fsync 追加。`freeze-claims` 输出 Rust 可读的 `{schema_version, claims}` bundle，排除
stale、deprecated、disputed、跨 attempt 与 barrier 之后的 snapshot。

## 冻结多题 pre-smoke

复制 `manifests/presmoke-run.example.json` 为本机绝对路径配置，填入已 checkout 的
DeepSWE / Pier、pinned `pier`、Linux `acn_eval`、冻结模型名和资源预算。**配置中不得放
credential**；它只从宿主环境读取。`frozen_skill` 必须指向含 `SKILL.md` 的完整 skill 目录。

先 dry-run 校验环境、两份 checkout revision、source/normalized 完整 task 目录 tree hash
和全部三臂计划：

```sh
ACN_EVAL_UPSTREAM_BASE_URL=<https-url> \
PYTHONPATH=benchmarks/deepswe/src python -m acn_deepswe.presmoke_cli \
  --config /absolute/path/to/presmoke-run.json --dry-run
```

真实执行时再注入 `ACN_EVAL_UPSTREAM_KEY` 并移除 `--dry-run`。不希望把 key 写进 shell 历史
或环境时用 `--read-key-stdin` 按提示隐藏输入；它只在真实执行且环境无该变量时读取，dry-run
不读，已有环境值优先，进程退出时清除。

冻结 manifest 的首题 `A → Gate → B_empty/B_claim` 完全通过后才继续。当前配置使用
`task_workers=1`，各题及每题内部的 `A → freeze → B_empty/B_claim` 都串行，
`max_retries=0`。任一失败以非零退出，不会自动重试 solve。输出 aggregate manifest 在
`output_dir/presmoke-aggregate.json`，各题 manifest、jobs 与 claim bundle 在
`output_dir/tasks/<task>/`。

真实执行在创建任何 attempt 目录前硬性检查：pinned `pier` 必须是其 venv 的 `bin/pier`，且
该 venv 的 `datacurve-pier==0.3.0` PEP 610 `direct_url` 明确指向 frozen Pier checkout；两份
checkout 均须是冻结 revision 且工作树干净。随后 `pier --help` 必须成功、
Docker daemon 可用、每个 task 的镜像引用能解析为本地 content digest，且 Docker 的 `NCPU`
与 `MemTotal` 足以容纳 `task_workers * cpus` 与 `task_workers * memory_mb`。资源不足直接
失败，不静默降低并发。

preflight 通过后，runner 会把 `acn_deepswe`、Pier package、console script 与 coding skill
一次性复制到 `output_dir/frozen-python/`，三臂只从该目录 import，并在每臂前复核二进制、
skill、task 与两份 Python source tree hash。`acn_revision` 必须等于当前 ACN `HEAD`；工作树
有改动时必须写成 `<HEAD>+evaluation-worktree`，具体内容仍由 staged tree hash 唯一标识。

16 GiB Mac 的 Docker VM 约 7.65 GiB，本机 pre-smoke 用 `task_workers=1`、`memory_mb=6144`、
`cpus=2`；该降配结果不能与官方 8 GiB 资源口径直接横向比较。

## 每个 attempt 的产物

位于 attempt 的 `output_path`：`host-config/`（attempt TOML、ACN config、Pier job 与 trial）、
`gate.json`、`attempt-result.json`。Pier trial 下的 `agent/evaluation/{result.json,events.jsonl}`、
`artifacts/model.patch` 与 pinned `TrialResult` 会被引用并解析。
`Task1ExecutionConfig.manifest_path` 写入实验与每个已执行 attempt 的结果/失败证据。

Pier 固定 `force_build=false`，使用冻结 `task.toml` 指向的官方预构建镜像，避免本机联网重建
和架构漂移；同时固定 `n_attempts=1`、`n_concurrent_trials=1`、`max_retries=0`。

`EvaluationProvenance.model` 是发送给模型服务的请求模型名，`expected_response_model` 是
上游实际返回的 checkpoint 名，二者都写入 execution manifest。当前示例使用
`deepseek-v4-flash-local` 作为两者的冻结值；若预探针发现响应 checkpoint 不同，必须以实际值
更新 `response_model`，不能静默忽略别名映射。`reasoning_effort` 是必填的 ACN
推理强度配置，示例使用 `high`。`resources` 必须记录 `cpus`、
`memory_mb`、`storage_mb`、`max_tokens`、`context_window`；`timeouts` 必须记录
`agent_seconds`、`deadline_reserve_seconds`，`llm_retry` 必须记录三项重试参数。
官方 task 的 agent timeout 为 5400 秒；若本地诊断需要把 `agent_seconds` 提高到 7200 秒，结果必须
标记为扩展预算，不能与官方 90 分钟口径直接比较。该值会同时覆盖 Pier 墙钟、ACN 请求 timeout 与
attempt deadline（扣除 `deadline_reserve_seconds`）。

`manifests/luna-smoke-v1.json` 从 DeepSWE v1.1 官方 trial artifact 中冻结
`gpt-5-6-luna / mini_swe_agent_gpt_5_6_luna_max` 的两个极端历史 cohort：5 题在四次
rollout 中 4/4 通过，5 题 0/4 通过。manifest 同时记录 artifact SHA-256、筛选口径、
历史 token/step 合计和本地 DeepSWE/Pier revision；它只用于任务抽样，真实请求使用上述冻结模型。

## Gate 判什么

Gate 只验证基础设施、claim 归因与隔离：artifact hash、verifier 是否真的跑过、usage 是否
完整上报、Pier task checksum/trial 隔离，以及 `B_empty` 不得见到任何 claim、`B_claim`
只能使用冻结 bundle 内的 claim。首题 hard gate 额外要求 `B_claim` 确实检索并注入 claim；
后续题即使模型未检索，也保留在原实验组。

**verifier 判 0 分与 agent 自身失败都是有效实验结果，不是 Gate 失败**，按未通过计分，不得
重跑刷分。只有 runner、容器或网络故障才允许原配置重试一次，并保留失败 attempt。
