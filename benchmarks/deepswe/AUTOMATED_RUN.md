# 自动化 Smoke 或直接全量

`acn-deepswe-auto` 由外部调度器启动，不会由监控方启动任何评测。它先冻结全部 113 题；当 `smoke_size` 大于 0 时，从中确定性抽取 Smoke，只有 Smoke 全部完成且没有 runner 或 Gate 失败时，才以相同的模型、资源、skill 和重试配置补跑其余任务。设为 `0` 时不创建或运行 Smoke，直接执行完整冻结任务集。两种方式都不会重复执行同一题。示例开启 `run_all_variants_without_claims`，因此每题均实际执行四臂；A 未生成 claim 时，claim 臂使用同题 freeze 后的空 bundle，并留下可汇总的空 bundle 标记。

复制 [automated-run.example.json](manifests/automated-run.example.json) 到仓库外的绝对路径，并填写本机 checkout、`acn_eval` 和 run root。配置中禁止 key；外部启动器必须沿用已有的 `ACN_EVAL_UPSTREAM_KEY`，同时提供 `ACN_EVAL_UPSTREAM_BASE_URL`。`model` 是路由别名，`response_model` 必须填写上游实际回显的 checkpoint；当前模板分别为 `deepseek-v4-flash-local-exp` 和 `deepseek-v4-flash`。

正式模板锚定产品提交 `9b818d70ddfad2f7d5e1972577dd294b19481c92`（v0.2.5），只接受基于该提交的干净评测 HEAD；`acn_eval --build-info-json` 还必须与 HEAD 和 v0.2.5 一致。20 个 `task_workers` 配合每题 2 CPU / 16 GiB，另预留 64 GiB 宿主内存；真实执行前会要求至少 40 CPU 和 384 GiB 可用内存。磁盘门禁为 64 GiB 固定余量加每 worker 4 GiB admission，即至少 144 GiB 可用空间。资源不足会直接退出，不会降低并发或启动部分任务。

正式运行持有全机 Docker 锁，拒绝与其他容器任务共享 daemon。模板开启的清理只删除已停止且带 Pier Compose 配置证据的容器，以及 `hb__`、Pier verifier/egress 和 Compose 生成镜像；官方 `public.ecr.*` 任务镜像与无关镜像不在删除集合。任务启动前及缺失镜像拉取后都会复查磁盘高水位。

外部启动器使用以下命令；先执行 `prepare` 只做冻结与配置生成，不请求模型，随后由调度器执行 `run`：

```sh
source export_env.sh
python -m acn_deepswe.auto_run --config /absolute/path/to/automated-run.json prepare
python -m acn_deepswe.auto_run --config /absolute/path/to/automated-run.json run
```

若宿主未设置模型 key，可将最后一条命令改为 `run --read-key-stdin`；自动化父进程只会在终端隐藏读取一次，并仅在自身内存中继承给所运行阶段，结束即清除。该值不会写入运行配置、manifest 或日志。若要跳过 Smoke，设置 `"smoke_size": 0`、`"full_size": 113`；`run` 会直接进入 `full` 阶段。

需要先评估 A、满意后再补三条 B 臂时，使用两个独立 `run_root`。第一阶段设置
`"run_a_only": true`、`"smoke_size": 0`；每题执行 A、写入 freeze barrier 和 claim bundle，三条 B
臂以 `A_ONLY` 终态留档。第二阶段保持相同的任务 seed、plan seed、模型、effort、资源、超时、镜像、
二进制与源码，设置 `"run_a_only": false`、`"run_all_variants_without_claims": true`，并增加：

```json
"b_only_from_a_output_dir": "/absolute/path/to/a-run/full/output"
```

第二阶段只调度 `B_empty`、`B_claim`、`B_forced_claim`，不会重跑 A。真实执行前会先校验全部 task 的
A-only manifest、Gate、freeze barrier、claim bundle、task checksum 与公平性 provenance；任一 task
缺失、被修改或配置漂移时，整批 B 在创建 attempt 目录前失败。B manifest 同时保留来源 A 的证据路径和
source manifest hash。B-only 必须设置 `smoke_size=0`，且不能与 `run_a_only` 同时启用。

`run` 会继承原有 key，但不打印、不写入配置、manifest 或命令行。同一 `run_root` 有跨进程锁，第二个
自动编排器会被拒绝，不能并行覆盖 checkpoint。若进程中断，普通 `run` 只报告阶段需要人工确认，不会自动
续跑。操作者确认是无终态的中断后，显式传 `run --resume-interrupted`，它才向阶段传递
`--resume --retry-interrupted`；每个 task 只允许一次。`task-completions.json` 会持久化所有 task
终态，Gate / 协议 / 基础设施失败均不可由该路径重跑。四臂完整且 Gate 通过的 task 会复用；授权的中断
task 在 `output/resumes/resume-XXX/` 重新执行，旧半成品不会被覆盖。有 Smoke 的配置在 Smoke 完整后才
启动后续任务。

监控端只运行以下只读命令。它汇总两个阶段、所有 `progress.json` 状态、疑似停滞条目及过期的 active 快照；后者表示运行进程可能已退出，不能将历史 `active` 当作仍在运行。该命令不创建、启动、终止或重试任务：

```sh
source export_env.sh
python -m acn_deepswe.auto_run --config /absolute/path/to/automated-run.json monitor
```
