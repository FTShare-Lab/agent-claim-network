# ACN 解题对齐全量运行

该轮使用正式 ACN harness：开启非交互文件修改权限、关闭 Memory 相关运行时路径，并在所有 ACN arms 使用相同的解题流程、非结束轮工具调用纪律和对齐的采样参数。它仍是 ACN 四臂实验，不能与单 agent 的外部得分直接并列。

运行时生成的 ACN TOML 固定为：

- `agent.tool.file_edit_authority_enabled=true`；
- `agent.memory.enabled=false`；
- `agent.session.memory_review.enabled=false`；
- `agent.llm.temperature=1.0`；
- `agent.llm.top_p=0.95`。

请求模型保持 `deepseek-v4-flash-local-exp`，预期响应 checkpoint 保持 `deepseek-v4-flash`，provider 保持 `openai_responses`，`reasoning_effort=max`，上下文能力声明为 1,000,000，输出上限为 65,536。模型凭据和 base URL 仅从受保护的运行环境读取，不能放入本文件、JSON 配置或命令行。

## 直接全量

将 [automated-run-solver-aligned-full.example.json](manifests/automated-run-solver-aligned-full.example.json) 复制到仓库外的私有位置，替换其中所有本机路径。该 profile 固定 v0.2.5 产品基线、`smoke_size=0`、`full_size=113`、`task_workers=20` 和每题四臂，因此计划总数为 452 个 task-arm，而不是重复执行四轮独立实验。

模型就绪后，先完成不请求模型的准备，再由常驻会话启动：

```sh
source export_env.sh
python -m acn_deepswe.auto_run --config /absolute/path/to/automated-run.json prepare
python -m acn_deepswe.auto_run --config /absolute/path/to/automated-run.json run --read-key-stdin
```

`prepare` 会验证冻结环境、Docker 容量与四臂计划。`run --read-key-stdin` 只在终端隐藏读取凭据，不把凭据写入 artifact；如受保护环境已注入对应变量，也可省略该参数。运行后的只读监控命令为：

```sh
source export_env.sh
python -m acn_deepswe.auto_run --config /absolute/path/to/automated-run.json monitor
```

除已确认的无终态中断外，不通过普通 `--resume` 重跑 task；需要恢复时使用既有的 `--resume-interrupted` 受限路径，以保留原始 attempt 和归档证据。

正式运行只使用冻结任务声明的官方镜像，不接受 `hb__` 本地重定向。trial 结束拆掉 Compose 容器但保留官方镜像供后续臂复用；运行前定向清除遗留 Pier 容器和生成镜像，并以固定余量加每 worker admission 的方式检查输出盘和 Docker 数据盘。
