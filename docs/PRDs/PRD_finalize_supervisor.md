# PRD: Finalize Supervisor

> 状态：已实现。本文保留后台 finalize、checkpoint、IPC 与通知语义。

## 背景

当前 TUI 在 `/exit` 或 Ctrl+C 退出时，会在前台 TUI 进程内执行session finalize。finalize 可能触发 LLM recap、claim/trace/dispute 落盘、maintainer upload，以及 session metadata 更新。用户已经表达退出意图后，终端仍会被 `finalizing` 占用，体验不够好。

本任务引入一个用户无感的轻量 supervisor：TUI 负责 enqueue finalize job 后尽快退出并释放终端，finalize 在后台 supervisor 中完成。

## 已拍板决策

1. 使用自写轻量 supervisor，不依赖 tmux，不做 OS daemon/service。
2. supervisor 用户无感、按需启动、空闲退出；`acn supervisor run` 仅作为内部命令。
3. 交互中的 TUI 仍保持当前单进程模型，不切到 streaming HTTP session manager。
4. v1 只支持 macOS；后续再扩展 Linux/Windows。
5. IPC 使用 Unix Domain Socket + JSON line 协议。
6. v1 supervisor job scope 只做 session finalize。
7. 不新增独立 maintainer upload retry job；现有 foreground opportunistic retry 保持不变。
8. `maintainer_uploads/pending.yaml` 必须增加跨进程文件锁。
9. 新增 `SessionStatus::Finalizing`；enqueue 成功后立即进入该状态，排队中也算 finalizing。
10. finalize 必须幂等：后台进程崩溃后重试不能再次调用 LLM 生成不同 claim。
11. executor 初版全局串行，job metadata 保留 agent/session/upstream 维度，方便后续扩展。
12. 通知内容参考当前 TUI finalize 完成时打印的 trace/claim/dispute 摘要，改写为系统通知文案。
13. supervisor idle timeout 不进 `config.toml`，但必须集中放在 `src/config.rs` 的代码常量中。
14. 同一 agent 运行目录只允许一个活动 supervisor。构建或运行环境指纹变化时安全接管旧实例；运行环境指纹覆盖有效配置、upstream 和 finalize 所需凭据摘要，不包含工具工作区或 Web 工具凭据。
15. 接管后由新运行环境继续 `queued` 和中断的 `running` job；中断的 `running` job 保留已计入的执行次数，终态 job 不自动重跑。
16. TUI 启动阶段负责运行环境接管；退出 enqueue 优先投递给当前健康 supervisor，防止较早启动的 TUI 用缓存指纹反向接管新实例。
17. 一个 session 在持久化队列中至多有一个 finalize job；重复 enqueue 返回同一个非失败 job，发现多个 job 时按不变量损坏拒绝处理。
18. `acn supervisor retry <session_id|job_id>` 只重试唯一的失败 job：复用原 job，保留失败记录并将 attempts 清零，记录 `manual_retries`，获得新的完整自动尝试预算。`queued`、`running`、`succeeded` 均拒绝 retry。
19. session ID 是 retry 的首选入口；当 `Finalizing` session 因崩溃没有任何 job 时，session ID retry 创建其首个恢复 job。job ID 只精确定位已有 job。两种入口解析成功后的输出均包含 session ID 和 job ID。
20. retry 使用本次命令的 `--config` / `--upstream` 并先确保对应指纹的 supervisor 已接管；`--cd` 仅属于交互式 TUI，所有 supervisor 命令均不支持。

## 用户体验

成功 enqueue 后：

- TUI 显示最终状态后退出，终端立即可用。
- session 在本地状态中显示为 `Finalizing`。
- finalize 成功后发送 macOS 通知，并写 job/session 日志。
- finalize 失败后发送 macOS 通知，并保留失败状态/日志，便于后续 retry 或排查。

enqueue 失败时：

- fallback 到现有同步 finalize 流程。
- 用户体验退回旧行为，但 finalize 语义不丢。

supervisor 启动失败时：

- 不阻塞 TUI 启动。
- `/exit` 时再次尝试；仍失败则 fallback 同步 finalize。

## 分阶段计划

### Phase 0: 文档和边界确认

- [x] 写入本 PRD。
- [x] 梳理 TUI `/exit`、session finalize、session metadata、maintainer upload 现状。
- [x] 确认 job scope：v1 只做 finalize。

验收：

- [x] 文档明确生命周期、job scope、fallback、幂等、通知和验证流程。

### Phase 1: Session 状态和 finalize checkpoint

- [x] 新增 `SessionStatus::Finalizing`。
- [x] enqueue finalize 成功后将 session 标记为 `Finalizing`。
- [x] `Finalizing` session 不允许 resume/继续 turn。
- [x] 新增 `finalize_checkpoint.yaml` 路径和读写 API。
- [x] finalize LLM 生成完成后保存完整 checkpoint：
  - used claim ids
  - prepared claims
  - prepared disputes
  - trace id
  - trace created_at
  - recap range/hash 或可校验的等价信息
- [x] retry 时如果 checkpoint 存在，必须直接 apply checkpoint，不再调用 LLM。

验收：

- [x] 进程在 checkpoint 写入后崩溃，再次执行 finalize 不产生新一批 claim id。
- [x] `Finalizing` 状态的 session 不会被当作可继续会话。

### Phase 2: Supervisor IPC 和 job queue

- [x] 新增 supervisor 模块。
- [x] 定义 UDS socket 路径、pid/lock、job 存储路径。
- [x] 定义 JSONL IPC：
  - `ping`
  - `enqueue_finalize`
  - 可选 `status`
- [x] 实现 `acn supervisor run` 内部命令。
- [x] 实现 TUI/CLI 侧 `ensure_supervisor_running()`。
- [x] job 持久化到磁盘，supervisor 启动时恢复 queued/running stale job。
- [x] executor 初版全局串行。
- [x] idle timeout 使用 `src/config.rs` 常量。

验收：

- [x] 没有 supervisor 时，TUI 能自动拉起。
- [x] 已有 supervisor 时，只连接不重复启动。
- [x] supervisor 崩溃后，下一次 ensure 可以清理 stale socket 并重启。
- [x] queued/running stale finalize job 可恢复执行。
- [x] 构建或运行环境变化后安全接管旧 supervisor，且不阻塞 TUI 启动。
- [x] enqueue 强制一个 session 对应一个 finalize job，并能报告既有重复记录。
- [x] 失败 job 可按 session ID 或 job ID 手动 retry；孤儿 `Finalizing` session 可按 session ID 恢复。

### Phase 3: TUI exit 接入和通知

- [x] `/exit` / Ctrl+C 改为 enqueue finalize。
- [x] enqueue 成功后退出 TUI，不等待 finalize 完成。
- [x] enqueue 失败时 fallback 同步 finalize。
- [x] 通知成功/失败结果：
  - 成功：session id、trace id、new claims、new disputes。
  - 失败：session id、简短错误。
- [x] 通知失败只记录日志，不影响 finalize。

验收：

- [x] `/exit` 后终端快速释放。
- [x] 成功/失败通知文案包含足够定位信息。
- [x] fallback 路径仍能完成 finalize。

### Phase 4: Maintainer upload 文件锁

- [x] 为 `maintainer_uploads/pending.yaml` 增加跨进程文件锁。
- [x] 所有 `upload_maintainer_batch()` 调用复用同一锁。
- [x] 锁文件路径集中由 path helper 生成，避免字符串拼路径。
- [x] 保持现有 pending merge/clear 语义。

验收：

- [x] 两个进程并发上传不会覆盖 pending。
- [x] retryable failure 仍保留 pending。
- [x] 成功后 pending 清除。

### Phase 5: 测试和验证

- [x] 为 checkpoint 恢复、job queue、IPC message parse、文件锁路径补单元测试。
- [x] 为 TUI exit enqueue/fallback 的可测试部分补测试。
- [x] 运行 targeted tests。
- [x] 运行 `/verify` skill 要求的完整流程：
  - `cargo clippy -- -D warnings`
  - `cargo test`
  - `cargo check`
  - TUI smoke test

验收：

- [x] 所有新增和现有测试通过。
- [x] clippy 无 warning。
- [x] TUI smoke test 可启动。

### Phase 6: code-review skill 验证

完成整体实现和基础验证后，按 code-review skill 检查以下风险域：

- [x] 覆盖：
  - supervisor/IPC/job queue
  - session finalize/checkpoint/status
  - maintainer upload 文件锁
  - TUI exit/通知
- [x] code-review skill 检查完成，无未处理的高风险问题。

验收：

- [x] 最终实现与本 PRD 一致，相关自动化验证与 TUI smoke test 通过。
