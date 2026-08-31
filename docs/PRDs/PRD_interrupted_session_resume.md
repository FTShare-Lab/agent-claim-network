# PRD: 异常退出 Session 恢复

> 状态：已实现（2026-08-28）。

> 后续范围说明（2026-08-31）：本文当时明确保留的“非空会话内 `/resume` 不支持、`/new` 不实现”限制，后续由 `docs/PRDs/PRD_in_session_new_resume.md` 替代。本文定义的异常 Open/Closed/Finalizing 恢复、候选筛选与 runtime lease 语义继续有效；本说明只记录后续需求覆盖关系，不改写本文实施时的历史拍板。

## 目标

允许用户通过启动参数 `acn --resume`、`acn --resume <session_id>`，以及空白启动 session 中的 `/resume`，继续因断电、关机或进程异常退出而仍为 `Open` 的 session，同时避免两个进程并发写同一 session。

## 产品语义

- 持久状态仍只有 `Open`、`Finalizing`、`Closed`；不新增 `Interrupted` 状态。
- Resume 列表中的 `Interrupted` 是派生展示：metadata 为一致的 `Open`，且 `runtime.lock` 当前可获取。
- 候选 session 必须属于当前 agent，包含 canonical 真实用户消息或 journal 已接受的真实用户输入，状态为 `Open` / `Closed`，且未被其他运行期占用。
- `Open` 异常 session 直接续接，保持 `Open`，不经过 finalize，不调用 `mark_open`，不删除 recap/finalize checkpoint。
- `Closed` session 沿用既有 reopen 行为。
- `Finalizing` session 不可 resume：queued/running 等待 supervisor；failed/orphaned 使用 `acn supervisor retry <session_id>`。不增加 `--force`。
- Resume 只恢复 TUI/journal 最后现场；不自动重跑模型、fallback 或工具，不补造 tool result，也不增加 pending tool 警告。下一次用户请求继续使用既有 provider-safe recovery projection。
- 会话内非空 session 的 `/resume` 限制保持不变；本期不实现会话内切换，也不实现 `/new`。

## 并发边界

每个前台 session 从创建或 resume 成功起持续持有独立的 `runtime.lock`，直到 TUI 退出或进程终止。锁文件是否存在不代表占用，只有 OS 文件锁状态有效。

会写当前 session 的 turn、shell、compact、inbox 与前台 finalize worker 共享同一租约；由 turn 登记后独立运行的 subagent 也继续持有该租约。即使 TUI 因终端 I/O/绘制错误先退出，锁也要由最后一个 detached writer 收束后再释放，不能让新进程与旧 worker 并发写入。

Resume 列表的锁检查只是快照。用户选中或 direct resume 时必须重新非阻塞获取 `runtime.lock`，持续持有锁，再读取 `session.yaml` 决定 `Open` / `Closed` 路径；抢锁失败则明确拒绝 session 已被其他进程打开。`session.lock` 继续只保护短期 metadata/messages 写入，`finalize.lock` 继续只保护 recap/finalize，本需求不改变 supervisor 流程。

## 验收

- 未占用且有真实输入的 `Open` session 出现在 picker，并显示 `Interrupted`。
- 活跃进程持锁时该 session 不出现；列表后发生竞争时只有一个进程能成功 resume。
- direct resume 接受一致的 `Open` / `Closed`，拒绝 wrong-agent、`Finalizing`、无效 Open metadata 和锁占用。
- crash-open 恢复保留 journal、未完成工具现场和共享 checkpoint；下一请求不由 resume 自动发起。
- `Closed` reopen、finalize supervisor retry、空 session 筛选和会话内 `/resume` 限制不回归。
