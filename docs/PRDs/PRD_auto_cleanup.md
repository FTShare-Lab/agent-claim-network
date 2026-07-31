# PRD: Agent Session Auto Cleanup

> 状态：已实现。本文保留 session 自动清理的产品决策与验收边界。

## 背景

ACN agent home 下的 session 目录会持续增长，长期运行后会造成磁盘占用、`/resume` 列表噪音和 session_search 派生索引残留。需要清理长期没有对话的旧 session，并同步清理 SQLite search index 中对应的派生数据。

清理不放在 finalize/退出链路，也不阻塞启动；它通过后台 housekeeping 延迟执行，并在用户活跃时继续延后。为保证会话数据安全，自动清理只处理 `Closed` session。

## 目标

- 自动清理近 30 天内没有对话过、且状态为 `Closed` 的旧 session。
- 同步清理 `session_search_index.sqlite` 中该 session 的 `sessions`、`messages`、`indexed_sessions` 和 FTS 表数据。
- 提供手动入口 `acn session cleanup`。
- 自动清理不阻塞 TUI 启动，不属于 supervisor finalize 职责。
- 清理逻辑宁可漏删，不误删当前或不可确认状态的 session。

## 非目标

- 不清理 `Open` / `Finalizing` session。
- 不删除 metadata 本身不可读、无法确认为 `Closed` 的 session。
- 不新增 `--include-corrupt` 之类危险开关。
- 不改变 `messages.jsonl`、`turn_events.jsonl`、session_search canonical 语义。
- 不让 supervisor finalize job 负责清理。

## 拍板决策

### 保留期与配置

- 新增配置项：`agent.session.cleanup_retention_days`（TOML 中为 `[agent.session] cleanup_retention_days`），默认 30。
- `0` 表示禁用自动后台清理。
- 手动命令不受该值为 0 的影响；手动命令可显式传参时按命令参数执行，暂时可只使用配置默认值。

### 自动触发

- TUI 启动后注册后台 housekeeping。
- 后台清理是 best-effort，不保证每次启动都完成。
- 后台清理先延迟一段时间，不阻塞启动和首轮交互。
- 如果用户近期有交互或 turn 正在运行，继续延后。
- 如果 TUI 在等待期间退出，清理不会继续执行，也不会写 marker。
- 只有真正完成一次自动清理后，才写 marker。
- marker 用于限制自动清理频率，24 小时内最多自动跑一次。
- 手动 `acn session cleanup` 不受 marker 限制。

### 清理条件

一个 session 只有同时满足以下条件才可删除：

- session 属于当前 agent。
- metadata 可读。
- metadata 状态为 `Closed`。
- 距离最后一次对话超过 retention cutoff。

“最后一次对话”判断：

- 优先读取 canonical `messages.jsonl` 最后一条消息的 `created_at`。
- 如果 `messages.jsonl` 读失败或没有可用时间，但 metadata 可读且状态为 `Closed`，则 fallback 到 session 目录 mtime。
- 空 `Closed` session 也允许按 fallback 时间清理。
- metadata 不可读时只记录 skipped/warn，不删除。

### 删除顺序与并发

- 自动清理获取 agent home 级 cleanup 文件锁，例如 `.session-cleanup.lock`。
- 拿不到锁时直接跳过本次自动清理。
- 每个候选 session 删除前二次读取 metadata，确认仍为 `Closed`。
- 删除顺序采用：先删除 session 目录，再清理 SQLite 派生索引。
- SQLite purge 失败不回滚文件删除；记录 warn/errors，下次 cleanup 或 repair 补齐。
- SQLite 写入复用现有 `BEGIN IMMEDIATE` + busy_timeout + retry 逻辑，不新增单独 SQLite 文件锁。

### 手动命令

- 命令：`acn session cleanup`。
- 默认是 dry-run。
- 默认输出必须提示：`This is a dry run. Use --apply to delete.`
- `acn session cleanup --apply` 才真正删除。
- 手动命令不受 marker 限制。
- 输出包含 scanned / eligible / deleted / skipped / sqlite_purged / errors，并列出 session id。
- 列表和统计需要列对齐、缩进稳定，便于终端阅读。

## 文件与模块规划

- `src/session/cleanup.rs`
  - 负责扫描 session 目录、判断候选、删除目录、汇总 report。
  - 只依赖 session 权威文件和 storage lock。

- `src/session_search/index.rs`
  - 暴露 `purge_session_from_index(...)` 或等价 API。
  - 内部复用 `delete_session_index_rows` 和 `run_immediate_transaction`。

- `src/storage/paths.rs`
  - 增加 cleanup lock / marker 路径 helper。

- `src/config.rs`
  - 增加 `agent.session.cleanup_retention_days` 配置。
  - 校验非负；`0` 为禁用自动清理。

- `src/bin/acn.rs`
  - 增加 `acn session cleanup [--apply]`。
  - 默认 dry-run，打印提示。

- TUI/bootstrap 入口
  - TUI 启动后后台注册 cleanup task。
  - 根据 marker、retention、用户活跃/turn 状态延迟执行。

## 分阶段 Todo 与验收

### Phase 0: PRD 固化

Todo:

- 写入本 PRD。
- 确认拍板内容和当前文档无冲突。

验收:

- `docs/PRDs/PRD_auto_cleanup.md` 存在。
- 文档明确包含：只删 Closed、默认 dry-run、marker 完成后写入、TUI 退出不继续清理、metadata 不可读不删、无危险开关。

### Phase 1: 清理核心与 SQLite purge

进入本阶段前重新读取本 PRD。

Todo:

- 新增 session cleanup 核心类型和 report。
- 实现候选判断：metadata 可读、agent 匹配、Closed、最后对话时间过期。
- metadata 可读但 messages 坏/空时 fallback 到目录 mtime。
- metadata 不可读时 skipped，不删除。
- 删除前二次确认 metadata 仍为 Closed。
- 暴露 session_search purge API，事务内删除普通表和 FTS 表。
- 添加 cleanup lock/marker path helper。

验收:

- 单元测试覆盖 Closed old 删除、Open/Finalizing 跳过、metadata 不可读跳过、messages 坏时按 mtime fallback、删除前状态变化跳过。
- 单元测试覆盖 sqlite purge 删除 `sessions/messages/indexed_sessions/messages_fts/messages_fts_trigram`。

### Phase 2: CLI 手动入口

进入本阶段前重新读取本 PRD。

Todo:

- 增加 `acn session cleanup`。
- 默认 dry-run，不删除，输出提示。
- `--apply` 执行删除。
- 输出列对齐：统计和 session 列表稳定缩进。
- 手动命令不检查/写入 marker。

验收:

- CLI dry-run 测试确认目录未删除且有 dry-run 提示。
- CLI `--apply` 测试确认 eligible session 被删除且 sqlite purge 被调用。
- 输出 snapshot/字符串测试覆盖列对齐。

### Phase 3: TUI 后台自动清理

进入本阶段前重新读取本 PRD。

Todo:

- TUI 启动后注册后台 cleanup task。
- 自动清理受 `agent.session.cleanup_retention_days` 控制，0 禁用。
- 24 小时 marker 限流。
- 清理执行前延迟；用户活跃或 turn 运行时继续延迟。
- 清理成功完成后写 marker。
- 失败只 log warn，不打断 TUI。

验收:

- 单元测试覆盖 retention=0 禁用。
- 单元测试覆盖 marker 24h 内跳过。
- 单元测试覆盖等待期间不写 marker。
- TUI smoke test 确认启动不被 cleanup 阻塞。

### Phase 4: 总体验证与 code-review skill

进入本阶段前重新读取本 PRD。

Todo:

- `cargo fmt`。
- `cargo clippy -- -D warnings`。
- `cargo test`。
- `cargo check`。
- TUI smoke test。
- 使用 code-review skill 检查：
  - session cleanup 核心与 sqlite purge。
  - CLI 和配置。
  - TUI 后台触发。

验收:

- 所有验证通过。
- code-review skill 无未处理的高风险问题。
- 最终实现完整对齐本 PRD。
