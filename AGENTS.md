# AGENTS.md

本文档为 Codex 在本仓库中工作时提供指引。整体使用中文与用户交互。

## 项目概览

Agent Claim Network（ACN）是 Rust 实现的通用领域协作型助手，主要通过终端 TUI 与用户交互。`acn` 可独立运行，也可连接 Router 与 Maintainer，共享 claim、接收 policy、上报 dispute 并维护团队知识。

以当前代码与现行文档为准，不以历史设计稿或已完成 PRD 的实施清单为准。按任务查阅：

- `README.md` / `docs/user_guide.md`
- `docs/architecture.md`
- `docs/core_behavior.md`
- `docs/memory_design.md`
- `docs/config_parameters.md`
- `docs/PRDs/`

需求或行为有歧义时先向用户确认，不替业务方拍板。

## 当前架构

模块细节见 `docs/architecture.md` 与 `src/`；改动时守住所有权即可：

- Agent（`src/agent/`、`src/session/`、`src/memory.rs`、`src/session_tui/`、`src/tool/`）：本地 session、Memory、TUI 与工具执行；inbox 内化后才落本地变更。
- API（`src/api/`）：provider-neutral turn loop 与各模型 adapter。
- Router（`src/router/`）：可引用的 claim / dispute 检索与派生视图，不拥有 agent 私有 Memory。
- Maintainer（`src/maintainer/`）：policy、dispute、stale sweep、outbox；对 claim 属性的更新是建议而非强制。
- Claim DTO（`src/claim/`）与装配入口（`src/bin/acn.rs`、`src/bootstrap.rs`）。

默认 ACN base 目录是 `~/.acn`。upstream 是 Agent 侧的运行与团队连接配置；Agent 选中后，私有运行时根目录是 `<acn_home>/<upstream>/`。Router 与 Maintainer 不解析或选择 Agent upstream，团队数据固定使用各自 `<acn_home>/data/team/`。工作区由 `--cd` 或启动 cwd 决定，不能与 ACN 数据目录混淆。

## Rust 规范

- 异步 I/O 使用 `tokio`；禁止在 async 上下文直接做阻塞网络、文件、子进程或 sleep。
- async 锁优先 `tokio::sync::Mutex`，不要持有同步锁跨越 `.await`。
- 应用入口使用 `anyhow::Result`；模块边界优先 `thiserror`。
- 避免 `unwrap`、`expect`、`panic!`、`todo!`、`unimplemented!`。确有静态安全前提时就近写清原因。
- 降位转换使用 `TryFrom` / `TryInto`，不要直接 `as`。
- 路径使用 `Path` / `PathBuf` 与 `join`，不要拼接路径字符串。
- YAML 使用 `serde-yaml-ng`。持久状态写入遵循现有原子写、文件锁和恢复边界，不自行发明第二套。
- 复用现有配置结构；重要 endpoint、路径、模型和超时不在业务代码硬编码。
- 日志沿用项目现有 `log`/`ftlog` 管线，按可恢复程度选择 error、warn、info、debug、trace。
- 文件过大或职责混杂时拆到现有同级模块，避免为单文件创建无意义层级。
- 模块顶部用简短中文说明职责；注释解释约束和原因，不复述代码。

## 数据与安全边界

- `USER.md` 永不进入 claim；`MEMORY.md` 是 agent 私有内容，不直接上传团队服务。
- Router 只返回可引用的候选 claim 与 dispute 信息，不拥有 agent 私有 Memory。
- Maintainer 的 claim 属性更新是建议；agent 在 inbox 内化后决定本地变更。
- 单人模式不得发起团队网络请求，也不为未来补传偷偷积累团队上传队列。
- Team Auth、MCP token、LLM key 只保存环境变量名；不要在测试、文档或日志写入真实 secret。
- 修改协议 DTO 前检查持久化兼容、HTTP 客户端/服务端、prompt 与相关测试。

## 测试规范

- 纯函数和小范围行为使用同文件单元测试；跨模块与 CLI 行为放 `tests/`。
- 异步测试使用 `#[tokio::test]`。文件存储用临时目录与真实 I/O；外部 LLM 用本地 fake server 或测试 adapter。
- 修复或重构完成后按 `.agents/skills/verify/SKILL.md` 验证（含 `source export_env.sh`）。
- TUI 改动另按 `.agents/skills/tui-smoke-test-with-tmux/SKILL.md` 做 tmux 验收。

## 文档规范

- 稳定用户行为写 README / 使用说明 / 核心文档；需求决策写 `docs/PRDs/`，并标明完成状态，避免把历史实施清单写成未完成任务。
- 文档描述当前模块/类型，不写易漂移的源码行号；改名或挪目录时同步更新 Markdown、HTML、prompt、脚本与 TUI help 引用。

## Git 约定

- 分支前缀：`feature/` / `fix/` / `chore/`
- commit message 使用用户指定格式，未指定时常用[feat] / [fix] / [chore] / [prompt] / [refactor] / [test] / [docs] / [ci] / [style] / [revert] 加上": "开头的中文分点格式；未指定时保持简洁并准确描述改动。
- 工作区已有变更属于用户。不要回退、覆盖或顺手整理无关内容。

## 八荣八耻

1. 以暗猜接口为耻，以认真查阅为荣。
2. 以模糊执行为耻，以寻求确认为荣。
3. 以盲想业务为耻，以人类确认为荣。
4. 以创造接口为耻，以复用现有为荣。
5. 以跳过验证为耻，以主动测试为荣。
6. 以破坏架构为耻，以遵循规范为荣。
7. 以假装理解为耻，以诚实无知为荣。
8. 以盲目修改为耻，以谨慎重构为荣。
