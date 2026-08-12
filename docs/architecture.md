# Agent Claim Network 系统架构

本文描述 ACN 的运行形态、组件职责、存储边界和核心数据流。

## 运行形态

### 单人模式

当 Agent 所选 upstream 配置的 `maintainer_endpoint` 与 `router_endpoint` 都为空时，`acn` 不创建团队 HTTP client，也不发起团队请求。Agent 仍可使用：

- 多轮 TUI session、resume、compact 与 finalize
- LLM、文件、进程、Web、MCP、Skill、Memory、session search
- 本地 claim、trace、inbox 与 session 存储
- session subagents 与后台 finalize supervisor

单人模式是明确的产品模式，不是连接故障后的临时降级。它不会为未来的团队连接预先积累上传队列。

### 团队模式

当 `maintainer_endpoint` 与 `router_endpoint` 都有值时，Agent 在 session 启动、恢复和显式 `/inbox` 时访问团队服务：

- 从 Maintainer 拉取并确认 inbox
- 将本地新 claim mirror 和 dispute 上传 Maintainer；Trace 保留在 Agent 本地
- 从 Router 获取 scope overview，并在需要时通过 `consult_router` 查询团队 claim

两个 endpoint 必须成对配置。单项配置会在启动校验阶段报错。

团队服务失败不会中止本地 session。ACN 会显示 warning，将对应连接状态记为失败，并继续处理已持久化的本地 inbox 与本地任务。

## 组件职责

### Agent

Agent 是用户直接运行的 `acn` TUI：

- 维护 session、turn journal、compact checkpoint 与 finalize checkpoint
- 运行 provider-neutral LLM tool loop
- 管理用户工作区内的文件、命令、后台进程与附件
- 维护私有 Memory、用户资料、本地 claim、trace、inbox 和 dispute 上报幂等台账
- 校验 LLM 返回的 claim 引用，防止引用未出现在本地或当前上下文中的 ID
- 在团队模式下访问 Router 与 Maintainer

Agent 是自己 claim 的 holder。Maintainer 可以发送属性调整建议，但不能绕过 Agent 直接修改其本地权威 claim。

### Router

Router 是团队知识的检索服务：

- 从团队侧 agent claim mirror 构建派生视图
- 维护 claim index、scope overview、lexical 检索文档和可选向量状态
- 按配置执行 lexical、vector/hybrid recall 与 rerank
- 返回完整候选 claim、相关 dispute 摘要和检索诊断信息

Router 的视图可以重建，不是 claim 的权威来源。它不读取 Agent 的 Memory、用户资料或 session transcript。

### Maintainer

Maintainer 是团队治理与投递服务：

- 接收 Agent 上传的 claim mirror 与 dispute
- 发布或废弃 policy
- 解决 dispute
- 根据 claim 最近语义更新时间执行 stale sweep，并向 holder 发送属性调整建议
- 维护 outbox、投递状态、send log、history 与团队 key
- 提供管理 API 和 Workbench 页面

Maintainer 不以 trace 引用次数直接修改 claim，也不删除已经解决的历史 dispute。

### Finalize Supervisor

TUI 退出非空 session 时，可将 finalize job 交给独立 supervisor。Supervisor 串行恢复 session checkpoint，完成 recap、claim/dispute/trace 本地落盘，并在团队模式下上传 claim mirror 与 dispute，使终端无需等待完整收尾。

它与 `code_run` 的进程管理器职责不同：前者管理 ACN 后台 finalize job，后者管理工具启动的 shell process。

## Provider 与工具边界

核心 turn loop 不依赖具体 LLM 协议。当前 adapter 支持：

- Anthropic Messages
- OpenAI-compatible Chat Completions
- OpenAI-compatible Responses（HTTP SSE/JSON，`store = false`）

普通 turn、compact、finalize recap、inbox 内化和 memory review 共享 provider-neutral DTO，但使用各自的 prompt 和工具权限。

工具按职责组织在 `src/tool/`，包括文件、受管进程、Web、working note、Memory、session search、MCP、subagent 与团队 Router 查询。MCP server 连接由 session 共享的 connection manager 管理；subagent 只能获得其权限边界允许的工具。

## 运行目录

`upstream` 是 Agent 侧的运行与团队连接配置，本地名称不是服务端团队 ID。`[storage].acn_home` 是 base 目录，默认 `~/.acn`；顶层配置文件属于 base。Agent 选中 upstream 后，私有运行数据写入独立 runtime root：

```text
<acn_home>/
  config.toml
  data/
    team/
      agents/
        <agent_id>/
          claims/
      router/
      maintainer/
  <upstream>/
    ACN.md
    .mcp.json
    skills/
    data/
      agents/
        <agent_id>/
          claims/
          traces/
          disputes/
          inbox/
          maintainer_uploads/
          memories/
            MEMORY.md
            USER.md
          sessions/
          runtime/
          session_search_index.sqlite
```

Agent 进程使用 `<acn_home>/<upstream>/data/agents/<agent_id>/`。Router 和 Maintainer 共同提供 Agent 所连接的团队服务，但 daemon 不解析或选择 Agent upstream，只使用各自 `<acn_home>/data/team/`。在分布式部署中，各进程通过 HTTP 交互；即使单机共用一个 base `acn_home`，团队侧目录也只属于 daemon，不由 Agent 迁移或直接读取。

工具工作区由 `--cd` 或启动 cwd 决定，与 ACN runtime root 没有从属关系。

## Session 生命周期

1. 启动或恢复时处理 inbox；团队模式还会拉取 Maintainer 消息并查询 Router scope overview。
2. 读取当时的 Memory、用户资料、本地 claim、Skill 摘要和 `ACN.md`，渲染并持久化 system prompt。
3. 每个用户 turn 经 provider tool loop 执行；canonical message 与增量 turn journal 分别承担稳定历史和中断恢复。
4. 上下文接近预算时在 provider request 前压缩已覆盖历史；手动 `/compact` 复用同一边界。
5. 退出后 finalize 生成 recap，形成或更新 claim 与 trace；团队模式下再持久上传 claim mirror，并报告符合条件的 dispute。
6. 已 finalize 的 session 关闭；空 session 可以直接清理。

已创建 session 的 system prompt 是冻结快照。修改 `ACN.md`、Memory、Skill 或本地 claim 只影响后续新 session。

## Claim 协作流

```text
用户任务
  → Agent 使用本地知识或 consult_router
  → turn / finalize 形成本地 Claim 与 Trace
  → 团队模式向 Maintainer 上传 Claim mirror，并报告 Dispute
  → Maintainer 更新 mirror / outbox / governance state
  → Router 刷新派生视图
  → 其他 Agent 查询并引用 Claim
```

<p align="center">
  <a href="assets/acn-team-claim-flow.webp">
    <img alt="ACN 团队模式下 Agent、Router、Maintainer 与 Claim 的协作流程" src="assets/acn-team-claim-flow.webp" width="960">
  </a>
</p>

借用其他 Agent 的 claim 不会自动复制为本地 claim。只有当前 Agent 形成了自己的稳定判断时，才创建以自己为 holder 的新 claim，并通过 `source_claim_ids` 记录来源。

## Inbox 投递

Maintainer outbox 是持久投递台账。消息可定向或广播，并为每个接收 Agent 派生稳定 inbox ID。Agent 的同步顺序是：

1. 拉取消息。
2. 逐条持久化到本地 inbox。
3. 对已经持久化的消息发送 receipt ACK。
4. 领取本地 pending 消息并交给内化流程。
5. 成功后写 `handled_at`，失败则释放 lease 供下次重试。

Policy 消息自包含完整 payload；Agent 不需要也不允许直接读取 Maintainer 的 policy 文件。

## 鉴权与故障边界

- LLM、Web、MCP bearer 与 Team Auth secret 从环境变量读取；通过 `acn mcp login` 获得的 MCP OAuth token 按 server 配置保存在系统 keyring，或 selected upstream runtime 下权限受限的 `.mcp-oauth/` 目录。
- Team Auth 请求使用带 `agent_id` 与 key 的信封；服务端可按配置启用校验。
- Maintainer 管理页面与管理 API 可以启用独立 Basic Auth。
- Router/Maintainer 网络错误按可重试性分类并进入 warning；本地持久化失败仍是当前操作的硬错误。
- Router 派生状态、session search SQLite 和 history current 文件都可从权威数据恢复。
