# ACN Upstream 鉴权与目录隔离 需求设计

> 状态：已实现。本文保留 Agent upstream 隔离、Team Auth 和服务端鉴权决策。

## 背景

不同用户需要访问各自团队的 ACN 服务与数据，团队之间必须隔离，因此引入鉴权。

`upstream` 是 Agent 侧的一份运行与团队连接配置。它包含 Agent 身份、router/maintainer endpoint、API key 来源和独立的本地私有数据目录；Router 与 Maintainer 共同提供团队服务，但自身不选择 upstream。

同一自然人可以拥有多个组织角色（例如「AI 实验室 Leader」与「公司决策层」），每个角色可以对应一份 upstream。用户在 `<acn_home>/config.toml` 里按 upstream 配好身份与 endpoint，启动时 `acn --upstream <name>` 选择身份；不传则用配置中的默认 upstream。

当前采用 **API key**，不做 password / 登录态 / 账号系统。注册方式：由有 Maintainer dashboard权限的人创建 `<agent_id, acn_key>` 并分发给用户，用户把它和团队 endpoint 填进`config.toml`。

## 目标

- 同一自然人通过 Agent upstream 配置选择团队身份，访问对应团队的 router/maintainer。
- 团队之间的对象与本地数据目录完全隔离，**零内容流动**。
- 鉴权用 per-agent `<agent_id, acn_key>` 凭据；服务端只保存 key hash，不存明文。
- 团队侧不可达或未授权时，本地普通对话仍可继续。
- 旧 `<acn_home>` 下已有数据可迁移到 upstream 隔离布局。

## 非目标

- password / 登录态 / 完整账号系统、OAuth / JWT / session token。
- key store 外部手工编辑的文件监听式热重载。
- 鉴权限流（列入后续增强）。
- **maintainer dashboard / operator 页面的访问鉴权**——这是 main 上已有的**另一套**前端鉴权，与本设计的团队鉴权「不冲突也不关联」，不在本设计范围内。
- 动态多 key / 一人多 key（当前一 key 约等于一人）。

## 配置形态

客户端 `config.toml`：

```toml
upstream = "team-a"                                # 默认 upstream，--upstream 可覆盖

[upstreams.team-a]
agent_id = "agent-operator"
acn_key_env = "TEAM_A_ACN_AUTH_KEY"                     # 指定团队 ACN key 从该环境变量读取
maintainer_endpoint = "https://maintainer.team-a.example.com"
router_endpoint = "https://router.team-a.example.com"

[upstreams.team-b]
agent_id = "agent-operator"
acn_key_env = "TEAM_B_ACN_AUTH_KEY"
maintainer_endpoint = "https://maintainer.team-b.example.com"
router_endpoint = "https://router.team-b.example.com"
```

字段：

- `upstream`：默认 upstream 名；缺省时必须用 `--upstream` 指定。
- `[upstreams.<name>]`：`agent_id` + `acn_key_env` + 两个 endpoint。
- `acn_key_env`：唯一支持的 upstream 团队鉴权 key 配置字段；持久化的只是环境变量名。未配置或环境变量不存在/为空时不阻塞 agent 启动，请求鉴权值为空字符串。

> 两个 upstream 可用同一个 `agent_id`（同一人的两个角色），因此本地状态必须按 upstream 隔离（见「目录隔离」）。

服务端 key store 由 maintainer 维护，落在团队数据根下，例如：`<team_root>/maintainer/auth_keys.yaml`。router 启动时读取同一份台账，只做只读校验；dashboard 的 key 创建 / 禁用由 maintainer 写入该文件。

key store `auth_keys.yaml` 只存 hash，不存明文：

```yaml
auth:
  enabled: true
  api_keys:
    # 普通团队成员（agent）
    - key_id: key_abcd1234
      agent_id: agent-operator
      key_hash: sha256:<64 hex>
      generated_time: "2026-06-26T12:00:00Z"
      status: active
    # maintainer→router 内部 service 凭据；明文由 maintainer 私有文件保存。
    - key_id: key_router_service
      agent_id: router-service
      key_hash: sha256:<64 hex>
      generated_time: "2026-06-26T12:00:00Z"
      status: active
```

字段：

- `key_id`：台账行 id，用于 dashboard 操作与审计；使用随机 8 位 hex。
- `agent_id`：凭据绑定的 agent 身份，必须匹配 `^[a-z0-9_-]+$`；`router-service` 是系统保留 service 身份。
- `key_hash`：`sha256:<64 hex>`，服务端不保存明文 key。
- `generated_time`：UTC 生成时间。
- `status`：支持 `active` / `revoked`；只有 `active` 放行。

约束：

- 同一个 `agent_id` 同时最多只能有一条 `active` key；创建重复 active key 返回冲突。
- `revoked` key 保留在台账里用于审计，不再放行。

## CLI 形态

```
acn --upstream team-b       # 选择团队身份并切换本地数据目录
acn                         # 用 config 中的默认 upstream
```

规则：

- 选中 upstream 后，`agent_id`、endpoint、团队 key、本地 ACN 根目录一起切换。
- 不保留 `--agent` 兼容入口；未配置 `[upstreams]` 或未能解析选中 upstream 时直接报错。

## 鉴权模型

agent 与团队侧交互只经过 **5 个 HTTP 端点**，它们的请求体统一改成 `{auth, data}` 信封：`auth` 放身份与凭据，`data` 放该端点原本接收的字段。

| 服务 | 端点 | `data` 内容 |
| --- | --- | --- |
| maintainer | `POST /inbox/pull` | `{ agent_id }` |
| maintainer | `POST /claims/upload` | 一条 `Claim` |
| maintainer | `POST /disputes/report` | 一条 `Dispute` |
| router | `POST /claims/query` | `{ scope, semantic_query? }` |
| router | `POST /claims/scopes/overview` | `{}`（无业务字段；为统一信封由 GET 调整为 POST） |

信封示例（上传 claim）：

```json
{
  "auth": { "agent_id": "agent-operator", "acn_key": "acn_xxxxxxxx" },
  "data": {
    "id": "claim_abcd1234",
    "holder": "agent-operator",
    "scope": "order-system / batch-submit",
    "...": "..."
  }
}
```

校验规则：

- 服务端对 `auth.acn_key` 取 hash，在团队 key store 里查找；必须命中一条 `<agent_id, key_hash>`，且 `agent_id == auth.agent_id`、`status == active`，否则拒绝（无法访问团队内容）。
- **对象级绑定**：`data` 里的 agent 字段必须等于 `auth.agent_id`——`inbox/pull` 的 `data.agent_id`、`claims/upload` 的 `claim.holder`、`disputes/report` 的 `dispute.reporter_agent_id`。`pull_inbox` 的 lazy-register 只在校验通过后才产生副作用。
- router 的 `claims/query`、`claims/scopes/overview` 是**团队内共享知识池**，任何持本团队合法凭据者均可全量读取，不做对象绑定。跨团队隔离由「每个团队独立 daemon + 数据根」保证。

失败状态：endpoint 对但 `auth` 错误或与团队不匹配 → 服务端拒绝（`401/403`）；错误文本保持泛化，不回显 key 或内部细节；`401/403` 不重试。

**maintainer → router（server-to-server）**：Maintainer daemon 启动时始终 ensure 一把系统保留 service 凭据，身份固定为 `agent_id = "router-service"`，不受 `maintainer.auth.team.enabled` / `router.auth.team.enabled` 开关影响。服务端 key store 仍只保存 hash；Maintainer 私有保存明文到 `<team_root>/maintainer/service_keys/router_service_acn_key`。若 key store 中已有 active `router-service` row 且私有明文 hash 匹配，则复用；若缺失、revoked、私有明文缺失或 hash 不匹配，则撤销旧 active row 并生成一把新的 service key。Maintainer Workbench 的 Router Query 始终展示，调用 router 时使用该 service 凭据填入同样的 `{ auth, data }` 信封。

`router-service` 是后端保留身份：Team Auth 列表不展示；用户创建 key 时禁止使用该 `agent_id`；maintainer 的 `/inbox/pull`、`/claims/upload`、`/disputes/report` 拒绝 `auth.agent_id == "router-service"`，避免内部 service key 被当作普通 agent key 使用。Router 的两个读端点允许合法 `router-service` 凭据读取团队知识池。

> maintainer dashboard / operator 页面本身的访问鉴权由 main 已有的前端鉴权负责，与本设计无关；本设计只约束上述 5 个端点的团队鉴权。

## 实现落点

### 配置与客户端装配

- `UpstreamConfig` 只支持 `acn_key_env` 作为 upstream 团队鉴权 key 配置字段；旧的明文 key 字段与别名字段不再兼容。
- `ResolvedUpstream` 带上运行期解析出的团队 key，`bootstrap::build_agent_cli_runner` 在团队模式下用它构造带鉴权的 `HttpMaintainerClient` / `HttpRouterClient`。
- `Config` 保留全局 `storage.acn_home`，解析 upstream 后再派生 runtime storage roots，避免不同 upstream 共享 `skills/ACN.md/data/agents/...`。

### 服务端鉴权

- 新增团队 key store 模块，负责读取 / 写入 `<team_root>/maintainer/auth_keys.yaml`、生成 key、hash、常量时间校验、status 判断。
- maintainer 的 3 个 agent-facing endpoint 增加团队鉴权；仍保持现有 admin auth 对 dashboard / operator API 的保护边界。
- router 的 `POST /claims/query` 与 `POST /claims/scopes/overview` 增加团队鉴权。`/claims/scopes/overview` 从 GET 改 POST 是协议变更，需要同步更新 `HttpRouterClient::scopes_overview`、maintainer 的 `/api/router-query` 代理路径、前端 Settings endpoint catalog 与相关测试。

### Dashboard key 管理

这些接口走现有 maintainer admin auth，不走团队 key auth：

| 方法 | 端点 | 说明 |
| --- | --- | --- |
| `GET` | `/api/team-auth/keys` | 列出 key 台账行，不返回明文 key |
| `POST` | `/api/team-auth/keys` | body: `{ agent_id }`；生成新 key，返回台账行与一次性明文 |
| `POST` | `/api/team-auth/keys/{key_id}/revoke` | 把 key 标记为 `revoked` |

前端新增 Team Auth / Keys 页面，支持创建、一次性复制明文、列表查看、禁用。列表只展示 `key_id / agent_id / generated_time / status`，不展示 hash 和明文。

## 失败与降级语义

团队侧不可达或未授权时，本地普通对话必须继续，不因团队交互失败而中断 session：

- **endpoint 错误 / 超时 / 重试耗尽**：HTTP client 走完 timeout / retry 后打 warning，本地会话继续。
- **`auth` 错误或与团队不匹配**：服务端 `401/403`，client 不重试，按 warning 处理。
- 上述两类失败对各端点的统一表现：
  - `pull_inbox` 失败：warning 后只处理本地已有 inbox。
  - `consult_router` / `scopes_overview` 失败：HTTP client / RouterClient 底层仍返回错误；tool / prompt 组装层捕获错误并转成结构化降级结果，让模型知晓团队知识暂不可达或团队鉴权失败，本轮对话继续。
  - `claims/upload`、`disputes/report` 失败：claim 已先写本地不丢，上传/上报失败只记 warning，**不中断会话**。超时 / 5xx 等 retryable 失败进入待重传；`401/403` 只提示修正 key 或对象归属，不进入自动 pending 重试队列。

`consult_router` 的降级边界：

- router endpoint / `HttpRouterClient` / `RouterClient` 不改成成功响应；`401/403/timeout/5xx` 仍按真实错误返回，便于日志、重试和调用方判断。
- agent 的 tool 层负责把 `RouterClient` 错误转成一次正常 tool result，例如 query 模式返回：

```json
{
  "mode": "query",
  "available": false,
  "reason": "router_auth_failed",
  "status": 401,
  "message": "团队 router 鉴权失败，请检查当前 upstream 的 acn_key_env。"
}
```

- endpoint / timeout / 5xx 等非鉴权错误继续使用 `reason: "router_unavailable"`；overview 模式同理返回 `available: false` 与空 `scopes`。session system prompt 组装层继续把 overview 错误渲染为普通提示。

## 目录隔离与迁移

**服务端**：每个团队一套 daemon + 一个独立数据根目录，端口隔离，天然隔开。

**客户端**：Agent 本地状态按 upstream 隔离。`storage.acn_home` 是全局 base；Agent 选中 upstream 后派生 `runtime_acn_home = <acn_home>/<upstream>/`，再从这个 runtime root 派生 `skills_root`、`agents_root`、`ACN.md`、`.mcp.json`。因此 `claim / inbox / memory / session / skills / ACN.md / .mcp.json` 等 Agent 本地 sink 一并收敛到选中的 upstream。Router / Maintainer 的 `team_root = <acn_home>/data/team` 属于服务端数据根，daemon 不解析或选择 Agent upstream：

```
<acn_home>/
  config.toml                 # 全局配置入口，留在 base，不随 upstream 迁移
  data/team/                 # Router / Maintainer 团队数据，不属于 Agent upstream
  team-a/                    # upstream = team-a 的 Agent 运行时根
    .mcp.json
    ACN.md
    skills/
    data/agents/<agent_id>/   # claims / traces / inbox / memory / sessions
  team-b/
    ...
```

约束：

- upstream 名作为顶层目录名，需保留名校验（禁止 `data`、`skills`、`config.toml`、`acn.md`、`logs`、`tmp`、`.git` 等）。
- 同 `agent_id` 的不同 upstream 本地状态互不共享。

**迁移**：旧用户在 `<acn_home>/data/agents/<id>` 等路径已有数据时，`acn` 用户入口在首次激活 upstream runtime 前自动迁移 `.mcp.json`、`ACN.md`、`skills/` 和当前 `agent_id` 对应的 `data/agents/<id>` 到 `<acn_home>/<选中 upstream>/...`。`<acn_home>/data/team` 始终保留给 Router / Maintainer，不进入 Agent 迁移清单。单独启动 Router / Maintainer daemon 不执行这类 legacy Agent 本地状态迁移：

- 仅当目标不存在时迁移，避免覆盖用户已经在 upstream runtime 下创建的新状态。
- 不迁移 `config.toml`。
- 迁移逻辑在 Rust 代码中实现，启动时复用 binary 的 upstream / agent_id 校验；执行迁移时拒绝 symlink 与越过 `<acn_home>/<upstream>` 的路径，避免旧数据被搬到 ACN home 之外。

## 密钥管理

- 每个 agent 单独 `<agent_id, acn_key>`；maintainer→router 使用自动托管的 `router-service` 内部凭据，与普通 agent 凭据分开。
- acn_key 由服务端生成（熵 ≥128bit），dashboard 创建后只展示一次明文，服务端只存 key hash。
- Maintainer dashboard 提供普通 agent key 管理闭环：列表展示 `agent_id / generated_time / status`，创建 key 时返回一次性明文，禁用 key 时把 `status` 改为 `revoked`。这些管理接口走 main 已有 maintainer admin auth，不走本设计的团队 key auth；如果 maintainer admin auth 未启用，key list/create/revoke 直接返回 `403`。`router-service` 不在列表展示，也不能由用户创建或禁用。
- 验证对 key digest 做常量时间比较；不可逆。
- agent 侧日志与错误消息不回显完整 acn_key。
- acn_key 随请求 `auth` 字段到达团队侧 maintainer/router 处理逻辑，但持久化审计日志前必须递归脱敏 `acn_key`，审计文件不落明文 key。
- 刷新语义：router 鉴权前按 key store 当前内容全量替换 verifier 快照，避免旧 key 在内存中滞留；maintainer dashboard 创建 / 撤销普通 key 后也刷新 maintainer verifier。
- 生产部署走 HTTPS 或受控内网隧道；纯 HTTP 仅限本地/内网开发。

## 测试与验收

**配置**：upstream key 只从 `acn_key_env` 解析；env 缺失/为空不阻塞 agent 启动；旧 key 字段与别名字段拒绝解析；不含 key 的 config 仍可加载。

**目录隔离 / 迁移**：同 `agent_id` 不同 upstream 的 session/claim/inbox/memory 互不共享；迁移后旧数据在目标 upstream 下可读。

**鉴权（信封）**：5 个端点正确解析 `{auth, data}`；`acn_key` 命中 key store 且 `agent_id` 匹配才放行；`auth` 缺失/错误/与端点不匹配 → 拒绝且不重试；`data` 内 agent 字段与 `auth.agent_id` 不一致 → 拒绝；`router-service` 不能访问 maintainer 的 agent-facing 端点；Maintainer Workbench 的 Router Query 在 router team auth 开启/关闭时均可使用。

**失败降级**：`401/403` 或超时下 `pull_inbox` warning 后继续；`claims/upload`、`disputes/report` 失败返回成功且本地 claim 已写入；`consult_router` 对 `401/403` 返回 `router_auth_failed`，对 endpoint / timeout / 5xx 返回 `router_unavailable`，对话继续。

**验收标准**：

- `acn --upstream <name>` 可选择团队身份并切换本地目录。
- 五个端点用 `{auth, data}` 信封鉴权，对象级绑定生效，跨团队不可访问。
- 团队不可达/未授权时本地对话不中断。
- 本地数据按 upstream 隔离，旧数据可迁移。

## 后续可选增强

- 鉴权端点限流（per-IP / per-agent）。
- key 轮换 overlap 窗口。
- durable team-sync outbox：失败的 claim/dispute 在团队恢复后自动补传。
- maintainer→router 独立网络边界（内网 ACL / loopback）作为 service 凭据的替代方案。

## 补充拍板

以下结论是当前实现的一部分：

- agent / maintainer / router 不再兼容旧 body 协议；5 个团队侧端点只接受 `{ auth, data }` 信封。团队鉴权关闭时也要求请求形状是信封，只是不校验 `auth.acn_key`。
- agent 侧启动时不强制校验选中 upstream 是否配置 `acn_key_env`，也不因环境变量缺失或为空阻塞启动。请求仍然携带 `auth`：`auth.agent_id` 来自选中 upstream，`auth.acn_key` 使用配置解析到的 key；未配置或缺失时传空字符串。
- 团队侧是否校验 agent 请求由服务端 `config.toml` 控制，默认关闭：

```toml
[maintainer.auth.team]
enabled = false

[router.auth.team]
enabled = false
```

- `maintainer.auth.team.enabled = true` 时，maintainer 的 `/inbox/pull`、`/claims/upload`、`/disputes/report` 校验团队 key，并继续执行对象级绑定；为 `false` 时只要求信封结构正确，不校验 key。
- `router.auth.team.enabled = true` 时，router 的 `POST /claims/query` 与 `POST /claims/scopes/overview` 校验团队 key；为 `false` 时只要求信封结构正确，不校验 key。
- Maintainer Workbench 的 Team Auth 页面顶部展示 maintainer 与 router 两侧团队鉴权开关状态。
- Maintainer 启动时自动 ensure `router-service` 内部 service key：已有 active row 且私有明文 hash 匹配则复用；否则撤销旧 active row 并生成新 key，hash 写入 `<team_root>/maintainer/auth_keys.yaml`，明文写入 `<team_root>/maintainer/service_keys/router_service_acn_key`。
- Maintainer Workbench 的 Router Query 页面始终显示；访问 router 时使用 `router-service` 凭据。Team Auth 页面不展示 `router-service`，用户创建 key 时也不能使用该 agent_id。
- Router 每次鉴权前从 key store 全量替换 verifier 快照，不做仅追加刷新；这保证新 service key 及时生效，旧 key / revoked key 不继续滞留在内存里。
- Maintainer 的 `/inbox/pull`、`/claims/upload`、`/disputes/report` 拒绝 `auth.agent_id == "router-service"`。
- 迁移逻辑必须由 Rust binary 自动执行；执行迁移时拒绝 symlink 目标与越过 `<acn_home>/<upstream>` 的路径，避免旧数据被搬到 ACN home 之外。
- Agent upstream 迁移不得移动、复制或删除 `<acn_home>/data/team`；单机共享 `acn_home` 时，Router / Maintainer 的团队数据必须保持原位。
- 对旧版本遗留的 `<acn_home>/<upstream>/data/team`，Agent 激活对应 upstream 时可删除普通空目录；非空目录或包含 symlink 的路径必须拒绝 Agent 启动并提示人工合并。Router / Maintainer 不扫描 Agent upstream 目录。
- agent 入口加载配置时不应先创建 base `<acn_home>/skills`、`<acn_home>/data/...` 等旧布局目录；应在解析并激活 upstream 后，只 ensure 当前 `<acn_home>/<upstream>/...` 运行时目录。
