# PRD: 支持按 upstream 隔离的自定义 MCP Server

> 状态：已实现。本文保留 MCP 配置、transport、工具暴露与 TUI 管理决策。

## 背景

ACN 当前已经有 provider-neutral 的工具回环：`ToolRegistry::definitions()`负责向模型暴露工具 schema，`ToolRegistry::dispatch_with_context()` 负责执行工具，`AgentTurnLoop` 负责 tool_use / tool_result 回灌，TUI 通过`SessionEvent::ToolCallStarted` / `ToolCallCompleted` 展示工具调用。

本需求是在现有链路上接入用户自定义 MCP server，让用户可以把本地 stdio或远程 Streamable HTTP MCP server 注册为 ACN 可用工具，并在 TUI 中看到MCP server 状态和 MCP tool 调用过程。

架构要求：

- Rust 侧通过 MCP connection manager 聚合 server、工具列表和 tool call 路由。
- 模型可见工具名与真实 MCP server/tool 名分离，使用 `mcp__server__tool` 作为稳定映射。
- CLI 负责增删和检查 server 配置，TUI 负责展示运行状态、工具及暴露诊断。

协议依据：

- 本实现使用 `rmcp 3.0.1`，建连时优先通过 `server/discover` 协商 `2026-07-28` 协议。
- 对明确不支持 `server/discover` 的旧 server，自动退回 `initialize`，使用 `2025-11-25` 的旧生命周期；不要求用户在配置中填写版本。
- 选型 MCP Rust SDK 时优先选择能跟随协议版本演进的实现，避免手写 transport后续迁移成本过高。

## 目标

- 支持 selected upstream 运行根下的 MCP 配置文件：`<base_acn_home>/<upstream>/.mcp.json`。
- 支持新增、删除、查看、检查 MCP server 的 CLI 命令。
- 支持本地 stdio MCP server。
- 支持远程 Streamable HTTP MCP server。
- 将 MCP `tools/list` 返回的工具注入现有 provider request 的 `tools` 字段。
- 支持模型实际调用 MCP tool，并把结果作为标准 tool_result 回灌。
- TUI 能展示 MCP server 启动失败 warning、`/mcp` 状态，以及 MCP tool 调用。
- MCP server 启动失败默认不阻塞 TUI，失败 server 的工具不暴露给模型。

## 非目标

本需求不做：

- 项目级 `.mcp.json`。
- 多 scope 合并，例如 user / project / local / enterprise。
- MCP elicitation 中途向用户展示 URL / form 并等待确认。
- SSE / WebSocket transport。
- MCP Tasks / task-augmented execution；客户端不宣告 `2026-07-28` Tasks extension，也不承诺识别或自动过滤旧版 task-required 工具。
- MCP prompts。
- MCP resources / resource templates 作为模型工具直接暴露。
- 运行中热重载 MCP 配置；用户修改后重启 TUI 或后续再做 `/mcp reload`。
- 复杂权限审批系统；MCP tool 与内置 tool 一样先走现有工具调用路径。

## 已拍板决策

1. transport：支持 `stdio` 和 `streamable_http`。
2. 配置位置：只做 selected upstream runtime 配置，文件为 `<base_acn_home>/<upstream>/.mcp.json`。
3. `acn mcp add` 只保存配置，不强制连接校验；用户可用 `acn mcp status` 检查。
4. `acn mcp add-json <name> '<server-json>'` 接受单个 server JSON，复用相同的校验、重复名称检查和原子写入逻辑。
5. TUI 启动时 MCP server 失败默认 warning，不阻塞 TUI。
6. 模型可见工具名使用 `mcp__server__tool`。
7. Streamable HTTP server 支持 OAuth login / logout：使用 OAuth discovery、PKCE 和 loopback callback，支持动态 client registration 或预注册 public client ID；token 与 client id 按配置保存到系统 keyring 或 selected upstream runtime 的私有文件。登录合并已有 grant、server challenge 与 resource metadata 的 scope 要求，并在授权、token 交换和 refresh 中携带 `resource`。
8. `/mcp` 使用 ACN live panel 交互，并提供工具暴露状态诊断。
9. `/mcp` 是不写 transcript 的 live panel，可在 turn 运行期间打开；session picker 等独占视图仍优先。
10. TUI tool list 显示 server 发现到的全部工具，并用不同颜色区分`exposed` / `filtered` / `unsupported`。
11. TUI 操作只做 View tools / Reconnect / Disable 或 Enable；Add / Remove继续走 CLI。
12. Disable / Enable 是持久配置开关：写入 selected upstream runtime 下 `.mcp.json` 的`enabled` 字段，并立即影响当前 session 的工具集合。
13. Reconnect 只做运行时重连和工具刷新，不自动修改配置。
14. 安装信息可在 TUI 查看，但 env value、bearer token 等敏感值必须隐藏。
15. Enable / Reconnect 是异步操作；期间不排普通 prompt queue，后续 provider request 只使用发送瞬间 `ready` 的 MCP tool snapshot。

项目级配置、resources 与运行中 reload 不属于本需求，后续需要时单独拍板。

## 用户体验

### 新增本地 stdio MCP server

```bash
acn mcp add pal \
  -e DEFAULT_MODEL=auto \
  -e DISABLED_TOOLS=analyze,refactor,testgen,secaudit,docgen,tracer \
  --env-var OPENAI_API_KEY \
  --env-var OPENAI_BASE_URL \
  -- uvx --from git+https://github.com/BeehiveInnovations/pal-mcp-server.git pal-mcp-server
```

含义：

- `pal` 是用户给这个 MCP server 起的本地名字。
- `--` 后面是本机启动 MCP server 的真实命令。
- 即使命令从 GitHub 下载代码运行，它仍然是本地 stdio transport。
- `-e KEY=VALUE` 将字面量写入 `.mcp.json`，适合非敏感配置。
- `--env-var KEY` 只记录环境变量名，运行时从当前进程环境继承，适合 token。

### 新增远程 Streamable HTTP MCP server

```bash
acn mcp add linear \
  --url https://mcp.linear.app/mcp \
  --bearer-token-env-var LINEAR_API_KEY
```

含义：

- 有 `--url` 时推断为远程 `streamable_http`。
- `bearer_token_env_var` 只保存环境变量名，不保存真实 token。
- OAuth server 可改用 `--oauth-client-id <public-id>`、`--oauth-callback-port <port>` 和 `--oauth-credentials-store keyring|file`；未配置 client ID 时登录流程使用动态注册。

### 通过单个 JSON 新增 MCP server

```bash
acn mcp add-json pal '{
  "type": "stdio",
  "command": "uvx",
  "args": ["pal-mcp-server"],
  "env_vars": ["OPENAI_API_KEY"],
  "startup_timeout_secs": 30
}'

acn mcp add-json linear '{
  "type": "streamable_http",
  "url": "https://mcp.linear.app/mcp",
  "bearer_token_env_var": "LINEAR_API_KEY"
}'
```

行为约束：

- JSON 只表示单个 server，不接受完整的 `{"mcpServers": {...}}`。
- 支持 `McpServerConfig` 已有全部字段，包括 `cwd`、超时、工具过滤和 `enabled`。
- 输入的 `type: "http"` 兼容为 `streamable_http`，落盘统一保存为 `streamable_http`。
- server 已存在时拒绝覆盖，要求先执行 `remove`。
- 未实现的 `sse`、嵌套 `oauth`、`headers` 和其他未知字段均明确拒绝；OAuth 非敏感选项使用顶层 `oauth_client_id`、`oauth_callback_port` 与 `oauth_credentials_store`。
- 不要在 JSON 中直接写入 token；stdio 使用 `env_vars`，远程 HTTP 使用 `bearer_token_env_var`。

### 查看配置

```bash
acn mcp list
acn mcp get pal
acn mcp get pal --json
```

### 检查连接和工具

```bash
acn mcp status
acn mcp status pal
```

`status` 才真实连接 MCP server，并显示：

- server name
- transport
- ready / failed / disabled
- tools 数量
- 失败错误摘要

`acn mcp status <name>` 只连接并检查指定 server；`acn mcp status` 不带 name时才连接所有 enabled server。

### OAuth 登录与退出

```bash
acn mcp login linear
acn mcp login linear --no-browser
acn mcp logout linear
```

`login` 只适用于支持 OAuth discovery，且支持动态 client registration 或配置了预注册 `oauth_client_id` 的 Streamable HTTP server。命令使用 PKCE 和 loopback redirect；桌面环境自动打开浏览器并监听 callback，`--no-browser` 则打印授权 URL，并要求用户从浏览器地址栏复制完整 redirect URL 粘贴回终端。scope 会合并已有 grant、server challenge 与 resource metadata 的要求；仅在这些来源均为空时使用 authorization server metadata。授权、token 交换和 refresh 均绑定当前 MCP resource。未登录的 OAuth server 在连接失败时会提示对应的 `login` 命令。

OAuth 凭据按 selected upstream、server name 与 URL 隔离。默认写入系统 keyring；`oauth_credentials_store = "file"` 时写入 selected upstream runtime 的 `.mcp-oauth/` 私有目录，供没有 Secret Service / D-Bus 的 headless Linux 使用。`logout` 只删除本地凭据，不请求远端 token revocation。当前不支持 client secret、CIMD 与 device flow。

运行时找不到凭据记录时按未登录连接；凭据库不可用、refresh 要求重新授权或已加载身份被删除时必须 fail closed，不得把 OAuth-managed 请求降级为匿名请求。

登录失败要区分 discovery / DCR、PKCE、callback state、RFC 9207 issuer 和 token endpoint 阶段；CLI 输出可行动的分类原因，但不直接透传可能包含 URL、响应 body 或凭据的底层错误文本。

### 删除 server

```bash
acn mcp remove pal
```

先写入不含 token 的私有待清理记录并锁定该 server 的凭据变更，再删除 selected upstream runtime 下的 server 配置，最后尽力删除本地 OAuth 凭据。凭据库不可用时命令返回成功并显示 warning，明确说明配置已删除、凭据清理失败；不能因为 keyring / D-Bus 故障阻止配置删除。待清理记录会保留凭据 backend 与不可逆 account hash，因此配置不存在时仍可执行同名 `acn mcp logout <name>` 重试；清理完成前不允许重新添加同名 server。

### 启用 / 禁用 server

```bash
acn mcp disable pal
acn mcp enable pal
```

含义：

- `disable` 写入 `enabled: false`，保留 server 配置但跳过连接和工具注入。
- `enable` 写入 `enabled: true`，下次启动或 TUI Reconnect 后重新连接。
- TUI 中的 Disable / Enable 与 CLI 写入同一配置字段；区别是 TUI 操作会同时更新当前进程的 runtime snapshot，外部 CLI 修改不热加载已运行的 TUI。

## `.mcp.json` 格式

文件路径：

```text
<base_acn_home>/<upstream>/.mcp.json
```

示例：

```json
{ "mcpServers": {
    "pal": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/BeehiveInnovations/pal-mcp-server.git",
        "pal-mcp-server" ],
      "env": {
        "DEFAULT_MODEL": "auto",
        "DISABLED_TOOLS": "analyze,refactor,testgen,secaudit,docgen,tracer"
      },
      "env_vars": [ "OPENAI_API_KEY", "OPENAI_BASE_URL" ]
    },
    "linear": {
      "type": "streamable_http",
      "url": "https://mcp.linear.app/mcp",
      "bearer_token_env_var": "LINEAR_API_KEY"
    } } }
```

### 字段约定

通用：

- `type`：`stdio` 或 `streamable_http`。读取兼容：
  - 有 `command` 且缺 `type` 时视为 `stdio`。
  - 有 `url` 且缺 `type` 时视为 `streamable_http`。
- `enabled`：可选，默认 `true`。
- `startup_timeout_secs`：可选，默认 `30`。
- `tool_timeout_secs`：可选，默认 `120`；从 `tools/call` 发起到收到最终response 的硬超时。
- `enabled_tools`：可选 allowlist，按 MCP server 原始 tool name 匹配。
- `disabled_tools`：可选 denylist，按 MCP server 原始 tool name 匹配。

stdio：

- `command`：必填。
- `args`：可选，默认空数组。
- `env`：可选，写入字面量环境变量。
- `env_vars`：可选，从 ACN 进程环境继承这些变量。
- `cwd`：可选；缺省时使用 ACN 启动时的工具 workspace root。

streamable_http：

- `url`：必填。
- `bearer_token_env_var`：可选；存在时读取对应环境变量并作为 bearer token。

实现细节：

- HTTP 请求带 `Accept: application/json, text/event-stream`。
- 每个 HTTP 请求按当前协商结果携带 `MCP-Protocol-Version`。
- 如果 server 返回 `MCP-Session-Id`，后续请求继续携带该 header。
- 支持 server 对 SSE stream 做 polling / reconnect，但不把断线误判为用户取消。

不支持把真实 bearer token 明文写进 `.mcp.json`。

## 鉴权范围

当前支持：

- stdio server 通过 `env` / `env_vars` 获取 API key。
- Streamable HTTP server 通过 `bearer_token_env_var` 获取 bearer token。
- 支持 OAuth discovery、动态 client registration 或预注册 public client ID、PKCE、桌面 loopback callback 与 headless redirect URL 粘贴。
- OAuth token / client id 可持久化到系统 keyring，或 selected upstream runtime 下权限受限的文件；凭据按 upstream、server name 与 URL 隔离。
- OAuth scope 合并已有 grant、`WWW-Authenticate` 的 `scope` 与 resource metadata 的 `scopes_supported`；仅当前述来源均为空时，使用授权服务器 metadata 的 `scopes_supported`。授权服务器声明支持且请求 scope 非空时追加 `offline_access`。
- OAuth discovery 优先使用 protected resource metadata 声明的 `resource`；没有声明时才使用 MCP server URL。authorization code exchange 与 refresh token 请求始终使用同一个值，保证 token audience 绑定。

当前不支持：

- MCP elicitation 的 URL / form 用户确认。

OAuth server 缺少本地登录凭据、或 tool 需要 elicitation 时，应返回明确且可行动的错误：

```json
{
  "ok": false,
  "error": "MCP server requires interactive elicitation, which is not supported"
}
```

## 流式和超时策略

MCP 普通 `tools/call` 的最终结果按 JSON-RPC request / response 模型返回。也就是说，模型真正收到的 tool_result 是最终一次性回灌，而不是把工具输出token-by-token 流给模型。

但 MCP 支持 request-scoped notification：

- 客户端可在 `tools/call` 请求 `_meta.progressToken`。
- server 可在最终 response 前发送 `notifications/progress`。
- Streamable HTTP 可通过当前请求的 SSE response stream 承载这些 notification。
- stdio transport 也可在同一连接上收到 progress notification。
- `2025-11-25` 新增实验性 Tasks，可用于长任务轮询和 deferred result retrieval；当前不实现。

当前行为：

- `tools/call` 默认带 progress token。
- progress notification 只用于 TUI 展示和日志，不作为中间 tool_result 回灌给模型。
- 收到最终 response 后，才把完整 MCP result 转成 ACN tool_result。
- 如果 server 完全不发 progress，TUI 仍显示 running 状态。
- `tool_timeout_secs` 是硬超时，不因 progress notification 自动续期。
- 超时后：
  - 尽量发送 MCP cancellation notification；streamable HTTP 只关闭当前 response stream，并标记本次调用超时。
  - 不因单项 timeout 关闭 ready client/session；同连接的其他 in-flight 请求和后续请求继续由各自request 收束。
  - 只有 transport/connection 错误、disable/reconnect 或 ACN shutdown 才摘除并清理共享 client。
- 超时错误要作为 tool_result 回灌给模型，不能 panic 或卡住 turn loop。
- 当前不实现 MCP Tasks，也不宣告 `2026-07-28` Tasks extension。普通工具仍按 `tools/call` 调用。
- 直接使用 crates.io `rmcp 3.0.1`，不 vendoring SDK 源码。该版本不暴露旧版 `execution.taskSupport` 字段，ACN 不增加旁路协议解析或自动过滤。
- `taskSupport = "required"` 的 legacy 工具可能仍被分类为 `exposed`，随后因 ACN 发送普通 `tools/call` 而被 server 拒绝；这不影响同一 `2025-11-25` server 上的普通工具。已知的 legacy Tasks 工具由用户通过 `disabled_tools` 手动关闭。

后续如果需要支持长任务，可单独设计 task-augmented execution 或 idle timeout；当前不做。

## 工具注入方式

MCP 工具不手写进 system prompt 的工具列表。

正确链路：

1. 启动时读取 selected upstream runtime 下的 `.mcp.json`。
2. 初始化 enabled MCP server。
3. 对 ready server 调用 `tools/list`。
4. 将 MCP tool 转成 ACN `ToolDefinition`。
5. 追加到 `ToolRegistry::definitions()` 返回值。
6. provider request 通过现有 `tools` 字段发送给模型。

system prompt 最多保留通用说明，例如：

```text
MCP 工具以 mcp__server__tool 命名。
```

具体工具名、参数 schema、描述均以 provider request 的 `tools` 字段为准。

## 工具命名和路由

模型可见名：

```text
mcp__<normalized_server_name>__<normalized_tool_name>
```

示例：

```text
mcp__pal__ask
mcp__linear__list_issues
```

内部必须保存映射：

```text
visible tool name -> raw server name + raw MCP tool name
```

调用时：

1. 模型发起 `tool_use(name="mcp__pal__ask")`。
2. ACN 解析 visible name。
3. 查映射得到 raw server `pal` 和 raw tool `ask`。
4. 调用对应 MCP client 的 `tools/call`。
5. 将 MCP result 转成 ACN 标准 JSON tool_result。

命名规范：

- server name 限制为 ASCII 字母、数字、`-`、`_`。
- visible name 中非 `[a-zA-Z0-9_-]` 字符替换为 `_`。
- 如归一化后冲突，应追加短 hash，保证 visible tool name 唯一。
- 内置工具名和 MCP 工具名不得直接共用命名空间；MCP 一律带 `mcp__` 前缀。

## 架构设计

新增模块：

```text
src/mcp/
  mod.rs
  config.rs              # .mcp.json DTO、读取、原子写、校验
  name.rs                # mcp__server__tool 归一化、解析、冲突处理
  oauth.rs               # OAuth login/logout、loopback/headless callback、凭据存储
  client.rs              # 单个 MCP client lifecycle
  connection_manager.rs  # 多 server 聚合、tools/list、tools/call、status
  tool.rs                # MCP tool -> ToolDefinition / tool_result 转换
```

### Config 层

- `Config` 保持主配置入口不变。
- `<base_acn_home>/<upstream>/.mcp.json` 不放进 `config.toml`。
- 新增 helper：
  - `Config::mcp_config_path() -> PathBuf`
  - 或在 `StorageConfig` / path helper 中集中生成。
- CLI `acn mcp ...` 通过 `--config` 或默认配置解析出 base `acn_home`，再按 `--upstream` 或默认 upstream 修改对应 runtime 下的 `.mcp.json`。

写入要求：

- JSON pretty format。
- 原子写：临时文件 -> flush/fsync -> rename。
- 删除最后一个 server 后保留空结构：

```json
{
  "mcpServers": {}
}
```

### MCP Connection Manager

职责：

- 读取 MCP config。
- 并发启动 enabled server。
- 每个 server 独立 startup timeout。
- 每个 ready server 在本次 ACN 运行期持有并复用一条 client/session，主 agent 与 delegation child共享它；Reconnect 才替换这条连接。
- 收集 ready / failed / disabled 状态。
- 对 ready server 调 `tools/list`。
- 支持 `tools/list` pagination，直到取完或命中安全上限。
- 按 allowlist / denylist 过滤 raw tool。
- 不宣告 Tasks extension，也不为旧版 `execution.taskSupport` 增加独立解析路径或自动过滤承诺。
- 生成 visible name 映射。
- 执行 `tools/call`，并通过 progress callback 把进度事件交给 TUI / 日志层。
- 对 `initialize`、`tools/list`、`tools/call` 分别套 timeout。
- 支持对单个 server reconnect，刷新状态、工具列表和 visible name 映射。
- 支持 disable 单个 server，写入 `enabled: false` 并从当前 session 移除工具。
- 支持 enable 单个 server，写入 `enabled: true` 并进入 reconnect / pending。
- shutdown 时清理 stdio 子进程和 HTTP session。

失败策略：

- 可选 server 失败：记录 warning，工具不暴露。
- 所有 server 失败：TUI 仍可启动，只显示 warning。
- `.mcp.json` 语法错误：TUI 启动 warning，禁用 MCP；`acn mcp status` 返回错误。

### ToolRegistry 接入

`ToolRegistry` 增加：

```rust
mcp_manager: Option<Arc<McpConnectionManager>>
```

新增 builder：

```rust
with_mcp_manager(...)
```

`definitions()`：

- 先保留现有内置工具。
- 再追加 ready MCP tools。

`dispatch_with_context()`：

- 如果工具名匹配 MCP visible name，交给 MCP manager。
- 其他工具保持现有 match 分发。

### Turn Loop 接入

本需求不改 provider-neutral turn loop 的基本结构。

可以增强 summary：

- started：`mcp pal/ask {...}`
- progress：`mcp pal/ask 50/100 <short message>`
- completed：`mcp pal/ask ok`
- failed：`mcp pal/ask failed <short error>`

避免把长参数和大结果刷进 TUI tool cell。

### TUI 接入

新增 `/mcp` slash command：

```text
/mcp
```

打开 MCP 面板。当前以键盘操作为主，鼠标点击可后续增强。

### `/mcp` live panel 约束

`/mcp` 是 active-turn live panel。模型流式输出、内置工具或 MCP 工具执行期间仍可打开，面板按键优先于 turn 取消与 composer 输入；关闭后回到原 turn。它与 `/ps`、`/subagents`共用全高 live 区域，同一时刻只展示一个。Session picker 等独占视图打开时不抢占。

原因：

- 当前 provider request 使用发送瞬间冻结的 tool snapshot；面板操作只影响后续 request。
- Reconnect / Enable / Disable 会更新 server 状态和后续工具集合，运行中状态通过事件即时重绘。
- 面板导航拥有按键优先级，不会把 Esc、方向键或确认键误送给 active turn。

重绘策略：

- MCP 状态变化通过 `McpStatusUpdated` 类事件进入 TUI state。
- 更新 server/tool snapshot 后标记 MCP panel、status line、tool registry summary dirty。
- 如果 MCP panel 正在打开，只重绘 panel 区域和必要状态行；transcript 不追加诊断内容，避免污染对话历史。
- 如果当前不在 MCP panel，仅更新 status/warning 和后续 provider request使用的 tool snapshot。

### Enable / Reconnect 异步交互

Enable / Reconnect 可能需要启动子进程、HTTP 初始化、`tools/list` 和 schema 转换，因此按异步任务处理。

触发 Enable：

1. 立即写入 selected upstream runtime 下 `.mcp.json` 的 `enabled: true`。
2. server 状态进入 `reconnecting`。
3. 后台执行 initialize / `tools/list` / 过滤 / schema 转换 / visible name 映射。
4. 成功后状态变 `ready`，更新 tool snapshot。
5. 失败后状态变 `failed`，配置仍保持 `enabled: true`，工具不暴露。

触发 Reconnect：

1. 不修改配置。
2. server 状态进入 `reconnecting`。
3. 后台重建连接并刷新工具列表。
4. 成功后替换该 server 的 tool snapshot；失败后保留 failed 状态和 last error。

等待期间：

- MCP 面板保持可见。
- 用户可以退出 MCP 面板。
- 用户可以浏览其他 server。
- 用户可以对其他非 busy server 操作。
- 当前 `reconnecting` server 的 `View tools`、`Enable`、`Reconnect` 暂时禁用。
- Disable 允许作为取消语义：写入 `enabled: false`，并请求取消/清理正在进行的reconnect。
- 普通 prompt 不进入 MCP panel queue。

如果用户退出 MCP 面板：

- reconnect 继续在后台运行。
- 完成后只更新 status line / warning / tool snapshot。
- 不往 transcript 追加诊断内容。

provider request 规则：

- 后续 provider request 只使用发送瞬间状态为 `ready` 的 MCP tool snapshot。
- `reconnecting`、`failed`、`disabled` server 的工具都不进入 `tools` 字段。
- 如果用户想确保新启用的 MCP tools 生效，需要等 server 显示 `ready` 后再发送prompt。

### MCP server list view

显示：

- MCP config path。
- server 数量。
- 每个 server 的 name、transport、状态、工具数、错误摘要。
- 状态包括 `ready`、`failed`、`disabled`、`starting`、`reconnecting`。

操作：

- `Enter`：进入选中 server detail。
- `r`：Reconnect 选中 server。
- `d`：Disable 选中 enabled server；Enable 选中 disabled server。
- `Esc`：关闭 MCP 面板。

### MCP server detail view

展示只读安装信息：

- server name。
- transport。
- config path。
- stdio：`command`、`args`、`cwd`、`env` key 列表、`env_vars` key 列表。
- streamable_http：`url`、`bearer_token_env_var` 名称。
- timeout：`startup_timeout_secs`、`tool_timeout_secs`。
- filters：`enabled_tools`、`disabled_tools`。
- last connected at / last error。

安全要求：

- 默认不展示 env 的 value，只展示 key。
- 如果 `env` 中存在 value，也在 TUI 中显示为 `<redacted>`。
- 不展示 bearer token 明文，只展示 env var 名称。

操作：

- `v`：View tools。
- `r`：Reconnect。
- `d`：Disable 或 Enable。
- `Esc`：返回 server list。

### MCP tool list view

显示选中 server 发现到的全部工具列表：

- `Tool name`：raw MCP tool name。
- `Full name`：模型可见全名，例如 `mcp__pal__ask`。
- title / description 短摘要。
- 状态 chip：
  - `exposed`：绿色；已进入后续 provider request 的 `tools` 字段。
  - `filtered`：黄色；被 `enabled_tools` / `disabled_tools` 配置过滤。
  - `unsupported`：红色或 warning 色；因 schema 无法转换等原因当前 ACN 不能暴露。

操作：

- `Enter`：进入 tool detail。
- `Esc`：返回 server detail。

### MCP tool detail view

只读展示单个工具信息：

- `Tool name`。
- `Full name`。
- 暴露状态：`exposed` / `filtered` / `unsupported`，使用与 list 一致的颜色。
- 未暴露原因，例如 `disabled_tools`、`not_in_enabled_tools`、`invalid_schema`。
- title。
- description。
- input schema。
- 参数列表，包括参数名、类型、required、description、default / enum。
- output schema，如 MCP server 提供。
- annotations，如 MCP server 提供。

展示原则：

- 参数 schema 用折叠/滚动区域展示，避免撑爆屏幕。
- 对超长 description 和 schema 做截断，并提供展开查看。
- 这是只读诊断界面，不允许在这里直接调用工具。
- `filtered` / `unsupported` 工具可以展示 `Full name` 用于排障，但不会进入provider request 的 `tools` 字段。

TUI 启动时：

- MCP 初始化进入 session startup 阶段。
- 启动失败 server 通过 system status/warning 展示一次。
- 不阻塞用户继续使用非 MCP 工具。

MCP tool cell：

- 对 `mcp__pal__ask` 展示为 `mcp pal/ask`。
- running 时展示 elapsed time。
- 若收到 progress notification，展示最近一条 progress message 和 progress/total。
- 完成后只显示状态和短摘要。

## 分阶段计划

### Phase 0: PRD 和边界确认

- [x] 写入本 PRD。
- [x] 确认 selected upstream runtime 配置位置：`<base_acn_home>/<upstream>/.mcp.json`。
- [x] 确认不做项目级配置；OAuth 仅覆盖 Streamable HTTP 的 authorization code + PKCE 登录，桌面使用 loopback callback，headless 使用完整 redirect URL 粘贴。

验收：

- [x] PRD 明确配置、CLI、TUI、tool 注入、失败策略和验证计划。

### Phase 1: `.mcp.json` 配置和 CLI 管理

- [x] 新增 `src/mcp/config.rs`。
- [x] 定义 `.mcp.json` DTO：
  - `McpJsonConfig`
  - `McpServerConfig`
  - `McpTransportConfig`
- [x] 支持读取缺失文件为空配置。
- [x] 支持 JSON 解析错误带路径和上下文。
- [x] 支持原子写 JSON。
- [x] 新增 server name 校验。
- [x] 新增 `acn mcp list`。
- [x] 新增 `acn mcp get <name> [--json]`。
- [x] 新增 `acn mcp add <name> -- <command...>`。
- [x] 新增 `acn mcp add <name> --url <url> [--bearer-token-env-var ENV] [--oauth-client-id ID] [--oauth-callback-port PORT] [--oauth-credentials-store keyring|file]`。
- [x] 新增 `acn mcp add-json <name> '<server-json>'`，支持单 server DTO 和 `http` 输入别名。
- [x] 新增 `-e KEY=VALUE` 和 `--env-var KEY`。
- [x] 新增 `acn mcp remove <name>`。
- [x] 新增 `acn mcp disable <name>`，写入 `enabled: false`。
- [x] 新增 `acn mcp enable <name>`，写入 `enabled: true`。
- [x] `acn mcp` 子命令支持 `--config <path>`，用于定位 `acn_home`。
- [x] 更新 `acn --help`。
- [x] 更新 `docs/config_parameters.md` 中 MCP 配置说明。

验收：

- [x] 缺失 `.mcp.json` 时 `list` 显示空列表。
- [x] add/get/list/remove 能 round trip。
- [x] server name 非法时拒绝写入。
- [x] `-e` 写入 `env`，`--env-var` 写入 `env_vars`。
- [x] 不把 bearer token 明文写入 `.mcp.json`。
- [x] enable / disable 只切换 `enabled` 字段，不删除 server 配置。

### Phase 2: MCP client 和 connection manager

- [x] 引入 MCP Rust SDK 依赖，优先复用成熟 transport 实现，不手写 JSON-RPC。
- [x] 实现 stdio client：
  - async 启动子进程
  - stdin/stdout 走 MCP transport
  - stderr 不污染 TUI，写入日志或错误详情
  - shutdown 时清理子进程
- [x] 实现 Streamable HTTP client：
  - 支持 `url`
  - 支持 `bearer_token_env_var`
  - 超时和错误分类清晰
- [x] 实现 `initialize`。
- [x] 实现 `tools/list`。
- [x] 实现 `tools/list` pagination。
- [x] 实现 `tools/call`。
- [x] 实现 startup timeout 和 tool timeout。
- [x] 实现 tool allowlist / denylist。
- [x] 明确不支持 MCP Tasks、不宣告 `2026-07-28` Tasks extension；普通旧协议工具继续兼容，legacy task-required 工具不保证识别或自动过滤。
- [x] Streamable HTTP 支持 `MCP-Protocol-Version` 和 `MCP-Session-Id`。
- [x] Streamable HTTP 支持 request-scoped SSE notification。
- [x] 实现状态快照：
  - disabled
  - starting
  - ready
  - failed
- [x] 实现 `acn mcp status [name]`，真实连接并展示工具列表摘要。

验收：

- [x] 能连接测试 stdio MCP server 并列出工具。
- [x] 能连接测试 Streamable HTTP MCP server 并列出工具。
- [x] server 启动超时返回清晰错误。
- [x] stdio server stderr 被捕获，不直接写入 TUI。
- [x] disabled server 不启动。

### Phase 3: 工具注入和实际调用

- [x] 新增 `src/mcp/name.rs`。
- [x] 实现 `mcp__server__tool` visible name 生成和解析。
- [x] 实现归一化冲突处理。
- [x] 新增 `src/mcp/tool.rs`。
- [x] 将 MCP tool input schema 转成 ACN `ToolDefinition`。
- [x] 遵循 MCP `2025-11-25` 的 JSON Schema 2020-12 默认语义。
- [x] 修正缺失 `properties` 的 schema；无参数工具使用 object schema，避免provider 拒绝。
- [x] `ToolRegistry` 增加 MCP manager。
- [x] `definitions()` 追加 ready MCP tools。
- [x] `dispatch_with_context()` 路由 MCP tool。
- [x] MCP result 转成标准 JSON：

```json
{
  "content": [],
  "structured_content": {},
  "is_error": false,
  "meta": {}
}
```

- [x] MCP `is_error = true` 时映射成失败 tool_result。
- [x] 对大结果做短摘要，避免 TUI cell 刷屏。

验收：

- [x] provider request 的 `tools` 包含 MCP tools。
- [x] 模型发起 MCP tool_use 后能成功调用 MCP server。
- [x] raw MCP tool 名和 visible tool 名映射正确。
- [x] 未 ready server 的工具不会出现在 `tools` 字段。
- [x] MCP tool 失败能作为 tool_result 返回模型，而不是 panic。

### Phase 4: TUI 状态展示

- [x] 新增 `/mcp` slash command。
- [x] `/mcp` 可在 active turn 中打开，但不抢占 session picker 等独占视图。
- [x] `/help` 展示 `/mcp`。
- [x] TUI 启动时展示 MCP warning。
- [x] `SessionEngine` 或 TUI state 持有 MCP status snapshot。
- [x] 实现 MCP server list view。
- [x] 实现 MCP server detail view，展示只读安装信息并隐藏敏感值。
- [x] 实现 View tools 的 tool list view，展示全部 discovered tools。
- [x] 实现 tool detail view，展示名称、描述、参数 schema、output schema 与 annotations。
- [x] 实现 Reconnect action，并刷新当前 session 的 tools snapshot。
- [x] 实现 Disable action，写入 `enabled: false` 并从当前 session 移除工具。
- [x] 实现 Enable action，写入 `enabled: true` 并进入 reconnect / pending 流程。
- [x] Enable / Reconnect 等待期间保持 MCP panel 可见，并禁用当前 server 的View tools / Enable / Reconnect。
- [x] Disable 可取消正在 reconnect 的 server，并持久写入 `enabled: false`。
- [x] 普通 prompt 不进入 MCP panel queue；provider request 按发送瞬间 ready snapshot 决定 tools。
- [x] 实现 tool 状态 chip：`exposed` 绿色、`filtered` 黄色、`unsupported`红色或 warning 色。
- [x] Tool detail 使用 `Tool name` / `Full name` 命名。
- [x] MCP 状态变化触发局部重绘和 tool snapshot invalidation。
- [x] Tool cell 将 `mcp__server__tool` 显示为 `mcp server/tool`。
- [x] Tool cell 展示 running elapsed time。
- [x] Tool cell 展示 progress notification 的最近一条消息。
- [x] 完成摘要只显示状态和短输出。
- [x] 为 `/mcp` 渲染和 tool cell 显示补单元测试。

验收：

- [x] 有失败 MCP server 时 TUI 仍能进入。
- [x] busy 状态下 `/mcp` 仍能打开 live panel，关闭后 active turn 继续。
- [x] `/mcp` 能看到 server 连接状态和失败原因摘要。
- [x] 能从 server list 进入 server detail。
- [x] 能从 server detail 进入 tools list。
- [x] 能从 tools list 进入只读 tool detail。
- [x] tool list 中 `exposed` / `filtered` / `unsupported` 颜色一眼可分。
- [x] tool detail 能看到 `Tool name` 和 `Full name`。
- [x] TUI 不展示 env value 或 bearer token 明文。
- [x] Reconnect 能刷新状态和工具列表。
- [x] Disable 能持久写入 `enabled: false`，并让该 server 的工具从后续provider request 中消失。
- [x] Enable 能持久写入 `enabled: true`，并尝试重连恢复工具。
- [x] Enable / Reconnect 等待期间用户可以退出面板或浏览其他 server。
- [x] reconnecting server 的工具不会进入 provider request。
- [x] prompt 不会排队等待 reconnect 完成。
- [x] MCP tool 调用中/完成后的 TUI 展示可读。

### Phase 5: 文档和示例

- [x] 更新 `docs/user_guide.md` 或新增 MCP 使用小节。
- [x] 增加本地 stdio 示例。
- [x] 增加远程 Streamable HTTP 示例。
- [x] 增加 `pal` 示例。
- [x] 说明 `-e` 与 `--env-var` 的安全区别。
- [x] 说明 OAuth login/logout 的适用范围，并说明 elicitation 暂不支持。
- [x] 说明修改 `.mcp.json` 后需要重启 TUI。
- [x] 说明 `acn mcp enable/disable` 与 TUI Enable/Disable 都是持久配置开关。
- [x] 说明外部 CLI 修改 `.mcp.json` 不热加载已运行 TUI；需重启或后续实现 reload。
- [x] 说明 `/mcp` 可在 active turn 中打开，且面板操作只影响后续 provider request。

验收：

- [x] 用户能照文档新增一个本地 MCP server。
- [x] 用户能照文档新增一个远程 bearer token MCP server。
- [x] 文档明确不应把真实 token 写入 `.mcp.json`。
- [x] 文档说明 Enable/Disable 不删除配置，只切换 `enabled`。

### Phase 6: 测试和验证

- [x] 单元测试：
  - `.mcp.json` 缺失 / 解析 / 写入 round trip
  - server name 校验
  - env / env_vars CLI 解析
  - enable / disable 写入 `enabled` 字段且保留 server 配置
  - visible name 归一化和冲突处理
  - MCP tool filter
  - MCP result 转 tool_result
  - OAuth discovery scope 与 `resource` 参数
  - progress notification 转 TUI event
  - TUI active-turn panel 优先级与独占视图判定
  - reconnecting server 的交互禁用状态
- [x] 集成测试：
  - stdio mock MCP server tools/list
  - stdio mock MCP server tools/call
  - stdio mock MCP server progress notification
  - Streamable HTTP mock MCP server tools/list
  - Streamable HTTP mock MCP server tools/call
  - Streamable HTTP mock MCP server request-scoped SSE progress notification
  - failed server 不阻塞 ToolRegistry
  - provider request 包含 MCP tool definition
  - disable 后 provider request 不再包含该 server 的工具
  - enable + reconnect 后 provider request 重新包含该 server 的工具
  - reconnecting 期间 provider request 不包含该 server 的工具
- [x] TUI 测试：
  - `/mcp` slash command 匹配和 help 展示
  - busy 状态下 `/mcp` 打开 live panel，关闭后恢复原 turn
  - MCP warning 展示
  - MCP server list / detail 渲染
  - MCP tool list / detail 渲染
  - `exposed` / `filtered` / `unsupported` 状态颜色渲染
  - Enable / Disable / Reconnect 后 panel 局部重绘
  - Enable / Reconnect 等待期间当前 server 操作禁用
  - prompt 不进入 MCP panel queue
  - MCP tool cell 显示名称压缩
  - MCP tool cell progress 展示
- [x] CLI 测试：
  - `mcp add/add-json/list/get/remove/enable/disable`
  - `mcp status` 成功和失败输出

完整验证：

- [x] `cargo fmt`
- [x] `cargo clippy -- -D warnings`
- [x] `cargo test`
- [x] `cargo check`
- [x] 针对 MCP 的 mock server 集成测试
- [x] TUI smoke test：
  - 启动 TUI
  - `/mcp`
  - busy turn 中输入 `/mcp` 验证 live panel 与按键优先级
  - 发起能触发 MCP tool 的简单 turn
  - 验证 tool cell 和 tool_result

### Phase 7: code-review skill 验证

完成实现和基础验证后，按 code-review skill 检查以下风险域：

- [x] 配置和 CLI
  - `src/mcp/config.rs`
  - `src/bin/acn.rs`
  - `.mcp.json` 原子写
  - `enable` / `disable` 持久语义
  - CLI 参数解析和帮助文案
- [x] MCP transport 和 lifecycle
  - stdio 子进程管理
  - Streamable HTTP client
  - timeout / shutdown / stderr
  - status 快照
- [x] ToolRegistry 和 turn loop 接入
  - tool definitions 注入
  - visible/raw name 映射
  - tool dispatch
  - MCP result -> tool_result
- [x] TUI 展示
  - `/mcp`
  - active-turn panel gating
  - startup warning
  - server/tool detail drill-down
  - `exposed` / `filtered` / `unsupported` 颜色和原因
  - Enable / Disable / Reconnect 后的局部重绘
  - tool cell 渲染
  - 不刷屏和截断
- [x] 测试覆盖和安全边界
  - token 不落明文
  - OAuth scope/resource 与 elicitation 错误清晰
  - failed server 不阻塞 TUI
  - mock MCP server 覆盖足够

验收：

- [x] code-review skill 覆盖上述风险域。
- [x] 不存在未处理的高风险问题。
- [x] 完整验证通过。

## 风险和注意事项

- stdio MCP server 是本地子进程，权限等同 ACN 进程；文档必须提醒用户只添加可信 server。
- `-e KEY=VALUE` 会把 value 写入 selected upstream runtime 下的 `.mcp.json`；真实 token 推荐用 `--env-var`。
- 远程 MCP server 的错误、超时、认证失败需要短错误回灌给模型，并在 TUI 中可诊断。
- MCP tool schema 来自外部 server，必须做基本结构修正和大小限制，避免超长描述撑爆上下文。
- MCP tool 名可能与内置工具或其他 MCP server 冲突，必须使用 `mcp__server__tool` 和冲突 hash。
- 原始设计中的 `.mcp.json` 是全局配置；合入 upstream 隔离后，agent / `acn mcp` 都先激活 selected upstream，因此实际位置为 `<base_acn_home>/<upstream>/.mcp.json`，不同 upstream 不共享。

## 附录：ACN 怎样连接不同版本的 MCP Server

这里有两种版本，含义不同：

- `rmcp 3.0.1` 是 ACN 内部使用的 Rust SDK 版本。
- `2025-03-26`、`2025-11-25`、`2026-07-28` 是 ACN 与 MCP server 在网络上说的协议版本。

升级 `rmcp` 不会要求所有 MCP server 一起升级。ACN 建连时由 SDK 和 server 协商共同支持的协议版本，不让用户在 `.mcp.json` 中手动填写版本。

```text
新 server：ACN 请求 2026-07-28，server 也支持 → 使用 2026-07-28
旧 server：ACN 先发 server/discover，明确收到“不支持”
          → 再以 2025-11-25 发 initialize
          → server 选择 2025-03-26 → 后续按 2025-03-26 通信
```

连接新 server 时，ACN 先请求 `server/discover`；当前新协议使用 `2026-07-28`。连接旧 server 时，对方返回 JSON-RPC `Method not found`，或在尚无 session 时以 HTTP `400` / `404` 和非 JSON-RPC body 拒绝 discovery，ACN 会退回传统的 `initialize` 流程；其他认证、限流、服务端和网络错误不会触发协议降级。旧 server 会在初始化响应中选定实际版本，ACN 随后按该版本发送 `tools/list`、`tools/call` 和 progress 消息；新协议专属字段不会发给旧 server。

如果双方没有共同版本，ACN 应明确报协议不兼容，而不是尝试发送可能被误解的数据。

协议协商和 OAuth 是两件事。前者只决定 MCP 消息格式；后者决定 token 申请给哪个 resource。即使 ACN 回退到旧的 `initialize` 协议，OAuth 仍使用 `rmcp 3.0.1` 的实现：从 protected resource metadata 取到的 `resource` 会同时用于授权、换 token 和 refresh token。因此“旧协议回退”不会把 refresh token 也回退到旧 SDK 的丢值行为。若 server 自己没有提供 resource metadata，SDK 才按规范使用 MCP URL 作为 fallback；这时失败会作为认证错误明确返回，不会静默换一种协议重试。
