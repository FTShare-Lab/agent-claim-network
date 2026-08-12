# 配置参数说明

本文说明 `config.template.toml` / `config.toml` 的配置方法。实际运行配置文件只保留常用字段和值；完整字段含义、默认值和注意事项统一放在这里维护。

阅读建议：

- 日常运行交互式 `acn`、接 LLM、接 MCP、调工具、调 session 行为时，优先看“Agent 侧配置”。
- 部署或调试 router / maintainer daemon 时，再看“Router / Maintainer 侧配置”。
- 不确定某个字段是否会影响 agent 时，先看它在哪个大节下面；服务侧字段通常不会改变单个 agent 的对话行为。

## 配置文件位置

启动时配置文件优先级为：

1. `--config <path>`
2. `ACN_CONFIG` 环境变量
3. `<acn_home>/config.toml`

默认 `acn_home` 为 `~/.acn`。如果没有指定 `--config` / `ACN_CONFIG`，且 `~/.acn/config.toml` 不存在，程序会用随 binary 编译的 `config.template.toml` 内容初始化该文件。生成的配置只列出 Agent、Router 与 Maintainer 首次使用或部署时通常需要修改的字段；其他未写出的字段继续使用代码默认值，需要调整时再参考本文补充。默认 upstream 固定为 `default`，其 `agent_id`、`acn_key_env`、`maintainer_endpoint`、`router_endpoint` 都写为空字符串。首次运行交互式 `acn` 会先生成配置，然后因 `agent_id` 为空报错退出；填写 `agent_id` 后即可使用不连接团队服务的单人模式，`acn_key_env` 和两个 endpoint 可以留空或省略。Router / Maintainer daemon 不校验 Agent upstream，可直接读取同一文件中的服务端配置。

若需要连接团队服务，必须填写 `maintainer_endpoint` 和 `router_endpoint`，`acn_key_env` 在团队未开启鉴权模式时可以留空或省略。

`upstream` 是 Agent 侧的一份运行与团队连接配置，决定 Agent 身份、团队服务地址、凭据来源和本地私有数据目录；名称只是本机别名，不是服务端团队 ID。交互式 `acn` 可通过 `--upstream <name>` 指定要使用的配置；不传时使用配置顶层 `upstream` 的值。顶层 `upstream` 可省略或留空，但这种情况下启动时必须显式传 `--upstream <name>`。

## Agent 侧配置

本节是交互式 agent、session、LLM、工具、MCP、附件，以及 agent 访问 router / maintainer 时最常用的配置。

### `upstream` / `[upstreams.<name>]`

- `upstream`：Agent 默认使用的运行与团队连接配置名称，例如 `dev`。如果启动时传了 `--upstream <name>`，则 CLI 参数优先生效；缺省时必须通过 CLI 指定。自动生成的配置使用 `default`。
- `agent_id`：当前 agent 的唯一标识，不能为空且必须匹配 `^[a-z0-9_-]+$`。交互式 `acn` 不再接受 `--agent`，agent 身份只来自选中的 upstream。自动生成配置中的空值必须填写。
- `maintainer_endpoint`：访问 maintainer daemon 的 base URL。与 `router_endpoint` 同时配置时启用团队模式；两者同时留空或省略时进入单人模式，不连接 maintainer、不同步 inbox/claim/dispute，也不维护待补传队列。
- `router_endpoint`：访问 router daemon 的 base URL。与 `maintainer_endpoint` 必须同时配置或同时留空；单人模式不获取 router overview，也不暴露 `consult_router` 工具。
- `acn_key_env`：唯一支持的 upstream 团队鉴权 key 配置字段，值为环境变量名，可以留空或省略。这是由团队管理员提供的服务访问凭据，与主 LLM key 和 Web Search key 无关。对应环境变量不存在/为空时，agent 不阻塞启动；团队模式请求仍会带鉴权信封，但鉴权值为空字符串，启用鉴权的服务会拒绝该请求。

示例：

```toml
upstream = "demo"

[upstreams.demo]
agent_id = "agent-a"
acn_key_env = "ACN_AUTH_KEY"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"
```

仅本地运行时可以省略两个 endpoint：

```toml
upstream = "local"

[upstreams.local]
agent_id = "agent-a"
```

单人模式切换到团队模式后，只同步切换后的新产物；不会静默补传单人模式期间形成的历史数据。历史数据如需导入，应通过后续提供的显式同步流程完成。

### `[storage]`

- `acn_home`：ACN base 目录。默认值为 `~/.acn`；目录不存在时启动时自动创建。交互式 Agent、supervisor、MCP 和 session 命令先选择 upstream，再把 Agent 私有状态放到 `<acn_home>/<upstream>/...`。Router 与 Maintainer daemon 不解析 Agent upstream，团队数据固定放在各自 `<acn_home>/data/team`。

相关存储路径：

- MCP 配置：交互式 agent / `acn mcp` 激活 selected upstream 后使用 `<acn_home>/<upstream>/.mcp.json`。MCP server 不写入 `config.toml`；CLI 通过 `--config <path>` 定位 base `acn_home`，再通过 `--upstream <name>` 或默认 upstream 选择 runtime。
- skills：`<acn_home>/<upstream>/skills/<skill-name>/SKILL.md`
- Markdown 指令：`<acn_home>/<upstream>/ACN.md`；文件不存在或为空时不注入。新建 session 时其内容会追加到 `prompts/agent_system.j2` 渲染结果末尾，已存在 session resume 继续使用当时固化的 `system_prompt.md`。
- agent 本地存储：`<acn_home>/<upstream>/data/agents/<agent-id>`
- daemon 中心存储：`<acn_home>/data/team`。Router / Maintainer 在此读写中心视图，例如 `router/derived_views.yaml`、`maintainer/policies/`、`maintainer/disputes/`、`maintainer/outbox/`。它不属于任何 Agent upstream。

旧版本已有的 Agent 本地数据会在通过 `acn` 用户入口首次激活 upstream runtime 时自动迁移到选中的 `<acn_home>/<upstream>/...`。迁移范围包括 `.mcp.json`、`ACN.md`、`skills/` 和当前 `agent_id` 对应的 `data/agents/<agent-id>`；`config.toml` 与 daemon 的 `data/team` 始终留在 base `acn_home`，目标已存在时不会覆盖。Router / Maintainer daemon 不解析 Agent upstream，也不检查或修改其中的目录。

若曾使用过会把团队数据写入 `<acn_home>/<upstream>/data/team` 的旧版本，应先停止 Agent、Router 和 Maintainer，再把该目录迁到 `<acn_home>/data/team`：目标不存在时可以整体移动，目标已存在时必须人工合并，不能直接覆盖。Agent 激活对应 upstream 时会删除遗留的普通空目录；若目录非空或路径链包含 symlink，则拒绝启动并保留原数据。

工具工作目录不写入配置。交互式 agent 通过 `--cd <dir>` 指定工具相对路径与命令默认 cwd；未指定时使用启动程序时的 cwd。`--cd` 支持绝对路径、相对路径和 `~`，解析后必须是已存在目录。


### `[agent.llm]`

- `provider`：agent 主对话 LLM provider。模板默认推荐 `openai_responses`，也支持 `openai_chat` 和 `anthropic`。Chat 与 Responses 是彼此独立的 wire protocol，ACN 不在两者之间自动降级。
- `endpoint`：与所选 provider 兼容的 LLM HTTP 地址，必须是绝对 HTTP(S) URL。可以填写服务 base URL，也可以填写完整请求 URL；OpenAI-compatible 的常见 base URL 形如 `https://llm.example.com/v1`，Anthropic-compatible 的常见 base URL 形如 `https://llm.example.com`。根 URL 会分别补全为 `/v1/chat/completions`、`/v1/responses` 或 `/v1/messages`；已有路径的 base URL 会追加相应末段，完整请求 URL 保持不变。
- `model`：模型名，以配置文件为准。
- `supports_websockets`：可选，默认 `false`。仅 `openai_responses` 可设为 `true`；请只在 endpoint 明确支持 Responses WebSocket 协议时开启。
- `reasoning_effort`：控制 agent 主 LLM 的推理强度，可选值为 `none`、`low`、`medium`、`high`、`xhigh`、`max`，未配置时默认 `none`。未配置或设为 `none` 时不发送推理强度参数。
- `anthropic_thinking`：只作用于 `provider = "anthropic"`，可选值为 `auto`、`enabled`、`adaptive`、`disabled`，默认 `auto`。`auto` 不发送 `thinking`，沿用上游默认行为；其他值显式发送对应 `thinking.type`。不作用于 Responses 或 Chat。
- `anthropic_thinking_budget_tokens`：只作用于 `anthropic_thinking = "enabled"` 的可选 `thinking.budget_tokens`。未配置时不发送；选择 `adaptive`、`disabled` 或 `auto` 时也不发送。它不从 `reasoning_effort` 或 `max_tokens` 推导。
- `api_key_env`：读取 agent LLM API key 的环境变量名，默认空字符串；真实 provider 必须填写。
- `max_tokens`：单次模型响应的最大输出 token 数，默认 `65536`。
- `context_window`：模型上下文窗口 token 数，默认 `200000`，必须大于 0。用于 TUI ctx 总量展示以及自动压缩阈值计算。
- `timeout_secs`：单次 LLM 请求超时，默认 `300` 秒。
- `retry_count`：首次失败后的额外重试次数，默认 `1`。`0` 表示一共只尝试一次，`1` 表示一共最多尝试两次。
- `retry_base_delay_ms`：重试退避基础间隔，默认 `200`ms。第 N 次等待约为 `base * 2^(N-1)`，并叠加随机抖动。
- `retry_max_delay_ms`：重试退避等待上限，默认 `5000`ms。

特别说明：`openai_chat` 会丢弃厂商扩展 Reasoning 字段，要求 Reasoning 回传的模型应改用 `openai_responses` 或 `anthropic`。

### `[agent.inbox]`

- `processing_stale_after_secs`：agent 本地 inbox processing lease 的 stale 阈值。超过该秒数仍未 ack 的 `*.processing.*.yaml` 会在下次扫描前恢复为 pending，以便重试。

### `[agent.session]`

- `id_mint_max_retries`：session id 创建时的最大重抽次数。总尝试次数为 `1 + id_mint_max_retries`。
- `notify_on_finalize_completion`：后台 supervisor finalize 完成后是否发送系统通知。默认 `true`；设为 `false` 后成功和失败都只写 job/session 日志，不弹系统通知。
- `cleanup_retention_days`：旧 Closed sessions 的保留天数，默认 `30`，最大 `36500`。自动后台清理按该值判断，`0` 表示禁用自动后台清理；手动 `acn session cleanup` 不受此禁用影响，配置为 `0` 时仍按默认 30 天判断，配置为非 0 时按配置值判断。

ACN 会用有效配置、选中的 upstream 和 finalize 所需凭据摘要生成 supervisor 运行环境指纹。相关内容变化后，下次启动会接管旧 supervisor；该行为无需配置额外参数，凭据明文不会写入指纹或状态输出。工具工作目录不参与指纹，`--cd` 只属于交互式 agent，所有 `acn supervisor` 子命令均不接受该参数。

### `[agent.session.compaction]`

- `summary_max_chars`：session 历史压缩 summary 最大字符数，默认 `40000`。
- `auto_compact_ctx_ratio`：provider request preflight 触发自动压缩的 ctx 使用比例，取值范围 `0.0` 到 `1.0`，默认 `0.80`。`0.0` 表示不触发自动压缩；其他值的触发阈值为 `[agent.llm].context_window * auto_compact_ctx_ratio`。preflight 发生在发起 provider request 之前，因此触发判断使用本地 token 估算。
- `tail_target_ctx_ratio`：compact 后 raw tail 的 soft target，占 `[agent.llm].context_window` 的比例，默认 `0.20`，必须大于 `0.0` 且不超过 `1.0`。
- `tail_hard_ctx_ratio`：compact 后 `raw tail + runtime-only projection` 的 hard limit，占 `[agent.llm].context_window` 的比例，默认 `0.30`，必须大于 `0.0` 且不超过 `1.0`。
- `tail_previous_real_user_turns`：compact 后尽量保留的最近 previous real user turn 数量，默认 `4`，允许范围 `1..=5`。
- `tool_result_raw_max_chars`：单个 tool result 允许进入 raw tail 的最大字符数，默认 `4096`。超过后进入 summary，不保留大段 raw preview。

### `[agent.session.memory_review]`

- `interval_turns`：后台 fork memory review 的触发间隔；触发时也只 review 最近同样数量的 user turn。必须大于 0。

### `[agent.session.subagents]`

- `max_concurrent`：同一个 parent session 内最多同时 running 的 subagent 数量。超过后进入 queued，等待前面的 subagent 结束再启动。默认 `6`，必须大于 0。
- `max_tool_loop_turns`：单个 subagent 在一次委托中最多执行的 tool-use 回环轮数，用于避免无人值守任务无限调用工具；达到上限后本次 subagent 执行失败并进入终态，其仍在运行的后台进程随 owner 清理。默认 `256`，必须大于 0。
- `wall_timeout_secs`：单个 subagent 从 queued 变成 running 后的总运行墙钟超时。queued 等待时间不计入；running 期间收到 `steer_subagent` 不会重置计时。默认 `7200`，必须大于 0。

### `[agent.session.subagents.wait]`

`wait_subagents` 使用这一组配置约束单次等待。配置在 ACN 进程启动时加载；修改配置文件后需要重启，已运行进程不会热更新。

- `default_timeout_secs`：调用 `wait_subagents` 时未传 `timeout_secs` 使用的默认等待时间。默认 `30` 秒。
- `min_timeout_secs`：工具参数 `timeout_secs` 可接受的最小值。默认 `10` 秒。
- `max_timeout_secs`：工具参数 `timeout_secs` 可接受的最大值。默认 `3600` 秒。

三者必须满足 `0 < min_timeout_secs <= default_timeout_secs <= max_timeout_secs`。该限制只约束主 agent 的单次 `wait_subagents` 调用；不改变 subagent 自身从 running 开始计算的 `wall_timeout_secs`。

### `[agent.session.turn_journal]`（建议保持默认值）

- `delta_snapshot_interval_ms`：assistant streaming delta 写入 `turn_events.jsonl` 的时间合并阈值，默认 `500`，必须大于 0。
- `delta_snapshot_chars`：assistant streaming delta 写入 `turn_events.jsonl` 的字符合并阈值，默认 `1024`，必须大于 0。
- `recovery_original_user_request_max_chars`：构造 `<interrupted_turn_context>` 时保留原始用户请求的最大字符数，默认 `8192`，必须大于 0。
- `recovery_partial_assistant_max_chars`：构造 `<interrupted_turn_context>` 时保留 partial assistant 文本的最大字符数，默认 `8192`，必须大于 0。
- `recovery_tool_input_max_chars`：写入 turn journal 与构造 recovery context 时保留工具输入/待处理工具摘要的最大字符数，默认 `2048`，必须大于 0。
- `recovery_tool_output_max_chars`：写入 turn journal 与构造 recovery context 时保留工具输出摘要的最大字符数，默认 `4096`，必须大于 0。
- `recovery_user_steer_max_chars`：构造 recovery context 时保留用户中途引导文本的最大字符数，默认 `8192`，必须大于 0。

这些配置只影响 turn journal 的 snapshot 和下一轮 LLM recovery projection。`turn_events.jsonl` 保留可重建的中间行为时间线；tool input/output 在 journal 中保存带 `truncated` 标记的有界预览，不承诺完整 payload。compact 超限恢复发生过重型 block 外置时，journal 只保存有上限的 session 资产路径与哈希，不复制附件正文或媒体 base64。session_search、compact、finalize、memory_review 仍以 `messages.jsonl` 为权威来源。

### `[agent.session.skills]`

- `max_body_bytes`：用户显式 `/skill` 时，单个 `SKILL.md` 正文允许注入当前 turn 的最大字节数。默认 `262144`（256 KiB），必须大于 0；超限时在调用 provider 前明确失败。
- `max_per_turn`：当前 turn 允许显式注入的去重后 skill 数量上限。默认 `8`，必须大于 0；按用户输入的首次出现顺序计算。

### `[agent.session.user_shell]`

- `enabled`：是否允许 TUI 使用 `!cmd` 运行本地 shell 命令。默认 `true`。
- `timeout_secs`：一次 `!cmd` 的整体最长执行时间，从父 shell 启动时开始计时，同时覆盖命令运行和退出后的 stdout/stderr 收尾；超时后标记为 `timed_out` 并清理相关进程。必须大于 0，默认 `180` 秒。父 shell 正常退出后的 pipe 收尾仍只使用内部固定的短暂宽限期，不会额外等待 180 秒。
- `max_output_chars`：stdout/stderr 写入 transcript 的总字符上限，超限后截断并标记 truncated。必须大于 0，默认 `100000`。
- `shell`：shell 选择策略或具体 shell。支持 `auto`、`sh`、`bash`、`zsh`、`pwsh`、`powershell`、`cmd`，以及绝对路径如 `/bin/zsh`。默认 `auto`。
- `login_shell`：Unix shell 是否使用 `-lc`；为 `false` 时使用 `-c`。PowerShell / cmd 忽略该字段。默认 `true`。

### `[agent.session.tui]`

- `live_response_preview_max_lines`：TUI 虚线框内部内容的最大视觉行数，assistant 文本、工具与 shell 状态、activity、网络状态、空行和超限提示 `...` 均计入；顶部标题边框和底部边框不计入。默认 `-1`，表示自动使用当前可用高度；也可设置为不小于 `5` 的显式上限。`0`、`1..=4` 及小于 `-1` 的值无效。超限时逐行保留最新内容；终端可用高度不足时会按当前可用空间临时缩小。

### MCP 配置

自定义 MCP server 使用独立文件 `.mcp.json`，交互式 agent / `acn mcp` 激活 upstream 后实际读取 `<base_acn_home>/<upstream>/.mcp.json`，格式为：

```json
{
  "mcpServers": {
    "pal": {
      "type": "stdio",
      "command": "uvx",
      "args": ["pal-mcp-server"],
      "env_vars": ["OPENAI_API_KEY"]
    },
    "linear": {
      "type": "streamable_http",
      "url": "https://mcp.linear.app/mcp",
      "bearer_token_env_var": "LINEAR_API_KEY"
    },
    "oauth-server": {
      "type": "streamable_http",
      "url": "https://example.com/mcp",
      "oauth_client_id": "public-client-id"
    }
  }
}
```

MCP server 按连接方式分两类：

- `stdio`：进程型 MCP server。ACN 会按 `command` / `args` 启动一个子进程，并通过 stdin/stdout 与它通信；适合 `npx`、`uvx`、本机脚本或本机二进制。它的文件、网络和环境变量权限等同 ACN 进程。
- `streamable_http`：远程 HTTP MCP server。可匿名访问，也可使用 bearer token 或 OAuth。

常用命令：

- `acn mcp list`
- `acn mcp get <name> [--json]`
- `acn mcp add <name> [-e KEY=VALUE] [--env-var KEY] -- <command...>`
- `acn mcp add <name> --url <url> [--bearer-token-env-var ENV]`
- `acn mcp add <name> --url <url> [--oauth-client-id ID] [--oauth-callback-port PORT] [--oauth-credentials-store keyring|file]`
- `acn mcp add-json <name> '<server-json>'`
- `acn mcp remove <name>`
- `acn mcp enable <name>` / `acn mcp disable <name>`
- `acn mcp login <name> [--no-browser]` / `acn mcp logout <name>`

字段要点：

- `enabled` 可选，默认 `true`。
- 默认 enabled 的 server 会在 TUI 启动时被自动连接；`acn mcp status` 不带 server name 时会连接所有 enabled server。
- `acn mcp status <name>` 只连接并检查指定 server，不会启动其他 enabled server。
- `stdio` MCP server 会作为本地子进程运行，权限等同 ACN 进程。只添加可信 server；不确定来源建议先 disable，确认后再 enable/status。
- `startup_timeout_secs` 默认 `30`，`tool_timeout_secs` 默认 `120`。
- `enabled_tools` / `disabled_tools` 按 MCP server 原始 tool name 过滤。
- `-e KEY=VALUE` 会把 value 写入 `.mcp.json`，真实 token 推荐用 `--env-var KEY` 或 `--bearer-token-env-var ENV`。
- `add-json` 接受单个 `McpServerConfig` JSON，支持上文列出的全部字段，不接受完整的 `{"mcpServers": {...}}`。输入的 `type: "http"` 会规范化并保存为 `type: "streamable_http"`。
- 不要在 `add-json` 中写入 token；stdio 使用 `env_vars`，远程 HTTP 使用 `bearer_token_env_var`。
- `oauth_client_id` 仅用于服务方提供的预注册 public client ID；未配置时登录会动态注册 public client。
- `oauth_callback_port` 仅在服务方要求固定 redirect URI 时配置；否则使用随机端口。
- `oauth_credentials_store` 可取 `keyring`（默认）或 `file`；没有系统 keyring 的 headless 环境使用 `file`。
- bearer 与 OAuth 选项互斥。OAuth server 添加后执行 `acn mcp login <name>`；SSH/headless 环境使用 `--no-browser`。
- OAuth access token 会在 MCP 调用前按授权服务器返回的有效期自动刷新；静态 bearer token 不会由 ACN 刷新。登录失效或修改 `oauth_client_id` 后需要重新执行 `login`。
- `logout` 删除本地 OAuth 凭据；`remove` 删除配置并清理对应的本地凭据。
- 当前不支持旧版独立 SSE transport、自定义 HTTP headers、OAuth client secret、device flow、MCP Tasks 与 MCP elicitation；Streamable HTTP 返回的 SSE event stream 正常支持。
- `.mcp.json` 不会自动热加载；修改已有 server 后可在 `/mcp` 中 Reconnect，新增、删除或重命名 server 需要重启 TUI 后生效。

### `[agent.memory]`

- `memory_char_limit`：agent 私有 `MEMORY.md` 容量上限。超限时 memory 工具拒绝写入。
- `user_char_limit`：agent 私有 `USER.md` 容量上限。超限时 memory 工具拒绝写入。
- `memory_safety_scan`：写入 MEMORY / USER 前是否拦截明显 prompt injection、密钥外传和后门持久化内容。

### `[agent.tool]`

- `file_read_max_chars`：`file_read` 单页及单个 `@` 文本文件可返回的最大字符数，默认 `100000`。`file_read` 超限时可按 `page.next_start` 继续分页；`@` 文本超限时只提供路径和字符数，并引导模型改用 `file_read`。
- `file_diff_max_changed_lines`：`file_write` / `file_patch` 修改成功后采集并在 TUI 历史区展示的 diff 最大**改动行数**（仅统计 +/- 行，上下文行不占额度），超出部分截断并提示剩余改动行数。
- `max_parallel_tool_calls`：一个 agent 当前 turn 内、连续可并发工具批次的最大活跃调用数，默认 `5`，必须大于 `0`。它不跨 turn、session 或 agent 共享，也不限制 provider 的 fallback 尝试次数。
- `code_run_max_output_chars`：单次 `code_run` / `write_stdin` 工具中每个 stdout/stderr stream 回传允许的最大输出字符数，默认 `1048576`，最多 `2097152`；pipe 模式两个 stream 各自适用该上限，PTY 只有 stdout。
- `write_stdin_max_poll_timeout_ms`：`write_stdin` 空轮询的最大观察窗口，默认且最大 `300000`ms。它必须不小于内部 `code_run` 最大观察窗口 `30000`ms；非空写入仍受内部 `30000`ms 上限约束。

background-shell 其余时序、容量和 PTY 参数是 `config.rs` 内部默认值与资源护栏，而不是部署 TOML 键：`code_run` 初始观察窗口 / 最小值 / 最大值固定为 `10000`ms / `250`ms / `30000`ms，写入和空轮询默认值固定为 `250`ms / `5000`ms；输出 buffer、owner entry 容量、PTY 尺寸与 stdin buffer 也由内部值约束。部署配置不能将这些值下调或覆盖。
- `session_search_default_limit`：session search 默认返回条数，默认 `3`。
- `session_search_max_limit`：session search 最大返回条数，默认 `5`。
- `session_search_sqlite_busy_timeout_ms`：session search SQLite busy timeout，默认 `500`ms。

### `[agent.tool.web]`

- `endpoint`：web search endpoint，默认 `https://open.bigmodel.cn/api/paas/v4/web_search`。
- `api_key_env`：读取 web search API key 的环境变量名，默认 `GLM_API_KEY`。实际 key 不写入配置文件。
- `lookup_max_chars`：`web_fetch` / `web_request` 单次保留的最大字符数，默认 `80000`，必须大于 `0`。
- `max_count`：web search 单次最大结果数，默认 `10`，必须大于 `0`。
- `max_content_chars`：单条搜索结果正文最大保留字符数，默认 `2500`，必须大于 `0`。
- `max_total_chars`：一次 web search 聚合结果的最大保留字符数，默认 `200000`，必须大于 `0`。

### `[agent.attachment]`

- `enabled`：是否启用 TUI 附件输入，包括 `@文件`、`@目录`、附件读取、`@` 高亮和路径补全。默认 `true`。关闭后所有 `@路径` 都按普通文本发送，不解析文件或目录，也不读取本地目录列表。
- `clipboard_image_enabled`：是否启用 macOS 剪贴板图片粘贴为附件。默认 `true`。
- `max_file_bytes`：单个附件规范化后的最大字节数，超过后拒绝发送。必须大于 0，默认 `5242880`。
- `max_files_per_turn`：单轮最多可发送的附件数量。必须大于 0，默认 `5`。

### `[clients.router]`

- `query_timeout_secs`：agent 访问 router 的 query / scopes overview 外层超时秒数。agent 的 `consult_router` 和 session system prompt 中的 router scope overview 都受该值限制，默认 `50`。

### `[clients.http]`

- `timeout_secs`：agent 通过 HTTP 访问 router / maintainer 时，底层 HTTP 请求超时秒数，默认 `30`。maintainer server 内部访问 router 的调试接口也复用该配置。
- `retry_count`：HTTP 请求首次失败后的额外重试次数。
- `retry_base_delay_ms`：HTTP 请求重试退避基础间隔毫秒数。
- `retry_max_delay_ms`：HTTP 请求重试退避上限毫秒数。

## Router / Maintainer 侧配置

本节主要给部署或调试 router / maintainer daemon 的人看。单独运行 agent 时，通常不需要改这些字段。

### `[router]`

- `refresh_interval_secs`：router 后台刷新派生视图的周期秒数，包括 claim index 和 scopes overview。

### `[router.daemon]`

- `listen`：router daemon 的监听地址，例如 `127.0.0.1:8061` 或服务器上对外监听的 `0.0.0.0:8061`。

### `[router.auth.team]`

- `enabled`：router 是否校验来自 agent / maintainer workbench router query 的团队鉴权信封。默认值：`false`。为 `false` 时仍要求请求体是 `{ auth, data }` 信封，但不校验 `auth.acn_key`。

### `[router.retrieval]`

- `enabled`：是否启用 hybrid retrieval。关闭后只保留基础 lexical 行为和对应调试信息。
- `lexical_top_n`：lexical recall 保留的候选数量。
- `vector_top_m`：vector recall 保留的候选数量。
- `top_k`：最终返回的候选上限。
- `rerank_enabled`：是否启用候选 Claim 重排。关闭或重排失败时使用 lexical/vector 交错去重顺序降级。

### `[router.retrieval.vector]`

- `worker_poll_secs`：router vector worker 轮询待 embedding 队列的周期秒数。
- `query_timeout_secs`：query 阶段生成查询向量的超时秒数。超时后降级使用 lexical 结果。
- `retry_base_delay_ms`：claim embedding 失败后的首次重试等待毫秒数；后续失败按指数增长，默认 `2000`（2 秒）。
- `retry_max_delay_ms`：claim embedding 指数退避的等待上限毫秒数，默认 `30000`（30 秒），且必须不小于 `retry_base_delay_ms`。

### `[router.embedding]`

- `provider`：embedding provider。当前支持 `openai_compatible`、`ark_multimodal`。
- `endpoint`：可直接 POST 的完整 embedding API endpoint。`openai_compatible` 使用 OpenAI embeddings 请求 / 响应形状（`input` 为字符串，响应读取 `data[0].embedding`）；`ark_multimodal` 使用 Ark multimodal embeddings 请求 / 响应形状（`input` 为 `[{ type = "text", text = ... }]`，响应读取 `data.embedding`）。
- `model`：embedding 模型名。
- `api_key_env`：读取 embedding API key 的环境变量名，例如 `EXAMPLE_EMBEDDING_API_KEY`。
- `timeout_secs`：单次 embedding 请求超时秒数。
- `max_concurrency`：vector worker 并发处理 embedding 队列的上限。

### `[router.rerank]`

- `provider`：候选 Claim 的重排方式，默认 `openai_responses`。`heuristic` 使用本地启发式规则；`openai_chat` 使用 Chat Completions；`openai_responses` 使用 Responses。两种远端协议都把 query 和候选 Claim 交给通用模型排序，不要求使用专用 rerank 模型。
- `endpoint`：远端重排服务地址，必须是绝对 HTTP(S) URL。可以填写 host root、常见的 `/v1` base URL 或完整的 `/v1/chat/completions`、`/v1/responses` 请求 URL；ACN 按所选 provider 补全缺失路径，不在两种协议间自动切换。
- `model`：执行重排任务的模型名。
- `api_key_env`：读取远端重排服务 API key 的环境变量名。
- `timeout_secs`：单次 rerank 请求超时秒数。
- `max_tokens`：模型返回排序结果时的最大输出 token 数；`openai_chat` 映射为 `max_tokens`，`openai_responses` 映射为 `max_output_tokens`。
- `retry_count`：rerank 请求首次失败后的额外重试次数。
- `retry_base_delay_ms`：rerank 请求重试退避基础间隔毫秒数。
- `retry_max_delay_ms`：rerank 请求重试退避上限毫秒数。

`openai_responses` rerank 固定使用 non-streaming、`store = false` 的单轮请求，不发送 `reasoning` 字段，也不保存或回传上游 reasoning。Responses 未完成、输出不是合法排序 JSON 或最终请求失败时，Router 沿用现有 lexical/vector 排序降级。

### `[maintainer.sweep]`

- `tick_interval_secs`：maintainer 周期性 stale sweep 的 tick 间隔秒数；maintainer 启动时会先以 `maintainer_startup` 触发一次 sweep，随后再按该间隔周期执行。默认值：`86400`。
- `stale_after_days`：active claim 从最近语义更新时间起超过该天数未更新后，建议调整为 stale；时间优先取 `updated_at`，缺失时回退到 `created_at`。默认值：`30`。
- `deprecated_after_days`：stale claim 从最近语义更新时间起超过该天数后，建议调整为 deprecated；时间同样优先取 `updated_at`。默认值：`90`。

### `[maintainer.daemon]`

- `listen`：maintainer daemon 的监听地址，例如 `127.0.0.1:8062` 或服务器上对外监听的 `0.0.0.0:8062`。

### `[maintainer.ui]`

- `frontend_dist_dir`：maintainer workbench 前端构建产物目录。maintainer HTTP server 用它提供 UI 和静态资源。默认相对路径适用于源码 checkout；该默认目录不存在时，daemon 会回退到与 binary 同一安装前缀下的 `share/acn/maintainer-workbench`。任何其他自定义路径都严格按配置使用，不自动回退。

### `[maintainer.auth.admin]`

- `enabled`：是否启用 maintainer 管理台管理员鉴权。默认值：`false`。
- `username`：Basic Auth 用户名。默认值：`admin`。
- `password_env`：Basic Auth 密码所在的环境变量名；启用鉴权时该环境变量必须存在且非空。默认值：`ACN_MAINTAINER_ADMIN_PASSWORD`。

### `[maintainer.auth.team]`

- `enabled`：maintainer 是否校验来自 agent 的团队鉴权信封。默认值：`false`。为 `false` 时仍要求请求体是 `{ auth, data }` 信封，但不校验 `auth.acn_key`。

### `[maintainer.id]`

- `mint_max_retries`：Maintainer 生成需要查重的 ID 时，发生碰撞后的最大重抽次数。当前包括 policy、outbox inbox 和 action ID；总尝试次数为 `1 + mint_max_retries`。dispute id 由 agent 侧派生，不走该配置。

### `[maintainer.history]`

- `max_file_bytes`：maintainer history/audit JSONL 单个文件最大字节数，超过后滚动。
- `backup_count`：保留的历史滚动文件数量。

### 团队 key store

团队侧 API key 台账不放在客户端 `config.toml`，由 maintainer 写入 `<team_root>/maintainer/auth_keys.yaml`，router 启动时读取同一份文件做校验。该文件只保存 `sha256:<64 hex>` hash，不保存明文 key。
YAML 结构：

```yaml
auth:
  enabled: true
  api_keys:
    - key_id: key_abcd1234
      agent_id: demo-agent
      key_hash: sha256:<64 hex>
      generated_time: "2026-06-26T12:00:00Z"
      status: active
```

- `key_id`：台账行 id，用于 dashboard 操作与审计。
- `agent_id`：凭据绑定的 agent。
- `key_hash`：`sha256:<64 hex>`。
- `generated_time`：UTC 生成时间。
- `status`：`active` 或 `revoked`。只有 `active` 放行；同一 `agent_id` 同时最多一条 active key。

Maintainer dashboard 的 Team Auth 页面提供 `GET /api/team-auth/keys`、`POST /api/team-auth/keys`、`POST /api/team-auth/keys/{key_id}/revoke`，这些接口走 `[maintainer.auth.admin]` 管理台鉴权，不走团队 key 鉴权；如果管理台鉴权未启用，key list/create/revoke 返回 `403`。`router-service` 是系统保留身份，不会在列表展示，也不能由用户创建或撤销。

Maintainer 启动时会自动 ensure `router-service` 内部 key：hash 写在 `<team_root>/maintainer/auth_keys.yaml`，明文私有保存在 maintainer 的 service key 私有文件中。Router 鉴权前按 key store 当前内容全量替换 verifier 快照；maintainer dashboard 创建 / 撤销普通 key 后也刷新 maintainer verifier。

## 环境变量速查

### Agent 侧环境变量

- `[agent.llm].api_key_env`：配置为要读取的 API key 环境变量名；实际 key 不写入配置文件。
- `[agent.tool.web].api_key_env`：web search 工具使用的 API key 环境变量名。

### Router / Maintainer 侧环境变量

- `[router.embedding].api_key_env`：真实 embedding 路径必需，配置为要读取的 embedding API key 环境变量名。
- `[router.rerank].api_key_env`：`provider = "openai_chat"` 或 `provider = "openai_responses"` 时必需，配置为要读取的远端重排服务 API key 环境变量名。
- `[maintainer.auth.admin].password_env`：启用 maintainer 管理台管理员鉴权时必需，配置为要读取的 Basic Auth 密码环境变量名。
