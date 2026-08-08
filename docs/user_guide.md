# ACN 使用指南

本文面向日常使用 ACN 的用户，说明首次配置、运行模式、TUI 交互、MCP、后台任务和团队连接。ACN 是面向通用领域的协作型助手，主要通过终端 TUI 与用户交互。

## 首次准备

### 安装并生成配置

推荐通过 Homebrew 安装：

```bash
brew install FTShare-Lab/tap/acn
```

该命令会安装 `acn`、`acn-router`、`acn-maintainer` 和 Maintainer Workbench；日常交互只需启动 `acn`。

需要自行构建时，进入已经克隆的仓库目录安装三个可执行文件：

```bash
cargo install --path . --bins --force
```

首次运行会生成 `~/.acn/config.toml`，随后因 `agent_id` 为空而退出。这是正常的初始化流程：

```bash
acn
```

### 配置主 LLM

主对话 LLM 是运行 ACN 的必需服务。打开生成的配置文件，为 Agent 设置稳定身份，并填写同一个 LLM 服务的接口协议、base URL、模型名和 key 环境变量名：

```toml
upstream = "default"

[upstreams.default]
agent_id = "your-agent-id"
acn_key_env = ""
maintainer_endpoint = ""
router_endpoint = ""

[agent.llm]
provider = "openai_responses"
endpoint = "https://your-llm-endpoint/v1"
model = "your-model"
api_key_env = "ACN_LLM_API_KEY"
```

这几个字段的关系如下：

| 字段 | 含义 |
| --- | --- |
| `provider` | 请求协议，默认推荐 `openai_responses`；也支持 `openai_chat` 和 `anthropic`。 |
| `endpoint` | 与所选协议兼容的 base URL 或完整请求 URL。OpenAI-compatible 的常见 base URL 形如 `https://llm.example.com/v1`，Anthropic-compatible 的常见 base URL 形如 `https://llm.example.com`。 |
| `model` | 该服务实际接受的模型 ID。 |
| `api_key_env` | 保存这个 LLM API key 的环境变量名，不是 key 本身。 |

`provider`、`endpoint` 和 `model` 必须属于同一个兼容服务。`endpoint` 必须是绝对 HTTP(S) URL；`model` 不能为空。

ACN 默认使用流式输出；流式失败时可能改用同协议的非流式请求重试，不会自动切换协议。

`openai_responses` 和 `anthropic` 支持在同一模型的连续会话中保存并回传 Reasoning，但 TUI 只显示最终回答；切换协议或模型后会使用新的 Reasoning 上下文。


`openai_chat` 不保留厂商扩展的 Reasoning；依赖这类上下文的模型建议使用 `openai_responses` 或 `anthropic`。Anthropic Thinking 的开关与预算配置见 [配置参数](config_parameters.md)。

`agent_id` 只能使用小写字母、数字、`_` 和 `-`，在团队内应保持唯一。它还决定本地数据目录，开始使用后不要随意修改。

设置配置中指定的 LLM key 环境变量：

```bash
export ACN_LLM_API_KEY="<your-llm-api-key>"
```

`export` 只对当前 shell 生效。长期使用时，可通过自己的 shell 配置或 secret 管理方式注入；不要把真实 key 直接写进 `config.toml`。ACN 当前要求 `api_key_env` 指向的变量存在且非空，即使所连接的 LLM 服务本身不校验 key。

### 关键词联网搜索

默认的 `web_search` 工具调用智谱 BigModel Web Search。它是独立于主 LLM 的搜索服务，因此使用另一套凭据。生成的配置已经包含以下内容，通常无需再次添加：

```toml
[agent.tool.web]
endpoint = "https://open.bigmodel.cn/api/paas/v4/web_search"
api_key_env = "GLM_API_KEY"
```

联网搜索使用以下环境变量：

```bash
export GLM_API_KEY="<your-web-search-api-key>"
```

`GLM_API_KEY` 只供 `web_search` 读取，普通对话仍使用主 LLM 的 key，`web_fetch` 和 `web_request` 也不读取它。可以修改 `api_key_env` 使用其他环境变量名。若更换 endpoint，新服务必须兼容当前智谱 Web Search 的请求、Bearer 鉴权和响应格式。

ACN 在进程启动时读取这些环境变量。配置完成后，到希望 ACN 工作的目录启动：

```bash
cd /path/to/workspace
acn
```

如果 ACN 已经在运行，之后才设置或更换 key，需要退出并重新启动。

## 运行模式

### 单人模式

保持 `maintainer_endpoint` 与 `router_endpoint` 为空即可。单人模式不发起团队请求，仍可使用完整 TUI、工具、Memory、Session、本地 Claim、Trace 和后台 Finalize Supervisor。

### 团队模式

同时配置 `maintainer_endpoint` 与 `router_endpoint` 后，ACN 会在 Session 启动、恢复或执行 `/inbox` 时同步团队消息，也会在任务中按需查询 Router，并在 compact 或 finalize 后上传新的 Claim mirror。只配置其中一项会被拒绝。

`acn_key_env` 是团队服务鉴权 key 的环境变量名，与主 LLM 和 Web Search 的 key 无关。团队启用鉴权时，使用管理员提供的 key；未启用时可以留空或省略。配置了变量名但没有设置值不会阻止启动，但开启鉴权的团队服务会拒绝请求。

从单人模式切换到团队模式后，只同步切换后的新产物；单人模式期间形成的历史 Claim 不会自动补传。完整参数见 [配置参数](config_parameters.md)。

## Agent Upstream 与持久文件

`upstream` 是 Agent 侧的一份运行与团队连接配置：它决定 Agent 身份、Router/Maintainer 地址、团队凭据来源和本地私有数据目录。名称只是本机别名，不是服务端团队 ID。两个 endpoint 都留空时，这份配置对应单人模式。

顶层 `upstream` 决定 Agent 默认使用哪组 `[upstreams.<name>]`。也可以在启动时临时选择：

```bash
acn --upstream team
```

配置文件中每个已经定义的 upstream 都必须填写合法的 `agent_id`，不只检查当前选中的那一个。

默认 base 目录是 `~/.acn`，配置文件位于 `~/.acn/config.toml`。Agent 在各 upstream 下的私有数据互相隔离：

```text
~/.acn/<upstream>/
  ACN.md
  .mcp.json
  skills/
  data/agents/<agent_id>/memories/
    MEMORY.md
    USER.md
```

- `ACN.md`：用户提供的长期指令与项目约定。
- `MEMORY.md`：agent 的私有经验与可复用工作记忆。
- `USER.md`：用户偏好和稳定资料。
- `.mcp.json`：该 Agent upstream 的 MCP server 配置。

这些内容会在新 session 启动时生成 system prompt 快照。修改文件不会改变已经创建的 session；重新进入旧 session 时仍复用原 system prompt。

## 启动与恢复

ACN 默认把当前终端目录作为工具工作目录：

```bash
cd /path/to/workspace
acn
```

也可以显式指定：

```bash
acn --cd /path/to/workspace
acn --config /path/to/config.toml --upstream team --cd /path/to/workspace
acn --resume
```

## TUI 交互

直接输入任务并按 `Enter`。`Shift+Enter` 插入换行；流式生成或工具执行期间，`Ctrl+Enter` 会把不带附件、不会被识别为命令的普通文本作为 steer，在安全边界交给当前 turn。带附件的输入会进入普通队列。

常用命令：

- `/help`：显示内置帮助
- `/compact`：手动压缩上下文
- `/copy`：复制最近一条 assistant 回复
- `/exit`：结束当前 session，并把 finalize 交给后台 supervisor
- `/inbox`：同步团队消息；单人模式会明确提示团队服务未配置
- `/mcp`：查看 MCP server、连接状态和工具
- `/ps`：查看、选择和终止当前 session 可见的受管进程
- `/resume`：选择可恢复的 session
- `/skills`：查看当前 Agent upstream 的 Skill
- `/subagents`：查看当前 session 的 subagents
- `!cmd`：运行本地 shell 命令
- `@path`：把文本、图片或 PDF 加入当前输入

附件相关按键：

- `Ctrl+V`：粘贴剪贴板图片
- `Ctrl+O`：预览光标所在的附件

单个 `@` 文本文件默认最多完整内联 100,000 个字符，可通过 `[agent.tool].file_read_max_chars` 调整。

`/mcp`、`/ps` 和 `/subagents` 是 live panel；在 turn 进行中也可以打开，关闭后返回原交互状态。

## 团队连接状态

欢迎面板显示本 session 最近一次 inbox 过程观察到的状态：

- `✅`：连接成功
- `❌`：连接失败或超时
- `❓`：当前 Agent upstream 未配置团队服务

团队请求失败会显示 warning，但不阻止本地对话和工具使用。session 进行中执行 `/inbox` 后，欢迎面板仍可见时会更新为最新结果。

## 自定义 MCP server

MCP 配置保存在当前 Agent upstream 的 `.mcp.json`，不是 `config.toml`。

添加本地 stdio server：

```bash
acn mcp add my-server \
  --env-var MY_API_KEY \
  -e DEFAULT_MODEL=auto \
  -- uvx my-mcp-server
```

添加远程 Streamable HTTP server：

```bash
acn mcp add linear \
  --url https://mcp.linear.app/mcp \
  --bearer-token-env-var LINEAR_API_KEY
```

添加单个 JSON 配置：

```bash
acn mcp add-json local-server '{
  "type": "stdio",
  "command": "uvx",
  "args": ["my-mcp-server"],
  "env_vars": ["MY_API_KEY"],
  "startup_timeout_secs": 30
}'

acn mcp add-json remote-server '{
  "type": "streamable_http",
  "url": "https://example.com/mcp",
  "bearer_token_env_var": "MCP_TOKEN"
}'
```

`add-json` 接受单个 server 对象，不接受 `{"mcpServers": {...}}` 包装。`"type": "http"` 会规范化为 `"streamable_http"`。

管理命令：

```bash
acn mcp list
acn mcp get my-server
acn mcp get my-server --json
acn mcp status
acn mcp status my-server
acn mcp disable my-server
acn mcp enable my-server
acn mcp remove my-server
```

注意：

- `-e KEY=VALUE` 会把值写入 `.mcp.json`，不要用于敏感 token。
- stdio server 继承 ACN 进程权限，只添加可信命令。
- 当前支持 stdio 与 Streamable HTTP；不支持 OAuth 登录、浏览器授权回调、SSE transport、自定义 HTTP headers 和 MCP elicitation。
- 外部修改 `.mcp.json` 后需要重启 TUI。TUI 内启用或禁用 server 会持久化 `enabled` 字段。

## 后台进程

模型通过 `code_run` 启动的长命令可在观察窗口后转入后台。`/ps` 展示进程状态、owner、TTY、启动时间、经过时间、cwd 与 command；选中后按 `t` 可确认终止。

进程属于创建它的 root session 或 subagent。turn 结束不会自动杀死仍在运行的受管进程；session finalize 会收束相应生命周期。

## 退出与 Finalize Supervisor

退出非空 session 时，ACN 通常将 recap、claim 和 trace 的 finalize 工作交给后台 supervisor；团队模式还会报告符合条件的 dispute，然后尽快归还终端。

查看任务：

```bash
acn supervisor status
acn supervisor jobs
acn supervisor retry <session_id>
```

停止 supervisor：

```bash
acn supervisor stop
```

使用自定义 `--config` 或 `--upstream` 启动时，管理 supervisor 也应传相同参数。

失败的 finalize 可按 session ID 重试，也可使用 `jobs` 显示的 job ID；若 session 已进入 `Finalizing` 但尚未创建 job，也可用 session ID 恢复。配置、相关凭据或 ACN 版本变化后，下次启动对应 upstream 时会自动接管旧 supervisor 并继续未完成任务。supervisor 连续空闲 5 分钟后会自行退出。

## 会话维护与更新

```bash
acn update
acn update --url <repository-url>
acn --version
acn session cleanup
acn session cleanup --apply
```

`acn update` 用于更新 Cargo 安装，`--url` 可临时指定其他可信仓库；Homebrew 安装请使用 `brew upgrade acn`。升级后 ACN 会自动更新后台 supervisor，并继续处理待完成任务。

`session cleanup` 默认只预览；加 `--apply` 才会删除符合保留期与状态条件的旧 session。

## 团队管理页面

团队模式下，Maintainer endpoint 同时提供知识管理页面。它用于查看 claim、dispute、policy、agent、outbox/send log、stale sweep、Router 查询和 HTTP audit。管理员鉴权与团队 key 配置见 [配置参数](config_parameters.md)。
