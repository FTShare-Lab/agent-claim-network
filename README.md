<h1 align="center">ACN · Agent Claim Network</h1>

<p align="center">
  跑在终端里的通用领域 AI 助手。<br>
  单人使用时就是完整助手；多人接入后，各自沉淀的判断还能连成可检索、可追溯的网络。
</p>

<p align="center">
  由 <a href="https://ft.tech">非凸科技 · Non-convex ft.tech</a> 研发
</p>

<p align="center">
  <img alt="license: MIT OR Apache-2.0" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg">
  <img alt="rust 1.90" src="https://img.shields.io/badge/rust-1.90-orange.svg">
  <img alt="version 0.2.5" src="https://img.shields.io/badge/version-0.2.5-brightgreen.svg">
  <a href="README_EN.md"><img alt="English README" src="https://img.shields.io/badge/README-English-blue.svg"></a>
</p>

<p align="center">
  <a href="#简介">简介</a> ·
  <a href="#能干什么">能干什么</a> ·
  <a href="#快速开始">快速开始</a> ·
  <a href="#记忆与-claim">记忆与 Claim</a> ·
  <a href="#接入团队之后">接入团队之后</a> ·
  <a href="#数据目录">数据目录</a> ·
  <a href="#名词">名词</a> ·
  <a href="#文档">文档</a>
</p>

<p align="center">
  <img alt="ACN 终端演示：借用团队里其他 Agent 的判断" src="docs/assets/acn-demo.gif" width="960">
</p>

这段演示中，当前 Agent 发现销售明细里的西部收入尚未回填，先通过团队检索服务 Router 找到另一位 Agent 已沉淀的两条可复用判断（Claim）：数据在 09:30 后才完整，缺失字段的合计必须标为“暂定”；随后按这些团队口径修改日报，并把文件 diff 展示出来。

如需浏览团队协作界面，可查看 [Web 界面预览](https://ftshare-lab.github.io/agent-claim-network/)；页面使用模拟服务端和虚构数据。

## 简介

执行 `acn`，用自然语言交代任务：查资料、改文件、跑命令、看图片和 PDF、接外部工具。命令在你本机执行，改动会以 diff 显示。这一层和常见的终端 coding agent 同类，但不限于写代码，而是一个通用助手。

另外一层是 Claim。压缩上下文或结束会话时，ACN 会复盘本轮里站得住、以后还用得上的结论，落成一条带适用范围和依据的判断。一个人用时，下次少重复问；多人把各自的 ACN 接入同一套团队服务后，这些判断可以互相看见、引用和质疑——流动起来，形成 Network。私有 Memory 仍只属于当前 Agent；团队里共享的是带 holder 的 Claim。

团队服务（Router、Maintainer）是仓库里另外两个可执行文件，按需部署。两个地址都留空，就是单人模式：功能齐全，只是不连团队服务。

> [!WARNING]
> ACN 会在本机执行命令、读写文件，**不是沙箱**。stdio 类型的 MCP server 也继承 ACN 进程权限。请在信任的工作目录里使用，只接入信任的工具。

## 能干什么

- 流式对话；`@路径` 附加文本 / 图片 / PDF；`Ctrl+V` 贴图，`Ctrl+O` 预览
- 跑到一半还能 `Ctrl+Enter` 插话；后续输入会排队，不打乱当前这一轮
- 耗时长的命令可转后台，`/ps` 里查看或终止
- 文件改动直接出 diff，且不会塞回模型上下文
- `acn --resume` 恢复历史会话；也能让它自己搜旧会话里说过什么
- `/exit` 后复盘进后台，终端马上归还
- 子代理可并行干活：创建、等待、插话、看进度都在 TUI 里
- 支持 MCP（stdio / Streamable HTTP，进程内共享连接）和 Skill（`/技能名` 显式注入）
- 主对话支持 Anthropic Messages、OpenAI-compatible Chat Completions 与 Responses；流式中断会自动改走同协议非流式重试

更详细的交互说明见 [使用指南](docs/user_guide.md)。

## 快速开始

### 安装

当前支持：

- Apple Silicon Mac（macOS 11 及以上）
- Intel Mac（macOS 11 及以上）
- x86_64 GNU/Linux（以 Ubuntu 22.04、glibc 2.35 为构建与验证基线）

Linux Release 面向使用兼容 glibc 的 x86_64 发行版，其他发行版尚未逐一验证。当前不支持 Alpine/musl、Linux ARM64 和 Windows。

推荐通过 Homebrew 安装：

```bash
brew install FTShare-Lab/tap/acn
```

该命令会安装 `acn`、`acn-router`、`acn-maintainer` 和 Maintainer Workbench。后续升级使用：

```bash
brew update
brew upgrade acn
```

需要从源码构建时：

```bash
git clone https://github.com/FTShare-Lab/agent-claim-network.git
cd agent-claim-network
cargo install --path . --bins --force
```

源码构建的 Rust 版本由 `rust-toolchain.toml` 固定为 1.90，有 rustup 会自动拉取。

### 生成配置

```bash
acn
```

> [!NOTE]
> 首次运行只写出 `~/.acn/config.toml` 就退出，因为 `agent_id` 还是空的——这是预期行为。

### 填写身份和模型

编辑 `~/.acn/config.toml`：

```toml
upstream = "default"

[upstreams.default]
agent_id = "your-agent-id"       # 小写字母、数字、_、-
acn_key_env = ""
maintainer_endpoint = ""         # 都留空 = 单人模式
router_endpoint = ""

[agent.llm]
provider = "openai_responses"
endpoint = "https://your-llm-endpoint/v1"
model = "your-model"
api_key_env = "ACN_LLM_API_KEY"  # 环境变量名
```

> [!NOTE]
> `openai_responses` 支持 Reasoning 的私有落盘和同模型连续回传；当前 TUI 只显示最终回答、后续将支持 Reasoning 的逐步推理过程显示。

`upstream` 是 Agent 侧的一份配置：身份、团队地址、本机数据目录。团队地址都留空即为单人模式（不连 Router / Maintainer）。

### 启动

```bash
export ACN_LLM_API_KEY="<your-api-key>"
cd /path/to/workspace
acn
```

启动时的目录会是工作目录，作为工具和 `!命令` 的 cwd。进 TUI 后 `/help` 看命令；常用：`Enter` 发送、`Shift+Enter` 换行、`Ctrl+Enter` 插话、`/new` 新建会话、`/resume` 切换历史会话、`/exit` 结束。

<details>
<summary><b>改用 Anthropic 协议</b></summary>

```toml
[agent.llm]
provider = "anthropic"
endpoint = "https://your-llm-endpoint"
model = "your-model"
reasoning_effort = "none"                # none | low | medium | high | xhigh | max
anthropic_thinking = "auto"              # auto | enabled | adaptive | disabled
# anthropic_thinking_budget_tokens = 4096 # enabled 时可选
api_key_env = "ACN_LLM_API_KEY"
```

`reasoning_effort` 会按协议字段发出去；ACN 不检查模型是否真支持。

</details>

<details>
<summary><b>改用 OpenAI Chat 协议</b></summary>

```toml
[agent.llm]
provider = "openai_chat"
endpoint = "https://your-llm-endpoint/v1"
model = "your-model"
reasoning_effort = "none"                # none | low | medium | high | xhigh | max
api_key_env = "ACN_LLM_API_KEY"
```

`openai_chat` 适用于只提供 Chat Completions 的兼容服务，但会丢弃厂商扩展的 Reasoning 字段。要求在后续请求或工具回环中回传 Reasoning 时，应使用 `openai_responses` 或 `anthropic`。

</details>

<details>
<summary><b>联网搜索</b></summary>

`web_search` 走独立搜索服务，默认智谱 BigModel，凭据与主对话分开：

```bash
export GLM_API_KEY="<your-web-search-api-key>"
```

不配也能启动，只是搜不了网；`web_fetch` / `web_request` 不读这个变量。

</details>

<details>
<summary><b>常用参数与子命令</b></summary>

```bash
acn --help
acn --cd /path/to/workspace
acn --resume
acn --upstream team
acn --version

acn session cleanup
acn session cleanup --apply

acn supervisor status
acn supervisor jobs
acn supervisor retry session_1234abcd

acn mcp list
acn mcp add / add-json / remove / enable / disable / login / logout / status

acn update
```

若启动时用了自定义 `--config` 或 `--upstream`，管理 supervisor 时带上同样参数。ACN 会按有效配置、upstream 和 finalize 所需凭据摘要识别 supervisor 运行环境；这些内容变化后，下次启动会安全接管旧 supervisor，并由新环境继续未完成的 finalize job。`Finalizing` session 通常也可以直接 Resume；如果仍由另一个前台进程收尾，请按提示稍后重试。`acn supervisor retry <session_id>` 作为运维命令，也可使用 `jobs` 显示的 job ID 指定重试。

</details>

<details>
<summary><b>TUI 命令与按键</b></summary>

| 输入 | 作用 |
| --- | --- |
| `/help` | 帮助 |
| `/compact` | 压缩上下文 |
| `/copy` | 复制最近一条回复 |
| `/inbox` | 同步并处理 inbox |
| `/mcp` | MCP 状态与工具 |
| `/new` | 创建并切换到新会话 |
| `/ps` | 后台进程 |
| `/resume` | 选择并切换到历史会话 |
| `/skills` | Skill 列表 |
| `/subagents` | 子代理 |
| `/exit` | 结束会话 |
| `!命令` | 本地 shell |
| `@路径` | 附加文件 |
| `Ctrl+Enter` | 运行中插话 |
| `Ctrl+V` / `Ctrl+O` | 贴图 / 预览附件 |

`/mcp`、`/ps`、`/subagents` 是实时面板，对话进行中也能开。

</details>

## 记忆与 Claim

### Memory

`MEMORY.md` 记经验和约定，`USER.md` 记你的偏好。会话里 agent 会自己写，也会有旁路 review 帮忙整理。

这两类 Memory **不会进团队通道**，别人、Router、Maintainer 都读不到。对于长期要 ACN 遵守的规则，可以写在 `~/.acn/config.toml` 下的 `ACN.md`。

### Claim

Claim 是一条站得住的判断，带 holder、scope、置信度和证据摘要，例如：

> statistic-online 数据源每天 09:30 后才完整；在此之前生成的日报需要标记为暂定。`holder` 是 `zhangsan`，`scope` 是 `daily-report`，置信度 0.95，证据是 `statistic-online 数据源每天 09:00 触发更新，耗时 20-30 分钟`。

不用手记：压缩或结束会话时自动复盘生成，并写 trace 记来源。**单人模式就会做**，和有没有团队服务无关。

相对整段聊天，Claim 更好检索和更新；相对 Memory，它是自包含的，读的人不必回看你的会话。

详见 [Memory 设计](docs/memory_design.md)。

## 接入团队之后

可以先一个人用。单人模式照常跑，Claim 和 trace 会在本机积累。

团队里有第二个人也在用时，部署 Router 和 Maintainer，各自配置里填上两个地址，之后新产生的 Claim 才开始互相可见。任务中可 `consult_router`，启动或 `/inbox` 时同步团队消息。

```toml
[upstreams.team]
agent_id = "your-agent-id"
acn_key_env = "ACN_AUTH_KEY"                  # 未开鉴权可留空
maintainer_endpoint = "http://maintainer.example"
router_endpoint = "http://router.example"
```

```bash
export ACN_AUTH_KEY="<team-key>"
acn --upstream team
```

切换到团队模式后只同步此后的新 Claim，单人阶段的历史不会自动补传。

用上一阵子之后，判断会互相引用；冲突不会被强行合成一句结论，而是留下 dispute 待复核。Maintainer 的管理台可以看到整体状态。

团队里没有可以强行写入各 Agent 的「真理」。Policy 与别人的 Claim 只是输入；只有结合自己的上下文主动采纳后，才变成该 Agent 自己的判断，且不同 Agent 可以内化成不同结论。

<p align="center">
  <a href="docs/assets/acn-team-claim-flow.webp">
    <img alt="ACN 团队模式下 Agent、Router、Maintainer 与 Claim 的协作流程" src="docs/assets/acn-team-claim-flow.webp" width="960">
  </a>
</p>

<p align="center">
  <sub>团队模式下 Claim 如何被发现、借用，以及 Dispute 如何回流。</sub>
</p>

<details>
<summary><b>部署 Router 与 Maintainer</b></summary>

二者是本仓库另外两个 binary，可用同一份配置：

```bash
cargo run --bin acn-router     -- --config /path/to/config.toml
cargo run --bin acn-maintainer -- --config /path/to/config.toml
```

本机试跑用上面即可；长期给团队用再加 `--release`（编译更慢，运行更省、二进制更小）。可以分开部署，只要每个 Agent 都能访问到这两个地址。

- **Router**：按 scope / 语义检索团队 Claim 与相关 dispute，只负责发现，不判对错。
- **Maintainer**：接收 Claim 镜像与 dispute，发布 policy、治理过期知识，并可选用独立 LLM 辅助分析冲突并解决。

Maintainer 还带 Web 管理台（默认 `/app`）：agents、claims、disputes、policies、sweep、Router 查询、审计、团队 key 等。构建见 [前端说明](frontend/README.md)。

参数见 [配置参数](docs/config_parameters.md)，结构见 [系统架构](docs/architecture.md)。

</details>

### 上传边界

| | |
| --- | --- |
| **会上传** | Claim 镜像；符合条件的 dispute |
| **不会上传** | `MEMORY.md`、`USER.md`、会话记录、trace |
| **单人模式** | 不发团队请求，也不偷偷攒待补传队列 |

## 数据目录

Agent 私有数据默认在 `~/.acn`，按所选 upstream 分开：

```text
~/.acn/
  config.toml
  <upstream>/
    ACN.md
    .mcp.json
    skills/
    data/agents/<agent_id>/
      memories/MEMORY.md
      memories/USER.md
      claims/
      sessions/
```

## 名词

| 词 | 含义 |
| --- | --- |
| **Agent** | 你启动的那个 `acn`：独立身份、私有记忆和工作记录 |
| **Claim** | 可检索、可引用的稳定判断 |
| **Trace** | Claim 从哪来，只留本地 |
| **Dispute** | 两条 Claim 冲突时的记录，留给后续复核 |
| **Policy** | Maintainer 下发的规则或建议 |
| **Router** | 团队 Claim 的检索入口 |
| **Maintainer** | 团队侧治理与管理台 |
| **Upstream** | Agent 侧一份配置（身份、团队地址、数据目录），名字只是本机别名 |

## 文档

- [使用指南](docs/user_guide.md)
- [配置参数](docs/config_parameters.md)
- [系统架构](docs/architecture.md)
- [核心行为与数据边界](docs/core_behavior.md)
- [Memory 设计](docs/memory_design.md)
- [文档索引](docs/README.md)

## 参与贡献

欢迎 issue 和 PR。提交前建议跑通：

```bash
scripts/check_version_consistency.sh
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo check
```

涉及 TUI 时请附终端验收说明。约定见 [AGENTS.md](AGENTS.md)。

## 致谢

ACN 的部分产品与工程设计受到以下开源项目启发：

- [OpenAI Codex](https://github.com/openai/codex)
- [Hermes Agent](https://github.com/NousResearch/hermes-agent)
- [GenericAgent](https://github.com/lsdefine/GenericAgent)

部分终端交互参考了 [Claude Code](https://code.claude.com/docs/en/overview) 公开呈现的产品体验。上述项目与 ACN 彼此独立。

## 许可证

MIT OR Apache-2.0

<p>
  <br>
  <a href="https://ft.tech">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/assets/non-convex-ft-tech-logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="docs/assets/non-convex-ft-tech-logo-light.png">
      <img alt="非凸科技 · Non-convex ft.tech" src="docs/assets/non-convex-ft-tech-logo-light.png" width="180">
    </picture>
  </a>
</p>
