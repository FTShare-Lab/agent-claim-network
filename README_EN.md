<h1 align="center">ACN · Agent Claim Network</h1>

<p align="center">
  A general-purpose AI assistant that runs in your terminal.<br>
  It works as a complete standalone assistant, while multiple connected agents can turn their individual judgments into a searchable, traceable network.
</p>

<p align="center">
  Developed by <a href="https://ft.tech">Non-convex ft.tech</a>
</p>

<p align="center">
  <img alt="license: MIT OR Apache-2.0" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg">
  <img alt="rust 1.90" src="https://img.shields.io/badge/rust-1.90-orange.svg">
  <img alt="version 0.2.3" src="https://img.shields.io/badge/version-0.2.3-brightgreen.svg">
  <a href="README.md"><img alt="Chinese README" src="https://img.shields.io/badge/README-简体中文-blue.svg"></a>
</p>

<p align="center">
  <a href="#introduction">Introduction</a> ·
  <a href="#capabilities">Capabilities</a> ·
  <a href="#quick-start">Quick Start</a> ·
  <a href="#memory-and-claims">Memory and Claims</a> ·
  <a href="#team-mode">Team Mode</a> ·
  <a href="#data-directory">Data Directory</a> ·
  <a href="#glossary">Glossary</a> ·
  <a href="#documentation">Documentation</a>
</p>

<p align="center">
  <img alt="ACN terminal demo: reusing judgments from other agents on the team" src="docs/assets/acn-demo.gif" width="960">
</p>

In this demo, the current Agent notices that revenue from the western region has not yet been filled into the sales details. It first queries the team Router and finds two reusable Claims produced by another Agent: the data is only complete after 09:30, and totals containing missing fields must be marked as provisional. It then updates the daily report according to those shared conventions and displays the file diff.

To explore the team collaboration interface, open the [Web UI preview](https://ftshare-lab.github.io/agent-claim-network/); it uses a mock server and fictional data.

## Introduction

Run `acn` and describe a task in natural language: research information, edit files, run commands, inspect images and PDFs, or connect external tools. Commands execute on your machine, and file changes are presented as diffs. This layer is similar to a terminal coding agent, but ACN is a general-purpose assistant rather than a coding-only tool.

Claims add another layer. When ACN compacts context or closes a session, it reviews the conversation and extracts conclusions that are well-supported and likely to remain useful. Each Claim records its scope and supporting evidence. When used by one person, Claims reduce repeated questions in later sessions. When multiple people connect their ACN instances to the same team services, their Claims can be discovered, cited, and disputed by one another—allowing judgments to flow and form a Network. Private Memory remains exclusive to each Agent; only Claims with an explicit holder are shared with the team.

Router and Maintainer are separate executables included in this repository and can be deployed when needed. Leaving both endpoints empty enables standalone mode: all local assistant capabilities remain available, but ACN does not connect to team services.

> [!WARNING]
> ACN executes commands and reads or writes files on your machine. It is **not a sandbox**. Stdio MCP servers also inherit the permissions of the ACN process. Use ACN only in trusted working directories and connect only trusted tools.

## Capabilities

- Streaming conversations; attach text, images, or PDFs with `@path`; paste images with `Ctrl+V` and preview them with `Ctrl+O`
- Steer an active turn with `Ctrl+Enter`; later inputs are queued without disrupting the current turn
- Move long-running commands to the background, then inspect or terminate them from `/ps`
- Display file changes as diffs without inserting those diffs back into the model context
- Resume previous sessions with `acn --resume`; ACN can also search earlier sessions for relevant information
- Move post-session review to the background after `/exit`, returning the terminal immediately
- Run subagents in parallel, with creation, waiting, steering, and progress inspection available in the TUI
- Support MCP (stdio / Streamable HTTP, with in-process shared connections) and Skills (explicitly injected with `/skill-name`)
- Support Anthropic Messages, OpenAI-compatible Chat Completions, and Responses for the main conversation; interrupted streaming automatically retries in non-streaming mode within the same protocol

See the [User Guide](docs/user_guide.md) for detailed interaction behavior.

## Quick Start

### Installation

Currently supported:

- Apple Silicon Macs running macOS 11 or later
- Intel Macs running macOS 11 or later
- x86_64 GNU/Linux, with Ubuntu 22.04 and glibc 2.35 as the build and validation baseline

The Linux release targets x86_64 distributions with a compatible glibc; other distributions have not been validated individually. Alpine/musl, Linux ARM64, and Windows are not currently supported.

The recommended installation method is Homebrew:

```bash
brew install FTShare-Lab/tap/acn
```

This installs `acn`, `acn-router`, `acn-maintainer`, and the Maintainer Workbench. To upgrade later:

```bash
brew update
brew upgrade acn
```

To build from source instead:

```bash
git clone https://github.com/FTShare-Lab/agent-claim-network.git
cd agent-claim-network
cargo install --path . --bins --force
```

The source build uses the Rust version pinned to 1.90 in `rust-toolchain.toml`. If rustup is installed, it downloads the required toolchain automatically.

### Generate the Configuration

```bash
acn
```

> [!NOTE]
> On the first run, ACN writes `~/.acn/config.toml` and exits because `agent_id` is still empty. This is expected.

### Configure Identity and Model

Edit `~/.acn/config.toml`:

```toml
upstream = "default"

[upstreams.default]
agent_id = "your-agent-id"       # lowercase letters, digits, _, and -
acn_key_env = ""
maintainer_endpoint = ""         # leave both empty for standalone mode
router_endpoint = ""

[agent.llm]
provider = "openai_responses"
endpoint = "https://your-llm-endpoint/v1"
model = "your-model"
api_key_env = "ACN_LLM_API_KEY"  # environment variable name
```

> [!NOTE]
> `openai_responses` supports private Reasoning persistence and replay across consecutive turns using the same model. The TUI currently displays only the final answer; step-by-step Reasoning display will be supported in the future.

An `upstream` is one Agent-side configuration containing its identity, team endpoints, and local data directory. Leaving both team endpoints empty selects standalone mode, with no connection to Router or Maintainer.

### Start ACN

```bash
export ACN_LLM_API_KEY="<your-api-key>"
cd /path/to/workspace
acn
```

The directory in which ACN starts becomes the working directory and the cwd for tools and `!commands`. In the TUI, use `/help` to see available commands. Common controls include `Enter` to send, `Shift+Enter` for a new line, `Ctrl+Enter` to steer an active turn, and `/exit` to close the session.

<details>
<summary><b>Use the Anthropic protocol</b></summary>

```toml
[agent.llm]
provider = "anthropic"
endpoint = "https://your-llm-endpoint"
model = "your-model"
reasoning_effort = "none"                # none | low | medium | high | xhigh | max
anthropic_thinking = "auto"              # auto | enabled | adaptive | disabled
# anthropic_thinking_budget_tokens = 4096 # optional when enabled
api_key_env = "ACN_LLM_API_KEY"
```

`reasoning_effort` is sent using the corresponding protocol field. ACN does not check whether the selected model supports it.

</details>

<details>
<summary><b>Use the OpenAI Chat protocol</b></summary>

```toml
[agent.llm]
provider = "openai_chat"
endpoint = "https://your-llm-endpoint/v1"
model = "your-model"
reasoning_effort = "none"                # none | low | medium | high | xhigh | max
api_key_env = "ACN_LLM_API_KEY"
```

`openai_chat` is intended for compatible services that expose only Chat Completions, but it discards provider-specific Reasoning fields. Use `openai_responses` or `anthropic` when the model requires Reasoning to be replayed in later requests or tool round trips.

</details>

<details>
<summary><b>Web search</b></summary>

`web_search` uses a separate search service, with Zhipu BigModel as the default. Its credentials are independent from the main conversation provider:

```bash
export GLM_API_KEY="<your-web-search-api-key>"
```

ACN can start without this variable, but web search will be unavailable. `web_fetch` and `web_request` do not use it.

</details>

<details>
<summary><b>Common options and subcommands</b></summary>

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

If ACN was started with a custom `--config` or `--upstream`, pass the same option when managing the supervisor. ACN identifies the supervisor environment using the effective configuration, upstream, and the credential fingerprint required for finalization. When any of these changes, the next launch safely takes over the previous supervisor and continues unfinished finalize jobs in the new environment. A failed finalize can be retried using the user-visible session ID with `acn supervisor retry <session_id>`, or using the job ID shown by `jobs`.

</details>

<details>
<summary><b>TUI commands and key bindings</b></summary>

| Input | Action |
| --- | --- |
| `/help` | Show help |
| `/compact` | Compact context |
| `/copy` | Copy the most recent response |
| `/inbox` | Sync team messages (reports an unconfigured service in standalone mode) |
| `/mcp` | Show MCP status and tools |
| `/ps` | Show background processes |
| `/resume` | Select a previous session |
| `/skills` | Show available Skills |
| `/subagents` | Show subagents |
| `/exit` | End the session |
| `!command` | Run a local shell command |
| `@path` | Attach a file |
| `Ctrl+Enter` | Steer an active turn |
| `Ctrl+V` / `Ctrl+O` | Paste an image / preview attachments |

`/mcp`, `/ps`, and `/subagents` open live panels and remain available while a turn is running.

</details>

## Memory and Claims

### Memory

`MEMORY.md` stores experience and conventions, while `USER.md` stores your preferences. The Agent can update them during a session, and a side review process also helps organize them.

These two types of Memory **never enter the team channel**. Other Agents, Router, and Maintainer cannot read them. Rules that ACN should follow over the long term can be written to `ACN.md` under the configured `~/.acn/config.toml` location.

### Claims

A Claim is a well-supported judgment with a holder, scope, confidence, and evidence summary. For example:

> The statistic-online data source is only complete after 09:30 each day; daily reports generated before then must be marked as provisional. The `holder` is `zhangsan`, the `scope` is `daily-report`, confidence is 0.95, and the evidence is that `the statistic-online source starts updating at 09:00 each day and takes 20–30 minutes`.

Claims do not need to be written manually. ACN generates them automatically when compacting or closing a session and writes a trace recording their origin. **This also happens in standalone mode** and does not depend on team services.

Compared with an entire conversation, a Claim is easier to retrieve and update. Compared with Memory, it is self-contained, so readers do not need access to the original session.

See [Memory Design](docs/memory_design.md) for details.

## Team Mode

You can start in standalone mode. Claims and traces still accumulate locally.

When a second person starts using ACN, deploy Router and Maintainer and configure both endpoints for each Agent. Claims created after that point become visible to the team. During a task, the Agent can call `consult_router`; team messages are synchronized on startup or through `/inbox`.

```toml
[upstreams.team]
agent_id = "your-agent-id"
acn_key_env = "ACN_AUTH_KEY"                  # leave empty when authentication is disabled
maintainer_endpoint = "http://maintainer.example"
router_endpoint = "http://router.example"
```

```bash
export ACN_AUTH_KEY="<team-key>"
acn --upstream team
```

After switching to team mode, only newly created Claims are synchronized. Historical Claims from standalone mode are not uploaded automatically.

Over time, Claims can cite one another. Conflicting judgments are not forcibly merged into a single conclusion; ACN instead records a dispute for later review. Maintainer's management console exposes the overall team state.

The team has no central “truth” that can be forced into every Agent. Policies and Claims from other Agents are inputs. They become an Agent's own judgments only after that Agent actively adopts them in its own context, and different Agents may internalize them differently.

<p align="center">
  <a href="docs/assets/acn-team-claim-flow.webp">
    <img alt="How Agent, Router, Maintainer, and Claims collaborate in ACN team mode" src="docs/assets/acn-team-claim-flow.webp" width="960">
  </a>
</p>

<p align="center">
  <sub>How Claims are discovered and reused in team mode, and how Disputes flow back for review.</sub>
</p>

<details>
<summary><b>Deploy Router and Maintainer</b></summary>

Both are separate binaries in this repository and can use the same configuration file:

```bash
cargo run --bin acn-router     -- --config /path/to/config.toml
cargo run --bin acn-maintainer -- --config /path/to/config.toml
```

The commands above are sufficient for local evaluation. For long-running team deployments, add `--release` for slower compilation but lower runtime overhead and smaller binaries. Router and Maintainer may be deployed separately as long as every Agent can reach both endpoints.

- **Router**: retrieves team Claims and related disputes by scope or semantic similarity. It helps discover information but does not decide what is correct.
- **Maintainer**: receives Claim mirrors and disputes, publishes policies, scans for stale information, and delivers suggestions to each Agent's inbox.

Maintainer also includes a web management console at `/app` by default, covering agents, claims, disputes, policies, sweeps, Router queries, audit records, and team keys. See the [Frontend Guide](frontend/README.md) for build instructions.

See [Configuration Parameters](docs/config_parameters.md) for available options and [Architecture](docs/architecture.md) for the system structure.

</details>

### Upload Boundaries

| | |
| --- | --- |
| **Uploaded** | Claim mirrors; qualifying disputes |
| **Never uploaded** | `MEMORY.md`, `USER.md`, session records, traces |
| **Standalone mode** | Makes no team requests and does not secretly accumulate an upload backlog |

## Data Directory

Private Agent data is stored under `~/.acn` by default and separated by the selected upstream:

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

## Glossary

| Term | Meaning |
| --- | --- |
| **Agent** | The `acn` instance you start, with its own identity, private Memory, and work records |
| **Claim** | A stable judgment that can be retrieved and cited |
| **Trace** | The local-only record of where a Claim came from |
| **Dispute** | A record of conflicting Claims, retained for later review |
| **Policy** | A rule or suggestion published by Maintainer |
| **Router** | The retrieval entry point for team Claims |
| **Maintainer** | Team-side governance and management console |
| **Upstream** | One Agent-side configuration containing identity, team endpoints, and data directory; its name is only a local alias |

## Documentation

- [User Guide](docs/user_guide.md)
- [Configuration Parameters](docs/config_parameters.md)
- [Architecture](docs/architecture.md)
- [Core Behavior and Data Boundaries](docs/core_behavior.md)
- [Memory Design](docs/memory_design.md)
- [Documentation Index](docs/README.md)

## Contributing

Issues and pull requests are welcome. Before submitting a change, we recommend running:

```bash
scripts/check_version_consistency.sh
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo check
```

For TUI changes, include terminal acceptance notes. See [AGENTS.md](AGENTS.md) for project conventions.

## Acknowledgments

Parts of ACN's product and engineering design were inspired by the following open-source projects:

- [OpenAI Codex](https://github.com/openai/codex)
- [Hermes Agent](https://github.com/NousResearch/hermes-agent)
- [GenericAgent](https://github.com/lsdefine/GenericAgent)

Some terminal interaction patterns draw on the publicly presented product experience of [Claude Code](https://code.claude.com/docs/en/overview). These projects and ACN are independent of one another.

## License

MIT OR Apache-2.0

<p>
  <br>
  <a href="https://ft.tech">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/assets/non-convex-ft-tech-logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="docs/assets/non-convex-ft-tech-logo-light.png">
      <img alt="Non-convex ft.tech" src="docs/assets/non-convex-ft-tech-logo-light.png" width="180">
    </picture>
  </a>
</p>
