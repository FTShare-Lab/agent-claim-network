# ACN TUI `!` Shell Command PRD

> 状态：已实现。本文保留用户 shell command 的 transcript、取消与恢复语义。

本文档定义 ACN 交互式 TUI 中 `!` shell command 的产品语义与分阶段实现计划：用户在输入框中以`!` 开头提交命令，系统在本地子进程中执行命令，把命令与输出写入 session transcript，后续真实用户请求触发 LLM 时再把这段终端上下文带入模型。

---

## 背景

`!` 是用户主动发起的本地终端活动，而不是一次普通 LLM prompt。命令完成后，ACN 将结果写成`role=user` 的 `<user_shell_command>` 上下文片段，但本次操作本身不请求模型。

ACN 已有 provider-neutral 的 `SessionTurnMessage`，Anthropic 与 OpenAI-compatible provider 都从同一 canonical transcript 转换请求。因此 `!` 应落在 TUI / session engine 边界，而不是分别写进Anthropic / OpenAI adapter。

---

## 目标

- TUI 支持输入 `!<command>` 执行本地 shell 命令。
- 命令在独立子进程中运行，捕获 exit code、stdout、stderr、duration、truncated 状态。
- 执行结果持久化到当前 session transcript，后续真实用户 turn 能看到。
- `!` 本身不立即触发 LLM 请求，不生成 assistant message。
- `!` shell message 不计入真实用户 turn，不影响 resume 表格的 last user prompt，不触发 memory review 计数。
- 支持 Anthropic 与 OpenAI-compatible provider，尽量复用现有 provider-neutral transcript。
- 默认复用 `agent.tool.workspace_root` 作为命令 cwd，与 `code_run` 行为保持一致。

---

## 非目标

- 暂时不实现持久交互式 shell，不保留 shell 进程。
- 暂时不让 `export` / `cd` 这类 shell 内状态反向修改 ACN 主进程或后续命令环境。
- 暂时不支持后台长任务、PTY、交互式 stdin、实时终端 UI。
- 暂时不实现命令权限审批、read-only 分类、sandbox。
- 暂时不让 `!` 命令触发自动 compact 或自动 memory review。

---

## 用户语义

### 输入

用户在 TUI 输入框提交：

```text
!echo hi
```

ACN 解析为 shell command：

```text
echo hi
```

空命令：

```text
!
```

应显示错误或帮助提示，不写入 transcript，不执行子进程。

### 执行影响

`!` 命令对文件系统、网络、进程等外部世界的副作用是真实的。例如：

```text
!touch tmp.txt
```

会真实创建文件。

但 shell 子进程内的环境变量和 cwd 修改不会影响 ACN 主进程：

```text
!export HTTP_PROXY=http://127.0.0.1:7890
!cd /tmp
```

上述命令只影响各自子进程，不会改变后续 provider 请求，也不会改变下一条 `!` 的 cwd。

---

## Transcript 语义

`!` 命令执行完成后，持久化为一条 `role=user` 的 text message。建议格式：

```xml
<user_shell_command>
<command>
echo hi
</command>
<result>
Exit code: 0
Duration: 0.0123 seconds
Stdout:
hi

Stderr:

</result>
</user_shell_command>
```

约束：

- 持久化层保留独立 message，不把它与下一条真实用户输入合并。
- 该 message 是上下文，不是真实用户 turn。
- 该 message 后面可以紧跟另一条 `role=user` 的真实用户 prompt。

---

## TUI 显示逻辑

`!` 命令的输出应显示在 TUI transcript 中，但不要按普通用户气泡或 assistant 回复展示，也不要把持久化用的 `<user_shell_command>` XML 原文直接渲染给用户。

建议新增独立 shell cell，例如 `HistoryEntry::ShellCommand(ShellCommandCell)`，语义接近当前 tool cell：

```rust
ShellCommandCell {
    command: String,
    status: Running | Completed | Failed | TimedOut | Cancelled,
    exit_code: Option<i32>,
    duration_ms: Option<u128>,
    stdout: String,
    stderr: String,
    truncated: bool,
}
```

### 提交后

用户提交：

```text
!echo hi
```

TUI 不再额外渲染一条普通 user prompt。`UserShellCommandStarted` 到达后，transcript 增加一个 in-progress shell cell：

```text
• shell echo hi
  └ running...
```

同时 status/activity 可显示：

```text
running shell command...
```

### 执行中

暂时不做实时 stdout/stderr streaming。执行期间只显示 running 状态，命令输出等子进程结束后一次性展示。

如果后续要支持实时输出，需要把 executor 从 `Command::output()` 升级为 pipe stdout/stderr 并增量发事件。

### 执行完成

成功且有 stdout：

```text
• shell echo hi
  └ exit 0 in 0.012s
    stdout
    hi
```

成功但无输出：

```text
• shell true
  └ exit 0 in 0.004s
```

非零退出码：

```text
• shell false
  └ exit 1 in 0.006s
```

stdout 和 stderr 都存在时分别展示：

```text
• shell ./script.sh
  └ exit 2 in 0.130s
    stdout
    partial output
    stderr
    error detail
```

超时：

```text
• shell sleep 999
  └ timed out after 60s
```

用户取消：

```text
• shell long-running-command
  └ cancelled
```

### 截断规则

- TUI 展示内容与写入 transcript 的内容使用同一份截断后的 stdout/stderr。
- 如果输出超过 `max_output_chars`，cell 末尾显示截断提示：

```text
    ... output truncated to 20000 chars
```

- 截断不应改变 exit code / duration / timeout 状态。

### 滚动与队列

- shell cell 是 finalized history entry，完成后进入正常 scrollback。
- shell task 运行时，后续输入沿用现有 input queue 机制排队。
- shell task 完成后，TUI 更新 shell cell，然后继续 dispatch 队列里的下一条输入。

### 取消语义

`!` shell task 运行时允许用户中断，但需要保留 TUI 现有输入编辑与 queued input 恢复优先级：

- `Esc`：如果 queued input 队列非空，先取出最后一个 queued input 恢复到输入栏，不取消 shell task。
- `Esc`：如果 queued input 队列为空且当前 shell task 正在运行，取消 shell task，kill 子进程，shell cell 更新为 `cancelled`。
- `Esc`：如果 queued input 队列为空且没有 shell task，再走 TUI 现有 escape 逻辑。
- `Ctrl-C`：如果输入栏当前有内容，先清空输入栏，不取消 shell task。
- `Ctrl-C`：如果输入栏为空且当前 shell task 正在运行，取消 shell task，kill 子进程，shell cell 更新为 `cancelled`。
- `Ctrl-C`：如果输入栏为空且没有 shell task，再走 TUI 现有退出 / 中断逻辑。
- `timeout_secs` 到期时自动 kill 子进程，shell cell 更新为 `timed out after Ns`。

实现时不能只 abort tokio task；必须确保子进程被 kill，避免 UI 显示已取消但命令仍在后台运行。

### Resume 显示

resume 历史时，暂时不要求重建历史 shell cell。`extract_last_n_turns` 应跳过 shell message，避免 shell record 出现在最近真实对话摘要中。完整历史仍保存在 `messages.jsonl`，后续真实 turn 会进入 LLM context。

如未来需要完整 transcript replay，再从 `<user_shell_command>` message 解析回 shell cell。

---

## Provider 请求规范

后续真实用户 turn 发起 LLM 请求时，provider 请求前允许对 canonical history 做轻量 normalize：

- 只合并相邻的 `role == "user"`。
- 只合并双方均为纯 `Text` block 的 messages。
- 不合并包含 `ToolResult` 的 user message。
- 不跨越 assistant message。
- 合并时使用明确分隔：

```text
<user_shell_command>...</user_shell_command>

用户的新问题...
```

这样持久化仍忠实记录事件边界，API 层则避免连续 user messages 带来的 provider 兼容性风险。

该 normalize 应放在 provider request 构造前的 canonical 层，并让 ctx 估算与实际 provider request 复用同一份 normalized messages。不要在 session storage 层改写历史，也不要分散到单个 provider adapter 内实现。

---

## Context 与 Compaction

`!` 命令完成后：

- 不立即更新 TUI ctx 估算。
- 不触发自动 compact。
- 不触发 fork memory review。

下一次真实用户 turn 到来时，现有 turn 流程会读取完整 messages 并构造 history；这时 `<user_shell_command>` 会自然进入上下文估算与 provider 请求。当前主线已经将 provider-neutral token 粗估集中在 `src/api/token_estimate.rs`，并在 turn loop 中按 provider 能力选择 provider usage 或本地 estimate 上报 ctx。因此 `!` 不需要新增独立 ctx 更新路径，只要保证下一次真实 turn 构造 history 时保留 shell record。

若上下文超限或达到 auto compact 条件，也在真实用户 turn 的常规路径中处理。当前自动压缩触发逻辑优先使用本次 turn 的 provider ctx usage；如果没有 provider usage，则 fallback 到本地 estimate。`!` 后不立即估算，意味着它不会单独触发这段逻辑。

风险与缓解：

- `!` 后 UI ctx 显示可能暂时偏低。
- 通过 `max_output_chars` 控制 shell 输出写入 transcript 的最大体积。
- 后续如发现大量 shell 输出常导致下一 turn 失败，再增加执行后提示或手动 compact 建议。

---

## 配置

新增配置建议：

```toml
[agent.session.user_shell]
enabled = true
timeout_secs = 180
max_output_chars = 100000
shell = "auto"
login_shell = true
```

字段含义：

| 字段 | 类型 | 默认值 | 含义 |
|---|---:|---:|---|
| `enabled` | bool | `true` | 是否允许 TUI 使用 `!` shell command |
| `timeout_secs` | u64 | `60` | 单条命令最长运行秒数，超时 kill 子进程 |
| `max_output_chars` | usize | `20000` | 写入 transcript 的 stdout/stderr 总字符上限 |
| `shell` | string | `"auto"` | shell 选择策略或具体 shell |
| `login_shell` | bool | `true` | Unix shell 是否用 login shell 参数执行 |

### `shell` 支持值

暂时支持：

```toml
shell = "auto"
shell = "sh"
shell = "bash"
shell = "zsh"
shell = "pwsh"
shell = "powershell"
shell = "cmd"
shell = "/bin/zsh"
```

语义：

- `auto`：Unix/macOS 优先 `$SHELL`，为空则 `/bin/sh`；Windows 优先 `pwsh`，再 `powershell`，最后 `cmd`。
- `sh` / `bash` / `zsh`：通过 PATH 查找，执行参数由 `login_shell` 决定。
- `pwsh` / `powershell`：使用 `-Command`，`login_shell` 不生效。
- `cmd`：使用 `/C`，`login_shell` 不生效。
- 绝对路径：按 basename 识别 shell 类型；识别不到时按 Unix shell 处理。

### `login_shell`

暂时只支持 bool：

- `true`：Unix shell 使用 `-lc`，例如 `/bin/zsh -lc '<cmd>'`。
- `false`：Unix shell 使用 `-c`，例如 `/bin/sh -c '<cmd>'`。

PowerShell / cmd 上忽略该字段。

暂时不支持数组形式：

```toml
shell = ["nu", "-c"]
```

如未来需要 nushell / fish / 自定义参数，再单独扩展。

---

## 分阶段计划

### Phase 0: 设计落档

目标：

- 固化产品语义、配置、transcript 格式、上下文更新策略。
- 明确暂时不做持久 shell、不做权限审批、不做 PTY。

产出：

- `docs/PRDs/PRD_shell_command.md`

验收：

- 文档明确 `!` 不是真实用户 turn。
- 文档明确 `!` 不立即触发 LLM。
- 文档明确 `export` 不影响主进程。

### Phase 1: 配置与命令执行器

目标：

- 新增 `UserShellConfig`。
- 新增 shell command executor。

实现建议：

- 在 `config.rs` 中新增 `agent.session.user_shell` 配置结构。
- 将该配置挂在现有 `AgentSessionConfig` 下，与 `compaction` / `memory_review` 并列。
- 在配置校验中要求 `timeout_secs > 0`、`max_output_chars > 0`。
- 复用 `agent.tool.workspace_root` 作为 cwd，不新增 cwd 配置。
- 新增模块，例如 `src/agent/user_shell.rs` 或 `src/session_shell.rs`。
- 使用 `tokio::process::Command`。
- 使用 `tokio::time::timeout` 或 `tokio::select!` 处理 timeout。
- 超时或取消时 kill child。
- 捕获 stdout/stderr，按 `max_output_chars` 截断。

验收：

- `echo hi` 能得到 stdout。
- 非零退出码能记录 exit code。
- timeout 能结束子进程。
- cwd 为 `workspace_root`。
- 输出超限会截断并标记 `truncated = true`。

### Phase 2: SessionEngine 持久化 API

目标：

- 新增 session-level API 执行 `!` 并追加 transcript。

实现建议：

- 在 `SessionEngine` 新增：

```rust
run_user_shell_command(&self, session: &mut SessionHandle, command: String, emit: impl FnMut(SessionEvent))
```

- 执行前检查 session 未关闭。
- 执行完成后使用 `NewSessionMessage::text_with_model(SessionMessageRole::User, record, self.session_model.clone())` 追加一条 message。
- 不调用 `turn_loop.run_session_turn`。
- 不触发 fork memory review。
- 不触发 auto compact。
- 不更新 ctx。

新增事件建议：

```rust
UserShellCommandStarted { command: String }
UserShellCommandCompleted {
    command: String,
    exit_code: Option<i32>,
    duration_ms: u128,
    stdout: String,
    stderr: String,
    truncated: bool,
    message_count: usize,
}
UserShellCommandFailed { command: String, error: String }
```

验收：

- shell command 成功后 `messages.jsonl` 增加一条 `role=user` message。
- 不增加 assistant message。
- `message_count` 更新。
- `turn_count` 不应更新。

### Phase 3: TUI 输入与 worker 调度

目标：

- TUI 识别 `!` 并以 session task 方式执行。
- TUI 用独立 shell cell 展示命令状态与输出，不显示原始 `<user_shell_command>` XML。

实现建议：

- 在 `InputAction` 增加 `ShellCommand(String)`。
- `classify_input` 识别 `!` 前缀。
- `dispatch_input` 中新增 `start_user_shell_command`。
- `runtime.rs` 新增 `ActiveSessionTask::UserShellCommand(task_id)` 或等价任务类型。
- 新增 `WorkerEvent::UserShellCommandFinished`。
- 在 transcript/cell 层新增 shell command cell，或复用 tool cell 的渲染模式但保持独立类型。
- 暂时保持现有队列模型：有任何 session task running 时，后续输入排队。
- `Esc` 遵循 queued input 优先级：队列非空时先取出最后一个 queued input 恢复到输入栏，队列为空时才取消 shell task。
- `Ctrl-C` 遵循输入栏优先级：有草稿时先清空草稿，输入栏为空时才取消 shell task。

UI 显示建议：

- `UserShellCommandStarted` 时新增 in-progress shell cell。
- `UserShellCommandCompleted` 时更新同一个 shell cell，显示 exit code、duration、stdout/stderr 摘要。
- `UserShellCommandFailed` / timeout / cancelled 时更新同一个 shell cell，显示错误状态。
- 暂时不做实时 stdout/stderr streaming，输出在命令完成后一次性出现。

验收：

- 输入 `!echo hi` 不触发 assistant。
- TUI transcript 显示 shell cell，而不是普通 user 气泡。
- 命令完成后 stdout/stderr 出现在 shell cell 中。
- TUI 不直接显示 `<user_shell_command>` XML。
- 执行期间普通输入会进入队列。
- 执行完成后自动 dispatch 队列中的下一条输入。
- `Esc` 在 queued input 队列非空时只恢复最后一个 queued input，不取消 shell task。
- `Esc` 在 queued input 队列为空时取消运行中的 shell task，并确保子进程被 kill。
- `Ctrl-C` 在输入栏有内容时只清空输入栏，不取消 shell task。
- `Ctrl-C` 在输入栏为空时取消运行中的 shell task，并确保子进程被 kill。
- `/help` 补充 `!` 用法说明。

### Phase 4: 真实用户 turn 识别修正

目标：

- shell transcript message 不被当作真实用户 turn。

实现建议：

- 新增 helper：

```rust
is_user_shell_command_message(message: &SessionMessage) -> bool
```

- 判断条件：
  - `role == User`
  - text 内容以 `<user_shell_command>` 开始并包含 `</user_shell_command>`

- 修改以下路径：
  - `count_real_user_turns`
  - `extract_last_n_turns`
  - `extract_last_user_text`
  - `is_memory_review_user_turn`
  - `is_real_user_turn`
  - compaction tail turn selection

验收：

- resume 后 last user prompt 不显示 `!` shell record。
- `turn_count` 不因 `!` 增加。
- memory review cadence 不因 `!` 推进。
- compaction 的 tail real turns 不把 `!` 当用户需求边界。

### Phase 5: Provider 请求 normalize

目标：

- 对 provider request 做窄规则的相邻 user text 合并。
- 让 ctx 估算与实际 provider request 使用同一份 normalized messages，避免 statusline / auto compact 判断与实际请求形状不一致。

实现建议：

- 新增 canonical helper：

```rust
normalize_provider_messages(messages: Vec<SessionTurnMessage>) -> Vec<SessionTurnMessage>
```

- 只合并相邻 pure-text user messages。
- 不合并 `ToolResult`。
- 不改变持久化 messages。
- 优先在 `AgentTurnLoop::call_provider` 内、调用 `estimate_provider_request_context_tokens` 和 `ProviderAdapter::send` 之前统一调用。
- `AgentTurnLoop::estimate_context_tokens` 也应复用同一个 normalize helper。
- 不建议放到单个 provider adapter 内，否则不同 provider 的 ctx estimate 与请求 messages 可能不一致。

验收：

- shell record + 下一条真实 user prompt 在 provider request 中可合并为一条 text user message。
- assistant / tool_result 边界不被破坏。
- Anthropic 与 OpenAI-compatible adapter 测试通过。
- preflight ctx estimate、`estimate_context_tokens` 与 provider request 基于同一 normalized history。

### Phase 6: 测试与验证

测试建议：

- `classify_input("!echo hi")` 返回 `ShellCommand("echo hi")`。
- `classify_input("!")` 不执行命令。
- shell executor 捕获 stdout/stderr/exit code。
- shell executor timeout 会 kill child。
- shell executor 使用 workspace root。
- `run_user_shell_command` 只追加一条 user message。
- `run_user_shell_command` 不触发 LLM provider。
- shell message 不计入 `count_real_user_turns`。
- resume last user text 跳过 shell message。
- memory review / compaction real user turn helper 跳过 shell message。
- provider normalize 只合并 pure-text user messages。
- provider normalize 不合并 tool result message。
- TUI worker 完成后继续 dispatch queued input。

验证命令：

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
cargo check
```

涉及 TUI 行为时，使用项目 TUI smoke test 流程补充验证。

完成后，使用 code-review skill 检查 shell 执行、会话持久化、provider normalize 与 TUI 调度边界，确认不存在未处理的高风险问题。

---

## 关键风险

- Shell 命令是高权限本地执行能力，暂时没有 sandbox / 审批，应默认只在可信本地环境使用。
- 大输出会挤占 context，通过 `max_output_chars` 控制。
- `login_shell = true` 更贴近日常终端，但也可能加载用户 shell 配置中的副作用。
- Windows shell 支持暂时为 best-effort，主要路径优先保证 Unix/macOS。
- XML 标签内容需要避免输出伪造结构导致模型误读；当前对 command/stdout/stderr 做 XML text escape，后续如需要结构化 replay 可考虑改成 JSON payload 或 CDATA 风格封装。

---

## 开放问题

- `enabled` 默认是否应为 `true`，还是为了安全默认 `false`。
- `max_output_chars` 默认已调整为 100000；后续如发现 transcript 压力过大，再结合 compact 策略重评。
- UI 是否需要显示 stdout/stderr 全量，还是只显示截断后的 transcript 内容。
- 是否需要 `/help` 明确提示 `!` 是高权限本地执行。
- 后续是否引入权限策略，例如只允许 read-only 命令免确认。
