# PRD: 并发工具调用

> 状态：已实现。本文保留工具分类、批次调度、取消和 TUI 投影决策。

本文记录 ACN 通用工具并发截至当前已经拍板的产品语义、调度边界与工具分类。

这里的“并发安全”是**调度资格**，不是权限授予、沙箱证明或“这个工具没有任何外部副作用”的泛化承诺。工具只有在其本次合法输入满足 `is_concurrency_safe(input) == true` 时，才可能和相邻的同类调用并发执行；无法证明时必须返回 `false`。

---

## 目标与非目标

目标：在一个 assistant 响应中，允许多个彼此独立、只读或观察性工具调用并发完成，缩短多文件读取、检索与查询的等待时间；同时让具有会话、副作用或生命周期语义的调用保留原始顺序。

本期不做：

- 不在 provider streaming 尚未结束时启动工具。必须先完整接收本轮 assistant 响应及全部 tool call，再开始调度。
- 不构建路径资源 DAG，不根据多个调用的目标路径推导并发关系。
- 不新增跨 agent、跨 session 或跨进程的全局工具信号量、文件 lease 或 worktree 协调。
- 不改变现有各工具自身的参数校验、文件写入保护或其他内部完整性机制；本 PRD 只增加普通tool-call 编排层的并发调度。
- 不以 OpenAI 的 `parallel_tool_calls` 等 provider 请求参数作为正确性前提。上游是否一次发出多个 tool call 由 provider 决定；ACN 只负责安全地执行已经收到的调用。

## 实施阶段与验收门槛

实施者在进入每一个新阶段前必须重新通读本 PRD；若代码现状与本文存在冲突，不得悄然改变已拍板语义，应先修正实现方案或升级为新的产品决策。

### 阶段 0：基线与落点确认

- 追踪当前 provider response 接纳、tool dispatch、取消边界、journal 和 TUI ToolCell 的完整路径，明确每个改动落点。
- 记录并保留现有串行回归行为，避免并发改造改变非 tool、单工具或 Barrier 的语义。

验收：现有基线测试可运行；实现计划能够逐项映射到本文的调度、分类、取消、上游回传和 TUI 条款。

### 阶段 1：分类、配置与调度器

- 增加 `max_parallel_tool_calls = 5` 配置和正整数校验。
- 按本节定义的本地输入验证边界实现 fail-closed 的 `is_concurrency_safe`，包括 MCP `readOnlyHint` 和 `code_run` 的 tree-sitter Bash 分类。
- 将完整 provider response 按 source index 分成并发批次与 Barrier 步骤；并发批次使用有界任务池，但保持 source-order 的 tool result 回传与既有取消边界。

验收：分类矩阵、Bash 正反例、上限、批次边界、失败不互相取消、取消前后派发边界和 source-order结果都有自动化测试；单工具及全 Barrier 调用行为与改造前一致。

### 阶段 2：事件、TUI 与会话投影

- 使 `ToolCallStarted` 只代表实际派发；每个 Started 调用都可靠地收束为 Completed、Interrupted 或Skipped。
- 调整 TUI/journal 投影，使并发 ToolCell 固定按 source index 显示、终态原位更新，且排队或 Barrier后的调用不提前显示为 Calling。
- 更新 system prompt 中的子代理等待纪律，不改变 `wait_subagents` 的串行分类。

验收：状态机与 journal/recovery 测试覆盖并发开始、乱序完成、取消、skipped 和固定 ToolCell 顺序；tmux 定向场景确认多条 Calling、原位完成和取消等待边界的真实终端显示。

### 阶段 3：整体验收与独立审查

- 运行格式化、clippy、全量测试、类型检查及 TUI smoke test。
- 在真实 LLM 配置下通过 TUI 进行针对性验收，验证模型实际发出相邻安全工具调用时的并发显示、source-order 回传和后续回答。
- 使用 code-review skill 检查“调度/分类”“取消/provider/journal”“TUI/prompt/配置”三个风险域。

验收：不存在未处理的高风险问题；所有自动化、TUI smoke 和真实 LLM 定向验收通过；最终实现逐条对照本文，不新增本文以外的并发语义。

---

## 核心模型

每个 ACN 原生工具和每个 MCP 工具都可以被调度器询问：

```text
is_concurrency_safe(tool_name, validated_input) -> bool
```

规则：

- `true`：调用可加入与其相邻的 `true` 调用形成的并发批次。
- `false`：调用单独串行执行，并把前后的并发批次隔开。
- 工具未知、ACN 原生工具 input 无法按参数结构解析、分类器报错或任何不确定情况：一律按`false` 处理。MCP input 的例外边界见工具分类矩阵。
- 分类只依据本次已校验的 input 和工具实现契约，不由模型声明“这是只读”决定。

例如，调用序列：

```text
file_read, web_fetch, file_write, web_search, file_read
```

调度结果为：

```text
[file_read, web_fetch] 并发
file_write              串行
[web_search, file_read] 并发
```

ACN 采用“连续可并发调用分批”模型，而不是全局读写锁。批次资格完全由 ACN 的工具分类契约决定。

---

## 调度时机与算法

### 先完整收流，再执行

ACN 不在 streaming 过程中提前执行工具。一个 assistant 响应只有在 provider 明确结束、所有tool input 都完整可解析后，才能进入本节调度。

因此：

- 流式文本、thinking 和 tool input 的半成品不会触发本地副作用。
- `max_parallel_tool_calls` 对每一个实际并发批次都生效。

### 非流式 fallback 边界

一次初始 streaming 尝试及其后的非流式 fallback 链，在本 PRD 中视为一次逻辑 provider call。只有最终完整成功、并通过 assistant message、tool id 和 provider stop 终态校验的`ProviderResponse`，才能生成 tool batch；其中 tool call 的 content 顺序、source index 和call id 是唯一权威来源。

失败 streaming 尝试中的文本、thinking、任何未形成完整响应的 tool input，以及失败或未通过校验的 fallback 响应，均不得触发工具分类、`ToolCallStarted`、并发批次或 tool result；也不得写入 provider history 或 canonical transcript。fallback 成功时，必须先持久化并发出 partial 的TUI/journal 原位替换事件（tool-only 响应时清空 partial），才允许调度首个工具批次；fallback耗尽或被取消时不创建工具批次。

这只定义 fallback 与工具调度的边界。它的触发条件、固定次数、退避、journal/recovery 与 TUI细节以 [PRD_retry_non_streaming.md](PRD_retry_non_streaming.md) 为准。已经在较早逻辑 provider call 中完成的工具仍是既成事实，fallback 不回滚或重复执行它们。

### 批次执行

调度器保留本次被接纳的完整 `ProviderResponse` 中 tool call 的 source index 与 call id，并按source index 线性分段：

1. 对 ACN 原生工具先验证工具名与本地 input 参数结构，再计算 `is_concurrency_safe`；MCP 只在本地确认 input 为 JSON object、工具可见且只读 annotation 有效，具体参数 schema 由 MCP server 在执行时校验。
2. 连续 `true` 调用组成一个并发批次；每个 `false` 调用独占一个串行步骤。
3. 并发批次使用有界任务池执行，活跃调用数不得超过配置上限。
4. 并发批次必须等到全部调用都进入成功、业务失败、超时或取消等终态后，才构造下一次上游tool result 回环。
5. 串行调用完成后，才开始它之后的批次。

调度器不在调用之间额外建立资源锁或依赖图。不同 agent 的 tool batch 也彼此不阻塞；这是已知且接受的边界，而非本期要通过工具编排解决的问题。

---

## 配置

新增配置位于 `[agent.tool]`：

```toml
[agent.tool]
max_parallel_tool_calls = 5
```

语义：

- 默认值为 `5`，必须是大于 0 的整数。
- 它限制**一个 agent 当前 turn 中一个连续并发批次**的活跃调用数。
- 它不是跨 turn、跨 session 或跨 agent 的全局配额；parent 和子 agent 各自的批次可以同时消耗各自的上限。
- 它与非流式 fallback 的最多 5 次尝试完全独立；两个数值当前相同只是巧合，前者不限制或影响后者。
- 暂时不增加 web、MCP server 或工具类别的子配额。实际观察到特定 provider 的 429、超时或不耐并发后，再单独增加有证据的限流设计。

ACN 通过配置结构提供并发上限，并执行显式正整数校验，以避免无效或负值造成调度器停滞；不增加额外的环境变量入口。

---

## 工具分类矩阵

下表是当前已拍板的分类。`true` 只表示可与相邻的 `true` 调用并发，不改变工具原本的可用性或权限。

| 工具 | 条件 | `is_concurrency_safe` | 原因 |
| --- | --- | --- | --- |
| `file_read` | 任意合法 input | `true` | 只读取目标内容。 |
| `file_patch` | 任意 input | `false` | 修改文件，必须保持 source order。 |
| `file_write` | 任意 input | `false` | 修改文件，必须保持 source order。 |
| `code_run` | `type` 缺省/`bash`，且脚本满足下述 Bash 只读分类 | `true` | 只允许可静态证明为只读的 Bash 子集。 |
| `code_run` | `python`、`powershell`、未知 type 或 Bash 分类失败 | `false` | 任意代码、未知语法或未白名单命令保持串行。 |
| `web_search` | 任意合法 input | `true` | 观察性外部检索。 |
| `web_fetch` | 任意合法 input | `true` | HTTP GET 读取。 |
| `web_request` | 所有 method | `false` | 暂时不根据 HTTP method 猜测幂等或只读性。 |
| `working_note` | `action = "list"` | `true` | 仅查看当前 note。 |
| `working_note` | `add`、`clear` 或未知 action | `false` | 修改工作笔记或语义不明。 |
| `ask_user` | 当前通知式实现 | `true` | 当前只把“需要用户输入”的信息写回模型，不真实等待用户。 |
| `memory` | 任意 input | `false` | 读写 agent 私有记忆，避免与同轮状态观察交错。 |
| `consult_router` | 当前内置实现 | `true` | 只查询或刷新内部派生/检索缓存；不得写 claim、dispute、session 或用户可见状态。 |
| `session_search` | 任意 input | `false` | session 索引与本轮 canonical history 具有时序关系，暂时保守串行。 |
| `create_subagent` | 任意 input | `false` | 创建会改变后续 `list/read/wait` 生命周期观察。 |
| `list_subagents` | 任意 input | `true` | 只读取当前时刻的 delegation 快照。 |
| `read_subagent` | 任意 input | `true` | 只读取 delegation 的持久化状态或结果。 |
| `wait_subagents` | 任意 input | `false` | 这是主 agent 的阻塞性生命周期观察，不与同轮其他工具交织。 |
| `steer_subagent` | 任意 input | `false` | 向运行中 delegation 写入新的指令。 |
| `update_subagent_progress` | 任意 input | `false` | 写入 delegation 进度并可能唤醒等待者。 |
| MCP 工具 | input 为 JSON object，且原始 MCP `annotations.readOnlyHint == true` | `true` | server 显式声明只读；ACN 不再自行猜测参数语义或名称。 |
| MCP 工具 | annotation 缺失、为 `false` 或无法读取 | `false` | fail-closed。 |

`consult_router = true` 依赖上述实现契约。将来若该工具加入 claim/dispute 写入、session 副作用或任何不可并发的刷新路径，必须同时改为 `false` 或拆分为不同工具。

当前 `ask_user` 也仅因其为占位实现而为 `true`。后续若实现真实的用户输入等待、审批或elicitation，它必须改为 `false`。

MCP 的本地验证边界是已拍板的例外：ACN 会拒绝非 object input、不可见工具、无效的工具 schema文档和非只读 annotation，但不在调度前用任意 MCP JSON Schema 验证本次 arguments。arguments不符合 server schema 时，允许调用进入只读并发批次，由 MCP server 返回该 call 自己的普通失败结果；失败随完整 batch 一起按 source order 回传，不取消同批其他调用。

### `code_run` 的 input-dependent Bash 分类

`code_run` 不能因工具名称看起来像“运行命令”就标记为可并发。它采用 input-dependent、fail-closed 的分类：

```text
input 无法反序列化 / script 为空 / type 未知  => false
type 缺省或 "bash"                              => 检查 Bash AST 与白名单
type = "python" 或 "powershell"                 => false
```

`cwd` 和 `timeout` 不改变这个分类。`cwd` 只决定本次 child process 的启动目录；当前实现为每次调用独立设置 `Command::current_dir(cwd)` 后运行 `bash -lc`，命令内的 `cd` 不会回写 agent 或其他并发调用的 cwd。因此并发调用之间不存在共享 cwd 的回写语义。

#### 解析与组合规则

实现使用 `tree-sitter-bash` 生成 Bash AST，出现语法错误或未明确允许的结构即返回 `false`。它不是简单检查脚本中是否出现 `rm` 等字符串，也不使用自定义 regex 解析来决定并发资格。

若每一个叶子 simple command 都通过“命令与 flag 白名单”，则下列组合节点可以递归判为 `true`：

- 控制列表：`&&`、`||`、`;` 和换行；
- 管道：`|`，但每一个 pipeline stage 都必须是白名单 simple command；
- 输入重定向：`< literal-file`；
- stderr 合并：精确的 `2>&1`；
- `cd <literal-directory>`：可作为上述列表中的一个 simple command。它只改变当前 child shell的 cwd；`cd` 必须显式带一个无展开的字面量目录，不能使用 bare `cd`、`cd -` 或 `~`。

例如，以下调用可并发：

```bash
cd src && rg -n "ToolRegistry" . | head -n 20
rg -n "TODO" src && git status --short
grep -R -n --include '*.md' parallel docs || true
```

以下 shell 结构一律 `false`：

- 输出或双向重定向：`>`、`>>`、`>|`、`<>`、`2>`、`&>` 等；
- 后台化与脱离生命周期：`&`、`nohup`、`disown`、`setsid`；
- heredoc、here-string；
- 变量/参数/tilde 展开、未引用 glob、brace expansion：`$VAR`、`${...}`、`~`、`*`、`?`、`[]`、`{a,b}`；
- command substitution、反引号和 process substitution：`$(...)`、`` `...` ``、`<(...)`、`>(...)`；
- `source`、`.`、`eval`、函数定义、循环、条件、case、subshell；
- 任意环境变量赋值，例如 `FOO=bar rg ...`。

其中有些结构理论上可能在个别输入下无副作用；暂时仍拒绝，是因为静态分类器无法可靠得出最终argv 或被执行的子命令。`false` 只会让该 `code_run` 串行，不会拒绝其原有功能。

#### Bash simple-command 白名单

白名单以“命令名 + 明确 flag 集合 + 无展开字面量参数”定义；所有未列出的 flag 都为 `false`。可引用字面量参数，例如 `--glob '*.rs'`，但引用内容中不得含 parameter/command expansion。短 flag 只有在每个组成 flag 都在允许集合且不需要参数时才允许合并。

| 命令 | 可允许的形式/flag |
| --- | --- |
| `pwd` | 仅无参数。 |
| `true`、`false` | 仅无参数。 |
| `ls` | `-a`、`-A`、`-l`、`-h`、`-1`、`-d`、`-R`，以及 `--all`、`--almost-all`、`--long`、`--human-readable`、`--one-per-line`、`--directory`、`--recursive`；后接字面量路径。 |
| `rg` | `-n`、`-i`、`-S`、`-F`、`-w`、`-x`、`-l`、`-c`、`--files`、`--hidden`、`--no-heading`、`--no-ignore`；`-A/-B/-C <number>`、`--glob/-g <literal>`、`--type <name>`、`--type-not <name>`、`--max-count <number>`。 |
| `grep` | `-n`、`-i`、`-E`、`-F`、`-w`、`-x`、`-v`、`-l`、`-c`、`-r`、`-R`；`-m <number>`、`--include/--exclude/--exclude-dir <literal>`。 |
| `cat` | 无 flag，或 `-n`、`-b`、`-s`、`-E`、`-T`。 |
| `head`、`tail` | 无 flag，或 `-n/--lines <number>`、`-c <number>`；不允许 `tail -f`、`--follow`。 |
| `wc` | `-l`、`-w`、`-c`、`-m`、`-L`。 |
| `stat` | 暂时设定为仅无 flag。 |
| `file` | 无 flag，或 `-b/--brief`、`--mime`、`--mime-type`。 |
| `cd` | 一个显式、无展开的字面量目录；仅改变 child shell cwd。 |

这份集合刻意保持最小化。复合命令中的所有子命令均需通过分类，任何未知命令或参数都拒绝并发；分类器只依据本文定义的 Bash 语法、命令和 flag 约束，不引入额外的 shell-quote、安全状态或sandbox 假设。

以下命令暂时明确为 `false`，即使某些看起来像只读：

- 所有 `git` 命令，包括 `git status`、`git diff`；
- 网络命令，如 `curl`、`wget`；应使用 `web_fetch` / `web_search`；
- 解释器与代码执行，如 `python`、`node`、`ruby`、`bash`、`sh`；
- 构建、测试和包管理，如 `cargo`、`npm`、`make`、`pytest`；
- `find`、`sed`、`awk`、`jq`、`xargs`、`tee` 以及所有未列出的命令；
- 明显修改命令，如 `touch`、`rm`、`mkdir`、`cp`、`mv`、`chmod`、`chown`、`dd`。

不放开 Git 的原因不是 `cd` 会修改 agent cwd。ACN 的 `cwd` 参数可以直接选择执行目录，为 `cd + git` 增加特判会留下等价绕过路径；Git 的 config、hook、index 等隐式行为也不应被当成可并发观察。

#### runner 边界

本期**保留当前 `bash -lc` 行为**，不为并发分类改变 `code_run` runner。因而 `true` 的准确含义是：

> 在可信本地 shell 环境下，模型提供的 Bash 脚本文本被保守判为只读，适合并发调度。

它不是强 sandbox 保证。login profile、alias、shell function 或 `BASH_ENV` 可以改变外部命令的实际行为；若未来要把 `true` 提升为“整个进程绝无副作用”的承诺，应另行把 runner 全局改为受控的 no-profile/no-rc 环境，并处理 PATH 与环境变量边界。

---

## 子代理使用纪律

`create_subagent`、`steer_subagent`、`update_subagent_progress` 和 `wait_subagents` 都保持串行。这不意味着主 agent 必须在子代理运行期间空等。

system prompt 应明确要求主 agent：

```text
创建子 agent 后，先继续所有不依赖其结果的主线工作；
只有下一步确实被结果阻塞时，才调用 wait_subagents；
wait 返回后，再在下一轮读取子 agent 结果。
```

这比把 `wait_subagents` 标成可并发更容易理解：调用 `wait` 的 assistant 回合明确把控制权交给等待动作；需要并行推进的其他工作应由模型在调用 `wait` 前先完成或另行委托。

---

## 结果、失败、取消与 TUI

### 上游回传

并发只改变本地执行时序，不改变 provider 看到的 tool-use 因果关系：

- 一次并发批次内的 tool result 必须在全批次结算后，按本次被接纳的完整 `ProviderResponse` 中tool call 的 source index 和 call id 顺序交给上游 adapter。
- 调度器不能把先完成的一个结果提前发起下一次 LLM 请求，也不能把半个批次写入 canonical transcript。
- ACN 的公共内部消息模型不新增通用 `role = tool`。各 provider adapter 保持自身协议：例如Anthropic Messages 使用带 `tool_result` content block 的 user 消息；其他 adapter 只在其自身协议要求时采用对应表示。

### 失败与取消

- 一个工具的业务失败、执行错误或单项超时只生成该 call 自己的失败结果，不取消同一并发批次中的其他调用。
- 用户/turn 取消才取消尚未完成的并发调用；取消后的旧 turn 不得以不完整批次继续请求 LLM。
- 第一期的取消边界是**实际派发前**，不是调度器对已开始调用的强制终止承诺。取消请求发出后，TUI 立即显示 `turn cancel pending: waiting for current turn boundary`；尚未实际派发的调用一律`ToolCallSkipped`。已经 `ToolCallStarted` 的调用必须继续等待真实终态：能够响应其 cancellation token 的工具以 `ToolCallInterrupted` 收束；尚未支持协作取消的工具允许以 `ToolCallCompleted`收束，不能被伪造为已中断。
- 并发批次中，这个“当前 turn 边界”是所有已开始调用都已收束的时刻，而不是其中任一个先结束的时刻。取消后，即使某个已开始调用随后完成，其结果也只用于 TUI/journal 的终态收束；不得写入canonical transcript、构造 tool result 或发起旧 turn 的 LLM 回环。
- `ToolCallStarted` 的含义是“已经实际派发执行”，不是“provider 提议了调用”或“调用已加入等待队列”。对并发批次而言，只有调用获得活跃任务位、即将启动实际 tool task 时才可发出该事件。
- 若取消或 steer 在某个调用实际派发前已生效，必须对该调用发出终态 `ToolCallSkipped`：取消使用`turn_cancelled_before_dispatch`，steer 使用 `turn_interrupted_before_dispatch`。此路径不得发送`ToolCallStarted`、执行工具、生成 tool result 或继续旧 turn 的 LLM 回环。它覆盖完整 provider response（包括成功 fallback）刚接纳后、队列等待中，以及前一调用结束后发现中断的所有边界。
- 已发出 `ToolCallStarted` 的调用仍须以 `ToolCallCompleted` 或 `ToolCallInterrupted` 收束；若它恰在取消竞争中完成，则完成事件优先，尚未派发的其余调用改为 `ToolCallSkipped`。每个调用都必须有一个可呈现的终态，避免 TUI 残留“执行中”。

### TUI

- 每个已经实际派发的调用独占一个 ToolCell；同一工具被调用多次时也不得合并，通过各自的输入摘要区分。一个并发批次开始后，TUI 可同时显示多条 `Calling <tool>`。
- ToolCell 的纵向位置按完整 provider response 中的 source index 固定。工具完成、失败或取消时，TUI 按真实终态到达时间立即原位更新对应 ToolCell，不按完成顺序移动或重新排列已有条目。
- provider 回传与 canonical transcript 同样按原始 source order 处理；TUI 的状态更新时间可以与该顺序不同，但工具条目的固定顺序一致。
- 并发批次超过 `max_parallel_tool_calls` 时，尚未获得活跃任务位的调用不显示 `Calling`。已有调用释放任务位后，等待调用按 source index 顺序获得任务位，实际派发时再追加对应 ToolCell。
- Barrier 调用只有在它之前的并发批次全部收束并轮到其实际派发时，才显示 `Calling`；它之后的调用也不得提前显示。
- `ToolCallSkipped` 是终态：TUI 显示 Skipped 和原因，不显示 Calling 或运行时长。journal/recovery应投影为 `tools_skipped`；`tools_pending_or_skipped` 只保留给旧 journal 中“已 Started 但崩溃前未写终态”的兼容恢复，不能用于新的未派发调用。

---

## 验收

- 相邻 `true` 调用可同时开始，活跃数不超过 `max_parallel_tool_calls = 5`；遇到 `false` 调用必须形成顺序边界。
- ACN 原生工具的无效 input、未知工具和分类器异常都不会被并发执行；MCP arguments 的具体 schema错误按上述 server-side 校验例外处理。
- 一个并发调用失败不会取消同批其他调用；turn 取消会收束未完成调用并闭合 TUI 状态。
- 在完整 tool-use response（包括 fallback 成功）接纳后、首个调用派发前取消或 steer：每个未派发调用只产生带正确原因的 `ToolCallSkipped`，不产生 Started、执行、tool result 或 canonical tool 回环；journal 与恢复上下文将其识别为 skipped，而非旧式 pending。
- 并发批次取消时，已经实际派发的调用各自以 completed/interrupted 收束；尚未获得任务位的调用一律skipped，且不会在 TUI 留下 Calling 状态。
- 上游收到的 tool result 严格保持原始 call 顺序，即使本地完成顺序不同。
- provider streaming 尚未完成时没有任何工具开始执行。
- 已显示 partial 的 streaming provider call 发生 fallback 时，失败流和每次失败/未通过校验的fallback 都不执行工具；只有最终成功的完整 fallback 响应可进入普通的分批调度。tool-only fallback 必须先清空 partial，再发出首个工具开始事件。
- 当前工具矩阵中每一个条件至少有一条分类单元测试；MCP 还要覆盖 `readOnlyHint` 为 true、false、缺失和 malformed 四种情况。
- `code_run` 覆盖 type、AST 组合、白名单命令/flag、允许重定向和拒绝结构的正反分类测试；未通过专用分类器的脚本必须保持 `false`，不能被泛化规则误放行。
