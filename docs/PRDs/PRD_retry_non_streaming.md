# 流式响应失败后回退非流式重试

> 状态：已实现。本文保留 provider request 级 fallback、journal 与 TUI 语义。

> 后续变更：本文实施时记录的 “Closed-only resume” 边界已由 `PRD_interrupted_session_resume.md` 取代。未占用的 crash-open `Open` session 现在可以恢复；本文拍板的“不自动续跑 fallback、不补工具结果、只恢复 journal 现场”语义保持不变。

## 1. 背景与目标

ACN 的交互式 session 默认通过流式请求展示 assistant 文本。当前 provider 在尚未产生可见输出时可以按既有策略重试，但一旦已经向 TUI 发出 assistant delta，流式连接中途失败就会直接令整个 turn 失败。

本需求只解决“已经显示 assistant partial 后，当前流式模型调用失败”的恢复问题：保留 partial 作为等待反馈，随后使用同一份 provider 请求做非流式重试；成功后原子替换 partial，失败后沿用 turn 的现有失败语义。

## 2. 改造前现状

### 2.1 Provider 与工具边界

- `AgentTurnLoop` 每次 provider call 都以 `stream = true` 发起。
- provider adapter 负责 HTTP、SSE/JSON 解析和 provider 协议转换；`AgentTurnLoop` 只有在得到完整 `ProviderResponse` 后才校验 tool use 并执行工具。
- 已经显示文本但未形成完整 `ProviderResponse` 时，不会执行该次响应中的工具。
- 一个外层 turn 可能包含多个 provider call。较早 provider call 已完整返回并执行的工具属于既成事实，后续 provider call 失败不能回滚或重复执行它们。

### 2.2 重试

- Anthropic 与 OpenAI-compatible adapter 都支持流式和非流式请求。
- 当前流式 retry 只允许发生在尚未产生会阻止安全重放的事件时；assistant delta 已发出后不会重试。
- `[agent.llm].timeout_secs = 300` 表示单次 LLM HTTP 请求最长等待 300 秒。
- `[agent.llm].retry_count` 是既有 provider 内部 retry 配置。本需求新增的 5 次非流式 fallback 不由该配置控制。

### 2.3 TUI

- assistant delta 追加到当前 active assistant cell。
- 完整 assistant 文本到达时，`complete_assistant_message` 会把 active assistant cell 的内容替换为完整文本。
- turn 最终失败时，TUI 保留已经显示的 partial，并追加现有 `turn failed: ...` 错误。

### 2.4 Session 存储与 resume

- `messages.jsonl` 是 canonical transcript。只有整个 turn 成功提交后才写入；失败 turn 不进入 canonical。
- `turn_events.jsonl` 是运行期 journal，保存用户输入、assistant delta/completed、工具事件和 turn 最终状态。
- 当前 resume 会重放失败 turn 的用户输入、assistant partial、工具事实和 `turn failed` 状态；下一条用户输入通过 `interrupted_turn_context` 获得这些恢复信息。
- 当前 journal 不保存 fallback 尝试状态、尝试次数和最后错误，因此无法区分“重试耗尽”和“进程在重试中退出”。
- 当前正常 TUI resume 只接受 `Closed` session；进程硬退出时 session 保持 `Open`，不会进入 resume picker，也不能用 `--resume <id>` 直接打开。

## 3. 已拍板产品语义

### 3.1 触发范围与请求边界

1. 每个 provider call 首次仍使用流式请求。
2. 只有该流式请求已经向 TUI 发出非空 assistant 文本、随后又失败时，才进入本需求定义的非流式 fallback。
3. fallback 重放的是当前 provider call 的稳定请求快照，不重跑整个外层 turn，不重复执行此前工具。
4. fallback 期间仍不执行工具；只有某次非流式请求完整成功并通过现有响应校验后，才进入既有工具执行路径。
5. 用户 cancel/steer 的安全边界语义不变；用户中断不能被当作 provider 失败进行 fallback。

### 3.2 尝试次数、超时与退避

1. 一条 fallback 链由 1 次已经失败的流式请求和最多 5 次非流式请求组成，即最多 6 次模式级请求尝试。
2. TUI 中非流式次数按 `1/5` 到 `5/5` 展示，原流式请求不计入分母。
3. 每次请求沿用当前 300 秒单请求 timeout；不增加 fallback 总时长配置。因此 6 次请求的等待预算最多为 30 分钟，另加短暂退避时间。
4. 5 次非流式 fallback 是固定产品语义，不复用 `[agent.llm].retry_count`，也不新增用户可配置项。
5. fallback 请求必须禁用 provider adapter 自身的嵌套 retry，保证一次 `1/5` 只代表一次 provider-call attempt。协议既有的 `max_tokens` continuation 不计作 retry，但必须包含在该 attempt 的同一个 300 秒逻辑 deadline 内。
6. 相邻 fallback 尝试之间使用源码内固定的短指数退避；退避参数不写入 `config.toml`，不开放用户配置。

### 3.3 TUI 运行时展示

1. 流式请求失败后，不清除、不置灰、不追加第二份 partial；冻结当前 active assistant cell。
2. 等待第 N 次非流式请求（包括该次请求前的退避）时，底部 activity 显示：

   ```text
   Falling Back to non-streaming · Retrying N/5...
   ```

3. 非流式请求不向 TUI 增量展示任何内容。
4. 某次非流式请求完整成功后，用完整 assistant 文本原子替换原 partial。完整响应没有文本、只有 tool call 时，也必须清空原 partial。
5. 替换完成后 activity 恢复为 `thinking...`，再由既有状态机进入 tool、commit 或下一次 provider call。
6. 5 次全部失败后，停止 fallback，保留原 partial，并沿用现有 live TUI 失败展示：清除 activity，追加 `turn failed: ...`。

### 3.4 Canonical 与 journal

1. 失败流式 partial 和失败 fallback 响应都不写入 `messages.jsonl`。
2. 非流式成功时，只有成功得到的完整 assistant message 有资格随整个 turn 一起进入 canonical；原 partial 永不进入 canonical。
3. `turn_events.jsonl` 记录每次 fallback attempt 的开始、失败和成功。开始事件包含本次序号、总次数及触发它的上一错误；失败事件包含该次错误；成功事件包含用于替换 partial 的完整 assistant 文本。开始、失败、成功事件均在相应 TUI 事件前通过 immediate journal 写入确认；开始事件写入前必须先 flush 已显示的 partial。
4. journal replay 遇到 fallback 成功事件时，必须用成功文本替换最近的未完成 assistant segment，而不是新增第二段 assistant 内容。
5. 一个外层 turn 可有多条 fallback 链；journal 按事件顺序分别投影，不能让后一次 provider call 覆盖前一次已完成的 assistant/tool timeline。

### 3.5 Resume

Resume 只恢复历史现场，不自动重新发起 LLM 请求。本文实施时通过已经按既有流程成为 `Closed` 的 session 验收；后续异常退出恢复能力也允许未占用的 crash-open `Open` session 进入同一套静态恢复语义。

#### 非流式 5 次全部失败

TUI 静态恢复用户输入、原 partial、已有工具事实，并显示：

```text
turn failed after non-streaming retries (5/5): <最后一次错误>
```

- 不恢复 `Falling Back...` activity；composer 处于可输入状态。
- `messages.jsonl` 保持未提交状态。
- 下一条用户输入仍通过 `interrupted_turn_context` 获得原始请求、partial、fallback 摘要和此前已经完成的工具事实。

#### 进程在 fallback 请求或退避中退出

若 journal 有 attempt started、但没有对应成功/最终 `TurnFinished`，journal replay / recovery projection 静态显示：

```text
turn interrupted during non-streaming fallback (attempt N/5)
```

若最后落盘的是 attempt failed、但还未开始下一次尝试，则显示该次失败已发生，并仍将整个 turn 视为中断现场，而不是“5 次已经耗尽”。恢复上下文将该 turn 标记为 `interrupted`；不自动续跑剩余次数。

进程硬退出会遗留 `Open` session。后续 `runtime.lock` 恢复入口允许它在原进程不再占用时通过 resume picker 或 `--resume <id>` 进入；仍只静态恢复以上中断投影，不自动续跑剩余 fallback 次数。

#### Fallback 已成功、但 turn 后续失败

重放成功替换后的 assistant timeline 和后续工具事实。若失败发生在后续 provider call 或其他阶段，不把较早成功的 fallback 错报为“fallback retries exhausted”。

## 4. 不在范围内

- 不改成 streaming 中途执行工具。
- 不把失败 partial 写入 canonical transcript。
- 不在 resume 时自动继续网络请求。
- 不增加 fallback 次数、总时长或退避配置。
- 不清除最终失败 turn 的 partial。
- 不重试用户 cancel/steer。
- 不为旧版 `turn_events.jsonl` 产物增加迁移或兼容分支；旧文件没有新事件时继续走既有泛化恢复语义即可。
- 本 provider fallback 需求本身不实现 session owner 判定；后续由独立的 `runtime.lock` 恢复能力承接 crash-open `Open` session。

## 5. 分阶段实现

### 阶段 A：Provider 调用编排

1. 在 provider-neutral `AgentTurnLoop` 的单次 `call_provider` 边界跟踪是否已经发出非空 assistant delta。
2. 流式中途失败后冻结原请求快照，依次发起最多 5 次 `stream = false` 请求。
3. fallback 请求关闭 adapter 内嵌 retry；增加固定短指数退避，并让 cancel token 同时中断退避和请求等待。
4. fallback 期间屏蔽 provider 的 assistant delta/completed 展示事件，只透传 context usage；成功后发出一次专用替换事件。

### 阶段 B：Journal 与恢复投影

1. 增加 fallback attempt started/failed/succeeded journal 事件。
2. replay 时以 fallback succeeded 替换最近未完成 assistant segment，并投影最新 fallback 链状态。
3. 将 fallback 摘要加入 `interrupted_turn_context`。
4. 为 recovery projection 提供“耗尽失败”和“运行中断”两类稳定状态文案；Closed session 的正常 resume 使用前者，Open session 的 takeover 不在范围内。

### 阶段 C：TUI

1. started 事件更新 activity，保留 active assistant。
2. succeeded 事件原子替换/清空 active assistant，并恢复 `thinking...`。
3. failed attempt 不产生额外 transcript cell；最终失败继续走现有 `TurnFailed`。
4. resume 不创建 live activity，只渲染静态状态行。

### 阶段 D：验证

1. provider-neutral 单元测试：stream partial 后非流式成功、前四次失败第五次成功、5 次耗尽、cancel 中断、请求模式和次数。
2. journal/replay 单元测试：partial 被成功文本替换、空文本清除 partial、耗尽状态、进程中断状态、多 provider call timeline。
3. TUI state 单元测试：fallback activity、成功替换、最终失败保留 partial、resume 文案。
4. 真实终端 smoke test：在 tmux 中运行 ACN TUI，通过可控代理让真实 LLM 的第一次流式响应在产生 partial 后断开，再验证非流式请求、activity 切换、partial 替换、最终提交和 stderr。
5. 完整执行 `cargo clippy -- -D warnings && cargo test && cargo check`，再运行项目默认 TUI smoke test。

## 6. 验收标准

- 失败流式 partial 在等待非流式响应时始终可见且只有一份。
- activity 精确显示 `Retrying N/5`，成功后回到 `thinking...`。
- 一条 fallback 链最多执行 5 次非流式 provider-call attempt，且每次关闭 adapter 嵌套 retry、整体受单次 300 秒逻辑 deadline 约束。
- 非流式成功文本原位替换 partial；tool-only 响应清除 partial 后才执行工具。
- 5 次失败后 live TUI 保留 partial 并按现有逻辑报错。
- 失败 turn 不进入 `messages.jsonl`；成功 turn 只提交非流式成功结果。
- recovery projection 能区分重试耗尽与进程中断，不自动请求，并保留 recovery context；正常 TUI resume 仍仅适用于 Closed session。
- 多 provider call / 已执行工具不被重复执行或错误覆盖。
- 针对性测试、真实 TUI LLM smoke test、clippy、全量测试和 check 全部通过。

## 7. 完成状态

阶段 A–D 已完成，实现与第 3 节的 provider fallback 产品语义一致。单元测试、集成测试、真实 LLM TUI smoke test以及 `cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check` 均已通过。进程硬退出时的 fallback 中断状态可由 journal/recovery projection 正确表达；后续异常退出恢复入口使未占用的 Open session 也能进入该投影，但仍不会自动重试。
