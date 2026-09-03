# PRD：Inbox 失败降级与 Session 启动恢复

> 状态：已完成（2026-09-03；`/new` 采用 finalize-first，已完成全量验证、TUI/真实 LLM smoke 与独立复审）。

## 背景与问题

Agent 在 Fresh 启动、会话内 `/new`、`/resume` 和 `/inbox` 中都会执行 inbox 流程。
当前不同失败阶段混用了 `SessionRuntimeStatus::Error`：Fresh 或 `/new` 启动期间一旦在
inbox、system prompt 或 session runtime 创建阶段返回硬错误，TUI 可能处于
`session=None`、`start_handle=None` 的 Error 状态。输入框仍允许提交，普通输入以及
`/new`、`/resume`、`/exit` 又会进入无人消费的队列，用户只能清空输入后通过 Ctrl+C
退出。

Inbox 的远端可用性、本地持久化、LLM 内化和本地应用具有不同可靠性边界。本需求统一
这些失败的用户体验：Inbox 失败允许 session 继续启动；真正的 session 启动失败进入
受限恢复模式，避免继续积累不可派发输入。

## 目标

- 明确 inbox 各阶段失败后是 warning、error 提示，还是阻止 session 创建。
- Inbox 失败不阻止 Fresh、`/new` 或 `/resume` 获得可对话的 Open session。
- 本地失败允许诚实暴露“可能已有部分副作用”，不承诺事务回滚。
- LLM 内化失败保留 pending 消息，允许用户在 session 内通过 `/inbox` 重试。
- 真正无法创建 session 时，只开放 `/new`、`/resume`、`/help`、`/exit` 四个恢复命令。
- 启动失败后的普通输入和其他命令不进入 queued input。
- 保持 active session 下既有 Error 输入与恢复语义。

## 非目标

- 不新增 `/retry` 命令；启动重试复用 `/new`。
- 不新增 `StartupFailed` runtime 状态或持久化 session 状态。
- 不新增 `Inbox degraded` 状态、badge 或常驻标记。
- 不为 inbox 本地处理增加跨文件事务或无副作用保证。
- 不在本需求中补齐 PolicyUpdate 与所有本地应用步骤的 exactly-once 保证。
- 不改变 Maintainer outbox、receipt ACK、Router scope overview 或远端上传协议。
- 不因会话内 `/inbox` 成功而重建当前 session 已冻结的 system prompt。
- 不自动循环重试 session 启动。

## 失败分类与已拍板处理

以下分类同时覆盖 Fresh 启动、`/new`、`/resume` 与会话内 `/inbox`；具体触发方式没有
active session 时，只有第 7 类可以阻止 session 创建。

| 编号 | 失败阶段 | 已拍板处理 |
|---|---|---|
| 1 | Maintainer inbox pull 或 Router scope overview 失败 | warning；继续处理已有本地 pending，并继续启动或使用 session |
| 2 | 本地持久收件完成后的 receipt ACK 失败 | warning；继续本地内化，并继续启动或使用 session |
| 3 | Inbox 本地持久化、读取、租约领取或释放失败 | 显示 error；允许已经发生部分持久化或 ACK；继续创建或使用 session，最终状态为 Open |
| 4 | LLM provider、输出解析、schema 或业务校验失败 | warning；失败 batch 在进入本地应用前不写 claim、trace 或 effect plan；继续创建或使用 session，最终状态为 Open |
| 5 | 内化结果写 claim、trace、effect journal、dispute ledger、本地上传队列或 done ACK 失败 | 显示 error；明确可能已有部分本地副作用；继续创建或使用 session，最终状态为 Open |
| 6 | 已进入本地上传队列后的 Maintainer 远端上传失败 | warning；保留既有重试与队列语义，继续启动或使用 session |
| 7 | Inbox 之后的 system prompt 或 session runtime 创建失败 | session 不可用；进入无 active session 的受限 Error 恢复模式 |

### 第 3、5 类的副作用契约

- 第 3 类可能已经持久化并 receipt ACK 同批前缀消息，也可能已经把部分 pending 文件移动
  到 processing。
- 第 5 类可能已经写入部分 claim、trace、effect journal、dispute ledger 或本地上传队列，
  也可能尚未完成 inbox done ACK。
- UI 必须显示具体原始错误，并明确提示可能已经发生部分本地变更。
- 该错误只作为 transcript 中的错误信息展示；session 创建成功后状态为 Open。
- 失败消息不得伪装成全部处理完成。现有 pending、processing lease、effect journal 与
  done ACK 恢复边界继续负责后续重试。
- `/inbox` 可以再次触发处理，但本需求不承诺普通 PolicyUpdate 重试无重复或无差异副作用。

### 第 4 类的无本地应用副作用边界

- “LLM 内化失败”只指 provider 调用、输出解析、schema 校验和业务校验在 prepared 结果
  产生前失败。
- 失败调用可能产生 token 消耗、provider 侧请求记录和本地日志。
- 同一次 drain 中更早的 batch 可能已经成功落地；该事实不归入失败 batch 的副作用。
- 失败 batch 保持可重试，不写入本地 done ACK。
- 用户可见提示固定为：

  ```text
  Warning: Inbox internalization failed. This session started without applying some pending updates. Run /inbox to retry.
  ```

- 提示中不增加“后续 session 启动时也会重试”的说明。

## Inbox 失败后的 Session 状态

- 第 1 至 6 类均不阻止 session 创建。
- Fresh 或 `/new` 在第 1 至 6 类失败后继续执行 system prompt 与 session runtime 创建。
- session 创建成功后直接进入 Open；不保留额外 degraded 状态或标记。
- `/resume` 的 inbox 失败继续沿用“展示提示后恢复目标 session 为 Open”的语义。
- 会话内 `/inbox` 失败后回到 Open，普通对话和再次 `/inbox` 均可继续执行。
- 单人模式的 `/inbox` 只处理已有本地 pending，不访问 Maintainer 或 Router；因此 Fresh 或
  Resume 留下的可重试消息仍可在当前 session 手动重试。
- system prompt 使用 inbox 尝试结束时已经持久化的本地 claims 快照；失败 batch 的未落地
  内容在当前 session 中可以不生效。

## 第 7 类：无 Active Session 的 Error 恢复模式

### 进入条件

TUI 同时满足以下条件时进入受限恢复模式：

```text
status == Error
session == None
start_handle == None
```

不新增新的 runtime 状态。active session 存在时的普通 Error 继续沿用现有输入、命令与
重试语义。

### 错误展示

```text
  Error Session startup failed:
  <具体错误>

  No active session.
  Use /new to try again, /resume to open an existing session,
  or /exit to quit.
```

底部提示使用：

```text
No active session · /new · /resume · /help · /exit
```

错误链应保留真实阶段上下文，例如读取 Memory、读取 `ACN.md`、渲染 system prompt、创建
session 元数据或取得 runtime lease 失败；固定文案不把所有第 7 类错误写成 prompt 失败。
`Error Session startup failed:` 下面的具体错误和恢复说明与 `Error` 标签的起始列对齐。
进入恢复模式时清除 startup activity；Attention 框不再显示已经结束的
`preparing session prompt...`，只保留 `local claims`。执行 `/help` 后该框也不增加空行。

### 支持的输入

| 输入 | 行为 |
|---|---|
| `/new` | 重新执行 Fresh session 启动；不增加 `/retry` 别名 |
| `/resume` | 直接打开已有 session picker |
| `/help` | 展示既有帮助 |
| `/exit` | 无 session 时直接退出 TUI，不执行 Finalize |
| 任何其他普通输入或命令 | 不进入 queued input；提示当前状态只支持上述四个命令 |

其他输入的固定提示为：

```text
No active session. Only /new, /resume, /help, and /exit are available.
```

### 输入队列

- Initializing 期间仍允许按现有语义暂存输入。
- 启动失败时继续把初始化期间尚未派发的 queued input 恢复到 composer，避免静默丢失。
- 进入受限 Error 恢复模式后，新提交的非白名单输入不得再次入队。
- 输入在提交时记录是否处于受限恢复模式；即使 `@path` 或按序输入稍后才完成异步回灌，
  也继续按提交时的四命令白名单处理，不因其间 Resume 成功而改成普通输入或 shell 执行。
- `/new` 会关闭已加载但尚未选择的 Resume picker；过期 picker 选择不能与 Fresh startup
  并发。恢复模式中提交的 `/exit` 即使稍后才回灌，也保持无 Finalize 直接退出语义。
- 恢复模式中提交的 `/new` 若直到 Resume 已安装 active session 后才回灌，按当前 active
  session 执行正常的 finalize-first handoff；只有派发时仍无 active session 才直接重试
  Fresh startup。
- `/new` 成功启动后，只派发本次重新初始化期间新进入的合法 queued input；失败前已经恢复
  到 composer 的草稿不自动提交。

## 与既有 `/new` PRD 的关系

`PRD_in_session_new_resume.md` 的早期 D5、D6 曾要求会话内 `/new` 先准备新 session；该文
后续 ND-1 已在冲突处正式覆盖早期决策，改为先完成旧 session handoff，再启动新 session。
本需求继续采用 ND-1 的 finalize-first 顺序。

因此，会话内 `/new` 的旧 session handoff 成功后才执行目标的
`inbox → prompt → create session/runtime lease → Open`。第 7 类失败发生时没有 active
session，进入本文定义的四命令受限 Error 恢复模式。本文覆盖旧 PRD ND-5、ND-7 中
“Resume inbox 失败降级而 New startup inbox 失败保持原行为”的范围限制；Finalize、
Supervisor、通知、输入锁和 interaction generation 的其他语义保持不变。

## 验收标准

- Fresh、`/new`、`/resume` 的第 1 至 6 类失败都不会因 inbox 错误阻止获得 Open session。
- 第 3、5 类错误可见，并明确可能有部分本地副作用；session 状态仍为 Open。
- 第 4 类使用固定 warning，失败 batch 不产生本地应用副作用且保持可重试。
- 会话内 `/inbox` 失败后状态恢复为 Open。
- 单人模式下 `/inbox` 能处理本地 pending，团队连接状态保持 Unknown。
- 第 7 类错误展示真实错误链和固定恢复提示。
- 第 7 类错误正文与 `Error` 标签起始列对齐，Attention 框不显示过期 startup activity，`/help` 后不增加空行。
- 无 active session 的 Error 下，只有 `/new`、`/resume`、`/help`、`/exit` 可执行。
- 其他输入不进入 queued input，并显示固定不支持提示。
- `/new` 只启动一个 startup worker；重复失败仍回到相同恢复模式。
- `/resume` 能打开 picker；`/exit` 能直接结束 TUI且不启动 Finalize。
- active session 下的普通 Error 仍允许正常输入，不受受限恢复路由影响。
- 初始化期间的 queued input 在失败后恢复到 composer。
- 最终实现按已拍板的 `/new` 启动顺序补充对应切换回归测试。

## 实施与验证记录

实现结果：

- Inbox runner 使用结构化 report 同时返回 warning、内化失败和本地失败；已完成本地处理并
  写入 done ACK 的消息才计入 processed total。
- Fresh、`/new`、`/resume` 和会话内 `/inbox` 都消费同一套失败分类；第 1 至 6 类展示对应
  notice 后恢复 Open。单人模式手动 `/inbox` 只处理本地 pending，团队连接状态保持 Unknown。
- 第 4 类在 prepared 结果产生前统一标记为 internalization failure，失败 batch 释放 lease、
  保留 pending，且不写 claim、trace、effect plan 或 done ACK。
- 第 3、5 类保留完整错误链，并统一提示可能已有部分本地变更；已有 pending、effect journal、
  上传队列和 done ACK 恢复边界继续承担后续重试。
- 第 7 类使用无 active session 的 Error 恢复界面、固定 footer 和四命令白名单。恢复上下文在
  提交时进入 ordered input，能够穿过 `@path` 异步解析；picker、startup/resume worker 与
  过期选择均有互斥或失效边界。
- 启动失败通过统一状态事件清理 startup activity；错误详情与恢复说明和 `Error` 标签起始列对齐，Attention
  框只保留 `local claims`，执行 `/help` 不会引入额外空行。
- 恢复态 `/exit` 使用同步 exit fence；同批后续 ready input、普通 submission 和 queued
  input 均不能再启动 turn 或 shell。延迟 `/new` 在派发时重新检查 active session，确保
  Resume 已成功时仍走 finalize-first。

验证结果：

- 版本一致性、`cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo check` 全部通过。
- `cargo test` 全部通过：2694 个 library tests、57 个 `acn` binary tests，以及 Maintainer、
  Router、session cleanup、session storage 和 doc tests，0 失败。
- 标准 tmux TUI smoke 通过，`target/tui-smoke/stderr.log` 为 0 字节。
- 启动失败聚焦 tmux 复验通过：错误详情和恢复说明与 `Error` 标签起始列对齐，Attention 框只显示
  `local claims`，执行 `/help` 后框内仍为一行，应用 stderr 为 0 字节。
- 真实 LLM TUI smoke 通过：覆盖真实 seed 对话、启动期草稿恢复、不支持输入拒绝、`/new`
  重试、`/resume` picker、真实 Resume 续聊、`/exit` 无 Finalize，以及 `/exit` 后续 shell
  不产生 marker；三份应用 stderr 总计 0 字节。

复审结果：

- 多轮独立只读复审发现并修复了恢复态输入被 Resume 等待队列绕过、提交/消费时状态竞争、
  picker 与 `/new` 竞争、solo `/inbox` 被错误拒绝、`/exit` 后续输入副作用，以及延迟
  `/new` 绕过 finalize-first 等现实 P1。
- 每轮 P1 修复后均重新执行聚焦测试、完整 Rust 验证和 TUI smoke。
- 最终独立只读复审结果为 0 个 P0/P1，并确认本文全部验收标准已由实现或回归覆盖。
