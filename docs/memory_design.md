# ACN Memory 设计

ACN 的 Memory 机制参考了 [Hermes Agent](https://github.com/nousresearch/hermes-agent) 的双文件长期记忆思路，并按 ACN 的 session、claim 与安全边界实现。

## 目标

Memory 为 Agent 提供跨 session 的私有长期上下文：

- 保存稳定、可复用的工作经验
- 保存用户偏好与长期资料
- 让模型通过受约束的工具主动整理
- 不与团队 Claim 身份、状态或投递协议耦合

## 双文件

每个 upstream、每个 Agent 独立保存：

```text
<acn_home>/<upstream>/data/agents/<agent_id>/memories/
  MEMORY.md
  USER.md
```

`MEMORY.md` 用于：

- 可复用的问题解决经验
- 环境和项目约定
- 经多次任务验证的工作模式
- 对未来 session 有帮助的技术事实

`USER.md` 用于：

- 用户偏好
- 稳定背景资料
- 用户明确要求长期记住的信息

`USER.md` 与 `MEMORY.md` 均采用独立的私有存储。Memory 条目是普通 Markdown 文本，不保存 Claim ID、来源关系或其他结构化绑定。

## 条目格式

文件以 `§` 作为条目分隔符。每条是普通 Markdown 文本，不引入额外 schema 或 ID：

```text
第一条长期经验

§

第二条长期经验
```

存储会规范化空白、移除空条目并去重。分隔符属于保留字符，Memory 工具输入不能包含它。

## 写入原语

Memory 工具提供三类操作：

- `add`：追加一条新内容；与现有条目完全重复时为成功的 no-op
- `replace`：以唯一 substring 找到目标并替换
- `remove`：以唯一 substring 找到目标并删除

`replace` 和 `remove` 要求 substring 在目标文件中只出现一次。零次或多次匹配都拒绝写入，避免模型误改相似条目。

一次工具调用可以包含多个操作。系统先在内存中按顺序应用全部操作并校验容量，只有整体成功才写回。

## 并发与持久化

Memory 写入遵循：

1. 对 `MEMORY.md` 与 `USER.md` 的锁路径按稳定顺序加文件锁。
2. 获得锁后重新读取当前内容，避免基于过期快照修改。
3. 执行安全扫描、条目操作、去重和容量校验。
4. 写临时文件、同步并原子 rename。

文件 I/O 在 blocking worker 中执行，不阻塞 async runtime。读 snapshot 也使用相同锁边界，保证返回同一时刻的双文件视图。

## 容量与安全

两个文件分别有字符容量上限。工具响应会返回：

- 当前使用量与上限
- 当前条目数量
- 成功后的最新条目
- no-op 或错误说明

超限时拒绝整批修改，并返回当前状态供模型重新整理。

启用 `safety_scan` 时，写入前会拒绝明显的 secret、credential 和危险持久指令。安全扫描是最低保障，不替代 prompt 中的隐私纪律。

## Session Prompt Snapshot

新 session 启动时读取 Memory 与用户资料，将容量 header 和条目内容渲染进 system prompt。这个 snapshot 在 session 生命周期内冻结：

- session 中调用 Memory 工具会立即更新磁盘
- 当前 session 的 system prompt 不随之改变
- 后续新 session 才读取更新后的内容
- resume 继续使用 session 已持久化的 system prompt

这样可以保持 provider prefix 稳定，并使历史 session 可重复恢复。

## Memory 与 Claim

二者职责不同：

- Memory 是单个 Agent 的私有长期上下文
- Claim 是 holder 愿意让团队检索、引用和治理的共享判断

Memory 不会被直接上传给 Router 或 Maintainer。Agent 可以基于私有经验形成 claim，但 claim 必须用独立、自包含的 statement、scope 和 evidence summary 表达，不能泄露 `USER.md` 或让团队反查具体 Memory 条目，团队也并没有这样的能力。

Maintainer Dispute Analysis 与 holder Agent 的结构化 Resolution inbox 内化都不读取 `MEMORY.md`、`USER.md`、session transcript、Trace 或工具上下文。仲裁 inbox 的一次专用调用只接收完整消息、非 deprecated 本地 Claim 与 direct Claim 快照。

Automatic/Manual Analysis、Resolution、observation 与 inbox effect journal 是执行恢复或审计记录，不是模型长期记忆，也不会被注入普通 session。Effect Journal 只保存已经校验的稳定 plan；崩溃恢复复用 plan，不再次调用内化模型。

借用团队 claim 也不会自动写 Memory。只有当 Agent 判断某项经验对未来任务稳定有用时，才通过 Memory 工具沉淀。

## 后台 Memory Review

可选的 background review 在用户 turn 提交后按配置 cadence 运行独立模型调用：

- 使用当前 session transcript 与 Memory snapshot
- 只开放 Memory 工具
- 不开放文件、命令、Router、Web、MCP、Skill 或 subagent
- 不把 review 自身写入 canonical 用户对话
- 失败只记录 warning，不让已完成的用户 turn 失败

Fork session 可以使用独立 cadence。Review 默认行为和间隔由 `[agent.session.memory_review]` 配置。

## Session Search 的关系

Memory 用于保存已经筛选过的长期知识；session search 用于从历史 transcript 回查原始上下文。模型不应把完整会话或一次性细节全部复制进 Memory。需要证据时先搜索 session，再只沉淀真正稳定、可复用的结论。
