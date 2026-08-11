# Prompt Cache 前缀稳定性修复

> 状态：已实现。实现不得静默修改本文已有决策。

本文定义主 agent 与 subagent 在普通对话、工具循环和异步运行时状态变化下的模型可见上下文规则。目标不是追求任意统计口径下的 100% cache hit，而是消除 ACN 自身对已经发送过的 Provider 前缀所做的非必要删除、移动和重算。

本文以 `openai_responses` 的连续请求为主要验收对象，同时要求 `openai_chat` 与 `anthropic` 的 provider-neutral history 遵循相同不变量。除自动压缩外，后一次 Provider 请求必须只在前一次请求的历史尾部增加新内容。

---

## 1. 背景与现状

Provider Prompt Cache 依赖精确前缀复用。正常工具循环和后续用户 turn 可以追加新的 user、assistant、tool use 与 tool result，但已经发送过的模型可见内容不应在下一次请求中消失、移动或改变文本。

当前 canonical transcript 的普通 user / assistant / tool result 基本保持 append-only，但存在三类 runtime-only projection：

1. 主 agent 的日期与时区 `<runtime_context>` 只临时添加到本次 Provider 所见的 user message；落盘的是未注入版本。下一顶层 user turn 重放历史时，上一条 user message 因而变短。
2. 主 agent 的 background process projection 在每次 Provider 请求前删除旧快照、读取当前态并插回 active suffix。快照还包含随墙钟变化的 `elapsed_minutes`。
3. 主 agent 的 delegation summary projection 当前每个顶层 user turn 读取一次、在该 turn 的工具循环内保持不变，但不落 canonical transcript；下一顶层 turn 会用当前态重新投影并移动到新的 active suffix 前。

subagent 使用独立的单次 `run_session_turn` 执行 objective，并可在其中产生多次 Provider tool loop：

- 初始日期与时区 runtime 在正常单次执行期间会一直留在内存 history，当前通常不会在 child 内部请求之间消失；但它没有进入 delegation transcript，不满足恢复与统一持久化不变量，也无法在长时间运行跨午夜时更新。
- child background process projection 与主 agent background 类似，每次 Provider 请求都会移除旧快照、重新读取并附着，而且明确不进入 delegation transcript。
- child 禁止创建下级 subagent，因此不存在 child delegation summary projection。
- parent 运行时 steering 已按递增 `seq` 构造新 message，先写 delegation transcript，再追加到 child Provider history；它不修改旧消息，不属于本期问题。

## 2. 目标

1. 除自动压缩外，主 agent 与 subagent 的每次 Provider 请求都只在既有模型可见历史尾部追加内容。
2. runtime、background 与 delegation 可以在每次 Provider 请求前观察当前态，但只有模型相关的语义状态发生变化时才追加一份新的有界快照。
3. 任何实际发送给 Provider 的 context snapshot 都以完全相同的内容和顺序持久化，下一次请求不得重新生成其历史版本。
4. Provider retry 必须重用已冻结的同一批 context snapshot，不得在 retry 间重新读取时钟、进程或 delegation store。
5. main 与 child 共用 runtime/background 的 append-on-semantic-change 语义；仅 owner scope 和持久化目标不同。
6. child 长时间运行跨本地午夜或本地时区发生变化时，在下一次既有 Provider 请求前追加新的 runtime snapshot。
7. synthetic context 不得被 UI、resume 摘要、memory review、session turn 统计或 compaction 分段误认为真实用户意图。
8. 保持 Responses reasoning replay、Anthropic reasoning replay、媒体历史和普通工具闭合规则不变。

## 3. 非目标

- 不处理不同 subagent 之间的 Prompt Cache 共享，也不调整 subagent system prompt 中动态身份字段的位置。
- 不修改 `normalize_provider_messages` 的 adjacent-user merge；只要求其输入历史稳定，使每次规范化结果自然稳定。
- 不实现 OpenAI Chat reasoning 保存或回传。
- 不改变 model / protocol 切换时的 replay 过滤语义。
- 不处理用户或外部程序直接修改 `system_prompt.md` 的场景。
- 不承诺 cache hit 指标达到 100%；每次新产生的 user、assistant、tool result 与 context tail 首次出现时本来就不是历史缓存内容。
- 不在第一阶段把所有 snapshot 重构成细粒度 event sourcing；先采用完整、有界、append-on-change 的快照。
- 不让 background stdout/stderr 自动持续流入模型上下文；完整输出继续由工具按需读取。
- 不允许 child 创建二级 subagent。

## 4. 核心不变量

### 4.1 请求关系

普通未压缩路径必须满足：

```text
Request_k = StableSystemAndTools + H_k + Delta_k
H_(k+1)  = H_k + Delta_k + Assistant_k + ToolResults_k
```

其中：

- `H_k` 是已经发送并持久化的精确模型历史；
- `Delta_k` 是本次请求前新观察到、尚未投递的 context snapshot；
- 没有语义变化时 `Delta_k` 为空；
- `Delta_k` 一旦进入请求，就成为不可变历史的一部分；
- 旧 snapshot 可以在语义上被新 revision 取代，但不能从 wire history 中删除、移动或改写。

### 4.2 观察、冻结、追加与持久化分离

四个动作必须分开：

1. **观察**：每次 Provider 请求前读取必要的当前态，或先检查 source revision 决定是否需要读取。
2. **比较**：使用稳定、排除非语义字段的 fingerprint 与最后一份已投递 snapshot 比较。
3. **冻结**：有变化时生成本次唯一的精确 snapshot；同一 Provider retry 链复用它。
4. **追加并持久化**：把同一个 snapshot 同时加入 Provider history 与持久化历史，不允许只有 Provider clone 可见。

“每次 Provider 请求观察”不等于“每次 Provider 请求追加”。

### 4.3 唯一允许的历史替换

自动压缩是唯一允许替换 Provider history 的普通运行路径。压缩生成新窗口后，应重置各 projection baseline，并在新窗口中建立当前完整状态 snapshot；压缩前后的 cache miss 属于预期行为。

model / protocol 切换和外部 system prompt 修改遵循各自已有语义，不纳入本 PRD 的 append-only 保证。

## 5. 模型可见 Context Message

### 5.1 独立、可识别的隐藏消息

runtime、background 和 delegation snapshot 使用独立的 provider-neutral context message，不再临时拼进一个只发送、不持久化的 user message。

持久化模型必须能区分至少以下来源：

```text
runtime
background_process
delegation
```

具体 Rust 类型名可在实现阶段结合现有 DTO 决定，但必须满足：

- 旧持久化消息反序列化兼容；新增来源字段应有安全默认值。
- context message 在 Provider adapter 中可以继续映射为 user-role 内容。
- context message 不属于真实用户 turn。
- context message 默认不显示成用户气泡，不参与 last-user、memory review turn window、session recap 的用户轮次选择。
- context message 保留稳定 source、snapshot revision 或等价去重标识；模型可见正文必须采用确定性序列化。
- 新 snapshot 应明确表示它是该 source 的当前 authoritative state，并在语义上取代更早 snapshot。

### 5.2 顺序

- main 新顶层 turn：先追加本次待投递 context snapshot，再追加当前真实用户 message。
- child 启动：先追加初始 runtime snapshot，再追加 objective。
- 工具循环：先保留已经闭合的 assistant/tool result，再在当前尾部追加新观察到的 snapshot，然后发起下一次 Provider 请求。
- parent steering：保持当前 seq 顺序追加；若同一请求前同时出现 steering 与 runtime/background 更新，只要冻结后的顺序确定且持久化一致即可。
- context snapshot 不得再插入、附着或移动到已经发送过的历史 message 内部。

## 6. Runtime Context 已拍板语义

### 6.1 main 与 child 共用规则

- main 与 child 每次 Provider 请求前都观察本地日期和时区。
- 第一次模型请求前追加完整 runtime snapshot。
- fingerprint 只包含当前产品实际需要的 `current_date` 与 `timezone`。
- 日期与时区均未变化时不追加。
- 跨本地午夜或时区变化时，在下一次既有 Provider 请求前追加新的 runtime snapshot。
- 旧 runtime snapshot 保留；新 snapshot 明确成为当前权威值。
- 不因为秒、分钟或普通墙钟流逝产生新 snapshot。
- 若运行期间没有新的 Provider 请求，不为更新时间单独触发 LLM 请求；在下一次原本就要发生的请求前更新即可。

### 6.2 持久化

- main runtime snapshot 最终进入 canonical session history。
- main active turn 在 Provider 请求发出前，先把冻结 snapshot 写入现有 turn journal 或等价的现有 write-ahead 边界；成功 turn 再按精确顺序 materialize 到 `messages.jsonl`。
- child runtime snapshot 在 Provider 请求发出前写入 delegation transcript，并以独立 context source 保存。
- 当前“Provider 看见 runtime + user，但 transcript 只保存 plain user/objective”的不对称必须消失。

## 7. Background Process 已拍板语义

### 7.1 main 与 child 共用 C 语义

main 与 child 都采用 append-on-semantic-change：

```text
每次 Provider 请求前观察 owner-scoped process state
  -> fingerprint 未变：不追加
  -> fingerprint 变化：冻结完整有界 snapshot，追加并持久化
```

不得再执行：

```text
remove old projection -> regenerate current projection -> insert/attach current projection
```

### 7.2 Owner 边界

- main owner：`(session_id, None)`。
- child owner：`(parent_session_id, Some(subagent_id))`。
- child 只能看到、轮询和控制自己的 managed processes；不扩大 parent / sibling 权限。

### 7.3 Semantic fingerprint

fingerprint 与模型可见 snapshot 只使用完成任务所需的稳定状态，至少包括：

- `process_id`；
- `instance_id`；
- lifecycle state；
- `exit_code` 或 signal；
- `final_output_available` 或等价终态输出可读标记。

下列字段不得单独触发新 snapshot：

- `elapsed_minutes`；
- 本次观察时间；
- 单纯 stdout/stderr 增长；
- output delivery cursor 的普通推进；
- 其他只随墙钟变化、没有生命周期语义的字段。

process 从 starting/running/terminating 到 terminal，或者新增/移除 owner-scoped process，属于语义变化。

### 7.4 输出与通知

- `code_run` 初始 tool result 继续记录 process id 与当次可见输出。
- 后续完整输出继续由 `write_stdin` 显式读取并自然形成 tool result。
- background snapshot 不主动内联持续增长的 stdout/stderr。
- 现有 completion delivery receipt、output cursor、owner isolation 和 provider-acknowledged delivery 机制继续复用。
- completion snapshot 一旦进入 Provider history，必须进入相应 main history 或 child transcript；不能在下一请求因 receipt 已提交而消失。

## 8. Main Delegation 已拍板语义

### 8.1 C 语义

main delegation 不再限制为“每个顶层 user turn 只读取一次”。它改为：

```text
每次 Provider 请求前先检查 delegation activity revision
  -> revision 未变：不读完整 store，不追加
  -> revision 变化：读取当前有界 projection，计算 semantic fingerprint
      -> fingerprint 未变：不追加
      -> fingerprint 变化：冻结、追加并持久化一份当前完整 snapshot
```

这不会因 subagent 状态变化额外触发 LLM 请求，只影响下一次原本就要发生的 Provider 请求。

### 8.2 快照与状态来源

- 第一阶段继续使用完整、有界 delegation snapshot，不要求改成逐事件 delta。
- 多个 subagent 在两次 Provider 请求之间发生的变化合并进下一份 snapshot。
- snapshot 相同则跨 Provider call、跨顶层 user turn 都不重复追加。
- 已持久化 snapshot 永不删除、移动或重算。
- `DelegationActivityHub` 只作为避免无变化时重复 I/O 的唤醒/revision 优化；事实来源仍是已持久化 delegation store。
- `list_subagents`、`wait_subagents`、`read_subagent` 继续提供显式完整查询与等待能力。

### 8.3 明确不属于 semantic change 的内容

- `updated_at` 单独变化；
- subagent 内部 `ToolStarted` / `ToolCompleted` 事件；
- event/transcript 日志尾部增长；
- 仅用于排序、TUI 动画或墙钟展示的字段。

普通 `current_step/progress_summary` 不自动触发新 snapshot，见第 11 节 D1-A。

## 9. Child 特殊边界

### 9.1 不存在 child delegation projection

- child tool profile 保持 `delegation = false`、`delegation_progress = true`。
- child 不创建、列出、读取、等待或 steering 二级 subagent。
- child preflight 不增加 delegation summary projection。

### 9.2 Parent steering 保持现状

- 初始 steering 与 objective 一起成为 child 初始输入事实。
- 运行中 steering 继续按递增 `seq` 分批读取。
- 每批 steering 先持久化到 delegation transcript，再追加到 Provider history。
- 已投递 steering 不修改 objective 或任何旧 message。
- 本期不改变 steering batch limit、截断上限和 parent/child 通信方向。

### 9.3 Child progress

- `update_subagent_progress` 仍是 child 的普通 tool use/tool result，并更新 parent 可见 delegation store。
- progress 更新不回头修改 child Provider history。
- main 是否因 progress 更新自动追加新的 delegation snapshot，由第 11 节决定。

## 10. Retry、失败、恢复与压缩

### 10.1 Provider retry

- context source 的观察与 snapshot 冻结发生在一次逻辑 Provider 请求进入 adapter retry 之前。
- streaming retry、non-streaming fallback 或相同逻辑请求的 transport retry 必须复用完全相同的 snapshot 文本、顺序和标识。
- retry 期间发生的新日期、进程或 delegation 变化留到下一次逻辑 Provider 请求观察，不能改变正在重试的请求。

### 10.2 写入与确认顺序

推荐并要求满足以下效果：

1. source 返回一批稳定 snapshot 与 delivery receipt/revision；
2. 在请求发出前，将精确 snapshot 写入现有 main turn journal 或 child delegation transcript；
3. Provider 请求使用同一批对象；
4. Provider 成功后提交相应 delivery receipt；
5. main turn 成功后按原顺序写入 canonical `messages.jsonl`；
6. 失败、cancel 或进程崩溃恢复时，按稳定 snapshot id 去重并复用 journal/transcript 中的原始内容，不能用当前态重建历史版本。

不得为本需求另造一套与 `messages.jsonl`、turn journal、delegation transcript 并行且没有统一顺序的持久化事实源。

### 10.3 Compaction

- compaction 可以替换旧 history，属于预期 cache break。
- compaction summary 不需要逐字保留所有历史 runtime/background/delegation snapshot。
- 新 compact window 必须重新建立当前完整 snapshot 和 baseline，避免 summary 中的旧状态被误认为当前态。
- compaction 后第一次 Provider 请求仍须满足“新窗口内只追加”的不变量。

## 11. 已拍板补充决策

### D1：普通 subagent progress 不自动进入 main projection（选择 A）

`current_step` / `progress_summary` 可能在 subagent 执行期间频繁更新。本期选择只自动投递协作状态与终态，普通 progress 由工具按需读取。

自动触发 delegation snapshot 的变化包括：

- subagent 新增；
- queued / running / completed / failed / abandoned 等 status 变化；
- terminal summary、error、result ref、changed files 变化。

普通 `current_step` / `progress_summary` 不单独触发 snapshot；main 需要进度时调用 `list_subagents` / `read_subagent`，或者等待终态。

选择原因：自动上下文增长最小，符合现有“内部过程克制感知”原则；main 已有 `list_subagents`、`read_subagent` 与 `wait_subagents` 获取明确进度或等待结果，不需要把高频普通进度被动灌入每次模型上下文。

若 `current_step` / `progress_summary` 与 status、terminal summary、error、result ref 或 changed files 在同一次 store 更新中一起变化，后者仍可触发 snapshot；本条只禁止“普通 progress 自身”成为自动触发条件。

### 11.1 决策变更纪律

- 本文第 4～11 节已拍板语义是实现硬约束，不能为迁就局部代码静默修改。
- 实现中若发现新的产品选择，必须先把候选、选择结果、原因和影响追加到本节，再继续依赖该选择的实现。
- 新决策只能补足本文未定义的细节，不能与已有决策冲突，也不能使已有验收标准失效。
- 如果真实 Provider 协议或现有持久化架构证明已有决策不可实现，必须停止相关实现，记录可复现证据并请用户重新拍板，不能自行降级语义。

### D2：同一次请求的多来源快照顺序

实现采用固定顺序：`runtime -> background_process -> delegation`。child 没有 delegation；
运行中 parent steering 保持其现有顺序，先于同一请求前新观察到的 runtime/background
快照追加。

选择原因：runtime 是所有执行器共享的基础环境，background 是 owner-scoped 执行状态，
delegation 是仅 main 拥有的协作聚合状态；从通用到专用的固定顺序最容易跨 main/child
复用并做精确 prefix 断言。steering 已有独立递增 seq 和“先落盘再追加”语义，保留它在
preflight 中的既有先后关系可避免重新解释已经拍板的 parent/child 通信顺序。

### D3：空状态使用显式 authoritative snapshot（选择 B）

候选方案：

- A：background/delegation 为空时不产生首份 snapshot，只在曾经非空后回到空时追加清空快照；
- B：首次建立 baseline 及 compaction 新窗口都追加一份确定性的 empty snapshot，后续空状态不重复追加。

实现选择 B。main 与 child 的 background 都使用稳定的 `Processes: - none` 完整快照；
main delegation 使用稳定的空 `subagents` 完整快照。状态从非空回到空时同样追加该快照，
旧的非空快照仍保留。

选择原因：空状态也是 authoritative state。显式 baseline 让 fingerprint、恢复去重、跨 turn
revision 缓存以及 compaction 后的状态重建使用同一规则，不需要通过“缺少消息”或删除旧消息
来暗示当前为空。代价是每个新 session / compact window 各增加两份很小且此后可缓存的固定文本，
相比避免歧义和保证 append-only 更可控。

### D4：compaction 后固化实际 Provider 窗口（选择 B）

候选方案：

- A：成功 turn 后只把 active summary 提升为 committed summary，后续请求再从 canonical
  transcript、summary 与当时预算重新计算 compact tail；
- B：在现有 session compaction state 中保存最近一次 compaction 后实际发送的
  provider-neutral history，以及它覆盖到的 canonical message cursor；后续请求从这份窗口
  继续追加 canonical tail，直到下一次 compaction 或 model/protocol 切换建立新窗口。

实现选择 B。保存的窗口绑定 replay protocol 与精确 model；identity 不匹配时遵循本文既有的
model/protocol 切换例外，回退 canonical/summary 投影并在新代际第一次成功请求后建立新基线。
窗口最多保留一份并在下一次 compaction 后覆盖，不形成并行 transcript 或无限 event log。

选择原因：active-turn summary、provider-private continuation replay、外置附件引用与按预算保留的
raw tail 共同存在时，仅保存 summary/cursor 不能保证重建结果逐条等于已经发送的窗口；而每个后续
请求重新计算 raw preserve 还会随 canonical tail 增长删除旧前缀。固化实际 provider-neutral 窗口
可以直接表达“compaction 发生一次 cache break，之后只追加”，同时 canonical transcript 仍保留
完整事实供 TUI、搜索、Memory 与再次 compaction 使用。代价是 session metadata 额外保存至多一个
已受 context window 约束的投影副本，换取精确恢复与跨 turn 前缀稳定性。

### D5：Chat max-token continuation 使用同协议 replay（选择 B）

候选方案：

- A：继续只持久化拼接后的 canonical assistant text；
- B：除 canonical text 外，保存 max-token continuation 实际追加的 assistant/user Chat message
  序列，并只在相同 Chat protocol 与精确 model 下原样 replay。

实现选择 B。普通未发生 continuation 的 Chat response 不增加 replay；跨协议或 model 时仍按
canonical assistant 表示回退。本决策只保存 continuation 的 message 顺序，不新增或声称保存
Chat reasoning，因此不改变第 3 节的 reasoning 非目标。

选择原因：方案 A 会把内部第二次请求的 `partial assistant + continuation user` 在下一次请求中
改写为单条 merged assistant，直接破坏 append-only；方案 B 与 Responses/Anthropic 已有的
provider-private replay 边界一致，且可以用“第二次 HTTP 请求是第三次请求前缀”直接验收。

### D6：compacted Provider 窗口使用 request 前 write-ahead（选择 B）

候选方案：

- A：只在整个 turn 成功、canonical messages 提交后保存最后一次 compacted Provider 窗口；
- B：从发生 compaction 或 replay identity 重建开始，在每次逻辑 Provider 请求发出前，把本次
  精确 provider-neutral history、pending turn 与预计 canonical cursor 写入现有 session
  compaction state；成功提交后再把 pending cursor 提升为已确认 cursor。

实现选择 B。pending cursor 等于 turn 开始时的 canonical message count 加上本请求已包含的
active canonical message 数，因此在请求已发送但 turn 尚未提交时可以暂时领先于
`messages.jsonl`。失败、cancel 或进程崩溃后保留该 write-ahead 窗口；下一 turn 先以它作为精确
前缀，再追加恢复输入。若 canonical messages 已写入而 cursor 提升失败，则按 pending cursor
只追加尚未包含的 canonical tail。journal 中的 context snapshot 仍会在下一成功 turn 落回
canonical transcript；已经存在于 write-ahead 窗口的同一快照不会再次追加到 Provider wire。

选择原因：方案 A 无法覆盖“请求已经到达 Provider，但流/fallback 随后失败、用户取消或进程
退出”的现实窗口，也无法覆盖 canonical commit 成功而 compaction metadata 提升失败的情况。
方案 B 复用现有 compaction state，不建立第二套 transcript，并把 D4 的精确窗口不变量扩展到
§10.2 已要求的失败与恢复边界；代价是 compacted turn 的每次 Provider 请求前增加一次原子
metadata 写入。

### D7：Anthropic adapter 不再二次合并相邻同角色消息（选择 B）

候选方案：

- A：provider-neutral history 冻结后，Anthropic adapter 再把相邻 user 或 assistant message
  合并成一条 wire message；
- B：Anthropic adapter 对已经冻结的 provider-neutral message 做一对一映射，不在 adapter
  内再次改写相邻 message；服务端按协议自行处理连续同角色 message。

实现选择 B。全局 `normalize_provider_messages` 的既有 adjacent-user merge 仍保持不变，符合第 3
节非目标；本决策只移除 Anthropic adapter 在冻结边界之后的第二次本地 merge。

选择原因：Anthropic Messages 协议允许连变续的 user 或 assistant turns，并在服务端把它们视作单个
turn。若 adapter 在每次请求时再次合并，新增尾部恰好与冻结前缀末项同角色时会回头改写上一条
wire message，破坏精确前缀；一对一映射既满足协议，也让已经发送的 HTTP body 前缀保持不。

### D8：成功终态响应纳入稳定 Provider 窗口（选择 B）

候选方案：

- A：write-ahead 窗口只保存最后一次实际请求；成功响应仅依赖下一 turn 从 canonical assistant
  message 和 replay metadata 重新投影；
- B：收到无需继续工具循环的成功终态响应后，把“最后一次实际请求 + 本次接受的完整 Provider
  response”固化为新的稳定窗口，并把 cursor 提升到该 turn 的完整 canonical message count。

实现选择 B。request 前仍按 D6 写入 pending 窗口；只有成功且被接受的最终响应才通过现有
compaction state 原子更新为 response-inclusive window。失败、取消和未完成响应不会冒充成功响应。

选择原因：方案 A 会在 Anthropic context-window continuation 等场景中同时重放请求内的 partial /
continuation trigger，又从 canonical merged assistant 重建同一答案，造成重复或重排。方案 B 保存
Provider 实际看见并接受的完整交换，下一 turn 只需追加新 canonical tail；同时 canonical transcript
仍是 UI、Memory 与再次 compaction 的事实源。

### D9：下一 turn 对账 pending Provider cursor（选择 B）

候选方案：

- A：无条件相信 D6 预估的 pending cursor，并从该位置之后追加 canonical tail；
- B：下一 turn 开始前复用现有 turn journal 与 canonical transcript 对 pending turn 做对账，再决定
  保留 expected cursor 还是回退到 turn 开始时的 base cursor。

实现选择 B：

- journal 已 committed，或虽然缺少 committed marker、但现有 canonical 内容能证明该 turn 已经落盘
  时，保留 expected cursor，并校验它没有超过 canonical 长度；
- failed / cancelled / interrupted、未知未提交状态、缺失 turn，或 canonical 不能证明已提交时，回退
  到 base cursor；
- 对账后清除 pending 标记并原子持久化，不建立新的恢复事实源。

选择原因：失败或取消的 turn 不会写 canonical user/assistant，但其后可能先写入 `!command` 等 shell
record。若仍使用领先的 expected cursor，会把这些合法 canonical tail 一并跳过。反过来，crash
发生在 canonical commit 后、pending 提升前时又必须保留 expected cursor，避免重复重放已覆盖内容。
现有 journal + canonical 事实足以区分两类情况。

### D10：重复 active-only compaction 丢弃旧投影窗口后重建（选择 B）

候选方案：

- A：active turn 再次 compaction 时保留上一份 `provider_history`，并在其后继续拼接新的 active
  summary / suffix；
- B：active-only compaction 明确清除上一份投影窗口，再从当前 committed baseline 与本次 active
  projection 建立一个新窗口。

实现选择 B。compaction 本身是第 4.3 节允许的 cache break；清除的是可替换的 compacted 投影副本，
不是 canonical transcript。

选择原因：上一份窗口可能已经包含与当前 active suffix 重叠的真实 user/tool history。方案 A 在同一
长 turn 内连续压缩时会重复该 suffix；方案 B 让每次允许的 compaction replacement 都从单一事实
边界重建，随后仍恢复只追加。

### D11：普通同模型 Chat assistant 不切断 continuation replay 代际（选择 B）

候选方案：

- A：任何没有 Chat replay metadata 的后续 assistant message 都开始新 replay generation，并丢弃
  更早的 max-token continuation replay；
- B：相同 Chat protocol 与精确 model 下，普通未 continuation 的 assistant message 不切断既有
  replay generation；protocol/model 不匹配或显式不同 replay identity 仍开始新代际。

实现选择 B。

选择原因：普通 Chat response 本来就没有 D5 replay metadata。若仅凭“没有 replay”切代，前一轮
真实发送过的 `partial assistant + continuation user` 会在再下一 turn 消失并退回 merged canonical
assistant，破坏跨多个普通 turn 的前缀稳定性。方案 B 保留已发送的 continuation wire 序列，同时不
新增 Chat reasoning 保存语义。

### D12：冻结已发送的规范化前缀，只规范化新增尾部（选择 B）

候选方案：

- A：每次逻辑 Provider 请求都把完整 provider-neutral history 重新交给全量 normalization；
- B：记录最近一次实际发送请求对应的规范化前缀长度；后续请求逐项复用该精确前缀，只对新追加
  suffix 执行既有 normalization。发生允许的 compaction replacement 时重新建立冻结边界。

实现选择 B。request-ready write-ahead 接收的就是本次将发送的同一份规范化 message vector，adapter
内部不再另做一次可能重写历史的全量 normalization。

选择原因：即使 context appender 只追加，全量 normalization 仍可能在旧前缀与新 suffix 的角色边界
重新合并消息，使“持久化的投影 history”与“实际 Provider 请求”不完全相同。冻结精确 wire 前缀后，
既保留既有 normalization 对新内容的规则，也把 D4/D6 的稳定性落实到真实请求对象。

### D13：所有 main Provider 请求都使用同一稳定窗口 WAL（选择 B）

候选方案：

- A：只有已进入 compaction 代际的 session 在请求前保存 `provider_history`；普通未压缩
  session 继续只依赖 canonical transcript 与 turn journal；
- B：每一个 main Provider 请求在发送前都把同一份规范化 provider-neutral history
  写入现有 `compaction.provider_history`；尚无 compaction state 时惰性建立空 summary 的有界窗口。

实现选择 B。该窗口仍只保存一份当前 context-window 投影；成功终态按 D8 提升 cursor，
失败、取消或 crash 按 D9 对账，不新增第二个恢复事实源。

选择原因：未压缩 session 同样会在 tool loop、失败或取消前向 Provider 送出尚未进入
canonical transcript 的 user/context/tool suffix。方案 A 会让恢复后的下一请求退回重建表示，
破坏 D2/D6 对“最后一次实际请求”的不变量。方案 B 使是否曾触发压缩不再决定
恢复正确性，同时复用已拍板的有界窗口。

### D14：adapter 内部 continuation 参与同一请求前 WAL（选择 B）

候选方案：

- A：把 Responses / Chat / Anthropic 的 max-token continuation 继续视为 adapter 私有实现，
  turn loop 只记录最外层请求，fallback 始终重放最外层 history；
- B：adapter 在每个新的 continuation 请求发送前，通过 provider-neutral observer
  上报“上一次已完成 partial response + continuation trigger”的只追加 replay suffix；
  main 将其写入 D13 的 WAL，fallback 从最新已上报请求继续。

实现选择 B。相同 HTTP request 的 transport retry 复用同一 snapshot，不重复推进 WAL；
只有 input/messages 真实追加了 continuation suffix 才写入。observer 写入失败是请求准备失败，
不得被误判为 streaming 故障而绕过 WAL。

选择原因：adapter 内部的第二次及后续 continuation 也是 Provider 真实接收的新请求。
方案 A 在后续请求失败、取消或进程中断时会丢失已发送的 partial/reasoning/trigger，
且 non-streaming fallback 会退回旧前缀。方案 B 把 D5/D6/D12 的边界从“外层 Rust 方法调用”
对齐到“真实 Provider 请求”，并保留三种协议各自的私有 replay 形状。

### D15：Provider 总超时按 WAL / transport 阶段分类（选择 B）

候选方案：

- A：`request_timeout` 继续把 adapter、continuation observer 与 WAL 持久化视为一个不可区分的
  Provider 调用；超时后只按 streaming / non-streaming 分类；
- B：保留覆盖整个调用的总 deadline，但显式跟踪“正在准备 continuation WAL”与“可以发送
  transport”两个阶段；deadline 在 WAL 阶段到期时统一返回 request-preparation failure，只有
  observer 已成功返回后发生的 timeout 才按 transport failure 进入既有 fallback。

实现选择 B。每次外层 streaming / fallback attempt 开始时重置为 transport 阶段；adapter 只有在
准备一份真实扩展的 continuation request 时进入 WAL 阶段，WAL 成功且最新请求基线已更新后再回到
transport 阶段。相同 transport request 的内部 retry 不推进阶段或 WAL。WAL 超时、失败或被总 deadline
取消均不得发送对应的新 HTTP 请求，也不得从旧请求基线 fallback。

选择原因：总 deadline 仍需约束慢磁盘或卡住的 observer，不能让一次调用无限等待；但 WAL 是发送
新请求的前置提交，不是 Provider streaming 故障。方案 A 会在第一份 HTTP response 已接近 deadline
时取消正在落盘的第二份请求快照，并用尚未更新的旧基线 fallback，直接绕过 D14。方案 B 同时保留
有界等待和 write-ahead 的安全含义。

### D16：context-window 恢复保护整条真实 continuation replay（选择 B）

候选方案：

- A：使用 adapter 返回、面向 canonical/UI 合并的完整 assistant message 作为 compaction recovery
  marker；
- B：使用本次实际 Provider history 中 recovery 链的最早精确消息作为 marker：若 adapter 已在内部
  追加 max-token continuation replay，则选择 `latest_request - outer_request` 的第一条消息；否则选择
  本轮随后实际追加的 response suffix message。

实现选择 B。marker 必须与下一次 compaction 输入中的某条 `SessionTurnMessage` 完全相等；从该 marker
到 active tail 的所有安全段整体受保护。canonical transcript 仍保存合并后的完整 assistant，Provider
稳定窗口则继续按 D14 保存内部 partial/reasoning/trigger 与响应 suffix 的精确拆分，两者不互相冒充。

选择原因：adapter 内部先因 max-token 续写、随后又返回 context-window exceeded 时，canonical 完整
assistant replay 并不是实际追加到 Provider history 的那条消息，无法被 compactor 定位；只保护最后
response suffix 又会允许更早的内部 partial/reasoning/trigger 被压掉。方案 B 以真实 wire 链的起点
建立稳定边界，使压缩后的续写请求仍精确保留整条未完成响应且每项只出现一次。

## 12. 实施边界

### 12.1 Provider-neutral appender

projection 逻辑应与 compactor 解耦：

- 普通 context appender 只能观察只读 history，并返回待追加 batch。
- 除 compaction 外，不应持有可任意删除、插入旧位置的 `&mut Vec<SessionTurnMessage>` 权限。
- batch 必须能同时进入 Provider history、main committed history/turn journal 或 child transcript。
- main 与 child runtime/background 共用 snapshot、fingerprint、冻结和 delivery 逻辑，不复制两套不同语义。

### 12.2 稳定序列化

- snapshot 字段顺序、列表排序和文本格式必须确定。
- fingerprint 不使用随机序列化顺序。
- snapshot id/revision 一旦生成，在 retry、resume 与 commit 中保持不变。
- 模型可见正文不包含仅用于内部去重且会无意义变化的随机值；如需唯一 id，可作为稳定 metadata 或一次生成后完整复用。

### 12.3 历史消费者

至少复核以下消费者不会把 context message 当成真实 user turn：

- TUI timeline 与用户气泡；
- resume last-user 展示；
- session search；
- memory review window；
- recap/finalize；
- compaction real-user segmentation；
- canonical user hash 与 turn journal 对齐；
- delegation transcript tail 与 compaction projection。

## 13. 验收标准

### 13.1 Responses 主要验收

使用本地 fake Provider 捕获连续 `openai_responses` 请求，排除明确允许的 compaction/model/protocol/system prompt 变化后验证：

- `instructions` 与 tool definitions 在同一运行配置下保持一致。
- 前一次请求的 Provider `input` 是后一次请求历史的精确前缀。
- main 跨顶层 user turn 时，上一条 user 前的 runtime snapshot 不消失。
- context snapshot 不因重新序列化、重新读取时间或重新排序而变化。
- provider retry 的请求 context 完全一致。

### 13.2 Runtime

- main 首次请求持久化 runtime snapshot，下一顶层 turn 精确重放。
- child objective 前持久化 runtime snapshot，后续 tool loop 精确重放。
- 同一天、同一时区的连续请求不重复追加。
- main 与 child 跨本地午夜时，下一次 Provider 请求只在尾部追加新 runtime snapshot。
- 时区变化同样只追加一次。

### 13.3 Background

- main 与 child 都不再删除、pop、替换或附着重算旧 background snapshot。
- process 状态未变时，连续 Provider 请求不增加 snapshot。
- 仅 elapsed 或普通输出增长不会产生 snapshot。
- process 从 running 进入 terminal 时只追加一份新 snapshot。
- final output 仍可通过 `write_stdin` 读取，delivery cursor 不跳过模型未见内容。
- main 不能因共用实现破坏 child owner isolation，child 不能看到 parent/sibling process。

### 13.4 Delegation 与 steering

- delegation activity revision 未变时不读取完整 projection、不追加消息。
- revision 变化但 semantic fingerprint 未变时不追加。
- 多个 child 状态变化在下一次 Provider 请求中合并为一份 snapshot。
- snapshot 持久化后，下一顶层 turn 不移动或删除它。
- parent runtime steering 保持按 seq 追加，旧 steering 与 objective 不变化。
- child tool list 仍不包含创建或管理二级 subagent 的工具。
- 普通 `current_step/progress_summary` 单独变化时不追加 snapshot；显式 `list/read/wait` 仍能读取最新进度。

### 13.5 其他协议与消费者

- Anthropic tool-result 相邻角色约束继续满足；不得以修改旧 user message 的方式附着新 context。
- OpenAI Chat 请求历史遵循相同 append-only 语义；其 reasoning 缺失不在本期修复。
- context message 不显示为用户输入，不增加真实 user turn 计数。
- resume、memory review、recap、session search 与 compaction 不把 context snapshot 当成用户请求。

## 14. 分阶段实施

### 阶段 0：PRD 与请求基线

- 固化本文决策与 D1-A 选择。
- 为 main Responses、child Responses 捕获当前连续请求，建立会失败的 prefix 稳定性测试。

### 阶段 1：Context message 与持久化骨架

- 增加 provider-neutral context source/origin。
- 打通 main turn journal、successful canonical commit 与 child delegation transcript。
- 修复 UI、真实 user turn、resume、memory/recap/compaction 消费边界。
- 建立冻结 batch 与 retry 复用机制。

### 阶段 2：Runtime

- 移除仅 Provider clone 可见的 runtime 包装。
- main/child 接入首次 snapshot 与日期/时区 append-on-change。
- 覆盖跨午夜、时区变化、retry 与 resume。

### 阶段 3：Background

- main/child 共用 owner-scoped semantic snapshot。
- 删除 remove/reinsert/pop/attach 旧 projection 路径。
- 保留 receipt、cursor、final output 与 owner isolation。

### 阶段 4：Main Delegation

- 接入 activity revision、semantic fingerprint 与 append-on-change。
- 删除每顶层 turn 的 runtime-only projection 移动逻辑。
- 按 D1 实现 progress 行为。

### 阶段 5：Compaction、恢复与协议回归

- 新 compact window 重建 snapshot baseline。
- 验证失败/cancel/restart 后 snapshot 不重算、不重复。
- 回归 Responses、Anthropic、Chat、reasoning replay、媒体和工具循环。

### 阶段 6：完整验证与 review

- 执行格式、Clippy、测试与类型检查。
- 使用真实 LLM 配置在 tmux TUI 中验收主 agent 连续多轮、工具循环、background 状态变化和 subagent 创建/完成后的正常对话；除模型输出本身外，检查 TUI、session history、stderr 与运行日志没有新增错误。fake Provider 测试不能替代这一步。
- 先按仓库 `code-review` 流程对本需求相关模块做针对性本地审查与一次独立只读审查，只把具有现实触发路径的 P0/P1 作为阻塞项。
- 修复针对性 review 的全部 P0/P1 后，重新执行受影响定向测试、完整 Rust verify 和真实 LLM TUI smoke test；任一失败都必须诊断修复，不能跳过。
- 只有针对性 review 修复后的复验全部通过，才能进入全量 diff review。
- 对最终完整 diff 再做一次本地全量 review 与独立只读 review；若发现新的 P0/P1，继续修复并再次执行定向测试、完整 verify、真实 LLM TUI smoke test和全量 review，直到没有未解决 P0/P1 且最终验收通过。

### 阶段 7：实施记录与交付

- 把最终实现落点、测试证据、真实 LLM TUI 场景、两轮 review 结论与修复闭环追加到本文。
- 将本文状态改为“已实现”的前提是第 15 节全部满足。
- 最终向用户逐项解释所有拍板：候选选项、最终选择和选择原因；新增决策必须与第 11.1 节纪律一致。

## 15. 完成定义

只有同时满足以下条件，本文状态才能改为“已实现”：

1. D1-A 与实现期间新增决策均已记录，且没有冲突或削弱既有语义。
2. main/child Responses 的连续请求 prefix 测试通过。
3. runtime、background、delegation 的 source-specific 验收全部通过。
4. main 与 child 持久化、retry、resume、compaction 行为符合本文不变量。
5. Chat 与 Anthropic 定向回归通过，reasoning/media/tool history 无倒退。
6. context message 未污染 UI、真实用户轮次、Memory、search、recap 或 finalize。
7. 完整 Rust verify 与真实 LLM TUI smoke test通过。
8. 针对性 code review 的 P0/P1 已全部修复并完成复验。
9. 修复复验通过后完成全量 diff review；最终没有未解决 P0/P1，且全量 review 后的最终验收通过。
10. PRD 已追加实施与验收记录，最终交付说明逐项包含拍板选项、选择结果和原因。

## 16. 实施与验收记录

### 16.1 实现落点

本需求已按 D1～D16 完成，主要落点如下：

- 增加 provider-neutral `ModelContext` message、`runtime` / `background_process` /
  `delegation` source、稳定 fingerprint 和显式持久化转换。main 同一 batch 进入 turn journal、
  canonical transcript 与 Provider history；child 同一 batch 进入 delegation transcript 与
  Provider history。
- runtime 改为 main/child 共用的 append-on-semantic-change 快照。首次请求建立 baseline；本地日期
  或时区变化时，只在下一次既有 Provider 请求前追加新快照；retry 复用已冻结内容。
- background 改为 owner-scoped、只包含生命周期语义的有界完整快照。elapsed 与普通输出增长不触发
  自动追加，running/terminal 等语义变化才追加；最终输出仍由 `write_stdin` 显式读取，delivery
  cursor 与 main/child owner isolation 保持不变。
- main delegation 使用 activity revision 作为廉价观察门槛，再以 semantic fingerprint 去重；普通
  progress 不触发投影，协作状态和终态变化合并为下一次请求的一份快照。child 继续禁止创建二级
  subagent，parent steering 继续按 seq 先落盘、后追加。
- TUI、resume、session search、Memory review、recap/finalize、真实 user turn 统计、canonical user
  hash、compaction 分段与 child transcript projection 均识别并过滤 synthetic context，不把它当成
  用户意图。
- compaction state 现在保存精确、绑定 protocol/model 的实际 Provider 窗口和 canonical cursor；
  compaction 是允许的 replacement，之后从冻结窗口只追加。重复 active-only compaction 会先清除旧
  投影再重建，不重复 active suffix。
- 每一个 main Provider 请求，包括 adapter 内部 continuation，请求发送前都写入同一份规范化窗口
  WAL；成功接受的终态响应再把 response-inclusive window 提升为稳定窗口。pending turn 会用 journal
  与 canonical transcript 对账，失败、取消、崩溃恢复和 post-commit failure 均有确定边界。
- Responses、Chat 与 Anthropic 都从同一冻结的 provider-neutral 前缀构造请求，只规范化新增 suffix。
  Chat 持久化同模型 continuation replay；Anthropic 对冻结消息一对一映射，不在 adapter 内二次合并；
  reasoning、媒体和工具闭合规则保持原有协议语义。
- continuation observer/WAL 纳入 Provider 总 deadline 的显式准备阶段；WAL 未成功时不发送新请求、
  不从旧 prefix fallback。context-window recovery 使用真实 continuation wire chain 的最早消息作为
  marker，保护整条未完成 replay。

实现末期额外修正了两个不新增产品语义的边界：adapter continuation 只把新增 wire suffix 同步回
raw history，保留 compactor 的 raw `active_start_index`；response-inclusive pending window 记录最后
一次真实 request 的精确 message boundary，未 canonical commit 时丢弃未被接受的 response suffix。
两者分别是 D12/D14 与 D8/D9 的实现一致性修正，不形成 D17。

### 16.2 Review 与修复闭环

针对性本地审查和独立只读审查未发现 P0；发现的现实 P1 均已修复，并在每轮修复后执行受影响定向
测试。修复闭环覆盖：

1. 截断 canonical history 时的 ModelContext recovery 判断；
2. successful active compaction 窗口跨 turn 丢失；
3. Chat max-token continuation replay 缺失；
4. context 变化与同请求 compaction 造成 baseline 重复；
5. failed/cancelled/crashed compacted request 未保存精确窗口；
6. WAL 保存 normalization 前而非实际 wire history；
7. 重复 active-only compaction 重复 active suffix；
8. 普通同模型 Chat assistant 错误切断旧 continuation generation；
9. Anthropic adapter 在冻结边界后二次合并；
10. Anthropic response-inclusive history 重复 continuation replay；
11. 领先的 pending cursor 跳过后来写入的 shell canonical tail；
12. continuation observer/WAL deadline 到期后从旧 prefix fallback；
13. max-token continuation 后再触发 context-window recovery 时未保护完整 replay chain；
14. adapter continuation 后 raw/wire `active_start_index` 漂移；
15. 未 canonical commit 的 response-inclusive pending window 重放未接受 response。

修复复验通过后完成本地全量 diff review，并使用独立只读 reviewer 对完整 tracked/untracked diff、
D1～D16、三类 adapter、WAL/恢复、main/child context、consumer 与序列化兼容做最终门禁。最终结论为：
没有可现实触发、具有实质影响的 P0/P1；实现与 PRD 完全对齐。

### 16.3 自动化验证

最终全量验证在 full-diff review 结束后重新执行并通过：

- 版本一致性：ACN `0.2.2`；
- `cargo fmt --check`；
- `cargo clippy -- -D warnings`；
- `cargo test`：library 2165、`acn` binary 57、maintainer 2、router 2、session cleanup
  integration 1、session storage integration 5，另含 doc tests，全部通过；
- `cargo check`；
- bundled tmux TUI smoke：启动页、`/help`、`/skills`、状态栏与 clean exit 均通过，`stderr.log`
  为 0 字节。

定向/fake Provider 覆盖 main/child Responses 精确前缀、retry、跨午夜/时区、background lifecycle、
delegation revision/fingerprint、Chat continuation、Anthropic mapping、重复 compaction、所有请求 WAL、
失败/取消/崩溃/post-commit 恢复、D15 timeout、D16 replay marker、raw/wire boundary 和未提交 response
rollback。Chat/Anthropic、reasoning/media/tool history 与 consumer filtering 回归全部通过。

### 16.4 真实 LLM TUI 验收

最终使用真实 `openai_responses`、模型 `gpt-5.6-luna` 在 tmux TUI 中完成连续四轮：

1. main 用 `code_run` 启动 background process，返回 `CACHE_BG_STARTED`；
2. 下一轮用 `write_stdin` 等待并读取 terminal output，返回 `CACHE_BG_READ_OK`；
3. main 创建并等待 child；child 自己启动、读取 background process 后完成，main 返回
   `CACHE_SUBAGENT_OK`；
4. 完成 subagent 后继续普通无工具对话，返回 `CACHE_FINAL_OK`。

最终 main session 为 `session_9ae8fd11`，child 为 `subagent_8d524897`。main canonical transcript
持久化 24 条 message，其中 8 条带 Responses replay，8 条含 ModelContext，source 覆盖
`runtime`、`background_process`、`delegation`；稳定 Provider history 同步覆盖到 canonical cursor
24。child transcript 持久化 23 条 entry，其中 5 条 ModelContext，source 覆盖 `runtime` 与
`background_process`。四个标记均在 turn 回到 `open` 后仍可见，TUI `stderr.log` 为 0 字节，session
运行日志无新增 error/panic。

### 16.5 第 15 节完成定义对账

| 条目 | 结果 | 证据 |
| --- | --- | --- |
| 1 | 满足 | D1-A 与 D2～D16 均已记录；末期两个修正只落实既有决策，没有 D17。 |
| 2 | 满足 | main/child Responses 连续请求精确 prefix 定向测试通过。 |
| 3 | 满足 | runtime、background、delegation source-specific 单元/集成/真实 LLM 场景通过。 |
| 4 | 满足 | main/child 持久化、retry、resume、compaction 与失败恢复测试通过。 |
| 5 | 满足 | Chat、Anthropic、reasoning/media/tool history 回归通过。 |
| 6 | 满足 | UI、真实 user turn、Memory、search、recap/finalize consumer 回归通过。 |
| 7 | 满足 | 最终完整 Rust verify、bundled TUI smoke 与真实 LLM TUI smoke 通过。 |
| 8 | 满足 | 针对性 review 无 P0；全部 P1 已修复并复验。 |
| 9 | 满足 | 修复后完成全量本地/独立 review；零未解决 P0/P1；review 后最终验收通过。 |
| 10 | 满足 | 本节已记录实现、测试、真实场景和 review 闭环；最终交付逐项解释 D1～D16。 |
