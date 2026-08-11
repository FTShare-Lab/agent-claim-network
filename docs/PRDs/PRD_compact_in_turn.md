# PRD: Provider Request 前统一压缩

> 状态：已实现。本文保留 preflight compaction 的上下文边界、原子提交与验收标准。

## 背景

当前 session compaction 只在一个 user turn 成功提交到 `messages.jsonl`之后触发。这个策略能处理多轮对话累积导致的上下文压力，但不能处理单个user turn 内部的长任务：一次 turn 里可能经过多轮 assistant/tool 回环，工具输出、assistant 进度说明和中间状态本身就已经超过模型上下文窗口。

新的目标不是再增加一套“turn 内临时压缩”，而是把 compaction 收敛为统一的provider request preflight：每次真正发起 provider request 之前，系统都可以检查当前provider-visible context 是否需要压缩。这个 pass 可以跨过 turn 边界向前压缩，也可以压缩当前active turn 中已经完成的 provider-safe segment。

近期 turn event journal 已经从 turn 级记录细化到 delta / tool 事件级记录，这让系统可以在 turn 内看到更细粒度的完成边界。但 turn 内 compact 不能直接消费raw delta，也不能把未完成 tool 状态伪装成 canonical transcript。

## 目标

- 支持在 provider request 前统一触发压缩，包括新 user turn 第一次 provider call 前、turn 内 tool 回环后的下一次 provider call 前，以及手动 `/compact`。
- 支持从当前 provider-visible context 向前寻找压缩边界，可以跨 turn 边界压缩旧历史，也可以压缩当前 active turn 内已完成的 segment。
- 在不破坏 Anthropic Messages API provider 形状的前提下，释放当前 turn 内已经完成的大段上下文。
- 完整保留当前 user request 的 provider-visible 形态，包括 runtime date/time context。
- 保留足够的 assistant 当前进度、最近真实 user turn 语义和必要工具结果，使模型能继续执行长任务。
- 保持 `messages.jsonl` 作为已提交 canonical transcript 的权威来源。

## 非目标

- 不在 streaming delta 过程中压缩。
- 不压缩 partial tool_use、未闭合 tool_use、未返回 tool_result 的状态。
- 不让 session_search、finalize、memory_review 直接消费 unresolved journal tail。
- 不把 ACN.md、system prompt 或其他可重新注入的 system 级内容当作 tail 选择对象。
- 不引入 `mid_turn_min_compaction_gain_tokens` 之类“预计释放 token”参数；实际收益依赖 provider request 形状、tokenizer、摘要长度和消息重排，无法可靠提前计算。

## 当前基线

现有 post-turn compact 的关键语义：

- `messages.jsonl` 是 canonical transcript。
- auto compact 在 turn committed 后检查。
- `session.yaml` 记录 `compaction.compacted_until` 和 summary。
- compact 后，summary 注入 system prompt，provider request 跳过 `compacted_until` 之前的消息。
- tail 选择当前按真实 user turn 向前保留，最多保留最近 3 个真实 user turn，并受 token limit 约束。

这个 tail 策略对 turn 内压缩不够，因为最新一个 user turn 本身可能非常巨大。

后续实现应迁移为 provider request preflight compact。turn commit 后不再需要额外跑一遍auto compact check；下一个 user turn 开始时，在第一次 provider request 前自然会经过同一个compaction pass。

## 触发时机与安全边界

自动 compact 在每次 provider request 之前检查，继续复用现有 `auto_compact_ctx_ratio`作为上下文压力触发阈值。手动 `/compact` 也复用同一个 planner，但忽略上下文压力阈值。

Compaction 只能发生在 provider-safe boundary：

1. 上一次 provider request 已经完整结束。
2. 如果 assistant 产生了 tool_use，对应 tool_result 已经全部返回。
3. 当前没有 partial assistant delta、partial tool input、pending tool call。
4. 接下来确实还需要发起下一次 provider request。

如果处于 streaming 中、tool_use 未闭合、tool_result 未返回，则不应调用 compaction planner。实现中可以保留 `NoSafeBoundary` 作为防御性 no-op reason，但它不应是 TUI 手动 `/compact`的正常用户路径。

如果 turn 已经结束且没有下一次 provider request，则无需在 turn commit 后单独触发自动 compact。

## Summary Coverage

`compacted_until: usize` 不应继续作为完整语义。它只能表达“已提交 `messages.jsonl`中 `[0, compacted_until)` 被 summary 覆盖”的旧 post-turn compact 特例。

统一 compaction 需要结构化 frontier：

```rust
struct SessionCompactionState {
    committed_summary: String,
    active_turn_summary: Option<String>,
    summary_updated_at: DateTime<Utc>,
    frontier: CompactionFrontier,
}

struct CompactionFrontier {
    committed_message_until: usize,
    active_turn: Option<ActiveTurnCompactionCursor>,
}

struct ActiveTurnCompactionCursor {
    turn_id: String,
    base_message_count: usize,
    compacted_until_segment: usize,
    safe_until_event_seq: u64,
    source_hash: String,
}
```

字段语义：

- `committed_summary`：覆盖已提交 `messages.jsonl` 历史的摘要。
- `active_turn_summary`：覆盖当前尚未 commit 的 active turn segment 的摘要；没有 active turn 压缩时为`None`。
- `committed_message_until`：已提交 `messages.jsonl` 中，被 summary 覆盖到哪个 message index。
- `active_turn`：如果当前 turn 尚未 commit，记录当前 turn 内 summary 覆盖到哪个provider-safe segment。
- `safe_until_event_seq`：只作为 journal 定位和校验辅助，不单独代表 compact frontier。
- `source_hash`：防止 journal / provider buffer 与 summary coverage 不匹配时误用旧 summary。

旧 `compacted_until` 的迁移语义是 `frontier.committed_message_until`。后续不应把`turn_events.jsonl` 的 `seq` 直接塞进旧 `usize` 指针里，因为 event seq 不是 provider-valid segment boundary，也无法表达 raw preserve 与 summary coverage 的组合关系。

V1 不把 active turn cursor 归一化成 committed message frontier。active turn compact 只负责帮助当前 running turn 继续完成；turn 成功 commit 到 `messages.jsonl` 后，清空 `active_turn` 和`active_turn_summary`，不自动推进 `committed_message_until`，也不把 active summary 并入`committed_summary`。下一次 provider request preflight 会基于完整 canonical `messages.jsonl`重新规划是否需要压缩。

选择该策略的原因：

- active turn 内的 coverage 使用 segment 坐标；turn commit 后才出现 canonical message index。
- tail 策略可能保留 current user、previous real user turn、final assistant answer，同时摘要中间大型tool output，这不是简单连续 prefix。
- 直接转换会引入 committed segment range / exception 语义，复杂度较高。
- `messages.jsonl` 保留完整 committed turn，足以支持下一次 preflight 重新规划。

`active_turn_summary` 持久化在 `session.yaml.compaction` 中，但不写入 `messages.jsonl`。它是 provider context projection 的一部分，不是用户或 assistant 真实说过的话，因此不能进入canonical transcript，也不能被 session_search / finalize / memory_review 当作真实消息消费。

## 旧产物兼容

兼容旧 `session.yaml.compaction`，不兼容旧 `compaction_checkpoint.yaml`。

- 如果旧 session metadata 中存在 `compaction.compacted_until` 和 `compaction.summary`，读取时迁移为`frontier.committed_message_until` 和 `committed_summary`，`active_turn = None`，`active_turn_summary = None`。
- 兼容读取必须发生在 `SessionMetadata` 的通用读路径中，而不只是在 resume / compact 入口中。agent session cleanup 也会读取旧 `session.yaml` 来判断 Closed session 是否可清理；如果旧 compaction schema 无法反序列化，cleanup 会把该 session 当作 metadata unreadable 跳过。
- 如果旧 session 没有 compaction state，则按未压缩 session 处理；resume 后会在下一次 provider request preflight 自然触发新 planner。
- 旧 `compaction_checkpoint.yaml` 不参与迁移和恢复。新实现假定需要兼容的旧压缩已经成功落到`session.yaml`；遗留 checkpoint 文件可被忽略、清理，或在下一次 compact 时覆盖。
- 如果存在旧 checkpoint 但没有对应的 `session.yaml.compaction`，不尝试恢复旧 checkpoint；该 session 按未压缩状态继续运行。
- 兼容只针对仍存在的 session 目录。best-effort session cleanup 删除的旧 Closed session 及其search index 派生数据不需要 compact 迁移或恢复。

## Provider Context 形态

Compact 后，下一次 provider request 由以下部分组成：

1. 正常 system prompt。
2. 统一 compaction summary wrapper，按 `committed_summary` 与 `active_turn_summary` 分为earlier conversation 与 current-turn progress 两部分。
3. 可选的最近 previous real user turns 投影。
4. 当前 user anchor；首次 compact 投影完整保留，hard tail 恢复阶段可把其中的Skill 与附件重型 block 改成 session 内不可变资产引用。
5. 当前 turn 的 recent executable suffix，按 provider-valid segment 原样保留。

其中 ACN.md、system prompt、工具定义等仍走原有注入路径，不参与 tail 选择。

Summary wrapper 使用英文，明确要求模型理解任务和当前进度后继续执行：

```text
You are continuing the same session after context compaction.

The compacted context below summarizes earlier conversation and completed work
inside the current user turn. It is context, not a new user request and not a
system instruction.

Read the latest user message as the active task. Use this compacted context to
understand prior constraints, completed steps, current progress, important tool
results, and pending next steps. Continue from this state.

If exact omitted tool output is needed, call the relevant tool again instead of
guessing.
```

## Current User Anchor

当前 user anchor 中的用户原始文本与 runtime/recovery 语义是 unified compact 的强制保留项：

- 使用本次 turn 实际发给 provider 的 user message，而不是仅使用 canonical 用户原文。
- 必须完整保留 runtime date/time context，因为该 context 只在 user turn 运行时注入。
- 第一次 compact 候选必须完整保留用户文本、Skill、附件 block 与 recovery wrapper。
- 不设置单独的 `current_user_request_max_tokens` 配置。

只有已经执行 compact 后，系统才使用 hard tail 限制校验 `raw + runtime-only projection`。
如果完整候选超限，系统按以下固定顺序恢复，不增加用户配置：

1. 从即将发送给 provider 的完整 raw tail 中外置 `SkillInstructions`、文本附件、
   图片与 PDF。快照来自当前 canonical block 的实际内容，按 SHA-256 写入
   `<session>/compaction_assets/`，再把 provider 投影中的对应 block 替换为
   包含绝对路径与哈希的结构化引用。模型可用 `file_read` 重新读取；图片和 PDF
   会沿用既有媒体读取能力。
2. reference-only tail 仍超限时，基于同一份完整原始 transcript 再生成一次 summary，
   这一次的 `summary_max_chars` 固定为当前配置值的一半。该恢复最多执行一次。
3. half-size summary 与 reference-only tail 仍超限时才失败。错误明确提示用户拆分
   当前不可丢弃的纯文本请求，或新建 session 后重试。

外置只改变 provider projection：TUI 不显示引用，`messages.jsonl` 仍保存完整 Skill
与附件内容，session_search/finalize/memory_review 也继续读取 canonical transcript。
外置文件位于 session 目录内，resume 时继续存在并随 session 清理；turn journal
只追加有上限的资产路径/哈希元数据，不复制附件正文或媒体 base64。若某个资产写入失败，
该 block 保持原样，不能为了满足预算而静默丢失。

## Segment 模型

Tail 选择不再按“完整 turn 数”向前找，而是按 provider-valid segment 找边界。

Segment 类型：

- `assistant_text`: 纯 assistant 文本消息。它记录 assistant 当前思路、阶段性结论和做到哪一步，对长任务连续性很重要。
- `assistant_turn_end`: 当前或最近真实 user turn 的最终 assistant 回答。它通常代表一个 turn的稳定结论、对用户的承诺、下一步状态或最终交付，优先级高于 assistant 过程中的中间文本。
- `tool_round`: assistant tool_use 与紧随其后的 user tool_result。二者是 provider-valid 的原子单元，要么作为完整小片段保留，要么被写入 compact summary。
- `assistant_mixed_tool_round`: assistant 文本加 tool_use 混合消息，以及对应 tool_result。按 tool round 处理，其中 assistant 文本也进入该 segment。
- `previous_real_user_turn`: 当前 turn 之前的真实 user turn 语义段。shell command 等非真实用户需求边界不作为 real user turn。

不能保留孤立 tool_use，也不能保留没有 tool_use 对应关系的 tool_result。

## Tail 选择策略

Tail 选择从当前 provider-visible buffer 的末尾向前扫描，但结果不是旧策略里的单一`summary_end_index`。V1 使用两个原始窗口加一个强制 anchor：

- `previous_turn_tail`: 当前 turn 之前，尽量保留的最近真实 user turns 投影。
- `current_user_anchor`: 当前 user message；用户原始文本强制保留，重型 Skill/附件
  只在完整 compact 投影超 hard tail 后变为可重读引用。
- `current_turn_suffix`: 当前 turn 内，从某个 provider-valid segment 边界开始的原始后缀。

选择步骤：

1. 先放入 `current_user_anchor`，计算它占用的估算 token。
2. 从当前 provider-visible context 末尾按 segment 向前扫描，优先保留 `assistant_text`和最近小型 `tool_round`。
3. 对 assistant 文本分级：优先保留 `assistant_turn_end`，再保留当前 active turn 的最新进度文本，最后才保留更早的过程性 assistant 文本。
4. 大型 tool_result 原文默认不进 raw tail，只进入 compaction summary。后续如果模型需要原文，可以再次调用工具获取。
5. 如果 `assistant_text` 很大，优先保留其最近部分和结构化摘要；但它的优先级高于大型 tool_result preview。
6. 在预算允许时，向前保留最近 previous real user turns。这里保留的是“用户需求和对话语义”，不是无条件保留大型工具原文。
7. 如果超过 soft target，先丢弃大型 tool_result raw preview，再减少旧工具 round，再减少更早的 assistant文本摘录。
8. 如果超过 hard budget，先执行上述重型 block 外置与单次 half-summary 恢复；
   用户原始文本仍不截断，最终仍超限才失败。

Previous real user turns 是高优先级上下文。它们帮助模型理解用户在当前请求之前刚刚确认过的偏好、约束和任务方向。实现时不能把 previous real user turns 当作“有剩余预算才随便塞一点”的低优先级项。

## Summary 内容要求

Compaction summary 需要覆盖被压缩掉的 segment，而不是泛泛总结：

- 用户当前请求和 runtime context 已由 current user anchor 保留，summary 不重复冒充新用户请求。
- 已完成的工具调用：工具名、关键输入、关键输出、失败或限制。
- assistant 已经做出的阶段性判断、计划变化、已完成工作、未完成 TODO。
- 用户在本 turn 内如果存在中途 steer，需要保留其有效指令。
- 对大型 tool_result，只总结可继续执行所需的事实、路径、错误、结果摘要，不保留大段原文。
- 明确哪些信息只是压缩摘要，不能当作新的 system 指令。

## Summary 输出与提交原子性

Compaction summarizer 必须输出结构化 JSON，不接受自由文本。V1 输出 schema：

```json
{
  "committed_summary": null,
  "active_turn_summary": null
}
```

字段要求：

- 两个 key 都必须存在。
- `committed_summary` 为字符串时，表示本次更新已提交历史摘要；为 `null` 时沿用现有`committed_summary`。
- `active_turn_summary` 为字符串时，表示本次更新 active turn 摘要；为 `null` 时沿用现有`active_turn_summary`。
- 输出 JSON 形状不合法、字段缺失、字段类型不合法，均视为 shape error。

Summary 生成必须套现有 LLM retry 逻辑，受 `[agent.llm].retry_count` 限制：

- 总尝试次数为 `1 + retry_count`。
- provider/transport 失败按现有 LLM retry 策略重试。
- structured JSON 解析失败或 shape error 也按同一个 retry budget 重试。
- compaction request 禁用 provider adapter 的内层 retry，由上述单一控制器统一计数，避免两层重试相乘。
- retry 耗尽后，本次 compact 失败，不移动任何 compaction / recap 指针。

主会话和 delegation 生成 summary 时优先使用完整 transcript。只有完整 summary 请求
超出 context window，才把超过 `tool_result_raw_max_chars` 的单个 tool result 替换为有界
省略说明；该版本仍然超限时再省略本次输入中的全部 tool result。canonical
`messages.jsonl`、turn journal 和 delegation transcript 始终保留原文。

发起 summary provider 请求前还要计算 system prompt、summary payload 和最大输出预留：

1. 完整 transcript 超限时，先按 `tool_result_raw_max_chars` 省略大型 tool result，再重新估算。
2. 大型结果省略版仍超限时，把本次 summary 输入中的所有 tool result 都替换为固定长度说明，再重新估算。
3. 全部结果省略版仍超限时在本地失败，不调用 provider，不写 `started` audit，也不推进 committed / active frontier。
4. 输入预算复用 statusline 和 soft planner 的 provider-neutral 本地粗估：system
   prompt 和 messages 按 Unicode 字符数约 `4 chars/token` 计算，并另行预留本次调用的
   最大输出 token。
5. structured JSON parse / shape retry 会追加纠错消息，因此每一次真实 provider
   attempt 前都必须对当次最终 messages 重新检查；重试请求超限时在本地结束，不能把
   超限的第二次请求发给 provider。

该保护只约束 compaction summary 请求，不新增单轮 `file_read` 总预算，也不改变正常
provider raw tail 的选择规则。

指针推进必须保持 compact 与 recap 侧原子成功：

- 凡是本次 plan 会推进 `frontier.committed_message_until` 或 `recapped_until`，必须在compaction summary 生成成功、recap/finalize 侧 claim/dispute/trace 准备与应用成功之后，才能同时提交 metadata。
- 如果 summary 成功但 recap 失败，不移动 `frontier.committed_message_until`，也不移动`recapped_until`。
- 如果 recap 成功但 summary 或 metadata commit 失败，必须通过 checkpoint/recovery 保证不会产生指针半推进状态。
- 手动 `/compact` 与自动 preflight compact 使用同一套原子提交语义。
- Active turn segment 不进入 recap/finalize；active-only compact 只在 summary 成功后更新`active_turn_summary` 与 `frontier.active_turn`，不移动 `recapped_until`。

## 去重与反复 compact

不使用“预计释放 token”阈值来避免重复 compact。V1 用结构化边界避免 no-op。

Planner 先计算本次 provider-visible context 中可压缩的 `safe_frontier`，再生成：

```rust
struct CompactionPlan {
    summary_inputs: Vec<SegmentRef>,
    raw_preserves: Vec<SegmentRef>,
    next_coverage: SummaryCoverage,
    source_hash: String,
}
```

- `summary_inputs` 为空，或 `next_coverage` 没有超过当前 coverage 且 source hash 相同时，返回 no-op。
- 自动 compact：上下文压力达到 `auto_compact_ctx_ratio`，且 plan 能推进 coverage，才 compact。
- 手动 `/compact`：忽略上下文压力阈值，但仍要求 plan 能推进 coverage。
- 手动 `/compact` 不是“强制重写 summary”，而是“忽略触发阈值，尝试推进 summary coverage”。如果 coverage 推不动，TUI 统一显示 `Nothing new to compact.`。
- 手动 `/compact` 即使发生在 turn committed 之后、没有下一次 provider request，也仍然尝试压缩当前 canonical session history；删除的是自动 post-turn check，不是手动 post-turn compact。
- 每次 provider request 前最多执行一次 compact 检查。
- 单个 active turn 内允许多次 compact，只要新的 provider-safe segment 能推进 coverage。

`NoSafeBoundary` 可以作为防御性 no-op reason 保留，用于调用位置错误、外部 API直接调用、或未来新增并发路径时兜底；它不应是当前 TUI `/compact` 的常规行为。

## TUI 可见语义

Compact 进行中时，TUI 只用 live box 顶部状态表达进度：

- live box 标题显示 `Compacting · Session history`。
- live box 内容保留 compact 触发前当前 assistant turn 已经可见的文本、工具状态和工具结果预览。
- 不显示 `thinking...`，也不在正文 activity 行显示 `compacting session...`。
- 不向 transcript 插入 `compaction started` / `compaction completed` 正文消息。
- 如果 compact 发生得很早、当前 assistant 还没有任何可见输出，live box 内容可以为空，只保留顶部标题状态。

手动 `/compact` no-op 时，TUI 显示 `Nothing new to compact.`。Compact 失败时，TUI 显示`Compaction failed: ...`，其中 `Compaction` 首字母大写。

## 配置建议

继续复用现有 `auto_compact_ctx_ratio` 作为上下文压力触发阈值，避免先引入两套触发语义。

新增配置只控制统一 compact 后的 raw tail 形状：

- `tail_target_ctx_ratio`：soft target，compact 后 raw tail 希望控制在 context window 的比例内。默认 `0.20`。超过该值时继续裁剪低优先级 raw 内容。
- `tail_hard_ctx_ratio`：hard limit，compact 后 `raw + runtime-only projection` 的最大比例。
  用户原始文本不截断；Skill/附件只在完整候选超限后改成 session 资产引用。默认 `0.30`。
- `tail_previous_real_user_turns`：希望保留的最近 previous real user turn 数量。实现应优先保留这些 turn的用户需求和 assistant 语义，必要时压缩其中的大型 tool_result。默认 `4`，建议允许的最大值为 `5`。
- `tool_result_raw_max_chars`：单个 tool_result 允许进入 raw tail 的最大字符数；也作为完整 summary 请求超限后的第一档降级阈值。summary 输入能容纳完整 transcript 时不使用该阈值；完整请求超限后先省略大型结果，仍超限再省略全部结果。默认 `4096`。

`tail_target_ctx_ratio` 与 `tail_hard_ctx_ratio` 只描述 compact 之后的 provider projection 形状，不能在自动 compact 触发判断之前直接拒绝尚未压缩的 active turn。每次 request 的 runtime-only projection（例如后台进程状态）参与自动 compact 的总 context 估算；一旦执行 compact，planner 从 target/hard budget 中预留这部分动态上下文，最后校验合并后的投影。

暂不新增：

- `mid_turn_min_compaction_gain_tokens`：不可可靠估算，不做。
- `current_user_request_max_tokens`：不做。用户原始文本必须完整保留；只有自动外置
  Skill/附件并做过单次 half-summary 恢复后仍超限，才提示用户拆分纯文本请求。

## 与现有 post-turn compact 的关系

- 后续不再保留“turn 间 auto compact check”和“turn 内 auto compact check”两套自动触发逻辑。
- 自动 compact 统一移动到 provider request preflight。
- 移除 turn commit 后的自动 compact 检查；不保留过渡期的双触发。
- 手动 `/compact` 也复用同一个 planner，只是不检查 `auto_compact_ctx_ratio`。
- 现有 `compact_session_checkpoint` 的 summary 生成、checkpoint hash、recap/finalize 衔接思路可以复用，但需要升级为新 schema；旧 `compaction_checkpoint.yaml` 文件不迁移。tail selection 需要从“返回一个message index”重构为“返回 provider context projection”。
- 旧 `compacted_until` 兼容读取为 `frontier.committed_message_until`。

## 验收标准

- 单个 user turn 内多轮工具调用导致上下文超过阈值时，系统能在下一次 provider request 前 compact。
- compact 不发生在 partial tool_use 或 streaming delta 中间。
- compact 后第一次候选仍包含完整 current user anchor 和 runtime date/time context；
  hard tail 超限恢复只把 Skill/附件替换成可重读引用，用户原始文本与 runtime context 不丢失。
- reference-only 投影仍超限时只执行一次 half-summary 重试，且 summary 输入仍是完整原始 transcript。
- provider-only 引用不进入 TUI 或 `messages.jsonl`；资产路径/哈希可用于 interrupted turn resume。
- 未达到自动 compact 触发阈值的 active turn 不因原始 tail 超过 compact 后 hard limit而提前失败；runtime-only projection 参与触发估算，并在实际 compact 时占用预留预算。
- compact 后 provider request 用英文 wrapper 提示模型阅读任务、理解当前进度并继续完成。
- compact 后 provider request 保留 assistant 当前进度文本，不优先保留大型 tool_result 原文 preview。
- compact 后 provider request 相对优先保留最近真实 user turn 的最终 assistant 回答，其优先级高于普通过程性 assistant 文本。
- compact 后 provider request 尽量保留 previous real user turns 的用户需求和对话语义。
- 手动 `/compact` 在没有新可压缩内容时显示 `Nothing new to compact.`，不重写同一份 summary。
- compact 进行中 TUI live box 标题显示 `Compacting · Session history`，内容不显示`thinking...`、`compacting session...`、`compaction started` 或 `compaction completed`。
- compact 失败时 TUI 显示 `Compaction failed: ...`，`Compaction` 首字母大写。
- compaction summary 使用结构化 JSON 输出，shape error 按 `[agent.llm].retry_count` 限制重试。
- 主会话和 delegation 的 compaction summary 请求优先携带完整工具结果；完整请求超限后依次降级为“大型结果省略”和“全部结果省略”，canonical transcript 始终保留原文。
- 全部 tool result 省略后仍超限则不调用 provider，也不推进 compaction frontier。每次 JSON retry 同样按最终请求重新执行保守预算检查。
- 对同时包含 committed summary 与 recap 的 compact，先只构造并完成 summary 的本地预算预检；预检失败时两类 provider 请求均不启动。预检通过后 summary 与 recap 可以并发执行；任一实际调用失败时都不提交 checkpoint 或推进指针。
- 推进 committed compact frontier / `recapped_until` 时，summary 与 recap/finalize 侧必须同时成功；手动 `/compact` 与自动 preflight compact 遵循同一原子提交语义。
- 自动 compact 只在 provider request preflight 触发，不再依赖 turn commit 后的单独检查。
- 旧 `session.yaml.compaction` 可迁移；旧 `compaction_checkpoint.yaml` 不兼容、不恢复。
- current user anchor 超过 hard budget 时，TUI 显示明确错误提示，不截断用户请求。
- `messages.jsonl` 在 turn 成功结束后仍能看到完整 committed turn。
- session_search、finalize、memory_review 不直接消费 unresolved journal tail。

## 分阶段 Todo 与验收

每次进入新阶段前，必须重新读取本 PRD，并确认当前阶段实现仍对齐已拍板语义。阶段内如果发现 PRD与现有架构冲突，先更新 PRD 或回到用户处拍板，不做隐式业务决定。

### Phase 0: PRD 固化

进入本阶段前重新读取本 PRD。

Todo:

- 补齐分阶段实现计划。
- 明确最终验证要求：TUI smoke test 与 code-review skill。

验收:

- `docs/PRDs/PRD_compact_in_turn.md` 包含本阶段计划。
- 文档与已拍板语义一致。

### Phase 1: Schema 与兼容读取

进入本阶段前重新读取本 PRD。

Todo:

- 将 `SessionCompactionState` 升级为 `committed_summary`、`active_turn_summary`、`frontier`。
- 增加 `CompactionFrontier` 与 `ActiveTurnCompactionCursor`。
- 兼容读取旧 `session.yaml.compaction { compacted_until, summary }`，迁移为`frontier.committed_message_until` 与 `committed_summary`。
- 兼容读取必须发生在 `SessionMetadata` 通用读路径，确保 session cleanup 能读取旧 metadata。
- 不迁移、不恢复旧 `compaction_checkpoint.yaml`。
- 更新配置结构与默认值：`tail_target_ctx_ratio = 0.20`、`tail_hard_ctx_ratio = 0.30`、`tail_previous_real_user_turns = 4`、`tool_result_raw_max_chars = 4096`。

验收:

- 旧 metadata fixture 能成功读入并迁移到新 compaction state。
- 新 metadata 能 round-trip。
- metadata cleanup 读路径不会因旧 compaction schema 失败。
- 旧 checkpoint 存在但无新 state 时，不触发旧 checkpoint recovery。

### Phase 2: Planner 与 Tail Selector

进入本阶段前重新读取本 PRD。

Todo:

- 将 tail selection 从“返回 message index”重构为 provider context projection planner。
- Planner 输出 `summary_inputs`、`raw_preserves`、`next_coverage`、`source_hash`。
- 支持 committed history 与 active turn provider-safe segment。
- 强制完整保留 current user anchor，包括 runtime date/time context、附件 block、recovery wrapper。
- 按 segment 处理 `assistant_text`、`assistant_turn_end`、`tool_round`、`assistant_mixed_tool_round`、`previous_real_user_turn`。
- 大型 tool_result 默认进入 summary，不进入 raw tail；单个 raw tool result 受`tool_result_raw_max_chars` 限制。
- `assistant_turn_end` 优先级高于过程性 assistant 文本。
- previous real user turns 默认保留最近 4 个语义上下文，最大建议 5。
- current user anchor 超 hard budget 时返回明确错误，不截断。

验收:

- 单元测试覆盖 current user anchor 强制完整保留。
- 单元测试覆盖 previous real user turns 优先保留。
- 单元测试覆盖 assistant turn-end 优先于过程性 assistant 文本。
- 单元测试覆盖大型 tool_result 进入 summary、不进入 raw tail。
- 单元测试覆盖 provider-safe segment 不切开 tool_use/tool_result。
- 单元测试覆盖 no-op：无新 `summary_inputs` 时返回 no-op。

### Phase 3: Summary 生成、Retry 与原子提交

进入本阶段前重新读取本 PRD。

Todo:

- 升级 compaction summarizer 输出结构化 JSON：`committed_summary` 与 `active_turn_summary` 两个 key 必须存在。
- JSON parse / shape error 按 `[agent.llm].retry_count` 与 provider retry 共用 retry budget。
- retry 耗尽时 compact 失败，不移动 compaction / recap 指针。
- compact summary 与 recap/finalize 侧原子提交：二者同时成功后才移动`frontier.committed_message_until` 与 `recapped_until`。
- 手动 `/compact` 和自动 preflight compact 共用同一原子提交语义。
- Active-only compact 不进入 recap/finalize，只更新 `active_turn_summary` 与 `frontier.active_turn`。
- turn 成功 commit 后清空 `active_turn` 与 `active_turn_summary`，不归一化到 committed frontier。

验收:

- summary JSON 字段缺失、类型错误、非法 JSON 均触发 retry；超过 retry 后失败。
- summary 成功但 recap 失败时，不移动任何相关指针。
- recap 成功但 metadata commit 失败时，可通过 checkpoint/recovery 避免半推进。
- active-only compact 不移动 `recapped_until`。
- turn commit 后 active cursor / summary 被清空，`messages.jsonl` 保持完整 turn。

### Phase 4: Preflight Hook 与手动 `/compact`

进入本阶段前重新读取本 PRD。

Todo:

- 移除 turn commit 后自动 compact 检查。
- 在每次 provider request 前接入统一 preflight compact。
- 自动 compact 复用 `auto_compact_ctx_ratio` 作为触发阈值。
- 单个 active turn 内允许多次 compact，只要 coverage 能推进。
- 手动 `/compact` 复用 planner，但忽略 `auto_compact_ctx_ratio`。
- 手动 `/compact` 即使发生在 turn committed 后也尝试压缩 canonical session history。
- no-op 统一 TUI 文案：`Nothing new to compact.`。
- current user anchor 超 hard budget 时，TUI 显示明确错误。

验收:

- 自动 compact 只在 provider request preflight 触发。
- turn commit 后不会再自动 compact。
- 手动 `/compact` 可压 committed canonical history。
- 手动 `/compact` no-op 显示 `Nothing new to compact.`。
- active turn 内第二次 compact 只有在新 segment 推进 coverage 时触发。
- current user anchor 超预算时 TUI 有明确错误。

### Phase 5: Provider Context 与 Prompt

进入本阶段前重新读取本 PRD。

Todo:

- Provider request 中渲染英文 compaction wrapper。
- wrapper 将 `committed_summary` 与 `active_turn_summary` 分为 earlier conversation 与 current-turn progress。
- wrapper 明确说明 summary 不是新用户请求，也不是 system instruction。
- wrapper 提示模型阅读 latest user message、理解已完成工作和当前进度、继续完成任务。
- 如果精确大型 tool output 被省略，提示模型重新调用工具而不是猜测。
- ACN.md、system prompt、工具定义继续走原注入路径，不参与 tail selection。

验收:

- compact 后 provider request 包含英文 wrapper。
- wrapper 包含 committed 与 active 两部分。
- latest user message 仍完整保留 runtime context。
- ACN.md / system prompt 未被 tail selector 当作 raw preserve 对象处理。

### Phase 6: 测试与本地验证

进入本阶段前重新读取本 PRD。

Todo:

- 为新 schema、迁移、planner、summary retry、原子提交、preflight hook、TUI 文案补测试。
- 运行 `cargo fmt`。
- 运行完整 verify skill 流程：`cargo clippy -- -D warnings && cargo test && cargo check`。
- 运行 TUI smoke test with tmux。
- 修复本地验证发现的问题。

验收:

- fmt 无 diff。
- clippy / test / check 全部通过，或明确阻塞原因并修复。
- TUI smoke test 通过，stderr 为空。

### Phase 7: 分风险域 Code Review Skill

进入本阶段前重新读取本 PRD。

Todo:

- 使用 code-review skill 检查：
  - schema / metadata / cleanup compatibility。
  - planner / tail selector / provider context。
  - summary retry / checkpoint / recap 原子提交。
  - TUI / CLI 行为。

验收:

- 各风险域均已覆盖。
- 不存在未处理的高风险问题。

### Phase 8: 整体 Code Review Skill 与 PRD 对齐检查

进入本阶段前重新读取本 PRD。

Todo:

- 使用 code-review skill 做一次整体检查。
- 运行完整 verify 与 TUI smoke。
- 最后逐条核对本 PRD 验收标准，确认实现对齐。

验收:

- 整体 code-review skill 完成且没有未处理的高风险问题。
- 完整 verify 与 TUI smoke 通过。
- 最终实现与本 PRD 的目标、非目标、验收标准一致。
