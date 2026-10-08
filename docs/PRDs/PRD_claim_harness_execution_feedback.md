# Claim Harness：执行反馈与压缩后的文件工作集

> 状态：已实现并通过离线验证（2026-09-05）。本文承接 [PRD_claim_harness.md](PRD_claim_harness.md)，只覆盖 ACN 运行时能力；评测侧的冻结 Router 目录与对照臂定义不在本文范围。

## 背景

[PRD_claim_harness.md](PRD_claim_harness.md) 解决了 claim 的可发现、可核对与可修订。长任务中还有两类信息损失没有处理：

- 终态长输出超过 `max_output_chars` 后，模型只能看到连续前缀。命令末尾的错误、测试汇总或退出摘要要等到下一轮轮询才可见，每次都多出一轮反馈。
- 上下文压缩后只保留模型生成的摘要。已读取、已修改的文件路径可能被摘要遗漏，模型会重复读文件，或忘记自己改过哪些文件。

同时，claim 内容约束沿用旧措辞时，容易把一次性日志写成知识，或者为了压短而删掉适用条件。本文一并收紧 claim 与 recap prompt 的表述，并把“目录摘要中的 ID 仅供发现”写进 recap 规则，与 [PRD_claim_harness.md](PRD_claim_harness.md) 的有界目录保持一致。

验收对象是可执行机制，不以离线测试代替模型效果实验。

## 设计选择

| 承载点 | 修改 | 验证的风险 |
| --- | --- | --- |
| `code_run` / `write_stdin` | 终态连续长输出在原字符预算内展示前缀与尾部预览 | 游标跳过未读内容、ACK 回滚丢输出、UTF-8 截断 |
| SessionEngine compaction projection | 从成功文件工具结果重算并保留文件工作集 | 摘要遗失路径、失败操作被算成功、漏计压缩预算 |
| 既有 claim / recap prompt | 保留判断的条件、机制、证据范围与未验证边界 | 一次性日志冒充知识、笼统高置信度、压短后丢限定 |

不新增模型调用、运行时依赖、部署入口、配置项或 claim 持久化 schema。TUI 继续现有精简工具摘要，不新增完整输出面板。

## 终态输出的尾部预览

终态输出只有在快照连续、无丢失且覆盖当前流结尾时才提供尾部预览。每个流沿用原 `max_output_chars`，其中四分之一分配给尾部，其余为连续前缀；不足四字符时保持原分页。`stdout_cursor` / `stderr_cursor` 只推进前缀，尾部预览另附起始字符游标 `*_tail_preview_start_cursor`。预览不是消费确认：中间内容仍可逐页读取，provider 失败回滚，最后连续页确认后才清理进程 entry。运行中、buffer gap 或快照还有后续保留页时继续原分页。

尾部预览进入工具的结构化结果，正常 provider 请求原样接收。若恰在此时发生 compaction，既有大工具结果省略规则仍可能省略整条输出；对应 receipt 会回滚，后续可重新读取。因此本改动不保证每次压缩后都能立即看到尾部。

## 压缩后的文件工作集

文件工作集由真实成功的 `file_read` / `file_write` / `file_patch` 结果与对应 ToolUse 配对提取，不猜测 shell 改动，不把未完成操作算成功。每次压缩从 canonical history 与 active messages 确定性重算，在模型生成的摘要之外保留；修改过的路径不再重复列为只读路径。

每类最多 64 条、单路径最多 512 字符、最终编码后的路径数组最多 2048 字符，超量报告 omitted；路径作为 JSON 数据并转义 `<`。列表仅记录操作发生过，不表示文件当前仍存在、内容已验证或拥有修改许可。其成本进入上下文预算和压缩 preflight。工作集不新增持久化文件，恢复时复用 session 与 turn journal。

## Claim 内容约束

claim 继续使用既有字段：`statement` 保存适用条件、判断及行动含义，保留会改变结论的限制或反例；`evidence_summary` 区分观察与推断，保留可核对的证据锚点和未覆盖边界，不复制原始日志或私有 trace。可复用的失败机制、约束或决策规则可以成为知识，一次性的 bug / PASS / FAIL 流水不能。不能仅凭任务成功、引用次数或自行新增测试提高置信度。

recap 的 `used_claim_ids` 与来源只能取自 `local_claims` 或 transcript 中已展示完整正文的 claim；目录摘要中的 ID 仅供发现，没有读取正文时不能计入使用、来源或 dispute。

## 外部参考及取舍

- [Pi compaction](https://github.com/earendil-works/pi/blob/9841914c71a74d81abe07f751aefd271fd924e63/packages/coding-agent/src/core/compaction/compaction.ts) 在摘要之外维护文件操作信息。ACN 进一步要求与成功工具结果配对，且只声称减少摘要遗忘，不声称能修正错误推理。
- [SWE-agent，NeurIPS 2024](https://proceedings.neurips.cc/paper_files/paper/2024/hash/5a7c947568c1b1328ccc5230172e1e7c-Abstract-Conference.html) 把 agent-computer interface 视为关键实验变量。尾部错误更早可见可能减少反馈轮数，但压缩前缀也可能推迟中段信息，净收益须由实验验证。
- [ACE](https://arxiv.org/html/2510.04618v1) 指出全量重写和过度压短会丢失积累的细节。ACN 复用现有 claim 更新与来源关系，去掉机械字数目标，保留必要条件，仍需防止 token 膨胀。

## 验收

- 尾部预览：多字节字符边界、ACK 回滚后重读、中段内容可继续分页读取、最后连续页确认后清理进程 entry；运行中、非连续页、快照截断或仍有保留页时不启用预览。
- 文件工作集：跨多次压缩确定性重算，失败与未完成操作不计入，shell 路径不猜测，数量 / 路径长度 / 编码总长边界与 omitted 计数正确，`<` 转义，且计入 committed summary 的 token 估算。
- prompt：`what_is_claim.j2`、`session_recap.j2` 的新约束由 prompt 渲染测试覆盖。
- 已通过版本一致性、`cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check`。
- 验证边界：fake provider 与单元测试证明的是传递与使用链路，不证明模型会自然作出正确选择；tail preview 遇 compaction omission 的回滚与重读由已有分层测试覆盖，尚无单个组合链路测试。
