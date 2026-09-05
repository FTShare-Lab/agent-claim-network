# DeepSWE Claim Harness：可发现的知识与可靠的执行反馈

状态：已实现并通过离线验证；真实 DeepSWE 分数待异机评测。本分支基于 `feature/claim-harness` 的 `ff12e50`，不变更测评任务、verifier、计分与四臂定义。

## 目标与最小改动面

让 claim 的可复用判断真正进入解题决策，并减少长输出和上下文压缩造成的信息损失。验收对象是可执行机制，不以离线测试代替模型效果实验。

| 承载点 | 修改 | 验证的风险 |
| --- | --- | --- |
| Frozen Router overview / evaluation prompt | 自动展示有界 claim 摘要，用既有 scope query 展开正文 | 摘要泄漏正文、越过一次查询、错误归因、污染 B_empty |
| `code_run` / `write_stdin` | 终态连续长输出在原字符预算内展示前缀与尾部预览 | 游标跳过未读内容、ACK 回滚丢输出、UTF-8 截断 |
| SessionEngine compaction projection | 从成功文件工具结果重算并保留文件工作集 | 摘要遗失路径、失败操作被算成功、漏计压缩预算 |
| 既有 claim / recap prompt | 保留判断的条件、机制、证据范围与未验证边界 | 一次性日志冒充知识、笼统高置信度、压短后丢限定 |
| 文档与现有测试 | 离线链路及异机实验说明 | 把流程通过误报为分数提升 |

不新增模型调用、运行时依赖、部署入口、配置项或 claim 持久化 schema。正常 Router 的 scope overview 保持原有输出；冻结目录通过可选默认字段扩展，旧响应仍可读取。

## Claim 的发现与使用

`B_claim` 首次 system context 除 scope 统计外，展示最多 20 条摘要，包含 `id/name/scope/holder/confidence`，字段有长度边界，超量明确报告 omitted。目录是待判断的数据，不是执行指令；不包含 `statement` 或 `evidence_summary`。名称本身可能提供知识线索，这属于实验处理的一部分。

模型从摘要选择最相关 scope，通过现有 `consult_router(scope, semantic_query)` 获取完整候选；同 scope 可一次返回多条。保留一次 query 和 `RouterEvidence` / Gate 契约，不增加按 ID 获取正文的路径。摘要中的 ID 不能仅因出现在目录就计为使用；实际引用与使用仍须由完整候选及执行过程支持。

`B_empty` 无摘要、无正文；`B_forced_claim` 仍强制注入全量正文，作为检索摩擦的诊断对照。冻结 claim 继续保留 producer holder，不落为 consumer 自有 claim，不启用自有 claim 编辑工具。跨 scope 取用和超量目录仍可能漏召回，此版本不宣称解决所有检索问题。

## 执行与压缩契约

终态输出只有在快照连续、无丢失且覆盖当前流结尾时才提供尾部预览。每个流沿用原 `max_output_chars`，其中四分之一分配给尾部，其余为连续前缀；不足四字符时保持原分页。`stdout_cursor` / `stderr_cursor` 只推进前缀，尾部预览另附起始字符游标。预览不是消费确认：中间内容仍可逐页读取，provider 失败回滚，最后连续页确认后才清理进程 entry。运行中、buffer gap 或快照还有后续保留页时继续原分页。

尾部预览进入工具的结构化结果，正常 provider 请求原样接收。若恰在此时发生 compaction，既有大工具结果省略规则仍可能省略整条输出；对应 receipt 会回滚，后续可重新读取。因此本改动不保证每次压缩后都能立即看到尾部。TUI 继续现有精简工具摘要，未新增完整输出面板。

文件工作集由真实成功的 `file_read/file_write/file_patch` 结果与对应 ToolUse 配对提取，不猜测 shell 改动，不把未完成操作算成功。每次压缩从 canonical history 与 active messages 确定性重算，在模型生成的摘要之外保留。每类最多 64 条、单路径最多 512 字符、最终编码后的路径数组最多 2048 字符，超量报告 omitted；路径作为 JSON 数据并转义 `<`。列表仅记录操作发生过，不表示文件当前仍存在、内容已验证或拥有修改许可。其成本进入上下文预算和压缩 preflight。

claim 继续使用既有字段：`statement` 保存适用条件、判断及行动含义；`evidence_summary` 区分观察与推断，保留可核对的证据锚点和未覆盖边界。可复用的失败机制可以成为知识，一次性的 bug / PASS / FAIL 流水不能。不能仅凭任务成功、引用次数或自行新增测试提高置信度，也不将 verifier 结果回灌为 producer 知识。

## 研究依据与可证伪推断

| 依据 | 借鉴点 | 在 ACN 中的推断与限制 |
| --- | --- | --- |
| [Pi compaction](https://github.com/earendil-works/pi/blob/9841914c71a74d81abe07f751aefd271fd924e63/packages/coding-agent/src/core/compaction/compaction.ts) | 摘要之外维护文件操作信息 | 确定性状态可减少摘要遗忘；ACN 进一步要求成功结果配对，但不声称它能直接修正错误推理 |
| [SWE-agent，NeurIPS 2024](https://proceedings.neurips.cc/paper_files/paper/2024/hash/5a7c947568c1b1328ccc5230172e1e7c-Abstract-Conference.html) | Agent-computer interface 是软件工程 agent 的关键实验变量 | 尾部错误更早可见，可能减少反馈轮数；压缩前缀也可能推迟中段重要信息，须测净收益 |
| [ExpeL，AAAI 2024](https://ojs.aaai.org/index.php/AAAI/article/view/29936) | 从经验提炼自然语言判断并在后续任务中检索 | claim 的收益依赖内容质量、相关性与使用，存储更多不等于更有效 |
| [ACE 原始论文](https://arxiv.org/html/2510.04618v1) | 全量重写和过度压短会丢失积累的细节，增量修订保留知识 | 复用现有 claim 更新与来源关系，避免新增整本记忆重写；去掉机械字数目标，保留必要条件，仍需防止 token 膨胀 |

上述来源支持机制选择，不提供 ACN 的预期分数。当前假设可拆成三条：摘要提高相关知识的发现概率；条件与证据提高知识被正确应用的概率；可靠反馈减少执行与压缩损失。任何一环失效，都可能使 `B_claim` 弱于 `B_empty`：错误 claim 引发确认偏误，无关目录消耗预算，过宽 scope 混入噪声，或获取知识的成本超过节省的探索成本。

更强的研究主张应是：带范围、证据、来源及可修订关系的 claim，比相同成本的自由文本记忆更可靠地迁移，并能在反例与冲突中纠正错误。当前同题 A→B 的暖启动四臂不能证明跨题泛化、claim 结构的独立贡献或论文新颖性。需要后续增加等 token / 等 producer 成本的普通记忆强基线、跨题与跨仓库拆分、错误知识和冲突注入、多 seed 配对置信区间及组件消融。这些是研究要求，不是本版 runner 已提供的选项。

## 验证与交付

离线已覆盖摘要→query→正文→文件行为的真实 Standard Evaluation SessionEngine 链路和 B_empty 对照，确认两臂工具 schema 相同且目录本身不产生 evidence。边界回归覆盖目录数量/长度/转义、终态输出 ACK/rollback 与 Unicode、连续压缩的文件工作集及预算。

交付检查：版本一致性、`cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test`、`cargo check`、`acn` 构建和 canonical tmux smoke 均通过；现有 DeepSWE Python 测试 229 项完成（1 项跳过），示例 schema 与 dry-run fixture 通过。独立 `gpt-5.6-sol` 只读审查未发现 P0/P1 问题。

验证边界：fake provider 证明的是传递与使用链路，不证明模型会自然作出正确选择。tail preview 遇 compaction omission 的回滚与重读由已有分层测试覆盖，尚无单个组合链路测试；B_forced_claim 首次交付主要由现有 helper / Router 测试覆盖。未运行真实模型评测或 Docker 评测任务。

完整测评在另一台机器运行。操作与归因见 [四臂配对实验说明](../../benchmarks/deepswe/CLAIM_HARNESS_EXPERIMENT.md)。分别比较新旧 `B_empty` 的共享 harness 效果、各版本 `B_claim − B_empty` 的知识收益、`B_forced_claim − B_empty` 的强制注入收益。由于 producer prompt 同时变化，跨版本 claim 差值包含知识内容与 consumer 两方面影响，不能当成纯检索效应。
