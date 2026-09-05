# Claim Harness：发现、核对与修订

状态：已实现并通过本地验收。基于 `deepswe-evaluation@8b2f52f`，工作分支为 `feature/claim-harness`。

## 问题与证据

当前普通 session 把全部有效本地 claim 的 statement 注入冻结 system prompt，却没有按需读取完整 evidence、来源 trace 或在主流程修订 claim 的工具。随着本地知识积累，启动上下文会持续增长；错误或过时的判断只能等待后台知识流程处理。trace 已经持久化，但没有本地检视入口。

[Full-113 报告](../../benchmarks/deepswe/reports/full-113-v4-flash-local-exp-20260815-directfull30-defaultcompact-r1.md) 中，`B_empty` 为 48/113，`B_claim` 为 46/113，`B_forced_claim` 为 50/113。强制披露比空白组只多通过 2 题，尚不能证明稳定收益。全体 attempt 的平均 steps/input token 较低，也不能替代同题均成功样本的效率比较。本地小样本周报还记录了自主 claim 组经常未查询 Router；这是发现与触发不足的证据，不能据此认定 claim 内容有害。

因此这次先改善知识的可发现性、可核对性和可修订性。不会宣称这些产品能力已经提高 DeepSWE 成绩，也不把历史 provider、并发或 compaction 故障未经复现地当成当前 bug。

## 设计选择

1. **目录 → 正文 → 历史依据。** 普通 session 启动只注入有界 claim 目录，正文通过 `claim` 工具按需读取；trace 列表只提供关联摘要，任务文本分页展开。目录在 session 内保持冻结，工具读取最新本地状态。
2. **用户与主 agent 共用领域操作。** `/claim` 和模型的 `claim` 工具使用同一个 `AgentRunner` 入口；不直接编辑 YAML，不新增独立 CLI、MCP server 或插件运行时。
3. **修订自己的判断。** 支持修改现有自有 claim 的 name、statement、scope、evidence_summary、confidence、status。id、holder、created_at 与 source_claim_ids 保持不变。新建、来源链重写和 dispute 仍由既有知识流程负责。
4. **读后再写，冲突可见。** 读取正文返回完整内容 revision；修改必须提交该 revision，在现有 `knowledge_apply.lock` 内重读并比较。相同 revision 的并发编辑只能有一个成功，冲突不静默覆盖。秒级 updated_at 严格递增，保持团队 mirror 新旧版本可辨。
5. **trace 是历史关系，不是质量分数。** 只读取当前 Agent 的本地 trace，展示 claim 曾作为输入或输出的任务。旧 trace 没有 claim 版本快照，不能证明当前修订内容仍成立；本次不伪造验证结果、不按引用次数提升 confidence、不把编辑事件塞进任务 trace。

## 外部参考及取舍

- [Pi 的 context engineering](https://pi.dev/) 把能力说明与按需正文分开，支持保持 prompt cache 稳定的渐进披露。本次借鉴这个方式，不引入其运行时或扩展生态。
- [Pi extensions](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/extensions.md) 让工具与命令共用宿主能力。本次对应为一个 ACN 领域入口、一个模型工具和一个 TUI 面板。
- [Codex App Server 的 dynamic tool calls](https://learn.chatgpt.com/docs/app-server#dynamic-tool-calls-experimental) 由宿主注册工具并承接调用。ACN 已有 provider-neutral registry，因此只增加一组稳定的 claim 操作；不为每条 claim 动态注册一个工具，也不复制实验协议。

## 最小改动面与失败边界

- `src/agent/claims.rs`：目录、详情、revision 修订和本地 trace 回查；复用既有 claim store、知识锁、原子写与 Maintainer 队列。
- `src/tool/claim.rs`、registry 与 bootstrap：注册并装配普通主 agent 专用工具；subagent、memory review 和 evaluation profile 不获得该权限。
- `src/agent/session_engine/prompts.rs`、`prompts/agent_system.j2`：普通会话用轻目录，说明检索、证据核对和修订规则。既有 evaluation prompt 和冻结 claim 输入保持原契约。
- `src/session_tui/claim_panel.rs` 与既有命令、事件、渲染入口：列表、详情、trace 和编辑状态机，复用 Composer。存储 I/O 不在绘制或键盘处理内执行。
- finalize checkpoint：阻止中断后重放旧准备结果覆盖后来已经生效的修订，保持原有幂等恢复能力。

Claim 与 Trace 的 YAML 格式和存储目录不迁移。目录与 trace 分页限制由接口显式返回，不悄悄截断全文。单人模式仅写本地；团队模式复用现有上传恢复路径，上传失败不能被报告成已经同步成功。敏感私有资料不得因修订进入共享 claim。

具体资源边界：claim/trace 列表默认每页 20 条、最多 100 条；claim 目录的 name/scope 分别最多展示 120/240 个字符，并标记是否截断。trace 任务正文默认每页 4,000 个字符、最多 16,000 个字符。完整 claim 正文通过单条读取保留原数据，不改变既有 Claim 容量契约。

团队模式的跨文件提交在 `<agent_home>/claim_edit_pending.yaml` 保存预期 revision 和目标 claim，依次完成本地原子写、既有 durable 上传入队、清除待完成记录。启动同步、后续 claim 操作和 finalize 在相同知识锁内恢复该记录；遇到较新的本地版本保留并同步新版本。单人模式只有本地单文件写入，不创建团队待完成记录；从团队模式切换而遗留的记录保留原样，恢复团队配置后再处理。队列交付仍由既有 inbox/finalize 流程触发，保存不等待网络。

## 验收

- 大量或超长 claim 不再使普通会话的初始 claim block 无界增长；目录不包含 statement/evidence，后续可以搜索并读取遗漏条目。
- 工具及 TUI 可以回查 claim 的完整正文、来源关系和分页 trace 文本。
- 编辑保留不可变字段，检测 stale revision，拒绝越权和非法输入；并发、团队同步、单人模式与 checkpoint 恢复有聚焦回归。
- `/claim` 可完成选择、查看、编辑、保存和取消；错误保留草稿，返回聊天后不丢原输入。通过实际 tmux 流程验收。
- 已通过版本一致性、fmt、Clippy、全量 Rust tests/check 与修复后的全部 TUI 回归。实际 tmux 验证覆盖分页、80 行编辑保存与取消、知识锁等待期间退出和重开，以及 canonical smoke；stderr 为空。独立只读 review 发现的恢复、异步交互和错误展示问题已修复并补回归。
- `benchmarks/deepswe/`、评测专用 prompt 与计分协议无改动；本地提交供审核，不修改 `main`。

## 后续实验

下一轮 DeepSWE 应在同一冻结任务、模型与资源设置下，单独验证“自主发现 → 返回候选 → 展开正文 → 使用或拒绝 → verifier”的链路，并用多次 rollout 和逐题配对比较区分触发收益、内容收益与随机波动。成功效率应只比较同题均通过的样本。当前迭代交付的是可检视、可修订的产品能力，不包含新一轮基准成绩。
