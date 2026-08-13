# 内部 LLM 任务流式化

> 状态：已完成（2026-08-12）

## 范围

以下调用改为 streaming-first，并只在完整终态后消费或提交结果：Inbox 内化、Session compact summary、Subagent compact summary、Recap、Memory review、Router LLM rerank。

Agent 内部任务复用 `[agent.llm]` 的 provider、timeout 与 retry 配置；Router rerank 复用 `[router.rerank]`，支持 `openai_chat`、`openai_responses` 与 `anthropic`。Router 不启用 WebSocket，也不发送 thinking/reasoning。

## 重试与降级

- 协议层没有取得完整可消费结果时，按配置重试当前 transport；耗尽后按 WS → SSE → non-streaming 或 SSE → non-streaming 降级。
- 完整结果的 JSON、schema 或引用校验失败时，使用独立的业务重试轮次；Inbox 重发原始请求，其余结构化任务可追加纠错信息。
- 流式 partial 在内部缓冲，失败后整段丢弃；只有完整终态可以进入业务校验、工具执行或持久化。
- 认证、非法请求、上下文耗尽与取消属于终止错误，不盲目切换 transport。
- MaxTokens 沿用各 adapter 已有的 continuation 与最终终止行为。

## WebSocket sticky

Responses continuation chain 与 fallback scope 相互独立。Inbox 使用 session root scope；其确定性 Upgrade 不支持、握手/连接或 WS stream 故障在协议重试耗尽后会使该 session root sticky 到 SSE。主 Agent 和所有当前或未来 Subagent 动态观察 root，同时各自维护互不影响的 local sticky。若主 Agent 已经 local sticky，后续 `/inbox` 会跳过 WS，并把该结论提升到 session root。该状态只存在于当前进程，resume 后重置。

429、5xx、连接池等待超时、previous-response 恢复、SSE 到 non-streaming 降级及业务输出无效都不会写入 sticky。

## 展示

活动状态保留原有标题，并在末尾追加从该状态开始连续累计的总秒数，例如 `Initializing · Syncing inbox · 8s`、`Working · Streaming response · 16s` 与 `Compacting · Session history · 5s`。不展示内部 attempt 或具体并行子任务；Idle、Error 与 Closed 标题不计时。
