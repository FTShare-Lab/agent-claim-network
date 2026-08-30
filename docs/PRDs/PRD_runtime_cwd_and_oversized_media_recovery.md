# Runtime cwd 与超大媒体请求恢复

状态：已实现

## 背景

模型需要知道 ACN 工具相对路径的实际基准，但进程 cwd、单次工具调用的 `cwd` 与
`agent.tool.workspace_root` 不是同一个事实源。另一方面，HTTP 413 表示请求体字节数
过大，不等同于上下文 token 超限；如果历史图片或 PDF 始终原样重放，单纯提示用户
减小新附件无法让既有会话恢复。

## 决策

### Runtime cwd

- Runtime Context 继续使用独立、可持久化的 `ModelContext` user message。
- snapshot 包含本地日期、时区和 `cwd`；`cwd` 取已解析的
  `agent.tool.workspace_root`，不调用进程级 `current_dir()`。
- 每次 Provider 请求前沿用既有观察与 fingerprint 去重：新 session 注入一次；日期、
  时区或 workspace root 变化时 append 一份新 snapshot；语义未变化时不追加。
- 同目录 resume 原样复用历史 snapshot；换目录 resume 追加新 snapshot。单次工具调用
  显式传入的 `cwd` 不改变 Runtime Context。
- Provider retry 复用本轮已冻结消息；该机制不改写旧前缀，保持 append-only 缓存边界。

### 请求过大与媒体剥离

- OpenAI-compatible Responses、Chat Completions 与 Anthropic adapter 把 HTTP 413
  统一映射为 provider-neutral `request too large` 分类；413 不进入 streaming
  non-streaming fallback，也不按上下文 token 超限处理。统一错误不携带上游 response
  body，避免兼容网关回显 system/user/tool 请求内容进入日志或 turn journal。
- Responses WebSocket 的兼容网关可能把上游 `close 1009 (message too big)` 包装成
  500/502。仅当状态为 500/502，且错误正文同时明确包含关闭码 `1009` 与
  `message too big` 时识别该信号：WS 跳过无效重试、清除当前 continuation chain，
  非 sticky 地立即 fallback 到 HTTP。HTTP 若成功则保留媒体；若仍返回相同明确信号，
  不重试该 HTTP 请求并映射为 `request too large`，再进入下述剥离流程。普通 5xx、仅有
  模糊尺寸文字或仅有关闭码的错误保持既有 retry/fallback 语义。
- 每次收到该分类，只要当前 provider-neutral history 仍含 `Image` 或 `Document`：
  - 把全部图片/PDF block 原位替换为不含 base64 的确定性文本占位符；
  - 清除 provider-private replay，清空 frozen provider prefix，并丢弃当前 transport
    runtime chain；
  - 如果失败的 provider attempt 已产生可见但未完成的流式文本，先持久化
    `AssistantOutputDiscarded` 并从 TUI 移除该临时文本，避免清理后的工具型响应留下
    canonical 中不存在的“幽灵”助手消息；
  - 追加一条独立的 `RequestSizeRecovery` ModelContext，说明旧媒体内容当前不可见，并
    提示模型仅重新读取完成任务必要的少数本地文件；
  - 使用 adapter 的正常 retry 配置重新请求，不占用或改写用户配置的 adapter retry
    count。
- 恢复不以 logical turn 为限。模型成功响应并重新读取文件后，如果新请求再次因媒体
  被归类为请求过大，则再次剥离新媒体、追加新边界并重试。清理后的紧邻请求若仍被
  同类拒绝，因已没有可剥离媒体而直接返回错误，不会原地无限重试；无媒体的请求过大
  也不误判为上下文 token 超限或强制 compact。
- `RequestSizeRecovery` 同时是按时间排序的媒体剥离边界：Provider 上下文从 canonical
  transcript 或 turn journal 重建时，最后一道边界之前仍存在的 `Image`/`Document`
  必须投影为同样的 provider-neutral 占位符；边界之后由新 user attachment 或
  `file_read` 产生的媒体正常发送。每次真实剥离都追加新边界，即使提示正文相同也不按
  fingerprint 去重。
- 持久化按既有三层职责进行：
  - `provider_history.json` 在下一次网络发送前保存实际重试的精确 Provider 窗口，旧
    媒体已变为占位符；
  - `turn_events.jsonl` 在重试前立即记录 `ModelContextAppended`，失败或中断的 turn
    仍可恢复剥离边界，但不重复保存附件 base64、数量、路径或哈希清单；
  - `messages.jsonl` 只在 turn 成功提交时保留真实原附件并追加该 ModelContext；失败
    turn 不提交 canonical，后续成功继续时再沿用既有 journal recovery 机制物化边界。
- Provider history 缺失或 replay identity 不匹配时，重建必须继续遵守已持久化边界。
  成功 turn 可从 canonical 原附件逐块重建占位符；失败 turn 若同时缺少 Provider WAL，
  只保证恢复提示与不重新发送旧媒体，不为恢复精确附件元数据建立第三份事实源。
- 每次真实剥离都即时显示：`Warning: 上游拒绝了过大的请求；已从上下文中移除图片 /
  PDF 并重试。`。Warning 不持久化或在 resume 时重放；持久恢复事实由 ModelContext
  承担。没有可读本地路径的附件仍由模型提示用户稍后提供较小版本。
- canonical transcript 永不改写或删除原附件。旧媒体的禁止重放只属于 Provider
  投影；模型显式重新读取同一文件属于新的真实会话事件，不维护路径黑名单。
- 新版本读取旧 session 不受影响；本功能暂不承诺旧版本读取已经写入
  `RequestSizeRecovery` source 的 session，也不根据历史错误文本自动迁移旧 session。

## 非目标

- 不新增通用的 session 消息编辑、删除附件或历史清理命令。
- 不把其他 4xx、上下文 token 超限、单图片格式/尺寸错误统一归入 413。
- 不修改稳定用户指南或核心行为文档；该恢复属于边界策略，记录在本 PRD。

## 验收

- Runtime Context 的 cwd 来源、同目录去重、换目录 append 与落盘重载均有回归测试。
- 三类 adapter 的 413 分类均有单元测试。
- Responses 覆盖精确 WS 1009/500/502 信号的立即 HTTP fallback、非 sticky、HTTP
  同类错误不重试并触发媒体剥离，以及普通 5xx 不误分类。
- turn loop 覆盖含媒体恢复、无媒体直错和第二次 413 直错。
- turn loop 额外覆盖同一 logical turn 内重新产生媒体后可再次恢复，以及紧邻无媒体
  413 不循环；流式 partial 后 fallback 返回 413 时，恢复重试前会丢弃 partial。
- SessionEngine 覆盖 canonical 附件与恢复 ModelContext 落盘、turn journal 先行持久化、
  剥离后 provider history 落盘、同 identity 重载复用干净前缀，以及 Provider WAL
  缺失或 identity 变化时从剥离边界重建。InlineImage 清理重试仍失败后的重载也保留
  干净前缀和 journal recovery 语义；被丢弃的流式 partial 不进入恢复 timeline。
