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

### HTTP 413 与媒体剥离

- OpenAI-compatible Responses、Chat Completions 与 Anthropic adapter 把 HTTP 413
  统一映射为 provider-neutral `request too large` 分类；413 不进入 streaming
  non-streaming fallback，也不按上下文 token 超限处理。统一错误不携带上游 response
  body，避免兼容网关回显 system/user/tool 请求内容进入日志或 turn journal。
- 当前 logical turn 第一次收到该分类，且当前 provider-neutral history 含 `Image` 或
  `Document` 时：
  - 把全部图片/PDF block 原位替换为不含 base64 的确定性文本占位符；
  - 清除 provider-private replay，清空 frozen provider prefix，并丢弃当前 transport
    runtime chain；
  - 额外执行一次恢复请求。恢复请求重新使用 adapter 的正常 retry 配置，不占用或改写
    用户配置的 adapter retry count。
- 本轮第二次 413，或请求中没有可剥离媒体时，直接返回错误，不继续恢复。
- 剥离只影响发给上游的 provider history。恢复成功提交时，canonical transcript 仍
  保存本轮原附件；下一份 provider-history WAL 写入剥离后的精确窗口，因此相同协议与
  精确 model 的 resume 不会再次发送旧媒体。
- 如果清理后的恢复请求仍失败，沿用现有 failed-turn 语义：本轮不提交 canonical
  transcript，但保留干净的 provider-history WAL 以便 resume。特别是剪贴板内联图片
  不会另建附件 sidecar；用户若仍需模型查看，后续需重新附加较小文件。
- 用户会收到一次 warning，明确本轮模型上下文已移除媒体，并提示需要时重新附加。

## 非目标

- 不新增通用的 session 消息编辑、删除附件或历史清理命令。
- 不把其他 4xx、上下文 token 超限、单图片格式/尺寸错误统一归入 413。
- 不修改稳定用户指南或核心行为文档；该恢复属于边界策略，记录在本 PRD。

## 验收

- Runtime Context 的 cwd 来源、同目录去重、换目录 append 与落盘重载均有回归测试。
- 三类 adapter 的 413 分类均有单元测试。
- turn loop 覆盖含媒体恢复、无媒体直错和第二次 413 直错。
- SessionEngine 覆盖 canonical 附件保留、剥离后 provider history 落盘，以及重载后继续
  复用干净前缀；同时覆盖 InlineImage 清理重试仍失败后的重载语义。
