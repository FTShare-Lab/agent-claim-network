# ACN 产品需求文档

本目录保存 ACN 已实现功能的需求决策、分阶段实施步骤与验收边界。每份 PRD顶部状态是当前结论；已完成 PRD 不保留逐轮 review、修复、复验、临时产物路径等实施流水账。

## Session 与恢复

- [OpenAI Responses API 支持](PRD_openai_responses.md)
- [Prompt Cache 前缀稳定性修复](PRD_fix_cache_hit.md)
- [Provider Request 前统一压缩](PRD_compact_in_turn.md)
- [Turn Journal 与 Mid-Turn 恢复](PRD_turn2message.md)
- [流式失败回退非流式重试](PRD_retry_non_streaming.md)
- [Finalize Supervisor](PRD_finalize_supervisor.md)
- [Session Auto Cleanup](PRD_auto_cleanup.md)

## TUI 与输入

- [附件与 `@路径` 输入](PRD_at_path.md)
- [`!` Shell Command](PRD_shell_command.md)
- [输入框导航、斜杠菜单与行内补全](PRD_tui_composer_navigation_and_completion.md)
- [File Diff 展示](PRD_file_diff_display.md)

## Tool 与后台执行

- [Claim Harness：发现、核对与修订](PRD_claim_harness.md)
- [DeepSWE Claim Harness：可发现的知识与可靠的执行反馈](PRD_deepswe_claim_harness.md)
- [并发工具调用](PRD_parallel_tools.md)
- [`code_run` 后台长命令与受管终端](PRD_background_shell.md)
- [Session Search](PRD_session_search.md)
- [Skill 显式调用与正文注入](PRD_skill_injection.md)

## MCP

- [按 upstream 隔离的自定义 MCP Server](PRD_support_mcp.md)
- [MCP 常驻连接复用与跨 Agent 并发](PRD_shared_mcp.md)

## Subagents

- [Session Delegation 子代理](PRD_subagents.md)
- [`wait_subagents` 子代理等待](PRD_wait_subagents.md)

## 团队服务

- [Upstream 鉴权与目录隔离](PRD_team_auth_design.md)
- [Inbox Delivery](PRD_inbox_delivery_refactor.md)
