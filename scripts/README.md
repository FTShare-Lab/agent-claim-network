# ACN 维护脚本

本目录保存仓库级、需要显式执行的维护或端到端验收脚本。确定性的 Rust 测试放在`tests/` 或对应模块中；TUI 的局部回归脚本放在`.agents/skills/tui-smoke-test-with-tmux/scripts/`。

## 版本一致性检查

`check_version_consistency.sh` 将 `Cargo.toml` 的 package version 视为 ACN 产品版本的唯一来源，并检查：

- `Cargo.lock` 与 manifest 是否同步。
- README 版本徽章与角色说明页展示的版本是否和 Cargo 一致。
- 其他持续更新的项目文档和静态页面是否误写产品版本字面量。
- 私有 Maintainer Workbench 包是否保持非产品版本 `0.0.0`，且 lockfile 根包元数据一致。
- `ACN_RELEASE_TAG`、GitLab `CI_COMMIT_TAG`、GitHub tag ref 或当前提交上的语义化版本 tag 是否与 Cargo 版本一致。

从仓库根目录执行：

```bash
scripts/check_version_consistency.sh
```

GitLab CI 与 GitHub Actions 会在 push、Merge/Pull Request 与 tag pipeline 中自动执行同一脚本；本地 commit 不会触发远端 CI。

## 真实 LLM 团队模式验收

`tui_real_llm_smoke.sh` 使用真实 LLM，同时启动 Router、Maintainer 和 ACN TUI，覆盖以下主流程：

- 新建并关闭 Session
- `file_read`、`working_note` 与 `consult_router`
- 手动 `/compact`
- `/resume` 后继续对话
- Session 消息、压缩状态和关闭状态落盘

运行前需要：

- Rust 与 Cargo
- `tmux`、`rg`、`lsof`、`nc`、`perl`
- 一份可用的 ACN 配置，包含 LLM、upstream、Router daemon 和 Maintainer daemon 配置段
- 配置中 `api_key_env` 对应的环境变量

脚本会在存在 `export_env.sh` 时加载它；没有该文件时直接使用调用者已经导出的环境变量。不要把真实密钥写进配置或提交到仓库。

从仓库根目录执行：

```bash
scripts/tui_real_llm_smoke.sh manual config.toml
```

第一个参数是本次运行标签，只接受字母、数字、下划线和连字符；第二个参数是源配置文件。脚本只修改复制到 `target/tui-real-smoke/<label>/` 的配置，并为Router 与 Maintainer 分配空闲本地端口，不修改源配置。

测试会产生真实 API 请求，可能消耗额度并受外部服务稳定性影响，因此它不是默认`cargo test` 或普通 CI 的组成部分。屏幕捕获、日志、最终 Session 元数据和消息保存在对应的 `target/tui-real-smoke/<label>/` 目录，便于失败后检查。

## MCP OAuth 本地验收

`examples/mcp_oauth_fake_server.rs` 是只监听 loopback 的 Rust fixture，不依赖真实 OAuth
或 MCP 服务。一个进程同时提供受 OAuth 保护的 MCP 2026-07-28 endpoint、匿名新协议
endpoint 和拒绝 `server/discover` 的 2025-11-25 兼容 endpoint：

```bash
cargo run --quiet --example mcp_oauth_fake_server -- \
  --port 8765 \
  --log-file target/mcp-oauth-requests.jsonl
```

默认 access token 只有 5 秒有效期，便于通过请求日志观察 refresh token 流程；
`--omit-pkce`、`--insecure-metadata` 和 `--mismatched-callback-issuer` 分别用于验证
缺少 PKCE S256、OAuth endpoint 使用远端明文 HTTP、callback `iss` 不匹配时客户端会
fail-closed。fixture 会打印用于 bearer 优先级测试的固定测试 token；它不接受非
loopback 监听地址。

完整的本地端到端验收可直接运行：

```bash
scripts/mcp_oauth_smoke.sh
```

脚本要求 `cargo`、`curl`、`expect`、`jq`、`rg` 与 `tmux`。它会在
`target/mcp-oauth-smoke.*` 下生成隔离的双 upstream 配置和验收轨迹，自动覆盖新旧协议
协商、bearer 优先级、桌面与 headless OAuth、DCR 与预注册 client、refresh、凭据隔离、
logout/remove、PKCE/HTTP/issuer fail-closed，以及 canonical 和 `/mcp` focused TUI smoke。

## Release 归档

`package_release.sh` 把指定目标的 `acn`、`acn-router`、`acn-maintainer`、生产 Workbench、README 与双许可证组装成独立归档，并生成 SHA-256：

```bash
scripts/package_release.sh aarch64-apple-darwin
scripts/package_release.sh x86_64-apple-darwin
scripts/package_release.sh x86_64-unknown-linux-gnu
```

默认从`target/<target>/release`读取 binary，从`frontend/maintainer-workbench/dist`读取`npm run build`产物，输出到`target/release-packages`。完整发布流程见[发布与分发](../docs/release.md)。
