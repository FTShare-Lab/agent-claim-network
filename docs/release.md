# ACN 发布与分发

ACN 通过 GitHub Release 提供三个原生平台归档；Homebrew tap 从这些不可变归档安装三个 binary 与 Maintainer Workbench。产品版本以`Cargo.toml`为唯一来源，发布 tag 使用`v<version>`。

## 发布目标

| Rust target | GitHub runner | 兼容口径 |
| --- | --- | --- |
| `aarch64-apple-darwin` | `macos-15` | Apple Silicon，最低 macOS 11 |
| `x86_64-apple-darwin` | `macos-15-intel` | Intel Mac，最低 macOS 11 |
| `x86_64-unknown-linux-gnu` | `ubuntu-22.04` | x86_64 GNU/Linux，以 Ubuntu 22.04 为首版构建基线 |

三个目标都在对应架构的原生 runner 上完成测试和 release 构建，不从开发者本机交叉编译。Windows 不在支持范围内。

ACN 的 HTTP 客户端使用 rustls，并从操作系统原生证书存储加载信任根。Linux Release
不依赖特定版本的`libssl`或`libcrypto`；通过`SSL_CERT_FILE`或`SSL_CERT_DIR`
配置的自定义 CA 仍会被读取。Release workflow 会检查三个 Linux binary 的动态依赖，
避免系统 OpenSSL 被意外重新引入。

## 归档契约

每个平台生成一个`agent-claim-network-v<version>-<target>.tar.gz`及同名`.sha256`。归档根目录与文件名 stem 相同，内部结构固定为：

```text
bin/
  acn
  acn-router
  acn-maintainer
share/acn/maintainer-workbench/
  app.html
  acn_landing.html
  assets/
  docs/
README.md
README_EN.md
LICENSE-APACHE
LICENSE-MIT
```

Workbench 只使用`npm run build`的生产产物，不使用 GitHub Pages 的演示模式。三份归档各自携带同一份 Workbench，使单个归档可独立安装。

## GitHub Actions

`.github/workflows/release.yml`支持两种入口：

- `workflow_dispatch`：构建、测试并保留三个 workflow artifacts，不创建 GitHub Release，适合发布前演练。
- 推送语义化版本 tag：完成相同验证后创建 Draft Release、上传归档与`SHA256SUMS`，全部成功后发布。

Release workflow 会执行版本一致性检查、前端 lint/test/build、三个原生目标的 Rust 测试与构建、binary `--version`/`--help`检查、归档完整性和 SHA-256 校验。已经发布的 Release 不允许自动覆盖；失败后只允许继续写入尚未发布的 Draft。

本地只需要验收当前平台。例如 Apple Silicon：

```bash
cd frontend/maintainer-workbench
npm ci
npm run build
cd ../..

cargo build --release --locked --bins --target aarch64-apple-darwin
scripts/package_release.sh aarch64-apple-darwin
```

## Homebrew

公开 tap 使用独立仓库`FTShare-Lab/homebrew-tap`，Formula 名为`acn`。Formula 根据操作系统和 CPU 选择对应 Release 归档，将`bin/*`安装到 Homebrew bin，将 Workbench 安装到`pkgshare/maintainer-workbench`。

`brew upgrade acn`只负责替换安装文件。新版`acn`首次需要 finalize supervisor 时会比较 supervisor 返回的产品版本与构建提交；不一致或旧协议实例经过 IPC/PID 校验后被终止，并由新版恢复持久化队列。

首版 Formula 在 Release 三个平台归档全部发布后，根据各归档 SHA-256 写入`FTShare-Lab/homebrew-tap`。后续版本暂时沿用相同步骤更新；如需自动化，再为源码仓库配置仅能写 tap 的 GitHub App 或细粒度 token，不复用个人全局凭据。
