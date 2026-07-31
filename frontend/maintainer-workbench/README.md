# Maintainer Workbench

Maintainer Workbench 是 ACN 团队服务的管理界面。它与 Maintainer HTTP server同源部署，通过相对路径访问管理 API，并以 `/app` 作为 SPA basename。

Workbench 面向 Maintainer 管理员，提供以下区域：

- Overview：待处理事项、网络状态和近期活动
- Claims / Agents：团队 Claim 镜像与 Agent 活动
- Disputes / Policies：争议复审和治理消息
- Sweep：Claim aging 检查和扫描历史
- Router Query：团队检索及召回诊断
- Team Auth：Agent 团队访问 key
- HTTP Audits / Settings：请求审计、运行状态和 endpoint 目录

## 环境

- Node.js `^20.19.0` 或 `>=22.12.0`
- npm；依赖版本由 `package-lock.json` 锁定

## 安装

```bash
cd frontend/maintainer-workbench
npm ci
```

## 开发命令

```bash
npm run dev      # 启动 Vite 开发服务器
npm run lint     # ESLint
npm run test     # Vitest
npm run build    # TypeScript 检查、Vite 构建并组装全部静态页面
npm run build:pages # 构建 GitHub Pages 公开静态演示
npm run preview  # 预览最近一次 Vite 构建
```

`npm run dev` 只启动前端开发服务器。Workbench 的 API 使用同源相对路径，而当前Vite 配置没有代理 Maintainer API，因此涉及真实数据的完整验收应使用构建产物和Rust Maintainer server。

## 完整运行

先构建前端：

```bash
cd frontend/maintainer-workbench
npm run build
```

再从仓库根目录启动 Maintainer：

```bash
cargo run --bin acn-maintainer -- --config /path/to/config.toml
```

访问 `http://<maintainer-listen>/` 查看 Landing，或打开`http://<maintainer-listen>/app` 进入 Workbench。监听地址、前端产物目录和管理员鉴权分别由 `[maintainer.daemon]`、`[maintainer.ui]` 和`[maintainer.auth.admin]` 配置。

GitHub Release 使用 `npm run build` 产物，并把它安装到`share/acn/maintainer-workbench`。只有默认源码目录不存在时才使用该随包目录；显式配置的自定义目录始终优先。

## 构建结构

Vite 以 `/app/` 为资源 base，先把 React SPA 输出到 `dist/`。`postbuild` 随后运行 `scripts/sync-static.sh`：

1. 将 SPA shell 从 `dist/index.html` 改名为 `dist/app.html`。
2. 从 `../static/` 复制 Landing 和角色说明页。
3. 为本地静态服务器额外保留一份 Landing `dist/index.html`。

`dist/` 是构建产物并已被忽略，不应手工修改。

## GitHub Pages 构建

公开演示使用单独的构建目标，不复用真实 Maintainer API：

```bash
ACN_PAGES_BASE=/ npm run build:pages
python3 -m http.server 4173 --directory dist-pages
```

然后访问 `http://127.0.0.1:4173/`。GitHub Actions 会在正式部署时把 `ACN_PAGES_BASE` 自动设为当前仓库名对应的项目路径前缀。

Pages 模式具有明确边界：

- 数据全部在 `src/lib/demoData.ts` 中合成；业务 ID、枚举值和响应字段遵循 Rust 服务端的正式接口契约。
- 不执行管理员鉴权，也不向任何 `/api`、Maintainer 或 Router endpoint 发请求。
- Workbench 使用 Hash Router，项目型 Pages 上刷新详情页面不会触发服务端路由。
- 改写类操作保持可识别但不可提交；Router Query 在浏览器内返回合成检索结果。
- 输出目录是 `dist-pages/`；正式 Maintainer 仍使用 `dist/`，两者互不覆盖。

GitHub 的自动部署配置位于仓库根目录 `.github/workflows/pages.yml`。首次部署前，在项目 `Settings → Pages` 中选择 `GitHub Actions` 作为发布来源。

## 源码布局

```text
src/
  app/          路由、全局 provider 和 UI 状态
  components/   表格、筛选、状态和详情面板
  features/     各 Maintainer 领域的 API、类型、hooks 与派生逻辑
  layouts/      顶栏、侧栏和页面框架
  lib/          API client、格式化与共享常量
  pages/        路由页面
  test/         Vitest 公共设置
```

前端整体页面关系、设计边界和可访问性约束见[`../README.md`](../README.md)；Maintainer 配置字段见[`../../docs/config_parameters.md`](../../docs/config_parameters.md)。
