# ACN 前端

ACN 前端由两个静态页面和一个 React 管理台组成，统一由 Maintainer HTTP server 发布。这里保存可部署页面的源码、前端工程和面向贡献者的设计约束；产品、架构与配置文档仍放在仓库根目录的 `docs/`。

## 页面与路由

| 页面 | 源码 | 运行时路由 | 职责 |
| --- | --- | --- | --- |
| Landing | `frontend/static/acn_landing.html` | `/` | 介绍 ACN Claim 来源链、角色边界与管理台入口 |
| 角色与交互说明 | `frontend/static/acn_roles_interaction.html` | `/docs/acn_roles_interaction.html` | 解释 Agent、Router、Maintainer、对象语义和协作流程 |
| Maintainer Workbench | `maintainer-workbench/src/` | `/app` | 处理 dispute、policy、sweep、agent、team key 与运行诊断 |

静态页面使用原生 HTML、CSS 和 JavaScript，内容在无 JavaScript 及打印场景下仍可阅读；Workbench 是以 `/app` 为 basename 的 React SPA，需要 JavaScript。表中的 `/docs/` 是 Maintainer 部署后的 HTTP 路由，不对应仓库根目录的`docs/`；两份静态页面的源码统一保存在 `frontend/static/`。

## 构建与发布

前端构建要求 Node.js 22.22.0 或更高版本，从 `frontend/maintainer-workbench/` 执行：

```bash
cd frontend/maintainer-workbench
npm ci
npm run build
```

Vite 先构建 Workbench，随后 `scripts/sync-static.sh` 将产物组装为 Maintainer server 使用的目录：

```text
dist/
  acn_landing.html
  app.html
  index.html
  assets/ fonts/
  docs/acn_roles_interaction.html
```

`dist/` 是忽略跟踪的构建产物，不直接修改。Maintainer 从`[maintainer.ui].frontend_dist_dir` 读取该目录；源码运行的默认值是`./frontend/maintainer-workbench/dist`。该默认目录不存在时，发布版 daemon 会回退到安装前缀下的`share/acn/maintainer-workbench`；自定义路径不回退。HTTP 路由和鉴权由 Rust 服务端负责，Workbench 的 API 请求使用同源相对路径。

### GitHub Pages 公开演示

仓库也提供独立的 Pages 构建，用于公开展示 Landing、角色说明和只读 Workbench：

```bash
cd frontend/maintainer-workbench
npm ci
ACN_PAGES_BASE=/agent-claim-network/ npm run build:pages
```

产物写入 `dist-pages/`。该构建使用完全合成的数据；其中业务 ID、枚举值和响应结构遵循正式 Maintainer / Router 接口契约。页面使用 Hash Router 和项目路径前缀，不检查管理员登录，也不会向 Maintainer、Router 或其他团队服务发起请求。治理写操作在界面中禁用，Router Query 只在浏览器内查询演示数据。正式的 `npm run build` 不受影响，仍用于 Maintainer 同源部署。

`.github/workflows/pages.yml` 会在 `main` 分支的前端内容变化后自动检查、构建并部署 `dist-pages/`。

Landing 正文使用系统字体；ID、时间和代码使用的 JetBrains Mono 以 WOFF2 保存在`frontend/static/fonts/`，构建后由 `/assets/fonts/` 同源提供。字体来源和 OFL 许可证见该目录的 `README.md`。

具体开发命令和目录说明见[`maintainer-workbench/README.md`](maintainer-workbench/README.md)。

## 产品边界

- Agent 是与用户交互并持有本地判断的主体。
- Router 发现相关 Claim 和 Dispute，不替 Agent 作出判断。
- Maintainer 发布 Policy、组织复审、投递建议并维护团队镜像，不能直接改写Agent 私有 Memory 或本地 Claim。
- Workbench 优先呈现需要管理员判断的事项，再展示健康度和历史统计。
- 空数据不等于异常；没有足够分母时使用 `Unknown` 或`insufficient signal`，不把缺少样本误报为 degraded。

## 设计约束

三个界面共享 ACN 标志、角色语义、来源节点与连线语法，但根据任务采用不同的信息密度：

- Landing 使用深色叙事界面建立产品认知，视觉效果必须服务 Claim 来源路径。
- 角色说明页使用浅色、可探索的说明界面，复杂内容应保持可扫描和可打印。
- Workbench 使用暖中性的紧凑运维界面，内容优先于装饰，操作优先级必须清晰。

角色颜色只用于对应对象或小面积状态提示，不能成为唯一的信息编码：

- Agent：green
- Router：blue
- Maintainer：coral
- Dispute：violet

ID、时间、scope、方法名和数值使用等宽字体；叙述内容使用系统无衬线字体。来源路径中的节点表示 holder 与对象，连线表示发现和流转，分叉表示 dispute。

Workbench 的只读详情使用不遮断列表上下文的 Inspector；需要提交变更的流程才使用模态界面。桌面端表格保持紧凑，窄屏下改为带字段名的对象行。平面内容使用边框或层级背景，阴影主要保留给 Drawer、Dialog 等浮层。

## 可访问性

前端以 WCAG 2.2 AA 为持续维护目标，而不是已经完成的外部认证。修改页面时应保持以下约束：

- 正文和大字号文本满足相应对比度要求，状态不能只依赖颜色区分。
- 链接、按钮、表格行、Tab、Drawer 和移动导航可通过键盘操作，并具有可见焦点。
- 关闭浮层后将焦点归还触发元素；模态界面管理焦点范围和背景交互。
- `prefers-reduced-motion` 下移除装饰动画，但保留状态变化和内容。
- 动效不能决定内容是否存在；静态页面应支持无 JavaScript 阅读和打印。

## 修改检查

修改静态页面、Workbench 或构建脚本后，至少运行：

```bash
cd frontend/maintainer-workbench
npm run lint
npm run test
npm run build
ACN_PAGES_BASE=/agent-claim-network/ npm run build:pages
```

构建完成后应确认 Landing、角色说明页、`/app` SPA 入口及 `/app/assets/` 均存在，并从仓库根目录启动 Maintainer 做同源页面与 API 验收。Pages 构建还应确认 `dist-pages/app/index.html` 使用 Hash Router、页面明确标注合成数据，并且浏览器没有发出 `/api` 请求。
