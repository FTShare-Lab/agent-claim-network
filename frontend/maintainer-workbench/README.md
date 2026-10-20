# Maintainer Workbench

Maintainer Workbench 是 ACN 团队服务的管理界面。它与 Maintainer HTTP server同源部署，通过相对路径访问管理 API，并以 `/app` 作为 SPA basename。

Workbench 面向 Maintainer 管理员，提供以下区域：

- Overview：待处理事项、网络状态和近期活动
- Claims Flow：按来源与影响方向浏览团队知识树
- Claims / Agents：团队 Claim 镜像与 Agent 活动
- Disputes / Policies：争议复审和治理消息
- Sweep：Claim aging 检查和扫描历史
- Router Query：团队检索及召回诊断
- Team Auth：Agent 团队访问 key
- HTTP Audits / Settings：请求审计、运行状态和 endpoint 目录

## 界面语言

登录页和工作台右上角可即时切换中文 / English，首次访问默认中文。同一浏览器记住最后选择，刷新、重新打开、退出登录后均保留；同源标签也会同步切换。存储被禁用时仍可在当前页面切换。

翻译覆盖界面标签、按钮、说明与提示语，Agent、Claim、Policy、Trace、Dispute、Maintainer、Router 保留英文。Claim / Policy / Dispute / Agent 的状态按当前语言显示，原始枚举值不变；名称、正文、证据、适用范围、ID 及其他业务字段内容保持原样；切换不提交业务请求，也不清空当前表单、筛选或详情。语言偏好独立保存在浏览器 localStorage 中，不写入团队数据或服务端配置。

## 管理员登录与验收

启用管理员鉴权后，Workbench 使用服务端签发的 HttpOnly Cookie，默认有效期为 7 天。有效期内关闭标签、重开或重启浏览器均可恢复登录；退出会撤销会话并同步其他同源标签。前端不保存管理员密码。配置与服务重启失效边界见 [配置说明](../../docs/config_parameters.md#maintainerauthadmin)。

在仓库根目录运行以下命令。需已安装 `agent-browser` 及其浏览器运行时；验收会创建临时服务、独立 profile 和测试密码，结束后清理，不读取真实团队配置。

```bash
if [[ -f export_env.sh ]]; then source export_env.sh; fi
cargo build --bins --examples
npm --prefix frontend/maintainer-workbench run build
target/debug/examples/auth_cookie_smoke
target/debug/examples/auth_cookie_smoke --localhost
```

默认通过指向本机的保留域名测试真正的普通 HTTP（确认无 Web Locks 和 randomUUID），并验证跨标签的 IndexedDB 协调；`--localhost` 复验安全上下文路径。脚本覆盖 HttpOnly / 无密码存储、新标签共享、标签关闭重开、浏览器进程重启、仅凭 Cookie 恢复、退出同步、服务重启失效、空闲过期和重新登录。迟到 401 与并发登录/注销顺序由前端会话测试覆盖，服务端撤销和 Agent 团队鉴权隔离由 Rust 路由测试覆盖。

## 知识树

侧栏 **Claim Flow**（`/app/knowledge-tree`）默认展示全部 Claim 的关系总览。每条 Claim 只画一次，保留所选 Claim 之间的全部有向来源关系，包括循环和自引用。总览还补充所选 Claim 直接引用的 Policy Update（PU）和 Claim Attribute Update（CAU），同一来源只画一次，保留其与所选 Claim 的全部连接；缺失来源不纳入总览。

总览会依据连接关系比较横向、纵向的多个分层布局，综合减少交叉、重叠和不必要的折线；长链可自动折行，独立连通分量分开排布，连线绕开节点。复杂关系不保证完全无交叉。鼠标悬停或键盘聚焦节点时突出它的直接来源与引申关系。布局只影响显示，不修改 Claim 数据。

超过 30 条时，总览最多显示 30 条：忽略方向划分连通分量，优先较大的分量，从连接数较多的节点起沿相邻关系扩展；同等情况下按 ID 稳定排序。这样优先保持连通，不承诺求得全局最大密度子图。关联 PU/CAU 合计最多 20 条，优先被当前所选 Claim 引用较多的，其次按最近更新时间（无更新时间时使用创建时间）降序、ID 稳定排序，总节点数最多 50 个。页面分别提示 Claim 和关联 PU/CAU 的显示数与总数，超限时说明截取规则；单条 Claim 视图仍可追溯其余来源。页面提示显示数、总数和截取规则，其余 Claim 仍可从列表检索。筛选只作用于左侧候选列表，总览始终基于全部 Claim。总览节点可打开详情，再从该节点展开来源或影响；再次点击左侧当前选中的根节点可返回总览，保留列表筛选与分页。

选择根节点后进入团队知识树：

1. 使用与 Claims 相同的标题、ID、正文、证据、scope、Agent、状态和争议筛选，再从结果中选择根节点。也可按知识类型选择 Policy 或属性更新建议。Agent 和争议筛选只匹配 Claim。
2. 右上角切换 **Sources** 或 **Impact**：前驱递归沿 `source_claim_ids` 向上展示来源；后继反向查找引用该 ID 的 Claim，向下展示影响链。筛选只影响根节点候选列表，不裁剪已选节点的关系树。
3. Claim 为绿色、Policy Update 为橙色、Claim Attribute Update 为紫色；图例色块与节点使用相同底色和边框。根节点为 Claim 时，holder 不同的 Claim 使用可清楚分辨线段与空档的绿色虚线边框，Policy Update、Claim Attribute Update 和同一 holder 的 Claim 保持实线，图例会标明该含义。节点显示标题，悬停可查看类型与 ID。卡片内部右上角用紧凑的 14px 图标表示 Active（绿色 CircleCheck）、Stale（琥珀色 Clock3）或 Deprecated（灰色 CircleSlash）；存在未解决争议的 Claim 额外显示玫红色 MessageCircleWarning。状态图标提供悬停说明和无障碍标签，画布上方有对应英文图例，缺失来源不显示推测状态。连线箭头始终从来源指向派生知识；展开按钮避开箭头末端。点击节点在右侧打开详情，可继续查看来源、返回上一节点，或以当前节点重新展开。

默认展示根节点与第一层关系，每个分支先显示最多 4 个直接关联节点。使用节点旁的 **+** 展开、**−** 折叠；数量按钮表示尚未显示的直接分支，每次再显示最多 4 个。同一知识经不同路径到达时会重复显示，各处的展开状态相互独立。点击标题仍打开详情。

**Expand all** 可展开完整关系，超过 500 个显示节点时用 **Show more nodes** 分批继续；**Collapse all** 收回到根节点。路径内遇到循环引用时，保留重复节点并标明停止展开；缺失来源以灰色占位节点保留其引用关系。鼠标左键拖拽画布空白处可平移，节点和分支按钮保留点击操作，触屏保留原生滚动。缩放围绕当前画布视口中心；**Fit** 查看整体后仍保留四周平移空间，在画布空白处双击可围绕该点放大，**Center root** 返回根节点。连续展开时，新一层节点及其分支按钮会一起进入可视区域。

底部同时显示 **displayed nodes**（当前可见节点数，含重复路径）与 **unique items**（当前可见节点按知识 ID 去重后的数量，包含缺失来源占位）。折叠的知识不计入这两个数，同名但不同 ID 的知识分别计数。

根节点和方向保存在 URL 的 `root_id`、`direction` 参数中，支持刷新和浏览器前进后退。Claims 详情底部的 **View claims flow** 可直接打开该 Claim 的来源树。数据复用现有 Claims 和 Policies 接口；Policy 当前没有来源字段，因此其前驱视图仅包含自身。

### 本地复杂树验收

复杂案例只用于本地验收和自动化测试，位于 `src/test/knowledgePreview.ts`，包含 630 条 Claim、6 条 Policy 和 3 条 Dispute。未解决争议分别关联 active、stale、deprecated Claim，已解决争议用于验证历史争议不会显示 Disputed 图标。GitHub Pages 继续使用 `src/lib/demoData.ts` 原有的 6 条 Claim 和 3 条 Policy，公开构建不包含这些压力测试案例。

在当前目录运行：

```bash
npm run preview:knowledge
# 可选：指定另一个本地端口
npm run preview:knowledge -- 18063
```

命令先构建正式前端，再在仓库 `target/knowledge-preview-*/` 下新建独立数据目录和配置，启动真实 Maintainer（默认 `127.0.0.1:18062`）。终端会打印数据目录和知识树网址；页面通过正式 API 读取测试数据。每次启动使用全新目录，不修改已有团队数据，也不会启用模型仲裁或启动 Router。按 Ctrl-C 停止后，测试目录保留供检查。

案例的知识标题、正文和证据说明使用中文；前端界面文案支持中英文，原始案例内容保持不变。可按下表的 `root_id` 打开 `http://127.0.0.1:18062/app/knowledge-tree`，Impact 模式追加 `direction=successors`。

| 案例 | root_id | 模式 | 完整树节点数 | 验收重点 |
| --- | --- | --- | --- | --- |
| 类型与状态验收 | `claim_c1000fa0` | Sources | 8 | 默认 5 个节点即覆盖全部图例颜色；Expand all 展示三种 Claim 状态各自与争议图标并列的效果 |
| 复杂发布决策 | `claim_c1000014` | Sources | 145 | 23 个不同知识对象、9 层引用、共享证据、历史规则与属性建议 |
| 边界案例 | `claim_c1000064` | Sources | 20 | 自引用、三节点循环、缺失 Claim/Policy、重复来源、同名不同 ID、长标题、文本转义 |
| 独立观测 | `claim_c100006f` | Sources / Impact | 1 | 无来源也无影响的独立节点 |
| 深层链路 64 | `claim_c1000107` | Sources | 65 | 64 层链路一直追溯到 Policy |
| 宽树案例 | `policy_c2000004` | Impact | 513 | 超过 500 节点预算后继续展开，检查最后一个采纳分支 |
| 交叉引用压力案例 | `claim_c1000c1c` | Sources | 1535 | 20 个不同对象形成大量重复路径，按 500、1000、1500、1535 分批显示 |

使用 Scope 筛选 `demo/knowledge/release`、`boundaries`、`isolated`、`deep`、`wide` 、`lattice` 或 `demo/knowledge/statuses` 可定位对应记录。来源缺失和循环仅在边界案例中有意构造；它们不代表服务异常。**Fit** 可缩小到全图预览，点击放大后可查看局部分支和节点详情。

## 环境

- Node.js `>=22.22.0`
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
