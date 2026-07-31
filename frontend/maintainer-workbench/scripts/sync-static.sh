#!/usr/bin/env bash
# postbuild: 把 vite 产出的 dist 组装成 maintainer HTTP server 期望的结构。
# npm 在 frontend/maintainer-workbench/ 目录下运行本脚本（postbuild 钩子）。
#
# vite build 产出:
#   dist/index.html        <- SPA shell (base=/app/)
#   dist/assets/           <- SPA 的 JS/CSS
#
# maintainer server 路由期望:
#   GET /            -> dist/acn_landing.html   (门厅页)
#   GET /app         -> dist/app.html           (SPA shell)
#   GET /assets/*    -> dist/assets/             (门厅页静态资源)
#   GET /app/assets/*-> dist/assets/             (SPA 静态资源)
#   GET /docs/*      -> dist/docs/              (详细说明等静态页)
#
# 因此需要: SPA shell 改名为 app.html，门厅页与说明页从 docs/ 复制进来。
set -euo pipefail

# 脚本位于 frontend/maintainer-workbench/scripts/，仓库根在往上 3 级。
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"

# 1. SPA shell 改名为 app.html（server /app 读它）。
#    Vite 刚产出的 index.html 才是 SPA；步骤 5 会把门厅页再写成 index.html。
#    若再次单独跑本脚本，不能把门厅页 mv 覆盖掉真正的 app.html。
if [ -f dist/index.html ] && grep -Eq 'id="root"|maintainer control plane' dist/index.html; then
  mv dist/index.html dist/app.html
elif [ ! -f dist/app.html ]; then
  echo "[sync-static] 缺少 SPA shell（dist/app.html）。请先在本目录执行 npm run build / vite build。" >&2
  exit 1
fi

# 2. 门厅页作为 / 入口（源文件在仓库 frontend/static/）。
cp "$REPO_ROOT/frontend/static/acn_landing.html" dist/acn_landing.html

# 3. 详细说明页（源文件同样在 frontend/static/）。
#    页面使用相对链接，可同时部署到 Maintainer 服务和项目型 GitHub Pages。
mkdir -p dist/docs
cp "$REPO_ROOT/frontend/static/acn_roles_interaction.html" dist/docs/acn_roles_interaction.html

# 4. 门厅页自托管字体及许可证。
mkdir -p dist/assets/fonts
cp "$REPO_ROOT/frontend/static/fonts/"* dist/assets/fonts/

echo "[sync-static] dist 已组装: app.html (SPA) + acn_landing.html (门厅) + docs/ (说明页) + assets/fonts/ (字体)"

# 5. 额外放一份 landing 到 dist/index.html —— 仅供本地 Python 预览服务器
#    (python -m http.server 的 / 默认读 index.html)。Rust maintainer server
#    的 / 路由直接读 acn_landing.html，不依赖此文件。
cp dist/acn_landing.html dist/index.html
