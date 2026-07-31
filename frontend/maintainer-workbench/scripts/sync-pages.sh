#!/usr/bin/env bash
# postbuild: 组装 GitHub Pages 使用的纯静态公开演示站点。
#
# vite --mode pages 产出:
#   dist-pages/app/index.html
#   dist-pages/app/assets/
#
# 本脚本补齐:
#   dist-pages/index.html
#   dist-pages/docs/acn_roles_interaction.html
#   dist-pages/assets/fonts/
#   dist-pages/favicon.svg
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PAGES_DIST="dist-pages"

if [ ! -f "$PAGES_DIST/app/index.html" ]; then
  echo "[sync-pages] 缺少 Workbench 入口（$PAGES_DIST/app/index.html）。请先执行 npm run build:pages。" >&2
  exit 1
fi

cp "$REPO_ROOT/frontend/static/acn_landing.html" "$PAGES_DIST/index.html"

mkdir -p "$PAGES_DIST/docs"
cp "$REPO_ROOT/frontend/static/acn_roles_interaction.html" "$PAGES_DIST/docs/acn_roles_interaction.html"

rm -rf "$PAGES_DIST/assets/fonts"
mkdir -p "$PAGES_DIST/assets/fonts"
cp "$REPO_ROOT/frontend/static/fonts/"* "$PAGES_DIST/assets/fonts/"

cp public/favicon.svg "$PAGES_DIST/favicon.svg"
touch "$PAGES_DIST/.nojekyll"

echo "[sync-pages] dist-pages 已组装: Landing + Workbench 静态演示 + 角色说明 + 自托管字体"
