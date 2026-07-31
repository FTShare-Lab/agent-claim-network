#!/usr/bin/env bash
# 校验 ACN 产品版本只由 Cargo.toml 定义，其余展示与发布元数据不发生漂移。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

fail() {
  echo "[version-check] $*" >&2
  exit 1
}

# --locked 会在 Cargo.toml 与 Cargo.lock 根包版本不一致时直接失败，避免检查过程
# 自己更新 lockfile 后掩盖漂移。
cargo metadata --locked --no-deps --format-version 1 >/dev/null

PACKAGE_ID="$(cargo pkgid)"
# 当 checkout 目录名与 package 名一致时，Cargo 会把
# `#package@version` 缩写成 `#version`，两种合法形式都要兼容。
PACKAGE_VERSION="${PACKAGE_ID##*#}"
PACKAGE_VERSION="${PACKAGE_VERSION##*@}"
if [[ "$PACKAGE_VERSION" == "$PACKAGE_ID" ]] \
  || [[ ! "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  fail "无法从 cargo pkgid 解析产品版本: $PACKAGE_ID"
fi

VERSION_FREE_FILES=(
  AGENTS.md
  docs/architecture.md
  docs/core_behavior.md
  docs/PRDs/README.md
  frontend/static/acn_landing.html
)
if grep -En \
  '(^|[^[:alnum:]_.-])v?[0-9]+\.[0-9]+\.[0-9]+([^[:alnum:]_.-]|$)' \
  "${VERSION_FREE_FILES[@]}"; then
  fail "持续更新的文档或静态页面不应硬编码产品版本"
fi

README_FILES=(
  README.md
  README_EN.md
)
README_VERSION_LINE="  <img alt=\"version $PACKAGE_VERSION\" src=\"https://img.shields.io/badge/version-$PACKAGE_VERSION-brightgreen.svg\">"
for readme in "${README_FILES[@]}"; do
  if ! grep -Fqx "$README_VERSION_LINE" "$readme"; then
    fail "$readme 版本徽章与 Cargo 版本不一致，应展示 $PACKAGE_VERSION"
  fi
done

ROLES_VERSION_LINE="          <span>角色与知识流转 · v$PACKAGE_VERSION</span>"
if ! grep -Fqx "$ROLES_VERSION_LINE" frontend/static/acn_roles_interaction.html; then
  fail "角色说明页版本与 Cargo 版本不一致，应展示 v$PACKAGE_VERSION"
fi

VERSION_PRESENTATION_FILES=(
  "${README_FILES[@]}"
  frontend/static/acn_roles_interaction.html
)
while IFS= read -r displayed_version; do
  [[ -z "$displayed_version" ]] && continue
  displayed_version="${displayed_version#v}"
  if [[ "$displayed_version" != "$PACKAGE_VERSION" ]]; then
    fail "中英文 README 或角色说明页存在漂移版本 $displayed_version，Cargo 版本为 $PACKAGE_VERSION"
  fi
done < <(
  grep -Eho \
    '(^|[^[:alnum:]_.-])v?[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?([^[:alnum:]_.-]|$)' \
    "${VERSION_PRESENTATION_FILES[@]}" \
    | grep -Eo 'v?[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?'
)

FRONTEND_MANIFEST="frontend/maintainer-workbench/package.json"
FRONTEND_LOCK="frontend/maintainer-workbench/package-lock.json"
FRONTEND_VERSION="$(
  awk '
    /^  "version": "/ {
      value = $0
      sub(/^[^"]*"version": "/, "", value)
      sub(/".*$/, "", value)
      print value
      exit
    }
  ' "$FRONTEND_MANIFEST"
)"
[[ "$FRONTEND_VERSION" == "0.0.0" ]] \
  || fail "私有 Maintainer Workbench 包版本应为 0.0.0，当前为 ${FRONTEND_VERSION:-<missing>}"

if ! awk -v expected="$FRONTEND_VERSION" '
    /^[[:space:]]*"version": "/ {
      value = $0
      sub(/^[^"]*"version": "/, "", value)
      sub(/".*$/, "", value)
      count += 1
      if (count <= 2 && value != expected) {
        exit 1
      }
    }
    END {
      if (count < 2) {
        exit 1
      }
    }
  ' "$FRONTEND_LOCK"; then
  fail "Maintainer Workbench package-lock.json 根包版本与 package.json 不一致"
fi

EXPECTED_TAG="v$PACKAGE_VERSION"
RELEASE_TAG="${ACN_RELEASE_TAG:-${CI_COMMIT_TAG:-}}"
if [[ -z "$RELEASE_TAG" && "${GITHUB_REF:-}" == refs/tags/* ]]; then
  RELEASE_TAG="${GITHUB_REF#refs/tags/}"
fi
if [[ -n "$RELEASE_TAG" && "$RELEASE_TAG" != "$EXPECTED_TAG" ]]; then
  fail "发布 tag $RELEASE_TAG 与 Cargo 版本 $PACKAGE_VERSION 不一致，应为 $EXPECTED_TAG"
fi

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  while IFS= read -r tag; do
    [[ -z "$tag" ]] && continue
    if [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]] \
      && [[ "$tag" != "$EXPECTED_TAG" ]]; then
      fail "当前提交 tag $tag 与 Cargo 版本 $PACKAGE_VERSION 不一致，应为 $EXPECTED_TAG"
    fi
  done < <(git tag --points-at HEAD)
fi

echo "[version-check] ok: ACN $PACKAGE_VERSION"
