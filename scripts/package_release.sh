#!/usr/bin/env bash
# 把指定目标的三个 ACN release binary 与生产 Workbench 组装为发布归档。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
用法:
  scripts/package_release.sh <target> [output-dir]

支持的 target:
  aarch64-apple-darwin
  x86_64-apple-darwin
  x86_64-unknown-linux-gnu

环境变量:
  ACN_RELEASE_BIN_DIR       binary 目录；默认 target/<target>/release
  ACN_RELEASE_FRONTEND_DIR  Workbench dist；默认 frontend/maintainer-workbench/dist
EOF
}

fail() {
  echo "[release-package] $*" >&2
  exit 1
}

TARGET="${1:-}"
OUTPUT_DIR="${2:-target/release-packages}"
if [[ -z "$TARGET" || "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  usage
  [[ -n "$TARGET" ]] && exit 0
  exit 1
fi
[[ $# -le 2 ]] || fail "参数过多"

case "$TARGET" in
  aarch64-apple-darwin|x86_64-apple-darwin|x86_64-unknown-linux-gnu) ;;
  *) fail "不支持的 target: $TARGET" ;;
esac

PACKAGE_ID="$(cargo pkgid)"
PACKAGE_VERSION="${PACKAGE_ID##*#}"
PACKAGE_VERSION="${PACKAGE_VERSION##*@}"
[[ "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]] \
  || fail "无法从 cargo pkgid 解析产品版本: $PACKAGE_ID"

BIN_DIR="${ACN_RELEASE_BIN_DIR:-target/$TARGET/release}"
FRONTEND_DIR="${ACN_RELEASE_FRONTEND_DIR:-frontend/maintainer-workbench/dist}"
for binary in acn acn-router acn-maintainer; do
  [[ -f "$BIN_DIR/$binary" ]] || fail "缺少 binary: $BIN_DIR/$binary"
  [[ -x "$BIN_DIR/$binary" ]] || fail "binary 不可执行: $BIN_DIR/$binary"
done
for required in app.html acn_landing.html favicon.svg assets docs; do
  [[ -e "$FRONTEND_DIR/$required" ]] \
    || fail "Workbench dist 缺少: $FRONTEND_DIR/$required；请先运行 npm run build"
done

ARCHIVE_STEM="agent-claim-network-v${PACKAGE_VERSION}-${TARGET}"
STAGING_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/acn-release-package.XXXXXX")"
trap 'rm -rf "$STAGING_ROOT"' EXIT
PACKAGE_ROOT="$STAGING_ROOT/$ARCHIVE_STEM"

mkdir -p \
  "$PACKAGE_ROOT/bin" \
  "$PACKAGE_ROOT/share/acn/maintainer-workbench" \
  "$OUTPUT_DIR"
for binary in acn acn-router acn-maintainer; do
  cp "$BIN_DIR/$binary" "$PACKAGE_ROOT/bin/$binary"
  chmod 0755 "$PACKAGE_ROOT/bin/$binary"
done
cp -R "$FRONTEND_DIR/." "$PACKAGE_ROOT/share/acn/maintainer-workbench/"
cp README.md README_EN.md LICENSE-APACHE LICENSE-MIT "$PACKAGE_ROOT/"

ARCHIVE_PATH="$OUTPUT_DIR/$ARCHIVE_STEM.tar.gz"
COPYFILE_DISABLE=1 tar -C "$STAGING_ROOT" -czf "$ARCHIVE_PATH" "$ARCHIVE_STEM"

if command -v sha256sum >/dev/null 2>&1; then
  (
    cd "$OUTPUT_DIR"
    sha256sum "$(basename "$ARCHIVE_PATH")" >"$(basename "$ARCHIVE_PATH").sha256"
  )
elif command -v shasum >/dev/null 2>&1; then
  (
    cd "$OUTPUT_DIR"
    shasum -a 256 "$(basename "$ARCHIVE_PATH")" >"$(basename "$ARCHIVE_PATH").sha256"
  )
else
  fail "系统缺少 sha256sum 或 shasum"
fi

tar -tzf "$ARCHIVE_PATH" >/dev/null
echo "[release-package] archive: $ARCHIVE_PATH"
echo "[release-package] checksum: $ARCHIVE_PATH.sha256"
