#!/usr/bin/env bash
# 将已审查的 Pier overlay 应用到干净的 Pier checkout，并校验 diff hash。
set -euo pipefail

readonly expected_pier_revision="0daf53d3599e58c4506cf0bcff5e12c77dc282d2"
readonly expected_pier_overlay_hash="7cb51ffacd2807a76d70c0ae22e051f840c3d499866233b328af676429a8b154"

: "${MINISWE_PIER_ROOT:?MINISWE_PIER_ROOT must be set}"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
patch_path="$repo_root/benchmarks/deepswe/patches/pier-overlay.patch"

if [[ "$(git -C "$MINISWE_PIER_ROOT" rev-parse HEAD)" != "$expected_pier_revision" ]]; then
  printf 'Unexpected Pier revision.\n' >&2
  exit 2
fi
if [[ -n "$(git -C "$MINISWE_PIER_ROOT" status --porcelain)" ]]; then
  printf 'Pier worktree must be clean before applying the overlay.\n' >&2
  exit 2
fi

git -C "$MINISWE_PIER_ROOT" apply "$patch_path"

overlay_hash="$(git -C "$MINISWE_PIER_ROOT" diff HEAD --binary | sha256sum | awk '{print $1}')"
if [[ "$overlay_hash" != "$expected_pier_overlay_hash" ]]; then
  printf 'Pier overlay hash mismatch after apply.\n' >&2
  exit 2
fi

printf 'Pier overlay applied.\n'
