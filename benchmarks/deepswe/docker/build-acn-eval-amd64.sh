#!/usr/bin/env sh
set -eu

usage() {
  printf '用法：%s [--mirror-prefix PREFIX | --direct]\n' "$0" >&2
}

mirror_prefix=${ACN_DOCKER_MIRROR_PREFIX:-m.daocloud.io/docker.io/library}
case "${1:-}" in
  "")
    ;;
  --mirror-prefix)
    [ "$#" -eq 2 ] && [ -n "$2" ] || {
      usage
      exit 2
    }
    mirror_prefix=${2%/}
    ;;
  --direct)
    [ "$#" -eq 1 ] || {
      usage
      exit 2
    }
    mirror_prefix=
    ;;
  *)
    usage
    exit 2
    ;;
esac

host_os=$(uname -s)
host_arch=$(uname -m)
host_kernel=$(uname -r)

case "$host_os/$host_arch" in
  Darwin/arm64 | Darwin/x86_64 | Linux/arm64 | Linux/aarch64 | Linux/x86_64) ;;
  *)
    printf '错误：不支持的宿主平台：%s/%s\n' "$host_os" "$host_arch" >&2
    exit 1
    ;;
esac

if ! docker_info=$(docker info --format '{{.OSType}}/{{.Architecture}}'); then
  printf '错误：无法连接 Docker daemon\n' >&2
  exit 1
fi
case "$docker_info" in
  linux/arm64 | linux/aarch64) builder_platform=linux/arm64 ;;
  linux/amd64 | linux/x86_64) builder_platform=linux/amd64 ;;
  *)
    printf '错误：不支持的 Docker daemon 平台：%s\n' "$docker_info" >&2
    exit 1
    ;;
esac

pull_base_image() {
  official_image=$1
  if [ -n "$mirror_prefix" ]; then
    source_image="$mirror_prefix/$official_image"
  else
    source_image=$official_image
  fi

  docker pull --platform "$builder_platform" "$source_image"
  if [ "$source_image" != "$official_image" ]; then
    docker tag "$source_image" "$official_image"
  fi
  image_platform=$(docker image inspect "$official_image" --format '{{.Os}}/{{.Architecture}}')
  [ "$image_platform" = "$builder_platform" ] || {
    printf '错误：镜像 %s 平台为 %s，预期 %s\n' \
      "$official_image" "$image_platform" "$builder_platform" >&2
    exit 1
  }
  docker image inspect "$official_image" --format '{{.RepoTags}} {{.Os}}/{{.Architecture}} {{.Id}}'
}

if [ -n "$mirror_prefix" ]; then
  debian_base_image="$mirror_prefix/debian:bookworm-slim"
else
  debian_base_image=debian:bookworm-slim
fi

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
image_name=${ACN_EVAL_BUILDER_IMAGE:-acn-eval-amd64-builder:rust-1.90}
output_dir="$repo_root/target/deepswe-linux-amd64"
binary_path="$output_dir/release/acn_eval"
git_commit=$(git -C "$repo_root" rev-parse HEAD)
git_commit_timestamp=$(git -C "$repo_root" show -s --date=format:%Y-%m-%d\ %H:%M:%S --format=%cd HEAD)

printf '构建环境：host=%s/%s kernel=%s docker=%s builder=%s target=linux/amd64\n' \
  "$host_os" "$host_arch" "$host_kernel" "$docker_info" "$builder_platform"

pull_base_image ubuntu:24.04
pull_base_image debian:bookworm-slim

docker build --platform "$builder_platform" \
  --build-arg "BASE_IMAGE=$debian_base_image" \
  -f "$repo_root/benchmarks/deepswe/docker/acn-eval-amd64-builder.Dockerfile" \
  -t "$image_name" \
  "$repo_root/benchmarks/deepswe/docker"
mkdir -p "$output_dir/release"
docker run --platform "$builder_platform" --rm -v "$repo_root:/work" -w /work \
  -e "ACN_GIT_COMMIT=$git_commit" \
  -e "ACN_GIT_COMMIT_TIMESTAMP=$git_commit_timestamp" \
  "$image_name" \
  cargo build --release --target x86_64-unknown-linux-gnu --bin acn_eval
cp "$repo_root/target/x86_64-unknown-linux-gnu/release/acn_eval" "$binary_path"

binary_info=$(file "$binary_path")
printf '%s\n' "$binary_info"
case "$binary_info" in
  *"ELF 64-bit"*"x86-64"*) ;;
  *)
    printf '错误：构建产物不是预期的 Linux x86_64 ELF：%s\n' "$binary_path" >&2
    exit 1
    ;;
esac

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$binary_path"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$binary_path"
else
  printf '错误：缺少 SHA-256 工具（需要 sha256sum 或 shasum）\n' >&2
  exit 1
fi
