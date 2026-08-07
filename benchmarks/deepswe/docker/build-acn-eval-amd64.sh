#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
image_name=${ACN_EVAL_BUILDER_IMAGE:-acn-eval-amd64-builder:rust-1.90}
output_dir="$repo_root/target/deepswe-linux-amd64"
git_commit=$(git -C "$repo_root" rev-parse HEAD)
git_commit_timestamp=$(git -C "$repo_root" show -s --date=format:%Y-%m-%d\ %H:%M:%S --format=%cd HEAD)

docker build --platform linux/arm64 \
  -f "$repo_root/benchmarks/deepswe/docker/acn-eval-amd64-builder.Dockerfile" \
  -t "$image_name" \
  "$repo_root/benchmarks/deepswe/docker"
mkdir -p "$output_dir/release"
docker run --platform linux/arm64 --rm -v "$repo_root:/work" -w /work \
  -e "ACN_GIT_COMMIT=$git_commit" \
  -e "ACN_GIT_COMMIT_TIMESTAMP=$git_commit_timestamp" \
  "$image_name" \
  cargo build --release --target x86_64-unknown-linux-gnu --bin acn_eval
cp "$repo_root/target/x86_64-unknown-linux-gnu/release/acn_eval" "$output_dir/release/acn_eval"
file "$output_dir/release/acn_eval"
shasum -a 256 "$output_dir/release/acn_eval"
