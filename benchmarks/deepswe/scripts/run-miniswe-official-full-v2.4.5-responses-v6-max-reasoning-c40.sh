#!/usr/bin/env bash
# MiniSWE 2.4.5 Responses 的 40 并发、显式 reasoning=max 全量入口。
set -euo pipefail

readonly job_name="miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40"
readonly config_path="benchmarks/deepswe/manifests/miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40.yaml"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
"$script_dir/audit-miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40.sh"

export MINISWE_PROFILE_AGENT_VERSION="2.4.5"
export MINISWE_PROFILE_ATTEMPTS_PER_TASK="2"
export MINISWE_PROFILE_TRIAL_COUNT="226"
export MINISWE_PROFILE_CONCURRENCY="40"
export MINISWE_PROFILE_JOB_NAME="$job_name"
export MINISWE_PROFILE_CONFIG_PATH="$config_path"
exec "$script_dir/run-miniswe-official-full.sh"
