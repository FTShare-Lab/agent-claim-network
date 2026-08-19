#!/usr/bin/env bash
# 运行官方 MiniSWE 的 DeepSWE profile，并保留可恢复的 Pier job 目录。
set -euo pipefail

readonly expected_deepswe_revision="435ee89ec2f2e2289f33b0da4f992f0b7b7266b9"
readonly expected_pier_revision="0daf53d3599e58c4506cf0bcff5e12c77dc282d2"
readonly expected_pier_overlay_hash="7cb51ffacd2807a76d70c0ae22e051f840c3d499866233b328af676429a8b154"
readonly expected_mini_swe_agent_version="${MINISWE_PROFILE_AGENT_VERSION:-2.3.0}"
readonly expected_task_count=113
readonly expected_attempts_per_task="${MINISWE_PROFILE_ATTEMPTS_PER_TASK:-4}"
readonly expected_trial_count="${MINISWE_PROFILE_TRIAL_COUNT:-452}"
readonly expected_concurrency="${MINISWE_PROFILE_CONCURRENCY:-30}"
readonly job_name="${MINISWE_PROFILE_JOB_NAME:-miniswe-official-full-v1}"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
config_relative_path="${MINISWE_PROFILE_CONFIG_PATH:-benchmarks/deepswe/manifests/miniswe-official-full-v1.yaml}"
if [[ ! "$expected_mini_swe_agent_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  printf 'Invalid MiniSWE version profile.\n' >&2
  exit 2
fi
if [[ ! "$expected_attempts_per_task" =~ ^[1-9][0-9]*$ ]] \
 || [[ ! "$expected_trial_count" =~ ^[1-9][0-9]*$ ]] \
  || [[ ! "$expected_concurrency" =~ ^[1-9][0-9]*$ ]] \
 || (( expected_trial_count != expected_task_count * expected_attempts_per_task )); then
  printf 'Invalid rollout-count profile.\n' >&2
  exit 2
fi
if [[ ! "$job_name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  printf 'Invalid job-name profile.\n' >&2
  exit 2
fi
if [[ "$config_relative_path" == /* || "$config_relative_path" == *".."* ]]; then
  printf 'Invalid run-configuration profile path.\n' >&2
  exit 2
fi
config_path="$repo_root/$config_relative_path"

: "${ACN_EVAL_UPSTREAM_KEY:?ACN_EVAL_UPSTREAM_KEY must be set}"
: "${ACN_EVAL_UPSTREAM_BASE_URL:?ACN_EVAL_UPSTREAM_BASE_URL must be set}"
: "${MINISWE_DEEPSWE_ROOT:?MINISWE_DEEPSWE_ROOT must be set}"
: "${MINISWE_PIER_ROOT:?MINISWE_PIER_ROOT must be set}"
: "${MINISWE_RUN_ROOT:?MINISWE_RUN_ROOT must be set}"

if [[ ! -f "$config_path" ]]; then
  printf 'Missing run configuration: %s\n' "$config_path" >&2
  exit 2
fi

if [[ "$(git -C "$MINISWE_DEEPSWE_ROOT" rev-parse HEAD)" != "$expected_deepswe_revision" ]]; then
  printf 'Unexpected DeepSWE revision.\n' >&2
  exit 2
fi

if [[ "$(git -C "$MINISWE_PIER_ROOT" rev-parse HEAD)" != "$expected_pier_revision" ]]; then
  printf 'Unexpected Pier revision.\n' >&2
  exit 2
fi

if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  printf 'Evaluation worktree must be clean.\n' >&2
  exit 2
fi

if [[ -n "$(git -C "$MINISWE_DEEPSWE_ROOT" status --porcelain)" ]]; then
  printf 'DeepSWE worktree must be clean.\n' >&2
  exit 2
fi

if [[ -n "$(git -C "$MINISWE_PIER_ROOT" ls-files --others --exclude-standard)" ]]; then
  printf 'Pier worktree must not contain untracked files.\n' >&2
  exit 2
fi

# 以 HEAD 为基准覆盖已暂存与未暂存改动；未跟踪文件则在上方显式拒绝。
pier_overlay_hash="$(git -C "$MINISWE_PIER_ROOT" diff HEAD --binary | sha256sum | awk '{print $1}')"
if [[ "$pier_overlay_hash" != "$expected_pier_overlay_hash" ]]; then
  printf 'Unexpected Pier overlay.\n' >&2
  exit 2
fi

acn_revision="$(git -C "$repo_root" rev-parse HEAD)"
config_hash="$(sha256sum "$config_path" | awk '{print $1}')"

export DEEPSWE_TASKS_DIR="$MINISWE_DEEPSWE_ROOT/tasks"
export MINISWE_JOBS_DIR="$MINISWE_RUN_ROOT/jobs"
export MINISWE_API_KEY="$ACN_EVAL_UPSTREAM_KEY"

base_url="${ACN_EVAL_UPSTREAM_BASE_URL%/}"
if [[ "$base_url" != */v1 ]]; then
  base_url="$base_url/v1"
fi
export MINISWE_OPENAI_BASE_URL="$base_url"
unset base_url
endpoint_hash="$(printf '%s' "$MINISWE_OPENAI_BASE_URL" | sha256sum | awk '{print $1}')"

if [[ ! -d "$DEEPSWE_TASKS_DIR" ]]; then
  printf 'Missing DeepSWE task directory.\n' >&2
  exit 2
fi

task_count="$(find "$DEEPSWE_TASKS_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d '[:space:]')"
if [[ "$task_count" != "$expected_task_count" ]]; then
  printf 'Unexpected task count: %s\n' "$task_count" >&2
  exit 2
fi

preflight_environment() {
  uv run --project "$MINISWE_PIER_ROOT" python - "$config_path" "$expected_attempts_per_task" "$expected_concurrency" <<'PY'
import sys
from pathlib import Path

import yaml

from pier.environments.factory import EnvironmentFactory
from pier.models.job.config import JobConfig

config = JobConfig.model_validate(yaml.safe_load(Path(sys.argv[1]).read_text()))
expected_attempts_per_task = int(sys.argv[2])
expected_concurrency = int(sys.argv[3])
if config.n_attempts != expected_attempts_per_task:
    raise SystemExit("Run configuration rollout count does not match its profile.")
if config.n_concurrent_trials != expected_concurrency:
    raise SystemExit("Run configuration concurrency does not match its profile.")
if (config.retry or {}).max_retries != 0:
    raise SystemExit("Run configuration must retain zero in-place retries.")
EnvironmentFactory.run_preflight(
    type=config.environment.type,
    import_path=config.environment.import_path,
)
PY
}

# Docker 等环境的可用性检查必须早于任何可恢复 run 状态的写入。
preflight_environment

umask 077
mkdir -p "$MINISWE_JOBS_DIR"
job_path="$MINISWE_JOBS_DIR/$job_name"
provenance_path="$MINISWE_RUN_ROOT/run-provenance.json"
runtime_config_path="$MINISWE_RUN_ROOT/resolved-config.yaml"

render_runtime_config() {
  python3 - "$config_path" "$runtime_config_path" \
    "$MINISWE_JOBS_DIR" "$DEEPSWE_TASKS_DIR" <<'PY'
import json
import os
import sys
from pathlib import Path

source_path = Path(sys.argv[1])
target_path = Path(sys.argv[2])
replacements = {
    "${MINISWE_JOBS_DIR}": json.dumps(sys.argv[3]),
    "${DEEPSWE_TASKS_DIR}": json.dumps(sys.argv[4]),
}
contents = source_path.read_text(encoding="utf-8")
for marker, value in replacements.items():
    if contents.count(marker) != 1:
        print(f"Expected exactly one {marker} in run configuration.", file=sys.stderr)
        raise SystemExit(2)
    contents = contents.replace(marker, value)

if any(marker in contents for marker in replacements):
    print("Runtime path templates remain unresolved.", file=sys.stderr)
    raise SystemExit(2)

temporary_path = target_path.with_name(f".{target_path.name}.tmp")
try:
    with open(temporary_path, "x", encoding="utf-8") as target:
        target.write(contents)
    os.replace(temporary_path, target_path)
except OSError as error:
    print(f"Unable to write resolved run configuration: {error}", file=sys.stderr)
    raise SystemExit(2)
PY
}

validate_existing_provenance() {
  python3 - "$provenance_path" \
    "$acn_revision" \
    "$expected_deepswe_revision" \
    "$expected_pier_revision" \
    "$pier_overlay_hash" \
    "$config_hash" \
    "$runtime_config_hash" \
    "$endpoint_hash" \
    "$expected_mini_swe_agent_version" \
    "$expected_task_count" \
    "$expected_attempts_per_task" \
    "$expected_trial_count" <<'PY'
import json
import sys

path = sys.argv[1]
expected = {
    "schema_version": 1,
    "acn_revision": sys.argv[2],
    "deepswe_revision": sys.argv[3],
    "pier_revision": sys.argv[4],
    "pier_overlay_sha256": sys.argv[5],
    "config_sha256": sys.argv[6],
    "runtime_config_sha256": sys.argv[7],
    "endpoint_sha256": sys.argv[8],
    "mini_swe_agent_version": sys.argv[9],
    "task_count": int(sys.argv[10]),
    "attempts_per_task": int(sys.argv[11]),
    "trial_count": int(sys.argv[12]),
}

try:
    with open(path, encoding="utf-8") as source:
        actual = json.load(source)
except (OSError, json.JSONDecodeError) as error:
    print(f"Unable to read run provenance: {error}", file=sys.stderr)
    raise SystemExit(2)

if not isinstance(actual, dict):
    print("Run provenance must be a JSON object.", file=sys.stderr)
    raise SystemExit(2)

mismatched = [key for key, value in expected.items() if actual.get(key) != value]
if mismatched:
    print(
        "Run provenance does not match this launch: " + ", ".join(mismatched),
        file=sys.stderr,
    )
    raise SystemExit(2)
PY
}

if [[ -e "$job_path/lock.json" ]]; then
  if [[ ! -f "$provenance_path" ]]; then
    printf 'Existing job is missing run provenance.\n' >&2
    exit 2
  fi
  if [[ ! -f "$runtime_config_path" ]]; then
    printf 'Existing job is missing its resolved run configuration.\n' >&2
    exit 2
  fi
  runtime_config_hash="$(sha256sum "$runtime_config_path" | awk '{print $1}')"
  validate_existing_provenance

  for trial_path in "$job_path"/*; do
    [[ -d "$trial_path" && ! -f "$trial_path/result.json" ]] || continue
    if [[ -f "$trial_path/config.json" || -f "$trial_path/trial.log" \
      || -d "$trial_path/agent" || -d "$trial_path/verifier" \
      || -d "$trial_path/artifacts" ]]; then
      printf 'Incomplete trial retained at %s; refusing a hidden re-run.\n' "$trial_path" >&2
      exit 2
    fi
  done

  # Pier 默认会删除 CancelledError 结果后重跑；空 filter 保持 profile 的计划 trial 数不变。
  exec uv run --project "$MINISWE_PIER_ROOT" pier job resume \
    --job-path "$job_path" --filter-error-type ''
fi

if [[ -e "$job_path" ]]; then
  printf 'Existing job directory has no lock and will not be overwritten: %s\n' "$job_path" >&2
  exit 2
fi

if [[ -e "$provenance_path" ]]; then
  printf 'Run root already contains provenance and will not be overwritten.\n' >&2
  exit 2
fi

if [[ -e "$runtime_config_path" ]]; then
  printf 'Run root already contains a resolved run configuration and will not be overwritten.\n' >&2
  exit 2
fi

render_runtime_config
runtime_config_hash="$(sha256sum "$runtime_config_path" | awk '{print $1}')"

provenance_temp="$(mktemp "$MINISWE_RUN_ROOT/.run-provenance.XXXXXX")"
printf '{\n  "schema_version": 1,\n  "acn_revision": "%s",\n  "deepswe_revision": "%s",\n  "pier_revision": "%s",\n  "pier_overlay_sha256": "%s",\n  "config_sha256": "%s",\n  "runtime_config_sha256": "%s",\n  "endpoint_sha256": "%s",\n  "mini_swe_agent_version": "%s",\n  "task_count": %s,\n  "attempts_per_task": %s,\n  "trial_count": %s\n}\n' \
  "$acn_revision" \
  "$expected_deepswe_revision" \
  "$expected_pier_revision" \
  "$pier_overlay_hash" \
  "$config_hash" \
  "$runtime_config_hash" \
  "$endpoint_hash" \
  "$expected_mini_swe_agent_version" \
  "$expected_task_count" \
  "$expected_attempts_per_task" \
  "$expected_trial_count" >"$provenance_temp"
mv "$provenance_temp" "$provenance_path"

exec uv run --project "$MINISWE_PIER_ROOT" pier run --config "$runtime_config_path"
