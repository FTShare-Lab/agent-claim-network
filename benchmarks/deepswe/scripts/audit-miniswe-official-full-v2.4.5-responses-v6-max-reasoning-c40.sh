#!/usr/bin/env bash
# 在启动 Responses c40 显式推理对照前，校验实际会传给 MiniSWE 的协议字段。
set -euo pipefail

readonly expected_deepswe_revision="435ee89ec2f2e2289f33b0da4f992f0b7b7266b9"
readonly expected_pier_revision="0daf53d3599e58c4506cf0bcff5e12c77dc282d2"
readonly expected_pier_overlay_hash="7cb51ffacd2807a76d70c0ae22e051f840c3d499866233b328af676429a8b154"
readonly expected_agent_version="2.4.5"
readonly expected_concurrency=40
readonly expected_job_name="miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40"
readonly config_relative_path="benchmarks/deepswe/manifests/miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40.yaml"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
config_path="$repo_root/$config_relative_path"

: "${MINISWE_DEEPSWE_ROOT:?MINISWE_DEEPSWE_ROOT must be set}"
: "${MINISWE_PIER_ROOT:?MINISWE_PIER_ROOT must be set}"

[[ "$(git -C "$MINISWE_DEEPSWE_ROOT" rev-parse HEAD)" == "$expected_deepswe_revision" ]] || {
  printf 'Unexpected DeepSWE revision.\n' >&2
  exit 2
}
[[ "$(git -C "$MINISWE_PIER_ROOT" rev-parse HEAD)" == "$expected_pier_revision" ]] || {
  printf 'Unexpected Pier revision.\n' >&2
  exit 2
}
[[ -z "$(git -C "$MINISWE_PIER_ROOT" ls-files --others --exclude-standard)" ]] || {
  printf 'Pier worktree must not contain untracked files.\n' >&2
  exit 2
}

pier_overlay_hash="$(git -C "$MINISWE_PIER_ROOT" diff HEAD --binary | sha256sum | awk '{print $1}')"
[[ "$pier_overlay_hash" == "$expected_pier_overlay_hash" ]] || {
  printf 'Unexpected Pier overlay.\n' >&2
  exit 2
}

uv run --project "$MINISWE_PIER_ROOT" python - "$config_path" "$expected_agent_version" "$expected_job_name" "$expected_concurrency" <<'PY'
import json
import shlex
import sys
from pathlib import Path

import yaml

from pier.agents.installed.mini_swe_agent import MiniSweAgent
from pier.models.job.config import JobConfig

config_path = Path(sys.argv[1])
expected_version = sys.argv[2]
expected_job_name = sys.argv[3]
expected_concurrency = int(sys.argv[4])
config = JobConfig.model_validate(yaml.safe_load(config_path.read_text(encoding="utf-8")))

if config.job_name != expected_job_name:
    raise SystemExit("Unexpected job name.")
if config.n_attempts != 2 or config.n_concurrent_trials != expected_concurrency:
    raise SystemExit("Unexpected rollout or concurrency setting.")
if (config.retry or {}).max_retries != 0:
    raise SystemExit("Unexpected in-place retry setting.")
if len(config.agents) != 1:
    raise SystemExit("Expected exactly one agent.")

agent_config = config.agents[0]
if agent_config.name != "mini-swe-agent" or agent_config.model_name != "openai/deepseek-v4-flash-local":
    raise SystemExit("Unexpected agent identity.")
kwargs = agent_config.kwargs
expected_kwargs = {
    "version": expected_version,
    "model_class": "litellm_response",
    "normalize_response_reasoning_text": True,
    "restrict_egress_to_configured_base": True,
    "cost_limit": 0,
}
for key, value in expected_kwargs.items():
    if kwargs.get(key) != value:
        raise SystemExit(f"Unexpected agent kwarg: {key}")
if "reasoning_effort" in kwargs:
    raise SystemExit("Responses profile must use model_kwargs.reasoning, not outer reasoning_effort.")

expected_model_kwargs = {
    "model_info": {
        "max_input_tokens": 1000000,
        "max_output_tokens": 65536,
        "litellm_provider": "openai",
        "mode": "responses",
    },
    "max_output_tokens": 65536,
    "reasoning": {"effort": "max"},
    "truncation": "disabled",
    "temperature": 1.0,
    "top_p": 0.95,
}
if kwargs.get("model_kwargs") != expected_model_kwargs:
    raise SystemExit("Unexpected Responses model kwargs.")

agent = MiniSweAgent(
    logs_dir=Path("/tmp/miniswe-audit"),
    model_name=agent_config.model_name,
    version=kwargs["version"],
    model_class=kwargs["model_class"],
    model_kwargs=kwargs["model_kwargs"],
    cost_limit=kwargs["cost_limit"],
    normalize_response_reasoning_text=kwargs["normalize_response_reasoning_text"],
    restrict_egress_to_configured_base=kwargs["restrict_egress_to_configured_base"],
)
flags = agent._build_config_flags()
flag_words = shlex.split(flags)
flag_values = {
    flag_words[index + 1]
    for index, word in enumerate(flag_words[:-1])
    if word == "-c"
}
required_flags = {
    "model.model_class=litellm_response",
    'model.model_kwargs.reasoning={"effort": "max"}',
    "model.model_kwargs.max_output_tokens=65536",
    "model.model_kwargs.truncation=\"disabled\"",
    "model.model_kwargs.temperature=1.0",
    "model.model_kwargs.top_p=0.95",
}
missing_flags = sorted(required_flags - flag_values)
if missing_flags:
    raise SystemExit("MiniSWE flags are missing: " + ", ".join(missing_flags))
for forbidden_flag in (
    "model.model_kwargs.reasoning_effort=max",
    "model.model_kwargs.max_tokens=65536",
):
    if forbidden_flag in flag_values:
        raise SystemExit("MiniSWE flags contain an incompatible field: " + forbidden_flag)
if f"mini-swe-agent=={expected_version}" not in agent.install_spec().steps[1].run:
    raise SystemExit("Pier would not pin the expected MiniSWE installation version.")

print(
    json.dumps(
        {
            "status": "aligned",
            "job_name": config.job_name,
            "agent_version": kwargs["version"],
            "model_class": kwargs["model_class"],
            "attempts": config.n_attempts,
            "concurrency": config.n_concurrent_trials,
            "reasoning": kwargs["model_kwargs"]["reasoning"],
            "max_output_tokens": kwargs["model_kwargs"]["max_output_tokens"],
        },
        sort_keys=True,
    )
)
PY
