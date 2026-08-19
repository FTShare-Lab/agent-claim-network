#!/usr/bin/env bash
# 从 MiniSWE 保存的原始 Responses 对象验证 c40 请求设置已被上游确认。
set -euo pipefail

readonly expected_job_name="miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40"
: "${MINISWE_RUN_ROOT:?MINISWE_RUN_ROOT must be set}"

job_path="$MINISWE_RUN_ROOT/jobs/$expected_job_name"
[[ -d "$job_path" ]] || {
  printf 'Missing Responses job directory.\n' >&2
  exit 2
}

python3 - "$job_path" <<'PY'
import json
import sys
from pathlib import Path

job_path = Path(sys.argv[1])
trajectory_paths = sorted(job_path.glob("*/agent/mini-swe-agent.trajectory.json"))
if not trajectory_paths:
    raise SystemExit("No completed MiniSWE trajectory is available for wire verification.")

responses = 0
bad_reasoning = 0
bad_max_output_tokens = 0
for path in trajectory_paths:
    data = json.loads(path.read_text(encoding="utf-8"))
    for message in data.get("messages", []):
        if not isinstance(message, dict) or message.get("object") != "response":
            continue
        responses += 1
        reasoning = message.get("reasoning")
        if not isinstance(reasoning, dict) or reasoning.get("effort") != "max":
            bad_reasoning += 1
        if message.get("max_output_tokens") != 65536:
            bad_max_output_tokens += 1

if responses == 0:
    raise SystemExit("Completed trajectories do not contain Responses objects.")
if bad_reasoning or bad_max_output_tokens:
    raise SystemExit(
        "Responses wire verification failed: "
        f"responses={responses}, reasoning_mismatch={bad_reasoning}, "
        f"max_output_tokens_mismatch={bad_max_output_tokens}"
    )

print(
    json.dumps(
        {
            "status": "verified",
            "trajectories": len(trajectory_paths),
            "responses": responses,
            "reasoning_effort": "max",
            "max_output_tokens": 65536,
        },
        sort_keys=True,
    )
)
PY
