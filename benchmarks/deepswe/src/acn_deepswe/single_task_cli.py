"""单题四臂链路验证入口：复用 pre-smoke 的全部冻结校验，只执行其中一题。

用途是在拉到五题规模之前，确认 A → freeze → B_empty/B_claim/B_forced_claim 的证据链闭合，
而不是产出 pass rate。任务必须来自冻结 manifest，配置字段与 pre-smoke 完全一致。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from .host_runner import Task1HostRunner, TaskExecutionError
from .presmoke_cli import (
    PresmokeCliError,
    build_task_specs,
    load_config,
    preflight_execution,
    stage_python_runtime,
    verify_acn_revision,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="acn-deepswe-single-task",
        description="按冻结 manifest 执行单个 task 的四臂链路验证。",
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="绝对路径 JSON 配置（不含 credential）"
    )
    parser.add_argument("--task-id", required=True, help="必须是冻结 manifest 中的 task id")
    parser.add_argument("--dry-run", action="store_true", help="只校验冻结输入并输出计划")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        upstream_base_url = os.environ.get("ACN_EVAL_UPSTREAM_BASE_URL")
        if not upstream_base_url:
            raise PresmokeCliError("宿主环境缺少 ACN_EVAL_UPSTREAM_BASE_URL")
        if not args.dry_run and not os.environ.get("ACN_EVAL_UPSTREAM_KEY"):
            raise PresmokeCliError("宿主环境缺少 ACN_EVAL_UPSTREAM_KEY")
        verify_acn_revision(config.acn_revision)
        if not args.dry_run:
            preflight_execution(config)
        frozen_runtime = None if args.dry_run else stage_python_runtime(config)
        specs, frozen_task_ids = build_task_specs(
            config,
            upstream_base_url,
            resolve_image_digests=not args.dry_run,
            frozen_runtime=frozen_runtime,
        )
        if args.task_id not in frozen_task_ids:
            raise PresmokeCliError(
                f"task-id 必须来自冻结 manifest: {args.task_id} 不在 {list(frozen_task_ids)}"
            )
        spec = next(item for item in specs if item.task_id == args.task_id)
        summary = {
            "task_id": spec.task_id,
            "arms": [attempt.variant for attempt in spec.experiment.attempts],
            "model": config.model,
            "reasoning_effort": config.reasoning_effort,
            "agent_seconds": config.timeouts["agent_seconds"],
            "manifest_path": str(spec.manifest_path),
        }
        if args.dry_run:
            print(json.dumps({"dry_run": True, **summary}, ensure_ascii=False, indent=2))
            return 0
        execution = (
            replace(spec.execution, require_eligible_claim=True)
            if spec.execution is not None
            else None
        )
        Task1HostRunner(spec.experiment, spec.jobs_directory, execution).run_task1(execute=True)
        print(json.dumps({"status": "completed", **summary}, ensure_ascii=False))
        return 0
    except (OSError, ValueError, subprocess.SubprocessError, TaskExecutionError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    sys.exit(main())
