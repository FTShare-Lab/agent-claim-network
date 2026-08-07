"""不触发真实模型或 Pier runtime 的审计型命令行入口。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .claim_freeze import append_freeze_barrier, freeze_claim_bundle
from .dataset import FrozenDatasetManifest, freeze_dataset
from .network import normalize_task_network
from .plan import AttemptPlan, build_attempt_plan
from .provenance import EvaluationProvenance
from .runner import build_experiment_manifest, build_task1_dry_run, write_experiment_manifest
from .sentinel import scan_for_sentinel_leaks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="acn-deepswe")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate-config", help="检查并生成 Pier 兼容的离线 task.toml")
    validate.add_argument("task", type=Path)
    validate.add_argument("output_directory", type=Path)
    freeze = subparsers.add_parser("freeze-dataset", help="冻结五个任务的无放回样本")
    freeze.add_argument("tasks_root", type=Path)
    freeze.add_argument("manifest", type=Path)
    freeze.add_argument("--seed", type=int, required=True)
    plan = subparsers.add_parser("plan", help="从冻结 manifest 构建 A/B 尝试计划")
    plan.add_argument("manifest", type=Path)
    plan.add_argument("output_root", type=Path)
    plan.add_argument("--seed", type=int, required=True)
    claims = subparsers.add_parser(
        "freeze-claims", help="从 host event ledger 的 barrier 冻结 claim bundle"
    )
    claims.add_argument("host_ledger", type=Path)
    claims.add_argument("attempt_id")
    claims.add_argument("output", type=Path)
    barrier = subparsers.add_parser(
        "append-freeze-barrier", help="在 attempt_finished 后追加唯一的 claim freeze barrier"
    )
    barrier.add_argument("host_ledger", type=Path)
    barrier.add_argument("attempt_id")
    barrier.add_argument("barrier_id")
    sentinels = subparsers.add_parser("scan-sentinels", help="扫描实验目录中的泄漏 sentinel")
    sentinels.add_argument("root", type=Path)
    sentinels.add_argument("--sentinel", action="append", required=True)
    dry_run = subparsers.add_parser("task1-dry-run", help="生成禁止解题重试的 task1 运行计划")
    dry_run.add_argument("attempt_plan", type=Path)
    dry_run.add_argument("output", type=Path)
    dry_run.add_argument("--experiment-id", required=True)
    dry_run.add_argument("--claim-bundle-hash")
    dry_run.add_argument("--provenance", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "validate-config":
            result = normalize_task_network(args.task.resolve(), args.output_directory.resolve())
            print(
                json.dumps(
                    {
                        "task_path": str(result.task_path),
                        "source_hash": result.source_hash,
                        "normalized_hash": result.normalized_hash,
                        "warning": result.warning,
                    },
                    ensure_ascii=False,
                )
            )
        elif args.command == "freeze-dataset":
            result = freeze_dataset(args.tasks_root.resolve(), args.manifest.resolve(), args.seed)
            print(
                json.dumps(
                    {"manifest_path": str(args.manifest.resolve()), **result.to_dict()},
                    ensure_ascii=False,
                )
            )
        elif args.command == "plan":
            raw = json.loads(args.manifest.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("冻结 manifest 必须是 JSON 对象")
            result = build_attempt_plan(
                FrozenDatasetManifest.from_dict(raw), args.output_root.resolve(), args.seed
            )
            output_path = args.output_root.resolve() / "attempt-plan.json"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(result.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            print(
                json.dumps(
                    {"plan_path": str(output_path), "attempt_count": len(result.attempts)},
                    ensure_ascii=False,
                )
            )
        elif args.command == "freeze-claims":
            result = freeze_claim_bundle(
                args.host_ledger.resolve(), args.attempt_id, args.output.resolve()
            )
            print(
                json.dumps(
                    {"bundle_path": str(args.output.resolve()), **result.manifest_dict()},
                    ensure_ascii=False,
                )
            )
        elif args.command == "append-freeze-barrier":
            result = append_freeze_barrier(
                args.host_ledger.resolve(), args.attempt_id, args.barrier_id
            )
            print(json.dumps(result.to_dict(), ensure_ascii=False))
        elif args.command == "scan-sentinels":
            result = scan_for_sentinel_leaks(args.root.resolve(), tuple(args.sentinel))
            print(
                json.dumps(
                    {"root": str(result.root), "files_scanned": result.files_scanned},
                    ensure_ascii=False,
                )
            )
        else:
            raw = json.loads(args.attempt_plan.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("attempt plan 必须是 JSON 对象")
            plan_value = AttemptPlan.from_dict(raw)
            provenance_raw = json.loads(args.provenance.read_text(encoding="utf-8"))
            if not isinstance(provenance_raw, dict):
                raise ValueError("provenance 必须是 JSON 对象")
            provenance = EvaluationProvenance.from_dict(provenance_raw)
            experiment = build_experiment_manifest(
                args.experiment_id, plan_value, args.claim_bundle_hash, provenance
            )
            write_experiment_manifest(args.output.resolve(), experiment)
            result = build_task1_dry_run(experiment)
            print(
                json.dumps(
                    {"experiment_path": str(args.output.resolve()), **result.to_dict()},
                    ensure_ascii=False,
                )
            )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    sys.exit(main())
