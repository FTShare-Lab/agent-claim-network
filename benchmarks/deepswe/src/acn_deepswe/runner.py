"""实验 manifest、目录隔离及 task1 单次解题 dry-run 编排。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from .plan import AttemptPlan
from .provenance import EvaluationProvenance
from .schemas import AttemptManifest


class AttemptDirectoryError(ValueError):
    """attempt 目录隔离关系不成立时抛出。"""


@dataclass(frozen=True)
class ExperimentManifest:
    schema_version: int
    experiment_id: str
    plan_hash: str
    claim_bundle_hash: str | None
    provenance: EvaluationProvenance
    attempts: tuple[AttemptManifest, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "experiment_id": self.experiment_id,
            "plan_hash": self.plan_hash,
            "claim_bundle_hash": self.claim_bundle_hash,
            "provenance": self.provenance.to_dict(),
            "attempts": [attempt.to_dict() for attempt in self.attempts],
        }


@dataclass(frozen=True)
class Task1DryRunStep:
    attempt_id: str
    variant: str
    solve_runs: int
    retry_allowed: bool


@dataclass(frozen=True)
class Task1DryRunPlan:
    experiment_id: str
    steps: tuple[Task1DryRunStep, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "experiment_id": self.experiment_id,
            "dry_run": True,
            "steps": [
                {
                    "attempt_id": step.attempt_id,
                    "variant": step.variant,
                    "solve_runs": step.solve_runs,
                    "retry_allowed": step.retry_allowed,
                }
                for step in self.steps
            ],
        }


def build_experiment_manifest(
    experiment_id: str,
    plan: AttemptPlan,
    claim_bundle_hash: str | None,
    provenance: EvaluationProvenance,
) -> ExperimentManifest:
    """记录可复现计划与 bundle hash，不记录任何 credential。"""
    if not experiment_id:
        raise ValueError("experiment_id 不能为空")
    if claim_bundle_hash is not None and (
        len(claim_bundle_hash) != 64
        or any(character not in "0123456789abcdef" for character in claim_bundle_hash)
    ):
        raise ValueError("claim_bundle_hash 必须是 64 位小写 hex 或 null")
    _validate_isolation(plan.attempts)
    plan_payload = {
        "freeze_candidates_hash": plan.freeze_candidates_hash,
        "seed": plan.seed,
        "attempts": [item.to_dict() for item in plan.attempts],
    }
    plan_hash = hashlib.sha256(
        json.dumps(plan_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if provenance.dataset_candidates_hash != plan.freeze_candidates_hash:
        raise ValueError("provenance dataset_candidates_hash 必须匹配 attempt plan")
    return ExperimentManifest(
        1, experiment_id, plan_hash, claim_bundle_hash, provenance, plan.attempts
    )


def build_task1_dry_run(experiment: ExperimentManifest) -> Task1DryRunPlan:
    """仅生成运行顺序；每个 attempt 的解题运行次数硬编码为一次，绝不重试。"""
    groups: dict[str, list[AttemptManifest]] = {}
    for attempt in experiment.attempts:
        groups.setdefault(attempt.task_id, []).append(attempt)
    steps: list[Task1DryRunStep] = []
    for task_id in sorted(groups):
        group = groups[task_id]
        variants = [item.variant for item in group]
        if (
            not variants
            or variants[0] != "A"
            or set(variants[1:]) != {"B_empty", "B_claim", "B_forced_claim"}
            or len(variants) != 4
        ):
            raise AttemptDirectoryError(
                f"task {task_id} 必须先 A，且 B_empty/B_claim/B_forced_claim 各一次"
            )
        steps.extend(Task1DryRunStep(item.attempt_id, item.variant, 1, False) for item in group)
    return Task1DryRunPlan(experiment.experiment_id, tuple(steps))


def write_experiment_manifest(path: Path, experiment: ExperimentManifest) -> None:
    """以稳定 JSON 写出不含 credential 的 experiment manifest。"""
    if not path.is_absolute():
        raise ValueError(f"experiment manifest 输出必须为绝对路径: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(experiment.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _validate_isolation(attempts: tuple[AttemptManifest, ...]) -> None:
    locations = [Path(attempt.output_path) for attempt in attempts]
    if len(locations) != len(set(locations)):
        raise AttemptDirectoryError("A/B attempt 不得共享 output 路径")
    for index, left in enumerate(locations):
        for right in locations[index + 1 :]:
            if left in right.parents or right in left.parents:
                raise AttemptDirectoryError("A/B attempt output 路径不得存在父子重叠")
