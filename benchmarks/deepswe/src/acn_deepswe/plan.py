"""基于已冻结数据集构建确定的 A/B 尝试计划，不读取 verifier 输出。"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .dataset import FrozenDatasetManifest
from .schemas import AttemptManifest


@dataclass(frozen=True)
class AttemptPlan:
    schema_version: int
    freeze_candidates_hash: str
    seed: int
    attempts: tuple[AttemptManifest, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "freeze_candidates_hash": self.freeze_candidates_hash,
            "seed": self.seed,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> AttemptPlan:
        attempts = data.get("attempts")
        if not isinstance(attempts, list) or not all(
            isinstance(item, Mapping) for item in attempts
        ):
            raise ValueError("attempt plan.attempts 必须是对象数组")
        schema_version = data.get("schema_version")
        seed = data.get("seed")
        frozen_hash = data.get("freeze_candidates_hash")
        if schema_version != 1 or isinstance(seed, bool) or not isinstance(seed, int):
            raise ValueError("attempt plan schema_version/seed 无效")
        if not isinstance(frozen_hash, str) or not frozen_hash:
            raise ValueError("attempt plan 缺少 freeze_candidates_hash")
        return cls(
            1, frozen_hash, seed, tuple(AttemptManifest.from_dict(item) for item in attempts)
        )


def build_attempt_plan(
    manifest: FrozenDatasetManifest, output_root: Path, seed: int
) -> AttemptPlan:
    """每个 task 固定先 A，随后以固定 seed 平衡 B_empty/B_claim。"""
    if not output_root.is_absolute():
        raise ValueError(f"计划输出根目录必须为绝对路径: {output_root}")
    shuffled_ids = list(manifest.task_ids)
    random.Random(seed).shuffle(shuffled_ids)
    first_b_variant = {
        task_id: ("B_empty" if index % 2 == 0 else "B_claim")
        for index, task_id in enumerate(shuffled_ids)
    }
    attempts: list[AttemptManifest] = []
    for task_id in manifest.task_ids:
        attempts.append(_attempt(task_id, "A", output_root))
        first = first_b_variant[task_id]
        second = "B_claim" if first == "B_empty" else "B_empty"
        attempts.append(_attempt(task_id, first, output_root))
        attempts.append(_attempt(task_id, second, output_root))
    return AttemptPlan(1, manifest.candidates_hash, seed, tuple(attempts))


def _attempt(task_id: str, variant: str, output_root: Path) -> AttemptManifest:
    digest = hashlib.sha256(f"{task_id}:{variant}".encode()).hexdigest()[:12]
    attempt_id = f"{task_id}-{variant.lower()}-{digest}"
    base = output_root / "attempts" / attempt_id
    return AttemptManifest(
        schema_version=1,
        attempt_id=attempt_id,
        task_id=task_id,
        variant=variant,
        output_path=str(base / "output"),
    )
