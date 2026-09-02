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
    """每题先冻结 A/B_empty producer wave，再平衡两个 claim consumer 的顺序。"""
    if not output_root.is_absolute():
        raise ValueError(f"计划输出根目录必须为绝对路径: {output_root}")
    shuffled_ids = list(manifest.task_ids)
    random.Random(seed).shuffle(shuffled_ids)
    consumer_orders = (
        ("B_claim", "B_forced_claim"),
        ("B_forced_claim", "B_claim"),
    )
    consumers_by_task = {
        task_id: consumer_orders[index % len(consumer_orders)]
        for index, task_id in enumerate(shuffled_ids)
    }
    attempts: list[AttemptManifest] = []
    for task_id in manifest.task_ids:
        attempts.append(_attempt(task_id, "A", output_root))
        attempts.append(_attempt(task_id, "B_empty", output_root))
        attempts.extend(
            _attempt(task_id, variant, output_root) for variant in consumers_by_task[task_id]
        )
    return AttemptPlan(1, manifest.candidates_hash, seed, tuple(attempts))


def _attempt(task_id: str, variant: str, output_root: Path) -> AttemptManifest:
    # 宿主 runner 以 resolve 后的路径写 attempt-result / gate；计划里的 output_path
    # 必须与之一致，否则符号链接路径下二者无法对接。
    output_root = output_root.resolve()
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
