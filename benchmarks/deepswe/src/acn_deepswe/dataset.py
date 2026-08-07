"""任务目录的可复现冻结；冻结发生在任何 verifier 结果之前。"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from pathlib import Path

FREEZE_ALGORITHM = "random.sample_without_replacement_v1"
SUPPORTED_FREEZE_ALGORITHMS = frozenset(
    {
        FREEZE_ALGORITHM,
        "official_v1.1_luna_max_extreme_cohorts_v1",
        "official_v1.1_luna_max_user_selected_followup_v1",
    }
)


class DatasetFreezeError(ValueError):
    """候选任务不完整或冻结输出不可审计时抛出。"""


@dataclass(frozen=True)
class FrozenDatasetManifest:
    schema_version: int
    algorithm: str
    seed: int
    candidates_hash: str
    task_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "algorithm": self.algorithm,
            "seed": self.seed,
            "candidates_hash": self.candidates_hash,
            "task_ids": list(self.task_ids),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> FrozenDatasetManifest:
        raw_ids = data.get("task_ids")
        if (
            not isinstance(raw_ids, list)
            or not raw_ids
            or not all(isinstance(item, str) and item for item in raw_ids)
            or len(set(raw_ids)) != len(raw_ids)
        ):
            raise DatasetFreezeError("冻结 manifest.task_ids 必须是非空且不重复的字符串数组")
        seed = data.get("seed")
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise DatasetFreezeError("冻结 manifest.seed 必须为整数")
        if data.get("schema_version") != 1:
            raise DatasetFreezeError("冻结 manifest 仅支持 schema_version=1")
        algorithm = data.get("algorithm")
        if not isinstance(algorithm, str) or algorithm not in SUPPORTED_FREEZE_ALGORITHMS:
            raise DatasetFreezeError("冻结 manifest.algorithm 不受支持")
        candidates_hash = data.get("candidates_hash")
        if (
            not isinstance(candidates_hash, str)
            or len(candidates_hash) != 64
            or any(character not in "0123456789abcdef" for character in candidates_hash)
        ):
            raise DatasetFreezeError("冻结 manifest.candidates_hash 必须是 64 位小写 SHA-256")
        return cls(1, algorithm, seed, candidates_hash, tuple(raw_ids))


def freeze_dataset(
    tasks_root: Path, manifest_path: Path, seed: int, sample_size: int = 5
) -> FrozenDatasetManifest:
    """从稳定排序的 task.toml 目录无放回抽样，绝不观察执行结果。"""
    if isinstance(sample_size, bool) or not isinstance(sample_size, int) or sample_size <= 0:
        raise DatasetFreezeError("抽样任务数必须为正整数")
    if not tasks_root.is_dir():
        raise DatasetFreezeError(f"任务根目录不存在: {tasks_root}")
    if not manifest_path.is_absolute():
        raise DatasetFreezeError(f"冻结 manifest 输出必须为绝对路径: {manifest_path}")
    candidates = sorted(
        (path for path in tasks_root.iterdir() if path.is_dir() and (path / "task.toml").is_file()),
        key=lambda path: path.name,
    )
    if len(candidates) < sample_size:
        raise DatasetFreezeError(f"候选任务不足: 需要 {sample_size}，实际 {len(candidates)}")
    fingerprint = [
        {
            "task_id": path.name,
            "task_toml_hash": hashlib.sha256((path / "task.toml").read_bytes()).hexdigest(),
        }
        for path in candidates
    ]
    candidates_hash = hashlib.sha256(
        json.dumps(fingerprint, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    selected = random.Random(seed).sample(candidates, sample_size)
    manifest = FrozenDatasetManifest(
        1, FREEZE_ALGORITHM, seed, candidates_hash, tuple(sorted(path.name for path in selected))
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest
