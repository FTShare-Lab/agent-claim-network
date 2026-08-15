"""按冻结 Pier checkout 的 `TrialResult` 产物读取 verifier 与 trial 路径。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .schemas import VerifierResult


class PierResultError(ValueError):
    """Pier 的单次 trial 产物不完整或不符合冻结 schema。"""


@dataclass(frozen=True)
class PierTrialEvidence:
    """来自 `pier.models.trial.result.TrialResult` 的最小审计投影。"""

    result_path: Path
    trial_name: str
    trial_uri: str
    task_name: str
    task_checksum: str
    verifier_rewards: dict[str, float | int] | None

    def to_dict(self) -> dict[str, object]:
        return {
            "result_path": str(self.result_path),
            "trial_name": self.trial_name,
            "trial_uri": self.trial_uri,
            "task_name": self.task_name,
            "task_checksum": self.task_checksum,
            "verifier_rewards": self.verifier_rewards,
        }

    def verifier_for(self, attempt_id: str) -> VerifierResult:
        """null verifier_result 表示未运行；其余值已在读取边界完成校验。"""
        passed = self.verifier_rewards is not None and self.verifier_rewards["reward"] == 1
        return VerifierResult(
            schema_version=1,
            attempt_id=attempt_id,
            verifier_exit_code=0 if self.verifier_rewards is not None else 1,
            passed=passed,
            result_path=str(self.result_path.resolve()),
            timestamp_utc=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )


def read_single_trial_evidence(job_directory: Path) -> tuple[Path, PierTrialEvidence]:
    """读取 JobConfig 产生的唯一 trial；一条 solve 只接受一个 trial `result.json`。"""
    if not job_directory.is_absolute() or not job_directory.is_dir():
        raise PierResultError(f"Pier job 目录必须是存在的绝对路径: {job_directory}")
    trial_results = sorted(
        child / "result.json"
        for child in job_directory.iterdir()
        if child.is_dir() and (child / "result.json").is_file()
    )
    if len(trial_results) != 1:
        raise PierResultError(
            f"Pier 单次 solve 必须恰有一个 trial result.json，实际为 {len(trial_results)}: {job_directory}"
        )
    result_path = trial_results[0]
    raw = _read_object(result_path)
    # 以下字段来自冻结 Pier 的 TrialResult 必填字段，避免根据目录名猜测产物。
    task_name = _string(raw, "task_name")
    trial_name = _string(raw, "trial_name")
    trial_uri = _string(raw, "trial_uri")
    task_checksum = _string(raw, "task_checksum")
    if not isinstance(raw.get("config"), Mapping):
        raise PierResultError("TrialResult.config 必须是对象")
    if not isinstance(raw.get("agent_info"), Mapping):
        raise PierResultError("TrialResult.agent_info 必须是对象")
    verifier_rewards = _verifier_rewards(raw.get("verifier_result"))
    return result_path.parent, PierTrialEvidence(
        result_path=result_path.resolve(),
        trial_name=trial_name,
        trial_uri=trial_uri,
        task_name=task_name,
        task_checksum=task_checksum,
        verifier_rewards=verifier_rewards,
    )


def _read_object(path: Path) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PierResultError(f"无法读取 Pier TrialResult: {path}") from error
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise PierResultError("Pier TrialResult 必须是 JSON 对象")
    return raw


def _string(raw: Mapping[str, object], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise PierResultError(f"TrialResult.{field} 必须是非空字符串")
    return value


def _verifier_rewards(value: object) -> dict[str, float | int] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise PierResultError("TrialResult.verifier_result 必须是对象或 null")
    rewards = value.get("rewards")
    if not isinstance(rewards, Mapping) or not all(
        isinstance(key, str) and isinstance(item, (int, float)) and not isinstance(item, bool)
        for key, item in rewards.items()
    ):
        raise PierResultError("TrialResult.verifier_result.rewards 必须是数值对象")
    if rewards.get("reward") not in {0, 1}:
        raise PierResultError("TrialResult.verifier_result.rewards.reward 必须是 0 或 1")
    return {str(key): item for key, item in rewards.items()}
