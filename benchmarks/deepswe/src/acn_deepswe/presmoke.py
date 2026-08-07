"""多题 pre-smoke 调度：首题 Gate 后才有限并发执行其余任务。"""

from __future__ import annotations

import json
import os
import secrets
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .dataset import FrozenDatasetManifest
from .host_runner import (
    Task1ExecutionConfig,
    Task1HostRunner,
    TaskExecutionError,
    TaskExecutionResult,
)
from .runner import ExperimentManifest


class PresmokeExecutionError(RuntimeError):
    """任一 task 的基础设施或 Gate 失败后的全局 pre-smoke 失败。"""


class _TaskRunner(Protocol):
    def run_task1(self, *, execute: bool = False) -> tuple[object, ...] | TaskExecutionResult: ...


@dataclass(frozen=True)
class PresmokeTaskSpec:
    """一个 task 的独立三臂实验与输出定位。"""

    task_id: str
    experiment: ExperimentManifest
    execution: Task1ExecutionConfig | None
    jobs_directory: Path
    manifest_path: Path


@dataclass(frozen=True)
class PresmokeTaskResult:
    task_id: str
    status: str
    manifest_path: str
    error: str | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "manifest_path": self.manifest_path,
            "error": self.error,
        }


def load_presmoke_task_ids(manifest_path: Path | None = None) -> tuple[str, ...]:
    """读取冻结 manifest 的顺序；第一个 task 是唯一的并发 gate。"""
    path = manifest_path or Path(__file__).resolve().parents[2] / "manifests" / "presmoke-v1.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"无法读取 pre-smoke 冻结 manifest: {path}") from error
    if not isinstance(raw, dict):
        raise ValueError("pre-smoke 冻结 manifest 必须是 JSON 对象")
    task_ids = FrozenDatasetManifest.from_dict(raw).task_ids
    if len(task_ids) < 2:
        raise ValueError("pre-smoke 冻结 manifest 必须包含至少两个不同 task")
    return task_ids


class PresmokeHostRunner:
    """Task1 完整 Gate 成功后，有限并发执行剩余 task 的三臂流程。"""

    def __init__(
        self,
        task_specs: tuple[PresmokeTaskSpec, ...],
        aggregate_manifest_path: Path,
        *,
        task_workers: int = 1,
        frozen_task_ids: tuple[str, ...] | None = None,
        task_runner_factory: Callable[[PresmokeTaskSpec], _TaskRunner] | None = None,
    ) -> None:
        if task_workers <= 0:
            raise ValueError("task_workers 必须为正整数")
        if not aggregate_manifest_path.is_absolute():
            raise ValueError("aggregate_manifest_path 必须为绝对路径")
        self.task_specs = task_specs
        self.aggregate_manifest_path = aggregate_manifest_path
        self.task_workers = task_workers
        self.frozen_task_ids = (
            load_presmoke_task_ids() if frozen_task_ids is None else frozen_task_ids
        )
        self._validate_specs()
        self._task_runner_factory = task_runner_factory or self._build_task_runner

    def run(self, *, execute: bool = False) -> tuple[PresmokeTaskResult, ...]:
        """执行一次 pre-smoke；不会对任意 task 或 solve 进行重试。"""
        results: dict[str, PresmokeTaskResult] = {}
        first = self.task_specs[0]
        first_result = self._run_task(first, execute)
        results[first.task_id] = first_result
        if first_result.status not in {"passed", "planned"}:
            results[first.task_id] = PresmokeTaskResult(
                first_result.task_id,
                "failed",
                first_result.manifest_path,
                first_result.error or first_result.status,
            )
            ordered = self._ordered_results(results)
            self._write_aggregate(ordered)
            raise PresmokeExecutionError(f"Task1 Gate/基础设施失败: {results[first.task_id].error}")

        remaining = self.task_specs[1:]
        with ThreadPoolExecutor(max_workers=min(self.task_workers, len(remaining))) as executor:
            futures = {
                executor.submit(self._run_task, spec, execute): spec.task_id for spec in remaining
            }
            for future in as_completed(futures):
                result = future.result()
                results[result.task_id] = result

        ordered = self._ordered_results(results)
        self._write_aggregate(ordered)
        failed = [result for result in ordered if result.status == "failed"]
        if failed:
            failed_ids = ",".join(result.task_id for result in failed)
            raise PresmokeExecutionError(f"pre-smoke task 失败: {failed_ids}")
        return ordered

    def _run_task(self, spec: PresmokeTaskSpec, execute: bool) -> PresmokeTaskResult:
        """task runner 必须返回明确执行状态，不能以正常返回推断为通过。"""
        try:
            outcome = self._task_runner_factory(spec).run_task1(execute=execute)
        except TaskExecutionError as error:
            return PresmokeTaskResult(spec.task_id, "failed", str(spec.manifest_path), str(error))
        if not execute:
            return PresmokeTaskResult(spec.task_id, "planned", str(spec.manifest_path), None)
        if not isinstance(outcome, TaskExecutionResult):
            return PresmokeTaskResult(
                spec.task_id,
                "failed",
                str(spec.manifest_path),
                "TASK_RUNNER_DID_NOT_REPORT_EXECUTION_RESULT",
            )
        if outcome.status == "no_eligible_claim":
            return PresmokeTaskResult(
                spec.task_id, "no_eligible_claim", str(spec.manifest_path), None
            )
        if outcome.status != "passed":
            return PresmokeTaskResult(
                spec.task_id,
                "failed",
                str(spec.manifest_path),
                f"TASK_RUNNER_STATUS_INVALID:{outcome.status}",
            )
        return PresmokeTaskResult(spec.task_id, "passed", str(spec.manifest_path), None)

    def _ordered_results(
        self, results: dict[str, PresmokeTaskResult]
    ) -> tuple[PresmokeTaskResult, ...]:
        return tuple(results[spec.task_id] for spec in self.task_specs if spec.task_id in results)

    @staticmethod
    def _build_task_runner(spec: PresmokeTaskSpec) -> Task1HostRunner:
        return Task1HostRunner(spec.experiment, spec.jobs_directory, spec.execution)

    def _validate_specs(self) -> None:
        expected = self.frozen_task_ids
        actual = tuple(spec.task_id for spec in self.task_specs)
        if actual != expected:
            raise ValueError("pre-smoke task 必须覆盖冻结 manifest 的全部任务且保持顺序")
        first_execution = self.task_specs[0].execution
        if first_execution is not None and not first_execution.require_eligible_claim:
            raise ValueError("pre-smoke 首题必须启用 require_eligible_claim hard gate")
        manifest_paths: set[Path] = set()
        attempt_paths: set[Path] = set()
        for spec in self.task_specs:
            attempts = spec.experiment.attempts
            if len(attempts) != 3 or any(attempt.task_id != spec.task_id for attempt in attempts):
                raise ValueError(f"task {spec.task_id} 必须拥有独立的三臂 ExperimentManifest")
            variants = tuple(attempt.variant for attempt in attempts)
            if variants[0] != "A" or set(variants[1:]) != {"B_empty", "B_claim"}:
                raise ValueError(f"task {spec.task_id} 三臂必须是 A 后接 B_empty/B_claim")
            if not spec.jobs_directory.is_absolute() or not spec.manifest_path.is_absolute():
                raise ValueError(f"task {spec.task_id} jobs/manifest 路径必须为绝对路径")
            if spec.manifest_path in manifest_paths:
                raise ValueError("每个 task 必须使用独立 manifest 路径")
            manifest_paths.add(spec.manifest_path)
            for attempt in attempts:
                path = Path(attempt.output_path)
                if path in attempt_paths:
                    raise ValueError("不同 task 的 attempt 输出路径必须完全隔离")
                attempt_paths.add(path)
            if (
                spec.execution is not None
                and spec.execution.manifest_path.resolve() != spec.manifest_path.resolve()
            ):
                raise ValueError(
                    f"task {spec.task_id} execution manifest_path 必须匹配 task manifest_path"
                )

    def _write_aggregate(self, results: tuple[PresmokeTaskResult, ...]) -> None:
        failed = [result for result in results if result.status == "failed"]
        no_eligible_claim = any(result.status == "no_eligible_claim" for result in results)
        if failed:
            status = "failed"
        elif no_eligible_claim:
            status = "completed_with_no_eligible_claim"
        else:
            status = "passed"
        payload = {
            "schema_version": 1,
            "status": status,
            "task_workers": self.task_workers,
            "task_order": list(self.frozen_task_ids),
            "task_results": [result.to_dict() for result in results],
        }
        _atomic_write_json(self.aggregate_manifest_path, payload)


def _atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()
