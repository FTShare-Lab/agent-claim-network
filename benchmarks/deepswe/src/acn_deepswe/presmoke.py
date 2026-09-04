"""多题 pre-smoke 调度：每个 task 独立完成四臂实验后汇总结果。"""

from __future__ import annotations

import hashlib
import json
import math
import os
import secrets
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
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
    """一个 task 的独立四臂实验与输出定位。"""

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
    """读取冻结 manifest 的确定性 task 顺序。"""
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
    """有限并发执行全部 task；claim 缺失仅使带 claim 的 B 臂不适用。"""

    def __init__(
        self,
        task_specs: tuple[PresmokeTaskSpec, ...],
        aggregate_manifest_path: Path,
        *,
        task_workers: int = 1,
        frozen_task_ids: tuple[str, ...] | None = None,
        task_runner_factory: Callable[[PresmokeTaskSpec], _TaskRunner] | None = None,
        completed_task_results: tuple[PresmokeTaskResult, ...] = (),
        completion_manifest_path: Path | None = None,
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
        if completion_manifest_path is not None and not completion_manifest_path.is_absolute():
            raise ValueError("completion_manifest_path 必须为绝对路径")
        self.completion_manifest_path = completion_manifest_path
        self._completed_task_results = completed_task_results
        self._validate_specs()
        # 每题内部有两个并行 arm；共享 semaphore 保证全机活跃 Pier attempt
        # 仍不超过用户配置的 task_workers，而不是把 20 静默放大成 40。
        self._attempt_semaphore = threading.BoundedSemaphore(task_workers)
        self._task_runner_factory = task_runner_factory or self._build_task_runner

    def run(self, *, execute: bool = False) -> tuple[PresmokeTaskResult, ...]:
        """执行一次 pre-smoke；不会对任意 task 或 solve 进行重试。"""
        results = {result.task_id: result for result in self._completed_task_results}
        self._write_completion_manifest(self._ordered_results(results))
        if self.task_specs:
            with ThreadPoolExecutor(
                max_workers=min(self.task_workers, len(self.task_specs))
            ) as executor:
                futures = {
                    executor.submit(self._run_task, spec, execute): spec.task_id
                    for spec in self.task_specs
                }
                for future in as_completed(futures):
                    result = future.result()
                    results[result.task_id] = result
                    self._write_completion_manifest(self._ordered_results(results))

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
        return tuple(results[task_id] for task_id in self.frozen_task_ids if task_id in results)

    def _build_task_runner(self, spec: PresmokeTaskSpec) -> Task1HostRunner:
        return Task1HostRunner(
            spec.experiment,
            spec.jobs_directory,
            spec.execution,
            attempt_semaphore=self._attempt_semaphore,
        )

    def _validate_specs(self) -> None:
        expected = self.frozen_task_ids
        actual = tuple(spec.task_id for spec in self.task_specs)
        completed_ids = tuple(result.task_id for result in self._completed_task_results)
        if len(set(actual)) != len(actual) or len(set(completed_ids)) != len(completed_ids):
            raise ValueError("pre-smoke task 不得重复")
        if set(actual) & set(completed_ids):
            raise ValueError("pre-smoke task 不得同时续用并重新执行")
        covered = set(actual) | set(completed_ids)
        if covered != set(expected):
            raise ValueError("pre-smoke task 必须覆盖冻结 manifest 的全部任务")
        if tuple(task_id for task_id in expected if task_id in actual) != actual:
            raise ValueError("pre-smoke task 必须保持冻结 manifest 顺序")
        if tuple(task_id for task_id in expected if task_id in completed_ids) != completed_ids:
            raise ValueError("pre-smoke task 必须保持冻结 manifest 顺序")
        if any(
            result.status not in {"passed", "no_eligible_claim"}
            for result in self._completed_task_results
        ):
            raise ValueError("续用 task 必须是可保留的成功或无可用 claim 终态")
        manifest_paths: set[Path] = set()
        attempt_paths: set[Path] = set()
        for spec in self.task_specs:
            attempts = spec.experiment.attempts
            if len(attempts) != 4 or any(attempt.task_id != spec.task_id for attempt in attempts):
                raise ValueError(f"task {spec.task_id} 必须拥有独立的四臂 ExperimentManifest")
            variants = tuple(attempt.variant for attempt in attempts)
            if variants[0] != "A" or set(variants[1:]) != {"B_empty", "B_claim", "B_forced_claim"}:
                raise ValueError(
                    f"task {spec.task_id} 四臂必须是 A 后接 B_empty/B_claim/B_forced_claim"
                )
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

    def _write_completion_manifest(self, results: tuple[PresmokeTaskResult, ...]) -> None:
        """持久化所有 task 终态，防止普通续跑静默抹去 Gate 或协议失败。"""
        if self.completion_manifest_path is None:
            return
        existing = _read_completion_manifest(self.completion_manifest_path)
        interrupted_retries = existing.get("interrupted_retries", {})
        if not isinstance(interrupted_retries, Mapping):
            interrupted_retries = {}
        terminal = [
            result
            for result in results
            if result.status in {"passed", "no_eligible_claim", "failed"}
        ]
        payload = {
            "schema_version": 2,
            "status": "completed" if len(terminal) == len(self.frozen_task_ids) else "in_progress",
            "task_order": list(self.frozen_task_ids),
            # 保留旧字段，供已有只读归档继续识别完整、Gate 通过的 task。
            "completed_tasks": [
                result.to_dict() for result in terminal if result.status == "passed"
            ],
            "task_results": [result.to_dict() for result in terminal],
            "interrupted_retries": dict(interrupted_retries),
        }
        _atomic_write_json(self.completion_manifest_path, payload)

    def _write_aggregate(self, results: tuple[PresmokeTaskResult, ...]) -> None:
        failed = [result for result in results if result.status == "failed"]
        no_eligible_claim = any(result.status == "no_eligible_claim" for result in results)
        if failed:
            status = "failed"
        elif no_eligible_claim:
            status = "completed_with_no_eligible_claim"
        else:
            status = "passed"
        cohort_metrics = _cohort_metrics(results)
        producer_variants = sorted(
            {str(row["claim_producer_variant"]) for row in cohort_metrics.rows.values()}
        )
        payload = {
            "schema_version": 1,
            "status": status,
            "task_workers": self.task_workers,
            "task_order": list(self.frozen_task_ids),
            "task_results": [result.to_dict() for result in results],
            "claim_funnel": _claim_funnel(results),
            "claim_producer_variants": producer_variants,
            "cohort_metrics": cohort_metrics.rows,
            "cohort_coverage": cohort_metrics.coverage_dict(self.frozen_task_ids),
            "cohort_definitions": {
                "success_efficiency": (
                    "selected producer verifier passed and produced at least one frozen claim; "
                    "primary cohort for claim-assisted success and efficiency comparisons"
                ),
                "failure_recovery": (
                    "selected producer verifier failed but produced at least one frozen claim; "
                    "reported separately as recovery from unverified producer output"
                ),
                "unpaired_no_claim": (
                    "selected producer produced no frozen claim; claim arms are not paired"
                ),
                "failed_producer_quarantine": (
                    "selected producer failed verification and its claims were quarantined; "
                    "all arm outcomes remain counted, without claim-effect pairs"
                ),
            },
            "paired_metric_definitions": {
                "paired_against_producer": (
                    "consumer arm minus selected producer arm on the same task"
                ),
                "paired_against_no_claim_baseline": (
                    "claim arm minus the non-producer arm that received no claim on the same "
                    "task; wins/losses count discordant verifier outcomes and exact_mcnemar_p "
                    "is the two-sided exact binomial test on them"
                ),
            },
            "usage_metric_definitions": {
                "token_totals_and_means": (
                    "observed values; lower bounds when incomplete_model_responses is non-zero"
                ),
                "paired_token_deltas": (
                    "consumer observed value minus selected producer observed value; either side "
                    "may be a lower bound "
                    "when token_delta_includes_observed_lower_bound is true"
                ),
            },
        }
        _atomic_write_json(self.aggregate_manifest_path, payload)


def load_terminal_task_results(
    task_specs: tuple[PresmokeTaskSpec, ...], completion_manifest_path: Path
) -> tuple[PresmokeTaskResult, ...]:
    """读取 checkpoint 中的终态；失败也必须被保留而非被普通续跑覆盖。"""
    records = _completion_manifest_records(completion_manifest_path)
    terminal: list[PresmokeTaskResult] = []
    for spec in task_specs:
        record = records.get(spec.task_id)
        # checkpoint 可能滞后或指向旧版续跑生成的成功记录；总是检查所有执行目录。
        paths = [spec.manifest_path] + [
            root / "tasks" / spec.task_id / "manifest.json"
            for root in sorted((completion_manifest_path.parent / "resumes").glob("resume-*"))
        ]
        if record is not None:
            recorded_path = record.get("manifest_path")
            if isinstance(recorded_path, str) and Path(recorded_path).is_absolute():
                paths.append(Path(recorded_path))
        recovered = [
            result
            for path in {path.resolve(): path for path in paths}.values()
            if (result := _terminal_result_from_task_manifest(spec, path)) is not None
        ]
        if len(recovered) > 1:
            raise ValueError(f"task 存在多个终态，拒绝选择性复用: {spec.task_id}")
        if record is None:
            terminal.extend(recovered)
            continue
        status = record.get("status")
        if recovered and status != recovered[0].status:
            raise ValueError(f"checkpoint 与 task manifest 终态冲突: {spec.task_id}")
        manifest_path = record.get("manifest_path")
        error = record.get("error")
        if not isinstance(manifest_path, str) or not Path(manifest_path).is_absolute():
            continue
        if status == "passed":
            if (
                _is_completed_task_manifest(spec, Path(manifest_path))
                or _is_a_only_task_manifest(spec, Path(manifest_path))
                or _is_producer_pair_task_manifest(spec, Path(manifest_path))
            ):
                terminal.append(
                    PresmokeTaskResult(
                        spec.task_id,
                        status,
                        manifest_path,
                        error if isinstance(error, str) else None,
                    )
                )
        elif status == "no_eligible_claim":
            if _is_no_eligible_claim_task_manifest(spec, Path(manifest_path)):
                terminal.append(
                    PresmokeTaskResult(
                        spec.task_id,
                        status,
                        manifest_path,
                        error if isinstance(error, str) else None,
                    )
                )
        elif status == "failed":
            terminal.append(
                PresmokeTaskResult(
                    spec.task_id,
                    "failed",
                    manifest_path,
                    error if isinstance(error, str) else "TASK_TERMINAL_FAILURE",
                )
            )
    return tuple(terminal)


def load_completed_task_results(
    task_specs: tuple[PresmokeTaskSpec, ...], completion_manifest_path: Path
) -> tuple[PresmokeTaskResult, ...]:
    """兼容旧调用：只返回有四臂有效 Gate 的可复用成功 task。"""
    return tuple(
        result
        for result in load_terminal_task_results(task_specs, completion_manifest_path)
        if result.status == "passed"
    )


def _completion_manifest_records(path: Path) -> dict[str, Mapping[str, object]]:
    raw = _read_completion_manifest(path)
    records = raw.get("task_results")
    if not isinstance(records, list):
        records = raw.get("completed_tasks")
    if not isinstance(records, list):
        return {}
    candidates: dict[str, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            continue
        task_id = record.get("task_id")
        if isinstance(task_id, str) and task_id:
            candidates[task_id] = record
    return candidates


def reserve_interrupted_retries(completion_manifest_path: Path, task_ids: tuple[str, ...]) -> None:
    """为被中断且无终态的 task 预留唯一一次显式重跑机会。"""
    if not completion_manifest_path.is_absolute():
        raise ValueError("completion_manifest_path 必须为绝对路径")
    raw = _read_completion_manifest(completion_manifest_path)
    retries_raw = raw.get("interrupted_retries", {})
    if not isinstance(retries_raw, Mapping):
        raise ValueError("task-completions interrupted_retries 必须是对象")
    retries: dict[str, int] = {}
    for task_id, count in retries_raw.items():
        if not isinstance(task_id, str) or isinstance(count, bool) or not isinstance(count, int):
            raise ValueError("task-completions interrupted_retries 无效")
        retries[task_id] = count
    exhausted = [task_id for task_id in task_ids if retries.get(task_id, 0) >= 1]
    if exhausted:
        raise ValueError(f"中断 task 已用尽唯一续跑机会: {','.join(exhausted)}")
    for task_id in task_ids:
        retries[task_id] = retries.get(task_id, 0) + 1
    raw["schema_version"] = 2
    raw["status"] = "in_progress"
    raw["interrupted_retries"] = retries
    raw.setdefault("task_order", [])
    raw.setdefault("completed_tasks", [])
    raw.setdefault("task_results", [])
    _atomic_write_json(completion_manifest_path, raw)


def _read_completion_manifest(path: Path) -> dict[str, object]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _is_completed_task_manifest(spec: PresmokeTaskSpec, path: Path) -> bool:
    """完成定义为四臂均有成功 Gate；agent 得分失败仍是有效实验观测。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        not isinstance(raw, Mapping)
        or raw.get("failure") is not None
        or not _task_manifest_matches_provenance(spec, raw)
    ):
        return False
    records = raw.get("attempt_results")
    if not isinstance(records, list) or len(records) != 4:
        return False
    expected = {attempt.attempt_id: attempt.variant for attempt in spec.experiment.attempts}
    observed: dict[str, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            return False
        attempt_id = record.get("attempt_id")
        variant = record.get("variant")
        if (
            not isinstance(attempt_id, str)
            or expected.get(attempt_id) != variant
            or attempt_id in observed
            or record.get("status") not in {"passed", "agent_failed"}
        ):
            return False
        observed[attempt_id] = record
    if set(observed) != set(expected):
        return False
    if not all(
        _has_valid_gated_attempt_evidence(attempt_id, expected[attempt_id], record)
        for attempt_id, record in observed.items()
    ):
        return False
    return True


def _terminal_result_from_task_manifest(
    spec: PresmokeTaskSpec, path: Path
) -> PresmokeTaskResult | None:
    """从 checkpoint 尚未来得及写入时留下的原子 task manifest 恢复终态。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, Mapping) or not isinstance(raw.get("attempt_results"), list):
        return None
    failure = raw.get("failure")
    if isinstance(failure, str) and failure:
        return PresmokeTaskResult(spec.task_id, "failed", str(path), failure)
    if _is_completed_task_manifest(spec, path):
        return PresmokeTaskResult(spec.task_id, "passed", str(path), None)
    if _is_a_only_task_manifest(spec, path):
        return PresmokeTaskResult(spec.task_id, "passed", str(path), None)
    if _is_producer_pair_task_manifest(spec, path):
        return PresmokeTaskResult(spec.task_id, "passed", str(path), None)
    if _is_no_eligible_claim_task_manifest(spec, path):
        return PresmokeTaskResult(spec.task_id, "no_eligible_claim", str(path), None)
    return None


def _is_a_only_task_manifest(spec: PresmokeTaskSpec, path: Path) -> bool:
    """确认只跑了 A 并完成 Gate，其余臂按 A-only 协议未运行；freeze 仍会留下 claim bundle。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        not isinstance(raw, Mapping)
        or raw.get("failure") is not None
        or not _task_manifest_matches_provenance(spec, raw)
    ):
        return False
    records = raw.get("attempt_results")
    if not isinstance(records, list) or len(records) != 4:
        return False
    expected = {attempt.attempt_id: attempt.variant for attempt in spec.experiment.attempts}
    observed: dict[str, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            return False
        attempt_id = record.get("attempt_id")
        variant = record.get("variant")
        if (
            not isinstance(attempt_id, str)
            or expected.get(attempt_id) != variant
            or attempt_id in observed
        ):
            return False
        observed[attempt_id] = record
    if set(observed) != set(expected):
        return False
    for attempt_id, variant in expected.items():
        record = observed[attempt_id]
        if variant == "A":
            if record.get("status") not in {
                "passed",
                "agent_failed",
            } or not _has_valid_gated_attempt_evidence(attempt_id, variant, record):
                return False
            continue
        if (
            record.get("status") != "not_run"
            or record.get("reason") != "A_ONLY"
            or record.get("result_path") is not None
            or record.get("gate_path") is not None
        ):
            return False
    return True


def _is_producer_pair_task_manifest(spec: PresmokeTaskSpec, path: Path) -> bool:
    """确认 S1=A、S2=B_empty 均完成 Gate，claim consumers 尚未启动。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        not isinstance(raw, Mapping)
        or raw.get("failure") is not None
        or not _task_manifest_matches_provenance(spec, raw)
    ):
        return False
    execution = raw.get("execution")
    if not isinstance(execution, Mapping) or execution.get("phase_mode") != "adaptive_producers":
        return False
    records = raw.get("attempt_results")
    if not isinstance(records, list) or len(records) != 4:
        return False
    expected = {attempt.attempt_id: attempt.variant for attempt in spec.experiment.attempts}
    observed = {
        record.get("attempt_id"): record
        for record in records
        if isinstance(record, Mapping) and isinstance(record.get("attempt_id"), str)
    }
    if set(observed) != set(expected):
        return False
    for attempt_id, variant in expected.items():
        record = observed[attempt_id]
        if record.get("variant") != variant:
            return False
        if variant in {"A", "B_empty"}:
            if record.get("status") not in {
                "passed",
                "agent_failed",
            } or not _has_valid_gated_attempt_evidence(attempt_id, variant, record):
                return False
            continue
        if (
            record.get("status") != "not_run"
            or record.get("reason") != "PRODUCER_PAIR_ONLY"
            or record.get("result_path") is not None
            or record.get("gate_path") is not None
        ):
            return False
    return True


def _is_no_eligible_claim_task_manifest(spec: PresmokeTaskSpec, path: Path) -> bool:
    """确认 A/B_empty 已通过 Gate，两个 claim arm 按协议未运行。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        not isinstance(raw, Mapping)
        or raw.get("failure") is not None
        or not _task_manifest_matches_provenance(spec, raw)
    ):
        return False
    records = raw.get("attempt_results")
    if not isinstance(records, list) or len(records) != 4:
        return False
    expected = {attempt.attempt_id: attempt.variant for attempt in spec.experiment.attempts}
    observed: dict[str, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            return False
        attempt_id = record.get("attempt_id")
        variant = record.get("variant")
        if (
            not isinstance(attempt_id, str)
            or expected.get(attempt_id) != variant
            or attempt_id in observed
        ):
            return False
        observed[attempt_id] = record
    if set(observed) != set(expected):
        return False
    for attempt_id, variant in expected.items():
        record = observed[attempt_id]
        if variant in {"B_claim", "B_forced_claim"}:
            if (
                record.get("status") != "not_run"
                or record.get("reason") != "NO_ELIGIBLE_CLAIM"
                or record.get("result_path") is not None
                or record.get("gate_path") is not None
            ):
                return False
        elif record.get("status") not in {
            "passed",
            "agent_failed",
        } or not _has_valid_gated_attempt_evidence(attempt_id, variant, record):
            return False
    return True


def _task_manifest_matches_provenance(spec: PresmokeTaskSpec, raw: Mapping[str, object]) -> bool:
    """resume 只能复用与当前 spec 完整 provenance 一致的旧 task。"""
    experiment = raw.get("experiment")
    provenance = experiment.get("provenance") if isinstance(experiment, Mapping) else None
    return provenance == spec.experiment.provenance.to_dict()


def _has_valid_gated_attempt_evidence(
    attempt_id: str, variant: str, record: Mapping[str, object]
) -> bool:
    """确认一个已运行 arm 的 result 与 Gate 都与冻结 attempt 对齐。"""
    result_path = record.get("result_path")
    gate_path = record.get("gate_path")
    result_hash = record.get("result_hash")
    gate_hash = record.get("gate_hash")
    if not all(
        isinstance(value, str) and value
        for value in (result_path, gate_path, result_hash, gate_hash)
    ):
        return False
    result_file = Path(result_path)
    gate_file = Path(gate_path)
    if (
        _sha256_file_if_present(result_file) != result_hash
        or _sha256_file_if_present(gate_file) != gate_hash
    ):
        return False
    try:
        result = json.loads(result_file.read_text(encoding="utf-8"))
        gate = json.loads(gate_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        isinstance(result, Mapping)
        and result.get("attempt_id") == attempt_id
        and result.get("variant") == variant
        and isinstance(gate, Mapping)
        and gate.get("attempt_id") == attempt_id
        and gate.get("decision") == "pass"
    )


def _claim_funnel(results: tuple[PresmokeTaskResult, ...]) -> dict[str, dict[str, int]]:
    """从每个 task 的已落盘记录汇总，不从模型文本或结果分数反推 claim 消费。"""
    fields = (
        "attempts",
        "bundle_available",
        "retrieved",
        "injected",
        "used",
        "delivery_evidence_count",
        "injected_claim_count",
        "used_claim_count",
    )
    funnel = {variant: {field: 0 for field in fields} for variant in ("B_claim", "B_forced_claim")}
    for result in results:
        path = Path(result.manifest_path)
        if not path.is_file():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        records = raw.get("attempt_results") if isinstance(raw, dict) else None
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            variant = record.get("variant")
            observation = record.get("claim_observation")
            if variant not in funnel or not isinstance(observation, dict):
                continue
            funnel[variant]["attempts"] += 1
            for name in ("bundle_available", "retrieved", "injected", "used"):
                if observation.get(name) is True:
                    funnel[variant][name] += 1
            for name in (
                "delivery_evidence_count",
                "injected_claim_count",
                "used_claim_count",
            ):
                value = observation.get(name)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    funnel[variant][name] += value
    return funnel


_METRIC_VARIANTS = ("A", "B_empty", "B_claim", "B_forced_claim")
_CLAIM_VARIANTS = ("B_claim", "B_forced_claim")
_REQUEST_USAGE_FIELDS = (
    "model_requests",
    "turn_model_requests",
    "finalize_model_requests",
    "complete_model_responses",
    "incomplete_model_responses",
)
_TOKEN_USAGE_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "reasoning_tokens",
)
_USAGE_FIELDS = _REQUEST_USAGE_FIELDS + _TOKEN_USAGE_FIELDS
_COHORTS = (
    "success_efficiency",
    "failure_recovery",
    "unpaired_no_claim",
    "failed_producer_quarantine",
)


@dataclass(frozen=True)
class _AttemptMetrics:
    verifier_passed: bool
    bundle_available: bool | None
    usage: Mapping[str, int]


class _CohortExclusion(Exception):
    """task 不能进入 cohort 统计；reason 写入 aggregate，保证分母变化可见。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class _VariantTotals:
    attempts: int = 0
    verifier_passed: int = 0
    incomplete_usage_attempts: int = 0
    empty_claim_bundle_attempts: int = 0
    usage_totals: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_USAGE_FIELDS, 0))

    def add(self, metrics: _AttemptMetrics) -> None:
        self.attempts += 1
        self.verifier_passed += int(metrics.verifier_passed)
        self.incomplete_usage_attempts += int(bool(metrics.usage["incomplete_model_responses"]))
        self.empty_claim_bundle_attempts += int(metrics.bundle_available is False)
        for name in _USAGE_FIELDS:
            self.usage_totals[name] += metrics.usage[name]

    def to_dict(self) -> dict[str, object]:
        attempts = self.attempts
        return {
            "attempts": attempts,
            "verifier_passed": self.verifier_passed,
            "verifier_pass_rate": self.verifier_passed / attempts if attempts else None,
            "incomplete_usage_attempts": self.incomplete_usage_attempts,
            "incomplete_usage_attempt_rate": (
                self.incomplete_usage_attempts / attempts if attempts else None
            ),
            "token_values_are_observed_lower_bound": self.incomplete_usage_attempts > 0,
            "empty_claim_bundle_attempts": self.empty_claim_bundle_attempts,
            "usage_totals": dict(self.usage_totals),
            "usage_means": {
                name: (total / attempts if attempts else None)
                for name, total in self.usage_totals.items()
            },
        }


@dataclass
class _PairTotals:
    """同题配对：subject 减 reference。wins/losses 是 verifier 结果不一致的配对数。"""

    pairs: int = 0
    wins: int = 0
    losses: int = 0
    pairs_with_incomplete_usage: int = 0
    usage_delta_totals: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(_USAGE_FIELDS, 0)
    )

    def add(self, subject: _AttemptMetrics, reference: _AttemptMetrics) -> None:
        self.pairs += 1
        self.wins += int(subject.verifier_passed and not reference.verifier_passed)
        self.losses += int(reference.verifier_passed and not subject.verifier_passed)
        self.pairs_with_incomplete_usage += int(
            bool(subject.usage["incomplete_model_responses"])
            or bool(reference.usage["incomplete_model_responses"])
        )
        for name in _USAGE_FIELDS:
            self.usage_delta_totals[name] += subject.usage[name] - reference.usage[name]

    def to_dict(self) -> dict[str, object]:
        pairs = self.pairs
        delta = self.wins - self.losses
        return {
            "pairs": pairs,
            "wins": self.wins,
            "losses": self.losses,
            "verifier_passed_delta": delta,
            "verifier_pass_rate_delta": delta / pairs if pairs else None,
            "exact_mcnemar_p": exact_mcnemar_p(self.wins, self.losses),
            "pairs_with_incomplete_usage": self.pairs_with_incomplete_usage,
            "pairs_with_incomplete_usage_rate": (
                self.pairs_with_incomplete_usage / pairs if pairs else None
            ),
            "token_delta_includes_observed_lower_bound": self.pairs_with_incomplete_usage > 0,
            "usage_delta_totals": dict(self.usage_delta_totals),
            "usage_delta_means": {
                name: (total / pairs if pairs else None)
                for name, total in self.usage_delta_totals.items()
            },
        }


def exact_mcnemar_p(wins: int, losses: int) -> float | None:
    """不一致配对上的双侧精确二项检验（p=0.5）；无不一致配对时无法检验，返回 None。"""
    discordant = wins + losses
    if discordant == 0:
        return None
    smaller = min(wins, losses)
    tail = sum(math.comb(discordant, index) for index in range(smaller + 1)) / 2**discordant
    return min(1.0, 2 * tail)


@dataclass
class _CohortRow:
    claim_producer_variant: str
    task_ids: list[str] = field(default_factory=list)
    variants: dict[str, _VariantTotals] = field(
        default_factory=lambda: {variant: _VariantTotals() for variant in _METRIC_VARIANTS}
    )
    paired_against_producer: dict[str, _PairTotals] = field(default_factory=dict)
    paired_against_no_claim_baseline: dict[str, _PairTotals] = field(
        default_factory=lambda: {variant: _PairTotals() for variant in _CLAIM_VARIANTS}
    )

    def __post_init__(self) -> None:
        self.paired_against_producer = {
            variant: _PairTotals()
            for variant in _METRIC_VARIANTS
            if variant != self.claim_producer_variant
        }

    @property
    def no_claim_baseline_variant(self) -> str:
        """未拿到 claim 的非 producer 臂，是 claim arm 的同题对照。"""
        return "B_empty" if self.claim_producer_variant == "A" else "A"

    def add(self, task_id: str, cohort: str, attempts: Mapping[str, _AttemptMetrics]) -> None:
        self.task_ids.append(task_id)
        for variant, metrics in attempts.items():
            self.variants[variant].add(metrics)
        if cohort in {"unpaired_no_claim", "failed_producer_quarantine"}:
            return
        producer = attempts[self.claim_producer_variant]
        for variant, pair in self.paired_against_producer.items():
            pair.add(attempts[variant], producer)
        baseline = attempts[self.no_claim_baseline_variant]
        for variant, pair in self.paired_against_no_claim_baseline.items():
            pair.add(attempts[variant], baseline)

    def to_dict(self) -> dict[str, object]:
        return {
            "task_count": len(self.task_ids),
            "task_ids": list(self.task_ids),
            "claim_producer_variant": self.claim_producer_variant,
            "no_claim_baseline_variant": self.no_claim_baseline_variant,
            "variants": {variant: totals.to_dict() for variant, totals in self.variants.items()},
            "paired_against_producer": {
                variant: pair.to_dict() for variant, pair in self.paired_against_producer.items()
            },
            "paired_against_no_claim_baseline": {
                variant: pair.to_dict()
                for variant, pair in self.paired_against_no_claim_baseline.items()
            },
        }


@dataclass(frozen=True)
class CohortMetrics:
    rows: dict[str, dict[str, object]]
    excluded_tasks: dict[str, str]

    def coverage_dict(self, planned_task_ids: tuple[str, ...]) -> dict[str, object]:
        included = sum(int(row["task_count"]) for row in self.rows.values())
        return {
            "planned_task_count": len(planned_task_ids),
            "included_task_count": included,
            "excluded_task_count": len(self.excluded_tasks),
            "excluded_tasks": dict(self.excluded_tasks),
        }


def _cohort_metrics(results: tuple[PresmokeTaskResult, ...]) -> CohortMetrics:
    """按 producer verifier cohort 汇总成功率、用量与同题配对差值；被排除的 task 逐一记录原因。"""
    cohorts: dict[str, _CohortRow] = {}
    excluded: dict[str, str] = {}
    for task in results:
        try:
            cohort, producer_variant, attempts = _validated_cohort_attempts(task)
        except _CohortExclusion as exclusion:
            excluded[task.task_id] = exclusion.reason
            continue
        row = cohorts.setdefault(cohort, _CohortRow(producer_variant))
        if row.claim_producer_variant != producer_variant:
            raise ValueError(f"同一 cohort 混入多个 claim producer: {cohort}")
        row.add(task.task_id, cohort, attempts)
    return CohortMetrics({cohort: row.to_dict() for cohort, row in cohorts.items()}, excluded)


def _validated_cohort_attempts(
    task: PresmokeTaskResult,
) -> tuple[str, str, dict[str, _AttemptMetrics]]:
    """汇总 Gate 与哈希闭合的已执行臂；无 claim 时保留两个 baseline，不伪造 claim 配对。"""
    if task.status not in {"passed", "no_eligible_claim"} or task.error is not None:
        raise _CohortExclusion(f"task_status={task.status}")
    manifest = _read_mapping_if_present(Path(task.manifest_path))
    if manifest is None:
        raise _CohortExclusion("task_manifest_unreadable")
    if manifest.get("failure") is not None:
        raise _CohortExclusion(f"task_failure={manifest['failure']}")
    cohort = manifest.get("experiment_cohort")
    producer_variant = manifest.get("claim_producer_variant", "A")
    logical_variant_map = manifest.get("logical_variant_map")
    execution = manifest.get("execution")
    records = manifest.get("attempt_results")
    if cohort not in _COHORTS:
        raise _CohortExclusion(f"experiment_cohort={cohort}")
    if producer_variant not in {"A", "B_empty"} or (
        isinstance(execution, Mapping)
        and execution.get("claim_producer_variant", producer_variant) != producer_variant
    ):
        raise _CohortExclusion("claim_producer_variant_inconsistent")
    if not isinstance(records, list) or len(records) != len(_METRIC_VARIANTS):
        raise _CohortExclusion("attempt_results_incomplete")
    attempts: dict[str, _AttemptMetrics] = {}
    seen_variants: set[str] = set()
    for record in records:
        if not isinstance(record, Mapping):
            raise _CohortExclusion("attempt_record_invalid")
        attempt_id = record.get("attempt_id")
        variant = record.get("variant")
        if (
            not isinstance(attempt_id, str)
            or not attempt_id
            or variant not in _METRIC_VARIANTS
            or variant in seen_variants
        ):
            raise _CohortExclusion("attempt_record_invalid")
        seen_variants.add(variant)
        if (
            task.status == "no_eligible_claim"
            and cohort in {"unpaired_no_claim", "failed_producer_quarantine"}
            and variant in _CLAIM_VARIANTS
            and record.get("status") == "not_run"
            and record.get("reason") == "NO_ELIGIBLE_CLAIM"
        ):
            continue
        if record.get("status") not in {"passed", "agent_failed"}:
            raise _CohortExclusion(f"{variant}_status={record.get('status')}")
        result_path = record.get("result_path")
        if not isinstance(result_path, str) or not _has_valid_gated_attempt_evidence(
            attempt_id, variant, record
        ):
            raise _CohortExclusion(f"{variant}_gate_evidence_invalid")
        attempt_result = _read_mapping_if_present(Path(result_path))
        if attempt_result is None:
            raise _CohortExclusion(f"{variant}_result_unreadable")
        verifier_passed = attempt_result.get("verifier_passed")
        usage = attempt_result.get("usage")
        if not isinstance(verifier_passed, bool) or record.get("verifier_passed") != verifier_passed:
            raise _CohortExclusion(f"{variant}_verifier_passed_inconsistent")
        if not isinstance(usage, Mapping):
            raise _CohortExclusion(f"{variant}_usage_missing")
        usage_values: dict[str, int] = {}
        for name in _USAGE_FIELDS:
            value = usage.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise _CohortExclusion(f"{variant}_usage_field_invalid={name}")
            usage_values[name] = value
        if (
            usage_values["complete_model_responses"] + usage_values["incomplete_model_responses"]
            != usage_values["model_requests"]
        ):
            raise _CohortExclusion(f"{variant}_usage_request_count_mismatch")
        if (
            usage_values["turn_model_requests"] + usage_values["finalize_model_requests"]
            != usage_values["model_requests"]
        ):
            raise _CohortExclusion(f"{variant}_usage_phase_count_mismatch")
        bundle_available: bool | None = None
        if variant in _CLAIM_VARIANTS:
            observation = record.get("claim_observation")
            if not isinstance(observation, Mapping) or not isinstance(
                observation.get("bundle_available"), bool
            ):
                raise _CohortExclusion(f"{variant}_claim_observation_missing")
            bundle_available = observation["bundle_available"]
        attempts[variant] = _AttemptMetrics(verifier_passed, bundle_available, usage_values)
    expected_variants = (
        {"A", "B_empty"} if task.status == "no_eligible_claim" else set(_METRIC_VARIANTS)
    )
    if set(attempts) != expected_variants:
        raise _CohortExclusion("attempt_results_incomplete")
    expected_cohort = (
        "success_efficiency" if attempts[producer_variant].verifier_passed else "failure_recovery"
    )
    if cohort == "failed_producer_quarantine":
        if attempts[producer_variant].verifier_passed or any(
            attempts[variant].bundle_available for variant in _CLAIM_VARIANTS if variant in attempts
        ):
            raise _CohortExclusion("quarantined_producer_or_claim_bundle_inconsistent")
    elif cohort != "unpaired_no_claim" and cohort != expected_cohort:
        raise _CohortExclusion(f"experiment_cohort={cohort}_but_producer_verifier_says={expected_cohort}")
    if logical_variant_map is not None:
        if (
            not isinstance(logical_variant_map, Mapping)
            or set(logical_variant_map) != set(_METRIC_VARIANTS)
            or not all(
                isinstance(physical_variant, str)
                for physical_variant in logical_variant_map.values()
            )
            or set(logical_variant_map.values()) != set(_METRIC_VARIANTS)
            or logical_variant_map.get("A") != producer_variant
            or logical_variant_map.get("B_claim") != "B_claim"
            or logical_variant_map.get("B_forced_claim") != "B_forced_claim"
        ):
            raise _CohortExclusion("logical_variant_map_invalid")
        attempts = {
            logical_variant: attempts[physical_variant]
            for logical_variant, physical_variant in logical_variant_map.items()
            if physical_variant in attempts
        }
        producer_variant = "A"
    return cohort, producer_variant, attempts


def _sha256_file_if_present(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _read_mapping_if_present(path: Path) -> Mapping[str, object] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, Mapping) else None


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
