"""多题 pre-smoke 调度：每个 task 独立完成四臂实验后汇总结果。"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from collections.abc import Callable, Mapping
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
        return tuple(
            results[task_id] for task_id in self.frozen_task_ids if task_id in results
        )

    @staticmethod
    def _build_task_runner(spec: PresmokeTaskSpec) -> Task1HostRunner:
        return Task1HostRunner(spec.experiment, spec.jobs_directory, spec.execution)

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
            if variants[0] != "A" or set(variants[1:]) != {
                "B_empty", "B_claim", "B_forced_claim"
            }:
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
            "completed_tasks": [result.to_dict() for result in terminal if result.status == "passed"],
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
        payload = {
            "schema_version": 1,
            "status": status,
            "task_workers": self.task_workers,
            "task_order": list(self.frozen_task_ids),
            "task_results": [result.to_dict() for result in results],
            "claim_funnel": _claim_funnel(results),
            "cohort_metrics": _cohort_metrics(results),
            "cohort_definitions": {
                "success_efficiency": (
                    "A verifier passed and A produced at least one frozen claim; "
                    "primary cohort for claim-assisted success and efficiency comparisons"
                ),
                "failure_recovery": (
                    "A verifier failed but A produced at least one frozen claim; "
                    "reported separately as recovery from unverified producer output"
                ),
                "unpaired_no_claim": "A produced no frozen claim; claim arms are not paired",
            },
            "usage_metric_definitions": {
                "token_totals_and_means": (
                    "observed values; lower bounds when incomplete_model_responses is non-zero"
                ),
                "paired_token_deltas": (
                    "B observed value minus A observed value; either side may be a lower bound "
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
        if record is None:
            # checkpoint 在父线程收集 future 后才落盘；若进程恰在此前退出，独立 task
            # manifest 已通过原子 replace 写入，必须优先恢复其中已经明确记录的失败或终态。
            recovered = _terminal_result_from_task_manifest(spec, spec.manifest_path)
            if recovered is not None:
                terminal.append(recovered)
            continue
        status = record.get("status")
        manifest_path = record.get("manifest_path")
        error = record.get("error")
        if not isinstance(manifest_path, str) or not Path(manifest_path).is_absolute():
            continue
        if status == "passed":
            if _is_completed_task_manifest(
                spec, Path(manifest_path)
            ) or _is_a_only_task_manifest(spec, Path(manifest_path)):
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


def reserve_interrupted_retries(
    completion_manifest_path: Path, task_ids: tuple[str, ...]
) -> None:
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
            if record.get("status") not in {"passed", "agent_failed"} or not _has_valid_gated_attempt_evidence(
                attempt_id, variant, record
            ):
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
        elif record.get("status") not in {"passed", "agent_failed"} or not _has_valid_gated_attempt_evidence(
            attempt_id, variant, record
        ):
            return False
    return True


def _task_manifest_matches_provenance(
    spec: PresmokeTaskSpec, raw: Mapping[str, object]
) -> bool:
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
    funnel = {
        variant: {field: 0 for field in fields}
        for variant in ("B_claim", "B_forced_claim")
    }
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
            for field in ("bundle_available", "retrieved", "injected", "used"):
                if observation.get(field) is True:
                    funnel[variant][field] += 1
            for field in (
                "delivery_evidence_count",
                "injected_claim_count",
                "used_claim_count",
            ):
                value = observation.get(field)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    funnel[variant][field] += value
    return funnel


_METRIC_VARIANTS = ("A", "B_empty", "B_claim", "B_forced_claim")
_REQUEST_USAGE_FIELDS = (
    "model_requests",
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


def _cohort_metrics(results: tuple[PresmokeTaskResult, ...]) -> dict[str, object]:
    """按 producer verifier cohort 汇总成功率、用量与同题 B-A 差值。"""
    cohorts: dict[str, dict[str, object]] = {}
    for task in results:
        validated = _validated_cohort_attempts(task)
        if validated is None:
            continue
        cohort, attempts = validated
        cohort_row = cohorts.setdefault(cohort, _new_cohort_row())
        cohort_row["task_count"] = int(cohort_row["task_count"]) + 1
        variants = cohort_row["variants"]
        if not isinstance(variants, dict):
            continue
        for variant, (passed, usage) in attempts.items():
            _accumulate_variant(variants[variant], passed, usage)
        a_attempt = attempts.get("A")
        paired = cohort_row["paired_against_a"]
        if cohort == "unpaired_no_claim" or a_attempt is None or not isinstance(paired, dict):
            continue
        a_passed, a_usage = a_attempt
        for variant in _METRIC_VARIANTS[1:]:
            b_attempt = attempts.get(variant)
            if b_attempt is None:
                continue
            b_passed, b_usage = b_attempt
            pair = paired[variant]
            if not isinstance(pair, dict):
                continue
            pair["pairs"] = int(pair["pairs"]) + 1
            pair["verifier_passed_delta"] = int(pair["verifier_passed_delta"]) + (
                int(b_passed) - int(a_passed)
            )
            usage_delta = pair["usage_delta_totals"]
            if isinstance(usage_delta, dict):
                for field in _USAGE_FIELDS:
                    usage_delta[field] = int(usage_delta[field]) + b_usage[field] - a_usage[field]
            if a_usage["incomplete_model_responses"] or b_usage["incomplete_model_responses"]:
                pair["pairs_with_incomplete_usage"] = (
                    int(pair["pairs_with_incomplete_usage"]) + 1
                )
    return {cohort: _finalize_cohort_row(row) for cohort, row in cohorts.items()}


def _validated_cohort_attempts(
    task: PresmokeTaskResult,
) -> tuple[str, dict[str, tuple[bool, dict[str, int]]]] | None:
    """只接受四臂均有 Gate pass 且哈希闭合的完整 task，避免部分结果污染统计。"""
    if task.status != "passed" or task.error is not None:
        return None
    manifest = _read_mapping_if_present(Path(task.manifest_path))
    if manifest is None or manifest.get("failure") is not None:
        return None
    cohort = manifest.get("experiment_cohort")
    records = manifest.get("attempt_results")
    if (
        cohort not in {"success_efficiency", "failure_recovery", "unpaired_no_claim"}
        or not isinstance(records, list)
        or len(records) != len(_METRIC_VARIANTS)
    ):
        return None
    attempts: dict[str, tuple[bool, dict[str, int]]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            return None
        attempt_id = record.get("attempt_id")
        variant = record.get("variant")
        if (
            not isinstance(attempt_id, str)
            or not attempt_id
            or variant not in _METRIC_VARIANTS
            or variant in attempts
            or record.get("status") not in {"passed", "agent_failed"}
        ):
            return None
        result_path = record.get("result_path")
        if not isinstance(result_path, str) or not _has_valid_gated_attempt_evidence(
            attempt_id, variant, record
        ):
            return None
        result_file = Path(result_path)
        attempt_result = _read_mapping_if_present(result_file)
        if attempt_result is None:
            return None
        verifier_passed = attempt_result.get("verifier_passed")
        usage = attempt_result.get("usage")
        if (
            not isinstance(verifier_passed, bool)
            or record.get("verifier_passed") != verifier_passed
            or not isinstance(usage, Mapping)
        ):
            return None
        usage_values: dict[str, int] = {}
        for field in _USAGE_FIELDS:
            value = usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return None
            usage_values[field] = value
        if (
            usage_values["complete_model_responses"]
            + usage_values["incomplete_model_responses"]
            != usage_values["model_requests"]
        ):
            return None
        attempts[variant] = (verifier_passed, usage_values)
    if set(attempts) != set(_METRIC_VARIANTS):
        return None
    return cohort, attempts


def _sha256_file_if_present(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _new_cohort_row() -> dict[str, object]:
    return {
        "task_count": 0,
        "variants": {
            variant: {
                "attempts": 0,
                "verifier_passed": 0,
                "incomplete_usage_attempts": 0,
                "usage_totals": {field: 0 for field in _USAGE_FIELDS},
            }
            for variant in _METRIC_VARIANTS
        },
        "paired_against_a": {
            variant: {
                "pairs": 0,
                "verifier_passed_delta": 0,
                "pairs_with_incomplete_usage": 0,
                "usage_delta_totals": {field: 0 for field in _USAGE_FIELDS},
            }
            for variant in _METRIC_VARIANTS[1:]
        },
    }


def _accumulate_variant(row: object, passed: bool, usage: Mapping[str, int]) -> None:
    if not isinstance(row, dict):
        return
    row["attempts"] = int(row["attempts"]) + 1
    row["verifier_passed"] = int(row["verifier_passed"]) + int(passed)
    if usage["incomplete_model_responses"]:
        row["incomplete_usage_attempts"] = int(row["incomplete_usage_attempts"]) + 1
    totals = row["usage_totals"]
    if isinstance(totals, dict):
        for field in _USAGE_FIELDS:
            totals[field] = int(totals[field]) + usage[field]


def _finalize_cohort_row(row: dict[str, object]) -> dict[str, object]:
    variants = row["variants"]
    if isinstance(variants, dict):
        for metrics in variants.values():
            if not isinstance(metrics, dict):
                continue
            attempts = int(metrics["attempts"])
            passed = int(metrics["verifier_passed"])
            incomplete = int(metrics["incomplete_usage_attempts"])
            totals = metrics["usage_totals"]
            metrics["verifier_pass_rate"] = passed / attempts if attempts else None
            metrics["incomplete_usage_attempt_rate"] = (
                incomplete / attempts if attempts else None
            )
            metrics["token_values_are_observed_lower_bound"] = incomplete > 0
            metrics["usage_means"] = {
                field: (int(totals[field]) / attempts if attempts else None)
                for field in _USAGE_FIELDS
            }
    paired = row["paired_against_a"]
    if isinstance(paired, dict):
        for metrics in paired.values():
            if not isinstance(metrics, dict):
                continue
            pairs = int(metrics["pairs"])
            delta = int(metrics["verifier_passed_delta"])
            incomplete_pairs = int(metrics["pairs_with_incomplete_usage"])
            totals = metrics["usage_delta_totals"]
            metrics["verifier_pass_rate_delta"] = delta / pairs if pairs else None
            metrics["pairs_with_incomplete_usage_rate"] = (
                incomplete_pairs / pairs if pairs else None
            )
            metrics["token_delta_includes_observed_lower_bound"] = incomplete_pairs > 0
            metrics["usage_delta_means"] = {
                field: (int(totals[field]) / pairs if pairs else None)
                for field in _USAGE_FIELDS
            }
    return row


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
