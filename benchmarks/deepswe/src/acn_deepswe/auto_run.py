"""准备并执行先 Smoke、后补齐全量的 DeepSWE 自动化实验。"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .dataset import FrozenDatasetManifest, freeze_execution_dataset
from .plan import build_attempt_plan
from .presmoke_cli import (
    ACN_REPOSITORY,
    FORMAL_ACN_MAIN_REVISION,
    FORMAL_ACN_VERSION,
    FORMAL_DOCKER_DISK_ADMISSION_MB_PER_WORKER,
    FORMAL_PIER_EGRESS_PROXY_IMAGE,
)
from .run_lock import exclusive_run_lock


class AutomatedRunError(ValueError):
    """自动化 run 的输入、阶段产物或终态不满足可审计约束。"""


@dataclass(frozen=True)
class AutomatedRunConfig:
    """不含 credential 的自动化运行配置。"""

    run_root: Path
    deepswe_checkout: Path
    source_tasks_root: Path
    pier_checkout: Path
    pier_executable: Path
    pier_egress_proxy_image: str
    pier_egress_proxy_content_digest: str
    acn_eval: Path
    frozen_skill: Path
    model: str
    response_model: str
    reasoning_effort: str
    run_class: str
    acn_main_revision: str
    acn_version: str
    model_egress_mode: str
    harness_mode: str
    claim_quality_gate: str
    file_edit_authority_enabled: bool
    task_workers: int
    smoke_size: int
    full_size: int
    dataset_seed: int
    smoke_plan_seed: int
    full_plan_seed: int
    resources: dict[str, int]
    timeouts: dict[str, int]
    llm_retry: dict[str, int]
    progress: dict[str, int]
    host_capacity: dict[str, int]
    cleanup_stale_pier_resources: bool
    run_all_variants_without_claims: bool = False
    run_a_only: bool = False
    b_only_from_a_output_dir: Path | None = None
    claim_producer_variant: str = "A"
    adaptive_producer_selection: bool = False
    reuse_local_agent_image_fingerprint: str | None = None


_REQUIRED_PATHS = (
    "run_root",
    "deepswe_checkout",
    "source_tasks_root",
    "pier_checkout",
    "pier_executable",
    "acn_eval",
    "frozen_skill",
)
_REQUIRED_RESOURCES = ("cpus", "memory_mb", "storage_mb", "max_tokens", "context_window")
_REQUIRED_TIMEOUTS = ("agent_seconds", "deadline_reserve_seconds", "verifier_seconds")
_REQUIRED_RETRY = ("retry_count", "retry_base_delay_ms", "retry_max_delay_ms")
_REQUIRED_HOST_CAPACITY = (
    "memory_reserve_mb",
    "disk_reserve_mb",
    "disk_admission_mb_per_worker",
)
_COMPLETED_PHASE_STATUSES = frozenset({"passed", "completed_with_no_eligible_claim"})
_ALLOWED_CONFIG_FIELDS = frozenset(
    {
        *_REQUIRED_PATHS,
        "model",
        "response_model",
        "reasoning_effort",
        "pier_egress_proxy_image",
        "pier_egress_proxy_content_digest",
        "run_class",
        "acn_main_revision",
        "acn_version",
        "model_egress_mode",
        "harness_mode",
        "claim_quality_gate",
        "file_edit_authority_enabled",
        "task_workers",
        "smoke_size",
        "full_size",
        "dataset_seed",
        "smoke_plan_seed",
        "full_plan_seed",
        "resources",
        "timeouts",
        "llm_retry",
        "progress",
        "host_capacity",
        "cleanup_stale_pier_resources",
        "run_all_variants_without_claims",
        "run_a_only",
        "b_only_from_a_output_dir",
        "claim_producer_variant",
        "adaptive_producer_selection",
        "reuse_local_agent_image_fingerprint",
    }
)


def main(argv: list[str] | None = None) -> int:
    """提供 prepare、run 和只读 monitor 三个动作。"""
    parser = argparse.ArgumentParser(
        prog="acn-deepswe-auto",
        description="可选先跑 Smoke；也可直接冻结并运行全量 DeepSWE 任务。",
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="绝对路径 JSON 配置（不含 credential）"
    )
    parser.add_argument(
        "--read-key-stdin",
        action="store_true",
        help="仅 run 时将隐藏读 key 请求传递给每个执行阶段",
    )
    parser.add_argument(
        "--resume-interrupted",
        action="store_true",
        help="仅 run 时显式授权一次中断 task 的续跑；Gate 或协议失败不会重跑",
    )
    parser.add_argument("action", choices=("prepare", "run", "monitor"))
    args = parser.parse_args(argv)
    injected_upstream_key = False
    try:
        config = load_config(args.config)
        if args.action == "prepare":
            print(json.dumps(prepare_run(config), ensure_ascii=False, indent=2))
        elif args.action == "run":
            if args.read_key_stdin and not os.environ.get("ACN_EVAL_UPSTREAM_KEY"):
                os.environ["ACN_EVAL_UPSTREAM_KEY"] = _read_upstream_key_stdin()
                injected_upstream_key = True
            print(
                json.dumps(
                    run_automated(
                        config,
                        read_key_stdin=args.read_key_stdin,
                        allow_interrupted_resume=args.resume_interrupted,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            print(json.dumps(monitor_run(config.run_root), ensure_ascii=False, indent=2))
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.error(str(error))
    finally:
        if injected_upstream_key:
            os.environ.pop("ACN_EVAL_UPSTREAM_KEY", None)
    return 0


def _read_upstream_key_stdin() -> str:
    """只为当前自动化父进程隐藏读取一次 key，供两个子阶段继承。"""
    try:
        key = getpass.getpass("ACN_EVAL_UPSTREAM_KEY: ")
    except (EOFError, OSError) as error:
        raise AutomatedRunError("无法从标准输入读取 ACN_EVAL_UPSTREAM_KEY") from error
    if not key:
        raise AutomatedRunError("ACN_EVAL_UPSTREAM_KEY 不能为空")
    return key


def load_config(path: Path) -> AutomatedRunConfig:
    """读取自动化配置，明确拒绝将模型 credential 写入文件。"""
    if not path.is_absolute():
        raise AutomatedRunError(f"config 路径必须为绝对路径: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AutomatedRunError(f"无法读取自动化运行配置: {path}") from error
    if not isinstance(raw, Mapping):
        raise AutomatedRunError("自动化运行配置必须是 JSON 对象")
    forbidden = {"ACN_EVAL_UPSTREAM_KEY", "upstream_key", "api_key", "key"}
    if forbidden & set(raw):
        raise AutomatedRunError("自动化运行配置不得包含 upstream credential")
    unknown = sorted(set(raw) - _ALLOWED_CONFIG_FIELDS)
    if unknown:
        raise AutomatedRunError("自动化运行配置包含未知字段: " + ",".join(unknown))
    paths = {name: _absolute_path(raw, name) for name in _REQUIRED_PATHS}
    resources = _positive_int_mapping(raw, "resources", _REQUIRED_RESOURCES)
    timeouts = _positive_int_mapping(raw, "timeouts", _REQUIRED_TIMEOUTS)
    llm_retry = _positive_int_mapping(raw, "llm_retry", _REQUIRED_RETRY)
    progress = _positive_int_mapping(raw, "progress", ("poll_secs", "stall_after_secs"))
    host_capacity = _positive_int_mapping(raw, "host_capacity", _REQUIRED_HOST_CAPACITY)
    if progress["stall_after_secs"] < progress["poll_secs"]:
        raise AutomatedRunError("progress.stall_after_secs 不得小于 poll_secs")
    smoke_size = _nonnegative_int(raw.get("smoke_size"), "smoke_size")
    full_size = _positive_int(raw.get("full_size"), "full_size")
    if smoke_size >= full_size:
        raise AutomatedRunError("smoke_size 必须小于 full_size，才能避免全量重复执行 Smoke")
    run_a_only = _boolean(raw.get("run_a_only", False), "run_a_only")
    b_only_from_a_output_dir = _optional_absolute_path(raw, "b_only_from_a_output_dir")
    claim_producer_variant = _claim_producer_variant(raw)
    adaptive_producer_selection = _boolean(
        raw.get("adaptive_producer_selection", False), "adaptive_producer_selection"
    )
    if run_a_only and b_only_from_a_output_dir is not None:
        raise AutomatedRunError("run_a_only 与 b_only_from_a_output_dir 不能同时启用")
    if b_only_from_a_output_dir is not None and smoke_size != 0:
        raise AutomatedRunError("B-only 接续必须设置 smoke_size=0，保持 A/B task 集合完全一致")
    if b_only_from_a_output_dir is not None and _paths_overlap(
        paths["run_root"] / "full" / "output", b_only_from_a_output_dir
    ):
        raise AutomatedRunError("B-only run_root 与 A-only source output 必须完全隔离")
    if claim_producer_variant != "A" and (run_a_only or b_only_from_a_output_dir is not None):
        raise AutomatedRunError("A-only/B-only 接续模式仅支持 claim_producer_variant=A")
    if adaptive_producer_selection:
        if smoke_size != 0:
            raise AutomatedRunError("adaptive producer selection 必须设置 smoke_size=0")
        if run_a_only or b_only_from_a_output_dir is not None:
            raise AutomatedRunError("adaptive producer selection 不可组合 A-only/B-only")
        if claim_producer_variant != "adaptive":
            raise AutomatedRunError(
                "adaptive producer selection 必须使用 claim_producer_variant=adaptive"
            )
    elif claim_producer_variant == "adaptive":
        raise AutomatedRunError(
            "claim_producer_variant=adaptive 必须显式启用 adaptive_producer_selection"
        )
    run_class = _run_class(raw)
    model_egress_mode = _model_egress_mode(raw)
    harness_mode = _harness_mode(raw)
    claim_quality_gate = _claim_quality_gate(raw)
    acn_main_revision = _git_revision(raw, "acn_main_revision")
    acn_version = _version(raw, "acn_version")
    file_edit_authority_enabled = _boolean(
        raw.get("file_edit_authority_enabled"), "file_edit_authority_enabled"
    )
    pier_egress_proxy_image = _nonempty_string(raw, "pier_egress_proxy_image")
    pier_egress_proxy_content_digest = _content_digest(raw, "pier_egress_proxy_content_digest")
    image_fingerprint = _optional_fingerprint(raw.get("reuse_local_agent_image_fingerprint"))
    if run_class == "formal":
        if model_egress_mode != "pier":
            raise AutomatedRunError("正式运行必须使用 model_egress_mode=pier")
        if acn_main_revision != FORMAL_ACN_MAIN_REVISION or acn_version != FORMAL_ACN_VERSION:
            raise AutomatedRunError(
                "正式运行必须锚定 "
                f"acn_main_revision={FORMAL_ACN_MAIN_REVISION} 和 acn_version={FORMAL_ACN_VERSION}"
            )
        if (
            host_capacity["disk_admission_mb_per_worker"]
            < FORMAL_DOCKER_DISK_ADMISSION_MB_PER_WORKER
        ):
            raise AutomatedRunError(
                "正式运行的 disk_admission_mb_per_worker 不得小于 "
                f"{FORMAL_DOCKER_DISK_ADMISSION_MB_PER_WORKER}"
            )
        if image_fingerprint is not None:
            raise AutomatedRunError("正式运行禁止复用本地 agent 镜像，必须使用冻结任务的官方镜像")
        if pier_egress_proxy_image != FORMAL_PIER_EGRESS_PROXY_IMAGE:
            raise AutomatedRunError(
                f"正式运行的 Pier egress proxy 镜像必须为 {FORMAL_PIER_EGRESS_PROXY_IMAGE}"
            )
    return AutomatedRunConfig(
        **paths,
        model=_nonempty_string(raw, "model"),
        response_model=_nonempty_string(raw, "response_model"),
        reasoning_effort=_nonempty_string(raw, "reasoning_effort"),
        pier_egress_proxy_image=pier_egress_proxy_image,
        pier_egress_proxy_content_digest=pier_egress_proxy_content_digest,
        run_class=run_class,
        acn_main_revision=acn_main_revision,
        acn_version=acn_version,
        model_egress_mode=model_egress_mode,
        harness_mode=harness_mode,
        claim_quality_gate=claim_quality_gate,
        file_edit_authority_enabled=file_edit_authority_enabled,
        task_workers=_positive_int(raw.get("task_workers"), "task_workers"),
        smoke_size=smoke_size,
        full_size=full_size,
        dataset_seed=_int(raw.get("dataset_seed"), "dataset_seed"),
        smoke_plan_seed=_int(raw.get("smoke_plan_seed"), "smoke_plan_seed"),
        full_plan_seed=_int(raw.get("full_plan_seed"), "full_plan_seed"),
        resources=resources,
        timeouts=timeouts,
        llm_retry=llm_retry,
        progress=progress,
        host_capacity=host_capacity,
        cleanup_stale_pier_resources=_boolean(
            raw.get("cleanup_stale_pier_resources", False),
            "cleanup_stale_pier_resources",
        ),
        run_all_variants_without_claims=_boolean(
            raw.get("run_all_variants_without_claims", False),
            "run_all_variants_without_claims",
        ),
        run_a_only=run_a_only,
        b_only_from_a_output_dir=b_only_from_a_output_dir,
        claim_producer_variant=claim_producer_variant,
        adaptive_producer_selection=adaptive_producer_selection,
        reuse_local_agent_image_fingerprint=image_fingerprint,
    )


def prepare_run(config: AutomatedRunConfig) -> dict[str, object]:
    """冻结任务全集，并可选划分 Smoke 与不重叠的后续全量阶段。"""
    if config.run_root.exists():
        raise AutomatedRunError(f"run_root 已存在，拒绝混入或覆盖历史 run: {config.run_root}")
    config.run_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{config.run_root.name}.", dir=config.run_root.parent))
    try:
        dataset_root = staging / "dataset"
        all_manifest_path = dataset_root / "full-frozen-manifest.json"
        normalized_root = staging / "normalized"
        freeze_execution_dataset(
            config.source_tasks_root,
            all_manifest_path,
            normalized_root,
            config.deepswe_checkout,
            config.pier_checkout,
            config.dataset_seed,
            sample_size=config.full_size,
            reuse_local_agent_image_fingerprint=config.reuse_local_agent_image_fingerprint,
        )
        all_manifest = _read_json_object(all_manifest_path, "全量冻结 manifest")
        all_dataset = FrozenDatasetManifest.from_dict(all_manifest)
        smoke_ids = (
            tuple(
                sorted(
                    random.Random(config.smoke_plan_seed).sample(
                        all_dataset.task_ids, config.smoke_size
                    )
                )
            )
            if config.smoke_size
            else ()
        )
        smoke_id_set = set(smoke_ids)
        full_ids = tuple(task_id for task_id in all_dataset.task_ids if task_id not in smoke_id_set)
        if len(full_ids) != config.full_size - config.smoke_size:
            raise AutomatedRunError("Smoke 与补齐全量阶段的任务划分不完整")

        acn_revision = _current_acn_revision()
        if config.adaptive_producer_selection:
            smoke = {"skipped": True, "task_count": 0, "task_ids": []}
            producers = _prepare_phase(
                config,
                staging,
                "producers",
                all_manifest,
                full_ids,
                normalized_root,
                acn_revision,
                config.full_plan_seed,
            )
            consumers = _prepare_phase(
                config,
                staging,
                "consumers",
                all_manifest,
                full_ids,
                normalized_root,
                acn_revision,
                config.full_plan_seed,
            )
            phases: dict[str, object] = {
                "smoke": smoke,
                "producers": producers,
                "consumers": consumers,
            }
        else:
            smoke = (
                _prepare_phase(
                    config,
                    staging,
                    "smoke",
                    all_manifest,
                    smoke_ids,
                    normalized_root,
                    acn_revision,
                    config.smoke_plan_seed,
                )
                if smoke_ids
                else {"skipped": True, "task_count": 0, "task_ids": []}
            )
            full = _prepare_phase(
                config,
                staging,
                "full",
                all_manifest,
                full_ids,
                normalized_root,
                acn_revision,
                config.full_plan_seed,
            )
            phases = {"smoke": smoke, "full": full}
        summary = {
            "schema_version": 1,
            "status": "prepared",
            "model": config.model,
            "response_model": config.response_model,
            "run_class": config.run_class,
            "acn_main_revision": config.acn_main_revision,
            "acn_version": config.acn_version,
            "model_egress_mode": config.model_egress_mode,
            "harness_mode": config.harness_mode,
            "claim_quality_gate": config.claim_quality_gate,
            "file_edit_authority_enabled": config.file_edit_authority_enabled,
            "pier_egress_proxy_image": config.pier_egress_proxy_image,
            "pier_egress_proxy_content_digest": config.pier_egress_proxy_content_digest,
            "task_workers": config.task_workers,
            "host_capacity": config.host_capacity,
            "claim_producer_variant": config.claim_producer_variant,
            "adaptive_producer_selection": config.adaptive_producer_selection,
            "phase_mode": (
                "adaptive_two_stage"
                if config.adaptive_producer_selection
                else (
                    "b_only_from_a"
                    if config.b_only_from_a_output_dir is not None
                    else ("a_only" if config.run_a_only else "full")
                )
            ),
            **phases,
        }
        _atomic_write_json(staging / "automation.json", summary)
        staging.replace(config.run_root)
        return {**summary, "run_root": str(config.run_root)}
    except Exception:
        if staging.exists():
            _remove_tree(staging)
        raise


def run_automated(
    config: AutomatedRunConfig,
    *,
    read_key_stdin: bool = False,
    allow_interrupted_resume: bool = False,
) -> dict[str, object]:
    """由外部调度器启动；同一 run_root 始终只允许一个编排器写入。"""
    lock_path = config.run_root.parent / f".{config.run_root.name}.automation.lock"
    with exclusive_run_lock(lock_path, "自动化评测"):
        return _run_automated_locked(
            config,
            read_key_stdin=read_key_stdin,
            allow_interrupted_resume=allow_interrupted_resume,
        )


def _run_automated_locked(
    config: AutomatedRunConfig,
    *,
    read_key_stdin: bool,
    allow_interrupted_resume: bool,
) -> dict[str, object]:
    """在自动化运行锁内准备或推进 Smoke / full 两个阶段。"""
    if not config.run_root.exists():
        prepare_run(config)
    summary_path = config.run_root / "orchestration-summary.json"
    if summary_path.is_file():
        previous = _read_json_object(summary_path, "自动化运行汇总")
        if previous.get("status") == "completed":
            return previous
    prepared = _read_json_object(config.run_root / "automation.json", "自动化运行描述")
    if prepared.get("status") != "prepared":
        raise AutomatedRunError("自动化运行描述不是可启动的 prepared 状态")

    if config.adaptive_producer_selection:
        producers = _completed_phase_or_run(
            config.run_root,
            "producers",
            read_key_stdin=read_key_stdin,
            allow_interrupted_resume=allow_interrupted_resume,
        )
        phases = {"producers": producers}
        selection: dict[str, object] | None = None
        if producers.get("completed") is True:
            selection = _select_adaptive_producer(config.run_root)
            phases["consumers"] = _completed_phase_or_run(
                config.run_root,
                "consumers",
                read_key_stdin=read_key_stdin,
                allow_interrupted_resume=allow_interrupted_resume,
            )
        else:
            phases["consumers"] = {
                "started": False,
                "completed": False,
                "reason": "producer_phase_not_completed",
            }
        completed = all(item.get("completed") is True for item in phases.values())
        status = (
            "completed"
            if completed
            else "consumers_not_completed"
            if producers.get("completed") is True
            else "producers_not_completed"
        )
    elif config.smoke_size == 0:
        phases: dict[str, dict[str, object]] = {
            "full": _completed_phase_or_run(
                config.run_root,
                "full",
                read_key_stdin=read_key_stdin,
                allow_interrupted_resume=allow_interrupted_resume,
            )
        }
        completed = phases["full"].get("completed") is True
        status = "completed" if completed else "full_not_completed"
    else:
        smoke = _completed_phase_or_run(
            config.run_root,
            "smoke",
            read_key_stdin=read_key_stdin,
            allow_interrupted_resume=allow_interrupted_resume,
        )
        phases = {"smoke": smoke}
        if smoke["completed"] is True:
            phases["full"] = _completed_phase_or_run(
                config.run_root,
                "full",
                read_key_stdin=read_key_stdin,
                allow_interrupted_resume=allow_interrupted_resume,
            )
        else:
            phases["full"] = {"started": False, "reason": "smoke_not_completed"}
        completed = all(item.get("completed") is True for item in phases.values())
        status = (
            "completed"
            if completed
            else "full_not_completed"
            if smoke["completed"] is True
            else "stopped_after_smoke"
        )
    summary = {
        "schema_version": 1,
        "status": status,
        "phases": phases,
    }
    if config.adaptive_producer_selection:
        summary["producer_selection"] = selection
    _atomic_write_json(summary_path, summary)
    return summary


def monitor_run(run_root: Path) -> dict[str, object]:
    """只读取阶段 aggregate 和 attempt progress，不创建、启动或终止任何任务。"""
    if not run_root.is_absolute() or not run_root.is_dir():
        raise AutomatedRunError(f"run_root 不存在或不是绝对目录: {run_root}")
    phase_statuses: dict[str, object] = {}
    phases = (
        ("producers", "consumers")
        if (run_root / "producers").is_dir() or (run_root / "consumers").is_dir()
        else ("smoke", "full")
    )
    for phase in phases:
        aggregate = run_root / phase / "output" / "presmoke-aggregate.json"
        if aggregate.is_file():
            phase_statuses[phase] = _read_json_object(aggregate, f"{phase} aggregate").get("status")
        else:
            phase_statuses[phase] = "not_started"
    progress_statuses: dict[str, int] = {}
    fresh_progress_statuses: dict[str, int] = {}
    possibly_stalled: list[str] = []
    stale_active_progress: list[str] = []
    latest_progress_observed: datetime | None = None
    now = datetime.now(UTC)
    snapshots: dict[str, tuple[Path, dict[str, object], datetime | None]] = {}
    for path in sorted(run_root.glob("**/attempts/*/output/progress.json")):
        raw = _read_json_object(path, "progress")
        attempt_id = raw.get("attempt_id")
        if not isinstance(attempt_id, str) or not attempt_id:
            continue
        observed = _progress_observed_at(raw)
        existing = snapshots.get(attempt_id)
        if existing is None or _progress_sort_key(path, observed) > _progress_sort_key(
            existing[0], existing[2]
        ):
            snapshots[attempt_id] = (path, raw, observed)
    for path, raw, observed in snapshots.values():
        status = raw.get("status")
        if not isinstance(status, str):
            continue
        progress_statuses[status] = progress_statuses.get(status, 0) + 1
        if raw.get("possibly_stalled") is True:
            possibly_stalled.append(str(path.relative_to(run_root)))
        if observed is not None and (
            latest_progress_observed is None or observed > latest_progress_observed
        ):
            latest_progress_observed = observed
        is_stale = status in {
            "active",
            "awaiting_session_event",
            "possibly_stalled",
        } and _is_stale_progress(raw, observed, now)
        if is_stale:
            stale_active_progress.append(str(path.relative_to(run_root)))
        else:
            fresh_progress_statuses[status] = fresh_progress_statuses.get(status, 0) + 1
    summary_path = run_root / "orchestration-summary.json"
    orchestration_status = (
        _read_json_object(summary_path, "自动化运行汇总").get("status")
        if summary_path.is_file()
        else "not_finished"
    )
    return {
        "schema_version": 1,
        "run_root": str(run_root),
        "orchestration_status": orchestration_status,
        "phase_statuses": phase_statuses,
        "progress_statuses": progress_statuses,
        "fresh_progress_statuses": fresh_progress_statuses,
        "possibly_stalled_progress": possibly_stalled,
        "stale_active_progress": stale_active_progress,
        "latest_progress_observed_at_utc": (
            latest_progress_observed.isoformat().replace("+00:00", "Z")
            if latest_progress_observed is not None
            else None
        ),
    }


def _progress_observed_at(raw: Mapping[str, object]) -> datetime | None:
    value = raw.get("observed_at_utc")
    if not isinstance(value, str):
        return None
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return observed.astimezone(UTC) if observed.tzinfo is not None else None


def _is_stale_progress(raw: Mapping[str, object], observed: datetime | None, now: datetime) -> bool:
    if observed is None:
        return True
    poll_secs = raw.get("progress_poll_secs")
    if isinstance(poll_secs, bool) or not isinstance(poll_secs, int) or poll_secs <= 0:
        return True
    # 允许一次采样抖动；超过两个轮询周期就不能再把历史 active 当作运行中。
    return (now - observed).total_seconds() > poll_secs * 2


def _progress_sort_key(path: Path, observed: datetime | None) -> tuple[float, int]:
    """同一 attempt 可有中断前与 resume 两个目录，只报告最新快照。"""
    observed_timestamp = observed.timestamp() if observed is not None else float("-inf")
    try:
        modified = path.stat().st_mtime_ns
    except OSError:
        modified = -1
    return observed_timestamp, modified


def _prepare_phase(
    config: AutomatedRunConfig,
    staging: Path,
    phase: str,
    all_manifest: Mapping[str, object],
    task_ids: tuple[str, ...],
    normalized_root: Path,
    acn_revision: str,
    plan_seed: int,
) -> dict[str, object]:
    adaptive_producers = config.adaptive_producer_selection and phase == "producers"
    adaptive_consumers = config.adaptive_producer_selection and phase == "consumers"
    if config.adaptive_producer_selection and not (adaptive_producers or adaptive_consumers):
        raise AutomatedRunError(f"adaptive run 不支持阶段: {phase}")
    phase_root = staging / phase
    phase_root.mkdir()
    # 所有文件先写入 staging，再由调用方原子发布为 run_root；供后续 CLI 读取的
    # 绝对路径必须预先指向发布后的目录，不能保留会被 rename 掉的 staging 路径。
    published_phase_root = config.run_root / phase
    manifest_path = phase_root / "frozen-manifest.json"
    manifest = _subset_manifest(all_manifest, task_ids)
    _atomic_write_json(manifest_path, manifest)
    dataset = FrozenDatasetManifest.from_dict(manifest)
    # 计划会在 staging 中生成后随目录原子发布；attempt 的运行输出必须直接
    # 指向发布后的运行根，不能保留 staging 临时路径。
    plan = build_attempt_plan(dataset, published_phase_root, plan_seed)
    plan_path = phase_root / "attempt-plan.json"
    _atomic_write_json(plan_path, plan.to_dict())
    output_dir = phase_root / "output"
    phase_config = {
        "frozen_manifest": str(published_phase_root / manifest_path.name),
        "attempt_plan": str(published_phase_root / plan_path.name),
        "deepswe_checkout": str(config.deepswe_checkout),
        "source_tasks_root": str(config.source_tasks_root),
        "pier_checkout": str(config.pier_checkout),
        "pier_executable": str(config.pier_executable),
        "pier_egress_proxy_image": config.pier_egress_proxy_image,
        "pier_egress_proxy_content_digest": config.pier_egress_proxy_content_digest,
        "acn_eval": str(config.acn_eval),
        "frozen_skill": str(config.frozen_skill),
        "normalized_root": str(config.run_root / normalized_root.name),
        "output_dir": str(published_phase_root / output_dir.name),
        "model": config.model,
        "response_model": config.response_model,
        "reasoning_effort": config.reasoning_effort,
        "run_class": config.run_class,
        "acn_main_revision": config.acn_main_revision,
        "acn_version": config.acn_version,
        "model_egress_mode": config.model_egress_mode,
        "harness_mode": config.harness_mode,
        "claim_quality_gate": config.claim_quality_gate,
        "file_edit_authority_enabled": config.file_edit_authority_enabled,
        "acn_revision": acn_revision,
        "task_workers": config.task_workers,
        "run_all_variants_without_claims": config.run_all_variants_without_claims,
        "run_a_only": config.run_a_only,
        "claim_producer_variant": (
            "adaptive" if adaptive_producers or adaptive_consumers else config.claim_producer_variant
        ),
        "producer_pair_only": adaptive_producers,
        "adaptive_source_output_dir": (
            str(config.run_root / "producers" / "output") if adaptive_consumers else None
        ),
        "producer_selection_manifest": (
            str(config.run_root / "producer-selection.json") if adaptive_consumers else None
        ),
        "b_only_from_a_output_dir": (
            str(config.b_only_from_a_output_dir)
            if config.b_only_from_a_output_dir is not None
            else None
        ),
        "progress": config.progress,
        "host_capacity": config.host_capacity,
        "cleanup_stale_pier_resources": config.cleanup_stale_pier_resources,
        "resources": config.resources,
        "timeouts": config.timeouts,
        "llm_retry": config.llm_retry,
    }
    phase_config_path = phase_root / "presmoke-run.json"
    _atomic_write_json(phase_config_path, phase_config)
    return {
        "task_count": len(task_ids),
        "task_ids": list(task_ids),
        "config": str(published_phase_root / phase_config_path.name),
        "output_dir": str(published_phase_root / output_dir.name),
    }


def _subset_manifest(
    full: Mapping[str, object], task_ids: tuple[str, ...]
) -> dict[str, object]:
    source_ids = full.get("task_ids")
    hashes = full.get("task_toml_hashes")
    directory_hashes = full.get("task_directory_hashes")
    if (
        not isinstance(source_ids, list)
        or not isinstance(hashes, Mapping)
        or not isinstance(directory_hashes, Mapping)
    ):
        raise AutomatedRunError("全量冻结 manifest 缺少可分割的 task 字段")
    if any(
        task_id not in source_ids or task_id not in hashes or task_id not in directory_hashes
        for task_id in task_ids
    ):
        raise AutomatedRunError("阶段 task 不属于全量冻结 manifest")
    copied = dict(full)
    # seed 是数据集抽样 seed，必须原样继承；阶段计划 seed 单独记录在 attempt-plan.json。
    copied["task_ids"] = list(task_ids)
    copied["task_toml_hashes"] = {task_id: hashes[task_id] for task_id in task_ids}
    copied["task_directory_hashes"] = {task_id: directory_hashes[task_id] for task_id in task_ids}
    return copied


def _run_phase(
    run_root: Path,
    phase: str,
    *,
    read_key_stdin: bool = False,
    resume: bool = False,
    retry_interrupted: bool = False,
) -> dict[str, object]:
    config_path = run_root / phase / "presmoke-run.json"
    config = _read_json_object(config_path, f"{phase} 启动配置")
    output_dir = _absolute_path(config, "output_dir")
    command = [sys.executable, "-m", "acn_deepswe.presmoke_cli"]
    if resume:
        command.append("--resume")
    if retry_interrupted:
        command.append("--retry-interrupted")
    if read_key_stdin:
        command.append("--read-key-stdin")
    command.extend(("--config", str(config_path)))
    completed = subprocess.run(command, check=False)
    aggregate_path = output_dir / "presmoke-aggregate.json"
    aggregate_status: object = "missing"
    if aggregate_path.is_file():
        aggregate_status = _read_json_object(aggregate_path, f"{phase} aggregate").get("status")
    phase_completed = completed.returncode == 0 and aggregate_status in _COMPLETED_PHASE_STATUSES
    return {
        "started": True,
        "return_code": completed.returncode,
        "aggregate_status": aggregate_status,
        "completed": phase_completed,
        "resumed": resume,
    }


def _completed_phase_or_run(
    run_root: Path,
    phase: str,
    *,
    read_key_stdin: bool = False,
    allow_interrupted_resume: bool = False,
) -> dict[str, object]:
    """仅自动复用完整阶段；中断阶段必须由调用方显式授权一次续跑。"""
    config_path = run_root / phase / "presmoke-run.json"
    config = _read_json_object(config_path, f"{phase} 启动配置")
    output_dir = _absolute_path(config, "output_dir")
    aggregate_path = output_dir / "presmoke-aggregate.json"
    if aggregate_path.is_file():
        aggregate_status = _read_json_object(aggregate_path, f"{phase} aggregate").get("status")
        if aggregate_status in _COMPLETED_PHASE_STATUSES:
            return {
                "started": False,
                "reused_completed_phase": True,
                "return_code": None,
                "aggregate_status": aggregate_status,
                "completed": True,
            }
    if output_dir.exists():
        if not allow_interrupted_resume:
            return {
                "started": False,
                "completed": False,
                "reason": "phase_requires_explicit_interrupted_resume",
            }
        return _run_phase(
            run_root,
            phase,
            read_key_stdin=read_key_stdin,
            resume=True,
            retry_interrupted=True,
        )
    return _run_phase(run_root, phase, read_key_stdin=read_key_stdin)


def _select_adaptive_producer(run_root: Path) -> dict[str, object]:
    """按预注册全量口径选择 producer；不允许逐 task 混合两个物理臂。"""
    source_output = (run_root / "producers" / "output").resolve()
    aggregate_path = source_output / "presmoke-aggregate.json"
    aggregate = _read_json_object(aggregate_path, "producer aggregate")
    if aggregate.get("status") != "passed":
        raise AutomatedRunError("producer aggregate 未完整通过，拒绝选择逻辑 A")
    task_order = aggregate.get("task_order")
    task_results = aggregate.get("task_results")
    if (
        not isinstance(task_order, list)
        or not task_order
        or not all(isinstance(task_id, str) and task_id for task_id in task_order)
        or len(set(task_order)) != len(task_order)
        or not isinstance(task_results, list)
        or len(task_results) != len(task_order)
    ):
        raise AutomatedRunError("producer aggregate task 集合不完整")

    aliases = {"S1": "A", "S2": "B_empty"}
    scores = {
        alias: {
            "physical_variant": variant,
            "valid_attempts": 0,
            "verifier_passed": 0,
            "f2p_passed": 0,
            "f2p_total": 0,
        }
        for alias, variant in aliases.items()
    }
    task_sources: dict[str, dict[str, str]] = {}
    for expected_task_id, raw_task in zip(task_order, task_results, strict=True):
        if (
            not isinstance(raw_task, Mapping)
            or raw_task.get("task_id") != expected_task_id
            or raw_task.get("status") != "passed"
        ):
            raise AutomatedRunError(f"producer task 未完整通过: {expected_task_id}")
        manifest_value = raw_task.get("manifest_path")
        if not isinstance(manifest_value, str):
            raise AutomatedRunError(f"producer task manifest 越出冻结目录: {expected_task_id}")
        expected_manifest = Path(manifest_value).resolve()
        task_source_output = _producer_task_source_root(
            source_output, expected_task_id, expected_manifest
        )
        manifest = _read_json_object(expected_manifest, f"producer task {expected_task_id}")
        task_sources[expected_task_id] = {
            "source_output_dir": str(task_source_output),
            "task_manifest_path": str(expected_manifest),
            "task_manifest_sha256": _sha256_file(expected_manifest),
        }
        execution = manifest.get("execution")
        records = manifest.get("attempt_results")
        if (
            manifest.get("failure") is not None
            or not isinstance(execution, Mapping)
            or execution.get("phase_mode") != "adaptive_producers"
            or execution.get("run_producer_pair_only") is not True
            or not isinstance(records, list)
            or len(records) != 4
        ):
            raise AutomatedRunError(f"producer task 不是完整双臂阶段: {expected_task_id}")
        by_variant = {
            record.get("variant"): record for record in records if isinstance(record, Mapping)
        }
        if set(by_variant) != {"A", "B_empty", "B_claim", "B_forced_claim"}:
            raise AutomatedRunError(f"producer task 四臂身份无效: {expected_task_id}")
        for alias, variant in aliases.items():
            record = by_variant[variant]
            if (
                record.get("status") not in {"passed", "agent_failed"}
                or not isinstance(record.get("verifier_passed"), bool)
            ):
                raise AutomatedRunError(f"producer {alias} 缺少有效 Gate 终态: {expected_task_id}")
            attempt_id = record.get("attempt_id")
            result_value = record.get("result_path")
            result_hash = record.get("result_hash")
            if not isinstance(attempt_id, str) or not attempt_id:
                raise AutomatedRunError(f"producer {alias} attempt_id 无效: {expected_task_id}")
            expected_result = (
                task_source_output
                / "attempts"
                / attempt_id
                / "output"
                / "attempt-result.json"
            )
            if (
                not isinstance(result_value, str)
                or Path(result_value).resolve() != expected_result
                or not isinstance(result_hash, str)
                or result_hash != _sha256_file(expected_result)
            ):
                raise AutomatedRunError(f"producer {alias} result 绑定无效: {expected_task_id}")
            result = _read_json_object(expected_result, f"producer {alias} attempt result")
            regrade = result.get("verifier_regrade")
            if regrade is not None and not isinstance(regrade, Mapping):
                raise AutomatedRunError(f"producer {alias} verifier 重判证据无效: {expected_task_id}")
            rewards = (regrade if regrade is not None else result).get("pier_trial")
            rewards = rewards.get("verifier_rewards") if isinstance(rewards, Mapping) else None
            if (
                result.get("attempt_id") != attempt_id
                or result.get("variant") != variant
                or result.get("verifier_passed") != record.get("verifier_passed")
                or not isinstance(rewards, Mapping)
            ):
                raise AutomatedRunError(f"producer {alias} verifier 证据无效: {expected_task_id}")
            f2p_passed = _reward_count(rewards.get("f2p_passed"), "f2p_passed")
            f2p_total = _reward_count(rewards.get("f2p_total"), "f2p_total")
            if f2p_passed > f2p_total:
                raise AutomatedRunError(f"producer {alias} F2P 计数无效: {expected_task_id}")
            score = scores[alias]
            score["valid_attempts"] += 1
            score["verifier_passed"] += int(record["verifier_passed"])
            score["f2p_passed"] += f2p_passed
            score["f2p_total"] += f2p_total
        for variant in ("B_claim", "B_forced_claim"):
            record = by_variant[variant]
            if record.get("status") != "not_run" or record.get("reason") != "PRODUCER_PAIR_ONLY":
                raise AutomatedRunError(f"producer 阶段提前执行 claim arm: {expected_task_id}")

    for score in scores.values():
        total = score["f2p_total"]
        if total <= 0:
            raise AutomatedRunError("producer 全量 F2P denominator 必须大于 0")
        score["f2p_micro"] = score["f2p_passed"] / total
    s1 = scores["S1"]
    s2 = scores["S2"]
    if s2["verifier_passed"] > s1["verifier_passed"]:
        winner_alias = "S2"
    elif s2["verifier_passed"] < s1["verifier_passed"]:
        winner_alias = "S1"
    else:
        s1_cross = s1["f2p_passed"] * s2["f2p_total"]
        s2_cross = s2["f2p_passed"] * s1["f2p_total"]
        winner_alias = "S2" if s2_cross > s1_cross else "S1"
    loser_alias = "S2" if winner_alias == "S1" else "S1"
    winner_variant = aliases[winner_alias]
    loser_variant = aliases[loser_alias]
    selection = {
        "schema_version": 1,
        "status": "selected",
        "candidate_aliases": aliases,
        "score_rule": ["verifier_passed", "f2p_micro", "S1_on_exact_tie"],
        "source_output_dir": str(source_output),
        "producer_aggregate_path": str(aggregate_path),
        "producer_aggregate_sha256": _sha256_file(aggregate_path),
        "task_order": task_order,
        "task_sources": task_sources,
        "candidates": scores,
        "winner_alias": winner_alias,
        "loser_alias": loser_alias,
        "winner_variant": winner_variant,
        "loser_variant": loser_variant,
        "logical_labels": {"A": winner_variant, "B_empty": loser_variant},
    }
    selection_path = run_root / "producer-selection.json"
    if selection_path.exists():
        if _read_json_object(selection_path, "producer selection") != selection:
            raise AutomatedRunError("既有 producer selection 与冻结评分证据不一致")
    else:
        _atomic_write_json(selection_path, selection)
    return selection


def _producer_task_source_root(
    producer_output: Path, task_id: str, manifest_path: Path
) -> Path:
    try:
        task_source_output = manifest_path.parents[2]
    except IndexError as error:
        raise AutomatedRunError(f"producer task manifest 层级无效: {task_id}") from error
    if manifest_path != task_source_output / "tasks" / task_id / "manifest.json":
        raise AutomatedRunError(f"producer task manifest 不在标准 task 目录: {task_id}")
    resumes_root = producer_output / "resumes"
    valid_resume = (
        task_source_output.parent == resumes_root
        and re.fullmatch(r"resume-[0-9]+", task_source_output.name) is not None
    )
    if task_source_output != producer_output and not valid_resume:
        raise AutomatedRunError(f"producer task source 越出原始或 resume 根目录: {task_id}")
    return task_source_output


def _reward_count(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AutomatedRunError(f"verifier reward {field} 必须为非负整数")
    return value


def _sha256_file(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise AutomatedRunError(f"无法读取冻结证据文件: {path}") from error


def _current_acn_revision() -> str:
    """正式冻结只接受可由单一 commit 完整还原的干净工作树。"""
    revision = subprocess.run(
        ["git", "-C", str(ACN_REPOSITORY), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    status = subprocess.run(
        ["git", "-C", str(ACN_REPOSITORY), "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    head = revision.stdout.strip() if revision.returncode == 0 else ""
    if not head or status.returncode != 0:
        raise AutomatedRunError("无法读取当前 ACN revision")
    if status.stdout.strip():
        raise AutomatedRunError("ACN 工作树不干净，正式运行拒绝冻结")
    return head


def _absolute_path(raw: Mapping[str, object], field: str) -> Path:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise AutomatedRunError(f"配置缺少非空路径: {field}")
    path = Path(value)
    if not path.is_absolute():
        raise AutomatedRunError(f"配置路径必须为绝对路径: {field}")
    return path


def _optional_absolute_path(raw: Mapping[str, object], field: str) -> Path | None:
    value = raw.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise AutomatedRunError(f"{field} 必须是绝对路径字符串或 null")
    path = Path(value)
    if not path.is_absolute():
        raise AutomatedRunError(f"{field} 必须是绝对路径: {path}")
    return path


def _paths_overlap(left: Path, right: Path) -> bool:
    resolved_left = left.resolve()
    resolved_right = right.resolve()
    return (
        resolved_left == resolved_right
        or resolved_left.is_relative_to(resolved_right)
        or resolved_right.is_relative_to(resolved_left)
    )


def _nonempty_string(raw: Mapping[str, object], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise AutomatedRunError(f"配置缺少非空字符串: {field}")
    return value


def _optional_fingerprint(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{16}", value):
        raise AutomatedRunError("reuse_local_agent_image_fingerprint 必须是 16 位小写十六进制")
    return value


def _git_revision(raw: Mapping[str, object], field: str) -> str:
    value = _nonempty_string(raw, field)
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise AutomatedRunError(f"{field} 必须是完整的 40 位小写 Git commit")
    return value


def _content_digest(raw: Mapping[str, object], field: str) -> str:
    value = _nonempty_string(raw, field)
    if (
        len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise AutomatedRunError(f"{field} 必须是 sha256 content digest")
    return value


def _version(raw: Mapping[str, object], field: str) -> str:
    value = _nonempty_string(raw, field)
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
        raise AutomatedRunError(f"{field} 必须是 x.y.z 版本")
    return value


def _run_class(raw: Mapping[str, object]) -> str:
    value = raw.get("run_class")
    if value not in {"formal", "diagnostic"}:
        raise AutomatedRunError("run_class 仅支持 formal 或 diagnostic")
    return value


def _model_egress_mode(raw: Mapping[str, object]) -> str:
    """默认继续走 Pier allowlist；direct 必须显式写入冻结运行配置。"""
    value = raw.get("model_egress_mode", "pier")
    if value not in {"pier", "direct"}:
        raise AutomatedRunError("model_egress_mode 仅支持 pier 或 direct")
    return value


def _harness_mode(raw: Mapping[str, object]) -> str:
    value = raw.get("harness_mode", "standard")
    if value not in {"standard", "minimal", "concise", "pi_like", "open_code_like"}:
        raise AutomatedRunError("harness_mode 无效")
    return value


def _claim_quality_gate(raw: Mapping[str, object]) -> str:
    value = raw.get("claim_quality_gate", "verified_producer_only")
    if value not in {"none", "verified_producer_only"}:
        raise AutomatedRunError("claim_quality_gate 无效")
    return value


def _claim_producer_variant(raw: Mapping[str, object]) -> str:
    value = raw.get("claim_producer_variant", "A")
    if value not in {"A", "B_empty", "adaptive"}:
        raise AutomatedRunError("claim_producer_variant 仅支持 A、B_empty 或 adaptive")
    return value


def _positive_int_mapping(
    raw: Mapping[str, object], field: str, required: tuple[str, ...]
) -> dict[str, int]:
    value = raw.get(field)
    if not isinstance(value, Mapping):
        raise AutomatedRunError(f"配置字段必须是整数对象: {field}")
    unknown = sorted(set(value) - set(required))
    if unknown:
        raise AutomatedRunError(f"{field} 包含未知字段: " + ",".join(unknown))
    result = {str(key): _positive_int(item, f"{field}.{key}") for key, item in value.items()}
    for key in required:
        if key not in result:
            raise AutomatedRunError(f"配置字段缺失: {field}.{key}")
    return result


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AutomatedRunError(f"配置字段必须为正整数: {field}")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AutomatedRunError(f"配置字段必须为非负整数: {field}")
    return value


def _int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AutomatedRunError(f"配置字段必须为整数: {field}")
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise AutomatedRunError(f"配置字段必须为布尔值: {field}")
    return value


def _read_json_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AutomatedRunError(f"无法读取 {label}: {path}") from error
    if not isinstance(value, dict):
        raise AutomatedRunError(f"{label} 必须是 JSON 对象: {path}")
    return value


def _atomic_write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _remove_tree(root: Path) -> None:
    """仅清理本函数刚创建、尚未发布的临时目录。"""
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_dir():
            path.rmdir()
        else:
            path.unlink()
    root.rmdir()


if __name__ == "__main__":
    sys.exit(main())
