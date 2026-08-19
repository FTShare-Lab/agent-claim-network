"""准备并执行先 Smoke、后补齐全量的 DeepSWE 自动化实验。"""

from __future__ import annotations

import argparse
import getpass
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
from .presmoke_cli import ACN_REPOSITORY
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
    acn_eval: Path
    frozen_skill: Path
    model: str
    response_model: str
    reasoning_effort: str
    model_egress_mode: str
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
    run_all_variants_without_claims: bool = False
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
_REQUIRED_TIMEOUTS = ("agent_seconds", "deadline_reserve_seconds")
_REQUIRED_RETRY = ("retry_count", "retry_base_delay_ms", "retry_max_delay_ms")
_COMPLETED_PHASE_STATUSES = frozenset({"passed", "completed_with_no_eligible_claim"})


def main(argv: list[str] | None = None) -> int:
    """提供 prepare、run 和只读 monitor 三个动作。"""
    parser = argparse.ArgumentParser(
        prog="acn-deepswe-auto",
        description="可选先跑 Smoke；也可直接冻结并运行全量 DeepSWE 任务。",
    )
    parser.add_argument("--config", type=Path, required=True, help="绝对路径 JSON 配置（不含 credential）")
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
    paths = {name: _absolute_path(raw, name) for name in _REQUIRED_PATHS}
    resources = _positive_int_mapping(raw, "resources", _REQUIRED_RESOURCES)
    timeouts = _positive_int_mapping(raw, "timeouts", _REQUIRED_TIMEOUTS)
    llm_retry = _positive_int_mapping(raw, "llm_retry", _REQUIRED_RETRY)
    progress = _positive_int_mapping(raw, "progress", ("poll_secs", "stall_after_secs"))
    if progress["stall_after_secs"] < progress["poll_secs"]:
        raise AutomatedRunError("progress.stall_after_secs 不得小于 poll_secs")
    smoke_size = _nonnegative_int(raw.get("smoke_size"), "smoke_size")
    full_size = _positive_int(raw.get("full_size"), "full_size")
    if smoke_size >= full_size:
        raise AutomatedRunError("smoke_size 必须小于 full_size，才能避免全量重复执行 Smoke")
    return AutomatedRunConfig(
        **paths,
        model=_nonempty_string(raw, "model"),
        response_model=_nonempty_string(raw, "response_model"),
        reasoning_effort=_nonempty_string(raw, "reasoning_effort"),
        model_egress_mode=_model_egress_mode(raw),
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
        run_all_variants_without_claims=_boolean(
            raw.get("run_all_variants_without_claims", False),
            "run_all_variants_without_claims",
        ),
        reuse_local_agent_image_fingerprint=_optional_fingerprint(
            raw.get("reuse_local_agent_image_fingerprint")
        ),
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
        summary = {
            "schema_version": 1,
            "status": "prepared",
            "model": config.model,
            "response_model": config.response_model,
            "model_egress_mode": config.model_egress_mode,
            "task_workers": config.task_workers,
            "smoke": smoke,
            "full": full,
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

    if config.smoke_size == 0:
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
    _atomic_write_json(summary_path, summary)
    return summary


def monitor_run(run_root: Path) -> dict[str, object]:
    """只读取阶段 aggregate 和 attempt progress，不创建、启动或终止任何任务。"""
    if not run_root.is_absolute() or not run_root.is_dir():
        raise AutomatedRunError(f"run_root 不存在或不是绝对目录: {run_root}")
    phase_statuses: dict[str, object] = {}
    for phase in ("smoke", "full"):
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


def _is_stale_progress(
    raw: Mapping[str, object], observed: datetime | None, now: datetime
) -> bool:
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
    phase_root = staging / phase
    phase_root.mkdir()
    # 所有文件先写入 staging，再由调用方原子发布为 run_root；供后续 CLI 读取的
    # 绝对路径必须预先指向发布后的目录，不能保留会被 rename 掉的 staging 路径。
    published_phase_root = config.run_root / phase
    manifest_path = phase_root / "frozen-manifest.json"
    manifest = _subset_manifest(all_manifest, task_ids, plan_seed)
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
        "acn_eval": str(config.acn_eval),
        "frozen_skill": str(config.frozen_skill),
        "normalized_root": str(config.run_root / normalized_root.name),
        "output_dir": str(published_phase_root / output_dir.name),
        "model": config.model,
        "response_model": config.response_model,
        "reasoning_effort": config.reasoning_effort,
        "model_egress_mode": config.model_egress_mode,
        "acn_revision": acn_revision,
        "task_workers": config.task_workers,
        "run_all_variants_without_claims": config.run_all_variants_without_claims,
        "progress": config.progress,
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
    full: Mapping[str, object], task_ids: tuple[str, ...], seed: int
) -> dict[str, object]:
    source_ids = full.get("task_ids")
    hashes = full.get("task_toml_hashes")
    directory_hashes = full.get("task_directory_hashes")
    if not isinstance(source_ids, list) or not isinstance(hashes, Mapping) or not isinstance(
        directory_hashes, Mapping
    ):
        raise AutomatedRunError("全量冻结 manifest 缺少可分割的 task 字段")
    if any(
        task_id not in source_ids or task_id not in hashes or task_id not in directory_hashes
        for task_id in task_ids
    ):
        raise AutomatedRunError("阶段 task 不属于全量冻结 manifest")
    copied = dict(full)
    copied["seed"] = seed
    copied["task_ids"] = list(task_ids)
    copied["task_toml_hashes"] = {task_id: hashes[task_id] for task_id in task_ids}
    copied["task_directory_hashes"] = {
        task_id: directory_hashes[task_id] for task_id in task_ids
    }
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


def _current_acn_revision() -> str:
    """沿用 presmoke 的 revision 标签语义，工作树有改动时显式标明。"""
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
    return f"{head}+evaluation-worktree" if status.stdout.strip() else head


def _absolute_path(raw: Mapping[str, object], field: str) -> Path:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise AutomatedRunError(f"配置缺少非空路径: {field}")
    path = Path(value)
    if not path.is_absolute():
        raise AutomatedRunError(f"配置路径必须为绝对路径: {field}")
    return path


def _nonempty_string(raw: Mapping[str, object], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise AutomatedRunError(f"配置缺少非空字符串: {field}")
    return value


def _optional_fingerprint(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{16}", value):
        raise AutomatedRunError(
            "reuse_local_agent_image_fingerprint 必须是 16 位小写十六进制"
        )
    return value


def _model_egress_mode(raw: Mapping[str, object]) -> str:
    """默认继续走 Pier allowlist；direct 必须显式写入冻结运行配置。"""
    value = raw.get("model_egress_mode", "pier")
    if value not in {"pier", "direct"}:
        raise AutomatedRunError("model_egress_mode 仅支持 pier 或 direct")
    return value


def _positive_int_mapping(
    raw: Mapping[str, object], field: str, required: tuple[str, ...]
) -> dict[str, int]:
    value = raw.get(field)
    if not isinstance(value, Mapping):
        raise AutomatedRunError(f"配置字段必须是整数对象: {field}")
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
