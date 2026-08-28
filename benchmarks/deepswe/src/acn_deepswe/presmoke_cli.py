"""冻结多题 pre-smoke 的非交互启动入口。"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

from .dataset import FrozenDatasetManifest
from .host_runner import (
    DEFAULT_PROGRESS_POLL_SECS,
    DEFAULT_PROGRESS_STALL_AFTER_SECS,
    EVALUATION_AUTO_COMPACT_CTX_RATIO,
    EVALUATION_CODE_RUN_MAX_OUTPUT_CHARS,
    EVALUATION_FILE_READ_MAX_CHARS,
    HostArtifacts,
    Task1ExecutionConfig,
    Task1HostRunner,
)
from .plan import AttemptPlan
from .presmoke import (
    PresmokeExecutionError,
    PresmokeHostRunner,
    PresmokeTaskResult,
    PresmokeTaskSpec,
    load_terminal_task_results,
    reserve_interrupted_retries,
)
from .provenance import (
    TASK_DIRECTORY_HASH_ALGORITHM,
    EvaluationProvenance,
    sha256_directory_tree,
)
from .runner import build_experiment_manifest
from .run_lock import exclusive_run_lock
from .resource_guard import (
    GLOBAL_DOCKER_LOCK,
    ResourceGuardError,
    cleanup_stale_pier_resources,
    reject_running_containers,
    verify_capacity,
    verify_disk_headroom,
)
from .schemas import AttemptManifest


class PresmokeCliError(ValueError):
    """pre-smoke 启动配置、冻结输入或环境不满足要求。"""


ACN_REPOSITORY = Path(__file__).resolve().parents[4]
ACN_SOURCE_ROOT = Path(__file__).resolve().parents[1]
FORMAL_ACN_MAIN_REVISION = "9b818d70ddfad2f7d5e1972577dd294b19481c92"
FORMAL_ACN_VERSION = "0.2.5"
FORMAL_PIER_EGRESS_PROXY_IMAGE = "pier-egress-proxy:ubuntu-24.04"
# Pier 的本地 Docker backend 只强制 CPU/内存，task.toml 的 storage_mb 不会预分配
# overlay 空间。正式门禁为每个并发 trial 保留独立的瞬时构建/写层预算。
FORMAL_DOCKER_DISK_ADMISSION_MB_PER_WORKER = 8192
_PIER_INSTALL_EVIDENCE_SCRIPT = (
    "import importlib.metadata as metadata, json; "
    "distribution = metadata.distribution('datacurve-pier'); "
    "print(json.dumps({'version': distribution.version, "
    "'direct_url': distribution.read_text('direct_url.json')}))"
)
_RESOURCE_FIELDS = ("cpus", "memory_mb", "storage_mb", "max_tokens", "context_window")
_TIMEOUT_FIELDS = ("agent_seconds", "deadline_reserve_seconds", "verifier_seconds")
_RETRY_FIELDS = ("retry_count", "retry_base_delay_ms", "retry_max_delay_ms")
_PROGRESS_FIELDS = ("poll_secs", "stall_after_secs")
_HOST_CAPACITY_FIELDS = (
    "memory_reserve_mb",
    "disk_reserve_mb",
    "disk_admission_mb_per_worker",
)
_ALLOWED_CONFIG_FIELDS = frozenset(
    {
        "frozen_manifest",
        "attempt_plan",
        "deepswe_checkout",
        "source_tasks_root",
        "pier_checkout",
        "pier_executable",
        "pier_egress_proxy_image",
        "pier_egress_proxy_content_digest",
        "acn_eval",
        "frozen_skill",
        "normalized_root",
        "output_dir",
        "model",
        "response_model",
        "reasoning_effort",
        "run_class",
        "acn_main_revision",
        "acn_version",
        "model_egress_mode",
        "harness_mode",
        "file_edit_authority_enabled",
        "resources",
        "timeouts",
        "llm_retry",
        "progress",
        "host_capacity",
        "acn_revision",
        "cleanup_stale_pier_resources",
        "task_workers",
        "run_all_variants_without_claims",
        "run_a_only",
        "b_only_from_a_output_dir",
    }
)


@dataclass(frozen=True)
class PresmokeConfig:
    frozen_manifest: Path
    attempt_plan: Path
    deepswe_checkout: Path
    source_tasks_root: Path
    pier_checkout: Path
    pier_executable: Path
    pier_egress_proxy_image: str
    pier_egress_proxy_content_digest: str
    acn_eval: Path
    frozen_skill: Path
    normalized_root: Path
    output_dir: Path
    model: str
    response_model: str
    reasoning_effort: str
    run_class: str
    acn_main_revision: str
    acn_version: str
    model_egress_mode: str
    harness_mode: str
    file_edit_authority_enabled: bool
    resources: dict[str, int]
    timeouts: dict[str, int]
    llm_retry: dict[str, int]
    progress: dict[str, int]
    host_capacity: dict[str, int]
    acn_revision: str
    cleanup_stale_pier_resources: bool = False
    task_workers: int = 1
    run_all_variants_without_claims: bool = False
    run_a_only: bool = False
    b_only_from_a_output_dir: Path | None = None


@dataclass(frozen=True)
class FrozenPythonRuntime:
    acn_source_root: Path
    pier_source_root: Path
    frozen_skill: Path
    pier_executable: Path
    acn_eval: Path
    acn_binary_hash: str
    acn_package_tree_hash: str
    pier_package_tree_hash: str


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="acn-deepswe-presmoke",
        description="按冻结 manifest 启动 ACN DeepSWE pre-smoke。",
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="绝对路径 JSON 启动配置（不含 credential）"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只校验冻结输入并输出计划；不调用 Docker、Pier 或模型",
    )
    parser.add_argument(
        "--read-key-stdin",
        action="store_true",
        help="真实执行且环境未提供 key 时，通过隐藏输入读取 ACN_EVAL_UPSTREAM_KEY",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="仅续跑尚未产生终态的 task；不会重跑 Gate 或协议失败",
    )
    parser.add_argument(
        "--retry-interrupted",
        action="store_true",
        help="与 --resume 一起使用：显式授权一次中断 task 的重新执行",
    )
    args = parser.parse_args(argv)
    if args.retry_interrupted and not args.resume:
        parser.error("--retry-interrupted 必须与 --resume 一起使用")
    injected_upstream_key = False
    try:
        config = load_config(args.config)
        upstream_base_url = os.environ.get("ACN_EVAL_UPSTREAM_BASE_URL")
        if not upstream_base_url:
            raise PresmokeCliError("宿主环境缺少 ACN_EVAL_UPSTREAM_BASE_URL")
        if not args.dry_run and not os.environ.get("ACN_EVAL_UPSTREAM_KEY"):
            if not args.read_key_stdin:
                raise PresmokeCliError(
                    "宿主环境缺少 ACN_EVAL_UPSTREAM_KEY；真实执行可传 --read-key-stdin 隐藏读取"
                )
            upstream_key = _read_upstream_key_stdin()
            os.environ["ACN_EVAL_UPSTREAM_KEY"] = upstream_key
            injected_upstream_key = True
        verify_acn_revision(
            config.acn_revision,
            config.acn_main_revision,
            config.acn_version,
        )
        if args.dry_run:
            all_specs, frozen_task_ids = build_task_specs(
                config,
                upstream_base_url,
                resolve_image_digests=False,
                frozen_runtime=None,
            )
            print(
                json.dumps(
                    dry_run_summary(config, all_specs, frozen_task_ids),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        with exclusive_run_lock(config.output_dir / ".presmoke.lock", "pre-smoke 阶段"):
            with exclusive_run_lock(GLOBAL_DOCKER_LOCK, "全机 Docker 正式评测"):
                docker_root = preflight_execution(config)
                frozen_runtime = stage_python_runtime(config, allow_existing=args.resume)
                all_specs, frozen_task_ids = build_task_specs(
                    config,
                    upstream_base_url,
                    resolve_image_digests=True,
                    frozen_runtime=frozen_runtime,
                    docker_root=docker_root,
                )
                validate_b_only_sources(all_specs)
                completion_manifest_path = config.output_dir / "task-completions.json"
                completed = (
                    load_terminal_task_results(all_specs, completion_manifest_path)
                    if args.resume
                    else ()
                )
                failed = [result.task_id for result in completed if result.status == "failed"]
                if failed:
                    raise PresmokeCliError(
                        "续跑拒绝覆盖已有失败终态（包括 Gate/协议失败）: " + ",".join(failed)
                    )
                completed_ids = {result.task_id for result in completed}
                pending_ids = tuple(
                    task_id for task_id in frozen_task_ids if task_id not in completed_ids
                )
                specs = all_specs
                if args.resume and pending_ids:
                    interrupted_ids = tuple(
                        spec.task_id
                        for spec in all_specs
                        if spec.task_id in pending_ids and _task_has_partial_artifacts(spec)
                    )
                    if interrupted_ids and not args.retry_interrupted:
                        raise PresmokeCliError(
                            "检测到中断 task；请显式传 --resume --retry-interrupted（每题最多一次）: "
                            + ",".join(interrupted_ids)
                        )
                    if interrupted_ids:
                        reserve_interrupted_retries(completion_manifest_path, interrupted_ids)
                    resume_root = _next_resume_root(config.output_dir)
                    specs, _ = build_task_specs(
                        config,
                        upstream_base_url,
                        resolve_image_digests=True,
                        frozen_runtime=frozen_runtime,
                        selected_task_ids=set(pending_ids),
                        attempt_output_root=resume_root,
                        docker_root=docker_root,
                    )
                    _write_resume_descriptor(resume_root, config, completed, specs)
                runner = PresmokeHostRunner(
                    specs,
                    config.output_dir / "presmoke-aggregate.json",
                    task_workers=config.task_workers,
                    frozen_task_ids=frozen_task_ids,
                    completed_task_results=completed,
                    completion_manifest_path=completion_manifest_path,
                )
                results = runner.run(execute=True)
                status = (
                    "completed_with_no_eligible_claim"
                    if any(item.status == "no_eligible_claim" for item in results)
                    else "passed"
                )
                print(
                    json.dumps(
                        {"status": status, "tasks": [item.to_dict() for item in results]},
                        ensure_ascii=False,
                    )
                )
                return 0
    except (
        OSError,
        ValueError,
        subprocess.SubprocessError,
        PresmokeExecutionError,
        ResourceGuardError,
    ) as error:
        parser.error(str(error))
    finally:
        if injected_upstream_key:
            os.environ.pop("ACN_EVAL_UPSTREAM_KEY", None)
    return 2


def _read_upstream_key_stdin() -> str:
    """从终端隐藏读取仅供本次真实执行使用的上游 credential。"""
    try:
        upstream_key = getpass.getpass("ACN_EVAL_UPSTREAM_KEY: ")
    except (EOFError, OSError) as error:
        raise PresmokeCliError("无法从标准输入读取 ACN_EVAL_UPSTREAM_KEY") from error
    if not upstream_key:
        raise PresmokeCliError("ACN_EVAL_UPSTREAM_KEY 不能为空")
    return upstream_key


def load_config(path: Path) -> PresmokeConfig:
    """读取 JSON 配置；credential 只允许由宿主环境提供。"""
    if not path.is_absolute():
        raise PresmokeCliError(f"config 路径必须为绝对路径: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PresmokeCliError(f"无法读取 pre-smoke config: {path}") from error
    if not isinstance(raw, Mapping):
        raise PresmokeCliError("pre-smoke config 必须是 JSON 对象")
    forbidden = {"ACN_EVAL_UPSTREAM_KEY", "upstream_key", "api_key", "key"}
    if forbidden & set(raw):
        raise PresmokeCliError("pre-smoke config 不得包含 upstream credential")
    unknown = sorted(set(raw) - _ALLOWED_CONFIG_FIELDS)
    if unknown:
        raise PresmokeCliError("pre-smoke config 包含未知字段: " + ",".join(unknown))
    required_paths = (
        "frozen_manifest",
        "attempt_plan",
        "deepswe_checkout",
        "source_tasks_root",
        "pier_checkout",
        "pier_executable",
        "acn_eval",
        "frozen_skill",
        "normalized_root",
        "output_dir",
    )
    paths = {name: _absolute_path(raw, name) for name in required_paths}
    resources = _positive_int_mapping(raw, "resources", _RESOURCE_FIELDS)
    timeouts = _positive_int_mapping(raw, "timeouts", _TIMEOUT_FIELDS)
    llm_retry = _positive_int_mapping(raw, "llm_retry", _RETRY_FIELDS)
    host_capacity = _positive_int_mapping(raw, "host_capacity", _HOST_CAPACITY_FIELDS)
    progress_raw = raw.get("progress", {})
    if not isinstance(progress_raw, Mapping):
        raise PresmokeCliError("config.progress 必须是整数对象")
    progress = {
        str(key): _positive_int(value, f"config.progress.{key}")
        for key, value in progress_raw.items()
    }
    unknown_progress = sorted(set(progress) - set(_PROGRESS_FIELDS))
    if unknown_progress:
        raise PresmokeCliError("config.progress 包含未知字段: " + ",".join(unknown_progress))
    if "max_tool_loop_turns" in resources:
        raise PresmokeCliError("resources 不再支持字段: max_tool_loop_turns")
    progress = {
        "poll_secs": progress.get("poll_secs", DEFAULT_PROGRESS_POLL_SECS),
        "stall_after_secs": progress.get("stall_after_secs", DEFAULT_PROGRESS_STALL_AFTER_SECS),
    }
    if progress["stall_after_secs"] < progress["poll_secs"]:
        raise PresmokeCliError("config.progress.stall_after_secs 不得小于 poll_secs")
    run_a_only = _boolean(raw.get("run_a_only", False), "run_a_only")
    b_only_from_a_output_dir = _optional_absolute_path(raw, "b_only_from_a_output_dir")
    if run_a_only and b_only_from_a_output_dir is not None:
        raise PresmokeCliError("run_a_only 与 b_only_from_a_output_dir 不能同时启用")
    if b_only_from_a_output_dir is not None and _paths_overlap(
        paths["output_dir"], b_only_from_a_output_dir
    ):
        raise PresmokeCliError("B-only output_dir 与 A-only source output 必须完全隔离")
    run_class = _run_class(raw)
    model_egress_mode = _model_egress_mode(raw)
    harness_mode = _harness_mode(raw)
    acn_main_revision = _git_revision(raw, "acn_main_revision")
    acn_version = _version(raw, "acn_version")
    file_edit_authority_enabled = _boolean(
        raw.get("file_edit_authority_enabled"), "file_edit_authority_enabled"
    )
    pier_egress_proxy_image = _nonempty_string(raw, "pier_egress_proxy_image")
    pier_egress_proxy_content_digest = _content_digest(raw, "pier_egress_proxy_content_digest")
    if run_class == "formal" and (harness_mode != "standard" or model_egress_mode != "pier"):
        raise PresmokeCliError("正式运行必须使用 harness_mode=standard 和 model_egress_mode=pier")
    if run_class == "formal" and (
        acn_main_revision != FORMAL_ACN_MAIN_REVISION or acn_version != FORMAL_ACN_VERSION
    ):
        raise PresmokeCliError(
            "正式运行必须锚定 "
            f"acn_main_revision={FORMAL_ACN_MAIN_REVISION} 和 acn_version={FORMAL_ACN_VERSION}"
        )
    if run_class == "formal" and (
        host_capacity["disk_admission_mb_per_worker"] < FORMAL_DOCKER_DISK_ADMISSION_MB_PER_WORKER
    ):
        raise PresmokeCliError(
            "正式运行的 disk_admission_mb_per_worker 不得小于 "
            f"{FORMAL_DOCKER_DISK_ADMISSION_MB_PER_WORKER}"
        )
    if run_class == "formal" and pier_egress_proxy_image != FORMAL_PIER_EGRESS_PROXY_IMAGE:
        raise PresmokeCliError(
            f"正式运行的 Pier egress proxy 镜像必须为 {FORMAL_PIER_EGRESS_PROXY_IMAGE}"
        )
    return PresmokeConfig(
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
        file_edit_authority_enabled=file_edit_authority_enabled,
        resources=resources,
        timeouts=timeouts,
        llm_retry=llm_retry,
        progress=progress,
        host_capacity=host_capacity,
        acn_revision=_nonempty_string(raw, "acn_revision"),
        cleanup_stale_pier_resources=_boolean(
            raw.get("cleanup_stale_pier_resources", False),
            "cleanup_stale_pier_resources",
        ),
        task_workers=_positive_int(raw.get("task_workers", 1), "task_workers"),
        run_all_variants_without_claims=_boolean(
            raw.get("run_all_variants_without_claims", False),
            "run_all_variants_without_claims",
        ),
        run_a_only=run_a_only,
        b_only_from_a_output_dir=b_only_from_a_output_dir,
    )


def build_task_specs(
    config: PresmokeConfig,
    upstream_base_url: str,
    *,
    resolve_image_digests: bool = False,
    frozen_runtime: FrozenPythonRuntime | None = None,
    selected_task_ids: set[str] | None = None,
    attempt_output_root: Path | None = None,
    docker_root: Path | None = None,
) -> tuple[tuple[PresmokeTaskSpec, ...], tuple[str, ...]]:
    """验证冻结版本与 hash，并为每个 task 装配现有 HostRunner 输入。"""
    _require_file(config.frozen_manifest, "frozen_manifest")
    _require_file(config.attempt_plan, "attempt_plan")
    _require_file(config.acn_eval, "acn_eval")
    _require_directory(config.frozen_skill, "frozen_skill")
    _require_file(config.frozen_skill / "SKILL.md", "frozen_skill/SKILL.md")
    _require_file(config.pier_executable, "pier_executable")
    _require_directory(ACN_SOURCE_ROOT / "acn_deepswe", "ACN evaluation package")
    _require_directory(config.pier_checkout / "src" / "pier", "Pier package")
    _require_directory(config.source_tasks_root, "source_tasks_root")
    _require_directory(config.normalized_root, "normalized_root")
    acn_source_root = (
        frozen_runtime.acn_source_root if frozen_runtime is not None else ACN_SOURCE_ROOT
    )
    pier_source_root = (
        frozen_runtime.pier_source_root
        if frozen_runtime is not None
        else config.pier_checkout / "src"
    )
    frozen_skill = (
        frozen_runtime.frozen_skill if frozen_runtime is not None else config.frozen_skill
    )
    pier_executable = (
        frozen_runtime.pier_executable if frozen_runtime is not None else config.pier_executable
    )
    acn_eval = frozen_runtime.acn_eval if frozen_runtime is not None else config.acn_eval
    acn_package_tree_hash = (
        frozen_runtime.acn_package_tree_hash
        if frozen_runtime is not None
        else sha256_directory_tree(acn_source_root)
    )
    pier_package_tree_hash = (
        frozen_runtime.pier_package_tree_hash
        if frozen_runtime is not None
        else sha256_directory_tree(pier_source_root)
    )
    raw_manifest = _read_object(config.frozen_manifest, "冻结 manifest")
    frozen_dataset = FrozenDatasetManifest.from_dict(raw_manifest)
    deepswe_revision = _nonempty_string(raw_manifest, "deepswe_revision")
    pier_revision = _nonempty_string(raw_manifest, "pier_revision")
    verify_checkout_revision(config.deepswe_checkout, deepswe_revision)
    verify_checkout_revision(config.pier_checkout, pier_revision)
    frozen_task_ids = frozen_dataset.task_ids
    if selected_task_ids is not None and not selected_task_ids <= set(frozen_task_ids):
        raise PresmokeCliError("续跑 task 不属于冻结 manifest")
    if attempt_output_root is not None and not attempt_output_root.is_absolute():
        raise PresmokeCliError("续跑 attempt 输出根目录必须为绝对路径")
    plan = AttemptPlan.from_dict(_read_object(config.attempt_plan, "attempt plan"))
    if plan.freeze_candidates_hash != frozen_dataset.candidates_hash:
        raise PresmokeCliError("attempt plan.freeze_candidates_hash 不匹配冻结 manifest")
    task_toml_hashes = raw_manifest.get("task_toml_hashes")
    if not isinstance(task_toml_hashes, Mapping):
        raise PresmokeCliError("冻结 manifest.task_toml_hashes 必须是对象")
    task_directory_hashes = raw_manifest.get("task_directory_hashes")
    if raw_manifest.get("task_directory_hash_algorithm") != TASK_DIRECTORY_HASH_ALGORITHM:
        raise PresmokeCliError(
            f"冻结 manifest.task_directory_hash_algorithm 必须为 {TASK_DIRECTORY_HASH_ALGORITHM}"
        )
    if not isinstance(task_directory_hashes, Mapping):
        raise PresmokeCliError(
            "冻结 manifest.task_directory_hashes 必须是对象；请从原 DeepSWE checkout 和 normalized task 目录重新生成清单"
        )
    by_task = _attempts_by_task(plan, frozen_task_ids)
    skill_hash = sha256_directory_tree(frozen_skill)
    acn_binary_hash = (
        frozen_runtime.acn_binary_hash if frozen_runtime is not None else _sha256_file(acn_eval)
    )
    verified_acn_eval = frozen_runtime.acn_eval if frozen_runtime is not None else acn_eval
    _verify_acn_eval_build_info(
        verified_acn_eval,
        expected_revision=config.acn_revision,
        expected_version=config.acn_version,
    )
    config_hash = _effective_config_hash(config)
    specs: list[PresmokeTaskSpec] = []
    for task_id in frozen_task_ids:
        if selected_task_ids is not None and task_id not in selected_task_ids:
            continue
        source_dir = config.source_tasks_root / task_id
        normalized_dir = config.normalized_root / task_id
        source = source_dir / "task.toml"
        normalized = normalized_dir / "task.toml"
        _require_directory(source_dir, f"source task 目录 ({task_id})")
        _require_directory(normalized_dir, f"normalized task 目录 ({task_id})")
        _require_file(source, f"source task.toml ({task_id})")
        _require_file(normalized, f"normalized task.toml ({task_id})")
        expected = task_toml_hashes.get(task_id)
        if not isinstance(expected, Mapping):
            raise PresmokeCliError(f"冻结 manifest 缺少 task hash: {task_id}")
        _verify_hash(source, expected.get("source"), f"source task.toml ({task_id})")
        _verify_hash(normalized, expected.get("normalized"), f"normalized task.toml ({task_id})")
        source_tree_hash = sha256_directory_tree(source_dir)
        normalized_tree_hash = sha256_directory_tree(normalized_dir)
        expected_tree = task_directory_hashes.get(task_id)
        if not isinstance(expected_tree, Mapping):
            raise PresmokeCliError(f"冻结 manifest 缺少 task directory hash: {task_id}")
        _verify_directory_hash(
            source_tree_hash, expected_tree.get("source"), f"source task 目录 ({task_id})"
        )
        _verify_directory_hash(
            normalized_tree_hash,
            expected_tree.get("normalized"),
            f"normalized task 目录 ({task_id})",
        )
        prompt = _read_prompt(normalized_dir / "instruction.md", task_id)
        agent_image = _task_image(normalized, task_id)
        image_content_digest = (
            _docker_image_content_digest(agent_image) if resolve_image_digests else None
        )
        provenance = EvaluationProvenance(
            deepswe_revision=deepswe_revision,
            pier_revision=pier_revision,
            acn_revision=config.acn_revision,
            acn_main_revision=config.acn_main_revision,
            acn_version=config.acn_version,
            run_class=config.run_class,
            acn_binary_hash=acn_binary_hash,
            acn_config_hash=config_hash,
            dataset_candidates_hash=plan.freeze_candidates_hash,
            dataset_seed=frozen_dataset.seed,
            dataset_task_ids=frozen_task_ids,
            skill_hash=skill_hash,
            acn_package_tree_hash=acn_package_tree_hash,
            pier_package_tree_hash=pier_package_tree_hash,
            source_task_tree_hash=source_tree_hash,
            normalized_task_tree_hash=normalized_tree_hash,
            agent_image_reference_sha256=_sha256_text(agent_image),
            verifier_image_reference_sha256=_sha256_text(agent_image),
            pier_egress_proxy_image_reference_sha256=_sha256_text(config.pier_egress_proxy_image),
            agent_image_content_digest=image_content_digest,
            verifier_image_content_digest=image_content_digest,
            pier_egress_proxy_image_content_digest=(config.pier_egress_proxy_content_digest),
            model=config.model,
            reasoning_effort=config.reasoning_effort,
            file_edit_authority_enabled=config.file_edit_authority_enabled,
            resources=config.resources,
            timeouts=config.timeouts,
            llm_retry=config.llm_retry,
            network_translation_warning="frozen normalized task directory validated by SHA-256",
        )
        attempts = by_task[task_id]
        if attempt_output_root is not None:
            attempts = tuple(
                AttemptManifest(
                    attempt.schema_version,
                    attempt.attempt_id,
                    attempt.task_id,
                    attempt.variant,
                    str(attempt_output_root / "attempts" / attempt.attempt_id / "output"),
                )
                for attempt in attempts
            )
        if config.b_only_from_a_output_dir is not None:
            source_a = attempts[0]
            attempts = (
                AttemptManifest(
                    source_a.schema_version,
                    source_a.attempt_id,
                    source_a.task_id,
                    source_a.variant,
                    str(
                        config.b_only_from_a_output_dir
                        / "attempts"
                        / source_a.attempt_id
                        / "output"
                    ),
                ),
                *attempts[1:],
            )
        task_plan = AttemptPlan(1, plan.freeze_candidates_hash, plan.seed, attempts)
        experiment = build_experiment_manifest(
            f"{config.frozen_manifest.stem}-{task_id}",
            task_plan,
            None,
            provenance,
        )
        task_output_root = (
            attempt_output_root if attempt_output_root is not None else config.output_dir
        )
        task_output = task_output_root / "tasks" / task_id
        a_only_source_manifest = (
            config.b_only_from_a_output_dir / "tasks" / task_id / "manifest.json"
            if config.b_only_from_a_output_dir is not None
            else None
        )
        claim_bundle = (
            config.b_only_from_a_output_dir / "tasks" / task_id / "claims.json"
            if config.b_only_from_a_output_dir is not None
            else task_output / "claims.json"
        )
        specs.append(
            PresmokeTaskSpec(
                task_id=task_id,
                experiment=experiment,
                execution=Task1ExecutionConfig(
                    artifacts=HostArtifacts(
                        acn_eval=acn_eval,
                        frozen_skill=frozen_skill,
                        claim_bundle=claim_bundle,
                        normalized_task_dir=(config.normalized_root / task_id),
                    ),
                    task_prompt=prompt,
                    upstream_base_url=upstream_base_url,
                    manifest_path=task_output / "manifest.json",
                    pier_executable=pier_executable,
                    expected_response_model=config.response_model,
                    frozen_acn_source_root=acn_source_root,
                    frozen_pier_source_root=pier_source_root,
                    model_egress_mode=config.model_egress_mode,
                    harness_mode=config.harness_mode,
                    task_workers=config.task_workers,
                    require_eligible_claim=False,
                    run_all_variants_without_claims=config.run_all_variants_without_claims,
                    run_a_only=config.run_a_only,
                    a_only_source_manifest=a_only_source_manifest,
                    progress_poll_secs=config.progress["poll_secs"],
                    progress_stall_after_secs=config.progress["stall_after_secs"],
                    docker_root=docker_root,
                    disk_reserve_mb=config.host_capacity["disk_reserve_mb"],
                    disk_admission_mb=config.host_capacity["disk_admission_mb_per_worker"],
                ),
                jobs_directory=task_output / "jobs",
                manifest_path=task_output / "manifest.json",
            )
        )
    return tuple(specs), frozen_task_ids


def validate_b_only_sources(specs: tuple[PresmokeTaskSpec, ...]) -> None:
    """在创建任何 attempt 目录前一次性验证全部 A-only 来源。"""
    for spec in specs:
        Task1HostRunner(
            spec.experiment,
            spec.jobs_directory,
            spec.execution,
        ).validate_b_only_source()


def verify_checkout_revision(checkout: Path, expected_revision: str) -> None:
    """确认 checkout 的 HEAD 精确匹配且工作树干净，避免运行漂移版本。"""
    _require_directory(checkout, "checkout")
    completed = _run_checkout_git(checkout, ["rev-parse", "HEAD"], "revision")
    actual = completed.stdout.strip() if completed.returncode == 0 else ""
    if actual != expected_revision:
        raise PresmokeCliError(
            f"checkout revision 不匹配: path={checkout}, expected={expected_revision}, actual={actual or 'unavailable'}"
        )
    status = _run_checkout_git(checkout, ["status", "--porcelain"], "工作树状态")
    if status.returncode != 0:
        raise PresmokeCliError(f"无法读取 checkout 工作树状态: {checkout}")
    if status.stdout.strip():
        raise PresmokeCliError(f"checkout 工作树不干净，拒绝运行: {checkout}")


def verify_acn_revision(
    expected_revision: str,
    expected_main_revision: str,
    expected_version: str,
    checkout: Path = ACN_REPOSITORY,
) -> None:
    """绑定干净的评测 HEAD，并验证其确实基于指定产品提交与版本。"""
    completed = _run_checkout_git(checkout, ["rev-parse", "HEAD"], "ACN revision")
    head = completed.stdout.strip() if completed.returncode == 0 else ""
    if not head:
        raise PresmokeCliError(f"无法读取 ACN revision: {checkout}")
    status = _run_checkout_git(checkout, ["status", "--porcelain"], "ACN 工作树状态")
    if status.returncode != 0:
        raise PresmokeCliError(f"无法读取 ACN 工作树状态: {checkout}")
    if status.stdout.strip():
        raise PresmokeCliError(f"ACN 工作树不干净，正式运行拒绝启动: {checkout}")
    if expected_revision != head:
        raise PresmokeCliError(f"ACN revision 不匹配: expected={expected_revision}, actual={head}")
    ancestor = _run_checkout_git(
        checkout,
        ["merge-base", "--is-ancestor", expected_main_revision, head],
        "ACN 产品基线",
    )
    if ancestor.returncode != 0:
        raise PresmokeCliError(
            f"ACN 评测提交不是指定产品基线的后代: base={expected_main_revision}, head={head}"
        )
    cargo = _run_checkout_git(
        checkout,
        ["show", f"{expected_main_revision}:Cargo.toml"],
        "ACN 产品版本",
    )
    try:
        product_version = tomllib.loads(cargo.stdout)["package"]["version"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise PresmokeCliError("无法从指定产品基线读取 Cargo.toml 版本") from error
    if cargo.returncode != 0 or product_version != expected_version:
        raise PresmokeCliError(
            "ACN 产品基线版本不匹配: "
            f"base={expected_main_revision}, expected={expected_version}, actual={product_version}"
        )


def _run_checkout_git(
    checkout: Path, arguments: list[str], label: str
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(checkout), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise PresmokeCliError(f"无法读取 checkout {label}: {checkout}") from error


def preflight_execution(config: PresmokeConfig) -> Path:
    """真实执行前检查 Pier、Docker 和本阶段所需的官方任务镜像。"""
    verify_pier_executable_binding(config.pier_checkout, config.pier_executable)
    pier_help = _run_preflight_command([str(config.pier_executable), "--help"], "pier --help")
    if pier_help.returncode != 0:
        raise PresmokeCliError(f"pier --help 失败: executable={config.pier_executable}")

    if config.cleanup_stale_pier_resources:
        cleanup = cleanup_stale_pier_resources({config.pier_egress_proxy_image})
        proxy_digest = _docker_image_content_digest(config.pier_egress_proxy_image)
        if proxy_digest != config.pier_egress_proxy_content_digest:
            raise PresmokeCliError(
                "Pier egress proxy 镜像 content digest 不匹配: "
                f"image={config.pier_egress_proxy_image}, "
                f"expected={config.pier_egress_proxy_content_digest}, actual={proxy_digest}"
            )
        _atomic_write_json(
            config.output_dir / "docker-cleanup.json",
            {
                "schema_version": 1,
                "containers_removed": cleanup.containers_removed,
                "image_references_removed": cleanup.image_references_removed,
                "pier_egress_proxy_image": config.pier_egress_proxy_image,
                "pier_egress_proxy_content_digest": proxy_digest,
            },
        )
    else:
        reject_running_containers()
        proxy_digest = _docker_image_content_digest(config.pier_egress_proxy_image)
        if proxy_digest != config.pier_egress_proxy_content_digest:
            raise PresmokeCliError(
                "Pier egress proxy 镜像 content digest 不匹配: "
                f"image={config.pier_egress_proxy_image}, "
                f"expected={config.pier_egress_proxy_content_digest}, actual={proxy_digest}"
            )
    docker_info = _run_preflight_command(
        ["docker", "info", "--format", "{{json .}}"], "docker daemon"
    )
    if docker_info.returncode != 0:
        raise PresmokeCliError("Docker daemon 不可用: docker info 失败")
    docker_root = _verify_docker_capacity(docker_info.stdout, config)
    _ensure_frozen_task_images_available(config, docker_root)
    return docker_root


def _ensure_frozen_task_images_available(config: PresmokeConfig, docker_root: Path) -> None:
    """预拉取本阶段去重后的官方镜像，再冻结其不可变 content digest。"""
    manifest = FrozenDatasetManifest.from_dict(
        _read_object(config.frozen_manifest, "冻结 manifest")
    )
    images = {
        _task_image(config.normalized_root / task_id / "task.toml", task_id)
        for task_id in manifest.task_ids
    }
    for image in sorted(images):
        try:
            _docker_image_content_digest(image)
            continue
        except PresmokeCliError:
            pass
        if image.startswith("hb__"):
            raise PresmokeCliError(f"本地评测镜像不存在，拒绝拉取: {image}")
        pulled = subprocess.run(
            ["docker", "pull", image], check=False, capture_output=True, text=True
        )
        if pulled.returncode != 0:
            raise PresmokeCliError(f"无法拉取官方 Docker image: {image}")
        _docker_image_content_digest(image)
        verify_disk_headroom(
            (config.output_dir, docker_root),
            config.host_capacity["disk_reserve_mb"]
            + config.task_workers * config.host_capacity["disk_admission_mb_per_worker"],
        )


def stage_python_runtime(
    config: PresmokeConfig, *, allow_existing: bool = False
) -> FrozenPythonRuntime:
    """把本次执行会 import 的 ACN/Pier 源码冻结；续跑只能复用既有定版。"""
    verify_acn_revision(
        config.acn_revision,
        config.acn_main_revision,
        config.acn_version,
    )
    sources = (
        (ACN_SOURCE_ROOT / "acn_deepswe", "ACN evaluation package"),
        (config.pier_checkout / "src" / "pier", "Pier package"),
        (config.frozen_skill, "frozen skill"),
    )
    source_hashes: dict[str, str] = {}
    for source, label in sources:
        _require_directory(source, label)
        source_hashes[label] = _runtime_tree_hash(source)
    _verify_acn_eval_build_info(
        config.acn_eval,
        expected_revision=config.acn_revision,
        expected_version=config.acn_version,
    )

    target = config.output_dir / "frozen-python"
    if target.exists():
        if allow_existing:
            return _load_frozen_python_runtime(target)
        raise PresmokeCliError(f"冻结 Python runtime 已存在，拒绝复用: {target}")
    config.output_dir.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".frozen-python.", dir=config.output_dir))
    try:
        acn_source_root = temporary / "acn-deepswe" / "src"
        pier_source_root = temporary / "pier" / "src"
        frozen_skill = temporary / "acn-deepswe" / "assets" / "coding-benchmark"
        staged_pier = temporary / "pier" / "bin" / "pier"
        staged_acn_eval = temporary / "acn-deepswe" / "bin" / "acn_eval"
        _copy_runtime_tree(ACN_SOURCE_ROOT / "acn_deepswe", acn_source_root / "acn_deepswe")
        _copy_runtime_tree(config.pier_checkout / "src" / "pier", pier_source_root / "pier")
        _copy_runtime_tree(config.frozen_skill, frozen_skill)
        staged_pier.parent.mkdir(parents=True)
        shutil.copy2(config.pier_executable, staged_pier, follow_symlinks=False)
        staged_acn_eval.parent.mkdir(parents=True)
        shutil.copy2(config.acn_eval, staged_acn_eval, follow_symlinks=False)
        _make_files_read_only(temporary)
        staged_source_hashes = {
            "ACN evaluation package": sha256_directory_tree(acn_source_root / "acn_deepswe"),
            "Pier package": sha256_directory_tree(pier_source_root / "pier"),
            "frozen skill": sha256_directory_tree(frozen_skill),
        }
        if staged_source_hashes != source_hashes:
            raise PresmokeCliError("冻结 runtime 与复制前源码 hash 不一致")
        verify_acn_revision(
            config.acn_revision,
            config.acn_main_revision,
            config.acn_version,
        )
        acn_hash = sha256_directory_tree(acn_source_root)
        pier_hash = sha256_directory_tree(pier_source_root)
        acn_binary_hash = _sha256_file(staged_acn_eval)
        temporary.replace(target)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    return FrozenPythonRuntime(
        acn_source_root=target / "acn-deepswe" / "src",
        pier_source_root=target / "pier" / "src",
        frozen_skill=target / "acn-deepswe" / "assets" / "coding-benchmark",
        pier_executable=target / "pier" / "bin" / "pier",
        acn_eval=target / "acn-deepswe" / "bin" / "acn_eval",
        acn_binary_hash=acn_binary_hash,
        acn_package_tree_hash=acn_hash,
        pier_package_tree_hash=pier_hash,
    )


def _load_frozen_python_runtime(target: Path) -> FrozenPythonRuntime:
    """续跑使用第一次运行留下的只读 runtime，不能混入当前工作树。"""
    acn_source_root = target / "acn-deepswe" / "src"
    pier_source_root = target / "pier" / "src"
    frozen_skill = target / "acn-deepswe" / "assets" / "coding-benchmark"
    pier_executable = target / "pier" / "bin" / "pier"
    acn_eval = target / "acn-deepswe" / "bin" / "acn_eval"
    _require_directory(acn_source_root / "acn_deepswe", "冻结 ACN evaluation package")
    _require_directory(pier_source_root / "pier", "冻结 Pier package")
    _require_file(frozen_skill / "SKILL.md", "冻结 frozen_skill/SKILL.md")
    _require_file(pier_executable, "冻结 pier executable")
    _require_file(acn_eval, "冻结 acn_eval")
    if pier_executable.stat().st_mode & stat.S_IWUSR:
        raise PresmokeCliError("冻结 pier executable 不可写保护")
    if acn_eval.stat().st_mode & stat.S_IWUSR:
        raise PresmokeCliError("冻结 acn_eval 不可写保护")
    return FrozenPythonRuntime(
        acn_source_root=acn_source_root,
        pier_source_root=pier_source_root,
        frozen_skill=frozen_skill,
        pier_executable=pier_executable,
        acn_eval=acn_eval,
        acn_binary_hash=_sha256_file(acn_eval),
        acn_package_tree_hash=sha256_directory_tree(acn_source_root),
        pier_package_tree_hash=sha256_directory_tree(pier_source_root),
    )


def _next_resume_root(output_dir: Path) -> Path:
    """每次续跑使用新目录，保留中断 task 的半成品供审计。"""
    parent = output_dir / "resumes"
    for index in range(1, 10_000):
        candidate = parent / f"resume-{index:03d}"
        if not candidate.exists():
            return candidate
    raise PresmokeCliError("续跑目录编号耗尽")


def _task_has_partial_artifacts(spec: PresmokeTaskSpec) -> bool:
    """原 task 目录或任一 arm 输出已出现，即视为中断而非尚未调度。"""
    source_manifest = spec.execution.a_only_source_manifest if spec.execution is not None else None
    return spec.manifest_path.exists() or any(
        Path(attempt.output_path).exists()
        for attempt in spec.experiment.attempts
        if source_manifest is None or attempt.variant != "A"
    )


def _write_resume_descriptor(
    resume_root: Path,
    config: PresmokeConfig,
    completed: tuple[PresmokeTaskResult, ...],
    specs: tuple[PresmokeTaskSpec, ...],
) -> None:
    """将跳过与重跑的 task 边界落盘，避免最终汇总误把半成品当结果。"""
    _atomic_write_json(
        resume_root / "resume.json",
        {
            "schema_version": 1,
            "source_attempt_plan": str(config.attempt_plan),
            "completed_task_ids": [result.task_id for result in completed],
            "rerun_task_ids": [spec.task_id for spec in specs],
            "attempts": [
                attempt.to_dict() for spec in specs for attempt in spec.experiment.attempts
            ],
        },
    )


def _atomic_write_json(path: Path, payload: object) -> None:
    """续跑元数据也使用原子替换，避免再次中断时写出半个 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, ensure_ascii=False, indent=2, sort_keys=True)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        temporary_path.replace(path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def _runtime_tree_hash(source: Path) -> str:
    """按实际冻结规则计算源码 hash，忽略不会复制的解释器缓存。"""
    with tempfile.TemporaryDirectory(prefix="acn-runtime-hash-") as directory:
        return _copy_runtime_tree(source, Path(directory) / "tree")


def _copy_runtime_tree(source: Path, target: Path) -> str:
    shutil.copytree(
        source,
        target,
        symlinks=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )
    return sha256_directory_tree(target)


def _make_files_read_only(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            mode = stat.S_IMODE(path.stat().st_mode)
            path.chmod(mode & ~0o222)


def verify_pier_executable_binding(pier_checkout: Path, pier_executable: Path) -> None:
    """将实际执行的 Pier 限定为冻结 checkout 内 source-installed 的当前源码。"""
    checkout = pier_checkout.resolve()
    executable = pier_executable
    if executable.is_symlink() or not executable.is_file():
        raise PresmokeCliError(f"pier_executable 必须是普通文件: {pier_executable}")
    if not os.access(executable, os.X_OK):
        raise PresmokeCliError(f"pier_executable 不具可执行权限: {pier_executable}")
    if executable.name != "pier" or executable.parent.name != "bin":
        raise PresmokeCliError(f"pier_executable 必须是 venv 的 bin/pier: {pier_executable}")
    try:
        shebang = executable.read_text(encoding="utf-8").splitlines()[0]
    except (OSError, IndexError) as error:
        raise PresmokeCliError(f"无法读取 pier console script: {pier_executable}") from error
    interpreter = Path(shebang.removeprefix("#!"))
    if (
        not shebang.startswith("#!")
        or not interpreter.is_absolute()
        or interpreter.parent != executable.parent
        or not os.access(interpreter, os.X_OK)
    ):
        raise PresmokeCliError("pier console script 未使用同一 venv 的 python")
    evidence = _run_preflight_command(
        [str(interpreter), "-c", _PIER_INSTALL_EVIDENCE_SCRIPT], "pier 安装来源"
    )
    if evidence.returncode != 0:
        raise PresmokeCliError("无法读取 pier 安装版本与来源证据")
    try:
        raw = json.loads(evidence.stdout)
    except json.JSONDecodeError as error:
        raise PresmokeCliError("pier 安装版本与来源证据不是 JSON") from error
    if not isinstance(raw, Mapping):
        raise PresmokeCliError("pier 安装版本与来源证据必须是对象")
    # PyPI metadata 的 version 可能滞后于 DeepSWE 所需的 Pier main revision；可复现性
    # 由 manifest 中的精确 checkout SHA 与下面的 PEP 610 editable 来源共同保证，而非
    # 用一个发布版本号错误地拒绝已冻结的新源码。
    if not isinstance(raw.get("version"), str) or not raw["version"]:
        raise PresmokeCliError("pier 安装来源证据缺少非空 package version")
    direct_url = raw.get("direct_url")
    if not isinstance(direct_url, str):
        raise PresmokeCliError("pier 安装缺少 PEP 610 direct_url 来源证据")
    try:
        direct_url_raw = json.loads(direct_url)
    except json.JSONDecodeError as error:
        raise PresmokeCliError("pier direct_url 来源证据不是 JSON") from error
    if not isinstance(direct_url_raw, Mapping):
        raise PresmokeCliError("pier direct_url 来源证据必须是对象")
    source_url = direct_url_raw.get("url")
    if not isinstance(source_url, str):
        raise PresmokeCliError("pier direct_url 缺少源码 URL")
    directory_info = direct_url_raw.get("dir_info")
    if not isinstance(directory_info, Mapping) or directory_info.get("editable") is not True:
        raise PresmokeCliError("pier 必须以 editable 方式从 frozen checkout 安装")
    source = urlparse(source_url)
    if source.scheme != "file" or source.netloc not in {"", "localhost"}:
        raise PresmokeCliError("pier 必须从 frozen checkout 的本地 file URL 安装")
    if Path(unquote(source.path)).resolve() != checkout:
        raise PresmokeCliError("pier 安装来源不匹配 frozen checkout")


def _run_preflight_command(command: list[str], label: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(command, check=False, capture_output=True, text=True)
    except OSError as error:
        raise PresmokeCliError(f"无法执行 {label}") from error


def _verify_docker_capacity(info_output: str, config: PresmokeConfig) -> Path:
    try:
        raw = json.loads(info_output)
    except json.JSONDecodeError as error:
        raise PresmokeCliError("Docker daemon 返回的 info JSON 无效") from error
    if not isinstance(raw, Mapping):
        raise PresmokeCliError("Docker daemon 返回的 info 必须是 JSON 对象")
    return verify_capacity(
        raw,
        workers=config.task_workers,
        resources=config.resources,
        host_capacity=config.host_capacity,
        output_path=config.output_dir,
    )


def dry_run_summary(
    config: PresmokeConfig,
    specs: tuple[PresmokeTaskSpec, ...],
    frozen_task_ids: tuple[str, ...],
) -> dict[str, object]:
    """仅输出不含 credential 的执行计划。"""
    return {
        "dry_run": True,
        "frozen_manifest": str(config.frozen_manifest),
        "output_dir": str(config.output_dir),
        "model": config.model,
        "response_model": config.response_model,
        "reasoning_effort": config.reasoning_effort,
        "pier_egress_proxy_image": config.pier_egress_proxy_image,
        "pier_egress_proxy_content_digest": config.pier_egress_proxy_content_digest,
        "run_class": config.run_class,
        "acn_main_revision": config.acn_main_revision,
        "acn_version": config.acn_version,
        "model_egress_mode": config.model_egress_mode,
        "harness_mode": config.harness_mode,
        "file_edit_authority_enabled": config.file_edit_authority_enabled,
        "task_workers": config.task_workers,
        "host_capacity": config.host_capacity,
        "run_all_variants_without_claims": config.run_all_variants_without_claims,
        "run_a_only": config.run_a_only,
        "phase_mode": (
            "b_only_from_a"
            if config.b_only_from_a_output_dir is not None
            else ("a_only" if config.run_a_only else "full")
        ),
        "b_only_from_a_output_dir": (
            str(config.b_only_from_a_output_dir)
            if config.b_only_from_a_output_dir is not None
            else None
        ),
        "progress": config.progress,
        "task_order": list(frozen_task_ids),
        "tasks": [
            {
                "task_id": spec.task_id,
                "arms": [
                    attempt.variant
                    for attempt in spec.experiment.attempts
                    if config.b_only_from_a_output_dir is None or attempt.variant != "A"
                ],
            }
            for spec in specs
        ],
    }


def _attempts_by_task(
    plan: AttemptPlan, task_ids: tuple[str, ...]
) -> dict[str, tuple[AttemptManifest, ...]]:
    grouped: dict[str, list[AttemptManifest]] = {task_id: [] for task_id in task_ids}
    for attempt in plan.attempts:
        if attempt.task_id not in grouped:
            raise PresmokeCliError(f"attempt plan 包含未冻结 task: {attempt.task_id}")
        grouped[attempt.task_id].append(attempt)
    typed: dict[str, tuple[AttemptManifest, ...]] = {}
    for task_id in task_ids:
        attempts = tuple(grouped[task_id])
        variants = tuple(item.variant for item in attempts)
        if (
            len(attempts) != 4
            or variants[0] != "A"
            or set(variants[1:]) != {"B_empty", "B_claim", "B_forced_claim"}
        ):
            raise PresmokeCliError(f"attempt plan task 四臂无效: {task_id}")
        typed[task_id] = attempts
    return typed


def _read_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PresmokeCliError(f"无法读取 {label}: {path}") from error
    if not isinstance(value, dict):
        raise PresmokeCliError(f"{label} 必须是 JSON 对象: {path}")
    return value


def _absolute_path(raw: Mapping[str, object], field: str) -> Path:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise PresmokeCliError(f"config 缺少绝对路径字段: {field}")
    path = Path(value)
    if not path.is_absolute():
        raise PresmokeCliError(f"config.{field} 必须为绝对路径: {value}")
    return path


def _content_digest(raw: Mapping[str, object], field: str) -> str:
    value = _nonempty_string(raw, field)
    if (
        len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise PresmokeCliError(f"config.{field} 必须是 sha256 content digest")
    return value


def _optional_absolute_path(raw: Mapping[str, object], field: str) -> Path | None:
    value = raw.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise PresmokeCliError(f"{field} 必须是绝对路径字符串或 null")
    path = Path(value)
    if not path.is_absolute():
        raise PresmokeCliError(f"{field} 必须是绝对路径: {path}")
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
        raise PresmokeCliError(f"config 缺少非空字符串字段: {field}")
    return value


def _git_revision(raw: Mapping[str, object], field: str) -> str:
    value = _nonempty_string(raw, field)
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise PresmokeCliError(f"config.{field} 必须是完整的 40 位小写 Git commit")
    return value


def _version(raw: Mapping[str, object], field: str) -> str:
    value = _nonempty_string(raw, field)
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
        raise PresmokeCliError(f"config.{field} 必须是 x.y.z 版本")
    return value


def _run_class(raw: Mapping[str, object]) -> str:
    value = raw.get("run_class")
    if value not in {"formal", "diagnostic"}:
        raise PresmokeCliError("config.run_class 仅支持 formal 或 diagnostic")
    return value


def _model_egress_mode(raw: Mapping[str, object]) -> str:
    """默认使用 Pier allowlist；direct 仅能从冻结 config 显式选择。"""
    value = raw.get("model_egress_mode", "pier")
    if value not in {"pier", "direct"}:
        raise PresmokeCliError("config.model_egress_mode 仅支持 pier 或 direct")
    return value


def _harness_mode(raw: Mapping[str, object]) -> str:
    value = raw.get("harness_mode", "standard")
    if value not in {"standard", "minimal"}:
        raise PresmokeCliError("config.harness_mode 仅支持 standard 或 minimal")
    return value


def _positive_int_mapping(
    raw: Mapping[str, object], field: str, required: tuple[str, ...]
) -> dict[str, int]:
    value = raw.get(field)
    if not isinstance(value, Mapping):
        raise PresmokeCliError(f"config.{field} 必须是整数对象")
    missing = sorted(set(required) - set(value))
    unknown = sorted(set(value) - set(required))
    if missing:
        raise PresmokeCliError(f"config.{field} 缺少字段: " + ",".join(missing))
    if unknown:
        raise PresmokeCliError(f"config.{field} 包含未知字段: " + ",".join(unknown))
    return {str(key): _positive_int(item, f"config.{field}.{key}") for key, item in value.items()}


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PresmokeCliError(f"{field} 必须为正整数")
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise PresmokeCliError(f"{field} 必须为布尔值")
    return value


def _require_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise PresmokeCliError(f"{label} 必须是存在的文件: {path}")


def _require_directory(path: Path, label: str) -> None:
    if not path.is_dir():
        raise PresmokeCliError(f"{label} 必须是存在的目录: {path}")


def _verify_hash(path: Path, expected: object, label: str) -> None:
    if not isinstance(expected, str) or len(expected) != 64:
        raise PresmokeCliError(f"冻结 manifest 中 {label} 的 SHA-256 无效")
    actual = _sha256_file(path)
    if actual != expected:
        raise PresmokeCliError(f"{label} hash 不匹配: path={path}")


def _verify_directory_hash(actual: str, expected: object, label: str) -> None:
    if not isinstance(expected, str) or len(expected) != 64:
        raise PresmokeCliError(f"冻结 manifest 中 {label} 的 SHA-256 无效")
    if actual != expected:
        raise PresmokeCliError(f"{label} hash 不匹配")


def _read_prompt(path: Path, task_id: str) -> str:
    _require_file(path, f"instruction.md ({task_id})")
    prompt = path.read_text(encoding="utf-8")
    if not prompt.strip():
        raise PresmokeCliError(f"instruction.md 不得为空: {path}")
    return prompt


def _task_image(task_toml: Path, task_id: str) -> str:
    try:
        raw = tomllib.loads(task_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise PresmokeCliError(f"无法解析 normalized task.toml: {task_toml}") from error
    environment = raw.get("environment")
    image = environment.get("docker_image") if isinstance(environment, dict) else None
    if not isinstance(image, str) or not image:
        raise PresmokeCliError(f"normalized task 缺少官方 docker image: {task_id}")
    return image


def _docker_image_content_digest(image: str) -> str:
    completed = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        check=False,
        capture_output=True,
        text=True,
    )
    digest = completed.stdout.strip() if completed.returncode == 0 else ""
    if (
        len(digest) != 71
        or not digest.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in digest[7:])
    ):
        raise PresmokeCliError(
            f"无法解析 Docker image content digest: image={image}, output={digest or 'unavailable'}"
        )
    return digest


def _effective_config_hash(config: PresmokeConfig) -> str:
    public = {
        "model": config.model,
        "response_model": config.response_model,
        "reasoning_effort": config.reasoning_effort,
        "pier_egress_proxy_image": config.pier_egress_proxy_image,
        "pier_egress_proxy_content_digest": config.pier_egress_proxy_content_digest,
        "run_class": config.run_class,
        "acn_main_revision": config.acn_main_revision,
        "acn_version": config.acn_version,
        "model_egress_mode": config.model_egress_mode,
        "harness_mode": config.harness_mode,
        "file_edit_authority_enabled": config.file_edit_authority_enabled,
        "host_capacity": config.host_capacity,
        "cleanup_stale_pier_resources": config.cleanup_stale_pier_resources,
        "resources": config.resources,
        "timeouts": config.timeouts,
        "llm_retry": config.llm_retry,
        "progress": config.progress,
        "run_all_variants_without_claims": config.run_all_variants_without_claims,
        "run_a_only": config.run_a_only,
        "phase_mode": (
            "b_only_from_a"
            if config.b_only_from_a_output_dir is not None
            else ("a_only" if config.run_a_only else "full")
        ),
        "auto_compact_ctx_ratio": EVALUATION_AUTO_COMPACT_CTX_RATIO,
        "file_read_max_chars": EVALUATION_FILE_READ_MAX_CHARS,
        "code_run_max_output_chars": EVALUATION_CODE_RUN_MAX_OUTPUT_CHARS,
    }
    return _sha256_text(json.dumps(public, sort_keys=True, separators=(",", ":")))


def _verify_acn_eval_build_info(
    executable: Path, *, expected_revision: str, expected_version: str
) -> dict[str, str]:
    """验证将实际上传的二进制来自冻结评测 commit，而非同名路径上的旧构建。"""
    try:
        completed = subprocess.run(
            [str(executable), "--build-info-json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PresmokeCliError(f"无法读取 acn_eval 构建身份: {executable}") from error
    if completed.returncode != 0:
        raise PresmokeCliError(f"acn_eval --build-info-json 失败: {executable}")
    try:
        raw = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise PresmokeCliError("acn_eval 构建身份不是 JSON") from error
    if not isinstance(raw, Mapping) or set(raw) != {"version", "commit", "commit_timestamp"}:
        raise PresmokeCliError("acn_eval 构建身份字段不符合冻结契约")
    if not all(isinstance(raw.get(key), str) and raw[key] for key in raw):
        raise PresmokeCliError("acn_eval 构建身份字段必须是非空字符串")
    if raw["commit"] != expected_revision or raw["version"] != expected_version:
        raise PresmokeCliError(
            "acn_eval 构建身份不匹配: "
            f"expected_commit={expected_revision}, actual_commit={raw['commit']}, "
            f"expected_version={expected_version}, actual_version={raw['version']}"
        )
    return {str(key): str(value) for key, value in raw.items()}


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    sys.exit(main())
