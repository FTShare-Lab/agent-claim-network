"""任务目录的可复现冻结；冻结发生在任何 verifier 结果之前。"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .network import _dump_toml, normalize_task_network
from .provenance import TASK_DIRECTORY_HASH_ALGORITHM, sha256_directory_tree

FREEZE_ALGORITHM = "random.sample_without_replacement_v1"
SUPPORTED_FREEZE_ALGORITHMS = frozenset(
    {
        FREEZE_ALGORITHM,
        "official_v1.1_luna_max_extreme_cohorts_v1",
        "official_v1.1_luna_max_user_selected_followup_v1",
        "official_v1.1_acn_harness_claim_canary_v1",
    }
)


class DatasetFreezeError(ValueError):
    """候选任务不完整或冻结输出不可审计时抛出。"""


_LOCAL_AGENT_FINGERPRINT = re.compile(r"^[0-9a-f]{16}$")


def sanitize_docker_image_name(name: str) -> str:
    """与 Pier 本地镜像 tag 规则一致，只用于复用已构建的 hb__ 镜像。"""
    sanitized = name.lower()
    if not re.match(r"^[a-z0-9]", sanitized):
        sanitized = "0" + sanitized
    return re.sub(r"[^a-z0-9._-]", "-", sanitized)


def local_agent_image_name(task_name: str, fingerprint: str) -> str:
    """返回 MiniSWE 预构建 agent 镜像名：hb__<task>__agent-<fingerprint>。"""
    if not task_name.strip():
        raise DatasetFreezeError("task.name 不能为空")
    if not _LOCAL_AGENT_FINGERPRINT.fullmatch(fingerprint):
        raise DatasetFreezeError("reuse_local_agent_image_fingerprint 必须是 16 位小写十六进制")
    return sanitize_docker_image_name(f"hb__{task_name}__agent-{fingerprint}")


def retarget_normalized_task_to_local_agent_image(task_dir: Path, fingerprint: str) -> str:
    """把冻结副本的 docker_image 改写为本地 hb__ agent 镜像，避免再 build 一层。"""
    path = task_dir / "task.toml"
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise DatasetFreezeError(f"无法读取冻结 task.toml: {task_dir}") from error
    task = data.get("task")
    environment = data.get("environment")
    if not isinstance(task, dict) or not isinstance(task.get("name"), str):
        raise DatasetFreezeError(f"冻结 task.toml 缺少 task.name: {task_dir}")
    if not isinstance(environment, dict) or not isinstance(environment.get("docker_image"), str):
        raise DatasetFreezeError(f"冻结 task.toml 缺少 environment.docker_image: {task_dir}")
    image = local_agent_image_name(task["name"], fingerprint)
    environment["docker_image"] = image
    path.write_text(_dump_toml(data), encoding="utf-8")
    return image


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
    if not manifest_path.is_absolute():
        raise DatasetFreezeError(f"冻结 manifest 输出必须为绝对路径: {manifest_path}")
    manifest = _select_dataset(tasks_root, seed, sample_size)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def freeze_execution_dataset(
    tasks_root: Path,
    manifest_path: Path,
    normalized_root: Path,
    deepswe_checkout: Path,
    pier_checkout: Path,
    seed: int,
    sample_size: int = 5,
    reuse_local_agent_image_fingerprint: str | None = None,
) -> FrozenDatasetManifest:
    """冻结可直接执行的任务集，并生成关闭普通公网的完整任务副本。"""
    if not manifest_path.is_absolute():
        raise DatasetFreezeError(f"冻结 manifest 输出必须为绝对路径: {manifest_path}")
    if not normalized_root.is_absolute():
        raise DatasetFreezeError(f"normalized_root 必须为绝对路径: {normalized_root}")
    if manifest_path.exists():
        raise DatasetFreezeError(f"冻结 manifest 已存在，拒绝覆盖: {manifest_path}")
    if normalized_root.exists():
        raise DatasetFreezeError(f"normalized_root 已存在，拒绝覆盖: {normalized_root}")

    resolved_tasks_root = tasks_root.resolve()
    resolved_deepswe = deepswe_checkout.resolve()
    try:
        resolved_tasks_root.relative_to(resolved_deepswe)
    except ValueError as error:
        raise DatasetFreezeError("tasks_root 必须位于 deepswe_checkout 内") from error
    deepswe_revision = _clean_checkout_revision(resolved_deepswe, "DeepSWE")
    pier_revision = _clean_checkout_revision(pier_checkout.resolve(), "Pier")
    manifest = _select_dataset(resolved_tasks_root, seed, sample_size)

    normalized_root.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(
        tempfile.mkdtemp(prefix=f".{normalized_root.name}.", dir=normalized_root.parent)
    )
    try:
        task_toml_hashes: dict[str, dict[str, str]] = {}
        task_directory_hashes: dict[str, dict[str, str]] = {}
        for task_id in manifest.task_ids:
            source_task = resolved_tasks_root / task_id
            normalized = normalize_task_network(source_task, temporary_root)
            normalized_task = temporary_root / task_id
            if reuse_local_agent_image_fingerprint is not None:
                retarget_normalized_task_to_local_agent_image(
                    normalized_task, reuse_local_agent_image_fingerprint
                )
            normalized_toml = (normalized_task / "task.toml").read_bytes()
            task_toml_hashes[task_id] = {
                "source": normalized.source_hash,
                "normalized": hashlib.sha256(normalized_toml).hexdigest(),
            }
            task_directory_hashes[task_id] = {
                "source": sha256_directory_tree(source_task),
                "normalized": sha256_directory_tree(normalized_task),
            }

        payload = {
            **manifest.to_dict(),
            "deepswe_revision": deepswe_revision,
            "pier_revision": pier_revision,
            "task_directory_hash_algorithm": TASK_DIRECTORY_HASH_ALGORITHM,
            "task_toml_hashes": task_toml_hashes,
            "task_directory_hashes": task_directory_hashes,
        }
        _write_frozen_manifest(manifest_path, payload)
        temporary_root.replace(normalized_root)
    except Exception:
        if temporary_root.exists():
            shutil.rmtree(temporary_root)
        if manifest_path.exists():
            manifest_path.unlink()
        raise
    return manifest


def _select_dataset(tasks_root: Path, seed: int, sample_size: int) -> FrozenDatasetManifest:
    """从稳定候选集构建冻结对象，但不写入任何执行产物。"""
    if isinstance(sample_size, bool) or not isinstance(sample_size, int) or sample_size <= 0:
        raise DatasetFreezeError("抽样任务数必须为正整数")
    if not tasks_root.is_dir():
        raise DatasetFreezeError(f"任务根目录不存在: {tasks_root}")
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
    return manifest


def _clean_checkout_revision(checkout: Path, label: str) -> str:
    """冻结前拒绝使用没有精确 revision 或含本地改动的 checkout。"""
    if not checkout.is_dir():
        raise DatasetFreezeError(f"{label} checkout 不存在: {checkout}")
    try:
        revision = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
        status = subprocess.run(
            ["git", "-C", str(checkout), "status", "--porcelain"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise DatasetFreezeError(f"无法读取 {label} checkout: {checkout}") from error
    head = revision.stdout.strip() if revision.returncode == 0 else ""
    if not head:
        raise DatasetFreezeError(f"无法读取 {label} revision: {checkout}")
    if status.returncode != 0:
        raise DatasetFreezeError(f"无法读取 {label} 工作树状态: {checkout}")
    if status.stdout.strip():
        raise DatasetFreezeError(f"{label} 工作树不干净，拒绝冻结: {checkout}")
    return head


def _write_frozen_manifest(manifest_path: Path, payload: dict[str, object]) -> None:
    """完整 manifest 只在所有输入已验证后以原子替换方式写入。"""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{manifest_path.name}.", suffix=".tmp", dir=manifest_path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(manifest_path)
    finally:
        if temporary.exists():
            temporary.unlink()
