"""正式评测门禁所需的不可变版本与输入来源记录。"""

from __future__ import annotations

import hashlib
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

TASK_DIRECTORY_HASH_ALGORITHM = "sha256_directory_tree_v2"


class _HashDigest(Protocol):
    def update(self, data: bytes) -> None: ...


@dataclass(frozen=True)
class EvaluationProvenance:
    deepswe_revision: str
    pier_revision: str
    acn_revision: str
    acn_binary_hash: str
    acn_config_hash: str
    dataset_candidates_hash: str
    dataset_seed: int
    dataset_task_ids: tuple[str, ...]
    skill_hash: str
    acn_package_tree_hash: str
    pier_package_tree_hash: str
    source_task_tree_hash: str
    normalized_task_tree_hash: str
    agent_image_reference_sha256: str
    verifier_image_reference_sha256: str
    agent_image_content_digest: str | None
    verifier_image_content_digest: str | None
    model: str
    reasoning_effort: str
    resources: Mapping[str, int]
    timeouts: Mapping[str, int]
    llm_retry: Mapping[str, int]
    network_translation_warning: str

    def to_dict(self) -> dict[str, object]:
        return {
            "deepswe_revision": self.deepswe_revision,
            "pier_revision": self.pier_revision,
            "acn_revision": self.acn_revision,
            "acn_binary_hash": self.acn_binary_hash,
            "acn_config_hash": self.acn_config_hash,
            "dataset_candidates_hash": self.dataset_candidates_hash,
            "dataset_seed": self.dataset_seed,
            "dataset_task_ids": list(self.dataset_task_ids),
            "skill_hash": self.skill_hash,
            "acn_package_tree_hash": self.acn_package_tree_hash,
            "pier_package_tree_hash": self.pier_package_tree_hash,
            "source_task_tree_hash": self.source_task_tree_hash,
            "normalized_task_tree_hash": self.normalized_task_tree_hash,
            "agent_image_reference_sha256": self.agent_image_reference_sha256,
            "verifier_image_reference_sha256": self.verifier_image_reference_sha256,
            "agent_image_content_digest": self.agent_image_content_digest,
            "verifier_image_content_digest": self.verifier_image_content_digest,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "resources": dict(self.resources),
            "timeouts": dict(self.timeouts),
            "llm_retry": dict(self.llm_retry),
            "network_translation_warning": self.network_translation_warning,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> EvaluationProvenance:
        strings = (
            "deepswe_revision",
            "pier_revision",
            "acn_revision",
            "acn_binary_hash",
            "acn_config_hash",
            "dataset_candidates_hash",
            "skill_hash",
            "acn_package_tree_hash",
            "pier_package_tree_hash",
            "source_task_tree_hash",
            "normalized_task_tree_hash",
            "agent_image_reference_sha256",
            "verifier_image_reference_sha256",
            "model",
            "reasoning_effort",
            "network_translation_warning",
        )
        values: dict[str, str] = {}
        for key in strings:
            value = data.get(key)
            if not isinstance(value, str) or not value:
                raise ValueError(f"provenance 缺少字符串字段: {key}")
            values[key] = value
        task_ids = data.get("dataset_task_ids")
        seed = data.get("dataset_seed")
        resources = data.get("resources")
        timeouts = data.get("timeouts")
        llm_retry = data.get("llm_retry")
        if not isinstance(task_ids, list) or not all(isinstance(item, str) for item in task_ids):
            raise ValueError("provenance.dataset_task_ids 必须是字符串数组")
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise ValueError("provenance.dataset_seed 必须是整数")
        if not all(isinstance(item, Mapping) for item in (resources, timeouts, llm_retry)):
            raise ValueError("provenance.resources/timeouts/llm_retry 必须是对象")
        typed_resources = {
            str(key): value
            for key, value in resources.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }
        typed_timeouts = {
            str(key): value
            for key, value in timeouts.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }
        typed_retry = {
            str(key): value
            for key, value in llm_retry.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }
        if (
            len(typed_resources) != len(resources)
            or len(typed_timeouts) != len(timeouts)
            or len(typed_retry) != len(llm_retry)
        ):
            raise ValueError("provenance.resources/timeouts/llm_retry 的值必须是整数")
        image_digests: dict[str, str | None] = {}
        for key in ("agent_image_content_digest", "verifier_image_content_digest"):
            value = data.get(key)
            if value is not None and (
                not isinstance(value, str)
                or len(value) != 71
                or not value.startswith("sha256:")
                or any(character not in "0123456789abcdef" for character in value[7:])
            ):
                raise ValueError(f"provenance.{key} 必须是 sha256 content digest 或 null")
            image_digests[key] = value
        return cls(
            dataset_seed=seed,
            dataset_task_ids=tuple(task_ids),
            resources=typed_resources,
            timeouts=typed_timeouts,
            llm_retry=typed_retry,
            **image_digests,
            **values,
        )


def sha256_directory_tree(path: Path) -> str:
    """计算 v2 目录树 SHA-256，覆盖目录、文件内容及可执行位。"""
    if path.is_symlink():
        raise ValueError(f"目录 tree hash 不允许 symlink: {path}")
    if not path.is_dir():
        raise ValueError(f"目录 tree hash 输入必须是目录: {path}")
    digest = hashlib.sha256()
    for item in sorted(
        path.rglob("*"), key=lambda candidate: candidate.relative_to(path).as_posix()
    ):
        relative = item.relative_to(path).as_posix()
        metadata = item.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"目录 tree hash 不允许 symlink: {relative}")
        encoded_path = relative.encode("utf-8")
        if stat.S_ISDIR(metadata.st_mode):
            _hash_tree_entry(digest, b"directory", encoded_path)
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"目录 tree hash 只允许普通文件: {relative}")
        content = item.read_bytes()
        _hash_tree_entry(digest, b"file", encoded_path)
        digest.update((metadata.st_mode & 0o111).to_bytes(1, "big"))
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _hash_tree_entry(digest: _HashDigest, entry_type: bytes, relative_path: bytes) -> None:
    digest.update(entry_type)
    digest.update(b"\0")
    digest.update(len(relative_path).to_bytes(8, "big"))
    digest.update(relative_path)
