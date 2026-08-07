"""把 DeepSWE 的 `network_mode` 转成 pinned Pier 认识的 `allow_internet`。

pinned Pier 0.3 不读 DeepSWE `task.toml` 里的 `agent.network_mode` /
`verifier.network_mode`。转换必须 fail-closed：只有两者都是 `"no-network"` 才生成
Pier 兼容副本，并对 agent 与 verifier 两个环境显式写入 `allow_internet = false`；
源文件与转换结果的 SHA-256 一并返回，供实验 manifest 冻结。
"""

from __future__ import annotations

import hashlib
import shutil
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

OFFLINE_NETWORK_MODE = "no-network"
NETWORK_TRANSLATION_WARNING = (
    "pinned Pier 不识别 network_mode；已按 no-network 转换为 allow_internet = false"
)


class NetworkNormalizationError(ValueError):
    """task.toml 的网络配置不是官方离线口径，拒绝转换。"""


@dataclass(frozen=True)
class NetworkNormalization:
    """一次目录级转换的结果；两个 hash 都要写进实验 manifest。"""

    task_path: Path
    source_hash: str
    normalized_hash: str
    warning: str


def normalize_task_network(task_directory: Path, output_directory: Path) -> NetworkNormalization:
    """把整个 task 目录复制到 `output_directory`，其中 task.toml 换成 Pier 兼容副本。"""
    source_toml = task_directory / "task.toml"
    if not source_toml.is_file():
        raise NetworkNormalizationError(f"task 目录缺少 task.toml: {task_directory}")
    source_bytes = source_toml.read_bytes()
    normalized_text = normalize_task_toml(source_bytes.decode("utf-8"))

    target = output_directory / task_directory.name
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(task_directory, target)
    normalized_bytes = normalized_text.encode("utf-8")
    (target / "task.toml").write_bytes(normalized_bytes)
    return NetworkNormalization(
        task_path=target,
        source_hash=hashlib.sha256(source_bytes).hexdigest(),
        normalized_hash=hashlib.sha256(normalized_bytes).hexdigest(),
        warning=NETWORK_TRANSLATION_WARNING,
    )


def normalize_task_toml(source_text: str) -> str:
    """返回 Pier 兼容的 task.toml 文本；未通过离线校验则抛错。"""
    try:
        raw = tomllib.loads(source_text)
    except tomllib.TOMLDecodeError as error:
        raise NetworkNormalizationError(f"task.toml 不是合法 TOML: {error}") from error

    for section in ("agent", "verifier"):
        table = raw.get(section)
        if not isinstance(table, Mapping):
            raise NetworkNormalizationError(f"task.toml 缺少 [{section}] 表")
        mode = table.get("network_mode")
        if mode != OFFLINE_NETWORK_MODE:
            raise NetworkNormalizationError(
                f"[{section}].network_mode 必须是 {OFFLINE_NETWORK_MODE!r}，实际为 {mode!r}"
            )

    normalized = _deep_copy(raw)
    _environment_table(normalized)["allow_internet"] = False
    _environment_table(_environment_parent(normalized, "verifier"))["allow_internet"] = False
    return _dump_toml(normalized)


def _environment_parent(root: dict[str, object], key: str) -> dict[str, object]:
    table = root.get(key)
    if not isinstance(table, dict):
        raise NetworkNormalizationError(f"task.toml 缺少 [{key}] 表")
    return table


def _environment_table(parent: dict[str, object]) -> dict[str, object]:
    """取 `[…environment]`；源任务未声明时补一张空表，转换后一定关网。"""
    table = parent.get("environment")
    if table is None:
        table = {}
        parent["environment"] = table
    if not isinstance(table, dict):
        raise NetworkNormalizationError("environment 必须是表")
    return table


def _deep_copy(value: object) -> object:
    if isinstance(value, dict):
        return {key: _deep_copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_deep_copy(item) for item in value]
    return value


def _dump_toml(data: Mapping[str, object]) -> str:
    """按 TOML 规范输出：每张表先写标量，再按出现顺序展开子表。"""
    lines: list[str] = []
    _dump_table(data, (), lines)
    return "\n".join(lines) + "\n"


def _dump_table(table: Mapping[str, object], path: tuple[str, ...], lines: list[str]) -> None:
    if path:
        if lines:
            lines.append("")
        lines.append("[" + ".".join(path) + "]")
    for key, value in table.items():
        if not isinstance(value, dict):
            lines.append(f"{key} = {_dump_value(value)}")
    for key, value in table.items():
        if isinstance(value, dict):
            _dump_table(value, path + (key,), lines)


def _dump_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return _dump_string(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(_dump_value(item) for item in value) + "]"
    raise NetworkNormalizationError(f"task.toml 含无法序列化的值类型: {type(value).__name__}")


def _dump_string(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'
