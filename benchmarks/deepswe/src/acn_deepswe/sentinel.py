"""扫描评估产物中不应出现的 sentinel，阻止任务泄漏进入实验记录。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class SentinelLeakError(ValueError):
    """发现 sentinel 泄漏时抛出。"""


@dataclass(frozen=True)
class SentinelScan:
    root: Path
    files_scanned: int
    matched_files: tuple[Path, ...]


def scan_for_sentinel_leaks(root: Path, sentinels: tuple[str, ...]) -> SentinelScan:
    """稳定遍历普通文件；任意匹配均 fail closed。"""
    if not root.is_dir():
        raise SentinelLeakError(f"sentinel 扫描根目录不存在: {root}")
    if not sentinels or any(not value for value in sentinels):
        raise SentinelLeakError("必须提供非空 sentinel")
    matches: list[Path] = []
    files = [path for path in sorted(root.rglob("*")) if path.is_file() and not path.is_symlink()]
    encoded = tuple(value.encode("utf-8") for value in sentinels)
    for path in files:
        content = path.read_bytes()
        if any(value in content for value in encoded):
            matches.append(path)
    scan = SentinelScan(root, len(files), tuple(matches))
    if matches:
        rendered = ", ".join(str(path.relative_to(root)) for path in matches)
        raise SentinelLeakError(f"sentinel 泄漏: {rendered}")
    return scan
