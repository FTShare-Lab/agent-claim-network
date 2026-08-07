"""评测三臂共同注入的冻结静态资产。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .provenance import sha256_directory_tree


@dataclass(frozen=True)
class FrozenSkillAsset:
    asset_id: str
    source_path: Path
    content_hash: str


def frozen_coding_benchmark_skill() -> FrozenSkillAsset:
    """返回三臂共同使用的最小通用 coding skill 及其内容哈希。"""
    source = Path(__file__).resolve().parents[2] / "assets" / "coding-benchmark"
    if not (source / "SKILL.md").is_file():
        raise FileNotFoundError(f"缺少 coding-benchmark skill: {source}")
    return FrozenSkillAsset("coding-benchmark", source, sha256_directory_tree(source))
