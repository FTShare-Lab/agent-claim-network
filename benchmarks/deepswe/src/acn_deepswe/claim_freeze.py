"""基于 host event ledger 中 freeze barrier 的稳定 claim bundle 冻结。"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .rust_contract import read_rust_event_ledger
from .schemas import EventLedger

CLAIM_SNAPSHOT_EVENT = "claim_snapshot"
FREEZE_BARRIER_EVENT = "claim_freeze_barrier"
EXCLUDED_STATUSES = frozenset({"stale", "disputed", "deprecated"})


class ClaimFreezeError(ValueError):
    """claim bundle 不能由可审计 barrier 唯一确定时抛出。"""


@dataclass(frozen=True)
class FrozenClaim:
    claim_id: str
    claim: dict[str, object]
    content_hash: str

    def to_dict(self) -> dict[str, object]:
        return dict(self.claim)

    def manifest_dict(self) -> dict[str, str]:
        return {"claim_id": self.claim_id, "content_hash": self.content_hash}


@dataclass(frozen=True)
class ClaimBundle:
    schema_version: int
    attempt_id: str
    barrier_seq: int
    source_ledger_hash: str
    claims: tuple[FrozenClaim, ...]
    bundle_hash: str

    def to_dict(self) -> dict[str, object]:
        """这是 Rust `FrozenClaimBundle` 可直接读取的 bundle，不能加入审计字段。"""
        return {
            "schema_version": self.schema_version,
            "claims": [claim.to_dict() for claim in self.claims],
        }

    def manifest_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "attempt_id": self.attempt_id,
            "barrier_seq": self.barrier_seq,
            "source_ledger_hash": self.source_ledger_hash,
            "claims": [claim.manifest_dict() for claim in self.claims],
            "bundle_hash": self.bundle_hash,
        }


def freeze_claim_bundle(host_ledger: Path, attempt_id: str, output_path: Path) -> ClaimBundle:
    """仅冻结 barrier 前、同一 attempt、状态 active 的 claim_snapshot。"""
    if not output_path.is_absolute():
        raise ClaimFreezeError(f"claim bundle 输出必须为绝对路径: {output_path}")
    metadata_path = output_path.with_name(output_path.name + ".manifest.json")
    if output_path.exists() or metadata_path.exists():
        raise ClaimFreezeError(f"claim bundle 输出已存在，拒绝覆盖: {output_path}")
    events = read_rust_event_ledger(host_ledger)
    target = [event for event in events if event.attempt_id == attempt_id]
    _validate_monotonic_sequences(target, attempt_id)
    barriers = [event for event in target if event.event_type == FREEZE_BARRIER_EVENT]
    if len(barriers) != 1:
        raise ClaimFreezeError(f"attempt {attempt_id} 必须恰有一个 freeze barrier")
    barrier = barriers[0]
    if not isinstance(barrier.payload.get("barrier_id"), str):
        raise ClaimFreezeError("freeze barrier 缺少 barrier_id")
    snapshots: dict[str, FrozenClaim] = {}
    for event in target:
        if event.seq >= barrier.seq or event.event_type != CLAIM_SNAPSHOT_EVENT:
            continue
        snapshot = _snapshot(event.payload)
        if snapshot is None:
            continue
        snapshots[snapshot.claim_id] = snapshot
    claims = tuple(sorted(snapshots.values(), key=lambda item: item.claim_id))
    source_hash = hashlib.sha256(host_ledger.read_bytes()).hexdigest()
    bundle_body = {"schema_version": 1, "claims": [claim.to_dict() for claim in claims]}
    bundle_bytes = _bundle_file_bytes(bundle_body)
    bundle_hash = hashlib.sha256(bundle_bytes).hexdigest()
    bundle = ClaimBundle(1, attempt_id, barrier.seq, source_hash, claims, bundle_hash)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_new(output_path, bundle_bytes)
    _atomic_write_new(
        metadata_path,
        (
            json.dumps(bundle.manifest_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8"),
    )
    return bundle


def append_freeze_barrier(host_ledger: Path, attempt_id: str, barrier_id: str) -> EventLedger:
    """仅在该 attempt 正常结束后，由宿主追加唯一且 fsync 的 freeze barrier。"""
    if not host_ledger.is_absolute() or not host_ledger.is_file():
        raise ClaimFreezeError(f"host event ledger 必须是存在的绝对路径: {host_ledger}")
    if not barrier_id:
        raise ClaimFreezeError("barrier_id 不能为空")
    events = read_rust_event_ledger(host_ledger)
    target = [event for event in events if event.attempt_id == attempt_id]
    if not target:
        raise ClaimFreezeError(f"host event ledger 未找到 attempt: {attempt_id}")
    _validate_monotonic_sequences(target, attempt_id)
    if any(event.event_type == FREEZE_BARRIER_EVENT for event in target):
        raise ClaimFreezeError(f"attempt {attempt_id} 已有 freeze barrier")
    if target[-1].event_type != "attempt_finished":
        raise ClaimFreezeError(f"attempt {attempt_id} 尚未以 attempt_finished 结束")
    barrier = EventLedger(
        schema_version=1,
        attempt_id=attempt_id,
        seq=target[-1].seq + 1,
        event_type=FREEZE_BARRIER_EVENT,
        timestamp_utc=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        payload={"barrier_id": barrier_id},
    )
    encoded = (
        json.dumps(barrier.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    )
    with host_ledger.open("a", encoding="utf-8") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return barrier


def _validate_monotonic_sequences(events: list[EventLedger], attempt_id: str) -> None:
    sequences = [event.seq for event in events]
    if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
        raise ClaimFreezeError(f"attempt {attempt_id} host event ledger 的 seq 不严格递增")


def _snapshot(payload: Mapping[str, object]) -> FrozenClaim | None:
    raw_claim = payload.get("claim")
    if not isinstance(raw_claim, Mapping) or not all(isinstance(key, str) for key in raw_claim):
        raise ClaimFreezeError("claim_snapshot.claim 必须是 Rust Claim 对象")
    claim = dict(raw_claim)
    status = claim.get("status")
    if status in EXCLUDED_STATUSES:
        return None
    if status != "active":
        raise ClaimFreezeError("claim_snapshot.status 必须为 active/stale/disputed")
    _validate_rust_claim(claim)
    claim_id = _payload_string(claim, "id")
    return FrozenClaim(claim_id, claim, _hash_json(claim))


def _payload_string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise ClaimFreezeError(f"claim_snapshot 缺少非空 {name}")
    return value


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _bundle_file_bytes(bundle: Mapping[str, object]) -> bytes:
    """Rust 消费的 bundle 文件字节；其 SHA-256 是跨边界唯一身份。"""
    return (json.dumps(bundle, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _atomic_write_new(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise ClaimFreezeError(f"claim bundle 输出已存在，拒绝覆盖: {path}") from error
    finally:
        if temporary.exists():
            temporary.unlink()


def _validate_rust_claim(claim: Mapping[str, object]) -> None:
    required_strings = (
        "id",
        "name",
        "statement",
        "scope",
        "holder",
        "created_at",
        "evidence_summary",
    )
    for name in required_strings:
        _payload_string(claim, name)
    if claim.get("confidence") not in {"high", "medium", "low"}:
        raise ClaimFreezeError("claim_snapshot.claim.confidence 无效")
    if not isinstance(claim.get("source_claim_ids"), list) or not all(
        isinstance(value, str) for value in claim["source_claim_ids"]
    ):
        raise ClaimFreezeError("claim_snapshot.claim.source_claim_ids 必须是字符串数组")
