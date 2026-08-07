"""评估过程写入物的版本化 schema 与基础校验。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


class SchemaError(ValueError):
    """评估审计记录不满足稳定 schema 时抛出。"""


def _required(data: Mapping[str, object], field: str) -> object:
    if field not in data:
        raise SchemaError(f"缺少字段: {field}")
    return data[field]


def _string(data: Mapping[str, object], field: str) -> str:
    value = _required(data, field)
    if not isinstance(value, str) or not value:
        raise SchemaError(f"字段 {field} 必须是非空字符串")
    return value


def _optional_string(data: Mapping[str, object], field: str) -> str | None:
    value = data.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SchemaError(f"字段 {field} 必须是非空字符串或 null")
    return value


def _integer(data: Mapping[str, object], field: str) -> int:
    value = _required(data, field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SchemaError(f"字段 {field} 必须是整数")
    return value


def _version(value: int, name: str) -> None:
    if value != 1:
        raise SchemaError(f"{name} 仅支持 schema_version=1")


def _string_list(data: Mapping[str, object], field: str) -> tuple[str, ...]:
    value = _required(data, field)
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise SchemaError(f"字段 {field} 必须是非空字符串数组")
    return tuple(value)


def _absolute_path(value: str, field: str) -> str:
    if not Path(value).is_absolute():
        raise SchemaError(f"字段 {field} 必须是绝对路径: {value}")
    return value


@dataclass(frozen=True)
class AttemptManifest:
    """单个 A/B 尝试的不可变宿主输出定位信息。"""

    schema_version: int
    attempt_id: str
    task_id: str
    variant: str
    output_path: str

    def __post_init__(self) -> None:
        _version(self.schema_version, "AttemptManifest")
        _absolute_path(self.output_path, "output_path")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "attempt_id": self.attempt_id,
            "task_id": self.task_id,
            "variant": self.variant,
            "output_path": self.output_path,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> AttemptManifest:
        return cls(
            schema_version=_integer(data, "schema_version"),
            attempt_id=_string(data, "attempt_id"),
            task_id=_string(data, "task_id"),
            variant=_string(data, "variant"),
            output_path=_absolute_path(_string(data, "output_path"), "output_path"),
        )


@dataclass(frozen=True)
class EventLedger:
    """append-only 事件账本中的一条有序记录。"""

    schema_version: int
    attempt_id: str
    seq: int
    event_type: str
    timestamp_utc: str
    payload: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        _version(self.schema_version, "EventLedger")
        if self.seq < 0:
            raise SchemaError("seq 不得为负数")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "attempt_id": self.attempt_id,
            "seq": self.seq,
            "event_type": self.event_type,
            "timestamp_utc": self.timestamp_utc,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> EventLedger:
        payload = _required(data, "payload")
        if not isinstance(payload, Mapping) or not all(isinstance(key, str) for key in payload):
            raise SchemaError("字段 payload 必须是对象")
        typed_payload: dict[str, JsonValue] = {}
        for key, value in payload.items():
            typed_payload[key] = value  # JSON 的外部 payload 在写入时由 json 再校验。
        return cls(
            schema_version=_integer(data, "schema_version"),
            attempt_id=_string(data, "attempt_id"),
            seq=_integer(data, "seq"),
            event_type=_string(data, "event_type"),
            timestamp_utc=_string(data, "timestamp_utc"),
            payload=typed_payload,
        )


@dataclass(frozen=True)
class RouterEvidence:
    schema_version: int
    evidence_id: str
    attempt_id: str
    bundle_hash: str | None
    query_hash: str
    candidate_claim_ids: tuple[str, ...]
    selected_claim_ids: tuple[str, ...]
    injected_claim_ids: tuple[str, ...]
    injected_content_hashes: tuple[str, ...]
    timestamp_utc: str

    def __post_init__(self) -> None:
        _version(self.schema_version, "RouterEvidence")

    def to_dict(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "schema_version": self.schema_version,
            "evidence_id": self.evidence_id,
            "attempt_id": self.attempt_id,
            "query_hash": self.query_hash,
            "candidate_claim_ids": list(self.candidate_claim_ids),
            "selected_claim_ids": list(self.selected_claim_ids),
            "injected_claim_ids": list(self.injected_claim_ids),
            "injected_content_hashes": list(self.injected_content_hashes),
            "timestamp_utc": self.timestamp_utc,
        }
        if self.bundle_hash is not None:
            result["bundle_hash"] = self.bundle_hash
        return result

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> RouterEvidence:
        return cls(
            _integer(data, "schema_version"),
            _string(data, "evidence_id"),
            _string(data, "attempt_id"),
            _optional_string(data, "bundle_hash"),
            _string(data, "query_hash"),
            _string_list(data, "candidate_claim_ids"),
            _string_list(data, "selected_claim_ids"),
            _string_list(data, "injected_claim_ids"),
            _string_list(data, "injected_content_hashes"),
            _string(data, "timestamp_utc"),
        )


@dataclass(frozen=True)
class VerifierResult:
    schema_version: int
    attempt_id: str
    verifier_exit_code: int
    passed: bool
    result_path: str
    timestamp_utc: str

    def __post_init__(self) -> None:
        _version(self.schema_version, "VerifierResult")
        _absolute_path(self.result_path, "result_path")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "attempt_id": self.attempt_id,
            "verifier_exit_code": self.verifier_exit_code,
            "passed": self.passed,
            "result_path": self.result_path,
            "timestamp_utc": self.timestamp_utc,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> VerifierResult:
        passed = _required(data, "passed")
        if not isinstance(passed, bool):
            raise SchemaError("字段 passed 必须是布尔值")
        return cls(
            _integer(data, "schema_version"),
            _string(data, "attempt_id"),
            _integer(data, "verifier_exit_code"),
            passed,
            _absolute_path(_string(data, "result_path"), "result_path"),
            _string(data, "timestamp_utc"),
        )


@dataclass(frozen=True)
class GateResult:
    schema_version: int
    attempt_id: str
    gate_name: str
    decision: str
    reason: str
    timestamp_utc: str

    def __post_init__(self) -> None:
        _version(self.schema_version, "GateResult")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "attempt_id": self.attempt_id,
            "gate_name": self.gate_name,
            "decision": self.decision,
            "reason": self.reason,
            "timestamp_utc": self.timestamp_utc,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> GateResult:
        return cls(
            _integer(data, "schema_version"),
            _string(data, "attempt_id"),
            _string(data, "gate_name"),
            _string(data, "decision"),
            _string(data, "reason"),
            _string(data, "timestamp_utc"),
        )
