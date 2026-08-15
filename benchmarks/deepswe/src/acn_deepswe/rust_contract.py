"""Rust `acn_eval` 的定版 JSON 契约解析器；拒绝历史字段别名。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .schemas import EventLedger, RouterEvidence


class RustContractError(ValueError):
    """Rust 评估产物不符合双方固定 JSON 契约时抛出。"""


@dataclass(frozen=True)
class RustUsage:
    """`acn_eval` 直接从上游响应累计的 token 用量。"""

    model_requests: int
    complete_model_responses: int
    incomplete_model_responses: int
    audit_incomplete: bool
    response_models: tuple[str, ...]
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    reasoning_tokens: int

    def to_dict(self) -> dict[str, int | float | bool | list[str]]:
        return {
            "model_requests": self.model_requests,
            "complete_model_responses": self.complete_model_responses,
            "incomplete_model_responses": self.incomplete_model_responses,
            "audit_incomplete": self.audit_incomplete,
            "response_models": list(self.response_models),
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_hit_rate": (
                self.cache_read_tokens / self.input_tokens if self.input_tokens else 0.0
            ),
            "reasoning_tokens": self.reasoning_tokens,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> RustUsage:
        def field(name: str) -> int:
            value = data.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RustContractError(f"usage.{name} 必须是非负整数")
            return value

        response_models = data.get("response_models")
        if not isinstance(response_models, list) or not all(
            isinstance(model, str) and model for model in response_models
        ):
            raise RustContractError("usage.response_models 必须是字符串数组")
        usage = cls(
            model_requests=field("model_requests"),
            complete_model_responses=field("complete_model_responses"),
            incomplete_model_responses=field("incomplete_model_responses"),
            audit_incomplete=_boolean(data, "audit_incomplete"),
            response_models=tuple(response_models),
            input_tokens=field("input_tokens"),
            output_tokens=field("output_tokens"),
            cache_read_tokens=field("cache_read_tokens"),
            reasoning_tokens=field("reasoning_tokens"),
        )
        if (
            usage.complete_model_responses + usage.incomplete_model_responses
            != usage.model_requests
        ):
            raise RustContractError(
                "usage.complete_model_responses + incomplete_model_responses "
                "必须等于 model_requests"
            )
        return usage


@dataclass(frozen=True)
class RustEvaluationResult:
    schema_version: int
    attempt_id: str
    exit_type: str
    agent_steps: int
    claim_new_ids: tuple[str, ...]
    claim_updated_ids: tuple[str, ...]
    claim_used_ids: tuple[str, ...]
    router_evidence: tuple[RouterEvidence, ...]
    router_evidence_incomplete: bool
    usage: RustUsage
    event_ledger_path: str
    failure_kind: str | None


def read_rust_event_ledger(path: Path) -> tuple[EventLedger, ...]:
    """读取 Rust JSONL；仅接受 event_type/timestamp_utc 的定版字段名。"""
    if not path.is_file():
        raise RustContractError(f"Rust event ledger 不存在: {path}")
    events: list[EventLedger] = []
    seen_sequences: set[tuple[str, int]] = set()
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = _json_object(line, path, line_number)
        if "event_type" not in raw or "timestamp_utc" not in raw:
            raise RustContractError(f"Rust event 第 {line_number} 行缺少 event_type/timestamp_utc")
        try:
            event = EventLedger.from_dict(raw)
        except ValueError as error:
            raise RustContractError(f"Rust event 第 {line_number} 行无效: {error}") from error
        key = (event.attempt_id, event.seq)
        if key in seen_sequences:
            raise RustContractError(f"Rust event 出现重复 seq: {event.attempt_id}/{event.seq}")
        seen_sequences.add(key)
        events.append(event)
    return tuple(events)


def read_rust_result(path: Path) -> RustEvaluationResult:
    """读取 Rust result.json，router evidence 必须是当前完整 schema。"""
    if not path.is_file():
        raise RustContractError(f"Rust result 不存在: {path}")
    raw = _json_object(path.read_text(encoding="utf-8"), path, None)
    try:
        evidence = _router_evidence(raw)
        return RustEvaluationResult(
            schema_version=_schema_version(raw),
            attempt_id=_string(raw, "attempt_id"),
            exit_type=_string(raw, "exit_type"),
            agent_steps=_integer(raw, "agent_steps"),
            claim_new_ids=_strings(raw, "claim_new_ids"),
            claim_updated_ids=_strings(raw, "claim_updated_ids"),
            claim_used_ids=_strings(raw, "claim_used_ids"),
            router_evidence=evidence,
            router_evidence_incomplete=_boolean(raw, "router_evidence_incomplete"),
            usage=_usage(raw),
            event_ledger_path=_absolute(_string(raw, "event_ledger_path")),
            failure_kind=_failure_kind(raw),
        )
    except ValueError as error:
        raise RustContractError(f"Rust result 无效: {error}") from error


def _json_object(text: str, path: Path, line_number: int | None) -> dict[str, object]:
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as error:
        location = f" 第 {line_number} 行" if line_number is not None else ""
        raise RustContractError(f"{path}{location} 不是合法 JSON") from error
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise RustContractError(f"{path} 必须是 JSON 对象")
    return raw


def _usage(data: Mapping[str, object]) -> RustUsage:
    raw = data.get("usage")
    if not isinstance(raw, Mapping) or not all(isinstance(key, str) for key in raw):
        raise RustContractError("usage 必须是对象")
    return RustUsage.from_dict(raw)


def _router_evidence(data: Mapping[str, object]) -> tuple[RouterEvidence, ...]:
    raw = data.get("router_evidence")
    if not isinstance(raw, list):
        raise RustContractError("router_evidence 必须是数组")
    parsed: list[RouterEvidence] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping) or not all(isinstance(key, str) for key in item):
            raise RustContractError(f"router_evidence[{index}] 必须是对象")
        parsed.append(RouterEvidence.from_dict(item))
    return tuple(parsed)


def _string(data: Mapping[str, object], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise RustContractError(f"{field} 必须是非空字符串")
    return value


def _integer(data: Mapping[str, object], field: str) -> int:
    value = data.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RustContractError(f"{field} 必须是整数")
    return value


def _schema_version(data: Mapping[str, object]) -> int:
    version = _integer(data, "schema_version")
    if version != 1:
        raise RustContractError("schema_version 仅支持 1")
    return version


def _boolean(data: Mapping[str, object], field: str) -> bool:
    value = data.get(field)
    if not isinstance(value, bool):
        raise RustContractError(f"{field} 必须是布尔值")
    return value


def _failure_kind(data: Mapping[str, object]) -> str | None:
    value = data.get("failure_kind")
    if value is None:
        return None
    if value != "upstream_concurrency_exhausted":
        raise RustContractError("failure_kind 不支持")
    return value


def _strings(data: Mapping[str, object], field: str) -> tuple[str, ...]:
    value = data.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RustContractError(f"{field} 必须是字符串数组")
    return tuple(value)


def _absolute(value: str) -> str:
    if not Path(value).is_absolute():
        raise RustContractError("event_ledger_path 必须是绝对路径")
    return value
