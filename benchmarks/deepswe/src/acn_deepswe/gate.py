"""四臂 attempt 的机器可判定硬门禁。

只检查基础设施、claim 归因与隔离是否成立。verifier 判 0 分是有效实验结果，
不算门禁失败；agent 自身失败同理，由调用方按未通过计分。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from .rust_contract import RustEvaluationResult, RustUsage
from .schemas import GateResult, RouterEvidence, VerifierResult

SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class AttemptGateInput:
    attempt_id: str
    variant: str
    artifact_hash: str
    verifier: VerifierResult
    usage: RustUsage
    expected_response_model: str
    router_evidence: tuple[RouterEvidence, ...]
    router_evidence_incomplete: bool
    frozen_claim_ids: tuple[str, ...]
    frozen_bundle_sha256: str | None
    frozen_claim_content_hashes: Mapping[str, str]
    claim_new_ids: tuple[str, ...]
    claim_updated_ids: tuple[str, ...]
    claim_used_ids: tuple[str, ...]
    isolation_checks: Mapping[str, bool]
    require_claim_injection: bool = False
    allow_empty_claim_bundle: bool = False

    @classmethod
    def from_rust_result(
        cls,
        result: RustEvaluationResult,
        *,
        variant: str,
        artifact_hash: str,
        verifier: VerifierResult,
        expected_response_model: str,
        frozen_claim_ids: tuple[str, ...],
        frozen_bundle_sha256: str | None,
        frozen_claim_content_hashes: Mapping[str, str],
        isolation_checks: Mapping[str, bool],
        require_claim_injection: bool = False,
        allow_empty_claim_bundle: bool = False,
    ) -> AttemptGateInput:
        """将 Rust 定版 result 直接映射为 gate 输入，字段不做历史别名兼容。"""
        return cls(
            attempt_id=result.attempt_id,
            variant=variant,
            artifact_hash=artifact_hash,
            verifier=verifier,
            usage=result.usage,
            expected_response_model=expected_response_model,
            router_evidence=result.router_evidence,
            router_evidence_incomplete=result.router_evidence_incomplete,
            frozen_claim_ids=frozen_claim_ids,
            frozen_bundle_sha256=frozen_bundle_sha256,
            frozen_claim_content_hashes=frozen_claim_content_hashes,
            claim_new_ids=result.claim_new_ids,
            claim_updated_ids=result.claim_updated_ids,
            claim_used_ids=result.claim_used_ids,
            isolation_checks=isolation_checks,
            require_claim_injection=require_claim_injection,
            allow_empty_claim_bundle=allow_empty_claim_bundle,
        )


class GateValidator:
    """只验证基础设施、归因与隔离；verifier 0 分不是门禁失败。"""

    def validate(self, value: AttemptGateInput) -> GateResult:
        failures: list[str] = []
        warnings: list[str] = []
        if not SHA256_PATTERN.fullmatch(value.artifact_hash):
            failures.append("ARTIFACT_HASH_INVALID")
        if value.verifier.attempt_id != value.attempt_id:
            failures.append("VERIFIER_ATTEMPT_MISMATCH")
        if value.verifier.verifier_exit_code != 0:
            failures.append("VERIFIER_DID_NOT_RUN")
        self._validate_usage(value.usage, value.expected_response_model, failures, warnings)
        if not value.isolation_checks or not all(value.isolation_checks.values()):
            failures.append("ISOLATION_CHECK_FAILED")
        if value.router_evidence_incomplete:
            failures.append("ROUTER_EVIDENCE_INCOMPLETE")
        self._validate_router(value, failures, warnings)
        reason = ",".join(failures) if failures else "ALL_REQUIRED_EVIDENCE_PRESENT"
        if warnings:
            reason = f"{reason}_WITH_{'_AND_'.join(warnings)}"
        return GateResult(
            1,
            value.attempt_id,
            "task1-attempt",
            "pass" if not failures else "fail",
            reason,
            datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )

    @staticmethod
    def _validate_usage(
        usage: RustUsage,
        expected_response_model: str,
        failures: list[str],
        warnings: list[str],
    ) -> None:
        """成功响应的 usage 必须完整；重试前的中断请求只作审计告警。"""
        if usage.model_requests <= 0:
            failures.append("NO_MODEL_REQUEST_RECORDED")
        elif usage.input_tokens <= 0 or usage.output_tokens <= 0:
            failures.append("USAGE_NOT_REPORTED")
        if usage.incomplete_model_responses:
            # 上游/链路的瞬时失败可由后续请求恢复。保留计数，避免它阻断 verifier
            # 结果和 claim 实验；token 统计由 PRD 规定为成功响应的观测下界。
            warnings.append("INCOMPLETE_MODEL_USAGE")
        if usage.audit_incomplete:
            failures.append("USAGE_AUDIT_INCOMPLETE")
        if not usage.response_models:
            failures.append("RESPONSE_MODEL_NOT_REPORTED")
        elif any(model != expected_response_model for model in usage.response_models):
            failures.append("RESPONSE_MODEL_MISMATCH")

    @staticmethod
    def _validate_router(
        value: AttemptGateInput, failures: list[str], warnings: list[str]
    ) -> None:
        if any(evidence.attempt_id != value.attempt_id for evidence in value.router_evidence):
            failures.append("ROUTER_EVIDENCE_ATTEMPT_MISMATCH")
        candidates = {
            claim_id
            for evidence in value.router_evidence
            for claim_id in evidence.candidate_claim_ids
        }
        selected = {
            claim_id
            for evidence in value.router_evidence
            for claim_id in evidence.selected_claim_ids
        }
        injected = {
            claim_id
            for evidence in value.router_evidence
            for claim_id in evidence.injected_claim_ids
        }
        used = set(value.claim_used_ids)
        if value.variant == "B_empty" and (candidates or selected or injected or used):
            failures.append("B_EMPTY_ROUTER_NOT_EMPTY")
        if value.variant not in {"B_claim", "B_forced_claim"}:
            return
        if len(value.router_evidence) > 1:
            failures.append("CLAIM_DELIVERY_REPEATED")
        frozen = set(value.frozen_claim_ids)
        if value.variant == "B_forced_claim" and frozen and len(value.router_evidence) != 1:
            failures.append("FORCED_CLAIM_DELIVERY_COUNT_INVALID")
        if not frozen:
            if value.allow_empty_claim_bundle:
                warnings.append("EMPTY_CLAIM_BUNDLE")
            else:
                failures.append("CLAIM_BUNDLE_EMPTY")
        if value.frozen_bundle_sha256 is None or not SHA256_PATTERN.fullmatch(
            value.frozen_bundle_sha256
        ):
            failures.append("CLAIM_BUNDLE_HASH_INVALID")
        if value.require_claim_injection and not injected and not (
            value.allow_empty_claim_bundle and not frozen
        ):
            failures.append("CLAIM_NOT_INJECTED")
        if not candidates | selected | injected | used <= frozen:
            failures.append("CLAIM_OUTSIDE_FROZEN_BUNDLE")
        for evidence in value.router_evidence:
            if evidence.bundle_hash != value.frozen_bundle_sha256:
                failures.append("CLAIM_BUNDLE_HASH_MISMATCH")
            candidate_ids = set(evidence.candidate_claim_ids)
            selected_ids = set(evidence.selected_claim_ids)
            injected_ids = evidence.injected_claim_ids
            if not selected_ids <= candidate_ids or not set(injected_ids) <= selected_ids:
                failures.append("CLAIM_ROUTER_HIERARCHY_INVALID")
            if len(evidence.injected_content_hashes) != len(injected_ids):
                failures.append("CLAIM_INJECTED_CONTENT_ARITY_INVALID")
                continue
            for claim_id, content_hash in zip(
                injected_ids, evidence.injected_content_hashes, strict=True
            ):
                if value.frozen_claim_content_hashes.get(claim_id) != content_hash:
                    failures.append("CLAIM_INJECTED_CONTENT_MISMATCH")
        if not used <= injected:
            failures.append("CLAIM_USED_NOT_INJECTED")
