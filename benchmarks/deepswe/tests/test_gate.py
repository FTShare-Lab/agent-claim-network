import unittest
from dataclasses import replace
from pathlib import Path

from acn_deepswe.gate import AttemptGateInput, GateValidator
from acn_deepswe.rust_contract import RustUsage, read_rust_result
from acn_deepswe.schemas import RouterEvidence, VerifierResult

FIXTURE = Path(__file__).parent / "fixtures" / "rust-acn-eval-result.json"
LIVE_USAGE = RustUsage(
    model_requests=12,
    complete_model_responses=12,
    incomplete_model_responses=0,
    audit_incomplete=False,
    response_models=("fixture-checkpoint",),
    input_tokens=42480,
    output_tokens=1930,
    cache_read_tokens=42368,
    reasoning_tokens=1600,
)


def _verifier(attempt_id: str, *, exit_code: int = 0, passed: bool = False) -> VerifierResult:
    return VerifierResult(
        1, attempt_id, exit_code, passed, "/tmp/verifier.json", "2026-07-26T00:00:00Z"
    )


def _b_claim_input() -> AttemptGateInput:
    return AttemptGateInput(
        attempt_id="attempt-1",
        variant="B_claim",
        artifact_hash="a" * 64,
        verifier=_verifier("attempt-1"),
        usage=LIVE_USAGE,
        expected_response_model="fixture-checkpoint",
        router_evidence=(
            RouterEvidence(
                schema_version=1,
                evidence_id="router-1",
                attempt_id="attempt-1",
                bundle_hash="d" * 64,
                query_hash="e" * 64,
                candidate_claim_ids=("claim-1",),
                selected_claim_ids=("claim-1",),
                injected_claim_ids=("claim-1",),
                injected_content_hashes=("f" * 64,),
                timestamp_utc="2026-07-26T00:00:00Z",
            ),
        ),
        router_evidence_incomplete=False,
        frozen_claim_ids=("claim-1",),
        frozen_bundle_sha256="d" * 64,
        frozen_claim_content_hashes={"claim-1": "f" * 64},
        claim_new_ids=(),
        claim_updated_ids=(),
        claim_used_ids=("claim-1",),
        isolation_checks={"workspace": True, "runtime": True, "environment": True},
    )


class GateValidatorTests(unittest.TestCase):
    def test_valid_b_claim_attempt_passes_and_zero_score_is_valid(self) -> None:
        result = GateValidator().validate(_b_claim_input())

        # verifier 判 0 分（passed=False）是有效实验结果，不是门禁失败。
        self.assertEqual(result.decision, "pass")
        self.assertEqual(result.reason, "ALL_REQUIRED_EVIDENCE_PRESENT")
        self.assertAlmostEqual(
            LIVE_USAGE.to_dict()["cache_hit_rate"],
            42368 / 42480,
        )

    def test_missing_usage_fails_because_token_metric_would_be_unusable(self) -> None:
        no_request = GateValidator().validate(
            replace(
                _b_claim_input(),
                usage=RustUsage(0, 0, 0, False, ("fixture-checkpoint",), 0, 0, 0, 0),
            )
        )
        self.assertIn("NO_MODEL_REQUEST_RECORDED", no_request.reason)

        silent_zero = GateValidator().validate(
            replace(
                _b_claim_input(),
                usage=RustUsage(12, 12, 0, False, ("fixture-checkpoint",), 0, 0, 0, 0),
            )
        )
        self.assertIn("USAGE_NOT_REPORTED", silent_zero.reason)

    def test_incomplete_response_usage_fails_the_gate(self) -> None:
        result = GateValidator().validate(
            replace(
                _b_claim_input(),
                usage=replace(
                    LIVE_USAGE,
                    complete_model_responses=11,
                    incomplete_model_responses=1,
                ),
            )
        )

        self.assertIn("INCOMPLETE_MODEL_USAGE", result.reason)

        audit_failure = GateValidator().validate(
            replace(_b_claim_input(), usage=replace(LIVE_USAGE, audit_incomplete=True))
        )
        self.assertIn("USAGE_AUDIT_INCOMPLETE", audit_failure.reason)

    def test_incomplete_router_audit_fails_the_gate(self) -> None:
        result = GateValidator().validate(
            replace(_b_claim_input(), router_evidence_incomplete=True)
        )
        self.assertIn("ROUTER_EVIDENCE_INCOMPLETE", result.reason)

    def test_response_model_must_match_the_frozen_mapping(self) -> None:
        missing = GateValidator().validate(
            replace(_b_claim_input(), usage=replace(LIVE_USAGE, response_models=()))
        )
        self.assertIn("RESPONSE_MODEL_NOT_REPORTED", missing.reason)

        drifted = GateValidator().validate(
            replace(
                _b_claim_input(),
                usage=replace(LIVE_USAGE, response_models=("other-checkpoint",)),
            )
        )
        self.assertIn("RESPONSE_MODEL_MISMATCH", drifted.reason)

    def test_verifier_that_never_ran_fails_the_gate(self) -> None:
        result = GateValidator().validate(
            replace(_b_claim_input(), verifier=_verifier("attempt-1", exit_code=1))
        )
        self.assertIn("VERIFIER_DID_NOT_RUN", result.reason)

    def test_b_empty_must_not_see_any_claim(self) -> None:
        leaked = replace(
            _b_claim_input(),
            variant="B_empty",
            frozen_claim_ids=(),
            frozen_bundle_sha256=None,
            frozen_claim_content_hashes={},
        )
        result = GateValidator().validate(leaked)

        self.assertEqual(result.decision, "fail")
        self.assertIn("B_EMPTY_ROUTER_NOT_EMPTY", result.reason)

    def test_b_claim_requires_non_empty_bundle_but_not_injection_by_default(self) -> None:
        no_retrieval = GateValidator().validate(
            replace(_b_claim_input(), router_evidence=(), claim_used_ids=())
        )
        self.assertEqual(no_retrieval.decision, "pass")

        empty = replace(
            _b_claim_input(),
            router_evidence=(),
            frozen_claim_ids=(),
            frozen_claim_content_hashes={},
            claim_used_ids=(),
        )
        result = GateValidator().validate(empty)

        self.assertEqual(result.decision, "fail")
        self.assertIn("B_CLAIM_BUNDLE_EMPTY", result.reason)
        self.assertNotIn("B_CLAIM_NOT_INJECTED", result.reason)

    def test_hard_gate_requires_b_claim_injection(self) -> None:
        result = GateValidator().validate(
            replace(
                _b_claim_input(),
                require_claim_injection=True,
                router_evidence=(),
                claim_used_ids=(),
            )
        )

        self.assertIn("B_CLAIM_NOT_INJECTED", result.reason)

    def test_isolation_check_failure_fails_the_gate(self) -> None:
        result = GateValidator().validate(
            replace(
                _b_claim_input(),
                isolation_checks={"workspace": True, "task_agent_no_network": False},
            )
        )
        self.assertIn("ISOLATION_CHECK_FAILED", result.reason)

    def test_gate_input_can_consume_final_rust_result_fixture(self) -> None:
        fixture = read_rust_result(FIXTURE)
        value = AttemptGateInput.from_rust_result(
            fixture,
            variant="B_claim",
            artifact_hash="a" * 64,
            verifier=_verifier("attempt-1", passed=True),
            expected_response_model="fixture-checkpoint",
            frozen_claim_ids=("claim-1",),
            frozen_bundle_sha256="a" * 64,
            frozen_claim_content_hashes={"claim-1": "c" * 64},
            isolation_checks={"workspace": True},
        )

        self.assertEqual(GateValidator().validate(value).decision, "pass")

    def test_b_claim_rejects_bundle_content_attempt_and_hierarchy_tampering(self) -> None:
        base = AttemptGateInput.from_rust_result(
            read_rust_result(FIXTURE),
            variant="B_claim",
            artifact_hash="a" * 64,
            verifier=_verifier("attempt-1", passed=True),
            expected_response_model="fixture-checkpoint",
            frozen_claim_ids=("claim-1",),
            frozen_bundle_sha256="a" * 64,
            frozen_claim_content_hashes={"claim-1": "c" * 64},
            isolation_checks={"ok": True},
        )

        self.assertIn(
            "B_CLAIM_BUNDLE_HASH_MISMATCH",
            GateValidator().validate(replace(base, frozen_bundle_sha256="d" * 64)).reason,
        )
        self.assertIn(
            "B_CLAIM_INJECTED_CONTENT_MISMATCH",
            GateValidator()
            .validate(replace(base, frozen_claim_content_hashes={"claim-1": "d" * 64}))
            .reason,
        )
        bad_evidence = replace(
            base.router_evidence[0],
            attempt_id="other",
            selected_claim_ids=(),
            injected_claim_ids=("claim-1",),
            injected_content_hashes=("c" * 64,),
        )
        result = GateValidator().validate(replace(base, router_evidence=(bad_evidence,)))
        self.assertIn("ROUTER_EVIDENCE_ATTEMPT_MISMATCH", result.reason)
        self.assertIn("B_CLAIM_ROUTER_HIERARCHY_INVALID", result.reason)
