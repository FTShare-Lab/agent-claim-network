import json
import tempfile
import unittest
from pathlib import Path

from acn_deepswe.rust_contract import RustContractError, read_rust_event_ledger, read_rust_result


class RustContractTests(unittest.TestCase):
    def test_parser_consumes_final_rust_event_and_router_evidence_fixture(self) -> None:
        fixture_root = Path(__file__).parent / "fixtures"
        parsed_event = read_rust_event_ledger(fixture_root / "rust-acn-eval-events.jsonl")
        parsed_result = read_rust_result(fixture_root / "rust-acn-eval-result.json")

        self.assertEqual(parsed_event[0].event_type, "finalize_completed")
        self.assertEqual(parsed_event[0].timestamp_utc, "2026-07-26T00:00:00Z")
        self.assertEqual(parsed_result.router_evidence[0].selected_claim_ids, ("claim-1",))
        self.assertEqual(parsed_result.usage.model_requests, 1)
        self.assertEqual(parsed_result.usage.complete_model_responses, 1)
        self.assertEqual(parsed_result.usage.incomplete_model_responses, 0)
        self.assertFalse(parsed_result.usage.audit_incomplete)
        self.assertFalse(parsed_result.router_evidence_incomplete)
        self.assertEqual(parsed_result.usage.response_models, ("fixture-checkpoint",))
        self.assertEqual(parsed_result.usage.input_tokens, 8462)
        self.assertEqual(parsed_result.usage.reasoning_tokens, 41)
        self.assertAlmostEqual(
            parsed_result.usage.to_dict()["cache_hit_rate"],
            0.0,
        )

    def test_python_field_aliases_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "events.jsonl"
            ledger.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "attempt_id": "attempt-1",
                        "seq": 1,
                        "timestamp": "2026-07-26T00:00:00Z",
                        "kind": "wrong",
                        "payload": {},
                    }
                )
                + "\n"
            )
            with self.assertRaisesRegex(RustContractError, "timestamp_utc"):
                read_rust_event_ledger(ledger)

    def test_result_rejects_unknown_schema_version(self) -> None:
        fixture = Path(__file__).parent / "fixtures" / "rust-acn-eval-result.json"
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result.json"
            payload = json.loads(fixture.read_text(encoding="utf-8"))
            payload["schema_version"] = 2
            result.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(RustContractError, "schema_version"):
                read_rust_result(result)

    def test_result_accepts_known_infrastructure_failure_kind_only(self) -> None:
        fixture = Path(__file__).parent / "fixtures" / "rust-acn-eval-result.json"
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result.json"
            raw = json.loads(fixture.read_text(encoding="utf-8"))
            raw["failure_kind"] = "upstream_concurrency_exhausted"
            result.write_text(json.dumps(raw), encoding="utf-8")
            self.assertEqual(
                read_rust_result(result).failure_kind, "upstream_concurrency_exhausted"
            )
            raw["failure_kind"] = "unknown"
            result.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(RustContractError, "failure_kind"):
                read_rust_result(result)
