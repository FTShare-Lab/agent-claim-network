import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from acn_deepswe.claim_freeze import ClaimFreezeError, append_freeze_barrier, freeze_claim_bundle


def event(
    seq: int, attempt_id: str, event_type: str, payload: dict[str, object]
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "attempt_id": attempt_id,
        "seq": seq,
        "timestamp_utc": "2026-07-26T00:00:00Z",
        "event_type": event_type,
        "payload": payload,
    }


class ClaimFreezeTests(unittest.TestCase):
    def test_later_inactive_snapshot_revokes_prior_active_claim(self) -> None:
        for status in ("stale", "disputed", "deprecated"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger = root / "events.jsonl"
                records = [
                    event(1, "attempt-a", "claim_snapshot", {"claim": claim("claim-1", "观察", "active")}),
                    event(2, "attempt-a", "claim_snapshot", {"claim": claim("claim-1", "已失效", status)}),
                    event(3, "attempt-a", "claim_freeze_barrier", {"barrier_id": "barrier-a"}),
                ]
                ledger.write_text("".join(json.dumps(item) + "\n" for item in records))
                bundle = freeze_claim_bundle(ledger, "attempt-a", root / "claims.json")
                self.assertEqual(bundle.claims, ())

    def test_freeze_uses_only_prior_same_attempt_active_claim_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "host-events.jsonl"
            records = [
                event(
                    1,
                    "attempt-a",
                    "claim_snapshot",
                    {
                        "claim": claim("claim-active", "可借用", "active"),
                    },
                ),
                event(
                    2,
                    "attempt-a",
                    "claim_snapshot",
                    {
                        "claim": claim("claim-stale", "过期", "stale"),
                    },
                ),
                event(
                    3,
                    "attempt-a",
                    "claim_snapshot",
                    {
                        "claim": claim("claim-deprecated", "废弃", "deprecated"),
                    },
                ),
                event(
                    4,
                    "attempt-b",
                    "claim_snapshot",
                    {
                        "claim": claim("claim-foreign", "跨 attempt", "active"),
                    },
                ),
                event(5, "attempt-a", "claim_freeze_barrier", {"barrier_id": "barrier-a"}),
                event(
                    6,
                    "attempt-a",
                    "claim_snapshot",
                    {
                        "claim": claim("claim-after", "太晚", "active"),
                    },
                ),
            ]
            ledger.write_text("".join(json.dumps(item) + "\n" for item in records))
            first = freeze_claim_bundle(ledger, "attempt-a", root / "claims.json")
            second = freeze_claim_bundle(ledger, "attempt-a", root / "claims-again.json")
            stored = json.loads((root / "claims.json").read_text())
            metadata = json.loads((root / "claims.json.manifest.json").read_text())
            bundle_file_hash = hashlib.sha256((root / "claims.json").read_bytes()).hexdigest()

        self.assertEqual([claim.claim_id for claim in first.claims], ["claim-active"])
        self.assertEqual(first.bundle_hash, second.bundle_hash)
        self.assertEqual(first.bundle_hash, bundle_file_hash)
        self.assertEqual(metadata["barrier_seq"], 5)
        self.assertEqual(stored["schema_version"], 1)
        self.assertNotIn("claim-after", json.dumps(stored))
        self.assertNotIn("claim-foreign", json.dumps(stored))

    def test_freeze_fails_closed_without_barrier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "events.jsonl"
            ledger.write_text(json.dumps(event(1, "attempt-a", "claim_snapshot", {})) + "\n")
            with self.assertRaisesRegex(ClaimFreezeError, "freeze barrier"):
                freeze_claim_bundle(ledger, "attempt-a", Path(directory) / "claims.json")

    def test_freeze_binds_optional_producer_verification_only_to_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "events.jsonl"
            ledger.write_text(
                json.dumps(event(1, "attempt-a", "claim_freeze_barrier", {"barrier_id": "a"}))
                + "\n"
            )
            producer = {
                "attempt_id": "attempt-a",
                "verifier_passed": True,
                "attempt_result_sha256": "a" * 64,
            }

            frozen = freeze_claim_bundle(
                ledger,
                "attempt-a",
                root / "claims.json",
                producer_verification=producer,
            )
            stored = json.loads((root / "claims.json").read_text())
            metadata = json.loads((root / "claims.json.manifest.json").read_text())

        self.assertEqual(frozen.producer_verification, producer)
        self.assertNotIn("producer_verification", stored)
        self.assertEqual(metadata["producer_verification"], producer)

    def test_verified_producer_gate_quarantines_failed_producer_claims(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "events.jsonl"
            ledger.write_text(
                "".join(
                    json.dumps(item) + "\n"
                    for item in (
                        event(
                            1,
                            "attempt-a",
                            "claim_snapshot",
                            {"claim": claim("claim-unverified", "局部线索", "active")},
                        ),
                        event(
                            2,
                            "attempt-a",
                            "claim_freeze_barrier",
                            {"barrier_id": "a"},
                        ),
                    )
                )
            )
            producer = {
                "attempt_id": "attempt-a",
                "verifier_passed": False,
                "attempt_result_sha256": "b" * 64,
            }

            frozen = freeze_claim_bundle(
                ledger,
                "attempt-a",
                root / "claims.json",
                producer_verification=producer,
                quality_gate="verified_producer_only",
            )
            stored = json.loads((root / "claims.json").read_text())
            metadata = json.loads((root / "claims.json.manifest.json").read_text())

        self.assertEqual(frozen.claims, ())
        self.assertEqual(
            [item.claim_id for item in frozen.quarantined_claims],
            ["claim-unverified"],
        )
        self.assertEqual(stored["claims"], [])
        self.assertEqual(metadata["quality_gate"], "verified_producer_only")
        self.assertEqual(
            [item["claim_id"] for item in metadata["quarantined_claims"]],
            ["claim-unverified"],
        )

    def test_verified_producer_gate_keeps_verified_claims(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "events.jsonl"
            ledger.write_text(
                "".join(
                    json.dumps(item) + "\n"
                    for item in (
                        event(
                            1,
                            "attempt-a",
                            "claim_snapshot",
                            {"claim": claim("claim-verified", "已验证线索", "active")},
                        ),
                        event(
                            2,
                            "attempt-a",
                            "claim_freeze_barrier",
                            {"barrier_id": "a"},
                        ),
                    )
                )
            )

            frozen = freeze_claim_bundle(
                ledger,
                "attempt-a",
                root / "claims.json",
                producer_verification={
                    "attempt_id": "attempt-a",
                    "verifier_passed": True,
                    "attempt_result_sha256": "c" * 64,
                },
                quality_gate="verified_producer_only",
            )

        self.assertEqual([item.claim_id for item in frozen.claims], ["claim-verified"])
        self.assertEqual(frozen.quarantined_claims, ())

    def test_verified_producer_gate_requires_verification_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "events.jsonl"
            ledger.write_text(
                json.dumps(
                    event(1, "attempt-a", "claim_freeze_barrier", {"barrier_id": "a"})
                )
                + "\n"
            )
            with self.assertRaisesRegex(ClaimFreezeError, "producer_verification"):
                freeze_claim_bundle(
                    ledger,
                    "attempt-a",
                    root / "claims.json",
                    quality_gate="verified_producer_only",
                )

    def test_freeze_refuses_to_overwrite_an_existing_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "events.jsonl"
            ledger.write_text(
                json.dumps(
                    event(
                        1,
                        "attempt-a",
                        "claim_freeze_barrier",
                        {"barrier_id": "barrier-a"},
                    )
                )
                + "\n"
            )
            output = root / "claims.json"
            output.write_text("existing")

            with self.assertRaisesRegex(ClaimFreezeError, "拒绝覆盖"):
                freeze_claim_bundle(ledger, "attempt-a", output)
            self.assertEqual(output.read_text(), "existing")

    def test_host_can_append_one_barrier_only_after_attempt_finished(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "events.jsonl"
            ledger.write_text(
                "".join(
                    json.dumps(item) + "\n"
                    for item in (
                        event(
                            1,
                            "attempt-a",
                            "claim_snapshot",
                            {"claim": claim("claim-before", "前", "active")},
                        ),
                        event(2, "attempt-a", "attempt_finished", {}),
                    )
                )
            )
            barrier = append_freeze_barrier(ledger, "attempt-a", "barrier-a")
            with ledger.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        event(
                            4,
                            "attempt-a",
                            "claim_snapshot",
                            {"claim": claim("claim-after", "后", "active")},
                        )
                    )
                    + "\n"
                )
            frozen = freeze_claim_bundle(ledger, "attempt-a", Path(directory) / "claims.json")
            with self.assertRaisesRegex(ClaimFreezeError, "已有 freeze barrier"):
                append_freeze_barrier(ledger, "attempt-a", "barrier-b")

        self.assertEqual(barrier.seq, 3)
        self.assertEqual([item.claim_id for item in frozen.claims], ["claim-before"])


def claim(claim_id: str, statement: str, status: str) -> dict[str, object]:
    return {
        "id": claim_id,
        "name": claim_id,
        "statement": statement,
        "scope": "repo/a",
        "holder": "benchmark",
        "confidence": "high",
        "status": status,
        "created_at": "2026-07-26T00:00:00Z",
        "source_claim_ids": [],
        "evidence_summary": "fixture",
    }
