import json
import tempfile
import unittest
from pathlib import Path

from acn_deepswe.cli import main

TASK = """
[agent]
network_mode = "no-network"
[verifier]
network_mode = "no-network"
"""


class CliTests(unittest.TestCase):
    def test_validate_freeze_and_plan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            task = root / "single-task"
            task.mkdir()
            (task / "task.toml").write_text(TASK)
            self.assertEqual(main(["validate-config", str(task), str(root / "checked")]), 0)
            tasks = root / "tasks"
            for index in range(5):
                candidate = tasks / f"task-{index}"
                candidate.mkdir(parents=True)
                (candidate / "task.toml").write_text(TASK)
            manifest = root / "freeze.json"
            self.assertEqual(main(["freeze-dataset", str(tasks), str(manifest), "--seed", "8"]), 0)
            self.assertEqual(main(["plan", str(manifest), str(root / "plan"), "--seed", "5"]), 0)
            self.assertTrue((root / "plan" / "attempt-plan.json").is_file())
            ledger = root / "host-events.jsonl"
            ledger.write_text(
                "\n".join(
                    json.dumps(item)
                    for item in (
                        {
                            "schema_version": 1,
                            "attempt_id": "task-0-a-1",
                            "seq": 1,
                            "event_type": "claim_snapshot",
                            "timestamp_utc": "2026-07-26T00:00:00Z",
                            "payload": {
                                "claim": {
                                    "id": "claim-1",
                                    "name": "claim-1",
                                    "statement": "s",
                                    "scope": "x",
                                    "holder": "benchmark",
                                    "confidence": "high",
                                    "status": "active",
                                    "created_at": "2026-07-26T00:00:00Z",
                                    "source_claim_ids": [],
                                    "evidence_summary": "fixture",
                                }
                            },
                        },
                        {
                            "schema_version": 1,
                            "attempt_id": "task-0-a-1",
                            "seq": 2,
                            "event_type": "attempt_finished",
                            "timestamp_utc": "2026-07-26T00:00:01Z",
                            "payload": {},
                        },
                    )
                )
                + "\n"
            )
            self.assertEqual(main(["append-freeze-barrier", str(ledger), "task-0-a-1", "b-1"]), 0)
            claims = root / "claims.json"
            self.assertEqual(main(["freeze-claims", str(ledger), "task-0-a-1", str(claims)]), 0)
