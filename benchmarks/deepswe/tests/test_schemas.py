import unittest

from acn_deepswe.schemas import AttemptManifest, EventLedger, SchemaError


class SchemaTests(unittest.TestCase):
    def test_event_ledger_rejects_missing_required_field(self) -> None:
        with self.assertRaisesRegex(SchemaError, "缺少字段: event_type"):
            EventLedger.from_dict(
                {
                    "schema_version": 1,
                    "attempt_id": "attempt-1",
                    "seq": 1,
                    "timestamp_utc": "2026-07-26T00:00:00Z",
                    "payload": {},
                }
            )

    def test_attempt_manifest_rejects_relative_output_path(self) -> None:
        with self.assertRaisesRegex(SchemaError, "output_path.*绝对路径"):
            AttemptManifest(
                schema_version=1,
                attempt_id="attempt-1",
                task_id="task-1",
                variant="A",
                output_path="output",
            )
