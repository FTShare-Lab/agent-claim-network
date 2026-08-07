import json
import tempfile
import unittest
from pathlib import Path

from acn_deepswe.dataset import DatasetFreezeError, FrozenDatasetManifest, freeze_dataset
from acn_deepswe.plan import build_attempt_plan
from acn_deepswe.provenance import TASK_DIRECTORY_HASH_ALGORITHM

TASK = """
[agent]
network_mode = "no-network"
[verifier]
network_mode = "no-network"
"""


class DatasetAndPlanTests(unittest.TestCase):
    def test_freeze_is_stable_and_plan_has_unique_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "tasks"
            for task_id in ("task-e", "task-a", "task-d", "task-c", "task-b", "task-f"):
                task = root / task_id
                task.mkdir(parents=True)
                (task / "task.toml").write_text(TASK)
            manifest_path = Path(directory) / "freeze.json"
            manifest = freeze_dataset(root, manifest_path, seed=17)
            again = freeze_dataset(root, Path(directory) / "freeze-again.json", seed=17)
            plan = build_attempt_plan(manifest, Path(directory) / "output", seed=99)
            stored_algorithm = json.loads(manifest_path.read_text())["algorithm"]

        self.assertEqual(manifest.task_ids, again.task_ids)
        self.assertEqual(len(manifest.task_ids), 5)
        self.assertEqual(stored_algorithm, "random.sample_without_replacement_v1")
        self.assertEqual(len(plan.attempts), 15)
        for offset in range(0, 15, 3):
            variants = [attempt.variant for attempt in plan.attempts[offset : offset + 3]]
            self.assertEqual(variants[0], "A")
            self.assertEqual(set(variants[1:]), {"B_empty", "B_claim"})
        output_paths = [attempt.output_path for attempt in plan.attempts]
        self.assertEqual(len(output_paths), len(set(output_paths)))
        self.assertEqual(
            {attempt.variant for attempt in plan.attempts if attempt.variant != "A"},
            {"B_empty", "B_claim"},
        )

    def test_frozen_manifest_rejects_unknown_schema_or_algorithm(self) -> None:
        manifest = {
            "schema_version": 1,
            "algorithm": "random.sample_without_replacement_v1",
            "seed": 17,
            "candidates_hash": "a" * 64,
            "task_ids": ["task-a"],
        }
        for field, value in (("schema_version", 2), ("algorithm", "unknown")):
            with self.subTest(field=field), self.assertRaises(DatasetFreezeError):
                FrozenDatasetManifest.from_dict({**manifest, field: value})

    def test_checked_in_manifests_parse_and_build_attempt_plans(self) -> None:
        manifests = (
            "presmoke-v1.json",
            "luna-smoke-v1.json",
            "luna-followup-v1.json",
        )
        for name in manifests:
            with self.subTest(manifest=name), tempfile.TemporaryDirectory() as directory:
                raw = json.loads((Path(__file__).parents[1] / "manifests" / name).read_text())
                manifest = FrozenDatasetManifest.from_dict(raw)
                plan = build_attempt_plan(manifest, Path(directory).resolve(), seed=7)

                self.assertEqual(
                    raw["task_directory_hash_algorithm"], TASK_DIRECTORY_HASH_ALGORITHM
                )
                self.assertEqual(len(plan.attempts), len(manifest.task_ids) * 3)
