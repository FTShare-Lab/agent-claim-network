import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from acn_deepswe.dataset import (
    DatasetFreezeError,
    FrozenDatasetManifest,
    freeze_dataset,
    freeze_execution_dataset,
)
from acn_deepswe.plan import build_attempt_plan
from acn_deepswe.provenance import TASK_DIRECTORY_HASH_ALGORITHM, sha256_directory_tree

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
        self.assertEqual(len(plan.attempts), 20)
        for offset in range(0, 20, 4):
            variants = [attempt.variant for attempt in plan.attempts[offset : offset + 4]]
            self.assertEqual(variants[0], "A")
            self.assertEqual(set(variants[1:]), {"B_empty", "B_claim", "B_forced_claim"})
        output_paths = [attempt.output_path for attempt in plan.attempts]
        self.assertEqual(len(output_paths), len(set(output_paths)))
        self.assertEqual(
            {attempt.variant for attempt in plan.attempts if attempt.variant != "A"},
            {"B_empty", "B_claim", "B_forced_claim"},
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

    def test_freeze_rejects_non_positive_sample_size(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "tasks"
            task = root / "task-a"
            task.mkdir(parents=True)
            (task / "task.toml").write_text(TASK)

            for sample_size in (0, -1):
                with self.subTest(sample_size=sample_size), self.assertRaisesRegex(
                    DatasetFreezeError, "正整数"
                ):
                    freeze_dataset(root, Path(directory) / "freeze.json", seed=17, sample_size=sample_size)

    def test_execution_freeze_records_revisions_hashes_and_normalized_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deepswe = root / "deep-swe"
            tasks = deepswe / "tasks"
            for index in range(6):
                task = tasks / f"task-{index}"
                task.mkdir(parents=True)
                (task / "task.toml").write_text(TASK)
                (task / "instruction.md").write_text(f"task-{index}")
                (task / "environment").mkdir()
                (task / "tests").mkdir()
            pier = root / "pier"
            pier.mkdir()
            (pier / "README.md").write_text("fixture")
            _commit_checkout(deepswe)
            _commit_checkout(pier)

            manifest_path = root / "frozen.json"
            normalized = root / "normalized"
            manifest = freeze_execution_dataset(
                tasks,
                manifest_path,
                normalized,
                deepswe,
                pier,
                seed=17,
                sample_size=6,
            )
            stored = json.loads(manifest_path.read_text())

            self.assertEqual(len(manifest.task_ids), 6)
            self.assertEqual(stored["task_directory_hash_algorithm"], TASK_DIRECTORY_HASH_ALGORITHM)
            self.assertEqual(stored["deepswe_revision"], _revision(deepswe))
            self.assertEqual(stored["pier_revision"], _revision(pier))
            for task_id in manifest.task_ids:
                source = tasks / task_id
                normalized_task = normalized / task_id
                self.assertTrue(normalized_task.is_dir())
                self.assertIn("allow_internet = false", (normalized_task / "task.toml").read_text())
                self.assertEqual(
                    stored["task_directory_hashes"][task_id]["source"],
                    sha256_directory_tree(source),
                )
                self.assertEqual(
                    stored["task_directory_hashes"][task_id]["normalized"],
                    sha256_directory_tree(normalized_task),
                )

            with self.assertRaisesRegex(DatasetFreezeError, "拒绝覆盖"):
                freeze_execution_dataset(
                    tasks,
                    root / "another.json",
                    normalized,
                    deepswe,
                    pier,
                    seed=17,
                    sample_size=6,
                )

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
                self.assertEqual(len(plan.attempts), len(manifest.task_ids) * 4)


def _commit_checkout(checkout: Path) -> None:
    subprocess.run(["git", "init", str(checkout)], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(checkout), "add", "."], check=True, capture_output=True, text=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(checkout),
            "-c",
            "user.name=Benchmark",
            "-c",
            "user.email=benchmark@example.test",
            "commit",
            "-m",
            "fixture",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _revision(checkout: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
