import json
import tempfile
import threading
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from acn_deepswe.dataset import FrozenDatasetManifest
from acn_deepswe.host_runner import TaskExecutionError, TaskExecutionResult
from acn_deepswe.plan import build_attempt_plan
from acn_deepswe.presmoke import (
    PresmokeExecutionError,
    PresmokeHostRunner,
    PresmokeTaskSpec,
    load_presmoke_task_ids,
)
from acn_deepswe.provenance import EvaluationProvenance
from acn_deepswe.runner import build_experiment_manifest

TASK_IDS = (
    "bandit-structured-nosec-directives",
    "ipython-session-bundle-replay",
    "koota-entity-snapshot-rollback",
    "pwntools-tube-multiplexing",
    "sql-formatter-bigquery-pipe-formatting",
)


class PresmokeRunnerTests(unittest.TestCase):
    def test_manifest_task_count_is_not_hardcoded_to_five(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tasks.json"
            task_ids = [f"task-{index}" for index in range(10)]
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "algorithm": "random.sample_without_replacement_v1",
                        "seed": 17,
                        "candidates_hash": "a" * 64,
                        "task_ids": task_ids,
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(load_presmoke_task_ids(manifest), tuple(task_ids))

    def test_task1_failure_does_not_start_later_tasks_and_writes_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            started: list[str] = []

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(spec.task_id, started, fail=spec.task_id == TASK_IDS[0])

            runner = PresmokeHostRunner(specs, root / "aggregate.json", task_runner_factory=factory)
            with self.assertRaises(PresmokeExecutionError):
                runner.run(execute=True)
            aggregate = json.loads((root / "aggregate.json").read_text())

        self.assertEqual(started, [TASK_IDS[0]])
        self.assertEqual(aggregate["status"], "failed")
        self.assertEqual(aggregate["task_results"][0]["status"], "failed")
        self.assertEqual(len(aggregate["task_results"]), 1)

    def test_task1_success_starts_remaining_four_with_bounded_parallelism(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            started: list[str] = []
            all_started = threading.Event()
            release = threading.Event()
            lock = threading.Lock()

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                def wait_for_peers() -> None:
                    with lock:
                        if len(started) == 5:
                            all_started.set()
                    release.wait(timeout=2)

                return FakeTaskRunner(
                    spec.task_id, started, wait_for_peers if spec.task_id != TASK_IDS[0] else None
                )

            runner = PresmokeHostRunner(
                specs, root / "aggregate.json", task_workers=4, task_runner_factory=factory
            )
            thread = threading.Thread(target=lambda: runner.run(execute=True))
            thread.start()
            self.assertTrue(all_started.wait(timeout=2))
            release.set()
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(started[0], TASK_IDS[0])
        self.assertEqual(set(started[1:]), set(TASK_IDS[1:]))

    def test_first_task_no_eligible_claim_stops_later_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            started: list[str] = []

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(
                    spec.task_id,
                    started,
                    outcome="no_eligible_claim" if spec.task_id == TASK_IDS[0] else "passed",
                )

            runner = PresmokeHostRunner(specs, root / "aggregate.json", task_runner_factory=factory)
            with self.assertRaises(PresmokeExecutionError):
                runner.run(execute=True)

        self.assertEqual(started, [TASK_IDS[0]])

    def test_later_no_eligible_claim_is_reported_without_failing_the_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(
                    spec.task_id,
                    [],
                    outcome=("no_eligible_claim" if spec.task_id == TASK_IDS[1] else "passed"),
                )

            results = PresmokeHostRunner(
                specs, root / "aggregate.json", task_runner_factory=factory
            ).run(execute=True)
            aggregate = json.loads((root / "aggregate.json").read_text())

        self.assertEqual(results[1].status, "no_eligible_claim")
        self.assertEqual(aggregate["status"], "completed_with_no_eligible_claim")

    def test_task_runner_is_called_once_per_task_and_later_failure_propagates_after_peers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            calls: list[tuple[str, bool]] = []

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(
                    spec.task_id,
                    [],
                    fail=spec.task_id == TASK_IDS[2],
                    calls=calls,
                )

            runner = PresmokeHostRunner(
                specs, root / "aggregate.json", task_workers=4, task_runner_factory=factory
            )
            with self.assertRaises(PresmokeExecutionError):
                runner.run(execute=True)
            aggregate = json.loads((root / "aggregate.json").read_text())

        self.assertEqual(sorted(task_id for task_id, _ in calls), sorted(TASK_IDS))
        self.assertTrue(all(execute for _, execute in calls))
        self.assertEqual(len(aggregate["task_results"]), 5)
        self.assertEqual(aggregate["status"], "failed")

    def test_rejects_invalid_worker_count_and_manifest_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            specs = build_specs(Path(directory))
            with self.assertRaisesRegex(ValueError, "task_workers"):
                PresmokeHostRunner(specs, Path(directory) / "aggregate.json", task_workers=0)
            with self.assertRaisesRegex(ValueError, "顺序"):
                PresmokeHostRunner(tuple(reversed(specs)), Path(directory) / "aggregate.json")


class FakeTaskRunner:
    def __init__(
        self,
        task_id: str,
        started: list[str],
        wait: Callable[[], None] | None = None,
        *,
        fail: bool = False,
        calls: list[tuple[str, bool]] | None = None,
        outcome: str = "passed",
    ) -> None:
        self.task_id = task_id
        self.started = started
        self.wait = wait
        self.fail = fail
        self.calls = calls
        self.outcome = outcome

    def run_task1(self, *, execute: bool) -> TaskExecutionResult:
        self.started.append(self.task_id)
        if self.calls is not None:
            self.calls.append((self.task_id, execute))
        if self.wait is not None:
            self.wait()
        if self.fail:
            raise TaskExecutionError(f"failed: {self.task_id}")
        return TaskExecutionResult(self.outcome)


def build_specs(root: Path) -> tuple[PresmokeTaskSpec, ...]:
    dataset = FrozenDatasetManifest(
        1, "random.sample_without_replacement_v1", 7, "a" * 64, TASK_IDS
    )
    plan = build_attempt_plan(dataset, root / "run", seed=2)
    provenance = EvaluationProvenance(
        deepswe_revision="deepswe@abc",
        pier_revision="pier@abc",
        acn_revision="acn@abc",
        acn_binary_hash="1" * 64,
        acn_config_hash="2" * 64,
        dataset_candidates_hash="a" * 64,
        dataset_seed=7,
        dataset_task_ids=TASK_IDS,
        skill_hash="3" * 64,
        acn_package_tree_hash="8" * 64,
        pier_package_tree_hash="9" * 64,
        source_task_tree_hash="4" * 64,
        normalized_task_tree_hash="5" * 64,
        agent_image_reference_sha256="6" * 64,
        verifier_image_reference_sha256="7" * 64,
        agent_image_content_digest=None,
        verifier_image_content_digest=None,
        model="fixture-model",
        reasoning_effort="max",
        resources={
            "cpus": 2,
            "memory_mb": 4096,
            "storage_mb": 1024,
            "max_tokens": 10,
            "context_window": 20,
            "max_requests": 2,
            "max_input_tokens": 20,
            "max_output_tokens": 20,
        },
        timeouts={"agent_seconds": 2, "deadline_reserve_seconds": 1},
        llm_retry={"retry_count": 1, "retry_base_delay_ms": 1, "retry_max_delay_ms": 2},
        network_translation_warning="translated",
    )
    experiment = build_experiment_manifest("presmoke", plan, "b" * 64, provenance)
    return tuple(
        PresmokeTaskSpec(
            task_id=task_id,
            experiment=replace(
                experiment,
                experiment_id=f"presmoke-{index}",
                attempts=tuple(
                    attempt for attempt in experiment.attempts if attempt.task_id == task_id
                ),
            ),
            execution=None,
            jobs_directory=root / "jobs" / task_id,
            manifest_path=root / "manifests" / f"{task_id}.json",
        )
        for index, task_id in enumerate(TASK_IDS)
    )
