import hashlib
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
    PresmokeTaskResult,
    PresmokeTaskSpec,
    _cohort_metrics,
    exact_mcnemar_p,
    load_completed_task_results,
    load_presmoke_task_ids,
    load_terminal_task_results,
    reserve_interrupted_retries,
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


def _write_metric_manifest(
    root: Path,
    *,
    failed_variant: str | None = None,
    incomplete_variant: str | None = None,
    claim_producer_variant: str = "A",
    logical_variant_map: dict[str, str] | None = None,
) -> Path:
    manifest = root / "task-manifest.json"
    records = []
    for variant, passed, requests, input_tokens in (
        ("A", True, 10, 1000),
        ("B_empty", False, 9, 900),
        ("B_claim", True, 7, 700),
        ("B_forced_claim", True, 6, 600),
    ):
        attempt_id = f"task-a-{variant}"
        incomplete = 1 if variant == incomplete_variant else 0
        result_path = root / f"{variant}.result.json"
        gate_path = root / f"{variant}.gate.json"
        result_path.write_text(
            json.dumps(
                {
                    "attempt_id": attempt_id,
                    "variant": variant,
                    "verifier_passed": passed,
                    "usage": {
                        "model_requests": requests,
                        "complete_model_responses": requests - incomplete,
                        "incomplete_model_responses": incomplete,
                        "input_tokens": input_tokens,
                        "output_tokens": 100,
                        "cache_read_tokens": 500,
                        "reasoning_tokens": 50,
                    },
                }
            ),
            encoding="utf-8",
        )
        gate_path.write_text(
            json.dumps(
                {
                    "attempt_id": attempt_id,
                    "decision": "fail" if variant == failed_variant else "pass",
                }
            ),
            encoding="utf-8",
        )
        records.append(
            {
                "attempt_id": attempt_id,
                "variant": variant,
                "status": "gate_failed" if variant == failed_variant else "passed",
                "result_path": str(result_path),
                "gate_path": str(gate_path),
                "result_hash": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                "gate_hash": hashlib.sha256(gate_path.read_bytes()).hexdigest(),
                "verifier_passed": passed,
                "claim_observation": (
                    {"bundle_available": True} if variant.endswith("claim") else None
                ),
            }
        )
    manifest.write_text(
        json.dumps(
            {
                "experiment_cohort": (
                    "success_efficiency" if claim_producer_variant == "A" else "failure_recovery"
                ),
                "claim_producer_variant": claim_producer_variant,
                "logical_variant_map": logical_variant_map,
                "execution": {"claim_producer_variant": claim_producer_variant},
                "attempt_results": records,
                "failure": "GATE_FAILED" if failed_variant is not None else None,
            }
        ),
        encoding="utf-8",
    )
    return manifest


class PresmokeRunnerTests(unittest.TestCase):
    def test_cohort_metrics_keep_success_efficiency_separate_and_report_paired_usage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = _write_metric_manifest(root)
            metrics = _cohort_metrics(
                (PresmokeTaskResult("task-a", "passed", str(manifest), None),)
            ).rows["success_efficiency"]

        self.assertEqual(metrics["task_count"], 1)
        self.assertEqual(metrics["variants"]["B_claim"]["verifier_pass_rate"], 1.0)
        self.assertEqual(
            metrics["paired_against_producer"]["B_claim"]["usage_delta_totals"]["model_requests"],
            -3,
        )
        self.assertEqual(
            metrics["paired_against_producer"]["B_forced_claim"]["usage_delta_totals"]["input_tokens"],
            -400,
        )

    def test_cohort_metrics_pair_against_b_empty_when_selected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = _write_metric_manifest(Path(directory), claim_producer_variant="B_empty")
            metrics = _cohort_metrics(
                (PresmokeTaskResult("task-a", "passed", str(manifest), None),)
            ).rows["failure_recovery"]

        self.assertEqual(metrics["claim_producer_variant"], "B_empty")
        self.assertEqual(
            metrics["paired_against_producer"]["B_claim"]["verifier_passed_delta"],
            1,
        )
        self.assertEqual(
            metrics["paired_against_producer"]["B_claim"]["usage_delta_totals"]["model_requests"],
            -2,
        )

    def test_adaptive_metrics_relabel_the_global_winner_as_logical_a(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = _write_metric_manifest(
                Path(directory),
                claim_producer_variant="B_empty",
                logical_variant_map={
                    "A": "B_empty",
                    "B_empty": "A",
                    "B_claim": "B_claim",
                    "B_forced_claim": "B_forced_claim",
                },
            )
            metrics = _cohort_metrics(
                (PresmokeTaskResult("task-a", "passed", str(manifest), None),)
            ).rows["failure_recovery"]

        self.assertEqual(metrics["claim_producer_variant"], "A")
        self.assertEqual(metrics["variants"]["A"]["verifier_pass_rate"], 0.0)
        self.assertEqual(metrics["variants"]["B_empty"]["verifier_pass_rate"], 1.0)
        self.assertEqual(
            metrics["paired_against_producer"]["B_claim"]["usage_delta_totals"]["model_requests"],
            -2,
        )

    def test_cohort_metrics_exclude_entire_task_when_any_arm_fails_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = _write_metric_manifest(Path(directory), failed_variant="B_claim")
            metrics = _cohort_metrics(
                (PresmokeTaskResult("task-a", "failed", str(manifest), "CLAIM_DELIVERY_REPEATED"),)
            )

        self.assertEqual(metrics.rows, {})
        self.assertEqual(metrics.excluded_tasks, {"task-a": "task_status=failed"})
        self.assertEqual(
            metrics.coverage_dict(("task-a", "task-b")),
            {
                "planned_task_count": 2,
                "included_task_count": 0,
                "excluded_task_count": 1,
                "excluded_tasks": {"task-a": "task_status=failed"},
            },
        )

    def test_cohort_metrics_pair_claim_arms_against_no_claim_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = _write_metric_manifest(Path(directory))
            metrics = _cohort_metrics(
                (PresmokeTaskResult("task-a", "passed", str(manifest), None),)
            )

        row = metrics.rows["success_efficiency"]
        self.assertEqual(row["task_ids"], ["task-a"])
        self.assertEqual(row["no_claim_baseline_variant"], "B_empty")
        pair = row["paired_against_no_claim_baseline"]["B_claim"]
        self.assertEqual((pair["pairs"], pair["wins"], pair["losses"]), (1, 1, 0))
        self.assertEqual(pair["verifier_passed_delta"], 1)
        self.assertEqual(pair["exact_mcnemar_p"], 1.0)
        self.assertEqual(pair["usage_delta_totals"]["model_requests"], -2)
        self.assertEqual(row["variants"]["B_claim"]["empty_claim_bundle_attempts"], 0)
        self.assertEqual(metrics.excluded_tasks, {})

    def test_exact_mcnemar_p_matches_two_sided_binomial_on_discordant_pairs(self) -> None:
        self.assertIsNone(exact_mcnemar_p(0, 0))
        self.assertEqual(exact_mcnemar_p(3, 3), 1.0)
        # 报告口径：B_claim 对 B_empty 赢 4 输 17，exact p = 0.0072。
        self.assertAlmostEqual(exact_mcnemar_p(4, 17), 0.0072, places=4)
        self.assertAlmostEqual(exact_mcnemar_p(11, 5), 0.210, places=3)

    def test_cohort_metrics_mark_recovered_incomplete_usage_as_token_lower_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = _write_metric_manifest(Path(directory), incomplete_variant="B_claim")
            metrics = _cohort_metrics(
                (PresmokeTaskResult("task-a", "passed", str(manifest), None),)
            ).rows["success_efficiency"]

        claim = metrics["variants"]["B_claim"]
        pair = metrics["paired_against_producer"]["B_claim"]
        self.assertEqual(claim["incomplete_usage_attempts"], 1)
        self.assertEqual(claim["incomplete_usage_attempt_rate"], 1.0)
        self.assertTrue(claim["token_values_are_observed_lower_bound"])
        self.assertEqual(pair["pairs_with_incomplete_usage"], 1)
        self.assertTrue(pair["token_delta_includes_observed_lower_bound"])

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

    def test_task_failure_does_not_prevent_peer_tasks_and_writes_aggregate(self) -> None:
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

        self.assertEqual(set(started), set(TASK_IDS))
        self.assertEqual(aggregate["status"], "failed")
        self.assertEqual(aggregate["task_results"][0]["status"], "failed")
        self.assertEqual(len(aggregate["task_results"]), len(TASK_IDS))

    def test_all_tasks_start_with_bounded_parallelism(self) -> None:
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
                        if len(started) == 4:
                            all_started.set()
                    release.wait(timeout=2)

                return FakeTaskRunner(spec.task_id, started, wait_for_peers)

            runner = PresmokeHostRunner(
                specs, root / "aggregate.json", task_workers=4, task_runner_factory=factory
            )
            thread = threading.Thread(target=lambda: runner.run(execute=True))
            thread.start()
            self.assertTrue(all_started.wait(timeout=2))
            release.set()
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(started), len(TASK_IDS))
        self.assertEqual(set(started), set(TASK_IDS))

    def test_no_eligible_claim_does_not_stop_peer_tasks(self) -> None:
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

            results = PresmokeHostRunner(
                specs, root / "aggregate.json", task_runner_factory=factory
            ).run(execute=True)
            aggregate = json.loads((root / "aggregate.json").read_text())

        self.assertEqual(set(started), set(TASK_IDS))
        self.assertEqual(results[0].status, "no_eligible_claim")
        self.assertEqual(aggregate["status"], "completed_with_no_eligible_claim")

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

    def test_resume_skips_only_task_with_four_valid_gated_arms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            write_completed_task_artifacts(specs[0])
            completion = root / "task-completions.json"
            completed = load_completed_task_results(specs, completion)
            started: list[str] = []

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(spec.task_id, started)

            results = PresmokeHostRunner(
                specs[1:],
                root / "aggregate.json",
                frozen_task_ids=TASK_IDS,
                task_runner_factory=factory,
                completed_task_results=completed,
                completion_manifest_path=completion,
            ).run(execute=True)
            checkpoint = json.loads(completion.read_text(encoding="utf-8"))

        self.assertEqual(tuple(result.task_id for result in completed), (TASK_IDS[0],))
        self.assertEqual(started, list(TASK_IDS[1:]))
        self.assertEqual(tuple(result.task_id for result in results), TASK_IDS)
        self.assertEqual(checkpoint["status"], "completed")
        self.assertEqual(
            [item["task_id"] for item in checkpoint["completed_tasks"]], list(TASK_IDS)
        )

    def test_resume_rejects_completed_task_from_different_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec = build_specs(root)[0]
            write_completed_task_artifacts(spec)
            changed = replace(
                spec,
                experiment=replace(
                    spec.experiment,
                    provenance=replace(spec.experiment.provenance, model="different-model"),
                ),
            )

            completed = load_completed_task_results((changed,), root / "task-completions.json")

        self.assertEqual(completed, ())

    def test_failure_is_checkpointed_as_a_terminal_state_and_not_reclassified_as_pending(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            completion = root / "task-completions.json"

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(spec.task_id, [], fail=spec.task_id == TASK_IDS[0])

            with self.assertRaises(PresmokeExecutionError):
                PresmokeHostRunner(
                    specs,
                    root / "aggregate.json",
                    task_runner_factory=factory,
                    completion_manifest_path=completion,
                ).run(execute=True)
            terminal = load_terminal_task_results(specs, completion)
            checkpoint = json.loads(completion.read_text(encoding="utf-8"))

        self.assertEqual(checkpoint["schema_version"], 2)
        self.assertEqual(checkpoint["task_results"][0]["status"], "failed")
        self.assertEqual(terminal[0].status, "failed")

    def test_failure_manifest_is_terminal_before_parent_checkpoint_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec = build_specs(root)[0]
            spec.manifest_path.parent.mkdir(parents=True)
            spec.manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "attempt_results": [],
                        "failure": "GATE: RESPONSE_MODEL_MISMATCH",
                    }
                ),
                encoding="utf-8",
            )
            terminal = load_terminal_task_results((spec,), root / "task-completions.json")

        self.assertEqual(
            terminal,
            (
                PresmokeTaskResult(
                    TASK_IDS[0],
                    "failed",
                    str(spec.manifest_path),
                    "GATE: RESPONSE_MODEL_MISMATCH",
                ),
            ),
        )

    def test_no_eligible_claim_checkpoint_is_a_reusable_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            write_no_eligible_claim_task_artifacts(specs[0])
            completion = root / "task-completions.json"
            completion.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "task_results": [
                            PresmokeTaskResult(
                                specs[0].task_id,
                                "no_eligible_claim",
                                str(specs[0].manifest_path),
                                None,
                            ).to_dict()
                        ],
                    }
                ),
                encoding="utf-8",
            )
            terminal = load_terminal_task_results(specs, completion)
            started: list[str] = []

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(spec.task_id, started)

            results = PresmokeHostRunner(
                specs[1:],
                root / "aggregate.json",
                frozen_task_ids=TASK_IDS,
                task_runner_factory=factory,
                completed_task_results=terminal,
                completion_manifest_path=completion,
            ).run(execute=True)

        self.assertEqual(terminal[0].status, "no_eligible_claim")
        self.assertEqual(started, list(TASK_IDS[1:]))
        self.assertEqual(results[0].status, "no_eligible_claim")

    def test_a_only_checkpoint_is_a_reusable_passed_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = build_specs(root)
            write_a_only_task_artifacts(specs[0])
            completion = root / "task-completions.json"
            completion.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "task_results": [
                            PresmokeTaskResult(
                                specs[0].task_id,
                                "passed",
                                str(specs[0].manifest_path),
                                None,
                            ).to_dict()
                        ],
                    }
                ),
                encoding="utf-8",
            )
            terminal = load_terminal_task_results(specs, completion)
            started: list[str] = []

            def factory(spec: PresmokeTaskSpec) -> FakeTaskRunner:
                return FakeTaskRunner(spec.task_id, started)

            results = PresmokeHostRunner(
                specs[1:],
                root / "aggregate.json",
                frozen_task_ids=TASK_IDS,
                task_runner_factory=factory,
                completed_task_results=terminal,
                completion_manifest_path=completion,
            ).run(execute=True)

        self.assertEqual(terminal[0].status, "passed")
        self.assertEqual(started, list(TASK_IDS[1:]))
        self.assertEqual(results[0].status, "passed")

    def test_interrupted_retry_can_be_reserved_only_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            completion = (Path(directory) / "task-completions.json").resolve()
            reserve_interrupted_retries(completion, (TASK_IDS[0],))
            with self.assertRaisesRegex(ValueError, "唯一续跑机会"):
                reserve_interrupted_retries(completion, (TASK_IDS[0],))


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
        acn_main_revision="9b818d70ddfad2f7d5e1972577dd294b19481c92",
        acn_version="0.2.5",
        run_class="diagnostic",
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
        pier_egress_proxy_image_reference_sha256="8" * 64,
        agent_image_content_digest=None,
        verifier_image_content_digest=None,
        pier_egress_proxy_image_content_digest="sha256:" + "9" * 64,
        model="fixture-model",
        reasoning_effort="max",
        file_edit_authority_enabled=True,
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
        timeouts={
            "agent_seconds": 2,
            "deadline_reserve_seconds": 1,
            "verifier_seconds": 2,
        },
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


def write_completed_task_artifacts(spec: PresmokeTaskSpec) -> None:
    records = []
    for attempt in spec.experiment.attempts:
        output = Path(attempt.output_path)
        output.mkdir(parents=True)
        result_path = output / "attempt-result.json"
        gate_path = output / "gate.json"
        result_path.write_text(
            json.dumps({"attempt_id": attempt.attempt_id, "variant": attempt.variant}),
            encoding="utf-8",
        )
        gate_path.write_text(
            json.dumps({"attempt_id": attempt.attempt_id, "decision": "pass"}),
            encoding="utf-8",
        )
        records.append(
            {
                "attempt_id": attempt.attempt_id,
                "variant": attempt.variant,
                "status": "passed",
                "result_path": str(result_path),
                "gate_path": str(gate_path),
                "result_hash": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                "gate_hash": hashlib.sha256(gate_path.read_bytes()).hexdigest(),
            }
        )
    spec.manifest_path.parent.mkdir(parents=True)
    spec.manifest_path.write_text(
        json.dumps(
            {
                "failure": None,
                "experiment": spec.experiment.to_dict(),
                "attempt_results": records,
            }
        ),
        encoding="utf-8",
    )


def write_a_only_task_artifacts(spec: PresmokeTaskSpec) -> None:
    records = []
    for attempt in spec.experiment.attempts:
        if attempt.variant != "A":
            records.append(
                {
                    "attempt_id": attempt.attempt_id,
                    "variant": attempt.variant,
                    "status": "not_run",
                    "reason": "A_ONLY",
                    "result_path": None,
                    "gate_path": None,
                }
            )
            continue
        output = Path(attempt.output_path)
        output.mkdir(parents=True)
        result_path = output / "attempt-result.json"
        gate_path = output / "gate.json"
        result_path.write_text(
            json.dumps({"attempt_id": attempt.attempt_id, "variant": attempt.variant}),
            encoding="utf-8",
        )
        gate_path.write_text(
            json.dumps({"attempt_id": attempt.attempt_id, "decision": "pass"}),
            encoding="utf-8",
        )
        records.append(
            {
                "attempt_id": attempt.attempt_id,
                "variant": attempt.variant,
                "status": "passed",
                "result_path": str(result_path),
                "gate_path": str(gate_path),
                "result_hash": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                "gate_hash": hashlib.sha256(gate_path.read_bytes()).hexdigest(),
            }
        )
    spec.manifest_path.parent.mkdir(parents=True)
    spec.manifest_path.write_text(
        json.dumps(
            {
                "failure": None,
                "experiment": spec.experiment.to_dict(),
                "attempt_results": records,
            }
        ),
        encoding="utf-8",
    )


def write_no_eligible_claim_task_artifacts(spec: PresmokeTaskSpec) -> None:
    records = []
    for attempt in spec.experiment.attempts:
        if attempt.variant in {"B_claim", "B_forced_claim"}:
            records.append(
                {
                    "attempt_id": attempt.attempt_id,
                    "variant": attempt.variant,
                    "status": "not_run",
                    "reason": "NO_ELIGIBLE_CLAIM",
                    "result_path": None,
                    "gate_path": None,
                }
            )
            continue
        output = Path(attempt.output_path)
        output.mkdir(parents=True)
        result_path = output / "attempt-result.json"
        gate_path = output / "gate.json"
        result_path.write_text(
            json.dumps({"attempt_id": attempt.attempt_id, "variant": attempt.variant}),
            encoding="utf-8",
        )
        gate_path.write_text(
            json.dumps({"attempt_id": attempt.attempt_id, "decision": "pass"}),
            encoding="utf-8",
        )
        records.append(
            {
                "attempt_id": attempt.attempt_id,
                "variant": attempt.variant,
                "status": "passed",
                "result_path": str(result_path),
                "gate_path": str(gate_path),
                "result_hash": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                "gate_hash": hashlib.sha256(gate_path.read_bytes()).hexdigest(),
            }
        )
    spec.manifest_path.parent.mkdir(parents=True)
    spec.manifest_path.write_text(
        json.dumps(
            {
                "failure": None,
                "experiment": spec.experiment.to_dict(),
                "attempt_results": records,
            }
        ),
        encoding="utf-8",
    )
