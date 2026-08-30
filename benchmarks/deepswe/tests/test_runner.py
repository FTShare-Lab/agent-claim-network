import hashlib
import json
import os
import tempfile
import threading
import tomllib
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from acn_deepswe.assets import frozen_coding_benchmark_skill
from acn_deepswe.dataset import FrozenDatasetManifest
from acn_deepswe.host_runner import (
    AttemptProgressMonitor,
    CONTAINER_MODEL_KEY_ENV,
    EVALUATION_AUTO_COMPACT_CTX_RATIO,
    EVALUATION_CODE_RUN_MAX_OUTPUT_CHARS,
    EVALUATION_FILE_DIFF_MAX_CHANGED_LINES,
    EVALUATION_FILE_READ_MAX_CHARS,
    EVALUATION_MAX_PARALLEL_TOOL_CALLS,
    HOST_MODEL_KEY_ENV,
    HostArtifacts,
    Task1ExecutionConfig,
    Task1HostRunner,
    TaskExecutionError,
    _pier_task_matches_attempt,
    attempt_deadline_secs,
    build_acn_config,
    build_attempt_toml,
    build_pier_job_config,
    write_attempt_files,
)
from acn_deepswe.plan import AttemptPlan, build_attempt_plan
from acn_deepswe.provenance import EvaluationProvenance
from acn_deepswe.runner import (
    ExperimentManifest,
    build_experiment_manifest,
)
from acn_deepswe.schemas import AttemptManifest
from acn_deepswe.sentinel import SentinelLeakError, scan_for_sentinel_leaks

UPSTREAM = "https://upstream.invalid"
DATASET = FrozenDatasetManifest(1, "random.sample_without_replacement_v1", 7, "a" * 64, ("task-1",))
TASK_TOML = """[task]
name = "datacurve/task-1"

[metadata]
task_id = "task-1"

[environment]
allow_internet = false

[verifier.environment]
allow_internet = false
"""

_cleanup_trial_images_patcher: object | None = None
_cleanup_trial_images_mock: object | None = None


def setUpModule() -> None:
    global _cleanup_trial_images_patcher, _cleanup_trial_images_mock
    _cleanup_trial_images_patcher = patch(
        "acn_deepswe.host_runner.cleanup_finished_trial_images", return_value=0
    )
    _cleanup_trial_images_mock = _cleanup_trial_images_patcher.start()


def tearDownModule() -> None:
    assert _cleanup_trial_images_patcher is not None
    _cleanup_trial_images_patcher.stop()


class ConfigGenerationTests(unittest.TestCase):
    def test_claim_variants_attempt_toml_declare_a_claim_bundle(self) -> None:
        plan = build_attempt_plan(DATASET, Path("/tmp/acn-eval-plan"), seed=2)
        for attempt in plan.attempts:
            with self.subTest(variant=attempt.variant):
                rendered = tomllib.loads(build_attempt_toml(attempt, "fix the bug", 5100))
                self.assertEqual(rendered["workspace_root"], "/app")
                self.assertTrue(rendered["task_prompt"].startswith("请执行 /coding-benchmark"))
                self.assertNotIn("读取并遵循", rendered["task_prompt"])
                self.assertEqual(rendered["attempt_deadline_secs"], 5100)
                self.assertEqual(rendered["model_egress_mode"], "pier")
                self.assertEqual(rendered["harness_mode"], "standard")
                self.assertEqual(
                    rendered.get("claim_bundle"),
                    (
                        "/opt/acn-eval/claims.json"
                        if attempt.variant in {"B_claim", "B_forced_claim"}
                        else None
                    ),
                )

    def test_minimal_attempt_uses_single_shell_workflow_without_loading_skill(self) -> None:
        attempt = build_attempt_plan(DATASET, Path("/tmp/acn-eval-plan"), seed=2).attempts[0]

        rendered = tomllib.loads(
            build_attempt_toml(attempt, "fix the bug", 5100, harness_mode="minimal")
        )

        self.assertEqual(rendered["harness_mode"], "minimal")
        self.assertTrue(rendered["task_prompt"].startswith("Please solve this issue:"))
        self.assertIn("Use `code_run` for shell commands and file edits", rendered["task_prompt"])
        self.assertIn("complete project test suite", rendered["task_prompt"])
        self.assertIn("malformed-input reproductions with a short timeout", rendered["task_prompt"])
        self.assertIn("/sys/fs/cgroup/pids.max", rendered["task_prompt"])
        self.assertIn("retry with fewer workers", rendered["task_prompt"])
        self.assertNotIn("/coding-benchmark", rendered["task_prompt"])
        self.assertNotIn("不要扫整个仓库", rendered["task_prompt"])

    def test_attempt_deadline_leaves_room_before_the_pier_wall_clock(self) -> None:
        self.assertEqual(attempt_deadline_secs(provenance()), 240)
        too_small = replace(
            provenance(), timeouts={"agent_seconds": 60, "deadline_reserve_seconds": 60}
        )
        with self.assertRaisesRegex(ValueError, "deadline_reserve_seconds"):
            attempt_deadline_secs(too_small)

    def test_acn_config_reads_key_from_env_and_points_at_the_real_upstream(self) -> None:
        rendered = build_acn_config(provenance(), UPSTREAM)
        parsed = tomllib.loads(rendered)

        llm = parsed["agent"]["llm"]
        self.assertEqual(llm["endpoint"], "https://upstream.invalid/v1")
        self.assertEqual(llm["api_key_env"], CONTAINER_MODEL_KEY_ENV)
        self.assertEqual(llm["provider"], "openai_responses")
        self.assertEqual(llm["reasoning_effort"], "max")
        self.assertEqual(llm["temperature"], 1.0)
        self.assertEqual(llm["top_p"], 0.95)
        self.assertEqual(llm["max_tokens"], provenance().resources["max_tokens"])
        rerank = parsed["router"]["rerank"]
        self.assertEqual(rerank["provider"], "openai_responses")
        self.assertEqual(rerank["endpoint"], "https://upstream.invalid/v1")
        self.assertEqual(rerank["model"], provenance().model)
        self.assertEqual(rerank["api_key_env"], CONTAINER_MODEL_KEY_ENV)
        self.assertEqual(
            parsed["agent"]["tool"]["code_run_max_output_chars"],
            EVALUATION_CODE_RUN_MAX_OUTPUT_CHARS,
        )
        self.assertEqual(
            parsed["agent"]["tool"]["file_read_max_chars"],
            EVALUATION_FILE_READ_MAX_CHARS,
        )
        self.assertEqual(
            parsed["agent"]["tool"]["file_diff_max_changed_lines"],
            EVALUATION_FILE_DIFF_MAX_CHANGED_LINES,
        )
        self.assertEqual(
            parsed["agent"]["tool"]["max_parallel_tool_calls"],
            EVALUATION_MAX_PARALLEL_TOOL_CALLS,
        )
        self.assertTrue(parsed["agent"]["tool"]["file_edit_authority_enabled"])
        self.assertFalse(parsed["agent"]["memory"]["enabled"])
        self.assertFalse(parsed["agent"]["session"]["memory_review"]["enabled"])
        self.assertEqual(
            parsed["agent"]["session"]["compaction"]["auto_compact_ctx_ratio"],
            EVALUATION_AUTO_COMPACT_CTX_RATIO,
        )
        # 配置文件本身绝不能出现 key 值。
        self.assertNotIn("secret", rendered.lower())

    def test_generated_acn_config_matches_the_rust_contract_fixture(self) -> None:
        """同一份文件同时被 Rust 集成测试加载，两侧契约不能漂移。"""
        generated = build_acn_config(provenance(), UPSTREAM)
        fixture = (Path(__file__).parent / "fixtures" / "generated-acn.toml").read_text()
        self.assertEqual(generated, fixture)

    def test_acn_config_rejects_missing_or_invalid_resource_budget(self) -> None:
        broken = replace(provenance(), resources={**provenance().resources, "max_tokens": 0})
        with self.assertRaisesRegex(ValueError, "max_tokens"):
            build_acn_config(broken, UPSTREAM)

    def test_pier_job_uses_default_environment_and_passes_upstream_to_agent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            files = write_attempt_files(
                plan.attempts[0], root / "configs", "fix test", provenance(), UPSTREAM
            )
            artifacts = _artifacts(root)
            job = build_pier_job_config(plan.attempts[0], files, provenance(), artifacts, UPSTREAM)

        # 不再自定义 environment import_path：用 Pier 自己的 Docker environment。
        self.assertNotIn("import_path", job["environment"])
        self.assertNotIn("kwargs", job["environment"])
        self.assertEqual(job["n_attempts"], 1)
        self.assertEqual(job["n_concurrent_trials"], 1)
        self.assertEqual(job["retry"], {"max_retries": 0})
        # 保留预拉取的官方任务镜像；Pier 的 delete=True 会执行 down --rmi all。
        self.assertFalse(job["environment"]["delete"])
        agent = job["agents"][0]
        self.assertIn("AcnEvalPierAgent", agent["import_path"])
        self.assertEqual(agent["kwargs"]["upstream_base_url"], UPSTREAM)
        self.assertEqual(agent["kwargs"]["host_model_key_env"], HOST_MODEL_KEY_ENV)
        self.assertEqual(agent["kwargs"]["container_model_key_env"], CONTAINER_MODEL_KEY_ENV)
        # Pier 墙钟与 attempt_deadline_secs 必须同源于 timeouts.agent_seconds
        self.assertEqual(agent["override_timeout_sec"], provenance().timeouts["agent_seconds"])
        self.assertLess(attempt_deadline_secs(provenance()), agent["override_timeout_sec"])
        self.assertEqual(
            set(job),
            {
                "job_name",
                "jobs_dir",
                "n_attempts",
                "n_concurrent_trials",
                "retry",
                "environment",
                "verifier",
                "agents",
                "datasets",
                "tasks",
                "artifacts",
                "metrics",
            },
        )

    def test_pier_job_rejects_unparseable_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            files = write_attempt_files(
                plan.attempts[0], root / "configs", "fix test", provenance(), UPSTREAM
            )
            with self.assertRaises(ValueError):
                build_pier_job_config(
                    plan.attempts[0], files, provenance(), _artifacts(root), "not-a-url"
                )

    def test_a_and_b_empty_do_not_require_claim_bundle_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            for attempt in plan.attempts[:2]:
                files = write_attempt_files(
                    attempt, root / attempt.variant, "fix test", provenance(), UPSTREAM
                )
                build_pier_job_config(attempt, files, provenance(), artifacts, UPSTREAM)

            claim_attempt = next(
                attempt for attempt in plan.attempts if attempt.variant == "B_claim"
            )
            files = write_attempt_files(
                claim_attempt, root / "B_claim", "fix test", provenance(), UPSTREAM
            )
            with self.assertRaisesRegex(ValueError, "claim bundle"):
                build_pier_job_config(claim_attempt, files, provenance(), artifacts, UPSTREAM)


class DryRunTests(unittest.TestCase):
    def test_dry_run_reports_four_arms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan = build_attempt_plan(DATASET, Path(directory) / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            steps = Task1HostRunner(experiment, Path(directory) / "jobs").run_task1()
        self.assertEqual(
            tuple(step.phase for step in steps),
            (
                "A",
                "B_empty",
                "freeze_A",
                "freeze_B_empty",
                plan.attempts[2].variant,
                plan.attempts[3].variant,
            ),
        )
        self.assertEqual(
            tuple(attempt.variant for attempt in plan.attempts[:2]),
            ("A", "B_empty"),
        )

    def test_runner_preserves_crossbalanced_b_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan = build_attempt_plan(DATASET, Path(directory) / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            crossbalanced = replace(
                experiment,
                attempts=(
                    experiment.attempts[0],
                    experiment.attempts[3],
                    experiment.attempts[1],
                    experiment.attempts[2],
                ),
            )
            steps = Task1HostRunner(crossbalanced, Path(directory) / "jobs").run_task1()
        self.assertEqual(
            tuple(step.phase for step in steps),
            (
                "A",
                "B_empty",
                "freeze_A",
                "freeze_B_empty",
                "B_forced_claim",
                "B_claim",
            ),
        )


class IsolationTests(unittest.TestCase):
    def test_pier_task_identity_requires_both_task_name_and_stable_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            task_toml = Path(directory) / "task.toml"
            task_toml.write_text(TASK_TOML, encoding="utf-8")

            self.assertTrue(_pier_task_matches_attempt(task_toml, "task-1", "datacurve/task-1"))
            self.assertFalse(_pier_task_matches_attempt(task_toml, "task-1", "task-1"))
            self.assertFalse(
                _pier_task_matches_attempt(task_toml, "another-task", "datacurve/task-1")
            )


class RealExecutionTests(unittest.TestCase):
    def test_three_arms_run_in_order_and_freeze_produces_the_claim_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)
            variants: list[str] = []
            pier_envs: list[dict[str, str]] = []

            def run(command: list[str], **kwargs: object) -> FakeCompleted:
                pier_envs.append(kwargs["env"])
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]), attempt, bundle_path=artifacts.claim_bundle
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())
            bundle = json.loads(artifacts.claim_bundle.read_text())
            bundle_hash = hashlib.sha256(artifacts.claim_bundle.read_bytes()).hexdigest()

        self.assertEqual(set(variants[:2]), {"A", "B_empty"})
        self.assertEqual(set(variants[2:]), {"B_claim", "B_forced_claim"})
        self.assertEqual([item["status"] for item in manifest["attempt_results"]], ["passed"] * 4)
        self.assertEqual(manifest["experiment_cohort"], "success_efficiency")
        self.assertEqual(bundle["claims"][0]["id"], "claim-1")
        self.assertEqual(manifest["frozen_claim_bundle_hash"], bundle_hash)
        self.assertEqual(manifest["execution"]["host_model_key_env"], HOST_MODEL_KEY_ENV)
        self.assertEqual(manifest["execution"]["container_model_key_env"], CONTAINER_MODEL_KEY_ENV)
        # Pier 子进程必须继承 key 才能注入容器，这是与官方 adapter 一致的口径。
        self.assertTrue(all(env[HOST_MODEL_KEY_ENV] == "upstream-secret" for env in pier_envs))
        self.assertTrue(
            all(
                env["PYTHONPATH"]
                == os.pathsep.join(
                    (str(execution.frozen_acn_source_root), str(execution.frozen_pier_source_root))
                )
                for env in pier_envs
            )
        )
        self.assertTrue(all(env["PYTHONDONTWRITEBYTECODE"] == "1" for env in pier_envs))
        self.assertTrue(
            all(
                item["progress_path"].endswith("/progress.json")
                for item in manifest["attempt_results"]
            )
        )

    def test_b_empty_can_be_the_bound_claim_producer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            a_bundle = root / "claims.json"
            b_empty_bundle = root / "claims-b-empty.json"
            artifacts = replace(
                _artifacts(root, claim_bundle=b_empty_bundle),
                a_claim_bundle=a_bundle,
                b_empty_claim_bundle=b_empty_bundle,
            )
            execution = replace(_execution(root, artifacts), claim_producer_variant="B_empty")

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())
            b_empty_metadata = json.loads(
                b_empty_bundle.with_name("claims-b-empty.json.manifest.json").read_text()
            )
            a_bundle_exists = a_bundle.is_file()
            b_empty_bundle_exists = b_empty_bundle.is_file()
            b_empty_bundle_hash = hashlib.sha256(b_empty_bundle.read_bytes()).hexdigest()

        self.assertTrue(a_bundle_exists)
        self.assertTrue(b_empty_bundle_exists)
        self.assertEqual(manifest["claim_producer_variant"], "B_empty")
        self.assertEqual(manifest["execution"]["claim_producer_variant"], "B_empty")
        self.assertEqual(
            b_empty_metadata["attempt_id"],
            next(item.attempt_id for item in plan.attempts if item.variant == "B_empty"),
        )
        self.assertEqual(
            manifest["frozen_claim_bundle_hash"],
            b_empty_bundle_hash,
        )
        self.assertEqual(set(manifest["frozen_claim_bundles"]), {"A", "B_empty"})

    def test_both_attempt_waves_execute_concurrently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = replace(_execution(root, artifacts), task_workers=2)
            wave_barrier = threading.Barrier(2)

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                wave_barrier.wait(timeout=2)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                result = Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )

        self.assertEqual(result.status, "passed")

    def test_operator_interruption_preserves_progress_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)

            def run(*_args: object, **_kwargs: object) -> FakeCompleted:
                raise KeyboardInterrupt

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "INTERRUPTED_BY_OPERATOR"),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text(encoding="utf-8"))
            progress = json.loads(
                Path(manifest["attempt_results"][0]["progress_path"]).read_text(encoding="utf-8")
            )

        self.assertEqual(manifest["attempt_results"][0]["reason"], "INTERRUPTED_BY_OPERATOR")
        self.assertEqual(progress["status"], "interrupted")
        self.assertEqual(progress["terminal_reason"], "INTERRUPTED_BY_OPERATOR")


class ProgressMonitorTests(unittest.TestCase):
    def test_active_session_events_are_observed_without_stopping_the_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = AttemptManifest(
                1, "task-1-a", "task-1", "A", str((root / "attempt").resolve())
            )
            ledger = (
                Path(attempt.output_path)
                / "host-config"
                / "pier-jobs"
                / attempt.attempt_id
                / "trial-1"
                / "agent"
                / "runtime"
                / "data"
                / "agents"
                / "eval"
                / "sessions"
                / "session-1"
                / "turn_events.jsonl"
            )
            ledger.parent.mkdir(parents=True)
            ledger.write_text(
                json.dumps(
                    {
                        "seq": 3,
                        "turn_id": "turn-1",
                        "created_at": "2026-08-12T14:07:21Z",
                        "kind": "tool_call_completed",
                        "name": "file_read",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            monitor = AttemptProgressMonitor(
                attempt,
                Path(attempt.output_path),
                poll_secs=1,
                stall_after_secs=60,
            )
            monitor.start()
            active = json.loads(monitor.progress_path.read_text(encoding="utf-8"))
            monitor.finish("pier_completed", pier_return_code=0)
            completed = json.loads(monitor.progress_path.read_text(encoding="utf-8"))

        self.assertEqual(active["status"], "active")
        self.assertEqual(active["event_count"], 1)
        self.assertEqual(active["last_event"]["kind"], "tool_call_completed")
        self.assertFalse(active["possibly_stalled"])
        self.assertEqual(completed["status"], "pier_completed")
        self.assertEqual(completed["pier_return_code"], 0)

    def test_staged_package_drift_stops_before_the_next_attempt_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            execution = _execution(root, _artifacts(root, claim_bundle=root / "claims.json"))
            runner = Task1HostRunner(experiment, root / "jobs", execution)
            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                runner._validate_execution(execution)
                (execution.frozen_acn_source_root / "acn_deepswe" / "__init__.py").write_text(
                    "changed", encoding="utf-8"
                )
                with self.assertRaisesRegex(TaskExecutionError, "frozen ACN source tree"):
                    runner._run_one_attempt(plan.attempts[0], execution)
        self.assertFalse(Path(plan.attempts[0].output_path).exists())


class ProvenanceTests(unittest.TestCase):
    def test_package_tree_hashes_are_required_and_round_trip(self) -> None:
        value = provenance()
        payload = value.to_dict()
        self.assertEqual(EvaluationProvenance.from_dict(payload), value)
        payload.pop("acn_package_tree_hash")
        with self.assertRaisesRegex(ValueError, "acn_package_tree_hash"):
            EvaluationProvenance.from_dict(payload)

    def test_missing_host_key_stops_before_creating_any_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            execution = _execution(root, _artifacts(root, claim_bundle=root / "claims.json"))

            def run(*_args: object, **_kwargs: object) -> FakeCompleted:
                raise AssertionError("缺少 key 时不应调用 pier")

            with (
                patch.dict("os.environ", {}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, HOST_MODEL_KEY_ENV),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )

    def test_existing_claim_bundle_stops_before_creating_any_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            artifacts.claim_bundle.write_text("stale")
            execution = _execution(root, artifacts)

            def run(*_args: object, **_kwargs: object) -> FakeCompleted:
                raise AssertionError("存在旧 claim bundle 时不应调用 pier")

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "拒绝复用旧产物"),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )

    def test_agent_failure_still_freezes_and_runs_every_arm(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    exit_type="failed",
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        # agent 自身失败按未通过计分，不中断实验。
        self.assertEqual(
            [item["status"] for item in manifest["attempt_results"]], ["agent_failed"] * 4
        )

    def test_producer_wave_gate_failure_stops_before_claim_consumers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    verifier_ran=False,
                )
                return FakeCompleted(0)

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaises(TaskExecutionError),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(len(manifest["attempt_results"]), 2)
        self.assertEqual(
            {item["variant"] for item in manifest["attempt_results"]}, {"A", "B_empty"}
        )
        self.assertTrue(
            all(item["status"] == "gate_failed" for item in manifest["attempt_results"])
        )
        self.assertTrue(
            all("VERIFIER_DID_NOT_RUN" in item["reason"] for item in manifest["attempt_results"])
        )

    def test_direct_model_egress_is_explicit_but_cannot_pass_the_formal_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = replace(_execution(root, artifacts), model_egress_mode="direct")

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                _write_fake_trial(
                    Path(job["jobs_dir"]), attempt, bundle_path=artifacts.claim_bundle
                )
                return FakeCompleted(0)

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "ISOLATION_CHECK_FAILED"),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(manifest["execution"]["model_egress_mode"], "direct")
        self.assertIn("ISOLATION_CHECK_FAILED", manifest["attempt_results"][0]["reason"])

    def test_a_without_claims_runs_b_empty_and_marks_b_claim_not_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    exit_type="failed",
                    emit_claim=False,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(set(variants), {"A", "B_empty"})
        self.assertIsNone(manifest["failure"])
        self.assertEqual(
            [item["variant"] for item in manifest["attempt_results"]],
            ["A", "B_empty", "B_claim", "B_forced_claim"],
        )
        self.assertEqual(
            [item["status"] for item in manifest["attempt_results"][-2:]], ["not_run", "not_run"]
        )
        self.assertEqual(
            [item["reason"] for item in manifest["attempt_results"][-2:]],
            ["NO_ELIGIBLE_CLAIM", "NO_ELIGIBLE_CLAIM"],
        )
        self.assertEqual(manifest["experiment_cohort"], "unpaired_no_claim")

    def test_a_without_claims_can_execute_all_four_arms_with_an_empty_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = replace(_execution(root, artifacts), run_all_variants_without_claims=True)
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    emit_claim=False,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(set(variants[:2]), {"A", "B_empty"})
        self.assertEqual(set(variants[2:]), {"B_claim", "B_forced_claim"})
        self.assertEqual([item["status"] for item in manifest["attempt_results"]], ["passed"] * 4)
        claim_records = [
            item
            for item in manifest["attempt_results"]
            if item["variant"] in {"B_claim", "B_forced_claim"}
        ]
        self.assertTrue(
            all(item["claim_observation"]["bundle_available"] is False for item in claim_records)
        )
        self.assertTrue(all("EMPTY_CLAIM_BUNDLE" in item["reason"] for item in claim_records))
        self.assertTrue(manifest["execution"]["run_all_variants_without_claims"])

    def test_run_a_only_freezes_claims_and_skips_b_arms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = replace(_execution(root, artifacts), run_a_only=True)
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())
            self.assertTrue(artifacts.claim_bundle.is_file())

        self.assertEqual(variants, ["A"])
        self.assertEqual(
            [item["variant"] for item in manifest["attempt_results"]],
            ["A", "B_empty", "B_claim", "B_forced_claim"],
        )
        self.assertEqual(manifest["attempt_results"][0]["status"], "passed")
        self.assertEqual(
            [item["status"] for item in manifest["attempt_results"][1:]],
            ["not_run", "not_run", "not_run"],
        )
        self.assertEqual(
            [item["reason"] for item in manifest["attempt_results"][1:]],
            ["A_ONLY", "A_ONLY", "A_ONLY"],
        )
        self.assertTrue(manifest["execution"]["run_a_only"])
        self.assertIsNone(manifest["failure"])

    def test_adaptive_producer_phase_freezes_both_candidates_and_skips_claim_arms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("producer-pair", plan, "b" * 64, provenance())
            a_bundle = root / "claims.json"
            b_empty_bundle = root / "claims-b-empty.json"
            artifacts = replace(
                _artifacts(root, claim_bundle=a_bundle),
                a_claim_bundle=a_bundle,
                b_empty_claim_bundle=b_empty_bundle,
            )
            execution = replace(
                _execution(root, artifacts),
                claim_producer_variant="adaptive",
                run_producer_pair_only=True,
            )
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())
            a_bundle_exists = a_bundle.is_file()
            b_empty_bundle_exists = b_empty_bundle.is_file()

        self.assertEqual(set(variants), {"A", "B_empty"})
        self.assertTrue(a_bundle_exists)
        self.assertTrue(b_empty_bundle_exists)
        self.assertEqual(manifest["execution"]["phase_mode"], "adaptive_producers")
        self.assertEqual(manifest["claim_producer_variant"], "adaptive")
        self.assertEqual(set(manifest["frozen_claim_bundles"]), {"A", "B_empty"})
        self.assertEqual(
            [item["status"] for item in manifest["attempt_results"][-2:]],
            ["not_run", "not_run"],
        )
        self.assertEqual(
            [item["reason"] for item in manifest["attempt_results"][-2:]],
            ["PRODUCER_PAIR_ONLY", "PRODUCER_PAIR_ONLY"],
        )

    def test_adaptive_consumer_uses_b_empty_winner_claims_without_rerunning_producers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_output = root / "source" / "output"
            source_plan = build_attempt_plan(DATASET, source_output, seed=2)
            source_experiment = build_experiment_manifest(
                "producer-pair", source_plan, "b" * 64, provenance()
            )
            source_fixture = root / "source" / "fixture"
            source_fixture.mkdir(parents=True)
            a_bundle = source_output / "tasks" / "task-1" / "claims.json"
            b_empty_bundle = source_output / "tasks" / "task-1" / "claims-b-empty.json"
            source_artifacts = replace(
                _artifacts(source_fixture, claim_bundle=a_bundle),
                a_claim_bundle=a_bundle,
                b_empty_claim_bundle=b_empty_bundle,
            )
            source_execution = replace(
                _execution(source_fixture, source_artifacts),
                manifest_path=source_output / "tasks" / "task-1" / "manifest.json",
                claim_producer_variant="adaptive",
                run_producer_pair_only=True,
            )

            def run_source(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(
                    item for item in source_plan.attempts if item.attempt_id == job["job_name"]
                )
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=a_bundle,
                    claim_variants=frozenset({"B_empty"}),
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(
                    source_experiment,
                    root / "source" / "jobs",
                    source_execution,
                    run=run_source,
                ).run_task1(execute=True)
            aggregate = source_output / "presmoke-aggregate.json"
            aggregate.write_text(json.dumps({"schema_version": 1, "status": "passed"}))
            selection = root / "producer-selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "selected",
                        "candidate_aliases": {"S1": "A", "S2": "B_empty"},
                        "score_rule": [
                            "verifier_passed",
                            "f2p_micro",
                            "S1_on_exact_tie",
                        ],
                        "source_output_dir": str(source_output),
                        "producer_aggregate_path": str(aggregate),
                        "producer_aggregate_sha256": hashlib.sha256(
                            aggregate.read_bytes()
                        ).hexdigest(),
                        "task_order": ["task-1"],
                        "task_sources": {
                            "task-1": {
                                "source_output_dir": str(source_output),
                                "task_manifest_path": str(source_execution.manifest_path),
                                "task_manifest_sha256": hashlib.sha256(
                                    source_execution.manifest_path.read_bytes()
                                ).hexdigest(),
                            }
                        },
                        "winner_alias": "S2",
                        "loser_alias": "S1",
                        "winner_variant": "B_empty",
                        "loser_variant": "A",
                        "logical_labels": {"A": "B_empty", "B_empty": "A"},
                    }
                )
            )

            consumer_output = root / "consumer" / "output"
            consumer_plan = build_attempt_plan(DATASET, consumer_output, seed=2)
            consumer_experiment = build_experiment_manifest(
                "claim-consumers", consumer_plan, "b" * 64, provenance()
            )
            consumer_fixture = root / "consumer" / "fixture"
            consumer_fixture.mkdir(parents=True)
            consumer_artifacts = replace(
                _artifacts(consumer_fixture, claim_bundle=b_empty_bundle),
                a_claim_bundle=a_bundle,
                b_empty_claim_bundle=b_empty_bundle,
            )
            consumer_execution = replace(
                _execution(consumer_fixture, consumer_artifacts),
                manifest_path=consumer_output / "tasks" / "task-1" / "manifest.json",
                claim_producer_variant="B_empty",
                adaptive_source_manifest=source_execution.manifest_path,
                producer_selection_manifest=selection,
            )
            variants: list[str] = []

            def run_consumer(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(
                    item for item in consumer_plan.attempts if item.attempt_id == job["job_name"]
                )
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=b_empty_bundle,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(
                    consumer_experiment,
                    root / "consumer" / "jobs",
                    consumer_execution,
                    run=run_consumer,
                ).run_task1(execute=True)
            manifest = json.loads(consumer_execution.manifest_path.read_text())

        self.assertEqual(set(variants), {"B_claim", "B_forced_claim"})
        self.assertEqual(manifest["execution"]["phase_mode"], "adaptive_consumers")
        self.assertEqual(manifest["claim_producer_variant"], "B_empty")
        self.assertEqual(
            manifest["logical_variant_map"],
            {"A": "B_empty", "B_empty": "A", "B_claim": "B_claim", "B_forced_claim": "B_forced_claim"},
        )
        self.assertTrue(
            all(
                item["claim_observation"]["bundle_available"]
                for item in manifest["attempt_results"][-2:]
            )
        )

    def test_b_only_followup_reuses_a_evidence_and_runs_only_three_b_arms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_plan, source_execution = _run_a_only_source(root / "source")
            b_plan, b_execution, b_experiment = _b_only_followup(
                root / "followup", source_execution, provenance()
            )
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(
                    item for item in b_plan.attempts if item.attempt_id == job["job_name"]
                )
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=b_execution.artifacts.claim_bundle,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(
                    b_experiment, root / "followup" / "jobs", b_execution, run=run
                ).run_task1(execute=True)
            manifest = json.loads(b_execution.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(variants, [attempt.variant for attempt in b_plan.attempts[1:]])
        self.assertEqual(
            manifest["attempt_results"][0]["result_path"],
            str(Path(source_plan.attempts[0].output_path) / "attempt-result.json"),
        )
        self.assertEqual([item["status"] for item in manifest["attempt_results"]], ["passed"] * 4)
        self.assertEqual(manifest["execution"]["phase_mode"], "b_only_from_a")
        self.assertEqual(
            manifest["execution"]["a_only_source_manifest"],
            str(source_execution.manifest_path),
        )
        self.assertRegex(manifest["execution"]["a_only_source_manifest_hash"], r"^[0-9a-f]{64}$")

    def test_b_only_followup_rejects_tampered_claim_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, source_execution = _run_a_only_source(root / "source")
            bundle = json.loads(source_execution.artifacts.claim_bundle.read_text())
            bundle["claims"][0]["statement"] = "tampered"
            source_execution.artifacts.claim_bundle.write_text(json.dumps(bundle))
            _, b_execution, b_experiment = _b_only_followup(
                root / "followup", source_execution, provenance()
            )

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "claim bundle 内容已漂移"),
            ):
                Task1HostRunner(
                    b_experiment, root / "followup" / "jobs", b_execution
                ).validate_b_only_source()

    def test_b_only_followup_rejects_missing_producer_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, source_execution = _run_a_only_source(root / "source")
            bundle = source_execution.artifacts.claim_bundle
            metadata_path = bundle.with_name(bundle.name + ".manifest.json")
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            del metadata["producer_verification"]
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            _, b_execution, b_experiment = _b_only_followup(
                root / "followup", source_execution, provenance()
            )

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "缺少完整 producer_verification"),
            ):
                Task1HostRunner(
                    b_experiment, root / "followup" / "jobs", b_execution
                ).validate_b_only_source()

    def test_b_only_followup_rejects_fairness_configuration_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, source_execution = _run_a_only_source(root / "source")
            drifted = replace(provenance(), reasoning_effort="high")
            _, b_execution, b_experiment = _b_only_followup(
                root / "followup", source_execution, drifted
            )

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "provenance 漂移: reasoning_effort"),
            ):
                Task1HostRunner(
                    b_experiment, root / "followup" / "jobs", b_execution
                ).validate_b_only_source()

    def test_b_only_followup_runs_all_three_arms_with_empty_claim_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, source_execution = _run_a_only_source(root / "source", emit_claim=False)
            b_plan, b_execution, b_experiment = _b_only_followup(
                root / "followup", source_execution, provenance()
            )
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(
                    item for item in b_plan.attempts if item.attempt_id == job["job_name"]
                )
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=b_execution.artifacts.claim_bundle,
                    emit_claim=False,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                result = Task1HostRunner(
                    b_experiment, root / "followup" / "jobs", b_execution, run=run
                ).run_task1(execute=True)
            manifest = json.loads(b_execution.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(result.status, "passed")
        self.assertEqual(variants, [attempt.variant for attempt in b_plan.attempts[1:]])
        self.assertEqual(manifest["experiment_cohort"], "unpaired_no_claim")
        self.assertTrue(
            all(
                "EMPTY_CLAIM_BUNDLE" in item["reason"]
                for item in manifest["attempt_results"]
                if item["variant"] in {"B_claim", "B_forced_claim"}
            )
        )

    def test_a_verifier_failure_with_claims_runs_failure_recovery_pair(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    verifier_passed=attempt.variant != "A",
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(set(variants), {"A", "B_empty", "B_claim", "B_forced_claim"})
        self.assertEqual(manifest["experiment_cohort"], "failure_recovery")

    def test_hard_gate_rejects_ineligible_a_before_b_arms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = replace(_execution(root, artifacts), require_eligible_claim=True)
            variants: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                variants.append(attempt.variant)
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    emit_claim=False,
                )
                return FakeCompleted(0)

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "NO_ELIGIBLE_CLAIM"),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(set(variants), {"A", "B_empty"})
        self.assertEqual(manifest["failure"], "NO_ELIGIBLE_CLAIM")

    def test_pier_process_failure_is_recorded_as_infrastructure_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            execution = _execution(root, _artifacts(root, claim_bundle=root / "claims.json"))

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaises(TaskExecutionError),
            ):
                Task1HostRunner(
                    experiment,
                    root / "jobs",
                    execution,
                    run=lambda *_a, **_k: FakeCompleted(1),
                ).run_task1(execute=True)
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(manifest["attempt_results"][0]["reason"], "PIER_INFRASTRUCTURE_FAILURE")

    def test_upstream_concurrency_exhaustion_is_infrastructure_failure_without_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = _execution(root, artifacts)

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
                _write_fake_trial(
                    Path(job["jobs_dir"]), attempt, bundle_path=artifacts.claim_bundle
                )
                result = next(Path(job["jobs_dir"]).glob("*/*/agent/evaluation/result.json"))
                raw = json.loads(result.read_text(encoding="utf-8"))
                raw["exit_type"] = "failed"
                raw["failure_kind"] = "upstream_concurrency_exhausted"
                result.write_text(json.dumps(raw), encoding="utf-8")
                return FakeCompleted(0)

            with (
                patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True),
                self.assertRaisesRegex(TaskExecutionError, "UPSTREAM_CONCURRENCY_EXHAUSTED"),
            ):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text(encoding="utf-8"))

        record = manifest["attempt_results"][0]
        self.assertEqual(record["status"], "infrastructure_failed")
        self.assertEqual(record["reason"], "UPSTREAM_CONCURRENCY_EXHAUSTED")
        self.assertIsNone(record["gate_path"])

    def test_verifier_timeout_replays_frozen_patch_once_without_model_agent(self) -> None:
        assert _cleanup_trial_images_mock is not None
        _cleanup_trial_images_mock.reset_mock()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = build_attempt_plan(DATASET, root / "run", seed=2)
            experiment = build_experiment_manifest("experiment-1", plan, "b" * 64, provenance())
            artifacts = _artifacts(root, claim_bundle=root / "claims.json")
            execution = replace(_execution(root, artifacts), run_a_only=True)
            agent_imports: list[str] = []

            def run(command: list[str], **_kwargs: object) -> FakeCompleted:
                job = json.loads(Path(command[-1]).read_text())
                agent_imports.append(job["agents"][0]["import_path"])
                attempt = plan.attempts[0]
                is_regrade = job["job_name"].endswith("-verifier-regrade-1")
                _write_fake_trial(
                    Path(job["jobs_dir"]),
                    attempt,
                    bundle_path=artifacts.claim_bundle,
                    verifier_ran=is_regrade,
                    job_name=job["job_name"],
                    exception_info=(
                        None
                        if is_regrade
                        else {
                            "exception_type": "VerifierTimeoutError",
                            "exception_message": "Verifier execution timed out",
                        }
                    ),
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                result = Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )

            manifest = json.loads(execution.manifest_path.read_text(encoding="utf-8"))
            attempt_result = json.loads(
                Path(manifest["attempt_results"][0]["result_path"]).read_text(encoding="utf-8")
            )

        self.assertEqual(result.status, "passed")
        self.assertEqual(len(agent_imports), 2)
        cleaned_trials = [item.args[0] for item in _cleanup_trial_images_mock.call_args_list]
        self.assertEqual(len(cleaned_trials), 2)
        self.assertEqual(len(set(cleaned_trials)), 2)
        self.assertTrue(agent_imports[0].endswith(":AcnEvalPierAgent"))
        self.assertTrue(agent_imports[1].endswith(":AcnPatchReplayPierAgent"))
        self.assertEqual(attempt_result["verifier_regrade"]["trigger"], "VERIFIER_TIMEOUT")
        self.assertTrue(attempt_result["verifier_passed"])


class SentinelTests(unittest.TestCase):
    def test_sentinel_scan_blocks_leaked_string(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.txt"
            path.write_text("contains SENTINEL-DO-NOT-LEAK")
            with self.assertRaisesRegex(SentinelLeakError, "artifact.txt"):
                scan_for_sentinel_leaks(Path(directory), ("SENTINEL-DO-NOT-LEAK",))


class FakeCompleted:
    def __init__(self, returncode: int, stdout: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def _artifacts(root: Path, claim_bundle: Path | None = None) -> HostArtifacts:
    task = root / "task"
    if not task.exists():
        task.mkdir()
        (task / "task.toml").write_text(TASK_TOML)
        (task / "environment").mkdir()
        (task / "tests").mkdir()
    binary = root / "acn_eval"
    if not binary.exists():
        binary.write_text("fixture")
    bundle = claim_bundle or root / "claims.json"
    return HostArtifacts(binary, frozen_coding_benchmark_skill().source_path, bundle, task)


def _execution(root: Path, artifacts: HostArtifacts) -> Task1ExecutionConfig:
    pier = root / "pier"
    if not pier.exists():
        pier.write_text("fixture")
    frozen_sources = root / "frozen-python"
    frozen_acn_source_root = frozen_sources / "acn" / "src"
    frozen_pier_source_root = frozen_sources / "pier" / "src"
    for source_root, package_name in (
        (frozen_acn_source_root, "acn_deepswe"),
        (frozen_pier_source_root, "pier"),
    ):
        package = source_root / package_name
        package.mkdir(parents=True, exist_ok=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
    return Task1ExecutionConfig(
        artifacts=artifacts,
        task_prompt="fix fixture",
        upstream_base_url=UPSTREAM,
        manifest_path=root / "execution-manifest.json",
        pier_executable=pier,
        expected_response_model="fixture-model",
        frozen_acn_source_root=frozen_acn_source_root,
        frozen_pier_source_root=frozen_pier_source_root,
    )


def _write_fake_trial(
    jobs_dir: Path,
    attempt: AttemptManifest,
    *,
    bundle_path: Path,
    exit_type: str = "completed",
    verifier_ran: bool = True,
    emit_claim: bool = True,
    claim_variants: frozenset[str] = frozenset({"A"}),
    verifier_passed: bool = True,
    job_name: str | None = None,
    exception_info: dict[str, str] | None = None,
) -> None:
    attempt_id = attempt.attempt_id
    variant = attempt.variant
    trial = jobs_dir / (job_name or attempt_id) / "trial-1"
    evaluation = trial / "agent" / "evaluation"
    evaluation.mkdir(parents=True)
    (trial / "artifacts").mkdir()
    (trial / "artifacts" / "model.patch").write_text("diff --git a/a b/a\n")

    evidence: list[dict[str, object]] = []
    used: list[str] = []
    if variant in {"B_claim", "B_forced_claim"}:
        body = bundle_path.read_bytes()
        claims = json.loads(body)["claims"]
        if claims:
            claim = claims[0]
            content_hash = hashlib.sha256(
                json.dumps(claim, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            ).hexdigest()
            evidence = [
                {
                    "schema_version": 1,
                    "evidence_id": "router-1",
                    "attempt_id": attempt_id,
                    "bundle_hash": hashlib.sha256(body).hexdigest(),
                    "query_hash": "b" * 64,
                    "candidate_claim_ids": ["claim-1"],
                    "selected_claim_ids": ["claim-1"],
                    "injected_claim_ids": ["claim-1"],
                    "injected_content_hashes": [content_hash],
                    "timestamp_utc": "2026-07-26T00:00:00Z",
                }
            ]
            used = ["claim-1"]

    (evaluation / "result.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "attempt_id": attempt_id,
                "exit_type": exit_type,
                "agent_steps": 1,
                "claim_new_ids": ["claim-1"] if variant == "A" and emit_claim else [],
                "claim_updated_ids": [],
                "claim_used_ids": used,
                "router_evidence": evidence,
                "router_evidence_incomplete": False,
                "usage": {
                    "model_requests": 3,
                    "complete_model_responses": 3,
                    "incomplete_model_responses": 0,
                    "audit_incomplete": False,
                    "response_models": ["fixture-model"],
                    "input_tokens": 1200,
                    "output_tokens": 90,
                    "cache_read_tokens": 800,
                    "reasoning_tokens": 60,
                },
                "event_ledger_path": str((evaluation / "events.jsonl").resolve()),
            }
        )
    )

    events: list[dict[str, object]] = []
    if variant in claim_variants and emit_claim:
        events.append(
            {
                "schema_version": 1,
                "attempt_id": attempt_id,
                "seq": 1,
                "event_type": "claim_snapshot",
                "timestamp_utc": "2026-07-26T00:00:00Z",
                "payload": {
                    "claim": {
                        "id": "claim-1",
                        "name": "name",
                        "statement": "statement",
                        "scope": "scope",
                        "holder": "holder",
                        "created_at": "2026-07-26T00:00:00Z",
                        "evidence_summary": "evidence",
                        "confidence": "high",
                        "status": "active",
                        "source_claim_ids": [],
                    }
                },
            }
        )
    events.append(
        {
            "schema_version": 1,
            "attempt_id": attempt_id,
            "seq": len(events) + 1,
            "event_type": "attempt_finished",
            "timestamp_utc": "2026-07-26T00:00:01Z",
            "payload": {},
        }
    )
    (evaluation / "events.jsonl").write_text("\n".join(json.dumps(item) for item in events) + "\n")
    (trial / "result.json").write_text(
        json.dumps(
            {
                "task_name": f"datacurve/{attempt.task_id}",
                "trial_name": (
                    f"{attempt.task_id}__def5678"
                    if job_name is not None and job_name.endswith("-verifier-regrade-1")
                    else f"{attempt.task_id}__abc1234"
                ),
                "trial_uri": trial.resolve().as_uri(),
                "task_checksum": "checksum",
                "config": {},
                "agent_info": {},
                "verifier_result": {"rewards": {"reward": 1 if verifier_passed else 0}}
                if verifier_ran
                else None,
                "exception_info": exception_info,
            }
        )
    )


def _run_a_only_source(
    root: Path, *, emit_claim: bool = True
) -> tuple[AttemptPlan, Task1ExecutionConfig]:
    root.mkdir(parents=True)
    output = root / "output"
    plan = build_attempt_plan(DATASET, output, seed=2)
    experiment = build_experiment_manifest("a-only-source", plan, None, provenance())
    fixture = root / "fixture"
    fixture.mkdir()
    artifacts = _artifacts(
        fixture,
        claim_bundle=output / "tasks" / "task-1" / "claims.json",
    )
    execution = replace(
        _execution(fixture, artifacts),
        manifest_path=output / "tasks" / "task-1" / "manifest.json",
        run_a_only=True,
    )

    def run(command: list[str], **_kwargs: object) -> FakeCompleted:
        job = json.loads(Path(command[-1]).read_text())
        attempt = next(item for item in plan.attempts if item.attempt_id == job["job_name"])
        _write_fake_trial(
            Path(job["jobs_dir"]),
            attempt,
            bundle_path=artifacts.claim_bundle,
            emit_claim=emit_claim,
        )
        return FakeCompleted(0)

    with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
        Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(execute=True)
    return plan, execution


def _b_only_followup(
    root: Path,
    source_execution: Task1ExecutionConfig,
    followup_provenance: EvaluationProvenance,
) -> tuple[AttemptPlan, Task1ExecutionConfig, ExperimentManifest]:
    root.mkdir(parents=True)
    output = root / "output"
    plan = build_attempt_plan(DATASET, output, seed=2)
    experiment = build_experiment_manifest("b-only-followup", plan, None, followup_provenance)
    fixture = root / "fixture"
    fixture.mkdir()
    artifacts = _artifacts(fixture, claim_bundle=source_execution.artifacts.claim_bundle)
    execution = replace(
        _execution(fixture, artifacts),
        manifest_path=output / "tasks" / "task-1" / "manifest.json",
        run_all_variants_without_claims=True,
        a_only_source_manifest=source_execution.manifest_path,
    )
    return plan, execution, experiment


def provenance() -> EvaluationProvenance:
    return EvaluationProvenance(
        deepswe_revision="deepswe@abc",
        pier_revision="datacurve-pier==0.3.0",
        acn_revision="acn@def",
        acn_main_revision="9b818d70ddfad2f7d5e1972577dd294b19481c92",
        acn_version="0.2.5",
        run_class="diagnostic",
        acn_binary_hash=hashlib.sha256(b"fixture").hexdigest(),
        acn_config_hash="2" * 64,
        dataset_candidates_hash="a" * 64,
        dataset_seed=7,
        dataset_task_ids=("task-1",),
        skill_hash=frozen_coding_benchmark_skill().content_hash,
        acn_package_tree_hash=_fixture_package_tree_hash("acn_deepswe"),
        pier_package_tree_hash=_fixture_package_tree_hash("pier"),
        source_task_tree_hash="3" * 64,
        normalized_task_tree_hash=_fixture_task_tree_hash(),
        agent_image_reference_sha256="5" * 64,
        verifier_image_reference_sha256="6" * 64,
        pier_egress_proxy_image_reference_sha256="8" * 64,
        agent_image_content_digest="sha256:" + "7" * 64,
        verifier_image_content_digest="sha256:" + "7" * 64,
        pier_egress_proxy_image_content_digest="sha256:" + "9" * 64,
        model="fixture-model",
        reasoning_effort="max",
        file_edit_authority_enabled=True,
        resources={
            "cpus": 2,
            "memory_mb": 4096,
            "storage_mb": 10240,
            "max_tokens": 1024,
            "context_window": 8192,
        },
        timeouts={
            "agent_seconds": 300,
            "deadline_reserve_seconds": 60,
            "verifier_seconds": 300,
        },
        llm_retry={"retry_count": 3, "retry_base_delay_ms": 1000, "retry_max_delay_ms": 30000},
        network_translation_warning="translated",
    )


def _fixture_task_tree_hash() -> str:
    content = TASK_TOML.encode()
    digest = hashlib.sha256()
    for entry_type, relative in (
        (b"directory", b"environment"),
        (b"file", b"task.toml"),
        (b"directory", b"tests"),
    ):
        digest.update(entry_type)
        digest.update(b"\0")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        if entry_type == b"file":
            digest.update(b"\0")
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
    return digest.hexdigest()


def _fixture_package_tree_hash(package_name: str) -> str:
    digest = hashlib.sha256()
    for entry_type, relative in (
        (b"directory", package_name.encode("utf-8")),
        (b"file", f"{package_name}/__init__.py".encode()),
    ):
        digest.update(entry_type)
        digest.update(b"\0")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        if entry_type == b"file":
            digest.update(b"\0")
            digest.update((0).to_bytes(8, "big"))
    return digest.hexdigest()
