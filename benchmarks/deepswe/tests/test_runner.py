import hashlib
import json
import os
import tempfile
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
    EVALUATION_FILE_READ_MAX_CHARS,
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
from acn_deepswe.plan import build_attempt_plan
from acn_deepswe.provenance import EvaluationProvenance
from acn_deepswe.runner import (
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


class ConfigGenerationTests(unittest.TestCase):
    def test_claim_variants_attempt_toml_declare_a_claim_bundle(self) -> None:
        plan = build_attempt_plan(DATASET, Path("/tmp/acn-eval-plan"), seed=2)
        for attempt in plan.attempts:
            with self.subTest(variant=attempt.variant):
                rendered = tomllib.loads(build_attempt_toml(attempt, "fix the bug", 5100))
                self.assertEqual(rendered["workspace_root"], "/app")
                self.assertTrue(
                    rendered["task_prompt"].startswith("先读取并遵循 /coding-benchmark")
                )
                self.assertEqual(rendered["attempt_deadline_secs"], 5100)
                self.assertEqual(rendered["model_egress_mode"], "pier")
                self.assertEqual(
                    rendered.get("claim_bundle"),
                    (
                        "/opt/acn-eval/claims.json"
                        if attempt.variant in {"B_claim", "B_forced_claim"}
                        else None
                    ),
                )

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
        self.assertEqual(llm["temperature"], 0.95)
        self.assertEqual(llm["top_p"], 1.0)
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
        self.assertFalse(parsed["agent"]["tool"]["file_edit_authority_enabled"])
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
            {step.phase for step in steps},
            {"A", "freeze", "B_empty", "B_claim", "B_forced_claim"},
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
            {step.phase for step in steps},
            {"A", "freeze", "B_empty", "B_claim", "B_forced_claim"},
        )


class IsolationTests(unittest.TestCase):
    def test_pier_task_identity_requires_both_task_name_and_stable_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            task_toml = Path(directory) / "task.toml"
            task_toml.write_text(TASK_TOML, encoding="utf-8")

            self.assertTrue(
                _pier_task_matches_attempt(task_toml, "task-1", "datacurve/task-1")
            )
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

        self.assertEqual(variants, [item.variant for item in plan.attempts])
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
            all(item["progress_path"].endswith("/progress.json") for item in manifest["attempt_results"])
        )

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

    def test_a_verifier_gate_failure_stops_before_b_empty(self) -> None:
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

        self.assertEqual(len(manifest["attempt_results"]), 1)
        self.assertEqual(manifest["attempt_results"][0]["status"], "gate_failed")
        self.assertIn("VERIFIER_DID_NOT_RUN", manifest["attempt_results"][0]["reason"])

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
                _write_fake_trial(Path(job["jobs_dir"]), attempt, bundle_path=artifacts.claim_bundle)
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

        self.assertEqual(variants, ["A", "B_empty"])
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
            execution = replace(
                _execution(root, artifacts), run_all_variants_without_claims=True
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
                    emit_claim=False,
                )
                return FakeCompleted(0)

            with patch.dict("os.environ", {HOST_MODEL_KEY_ENV: "upstream-secret"}, clear=True):
                Task1HostRunner(experiment, root / "jobs", execution, run=run).run_task1(
                    execute=True
                )
            manifest = json.loads(execution.manifest_path.read_text())

        self.assertEqual(variants, [item.variant for item in plan.attempts])
        self.assertEqual(
            [item["status"] for item in manifest["attempt_results"]], ["passed"] * 4
        )
        claim_records = [
            item for item in manifest["attempt_results"] if item["variant"] in {"B_claim", "B_forced_claim"}
        ]
        self.assertTrue(
            all(item["claim_observation"]["bundle_available"] is False for item in claim_records)
        )
        self.assertTrue(
            all("EMPTY_CLAIM_BUNDLE" in item["reason"] for item in claim_records)
        )
        self.assertTrue(manifest["execution"]["run_all_variants_without_claims"])

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

        self.assertEqual(variants, ["A"])
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
    verifier_passed: bool = True,
) -> None:
    attempt_id = attempt.attempt_id
    variant = attempt.variant
    trial = jobs_dir / attempt_id / "trial-1"
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
    if variant == "A" and emit_claim:
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
                "trial_name": attempt.attempt_id,
                "trial_uri": trial.resolve().as_uri(),
                "task_checksum": "checksum",
                "config": {},
                "agent_info": {},
                "verifier_result": {"rewards": {"reward": 1 if verifier_passed else 0}}
                if verifier_ran
                else None,
            }
        )
    )


def provenance() -> EvaluationProvenance:
    return EvaluationProvenance(
        deepswe_revision="deepswe@abc",
        pier_revision="datacurve-pier==0.3.0",
        acn_revision="acn@def",
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
        agent_image_content_digest="sha256:" + "7" * 64,
        verifier_image_content_digest="sha256:" + "7" * 64,
        model="fixture-model",
        reasoning_effort="max",
        resources={
            "cpus": 2,
            "memory_mb": 4096,
            "storage_mb": 10240,
            "max_tokens": 1024,
            "context_window": 8192,
        },
        timeouts={"agent_seconds": 300, "deadline_reserve_seconds": 60},
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
