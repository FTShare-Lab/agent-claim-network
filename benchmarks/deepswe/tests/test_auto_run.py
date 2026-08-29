import json
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from acn_deepswe.auto_run import (
    AutomatedRunError,
    _read_upstream_key_stdin,
    load_config,
    monitor_run,
    prepare_run,
    run_automated,
)
from acn_deepswe.dataset import FrozenDatasetManifest
from acn_deepswe.run_lock import RunLockError


TASK_IDS = tuple(f"task-{index}" for index in range(6))


class AutomatedRunTests(unittest.TestCase):
    def test_hidden_key_read_requires_a_nonempty_value(self) -> None:
        with patch("acn_deepswe.auto_run.getpass.getpass", return_value="test-credential"):
            self.assertEqual(_read_upstream_key_stdin(), "test-credential")
        with patch("acn_deepswe.auto_run.getpass.getpass", return_value=""):
            with self.assertRaisesRegex(AutomatedRunError, "不能为空"):
                _read_upstream_key_stdin()

    def test_config_forwards_run_a_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(
                write_config(
                    root,
                    harness_mode="minimal",
                    run_a_only=True,
                    smoke_size=0,
                    full_size=len(TASK_IDS),
                )
            )
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                prepare_run(config)
            full_config = json.loads((config.run_root / "full" / "presmoke-run.json").read_text())

        self.assertTrue(config.run_a_only)
        self.assertEqual(config.harness_mode, "minimal")
        self.assertTrue(full_config["run_a_only"])
        self.assertEqual(full_config["harness_mode"], "minimal")

    def test_config_forwards_b_only_source_for_full_task_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_output = root / "a-only-output"
            config = load_config(
                write_config(
                    root,
                    b_only_from_a_output_dir=str(source_output),
                    run_all_variants_without_claims=True,
                    smoke_size=0,
                    full_size=len(TASK_IDS),
                )
            )
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                summary = prepare_run(config)
            full_config = json.loads((config.run_root / "full" / "presmoke-run.json").read_text())

        self.assertEqual(summary["phase_mode"], "b_only_from_a")
        self.assertEqual(full_config["b_only_from_a_output_dir"], str(source_output))
        self.assertTrue(full_config["run_all_variants_without_claims"])

    def test_b_only_source_rejects_smoke_partition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = write_config(
                root,
                b_only_from_a_output_dir=str(root / "a-only-output"),
                smoke_size=1,
                full_size=len(TASK_IDS),
            )
            with self.assertRaisesRegex(AutomatedRunError, "smoke_size=0"):
                load_config(path)

    def test_b_only_source_rejects_overlapping_output_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = write_config(
                root,
                b_only_from_a_output_dir=str(root / "run" / "full" / "output"),
                smoke_size=0,
                full_size=len(TASK_IDS),
            )
            with self.assertRaisesRegex(AutomatedRunError, "必须完全隔离"):
                load_config(path)

    def test_config_rejects_credential_field(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(Path(directory), api_key="must-not-be-here")
            with self.assertRaisesRegex(AutomatedRunError, "credential"):
                load_config(path)

    def test_formal_config_accepts_disabled_file_edit_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(
                Path(directory),
                run_class="formal",
                file_edit_authority_enabled=False,
                host_capacity={
                    "memory_reserve_mb": 1024,
                    "disk_reserve_mb": 1024,
                    "disk_admission_mb_per_worker": 8192,
                },
            )
            config = load_config(path)

        self.assertFalse(config.file_edit_authority_enabled)

    def test_formal_config_accepts_minimal_harness_with_pier_egress(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(
                Path(directory),
                run_class="formal",
                harness_mode="minimal",
                model_egress_mode="pier",
                host_capacity={
                    "memory_reserve_mb": 1024,
                    "disk_reserve_mb": 1024,
                    "disk_admission_mb_per_worker": 8192,
                },
            )
            config = load_config(path)

        self.assertEqual(config.harness_mode, "minimal")
        self.assertEqual(config.model_egress_mode, "pier")

    def test_formal_minimal_config_rejects_direct_model_egress(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(
                Path(directory),
                run_class="formal",
                harness_mode="minimal",
                model_egress_mode="direct",
                host_capacity={
                    "memory_reserve_mb": 1024,
                    "disk_reserve_mb": 1024,
                    "disk_admission_mb_per_worker": 8192,
                },
            )
            with self.assertRaisesRegex(AutomatedRunError, "model_egress_mode=pier"):
                load_config(path)

    def test_formal_config_requires_transient_docker_budget_per_worker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(Path(directory), run_class="formal")
            with self.assertRaisesRegex(AutomatedRunError, "8192"):
                load_config(path)

    def test_formal_config_does_not_preallocate_nominal_task_storage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(
                Path(directory),
                run_class="formal",
                host_capacity={
                    "memory_reserve_mb": 1024,
                    "disk_reserve_mb": 1024,
                    "disk_admission_mb_per_worker": 8192,
                },
            )
            config = load_config(path)
        self.assertEqual(config.resources["storage_mb"], 20480)
        self.assertEqual(config.host_capacity["disk_admission_mb_per_worker"], 8192)

    def test_formal_config_rejects_a_different_product_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(
                Path(directory),
                run_class="formal",
                acn_main_revision="a" * 40,
                host_capacity={
                    "memory_reserve_mb": 1024,
                    "disk_reserve_mb": 1024,
                    "disk_admission_mb_per_worker": 8192,
                },
            )
            with self.assertRaisesRegex(AutomatedRunError, "9b818d70"):
                load_config(path)

    def test_formal_config_requires_the_frozen_pier_proxy_image(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_config(
                Path(directory),
                run_class="formal",
                pier_egress_proxy_image="other/image:latest",
                host_capacity={
                    "memory_reserve_mb": 1024,
                    "disk_reserve_mb": 1024,
                    "disk_admission_mb_per_worker": 8192,
                },
            )
            with self.assertRaisesRegex(AutomatedRunError, "Pier egress proxy"):
                load_config(path)

    def test_prepare_splits_smoke_from_remaining_full_without_repeating_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=2, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                summary = prepare_run(config)

            smoke = json.loads((config.run_root / "smoke" / "frozen-manifest.json").read_text())
            full = json.loads((config.run_root / "full" / "frozen-manifest.json").read_text())
            smoke_config = json.loads((config.run_root / "smoke" / "presmoke-run.json").read_text())
            smoke_plan = json.loads((config.run_root / "smoke" / "attempt-plan.json").read_text())

        self.assertEqual(summary["status"], "prepared")
        self.assertEqual(len(smoke["task_ids"]), 2)
        self.assertEqual(len(full["task_ids"]), 4)
        self.assertEqual(set(smoke["task_ids"]) | set(full["task_ids"]), set(TASK_IDS))
        self.assertFalse(set(smoke["task_ids"]) & set(full["task_ids"]))
        self.assertEqual(smoke_config["model"], "deepseek-v4-flash-local-exp")
        self.assertEqual(smoke_config["response_model"], "deepseek-v4-flash-local-exp")
        self.assertEqual(smoke_config["task_workers"], 30)
        self.assertEqual(smoke_config["harness_mode"], "standard")
        self.assertFalse(smoke_config["run_a_only"])
        self.assertNotIn("ACN_EVAL_UPSTREAM_KEY", smoke_config)
        self.assertEqual(
            Path(smoke_config["frozen_manifest"]),
            config.run_root / "smoke" / "frozen-manifest.json",
        )
        self.assertEqual(
            Path(smoke_config["attempt_plan"]),
            config.run_root / "smoke" / "attempt-plan.json",
        )
        self.assertEqual(Path(smoke_config["normalized_root"]), config.run_root / "normalized")
        self.assertEqual(Path(smoke_config["output_dir"]), config.run_root / "smoke" / "output")
        self.assertTrue(smoke_plan["attempts"])
        self.assertTrue(
            all(
                Path(attempt["output_path"]).is_relative_to(config.run_root / "smoke")
                for attempt in smoke_plan["attempts"]
            )
        )

    def test_zero_smoke_prepares_and_runs_the_full_task_set_directly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=0, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                summary = prepare_run(config)

            full = json.loads((config.run_root / "full" / "frozen-manifest.json").read_text())
            calls: list[str] = []

            def run_phase(command: list[str], *, check: bool) -> CompletedProcess[str]:
                config_path = Path(command[-1])
                calls.append(config_path.parent.name)
                phase_config = json.loads(config_path.read_text())
                output_dir = Path(phase_config["output_dir"])
                output_dir.mkdir(parents=True)
                (output_dir / "presmoke-aggregate.json").write_text(
                    json.dumps({"status": "passed"}), encoding="utf-8"
                )
                return CompletedProcess(command, 0)

            with patch("acn_deepswe.auto_run.subprocess.run", side_effect=run_phase):
                run_summary = run_automated(config)

        self.assertTrue(summary["smoke"]["skipped"])
        self.assertEqual(full["task_ids"], list(TASK_IDS))
        self.assertEqual(calls, ["full"])
        self.assertEqual(run_summary["status"], "completed")

    def test_run_starts_remaining_full_only_after_completed_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=2, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                prepare_run(config)

            calls: list[str] = []

            def run_phase(command: list[str], *, check: bool) -> CompletedProcess[str]:
                config_path = Path(command[-1])
                calls.append(config_path.parent.name)
                phase_config = json.loads(config_path.read_text())
                output_dir = Path(phase_config["output_dir"])
                output_dir.mkdir(parents=True)
                (output_dir / "presmoke-aggregate.json").write_text(
                    json.dumps({"status": "passed"}), encoding="utf-8"
                )
                return CompletedProcess(command, 0)

            with patch("acn_deepswe.auto_run.subprocess.run", side_effect=run_phase):
                summary = run_automated(config)

        self.assertEqual(calls, ["smoke", "full"])
        self.assertEqual(summary["status"], "completed")
        self.assertTrue(summary["phases"]["full"]["completed"])

    def test_run_forwards_hidden_key_read_to_each_phase(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=2, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                prepare_run(config)

            calls: list[list[str]] = []

            def run_phase(command: list[str], *, check: bool) -> CompletedProcess[str]:
                calls.append(command)
                config_path = Path(command[command.index("--config") + 1])
                phase_config = json.loads(config_path.read_text())
                output_dir = Path(phase_config["output_dir"])
                output_dir.mkdir(parents=True)
                (output_dir / "presmoke-aggregate.json").write_text(
                    json.dumps({"status": "passed"}), encoding="utf-8"
                )
                return CompletedProcess(command, 0)

            with patch("acn_deepswe.auto_run.subprocess.run", side_effect=run_phase):
                summary = run_automated(config, read_key_stdin=True)

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(len(calls), 2)
        self.assertTrue(all("--read-key-stdin" in command for command in calls))

    def test_run_does_not_start_full_after_smoke_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=2, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                prepare_run(config)

            with patch(
                "acn_deepswe.auto_run.subprocess.run",
                return_value=CompletedProcess(["python"], 2),
            ) as run:
                summary = run_automated(config)

        self.assertEqual(run.call_count, 1)
        self.assertEqual(summary["status"], "stopped_after_smoke")
        self.assertFalse(summary["phases"]["full"]["started"])

    def test_interrupted_phase_resumes_only_that_phase_before_starting_full(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=2, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                prepare_run(config)
            smoke_output = config.run_root / "smoke" / "output"
            smoke_output.mkdir(parents=True)
            calls: list[list[str]] = []

            def run_phase(command: list[str], *, check: bool) -> CompletedProcess[str]:
                calls.append(command)
                config_path = Path(command[-1])
                output_dir = Path(json.loads(config_path.read_text())["output_dir"])
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / "presmoke-aggregate.json").write_text(
                    json.dumps({"status": "passed"}), encoding="utf-8"
                )
                return CompletedProcess(command, 0)

            with patch("acn_deepswe.auto_run.subprocess.run", side_effect=run_phase):
                summary = run_automated(config, allow_interrupted_resume=True)

        self.assertEqual(summary["status"], "completed")
        self.assertIn("--resume", calls[0])
        self.assertIn("--retry-interrupted", calls[0])
        self.assertNotIn("--resume", calls[1])

    def test_second_automator_rejected_by_lock_does_not_start_or_overwrite_a_phase(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(write_config(root, smoke_size=0, full_size=len(TASK_IDS)))
            with (
                patch("acn_deepswe.auto_run.freeze_execution_dataset", side_effect=freeze_fixture),
                patch("acn_deepswe.auto_run._current_acn_revision", return_value="acn@fixture"),
            ):
                prepare_run(config)
            with (
                patch(
                    "acn_deepswe.auto_run.exclusive_run_lock",
                    side_effect=RunLockError("already running"),
                ),
                patch("acn_deepswe.auto_run.subprocess.run") as run,
                self.assertRaisesRegex(RunLockError, "already running"),
            ):
                run_automated(config)

        run.assert_not_called()

    def test_monitor_reads_progress_without_starting_or_mutating_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory).resolve()
            progress = run_root / "smoke" / "attempts" / "attempt-1" / "output" / "progress.json"
            progress.parent.mkdir(parents=True)
            progress.write_text(
                json.dumps(
                    {
                        "attempt_id": "attempt-1",
                        "status": "possibly_stalled",
                        "possibly_stalled": True,
                        "observed_at_utc": "2000-01-01T00:00:00Z",
                        "progress_poll_secs": 30,
                    }
                ),
                encoding="utf-8",
            )
            aggregate = run_root / "smoke" / "output" / "presmoke-aggregate.json"
            aggregate.parent.mkdir(parents=True)
            aggregate.write_text(json.dumps({"status": "passed"}), encoding="utf-8")

            summary = monitor_run(run_root)

        self.assertEqual(summary["phase_statuses"], {"smoke": "passed", "full": "not_started"})
        self.assertEqual(summary["progress_statuses"], {"possibly_stalled": 1})
        self.assertEqual(summary["fresh_progress_statuses"], {})
        self.assertEqual(len(summary["possibly_stalled_progress"]), 1)
        self.assertEqual(len(summary["stale_active_progress"]), 1)


def write_config(root: Path, **overrides: object) -> Path:
    config = {
        "run_root": str((root / "run").resolve()),
        "deepswe_checkout": str((root / "deep-swe").resolve()),
        "source_tasks_root": str((root / "deep-swe" / "tasks").resolve()),
        "pier_checkout": str((root / "pier").resolve()),
        "pier_executable": str((root / "pier" / ".venv" / "bin" / "pier").resolve()),
        "pier_egress_proxy_image": "pier-egress-proxy:ubuntu-24.04",
        "pier_egress_proxy_content_digest": "sha256:" + "1" * 64,
        "acn_eval": str((root / "acn_eval").resolve()),
        "frozen_skill": str((root / "skill").resolve()),
        "model": "deepseek-v4-flash-local-exp",
        "response_model": "deepseek-v4-flash-local-exp",
        "reasoning_effort": "max",
        "run_class": "diagnostic",
        "acn_main_revision": "9b818d70ddfad2f7d5e1972577dd294b19481c92",
        "acn_version": "0.2.5",
        "file_edit_authority_enabled": True,
        "task_workers": 30,
        "smoke_size": 2,
        "full_size": len(TASK_IDS),
        "dataset_seed": 7,
        "smoke_plan_seed": 8,
        "full_plan_seed": 9,
        "progress": {"poll_secs": 30, "stall_after_secs": 600},
        "resources": {
            "cpus": 2,
            "memory_mb": 8192,
            "storage_mb": 20480,
            "max_tokens": 65536,
            "context_window": 1000000,
        },
        "timeouts": {
            "agent_seconds": 5400,
            "deadline_reserve_seconds": 120,
            "verifier_seconds": 2700,
        },
        "llm_retry": {"retry_count": 2, "retry_base_delay_ms": 1000, "retry_max_delay_ms": 30000},
        "host_capacity": {
            "memory_reserve_mb": 1024,
            "disk_reserve_mb": 1024,
            "disk_admission_mb_per_worker": 1024,
        },
    }
    config.update(overrides)
    path = root / "auto-run.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path.resolve()


def freeze_fixture(
    tasks_root: Path,
    manifest_path: Path,
    normalized_root: Path,
    deepswe_checkout: Path,
    pier_checkout: Path,
    seed: int,
    *,
    sample_size: int,
    reuse_local_agent_image_fingerprint: str | None = None,
) -> FrozenDatasetManifest:
    del tasks_root, deepswe_checkout, pier_checkout, reuse_local_agent_image_fingerprint
    assert sample_size == len(TASK_IDS)
    normalized_root.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "algorithm": "random.sample_without_replacement_v1",
        "seed": seed,
        "candidates_hash": "a" * 64,
        "task_ids": list(TASK_IDS),
        "deepswe_revision": "deep-swe@fixture",
        "pier_revision": "pier@fixture",
        "task_directory_hash_algorithm": "sha256_directory_tree_v2",
        "task_toml_hashes": {
            task_id: {"source": "b" * 64, "normalized": "c" * 64} for task_id in TASK_IDS
        },
        "task_directory_hashes": {
            task_id: {"source": "d" * 64, "normalized": "e" * 64} for task_id in TASK_IDS
        },
    }
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return FrozenDatasetManifest(
        1, "random.sample_without_replacement_v1", seed, "a" * 64, TASK_IDS
    )
