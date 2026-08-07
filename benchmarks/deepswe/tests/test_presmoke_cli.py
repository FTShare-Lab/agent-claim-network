import hashlib
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from acn_deepswe.presmoke_cli import (
    PresmokeCliError,
    _effective_config_hash,
    build_task_specs,
    load_config,
    main,
    stage_python_runtime,
    verify_acn_revision,
    verify_checkout_revision,
    verify_pier_executable_binding,
)
from acn_deepswe.provenance import TASK_DIRECTORY_HASH_ALGORITHM, sha256_directory_tree

TASK_IDS = (
    "bandit-structured-nosec-directives",
    "ipython-session-bundle-replay",
    "koota-entity-snapshot-rollback",
    "pwntools-tube-multiplexing",
    "sql-formatter-bigquery-pipe-formatting",
)


class PresmokeCliTests(unittest.TestCase):
    def test_directory_tree_hash_covers_instruction_environment_and_tests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            task = Path(directory) / "task"
            (task / "environment").mkdir(parents=True)
            (task / "tests").mkdir()
            (task / "task.toml").write_text("task", encoding="utf-8")
            (task / "instruction.md").write_text("instruction", encoding="utf-8")
            (task / "environment" / "Dockerfile").write_text("FROM base", encoding="utf-8")
            (task / "tests" / "test_task.py").write_text("assert True", encoding="utf-8")
            initial = sha256_directory_tree(task)
            (task / "instruction.md").write_text("changed instruction", encoding="utf-8")
            self.assertNotEqual(initial, sha256_directory_tree(task))
            (task / "instruction.md").write_text("instruction", encoding="utf-8")
            initial = sha256_directory_tree(task)
            (task / "environment" / "Dockerfile").write_text("FROM changed", encoding="utf-8")
            self.assertNotEqual(initial, sha256_directory_tree(task))
            (task / "environment" / "Dockerfile").write_text("FROM base", encoding="utf-8")
            initial = sha256_directory_tree(task)
            (task / "tests" / "test_task.py").write_text("assert False", encoding="utf-8")
            self.assertNotEqual(initial, sha256_directory_tree(task))

    def test_directory_tree_hash_rejects_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            task = Path(directory) / "task"
            task.mkdir()
            (task / "task.toml").write_text("task", encoding="utf-8")
            (task / "linked.txt").symlink_to(task / "task.toml")
            with self.assertRaisesRegex(ValueError, "symlink"):
                sha256_directory_tree(task)

    def test_directory_tree_hash_covers_executable_bit_and_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            task = Path(directory) / "task"
            task.mkdir()
            script = task / "script.sh"
            script.write_text("#!/bin/sh\n", encoding="utf-8")
            initial = sha256_directory_tree(task)
            script.chmod(0o755)
            self.assertNotEqual(initial, sha256_directory_tree(task))
            script.chmod(0o644)
            initial = sha256_directory_tree(task)
            (task / "empty").mkdir()
            self.assertNotEqual(initial, sha256_directory_tree(task))

    def test_required_directory_hash_freezes_non_toml_task_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_fixture(Path(directory))
            config = load_config(config_path)
            task_id = TASK_IDS[0]
            (config.source_tasks_root / task_id / "environment" / "Dockerfile").write_text(
                "FROM changed", encoding="utf-8"
            )
            with (
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                self.assertRaisesRegex(PresmokeCliError, "source task 目录.*hash 不匹配"),
            ):
                build_task_specs(config, "https://upstream.invalid")

    def test_manifest_without_directory_hashes_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_fixture(Path(directory))
            manifest_path = Path(
                json.loads(config_path.read_text(encoding="utf-8"))["frozen_manifest"]
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest.pop("task_directory_hashes")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with (
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                self.assertRaisesRegex(PresmokeCliError, "task_directory_hashes"),
            ):
                build_task_specs(load_config(config_path), "https://upstream.invalid")

    def test_manifest_without_directory_hash_algorithm_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_fixture(Path(directory))
            manifest_path = Path(
                json.loads(config_path.read_text(encoding="utf-8"))["frozen_manifest"]
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest.pop("task_directory_hash_algorithm")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with (
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                self.assertRaisesRegex(PresmokeCliError, "task_directory_hash_algorithm"),
            ):
                build_task_specs(load_config(config_path), "https://upstream.invalid")

    def test_checkout_revision_rejects_dirty_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout = Path(directory) / "checkout"
            checkout.mkdir()
            with (
                patch(
                    "acn_deepswe.presmoke_cli.subprocess.run",
                    side_effect=[
                        completed(["git"], stdout="frozen-revision\n"),
                        completed(["git"], stdout=" M src.py\n"),
                    ],
                ),
                self.assertRaisesRegex(PresmokeCliError, "工作树不干净"),
            ):
                verify_checkout_revision(checkout, "frozen-revision")

    def test_acn_revision_binds_head_and_dirty_label(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout = Path(directory)
            with patch(
                "acn_deepswe.presmoke_cli._run_checkout_git",
                side_effect=[
                    completed(["git"], stdout="abc123\n"),
                    completed(["git"], stdout=" M file.py\n"),
                ],
            ):
                verify_acn_revision("abc123+evaluation-worktree", checkout)

    def test_staged_python_runtime_is_immutable_after_sources_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = load_config(write_fixture(Path(directory)))
            runtime = stage_python_runtime(config)
            staged_pier = runtime.pier_source_root / "pier" / "__init__.py"
            before = staged_pier.read_text(encoding="utf-8")

            (config.pier_checkout / "src" / "pier" / "__init__.py").write_text(
                "changed\n", encoding="utf-8"
            )

            self.assertEqual(staged_pier.read_text(encoding="utf-8"), before)
            self.assertFalse(staged_pier.stat().st_mode & 0o222)

    def test_pier_executable_binding_requires_checkout_source_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout, executable = write_pier_venv(Path(directory))
            evidence = pier_install_evidence(checkout)
            with patch(
                "acn_deepswe.presmoke_cli._run_preflight_command",
                return_value=completed(["python"], stdout=json.dumps(evidence)),
            ):
                verify_pier_executable_binding(checkout, executable)

    def test_pier_executable_binding_rejects_external_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkout, executable = write_pier_venv(root)
            foreign = root / "foreign-pier"
            foreign.mkdir()
            evidence = pier_install_evidence(foreign)
            with (
                patch(
                    "acn_deepswe.presmoke_cli._run_preflight_command",
                    return_value=completed(["python"], stdout=json.dumps(evidence)),
                ),
                self.assertRaisesRegex(PresmokeCliError, "来源不匹配"),
            ):
                verify_pier_executable_binding(checkout, executable)

    def test_pier_executable_binding_rejects_noneditable_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout, executable = write_pier_venv(Path(directory))
            evidence = pier_install_evidence(checkout)
            evidence["direct_url"] = json.dumps(
                {"url": checkout.as_uri(), "dir_info": {"editable": False}}
            )
            with (
                patch(
                    "acn_deepswe.presmoke_cli._run_preflight_command",
                    return_value=completed(["python"], stdout=json.dumps(evidence)),
                ),
                self.assertRaisesRegex(PresmokeCliError, "editable"),
            ):
                verify_pier_executable_binding(checkout, executable)

    def test_pier_executable_binding_rejects_foreign_console_interpreter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout, executable = write_pier_venv(Path(directory))
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            with self.assertRaisesRegex(PresmokeCliError, "同一 venv"):
                verify_pier_executable_binding(checkout, executable)

    def test_checked_in_presmoke_manifest_freezes_every_task_directory(self) -> None:
        manifest_path = Path(__file__).parents[1] / "manifests" / "presmoke-v1.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest["task_directory_hash_algorithm"], TASK_DIRECTORY_HASH_ALGORITHM)
        hashes = manifest["task_directory_hashes"]
        self.assertEqual(set(hashes), set(manifest["task_ids"]))
        for task_id in manifest["task_ids"]:
            self.assertEqual(set(hashes[task_id]), {"source", "normalized"})
            self.assertTrue(all(len(value) == 64 for value in hashes[task_id].values()))

    def test_missing_upstream_key_returns_nonzero_without_leaking_environment_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            environment = {"ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid"}
            with (
                patch.dict(os.environ, environment, clear=True),
                self.assertRaises(SystemExit) as raised,
            ):
                main(["--config", str(config)])
        self.assertNotEqual(raised.exception.code, 0)

    def test_dry_run_prints_plan_without_secret_or_runner_invocation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            secret = "do-not-print-this-key"
            with (
                patch.dict(
                    os.environ,
                    {
                        "ACN_EVAL_UPSTREAM_KEY": secret,
                        "ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid",
                    },
                    clear=True,
                ),
                patch("acn_deepswe.presmoke_cli.PresmokeHostRunner") as runner,
                patch("sys.stdout") as stdout,
                patch("acn_deepswe.presmoke_cli.verify_acn_revision"),
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                patch("acn_deepswe.presmoke_cli.subprocess.run") as commands,
            ):
                result = main(["--config", str(config), "--dry-run"])
        self.assertEqual(result, 0)
        runner.assert_not_called()
        commands.assert_not_called()
        rendered = "".join(str(call.args[0]) for call in stdout.write.call_args_list)
        self.assertNotIn(secret, rendered)
        self.assertIn("B_claim", rendered)
        self.assertIn("task_workers", rendered)
        self.assertIn("fixture-model", rendered)
        self.assertIn("fixture-checkpoint", rendered)

    def test_response_model_is_required_and_not_implicitly_copied_from_model(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_fixture(Path(directory))
            raw = json.loads(config_path.read_text(encoding="utf-8"))
            raw.pop("response_model")
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(PresmokeCliError, "response_model"):
                load_config(config_path)

    def test_reasoning_effort_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_fixture(Path(directory))
            raw = json.loads(config_path.read_text(encoding="utf-8"))
            raw.pop("reasoning_effort")
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(PresmokeCliError, "reasoning_effort"):
                load_config(config_path)

    def test_effective_config_hash_locks_response_model_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = load_config(write_fixture(Path(directory)))
        changed = replace(config, response_model="other-checkpoint")
        self.assertNotEqual(_effective_config_hash(config), _effective_config_hash(changed))
        changed_effort = replace(config, reasoning_effort="high")
        self.assertNotEqual(_effective_config_hash(config), _effective_config_hash(changed_effort))

    def test_rejects_removed_root_tool_loop_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_fixture(Path(directory))
            raw = json.loads(config_path.read_text(encoding="utf-8"))
            raw["resources"]["max_tool_loop_turns"] = 32
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(PresmokeCliError, "max_tool_loop_turns"):
                load_config(config_path)

    def test_rejects_relative_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory), output_dir="relative-output")
            with (
                patch.dict(
                    os.environ,
                    {
                        "ACN_EVAL_UPSTREAM_KEY": "test-key",
                        "ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid",
                    },
                    clear=True,
                ),
                self.assertRaises(SystemExit) as raised,
            ):
                main(["--config", str(config), "--dry-run"])
        self.assertNotEqual(raised.exception.code, 0)

    def test_constructs_presmoke_runner_and_executes_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            fake_runner = _FakeRunner()
            with (
                patch.dict(
                    os.environ,
                    {
                        "ACN_EVAL_UPSTREAM_KEY": "test-key",
                        "ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid",
                    },
                    clear=True,
                ),
                patch(
                    "acn_deepswe.presmoke_cli.PresmokeHostRunner", return_value=fake_runner
                ) as runner,
                patch("acn_deepswe.presmoke_cli.verify_acn_revision"),
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                patch("acn_deepswe.presmoke_cli.os.access", return_value=True),
                patch(
                    "acn_deepswe.presmoke_cli.subprocess.run",
                    side_effect=successful_preflight_commands(config.parent),
                ),
            ):
                result = main(["--config", str(config)])
        self.assertEqual(result, 0)
        runner.assert_called_once()
        specs = runner.call_args.args[0]
        self.assertEqual(tuple(spec.task_id for spec in specs), TASK_IDS)
        self.assertEqual(fake_runner.calls, [True])

    def test_existing_upstream_key_takes_priority_over_stdin_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            existing_key = "existing-test-secret"
            fake_runner = _FakeRunner()
            with (
                patch.dict(
                    os.environ,
                    {
                        "ACN_EVAL_UPSTREAM_KEY": existing_key,
                        "ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid",
                    },
                    clear=True,
                ),
                patch("acn_deepswe.presmoke_cli.getpass.getpass") as read_key,
                patch("acn_deepswe.presmoke_cli.PresmokeHostRunner", return_value=fake_runner),
                patch("acn_deepswe.presmoke_cli.verify_acn_revision"),
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                patch("acn_deepswe.presmoke_cli.os.access", return_value=True),
                patch(
                    "acn_deepswe.presmoke_cli.subprocess.run",
                    side_effect=successful_preflight_commands(config.parent),
                ),
            ):
                result = main(["--config", str(config), "--read-key-stdin"])
                self.assertEqual(os.environ.get("ACN_EVAL_UPSTREAM_KEY"), existing_key)
        self.assertEqual(result, 0)
        read_key.assert_not_called()

    def test_read_key_stdin_injects_only_for_execution_and_clears_afterward(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            secret = "stdin-test-secret"
            fake_runner = _FakeRunner()
            with (
                patch.dict(
                    os.environ,
                    {"ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid"},
                    clear=True,
                ),
                patch("acn_deepswe.presmoke_cli.getpass.getpass", return_value=secret) as read_key,
                patch("acn_deepswe.presmoke_cli.PresmokeHostRunner", return_value=fake_runner),
                patch("acn_deepswe.presmoke_cli.verify_acn_revision"),
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                patch("acn_deepswe.presmoke_cli.os.access", return_value=True),
                patch(
                    "acn_deepswe.presmoke_cli.subprocess.run",
                    side_effect=successful_preflight_commands(config.parent),
                ),
                patch("sys.stdout") as stdout,
                patch("sys.stderr") as stderr,
            ):
                result = main(["--config", str(config), "--read-key-stdin"])
        self.assertEqual(result, 0)
        read_key.assert_called_once()
        self.assertEqual(fake_runner.calls, [True])
        self.assertEqual(fake_runner.upstream_key_during_run, secret)
        self.assertNotIn("ACN_EVAL_UPSTREAM_KEY", os.environ)
        rendered = "".join(
            str(call.args[0]) for stream in (stdout, stderr) for call in stream.write.call_args_list
        )
        self.assertNotIn(secret, rendered)

    def test_read_key_stdin_rejects_empty_value_without_running(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            secret = "empty-stdin-test-secret"
            with (
                patch.dict(
                    os.environ,
                    {"ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid"},
                    clear=True,
                ),
                patch("acn_deepswe.presmoke_cli.getpass.getpass", return_value="") as read_key,
                patch("sys.stderr") as stderr,
                patch("acn_deepswe.presmoke_cli.PresmokeHostRunner") as runner,
                self.assertRaises(SystemExit) as raised,
            ):
                main(["--config", str(config), "--read-key-stdin"])
        self.assertNotEqual(raised.exception.code, 0)
        read_key.assert_called_once()
        runner.assert_not_called()
        rendered = "".join(str(call.args[0]) for call in stderr.write.call_args_list)
        self.assertNotIn(secret, rendered)

    def test_dry_run_never_reads_key_from_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            with (
                patch.dict(
                    os.environ,
                    {"ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid"},
                    clear=True,
                ),
                patch("acn_deepswe.presmoke_cli.getpass.getpass") as read_key,
                patch("acn_deepswe.presmoke_cli.verify_acn_revision"),
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                patch("acn_deepswe.presmoke_cli.subprocess.run"),
            ):
                result = main(["--config", str(config), "--dry-run", "--read-key-stdin"])
        self.assertEqual(result, 0)
        read_key.assert_not_called()

    def test_preflight_rejects_insufficient_docker_resources_before_runner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = write_fixture(Path(directory))
            with (
                patch.dict(
                    os.environ,
                    {
                        "ACN_EVAL_UPSTREAM_KEY": "test-key",
                        "ACN_EVAL_UPSTREAM_BASE_URL": "https://upstream.invalid",
                    },
                    clear=True,
                ),
                patch("acn_deepswe.presmoke_cli.verify_acn_revision"),
                patch("acn_deepswe.presmoke_cli.verify_checkout_revision"),
                patch("acn_deepswe.presmoke_cli.os.access", return_value=True),
                patch(
                    "acn_deepswe.presmoke_cli.subprocess.run",
                    side_effect=insufficient_resource_commands(config.parent),
                ),
                patch("acn_deepswe.presmoke_cli.PresmokeHostRunner") as runner,
                self.assertRaises(SystemExit) as raised,
            ):
                main(["--config", str(config)])
        self.assertNotEqual(raised.exception.code, 0)
        runner.assert_not_called()


class _FakeRunner:
    def __init__(self) -> None:
        self.calls: list[bool] = []
        self.upstream_key_during_run: str | None = None

    def run(self, *, execute: bool) -> tuple[object, ...]:
        self.calls.append(execute)
        self.upstream_key_during_run = os.environ.get("ACN_EVAL_UPSTREAM_KEY")
        return ()


def write_fixture(root: Path, *, output_dir: str | None = None) -> Path:
    normalized = root / "normalized"
    source = root / "source-tasks"
    for task_id in TASK_IDS:
        for task_root in (normalized / task_id, source / task_id):
            (task_root / "environment").mkdir(parents=True)
            (task_root / "tests").mkdir()
            (task_root / "task.toml").write_text(
                '[environment]\ndocker_image = "registry.example/task:1"\n', encoding="utf-8"
            )
        (normalized / task_id / "instruction.md").write_text(task_id, encoding="utf-8")
    acn_eval = root / "acn_eval"
    acn_eval.write_text("binary", encoding="utf-8")
    pier_checkout, pier = write_pier_venv(root)
    skill = root / "coding-benchmark"
    skill.mkdir()
    (skill / "SKILL.md").write_text("skill", encoding="utf-8")
    manifest = {
        "schema_version": 1,
        "algorithm": "random.sample_without_replacement_v1",
        "seed": 20260726,
        "candidates_hash": "a" * 64,
        "deepswe_revision": "deepswe-rev",
        "pier_revision": "pier-rev",
        "task_ids": list(TASK_IDS),
        "task_directory_hash_algorithm": TASK_DIRECTORY_HASH_ALGORITHM,
        "task_toml_hashes": {
            task_id: {
                "source": digest(source / task_id / "task.toml"),
                "normalized": digest(normalized / task_id / "task.toml"),
            }
            for task_id in TASK_IDS
        },
        "task_directory_hashes": {
            task_id: {
                "source": sha256_directory_tree(source / task_id),
                "normalized": sha256_directory_tree(normalized / task_id),
            }
            for task_id in TASK_IDS
        },
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    attempts = []
    for task_id in TASK_IDS:
        for variant in ("A", "B_empty", "B_claim"):
            base = root / "attempts" / f"{task_id}-{variant}"
            attempts.append(
                {
                    "schema_version": 1,
                    "attempt_id": f"{task_id}-{variant}",
                    "task_id": task_id,
                    "variant": variant,
                    "output_path": str(base / "output"),
                }
            )
    (root / "attempt-plan.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "freeze_candidates_hash": "a" * 64,
                "seed": 20260726,
                "attempts": attempts,
            }
        ),
        encoding="utf-8",
    )
    config = {
        "frozen_manifest": str(manifest_path),
        "attempt_plan": str(root / "attempt-plan.json"),
        "deepswe_checkout": str(root / "deepswe"),
        "source_tasks_root": str(source),
        "pier_checkout": str(pier_checkout),
        "pier_executable": str(pier),
        "acn_eval": str(acn_eval),
        "frozen_skill": str(skill),
        "normalized_root": str(normalized),
        "output_dir": output_dir or str(root / "output"),
        "model": "fixture-model",
        "response_model": "fixture-checkpoint",
        "reasoning_effort": "max",
        "acn_revision": "acn-rev",
        "resources": {
            "cpus": 2,
            "memory_mb": 4096,
            "storage_mb": 1024,
            "max_tokens": 128,
            "context_window": 256,
        },
        "timeouts": {"agent_seconds": 60, "deadline_reserve_seconds": 30},
        "llm_retry": {"retry_count": 3, "retry_base_delay_ms": 1000, "retry_max_delay_ms": 30000},
    }
    path = root / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_pier_venv(root: Path) -> tuple[Path, Path]:
    checkout = root / "pier-checkout"
    checkout.mkdir()
    package = checkout / "src" / "pier"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("__version__ = 'fixture'\n", encoding="utf-8")
    bin_dir = root / "pier-venv" / "bin"
    bin_dir.mkdir(parents=True)
    python = bin_dir / "python"
    python.write_text("#!/bin/sh\n", encoding="utf-8")
    python.chmod(0o755)
    python3 = bin_dir / "python3"
    python3.symlink_to("python")
    executable = bin_dir / "pier"
    executable.write_text(f"#!{python3}\n", encoding="utf-8")
    executable.chmod(0o755)
    return checkout, executable


def pier_install_evidence(checkout: Path, *, version: str = "0.3.0") -> dict[str, str]:
    return {
        "version": version,
        "direct_url": json.dumps({"url": checkout.as_uri(), "dir_info": {"editable": True}}),
    }


def successful_preflight_commands(root: Path) -> list[object]:
    return [
        completed(["python"], stdout=json.dumps(pier_install_evidence(root / "pier-checkout"))),
        completed(["pier", "--help"]),
        completed(
            ["docker", "info", "--format", "{{json .}}"],
            stdout=json.dumps(
                {
                    "NCPU": 8,
                    "MemTotal": 4 * 4096 * 1024 * 1024,
                }
            ),
        ),
        *[
            completed(
                [
                    "docker",
                    "image",
                    "inspect",
                    "--format",
                    "{{.Id}}",
                    "registry.example/task:1",
                ],
                stdout="sha256:" + "d" * 64,
            )
            for _ in TASK_IDS
        ],
    ]


def insufficient_resource_commands(root: Path) -> list[object]:
    return [
        completed(["python"], stdout=json.dumps(pier_install_evidence(root / "pier-checkout"))),
        completed(["pier", "--help"]),
        completed(
            ["docker", "info", "--format", "{{json .}}"],
            stdout=json.dumps(
                {
                    "NCPU": 1,
                    "MemTotal": 2048 * 1024 * 1024,
                }
            ),
        ),
    ]


def completed(command: list[str], *, stdout: str = "") -> object:
    from subprocess import CompletedProcess

    return CompletedProcess(command, 0, stdout=stdout, stderr="")
