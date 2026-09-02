import json
import tempfile
import unittest
from pathlib import Path

from acn_deepswe.pier_result import PierResultError, read_single_trial_evidence


class PierResultTests(unittest.TestCase):
    def test_reads_pinned_trial_result_verifier_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            job = Path(directory) / "job"
            trial = job / "trial-1"
            trial.mkdir(parents=True)
            (trial / "result.json").write_text(
                json.dumps(
                    {
                        "task_name": "task",
                        "trial_name": "trial-1",
                        "trial_uri": "file:///trial-1",
                        "task_checksum": "checksum",
                        "config": {},
                        "agent_info": {},
                        "verifier_result": {"rewards": {"reward": 1, "extra": 0.5}},
                        "exception_info": None,
                    }
                )
            )
            trial_dir, evidence = read_single_trial_evidence(job.resolve())
            verifier = evidence.verifier_for("attempt-1")
        self.assertEqual(trial_dir.name, "trial-1")
        self.assertEqual(evidence.task_checksum, "checksum")
        self.assertEqual(evidence.to_dict()["task_checksum"], "checksum")
        self.assertTrue(verifier.passed)
        self.assertEqual(verifier.verifier_exit_code, 0)

    def test_classifies_verifier_timeout_as_infrastructure_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            job = Path(directory) / "job"
            trial = job / "trial-1"
            trial.mkdir(parents=True)
            (trial / "result.json").write_text(
                json.dumps(
                    {
                        "task_name": "task",
                        "trial_name": "trial-1",
                        "trial_uri": "file:///trial-1",
                        "task_checksum": "checksum",
                        "config": {},
                        "agent_info": {},
                        "verifier_result": None,
                        "exception_info": {
                            "exception_type": "VerifierTimeoutError",
                            "exception_message": "Verifier execution timed out",
                        },
                    }
                )
            )
            _, evidence = read_single_trial_evidence(job.resolve())

        self.assertEqual(evidence.infrastructure_failure_reason(), "VERIFIER_TIMEOUT")
        self.assertEqual(evidence.to_dict()["exception_type"], "VerifierTimeoutError")

    def test_classifies_frozen_pier_compose_runtime_error_as_infrastructure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            job = Path(directory) / "job"
            trial = job / "trial-1"
            trial.mkdir(parents=True)
            (trial / "result.json").write_text(
                json.dumps(
                    {
                        "task_name": "task",
                        "trial_name": "trial-1",
                        "trial_uri": "file:///trial-1",
                        "task_checksum": "checksum",
                        "config": {},
                        "agent_info": {},
                        "verifier_result": None,
                        "exception_info": {
                            "exception_type": "RuntimeError",
                            "exception_message": (
                                "Docker compose command failed: verifier service exited 1"
                            ),
                        },
                    }
                )
            )
            _, evidence = read_single_trial_evidence(job.resolve())

        self.assertEqual(
            evidence.infrastructure_failure_reason(),
            "PIER_DOCKER_INFRASTRUCTURE_FAILURE",
        )

    def test_rejects_multiple_trial_results_for_one_solve(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            job = Path(directory) / "job"
            for name in ("trial-1", "trial-2"):
                path = job / name
                path.mkdir(parents=True)
                (path / "result.json").write_text("{}")
            with self.assertRaisesRegex(PierResultError, "恰有一个"):
                read_single_trial_evidence(job.resolve())

    def test_rejects_missing_or_invalid_primary_reward(self) -> None:
        cases = ({}, {"reward": True}, {"reward": 0.5}, {"reward": 2})
        for rewards in cases:
            with self.subTest(rewards=rewards), tempfile.TemporaryDirectory() as directory:
                job = Path(directory) / "job"
                trial = job / "trial-1"
                trial.mkdir(parents=True)
                (trial / "result.json").write_text(
                    json.dumps(
                        {
                            "task_name": "task",
                            "trial_name": "trial-1",
                            "trial_uri": "file:///trial-1",
                            "task_checksum": "checksum",
                            "config": {},
                            "agent_info": {},
                            "verifier_result": {"rewards": rewards},
                        }
                    )
                )
                with self.assertRaisesRegex(PierResultError, "reward"):
                    read_single_trial_evidence(job.resolve())

    def test_accepts_zero_and_float_one_primary_reward(self) -> None:
        for reward, passed in ((0, False), (1.0, True)):
            with self.subTest(reward=reward), tempfile.TemporaryDirectory() as directory:
                job = Path(directory) / "job"
                trial = job / "trial-1"
                trial.mkdir(parents=True)
                (trial / "result.json").write_text(
                    json.dumps(
                        {
                            "task_name": "task",
                            "trial_name": "trial-1",
                            "trial_uri": "file:///trial-1",
                            "task_checksum": "checksum",
                            "config": {},
                            "agent_info": {},
                            "verifier_result": {"rewards": {"reward": reward}},
                        }
                    )
                )
                _, evidence = read_single_trial_evidence(job.resolve())
                self.assertEqual(evidence.verifier_for("attempt-1").passed, passed)
