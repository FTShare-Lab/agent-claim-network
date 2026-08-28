import json
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from acn_deepswe.resource_guard import (
    MIB,
    ResourceGuardError,
    cleanup_finished_trial_images,
    cleanup_stale_pier_resources,
    verify_capacity,
)


def completed(stdout: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], 0, stdout=stdout, stderr="")


class ResourceGuardTests(unittest.TestCase):
    def test_finished_trial_cleanup_removes_only_exact_derived_images(self) -> None:
        images = "\n".join(
            json.dumps({"Repository": repository, "Tag": "latest"})
            for repository in (
                "task__abc123d-pier-egress-proxy",
                "task__abc123d__verifier__trial-main",
                "task__abc123d-unrelated",
                "other__abc123d__verifier__trial-main",
            )
        )
        with patch(
            "acn_deepswe.resource_guard._docker",
            side_effect=[completed(images), completed()],
        ) as docker:
            removed = cleanup_finished_trial_images("task__AbC123D")

        self.assertEqual(removed, 2)
        self.assertEqual(
            docker.call_args_list[1].args[0],
            [
                "image",
                "rm",
                "task__abc123d-pier-egress-proxy:latest",
                "task__abc123d__verifier__trial-main:latest",
            ],
        )

    def test_finished_trial_cleanup_rejects_unbound_name(self) -> None:
        with self.assertRaisesRegex(ResourceGuardError, "trial_name"):
            cleanup_finished_trial_images("task")

    def test_capacity_uses_requested_worker_count_without_silent_reduction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            info = {
                "NCPU": 40,
                "MemTotal": 100 * MIB,
                "DockerRootDir": str(root),
            }
            with (
                patch("acn_deepswe.resource_guard._host_available_memory_bytes", return_value=100 * MIB),
                patch("acn_deepswe.resource_guard.verify_disk_headroom") as disk,
            ):
                docker_root = verify_capacity(
                    info,
                    workers=20,
                    resources={"cpus": 2, "memory_mb": 4},
                    host_capacity={
                        "memory_reserve_mb": 20,
                        "disk_reserve_mb": 10,
                        "disk_admission_mb_per_worker": 3,
                    },
                    output_path=root / "output",
                )
            self.assertEqual(docker_root, root)
            disk.assert_called_once_with((root / "output", root), 70)

            with (
                patch("acn_deepswe.resource_guard._host_available_memory_bytes", return_value=99 * MIB),
                self.assertRaisesRegex(ResourceGuardError, "拒绝静默降低 task_workers"),
            ):
                verify_capacity(
                    info,
                    workers=20,
                    resources={"cpus": 2, "memory_mb": 4},
                    host_capacity={
                        "memory_reserve_mb": 20,
                        "disk_reserve_mb": 10,
                        "disk_admission_mb_per_worker": 3,
                    },
                    output_path=root / "output",
                )

    def test_cleanup_is_limited_to_stopped_pier_resources_and_generated_images(self) -> None:
        containers = [
            {
                "Id": "owned",
                "State": {"Running": False},
                "Config": {
                    "Image": "task__abcdef0-main:latest",
                    "Labels": {
                        "com.docker.compose.project.config_files": "/tmp/frozen/pier/task.yaml"
                    }
                },
            },
            {
                "Id": "foreign",
                "State": {"Running": False},
                "Config": {
                    "Image": "foreign__abcdef1-main:latest",
                    "Labels": {
                        "com.docker.compose.project.config_files": "/tmp/other/task.yaml"
                    }
                },
            },
        ]
        image_lines = "\n".join(
            json.dumps({"Repository": repository, "Tag": "latest"})
            for repository in (
                "hb__task",
                "task__verifier__trial-main",
                "task__abcdef0-main",
                "foreign__abcdef1-main",
                "public.ecr.aws/example/task",
                "unrelated/image",
            )
        )
        with patch(
            "acn_deepswe.resource_guard._docker",
            side_effect=[
                completed(),
                completed("owned\nforeign\n"),
                completed(json.dumps(containers)),
                completed(),
                completed(image_lines),
                completed(),
            ],
        ) as docker:
            summary = cleanup_stale_pier_resources()

        self.assertEqual(summary.containers_removed, 1)
        self.assertEqual(summary.image_references_removed, 2)
        self.assertEqual(docker.call_args_list[3].args[0], ["rm", "owned"])
        removed_images = docker.call_args_list[5].args[0]
        self.assertNotIn("hb__task:latest", removed_images)
        self.assertNotIn("public.ecr.aws/example/task:latest", removed_images)
        self.assertNotIn("unrelated/image:latest", removed_images)
        self.assertNotIn("foreign__abcdef1-main:latest", removed_images)


if __name__ == "__main__":
    unittest.main()
