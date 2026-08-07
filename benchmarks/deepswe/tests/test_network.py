import hashlib
import json
import unittest
from pathlib import Path

from acn_deepswe.network import (
    NetworkNormalizationError,
    normalize_task_toml,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
FROZEN_MANIFEST = REPO_ROOT / "benchmarks/deepswe/manifests/presmoke-v1.json"
DEEPSWE_TASKS = Path("/private/tmp/acn-eval-deep-swe/tasks")
FROZEN_NORMALIZED = REPO_ROOT / "target/deepswe-runs/presmoke-20260726/normalized"

OFFLINE_TASK = """\
schema_version = "1.3"
artifacts = ["/logs/artifacts/model.patch"]

[verifier]
network_mode = "no-network"
timeout_sec = 1800.0

[verifier.environment]
cpus = 2

[agent]
network_mode = "no-network"

[environment]
docker_image = "example:v1"
gpus = 0
mcp_servers = []
"""


class NetworkNormalizationTests(unittest.TestCase):
    def test_offline_task_gets_allow_internet_false_on_both_environments(self) -> None:
        rendered = normalize_task_toml(OFFLINE_TASK)

        self.assertIn("[environment]\n", rendered)
        self.assertEqual(rendered.count("allow_internet = false"), 2)
        # agent 与 verifier 两个环境都必须显式关网，缺一不可。
        environment_block = rendered.split("[environment]\n", 1)[1]
        verifier_block = rendered.split("[verifier.environment]\n", 1)[1].split("\n[", 1)[0]
        self.assertIn("allow_internet = false", environment_block)
        self.assertIn("allow_internet = false", verifier_block)

    def test_any_non_offline_network_mode_fails_closed(self) -> None:
        for original, replacement in (
            ('[agent]\nnetwork_mode = "no-network"', '[agent]\nnetwork_mode = "full"'),
            ('[verifier]\nnetwork_mode = "no-network"', '[verifier]\nnetwork_mode = "proxy"'),
        ):
            with (
                self.subTest(replacement=replacement),
                self.assertRaises(NetworkNormalizationError),
            ):
                normalize_task_toml(OFFLINE_TASK.replace(original, replacement))

    def test_missing_section_or_invalid_toml_fails_closed(self) -> None:
        with self.assertRaises(NetworkNormalizationError):
            normalize_task_toml('[agent]\nnetwork_mode = "no-network"\n')
        with self.assertRaises(NetworkNormalizationError):
            normalize_task_toml("not = [toml")

    @unittest.skipUnless(
        DEEPSWE_TASKS.is_dir() and FROZEN_NORMALIZED.is_dir(),
        "冻结 DeepSWE checkout 或 normalized 产物不可用",
    )
    def test_reproduces_every_frozen_normalized_task_byte_for_byte(self) -> None:
        frozen = json.loads(FROZEN_MANIFEST.read_text(encoding="utf-8"))
        for task_id, expected in frozen["task_toml_hashes"].items():
            with self.subTest(task_id=task_id):
                source = (DEEPSWE_TASKS / task_id / "task.toml").read_bytes()
                rendered = normalize_task_toml(source.decode("utf-8")).encode("utf-8")
                self.assertEqual(hashlib.sha256(source).hexdigest(), expected["source"])
                self.assertEqual(hashlib.sha256(rendered).hexdigest(), expected["normalized"])
                self.assertEqual(rendered, (FROZEN_NORMALIZED / task_id / "task.toml").read_bytes())
