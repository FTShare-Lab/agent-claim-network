import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from acn_deepswe.assets import frozen_coding_benchmark_skill
from acn_deepswe.pier_adapter import (
    AcnEvalPierAgent,
    AcnPierAdapter,
    CONTAINER_MODEL_KEY_FILE,
    upstream_host,
)

UPSTREAM = "https://llm-proxy.example.test"
HOST_KEY_ENV = "ACN_EVAL_TEST_KEY"
CONTAINER_KEY_ENV = "ACN_EVAL_MODEL_KEY"

ATTEMPT_TEMPLATE = """\
schema_version = 1
attempt_id = "task-1"
task_prompt = "solve"
workspace_root = "/app"
runtime_root = "/logs/agent/runtime"
acn_config = "/opt/acn-eval/acn.toml"
output_dir = "/logs/agent/evaluation"
upstream = "eval"
variant = "{variant}"
"""


def _adapter() -> AcnPierAdapter:
    return AcnPierAdapter(UPSTREAM, HOST_KEY_ENV, CONTAINER_KEY_ENV)


def _artifacts(root: Path, variant: str) -> tuple[Path, Path, Path, Path, Path]:
    acn_eval = root / "acn_eval"
    acn_config = root / "acn.toml"
    claim_bundle = root / "claims.json"
    for path in (acn_eval, acn_config, claim_bundle):
        path.write_text("artifact")
    attempt = root / "attempt.toml"
    body = ATTEMPT_TEMPLATE.format(variant=variant)
    if variant == "B_claim":
        body += 'claim_bundle = "/opt/acn-eval/claims.json"\n'
    attempt.write_text(body)
    return acn_eval, attempt, acn_config, frozen_coding_benchmark_skill().source_path, claim_bundle


class UpstreamHostTests(unittest.TestCase):
    def test_extracts_bare_host_for_squid_allowlist(self) -> None:
        self.assertEqual(upstream_host("https://model-gateway.example"), "model-gateway.example")
        self.assertEqual(upstream_host("https://Model-Gateway.example/v1/"), "model-gateway.example")
        self.assertEqual(upstream_host("http://gateway.example:8080/v1"), "gateway.example")

    def test_rejects_unparseable_base_url(self) -> None:
        for value in ("", "model-gateway.example", "ftp://host/v1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                upstream_host(value)


class AcnPierAdapterTests(unittest.TestCase):
    def test_allowlist_contains_only_the_upstream_model_host(self) -> None:
        self.assertEqual(_adapter().network_allowlist(), ("llm-proxy.example.test",))

    def test_run_command_reads_the_ephemeral_key_file_without_a_credential(self) -> None:
        command = _adapter().build_run_command()

        self.assertIn("--config /opt/acn-eval/attempt.toml", command)
        self.assertIn("exec /opt/acn-eval/acn_eval", command)
        self.assertIn(CONTAINER_MODEL_KEY_FILE, command)
        self.assertIn(CONTAINER_KEY_ENV, command)
        self.assertNotIn("--capability-file", command)
        self.assertNotIn(HOST_KEY_ENV, command)
        self.assertNotIn("git add", command)
        # 任务文本与 key 值都不进 argv；agent 结果通过 exit code 传回。
        self.assertNotIn("exit_code", command)

    def test_commit_command_has_no_credential(self) -> None:
        command = _adapter().build_commit_command("attempt")

        self.assertIn("git add -A", command)
        self.assertNotIn(HOST_KEY_ENV, command)
        self.assertNotIn(CONTAINER_KEY_ENV, command)

    def test_commit_command_rejects_unsafe_task_id(self) -> None:
        with self.assertRaises(ValueError):
            _adapter().build_commit_command("task; rm -rf /")

    def test_container_env_strips_model_key_from_squid_proxy_environment(self) -> None:
        proxy = {
            "HTTP_PROXY": "http://agent:token@pier-egress-proxy:8080",
            HOST_KEY_ENV: "must-not-reach-container",
            CONTAINER_KEY_ENV: "must-not-override-host-key",
        }
        env = _adapter().container_process_env(proxy)

        self.assertEqual(env["HTTP_PROXY"], proxy["HTTP_PROXY"])
        self.assertNotIn(HOST_KEY_ENV, env)
        self.assertNotIn(CONTAINER_KEY_ENV, env)

    def test_read_model_key_fails_closed_without_host_key(self) -> None:
        os.environ.pop(HOST_KEY_ENV, None)
        with self.assertRaisesRegex(ValueError, HOST_KEY_ENV):
            _adapter().read_model_key()

    def test_read_model_key_rejects_line_breaks(self) -> None:
        os.environ[HOST_KEY_ENV] = "invalid\nvalue"
        try:
            with self.assertRaisesRegex(ValueError, "换行"):
                _adapter().read_model_key()
        finally:
            os.environ.pop(HOST_KEY_ENV, None)

    def test_only_b_claim_uploads_the_frozen_claim_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for variant, expects_bundle in (("A", False), ("B_empty", False), ("B_claim", True)):
                with self.subTest(variant=variant):
                    plan = _adapter().build_setup_plan(*_artifacts(root, variant))
                    remotes = [item.remote_path for item in plan.uploads]
                    self.assertEqual("/opt/acn-eval/claims.json" in remotes, expects_bundle)
                    self.assertIn("/opt/acn-eval/acn_eval", remotes)
                    self.assertIn("/logs/agent/runtime/skills/coding-benchmark", remotes)
                    self.assertEqual(
                        plan.frozen_skill_hash,
                        frozen_coding_benchmark_skill().content_hash,
                    )

    def test_attempt_config_variant_and_bundle_must_agree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            acn_eval, attempt, acn_config, skill, bundle = _artifacts(root, "A")
            attempt.write_text(
                ATTEMPT_TEMPLATE.format(variant="A")
                + 'claim_bundle = "/opt/acn-eval/claims.json"\n'
            )
            with self.assertRaisesRegex(ValueError, "不得设置 claim_bundle"):
                _adapter().build_setup_plan(acn_eval, attempt, acn_config, skill, bundle)

            attempt.write_text(ATTEMPT_TEMPLATE.format(variant="C"))
            with self.assertRaisesRegex(ValueError, "variant"):
                _adapter().build_setup_plan(acn_eval, attempt, acn_config, skill, bundle)


class AcnEvalPierAgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_uses_proxy_only_and_reads_key_inside_the_container(self) -> None:
        agent = object.__new__(AcnEvalPierAgent)
        agent.adapter = _adapter()
        proxy_environment = {
            "HTTP_PROXY": "http://agent:test@pier-egress-proxy:8080",
            "HTTPS_PROXY": "http://agent:test@pier-egress-proxy:8080",
        }
        environment = SimpleNamespace(
            agent_process_env=Mock(return_value=proxy_environment),
            exec=AsyncMock(return_value=SimpleNamespace(return_code=0)),
        )
        context = SimpleNamespace(metadata=None)
        await agent.run("ignored", environment, context)

        self.assertEqual(environment.exec.await_count, 2)
        first_call, second_call = environment.exec.await_args_list
        first_command = first_call.args[0]
        second_command = second_call.args[0]
        first_env = first_call.kwargs["env"]
        second_env = second_call.kwargs["env"]
        self.assertIn("exec /opt/acn-eval/acn_eval", first_command)
        self.assertIn(CONTAINER_MODEL_KEY_FILE, first_command)
        self.assertNotIn("git add", first_command)
        self.assertIn("git add -A", second_command)
        self.assertEqual(first_env["HTTPS_PROXY"], proxy_environment["HTTPS_PROXY"])
        self.assertNotIn(HOST_KEY_ENV, first_env)
        self.assertNotIn(CONTAINER_KEY_ENV, first_env)
        self.assertEqual(second_env["HTTPS_PROXY"], proxy_environment["HTTPS_PROXY"])
        self.assertNotIn(HOST_KEY_ENV, second_env)
        self.assertNotIn(CONTAINER_KEY_ENV, second_env)
        self.assertEqual(context.metadata, {"acn_eval_exit_code": 0})

    async def test_setup_uploads_a_short_lived_model_key_file(self) -> None:
        uploaded_key: dict[str, object] = {}

        async def record_upload(source: Path, target: str) -> None:
            if target == CONTAINER_MODEL_KEY_FILE:
                uploaded_key["contents"] = source.read_text(encoding="utf-8")
                uploaded_key["mode"] = stat.S_IMODE(source.stat().st_mode)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            acn_eval, attempt, acn_config, skill, claim_bundle = _artifacts(root, "A")
            agent = AcnEvalPierAgent(
                logs_dir=root / "logs",
                acn_eval=str(acn_eval),
                attempt_config=str(attempt),
                acn_config=str(acn_config),
                frozen_skill=str(skill),
                claim_bundle=str(claim_bundle),
                upstream_base_url=UPSTREAM,
                host_model_key_env=HOST_KEY_ENV,
                container_model_key_env=CONTAINER_KEY_ENV,
            )
            environment = SimpleNamespace(
                exec=AsyncMock(return_value=SimpleNamespace(return_code=0)),
                upload_file=AsyncMock(side_effect=record_upload),
                upload_dir=AsyncMock(),
            )
            os.environ[HOST_KEY_ENV] = "upstream-secret"
            try:
                await agent.setup(environment)
            finally:
                os.environ.pop(HOST_KEY_ENV, None)

        key_upload = next(
            call for call in environment.upload_file.await_args_list
            if call.args[1] == CONTAINER_MODEL_KEY_FILE
        )
        self.assertTrue(key_upload.args[0].name.startswith("acn-eval-model-key-"))
        self.assertFalse(key_upload.args[0].exists())
        self.assertEqual(uploaded_key, {"contents": "upstream-secret\n", "mode": 0o600})
        self.assertIn(CONTAINER_MODEL_KEY_FILE, environment.exec.await_args_list[-1].args[0])

    async def test_import_path_is_stable_for_pier_job_configs(self) -> None:
        self.assertEqual(
            AcnEvalPierAgent.import_path(), "acn_deepswe.pier_adapter:AcnEvalPierAgent"
        )
