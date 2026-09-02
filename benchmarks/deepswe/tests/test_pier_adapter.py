import hashlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from acn_deepswe.assets import frozen_coding_benchmark_skill
from acn_deepswe.pier_adapter import (
    CONTAINER_MODEL_EGRESS_ENV,
    CONTAINER_MODEL_KEY_FILE,
    CONTAINER_MODEL_PROXY_ENV,
    AcnEvalPierAgent,
    AcnPatchReplayPierAgent,
    AcnPierAdapter,
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
model_egress_mode = "{model_egress_mode}"
"""


def _adapter() -> AcnPierAdapter:
    return AcnPierAdapter(UPSTREAM, HOST_KEY_ENV, CONTAINER_KEY_ENV)


def _artifacts(
    root: Path, variant: str, model_egress_mode: str = "pier"
) -> tuple[Path, Path, Path, Path, Path]:
    acn_eval = root / "acn_eval"
    acn_config = root / "acn.toml"
    claim_bundle = root / "claims.json"
    for path in (acn_eval, acn_config, claim_bundle):
        path.write_text("artifact")
    attempt = root / "attempt.toml"
    body = ATTEMPT_TEMPLATE.format(variant=variant, model_egress_mode=model_egress_mode)
    if variant in {"B_claim", "B_forced_claim"}:
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

    def test_direct_model_proxy_is_opt_in_and_overrides_all_http_proxy_spellings(self) -> None:
        with patch.dict(
            os.environ,
            {CONTAINER_MODEL_PROXY_ENV: "http://host.docker.internal:7890"},
            clear=False,
        ):
            env = _adapter().direct_model_proxy_env()

        self.assertEqual(
            env,
            {
                "HTTP_PROXY": "http://host.docker.internal:7890",
                "HTTPS_PROXY": "http://host.docker.internal:7890",
                "http_proxy": "http://host.docker.internal:7890",
                "https_proxy": "http://host.docker.internal:7890",
            },
        )

    def test_direct_model_proxy_rejects_credentials_and_non_http_urls(self) -> None:
        for proxy_url in (
            "https://proxy.example:443",
            "http://user:password@proxy.example:8080",
            "http://proxy.example",
            "http://proxy.example:0",
            "http://proxy.example:8080/path",
        ):
            with self.subTest(proxy_url=proxy_url), patch.dict(
                os.environ, {CONTAINER_MODEL_PROXY_ENV: proxy_url}, clear=False
            ):
                with self.assertRaisesRegex(ValueError, CONTAINER_MODEL_PROXY_ENV):
                    _adapter().direct_model_proxy_env()

    def test_direct_egress_is_selected_only_from_the_frozen_attempt_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            attempt = _artifacts(Path(directory), "A", "direct")[1]
            env = _adapter().model_egress_env(attempt)

        self.assertEqual(
            env,
            {
                "HTTP_PROXY": "",
                "HTTPS_PROXY": "",
                "http_proxy": "",
                "https_proxy": "",
                "NO_PROXY": "*",
                "no_proxy": "*",
            },
        )

    def test_environment_egress_override_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            attempt = _artifacts(Path(directory), "A")[1]
        with patch.dict(
            os.environ,
            {
                CONTAINER_MODEL_EGRESS_ENV: "direct",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "不允许覆盖"):
                _adapter().model_egress_env(attempt)

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

    def test_claim_variants_upload_the_frozen_claim_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for variant, expects_bundle in (
                ("A", False),
                ("B_empty", False),
                ("B_claim", True),
                ("B_forced_claim", True),
            ):
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
                ATTEMPT_TEMPLATE.format(variant="A", model_egress_mode="pier")
                + 'claim_bundle = "/opt/acn-eval/claims.json"\n'
            )
            with self.assertRaisesRegex(ValueError, "不得设置 claim_bundle"):
                _adapter().build_setup_plan(acn_eval, attempt, acn_config, skill, bundle)

            attempt.write_text(ATTEMPT_TEMPLATE.format(variant="C", model_egress_mode="pier"))
            with self.assertRaisesRegex(ValueError, "variant"):
                _adapter().build_setup_plan(acn_eval, attempt, acn_config, skill, bundle)


class AcnEvalPierAgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_uses_proxy_only_and_reads_key_inside_the_container(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            agent = object.__new__(AcnEvalPierAgent)
            agent.adapter = _adapter()
            agent.attempt_config = _artifacts(Path(directory), "A")[1]
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

    async def test_run_uses_direct_model_proxy_when_explicitly_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            agent = object.__new__(AcnEvalPierAgent)
            agent.adapter = _adapter()
            agent.attempt_config = _artifacts(Path(directory), "A", "direct")[1]
            environment = SimpleNamespace(
                agent_process_env=Mock(side_effect=lambda env: env),
                exec=AsyncMock(return_value=SimpleNamespace(return_code=0)),
            )
            context = SimpleNamespace(metadata=None)
            with patch.dict(
                os.environ,
                {CONTAINER_MODEL_PROXY_ENV: "http://host.docker.internal:7890"},
                clear=False,
            ):
                await agent.run("ignored", environment, context)

        first_env = environment.exec.await_args_list[0].kwargs["env"]
        self.assertEqual(first_env["HTTPS_PROXY"], "http://host.docker.internal:7890")
        self.assertEqual(first_env["https_proxy"], "http://host.docker.internal:7890")
        self.assertEqual(environment.agent_process_env.call_count, 2)

    async def test_run_uses_direct_egress_only_for_agent_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            agent = object.__new__(AcnEvalPierAgent)
            agent.adapter = _adapter()
            agent.attempt_config = _artifacts(Path(directory), "A", "direct")[1]
            environment = SimpleNamespace(
                agent_process_env=Mock(side_effect=lambda env: env),
                exec=AsyncMock(return_value=SimpleNamespace(return_code=0)),
            )
            context = SimpleNamespace(metadata=None)
            await agent.run("ignored", environment, context)

        first_env = environment.exec.await_args_list[0].kwargs["env"]
        second_env = environment.exec.await_args_list[1].kwargs["env"]
        self.assertEqual(first_env["HTTPS_PROXY"], "")
        self.assertEqual(first_env["NO_PROXY"], "*")
        self.assertEqual(second_env, {})

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
                acn_version="0.2.5",
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

    async def test_patch_replay_agent_uploads_and_applies_only_the_frozen_patch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frozen_patch = root / "model.patch"
            frozen_patch.write_text("diff --git a/a b/a\n", encoding="utf-8")
            digest = hashlib.sha256(frozen_patch.read_bytes()).hexdigest()
            agent = AcnPatchReplayPierAgent(
                logs_dir=root / "logs",
                patch_path=str(frozen_patch),
                patch_sha256=digest,
            )
            environment = SimpleNamespace(
                exec=AsyncMock(return_value=SimpleNamespace(return_code=0)),
                upload_file=AsyncMock(),
            )
            context = SimpleNamespace(metadata=None)

            await agent.setup(environment)
            await agent.run("ignored", environment, context)

        environment.upload_file.assert_awaited_once_with(
            frozen_patch, "/opt/acn-eval/model.patch"
        )
        replay_command = environment.exec.await_args_list[-1].args[0]
        self.assertIn("git apply --check /opt/acn-eval/model.patch", replay_command)
        self.assertIn("git apply /opt/acn-eval/model.patch", replay_command)
        self.assertIn("git add -A", replay_command)
        self.assertNotIn("git apply --index", replay_command)
        self.assertEqual(context.metadata["patch_sha256"], digest)
        self.assertEqual(agent.version(), "1.0.1")
