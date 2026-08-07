"""Pier `BaseAgent` 适配：上传 ACN 产物、声明出网域名、执行单个 attempt。

模型 key 只经一次性受限文件进入容器，出网由宿主的域名 allowlist 限死，
适配层不自建代理。
"""

from __future__ import annotations

import os
import re
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .assets import frozen_coding_benchmark_skill

try:
    from pier.agents.base import BaseAgent
    from pier.models.agent.network import NetworkAllowlist
except ModuleNotFoundError:

    class BaseAgent:
        """未安装 optional runtime 时，仅让纯逻辑模块保持可导入。"""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

    NetworkAllowlist = None

if TYPE_CHECKING:
    from pier.environments.base import BaseEnvironment
    from pier.models.agent.context import AgentContext

CONTAINER_ROOT = "/opt/acn-eval"
CONTAINER_ATTEMPT_CONFIG = f"{CONTAINER_ROOT}/attempt.toml"
CONTAINER_ACN_CONFIG = f"{CONTAINER_ROOT}/acn.toml"
CONTAINER_CLAIM_BUNDLE = f"{CONTAINER_ROOT}/claims.json"
CONTAINER_MODEL_KEY_FILE = f"{CONTAINER_ROOT}/model-key"
CONTAINER_SKILL_PATH = "/logs/agent/runtime/skills/coding-benchmark"


@dataclass(frozen=True)
class Upload:
    local_path: Path
    remote_path: str


@dataclass(frozen=True)
class SetupPlan:
    uploads: tuple[Upload, ...]
    frozen_skill_hash: str


def upstream_host(base_url: str) -> str:
    """从上游 base URL 取出 Squid allowlist 需要的裸主机名。"""
    match = re.fullmatch(r"https?://([^/:?#]+)(?::\d+)?(?:/.*)?", base_url.strip())
    if not match:
        raise ValueError(f"上游 base URL 无法解析出主机名: {base_url}")
    return match.group(1).lower()


class AcnPierAdapter:
    """可独立测试的宿主适配逻辑：上传清单、出网域名与容器执行命令。"""

    def __init__(
        self,
        upstream_base_url: str,
        host_model_key_env: str,
        container_model_key_env: str,
    ) -> None:
        if not host_model_key_env.strip() or not container_model_key_env.strip():
            raise ValueError("host_model_key_env 和 container_model_key_env 不能为空")
        self.upstream_host = upstream_host(upstream_base_url)
        self.host_model_key_env = host_model_key_env
        self.container_model_key_env = container_model_key_env

    def network_allowlist(self) -> tuple[str, ...]:
        """agent 容器只允许访问上游模型域名，其余出网由 Pier 的 Squid 拒绝。"""
        return (self.upstream_host,)

    def build_setup_plan(
        self,
        acn_eval: Path,
        attempt_config: Path,
        acn_config: Path,
        frozen_skill: Path,
        claim_bundle: Path,
    ) -> SetupPlan:
        for artifact in (acn_eval, attempt_config, acn_config, frozen_skill):
            if not artifact.is_absolute() or not artifact.exists():
                raise ValueError(f"预构建上传物必须存在且为绝对路径: {artifact}")
        variant = self._validate_container_attempt_config(attempt_config)
        if variant == "B_claim" and (not claim_bundle.is_absolute() or not claim_bundle.exists()):
            raise ValueError("B_claim 的冻结 claim bundle 必须存在且为绝对路径")
        frozen_asset = frozen_coding_benchmark_skill()
        if frozen_skill.resolve() != frozen_asset.source_path.resolve():
            raise ValueError("三臂只能注入冻结的 coding-benchmark skill")
        uploads = [
            Upload(acn_eval, f"{CONTAINER_ROOT}/acn_eval"),
            Upload(attempt_config, CONTAINER_ATTEMPT_CONFIG),
            Upload(acn_config, CONTAINER_ACN_CONFIG),
            Upload(frozen_skill, CONTAINER_SKILL_PATH),
        ]
        if variant == "B_claim":
            uploads.append(Upload(claim_bundle, CONTAINER_CLAIM_BUNDLE))
        return SetupPlan(tuple(uploads), frozen_asset.content_hash)

    def read_model_key(self) -> str:
        """读取仅供本次容器 setup 使用的模型 key，拒绝不能安全写入文件的值。"""
        key = os.environ.get(self.host_model_key_env)
        if not key:
            raise ValueError(f"宿主环境缺少模型 key: {self.host_model_key_env}")
        if any(char in key for char in ("\x00", "\r", "\n")):
            raise ValueError("模型 key 不能包含空字节或换行")
        return key

    def container_process_env(self, proxy_env: dict[str, str] | None) -> dict[str, str]:
        """仅保留 Pier 代理变量；模型 key 不得进入 docker compose 的 argv。"""
        environment = dict(proxy_env or {})
        environment.pop(self.host_model_key_env, None)
        environment.pop(self.container_model_key_env, None)
        return environment

    def build_run_command(self) -> str:
        return (
            "set -eu; "
            f"export {self.container_model_key_env}=\"$(cat {CONTAINER_MODEL_KEY_FILE})\"; "
            f"rm -f {CONTAINER_MODEL_KEY_FILE}; "
            f"cd /app && exec {CONTAINER_ROOT}/acn_eval --config {CONTAINER_ATTEMPT_CONFIG}"
            " > /logs/agent/acn_eval.stdout 2> /logs/agent/acn_eval.stderr"
        )

    def build_commit_command(self, task_id: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9._-]+", task_id):
            raise ValueError("task_id 仅允许字母、数字、点、下划线和连字符")
        return (
            "git add -A && "
            "git -c user.name=acn-eval -c user.email=eval@invalid "
            f"commit -m 'acn DeepSWE {task_id}' || true"
        )

    def container_post_run_env(self, proxy_env: dict[str, str] | None) -> dict[str, str]:
        """提交阶段复用代理配置，但绝不把模型 key 传给第二个容器进程。"""
        env = dict(proxy_env or {})
        env.pop(self.host_model_key_env, None)
        env.pop(self.container_model_key_env, None)
        return env

    @staticmethod
    def _validate_container_attempt_config(path: Path) -> str:
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"无法解析 attempt config: {path}") from error
        required = {
            "workspace_root": "/app",
            "runtime_root": "/logs/agent/runtime",
            "output_dir": "/logs/agent/evaluation",
            "acn_config": CONTAINER_ACN_CONFIG,
        }
        for field, expected in required.items():
            if raw.get(field) != expected:
                raise ValueError(f"attempt config {field} 必须为 {expected}")
        variant = raw.get("variant")
        bundle = raw.get("claim_bundle")
        if variant == "B_claim" and bundle != CONTAINER_CLAIM_BUNDLE:
            raise ValueError(f"B_claim 的 claim_bundle 必须为 {CONTAINER_CLAIM_BUNDLE}")
        if variant in {"A", "B_empty"} and bundle is not None:
            raise ValueError(f"{variant} 不得设置 claim_bundle")
        if variant not in {"A", "B_empty", "B_claim"}:
            raise ValueError("attempt config variant 必须为 A/B_empty/B_claim")
        return variant


class AcnEvalPierAgent(BaseAgent):
    """由 Pier 以 `acn_deepswe.pier_adapter:AcnEvalPierAgent` 加载的真实 BaseAgent。"""

    def __init__(
        self,
        *args: object,
        acn_eval: str,
        attempt_config: str,
        acn_config: str,
        frozen_skill: str,
        claim_bundle: str,
        upstream_base_url: str,
        host_model_key_env: str,
        container_model_key_env: str,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.acn_eval = Path(acn_eval)
        self.attempt_config = Path(attempt_config)
        self.acn_config = Path(acn_config)
        self.frozen_skill = Path(frozen_skill)
        self.claim_bundle = Path(claim_bundle)
        self.adapter = AcnPierAdapter(
            upstream_base_url,
            host_model_key_env,
            container_model_key_env,
        )

    @staticmethod
    def name() -> str:
        return "acn_eval"

    @classmethod
    def import_path(cls) -> str:
        return f"{cls.__module__}:{cls.__name__}"

    def version(self) -> str:
        return "0.2.0"

    def network_allowlist(self) -> object:
        domains = list(self.adapter.network_allowlist())
        if NetworkAllowlist is None:
            return tuple(domains)
        return NetworkAllowlist(domains=domains)

    async def setup(self, environment: BaseEnvironment) -> None:
        model_key = self.adapter.read_model_key()
        created = await environment.exec(
            f"mkdir -p {CONTAINER_ROOT} /logs/agent/runtime/skills /logs/agent/evaluation",
            user="root",
            timeout_sec=30,
        )
        if created.return_code != 0:
            raise RuntimeError("Pier setup 无法创建 ACN 运行目录")
        setup = self.adapter.build_setup_plan(
            self.acn_eval,
            self.attempt_config,
            self.acn_config,
            self.frozen_skill,
            self.claim_bundle,
        )
        for upload in setup.uploads:
            if upload.local_path.is_dir():
                await environment.upload_dir(upload.local_path, upload.remote_path)
            else:
                await environment.upload_file(upload.local_path, upload.remote_path)
        key_path = _write_ephemeral_model_key(model_key)
        try:
            await environment.upload_file(key_path, CONTAINER_MODEL_KEY_FILE)
        finally:
            key_path.unlink(missing_ok=True)
        permissions = await environment.exec(
            f"chmod 0755 {CONTAINER_ROOT}/acn_eval && chmod 0600 {CONTAINER_MODEL_KEY_FILE}",
            user="root",
            timeout_sec=30,
        )
        if permissions.return_code != 0:
            raise RuntimeError("Pier setup 无法设置 acn_eval 可执行权限")

    async def run(
        self, instruction: str, environment: BaseEnvironment, context: AgentContext
    ) -> None:
        del instruction  # attempt TOML 是唯一任务输入，避免把任务文本另行放入 argv。
        result = await environment.exec(
            self.adapter.build_run_command(),
            cwd="/app",
            env=self.adapter.container_process_env(environment.agent_process_env(None)),
        )
        await environment.exec(
            self.adapter.build_commit_command("attempt"),
            cwd="/app",
            env=self.adapter.container_post_run_env(environment.agent_process_env(None)),
        )
        context.metadata = {"acn_eval_exit_code": result.return_code}


def _write_ephemeral_model_key(key: str) -> Path:
    """写入仅供 Pier upload_file 消费的 0600 临时文件，调用者必须立即删除。"""
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="acn-eval-model-key-",
        delete=False,
    ) as handle:
        handle.write(key)
        handle.write("\n")
        return Path(handle.name)
