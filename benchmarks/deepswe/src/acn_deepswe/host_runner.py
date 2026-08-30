"""单个 task 的四臂宿主编排：A → freeze barrier → B_empty / B_claim / B_forced_claim。

每个 attempt 生成一份 Pier job 配置并调用 pinned `pier run`，随后收集 Pier 判卷结果
与 ACN 自己的 result.json / events.jsonl，交给 Gate 做机器可判定检查。
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import subprocess
import threading
import time
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from .claim_freeze import append_freeze_barrier, freeze_claim_bundle
from .gate import AttemptGateInput, GateValidator
from .pier_adapter import AcnEvalPierAgent, AcnPatchReplayPierAgent, upstream_host
from .pier_result import PierResultError, PierTrialEvidence, read_single_trial_evidence
from .provenance import EvaluationProvenance, sha256_directory_tree
from .resource_guard import (
    ResourceGuardError,
    cleanup_finished_trial_images,
    verify_disk_headroom,
)
from .runner import ExperimentManifest
from .rust_contract import read_rust_event_ledger, read_rust_result
from .schemas import AttemptManifest, RouterEvidence

HOST_MODEL_KEY_ENV = "ACN_EVAL_UPSTREAM_KEY"
CONTAINER_MODEL_KEY_ENV = "ACN_EVAL_MODEL_KEY"
# 与 ACN 的项目默认值保持一致；评测 provenance 会冻结该有效值。
EVALUATION_AUTO_COMPACT_CTX_RATIO = 0.80
EVALUATION_FILE_READ_MAX_CHARS = 20_000
EVALUATION_CODE_RUN_MAX_OUTPUT_CHARS = 20_000
# 评测原先钉死 parallel=1 / diff=20，单次修补和同轮读文件都偏碎。
# parallel 回到 ACN 默认 5；diff 提到 200，让一处函数级改动能一次 file_patch 落地。
# code_run yield 是产品内部护栏，评测不覆盖。
EVALUATION_FILE_DIFF_MAX_CHANGED_LINES = 200
EVALUATION_MAX_PARALLEL_TOOL_CALLS = 5
# 与 MiniSWE Responses 对照请求对齐：temperature=1.0、top_p=0.95。
EVALUATION_TEMPERATURE = 1.0
EVALUATION_TOP_P = 0.95
CLAIM_BUNDLE_VARIANTS = frozenset(("B_claim", "B_forced_claim"))
DEFAULT_PROGRESS_POLL_SECS = 30
DEFAULT_PROGRESS_STALL_AFTER_SECS = 600
EVALUATION_HARNESS_MODES = frozenset(("standard", "minimal"))
MINIMAL_TASK_GUIDANCE = """

You can execute shell commands and edit files to implement the necessary changes.

## Recommended Workflow

Work step-by-step so you can iterate on the implementation and any failures:

1. Analyze the codebase by finding and reading relevant files.
2. Reproduce the issue or required behavior.
3. Edit the source code with the smallest complete change.
4. Verify the fix with targeted tests, then run the complete project test suite when feasible.
5. Check edge cases and inspect the final diff.
6. Submit by calling the no-argument `submit_task` tool. It must be the only tool call in that response; do not continue after it.

## Command Execution Rules

- Every response before submission must include reasoning text and at least one tool call that gathers evidence or advances the implementation.
- Use `code_run` for shell commands and file edits. Each call starts in a fresh shell, so include any required `cd` in the command.
- If a command returns a `process_id`, poll it with `write_stdin`; use `process_list` only to rediscover managed processes.
- Bound exploratory commands and malformed-input reproductions with a short timeout; this does not replace the complete project test suite.
- The container has a strict process/thread budget. Before a full test command that starts workers, inspect `/sys/fs/cgroup/pids.max` and use the test runner's worker option to stay below it. If you see `EAGAIN` or a thread/process creation failure, retry with fewer workers.
- Do not call `submit_task` while implementation, verification, or diff inspection remains.
""".strip()


@dataclass(frozen=True)
class AttemptFiles:
    attempt_toml: Path
    acn_config: Path


@dataclass(frozen=True)
class HostArtifacts:
    acn_eval: Path
    frozen_skill: Path
    claim_bundle: Path
    normalized_task_dir: Path


@dataclass(frozen=True)
class HostRunStep:
    phase: str
    attempt_id: str | None
    execute: bool


class TaskExecutionError(RuntimeError):
    """真实执行在第一个基础设施或 Gate 失败处停止。"""


@dataclass(frozen=True)
class Task1ExecutionConfig:
    """真实执行所需的宿主输入；所有路径均必须是绝对路径。"""

    artifacts: HostArtifacts
    task_prompt: str
    upstream_base_url: str
    manifest_path: Path
    pier_executable: Path
    expected_response_model: str
    frozen_acn_source_root: Path
    frozen_pier_source_root: Path
    model_egress_mode: str = "pier"
    harness_mode: str = "standard"
    task_workers: int = 1
    require_eligible_claim: bool = False
    run_all_variants_without_claims: bool = False
    run_a_only: bool = False
    a_only_source_manifest: Path | None = None
    progress_poll_secs: int = DEFAULT_PROGRESS_POLL_SECS
    progress_stall_after_secs: int = DEFAULT_PROGRESS_STALL_AFTER_SECS
    docker_root: Path | None = None
    disk_reserve_mb: int = 1
    disk_admission_mb: int = 1


@dataclass(frozen=True)
class AttemptExecutionRecord:
    attempt_id: str
    variant: str
    status: str
    reason: str
    result_path: str | None
    gate_path: str | None
    verifier_passed: bool | None = None
    claim_observation: dict[str, object] | None = None
    progress_path: str | None = None
    result_hash: str | None = None
    gate_hash: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "attempt_id": self.attempt_id,
            "variant": self.variant,
            "status": self.status,
            "reason": self.reason,
            "result_path": self.result_path,
            "gate_path": self.gate_path,
            "verifier_passed": self.verifier_passed,
            "claim_observation": self.claim_observation,
            "progress_path": self.progress_path,
            "result_hash": self.result_hash,
            "gate_hash": self.gate_hash,
        }


@dataclass(frozen=True)
class AOnlySourceEvidence:
    """B-only 阶段从已完成 A-only task 中验证并绑定的不可变证据。"""

    a_record: AttemptExecutionRecord
    cohort: str
    claim_ids: tuple[str, ...]
    claim_bundle_hash: str
    pier_task_checksum: str
    source_manifest_hash: str


@dataclass(frozen=True)
class TaskExecutionResult:
    status: str


class AttemptProgressMonitor:
    """观测 Pier 运行中 session 事件；仅写状态，不干预 agent 生命周期。"""

    def __init__(
        self,
        attempt: AttemptManifest,
        attempt_dir: Path,
        *,
        poll_secs: int,
        stall_after_secs: int,
    ) -> None:
        self.attempt = attempt
        self.attempt_dir = attempt_dir
        self.poll_secs = poll_secs
        self.stall_after_secs = stall_after_secs
        self.progress_path = attempt_dir / "progress.json"
        self._started_wall = datetime.now(UTC)
        self._started_monotonic = time.monotonic()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._write_snapshot()
        self._thread = threading.Thread(
            target=self._observe,
            name=f"acn-eval-progress-{self.attempt.attempt_id[:12]}",
            daemon=True,
        )
        self._thread.start()

    def finish(
        self,
        status: str,
        *,
        reason: str | None = None,
        pier_return_code: int | None = None,
    ) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.poll_secs + 1)
        self._write_snapshot(
            terminal_status=status,
            terminal_reason=reason,
            pier_return_code=pier_return_code,
        )

    def _observe(self) -> None:
        while not self._stop.wait(self.poll_secs):
            self._write_snapshot()

    def _write_snapshot(
        self,
        *,
        terminal_status: str | None = None,
        terminal_reason: str | None = None,
        pier_return_code: int | None = None,
    ) -> None:
        with self._lock:
            now = datetime.now(UTC)
            ledgers = self._turn_event_ledgers()
            latest = ledgers[-1] if ledgers else None
            latest_stat = None
            if latest is not None:
                try:
                    latest_stat = latest.stat()
                except OSError:
                    latest = None
            last_event = _last_turn_event(latest) if latest is not None else None
            seconds_since_activity: int | None = None
            if latest_stat is not None:
                seconds_since_activity = max(0, int(now.timestamp() - latest_stat.st_mtime))
            elapsed = max(0, int(time.monotonic() - self._started_monotonic))
            if terminal_status is not None:
                status = terminal_status
                possible_stall = False
            elif latest is None:
                possible_stall = elapsed >= self.stall_after_secs
                status = "possibly_stalled" if possible_stall else "awaiting_session_event"
            else:
                possible_stall = seconds_since_activity >= self.stall_after_secs
                status = "possibly_stalled" if possible_stall else "active"
            payload: dict[str, object] = {
                "schema_version": 1,
                "attempt_id": self.attempt.attempt_id,
                "variant": self.attempt.variant,
                "status": status,
                "started_at_utc": _utc_text(self._started_wall),
                "observed_at_utc": _utc_text(now),
                "elapsed_secs": elapsed,
                "progress_poll_secs": self.poll_secs,
                "progress_stall_after_secs": self.stall_after_secs,
                "session_event_ledgers": [str(path.resolve()) for path in ledgers],
                "event_count": sum(_turn_event_count(path) for path in ledgers),
                "last_activity_at_utc": (
                    _utc_text(datetime.fromtimestamp(latest_stat.st_mtime, UTC))
                    if latest_stat is not None
                    else None
                ),
                "seconds_since_activity": seconds_since_activity,
                "possibly_stalled": possible_stall,
                "last_event": last_event,
                "terminal_reason": terminal_reason,
                "pier_return_code": pier_return_code,
            }
            _write_json(self.progress_path, payload)

    def _turn_event_ledgers(self) -> tuple[Path, ...]:
        job_dir = Task1HostRunner._pier_job_directory(self.attempt)
        paths = tuple(
            path
            for path in job_dir.glob("*/agent/runtime/data/agents/*/sessions/*/turn_events.jsonl")
            if path.is_file()
        )
        return tuple(sorted(paths, key=lambda path: (_file_mtime_ns(path), str(path))))


def build_attempt_toml(
    attempt: AttemptManifest,
    task_prompt: str,
    attempt_deadline_secs: int,
    model_egress_mode: str = "pier",
    harness_mode: str = "standard",
) -> str:
    """容器内 attempt 配置；模型出口与 harness 模式必须随输入一起冻结。"""
    if model_egress_mode not in {"pier", "direct"}:
        raise ValueError("model_egress_mode 仅支持 pier 或 direct")
    if harness_mode not in EVALUATION_HARNESS_MODES:
        raise ValueError("harness_mode 仅支持 standard 或 minimal")
    prompt = (
        f"Please solve this issue:\n\n{task_prompt}\n\n{MINIMAL_TASK_GUIDANCE}"
        if harness_mode == "minimal"
        else "请执行 /coding-benchmark，并解决以下任务：\n\n" + task_prompt
    )
    lines = [
        "schema_version = 1",
        f"attempt_id = {json.dumps(attempt.attempt_id)}",
        f"task_prompt = {json.dumps(prompt)}",
        'workspace_root = "/app"',
        'runtime_root = "/logs/agent/runtime"',
        'acn_config = "/opt/acn-eval/acn.toml"',
        'output_dir = "/logs/agent/evaluation"',
        'upstream = "eval"',
        f"variant = {json.dumps(attempt.variant)}",
        f"attempt_deadline_secs = {attempt_deadline_secs}",
        f"model_egress_mode = {json.dumps(model_egress_mode)}",
        f"harness_mode = {json.dumps(harness_mode)}",
    ]
    if attempt.variant in CLAIM_BUNDLE_VARIANTS:
        lines.append('claim_bundle = "/opt/acn-eval/claims.json"')
    return "\n".join(lines) + "\n"


def attempt_deadline_secs(provenance: EvaluationProvenance) -> int:
    """留出收尾余量：acn_eval 必须早于 Pier 墙钟停下并写出证据。"""
    agent_seconds = _positive_int(provenance.timeouts, "agent_seconds")
    reserve = _positive_int(provenance.timeouts, "deadline_reserve_seconds")
    if reserve >= agent_seconds:
        raise ValueError("deadline_reserve_seconds 必须小于 agent_seconds")
    return agent_seconds - reserve


def build_acn_config(provenance: EvaluationProvenance, upstream_base_url: str) -> str:
    """容器内 ACN 配置；模型 key 只经 api_key_env 从容器环境读取。"""
    resources = provenance.resources
    sections = {
        "": {"upstream": "eval"},
        "upstreams.eval": {
            "agent_id": "evaluation",
            "acn_key_env": "",
            "maintainer_endpoint": "http://127.0.0.1:1",
            "router_endpoint": "http://127.0.0.1:1",
        },
        "storage": {"acn_home": "/logs/agent/runtime"},
        "clients.router": {"query_timeout_secs": 1},
        "clients.http": {
            "timeout_secs": 30,
            "retry_count": 0,
            "retry_base_delay_ms": 1,
            "retry_max_delay_ms": 1,
        },
        # 评测如启动 router rerank，也必须与 agent 一样走 OAI Responses，
        # 并使用同一容器内 key；禁止默认回落到外部 OpenAI 配置。
        "router.rerank": {
            "provider": "openai_responses",
            "endpoint": f"{upstream_base_url.rstrip('/')}/v1",
            "model": provenance.model,
            "timeout_secs": 30,
            "max_tokens": 512,
            "api_key_env": CONTAINER_MODEL_KEY_ENV,
            "retry_count": _positive_int(provenance.llm_retry, "retry_count"),
            "retry_base_delay_ms": _positive_int(
                provenance.llm_retry, "retry_base_delay_ms"
            ),
            "retry_max_delay_ms": _positive_int(
                provenance.llm_retry, "retry_max_delay_ms"
            ),
        },
        "agent.llm": {
            "provider": "openai_responses",
            "endpoint": f"{upstream_base_url.rstrip('/')}/v1",
            "model": provenance.model,
            "reasoning_effort": provenance.reasoning_effort,
            "temperature": EVALUATION_TEMPERATURE,
            "top_p": EVALUATION_TOP_P,
            "api_key_env": CONTAINER_MODEL_KEY_ENV,
            "max_tokens": _positive_int(resources, "max_tokens"),
            "context_window": _positive_int(resources, "context_window"),
            "timeout_secs": _positive_int(provenance.timeouts, "agent_seconds"),
            # 上游偶发 "connection closed before message completed"；不重试会把网络抖动
            # 记成 agent 失败，污染 pass rate（PRD §9 要求二者分开）。
            "retry_count": _positive_int(provenance.llm_retry, "retry_count"),
            "retry_base_delay_ms": _positive_int(provenance.llm_retry, "retry_base_delay_ms"),
            "retry_max_delay_ms": _positive_int(provenance.llm_retry, "retry_max_delay_ms"),
        },
        "agent.session": {
            "id_mint_max_retries": 3,
            "notify_on_finalize_completion": False,
        },
        "agent.session.compaction": {
            "auto_compact_ctx_ratio": EVALUATION_AUTO_COMPACT_CTX_RATIO,
        },
        "agent.session.memory_review": {"enabled": False},
        "agent.memory": {"enabled": False},
        "agent.tool": {
            "file_read_max_chars": EVALUATION_FILE_READ_MAX_CHARS,
            "file_diff_max_changed_lines": EVALUATION_FILE_DIFF_MAX_CHANGED_LINES,
            "file_edit_authority_enabled": provenance.file_edit_authority_enabled,
            "max_parallel_tool_calls": EVALUATION_MAX_PARALLEL_TOOL_CALLS,
            "code_run_max_output_chars": EVALUATION_CODE_RUN_MAX_OUTPUT_CHARS,
        },
        "agent.attachment": {
            "enabled": False,
            "clipboard_image_enabled": False,
            "max_file_bytes": 5242880,
            "max_files_per_turn": 1,
        },
    }
    lines: list[str] = []
    for header, values in sections.items():
        if header:
            lines.append(f"[{header}]")
        lines.extend(f"{key} = {_toml_scalar(value)}" for key, value in values.items())
    return "\n".join(lines) + "\n"


def _toml_scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return json.dumps(value)


def write_attempt_files(
    attempt: AttemptManifest,
    directory: Path,
    task_prompt: str,
    provenance: EvaluationProvenance,
    upstream_base_url: str,
    model_egress_mode: str = "pier",
    harness_mode: str = "standard",
) -> AttemptFiles:
    """写入不含 credential 的容器 attempt TOML 与 ACN 配置。"""
    directory.mkdir(parents=True, exist_ok=True)
    attempt_path = directory / f"{attempt.attempt_id}.toml"
    acn_path = directory / f"{attempt.attempt_id}.acn.toml"
    _atomic_write_text(
        attempt_path,
        build_attempt_toml(
            attempt,
            task_prompt,
            attempt_deadline_secs(provenance),
            model_egress_mode,
            harness_mode,
        ),
    )
    _atomic_write_text(acn_path, build_acn_config(provenance, upstream_base_url))
    return AttemptFiles(attempt_path, acn_path)


def build_pier_job_config(
    attempt: AttemptManifest,
    files: AttemptFiles,
    provenance: EvaluationProvenance,
    artifacts: HostArtifacts,
    upstream_base_url: str,
) -> dict[str, object]:
    """每个 attempt 固定单次、单并发、零 solve retry 的 Pier job。"""
    required_artifacts = (artifacts.acn_eval, artifacts.frozen_skill)
    if attempt.variant in CLAIM_BUNDLE_VARIANTS:
        required_artifacts += (artifacts.claim_bundle,)
    for path in required_artifacts:
        if not path.is_absolute() or not path.exists():
            if path == artifacts.claim_bundle:
                raise ValueError(f"{attempt.variant} 的 claim bundle 必须存在且为绝对路径")
            raise ValueError(f"Pier job artifact 必须存在且为绝对路径: {path}")
    task_dir = artifacts.normalized_task_dir
    if (
        not task_dir.is_absolute()
        or not task_dir.is_dir()
        or not all((task_dir / name).exists() for name in ("task.toml", "environment", "tests"))
    ):
        raise ValueError("normalized_task_dir 必须是含 task.toml/environment/tests 的绝对任务目录")
    upstream_host(upstream_base_url)
    jobs_dir = files.attempt_toml.parent / "pier-jobs"
    jobs_dir.mkdir(exist_ok=True)
    resources = provenance.resources
    return {
        "job_name": attempt.attempt_id,
        "jobs_dir": str(jobs_dir),
        "n_attempts": 1,
        "n_concurrent_trials": 1,
        "retry": {"max_retries": 0},
        "environment": {
            "force_build": False,
            # 结束 trial 时只拆 Compose 容器，不 --rmi。Pier 的 delete=True 会
            # `down --rmi all`，把已预构建的官方题面镜像一并删掉，随后重复拉取，撑爆磁盘。
            "delete": False,
            "override_cpus": resources.get("cpus"),
            "override_memory_mb": resources.get("memory_mb"),
            "override_storage_mb": resources.get("storage_mb"),
            "env": {},
        },
        "verifier": {
            "env": {},
            "disable": False,
            "override_timeout_sec": _positive_int(
                provenance.timeouts, "verifier_seconds"
            ),
        },
        "agents": [
            {
                "import_path": AcnEvalPierAgent.import_path(),
                "model_name": provenance.model,
                # Pier 的墙钟与 acn_eval 的自有 deadline 必须同源，否则 deadline 等不到
                # 就先被 SIGKILL，证据全丢。冻结 task.toml 的 timeout_sec 在此被显式覆盖。
                "override_timeout_sec": _positive_int(provenance.timeouts, "agent_seconds"),
                "kwargs": {
                    "attempt_config": str(files.attempt_toml),
                    "acn_config": str(files.acn_config),
                    "acn_eval": str(artifacts.acn_eval),
                    "frozen_skill": str(artifacts.frozen_skill),
                    "claim_bundle": str(artifacts.claim_bundle),
                    "upstream_base_url": upstream_base_url,
                    "host_model_key_env": HOST_MODEL_KEY_ENV,
                    "container_model_key_env": CONTAINER_MODEL_KEY_ENV,
                    "acn_version": provenance.acn_version,
                },
                "env": {},
            }
        ],
        "datasets": [],
        "tasks": [{"path": str(task_dir)}],
        "artifacts": [],
        "metrics": [],
    }


def build_verifier_regrade_job_config(
    attempt: AttemptManifest,
    provenance: EvaluationProvenance,
    artifacts: HostArtifacts,
    patch: Path,
    patch_sha256: str,
    jobs_dir: Path,
) -> dict[str, object]:
    """在全新任务环境中只重放冻结 patch，不再次调用模型。"""
    if not patch.is_absolute() or not patch.is_file():
        raise ValueError("verifier 重判 patch 必须是存在的绝对文件")
    if _sha256_file(patch) != patch_sha256:
        raise ValueError("verifier 重判 patch 在配置生成前已变化")
    if not jobs_dir.is_absolute():
        raise ValueError("verifier 重判 jobs_dir 必须是绝对路径")
    task_dir = artifacts.normalized_task_dir
    if not task_dir.is_absolute() or not task_dir.is_dir():
        raise ValueError("verifier 重判任务目录必须是存在的绝对路径")
    resources = provenance.resources
    return {
        "job_name": f"{attempt.attempt_id}-verifier-regrade-1",
        "jobs_dir": str(jobs_dir),
        "n_attempts": 1,
        "n_concurrent_trials": 1,
        "retry": {"max_retries": 0},
        "environment": {
            "force_build": False,
            "delete": False,
            "override_cpus": resources.get("cpus"),
            "override_memory_mb": resources.get("memory_mb"),
            "override_storage_mb": resources.get("storage_mb"),
            "env": {},
        },
        "verifier": {
            "env": {},
            "disable": False,
            "override_timeout_sec": _positive_int(
                provenance.timeouts, "verifier_seconds"
            ),
        },
        "agents": [
            {
                "import_path": AcnPatchReplayPierAgent.import_path(),
                "override_timeout_sec": 300,
                "kwargs": {
                    "patch_path": str(patch),
                    "patch_sha256": patch_sha256,
                },
                "env": {},
            }
        ],
        "datasets": [],
        "tasks": [{"path": str(task_dir)}],
        "artifacts": [],
        "metrics": [],
    }


class Task1HostRunner:
    """编排 A → Gate → freeze → B_empty / B_claim / B_forced_claim；默认只生成计划。"""

    def __init__(
        self,
        experiment: ExperimentManifest,
        jobs_directory: Path,
        execution: Task1ExecutionConfig | None = None,
        *,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        cleanup_trial_images: Callable[[str], int] | None = None,
    ) -> None:
        self.experiment = experiment
        self.jobs_directory = jobs_directory.resolve()
        self.execution = execution
        self._run = run
        self._cleanup_trial_images = cleanup_trial_images or cleanup_finished_trial_images
        self._frozen_claim_bundle_hash: str | None = None
        self._frozen_claim_ids: tuple[str, ...] = ()
        self._pier_trial_uris: set[str] = set()
        self._pier_trial_directories: set[Path] = set()
        self._pier_task_checksum: str | None = None
        self._a_only_source_manifest_hash: str | None = None

    def run_task1(self, *, execute: bool = False) -> tuple[HostRunStep, ...] | TaskExecutionResult:
        attempts = self._ordered_attempts()
        if self.execution is not None and self.execution.a_only_source_manifest is not None:
            steps = tuple(
                HostRunStep(attempt.variant, attempt.attempt_id, execute)
                for attempt in attempts[1:]
            )
        else:
            steps = tuple(
                [
                    HostRunStep("A", attempts[0].attempt_id, execute),
                    HostRunStep("freeze", None, execute),
                ]
                + [
                    HostRunStep(attempt.variant, attempt.attempt_id, execute)
                    for attempt in attempts[1:]
                ]
            )
        if execute:
            return self._execute_attempts(attempts)
        return steps

    def _ordered_attempts(self) -> tuple[AttemptManifest, ...]:
        attempts = self.experiment.attempts
        b_variants = {item.variant for item in attempts[1:]}
        valid_variants = (
            (len(attempts) == 3 and b_variants == {"B_empty", "B_claim"})
            or (
                len(attempts) == 4
                and b_variants == {"B_empty", "B_claim", "B_forced_claim"}
            )
        )
        if (
            not attempts
            or attempts[0].variant != "A"
            or not valid_variants
        ):
            raise ValueError(
                "task1 host runner 只接受 A 后接 B_empty/B_claim 的旧三臂，或包含 "
                "B_forced_claim 的四臂"
            )
        return attempts

    def _execute_attempts(
        self, attempts: tuple[AttemptManifest, ...]
    ) -> TaskExecutionResult:
        execution = self.execution
        if execution is None:
            raise TaskExecutionError("execute=True 必须提供 Task1ExecutionConfig")
        self._validate_execution(execution)
        self._frozen_claim_bundle_hash = None
        self._frozen_claim_ids = ()
        self._pier_trial_uris.clear()
        self._pier_trial_directories.clear()
        self._pier_task_checksum = None
        self._a_only_source_manifest_hash = None
        records: list[AttemptExecutionRecord] = []
        cohort: str | None = None
        try:
            if execution.a_only_source_manifest is not None:
                source = self._load_a_only_source(attempts, execution)
                records.append(source.a_record)
                cohort = source.cohort
                self._frozen_claim_ids = source.claim_ids
                self._frozen_claim_bundle_hash = source.claim_bundle_hash
                self._pier_task_checksum = source.pier_task_checksum
                self._a_only_source_manifest_hash = source.source_manifest_hash
            else:
                a_record = self._run_one_attempt(attempts[0], execution)
                records.append(a_record)
                if a_record.status in {"gate_failed", "infrastructure_failed"}:
                    raise TaskExecutionError(a_record.reason)
                self._freeze_after_a(attempts[0], execution, a_record)
                has_frozen_claims = bool(self._frozen_claim_ids)
                cohort = (
                    "success_efficiency"
                    if a_record.verifier_passed is True
                    else "failure_recovery"
                ) if has_frozen_claims else "unpaired_no_claim"
            if execution.run_a_only:
                for attempt in attempts[1:]:
                    records.append(
                        AttemptExecutionRecord(
                            attempt.attempt_id,
                            attempt.variant,
                            "not_run",
                            "A_ONLY",
                            None,
                            None,
                        )
                    )
                self._write_execution_manifest(execution, records, None, cohort)
                return TaskExecutionResult("passed")
            has_frozen_claims = bool(self._frozen_claim_ids)
            if not has_frozen_claims and not execution.run_all_variants_without_claims:
                if execution.require_eligible_claim:
                    raise TaskExecutionError("NO_ELIGIBLE_CLAIM")
                for attempt in attempts[1:]:
                    if attempt.variant in CLAIM_BUNDLE_VARIANTS:
                        records.append(
                            AttemptExecutionRecord(
                                attempt.attempt_id,
                                attempt.variant,
                                "not_run",
                                "NO_ELIGIBLE_CLAIM",
                                None,
                                None,
                            )
                        )
                        continue
                    record = self._run_one_attempt(attempt, execution)
                    records.append(record)
                    if record.status in {"gate_failed", "infrastructure_failed"}:
                        raise TaskExecutionError(record.reason)
                self._write_execution_manifest(execution, records, None, cohort)
                return TaskExecutionResult("no_eligible_claim")
            for attempt in attempts[1:]:
                record = self._run_one_attempt(attempt, execution)
                records.append(record)
                if record.status in {"gate_failed", "infrastructure_failed"}:
                    raise TaskExecutionError(record.reason)
        except (OSError, ValueError, PierResultError, TaskExecutionError) as error:
            self._write_execution_manifest(execution, records, str(error), cohort)
            raise TaskExecutionError(str(error)) from error
        self._write_execution_manifest(execution, records, None, cohort)
        return TaskExecutionResult("passed")

    def _run_one_attempt(
        self, attempt: AttemptManifest, execution: Task1ExecutionConfig
    ) -> AttemptExecutionRecord:
        self._assert_a_only_source_still_bound(execution)
        self._validate_frozen_inputs(execution)
        attempt_dir = Path(attempt.output_path).resolve()
        disk_paths = [attempt_dir]
        if execution.docker_root is not None:
            disk_paths.append(execution.docker_root)
        verify_disk_headroom(
            disk_paths,
            execution.disk_reserve_mb
            + execution.task_workers * execution.disk_admission_mb,
        )
        attempt_dir.mkdir(parents=True, exist_ok=False)
        files = write_attempt_files(
            attempt,
            attempt_dir / "host-config",
            execution.task_prompt,
            self.experiment.provenance,
            execution.upstream_base_url,
            execution.model_egress_mode,
            execution.harness_mode,
        )
        progress = AttemptProgressMonitor(
            attempt,
            attempt_dir,
            poll_secs=execution.progress_poll_secs,
            stall_after_secs=execution.progress_stall_after_secs,
        )
        progress.start()
        try:
            job = build_pier_job_config(
                attempt,
                files,
                self.experiment.provenance,
                execution.artifacts,
                execution.upstream_base_url,
            )
            self.jobs_directory.mkdir(parents=True, exist_ok=True)
            job_path = self.jobs_directory / f"{attempt.attempt_id}.json"
            _write_json(job_path, job)
            # Pier 子进程继承宿主环境以取得模型 key；PATH 用于定位 pinned `pier`。
            completed = self._run_pier_process(
                execution,
                job_path,
                stdout_path=attempt_dir / "pier.stdout.log",
                stderr_path=attempt_dir / "pier.stderr.log",
            )
            if completed.returncode != 0:
                reason = "PIER_INFRASTRUCTURE_FAILURE"
                progress.finish(
                    "pier_failed", reason=reason, pier_return_code=completed.returncode
                )
                return self._write_failed_attempt(
                    attempt, attempt_dir, reason, progress.progress_path
                )
        except (OSError, ValueError, KeyboardInterrupt) as error:
            reason = _attempt_start_failure_reason(error)
            progress.finish("interrupted", reason=reason)
            return self._write_failed_attempt(attempt, attempt_dir, reason, progress.progress_path)
        try:
            record, trial_names = self._collect_and_gate(
                attempt, execution, attempt_dir, progress.progress_path
            )
        except KeyboardInterrupt:
            reason = "INTERRUPTED_BY_OPERATOR"
            progress.finish("interrupted", reason=reason)
            return self._write_failed_attempt(attempt, attempt_dir, reason, progress.progress_path)
        except (OSError, ValueError, PierResultError) as error:
            reason = f"EVIDENCE_PARSE_FAILURE:{error}"
            progress.finish("evidence_parse_failed", reason=reason)
            return self._write_failed_attempt(attempt, attempt_dir, reason, progress.progress_path)
        try:
            cleanup = {
                trial_name: self._cleanup_trial_images(trial_name)
                for trial_name in trial_names
            }
        except ResourceGuardError as error:
            reason = f"DOCKER_TRIAL_IMAGE_CLEANUP_FAILED:{error}"
            _write_json(
                attempt_dir / "docker-image-cleanup.json",
                {"schema_version": 1, "status": "failed", "reason": reason},
            )
            progress.finish("docker_cleanup_failed", reason=reason)
            return replace(
                record,
                status="infrastructure_failed",
                reason=reason,
                progress_path=str(progress.progress_path.resolve()),
            )
        _write_json(
            attempt_dir / "docker-image-cleanup.json",
            {
                "schema_version": 1,
                "status": "completed",
                "removed_image_references_by_trial": cleanup,
            },
        )
        progress.finish("pier_completed", pier_return_code=completed.returncode)
        self._assert_a_only_source_still_bound(execution)
        return replace(record, progress_path=str(progress.progress_path.resolve()))

    def _run_pier_process(
        self,
        execution: Task1ExecutionConfig,
        job_path: Path,
        *,
        stdout_path: Path,
        stderr_path: Path,
    ) -> subprocess.CompletedProcess[str]:
        """把 Pier 输出流式落盘，避免长任务的 stdout/stderr 常驻宿主内存。"""
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            stdout_path.open("w", encoding="utf-8") as stdout,
            stderr_path.open("w", encoding="utf-8") as stderr,
        ):
            return self._run(
                [str(execution.pier_executable), "run", "-c", str(job_path)],
                check=False,
                stdout=stdout,
                stderr=stderr,
                text=True,
                env={
                    **os.environ,
                    "PYTHONPATH": os.pathsep.join(
                        (
                            str(execution.frozen_acn_source_root),
                            str(execution.frozen_pier_source_root),
                        )
                    ),
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
            )

    def _collect_and_gate(
        self,
        attempt: AttemptManifest,
        execution: Task1ExecutionConfig,
        attempt_dir: Path,
        progress_path: Path,
    ) -> tuple[AttemptExecutionRecord, tuple[str, ...]]:
        trial_dir, pier = read_single_trial_evidence(self._pier_job_directory(attempt))
        trial_names = [pier.trial_name]
        evaluation_dir = trial_dir / "agent" / "evaluation"
        rust_result = read_rust_result(evaluation_dir / "result.json")
        events_path = evaluation_dir / "events.jsonl"
        events = read_rust_event_ledger(events_path)
        if rust_result.attempt_id != attempt.attempt_id or any(
            event.attempt_id != attempt.attempt_id for event in events
        ):
            raise ValueError("Rust result/events attempt_id 与当前 attempt 不一致")
        if rust_result.exit_type not in {"completed", "failed"}:
            raise ValueError(f"Rust exit_type 无效: {rust_result.exit_type}")
        if rust_result.failure_kind == "upstream_concurrency_exhausted":
            reason = "UPSTREAM_CONCURRENCY_EXHAUSTED"
            _write_json(
                attempt_dir / "attempt-result.json",
                {
                    "schema_version": 1,
                    "attempt_id": attempt.attempt_id,
                    "variant": attempt.variant,
                    "failure": reason,
                    "rust_result": str((evaluation_dir / "result.json").resolve()),
                    "rust_events": str(events_path.resolve()),
                    "progress_path": str(progress_path.resolve()),
                    "failure_kind": rust_result.failure_kind,
                },
            )
            return AttemptExecutionRecord(
                attempt.attempt_id,
                attempt.variant,
                "infrastructure_failed",
                reason,
                str(attempt_dir / "attempt-result.json"),
                None,
                progress_path=str(progress_path.resolve()),
            ), tuple(trial_names)
        patch = trial_dir / "artifacts" / "model.patch"
        artifact_hash = _sha256_file(patch)
        verifier_evidence = pier
        verifier_regrade: dict[str, object] | None = None
        infrastructure_reason = pier.infrastructure_failure_reason()
        if infrastructure_reason is not None:
            try:
                regrade_dir, regrade = self._run_verifier_regrade(
                    attempt,
                    execution,
                    attempt_dir,
                    patch,
                    artifact_hash,
                    pier,
                )
            except (OSError, ValueError, PierResultError) as error:
                reason = f"{infrastructure_reason}:VERIFIER_REGRADE_FAILED:{error}"
                result_path = attempt_dir / "attempt-result.json"
                _write_json(
                    result_path,
                    {
                        "schema_version": 1,
                        "attempt_id": attempt.attempt_id,
                        "variant": attempt.variant,
                        "failure": reason,
                        "pier_trial": pier.to_dict(),
                        "rust_result": str((evaluation_dir / "result.json").resolve()),
                        "rust_events": str(events_path.resolve()),
                        "artifact_patch": str(patch.resolve()),
                        "artifact_hash": artifact_hash,
                        "progress_path": str(progress_path.resolve()),
                    },
                )
                return AttemptExecutionRecord(
                    attempt.attempt_id,
                    attempt.variant,
                    "infrastructure_failed",
                    reason,
                    str(result_path),
                    None,
                    progress_path=str(progress_path.resolve()),
                ), tuple(trial_names)
            verifier_evidence = regrade
            trial_names.append(regrade.trial_name)
            verifier_regrade = {
                "trigger": infrastructure_reason,
                "source_patch_sha256": artifact_hash,
                "trial_directory": str(regrade_dir.resolve()),
                "pier_trial": regrade.to_dict(),
            }
        if attempt.variant in CLAIM_BUNDLE_VARIANTS:
            frozen_bundle_sha256, frozen_claim_content_hashes = _frozen_bundle_evidence(
                execution.artifacts.claim_bundle
            )
        else:
            frozen_bundle_sha256, frozen_claim_content_hashes = None, {}
        isolation_checks = self._isolation_checks(attempt, execution, trial_dir, pier)
        verifier = verifier_evidence.verifier_for(attempt.attempt_id)
        gate = GateValidator().validate(
            AttemptGateInput.from_rust_result(
                rust_result,
                variant=attempt.variant,
                artifact_hash=artifact_hash,
                verifier=verifier,
                expected_response_model=execution.expected_response_model,
                frozen_claim_ids=tuple(frozen_claim_content_hashes),
                frozen_bundle_sha256=frozen_bundle_sha256,
                frozen_claim_content_hashes=frozen_claim_content_hashes,
                isolation_checks=isolation_checks,
                require_claim_injection=(
                    bool(self._frozen_claim_ids)
                    and (
                        attempt.variant == "B_forced_claim"
                        or (execution.require_eligible_claim and attempt.variant == "B_claim")
                    )
                ),
                allow_empty_claim_bundle=(
                    execution.run_all_variants_without_claims
                    and attempt.variant in CLAIM_BUNDLE_VARIANTS
                    and not self._frozen_claim_ids
                ),
            )
        )
        gate_path = attempt_dir / "gate.json"
        _write_json(gate_path, gate.to_dict())
        result_path = attempt_dir / "attempt-result.json"
        _write_json(
            result_path,
            {
                "schema_version": 1,
                "attempt_id": attempt.attempt_id,
                "variant": attempt.variant,
                "pier_trial": pier.to_dict(),
                "verifier_regrade": verifier_regrade,
                "rust_result": str((evaluation_dir / "result.json").resolve()),
                "rust_events": str(events_path.resolve()),
                "artifact_patch": str(patch.resolve()),
                "artifact_hash": artifact_hash,
                "attempt_config_hash": _sha256_file(
                    attempt_dir / "host-config" / f"{attempt.attempt_id}.toml"
                ),
                "acn_config_hash": _sha256_file(
                    attempt_dir / "host-config" / f"{attempt.attempt_id}.acn.toml"
                ),
                "frozen_claim_bundle_hash": (
                    frozen_bundle_sha256 if attempt.variant in CLAIM_BUNDLE_VARIANTS else None
                ),
                "claim_observation": _claim_observation(
                    attempt.variant,
                    rust_result.router_evidence,
                    rust_result.claim_used_ids,
                    frozen_claim_content_hashes,
                ),
                "rust_exit_type": rust_result.exit_type,
                "failure_kind": rust_result.failure_kind,
                "agent_steps": rust_result.agent_steps,
                "usage": rust_result.usage.to_dict(),
                "model": self.experiment.provenance.model,
                "expected_response_model": execution.expected_response_model,
                "progress_path": str(progress_path.resolve()),
                "verifier_passed": verifier.passed,
                "isolation_checks": isolation_checks,
                "gate": gate.to_dict(),
            },
        )
        if gate.decision == "fail":
            status = "gate_failed"
        elif rust_result.exit_type == "failed":
            status = "agent_failed"
        else:
            status = "passed"
        reason = (
            f"RUST_EXIT_FAILED,{gate.reason}" if rust_result.exit_type == "failed" else gate.reason
        )
        return AttemptExecutionRecord(
            attempt.attempt_id,
            attempt.variant,
            status,
            reason,
            str(result_path),
            str(gate_path),
            verifier.passed,
            _claim_observation(
                attempt.variant,
                rust_result.router_evidence,
                rust_result.claim_used_ids,
                frozen_claim_content_hashes,
            ),
            str(progress_path.resolve()),
        ), tuple(trial_names)

    def _run_verifier_regrade(
        self,
        attempt: AttemptManifest,
        execution: Task1ExecutionConfig,
        attempt_dir: Path,
        patch: Path,
        patch_sha256: str,
        original: PierTrialEvidence,
    ) -> tuple[Path, PierTrialEvidence]:
        """对已知基础设施故障最多重判一次，且不执行 ACN 或模型请求。"""
        disk_paths = [attempt_dir]
        if execution.docker_root is not None:
            disk_paths.append(execution.docker_root)
        verify_disk_headroom(
            disk_paths,
            execution.disk_reserve_mb
            + execution.task_workers * execution.disk_admission_mb,
        )
        regrade_root = (attempt_dir / "verifier-regrade").resolve()
        jobs_dir = regrade_root / "pier-jobs"
        job = build_verifier_regrade_job_config(
            attempt,
            self.experiment.provenance,
            execution.artifacts,
            patch.resolve(),
            patch_sha256,
            jobs_dir,
        )
        regrade_root.mkdir(parents=True, exist_ok=False)
        job_path = regrade_root / "job.json"
        _write_json(job_path, job)
        completed = self._run_pier_process(
            execution,
            job_path,
            stdout_path=regrade_root / "pier.stdout.log",
            stderr_path=regrade_root / "pier.stderr.log",
        )
        if completed.returncode != 0:
            raise ValueError(f"Pier verifier 重判退出码为 {completed.returncode}")
        job_directory = jobs_dir / str(job["job_name"])
        trial_dir, evidence = read_single_trial_evidence(job_directory)
        if evidence.task_checksum != original.task_checksum:
            raise ValueError("verifier 重判 task checksum 与原 trial 不一致")
        replay_patch = trial_dir / "artifacts" / "model.patch"
        if _sha256_file(replay_patch) != patch_sha256:
            raise ValueError("verifier 重判产出的 patch 与冻结源 patch 不一致")
        if evidence.verifier_rewards is None:
            reason = evidence.infrastructure_failure_reason() or "VERIFIER_DID_NOT_RUN"
            raise ValueError(f"verifier 重判仍无结果: {reason}")
        return trial_dir, evidence

    def _freeze_after_a(
        self,
        attempt: AttemptManifest,
        execution: Task1ExecutionConfig,
        a_record: AttemptExecutionRecord,
    ) -> None:
        evaluation_dir = self._find_evaluation_dir(attempt)
        result = read_rust_result(evaluation_dir / "result.json")
        if Path(result.event_ledger_path).name != "events.jsonl":
            raise TaskExecutionError("Rust result event_ledger_path 不符合定版文件名")
        host_ledger = (evaluation_dir / "events.jsonl").resolve()
        if a_record.result_path is None or a_record.verifier_passed is None:
            raise TaskExecutionError("A attempt 缺少 producer verifier 证据，不能冻结 claim")
        attempt_result_path = Path(a_record.result_path).resolve()
        attempt_result_hash = _sha256_file(attempt_result_path)
        append_freeze_barrier(host_ledger, attempt.attempt_id, f"freeze-{attempt.attempt_id}")
        bundle = freeze_claim_bundle(
            host_ledger,
            attempt.attempt_id,
            execution.artifacts.claim_bundle.resolve(),
            producer_verification={
                "attempt_id": attempt.attempt_id,
                "verifier_passed": a_record.verifier_passed,
                "attempt_result_sha256": attempt_result_hash,
            },
        )
        self._frozen_claim_ids = tuple(claim.claim_id for claim in bundle.claims)
        self._frozen_claim_bundle_hash = _sha256_file(execution.artifacts.claim_bundle)

    def _find_evaluation_dir(self, attempt: AttemptManifest) -> Path:
        trial_dir, _ = read_single_trial_evidence(self._pier_job_directory(attempt))
        return (trial_dir / "agent" / "evaluation").resolve()

    def _isolation_checks(
        self,
        attempt: AttemptManifest,
        execution: Task1ExecutionConfig,
        trial_dir: Path,
        pier: PierTrialEvidence,
    ) -> dict[str, bool]:
        resolved_trial_dir = trial_dir.resolve()
        trial_uri_unique = pier.trial_uri not in self._pier_trial_uris
        trial_directory_unique = resolved_trial_dir not in self._pier_trial_directories
        self._pier_trial_uris.add(pier.trial_uri)
        self._pier_trial_directories.add(resolved_trial_dir)

        if self._pier_task_checksum is None:
            task_checksum_matches_a = attempt.variant == "A"
            self._pier_task_checksum = pier.task_checksum
        else:
            task_checksum_matches_a = pier.task_checksum == self._pier_task_checksum

        agent_offline, verifier_offline = _task_network_disabled(
            execution.artifacts.normalized_task_dir / "task.toml"
        )
        return {
            "pier_task_matches_attempt": _pier_task_matches_attempt(
                execution.artifacts.normalized_task_dir / "task.toml",
                attempt.task_id,
                pier.task_name,
            ),
            "pier_task_checksum_matches_a": task_checksum_matches_a,
            "pier_trial_uri_unique": trial_uri_unique,
            "pier_trial_directory_unique": trial_directory_unique,
            "normalized_task_tree_matches_provenance": (
                sha256_directory_tree(execution.artifacts.normalized_task_dir)
                == self.experiment.provenance.normalized_task_tree_hash
            ),
            "task_agent_no_network": agent_offline,
            "task_verifier_no_network": verifier_offline,
            # 只有经过 Pier allowlist 的模型出口可作为正式结果；direct 仅供诊断，
            # 即使 task.toml 仍声明离线，也不能让 Gate 误把它记成正式隔离。
            "model_egress_is_formal": execution.model_egress_mode == "pier"
            and _attempt_model_egress_matches(
                Path(attempt.output_path) / "host-config" / f"{attempt.attempt_id}.toml",
                "pier",
            ),
            "claim_visibility_matches_variant": _claim_visibility_matches_variant(
                Path(attempt.output_path) / "host-config" / f"{attempt.attempt_id}.toml",
                attempt.variant,
            ),
        }

    @staticmethod
    def _pier_job_directory(attempt: AttemptManifest) -> Path:
        return (
            Path(attempt.output_path).resolve() / "host-config" / "pier-jobs" / attempt.attempt_id
        )

    def _write_failed_attempt(
        self, attempt: AttemptManifest, attempt_dir: Path, reason: str, progress_path: Path
    ) -> AttemptExecutionRecord:
        path = attempt_dir / "attempt-result.json"
        _write_json(
            path,
            {
                "schema_version": 1,
                "attempt_id": attempt.attempt_id,
                "variant": attempt.variant,
                "failure": reason,
                "progress_path": str(progress_path.resolve()),
            },
        )
        return AttemptExecutionRecord(
            attempt.attempt_id,
            attempt.variant,
            "infrastructure_failed",
            reason,
            str(path),
            None,
            progress_path=str(progress_path.resolve()),
        )

    def _write_execution_manifest(
        self,
        execution: Task1ExecutionConfig,
        records: list[AttemptExecutionRecord],
        failure: str | None,
        cohort: str | None = None,
    ) -> None:
        experiment = self.experiment.to_dict()
        if self._frozen_claim_bundle_hash is not None:
            experiment = {**experiment, "claim_bundle_hash": self._frozen_claim_bundle_hash}
        _write_json(
            execution.manifest_path.resolve(),
            {
                "schema_version": 1,
                "experiment": experiment,
                "frozen_claim_bundle_hash": self._frozen_claim_bundle_hash,
                "experiment_cohort": cohort,
                "execution": {
                    "model": self.experiment.provenance.model,
                    "response_model": execution.expected_response_model,
                    "upstream_base_url": execution.upstream_base_url,
                    "host_model_key_env": HOST_MODEL_KEY_ENV,
                    "container_model_key_env": CONTAINER_MODEL_KEY_ENV,
                    "model_egress_mode": execution.model_egress_mode,
                    "harness_mode": execution.harness_mode,
                    "task_workers": execution.task_workers,
                    "progress_poll_secs": execution.progress_poll_secs,
                    "progress_stall_after_secs": execution.progress_stall_after_secs,
                    "docker_root": (
                        str(execution.docker_root) if execution.docker_root is not None else None
                    ),
                    "disk_reserve_mb": execution.disk_reserve_mb,
                    "disk_admission_mb": execution.disk_admission_mb,
                    "pier_executable": str(execution.pier_executable),
                    "task_prompt_hash": hashlib.sha256(
                        execution.task_prompt.encode("utf-8")
                    ).hexdigest(),
                    "run_all_variants_without_claims": execution.run_all_variants_without_claims,
                    "run_a_only": execution.run_a_only,
                    "phase_mode": (
                        "b_only_from_a"
                        if execution.a_only_source_manifest is not None
                        else ("a_only" if execution.run_a_only else "full")
                    ),
                    "a_only_source_manifest": (
                        str(execution.a_only_source_manifest.resolve())
                        if execution.a_only_source_manifest is not None
                        else None
                    ),
                    "a_only_source_manifest_hash": (
                        self._a_only_source_manifest_hash
                        if execution.a_only_source_manifest is not None
                        else None
                    ),
                },
                "attempt_results": [_attempt_record_dict(record) for record in records],
                "failure": failure,
            },
        )

    def validate_b_only_source(self) -> AOnlySourceEvidence | None:
        """在批量调度前验证 A-only 来源，避免部分 task 已启动后才发现漂移。"""
        execution = self.execution
        if execution is None or execution.a_only_source_manifest is None:
            return None
        attempts = self._ordered_attempts()
        self._validate_execution(execution)
        return self._load_a_only_source(attempts, execution)

    def _assert_a_only_source_still_bound(self, execution: Task1ExecutionConfig) -> None:
        if execution.a_only_source_manifest is None:
            return
        source = self._load_a_only_source(self._ordered_attempts(), execution)
        if self._a_only_source_manifest_hash is None:
            return
        if (
            source.source_manifest_hash != self._a_only_source_manifest_hash
            or source.claim_bundle_hash != self._frozen_claim_bundle_hash
        ):
            raise TaskExecutionError("A-only source 在 B-only 执行期间发生漂移")

    def _load_a_only_source(
        self,
        attempts: tuple[AttemptManifest, ...],
        execution: Task1ExecutionConfig,
    ) -> AOnlySourceEvidence:
        source_manifest = execution.a_only_source_manifest
        if source_manifest is None:
            raise TaskExecutionError("B-only 阶段缺少 A-only source manifest")
        source_manifest = source_manifest.resolve()
        try:
            source = _read_json_mapping(source_manifest, "A-only source manifest")
            if source.get("schema_version") != 1 or source.get("failure") is not None:
                raise TaskExecutionError("A-only source manifest 未成功完成或 schema 无效")
            source_experiment = _required_mapping(source, "experiment")
            source_execution = _required_mapping(source, "execution")
            source_output = _source_output_root(source_manifest, attempts[0].task_id)
            expected_bundle = source_manifest.parent / "claims.json"
            if execution.artifacts.claim_bundle.resolve() != expected_bundle.resolve():
                raise TaskExecutionError("B-only claim bundle 必须来自同一个 A-only task 目录")
            self._validate_source_experiment(
                source_experiment, source_execution, source_output, attempts, execution
            )
            records = _source_attempt_records(source, attempts)
            a_record = records[0]
            source_result, source_gate = _validate_source_a_record(
                a_record, attempts[0], source_output
            )
            bundle_hash, claim_ids = _validate_source_claim_bundle(
                expected_bundle,
                attempts[0].attempt_id,
                source_result,
            )
            if source.get("frozen_claim_bundle_hash") != bundle_hash:
                raise TaskExecutionError("A-only source manifest 的 claim bundle hash 不一致")
            if source_experiment.get("claim_bundle_hash") != bundle_hash:
                raise TaskExecutionError("A-only experiment 的 claim bundle hash 不一致")
            verifier_passed = source_result.get("verifier_passed")
            if not isinstance(verifier_passed, bool) or a_record.verifier_passed != verifier_passed:
                raise TaskExecutionError("A-only verifier 证据与 attempt record 不一致")
            cohort = source.get("experiment_cohort")
            expected_cohort = (
                ("success_efficiency" if verifier_passed else "failure_recovery")
                if claim_ids
                else "unpaired_no_claim"
            )
            if cohort != expected_cohort:
                raise TaskExecutionError("A-only experiment cohort 与冻结证据不一致")
            pier_trial = _required_mapping(source_result, "pier_trial")
            task_checksum = pier_trial.get("task_checksum")
            if not isinstance(task_checksum, str) or not task_checksum:
                raise TaskExecutionError("A-only Pier 证据缺少 task_checksum")
            if source_gate.get("decision") != "pass":
                raise TaskExecutionError("A-only Gate 未通过")
            return AOnlySourceEvidence(
                a_record=a_record,
                cohort=expected_cohort,
                claim_ids=claim_ids,
                claim_bundle_hash=bundle_hash,
                pier_task_checksum=task_checksum,
                source_manifest_hash=_sha256_file(source_manifest),
            )
        except TaskExecutionError:
            raise
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise TaskExecutionError(f"A-only source 证据无效: {error}") from error

    def _validate_source_experiment(
        self,
        source_experiment: Mapping[str, object],
        source_execution: Mapping[str, object],
        source_output: Path,
        attempts: tuple[AttemptManifest, ...],
        execution: Task1ExecutionConfig,
    ) -> None:
        source_attempts = source_experiment.get("attempts")
        if not isinstance(source_attempts, list) or len(source_attempts) != len(attempts):
            raise TaskExecutionError("A-only source attempt plan 数量不一致")
        for raw, expected in zip(source_attempts, attempts, strict=True):
            if not isinstance(raw, Mapping):
                raise TaskExecutionError("A-only source attempt plan 含无效记录")
            if any(
                raw.get(field) != value
                for field, value in (
                    ("attempt_id", expected.attempt_id),
                    ("task_id", expected.task_id),
                    ("variant", expected.variant),
                )
            ):
                raise TaskExecutionError("A-only source attempt identity 与 B-only plan 不一致")
            expected_output = source_output / "attempts" / expected.attempt_id / "output"
            raw_output = raw.get("output_path")
            if not isinstance(raw_output, str) or Path(raw_output).resolve() != expected_output:
                raise TaskExecutionError("A-only source attempt output_path 越出冻结 run 边界")
        source_provenance = _required_mapping(source_experiment, "provenance")
        current_provenance = self.experiment.provenance.to_dict()
        for name, value in current_provenance.items():
            # 两阶段的 config hash 因 phase flag 不同；其余公平性输入必须完全一致。
            if name != "acn_config_hash" and source_provenance.get(name) != value:
                raise TaskExecutionError(f"A-only source provenance 漂移: {name}")
        expected_execution: tuple[tuple[str, object], ...] = (
            ("model", self.experiment.provenance.model),
            ("response_model", execution.expected_response_model),
            ("upstream_base_url", execution.upstream_base_url),
            ("model_egress_mode", execution.model_egress_mode),
            ("harness_mode", execution.harness_mode),
            ("task_workers", execution.task_workers),
            ("progress_poll_secs", execution.progress_poll_secs),
            ("progress_stall_after_secs", execution.progress_stall_after_secs),
            (
                "task_prompt_hash",
                hashlib.sha256(execution.task_prompt.encode("utf-8")).hexdigest(),
            ),
            ("run_a_only", True),
        )
        for name, value in expected_execution:
            if source_execution.get(name) != value:
                raise TaskExecutionError(f"A-only source execution 配置漂移: {name}")
        source_pier = source_execution.get("pier_executable")
        if (
            not isinstance(source_pier, str)
            or not Path(source_pier).is_absolute()
            or _sha256_file(Path(source_pier)) != _sha256_file(execution.pier_executable)
        ):
            raise TaskExecutionError("A-only source execution 配置漂移: pier_executable")

    def _validate_execution(self, execution: Task1ExecutionConfig) -> None:
        if not execution.upstream_base_url or not execution.expected_response_model:
            raise TaskExecutionError("upstream_base_url 与 expected_response_model 不得为空")
        if not os.environ.get(HOST_MODEL_KEY_ENV):
            raise TaskExecutionError(f"宿主环境缺少模型 key: {HOST_MODEL_KEY_ENV}")
        if execution.progress_poll_secs <= 0 or execution.progress_stall_after_secs <= 0:
            raise TaskExecutionError("progress_poll_secs 与 progress_stall_after_secs 必须为正整数")
        if execution.task_workers <= 0:
            raise TaskExecutionError("task_workers 必须为正整数")
        if execution.disk_reserve_mb <= 0 or execution.disk_admission_mb <= 0:
            raise TaskExecutionError("disk_reserve_mb 与 disk_admission_mb 必须为正整数")
        if execution.progress_stall_after_secs < execution.progress_poll_secs:
            raise TaskExecutionError("progress_stall_after_secs 不得小于 progress_poll_secs")
        if execution.require_eligible_claim and execution.run_all_variants_without_claims:
            raise TaskExecutionError(
                "require_eligible_claim 与 run_all_variants_without_claims 不能同时启用"
            )
        if execution.run_a_only and execution.require_eligible_claim:
            raise TaskExecutionError("run_a_only 与 require_eligible_claim 不能同时启用")
        if execution.run_a_only and execution.a_only_source_manifest is not None:
            raise TaskExecutionError("run_a_only 与 a_only_source_manifest 不能同时启用")
        if execution.model_egress_mode not in {"pier", "direct"}:
            raise TaskExecutionError("model_egress_mode 仅支持 pier 或 direct")
        if execution.harness_mode not in EVALUATION_HARNESS_MODES:
            raise TaskExecutionError("harness_mode 仅支持 standard 或 minimal")
        for path in (
            execution.artifacts.acn_eval,
            execution.artifacts.frozen_skill,
            execution.artifacts.claim_bundle,
            execution.artifacts.normalized_task_dir,
            execution.manifest_path.parent,
            execution.pier_executable,
            execution.frozen_acn_source_root,
            execution.frozen_pier_source_root,
        ):
            if not path.is_absolute():
                raise TaskExecutionError(f"真实执行路径必须是绝对路径: {path}")
        if (
            execution.a_only_source_manifest is not None
            and not execution.a_only_source_manifest.is_absolute()
        ):
            raise TaskExecutionError(
                f"A-only source manifest 必须是绝对路径: {execution.a_only_source_manifest}"
            )
        if execution.docker_root is not None and not execution.docker_root.is_absolute():
            raise TaskExecutionError("docker_root 必须是绝对路径")
        if not execution.pier_executable.is_file():
            raise TaskExecutionError(
                f"pier_executable 必须是存在的可执行文件: {execution.pier_executable}"
            )
        self._validate_frozen_inputs(execution)
        bundle_metadata = execution.artifacts.claim_bundle.with_name(
            execution.artifacts.claim_bundle.name + ".manifest.json"
        )
        if execution.a_only_source_manifest is None:
            if execution.artifacts.claim_bundle.exists() or bundle_metadata.exists():
                raise TaskExecutionError(
                    f"claim bundle 输出已存在，拒绝复用旧产物: {execution.artifacts.claim_bundle}"
                )
        else:
            self._load_a_only_source(self._ordered_attempts(), execution)
        if (
            self.experiment.provenance.agent_image_content_digest is None
            or self.experiment.provenance.verifier_image_content_digest is None
        ):
            raise TaskExecutionError("真实执行缺少 agent/verifier image content digest")

    def _validate_frozen_inputs(self, execution: Task1ExecutionConfig) -> None:
        """确认每臂的二进制、skill、任务与 Python 源码均未偏离 provenance。"""
        try:
            acn_binary_hash = hashlib.sha256(execution.artifacts.acn_eval.read_bytes()).hexdigest()
            skill_hash = sha256_directory_tree(execution.artifacts.frozen_skill)
            normalized_task_hash = sha256_directory_tree(execution.artifacts.normalized_task_dir)
        except (OSError, ValueError) as error:
            raise TaskExecutionError("冻结输入无法读取或不是受支持的目录树") from error
        if acn_binary_hash != self.experiment.provenance.acn_binary_hash:
            raise TaskExecutionError("acn_eval 与冻结 provenance 不一致")
        if skill_hash != self.experiment.provenance.skill_hash:
            raise TaskExecutionError("frozen skill 与 provenance 不一致")
        if normalized_task_hash != self.experiment.provenance.normalized_task_tree_hash:
            raise TaskExecutionError("normalized task 目录与冻结 provenance 不一致")
        expected_sources = (
            (
                execution.frozen_acn_source_root,
                "acn_deepswe",
                self.experiment.provenance.acn_package_tree_hash,
                "ACN",
            ),
            (
                execution.frozen_pier_source_root,
                "pier",
                self.experiment.provenance.pier_package_tree_hash,
                "Pier",
            ),
        )
        for source_root, package_name, expected_hash, label in expected_sources:
            if not source_root.is_dir() or not (source_root / package_name).is_dir():
                raise TaskExecutionError(f"frozen {label} source root 无效: {source_root}")
            try:
                actual_hash = sha256_directory_tree(source_root)
            except ValueError as error:
                raise TaskExecutionError(
                    f"frozen {label} source tree 无效: {source_root}"
                ) from error
            if actual_hash != expected_hash:
                raise TaskExecutionError(f"frozen {label} source tree 与 provenance 不一致")


def _read_json_mapping(path: Path, label: str) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TaskExecutionError(f"{label} 无法读取: {path}") from error
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise TaskExecutionError(f"{label} 必须是 JSON 对象")
    return raw


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _required_mapping(data: Mapping[str, object], name: str) -> Mapping[str, object]:
    value = data.get(name)
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise TaskExecutionError(f"A-only source 缺少对象字段: {name}")
    return value


def _source_output_root(source_manifest: Path, task_id: str) -> Path:
    try:
        source_output = source_manifest.parents[2]
    except IndexError as error:
        raise TaskExecutionError("A-only source manifest 路径层级无效") from error
    expected = source_output / "tasks" / task_id / "manifest.json"
    if source_manifest != expected:
        raise TaskExecutionError("A-only source manifest 不在标准 task 输出目录")
    return source_output


def _source_attempt_records(
    source: Mapping[str, object], attempts: tuple[AttemptManifest, ...]
) -> tuple[AttemptExecutionRecord, ...]:
    raw_records = source.get("attempt_results")
    if not isinstance(raw_records, list) or len(raw_records) != len(attempts):
        raise TaskExecutionError("A-only source attempt_results 数量无效")
    records: list[AttemptExecutionRecord] = []
    for index, (raw, expected) in enumerate(zip(raw_records, attempts, strict=True)):
        if not isinstance(raw, Mapping):
            raise TaskExecutionError("A-only source attempt_results 含无效记录")
        if raw.get("attempt_id") != expected.attempt_id or raw.get("variant") != expected.variant:
            raise TaskExecutionError("A-only source attempt_results identity 不一致")
        if index == 0:
            if raw.get("status") not in {"passed", "agent_failed"}:
                raise TaskExecutionError("A-only source A 臂未形成有效实验结果")
            if not isinstance(raw.get("reason"), str):
                raise TaskExecutionError("A-only source A 臂缺少 reason")
            if not isinstance(raw.get("verifier_passed"), bool):
                raise TaskExecutionError("A-only source A 臂缺少 verifier_passed")
        elif any(
            (
                raw.get("status") != "not_run",
                raw.get("reason") != "A_ONLY",
                raw.get("result_path") is not None,
                raw.get("gate_path") is not None,
            )
        ):
            raise TaskExecutionError("A-only source 中存在已执行或状态异常的 B 臂")
        result_path = raw.get("result_path")
        gate_path = raw.get("gate_path")
        progress_path = raw.get("progress_path")
        claim_observation = raw.get("claim_observation")
        if result_path is not None and not isinstance(result_path, str):
            raise TaskExecutionError("A-only source result_path 无效")
        if gate_path is not None and not isinstance(gate_path, str):
            raise TaskExecutionError("A-only source gate_path 无效")
        if progress_path is not None and not isinstance(progress_path, str):
            raise TaskExecutionError("A-only source progress_path 无效")
        if claim_observation is not None and not isinstance(claim_observation, dict):
            raise TaskExecutionError("A-only source claim_observation 无效")
        status = raw.get("status")
        reason = raw.get("reason")
        if not isinstance(status, str) or not isinstance(reason, str):
            raise TaskExecutionError("A-only source attempt status/reason 无效")
        verifier_passed = raw.get("verifier_passed")
        if verifier_passed is not None and not isinstance(verifier_passed, bool):
            raise TaskExecutionError("A-only source verifier_passed 无效")
        result_hash = raw.get("result_hash")
        gate_hash = raw.get("gate_hash")
        if index == 0 and (
            not _is_sha256(result_hash) or not _is_sha256(gate_hash)
        ):
            raise TaskExecutionError("A-only source A 臂缺少 result/gate hash")
        if result_hash is not None and not _is_sha256(result_hash):
            raise TaskExecutionError("A-only source result_hash 无效")
        if gate_hash is not None and not _is_sha256(gate_hash):
            raise TaskExecutionError("A-only source gate_hash 无效")
        records.append(
            AttemptExecutionRecord(
                attempt_id=expected.attempt_id,
                variant=expected.variant,
                status=status,
                reason=reason,
                result_path=result_path,
                gate_path=gate_path,
                verifier_passed=verifier_passed,
                claim_observation=claim_observation,
                progress_path=progress_path,
                result_hash=result_hash,
                gate_hash=gate_hash,
            )
        )
    return tuple(records)


def _validate_source_a_record(
    record: AttemptExecutionRecord,
    attempt: AttemptManifest,
    source_output: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    attempt_output = source_output / "attempts" / attempt.attempt_id / "output"
    expected_result = attempt_output / "attempt-result.json"
    expected_gate = attempt_output / "gate.json"
    if record.result_path is None or Path(record.result_path).resolve() != expected_result:
        raise TaskExecutionError("A-only source A result_path 越出冻结 attempt 目录")
    if record.gate_path is None or Path(record.gate_path).resolve() != expected_gate:
        raise TaskExecutionError("A-only source A gate_path 越出冻结 attempt 目录")
    result = _read_json_mapping(expected_result, "A-only A attempt result")
    gate = _read_json_mapping(expected_gate, "A-only A Gate")
    if record.result_hash != _sha256_file(expected_result):
        raise TaskExecutionError("A-only A attempt result 内容已漂移")
    if record.gate_hash != _sha256_file(expected_gate):
        raise TaskExecutionError("A-only A Gate 内容已漂移")
    if result.get("attempt_id") != attempt.attempt_id or result.get("variant") != "A":
        raise TaskExecutionError("A-only A attempt result identity 不一致")
    if gate.get("attempt_id") != attempt.attempt_id or gate.get("decision") != "pass":
        raise TaskExecutionError("A-only A Gate identity 或 decision 无效")
    return result, gate


def _validate_source_claim_bundle(
    bundle_path: Path,
    attempt_id: str,
    source_result: Mapping[str, object],
) -> tuple[str, tuple[str, ...]]:
    metadata_path = bundle_path.with_name(bundle_path.name + ".manifest.json")
    metadata = _read_json_mapping(metadata_path, "A-only claim bundle manifest")
    bundle_hash, content_hashes = _frozen_bundle_evidence(bundle_path)
    if metadata.get("schema_version") != 1 or metadata.get("attempt_id") != attempt_id:
        raise TaskExecutionError("A-only claim bundle manifest identity 无效")
    if metadata.get("bundle_hash") != bundle_hash:
        raise TaskExecutionError("A-only claim bundle 内容已漂移")
    producer = metadata.get("producer_verification")
    expected_producer_fields = {
        "attempt_id",
        "verifier_passed",
        "attempt_result_sha256",
    }
    if not isinstance(producer, Mapping) or set(producer) != expected_producer_fields:
        raise TaskExecutionError("A-only claim bundle 缺少完整 producer_verification")
    verifier_passed = source_result.get("verifier_passed")
    producer_passed = producer.get("verifier_passed")
    producer_result_hash = producer.get("attempt_result_sha256")
    if (
        producer.get("attempt_id") != attempt_id
        or not isinstance(producer_passed, bool)
        or producer_passed != verifier_passed
        or not _is_sha256(producer_result_hash)
    ):
        raise TaskExecutionError("A-only claim bundle producer verifier identity 不一致")
    attempt_result_path = (
        bundle_path.parents[2] / "attempts" / attempt_id / "output" / "attempt-result.json"
    )
    if producer_result_hash != _sha256_file(attempt_result_path):
        raise TaskExecutionError("A-only claim bundle producer verifier 证据已漂移")
    raw_claims = json.loads(bundle_path.read_text(encoding="utf-8")).get("claims")
    metadata_claims = metadata.get("claims")
    if not isinstance(raw_claims, list) or not isinstance(metadata_claims, list):
        raise TaskExecutionError("A-only claim bundle claims schema 无效")
    if len(content_hashes) != len(raw_claims) or len(metadata_claims) != len(raw_claims):
        raise TaskExecutionError("A-only claim bundle 含重复或缺失 claim")
    claim_ids: list[str] = []
    for raw_claim, raw_metadata in zip(raw_claims, metadata_claims, strict=True):
        if not isinstance(raw_claim, Mapping) or not isinstance(raw_metadata, Mapping):
            raise TaskExecutionError("A-only claim bundle 含无效 claim 记录")
        claim_id = raw_claim.get("id")
        if not isinstance(claim_id, str) or not claim_id:
            raise TaskExecutionError("A-only claim bundle claim.id 无效")
        if raw_metadata.get("claim_id") != claim_id:
            raise TaskExecutionError("A-only claim bundle claim 顺序或 identity 漂移")
        if raw_metadata.get("content_hash") != _canonical_json_hash(raw_claim):
            raise TaskExecutionError("A-only claim bundle claim 内容已漂移")
        claim_ids.append(claim_id)
    rust_events = source_result.get("rust_events")
    if not isinstance(rust_events, str) or not Path(rust_events).is_absolute():
        raise TaskExecutionError("A-only A result 缺少绝对 rust_events 路径")
    ledger_path = Path(rust_events).resolve()
    source_attempt_output = bundle_path.parents[2] / "attempts" / attempt_id / "output"
    if not ledger_path.is_relative_to(source_attempt_output):
        raise TaskExecutionError("A-only event ledger 越出冻结 attempt 目录")
    if metadata.get("source_ledger_hash") != _sha256_file(ledger_path):
        raise TaskExecutionError("A-only event ledger 与 claim bundle manifest 不一致")
    events = tuple(event for event in read_rust_event_ledger(ledger_path) if event.attempt_id == attempt_id)
    barriers = tuple(event for event in events if event.event_type == "claim_freeze_barrier")
    if (
        len(barriers) != 1
        or barriers[0].seq != metadata.get("barrier_seq")
        or not events
        or events[-1] != barriers[0]
        or len(events) < 2
        or events[-2].event_type != "attempt_finished"
    ):
        raise TaskExecutionError("A-only freeze barrier 与 event ledger 不一致")
    return bundle_hash, tuple(claim_ids)


def _positive_int(values: dict[str, int], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"provenance.{key} 必须为正整数")
    return value


def _attempt_start_failure_reason(error: BaseException) -> str:
    if isinstance(error, KeyboardInterrupt):
        return "INTERRUPTED_BY_OPERATOR"
    return f"PIER_JOB_FAILURE:{error}"


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _file_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return -1


def _turn_event_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())
    except OSError:
        return 0


def _last_turn_event(path: Path) -> dict[str, object] | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, dict):
            continue
        event: dict[str, object] = {}
        for key in ("seq", "turn_id", "created_at", "kind", "name", "summary"):
            value = raw.get(key)
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                event[key] = value
        return event
    return None


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _attempt_record_dict(record: AttemptExecutionRecord) -> dict[str, object]:
    """manifest 绑定 attempt result/Gate 内容，不只记录可被替换的绝对路径。"""
    raw = record.to_dict()
    if record.result_path is not None:
        raw["result_hash"] = _sha256_file(Path(record.result_path))
    if record.gate_path is not None:
        raw["gate_hash"] = _sha256_file(Path(record.gate_path))
    return raw


def _atomic_write_text(path: Path, content: str) -> None:
    """同目录 replace，避免崩溃时留下半写配置或 manifest。"""
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        raise ValueError(f"待计算 SHA-256 的文件不存在: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _frozen_bundle_evidence(path: Path) -> tuple[str, dict[str, str]]:
    """按 Rust 对齐的 canonical JSON 计算 raw bundle 与逐 claim 内容 hash。"""
    try:
        raw_bytes = path.read_bytes()
        raw = json.loads(raw_bytes)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"frozen claim bundle 无法读取: {path}") from error
    claims = raw.get("claims") if isinstance(raw, dict) else None
    if not isinstance(claims, list):
        raise ValueError("frozen claim bundle.claims 必须是数组")
    hashes: dict[str, str] = {}
    for claim in claims:
        if not isinstance(claim, dict) or not isinstance(claim.get("id"), str):
            raise ValueError("frozen claim bundle 含无效 claim")
        hashes[claim["id"]] = _canonical_json_hash(claim)
    return hashlib.sha256(raw_bytes).hexdigest(), hashes


def _canonical_json_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _task_network_disabled(task_toml: Path) -> tuple[bool, bool]:
    try:
        raw = tomllib.loads(task_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return False, False
    environment = raw.get("environment")
    verifier = raw.get("verifier")
    verifier_environment = verifier.get("environment") if isinstance(verifier, dict) else None
    return (
        isinstance(environment, dict) and environment.get("allow_internet") is False,
        isinstance(verifier_environment, dict)
        and verifier_environment.get("allow_internet") is False,
    )


def _pier_task_matches_attempt(
    task_toml: Path, attempt_task_id: str, pier_task_name: str
) -> bool:
    """同时核验稳定 task_id 与 Pier 写入的 namespaced task.name。"""
    try:
        raw = tomllib.loads(task_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return False
    task = raw.get("task")
    metadata = raw.get("metadata")
    if not isinstance(task, dict) or not isinstance(metadata, dict):
        return False
    expected_task_name = task.get("name")
    configured_task_id = metadata.get("task_id")
    return (
        isinstance(expected_task_name, str)
        and isinstance(configured_task_id, str)
        and configured_task_id == attempt_task_id
        and pier_task_name == expected_task_name
    )


def _claim_visibility_matches_variant(attempt_toml: Path, variant: str) -> bool:
    try:
        raw = tomllib.loads(attempt_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return False
    claim_bundle = raw.get("claim_bundle")
    if variant in CLAIM_BUNDLE_VARIANTS:
        return claim_bundle == "/opt/acn-eval/claims.json"
    return claim_bundle is None


def _attempt_model_egress_matches(attempt_toml: Path, expected_mode: str) -> bool:
    """Gate 复读冻结 attempt TOML，拒绝环境变量隐式改变模型出口。"""
    try:
        raw = tomllib.loads(attempt_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return False
    return raw.get("model_egress_mode") == expected_mode


def _claim_observation(
    variant: str,
    router_evidence: tuple[RouterEvidence, ...],
    claim_used_ids: tuple[str, ...],
    frozen_claim_content_hashes: dict[str, str],
) -> dict[str, object] | None:
    """输出按 attempt 可审计的 claim 消费漏斗，供 aggregate 汇总而非推断。"""
    if variant not in CLAIM_BUNDLE_VARIANTS:
        return None
    injected_ids = {
        claim_id
        for evidence in router_evidence
        for claim_id in evidence.injected_claim_ids
    }
    used_ids = set(claim_used_ids)
    return {
        "delivery": "forced" if variant == "B_forced_claim" else "on_demand",
        "delivery_evidence_count": len(router_evidence),
        "bundle_available": bool(frozen_claim_content_hashes),
        "retrieved": bool(router_evidence),
        "injected": bool(injected_ids),
        "used": bool(used_ids),
        "used_attribution": "finalize_recap_self_report",
        "injected_claim_count": len(injected_ids),
        "used_claim_count": len(used_ids),
    }
