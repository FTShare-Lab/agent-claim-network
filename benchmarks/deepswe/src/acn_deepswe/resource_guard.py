"""DeepSWE 正式运行的 Docker 全局互斥、容量门禁与定向遗留清理。"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

MIB = 1024 * 1024
_GENERATED_MAIN_IMAGE = re.compile(r"__[a-z0-9]{7}-main$")


def _global_docker_lock_path() -> Path:
    """优先使用当前用户私有的 runtime directory，避免共享 /tmp 名称劫持。"""
    runtime = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))
    try:
        metadata = runtime.stat()
    except OSError:
        metadata = None
    if (
        runtime.is_absolute()
        and metadata is not None
        and stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and metadata.st_mode & 0o077 == 0
    ):
        return runtime / "acn-deepswe-docker.lock"
    return Path(f"/tmp/acn-deepswe-docker-{os.getuid()}.lock")


GLOBAL_DOCKER_LOCK = _global_docker_lock_path()


class ResourceGuardError(ValueError):
    """宿主资源不足或 Docker 遗留资源不符合安全清理边界。"""


@dataclass(frozen=True)
class CleanupSummary:
    containers_removed: int
    image_references_removed: int


def verify_capacity(
    docker_info: Mapping[str, object],
    *,
    workers: int,
    resources: Mapping[str, int],
    host_capacity: Mapping[str, int],
    output_path: Path,
) -> Path:
    """按当前可用量验证 20-worker admission，并返回 Docker 数据根。"""
    cpus = _positive_int(docker_info.get("NCPU"), "Docker info.NCPU")
    docker_memory = _positive_int(docker_info.get("MemTotal"), "Docker info.MemTotal")
    docker_root_raw = docker_info.get("DockerRootDir")
    if not isinstance(docker_root_raw, str) or not docker_root_raw:
        raise ResourceGuardError("Docker info.DockerRootDir 必须是非空绝对路径")
    docker_root = Path(docker_root_raw)
    if not docker_root.is_absolute():
        raise ResourceGuardError("Docker info.DockerRootDir 必须是绝对路径")

    required_cpus = workers * resources["cpus"]
    required_memory = workers * resources["memory_mb"] * MIB
    memory_reserve = host_capacity["memory_reserve_mb"] * MIB
    # Docker Desktop / 远程 daemon 时 Docker info.MemTotal 已是 VM 的内存，宿主
    # /proc/meminfo 不存在或描述的是另一台机器；只有原生 Linux daemon 才叠加 MemAvailable。
    available_memory = docker_memory
    if _PROC_MEMINFO.exists():
        available_memory = min(docker_memory, _host_available_memory_bytes())
    if cpus < required_cpus or available_memory < required_memory + memory_reserve:
        raise ResourceGuardError(
            "宿主资源不足，拒绝静默降低 task_workers: "
            f"required_cpus={required_cpus}, available_cpus={cpus}, "
            f"required_memory_bytes={required_memory}, "
            f"memory_reserve_bytes={memory_reserve}, available_memory_bytes={available_memory}"
        )

    required_free_mb = (
        host_capacity["disk_reserve_mb"] + workers * host_capacity["disk_admission_mb_per_worker"]
    )
    verify_disk_headroom((output_path, docker_root), required_free_mb)
    return docker_root


def verify_disk_headroom(paths: Sequence[Path], required_free_mb: int) -> None:
    """对不同文件系统分别检查剩余空间；同一设备不重复计算。"""
    if required_free_mb <= 0:
        raise ResourceGuardError("required_free_mb 必须为正整数")
    checked_devices: set[int] = set()
    for original in paths:
        candidate = _existing_parent(original)
        try:
            device = candidate.stat().st_dev
            free = shutil.disk_usage(candidate).free
        except OSError as error:
            raise ResourceGuardError(f"无法读取磁盘容量: {original}") from error
        if device in checked_devices:
            continue
        checked_devices.add(device)
        required = required_free_mb * MIB
        if free < required:
            raise ResourceGuardError(
                "磁盘高水位门禁失败: "
                f"path={original}, required_free_bytes={required}, available_bytes={free}"
            )


def reject_running_containers() -> None:
    """正式评测独占 daemon；存在任意运行容器时 fail closed。"""
    completed = _docker(["ps", "-q"])
    running = [line for line in completed.stdout.splitlines() if line.strip()]
    if running:
        raise ResourceGuardError(
            f"Docker daemon 存在 {len(running)} 个运行中容器，正式评测拒绝并发占用"
        )


def cleanup_stale_pier_resources(
    protected_image_references: Collection[str] = (),
) -> CleanupSummary:
    """只删除已停止且有 Pier Compose 证据的容器及明确生成镜像。"""
    reject_running_containers()
    listed = _docker(["ps", "-aq", "--filter", "label=com.docker.compose.project"])
    container_ids = [line.strip() for line in listed.stdout.splitlines() if line.strip()]
    owned: list[str] = []
    owned_image_refs: set[str] = set()
    if container_ids:
        inspected = _docker(["inspect", *container_ids])
        try:
            containers = json.loads(inspected.stdout)
        except json.JSONDecodeError as error:
            raise ResourceGuardError("docker inspect 容器结果不是 JSON") from error
        if not isinstance(containers, list):
            raise ResourceGuardError("docker inspect 容器结果必须是数组")
        for item in containers:
            if not isinstance(item, Mapping):
                continue
            state = item.get("State")
            config = item.get("Config")
            labels = config.get("Labels") if isinstance(config, Mapping) else None
            config_files = (
                labels.get("com.docker.compose.project.config_files")
                if isinstance(labels, Mapping)
                else None
            )
            container_id = item.get("Id")
            if (
                isinstance(state, Mapping)
                and state.get("Running") is False
                and isinstance(config_files, str)
                and "/pier/" in config_files
                and isinstance(container_id, str)
                and container_id
            ):
                owned.append(container_id)
                image_ref = config.get("Image")
                if isinstance(image_ref, str):
                    normalized = _normalize_image_ref(image_ref)
                    if normalized is not None:
                        owned_image_refs.add(normalized)
    for batch in _batches(owned, 100):
        _docker(["rm", *batch])

    images = _docker(["image", "ls", "--format", "{{json .}}"])
    generated_refs: list[str] = []
    for line in images.stdout.splitlines():
        try:
            image = json.loads(line)
        except json.JSONDecodeError as error:
            raise ResourceGuardError("docker image ls 结果不是逐行 JSON") from error
        if not isinstance(image, Mapping):
            continue
        repository = image.get("Repository")
        tag = image.get("Tag")
        if (
            isinstance(repository, str)
            and isinstance(tag, str)
            and tag != "<none>"
            and _is_generated_image(
                repository,
                tag,
                owned_image_refs,
                protected_image_references,
            )
        ):
            generated_refs.append(f"{repository}:{tag}")
    for batch in _batches(generated_refs, 100):
        _docker(["image", "rm", *batch])
    return CleanupSummary(len(owned), len(generated_refs))


def cleanup_finished_trial_images(trial_name: str) -> int:
    """按 Pier 随机 trial 名精确回收 verifier/egress 派生镜像。"""
    normalized = trial_name.lower()
    if re.fullmatch(r"[a-z0-9][a-z0-9_.-]*__[a-z0-9]{7}", normalized) is None:
        raise ResourceGuardError("Pier trial_name 不符合可安全清理的冻结格式")
    allowed = {
        f"{normalized}-pier-egress-proxy",
        f"{normalized}__verifier__trial-main",
    }
    images = _docker(
        ["image", "ls", "--filter", f"reference={normalized}*", "--format", "{{json .}}"]
    )
    references: list[str] = []
    for line in images.stdout.splitlines():
        try:
            image = json.loads(line)
        except json.JSONDecodeError as error:
            raise ResourceGuardError("docker image ls trial 结果不是逐行 JSON") from error
        if not isinstance(image, Mapping):
            continue
        repository = image.get("Repository")
        tag = image.get("Tag")
        if isinstance(repository, str) and repository in allowed and isinstance(tag, str):
            if tag != "<none>":
                references.append(f"{repository}:{tag}")
    for batch in _batches(references, 100):
        _docker(["image", "rm", *batch])
    return len(references)


def _is_generated_image(
    repository: str,
    tag: str,
    owned_image_refs: set[str],
    protected_image_references: Collection[str],
) -> bool:
    reference = f"{repository}:{tag}"
    if reference in protected_image_references or repository.startswith("public.ecr."):
        return False
    return (
        "pier-egress-proxy" in repository
        or "__verifier__trial-main" in repository
        or (_GENERATED_MAIN_IMAGE.search(repository) is not None and reference in owned_image_refs)
    )


def _normalize_image_ref(value: str) -> str | None:
    """把容器 Config.Image 归一成 image ls 的 repository:tag；digest/ID 不参与删除。"""
    if not value or "@" in value or value.startswith("sha256:"):
        return None
    last_component = value.rsplit("/", 1)[-1]
    return value if ":" in last_component else f"{value}:latest"


def _docker(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            ["docker", *arguments], check=False, capture_output=True, text=True
        )
    except OSError as error:
        raise ResourceGuardError("无法执行 Docker CLI") from error
    if completed.returncode != 0:
        tail = completed.stderr[-4000:].strip()
        raise ResourceGuardError(
            f"Docker CLI 失败: docker {' '.join(arguments[:2])}; stderr_tail={tail}"
        )
    return completed


_PROC_MEMINFO = Path("/proc/meminfo")


def _host_available_memory_bytes() -> int:
    try:
        lines = _PROC_MEMINFO.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ResourceGuardError("无法读取 /proc/meminfo") from error
    for line in lines:
        if line.startswith("MemAvailable:"):
            fields = line.split()
            if len(fields) == 3 and fields[2] == "kB" and fields[1].isdigit():
                return int(fields[1]) * 1024
    raise ResourceGuardError("/proc/meminfo 缺少有效 MemAvailable")


def _existing_parent(path: Path) -> Path:
    candidate = path.resolve()
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise ResourceGuardError(f"路径及其父目录均不存在: {path}")
        candidate = parent
    return candidate


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ResourceGuardError(f"{field} 必须是正整数")
    return value


def _batches(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]
