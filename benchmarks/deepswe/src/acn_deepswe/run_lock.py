"""评测运行的跨进程互斥锁。

同一输出根目录只能有一个编排器或阶段执行器写入，避免并发续跑覆盖 checkpoint。
"""

from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path


class RunLockError(ValueError):
    """已有进程持有同一评测运行锁。"""


@contextmanager
def exclusive_run_lock(path: Path, description: str) -> Generator[None, None, None]:
    """非阻塞获取由绝对路径定位的 advisory lock，并在退出时可靠释放。"""
    if not path.is_absolute():
        raise ValueError("评测运行锁路径必须为绝对路径")
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        raise RunLockError(f"{description} 锁文件无法安全打开: {path}") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise RunLockError(f"{description} 锁文件不是当前用户拥有的普通文件: {path}")
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RunLockError(f"{description} 已由另一个进程运行: {path}") from error
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"pid={os.getpid()}\n".encode("ascii"))
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
