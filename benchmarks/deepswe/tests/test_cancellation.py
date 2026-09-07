"""验证实验取消可回收真实子进程，并阻止占用并发槽的排队任务。"""
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from acn_deepswe.host_runner import (
    OPERATOR_INTERRUPT, Task1HostRunner, _run_interruptible_pier,
)
from acn_deepswe.presmoke import PresmokeHostRunner


class CancellationTests(unittest.TestCase):
    def setUp(self):
        OPERATOR_INTERRUPT.clear()
        self.addCleanup(OPERATOR_INTERRUPT.clear)

    def test_cancelled_process_runs_cleanup_and_is_reaped(self):
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory) / 'ready'
            cleaned = Path(directory) / 'cleaned'
            program = ('import signal, time, sys\nfrom pathlib import Path\n'
                       'def stop(*args):\n Path(sys.argv[2]).write_text("cleaned")\n sys.exit(143)\n'
                       'signal.signal(signal.SIGTERM, stop)\n'
                       'Path(sys.argv[1]).write_text("ready")\nwhile True: time.sleep(1)\n')
            result = []
            worker = threading.Thread(target=lambda: result.append(_run_interruptible_pier(
                [sys.executable, '-c', program, str(ready), str(cleaned)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)))
            worker.start()
            self.addCleanup(lambda: (OPERATOR_INTERRUPT.set(), worker.join(35)))
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists())
            OPERATOR_INTERRUPT.set()
            worker.join(5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(result[0].returncode, 143)
            self.assertEqual(cleaned.read_text(), 'cleaned')

    def test_cancelled_before_launch_does_not_spawn(self):
        OPERATOR_INTERRUPT.set()
        with patch('acn_deepswe.host_runner.subprocess.Popen') as popen:
            result = _run_interruptible_pier(['unused'])
            self.assertNotEqual(result.returncode, 0)
            popen.assert_not_called()

    def test_waiting_attempt_does_not_launch_or_release_foreign_slot(self):
        runner = Task1HostRunner.__new__(Task1HostRunner)
        runner._attempt_semaphore = threading.BoundedSemaphore(1)
        runner._attempt_semaphore.acquire()
        runner._run_one_attempt_unbounded = Mock()
        failures = []
        def wait_for_slot():
            result = runner._run_one_attempt(Mock(attempt_id='attempt', variant='A'), None)
            failures.append(result.reason)
        worker = threading.Thread(target=wait_for_slot)
        worker.start()
        OPERATOR_INTERRUPT.set()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, ['INTERRUPTED_BY_OPERATOR'])
        runner._run_one_attempt_unbounded.assert_not_called()
        self.assertFalse(runner._attempt_semaphore.acquire(blocking=False))
        runner._attempt_semaphore.release()

    def test_queued_task_does_not_construct_runner(self):
        runner = PresmokeHostRunner.__new__(PresmokeHostRunner)
        runner._task_runner_factory = Mock()
        spec = Mock(task_id='task', manifest_path=Path('/tmp/task-manifest.json'))
        OPERATOR_INTERRUPT.set()
        result = runner._run_task(spec, True)
        self.assertEqual(result.error, 'INTERRUPTED_BY_OPERATOR')
        runner._task_runner_factory.assert_not_called()

if __name__ == '__main__':
    unittest.main()
