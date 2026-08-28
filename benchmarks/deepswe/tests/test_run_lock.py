import tempfile
import unittest
from pathlib import Path

from acn_deepswe.run_lock import RunLockError, exclusive_run_lock


class RunLockTests(unittest.TestCase):
    def test_symlink_lock_is_rejected_without_truncating_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.json"
            target.write_text("preserve-me", encoding="utf-8")
            lock = root / "run.lock"
            lock.symlink_to(target)

            with self.assertRaises(RunLockError):
                with exclusive_run_lock(lock, "fixture"):
                    pass

            self.assertEqual(target.read_text(encoding="utf-8"), "preserve-me")

    def test_second_descriptor_cannot_acquire_held_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "run.lock"
            with exclusive_run_lock(lock, "fixture"):
                with self.assertRaisesRegex(RunLockError, "另一个进程"):
                    with exclusive_run_lock(lock, "fixture"):
                        pass


if __name__ == "__main__":
    unittest.main()
