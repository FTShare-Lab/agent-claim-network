import unittest

from acn_deepswe.assets import frozen_coding_benchmark_skill


class AssetTests(unittest.TestCase):
    def test_coding_benchmark_skill_is_hashed_stable_and_contains_no_task_answer(self) -> None:
        first = frozen_coding_benchmark_skill()
        second = frozen_coding_benchmark_skill()
        content = (first.source_path / "SKILL.md").read_text(encoding="utf-8")
        self.assertEqual(first.content_hash, second.content_hash)
        self.assertEqual(len(first.content_hash), 64)
        self.assertIn("读题", content)
        self.assertIn("最小修复", content)
        self.assertIn("不得写入题目答案", content)
        self.assertIn("不得根据 claim", content)
