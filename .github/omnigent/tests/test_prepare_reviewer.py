"""Tests for expanding shared instruction blocks into a runnable reviewer bundle."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest

REVIEWER_DIR = Path(__file__).parents[1] / "reviewer"


def _load_prepare_reviewer():
    path = Path(__file__).parents[1] / "prepare_reviewer.py"
    spec = importlib.util.spec_from_file_location("prepare_reviewer", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PrepareReviewerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prepare_reviewer = _load_prepare_reviewer()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "reviewer"
        self.shared = self.source / "shared"
        self.shared.mkdir(parents=True)
        (self.shared / "review_rules.md").write_text("## Rules\n\nShared rule.\n", encoding="utf-8")

    def _expand(self, text: str) -> str:
        return self.prepare_reviewer.expand_instructions(text, self.shared)

    def test_placeholder_line_becomes_the_shared_block(self) -> None:
        self.assertEqual(
            self._expand("Role text.\n\n${review_rules}\n\nMore role text.\n"),
            "Role text.\n\n## Rules\n\nShared rule.\n\nMore role text.\n",
        )

    def test_text_that_is_not_a_whole_line_placeholder_is_unchanged(self) -> None:
        for text in (
            "Run `echo ${review_rules}` in a shell.\n",
            "${review_rules} trailing words\n",
            "Costs $5 and uses $HOME.\n",
        ):
            with self.subTest(text=text):
                self.assertEqual(self._expand(text), text)

    def test_line_that_only_resembles_a_placeholder_is_rejected(self) -> None:
        for text in ("  ${review_rules}\n", "${review_rules} \n", "${Review-Rules}\n", "${}\n"):
            with self.subTest(text=text):
                with self.assertRaisesRegex(ValueError, "not a valid placeholder line"):
                    self._expand(text)

    def test_placeholder_without_a_shared_block_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing_block"):
            self._expand("${missing_block}\n")

    def test_shared_block_that_contains_a_placeholder_is_rejected(self) -> None:
        (self.shared / "outer.md").write_text("${review_rules}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "outer.md"):
            self._expand("${outer}\n")

    def test_prepared_bundle_has_complete_instructions_and_leaves_the_source_unchanged(
        self,
    ) -> None:
        child = self.source / "agents" / "example-reviewer"
        child.mkdir(parents=True)
        (self.source / "REVIEW.md").write_text("Parent.\n\n${review_rules}\n", encoding="utf-8")
        (child / "REVIEW.md").write_text("${review_rules}\n\nChild.\n", encoding="utf-8")
        (child / "config.yaml").write_text("instructions: REVIEW.md\n", encoding="utf-8")
        destination = self.root / "prepared"

        self.prepare_reviewer.prepare_bundle(destination, self.source)

        self.assertEqual(
            (destination / "REVIEW.md").read_text(encoding="utf-8"),
            "Parent.\n\n## Rules\n\nShared rule.\n",
        )
        self.assertEqual(
            (destination / "agents" / "example-reviewer" / "REVIEW.md").read_text(encoding="utf-8"),
            "## Rules\n\nShared rule.\n\nChild.\n",
        )
        self.assertEqual(
            (destination / "agents" / "example-reviewer" / "config.yaml").read_text(
                encoding="utf-8"
            ),
            "instructions: REVIEW.md\n",
        )
        self.assertEqual(
            (child / "REVIEW.md").read_text(encoding="utf-8"), "${review_rules}\n\nChild.\n"
        )
        with self.assertRaises(FileExistsError):
            self.prepare_reviewer.prepare_bundle(destination, self.source)

    def test_every_checked_in_instruction_file_expands_completely(self) -> None:
        destination = self.root / "prepared"
        self.prepare_reviewer.prepare_bundle(destination)
        instruction_files = sorted(destination.rglob("REVIEW.md"))
        self.assertEqual(
            [path.relative_to(destination) for path in instruction_files],
            [path.relative_to(REVIEWER_DIR) for path in sorted(REVIEWER_DIR.rglob("REVIEW.md"))],
        )
        for path in instruction_files:
            with self.subTest(instructions=str(path.relative_to(destination))):
                text = path.read_text(encoding="utf-8")
                self.assertTrue(text.strip())
                self.assertNotRegex(text, r"(?m)^\$\{")

    def test_every_shared_block_is_used_by_an_instruction_file(self) -> None:
        instructions = "".join(
            path.read_text(encoding="utf-8") for path in REVIEWER_DIR.rglob("REVIEW.md")
        )
        for block in sorted((REVIEWER_DIR / "shared").glob("*.md")):
            with self.subTest(block=block.name):
                self.assertIn(f"${{{block.stem}}}\n", instructions)

    def test_maintainer_reviewers_share_one_instruction_text(self) -> None:
        claude, codex = (
            (REVIEWER_DIR / "agents" / name / "REVIEW.md").read_text(encoding="utf-8")
            for name in ("maintainer-claude-reviewer", "maintainer-codex-reviewer")
        )
        self.assertEqual(claude, codex)
        self.assertIn("${maintainer}\n", claude)
