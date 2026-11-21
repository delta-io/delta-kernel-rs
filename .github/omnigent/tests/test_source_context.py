"""Tests for the AI review source-context tools."""

from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_source_context():
    path = Path(__file__).parents[1] / "source_context.py"
    spec = importlib.util.spec_from_file_location("source_context", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SourceContextTest(unittest.TestCase):
    def setUp(self):
        self.source_context = _load_source_context()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.roots = {}
        env = {}
        for repository in ("pr", "delta"):
            root = self.root / repository
            root.mkdir()
            self.roots[repository] = root
            env[f"{repository.upper()}_SOURCE_ROOT"] = str(root)
        environment = patch.dict(os.environ, env)
        environment.start()
        self.addCleanup(environment.stop)

    def test_read_list_and_search_are_confined_to_each_root(self):
        secret = self.root / "secret.txt"
        secret.write_text("do not expose\n", encoding="utf-8")
        secret_dir = self.root / "secret-dir"
        secret_dir.mkdir()
        (secret_dir / "hidden.txt").write_text("directory symlink secret\n", encoding="utf-8")
        for repository, root in self.roots.items():
            with self.subTest(repository=repository):
                (root / "src").mkdir()
                (root / "src" / "reference.md").write_text(
                    f"# {repository}\nMUST retain context\n", encoding="utf-8"
                )
                (root / "outside").symlink_to(secret)
                (root / "outside-dir").symlink_to(secret_dir, target_is_directory=True)
                self.assertEqual(
                    self.source_context.read_source_file(repository, "src/reference.md", 2, 1),
                    "2\tMUST retain context\nEnd of file.",
                )
                self.assertEqual(
                    self.source_context.list_source_files(repository), "src/reference.md"
                )
                self.assertEqual(
                    self.source_context.search_source_code(repository, "MUST"),
                    "src/reference.md:2: MUST retain context",
                )
                for path in (
                    "../secret.txt", str(secret), "outside", "outside-dir/hidden.txt", ".git/config"
                ):
                    with self.subTest(path=path):
                        self.assertTrue(
                            self.source_context.read_source_file(repository, path).startswith(
                                "Error:"
                            )
                        )
                        self.assertTrue(
                            self.source_context.list_source_files(repository, path).startswith(
                                "Error:"
                            )
                        )
                        self.assertTrue(
                            self.source_context.search_source_code(
                                repository, "secret", path
                            ).startswith("Error:")
                        )
                self.assertEqual(
                    self.source_context.search_source_code(repository, "directory symlink secret"),
                    "",
                )

    def test_pages_reconstruct_complete_text_at_character_and_line_limits(self):
        lines = [f"{index}: " + "reference\ttext " * 20 for index in range(450)]
        lines[17] = ""
        original = "\n".join(lines) + "\n"
        for repository, root in self.roots.items():
            (root / "long.md").write_text(original, encoding="utf-8")
            for line_count in (25, 400):
                with self.subTest(repository=repository, line_count=line_count):
                    reconstructed = []
                    start_line = 1
                    while True:
                        page = self.source_context.read_source_file(
                            repository, "long.md", start_line, line_count
                        )
                        self.assertLessEqual(len(page), 40_000)
                        numbered, footer = page.splitlines()[:-1], page.splitlines()[-1]
                        self.assertGreater(len(numbered), 0)
                        self.assertLessEqual(len(numbered), line_count)
                        for offset, line in enumerate(numbered):
                            number, text = line.split("\t", 1)
                            self.assertEqual(int(number), start_line + offset)
                            reconstructed.append(text)
                        if footer == "End of file.":
                            break
                        self.assertEqual(footer, f"Next line: {start_line + len(numbered)}")
                        start_line += len(numbered)
                    self.assertEqual("\n".join(reconstructed) + "\n", original)

    def test_line_numbers_match_git_when_a_file_has_other_line_break_characters(self):
        path = self.roots["pr"] / "breaks.rs"
        path.write_bytes(
            "first\x0cstill first\u2028still first\r\nsecond\rstill second\n\nfourth".encode()
        )
        self.assertEqual(
            self.source_context.read_source_file("pr", "breaks.rs"),
            "1\tfirst\x0cstill first\u2028still first\n2\tsecond\rstill second\n3\t\n"
            "4\tfourth\nEnd of file.",
        )
        self.assertEqual(
            self.source_context.search_source_code("pr", "fourth"), "breaks.rs:4: fourth"
        )

    def test_eof_and_requested_line_count(self):
        path = self.roots["pr"] / "short.md"
        path.write_text("first\nlast\n", encoding="utf-8")
        read = self.source_context.read_source_file
        self.assertEqual(read("pr", "short.md", 1, 1), "1\tfirst\nNext line: 2")
        self.assertEqual(read("pr", "short.md", 2, 1), "2\tlast\nEnd of file.")
        self.assertEqual(read("pr", "short.md", 3), "End of file.")
        path.write_text("", encoding="utf-8")
        self.assertEqual(read("pr", "short.md"), "End of file.")

    def test_single_line_fills_output_budget_or_returns_error(self):
        path = self.roots["pr"] / "wide.md"
        text = "x" * (40_000 - len("1\t\nEnd of file."))
        path.write_text(text, encoding="utf-8")
        page = self.source_context.read_source_file("pr", "wide.md")
        self.assertEqual(page, f"1\t{text}\nEnd of file.")
        self.assertEqual(len(page), 40_000)
        path.write_text(text + "x", encoding="utf-8")
        self.assertIn(
            "Error: source line 1 exceeds",
            self.source_context.read_source_file("pr", "wide.md"),
        )

    def test_oversized_line_in_mid_file_can_be_skipped(self):
        path = self.roots["pr"] / "wide.md"
        path.write_text("first\n" + "x" * 40_000 + "\nlast\n", encoding="utf-8")
        read = self.source_context.read_source_file
        self.assertEqual(read("pr", "wide.md"), "1\tfirst\nNext line: 2")
        self.assertEqual(
            read("pr", "wide.md", 2),
            "Error: source line 2 exceeds the 40000-character output limit; continue from line 3",
        )
        self.assertEqual(read("pr", "wide.md", 3), "3\tlast\nEnd of file.")

    def test_missing_binary_and_oversized_files_are_rejected(self):
        for repository, root in self.roots.items():
            with self.subTest(repository=repository):
                read = self.source_context.read_source_file
                self.assertTrue(read(repository, "missing.md").startswith("Error:"))
                (root / "binary.md").write_bytes(b"\x00")
                self.assertIn("binary files are not supported", read(repository, "binary.md"))
                (root / "large.md").write_bytes(b"x" * 2_000_001)
                self.assertIn("file exceeds", read(repository, "large.md"))

    def test_invalid_pagination_and_repository_are_rejected(self):
        (self.roots["pr"] / "short.md").write_text("first\n", encoding="utf-8")
        for start_line, line_count in ((0, 1), (1, 0), (1, 1001)):
            with self.subTest(start_line=start_line, line_count=line_count):
                self.assertIn(
                    "Error: start_line must be positive",
                    self.source_context.read_source_file(
                        "pr", "short.md", start_line, line_count
                    ),
                )
        self.assertIn(
            "repository must be", self.source_context.read_source_file("unknown", "missing.md")
        )
        with patch.dict(os.environ, {"PR_SOURCE_ROOT": ""}):
            self.assertIn(
                "PR_SOURCE_ROOT is not configured",
                self.source_context.read_source_file("pr", "missing.md"),
            )
