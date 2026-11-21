"""Tests for the AI review source-context tools."""

from __future__ import annotations

import importlib.util
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
    def test_read_list_and_search_are_confined_to_source_roots(self) -> None:
        source_context = _load_source_context()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pr_root = root / "pr"
            delta_root = root / "delta"
            pr_root.mkdir()
            delta_root.mkdir()
            (pr_root / "src").mkdir()
            (pr_root / "src" / "lib.rs").write_text("fn visible() {}\nfn target() {}\n")
            (delta_root / "PROTOCOL.md").write_text("# Protocol\nMUST retain context\n")
            secret = root / "secret.txt"
            secret.write_text("do not expose\n")
            (pr_root / "outside").symlink_to(secret)
            secret_dir = root / "secret-dir"
            secret_dir.mkdir()
            (secret_dir / "hidden.txt").write_text("directory symlink secret\n")
            (pr_root / "outside-dir").symlink_to(secret_dir, target_is_directory=True)

            with patch.dict(
                "os.environ", {"PR_SOURCE_ROOT": str(pr_root), "DELTA_SOURCE_ROOT": str(delta_root)}
            ):
                self.assertIn(
                    "2\tfn target() {}", source_context.read_source_file("pr", "src/lib.rs", 2, 1)
                )
                self.assertEqual(source_context.list_source_files("pr"), "src/lib.rs")
                self.assertEqual(
                    source_context.search_source_code("delta", "MUST"),
                    "PROTOCOL.md:2: MUST retain context",
                )
                self.assertTrue(
                    source_context.read_source_file("pr", "../secret.txt").startswith("Error:")
                )
                self.assertTrue(
                    source_context.read_source_file("pr", "outside").startswith("Error:")
                )
                self.assertNotIn("outside-dir", source_context.list_source_files("pr"))
                self.assertEqual(
                    source_context.search_source_code("pr", "directory symlink secret"), ""
                )
