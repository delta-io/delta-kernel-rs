"""Scoped diff budgeting, Git path decoding and the bounded path-manifest reader."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1]))
try:
    import diff_coverage
finally:
    sys.path.pop(0)

BASE = "a" * 40
CHILD_BASE = "b" * 40
HEAD = "c" * 40


def file_diff(path: str = "file.rs", size: int | None = None) -> bytes:
    header = f"diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n"
    data = header.encode() + b"-old\n+new\n"
    if size is not None:
        if size < len(data):
            raise ValueError("fixture size is too short")
        if size > len(data):
            data += b" " * (size - len(data) - 1) + b"\n"
    return data


def stack_header(*, decorated: bool = False) -> str:
    title = "## 🥞 Stacked PR" if decorated else "## Stacked PR"
    return (
        title + "\nUse this [link](https://github.com/example/project/pull/2/files) "
        "to review incremental changes.\n"
        "- [parent](https://github.com/example/project/pull/1) "
        "[[Files changed](https://github.com/example/project/pull/1/files)] [MERGED]\n"
        "  - [**stack/child**](https://github.com/example/project/pull/2) "
        "[[Files changed](https://github.com/example/project/pull/2/files)] <- _this PR_\n"
        "\n---------\n"
    )


class DiffCoverageTest(unittest.TestCase):
    def test_exact_subagent_patch_budget_and_one_extra_byte_for_both_review_ranges(self):
        for base_sha in (BASE, CHILD_BASE):
            for size, complete in ((100_000, True), (100_001, False)):
                with self.subTest(base_sha=base_sha, size=size):
                    prepared = diff_coverage.prepare_review_inputs(
                        file_diff(size=size), "Author.", base_sha, HEAD
                    )
                    coverage = prepared["coverage"]
                    self.assertEqual(coverage["base_sha"], base_sha)
                    self.assertEqual(coverage["head_sha"], HEAD)
                    self.assertEqual(coverage["total_bytes"], size)
                    self.assertEqual(coverage["excerpt_bytes"], 100_000)
                    self.assertEqual(coverage["complete"], complete)
                    self.assertLessEqual(len(prepared["subagent_patch_excerpt_text"].encode()), 100_000)
                    self.assertEqual(coverage["partial_files"], int(not complete))

    def test_mid_file_cut_preserves_partial_and_omitted_paths_in_original_order(self):
        first = file_diff("first.rs", 90_000)
        second = file_diff("second.rs", 20_000)
        third = file_diff("last.rs")
        coverage = diff_coverage.prepare_review_inputs(first + second + third, "", BASE, HEAD)["coverage"]
        self.assertEqual(coverage["total_files"], 3)
        self.assertEqual(
            [item["path"] for item in coverage["files"]], ["first.rs", "second.rs", "last.rs"]
        )
        self.assertEqual(
            [item["status"] for item in coverage["files"]], ["full", "partial", "omitted"]
        )
        self.assertEqual([item["start"] for item in coverage["files"]], [0, 90_000, 110_000])
        self.assertEqual(coverage["files"][-1]["end"], len(first + second + third))
        self.assertEqual(
            (coverage["full_files"], coverage["partial_files"], coverage["omitted_files"]), (1, 1, 1)
        )

    def test_byte_limit_does_not_split_utf8_diff_or_description(self):
        raw = file_diff(size=99_999) + "é".encode()
        prepared = diff_coverage.prepare_review_inputs(raw, "é" * 5000, BASE, HEAD)
        self.assertEqual(prepared["coverage"]["excerpt_bytes"], 99_999)
        self.assertNotIn("�", prepared["subagent_patch_excerpt_text"])
        self.assertEqual(len(prepared["pr_description_excerpt_text"].encode()), 8192)
        self.assertTrue(prepared["pr_description_truncated"])
        self.assertEqual(diff_coverage.utf8_prefix("é".encode(), 1), "")

    def test_only_recognized_leading_generated_stack_header_is_removed(self):
        author = "## What changes are proposed?\nPreserve the author's explanation."
        for decorated in (False, True):
            for newline in ("\n", "\r\n"):
                with self.subTest(decorated=decorated, newline=newline):
                    header = stack_header(decorated=decorated).replace("\n", newline)
                    raw = header + author
                    prepared = diff_coverage.prepare_review_inputs(file_diff(), raw, BASE, HEAD)
                    self.assertEqual(prepared["pr_description_excerpt_text"], author)
                    self.assertTrue(raw.startswith(header))
                    self.assertFalse(prepared["pr_description_truncated"])

    def test_generated_navigation_preserves_unpublished_descendants_and_this_pr_suffix(self):
        for suffix in ("<- _this PR_", "(this PR)"):
            with self.subTest(suffix=suffix):
                header = stack_header().replace("<- _this PR_", suffix)
                header = header.replace("\n---------", "\n    - stack/unpublished-descendant\n---------")
                self.assertEqual(
                    diff_coverage.strip_stack_header(header + "Author context."), "Author context."
                )
        author = (
            "## Stacked PR\n"
            "- [parent](https://github.com/example/project/pull/1) "
            "[[Files changed](https://github.com/example/project/pull/1/files)]\n"
            "  - stack/author-example\n---\nAuthor context."
        )
        self.assertEqual(diff_coverage.strip_stack_header(author), author)

    def test_similar_author_headings_and_incomplete_generated_blocks_are_preserved(self):
        author_descriptions = (
            "## Stacked PR design\nMy explanation.\n---\nDetails.",
            "## Stacked PR\nMy explanation.\n---\nDetails.",
            "## Stacked PR\n- [parent](https://github.com/example/project/pull/1)\n---\nDetails.",
            stack_header().rsplit("---------", 1)[0],
            "Author context.\n" + stack_header(),
            "```markdown\n" + stack_header() + "```",
        )
        for author in author_descriptions:
            with self.subTest(author=author):
                self.assertEqual(diff_coverage.strip_stack_header(author), author)

    def test_author_text_is_budgeted_after_generated_navigation(self):
        raw = stack_header() + "A" * 8192
        prepared = diff_coverage.prepare_review_inputs(file_diff(), raw, BASE, HEAD)
        self.assertEqual(prepared["pr_description_excerpt_text"], "A" * 8192)
        self.assertFalse(prepared["pr_description_truncated"])
        prepared = diff_coverage.prepare_review_inputs(file_diff(), raw + "B", BASE, HEAD)
        self.assertTrue(prepared["pr_description_truncated"])

    def test_late_pr_hunk_reaches_subagents_and_orchestrator_gets_all_changed_paths(self):
        first = file_diff("first.rs", 85_000)
        late = file_diff("late_child.rs", 10_000)
        omitted = file_diff("omitted.rs", 10_000)
        raw = first + late + omitted
        prepared = diff_coverage.prepare_review_inputs(raw, "author", CHILD_BASE, HEAD)
        parent = diff_coverage.build_orchestrator_task_message("Instructions.", raw, prepared["coverage"])
        self.assertIn("diff --git a/late_child.rs", prepared["subagent_patch_excerpt_text"])
        self.assertIn('"late_child.rs"', parent)
        self.assertIn('"omitted.rs"', parent)
        self.assertNotIn("diff --git a/late_child.rs", parent)
        notice = parent.split("## Patch excerpts\n", 1)[1].split("## PR Diff", 1)[0]
        self.assertIn(
            f"This orchestrator task message includes 80000 of {len(raw)} UTF-8 patch bytes.",
            notice,
        )
        self.assertIn(
            f"Reviewer and Checker task messages include 100000 of {len(raw)} "
            "UTF-8 patch bytes in a separate excerpt.",
            notice,
        )
        self.assertLessEqual(len(parent.encode()), 120_000)

    def test_orchestrator_message_reserves_changed_paths_and_counts_utf8_bytes(self):
        raw = file_diff("é.rs", 100_001)
        coverage = diff_coverage.prepare_review_inputs(raw, "", BASE, HEAD)["coverage"]
        prefix = "é" * 30_000
        parent = diff_coverage.build_orchestrator_task_message(prefix, raw, coverage)
        self.assertTrue(parent.startswith(prefix))
        self.assertIn('"é.rs"', parent)
        self.assertLessEqual(len(parent.encode()), 120_000)
        with self.assertRaisesRegex(ValueError, "task text without the patch"):
            diff_coverage.build_orchestrator_task_message("a" * 120_001, raw, coverage)

        excerpt = parent.split("```diff\n", 1)[1].rsplit("\n```", 1)[0]
        excerpt_bytes = len(excerpt.encode("utf-8"))
        self.assertLess(excerpt_bytes, 80_000)
        notice = parent.split("## Patch excerpts\n", 1)[1].split("## PR Diff", 1)[0]
        self.assertIn(
            f"This orchestrator task message includes {excerpt_bytes} of 100001 UTF-8 patch bytes.",
            notice,
        )
        self.assertIn(
            "Reviewer and Checker task messages include 100000 of 100001 "
            "UTF-8 patch bytes in a separate excerpt.",
            notice,
        )

    def test_orchestrator_rejects_task_text_that_leaves_no_room_for_excerpt_counts(self):
        raw = file_diff()
        coverage = diff_coverage.prepare_review_inputs(raw, "", BASE, HEAD)["coverage"]
        with self.assertRaisesRegex(ValueError, "no room for patch excerpt byte counts"):
            diff_coverage.build_orchestrator_task_message("a" * 120_000, raw, coverage)

    def test_orchestrator_reports_zero_patch_bytes_when_only_metadata_and_notice_fit(self):
        raw = file_diff(size=100_001)
        coverage = diff_coverage.prepare_review_inputs(raw, "", BASE, HEAD)["coverage"]
        message = diff_coverage.build_orchestrator_task_message(
            "Instructions.", raw, coverage, limit=260
        )
        self.assertLessEqual(len(message.encode("utf-8")), 260)
        self.assertIn(
            "This orchestrator task message includes 0 of 100001 UTF-8 patch bytes.", message
        )
        self.assertNotIn("```diff", message)

    def test_huge_manifest_uses_paged_tool_and_all_paths_remain_retrievable(self):
        paths = [f"directory/{index:04d}/" + "x" * 170 + ".rs" for index in range(1300)]
        raw = b"".join(file_diff(path) for path in paths)
        coverage = diff_coverage.prepare_review_inputs(raw, "", BASE, HEAD)["coverage"]
        parent = diff_coverage.build_orchestrator_task_message("Instructions.", raw, coverage)
        self.assertIn("list_changed_paths", parent)
        self.assertLessEqual(len(parent.encode()), 120_000)
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "paths.json"
            manifest.write_text(json.dumps(coverage))
            actual = []
            offset = 0
            with patch.object(diff_coverage, "CHANGED_PATHS_FILE", manifest):
                while offset is not None:
                    response = diff_coverage.list_changed_paths(offset, 1000)
                    self.assertLessEqual(len(response.encode()), 32_000)
                    page = json.loads(response)
                    actual.extend(item["path"] for item in page["paths"])
                    self.assertEqual(page["base_sha"], BASE)
                    offset = page["next_offset"]
                self.assertEqual(
                    json.loads(diff_coverage.list_changed_paths(len(paths)))["paths"], []
                )
            self.assertEqual(actual, paths)

    def test_path_pages_reject_invalid_cursors_and_limits(self):
        for offset, maximum in ((-1, 10), (True, 10), ("0", 10), (0, 0), (0, 1001), (0, True)):
            with self.subTest(offset=offset, maximum=maximum), self.assertRaises(ValueError):
                diff_coverage.list_changed_paths(offset, maximum)

    def test_git_quoted_octal_utf8_and_c_escapes_are_decoded(self):
        quoted = b'"b/dir/caf\\303\\251\\tline\\nname\\\\quoted\\\".rs"'
        raw = (
            b'diff --git "a/old" ' + quoted + b'\n--- "a/old"\n+++ ' + quoted
            + b'\n@@ -1 +1 @@\n-old\n+new\n'
        )
        sections = diff_coverage.file_sections(raw)
        self.assertEqual(sections[0]["path"], 'dir/café\tline\nname\\quoted".rs')
        prepared = diff_coverage.prepare_review_inputs(raw, "", BASE, HEAD)
        parent = diff_coverage.build_orchestrator_task_message("Instructions.", raw, prepared["coverage"])
        self.assertIn("\\tline\\nname", parent)

    def test_rename_deletion_binary_and_space_paths_keep_one_section_each(self):
        raw = (
            b'diff --git a/old name b/new name\nsimilarity index 100%\n'
            b'rename from old name\nrename to new name\n'
            b'diff --git a/deleted.rs b/deleted.rs\n--- a/deleted.rs\n+++ /dev/null\n'
            b'@@ -1 +0,0 @@\n-deleted\n'
            b'diff --git a/binary image.png b/binary image.png\n'
            b'Binary files a/binary image.png and b/binary image.png differ\n'
            b'diff --git "a/old\\tname" "b/new\\tname"\n'
            b'similarity index 100%\nrename from "old\\tname"\nrename to "new\\tname"\n'
            b'diff --git a/a b/c b/a b/c\nGIT binary patch\nliteral 0\n'
        )
        paths = [section["path"] for section in diff_coverage.file_sections(raw)]
        self.assertEqual(
            paths, ["new name", "deleted.rs", "binary image.png", "new\tname", "a b/c"]
        )

    def test_empty_diff_is_complete_and_malformed_input_fails_explicitly(self):
        coverage = diff_coverage.prepare_review_inputs(b"", "", BASE, HEAD)["coverage"]
        self.assertTrue(coverage["complete"])
        self.assertEqual(coverage["total_files"], 0)
        with self.assertRaisesRegex(ValueError, "Git file sections"):
            diff_coverage.prepare_review_inputs(b"not a diff", "", BASE, HEAD)
        with self.assertRaisesRegex(ValueError, "UTF-8"):
            diff_coverage.prepare_review_inputs(file_diff() + b"\xff", "", BASE, HEAD)
        with self.assertRaisesRegex(ValueError, "SHAs"):
            diff_coverage.prepare_review_inputs(file_diff(), "", "bad", HEAD)

    def test_source_tool_wrapper_reads_installed_library_without_repo_imports(self):
        root = Path(__file__).parents[1]
        client = types.ModuleType("omnigent_client")
        client.tool = lambda function: function
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory)
            tools = bundle / "tools" / "python"
            library = bundle / "lib"
            tools.mkdir(parents=True)
            library.mkdir()
            wrapper = tools / "list_changed_paths.py"
            wrapper.write_bytes((root / "source_tools/list_changed_paths.py").read_bytes())
            (library / "diff_coverage.py").write_bytes((root / "diff_coverage.py").read_bytes())
            spec = importlib.util.spec_from_file_location("installed_changed_paths", wrapper)
            module = importlib.util.module_from_spec(spec)
            with patch.dict(sys.modules, {"omnigent_client": client}):
                spec.loader.exec_module(module)
            implementation = module._implementation()
            self.assertTrue(callable(implementation.list_changed_paths))
            self.assertEqual(
                implementation.CHANGED_PATHS_FILE, Path("/tmp/ai_review_changed_paths.json")
            )


if __name__ == "__main__":
    unittest.main()
