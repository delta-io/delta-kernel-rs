"""Supported prefixes return the exact suffix boundary; unsupported descriptions return None."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from stack_description import stack_prefix_end  # noqa: E402

REPOSITORY = "delta-io/delta-kernel-rs"
PR_URL = f"https://github.com/{REPOSITORY}/pull"
SUFFIX = "## What changes are proposed in this pull request?\r\n\r\nBody é \U0001f95e\r\n"


def _row(number: int, label: str, indent: int = 0, marker: str = "", files: str = "") -> str:
    return (
        f"{' ' * indent}- [{label}]({PR_URL}/{number})"
        f" [[Files changed]({PR_URL}/{number}/files{files})]{marker}"
    )


def _block(
    heading: str = "## \U0001f95e Stacked PR",
    intro_files: str = "",
    rows: list[str] | None = None,
    blank: str = "\n",
    terminator: str = "---------\n",
    newline: str = "\n",
) -> str:
    if rows is None:
        rows = [_row(2, "**stack/child**", marker=" <- _this PR_")]
    intro = f"Use this [link]({PR_URL}/2/files{intro_files}) to review incremental changes."
    return newline.join([heading, intro, *rows]) + newline + blank + terminator


def _end(body: str, number: int = 2) -> int | None:
    return stack_prefix_end(body, REPOSITORY, number)


class StackPrefixTest(unittest.TestCase):
    def assert_prefix(self, prefix: str, suffix: str = SUFFIX) -> None:
        body = prefix + suffix
        end = _end(body)
        self.assertEqual(end, len(prefix))
        self.assertEqual(body[end:], suffix)

    def assert_unchanged(self, body: str) -> None:
        self.assertIsNone(_end(body))

    def test_pancake_heading_returns_prefix_end(self) -> None:
        self.assert_prefix(_block())

    def test_unsupported_headings_return_none(self) -> None:
        cases = {
            "plain heading": "## Stacked PR",
            "extra space": "##  \U0001f95e Stacked PR",
            "wrong heading level": "# \U0001f95e Stacked PR",
            "plural heading": "## \U0001f95e Stacked PRs",
            "wrong case": "## \U0001f95e stacked pr",
            "missing heading": "",
        }
        for scenario, heading in cases.items():
            with self.subTest(scenario=scenario):
                self.assert_unchanged(_block(heading=heading) + SUFFIX)

    def test_crlf_and_mixed_line_endings_preserve_suffix(self) -> None:
        cases = {
            "CRLF throughout": _block(newline="\r\n", blank="\r\n", terminator="---------\r\n"),
            "CRLF rows, LF rule": _block(newline="\r\n", blank="\n", terminator="---------\n"),
            "LF rows, CRLF rule": _block(newline="\n", blank="\r\n", terminator="---------\r\n"),
        }
        for scenario, prefix in cases.items():
            with self.subTest(scenario=scenario):
                self.assert_prefix(prefix)
        self.assert_prefix(_block(), suffix="\n\n\n   leading blanks\n")

    def test_suffix_may_be_empty(self) -> None:
        self.assert_prefix(_block(), suffix="")
        self.assert_prefix(_block(terminator="---------"), suffix="")

    def test_nested_rows_with_markers_and_ranges_return_prefix_end(self) -> None:
        sha = "a" * 40
        rows = [
            _row(1, "stack/parent", marker=" [MERGED]"),
            _row(2, "**stack/child**", indent=2, marker=" ← _this PR_", files=f"/{sha}..{sha}"),
            _row(3, "stack/grandchild", indent=4, files="/f4cb24eb..93cdbfa4"),
            _row(4, "stack/sibling", indent=2),
        ]
        self.assert_prefix(_block(rows=rows, intro_files=f"/{sha}..{sha}"))
        for marker in [" <- _this PR_", " <- this PR", " - _this PR_", " (this PR)"]:
            self.assert_prefix(_block(rows=[_row(2, "stack/child", marker=marker)]))

    def test_terminator_without_blank_line(self) -> None:
        self.assert_prefix(_block(blank=""))

    def test_blank_lines_outside_the_terminator_position_are_unchanged(self) -> None:
        self.assert_unchanged(_block().replace("\n", "\n\n", 1) + SUFFIX)
        self.assert_unchanged(_block(blank="\n\n") + SUFFIX)
        self.assert_unchanged(_block(blank=" \n") + SUFFIX)
        self.assert_unchanged(_block(rows=[_row(2, "stack/child"), ""]) + SUFFIX)

    def test_intro_variants_are_unchanged(self) -> None:
        intro = f"Use this [link]({PR_URL}/2/files) to review incremental changes."
        cases = {
            "wording": intro.replace("incremental changes", "this PR's incremental changes"),
            "wrong PR": intro.replace("/2/files", "/3/files"),
            "wrong repository": intro.replace(REPOSITORY, "other/repo"),
            "trailing space": intro + " ",
            "custom intro": f"Review the [incremental diff]({PR_URL}/2/files) for `stack/child`.",
        }
        for scenario, variant in cases.items():
            with self.subTest(scenario=scenario):
                self.assert_unchanged(_block().replace(intro, variant) + SUFFIX)

    def test_author_prose_inside_the_block_is_unchanged(self) -> None:
        rows = [_row(2, "stack/child", marker=" <- _this PR_"), "Splitting #1 per review feedback."]
        self.assert_unchanged(_block(rows=rows) + SUFFIX)
        self.assert_unchanged(_block(rows=["Each PR is based on main.", *rows[:1]]) + SUFFIX)
        self.assert_unchanged(_block(rows=[rows[0], "  - stack/unlinked-branch"]) + SUFFIX)
        self.assert_unchanged(_block(rows=[rows[0], "<!-- note -->"]) + SUFFIX)

    def test_malformed_rows_are_unchanged(self) -> None:
        good = _row(2, "stack/child", marker=" <- _this PR_")
        cases = {
            "wrong files label": good.replace("[[Files changed]", "[[Files]"),
            "mismatched PR and files links": good.replace("/2/files", "/3/files"),
            "foreign repository": good.replace(REPOSITORY, "other/repo", 1),
            "unbalanced bold label": _row(2, "**stack/child", marker=" <- _this PR_"),
            "space in branch label": _row(2, "stack child", marker=" <- _this PR_"),
            "short SHAs": _row(2, "stack/child", files="/abc..def"),
            "unknown marker": _row(2, "stack/child", marker=" (merged)"),
            "current PR marked merged": _row(2, "stack/child", marker=" [MERGED]"),
            "trailing space": good + " ",
            "asterisk list item": "* " + good[2:],
            "tab indentation": "\t" + good,
            "odd indentation": " " + good,
        }
        for scenario, bad in cases.items():
            with self.subTest(scenario=scenario):
                self.assert_unchanged(_block(rows=[bad]) + SUFFIX)
        parent = _row(1, "stack/parent")
        for rows in [
            [_row(1, "stack/parent", marker=" <- _this PR_"), good],
            [parent, _row(2, "stack/child", indent=4)],
            [parent, _row(2, "stack/child", indent=1)],
        ]:
            self.assert_unchanged(_block(rows=rows) + SUFFIX)

    def test_current_pr_identity_is_required(self) -> None:
        body = _block() + SUFFIX
        self.assertIsNone(stack_prefix_end(body, REPOSITORY, 3))
        self.assertIsNone(stack_prefix_end(body, "other/repo", 2))
        self.assert_unchanged(_block(rows=[_row(1, "stack/parent")]) + SUFFIX)
        twice = [_row(2, "stack/a"), _row(2, "stack/b", indent=2)]
        self.assert_unchanged(_block(rows=twice) + SUFFIX)

    def test_missing_or_different_terminator_is_unchanged(self) -> None:
        self.assert_unchanged(_block(terminator="") + SUFFIX)
        self.assert_unchanged(_block(terminator=""))
        self.assert_unchanged(_block(terminator="---\n") + SUFFIX)
        self.assert_unchanged(_block(terminator="----------\n") + SUFFIX)
        self.assert_unchanged(_block(terminator="--------- \n") + SUFFIX)
        self.assert_unchanged(_block(terminator="\n"))

    def test_block_must_start_at_offset_zero(self) -> None:
        self.assert_unchanged("\n" + _block() + SUFFIX)
        self.assert_unchanged(" " + _block() + SUFFIX)
        self.assert_unchanged("﻿" + _block() + SUFFIX)
        self.assert_unchanged("Intro paragraph.\n\n" + _block() + SUFFIX)
        self.assert_unchanged("```\n" + _block() + "```\n" + SUFFIX)
        self.assert_unchanged("> " + _block() + SUFFIX)

    def test_cleaned_description_is_unchanged(self) -> None:
        self.assert_unchanged(SUFFIX)
        self.assert_unchanged("")
        self.assert_unchanged("Discussion of the ## Stacked PR heading format.\n")

    def test_second_block_is_retained(self) -> None:
        first = _block()
        second = _block(newline="\r\n", blank="\r\n", terminator="---------\r\n")
        self.assert_prefix(first, suffix=second + SUFFIX)


if __name__ == "__main__":
    unittest.main()
