"""Tests for recognizing the generated stack prefix of a PR description."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from stack_description import stack_prefix_end  # noqa: E402

REPOSITORY = "delta-io/delta-kernel-rs"
PR_URL = f"https://github.com/{REPOSITORY}/pull"
FIXTURE = Path(__file__).parent / "fixtures" / "stack_descriptions.json"
SUFFIX = "## What changes are proposed in this pull request?\r\n\r\nBody é \U0001f95e\r\n"


def _row(number: int, label: str, indent: int = 0, marker: str = "", files: str = "") -> str:
    return (
        f"{' ' * indent}- [{label}]({PR_URL}/{number})"
        f" [[Files changed]({PR_URL}/{number}/files{files})]{marker}"
    )


def _block(
    heading: str = "## Stacked PR",
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
        self.assertEqual(_end(body), len(prefix))
        self.assertEqual(body[len(prefix) :], suffix)

    def assert_unchanged(self, body: str) -> None:
        self.assertIsNone(_end(body))

    def test_plain_and_pancake_headings(self) -> None:
        self.assert_prefix(_block())
        self.assert_prefix(_block(heading="## \U0001f95e Stacked PR"))

    def test_other_headings_are_unchanged(self) -> None:
        for heading in ["##  Stacked PR", "# Stacked PR", "## Stacked PRs", "## stacked pr", ""]:
            self.assert_unchanged(_block(heading=heading) + SUFFIX)

    def test_crlf_and_mixed_line_endings_preserve_suffix(self) -> None:
        self.assert_prefix(_block(newline="\r\n", blank="\r\n", terminator="---------\r\n"))
        self.assert_prefix(_block(newline="\r\n", blank="\n", terminator="---------\n"))
        self.assert_prefix(_block(newline="\n", blank="\r\n", terminator="---------\r\n"))
        self.assert_prefix(_block(), suffix="\n\n\n   leading blanks\n")

    def test_suffix_may_be_empty(self) -> None:
        self.assert_prefix(_block(), suffix="")
        self.assert_prefix(_block(terminator="---------"), suffix="")

    def test_nested_rows_with_markers_and_ranges(self) -> None:
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
        self.assert_unchanged("## Stacked PR\n\n" + _block().split("\n", 1)[1] + SUFFIX)
        self.assert_unchanged(_block(blank="\n\n") + SUFFIX)
        self.assert_unchanged(_block(blank=" \n") + SUFFIX)
        self.assert_unchanged(_block(rows=[_row(2, "stack/child"), ""]) + SUFFIX)

    def test_intro_variants_are_unchanged(self) -> None:
        intro = f"Use this [link]({PR_URL}/2/files) to review incremental changes."
        for variant in [
            intro.replace("incremental changes", "this PR's incremental changes"),
            intro.replace("/2/files", "/3/files"),
            intro.replace(REPOSITORY, "other/repo"),
            intro + " ",
            f"Review the [incremental diff]({PR_URL}/2/files) for `stack/child`.",
        ]:
            body = _block().replace(intro, variant) + SUFFIX
            self.assert_unchanged(body)

    def test_author_prose_inside_the_block_is_unchanged(self) -> None:
        rows = [_row(2, "stack/child", marker=" <- _this PR_"), "Splitting #1 per review feedback."]
        self.assert_unchanged(_block(rows=rows) + SUFFIX)
        self.assert_unchanged(_block(rows=["Each PR is based on main.", *rows[:1]]) + SUFFIX)
        self.assert_unchanged(_block(rows=[rows[0], "  - stack/unlinked-branch"]) + SUFFIX)
        self.assert_unchanged(_block(rows=[rows[0], "<!-- note -->"]) + SUFFIX)

    def test_malformed_rows_are_unchanged(self) -> None:
        good = _row(2, "stack/child", marker=" <- _this PR_")
        for bad in [
            _row(2, "stack/child", marker=" <- _this PR_").replace("[[Files changed]", "[[Files]"),
            f"- [stack/child]({PR_URL}/2) [[Files changed]({PR_URL}/3/files)] <- _this PR_",
            good.replace(f"]({PR_URL}/2)", "](https://github.com/other/repo/pull/2)"),
            _row(2, "**stack/child", marker=" <- _this PR_"),
            _row(2, "stack child", marker=" <- _this PR_"),
            _row(2, "stack/child", marker=" <- _this PR_", files="/abc..def"),
            _row(2, "stack/child", marker=" (merged)"),
            _row(2, "stack/child", marker=" [MERGED]"),
            _row(2, "stack/child", marker=" <- _this PR_") + " ",
            "* " + good[2:],
            "\t" + good,
            " " + good,
        ]:
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

    def test_historical_descriptions(self) -> None:
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(fixture["repository"], REPOSITORY)
        cleaned = 0
        for entry in fixture["pull_requests"]:
            body, expected = entry["body"], entry["prefix_end"]
            with self.subTest(pr=entry["number"]):
                end = stack_prefix_end(body, REPOSITORY, entry["number"])
                self.assertEqual(end, expected)
                if end is None:
                    continue
                cleaned += 1
                prefix, suffix = body[:end], body[end:]
                heading = prefix.split("\n", 1)[0].rstrip("\r")
                self.assertIn(heading, {"## Stacked PR", "## \U0001f95e Stacked PR"})
                self.assertRegex(prefix, r"---------(\r\n|\n)?\Z")
                self.assertNotIn("---------", prefix[: -len("---------\r\n")])
                if not suffix.startswith("## Stacked PR"):
                    self.assertIsNone(stack_prefix_end(suffix, REPOSITORY, entry["number"]))
        self.assertEqual((cleaned, len(fixture["pull_requests"])), (86, 100))

    def test_documented_corpus_boundaries(self) -> None:
        bodies = {e["number"]: e["body"] for e in json.loads(FIXTURE.read_text())["pull_requests"]}
        template = "## What changes are proposed in this pull request?\r\n\r\n"
        lf = bodies[4068]
        self.assertTrue(lf[: _end(lf, 4068)].endswith("\n\n---------\n"))
        self.assertTrue(lf[_end(lf, 4068) :].startswith(template + "Return a NULL struct"))
        crlf = bodies[3222]
        self.assertTrue(crlf[: _end(crlf, 3222)].endswith("\r\n\r\n---------\r\n"))
        self.assertTrue(crlf[_end(crlf, 3222) :].startswith(template + "This PR adds"))
        self.assertEqual(_end(bodies[4082], 4082), len(bodies[4082]))


if __name__ == "__main__":
    unittest.main()
