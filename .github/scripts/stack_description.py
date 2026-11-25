"""Recognize one complete git-stack prefix at character zero.

For repository a/b and PR 2, the minimal supported block is:
    ## \U0001f95e Stacked PR
    Use this [link](https://github.com/a/b/pull/2/files) to review incremental changes.
    - [branch](https://github.com/a/b/pull/2) [[Files changed](https://github.com/a/b/pull/2/files)]
    ---------

The heading must exactly match _HEADING, including the pancake. Rows use single-token labels
(optionally bold), two-space indentation steps, and optional markers from the constants below.
Both links in each row must name the same PR in this repository, with exactly one row for the
current PR. File links may include /base..head ranges with 7-40 lowercase hex digits per SHA.
Zero or one empty line may precede the terminator; LF, CRLF, and mixed endings are accepted.

Any mismatch, such as `## Stacked PR`, `---`, or prose between rows, returns None. The controller
then skips the description write and still requests merging. Otherwise, it writes body[end:],
preserving all remaining content, whitespace, line endings, and any later stack section.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

_HEADING = "## \U0001f95e Stacked PR"
_TERMINATOR = "---------"
_CURRENT_PR_MARKERS = frozenset(
    {" ← _this PR_", " <- _this PR_", " <- this PR", " - _this PR_", " (this PR)"}
)
_OTHER_PR_MARKERS = frozenset({" [MERGED]"})
_RANGE = r"(?:/[0-9a-f]{7,40}\.\.[0-9a-f]{7,40})?"


def stack_prefix_end(body: str, repository: str, number: int) -> int | None:
    """Return the prefix's end offset, or None if any required line is unsupported."""
    pull_url = re.escape(f"https://github.com/{repository}/pull/")
    intro = re.compile(
        rf"Use this \[link\]\({pull_url}{number}/files{_RANGE}\) to review incremental changes\."
    )
    row = re.compile(
        rf"( *)- \[(?:\*\*[^\[\]\s*]+\*\*|[^\[\]\s*]+)\]\({pull_url}([0-9]+)\)"
        rf" \[\[Files changed\]\({pull_url}([0-9]+)/files{_RANGE}\)\](.*)"
    )
    lines = _lines(body)

    content, _ = next(lines, ("", 0))
    if content != _HEADING:
        return None
    content, _ = next(lines, ("", 0))
    if intro.fullmatch(content) is None:
        return None

    rows = 0
    current_rows = 0
    previous_indent = 0
    for content, end in lines:
        if content == "":
            content, end = next(lines, ("", 0))
            if content != _TERMINATOR:
                return None
        if content == _TERMINATOR:
            return end if rows and current_rows == 1 else None
        match = row.fullmatch(content)
        if match is None:
            return None
        indent, pr_number, files_number, marker = match.groups()
        is_current = pr_number == str(number)
        allowed_markers = _CURRENT_PR_MARKERS if is_current else _OTHER_PR_MARKERS
        if (
            pr_number != files_number
            or len(indent) % 2
            or (rows == 0 and indent)
            or len(indent) > previous_indent + 2
            or (marker and marker not in allowed_markers)
        ):
            return None
        rows += 1
        current_rows += is_current
        previous_indent = len(indent)
    return None


def _lines(text: str) -> Iterator[tuple[str, int]]:
    """Yield each line's content and the offset after its LF or CRLF ending."""
    position = 0
    while position < len(text):
        newline = text.find("\n", position)
        if newline < 0:
            yield text[position:], len(text)
            return
        content_end = newline - 1 if newline > position and text[newline - 1] == "\r" else newline
        yield text[position:content_end], newline + 1
        position = newline + 1
