"""Recognize the generated git-stack prefix at the start of a PR description.

The recognizer admits one leading block in the layout git stack writes: a heading, the
incremental-review sentence for the current PR, linked stack rows, at most one blank line, and
the exact terminator. Every line must match; author prose, unknown markup, a missing terminator,
or a link that disagrees with the repository or PR identity leaves the whole description
unchanged. The caller slices the original string at the returned boundary, so retained content
keeps its bytes, whitespace, and line endings.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

_HEADINGS = frozenset({"## Stacked PR", "## \U0001f95e Stacked PR"})
_TERMINATOR = "---------"
_CURRENT_PR_MARKERS = frozenset(
    {" ← _this PR_", " <- _this PR_", " <- this PR", " - _this PR_", " (this PR)"}
)
_OTHER_PR_MARKERS = frozenset({" [MERGED]"})
_RANGE = r"(?:/[0-9a-f]{7,40}\.\.[0-9a-f]{7,40})?"


def stack_prefix_end(body: str, repository: str, number: int) -> int | None:
    """Return the offset where the description continues after its generated stack prefix.

    Return None when the description does not start with a complete supported prefix for this
    repository and PR, including when it has already been cleaned. `body[end:]` is the exact
    retained description; it may be empty when the prefix was the whole description.
    """
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
    if content not in _HEADINGS:
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
