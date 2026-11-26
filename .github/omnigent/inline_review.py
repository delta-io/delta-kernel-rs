"""Define the finding-output contract and parse review metadata and unified diffs."""

from __future__ import annotations

import json
import re
from typing import Any

INLINE_FINDING_FIELDS = ("id", "path", "line", "side", "body")
INLINE_FINDING_SIDES = ("LEFT", "RIGHT")
_HUNK_HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def extract_inline_findings(
    review: str, marker: str
) -> tuple[str, list[dict[str, Any]]]:
    """Remove and decode the per-run inline-finding block from a review."""
    start, end = _inline_markers(marker)
    if review.count(start) != 1 or review.count(end) != 1:
        raise ValueError("inline review output must contain one location block")

    start_index = review.index(start)
    payload_start = start_index + len(start)
    end_index = review.index(end, payload_start)
    document = json.loads(review[payload_start:end_index].strip())
    if not isinstance(document, dict) or set(document) != {"findings"}:
        raise ValueError("inline review document must contain only 'findings'")
    findings = document["findings"]
    if not isinstance(findings, list):
        raise ValueError("inline review findings must be a list")

    clean_review = (review[:start_index] + review[end_index + len(end) :]).strip()
    if not clean_review:
        raise ValueError("inline review has no human-readable content")
    return clean_review, findings


def inline_prompt_instructions(marker: str) -> str:
    """Return a complete inline-review output contract using the per-run marker."""
    start, end = _inline_markers(marker)
    review_start = f"<!-- AI_REVIEW_START_{marker} -->"
    review_end = f"<!-- AI_REVIEW_END_{marker} -->"
    example = json.dumps(
        {
            "findings": [
                dict(
                    zip(
                        INLINE_FINDING_FIELDS,
                        (
                            "Nit1",
                            "kernel/src/file.rs",
                            10,
                            "RIGHT",
                            (
                                "Complete, concise finding with failure mode, "
                                "reviewer attribution, and suggested fix."
                            ),
                        ),
                        strict=True,
                    )
                )
            ]
        },
        separators=(",", ":"),
    )
    return f"""

In PR comment modes, findings can appear inline or in the PR Conversation. For a completed
review, use this complete response layout, with both the human-readable review
and the machine-readable block inside the outer review markers:

{review_start}
<human-readable review, including findings and Summary>
{start}
{example}
{end}
{review_end}

Do not close the outer review before the machine-readable block. For a completed
review, the outer review end marker must be the final line of your response.

Include every final retained Blocker, Should Fix and Nit in this block. Use null
for path, line and side when no usable changed line exists. Include each retained finding exactly
once in the Markdown report and exactly once in the JSON block. Use Markdown headings
`### Blocker 1`, `### Should Fix 1` or `### Nit 1`, numbering from 1 within each severity.
Use compact JSON IDs such as `Blocker1`, `ShouldFix1` or `Nit1`; pair each Markdown finding
with its JSON entry by severity and number.
Keep the Summary to an overall assessment rather than repeating finding details.
Do not add an entry for the Summary. Use the repository-relative path with no
backticks. Use {INLINE_FINDING_SIDES[1]} and the head-file line
number for additions or context; use {INLINE_FINDING_SIDES[0]} and the base-file
line number only for deleted lines. Every non-null location must occur in the supplied
unified diff. Use an empty findings list when there are no findings. Do not wrap
the JSON in a Markdown code fence.
"""


def diff_positions(diff: str) -> set[tuple[str, int, str]]:
    """Return GitHub-reviewable ``(path, line, side)`` positions from a diff."""
    positions: set[tuple[str, int, str]] = set()
    old_path: str | None = None
    new_path: str | None = None
    old_line: int | None = None
    new_line: int | None = None
    in_hunk = False

    # Git ends a line only at a newline. `str.splitlines` also breaks at form feeds and other
    # separators, which would shift every later position.
    for raw_line in diff.removesuffix("\n").split("\n"):
        if raw_line.startswith("diff --git "):
            old_path = None
            new_path = None
            old_line = None
            new_line = None
            in_hunk = False
            continue
        if not in_hunk and raw_line.startswith("--- "):
            old_path = _diff_path(raw_line[4:])
            continue
        if not in_hunk and raw_line.startswith("+++ "):
            new_path = _diff_path(raw_line[4:])
            continue

        hunk = _HUNK_HEADER.match(raw_line)
        if hunk:
            old_line = int(hunk.group(1))
            new_line = int(hunk.group(3))
            in_hunk = True
            continue
        if old_line is None or new_line is None:
            continue
        if raw_line.startswith("\\ No newline at end of file"):
            continue

        review_path = new_path or old_path
        if review_path is None:
            continue
        if raw_line.startswith("+"):
            positions.add((review_path, new_line, "RIGHT"))
            new_line += 1
        elif raw_line.startswith("-"):
            positions.add((review_path, old_line, "LEFT"))
            old_line += 1
        else:
            positions.add((review_path, new_line, "RIGHT"))
            old_line += 1
            new_line += 1

    return positions


def diff_sections(diff: str) -> dict[str, str]:
    """Return each file's unified diff section, keyed by its review path."""
    sections = {}
    for section in re.split(r"(?=^diff --git )", diff, flags=re.MULTILINE):
        old = re.search(r"^--- (.+)$", section, re.MULTILINE)
        new = re.search(r"^\+\+\+ (.+)$", section, re.MULTILINE)
        path = _diff_path(new[1]) if new else None
        if path is None and old:
            path = _diff_path(old[1])
        if path is not None:
            sections[path] = section
    return sections


def _diff_path(value: str) -> str | None:
    value = value.split("\t", 1)[0]
    if value == "/dev/null":
        return None
    if value.startswith(("a/", "b/")):
        value = value[2:]
    return value


def _inline_markers(marker: str) -> tuple[str, str]:
    return (
        f"<!-- AI_REVIEW_INLINE_START_{marker} -->",
        f"<!-- AI_REVIEW_INLINE_END_{marker} -->",
    )
