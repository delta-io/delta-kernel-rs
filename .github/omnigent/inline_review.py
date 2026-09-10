"""Build a SHA-bound GitHub review payload from validated AI findings."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from review_publish import format_review_body as _format_review_body


MAX_INLINE_FINDINGS = 12
INLINE_FINDING_FIELDS = ("id", "path", "line", "side", "body")
INLINE_FINDING_SIDES = ("LEFT", "RIGHT")
_FINDING_ID = re.compile(r"(?:Blocker|Nit)[1-9][0-9]*")
_HUNK_HEADER = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@"
)


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
    if len(findings) > MAX_INLINE_FINDINGS:
        raise ValueError(
            f"inline review contains more than {MAX_INLINE_FINDINGS} findings"
        )

    clean_review = (review[:start_index] + review[end_index + len(end) :]).strip()
    if not clean_review:
        raise ValueError("inline review has no human-readable content")
    return clean_review, findings


def inline_prompt_instructions(marker: str) -> str:
    """Build the prompt contract for machine-readable inline findings."""
    start, end = _inline_markers(marker)
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

This review will also be published as GitHub inline comments. After the
human-readable review and before the final end marker, emit this exact
machine-readable block:

{start}
{example}
{end}

Include entries for up to {MAX_INLINE_FINDINGS} Blocker/Nit findings, prioritizing
blockers and then the most useful notes. Findings omitted from this block remain
in the collapsed review. Use IDs matching `{_FINDING_ID.pattern}` (for example,
`Blocker1` or `Nit1`) and do not add an entry for the Summary. Use the repository-relative
path with no backticks. Use {INLINE_FINDING_SIDES[1]} and the head-file line
number for additions or context; use {INLINE_FINDING_SIDES[0]} and the base-file
line number only for deleted lines. The location must occur in the supplied
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

    for raw_line in diff.splitlines():
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


def build_review_payload(
    *,
    review: str,
    findings: list[dict[str, Any]],
    diff: str,
    head_sha: str,
    run_url: str,
) -> tuple[dict[str, Any], list[str]]:
    """Build a non-blocking review and return labels for findings not attached."""
    if re.fullmatch(r"[0-9a-f]{40}", head_sha) is None:
        raise ValueError("head SHA must be a 40-character lowercase hex value")

    allowed_positions = diff_positions(diff)
    comments: list[dict[str, Any]] = []
    unmapped: list[str] = []
    seen_ids: set[str] = set()

    for index, finding in enumerate(findings, start=1):
        try:
            finding_id, path, line, side, body = _validate_finding(finding)
        except ValueError:
            unmapped.append(f"entry {index}")
            continue
        if finding_id in seen_ids:
            unmapped.append(finding_id)
            continue
        seen_ids.add(finding_id)

        if (path, line, side) not in allowed_positions:
            unmapped.append(finding_id)
            continue
        comments.append(
            {
                "path": path,
                "line": line,
                "side": side,
                "body": f"**{finding_id}** {body}",
            }
        )

    return (
        {
            "body": _format_review_body(review, run_url, collapsed=True),
            "commit_id": head_sha,
            "event": "COMMENT",
            "comments": comments,
        },
        unmapped,
    )


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


def _validate_finding(finding: Any) -> tuple[str, str, int, str, str]:
    expected = set(INLINE_FINDING_FIELDS)
    if not isinstance(finding, dict) or set(finding) != expected:
        fields = ", ".join(INLINE_FINDING_FIELDS)
        raise ValueError(f"each inline finding must contain exactly: {fields}")

    finding_id = finding["id"]
    path = finding["path"]
    line = finding["line"]
    side = finding["side"]
    body = finding["body"]
    if not isinstance(finding_id, str) or _FINDING_ID.fullmatch(finding_id) is None:
        raise ValueError("inline finding ID must match Blocker1 or Nit1 form")
    if (
        not isinstance(path, str)
        or not path
        or len(path) > 500
        or path.startswith("/")
        or "\x00" in path
        or ".." in Path(path).parts
    ):
        raise ValueError(f"inline finding {finding_id} has an invalid path")
    if isinstance(line, bool) or not isinstance(line, int) or line < 1:
        raise ValueError(f"inline finding {finding_id} has an invalid line")
    if side not in INLINE_FINDING_SIDES:
        raise ValueError(f"inline finding {finding_id} has an invalid side")
    if not isinstance(body, str) or not body.strip() or len(body) > 10_000:
        raise ValueError(f"inline finding {finding_id} has an invalid body")
    if any(ord(char) < 32 and char not in "\n\t" for char in body):
        raise ValueError(f"inline finding {finding_id} has control characters")
    return finding_id, path, line, side, body.strip()


def main() -> None:
    """Build a GitHub review request from workflow-owned files."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--review", required=True, type=Path)
    parser.add_argument("--findings", required=True, type=Path)
    parser.add_argument("--diff", required=True, type=Path)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--unmapped-output", required=True, type=Path)
    args = parser.parse_args()

    payload, unmapped = build_review_payload(
        review=args.review.read_text(),
        findings=json.loads(args.findings.read_text()),
        diff=args.diff.read_text(errors="replace"),
        head_sha=args.head_sha,
        run_url=args.run_url,
    )
    args.output.write_text(json.dumps(payload))
    args.unmapped_output.write_text("\n".join(unmapped) + ("\n" if unmapped else ""))


if __name__ == "__main__":
    main()
