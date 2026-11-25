"""Build Markdown for AI review comments and the AI Review Summary Check Run."""

from __future__ import annotations

import json
import re
from collections import Counter
from html import escape
from typing import Any

from markdown_utils import markdown_lines


BOT_MARKER = "<!-- ai-review-bot -->"
AI_REVIEW_TITLE = "🤖 AI Review 🧐"
FINDING_FEEDBACK_PROMPT = (
    "Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, "
    "or 👀 if out of scope."
)
FINDING_FEEDBACK_FOOTER = f"\n\n---\n<sub>{FINDING_FEEDBACK_PROMPT}</sub>"
REVIEW_HEADER = "## AI Review <sub>(draft - human review required)</sub>"
_REVIEW_FOOTER = re.compile(
    r"\n+---\n+<sub>Automated review - \[workflow run\]\([^\n]+\)</sub>\s*$"
)
_PUBLICATION_MARKER = re.compile(r"[0-9a-f]{32}")


def extract_marked_review(raw: str, marker: str) -> str:
    """Return the final complete review delimited by a per-run marker pair.

    ``raw`` is the output of ``omnigent run -p``, which can contain a truncated copy of the
    review before the complete one. Omnigent waits for the orchestrator's output in bounded
    intervals. When an interval ends while the review is streaming, Omnigent keeps the text
    received so far, reconnects, and receives the whole review again. A start marker with no end
    marker is therefore accepted only when the text after it is a prefix of the next complete
    review.

    Raises ``ValueError`` for any other unbalanced markers and when no complete review exists.
    """
    if _PUBLICATION_MARKER.fullmatch(marker) is None:
        raise ValueError("publication marker is invalid")

    start = f"<!-- AI_REVIEW_START_{marker} -->"
    end = f"<!-- AI_REVIEW_END_{marker} -->"
    token_pattern = re.compile(f"({re.escape(start)}|{re.escape(end)})")
    current_start: int | None = None
    truncated_copies: list[str] = []
    reviews: list[str] = []
    for token in token_pattern.finditer(raw):
        if token.group(0) == start:
            if current_start is not None:
                truncated_copies.append(raw[current_start : token.start()].rstrip())
            current_start = token.end()
            continue
        if current_start is None:
            raise ValueError("publication end marker has no matching start marker")
        # The comparison includes the end marker because a copy can be cut inside that marker.
        complete_copy = raw[current_start : token.end()]
        if not all(complete_copy.startswith(copy) for copy in truncated_copies):
            raise ValueError("publication markers are nested")
        truncated_copies.clear()
        reviews.append(raw[current_start : token.start()].strip())
        current_start = None

    if current_start is not None:
        raise ValueError("publication start marker has no matching end marker")
    if not reviews:
        raise ValueError("publication markers are missing")
    if not reviews[-1]:
        raise ValueError("final marked review is empty")
    return reviews[-1]


def strip_review_body(body: str) -> str:
    """Extract assessment text from main comments or wrapped review bodies."""
    body = body.strip()
    if body.startswith(BOT_MARKER):
        body = body[len(BOT_MARKER) :].lstrip()
    body = re.sub(r"^<!-- ai-review-main \{[^\n]*\} -->\s*", "", body)
    if body.startswith(REVIEW_HEADER):
        body = body[len(REVIEW_HEADER) :].lstrip()
    elif re.match(rf"## (?:{re.escape(AI_REVIEW_TITLE)}|AI Review)(?: - [A-Z ]+)?\n", body):
        summary = re.search(
            r"<details><summary>(?:(?:PR|Assessment) )?Summary</summary>\s*\n(.*?)\n\s*</details>",
            body,
            re.DOTALL,
        )
        if summary:
            return summary[1].strip()
    body = _REVIEW_FOOTER.sub("", body)
    details_start = "<details><summary>Show review</summary>"
    if body.startswith(details_start) and body.rstrip().endswith("</details>"):
        body = body[len(details_start) :].lstrip()
        body = body.rstrip()[: -len("</details>")].rstrip()
    return body.strip()


def finding_label(finding_id: str) -> str:
    """Return the per-severity label displayed on an individual finding."""
    match = re.fullmatch(r"(Blocker|ShouldFix|Nit)([1-9][0-9]*)", finding_id)
    if match is None:
        raise ValueError("invalid final finding ID")
    severity = "Should Fix" if match[1] == "ShouldFix" else match[1]
    return f"[{severity} {match[2]}]"


def format_finding(
    finding: dict[str, Any], *, head_sha: str, main_url: str
) -> str:
    """Keep finding text visible and put review metadata above the feedback prompt in a small footer."""
    attribution = []
    lines = []
    for _, line, fence in markdown_lines(finding["body"].strip()):
        field = re.match(
            r"^(?:\*\*)?(Explanation|Suggested fix|Raised by)(?:\*\*)?:[ \t]*"
            r"(?:\*\*)?[ \t]*(.*)", line, re.I,
        ) if fence is None else None
        if field:
            label = field[1].casefold()
            if label == "raised by":
                attribution.append(field[2].strip())
                continue
            label = "Explanation" if label == "explanation" else "Suggested fix"
            line = f"**{label}:** {field[2]}\n"
        lines.append(line)
    body = "".join(lines).strip()
    claim, separator, detail = body.partition("\n\n")
    text = f"**{finding_label(finding['id'])}** {claim}"
    if separator:
        if not detail.startswith(("**Explanation:**", "**Suggested fix:**")):
            detail = "**Explanation:**\n\n" + detail
        text += f"\n\n{detail}"
    raised_by = f"Raised by: {', '.join(attribution)} · " if attribution else ""
    return (
        f"{text}\n\n---\n"
        f"<sub>{raised_by}Reviewed `{head_sha[:8]}` · [AI Review]({main_url})</sub><br>\n"
        f"<sub>{FINDING_FEEDBACK_PROMPT}</sub>"
    )


def format_review_summary(
    *,
    overview: str,
    head_sha: str,
    findings: list[dict[str, Any]],
    run_url: str,
    errors: list[str] | None = None,
    counts_available: bool = True,
    checks_url: str | None = None,
    coverage: dict[str, Any] | None = None,
    verdict: dict[str, Any] | None = None,
) -> str:
    """Build Markdown for the main AI review Conversation comment.

    Count final findings with confirmed comment URLs and link Blocker and Should Fix comments
    from Review Summary. If verdict is supplied, include its advisory status, coverage marker
    and reasons for incomplete review work.
    """
    confirmed = [finding for finding in findings if finding.get("url")]
    counts = Counter(finding["severity"] for finding in confirmed)
    headline = f"**{_finding_counts(counts)}**" if counts_available else "Finding counts unavailable."
    title = f"## AI Review - {verdict['status']}" if verdict else "## AI Review"
    reviewed = f"Reviewed `{head_sha[:8]}`"
    if verdict:
        reviewed += " · Full Coverage ✓" if verdict["coverage_full"] else " · Partial Coverage ⚠"
    parts = [BOT_MARKER, title, headline, reviewed]
    if verdict and not verdict["coverage_full"]:
        rows = []
        for reason in verdict["reasons"]:
            message = reason["message"]
            if reason.get("url"):
                message += f" [Earlier finding]({reason['url']})"
            rows.append("- " + message)
        if coverage is not None and not coverage.get("complete"):
            rows.append(
                f"- Diff input: {coverage.get('full_files', 0)} of "
                f"{coverage.get('total_files', 0)} files fully delivered; "
                f"{coverage.get('partial_files', 0)} partial and "
                f"{coverage.get('omitted_files', 0)} omitted. "
                f"[Diff details in Checks]({checks_url or run_url})."
            )
        parts.append(
            "<details><summary>Partial Coverage Details</summary>\n\n"
            + "\n".join(rows) + "\n\n</details>"
        )
    if verdict is None and coverage is not None and not coverage.get("complete"):
        parts.append(
            f"Review input truncated: {coverage.get('full_files', 0)} of "
            f"{coverage.get('total_files', 0)} file sections fully included; "
            f"{coverage.get('partial_files', 0)} partial and "
            f"{coverage.get('omitted_files', 0)} omitted. "
            f"[Diff details in Checks]({checks_url or run_url})."
        )
    if errors and verdict is None:
        parts.append("Posting review comments is incomplete: " + " ".join(dict.fromkeys(errors)))
    parts.append(
        "<details><summary>Assessment Summary</summary>\n\n"
        + (overview.strip() or "See the full AI review in Checks.")
        + "\n\n</details>"
    )
    rows = []
    for finding in confirmed:
        if finding["severity"] == "Nit":
            continue
        claim = " ".join(finding["body"].split("\n\n", 1)[0].split())[:400]
        claim = claim.replace("[", "\\[").replace("]", "\\]")
        reused = " (existing thread)" if finding.get("delivery") == "reused" else ""
        rows.append(
            f"- **{finding_label(finding['id'])}** [{claim}]({finding['url']}){reused}"
        )
    if rows:
        parts.append(
            "<details><summary>Review Summary</summary>\n\n"
            + "\n".join(rows)
            + "\n\n</details>"
        )
    checks_url = checks_url or run_url
    parts.append(
        f"<sub>Reviewed `{head_sha[:8]}` · [Full AI review → Checks]({checks_url})"
        f" · [Workflow run]({run_url})</sub>"
    )
    return "\n\n".join(parts)


def format_diff_coverage(coverage: dict[str, Any]) -> str:
    """Describe the subagent patch excerpt's byte count and partly included or omitted file sections."""
    parts = [
        "## Subagent patch excerpt",
        f"Reviewed range: `{coverage['base_sha']}...{coverage['head_sha']}`",
        f"Includes {coverage['excerpt_bytes']} of {coverage['total_bytes']} UTF-8 patch bytes; "
        f"{coverage['full_files']} full, {coverage['partial_files']} partial, "
        f"{coverage['omitted_files']} omitted files.",
    ]
    parts.extend(
        f"- {entry['status'].capitalize()}: <code>{escape(json.dumps(entry['path']))}</code>"
        for entry in coverage["files"] if entry["status"] != "full"
    )
    return "\n\n".join(parts)


def format_status_check(verdict: dict[str, Any]) -> str:
    """Build the status section for the AI Review Summary Check Run report."""
    coverage = "Full Coverage ✓" if verdict["coverage_full"] else "Partial Coverage ⚠"
    parts = [
        f"## AI Review - {verdict['status']}",
        f"**{_finding_counts(verdict['counts'])}**",
        f"Reviewed `{verdict['head_sha'][:8]}` · {coverage}",
    ]
    for reason in verdict["reasons"]:
        text = reason["message"]
        if reason.get("url"):
            text += f" [Earlier finding]({reason['url']})"
        parts.append("- " + text)
    if verdict["sensitive_paths"]:
        parts.append("Sensitive areas: " + ", ".join(
            f"<code>{escape(path)}</code>" for path in verdict["sensitive_paths"]
        ))
    if verdict["breaking_change"]:
        parts.append("The PR declares a breaking change.")
    return "\n\n".join(parts)


def _finding_counts(counts) -> str:
    blockers, should_fix, nits = (counts[severity] for severity in ("Blocker", "Should Fix", "Nit"))
    blocker_label = "Blocker" if blockers == 1 else "Blockers"
    nit_label = "Nit" if nits == 1 else "Nits"
    return f"🛑 {blockers} {blocker_label} · 🔧 {should_fix} Should Fix · 💡 {nits} {nit_label}"


def bound_check_summary(summary: str, report_url: str, limit: int = 60_000) -> str:
    """Link an oversized Check Run report to the artifact containing its complete text."""
    if len(summary.encode("utf-8")) <= limit:
        return summary
    return (
        "The complete AI review and coverage report exceed GitHub's display limit. "
        f"[Open the full report]({report_url}) in the run's evidence artifact "
        "(retained for seven days)."
    )
