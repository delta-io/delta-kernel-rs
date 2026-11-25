"""Build Markdown for AI review comments and the AI Review Summary Check Run."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any


BOT_MARKER = "<!-- ai-review-bot -->"
INLINE_FEEDBACK_FOOTER = (
    "\n\n---\n"
    "<sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, "
    "or 👀 if out of scope.</sub>"
)
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


def format_review_body(review: str, run_url: str, *, collapsed: bool) -> str:
    """Add the shared header and footer to a publishable review."""
    if not run_url.startswith("https://github.com/"):
        raise ValueError("run URL must be a GitHub HTTPS URL")

    body = review.strip()
    if collapsed:
        body = f"<details><summary>Show review</summary>\n\n{body}\n\n</details>"
    return (
        f"{BOT_MARKER}\n"
        f"{REVIEW_HEADER}\n\n"
        f"{body}\n\n"
        "---\n"
        f"<sub>Automated review - [workflow run]({run_url})</sub>"
    )


def strip_review_body(body: str) -> str:
    """Remove the wrapper added by :func:`format_review_body`."""
    body = body.strip()
    if body.startswith(BOT_MARKER):
        body = body[len(BOT_MARKER) :].lstrip()
    body = re.sub(r"^<!-- ai-review-main \{[^\n]*\} -->\s*", "", body)
    if body.startswith(REVIEW_HEADER):
        body = body[len(REVIEW_HEADER) :].lstrip()
    elif body.startswith("## AI Review\n"):
        summary = re.search(
            r"<details><summary>PR Summary</summary>\s*\n(.*?)\n\s*</details>",
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
    finding: dict[str, Any], *, head_sha: str, main_url: str, collapsed: bool = False
) -> str:
    """Build one finding comment, optionally folding inline detail under a Details disclosure."""
    body = finding["body"].strip()
    attribution = re.findall(r"(?im)^Raised by:\s*([^\n]+)\s*$", body)
    body = re.sub(r"(?im)^Raised by:\s*[^\n]+\n?", "", body).strip()
    claim, separator, detail = body.partition("\n\n")
    text = f"**{finding_label(finding['id'])}** {claim}"
    if separator:
        text += (
            f"\n\n<details><summary>Details</summary>\n\n{detail}\n\n</details>"
            if collapsed else f"\n\n{detail}"
        )
    raised_by = f"Raised by: {', '.join(attribution)} · " if attribution else ""
    footer = INLINE_FEEDBACK_FOOTER if collapsed else ""
    return f"{text}\n\n{raised_by}Reviewed `{head_sha[:8]}` · [AI Review]({main_url}){footer}"


def format_review_summary(
    *,
    overview: str,
    head_sha: str,
    findings: list[dict[str, Any]],
    run_url: str,
    errors: list[str] | None = None,
    counts_available: bool = True,
    checks_url: str | None = None,
) -> str:
    """Build the main AI review Conversation comment from findings with confirmed comment URLs."""
    confirmed = [finding for finding in findings if finding.get("url")]
    counts = Counter(finding["severity"] for finding in confirmed)
    headline = (
        f"**{counts['Blocker']} Blockers · {counts['Should Fix']} Should Fix · "
        f"{counts['Nit']} Nits**"
        if counts_available
        else "Finding counts unavailable."
    )
    parts = [BOT_MARKER, "## AI Review", "---", headline, f"Reviewed `{head_sha[:8]}`"]
    if errors:
        parts.append("Publication incomplete: " + " ".join(dict.fromkeys(errors)))
    parts.append(
        "<details><summary>PR Summary</summary>\n\n"
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
    parts.append(f"[Full AI review → Checks]({checks_url}) · [Workflow run]({run_url})")
    return "\n\n".join(parts)
