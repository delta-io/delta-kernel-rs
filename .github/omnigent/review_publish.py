"""Format and unwrap validated AI review text for GitHub publication."""

from __future__ import annotations

import re


BOT_MARKER = "<!-- ai-review-bot -->"
REVIEW_HEADER = "## AI Review <sub>(draft - human review required)</sub>"
_REVIEW_FOOTER = re.compile(
    r"\n+---\n+<sub>Automated review - \[workflow run\]\([^\n]+\)</sub>\s*$"
)


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
    if body.startswith(REVIEW_HEADER):
        body = body[len(REVIEW_HEADER) :].lstrip()
    body = _REVIEW_FOOTER.sub("", body)
    details_start = "<details><summary>Show review</summary>"
    if body.startswith(details_start) and body.rstrip().endswith("</details>"):
        body = body[len(details_start) :].lstrip()
        body = body.rstrip()[: -len("</details>")].rstrip()
    return body.strip()
