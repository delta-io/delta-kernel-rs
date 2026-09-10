"""Format validated AI review text for GitHub publication."""


def format_review_body(review: str, run_url: str, *, collapsed: bool) -> str:
    """Add the shared header and footer to a publishable review."""
    if not run_url.startswith("https://github.com/"):
        raise ValueError("run URL must be a GitHub HTTPS URL")

    body = review.strip()
    if collapsed:
        body = f"<details><summary>Show review</summary>\n\n{body}\n\n</details>"
    return (
        "<!-- ai-review-bot -->\n"
        "## AI Review <sub>(draft - human review required)</sub>\n\n"
        f"{body}\n\n"
        "---\n"
        f"<sub>Automated review - [workflow run]({run_url})</sub>"
    )
