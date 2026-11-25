"""Post stack-repair instructions as one PR Conversation comment per PR head commit.

For an ambiguous_ancestry review-range outcome, create or update the GitHub Actions bot
comment. Write the same Markdown to the AI Review Summary Check Run summary file. Add the
run URL and confirmed comment URL to the outcome JSON supplied by review_scope.py. The
workflow retains that file as a scope artifact.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Callable
from pathlib import Path


def render_ambiguity_message(outcome: dict, run_url: str) -> str:
    """Build repair text for the PR Conversation comment and AI Review Summary Check Run."""
    parent = outcome["parent_number"]
    return (
        "**AI review did not run.**\n\n"
        f"This PR declares PR #{parent} as its parent. The workflow could not select a "
        f"review base: {outcome['reason']}.\n\n"
        f"**To get a review:** {outcome['suggested_fix']}.\n\n"
        f"PR head commit `{outcome['head_sha'][:8]}` · [Workflow run]({run_url})"
    )


def publish_ambiguity(
    outcome: dict, repository: str, run_url: str,
    get_json: Callable[[str], list], write_json: Callable[[str, str, dict], dict],
) -> str:
    """Post or update one trusted repair comment for this PR head commit and return its URL.

    A matching GitHub Actions bot comment establishes prior posting. Refresh that comment so
    an edited parent declaration cannot leave a stale rebase target. Read comments after an
    uncertain write before considering another write; failed reads do not permit a new comment.
    """
    marker = f"<!-- ai-review-ambiguous-ancestry {outcome['head_sha']} -->"
    endpoint = f"repos/{repository}/issues/{outcome['pr_number']}/comments"

    def existing() -> dict | None:
        page = 1
        while True:
            comments = get_json(f"{endpoint}?per_page=100&page={page}")
            for comment in comments:
                user = comment.get("user") or {}
                if (
                    user.get("type") == "Bot" and user.get("login") == "github-actions[bot]"
                    and (comment.get("body") or "").startswith(marker + "\n")
                ):
                    return comment
            if len(comments) < 100:
                return None
            page += 1

    body = f"{marker}\n{render_ambiguity_message(outcome, run_url)}"
    previous = existing()
    if previous and previous["body"] == body:
        return previous["html_url"]
    method = "PATCH" if previous else "POST"
    destination = (
        f"repos/{repository}/issues/comments/{previous['id']}" if previous else endpoint
    )
    try:
        response = write_json(method, destination, {"body": body})
        return response["html_url"]
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        confirmed = existing()
        if confirmed and confirmed["body"] == body:
            return confirmed["html_url"]
        raise


def _github_json(method: str, endpoint: str, payload: dict | None = None):
    command = ["gh", "api", "--method", method, endpoint]
    if payload is not None:
        command += ["--input", "-"]
    result = subprocess.run(
        command, input=json.dumps(payload) if payload is not None else None,
        capture_output=True, text=True, check=True, timeout=60,
    )
    return json.loads(result.stdout)


def main() -> None:
    """Save review-range diagnostics and Check Run text before posting the repair comment.

    The workflow supplies /tmp/review_scope_outcome.json through --outcome and uses
    --check-output for /tmp/check-summary.txt. Save the run URL before the GitHub write so
    a failed comment request still leaves a per-run review-range outcome.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outcome", type=Path, required=True)
    parser.add_argument("--check-output", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-url", required=True)
    args = parser.parse_args()
    outcome = json.loads(args.outcome.read_text())
    if (
        outcome.get("outcome") != "ambiguous_ancestry"
        or re.fullmatch(r"[0-9a-f]{40}", outcome.get("head_sha", "")) is None
        or not isinstance(outcome.get("pr_number"), int) or outcome["pr_number"] < 1
        or not isinstance(outcome.get("parent_number"), int) or outcome["parent_number"] < 1
        or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository) is None
        or not args.run_url.startswith(f"https://github.com/{args.repository}/actions/runs/")
    ):
        raise ValueError("invalid ambiguous ancestry outcome or destination")
    outcome["run_url"] = args.run_url
    args.outcome.write_text(json.dumps(outcome, indent=2) + "\n")
    args.check_output.write_text(render_ambiguity_message(outcome, args.run_url))
    outcome["comment_url"] = publish_ambiguity(
        outcome, args.repository, args.run_url,
        lambda path: _github_json("GET", path),
        _github_json,
    )
    args.outcome.write_text(json.dumps(outcome, indent=2) + "\n")


if __name__ == "__main__":
    main()
