"""Resolve the commit boundary for an individual PR in a git stack."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Callable
from pathlib import Path


_SHA = re.compile(r"[0-9a-f]{40}")


def resolve_review_base(
    metadata: dict,
    repository: str,
    get_json: Callable[[str], dict],
) -> str:
    """Return the review base SHA for GitHub PR metadata and its repository.

    Read the current PR's git-stack entry from its full description. Prefer its pinned
    comparison link; otherwise request the immediate parent PR through ``get_json``, which
    accepts a GitHub API endpoint. Ordinary PRs and stack roots without a pinned link use
    the GitHub base SHA. A merged parent's original head remains the boundary while it is
    an ancestor of the child; a rebased child uses the GitHub base instead. Stack boundaries
    are author supplied: ancestry checks do not establish that the selected scope is complete.
    Raise ValueError if a stack boundary is missing, stale, or not an ancestor of the head.
    API errors propagate to the caller rather than expanding the review to ancestor PRs.
    """
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("invalid repository")
    head_sha = metadata["head"]["sha"]
    github_base = metadata["base"]["sha"]
    if _SHA.fullmatch(head_sha) is None or _SHA.fullmatch(github_base) is None:
        raise ValueError("invalid PR base or head SHA")

    entry = _stack_entry(metadata.get("body") or "", repository, metadata["number"])
    if entry is None:
        return github_base
    current, parent = entry
    parent_merged = False
    current_url = f"https://github.com/{repository}/pull/{metadata['number']}"
    comparison_link = re.search(re.escape(current_url) + r"/files/([^\s)]+)", current)
    if comparison_link is not None:
        boundary = re.fullmatch(r"([0-9a-f]{40})\.{2,3}([0-9a-f]{40})", comparison_link[1])
        if boundary is None or boundary[2] != head_sha:
            raise ValueError(
                "stale or invalid stack comparison link; refresh the stack description"
            )
        review_base = boundary[1]
    elif parent is not None:
        parent_link = re.search(
            rf"\]\(https://github\.com/{re.escape(repository)}/pull/([0-9]+)\)", parent
        )
        if parent_link is None:
            raise ValueError("stack parent has no PR or pinned comparison link")
        parent_metadata = get_json(f"repos/{repository}/pulls/{parent_link[1]}")
        parent_merged = bool(parent_metadata.get("merged_at"))
        review_base = parent_metadata["head"]["sha"]
    else:
        return github_base

    if _SHA.fullmatch(review_base) is None:
        raise ValueError("invalid stack parent SHA")
    comparison = get_json(f"repos/{repository}/compare/{review_base}...{head_sha}")
    if (
        comparison.get("status") not in {"ahead", "identical"}
        or comparison.get("merge_base_commit", {}).get("sha") != review_base
    ):
        if parent_merged and comparison.get("status") in {"behind", "diverged"}:
            return github_base
        raise ValueError("stack parent is not an ancestor of the PR head; refresh the stack")
    return review_base


def _stack_entry(body: str, repository: str, number: int) -> tuple[str, str | None] | None:
    current_link = re.compile(
        rf"\]\(https://github\.com/{re.escape(repository)}/pull/{number}\)"
    )
    parents: list[tuple[int, str]] = []
    entry = None
    in_stack = False
    has_stack = False
    fence: str | None = None
    for line in body.splitlines():
        fence_match = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence is not None:
            if (
                fence_match
                and fence_match[1][0] == fence[0]
                and len(fence_match[1]) >= len(fence)
                and not fence_match[2].strip()
            ):
                fence = None
            continue
        if fence_match:
            fence = fence_match[1]
            continue
        if re.match(r"^#{1,6}\s", line):
            in_stack = re.fullmatch(
                r"#{1,6}\s+(?:[^\w\s]+\s+)?Stacked PR\s*#*\s*", line, re.IGNORECASE
            ) is not None
            has_stack = has_stack or in_stack
            parents.clear()
            continue
        item = re.match(r"^([ \t]*)[-*]\s+(.+)", line)
        if not in_stack or item is None:
            continue
        indent = len(item[1].expandtabs())
        while parents and parents[-1][0] >= indent:
            parents.pop()
        if current_link.search(item[2]):
            if entry is not None:
                raise ValueError("multiple stack entries for the current PR")
            entry = (item[2], parents[-1][1] if parents else None)
        parents.append((indent, item[2]))
    if has_stack and entry is None:
        raise ValueError("stack description has no entry for the current PR")
    return entry


def _github_json(endpoint: str) -> dict:
    return json.loads(subprocess.check_output(["gh", "api", endpoint], text=True))


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    args = parser.parse_args()
    try:
        metadata = json.loads(args.metadata.read_text())
        review_base = resolve_review_base(metadata, args.repository, _github_json)
    except (KeyError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Cannot resolve PR review scope: {error}") from error
    print(review_base)


if __name__ == "__main__":
    _main()
