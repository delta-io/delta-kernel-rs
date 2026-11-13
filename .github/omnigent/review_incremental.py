"""Choose a repeat-review diff while preserving the complete PR diff for publication."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Collection
from typing import Any

from review_history import (
    DEFAULT_TRUSTED_BOT_LOGINS,
    _is_marked_bot_entry,
    _review_nodes,
)
from review_publish import REVISION_MARKER, format_review_body

_SHA = re.compile(r"[0-9a-f]{40}")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


def revision_marker(base: str, head: str) -> str:
    """Record the validated review's base and head; reject malformed commit IDs."""
    if not all(_SHA.fullmatch(sha) for sha in (base, head)):
        raise ValueError("review revisions must be full lowercase commit SHAs")
    return f"<!-- ai-review-revision base={base} head={head} -->"


def latest_reviewed_revision(
    history: Any,
    trusted_bot_logins: Collection[str] = DEFAULT_TRUSTED_BOT_LOGINS,
) -> tuple[str, str] | None:
    """Return the newest trusted, workflow-stamped base/head pair, or no baseline."""
    entries = []
    for node in _review_nodes(history):
        if not _is_marked_bot_entry(node, trusted_bot_logins):
            continue
        timestamp = node.get("submittedAt") or node.get("createdAt")
        if not isinstance(timestamp, str):
            continue
        entries.append((timestamp, node))
    if not entries:
        return None
    node = max(entries, key=lambda entry: entry[0])[1]
    markers = list(REVISION_MARKER.finditer(node["body"]))
    if len(markers) != 1:
        return None
    base, head = markers[0].groups()
    commit = node.get("commit")
    if not isinstance(commit, dict) or commit.get("oid") != head:
        return None
    return base, head


def select_review_diff(
    *,
    metadata: dict[str, Any],
    history: Any,
    full_diff: str,
    trusted_bot_logins: Collection[str] = DEFAULT_TRUSTED_BOT_LOGINS,
    force_full: bool = False,
    compare: Callable[[str, str, str, bool], Any] | None = None,
) -> tuple[str, str, bool]:
    """Return diff, scope explanation, and whether the range is incremental.

    Fall back to a full review on uncertainty.

    ``compare`` fetches a GitHub comparison as JSON or a diff. It can raise an exception when
    unavailable; the full PR diff remains the fallback. Publication must always use ``full_diff``.
    """
    base = metadata["base"]["sha"]
    head = metadata["head"]["sha"]
    repository = metadata["base"]["repo"]["full_name"]
    if not all(isinstance(sha, str) and _SHA.fullmatch(sha) for sha in (base, head)):
        raise ValueError("PR revisions must be full lowercase commit SHAs")
    if not isinstance(repository, str) or not _REPOSITORY.fullmatch(repository):
        raise ValueError("invalid PR repository")

    def full(reason: str) -> tuple[str, str, bool]:
        return full_diff, f"Full PR review ({base}...{head}): {reason}.", False

    if force_full:
        return full("explicit full review requested")
    previous = latest_reviewed_revision(history, trusted_bot_logins)
    if previous is None:
        return full("no complete trusted review baseline")
    previous_base, previous_head = previous
    if previous_base != base:
        return full("the target base changed")
    if previous_head == head:
        return (
            "",
            (
                "No code changes since the previous review. Review the current PR title and "
                "description for material inaccuracies; do not rediscover unchanged code issues."
            ),
            True,
        )
    fetch = compare or _compare
    try:
        comparison = fetch(repository, previous_head, head, False)
        if (
            not isinstance(comparison, dict)
            or comparison.get("status") != "ahead"
            or (comparison.get("merge_base_commit") or {}).get("sha") != previous_head
            or (comparison.get("base_commit") or {}).get("sha") != previous_head
        ):
            return full("the reviewed head is not a verified ancestor")
        commits = comparison.get("commits")
        if (
            not isinstance(commits, list)
            or not commits
            or comparison.get("total_commits") != len(commits)
            or any(
                not isinstance(commit, dict)
                or not isinstance(commit.get("parents"), list)
                or len(commit["parents"]) != 1
                for commit in commits
            )
        ):
            return full("the update has merge commits or incomplete commit ancestry")
        files = comparison.get("files")
        if not isinstance(files, list) or len(files) >= 300:
            return full("the comparison file list is unavailable or may be truncated")
        diff = fetch(repository, previous_head, head, True)
        if not isinstance(diff, str) or (files and not diff.strip()):
            return full("the incremental diff is unavailable")
    except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
        return full("the comparison could not be retrieved")
    risk = major_change_reason(files, diff)
    if risk:
        return full(risk)
    return (
        diff,
        (
            f"Incremental review ({previous_head}..{head}); the PR base remains {base}. "
            "Focus new findings on this update and regressions in its affected callers and dependencies. "
            "Verify prior fixes using current source and review history. Unchanged code is available "
            "through the read-only source tools. Expand source inspection when a changed contract "
            "affects other paths; do not treat the smaller diff as proof that those paths are correct. "
            "Always check the current PR title and description. Deleted lines refer to the prior "
            "reviewed head, so keep those findings in the review body. Only current head locations "
            "that also belong to the complete PR diff can be posted inline."
        ),
        True,
    )


def stamp_complete_review(review: str, base: str, head: str, *, truncated: bool) -> str:
    """Stamp only reviews whose code diff was complete enough to serve as a baseline."""
    review = REVISION_MARKER.sub("", review).strip()
    return review if truncated else review + "\n\n" + revision_marker(base, head)


def fallback_review_body(review: str, run_url: str) -> str:
    """Retain all findings after rejected inline publication without advancing the baseline."""
    return format_review_body(REVISION_MARKER.sub("", review), run_url, collapsed=True)


def publication_findings(findings: list[dict[str, Any]], incremental: bool) -> list[dict[str, Any]]:
    """Keep previous-head deletion anchors in the body instead of treating them as PR-base lines."""
    if not incremental:
        return findings
    return [
        finding
        for finding in findings
        if not isinstance(finding, dict) or finding.get("side") != "LEFT"
    ]


def _compare(repository: str, base: str, head: str, diff: bool) -> Any:
    result = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repository}/compare/{base}...{head}",
            "-H",
            "Accept: application/vnd.github.v3.diff"
            if diff
            else "Accept: application/vnd.github+json",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout if diff else json.loads(result.stdout)


def major_change_reason(files: list[dict[str, Any]], diff: str) -> str | None:
    """Conservatively send broad or contract-changing updates through a full review.

    These signals cannot identify every redesign; callers must also expose a full-review override.
    """
    if any(
        not isinstance(file, dict) or not isinstance(file.get("filename"), str) for file in files
    ):
        return "the comparison file metadata is incomplete"
    if len(files) > 20:
        return "the update spans more than 20 files"
    if any(
        file["filename"].endswith(("Cargo.toml", "Cargo.lock", ".proto"))
        or file.get("status") in {"renamed", "removed"}
        for file in files
    ):
        return "the update changes dependencies, wire contracts, or file structure"
    changed = [
        line[1:]
        for line in diff.splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    ]
    if len(changed) > 200:
        return "the update changes more than 200 lines"
    if any(
        re.search(r"\b(pub(?:\([^)]*\))?\s+|trait\s+|impl(?:\s|<)|(?:async\s+)?fn\s)", line)
        for line in changed
    ):
        return "the update changes a Rust API or implementation contract"
    if any(re.match(r"\s*(?:[A-Za-z_]\w*\s*:|\)\s*->)", line) for line in changed):
        return "the update changes a field, parameter, or return-type declaration"
    return None
