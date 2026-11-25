"""Resolve the commit boundary for an individual PR in a git stack."""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
from collections.abc import Callable
from pathlib import Path

from markdown_utils import markdown_lines

_SHA = re.compile(r"[0-9a-f]{40}")
_STACK_HEADING = re.compile(
    r"^(?:#{1,6}\s+)?(?:\*\*)?(?:[^\w\s]+\s+)?Stacked PRs?(?:\*\*)?\s*#*\s*$",
    re.IGNORECASE,
)
_HORIZONTAL_RULE = re.compile(r" {0,3}(?:(?:-\s*){3,}|(?:\*\s*){3,}|(?:_\s*){3,})")
_LOG = logging.getLogger(__name__)


class AmbiguousStackRange(ValueError):
    """The workflow cannot select a review base from the declared parent PR's ancestry."""

    def __init__(self, parent_number: int, reason: str):
        super().__init__(reason)
        self.parent_number = parent_number
        self.reason = reason
        self.suggested_fix = (
            f"rebase this PR's branch onto PR #{parent_number}'s current head, push the branch, "
            "then comment `/review inline`"
        )


def resolve_review_base(
    metadata: dict,
    repository: str,
    get_json: Callable[[str], dict],
) -> str:
    """Return the review base SHA using the declared parent's GitHub ancestry.

    The description's git-stack list identifies the immediate parent PR; comparison links
    and branch labels do not select commits. Ordinary PRs and stack roots use the GitHub
    base. Children must inherit their parent's head; divergent histories require a stack
    sync because ancestry cannot distinguish an advanced parent from a rewritten one.
    Use the GitHub base when it contains the inherited parent head, or when both it and
    the child inherit the parent's merge result, including after squash merges.

    Raise AmbiguousStackRange for divergent parent/child histories or a base-branch merge
    that would inflate the PR change patch. Invalid descriptions and empty review ranges
    raise ValueError.
    API failures propagate instead of widening the review. The declared parent
    relationship remains author supplied; ancestry cannot establish its completeness.
    """
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("invalid repository")
    head_sha = _validate_sha(metadata["head"]["sha"], "PR head")
    github_base = _validate_sha(metadata["base"]["sha"], "PR base")
    entry = _stack_entry(metadata.get("body") or "", repository, metadata["number"])
    if entry is None or entry[1] is None:
        reason = "ordinary PR" if entry is None else "stack root"
        _LOG.info("Review scope: %s; GitHub base %s", reason, github_base)
        return github_base

    parent_link = re.search(
        rf"\]\(https://github\.com/{re.escape(repository)}/pull/([0-9]+)\)", entry[1]
    )
    if parent_link is None:
        raise ValueError("stack parent has no PR link; refresh the stack description")
    parent_number = parent_link[1]
    parent = get_json(f"repos/{repository}/pulls/{parent_number}")
    parent_head = _validate_sha(parent["head"]["sha"], "stack parent")
    if parent_head == github_base:
        _LOG.info("Review scope: GitHub base is parent PR #%s head %s", parent_number, github_base)
        return github_base

    def compare(base: str, head: str) -> dict:
        if base == head:
            return {"status": "identical", "merge_base_commit": {"sha": base}}
        result = get_json(f"repos/{repository}/compare/{base}...{head}")
        _validate_sha(result["merge_base_commit"]["sha"], "comparison merge base")
        if result["status"] not in {"ahead", "behind", "diverged", "identical"}:
            raise ValueError("invalid comparison status")
        return result

    def is_ancestor(base: str, head: str) -> bool:
        result = compare(base, head)
        return (
            result["status"] in {"ahead", "identical"}
            and result["merge_base_commit"]["sha"] == base
        )

    if parent.get("merged_at"):
        merge_sha = _validate_sha(parent["merge_commit_sha"], "parent merge")
        if is_ancestor(merge_sha, head_sha) and is_ancestor(merge_sha, github_base):
            _LOG.info(
                "Review scope: merged parent PR #%s; GitHub base %s", parent_number, github_base
            )
            return github_base

    parent_comparison = compare(parent_head, head_sha)
    review_base = parent_comparison["merge_base_commit"]["sha"]
    if review_base == head_sha:
        raise ValueError("no child changes remain after the stack parent")
    if parent_comparison["status"] != "ahead" or review_base != parent_head:
        raise AmbiguousStackRange(
            int(parent_number),
            "stack parent and child histories have diverged; sync the stack before reviewing"
        )
    boundary_fork = compare(github_base, review_base)["merge_base_commit"]["sha"]
    if boundary_fork == review_base:
        _LOG.info(
            "Review scope: parent PR #%s is already in GitHub base %s", parent_number, github_base
        )
        return github_base
    head_fork = compare(github_base, head_sha)["merge_base_commit"]["sha"]
    if head_fork != boundary_fork:
        raise AmbiguousStackRange(
            int(parent_number),
            "child merged base-branch changes absent from its parent; refresh the stack"
        )
    _LOG.info("Review scope: inherited parent PR #%s head %s", parent_number, review_base)
    return review_base


def _stack_entry(body: str, repository: str, number: int) -> tuple[str, str | None] | None:
    current_link = re.compile(rf"\]\(https://github\.com/{re.escape(repository)}/pull/{number}\)")
    parents: list[tuple[int, str]] = []
    entry = None
    in_stack = False
    has_stack = False
    hidden_stack_fences: set[int] = set()
    fence_start = None
    root_indent = None
    for _, raw_line, fence_start in markdown_lines(body):
        line = raw_line.rstrip("\r\n")
        if fence_start is not None:
            if _STACK_HEADING.fullmatch(line):
                hidden_stack_fences.add(fence_start)
            continue
        if _STACK_HEADING.fullmatch(line):
            in_stack = True
            has_stack = True
            parents.clear()
            root_indent = None
            continue
        if re.match(r"^#{1,6}\s", line) or _HORIZONTAL_RULE.fullmatch(line):
            in_stack = False
            continue
        item = re.match(r"^([ \t]*)[-*]\s+(.+)", line)
        if not in_stack or item is None:
            continue
        indent = len(item[1].expandtabs())
        if root_indent is None:
            root_indent = indent
        elif indent <= root_indent:
            raise ValueError("ambiguous flat stack list; indent children beneath their parent")
        while parents and parents[-1][0] >= indent:
            parents.pop()
        if current_link.search(item[2]):
            if entry is not None:
                raise ValueError("multiple stack entries for the current PR")
            entry = (item[2], parents[-1][1] if parents else None)
        parents.append((indent, item[2]))
    if fence_start in hidden_stack_fences:
        raise ValueError("unclosed code fence hides a stack description; refresh the description")
    if has_stack and entry is None:
        raise ValueError("stack description has no entry for the current PR")
    return entry


def _validate_sha(value: str, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError(f"invalid {label} SHA")
    return value


def _github_json(endpoint: str) -> dict:
    fields = (
        "{status, merge_base_commit: {sha: .merge_base_commit.sha}}"
        if "/compare/" in endpoint
        else "{head: {sha: .head.sha}, merged_at, merge_commit_sha}"
    )
    return json.loads(subprocess.check_output(["gh", "api", endpoint, "--jq", fields], text=True))


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument(
        "--outcome", type=Path, help="Write ambiguous-ancestry diagnostics for the repair comment"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        metadata = json.loads(args.metadata.read_text())
        review_base = resolve_review_base(metadata, args.repository, _github_json)
    except AmbiguousStackRange as error:
        if args.outcome:
            args.outcome.write_text(json.dumps({
                "outcome": "ambiguous_ancestry",
                "pr_number": metadata["number"],
                "head_sha": metadata["head"]["sha"],
                "parent_number": error.parent_number,
                "reason": error.reason,
                "suggested_fix": error.suggested_fix,
            }, indent=2) + "\n")
        raise SystemExit(f"Cannot resolve PR review scope: {error}") from error
    except (KeyError, TypeError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Cannot resolve PR review scope: {error}") from error
    print(review_base)


if __name__ == "__main__":
    _main()
