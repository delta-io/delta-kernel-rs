"""Reconcile a pull request's native merge request with its `automerge` label.

Applying the label requests GitHub's merge-when-ready behavior and declares the description
final. The controller reads live PR state, authorizes the latest active label application,
removes a recognized generated stack prefix once, and then asks GitHub to merge when its
required checks and reviews pass. Removing the label cancels waiting or dequeues where GitHub
still allows it. Every decision uses live state, so stale or duplicate events reconcile to the
same result. See CONTRIBUTING.md for the contributor-facing contract.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "omnigent"))

from review_scope import resolve_review_base  # noqa: E402
from stack_description import stack_prefix_end  # noqa: E402

LABEL = "automerge"
SUPPORTED_BASE = "main"
RECOVERY = f"Remove the '{LABEL}' label and apply it again to request merging."
_WRITE_PERMISSIONS = frozenset({"admin", "maintain", "write"})
_LOG = logging.getLogger(__name__)

PULL_REQUEST_STATE = """
query PullRequestState($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      id state isDraft merged headRefOid baseRefName baseRefOid body
      labels(first: 100) { nodes { name } }
      autoMergeRequest { enabledAt }
      mergeQueueEntry { state }
      timelineItems(last: 100, itemTypes: [LABELED_EVENT, UNLABELED_EVENT]) {
        nodes {
          __typename
          ... on LabeledEvent { actor { login } label { name } }
          ... on UnlabeledEvent { actor { login } label { name } }
        }
      }
    }
  }
}
"""
UPDATE_DESCRIPTION = """
mutation UpdateDescription($id: ID!, $body: String!) {
  updatePullRequest(input: {pullRequestId: $id, body: $body}) { clientMutationId }
}
"""
ENABLE_AUTO_MERGE = """
mutation EnableAutoMerge($id: ID!, $head: GitObjectID!) {
  enablePullRequestAutoMerge(input: {pullRequestId: $id, expectedHeadOid: $head}) {
    clientMutationId
  }
}
"""
ENQUEUE = """
mutation Enqueue($id: ID!, $head: GitObjectID!) {
  enqueuePullRequest(input: {pullRequestId: $id, expectedHeadOid: $head}) { clientMutationId }
}
"""
DISABLE_AUTO_MERGE = """
mutation DisableAutoMerge($id: ID!) {
  disablePullRequestAutoMerge(input: {pullRequestId: $id}) { clientMutationId }
}
"""
DEQUEUE = """
mutation Dequeue($id: ID!) {
  dequeuePullRequest(input: {id: $id}) { clientMutationId }
}
"""


class GitHubError(RuntimeError):
    """A GitHub API call failed or returned errors."""


class AutomergeError(RuntimeError):
    """The merge request was not honored; the message tells the maintainer how to recover."""


class GitHub(Protocol):
    def graphql(self, query: str, variables: dict) -> dict:
        """Return the `data` object of a GraphQL response."""

    def rest(self, endpoint: str) -> dict:
        """Return the JSON object of a REST GET."""


@dataclass
class PullRequest:
    id: str
    number: int
    state: str
    is_draft: bool
    merged: bool
    head: str
    base_ref: str
    base_sha: str
    body: str
    labels: set[str]
    waiting: bool
    queued: bool
    label_events: list[tuple[str, str | None, str]] = field(default_factory=list)

    @property
    def armed(self) -> bool:
        return self.waiting or self.queued

    @property
    def merge_state(self) -> str:
        if self.merged:
            return "merged"
        if self.queued:
            return "in the merge queue"
        if self.waiting:
            return "waiting to merge when its requirements pass"
        return "not requested to merge"


def reconcile(github: GitHub, repository: str, number: int) -> str:
    """Bring GitHub's merge request for the PR in line with its current label state.

    Return a maintainer-facing summary. Raise AutomergeError when the label is present but
    the merge request is not honored, or when GitHub rejected a required mutation.
    """
    pr = _read(github, repository, number)
    if pr.merged:
        return "The PR is already merged; a completed merge cannot be revoked."
    if pr.state != "OPEN":
        return f"The PR is {pr.state.lower()}; nothing to reconcile."

    requester = _active_requester(pr)
    if requester is None:
        cancelled = _cancel(github, repository, pr)
        if LABEL in pr.labels:
            raise AutomergeError(
                f"Cannot establish who applied '{LABEL}' from the label history; {cancelled} "
                + RECOVERY
            )
        return cancelled
    if not _has_write(github, repository, requester):
        cancelled = _cancel(github, repository, pr)
        raise AutomergeError(
            f"{requester} applied '{LABEL}' without write permission, so the merge request is not "
            f"honored; {cancelled} A maintainer with write access must apply the label."
        )
    if pr.is_draft:
        raise AutomergeError(f"The PR is a draft. Mark it ready for review first. {RECOVERY}")
    if pr.base_ref != SUPPORTED_BASE:
        raise AutomergeError(
            f"'{LABEL}' only supports PRs targeting {SUPPORTED_BASE}, not {pr.base_ref}."
        )
    if pr.armed:
        return f"The PR is already {pr.merge_state}; the description is left as is."

    cleanup = _clean_description(github, repository, pr)
    _request_merge(github, repository, pr)
    pr = _read(github, repository, number)
    if _active_requester(pr) is None:
        cancelled = _cancel(github, repository, pr)
        return f"{cleanup} The label was removed while requesting the merge; {cancelled}"
    if not pr.armed and not pr.merged:
        raise AutomergeError(
            f"{cleanup} GitHub accepted the merge request but reports none. {RECOVERY}"
        )
    return f"{cleanup} The PR is {pr.merge_state}."


def _active_requester(pr: PullRequest) -> str | None:
    """Return the actor of the latest still-active label application, or None to fail closed."""
    if LABEL not in pr.labels:
        return None
    for kind, actor, name in reversed(pr.label_events):
        if name != LABEL:
            continue
        if kind == "LabeledEvent" and actor:
            return actor
        _LOG.info("Latest '%s' event is %s by %s; no active merge request", LABEL, kind, actor)
        return None
    _LOG.info("'%s' is present but its application is absent from the recent label history", LABEL)
    return None


def _has_write(github: GitHub, repository: str, login: str) -> bool:
    try:
        permission = github.rest(f"repos/{repository}/collaborators/{login}/permission")
    except GitHubError as error:
        _LOG.info("Cannot read %s's permission: %s", login, error)
        return False
    return permission.get("permission") in _WRITE_PERMISSIONS


def _clean_description(github: GitHub, repository: str, pr: PullRequest) -> str:
    end = stack_prefix_end(pr.body, repository, pr.number)
    if end is None:
        return "The description has no supported generated stack prefix."
    metadata = {
        "number": pr.number,
        "body": pr.body,
        "head": {"sha": pr.head},
        "base": {"sha": pr.base_sha},
    }
    try:
        review_base = resolve_review_base(metadata, repository, github.rest)
    except (KeyError, TypeError, ValueError, GitHubError) as error:
        _LOG.info("Cannot resolve the AI review scope: %s", error)
        review_base = None
    if review_base != pr.base_sha:
        return "The stack list still scopes AI review, so the description is left unchanged."
    try:
        github.graphql(UPDATE_DESCRIPTION, {"id": pr.id, "body": pr.body[end:]})
    except GitHubError as error:
        raise AutomergeError(
            f"Could not remove the stack prefix from the description: {error}. {RECOVERY}"
        ) from error
    return "Removed the generated stack prefix from the description."


def _request_merge(github: GitHub, repository: str, pr: PullRequest) -> None:
    """Ask GitHub to merge when ready, or enqueue immediately when waiting is rejected."""
    variables = {"id": pr.id, "head": pr.head}
    rejections = []
    for mutation in (ENABLE_AUTO_MERGE, ENQUEUE):
        try:
            github.graphql(mutation, variables)
            return
        except GitHubError as error:
            rejections.append(str(error))
            _LOG.info("Merge request rejected: %s", error)
        current = _read(github, repository, pr.number)
        if current.armed or current.merged:
            return
        if current.head != pr.head:
            raise AutomergeError(
                f"The PR head changed from {pr.head} to {current.head} while requesting the "
                f"merge. {RECOVERY}"
            )
    raise AutomergeError(f"GitHub rejected the merge request: {'; '.join(rejections)}. {RECOVERY}")


def _cancel(github: GitHub, repository: str, pr: PullRequest) -> str:
    """Best-effort cancellation of waiting and queued state; GitHub may already be merging."""
    if not pr.armed:
        return "No merge request is active."
    try:
        if pr.queued:
            github.graphql(DEQUEUE, {"id": pr.id})
        if pr.waiting:
            github.graphql(DISABLE_AUTO_MERGE, {"id": pr.id})
    except GitHubError as error:
        _LOG.info("Cancellation rejected: %s", error)
    current = _read(github, repository, pr.number)
    if current.merged:
        return "GitHub completed the merge before it could be cancelled."
    if current.armed:
        raise AutomergeError(
            f"Could not cancel the merge request; the PR is still {current.merge_state}. "
            "Check the merge queue, then retry by removing the label again."
        )
    return "Cancelled the merge request."


def _read(github: GitHub, repository: str, number: int) -> PullRequest:
    owner, name = repository.split("/")
    data = github.graphql(PULL_REQUEST_STATE, {"owner": owner, "name": name, "number": number})
    node = (data.get("repository") or {}).get("pullRequest")
    if node is None:
        raise AutomergeError(f"PR #{number} was not found in {repository}.")
    events = []
    for item in node["timelineItems"]["nodes"]:
        actor = item.get("actor") or {}
        events.append((item["__typename"], actor.get("login"), item["label"]["name"]))
    return PullRequest(
        id=node["id"],
        number=number,
        state=node["state"],
        is_draft=node["isDraft"],
        merged=node["merged"],
        head=node["headRefOid"],
        base_ref=node["baseRefName"],
        base_sha=node["baseRefOid"],
        body=node["body"] or "",
        labels={label["name"] for label in node["labels"]["nodes"]},
        waiting=node["autoMergeRequest"] is not None,
        queued=node["mergeQueueEntry"] is not None,
        label_events=events,
    )


class GhClient:
    """GitHub access through the authenticated `gh` CLI."""

    def graphql(self, query: str, variables: dict) -> dict:
        payload = json.dumps({"query": query, "variables": variables})
        response = self._run(["gh", "api", "graphql", "--input", "-"], payload)
        if response.get("errors"):
            raise GitHubError("; ".join(error.get("message", "") for error in response["errors"]))
        return response.get("data") or {}

    def rest(self, endpoint: str) -> dict:
        return self._run(["gh", "api", endpoint])

    @staticmethod
    def _run(command: list[str], stdin: str | None = None) -> dict:
        result = subprocess.run(command, input=stdin, capture_output=True, text=True, check=False)
        try:
            response = json.loads(result.stdout) if result.stdout.strip() else {}
        except json.JSONDecodeError:
            response = {}
        if result.returncode != 0:
            messages = [error.get("message", "") for error in response.get("errors", [])]
            raise GitHubError("; ".join(messages) or result.stderr.strip() or "gh api failed")
        return response


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr", type=int, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository) is None:
        raise SystemExit("invalid repository")
    try:
        message = reconcile(GhClient(), args.repository, args.pr)
    except (AutomergeError, GitHubError, KeyError, TypeError) as error:
        print(f"::error::{error}")
        raise SystemExit(1) from error
    print(f"::notice::{message}")


if __name__ == "__main__":
    _main()
