"""Tests for the `automerge` label controller."""

from __future__ import annotations

import copy
import json
import re
import subprocess
import sys
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1]))

import automerge  # noqa: E402
from automerge import AutomergeError, GitHubError  # noqa: E402

REPOSITORY = "delta-io/delta-kernel-rs"
PR_URL = f"https://github.com/{REPOSITORY}/pull"
MAINTAINER = "maintainer"
CONTRIBUTOR = "contributor"
BASE = "a" * 40
PARENT = "b" * 40
HEAD = "c" * 40
FORK = "d" * 40
MERGE = "e" * 40
NEW_HEAD = "f" * 40
SUFFIX = "## What changes are proposed in this pull request?\r\n\r\nBody é\r\n"
LABELED = ("LabeledEvent", MAINTAINER, "automerge")
UNLABELED = ("UnlabeledEvent", MAINTAINER, "automerge")


def _stack_body(parent: int | None = None) -> str:
    rows = []
    indent = ""
    if parent is not None:
        rows.append(
            f"- [stack/parent]({PR_URL}/{parent}) [[Files changed]({PR_URL}/{parent}/files)]"
        )
        indent = "  "
    rows.append(
        f"{indent}- [**stack/child**]({PR_URL}/2) [[Files changed]({PR_URL}/2/files)] <- _this PR_"
    )
    intro = f"Use this [link]({PR_URL}/2/files) to review incremental changes."
    return "\n".join(["## \U0001f95e Stacked PR", intro, *rows]) + "\n\n---------\n" + SUFFIX


def _node(
    body: str = SUFFIX,
    labels: tuple[str, ...] = ("automerge",),
    events: tuple[tuple[str, str | None, str], ...] = (LABELED,),
    waiting: bool = False,
    queued: bool = False,
    draft: bool = False,
    state: str = "OPEN",
    merged: bool = False,
    head: str = HEAD,
    base_ref: str = "main",
) -> dict:
    return {
        "id": "PR_node",
        "state": state,
        "isDraft": draft,
        "merged": merged,
        "headRefOid": head,
        "baseRefName": base_ref,
        "baseRefOid": BASE,
        "body": body,
        "labels": {"nodes": [{"name": name} for name in labels]},
        "autoMergeRequest": {"enabledAt": "2026-01-01T00:00:00Z"} if waiting else None,
        "mergeQueueEntry": {"state": "QUEUED"} if queued else None,
        "timelineItems": {
            "nodes": [
                {
                    "__typename": kind,
                    "actor": {"login": actor} if actor else None,
                    "label": {"name": name},
                }
                for kind, actor, name in events
            ]
        },
    }


def _comparison(base: str, status: str = "ahead") -> dict:
    return {"status": status, "merge_base_commit": {"sha": base}}


class FakeGitHub:
    """Stateful stand-in for GitHub: mutations update the node that later reads return."""

    def __init__(
        self, node: dict, permissions: dict[str, str] | None = None, rest: dict | None = None
    ):
        self.node = node
        self.permissions = {MAINTAINER: "write", **(permissions or {})}
        self.rest_responses = rest or {}
        self.failures: dict[str, list[Exception]] = {}
        self.on: dict[str, Callable[[dict], None]] = {}
        self.calls: list[tuple[str, object]] = []

    def graphql(self, query: str, variables: dict) -> dict:
        operation = re.match(r"\s*(?:query|mutation)\s+(\w+)", query)[1]
        self.calls.append((operation, variables))
        if operation in self.on:
            self.on[operation](self.node)
        if self.failures.get(operation):
            raise self.failures[operation].pop(0)
        if operation == "PullRequestState":
            return {"repository": {"pullRequest": copy.deepcopy(self.node)}}
        if operation == "UpdateDescription":
            self.node["body"] = variables["body"]
        elif operation == "EnableAutoMerge":
            self.node["autoMergeRequest"] = {"enabledAt": "now"}
        elif operation == "Enqueue":
            self.node["mergeQueueEntry"] = {"state": "QUEUED"}
        elif operation == "DisableAutoMerge":
            self.node["autoMergeRequest"] = None
        elif operation == "Dequeue":
            self.node["mergeQueueEntry"] = None
        else:
            raise AssertionError(f"unexpected operation {operation}")
        return {}

    def rest(self, endpoint: str) -> dict:
        self.calls.append(("REST", endpoint))
        permission = re.fullmatch(r"repos/[^/]+/[^/]+/collaborators/([^/]+)/permission", endpoint)
        if permission is not None:
            if permission[1] not in self.permissions:
                raise GitHubError("HTTP 404: Not Found")
            return {"permission": self.permissions[permission[1]]}
        if endpoint not in self.rest_responses:
            raise GitHubError(f"HTTP 500 for {endpoint}")
        return self.rest_responses[endpoint]

    def operations(self) -> list[str]:
        return [operation for operation, _ in self.calls if operation != "REST"]

    def mutations(self) -> list[str]:
        return [operation for operation in self.operations() if operation != "PullRequestState"]


def _remove_label(node: dict) -> None:
    node["labels"]["nodes"] = []
    node["timelineItems"]["nodes"].append(
        {
            "__typename": "UnlabeledEvent",
            "actor": {"login": MAINTAINER},
            "label": {"name": "automerge"},
        }
    )


class ReconcileTest(unittest.TestCase):
    def reconcile(self, fake: FakeGitHub) -> str:
        return automerge.reconcile(fake, REPOSITORY, 2)

    def assert_not_honored(self, fake: FakeGitHub, *fragments: str) -> None:
        with self.assertRaises(AutomergeError) as raised:
            self.reconcile(fake)
        for fragment in fragments:
            self.assertIn(fragment, str(raised.exception))

    def test_maintainer_label_cleans_description_then_requests_merge_when_ready(self) -> None:
        fake = FakeGitHub(_node(body=_stack_body()))
        message = self.reconcile(fake)
        self.assertEqual(
            fake.operations(),
            ["PullRequestState", "UpdateDescription", "EnableAutoMerge", "PullRequestState"],
        )
        self.assertEqual(fake.calls[2][1], {"id": "PR_node", "body": SUFFIX})
        self.assertEqual(fake.calls[3][1], {"id": "PR_node", "head": HEAD})
        self.assertEqual(fake.node["body"], SUFFIX)
        self.assertIn("Removed the generated stack prefix", message)
        self.assertIn("waiting to merge", message)

    def test_unsupported_or_clean_description_is_preserved_without_blocking_merge(self) -> None:
        prose = "## \U0001f95e Stacked PR\n\nSplitting #1 per review feedback.\n\n---\n" + SUFFIX
        cases = {
            "already clean": SUFFIX,
            "empty": "",
            "author prose": prose,
            "plain heading": _stack_body().replace("\U0001f95e ", "", 1),
            "extra heading space": _stack_body().replace("## ", "##  ", 1),
            "changed intro": _stack_body().replace("incremental", "scoped", 1),
            "different terminator": _stack_body().replace("\n---------\n", "\n---\n", 1),
        }
        for scenario, body in cases.items():
            with self.subTest(scenario=scenario):
                fake = FakeGitHub(_node(body=body))
                message = self.reconcile(fake)
                self.assertEqual(fake.mutations(), ["EnableAutoMerge"])
                self.assertEqual(fake.node["body"], body)
                self.assertIn("no supported generated stack prefix", message)

    def test_already_waiting_or_queued_pr_is_left_alone(self) -> None:
        for state in [{"waiting": True}, {"queued": True}, {"waiting": True, "queued": True}]:
            with self.subTest(**state):
                fake = FakeGitHub(_node(body=_stack_body(), **state))
                message = self.reconcile(fake)
                self.assertEqual(fake.operations(), ["PullRequestState"])
                self.assertEqual(fake.node["body"], _stack_body())
                self.assertIn("already", message)

    def test_rejected_waiting_request_falls_back_to_immediate_enqueue(self) -> None:
        fake = FakeGitHub(_node())
        fake.failures["EnableAutoMerge"] = [GitHubError("Pull request is in clean status")]
        message = self.reconcile(fake)
        self.assertEqual(fake.mutations(), ["EnableAutoMerge", "Enqueue"])
        self.assertEqual(fake.calls[-2][1], {"id": "PR_node", "head": HEAD})
        self.assertIn("in the merge queue", message)

    def test_rejected_requests_are_reported_once_without_retry(self) -> None:
        fake = FakeGitHub(_node())
        fake.failures["EnableAutoMerge"] = [GitHubError("expected head oid mismatch")]
        fake.failures["Enqueue"] = [GitHubError("expected head oid mismatch")]
        self.assert_not_honored(
            fake, "GitHub rejected the merge request", "expected head oid", automerge.RECOVERY
        )
        self.assertEqual(fake.mutations(), ["EnableAutoMerge", "Enqueue"])

    def test_head_change_during_request_is_reported_without_enqueue(self) -> None:
        fake = FakeGitHub(_node())
        fake.on["EnableAutoMerge"] = lambda node: node.update(headRefOid=NEW_HEAD)
        fake.failures["EnableAutoMerge"] = [GitHubError("expected head oid mismatch")]
        self.assert_not_honored(fake, f"head changed from {HEAD} to {NEW_HEAD}", automerge.RECOVERY)
        self.assertEqual(fake.mutations(), ["EnableAutoMerge"])

    def test_request_error_with_armed_server_state_is_accepted(self) -> None:
        fake = FakeGitHub(_node())
        fake.on["EnableAutoMerge"] = lambda node: node.update(autoMergeRequest={"enabledAt": "t"})
        fake.failures["EnableAutoMerge"] = [GitHubError("timeout")]
        message = self.reconcile(fake)
        self.assertEqual(fake.mutations(), ["EnableAutoMerge"])
        self.assertIn("waiting to merge", message)

    def test_label_removed_during_request_cancels_the_merge(self) -> None:
        fake = FakeGitHub(_node(body=_stack_body()))
        fake.on["EnableAutoMerge"] = _remove_label
        message = self.reconcile(fake)
        self.assertEqual(
            fake.mutations(), ["UpdateDescription", "EnableAutoMerge", "DisableAutoMerge"]
        )
        self.assertIsNone(fake.node["autoMergeRequest"])
        self.assertIn("label was removed", message)
        self.assertIn("Cancelled the merge request", message)

    def test_description_write_failure_stops_before_requesting_merge(self) -> None:
        fake = FakeGitHub(_node(body=_stack_body()))
        fake.failures["UpdateDescription"] = [GitHubError("HTTP 502")]
        self.assert_not_honored(
            fake, "Could not remove the stack prefix", "HTTP 502", automerge.RECOVERY
        )
        self.assertEqual(fake.mutations(), ["UpdateDescription"])

    def test_cleanup_reads_description_once_and_writes_only_the_body(self) -> None:
        fake = FakeGitHub(_node(body=_stack_body()))
        self.reconcile(fake)
        write = fake.operations().index("UpdateDescription")
        self.assertEqual(fake.operations()[:write].count("PullRequestState"), 1)
        (write_variables,) = [v for operation, v in fake.calls if operation == "UpdateDescription"]
        self.assertEqual(set(write_variables), {"id", "body"})

    def test_label_removal_cancels_waiting_and_queued_state(self) -> None:
        for state, expected in [
            ({"waiting": True}, ["DisableAutoMerge"]),
            ({"queued": True}, ["Dequeue"]),
            ({"waiting": True, "queued": True}, ["Dequeue", "DisableAutoMerge"]),
            ({}, []),
        ]:
            with self.subTest(**state):
                node = _node(body=_stack_body(), labels=(), events=(LABELED, UNLABELED), **state)
                fake = FakeGitHub(node)
                message = self.reconcile(fake)
                self.assertEqual(fake.mutations(), expected)
                self.assertEqual(fake.node["body"], _stack_body())
                self.assertIn("Cancelled" if expected else "No merge request", message)

    def test_cancellation_after_merge_completed_is_reported(self) -> None:
        fake = FakeGitHub(_node(labels=(), events=(LABELED, UNLABELED), queued=True))
        fake.on["Dequeue"] = lambda node: node.update(merged=True, state="MERGED")
        fake.failures["Dequeue"] = [GitHubError("already merging")]
        self.assertIn("completed the merge", self.reconcile(fake))

    def test_cancellation_that_leaves_the_pr_armed_is_an_error(self) -> None:
        fake = FakeGitHub(_node(labels=(), events=(LABELED, UNLABELED), queued=True))
        fake.failures["Dequeue"] = [GitHubError("locked")]
        self.assert_not_honored(fake, "Could not cancel", "in the merge queue")

    def test_duplicate_label_event_after_arming_does_nothing(self) -> None:
        fake = FakeGitHub(_node(events=(LABELED, UNLABELED, LABELED), waiting=True))
        self.reconcile(fake)
        self.assertEqual(fake.mutations(), [])

    def test_actor_without_write_permission_is_not_honored(self) -> None:
        for permission in ["read", "none", None]:
            with self.subTest(permission=permission):
                permissions = {CONTRIBUTOR: permission} if permission else {}
                events = (("LabeledEvent", CONTRIBUTOR, "automerge"),)
                fake = FakeGitHub(_node(events=events), permissions)
                self.assert_not_honored(fake, f"{CONTRIBUTOR} applied 'automerge' without write")
                self.assertEqual(fake.mutations(), [])

    def test_unauthorized_reapplication_cancels_an_earlier_request(self) -> None:
        events = (
            LABELED,
            ("UnlabeledEvent", CONTRIBUTOR, "automerge"),
            ("LabeledEvent", CONTRIBUTOR, "automerge"),
        )
        fake = FakeGitHub(_node(events=events, waiting=True), {CONTRIBUTOR: "read"})
        self.assert_not_honored(fake, "without write permission", "Cancelled the merge request")
        self.assertEqual(fake.mutations(), ["DisableAutoMerge"])

    def test_later_unrelated_label_events_do_not_change_the_requester(self) -> None:
        events = (
            LABELED,
            ("LabeledEvent", CONTRIBUTOR, "bug"),
            ("UnlabeledEvent", CONTRIBUTOR, "bug"),
        )
        fake = FakeGitHub(_node(labels=("automerge", "bug"), events=events), {CONTRIBUTOR: "read"})
        self.reconcile(fake)
        self.assertEqual(fake.mutations(), ["EnableAutoMerge"])
        permission = f"repos/{REPOSITORY}/collaborators/{MAINTAINER}/permission"
        self.assertIn(("REST", permission), fake.calls)

    def test_label_without_establishable_application_fails_closed(self) -> None:
        for events in [(), (("LabeledEvent", None, "automerge"),), (LABELED, UNLABELED)]:
            with self.subTest(events=events):
                fake = FakeGitHub(_node(events=events, waiting=True))
                self.assert_not_honored(fake, "Cannot establish who applied", automerge.RECOVERY)
                self.assertEqual(fake.mutations(), ["DisableAutoMerge"])

    def test_draft_and_unsupported_base_are_not_honored(self) -> None:
        fake = FakeGitHub(_node(body=_stack_body(), draft=True))
        self.assert_not_honored(fake, "draft", automerge.RECOVERY)
        self.assertEqual(fake.mutations(), [])
        fake = FakeGitHub(_node(body=_stack_body(), base_ref="release/0.20"))
        self.assert_not_honored(fake, "only supports PRs targeting main")
        self.assertEqual(fake.mutations(), [])

    def test_merged_or_closed_pr_needs_no_reconciliation(self) -> None:
        fake = FakeGitHub(_node(merged=True, state="MERGED", labels=()))
        self.assertIn("already merged", self.reconcile(fake))
        fake = FakeGitHub(_node(state="CLOSED", waiting=True))
        self.assertIn("closed", self.reconcile(fake))
        self.assertEqual(fake.mutations(), [])

    def test_unmerged_parent_keeps_the_stack_list_for_review_scope(self) -> None:
        rest = {
            f"repos/{REPOSITORY}/pulls/1": {"head": {"sha": PARENT}, "merged_at": None},
            f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}": _comparison(PARENT),
            f"repos/{REPOSITORY}/compare/{BASE}...{PARENT}": _comparison(FORK, "diverged"),
            f"repos/{REPOSITORY}/compare/{BASE}...{HEAD}": _comparison(FORK, "diverged"),
        }
        fake = FakeGitHub(_node(body=_stack_body(parent=1)), rest=rest)
        message = self.reconcile(fake)
        self.assertEqual(fake.mutations(), ["EnableAutoMerge"])
        self.assertIn("still scopes AI review", message)

    def test_merged_parent_inherited_by_head_and_base_allows_cleanup(self) -> None:
        rest = {
            f"repos/{REPOSITORY}/pulls/1": {
                "head": {"sha": PARENT},
                "merged_at": "2026-01-01T00:00:00Z",
                "merge_commit_sha": MERGE,
            },
            f"repos/{REPOSITORY}/compare/{MERGE}...{HEAD}": _comparison(MERGE),
            f"repos/{REPOSITORY}/compare/{MERGE}...{BASE}": _comparison(MERGE),
        }
        fake = FakeGitHub(_node(body=_stack_body(parent=1)), rest=rest)
        self.reconcile(fake)
        self.assertEqual(fake.mutations(), ["UpdateDescription", "EnableAutoMerge"])
        self.assertEqual(fake.node["body"], SUFFIX)

    def test_review_scope_lookup_failure_skips_cleanup_but_requests_merge(self) -> None:
        fake = FakeGitHub(_node(body=_stack_body(parent=1)))
        message = self.reconcile(fake)
        self.assertEqual(fake.mutations(), ["EnableAutoMerge"])
        self.assertEqual(fake.node["body"], _stack_body(parent=1))
        self.assertIn("left unchanged", message)


class GhClientTest(unittest.TestCase):
    def test_graphql_sends_json_payload_and_unwraps_data(self) -> None:
        completed = subprocess.CompletedProcess([], 0, stdout='{"data": {"ok": true}}', stderr="")
        with patch("subprocess.run", return_value=completed) as run:
            data = automerge.GhClient().graphql("query Q { x }", {"body": "a\r\né"})
        self.assertEqual(data, {"ok": True})
        self.assertEqual(run.call_args.args[0], ["gh", "api", "graphql", "--input", "-"])
        self.assertEqual(
            json.loads(run.call_args.kwargs["input"]),
            {"query": "query Q { x }", "variables": {"body": "a\r\né"}},
        )

    def test_errors_become_github_errors(self) -> None:
        failed = subprocess.CompletedProcess(
            [], 1, stdout='{"errors": [{"message": "Expected head"}]}', stderr="gh: Expected head"
        )
        with patch("subprocess.run", return_value=failed):
            with self.assertRaisesRegex(GitHubError, "Expected head"):
                automerge.GhClient().graphql("mutation M { x }", {})
        not_found = subprocess.CompletedProcess([], 1, stdout="", stderr="gh: HTTP 404: Not Found")
        with patch("subprocess.run", return_value=not_found):
            with self.assertRaisesRegex(GitHubError, "404"):
                automerge.GhClient().rest("repos/x/y/collaborators/z/permission")


if __name__ == "__main__":
    unittest.main()
