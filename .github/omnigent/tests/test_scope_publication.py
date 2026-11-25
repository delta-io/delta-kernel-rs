"""Verify ambiguity guidance, trusted retry reuse and evidence retained before failed writes."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1]))
try:
    import scope_publication
    from scope_publication import publish_ambiguity, render_ambiguity_message
finally:
    sys.path.pop(0)

REPOSITORY = "example/project"
RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/123"
HEAD = "a" * 40
OUTCOME = {
    "outcome": "ambiguous_ancestry", "pr_number": 12, "parent_number": 11,
    "head_sha": HEAD, "reason": "the declared parent is not inherited by this branch",
    "suggested_fix": (
        "rebase this PR's branch onto PR #11's current head, push the branch, "
        "then comment `/review inline`"
    ),
}


class ScopeGitHub:
    """Keep accepted comments when the simulated POST response is lost."""

    def __init__(self):
        self.comments = []
        self.reads = []
        self.posts = []
        self.patches = []
        self.read_error = None
        self.post_failure = None
        self.patch_failure = None

    def write(self, method, endpoint, payload):
        if method == "POST":
            return self.post(endpoint, payload)
        self.patches.append((endpoint, payload))
        if self.patch_failure == "unobserved":
            raise OSError("PATCH response unavailable")
        identifier = int(endpoint.rsplit("/", 1)[1])
        comment = next(item for item in self.comments if item.get("id") == identifier)
        comment.update(payload)
        if self.patch_failure == "accepted-response-lost":
            raise subprocess.TimeoutExpired(["gh", "api"], 60)
        return comment.copy()

    def read(self, endpoint):
        self.reads.append(endpoint)
        if self.read_error is not None:
            raise self.read_error
        page = int(endpoint.rsplit("page=", 1)[1])
        return self.comments[(page - 1) * 100:page * 100]

    def post(self, endpoint, payload):
        self.posts.append((endpoint, payload))
        if self.post_failure == "unobserved":
            raise OSError("POST response unavailable")
        identifier = len(self.comments) + 1
        comment = {
            **payload, "id": identifier,
            "user": {"type": "Bot", "login": "github-actions[bot]"},
            "html_url": f"https://github.com/{REPOSITORY}/pull/12#issuecomment-{identifier}",
        }
        self.comments.append(comment)
        if self.post_failure == "accepted-response-lost":
            raise subprocess.TimeoutExpired(["gh", "api"], 60)
        return comment.copy()


def publish(github, *, outcome=None, run_url=RUN_URL):
    return publish_ambiguity(
        OUTCOME if outcome is None else outcome, REPOSITORY, run_url,
        github.read, github.write,
    )


class ScopePublicationTest(unittest.TestCase):
    def test_reruns_on_the_same_sha_reuse_one_comment(self):
        github = ScopeGitHub()
        first = publish(github)
        second = publish(github, run_url=RUN_URL + "4")

        self.assertEqual(first, second)
        self.assertEqual(len(github.posts), 1)
        self.assertEqual(len(github.comments), 1)
        self.assertIn("**AI review did not run.**", github.comments[0]["body"])
        self.assertIn("`/review inline`", github.comments[0]["body"])
        self.assertIn(RUN_URL + "4", github.comments[0]["body"])
        self.assertEqual(len(github.patches), 1)

    def test_changed_parent_and_reason_refresh_the_single_same_sha_comment(self):
        github = ScopeGitHub()
        original = publish(github)
        updated = {
            **OUTCOME, "parent_number": 22, "reason": "parent #22 is not inherited",
            "suggested_fix": (
                "rebase this PR's branch onto PR #22's current head, push the branch, "
                "then comment `/review inline`"
            ),
        }

        self.assertEqual(publish(github, outcome=updated), original)
        self.assertEqual(len(github.comments), 1)
        self.assertEqual(len(github.posts), 1)
        self.assertEqual(len(github.patches), 1)
        self.assertEqual(
            github.comments[0]["body"].split("\n", 1)[1],
            render_ambiguity_message(updated, RUN_URL),
        )
        self.assertNotIn("#11", github.comments[0]["body"])

    def test_uncertain_updates_require_confirmation_of_the_current_guidance(self):
        for failure in ("accepted-response-lost", "unobserved"):
            with self.subTest(failure=failure):
                github = ScopeGitHub()
                original = publish(github)
                github.patch_failure = failure
                updated = {**OUTCOME, "reason": "the current repair reason"}
                if failure == "accepted-response-lost":
                    self.assertEqual(publish(github, outcome=updated), original)
                    self.assertIn(updated["reason"], github.comments[0]["body"])
                else:
                    with self.assertRaisesRegex(OSError, "PATCH response unavailable"):
                        publish(github, outcome=updated)
                    self.assertNotIn(updated["reason"], github.comments[0]["body"])
                self.assertEqual(len(github.comments), 1)
                self.assertEqual(len(github.posts), 1)
                self.assertEqual(len(github.patches), 1)

    def test_a_new_sha_gets_a_separate_comment(self):
        github = ScopeGitHub()
        first = publish(github)
        second = publish(github, outcome={**OUTCOME, "head_sha": "b" * 40})

        self.assertNotEqual(first, second)
        self.assertEqual(len(github.posts), 2)
        self.assertIn(HEAD, github.comments[0]["body"])
        self.assertIn("b" * 40, github.comments[1]["body"])

    def test_reuse_finds_a_trusted_comment_beyond_the_first_page(self):
        github = ScopeGitHub()
        original = publish(github)
        github.comments[:0] = [{
            "body": "Unrelated comment.", "user": {"type": "User", "login": "author"},
        } for _ in range(100)]
        github.posts.clear()

        self.assertEqual(publish(github), original)
        self.assertEqual(github.posts, [])
        self.assertTrue(any(endpoint.endswith("page=2") for endpoint in github.reads))

    def test_matching_markers_from_humans_or_other_bots_do_not_suppress_guidance(self):
        marker = f"<!-- ai-review-ambiguous-ancestry {HEAD} -->"
        for user_type, login in (("User", "github-actions[bot]"), ("Bot", "other-app[bot]")):
            with self.subTest(user_type=user_type, login=login):
                github = ScopeGitHub()
                forged_url = f"https://github.com/{REPOSITORY}/pull/12#issuecomment-forged"
                github.comments.append({
                    "body": marker + "\nForged guidance.",
                    "user": {"type": user_type, "login": login}, "html_url": forged_url,
                })

                confirmed = publish(github)

                self.assertNotEqual(confirmed, forged_url)
                self.assertEqual(len(github.posts), 1)
                self.assertEqual(len(github.comments), 2)

    def test_lost_accepted_post_response_is_reconciled_without_a_duplicate(self):
        github = ScopeGitHub()
        github.post_failure = "accepted-response-lost"
        confirmed = publish(github)

        self.assertEqual(confirmed, github.comments[0]["html_url"])
        self.assertEqual(len(github.posts), 1)
        self.assertEqual(len(github.comments), 1)
        self.assertEqual(publish(github), confirmed)
        self.assertEqual(len(github.posts), 1)

    def test_failed_history_reads_never_permit_a_post(self):
        for error in (
            OSError("history unavailable"),
            subprocess.CalledProcessError(1, ["gh", "api"]),
            ValueError("history response is not JSON"),
        ):
            with self.subTest(error=type(error).__name__):
                github = ScopeGitHub()
                github.read_error = error

                with self.assertRaises(type(error)):
                    publish(github)

                self.assertEqual(github.posts, [])
                self.assertEqual(github.comments, [])

    def test_unobserved_post_without_a_matching_comment_propagates_the_failure(self):
        github = ScopeGitHub()
        github.post_failure = "unobserved"

        with self.assertRaisesRegex(OSError, "POST response unavailable"):
            publish(github)

        self.assertEqual(len(github.posts), 1)
        self.assertEqual(len(github.reads), 2)
        self.assertEqual(github.comments, [])

    def test_cli_retains_outcome_and_matching_check_text_before_a_failed_post(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outcome_path, check_path = root / "outcome.json", root / "check.md"
            outcome_path.write_text(json.dumps(OUTCOME))
            arguments = [
                "scope_publication.py", "--outcome", str(outcome_path),
                "--check-output", str(check_path), "--repository", REPOSITORY,
                "--run-url", RUN_URL,
            ]
            github = ScopeGitHub()
            github.post_failure = "unobserved"

            def transport(method, endpoint, payload=None):
                if method == "GET":
                    return github.read(endpoint)
                retained = json.loads(outcome_path.read_text())
                self.assertEqual(retained["run_url"], RUN_URL)
                self.assertEqual(check_path.read_text(), payload["body"].split("\n", 1)[1])
                return github.write(method, endpoint, payload)

            with patch.object(sys, "argv", arguments), patch.object(
                scope_publication, "_github_json", side_effect=transport,
            ):
                with self.assertRaisesRegex(OSError, "POST response unavailable"):
                    scope_publication.main()

            retained = json.loads(outcome_path.read_text())
            expected = {**OUTCOME, "run_url": RUN_URL}
            self.assertEqual(retained, expected)
            self.assertEqual(check_path.read_text(), render_ambiguity_message(expected, RUN_URL))
            self.assertEqual(len(github.posts), 1)


if __name__ == "__main__":
    unittest.main()
