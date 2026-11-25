"""Retain distinct Blockers using publication and history provenance across reruns."""

from __future__ import annotations

import unittest

from test_review_status_publication import (
    HEAD, FakeGitHub, compute_verdict, history_from_github, pipeline, publish_status,
)

CLAIM = "A concrete retained claim."
SECOND = "b" * 40


def finding(path, *, line=1, side="RIGHT", finding_id="Blocker1"):
    return {"id": finding_id, "body": CLAIM, "path": path, "line": line, "side": side}


class ReviewIdentityTest(unittest.TestCase):
    def test_identical_claims_in_different_files_remain_distinct_on_the_same_head(self):
        github = FakeGitHub()
        first = publish_status(github, pipeline(github, "Blocker"))
        arguments = pipeline(github, findings=[finding("other.rs")])
        result = compute_verdict(**arguments)

        self.assertTrue(arguments["completion"]["complete"], arguments["completion"]["reasons"])
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 2)
        self.assertEqual({item["path"] for item in result["findings"]}, {"file.rs", "other.rs"})
        self.assertIn(first["findings"][0]["url"], {item["url"] for item in result["findings"]})

    def test_identical_claim_in_another_file_does_not_recheck_an_earlier_head_blocker(self):
        github = FakeGitHub()
        first = publish_status(github, pipeline(github, "Blocker"))
        arguments = pipeline(github, head_sha=SECOND, findings=[finding("other.rs")])
        result = compute_verdict(**arguments)

        self.assertTrue(arguments["completion"]["complete"], arguments["completion"]["reasons"])
        self.assertEqual(result["status"], "NO VERDICT")
        self.assertEqual(result["counts"]["Blocker"], 1)
        reason = next(r for r in result["reasons"] if r["code"] == "earlier_blocker_unchecked")
        self.assertEqual(reason["url"], first["findings"][0]["url"])

    def test_identical_claims_at_different_lines_remain_distinct_on_the_same_head(self):
        github = FakeGitHub()
        publish_status(github, pipeline(github, "Blocker"))
        arguments = pipeline(github, findings=[finding("file.rs", line=2)])
        result = compute_verdict(**arguments)

        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 2)
        self.assertEqual({item["line"] for item in result["findings"]}, {1, 2})
        self.assertEqual(len({item["url"] for item in result["findings"]}), 2)

    def test_conversation_findings_keep_each_location_through_history_and_reruns(self):
        github = FakeGitHub()
        entries = [
            finding("first.rs", line=5, side="LEFT"),
            finding("second.rs", line=9, finding_id="Blocker2"),
        ]
        first = publish_status(github, pipeline(github, findings=entries))
        history = history_from_github(github)

        self.assertEqual(github.inline, [])
        self.assertEqual(first["counts"]["Blocker"], 2)
        self.assertTrue(history["complete"], history["reasons"])
        self.assertEqual(
            {(item["path"], item["line"], item["side"]) for item in history["blockers"]},
            {("first.rs", 5, "LEFT"), ("second.rs", 9, "RIGHT")},
        )
        result = compute_verdict(**pipeline(github, findings=[finding("third.rs", line=7)]))
        self.assertEqual(result["counts"]["Blocker"], 3)
        self.assertEqual(
            {item["path"] for item in result["findings"]}, {"first.rs", "second.rs", "third.rs"},
        )

    def test_legacy_body_only_blocker_still_matches_without_known_location(self):
        github = FakeGitHub()
        publish_status(github, pipeline(github, "Blocker", mode="comment"), mode="comment")
        original = next(item for item in github.issues if "<!-- ai-review-finding " in item["body"])
        original["body"] = (
            "<!-- ai-review-bot -->\nReviewed `" + HEAD + "`\n### Blocker1\n" + CLAIM
        )
        history = history_from_github(github)
        self.assertTrue(history["complete"], history["reasons"])
        self.assertNotIn("path", history["blockers"][0])
        arguments = pipeline(github, head_sha=SECOND, findings=[finding("other.rs")])
        result = compute_verdict(**arguments)

        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)
        self.assertEqual(result["reasons"], [])

    def test_confirmed_reused_destination_survives_a_moved_display_anchor(self):
        github = FakeGitHub()
        first = publish_status(github, pipeline(github, "Blocker"))
        arguments = pipeline(github, "Blocker", head_sha=SECOND)
        current = arguments["publication"]["findings"][0]
        self.assertEqual(current["delivery"], "reused")
        self.assertEqual(current["url"], first["findings"][0]["url"])
        current["line"] = 20
        result = compute_verdict(**arguments)

        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)
        self.assertEqual(result["reasons"], [])

    def test_comment_delimiter_in_path_cannot_end_the_finding_metadata_marker(self):
        github = FakeGitHub()
        path = "src/odd-->name.rs"
        arguments = pipeline(github, findings=[finding(path, line=4)])
        self.assertTrue(arguments["publication"]["complete"])
        comment = next(item for item in github.issues if "<!-- ai-review-finding " in item["body"])
        marker = next(line for line in comment["body"].splitlines()
                      if line.startswith("<!-- ai-review-finding "))

        self.assertEqual(marker.count("-->"), 1)
        self.assertIn(r"\u003e", marker)
        history = history_from_github(github)
        self.assertTrue(history["complete"], history["reasons"])
        self.assertEqual(history["blockers"][0]["path"], path)
        self.assertEqual(history["blockers"][0]["line"], 4)


if __name__ == "__main__":
    unittest.main()
