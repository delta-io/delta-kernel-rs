"""Repeat-review selection and publication safety regressions."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))
from review_incremental import (
    latest_reviewed_revision,
    publication_findings,
    revision_marker,
    select_review_diff,
)
from review_publish import BOT_MARKER, format_review_body, strip_review_body

BASE, OLD, HEAD = "a" * 40, "b" * 40, "c" * 40


def history(*, login="github-actions", author_type="Bot", marker=True, commit=OLD):
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "reviews": {
                        "nodes": [
                            {
                                "author": {"login": login, "__typename": author_type},
                                "body": BOT_MARKER
                                + (revision_marker(BASE, OLD) if marker else "legacy review"),
                                "submittedAt": "2026-11-13T01:00:00Z",
                                "commit": {"oid": commit},
                            }
                        ]
                    }
                }
            }
        }
    }


class IncrementalReviewTest(unittest.TestCase):
    def setUp(self):
        self.metadata = {
            "base": {"sha": BASE, "repo": {"full_name": "delta-io/delta-kernel-rs"}},
            "head": {"sha": HEAD},
        }
        self.comparison = {
            "status": "ahead",
            "merge_base_commit": {"sha": OLD},
            "base_commit": {"sha": OLD},
            "files": [{"filename": "a.rs"}],
            "total_commits": 1,
            "commits": [{"parents": [{"sha": OLD}]}],
        }
        self.calls = []

    def compare(self, repository, base, head, diff):
        self.calls.append((repository, base, head, diff))
        return "incremental diff" if diff else self.comparison

    def select(self, document=None):
        return select_review_diff(
            metadata=self.metadata,
            history=history() if document is None else document,
            full_diff="complete PR diff",
            compare=self.compare,
        )

    def assertFull(self, document=None):
        diff, reason, incremental = self.select(document)
        self.assertEqual(diff, "complete PR diff")
        self.assertFalse(incremental)
        self.assertIn("Full PR review", reason)

    def test_verified_ancestor_uses_incremental_range(self):
        diff, scope, incremental = self.select()
        self.assertTrue(incremental)
        self.assertEqual(diff, "incremental diff")
        self.assertIn(OLD + ".." + HEAD, scope)
        self.assertEqual([c[1:] for c in self.calls], [(OLD, HEAD, False), (OLD, HEAD, True)])

    def test_same_head_reviews_description_without_comparison(self):
        self.metadata["head"]["sha"] = OLD
        diff, scope, incremental = self.select()
        self.assertEqual(diff, "")
        self.assertTrue(incremental)
        self.assertIn("description", scope)
        self.assertEqual(self.calls, [])

    def test_base_change_requires_full_review(self):
        self.metadata["base"]["sha"] = "d" * 40
        self.assertFull()
        self.assertEqual(self.calls, [])

    def test_untrusted_or_legacy_baselines_require_full_review(self):
        for document in (
            {},
            history(login="another-bot"),
            history(author_type="User"),
            history(marker=False),
            history(commit=HEAD),
        ):
            with self.subTest(document=document):
                self.assertFull(document)
        self.assertEqual(self.calls, [])

    def test_newest_incomplete_review_does_not_reuse_older_baseline(self):
        document = history()
        nodes = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"]
        newest = copy.deepcopy(nodes[0])
        newest.update(body=BOT_MARKER + "legacy review", submittedAt="2026-11-13T02:00:00Z")
        nodes.append(newest)
        self.assertFull(document)

    def test_ambiguous_marker_requires_full_review(self):
        document = history()
        node = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        node["body"] += revision_marker(BASE, HEAD)
        self.assertIsNone(latest_reviewed_revision(document))

    def test_rebase_and_malformed_comparisons_require_full_review(self):
        for update in (
            {"status": "diverged"},
            {"merge_base_commit": {"sha": BASE}},
            {"merge_base_commit": None},
            {"base_commit": "bad"},
            {"files": None},
            {"files": [{}] * 300},
            {"files": [{}] * 101},
        ):
            with self.subTest(update=update):
                previous = self.comparison.copy()
                self.comparison.update(update)
                self.assertFull()
                self.comparison = previous

    def test_api_failure_and_empty_diff_require_full_review(self):
        def unavailable(*args):
            raise OSError("unavailable")

        self.compare = unavailable
        self.assertFull()
        self.compare = lambda *args: "" if args[-1] else self.comparison
        self.assertFull()

    def test_previous_head_deleted_lines_cannot_be_published_inline(self):
        findings = [{"id": "Blocker1", "side": "LEFT"}, {"id": "Blocker2", "side": "RIGHT"}]
        self.assertEqual(publication_findings(findings, True), [findings[1]])
        self.assertEqual(publication_findings(findings, False), findings)

    def test_workflow_marker_does_not_change_duplicate_body(self):
        for collapsed in (False, True):
            body = format_review_body(
                "Review text\n\n" + revision_marker(BASE, HEAD),
                "https://github.com/example/actions/runs/1",
                collapsed=collapsed,
            )
            self.assertEqual(strip_review_body(body), "Review text")

    def test_major_rework_requires_full_review(self):
        from review_incremental import major_change_reason

        for files, diff in [
            ([{"filename": "Cargo.toml"}], "+dependency = '2'"),
            ([{"filename": "kernel/api.rs"}], "+pub fn changed() {}"),
            ([{"filename": "kernel/api.rs"}], "+    value: Option<i64>,"),
            ([{"filename": "kernel/api.rs"}], "+) -> Result<i64> {"),
            ([{"filename": "kernel/api.rs", "status": "renamed"}], "+code"),
            ([{"filename": "a.rs"}], "\n".join(["+changed"] * 201)),
            ([{"filename": f"{i}.rs"} for i in range(21)], "+code"),
        ]:
            with self.subTest(files=files):
                self.assertIsNotNone(major_change_reason(files, diff))
        self.assertIsNone(major_change_reason([{"filename": "a.rs"}], "+    return Err(error);"))
        self.compare = lambda *args: "+pub fn changed() {}" if args[-1] else self.comparison
        self.assertFull()

    def test_explicit_full_review_overrides_even_unchanged_head(self):
        self.metadata["head"]["sha"] = OLD
        diff, reason, incremental = select_review_diff(
            metadata=self.metadata,
            history=history(),
            full_diff="whole PR",
            force_full=True,
            compare=self.compare,
        )
        self.assertEqual(diff, "whole PR")
        self.assertFalse(incremental)
        self.assertIn("explicit", reason)
        self.assertEqual(self.calls, [])

    def test_clean_review_advances_baseline_without_republishing_same_revision(self):
        from review_history import is_duplicate_review

        document = history()
        node = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        node["body"] = BOT_MARKER + "No blocking issues\n" + revision_marker(BASE, OLD)
        current = "No blocking issues\n" + revision_marker(BASE, HEAD)
        self.assertFalse(is_duplicate_review(current, document))
        node["body"] = BOT_MARKER + current
        node["commit"]["oid"] = HEAD
        self.assertTrue(is_duplicate_review(current, document))
        self.assertEqual(latest_reviewed_revision(document), (BASE, HEAD))

    def test_workflow_passes_authorized_override_to_context_builder(self):
        import yaml

        document = yaml.safe_load(
            (Path(__file__).parents[2] / "workflows/ai-review.yml").read_text()
        )
        context_steps = [
            step
            for job in document["jobs"].values()
            for step in job.get("steps", [])
            if step.get("id") == "context"
        ]
        self.assertEqual(len(context_steps), 1)
        context = context_steps[0]
        self.assertEqual(
            context["env"]["FORCE_FULL_REVIEW"], "${{ needs.authorize.outputs.full_review }}"
        )
        self.assertIn('force_full=os.environ.get("FORCE_FULL_REVIEW") == "true"', context["run"])
        self.assertIn("Complete PR diff for title and description verification", context["run"])

    def test_issue_comments_cannot_establish_review_baseline(self):
        document = history()
        pr = document["data"]["repository"]["pullRequest"]
        comment = pr["reviews"]["nodes"].pop()
        comment["createdAt"] = comment.pop("submittedAt")
        comment.pop("commit")
        pr["comments"] = {"nodes": [comment]}
        self.assertFull(document)

    def test_review_requires_matching_commit_metadata(self):
        document = history()
        document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0].pop("commit")
        self.assertFull(document)

    def test_merge_or_incomplete_commit_list_requires_full_review(self):
        for update in (
            {"commits": [{"parents": [{"sha": OLD}, {"sha": BASE}]}]},
            {"total_commits": 2},
            {"commits": None},
            {"commits": [{}]},
        ):
            with self.subTest(update=update):
                previous = self.comparison.copy()
                self.comparison.update(update)
                self.assertFull()
                self.comparison = previous

    def test_truncated_or_fallback_review_does_not_advance_baseline(self):
        from review_incremental import fallback_review_body, stamp_complete_review

        review = "## Blocking issues\n### Blocker1\nKeep this finding."
        self.assertEqual(stamp_complete_review(review, BASE, HEAD, truncated=True), review)
        stamped = stamp_complete_review(review, BASE, HEAD, truncated=False)
        self.assertIn(revision_marker(BASE, HEAD), stamped)
        model_text = review + "\n" + revision_marker(BASE, OLD)
        self.assertEqual(stamp_complete_review(model_text, BASE, HEAD, truncated=True), review)
        self.assertEqual(stamp_complete_review(model_text, BASE, HEAD, truncated=False), stamped)
        fallback = fallback_review_body(stamped, "https://github.com/example/actions/runs/1")
        self.assertIn("Keep this finding", fallback)
        self.assertNotIn("ai-review-revision", fallback)

    def test_invalid_revision_is_rejected(self):
        with self.assertRaises(ValueError):
            revision_marker("main", HEAD)


if __name__ == "__main__":
    unittest.main()
