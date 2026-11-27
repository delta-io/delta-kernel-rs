"""Finding placement and confirmed delivery using a paged in-memory GitHub API."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1]))
try:
    import finding_publication
    from finding_publication import (
        PublicationError, collect_findings, complete_review, publication_check,
        publish_findings, review_overview,
    )
    from review_history import canonical_finding_body
    from review_publish import BOT_MARKER, format_finding, format_review_summary, strip_review_body
finally:
    sys.path.pop(0)

HEAD_SHA = "a" * 40
RUN_URL = "https://github.com/example/project/actions/runs/1"
DIFF = (
    "diff --git a/file.rs b/file.rs\n--- a/file.rs\n+++ b/file.rs\n"
    "@@ -1,2 +1,2 @@\n-old\n+new\n context\n"
)


class FakeGitHub:
    """Store accepted writes even when a simulated response is lost."""

    def __init__(self):
        self.issues = []
        self.inline = []
        self.reviews = []
        self.writes = []
        self.reads = []
        self.failures = {}
        self.read_failure = False
        self.next_id = 1

    def read(self, path):
        self.reads.append(path)
        if self.read_failure:
            raise PublicationError("history unavailable")
        if "/issues/comments/" in path:
            return next(item for item in self.issues if item["id"] == int(path.rsplit("/", 1)[1]))
        bare, _, query = path.partition("?")
        page = int(query.rsplit("page=", 1)[1]) if query else 1
        if bare.endswith("/reviews"):
            rows = self.reviews
        elif "/pulls/" in bare:
            rows = self.inline
        else:
            rows = self.issues
        return rows[(page - 1) * 100:page * 100]

    def write(self, method, path, payload):
        self.writes.append((method, path, payload.copy()))
        if method == "PATCH":
            item = next(item for item in self.issues if item["id"] == int(path.rsplit("/", 1)[1]))
            item.update(payload)
            return item.copy()
        inline = "/pulls/" in path
        failure = next((kind for key, kind in self.failures.items() if f'"id": "{key}"' in payload["body"]), None)
        if failure == "reject" and inline:
            raise PublicationError("HTTP 422", ambiguous=False)
        if failure == "ambiguous-missing":
            raise PublicationError("response lost")
        item = {
            **payload, "id": self.next_id,
            "html_url": f"https://github.com/example/project/pull/1#comment-{self.next_id}",
            "user": {"login": "github-actions[bot]", "type": "Bot"},
        }
        self.next_id += 1
        (self.inline if inline else self.issues).append(item)
        if failure == "ambiguous-posted":
            raise PublicationError("response lost")
        return item.copy()


def finding(finding_id="Nit1", body="A retained claim.", *, line=1):
    return {"id": finding_id, "body": body, "path": "file.rs", "line": line, "side": "RIGHT"}


def publish(github, findings, **kwargs):
    arguments = dict(
        github=github, repository="example/project", pr_number=1, head_sha=HEAD_SHA,
        run_url=RUN_URL, review="## PR Summary\nChanges a call.", structured=findings,
        github_pr_patch=DIFF, pr_change_patch=DIFF, trusted_bot_logins=["github-actions"],
    )
    arguments.update(kwargs)
    return publish_findings(**arguments)


class FindingPublicationTest(unittest.TestCase):
    def test_two_threads_and_three_conversation_findings_have_confirmed_destinations(self):
        github = FakeGitHub()
        entries = [finding("Nit1"), finding("Nit2", "Another claim.", line=2)]
        entries += [finding("ShouldFix1", "Derive this value once.", line=None)]
        entries += [finding(f"Nit{i}", f"PR-wide claim {i}.", line=None) for i in (3, 4)]
        record = publish(github, entries)
        self.assertTrue(record["complete"], record["errors"])
        self.assertEqual(len(github.inline), 2)
        self.assertEqual(len(github.issues), 4)
        self.assertTrue(all(item["url"] for item in record["findings"]))
        main = github.issues[0]["body"]
        order = ["## 🤖 AI Review 🧐", "🛑 0 Blockers · 🔧 1 Should Fix · 💡 4 Nits",
                 "<summary>Overall assessment", "<summary>Key findings", "<sub>Reviewed", "Full AI review"]
        offsets = [main.index(part) for part in order]
        self.assertEqual(offsets, sorted(offsets))
        self.assertNotIn("\n---\n", main)
        self.assertEqual(main.count("Reviewed `"), 1)
        self.assertTrue(main.endswith(f"[Workflow run]({RUN_URL})</sub>"))
        self.assertNotIn("Coverage", main)
        self.assertNotIn("VERDICT", main)
        self.assertEqual(main.count("[Should Fix 1]"), 1)
        self.assertNotIn("[Nit 1]", main)
        self.assertIn("https://github.com/example/project/pull/1/checks", main)

    def test_every_valid_anchor_is_posted_beyond_twelve_findings(self):
        github = FakeGitHub()
        record = publish(github, [finding(f"Nit{i}", f"Claim {i}.") for i in range(1, 16)])
        self.assertTrue(record["complete"])
        self.assertEqual(len(github.inline), 15)

    def test_duplicate_final_labels_count_as_one_finding_with_aliases(self):
        github = FakeGitHub()
        record = publish(github, [finding("Nit1"), finding("Nit2")])

        self.assertTrue(record["complete"], record["errors"])
        self.assertEqual(len(github.inline), 1)
        self.assertEqual(len(record["findings"]), 1)
        retained = record["findings"][0]
        self.assertEqual(retained["id"], "Nit1")
        self.assertEqual(retained["aliases"], ["Nit2"])
        self.assertIn("🛑 0 Blockers · 🔧 0 Should Fix · 💡 1 Nit", github.issues[0]["body"])
        check = publication_check("## PR Summary\nChanges a call.", record)
        self.assertNotIn("## Finding publication", check)
        self.assertEqual(check.count("A retained claim."), 1)

    def test_published_main_comment_unwraps_its_workflow_metadata(self):
        github = FakeGitHub()
        publish(github, [finding()])

        body = github.issues[0]["body"]
        self.assertIn("<!-- ai-review-main ", body)
        for label in ("Overall assessment", "Assessment Summary", "PR Summary", "Summary"):
            with self.subTest(label=label):
                self.assertEqual(
                    strip_review_body(body.replace("Overall assessment", label)), "Changes a call."
                )

    def test_copied_markers_from_untrusted_authors_are_not_reused_or_updated(self):
        authors = (
            {"login": "contributor", "type": "User"},
            {"login": "github-actions", "type": "User"},
            {"login": "another-app[bot]", "type": "Bot"},
        )
        for author in authors:
            with self.subTest(author=author):
                github = FakeGitHub()
                original = publish(github, [finding()])
                copied_main = github.issues[0]
                copied_finding = github.inline[0]
                for comment in (copied_main, copied_finding):
                    comment["user"] = author.copy()
                github.writes.clear()

                record = publish(github, [finding()])

                self.assertTrue(record["complete"], record["errors"])
                self.assertEqual(record["findings"][0]["delivery"], "new")
                self.assertNotEqual(record["findings"][0]["url"], original["findings"][0]["url"])
                self.assertNotEqual(record["main_url"], original["main_url"])
                self.assertFalse(any(
                    method == "PATCH" and path.endswith(f"/{copied_main['id']}")
                    for method, path, _ in github.writes
                ))

    def test_left_findings_use_conversation_when_the_two_diff_sections_differ(self):
        wider_diff = DIFF.replace("@@ -1,2 +1,2 @@", "@@ -1,3 +1,3 @@") + " more context\n"
        for pr_change_patch, expected in ((DIFF, "inline"), (wider_diff, "conversation")):
            with self.subTest(placement=expected):
                github = FakeGitHub()
                record = publish(
                    github, [dict(finding(), side="LEFT")], pr_change_patch=pr_change_patch,
                )

                self.assertTrue(record["complete"], record["errors"])
                self.assertEqual(record["findings"][0]["placement"], expected)
                self.assertEqual(bool(github.inline), expected == "inline")

    def test_checks_use_the_same_final_structured_claim_as_the_thread(self):
        github = FakeGitHub()
        review = "### Nit1\nSuperseded prose claim.\n\n## PR Summary\nChanges a call."
        record = publish(github, [finding(body="Final retained claim.")], review=review)

        self.assertTrue(record["complete"], record["errors"])
        self.assertIn("Final retained claim.", github.inline[0]["body"])
        check = publication_check(review, record)
        self.assertIn("Final retained claim.", check)
        self.assertNotIn("Superseded prose claim.", check)

    def test_fenced_summary_and_malformed_finding_examples_are_ignored(self):
        github = FakeGitHub()
        review = (
            "### Nit1\nA retained claim.\n\n```markdown\n### Blocker0\nExample only.\n"
            "## PR Summary\nExample summary.\n```\n\n"
            "## PR Summary\nActual change summary."
        )
        record = publish(github, [finding()], review=review)

        self.assertTrue(record["complete"], record["errors"])
        self.assertEqual(strip_review_body(github.issues[0]["body"]), "Actual change summary.")

    def test_publisher_cli_preserves_embedded_carriage_returns_in_both_diff_inputs(self):
        raw_diff = DIFF.replace("+new\n", "+first\rstill first\n").encode()
        wider_diff = DIFF.replace("@@ -1,2 +1,2 @@", "@@ -1,3 +1,3 @@") + " third\n"
        for raw_input in ("github-pr-patch", "pr-change-patch"):
            with self.subTest(raw_input=raw_input), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                paths = {
                    name: root / name
                    for name in (
                        "review", "findings", "github-pr-patch", "pr-change-patch", "trusted-bot-logins",
                        "output", "check-output", "full-review-output",
                    )
                }
                paths["review"].write_text("## PR Summary\nChanges a call.")
                paths["findings"].write_text(json.dumps([finding(line=3)]))
                paths["trusted-bot-logins"].write_text('["github-actions"]')
                for name in ("github-pr-patch", "pr-change-patch"):
                    paths[name].write_bytes(raw_diff if name == raw_input else wider_diff.encode())
                arguments = ["finding_publication.py"]
                for name, path in paths.items():
                    arguments.extend((f"--{name}", str(path)))
                arguments.extend((
                    "--head-sha", HEAD_SHA, "--run-url", RUN_URL,
                    "--repository", "example/project", "--pr-number", "1", "--mode", "inline",
                ))
                github = FakeGitHub()

                with patch.object(sys, "argv", arguments), patch.object(
                    finding_publication, "GitHub", return_value=github,
                ):
                    finding_publication.main()

                record = json.loads(paths["output"].read_text())
                self.assertTrue(record["complete"], record["errors"])
                self.assertEqual(github.inline, [])
                self.assertEqual(record["findings"][0]["placement"], "conversation")
                self.assertEqual(len(github.issues), 2)

    def test_invalid_location_and_prose_only_finding_keep_their_claims(self):
        github = FakeGitHub()
        review = "### ShouldFix1: derive once\nEvidence.\n\n## PR Summary\nChanges a call."
        record = publish(github, [finding("Nit1", line=-1)], review=review)
        self.assertTrue(record["complete"])
        self.assertEqual(len(github.inline), 0)
        self.assertEqual(len(github.issues), 3)
        self.assertIn("derive once", github.issues[1]["body"])
        self.assertEqual(record["findings"][0]["severity"], "Should Fix")

    def test_malformed_candidate_does_not_remove_valid_sibling(self):
        github = FakeGitHub()
        record = publish(github, [{"id": "bad"}, finding()])
        self.assertFalse(record["complete"])
        self.assertEqual(len(github.inline), 1)
        self.assertIn("Finding counts unavailable", github.issues[0]["body"])

    def test_invalid_prose_id_reports_incomplete_publication_and_keeps_valid_sibling(self):
        github = FakeGitHub()
        review = "### Blocker0\nA malformed finding.\n\n## Summary\nReview assessment."

        record = publish(github, [finding()], review=review)

        self.assertFalse(record["complete"])
        self.assertEqual(record["errors"], ["Invalid final finding heading: ### Blocker0."])
        self.assertEqual([item["id"] for item in record["findings"]], ["Nit1"])
        self.assertTrue(record["findings"][0]["url"])
        self.assertIn("Publication incomplete:", github.issues[0]["body"])
        self.assertIn("Invalid final finding heading: ### Blocker0.", github.issues[0]["body"])
        self.assertIn("Finding counts unavailable", github.issues[0]["body"])

    def test_invalid_locations_and_whitespace_ids_do_not_abort_valid_siblings(self):
        for side, finding_id in (([], "ShouldFix1"), ({}, "ShouldFix1"),
                                 ("RIGHT", "Should\tFix1")):
            with self.subTest(side=side, finding_id=finding_id):
                github = FakeGitHub()
                entry = dict(finding(finding_id, "A separate advisory concern."), side=side)
                record = publish(github, [entry, finding()])
                self.assertTrue(record["complete"], record["errors"])
                self.assertEqual([item["id"] for item in record["findings"]], ["ShouldFix1", "Nit1"])
                self.assertTrue(all(item["url"] for item in record["findings"]))

    def test_rejected_inline_and_structured_only_finding_use_visible_fallback(self):
        github = FakeGitHub()
        github.failures["Nit1"] = "reject"
        record = publish(github, [finding(body="JSON-only claim.\n\nSuggested fix: change it.")])
        self.assertTrue(record["complete"])
        self.assertEqual(len(github.inline), 0)
        self.assertEqual(len(github.issues), 2)
        self.assertNotIn("<details>", github.issues[1]["body"])
        check = publication_check("## PR Summary\nChanges a call.", record)
        self.assertIn("JSON-only claim.", check)
        self.assertNotIn("New finding comments:", check)

    def test_lost_response_stays_unconfirmed_without_readback_or_fallback(self):
        github = FakeGitHub()
        github.failures["Nit1"] = "ambiguous-posted"
        record = publish(github, [finding()])
        self.assertFalse(record["complete"])
        self.assertEqual(len(github.inline), 1)
        self.assertEqual(len(github.issues), 1)
        self.assertIsNone(record["findings"][0]["url"])
        self.assertIn("could not be confirmed", record["findings"][0]["error"])
        self.assertEqual(len(github.reads), 3)

    def test_unobserved_write_stays_incomplete_and_retry_reuses_prior_success(self):
        github = FakeGitHub()
        github.failures["Nit2"] = "ambiguous-missing"
        entries = [finding(), finding("Nit2", "Second claim.")]
        first = publish(github, entries)
        self.assertFalse(first["complete"])
        self.assertEqual(len(github.inline), 1)
        github.failures.clear()
        second = publish(github, entries)
        self.assertTrue(second["complete"])
        self.assertEqual(len(github.inline), 2)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual([item["delivery"] for item in second["findings"]], ["reused", "new"])

    def test_reused_findings_keep_final_counts_and_advisory_summary_links(self):
        for severity in ("Blocker", "ShouldFix", "Nit"):
            with self.subTest(severity=severity):
                github = FakeGitHub()
                entries = [finding(f"{severity}1")]
                publish(github, entries)
                second = publish(github, entries, run_url=RUN_URL + "2")
                self.assertTrue(second["complete"])
                self.assertEqual(len(github.inline), 1)
                self.assertEqual(second["findings"][0]["delivery"], "reused")
                check = publication_check("review", second)
                self.assertNotIn("## Finding publication", check)
                main = github.issues[-1]["body"]
                self.assertEqual("existing thread" in main, severity != "Nit")

    def test_labels_do_not_identify_different_findings_on_later_runs(self):
        github = FakeGitHub()
        publish(github, [finding()])
        record = publish(github, [finding(body="An unrelated claim.")], run_url=RUN_URL + "2")
        self.assertTrue(record["complete"])
        self.assertEqual(len(github.inline), 2)

    def test_complete_paged_history_prevents_a_duplicate_after_first_page(self):
        github = FakeGitHub()
        publish(github, [finding()])
        github.inline[:0] = [dict(github.inline[0], id=100 + i, body="unrelated") for i in range(100)]
        second = publish(github, [finding()], run_url=RUN_URL + "2")
        self.assertTrue(second["complete"])
        self.assertEqual(second["findings"][0]["delivery"], "reused")
        self.assertTrue(any("page=2" in path for path in github.reads))

    def test_unknown_history_does_not_risk_duplicate_writes(self):
        github = FakeGitHub()
        github.read_failure = True
        record = publish(github, [finding()])
        self.assertFalse(record["complete"])
        self.assertEqual(github.writes, [])

    def test_summary_and_artifact_modes_preserve_findings_without_claiming_comment_delivery(self):
        for mode in ("summary", "artifact"):
            with self.subTest(mode=mode):
                github = FakeGitHub()
                record = publish(github, [finding()], mode=mode)
                self.assertTrue(record["complete"])
                self.assertEqual(github.writes, [])
                self.assertEqual(record["findings"][0]["placement"], mode)
                self.assertIsNone(record["findings"][0]["url"])
                self.assertIsNone(record["findings"][0]["delivery"])
                check = publication_check("## PR Summary\nChanges a call.", record)
                self.assertIn("A retained claim.", check)
                self.assertNotIn("## Finding publication", check)
                self.assertNotIn("Report output:", check)

    def test_conversation_modes_and_setting_preserve_each_finding(self):
        for mode, placement in (("comment", "inline-or-conversation"),
                                ("collapsed", "inline-or-conversation"),
                                ("inline", "conversation")):
            with self.subTest(mode=mode, placement=placement):
                github = FakeGitHub()
                record = publish(github, [finding()], mode=mode, placement=placement)
                self.assertTrue(record["complete"])
                self.assertEqual(len(github.inline), 0)
                self.assertEqual(len(github.issues), 2)

    def test_prose_and_json_use_only_final_severity(self):
        findings, errors = collect_findings(
            "### Nit1\nDowngraded candidate.\n## PR Summary\nContext.",
            [finding("Nit1", "Downgraded candidate."), finding("ShouldFix1", "Still advisory.")],
        )
        self.assertEqual(errors, [])
        self.assertEqual([item["severity"] for item in findings], ["Nit", "Should Fix"])
        _, errors = collect_findings("review", [dict(finding(), severity="Blocker")])
        self.assertTrue(errors)

    def test_visible_explanation_and_fix_preserve_attribution_and_finding_identity(self):
        bodies = (
            "Claim.\n\nEvidence.\n\nRaised by: protocol-reviewer\n\nSuggested fix: use x.",
            "Claim.\n\nExplanation: Evidence.\n\nSuggested fix: use x.\n\nRaised by: protocol-reviewer",
        )
        for body in bodies:
            with self.subTest(body=body):
                comment = format_finding(finding(body=body), head_sha=HEAD_SHA, main_url=RUN_URL)
                self.assertNotIn("<details>", comment)
                self.assertIn("**Explanation:**", comment)
                self.assertIn("Evidence.", comment)
                self.assertIn("**Suggested fix:** use x.", comment)
                self.assertIn("<sub>Raised by: protocol-reviewer · Reviewed", comment)
                self.assertIn("</sub>\n<sub>Feedback:", comment)
                self.assertEqual(canonical_finding_body(comment), canonical_finding_body(body))
                previous_footer = comment.replace("</sub>\n<sub>Feedback:", "</sub><br>\n<sub>Feedback:")
                self.assertEqual(canonical_finding_body(previous_footer), canonical_finding_body(body))

    def test_finding_fields_inside_code_examples_are_not_reformatted_or_moved(self):
        example = "```text\nRaised by: example\nSuggested fix: example\n```"
        body = f"Claim.\n\n{example}\n\nSuggested fix: use x.\n\nRaised by: docs-reviewer"
        comment = format_finding(finding(body=body), head_sha=HEAD_SHA, main_url=RUN_URL)
        self.assertIn(example, comment)
        self.assertIn("<sub>Raised by: docs-reviewer · Reviewed", comment)
        self.assertEqual(canonical_finding_body(comment), canonical_finding_body(body))

    def test_summary_headings_and_structured_only_check_text(self):
        for heading in ("## Overall assessment", "## Assessment Summary", "## PR Summary",
                        "## Summary", "### Summary", "3. **Summary**", "3. **Overall assessment**"):
            with self.subTest(heading=heading):
                self.assertEqual(review_overview(f"### Nit1\nA claim.\n{heading}\nChange context."), "Change context.")
        entries, _ = collect_findings("## Summary\nContext.", [finding()])
        self.assertIn("### Nit 1", complete_review("## Summary\nContext.", entries))
        body = format_review_summary(
            overview="Context.", head_sha=HEAD_SHA, findings=[], run_url=RUN_URL,
        )
        self.assertEqual(strip_review_body(body), "Context.")
        self.assertNotIn("<summary>Key findings", body)

    def test_readable_headings_keep_compact_metadata_ids(self):
        for finding_id, heading in (("Blocker1", "Blocker 1"),
                                    ("ShouldFix1", "Should Fix 1"), ("Nit1", "Nit 1")):
            with self.subTest(finding_id=finding_id):
                entries, errors = collect_findings(
                    f"### {heading}\nA retained claim.\n\n## Summary\nAssessment.",
                    [finding(finding_id)],
                )

                self.assertEqual(errors, [])
                self.assertEqual([item["id"] for item in entries], [finding_id])
                report = complete_review("## Summary\nAssessment.", entries)
                self.assertIn("## Overall assessment\n\nAssessment.", report)
                self.assertIn(f"### {heading}\n\nA retained claim.", report)

    def test_legacy_unknown_severity_split_does_not_fabricate_zeroes(self):
        body = format_review_summary(
            overview="Legacy review.", head_sha=HEAD_SHA, findings=[], run_url=RUN_URL,
            counts_available=False,
        )
        self.assertIn("Finding counts unavailable", body)
        self.assertNotIn("0 Blockers", body)


if __name__ == "__main__":
    unittest.main()
