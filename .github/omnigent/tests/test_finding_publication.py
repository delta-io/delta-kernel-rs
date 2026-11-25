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
    from review_publish import (
        BOT_MARKER, bound_check_summary, format_finding, format_review_summary, strip_review_body,
    )
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
        order = ["## AI Review", "---", "0 Blockers · 1 Should Fix · 4 Nits", "Reviewed",
                 "<summary>PR Summary", "<summary>Review Summary", "Full AI review"]
        offsets = [main.index(part) for part in order]
        self.assertEqual(offsets, sorted(offsets))
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
        self.assertIn("0 Blockers · 0 Should Fix · 1 Nit", github.issues[0]["body"])
        check = publication_check("## PR Summary\nChanges a call.", record)
        self.assertIn("New finding comments: 1 · Reused finding comments: 0", check)
        self.assertEqual(check.count("A retained claim."), 1)

    def test_published_main_comment_unwraps_its_workflow_metadata(self):
        github = FakeGitHub()
        publish(github, [finding()])

        body = github.issues[0]["body"]
        self.assertIn("<!-- ai-review-main ", body)
        self.assertEqual(strip_review_body(body), "Changes a call.")

    def test_checks_use_the_same_final_structured_claim_as_the_thread(self):
        github = FakeGitHub()
        review = "### Nit1\nSuperseded prose claim.\n\n## PR Summary\nChanges a call."
        record = publish(github, [finding(body="Final retained claim.")], review=review)

        self.assertTrue(record["complete"], record["errors"])
        self.assertIn("Final retained claim.", github.inline[0]["body"])
        check = publication_check(review, record)
        self.assertIn("Final retained claim.", check)
        self.assertNotIn("Superseded prose claim.", check)

    def test_fenced_summary_example_does_not_replace_the_pr_summary(self):
        github = FakeGitHub()
        review = (
            "### Nit1\nA retained claim.\n\n```markdown\n"
            "## PR Summary\nExample summary.\n```\n\n"
            "## PR Summary\nActual change summary."
        )
        publish(github, [finding()], review=review)

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
        self.assertIn("New finding comments: 1", check)

    def test_lost_accepted_response_is_reconciled_without_fallback_duplicate(self):
        github = FakeGitHub()
        github.failures["Nit1"] = "ambiguous-posted"
        record = publish(github, [finding()])
        self.assertTrue(record["complete"], record["errors"])
        self.assertEqual(len(github.inline), 1)
        self.assertEqual(len(github.issues), 1)

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
                self.assertIn("New finding comments: 0 · Reused finding comments: 1", check)
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
                self.assertIn("New finding comments: 0 · Reused finding comments: 0", check)
                self.assertIn("No PR comments requested.", check)
                for report in (record, {key: value for key, value in record.items() if key != "mode"}):
                    check = publication_check("Review context.", report)
                    self.assertIn(
                        "New finding comments: 0 · Reused finding comments: 0", check
                    )
                    self.assertIn(f"Findings in {mode} report: 1", check)
                    self.assertNotIn("New finding comments: 1", check)

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

    def test_inline_detail_folding_preserves_attribution_and_conversation_visibility(self):
        item = finding(body="Claim.\n\nEvidence.\n\nRaised by: protocol-reviewer\n\nSuggested fix: use x.")
        inline = format_finding(item, head_sha=HEAD_SHA, main_url=RUN_URL, collapsed=True)
        conversation = format_finding(item, head_sha=HEAD_SHA, main_url=RUN_URL)
        self.assertIn("<summary>Details</summary>", inline)
        self.assertNotIn("<details>", conversation)
        self.assertIn("Raised by: protocol-reviewer · Reviewed", inline)
        self.assertEqual(canonical_finding_body(inline), canonical_finding_body(conversation))

    def test_pr_summary_legacy_summary_and_structured_only_check_text(self):
        for heading in ("## PR Summary", "## Summary", "### Summary", "3. **Summary**"):
            with self.subTest(heading=heading):
                self.assertEqual(review_overview(f"### Nit1\nA claim.\n{heading}\nChange context."), "Change context.")
        entries, _ = collect_findings("## Summary\nContext.", [finding()])
        self.assertIn("### Nit1", complete_review("## Summary\nContext.", entries))
        body = format_review_summary(
            overview="Context.", head_sha=HEAD_SHA, findings=[], run_url=RUN_URL,
        )
        self.assertEqual(strip_review_body(body), "Context.")
        self.assertNotIn("<summary>Review Summary", body)

    def test_cut_diff_shows_file_totals_and_keeps_missing_paths_in_checks(self):
        coverage = {
            "base_sha": "b" * 40, "head_sha": HEAD_SHA,
            "total_bytes": 120_000, "excerpt_bytes": 100_000, "complete": False,
            "total_files": 3, "full_files": 1, "partial_files": 1, "omitted_files": 1,
            "files": [
                {"path": "full.rs", "status": "full"},
                {"path": "partial.rs", "status": "partial"},
                {"path": "missing.rs", "status": "omitted"},
            ],
        }
        github = FakeGitHub()
        record = publish(github, [finding()], coverage=coverage)
        self.assertTrue(record["complete"])
        main = github.issues[0]["body"]
        self.assertIn("1 of 3 file sections fully included; 1 partial and 1 omitted", main)
        self.assertNotIn("Status requirements met", main)
        self.assertNotIn("Status requirements unmet", main)
        self.assertNotIn("missing.rs", main)
        check = publication_check("Context.", record)
        self.assertIn("100000 of 120000", check)
        self.assertIn("partial.rs", check)
        self.assertIn("missing.rs", check)
        self.assertIn(HEAD_SHA, check)
        coverage.update(complete=True)
        github = FakeGitHub()
        publish(github, [], coverage=coverage)
        self.assertNotIn("Review input truncated", github.issues[0]["body"])

    def test_large_coverage_report_links_complete_artifact_within_check_api_budget(self):
        coverage = {
            "base_sha": "b" * 40, "head_sha": HEAD_SHA,
            "total_bytes": 500_000, "excerpt_bytes": 100_000, "complete": False,
            "total_files": 1300, "full_files": 0, "partial_files": 0, "omitted_files": 1300,
            "files": [{"path": f"directory/{i}/" + "x" * 170, "status": "omitted"}
                      for i in range(1300)],
        }
        record = {"findings": [], "errors": [], "coverage": coverage}
        full = publication_check("PR context.", record)
        self.assertGreater(len(full.encode()), 65_535)
        url = RUN_URL + "/artifacts/123"
        summary = bound_check_summary(full, url)
        self.assertLess(len(summary.encode()), 60_000)
        self.assertIn(url, summary)
        self.assertIn("seven days", summary)
        self.assertIn("directory/1299/", full)
        self.assertEqual(bound_check_summary("A complete short report.", url),
                         "A complete short report.")

    def test_legacy_unknown_severity_split_does_not_fabricate_zeroes(self):
        body = format_review_summary(
            overview="Legacy review.", head_sha=HEAD_SHA, findings=[], run_url=RUN_URL,
            counts_available=False,
        )
        self.assertIn("Finding counts unavailable", body)
        self.assertNotIn("0 Blockers", body)


if __name__ == "__main__":
    unittest.main()
