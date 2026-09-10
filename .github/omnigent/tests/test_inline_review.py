"""Tests for SHA-bound inline GitHub review publication."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


def _load_module(name):
    module_dir = Path(__file__).parents[1]
    path = module_dir / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(module_dir))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def _configured_prompt(path: Path) -> str:
    config = path.read_text()
    _, separator, prompt_block = config.partition("prompt: |\n")
    assert separator
    configured_lines = []
    for line in prompt_block.splitlines():
        if line and not line.startswith("  "):
            break
        configured_lines.append(line[2:])
    return "\n".join(configured_lines).strip()


DIFF = """\
diff --git a/kernel/src/example.rs b/kernel/src/example.rs
index 1111111..2222222 100644
--- a/kernel/src/example.rs
+++ b/kernel/src/example.rs
@@ -8,5 +8,6 @@ fn example() {
 unchanged();
-old_call();
+new_call();
+added_call();
--- old heading;
+++ new heading;
 trailing();
"""

FILE_LIFECYCLE_DIFF = """\
diff --git a/kernel/src/new.rs b/kernel/src/new.rs
new file mode 100644
--- /dev/null
+++ b/kernel/src/new.rs
@@ -0,0 +1 @@
+new line
\\ No newline at end of file
diff --git a/kernel/src/old.rs b/kernel/src/old.rs
deleted file mode 100644
--- a/kernel/src/old.rs
+++ /dev/null
@@ -1 +0,0 @@
-old line
\\ No newline at end of file
"""


class InlineReviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.inline_review = _load_module("inline_review")
        self.review_publish = _load_module("review_publish")

    def test_extract_inline_findings_removes_machine_block(self) -> None:
        marker = "a" * 32
        review, findings = self.inline_review.extract_inline_findings(
            "Summary\n"
            f"<!-- AI_REVIEW_INLINE_START_{marker} -->\n"
            '{"findings":[{"id":"Nit1"}]}\n'
            f"<!-- AI_REVIEW_INLINE_END_{marker} -->",
            marker,
        )

        self.assertEqual(review, "Summary")
        self.assertEqual(findings, [{"id": "Nit1"}])

    def test_inline_prompt_uses_parser_contract(self) -> None:
        marker = "a" * 32
        prompt = self.inline_review.inline_prompt_instructions(marker)

        self.assertIn(f"AI_REVIEW_INLINE_START_{marker}", prompt)
        self.assertIn(f"up to {self.inline_review.MAX_INLINE_FINDINGS}", prompt)
        for field in self.inline_review.INLINE_FINDING_FIELDS:
            self.assertIn(f'"{field}"', prompt)
        self.assertIn(self.inline_review._FINDING_ID.pattern, prompt)
        for side in self.inline_review.INLINE_FINDING_SIDES:
            self.assertIn(side, prompt)

    def test_reviewer_contract_matches_configured_prompt(self) -> None:
        reviewer_dir = Path(__file__).parents[1] / "reviewer"
        contract = (reviewer_dir / "REVIEW.md").read_text()
        _, separator, standalone_prompt = contract.partition("\n---\n\n")
        self.assertTrue(separator)

        self.assertEqual(
            standalone_prompt.strip(),
            _configured_prompt(reviewer_dir / "config.yaml"),
        )

    def test_shared_policies_reach_parent_and_child_reviewers(self) -> None:
        omnigent_dir = Path(__file__).parents[1]
        reviewer_dir = omnigent_dir / "reviewer"
        reviewer_contract = (reviewer_dir / "REVIEW.md").read_text()
        workflow = (omnigent_dir.parent / "workflows" / "ai-review.yml").read_text()
        review_policy = _load_module("review_policy")
        for policy in (
            review_policy.KNOWN_ISSUE_POLICY.strip(),
            review_policy.PREVIOUS_REVIEW_POLICY.strip(),
        ):
            self.assertIn(policy, reviewer_contract)
            self.assertIn(policy, _configured_prompt(reviewer_dir / "config.yaml"))
            for agent_dir in (reviewer_dir / "agents").iterdir():
                if not agent_dir.is_dir():
                    continue
                with self.subTest(agent=agent_dir.name, policy=policy.partition("\n")[0]):
                    self.assertIn(policy, (agent_dir / "REVIEW.md").read_text())
                    self.assertIn(policy, _configured_prompt(agent_dir / "config.yaml"))
        self.assertIn(
            "from review_policy import KNOWN_ISSUE_POLICY, PREVIOUS_REVIEW_POLICY",
            workflow,
        )
        self.assertIn("known_issue_policy = KNOWN_ISSUE_POLICY.strip()", workflow)
        self.assertIn(
            "previous_review_policy = PREVIOUS_REVIEW_POLICY.strip()", workflow
        )
        self.assertEqual(workflow.count("{known_issue_policy}"), 1)
        self.assertEqual(workflow.count("{previous_review_policy}"), 1)
        self.assertIn("format_review_history", workflow)

    def test_automatic_reviews_default_to_inline(self) -> None:
        workflow = (Path(__file__).parents[2] / "workflows" / "ai-review.yml").read_text()
        automatic_trigger = workflow.partition("            pull_request_target)")[2].partition(
            "            workflow_dispatch)"
        )[0]

        self.assertIn("mode=inline", automatic_trigger)

    def test_diff_positions_tracks_both_sides_and_context(self) -> None:
        self.assertEqual(
            self.inline_review.diff_positions(DIFF),
            {
                ("kernel/src/example.rs", 8, "RIGHT"),
                ("kernel/src/example.rs", 9, "LEFT"),
                ("kernel/src/example.rs", 9, "RIGHT"),
                ("kernel/src/example.rs", 10, "LEFT"),
                ("kernel/src/example.rs", 10, "RIGHT"),
                ("kernel/src/example.rs", 11, "RIGHT"),
                ("kernel/src/example.rs", 12, "RIGHT"),
            },
        )

    def test_diff_positions_handles_added_deleted_and_no_newline_files(self) -> None:
        self.assertEqual(
            self.inline_review.diff_positions(FILE_LIFECYCLE_DIFF),
            {
                ("kernel/src/new.rs", 1, "RIGHT"),
                ("kernel/src/old.rs", 1, "LEFT"),
            },
        )

    def test_build_payload_keeps_only_locations_in_diff(self) -> None:
        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review=(
                "## Non-blocking notes\n"
                "### Nit1: use the new call\nAttached detail.\n"
                "### Nit2: check the other call\nUnmapped detail.\n"
                "## Summary\nNeeds a small follow-up."
            ),
            findings=[
                {
                    "id": "Nit1",
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "This is attached to the added line.",
                },
                {
                    "id": "Nit2",
                    "path": "kernel/src/example.rs",
                    "line": 100,
                    "side": "RIGHT",
                    "body": "This remains in the full review only.",
                },
            ],
            diff=DIFF,
            head_sha="b" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            history={},
        )

        self.assertEqual(payload["event"], "COMMENT")
        self.assertEqual(payload["commit_id"], "b" * 40)
        self.assertEqual(
            payload["comments"],
            [
                {
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "**Nit1** This is attached to the added line.",
                }
            ],
        )
        self.assertIn("<details><summary>Show review</summary>", payload["body"])
        self.assertTrue(payload["body"].startswith("<!-- ai-review-bot -->"))
        self.assertNotIn("### Nit1", payload["body"])
        self.assertIn("### Nit2", payload["body"])
        self.assertIn("## Summary", payload["body"])
        self.assertEqual(unmapped, ["Nit2"])
        self.assertEqual(duplicates, [])

    def test_build_payload_accepts_multiline_finding_body(self) -> None:
        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review="### Nit1\nreview\n\n## Summary\nsummary",
            findings=[
                {
                    "id": "Nit1",
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "First line.\n\tIndented detail.",
                }
            ],
            diff=DIFF,
            head_sha="b" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
        )

        self.assertEqual(len(payload["comments"]), 1)
        self.assertEqual(unmapped, [])
        self.assertEqual(duplicates, [])

    def test_format_review_body_supports_expanded_comments(self) -> None:
        body = self.review_publish.format_review_body(
            "Review",
            "https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            collapsed=False,
        )

        self.assertNotIn("<details>", body)
        self.assertIn("\nReview\n", body)

    def test_build_payload_skips_duplicate_ids(self) -> None:
        finding = {
            "id": "Blocker1",
            "path": "kernel/src/example.rs",
            "line": 9,
            "side": "RIGHT",
            "body": "Duplicate.",
        }

        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review="### Blocker1\nreview\n\n## Summary\nsummary",
            findings=[finding, finding],
            diff=DIFF,
            head_sha="c" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
        )

        self.assertEqual(len(payload["comments"]), 1)
        self.assertEqual(unmapped, ["Blocker1"])
        self.assertEqual(duplicates, [])

    def test_build_payload_skips_untrusted_finding_fields(self) -> None:
        valid = {
            "id": "Nit1",
            "path": "kernel/src/example.rs",
            "line": 10,
            "side": "RIGHT",
            "body": "Finding.",
        }
        invalid_updates = (
            {"id": "other"},
            {"id": 1},
            {"path": ""},
            {"path": "a" * 501},
            {"path": "/etc/passwd"},
            {"path": "bad\x00path"},
            {"path": "../example.rs"},
            {"line": True},
            {"line": "10"},
            {"line": 0},
            {"side": "BOTH"},
            {"body": ""},
            {"body": 1},
            {"body": "a" * 10_001},
            {"body": "bad\x00body"},
        )

        for update in invalid_updates:
            with self.subTest(update=update):
                payload, unmapped, duplicates = self.inline_review.build_review_payload(
                    review="### Nit1\nreview\n\n## Summary\nsummary",
                    findings=[valid | update, valid],
                    diff=DIFF,
                    head_sha="d" * 40,
                    run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
                )
                self.assertEqual(len(payload["comments"]), 1)
                self.assertEqual(unmapped, ["entry 1"])
                self.assertEqual(duplicates, [])

        invalid_findings = (None, {}, valid | {"extra": "field"})
        for finding in invalid_findings:
            with self.subTest(finding=finding):
                payload, unmapped, duplicates = self.inline_review.build_review_payload(
                    review="### Nit1\nreview\n\n## Summary\nsummary",
                    findings=[finding, valid],
                    diff=DIFF,
                    head_sha="d" * 40,
                    run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
                )
                self.assertEqual(len(payload["comments"]), 1)
                self.assertEqual(unmapped, ["entry 1"])
                self.assertEqual(duplicates, [])

    def test_build_payload_rejects_untrusted_metadata(self) -> None:
        finding = {
            "id": "Nit1",
            "path": "kernel/src/example.rs",
            "line": 10,
            "side": "RIGHT",
            "body": "Finding.",
        }
        invalid_metadata = (
            {"head_sha": "ABC"},
            {"run_url": "http://github.com/actions/runs/1"},
            {"run_url": "https://example.com/actions/runs/1"},
        )

        for update in invalid_metadata:
            arguments = {
                "review": "### Nit1\nreview\n\n## Summary\nsummary",
                "findings": [finding],
                "diff": DIFF,
                "head_sha": "d" * 40,
                "run_url": "https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            }
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.inline_review.build_review_payload(**(arguments | update))

    def test_build_payload_suppresses_exact_prior_inline_finding(self) -> None:
        history = {
            "data": {
                "repository": {
                    "pullRequest": {
                        "comments": {"nodes": []},
                        "reviews": {
                            "nodes": [
                                {
                                    "author": {"__typename": "Bot"},
                                    "body": "<!-- ai-review-bot -->\nprior review",
                                    "comments": {
                                        "nodes": [
                                            {
                                                "path": "kernel/src/example.rs",
                                                "line": 10,
                                                "originalLine": 10,
                                                "side": "RIGHT",
                                                "body": "**Blocker9** Repeated finding.",
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            }
        }

        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review=(
                "## Blocking issues\n"
                "### Blocker1: repeated\nRepeated finding.\n"
                "## Summary\nNo new findings."
            ),
            findings=[
                {
                    "id": "Blocker1",
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "Repeated finding.",
                }
            ],
            diff=DIFF,
            head_sha="f" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            history=history,
        )

        self.assertEqual(payload["comments"], [])
        self.assertNotIn("Blocker1", payload["body"])
        self.assertNotIn("Blocking issues", payload["body"])
        self.assertIn("## Summary", payload["body"])
        self.assertEqual(unmapped, [])
        self.assertEqual(duplicates, ["Blocker1"])

        findings = [
            {
                "id": "Blocker1",
                "path": "kernel/src/example.rs",
                "line": 11,
                "side": "RIGHT",
                "body": "Repeated finding.",
            }
        ]
        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review="### Blocker1\nRepeated finding.\n\n## Summary\nNew location.",
            findings=findings,
            diff=DIFF,
            head_sha="f" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            history=history,
        )

        self.assertEqual(len(payload["comments"]), 1)
        self.assertEqual(unmapped, [])
        self.assertEqual(duplicates, [])

        findings[0]["line"] = 10
        findings[0]["side"] = "LEFT"
        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review="### Blocker1\nRepeated finding.\n\n## Summary\nDifferent side.",
            findings=findings,
            diff=DIFF,
            head_sha="f" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            history=history,
        )

        self.assertEqual(len(payload["comments"]), 1)
        self.assertEqual(payload["comments"][0]["side"], "LEFT")
        self.assertEqual(unmapped, [])
        self.assertEqual(duplicates, [])

    def test_removed_finding_ignores_heading_like_code_fence_lines(self) -> None:
        review = (
            "## Blocking issues\n"
            "### Blocker1\n"
            "The command demonstrates the failure:\n\n"
            "```bash\n"
            "# fetch history\n"
            "gh api graphql\n"
            "```\n\n"
            "Trailing finding detail.\n"
            "### Blocker2\n"
            "Finding that remains.\n"
            "## Summary\n"
            "Needs changes."
        )

        remaining = self.inline_review._remove_finding_sections(review, {"Blocker1"})

        self.assertNotIn("fetch history", remaining)
        self.assertNotIn("Trailing finding detail", remaining)
        self.assertIn("### Blocker2", remaining)
        self.assertIn("## Summary", remaining)

    def test_removed_finding_ignores_internal_markdown_heading(self) -> None:
        review = (
            "## Blocking issues\n"
            "### Blocker1\n"
            "Finding detail.\n"
            "### Reproduction\n"
            "More finding detail.\n"
            "### Blocker2\n"
            "Finding that remains.\n"
            "## Summary\n"
            "Needs changes."
        )

        remaining = self.inline_review._remove_finding_sections(review, {"Blocker1"})

        self.assertNotIn("Reproduction", remaining)
        self.assertNotIn("More finding detail", remaining)
        self.assertIn("### Blocker2", remaining)
        self.assertIn("## Summary", remaining)

    def test_removed_finding_recovers_from_unclosed_code_fence(self) -> None:
        review = (
            "## Blocking issues\n"
            "### Blocker1\n"
            "```text\n"
            "Unclosed example.\n"
            "### Blocker2\n"
            "Finding that remains.\n"
            "Summary:\n"
            "Needs changes."
        )

        remaining = self.inline_review._remove_finding_sections(review, {"Blocker1"})

        self.assertNotIn("Unclosed example", remaining)
        self.assertIn("### Blocker2", remaining)
        self.assertIn("Summary:\nNeeds changes.", remaining)

    def test_removed_finding_preserves_plain_summary_and_drops_empty_group(self) -> None:
        review = (
            "No blocking issues.\n\n"
            "Review overview.\n\n"
            "Non-blocking notes:\n\n"
            "### Nit1\n"
            "Finding published inline.\n\n"
            "Summary:\n"
            "Overall assessment."
        )

        remaining = self.inline_review._remove_finding_sections(review, {"Nit1"})

        self.assertIn("Review overview.", remaining)
        self.assertNotIn("Non-blocking notes", remaining)
        self.assertNotIn("Finding published inline", remaining)
        self.assertIn("Summary:\nOverall assessment.", remaining)

    def test_removed_finding_drops_empty_numbered_group(self) -> None:
        review = (
            "1. **Non-blocking notes**\n\n"
            "### Nit1\n"
            "Finding published inline.\n\n"
            "2. **Summary**\n"
            "Overall assessment."
        )

        remaining = self.inline_review._remove_finding_sections(review, {"Nit1"})

        self.assertNotIn("Non-blocking notes", remaining)
        self.assertNotIn("Finding published inline", remaining)
        self.assertIn("2. **Summary**\nOverall assessment.", remaining)

    def test_build_payload_keeps_finding_without_matching_heading_in_summary(self) -> None:
        payload, unmapped, duplicates = self.inline_review.build_review_payload(
            review="## Summary\nNit1 needs attention.",
            findings=[
                {
                    "id": "Nit1",
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "Finding.",
                }
            ],
            diff=DIFF,
            head_sha="f" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
        )

        self.assertEqual(payload["comments"], [])
        self.assertIn("Nit1 needs attention", payload["body"])
        self.assertEqual(unmapped, ["Nit1"])
        self.assertEqual(duplicates, [])

    def test_exact_duplicate_inline_body_is_skipped_without_new_comments(self) -> None:
        review = "## Summary\nNo new findings."
        prior_body = self.review_publish.format_review_body(
            review,
            "https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            collapsed=True,
        )
        history = {
            "data": {
                "repository": {
                    "pullRequest": {
                        "comments": {"nodes": []},
                        "reviews": {
                            "nodes": [
                                {
                                    "author": {"__typename": "Bot"},
                                    "body": prior_body,
                                    "comments": {"nodes": []},
                                }
                            ]
                        },
                    }
                }
            }
        }
        payload, _, _ = self.inline_review.build_review_payload(
            review=review,
            findings=[],
            diff=DIFF,
            head_sha="f" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/2",
            history=history,
        )

        self.assertTrue(self.inline_review.should_skip_inline_review(payload, history))

        payload["comments"].append({"body": "new finding"})
        self.assertFalse(self.inline_review.should_skip_inline_review(payload, history))

    def test_extract_inline_findings_rejects_invalid_envelopes(self) -> None:
        marker = "e" * 32
        start = f"<!-- AI_REVIEW_INLINE_START_{marker} -->"
        end = f"<!-- AI_REVIEW_INLINE_END_{marker} -->"
        invalid_reviews = (
            "Review",
            f"Review\n{start}\nnot-json\n{end}",
            f'Review\n{start}\n{{"findings":[],"extra":true}}\n{end}',
            f'Review\n{start}\n{{"findings":{{}}}}\n{end}',
            f"{start}\n{{\"findings\":[]}}\n{end}",
        )

        for review in invalid_reviews:
            with self.subTest(review=review), self.assertRaises(ValueError):
                self.inline_review.extract_inline_findings(review, marker)

    def test_extract_inline_findings_enforces_cap(self) -> None:
        marker = "e" * 32
        document = {"findings": [{"id": f"N{index}"} for index in range(1, 14)]}
        review = (
            "Review\n"
            f"<!-- AI_REVIEW_INLINE_START_{marker} -->\n"
            f"{json.dumps(document)}\n"
            f"<!-- AI_REVIEW_INLINE_END_{marker} -->"
        )

        with self.assertRaisesRegex(ValueError, "more than"):
            self.inline_review.extract_inline_findings(review, marker)


if __name__ == "__main__":
    unittest.main()
