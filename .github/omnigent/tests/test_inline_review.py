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

        config = (reviewer_dir / "config.yaml").read_text()
        _, separator, prompt_block = config.partition("prompt: |\n")
        self.assertTrue(separator)
        configured_lines = []
        for line in prompt_block.splitlines():
            if line and not line.startswith("  "):
                break
            configured_lines.append(line[2:])

        self.assertEqual(standalone_prompt.strip(), "\n".join(configured_lines).strip())

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
        payload, unmapped = self.inline_review.build_review_payload(
            review="### Nit1: use the new call",
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
        self.assertEqual(unmapped, ["Nit2"])

    def test_build_payload_accepts_multiline_finding_body(self) -> None:
        payload, unmapped = self.inline_review.build_review_payload(
            review="review",
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

        payload, unmapped = self.inline_review.build_review_payload(
            review="review",
            findings=[finding, finding],
            diff=DIFF,
            head_sha="c" * 40,
            run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
        )

        self.assertEqual(len(payload["comments"]), 1)
        self.assertEqual(unmapped, ["Blocker1"])

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
                payload, unmapped = self.inline_review.build_review_payload(
                    review="review",
                    findings=[valid | update, valid],
                    diff=DIFF,
                    head_sha="d" * 40,
                    run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
                )
                self.assertEqual(len(payload["comments"]), 1)
                self.assertEqual(unmapped, ["entry 1"])

        invalid_findings = (None, {}, valid | {"extra": "field"})
        for finding in invalid_findings:
            with self.subTest(finding=finding):
                payload, unmapped = self.inline_review.build_review_payload(
                    review="review",
                    findings=[finding, valid],
                    diff=DIFF,
                    head_sha="d" * 40,
                    run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
                )
                self.assertEqual(len(payload["comments"]), 1)
                self.assertEqual(unmapped, ["entry 1"])

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
                "review": "review",
                "findings": [finding],
                "diff": DIFF,
                "head_sha": "d" * 40,
                "run_url": "https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            }
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.inline_review.build_review_payload(**(arguments | update))

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
