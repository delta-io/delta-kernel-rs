"""Tests for bounded previous-review selection and deduplication."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


def _load_module():
    path = Path(__file__).parents[1] / "review_history.py"
    spec = importlib.util.spec_from_file_location("review_history", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _document(*, author_type: str = "Bot") -> dict:
    marker = "<!-- ai-review-bot -->"
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "comments": {
                        "nodes": [
                            {
                                "author": {"__typename": author_type, "login": "reviewer"},
                                "body": f"{marker}\n## AI Review\n\nOld collapsed finding",
                            }
                        ]
                    },
                    "reviews": {
                        "nodes": [
                            {
                                "author": {"__typename": author_type, "login": "reviewer"},
                                "body": f"{marker}\n## AI Review\n\nOld review summary",
                                "comments": {
                                    "nodes": [
                                        {
                                            "path": "kernel/src/example.rs",
                                            "body": "**Blocker1** Repeated finding.",
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


class ReviewHistoryTest(unittest.TestCase):
    def test_history_includes_marked_bot_reviews_and_inline_comments(self) -> None:
        history = _load_module().format_review_history(_document())

        self.assertIn("untrusted historical data", history)
        self.assertIn("Old collapsed finding", history)
        self.assertIn("Old review summary", history)
        self.assertIn("Repeated finding", history)
        self.assertIn("kernel/src/example.rs", history)

    def test_history_ignores_spoofed_user_marker(self) -> None:
        module = _load_module()
        document = _document(author_type="User")

        self.assertEqual(
            module.format_review_history(document),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments(document), [])

    def test_inline_comments_are_available_for_exact_deduplication(self) -> None:
        comments = _load_module().previous_inline_comments(_document())

        self.assertEqual(
            comments,
            [
                {
                    "path": "kernel/src/example.rs",
                    "body": "**Blocker1** Repeated finding.",
                }
            ],
        )

    def test_finding_normalization_ignores_run_specific_id_and_whitespace(self) -> None:
        module = _load_module()

        self.assertEqual(
            module.canonical_finding_body("**Nit2**  Repeated\n finding."),
            module.canonical_finding_body("**Blocker1** Repeated finding."),
        )

    def test_complete_review_deduplication_ignores_publication_wrapper(self) -> None:
        module = _load_module()

        self.assertTrue(module.is_duplicate_review("Old collapsed finding", _document()))
        self.assertFalse(module.is_duplicate_review("New finding", _document()))


if __name__ == "__main__":
    unittest.main()
