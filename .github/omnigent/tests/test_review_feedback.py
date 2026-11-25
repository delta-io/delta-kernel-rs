import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from review_feedback import evaluate_feedback
from review_publish import BOT_MARKER


BOT = {"__typename": "Bot", "login": "github-actions[bot]"}


def thread(number, *, resolved=True, reactions=(), review_body=BOT_MARKER, author=BOT):
    return {
        "isResolved": resolved,
        "comments": {
            "nodes": [
                {
                    "author": author,
                    "pullRequestReview": {"author": author, "body": review_body},
                    "reactionGroups": [
                        {"content": content, "reactors": {"totalCount": count}}
                        for content, count in reactions
                    ],
                    "url": (
                        "https://github.com/delta-io/delta-kernel-rs/pull/1"
                        f"#discussion_r{number}"
                    ),
                }
            ]
        },
    }


class TestReviewFeedback(unittest.TestCase):
    def test_each_feedback_reaction_counts_on_a_resolved_thread(self):
        for reaction in ("THUMBS_UP", "THUMBS_DOWN", "EYES"):
            with self.subTest(reaction=reaction):
                passed, summary = evaluate_feedback(
                    [thread(1, reactions=[(reaction, 1)])], 100
                )
                self.assertTrue(passed)
                self.assertIn("1 of 1", summary)

    def test_both_resolution_and_a_supported_reaction_are_required(self):
        threads = [
            thread(1, resolved=False, reactions=[("THUMBS_UP", 1)]),
            thread(2, reactions=[("HEART", 1), ("EYES", 0)]),
        ]
        passed, summary = evaluate_feedback(threads, 1)
        self.assertFalse(passed)
        self.assertIn("0 of 2", summary)
        self.assertIn("#discussion_r1) — resolve the thread.", summary)
        self.assertIn("#discussion_r2) — react with 👍, 👎, or 👀.", summary)
        self.assertTrue(evaluate_feedback(threads, 0)[0])

    def test_exact_percentage_boundary(self):
        reacted = thread(1, reactions=[("EYES", 1)])
        unrated = thread(2)
        self.assertTrue(evaluate_feedback([reacted, unrated], 50)[0])
        self.assertFalse(evaluate_feedback([reacted, unrated], 51)[0])
        another_reacted = thread(3, reactions=[("THUMBS_DOWN", 1)])
        self.assertFalse(evaluate_feedback([reacted, another_reacted, unrated], 67)[0])

    def test_zero_matching_findings_passes(self):
        threads = [
            thread(1, review_body="An unrelated GitHub Actions review"),
            thread(2, author={"__typename": "User", "login": "someone"}),
            thread(3, author={"__typename": "Bot", "login": "another-bot[bot]"}),
            {"isResolved": False, "comments": {"nodes": []}},
        ]
        passed, summary = evaluate_feedback(threads, 100)
        self.assertTrue(passed)
        self.assertIn("No inline AI review findings", summary)


if __name__ == "__main__":
    unittest.main()
