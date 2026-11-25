import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from review_feedback import CHECK_NAME, MAX_LISTED_FINDINGS, evaluate_feedback, main
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


    def test_outstanding_findings_summary_limits_links(self):
        findings = [thread(i, resolved=False) for i in range(MAX_LISTED_FINDINGS + 1)]

        passed, summary = evaluate_feedback(findings, 100)

        self.assertFalse(passed)
        self.assertEqual(summary.count("[Inline finding]"), MAX_LISTED_FINDINGS)
        self.assertIn("resolve the thread and react with 👍, 👎, or 👀", summary)
        self.assertIn(f"Showing {MAX_LISTED_FINDINGS} of {len(findings)} findings", summary)


class TestReviewFeedbackMain(unittest.TestCase):
    def test_publishes_check_from_all_pages_including_configured_app(self):
        repo = "delta-io/delta-kernel-rs"
        app_bot = {"__typename": "Bot", "login": "omnigent-example[bot]"}
        head_sha = "d" * 40
        pages = [
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "headRefOid": head_sha,
                            "reviewThreads": {"nodes": [finding]},
                        }
                    }
                }
            }
            for finding in (
                thread(1, reactions=[("EYES", 1)]),
                thread(2, author=app_bot, resolved=False),
            )
        ]
        for minimum, conclusion in (("50", "success"), ("51", "failure")):
            with (
                self.subTest(minimum=minimum),
                tempfile.TemporaryDirectory() as directory,
            ):
                summary_path = pathlib.Path(directory) / "summary.md"
                env = {
                    "PR_NUMBER": "1",
                    "MIN_ADDRESSED_PERCENT": minimum,
                    "OMNIGENT_BOT_LOGIN": " Omnigent-Example[bot] ",
                    "GITHUB_REPOSITORY": repo,
                    "GITHUB_SERVER_URL": "https://github.com",
                    "GITHUB_RUN_ID": "123",
                    "GITHUB_STEP_SUMMARY": str(summary_path),
                }
                with (
                    patch.dict(os.environ, env),
                    patch("review_feedback.subprocess.check_output") as query,
                    patch("review_feedback.subprocess.run") as publish,
                ):
                    query.return_value = json.dumps(pages).encode()
                    self.assertIsNone(main())

                query.assert_called_once()
                for option in ("--paginate", "--slurp", "pr=1"):
                    self.assertIn(option, query.call_args.args[0])
                publish.assert_called_once()
                self.assertIn(f"repos/{repo}/check-runs", publish.call_args.args[0])
                self.assertTrue(publish.call_args.kwargs["check"])
                check = json.loads(publish.call_args.kwargs["input"])
                self.assertEqual(check["name"], CHECK_NAME)
                self.assertEqual(check["head_sha"], head_sha)
                self.assertEqual(check["conclusion"], conclusion)
                self.assertIn("1 of 2 inline AI review findings", check["output"]["summary"])
                self.assertIn("#discussion_r2", check["output"]["summary"])
                self.assertEqual(
                    summary_path.read_text(encoding="utf-8"), check["output"]["summary"] + "\n"
                )

    def test_invalid_input_stops_before_querying_github(self):
        for pr, minimum, error in (
            ("unknown", "100", "positive integer"),
            ("0", "100", "positive integer"),
            ("1", "1.5", "integer from 0 to 100"),
            ("1", "101", "integer from 0 to 100"),
        ):
            with (
                self.subTest(pr=pr, minimum=minimum),
                patch.dict(os.environ, {"PR_NUMBER": pr, "MIN_ADDRESSED_PERCENT": minimum}),
                patch("review_feedback.subprocess.check_output") as query,
            ):
                with self.assertRaisesRegex(SystemExit, error):
                    main()
                query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
