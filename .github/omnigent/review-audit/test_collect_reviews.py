import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import collect_reviews as audit

SINCE = audit.timestamp("2026-09-17T00:00:00Z")
UNTIL = audit.timestamp("2026-11-12T00:00:00Z")


def pull(number=3831, state="open", merged_at=None, updated_at="2026-11-11T00:00:00Z"):
    return {
        "number": number,
        "title": "example",
        "state": state,
        "merged_at": merged_at,
        "updated_at": updated_at,
        "user": {"login": "author"},
    }


def comment(identifier, date, body="feedback", reply=None, login="reviewer"):
    return {
        "id": identifier,
        "created_at": date,
        "body": body,
        "html_url": f"https://github.com/example/repo/pull/3831#discussion_r{identifier}",
        "user": {"login": login, "type": "User"},
        "in_reply_to_id": reply,
    }


class ReviewAuditTests(unittest.TestCase):
    def test_identity_assertion_is_optional_and_checks_effective_api_identity(self):
        for required, accepted in ((None, True), ("MAINTAINER", True), ("different-user", False)):
            with self.subTest(required=required), tempfile.TemporaryDirectory() as directory:
                argv = ["collect_reviews.py", "example/repo", "--output", directory]
                if required:
                    argv += ["--github-user", required]
                with (
                    patch.object(audit.sys, "argv", argv),
                    patch.object(audit, "api", return_value={"login": "maintainer"}),
                    patch.object(audit, "discover", return_value=[]) as discover,
                    patch.object(audit, "export", return_value={}),
                    redirect_stdout(StringIO()),
                    redirect_stderr(StringIO()),
                ):
                    if accepted:
                        audit.main()
                        discover.assert_called_once()
                    else:
                        with self.assertRaises(SystemExit) as error:
                            audit.main()
                        self.assertEqual(error.exception.code, 2)
                        discover.assert_not_called()

    def test_calendar_month_window_preserves_time_and_clamps_month_end(self):
        for end, months, start in (
            ("2026-11-12T04:43:42Z", 4, "2026-07-12T04:43:42Z"),
            ("2026-03-31T12:34:56Z", 1, "2026-02-28T12:34:56Z"),
            ("2024-03-31T12:34:56Z", 1, "2024-02-29T12:34:56Z"),
            ("2026-01-31T12:34:56Z", 4, "2025-09-30T12:34:56Z"),
        ):
            with self.subTest(end=end, months=months):
                self.assertEqual(
                    audit.iso(audit.months_before(audit.timestamp(end), months)), start
                )

    def test_discovery_includes_old_merged_and_open_prs_and_stops_after_cutoff(self):
        batches = [
            [pull(1), pull(2, "closed", "2026-10-01T00:00:00Z"), pull(3, "closed")],
            [pull(4), pull(5, updated_at="2026-09-16T00:00:00Z")],
        ]
        with patch.object(audit, "api", side_effect=batches) as api:
            found = audit.discover("example/repo", None, SINCE)
        self.assertEqual([p["number"] for p in found], [1, 2, 4])
        self.assertEqual(api.call_count, 2)

    def test_filters_creation_not_edit_time_and_preserves_old_thread_context(self):
        old = comment(1, audit.iso(SINCE - timedelta(seconds=1)))
        old["updated_at"] = audit.iso(UNTIL)
        document = {
            "pull": pull(),
            "inline": [
                old,
                comment(2, audit.iso(SINCE), reply=1),
                comment(3, audit.iso(UNTIL), reply=2),
                comment(4, audit.iso(UNTIL + timedelta(seconds=1))),
            ],
            "review": [],
            "discussion": [],
        }
        events = audit.normalize(document, SINCE, UNTIL)
        self.assertEqual([e["id"] for e in events if e["in_window"]], [2, 3])
        threads = audit.review_threads(
            [e for e in events if audit.timestamp(e["created_at"]) <= UNTIL]
        )
        self.assertEqual(len(threads), 1)
        self.assertEqual(threads[0]["root_id"], 1)
        self.assertEqual([e["id"] for e in threads[0]["comments"]], [1, 2, 3])

    def test_submitted_review_counts_do_not_conflate_empty_bodies_or_bot_identity(self):
        review = comment(1, audit.iso(SINCE), body="")
        review["submitted_at"] = audit.iso(UNTIL)
        review["state"] = "APPROVED"
        pending = {**review, "id": 2, "submitted_at": None}
        bot = comment(1, audit.iso(SINCE), login="automation[bot]")
        author = comment(2, audit.iso(SINCE), body="[ 🤖 ] done", login="author")
        document = {
            "pull": pull(),
            "inline": [bot, author],
            "review": [review, pending],
            "discussion": [],
        }
        events = audit.normalize(document, SINCE, UNTIL)
        self.assertEqual(len(events), 3)
        self.assertEqual(len({e["key"] for e in events}), 3)
        self.assertEqual([e["role"] for e in events if e["kind"] == "inline"], ["bot", "author"])
        self.assertTrue(next(e for e in events if e["id"] == 2)["automation_marker"])
        with tempfile.TemporaryDirectory() as directory:
            summary = audit.export(
                [document],
                {"repo": "example/repo", "since": audit.iso(SINCE), "until": audit.iso(UNTIL)},
                Path(directory),
            )
            self.assertEqual(summary["event_counts"], {"inline": 2, "review": 1})
            self.assertEqual(summary["review_bodies"], 0)
            self.assertNotIn("empty review body", (Path(directory) / "corpus.md").read_text())

    def test_pagination_flattens_every_page_without_a_shell(self):
        with patch.object(audit.subprocess, "run") as run:
            run.return_value.stdout = '[[{"id": 1}], [{"id": 2}]]'
            self.assertEqual(
                audit.api("repos/example/repo/pulls/1/comments", True), [{"id": 1}, {"id": 2}]
            )
        self.assertIn("--paginate", run.call_args.args[0])
        self.assertIn("--method", run.call_args.args[0])
        self.assertIn("GET", run.call_args.args[0])

    def test_target_and_timezone_validation(self):
        self.assertEqual(
            audit.parse_target("https://github.com/delta-io/delta-kernel-rs/pull/3831"),
            ("delta-io/delta-kernel-rs", 3831),
        )
        self.assertEqual(
            audit.parse_target("delta-io/delta-kernel-rs"), ("delta-io/delta-kernel-rs", None)
        )
        for target in ("https://elsewhere.test/a/b", "a/b/pull/1;pwd", "a/b/pull/0"):
            with self.subTest(target=target), self.assertRaises(ValueError):
                audit.parse_target(target)
        with self.assertRaises(ValueError):
            audit.timestamp("2026-11-12T00:00:00")


if __name__ == "__main__":
    unittest.main()
