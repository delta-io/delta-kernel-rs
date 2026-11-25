"""Tests for bounded previous-review selection and deduplication."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


def _load_module():
    path = Path(__file__).parents[1] / "review_history.py"
    spec = importlib.util.spec_from_file_location("review_history", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def _document(
    *, author_type: str = "Bot", author_login: str = "github-actions"
) -> dict:
    marker = "<!-- ai-review-bot -->"
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "comments": {
                        "nodes": [
                            {
                                "author": {
                                    "__typename": author_type,
                                    "login": author_login,
                                },
                                "body": f"{marker}\n## AI Review\n\nOld collapsed finding",
                                "createdAt": "2026-09-10T01:00:00Z",
                            }
                        ]
                    },
                    "reviews": {
                        "nodes": [
                            {
                                "author": {
                                    "__typename": author_type,
                                    "login": author_login,
                                },
                                "body": f"{marker}\n## AI Review\n\nOld review summary",
                                "submittedAt": "2026-09-10T02:00:00Z",
                                "comments": {
                                    "nodes": [
                                        {
                                            "path": "kernel/src/example.rs",
                                            "line": 10,
                                            "originalLine": 10,
                                            "fullDatabaseId": 101,
                                            "side": "RIGHT",
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


def _evidence_document() -> dict:
    document = _document()
    pull_request = document["data"]["repository"]["pullRequest"]
    for name in ("comments", "reviews"):
        pull_request[name]["pageInfo"] = {"hasPreviousPage": False, "hasNextPage": False}
    issue = pull_request["comments"]["nodes"][0]
    issue.update(id="issue-1", url="https://github.com/example/project/pull/1#issuecomment-1")
    review = pull_request["reviews"]["nodes"][0]
    review.update(
        id="review-1", url="https://github.com/example/project/pull/1#pullrequestreview-1",
        commit={"oid": "a" * 40},
    )
    review["comments"]["pageInfo"] = {"hasPreviousPage": False, "hasNextPage": False}
    comment = review["comments"]["nodes"][0]
    comment.update(
        id="inline-1", author={"__typename": "Bot", "login": "github-actions"},
        url="https://github.com/example/project/pull/1#discussion_r1",
    )
    return document


class ReviewHistoryTest(unittest.TestCase):
    def test_complete_history_retains_blocker_sha_source_and_coordinates(self) -> None:
        evidence = _load_module().history_evidence(_evidence_document())
        self.assertTrue(evidence["complete"], evidence["reasons"])
        self.assertEqual(len(evidence["blockers"]), 1)
        blocker = evidence["blockers"][0]
        self.assertEqual(blocker["id"], "Blocker1")
        self.assertEqual(blocker["head_sha"], "a" * 40)
        self.assertEqual(blocker["source_review_id"], "review-1")
        self.assertEqual(blocker["source_comment_id"], "inline-1")
        self.assertTrue(blocker["url"].endswith("#discussion_r1"))
        self.assertEqual((blocker["path"], blocker["line"], blocker["side"]),
                         ("kernel/src/example.rs", 10, "RIGHT"))
        self.assertEqual(len(blocker["fingerprint"]), 64)

    def test_history_fetch_errors_keep_known_blockers_and_mark_history_partial(self) -> None:
        module = _load_module()
        for flag in (
            "errors", "_history_available", "_inline_locations_available", "_history_clipped"
        ):
            with self.subTest(flag=flag):
                document = _evidence_document()
                document[flag] = [{"message": "partial data"}] if flag == "errors" else (
                    True if flag == "_history_clipped" else False
                )
                evidence = module.history_evidence(document)
                self.assertFalse(evidence["complete"])
                self.assertEqual(len(evidence["blockers"]), 1)
        self.assertFalse(module.history_evidence(
            _evidence_document(), inline_locations_available=False
        )["complete"])

    def test_missing_connections_pages_and_records_cannot_establish_completion(self) -> None:
        module = _load_module()
        self.assertFalse(module.history_evidence({})["complete"])
        for part in ("comments", "reviews", "inline"):
            for failure in ("pageInfo", "nodes", "page", "total", "record"):
                with self.subTest(part=part, failure=failure):
                    document = _evidence_document()
                    pull_request = document["data"]["repository"]["pullRequest"]
                    connection = (
                        pull_request["reviews"]["nodes"][0]["comments"]
                        if part == "inline" else pull_request[part]
                    )
                    if failure in {"pageInfo", "nodes"}:
                        connection.pop(failure)
                    elif failure == "page":
                        connection["pageInfo"]["hasNextPage"] = True
                    elif failure == "total":
                        connection["totalCount"] = 1000
                    else:
                        connection["nodes"].append(None)
                    self.assertFalse(module.history_evidence(document)["complete"])

    def test_body_only_legacy_blocker_labels_remain_in_history(self) -> None:
        module = _load_module()
        for label in (
            "### Blocker1", "### B1", "### [Blocker 1]", "**Blocker1**", "**[Blocker 1]**"
        ):
            with self.subTest(label=label):
                document = _evidence_document()
                review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
                review["comments"]["nodes"].clear()
                review["body"] = (
                    f"{module.BOT_MARKER}\n{label}\nBroken contract.\n## Summary\nContext."
                )
                evidence = module.history_evidence(document)
                self.assertTrue(evidence["complete"], evidence["reasons"])
                self.assertEqual(len(evidence["blockers"]), 1)
                blocker = evidence["blockers"][0]
                self.assertEqual(blocker["head_sha"], "a" * 40)
                self.assertTrue(blocker["url"].endswith("#pullrequestreview-1"))
                self.assertIn("Broken contract.", blocker["body"])
                self.assertNotIn("Context.", blocker["body"])

    def test_standalone_finding_metadata_and_full_reviewed_sha_are_trusted_sources(self) -> None:
        module = _load_module()
        for metadata in (True, False):
            with self.subTest(metadata=metadata):
                document = _evidence_document()
                pull_request = document["data"]["repository"]["pullRequest"]
                pull_request["reviews"]["nodes"].clear()
                comment = pull_request["comments"]["nodes"][0]
                source = (
                    '<!-- ai-review-finding '
                    + json.dumps({"id": "Blocker1", "head_sha": "b" * 40}) + ' -->'
                    if metadata else "Reviewed `" + "b" * 40 + "`"
                )
                comment["body"] = (
                    f"{module.BOT_MARKER}\n{source}\n**[Blocker 1]** A retained defect."
                )
                evidence = module.history_evidence(document)
                self.assertTrue(evidence["complete"], evidence["reasons"])
                self.assertEqual(evidence["blockers"][0]["head_sha"], "b" * 40)
                self.assertEqual(evidence["blockers"][0]["source_comment_id"], "issue-1")

    def test_independently_marked_inline_blocker_survives_empty_review_body(self) -> None:
        module = _load_module()
        document = _evidence_document()
        review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        review["body"] = ""
        review["comments"]["nodes"][0]["body"] = f"{module.BOT_MARKER}\n**[Blocker 1]** A defect."
        evidence = module.history_evidence(document)
        self.assertTrue(evidence["complete"])
        self.assertEqual(len(evidence["blockers"]), 1)

    def test_unknown_or_abbreviated_legacy_blocker_sha_prevents_complete_history(self) -> None:
        module = _load_module()
        document = _evidence_document()
        review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        review.pop("commit")
        review["comments"]["nodes"][0]["body"] += "\nReviewed `aaaaaaaa`"
        evidence = module.history_evidence(document)
        self.assertFalse(evidence["complete"])
        self.assertIn("blocker_sha_unknown", evidence["reasons"])
        self.assertIsNone(evidence["blockers"][0]["head_sha"])

    def test_unknown_blocker_destination_prevents_complete_history(self) -> None:
        module = _load_module()
        document = _evidence_document()
        review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        review.pop("url")
        review["comments"]["nodes"][0].pop("url")
        evidence = module.history_evidence(document)
        self.assertIn("blocker_url_unknown", evidence["reasons"])
        self.assertIsNone(evidence["blockers"][0]["url"])

    def test_fenced_metadata_and_reviewed_sha_do_not_assign_a_legacy_blocker_commit(self):
        module = _load_module()
        document = _evidence_document()
        review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        review.pop("commit")
        review["comments"]["nodes"].clear()
        metadata = json.dumps({"head_sha": "a" * 40})
        review["body"] = (
            f"{module.BOT_MARKER}\n```markdown\n<!-- ai-review-finding {metadata} -->\n"
            "Reviewed `" + "a" * 40 + "`\n```\n### B1\nAn actual legacy defect."
        )
        evidence = module.history_evidence(document)
        self.assertFalse(evidence["complete"])
        self.assertIsNone(evidence["blockers"][0]["head_sha"])

    def test_spoofed_blocker_markers_do_not_create_trusted_findings(self) -> None:
        module = _load_module()
        for author_type, login in (("User", "github-actions"), ("Bot", "untrusted-app")):
            with self.subTest(author_type=author_type, login=login):
                document = _evidence_document()
                pull_request = document["data"]["repository"]["pullRequest"]
                entries = [*pull_request["comments"]["nodes"], *pull_request["reviews"]["nodes"]]
                for entry in entries:
                    entry["author"] = {"__typename": author_type, "login": login}
                pull_request["reviews"]["nodes"][0]["comments"]["nodes"][0]["author"] = {
                    "__typename": author_type, "login": login,
                }
                evidence = module.history_evidence(document)
                self.assertTrue(evidence["complete"])
                self.assertEqual(evidence["blockers"], [])

    def test_fenced_blocker_examples_and_clean_review_do_not_create_blockers(self) -> None:
        module = _load_module()
        document = _evidence_document()
        review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        review["comments"]["nodes"].clear()
        review["body"] = (
            f"{module.BOT_MARKER}\nNo blocking issues.\n```markdown\n"
            "### Blocker1\nExample only.\n```\n> **B2** Quoted example."
        )
        evidence = module.history_evidence(document)
        self.assertTrue(evidence["complete"])
        self.assertEqual(evidence["blockers"], [])

    def test_summary_link_and_its_actual_comment_count_as_one_blocker_source(self) -> None:
        module = _load_module()
        document = _evidence_document()
        pull_request = document["data"]["repository"]["pullRequest"]
        comment = pull_request["comments"]["nodes"][0]
        metadata = json.dumps({"head_sha": "a" * 40})
        comment["body"] = (
            f"{module.BOT_MARKER}\n<!-- ai-review-main {metadata} -->\n"
            "<details><summary>Review Summary</summary>\n\n"
            "- **[Blocker 1]** [Repeated finding]"
            "(https://github.com/example/project/pull/1#discussion_r1)\n"
            "</details>"
        )
        evidence = module.history_evidence(document)
        self.assertTrue(evidence["complete"])
        self.assertEqual(len(evidence["blockers"]), 1)
        self.assertEqual(evidence["blockers"][0]["source_comment_id"], "inline-1")

    def test_same_destination_reaffirmed_on_another_sha_keeps_both_observations(self) -> None:
        module = _load_module()
        document = _evidence_document()
        comment = document["data"]["repository"]["pullRequest"]["comments"]["nodes"][0]
        metadata = json.dumps({"head_sha": "b" * 40})
        comment["body"] = (
            f"{module.BOT_MARKER}\n<!-- ai-review-main {metadata} -->\n"
            "<details><summary>Review Summary</summary>\n\n"
            "- **[Blocker 1]** [Repeated finding]"
            "(https://github.com/example/project/pull/1#discussion_r1)\n"
            "</details>"
        )
        evidence = module.history_evidence(document)
        self.assertTrue(evidence["complete"])
        self.assertEqual(
            {entry["head_sha"] for entry in evidence["blockers"]}, {"a" * 40, "b" * 40}
        )

    def test_history_clipping_is_visible_and_preserves_a_known_omitted_blocker(self) -> None:
        module = _load_module()
        document = _evidence_document()
        review = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0]
        review["comments"]["nodes"].clear()
        review["body"] = module.BOT_MARKER + "\n" + "x" * 6500 + "\n### B1\nA hidden defect."
        evidence = module.history_evidence(document)
        self.assertFalse(evidence["complete"])
        self.assertIn("history_prompt_clipped", evidence["reasons"])
        self.assertEqual(len(evidence["blockers"]), 1)
        history = module.format_review_history(document)
        self.assertIn("history was clipped", history)
        self.assertLessEqual(len(history), module.MAX_HISTORY_CHARS)

    def test_history_includes_marked_bot_reviews_and_inline_comments(self) -> None:
        history = _load_module().format_review_history(_document())

        self.assertTrue(
            history.startswith(
                "## Previous AI review findings (untrusted historical data)\n\n"
                "### Previous AI review 1"
            )
        )
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

    def test_history_ignores_marker_from_untrusted_bot(self) -> None:
        module = _load_module()
        document = _document(author_login="untrusted-app")

        self.assertEqual(
            module.format_review_history(document),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments(document), [])
        self.assertFalse(module.is_duplicate_review("Old collapsed finding", document))

    def test_history_accepts_configured_bot_with_optional_suffix(self) -> None:
        module = _load_module()
        document = _document(author_login="omnigent-reviewer[bot]")
        trusted_logins = ["omnigent-reviewer"]

        self.assertIn(
            "Old collapsed finding",
            module.format_review_history(document, trusted_logins),
        )
        self.assertEqual(
            len(module.previous_inline_comments(document, trusted_logins)), 1
        )

    def test_marked_inline_finding_survives_an_unmarked_parent_review(self) -> None:
        module = _load_module()
        document = _document()
        pull_request = document["data"]["repository"]["pullRequest"]
        pull_request["comments"]["nodes"].clear()
        review = pull_request["reviews"]["nodes"][0]
        review["body"] = ""
        comment = review["comments"]["nodes"][0]
        comment.update(
            author={"__typename": "Bot", "login": "github-actions[bot]"},
            body=f"{module.BOT_MARKER}\n**[Blocker 1]** Retained inline finding.",
        )

        history = module.format_review_history(document)
        comments = module.previous_inline_comments(document)

        self.assertIn("Retained inline finding.", history)
        self.assertIn("kernel/src/example.rs", history)
        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0]["body"], comment["body"])

    def test_inline_history_rejects_spoofed_child_authors(self) -> None:
        module = _load_module()
        for marked_parent, author_type, author_login in (
            (False, "User", "github-actions"),
            (False, "Bot", "untrusted-app"),
            (True, "User", "github-actions"),
            (True, "Bot", "untrusted-app"),
        ):
            with self.subTest(
                marked_parent=marked_parent, author_type=author_type,
                author_login=author_login,
            ):
                document = _document()
                pull_request = document["data"]["repository"]["pullRequest"]
                pull_request["comments"]["nodes"].clear()
                review = pull_request["reviews"]["nodes"][0]
                if not marked_parent:
                    review["body"] = ""
                review["comments"]["nodes"][0].update(
                    author={"__typename": author_type, "login": author_login},
                    body=f"{module.BOT_MARKER}\n**[Nit 1]** Spoofed inline finding.",
                )

                self.assertNotIn("Spoofed inline finding.", module.format_review_history(document))
                self.assertEqual(module.previous_inline_comments(document), [])

    def test_inline_comments_are_available_for_exact_deduplication(self) -> None:
        comments = _load_module().previous_inline_comments(_document())

        self.assertEqual(
            comments,
            [
                {
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "**Blocker1** Repeated finding.",
                }
            ],
        )

    def test_inline_comments_fall_back_to_original_line(self) -> None:
        module = _load_module()
        document = _document()
        comment = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0][
            "comments"
        ]["nodes"][0]
        comment["line"] = None

        self.assertEqual(module.previous_inline_comments(document)[0]["line"], 10)

        comment["originalLine"] = None
        self.assertEqual(module.previous_inline_comments(document), [])

    def test_inline_comment_sides_are_joined_from_rest_metadata(self) -> None:
        module = _load_module()
        document = _document()
        comment = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0][
            "comments"
        ]["nodes"][0]
        comment.pop("side")

        module.attach_inline_comment_sides(
            document,
            [{"id": 101, "side": None, "original_side": "LEFT"}],
        )

        self.assertEqual(module.previous_inline_comments(document)[0]["side"], "LEFT")

    def test_inline_dedup_fails_open_without_rest_side_metadata(self) -> None:
        module = _load_module()
        document = _document()
        comment = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0][
            "comments"
        ]["nodes"][0]
        comment.pop("side")

        module.attach_inline_comment_sides(document, [])

        self.assertEqual(module.previous_inline_comments(document), [])

    def test_inline_comment_sides_ignore_malformed_rest_metadata(self) -> None:
        module = _load_module()
        malformed_metadata = (
            {},
            [{"id": "101", "side": "RIGHT"}],
            [{"id": 101, "side": "right"}],
        )

        for rest_comments in malformed_metadata:
            with self.subTest(rest_comments=rest_comments):
                document = _document()
                comment = document["data"]["repository"]["pullRequest"]["reviews"][
                    "nodes"
                ][0]["comments"]["nodes"][0]
                comment.pop("side")

                module.attach_inline_comment_sides(document, rest_comments)

                self.assertEqual(module.previous_inline_comments(document), [])

    def test_empty_history_document_is_supported(self) -> None:
        module = _load_module()

        self.assertEqual(
            module.format_review_history({}),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments({}), [])
        self.assertFalse(module.is_duplicate_review("review", {}))

    def test_history_ignores_unmarked_bot_entries(self) -> None:
        module = _load_module()
        document = _document()
        pull_request = document["data"]["repository"]["pullRequest"]
        pull_request["comments"]["nodes"][0]["body"] = "Unmarked bot comment"
        pull_request["reviews"]["nodes"][0]["body"] = "Unmarked bot review"

        self.assertEqual(
            module.format_review_history(document),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments(document), [])

    def test_finding_normalization_ignores_run_specific_id_and_whitespace(self) -> None:
        module = _load_module()

        self.assertEqual(
            module.canonical_finding_body("**Nit2**  Repeated\n finding."),
            module.canonical_finding_body("**Blocker1** Repeated finding."),
        )

    def test_complete_review_deduplication_round_trips_publication_wrapper(self) -> None:
        module = _load_module()
        review_publish_path = Path(__file__).parents[1] / "review_publish.py"
        spec = importlib.util.spec_from_file_location(
            "review_publish", review_publish_path
        )
        assert spec is not None and spec.loader is not None
        review_publish = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(review_publish)
        document = _document()
        document["data"]["repository"]["pullRequest"]["comments"]["nodes"][0][
            "body"
        ] = review_publish.format_review_body(
            "Old collapsed finding",
            "https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            collapsed=True,
        )

        self.assertTrue(module.is_duplicate_review("Old collapsed finding", document))
        self.assertIn("Old collapsed finding", module.format_review_history(document))
        self.assertNotIn("<details>", module.format_review_history(document))
        self.assertFalse(module.is_duplicate_review("New finding", document))

    def test_history_is_newest_first_and_bounded(self) -> None:
        module = _load_module()
        document = _document()
        nodes = document["data"]["repository"]["pullRequest"]["comments"]["nodes"]
        nodes.clear()
        review_nodes = document["data"]["repository"]["pullRequest"]["reviews"][
            "nodes"
        ]
        review_nodes.clear()
        marker = module.BOT_MARKER
        for index in (0, 2, 3):
            nodes.append(
                {
                    "author": {
                        "__typename": "Bot",
                        "login": "github-actions",
                    },
                    "body": f"{marker}\nreview-{index}-" + "x" * 3_500,
                    "createdAt": f"2026-09-10T0{index}:00:00Z",
                }
            )
        review_nodes.append(
            {
                "author": {"__typename": "Bot", "login": "github-actions"},
                "body": f"{marker}\nreview-1-" + "x" * 3_500,
                "submittedAt": "2026-09-10T01:00:00Z",
                "comments": {"nodes": []},
            }
        )

        history = module.format_review_history(document)

        self.assertLessEqual(len(history), module.MAX_HISTORY_CHARS)
        self.assertLess(history.index("review-3"), history.index("review-2"))
        self.assertLess(history.index("review-2"), history.index("review-1"))
        self.assertNotIn("review-0", history)

    def test_history_truncates_each_entry(self) -> None:
        module = _load_module()
        document = _document()
        node = document["data"]["repository"]["pullRequest"]["comments"]["nodes"][0]
        node["body"] = module.BOT_MARKER + "\n" + "Z" * (module.MAX_ENTRY_CHARS + 100)

        history = module.format_review_history(document)

        self.assertEqual(history.count("Z"), module.MAX_ENTRY_CHARS)


if __name__ == "__main__":
    unittest.main()
