import unittest

from extract_patterns import source_lane
from review_feedback import feedback_index, rank_feedback, validate_reply_annotations


def comment(identifier, author, body="body", parent=None, **extra):
    return {
        "id": identifier,
        "key": f"inline:{identifier}",
        "pr": 1,
        "author": author,
        "role": "bot" if author.endswith("[bot]") else "author",
        "body": body,
        "url": f"https://example/{identifier}",
        "reply_to": parent,
        "automation_marker": False,
        "reactions": {},
        **extra,
    }


def annotation(finding, reply, signal):
    return {
        "finding_key": finding,
        "event_key": reply,
        "quote": "body",
        "signal": signal,
        "rationale": "The reply explicitly states this outcome.",
    }


class ReviewFeedbackTests(unittest.TestCase):
    def test_reply_binding_follows_ancestors_and_excludes_unrelated_conversations(self):
        comments = [
            comment(1, "github-actions[bot]", reactions={"+1": 1}),
            comment(2, "alice", parent=1),
            comment(3, "bob", parent=2),
            comment(4, "alice"),
            comment(5, "alice", parent=4),
        ]
        index = feedback_index([{"comments": comments}], source_lane)
        self.assertEqual(
            [r["event_key"] for r in index["inline:1"]["replies"]], ["inline:2", "inline:3"]
        )
        with self.assertRaises(ValueError):
            validate_reply_annotations(
                [annotation("inline:1", "inline:5", "endorsed")], {"inline:1"}, index
            )

    def test_thumb_up_weighs_more_than_one_positive_reply(self):
        index = {"inline:1": {"pr": 1, "reactions": {"+1": 1}, "replies": []}}
        up = rank_feedback({"inline:1"}, index, [])
        reply = {
            **annotation("inline:1", "inline:2", "implemented"),
            "author": "alice",
            "automation_marker": False,
        }
        index["inline:1"]["reactions"] = {}
        index["inline:1"]["replies"] = [reply]
        implemented = rank_feedback({"inline:1"}, index, [reply])
        self.assertGreater(up["add_score"], implemented["add_score"])
        self.assertGreater(implemented["add_score"], 0)

    def test_friendly_correction_and_eyes_route_to_change_without_asserting_a_bug(self):
        index = {"inline:1": {"pr": 1, "reactions": {"eyes": 1}, "replies": [{}]}}
        corrected = {
            **annotation("inline:1", "inline:2", "corrected"),
            "author": "alice",
            "automation_marker": False,
        }
        score = rank_feedback({"inline:1"}, index, [corrected])
        self.assertEqual(score["add_score"], 0)
        self.assertGreater(score["findings"][0]["scope_change_score"], 0)
        eyes_only = rank_feedback({"inline:1"}, index, [])
        self.assertEqual(eyes_only["findings"][0]["correctness_change_score"], 0)

    def test_acknowledgment_and_ambiguous_replies_do_not_boost_rank(self):
        index = {"inline:1": {"pr": 1, "reactions": {}, "replies": [{}]}}
        for signal in ("acknowledged", "ambiguous"):
            reply = {
                **annotation("inline:1", "inline:2", signal),
                "author": "alice",
                "automation_marker": False,
            }
            with self.subTest(signal=signal):
                score = rank_feedback({"inline:1"}, index, [reply])
                self.assertEqual((score["add_score"], score["change_score"]), (0, 0))

    def test_ai_marked_and_repeated_replies_do_not_outweigh_independent_feedback(self):
        index = {"inline:1": {"pr": 1, "reactions": {}, "replies": [{}, {}]}}
        replies = [
            {
                **annotation("inline:1", f"inline:{n}", "implemented"),
                "author": "alice",
                "automation_marker": True,
            }
            for n in (2, 3)
        ]
        score = rank_feedback({"inline:1"}, index, replies)
        self.assertEqual(score["findings"][0]["positive_reply_units"], 0.25)
        replies[0]["automation_marker"] = False
        score = rank_feedback({"inline:1"}, index, replies)
        self.assertEqual(score["findings"][0]["positive_reply_units"], 1)

    def test_mixed_votes_remain_visible_and_same_pr_findings_cannot_inflate_priority(self):
        index = {
            key: {"pr": 1, "reactions": {"+1": 1, "-1": 1}, "replies": []}
            for key in ("inline:1", "inline:2")
        }
        one = rank_feedback({"inline:1"}, index, [])
        two = rank_feedback(set(index), index, [])
        self.assertTrue(two["mixed_feedback"])
        self.assertEqual(two["add_score"], one["add_score"])
        self.assertEqual(two["change_score"], one["change_score"])

    def test_disagreement_across_prs_is_also_mixed_feedback(self):
        index = {
            "inline:1": {"pr": 1, "reactions": {"+1": 1}, "replies": []},
            "inline:2": {"pr": 2, "reactions": {"-1": 1}, "replies": []},
        }
        result = rank_feedback(set(index), index, [])
        self.assertTrue(result["mixed_feedback"])


if __name__ == "__main__":
    unittest.main()
