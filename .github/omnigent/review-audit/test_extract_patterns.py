import copy
import json
import tempfile
import unittest
from pathlib import Path

import extract_patterns as core


def event(key, pr, role="reviewer", author="reviewer", body="Reuse the existing type."):
    return {
        "key": key,
        "pr": pr,
        "body": body,
        "role": role,
        "author": author,
        "in_window": True,
        "automation_marker": False,
        "url": f"https://example/{key}",
    }


def proposal(keys):
    return {
        "id": "reuse-types",
        "major_category": "design-structure",
        "category": "design",
        "principle": "Reuse an existing type.",
        "scope": "Overlapping private types",
        "exceptions": "Keep types that enforce invariants.",
        "integration": {
            "path": "AGENTS.md",
            "anchor": "## Code Style",
            "status": "strengthen",
            "existing_quote": "Existing rules.",
            "proposed_text": "Prefer an existing equivalent type.",
            "rationale": "Teach reviewers to check existing representations in src.",
            "code_paths": ["src"],
        },
        "evidence": [
            {"event_key": key, "quote": "Reuse the existing type.", "stance": "support"}
            for key in keys
        ],
    }


class PatternCoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        (self.repo / "src").mkdir()
        (self.repo / "AGENTS.md").write_text("## Code Style\nExisting rules.\n")

    def validate(self, candidate, events):
        return core.validated_pattern(
            candidate, events, {key: f"thread-{e['pr']}" for key, e in events.items()}, self.repo
        )

    def test_repeated_thread_comments_and_summary_on_one_pr_do_not_make_recurrence(self):
        events = {key: event(key, 1) for key in ("inline:1", "inline:2", "review:1")}
        result = self.validate(proposal(list(events) + ["inline:1"]), events)
        self.assertFalse(result["support"]["recurring"])
        self.assertEqual(result["support"]["prs"], [1])
        self.assertEqual(len(result["support"]["discussion_units"]), 1)
        self.assertEqual(len(result["evidence"]), 3)
        self.assertEqual(result["recommendation_state"], "idea")

    def test_user_ideas_need_neither_reactions_recurrence_nor_final_wording(self):
        candidate = proposal(["inline:1"])
        candidate["integration"] = {
            "path": "AGENTS.md",
            "status": "unassessed",
            "rationale": "Look for overlapping types in src.",
            "code_paths": ["src"],
        }
        events = {"inline:1": event("inline:1", 1)}
        results = []
        for reactions in ({}, {"+1": 100}, {"-1": 100, "eyes": 100}):
            events["inline:1"]["reactions"] = reactions
            results.append(core.validated_pattern(candidate, events, {}, self.repo))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])
        self.assertEqual(results[0]["recommendation_state"], "idea")
        self.assertEqual(results[0]["bucket"], "unassessed")
        self.assertIsNone(results[0]["feedback"])
        self.assertEqual(results[0]["integration"]["path"], "AGENTS.md")
        self.assertFalse(results[0]["support"]["recurring"])
        for action, bucket in (
            ("add", "add"),
            ("change", "remove-change"),
            ("remove", "remove-change"),
        ):
            with self.subTest(action=action):
                candidate["suggested_action"] = action
                self.assertEqual(
                    core.validated_pattern(candidate, events, {}, self.repo)["bucket"], bucket
                )

    def test_other_bots_stay_separate_and_do_not_use_emoji_scoring(self):
        candidate = proposal(["inline:1"])
        events = {"inline:1": event("inline:1", 1, role="bot", author="other[bot]")}
        result = core.validated_pattern(candidate, events, {}, self.repo, lane="other-bots")
        self.assertEqual(result["recommendation_state"], "idea")
        self.assertEqual(result["lane"], "other-bots")
        self.assertIsNone(result["feedback"])

    def test_bot_evidence_cannot_enter_user_synthesis_even_as_context(self):
        events = {
            "inline:1": event("inline:1", 1),
            "inline:2": event("inline:2", 2, role="bot"),
            "inline:3": event("inline:3", 3, role="author"),
        }
        for stance in ("support", "context", "oppose"):
            candidate = proposal(list(events))
            candidate["evidence"][1]["stance"] = stance
            with self.subTest(stance=stance), self.assertRaises(ValueError):
                self.validate(candidate, events)
        result = self.validate(proposal(["inline:1", "inline:3"]), events)
        self.assertEqual(result["support"]["prs"], [1])
        self.assertFalse(result["support"]["recurring"])
        self.assertEqual(len(result["evidence"]), 2)

    def test_prepare_separates_review_bodies_and_labels_attached_user_feedback(self):
        corpus = self.repo / "corpus"
        corpus.mkdir()
        bot = {
            **event("inline:1", 1, role="bot", author="github-actions[bot]", body="BOT_ONLY_TEXT"),
            "id": 1,
            "kind": "inline",
            "pr_title": "title",
            "path": "a.rs",
            "line": 1,
            "original_line": 1,
            "reply_to": None,
            "reactions": {"+1": 1},
            "diff_hunk": "@@ example context @@",
            "commit_id": "reviewed-commit",
        }
        user = {**bot, **event("inline:2", 1, body="USER_ONLY_TEXT"), "id": 2, "reply_to": 1}
        thread = {"pr": 1, "root_id": 1, "comments": [bot, user]}
        (corpus / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in (bot, user)))
        (corpus / "threads.jsonl").write_text(json.dumps(thread) + "\n")
        for lane, present, absent in (
            ("users", "USER_ONLY_TEXT", "BOT_ONLY_TEXT"),
            ("github-actions", "BOT_ONLY_TEXT", "USER_ONLY_TEXT"),
        ):
            destination = self.repo / lane
            core.prepare(corpus, destination, lane=lane)
            text = (destination / "units.jsonl").read_text()
            unit = json.loads(text)
            review_text = json.dumps(unit["events"])
            self.assertIn(present, review_text)
            self.assertNotIn(absent, review_text)
            if lane == "users":
                self.assertNotIn("BOT_ONLY_TEXT", text)
                self.assertNotIn("feedback_on_findings", unit)
                self.assertEqual(unit["events"][0]["diff_hunk"], "@@ example context @@")
                self.assertEqual(unit["events"][0]["commit_id"], "reviewed-commit")
            else:
                self.assertEqual(
                    unit["feedback_on_findings"][0]["replies"][0]["body"], "USER_ONLY_TEXT"
                )
                self.assertEqual(unit["events"][0]["reactions"]["+1"], 1)

    def test_prepare_keeps_old_thread_context_author_replies_and_pr_conversations(self):
        corpus = self.repo / "corpus"
        corpus.mkdir()
        older = {
            **event("inline:1", 1),
            "id": 1,
            "in_window": False,
            "kind": "inline",
            "pr_title": "title",
            "path": "a.rs",
            "line": 1,
            "original_line": 1,
            "reply_to": None,
        }
        author = {
            **older,
            **event("inline:2", 1, role="author", body="The wrapper is needed for ownership."),
            "id": 2,
            "reply_to": 1,
        }
        discussion = {
            **author,
            "key": "discussion:3",
            "id": 3,
            "kind": "discussion",
            "reply_to": None,
        }
        review = {**older, **event("review:4", 1), "id": 4, "kind": "review"}
        events = [author, discussion, review]
        (corpus / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
        (corpus / "threads.jsonl").write_text(
            json.dumps({"pr": 1, "root_id": 1, "comments": [older, author]}) + "\n"
        )
        destination = self.repo / "prepared"
        core.prepare(corpus, destination)
        units = core.read_jsonl(destination / "units.jsonl")
        self.assertEqual(len(units), 2)
        thread = next(unit for unit in units if "thread" in unit["unit_id"])
        self.assertEqual([e["key"] for e in thread["events"]], ["inline:1", "inline:2"])
        self.assertEqual(thread["feedback_keys"], [])
        self.assertEqual(thread["recent_keys"], ["inline:2"])
        conversation = next(unit for unit in units if "conversation" in unit["unit_id"])
        self.assertEqual([e["key"] for e in conversation["events"]], ["discussion:3", "review:4"])
        candidate = proposal(["inline:1", "inline:2"])
        candidate["evidence"][1]["quote"] = author["body"]
        result = self.validate(candidate, {e["key"]: e for e in [older, author]})
        self.assertEqual(result["support"]["prs"], [])
        self.assertEqual(result["recommendation_state"], "context-only-idea")

    def test_bot_patterns_remain_isolated_even_when_recurring(self):
        events = {
            key: event(key, pr, role="bot", author="github-actions[bot]")
            for pr, key in enumerate(("inline:1", "inline:2"), 1)
        }
        result = core.validated_pattern(
            proposal(list(events)),
            events,
            {key: key for key in events},
            self.repo,
            lane="github-actions",
        )
        self.assertTrue(result["support"]["recurring"])
        self.assertEqual(result["recommendation_state"], "insufficient-feedback")
        self.assertEqual(result["lane"], "github-actions")

    def test_positive_single_pr_bot_feedback_stays_a_provisional_idea(self):
        events = {"inline:1": event("inline:1", 1, role="bot", author="github-actions[bot]")}
        candidate = proposal(["inline:1"])
        candidate["integration"]["status"] = "new"
        feedback = {"inline:1": {"pr": 1, "reactions": {"+1": 1}, "replies": []}}
        result = core.validated_pattern(
            candidate, events, {}, self.repo, lane="github-actions", feedback=feedback
        )
        self.assertEqual(result["recommendation_state"], "single-pr-bot-idea")
        self.assertEqual(result["feedback"]["add_score"], 3)
        self.assertFalse(result["support"]["recurring"])

    def test_add_and_change_buckets_depend_on_existing_guidance(self):
        events = {key: event(key, pr) for pr, key in enumerate(("inline:1", "inline:2"), 1)}
        candidate = proposal(list(events))
        self.assertEqual(self.validate(candidate, events)["bucket"], "remove-change")
        candidate["integration"]["status"] = "new"
        self.assertEqual(self.validate(candidate, events)["bucket"], "add")
        candidate["integration"]["status"] = "strengthen"
        candidate["integration"]["operation"] = "remove"
        candidate["integration"]["proposed_text"] = ""
        self.assertEqual(self.validate(candidate, events)["integration"]["operation"], "remove")
        del candidate["integration"]["existing_quote"]
        # A tentative deletion idea does not need the final patch or guidance excerpt.
        self.assertEqual(self.validate(candidate, events)["recommendation_state"], "idea")

    def test_bot_removals_still_require_existing_wording(self):
        candidate = proposal(["inline:1"])
        candidate["integration"]["operation"] = "remove"
        del candidate["integration"]["existing_quote"]
        events = {"inline:1": event("inline:1", 1, role="bot", author="github-actions[bot]")}
        with self.assertRaises(ValueError):
            core.validated_pattern(candidate, events, {}, self.repo, lane="github-actions")

    def test_skill_files_are_included_in_guidance_scope(self):
        skill = self.repo / ".agents" / "skills" / "fixture"
        skill.mkdir(parents=True)
        (skill / "SKILL.md").write_text("Use named cases.")
        inventory = core.guidance_inventory(self.repo)
        self.assertEqual(
            {source["path"] for source in inventory},
            {"AGENTS.md", ".agents/skills/fixture/SKILL.md"},
        )

    def test_cross_pr_support_is_recurring_but_opposition_requires_review(self):
        events = {
            key: event(key, pr, author=f"reviewer{pr}")
            for pr, key in enumerate(("inline:1", "inline:2", "inline:3"), 1)
        }
        candidate = proposal(["inline:1", "inline:2"])
        result = self.validate(candidate, events)
        self.assertTrue(result["support"]["recurring"])
        self.assertEqual(result["support"]["breadth"], "multiple-reviewers")
        self.assertEqual(result["recommendation_state"], "idea")
        candidate["evidence"].append(
            {"event_key": "inline:3", "quote": "Reuse the existing type.", "stance": "oppose"}
        )
        result = self.validate(candidate, events)
        self.assertEqual(result["recommendation_state"], "idea-with-disagreement")
        self.assertEqual(result["support"]["opposing_prs"], [3])

    def test_fabricated_quotes_ids_and_integration_anchors_are_rejected(self):
        events = {"inline:1": event("inline:1", 1)}
        original = proposal(["inline:1"])
        for field, bad in (("quote", "An invented quotation."), ("event_key", "inline:404")):
            candidate = copy.deepcopy(original)
            candidate["evidence"][0][field] = bad
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(candidate, events)
        for field, bad in (("anchor", "## Missing"), ("path", "../outside.md")):
            candidate = copy.deepcopy(original)
            candidate["integration"][field] = bad
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(candidate, events)

    def test_existing_guidance_is_kept_as_covered_even_when_recurring(self):
        events = {key: event(key, pr) for pr, key in enumerate(("inline:1", "inline:2"), 1)}
        candidate = proposal(list(events))
        candidate["integration"]["status"] = "already-covered"
        result = self.validate(candidate, events)
        self.assertTrue(result["support"]["recurring"])
        self.assertEqual(result["recommendation_state"], "idea")
        self.assertEqual(result["integration"]["status"], "already-covered")
        self.assertEqual(len(result["integration"]["source_sha256"]), 64)

    def test_every_idea_requires_a_concrete_target_and_code_scope(self):
        events = {"inline:1": event("inline:1", 1)}
        for field in ("path", "code_paths", "rationale"):
            candidate = proposal(["inline:1"])
            del candidate["integration"][field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(candidate, events)
        candidate = proposal(["inline:1"])
        del candidate["integration"]
        with self.assertRaises(ValueError):
            self.validate(candidate, events)

    def test_nested_agents_proposals_record_inheritance_without_creating_files(self):
        (self.repo / "src" / "io").mkdir()
        events = {"inline:1": event("inline:1", 1)}
        candidate = proposal(["inline:1"])
        candidate["integration"] = {
            "path": "src/io/AGENTS.md",
            "status": "new",
            "code_paths": ["src/io"],
            "rationale": "Keep IO-specific invariants local, inheriting repository-wide rules.",
        }
        result = self.validate(candidate, events)
        self.assertFalse(result["integration"]["target_exists"])
        self.assertEqual(result["integration"]["inherited_guidance"], ["AGENTS.md"])
        self.assertFalse((self.repo / "src/io/AGENTS.md").exists())
        candidate["integration"]["code_paths"] = ["src"]
        with self.assertRaises(ValueError):
            self.validate(candidate, events)

    def test_build_renders_targeted_ideas_without_final_wording(self):
        corpus, prepared, output = (self.repo / name for name in ("corpus", "prepared", "output"))
        corpus.mkdir()
        prepared.mkdir()
        (corpus / "events.jsonl").write_text(json.dumps(event("inline:1", 1)) + "\n")
        (corpus / "summary.json").write_text(
            json.dumps({"repo": "owner/repo", "since": "start", "until": "end"})
        )
        (prepared / "units.jsonl").write_text("")
        (prepared / "preparation.json").write_text(json.dumps({"lane": "users"}))
        candidate = proposal(["inline:1"])
        candidate["integration"] = {
            "path": "AGENTS.md",
            "status": "unassessed",
            "rationale": "Check overlapping types in src.",
            "code_paths": ["src"],
        }
        proposals = self.repo / "proposals.json"
        proposals.write_text(
            json.dumps({"lane": "users", "analysis_scope": "Fixture", "patterns": [candidate]})
        )
        result = core.synthesize(corpus, prepared, proposals, self.repo, output)
        self.assertEqual(result["integration_status"], {"unassessed": 1})
        report = (output / "recommendations.md").read_text()
        self.assertIn("Other targeted ideas for review", report)
        self.assertIn("Integration target: `AGENTS.md`", report)
        self.assertIn("Applicable code: `src`", report)
        self.assertIn("Reuse the existing type.", report)
        self.assertIn("State: idea.", report)

    def test_reports_group_by_major_category_before_integration_action(self):
        corpus, prepared, output = (self.repo / name for name in ("corpus", "prepared", "output"))
        corpus.mkdir()
        prepared.mkdir()
        (corpus / "events.jsonl").write_text(json.dumps(event("inline:1", 1)) + "\n")
        (corpus / "summary.json").write_text(
            json.dumps({"repo": "owner/repo", "since": "start", "until": "end"})
        )
        (prepared / "units.jsonl").write_text("")
        (prepared / "preparation.json").write_text(json.dumps({"lane": "users"}))
        candidates = []
        for group in reversed(core.MAJOR_CATEGORIES):
            p = proposal(["inline:1"])
            p.update(id=f"example-{group}", major_category=group, category="correctness")
            candidates.append(p)
        proposals = self.repo / "proposals.json"
        proposals.write_text(
            json.dumps({"lane": "users", "analysis_scope": "Fixture", "patterns": candidates})
        )
        result = core.synthesize(corpus, prepared, proposals, self.repo, output)
        self.assertEqual(result["groups"], {"tests": 1, "comments": 1, "design-structure": 1})
        report = (output / "recommendations.md").read_text()
        self.assertLess(report.index("## Tests"), report.index("## Comments"))
        self.assertLess(report.index("## Comments"), report.index("## Design/structure"))
        index = self.repo / "index.md"
        core.overview([output], index)
        text = index.read_text()
        self.assertIn("### users", text)
        self.assertIn("output/recommendations.md#example-tests-correctness", text)
        data = json.loads((output / "patterns.json").read_text())
        self.assertEqual(data["groups"]["tests"], ["example-tests"])
        for value in ("unknown", "testing"):
            p = proposal(["inline:1"])
            p["major_category"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.validate(p, {"inline:1": event("inline:1", 1)})


if __name__ == "__main__":
    unittest.main()
