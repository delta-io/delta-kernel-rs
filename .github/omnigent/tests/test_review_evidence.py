"""Exercise runtime observation, candidate accounting and confirmed publication joins."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1]))
try:
    import review_evidence
    from review_evidence import (
        CHECKER_ROLE, accounting_prompt_instructions, build_evidence, execution_summary,
        extract_accounting, observe_review,
    )
finally:
    sys.path.pop(0)

RUN_ID = "123"
HEAD_SHA = "a" * 40
MARKER = "b" * 32
ROLES = [
    "delta-protocol-reviewer", "maintainer-claude-reviewer", "maintainer-codex-reviewer",
    "architecture-reviewer", "test-coverage-reviewer", "docs-reviewer",
]
ARCH, DOCS = "architecture-reviewer", "docs-reviewer"


def metadata(marker, key, entries):
    return (
        f"<!-- AI_REVIEW_{marker} -->" + json.dumps({key: entries})
        + f"<!-- AI_REVIEW_{marker}_END -->"
    )


def role_events(role, candidates=None, *, attempt=1, output=None, batch=None, status=None):
    title = f"{role}-{attempt}"
    child_id = f"child-{title}"
    assignment = metadata("CHECK_BATCH", "candidates", batch) if batch is not None else "Review."
    arguments = {
        "agent": role, "title": title, "args": {"input": assignment, "purpose": "review"},
    }
    if output is None:
        marker, key = ("DISPOSITIONS", "dispositions") if role == CHECKER_ROLE else (
            "CANDIDATES", "candidates",
        )
        output = metadata(marker, key, candidates or [])
    result = {"result": output}
    if status:
        result["status"] = status
    return [
        {"type": "tool_call", "data": {"name": "sys_session_send", "arguments": arguments}},
        {
            "type": "tool_result", "request_data": {
                "name": "sys_session_send", "arguments": arguments,
            },
            "data": {"result": json.dumps({
                "kind": "sub_agent", "agent": role, "title": title,
                "conversation_id": child_id, "status": "launching",
            })},
        },
        {
            "type": "tool_result", "request_data": {
                "name": "sys_session_send", "tool": "sys_session_send", "args": {
                    "agent": role, "title": title, "conversation_id": child_id,
                },
            }, "data": result,
        },
    ]


def empty_events(replacements=None):
    replacements = replacements or {}
    return [event for role in ROLES for event in replacements.get(role, role_events(role))]


def observe(events):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "observations.jsonl"
        policy = observe_review(
            path=str(path), run_id=RUN_ID, head_sha=HEAD_SHA, required_roles=ROLES,
        )
        for event in events:
            original = copy.deepcopy(event)
            assert policy(event) == {"result": "ALLOW"}
            assert event == original
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def publication(findings=None):
    return {
        "head_sha": HEAD_SHA,
        "run_url": f"https://github.com/example/project/actions/runs/{RUN_ID}",
        "complete": True, "findings": findings or [],
    }


def final(finding_id, severity, *, delivery="new", aliases=None):
    return {
        "id": finding_id, "severity": severity, "head_sha": HEAD_SHA,
        "url": f"https://github.com/example/project/pull/1#discussion-{finding_id}",
        "placement": "inline", "delivery": delivery, "aliases": aliases or [],
    }


def evidence(events=None, *, outcomes=None, published=None, history=None):
    return build_evidence(
        observe(empty_events() if events is None else events), {"outcomes": outcomes or []},
        publication() if published is None else published,
        {"complete": True, "reasons": [], "blockers": []} if history is None else history,
        run_id=RUN_ID, head_sha=HEAD_SHA, required_roles=ROLES,
    )


def candidate(candidate_id="C1", severity="Blocker"):
    return {"id": candidate_id, "severity": severity}


def outcome(role=ARCH, candidate_id="C1", state="retained", final_id="Blocker1", reason=""):
    return {
        "role": role, "id": candidate_id, "outcome": state,
        "final_id": final_id, "reason": reason,
    }


def checker_events(batch, verdicts=None):
    if verdicts is None:
        verdicts = [{"batch_id": entry["batch_id"], "verdict": "CONFIRMED"} for entry in batch]
    return role_events(CHECKER_ROLE, verdicts, batch=batch)


class ReviewEvidenceTest(unittest.TestCase):
    def test_all_primaries_return_explicit_empty_results_without_a_checker(self):
        record = evidence()

        self.assertTrue(record["complete"], record["reasons"])
        self.assertEqual(len(record["attempts"]), len(ROLES))
        self.assertEqual(record["candidates"], [])
        self.assertTrue(all(a["outcome"] == "returned" for a in record["attempts"]))
        self.assertTrue(all(a["started_at"] is None for a in record["attempts"]))
        self.assertTrue(all(a["ended_at"] is None for a in record["attempts"]))

    def test_dispatch_acceptance_and_process_exit_do_not_establish_a_return(self):
        events = empty_events({DOCS: role_events(DOCS)[:2]})
        events.append({"type": "response", "data": "Process exited 0; every reviewer finished."})
        record = evidence(events)

        self.assertFalse(record["complete"])
        attempt = next(a for a in record["attempts"] if a["role"] == DOCS)
        self.assertEqual(attempt["outcome"], "unobserved")
        self.assertIsNone(attempt["returned_at"])
        self.assertIn(DOCS, " ".join(record["reasons"]))

    def test_interleaved_returns_and_successful_retry_keep_distinct_attempts(self):
        groups = [role_events(role) for role in ROLES if role != DOCS]
        events = [event for group in groups for event in group[:2]]
        events += role_events(DOCS, status="failed")
        events += [group[2] for group in reversed(groups)]
        events += role_events(DOCS, attempt=2)
        record = evidence(events)

        self.assertTrue(record["complete"], record["reasons"])
        attempts = [a for a in record["attempts"] if a["role"] == DOCS]
        self.assertEqual([a["attempt"] for a in attempts], [1, 2])
        self.assertEqual([a["outcome"] for a in attempts], ["failed", "returned"])
        self.assertNotEqual(attempts[0]["session_id"], attempts[1]["session_id"])
        self.assertTrue(all(a["dispatched_at"] and a["returned_at"] for a in attempts))

    def test_later_empty_success_cannot_erase_an_earlier_blocker_candidate(self):
        events = empty_events({ARCH: role_events(ARCH, [candidate()])})
        events += role_events(ARCH, [], attempt=2)

        record = evidence(events)

        self.assertFalse(record["complete"])
        self.assertEqual(len(record["candidates"]), 1)
        retained = record["candidates"][0]
        self.assertEqual((retained["role"], retained["id"], retained["severity"]),
                         (ARCH, "C1", "Blocker"))
        self.assertEqual(retained["attempts"], [1])
        reasons = " ".join(record["reasons"])
        self.assertIn("conflicting candidate-finding lists", reasons)
        self.assertIn("checker", reasons)
        self.assertIn("has no consolidation outcome", reasons)

    def test_conflicting_retry_severities_preserve_the_strongest_candidate_and_partial_state(self):
        batch = [{"batch_id": "check1", "role": ARCH, "id": "C1"}]
        for severities in (("Blocker", "Nit"), ("Nit", "Blocker")):
            with self.subTest(severities=severities):
                events = empty_events({
                    ARCH: role_events(ARCH, [candidate(severity=severities[0])]),
                })
                events += role_events(ARCH, [candidate(severity=severities[1])], attempt=2)
                events += checker_events(batch)

                record = evidence(
                    events, outcomes=[outcome()],
                    published=publication([final("Blocker1", "Blocker")]),
                )

                self.assertFalse(record["complete"])
                self.assertEqual(len(record["candidates"]), 1)
                retained = record["candidates"][0]
                self.assertEqual(retained["severity"], "Blocker")
                self.assertEqual(retained["attempts"], [1, 2])
                self.assertEqual(retained["checker"]["verdict"], "CONFIRMED")
                self.assertEqual(retained["final_id"], "Blocker1")
                self.assertIn("conflicting severities", " ".join(record["reasons"]))

    def test_identical_successful_retries_share_one_accounted_candidate_with_provenance(self):
        batch = [{"batch_id": "check1", "role": ARCH, "id": "C1"}]
        events = empty_events({ARCH: role_events(ARCH, [candidate()])})
        events += role_events(ARCH, [candidate()], attempt=2)
        events += checker_events(batch)

        record = evidence(
            events, outcomes=[outcome()],
            published=publication([final("Blocker1", "Blocker")]),
        )

        self.assertTrue(record["complete"], record["reasons"])
        self.assertEqual(len(record["candidates"]), 1)
        self.assertEqual(record["candidates"][0]["attempts"], [1, 2])
        attempts = [attempt for attempt in record["attempts"] if attempt["role"] == ARCH]
        self.assertEqual(len(attempts), 2)
        self.assertNotEqual(attempts[0]["session_id"], attempts[1]["session_id"])

    def test_explicit_terminal_outcomes_remain_distinct_from_unobserved(self):
        for state in ("failed", "cancelled", "timed_out"):
            with self.subTest(state=state):
                record = evidence(empty_events({DOCS: role_events(DOCS, status=state)}))
                attempt = next(a for a in record["attempts"] if a["role"] == DOCS)
                self.assertEqual(attempt["outcome"], state)
                self.assertFalse(record["complete"])

    def test_successful_return_does_not_hide_another_unobserved_attempt(self):
        events = empty_events() + role_events(DOCS, attempt=2)[:2]
        record = evidence(events)

        self.assertFalse(record["complete"])
        self.assertIn("attempt 2 has no task result recorded", " ".join(record["reasons"]))

    def test_missing_or_malformed_candidate_metadata_leaves_completion_partial(self):
        invalid_outputs = (
            "No findings.", metadata("CANDIDATES", "candidates", {}),
            metadata("CANDIDATES", "candidates", [candidate(severity="High")]),
            metadata("CANDIDATES", "candidates", [candidate(), candidate()]),
        )
        for output in invalid_outputs:
            with self.subTest(output=output):
                record = evidence(empty_events({DOCS: role_events(DOCS, output=output)}))
                self.assertFalse(record["complete"])
                self.assertIn(DOCS, " ".join(record["reasons"]))

    def test_observer_discards_assignment_and_return_prose_without_changing_dispatch(self):
        events = role_events(
            DOCS, output="PRIVATE_RAW_OUTPUT\n" + metadata("CANDIDATES", "candidates", []),
        )
        events[0]["data"]["arguments"]["args"]["input"] = "PRIVATE_PR_DESCRIPTION"
        events[0]["context"] = {"environment": "PRIVATE_ENVIRONMENT"}
        observations = observe(events)

        serialized = json.dumps(observations)
        self.assertNotIn("PRIVATE_", serialized)
        self.assertEqual(
            [row["kind"] for row in observations], ["dispatch", "accepted", "returned"],
        )
        self.assertEqual(observations[-1]["candidates"], [])
        self.assertTrue(observations[-1]["metadata_valid"])

    def test_candidates_join_checker_outcomes_aliases_and_reused_destinations(self):
        arch = [candidate(), candidate("C2", "Should Fix")]
        docs = [candidate("C1", "Should Fix"), candidate("C2"), candidate("C3", "Nit"),
                candidate("C4", "Should Fix")]
        batch = [
            {"batch_id": f"check{i}", "role": role, "id": entry["id"]}
            for i, (role, entry) in enumerate(
                [(ARCH, c) for c in arch] + [(DOCS, c) for c in docs if c["severity"] != "Nit"], 1
            )
        ]
        verdicts = [
            {"batch_id": entry["batch_id"], "verdict": verdict}
            for entry, verdict in zip(batch, (
                "CONFIRMED", "DISPROVED", "NITPICK", "CONFIRMED", "CONFIRMED",
            ))
        ]
        events = empty_events({ARCH: role_events(ARCH, arch), DOCS: role_events(DOCS, docs)})
        events += checker_events(batch, verdicts)
        record = evidence(events, outcomes=[
            outcome(), outcome(ARCH, "C2", "dropped", None, "The premise was disproved."),
            outcome(DOCS, "C1", "downgraded", "Nit1", "Only a small correction is needed."),
            outcome(DOCS, "C2", "merged", "Blocker2", "Same defect as the architecture finding."),
            outcome(DOCS, "C3", final_id="Nit2"),
            outcome(DOCS, "C4", final_id="ShouldFix1"),
        ], published=publication([
            final("Blocker1", "Blocker", delivery="reused", aliases=["Blocker2"]),
            final("Nit1", "Nit"), final("Nit2", "Nit", delivery="reused"),
            final("ShouldFix1", "Should Fix"),
        ]))

        self.assertTrue(record["complete"], record["reasons"])
        candidates = {(c["role"], c["id"]): c for c in record["candidates"]}
        self.assertEqual(candidates[ARCH, "C1"]["delivery"], "reused")
        self.assertEqual(candidates[ARCH, "C2"]["outcome"], "dropped")
        self.assertEqual(candidates[DOCS, "C1"]["final_severity"], "Nit")
        self.assertEqual(candidates[DOCS, "C2"]["final_id"], "Blocker1")
        self.assertIsNone(candidates[DOCS, "C3"]["checker"])
        self.assertIn("The premise was disproved.", execution_summary(record))

    def test_blocker_requires_observed_checker_and_every_disposition(self):
        batch = [{"batch_id": "check1", "role": ARCH, "id": "C1"}]
        base = empty_events({ARCH: role_events(ARCH, [candidate()])})
        for extra in ([], checker_events(batch, [])):
            with self.subTest(checker_observed=bool(extra)):
                record = evidence(
                    base + extra, outcomes=[outcome()],
                    published=publication([final("Blocker1", "Blocker")]),
                )
                self.assertFalse(record["complete"])
                self.assertIn("checker", " ".join(record["reasons"]).lower())

    def test_contested_correctness_remains_partial_with_a_visible_known_blocker(self):
        batch = [{"batch_id": "check1", "role": ARCH, "id": "C1"}]
        events = empty_events({ARCH: role_events(ARCH, [candidate()])})
        events += checker_events(batch, [{"batch_id": "check1", "verdict": "CONTESTED"}])
        record = evidence(
            events, outcomes=[outcome()], published=publication([final("Blocker1", "Blocker")]),
        )

        self.assertFalse(record["complete"])
        self.assertIn("contested", " ".join(record["reasons"]))
        self.assertEqual(record["candidates"][0]["final_id"], "Blocker1")
        self.assertTrue(record["candidates"][0]["url"])

    def test_nits_require_accounting_and_dropped_candidates_require_a_reason(self):
        events = empty_events({ARCH: role_events(ARCH, [candidate(severity="Nit")])})
        for outcomes in ([], [outcome(state="dropped", final_id=None)]):
            with self.subTest(outcomes=outcomes):
                record = evidence(events, outcomes=outcomes)
                self.assertFalse(record["complete"])
                self.assertIn("Candidate", " ".join(record["reasons"]))

    def test_malformed_consolidation_outcome_leaves_a_record_instead_of_crashing(self):
        events = empty_events({ARCH: role_events(ARCH, [candidate(severity="Nit")])})
        record = evidence(events, outcomes=[outcome(state=["retained"], final_id=["Nit1"])])

        self.assertFalse(record["complete"])
        self.assertIn("invalid outcome", " ".join(record["reasons"]))

    def test_observation_size_limit_is_explicit_in_the_completion_record(self):
        with patch.object(review_evidence, "MAX_EVENTS_BYTES", 1):
            observations = observe(role_events(DOCS))
        record = build_evidence(
            observations, {"outcomes": []}, publication(), {"complete": True},
            run_id=RUN_ID, head_sha=HEAD_SHA, required_roles=ROLES,
        )

        self.assertFalse(record["complete"])
        self.assertIn("truncated", " ".join(record["reasons"]))

    def test_orphan_final_finding_and_failed_publication_leave_completion_partial(self):
        for published in (
            publication([final("Nit1", "Nit")]),
            {**publication(), "complete": False},
        ):
            with self.subTest(published=published):
                self.assertFalse(evidence(published=published)["complete"])

    def test_wrong_run_or_head_cannot_supply_execution_or_publication_evidence(self):
        for key, value in (("run_id", "other"), ("head_sha", "b" * 40)):
            with self.subTest(key=key):
                observations = observe(empty_events())
                observations[0][key] = value
                record = build_evidence(
                    observations, {"outcomes": []}, publication(), {"complete": True},
                    run_id=RUN_ID, head_sha=HEAD_SHA, required_roles=ROLES,
                )
                self.assertFalse(record["complete"])
        self.assertFalse(evidence(published={**publication(), "head_sha": "b" * 40})["complete"])
        self.assertFalse(evidence(published={**publication(), "run_url": "other"})["complete"])

    def test_history_failure_and_legacy_blocker_identity_survive_without_raw_body(self):
        history = {
            "complete": False, "reasons": ["An older review page was clipped."],
            "blockers": [{
                "id": "legacy-1", "head_sha": None, "body": "PRIVATE_RAW_BLOCKER_BODY",
                "url": "https://github.com/example/project/pull/1#issuecomment-1",
                "fingerprint": "c" * 64, "source_comment_id": 1,
            }],
        }
        record = evidence(history=history)

        self.assertFalse(record["complete"])
        self.assertIsNone(record["history"]["blockers"][0]["head_sha"])
        self.assertEqual(record["history"]["blockers"][0]["fingerprint"], "c" * 64)
        self.assertNotIn("PRIVATE_RAW_BLOCKER_BODY", json.dumps(record))
        self.assertIn("clipped", record["history"]["reasons"][0])

    def test_accounting_round_trip_and_missing_or_repeated_markers(self):
        contract = accounting_prompt_instructions(MARKER)
        block = (
            f"<!-- AI_REVIEW_ACCOUNTING_START_{MARKER} -->"
            + json.dumps({"outcomes": []}) + f"<!-- AI_REVIEW_ACCOUNTING_END_{MARKER} -->"
        )
        clean, document = extract_accounting("Review prose.\n" + block, MARKER)

        self.assertEqual(clean, "Review prose.")
        self.assertEqual(document, {"outcomes": []})
        self.assertIn(f"AI_REVIEW_ACCOUNTING_START_{MARKER}", contract)
        start = f"<!-- AI_REVIEW_ACCOUNTING_START_{MARKER} -->"
        end = f"<!-- AI_REVIEW_ACCOUNTING_END_{MARKER} -->"
        for invalid in (
            "Review prose.", block + block, block.replace(MARKER, "d" * 32),
            start + '{"outcomes":[]}', '{"outcomes":[]}' + end,
            end + '{"outcomes":[]}' + start,
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    extract_accounting(invalid, MARKER)

    def test_balanced_malformed_accounting_is_removed_without_publishing_its_payload(self):
        start = f"<!-- AI_REVIEW_ACCOUNTING_START_{MARKER} -->"
        end = f"<!-- AI_REVIEW_ACCOUNTING_END_{MARKER} -->"
        for payload in (
            "not-json", '{"unexpected":[]}', "null", "[]", '{"outcomes":{}}',
        ):
            with self.subTest(payload=payload):
                clean, document = extract_accounting(
                    f"Human review.\n{start}\n{payload}\n{end}\nHuman footer.", MARKER,
                )

                self.assertEqual(document, {})
                self.assertIn("Human review.", clean)
                self.assertIn("Human footer.", clean)
                self.assertNotIn("AI_REVIEW_ACCOUNTING", clean)
                self.assertNotIn(payload, clean)

    def test_cli_writes_partial_record_when_execution_files_are_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {name: root / name for name in (
                "observations", "accounting", "publication", "history", "required-roles",
                "output", "check-output",
            )}
            paths["required-roles"].write_text(json.dumps(ROLES))
            paths["check-output"].write_text("Existing full review.\n")
            args = ["review_evidence.py", "--run-id", RUN_ID, "--head-sha", HEAD_SHA]
            for name, path in paths.items():
                args.extend((f"--{name}", str(path)))

            with patch.object(sys, "argv", args):
                review_evidence.main()

            record = json.loads(paths["output"].read_text())
            self.assertFalse(record["complete"])
            self.assertEqual(record["head_sha"], HEAD_SHA)
            summary = paths["check-output"].read_text()
            self.assertTrue(summary.startswith("Existing full review."))
            self.assertIn("Review execution", summary)


if __name__ == "__main__":
    unittest.main()
