"""Tests for SHA-bound AI review gate classification."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


SCRIPT = Path(__file__).parents[1] / "review_gate.py"
WORKFLOW = Path(__file__).parents[2] / "workflows" / "ai-review.yml"


def _load_module():
    spec = importlib.util.spec_from_file_location("review_gate", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReviewGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = _load_module()

    def test_review_with_blocker_heading_is_blocking(self) -> None:
        review = "## Blocking issues\n\n### Blocker1: broken invariant\nDetails."

        self.assertEqual(self.gate.review_state(review), "blockers")

    def test_bold_blocker_id_is_blocking(self) -> None:
        for review in (
            "**Blocker1** Broken invariant.",
            "  ### Blocker1: Broken invariant.",
            "- **Blocker1**: Broken invariant.",
        ):
            with self.subTest(review=review):
                self.assertEqual(self.gate.review_state(review), "blockers")

    def test_no_blocking_issues_is_clean(self) -> None:
        self.assertEqual(self.gate.review_state("No blocking issues."), "clean")

    def test_non_blocking_prose_that_mentions_blockers_is_clean(self) -> None:
        review = "## Non-blocking notes\nDo not add blockers for this style issue."

        self.assertEqual(self.gate.review_state(review), "clean")

    def test_prose_starting_with_blocker_words_is_clean(self) -> None:
        for review in (
            "Blocker1-Blocker4 from the previous review are fixed.",
            "blockers are resolved on the current SHA.",
        ):
            with self.subTest(review=review):
                self.assertEqual(self.gate.review_state(review), "clean")

    def test_machine_blocker_is_blocking(self) -> None:
        findings = [{"id": "Blocker1", "body": "Failure."}]

        self.assertEqual(self.gate.review_state("Summary.", findings), "blockers")

    def test_non_list_finding_metadata_is_ignored(self) -> None:
        self.assertEqual(
            self.gate.review_state("No blocking issues.", {"id": "Blocker1"}),
            "clean",
        )

    def test_malformed_blocker_formats_are_unavailable(self) -> None:
        for review, findings in (
            ("### Blocker 1\nFailure.", []),
            ("#### Blocker1\nFailure.", []),
            ("## Blocking issues\nFailure without an ID.", []),
            ("## Blockers\nFailure without an ID.", []),
            ("### Blocking issues: failure without an ID.", []),
            ("**Blocking issues**: failure without an ID.", []),
            ("1. **Blocking issues** -- failure without an ID.", []),
            ("Summary.", [{"id": "Blocker-1"}]),
        ):
            with self.subTest(review=review, findings=findings):
                self.assertEqual(
                    self.gate.review_state(review, findings), "unavailable"
                )

    def test_latest_gate_state_uses_newest_trusted_check(self) -> None:
        document = {
            "check_runs": [
                self._check(10, "blockers", override="active"),
                self._check(12, "clean"),
                self._check(20, "blockers", app="untrusted-app"),
            ]
        }

        self.assertEqual(
            self.gate.latest_gate_record(document),
            self.gate.GateRecord("clean", "inactive"),
        )

    def test_latest_gate_state_rejects_wrong_name_and_missing_marker(self) -> None:
        wrong_name = self._check(10, "blockers")
        wrong_name["name"] = "AI Review Summary"
        no_marker = self._check(11, "clean")
        no_marker["output"]["summary"] = "No machine-readable state."

        self.assertIsNone(
            self.gate.latest_gate_record({"check_runs": [wrong_name, no_marker]})
        )

    def test_recovery_ignores_newer_markerless_in_progress_check(self) -> None:
        in_progress = self._check(20, "clean")
        in_progress["output"]["summary"] = "Review running."

        self.assertEqual(
            self.gate.latest_gate_record(
                {"check_runs": [self._check(10, "blockers"), in_progress]}
            ),
            self.gate.GateRecord("blockers", "inactive"),
        )

    def test_failed_markerless_placeholder_preserves_previous_gate(self) -> None:
        failed = self._check(20, "unavailable")
        failed["output"]["summary"] = "AI review gate publication failed."

        self.assertEqual(
            self.gate.latest_gate_record(
                {"check_runs": [self._check(10, "blockers"), failed]}
            ),
            self.gate.GateRecord("blockers", "inactive"),
        )

    def test_override_request_survives_replaced_publisher(self) -> None:
        document = {
            "check_runs": [
                self._check(10, "clean"),
                self._override_request(20, "active", event_id=30),
                self._override_request(30, "inactive", event_id=20),
            ]
        }
        previous = self.gate.latest_gate_record(document)
        assert previous is not None
        override = self.gate.latest_override_request(document)

        self.assertEqual(override, "active")
        self.assertEqual(
            self.gate.GateRecord(previous.state, override),
            self.gate.GateRecord("clean", "active"),
        )

    def test_stale_clean_evidence_does_not_replace_newer_unavailable_gate(self) -> None:
        gates = {"check_runs": [self._check(20, "unavailable")]}
        evidence = {"check_runs": [self._evidence(10, "clean")]}

        self.assertEqual(
            self.gate.recover_record(gates, evidence),
            self.gate.GateRecord("unavailable", "inactive"),
        )

    def test_unresolved_review_request_fails_closed_when_evidence_is_missing(self) -> None:
        request = self._check(20, "unavailable")
        request["output"]["summary"] = (
            "<!-- ai-review-request:state=pending -->\nReview running."
        )
        gates = {"check_runs": [self._check(10, "clean"), request]}

        self.assertEqual(
            self.gate.recover_record(gates, {"check_runs": []}),
            self.gate.GateRecord("unavailable", "inactive"),
        )

    def test_newer_clean_evidence_resolves_pending_review_request(self) -> None:
        request = self._check(20, "unavailable")
        request["output"]["summary"] = (
            "<!-- ai-review-request:state=pending -->\nReview running."
        )
        gates = {"check_runs": [self._check(10, "unavailable"), request]}
        evidence = {"check_runs": [self._evidence(30, "clean", request_id=20)]}

        self.assertEqual(
            self.gate.recover_record(gates, evidence),
            self.gate.GateRecord("clean", "inactive"),
        )

    def test_other_review_records_do_not_resolve_latest_request(self) -> None:
        request = self._check(20, "unavailable")
        request["output"]["summary"] = (
            "<!-- ai-review-request:state=pending -->\nReview running."
        )
        gates = {
            "check_runs": [
                self._check(10, "clean", request_id=10),
                request,
                self._check(31, "clean", request_id=10),
            ]
        }
        evidence = {"check_runs": [self._evidence(30, "clean", request_id=10)]}

        self.assertEqual(
            self.gate.recover_record(gates, evidence),
            self.gate.GateRecord("unavailable", "inactive"),
        )

        evidence["check_runs"].append(self._evidence(32, "clean", request_id=20))
        self.assertEqual(
            self.gate.recover_record(gates, evidence),
            self.gate.GateRecord("clean", "inactive"),
        )

    def test_recovery_requires_every_pending_request_to_be_answered(self) -> None:
        first = self._pending_request(20)
        second = self._pending_request(30)
        gates = {"check_runs": [self._check(10, "clean"), first, second]}
        evidence = {"check_runs": [self._evidence(40, "clean", request_id=30)]}

        self.assertEqual(
            self.gate.unanswered_review_requests(gates, evidence),
            frozenset({20}),
        )
        self.assertEqual(
            self.gate.recover_record(gates, evidence),
            self.gate.GateRecord("unavailable", "inactive"),
        )

    def test_failed_request_is_terminal_and_preserves_only_blockers(self) -> None:
        later_gate = self._check(30, "clean", request_id=20)
        for failed_state, expected in (
            ("clean", "clean"),
            ("unavailable", "clean"),
            ("blockers", "blockers"),
        ):
            with self.subTest(failed_state=failed_state):
                gates = {
                    "check_runs": [
                        self._failed_request(10, failed_state),
                        self._pending_request(20),
                        later_gate,
                    ]
                }
                self.assertEqual(
                    self.gate.recover_record(gates, {"check_runs": []}),
                    self.gate.GateRecord(expected, "inactive"),
                )

        self.assertEqual(
            self.gate.recover_record(
                {
                    "check_runs": [
                        self._check(5, "clean"),
                        self._failed_request(10, "clean"),
                    ]
                }
            ),
            self.gate.GateRecord("unavailable", "inactive"),
        )

    def test_unavailable_gate_resolves_its_own_pending_request(self) -> None:
        request = self._check(20, "unavailable")
        request["output"]["summary"] = (
            "<!-- ai-review-request:state=pending -->\nReview running."
        )
        gates = {
            "check_runs": [
                self._check(10, "clean", request_id=10),
                request,
                self._check(31, "unavailable", request_id=20),
            ]
        }
        evidence = {"check_runs": [self._evidence(30, "clean", request_id=10)]}

        self.assertEqual(
            self.gate.recover_record(gates, evidence),
            self.gate.GateRecord("unavailable", "inactive"),
        )

    def test_review_evidence_preserves_blockers_across_replaced_gate_job(self) -> None:
        evidence = {
            "check_runs": [self._evidence(10, "blockers"), self._evidence(20, "clean")]
        }
        previous = self.gate.GateRecord("clean", "inactive")

        self.assertEqual(self.gate.review_evidence_state(evidence), "blockers")
        self.assertEqual(
            self.gate.reconcile_evidence(previous, "blockers"),
            self.gate.GateRecord("blockers", "inactive"),
        )

    def test_review_evidence_rejects_untrusted_and_markerless_checks(self) -> None:
        evidence = {
            "check_runs": [
                self._evidence(10, "blockers", app="untrusted"),
                self._evidence(20, "clean"),
                self._evidence(30, "blockers"),
            ]
        }
        evidence["check_runs"][2]["output"]["summary"] = "No marker."

        self.assertEqual(self.gate.review_evidence_state(evidence), "clean")
        self.assertEqual(
            self.gate.reconcile_evidence(
                self.gate.GateRecord("unavailable", "inactive"),
                "clean",
                gate_id=10,
                evidence_id=20,
            ),
            self.gate.GateRecord("clean", "inactive"),
        )

    def test_latest_label_actor_uses_complete_pages_and_highest_event_id(self) -> None:
        events = [
            [self._label_event(30, "newer"), self._label_event(10, "older")],
            [self._label_event(20, "middle", label="other")],
        ]

        self.assertEqual(self.gate.latest_label_actor(events, "ai-review"), "newer")
        self.assertIsNone(self.gate.latest_label_actor([events[0], {}], "ai-review"))

    def test_latest_label_actor_ignores_malformed_events(self) -> None:
        no_id = self._label_event(10, "actor")
        no_id.pop("id")
        no_login = self._label_event(20, "actor")
        no_login["actor"] = {}

        self.assertIsNone(
            self.gate.latest_label_actor([[no_id, no_login]], "ai-review")
        )
        self.assertIsNone(
            self.gate.latest_label_actor(
                [[self._label_event(30, "actor", label="other")]], "ai-review"
            )
        )

    def test_latest_label_event_supports_removal(self) -> None:
        removed = self._label_event(
            40, "maintainer", label="ai-review-bypass", event="unlabeled"
        )

        self.assertEqual(
            self.gate.latest_label_event(
                [[self._label_event(20, "maintainer"), removed]],
                "ai-review-bypass",
                "unlabeled",
            ),
            (40, "maintainer"),
        )

    def test_gate_resolution_matrix(self) -> None:
        blockers = self.gate.GateRecord("blockers", "inactive")
        bypassed = self.gate.GateRecord("blockers", "active")

        cases = (
            ("review", blockers, "clean", True, True, blockers),
            ("review", bypassed, "blockers", True, True, bypassed),
            (
                "review",
                self.gate.GateRecord("unavailable", "inactive"),
                "clean",
                True,
                True,
                self.gate.GateRecord("clean", "inactive"),
            ),
            (
                "review",
                blockers,
                "clean",
                False,
                True,
                blockers,
            ),
            (
                "review",
                self.gate.GateRecord("clean", "inactive"),
                "clean",
                True,
                False,
                self.gate.GateRecord("unavailable", "inactive"),
            ),
            (
                "bypass",
                bypassed,
                "unavailable",
                False,
                False,
                bypassed,
            ),
            ("restore", blockers, "unavailable", False, False, blockers),
        )
        for action, previous, fresh, succeeded, matches, expected in cases:
            with self.subTest(action=action, previous=previous, fresh=fresh):
                self.assertEqual(
                    self.gate.resolve_gate_record(
                        action,
                        previous,
                        fresh_state=fresh,
                        review_succeeded=succeeded,
                        head_matches=matches,
                    ),
                    expected,
                )

    def test_blockers_remain_sticky_across_failed_and_clean_reruns(self) -> None:
        blockers = self.gate.GateRecord("blockers", "inactive")

        failed = self.gate.resolve_gate_record(
            "review",
            blockers,
            fresh_state="unavailable",
            review_succeeded=False,
            head_matches=True,
        )
        clean = self.gate.resolve_gate_record(
            "review",
            failed,
            fresh_state="clean",
            review_succeeded=True,
            head_matches=True,
        )

        self.assertEqual(failed, blockers)
        self.assertEqual(clean, blockers)

    def test_bypass_does_not_carry_to_a_new_sha(self) -> None:
        new_sha = self.gate.GateRecord("unavailable", "inactive")

        reviewed = self.gate.resolve_gate_record(
            "review",
            new_sha,
            fresh_state="blockers",
            review_succeeded=True,
            head_matches=True,
        )

        self.assertEqual(reviewed, self.gate.GateRecord("blockers", "inactive"))

    def test_failed_review_without_prior_blockers_is_unavailable(self) -> None:
        for previous in (
            self.gate.GateRecord("clean", "inactive"),
            self.gate.GateRecord("unavailable", "inactive"),
        ):
            with self.subTest(previous=previous):
                self.assertEqual(
                    self.gate.resolve_gate_record(
                        "review",
                        previous,
                        fresh_state="clean",
                        review_succeeded=False,
                        head_matches=True,
                    ),
                    self.gate.GateRecord("unavailable", "inactive"),
                )

        with self.assertRaisesRegex(ValueError, "unknown gate action"):
            self.gate.resolve_gate_record(
                "unknown", self.gate.GateRecord("clean", "inactive")
            )

    def test_other_pending_request_overrides_a_fresh_clean_result(self) -> None:
        self.assertEqual(
            self.gate.resolve_gate_record(
                "review",
                self.gate.GateRecord("clean", "inactive"),
                fresh_state="clean",
                review_succeeded=True,
                head_matches=True,
                other_request_pending=True,
            ),
            self.gate.GateRecord("unavailable", "inactive"),
        )

    def test_matching_blocker_survives_later_review_job_failure(self) -> None:
        self.assertEqual(
            self.gate.resolve_gate_record(
                "review",
                self.gate.GateRecord("clean", "inactive"),
                fresh_state="blockers",
                review_succeeded=False,
                head_matches=True,
            ),
            self.gate.GateRecord("blockers", "inactive"),
        )
        self.assertEqual(
            self.gate.resolve_gate_record(
                "review",
                self.gate.GateRecord("unavailable", "inactive"),
                fresh_state="blockers",
                review_succeeded=False,
                head_matches=True,
                other_request_pending=True,
            ),
            self.gate.GateRecord("blockers", "inactive"),
        )

    def test_lifecycle_stays_unavailable_while_another_request_is_pending(self) -> None:
        current_request = self._pending_request(20)
        other_request = self._pending_request(30)
        gates = {
            "check_runs": [self._check(10, "clean"), current_request, other_request]
        }
        evidence = {"check_runs": []}
        previous = self.gate.recover_record(gates, evidence)
        other_pending = self.gate.unanswered_review_requests(
            gates,
            evidence,
            ignored_answer_request_ids=frozenset({20}),
        ) - {20}

        resolved = self.gate.resolve_gate_record(
            "review",
            previous,
            fresh_state="clean",
            review_succeeded=True,
            head_matches=True,
            other_request_pending=bool(other_pending),
        )

        self.assertEqual(resolved, self.gate.GateRecord("unavailable", "inactive"))
        self.assertEqual(
            self.gate.publication(*resolved).conclusion,
            "failure",
        )

    def test_publication_matrix(self) -> None:
        cases = (
            ("clean", "inactive", "success"),
            ("clean", "active", "success"),
            ("blockers", "inactive", "failure"),
            ("blockers", "active", "success"),
            ("unavailable", "inactive", "failure"),
            ("unavailable", "active", "failure"),
        )
        for state, override, expected in cases:
            with self.subTest(state=state, override=override):
                self.assertEqual(
                    self.gate.publication(state, override).conclusion, expected
                )

    def test_cli_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            review = root / "review.md"
            findings = root / "findings.json"
            checks = root / "checks.json"
            empty_checks = root / "empty-checks.json"
            evidence = root / "evidence.json"
            events = root / "events.json"
            pending_checks = root / "pending-checks.json"
            pending_evidence = root / "pending-evidence.json"
            review.write_text("### Blocker1\nFailure.")
            findings.write_text("[]")
            checks.write_text(
                json.dumps(
                    {
                        "check_runs": [
                            self._check(10, "blockers"),
                            self._override_request(20, "active"),
                        ]
                    }
                )
            )
            empty_checks.write_text(json.dumps({"check_runs": []}))
            evidence.write_text(
                json.dumps({"check_runs": [self._evidence(20, "blockers")]})
            )
            pending_checks.write_text(
                json.dumps(
                    {
                        "check_runs": [
                            self._pending_request(20),
                            self._pending_request(30),
                        ]
                    }
                )
            )
            pending_evidence.write_text(
                json.dumps(
                    {
                        "check_runs": [
                            self._evidence(40, "clean", request_id=20),
                            self._evidence(50, "blockers", request_id=30),
                        ]
                    }
                )
            )
            events.write_text(
                json.dumps(
                    [
                        [
                            self._label_event(30, "maintainer"),
                            self._label_event(
                                40,
                                "maintainer",
                                label="ai-review-bypass",
                                event="unlabeled",
                            ),
                        ]
                    ]
                )
            )

            self.assertEqual(
                self._run_cli("classify", review, "--findings", findings), "blockers"
            )
            self.assertEqual(self._run_cli("recover", checks), "blockers active")
            self.assertEqual(
                self._run_cli("recover", empty_checks), "unavailable inactive"
            )
            self.assertEqual(
                self._run_cli("recover", empty_checks, "--evidence", evidence),
                "blockers inactive",
            )
            self.assertEqual(
                self._run_cli(
                    "pending",
                    pending_checks,
                    "--evidence",
                    pending_evidence,
                    "--ignore-answer-for",
                    "20",
                ),
                "true",
            )
            self.assertEqual(
                self._run_cli(
                    "pending",
                    pending_checks,
                    "--evidence",
                    pending_evidence,
                    "--ignore-answer-for",
                    "20",
                    "--answering-request",
                    "20",
                ),
                "false",
            )
            self.assertEqual(
                self._run_cli(
                    "resolve",
                    "--action",
                    "review",
                    "--previous-state",
                    "clean",
                    "--previous-override",
                    "inactive",
                    "--fresh-state",
                    "clean",
                    "--review-succeeded",
                    "--head-matches",
                ),
                "clean inactive",
            )
            publication = json.loads(
                self._run_cli(
                    "publish", "--state", "blockers", "--override", "inactive"
                )
            )
            self.assertEqual(publication["conclusion"], "failure")
            self.assertEqual(
                self._run_cli("label-actor", events, "ai-review"), "maintainer"
            )
            self.assertEqual(
                self._run_cli(
                    "label-event", events, "ai-review-bypass", "unlabeled"
                ),
                "40 maintainer",
            )

    def test_workflow_contract_uses_labels_and_exact_sha_gate(self) -> None:
        workflow = WORKFLOW.read_text()
        document = yaml.load(workflow, Loader=yaml.BaseLoader)
        triggers = document["on"]
        jobs = document["jobs"]
        authorize = jobs["authorize"]
        review = jobs["review"]
        gate = jobs["gate"]
        cancelled_gate = jobs["finalize-cancelled-gate"]

        self.assertEqual(
            triggers["pull_request_target"]["types"],
            ["opened", "reopened", "ready_for_review", "labeled", "unlabeled"],
        )
        self.assertNotIn("synchronize", triggers["pull_request_target"]["types"])
        self.assertNotIn("concurrency", document)
        self.assertEqual(authorize["permissions"]["checks"], "write")
        self.assertEqual(
            authorize["outputs"]["gate_check_id"],
            "${{ steps.gate-start.outputs.check_id }}",
        )
        self.assertEqual(
            authorize["outputs"]["gate_marker"],
            "${{ steps.gate-start.outputs.marker }}",
        )
        self.assertIn("publish_gate", review["concurrency"]["group"])
        self.assertEqual(review["concurrency"]["cancel-in-progress"], "false")
        self.assertIn("head_sha", gate["concurrency"]["group"])
        self.assertEqual(gate["concurrency"]["cancel-in-progress"], "false")
        self.assertIn("always()", gate["if"])
        self.assertIn("needs.gate.result == 'cancelled'", cancelled_gate["if"])

        authorize_steps = {step["name"]: step for step in authorize["steps"]}
        review_steps = {step["name"]: step for step in review["steps"]}
        gate_steps = {step["name"]: step for step in gate["steps"]}
        trigger_step = authorize_steps["Resolve trigger"]
        self.assertIn("Mark exact-SHA gate in progress", authorize_steps)
        self.assertIn("Check out trusted gate policy", authorize_steps)
        self.assertNotIn("Check out trusted label policy", authorize_steps)
        evidence_step = review_steps["Persist exact-SHA review evidence"]
        self.assertEqual(evidence_step["continue-on-error"], "true")
        self.assertIn("clean|unavailable)", evidence_step["run"])
        self.assertIn(";request=${REQUEST_ID}", evidence_step["run"])
        self.assertIn("Publish exact-SHA gate", gate_steps)
        self.assertIn("Finalize gate placeholder", gate_steps)
        self.assertNotIn("Remove completed review trigger label", gate_steps)
        publish_step = gate_steps["Publish exact-SHA gate"]
        self.assertIn("needs.review.result != 'cancelled'", publish_step["if"])
        self.assertIn('--method POST "repos/${REPO}/check-runs"', publish_step["run"])
        placeholder_step = gate_steps["Finalize gate placeholder"]
        self.assertIn("--method PATCH", placeholder_step["run"])
        self.assertNotIn("ai-review-gate:state=", placeholder_step["run"])
        gate_start = authorize_steps["Mark exact-SHA gate in progress"]
        self.assertIn("ai-review-override:state=${override}", gate_start["run"])
        self.assertIn("opened:*|reopened:*|ready_for_review:*", workflow)
        self.assertNotIn("labeled:ai-review)", trigger_step["run"])
        self.assertNotIn("github.event.label.name == 'ai-review'", workflow)
        self.assertIn('if [ "$skip" != "true" ] && [ "$mode" = "inline" ]', trigger_step["run"])
        self.assertIn('gh api "repos/${REPO}/pulls/${n}"', trigger_step["run"])
        self.assertIn("publish_gate=true", trigger_step["run"])
        self.assertIn("labeled:ai-review-bypass", workflow)
        self.assertIn("unlabeled:ai-review-bypass", workflow)
        self.assertNotIn("consume-review", workflow)
        self.assertIn('name: "AI Review Gate"', workflow)
        self.assertIn("commits/${head_sha}/check-runs", workflow)
        self.assertIn("check_name=AI%20Review%20Gate&filter=all", workflow)
        self.assertIn("check_name=AI%20Review%20Evidence&filter=all", workflow)
        resolve_step = gate_steps["Resolve exact-SHA review state"]
        self.assertIn("gh api --paginate --slurp", resolve_step["run"])
        self.assertIn('--ignore-answer-for "$GATE_CHECK_ID"', resolve_step["run"])
        self.assertIn("--other-request-pending", resolve_step["run"])
        self.assertIn('any(.[]; .name == "ai-review-bypass")', workflow)
        self.assertIn("collaborators/${bypass_actor}/permission", workflow)
        self.assertIn(
            "<!-- ai-review-gate:state=%s;override=%s%s -->", publish_step["run"]
        )
        self.assertIn(
            '"$STATE" "$override" "$request_marker" "$message" "$RUN_URL"',
            publish_step["run"],
        )
        self.assertIn("review_gate.py publish", workflow)
        self.assertIn("PR_AUTHOR: ${{ github.event.pull_request.user.login }}", workflow)
        self.assertIn("ai-review-request:state=answered", placeholder_step["run"])
        self.assertIn("ai-review-request:state=failed;fresh=", placeholder_step["run"])
        self.assertIn("ai-review-request:state=superseded", placeholder_step["run"])

    def test_stale_label_event_does_not_publish_a_gate(self) -> None:
        script = self._workflow_step("authorize", "Resolve trigger")["run"]
        old_sha = "a" * 40
        new_sha = "b" * 40

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            self._write_fake_gh(root, f'printf "%s\\n" "{new_sha}"')
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTOR="maintainer",
                    COMMENT_BODY="",
                    DISPATCH_MODE="",
                    DISPATCH_PR="",
                    EVENT_ACTION="labeled",
                    EVENT_NAME="pull_request_target",
                    GITHUB_OUTPUT=str(output),
                    HEAD_SHA=old_sha,
                    ISSUE_NUMBER="",
                    LABEL_NAME="ai-review-bypass",
                    PR_AUTHOR="author",
                    PR_NUMBER="1",
                    REPO="delta-io/delta-kernel-rs",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            outputs = output.read_text().splitlines()
            self.assertIn("skip=true", outputs)
            self.assertIn("publish_gate=false", outputs)

    def test_label_history_failure_creates_a_markerless_placeholder(self) -> None:
        script = self._workflow_step(
            "authorize", "Mark exact-SHA gate in progress"
        )["run"]
        fake_gh = """
        if [[ "$*" == *"--paginate --slurp"* ]]; then
          exit 1
        fi
        if [[ "$*" == *"--method POST"* && "$*" == *"/check-runs"* ]]; then
          printf '%s\\n' '{"id":123}'
          exit 0
        fi
        exit 2
        """

        for action in ("bypass", "restore"):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = root / "output"
                self._write_fake_gh(root, fake_gh)
                result = subprocess.run(
                    ["bash", "-c", script],
                    cwd=WORKFLOW.parents[2],
                    env=self._script_env(
                        root,
                        ACTION=action,
                        GH_TOKEN="token",
                        GITHUB_OUTPUT=str(output),
                        HEAD_SHA="a" * 40,
                        PR_NUMBER="1",
                        REMOVE_LABEL="false",
                        REPO="delta-io/delta-kernel-rs",
                        RUN_URL="https://github.example/run/1",
                    ),
                    capture_output=True,
                    text=True,
                )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    output.read_text().splitlines(), ["check_id=123", "marker="]
                )

    def test_restore_continues_when_initial_head_lookup_fails(self) -> None:
        script = self._workflow_step("authorize", "Resolve trigger")["run"]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            self._write_fake_gh(root, "exit 1")
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTOR="maintainer",
                    COMMENT_BODY="",
                    DISPATCH_MODE="",
                    DISPATCH_PR="",
                    EVENT_ACTION="unlabeled",
                    EVENT_NAME="pull_request_target",
                    GITHUB_OUTPUT=str(output),
                    HEAD_SHA="a" * 40,
                    ISSUE_NUMBER="",
                    LABEL_NAME="ai-review-bypass",
                    PR_AUTHOR="author",
                    PR_NUMBER="1",
                    REPO="delta-io/delta-kernel-rs",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            outputs = output.read_text().splitlines()
            self.assertIn("action=restore", outputs)
            self.assertIn("skip=false", outputs)
            self.assertIn("publish_gate=true", outputs)

    def test_head_change_during_label_lookup_discards_override(self) -> None:
        script = self._workflow_step(
            "authorize", "Mark exact-SHA gate in progress"
        )["run"]
        fake_gh = """
        if [[ "$*" == *"--paginate --slurp"* ]]; then
          printf '%s\n' \
            '[[{"id":10,"event":"labeled","label":{"name":"ai-review-bypass"},'\
'"actor":{"login":"maintainer"}}]]'
          exit 0
        fi
        if [[ "$*" == *"/pulls/1"* ]]; then
          printf '%s\n' 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
          exit 0
        fi
        if [[ "$*" == *"--method POST"* && "$*" == *"/check-runs"* ]]; then
          printf '%s\n' '{"id":123}'
          exit 0
        fi
        exit 2
        """

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            self._write_fake_gh(root, fake_gh)
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="bypass",
                    GH_TOKEN="token",
                    GITHUB_OUTPUT=str(output),
                    HEAD_SHA="a" * 40,
                    PR_NUMBER="1",
                    REMOVE_LABEL="false",
                    REPO="delta-io/delta-kernel-rs",
                    RUN_URL="https://github.example/run/1",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                output.read_text().splitlines(), ["check_id=123", "marker="]
            )

    def test_markerless_bypass_cannot_activate_override(self) -> None:
        script = self._workflow_step("gate", "Publish exact-SHA gate")["run"]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / "calls"
            self._write_fake_gh(
                root,
                f"printf '%s\\n' \"$*\" >> {calls}\nexit 0",
            )
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="bypass",
                    GATE_CHECK_ID="123",
                    GATE_MARKER="",
                    GH_TOKEN="token",
                    HEAD_SHA="a" * 40,
                    PR_NUMBER="1",
                    REMOVE_LABEL="false",
                    REPO="delta-io/delta-kernel-rs",
                    RESOLVED_OVERRIDE="active",
                    RUN_URL="https://github.example/run/1",
                    STATE="blockers",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("--method DELETE", calls.read_text())
            publication = json.loads(Path("/tmp/gate-check.json").read_text())
            self.assertEqual(publication["conclusion"], "failure")
            self.assertIn("override=inactive", publication["output"]["summary"])

    def test_serialized_restore_removes_only_unauthorized_live_label(self) -> None:
        script = self._workflow_step("gate", "Publish exact-SHA gate")["run"]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / "calls"
            fake_gh = f"""
            printf '%s\n' "$*" >> {calls}
            if [[ "$*" == *"/labels"* && "$*" != *"--method DELETE"* ]]; then
              printf '%s\n' true
              exit 0
            fi
            if [[ "$*" == *"--paginate --slurp"* ]]; then
              printf '%s\n' \
                '[[{{"id":10,"event":"labeled",'\
'"label":{{"name":"ai-review-bypass"}},"actor":{{"login":"reader"}}}}]]'
              exit 0
            fi
            if [[ "$*" == *"/collaborators/reader/permission"* ]]; then
              printf '%s\n' read
              exit 0
            fi
            if [[ "$*" == *"--method DELETE"* || "$*" == *"--method POST"* ]]; then
              exit 0
            fi
            exit 2
            """
            self._write_fake_gh(root, fake_gh)
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="restore",
                    GATE_CHECK_ID="123",
                    GATE_MARKER="",
                    GH_TOKEN="token",
                    HEAD_SHA="a" * 40,
                    PR_NUMBER="1",
                    REMOVE_LABEL="true",
                    REPO="delta-io/delta-kernel-rs",
                    RESOLVED_OVERRIDE="active",
                    RUN_URL="https://github.example/run/1",
                    STATE="blockers",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("--method DELETE", calls.read_text())
            publication = json.loads(Path("/tmp/gate-check.json").read_text())
            self.assertEqual(publication["conclusion"], "failure")
            self.assertIn("state=blockers;override=inactive", publication["output"]["summary"])

    def test_restore_publishes_without_a_placeholder_id(self) -> None:
        gate_start = self._workflow_step(
            "authorize", "Mark exact-SHA gate in progress"
        )["run"]
        resolve = self._workflow_step("gate", "Resolve exact-SHA review state")["run"]
        publish = self._workflow_step("gate", "Publish exact-SHA gate")["run"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            fake_gh = """
            if [[ "$*" == *"--paginate --slurp"* ]]; then
              exit 1
            fi
            if [[ "$*" == *"/pulls/1"* ]]; then
              printf '%s\n' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
              exit 0
            fi
            if [[ "$*" == *"--method POST"* ]]; then
              exit 1
            fi
            exit 2
            """
            self._write_fake_gh(root, fake_gh)
            result = subprocess.run(
                ["bash", "-c", gate_start],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="restore",
                    GH_TOKEN="token",
                    GITHUB_OUTPUT=str(output),
                    HEAD_SHA="a" * 40,
                    PR_NUMBER="1",
                    REMOVE_LABEL="false",
                    REPO="delta-io/delta-kernel-rs",
                    RUN_URL="https://github.example/run/1",
                ),
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            self._write_fake_gh(
                root,
                "printf '%s\\n' '[{\"check_runs\":[]}]'",
            )
            result = subprocess.run(
                ["bash", "-c", resolve],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="restore",
                    EVENT_HEAD_SHA="a" * 40,
                    FRESH_HEAD_SHA="",
                    FRESH_STATE="",
                    GATE_CHECK_ID="",
                    GH_TOKEN="token",
                    GITHUB_OUTPUT=str(output),
                    REPO="delta-io/delta-kernel-rs",
                    REVIEW_RESULT="skipped",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("state=unavailable", output.read_text().splitlines())

        fake_gh = """
        if [[ "$*" == *"/labels"* ]]; then
          printf '%s\n' false
          exit 0
        fi
        if [[ "$*" == *"--method POST"* && "$*" == *"/check-runs"* ]]; then
          exit 0
        fi
        exit 2
        """

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_fake_gh(root, fake_gh)
            result = subprocess.run(
                ["bash", "-c", publish],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="restore",
                    GATE_CHECK_ID="",
                    GATE_MARKER="",
                    GH_TOKEN="token",
                    HEAD_SHA="a" * 40,
                    PR_NUMBER="1",
                    REMOVE_LABEL="false",
                    REPO="delta-io/delta-kernel-rs",
                    RESOLVED_OVERRIDE="active",
                    RUN_URL="https://github.example/run/1",
                    STATE="blockers",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            publication = json.loads(Path("/tmp/gate-check.json").read_text())
            self.assertEqual(publication["conclusion"], "failure")
            self.assertNotIn(";request=", publication["output"]["summary"])

    def test_failed_publication_does_not_copy_blockers_across_shas(self) -> None:
        script = self._workflow_step("gate", "Finalize gate placeholder")["run"]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_fake_gh(root, "exit 0")
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="review",
                    FRESH_HEAD_SHA="b" * 40,
                    FRESH_STATE="blockers",
                    GATE_CHECK_ID="123",
                    GATE_MARKER="<!-- ai-review-request:state=pending -->",
                    GH_TOKEN="token",
                    HEAD_SHA="a" * 40,
                    PUBLISH_OUTCOME="failure",
                    REPO="delta-io/delta-kernel-rs",
                    REVIEW_RESULT="failure",
                    RUN_URL="https://github.example/run/1",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            placeholder = json.loads(Path("/tmp/gate-placeholder.json").read_text())
            self.assertIn("state=failed;fresh=unavailable", placeholder["output"]["summary"])

    def test_cancelled_gate_finalizer_preserves_matching_blockers(self) -> None:
        script = self._workflow_step(
            "finalize-cancelled-gate", "Finalize cancelled gate placeholder"
        )["run"]
        fake_gh = """
        if [[ "$*" == *"--method PATCH"* ]]; then
          exit 0
        fi
        printf '%s\n' 0
        """

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_fake_gh(root, fake_gh)
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="review",
                    FRESH_HEAD_SHA="a" * 40,
                    FRESH_STATE="blockers",
                    GATE_CHECK_ID="123",
                    GH_TOKEN="token",
                    HEAD_SHA="a" * 40,
                    REPO="delta-io/delta-kernel-rs",
                    RUN_URL="https://github.example/run/1",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            placeholder = json.loads(
                Path("/tmp/cancelled-gate-placeholder.json").read_text()
            )
            self.assertEqual(placeholder["conclusion"], "failure")
            self.assertIn("state=failed;fresh=blockers", placeholder["output"]["summary"])

    def test_unauthorized_bypass_is_routed_to_serialized_restore(self) -> None:
        script = self._workflow_step(
            "authorize", "Check trigger subject has write access"
        )["run"]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"
            self._write_fake_gh(root, "printf '%s\\n' read")
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=WORKFLOW.parents[2],
                env=self._script_env(
                    root,
                    ACTION="bypass",
                    AUTH_SUBJECT="reader",
                    GH_TOKEN="token",
                    GITHUB_OUTPUT=str(output),
                    REPO="delta-io/delta-kernel-rs",
                ),
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                output.read_text().splitlines(),
                ["action=restore", "allowed=true", "remove_label=true"],
            )

    @staticmethod
    def _run_cli(*args: object) -> str:
        result = subprocess.run(
            [sys.executable, str(SCRIPT), *(str(arg) for arg in args)],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    @staticmethod
    def _workflow_step(job: str, name: str) -> dict:
        document = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        return next(step for step in document["jobs"][job]["steps"] if step["name"] == name)

    @staticmethod
    def _write_fake_gh(root: Path, body: str) -> None:
        executable = root / "gh"
        executable.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{body}\n")
        executable.chmod(0o755)
        (root / "python3").symlink_to(sys.executable)

    @staticmethod
    def _script_env(root: Path, **values: str) -> dict[str, str]:
        return {
            **os.environ,
            "PATH": f"{root}:{os.environ['PATH']}",
            **values,
        }

    @staticmethod
    def _check(
        check_id: int,
        state: str,
        app: str = "github-actions",
        override: str = "inactive",
        request_id: int | None = None,
    ) -> dict:
        request = f";request={request_id}" if request_id is not None else ""
        return {
            "id": check_id,
            "name": "AI Review Gate",
            "app": {"slug": app},
            "output": {
                "summary": (
                    f"<!-- ai-review-gate:state={state};override={override}{request} -->"
                    "\nResult."
                )
            },
        }

    @staticmethod
    def _evidence(
        check_id: int,
        state: str,
        app: str = "github-actions",
        request_id: int | None = None,
    ) -> dict:
        request = f";request={request_id}" if request_id is not None else ""
        return {
            "id": check_id,
            "name": "AI Review Evidence",
            "app": {"slug": app},
            "output": {
                "summary": (
                    f"<!-- ai-review-evidence:state={state}{request} -->\nResult."
                )
            },
        }

    @staticmethod
    def _pending_request(check_id: int) -> dict:
        check = ReviewGateTest._check(check_id, "unavailable")
        check["output"]["summary"] = (
            "<!-- ai-review-request:state=pending -->\nReview running."
        )
        return check

    @staticmethod
    def _failed_request(check_id: int, fresh_state: str) -> dict:
        check = ReviewGateTest._check(check_id, "unavailable")
        check["output"]["summary"] = (
            f"<!-- ai-review-request:state=failed;fresh={fresh_state} -->\n"
            "Review publication failed."
        )
        return check

    @staticmethod
    def _override_request(
        check_id: int, state: str, event_id: int | None = None
    ) -> dict:
        check = ReviewGateTest._check(check_id, "unavailable")
        event_id = check_id if event_id is None else event_id
        check["output"]["summary"] = (
            f"<!-- ai-review-override:state={state};event-id={event_id} -->\n"
            "Gate updating."
        )
        return check

    @staticmethod
    def _label_event(
        event_id: int,
        actor: str,
        label: str = "ai-review",
        event: str = "labeled",
    ) -> dict:
        return {
            "id": event_id,
            "event": event,
            "label": {"name": label},
            "actor": {"login": actor},
        }


if __name__ == "__main__":
    unittest.main()
