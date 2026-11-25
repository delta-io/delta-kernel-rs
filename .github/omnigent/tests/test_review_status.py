"""Commit-specific status precedence, retained findings and conservative Blocker history."""

from __future__ import annotations

import copy
import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))
try:
    from review_history import canonical_finding_body
    from review_status import compute_review_status
finally:
    sys.path.pop(0)

BASE, HEAD, SECOND, THIRD = (char * 40 for char in "abcd")
RUN_ID = "123"
RUN_URL = f"https://github.com/example/project/actions/runs/{RUN_ID}"
ROLES = [
    "delta-protocol-reviewer", "maintainer-claude-reviewer", "maintainer-codex-reviewer",
    "architecture-reviewer", "test-coverage-reviewer", "docs-reviewer",
]


def finding(finding_id="Blocker1", severity="Blocker", *, head_sha=HEAD,
            body="A retained defect.", url=None, delivery="new"):
    return {
        "id": finding_id, "severity": severity, "head_sha": head_sha, "body": body,
        "url": url or f"https://github.com/example/project/pull/1#discussion_{finding_id}",
        "placement": "inline", "delivery": delivery,
    }


def blocker(*, head_sha=HEAD, finding_id="Blocker1", body="A retained defect.", url=None):
    return {
        "id": finding_id, "head_sha": head_sha, "body": body,
        "fingerprint": hashlib.sha256(canonical_finding_body(body).encode()).hexdigest(),
        "url": url or f"https://github.com/example/project/pull/1#discussion_{finding_id}",
    }


def inputs(*, head_sha=HEAD, findings=None, blockers=None):
    findings = copy.deepcopy(findings or [])
    candidates = [{
        "role": "architecture-reviewer", "id": f"C{index}", "severity": item["severity"],
        "checker": {"decision": "CONFIRMED"} if item["severity"] != "Nit" else None,
        "outcome": "retained", "final_id": item["id"], "url": item["url"],
    } for index, item in enumerate(findings, 1)]
    attempts = [{
        "role": role, "outcome": "returned", "session_id": f"session-{role}",
        "metadata_valid": True,
        "candidates": [
            {"id": candidate["id"], "severity": candidate["severity"]}
            for candidate in candidates if candidate["role"] == role
        ],
    } for role in ROLES]
    return {
        "head_sha": head_sha, "run_id": RUN_ID,
        "coverage": {
            "head_sha": head_sha, "base_sha": BASE, "complete": True,
            "total_bytes": 100, "delivered_bytes": 100,
            "total_files": 1, "full_files": 1, "partial_files": 0, "omitted_files": 0,
            "files": [{"path": "README.md", "start": 0, "end": 100, "status": "full"}],
        },
        "completion": {
            "head_sha": head_sha, "run_id": RUN_ID, "complete": True, "reasons": [],
            "configured_reviewer_roles": ROLES, "attempts": attempts, "candidates": candidates,
        },
        "publication": {
            "head_sha": head_sha, "run_url": RUN_URL, "complete": True,
            "errors": [], "findings": findings,
        },
        "history": {"complete": True, "reasons": [], "blockers": copy.deepcopy(blockers or [])},
    }


class ReviewStatusTest(unittest.TestCase):
    def test_complete_empty_review_is_approved(self):
        result = compute_review_status(**inputs())
        self.assertEqual(result["status"], "APPROVED")
        self.assertTrue(result["status_requirements_met"])
        self.assertEqual(result["reasons"], [])
        self.assertEqual(result["counts"], {"Blocker": 0, "Should Fix": 0, "Nit": 0})

    def test_only_blockers_withhold_approval_when_all_evidence_is_complete(self):
        for finding_id, severity, status in (
            ("Blocker1", "Blocker", "NOT APPROVED"),
            ("ShouldFix1", "Should Fix", "APPROVED WITH FEEDBACK"),
            ("Nit1", "Nit", "APPROVED WITH FEEDBACK"),
        ):
            with self.subTest(severity=severity):
                result = compute_review_status(**inputs(findings=[finding(finding_id, severity)]))
                self.assertEqual(result["status"], status)
                self.assertEqual(result["counts"][severity], 1)

    def test_many_should_fix_findings_remain_advisory(self):
        findings = [finding(f"ShouldFix{i}", "Should Fix") for i in range(1, 9)]
        result = compute_review_status(**inputs(findings=findings))
        self.assertEqual(result["status"], "APPROVED WITH FEEDBACK")
        self.assertEqual(result["counts"]["Should Fix"], 8)

    def test_three_head_sequence_requires_rechecking_the_earlier_blocker(self):
        first = compute_review_status(**inputs(findings=[finding()]))
        previous = blocker()
        second_finding = finding(head_sha=SECOND, url=previous["url"], delivery="reused")
        second = compute_review_status(**inputs(
            head_sha=SECOND, findings=[second_finding], blockers=[previous],
        ))
        third = compute_review_status(**inputs(
            head_sha=THIRD, blockers=[previous, blocker(head_sha=SECOND)],
        ))
        self.assertEqual([first["status"], second["status"], third["status"]],
                         ["NOT APPROVED", "NOT APPROVED", "NO VERDICT"])
        self.assertEqual(third["findings"], [])
        self.assertEqual(third["counts"]["Blocker"], 0)
        reason = next(r for r in third["reasons"] if r["code"] == "earlier_blocker_unchecked")
        self.assertEqual(
            reason["message"], "Earlier Blocker has not been rechecked on this commit."
        )
        self.assertEqual(reason["url"], previous["url"])

    def test_re_raised_legacy_body_claim_matches_a_new_destination(self):
        previous = blocker(body="### Blocker1\nA retained defect.")
        current = finding(head_sha=SECOND, url="https://github.com/example/project/pull/1#new")
        result = compute_review_status(**inputs(
            head_sha=SECOND, findings=[current], blockers=[previous],
        ))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)

    def test_same_sha_silent_rerun_keeps_the_prior_blocker_visible(self):
        result = compute_review_status(**inputs(blockers=[blocker()]))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)
        retained = result["findings"][0]
        self.assertEqual(retained["delivery"], "reused")
        self.assertTrue(retained["url"])
        self.assertIn("retained defect", retained["body"])

    def test_same_sha_history_and_reused_current_finding_count_once(self):
        result = compute_review_status(**inputs(
            findings=[finding(delivery="reused")], blockers=[blocker()],
        ))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)
        self.assertEqual(len(result["findings"]), 1)

    def test_reused_should_fix_and_nit_findings_still_count(self):
        for finding_id, severity in (("ShouldFix1", "Should Fix"), ("Nit1", "Nit")):
            with self.subTest(severity=severity):
                retained = finding(finding_id, severity, delivery="reused")
                result = compute_review_status(**inputs(findings=[retained]))
                self.assertEqual(result["status"], "APPROVED WITH FEEDBACK")
                self.assertEqual(result["counts"][severity], 1)
                self.assertEqual(result["findings"][0]["delivery"], "reused")

    def test_rerun_with_one_retained_and_one_new_nit_counts_both(self):
        findings = [finding("Nit1", "Nit", delivery="reused"), finding("Nit2", "Nit")]
        result = compute_review_status(**inputs(findings=findings))
        self.assertEqual(result["status"], "APPROVED WITH FEEDBACK")
        self.assertEqual(result["counts"]["Nit"], 2)

    def test_missing_evidence_precedes_known_current_blockers_without_hiding_them(self):
        for key in ("coverage", "completion", "publication", "history"):
            with self.subTest(key=key):
                arguments = inputs(findings=[finding()], blockers=[blocker()])
                arguments[key]["complete"] = False
                result = compute_review_status(**arguments)
                self.assertEqual(result["status"], "NO VERDICT")
                self.assertFalse(result["status_requirements_met"])
                self.assertEqual(result["counts"]["Blocker"], 1)
                self.assertTrue(result["findings"][0]["url"])

    def test_unavailable_required_records_cannot_be_treated_as_clean(self):
        for key in ("coverage", "completion", "publication", "history"):
            with self.subTest(key=key):
                arguments = inputs()
                arguments[key] = None
                self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")

    def test_unknown_legacy_sha_or_url_makes_history_incomplete(self):
        for field in ("head_sha", "url"):
            with self.subTest(field=field):
                previous = blocker()
                previous[field] = None
                result = compute_review_status(**inputs(blockers=[previous]))
                self.assertEqual(result["status"], "NO VERDICT")
                self.assertEqual(result["counts"]["Blocker"], 0)

    def test_history_pages_and_clipping_remain_visible_as_missing_evidence(self):
        for code in ("history_pages_missing", "history_prompt_clipped"):
            with self.subTest(code=code):
                arguments = inputs()
                arguments["history"].update(complete=False, reasons=[code])
                result = compute_review_status(**arguments)
                self.assertEqual(result["status"], "NO VERDICT")
                self.assertTrue(any(r["code"] == "history_incomplete" for r in result["reasons"]))

    def test_missing_reviewer_result_or_checker_decision_prevents_approval(self):
        missing_role = inputs()
        missing_role["completion"]["attempts"].pop()
        result = compute_review_status(**missing_role)
        self.assertEqual(result["status"], "NO VERDICT")
        self.assertTrue(any(r["code"] == "reviewer_missing" for r in result["reasons"]))
        missing_checker = inputs(findings=[finding()])
        missing_checker["completion"]["candidates"][0]["checker"] = None
        result = compute_review_status(**missing_checker)
        self.assertEqual(result["status"], "NO VERDICT")
        self.assertTrue(any(r["code"] == "checker_incomplete" for r in result["reasons"]))

    def test_contested_correctness_and_incomplete_selected_rules_prevent_approval(self):
        arguments = inputs(findings=[finding()])
        arguments["completion"]["candidates"][0]["checker"]["decision"] = "CONTESTED"
        result = compute_review_status(**arguments)
        self.assertEqual(result["status"], "NO VERDICT")
        self.assertTrue(any(r["code"] == "correctness_contested" for r in result["reasons"]))
        for rule_record in ({"complete": False}, None, {}):
            with self.subTest(rule_record=rule_record):
                arguments = inputs()
                arguments["completion"]["rule_evaluation"] = rule_record
                self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")
        arguments["completion"]["rule_evaluation"] = {"complete": True}
        self.assertEqual(compute_review_status(**arguments)["status"], "APPROVED")

    def test_run_and_head_mismatches_never_approve(self):
        for key in ("coverage", "completion", "publication"):
            with self.subTest(key=key):
                arguments = inputs(findings=[finding()])
                arguments[key]["head_sha"] = SECOND
                result = compute_review_status(**arguments)
                self.assertEqual(result["status"], "NO VERDICT")
                self.assertEqual(result["counts"]["Blocker"], 1)
        for key in ("completion", "publication"):
            with self.subTest(key=key, mismatch="run"):
                arguments = inputs()
                arguments[key]["run_id"] = "other"
                self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")
        arguments = inputs()
        arguments["publication"]["run_url"] += "other"
        self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")

    def test_missing_patch_coverage_fields_prevent_approval(self):
        for field in ("files", "base_sha", "total_files", "full_files", "delivered_bytes"):
            with self.subTest(field=field):
                arguments = inputs()
                arguments["coverage"].pop(field)
                self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")

    def test_malformed_nested_metadata_abstains_without_crashing(self):
        arguments = inputs()
        arguments["coverage"]["files"][0]["status"] = []
        self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")
        for field, value in (("severity", []), ("checker", {"decision": []})):
            with self.subTest(field=field):
                arguments = inputs(findings=[finding()])
                arguments["completion"]["candidates"][0][field] = value
                self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")

    def test_same_destination_reuse_survives_display_id_and_claim_wording_changes(self):
        previous = blocker()
        current = finding("Blocker2", body="The same defect with more detail.")
        current["url"] = previous["url"]
        result = compute_review_status(**inputs(findings=[current], blockers=[previous]))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)

    def test_empty_patch_is_complete_and_nonempty_patch_requires_paths(self):
        arguments = inputs()
        arguments["coverage"].update(
            total_bytes=0, delivered_bytes=0, total_files=0, full_files=0, files=[],
        )
        self.assertEqual(compute_review_status(**arguments)["status"], "APPROVED")
        arguments["coverage"].update(total_bytes=100, delivered_bytes=100)
        self.assertEqual(compute_review_status(**arguments)["status"], "NO VERDICT")

    def test_same_sha_history_blockers_receive_noncolliding_display_ids(self):
        result = compute_review_status(**inputs(
            findings=[finding(body="A different current defect.")],
            blockers=[blocker(url="https://github.com/example/project/pull/1#older")],
        ))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 2)
        self.assertEqual({f["id"] for f in result["findings"]}, {"Blocker1", "Blocker2"})

    def test_distinct_body_only_blockers_sharing_one_review_url_remain_distinct(self):
        url = "https://github.com/example/project/pull/1#pullrequestreview-1"
        prior = [blocker(url=url), blocker(finding_id="Blocker2", body="Another defect.", url=url)]
        result = compute_review_status(**inputs(blockers=prior))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 2)
        current = finding(head_sha=SECOND, url=url)
        result = compute_review_status(**inputs(head_sha=SECOND, findings=[current], blockers=prior))
        self.assertEqual(result["status"], "NO VERDICT")
        self.assertEqual(result["counts"]["Blocker"], 1)

    def test_current_sha_reaffirmation_covers_an_older_observation_at_the_same_destination(self):
        prior = [blocker(), blocker(head_sha=SECOND)]
        result = compute_review_status(**inputs(head_sha=SECOND, blockers=prior))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertEqual(result["counts"]["Blocker"], 1)

    def test_report_modes_count_distinct_findings_without_comment_destinations(self):
        for mode in ("summary", "artifact"):
            with self.subTest(mode=mode):
                findings = [
                    finding("Nit1", "Nit", url=RUN_URL), finding("Nit2", "Nit", url=RUN_URL)
                ]
                for item in findings:
                    item.update(placement=mode, url=None, delivery=None)
                arguments = inputs(findings=findings)
                arguments["publication"]["mode"] = mode
                result = compute_review_status(**arguments)
                self.assertEqual(result["status"], "APPROVED WITH FEEDBACK")
                self.assertEqual(result["counts"]["Nit"], 2)

    def test_missing_historical_claim_uses_a_meaningful_generic_link(self):
        previous = blocker()
        previous.pop("body")
        result = compute_review_status(**inputs(blockers=[previous]))
        self.assertEqual(result["status"], "NOT APPROVED")
        self.assertIn("Blocker retained", result["findings"][0]["body"])

    def test_sensitive_paths_use_the_complete_manifest_including_omitted_files(self):
        arguments = inputs()
        paths = [
            "kernel/src/log_segment/mod.rs", "ffi/src/lib.rs", ".github/workflows/build.yml",
            "kernel/Cargo.toml", "AGENTS/testing.md", "kernel/tests/data/table/0.json",
            "docs/user-guide/src/README.md",
        ]
        arguments["coverage"].update(
            complete=False, total_bytes=200, total_files=len(paths), full_files=len(paths) - 1,
            omitted_files=1, files=[
                {"path": path, "status": "omitted" if index == 0 else "full"}
                for index, path in enumerate(paths)
            ],
        )
        result = compute_review_status(**arguments, breaking_change=True)
        self.assertEqual(result["status"], "NO VERDICT")
        self.assertEqual(result["sensitive_paths"], sorted([
            "kernel/src/log_segment/", "ffi/", ".github/workflows/", "kernel/Cargo.toml",
            "AGENTS/testing.md", "kernel/tests/data/",
        ]))
        self.assertTrue(result["breaking_change"])


if __name__ == "__main__":
    unittest.main()
