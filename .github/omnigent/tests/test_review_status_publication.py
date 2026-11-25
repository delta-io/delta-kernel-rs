"""Exercise status publication with real coverage, execution and history producers."""

from __future__ import annotations

import copy
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).parents[1]))
try:
    import review_status
    from diff_coverage import prepare_review_inputs
    from finding_publication import PublicationError, publication_check, publish_findings
    from review_evidence import build_execution_record, execution_summary, observe_review
    from review_history import history_evidence
    from review_publish import format_review_summary
    from review_status import compute_review_status, publish_review_status
finally:
    sys.path.pop(0)

from test_finding_publication import DIFF, FakeGitHub
from test_review_evidence import (
    ARCH, ROLES, candidate, checker_events, empty_events, outcome, role_events,
)

HEAD = "a" * 40
RUN_ID = "123"
RUN_URL = f"https://github.com/example/project/actions/runs/{RUN_ID}"
REVIEW = "## PR Summary\nUpdates a call."


def history_from_github(github):
    def node(item):
        return {
            "id": item["id"], "url": item["html_url"], "body": item["body"],
            "author": {"__typename": "Bot", "login": "github-actions"},
        }

    def connection(nodes):
        return {"nodes": nodes, "pageInfo": {"hasPreviousPage": False, "hasNextPage": False}}

    reviews = [{
        "body": "", "author": {"__typename": "Bot", "login": "github-actions"},
        "commit": {"oid": item["commit_id"]},
        "comments": connection([{**node(item), "path": item["path"], "line": item["line"]}]),
    } for item in github.inline]
    document = {"data": {"repository": {"pullRequest": {
        "comments": connection([node(item) for item in github.issues]),
        "reviews": connection(reviews),
    }}}}
    return history_evidence(document)


def pipeline(github, severity=None, *, head_sha=HEAD, mode="inline", findings=None):
    history = history_from_github(github)
    coverage = prepare_review_inputs(DIFF.encode(), "Author context.", "f" * 40, head_sha)["coverage"]
    entries = [] if severity is None else [{
        "id": severity.replace(" ", "") + "1", "body": "A concrete retained claim.",
        "path": "file.rs", "line": 1, "side": "RIGHT",
    }]
    if findings is not None:
        entries = findings
    publication = publish_findings(
        github=github, repository="example/project", pr_number=1, head_sha=head_sha,
        run_url=RUN_URL, review=REVIEW, structured=entries, github_pr_patch=DIFF, pr_change_patch=DIFF,
        trusted_bot_logins=["github-actions"], mode=mode, coverage=coverage,
    )
    candidates = [candidate(f"C{i}", item["severity"])
                  for i, item in enumerate(publication["findings"], 1)]
    events = empty_events({ARCH: role_events(ARCH, candidates)})
    batch = [
        {"batch_id": f"check{i}", "role": ARCH, "id": item["id"]}
        for i, item in enumerate(candidates, 1) if item["severity"] != "Nit"
    ]
    if batch:
        events += checker_events(batch)
    outcomes = [outcome(ARCH, item["id"], final_id=final["id"])
                for item, final in zip(candidates, publication["findings"])]
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "observations.jsonl"
        observer = observe_review(
            path=str(path), run_id=RUN_ID, head_sha=head_sha, configured_reviewer_roles=ROLES,
        )
        for event in events:
            observer(event)
        observations = [json.loads(line) for line in path.read_text().splitlines()]
    completion = build_execution_record(
        observations, {"outcomes": outcomes}, publication, history,
        run_id=RUN_ID, head_sha=head_sha, configured_reviewer_roles=ROLES,
    )
    return dict(head_sha=head_sha, run_id=RUN_ID, coverage=coverage, completion=completion,
                publication=publication, history=history)


def publish_status(github, arguments, *, mode="inline"):
    review_status = compute_review_status(**arguments)
    return publish_review_status(
        github=github, repository="example/project", pr_number=1,
        publication=arguments["publication"], review=REVIEW, run_url=RUN_URL,
        review_status=review_status, coverage=arguments["coverage"], mode=mode,
    )


class ReviewStatusPublicationTest(unittest.TestCase):
    def test_three_heads_and_silent_current_blocker_on_both_publication_paths(self):
        for mode in ("inline", "comment"):
            with self.subTest(mode=mode):
                github = FakeGitHub()
                first = publish_status(github, pipeline(github, "Blocker", mode=mode), mode=mode)
                silent = publish_status(github, pipeline(github, mode=mode), mode=mode)
                second = publish_status(
                    github, pipeline(github, "Blocker", head_sha="b" * 40, mode=mode), mode=mode,
                )
                third = publish_status(
                    github, pipeline(github, head_sha="c" * 40, mode=mode), mode=mode,
                )
                self.assertEqual([v["status"] for v in (first, silent, second, third)],
                                 ["NOT APPROVED", "NOT APPROVED", "NOT APPROVED", "NO VERDICT"])
                self.assertEqual(first["counts"]["Blocker"], 1)
                self.assertEqual(silent["counts"]["Blocker"], 1)
                self.assertEqual(third["counts"]["Blocker"], 0)
                main = github.issues[-1]["body"]
                self.assertIn("Status requirements unmet", main)
                self.assertIn("Earlier Blocker has not been rechecked on this commit.", main)
                self.assertNotIn("<summary>Review Summary", main)
                self.assertEqual(len(github.inline), int(mode == "inline"))

    def test_advisory_repeat_keeps_count_and_new_rerun_nit_remains_visible(self):
        for severity in ("Nit", "Should Fix"):
            with self.subTest(severity=severity):
                github = FakeGitHub()
                publish_status(github, pipeline(github, severity))
                arguments = pipeline(github, severity)
                review_status = publish_status(github, arguments)
                self.assertEqual(review_status["status"], "APPROVED WITH FEEDBACK")
                self.assertEqual(review_status["counts"][severity], 1)
                self.assertIn("New finding comments: 0 · Reused finding comments: 1",
                              publication_check(REVIEW, arguments["publication"]))
                main = github.issues[0]["body"]
                self.assertIn("Status requirements met", main)
                self.assertNotIn("Unmet status requirements", main)
                self.assertEqual("existing thread" in main, severity == "Should Fix")
                entries = [{key: item.get(key) for key in ("id", "body", "path", "line", "side")}
                           for item in arguments["publication"]["findings"]]
                entries.append({"id": "Nit2", "body": "A new useful correction.",
                                "path": "file.rs", "line": 2, "side": "RIGHT"})
                publish_status(github, pipeline(github, findings=entries))
                self.assertEqual(len(github.inline), 2)

    def test_partial_review_preserves_visible_current_blocker_and_layout_order(self):
        github = FakeGitHub()
        arguments = pipeline(github, "Blocker")
        arguments["completion"]["complete"] = False
        arguments["completion"]["reasons"] = [
            "Required reviewer docs-reviewer has no valid return."
        ]
        review_status = publish_status(github, arguments)
        self.assertEqual(review_status["status"], "NO VERDICT")
        main = github.issues[0]["body"]
        order = ["AI Review - NO VERDICT", "---", "1 Blocker", "Reviewed",
                 "Status requirements unmet", "<summary>Unmet status requirements",
                 "<summary>PR Summary", "<summary>Review Summary", "Full AI review"]
        offsets = [main.index(part) for part in order]
        self.assertEqual(offsets, sorted(offsets))
        self.assertIn("[Blocker 1]", main)
        self.assertEqual(len(github.inline), 1)

    def test_mismatched_publication_never_rewrites_another_heads_summary(self):
        github = FakeGitHub()
        arguments = pipeline(github)
        review_status = compute_review_status(**arguments)
        before = copy.deepcopy(github.writes)
        wrong = dict(arguments["publication"], head_sha="d" * 40)
        publish_review_status(
            github=github, repository="example/project", pr_number=1, publication=wrong,
            review=REVIEW, run_url=RUN_URL, review_status=review_status, coverage=arguments["coverage"],
            mode="inline",
        )
        self.assertEqual(github.writes, before)
        self.assertEqual(review_status["status"], "NO VERDICT")

    def test_unconfirmed_status_update_stays_no_verdict(self):
        github = FakeGitHub()
        arguments = pipeline(github)
        with patch.object(github, "write", side_effect=PublicationError("unobserved")):
            review_status = publish_status(github, arguments)
        self.assertEqual(review_status["status"], "NO VERDICT")
        self.assertFalse(review_status["status_requirements_met"])
        self.assertTrue(arguments["coverage"]["complete"])
        self.assertTrue(
            any(r["code"] == "status_publication_unconfirmed" for r in review_status["reasons"])
        )

    def test_report_modes_record_findings_without_creating_or_updating_pr_comments(self):
        for mode in ("summary", "artifact"):
            with self.subTest(mode=mode):
                github = FakeGitHub()
                arguments = pipeline(github, "Blocker", mode=mode)
                status = publish_status(github, arguments, mode=mode)
                self.assertEqual(status["status"], "NOT APPROVED")
                self.assertTrue(status["status_requirements_met"])
                self.assertEqual(status["counts"]["Blocker"], 1)
                self.assertEqual(github.writes, [])
                self.assertEqual(github.issues, [])
                self.assertEqual(github.inline, [])
                output = status["findings"][0]
                self.assertIsNone(output["url"])
                self.assertIsNone(output["delivery"])
                self.assertEqual(output["placement"], mode)
                report = publication_check(REVIEW, arguments["publication"])
                self.assertIn(f"Findings in {mode} report: 1", report)
                self.assertIn("New finding comments: 0 · Reused finding comments: 0", report)

    def test_workflow_cli_retains_filtered_status_and_neutral_check_mirrors_it(self):
        repository = Path(__file__).resolve().parents[3]
        workflow = yaml.safe_load((repository / ".github/workflows/ai-review.yml").read_text())
        steps = {step.get("name"): step for job in workflow["jobs"].values()
                 for step in job.get("steps", [])}
        for historical_blocker in (False, True):
            with self.subTest(historical_blocker=historical_blocker):
                github = FakeGitHub()
                if historical_blocker:
                    first = pipeline(github, "Blocker")
                    publish_status(github, first)
                    prior_url = first["publication"]["findings"][0]["url"]
                    arguments = pipeline(github)
                    expected_status = "NOT APPROVED"
                else:
                    arguments = pipeline(github, "Nit")
                    expected_status = "APPROVED WITH FEEDBACK"
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    for name, value in (
                        ("diff_coverage", arguments["coverage"]),
                        ("review_execution", arguments["completion"]),
                        ("publication_record", arguments["publication"]),
                        ("review_history_evidence", arguments["history"]),
                        ("pr_meta", {"title": "ci!: change review input", "labels": []}),
                    ):
                        (root / f"{name}.json").write_text(json.dumps(value))
                    (root / "review_output_complete.txt").write_text(REVIEW)
                    (root / "check-summary.txt").write_text(
                        publication_check(REVIEW, arguments["publication"])
                        + execution_summary(arguments["completion"])
                    )
                    env = {"HEAD_SHA": HEAD, "REVIEW_RUN_ID": RUN_ID, "REPO": "example/project",
                           "PR_NUMBER": "1", "RUN_URL": RUN_URL, "OUTPUT_MODE": "inline"}
                    command = steps["Publish reviewed commit status"]["run"].split("python3 ", 1)[1]
                    tokens = shlex.split(command.replace("\\\n", "").replace("/tmp/", f"{root}/"))
                    argv = [
                        env.get(token[1:], token) if token.startswith("$") else token
                        for token in tokens
                    ]
                    with (
                        patch.object(sys, "argv", argv),
                        patch.object(review_status, "GitHub", return_value=github),
                    ):
                        review_status.main()
                    record = json.loads((root / "review_execution.json").read_text())
                    self.assertEqual(record["review_status"]["status"], expected_status)
                    self.assertTrue(record["review_status"]["breaking_change"])
                    retained = record["review_status"]["findings"][0]
                    self.assertNotIn("body", retained)
                    final_check = (root / "check-summary.txt").read_text()
                    if historical_blocker:
                        self.assertEqual(record["review_status"]["counts"]["Blocker"], 1)
                        self.assertEqual(retained["delivery"], "reused")
                        self.assertEqual(retained["placement"], "history")
                        self.assertEqual(retained["url"], prior_url)
                        self.assertIn("A concrete retained claim.", final_check)
                        self.assertIn(prior_url, final_check)
                        self.assertIn("1 Blocker", final_check)
                        self.assertIn(
                            "New finding comments: 0 · Reused finding comments: 0", final_check
                        )
                        self.assertIn("Earlier finding references: 1", final_check)
                    else:
                        self.assertIn("New finding comments: 1 · Reused finding comments: 0", final_check)
                    gh = root / "gh"
                    gh.write_text(
                        "#!/usr/bin/env python3\nimport pathlib, shutil, sys\n"
                        "source = sys.argv[sys.argv.index('--input') + 1]\n"
                        "shutil.copyfile(source, "
                        "pathlib.Path(__file__).with_name('sent-check.json'))\n"
                    )
                    gh.chmod(0o755)
                    check_script = steps["Publish non-blocking PR check"]["run"].replace(
                        "/tmp/", f"{root}/"
                    )
                    check = subprocess.run(
                        ["bash", "-c", check_script],
                        cwd=repository, capture_output=True, text=True,
                        env={**os.environ, **env, "PATH": f"{root}:{os.environ['PATH']}",
                             "REVIEW_READY": "true", "REVIEW_ATTEMPTED": "true", "BOT_TOKEN": ""},
                    )
                    self.assertEqual(check.returncode, 0, check.stdout + check.stderr)
                    sent = json.loads((root / "sent-check.json").read_text())
                    self.assertEqual(sent["conclusion"], "neutral")
                    self.assertEqual(sent["output"]["title"], f"AI Review - {expected_status}")
                    self.assertIn("Status requirements met", sent["output"]["summary"])
                    if historical_blocker:
                        self.assertIn(prior_url, sent["output"]["summary"])
                        self.assertIn("A concrete retained claim.", sent["output"]["summary"])


if __name__ == "__main__":
    unittest.main()
