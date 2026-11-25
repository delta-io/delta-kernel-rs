"""Verify workflow-owned identities and incomplete execution through the real recorder CLI."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


class ReviewEvidenceWorkflowTest(unittest.TestCase):
    def test_workflow_binds_trusted_roster_and_preserves_missing_returns_as_incomplete(self):
        repository = Path(__file__).resolve().parents[3]
        source = repository / ".github/omnigent"
        workflow = yaml.safe_load((repository / ".github/workflows/ai-review.yml").read_text())
        steps = {
            step.get("name"): step
            for job in workflow["jobs"].values() for step in job.get("steps", [])
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / ".github/omnigent/reviewer/config.yaml"
            config_path.parent.mkdir(parents=True)
            config_path.write_bytes((source / "reviewer/config.yaml").read_bytes())
            (root / "review_observations.jsonl").write_text('stale observations\n')
            env = {**os.environ, "REVIEW_RUN_ID": "123", "HEAD_SHA": "a" * 40}
            bind_script = steps["Bind reviewer execution record"]["run"].replace(
                "/tmp/", f"{root}/"
            )
            bind = subprocess.run(
                ["bash", "-c", bind_script],
                cwd=root, env=env, text=True, capture_output=True,
            )
            self.assertEqual(bind.returncode, 0, bind.stdout + bind.stderr)
            config = yaml.safe_load(config_path.read_text())
            policies = config["guardrails"]["policies"]
            order = list(policies)
            self.assertLess(
                order.index("record_review_evidence"), order.index("append_child_task_body")
            )
            arguments = policies["record_review_evidence"]["function"]["arguments"]
            self.assertEqual(arguments["head_sha"], "a" * 40)
            self.assertEqual(arguments["run_id"], "123")
            roles = config["tools"]["agents"]
            self.assertEqual(
                arguments["required_roles"], [r for r in roles if r != "disprove-reviewer"]
            )
            self.assertEqual((root / "review_observations.jsonl").read_text(), "")
            connection = {
                "nodes": [], "pageInfo": {"hasPreviousPage": False, "hasNextPage": False}
            }
            history = {"data": {"repository": {"pullRequest": {
                "comments": connection, "reviews": connection,
            }}}, "_inline_locations_available": True}
            (root / "pr_history.json").write_text(json.dumps(history))
            (root / "trusted_bot_logins.json").write_text('["github-actions"]')
            (root / "review_accounting.json").write_text('{"outcomes":[]}')
            collect_script = steps["Collect reviewer execution evidence"]["run"].replace(
                "/tmp/", f"{root}/"
            )
            collect = subprocess.run(
                ["bash", "-c", collect_script],
                cwd=repository, env=env, text=True, capture_output=True,
            )
            self.assertEqual(collect.returncode, 0, collect.stdout + collect.stderr)
            record = json.loads((root / "review_execution.json").read_text())
            self.assertEqual(record["run_id"], "123")
            self.assertEqual(record["required_roles"], arguments["required_roles"])
            self.assertFalse(record["complete"])
            self.assertTrue(
                any("no valid task result recorded" in reason for reason in record["reasons"])
            )
            summary = (root / "check-summary.txt").read_text()
            self.assertIn("Review execution", summary)
            self.assertIn("Incomplete", summary)
            artifact = steps["Retain reviewer execution evidence"]["with"]["path"]
            self.assertEqual(artifact, "/tmp/review_execution.json")

    def test_malformed_accounting_stays_incomplete_and_is_removed_from_human_review(self):
        repository = Path(__file__).resolve().parents[3]
        workflow = yaml.safe_load((repository / ".github/workflows/ai-review.yml").read_text())
        steps = {
            step.get("name"): step
            for job in workflow["jobs"].values() for step in job.get("steps", [])
        }
        marker = "b" * 32
        raw = (
            f"<!-- AI_REVIEW_START_{marker} -->\n## PR Summary\nChange context.\n"
            f"<!-- AI_REVIEW_ACCOUNTING_START_{marker} -->not-json"
            f"<!-- AI_REVIEW_ACCOUNTING_END_{marker} -->\n"
            f"<!-- AI_REVIEW_INLINE_START_{marker} -->{{\"findings\":[]}}"
            f"<!-- AI_REVIEW_INLINE_END_{marker} -->\n"
            f"<!-- AI_REVIEW_END_{marker} -->"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "review_raw_output.txt").write_text(raw)
            (root / "review_stderr.log").write_text("")
            output = root / "github-output"
            env = {
                **os.environ, "REVIEW_STATUS": "0", "REVIEW_MARKER": marker,
                "REVIEW_RUN_ID": "123", "HEAD_SHA": "a" * 40,
                "REVIEW_BASE_SHA": "c" * 40, "GITHUB_BASE_SHA": "c" * 40,
                "REPO": "example/project", "OUTPUT_MODE": "inline",
                "GITHUB_OUTPUT": str(output), "LLM_API_KEY": "",
            }
            validate = subprocess.run(
                ["bash", "-c", steps["Validate review output"]["run"].replace("/tmp/", f"{root}/")],
                cwd=repository, env=env, text=True, capture_output=True,
            )
            self.assertEqual(validate.returncode, 0, validate.stdout + validate.stderr)
            human = (root / "review_output.txt").read_text()
            self.assertIn("Change context.", human)
            self.assertNotIn("AI_REVIEW_ACCOUNTING", human)
            self.assertNotIn("not-json", human)
            self.assertEqual(json.loads((root / "review_accounting.json").read_text()), {})
            config = yaml.safe_load(
                (repository / ".github/omnigent/reviewer/config.yaml").read_text()
            )
            roles = [r for r in config["tools"]["agents"] if r != "disprove-reviewer"]
            (root / "required_reviewers.json").write_text(json.dumps(roles))
            (root / "pr_history.json").write_text("{}")
            (root / "trusted_bot_logins.json").write_text('["github-actions"]')
            (root / "review_observations.jsonl").write_text("")
            (root / "check-summary.txt").write_text(human)
            collect_script = steps["Collect reviewer execution evidence"]["run"].replace(
                "/tmp/", f"{root}/"
            )
            collect = subprocess.run(
                ["bash", "-c", collect_script], cwd=repository, env=env,
                text=True, capture_output=True,
            )
            self.assertEqual(collect.returncode, 0, collect.stdout + collect.stderr)
            record = json.loads((root / "review_execution.json").read_text())
            self.assertFalse(record["complete"])
            self.assertIn("The orchestrator's candidate-to-final mapping is missing or malformed.", record["reasons"])
            self.assertNotIn("AI_REVIEW_ACCOUNTING", (root / "check-summary.txt").read_text())


if __name__ == "__main__":
    unittest.main()
