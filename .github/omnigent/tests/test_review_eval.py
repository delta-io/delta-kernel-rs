"""Behavioral replay mechanics and scoring, without paid model calls."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
from review_eval import (
    MARKER,
    ROOT,
    case_context,
    digest,
    prepare_bundle,
    run_case,
    score,
    source_tree,
    validate_trace,
)
from review_eval_trace import record_events

MODELS = {
    name: "test-model" for name in ("default", "maintainer-claude", "maintainer-codex", "disprove")
}


class ReviewEvalTest(unittest.TestCase):
    def case(self):
        return json.loads((ROOT / "evaluations/panic.input.json").read_text())

    def test_input_is_frozen_and_labels_are_not_reviewer_context(self):
        case = self.case()
        case["labels"] = "DO NOT LEAK THIS ANSWER"
        context, diff = case_context(case)
        self.assertNotIn("DO NOT LEAK", context)
        self.assertIn("-    value.ok_or", diff)
        self.assertIn("+    Ok(value.unwrap())", diff)
        changed = dict(case, after={"x.rs": "other source"})
        self.assertNotEqual(digest(case), digest(changed))

    def test_snapshot_without_final_newline_has_valid_diff_positions(self):
        from inline_review import diff_positions

        case = dict(self.case(), before={"a.rs": "old"}, after={"a.rs": "new"})
        _, diff = case_context(case)
        self.assertIn("-old\n\\ No newline at end of file\n+new\n", diff)
        self.assertIn(("a.rs", 1, "RIGHT"), diff_positions(diff))

    def test_source_tree_rejects_escaping_and_history_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            for path in ("../answer", "/tmp/answer", ".git/config"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    source_tree(Path(directory), {path: "bad"})

    def test_bundle_preserves_prompts_and_isolates_context_and_model_resolution(self):
        import yaml

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepare_bundle(root / "bundle", root / "context", root / "trace", MODELS)
            config = yaml.safe_load((root / "bundle/config.yaml").read_text())
            self.assertEqual(config["executor"]["model"], "test-model")
            self.assertEqual(
                config["guardrails"]["policies"]["inject_review_context"]["function"]["arguments"][
                    "context_path"
                ],
                str(root / "context"),
            )
            self.assertEqual(
                (root / "bundle/REVIEW.md").read_text(), (ROOT / "reviewer/REVIEW.md").read_text()
            )
            self.assertTrue(
                (root / "bundle/agents/disprove-reviewer/tools/python/read_source_file.py").exists()
            )

    def test_trace_preserves_raw_calls_and_results_without_changing_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            record = record_events(path=str(path))
            events = [
                {
                    "type": "tool_call",
                    "data": {"name": "sys_session_send", "arguments": {"input": "candidate"}},
                },
                {"type": "PHASE_TOOL_RESULT", "data": {"result": "CONTESTED"}},
            ]
            for event in events:
                self.assertEqual(record(dict(event, llm_client=object())), {"result": "ALLOW"})
            self.assertEqual([json.loads(line) for line in path.read_text().splitlines()], events)

    def run_fake(self, root, script, *, mode="gate", timeout=5):
        command = root / "runtime.py"
        command.write_text(script)
        return run_case(
            self.case(),
            mode=mode,
            output=root / "output",
            command=[sys.executable, str(command)],
            models=MODELS,
            runtime="test-runtime",
            timeout=timeout,
        )

    def test_gate_replay_records_raw_evidence_and_runtime_failure(self):
        for script, expected in [
            ("print('**Verdict**: CONFIRMED\\n**Evidence**: unwrap(None) panics')", "complete"),
            ("raise SystemExit(9)", "failed"),
        ]:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result = self.run_fake(root, script)
                self.assertEqual(result["status"], expected)
                self.assertEqual(json.loads((root / "output/run.json").read_text()), result)
                self.assertIsNone(result["cost_usd"])

    def test_pipeline_uses_publication_parser_and_requires_trace(self):
        raw = (
            f"<!-- AI_REVIEW_START_{MARKER} -->\nNo blocking issues.\n"
            f'<!-- AI_REVIEW_INLINE_START_{MARKER} -->\n{{"findings":[]}}\n'
            f"<!-- AI_REVIEW_INLINE_END_{MARKER} -->\n<!-- AI_REVIEW_END_{MARKER} -->"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = self.run_fake(root, f"print({raw!r})", mode="pipeline")
            self.assertEqual(result["status"], "failed")
            self.assertIn("trace", result["error"])
            self.assertTrue((root / "output/publication.json").exists())

    def test_trace_rejects_dispatches_without_returned_evidence(self):
        events = []
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            for name in ("maintainer-codex-reviewer", "docs-reviewer", "disprove-reviewer"):
                events.append(
                    {
                        "type": "tool_call",
                        "data": {"name": "sys_session_send", "arguments": {"agent": name}},
                    }
                )
                if name != "disprove-reviewer":
                    events.append(
                        {
                            "type": "tool_result",
                            "data": {"result": "findings"},
                            "request_data": {"name": "sys_session_send", "args": {"agent": name}},
                        }
                    )
            path.write_text("\n".join(json.dumps(event) for event in events))
            with self.assertRaisesRegex(ValueError, "disprove"):
                validate_trace(path)
            events.append(
                {
                    "type": "tool_result",
                    "data": {"result": "CONTESTED"},
                    "request_data": {
                        "name": "sys_session_send",
                        "args": {"agent": "disprove-reviewer"},
                    },
                }
            )
            path.write_text("\n".join(json.dumps(event) for event in events))
            validate_trace(path)

    def test_dispatch_evaluation_isolated_from_pr_publication(self):
        import ast
        import re
        import yaml

        workflow = yaml.safe_load((ROOT.parent / "workflows/ai-review.yml").read_text())
        steps = workflow["jobs"]["review"]["steps"]
        named = {step["name"]: step for step in steps if "name" in step}
        self.assertLess(
            steps.index(named["Run behavioral replay pilot"]),
            steps.index(named["Materialize reviewer model config"]),
        )
        for name in (
            "Materialize reviewer model config",
            "Collect PR context and build review prompt",
            "Materialize PR source reference",
            "Run AI review",
        ):
            self.assertIn("inputs.evaluation_mode != 'gate'", named[name]["if"])
            self.assertIn("inputs.evaluation_mode != 'pipeline'", named[name]["if"])
        script = named["Run behavioral replay pilot"]["run"]
        self.assertIn('command=["omnigent"]', script)
        self.assertIn('result["status"] == "complete"', script)
        for source in re.findall(r"python3[^\n]*<<'PYEOF'\n(.*?)\nPYEOF", script, re.S):
            ast.parse(source)
        self.assertIn("always()", named["Upload behavioral replay evidence"]["if"])
        self.assertIn(
            "steps.evaluation_artifacts.outcome == 'success'",
            named["Upload behavioral replay evidence"]["if"],
        )

    def test_artifact_upload_withholds_credentials_without_rewriting_evidence(self):
        from review_eval import verify_artifacts

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout.txt"
            for text in (
                "fake-eval-sentinel",
                "Authorization: Bearer fake-value",
                "token=fake-value",
            ):
                path.write_text(text)
                with self.assertRaisesRegex(ValueError, "withheld"):
                    verify_artifacts(Path(directory), "fake-eval-sentinel")
                self.assertEqual(path.read_text(), text)
            path.write_text("**Verdict**: CONFIRMED")
            verify_artifacts(Path(directory), "fake-eval-sentinel")

    def test_trace_correlates_omnigent_082_inbox_results_with_launch_receipts(self):
        events = []
        for index, agent in enumerate(
            ("maintainer-codex-reviewer", "docs-reviewer", "disprove-reviewer")
        ):
            task = f"{index:032x}"
            events.extend(
                [
                    {
                        "type": "tool_call",
                        "target": "sys_session_send",
                        "data": {"name": "sys_session_send", "arguments": {"agent": agent}},
                    },
                    {
                        "type": "tool_result",
                        "target": "sys_session_send",
                        "data": {
                            "result": json.dumps(
                                {"task_id": task, "agent": agent, "status": "launching"}
                            )
                        },
                    },
                ]
            )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text("\n".join(json.dumps(e) for e in events))
            with self.assertRaisesRegex(ValueError, "quorum"):
                validate_trace(path)
            for index, agent in enumerate(
                ("maintainer-codex-reviewer", "docs-reviewer", "disprove-reviewer")
            ):
                events.append(
                    {
                        "type": "tool_result",
                        "target": "sys_read_inbox",
                        "data": {
                            "result": f"[System: sub-agent task {index:032x} completed \u2014 {agent}:review returned: evidence]"
                        },
                    }
                )
            path.write_text("\n".join(json.dumps(e) for e in events))
            validate_trace(path)

    def test_timeout_is_a_failed_run_not_a_clean_review(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_fake(Path(directory), "import time; time.sleep(2)", timeout=1)
            self.assertEqual(result["status"], "failed")

    def test_score_credits_new_true_findings_and_separates_gate_from_pipeline(self):
        labels = {"input_digest": "frozen", "issues": [{"id": "C1", "truth": "true"}]}
        run = {
            "input_digest": "frozen",
            "status": "complete",
            "mode": "pipeline",
            "seconds": 1,
            "cost_usd": None,
        }
        row = {
            "issue_id": "C1",
            "discovered": True,
            "published": True,
            "truth": "true",
            "harmful_fix": False,
            "expected_scope": "in",
            "reported_scope": "in",
            "kind": "runtime",
            "gate_verdict": "CONFIRMED",
            "evidence": "source + gate + final references",
        }
        adjudication = {
            "input_digest": "frozen",
            "run_digest": digest(run),
            "findings": [row, dict(row, issue_id=None)],
        }
        result = score(labels, adjudication, run)
        self.assertEqual(result["discovery_recall"], 1)
        self.assertEqual(result["new_valid_published"], 1)
        self.assertEqual(result["false_published"], 0)
        adjudication["findings"] = [dict(row, truth="false")]
        self.assertEqual(score(labels, adjudication, run)["discovery_recall"], 0)
        adjudication["findings"] = [row]

        run["mode"] = "gate"
        adjudication["run_digest"] = digest(run)
        with self.assertRaises(ValueError):
            score(labels, adjudication, run)
        adjudication["findings"] = [dict(row, discovered=False, published=False)]
        self.assertIsNone(score(labels, adjudication, run)["discovery_recall"])
        run["status"] = "failed"
        with self.assertRaises(ValueError):
            score(labels, adjudication, run)


if __name__ == "__main__":
    unittest.main()
