"""Tests for mechanically binding sub-agent dispatches to PR context."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import yaml


def _load_policy():
    path = Path(__file__).parents[1] / "review_context_policy.py"
    spec = importlib.util.spec_from_file_location("review_context_policy", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dispatch(input_text: str) -> dict:
    return {
        "type": "tool_call",
        "target": "sys_session_send",
        "data": {
            "name": "sys_session_send",
            "arguments": {
                "agent": "architecture-reviewer",
                "title": "architecture-review",
                "args": {"input": input_text, "purpose": "review"},
            },
        },
    }


class ReviewContextPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_dispatch_includes_canonical_context_even_when_input_is_placeholder(self) -> None:
        context_path = self.root / "review-context.txt"
        context_path.write_text("Head SHA: abc123\n\n```diff\n+actual change\n```\n")

        result = _load_policy().inject_review_context(context_path=str(context_path))(
            _dispatch("review it")
        )

        self.assertEqual(result["result"], "ALLOW")
        child_input = result["data"]["args"]["input"]
        self.assertTrue(child_input.startswith("review it\n\n## Canonical PR context"))
        self.assertIn("Head SHA: abc123", child_input)
        self.assertIn("+actual change", child_input)

    def test_dispatch_fails_closed_without_canonical_context(self) -> None:
        evaluate = _load_policy().inject_review_context(context_path=str(self.root / "missing.txt"))

        self.assertEqual(evaluate(_dispatch("review it"))["result"], "DENY")

    def test_non_dispatch_tool_is_unchanged(self) -> None:
        evaluate = _load_policy().inject_review_context(context_path=str(self.root / "missing.txt"))

        self.assertEqual(
            evaluate({"type": "tool_call", "target": "sys_read_inbox"}), {"result": "ALLOW"}
        )

    def test_dispatch_drops_model_and_reasoning_effort_overrides(self) -> None:
        event = _dispatch("review it")
        event["data"]["arguments"]["args"].update(
            model="another-model", reasoning_effort="max"
        )
        with tempfile.TemporaryDirectory() as tmp:
            context_path = Path(tmp) / "review-context.txt"
            context_path.write_text("Head SHA: abc123\n")
            result = _load_policy().inject_review_context(context_path=str(context_path))(event)

        self.assertEqual(result["result"], "ALLOW")
        self.assertEqual(set(result["data"]["args"]), {"input", "purpose"})

    def test_dispatch_guards_run_before_the_context_hook(self) -> None:
        config_path = Path(__file__).parents[1] / "reviewer" / "config.yaml"
        policies = yaml.safe_load(config_path.read_text())["guardrails"]["policies"]
        self.assertEqual(
            list(policies),
            ["spawn_bounds", "headless_subagent_purpose_guard", "inject_review_context"],
        )
