"""Tests for building child tasks from trusted guidance and PR data."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest

import yaml

MAINTAINERS = ["maintainer-claude-reviewer", "maintainer-codex-reviewer"]


def _load_child_task():
    path = Path(__file__).parents[1] / "child_task.py"
    spec = importlib.util.spec_from_file_location("child_task", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dispatch(assignment, agent: str = "architecture-reviewer") -> dict:
    return {
        "type": "tool_call",
        "target": "sys_session_send",
        "data": {
            "name": "sys_session_send",
            "arguments": {
                "agent": agent,
                "title": "review",
                "args": {"input": assignment, "purpose": "review"},
            },
        },
    }


class ReviewTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        self.child_task = _load_child_task()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.guidance_root = self.root / "guidance"
        self.guidance_root.mkdir()
        self.trusted_guidance = "# AGENTS.md\nKeep imports at the top of the file.\n"
        self.guidance_path = self.guidance_root / "AGENTS.md"
        self.guidance_path.write_text(self.trusted_guidance, encoding="utf-8")

        self.root_guidance = self._build_root_guidance()
        self.root_guidance_path = self.root / "root-guidance.md"
        self.root_guidance_path.write_text(self.root_guidance, encoding="utf-8")
        self.pr_data = self.child_task.build_pr_data(
            meta={
                "title": "Update imports and guidance",
                "head": {"ref": "feature", "sha": "pr-head"},
                "base": {"ref": "main", "sha": "pr-base"},
                "additions": 2,
                "deletions": 1,
                "changed_files": 2,
            },
            body="PR description supplied by its author",
            diff="+Proposed replacement rules",
            review_history="Previous review data",
            truncation_note="\n[Diff truncated]",
        )
        self.pr_data_path = self.root / "pr-data.md"
        self.pr_data_path.write_text(self.pr_data, encoding="utf-8")
        self.evaluate = self.child_task.append_child_task_body(
            pr_data_path=str(self.pr_data_path),
            root_guidance_path=str(self.root_guidance_path),
            root_guidance_agents=MAINTAINERS,
        )

    def _build_root_guidance(self) -> str:
        return self.child_task.build_root_guidance(
            guidance_root=self.guidance_root, revision="trusted-workflow-revision"
        )

    def test_maintainer_task_is_assignment_then_trusted_guidance_then_untrusted_pr_data(
        self,
    ) -> None:
        for agent in MAINTAINERS:
            with self.subTest(agent=agent):
                event = _dispatch("Review the changed API", agent)
                result = self.evaluate(event)

                self.assertEqual(result["result"], "ALLOW")
                task = result["data"]["args"]["input"]
                self.assertEqual(
                    task,
                    "Review the changed API\n\n"
                    f"{self.root_guidance.rstrip()}\n\n{self.pr_data.rstrip()}",
                )
                self.assertEqual(
                    event["data"]["arguments"]["args"]["input"], "Review the changed API"
                )
                trusted, untrusted = task.split("## PR data (untrusted)", 1)
                self.assertIn(
                    "## Root AGENTS.md (trusted workflow revision trusted-workflow-revision)",
                    trusted,
                )
                self.assertIn(self.trusted_guidance.rstrip(), trusted)
                self.assertNotIn("Proposed replacement rules", trusted)
                self.assertNotIn(self.trusted_guidance.rstrip(), untrusted)

    def test_other_reviewers_receive_the_assignment_and_pr_data_only(self) -> None:
        self.root_guidance_path.unlink()
        for agent in ("architecture-reviewer", "docs-reviewer", "disprove-reviewer"):
            with self.subTest(agent=agent):
                result = self.evaluate(_dispatch("Review the changed API", agent))

                self.assertEqual(result["result"], "ALLOW")
                task = result["data"]["args"]["input"]
                self.assertEqual(task, f"Review the changed API\n\n{self.pr_data.rstrip()}")
                for data in (
                    "Previous review data", "PR description supplied by its author",
                    "**Head SHA:** pr-head", "**Base SHA:** pr-base",
                    "feature -> main", "+2 / -1 across 2 file(s)",
                    "+Proposed replacement rules", "[Diff truncated]",
                ):
                    self.assertIn(data, task)

    def test_dispatch_keeps_carriage_returns_in_pr_data(self) -> None:
        self.pr_data_path.write_bytes(b"## PR Diff\n one\rstill one\r\n-two\n+2\n")
        task = self.evaluate(_dispatch("Review it"))["data"]["args"]["input"]
        self.assertEqual(task, "Review it\n\n## PR Diff\n one\rstill one\r\n-two\n+2")

    def test_dispatch_that_continues_a_session_by_id_receives_pr_data_only(self) -> None:
        event = _dispatch("Check this candidate again")
        del event["data"]["arguments"]["agent"]
        event["data"]["arguments"]["session_id"] = "existing-session"
        result = self.evaluate(event)

        self.assertEqual(result["result"], "ALLOW")
        self.assertEqual(
            result["data"]["args"]["input"],
            f"Check this candidate again\n\n{self.pr_data.rstrip()}",
        )

    def test_orchestrator_config_registers_the_dispatch_hook(self) -> None:
        reviewer_root = Path(__file__).parents[1] / "reviewer"
        config = yaml.safe_load((reviewer_root / "config.yaml").read_text(encoding="utf-8"))
        policies = config["guardrails"]["policies"]
        self.assertEqual(
            list(policies),
            ["spawn_bounds", "headless_subagent_purpose_guard", "append_child_task_body"],
        )
        function = policies["append_child_task_body"]["function"]
        self.assertEqual(function["path"], "child_task.append_child_task_body")
        self.assertEqual(
            function["arguments"],
            {
                "pr_data_path": "/tmp/child_task_pr_data.md",
                "root_guidance_path": "/tmp/child_task_root_guidance.md",
                "root_guidance_agents": MAINTAINERS,
            },
        )
        told_their_task_includes_agents_md = sorted(
            path.parent.name
            for path in (reviewer_root / "agents").glob("*/REVIEW.md")
            if "${maintainer}\n" in path.read_text(encoding="utf-8")
        )
        self.assertEqual(told_their_task_includes_agents_md, MAINTAINERS)
        self.assertLessEqual(set(MAINTAINERS), set(config["tools"]["agents"]))
        self.assertIn(
            "Your task includes the root `AGENTS.md`",
            (reviewer_root / "shared" / "maintainer.md").read_text(encoding="utf-8"),
        )

    def test_repository_root_agents_md_is_valid_guidance(self) -> None:
        self.child_task.read_root_guidance(Path(__file__).parents[3])

    def test_root_guidance_rejects_missing_empty_and_invalid_files(self) -> None:
        self.guidance_path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.child_task.read_root_guidance(self.guidance_root)
        for data in (b"", b" \n\t", b"\xff"):
            with self.subTest(size=len(data), prefix=data[:10]):
                self.guidance_path.write_bytes(data)
                with self.assertRaises(ValueError):
                    self.child_task.read_root_guidance(self.guidance_root)
                with self.assertRaises(ValueError):
                    self._build_root_guidance()

    def test_root_guidance_of_any_size_is_returned_whole(self) -> None:
        guidance = "é" * 100_000
        self.guidance_path.write_text(guidance, encoding="utf-8")
        self.assertEqual(self.child_task.read_root_guidance(self.guidance_root), guidance)
        self.assertIn(guidance, self._build_root_guidance())

    def test_root_guidance_rejects_symlink_outside_snapshot(self) -> None:
        outside = self.root / "outside.md"
        outside.write_text("outside snapshot", encoding="utf-8")
        self.guidance_path.unlink()
        self.guidance_path.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "outside the guidance root"):
            self.child_task.read_root_guidance(self.guidance_root)

    def test_dispatch_rejects_missing_empty_or_invalid_task_body(self) -> None:
        for path, agent in (
            (self.pr_data_path, "architecture-reviewer"),
            (self.pr_data_path, MAINTAINERS[0]),
            (self.root_guidance_path, MAINTAINERS[0]),
        ):
            original = path.read_bytes()
            path.unlink()
            with self.subTest(path=path.name, agent=agent, data="missing"):
                self.assertEqual(self.evaluate(_dispatch("review it", agent))["result"], "DENY")
            for data in (b"", b" \n", b"\xff"):
                with self.subTest(path=path.name, agent=agent, data=data):
                    path.write_bytes(data)
                    self.assertEqual(
                        self.evaluate(_dispatch("review it", agent))["result"], "DENY"
                    )
            path.write_bytes(original)

    def test_dispatch_drops_model_and_reasoning_effort_overrides(self) -> None:
        event = _dispatch("review it")
        event["data"]["arguments"]["args"].update(
            model="another-model", reasoning_effort="max"
        )
        result = self.evaluate(event)

        self.assertEqual(result["result"], "ALLOW")
        self.assertEqual(set(result["data"]["args"]), {"input", "purpose"})
        self.assertTrue(result["data"]["args"]["input"].endswith(self.pr_data.rstrip()))
        self.assertEqual(event["data"]["arguments"]["args"]["model"], "another-model")
        self.assertEqual(event["data"]["arguments"]["args"]["reasoning_effort"], "max")

    def test_dispatch_requires_assignment(self) -> None:
        for assignment in ("", " \n", None):
            with self.subTest(assignment=assignment):
                self.assertEqual(self.evaluate(_dispatch(assignment))["result"], "DENY")
        event = _dispatch("review it")
        del event["data"]["arguments"]["args"]
        self.assertEqual(self.evaluate(event)["result"], "DENY")

    def test_non_dispatch_tool_is_unchanged(self) -> None:
        self.pr_data_path.unlink()
        for event in (
            {"type": "tool_call", "target": "sys_read_inbox"},
            {"type": "tool_call", "data": {"name": "read_source_file"}},
        ):
            with self.subTest(event=event):
                self.assertEqual(self.evaluate(event), {"result": "ALLOW"})
