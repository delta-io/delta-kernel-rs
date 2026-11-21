"""Tests for reviewing only an individual stacked PR's changes."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

REPOSITORY = "delta-io/delta-kernel-rs"
PR_URL = f"https://github.com/{REPOSITORY}/pull"
GITHUB_BASE = "a" * 40
PARENT = "b" * 40
HEAD = "c" * 40
FORK = "d" * 40
MERGE = "e" * 40
ADVANCED = "f" * 40


def _load_module():
    path = Path(__file__).parents[1] / "review_scope.py"
    spec = importlib.util.spec_from_file_location("review_scope", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def _metadata(body: str = "") -> dict:
    return {
        "number": 2,
        "title": "feat: child change",
        "body": body,
        "base": {"sha": GITHUB_BASE, "ref": "main"},
        "head": {
            "sha": HEAD,
            "ref": "stack/child",
            "repo": {"full_name": "contributor/delta-kernel-rs"},
        },
    }


def _stack_body(parent: str | None = None, boundary: str | None = None) -> str:
    lines = ["## Stacked PR"]
    indent = ""
    if parent is not None:
        lines.append(f"- {parent}")
        indent = "  "
    current = f"{indent}- [**stack/child**]({PR_URL}/2)"
    if boundary is not None:
        current += f" [[Files changed]({PR_URL}/2/files/{boundary}..{HEAD})]"
    lines.append(current + " <- _this PR_")
    lines.append(f"{indent}  - [stack/grandchild]({PR_URL}/3)")
    return "\n".join(lines)


def _comparison(base: str, status: str = "ahead") -> dict:
    return {"status": status, "merge_base_commit": {"sha": base}}


class ReviewScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()
        self.responses = {
            f"repos/{REPOSITORY}/pulls/1": {"head": {"sha": PARENT}, "merged_at": None},
            f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}": _comparison(PARENT),
            f"repos/{REPOSITORY}/compare/{GITHUB_BASE}...{HEAD}": _comparison(FORK, "diverged"),
            f"repos/{REPOSITORY}/compare/{GITHUB_BASE}...{PARENT}": _comparison(FORK, "diverged"),
            f"repos/{REPOSITORY}/compare/{MERGE}...{HEAD}": _comparison(FORK, "diverged"),
        }
        self.get_json = Mock(side_effect=self.responses.__getitem__)

    def resolve(self, body: str) -> str:
        return self.module.resolve_review_base(_metadata(body), REPOSITORY, self.get_json)

    def test_ordinary_pr_and_stack_root_keep_github_base_and_log_reason(self) -> None:
        for body, reason in (
            ("", "ordinary PR"),
            (_stack_body(), "stack root"),
            (_stack_body(boundary=HEAD), "stack root"),
        ):
            with self.subTest(reason=reason), self.assertLogs("review_scope", level="INFO") as logs:
                self.assertEqual(self.resolve(body), GITHUB_BASE)
                self.assertIn(reason, logs.output[0])
        self.get_json.assert_not_called()

    def test_parent_lookup_ignores_stale_malformed_and_empty_comparison_pins(self) -> None:
        for boundary in (PARENT, HEAD, "not-a-sha"):
            for pinned_head in (HEAD, FORK):
                body = _stack_body(f"[parent]({PR_URL}/1)", boundary)
                body = body.replace(f"..{HEAD}", f"..{pinned_head}")
                with self.subTest(boundary=boundary, pinned_head=pinned_head):
                    self.assertEqual(self.resolve(body), PARENT)
        self.assertEqual(self.get_json.call_args_list[0].args, (f"repos/{REPOSITORY}/pulls/1",))

    def test_parent_lookup_skips_siblings_and_uses_immediate_ancestor(self) -> None:
        body = (
            "## Stacked PR\n"
            f"- [root]({PR_URL}/4)\n"
            f"  - [parent]({PR_URL}/1)\n"
            f"    - [sibling]({PR_URL}/5)\n"
            f"    - [**renamed child**]({PR_URL}/2) <- _this PR_\n"
        )
        for listing in (body, body.replace("- ", "* ").replace("  ", "\t")):
            with self.subTest(listing=listing):
                self.assertEqual(self.resolve(listing), PARENT)

    def test_pr_targeting_parent_branch_uses_its_configured_base(self) -> None:
        self.responses[f"repos/{REPOSITORY}/pulls/1"]["head"]["sha"] = GITHUB_BASE
        self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)")), GITHUB_BASE)
        self.get_json.assert_called_once_with(f"repos/{REPOSITORY}/pulls/1")

    def test_advanced_parent_uses_the_inherited_merge_base(self) -> None:
        self.responses[f"repos/{REPOSITORY}/pulls/1"]["head"]["sha"] = ADVANCED
        self.responses[f"repos/{REPOSITORY}/compare/{ADVANCED}...{HEAD}"] = _comparison(
            PARENT, "diverged"
        )
        self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)")), PARENT)

    def test_child_merging_main_without_parent_stops_inflated_scope(self) -> None:
        self.responses[f"repos/{REPOSITORY}/compare/{GITHUB_BASE}...{HEAD}"] = _comparison(
            MERGE, "diverged"
        )
        with self.assertRaisesRegex(ValueError, "base-branch changes"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))

    def test_squash_merged_parent_is_excluded_until_merge_result_is_inherited(self) -> None:
        self.responses[f"repos/{REPOSITORY}/pulls/1"].update(
            merged_at="2026-10-21T00:00:00Z", merge_commit_sha=MERGE
        )
        self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)", PARENT)), PARENT)
        self.get_json.reset_mock()
        self.responses[f"repos/{REPOSITORY}/compare/{MERGE}...{HEAD}"] = _comparison(MERGE)
        self.responses[f"repos/{REPOSITORY}/compare/{MERGE}...{GITHUB_BASE}"] = _comparison(MERGE)
        self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)", PARENT)), GITHUB_BASE)
        endpoints = [call.args[0] for call in self.get_json.call_args_list]
        self.assertNotIn(f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}", endpoints)

    def test_parent_merge_into_other_target_does_not_select_github_base(self) -> None:
        self.responses[f"repos/{REPOSITORY}/pulls/1"].update(
            merged_at="2026-10-21T00:00:00Z", merge_commit_sha=MERGE
        )
        self.responses[f"repos/{REPOSITORY}/compare/{MERGE}...{HEAD}"] = _comparison(MERGE)
        self.responses[f"repos/{REPOSITORY}/compare/{MERGE}...{GITHUB_BASE}"] = _comparison(
            FORK, "diverged"
        )
        self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)")), PARENT)

    def test_rewritten_parent_with_only_base_branch_history_stops_review(self) -> None:
        self.responses[f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}"] = _comparison(
            FORK, "diverged"
        )
        self.responses[f"repos/{REPOSITORY}/compare/{GITHUB_BASE}...{FORK}"] = _comparison(
            FORK, "behind"
        )
        with self.assertRaisesRegex(ValueError, "no shared changes"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))

    def test_parent_containing_child_head_stops_empty_review(self) -> None:
        self.responses[f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}"] = _comparison(
            HEAD, "behind"
        )
        with self.assertRaisesRegex(ValueError, "no child changes"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))

    def test_stack_header_variants_and_horizontal_rule_are_supported(self) -> None:
        for header in ("## Stacked PR", "## Stacked PRs", "**Stacked PR**", "## 🔗 Stacked PR"):
            for rule in ("---------", "* * *", "___"):
                body = _stack_body(f"[parent]({PR_URL}/1)").replace("## Stacked PR", header)
                body += f"\n{rule}\n- [related]({PR_URL}/2)"
                with self.subTest(header=header, rule=rule):
                    self.assertEqual(self.resolve(body), PARENT)

    def test_unrelated_headings_and_fenced_examples_leave_ordinary_scope(self) -> None:
        for body in (
            "## Not a stacked PR",
            "## Stacked PR migration notes",
            f"```markdown\n{_stack_body()}\n```",
            f"~~~~markdown\n~~~\n{_stack_body()}\n~~~~",
        ):
            with self.subTest(body=body):
                self.assertEqual(self.resolve(body), GITHUB_BASE)
        self.get_json.assert_not_called()

    def test_fenced_example_does_not_override_real_stack(self) -> None:
        body = f"```markdown\n{_stack_body()}\n```\n" + _stack_body(f"[parent]({PR_URL}/1)")
        self.assertEqual(self.resolve(body), PARENT)

    def test_ambiguous_missing_and_unlinked_entries_stop_review(self) -> None:
        body = _stack_body(f"[parent]({PR_URL}/1)")
        cases = (
            (body.replace("  - [**stack/child", "- [**stack/child"), "ambiguous flat"),
            (body + "\n" + body, "multiple stack entries"),
            (body.replace(f"{PR_URL}/2)", f"{PR_URL}/20)"), "no entry"),
            (_stack_body("unlinked parent", PARENT), "no PR link"),
            ("```\n" + body, "unclosed code fence"),
        )
        for listing, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.resolve(listing)
        self.get_json.assert_not_called()

    def test_repository_and_metadata_sha_guards_stop_before_api_calls(self) -> None:
        for repository, field, value in (
            ("../other/repo", "head", HEAD),
            (REPOSITORY, "head", "invalid"),
            (REPOSITORY, "base", None),
        ):
            metadata = _metadata()
            metadata[field]["sha"] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "invalid"):
                self.module.resolve_review_base(metadata, repository, self.get_json)
        self.get_json.assert_not_called()

    def test_invalid_parent_and_comparison_responses_stop_review(self) -> None:
        for endpoint, response in (
            (f"repos/{REPOSITORY}/pulls/1", {"head": {"sha": "bad"}}),
            (f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}", {}),
            (f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}", _comparison("bad")),
            (f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}", _comparison(PARENT, "unknown")),
        ):
            with self.subTest(response=response):
                original = self.responses[endpoint]
                self.responses[endpoint] = response
                with self.assertRaises((ValueError, KeyError)):
                    self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))
                self.responses[endpoint] = original

    def test_cli_formats_api_and_missing_field_errors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "meta.json"
            path.write_text(json.dumps(_metadata(_stack_body(f"[parent]({PR_URL}/1)"))))
            arguments = ["review_scope.py", "--metadata", str(path), "--repository", REPOSITORY]
            for response in (
                {"side_effect": subprocess.CalledProcessError(1, ["gh", "api"])},
                {"return_value": {}},
            ):
                with self.subTest(response=response), patch.object(sys, "argv", arguments):
                    with patch.object(self.module, "_github_json", **response):
                        with self.assertRaisesRegex(SystemExit, "Cannot resolve PR review scope"):
                            self.module._main()

    def test_workflow_diff_and_all_reviewer_prompts_use_the_resolved_scope(self) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        workflow = yaml.safe_load((repo_root / ".github/workflows/ai-review.yml").read_text())
        context_step = next(
            step
            for job in workflow["jobs"].values()
            for step in job["steps"]
            if step.get("id") == "context"
        )
        # Execute the actual workflow from scope resolution through prompt generation.
        script = "read -r base_sha" + context_step["run"].split("read -r base_sha", 1)[1]
        cases = (
            ("", GITHUB_BASE),
            (_stack_body(f"[parent]({PR_URL}/1)", PARENT), PARENT),
            (
                _stack_body(f"[parent]({PR_URL}/1)", PARENT).replace(f"..{HEAD}", f"..{FORK}"),
                PARENT,
            ),
            (_stack_body("unlinked parent", PARENT), None),
        )
        for body, review_base in cases:
            with self.subTest(review_base=review_base), tempfile.TemporaryDirectory() as directory:
                temp = Path(directory)
                (temp / "pr_meta.json").write_text(json.dumps(_metadata(body)))
                (temp / "pr_history.json").write_text("{}")
                (temp / "pr_review_comments.json").write_text("[]")
                (temp / "github-output").touch()
                (temp / "python3").symlink_to(sys.executable)
                responses = json.dumps(self.responses)
                gh = temp / "gh"
                gh.write_text(
                    f"#!{sys.executable}\n"
                    "import json, os, pathlib, sys\n"
                    "root = pathlib.Path(os.environ['TEST_ROOT'])\n"
                    "with (root / 'api-calls').open('a') as calls:\n"
                    "    calls.write(sys.argv[2] + '\\n')\n"
                    "if '--method' in sys.argv:\n"
                    "    raise SystemExit(0)\n"
                    "if '-H' in sys.argv:\n"
                    "    diff = ('diff --git a/child.rs b/child.rs\\n'\n"
                    "            '--- a/child.rs\\n+++ b/child.rs\\n'\n"
                    "            '@@ -0,0 +1 @@\\n+child_change();')\n"
                    f"    if '/compare/{GITHUB_BASE}...' in sys.argv[2]:\n"
                    "        diff += '\\n+ancestor_change();'\n"
                    "    print(diff)\n"
                    "else:\n"
                    f"    print(json.dumps(json.loads({responses!r})[sys.argv[2]]))\n"
                )
                gh.chmod(0o700)
                result = subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", script.replace("/tmp/", f"{temp}/")],
                    cwd=repo_root,
                    env={
                        **os.environ,
                        "PATH": f"{temp}:{os.environ['PATH']}",
                        "TEST_ROOT": str(temp),
                        "GITHUB_OUTPUT": str(temp / "github-output"),
                        "OUTPUT_MODE": "inline",
                        "REPO": REPOSITORY,
                    },
                    capture_output=True,
                    text=True,
                )
                if review_base is None:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("::error::Cannot select", result.stdout)
                    self.assertIn("no PR link", (temp / "check-summary.txt").read_text())
                    self.assertIn(f"head_sha={HEAD}", (temp / "github-output").read_text())
                    self.assertFalse((temp / "api-calls").exists())
                    self.assertFalse((temp / "review_prompt.txt").exists())
                    check_step = next(
                        step
                        for job in workflow["jobs"].values()
                        for step in job["steps"]
                        if step.get("name") == "Publish non-blocking PR check"
                    )
                    self.assertIn("steps.context.outputs.head_sha != ''", check_step["if"])
                    check = subprocess.run(
                        [
                            "bash",
                            "-euo",
                            "pipefail",
                            "-c",
                            check_step["run"].replace("/tmp/", f"{temp}/"),
                        ],
                        cwd=repo_root,
                        env={
                            **os.environ,
                            "PATH": f"{temp}:{os.environ['PATH']}",
                            "TEST_ROOT": str(temp),
                            "BOT_TOKEN": "",
                            "REVIEW_READY": "false",
                            "HEAD_SHA": HEAD,
                            "PR_NUMBER": "2",
                            "REPO": REPOSITORY,
                            "RUN_URL": f"https://github.com/{REPOSITORY}/actions/runs/1",
                        },
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(check.returncode, 0, check.stderr)
                    payload = json.loads((temp / "check-run.json").read_text())
                    self.assertEqual(payload["head_sha"], HEAD)
                    self.assertEqual(payload["conclusion"], "neutral")
                    self.assertIn("no PR link", payload["output"]["summary"])
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (temp / "api-calls").read_text().splitlines()
                self.assertTrue(calls)
                self.assertEqual(
                    {call for call in calls if "/compare/" in call and call.endswith(f"...{HEAD}")},
                    {
                        f"repos/{REPOSITORY}/compare/{review_base}...{HEAD}",
                        f"repos/{REPOSITORY}/compare/{GITHUB_BASE}...{HEAD}",
                    },
                )
                output = (temp / "github-output").read_text()
                self.assertIn(f"base_sha={GITHUB_BASE}\n", output)
                self.assertIn(f"review_base_sha={review_base}\n", output)
                for name in ("review_prompt.txt", "reviewer_context.txt"):
                    prompt = (temp / name).read_text()
                    self.assertIn(f"**Review base SHA:** {review_base}", prompt)
                    self.assertIn(f"**GitHub base SHA:** {GITHUB_BASE}", prompt)
                    if review_base != GITHUB_BASE:
                        self.assertIn(
                            "Changes inherited from ancestor PRs are context only", prompt
                        )
                        self.assertIn("Check effects on unchanged callers", prompt)
                        if name == "reviewer_context.txt":
                            self.assertLess(
                                prompt.index("## Review scope"),
                                prompt.index("untrusted review data"),
                            )
                    else:
                        self.assertNotIn("## Review scope", prompt)
                    self.assertIn("+child_change();", prompt)
                    if review_base != GITHUB_BASE:
                        self.assertNotIn("+ancestor_change();", prompt)
                self.assertIn("+ancestor_change();", (temp / "github_pr_diff.txt").read_text())


if __name__ == "__main__":
    unittest.main()
