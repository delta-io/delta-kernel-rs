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


def _load_module():
    path = Path(__file__).parents[1] / "review_scope.py"
    spec = importlib.util.spec_from_file_location("review_scope", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
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


class ReviewScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()
        self.get_json = Mock(
            return_value={"status": "ahead", "merge_base_commit": {"sha": PARENT}}
        )

    def resolve(self, body: str) -> str:
        return self.module.resolve_review_base(_metadata(body), REPOSITORY, self.get_json)

    def test_ordinary_pr_and_stack_root_keep_github_base(self) -> None:
        for body in ("", _stack_body(), f"Related PR: {PR_URL}/1"):
            with self.subTest(body=body):
                self.assertEqual(self.resolve(body), GITHUB_BASE)
        self.get_json.assert_not_called()

    def test_pinned_parent_excludes_ancestor_pr_even_when_github_base_is_main(self) -> None:
        body = _stack_body(f"[stack/parent]({PR_URL}/1)", PARENT)

        self.assertEqual(self.resolve(body), PARENT)
        self.get_json.assert_called_once_with(f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}")

    def test_pinned_boundary_supports_unlinked_parent_and_renamed_branch(self) -> None:
        body = _stack_body("stack/unlinked-parent", PARENT)
        body = body.replace("**stack/child**", "**#2: child change**")

        self.assertEqual(self.resolve(body), PARENT)

    def test_parent_lookup_skips_siblings_and_uses_immediate_ancestor(self) -> None:
        body = (
            "## Stacked PR\n"
            f"- [stack/root]({PR_URL}/4)\n"
            f"  - [stack/parent]({PR_URL}/1)\n"
            f"    - [stack/sibling]({PR_URL}/5)\n"
            f"    - [**stack/child**]({PR_URL}/2) <- _this PR_\n"
        )
        for listing in (body, body.replace("- ", "* ").replace("  ", "\t")):
            with self.subTest(listing=listing):
                self.get_json.reset_mock()
                self.get_json.side_effect = [
                    {"head": {"sha": PARENT}, "merged_at": None},
                    {"status": "ahead", "merge_base_commit": {"sha": PARENT}},
                ]
                self.assertEqual(self.resolve(listing), PARENT)
                self.assertEqual(
                    self.get_json.call_args_list[0].args, (f"repos/{REPOSITORY}/pulls/1",)
                )

    def test_squash_merged_parent_is_excluded_until_child_is_rebased(self) -> None:
        for status, merge_base, expected in (
            ("ahead", PARENT, PARENT),
            ("diverged", "d" * 40, GITHUB_BASE),
        ):
            with self.subTest(status=status):
                self.get_json.reset_mock()
                self.get_json.side_effect = [
                    {"head": {"sha": PARENT}, "merged_at": "2026-10-21T00:00:00Z"},
                    {"status": status, "merge_base_commit": {"sha": merge_base}},
                ]
                self.assertEqual(self.resolve(_stack_body(f"[parent]({PR_URL}/1)")), expected)
                self.assertEqual(
                    self.get_json.call_args_list[1].args,
                    (f"repos/{REPOSITORY}/compare/{PARENT}...{HEAD}",),
                )

    def test_other_sections_and_descendant_comparisons_do_not_change_scope(self) -> None:
        body = _stack_body() + (
            f" [[Files changed]({PR_URL}/3/files/{PARENT}..{HEAD})]\n"
            "## Examples\n"
            f"- [stack/parent]({PR_URL}/1)\n"
            f"  - [stack/child]({PR_URL}/2)\n"
        )

        self.assertEqual(self.resolve(body), GITHUB_BASE)
        self.get_json.assert_not_called()

    def test_stack_header_with_prefix_is_supported(self) -> None:
        body = _stack_body("stack/parent", PARENT).replace(
            "## Stacked PR", "## \U0001f95e Stacked PR"
        )

        self.assertEqual(self.resolve(body), PARENT)

    def test_unrelated_headings_and_fenced_examples_leave_ordinary_pr_scope(self) -> None:
        example = _stack_body("parent", PARENT)
        for body in (
            "## Not a stacked PR\nThis change stands alone.",
            "## Stacked PR migration notes\nRelated work.",
            f"```markdown\n{example}\n```",
            f"~~~~markdown\n~~~\n{example}\n~~~~",
        ):
            with self.subTest(body=body):
                self.assertEqual(self.resolve(body), GITHUB_BASE)
        self.get_json.assert_not_called()

    def test_fenced_stack_example_does_not_override_the_real_boundary(self) -> None:
        body = f"```markdown\n{_stack_body('example', 'd' * 40)}\n```\n"
        body += _stack_body("actual parent", PARENT)

        self.assertEqual(self.resolve(body), PARENT)

    def test_repository_and_metadata_sha_guards_stop_before_api_calls(self) -> None:
        for repository, field, value in (
            ("../other/repo", "head", HEAD),
            (REPOSITORY, "head", "not-a-sha"),
            (REPOSITORY, "base", "not-a-sha"),
        ):
            with self.subTest(repository=repository, field=field):
                metadata = _metadata()
                metadata[field]["sha"] = value
                with self.assertRaisesRegex(ValueError, "invalid"):
                    self.module.resolve_review_base(metadata, repository, self.get_json)
        self.get_json.assert_not_called()

    def test_invalid_parent_head_stops_before_comparison(self) -> None:
        self.get_json.return_value = {"head": {"sha": "not-a-sha"}}

        with self.assertRaisesRegex(ValueError, "invalid stack parent SHA"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))
        self.get_json.assert_called_once_with(f"repos/{REPOSITORY}/pulls/1")

    def test_identical_boundary_and_three_dot_comparison_are_supported(self) -> None:
        body = _stack_body("parent", PARENT).replace(f"{PARENT}..", f"{PARENT}...")
        self.assertEqual(self.resolve(body), PARENT)

        self.get_json.return_value = {"status": "identical", "merge_base_commit": {"sha": HEAD}}
        self.assertEqual(self.resolve(_stack_body("parent", HEAD)), HEAD)

    def test_cli_formats_api_and_missing_response_field_errors(self) -> None:
        responses = (
            {"side_effect": subprocess.CalledProcessError(1, ["gh", "api"])},
            {"return_value": {}},
        )
        with tempfile.TemporaryDirectory() as directory:
            metadata_path = Path(directory) / "meta.json"
            metadata_path.write_text(json.dumps(_metadata(_stack_body(f"[parent]({PR_URL}/1)"))))
            arguments = [
                "review_scope.py", "--metadata", str(metadata_path), "--repository", REPOSITORY
            ]
            for response in responses:
                with self.subTest(response=response):
                    with patch.object(sys, "argv", arguments):
                        with patch.object(self.module, "_github_json", **response):
                            with self.assertRaisesRegex(
                                SystemExit, "Cannot resolve PR review scope"
                            ):
                                self.module._main()

    def test_stale_malformed_and_missing_boundaries_stop_review(self) -> None:
        valid = _stack_body("stack/parent", PARENT)
        cases = (
            (valid.replace(f"..{HEAD}", f"..{'d' * 40}"), "stale or invalid"),
            (valid.replace(f"{PARENT}..", "not-a-sha.."), "stale or invalid"),
            (_stack_body("stack/parent"), "no PR or pinned comparison"),
            (valid + "\n" + valid, "multiple stack entries"),
            (valid.replace(f"{PR_URL}/2)", f"{PR_URL}/20)"), "no entry for the current PR"),
        )
        for body, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                self.resolve(body)
        self.get_json.assert_not_called()

    def test_non_ancestor_parent_stops_review(self) -> None:
        for comparison in (
            {"status": "diverged", "merge_base_commit": {"sha": GITHUB_BASE}},
            {"status": "ahead", "merge_base_commit": {"sha": GITHUB_BASE}},
            {},
        ):
            with self.subTest(comparison=comparison):
                self.get_json.return_value = comparison
                with self.assertRaisesRegex(ValueError, "not an ancestor"):
                    self.resolve(_stack_body("stack/parent", PARENT))

    def test_parent_api_failure_does_not_fall_back_to_cumulative_diff(self) -> None:
        self.get_json.side_effect = RuntimeError("GitHub unavailable")

        with self.assertRaisesRegex(RuntimeError, "GitHub unavailable"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))

    def test_merged_parent_with_unknown_ancestry_stops_review(self) -> None:
        self.get_json.side_effect = [
            {"head": {"sha": PARENT}, "merged_at": "2026-10-21T00:00:00Z"},
            {},
        ]

        with self.assertRaisesRegex(ValueError, "not an ancestor"):
            self.resolve(_stack_body(f"[parent]({PR_URL}/1)"))

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
            (_stack_body("parent", PARENT), PARENT),
            (_stack_body("parent", PARENT).replace(f"..{HEAD}", f"..{'d' * 40}"), None),
        )
        for body, review_base in cases:
            with self.subTest(review_base=review_base), tempfile.TemporaryDirectory() as directory:
                temp = Path(directory)
                (temp / "pr_meta.json").write_text(json.dumps(_metadata(body)))
                (temp / "pr_history.json").write_text("{}")
                (temp / "pr_review_comments.json").write_text("[]")
                (temp / "github-output").touch()
                (temp / "python3").symlink_to(sys.executable)
                comparison_response = json.dumps(
                    {"status": "ahead", "merge_base_commit": {"sha": PARENT}}
                )
                gh = temp / "gh"
                gh.write_text(
                    f"#!{sys.executable}\n"
                    "import os, pathlib, sys\n"
                    "root = pathlib.Path(os.environ['TEST_ROOT'])\n"
                    "with (root / 'api-calls').open('a') as calls:\n"
                    "    calls.write(sys.argv[2] + '\\n')\n"
                    "if '-H' in sys.argv:\n"
                    "    diff = ('diff --git a/child.rs b/child.rs\\n'\n"
                    "            '--- a/child.rs\\n+++ b/child.rs\\n'\n"
                    "            '@@ -0,0 +1 @@\\n+child_change();')\n"
                    f"    if '/compare/{GITHUB_BASE}...' in sys.argv[2]:\n"
                    "        diff += '\\n+ancestor_change();'\n"
                    "    print(diff)\n"
                    "else:\n"
                    f"    print({comparison_response!r})\n"
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
                    self.assertIn("stale or invalid", result.stderr)
                    self.assertFalse((temp / "api-calls").exists())
                    self.assertFalse((temp / "review_prompt.txt").exists())
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (temp / "api-calls").read_text().splitlines()
                self.assertTrue(calls)
                self.assertEqual(
                    set(calls),
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
                    self.assertIn("Review only defects introduced by the supplied diff", prompt)
                    self.assertIn("author supplied and verified only for ancestry", prompt)
                    self.assertIn("+child_change();", prompt)
                    if review_base != GITHUB_BASE:
                        self.assertNotIn("+ancestor_change();", prompt)
                self.assertIn("+ancestor_change();", (temp / "github_pr_diff.txt").read_text())


if __name__ == "__main__":
    unittest.main()
