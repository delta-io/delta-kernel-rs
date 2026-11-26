"""Exercise the publication workflow step through its CLI and authenticated transport boundary."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


class PublicationWorkflowTest(unittest.TestCase):
    def test_inline_rejection_preserves_finding_and_links_main_comment_to_published_check(self):
        repository = Path(__file__).resolve().parents[3]
        workflow = yaml.safe_load((repository / ".github/workflows/ai-review.yml").read_text())
        step = next(
            step for job in workflow["jobs"].values() for step in job.get("steps", [])
            if step.get("name") == "Publish review findings and summary"
        )
        check_step = next(
            step for job in workflow["jobs"].values() for step in job.get("steps", [])
            if step.get("name") == "Publish non-blocking PR check"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.json"
            state.write_text("[]")
            gh = root / "gh"
            gh.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, sys\n"
                "state = pathlib.Path(os.environ['FAKE_STATE'])\n"
                "rows = json.loads(state.read_text())\n"
                "i = sys.argv.index('--method')\n"
                "method, path = sys.argv[i + 1:i + 3]\n"
                "if method == 'GET':\n"
                "    if '/issues/comments/' in path:\n"
                "        result = next(r for r in rows if str(r['id']) == path.rsplit('/', 1)[1])\n"
                "    else:\n"
                "        result = rows if '/issues/' in path else []\n"
                "elif '/pulls/' in path:\n"
                "    print('HTTP 422', file=sys.stderr)\n"
                "    sys.exit(1)\n"
                "else:\n"
                "    source = sys.argv[sys.argv.index('--input') + 1]\n"
                "    payload = (json.load(sys.stdin) if source == '-' else\n"
                "               json.loads(pathlib.Path(source).read_text()))\n"
                "    if path.endswith('/check-runs'):\n"
                "        result = dict(payload, id=123,\n"
                "            html_url='https://github.com/example/project/runs/123')\n"
                "    elif method == 'PATCH':\n"
                "        result = next(r for r in rows if str(r['id']) == path.rsplit('/', 1)[1])\n"
                "        result.update(payload)\n"
                "    else:\n"
                "        n = len(rows) + 1\n"
                "        result = dict(payload, id=n, user={'login':'github-actions[bot]', 'type':'Bot'},\n"
                "            html_url=f'https://github.com/example/project/pull/1#issuecomment-{n}')\n"
                "        rows.append(result)\n"
                "    state.write_text(json.dumps(rows))\n"
                "print(json.dumps(result))\n"
            )
            gh.chmod(0o755)
            (root / "review_output.txt").write_text("## PR Summary\nChanges a call.\n")
            (root / "inline_findings.json").write_text(json.dumps([{
                "id": "ShouldFix1", "path": "file.rs", "line": 1, "side": "RIGHT",
                "body": "Derive the mode once.\n\nSuggested fix: reuse the selected mode.",
            }]))
            diff = (
                "diff --git a/file.rs b/file.rs\n--- a/file.rs\n+++ b/file.rs\n"
                "@@ -1 +1 @@\n-old\n+new\n"
            )
            (root / "diff_coverage.json").write_text(json.dumps({
                "base_sha": "b" * 40, "head_sha": "a" * 40, "complete": True,
                "total_bytes": len(diff), "excerpt_bytes": len(diff),
                "total_files": 1, "full_files": 1, "partial_files": 0, "omitted_files": 0,
                "files": [{"path": "file.rs", "start": 0, "end": len(diff), "status": "full"}],
            }))
            for name in ("pr_diff.txt", "github_pr_diff.txt"):
                (root / name).write_text(diff)
            (root / "trusted_bot_logins.json").write_text('["github-actions"]')
            result = subprocess.run(
                ["bash", "-c", step["run"].replace("/tmp/", f"{root}/")],
                cwd=repository, capture_output=True, text=True,
                env={
                    **os.environ, "PATH": f"{root}:{os.environ['PATH']}",
                    "FAKE_STATE": str(state), "HEAD_SHA": "a" * 40,
                    "PR_NUMBER": "1", "REPO": "example/project", "OUTPUT_MODE": "inline",
                    "FINDING_PLACEMENT": step["env"]["FINDING_PLACEMENT"],
                    "RUN_URL": "https://github.com/example/project/actions/runs/1",
                },
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            record = json.loads((root / "publication_record.json").read_text())
            self.assertTrue(record["complete"], record["errors"])
            self.assertEqual(record["findings"][0]["placement"], "conversation")
            self.assertEqual(len(json.loads(state.read_text())), 2)
            self.assertIn("Derive the mode once", (root / "review_output_complete.txt").read_text())
            self.assertIn("Derive the mode once", (root / "check-summary.txt").read_text())
            check = subprocess.run(
                ["bash", "-c", check_step["run"].replace("/tmp/", f"{root}/")],
                cwd=repository, capture_output=True, text=True,
                env={
                    **os.environ, "PATH": f"{root}:{os.environ['PATH']}",
                    "FAKE_STATE": str(state), "HEAD_SHA": "a" * 40,
                    "PR_NUMBER": "1", "REPO": "example/project", "BOT_TOKEN": "",
                    "GH_TOKEN": "test-token", "REVIEW_READY": "true",
                    "RUN_URL": "https://github.com/example/project/actions/runs/1",
                },
            )
            self.assertEqual(check.returncode, 0, check.stdout + check.stderr)
            comments = json.loads(state.read_text())
            main = next(item for item in comments if item["id"] == record["main_comment_id"])
            self.assertIn(
                "[Full AI review](https://github.com/example/project/runs/123)", main["body"]
            )
            self.assertNotIn("https://github.com/example/project/pull/1/checks", main["body"])
            self.assertEqual(len(comments), 2)


if __name__ == "__main__":
    unittest.main()
