#!/usr/bin/env python3
"""Build the AI PR review prompt from PR metadata, diff, and review policy."""

import argparse
import json
from pathlib import Path


def build_prompt(
    meta_path: Path,
    diff_path: Path,
    policy_path: Path,
    max_diff_chars: int,
) -> str:
    meta = json.loads(meta_path.read_text())
    review_policy = policy_path.read_text()
    body = (meta.get("body") or "")[:4096]
    diff = diff_path.read_text(errors="replace")
    truncated = len(diff) > max_diff_chars
    diff = diff[:max_diff_chars]
    truncation_note = (
        f"\n\n[Diff truncated at {max_diff_chars} characters by the prompt builder. "
        "Focus on the visible diff and say if full context is required.]"
        if truncated
        else ""
    )

    stats = (
        f"+{meta['additions']} / -{meta['deletions']} across "
        f"{meta['changedFiles']} file(s)"
    )

    return f"""Orchestrate a review of this pull request.

## PR Metadata
- **Title:** {meta['title']}
- **Branch:** {meta['headRefName']} -> {meta['baseRefName']}
- **Stats:** {stats}

## PR Description
{body}

## Review Policy
{review_policy}

## PR Diff

```diff
{diff}
```{truncation_note}
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--meta", type=Path, default=Path("/tmp/pr_meta.json"))
    parser.add_argument("--diff", type=Path, default=Path("/tmp/pr_diff.txt"))
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path(".github/omnigent/reviewer/REVIEW_POLICY.md"),
    )
    parser.add_argument("--out", type=Path, default=Path("/tmp/review_prompt.txt"))
    parser.add_argument("--max-diff-chars", type=int, default=250_000)
    args = parser.parse_args()

    args.out.write_text(
        build_prompt(args.meta, args.diff, args.policy, args.max_diff_chars)
    )


if __name__ == "__main__":
    main()
