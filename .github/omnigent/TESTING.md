# Testing Omnigent before merge

Use a manual `workflow_dispatch` run to test changes to the workflow, reviewer instructions,
Python helpers or dependency locks. Normal PR and `/review` events use the default branch's
reviewer. Opening a PR for reviewer changes therefore does not exercise those changes.

The [reviewer README](reviewer/README.md) defines the architecture, terminology,
[review range](reviewer/README.md#pr-change-patch-and-stacked-prs),
[input limits](reviewer/README.md#input-limits) and
[output destinations](reviewer/README.md#github-comments-checks-and-saved-reports).

## Choose the behavior and fixture

Before running commands, record:

- The candidate PR and its exact head SHA: the workflow, instructions and dependencies to test.
  When testing one PR in a stack, use that PR's own head, including its required ancestors.
  A later stack tip tests a different candidate, even if it contains the same PR's changes.
- The target fixture PR, its expected head SHA, and the expected review base or failure condition.
  This is the source being reviewed; it can be a different PR from the candidate.
- The output mode and expected result, including which publication paths must execute.

Choose a fixture that exercises the changed behavior:

| Behavior | Fixture and mode | Expected result |
| --- | --- | --- |
| Setup, model execution or dependencies | A PR with a nonempty diff; `artifact` | Actual model execution and a validated report. |
| Finding publication | A designated PR with a finding that exercises the changed destination; `inline`, `comment` or `collapsed` | The intended review/comments, or the expected fallback or duplicate suppression. If no finding is produced, the finding-publication path remains untested. |
| Stack boundary selection | A child PR with a known immediate parent; the mode needed for the change | A selected review range that excludes inherited parent changes. |
| Ambiguous-stack diagnostics | The scope tests, or a designated PR with ambiguous ancestry | Resolution fails before model execution, and a live run's Check Run explains the boundary problem. |
| Truncation | A diff larger than the candidate's patch-excerpt limit; `artifact` for direct input inspection | The expected supplied bytes and truncation warning. A run below the limit verifies the untruncated path only. |
| Actions job summary | A nonempty target; `summary` | The validated report in the Actions job summary. |

Keep the target unchanged during the run when possible. Dispatch takes a PR number, not a pinned
target SHA; the workflow reads its current head during input collection. Coordinate with other
testers: a newer review of the same PR can cancel an earlier run across workflow branches.

## Run local checks

Use the checkout of the recorded candidate, rather than another stack branch. Run the same Python
checks as the `ai-review-config` CI job. That job uses Python 3.12 and `PyYAML==6.0.3`; install
PyYAML in your Python environment first.

```bash
python3 -m unittest discover \
  --start-directory .github/omnigent/tests --pattern 'test_*.py'
```

These tests do not call models. Commit the candidate before live testing; the dispatched workflow
cannot see uncommitted files. The remaining commands use Bash and an authenticated GitHub CLI.

## Publish the candidate

Use an account with write, maintain or admin permission on `delta-io/delta-kernel-rs`.
`AI_REVIEW_ALLOWLIST` does not authorize manual dispatches. The candidate must exist in the
canonical repository; a fork-only branch cannot be selected there. Maintainers must inspect the
candidate before running it with repository credentials.

The repository needs secrets `LLM_API_KEY` and `GATEWAY_BASE_URL`, plus variables `GATEWAY_HOST`,
`MODEL`, `CLAUDE_MAINTAINER_MODEL`, `CODEX_MAINTAINER_MODEL` and `DISPROVE_MODEL`.
`GATEWAY_HOST` is the bare gateway hostname used by the egress allowlist. A fork does not inherit
this configuration. Optional GitHub App settings are listed in the
[workflow header](../workflows/ai-review.yml).

Set the following values from the test case recorded above. In particular, copy the candidate
PR's full head SHA into `AI_REVIEW_SHA`; do not derive it from whichever branch is checked out.
Set `AI_REVIEW_REMOTE` to the remote whose push URL points to `delta-io/delta-kernel-rs`.
Git-over-SSH authentication is separate from the CLI account; verify that the remote uses an
identity allowed to push workflow changes.

```bash
AI_REVIEW_ACCOUNT=your-maintainer-login
AI_REVIEW_REPO=delta-io/delta-kernel-rs
AI_REVIEW_REMOTE=upstream
AI_REVIEW_SHA=replace-with-full-candidate-pr-head-sha
AI_REVIEW_PR=1234  # Replace with the fixture PR to review.
AI_REVIEW_PR_SHA=replace-with-expected-fixture-head-sha
AI_REVIEW_MODE=artifact  # Use the mode chosen for this test case.
AI_REVIEW_REF="ci/ai-review-preview-${AI_REVIEW_SHA:0:12}-$$"

ai_review_gh() {
  local ai_review_token
  ai_review_token="$(gh auth token --user "$AI_REVIEW_ACCOUNT")" || return 1
  if [ -z "$ai_review_token" ]; then
    printf 'No token for %s\n' "$AI_REVIEW_ACCOUNT" >&2
    return 1
  fi
  GH_TOKEN="$ai_review_token" gh "$@"
}

ai_review_gh api user --jq .login
ai_review_gh api "repos/$AI_REVIEW_REPO" --jq .permissions
git remote get-url --push "$AI_REVIEW_REMOTE"
git status --short
test "$(git rev-parse HEAD)" = "$AI_REVIEW_SHA"
test "$(ai_review_gh api \
  "repos/$AI_REVIEW_REPO/pulls/$AI_REVIEW_PR" --jq .head.sha)" = "$AI_REVIEW_PR_SHA"
```

Resolve any mismatch before proceeding. Use a new preview ref; if the selected name already
exists, choose another. The empty expected value in this lease protects an existing ref from
being moved to your candidate.

```bash
git push --force-with-lease="refs/heads/$AI_REVIEW_REF:" \
  "$AI_REVIEW_REMOTE" "$AI_REVIEW_SHA:refs/heads/$AI_REVIEW_REF"

test "$(ai_review_gh api \
  "repos/$AI_REVIEW_REPO/git/ref/heads/$AI_REVIEW_REF" \
  --jq .object.sha)" = "$AI_REVIEW_SHA"
```

Keep the preview ref unchanged for the test. Record the workflow SHA, reviewed PR head and review
base separately; they identify different parts of the test.

## Dispatch

Every live model run spends model quota and can publish a neutral `AI Review Summary` Check Run.
The chosen output mode controls the other outputs. Artifact mode uploads report files; inline,
comment and collapsed modes publish review/comment output; summary mode writes the Actions job
summary. Use a designated test PR when publication writes are part of the test.

```bash
ai_review_gh workflow run ai-review.yml --repo "$AI_REVIEW_REPO" \
  --ref "$AI_REVIEW_REF" -f pr="$AI_REVIEW_PR" -f output_mode="$AI_REVIEW_MODE"

ai_review_gh run list --repo "$AI_REVIEW_REPO" --workflow ai-review.yml \
  --branch "$AI_REVIEW_REF" --event workflow_dispatch --limit 5 \
  --json databaseId,headSha,url,status,conclusion
```

Select the new run whose `headSha` matches `AI_REVIEW_SHA`. It may take a moment to appear.
Use that run ID for every subsequent command, rather than selecting the latest run again.

## Verify common results

Collect run metadata and logs in every output mode:

```bash
AI_REVIEW_RUN=123456789  # Replace with the matching run's databaseId.
AI_REVIEW_EVIDENCE="$(mktemp -d)"

ai_review_gh run watch "$AI_REVIEW_RUN" --repo "$AI_REVIEW_REPO" --exit-status
ai_review_gh run view "$AI_REVIEW_RUN" --repo "$AI_REVIEW_REPO" \
  --json url,event,headBranch,headSha,status,conclusion,jobs \
  > "$AI_REVIEW_EVIDENCE/run.json"
ai_review_gh run view "$AI_REVIEW_RUN" --repo "$AI_REVIEW_REPO" --log \
  > "$AI_REVIEW_EVIDENCE/run.log"
```

If the run fails, still collect its metadata and logs. Check these against the expected result:

- The event is `workflow_dispatch`, and `headSha` and the checkout log match the candidate SHA.
- The actual reviewed head and selected review base match the planned fixture and range. The
  `Materialize PR source reference` step exposes its `HEAD_SHA` in the log, and the scope-resolution
  log explains the selected boundary. Artifact mode also records both SHAs in its saved task text.
  If the fixture moved, record the actual head and assess whether the intended case was exercised.
- For a successful model test, `Install Omnigent`, `Validate AI reviewer runtime`, `Run AI review`
  and `Validate review output` executed successfully. A green run with missing credentials or a
  replay that skips the model step does not establish a live model test.
- For an expected ambiguous-ancestry failure, resolution stops before model execution and the
  Check Run explains the boundary problem. A missing report is expected in this case.

Use the actual reviewed head in `AI_REVIEW_PR_SHA` when querying its checks:

```bash
ai_review_gh api --paginate --slurp \
  "repos/$AI_REVIEW_REPO/commits/$AI_REVIEW_PR_SHA/check-runs" \
  > "$AI_REVIEW_EVIDENCE/checks.json"
```

Find the `AI Review Summary` check whose summary links to this workflow run. Confirm its target
SHA and report or expected diagnostic. A neutral conclusion alone does not establish success.
Inspect the summary's workflow link as well as the check URL when correlating runs.

For tests expecting a validated report, also verify the outputs for the selected mode below.
Successful non-artifact runs do not upload the report artifact.

### Artifact mode

```bash
ai_review_gh run download "$AI_REVIEW_RUN" --repo "$AI_REVIEW_REPO" \
  --name "ai-review-$AI_REVIEW_PR-$AI_REVIEW_RUN" \
  --dir "$AI_REVIEW_EVIDENCE/report"
```

Read `review_output.txt` for the validated report and `child_task_pr_data.md` for the actual
`Head SHA` and `Review base SHA`. Compare `pr_diff.txt` with the patch excerpt in the task text.
For a truncation test, verify the supplied byte count and warning against the candidate's
[input limits](reviewer/README.md#input-limits). A below-limit run establishes the untruncated
path only. Passing output validation does not establish full input coverage or completion of
every reviewer; inspect any completion or coverage records the candidate emits.

### Inline, comment and collapsed modes

Use the logs, the correlated Check Run and the actual GitHub review/comments as evidence.
Collect the relevant objects:

```bash
ai_review_gh api --paginate --slurp \
  "repos/$AI_REVIEW_REPO/pulls/$AI_REVIEW_PR/reviews?per_page=100" \
  > "$AI_REVIEW_EVIDENCE/reviews.json"
ai_review_gh api --paginate --slurp \
  "repos/$AI_REVIEW_REPO/pulls/$AI_REVIEW_PR/comments?per_page=100" \
  > "$AI_REVIEW_EVIDENCE/inline-comments.json"
ai_review_gh api --paginate --slurp \
  "repos/$AI_REVIEW_REPO/issues/$AI_REVIEW_PR/comments?per_page=100" \
  > "$AI_REVIEW_EVIDENCE/conversation-comments.json"
```

Match the run URL, timestamps, reviewed commit and review/comment IDs to this run. In inline mode,
inspect the `COMMENT` review body and its line comments. In comment or collapsed mode, inspect the
Conversation comment. Compare the findings and their destinations with the fixture's expected
result. If inline publication falls back to a Conversation comment, verify that it preserves the
findings. For a duplicate-suppression case, confirm the reuse or skip in the logs and comment
history. If no finding exercises the changed publication path, record that path as untested.

### Summary mode

Open the workflow run's Actions summary and check its report against the fixture's expected
result and the correlated Check Run. The report files are not uploaded in this mode.

Record the workflow SHA, reviewed head, review base, mode, run link, relevant artifact or comment
links, observed behavior, and any incomplete checks in the PR description. Distinguish live model
execution from offline replay. If code or instructions change afterward, state which revision
ran live and how the remaining changes were verified.

## Cleanup

Preserve the evidence before the artifact retention period expires; see the README's
[saved-report reference](reviewer/README.md#github-comments-checks-and-saved-reports).
After the run finishes, delete only the temporary ref created above. The exact-SHA lease rejects
deletion if someone has moved it. Keep the PR branches, workflow run and downloaded evidence.

```bash
git push --force-with-lease="refs/heads/$AI_REVIEW_REF:$AI_REVIEW_SHA" \
  "$AI_REVIEW_REMOTE" ":refs/heads/$AI_REVIEW_REF"
git ls-remote --exit-code --heads "$AI_REVIEW_REMOTE" "refs/heads/$AI_REVIEW_REF"
```

The final command should print no matching ref and exit with status 2.
