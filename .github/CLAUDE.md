# .github/CLAUDE.md

Workflow-authoring rules for GitHub Actions.

## Triggers: restrict `push` to long-lived branches

The CI trigger events:

- `pull_request`: a commit is pushed to a branch with an open PR. Runs CI on the
  PR.
- `merge_group`: the merge queue builds a temporary `gh-readonly-queue/*` branch
  and tests the change as it would land on `main`.
- `push`: a branch ref is updated. We only want this for the long-lived branches
  (`main` and `release/**`), i.e. the post-merge run once a PR lands.

We want CI on `pull_request`, on `merge_group`, and on `push` to a long-lived
branch: one run per stage. A bare `push` trigger instead duplicates two of these:

- `push` + `pull_request`: a branch pushed directly to this repo (a maintainer
  branch, not a fork) fires both. Fork PRs avoid this because the `push` lands on
  the fork, not on this repo.
- `push` + `merge_group`: the merge queue's push to the `gh-readonly-queue/*`
  branch fires both.

Restricting `push` to long-lived branches drops the redundant `push` in each
case:

```yaml
on:
  push:
    branches: [main, "release/**"]
  pull_request:
  merge_group:
```

## Skipping docs-only PRs without wedging required checks

GitHub treats two kinds of "skip" differently for required status checks:

- A **workflow** skipped by `paths`/`paths-ignore` never reports its checks, so any
  required check stays "Expected, waiting for status" and blocks the merge forever.
  NEVER add `paths`/`paths-ignore` to a workflow whose jobs are required.
- A **job** skipped by its own `if:` reports "skipped", which satisfies a required
  check of the same name. This is the mechanism we use.

The shared `detect-changes.yml` reusable workflow emits `docs_only`. Callers gate on
`needs: detect_changes` + `if: needs.detect_changes.outputs.docs_only == 'false'`. How to
gate depends on the job shape:

- **Single (non-matrix) jobs** (`format`, `docs`, `run-examples`, ...): the job-level `if:`
  is enough. A skipped job reports its own name as "skipped", satisfying its required check.
- **Matrix jobs** (`build`, `test`, `ffi_test`, `miri`): a job-level `if:` skip never expands
  the matrix, so its per-leg checks (`build (ubuntu-latest)`, ...) never report and the PR
  blocks. These CANNOT be individually required. Instead, `build.yml` funnels every job into
  one `all-required-checks-pass` aggregator (`if: always()`, `needs:` every job) whose step
  fails only when a needed job is `failure`/`cancelled` (`success`/`skipped` pass). Branch
  protection requires ONLY that aggregator, so matrix jobs skip cleanly as a unit with a
  single job-level `if:` and no per-step gating.

The aggregator's `name:` is load-bearing: renaming it silently disables branch protection.
When adding a job to `build.yml`, add it to the aggregator's `needs:` unless it is
intentionally non-required (e.g. `invalid-handle-code`).

## Supply chain security: `--locked`

Every `cargo` command in CI that resolves dependencies MUST use `--locked` to
enforce the committed `Cargo.lock`. This prevents CI from silently picking up a
newer (potentially compromised) transitive dependency. If `Cargo.lock` is out of
sync with `Cargo.toml`, the build fails immediately, forcing dependency changes to
be explicit and reviewable. See the top-level comment in `build.yml` for full
rationale. Commands exempt from `--locked`: `cargo +nightly fmt` (no dep
resolution), `cargo msrv verify/show` (wrapper tool), `cargo miri setup` (tooling
setup).

## Action safety

When writing any github action, consider safety including mitigating common attack
vectors such as expression injection and pull request target attacks.

```yaml
# The code below is vulnerable to expression injection
run: |
    echo "Comment: ${{ github.event.comment.body }}"

# To mitigate instead use environment variables
env:
    COMMENT_BODY: ${{ github.event.comment.body }}
run: |
    echo "Comment: $COMMENT_BODY"
```
