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

## Never `paths`/`paths-ignore` a workflow whose jobs are required checks

Do NOT add `paths` or `paths-ignore` filters to a workflow whose jobs are required
status checks (most of `build.yml`). When a path filter skips the whole workflow,
the required checks never report, and the PR stays blocked on "Expected, waiting
for status" indefinitely. A skipped workflow is not the same as a passing one.

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
