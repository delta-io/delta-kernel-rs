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

## Rust API checks

Result alias and public signature changes need doctests and user-guide example compilation in
addition to workspace checks. Root workspace commands exclude `integration-tests`.
Validate that workspace separately with its supported Arrow features.

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

## Release tooling checks

The `release-tooling` job in `build.yml` fetches full history and tags because release branches
verify the pending changelog against every PR since the previous Kernel release. Keep
`fetch-depth: 0` on that checkout. The regression tests run on every pull request, while live
verification is restricted to branches whose names start with `release/`; otherwise the window
between merging a release PR and pushing its tag could block unrelated pull requests.

## `automerge` label controller

`automerge.yml` runs `.github/scripts/automerge.py` on `pull_request_target` label events with a
write-capable token. It must execute only trusted `main` code: never check out or run the PR head.
Every decision reads live PR state rather than the event payload, so stale or duplicate deliveries
reconcile to the same result, and the per-PR concurrency group must keep `cancel-in-progress`
false so an in-flight mutation is never abandoned.

Keep `stack_description.py` strict. It recognizes one complete generated prefix at offset zero and
returns a slice boundary; the caller writes `body[end:]` and nothing else. Do not add heuristics
that search for a heading or terminator farther down, admit prose or unknown rows, normalize
whitespace or line endings, or rewrite retained content. An unrecognized description is an
accepted miss, never a failing check. `.github/scripts/tests/fixtures/stack_descriptions.json`
holds historical descriptions with their expected boundaries; extend it when the grammar changes.
CONTRIBUTING.md owns the contributor-facing rules.
