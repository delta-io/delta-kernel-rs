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
addition to workspace checks. Root workspace commands exclude `datafusion-executor` and
`integration-tests`. Validate those workspaces separately with their supported Arrow features.

## Action safety

Use exact release tags for external actions and commit the generated native GitHub Actions
lockfile, `.github/workflows/actions.lock`. The runner uses its immutable commits, including
nested composite dependencies. Every workflow must be onboarded. Reference local actions with
`$/` so their dependencies are covered too. For `taiki-e/install-action`, use an explicit `tool`
input instead of tool-specific action tags.

All uses of the same action repository must resolve to one commit, including subactions,
third-party nested dependencies, and the `action-versions` job's own actions. Different refs
resolving to the same commit are allowed. When a nested dependency conflicts, select compatible
parent action releases and align direct uses; do not exclude the nested dependency, even when
its step is conditional.

Install the pinned native CLI and update the lockfile after changing action references:

```bash
gh extension install github/gh-actions-lock --pin v0.3.0
python3 -m pip install --no-deps PyYAML==6.0.3
GH_ACTIONS_LOCK_REPO_ROOT="$PWD" gh actions-lock --migrate-local-actions --no-interactive
GH_ACTIONS_LOCK_REPO_ROOT="$PWD" gh actions-lock --verify \
  --json=valid,findings,workflows,dependencies > /tmp/action-dependencies.json
python3 .github/scripts/check_action_versions.py /tmp/action-dependencies.json
python3 -m unittest discover --start-directory .github/scripts --pattern 'test_*.py'
```

Review the generated lockfile diff along with the workflow changes. Do not edit it manually or
automatically accept moved refs. `gh actions-lock --verify-local` provides an offline coverage
check; CI uses `--verify` to check provenance too. The single-version checker validates the native
report and traverses the lockfile's `uses` graph because the CLI's JSON inventory omits nested
dependencies. There is no second SHA manifest.

Repository administrators must require the `action-versions` status check for merges and enable
GitHub's **Require lockfile** Actions policy to prevent untracked workflows from running.
The native lockfile feature is in public preview. It does not lock container images or the
internals of external reusable workflows; assess coverage before introducing either dependency
type. See [GitHub's lockfile documentation](https://gh.io/actions-lockfile).

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
