# Benchmark workflow security model

Scope: [`benchmark.yml`](workflows/benchmark.yml) (runs bench on PR head) and
[`benchmark-post-comment.yml`](workflows/benchmark-post-comment.yml) (posts the
result comment in base-branch context). Other workflows in this repo may follow
different patterns -- this document is specific to the benchmark pair.

## Running untrusted PR code

The `run-benchmark` job in `benchmark.yml` checks out the PR head and runs
its build + bench harness. To bound what that code can do:

- Workflow-level `permissions: {}` is the default; each job opts in to the
  minimum scope it needs. `run-benchmark` declares only `contents: read`,
  so any leaked GITHUB_TOKEN has no write power. Fork PRs additionally get
  a read-only token from GitHub by default under the `pull_request` event.
- `actions/checkout` uses `persist-credentials: false` so the token isn't
  left in the local git config after checkout.
- `cargo install critcmp` runs *before* the PR-head checkout, so the PR's
  `.cargo/config.toml` cannot redirect the registry for that install.

Do not add other secrets or grant write permissions to `run-benchmark`.

## Rust-cache poisoning

`Swatinem/rust-cache` is configured with
`save-if: github.event_name == 'push' && github.ref == 'refs/heads/main'`.
PR runs read-restore from main's cache; they never save. The
`github.event_name == 'push'` clause is the load-bearing half -- the
benchmark workflow has no `push` trigger outside of `push: main` for the
cache-warming job, so PR runs cannot match this condition and therefore
cannot write the cache.

Do not relax the `github.event_name == 'push'` clause. If you do, a PR-head
run can land compile artifacts in main's cache scope (poisoning subsequent
runs) or evict main's entries through the shared cache budget.

## Cross-PR comment forgery

`benchmark-post-comment.yml` runs in base-branch context with
`pull-requests: write`. It must derive the target PR number from a source
the fork cannot forge. The choice depends on which event triggered the
upstream bench workflow:

- **`pull_request` upstream:** use `gh pr view --repo "$REPO" "owner:branch"`,
  where `owner:branch` is built from `workflow_run.head_repository.owner.login`
  and `workflow_run.head_branch`. Both fields are populated by GitHub from the
  trusted workflow_run event payload, not from anything the fork wrote.
- **`issue_comment` upstream:** trust the `bench-pr-number` artifact. For
  `issue_comment` events GitHub serves `benchmark.yml` from the default
  branch (not the PR head), so the "Stash PR number" + "Upload PR number"
  steps run trusted code. The upload happens *before* `actions/checkout`
  brings PR code onto the runner, so once the artifact is on GitHub's
  storage it is immutable for the rest of the run -- the bench script
  cannot alter it, and `contents: read` denies any API-level mutation.

`workflow_run` itself runs the post-comment workflow from the default
branch (a GitHub guarantee), so a fork PR cannot modify the resolver to
trust the wrong source.
