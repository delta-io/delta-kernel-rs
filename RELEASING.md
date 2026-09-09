# Releasing Delta Kernel Rust

This runbook covers the workspace-versioned Kernel crates:

- `delta_kernel_derive`
- `delta_kernel`
- `delta_kernel_default_engine`

Independently versioned crates use their own release instructions until the per-crate release
workflow is available.

## Prerequisites

Install `cargo-release`, `git-cliff`, and `jq`. Configure the `origin` remote to point to your fork
and `upstream` to point to `delta-io/delta-kernel-rs`. Fetch `main` and all tags before starting:

```bash
git fetch upstream main --tags
git switch -c release/0.29.0 upstream/main
```

The working tree must be clean.

### Alternate Cargo registry

If `cargo-release` must use an alternate registry, select it through the environment instead of
editing `release.sh`:

```bash
DELTA_KERNEL_RELEASE_REGISTRY=<registry-name> ./release.sh 0.29.0
```

The variable is passed only to `cargo release`; publishing still uses the release destination
described below. Do not mark `release.sh` as `skip-worktree` or keep a private edit to it.

## Prepare the release PR

Run the release script from a branch created at the latest `upstream/main`:

```bash
./release.sh 0.29.0
```

The script updates workspace versions, refreshes `CHANGELOG.md`, creates the release commit, and
can push the branch and open the PR. Review the generated changelog before requesting approval:

1. Compare the commits after the previous release tag with the changelog.
2. Remove entries already shipped in a patch release.
3. Move true API or contract breaks into **Breaking changes** and explain their impact. New APIs and
   changes restricted to `internal-api` are not breaking changes.
4. Run `./release.sh verify-changelog 0.29.0`.

Kernel changelogs only use plain semantic-version tags such as `v0.28.0` as release boundaries.
Artifact-specific tags such as `v0.0.1_dat` do not truncate the changelog range.

### If `main` changes while the PR is open

The `release-tooling` CI job compares the release section with every merged PR since the previous
Kernel release. A new merge to `main` makes a stale changelog check fail. Update the release branch
and regenerate the section:

```bash
git fetch upstream main
git rebase upstream/main
./release.sh changelog 0.29.0
git add CHANGELOG.md
git commit -m "chore: refresh release changelog"
git push --force-with-lease origin HEAD
```

The refresh command replaces the pending version's section, so it is safe to rerun. Do not merge a
release PR until `release-tooling` passes against the current base branch.

## Publish and tag

After the release PR merges, update local `main` and verify that it points at the release commit:

```bash
git switch main
git pull --ff-only upstream main
```

Maintainers publishing directly to crates.io can then run:

```bash
./release.sh
```

The script publishes in dependency order (`delta_kernel_derive`, `delta_kernel`, then
`delta_kernel_default_engine`) and creates the `v<version>` tag. Check each crate on crates.io and
announce the release in the Delta community channels.

## Patch releases

Create the patch branch from the previous release tag, cherry-pick only the intended fixes, and open
the generated release PR against that patch branch. Keep patch-only entries out of the next minor
release changelog when reviewing it.
