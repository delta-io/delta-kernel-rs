# Releasing Delta Kernel Rust

The Kernel crates share the workspace version and a single `v<version>` release tag:

- `delta_kernel_derive`
- `delta_kernel`
- `delta_kernel_default_engine`

The Unity Catalog (UC) crates have independent versions and `v<version>_<crate>` release tags:

- `unity-catalog-delta-client-api`
- `unity-catalog-delta-rest-client`
- `delta-kernel-unity-catalog`

`release.sh` provides these subcommands:

- `./release.sh release <version>` prepares a Kernel release PR from a release branch.
- `./release.sh release` publishes and tags the Kernel crates when run on `main`.
- `./release.sh crate <crate> <version>` prepares a release PR for one UC crate.
- `./release.sh tag <crate> [commit]` creates and pushes an annotated tag without publishing.

Use `./release.sh tag delta_kernel` to create the shared Kernel tag when publishing is handled
separately.

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
DELTA_KERNEL_RELEASE_REGISTRY=<registry-name> ./release.sh release 0.29.0
```

The variable is passed only to `cargo release`; publishing still uses the release destination
described below. Do not mark `release.sh` as `skip-worktree` or keep a private edit to it.

## Prepare the Kernel release PR

Run the release script from a branch created at the latest `upstream/main`:

```bash
./release.sh release 0.29.0
```

The script updates workspace versions, refreshes `CHANGELOG.md`, creates the release commit, and
can push the branch and open the PR. Review the generated changelog before requesting approval:

1. Compare the commits after the previous release tag with the changelog.
2. Remove entries already shipped in a patch release.
3. Move true API or contract breaks into **Breaking changes** and explain their impact. New APIs and
   changes restricted to `internal-api` are not breaking changes.
4. Run `./release.sh verify-changelog 0.29.0`.

Kernel changelogs only use plain or pre-release semantic-version tags such as `v0.28.0` or
`v0.29.0-rc.1` as release boundaries. Artifact-specific tags such as `v0.0.1_dat` do not truncate
the changelog range.

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

## Publish and tag the Kernel crates

After the release PR merges, update local `main` and verify that it points at the release commit:

```bash
git switch main
git pull --ff-only upstream main
```

Maintainers publishing directly to crates.io can then run:

```bash
./release.sh release
```

The script publishes in dependency order (`delta_kernel_derive`, `delta_kernel`, then
`delta_kernel_default_engine`) and creates the `v<version>` tag. Check each crate on crates.io and
announce the release in the Delta community channels.

## Release a UC crate

### Prepare the release PR

Create a release branch from the latest `upstream/main` and run the per-crate command:

```bash
git fetch upstream main --tags
git switch -c uc-crate-release/unity-catalog-delta-client-api-0.2.0 upstream/main
./release.sh crate unity-catalog-delta-client-api 0.2.0
```

The command refuses to run on `main` and requires a clean working tree, including untracked files.
It bumps the selected crate, updates its dependents' version requirements, and prepends release
notes to `<crate>/CHANGELOG.md`. It creates a release commit and can push the branch and open a PR.
Review the manifests and changelog before merging; add dependency-upgrade notes when relevant.

Use the `uc-crate-release/` prefix for UC release branches. The build workflow runs Kernel-only
changelog verification on branches beginning with `release/`. A different prefix avoids that
unrelated check for UC release PRs; the workflow does not publish crates.

Each crate's changelog uses its own release tags as boundaries. Kernel tags and other crates' tags
do not truncate its history. `changelog` and `verify-changelog` apply to the Kernel changelog only.

### Version compatibility

UC crates evolve independently. A Kernel version bump updates their Kernel dependency requirements
without changing their own versions. When a dependency makes a breaking change, also bump each
affected dependent's breaking version before tagging. Before `1.0`, this means a minor bump,
such as `0.1.0` to `0.2.0`. A compatible patch does not require a dependent version bump.

`delta-kernel-unity-catalog` depends on `delta_kernel` and `unity-catalog-delta-client-api`.
`unity-catalog-delta-rest-client` depends on `unity-catalog-delta-client-api`. The script warns about
independently versioned dependents; review their APIs to decide which versions need to change.

Prepare each dependent release on a separate `uc-crate-release/` branch and PR. Merge the
dependency's release PR first, then create the dependent's branch from updated `upstream/main`
so it includes the new dependency requirement. Prepare affected dependent releases before tagging.

### Tag the release

After merging, update `main` and verify that the checkout's manifests match the intended release:

```bash
git switch main
git pull --ff-only upstream main
./release.sh tag unity-catalog-delta-client-api
```

For version `0.2.0`, this creates and pushes the annotated tag
`v0.2.0_unity-catalog-delta-client-api`. To tag a historical release, check out its commit first:

```bash
git switch --detach <release-commit>
./release.sh tag unity-catalog-delta-client-api
```

The version in the tag comes from the current checkout's manifest, which must match the target
commit's manifest. Tagging does not publish the crate, and `release` on `main`
publishes only the Kernel crates.
UC publication is handled separately from this script. If a tag push fails, retry with
`git push upstream tag <tag>`; the annotated tag remains local.

## Patch releases

Create the patch branch from the previous release tag, cherry-pick only the intended fixes, and open
the generated release PR against that patch branch. Keep patch-only entries out of the next minor
release changelog when reviewing it.
