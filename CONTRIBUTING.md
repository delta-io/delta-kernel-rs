# Contributing to Delta Kernel (Rust)

> [!NOTE]
> **Found a bug?** Please first search [existing issues] to avoid duplicates. If you find a related
> issue, add your details there. Otherwise, open a new issue with a reproducible example including
> Rust version, delta-kernel-rs version, code executed, and error message.

[existing issues]: (https://github.com/delta-io/delta-kernel-rs/issues)

## How to Contribute

For trivial fixes, etc. please feel free to open a PR directly. For larger changes, we follow a
structured contribution process to ensure high-quality code:

1. **Start with an issue and/or design sketch**: Open an issue describing what you want to
   contribute and why. Continue to step 2 after reaching some consensus. This helps us avoid wasted
   effort (perhaps you were building something that someone else was already pursuing or already
   explored and rejected). Including a design sketch will help drive consensus (often a simple
   diagram or bullet points outlining high-level changes is sufficient).
2. **Prototype/POC**: Create a PR marked as prototype/draft (not intended to merge) and gather
   feedback to further derisk the design. This PR is not intended to be merged but will guide the
   implementation and serve as a proving ground for the design. Then, pieces are torn out into
   smaller PRs that can be merged.
3. **Implementation**: Finally, create PR(s) to implement the feature (production code, tests,
   thorough docs, etc.). Often the initial POC will be split into multiple smaller PRs (e.g.,
   refactors, then feature additions, then public APIs specifically). Care should be taken to ensure
   each PR is easily reviewable and thoroughly tested.

## Forking and Setup

1. Fork the repository into your account
2. Clone your fork locally:
   ```bash
   git clone git@github.com:YOUR_USERNAME/delta-kernel-rs.git
   cd delta-kernel-rs
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream git@github.com:delta-io/delta-kernel-rs.git
   ```

Now you have:
- `origin` pointing to your fork
- `upstream` pointing to the original repository

## Development Workflow

Our trunk branch is named `main`. Here's the typical workflow:

1. Pull the latest main to get a fresh checkout:
   ```bash
   git checkout main
   git pull upstream main
   ```
2. Create a new feature branch:
   ```bash
   git checkout -b my-feature
   ```
   (NB: Consider using `git worktrees` for managing multiple branches!)
3. Make your changes and test them locally. See our CI runs for a full set of tests.
   ```bash
   # run most of our tests, typically sufficient for quick iteration
   cargo test
   # run clippy
   cargo clippy --all-features --tests --benches -- -D warnings
   # build docs
   cargo doc --workspace --all-features
   # highly recommend editor that automatically formats, but in case you need to:
   cargo fmt

   # run more tests
   cargo test --workspace --all-features -- --skip read_table_version_hdfs

   # see ffi/ dir for more about testing FFI specifically
   ```
4. Push to your fork:
   ```bash
   git push origin my-feature
   ```
5. Open a PR from `origin/my-feature` to `upstream/main`
6. Celebrate! 🎉

**Note**: Our CI runs all tests and clippy checks. Warnings will cause CI to fail.

**Note**: We require two approvals from code owners for any PR to be merged.

## Resources

- [Delta Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Delta Lake Slack](https://go.delta.io/slack) - Join us in the `#delta-kernel` channel