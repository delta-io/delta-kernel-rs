# Test utilities guidelines

Scope: `test-utils/**`. Cross-cutting conventions live in the root `CLAUDE.md`; the universal
testing philosophy is there too, and the curated catalog of what these helpers are and when to
reach for them lives in `kernel/tests/CLAUDE.md`. This file is about *maintaining* the shared
helper crate.

## What this crate is

`test_utils` is the shared test-support surface for the whole workspace: engine/table setup,
table builders and parameterized sweep templates, commit/read helpers, schema fixtures, a
counting metrics reporter, and a mock catalog committer. Other crates depend on it for tests, so
its public items are effectively an internal API.

## Invariants to uphold

- **Add the helper here, not a private copy in a test.** The reason this crate exists is to stop
  every test file from hand-rolling table setup or commit-JSON parsing. Before adding a helper,
  check whether one already exists (and is catalogued in `kernel/tests/CLAUDE.md`); when you add
  or rename one, update that catalog so it doesn't drift.
- **Keep helpers reusable and engine-agnostic where they can be.** A helper baked to one test's
  specifics defeats the purpose; prefer parameters and the existing sweep templates over forking.
- **Exported sweep templates are a shared macro surface.** Templates are reachable from other
  crates; renaming or repurposing one ripples across consumers -- treat it like an API change.
- **Honor the multi-thread runtime requirement.** Setup helpers come in single- and multi-thread
  variants because checkpoint-style operations deadlock on a single-threaded runtime; keep both
  paths working and point callers at the `_mt` variant when nested blocking is possible.
