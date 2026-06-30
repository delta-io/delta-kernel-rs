# Test utilities guidelines

This file is about *maintaining* the shared helper crate. How to *use* these helpers from tests
is in `kernel/tests/CLAUDE.md`.

## What this crate is

`test_utils` is the shared test-support surface for the whole workspace: engine/table setup,
table builders and parameterized sweep templates, commit/read helpers, schema fixtures, a
counting metrics reporter, and a mock catalog committer. Other crates depend on it for tests, so
its public items are effectively an internal API.

## Invariants to uphold

- **Add the helper here, not a private copy in a test.** The reason this crate exists is to stop
  every test file from hand-rolling table setup or commit-JSON parsing. Before adding a helper,
  check whether one already exists; give every new public helper a doc comment so callers can
  discover it from the source.
- **Keep helpers reusable and engine-agnostic where they can be.** A helper baked to one test's
  specifics defeats the purpose; prefer parameters and the existing sweep templates over forking.
- **Exported sweep templates are a shared macro surface.** Templates are reachable from other
  crates; renaming or repurposing one ripples across consumers -- treat it like an API change.
- **Honor the multi-thread runtime requirement.** Setup helpers come in single- and multi-thread
  variants because some kernel operations need the multi-thread runtime (see
  `default-engine/CLAUDE.md` for the nested-`block_on` reason); keep both paths working and point
  callers at the `_mt` variant when nested blocking is possible.
