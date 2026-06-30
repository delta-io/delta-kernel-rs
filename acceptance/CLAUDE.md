# Acceptance test guidelines

The `acceptance` crate runs the Delta Acceptance Tests (DAT): a cross-implementation conformance
suite. "Conformance" here means every Delta implementation (kernel, Spark, delta-rs, ...) is held
to the same externally generated golden cases, so the suite checks kernel against a shared spec
rather than against itself. Each case is a table plus a JSON description of the expected results;
the harness loads a case, builds a snapshot, and asserts kernel's reported metadata and data match
that description.

Kernel's own integration tests (table setup, helpers) are a separate thing under `kernel/tests/`
-- see `kernel/tests/CLAUDE.md`.

## Invariants to uphold

- **Golden expectations are the source of truth; don't loosen an assertion to make a case pass.**
  A failing DAT case is signal that kernel diverged from the spec. Fix kernel (or escalate a
  genuine spec/case bug) -- never weaken the comparison against the golden case to get green.
- **Cases come from generated DAT resources.** The data is downloaded/generated, not committed.
  If the resource tree is absent the harness has nothing to run; a "passing" empty run is not
  coverage.
- **Cover the protocol versions the suite spans** -- both legacy (reader/writer 1/2) and
  table-features (3/7) cases -- when adding or adjusting validation logic.
