# Acceptance test guidelines

Scope: `acceptance/**`. Cross-cutting conventions live in the root `CLAUDE.md`. For writing
kernel's own integration tests (table setup, helpers), see `kernel/tests/CLAUDE.md` -- this crate
is different: it runs the cross-implementation conformance suite.

## What this crate is

The `acceptance` crate runs the Delta Acceptance Tests (DAT): externally generated test cases,
each a table plus a JSON spec of expected metadata and scan results. It loads a case, builds a
snapshot, and asserts kernel's snapshot metadata and full scan output match the golden spec. This
is the suite that proves kernel agrees with the protocol across implementations.

## Invariants to uphold

- **Golden expectations are the source of truth; don't loosen an assertion to make a case pass.**
  A failing DAT case is signal that kernel diverged from the spec. Fix kernel (or escalate a
  genuine spec/case bug) -- never weaken the metadata/scan comparison to get green.
- **Cases come from generated DAT resources.** The data is downloaded/generated, not committed.
  If the resource tree is absent the harness has nothing to run; a "passing" empty run is not
  coverage.
- **Cover the protocol versions the suite spans** -- both legacy (reader/writer 1/2) and
  table-features (3/7) cases -- when adding or adjusting validation logic.
