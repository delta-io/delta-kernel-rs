# Workloads guidelines

## What this crate is

`delta_kernel_workloads` holds the shared description of a benchmark/acceptance workload --
table identity and layout, read configuration (serial vs parallel scan), and a SQL predicate
parser that turns a WHERE clause into a kernel expression. It is the common vocabulary the
benchmark and remote-table suites build on; aside from loading its own spec JSON it drives no
engine and runs no workload itself.

## Invariants to uphold

- **A workload points at a table one way or the other** -- a filesystem/object-store path or UC
  catalog identity, not both. The two are modeled as separate optional fields with the exclusivity
  checked at parse time; preserve that invariant and never let both fields be populated.
- **The predicate parser must emit the same expression kernel would evaluate.** It is a thin SQL
  front-end over kernel's expression types; when extending it, mirror kernel's operator and
  literal semantics exactly -- a parser that subtly diverges produces benchmarks that don't
  measure the real predicate.
- **Specs must round-trip and stay engine-neutral.** These types are serialized and shared across
  suites; keep them serde-stable and free of engine- or backend-specific assumptions.
