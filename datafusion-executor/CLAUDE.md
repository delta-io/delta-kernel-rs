# CLAUDE.md -- datafusion-executor

`datafusion_executor` is a DataFusion-based `PlanExecutor` for delta_kernel declarative plans.
Kernel emits executor-independent logical `Plan`s; this crate executes them by lowering each
plan to a DataFusion `LogicalPlan`, optimizing it, and running the resulting `ExecutionPlan`.

## Separate workspace

This crate has its own `[workspace]`, so it is NOT a member of the root workspace and the root's
`--workspace` commands do not touch it. The isolation exists to avoid an arrow version mismatch:
DataFusion pins a specific arrow major, so kernel must be built against that same major here. The
root workspace builds kernel against its own default arrow major, which need not match. As a root
member, Cargo feature unification would compile kernel against a single arrow major shared with
the rest of the workspace, so DataFusion's types would link against a kernel built on a different
arrow major -- a mismatch that breaks the build. A separate workspace resolves kernel's arrow
major in isolation, matched to DataFusion's.

## Build & Test Commands

cd into the crate (the subshell keeps that from leaking out); cargo picks up the nested workspace
automatically.

```bash
(cd datafusion-executor && cargo build)
(cd datafusion-executor && cargo test)
```
