# Default engine guidelines

Scope: `default-engine/**`. Cross-cutting Rust/comment/protocol conventions live in the root
`CLAUDE.md`; this file is the default-engine context. Kernel architecture (the `Engine` trait
and its handlers) is in `CLAUDE/architecture.md`.

## What this crate is

`delta_kernel_default_engine` is the reference `Engine` implementation: an Arrow + Tokio +
`object_store` backing for the five kernel handlers (storage, JSON, Parquet, evaluation, and
the optional metrics reporter). It is what most connectors use out of the box, and the bar for
"how an engine is supposed to behave". Kernel itself does no I/O and does not depend on Arrow --
that all lives here.

`DefaultEngine<E: TaskExecutor>` wraps the handlers; build it via `DefaultEngineBuilder`. The
crate is organized by handler concern (storage/filesystem, json, parquet, plus stats and the
file-stream adapters that feed them).

## Async execution

All I/O is async and runs through a `TaskExecutor` (`executor.rs`). Two Tokio executors ship:
`TokioBackgroundExecutor` (single background runtime) and `TokioMultiThreadExecutor`. Kernel's
public API is synchronous, so the engine bridges sync->async with `block_on`.

- **Operations that re-enter the engine (notably `checkpoint()`) issue nested `block_on` calls
  and deadlock on a single-threaded runtime.** Such paths require the multi-thread executor.
  This is the same hazard the test helper `test_table_setup_mt` exists to avoid.
- A custom `TaskExecutor` is a valid extension point, but it must uphold this same nested-blocking
  contract.

## Invariants to uphold

- **`EngineData` produced here is still opaque to kernel.** It is concretely `ArrowEngineData`,
  but the engine must not assume kernel will downcast it, and must never assume one batch per
  file -- always stream batches.
- **TLS and Arrow version are feature-selected.** TLS backend (`rustls` vs `native-tls`) lives
  on this crate, not on kernel. Arrow version features are mutually exclusive; code must build
  under each supported version, not just the default.
- **Honor presigned/direct URLs.** The filesystem layer detects presigned object-store URLs and
  reads them directly; don't route those through credentialed object-store clients.
