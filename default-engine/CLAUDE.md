# Default engine guidelines

`delta_kernel_default_engine` is the reference `Engine` implementation: an Arrow + Tokio +
`object_store` backing for the five kernel handlers (storage, JSON, Parquet, evaluation, and
the optional metrics reporter). Most connectors use it out of the box.

Keep the kernel-core vs engine distinction straight. "Kernel" (the `delta_kernel` crate) is the
protocol core: synchronous APIs that do no I/O and carry no Arrow dependency. `Engine` is the
trait that supplies I/O and compute, and this crate is one Arrow-based implementation of it.
Kernel core must never assume an engine uses Arrow -- Arrow lives on this side of the trait.

`DefaultEngine<E: TaskExecutor>` wraps the handlers; build it via `DefaultEngineBuilder`. The
crate is organized by handler concern (storage/filesystem, json, parquet, plus stats and the
file-stream adapters that feed them).

The `Engine` trait and its handlers are described in `CLAUDE/architecture.md`.

## Async execution

All I/O is async and runs through a `TaskExecutor` (`executor.rs`). Two Tokio executors ship:
`TokioBackgroundExecutor` (single background runtime) and `TokioMultiThreadExecutor`. Kernel's
public API is synchronous, so the engine bridges sync->async with `block_on`.

- **Any kernel operation that re-enters the engine issues nested `block_on` calls and deadlocks
  on a single-threaded runtime** (`checkpoint()` is one such path). These require the multi-thread
  executor. This is the canonical description of the nested-`block_on` hazard that the rest of the
  docs point back to.
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
