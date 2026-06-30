# FFI Layer

The `delta_kernel_ffi` crate exposes the kernel to C/C++ via a stable FFI boundary using
cbindgen-generated headers (`.h` and `.hpp`).

## Terminology

- **Kernel vs engine** -- "kernel" is the native Rust side; "engine" is the foreign runtime
  on the other side of the boundary. Use this split to reason about which side allocated and owns
  a value.
- **Downcall vs upcall** -- a downcall is the engine invoking an exported Rust function; an
  upcall is Rust invoking an engine-provided function pointer.
- **Allocator vs owner** -- the allocator initialized the memory and knows *how* to free it;
  the owner decides *when*. Ownership can transfer across the boundary (e.g. a handle returned
  from a builder), so track both independently.

## Handle System

Objects crossing the FFI boundary may be wrapped in **handles** -- opaque pointers with
ownership semantics:
- **Exclusive handles** (`Box`-like) -- exclusive ownership, neither `Copy` nor `Clone`
- **Shared handles** (`Arc`-like) -- shared ownership via reference counting

Declare them with the `#[handle_descriptor]` macro and prefix the type name with `Exclusive`
or `Shared` so the ownership model is visible at the use site (e.g. `SharedSnapshot`,
`ExclusiveTransaction`). The public type convention is `Exclusive`/`Shared`, but the
macro-internal vocabulary is still "mutable" -- the `#[handle_descriptor(mutable = ...)]` argument
and `HandleDescriptor::Mutable` -- so don't be surprised by the mismatch when editing the macro.

A handle is needed when a value might outlive the function call that passes it across the
FFI boundary, or when the type is not representable in C/C++ (dyn trait references, slices,
options, etc.). Short-lived "plain old data" types like `ExternResult`, `KernelError`,
`KernelStringSlice`, and `EngineIterator` do not need handles.

Every handle has a corresponding `free_*` function (e.g. `free_engine`, `free_snapshot`).

## Memory Ownership

Default to **kernel-allocated** data whose ownership transfers via a handle: it keeps freeing
explicit (the engine calls `free_*`) and avoids upcalls just to release memory. Reach for
**engine-allocated** data only where it genuinely pays off:
- Arrow `EngineData` (Arrow's C Data Interface already carries ownership-transfer semantics),
- error strings (the callee allocates them in the caller's space; see Error Handling), and
- engine-owned iterators, where kernel must re-enter engine state across upcalls.

For engine-allocated state, the engine supplies the data plus a `free` callback. Mirror its
layout with a `repr(C)` struct prefixed `C` (e.g. `CEngineDataIterator`), and wrap it in a
Rust adapter prefixed `Ffi` (e.g. `FfiEngineDataIter`) whose `Drop` invokes that `free`
callback -- so the engine resource is released deterministically by Rust's ownership rules.

## repr(C) structs & out-pointers

Small `repr(C)` structs cross the boundary by value (inputs, grouped primitive returns,
tagged unions like `ExternResult`/`OptionalValue`). Prefix the transparent type with `C`.

Upcalls that return a struct use an **out-pointer**: Rust passes `*mut T` and the engine
writes into it. Before invoking the upcall, always initialize `*out` to a well-defined
sentinel (e.g. `EngineExecResult::Uninit`) so a callback that fails to write cannot leave
Rust reading uninitialized memory -- treat a still-sentinel value after the call as failure.

## Error Handling

Fallible functions return `ExternResult` (tagged union of Ok/Err). The caller provides an
`allocate_error` callback when creating the engine; kernel calls this to allocate errors in
the caller's memory space.

## Key Files

- `src/lib.rs` -- main FFI entry points and type definitions
- `src/handle.rs` -- opaque handle system for passing Rust objects across FFI
- `src/scan.rs` -- scan FFI interface
- `src/schema_visitor.rs` -- visitor pattern for schema traversal
- `src/ffi_tracing.rs` -- log/tracing and metrics callback registration (`#[cfg(feature = "tracing")]`)
- `src/ffi_metrics.rs` -- `repr(C)` mirror of kernel `MetricEvent` types (`#[cfg(feature = "tracing")]`)

## Read Flow

```
get_default_engine() -> get_snapshot_builder() -> snapshot_builder_build() -> scan() -> scan_metadata() -> read + transform
```

Snapshot builder API (`ffi/src/lib.rs`):
- `get_snapshot_builder(path, engine)` -- fresh snapshot from a table path
- `get_snapshot_builder_from(old_snapshot, engine)` -- incremental update reusing an existing snapshot (avoids re-reading the log)
- `snapshot_builder_set_version(builder, version)` -- optional: pin to a specific version
- `snapshot_builder_set_log_tail(builder, log_tail)` -- optional: set log tail (for catalog-managed tables)
- `snapshot_builder_set_max_catalog_version(builder, version)` -- optional: set max catalog version (for catalog-managed tables)
- `snapshot_builder_build(builder)` -- consume the builder and produce a `SharedSnapshot`
- `free_snapshot_builder(builder)` -- discard without building (e.g. on error paths)

The caller owns the returned builder handle and must call either `snapshot_builder_build` or `free_snapshot_builder`.

## Commit Range Flow

A `CommitRange` describes a contiguous commits of table. Build one
via the commit range builder (`ffi/src/commit_range.rs`):

```
commit_range_builder_for(path, start_version, engine)
  -> commit_range_builder_set_end_version(builder, end_version)  // optional; else latest version
  -> commit_range_builder_build(builder)                         // -> SharedCommitRange, always consume builder
```

The caller owns the builder and must call either `commit_range_builder_build` or
`free_commit_range_builder`. Release the range with `free_commit_range`.

## Write Flow

```
get_default_engine() -> transaction() -> with_engine_info() -> with_operation() -> add_files() -> commit()
                                                                                  |
                                                                                  v
              committed_transaction_version / committed_transaction_post_commit_snapshot
                                                                                  |
                                                                                  v
                                                                  free_committed_transaction
```

`commit()` and `create_table_commit()` return a `Handle<ExclusiveCommittedTransaction>` that the caller can read via `committed_transaction_version` and `committed_transaction_post_commit_snapshot`, then must release with `free_committed_transaction`. The post-commit snapshot, when present, is a separate `SharedSnapshot` handle that must be freed with `free_snapshot`.

Deletion vector update flow:

```
transaction()
  -> dv_descriptor_map_new()
  -> dv_descriptor_new()
  -> dv_descriptor_map_insert()
  -> scan() -> scan_metadata_iter_init()
  -> transaction_update_deletion_vectors()
  -> commit()
```

The engine authors the DV file and passes descriptor fields to `dv_descriptor_new`. The
descriptor map and scan iterator are both consumed by `transaction_update_deletion_vectors`;
descriptor handles are consumed by `dv_descriptor_map_insert` only on success and must be
freed by the caller on error. DV updates require both the `deletionVectors` reader/writer
feature and `delta.enableDeletionVectors=true`.

## Tracing & Metrics

Gated behind the `tracing` feature. A single global `tracing` subscriber backs both logging and
metrics; it is installed lazily the first time any `enable_*` function below is called. The
subscriber has two reloadable slots: a logging layer (swapped wholesale between event-based and
log-line formats) and a metrics layer (a fixed `ReportGeneratorLayer` toggled on/off via a
reloadable level filter).

Logging registration (each re-callable to replace the active callback, format, and level):
- `enable_event_tracing(callback, max_level)` -- structured `Event`s; the engine formats them
- `enable_log_line_tracing(callback, max_level)` -- pre-formatted log lines, default options
- `enable_formatted_log_line_tracing(callback, max_level, format, ansi, with_time, with_level, with_target)`
  -- pre-formatted log lines with explicit formatting options

Metrics registration:
- `enable_metrics_reporting(callback)` -- forwards each kernel `MetricEvent` to the callback as a
  `repr(C)` `MetricEvent` (see `src/ffi_metrics.rs`). Re-calling replaces the callback.

The `MetricEvent` and any `KernelStringSlice` it carries are only valid for the duration of the
callback. Durations are `u64`, suffixed `_ns` (nanoseconds) or `_ms` (milliseconds). Operation ids
are the raw 16 bytes of the kernel UUID (`MetricId`).

## Building

```bash
cargo build -p delta_kernel_ffi --release
# Headers written to target/ffi-headers/
```

Feature flags:
- `default-engine-rustls` (default)
- `default-engine-native-tls`
- `arrow` (default; currently maps to `arrow-58`)
- `arrow-58`, `arrow-57`
- `delta-kernel-unity-catalog`
- `tracing`
