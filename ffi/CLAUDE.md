# FFI Layer

The `delta_kernel_ffi` crate exposes the kernel to C/C++ via a stable FFI boundary using
cbindgen-generated headers (`.h` and `.hpp`).

## Handle System

Objects crossing the FFI boundary may be wrapped in **handles** -- opaque pointers with
ownership semantics:
- **Mutable handles** (`Box`-like) -- exclusive ownership, neither `Copy` nor `Clone`
- **Shared handles** (`Arc`-like) -- shared ownership via reference counting

A handle is needed when a value might outlive the function call that passes it across the
FFI boundary, or when the type is not representable in C/C++ (dyn trait references, slices,
options, etc.). Short-lived "plain old data" types like `ExternResult`, `KernelError`,
`KernelStringSlice`, and `EngineIterator` do not need handles.

Every handle has a corresponding `free_*` function (e.g. `free_engine`, `free_snapshot`).

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

A `CommitRange` describes a contiguous range of a table's commits. Build one via the commit range
builder (`ffi/src/commit_range.rs`):

```
commit_range_builder_for(path, start_version, engine)
  -> commit_range_builder_set_end_version(builder, end_version)  // optional; else latest version
  -> commit_range_builder_build(builder)                         // -> SharedCommitRange, always consume builder
  -> commit_range_commits(range, engine, actions, actions_len)   // -> SharedCommitActionsIterator
       // or commit_range_commits_with_snapshot(range, engine, start_snapshot, actions, actions_len)
  -> commit_range_commits_next(iter, ctx, visitor)               // visitor receives a SharedCommitAction
       // in the visitor: commit_action_version / commit_action_timestamp
       //                  commit_action_get_actions(action, engine) -> ExclusiveFileReadResultIterator
       //                    -> read_result_next(...) -> free_read_result_iter(...)
```

The caller owns the builder and must call either `commit_range_builder_build` or
`free_commit_range_builder`. Release the range with `free_commit_range` and the commits iterator
with `free_commit_actions_iter`. Each `SharedCommitAction` handed to the visitor must be released
with `free_commit_action`.

## Incremental Scan Flow

An incremental scan streams the file-action diff between a base version and a target snapshot,
letting an engine advance a cached file listing without a full log replay
(`ffi/src/incremental_scan.rs`):

```
snapshot_incremental_scan_builder(snapshot, base_version, engine)
  -> incremental_scan_builder_build(builder)      // -> OptionalValue<stream>; None => full-scan fallback
  -> incremental_scan_stream_next_arrow(stream)    // drain live-Add batches (Arrow), newest-first
  -> incremental_scan_stream_into_summary(stream)  // -> summary; consumes the stream
```

`incremental_scan_builder_build` returns `OptionalValue::None` (not an error) when the target
snapshot's commit list can't cover `(base_version, target_version]`, which is the signal to fall
back to a full scan. The builder is always consumed by `build`; discard it early with
`free_incremental_scan_builder`.

Read the diff via `incremental_scan_summary_base_version` / `_target_version` and the
`incremental_scan_summary_visit_live_adds` / `_visit_removes` callbacks (each key is a `(path,
dv_unique_id)` pair; `dv_unique_id` is a zero-length slice when absent). Release handles with
`free_incremental_scan_stream`, `free_incremental_scan_summary`, and each Arrow batch with
`free_filtered_engine_data_arrow_result`.

To advance a cached listing, evict base entries whose key is in `removes` OR is re-added by the
range. OPTIMIZE / clustering re-tags a file under the same `(path, dv_unique_id)` key, so it
lands in `live_adds` but not `removes`; masking with `removes` alone leaves a stale row. The
full mask is `removes` unioned with the base keys also present in `live_adds`. The Rust API
computes this via `into_summary_against_base_*`, which the FFI does not yet expose.

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

Write context: `get_unpartitioned_write_context` covers unpartitioned tables. For partitioned tables, build a `PartitionValueMap` (`partition_value_map_new` + the typed `partition_value_map_insert_*` functions, one entry per partition column keyed by logical name) and pass it to `get_partitioned_write_context` (consumes the map). Then use `get_write_dir` for the partition's target directory (Hive-style prefix or random prefix), `visit_partition_values` to read the physical `partitionValues` to record in each Add action, and `resolve_file_path` to turn a written file's URL into its relative `add.path`. The `create_table_*` variants apply the same flow to a create-table transaction whose partition columns were declared with `create_table_builder_with_partition_columns`.

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
- `arrow` (default; currently maps to `arrow-59`)
- `arrow-59`, `arrow-58`
- `delta-kernel-unity-catalog`
- `tracing`
