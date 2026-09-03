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
options, etc.). Short-lived "plain old data" types like `ExternResult`, `FFIKernelError`,
`KernelStringSlice`, and `EngineIterator` do not need handles.

Every handle has a corresponding `free_*` function (e.g. `free_engine`, `free_snapshot`).

## Error Handling

Fallible functions return `ExternResult` (tagged union of Ok/Err). The caller provides an
`allocate_error` callback when creating the engine; kernel calls this to allocate errors in
the caller's memory space. Existing `FFIKernelError` discriminants `0..=46` retain their ABI
values, and user-facing Delta errors use the appended `DeltaError = 47` code. The existing
allocator callback signature is unchanged and receives the rendered message as a borrowed
`KernelStringSlice`; an allocator that retains it must copy it before returning. `DeltaError` is
an outbound classification. If an engine returns code 47 through an upcall, kernel preserves its
message in a generic kernel error because the engine cannot author a structured Delta condition
through `EngineExecError`.

## Key Files

- `src/lib.rs` -- main FFI entry points and type definitions
- `src/handle.rs` -- opaque handle system for passing Rust objects across FFI
- `src/column_default.rs` -- column-default (`allowColumnDefaults`) reads and the write-path ack
- `src/scan.rs` -- scan FFI interface
- `src/schema_visitor.rs` -- visitor pattern for schema traversal
- `src/ffi_tracing.rs` -- log/tracing and metrics callback registration (`#[cfg(feature = "tracing")]`)
- `src/ffi_metrics.rs` -- `repr(C)` mirror of kernel `MetricEvent` types (`#[cfg(feature = "tracing")]`)
- `src/alloc_stats.rs` -- `peak_alloc` global allocator and native-heap FFI getters
  (`alloc-tracking`)

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

Snapshot accessors (`ffi/src/lib.rs`) read a built `SharedSnapshot` without I/O -- e.g. `version`,
`snapshot_timestamp`, and `snapshot_file_stats`, which returns `OptionalValue<FfiFileStats>` (scalar
`num_files` / `table_size_bytes` from the CRC; `None` when the snapshot has no CRC, or its CRC lacks
complete file stats). `visit_file_size_histogram` exposes the optional variable-length histogram in
the same file stats: it invokes one callback with borrowed `i64` slices for bin boundaries, file
counts, and total bytes; callers must copy values they retain after the callback.

Domain-metadata reads live in `ffi/src/domain_metadata.rs`: `get_domain_metadata` /
`visit_domain_metadata` for user domains, and `visit_clustering_columns`, which reports one
descriptor per clustering column -- logical name, physical name (what per-file stats are keyed on),
and a type tag -- without exposing the guarded `delta.*` domain JSON directly. It returns
`OptionalValue<usize>`: `None` means not clustered, `Some(0)` means clustered on no columns. The
type tag reuses the `visit_expression_literal_null` encoding, with 255 for types that don't fit a
compact tag (struct, array, map, variant, void, and geometry/geography).

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

An incremental scan streams the file-action diff between a base version and a target snapshot
(`ffi/src/incremental_scan.rs`):

```
snapshot_incremental_scan_builder(snapshot, base_version, engine)
  -> incremental_scan_builder_with_predicate(builder, engine, predicate)  // optional; prunes live Adds
  -> incremental_scan_builder_build(builder)      // -> OptionalValue<stream>; None => full-scan fallback
  -> incremental_scan_stream_next_arrow(stream)*  // optional: pull live-Add batches as Arrow
  -> incremental_scan_stream_into_summary(stream) // live-Add / Remove key sets; consumes the stream
```

The module-level docs in `incremental_scan.rs` are the source of truth for the error contract
(any `next_arrow` error kills the stream), the `OptionalValue::None` full-scan-fallback signal,
the pass-through-field version caveat (kernel issue #2552), and handle release. The Arrow batch
reuses `ScanMetadataArrowResult` with null `transforms`.

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

Catalog-managed publish flow (after a catalog committer stages commits):

```
committed_transaction_post_commit_snapshot()
  -> snapshot_publish_with_committer(snapshot, committer, engine)
     // borrows snapshot; consumes committer (do not free)
  -> use returned snapshot for subsequent transaction_with_committer / checkpoint
     // mint a fresh get_uc_committer for that transaction -- it also consumes
  -> free_snapshot (returned snapshot) when done
  -> free_snapshot (post-commit input snapshot)
```

`snapshot_publish_with_committer` mirrors kernel `Snapshot::publish`: it copies ratified staged
commits into `_delta_log/` via the catalog committer's `publish()` implementation. The input
snapshot is borrowed; the committer is consumed (do not free). The caller owns the returned
snapshot handle. The returned snapshot carries the published watermark (`max_published_version`)
needed for the next catalog commit; do not continue from the pre-publish post-commit snapshot.

Column defaults (`allowColumnDefaults`) live in `ffi/src/column_default.rs`. The kernel reports
defaults but never materializes them, so the connector fills every omitted column itself:

```
transaction()
  -> transaction_visit_top_level_column_defaults(txn, engine, ctx, visitor)
  -> transaction_ack_column_defaults(txn)   // REQUIRED, else the write context errors with
                                            // FFIKernelError::InvalidTransactionStateError
  -> get_unpartitioned_write_context(txn, engine) ... add_files ... commit
```

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
- `alloc-tracking` -- installs `peak_alloc` as the tracking global allocator; enables meaningful
  `*_native_bytes` / `alloc_tracking_enabled` getters (cdylib only; conflicts with
  another `#[global_allocator]` if linked as an rlib)

## Testing under Miri

CI runs this crate's tests under Miri (the `miri` job in `build.yml`) to catch undefined
behavior in the `unsafe` FFI boundary: raw-pointer reads/writes, `unsafe impl Send/Sync`,
`Handle` conversions (`as_ref` / `clone_as_arc` / `into_inner`), and `free_*`. Miri is a MIR
interpreter, so it runs 10x-100x slower than native and is billed by the interpreted
instruction, not wall-clock work.

The consequence: a test is expensive under Miri in proportion to how much *code it executes*,
not how much it asserts. Tests that construct heavyweight but **safe** machinery -- a
reqwest/rustls client (crypto init), a multi-threaded tokio runtime, an Arrow/parquet write --
cost minutes under the interpreter while exercising none of our `unsafe`. That time buys no
UB detection.

Guidance for adding or triaging FFI tests:

- **Keep under Miri** any test that executes `unsafe` whose correctness Miri can check. This is
  the reason the job exists; do not skip these for speed.
- **Never add new `unsafe` to a Miri-ignored test.** An ignored test is invisible to Miri, so
  `unsafe` introduced in one is never checked for undefined behavior, and nothing in CI reports
  the gap. When a change needs new `unsafe` in an ignored test, either exercise that `unsafe`
  from a test that runs under Miri, or un-skip the test.
- **`#[cfg_attr(miri, ignore)]` is legitimate for two reasons, and only these two:**
  1. Miri cannot run it (e.g. an unsupported foreign function). Before accepting this, check
     whether the blocker is avoidable: local-filesystem storage reaches `std::fs::hard_link`
     (`linkat`), which Miri rejects, but in-memory storage does not. Prefer an in-memory store.
  2. The test executes no `unsafe`, OR only `unsafe` that a kept test already covers, AND it is
     expensive under Miri. Skip only with the coverage argument; skipping for cost alone drops
     UB coverage.
- When you skip under reason 2, **prove the coverage is preserved**: the kept tests' set of
  `unsafe` FFI functions must be a superset of the skipped test's. Name the covering test in the
  `ignore` reason or a nearby comment so a future reader can re-check it.
- Miri's leak check also flags handles a test itself forgot to free. Triage before assuming a bug
  in the code under test: a missing `free_*` in the test is a test fix, while an unjoined
  background thread at teardown can be an artifact of how the test ends.
- Prefer picking the **cheapest** test that crosses a given `unsafe` path over keeping several
  that cross the same path with more safe work each.
- `rest_engine` and the checkpoint tests in `lib.rs` document worked examples of this split.
- `-Zmiri-provenance-gc=1000000` on the Miri step is a pure speed knob (GC frequency) and does
  not weaken detection. Do NOT add `-Zmiri-disable-stacked-borrows`, `-disable-validation`,
  `-disable-data-race-detector`, or `-Zmiri-preemption-rate=0`: the first three are unsound, and
  the last reduces data-race schedule exploration.
