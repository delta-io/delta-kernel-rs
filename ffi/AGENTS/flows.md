# FFI flows

Key files and call sequences for the exported FFI flows. The boundary rules are in
`ffi/AGENTS.md`.

## Key Files

- `src/lib.rs` -- main FFI entry points and type definitions
- `src/delta_types.rs` -- reusable borrowed C representations of Delta state and actions,
  with their backing storage and conversions colocated
- `src/snapshot/builder.rs` -- snapshot builders and their configuration
- `src/snapshot/hint/` -- typed snapshot-hint input and callback export
- `src/handle.rs` -- opaque handle system for passing Rust objects across FFI
- `src/column_default.rs` -- column-default (`allowColumnDefaults`) reads and the write-path ack
- `src/scan.rs` -- scan FFI interface
- `src/schema_visitor.rs` -- visitor pattern for schema traversal
- `src/ffi_tracing.rs` -- log, metrics, and frame callback registration
  (`#[cfg(feature = "tracing")]`)
- `src/ffi_metrics.rs` -- `repr(C)` mirror of kernel `MetricEvent` types (`#[cfg(feature = "tracing")]`)
- `src/alloc_stats.rs` -- `peak_alloc` global allocator and native-heap FFI getters
  (`alloc-tracking`)

## Read Flow

```
get_default_engine() -> get_snapshot_builder() -> snapshot_builder_build() -> scan()
  -> scan_metadata_iter_init() -> scan_metadata_next() -> read + transform
```

Snapshot builder API (`ffi/src/snapshot/builder.rs` and `ffi/src/snapshot/hint/mod.rs`):
- `get_snapshot_builder(path, engine)` -- fresh snapshot from a table path
- `get_snapshot_builder_from(old_snapshot, engine)` -- incremental update reusing an existing snapshot (avoids re-reading the log)
- `snapshot_builder_with_version(builder, version)` -- optional: pin to a specific version
- `snapshot_builder_with_incremental_crc_replay(builder, mode)` -- optional: advance a stale CRC
  within a commit limit or without a limit; defaults to disabled for fresh and incremental builds
- `snapshot_builder_with_log_tail(builder, log_tail)` -- optional: set log tail (for catalog-managed tables)
- `snapshot_builder_with_max_catalog_version(builder, version)` -- optional: set max catalog version (for catalog-managed tables)
- `snapshot_builder_with_snapshot_hint(builder, hint)` -- optional: validate and copy a complete
  typed snapshot hint into the builder. Log paths may name published or staged commits, checkpoint
  files, or CRC files; log compaction paths are rejected. Kernel validates every supplied path beneath
  the builder's `_delta_log` root before grouping and checkpoint selection, preserving supplied
  locations. The connector must canonicalize paths into the same URL form as the table root.
  A failed call consumes and drops the builder
- `snapshot_builder_build(builder)` -- consume the builder and produce a `SharedSnapshot`
- `free_snapshot_builder(builder)` -- discard without building (e.g. on error paths)

Each `snapshot_builder_with_*` call consumes its input handle and returns the updated handle on
success. The caller must replace the input handle with that result. On error, the builder is
dropped. Snapshot-hint inputs and all nested pointers are borrowed only for the call and copied
into the builder. Cross-component and table validation occurs when the builder is built. The caller
must eventually pass the final returned handle to either `snapshot_builder_build` or
`free_snapshot_builder`.

`snapshot_to_snapshot_hint(snapshot, engine, context, visitor)` exports retained state without
engine I/O through one callback with a complete borrowed `FfiSnapshotHint`. Both input handles are
borrowed. The callback runs once on success and is not invoked on error. All nested pointers expire
when the callback returns; copy retained values during the callback. No hint handle is returned.
Supply the copied values through `snapshot_builder_with_snapshot_hint` when rebuilding a snapshot.
The hint preserves build-time freshness, not current freshness, and includes only a matching
checkpoint hint and an at-version CRC. Compaction files are unsupported.

Export includes an explicit publication watermark,
preserving absence even when published commits are not represented by the retained paths.
`FfiSnapshotHint.publication_watermark` is a tagged `FfiPublicationWatermark`:
`InferFromLogPaths` derives publication from supplied published commit paths,
`NoPublishedCommits` explicitly preserves absence without inference, and `PublishedThrough(version)`
preserves the observed highest published version independently of the retained paths. Export
always uses an explicit variant. Neither the snapshot version nor its freshness claim is a
publication watermark.
When built with `adaptive-metadata-in-dev`, typed export rejects CRCs containing experimental
adaptive-metadata `lastManifestCommit` or Add `backReference` state.

`snapshot_log_segment(snapshot, context, visitor)` exposes the existing retained segment without
I/O through one borrowed `FfiLogSegment` callback. It includes commit, compaction, and top-level
checkpoint files, optional latest commit/CRC metadata, checkpoint version, and the known publication
watermark. It preserves staged paths independently of that watermark. The snapshot is borrowed;
copy all retained values before the callback returns. It does not implement snapshot equivalence
or establish freshness.

Snapshot accessors (`ffi/src/lib.rs`) include no-I/O reads such as `version` and
`snapshot_file_stats`, which returns `OptionalValue<FfiFileStats>` (scalar
`num_files` / `table_size_bytes` from the CRC; `None` when the snapshot has no CRC, or its CRC lacks
complete file stats). `visit_file_size_histogram` exposes the optional variable-length histogram in
the same file stats: it invokes one callback with borrowed `i64` slices for bin boundaries, file
counts, and total bytes; callers must copy values they retain after the callback.

`snapshot_timestamp` uses a synchronous `SnapshotReader` and may read commit JSON when the
In-Commit Timestamp is unavailable from retained CRC state. Domain-metadata and transaction-version
FFI reads also use an unscoped reader, preserving the engine's cancellation and timeout behavior.
The C API signatures do not expose reader state or a cancellation token.

Domain-metadata reads live in `ffi/src/domain_metadata.rs`: `get_domain_metadata` /
`visit_domain_metadata` for user domains, and `visit_clustering_columns`, which reports one
descriptor per clustering column -- logical name, physical name (what per-file stats are keyed on),
and a type tag -- without exposing the guarded `delta.*` domain JSON directly. It returns
`ExternResult<OptionalValue<usize>>`: `None` means not clustered, `Some(0)` means clustered on no
columns. The type tag reuses the `visit_expression_literal_null` encoding, with 255 for types that
don't fit a compact tag (struct, array, map, variant, void, and geometry/geography).

## Commit Range Flow

A `CommitRange` describes a contiguous range of a table's commits. Build one via the commit range
builder (`ffi/src/commit_range.rs`):

```
commit_range_builder_for(path, start_version, engine)
  -> builder = commit_range_builder_with_end_version(builder, end_version) // optional
  -> builder = commit_range_builder_with_log_tail(builder, log_tail, max)  // optional
  -> builder = commit_range_builder_with_max_catalog_version(builder, max) // optional
  -> commit_range_builder_build(builder)                         // -> SharedCommitRange, always consume builder
  -> commit_range_commits(range, engine, actions, actions_len)   // -> SharedCommitActionsIterator
       // or commit_range_commits_with_snapshot(range, engine, start_snapshot, actions, actions_len)
  -> commit_range_commits_next(iter, ctx, visitor)               // visitor receives a SharedCommitAction
       // in the visitor: commit_action_version / commit_action_timestamp
       //                  commit_action_get_actions(action, engine) -> ExclusiveFileReadResultIterator
       //                    -> read_result_next(...) -> free_read_result_iter(...)
```

Each `commit_range_builder_with_*` call consumes its input handle and returns the updated handle on
success; on error, it drops the builder. The caller must pass the final returned handle to either
`commit_range_builder_build` or `free_commit_range_builder`. Release the range with
`free_commit_range` and the commits iterator with `free_commit_actions_iter`. Each
`SharedCommitAction` handed to the visitor must be released with `free_commit_action`.

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

Rust builders are aliases for `TransactionBuilder<CreateTable>` and
`TransactionBuilder<UpdateTable>`, both using the default `Imperative` execution mode. Rust callers
can select another mode on the builder; the FFI descriptors expose imperative transactions.
Keep their concrete opaque FFI descriptors and explicit
`create_table_txn_builder_*` / `update_table_txn_builder_*` exports. Shared Rust setters do not imply
a universal C handle; custom setters are exposed only for their applicable builder descriptor.
Rust-only builder aliases carry `cbindgen:ignore` so header generation uses the descriptors rather
than monomorphizing builder state.

```
get_default_engine() -> get_snapshot_builder() -> snapshot_builder_build()
  -> new_update_table_txn_builder(snapshot, operation, engine_info, data_change, engine)
  -> update_table_txn_builder_build()
  -> update_table_txn_add_files() -> update_table_txn_commit()
                    |
                    v
              committed_transaction_version / committed_transaction_post_commit_snapshot
                                                                                  |
                                                                                  v
                                                                  free_committed_transaction
```

Update operations, engine info, and `data_change` are required at construction and cannot be changed.
`new_update_table_txn_builder` accepts a typed operation;
`new_update_table_txn_builder_with_custom_operation` accepts a connector-defined name.
Both borrow all inputs and use the engine for UTF-8 errors. They retain a snapshot reference only
after decoding the strings. Custom-name restrictions are validated at build. Create-table
constructors likewise require engine info and `data_change`, with the operation fixed to
`CREATE TABLE`.

`update_table_txn_commit()` and `create_table_txn_commit()` return a `Handle<ExclusiveCommittedTransaction>` that the caller can read via `committed_transaction_version` and `committed_transaction_post_commit_snapshot`, then must release with `free_committed_transaction`. The post-commit snapshot, when present, is a separate `SharedSnapshot` handle that must be freed with `free_snapshot`.

Transaction identifiers can be added before build with the `*_txn_builder_with_transaction_id`
setters, or after build with `update_table_txn_with_transaction_id` /
`create_table_txn_with_transaction_id`. The post-build setters consume the transaction on success
or error and return its replacement on success. Duplicate application ids among builder ids are
rejected at build. Duplicates involving ids added after build, including collisions with builder
ids, are rejected at commit.

Write context: `update_table_txn_get_unpartitioned_write_context` covers unpartitioned tables. For partitioned tables, build a `PartitionValueMap` (`partition_value_map_new` + the typed `partition_value_map_insert_*` functions, one entry per partition column keyed by logical name) and pass it to `update_table_txn_get_partitioned_write_context` (consumes the map). Then use `get_write_dir` for the partition's target directory (Hive-style prefix or random prefix), `visit_partition_values` to read the physical `partitionValues` to record in each Add action, and `resolve_file_path` to turn a written file's URL into its relative `add.path`. The `create_table_*` variants apply the same flow to a create-table transaction whose partition columns were declared with `create_table_txn_builder_with_partition_columns`.

Catalog-managed publish flow (after a catalog committer stages commits):

```
committed_transaction_post_commit_snapshot()
  -> snapshot_publish_with_committer(snapshot, committer, engine)
     // borrows snapshot; consumes committer (do not free)
  -> use returned snapshot for a subsequent update_table_txn_builder_build_with_committer / checkpoint
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
new_update_table_txn_builder(snapshot, operation, engine_info, data_change, engine)
  -> update_table_txn_builder_build()
  -> update_table_txn_visit_top_level_column_defaults(txn, engine, ctx, visitor)
  -> update_table_txn_ack_column_defaults(txn)   // REQUIRED, else the write context errors with
                                            // FFIKernelError::InvalidTransactionStateError
  -> update_table_txn_get_unpartitioned_write_context(txn, engine)
  -> update_table_txn_add_files(txn, metadata)
  -> update_table_txn_commit(txn, engine)
```

Deletion vector update flow:

```
new_update_table_txn_builder(snapshot, operation, engine_info, data_change, engine)
  -> update_table_txn_builder_build()
  -> dv_descriptor_map_new()
  -> dv_descriptor_new()
  -> dv_descriptor_map_insert()
  -> scan() -> scan_metadata_iter_init()
  -> update_table_txn_update_deletion_vectors()
  -> update_table_txn_commit()
```

The engine authors the DV file and passes descriptor fields to `dv_descriptor_new`. The
descriptor map and scan iterator are both consumed by `update_table_txn_update_deletion_vectors`.
Descriptor handles are consumed by `dv_descriptor_map_insert` regardless of the result. DV
updates require both the `deletionVectors` reader/writer feature and
`delta.enableDeletionVectors=true`.

For distributed writes, `update_table_txn_write_state` returns an owned state handle that outlives the
transaction. `write_state_encode` returns opaque bytes to copy to workers. Decode them once on each
worker with `write_state_decode`. Local writers can skip this round trip.
Create-table writes use the `create_table_txn_get_*_write_context` functions and do not expose
transportable write state.
Create a builder for each output partition with `write_context_builder`. Partitioned writers set
values keyed by logical names with `write_context_builder_with_partition_values` or exact physical
names with `write_context_builder_with_physical_partition_values`; writers that provide materialized
row-tracking columns also call `write_context_builder_with_row_tracking_columns`. Finish with
`write_context_builder_build`. Builders and bound contexts hold their own state reference, so they
remain valid after `free_write_state`. Drop an unused builder with `free_write_context_builder` and
a bound context with `free_write_context`. Driver and workers must use the same kernel version.
`get_write_state_stats_columns` passes every physical statistics column path to one allocation
callback as a borrowed `FfiSlice<FfiColumnName>`. `snapshot_physical_schema` returns the full
physical table schema, which may contain fields that are not stored in data files.

## Tracing & Metrics

Gated behind the `tracing` feature. A single global `tracing` subscriber backs logging, metrics,
and frame lifecycle reporting; it is installed lazily the first time any `enable_*` function below
is called. The subscriber has three reloadable slots: a logging layer (swapped wholesale between
event-based and log-line formats), a metrics layer (a fixed `ReportGeneratorLayer` toggled on/off
via a reloadable level filter), and a frame layer (a fixed `FrameReporterLayer` toggled on/off via
a reloadable level filter).

Logging registration (each re-callable to replace the active callback, format, and level):
- `enable_event_tracing(callback, max_level)` -- structured `Event`s; the engine formats them
- `enable_log_line_tracing(callback, max_level)` -- pre-formatted log lines, default options
- `enable_formatted_log_line_tracing(callback, max_level, format, ansi, with_time, with_level, with_target)`
  -- pre-formatted log lines with explicit formatting options

Metrics registration:
- `enable_metrics_reporting(callback)` -- forwards each kernel `MetricEvent` to the callback as a
  `repr(C)` `MetricEvent` (see `src/ffi_metrics.rs`). Re-calling replaces the callback.

Frame lifecycle registration:
- `enable_frame_reporting(callback)` -- forwards OPEN/CLOSE for each dynamic activation of a span
  declaring the static `enable_call_frame` field. The callback runs synchronously on the entering
  or exiting thread and receives a tagged `FrameEvent` union. OPEN includes the span id and a
  borrowed UTF-8 name; CLOSE includes the matching id. Calls may overlap across threads; callback
  state must be thread-safe, and profile consumers must maintain a separate event stack for each
  callback thread. Registration is one-shot so a callback cannot be replaced between a span's OPEN
  and CLOSE events; another call fails and leaves the existing callback active.

The `MetricEvent` and any `KernelStringSlice` it carries are only valid for the duration of the
callback. Durations are `u64`, suffixed `_ns` (nanoseconds) or `_ms` (milliseconds). Operation ids
are the raw 16 bytes of the kernel UUID (`MetricId`).
