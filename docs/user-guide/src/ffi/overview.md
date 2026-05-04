# FFI (C/C++ integration)

The `delta_kernel_ffi` crate exposes delta-kernel-rs to C and C++ through a stable FFI
boundary. It uses [cbindgen](https://github.com/mozilla/cbindgen) to generate header
files (`.h` and `.hpp`) at build time. This matters because it lets you build a
[connector](../connector/overview.md) in any language that can call C functions, not
only Rust.

## Building the FFI crate

The crate can be built as a shared library (`cdylib`) or static library (`staticlib`):

```bash
# Shared library (e.g. libdelta_kernel_ffi.so / .dylib / .dll)
cargo build -p delta_kernel_ffi --release

# Generated headers are written to target/ffi-headers/
# - delta_kernel_ffi.h   (C)
# - delta_kernel_ffi.hpp (C++)
```

### Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `default-engine-rustls` | yes | Includes the `DefaultEngine` with rustls TLS |
| `default-engine-native-tls` | no | Includes the `DefaultEngine` with native TLS (instead of rustls) |
| `arrow` | yes | Enables Arrow integration (selects `arrow-58` by default) |
| `arrow-58` | yes | Pin to Arrow 58 explicitly (enabled transitively by `arrow`) |
| `arrow-57` | no | Pin to Arrow 57 explicitly |
| `delta-kernel-unity-catalog` | no | Enables Unity Catalog integration for catalog-managed tables |
| `tracing` | no | Enables tracing/logging support via `tracing-subscriber` |

> **Note:** You must enable exactly one of `default-engine-rustls` or
> `default-engine-native-tls`. The `default-engine-base` feature contains
> shared implementation details and is not meant to be enabled directly.

## The handle system

Objects that cross the FFI boundary are wrapped in **handles**. These are opaque pointers
that carry ownership semantics. There are two kinds:

- **Mutable handles** (`Box`-like) represent exclusive ownership. Dropping the handle
  drops the underlying object. These are neither `Copy` nor `Clone`.
- **Shared handles** (`Arc`-like) represent shared ownership. Dropping the handle only
  drops the underlying object if it was the last reference.

Every handle has a corresponding `free_*` function that you must call to release it.
For example, `free_engine`, `free_snapshot`, `free_scan`, `free_transaction`.

Several FFI functions _consume_ their handle argument and return a new handle. After
calling such a function, you must not use the old handle. The function documentation
notes this with "CONSUMES the handle."

## Core API surface

The FFI mirrors the Rust API. A typical read flow looks like (no transaction
is needed for reads):

```text
get_default_engine()        ->  Handle<SharedExternEngine>
        |
get_snapshot_builder()      ->  Handle<MutableFfiSnapshotBuilder>
        |
snapshot_builder_build()    ->  Handle<SharedSnapshot>
        |
      scan()                ->  Handle<SharedScan>
        |
scan_metadata_iter_init()   ->  Handle<SharedScanMetadataIterator>
        |
  (read parquet, apply transforms, apply selection vectors)
```

A typical write flow:

```text
get_default_engine()  ->  Handle<SharedExternEngine>
        |
    transaction()     ->  Handle<ExclusiveTransaction>
        |
  with_engine_info()  ->  Handle<ExclusiveTransaction>
        |
    add_files()
        |
    commit()          ->  ExternResult<u64>  (committed version)
```

For more control over scans, you can use the scan builder API instead of the
convenience `scan()` function:

```text
scan_builder()                ->  Handle<ExclusiveScanBuilder>
        |
scan_builder_with_predicate() ->  Handle<ExclusiveScanBuilder>
        |
scan_builder_with_schema()    ->  Handle<ExclusiveScanBuilder>
        |
scan_builder_build()          ->  Handle<SharedScan>
```

### Public FFI functions

The tables below group the stable FFI functions by purpose. Unless noted,
each function is available with the default feature flags. For the full,
authoritative list and signatures, consult the generated
`delta_kernel_ffi.h` header.

**Engine creation**

| Function | Purpose |
|----------|---------|
| `get_default_engine` | Create an engine from a table path with default options |
| `get_engine_builder` / `set_builder_option` / `builder_build` | Create an engine with custom storage options |
| `set_builder_with_multithreaded_executor` | Configure the builder to use a multi-threaded tokio executor |
| `free_engine` | Release the engine handle |

**Snapshots**

| Function | Purpose |
|----------|---------|
| `get_snapshot_builder` | Create a snapshot builder from a table path |
| `get_snapshot_builder_from` | Create a snapshot builder incrementally from an existing snapshot |
| `snapshot_builder_set_version` | Pin the snapshot to a specific table version |
| `snapshot_builder_set_log_tail` | Provide a log tail for catalog-managed tables |
| `snapshot_builder_set_max_catalog_version` | Bound the snapshot to the version the catalog has ratified |
| `snapshot_builder_build` | Consume the builder and produce the snapshot |
| `free_snapshot_builder` / `free_snapshot` | Release snapshot-related handles |

**Snapshot inspection**

Use these on a `Handle<SharedSnapshot>` to read table metadata, protocol, and
partition columns without building a scan.

| Function | Purpose |
|----------|---------|
| `version` | Return the version number of a snapshot |
| `snapshot_timestamp` | Return the snapshot's commit timestamp (milliseconds since epoch) |
| `snapshot_table_root` | Return the table root URL as an engine-allocated string |
| `logical_schema` | Return the table's logical schema as a `Handle<SharedSchema>` |
| `snapshot_get_metadata` | Clone the table metadata into a `Handle<SharedMetadata>` |
| `snapshot_get_protocol` | Clone the protocol into a `Handle<SharedProtocol>` |
| `get_partition_column_count` / `get_partition_columns` | Count partition columns and iterate their names as a `StringSliceIterator` |
| `string_slice_next` / `free_string_slice_data` | Iterate and release a `StringSliceIterator` (e.g. returned by `get_partition_columns`) |
| `get_app_id_version` | Look up the last committed transaction version for an `app_id` (the read side of [idempotent writes](../writing/idempotent_writes.md)) |
| `free_metadata` / `free_protocol` / `free_schema` | Release the corresponding handles |

**Schema and metadata visitors**

Kernel exposes schemas, protocols, and metadata through visitor callbacks so
the engine can materialize them into its own types without Kernel allocating
engine-owned memory.

| Function | Purpose |
|----------|---------|
| `visit_schema` | Walk a `SharedSchema` by invoking per-field callbacks on an `EngineSchemaVisitor` |
| `visit_protocol` | Invoke a `visit_versions` callback, then a `visit_feature` callback per reader/writer feature |
| `visit_metadata` | Invoke a single callback with `(id, name, description, format_provider, has_created_time, created_time_ms)` |
| `visit_metadata_configuration` | Iterate the `configuration` key/value map (takes a snapshot handle, not a metadata handle) |
| `visit_string_map` / `get_from_string_map` | Iterate or look up entries in an opaque `CStringMap` (used by both metadata and scan-metadata surfaces) |

See [Visitor callbacks](#visitor-callbacks) below for the pattern.

**Schema construction (projection pushdown)**

The build-side counterpart to `visit_schema`: per-field callbacks that let the
engine construct a Kernel `StructType` from its own type system (for example,
to pass to `scan_builder_with_schema`).

| Function | Purpose |
|----------|---------|
| `visit_field_byte` / `visit_field_short` / `visit_field_integer` / `visit_field_long` / `visit_field_float` / `visit_field_double` / `visit_field_boolean` | Build a numeric or boolean primitive `StructField` |
| `visit_field_string` / `visit_field_binary` / `visit_field_date` / `visit_field_timestamp` / `visit_field_timestamp_ntz` | Build a string, binary, or date/time primitive `StructField` |
| `visit_field_decimal` | Build a decimal `StructField` with explicit precision and scale |
| `visit_field_struct` / `visit_field_array` / `visit_field_map` / `visit_field_variant` | Build a complex `StructField` (struct, array, map, or variant) from previously created field or struct IDs |

**Reading (scans)**

| Function | Purpose |
|----------|---------|
| `scan` | Create a scan with optional predicate and projection (convenience function) |
| `scan_builder` / `scan_builder_with_predicate` / `scan_builder_with_schema` / `scan_builder_build` | Build a scan incrementally with the builder pattern |
| `scan_logical_schema` / `scan_physical_schema` / `scan_table_root` | Inspect the scan's logical/physical read schemas and table root |
| `scan_metadata_iter_init` / `scan_metadata_next` | Iterate over scan metadata (per-file lists, deletion vectors, transforms) |
| `scan_metadata_next_arrow` / `free_scan_metadata_arrow_result` | Pull the next scan-metadata batch as an Arrow `RecordBatch` and release it (requires `default-engine-base`) |
| `visit_scan_metadata` | Invoke a callback for each scan file in a `SharedScanMetadata` batch |
| `selection_vector_from_scan_metadata` | Materialize the per-row selection bitmap from a scan-metadata batch |
| `selection_vector_from_dv` / `row_indexes_from_dv` | Materialize a selection bitmap or row-index array from a `DvInfo` |
| `get_transform_for_row` | Look up the per-file transform expression for a given row in a scan-metadata batch |
| `free_scan` / `free_scan_builder` / `free_scan_metadata` / `free_scan_metadata_iter` / `free_bool_slice` / `free_row_indexes` | Release scan-related handles and allocations |

**Engine data and Arrow interop**

Rows marked `(requires default-engine-base)` are only compiled when that
feature is enabled; the rest are always available.

| Function | Purpose |
|----------|---------|
| `engine_data_length` | Return the row count of an `ExclusiveEngineData` batch |
| `get_engine_data` | Import Arrow C Data Interface array + schema into an `ExclusiveEngineData` (requires `default-engine-base`) |
| `get_raw_arrow_data` | Export an `ExclusiveEngineData` batch as Arrow C Data Interface structs (requires `default-engine-base`) |
| `read_result_next` / `free_read_result_iter` | Iterate a scan's parquet read iterator and release it |
| `free_engine_data` | Drop a single `ExclusiveEngineData` batch |
| `read_parquet_file` | Directly read a single parquet file via the engine's parquet handler |

> **Warning:** `get_raw_engine_data` is always exported (regardless of feature
> flags) but unimplemented. It calls `todo!()` and will panic. Do not use it.

**Writing (transactions)**

| Function | Purpose |
|----------|---------|
| `transaction` | Start a write transaction on the latest snapshot |
| `transaction_with_committer` | Start a transaction with a custom committer |
| `with_engine_info` | Record a free-form engine identifier on the transaction (consumes and returns a new handle) |
| `with_transaction_id` | Set an `(app_id, version)` pair for idempotent writes (consumes and returns a new handle; see [Idempotent Writes](../writing/idempotent_writes.md)) |
| `with_domain_metadata` / `with_domain_metadata_removed` | Attach or remove a domain-metadata entry (each consumes and returns a new handle) |
| `add_files` | Append file-level write metadata to the transaction |
| `set_data_change` | Toggle the transaction's data-change flag (does not consume the handle) |
| `remove_files` | Register Remove actions for the files selected by a scan-metadata batch |
| `commit` | Commit the transaction and return the new version number |
| `free_transaction` | Release the transaction handle without committing |

**Write context and file writing**

Use a `WriteContext` to learn where to write parquet files and what schema to
write. For unpartitioned writes, one context serves the whole transaction.
Partitioned writes (which would use one context per partition) are tracked in
[#2355](https://github.com/delta-io/delta-kernel-rs/issues/2355).

Engines must append their own `<uuid>.parquet` filename (and any subdirectory
layout) onto the returned table root. The kernel-side `WriteContext::write_dir`
helper -- which produces the recommended directory (Hive-style partition paths
for partitioned tables when column mapping is off, or a random 2-char prefix
when column mapping is on) -- is internal and has no FFI binding.

| Function | Purpose |
|----------|---------|
| `get_unpartitioned_write_context` | Get a `SharedWriteContext` covering all rows in the transaction |
| `get_write_path` | Return the table root URL from a `SharedWriteContext` (engines append their own subdirectory and filename) |
| `get_write_schema` | Return the logical (user-facing) write schema from a `SharedWriteContext` |
| `free_write_context` | Release the write-context handle |

**Domain metadata**

| Function | Purpose |
|----------|---------|
| `get_domain_metadata` | Look up the configuration string for a specific domain on a snapshot |
| `visit_domain_metadata` | Iterate all domain metadata entries on a snapshot |

**Table creation**

| Function | Purpose |
|----------|---------|
| `get_create_table_builder` | Create a builder for a new Delta table with a schema |
| `create_table_builder_with_table_property` | Add a table property to the builder |
| `create_table_builder_build` | Consume the builder and produce a create-table transaction using the default (filesystem) committer |
| `create_table_builder_build_with_committer` | Consume the builder and produce a create-table transaction with a custom committer |
| `create_table_with_engine_info` | Attach a free-form engine identifier to a create-table transaction (consumes and returns a new handle) |
| `create_table_set_data_change` | Toggle the data-change flag on a create-table transaction (does not consume the handle) |
| `create_table_get_unpartitioned_write_context` | Get a `WriteContext` to stage initial data files during table creation |
| `create_table_add_files` | Register file metadata for initial data being written alongside the CREATE TABLE commit |
| `create_table_commit` | Commit the create-table transaction |
| `free_create_table_builder` | Release a create-table builder handle (before it is consumed by `create_table_builder_build*`) |
| `create_table_free_transaction` | Release a create-table transaction handle (after build, before commit) |

**Change data feed (table changes)**

Incremental log reads for change data feed. Mirrors the Rust
[`TableChanges`](../reading/change_data_feed.md) API. The entire `table_changes`
module is gated behind the `default-engine-base` feature.

| Function | Purpose |
|----------|---------|
| `table_changes_from_version` | Open a `TableChanges` from `start_version` to the latest |
| `table_changes_between_versions` | Open a `TableChanges` between `start_version` and `end_version` (inclusive) |
| `table_changes_start_version` / `table_changes_end_version` | Read the requested start/end versions |
| `table_changes_schema` / `table_changes_table_root` | Inspect the CDF schema and table root |
| `table_changes_scan` | Apply an optional predicate and produce a `SharedTableChangesScan` |
| `table_changes_scan_logical_schema` / `table_changes_scan_physical_schema` / `table_changes_scan_table_root` | Inspect the resulting scan |
| `table_changes_scan_execute` | Produce an iterator of CDF data |
| `scan_table_changes_next` | Pull the next `*mut ArrowFFIData` batch from the CDF iterator; the engine must release each non-null batch via `free_arrow_ffi_data` |
| `free_table_changes` / `free_table_changes_scan` / `free_scan_table_changes_iter` | Release CDF-related handles |

> [!WARNING]
> `scan_table_changes_next` returns `*mut ArrowFFIData` (a heap-allocated
> Arrow C Data Interface batch). C callers must release each non-null result
> with `free_arrow_ffi_data` exactly once. This is an ABI-breaking change
> from earlier releases that used a different return shape, so connectors
> upgrading across that boundary need to update both the type and the
> cleanup path.

**Checkpointing**

| Function | Purpose |
|----------|---------|
| `checkpoint_snapshot` | Write a checkpoint for the given snapshot |

**Unity Catalog integration**

Requires the `delta-kernel-unity-catalog` feature. Lets the engine provide a
catalog-aware committer without implementing the committer trait from scratch.

| Function | Purpose |
|----------|---------|
| `get_uc_commit_client` | Wrap an engine-provided `CCommit` callback in a `SharedFfiUCCommitClient` |
| `get_uc_committer` | Produce a `MutableCommitter` bound to a specific `table_id`, ready to pass to `transaction_with_committer` |
| `free_uc_commit_client` / `free_uc_committer` | Release the corresponding handles |

**Expressions and predicates**

Kernel's expression system is exposed through two parallel surfaces:

- **Build** (engine AST -> Kernel expression): the engine calls
  `visit_engine_expression` / `visit_engine_predicate`, providing an
  engine-side iterator. From inside that callback, the engine uses the
  `visit_expression_*` / `visit_predicate_*` builder functions to append
  Kernel-side nodes (columns, literals, operators) into an internal
  `KernelExpressionVisitorState`. The final handle is a `SharedExpression` or
  `SharedPredicate`.
- **Walk** (Kernel expression -> engine type): the engine calls
  `visit_expression` / `visit_predicate`, supplying an `EngineExpressionVisitor`
  struct whose function pointers Kernel invokes as it traverses the tree.

Most engines need only one of the two surfaces.

| Function | Purpose |
|----------|---------|
| `visit_engine_expression` / `visit_engine_predicate` | Build a Kernel `SharedExpression` / `SharedPredicate` from an engine-side AST (Build surface) |
| `visit_expression` / `visit_predicate` | Walk a Kernel expression or predicate with an `EngineExpressionVisitor` (Walk surface) |
| `visit_expression_ref` / `visit_predicate_ref` | Walk a pre-interned expression or predicate reference (Walk surface) |
| `visit_expression_column` / `visit_expression_struct` / `visit_expression_plus` / `visit_expression_literal_*` / ... | Builder functions the engine calls from inside `visit_engine_expression` to construct Kernel expression nodes; see `delta_kernel_ffi.h` for the full list |
| `visit_predicate_eq` / `visit_predicate_and` / `visit_predicate_or` / ... | Builder functions the engine calls from inside `visit_engine_predicate` to construct Kernel predicate nodes |
| `visit_expression_unknown` / `visit_predicate_unknown` | Builder helpers that bridge an opaque engine operator through Kernel unchanged |
| `visit_kernel_opaque_expression_op_name` / `visit_kernel_opaque_predicate_op_name` | Inspect the name of an opaque op carried through by the above |
| `new_expression_evaluator` / `evaluate_expression` / `free_expression_evaluator` | Compile and invoke an expression against an `EngineData` batch |
| `expressions_are_equal` / `predicates_are_equal` | Compare two expressions or predicates for structural equality |
| `free_kernel_expression` / `free_kernel_predicate` / `free_kernel_opaque_expression_op` / `free_kernel_opaque_predicate_op` | Release the corresponding handles |

**Tracing**

Enable Kernel's internal `tracing` instrumentation. Requires the `tracing`
feature flag. Call at most one of these during a process lifetime; later
calls return `false`.

| Function | Purpose |
|----------|---------|
| `enable_log_line_tracing` | Forward each log line to a `TracingLogLineFn` callback |
| `enable_formatted_log_line_tracing` | Forward fully formatted log lines to a `TracingLogLineFn` callback |
| `enable_event_tracing` | Forward structured `TracingEvent` records instead of log lines |

**Common utilities**

| Function | Purpose |
|----------|---------|
| `allocate_kernel_string` | Create a Kernel-owned string from a `KernelStringSlice` |

## Visitor callbacks

Several FFI entry points take a visitor struct (for example,
`EngineSchemaVisitor` for `visit_schema`, `EngineExpressionVisitor` for
`visit_expression`) or individual callbacks (as in `visit_metadata`). The
pattern is the same in every case:

1. You allocate a context (a `NullableCvoid` you own) that the callbacks can
   write into. Kernel does not interpret this value.
2. You fill in one callback per node kind. Kernel invokes the callbacks
   passing the context plus node-specific arguments (names, types, literal
   values, child handles). For tree-structured inputs (schemas, expressions,
   predicates), callbacks fire in depth-first order; for flat inputs (e.g.
   `visit_metadata`), the callback fires once.
3. When `visit_*` returns, the context holds your engine-side representation.

Callbacks run synchronously on the same thread that called `visit_*`. Strings
passed to callbacks (`KernelStringSlice`) are borrowed for the duration of the
call; copy them if you need to retain them beyond the callback.

## Error handling

Kernel functions that can fail return an `ExternResult<T>`, which is a tagged union:

```c
// C representation (simplified)
typedef enum { Ok, Err } ExternResultTag;
typedef struct {
    ExternResultTag tag;
    union {
        T ok;
        EngineError* err;
    };
} ExternResult;
```

You provide an `allocate_error` callback when creating the engine. Kernel calls this
callback to allocate error objects in your memory space whenever an operation fails.
Because the engine allocates these errors, the engine is also responsible for freeing
them. Kernel returns the error pointer immediately and does not retain it.

The `EngineError` struct contains a `KernelError` enum that classifies the error type
(e.g., `GenericError`, `FileNotFoundError`, `InvalidUrlError`). The error message
string passed to `allocate_error` is only valid for the duration of the callback, so
you must copy it if you need to keep it.

## C examples

The repository ships four runnable C examples under
[`ffi/examples/`](https://github.com/delta-io/delta-kernel-rs/tree/main/ffi/examples).
Each is a complete program that links against `delta_kernel_ffi` and
exercises a different slice of the API.

| Example | Demonstrates |
|---------|--------------|
| [`read-table`](https://github.com/delta-io/delta-kernel-rs/tree/main/ffi/examples/read-table) | The full read path: schema visiting, scan-metadata iteration, and Arrow data handling. Pass `-a` to switch from the callback-based scan-metadata path to the Arrow batch-mode path (`scan_metadata_next_arrow`). |
| [`read-table-changes`](https://github.com/delta-io/delta-kernel-rs/tree/main/ffi/examples/read-table-changes) | Reading a [change data feed](../reading/change_data_feed.md) using `table_changes_*` and consuming `ArrowFFIData` batches from `scan_table_changes_next`. |
| [`create-table`](https://github.com/delta-io/delta-kernel-rs/tree/main/ffi/examples/create-table) | Creating a new Delta table via the `get_create_table_builder` / `create_table_builder_build` / `create_table_commit` flow. |
| [`write-table`](https://github.com/delta-io/delta-kernel-rs/tree/main/ffi/examples/write-table) | Appending data to an existing table via the `transaction` / `add_files` / `commit` flow. |

The high-level flow in the `read-table` example:

```c
// 1. Create an engine
ExternResultHandleSharedExternEngine engine_res =
    get_default_engine(table_path, allocate_error);

// 2. Build a snapshot
ExternResultHandleMutableFfiSnapshotBuilder builder_res =
    get_snapshot_builder(table_path, engine);
ExternResultHandleSharedSnapshot snap_res =
    snapshot_builder_build(builder);

// 3. Create a scan
ExternResultHandleSharedScan scan_res =
    scan(snap, engine, NULL, NULL);

// 4. Iterate over scan metadata and read data
// ... (see the full example for details)

// 5. Clean up
free_scan(the_scan);
free_snapshot(snap);
free_engine(engine);
```

## What's next

- [Building a Connector](../connector/overview.md): implementing a custom engine in
  Rust
- [The Engine Trait](../concepts/engine_trait.md): understanding the engine interface
  that the FFI wraps
