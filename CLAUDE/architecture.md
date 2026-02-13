# Architecture

## Layered Design

```
Compute Engine (Spark, Flink, DuckDB, Polars, ...)
  -> Your Delta Connector (implements compute engine's DataSource API)
    -> Delta Kernel (snapshot loading, scan orchestration, write transaction coordination,
       log replay, data skipping, schema enforcement, predicate evaluation,
       physical-to-logical transforms, deletion vector handling, checkpointing)
      -> Engine trait (abstraction for I/O and compute)
        -> DefaultEngine (Arrow + object_store + Tokio) or custom engine
          -> Storage (local FS, S3, GCS, Azure, HDFS, ...)
```

Kernel handles the Delta protocol; connectors handle execution, distribution, and data flow.
Kernel never does I/O directly -- it delegates all I/O to the Engine trait.

## Snapshot

`Snapshot` (`kernel/src/snapshot.rs`) is the entry point for everything. It is an immutable
point-in-time view of a Delta table at a specific version, providing the table schema, metadata,
properties, and version number.

Built via `Snapshot::builder_for(url).build(engine)` (latest version) or
`.at_version(v).build(engine)` (specific version). For catalog-managed tables,
`.with_log_tail(commits)` supplies recent unpublished commits from the catalog.

**Snapshot loading internals:**
1. **LogSegment** (`kernel/src/log_segment/`) -- discovers commits + checkpoints for the
   requested version, replays Protocol and Metadata (`protocol_metadata_replay.rs`), and
   replays domain metadata (`domain_metadata_replay.rs`)
2. **Log replay** (`kernel/src/log_replay.rs`) -- file-action deduplication via
   `FileActionDeduplicator` and `LogReplayProcessor` trait (distinct from Protocol/Metadata
   replay above)

From a snapshot you can: read the schema and table properties, build a `Scan` to read data,
start a `Transaction` to write data, or create a checkpoint.

## Read Path

`Snapshot` -> `ScanBuilder` -> `Scan` -> data

The scan pipeline: log replay (build active file list) -> data skipping (prune files via stats)
-> file reading -> physical-to-logical transform (partition values, column mapping, schema
evolution) -> deletion vector filtering.

**Key modules** (`kernel/src/scan/`): `log_replay.rs` (reconcile Add/Remove into active file
set), `data_skipping.rs` (rewrite predicates against min/max/nullCount stats).

**Execution paths:**
- `scan.execute(engine)` -- kernel handles everything end-to-end, returns `EngineData`
- `scan.scan_metadata(engine)` -- returns file list + transforms; connector reads files and
  calls `transform_to_logical` / `DvInfo::get_selection_vector`
- `scan.parallel_scan_metadata(engine)` -- two-phase distributed log replay (`pub(crate)`,
  requires `internal-api` feature)

## Write Path

`Snapshot` -> `Transaction` -> commit

The kernel coordinates the write transaction: it provides the write context (target directory,
physical schema, stats columns), assembles commit actions (CommitInfo, Add files), enforces
protocol compliance (table features, schema validation), and delegates the atomic commit to a
`Committer`.

**Steps:**
1. Create `Transaction` from a snapshot with a `Committer` (e.g. `FileSystemCommitter`)
2. Get `WriteContext` for target dir, physical schema, and stats columns
3. Write Parquet files (via engine), collect file metadata
4. Register files via `txn.add_files(metadata)`
5. Commit: returns `CommittedTransaction`, `ConflictedTransaction`, or `RetryableTransaction`

- **Transaction** (`kernel/src/transaction/`) -- blind append writes, table creation (via
  `create_table` builder, including clustered tables via `DataLayout`)
- **Committer** (`kernel/src/committer/`) -- commit coordination. `FileSystemCommitter` for
  filesystem tables (atomic put-if-absent to `_delta_log/`); custom `Committer` implementations
  for catalog-managed tables (staging, ratifying, publishing).

## Engine Trait System

The kernel is built around the `Engine` trait (`kernel/src/lib.rs`), which provides four handlers:

| Handler              | Purpose                          | Key Methods                                |
|----------------------|----------------------------------|--------------------------------------------|
| `StorageHandler`     | File system operations           | `list_from`, `read_files`, etc.            |
| `JsonHandler`        | Delta log commit parsing/writing | `parse_json`, `read_json_files`            |
| `ParquetHandler`     | Data file and checkpoint I/O     | `read_parquet_files`, `write_parquet_file`  |
| `EvaluationHandler`  | Expression/predicate evaluation  | `new_expression_evaluator`, etc.           |
| `MetricsReporter`    | Optional observability           | `get_metrics_reporter` (default: None)     |

A `DefaultEngine` (Arrow + `object_store` + Tokio) lives in `kernel/src/engine/default/`. Custom
engines only need to replace specific handlers -- they can reuse defaults for the rest.

## EngineData Trait

Kernel never assumes data is Arrow. It uses the `EngineData` trait -- an opaque columnar data
interface. The kernel extracts data via a visitor pattern (`visit_rows` with typed `GetData`
accessors), not by inspecting columns directly. Never downcast `EngineData` to a concrete type
(e.g. `ArrowEngineData`) in kernel code -- only engine *implementations* know the concrete type.

`DefaultEngine` uses `ArrowEngineData` (wrapping Arrow `RecordBatch`). Custom engines implement
`EngineData` for their own columnar format.

Key methods: `visit_rows`, `len`, `append_columns` (for partition value injection/column mapping),
`apply_selection_vector` (for deletion vectors).

## Key Modules

- `kernel/src/snapshot/` -- `Snapshot`, `SnapshotBuilder`, entry point for reads/writes
- `kernel/src/scan/` -- `Scan`, `ScanBuilder`, log replay, data skipping
- `kernel/src/transaction/` -- `Transaction`, `WriteContext`, `create_table` builder
- `kernel/src/committer/` -- `Committer` trait, `FileSystemCommitter`
- `kernel/src/log_segment/` -- log file discovery, Protocol/Metadata replay
- `kernel/src/log_replay.rs` -- file-action deduplication, `LogReplayProcessor` trait
- `kernel/src/log_reader/` -- I/O layer for reading commit and checkpoint files
- `kernel/src/actions/` -- Delta action types (Protocol, Metadata, CommitInfo, Add, Remove, Cdc,
   SetTransaction, DomainMetadata, Sidecar, CheckpointMetadata)
- `kernel/src/schema/` -- `StructType`/`StructField`/`DataType`, projections
- `kernel/src/expressions/` -- expression AST (`Expression`, `Predicate`, `Scalar`),
  `column_expr!` macro
- `kernel/src/checkpoint/` -- checkpoint writing (V1 and V2 single-file classic-named)
- `kernel/src/table_configuration.rs` -- table metadata, properties, feature management
- `kernel/src/table_features/` -- protocol feature definitions, `TableFeature` enum
- `kernel/src/table_properties.rs` -- table property parsing (delta.appendOnly, etc.)
- `kernel/src/table_changes/` -- Change Data Feed (CDF) API (`TableChanges`)
- `kernel/src/path.rs` -- Delta log path parsing

## Catalog-Managed Tables

Tables whose commits go through a catalog (e.g. Unity Catalog) instead of direct filesystem
writes. Kernel doesn't know about catalogs -- the catalog client provides a log tail via
`SnapshotBuilder::with_log_tail()` and a custom `Committer` for staging/ratifying/publishing
commits. Requires `catalog-managed` feature flag.

The `UCCommitter` (in the `uc-catalog` crate) is the reference implementation of a catalog
committer for Unity Catalog. It stages commits to `_staged_commits/`, calls the UC commit API to
ratify them, and publishes by copying to `_delta_log/`.

Commit types: staged (written to `_staged_commits/`), ratified (accepted by catalog for a
version), published (copied to `_delta_log/` as a normal delta file).
