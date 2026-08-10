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
Kernel never does I/O directly: it delegates all I/O to the Engine trait. Kernel also leaves
columnar memory representation and I/O scheduling to the connector and engine. For example, during
log replay or checkpoint writes, kernel receives opaque `EngineData` batches, inspects them via the
visitor pattern, updates a selection vector, and hands them back to the engine: it never
deserializes the full batch into in-memory structs.

## Snapshot

`Snapshot` (`kernel/src/snapshot/`) is the primary entry point for operations on an existing table.
It is an immutable point-in-time view of a Delta table at a specific version, providing the table
schema, metadata, properties, and version number.

Built via `Snapshot::builder_for(url).build(engine)` (latest version) or
`.at_version(v).build(engine)` (specific version). For catalog-managed tables,
`.with_log_tail(commits)` supplies recent unpublished commits from the catalog and
`.with_max_catalog_version(v)` caps the snapshot at the latest catalog-ratified version.

**Snapshot loading internals:**
1. **LogSegment** (`kernel/src/log_segment/`): discovers commits + checkpoints for the
   requested version, replays Protocol and Metadata (`protocol_metadata_replay.rs`), and
   replays domain metadata (`domain_metadata_replay.rs`)
2. **Log replay** (`kernel/src/log_replay/`): file-action deduplication via
   `FileActionDeduplicator` and `LogReplayProcessor` trait (distinct from Protocol/Metadata
   replay above)

From a snapshot you can: read the schema and table properties, build a `Scan` to read data,
start a `Transaction` to write data, or create a checkpoint.

## Read Path

`Snapshot` -> `ScanBuilder` -> `Scan` -> data

The scan pipeline: log replay (build active file list) -> data skipping (prune files via stats
and partition values) -> file reading -> physical-to-logical transform (partition values,
column mapping, schema evolution) -> deletion vector filtering.

**Key modules** (`kernel/src/scan/`): `log_replay.rs` (reconcile Add/Remove into active file
set), `data_skipping.rs` (rewrite predicates against min/max/nullCount stats and partition values).

**Execution paths:**
- `scan.execute(engine)`: kernel handles everything end-to-end, returns `EngineData`
- `scan.scan_metadata(engine)`: returns file list + transforms; connector reads files and
  calls `transform_to_logical` / `DvInfo::get_selection_vector`
- `scan.parallel_scan_metadata(engine)`: two-phase distributed log replay (requires the
  `internal-api` feature)

**Incremental read:** `Snapshot::incremental_scan_builder(base_version)` streams the file-action
diff over `(base_version, target_version]`: live Adds as a `FilteredEngineData` iterator
plus a terminal summary of live Add and Remove file keys. Use this to advance a cached
file listing without re-scanning the table.

## Write Path

`Snapshot` -> `Transaction` -> (`WriteState` -> `BoundWriteContext`) -> commit

<<<<<<< HEAD

The kernel coordinates the write transaction: it freezes table-wide write configuration in a
serializable `WriteState`, binds partition values into a `BoundWriteContext` (validated partition
values, recommended write directory, physical schema, stats columns), assembles commit actions
(CommitInfo, Add files), enforces protocol compliance (table features, schema validation), and
delegates the atomic commit to a `Committer`.
=======
Kernel captures table-wide configuration in a transportable `WriteState`. Each writer binds
partition values to create a `BoundWriteContext`; the transaction then registers the resulting
files and commits them.
>>>>>>> 78d1a6cc (self review feedback and humanize)

**Data-write steps:**
1. Create `Transaction` from a snapshot with a `Committer` (e.g. `FileSystemCommitter`)
2. Get `BoundWriteContext` directly from the transaction, or transport `txn.write_state()` and bind
   partition values on a distributed writer
3. Write Parquet files (via engine), collect file metadata
4. Register files via `txn.add_files(metadata)` and stage any removals or deletion-vector updates
5. Commit: returns `CommittedTransaction`, `ConflictedTransaction`, or `RetryableTransaction`

- **Transaction** (`kernel/src/transaction/`): blind append writes, file removals, deletion-vector
  updates, table creation (including clustered tables via `DataLayout`), and limited schema
  evolution
- **Committer** (`kernel/src/committer/`): commit coordination. `FileSystemCommitter` for
  filesystem tables (atomic put-if-absent to `_delta_log/`); custom `Committer` implementations
  for catalog-managed tables (staging, ratifying, publishing).

## Engine Trait System

The kernel is built around the `Engine` trait (`kernel/src/lib.rs`), which provides the required
handlers below and an optional `PlanExecutor` under the `declarative-plans` feature:

| Handler              | Purpose                          | Key Methods                                |
|----------------------|----------------------------------|--------------------------------------------|
| `StorageHandler`     | File system operations           | `list_from`, `read_files`, etc.            |
| `JsonHandler`        | Delta log commit parsing/writing | `parse_json`, `read_json_files`            |
| `ParquetHandler`     | Data file and checkpoint I/O     | `read_parquet_files`, `write_parquet_file`  |
| `EvaluationHandler`  | Expression/predicate evaluation  | `new_expression_evaluator`, etc.           |

Metrics are emitted as tracing events and collected by tracing layers. A `DefaultEngine` (Arrow +
`object_store` + Tokio) lives in `default-engine/src/`. Custom engines only need to replace
specific handlers: they can reuse defaults for the rest.

## EngineData Trait

Kernel never assumes data is Arrow. It uses the `EngineData` trait: an opaque columnar data
interface. The kernel extracts data via a visitor pattern (`visit_rows` with typed `GetData`
accessors), not by inspecting columns directly. Never downcast `EngineData` to a concrete type
(e.g. `ArrowEngineData`) in prod kernel code: only engine *implementations* know the concrete
type. (Unit tests using the default engine may legitimately downcast.)

`DefaultEngine` uses `ArrowEngineData` (wrapping Arrow `RecordBatch`). Custom engines implement
`EngineData` for their own columnar format.

Key methods: `visit_rows`, `len`, `append_columns` (for partition value injection/column mapping),
`apply_selection_vector` (for deletion vectors).

**IMPORTANT:** Never assume that reading one file produces exactly one batch. Always iterate over
all returned batches: the engine may split a single file across multiple batches.

## Key Modules

- `kernel/src/snapshot/`: `Snapshot`, `SnapshotBuilder`, entry point for reads/writes
- `kernel/src/scan/`: `Scan`, `ScanBuilder`, log replay, data skipping
- `kernel/src/incremental_scan/`: `IncrementalScanBuilder`, streaming file-action diff
  between two versions

- `kernel/src/transaction/` -- `Transaction`, `WriteState`, `BoundWriteContext`, `create_table`
  builder
- `kernel/src/partition/` -- partition value validation, serialization, Hive-style path
   encoding, URI encoding for `add.path`
- `kernel/src/committer/`: `Committer` trait, `FileSystemCommitter`
- `kernel/src/log_segment/`: log file discovery, Protocol/Metadata replay
- `kernel/src/log_replay/`: file-action deduplication, `LogReplayProcessor` trait
- `kernel/src/log_reader/`: I/O layer for reading commit and checkpoint files
- `kernel/src/actions/`: Delta action types (Protocol, Metadata, CommitInfo, Add, Remove, Cdc,
   SetTransaction, DomainMetadata, Sidecar, CheckpointMetadata)
- `kernel/src/schema/`: `StructType`/`StructField`/`DataType`, projections
- `kernel/src/expressions/`: expression AST (`Expression`, `Predicate`, `Scalar`),
  `col!` macro
- `kernel/src/transforms/`: generic recursive transforms (`ExpressionTransform`,
  `SchemaTransform`)
- `kernel/src/checkpoint/`: V1 and V2 checkpoint writing (V2 with or without sidecars)
- `kernel/src/crc/`: version checksum reading, writing, and state tracking
- `kernel/src/table_configuration.rs`: table metadata, properties, feature management
- `kernel/src/table_features/`: protocol feature definitions, `TableFeature` enum
- `kernel/src/table_properties.rs`: table property parsing (delta.appendOnly, etc.)
- `kernel/src/table_changes/`: Change Data Feed (CDF) API (`TableChanges`)
- `kernel/src/path.rs`: Delta log path parsing

## Catalog-Managed Tables

Tables whose commits go through a catalog (e.g. Unity Catalog) instead of direct filesystem
writes. Kernel doesn't know about catalogs: the catalog client provides a log tail via
`SnapshotBuilder::with_log_tail()`, caps the version via `with_max_catalog_version()`, and
uses a custom `Committer` for staging/ratifying/publishing commits.

The `UCCommitter` (in the `delta-kernel-unity-catalog` crate) is the reference implementation of a
catalog committer for Unity Catalog. It writes version 0 directly to `_delta_log/`. For later
versions, it stages commits in `_staged_commits/`, calls the UC commit API to ratify them, and
publishes them by atomically copying them to `_delta_log/`.

For versions after 0, commit types are staged (written to `_staged_commits/`), ratified (accepted
by the catalog for a version), and published (copied to `_delta_log/` as a normal Delta file).
