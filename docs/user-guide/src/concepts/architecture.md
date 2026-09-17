# Architecture overview

Delta Kernel is a Rust library that encapsulates the Delta Lake protocol so you can build
connectors without understanding protocol internals. This matters because the Delta protocol
is complex and evolving. Kernel absorbs that complexity and exposes a small, stable API
surface for reading and writing Delta tables.

## Terminology

Before diving in, here are the key terms used throughout this guide:

- **Compute engine** is a data processing framework like Apache Spark, Apache Flink, DuckDB,
  Polars, or DataFusion. Each defines its own DataSource API for reading and writing tables
  (e.g., Spark's DataSourceV2, Flink's Source/Sink APIs).

- **Connector** is the integration layer between a compute engine and Delta tables. It
  implements the compute engine's DataSource API (Table, Scan, Writer, etc.) and uses Delta
  Kernel to fulfill those interfaces. For example, the Delta Spark connector implements
  Spark's `Table`, `ScanBuilder`, `Scan`, and `PartitionReader` interfaces, delegating the
  actual Delta protocol work to Kernel.

- **Delta Kernel** is the library (this project). It implements the Delta protocol and
  exposes APIs (`Snapshot`, `Scan`, `Transaction`) that connectors use. It never does I/O
  directly.

- **Workflow and Generator** are lazy connector-driven operations. Your connector starts a root
  task, advances it, and serves each typed request without Kernel invoking connector code.

- **Engine trait** is the synchronous compatibility abstraction that Kernel calls into. It has four
  required handlers (`StorageHandler`, `ParquetHandler`, `JsonHandler`,
  `EvaluationHandler`) and an optional `PlanExecutor`. Metrics use tracing layers.
  A `DefaultEngine` is provided. Connectors can implement their own for better performance
  with their native data formats and I/O.

## Layered design

Delta Kernel is organized into layers. Each layer has a clear responsibility and a well-defined
interface to the layer above and below it.

```text
 ┌────────────────────────────────────────────┐
 │           Compute Engine                   │
 │   (Spark, Flink, DuckDB, Polars, ...)      │
 └──────────────────┬─────────────────────────┘
                    │  calls DataSource API
 ┌──────────────────▼──────────────────────────┐
 │         Your Delta Connector                │
 │                                             │
 │  Implements the compute engine's DataSource │
 │  API and uses Kernel to fulfill it          │
 └──────────────────┬──────────────────────────┘
                    │  calls Kernel APIs
 ┌──────────────────▼──────────────────────────┐
 │            Delta Kernel (core logic)        │
 │                                             │
 │  Snapshot · Scan · Transaction · Log Replay │
 │  Data Skipping · Predicate Pushdown         │
 │  Protocol Compliance · Table Features       │
 └──────────────┬───────────────────┬───────────┘
                │ typed requests    │ Engine compatibility calls
 ┌──────────────▼────────────┐ ┌────▼────────────────────────┐
 │ Connector-owned driver    │ │ Engine trait implementation │
 │                           │ │                             │
 │ AsyncEngineConnector or   │ │ DefaultEngine or custom     │
 │ your own request loop     │ │ handlers                    │
 └──────────────┬────────────┘ └────┬────────────────────────┘
                └─────────────┬─────┘
                              │ I/O and compute
 ┌────────────────────────────▼─────────────────┐
 │ Arrow or native data · object_store or       │
 │ connector-native storage                     │
 └────────────────────────────┬─────────────────┘
                    │
 ┌──────────────────▼──────────────────────────┐
 │              Storage                        │
 │   (Local FS, S3, GCS, Azure, HDFS, ...)     │
 └─────────────────────────────────────────────┘
```

**Kernel** contains all Delta protocol logic: log replay, data skipping,
schema enforcement, table features, and transaction coordination. It never does I/O directly.

**Connector-driven execution** returns lazy operations. Your connector calls `start` for a root,
retains the task, serves each `Request`, and completes its `Reply`. Kernel code with a parent
channel composes workflows through `run_with` and generators through `bind`. See
[Driving connector workflows](../connector/coroutines.md).

**Engine compatibility** lets synchronous callers use the built-in `DefaultEngine` (Arrow +
`object_store`) or implement the `Engine` trait. See [The Engine trait](./engine_trait.md).

**Your connector** implements your compute engine's DataSource API and calls kernel APIs
(`Snapshot`, `Scan`, `Transaction`) to do the Delta work. The kernel handles the protocol;
your connector controls execution, distribution, and data flow. See
[Building a Connector](../connector/overview.md) for details.

## Key types

### Snapshot

A `Snapshot` is an immutable view of a Delta table at a specific version. It is the entry point
for everything: reading, writing, and inspecting table metadata.

```rust,ignore
let snapshot = Snapshot::builder_for("/path/to/table")
    .build(&engine)?;                // returns Arc<Snapshot>

let snapshot_v5 = Snapshot::builder_for("/path/to/table")
    .at_version(5)
    .build(&engine)?;

println!("Version: {}", snapshot.version());
println!("Schema: {:?}", snapshot.schema());
```

From a snapshot you can:
- Read the table schema and properties
- Build a `Scan` to read data
- Start a `Transaction` to write data
- Create a checkpoint

### Scan

A `Scan` reads data from a table. It is built from a snapshot via `ScanBuilder`:

```rust,ignore
let scan = snapshot
    .scan_builder()
    .with_schema(my_schema)           // column selection (optional)
    .with_predicate(my_predicate)     // filter pushdown (optional)
    .build()?;
```

There are two ways to execute a scan:

**Simple path.** `execute()` does everything for you:

```rust,ignore
for data in scan.execute(engine)? {
    let batch = data?.try_into_record_batch()?;
    // process batch
}
```

**Advanced path.** `scan_metadata()` gives you control over parallelism:

```rust,ignore
for metadata in scan.scan_metadata(engine)? {
    let metadata = metadata?;
    // Each ScanMetadata contains the files to read
    // and per-file transforms to apply.
    // You can distribute these across threads or workers.
}
```

The advanced path is how you build a distributed connector. See
[Building a Connector](../connector/overview.md) for details.

### Transaction

A `Transaction` describes a write to a table. Bind it to a `Committer` before committing. For a
filesystem-managed table, use the convenience method on `Snapshot`:

```rust,ignore
let mut txn = snapshot                              // Arc<Snapshot>
    .transaction_with_filesystem_committer(&engine)?
    .with_operation("INSERT".to_string())
    .with_data_change(true);

// Write Parquet files, then register their metadata.
txn.add_files(file_metadata);

// Commit atomically
match txn.commit(&engine)? {
    (CommitResult::Committed(c), _) => println!("v{}", c.commit_version()),
    (CommitResult::Conflicted(_), _) => { /* handle conflict */ }
    (CommitResult::Retryable(_), _) => { /* retry */ }
}
```

The `file_metadata` argument is an `EngineData` batch that matches
`txn.add_files_schema()`, not raw file paths. See
[Appending data](../writing/append.md) for the full pattern, including how to
build that batch from Parquet write results.

> [!NOTE]
> `with_operation` applies to update transactions (append, delete, etc.). For
> create-table transactions the operation is fixed to `"CREATE TABLE"` and
> cannot be overridden.

For schema evolution, start from the snapshot's `alter_table()` builder
instead of `transaction()`. See
[Altering a Table](../writing/alter_table.md).

See [Quick Start: Writing a Table](../getting_started/quick_start_write.md) for a complete example.

## How an Engine-compatible read works

When you call `scan.execute(engine)`, here is what happens internally:

```text
1. LOG REPLAY (kernel)
   Read delta log commits via engine.json_handler() and checkpoint
   parquet files (including parquet sidecars) via engine.parquet_handler().
   Determine the set of active files for this table version.

2. DATA SKIPPING (kernel)
   If a predicate was provided, evaluate file-level statistics
   (min/max values, null counts) to skip files that cannot match.
   This happens via engine.evaluation_handler().

3. FILE READING (engine)
   For each remaining file, read the Parquet data via
   engine.parquet_handler().read_parquet_files().

4. TRANSFORM (kernel + engine)
   Apply per-file transformations: partition value injection,
   column mapping, deletion vectors. Produces the final logical
   data that your connector consumes.
```

The kernel handles steps 1, 2, and 4. The engine handles step 3. This separation means
the kernel never touches raw bytes. It works purely with metadata and delegates all I/O.

The connector-driven path performs the same stages, but Kernel returns typed requests instead of
calling Engine handlers. Your driver decides how to execute each request.

## How a write works

```text
1. START TRANSACTION (kernel)
   Create a Transaction from a snapshot and bind it to a Committer.
   The snapshot pins the table version you're writing against.

2. WRITE DATA (engine / your code)
   Write Parquet files using the engine. Collect file metadata
   (path, size, statistics) and register it with the transaction
   via add_files().

3. COMMIT (kernel + committer)
   The kernel assembles the commit actions (CommitInfo, Add files,
   etc.) and hands them to the Committer. For filesystem tables,
   the Committer writes a JSON delta file atomically. For
   catalog-managed tables, it goes through the catalog.

4. HANDLE RESULT
   Committed: success.
   Conflicted: another writer committed first.
   Retryable: transient I/O error, safe to retry.
```

## EngineData: staying engine-agnostic

The kernel never assumes your data is Arrow. Instead, it uses the `EngineData` trait, an
opaque interface that any engine can implement. The kernel accesses data through visitor
callbacks, not by inspecting columns directly.

```text
 Kernel                          Engine
 ──────                          ──────
 "I need columns [path, size]"
       ──────────────────>
                                 "Here are GetData accessors
                                  for those columns"
       <──────────────────
 Visits rows via GetData
```

The `DefaultEngine` implements `EngineData` with `ArrowEngineData` (wrapping Arrow
`RecordBatch`). If you use the default engine, you can convert back to `RecordBatch`
with the `EngineDataArrowExt` trait. If you build a custom engine, you implement
`EngineData` for your own columnar format.

See [The Engine Trait](./engine_trait.md) for more on how this works.

## Crate structure

The project is organized into several crates:

| Crate | Description |
|-------|-------------|
| `delta_kernel` | Core library: protocol logic, table operations, coroutine types, and Engine traits |
| `delta_kernel_default_engine` | Arrow and `object_store` implementations for synchronous Engine compatibility and native async coroutine driving |
| `delta_kernel_ffi` | C/C++ Foreign Function Interface for cross-language integration |
| `delta_kernel_derive` | Procedural macros for internal code generation |
| `delta_kernel_unity_catalog` | Unity Catalog integration for catalog-managed tables (see [Unity Catalog Integration](../unity_catalog/overview.md)) |
| `unity_catalog_delta_client_api` | Trait definitions for Unity Catalog client implementations |
| `unity_catalog_delta_rest_client` | REST-based Unity Catalog client built on the client API |
| `acceptance` | Delta Acceptance Tests (DAT) validation suite |
| `test_utils` | Shared test utilities |
| `feature_tests` | Feature flag compatibility tests |

## What's next

- [The Engine Trait](./engine_trait.md) explains the `Engine` abstraction and its handlers in detail.
- [Driving Connector Workflows](../connector/coroutines.md) explains connector-owned execution.
- [Building a Scan](../reading/building_a_scan.md) walks through reading data from a Delta table.
- [Building a Connector](../connector/overview.md) describes how to integrate Kernel with a compute engine.

## See also

- [Schema and Types](./schema_and_types.md) for Kernel's type system.
- [Appending Data](../writing/append.md) for a complete write example.
