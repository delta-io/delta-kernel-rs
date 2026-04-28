# Appending Data

To append data to an existing Delta table, you create a `Transaction` from a
[Snapshot](../concepts/architecture.md#snapshot), write Parquet files through the engine, register them,
and commit. For a quick end-to-end example that creates a table and writes data, see
[Quick Start: Writing a Table](../getting_started/quick_start_write.md).

## The write flow

Appending data to a Delta table follows these steps:

1. Get a `Snapshot` of the table
2. Create a `Transaction` from the snapshot
3. Get the `WriteContext` from the transaction
4. Write Parquet files using the engine and `WriteContext`
5. Register the written files with the transaction via `add_files`
6. Commit the transaction

The following example assumes you already have an `engine: DefaultEngine`, which provides
an async `write_parquet` helper. If you use a custom `Engine`, the step 4
may differ.

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::arrow::array::{Int32Array, RecordBatch, StringArray};
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_conversion::TryIntoArrow;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Get a snapshot
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Create a transaction
let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("INSERT".to_string())
    .with_engine_info("my-app/1.0")
    .with_data_change(true);

// 3. Get write context
let write_context = Arc::new(txn.unpartitioned_write_context()?);

// 4. Write Parquet file(s)
// Assumes the table schema is: name (STRING), age (INTEGER), city (STRING)
let batch = RecordBatch::try_new(
    Arc::new(write_context.logical_schema().as_ref().try_into_arrow()?),
    vec![
        Arc::new(StringArray::from(vec!["Dave", "Eve", "Frank"])),
        Arc::new(Int32Array::from(vec![4, 5, 6])),
        Arc::new(StringArray::from(vec!["Austin", "Boston", "Chicago"])),
    ],
)?;
let data = ArrowEngineData::new(batch);
let file_metadata = engine
    .write_parquet(&data, write_context.as_ref())
    .await?;

// 5. Register the files
txn.add_files(file_metadata);

// 6. Commit
match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => {
        println!("Committed version {}", committed.commit_version());
    }
    _ => eprintln!("commit did not succeed"),
}
# Ok(())
# }
```

## Creating a transaction

A transaction is created from a snapshot. The snapshot pins the table version you are
writing against:

```rust,ignore
let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("INSERT".to_string())
    .with_engine_info("my-app/1.0")
    .with_data_change(true);
```

The builder methods:

| Method | Purpose |
|--------|---------|
| `with_operation(String)` | Operation name stored in the commit log (e.g. `"INSERT"`, `"MERGE"`) |
| `with_engine_info(impl Into<String>)` | Identifies your application in the commit log |
| `with_data_change(bool)` | Whether this commit materially changes data (`true`) or just reorganizes it (`false`, e.g. OPTIMIZE) |

## The WriteContext

Before writing data, obtain a `WriteContext`. A `WriteContext` bundles everything
needed to correctly write Parquet files:

```rust,ignore
// For unpartitioned tables
let write_context = txn.unpartitioned_write_context()?;

// For partitioned tables, pass the partition values for this file
let write_context = txn.partitioned_write_context(partition_values)?;
```

For partitioned tables, see
[Writing to Partitioned Tables](./partitioned_writes.md).

`WriteContext` provides:

| Method | Returns | Purpose |
|--------|---------|---------|
| `table_root_dir()` | `&Url` | The table root URL |
| `write_dir()` | `Url` | The URL for writing files |
| `logical_schema()` | `&SchemaRef` | The full user-defined table schema |
| `physical_schema()` | `&SchemaRef` | The schema for the on-disk physical data |
| `logical_to_physical()` | `ExpressionRef` | Expression that transforms logical data to physical |
| `column_mapping_mode()` | `ColumnMappingMode` | The column mapping mode for this table |
| `stats_columns()` | `&[ColumnName]` | Columns that should have statistics collected |
| `physical_partition_values()` | `&HashMap<String, Option<String>>` | Serialized partition values keyed by physical column name |

## Writing Parquet files

The `DefaultEngine` provides an async helper for writing Parquet:

```rust,ignore
let file_metadata = engine
    .write_parquet(&data, write_context.as_ref())
    .await?;
```

- **`data`**: An `ArrowEngineData` wrapping a `RecordBatch` matching the logical schema
- **`write_context`**: From `unpartitioned_write_context()` or `partitioned_write_context()`

`DefaultEngine::write_parquet` handles the logical-to-physical transformation, generates a unique filename,
writes the file, collects statistics, and returns file metadata that you pass to
`txn.add_files()`.

You can call `write_parquet` and `add_files` multiple times to write multiple files in one
transaction.

> [!NOTE]
> Methods that produce or register data files (`unpartitioned_write_context`,
> `partitioned_write_context`, `add_files`, `stats_schema`) are gated by the
> `SupportsDataFiles` trait bound and are available on standard write transactions but not
> on metadata-only transaction states (such as a future `AlterTable`).

## Committing

`commit()` consumes the transaction and returns a `CommitResult`:

```rust,ignore
match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => {
        println!("Committed version {}", committed.commit_version());
    }
    _ => {
        eprintln!("commit did not succeed");
    }
}
```

> [!NOTE]
> `commit()` returns a `CommitResult` with three variants: `CommittedTransaction` on success,
> `ConflictedTransaction` if another writer committed first, and `RetryableTransaction` for
> transient IO errors. Automatic conflict resolution is not yet supported. A blind append to
> a table with no concurrent writers always succeeds.

## Blind appends

A **blind append** is a write that adds new files without reading or depending on existing
table state. To mark a transaction as a blind append, call `with_blind_append()` during
construction:

```rust,ignore
let txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("INSERT".to_string())
    .with_blind_append();
```

Kernel records `isBlindAppend: true` in the commit's `commitInfo` action. This flag
enables conflict resolution optimizations: two blind appends to the same table can never
conflict with each other, because neither depends on the other's output.

Kernel validates the following rules at commit time. If any rule is violated, `commit()`
returns an error:

| Rule | Rationale |
|------|-----------|
| The transaction must add at least one file | A blind append with no data is meaningless |
| `data_change` must be `true` | Blind appends are logical data additions, not reorganizations |
| The transaction must not remove any files | Removing files means the write depends on existing state |
| The transaction must not update deletion vectors | Deletion vector updates depend on existing state |
| The transaction must not be a create-table transaction | Table creation is not an append |

> [!TIP]
> Mark your transaction as a blind append whenever you are inserting new data without reading
> the table first. This gives the committer the information it needs to resolve conflicts
> safely in multi-writer scenarios.

## Custom commit info

Kernel always writes a `commitInfo` action for every commit. To include your own fields in
that action, call `with_commit_info()` with your custom data and its schema:

```rust,ignore
let txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("INSERT".to_string())
    .with_commit_info(engine_commit_info, commit_info_schema);
```

The `engine_commit_info` argument is a `Box<dyn EngineData>` containing the fields you want
to add, and `commit_info_schema` is the corresponding `SchemaRef`. Kernel merges your fields
into the final `commitInfo` action.

Kernel reserves certain fields and overrides them regardless of what you provide. Do not set
these fields in your custom commit info:

| Field | Set by Kernel to |
|-------|------------------|
| `timestamp` | The transaction's commit timestamp |
| `inCommitTimestamp` | The in-commit timestamp (if ICT is enabled on the table) |
| `operation` | The value from `with_operation()` |
| `operationParameters` | Operation parameters (if any) |
| `kernelVersion` | The Kernel library version |
| `isBlindAppend` | `true` if `with_blind_append()` was called, omitted otherwise |
| `engineInfo` | The value from `with_engine_info()` |
| `txnId` | A unique transaction identifier |

Any field in your custom data that shares a name with a Kernel-reserved field is replaced
with Kernel's value. Fields with names that do not collide are preserved as-is in the final
`commitInfo`.

## After committing

A successful commit returns a `CommittedTransaction` with access to a post-commit
snapshot and post-commit statistics:

```rust,ignore
let committed = match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(c) => c,
    _ => panic!("unexpected result"),
};

// The version that was committed
let version = committed.commit_version();

// Post-commit statistics help you decide when to run maintenance
let stats = committed.post_commit_stats();
println!("Commits since last checkpoint: {}", stats.commits_since_checkpoint);
println!("Commits since last log compaction: {}", stats.commits_since_log_compaction);

// The post-commit snapshot reflects the table state after this commit.
// Use it for maintenance operations like publishing, checkpointing, and checksums.
if let Some(snapshot) = committed.post_commit_snapshot() {
    // Write a checksum file for this version
    let (result, snapshot) = snapshot.write_checksum(&engine)?;

    // For catalog-managed tables: snapshot.publish(engine, committer)?
    // For checkpointing: snapshot.checkpoint(&engine)?
}
```

### Post-commit statistics

`CommittedTransaction::post_commit_stats()` returns a `PostCommitStats` struct with two
fields:

| Field | Meaning |
|-------|---------|
| `commits_since_checkpoint` | Number of commits since the last checkpoint. Commit 0 counts as a checkpoint. |
| `commits_since_log_compaction` | Number of commits since the last log compaction or checkpoint. A checkpoint resets this counter too. |

Use these values to decide when to trigger maintenance. For example, you might checkpoint
every 10 commits or compact the log every 50 commits.

### Writing a checksum

You can call `write_checksum()` on the post-commit snapshot to write a checksum file (CRC
file), which enables faster snapshot loading and table state validation:

```rust,ignore
if let Some(snapshot) = committed.post_commit_snapshot() {
    let (checksum_result, updated_snapshot) = snapshot.write_checksum(&engine)?;
    // Use updated_snapshot for subsequent operations (it includes the new CRC file)
}
```

`write_checksum()` returns a `ChecksumWriteResult` and an updated `SnapshotRef`.
`ChecksumWriteResult::Written` means the CRC file was created successfully.
`ChecksumWriteResult::AlreadyExists` means a CRC file already exists at this version, and
the original snapshot is returned unchanged. Per the Delta protocol, writers must not
overwrite existing checksum files.

> [!NOTE]
> `write_checksum()` requires in-memory CRC information, which is only available on
> post-commit snapshots. Calling it on a snapshot loaded from disk (without a pre-computed
> CRC) returns a `ChecksumWriteUnsupported` error.

The post-commit snapshot is the entry point for maintenance operations that should happen
after a successful write. See [Checkpointing](../maintenance/checkpointing.md) and
[Catalog-Managed Tables](../catalog_managed/overview.md) for details.

## What's next

- [Writing to Partitioned Tables](./partitioned_writes.md) covers writing data with partition values.
- [Creating a Table](./create_table.md) covers creating a new table from scratch.
- [Checkpointing](../maintenance/checkpointing.md) explains when and how to write checkpoints.
