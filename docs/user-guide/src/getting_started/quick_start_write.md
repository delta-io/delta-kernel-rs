# Quick Start: Writing a Table

In this tutorial, you will create a new Delta table, write data to it, and read it back.
It builds on the concepts from [Quick Start: Reading a Table](./quick_start_read.md).

## Setup

Create a new project and add dependencies:

```sh
cargo new delta_write_example
cd delta_write_example
```

Writing data requires `tokio` because the default engine's Parquet writer is async:

```sh
cargo add delta_kernel -F default-engine-rustls -F arrow -F internal-api
cargo add tokio -F rt-multi-thread -F macros
```

## Write the code

Replace `src/main.rs` with the following:

Filename: src/main.rs

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch, StringArray};
use delta_kernel::arrow::util::pretty::print_batches;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};

#[tokio::main]
async fn main() -> DeltaResult<()> {
    let table_path = std::env::args()
        .nth(1)
        .expect("usage: delta_write_example <TABLE_DIR>");
    let url = delta_kernel::try_parse_uri(&table_path)?;

    // Build the engine
    let engine = DefaultEngine::builder(store_from_url(&url)?).build();

    // 1. Create the table
    let schema = Arc::new(StructType::try_new(vec![
        StructField::not_null("id", DataType::INTEGER),
        StructField::nullable("name", DataType::STRING),
    ])?);

    create_table(url.as_str(), schema.clone(), "quick-start/1.0")
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?;
    println!("Created table at {url}");

    // 2. Write data
    let snapshot = Snapshot::builder_for(url.clone()).build(&engine)?;

    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_operation("INSERT".to_string())
        .with_engine_info("quick-start/1.0")
        .with_data_change(true);

    // Build an Arrow RecordBatch
    let arrow_schema: delta_kernel::arrow::datatypes::Schema =
        schema.as_ref().try_into_arrow()?;
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // Write Parquet and add file metadata to the transaction
    let write_context = Arc::new(txn.unpartitioned_write_context()?);
    let data = ArrowEngineData::new(batch);
    let file_metadata = engine
        .write_parquet(&data, write_context.as_ref())
        .await?;
    txn.add_files(file_metadata);

    // Commit
    match txn.commit(&engine)? {
        CommitResult::CommittedTransaction(committed) => {
            println!("Committed version {}", committed.commit_version());
        }
        CommitResult::ConflictedTransaction(_) => {
            panic!("unexpected conflict on a brand new table");
        }
        CommitResult::RetryableTransaction(retry) => {
            panic!("commit failed with retryable error: {}", retry.error);
        }
    }

    // 3. Read it back
    let snapshot = Snapshot::builder_for(url).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    let batches: Vec<RecordBatch> = scan
        .execute(Arc::new(engine))?
        .map(|data| -> DeltaResult<RecordBatch> {
            Ok(data?.try_into_record_batch()?)
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    print_batches(&batches)?;

    Ok(())
}
```

## Step by step

### 1. Create the table

```rust,ignore
let schema = Arc::new(StructType::try_new(vec![
    StructField::not_null("id", DataType::INTEGER),
    StructField::nullable("name", DataType::STRING),
])?);

create_table(url.as_str(), schema.clone(), "quick-start/1.0")
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
```

`create_table` returns a builder. You provide:
- The table path
- A schema (using kernel's `StructType`)
- An engine info string (identifies your application)

`.build()` takes the engine and a `Committer`. For local filesystem tables, use
`FileSystemCommitter`. For catalog-managed tables, you provide your own committer.
[Catalog-Managed Tables](../catalog_managed/overview.md) covers that topic.

`.commit()` writes version 0 of the table (the initial Protocol and Metadata actions).

### 2. Write data

The write flow has four parts:

**Start a transaction:**
```rust,ignore
let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("INSERT".to_string())
    .with_data_change(true);
```

**Build your data as an Arrow RecordBatch and wrap it:**
```rust,ignore
let arrow_schema: delta_kernel::arrow::datatypes::Schema =
    schema.as_ref().try_into_arrow()?;
let batch = RecordBatch::try_new(
    Arc::new(arrow_schema),
    vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
    ],
)?;
let data = ArrowEngineData::new(batch);
```

**Write the Parquet file and collect file metadata:**
```rust,ignore
let write_context = Arc::new(txn.unpartitioned_write_context()?);
let file_metadata = engine
    .write_parquet(&data, write_context.as_ref())
    .await?;
txn.add_files(file_metadata);
```

`unpartitioned_write_context()` creates a `WriteContext` with the target directory, schema, and stats configuration.
`write_parquet` writes a Parquet file and returns metadata (path, size, stats) that the
transaction needs. `add_files` registers that metadata with the transaction.

**Commit:**
```rust,ignore
match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => { /* success */ }
    CommitResult::ConflictedTransaction(_) => { /* another writer won */ }
    CommitResult::RetryableTransaction(retry) => { /* transient error, retry */ }
}
```

`commit()` returns a `CommitResult` with three variants. For blind appends to a table with no
concurrent writers, you'll always get `CommittedTransaction`.

## Run it

```sh
mkdir -p /tmp/my_delta_table
cargo run -- /tmp/my_delta_table
```

Expected output:

```text
Created table at file:///tmp/my_delta_table
Committed version 1
+----+---------+
| id | name    |
+----+---------+
| 1  | Alice   |
| 2  | Bob     |
| 3  | Charlie |
+----+---------+
```

## What's next

- [Creating a Table](../writing/create_table.md) covers table properties, partition columns, and more.
- [Appending Data](../writing/append.md) covers writing to existing tables and retry logic.
