# Quick Start: Reading a table

In this tutorial, you will read a Delta table and get the data as
[Arrow](https://arrow.apache.org) `RecordBatch`es using the default engine.

## Create a new project

```sh
cargo new delta_read_example
cd delta_read_example
```

Add the kernel dependency (see [Installation](./installation.md) for details):

```sh
cargo add delta_kernel -F default-engine-rustls -F arrow -F internal-api
```

Your `Cargo.toml` should look like:

```toml
[dependencies]
delta_kernel = { version = "0.21", features = ["default-engine-rustls", "arrow", "internal-api"] }
```

> [!NOTE]
> The `internal-api` feature exposes `try_parse_uri`, a convenience function used in this tutorial
> and throughout the guide. This feature flag may be removed in a future release once the API
> stabilizes.

## Write the code

Replace `src/main.rs` with the following. We'll walk through each piece below.

Filename: src/main.rs

```rust,no_run
# extern crate delta_kernel;
use std::sync::Arc;

use delta_kernel::arrow::util::pretty::print_batches;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Snapshot};

fn main() -> DeltaResult<()> {
    // 1. Parse the table location
    let table_path = std::env::args().nth(1).expect("usage: delta_read_example <TABLE_PATH>");
    let url = delta_kernel::try_parse_uri(&table_path)?;

    // 2. Build an object store and engine
    let store = store_from_url(&url)?;
    let engine = DefaultEngine::builder(store).build();

    // 3. Get a snapshot of the table at the latest version
    let snapshot = Snapshot::builder_for(url).build(&engine)?;
    println!("Table version: {}", snapshot.version());
    println!("Schema:\n{}", snapshot.schema());

    // 4. Build and execute a scan
    let scan = snapshot.scan_builder().build()?;
    let mut batches = vec![];
    for data in scan.execute(Arc::new(engine))? {
        let record_batch: delta_kernel::arrow::record_batch::RecordBatch =
            data?.try_into_record_batch()?;
        batches.push(record_batch);
    }

    // 5. Print the results
    print_batches(&batches)?;
    Ok(())
}
```

## Step by step

### 1. Parse the table location

```rust,ignore
let url = delta_kernel::try_parse_uri(&table_path)?;
```

`try_parse_uri` converts a path string (local path or URI like `s3://bucket/path`) into a `Url`.

### 2. Build an object store and engine

```rust,ignore
let store = store_from_url(&url)?;
let engine = DefaultEngine::builder(store).build();
```

`store_from_url` creates an object store from the URL. For cloud storage with custom credentials,
use `store_from_url_opts` instead. See [Configuring Storage](../storage/configuring_storage.md)
for S3, Azure, and GCS options.

`DefaultEngine::builder(store).build()` constructs the default engine, which handles all I/O
(Parquet, JSON, file listing) and expression evaluation using Arrow.

### 3. Get a snapshot

```rust,ignore
let snapshot = Snapshot::builder_for(url).build(&engine)?;
```

A `Snapshot` is an immutable view of the table at a specific version. Without calling
`.at_version(v)` on the builder, this gives you the latest version.

The snapshot gives you access to the table's schema and properties, and serves as the entry point
for scanning and writing.

### 4. Build and execute a scan

```rust,ignore
let scan = snapshot.scan_builder().build()?;
for data in scan.execute(Arc::new(engine))? {
    let record_batch = data?.try_into_record_batch()?;
    batches.push(record_batch);
}
```

`scan_builder()` returns a `ScanBuilder` which you can configure with column selection
(`.with_schema()`) or filter predicates (`.with_predicate()`). Here we use the defaults: all
columns, no filter.

`execute()` returns an iterator of `EngineData` results. Since we're using the default engine,
each item is backed by an Arrow `RecordBatch`. The `try_into_record_batch()` method (from the
`EngineDataArrowExt` extension trait) unwraps it.

## Run it

If you have the delta-kernel-rs repo checked out locally, you can test with one of its test tables:

```sh
cargo run -- /path/to/delta-kernel-rs/kernel/tests/data/basic_partitioned/
```

Expected output:

```text
Table version: 1
Schema:
struct:
├─letter: string (is nullable: true, metadata: {})
├─number: long (is nullable: true, metadata: {})
└─a_float: double (is nullable: true, metadata: {})

+--------+--------+---------+
| letter | number | a_float |
+--------+--------+---------+
|        | 6      | 6.6     |
| a      | 4      | 4.4     |
| e      | 5      | 5.5     |
| a      | 1      | 1.1     |
| b      | 2      | 2.2     |
| c      | 3      | 3.3     |
+--------+--------+---------+
```

## What's next

- [Quick Start: Writing a Table](./quick_start_write.md) covers creating a table and writing data.
- [Building a Scan](../reading/building_a_scan.md) explores column selection, filter pushdown, and more.
