# Building a scan

To read data from a Delta table, you build a `Scan` from a `Snapshot`, optionally
configure column selection and filter predicates, and then execute it.

## The basic pattern

Every scan follows the same pattern:

1. Get a `Snapshot` of the table
2. Call `snapshot.scan_builder()` to get a `ScanBuilder`
3. Configure the builder (optional)
4. Call `.build()` to create the `Scan`
5. Execute the scan

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;

let scan = snapshot
    .scan_builder()
    .build()?;
# Ok(())
# }
```

Without any configuration, this scans all columns with no filter. It's equivalent to
`SELECT * FROM table`.

## Configuring a scan

`ScanBuilder` supports two main configuration options:

### Column selection with `with_schema`

Pass a schema containing only the columns you want to read:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField, StructType};
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let read_schema = Arc::new(StructType::try_new([
    StructField::nullable("name", DataType::STRING),
    StructField::nullable("age", DataType::INTEGER),
])?);

let scan = snapshot
    .scan_builder()
    .with_schema(read_schema)
    .build()?;
# Ok(())
# }
```

The schema you provide must be a subset of the table's schema. Kernel only reads the
columns you specify from each Parquet file.

For more details, see [Column Selection](./column_selection.md).

### Filter pushdown with `with_predicate`

Pass a predicate expression to skip files that cannot contain matching rows:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::expressions::{column_expr, Predicate, Scalar};
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let predicate = Arc::new(
    Predicate::lt(column_expr!("age"), Scalar::from(30))
);

let scan = snapshot
    .scan_builder()
    .with_predicate(predicate)
    .build()?;
# Ok(())
# }
```

Kernel uses the predicate to evaluate file-level statistics (min/max values) and skip
entire files that cannot match. This is called **data skipping** and can significantly
reduce the amount of data read.

> [!NOTE]
> Filtering is **best-effort**. The scan may still include rows that don't match the
> predicate. Your connector should apply the filter to the returned data for exact
> results.

For more details, see [Filter Pushdown and File Skipping](./filter_pushdown.md).

## Executing a scan

There are two ways to execute a scan: the **simple path** for single-process use, and the
**advanced path** for when you need control over parallelism.

### Simple path: `execute()`

`execute()` handles log replay, data skipping, file reading, and physical-to-logical
transformations. It returns an iterator of `EngineData` results.

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot.scan_builder().build()?;

let mut batches = vec![];
for data in scan.execute(Arc::new(engine))? {
    let record_batch: delta_kernel::arrow::record_batch::RecordBatch =
        data?.try_into_record_batch()?;
    batches.push(record_batch);
}
# Ok(())
# }
```

`execute()` takes an `Arc<dyn Engine>` (not a reference) because it needs to keep the
engine alive for the lifetime of the returned iterator.

If you're using the default engine, each `EngineData` is backed by an Arrow `RecordBatch`.
The `try_into_record_batch()` method (from the `EngineDataArrowExt` trait) unwraps it.

This is the right choice when you're running in a single process and don't need to control
how files are distributed across threads.

## What's next

- [Column Selection](./column_selection.md): projecting specific columns
- [Advanced Reads with scan_metadata()](./scan_metadata.md): distributed and multi-threaded reads
- [Filter Pushdown and File Skipping](./filter_pushdown.md): predicate-based optimization
