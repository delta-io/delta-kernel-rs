# Writing to partitioned tables

To write data to a partitioned table, you create a `WriteContext` for each distinct
set of partition values, write Parquet files through the engine for each one, and
commit. Kernel validates partition values, serializes them per the Delta protocol,
and constructs the correct directory paths.

Before reading this page, make sure you understand
[Appending Data](./append.md) and
[Creating a Table](./create_table.md#partitioned-tables).

## How partitioned writes differ

For unpartitioned tables, you create one `WriteContext` and write all data through it.
For partitioned tables, you create one `WriteContext` per distinct partition value
combination. Partition values are baked into the `WriteContext` at creation time, not
passed at write time.

```text
Unpartitioned:  1 WriteContext  -->  write all data
Partitioned:    1 WriteContext per distinct partition  -->  write that partition's data
```

## The write flow

The pattern for partitioned writes is: **group your data by partition values, create a
`WriteContext` per group, and write each group**.

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::collections::HashMap;
# use delta_kernel::arrow::array::RecordBatch;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::expressions::Scalar;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/partitioned_table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;
let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("INSERT".to_string())
    .with_data_change(true);

// Suppose you have data grouped by partition values already.
// For each partition, create a WriteContext and write.
let partitions: Vec<(HashMap<String, Scalar>, RecordBatch)> = todo!("group your data");

for (partition_values, batch) in partitions {
    // 1. Create a WriteContext for this partition
    let wc = txn.partitioned_write_context(partition_values)?;

    // 2. Write the data (physical schema excludes partition columns)
    let data = ArrowEngineData::new(batch);
    let file_metadata = engine.write_parquet(&data, &wc).await?;

    // 3. Register the written file
    txn.add_files(file_metadata);
}

// Commit all partitions in a single transaction
txn.commit(&engine)?;
# Ok(())
# }
```

Each `partitioned_write_context` call takes a `HashMap<String, Scalar>` mapping logical
partition column names to typed values:

```rust,ignore
let partition_values = HashMap::from([
    ("year".to_string(), Scalar::Integer(2024)),
    ("month".to_string(), Scalar::Integer(3)),
]);
let wc = txn.partitioned_write_context(partition_values)?;
```

Key points:

- **Typed values**: partition values are `Scalar` values, not strings. Pass
  `Scalar::Integer(2024)` for an integer column, not `Scalar::String("2024".into())`.
  Kernel rejects type mismatches.
- **Case-insensitive keys**: `"YEAR"` matches schema column `"year"`. Kernel normalizes
  to the schema case.
- **Physical schema**: `wc.physical_schema()` excludes partition columns. Data files
  contain only the non-partition columns.

> [!TIP]
> To get the partition column names at runtime, call `txn.logical_partition_columns()`.
> This is useful when your connector handles arbitrary tables and does not know the
> partition columns in advance.

## Grouping data by partition values

How you group data by partition values is up to your connector. Kernel's contract is
that each `partitioned_write_context` call receives a `HashMap<String, Scalar>` for one
distinct partition, and the corresponding data files contain only that partition's rows.

Kernel provides `serialize_partition_value` as a public utility for building hashable
group keys from `Scalar` values. It returns a `DeltaResult<Option<String>>` per value,
which you can collect into a `Vec<Option<String>>` group key for use in a `HashMap`.

## Partition value validation

`partitioned_write_context` validates the provided values before creating the
`WriteContext`:

| Check | Example |
|-------|---------|
| Missing partition column | Table has `["year", "month"]` but only `"year"` provided |
| Extra key | `"region"` provided but is not a partition column |
| Type mismatch | `Scalar::String("2024")` for an `INTEGER` column |
| Duplicate after case normalization | Both `"YEAR"` and `"year"` provided |

If validation fails, `partitioned_write_context` returns an error before any data reaches
disk.

## What Kernel handles

When you call `partitioned_write_context`, Kernel performs the following steps internally.
Your connector does not need to implement any of this:

1. **Key validation**: all partition columns present, no extra keys
2. **Case normalization**: keys matched case-insensitively against the schema
3. **Type checking**: each `Scalar`'s type must match the partition column's schema type
4. **Value serialization**: `Scalar` values converted to protocol-compliant strings
   (for example, `Scalar::Date(19723)` becomes `"2024-01-01"`)
5. **Key translation**: logical column names translated to physical names when
   column mapping is enabled

`write_dir()` on the resulting `WriteContext` returns the directory where data files
should be written. Without column mapping, this is a Hive-style path like
`<table_root>/year=2024/month=3/`. With column mapping enabled, this is a
random two-character prefix directory like `<table_root>/aB/` that avoids
exposing physical column names and distributes files across object store
prefixes to prevent S3 hotspots.

> [!NOTE]
> Hive partition path segments are URI-encoded on top of Hive escaping, matching the
> on-disk layout produced by Delta-Spark and Delta-Kernel-Java. For example, a partition
> value `2025-03-31T15:30:00Z` produces the path segment
> `p=2025-03-31T15%253A30%253A00Z/` (the `:` is Hive-escaped to `%3A`, and the `%` is then
> URI-encoded to `%25`). The same encoded path is recorded in the `add.path` field of the
> commit. Callers writing through the filesystem must URI-decode `write_dir()` once before
> using it as an OS path.

## What's next

- [Appending Data](./append.md) covers the general write flow, commit handling, and
  blind appends.
- [Creating a Table](./create_table.md#partitioned-tables) covers creating partitioned
  tables.
- [Idempotent Writes](./idempotent_writes.md) for at-most-once write guarantees.
