# Creating a Table

To create a new Delta table, you configure a `CreateTableTransactionBuilder` with your
schema and table options, then commit it. For a quick end-to-end example that creates a
table, writes data, and reads it back, see
[Quick Start: Writing a Table](../getting_started/quick_start_write.md).

## Basic table creation

The `create_table` function returns a builder that you configure and then commit:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField, StructType};
# use delta_kernel::transaction::create_table::create_table;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let schema = Arc::new(StructType::try_new([
    StructField::not_null("id", DataType::INTEGER),
    StructField::nullable("name", DataType::STRING),
    StructField::nullable("age", DataType::INTEGER),
])?);

create_table(url.as_str(), schema, "my-app/1.0")
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
# Ok(())
# }
```

The three required arguments are:
- **`path`**: Where to create the table (local path or URI like `s3://bucket/path`)
- **`schema`**: The table's column definitions as a `StructType`
- **`engine_info`**: A string identifying your application (stored in the commit log)

`.build()` validates the inputs and creates a `CreateTableTransaction`. `.commit()` writes
version 0 of the table, producing the initial Protocol and Metadata actions.

## Defining a schema

Schemas are built from `StructField`s, each with a name, data type, and nullability:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
use std::sync::Arc;
use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

let schema = Arc::new(StructType::try_new([
    // Non-nullable integer
    StructField::not_null("id", DataType::LONG),

    // Nullable string
    StructField::nullable("name", DataType::STRING),

    // Nested struct
    StructField::nullable(
        "address",
        StructType::try_new([
            StructField::nullable("street", DataType::STRING),
            StructField::nullable("city", DataType::STRING),
        ])?,
    ),

    // Array of integers
    StructField::nullable(
        "scores",
        ArrayType::new(DataType::INTEGER, true),  // true = elements are nullable
    ),

    // Map from string to double
    StructField::nullable(
        "metrics",
        MapType::new(DataType::STRING, DataType::DOUBLE, true),  // true = values are nullable
    ),
])?);
# Ok(())
# }
```

For the full list of supported data types, see [Schemas and Data Types](../concepts/schema_and_types.md).

## Table properties

You can set custom application properties on the table:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField, StructType};
# use delta_kernel::transaction::create_table::create_table;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
# let schema = Arc::new(StructType::try_new([
#     StructField::not_null("id", DataType::INTEGER),
# ])?);
create_table(url.as_str(), schema, "my-app/1.0")
    .with_table_properties([
        ("myapp.version", "2.0"),
        ("myapp.owner", "data-team"),
    ])
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
# Ok(())
# }
```

Custom properties (those not starting with `delta.`) are always allowed. Delta properties
(`delta.*`) are validated against an allow list. Kernel only permits properties for
features it supports.

## Clustered tables

You can create a clustered table using `with_data_layout`. Clustering optimizes data file
layout for queries that filter on the clustering columns:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField, StructType};
# use delta_kernel::transaction::create_table::create_table;
# use delta_kernel::transaction::data_layout::DataLayout;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let schema = Arc::new(StructType::try_new([
    StructField::not_null("id", DataType::INTEGER),
    StructField::nullable("region", DataType::STRING),
    StructField::nullable("timestamp", DataType::TIMESTAMP),
])?);

create_table(url.as_str(), schema, "my-app/1.0")
    .with_data_layout(DataLayout::clustered(["region", "timestamp"]))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
# Ok(())
# }
```

Clustering columns can be top-level or nested. The `DataLayout::clustered([...])` helper
treats each string as a single top-level column name. To cluster on a nested column,
construct the `DataLayout::Clustered` variant directly with a multi-segment `ColumnName`:

```rust,ignore
use delta_kernel::expressions::ColumnName;
use delta_kernel::transaction::data_layout::DataLayout;

let layout = DataLayout::Clustered {
    columns: vec![
        ColumnName::new(["region"]),
        ColumnName::new(["address", "city"]),
    ],
};
```

For nested clustering columns, every intermediate path component must be a struct field,
and the leaf column must have a stats-eligible primitive type.

The kernel automatically enables the required table features (`DomainMetadata` and
`ClusteredTable`) when clustering is specified.

## Partitioned tables

You can create a partitioned table using `DataLayout::partitioned()`. Partitioning organizes
data files into directories based on partition column values, which allows readers to skip
entire directories when filtering on those columns.

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField, StructType};
# use delta_kernel::transaction::create_table::create_table;
# use delta_kernel::transaction::data_layout::DataLayout;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let schema = Arc::new(StructType::try_new([
    StructField::not_null("id", DataType::INTEGER),
    StructField::nullable("name", DataType::STRING),
    StructField::not_null("year", DataType::INTEGER),
    StructField::not_null("month", DataType::INTEGER),
])?);

create_table(url.as_str(), schema, "my-app/1.0")
    .with_data_layout(DataLayout::partitioned(["year", "month"]))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
# Ok(())
# }
```

### Validation rules

`build()` validates partition columns against these rules:

- **Top-level only**: partition columns must be top-level fields in the schema. Nested paths
  like `address.city` are not supported.
- **Primitive types only**: each partition column must have a primitive type (`STRING`,
  `INTEGER`, `LONG`, `DATE`, `TIMESTAMP`, etc.). Struct, array, and map types are rejected.
- **At least one non-partition column**: the schema must contain at least one column that is
  not a partition column. A table with every column marked as a partition column is invalid.
- **No duplicates**: the same column cannot appear more than once in the partition column list.
- **Schema presence**: every partition column must exist in the schema.
- **At least one partition column**: if `DataLayout::partitioned(...)` is used, the column
  list cannot be empty.

### Physical vs. logical schema

Partition column values are stored in the directory path rather than inside the data files
themselves. When you call `WriteContext::physical_schema()`, the returned schema excludes
partition columns. Your connector writes data files using the physical schema, and Kernel
reconstructs the full logical schema (including partition values from the path) at read time.

> [!NOTE]
> Partitioning and clustering are mutually exclusive. You can call `with_data_layout()` with
> either `DataLayout::partitioned()` or `DataLayout::clustered()`, but not both. Only the
> last `with_data_layout()` call takes effect.

## The Committer

The `build()` method takes a `Box<dyn Committer>` that controls how the commit is
persisted:

- **`FileSystemCommitter`**: For standalone filesystem-based tables. Writes commit files
  directly to `_delta_log/` using atomic put-if-absent. This is the default for most use
  cases.

- **Custom `Committer`**: For catalog-managed tables (e.g. Unity Catalog), you implement
  the `Committer` trait to route commits through the catalog. See
  [Catalog-Managed Tables](../catalog_managed/overview.md).

## Handling the result

`commit()` returns a `CommitResult`:

```rust,ignore
match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => {
        println!("Created table at version {}", committed.commit_version());
    }
    CommitResult::ConflictedTransaction(_) => {
        // Another writer created the table concurrently
    }
    CommitResult::RetryableTransaction(retry) => {
        // Transient I/O error. Safe to retry.
        println!("Retryable error: {}", retry.error);
    }
}
```

For table creation, `CommittedTransaction` is the expected result (version 0).
`ConflictedTransaction` means another process created the table between your existence
check and commit. `RetryableTransaction` indicates a transient error.

## Validations

`build()` performs these checks before creating the transaction:

**Path and existence:**
- The path is a valid URI
- No Delta table already exists at that path

**Schema:**
- The schema has at least one field
- Column names are non-empty
- When column mapping is disabled, column names cannot contain Parquet special characters
  (space, comma, semicolon, braces, parentheses, tab, newline, or `=`). Enable column
  mapping to use these characters.
- When column mapping is enabled, column names cannot contain newlines
- No duplicate column names, case-insensitive, across all nested paths
- Non-null columns (fields with `nullable: false`) are accepted. When the schema contains
  any non-null field anywhere in the tree, Kernel auto-enables the `invariants` writer
  feature on the new table. This matches Delta-Spark, which treats `nullable: false` as an
  implicit column invariant.

**Table properties:**
- Custom properties (not starting with `delta.`) are always allowed
- `delta.*` properties are checked against an allow list
- Feature signals (`delta.feature.*`) are checked against an allow list
- Catalog-managed tables cannot set `delta.enableInCommitTimestamps=false`. Kernel
  auto-enables ICT for catalog-managed tables.

**Data layout:**
- Clustering columns (if specified) exist in the schema. Both top-level and nested paths
  are supported. Each leaf column must have a stats-eligible primitive type.
- Partition columns (if specified) exist in the schema, are top-level, have primitive
  types, and leave at least one non-partition column. The partition column list cannot
  be empty.

## Auto-enabled features

`build()` auto-enables certain table features based on the schema, properties, and data
layout, so you do not need to set them manually. The triggers fall into four groups.
Each enabled feature is either a reader/writer feature (bumps both the reader and writer
protocol) or a writer-only feature (bumps the writer protocol only).

### Table-property-derived

Features enabled from the values of `delta.*` properties you set with
`with_table_properties()` or from the feature signals that appear in that property bag.

| Trigger | Reader/Writer feature | Writer feature | Properties set |
|---------|-----------------------|----------------|----------------|
| `delta.columnMapping.mode=name` or `id` | `columnMapping` (assigns physical names and IDs) | — | `delta.columnMapping.maxColumnId` |
| `delta.enableDeletionVectors=true` | `deletionVectors` | — | — |
| `delta.enableTypeWidening=true` | `typeWidening` | — | — |
| `delta.feature.catalogManaged=supported` | `catalogManaged` | `inCommitTimestamp` | `delta.enableInCommitTimestamps=true` |
| `delta.enableInCommitTimestamps=true` | — | `inCommitTimestamp` | — |
| `delta.enableRowTracking=true` | — | `rowTracking`, `domainMetadata` | `delta.rowTracking.materializedRowIdColumnName`, `delta.rowTracking.materializedRowCommitVersionColumnName` |
| `delta.enableChangeDataFeed=true` | — | `changeDataFeed` | — |
| `delta.appendOnly=true` | — | `appendOnly` | — |

### Data-layout-derived

Features enabled by the data layout you pass to `with_data_layout()`.

| Trigger | Reader/Writer feature | Writer feature | Properties set |
|---------|-----------------------|----------------|----------------|
| `DataLayout::clustered(...)` (or the `DataLayout::Clustered` variant) | — | `domainMetadata`, `clusteredTable` | — |

### Schema-derived

Features enabled when specific column types or annotations appear in the schema.

| Trigger | Reader/Writer feature | Writer feature | Properties set |
|---------|-----------------------|----------------|----------------|
| `VARIANT` type in schema | `variantType` | — | — |
| `TIMESTAMP_NTZ` type in schema | `timestampNtz` | — | — |
| Any field with `nullable: false` in schema | — | `invariants` | — |

### Feature-signal only

Features enabled only when you explicitly include the feature signal in
`with_table_properties()`. Kernel does not derive these from the schema or from other
properties.

| Trigger | Reader/Writer feature | Writer feature | Properties set |
|---------|-----------------------|----------------|----------------|
| `delta.feature.v2Checkpoint=supported` | `v2Checkpoint` | — | — |
| `delta.feature.vacuumProtocolCheck=supported` | `vacuumProtocolCheck` | — | — |

> [!NOTE]
> Auto-enabled features cannot always be set explicitly via `delta.feature.*=supported`.
> Kernel enforces that the protocol and metadata stay consistent with the schema and data
> layout, so some features are controllable only through their driving condition:
>
> - `variantType` and `timestampNtz` are enabled only when the schema uses those types.
>   Explicit feature signals for them are rejected.
> - `clusteredTable` is enabled only by passing clustering columns to `with_data_layout()`.
>   `delta.feature.clustering=supported` is rejected.
> - For `catalogManaged` tables, `delta.enableInCommitTimestamps=false` is rejected because
>   ICT is required.
>
> Other auto-enabled features (such as `columnMapping`, `inCommitTimestamp`) can still
> be enabled explicitly via `delta.feature.X=supported` if you want to pre-enable them
> before the schema or property trigger applies.

## What's next

- [Appending Data](./append.md) to write rows to an existing table
- [Writing to Partitioned Tables](./partitioned_writes.md) to write data with partition values
- [Quick Start: Writing a Table](../getting_started/quick_start_write.md) for a complete end-to-end example
