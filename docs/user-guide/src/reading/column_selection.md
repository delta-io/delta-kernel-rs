# Column Selection

By default a scan reads all columns from a table. You can select a subset of columns
(projection pushdown) so the engine only reads the data you need.

## Projecting columns

Use `Schema::project()` to create a schema containing only the columns you want, then
pass it to `ScanBuilder::with_schema()`:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
// Table has columns [id, name, email, created_at]
// Select only id and name
let projected_schema = snapshot.schema().project(&["id", "name"])?;

let scan = snapshot
    .scan_builder()
    .with_schema(projected_schema)
    .build()?;
# Ok(())
# }
```

The returned data will contain only the projected columns, in the order you specified.
Requesting a column that does not exist in the table schema returns an error.

## Reordering columns

`project()` returns columns in the order you provide, which can differ from the table
schema order:

```rust,ignore
// Table schema is [id, name, email]
// Return [email, id]
let reordered = snapshot.schema().project(&["email", "id"])?;
```

## Metadata columns

You can request metadata columns that are not part of the table data but provide
information about each row's origin. Add them to your scan schema with
`Schema::add_metadata_column()`:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::MetadataColumnSpec;
# use delta_kernel::{DeltaResult, Snapshot};
# fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
// Start with a projection
let schema = snapshot.schema().project_as_struct(&["id", "name"])?;

// Add a row index metadata column
let schema = schema.add_metadata_column("row_idx", MetadataColumnSpec::RowIndex)?;

let scan = snapshot
    .scan_builder()
    .with_schema(Arc::new(schema))
    .build()?;
# Ok(())
# }
```

The available metadata columns:

| Spec | Data type | Description |
|------|-----------|-------------|
| `MetadataColumnSpec::FilePath` | `STRING` | Path of the Parquet file containing the row |
| `MetadataColumnSpec::RowIndex` | `LONG` | Zero-based row position within the Parquet file |
| `MetadataColumnSpec::RowId` | `LONG` | Stable row identifier (requires row tracking on the table) |
| `MetadataColumnSpec::RowCommitVersion` | `LONG` | Commit version that last wrote or updated the row (requires row tracking on the table) |

You choose the column name when calling `add_metadata_column()`. Only one metadata
column of each type is allowed per scan.

## What's next

- [Building a Scan](./building_a_scan.md) covers the full scan API.
- [Filter Pushdown and File Skipping](./filter_pushdown.md) covers predicate-based
  optimization.
