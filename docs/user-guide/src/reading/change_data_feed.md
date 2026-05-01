# Reading change data feed

To read a row-level changelog of what changed between two versions of a Delta table, you
use `TableChanges` and `TableChangesScan`. This gives you every insert, update, and delete
that occurred in the specified version range, with metadata columns that identify the type
of change and the commit it came from.

Before reading this page, make sure you understand
[Building a Scan](./building_a_scan.md).

## Prerequisites

**Change Data Feed** (CDF) is a Delta feature that records row-level changes (inserts,
updates, deletes) as part of each commit. The table must have the `delta.enableChangeDataFeed`
table property set to `true` for every version in the range you want to read. If CDF is
disabled for any version in the range, `TableChanges::try_new` returns an error.

## Creating a TableChanges

`TableChanges::try_new` takes a table URL, an `Engine` reference, a start version, and an
optional end version. It validates that CDF is enabled and that the schema is compatible
across the requested range.

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::table_changes::TableChanges;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/my-table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
// Read changes from version 0 through version 5 (inclusive)
let table_changes = TableChanges::try_new(url, &engine, 0, Some(5))?;
# Ok(())
# }
```

If you omit the end version by passing `None`, Kernel defaults to the latest version of the
table at the time of the call.

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::table_changes::TableChanges;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/my-table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
// Read all changes from version 0 to the latest version
let table_changes = TableChanges::try_new(url, &engine, 0, None)?;
# Ok(())
# }
```

`try_new` performs several validation checks before returning:

- CDF must be enabled at both the start and end versions.
- The table schema at the start and end versions must be identical.
- No unsupported reader features (other than deletion vectors) are enabled.

If any check fails, it returns an `Error`.

## CDF metadata columns

The schema of `TableChanges` is the table's schema at the end version plus three additional
columns:

| Column | Type | Description |
|--------|------|-------------|
| `_change_type` | `STRING` (non-nullable) | One of `insert`, `delete`, `update_preimage`, or `update_postimage` |
| `_commit_version` | `LONG` (non-nullable) | The table version where this change was committed |
| `_commit_timestamp` | `TIMESTAMP` (non-nullable) | The timestamp of the commit. When In-Commit Timestamps are enabled, this is the ICT value; otherwise it falls back to the log file's modification time. |

You can access the full schema (table columns plus CDF columns) via
`table_changes.schema()`. You can include any of the CDF columns in a projection, the same
way you would project regular table columns.

## Building and executing a scan

`TableChangesScanBuilder` works like `ScanBuilder` for regular scans. You can optionally
project columns with `with_schema` and filter rows with `with_predicate`.

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::expressions::{column_expr, Scalar};
# use delta_kernel::table_changes::TableChanges;
# use delta_kernel::{DeltaResult, Predicate};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/my-table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let table_changes = TableChanges::try_new(url, &engine, 0, Some(5))?;
// 1. Project only the columns you need (including CDF metadata)
let schema = table_changes
    .schema()
    .project(&["name", "age", "_change_type", "_commit_version"])?;

// 2. Build a predicate on table columns
let predicate = Arc::new(Predicate::gt(column_expr!("age"), Scalar::from(25)));

// 3. Build and execute the scan
let scan = table_changes
    .into_scan_builder()
    .with_schema(schema)
    .with_predicate(predicate)
    .build()?;

for data in scan.execute(Arc::new(engine))? {
    let batch = data?;
    println!("Got {} rows of change data", batch.len());
}
# Ok(())
# }
```

If you skip `with_schema`, the scan returns all table columns plus all three CDF columns.
If you skip `with_predicate`, no filtering is applied.

> [!NOTE]
> Predicates on the CDF metadata columns (`_change_type`, `_commit_version`,
> `_commit_timestamp`) are not currently supported. Apply predicates only to regular table
> columns. Filtering on CDF columns after the scan returns is still possible in your
> connector code.

> [!NOTE]
> Like regular scans, CDF filtering is best-effort. The scan may return rows that do not
> match your predicate. Apply row-level filtering in your connector if exact results are
> required.

## Using `Arc<TableChanges>`

If you need to retain ownership of the `TableChanges` (for example, to inspect its schema
after building the scan), use `scan_builder` on an `Arc<TableChanges>` instead of
`into_scan_builder`:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::table_changes::TableChanges;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/my-table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
let table_changes = Arc::new(
    TableChanges::try_new(url, &engine, 0, Some(5))?
);

let scan = table_changes.clone().scan_builder().build()?;

// You can still access table_changes after building the scan
println!("CDF range: version {} to {}", table_changes.start_version(), table_changes.end_version());
# Ok(())
# }
```

## What's next

- [Building a Scan](./building_a_scan.md): the core scan pattern that CDF scanning builds on
- [Filter Pushdown and File Skipping](./filter_pushdown.md): predicate construction and
  data skipping behavior
- [Column Selection](./column_selection.md): projecting specific columns from a scan
