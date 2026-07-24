# Filter pushdown and file skipping

To reduce the amount of data your connector reads from storage, you can provide a predicate
to a scan. Kernel uses the predicate for **data skipping**, evaluating file-level statistics
to skip entire Parquet files that cannot contain matching rows.

Before reading this page, make sure you understand
[Building a Scan](./building_a_scan.md).

## Building predicates

A **predicate** is a boolean expression that describes which rows you want. Kernel
evaluates predicates against file-level statistics to skip files before reading them.

Predicates are built from the `Predicate` type and `Expression` values. The simplest way
is to use the `column_expr!` macro for column references and `Scalar` for literal values.

### Comparison operators

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{column_expr, Predicate, Scalar};

// age < 30
let pred = Predicate::lt(column_expr!("age"), Scalar::from(30));

// price >= 100.0
let pred = Predicate::ge(column_expr!("price"), Scalar::from(100.0_f64));

// name == "Alice"
let pred = Predicate::eq(column_expr!("name"), Scalar::from("Alice"));

// status != "deleted"
let pred = Predicate::ne(column_expr!("status"), Scalar::from("deleted"));
```

The full set of comparison constructors:

| Constructor | SQL equivalent |
|-------------|----------------|
| `Predicate::eq(a, b)` | `a = b` |
| `Predicate::ne(a, b)` | `a != b` |
| `Predicate::lt(a, b)` | `a < b` |
| `Predicate::le(a, b)` | `a <= b` |
| `Predicate::gt(a, b)` | `a > b` |
| `Predicate::ge(a, b)` | `a >= b` |
| `Predicate::distinct(a, b)` | `a IS DISTINCT FROM b` |

`distinct` is a NULL-safe inequality: it returns true when `a` and `b` differ, even when
one or both are NULL. The other comparisons follow SQL NULL semantics and produce NULL when
either input is NULL.

Each constructor takes `impl Into<Expression>` for both arguments, so you can pass
`column_expr!()` results, `Scalar` values, or any `Expression` directly.

### Combining predicates

Use `and`, `or`, and `not` to build compound predicates:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{column_expr, Predicate, Scalar};

// age >= 18 AND age < 65
let pred = Predicate::and(
    Predicate::ge(column_expr!("age"), Scalar::from(18)),
    Predicate::lt(column_expr!("age"), Scalar::from(65)),
);

// status == "active" OR status == "pending"
let pred = Predicate::or(
    Predicate::eq(column_expr!("status"), Scalar::from("active")),
    Predicate::eq(column_expr!("status"), Scalar::from("pending")),
);

// NOT (archived)
let pred = Predicate::not(
    Predicate::eq(column_expr!("archived"), Scalar::from(true)),
);
```

For combining more than two predicates, use `and_from` or `or_from`:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{column_expr, Predicate, Scalar};

// age >= 18 AND country == "US" AND active == true
let pred = Predicate::and_from([
    Predicate::ge(column_expr!("age"), Scalar::from(18)),
    Predicate::eq(column_expr!("country"), Scalar::from("US")),
    Predicate::eq(column_expr!("active"), Scalar::from(true)),
]);
```

### NULL checks

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{column_expr, Predicate};

// email IS NULL
let pred = Predicate::is_null(column_expr!("email"));

// email IS NOT NULL
let pred = Predicate::is_not_null(column_expr!("email"));
```

### Nested columns

The `column_expr!` macro supports dot-separated paths for nested struct fields:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{column_expr, Predicate, Scalar};

// address.city == "Seattle"
let pred = Predicate::eq(
    column_expr!("address.city"),
    Scalar::from("Seattle"),
);
```

### Method syntax

You can also build predicates using method syntax on `Expression`:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{column_expr, Scalar};

// age < 30
let pred = column_expr!("age").lt(Scalar::from(30));

// name == "Alice"
let pred = column_expr!("name").eq(Scalar::from("Alice"));

// email IS NOT NULL
let pred = column_expr!("email").is_not_null();
```

## Applying a predicate to a scan

Pass the predicate to `ScanBuilder::with_predicate`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::expressions::{column_expr, Predicate, Scalar};
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let predicate = Arc::new(
    Predicate::and(
        Predicate::ge(column_expr!("age"), Scalar::from(18)),
        Predicate::lt(column_expr!("age"), Scalar::from(65)),
    )
);

let scan = snapshot
    .scan_builder()
    .with_predicate(predicate)
    .build()?;
# Ok(())
# }
```

`with_predicate` takes `impl Into<Option<PredicateRef>>`, so you can pass an
`Arc<Predicate>` directly.

## How data skipping works

When a scan has a predicate, Kernel applies it in two stages to eliminate files before
your connector reads them.

**File skipping using statistics.** Each Parquet file in a Delta table has associated
statistics: minimum and maximum values per column, null counts, and row counts. Kernel
rewrites your predicate into a data skipping predicate that evaluates against these
statistics. For example, given the predicate `age < 30`, if a file's minimum value for
`age` is 35, Kernel knows no rows in that file can match and skips it entirely. If a
file's minimum is 10 and maximum is 50, the file *might* contain matching rows, so
Kernel keeps it.

**Partition pruning.** For partitioned tables, partition column values are stored in the
Delta log metadata rather than in the Parquet files. Kernel evaluates predicates on
partition columns directly against these metadata values, which is even cheaper than
statistics-based skipping because no file I/O is required.

## Filtering is best-effort

> [!WARNING]
> Data skipping is an optimization, not a guarantee. The scan may return rows that do
> not match your predicate. Your connector must apply row-level filtering after reading
> the data if exact results are required.

This happens for several reasons:

- **Statistics are at the file level**, not the row level. A file whose min/max range
  overlaps the predicate may still contain non-matching rows.
- **Not all columns have statistics.** Delta tables have a configurable limit on how
  many columns collect statistics (default: 32).
- **Kernel may not fully evaluate complex predicates.** It skips what it can and passes
  through the rest.

<!-- TODO: Clarify row-level filtering guidance. -->

## Choosing data-skipping mechanisms

To choose where Kernel performs data skipping, pass a `DataSkippingOptions` value to
`ScanBuilder::with_data_skipping`. Kernel has two independent mechanisms:

- **In-memory data skipping** evaluates the predicate during transaction-log replay. It includes
  statistics-based data skipping, partition pruning, and statically false predicates.
- **Checkpoint predicate pushdown** passes a statistics-based predicate to the Engine while it
  reads checkpoint and sidecar Parquet files. A checkpoint is a Parquet snapshot of transaction-log
  actions. This mechanism doesn't apply to JSON transaction-log files.

The four modes select a combination of these mechanisms:

| Mode | In-memory data skipping | Checkpoint predicate pushdown |
|------|-------------------------|-------------------------------|
| `DataSkippingOptions::All` | Yes | Yes |
| `DataSkippingOptions::InMemory` | Yes | No |
| `DataSkippingOptions::CheckpointPushdown` | No | Yes |
| `DataSkippingOptions::None` | No | No |

`DataSkippingOptions::All` is the default. Use `InMemory` when your Engine shouldn't receive a
checkpoint predicate. Use `CheckpointPushdown` when the Engine can prune checkpoint reads and you
don't want Kernel to evaluate every active file in memory. Use `None` when your connector or compute
engine owns all data skipping.

For example, this Scan leaves data skipping to the connector:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::scan::DataSkippingOptions;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot
    .scan_builder()
    .with_data_skipping(DataSkippingOptions::None)
    .build()?;
# Ok(())
# }
```

Disabling Kernel's mechanisms doesn't remove the Scan's physical predicate. Your connector can
still apply that predicate when it reads table data.

## Controlling statistics output

To choose which file statistics your connector receives in scan metadata, pass a `StatsOptions`
value to `ScanBuilder::with_stats`. This setting doesn't enable or disable data skipping. Kernel
may read predicate statistics internally and remove them before returning scan metadata.

| Goal | Option |
|------|--------|
| Receive JSON statistics only | `StatsOptions::json_only()` (default) |
| Receive all structured statistics without JSON | `StatsOptions::all_struct()` |
| Receive structured statistics for selected columns | `StatsOptions::struct_columns(cols)` |
| Receive JSON and all structured statistics | `StatsOptions::all()` |
| Receive no statistics | `StatsOptions::none()` |

`StatsOptions::all_struct()` exposes min values, max values, null counts, and row counts in the
`stats_parsed` column. It avoids serializing structured statistics to JSON on checkpoints that
store only the structured representation:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::scan::StatsOptions;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot
    .scan_builder()
    .with_stats(StatsOptions::all_struct())
    .build()?;
# Ok(())
# }
```

Which columns have statistics depends on the table's configuration. To request a subset, pass
logical column names to `StatsOptions::struct_columns`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::expressions::ColumnName;
# use delta_kernel::scan::StatsOptions;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot
    .scan_builder()
    .with_stats(StatsOptions::struct_columns(vec![
        ColumnName::new(["age"]),
        ColumnName::new(["city"]),
    ]))
    .build()?;
# Ok(())
# }
```

Only the requested columns appear in `stats_parsed`. An empty column list normalizes to
`StatsOptions::none()` and changes only statistics output. The selected `DataSkippingOptions` mode
still controls data skipping.

### Combining the controls

To keep Kernel data skipping without returning statistics, combine `All` with `none()`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::scan::{DataSkippingOptions, StatsOptions};
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot
    .scan_builder()
    .with_data_skipping(DataSkippingOptions::All)
    .with_stats(StatsOptions::none())
    .build()?;
# Ok(())
# }
```

Kernel can still read the statistics needed by the predicate, but those internal statistics don't
appear in scan metadata. To avoid statistics reads as well as statistics output, use
`DataSkippingOptions::None` with `StatsOptions::none()`.

## What's next

- [Column Selection](./column_selection.md) covers projecting specific columns to further
  reduce the data you read.
- [Scan Metadata](./scan_metadata.md) explains how to access per-file scan information,
  including partition values and deletion vectors.
