# Filter pushdown and file skipping

To reduce the amount of data your connector reads from storage, you can provide a predicate
to a scan. Kernel uses the predicate for **data skipping**, evaluating file-level statistics
to skip entire Parquet files that cannot contain matching rows.

Before reading this page, make sure you understand
[Building a Scan](./building_a_scan.md).

## Building predicates

A **predicate** is a boolean expression that describes which rows you want. Kernel
evaluates predicates against file-level statistics to skip files before reading them.

Use the `expr!` macro to build expressions with infix comparisons and `col("name")`
column references. Convert its `Expression` result with `Predicate::from_expr` when
an API requires a `Predicate`.

### Comparison operators

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{expr, Predicate};

// age < 30
let pred = Predicate::from_expr(expr!(col("age") < 30));

// price >= 100.0
let pred = Predicate::from_expr(expr!(col("price") >= 100.0_f64));

// name == "Alice"
let pred = Predicate::from_expr(expr!(col("name") == "Alice"));

// status != "deleted"
let pred = Predicate::from_expr(expr!(col("status") != "deleted"));
```

The full set of comparisons inside `expr!`:

| Expression | SQL equivalent |
|-------------|----------------|
| `col("a") == col("b")` | `a = b` |
| `col("a") != col("b")` | `a != b` |
| `col("a") < col("b")` | `a < b` |
| `col("a") <= col("b")` | `a <= b` |
| `col("a") > col("b")` | `a > b` |
| `col("a") >= col("b")` | `a >= b` |
| `distinct(col("a"), col("b"))` | `a IS DISTINCT FROM b` |

`distinct` returns true when non-NULL operands differ or exactly one operand is NULL.
It returns false for two NULL operands. Other comparisons produce NULL when either operand is NULL.

Literals retain their Rust types: `30` is INTEGER and `30_i64` is LONG. Use
`lit(value)` inside `expr!` for a computed scalar value.

### Combining predicates

Use `and`, `or`, and `not` to build compound predicates:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{expr, Predicate};

// age >= 18 AND age < 65
let pred = Predicate::from_expr(expr!(and(col("age") >= 18, col("age") < 65)));

// status == "active" OR status == "pending"
let pred = Predicate::from_expr(expr!(or(
    col("status") == "active",
    col("status") == "pending",
)));

// NOT (archived)
let pred = Predicate::from_expr(expr!(not(col("archived") == true)));
```

The `and` and `or` forms accept more than two predicates:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{expr, Predicate};

// age >= 18 AND country == "US" AND active == true
let pred = Predicate::from_expr(expr!(and(
    col("age") >= 18,
    col("country") == "US",
    col("active") == true,
)));
```

### NULL checks

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{expr, Predicate};

// email IS NULL
let pred = Predicate::from_expr(expr!(is_null(col("email"))));

// email IS NOT NULL
let pred = Predicate::from_expr(expr!(is_not_null(col("email"))));
```

### Nested columns

Inside `expr!`, `col` supports dot-separated paths for nested struct fields:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{expr, Predicate};

// address.city == "Seattle"
let pred = Predicate::from_expr(expr!(col("address.city") == "Seattle"));
```

### Computed values and column names

Use `lit` for computed scalar values and `path` for explicit column-name segments:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::expressions::{expr, Predicate};

let cutoff = 30;
let column = "age";
let pred = Predicate::from_expr(expr!(path(column) < lit(cutoff)));
```

`path("address.city")` addresses one field whose name contains a dot.
`path("address", "city")` addresses the nested `city` field under `address`.

## Applying a predicate to a scan

Pass the predicate to `ScanBuilder::with_predicate`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::expressions::{expr, Predicate};
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let predicate = Arc::new(Predicate::from_expr(expr!(and(
    col("age") >= 18,
    col("age") < 65,
))));

let scan = snapshot
    .scan_builder()
    .with_predicate(predicate)
    .build()?;
# Ok(())
# }
```

`with_predicate` takes `impl Into<Option<PredicateRef>>`, so you can pass an
`Arc<Predicate>` directly.

## Collation-aware queries

To execute a collation-aware query, your connector owns the string comparison rules and
the original row filter. Kernel preserves `__COLLATIONS` schema annotations. Existing
`statsWithCollation` bounds reach your connector when you request JSON statistics.
Structured statistics omit these bounds. `StatsOptions::all_struct()` disables JSON synthesis.
Kernel's predicates and internal [data skipping](#how-data-skipping-works) use UTF-8 binary semantics.
An ordinary equality predicate for `"alice"` can skip a file containing `"Alice"`.
Evaluating a case-insensitive comparison later cannot recover that file.

For connector-owned skipping, [request JSON stats](#controlling-statistics) with
`StatsOptions::json_only()` or `StatsOptions::all()` and select the bounds that match the
filtering operation's collation exactly: provider, name, and version. The operation determines
the collation, which can differ from the schema's default. Use those bounds only with a
comparison you implement for that exact collation. If the matching entry or required bounds
are absent, retain the file.
Never substitute binary bounds or another collation/version.

Where your evaluator supports opaque predicates, you can implement `OpaquePredicateOp`.
Each `as_data_skipping_predicate` or `eval_as_data_skipping_predicate` callback must return a
safe result for its `inverted` flag's polarity, or `None` when it can't safely prune.
Kernel combines those results conservatively. Scalar evaluation has a different contract:
`eval_pred_scalar` returns `Err` for unsupported evaluation and `Ok(None)` for a SQL NULL result.
Opaque predicates can't be serialized; reconstruct them on each worker when distributing a scan.

If you don't use opaque predicates, derive a conservative binary predicate before calling
`ScanBuilder::with_predicate`:
every row that is TRUE under the original filter must also be TRUE under the derived
predicate. Let `C` be an unsupported collation-aware comparison and `B` a supported binary
predicate:

| Original filter | Conservative pushdown |
|-----------------|-----------------------|
| `C AND B` | `B` |
| `C OR B` | `TRUE` |
| `NOT C` | `TRUE` |
| `NOT (C AND B)` | `TRUE` |
| `NOT (C OR B)` | `NOT B` |

The last two rows show why negation must precede relaxation: De Morgan's laws produce different
safe predicates for negated `AND` and `OR`. Push negations to leaves before replacing unsupported
comparisons, including their negated forms, with `TRUE`. Then simplify the predicate.
Dropping an unsupported `OR` branch or negating an already relaxed predicate can discard matching
rows.

Apply the same restriction to filter and row-group-skipping predicates sent to an underlying
Parquet reader. Keep the original collation-aware filter and evaluate it on the rows that
survive these optimizations. The [collation RFC](https://github.com/delta-io/delta/blob/master/protocol_rfcs/collated-string-type.md#reader-requirements-for-collations)
defines the statistics-matching requirements.

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

## Controlling statistics

By default, Kernel reads file-level statistics from the transaction log
and uses them internally for data skipping, but does not expose those statistics to your
connector. To override the default, call `ScanBuilder::with_stats` with a
`StatsOptions` value. The named constructors cover the common shapes; you can also
build the struct directly for any combination.

### Disabling data skipping entirely

If your compute engine performs its own data skipping, you can tell Kernel to skip reading
statistics altogether. This avoids the cost of parsing statistics from checkpoint files.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::scan::StatsOptions;
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot
    .scan_builder()
    .with_stats(StatsOptions::none())
    .build()?;
# Ok(())
# }
```

With `StatsOptions::none()`:

- Kernel skips the stats column entirely during checkpoint reads.
- No statistics-based or partition-value-based file pruning occurs (row-level
  partition filtering still applies).
- The `stats` field on each `ScanFile` is `None`.

Use this when your connector or compute engine already handles file pruning and you want to
avoid the overhead of parsing statistics you won't use.

### Including all statistics in scan metadata

To receive pre-parsed statistics (min/max values, null counts, row counts) for every file in your
scan metadata, pass `StatsOptions::all_struct()` (structured stats without JSON synthesis) or
`StatsOptions::all()` (both structured stats and the legacy JSON `stats` column):

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::scan::StatsOptions;
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
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

The statistics appear in a `stats_parsed` column in the scan metadata. Which columns have
statistics depends on the table's configuration (`delta.dataSkippingStatsColumns` or
`delta.dataSkippingNumIndexedCols`).

For compatible checkpoints, `all_struct` leaves `stats` null and avoids reading or synthesizing
JSON stats. JSON commits and fallback checkpoints preserve existing JSON.

You can combine this with `with_predicate`. When both are set, Kernel performs its own data
skipping internally and exposes the parsed statistics so your connector can apply
additional pruning logic.

### Including statistics for specific columns

To receive statistics for only a subset of columns, pass `StatsOptions::struct_columns`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::expressions::column_name;
# use delta_kernel::scan::StatsOptions;
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot
    .scan_builder()
    .with_stats(StatsOptions::struct_columns(vec![
        column_name!("age"),
        column_name!("city"),
    ]))
    .build()?;
# Ok(())
# }
```

Only the named data columns appear in `stats_parsed`. Kernel may read additional statistics for
data skipping, but it removes predicate-only fields from scan metadata before returning it.

### Choosing the right mode

| Goal | Constructor |
|------|-------------|
| Default behavior (Kernel skips files internally, no stats exposed) | No call needed (or `StatsOptions::json_only()`) |
| Disable all stats reading for performance | `StatsOptions::none()` |
| Expose all structured stats without JSON synthesis | `StatsOptions::all_struct()` |
| Expose both struct stats and the JSON `stats` column | `StatsOptions::all()` |
| Expose selected structured stats without JSON synthesis | `StatsOptions::struct_columns(cols)` |

`with_stats` takes a single `StatsOptions` value, so each call fully replaces any prior
configuration. There is no "last call wins" composition to track.

## What's next

- [Column Selection](./column_selection.md) covers projecting specific columns to further
  reduce the data you read.
- [Scan Metadata](./scan_metadata.md) explains how to access per-file scan information,
  including partition values and deletion vectors.
