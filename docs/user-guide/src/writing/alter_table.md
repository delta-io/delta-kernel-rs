# Altering a table

To add a column to an existing Delta table, you build an `AlterTableTransaction`
from a `Snapshot`, queue one or more schema operations, and commit. The result
is a metadata-only commit that updates the table's schema without rewriting any
data files.

Before reading this page, make sure you understand
[Creating a Table](./create_table.md) and
[Appending Data](./append.md).

## When to use alter table

Use `alter_table()` when you need to evolve a table's schema in place. The
common case today is adding a new column to a table that already has data,
without rewriting the existing files. Existing rows read back `NULL` for the
new column. Subsequent writes can populate it.

Schema evolution is a metadata-only change. The transaction emits an updated
`Metadata` action and commits with `data_change: false`. No `Add` or `Remove`
file actions are produced. This matters because readers can apply the new
schema to existing files without scanning them, and writers that are only
concerned with data changes can ignore the commit.

> [!NOTE]
> The first supported operation is `add_column()`. Other schema operations
> (drop column, rename, type changes) are not yet available through the
> `AlterTableTransaction` API.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.alter_table()` to get an `AlterTableTransactionBuilder`.
3. Call `add_column()` with the new field.
4. Call `build()` to produce an `AlterTableTransaction`.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit an alter-table transaction that adds a new column.
let result = snapshot
    .alter_table()
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .with_engine_info("my-app/1.0")
    .commit(&engine)?;

match result {
    CommitResult::CommittedTransaction(committed) => {
        println!("Schema evolved at version {}", committed.commit_version());
    }
    _ => eprintln!("alter table did not succeed"),
}
# Ok(())
# }
```

After this commit, the table schema has four fields. Existing rows for Alice,
Bob, and Carol read back `NULL` for `country`. New writes can populate the
column by including it in the `RecordBatch` they pass to
`engine.write_parquet()`.

## Validation rules

`add_column()` checks the new field at `build()` time. If any rule is violated,
`build()` returns an error and no commit is attempted.

| Rule | Why |
|------|-----|
| The field name must not already exist (case-insensitive) | Delta column names are unique within a struct. |
| The field must be nullable | Existing files do not contain the new column. They read back `NULL`, which would violate a `NOT NULL` constraint. |
| The table must not have column mapping enabled | The current implementation supports add-column only on tables without column mapping. |
| The table must support writes | Tables with unsupported writer features cannot be altered. |
| The evolved schema must not require protocol features the table does not enable | For example, adding a `TIMESTAMP_NTZ` column to a table without the `timestampNtz` feature fails. |

> [!NOTE]
> The column-mapping limitation applies to add-column only. If your table uses
> column mapping (`delta.columnMapping.mode = "name"` or `"id"`), you cannot
> currently add a column through `alter_table()`. This restriction is expected
> to be lifted as the alter-table framework grows.

## Chaining multiple operations

`add_column()` can be called more than once to add several columns in a single
commit. The operations are applied in order, and the resulting schema is
validated as a whole before the commit is constructed:

```rust,ignore
let result = snapshot
    .alter_table()
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
```

The builder uses a type-state pattern to enforce that at least one operation is
queued before `build()` is callable. Calling `.build()` directly on
`snapshot.alter_table()` without first calling `add_column()` is a compile
error.

## What you cannot do on an alter-table transaction

`AlterTableTransaction` is a metadata-only transaction. It does not implement
`SupportsDataFiles`, so the data-file methods are not available at compile
time. In particular, the following are not callable on an `AlterTableTransaction`:

| Method | Used for |
|--------|----------|
| `unpartitioned_write_context()` / `partitioned_write_context()` | Obtaining a `WriteContext` to write Parquet files |
| `add_files()` | Registering newly written data files |
| `stats_schema()` | Retrieving the statistics schema for written files |

If you need to add data and evolve the schema, run two transactions: an
alter-table transaction first, then a write transaction against the post-commit
snapshot. See [Appending Data](./append.md) for the write flow.

## What's next

- [Appending Data](./append.md) walks through writing data to the evolved
  table.
- [Creating a Table](./create_table.md) covers creating a new table with the
  schema you want from the start.
- [Schemas and Data Types](../concepts/schema_and_types.md) describes
  Kernel's type system in detail.
