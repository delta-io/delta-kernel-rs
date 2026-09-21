# Altering a table

To add a column to an existing Delta table, you create an existing-table transaction builder
from a `Snapshot`, queue one or more schema operations, and commit. The result is a metadata-only
commit that updates the table's schema without rewriting any data files.

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
> (drop column, rename, type changes) are not yet available through `alter_table()`.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.alter_table()` to get an `ExistingTableTransactionBuilder` configured for
   schema evolution.
3. Call `add_column()` with the new field.
4. Call `build(&engine)` to produce a `Transaction<ExistingTable>`.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::{CommitResult, TransactionOptions};
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit an alter-table transaction that adds a new column.
let result = snapshot
    .alter_table()
    .with_options(TransactionOptions::new().with_engine_info("my-app/1.0"))
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine)?
    .commit(
        &engine,
        &FileSystemCommitter::new(),
        delta_kernel::transaction::CommitActions::new(),
    )?;

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
| The table must support writes | Tables with unsupported writer features cannot be altered. |
| The evolved schema must not require protocol features the table does not enable | For example, adding a `TIMESTAMP_NTZ` column to a table without the `timestampNtz` feature fails. |
| Column mapping tables must be protocol-valid | When column mapping is enabled, Kernel assigns or preserves column-mapping IDs and physical names for the added column and updates `delta.columnMapping.maxColumnId`. |

> [!NOTE]
> `ALTER TABLE` is still rejected on tables with unsupported writer features, and
> currently on tables with `icebergCompatV3` or `allowColumnDefaults` enabled.

## Chaining multiple operations

`add_column()` can be called more than once to add several columns in a single
commit. The operations are applied in order, and the resulting schema is
validated as a whole before the commit is constructed:

```rust,ignore
let result = snapshot
    .alter_table()
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build(&engine)?
    .commit(
        &engine,
        &FileSystemCommitter::new(),
        delta_kernel::transaction::CommitActions::new(),
    )?;
```

The builder requires at least one schema operation. Calling `.build(&engine)` directly on
`snapshot.alter_table()` returns an error.

## Alter-table validation

`alter_table()` is a convenience entry point for an existing-table transaction. It selects the
`ALTER TABLE` operation and configures the transaction as a non-data-changing commit. The builder
validates that the transaction contains a schema change and rejects incompatible options such as
blind append. The resulting transaction uses the same existing-table API as other updates; it is
not a separate type-state.

## What's next

- [Appending Data](./append.md) walks through writing data to the evolved
  table.
- [Creating a Table](./create_table.md) covers creating a new table with the
  schema you want from the start.
- [Schemas and Data Types](../concepts/schema_and_types.md) describes
  Kernel's type system in detail.
