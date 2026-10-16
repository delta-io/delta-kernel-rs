# Altering a table

To add a column to an existing Delta table, you build an `AlterTableTransaction`
from a `Snapshot`, queue one or more schema operations, and commit. The result
is a metadata-only commit that updates the table's schema without rewriting any
data files.

Before reading this page, make sure you understand
[Creating a Table](./create_table.md) and
[Appending Data](./append.md).

## ALTER TABLE versus schema evolution

Use **ALTER TABLE** for a standalone, metadata-only schema change. For example, you can add a nullable
column without writing or removing data files.

Use **SCHEMA EVOLUTION** to change the schema while writing or rewriting data files in the same
transaction. For example, you can add a nullable column and append rows containing values for that
column atomically.

The `dataChange` flag indicates whether file actions change the table's logical data.

| Transaction Component | ALTER TABLE | SCHEMA EVOLUTION |
|--------|------------------------|---------------------------------|
| Builder | `AlterTableTransactionBuilder` | `UpdateTableTransactionBuilder` |
| Schema operations | Required | Queued alongside the data operation |
| Data-file changes | Unavailable at compile time | Staged on the built transaction |
| `dataChange` | Always `false` | Required when constructing the builder |

Both builders use `TransactionBuilder<T>` and share schema operations and common setters.
You queue all schema operations before `build()`; Kernel validates and applies them during
`build()`. Schema evolution during a write also lets you stage data-file changes on the returned
`Transaction`, whose `WriteState` uses the evolved schema.

For schema evolution during a write, supply `data_change` when constructing the update-table builder.
Kernel preserves this value even for empty commits. For protocol-valid rewrites that preserve
logical data, use `snapshot.update_table(operation, engine_info, false)`.

> [!NOTE]
> The supported operations are adding columns and making columns nullable. Other schema operations
> (drop column, rename, type changes) are not yet available through the
> transaction builder.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.alter_table("my-app/1.0")` to get an `AlterTableTransactionBuilder`.
3. Call `add_column()` with the new field.
4. Call `build()` to produce an `AlterTableTransaction`.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{Result, Snapshot};
# fn example() -> Result<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit an alter-table transaction that adds a new column.
let result = snapshot
    .alter_table("my-app/1.0")
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;

match result {
    CommitResult::Committed(committed) => {
        println!("Schema altered at version {}", committed.commit_version());
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
> Kernel rejects schema changes at `build()` when `icebergCompatV2`, `icebergCompatV3`, or
> `allowColumnDefaults` is enabled. This restriction applies to both workflows, regardless of the
> transaction operation or `dataChange` value.

## Chaining multiple operations

`add_column()` can be called more than once to add several columns in a single
commit. The operations are applied in order, and the resulting schema is
validated as a whole before the commit is constructed:

```rust,ignore
let result = snapshot
    .alter_table("my-app/1.0")
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
```

The builder uses a type-state pattern to enforce that at least one operation is
queued before `build()` is callable. Calling `.build()` directly on
`snapshot.alter_table("my-app/1.0")` without first queuing a schema operation is a compile
error.

## What you cannot do on an alter-table transaction

`AlterTableTransaction` is a metadata-only transaction. It does not implement
`SupportsDataFiles`, so the data-file methods are not available at compile
time. In particular, the following are not callable on an `AlterTableTransaction`:

| Method | Used for |
|--------|----------|
| `write_state()` | Creating the `WriteState` used to bind a `BoundWriteContext` |
| `add_files()` | Registering newly written data files |
| `stats_schema()` | Retrieving the statistics schema for written files |

The ALTER builder also has no `with_data_change()` or `with_blind_append()` setter.

## Evolving a schema while writing

Select a data operation, such as `UpdateTableOperation::Write`, rather than `AlterTable` when the
same transaction evolves the schema and writes files. Choose `data_change` based on whether the
logical data changes, not merely whether files are written. Kernel applies the schema operations
during `build()`, so the transaction's `WriteState` uses the evolved schema.

```rust,ignore
let mut transaction = snapshot
    .update_table(UpdateTableOperation::Write, "my-app/1.0", /* data_change */ true)
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?;

let write_state = transaction.write_state()?;
let write_context = write_state.write_context_builder().build()?;
// Write files using the evolved schema, add their metadata, and commit.
```

## What's next

- [Appending Data](./append.md) walks through writing data to the evolved
  table.
- [Creating a Table](./create_table.md) covers creating a new table with the
  schema you want from the start.
- [Schemas and Data Types](../concepts/schema_and_types.md) describes
  Kernel's type system in detail.
