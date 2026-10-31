# Altering a table

To add a column to an existing Delta table, you configure an existing-table transaction with one
or more schema operations and commit it. A standalone ALTER commit updates the table's schema
without rewriting data files. The same builder also supports schema evolution alongside a write.

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
Pass it explicitly when you construct the builder. The table shows typical workflows.

| Transaction Component | ALTER TABLE | SCHEMA EVOLUTION |
|--------|------------------------|---------------------------------|
| Operation | `UpdateTableOperation::AlterTable` | The data operation, such as `UpdateTableOperation::Write` |
| Schema operations | Required | Queued alongside the data operation |
| Data-file changes | None staged | Staged on the built transaction |
| `data_change` constructor argument | `false` | `true` if records change; `false` otherwise |

Both workflows use `UpdateTableTransactionBuilder`. You queue all schema operations before `build()`;
Kernel validates and applies them during `build()`. For schema evolution, you stage data-file changes
on the returned `Transaction`, whose `WriteState` uses the evolved schema.

`ALTER TABLE` requires `dataChange = false`. Kernel checks this at `build()` and retains a defensive
check at `commit()`. Commit validation also rejects staged file additions, removals, and
deletion-vector updates, including empty or fully unselected batches. Staged manifest writes are
also rejected.
The supplied value cannot be changed and is preserved even when no file batches are staged.

For a rewrite that preserves logical data, select the data operation, such as
`UpdateTableOperation::Optimize`, and pass `false` to `update_table()`.

> [!NOTE]
> The supported operations are adding columns and making columns nullable. Other schema operations
> (drop column, rename, type changes) are not yet available through the
> transaction builder.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.update_table(UpdateTableOperation::AlterTable, "my-app/1.0", false)` to get an
   `UpdateTableTransactionBuilder`.
3. Call `add_column()` with the new field.
4. Call `build()` to produce a `Transaction`.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::{CommitResult, UpdateTableOperation};
# use delta_kernel::{KernelResult, Snapshot};
# fn example() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit an alter-table transaction that adds a new column.
let result = snapshot
    .update_table(UpdateTableOperation::AlterTable, "my-app/1.0", /* data_change */ false)
    .add_column(StructField::nullable("country", DataType::STRING))
    .build_with_filesystem_committer(&engine)?
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

If the table declares CHECK constraints, acknowledge that your connector enforces them before you
commit. See [Check constraints](./check_constraints.md) for the acknowledgement.

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
    .update_table(UpdateTableOperation::AlterTable, "my-app/1.0", /* data_change */ false)
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build_with_filesystem_committer(&engine)?
    .commit(&engine)?;
```

Building an `UpdateTableOperation::AlterTable` transaction without adding a schema operation returns an
error. The unified builder performs this check at runtime because other operations may combine
schema evolution with data-file actions.

## Evolving a schema while writing

Select a data operation, such as `UpdateTableOperation::Write`, rather than `AlterTable` when the
same transaction evolves the schema and writes files. Choose `data_change` based on whether the
logical data changes, not merely whether files are written. Kernel applies the schema operations
during `build()`, so the transaction's `WriteState` uses the evolved schema.

```rust,ignore
let mut transaction = snapshot
    .update_table(UpdateTableOperation::Write, "my-app/1.0", /* data_change */ true)
    .add_column(StructField::nullable("country", DataType::STRING))
    .build_with_filesystem_committer(&engine)?;

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
