# Altering a table

To add a column to an existing Delta table, you stage one or more schema
operations on the update transaction builder and commit. The result is a commit
that updates the table's schema without rewriting any data files.

Before reading this page, make sure you understand
[Creating a Table](./create_table.md) and
[Appending Data](./append.md).

## When to evolve a schema

Evolve a table's schema in place when you need to change its structure without
rewriting existing files. The common case today is adding a new column to a
table that already has data. Existing rows read back `NULL` for the new column.
Subsequent writes can populate it.

Schema evolution emits an updated `Metadata` action. Readers apply the new
schema to existing files without scanning them.

> [!NOTE]
> The first supported operation is `add_column()`. Other schema operations
> (drop column, rename, type changes) are not yet available.

## Adding a column

Suppose your table has the canonical schema `name STRING, age INTEGER, city
STRING` with rows for Alice, Bob, and Carol, and you want to add a `country`
column. The flow is:

1. Load a `Snapshot` of the table.
2. Call `snapshot.transaction()` to get an `UpdateTableTransactionBuilder`.
3. Call `add_column()` with the new field.
4. Call `build()` to produce the transaction.
5. Call `commit()` to atomically apply the schema change.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::schema::{DataType, StructField};
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Load a snapshot of the existing table.
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Build and commit a transaction that adds a new column.
let result = snapshot
    .transaction()
    .with_engine_info("my-app/1.0")
    .add_column(StructField::nullable("country", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;

match result {
    CommitResult::CommittedTransaction(committed) => {
        println!("Schema evolved at version {}", committed.commit_version());
    }
    _ => eprintln!("schema evolution did not succeed"),
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
| The table must not enable `icebergCompatV3` or `allowColumnDefaults` | Schema evolution on these tables is not yet supported. |
| The evolved schema must not require protocol features the table does not enable | For example, adding a `TIMESTAMP_NTZ` column to a table without the `timestampNtz` feature fails. |

## Chaining multiple operations

`add_column()` can be called more than once to add several columns in a single
commit. The operations are applied in order, and the resulting schema is
validated as a whole before the commit is constructed:

```rust,ignore
let result = snapshot
    .transaction()
    .add_column(StructField::nullable("country", DataType::STRING))
    .add_column(StructField::nullable("postal_code", DataType::STRING))
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?;
```

## Adding data after evolving the schema

To populate the new column, run a write transaction against the post-commit
snapshot: evolve the schema first, then write data with the new schema in a
follow-up transaction. See [Appending Data](./append.md) for the write flow.

## What's next

- [Appending Data](./append.md) walks through writing data to the evolved
  table.
- [Creating a Table](./create_table.md) covers creating a new table with the
  schema you want from the start.
- [Schemas and Data Types](../concepts/schema_and_types.md) describes
  Kernel's type system in detail.
