# Generated columns

A **generated column** is a column whose value is defined by a SQL expression over the row's other
columns. To write to a table with generated columns, you discover the generated columns, acknowledge
that your connector enforces them, compute or verify their values in each batch before writing it,
and commit.

Before reading this page, make sure you understand [Appending data](./append.md).

> [!NOTE]
> Generated column support is experimental and requires the `generated-columns-in-dev` Cargo
> feature on `delta_kernel`. Without it, the APIs on this page don't exist and Kernel rejects
> writes to any table that supports the `generatedColumns` table feature. See
> [Feature flags](../concepts/feature_flags.md).

## Generated columns example

Suppose the `people` table has columns `name` (STRING), `age` (INTEGER), `city` (STRING), and
`city_upper` (STRING), where `city_upper` is generated as `UPPER(city)`. These SQL inserts produce
different results:

| SQL input | Result |
|-----------|--------|
| `INSERT INTO people (name, age, city) VALUES ('Alice', 30, 'Seattle')` | Written, with `city_upper` computed as `'SEATTLE'` |
| `INSERT INTO people VALUES ('Bob', 25, 'Portland', 'PORTLAND')` | Written, because the supplied value matches `UPPER(city)` |
| `INSERT INTO people VALUES ('Carol', 35, 'Denver', 'Boulder')` | Rejected, because `'Boulder'` doesn't match `UPPER(city)` |

Every row's value must match the expression, and the comparison is null-safe: a `NULL` value
matches a `NULL` expression result. For the protocol contract, see
[Generated columns in the Delta protocol][generated-columns].

Kernel's public create-table API can't enable generated columns. Create generated columns with
another Delta writer. For example, in a SQL engine that supports Delta generated columns:

```sql
CREATE TABLE people (
  name STRING,
  age INT,
  city STRING,
  city_upper STRING GENERATED ALWAYS AS (UPPER(city))
) USING DELTA;
```

## How Kernel supports generated columns

Kernel never sees the rows your connector writes, so it can't evaluate generation expressions.
Instead, it exposes each generated column's SQL and requires your connector to take responsibility
for enforcing it. A write follows this order:

```text
snapshot.update_table(..)    start the transaction
txn.generated_columns()      discover the generated columns and compile them in your evaluator
txn.ack_generated_columns()  acknowledge that your connector enforces them
txn.write_state()            fails on a table with generated columns without the acknowledgement
  compute, verify, write,    for each batch
  add_files
txn.commit(..)               fails on a table with generated columns without the acknowledgement
```

### Discovering generated columns

`Snapshot::generated_columns()` returns a table's generated columns at the snapshot's version, and
`Transaction::generated_columns()` returns the effective generated columns of the transaction.
Each `GeneratedColumn` provides the column's name through `name()` and its generation expression,
verbatim from the table, through `raw_sql()`.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{KernelResult, Snapshot};
# fn main() -> KernelResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/people")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();

let snapshot = Snapshot::builder_for(url).build(&engine)?;
for column in snapshot.generated_columns()? {
    println!("{}: {}", column.name(), column.raw_sql());
}
# Ok(())
# }
```

For the `people` table, this prints:

```text
city_upper: UPPER(city)
```

Generated columns come in schema order. The SQL refers to logical column names, so parse it
against the table's logical schema. Discovery fails with a schema error when a generation
expression isn't a string, and Kernel rejects writes to such a table.

### Enforcing generated columns during an append

This example appends batches to an existing unpartitioned table. Your connector supplies
`compile_generation`, which parses one generation expression with your compute engine. It returns a
`CompiledExpression`, a function that evaluates the expression over a batch and returns one value
per row, cast to the generated column's declared type. Kernel doesn't provide a SQL evaluator for
generation expressions.

For each generated column, the example computes the expression over the batch. When the batch omits
the column, the example appends the computed values with the hidden `append_column` helper, which
keeps the `people` schema order because `city_upper` is its last column. When the batch supplies
the column, the example compares the supplied values with the computed ones.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel::arrow::array::{Array, ArrayRef, RecordBatch};
# use delta_kernel::arrow::compute::kernels::cmp::not_distinct;
# use delta_kernel::arrow::datatypes::{Field, Schema};
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::transaction::{CommitResult, UpdateTableOperation};
# use delta_kernel::{KernelError, KernelResult, SnapshotRef};
# use delta_kernel_default_engine::executor::TaskExecutor;
# use delta_kernel_default_engine::DefaultEngine;
# type CompiledExpression = Box<dyn Fn(&RecordBatch) -> KernelResult<ArrayRef>>;
# fn append_column(batch: RecordBatch, name: &str, values: ArrayRef) -> KernelResult<RecordBatch> {
#     let mut fields = batch.schema().fields().to_vec();
#     fields.push(Arc::new(Field::new(name, values.data_type().clone(), true)));
#     let mut columns = batch.columns().to_vec();
#     columns.push(values);
#     Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
# }
async fn append_with_generated_columns(
    engine: &DefaultEngine<impl TaskExecutor>,
    snapshot: SnapshotRef,
    batches: impl IntoIterator<Item = KernelResult<RecordBatch>>,
    compile_generation: impl Fn(&str) -> KernelResult<CompiledExpression>,
) -> KernelResult<CommitResult> {
    // 1. Start the transaction.
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "my-app/1.0", /* data_change */ true)
        .build(engine, Box::new(FileSystemCommitter::new()))?;

    // 2. Discover the generated columns and compile each one before writing anything.
    let mut generations = Vec::new();
    for column in txn.generated_columns()? {
        let expression = compile_generation(column.raw_sql())?;
        generations.push((column.name().to_string(), expression));
    }

    // 3. Acknowledge responsibility for enforcement and prepare the write context.
    txn.ack_generated_columns();
    let write_state = txn.write_state()?;
    let write_context = write_state.write_context_builder().build()?;

    // 4. Compute omitted generated columns and verify supplied ones before writing each batch.
    for batch in batches {
        let mut batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        for (name, expression) in &generations {
            let generated = expression(&batch)?;
            match batch.column_by_name(name) {
                Some(supplied) => {
                    // `not_distinct` is null-safe equality, so a `NULL` value matches a `NULL`
                    // expression result.
                    let matched = not_distinct(supplied, &generated)?;
                    if matched.true_count() != batch.num_rows() {
                        return Err(KernelError::generic(format!(
                            "generated column {name} doesn't match its expression"
                        )));
                    }
                }
                None => batch = append_column(batch, name, generated)?,
            }
        }
        let data = ArrowEngineData::new(batch);
        let file_metadata = engine.write_parquet(&data, &write_context).await?;
        txn.add_files(file_metadata);
    }

    // 5. Commit every file.
    txn.commit(engine)
}
```

For the `people` table, a batch containing only Alice's `name`, `age`, and `city` gets
`city_upper` computed as `'SEATTLE'` and is written. A batch that supplies Bob's `'PORTLAND'`
passes verification. A batch that supplies Carol's `'Boulder'` fails before it reaches
`write_parquet`, and the function returns an error without committing. Files written for earlier
batches never become part of the table, because the transaction doesn't commit.

If your evaluator can't compile a generation expression, fail the write. Skipping one lets values
that don't match it into the table. Handle the returned `CommitResult` as described in
[Appending data](./append.md#committing).

> [!WARNING]
> Calling `ack_generated_columns()` is a promise that every row this transaction commits matches
> every generation expression. Kernel doesn't verify it. Query engines derive filters, including
> partition pruning, from generation expressions, so a value that doesn't match its expression can
> make queries silently skip the row.

## Limitations and common questions

### Which operations need the acknowledgement?

On a table that has generated columns, `write_state()` and `commit()` both fail with an
`InvalidTransactionState` error until you call `ack_generated_columns()`. Kernel applies this to
every commit, including schema changes and commits that only remove files or record a transaction
identifier. The acknowledgement covers everything the transaction commits. A table that supports
the `generatedColumns` feature but has no generated columns needs no acknowledgement.

The acknowledgement belongs to one transaction. Acknowledge again on each new transaction.

### What happens when I retry after a conflict?

A retry starts a new transaction from a newer Snapshot, and the table's generated columns might
have changed in between. Discover the generated columns again on the new transaction. Revalidate
any files you carry over from the earlier attempt before you acknowledge and commit them.

### How do partition columns and column mapping affect generated columns?

A generated column is often a partition column, such as a date computed from a timestamp. Compute
it against the logical rows before you split rows into partitions, and bind the computed value as
the partition value. See [Writing to partitioned tables](./partitioned_writes.md).

With column mapping, the SQL still uses logical column names. Compute and verify against the
logical data, and let Kernel's write context handle the mapping to physical names.

### Can I add or drop generated columns through Kernel?

No. Use another Delta writer to add or drop generated columns.

### Why doesn't Kernel report a column that has a generation expression?

Only top-level columns can be generated columns, and only on a table whose protocol supports the
`generatedColumns` table feature. When a top-level column carries `delta.generationExpression`
metadata on a table whose protocol doesn't support the feature, Kernel reads the table but rejects
writes to it with an `InvalidProtocol` error.

## What's next

- [Appending data](./append.md) covers writing files and committing your data.
- [Writing to partitioned tables](./partitioned_writes.md) covers binding partition values.
- [Column defaults](./column_defaults.md) uses the same discover and acknowledge pattern.

[generated-columns]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#generated-columns
