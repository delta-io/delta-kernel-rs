# Check constraints

**CHECK constraints** are Boolean SQL expressions that every row in a Delta table must satisfy. To
write to a table with CHECK constraints, you discover the constraints, acknowledge that your
connector enforces them, validate each batch before writing it, and commit.

Before reading this page, make sure you understand [Appending data](./append.md).

> [!NOTE]
> CHECK constraint support is experimental and requires the `check-constraints-in-dev` Cargo feature
> on `delta_kernel`. Without it, the APIs on this page don't exist and Kernel rejects writes to any
> table that supports the `checkConstraints` table feature. See
> [Feature flags](../concepts/feature_flags.md).

## Check constraints example

Suppose the `people` table has columns `name` (STRING), `age` (INTEGER), and `city` (STRING), with
a constraint named `valid_age` that requires `age > 0`. These SQL inserts produce different results:

| SQL input | Result |
|-----------|--------|
| `INSERT INTO people VALUES ('Alice', 30, 'Seattle')` | Written |
| `INSERT INTO people VALUES ('Bob', -25, 'Portland')` | Rejected, because `age > 0` is `false` |
| `INSERT INTO people VALUES ('Carol', NULL, 'Denver')` | Rejected, because `age > 0` is `NULL` |

A row passes only when every constraint evaluates to `true`. Unlike a SQL `WHERE` clause, a `NULL`
result is a violation. For the protocol contract, see
[CHECK constraints in the Delta protocol][check-constraints].

Kernel's create-table API rejects CHECK constraint properties. Add constraints with another Delta
writer. For example, in a SQL engine that supports Delta CHECK constraints:

```sql
ALTER TABLE people ADD CONSTRAINT valid_age CHECK (age > 0);
```

## How Kernel supports check constraints

Kernel never sees the rows your connector writes, so it can't evaluate constraints. Instead, it
exposes each constraint's SQL and requires your connector to take responsibility for enforcing it.
A write follows this order:

```text
snapshot.transaction(..)      start the transaction
txn.check_constraints()       discover the constraints and compile them in your evaluator
txn.ack_check_constraints()   acknowledge that your connector enforces them
txn.write_state()             fails on a constrained table without the acknowledgement
  validate, write, add_files  for each batch
txn.commit(..)                fails on a constrained table without the acknowledgement
```

### Discovering constraints

The `TableWriteExpressions` trait exposes a table's constraints through `check_constraints()`.
Both `Snapshot` and `Transaction` implement it. Each `CheckConstraint` provides the constraint's
name through `name()` and its SQL expression, verbatim from the table, through `raw_sql()`.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{Result, Snapshot};
use delta_kernel::write_expressions::TableWriteExpressions;
# fn main() -> Result<()> {
# let url = delta_kernel::try_parse_uri("/tmp/people")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();

let snapshot = Snapshot::builder_for(url).build(&engine)?;
for constraint in snapshot.check_constraints() {
    println!("{}: {}", constraint.name(), constraint.raw_sql());
}
# Ok(())
# }
```

For the `people` table, this prints:

```text
valid_age: age > 0
```

Constraints are sorted by name. The SQL refers to logical column names, so parse it against the
table's logical schema. Discovery has no side effects. It doesn't acknowledge anything.

### Enforcing constraints during an append

This example appends batches to an existing unpartitioned table. Your connector supplies
`compile_check`, which parses one constraint's SQL with your compute engine. It returns a
`CompiledCheck`, a function that evaluates the constraint over a batch and returns one Boolean per
row. Kernel doesn't provide a SQL evaluator for constraints.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::arrow::array::{BooleanArray, RecordBatch};
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{KernelError, Result, SnapshotRef};
# use delta_kernel_default_engine::executor::TaskExecutor;
# use delta_kernel_default_engine::DefaultEngine;
# type CompiledCheck = Box<dyn Fn(&RecordBatch) -> Result<BooleanArray>>;
use delta_kernel::write_expressions::TableWriteExpressions;

async fn append_with_check_constraints(
    engine: &DefaultEngine<impl TaskExecutor>,
    snapshot: SnapshotRef,
    batches: impl IntoIterator<Item = Result<RecordBatch>>,
    compile_check: impl Fn(&str) -> Result<CompiledCheck>,
) -> Result<CommitResult> {
    // 1. Start the transaction.
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine)?;

    // 2. Discover the constraints and compile each one before writing anything.
    let mut checks = Vec::new();
    for constraint in txn.check_constraints() {
        let check = compile_check(constraint.raw_sql())?;
        checks.push((constraint.name().to_string(), check));
    }

    // 3. Acknowledge responsibility for enforcement and prepare the write context.
    txn.ack_check_constraints();
    let write_state = txn.write_state()?;
    let write_context = write_state.write_context_builder().build()?;

    // 4. Validate each batch before writing it.
    for batch in batches {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        for (name, check) in &checks {
            // `true_count` skips nulls, so a `NULL` result fails the check like `false` does.
            let passed = check(&batch)?;
            if passed.true_count() != batch.num_rows() {
                return Err(KernelError::generic(format!("CHECK constraint {name} violated")));
            }
        }
        let data = ArrowEngineData::new(batch);
        let file_metadata = engine.write_parquet(&data, &write_context).await?;
        txn.add_files(file_metadata);
    }

    // 5. Commit every validated file.
    txn.commit(engine)
}
```

For the `people` table, a batch containing Alice's row passes `valid_age` and is written. A batch
containing Bob's or Carol's row fails before it reaches `write_parquet`, and the function returns
an error without committing. Files written for earlier batches never become part of the table,
because the transaction doesn't commit. Other Delta writers also fail the whole write on the first
violation.

If your evaluator can't compile a constraint, fail the write. Skipping a constraint lets rows that
violate it into the table. Handle the returned `CommitResult` as described in
[Appending data](./append.md#committing).

> [!WARNING]
> Calling `ack_check_constraints()` is a promise that every row this transaction commits satisfies
> every constraint. Kernel doesn't verify it. Acknowledging without validating writes rows that
> other Delta writers consider invalid, and readers can't detect them.

## Limitations and common questions

### Which operations need the acknowledgement?

On a table that has CHECK constraints, `write_state()` and `commit()` both fail with an
`InvalidTransactionState` error until you call `ack_check_constraints()`. Kernel applies this to
every commit, including commits that only remove files or record a transaction identifier. Kernel
can't tell which commits add unchecked rows, so it doesn't make exceptions. A table that supports
the `checkConstraints` feature but declares no constraints needs no acknowledgement.

The acknowledgement belongs to one transaction. Acknowledge again on each new transaction.

### What happens when I retry after a conflict?

A retry starts a new transaction from a newer Snapshot, and the table's constraints might have
changed in between. Discover the constraints again on the new transaction. Revalidate any files you
carry over from the earlier attempt before you acknowledge and commit them.

### How do partition columns and column mapping affect constraints?

Constraints can reference partition columns. Evaluate them against the logical rows, including
partition values, before you split rows into partitions and drop the partition columns from the
data. See [Writing to partitioned tables](./partitioned_writes.md).

With column mapping, the SQL still uses logical column names. Validate the logical data, and let
Kernel's write context handle the mapping to physical names.

### Can I add or drop constraints through Kernel?

No. Kernel's create-table API rejects `delta.constraints.<name>` properties, and its alter-table
API can't change constraints. Use another Delta writer for these operations.

### Why can I read a table that Kernel won't write to?

`checkConstraints` is a writer-only table feature, so reads never need constraint support. If a
table declares constraints but its protocol doesn't support `checkConstraints`, Kernel still reads
it. Creating a transaction on it fails with an `InvalidProtocol` error, because the table violates
the protocol.

## What's next

- [Appending data](./append.md) covers writing files and committing your validated data.
- [Writing to partitioned tables](./partitioned_writes.md) covers binding partition values.
- [Column defaults](./column_defaults.md) uses the same discover and acknowledge pattern.

[check-constraints]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#check-constraints
