# CHECK constraints

> This feature is in development, gated behind the `check-constraints-in-dev` cargo feature.
> The APIs on this page are unstable and are not compiled into release builds.

A CHECK constraint is a boolean SQL expression stored on the table (for example `amount > 0`).
The Delta protocol requires every row added to the table to satisfy every constraint. A row
passes only when the expression evaluates to `true`. Both `false` and `NULL` are violations.

Kernel never sees your row data on the write path, so enforcement is a contract between kernel and
your connector. Kernel discovers the constraints, parses the ones it can into predicates, and
enforces them only when you run a validator over your data. Your connector decides when to validate
and handles any constraint kernel cannot parse.

Before reading this page, make sure you understand [Appending Data](./append.md) and
[Writing to partitioned tables](./partitioned_writes.md).

## Discover and acknowledge

`Transaction::check_constraints()` returns the table's `CheckConstraints`. Calling it is also your
acknowledgment that you will enforce them. Kernel records the acknowledgment and fails a data-adding
commit to a constrained table that never acknowledged, so a connector unaware of the feature cannot
silently commit violating data.

```rust,ignore
let constraints = txn.check_constraints();
```

Each `CheckConstraint` exposes its `name()`, its `raw_sql()`, and a `predicate()`. The predicate is
`Some` when kernel parsed the expression and `None` when only the raw SQL is available. Kernel does
not tell you which constraints are yours to enforce. You iterate the set and branch on
`predicate()`. The set-level `is_kernel_parsable()` answers the first question: did kernel parse
every constraint?

Use `Snapshot::check_constraints()` to discover constraints without acknowledging, for example to
validate before you build a transaction. Snapshot discovery is read-only, so a later data-adding
commit still requires `Transaction::check_constraints()` or it fails closed.

## Enforce with a validator

When every constraint is kernel-parsable, build one `CheckConstraintValidator` and reuse it for
every batch. Binding fails closed if any constraint is unparsable, before any data is written.

```rust,ignore
let validator = constraints.validator(engine.evaluation_handler().as_ref())?;
validator.validate(&batch)?; // Err(CheckConstraintViolation) on the first violating row
```

If `is_kernel_parsable()` is `false`, at least one constraint exposes only its `raw_sql()`. Enforce
those with your own SQL engine, or refuse the write. A connector with no SQL engine of its own
should treat an unparsable constraint as a hard failure.

## Validate before partitioning

Validate the full logical batch before you partition or write anything. This ordering is a contract
kernel cannot enforce for you, and it delivers two guarantees.

```text
acknowledge -> build validator -> validate whole batch -> partition -> write parquet -> commit
```

First, validating before partitioning is correct by construction. The batch still carries the
partition column as ordinary per-row data, and the partition a file lands in is derived from that
same column. So the value you validate is exactly the value a reader reconstructs from the file's
partition metadata. A constraint on a partition column, or one mixing partition and data columns, is
just a predicate over the full batch. It needs no special handling.

Second, validating before writing makes the whole write atomic. Because validation completes before
the first file is written, a violation writes no Parquet and you never reach `commit()`. The table
is left unchanged.

For a distributed write, the driver acknowledges and ships the serializable inputs (the constraint
SQL, the schema, and the partition columns). Each worker rebuilds `CheckConstraints` and validates
its own batches with its own handler. The validator is decoupled from the write context, which is
not serializable.

## Complete example

```rust,ignore
// Discovering the constraints acknowledges them.
let constraints = txn.check_constraints();
if !constraints.is_kernel_parsable() {
    // At least one constraint is not kernel-parsable. Enforce those from raw_sql() with your own
    // engine, or refuse the write.
}
let validator = constraints.validator(engine.evaluation_handler().as_ref())?;

// Validate the full logical batch (partition columns present) before partitioning or writing.
validator.validate(&batch)?;

// Only now partition, write, and commit.
let write_context = txn.unpartitioned_write_context()?;
let add_files = engine.write_parquet(&batch, &write_context).await?;
txn.add_files(add_files);
txn.commit(&engine)?;
```
