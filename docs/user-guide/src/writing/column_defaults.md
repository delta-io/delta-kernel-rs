# Column defaults

**Column defaults** are SQL expressions that supply values when a write omits a column or explicitly
requests its `DEFAULT` value. To write these values with Kernel, you discover the defaults, resolve
the ones your input needs, and materialize them before writing data files.

Before reading this page, make sure you understand [Appending data](./append.md).

## Column defaults example

Suppose your table has columns `name` (STRING), `age` (INTEGER), and `city` (STRING), with a default
of `'Seattle'` on `city`. These SQL inserts into `people` produce different results:

| SQL input | Row written |
|-----------|-------------|
| `INSERT INTO people (name, age) VALUES ('Alice', 30)` | `('Alice', 30, 'Seattle')` |
| `INSERT INTO people VALUES ('Bob', 25, NULL)` | `('Bob', 25, NULL)` |
| `INSERT INTO people VALUES ('Carol', 35, DEFAULT)` | `('Carol', 35, 'Seattle')` |

An explicit `NULL` is a supplied value, so it doesn't trigger the default. Your connector must
preserve the distinction between omitted values, `DEFAULT` requests, and explicit nulls until it
prepares the data. Kernel doesn't interpret a SQL `INSERT` statement or a `DEFAULT` sentinel
for you.

Defaults apply during writes. They don't backfill existing rows or substitute values during reads.
For the protocol contract, see [Default columns in the Delta protocol][default-columns].

## How Kernel supports column defaults

> [!NOTE]
> Kernel's transaction API exposes defaults only for top-level columns. See
> [What about nested defaults?](#what-about-nested-defaults) for field-level metadata access.

Kernel exposes defaults through `Transaction::top_level_column_defaults()`, keyed by logical column
name. Each `ColumnDefault` provides the original SQL through `raw_sql()`, the column's declared type
through `data_type()`, and an optional parsed `Scalar` through `to_scalar()`.

You choose whether to use Kernel's literal parser or your own SQL evaluator. In either case, your
connector fills the required values and then calls `txn.ack_column_defaults()` before requesting
`txn.write_state()`. Neither acknowledgement nor `DefaultEngine::write_parquet` fills defaults.

### Requirements and dependencies

- The table must use writer protocol version 7 and declare `allowColumnDefaults` in its
  `writerFeatures`. This writer-only table feature adds no reader requirement and has no
  dependent table features.
- Each declared default must be a SQL string in the column's `CURRENT_DEFAULT` schema metadata.
  The table feature alone doesn't give every column a default.
- Other features and data types in the table must also support writes. For example,
  `TIMESTAMP_NTZ` columns still require the `timestampNtz` table feature.
- Your connector must resolve every omitted or explicitly default-requested value that has a
  default, preserve supplied values, and produce data matching the write context's logical schema.
  Resolved values must respect the column's type and nullability.
- A transaction with the feature enabled and at least one declared default requires acknowledgement
  before `write_state()`, even if your input supplies all values explicitly.

> [!NOTE]
> Discovery and literal parsing need no dedicated Cargo feature, Arrow dependency, or SQL engine.
> The Arrow materialization example
> below uses `arrow-expression` and an Arrow version feature. `delta_kernel_default_engine` supplies
> both when configured with Arrow. See [Feature flags](../concepts/feature_flags.md).

Kernel supports writing to existing tables with column defaults, but its public create-table API
can't enable `allowColumnDefaults`. Configure defaults using another supporting Delta writer. For
example, in a SQL engine that supports Delta column defaults:

```sql
CREATE TABLE people (
  name STRING,
  age INT,
  city STRING DEFAULT 'Seattle'
) USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
```

### Inserting into a table with column defaults

For each insert, identify the columns your input omits or explicitly requests as `DEFAULT`. The
defaults for those columns are the **required defaults** for that insert. Resolve them and fill
the requested values before passing data to Kernel's write path. Keep explicitly supplied values,
including nulls, unchanged.

Kernel can parse the [supported literals](#supported-literals-and-parsing-limits) into scalars.
If a required default falls outside that subset, evaluate it in your connector or reject the insert.
Your connector can also use its own evaluator for all required defaults.

#### When Kernel can parse the required defaults

Use this path when your required defaults are literals supported by Kernel. Prepare column sources
once per transaction, then apply it to each incoming batch. Supplied columns keep their values,
including explicit nulls. Omitted columns receive their parsed default, repeated for the batch's
row count.

This example accepts batches with a fixed input schema and writes to an existing unpartitioned
table. Columns can arrive in any order. The function rejects omitted columns without a default,
including nullable columns, so your connector must supply those explicitly. Nested field omissions
and per-row `DEFAULT` requests need separate handling.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel::arrow::array::RecordBatch;
# use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_conversion::TryIntoArrow;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::expressions::Scalar;
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Error, SnapshotRef};
# use delta_kernel_default_engine::executor::TaskExecutor;
# use delta_kernel_default_engine::DefaultEngine;
// Describe how each table column gets its values.
enum ColumnSource {
    Input(usize),
    Default(Scalar),
}

async fn append_with_defaults(
    engine: &DefaultEngine<impl TaskExecutor>,
    snapshot: SnapshotRef,
    input_schema: ArrowSchemaRef,
    batches: impl IntoIterator<Item = DeltaResult<RecordBatch>>,
) -> DeltaResult<CommitResult> {
    // 1. Start the transaction.
    let table_schema = snapshot.schema();
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine)?;

    // 2. Build one reusable mapping from input columns or parsed defaults.
    let defaults = txn.top_level_column_defaults()?;
    let mut column_sources = Vec::new();
    for field in table_schema.fields() {
        let source = match input_schema.index_of(field.name()) {
            Ok(index) => ColumnSource::Input(index),
            Err(_) => {
                let column_default = defaults.get(field.name()).ok_or_else(|| {
                    Error::generic(format!("missing column without a default: {}", field.name()))
                })?;
                let scalar = column_default.to_scalar()?.ok_or_else(|| {
                    Error::generic(format!("cannot evaluate default for {}", field.name()))
                })?;
                ColumnSource::Default(scalar)
            }
        };
        column_sources.push(source);
    }

    // 3. Acknowledge responsibility for defaults and prepare the write context.
    txn.ack_column_defaults();
    let write_state = txn.write_state()?;
    let write_context = write_state.write_context_builder().build()?;
    let output_schema = Arc::new(write_context.logical_data_schema().as_ref().try_into_arrow()?);

    // 4. Fill omitted columns and write each incoming batch without collecting them.
    for batch in batches {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let columns = column_sources
            .iter()
            .map(|source| match source {
                ColumnSource::Input(index) => Ok(Arc::clone(batch.column(*index))),
                ColumnSource::Default(scalar) => scalar.to_array(batch.num_rows()),
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        let output_batch = RecordBatch::try_new(Arc::clone(&output_schema), columns)?;
        let data = ArrowEngineData::new(output_batch);
        let file_metadata = engine.write_parquet(&data, &write_context).await?;
        txn.add_files(file_metadata);
    }

    // 5. Commit every file written by this transaction.
    txn.commit(engine)
}
```

For the `people` table above, batches containing only `age` and `name` gain a `city` column filled
with `Seattle`. Batches that also supply `city` keep that column unchanged. The function resolves
only the defaults needed by the input schema and reuses those scalars across batches of any size.
It writes one batch at a time without collecting the entire input. For small incoming batches,
consider coalescing them before calling this function to avoid writing many small files.

Handle the returned `CommitResult` as described in [Appending data](./append.md#committing).

`Scalar::to_array(row_count)` broadcasts one value over a whole column. For mixed input containing
supplied values and `DEFAULT` requests, replace only the requested cells in your connector. Don't
use a null bitmap as a default-request bitmap: explicit nulls must stay null.

Interpret the discovery and parsing results separately:

| Result | Meaning |
|--------|---------|
| No map entry for a top-level column | No active default is exposed for that column |
| `to_scalar()` returns `Ok(Some(scalar))` | Kernel parsed the default into a typed value |
| `to_scalar()` returns `Ok(Some(Scalar::Null(data_type)))` | The declared default is SQL `NULL` |
| `to_scalar()` returns `Ok(None)` | Unsupported SQL or an incompatible value |
| `to_scalar()` returns `Err(error)` | Propagate or handle the error without substituting a value |

If Kernel can't parse a required default, use your own evaluator or reject the write. Parse failure
doesn't prove the stored SQL is invalid: Kernel supports a subset of valid SQL.

#### When your connector evaluates the required defaults

Use `raw_sql()` and `data_type()` when your connector already evaluates SQL or needs an expression
outside Kernel's supported subset. For example, `concat('Sea', 'ttle')` requires your evaluator even
though the result is a string. You don't need to call `to_scalar()` first.

This connector helper accepts your evaluator as a callback. The callback must evaluate the stored
SQL with the appropriate SQL semantics and return a value of the declared type:

```rust
# extern crate delta_kernel;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{ColumnDefault, DataType};
use delta_kernel::{DeltaResult, Error};

fn resolve_default(
    column_default: &ColumnDefault<'_>,
    evaluate_sql: impl FnOnce(&str, &DataType) -> DeltaResult<Scalar>,
) -> DeltaResult<Scalar> {
    let scalar = evaluate_sql(column_default.raw_sql(), column_default.data_type())?;
    if &scalar.data_type() != column_default.data_type() {
        return Err(Error::generic("default evaluator returned the wrong type"));
    }
    Ok(scalar)
}
```

In the earlier example, replace the `to_scalar()` call and its unsupported-expression check with
`resolve_default(column_default, your_evaluator)?`. This evaluates each required default while
preparing the column mapping, then reuses the resulting scalar across batches. `your_evaluator`
is your connector's implementation, not a Kernel API.

For expressions involving time, randomness, or other context, your evaluator must also choose the
correct evaluation scope according to its SQL semantics. Don't broadcast a single result when the
expression requires evaluation per row. You can materialize data directly in your compute engine
instead of converting through Kernel scalars or Arrow arrays.

This path doesn't depend on Kernel's parsing result. Constructing a `ColumnDefault` still attempts
parsing internally, and table metadata validation still applies.

### Supported literals and parsing limits

Kernel parses literals against the column's declared type. Leading and trailing whitespace is
ignored, and literal keywords are case-insensitive.

| Column type | Supported forms and limits |
|-------------|----------------------------|
| Any type | Bare `NULL`, producing a typed null |
| Integer types | Unquoted signed integers within the target type's range |
| FLOAT, DOUBLE | Finite numbers, including exponent notation such as `1.5e2` |
| DECIMAL | Must match declared precision and scale. Scale 2 accepts `1.20`, not `1.2` |
| BOOLEAN | Bare `TRUE` or `FALSE` |
| STRING | Single-quoted strings such as `'Seattle'`, `''`, or `'it''s'` |
| BINARY | Hex literals with an even number of digits, such as `X'DEAD'` or `X''` |
| DATE | `'2026-01-01'` or `DATE '2026-01-01'` |
| TIMESTAMP | `'2026-01-01T12:00:00Z'`, optionally prefixed by `TIMESTAMP` or `TIMESTAMP_LTZ` |
| TIMESTAMP_NTZ | `'2026-01-01 12:00:00'`, optionally prefixed by `TIMESTAMP_NTZ` |
| ARRAY, MAP, STRUCT | Only `NULL`. Other expressions remain available as raw SQL |
| VARIANT | Only `NULL`. A non-null default is rejected when loading the table |

Parser support doesn't enable otherwise unsupported data writes. In particular, parsing a value
doesn't bypass the table's feature, schema, or write validation.

Kernel doesn't parse function calls such as `current_timestamp()`, arithmetic such as `1 + 1`,
casts, or numeric suffixes such as `1L`, `1.5F`, and `1.23BD`. Strings containing backslashes or
using double quotes also aren't supported. A quoted number such as `'42'` isn't parsed as
an integer.

TIMESTAMP literals require an explicit uppercase UTC `Z` suffix. Zoneless timestamps and numeric
offsets, including `+00:00`, aren't supported by the default parser. If you use a `T` separator, it
must be uppercase. TIMESTAMP_NTZ uses a space-separated date and time without a zone. Both support
fractional seconds. Floating-point parsing rejects non-finite results and non-exponent literals
whose implied decimal precision exceeds 38.

## Limitations and common questions

### Why does `write_state()` require acknowledgement?

Kernel needs your connector to take responsibility for applying defaults before writing. Calling
`top_level_column_defaults()` or `to_scalar()` doesn't acknowledge that responsibility.
Call `ack_column_defaults()` for each new transaction after you've resolved the defaults your write
needs, including when every column is explicitly supplied. The call doesn't inspect your batches.
If the feature is enabled but no defaults are declared anywhere in the schema, acknowledgement
isn't required.

### Can I create, change, or remove defaults through Kernel?

Kernel's public create-table API can't enable column defaults. `ALTER TABLE` and transaction schema
changes are also rejected on tables with `allowColumnDefaults` enabled, even if the requested change
doesn't modify a default. Use another supporting Delta writer for these operations.

### What about nested defaults?

`top_level_column_defaults()` exposes only top-level columns. Kernel retains and validates defaults
on nested fields, and `StructField::column_default()` lets you inspect a field's metadata.
That field-level API doesn't check whether the table enables `allowColumnDefaults`.

A table with only nested defaults can return an empty top-level map and still require
acknowledgement. Don't treat that empty map as proof that the table has no defaults. You must
explicitly handle omitted nested fields or reject writes that need unsupported nested defaults.

### How do partitioning and column mapping affect defaults?

Discovery returns logical column names, including partition column names. Resolve partition defaults
before grouping rows by partition, then supply every partition column's typed value through
`write_state.write_context_builder().with_partition_values(...)`. Defaults don't make partition-map
entries optional.

With column mapping, continue using logical names for discovery and input data. Kernel's write
context handles the mapping to physical names.

### Why can a table load even when Kernel can't parse its defaults?

Unsupported SQL can be evaluated by your connector, so parse failure alone doesn't prevent loading
a Snapshot. Loading does reject non-string `CURRENT_DEFAULT` metadata and non-null Variant defaults,
including on nested fields. These checks also apply when the feature isn't enabled.

Valid metadata without `allowColumnDefaults` is tolerated but inactive: transaction discovery
returns an empty map, and the metadata alone doesn't require acknowledgement.

### What does an IcebergCompatV3 warning mean?

IcebergCompatV3 requires literal defaults. During write validation, Kernel logs a warning when it
can't verify that a default is a literal. A valid literal outside Kernel's parser subset can trigger
the same warning as a non-literal expression. Your connector must ensure compliance. Successful
transaction creation or custom evaluation doesn't prove the expression meets this restriction.

## What's next

- [Appending data](./append.md) covers writing files and committing your prepared data.
- [Writing to partitioned tables](./partitioned_writes.md) covers binding resolved partition values.

[default-columns]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#default-columns
