# Column Defaults

A column default is a SQL expression stored in a field's column metadata within the table schema.
It supplies a value when a write omits that column or provides the `DEFAULT` keyword for it. A
value explicitly supplied by the write, including `NULL`, is not replaced by the default.

Column defaults are a writer feature because the writer materializes the evaluated value into
each new data file. A writer must apply the current default whenever its input omits a column that
declares one or requests `DEFAULT` for that column. Readers then see ordinary stored values and do
not need special default evaluation logic.

Before reading the writing sections below, make sure you understand
[Appending Data](./append.md).

## Accessing column defaults

| API | When to use it |
|-----|----------------|
| `Transaction::top_level_column_defaults()` | Discover defaults while preparing an ordinary table write. Names are logical names. |
| `StructField::column_default()` | Inspect one field directly or traverse a schema that may contain nested defaults. |
| `ColumnDefault::raw_sql()` | Surface the stored metadata or obtain the original SQL for connector-side evaluation. |
| `ColumnDefault::to_scalar()` | Ask Kernel to parse a supported literal into a typed `Scalar` for materialization. |
| `ColumnDefault::data_type()` | Obtain the declared result type for validation or materialization. |

If your connector only needs to surface column-default metadata, use `raw_sql()`. It returns the
original SQL expression stored in the schema without requiring Kernel to understand it. Do not
use `to_scalar()` for metadata display: that method is intended for writers that want Kernel to
parse a literal for materialization, and it returns `None` when Kernel cannot parse the SQL.

Prefer the transaction method for normal writes. It checks that `allowColumnDefaults` is enabled
and gives you the top-level logical names expected by the write path. Use the field method only
when you intentionally need to inspect a specific field or recurse through a nested schema.

## Protocol requirements

The Delta protocol describes the behavior in
[Default Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#default-columns).
The feature has these protocol requirements:

- The table lists `allowColumnDefaults` in its writer features. See the
  [table-feature registry](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).
- Each declared expression is stored as `CURRENT_DEFAULT` in the field's
  [column metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-metadata).
- Writers must materialize the current default when the input row omits the column or uses the
  `DEFAULT` keyword.

> [!IMPORTANT]
> Kernel currently supports writing to tables that already declare column defaults. The
> `create_table()` builder cannot enable `allowColumnDefaults`, and `alter_table()` cannot add or
> change a default. Create or alter the table with another Delta client before using Kernel to
> write data to it.

## Discovering defaults for a write

Use `Transaction::top_level_column_defaults()` as the main entry point when preparing a write.
It returns a map from logical column name to `ColumnDefault` for every top-level column that
declares a default.

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::transaction::Transaction;
# use delta_kernel::DeltaResult;
# fn inspect_defaults(transaction: &Transaction) -> DeltaResult<()> {
for (column_name, default) in transaction.top_level_column_defaults()? {
    match default.to_scalar()? {
        Some(value) => {
            println!("Materialize {column_name} from the literal {value:?}");
        }
        None => {
            println!(
                "Evaluate {:?} as {:?} for {column_name}",
                default.raw_sql(),
                default.data_type(),
            );
        }
    }
}
# Ok(())
# }
```

`to_scalar()` returns `Some(Scalar)` when Kernel can parse the default SQL as a supported literal.
It returns `None` only when Kernel cannot parse that SQL; `None` does not mean the stored
expression is invalid. Use `raw_sql()` to access the original SQL, then evaluate it with your
connector's SQL engine using `data_type()` as the expected result type.

An empty map means either no top-level column has a default or the table does not enable
`allowColumnDefaults`. Kernel does not surface orphaned `CURRENT_DEFAULT` metadata when the
feature is disabled.

## Materializing defaults

Discovering a default does not change the input data. For each omitted column or column marked
with `DEFAULT`, your connector must produce an array containing the evaluated default and insert
it into the logical data before the normal write transformation.

Prepare data in this order:

1. Determine which fields in the table's logical schema are omitted or marked with `DEFAULT` in
   the input.
2. Look up those logical names in `top_level_column_defaults()`.
3. Evaluate each matching default once per write when the expression is constant, or once per row
   when the expression's semantics require it.
4. Materialize an array with the table field's data type and the input batch's row count.
5. Insert the arrays into the logical data in table-schema order.
6. Apply `WriteContext::logical_to_physical()` and write the physical data as described in
   [Appending Data](./append.md#writing-parquet-files).

If an omitted column has no default, apply the same missing-column behavior your connector uses
for other writes. For example, a connector may materialize `NULL` for a nullable field or reject a
missing required field. Column defaults do not override an explicitly supplied value.

## Parser and type limitations

Kernel's SQL parser is intentionally limited. It supports `NULL` and supported literal forms for
primitive types. It does not currently evaluate general SQL expressions such as arithmetic or
function calls. For example, `current_timestamp()` remains available through `raw_sql()`, but
`to_scalar()` returns `None`.

The following type-specific rules also apply:

- A Variant column may only have a `NULL` default. Kernel rejects a non-`NULL` Variant default
  when it loads the table schema.
- Non-`NULL` defaults for Array, Map, and Struct columns are valid table metadata, but Kernel
  cannot materialize them.
- `NULL` can be parsed for any column type.

Treat `to_scalar() == None` as a request for connector-side evaluation, not as evidence that the
stored expression is invalid. Kernel may simply lack support for that SQL form.

## Nested defaults

The Delta protocol permits defaults on nested struct fields. Kernel validates nested defaults
when it loads a snapshot, but `top_level_column_defaults()` intentionally returns only top-level
columns.

If your connector supports omitting nested fields, recursively walk `Snapshot::schema()` and call
`StructField::column_default()` on each field. Container elements do not carry field metadata, so
continue into struct fields inside arrays and maps when traversing the schema.

Kernel does not provide a helper that materializes nested defaults into your input data. Your
connector must rebuild the affected struct values while preserving the logical schema.

## Iceberg compatibility v3

Tables using `icebergCompatV3` may also use `allowColumnDefaults`, but Iceberg compatibility v3
requires every default to be a literal.

Kernel checks each default when it constructs a write transaction. If Kernel can parse the
expression as a literal, the check succeeds. If its limited parser cannot verify the expression,
Kernel logs a warning and allows the transaction to proceed. This can mean either that the SQL is
a literal Kernel does not understand or that it is a non-literal expression that violates the
Iceberg compatibility requirement.

When you receive `None` from `to_scalar()` on an `icebergCompatV3` table, your connector should
parse the raw SQL and verify that it is a literal before writing. Do not evaluate and accept an
arbitrary function or computed expression merely because Kernel allowed the transaction to be
constructed.

Iceberg compatibility v3 also enables column mapping. The defaults returned by
`top_level_column_defaults()` are still keyed by logical column name. Materialize them into the
logical data before the write context applies column mapping and other physical-schema
transformations.

## Current limitations

Keep these boundaries in mind when adding column-default support to a connector:

- Kernel discovers and parses defaults but never modifies your input data.
- Kernel cannot currently define, change, or remove defaults.
- The transaction API exposes only top-level defaults.
- Nested materialization is connector-owned.
- General SQL evaluation is connector-owned.
- Iceberg compatibility v3 expressions that Kernel cannot verify require connector validation.

These limitations make `top_level_column_defaults()` a discovery API rather than a complete
default-application pipeline. Your connector remains responsible for producing logical data that
matches the table schema before writing Parquet files.

## What's next

- [Writing to Partitioned Tables](./partitioned_writes.md) explains how logical data and
  partition values become physical files.
- [Altering a Table](./alter_table.md) covers the schema changes Kernel currently supports.
