# Column defaults

A **column default** is a SQL expression stored in a field's column metadata within the table
schema. It supplies a value when a write omits that column or provides the `DEFAULT` keyword for
it. A value explicitly supplied by the write, including `NULL`, is not replaced by the default.

Writers materialize evaluated defaults in new data files, so readers need no default-evaluation
logic.

Before reading the writing sections below, make sure you understand
[Appending Data](./append.md).

## Discovering defaults for a write

Before assembling logical data, call `Transaction::top_level_column_defaults()`. It returns a map
from logical column name to `ColumnDefault` for every top-level column that declares one. The map
is empty when the table does not enable `allowColumnDefaults`, so orphaned default metadata is not
applied during writes.

Writers must materialize each returned default when an input row omits the column or uses the
`DEFAULT` keyword. See the Delta protocol's
[Default Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#default-columns)
section for wire-level rules.

> [!NOTE]
> Kernel can write to tables that already declare column defaults. The `create_table()` builder
> cannot enable `allowColumnDefaults`, and `alter_table()` rejects every alteration on a table
> with the feature enabled. Use another Delta client to create or alter such a table.

The following example assumes `age INTEGER DEFAULT 18` is the table's only top-level default.

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

The function prints:

```text
Materialize age from the literal Integer(18)
```

`to_scalar()` returns `Some(Scalar)` when Kernel can parse the default SQL as a supported literal.
It returns `None` only when Kernel cannot parse that SQL; `None` does not mean the stored
expression is invalid. Use `raw_sql()` to access the original SQL, then evaluate it with your
connector's SQL engine using `data_type()` as the expected result type.

If your connector only needs to display column-default metadata, use `raw_sql()` directly rather
than asking Kernel to parse it with `to_scalar()`.

## Materializing defaults

`DEFAULT` is a connector-planning marker, not an `EngineData` value. Resolve omitted columns and
`DEFAULT` before constructing logical `EngineData`, and preserve an explicitly supplied `NULL`.
Discovering a default does not modify the input data.

Prepare data in this order:

1. Determine which fields in the table's logical schema are omitted or marked with `DEFAULT` in
   the input.
2. Look up those logical names in `top_level_column_defaults()`.
3. Evaluate each matching default once per write when the expression is constant, or once per row
   when the expression's semantics require it.
4. For each defaulted partition column, convert the result to a `Scalar` and combine it with the
   explicitly supplied partition values. If the values vary by row, group rows by the complete
   partition-value map.
5. For a partitioned table, call `partitioned_write_context()` with the complete map for each row
   group. For an unpartitioned table, call `unpartitioned_write_context()` once.
6. For each defaulted non-partition column, materialize an array with the current batch's row
   count. Insert these arrays in `write_context.logical_schema()` order; that schema excludes
   partition columns.
7. Apply `WriteContext::logical_to_physical()` and write the physical data as described in
   [Appending Data](./append.md#writing-parquet-files).

If an omitted column has no default, apply the same missing-column behavior your connector uses
for other writes. For example, a connector may materialize `NULL` for a nullable field or reject a
missing required field. Column defaults do not override an explicitly supplied value.

## Parser and type limitations

Kernel's SQL parser supports `NULL` and supported literal forms for primitive types. It does not
evaluate general SQL expressions such as arithmetic or function calls. For example,
`current_timestamp()` remains available through `raw_sql()`, but `to_scalar()` returns `None`.

The following type-specific rules also apply:

- A Variant column may only have a `NULL` default. Kernel rejects a non-`NULL` Variant default
  when it loads the table schema.
- Non-`NULL` defaults for Array, Map, and Struct columns are valid table metadata, but Kernel
  cannot materialize them.
- `NULL` can be parsed for any column type.

## Nested defaults

The Delta protocol permits defaults on nested struct fields, and Kernel validates nested defaults
when it loads a snapshot. However, `top_level_column_defaults()` intentionally returns only
top-level columns.

`StructField::column_default()` exposes raw field metadata and does not check whether the table
enables `allowColumnDefaults`. Do not use it to drive a write unless your connector independently
verifies the feature. If your connector cannot verify the feature, reject writes that omit nested
fields instead of applying values from raw metadata.

After verifying the feature, recursively walk `Snapshot::schema()` and call
`StructField::column_default()` on each field. Container elements do not carry field metadata, so
continue into struct fields inside arrays and maps. Kernel does not materialize nested defaults;
your connector must rebuild affected struct values while preserving the logical schema.

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

## What's next

- [Writing to Partitioned Tables](./partitioned_writes.md) explains how logical data and
  partition values become physical files.
- [Altering a Table](./alter_table.md) covers the schema changes available through Kernel.
