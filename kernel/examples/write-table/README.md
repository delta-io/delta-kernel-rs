Write Table
===========

## About

This example shows a simple program that writes to a Delta table. 

It demonstrates how to:

0. (Optional) Create a new Delta table with a specified schema<sup>*</sup>
1. Write data to the table using a transaction
2. Commit changes to the table
3. Read back the table's contents to verify the write operation

<sup>*</sup>Creating tables is not officially supported by `kernel-rs` yet, so this example uses an unofficial API to create an empty table if there is no existing table at the specified path.

The example uses the default engine and demonstrates basic write operations including:
- Writing Arrow RecordBatches to Parquet files
- Transaction management and commits
- Basic error handling

You can run this example from the same directory as this `README.md` by running `cargo run -- [args]`.

## Examples

Assuming you're running in the directory this README is in:

- Create and write to a new table in the current directory:

```bash
mkdir ./my_table
cargo run -- ./my_table
```

- Create a table with a custom schema:

```bash
mkdir ./custom_table
cargo run -- ./custom_table --schema "id:integer,name:string,score:double"
```

- Get usage info:

```bash
cargo run -- --help
```

### Schema Specification

The `--schema` argument accepts a comma-separated list of field definitions in the format:
`field_name:data_type`

Supported data types:
- `string` - UTF-8 strings
- `integer` - 32-bit integers
- `long` - 64-bit integers  
- `double` - 64-bit floating point
- `boolean` - true/false values
- `timestamp` - timestamp with timezone

Example: `id:integer,name:string,score:double,active:boolean`
