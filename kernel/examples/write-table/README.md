Write Table
===========

# About

This example shows how to write a Delta table using the default engine by:
- Creating a schema defined by the command line arguments
- Generating random Apache Arrow data for the table
- Committing the transaction

Note: As of July 2025, the Rust kernel does not officially expose APIs for creating tables. This example uses unofficial, internal APIs to create the table.

Additional details about the example:
- A default schema (`id:integer,name:string,score:double`) will be used in the case that the schema is not specified in the command like arguments
- Table contents will be printed in the command line after successfully creating tables

You can run this example from anywhere in this repository by running `cargo run -p write-table -- [args]` or by navigating to this directory and running `cargo run -- [args]`.

# Examples

Assuming you're running in the directory of this example:

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

## Schema Specification

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
