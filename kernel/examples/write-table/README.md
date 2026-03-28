Write Table
===========

# About

This example shows how to write a Delta table using the default engine by:
- Creating a schema defined by the command line arguments
- Writing data from a JSON file or generating random Apache Arrow data for the table
- Committing the transaction

Note: As of July 2025, the Rust kernel does not officially expose APIs for creating tables. This example uses unofficial, internal APIs to create the table.

Additional details about the example:
- A default schema (`id:integer,name:string,score:double`) will be used in the case that the schema is not specified in the command line arguments
- If `--data` is not provided, random sample data is generated based on the schema and `--num-rows`
- Table contents will be printed in the command line after successfully writing to the table

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

- Write data from a JSON file:

```bash
mkdir ./my_table
cargo run -- ./my_table --data ./data.json
```

The JSON file must be an array of row objects whose field names match the table schema:

```json
[
  {"id": 1, "name": "Alice", "score": 95.5},
  {"id": 2, "name": "Bob", "score": 82.0}
]
```

Missing fields are written as null. Extra fields not present in the schema are ignored.

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
