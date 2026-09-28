Read Table Single-Threaded
=========================

# About
This example shows a program that reads a table using a single thread. It uses the "all-in-one"
`Scan::execute` method, which simplifies reading when one process executes every candidate file.

`Scan::execute` returns logical table batches after applying deletion vectors and row transforms.
The data is in Arrow format because this example uses the default engine, so the example converts
the opaque engine data into Arrow record batches before printing it. Connectors using the
experimental `internal-api` feature can call `Scan::execute_with_file_filter` to select whole data
files before deletion-vector and data-file Parquet I/O; checkpoint replay still occurs.

You can run this example from anywhere in this repository by running `cargo run -p read-table-single-threaded -- [args]` or by navigating to this directory and running `cargo run -- [args]`.

# Examples

Assuming you're running in the directory of this example:

- Read and print the table in `kernel/tests/data/table-with-dv-small/`:

`cargo run -- ../../../kernel/tests/data/table-with-dv-small/`

- Get usage info:

`cargo run -- --help`

## selecting specific columns

To select specific columns you need a `--` after the column list specification.

- Read `letter` and `data` columns from the `multi_partitioned` dat table:

`cargo run -- --columns letter,data -- ../../../acceptance/tests/dat/out/reader_tests/generated/multi_partitioned/delta/`
