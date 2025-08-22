Read Table Single-Threaded
=========================

# About
This example shows a program that reads a table using a single thread. It uses the "all-in-one"
`Scan::execute` method, which simplifies reading, but does not allow for distributing the work of
reading the table.

`Scan::execute` returns batches of data along with a deletion vector mask. The data is in arrow
format, since we're using the default client, which uses arrow. We therefore
downcast into arrow, and use the arrow functions to filter out deleted rows, and then to print the
final data.

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
