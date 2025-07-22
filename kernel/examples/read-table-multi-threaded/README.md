Read Table Multi-Threaded
=========================

# About
This example shows a program that reads a table using multiple threads. This shows the use of the
`scan_metadata`, `global_scan_state`, and `visit_scan_files` methods, that can be used to partition work
to either multiple threads, or workers (in the case of a distributed engine).

You can run this from the same directory as this `README.md` by running `cargo run -- [args]`.

We use a single-producer-multi-consumer channel to send each file and its metadata that needs to be
read out to a pool of threads. The data is sent as a [`ScanFile`], a struct we define that holds all
the metadata needed to read a file. Each thread reads from the channel, and then processes any files
it receives. The results are sent back as Arrow `RecordBatch`s on a mutli-producer-single-consumer
channel.

Once the main thread has sent all the files out, we close the `ScanFile` sender, which means that
once the last `ScanFile` has been received by a thread, subsequent `recv` calls in any thread will
start to return errors. The threads take this as a signal to shut down.

We also ensure that _only_ the threads have copies of the `Sender`s used to send the `RecordBatch`s,
by closing the copy that the main thread has once all the threads have been created. This means that
we can simply loop over our `RecordBatch` receiver, because it will return results until the last
thread has exited (which closes that last sender).

# Examples

Assuming you're running in the directory this README is in:

- Read and print the table in `kernel/tests/data/table-with-dv-small/`:

`cargo run -- ../../../kernel/tests/data/table-with-dv-small/`

- Get usage info:

`cargo run -- --help`

- Use the sync engine to read `kernel/tests/data/basic_partitioned/`

`cargo run -- -e sync ../../../kernel/tests/data/basic_partitioned/`

- Read some giant table using 100 threads:

`cargo run -- -t 100 /path/to/my/giant/table`

## selecting specific columns

To select specific columns you need a `--` after the column list specification.

- Read `letter` and `data` columns from the `multi_partitioned` dat table:

`cargo run -- --columns letter,data -- ../../../acceptance/tests/dat/out/reader_tests/generated/multi_partitioned/delta/`