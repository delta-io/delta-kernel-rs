# delta-kernel-rs ffi

This crate provides a C foreign function interface (ffi) for delta-kernel-rs.

## Building an update transaction

Connectors configure transaction intent on a builder and use the built transaction for
write-dependent information and staged actions. Every `update_table_txn_builder_with_*` function
consumes its builder handle and returns a replacement, so callers must overwrite the old handle.

```c
HandleSharedSnapshot snapshot = /* unwrap(snapshot_builder_build(...)) */;
HandleExclusiveUpdateTableTransactionBuilder builder = new_update_table_txn_builder(snapshot);
builder = unwrap(update_table_txn_builder_with_engine_info(
    builder, slice("my-engine/1.0"), engine));
builder = update_table_txn_builder_with_operation(
    builder, KernelUpdateTableOperationWrite);
builder = update_table_txn_builder_with_data_change(builder, true);

HandleExclusiveUpdateTableTransaction txn = unwrap(update_table_txn_builder_build(builder, engine));
/* Write files, then call update_table_txn_add_files(txn, metadata). */
txn = unwrap(update_table_txn_with_operation_metrics(txn, &metrics, engine));
HandleExclusiveCommittedTransaction committed = unwrap(update_table_txn_commit(txn, engine));
uint64_t version = committed_transaction_version(&committed);
free_committed_transaction(committed);
```

Here `slice` and `unwrap` stand for the connector's usual `KernelStringSlice` and `ExternResult`
helpers.

Create-table connectors use the same shape: call `new_create_table_txn_builder`, apply
`create_table_txn_builder_with_*` functions, then call `create_table_txn_builder_build`. Application
transaction identifiers are available on both builders; metadata removals are update-only. Nested
schema changes use `update_table_txn_builder_add_column_at` and
`update_table_txn_builder_set_nullable` with segmented `FfiColumnName` paths. A snapshot returned
by `committed_transaction_post_commit_snapshot` is independently owned and must be released with
`free_snapshot`.

## Building

### Building Kernel and Headers
You can build static and shared-libraries, as well as the include headers by running:

```sh
cargo build [--release]
```

For additional features like tracing support, use:

```sh
cargo build [--release] --features tracing
```

This will place libraries in the root `target` dir (`../target/[debug,release]` from the directory containing this README), and headers in `../target/ffi-headers`. In that directory there will be a `delta_kernel_ffi.h` file, which is the C header, and a `delta_kernel_ffi.hpp` which is the C++ header.

## Examples

This crate provides two main examples demonstrating different aspects of the FFI:

### 1. Read Table Example (`examples/read-table`)

This example shows how to read data from a Delta table using the FFI. It demonstrates:
- Opening and reading Delta tables
- Schema inspection
- Data retrieval with optional Arrow integration

To build and run this example (after building the ffi as above):

```sh
cd examples/read-table
mkdir build
cd build
cmake ..
make
./read_table ../../../../kernel/tests/data/table-with-dv-small
```

Note there are two configurations that can currently be configured in cmake:
```bash
# turn on VERBOSE mode (default is off) - print more diagnostics
$ cmake -DVERBOSE=yes ..
# turn off PRINT_DATA (default is on) - see below
$ cmake -DPRINT_DATA=no ..
```

By default this has a dependency on
[`arrow-glib`](https://github.com/apache/arrow/blob/main/c_glib/README.md). You can read install
instructions for your platform [here](https://arrow.apache.org/install/).

If you don't want to install `arrow-glib` you can run the above `cmake` command as:

```sh
cmake -DPRINT_DATA=no ..
```

and the example will only print out the schema of the table, not the data.

### 2. Visit Expression Example (`examples/visit-expression`)

This example demonstrates how to work with Delta expressions through the FFI:
- Expression parsing and traversal
- Expression visitor pattern implementation
- Testing expression functionality

To build and run this example:

```sh
cd examples/visit-expression
mkdir build
cd build
cmake ..
make
./visit_expression
```

## Testing

The examples include comprehensive testing capabilities:

### Running Tests

After building an example, you can run the associated tests:

```sh
# For read-table example
cd examples/read-table/build
make test

# For visit-expression example
cd examples/visit-expression/build
make test
```

### Test Scripts

The examples use test scripts located in the `tests/` directory:
- `tests/read-table-testing/run_test.sh` - Tests table reading functionality
- `tests/test-expression-visitor/run_test.sh` - Tests expression visitor functionality

These scripts validate the output against expected results and provide detailed diagnostics.

## C/C++ Extension (VSCode)

By default the VSCode C/C++ Extension does not use any defines flags. You can open `settings.json` and set the following line:
```
    "C_Cpp.default.defines": [
        "DEFINE_DEFAULT_ENGINE_BASE",
        "DEFINE_SYNC_ENGINE"
    ]
```
