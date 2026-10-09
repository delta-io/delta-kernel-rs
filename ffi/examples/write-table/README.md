write-table
===========

C FFI example for the write transaction surface. Demonstrates `new_update_table_txn_builder`,
`update_table_txn_builder_with_engine_info`, `update_table_txn_get_unpartitioned_write_context`, the four write-context accessors
(`get_write_schema`, `get_physical_write_schema`, `get_logical_to_physical`, `get_write_path`),
`update_table_txn_builder_with_data_change`, and `update_table_txn_commit` against an existing
table.

# Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
$ ./write_table /path/to/existing/table
```

# Limitations

This example commits an empty transaction. Staging files requires an Arrow batch matching
`Transaction::add_files_schema`, which this example doesn't construct.
