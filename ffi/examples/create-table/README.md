create-table
============

C FFI example for CREATE TABLE. Demonstrates the schema-visitor handoff
(`KernelSchemaVisitorState` + `visit_field_*`) used by `new_create_table_txn_builder`, the
consuming-and-returning handle pattern of `create_table_txn_builder_with_table_property` /
`create_table_txn_builder_build`, and the final `create_table_txn_commit`.

# Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
$ ./create_table /path/to/new/table
```

# Notes

- The example does not stage any initial data files (`create_table_txn_add_files` is
  omitted). Initial-data staging requires building an Arrow batch that matches
  `Transaction::add_files_schema`, which needs arrow-glib (or a similar C-level Arrow
  builder) on the C side. A shared writer helper is planned as a follow-up.
- The table is created with `delta.enableChangeDataFeed=true` to exercise
  `create_table_txn_builder_with_table_property`.
