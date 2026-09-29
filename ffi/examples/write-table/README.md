write-table
===========

C FFI example for the write transaction surface. Demonstrates `get_update_table_txn_builder`,
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

This example currently commits **empty** transactions. Staging new parquet files requires
building an Arrow batch that matches `Transaction::add_files_schema` (`path`,
`partitionValues`, `size`, `modificationTime`, `stats`) and handing it to
`update_table_txn_add_files` via
`get_engine_data`. Constructing that batch from C needs arrow-glib (or a similar C-level
Arrow builder). A shared `ffi/examples/common/` arrow-glib writer helper is planned as a
follow-up; once it lands, this example should grow an `update_table_txn_add_files` flow alongside a
`update_table_txn_with_domain_metadata` / `with_domain_metadata_removed` demo (the `domainMetadata` writer
feature can be enabled today via the existing `create_table_txn_builder_with_table_property`
API by setting `delta.feature.domainMetadata=supported`).
