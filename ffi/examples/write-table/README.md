write-table
===========

C FFI example for the write transaction surface. Demonstrates `transaction`,
`with_engine_info`, `get_unpartitioned_write_context`, `get_write_schema`, `get_write_path`,
`set_data_change`, and `commit` against an existing table.

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
`partitionValues`, `size`, `modificationTime`, `stats`) and handing it to `add_files` via
`get_engine_data`. Constructing that batch from C needs arrow-glib (or a similar C-level
Arrow builder). A shared `ffi/examples/common/` arrow-glib writer helper is planned as a
follow-up; once it lands, this example should grow an `add_files` flow alongside a
`with_domain_metadata` / `with_domain_metadata_removed` demo (the `domainMetadata` writer
feature can be enabled today via the existing `create_table_builder_with_table_property`
API by setting `delta.feature.domainMetadata=supported`).
