read-table-changes
==================

C FFI example for the Change Data Feed (CDF) surface.

Exercises the full `table_changes_*` and `scan_table_changes_*` flow:

- `table_changes_from_version` / `table_changes_between_versions`
- `table_changes_{schema,table_root,start_version,end_version}`
- `table_changes_scan` + `table_changes_scan_execute`
- `scan_table_changes_next` iteration -> `free_arrow_ffi_data`

For each returned Arrow batch the example reads the row count directly off the
`FFI_ArrowArray` and prints a per-batch summary, then releases the batch via
`free_arrow_ffi_data`. A real engine would instead hand the inner
`FFI_ArrowArray` + `FFI_ArrowSchema` to its own arrow layer (e.g. arrow-glib's
`garrow_record_batch_import`, mirroring `read-table/arrow.c`) before freeing.

# Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
$ ./read_table_changes [-s start_version] [-e end_version] /path/to/table
```
