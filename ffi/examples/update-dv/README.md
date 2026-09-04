update-dv
=========

C FFI example for connector-authored deletion vector updates. The example assumes the
connector has already written a DV file (or built inline DV bytes) and knows the descriptor
fields to install in the Delta log.

It demonstrates:

- `dv_descriptor_map_new`
- `dv_descriptor_new`
- `dv_descriptor_map_insert`
- `scan_metadata_iter_init`
- `transaction_update_deletion_vectors`
- `commit`

# Building

```bash
# from repo root
$ cargo build -p delta_kernel_ffi
# from this directory
$ mkdir build && cd build && cmake .. && make
```

# Running

```bash
$ ./update_dv /path/to/table data-file.parquet p file:///tmp/table/dv.bin 1 36 2
```

Arguments:

1. Table path
2. Data file path exactly as it appears in scan metadata / the Add action
3. Storage type: `u` (persisted relative), `i` (inline), or `p` (persisted absolute)
4. `pathOrInlineDv`
5. Offset, or `-` to omit it
6. `sizeInBytes`
7. Cardinality

The table must have the `deletionVectors` reader/writer feature and
`delta.enableDeletionVectors=true`. The example does not write the DV file itself; it only
installs the descriptor and lets the kernel stage the matching remove/add action pair.
