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
- `update_table_txn_update_deletion_vectors`
- `update_table_txn_commit`

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

# Kernel DV serialization

The FFI exposes the lifecycle of Rust's `StreamingDeletionVectorWriter`:

```text
writer = new_dv_writer(output, allocate_error)
a = dv_writer_write(&writer, complete_deleted_rows_for_a, allocate_error)
b = dv_writer_write(&writer, complete_deleted_rows_for_b, allocate_error)
dv_writer_finalize(writer, allocate_error)
```

Create one writer per desired output file. Repeated writes put independent vectors
in a shared output, each with its own offset. Create separate writers for separate
outputs. Each write returns `FfiDvWriteResult` with `offset`, `size_in_bytes`, and
`cardinality`; it does not create a descriptor or choose a path.

Each row-index slice is the **complete** vector, including previous deletions, not
just newly deleted indexes. Indexes are zero-based physical row ordinals across the
whole data file, must not exceed `i64::MAX`, and must be below that file's physical
row count. The serializer enforces the protocol's `i64::MAX` limit; the connector
validates the file-specific bound. Unsorted indexes and duplicates are deduplicated.
An empty slice, including `{NULL, 0}`, serializes an empty vector.
Omitting earlier deletions when replacing a DV would make those rows visible again.

## Output ownership and errors

`FfiDvWriterOutput` contains a caller-owned context and write/flush callbacks.
Creation copies the record, so it may be moved, modified, or dropped after
`new_dv_writer` returns, including when a helper used a stack-local record. The
callbacks and the context they reference must remain valid until finalization or
`free_dv_writer`. The callbacks run synchronously. Write receives
borrowed bytes and returns the number accepted; partial writes are retried. Flush
must return success. Callbacks report failures with `EngineExecResult` using the
existing Rust-owned error-message convention.

The serializer requires no Engine. Each fallible entry point uses the supplied error
allocator for its FFI result. The host retains ownership of the output and context.
Callbacks must not unwind or re-enter the same writer. If the handle moves between
threads, the output must support those sequential calls.

`dv_writer_finalize` consumes the handle on success or error. `free_dv_writer`
abandons it without flushing. Invalid input is rejected without changing the
writer; serialization or output failures make it unusable. Abandon failed output
rather than retrying into a potentially partially written stream. The serializer
does not upload, publish, or clean up that output.

## Connector responsibilities

Snapshot lookup, file/row validation, existing-DV reads and merging, path generation,
and upload remain outside serialization. For example, `{1, 3}` from an existing DV
plus new deletions `{7}` must be supplied as `{1, 3, 7}`.

After finalization, use the write results and the caller-selected output paths to
build descriptors with `dv_descriptor_new`, then insert them into a descriptor map.
The serializer FFI does not generate paths: the connector chooses a location and
uploads the finalized output. For example, `dv_descriptor_new` accepts
`KernelDvStorageType::PersistedAbsolute` with an absolute output URL. A connector
choosing persisted-relative descriptors is responsible for that representation's
path encoding; no FFI path-generator entry point is provided here.
The existing update-table transaction API can stage that map together with new-file
metadata and commit once. Resolving locations, merging prior DVs, and staging updates
must use the same snapshot. On conflict, load a fresh snapshot and resolve again.

This separation leaves file grouping, upload, and transaction policy under the
connector's control while Kernel remains responsible for the binary DV format.
