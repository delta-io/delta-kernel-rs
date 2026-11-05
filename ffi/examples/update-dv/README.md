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

The FFI exposes Rust's separate compressed-vector and serializer lifecycles:

```text
dv = new_deletion_vector()
deletion_vector_add_indexes(&dv, previous_deleted_rows, &error_allocators)
deletion_vector_add_indexes(&dv, new_deleted_rows_chunk, &error_allocators)
deletion_vector_add_indexes(&dv, another_chunk, &error_allocators)
writer = new_dv_writer(output, &error_allocators)
result = dv_writer_write(&writer, dv, &error_allocators) // consumes dv, also on error
// Write additional completed vectors into this output, or create separate writers.
dv_writer_finalize(writer, &error_allocators)
```

Create one writer per desired output file. Repeated writes put independent vectors
in a shared output, each with its own offset. Create separate writers for separate
outputs. Each write returns `FfiDvWriteResult` with `offset`, `size_in_bytes`, and
`cardinality`; it does not create a descriptor or choose a path.

Add prior deletions and new deletions in chunks with `deletion_vector_add_indexes`.
Each chunk is copied into the compressed bitmap, so the input array can be released
after the call; the final write requires no complete uncompressed index array.
Before writing, the vector must contain **all** deletions, not just the newly deleted
indexes. Indexes are zero-based physical row ordinals across the
whole data file, must not exceed `i64::MAX`, and must be below that file's physical
row count. Adding indexes validates the protocol limit before changing the bitmap;
the connector validates the file-specific bound. Unsorted indexes and duplicates
across chunks are deduplicated. An empty chunk, including `{NULL, 0}`, is a no-op.
Writing a newly created vector with no additions serializes an empty DV.
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

The serializer requires no Engine. Each fallible entry point borrows a non-null
`ErrorAllocators` table for that call, using its callbacks and context to allocate
connector-owned errors. The table and its callback state must satisfy the standard
FFI error-allocation contract; the serializer does not retain them. The host retains
ownership of the output and context.
Callbacks must not unwind or re-enter the same writer. If the handle moves between
threads, the output must support those sequential calls.

The output-record argument to `new_dv_writer` and the borrowed handle arguments to
`deletion_vector_add_indexes` and `dv_writer_write` are Rust references. C and other
FFI callers must supply valid, non-null, aligned pointers, with exclusive access for
mutable handle arguments. Invalid reference arguments are undefined behavior, not
recoverable FFI errors. Null stateless callback contexts and empty index slices
with `{NULL, 0}` remain valid.

`dv_writer_finalize` consumes the handle on success or error. `free_dv_writer`
abandons it without flushing. `dv_writer_write` consumes the vector on success and
error for valid arguments, including when the writer has already failed.
Use `free_deletion_vector` to release a vector abandoned before writing.
Invalid chunks leave the vector unchanged; serialization or output failures make
the writer unusable. Abandon failed output
rather than retrying into a potentially partially written stream. The serializer
does not upload, publish, or clean up that output.

## Connector responsibilities

Snapshot lookup, file/row validation, existing-DV reads and merging, path generation,
and upload remain outside serialization. For example, add `{1, 3}` from an existing
DV and `{7}` as a subsequent chunk to obtain the complete vector `{1, 3, 7}`.

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
