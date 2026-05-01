# Version checksums

To speed up snapshot loading and enable table state validation, you write a
**version checksum** (CRC file) after each commit. A CRC file records a compact
summary of the table state at a given version. When a CRC file exists, Kernel
can skip expensive log replay on the next snapshot load.

Version checksums are independent of [checkpoints](./checkpointing.md).
Checkpoints compact the transaction log into Parquet. Checksums are small JSON
files that validate and accelerate snapshot construction. You may write both
after every commit.

## Writing a checksum

Call `write_checksum()` on the post-commit snapshot after every successful
commit. The method writes a small JSON CRC file into the `_delta_log/`
directory and returns a `ChecksumWriteResult` indicating whether the file was
written or already existed.

```rust,ignore
use delta_kernel::snapshot::ChecksumWriteResult;

let committed = match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(c) => c,
    _ => panic!("unexpected result"),
};

if let Some(snapshot) = committed.post_commit_snapshot() {
    let (result, new_snapshot) = snapshot.write_checksum(&engine)?;
    match result {
        ChecksumWriteResult::Written => println!("CRC file written"),
        ChecksumWriteResult::AlreadyExists => println!("CRC file already exists"),
    }
}
```

`write_checksum()` is safe to call unconditionally. If another writer already
wrote the CRC file for this version, it returns
`ChecksumWriteResult::AlreadyExists` without error. Per the Delta protocol,
writers must not overwrite existing checksum files.

## ChecksumWriteResult

The return type is `DeltaResult<(ChecksumWriteResult, SnapshotRef)>`:

| Variant | Meaning |
|---------|---------|
| `ChecksumWriteResult::Written` | The CRC file was created. The returned `SnapshotRef` includes the new CRC file in its log segment. |
| `ChecksumWriteResult::AlreadyExists` | A CRC file already exists at this version. The original snapshot is returned unchanged. |

Use the returned `SnapshotRef` for subsequent operations (checkpointing,
publishing) so that downstream code sees the CRC file.

> [!NOTE]
> `write_checksum()` currently requires a post-commit snapshot with
> pre-computed CRC information in memory. Calling it on a snapshot loaded from
> disk (without a pre-computed CRC) returns an
> `Error::ChecksumWriteUnsupported` error. In practice, this means you call
> `write_checksum()` on the snapshot returned by
> `CommittedTransaction::post_commit_snapshot()`.

## Recommended post-commit pattern

After every commit, write the checksum first, then decide whether to
checkpoint:

```rust,ignore
let committed = match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(c) => c,
    _ => panic!("unexpected result"),
};

if let Some(snapshot) = committed.post_commit_snapshot() {
    // 1. Write the version checksum
    let (_, snapshot) = snapshot.write_checksum(&engine)?;

    // 2. Check whether it is time to checkpoint
    let stats = committed.post_commit_stats();
    let interval = snapshot
        .table_properties()
        .checkpoint_interval
        .map(|n| n.get())
        .unwrap_or(10);

    if stats.commits_since_checkpoint >= interval {
        let (_result, _new_snapshot) = snapshot.checkpoint(&engine)?;
    }
}
```

This pattern keeps your connector's maintenance logic in one place: commit,
write the CRC file, then conditionally checkpoint based on the table's
configured interval.

## Reading file stats from a checksum

When Kernel loads a snapshot whose version has a CRC file, it parses the
checksum's table-level statistics and caches them on the snapshot. Call
`Snapshot::get_file_stats_if_loaded()` to read them back:

```rust,ignore
if let Some(stats) = snapshot.get_file_stats_if_loaded() {
    println!("num files:        {}", stats.num_files);
    println!("table size bytes: {}", stats.table_size_bytes);
}
```

`get_file_stats_if_loaded()` returns `None` when no CRC file was loaded for
this snapshot's version. It never triggers a CRC read. It only returns stats
that were already materialized during snapshot construction, so the call is
cheap and always synchronous.

### File size histogram

When the writer that produced the CRC file recorded a file size histogram,
Kernel exposes it alongside the scalar totals. The histogram groups the
snapshot's live files into size bins:

```rust,ignore
if let Some(stats) = snapshot.get_file_stats_if_loaded() {
    if let Some(histogram) = &stats.file_size_histogram {
        for ((bin_start, count), bytes) in histogram
            .sorted_bin_boundaries
            .iter()
            .zip(&histogram.file_counts)
            .zip(&histogram.total_bytes)
        {
            println!(">= {bin_start:>12} B: {count} files, {bytes} bytes");
        }
    }
}
```

`sorted_bin_boundaries` gives the inclusive lower bound of each bin. The next
boundary is the exclusive upper bound. `file_counts` and `total_bytes` are
parallel arrays with the same length. The histogram is `None` when the writer
did not populate it.

Use these stats for lightweight planning (cost estimation, compaction
heuristics, monitoring) without replaying the log. When
`get_file_stats_if_loaded()` returns `None`, either fall back to aggregating
`ScanFile.size` and `ScanFile.stats` via `visit_scan_files`, or write a CRC
file on the next commit so subsequent snapshots have stats available.

> [!NOTE]
> `file_size_histogram` is an optional field in the Delta protocol. Kernel
> surfaces whatever the CRC file contains. A `None` value means the histogram
> was omitted, not that the table has no files.

## What's next

- [Checkpointing](./checkpointing.md) for compacting the transaction log into
  Parquet
- [Appending Data](../writing/append.md) for the full write and commit flow
