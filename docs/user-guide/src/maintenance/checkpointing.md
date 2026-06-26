# Checkpointing

To compact a Delta table's transaction log into a single Parquet file, you write a
**checkpoint**. A checkpoint allows readers to skip old JSON commit files and start from
the checkpoint instead, which speeds up table discovery for tables with many commits.

## When to checkpoint

Kernel does not checkpoint automatically. Your application decides when to trigger one.
The table property `delta.checkpointInterval` controls the recommended frequency. You can
read it via `snapshot.table_properties().checkpoint_interval`, which returns an
`Option<NonZero<u64>>` representing the number of commits between checkpoints (e.g., 10
means checkpoint every 10 commits).

## The simple path: `Snapshot::checkpoint()`

The easiest way to write a checkpoint is the convenience method on `Snapshot`:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::snapshot::CheckpointWriteResult;
# use delta_kernel::{DeltaResult, Snapshot};
# fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;
let (result, new_snapshot) = snapshot.checkpoint(&engine, None)?;
match result {
    CheckpointWriteResult::Written => println!("Checkpoint written"),
    CheckpointWriteResult::AlreadyExists => println!("Checkpoint already exists"),
}
# Ok(())
# }
```

`checkpoint()` takes a `&SnapshotRef` (i.e., `&Arc<Snapshot>`) and returns a
`DeltaResult<(CheckpointWriteResult, SnapshotRef)>`. The returned `SnapshotRef` reflects
the new checkpoint when `Written`, or the original snapshot when `AlreadyExists`.

This method handles everything in one call: reads the log, reconciles actions, writes the
checkpoint Parquet file, and updates the `_last_checkpoint` hint file.

> [!NOTE]
> `checkpoint()` calls `ParquetHandler::write_parquet_file` to write the checkpoint and
> `StorageHandler::head` to retrieve its metadata. The `DefaultEngine` implements both.
> If you use a custom engine, make sure it provides working implementations of both.

## After a commit

The most common time to checkpoint is right after a successful commit. The post-commit
snapshot gives you a checkpoint-ready `SnapshotRef`:

```rust,ignore
let committed = match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(c) => c,
    _ => panic!("unexpected result"),
};

if let Some(snapshot) = committed.post_commit_snapshot() {
    let (_result, _new_snapshot) = snapshot.clone().checkpoint(&engine, None)?;
}
```

> [!WARNING]
> For catalog-managed tables, you must
> [publish](../catalog_managed/writing.md) the commit before checkpointing.
> `create_checkpoint_writer()` returns an error if the snapshot is not published.

## The custom path: `CheckpointWriter`

If you need control over how the checkpoint file is written, use the lower-level
`CheckpointWriter` API. `create_checkpoint_writer()` consumes an `Arc<Snapshot>`:

```rust,ignore
// 1. Create a CheckpointWriter from a snapshot (consumes the Arc<Snapshot>)
let writer = snapshot.create_checkpoint_writer()?;

// 2. Get the path where the checkpoint should be written
let checkpoint_path = writer.checkpoint_path()?;

// 3. Get the checkpoint data as an ActionReconciliationIterator
let checkpoint_data = writer.checkpoint_data(&engine)?;
let state = checkpoint_data.state();

// 4. Write data to storage (engine-specific).
//    You must fully consume the iterator and write all data.
let lazy_data = checkpoint_data
    .map(|r| r.and_then(|f| f.apply_selection_vector()));
// ... write lazy_data to checkpoint_path in your own way ...

// 5. Get file metadata (size, modification time, etc.)
let file_meta = engine.storage_handler().head(&checkpoint_path)?;

// 6. Build LastCheckpointHintStats from the now-exhausted iterator state.
//    Use 0 for num_sidecars on V1 checkpoints or V2 checkpoints without sidecars.
let state = Arc::into_inner(state)
    .ok_or_else(|| Error::internal_error("checkpoint state Arc still has other references"))?;
let last_checkpoint_stats = LastCheckpointHintStats::from_reconciliation_state(
    state,
    file_meta.size,
    0, // num_sidecars
)?;

// 7. Finalize. Writes the _last_checkpoint hint file.
writer.finalize(&engine, &last_checkpoint_stats)?;
```

The key requirement is that the data iterator must be **fully consumed** before building
the `LastCheckpointHintStats`. `LastCheckpointHintStats::from_reconciliation_state` returns
an error if the iterator has not been exhausted. Because `finalize()` reads the action
counts from the stats struct rather than from the iterator, you build the stats first and
then pass them to `finalize()`.

## Checkpoint format

Kernel automatically selects the checkpoint format based on table features:

| Table feature | Checkpoint format |
|---------------|-------------------|
| No `v2Checkpoints` | Classic V1 single-file checkpoint |
| `v2Checkpoints` enabled | Classic-named V2 checkpoint (with `CheckpointMetadata` action) |

You do not need to manage this yourself. `CheckpointWriter` handles format selection and
includes the appropriate actions.

## Deciding when to checkpoint

After a successful commit, `CommittedTransaction` exposes a `PostCommitStats` struct through
`post_commit_stats()`. You can use these stats to decide whether to trigger a checkpoint or
other maintenance operations.

`PostCommitStats` has two fields:

| Field | Meaning |
|-------|---------|
| `commits_since_checkpoint` | Number of commits since the last checkpoint. Commit 0 counts as a checkpoint. |
| `commits_since_log_compaction` | Number of commits since the last log compaction or checkpoint. A checkpoint resets this counter too. |

The typical pattern is to check `commits_since_checkpoint` against the table's
`delta.checkpointInterval` property and checkpoint when the threshold is reached:

```rust,ignore
let committed = match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(c) => c,
    _ => panic!("unexpected result"),
};

let stats = committed.post_commit_stats();
let interval = committed
    .post_commit_snapshot()
    .map(|s| {
        s.table_properties()
            .checkpoint_interval
            .map(|n| n.get())
            .unwrap_or(10)
    })
    .unwrap_or(10);

if stats.commits_since_checkpoint >= interval {
    if let Some(snapshot) = committed.post_commit_snapshot() {
        let (_result, _new_snapshot) = snapshot.clone().checkpoint(&engine, None)?;
    }
}
```

See [Version Checksums](./version_checksums.md) for the recommended full
post-commit pattern that includes writing a CRC file before checkpointing.

## Log compaction

**Log compaction** aggregates multiple commit JSON files into a single compacted file,
reducing the number of files Kernel must read during log replay. The API follows a similar
pattern to the `CheckpointWriter` API: create a `LogCompactionWriter` from a `Snapshot`,
retrieve the compaction path and data, then write the data to storage.

> [!NOTE]
> Log compaction is currently disabled on both reads and writes due to insufficient
> integration test coverage. See [issue #2337](https://github.com/delta-io/delta-kernel-rs/issues/2337)
> for tracking. The `Snapshot::log_compaction_writer()` method exists but returns an error
> if called. This section will be updated when the feature is re-enabled.

## What's next

- [Version Checksums](./version_checksums.md) for writing CRC files after every commit
- [Appending Data](../writing/append.md) to learn about writing data and committing
- [Catalog-Managed Tables](../catalog_managed/overview.md) to understand publishing
  commits for catalog-managed tables
