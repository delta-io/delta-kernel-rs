# Writing: the commit and publish flow

To write to a catalog-managed table, you follow the same transaction pattern as a
filesystem-managed table. The difference is that your catalog `Committer` stages, ratifies,
and publishes commits through the catalog instead of writing directly to the filesystem.

Before reading this page, make sure you understand [Catalog-managed tables](./overview.md).

## The write lifecycle

A catalog-managed write has four phases:

```text
1. LOAD SNAPSHOT
   Catalog client resolves the table and fetches recent commits.
   Build snapshot with Snapshot::builder_for(path).with_log_tail(commits).build()

2. COMMIT
   Transaction generates actions. Committer stages them to _staged_commits/.
   Committer calls the catalog API to ratify the staged commit.

3. HANDLE RESULT
   Committed: get post-commit snapshot, proceed to publish.
   Conflict: rebase and retry.
   Retryable: transient I/O error, retry.

4. PUBLISH
   Copy staged commits from _staged_commits/ to _delta_log/ as published delta files.
   Returns a new snapshot with all commits marked as published.
```

## Phase 1: Load a snapshot

Your catalog client resolves the table, fetches credentials and recent commits, then
builds a snapshot. See [Reading catalog-managed tables](./reading.md) for the full
details.

```rust,ignore
// Catalog client resolves table name -> path + credentials + commits
let (path, credentials, commits, latest_version) = catalog_client.load_table(table_name)?;

// Build snapshot with log tail and cap at the catalog-ratified version
let snapshot = Snapshot::builder_for(path)
    .with_max_catalog_version(latest_version)
    .with_log_tail(commits)
    .build(&engine)?;
```

## Phase 2: Create a transaction and commit

To begin a write, create a transaction with your catalog's `Committer`, add files, and
call `commit()`:

```rust,ignore
// transaction() moves the Box<dyn Committer> into the Transaction, and commit()
// consumes the Transaction, so the boxed committer is gone by the time you need
// to publish. Construct a second committer for publish() in Phase 3 and clone
// any catalog-client state you need to keep in scope across both calls.
let committer = Box::new(MyCatalogCommitter::new(
    catalog_client.clone(),
    table_id.clone(),
));
let mut txn = snapshot
    .transaction(committer, &engine)?
    .with_operation("INSERT".to_string());

// Drive your Parquet writer from the write context, then hand the resulting
// add-file metadata batch to the transaction. See the
// [Appending data](../writing/append.md) how-to for the full Parquet-writing flow.
let write_context = txn.unpartitioned_write_context()?;
// ... use write_context to produce add_metadata: Box<dyn EngineData>
//     matching txn.add_files_schema() ...
txn.add_files(add_metadata);

// Commit the transaction. Kernel invokes committer.commit() internally; Phase 3
// handles the result.
let commit_result = txn.commit(&engine)?;
```

The `?` on `txn.commit(&engine)?` only propagates non-recoverable errors. Successful
commits, conflicts, and retryable I/O errors arrive as the three variants of
`CommitResult` in the match below. Everything else (auth errors, catalog protocol
errors, etc.) bubbles out directly.

When `txn.commit()` runs, Kernel:

1. Assembles the actions: a leading `CommitInfo`, any Protocol/Metadata updates
   this transaction makes (absent on steady-state appends), and Add/Remove file
   actions.
2. Verifies that a catalog committer is being used. Kernel rejects `FileSystemCommitter`
   for catalog-managed tables.
3. Calls `committer.commit(engine, actions, commit_metadata)`.

Your committer then:

1. Writes the actions to `_staged_commits/<version>.<uuid>.json`.
2. Calls the catalog API to ratify the staged commit.
3. Returns `CommitResponse::Committed` or `CommitResponse::Conflict`.

## Phase 3: Handle the result

On success, `CommittedTransaction` provides the commit version and a **post-commit
snapshot** that reflects the newly committed state:

```rust,ignore
use delta_kernel::transaction::CommitResult;

match commit_result {
    CommitResult::CommittedTransaction(committed) => {
        let version = committed.commit_version();
        // post_commit_snapshot() returns an Option. For catalog-managed
        // commits today, Kernel returns Some. The Option exists for
        // incremental-development paths (e.g., table creation). Treat a
        // None as an error rather than silently skipping publish, so the
        // problem surfaces loudly if the invariant ever changes.
        let post_commit = committed
            .post_commit_snapshot()
            .ok_or_else(|| Error::generic("missing post-commit snapshot"))?;

        // commit() consumed the Box<dyn Committer> from Phase 2. publish() only
        // needs &dyn Committer, so construct a fresh instance here. This moves
        // catalog_client and table_id; clone them if you need them for a retry
        // loop around the whole write.
        let publish_committer =
            MyCatalogCommitter::new(catalog_client, table_id);

        // Proceed to publish (Phase 4).
        let published_snapshot = post_commit.publish(&engine, &publish_committer)?;
    }
    CommitResult::ConflictedTransaction(conflicted) => {
        // Another writer already committed at this version.
        // `conflicted.conflict_version()` returns the version this transaction
        // attempted. Rebase onto the new table state and retry.
    }
    CommitResult::RetryableTransaction(retryable) => {
        // Transient I/O error. `retryable.error` gives the underlying cause;
        // `retryable.transaction` is the original transaction you can retry
        // without rebasing. Kernel reaches this arm only for `Error::IOError`
        // variants; return other error kinds as-is rather than disguising
        // them as IOError to force retry.
    }
}
```

## Phase 4: Publish

Call `Snapshot::publish()` on the post-commit snapshot (shown in Phase 3 above) to
make ratified commits visible as normal delta files and to unlock maintenance
operations. `Snapshot::publish()`:

1. Finds all unpublished catalog commits in the snapshot's log segment.
2. Validates that the table is catalog-managed and the committer is a catalog committer.
3. Builds a `PublishMetadata` containing the list of `CatalogCommit` entries to publish.
4. Delegates to `committer.publish(engine, publish_metadata)`, which copies each staged
   commit from `_staged_commits/` to `_delta_log/<version>.json`.
5. Returns a **new snapshot** where all commits are marked as published.

Publishing matters because:

- It makes commits visible to filesystem-based readers.
- It enables maintenance operations such as checkpointing, which can only
  operate on published versions.
- It reduces the number of commits the catalog needs to store and serve.

## Maintenance operations

Once commits are published, you can perform maintenance operations on the published
snapshot:

```rust,ignore
// Checkpoint the published snapshot. checkpoint() returns
// (CheckpointWriteResult, SnapshotRef): the first element is an enum reporting
// whether a checkpoint was written or one already existed, and the second is
// the snapshot with the updated log segment.
let (_checkpoint_result, _post_checkpoint_snapshot) =
    published_snapshot.checkpoint(&engine)?;
```

> [!NOTE]
> Maintenance operations on catalog-managed tables are subject to extra rules:
> Kernel requires published versions, and the managing catalog controls which
> operations a client may run.

For a complete Unity Catalog example, see
[Writing to Unity Catalog tables](../unity_catalog/writing.md).

## What's next

- [Implementing a catalog committer](./committer.md): how to build a `Committer` for
  your catalog.
- [Reading catalog-managed tables](./reading.md): how to provide catalog commits for
  snapshot construction.
- [Appending data](../writing/append.md): the basics of writing data with Kernel.
