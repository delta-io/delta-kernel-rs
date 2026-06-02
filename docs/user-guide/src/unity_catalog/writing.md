# Writing to Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-client-api, unity-catalog-delta-rest-client -->

To write to a Unity Catalog-managed Delta table, you create a `UCCommitter`,
pass it to a Kernel transaction, and then publish the staged commit to make it
visible in `_delta_log/`.

Before reading this page, make sure you understand the generic
[catalog-managed write lifecycle](../catalog_managed/writing.md) and the
[Unity Catalog integration overview](./overview.md).

> [!NOTE]
> This page uses the `delta-kernel-unity-catalog` and
> `unity-catalog-delta-rest-client` crates. All code examples use
> `rust,ignore` because they require these external crates.

## Set up clients and resolve the table

Use `UCClient` to resolve the table name and fetch read-write credentials, then
build a `UCCommitsRestClient` for the commits API. Both clients share a
`ClientConfig`.

```rust,ignore
use std::sync::Arc;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCCommitsRestClient};

let config = ClientConfig::build("my-workspace.cloud.databricks.com", token).build()?;
let uc_client = UCClient::new(config.clone())?;
let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

// Resolve table name to table ID and storage location
let table_info = uc_client.get_table("my_catalog.my_schema.my_table").await?;
let table_id = &table_info.table_id;
let table_uri = &table_info.storage_location;

// Fetch read-write credentials for the table's cloud storage
let creds = uc_client.get_credentials(table_id, Operation::ReadWrite).await?;
```

For the full details on resolving tables and building an engine with vended
credentials, see [Reading UC Tables](./reading.md).

## Load a snapshot with the log tail

Use `UCKernelClient` to load a snapshot. It fetches the catalog's log tail
(staged commits) and passes them to Kernel's `Snapshot::builder_for` with
`with_log_tail` and `with_max_catalog_version`.

```rust,ignore
use delta_kernel_unity_catalog::UCKernelClient;

let catalog = UCKernelClient::new(commits_client.as_ref());
let snapshot = catalog.load_snapshot(table_id, table_uri, &engine).await?;
```

The returned `Snapshot` reflects all ratified commits the catalog knows about,
including those that haven't been published to `_delta_log/` yet.

## Create a transaction with `UCCommitter`

`UCCommitter` implements Kernel's `Committer` trait. It stages the commit to
`_staged_commits/`, then calls the UC commits API to ratify it.

```rust,ignore
use delta_kernel_unity_catalog::UCCommitter;

let committer = Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()));
let mut txn = snapshot.clone().transaction(committer, &engine)?
    .with_operation("INSERT".to_string());
```

`UCCommitter` requires a multi-threaded tokio runtime. The default Kernel
engine already uses tokio, so this is compatible as long as you use the
multi-threaded runtime (the default for `#[tokio::main]`).

> [!WARNING]
> `UCCommitter` validates that every commit targets a catalog-managed table
> with in-commit timestamps enabled. It rejects the commit if the table is
> missing the `catalogManaged`, `vacuumProtocolCheck`, or `inCommitTimestamp`
> writer features, if `delta.enableInCommitTimestamps` is not `"true"`, or if
> `io.unitycatalog.tableId` does not match the committer's `table_id`. Tables
> created through [Creating UC Tables](./creating_tables.md) satisfy all of
> these automatically.

## Write data

From this point, writing data works the same as any Kernel transaction. Get the
write context, write Parquet files to the table's storage location, and add the
resulting file metadata to the transaction.

```rust,ignore
let write_context = txn.unpartitioned_write_context()?;
// ... write Parquet files using write_context ...
txn.add_files(file_metadata);
```

See [Appending Data](../writing/append.md) for the full details on writing
Parquet files and registering file metadata.

## Commit and handle the result

Call `txn.commit()` to stage the commit and ratify it through UC.

```rust,ignore
use delta_kernel::transaction::CommitResult;

match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => {
        let version = committed.commit_version();
        let post_commit_snapshot = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot");
        // Proceed to publish (next step)
    }
    CommitResult::ConflictedTransaction(conflicted) => {
        // Another writer committed this version first. Rebase onto the new
        // snapshot and retry. UCCommitter does not retry at this level.
    }
    CommitResult::RetryableTransaction(_retryable) => {
        // Transient I/O or server error after the UC HTTP client's own retry
        // budget was exhausted. Retry the commit from scratch.
    }
}
```

The REST client automatically retries transport-level failures (HTTP 5xx,
connection errors) according to the retry knobs on `ClientConfigBuilder`. See
[Client configuration and retries](./overview.md#client-configuration-and-retries).
Once that budget is exhausted, `UCCommitter` surfaces the failure as
`CommitResult::RetryableTransaction`. Transaction-level retries (including
rebasing after a `ConflictedTransaction`) are the connector's responsibility;
`UCCommitter` does not retry commits itself.

Under the hood, `UCCommitter::commit` does two things for versions >= 1:

1. Writes the transaction's actions to
   `_delta_log/_staged_commits/<version>.<uuid>.json`
2. Calls the UC commits API to ratify the staged commit

For version 0 (table creation), the committer writes directly to
`_delta_log/00000000000000000000.json` instead and skips the UC commits API.
See [Creating UC Tables](./creating_tables.md) for the full creation flow and
[the catalog-managed write lifecycle](../catalog_managed/writing.md) for the
generic ratification flow.

> [!WARNING]
> `UCCommitter` does not support ALTER TABLE operations (protocol changes,
> metadata changes, or clustering column changes). It also rejects attempts to
> upgrade a path-based table to catalog-managed or downgrade a catalog-managed
> table to path-based. Attempting any of these returns an error.

## Publish staged commits

After a successful commit, publish the staged commit so it becomes visible as a
normal delta file in `_delta_log/`. Without publishing, only catalog-aware
readers can see the commit.

```rust,ignore
use delta_kernel_unity_catalog::UCCommitter;

// Keep a reference to the committer for publishing
let committer: Box<dyn delta_kernel::committer::Committer> =
    Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()));

let published_snapshot = post_commit_snapshot
    .publish(&engine, committer.as_ref())?;
```

Publishing copies each staged commit from `_staged_commits/<version>.<uuid>.json`
to `_delta_log/<version>.json`. If a published file already exists (from a
previous publish attempt), the copy is silently skipped.

## Post-publish maintenance

Once commits are published, you can checkpoint the table:

```rust,ignore
published_snapshot.checkpoint(&engine)?;
```

Checkpointing requires published commits. If you skip the publish step,
checkpointing fails because it can only operate on published versions.

## Complete example

```rust,ignore
use std::sync::Arc;
use delta_kernel::transaction::CommitResult;
use delta_kernel_unity_catalog::{UCCommitter, UCKernelClient};
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCCommitsRestClient};

// 1. Set up clients
let config = ClientConfig::build("my-workspace.cloud.databricks.com", token).build()?;
let uc_client = UCClient::new(config.clone())?;
let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

// 2. Resolve table and fetch credentials
let table_info = uc_client.get_table("my_catalog.my_schema.my_table").await?;
let table_id = &table_info.table_id;
let table_uri = &table_info.storage_location;
let creds = uc_client.get_credentials(table_id, Operation::ReadWrite).await?;

// 3. Build engine with vended credentials. `build_engine_with_credentials` is
//    a connector-owned helper, not part of the library. See Step 4 of
//    [Reading UC Tables](./reading.md) for the full expansion.
let engine = build_engine_with_credentials(table_uri, &creds)?;

// 4. Load snapshot via UC log tail
let catalog = UCKernelClient::new(commits_client.as_ref());
let snapshot = catalog.load_snapshot(table_id, table_uri, &engine).await?;

// 5. Create transaction with UCCommitter
let committer = Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()));
let mut txn = snapshot.clone().transaction(committer, &engine)?
    .with_operation("INSERT".to_string());

// 6. Write data
let write_context = txn.unpartitioned_write_context()?;
// ... write Parquet files using write_context ...
txn.add_files(file_metadata);

// 7. Commit, publish, and checkpoint
let committer_for_publish: Box<dyn delta_kernel::committer::Committer> =
    Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()));

match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => {
        let post_commit_snapshot = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot");

        // Publish staged commits to _delta_log/
        let published_snapshot = post_commit_snapshot
            .publish(&engine, committer_for_publish.as_ref())?;

        // Checkpoint the published snapshot
        published_snapshot.checkpoint(&engine)?;
    }
    CommitResult::ConflictedTransaction(_) => { /* rebase and retry */ }
    CommitResult::RetryableTransaction(_) => { /* retry the commit */ }
}
```

## What's next

- [Creating UC Tables](./creating_tables.md) for creating a brand-new
  UC-managed table before you can write to it
- [Catalog-managed write lifecycle](../catalog_managed/writing.md) for the
  generic commit and publish flow
- [Appending Data](../writing/append.md) for the details of writing Parquet
  files and collecting file metadata
- [Reading UC Tables](./reading.md) for resolving tables and loading snapshots
