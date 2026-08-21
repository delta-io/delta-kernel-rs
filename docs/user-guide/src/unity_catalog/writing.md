# Writing to Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-client-api, unity-catalog-delta-rest-client -->

To write to a Unity Catalog-managed Delta table, you create a `UCCommitter`,
pass it to a Kernel transaction, and then publish the staged commit to make it
visible in `_delta_log/`.

Before reading this page, make sure you understand the generic
[catalog-managed write lifecycle](../catalog_managed/writing.md) and the
[Unity Catalog Integration overview](./overview.md).

> [!NOTE]
> This page uses the `delta-kernel-unity-catalog` and
> `unity-catalog-delta-rest-client` crates. All code examples use
> `rust,ignore` because they require these external crates.

## Set up clients and resolve the table

Use `UCClient` to load the table and fetch read-write credentials, and build a
`UCUpdateTableRestClient` for the commit path. Both clients share a
`ClientConfig`.

```rust,ignore
use std::sync::Arc;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCUpdateTableRestClient};

let config = ClientConfig::build("my-workspace.cloud.databricks.com", token).with_additional_user_agent([("MyConnector", "1.0")]).build()?;
let uc_client = UCClient::new(config.clone())?;
let update_client = Arc::new(UCUpdateTableRestClient::new(config)?);

// Load the table: metadata + inline log tail
let resp = uc_client
    .load_table("my_catalog", "my_schema", "my_table")
    .await?;
let table_uri = url::Url::parse(&resp.metadata.location)?;

// Fetch read-write credentials for the table's cloud storage
let creds = uc_client
    .get_table_credentials("my_catalog", "my_schema", "my_table", Operation::ReadWrite)
    .await?;
```

For the full details on building an engine from vended credentials, see
[Reading UC Tables](./reading.md).

## Load a snapshot from the load_table response

Build the Snapshot from the `load_table` response. Pass it to
`snapshot_builder_from_load_table` to get a `SnapshotBuilder` with the log tail
and max catalog version already applied, then call `build`.

```rust,ignore
use delta_kernel_unity_catalog::snapshot_builder_from_load_table;

let snapshot = snapshot_builder_from_load_table(&resp)?.build(&engine)?;
```

The returned `Snapshot` reflects all ratified commits the catalog knows about,
including those that haven't been published to `_delta_log/` yet.

## Create a transaction and `UCCommitter`

`UCCommitter` implements Kernel's `Committer` trait. It stages the commit to
`_staged_commits/`, then submits it to the UC `update_table` API for Unity
Catalog to ratify. Construct it with the commit client and the table's
three-part name plus its table ID.

```rust,ignore
use delta_kernel_unity_catalog::UCCommitter;
use unity_catalog_delta_client_api::TableIdentifier;

let table_id = resp.metadata.table_uuid.clone();
let committer = UCCommitter::new(
    update_client.clone(),
    table_id.clone(),
    TableIdentifier::new("my_catalog", "my_schema", "my_table"),
);
let txn = snapshot.clone().transaction_builder()
    .with_operation(delta_kernel::transaction::Operation::Write)
    .build(&engine)?;
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

From this point, writing data works the same as any Kernel transaction. Get the write context,
write Parquet files to the table's storage location, and pass the resulting file metadata to
`commit()` directly or collect multiple batches in `CommitActions`.

```rust,ignore
let write_context = txn.unpartitioned_write_context()?;
// ... write Parquet files using write_context ...
```

See [Appending Data](../writing/append.md) for the full details on writing
Parquet files and registering file metadata.

## Commit and handle the result

Call `txn.commit()` to stage the commit and ratify it through UC.

```rust,ignore
use delta_kernel::transaction::CommitResult;

match txn.commit(&engine, &committer, file_metadata.into())? {
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
    CommitResult::RetryableTransaction(retryable) => {
        // Transient I/O or server error after the UC HTTP client's own retry
        // budget was exhausted. Retry the exact transaction and actions.
        let next_result = retryable.retry(&engine, &committer)?;
        // Handle `next_result` with the same result loop.
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
2. Submits the staged commit to the UC `update_table` API, where Unity Catalog
   ratifies it

Version 0 (table creation) takes a different path: `UCCommitter` writes
`_delta_log/00000000000000000000.json` directly and skips the `update_table` API.
See [Creating UC Tables](./creating_tables.md) for the creation flow and
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
use unity_catalog_delta_client_api::TableIdentifier;

// Build a committer to drive publishing
let committer = UCCommitter::new(
    update_client.clone(),
    table_id.clone(),
    TableIdentifier::new("my_catalog", "my_schema", "my_table"),
);

let published_snapshot = post_commit_snapshot
    .publish(&engine, &committer)?;
```

Publishing copies each staged commit from `_staged_commits/<version>.<uuid>.json`
to `_delta_log/<version>.json`. If a published file already exists (from a
previous publish attempt), the copy is silently skipped.

## Report commit metrics

After a successful commit, report commit metrics to the catalog. For a
catalog-managed table, the catalog owns table maintenance, not your connector.
Unity Catalog uses the per-commit metrics you report to decide when to schedule
OPTIMIZE and VACUUM.

> [!NOTE]
> Support for the metrics endpoint depends on the catalog. Some catalogs accept
> reported metrics; others, including the open-source Unity Catalog server, do
> not implement it yet and return a 404. Treat the report as best-effort: a
> failure never affects commit correctness.

You supply the counts the write engine observed. The row counts and the
file-size histogram are only known to your engine. File and byte counts are
derivable from the commit's add and remove actions.

The catalog requires the commit version on every report, and reads it from
`file_size_histogram.commit_version`. Set it to the version your commit
produced. When you have no size distribution to report, send a histogram with
empty bins that carries only the version.

```rust,ignore
use unity_catalog_delta_client_api::{CommitReport, FileSizeHistogram};

let report = CommitReport {
    num_files_added: 3,
    num_bytes_added: 4096,
    num_rows_inserted: Some(1000),
    file_size_histogram: FileSizeHistogram {
        commit_version,
        ..Default::default()
    },
    ..Default::default()
};

// table_id comes from the load_table response (metadata.table_uuid).
if let Err(e) = uc_client
    .report_metrics("my_catalog", "my_schema", "my_table", &table_id, report)
    .await
{
    eprintln!("metrics report failed (non-fatal): {e}");
}
```

## Post-publish maintenance

Once commits are published, you can checkpoint the table:

```rust,ignore
published_snapshot.checkpoint(&engine, None)?;
```

Checkpointing requires published commits. If you skip the publish step,
checkpointing fails because it can only operate on published versions.

## Complete example

```rust,ignore
use std::sync::Arc;
use delta_kernel::transaction::CommitResult;
use delta_kernel_unity_catalog::{snapshot_builder_from_load_table, UCCommitter};
use unity_catalog_delta_client_api::{Operation, TableIdentifier};
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCUpdateTableRestClient};

// 1. Set up clients
let config = ClientConfig::build("my-workspace.cloud.databricks.com", token).with_additional_user_agent([("MyConnector", "1.0")]).build()?;
let uc_client = UCClient::new(config.clone())?;
let update_client = Arc::new(UCUpdateTableRestClient::new(config)?);

// 2. Load the table and fetch read-write credentials
let resp = uc_client
    .load_table("my_catalog", "my_schema", "my_table")
    .await?;
let table_uri = url::Url::parse(&resp.metadata.location)?;
let table_id = resp.metadata.table_uuid.clone();
let creds = uc_client
    .get_table_credentials("my_catalog", "my_schema", "my_table", Operation::ReadWrite)
    .await?;

// 3. Build engine with vended credentials. `build_engine_with_credentials` is
//    a connector-owned helper, not part of the library. See Step 4 of
//    [Reading UC Tables](./reading.md) for the full expansion.
let engine = build_engine_with_credentials(&table_uri, &creds)?;

// 4. Build a Snapshot from the load_table response
let snapshot = snapshot_builder_from_load_table(&resp)?.build(&engine)?;

// 5. Create the transaction and its commit-time UCCommitter
let committer = UCCommitter::new(
    update_client.clone(),
    table_id.clone(),
    TableIdentifier::new("my_catalog", "my_schema", "my_table"),
);
let txn = snapshot.clone().transaction_builder()
    .with_operation(delta_kernel::transaction::Operation::Write)
    .build(&engine)?;

// 6. Write data
let write_context = txn.unpartitioned_write_context()?;
// ... write Parquet files using write_context ...

// 7. Commit, publish, and checkpoint
match txn.commit(&engine, &committer, file_metadata.into())? {
    CommitResult::CommittedTransaction(committed) => {
        let post_commit_snapshot = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot");

        // Publish staged commits to _delta_log/
        let published_snapshot = post_commit_snapshot
            .publish(&engine, &committer)?;

        // Checkpoint the published snapshot
        published_snapshot.checkpoint(&engine, None)?;
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
