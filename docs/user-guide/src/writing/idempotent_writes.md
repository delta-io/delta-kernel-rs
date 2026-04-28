# Idempotent writes

To guarantee at-most-once semantics for retryable write pipelines (e.g., a streaming job
or a queue consumer), you attach a **transaction identifier** to each commit. If the same
logical write is attempted twice, the second attempt becomes a no-op. Delta supports this
through `SetTransaction` actions recorded in the transaction log.

## How it works

Each transaction can carry a `SetTransaction` action containing:

- **`app_id`**: a string identifying the application (e.g., `"my-streaming-job"`)
- **`version`**: an application-defined integer. The Delta protocol leaves its semantics
  up to the connector. A common pattern is a monotonically increasing counter (batch
  number, Kafka offset, etc.), but other schemes are valid. The protocol only guarantees
  that the latest committed `version` per `app_id` is retrievable.

Before writing, check the table for the latest committed version for your `app_id`. If
that version indicates the write has already been applied, skip it. The comparison logic
is up to you.

## Writing with a transaction ID

Call `with_transaction_id()` on the transaction and check `get_app_id_version()` before
committing:

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let app_id = "my-streaming-job";
let batch_version: i64 = 42;

// Check if this batch was already committed
if let Some(committed_version) = snapshot.get_app_id_version(app_id, &engine)? {
    if committed_version >= batch_version {
        println!("batch {batch_version} already committed (table has {committed_version})");
        return Ok(());
    }
}

// Not yet committed. Proceed with the write.
let txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_transaction_id(app_id.to_string(), batch_version)
    .with_operation("STREAMING UPDATE".to_string());

// ... write data, add files, commit ...
# Ok(())
# }
```

The `>=` check above assumes the application uses monotonically increasing versions. A
connector with a different scheme (for example, tracking a set of known batch IDs) would
compare differently.

The `SetTransaction` action is written to the commit log alongside the data actions. On
the next run, `get_app_id_version()` will find it and the duplicate write is skipped.

## Reading the latest transaction version

`Snapshot::get_app_id_version()` scans the log for the latest `SetTransaction` with the
given `app_id`:

```rust,ignore
let latest: Option<i64> = snapshot.get_app_id_version("my-app", &engine)?;
```

Returns `None` if no transaction with that `app_id` has been committed.

## Transaction retention

By default, `SetTransaction` actions are retained indefinitely. You can configure a
retention duration via the `delta.setTransactionRetentionDuration` table property (e.g.
`"interval 30 days"`). When set, transactions whose `lastUpdated` timestamp is older than
the retention window are filtered out by `get_app_id_version()`.

> [!NOTE]
> Kernel automatically sets `lastUpdated` to the commit timestamp when you call
> `with_transaction_id()`. You don't need to set it manually.

## What's next

- [Appending data](./append.md) covers the basics of writing data with Kernel
- [Checkpointing](../maintenance/checkpointing.md) explains how to write checkpoints after commits
