# Creating Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-rest-client, unity-catalog-delta-client-api -->

To create a new Unity Catalog-managed Delta table, you register the table with
your UC server to obtain a table ID and storage location, write a version 0
Delta log commit with the required catalog-managed properties, and then send a
second set of properties back to UC to finalize registration.

Before reading this page, make sure you understand
[Creating a Table](../writing/create_table.md) and the
[Unity Catalog integration overview](./overview.md).

> [!WARNING]
> Steps 1 and 5 below call UC endpoints that are not yet exposed by the Rust
> `unity-catalog-delta-rest-client` crate. Route those calls through your
> connector's own UC client. They are planned for inclusion in the
> `unity-catalog-delta-client-api` crate.

## Prerequisites

- A three-part table name, Delta schema, and target storage location.
- A connector-owned UC client that can call UC's staging and create-table
  endpoints.

## Step 1: Reserve the table in Unity Catalog

```rust,ignore
// TODO: not yet exposed by `unity-catalog-delta-rest-client`. Call through
// your connector's own UC client.
let staging_info = my_uc_client.get_staging_table(
    "main.default.my_table",
    &schema,
).await?;
let table_id = staging_info.table_id;
let table_uri = staging_info.storage_location;
```

## Step 2: Collect the disk-bound properties

```rust,ignore
use delta_kernel_unity_catalog::get_required_properties_for_disk;

let disk_props = get_required_properties_for_disk(&table_id);
```

The returned map has exactly three entries:

| Key | Value |
|-----|-------|
| `delta.feature.catalogManaged` | `supported` |
| `delta.feature.vacuumProtocolCheck` | `supported` |
| `io.unitycatalog.tableId` | the UC-assigned table ID |

> [!NOTE]
> The map intentionally omits the `inCommitTimestamp` feature and
> `delta.enableInCommitTimestamps=true`. Kernel's `create_table()` auto-enables
> both when it sees the `catalogManaged` feature.

## Step 3: Build and commit the version 0 transaction

```rust,ignore
use std::sync::Arc;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel_unity_catalog::UCCommitter;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCCommitsRestClient};

let config = ClientConfig::build(&endpoint, &token).build()?;
let uc_client = UCClient::new(config.clone())?;
let commits_client = Arc::new(UCCommitsRestClient::new(config)?);

// Credentials. Use ReadWrite so the engine can write 000.json into storage.
let creds = uc_client.get_credentials(&table_id, Operation::ReadWrite).await?;
let engine = build_engine_with_credentials(&table_uri, &creds)?;

// Build the create-table transaction with the disk-bound properties.
let committer = Box::new(UCCommitter::new(commits_client.clone(), table_id.clone()));
let create_txn = create_table(table_uri.as_str(), Arc::new(schema), "MyApp/1.0")
    .with_table_properties(disk_props)
    .build(&engine, committer)?;

let post_commit_snapshot = match create_txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => committed
        .post_commit_snapshot()
        .cloned()
        .expect("post-commit snapshot is always populated for create table"),
    CommitResult::ConflictedTransaction(_) => {
        // Another writer created the table first. Delete the UC reservation
        // and fail, or fall through to read the existing table.
        return Err("table already exists".into());
    }
    CommitResult::RetryableTransaction(_) => {
        return Err("version 0 commit failed with a transient error; retry".into());
    }
};
```

On version 0, `UCCommitter` writes `_delta_log/00000000000000000000.json`
directly and skips the UC commits API.

See `build_engine_with_credentials` in
[Step 4 of Reading UC Tables](./reading.md#step-4-build-an-engine-with-vended-credentials)
for the engine construction details.

## Step 4: Collect the final UC-bound properties

```rust,ignore
use delta_kernel_unity_catalog::get_final_required_properties_for_uc;

let uc_props = get_final_required_properties_for_uc(&post_commit_snapshot, &engine)?;
```

The returned map contains:

- Every entry from the table's metadata configuration, including
  `io.unitycatalog.tableId`, `delta.enableInCommitTimestamps=true`, and any
  user-supplied custom properties.
- `delta.minReaderVersion` and `delta.minWriterVersion`.
- `delta.feature.<name>=supported` for every reader and writer feature on the
  protocol (for a freshly created UC table this is at least `catalogManaged`,
  `vacuumProtocolCheck`, and `inCommitTimestamp`).
- `delta.lastUpdateVersion=0`.
- `delta.lastCommitTimestamp` set to the in-commit timestamp of version 0.
- `clusteringColumns` as a JSON array of logical column paths, if the table is
  clustered.

> [!NOTE]
> `get_final_required_properties_for_uc` requires a version 0 snapshot with an
> in-commit timestamp. The `post_commit_snapshot` from Step 3 satisfies both.

## Step 5: Finalize the table in Unity Catalog

```rust,ignore
// TODO: not yet exposed by `unity-catalog-delta-rest-client`. Call through
// your connector's own UC client.
my_uc_client.create_table(&table_id, uc_props).await?;
```

## Clustered tables

Chain `with_data_layout` on the create-table builder:

```rust,ignore
use delta_kernel::transaction::data_layout::DataLayout;

let create_txn = create_table(table_uri.as_str(), Arc::new(schema), "MyApp/1.0")
    .with_table_properties(disk_props)
    .with_data_layout(DataLayout::clustered(["region"]))
    .build(&engine, committer)?;
```

`get_final_required_properties_for_uc` adds a `clusteringColumns` entry (a
JSON array of logical column paths) to its output when clustering is enabled.

## What's next

- [Writing to UC Tables](./writing.md) for the version >= 1 write flow that
  follows table creation.
- [Reading UC Tables](./reading.md) for the read flow.
- [Creating a Table](../writing/create_table.md) for the generic Kernel
  create-table APIs.

## See also

- [Catalog-Managed Tables: Overview](../catalog_managed/overview.md) for
  staged / ratified / published commit terminology.
- [Implementing a Catalog Committer](../catalog_managed/committer.md) for the
  `Committer` trait behind `UCCommitter`.
