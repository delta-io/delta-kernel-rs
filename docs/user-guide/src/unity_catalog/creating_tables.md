# Creating Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-rest-client, unity-catalog-delta-client-api -->

To create a new Unity Catalog-managed Delta table, you register the table with
your UC server to obtain a table ID and storage location, write a version 0
Delta log commit with the required catalog-managed properties, and then send a
second set of properties back to UC to finalize registration.

Before reading this page, make sure you understand
[Creating a Table](../writing/create_table.md) and the
[Unity Catalog Integration overview](./overview.md).

> [!NOTE]
> Steps 1 and 5 call UC endpoints that the Rust `unity-catalog-delta-rest-client`
> crate does not yet expose. Route those through your connector's own UC client;
> the request and response wire types live in `unity-catalog-delta-client-api`.

## Prerequisites

- A three-part table name, Delta schema, and target storage location.
- A connector-owned UC client that can call UC's staging and create-table
  endpoints.

## Step 1: Reserve the table in Unity Catalog

POST a `CreateStagingTableRequest` to the UC `staging-tables` endpoint. UC allocates a table ID
and storage location and returns temporary credentials for the initial commit.

```rust,ignore
use unity_catalog_delta_client_api::{CreateStagingTableRequest, CreateStagingTableResponse};

// The `staging-tables` POST is not yet exposed as a method on `UCClient`. Send the request
// through your connector's own UC HTTP client; the request and response wire types live in
// `unity-catalog-delta-client-api`.
let staging_req = CreateStagingTableRequest { name: "my_table".to_string() };
let staging_info: CreateStagingTableResponse = my_uc_client.post_staging_table(staging_req).await?;
let table_id = staging_info.table_id;
let table_uri = staging_info.location;
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
use unity_catalog_delta_client_api::TableName;
use unity_catalog_delta_rest_client::{ClientConfig, UCUpdateTableRestClient};

let config = ClientConfig::build(&endpoint, &token).build()?;
let update_client = Arc::new(UCUpdateTableRestClient::new(config)?);

// Build the engine over the staging storage location using the staging credentials
// (`staging_info.storage_credentials`). `build_engine_with_credentials` is a connector-owned
// helper; see Step 4 of [Reading UC Tables](./reading.md#step-4-build-an-engine-with-vended-credentials).
let engine = build_engine_with_credentials(&table_uri, &staging_info.storage_credentials)?;

// Build the create-table transaction with the disk-bound properties. `UCCommitter::new` takes the
// commit client plus the table's UC-assigned id and its three-part name.
let committer = Box::new(UCCommitter::new(
    update_client.clone(),
    table_id.clone(),
    TableName::new("main", "default", "my_table"),
));
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
directly and skips the `update_table` API.

## Step 4: Build the create-table request for UC

`build_uc_create_table_request` reads the post-commit version 0 snapshot and produces a typed
`CreateTableRequest` to send to UC's create-table endpoint. Each part of the request maps to a
distinct typed field, so the same information is never duplicated across fields:

```rust,ignore
use delta_kernel_unity_catalog::build_uc_create_table_request;

let create_req = build_uc_create_table_request(&post_commit_snapshot, &engine, "my_table")?;
```

- `columns` carries the serialized table schema, and `partition_columns` the partition column names.
- `protocol` is a typed field holding the min reader and writer versions and the reader and writer
  feature names (for a freshly created UC table this is at least `catalogManaged`,
  `vacuumProtocolCheck`, and `inCommitTimestamp`). Features are not flattened into `properties`.
- `domain_metadata` carries the `delta.clustering` and `delta.rowTracking` domains verbatim when
  present. UC ignores any other domain.
- `properties` is the table's metadata configuration as-is, such as `io.unitycatalog.tableId`,
  `delta.enableInCommitTimestamps`, and any user-supplied custom properties. Protocol and clustering
  data live in their own fields above, not here.

> [!NOTE]
> `build_uc_create_table_request` requires a version 0 snapshot with an in-commit timestamp. The
> `post_commit_snapshot` from Step 3 satisfies both.
>
> If your UC server requires `delta.checkpointPolicy=v2` for `v2Checkpoint` tables, insert it into
> `create_req.properties` before sending: kernel enables the `v2Checkpoint` feature without writing
> the companion property.

## Step 5: Finalize the table in Unity Catalog

POST the `CreateTableRequest` to UC's create-table (`tables`) endpoint to register the table.

```rust,ignore
// The create-table POST is not yet exposed as a method on `UCClient`. Send the typed
// `CreateTableRequest` through your connector's own UC HTTP client.
my_uc_client.post_create_table(create_req).await?;
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

`build_uc_create_table_request` forwards the committed `delta.clustering` domain verbatim into the
request's domain metadata. Its `clusteringColumns` paths are physical column names when column
mapping is enabled, matching what the table committed.

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
