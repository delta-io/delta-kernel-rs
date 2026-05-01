# Reading Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-rest-client -->

To read a Unity Catalog-managed Delta table, you resolve the table name through
the UC REST API, fetch temporary storage credentials, load a Snapshot through
the `UCKernelClient`, and then build a Scan exactly as you would for a
filesystem-managed table.

Before reading this page, make sure you understand
[Catalog-Managed Tables](../catalog_managed/overview.md) and the
[Unity Catalog Integration overview](./overview.md).

> [!NOTE]
> This example uses the `delta-kernel-unity-catalog` and
> `unity-catalog-delta-rest-client` crates, which are not part of the
> `delta_kernel` crate itself. Add them as dependencies alongside `delta_kernel`.

## Dependencies

Add the following to your `Cargo.toml`:

```toml
[dependencies]
delta_kernel = { version = "...", features = ["default-engine-rustls"] }
delta-kernel-unity-catalog = "..."
unity-catalog-delta-rest-client = "..."
unity-catalog-delta-client-api = "..."
url = "2"
tokio = { version = "1", features = ["full"] }
```

Use `default-engine-rustls` for a pure-Rust TLS stack or `default-engine-native-tls`
to link against the system's TLS implementation. You need
`unity-catalog-delta-client-api` directly to import types like `Operation`, since
the REST client crate does not re-export them.

## Step 1: Build the UC clients

The UC integration uses two clients that share a `ClientConfig`:

- `UCClient` handles table resolution (`get_table`) and credential vending
  (`get_credentials`).
- `UCCommitsRestClient` implements the `GetCommitsClient` trait, which
  `UCKernelClient` uses to fetch ratified commits from the catalog.

```rust,ignore
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCCommitsRestClient};

let config = ClientConfig::build(&endpoint, &token).build()?;
let uc_client = UCClient::new(config.clone())?;
let commits_client = UCCommitsRestClient::new(config)?;
```

The `endpoint` is your UC workspace URL (e.g. `"my-workspace.cloud.databricks.com"`),
and `token` is a valid authentication token.

## Step 2: Resolve the table name

Call `get_table` with the three-level table name to get the table's storage
location and table ID. The `table_id` identifies the table in UC's commits API,
and the `storage_location` points to the table's root directory in cloud
storage.

```rust,ignore
let table_info = uc_client.get_table("my_catalog.my_schema.my_table").await?;
let table_id = &table_info.table_id;
let table_uri = &table_info.storage_location;
```

The `TablesResponse` also includes metadata like `catalog_name`, `schema_name`,
`data_source_format`, and `table_type`. You can verify the table is a Delta
table by calling `table_info.is_delta_table()`.

## Step 3: Fetch temporary credentials

UC vends short-lived cloud storage credentials scoped to the table's storage
location. For a read operation, request `Operation::Read`:

```rust,ignore
use unity_catalog_delta_client_api::Operation;

let creds = uc_client.get_credentials(table_id, Operation::Read).await?;
```

The returned `TemporaryTableCredentials` contains cloud-provider-specific
credentials. For AWS, extract the temporary credentials:

```rust,ignore
let aws_creds = creds.aws_temp_credentials
    .ok_or("No AWS temporary credentials in response")?;
```

> [!WARNING]
> Vended credentials expire. The `TemporaryTableCredentials` struct provides
> `expiration_time`, `is_expired()`, and `time_until_expiry()` to check
> validity. If your scan takes longer than the credential lifetime, you need to
> refresh credentials and rebuild the Engine before continuing. See
> [Credential refresh](#credential-refresh-for-long-running-operations) below.

> [!NOTE]
> Today `TemporaryTableCredentials` only exposes `aws_temp_credentials`. Azure
> and GCP credential vending are tracked in
> [delta-io/delta-kernel-rs#2434](https://github.com/delta-io/delta-kernel-rs/issues/2434).

## Step 4: Build an Engine with vended credentials

Pass the temporary credentials as storage options when constructing the object
store, then wrap it in a `DefaultEngineBuilder`:

```rust,ignore
use std::sync::Arc;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store;

let table_url = url::Url::parse(table_uri)?;
let options = [
    ("region", "us-west-2"),
    ("access_key_id", &aws_creds.access_key_id),
    ("secret_access_key", &aws_creds.secret_access_key),
    ("session_token", &aws_creds.session_token),
];
let (store, _path) = object_store::parse_url_opts(&table_url, options)?;
let engine = DefaultEngineBuilder::new(store.into()).build();
```

The `.into()` converts the `Box<dyn ObjectStore>` returned by `parse_url_opts`
into the `Arc<dyn ObjectStore>` that `DefaultEngineBuilder::new` expects. Set
the `region` to match the bucket's actual AWS region.

## Step 5: Load a Snapshot through UCKernelClient

`UCKernelClient` wraps a `GetCommitsClient` and handles the full snapshot
loading flow: it calls the UC commits API to get the latest ratified commits,
translates them into `LogPath` entries, and builds a
[Snapshot](../catalog_managed/reading.md) with the log tail.

```rust,ignore
use delta_kernel_unity_catalog::UCKernelClient;

let catalog = UCKernelClient::new(&commits_client);
let snapshot = catalog.load_snapshot(table_id, table_uri, &engine).await?;

println!("Table version: {}", snapshot.version());
```

To read a specific version, use `load_snapshot_at`:

```rust,ignore
let snapshot = catalog.load_snapshot_at(table_id, table_uri, 5, &engine).await?;
```

The requested version must not exceed the latest version the catalog has
ratified. If it does, `load_snapshot_at` returns an error.

## Step 6: Build and execute a Scan

From this point on, reading works identically to a filesystem-managed table.
Build a Scan from the Snapshot and iterate over the results:

```rust,ignore
let scan = snapshot.scan_builder().build()?;
for data in scan.execute(Arc::new(engine))? {
    let batch = data?;
    // process each batch of data
}
```

You can use all the same Scan features: [column selection](../reading/column_selection.md),
[filter pushdown](../reading/filter_pushdown.md), and
[scan metadata](../reading/scan_metadata.md) for distributed reads.

## Complete example

This example puts all the steps together. It connects to Unity Catalog,
resolves a table, fetches credentials, loads a Snapshot, and reads the data.

```rust,ignore
use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store;
use delta_kernel_unity_catalog::UCKernelClient;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCCommitsRestClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build UC clients
    let config = ClientConfig::build(&endpoint, &token).build()?;
    let uc_client = UCClient::new(config.clone())?;
    let commits_client = UCCommitsRestClient::new(config)?;

    // 2. Resolve the table name
    let table_info = uc_client.get_table("my_catalog.my_schema.my_table").await?;
    let table_id = &table_info.table_id;
    let table_uri = &table_info.storage_location;

    // 3. Fetch temporary credentials
    let creds = uc_client.get_credentials(table_id, Operation::Read).await?;
    let aws_creds = creds.aws_temp_credentials
        .ok_or("No AWS temporary credentials")?;

    // 4. Build an Engine with the vended credentials
    let table_url = url::Url::parse(table_uri)?;
    let (store, _) = object_store::parse_url_opts(&table_url, [
        ("region", "us-west-2"),
        ("access_key_id", &aws_creds.access_key_id),
        ("secret_access_key", &aws_creds.secret_access_key),
        ("session_token", &aws_creds.session_token),
    ])?;
    let engine = DefaultEngineBuilder::new(store.into()).build();

    // 5. Load the Snapshot via UCKernelClient
    let catalog = UCKernelClient::new(&commits_client);
    let snapshot = catalog.load_snapshot(table_id, table_uri, &engine).await?;
    println!("Loaded table at version {}", snapshot.version());

    // 6. Build and execute the Scan
    let scan = snapshot.scan_builder().build()?;
    for data in scan.execute(Arc::new(engine))? {
        let batch = data?;
        // process each batch of data
    }

    Ok(())
}
```

## Credential refresh for long-running operations

UC vends temporary credentials with a limited lifetime. For short-lived scans,
you don't need to worry about expiration. For long-running operations (large
table scans, streaming reads), check `creds.is_expired()` or
`creds.time_until_expiry()` before starting a new phase of work.

If credentials have expired, call `get_credentials` again and rebuild the
Engine with the fresh credentials before continuing. Kernel does not manage
credential lifecycle for you.

## What's next

- [Writing to UC Tables](./writing.md): how to append data and commit through
  Unity Catalog
- [Building a Scan](../reading/building_a_scan.md): scan configuration options
  that apply to both filesystem and catalog-managed tables
- [Filter Pushdown and File Skipping](../reading/filter_pushdown.md): reduce
  the data you read with predicates
