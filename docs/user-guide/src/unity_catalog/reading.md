# Reading Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-rest-client -->

To read a Unity Catalog-managed Delta table, you load the table's metadata and
inline commits through the UC REST API, fetch temporary storage credentials,
build a Snapshot from those commits, and then build a Scan exactly as you would
for a filesystem-managed table.

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
delta_kernel = "..."
delta_kernel_default_engine = { version = "...", features = ["rustls"] }
delta-kernel-unity-catalog = "..."
unity-catalog-delta-rest-client = "..."
unity-catalog-delta-client-api = "..."
url = "2"
tokio = { version = "1", features = ["full"] }
```

Use `rustls` on `delta_kernel_default_engine` for a pure-Rust TLS stack or `native-tls`
to link against the system's TLS implementation. You need
`unity-catalog-delta-client-api` directly to import types like `Operation`, since
the REST client crate does not re-export them.

## Step 1: Build the UC client

The read path uses a single `UCClient`, built from a `ClientConfig`:

```rust,ignore
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};

let config = ClientConfig::build(&endpoint, &token).build()?;
let uc_client = UCClient::new(config)?;
```

The `endpoint` is your UC workspace URL (e.g. `"my-workspace.cloud.databricks.com"`),
and `token` is a valid authentication token.

## Step 2: Load the table

Call `load_table` with the catalog, schema, and table names. A single call
returns the table's metadata, any inline unpublished commits, and the latest
catalog-known version.

```rust,ignore
let resp = uc_client.load_table("my_catalog", "my_schema", "my_table").await?;
let table_uri = &resp.metadata.location;
let latest_version = resp.latest_table_version.unwrap_or_default() as u64;
```

`resp.metadata.location` points to the table's root directory in cloud storage.
`resp.commits` holds the inline unpublished commits used to build the log tail
in Step 5, and `resp.latest_table_version` is the highest version the catalog
knows about.

## Step 3: Fetch temporary credentials

UC vends short-lived cloud storage credentials scoped to the table's storage
location. For a read operation, request `Operation::Read`:

```rust,ignore
use unity_catalog_delta_client_api::Operation;

let creds = uc_client
    .get_table_credentials("my_catalog", "my_schema", "my_table", Operation::Read)
    .await?;
let cred = creds.storage_credentials.into_iter().next()
    .ok_or("No storage credentials in response")?;
```

The returned `CredentialsResponse` holds a list of `StorageCredential` entries.
Each entry carries a storage `prefix` it applies to and a `config` map of
provider-specific keys (for AWS, `s3.access-key-id`, `s3.secret-access-key`, and
`s3.session-token`).

> [!WARNING]
> Vended credentials expire. `StorageCredential` provides `expiration_time_ms`,
> `is_expired()`, and `time_until_expiry()` to check validity. If your scan
> takes longer than the credential lifetime, refresh credentials and rebuild the
> Engine before continuing. See
> [Credential refresh](#credential-refresh-for-long-running-operations) below.

## Step 4: Build an Engine with vended credentials

Pass the temporary credentials as storage options when constructing the object
store, then wrap it in a `DefaultEngineBuilder`:

```rust,ignore
use std::sync::Arc;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel::object_store;

let table_url = url::Url::parse(table_uri)?;
let options = [
    ("region", "us-west-2"),
    ("access_key_id", &cred.config["s3.access-key-id"]),
    ("secret_access_key", &cred.config["s3.secret-access-key"]),
    ("session_token", &cred.config["s3.session-token"]),
];
let (store, _path) = object_store::parse_url_opts(&table_url, options)?;
let engine = DefaultEngineBuilder::new(store.into()).build();
```

The `.into()` converts the `Box<dyn ObjectStore>` returned by `parse_url_opts`
into the `Arc<dyn ObjectStore>` that `DefaultEngineBuilder::new` expects. Set
the `region` to match the bucket's actual AWS region.

## Step 5: Build a Snapshot from the inline commits

Convert the inline commits from Step 2 into a log tail with
`log_tail_from_commits`, then pass it to `Snapshot::builder_for` along with the
latest catalog version. Kernel uses the log tail to build a
[Snapshot](../catalog_managed/reading.md) that includes commits not yet
published to `_delta_log/`.

```rust,ignore
use delta_kernel::Snapshot;
use delta_kernel_unity_catalog::log_tail_from_commits;

let table_url = url::Url::parse(table_uri)?;
let log_tail = log_tail_from_commits(&resp.commits, &table_url)?;
let snapshot = Snapshot::builder_for(table_url)
    .with_log_tail(log_tail)
    .with_max_catalog_version(latest_version)
    .build(&engine)?;

println!("Table version: {}", snapshot.version());
```

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

use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel::object_store;
use delta_kernel::Snapshot;
use delta_kernel_unity_catalog::log_tail_from_commits;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build the UC client
    let config = ClientConfig::build(&endpoint, &token).build()?;
    let uc_client = UCClient::new(config)?;

    // 2. Load the table (metadata + inline commits + latest version)
    let resp = uc_client.load_table("my_catalog", "my_schema", "my_table").await?;
    let table_uri = &resp.metadata.location;
    let latest_version = resp.latest_table_version.unwrap_or_default() as u64;

    // 3. Fetch temporary credentials
    let creds = uc_client
        .get_table_credentials("my_catalog", "my_schema", "my_table", Operation::Read)
        .await?;
    let cred = creds.storage_credentials.into_iter().next()
        .ok_or("No storage credentials")?;

    // 4. Build an Engine with the vended credentials
    let table_url = url::Url::parse(table_uri)?;
    let (store, _) = object_store::parse_url_opts(&table_url, [
        ("region", "us-west-2"),
        ("access_key_id", &cred.config["s3.access-key-id"]),
        ("secret_access_key", &cred.config["s3.secret-access-key"]),
        ("session_token", &cred.config["s3.session-token"]),
    ])?;
    let engine = DefaultEngineBuilder::new(store.into()).build();

    // 5. Build the Snapshot from the inline commits
    let log_tail = log_tail_from_commits(&resp.commits, &table_url)?;
    let snapshot = Snapshot::builder_for(table_url)
        .with_log_tail(log_tail)
        .with_max_catalog_version(latest_version)
        .build(&engine)?;
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
table scans, streaming reads), check `cred.is_expired()` or
`cred.time_until_expiry()` before starting a new phase of work.

If credentials have expired, call `get_table_credentials` again and rebuild the
Engine with the fresh credentials before continuing. Kernel does not manage
credential lifecycle for you.

## What's next

- [Writing to UC Tables](./writing.md): how to append data and commit through
  Unity Catalog
- [Building a Scan](../reading/building_a_scan.md): scan configuration options
  that apply to both filesystem and catalog-managed tables
- [Filter Pushdown and File Skipping](../reading/filter_pushdown.md): reduce
  the data you read with predicates
