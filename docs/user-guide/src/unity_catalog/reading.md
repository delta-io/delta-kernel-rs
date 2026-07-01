# Reading Unity Catalog tables

<!-- Page type: How-to -->
<!-- Crates: delta-kernel-unity-catalog, unity-catalog-delta-rest-client -->

To read a Unity Catalog-managed Delta table, you load the table through the UC
Delta-Tables API, fetch temporary storage credentials, build a Snapshot from the
catalog's log tail, and then build a Scan exactly as you would for a
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
delta_kernel = "..."
delta_kernel_default_engine = { version = "...", features = ["rustls"] }
delta-kernel-unity-catalog = "..."
unity-catalog-delta-rest-client = "..."
unity-catalog-delta-client-api = "..."
url = "2"
tokio = { version = "1", features = ["full"] }
```

Use `rustls` on `delta_kernel_default_engine` for a pure-Rust TLS stack or
`native-tls` to link against the system's TLS implementation. You need
`unity-catalog-delta-client-api` directly to import types like `Operation`, since
the REST client crate does not re-export them.

## Step 1: Build the UC client

`UCClient` calls the UC Delta-Tables API. It shares a `ClientConfig` with the
rest of the UC integration.

```rust,ignore
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};

let config = ClientConfig::build(&endpoint, &token).build()?;
let uc_client = UCClient::new(config)?;
```

The `endpoint` is your UC workspace URL (e.g. `"my-workspace.cloud.databricks.com"`),
and `token` is a valid authentication token.

## Step 2: Load the table

Call `load_table` with the three-part table name split into catalog, schema, and
table. A single call returns the table's metadata (including its storage
`location`), the inline unpublished commits that form the log tail, and the
latest version the catalog has ratified.

```rust,ignore
let resp = uc_client
    .load_table("my_catalog", "my_schema", "my_table")
    .await?;
let table_uri = url::Url::parse(&resp.metadata.location)?;
```

The `LoadTableResponse` also carries `commits` (the log tail, covered in Step 5)
and `latest_table_version` (the catalog's current ratified version).

## Step 3: Fetch temporary credentials

UC vends short-lived cloud storage credentials scoped to the table's storage
location. For a read operation, request `Operation::Read`:

```rust,ignore
use unity_catalog_delta_client_api::Operation;

let creds = uc_client
    .get_table_credentials("my_catalog", "my_schema", "my_table", Operation::Read)
    .await?;
```

The returned `CredentialsResponse` holds a list of `StorageCredential`s, each
scoped to a storage `prefix`. Every credential carries a `config` map whose keys
are provider-namespaced (`s3.access-key-id`, `azure.sas-token`,
`gcs.oauth-token`, and so on), so the same response shape works across clouds.

> [!WARNING]
> Vended credentials can expire. Each `StorageCredential` carries an
> `expiration_time_ms` (or `None` when the server omits it, as it does for local
> `file://` and static credentials). If your scan outlives the credential
> lifetime, you need to fetch fresh credentials and rebuild the Engine before
> continuing. See [Credential refresh](#credential-refresh-for-long-running-operations)
> below.

## Step 4: Build an Engine with vended credentials

The `config` map matches the option shape `object_store` expects. Translate the
provider-namespaced keys into the `object_store` option names, then wrap the
resulting store in a `DefaultEngineBuilder`. Mapping the keys is connector-owned
work, because only your connector knows which cloud it targets.

```rust,ignore
use std::sync::Arc;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel::object_store;

// Map the vended `config` keys to object_store option names.
let mut options: Vec<(String, String)> = Vec::new();
for cred in &creds.storage_credentials {
    for (key, value) in &cred.config {
        let mapped = match key.as_str() {
            "s3.access-key-id" => "access_key_id",
            "s3.secret-access-key" => "secret_access_key",
            "s3.session-token" => "session_token",
            other => other,
        };
        options.push((mapped.to_string(), value.clone()));
    }
}
options.push(("region".to_string(), "us-west-2".to_string()));

let (store, _path) = object_store::parse_url_opts(&table_uri, options)?;
let engine = DefaultEngineBuilder::new(store.into()).build();
```

The `.into()` converts the `Box<dyn ObjectStore>` returned by `parse_url_opts`
into the `Arc<dyn ObjectStore>` that `DefaultEngineBuilder::new` expects. Set the
`region` to match the bucket's actual AWS region.

## Step 5: Build a Snapshot from the load_table response

The `load_table` response already contains the log tail. Pass it to
`snapshot_builder_from_load_table` to get a `SnapshotBuilder` with the log tail
and max catalog version already applied, then call `build`.

```rust,ignore
use delta_kernel_unity_catalog::snapshot_builder_from_load_table;

let snapshot = snapshot_builder_from_load_table(&resp)?.build(&engine)?;

println!("Table version: {}", snapshot.version());
```

The builder pins the Snapshot to the max catalog version, so it reflects commits
that haven't been published to `_delta_log/` yet. To read an earlier version, add
`.at_version(5)` before `.build()`. The requested version must not exceed the max
catalog version.

> [!NOTE]
> If you need the log tail directly (for example, to assemble the builder
> yourself), use the lower-level `log_tail_from_commits`, which converts the
> response's `commits` into a `Vec<LogPath>`.

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

This example puts all the steps together. It connects to Unity Catalog, loads a
table, fetches credentials, builds a Snapshot from the log tail, and reads the
data.

```rust,ignore
use std::sync::Arc;

use delta_kernel::object_store;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_unity_catalog::snapshot_builder_from_load_table;
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build the UC client
    let config = ClientConfig::build(&endpoint, &token).build()?;
    let uc_client = UCClient::new(config)?;

    // 2. Load the table (metadata + inline log tail)
    let resp = uc_client
        .load_table("my_catalog", "my_schema", "my_table")
        .await?;
    let table_uri = url::Url::parse(&resp.metadata.location)?;

    // 3. Fetch temporary read credentials
    let creds = uc_client
        .get_table_credentials("my_catalog", "my_schema", "my_table", Operation::Read)
        .await?;

    // 4. Build an Engine from the vended credentials
    let mut options: Vec<(String, String)> = Vec::new();
    for cred in &creds.storage_credentials {
        for (key, value) in &cred.config {
            let mapped = match key.as_str() {
                "s3.access-key-id" => "access_key_id",
                "s3.secret-access-key" => "secret_access_key",
                "s3.session-token" => "session_token",
                other => other,
            };
            options.push((mapped.to_string(), value.clone()));
        }
    }
    options.push(("region".to_string(), "us-west-2".to_string()));
    let (store, _) = object_store::parse_url_opts(&table_uri, options)?;
    let engine = DefaultEngineBuilder::new(store.into()).build();

    // 5. Build a Snapshot from the load_table response
    let snapshot = snapshot_builder_from_load_table(&resp)?.build(&engine)?;
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
table scans, streaming reads), check each credential's `expiration_time_ms`
before starting a new phase of work.

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
