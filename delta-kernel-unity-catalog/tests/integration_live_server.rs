//! Live end-to-end tests against a running Unity Catalog server.
//!
//! These are gated behind the `integration-test` feature so they are never compiled or run by a
//! normal `cargo nextest`. Even with the feature enabled, each test skips (passes as a no-op) when
//! `UC_SERVER_URL` is unset, so a developer can build the suite without a server on hand.
//!
//! Run against a local UC OSS server:
//! ```bash
//! UC_SERVER_URL=http://localhost:8080 UC_TOKEN=not-used \
//!   cargo nextest run -p delta-kernel-unity-catalog --features integration-test
//! ```
//!
//! Optional overrides for the read-path test: `UC_TEST_CATALOG`, `UC_TEST_SCHEMA`,
//! `UC_TEST_TABLE` (the read test skips unless `UC_TEST_TABLE` is set).
//!
//! The mutating tests are double-gated: `live_alter_set_table_property` requires `UC_ALTER=1` and
//! `live_create_table` requires `UC_CREATE=1` (each also skips with an eprintln otherwise). Point
//! them only at throwaway tables/schemas.
#![cfg(feature = "integration-test")]

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::storage::store_from_url_opts;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_unity_catalog::{
    build_uc_create_table_request, log_tail_from_commits, normalize_table_root, UCCommitter,
};
use test_utils::{insert_data_with, read_scan};
use unity_catalog_delta_client_api::{
    CreateStagingTableRequest, CreateStagingTableResponse, LoadTableResponse, Operation,
    StorageCredential,
};
use unity_catalog_delta_rest_client::{ClientConfig, UCClient, UCUpdateTableRestClient};
use url::Url;

/// Returns `(server_url, token)` from the environment, or `None` to signal the caller to skip.
/// `UC_TOKEN` defaults to a dummy value since a dev-mode server runs with auth disabled.
fn server_env() -> Option<(String, String)> {
    let url = std::env::var("UC_SERVER_URL").ok()?;
    let token = std::env::var("UC_TOKEN").unwrap_or_else(|_| "not-used".to_string());
    Some((url, token))
}

/// Builds a `ClientConfig`, applying `UC_USER_AGENT` when set. Some catalogs require a specific
/// `User-Agent` value and reject others; others ignore the header entirely.
fn client_config(url: &str, token: &str) -> ClientConfig {
    let mut builder = ClientConfig::build(url, token);
    if let Ok(user_agent) = std::env::var("UC_USER_AGENT") {
        builder = builder.with_user_agent(user_agent);
    }
    builder.build().expect("failed to build ClientConfig")
}

fn client(url: &str, token: &str) -> UCClient {
    UCClient::new(client_config(url, token)).expect("failed to build UCClient")
}

/// Default `User-Agent` when `UC_USER_AGENT` is unset. Derived from the crate version to match the
/// production default.
const DEFAULT_USER_AGENT: &str = concat!(
    "Delta/",
    env!("CARGO_PKG_VERSION"),
    " delta-kernel-rs/",
    env!("CARGO_PKG_VERSION")
);

/// Returns the base URL (`<workspace>/api/2.1/unity-catalog/delta/v1/`) plus an authed
/// `reqwest::Client` for the hand-rolled staging-tables/tables POSTs.
fn raw_delta_rest_client(url: &str, token: &str) -> (Url, reqwest::Client) {
    let base = ClientConfig::build(url, token)
        .build()
        .expect("failed to build ClientConfig")
        .workspace_url
        .join("delta/v1/")
        .expect("failed to join delta/v1/ onto workspace URL");
    let user_agent =
        std::env::var("UC_USER_AGENT").unwrap_or_else(|_| DEFAULT_USER_AGENT.to_string());

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("invalid bearer token"),
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        reqwest::header::USER_AGENT,
        reqwest::header::HeaderValue::from_str(&user_agent).expect("invalid user agent"),
    );
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .expect("failed to build reqwest client");
    (base, http)
}

/// Maps vended storage credentials into `object_store` option keys so the table's cloud store can
/// be constructed. Currently handles the AWS S3 credential keys; pass-through for anything else
/// (e.g. region), plus a `UC_AWS_REGION` fallback since vended creds may omit the region. Shared by
/// the ALTER path (`CredentialsResponse.storage_credentials`) and the CREATE path
/// (`CreateStagingTableResponse.storage_credentials`).
fn object_store_options(creds: &[StorageCredential]) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    for cred in creds {
        for (key, value) in &cred.config {
            let mapped = match key.as_str() {
                "s3.access-key-id" => "access_key_id",
                "s3.secret-access-key" => "secret_access_key",
                "s3.session-token" => "session_token",
                // Pass keys we don't remap (e.g. region) through unchanged; intentional, test-only.
                other => other,
            };
            opts.insert(mapped.to_string(), value.clone());
        }
    }
    if let Ok(region) = std::env::var("UC_AWS_REGION") {
        opts.insert("region".to_string(), region);
    }
    opts
}

/// Builds a `Snapshot` from a `load_table` response: normalize the location, build the log tail
/// from the inline commits, and pin the snapshot to the catalog's max version. Returns the
/// normalized table root alongside the snapshot.
fn snapshot_from_load_table(resp: &LoadTableResponse, engine: &dyn Engine) -> (Url, Arc<Snapshot>) {
    let root = normalize_table_root(
        &Url::parse(&resp.metadata.location).expect("location is not a valid URL"),
    );
    let max_version: u64 = resp
        .latest_table_version
        .unwrap_or(0)
        .try_into()
        .expect("latest_table_version does not fit u64");
    let log_tail = log_tail_from_commits(&resp.commits, &root).expect("log tail");
    let snapshot = Snapshot::builder_for(root.clone())
        .with_log_tail(log_tail)
        .with_max_catalog_version(max_version)
        .build(engine)
        .expect("failed to build snapshot from load_table response");
    (root, snapshot)
}

/// `/config` handshake round-trips: the server echoes a protocol version and a non-empty endpoint
/// set for the catalog. This is the cheapest proof that our URL shaping, query params, and response
/// deserialization match the server contract.
#[tokio::test(flavor = "multi_thread")]
async fn live_get_config_round_trips() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_get_config_round_trips");
        return;
    };
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());

    let resp = client(&url, &token)
        .get_config(&catalog, &["1.0"])
        .await
        .expect("get_config failed");

    assert!(
        !resp.protocol_version.is_empty(),
        "expected a protocol version from the server"
    );
    assert!(
        !resp.endpoints.is_empty(),
        "expected the server to advertise at least one endpoint"
    );
}

/// `load_table` returns parseable metadata + commits, and the commits convert to a kernel log tail.
/// Skips unless `UC_TEST_TABLE` names an existing managed delta table. This validates the read-path
/// wire types without requiring storage credentials (it does not read data files).
#[tokio::test(flavor = "multi_thread")]
async fn live_load_table_builds_log_tail() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_load_table_builds_log_tail");
        return;
    };
    let Some(table) = std::env::var("UC_TEST_TABLE").ok() else {
        eprintln!("UC_TEST_TABLE unset; skipping live_load_table_builds_log_tail");
        return;
    };
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());
    let schema = std::env::var("UC_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string());

    let resp = client(&url, &token)
        .load_table(&catalog, &schema, &table)
        .await
        .expect("load_table failed");

    assert!(
        !resp.metadata.table_uuid.is_empty(),
        "expected a table_uuid in the load_table response"
    );
    let table_url = Url::parse(&resp.metadata.location).expect("location is not a valid URL");

    // Pure transform (no I/O): proves the commit wire shape maps onto kernel log paths.
    let log_tail = log_tail_from_commits(&resp.commits, &table_url)
        .expect("log_tail_from_commits failed on the server's commit list");
    assert_eq!(
        log_tail.len(),
        resp.commits.len(),
        "every returned commit should map to a log path"
    );
}

/// `get_table_credentials` vends temporary storage credentials for an existing table. Validates the
/// credential-vending endpoint and its wire types (read-only; does not mutate the table). Skips
/// unless `UC_TEST_TABLE` names an existing table.
#[tokio::test(flavor = "multi_thread")]
async fn live_get_table_credentials() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_get_table_credentials");
        return;
    };
    let Some(table) = std::env::var("UC_TEST_TABLE").ok() else {
        eprintln!("UC_TEST_TABLE unset; skipping live_get_table_credentials");
        return;
    };
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());
    let schema = std::env::var("UC_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string());

    let creds = client(&url, &token)
        .get_table_credentials(&catalog, &schema, &table, Operation::Read)
        .await
        .expect("get_table_credentials failed");

    assert!(
        !creds.storage_credentials.is_empty(),
        "expected at least one vended storage credential"
    );
    for cred in &creds.storage_credentials {
        assert!(
            !cred.prefix.is_empty(),
            "vended credential missing a prefix"
        );
        assert!(
            !cred.config.is_empty(),
            "vended credential missing cloud config (e.g. s3.access-key-id)"
        );
    }
}

/// Live ALTER through the full kernel path: load the table, build a `Snapshot` over its real cloud
/// storage (via vended credentials), set a table property through `UCCommitter`, then reload and
/// assert the property landed in the catalog. Exercises `alter_table` -> intent capture ->
/// `derive_updates` -> real `update_table` `SetProperties`.
///
/// This mutates the table, so it is double-gated: requires `UC_TEST_TABLE` and `UC_ALTER=1`. Point
/// it only at a throwaway table.
#[tokio::test(flavor = "multi_thread")]
async fn live_alter_set_table_property() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_alter_set_table_property");
        return;
    };
    if std::env::var("UC_ALTER").is_err() {
        eprintln!("UC_ALTER unset; skipping mutating live_alter_set_table_property");
        return;
    }
    let Some(table) = std::env::var("UC_TEST_TABLE").ok() else {
        eprintln!("UC_TEST_TABLE unset; skipping live_alter_set_table_property");
        return;
    };
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());
    let schema = std::env::var("UC_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string());

    let config = client_config(&url, &token);
    let uc = UCClient::new(config.clone()).expect("failed to build UCClient");

    // 1. Current table state: location, uuid, version watermark, inline commits.
    let resp = uc
        .load_table(&catalog, &schema, &table)
        .await
        .expect("load_table failed");
    let table_uuid = resp.metadata.table_uuid.clone();
    let max_version: u64 = resp
        .latest_table_version
        .unwrap_or(0)
        .try_into()
        .expect("latest_table_version does not fit u64");

    // 2. Vend read-write credentials and build an engine over the table's cloud storage.
    let creds = uc
        .get_table_credentials(&catalog, &schema, &table, Operation::ReadWrite)
        .await
        .expect("get_table_credentials failed");
    let table_url = normalize_table_root(
        &Url::parse(&resp.metadata.location).expect("location is not a valid URL"),
    );
    let store = store_from_url_opts(&table_url, object_store_options(&creds.storage_credentials))
        .expect("failed to build object store from vended credentials");
    let engine = DefaultEngineBuilder::new(store).build();

    // 3. Snapshot of the current table (reads the log from cloud storage).
    let (_, snapshot) = snapshot_from_load_table(&resp, &engine);

    // 4. ALTER: set a property, committed through the UC committer (real update_table
    //    SetProperties).
    let key = "user.kernel_rs_live_test";
    let value = format!("v{max_version}");
    let update_client = Arc::new(UCUpdateTableRestClient::new(config).expect("update client"));
    let committer = Box::new(UCCommitter::new(
        update_client,
        table_uuid,
        catalog.clone(),
        schema.clone(),
        table.clone(),
    ));
    snapshot
        .alter_table()
        .set_table_property(key, &value)
        .build(&engine, committer)
        .expect("failed to build alter transaction")
        .commit(&engine)
        .expect("failed to commit alter transaction")
        .unwrap_committed();

    // 5. Reload and verify the property landed in the catalog metadata.
    let after = uc
        .load_table(&catalog, &schema, &table)
        .await
        .expect("reload load_table failed");
    assert_eq!(
        after.metadata.properties.get(key),
        Some(&value),
        "property should be present in catalog metadata after ALTER"
    );
}

/// Translate the staging response's required protocol + properties into the table properties the v0
/// create commit must carry. Each required protocol feature becomes
/// `delta.feature.<name>=supported` so kernel enables it; required raw properties with concrete
/// values pass through (a null value means "any value is acceptable"). Honors the server's feature
/// advertisement rather than hardcoding a feature set.
///
/// Compare `get_required_properties_for_disk` in
/// `delta-kernel-unity-catalog/src/utils/create_table.rs`; this live version is intentionally
/// broader because it honors the server's advertised feature set.
fn disk_properties_from_staging(resp: &CreateStagingTableResponse) -> HashMap<String, String> {
    let mut props: HashMap<String, String> = resp
        .required_protocol
        .reader_features
        .iter()
        .chain(resp.required_protocol.writer_features.iter())
        .map(|f| (format!("delta.feature.{f}"), "supported".to_string()))
        .collect();
    for (k, v) in &resp.required_properties {
        // Skip properties kernel derives from the enabled features and forbids setting at CREATE
        // (e.g. `delta.checkpointPolicy` is implied by `delta.feature.v2Checkpoint`).
        if k == "delta.checkpointPolicy" {
            continue;
        }
        if let Some(v) = v {
            props.insert(k.clone(), v.clone());
        }
    }
    props.insert("io.unitycatalog.tableId".to_string(), resp.table_id.clone());
    props
}

/// Live CREATE through the full connector flow. The two CREATE endpoints (`staging-tables`,
/// `tables`) are deliberately not in the REST client, so this test hand-rolls those POSTs with raw
/// `reqwest` while using kernel for the v0 commit and the typed `tables` body:
///   1. POST `staging-tables` -> table id, storage location, staging credentials, required
///      protocol.
///   2. Build a cloud store from the staging credentials + `DefaultEngine`.
///   3. kernel `create_table` (with the disk-required properties) commits v0 directly to storage
///      via `UCCommitter` (the v0 path writes `000.json`; it does NOT call the catalog).
///   4. Load the post-commit v0 `Snapshot`.
///   5. `build_uc_create_table_request` -> typed `CreateTableRequest`.
///   6. POST `tables` to register the table.
///   7. Verify via `UCClient::load_table` (uuid matches the staging table id).
///   8. Append 3 rows through the connector write path and scan them back.
///   9. ALTER `add_column` through `UCCommitter`, reload, and confirm the evolved schema
///      round-trips back via `load_table` -> snapshot.
///
/// This mutates the catalog, so it is double-gated: requires `UC_CREATE=1`. Uses a unique table
/// name and best-effort DELETEs before and after to keep reruns idempotent.
#[tokio::test(flavor = "multi_thread")]
async fn live_create_append_and_alter() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_create_append_and_alter");
        return;
    };
    if std::env::var("UC_CREATE").is_err() {
        eprintln!("UC_CREATE unset; skipping mutating live_create_append_and_alter");
        return;
    }
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());
    let schema_name = std::env::var("UC_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string());
    // Unique per run: a leftover table/staging-table from a prior run (best-effort cleanup can
    // fail) otherwise 409s the staging-tables POST. Suffix keeps reruns collision-free.
    let name = format!("kernel_rs_create_test_{}", uuid::Uuid::new_v4().simple());
    let name = name.as_str();

    let (base, http) = raw_delta_rest_client(&url, &token);
    let tables_base = base
        .join(&format!("catalogs/{catalog}/schemas/{schema_name}/"))
        .expect("tables base URL");

    // (a) Best-effort cleanup of a prior run; ignore non-2xx/404.
    let table_url = tables_base
        .join(&format!("tables/{name}"))
        .expect("table URL");
    let _ = http.delete(table_url.clone()).send().await;

    // (b) POST staging-tables -> allocate uuid + storage + staging credentials.
    let staging_url = tables_base
        .join("staging-tables")
        .expect("staging-tables URL");
    let staging_req = CreateStagingTableRequest {
        name: name.to_string(),
    };
    let staging_req_json =
        serde_json::to_string_pretty(&staging_req).expect("serialize CreateStagingTableRequest");
    let staging_resp = http
        .post(staging_url)
        .json(&staging_req)
        .send()
        .await
        .expect("staging-tables POST failed");
    let staging_status = staging_resp.status();
    if !staging_status.is_success() {
        let body = staging_resp.text().await.unwrap_or_default();
        panic!(
            "staging-tables POST returned {staging_status}\nrequest body:\n{staging_req_json}\nresponse body:\n{body}"
        );
    }
    let resp: CreateStagingTableResponse = staging_resp
        .json()
        .await
        .expect("failed to deserialize CreateStagingTableResponse");

    // (c)/(d) Build the engine over the staging storage location.
    let table_root = normalize_table_root(
        &Url::parse(&resp.storage_location).expect("storage_location is not a valid URL"),
    );
    let store = store_from_url_opts(&table_root, object_store_options(&resp.storage_credentials))
        .expect("failed to build object store from staging credentials");
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    // A server backed by local-FS storage (e.g. the OSS server in CI) allocates the table location
    // logically without creating the directory; kernel lists the table root during create-table and
    // `LocalFileSystem` errors on a missing path (a cloud object store returns empty). Create it
    // for file:// locations so the create-table build can proceed.
    if table_root.scheme() == "file" {
        if let Ok(path) = table_root.to_file_path() {
            std::fs::create_dir_all(&path).expect("failed to create local table directory");
        }
    }

    // (e) Minimal schema: id INT, name STRING.
    let schema: SchemaRef = Arc::new(
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .expect("failed to build schema"),
    );

    // (f)/(g) Commit v0 directly to storage via the UC committer (v0 path does not call the
    // catalog).
    let committer = Box::new(UCCommitter::new(
        Arc::new(UCUpdateTableRestClient::new(client_config(&url, &token)).expect("update client")),
        resp.table_id.clone(),
        catalog.clone(),
        schema_name.clone(),
        name.to_string(),
    ));
    create_table(table_root.as_str(), schema, "delta-kernel-rs-live-test")
        .with_table_properties(disk_properties_from_staging(&resp))
        .build(engine.as_ref(), committer)
        .expect("failed to build create-table transaction")
        .commit(engine.as_ref())
        .expect("failed to commit create-table transaction")
        .unwrap_committed();

    // (h) Load the post-commit v0 snapshot. The table is catalog-managed, so the snapshot build
    // requires the catalog's max version; the just-created table is at v0.
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_max_catalog_version(0)
        .build(engine.as_ref())
        .expect("failed to build post-commit snapshot");
    assert_eq!(
        snapshot.version(),
        0,
        "post-create snapshot should be at version 0"
    );

    // (i)/(j) Build the typed create body and register the table.
    let mut req = build_uc_create_table_request(&snapshot, engine.as_ref(), name)
        .expect("failed to build CreateTableRequest");
    // The server requires `delta.checkpointPolicy=v2` in the create body for v2Checkpoint tables.
    // The kernel does not write the companion `delta.checkpointPolicy` property when enabling the
    // `v2Checkpoint` feature, so it is absent from the committed metadata. The connector supplies
    // it here to satisfy the server contract.
    req.properties
        .insert("delta.checkpointPolicy".to_string(), "v2".to_string());
    let req_json = serde_json::to_string_pretty(&req).expect("serialize CreateTableRequest");
    let create_resp = http
        .post(tables_base.join("tables").expect("tables URL"))
        .json(&req)
        .send()
        .await
        .expect("tables POST failed");
    let create_status = create_resp.status();
    if !create_status.is_success() {
        let body = create_resp.text().await.unwrap_or_default();
        panic!(
            "tables POST returned {create_status}\nrequest body:\n{req_json}\nresponse body:\n{body}"
        );
    }

    // (k) Verify registration: the table loads and its uuid matches the staging table id.
    let loaded = client(&url, &token)
        .load_table(&catalog, &schema_name, name)
        .await
        .expect("load_table after create failed");
    assert_eq!(
        loaded.metadata.table_uuid, resp.table_id,
        "loaded table uuid should match the staging table id"
    );

    // (m) Append 3 rows through the connector write path, then scan them back.
    let uc = client(&url, &token);
    let pre = uc
        .load_table(&catalog, &schema_name, name)
        .await
        .expect("load_table before append failed");
    let (_, snapshot) = snapshot_from_load_table(&pre, engine.as_ref());

    let id_col: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let name_col: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z"]));
    let append_committer = Box::new(UCCommitter::new(
        Arc::new(UCUpdateTableRestClient::new(client_config(&url, &token)).expect("update client")),
        resp.table_id.clone(),
        catalog.clone(),
        schema_name.clone(),
        name.to_string(),
    ));
    insert_data_with(
        snapshot,
        &engine,
        vec![id_col, name_col],
        append_committer,
        "WRITE",
        true,
        false,
    )
    .await
    .expect("append failed")
    .unwrap_committed();

    let post = uc
        .load_table(&catalog, &schema_name, name)
        .await
        .expect("load_table after append failed");
    let (_, snapshot) = snapshot_from_load_table(&post, engine.as_ref());
    let scan = snapshot.scan_builder().build().expect("scan build failed");
    let batches = read_scan(&scan, engine.clone() as Arc<dyn Engine>).expect("read_scan failed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "appended rows should be returned by the scan");

    // (n) ALTER: add a nullable column through the UC committer (real update_table SetColumns).
    let (_, snapshot) = snapshot_from_load_table(&post, engine.as_ref());
    let alter_committer = Box::new(UCCommitter::new(
        Arc::new(UCUpdateTableRestClient::new(client_config(&url, &token)).expect("update client")),
        resp.table_id.clone(),
        catalog.clone(),
        schema_name.clone(),
        name.to_string(),
    ));
    snapshot
        .alter_table()
        .add_column(StructField::nullable("added_col", DataType::STRING))
        .build(engine.as_ref(), alter_committer)
        .expect("failed to build alter transaction")
        .commit(engine.as_ref())
        .expect("failed to commit alter transaction")
        .unwrap_committed();

    // (o) Reload and confirm the evolved schema round-trips back through the catalog.
    let after_alter = uc
        .load_table(&catalog, &schema_name, name)
        .await
        .expect("load_table after alter failed");
    let (_, snapshot) = snapshot_from_load_table(&after_alter, engine.as_ref());
    assert!(
        snapshot.schema().field("added_col").is_some(),
        "added column should be visible after ALTER round-trips through the catalog"
    );

    // (l) Best-effort cleanup.
    let _ = http.delete(table_url).send().await;
}
