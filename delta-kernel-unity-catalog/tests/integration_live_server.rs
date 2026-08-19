//! Live end-to-end tests against a running Unity Catalog server.
//!
//! These are gated behind the `integration-test` feature so they are never compiled by a normal
//! `cargo nextest`, and marked `#[ignore]` so that even with the feature enabled they report as
//! skipped rather than passing. Run them against a UC server with
//! `--run-ignored only -E 'test(live_)'` (see the UC CI job). Each also returns early if
//! `UC_SERVER_URL` is unset.
//!
//! Run against a local UC server:
//! ```bash
//! UC_SERVER_URL=http://localhost:8080 UC_TOKEN=not-used \
//!   cargo nextest run -p delta-kernel-unity-catalog --features integration-test
//! ```
//!
//! Optional overrides for the read-path test: `UC_TEST_CATALOG`, `UC_TEST_SCHEMA`,
//! `UC_TEST_TABLE` (the read test skips unless `UC_TEST_TABLE` is set).
//!
//! The CREATE test (`live_create_table`) mutates the catalog, so point `UC_SERVER_URL` only at a
//! throwaway server or schema.
//!
//! These tests prove the RPCs round-trip, not that the payloads are semantically correct.
#![cfg(feature = "integration-test")]

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::expressions::column_name;
use delta_kernel::schema::{schema_ref, SchemaRef};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::storage::store_from_url_opts;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_unity_catalog::{
    aws_object_store_options, build_uc_create_table_request, get_required_properties_for_disk,
    log_tail_from_commits, snapshot_builder_from_load_table, UCCommitter,
};
use test_utils::{insert_data_with, read_scan};
use unity_catalog_delta_client_api::{
    CreateStagingTableRequest, LoadTableResponse, TableIdentifier,
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

fn client_config(url: &str, token: &str) -> ClientConfig {
    ClientConfig::build(url, token)
        .build()
        .expect("failed to build ClientConfig")
}

fn client(url: &str, token: &str) -> UCClient {
    UCClient::new(client_config(url, token)).expect("failed to build UCClient")
}

/// Normalize a UC table location to a root URL ending in `/`. UC returns the location without a
/// trailing slash, but staged-commit path resolution and snapshot building require one.
fn normalize_table_root(table_url: &Url) -> Url {
    let mut root = table_url.clone();
    if !root.path().ends_with('/') {
        root.set_path(&format!("{}/", root.path()));
    }
    root
}

fn snapshot_from_load_table(resp: &LoadTableResponse, engine: &dyn Engine) -> Arc<Snapshot> {
    snapshot_builder_from_load_table(resp)
        .expect("build snapshot builder from load_table response")
        .build(engine)
        .expect("build snapshot from load_table response")
}

/// `load_table` returns parseable metadata + commits, and the commits convert to a kernel log tail.
/// Skips unless `UC_TEST_TABLE` names an existing managed delta table. This validates the read-path
/// wire types without requiring storage credentials (it does not read data files).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running UC server; run with --run-ignored (UC CI job or manual)"]
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
    let log_tail = log_tail_from_commits(&resp.commits, table_url)
        .expect("log_tail_from_commits failed on the server's commit list");
    assert_eq!(
        log_tail.len(),
        resp.commits.len(),
        "every returned commit should map to a log path"
    );
}

/// Live CREATE through the full connector flow: `UCClient::create_staging_table` to reserve,
/// kernel for the v0 commit, then `UCClient::create_table` to register.
///
/// The table enables row tracking and clustering so the create body and the write path exercise the
/// `delta.rowTracking` and `delta.clustering` domains in one round trip: the initial watermark (-1)
/// is forwarded at create and advances to 2 after the append (the CTAS concern), and the clustering
/// columns are forwarded under `delta.clustering`.
///
/// This mutates the catalog, so point it only at a throwaway server or schema.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running UC server; run with --run-ignored (UC CI job or manual)"]
async fn live_create_table() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_create_table");
        return;
    };
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());
    let schema_name = std::env::var("UC_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string());
    // Unique per run so reruns never collide on the staging-tables POST (which 409s on a name
    // already present).
    let name = format!("kernel_rs_create_test_{}", uuid::Uuid::new_v4().simple());
    let name = name.as_str();

    let uc_client = client(&url, &token);

    // ===== Step 1: reserve a staging table -> allocate uuid + storage + staging credentials =====
    let resp = uc_client
        .create_staging_table(
            &catalog,
            &schema_name,
            CreateStagingTableRequest {
                name: name.to_string(),
            },
        )
        .await
        .expect("create_staging_table failed");

    // ===== Step 2: Build the engine over the staging storage location =====
    let table_root =
        normalize_table_root(&Url::parse(&resp.location).expect("location is not a valid URL"));
    let store = store_from_url_opts(
        &table_root,
        aws_object_store_options(&resp.storage_credentials, "us-east-1"),
    )
    .expect("failed to build object store from staging credentials");
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    // A server backed by local-FS storage (e.g. the server in CI) allocates the table location
    // logically without creating the directory; kernel lists the table root during create-table and
    // `LocalFileSystem` errors on a missing path (a cloud object store returns empty). Create it
    // for file:// locations so the create-table build can proceed.
    if table_root.scheme() == "file" {
        if let Ok(path) = table_root.to_file_path() {
            std::fs::create_dir_all(&path).expect("failed to create local table directory");
        }
    }

    // ===== Step 3: Define the schema =====
    // A nested struct so clustering can target both a top-level and a nested
    // column.
    let schema: SchemaRef = schema_ref! {
        not_null "id": INTEGER,
        nullable "name": STRING,
        nullable "address": {
            nullable "city": STRING,
            nullable "zip": STRING,
        },
    };

    // ===== Step 4: Invoke kernel to write the v0 commit (00.json) via the UC committer =====
    // The v0 path writes directly to storage and does not call the catalog. Enable row tracking so
    // the create body carries `delta.rowTracking`, and cluster on `name` and the nested
    // `address.city` so it carries `delta.clustering`.
    let mut disk_props = get_required_properties_for_disk(&resp.table_id);
    disk_props.insert("delta.enableRowTracking".to_string(), "true".to_string());
    let committer = Box::new(UCCommitter::new(
        Arc::new(UCUpdateTableRestClient::new(client_config(&url, &token)).expect("update client")),
        resp.table_id.clone(),
        TableIdentifier::new(catalog.clone(), schema_name.clone(), name.to_string()),
    ));
    create_table(table_root.as_str(), schema, "delta-kernel-rs-live-test")
        .with_table_properties(disk_props)
        .with_data_layout(DataLayout::Clustered {
            columns: vec![column_name!("name"), column_name!("address", "city")],
        })
        .build(engine.as_ref(), committer)
        .expect("failed to build create-table transaction")
        .commit(engine.as_ref())
        .expect("failed to commit create-table transaction")
        .unwrap_committed();

    // ===== Step 5: Load the post-commit v0 snapshot =====
    // The table is catalog-managed, so the snapshot build requires the catalog's max version; the
    // just-created table is at v0.
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_max_catalog_version(0)
        .build(engine.as_ref())
        .expect("failed to build post-commit snapshot");
    assert_eq!(
        snapshot.version(),
        0,
        "post-create snapshot should be at version 0"
    );

    // ===== Step 6: Build the typed create body and register the table with UC (POST tables) =====
    let req = build_uc_create_table_request(&snapshot, engine.as_ref(), name)
        .expect("failed to build CreateTableRequest");
    // Empty create forwards the initial row-tracking watermark and the clustering columns.
    assert_eq!(
        req.domain_metadata["delta.rowTracking"]["rowIdHighWaterMark"].as_i64(),
        Some(-1),
        "empty create should forward the initial watermark"
    );
    let clustering: Vec<Vec<String>> = serde_json::from_value(
        req.domain_metadata["delta.clustering"]["clusteringColumns"].clone(),
    )
    .expect("clusteringColumns should deserialize");
    assert_eq!(clustering, vec![vec!["name"], vec!["address", "city"]]);
    for (key, value) in [
        ("delta.checkpoint.writeStatsAsStruct", "true"),
        ("delta.checkpoint.writeStatsAsJson", "true"),
        ("delta.enableDeletionVectors", "true"),
        ("delta.checkpointPolicy", "v2"),
    ] {
        assert_eq!(
            req.properties.get(key).map(String::as_str),
            Some(value),
            "{key} should be present in the create body"
        );
    }
    uc_client
        .create_table(&catalog, &schema_name, req)
        .await
        .expect("create_table failed");

    // ===== Step 7: Verify registration: the table loads and its uuid matches the staging id =====
    let loaded = uc_client
        .load_table(&catalog, &schema_name, name)
        .await
        .expect("load_table after create failed");
    assert_eq!(
        loaded.metadata.table_uuid, resp.table_id,
        "loaded table uuid should match the staging table id"
    );

    // ===== Step 8: Append 3 rows through the connector write path, then scan them back =====
    let uc = client(&url, &token);
    let pre = uc
        .load_table(&catalog, &schema_name, name)
        .await
        .expect("load_table before append failed");
    let snapshot = snapshot_from_load_table(&pre, engine.as_ref());

    let id_col: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let name_col: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z"]));
    let address_col: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("city", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["nyc", "sf", "la"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("zip", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["10001", "94103", "90001"])) as ArrayRef,
        ),
    ]));
    let append_committer = Box::new(UCCommitter::new(
        Arc::new(UCUpdateTableRestClient::new(client_config(&url, &token)).expect("update client")),
        resp.table_id.clone(),
        TableIdentifier::new(catalog.clone(), schema_name.clone(), name.to_string()),
    ));
    insert_data_with(
        snapshot,
        &engine,
        vec![id_col, name_col, address_col],
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
    let snapshot = snapshot_from_load_table(&post, engine.as_ref());

    // Appending 3 rows to a row-tracking table assigns IDs 0..=2, advancing the mark from -1 to 2.
    let row_tracking = snapshot
        .get_domain_metadata_internal("delta.rowTracking", engine.as_ref())
        .expect("failed to read delta.rowTracking domain metadata");
    assert_eq!(
        row_tracking.as_deref(),
        Some(r#"{"rowIdHighWaterMark":2}"#),
        "after appending 3 rows the high water mark should advance from -1 to 2"
    );

    let scan = snapshot.scan_builder().build().expect("scan build failed");
    let batches = read_scan(&scan, engine.clone() as Arc<dyn Engine>).expect("read_scan failed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "appended rows should be returned by the scan");
}
