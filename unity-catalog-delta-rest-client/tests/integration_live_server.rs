//! Live integration tests that exercise the UC REST client against a real Unity Catalog OSS
//! server.
//!
//! These are gated behind the `integration-test` feature so they are never compiled or run by a
//! normal `cargo test`. The dedicated CI workflow (`.github/workflows/unitycatalog_oss_test.yml`)
//! builds + starts a UC OSS server and runs them:
//!
//!   cargo nextest run -p unity-catalog-delta-rest-client --features integration-test -E
//! 'test(live_)'
#![cfg(feature = "integration-test")]

use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};

/// Reads the server URL + token from the environment, or `None` to skip the test.
fn server_env() -> Option<(String, String)> {
    let url = std::env::var("UC_SERVER_URL").ok()?;
    let token = std::env::var("UC_TOKEN").unwrap_or_else(|_| "not-used".to_string());
    Some((url, token))
}

fn client(url: &str, token: &str) -> UCClient {
    let config = ClientConfig::build(url, token)
        .build()
        .expect("failed to build ClientConfig");
    UCClient::new(config).expect("failed to build UCClient")
}

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

#[tokio::test(flavor = "multi_thread")]
async fn live_load_table_reads_metadata() {
    let Some((url, token)) = server_env() else {
        eprintln!("UC_SERVER_URL unset; skipping live_load_table_reads_metadata");
        return;
    };
    let Some(table) = std::env::var("UC_TEST_TABLE").ok() else {
        eprintln!("UC_TEST_TABLE unset; skipping live_load_table_reads_metadata");
        return;
    };
    let catalog = std::env::var("UC_TEST_CATALOG").unwrap_or_else(|_| "unity".to_string());
    let schema = std::env::var("UC_TEST_SCHEMA").unwrap_or_else(|_| "default".to_string());

    let resp = client(&url, &token)
        .load_table(&catalog, &schema, &table)
        .await
        .expect("load_table failed");
    let meta = &resp.metadata;

    assert!(
        !meta.table_uuid.is_empty(),
        "expected a table_uuid in the load_table response"
    );
    assert_eq!(meta.table_type, "MANAGED", "expected a managed table");
    assert!(
        meta.location.starts_with("file:") || meta.location.starts_with("s3"),
        "expected an absolute storage location, got {:?}",
        meta.location
    );
    let columns = meta
        .columns
        .get("fields")
        .and_then(|f| f.as_array())
        .expect("expected columns to decode as a StructType with a `fields` array");
    let names: Vec<&str> = columns
        .iter()
        .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
        .collect();
    assert_eq!(names, ["id", "name"], "expected the seeded columns");

    assert_eq!(
        resp.latest_table_version,
        Some(0),
        "expected latest_table_version 0 for a freshly created table"
    );
}

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

    let client = client(&url, &token);

    let location = client
        .load_table(&catalog, &schema, &table)
        .await
        .expect("load_table failed")
        .metadata
        .location;

    let resp = client
        .get_table_credentials(&catalog, &schema, &table, Operation::Read)
        .await
        .expect("get_table_credentials failed");

    assert!(
        !resp.storage_credentials.is_empty(),
        "expected the server to vend at least one credential"
    );
    for cred in &resp.storage_credentials {
        assert!(
            cred.prefix.contains("://"),
            "expected a storage-URL prefix, got {:?}",
            cred.prefix
        );
        assert_eq!(
            cred.operation,
            Operation::Read,
            "expected the vended credential to echo the requested operation"
        );
        let prefix = cred.prefix.trim_end_matches('/');
        let table_location = location.trim_end_matches('/');
        assert!(
            table_location == prefix || table_location.starts_with(&format!("{prefix}/")),
            "vended prefix {:?} should scope the table location {:?}",
            cred.prefix,
            location
        );
    }
}
