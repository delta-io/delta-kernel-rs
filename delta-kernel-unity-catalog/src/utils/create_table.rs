//! Utilities for Unity Catalog catalog-managed table creation.
//!
//! These utilities help connectors create UC-managed tables by providing the required properties
//! for both the Delta log (disk) and the UC server registration.
//!
//! TODO(#3032): add a high-level helper that runs the whole flow (reserve staging table, vend
//! credentials, kernel v0 commit, register with UC) in one call.
//!
//! # Usage
//!
//! ```ignore
//! // Step 1: Get staging info from UC
//! let staging_info = my_uc_client.post_staging_table(..).await?;
//!
//! // Step 2: Build and commit the create-table transaction
//! let disk_props = get_required_properties_for_disk(&staging_info.table_id);
//! let create_table_txn = kernel::create_table(path, schema, "MyApp/1.0")
//!     .with_table_properties(disk_props)
//!     .build(engine);
//! create_table_txn.commit(engine, committer, delta_kernel::transaction::CommitActions::new())?;
//!
//! // Step 3: Finalize table in UC
//! let snapshot = /* load post-commit snapshot at version 0 */;
//! let create_req = build_uc_create_table_request(&snapshot, engine, table_name)?;
//! my_uc_client.post_create_table(create_req).await?;
//! ```

use std::collections::{HashMap, HashSet};

use delta_kernel::actions::Protocol;
use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
use unity_catalog_delta_client_api::{
    CreateTableRequest, Protocol as WireProtocol, StorageCredential,
};

use crate::constants::{
    CATALOG_MANAGED_FEATURE_KEY, CHECKPOINT_POLICY_KEY, CHECKPOINT_POLICY_V2,
    CHECKPOINT_WRITE_STATS_AS_JSON_KEY, CHECKPOINT_WRITE_STATS_AS_STRUCT_KEY,
    CLUSTERING_DOMAIN_NAME, CONFIG_TRUE, DELETION_VECTORS_FEATURE_KEY, ENABLE_DELETION_VECTORS_KEY,
    FEATURE_SUPPORTED, ROW_TRACKING_DOMAIN_NAME, UC_TABLE_ID_KEY, V2_CHECKPOINT_FEATURE_KEY,
    VACUUM_PROTOCOL_CHECK_FEATURE_KEY,
};

/// Returns the table properties that must be written to disk (in `000.json`) for a UC
/// catalog-managed table creation.
///
/// These properties must be persisted in the Delta log so that the table is recognized as
/// catalog-managed. Note: ICT enablement is handled automatically by kernel's CREATE TABLE
/// when the `catalogManaged` feature is present.
///
/// TODO(delta-io/delta#7179): this hardcodes the required table features and properties. The create
/// table flow should instead derive them from the `createStagingTable` response's advertised
/// `required_protocol` and `required_properties`, so it stays correct if UC changes what it
/// requires.
pub fn get_required_properties_for_disk(uc_table_id: &str) -> HashMap<String, String> {
    [
        (CATALOG_MANAGED_FEATURE_KEY, FEATURE_SUPPORTED),
        (VACUUM_PROTOCOL_CHECK_FEATURE_KEY, FEATURE_SUPPORTED),
        (V2_CHECKPOINT_FEATURE_KEY, FEATURE_SUPPORTED),
        (DELETION_VECTORS_FEATURE_KEY, FEATURE_SUPPORTED),
        (ENABLE_DELETION_VECTORS_KEY, CONFIG_TRUE),
        (CHECKPOINT_WRITE_STATS_AS_STRUCT_KEY, CONFIG_TRUE),
        (CHECKPOINT_WRITE_STATS_AS_JSON_KEY, CONFIG_TRUE),
        (UC_TABLE_ID_KEY, uc_table_id),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Map AWS S3 credentials vended by UC (`CreateStagingTableResponse.storage_credentials` or
/// `get_table_credentials`) into `object_store` option keys, for building an engine over the
/// table's storage.
///
/// This is a convenience over the public `store_from_url_opts`: connectors can build a store
/// themselves by passing option keys directly. It only translates S3's key names today.
///
/// TODO(#3016): also translate GCS (`gcs.*`) and Azure (`azure.*`) credential keys so connectors
/// don't each re-derive the mapping.
pub fn aws_object_store_options(
    creds: &[StorageCredential],
    region: &str,
) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    for cred in creds {
        for (key, value) in &cred.config {
            let mapped = match key.as_str() {
                "s3.access-key-id" => "access_key_id",
                "s3.secret-access-key" => "secret_access_key",
                "s3.session-token" => "session_token",
                other => other,
            };
            opts.insert(mapped.to_string(), value.clone());
        }
    }
    opts.insert("region".to_string(), region.to_string());
    opts
}

/// Build the typed `CreateTableRequest` body the connector sends to the UC `tables` endpoint after
/// the v0 commit succeeds, built from the post-commit v0 snapshot.
///
/// User properties set via `create_table`'s `with_table_properties` flow through automatically
/// (they land in `metaData.configuration`). To add or override properties afterward, mutate
/// `req.properties` on the returned request before sending.
///
/// UC requires `delta.checkpointPolicy=v2` in the request body, but kernel's CREATE TABLE rejects
/// it as a table property, so it never lands in the committed metadata. This function adds it to
/// the request body so callers get a complete, contract-valid request.
///
/// # Errors
///
/// Returns an error if `snapshot` is not at version 0, if the schema can't be serialized, or if the
/// engine fails to read clustering metadata or the commit timestamp.
pub fn build_uc_create_table_request(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    table_name: impl Into<String>,
) -> DeltaResult<CreateTableRequest> {
    if snapshot.version() != 0 {
        return Err(Error::generic(format!(
            "build_uc_create_table_request is only valid for version 0 (table creation) \
             snapshots, but snapshot is at version {}",
            snapshot.version()
        )));
    }

    let columns = serde_json::to_value(snapshot.schema().as_ref())
        .map_err(|e| Error::generic(format!("Failed to serialize table schema: {e}")))?;

    let table_config = snapshot.table_configuration();
    let metadata = table_config.metadata();
    let protocol = table_config.protocol();

    let partition_columns = metadata.partition_columns().to_vec();
    let mut properties = metadata.configuration().clone();
    properties.insert(
        CHECKPOINT_POLICY_KEY.to_string(),
        CHECKPOINT_POLICY_V2.to_string(),
    );
    let wire_protocol = to_wire_protocol(protocol);

    // UC only recognizes the `delta.clustering` and `delta.rowTracking` domain metadatas.
    let uc_recognized_domains = HashSet::from([CLUSTERING_DOMAIN_NAME, ROW_TRACKING_DOMAIN_NAME]);
    let mut domain_metadata: HashMap<String, serde_json::Value> = HashMap::new();
    for (domain, dm) in
        snapshot.get_domain_metadatas_internal(engine, Some(&uc_recognized_domains))?
    {
        let value = serde_json::from_str(dm.configuration())
            .map_err(|e| Error::generic(format!("malformed {domain} domain metadata: {e}")))?;
        domain_metadata.insert(domain, value);
    }

    let in_commit_timestamp_ms = snapshot.get_timestamp(engine)?;

    Ok(CreateTableRequest {
        name: table_name.into(),
        location: snapshot.table_root().to_string(),
        table_type: "MANAGED".to_string(),
        comment: None,
        columns,
        partition_columns,
        protocol: wire_protocol,
        properties,
        domain_metadata,
        last_commit_timestamp_ms: in_commit_timestamp_ms,
    })
}

/// Convert a kernel `Protocol` into the api crate's wire `Protocol`. The api crate is kernel-free,
/// so it defines its own serializable transport type rather than reusing `delta_kernel::Protocol`.
fn to_wire_protocol(protocol: &Protocol) -> WireProtocol {
    WireProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: protocol
            .reader_features()
            .into_iter()
            .flatten()
            .map(|f| f.as_ref().to_string())
            .collect(),
        writer_features: protocol
            .writer_features()
            .into_iter()
            .flatten()
            .map(|f| f.as_ref().to_string())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::expressions::column_name;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::schema::{schema_ref, SchemaRef};
    use delta_kernel::snapshot::Snapshot;
    use delta_kernel::transaction::create_table::create_table;
    use delta_kernel::transaction::data_layout::DataLayout;
    use delta_kernel::transaction::TransactionOptions;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use test_utils::TestCatalogCommitter;

    use super::*;

    /// Create a table at version 0 (optionally clustered), then build the UC create request from
    /// its post-commit snapshot. Shared setup for the version-0 request tests.
    fn create_v0_and_build_request(
        engine: &dyn Engine,
        table_path: &str,
        schema: SchemaRef,
        disk_props: HashMap<String, String>,
        data_layout: DataLayout,
    ) -> CreateTableRequest {
        create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .with_data_layout(data_layout)
            .build(engine)
            .unwrap()
            .commit(
                engine,
                &TestCatalogCommitter,
                delta_kernel::transaction::CommitActions::new(),
            )
            .unwrap()
            .unwrap_committed();
        let snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(0)
            .build(engine)
            .unwrap();
        assert_eq!(snapshot.version(), 0);
        build_uc_create_table_request(&snapshot, engine, "my_table").unwrap()
    }

    #[test]
    fn test_get_required_properties_for_disk() {
        let props = get_required_properties_for_disk("my-uc-table-123");
        assert_eq!(props.len(), 8);
        assert_eq!(props["delta.feature.catalogManaged"], "supported");
        assert_eq!(props["delta.feature.vacuumProtocolCheck"], "supported");
        assert_eq!(props["delta.feature.v2Checkpoint"], "supported");
        assert_eq!(props["delta.feature.deletionVectors"], "supported");
        assert_eq!(props["delta.enableDeletionVectors"], "true");
        assert_eq!(props["delta.checkpoint.writeStatsAsStruct"], "true");
        assert_eq!(props["delta.checkpoint.writeStatsAsJson"], "true");
        assert_eq!(props["io.unitycatalog.tableId"], "my-uc-table-123");
    }

    #[tokio::test]
    async fn test_build_uc_create_table_request() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_create_req/";
        let schema = schema_ref! {
            nullable "id": INTEGER,
            nullable "region": STRING,
        };

        let disk_props = get_required_properties_for_disk("test-table-id-456");
        let req = create_v0_and_build_request(
            &engine,
            table_path,
            schema,
            disk_props,
            DataLayout::clustered(["region"]),
        );

        assert_eq!(req.protocol.min_reader_version, 3);
        assert_eq!(req.protocol.min_writer_version, 7);
        assert!(req
            .protocol
            .reader_features
            .iter()
            .any(|f| f == "catalogManaged"));
        assert!(req
            .protocol
            .writer_features
            .iter()
            .any(|f| f == "inCommitTimestamp"));

        assert_eq!(
            req.properties["io.unitycatalog.tableId"],
            "test-table-id-456"
        );
        assert_eq!(req.properties["delta.checkpointPolicy"], "v2");
        assert!(!req.properties.contains_key("delta.feature.catalogManaged"));
        assert!(!req.properties.contains_key("delta.minReaderVersion"));
        assert!(!req.properties.contains_key("delta.lastUpdateVersion"));

        // Clustering surfaces under domain_metadata in typed JSON form.
        let clustering = req
            .domain_metadata
            .get("delta.clustering")
            .expect("clustering domain metadata should be present");
        let cols = clustering
            .get("clusteringColumns")
            .and_then(|v| v.as_array())
            .expect("clusteringColumns should be a JSON array");
        assert_eq!(cols.len(), 1);

        assert_eq!(
            req.columns.get("type").and_then(|v| v.as_str()),
            Some("struct")
        );
        assert_eq!(req.name, "my_table");
        assert!(req.location.contains("test_create_req"));

        assert!(
            req.last_commit_timestamp_ms > 0,
            "expected positive in-commit timestamp, got {}",
            req.last_commit_timestamp_ms
        );
    }

    #[tokio::test]
    async fn test_build_uc_create_table_request_forwards_row_tracking() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_row_tracking/";
        let schema = schema_ref! { nullable "id": INTEGER };

        let mut disk_props = get_required_properties_for_disk("test-table-id");
        disk_props.insert("delta.enableRowTracking".to_string(), "true".to_string());
        let req =
            create_v0_and_build_request(&engine, table_path, schema, disk_props, DataLayout::None);

        let hwm = req.domain_metadata[ROW_TRACKING_DOMAIN_NAME]["rowIdHighWaterMark"].as_i64();
        assert_eq!(
            hwm,
            Some(-1),
            "empty create should forward the initial watermark"
        );
    }

    #[tokio::test]
    async fn test_build_uc_create_table_request_silently_drops_user_domain_metadata() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_user_domain/";
        let schema = schema_ref! { nullable "id": INTEGER };

        let mut disk_props = get_required_properties_for_disk("test-table-id");
        disk_props.insert(
            "delta.feature.domainMetadata".to_string(),
            "supported".to_string(),
        );
        create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .with_options(
                TransactionOptions::new().with_domain_metadata(
                    "myApp.retention".to_string(),
                    r#"{"days":30}"#.to_string(),
                ),
            )
            .build(&engine)
            .unwrap()
            .commit(
                &engine,
                &TestCatalogCommitter,
                delta_kernel::transaction::CommitActions::new(),
            )
            .unwrap()
            .unwrap_committed();

        let snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(0)
            .build(&engine)
            .unwrap();
        let req = build_uc_create_table_request(&snapshot, &engine, "test_user_domain").unwrap();
        assert!(
            !req.domain_metadata.contains_key("myApp.retention"),
            "user domain should be dropped, not forwarded"
        );
    }

    // Clustering columns are forwarded verbatim from the committed `delta.clustering` domain. Under
    // column mapping the committed paths are physical names, so the request must carry those, not
    // the logical names.
    #[rstest]
    #[case::logical_names_without_column_mapping(false)]
    #[case::physical_names_under_column_mapping(true)]
    #[tokio::test]
    async fn test_clustering_columns_forwarded(#[case] column_mapping: bool) {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_clustering/";
        let schema = schema_ref! {
            nullable "id": INTEGER,
            nullable "region": STRING,
            nullable "address": {
                nullable "city": STRING,
                nullable "zip": STRING,
            },
        };

        let mut disk_props = get_required_properties_for_disk("test-table-id");
        if column_mapping {
            disk_props.insert("delta.columnMapping.mode".to_string(), "name".to_string());
        }
        let req = create_v0_and_build_request(
            &engine,
            table_path,
            schema,
            disk_props,
            DataLayout::Clustered {
                columns: vec![column_name!("region"), column_name!("address", "city")],
            },
        );

        let cols = req.domain_metadata[CLUSTERING_DOMAIN_NAME]["clusteringColumns"].clone();
        let parsed: Vec<Vec<String>> = serde_json::from_value(cols).unwrap();
        if column_mapping {
            assert_eq!(parsed.len(), 2);
            let flat: Vec<&str> = parsed.iter().flatten().map(String::as_str).collect();
            assert!(!flat.contains(&"region") && !flat.contains(&"city"));
        } else {
            assert_eq!(parsed, vec![vec!["region"], vec!["address", "city"]]);
        }
    }

    #[tokio::test]
    async fn test_build_uc_create_table_request_rejects_non_zero_version() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_create_req_version/";
        let schema = schema_ref! { nullable "id": INTEGER };

        // Create a table (version 0) and append (version 1).
        let disk_props = get_required_properties_for_disk("test-table-id");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .build(&engine)
            .unwrap()
            .commit(
                &engine,
                &TestCatalogCommitter,
                delta_kernel::transaction::CommitActions::new(),
            )
            .unwrap();
        let v0_snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(0)
            .build(&engine)
            .unwrap();
        let result = v0_snapshot
            .transaction_builder()
            .build(&engine)
            .unwrap()
            .commit(
                &engine,
                &TestCatalogCommitter,
                delta_kernel::transaction::CommitActions::new(),
            )
            .unwrap();
        assert!(result.is_committed());

        // Load snapshot at version 1.
        let snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(1)
            .build(&engine)
            .unwrap();
        assert_eq!(snapshot.version(), 1);

        // Should fail because version != 0.
        let err = build_uc_create_table_request(&snapshot, &engine, "x").unwrap_err();
        assert!(
            err.to_string().contains("version 0"),
            "expected version 0 error, got: {err}"
        );
    }
}
