//! Utilities for Unity Catalog catalog-managed table creation under the
//! UC API.
//!
//! These helpers produce:
//!
//! - the disk-side properties the v0 commit must include ([`get_required_properties_for_disk`]),
//!   and
//! - the typed `CreateTableRequest` body the connector sends to the UC `tables` endpoint after the
//!   v0 commit succeeds ([`build_uc_create_table_request`]).
//!
//! # Usage
//!
//! ```ignore
//! // Step 1: Allocate UUID + storage via UC `staging-tables` (connector-driven).
//! let staging = my_uc_client.create_staging_table(..);
//!
//! // Step 2: Build and commit the create-table transaction.
//! let disk_props = get_required_properties_for_disk(&staging.table_id);
//! let create_table_txn = kernel::create_table(path, schema, "MyApp/1.0")
//!     .with_table_properties(disk_props)
//!     .build(engine, committer);
//! create_table_txn.commit(engine)?;
//!
//! // Step 3: Finalize in UC via the typed `tables` endpoint (connector-driven).
//! let snapshot = /* load post-commit snapshot at version 0 */;
//! let req = build_uc_create_table_request(&snapshot, engine, "my_table")?;
//! my_uc_client.create_table(catalog, schema, req);
//! ```

use std::collections::HashMap;

use delta_kernel::{Engine, Snapshot};
use unity_catalog_delta_client_api::CreateTableRequest;

use crate::constants::{
    CATALOG_MANAGED_FEATURE_KEY, FEATURE_SUPPORTED, UC_TABLE_ID_KEY,
    VACUUM_PROTOCOL_CHECK_FEATURE_KEY,
};
use crate::conversions::{parse_or_string, to_api_protocol, CLUSTERING_DOMAIN};

/// Properties that must be persisted in `000.json` for a UC catalog-managed
/// table creation. ICT enablement is handled automatically by kernel's
/// CREATE TABLE when the `catalogManaged` feature is present.
pub fn get_required_properties_for_disk(uc_table_id: &str) -> HashMap<String, String> {
    [
        (CATALOG_MANAGED_FEATURE_KEY, FEATURE_SUPPORTED),
        (VACUUM_PROTOCOL_CHECK_FEATURE_KEY, FEATURE_SUPPORTED),
        (UC_TABLE_ID_KEY, uc_table_id),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Build the typed `CreateTableRequest` body the connector sends to the UC `tables` endpoint after
/// the v0 commit succeeds, driving off the post-commit v0 snapshot.
///
/// # Errors
///
/// Returns an error if `snapshot` is not at version 0, if the schema can't be serialized, or if the
/// engine fails to read clustering metadata or the commit timestamp.
pub fn build_uc_create_table_request(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    table_name: impl Into<String>,
) -> delta_kernel::DeltaResult<CreateTableRequest> {
    if snapshot.version() != 0 {
        return Err(delta_kernel::Error::generic(format!(
            "build_uc_create_table_request is only valid for version 0 (table creation) \
             snapshots, but snapshot is at version {}",
            snapshot.version()
        )));
    }

    let columns = serde_json::to_value(snapshot.schema().as_ref()).map_err(|e| {
        delta_kernel::Error::generic(format!("Failed to serialize table schema: {e}"))
    })?;

    let table_config = snapshot.table_configuration();
    let metadata = table_config.metadata();
    let protocol = table_config.protocol();

    let partition_columns = metadata.partition_columns().to_vec();
    let properties = metadata.configuration().clone();
    let typed_protocol = to_api_protocol(protocol);

    // Domain metadata: the CREATE path forwards user domains plus the typed clustering domain
    // (injected below under its logical-column shape). Internal `delta.*` domains are filtered out
    // by `get_all_domain_metadata`. The UPDATE path forwards only UC-mirrored domains; see
    // [`crate::conversions::UC_MIRRORED_DOMAINS`].
    let mut domain_metadata: HashMap<String, serde_json::Value> = HashMap::new();
    if let Some(columns) = snapshot.get_logical_clustering_columns(engine)? {
        let arrays: Vec<Vec<&str>> = columns
            .iter()
            .map(|c| c.path().iter().map(String::as_str).collect())
            .collect();
        domain_metadata.insert(
            CLUSTERING_DOMAIN.to_string(),
            serde_json::json!({ "clusteringColumns": arrays }),
        );
    }
    for dm in snapshot.get_all_domain_metadata(engine)? {
        // Never overwrite the typed clustering shape inserted above, even if a future change to
        // `get_all_domain_metadata` starts surfacing it.
        if dm.domain() == CLUSTERING_DOMAIN {
            continue;
        }
        domain_metadata.insert(dm.domain().to_string(), parse_or_string(dm.configuration()));
    }

    // v0 CREATE flow only handles managed Delta tables; external/iceberg are out of scope.
    // ICT is active on catalog-managed tables, so in practice this is the in-commit timestamp,
    // though `get_timestamp` falls back to filesystem mtime if ICT is absent.
    let last_commit_timestamp_ms = snapshot.get_timestamp(engine)?;

    Ok(CreateTableRequest {
        name: table_name.into(),
        storage_location: snapshot.table_root().to_string(),
        table_type: "MANAGED".to_string(),
        comment: None,
        columns,
        partition_columns,
        protocol: typed_protocol,
        properties,
        domain_metadata,
        last_commit_timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::snapshot::Snapshot;
    use delta_kernel::transaction::create_table::create_table;
    use delta_kernel::transaction::data_layout::DataLayout;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use test_utils::TestCatalogCommitter;

    use super::*;

    #[test]
    fn test_get_required_properties_for_disk() {
        let props = get_required_properties_for_disk("my-uc-table-123");
        assert_eq!(props.len(), 3);
        assert_eq!(props["delta.feature.catalogManaged"], "supported");
        assert_eq!(props["delta.feature.vacuumProtocolCheck"], "supported");
        assert_eq!(props["io.unitycatalog.tableId"], "my-uc-table-123");
    }

    #[tokio::test]
    async fn test_build_uc_create_table_request() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_table/";
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::new("id", DataType::INTEGER, true),
                StructField::new("region", DataType::STRING, true),
            ])
            .unwrap(),
        );

        let disk_props = get_required_properties_for_disk("test-table-id-456");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .with_data_layout(DataLayout::clustered(["region"]))
            .build(&engine, Box::new(TestCatalogCommitter))
            .unwrap()
            .commit(&engine)
            .unwrap();

        let snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(0)
            .build(&engine)
            .unwrap();
        assert_eq!(snapshot.version(), 0);
        let req = build_uc_create_table_request(&snapshot, &engine, "my_table").unwrap();

        // Typed protocol: feature names come through as plain strings.
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

        // Raw configuration only; no flattened delta.feature.* etc.
        assert_eq!(
            req.properties["io.unitycatalog.tableId"],
            "test-table-id-456"
        );
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

        // Schema serialized.
        assert_eq!(
            req.columns.get("type").and_then(|v| v.as_str()),
            Some("struct")
        );
        assert_eq!(req.name, "my_table");
        assert!(req.storage_location.contains("test_table"));

        // ICT is enabled on catalog-managed tables, so the v0 timestamp must be a real value.
        assert!(
            req.last_commit_timestamp_ms > 0,
            "expected positive in-commit timestamp, got {}",
            req.last_commit_timestamp_ms
        );
    }

    #[tokio::test]
    async fn test_build_uc_create_table_request_rejects_non_zero_version() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_version_check/";
        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("id", DataType::INTEGER, true)]).unwrap(),
        );

        let disk_props = get_required_properties_for_disk("test-table-id");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .build(&engine, Box::new(TestCatalogCommitter))
            .unwrap()
            .commit(&engine)
            .unwrap();
        let v0_snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(0)
            .build(&engine)
            .unwrap();
        let result = v0_snapshot
            .transaction(Box::new(TestCatalogCommitter), &engine)
            .unwrap()
            .commit(&engine)
            .unwrap();
        assert!(result.is_committed());

        let snapshot = Snapshot::builder_for(table_path)
            .with_max_catalog_version(1)
            .build(&engine)
            .unwrap();
        assert_eq!(snapshot.version(), 1);

        let err = build_uc_create_table_request(&snapshot, &engine, "x").unwrap_err();
        assert!(
            err.to_string().contains("version 0"),
            "expected version 0 error, got: {err}"
        );
    }
}
