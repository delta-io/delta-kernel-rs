//! Utilities for Unity Catalog catalog-managed table creation.
//!
//! These utilities help connectors create UC-managed tables by providing the required properties
//! for both the Delta log (disk) and the UC server registration.
//!
//! # Usage
//!
//! ```ignore
//! // Step 1: Get staging info from UC
//! let staging_info = my_uc_client.get_staging_table(..);
//!
//! // Step 2: Build and commit the create-table transaction
//! let disk_props = get_create_table_disk_properties(staging_info.table_id);
//! let create_table_txn = kernel::create_table(path, schema, "MyApp/1.0")
//!     .with_table_properties(disk_props)
//!     .build(engine, committer);
//! let result = create_table_txn.commit(engine);
//!
//! // Step 3: Finalize table in UC
//! let snapshot = /* load post-commit snapshot at version 0 */;
//! let uc_props = get_create_table_uc_properties(&snapshot, engine)?;
//! my_uc_client.create_table(.., uc_props);
//! ```

use std::collections::HashMap;

use delta_kernel::{Engine, Snapshot};

use crate::constants::{
    CATALOG_MANAGED_FEATURE_KEY, FEATURE_SUPPORTED, METASTORE_LAST_COMMIT_TIMESTAMP,
    METASTORE_LAST_UPDATE_VERSION, UC_TABLE_ID_KEY, VACUUM_PROTOCOL_CHECK_FEATURE_KEY,
};

/// Returns the table properties that must be written to disk (in `000.json`) for a UC
/// catalog-managed table creation.
///
/// These properties must be persisted in the Delta log so that the table is recognized as
/// catalog-managed. Note: ICT enablement is handled automatically by kernel's CREATE TABLE
/// when the `catalogManaged` feature is present.
pub fn get_create_table_disk_properties(uc_table_id: &str) -> HashMap<String, String> {
    [
        (CATALOG_MANAGED_FEATURE_KEY, FEATURE_SUPPORTED),
        (VACUUM_PROTOCOL_CHECK_FEATURE_KEY, FEATURE_SUPPORTED),
        (UC_TABLE_ID_KEY, uc_table_id),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Extracts the properties that must be sent to the UC server when finalizing a table creation.
///
/// These properties are derived from the post-commit snapshot (after `000.json` has
/// been written). The connector should pass these to the UC `create_table` API.
///
/// # Properties returned
///
/// - All entries from `Metadata.configuration` (includes `io.unitycatalog.tableId`, user props)
/// - `delta.minReaderVersion` and `delta.minWriterVersion`
/// - `delta.feature.<name> = "supported"` for every reader and writer table feature
/// - `delta.lastUpdateVersion` -- the snapshot version
/// - `delta.lastCommitTimestamp` -- the snapshot's in-commit timestamp (requires ICT enabled)
/// - `clusteringColumns` -- JSON-serialized clustering columns (if clustering is enabled)
///
/// # Clustering columns
///
/// Clustering columns are returned as logical column names. When column mapping is enabled,
/// the physical names stored in domain metadata are converted to logical names using the
/// table schema.
pub fn get_create_table_uc_properties(
    snapshot: &Snapshot,
    engine: &dyn Engine,
) -> delta_kernel::DeltaResult<HashMap<String, String>> {
    if snapshot.version() != 0 {
        return Err(delta_kernel::Error::generic(format!(
            "get_create_table_uc_properties is only valid for version 0 (table creation) \
             snapshots, but snapshot is at version {}",
            snapshot.version()
        )));
    }

    // Start with metadata configuration (user + delta properties)
    let mut properties = snapshot.metadata_configuration().clone();

    // Protocol-derived properties (versions + feature signals)
    properties.extend(snapshot.get_protocol_derived_properties());

    // UC-specific properties
    properties.insert(
        METASTORE_LAST_UPDATE_VERSION.to_string(),
        snapshot.version().to_string(),
    );
    let timestamp = snapshot.get_in_commit_timestamp(engine)?.ok_or_else(|| {
        delta_kernel::Error::generic(
            "In-commit timestamp is required for UC catalog-managed tables but was not found",
        )
    })?;
    properties.insert(
        METASTORE_LAST_COMMIT_TIMESTAMP.to_string(),
        timestamp.to_string(),
    );

    // Clustering columns as logical names (if present)
    if let Some(columns) = snapshot.get_logical_clustering_columns(engine)? {
        let column_arrays: Vec<Vec<&str>> = columns
            .iter()
            .map(|c| c.path().iter().map(|s| s.as_str()).collect())
            .collect();
        let json = serde_json::to_string(&column_arrays).map_err(|e| {
            delta_kernel::Error::generic(format!("Failed to serialize clustering columns: {e}"))
        })?;
        properties.insert("clusteringColumns".to_string(), json);
    }

    Ok(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, PublishMetadata};
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::snapshot::Snapshot;
    use delta_kernel::transaction::create_table::create_table;
    use delta_kernel::transaction::data_layout::DataLayout;
    use delta_kernel::{DeltaResult, Engine, FileMeta, FilteredEngineData};

    /// A mock catalog committer that writes directly to the published path.
    struct MockCatalogCommitter;
    impl Committer for MockCatalogCommitter {
        fn commit(
            &self,
            engine: &dyn Engine,
            actions: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
            commit_metadata: CommitMetadata,
        ) -> DeltaResult<CommitResponse> {
            let path = commit_metadata.published_commit_path()?;
            engine
                .json_handler()
                .write_json_file(&path, Box::new(actions), false)?;
            Ok(CommitResponse::Committed {
                file_meta: FileMeta::new(path, commit_metadata.in_commit_timestamp(), 0),
            })
        }
        fn is_catalog_committer(&self) -> bool {
            true
        }
        fn publish(&self, _: &dyn Engine, _: PublishMetadata) -> DeltaResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_get_create_table_disk_properties() {
        let props = get_create_table_disk_properties("my-uc-table-123");
        assert_eq!(props.len(), 3);
        assert_eq!(props["delta.feature.catalogManaged"], "supported");
        assert_eq!(props["delta.feature.vacuumProtocolCheck"], "supported");
        assert_eq!(props["io.unitycatalog.tableId"], "my-uc-table-123");
    }

    #[tokio::test]
    async fn test_get_create_table_uc_properties() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_table/";
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::new("id", DataType::INTEGER, false),
                StructField::new("region", DataType::STRING, true),
            ])
            .unwrap(),
        );

        // Create a UC catalog-managed table with clustering
        let disk_props = get_create_table_disk_properties("test-table-id-456");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .with_data_layout(DataLayout::clustered(["region"]))
            .build(&engine, Box::new(MockCatalogCommitter))
            .unwrap()
            .commit(&engine)
            .unwrap();

        let snapshot = Snapshot::builder_for(table_path).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 0);
        let uc_props = get_create_table_uc_properties(&snapshot, &engine).unwrap();

        // Protocol-derived properties
        assert_eq!(uc_props["delta.minReaderVersion"], "3");
        assert_eq!(uc_props["delta.minWriterVersion"], "7");
        assert_eq!(uc_props["delta.feature.catalogManaged"], "supported");
        assert_eq!(uc_props["delta.feature.vacuumProtocolCheck"], "supported");
        assert_eq!(uc_props["delta.feature.inCommitTimestamp"], "supported");
        assert_eq!(uc_props["delta.feature.clustering"], "supported");

        // Metadata configuration
        assert_eq!(uc_props["io.unitycatalog.tableId"], "test-table-id-456");

        // UC-specific properties
        assert_eq!(uc_props["delta.lastUpdateVersion"], "0");
        let timestamp: i64 = uc_props["delta.lastCommitTimestamp"]
            .parse()
            .expect("timestamp should be a valid i64");
        assert!(
            timestamp > 0,
            "ICT timestamp should be non-zero, got {timestamp}"
        );

        // Clustering columns
        let parsed: Vec<Vec<String>> =
            serde_json::from_str(&uc_props["clusteringColumns"]).unwrap();
        assert_eq!(parsed, vec![vec!["region"]]);
    }

    #[tokio::test]
    async fn test_get_create_table_uc_properties_rejects_non_zero_version() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_version_check/";
        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap(),
        );

        // Create a table (version 0) and append (version 1)
        let disk_props = get_create_table_disk_properties("test-table-id");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .build(&engine, Box::new(MockCatalogCommitter))
            .unwrap()
            .commit(&engine)
            .unwrap();
        let v0_snapshot = Snapshot::builder_for(table_path).build(&engine).unwrap();
        let result = v0_snapshot
            .transaction(Box::new(MockCatalogCommitter), &engine)
            .unwrap()
            .commit(&engine)
            .unwrap();
        assert!(result.is_committed());

        // Load snapshot at version 1
        let snapshot = Snapshot::builder_for(table_path).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 1);

        // Should fail because version != 0
        let err = get_create_table_uc_properties(&snapshot, &engine).unwrap_err();
        assert!(
            err.to_string().contains("version 0"),
            "expected version 0 error, got: {err}"
        );
    }
}
