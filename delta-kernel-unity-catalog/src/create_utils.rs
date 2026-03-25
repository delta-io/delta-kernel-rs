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
//! let disk_props = get_required_properties_for_disk(staging_info.table_id);
//! let create_table_txn = kernel::create_table(path, schema, "MyApp/1.0")
//!     .with_table_properties(disk_props)
//!     .build(engine, committer);
//! let result = create_table_txn.commit(engine);
//!
//! // Step 3: Finalize table in UC
//! let snapshot = /* load post-commit snapshot at version 0 */;
//! let uc_props = get_final_required_properties_for_uc(&snapshot, engine)?;
//! my_uc_client.create_table(.., uc_props);
//! ```

use std::collections::HashMap;

use delta_kernel::{Engine, Snapshot};

/// Property key for the UC table ID, stored in Delta metadata configuration.
const UC_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
/// Property key to enable in-commit timestamps.
const ENABLE_IN_COMMIT_TIMESTAMPS: &str = "delta.enableInCommitTimestamps";
/// Feature supported value.
const FEATURE_SUPPORTED: &str = "supported";
/// The property key prefix for table feature signals.
const FEATURE_PREFIX: &str = "delta.feature.";
/// Feature signal key for catalog-managed tables.
const CATALOG_MANAGED_FEATURE_KEY: &str = "delta.feature.catalogManaged";
/// Feature signal key for vacuum protocol check.
const VACUUM_PROTOCOL_CHECK_FEATURE_KEY: &str = "delta.feature.vacuumProtocolCheck";
/// The property key for the minimum reader version.
const MIN_READER_VERSION_KEY: &str = "delta.minReaderVersion";
/// The property key for the minimum writer version.
const MIN_WRITER_VERSION_KEY: &str = "delta.minWriterVersion";
/// UC property for the last committed version.
const METASTORE_LAST_UPDATE_VERSION: &str = "delta.lastUpdateVersion";
/// UC property for the last commit timestamp.
const METASTORE_LAST_COMMIT_TIMESTAMP: &str = "delta.lastCommitTimestamp";

/// Returns the table properties that must be written to disk (in `000.json`) for a UC catalog-managed table.
///
/// These properties must be persisted in the Delta log so that the table is
/// recognized as catalog-managed. Pass the returned map to
/// `CreateTableTransactionBuilder::with_table_properties()`. The builder will separate feature
/// signals from configuration properties internally.
///
/// # Properties returned
///
/// - `delta.feature.catalogManaged = "supported"` -- enables the catalog-managed table feature
/// - `delta.feature.vacuumProtocolCheck = "supported"` -- required by UC
/// - `io.unitycatalog.tableId = <uc_table_id>` -- persisted in metadata configuration
/// - `delta.enableInCommitTimestamps = "true"` -- enables in-commit timestamps (required by
///   catalog-managed tables)
pub fn get_required_properties_for_disk(uc_table_id: &str) -> HashMap<String, String> {
    HashMap::from([
        (
            CATALOG_MANAGED_FEATURE_KEY.to_string(),
            FEATURE_SUPPORTED.to_string(),
        ),
        (
            VACUUM_PROTOCOL_CHECK_FEATURE_KEY.to_string(),
            FEATURE_SUPPORTED.to_string(),
        ),
        (UC_TABLE_ID_KEY.to_string(), uc_table_id.to_string()),
        (ENABLE_IN_COMMIT_TIMESTAMPS.to_string(), "true".to_string()),
    ])
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
/// Note: This performs log replay to read in-commit timestamp and clustering domain metadata.
///
/// # Clustering columns
///
/// Clustering columns are returned as logical column names.
/// When column mapping is enabled, the physical names stored in domain metadata are converted
/// to logical names using the table schema.
pub fn get_final_required_properties_for_uc(
    snapshot: &Snapshot,
    engine: &dyn Engine,
) -> delta_kernel::DeltaResult<HashMap<String, String>> {
    // Start with metadata configuration (user + delta properties)
    let mut properties = snapshot.metadata_configuration().clone();

    // Protocol-derived properties (versions + features)
    properties.insert(
        MIN_READER_VERSION_KEY.to_string(),
        snapshot.min_reader_version().to_string(),
    );
    properties.insert(
        MIN_WRITER_VERSION_KEY.to_string(),
        snapshot.min_writer_version().to_string(),
    );

    // Feature signals: delta.feature.<name> = "supported" for all reader + writer features
    if let Some(features) = snapshot.reader_features() {
        for feature_name in features {
            properties
                .entry(format!("{FEATURE_PREFIX}{feature_name}"))
                .or_insert_with(|| FEATURE_SUPPORTED.to_string());
        }
    }
    if let Some(features) = snapshot.writer_features() {
        for feature_name in features {
            properties
                .entry(format!("{FEATURE_PREFIX}{feature_name}"))
                .or_insert_with(|| FEATURE_SUPPORTED.to_string());
        }
    }

    // UC-specific properties
    properties.insert(
        METASTORE_LAST_UPDATE_VERSION.to_string(),
        snapshot.version().to_string(),
    );
    let timestamp = snapshot
        .get_in_commit_timestamp(engine)?
        .ok_or_else(|| {
            delta_kernel::Error::generic(
                "In-commit timestamp is required for UC catalog-managed tables but was not found",
            )
        })?;
    properties.insert(
        METASTORE_LAST_COMMIT_TIMESTAMP.to_string(),
        timestamp.to_string(),
    );

    // Clustering columns as logical names (if present)
    if let Some(columns) = snapshot.get_clustering_columns(engine)? {
        let column_arrays: Vec<Vec<&str>> = columns
            .iter()
            .map(|c| c.path().iter().map(|s| s.as_str()).collect())
            .collect();
        let json = serde_json::to_string(&column_arrays).map_err(|e| {
            delta_kernel::Error::generic(format!(
                "Failed to serialize clustering columns: {e}"
            ))
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

    /// A mock catalog committer that writes directly to the published path (like
    /// FileSystemCommitter) but reports `is_catalog_committer() = true`.
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
    fn test_get_required_properties_for_disk() {
        let props = get_required_properties_for_disk("my-uc-table-123");

        // Feature signals
        assert_eq!(
            props.get("delta.feature.catalogManaged"),
            Some(&"supported".to_string())
        );
        assert_eq!(
            props.get("delta.feature.vacuumProtocolCheck"),
            Some(&"supported".to_string())
        );
        // Configuration
        assert_eq!(
            props.get("io.unitycatalog.tableId"),
            Some(&"my-uc-table-123".to_string())
        );
        assert_eq!(
            props.get("delta.enableInCommitTimestamps"),
            Some(&"true".to_string())
        );
        assert_eq!(props.len(), 4);
    }

    #[tokio::test]
    async fn test_get_final_required_properties_for_uc() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_table/";
        let schema = Arc::new(
            StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap(),
        );

        // Create a UC catalog-managed table with the required properties.
        let disk_props = get_required_properties_for_disk("test-table-id-456");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .build(&engine, Box::new(MockCatalogCommitter))
            .unwrap()
            .commit(&engine)
            .unwrap();

        // Load the post-commit snapshot
        let snapshot = Snapshot::builder_for(table_path).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 0);

        // Extract UC properties
        let uc_props = get_final_required_properties_for_uc(&snapshot, &engine).unwrap();

        // Verify protocol-derived properties
        assert_eq!(
            uc_props.get("delta.minReaderVersion"),
            Some(&"3".to_string())
        );
        assert_eq!(
            uc_props.get("delta.minWriterVersion"),
            Some(&"7".to_string())
        );

        // Verify feature signals are present for all protocol features
        assert_eq!(
            uc_props.get("delta.feature.catalogManaged"),
            Some(&"supported".to_string())
        );
        assert_eq!(
            uc_props.get("delta.feature.vacuumProtocolCheck"),
            Some(&"supported".to_string())
        );
        // ICT is enabled via delta.enableInCommitTimestamps=true in disk properties
        assert_eq!(
            uc_props.get("delta.feature.inCommitTimestamp"),
            Some(&"supported".to_string())
        );

        // Verify metadata configuration properties
        assert_eq!(
            uc_props.get("io.unitycatalog.tableId"),
            Some(&"test-table-id-456".to_string())
        );

        // Verify UC-specific properties
        assert_eq!(
            uc_props.get("delta.lastUpdateVersion"),
            Some(&"0".to_string())
        );
        // ICT is enabled, so the timestamp should be a non-zero value
        let timestamp: i64 = uc_props
            .get("delta.lastCommitTimestamp")
            .expect("delta.lastCommitTimestamp should be present")
            .parse()
            .expect("timestamp should be a valid i64");
        assert!(timestamp > 0, "ICT timestamp should be non-zero, got {timestamp}");
    }

    #[tokio::test]
    async fn test_get_final_required_properties_for_uc_with_clustering() {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let table_path = "memory:///test_clustered_table/";
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::new("id", DataType::INTEGER, false),
                StructField::new("region", DataType::STRING, true),
            ])
            .unwrap(),
        );

        let disk_props = get_required_properties_for_disk("clustered-table-id");
        let _ = create_table(table_path, schema, "Test/1.0")
            .with_table_properties(disk_props)
            .with_data_layout(DataLayout::clustered(["region"]))
            .build(&engine, Box::new(MockCatalogCommitter))
            .unwrap()
            .commit(&engine)
            .unwrap();

        let snapshot = Snapshot::builder_for(table_path).build(&engine).unwrap();
        let uc_props = get_final_required_properties_for_uc(&snapshot, &engine).unwrap();

        // Verify clustering columns are present in UC properties
        let clustering_json = uc_props
            .get("clusteringColumns")
            .expect("clusteringColumns should be present");
        let parsed: Vec<Vec<String>> = serde_json::from_str(clustering_json).unwrap();
        assert_eq!(parsed, vec![vec!["region"]]);

        // Verify the clustering feature is in the protocol
        assert_eq!(
            uc_props.get("delta.feature.clustering"),
            Some(&"supported".to_string())
        );
    }
}
