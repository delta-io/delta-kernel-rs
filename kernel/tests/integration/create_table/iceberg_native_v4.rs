//! IcebergNativeV4 integration tests for the CreateTable API.
#![cfg(feature = "adaptive-metadata-in-dev")]

use std::collections::HashSet;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::UpdateTableOperation;
use test_utils::{read_actions_from_commit, test_table_setup};
use url::Url;

#[tokio::test]
async fn test_create_table_iceberg_native_v4() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! {
        nullable "id": LONG,
        nullable "name": STRING,
    };

    create_table(&table_path, schema, "Test/1.0", true)
        .with_table_properties([("delta.enableIcebergNativeV4", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    let table_configuration = snapshot.table_configuration();
    assert!(table_configuration.is_feature_enabled(&TableFeature::IcebergNativeV4Preview));

    let protocol = table_configuration.protocol();
    let reader_features: HashSet<_> = protocol.reader_features().unwrap().iter().collect();
    assert_eq!(
        reader_features,
        HashSet::from([
            &TableFeature::ColumnMapping,
            &TableFeature::DeletionVectors,
            &TableFeature::AdaptiveMetadataPreview,
        ])
    );
    let writer_features: HashSet<_> = protocol.writer_features().unwrap().iter().collect();
    assert_eq!(
        writer_features,
        HashSet::from([
            &TableFeature::ColumnMapping,
            &TableFeature::DeletionVectors,
            &TableFeature::RowTracking,
            &TableFeature::DomainMetadata,
            &TableFeature::InCommitTimestamp,
            &TableFeature::AdaptiveMetadataPreview,
            &TableFeature::IcebergNativeV4Preview,
        ])
    );

    let properties = snapshot.table_properties();
    assert_eq!(properties.enable_iceberg_native_v4, Some(true));
    assert_eq!(properties.column_mapping_mode, Some(ColumnMappingMode::Id));
    assert_eq!(properties.column_mapping_max_column_id, Some(2));
    assert_eq!(properties.enable_row_tracking, Some(true));
    assert_eq!(properties.enable_deletion_vectors, Some(true));
    assert_eq!(properties.enable_in_commit_timestamps, Some(true));
    assert!(properties.materialized_row_id_column_name.is_some());
    assert!(properties
        .materialized_row_commit_version_column_name
        .is_some());

    let table_url = Url::from_directory_path(&table_path).expect("valid path");
    assert!(read_actions_from_commit(&table_url, 0, "add")?.is_empty());
    assert!(read_actions_from_commit(&table_url, 0, "checkpoint")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_commit_without_file_actions_after_create_iceberg_native_v4(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    create_table(
        &table_path,
        schema_ref! { nullable "id": LONG },
        "Test/1.0",
        true,
    )
    .with_table_properties([("delta.enableIcebergNativeV4", "true")])
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_committed();

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let committed = snapshot
        .update_table(UpdateTableOperation::Write, "Test/1.0", false)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    assert_eq!(committed.commit_version(), 1);
    Ok(())
}
