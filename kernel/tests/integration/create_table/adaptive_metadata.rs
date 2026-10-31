//! Integration tests for adaptive metadata table creation.
#![cfg(feature = "adaptive-metadata-in-dev")]

use delta_kernel::schema::schema_ref;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::Result;
use rstest::rstest;
use test_utils::{assert_result_error_with_message, test_table_setup, TestCatalogCommitter};

const ADAPTIVE_METADATA_FEATURE: &str = "delta.feature.adaptiveMetadata-preview";

#[rstest]
#[case::adaptive_metadata(false)]
#[case::iceberg_native_v4(true)]
#[tokio::test]
async fn test_adaptive_metadata_create_reloads_v0(#[case] native_v4: bool) -> Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };

    let mut builder = create_table(&table_path, schema, "test").with_table_properties([
        ("delta.enableInCommitTimestamps", "true"),
        ("delta.universalFormat.enabledFormats", "iceberg"),
        ("delta.feature.vacuumProtocolCheck", "supported"),
        ("delta.feature.timestampNtz", "supported"),
        ("delta.enableTypeWidening", "true"),
        ("delta.columnMapping.mode", "id"),
        ("delta.enableRowTracking", "true"),
        ("delta.enableDeletionVectors", "true"),
        ("delta.feature.catalogManaged", "supported"),
        ("format-version", "4"),
        (ADAPTIVE_METADATA_FEATURE, "supported"),
    ]);
    if native_v4 {
        builder =
            builder.with_table_properties([("delta.feature.icebergNativeV4-preview", "supported")]);
    }
    builder
        .build(engine.as_ref(), Box::new(TestCatalogCommitter))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let v0 = Snapshot::builder_for(&table_path)
        .with_max_catalog_version(0)
        .build(engine.as_ref())?;
    assert_eq!(v0.version(), 0);
    assert_eq!(
        v0.table_properties().column_mapping_mode,
        Some(ColumnMappingMode::Id)
    );
    assert_eq!(v0.table_properties().enable_row_tracking, Some(true));
    assert_eq!(v0.table_properties().enable_deletion_vectors, Some(true));
    assert_eq!(
        v0.table_properties().enable_in_commit_timestamps,
        Some(true)
    );
    for feature in [
        TableFeature::AdaptiveMetadataPreview,
        TableFeature::CatalogManaged,
        TableFeature::ColumnMapping,
        TableFeature::DeletionVectors,
        TableFeature::RowTracking,
        TableFeature::DomainMetadata,
        TableFeature::InCommitTimestamp,
    ] {
        assert!(v0.table_configuration().is_feature_enabled(&feature));
    }
    assert_eq!(
        v0.table_configuration()
            .is_feature_enabled(&TableFeature::IcebergNativeV4Preview),
        native_v4
    );
    assert!(!v0
        .table_configuration()
        .is_feature_enabled(&TableFeature::IcebergCompatV3));

    Ok(())
}

#[rstest]
#[case::missing_adaptive_metadata(None, "adaptiveMetadata-preview")]
#[case::incompatible_iceberg_v3(Some("delta.enableIcebergCompatV3"), "icebergCompatV3")]
#[case::incompatible_append_only(Some("delta.appendOnly"), "appendOnly")]
#[tokio::test]
async fn test_native_v4_create_rejects_invalid_protocol(
    #[case] incompatible_property: Option<&str>,
    #[case] expected_error: &str,
) -> Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let mut builder = create_table(&table_path, schema, "test").with_table_properties([
        ("delta.feature.icebergNativeV4-preview", "supported"),
        ("delta.feature.catalogManaged", "supported"),
        ("delta.columnMapping.mode", "id"),
        ("delta.enableRowTracking", "true"),
        ("delta.enableDeletionVectors", "true"),
        ("delta.enableInCommitTimestamps", "true"),
    ]);
    if let Some(property) = incompatible_property {
        builder = builder
            .with_table_properties([(ADAPTIVE_METADATA_FEATURE, "supported"), (property, "true")]);
    }
    assert_result_error_with_message(
        builder.build(engine.as_ref(), Box::new(TestCatalogCommitter)),
        expected_error,
    );
    Ok(())
}
