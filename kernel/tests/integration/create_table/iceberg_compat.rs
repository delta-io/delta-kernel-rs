//! IcebergCompatV3 integration tests for the CreateTable API.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::test_table_setup;

/// V3 create-table negative paths: enabling V3 alongside an incompatible property must fail at
/// `.build(...)` with a clear error.
///
/// `cm_mode_none` and `row_tracking_disabled` are blocked by V3's dependency check
/// (`maybe_enable_iceberg_compat_v3_dependencies`); the others are blocked earlier because
/// the property is not in `ALLOWED_DELTA_PROPERTIES` for CREATE TABLE. Keep them here
/// so that in the future when we support these properties for create table, we will remember
/// to update this test.
#[rstest]
#[case::cm_mode_none(
    &[("delta.columnMapping.mode", "none")],
    "to be 'name' or 'id', got 'none'",
)]
#[case::row_tracking_disabled(
    &[("delta.enableRowTracking", "false")],
    "to be 'true', got 'false'",
)]
#[case::iceberg_compat_v1_active(
    &[("delta.enableIcebergCompatV1", "true")],
    "Setting delta property 'delta.enableIcebergCompatV1' is not supported",
)]
#[case::iceberg_compat_v2_active(
    &[("delta.enableIcebergCompatV2", "true")],
    "Setting delta property 'delta.enableIcebergCompatV2' is not supported",
)]
fn v3_create_table_rejects_incompatible_props(
    #[case] extra_props: &[(&str, &str)],
    #[case] err_substring: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let mut props: Vec<(&str, &str)> = vec![("delta.enableIcebergCompatV3", "true")];
    props.extend_from_slice(extra_props);

    let err = create_table(&table_path, super::simple_schema()?, "Test/1.0")
        .with_table_properties(props)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(err_substring),
        "expected error containing {err_substring:?}, got: {err}",
    );
    Ok(())
}

/// Listing IcebergCompatV3 in writerFeatures (i.e. "supported") without setting
/// `delta.enableIcebergCompatV3=true` must not activate V3: column mapping stays off and
/// no nested-id metadata is set on the Map field.
#[test]
fn v3_supported_but_not_enabled_skips_cm_and_nested_ids() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "data",
        DataType::Map(Box::new(MapType::new(
            DataType::INTEGER,
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            true,
        ))),
    )])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.icebergCompatV3", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;
    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;

    // 1. V3 is in writerFeatures (supported).
    let writer_features = snapshot
        .table_configuration()
        .protocol()
        .writer_features()
        .expect("writerFeatures present");
    assert!(
        writer_features.contains(&TableFeature::IcebergCompatV3),
        "expected icebergCompatV3 in writerFeatures, got: {writer_features:?}",
    );

    // 2. CM is not enabled (no auto-enable from V3 being merely supported).
    assert_eq!(
        snapshot.table_configuration().column_mapping_mode(),
        ColumnMappingMode::None,
    );

    // 3. No nested-id metadata set on the Map field.
    let loaded_schema = snapshot.schema();
    let data_field = loaded_schema.field("data").expect("data field present");
    assert!(
        !data_field
            .metadata
            .contains_key(ColumnMetadataKey::ColumnMappingNestedIds.as_ref()),
        "unexpected delta.columnMapping.nested.ids on data: {:?}",
        data_field.metadata,
    );
    Ok(())
}
