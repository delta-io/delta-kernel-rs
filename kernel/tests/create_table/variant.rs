//! Variant type integration tests for the CreateTable API.
//!
//! Tests that creating a table with Variant columns in the schema automatically adds the
//! `variantType` feature to the protocol, and that Variant columns interact correctly
//! with other features (column mapping, clustering rejection).

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    ColumnMappingMode, TableFeature, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::{assert_result_error_with_message, test_table_setup};

/// Schema with a top-level variant column: (id INT, v VARIANT)
fn top_level_variant_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("v", DataType::unshredded_variant(), true),
    ]))
}

/// Schema with a variant nested inside a struct: (id INT, nested STRUCT<inner_v VARIANT>)
fn nested_variant_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new(
            "nested",
            DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                "inner_v",
                DataType::unshredded_variant(),
                true,
            )]))),
            true,
        ),
    ]))
}

/// Schema with multiple top-level variant columns: (id INT, v1 VARIANT, v2 VARIANT)
fn multiple_variant_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("v1", DataType::unshredded_variant(), true),
        StructField::new("v2", DataType::unshredded_variant(), true),
    ]))
}

/// Returns column mapping table properties for the given mode, or empty for "none".
fn cm_properties(mode: &str) -> Vec<(&str, &str)> {
    if mode == "none" {
        vec![]
    } else {
        vec![("delta.columnMapping.mode", mode)]
    }
}

/// Asserts the snapshot's protocol includes variantType with correct reader/writer versions.
fn assert_variant_protocol(snapshot: &Snapshot) {
    let table_config = snapshot.table_configuration();
    assert!(
        table_config.is_feature_supported(&TableFeature::VariantType),
        "variantType feature should be supported"
    );
    let protocol = table_config.protocol();
    assert!(
        protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION,
        "Reader version should be at least {}",
        TABLE_FEATURES_MIN_READER_VERSION
    );
    assert!(
        protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION,
        "Writer version should be at least {}",
        TABLE_FEATURES_MIN_WRITER_VERSION
    );
}

/// Variant schema auto-enables variantType across schema shapes and column mapping modes.
#[rstest::rstest]
#[case::top_level_no_cm(top_level_variant_schema(), "none")]
#[case::top_level_cm_name(top_level_variant_schema(), "name")]
#[case::top_level_cm_id(top_level_variant_schema(), "id")]
#[case::nested_no_cm(nested_variant_schema(), "none")]
#[case::nested_cm_name(nested_variant_schema(), "name")]
#[case::nested_cm_id(nested_variant_schema(), "id")]
#[case::multiple_no_cm(multiple_variant_schema(), "none")]
#[case::multiple_cm_name(multiple_variant_schema(), "name")]
#[case::multiple_cm_id(multiple_variant_schema(), "id")]
#[test]
fn test_create_table_with_variant(
    #[case] schema: Arc<StructType>,
    #[case] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties(cm_properties(cm_mode))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_variant_protocol(&snapshot);

    if cm_mode != "none" {
        let table_config = snapshot.table_configuration();
        assert!(
            table_config.is_feature_supported(&TableFeature::ColumnMapping),
            "columnMapping feature should be supported when cm_mode={cm_mode}"
        );
        let expected_mode = match cm_mode {
            "name" => ColumnMappingMode::Name,
            "id" => ColumnMappingMode::Id,
            _ => unreachable!(),
        };
        assert_eq!(table_config.column_mapping_mode(), expected_mode);
    }

    // Verify the schema round-trips correctly (strip CM metadata before comparing,
    // since the read-back schema has physical names/IDs that the original doesn't).
    let read_schema = snapshot.schema();
    let stripped = super::column_mapping::strip_column_mapping_metadata(&read_schema);
    assert_eq!(
        &stripped,
        schema.as_ref(),
        "Schema should round-trip through create table"
    );

    Ok(())
}

/// A schema without variant columns should not add the variantType feature.
#[test]
fn test_create_table_no_variant_no_feature() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let table_config = snapshot.table_configuration();
    assert!(
        !table_config.is_feature_supported(&TableFeature::VariantType),
        "variantType feature should NOT be in protocol for non-variant schema"
    );

    Ok(())
}

/// Clustering on a variant column is rejected per the Delta spec.
#[test]
fn test_create_table_variant_clustering_rejected() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("v", DataType::unshredded_variant(), true),
    ]));

    let result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::clustered(["v"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "unsupported type");

    Ok(())
}
