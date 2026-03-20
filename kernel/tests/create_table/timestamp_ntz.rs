//! TimestampNTZ integration tests for the CreateTable API.
//!
//! Tests that creating a table with TimestampNTZ columns in the schema automatically adds the
//! `timestampNtz` feature to the protocol, and that TimestampNTZ columns interact correctly
//! with other features (column mapping, variant).

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    ColumnMappingMode, TableFeature, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use test_utils::{
    cm_properties, multiple_ntz_schema, nested_ntz_schema, test_table_setup, top_level_ntz_schema,
};

/// Asserts the snapshot's protocol includes timestampNtz with correct reader/writer versions.
fn assert_timestamp_ntz_protocol(snapshot: &Snapshot) {
    let table_config = snapshot.table_configuration();
    assert!(
        table_config.is_feature_supported(&TableFeature::TimestampWithoutTimezone),
        "timestampNtz feature should be supported"
    );
    let protocol = table_config.protocol();
    assert!(
        protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION,
        "Reader version should be at least {TABLE_FEATURES_MIN_READER_VERSION}"
    );
    assert!(
        protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION,
        "Writer version should be at least {TABLE_FEATURES_MIN_WRITER_VERSION}"
    );
}

/// TimestampNTZ schema auto-enables timestampNtz across schema shapes and column mapping modes.
#[rstest::rstest]
fn test_create_table_with_timestamp_ntz(
    #[values(top_level_ntz_schema(), nested_ntz_schema(), multiple_ntz_schema())] schema: Arc<
        StructType,
    >,
    #[values("none", "name", "id")] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties(cm_properties(cm_mode))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_timestamp_ntz_protocol(&snapshot);

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

    let read_schema = snapshot.schema();
    let stripped = super::column_mapping::strip_column_mapping_metadata(&read_schema);
    assert_eq!(
        &stripped,
        schema.as_ref(),
        "Schema should round-trip through create table"
    );

    Ok(())
}

/// A schema without TimestampNTZ columns should not add the timestampNtz feature.
#[test]
fn test_create_table_no_timestamp_ntz_no_feature() -> DeltaResult<()> {
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
        !table_config.is_feature_supported(&TableFeature::TimestampWithoutTimezone),
        "timestampNtz feature should NOT be in protocol for non-NTZ schema"
    );

    Ok(())
}

/// A schema with both TimestampNTZ and Variant columns enables both features.
#[test]
fn test_create_table_timestamp_ntz_and_variant() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("ts", DataType::TIMESTAMP_NTZ, true),
        StructField::new("v", DataType::unshredded_variant(), true),
    ]));

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let table_config = snapshot.table_configuration();
    assert!(
        table_config.is_feature_supported(&TableFeature::TimestampWithoutTimezone),
        "timestampNtz feature should be supported"
    );
    assert!(
        table_config.is_feature_supported(&TableFeature::VariantType),
        "variantType feature should be supported"
    );

    Ok(())
}
