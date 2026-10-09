//! File type integration tests for the CreateTable API.
//!
//! Tests that creating a table with File columns in the schema automatically adds the
//! `fileType-preview` feature to the protocol (and not the stable `fileType`), and that File
//! columns interact correctly with other features (column mapping, clustering and map-key
//! rejection).

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{schema_ref, ArrayType, DataType, MapType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    ColumnMappingMode, TableFeature, TABLE_FEATURES_MIN_READER_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Result;
use test_utils::{
    assert_result_error_with_message, cm_properties, multiple_file_schema, nested_file_schema,
    test_table_setup, top_level_file_schema,
};

/// A schema with a single `file` column held in an array.
fn array_of_file_schema() -> Arc<StructType> {
    let column = StructField::nullable("col", ArrayType::new(DataType::file_type(), true));
    Arc::new(StructType::try_new([column]).unwrap())
}

/// A schema with a single column that is a map with `file` values.
fn map_value_file_schema() -> Arc<StructType> {
    let map = MapType::new(DataType::STRING, DataType::file_type(), true);
    Arc::new(StructType::try_new([StructField::nullable("col", map)]).unwrap())
}

/// Asserts the snapshot's protocol includes fileType-preview, and not the stable fileType, with
/// correct reader/writer versions.
fn assert_file_protocol(snapshot: &Snapshot) {
    let table_config = snapshot.table_configuration();
    assert!(
        table_config.is_feature_supported(&TableFeature::FileTypePreview),
        "fileType-preview feature should be supported"
    );
    let protocol = table_config.protocol();
    assert!(
        !table_config.is_feature_supported(&TableFeature::FileType),
        "the stable fileType feature must not be written while the protocol change is only proposed"
    );
    assert!(
        protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION,
        "Reader version should be at least {TABLE_FEATURES_MIN_READER_VERSION}"
    );
    assert!(
        protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION,
        "Writer version should be at least {TABLE_FEATURES_MIN_WRITER_VERSION}"
    );
}

/// File schema auto-enables fileType-preview across schema shapes and column mapping modes.
#[rstest::rstest]
fn test_create_table_with_file(
    #[values(
        top_level_file_schema(),
        nested_file_schema(),
        multiple_file_schema(),
        array_of_file_schema(),
        map_value_file_schema()
    )]
    schema: Arc<StructType>,
    #[values("none", "name", "id")] cm_mode: &str,
) -> Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties(cm_properties(cm_mode))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_file_protocol(&snapshot);

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

/// A schema without file columns should not add either file feature.
#[test]
fn test_create_table_no_file_no_feature() -> Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = schema_ref! {
        nullable "id": INTEGER,
        nullable "name": STRING,
    };

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let table_config = snapshot.table_configuration();
    for feature in [TableFeature::FileType, TableFeature::FileTypePreview] {
        assert!(
            !table_config.is_feature_supported(&feature),
            "{feature:?} should NOT be in protocol for non-file schema"
        );
    }

    Ok(())
}

/// A `file` cannot be a map key (it is not comparable), including inside a key type.
#[rstest::rstest]
#[case::file_key(MapType::new(DataType::file_type(), DataType::STRING, true))]
#[case::array_of_file_key(MapType::new(
    ArrayType::new(DataType::file_type(), true),
    DataType::STRING,
    true
))]
fn test_create_table_file_map_key_rejected(#[case] map: MapType) -> Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new([StructField::nullable("col", map)])?);

    let result = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "map key");

    Ok(())
}

/// Clustering on a file column is rejected (a `file` value is not a comparable type).
#[test]
fn test_create_table_file_clustering_rejected() -> Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let result = create_table(&table_path, top_level_file_schema(), "Test/1.0")
        .with_data_layout(DataLayout::clustered(["col"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "unsupported type");

    Ok(())
}
