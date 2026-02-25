//! Column Mapping integration tests for the CreateTable API.
//!
//! These tests use kernel's snapshot API to read back the table, which exercises
//! the full column mapping validation path (via TableConfiguration::try_new ->
//! validate_schema_column_mapping). This ensures the written schema is valid and
//! readable by kernel.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::{create_table_and_load_snapshot, test_table_setup};

use super::simple_schema;

/// Helper to strip column mapping metadata (IDs and physical names) from all StructFields recursively.
fn strip_column_mapping_metadata(schema: &StructType) -> StructType {
    let cm_id = ColumnMetadataKey::ColumnMappingId.as_ref();
    let cm_name = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();

    fn strip_field(field: &StructField, cm_id: &str, cm_name: &str) -> StructField {
        let mut metadata = field.metadata().clone();
        metadata.remove(cm_id);
        metadata.remove(cm_name);

        let data_type = strip_data_type(field.data_type(), cm_id, cm_name);
        StructField::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
    }

    fn strip_data_type(dt: &DataType, cm_id: &str, cm_name: &str) -> DataType {
        match dt {
            DataType::Struct(s) => {
                let fields: Vec<_> = s.fields().map(|f| strip_field(f, cm_id, cm_name)).collect();
                DataType::Struct(Box::new(StructType::new_unchecked(fields)))
            }
            DataType::Array(a) => DataType::from(ArrayType::new(
                strip_data_type(a.element_type(), cm_id, cm_name),
                a.contains_null(),
            )),
            DataType::Map(m) => DataType::from(MapType::new(
                strip_data_type(m.key_type(), cm_id, cm_name),
                strip_data_type(m.value_type(), cm_id, cm_name),
                m.value_contains_null(),
            )),
            other => other.clone(),
        }
    }

    let fields: Vec<_> = schema
        .fields()
        .map(|f| strip_field(f, cm_id, cm_name))
        .collect();
    StructType::new_unchecked(fields)
}

/// Assert column mapping configuration on a snapshot.
///
/// For `Name` / `Id`: feature supported & enabled, mode matches, `maxColumnId` equals
/// the recursive field count.
///
/// For `None`: mode is `None`, no `maxColumnId`, and no column mapping metadata (IDs or
/// physical names) on any field. Note: whether `ColumnMapping` appears in the protocol
/// depends on whether the feature flag was explicitly set, so that check is left to the
/// caller.
fn assert_column_mapping_config(snapshot: &Snapshot, expected_mode: ColumnMappingMode) {
    let table_config = snapshot.table_configuration();

    assert_eq!(
        table_config.column_mapping_mode(),
        expected_mode,
        "Column mapping mode mismatch"
    );

    match expected_mode {
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            assert!(
                table_config.is_feature_supported(&TableFeature::ColumnMapping),
                "Protocol should support columnMapping feature"
            );
            assert!(
                table_config.is_feature_enabled(&TableFeature::ColumnMapping),
                "ColumnMapping feature should be enabled"
            );

            let expected_max_id = snapshot.schema().total_struct_fields();
            let max_id_str = expected_max_id.to_string();
            let config = table_config.metadata().configuration();
            assert_eq!(
                config
                    .get("delta.columnMapping.maxColumnId")
                    .map(|s| s.as_str()),
                Some(max_id_str.as_str()),
                "maxColumnId should equal the total number of struct fields ({expected_max_id})"
            );
        }
        ColumnMappingMode::None => {
            // No maxColumnId property
            let config = table_config.metadata().configuration();
            assert!(
                config.get("delta.columnMapping.maxColumnId").is_none(),
                "maxColumnId should not be present when column mapping mode is None"
            );

            // No column mapping metadata on any field
            for field in snapshot.schema().fields() {
                assert!(
                    field
                        .get_config_value(&ColumnMetadataKey::ColumnMappingId)
                        .is_none(),
                    "Field '{}' should not have a column mapping ID when mode is None",
                    field.name()
                );
                assert!(
                    field
                        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                        .is_none(),
                    "Field '{}' should not have a physical name when mode is None",
                    field.name()
                );
            }
        }
    }
}

#[test]
fn test_create_table_with_column_mapping_name_mode() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create table and load snapshot (this validates column mapping annotations on read)
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);

    // Verify schema preserves field names, types, and nullability
    let read_schema = snapshot.schema();
    assert_eq!(read_schema.fields().count(), 2);

    let id_field = read_schema.field("id").expect("id field should exist");
    assert_eq!(id_field.data_type(), &DataType::INTEGER);
    assert!(!id_field.is_nullable());

    let value_field = read_schema
        .field("value")
        .expect("value field should exist");
    assert_eq!(value_field.data_type(), &DataType::STRING);
    assert!(value_field.is_nullable());

    Ok(())
}

#[test]
fn test_create_table_with_column_mapping_id_mode() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![StructField::new(
        "id",
        DataType::INTEGER,
        false,
    )])?);

    // Create table and load snapshot (validates column mapping on read)
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "id")],
    )?;

    assert_column_mapping_config(&snapshot, ColumnMappingMode::Id);

    // Verify schema
    let read_schema = snapshot.schema();
    assert_eq!(read_schema.fields().count(), 1);
    let id_field = read_schema.field("id").expect("id field should exist");
    assert_eq!(id_field.data_type(), &DataType::INTEGER);
    assert!(!id_field.is_nullable());

    Ok(())
}

#[test]
fn test_column_mapping_mode_none_no_annotations() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create table WITHOUT column mapping and load snapshot
    let snapshot = create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &[])?;

    // Verify protocol does NOT have columnMapping feature
    assert!(
        !snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::ColumnMapping),
        "Protocol should NOT have columnMapping feature when mode is not set"
    );

    // Verify no column mapping config (mode=None, no maxColumnId, no field metadata)
    assert_column_mapping_config(&snapshot, ColumnMappingMode::None);

    // Verify schema preserves fields
    let read_schema = snapshot.schema();
    assert_eq!(read_schema.fields().count(), 2);
    assert!(read_schema.field("id").is_some());
    assert!(read_schema.field("value").is_some());

    Ok(())
}

/// Test: setting `delta.feature.columnMapping=supported` without a mode means the feature
/// is in the protocol but column mapping is not active (mode resolves to `None`).
/// The schema should NOT have column mapping IDs or physical names.
#[test]
fn test_column_mapping_feature_only_without_mode() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create table with ONLY the feature flag, no delta.columnMapping.mode
    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.columnMapping", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Feature IS in the protocol (the feature signal put it there)
    assert!(
        snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::ColumnMapping),
        "Protocol should list columnMapping as a supported feature"
    );

    // But mode is None, no maxColumnId, no field metadata
    assert_column_mapping_config(&snapshot, ColumnMappingMode::None);

    Ok(())
}

#[test]
fn test_column_mapping_invalid_mode_rejected() {
    let (_temp_dir, table_path, engine) = test_table_setup().unwrap();

    let schema = Arc::new(
        StructType::try_new(vec![StructField::new("id", DataType::INTEGER, false)]).unwrap(),
    );

    // Try to create table with invalid column mapping mode
    let result = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "invalid")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid column mapping mode"));
}

/// Test cases for clustering columns with column mapping enabled.
/// Each case specifies: (logical_column_names, description)
#[rstest::rstest]
#[case::single_column(&["id"], "single clustering column")]
#[case::multiple_columns(&["id", "value"], "multiple clustering columns")]
#[test]
fn test_create_clustered_table_with_column_mapping(
    #[case] clustering_cols: &[&str],
    #[case] description: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create clustered table with column mapping enabled
    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .with_data_layout(DataLayout::clustered(clustering_cols.iter().copied()))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Load snapshot (validates column mapping annotations on read)
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Verify column mapping configuration
    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);

    // Verify clustering-specific features
    let table_config = snapshot.table_configuration();
    assert!(table_config.is_feature_supported(&TableFeature::ClusteredTable));
    assert!(table_config.is_feature_supported(&TableFeature::DomainMetadata));

    // Verify clustering domain metadata exists and uses physical column names
    let clustering_columns = snapshot.get_clustering_columns(engine.as_ref())?;
    let columns = clustering_columns.expect("Clustering columns should be present");
    assert_eq!(
        columns.len(),
        clustering_cols.len(),
        "{}: expected {} clustering columns, got {}",
        description,
        clustering_cols.len(),
        columns.len()
    );

    // With column mapping enabled, clustering domain metadata stores physical names
    for (i, col) in columns.iter().enumerate() {
        let physical_name: &str = col.path()[0].as_ref();
        let logical_name = clustering_cols[i];
        assert!(
            physical_name.starts_with("col-"),
            "{}: clustering column {} should use physical name '{}', not logical name '{}'",
            description,
            i,
            physical_name,
            logical_name
        );
    }

    Ok(())
}

#[test]
fn test_column_mapping_nested_schema() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Create nested schema
    let address_type = StructType::try_new(vec![
        StructField::new("street", DataType::STRING, true),
        StructField::new("city", DataType::STRING, true),
    ])?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("address", DataType::Struct(Box::new(address_type)), true),
    ])?);

    // Create table and load snapshot (validates column mapping for nested schema on read)
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    // Verify column mapping config (maxColumnId = 4: id, address, street, city)
    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);

    // Verify schema preserves the full nested structure
    let read_schema = snapshot.schema();
    assert_eq!(read_schema.fields().count(), 2);

    // Verify top-level fields
    let id_field = read_schema.field("id").expect("id field should exist");
    assert_eq!(id_field.data_type(), &DataType::INTEGER);
    assert!(!id_field.is_nullable());

    let address_field = read_schema
        .field("address")
        .expect("address field should exist");
    assert!(address_field.is_nullable());

    // Verify nested struct fields are preserved
    match address_field.data_type() {
        DataType::Struct(nested) => {
            assert_eq!(nested.fields().count(), 2);

            let street = nested.field("street").expect("street field should exist");
            assert_eq!(street.data_type(), &DataType::STRING);
            assert!(street.is_nullable());

            let city = nested.field("city").expect("city field should exist");
            assert_eq!(city.data_type(), &DataType::STRING);
            assert!(city.is_nullable());
        }
        other => panic!("Expected Struct type for address, got {:?}", other),
    }

    Ok(())
}

/// E2E test: create a table with column mapping on a schema containing map and array types,
/// then read it back via snapshot and verify column mapping metadata survives the roundtrip.
#[test]
fn test_column_mapping_schema_with_maps_and_arrays() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Schema:
    //   id: int (not null)
    //   tags: map<string, string>
    //   scores: array<int>
    //   metadata: struct<
    //     labels: map<string, array<int>>
    //   >
    let labels_type = MapType::new(
        DataType::STRING,
        ArrayType::new(DataType::INTEGER, true),
        true,
    );

    let metadata_type = StructType::try_new(vec![StructField::new(
        "labels",
        DataType::from(labels_type),
        true,
    )])?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new(
            "tags",
            DataType::from(MapType::new(DataType::STRING, DataType::STRING, true)),
            true,
        ),
        StructField::new(
            "scores",
            DataType::from(ArrayType::new(DataType::INTEGER, true)),
            true,
        ),
        StructField::new("metadata", DataType::Struct(Box::new(metadata_type)), true),
    ])?);

    // Create table with column mapping and read back the snapshot.
    // The snapshot read exercises validate_schema_column_mapping, which verifies
    // that all fields (including map key/value, array element, and nested structs)
    // have valid column mapping metadata.
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema.clone(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    // First verify column mapping annotations (IDs, physical names, maxColumnId, feature flags)
    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);

    // Then strip column mapping metadata and verify the schema structure matches the input.
    let read_schema = strip_column_mapping_metadata(&snapshot.schema());
    assert_eq!(&read_schema, schema.as_ref(), "Schema roundtrip mismatch");

    Ok(())
}

/// Builds a schema that supports clustering at depths 1, 2, and 5:
///   { id: int, name: string, address: { city: string, zip: string },
///     l1: { l2: { l3: { l4: { value: double } } } } }
fn clustering_cm_test_schema() -> DeltaResult<Arc<StructType>> {
    let address = StructType::try_new(vec![
        StructField::new("city", DataType::STRING, true),
        StructField::new("zip", DataType::STRING, true),
    ])?;
    let l4 = StructType::try_new(vec![StructField::new("value", DataType::DOUBLE, true)])?;
    let l3 = StructType::try_new(vec![StructField::new(
        "l4",
        DataType::Struct(Box::new(l4)),
        true,
    )])?;
    let l2 = StructType::try_new(vec![StructField::new(
        "l3",
        DataType::Struct(Box::new(l3)),
        true,
    )])?;
    let l1 = StructType::try_new(vec![StructField::new(
        "l2",
        DataType::Struct(Box::new(l2)),
        true,
    )])?;
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
        StructField::new("address", DataType::Struct(Box::new(address)), true),
        StructField::new("l1", DataType::Struct(Box::new(l1)), true),
    ])?))
}

#[rstest::rstest]
#[case::top_level_cm_none(vec![vec!["id"]], "none")]
#[case::top_level_cm_name(vec![vec!["id"]], "name")]
#[case::top_level_cm_id(vec![vec!["id"]], "id")]
#[case::nested_2_cm_none(vec![vec!["address", "city"]], "none")]
#[case::nested_2_cm_name(vec![vec!["address", "city"]], "name")]
#[case::nested_2_cm_id(vec![vec!["address", "city"]], "id")]
#[case::mixed_cm_none(vec![vec!["id"], vec!["name"], vec!["address", "city"], vec!["address", "zip"], vec!["l1", "l2", "l3", "l4", "value"]], "none")]
#[case::mixed_cm_name(vec![vec!["id"], vec!["name"], vec!["address", "city"], vec!["address", "zip"], vec!["l1", "l2", "l3", "l4", "value"]], "name")]
#[case::mixed_cm_id(vec![vec!["id"], vec!["name"], vec!["address", "city"], vec!["address", "zip"], vec!["l1", "l2", "l3", "l4", "value"]], "id")]
#[test]
fn test_create_clustered_table_nested_with_column_mapping(
    #[case] col_paths: Vec<Vec<&str>>,
    #[case] cm_mode: &str,
) -> DeltaResult<()> {
    use delta_kernel::expressions::ColumnName;

    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = clustering_cm_test_schema()?;
    let expected_cols: Vec<ColumnName> = col_paths
        .iter()
        .map(|p| ColumnName::new(p.iter().copied()))
        .collect();

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", cm_mode)])
        .with_data_layout(DataLayout::Clustered {
            columns: expected_cols.clone(),
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let table_configuration = snapshot.table_configuration();
    assert!(
        table_configuration.is_feature_supported(&TableFeature::DomainMetadata),
        "Protocol should support domainMetadata feature"
    );
    assert!(
        table_configuration.is_feature_supported(&TableFeature::ClusteredTable),
        "Protocol should support clustering feature"
    );

    let expected_cm_mode = match cm_mode {
        "name" => ColumnMappingMode::Name,
        "id" => ColumnMappingMode::Id,
        _ => ColumnMappingMode::None,
    };
    assert_column_mapping_config(&snapshot, expected_cm_mode);

    let clustering_columns = snapshot.get_clustering_columns(engine.as_ref())?;
    let columns = clustering_columns.expect("Clustering columns should be present");
    assert_eq!(columns.len(), expected_cols.len());

    for (col, expected_path) in columns.iter().zip(col_paths.iter()) {
        assert_eq!(col.path().len(), expected_path.len());
        match expected_cm_mode {
            ColumnMappingMode::Name | ColumnMappingMode::Id => {
                for field_name in col.path() {
                    assert!(
                        field_name.starts_with("col-"),
                        "Clustering path field '{field_name}' should use physical name"
                    );
                }
            }
            ColumnMappingMode::None => {
                let expected_col = ColumnName::new(expected_path.iter().copied());
                assert_eq!(*col, expected_col);
            }
        }
    }

    Ok(())
}
