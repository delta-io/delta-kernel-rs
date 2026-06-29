//! Column Mapping integration tests for the CreateTable API.
//!
//! These tests use kernel's snapshot API to read back the table, which exercises
//! the full column mapping validation path (via TableConfiguration::try_new ->
//! validate_schema_column_mapping). This ensures the written schema is valid and
//! readable by kernel.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::{
    column_mapping_fixtures as fixtures, create_table_and_load_snapshot, test_table_setup,
};

use super::simple_schema;

/// Helper to strip column mapping metadata (IDs and physical names) from all StructFields
/// recursively.
pub(super) fn strip_column_mapping_metadata(schema: &StructType) -> StructType {
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
                DataType::from(StructType::new_unchecked(fields))
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
/// the maximum `delta.columnMapping.id` reachable in the schema. Callers can pre-populate
/// CM annotations on the input schema, in which case the persisted `maxColumnId` reflects
/// any preserved IDs (matching Spark's `assignColumnIdAndPhysicalName`).
///
/// For `None`: mode is `None`, no `maxColumnId`, and no column mapping metadata (IDs or
/// physical names) on any field. Note: whether `ColumnMapping` appears in the protocol
/// depends on whether the feature flag was explicitly set, so that check is left to the
/// caller.
pub(super) fn assert_column_mapping_config(snapshot: &Snapshot, expected_mode: ColumnMappingMode) {
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

            let expected_max_id = snapshot
                .schema()
                .fields()
                .flat_map(|f| f.collect_column_mapping_ids())
                .max()
                .expect("CM-enabled table must have at least one column mapping id");
            let max_id_str = expected_max_id.to_string();
            let config = table_config.metadata().configuration();
            assert_eq!(
                config
                    .get("delta.columnMapping.maxColumnId")
                    .map(|s| s.as_str()),
                Some(max_id_str.as_str()),
                "maxColumnId should equal the largest column mapping id in the schema ({expected_max_id})"
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
                    field.column_mapping_id().is_none(),
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
    assert!(id_field.is_nullable());

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
        true,
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
    assert!(id_field.is_nullable());

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
        StructType::try_new(vec![StructField::new("id", DataType::INTEGER, true)]).unwrap(),
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
    let clustering_columns = snapshot.get_physical_clustering_columns(engine.as_ref())?;
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
            "{description}: clustering column {i} should use physical name '{physical_name}', not logical name '{logical_name}'"
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
        StructField::new("id", DataType::INTEGER, true),
        StructField::new("address", address_type, true),
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
    assert!(id_field.is_nullable());

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
        other => panic!("Expected Struct type for address, got {other:?}"),
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
        StructField::new("id", DataType::INTEGER, true),
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
        StructField::new("metadata", metadata_type, true),
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
    let l3 = StructType::try_new(vec![StructField::new("l4", l4, true)])?;
    let l2 = StructType::try_new(vec![StructField::new("l3", l3, true)])?;
    let l1 = StructType::try_new(vec![StructField::new("l2", l2, true)])?;
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, true),
        StructField::new("name", DataType::STRING, true),
        StructField::new("address", address, true),
        StructField::new("l1", l1, true),
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

    let clustering_columns = snapshot.get_physical_clustering_columns(engine.as_ref())?;
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

#[rstest::rstest]
#[case::single_column(&["id"])]
#[case::multiple_columns(&["id", "date"])]
fn test_partitioned_table_stores_logical_column_names_with_column_mapping(
    #[case] partition_cols: &[&str],
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = super::partition_test_schema()?;

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .with_data_layout(DataLayout::partitioned(partition_cols.iter().copied()))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);

    let log_file_path = format!("{table_path}/_delta_log/00000000000000000000.json");
    let log_contents = std::fs::read_to_string(&log_file_path).expect("Failed to read log file");
    let actions: Vec<serde_json::Value> = log_contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON"))
        .collect();

    let metadata_action = actions
        .iter()
        .find(|a| a.get("metaData").is_some())
        .expect("Should have metaData action");
    let metadata = metadata_action.get("metaData").unwrap();
    let stored_partition_columns: Vec<String> = metadata["partitionColumns"]
        .as_array()
        .expect("partitionColumns should be an array")
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();

    assert_eq!(stored_partition_columns.len(), partition_cols.len());

    for (i, stored_name) in stored_partition_columns.iter().enumerate() {
        let logical_name = partition_cols[i];
        assert_eq!(
            stored_name, logical_name,
            "partition column {i} should be logical name '{logical_name}', got '{stored_name}'"
        );
    }

    let clustering = snapshot.get_physical_clustering_columns(engine.as_ref())?;
    assert!(
        clustering.is_none(),
        "Partitioned table should not have clustering columns"
    );

    Ok(())
}

/// Create table with pre-existing `delta.columnMapping.physicalName` annotations,
/// verify the physical name duplicate validation logic.
///
/// - **accept**: same leaf physical name under two different parent structs.
/// - **reject**: two siblings inside `struct -> map -> array -> struct` share same physical name.
#[rstest::rstest]
#[case::accept(fixtures::same_leaf_phy_name_under_different_parents(), None)]
#[case::reject(
    fixtures::nested_field_with_same_phy_path(),
    Some("Duplicate `delta.columnMapping.physicalName`")
)]
fn test_create_table_dup_physical_name(
    #[case] schema: StructType,
    #[case] expected_error_substring: Option<&str>,
    #[values("name", "id")] cm_mode: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let result = create_table(&table_path, Arc::new(schema), "Test/1.0")
        .with_table_properties([("delta.columnMapping.mode", cm_mode)])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    match expected_error_substring {
        None => {
            let _commit = result?.commit(engine.as_ref())?;
        }
        Some(substr) => {
            let msg = result
                .expect_err("dup physicalName must be rejected at create-table build")
                .to_string();
            assert!(
                msg.contains(substr) && msg.contains(".a'") && msg.contains(".b'"),
                "expected path-aware dedup error naming colliding leaves under {cm_mode}, got: {msg}"
            );
        }
    }
    Ok(())
}

// ============================================================================
// CREATE TABLE accepts pre-populated column mapping metadata (Spark parity).
// See https://github.com/delta-io/delta-kernel-rs/issues/2377.
// ============================================================================

/// Connector-supplied CM metadata survives create -> reload: a complete annotation is kept
/// verbatim, a lone `id` keeps its value and gets a generated `col-<uuid>` physical name, and
/// a lone `physicalName` is kept while an id is allocated. `assert_column_mapping_config` also
/// pins `maxColumnId` to the largest reachable id.
#[rstest::rstest]
#[case::complete(
    fixtures::cm_field("f", 5, "phys-f", DataType::INTEGER),
    Some(5),
    Some("phys-f")
)]
#[case::id_only(fixtures::cm_field_id_only("f", 7, DataType::INTEGER), Some(7), None)]
#[case::physical_name_only(
    fixtures::cm_field_physical_name_only("f", "user-supplied", DataType::INTEGER),
    None,
    Some("user-supplied")
)]
fn test_create_table_preserves_or_fills_cm_metadata(
    #[case] field: StructField,
    #[case] expected_id: Option<i64>,
    #[case] expected_physical: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![field])?);

    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);
    let read_schema = snapshot.schema();
    let f = read_schema.field("f").unwrap();

    match expected_id {
        Some(id) => assert_eq!(f.column_mapping_id(), Some(id)),
        // No id supplied: kernel allocates one.
        None => assert!(f.column_mapping_id().expect("id must be allocated") > 0),
    }
    match expected_physical {
        Some(phys) => assert_eq!(
            f.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String(phys.to_string()))
        ),
        // No physical name supplied: kernel fills `col-<uuid>`.
        None => match f.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
            Some(MetadataValue::String(s)) => assert!(s.starts_with("col-")),
            other => panic!("expected col-<uuid>, got {other:?}"),
        },
    }

    Ok(())
}

/// Sparse preserved ids (1, 5, 100) seed assignment: bare fields get ids strictly above the
/// preserved max, in field order, and `maxColumnId` (checked by `assert_column_mapping_config`)
/// lands on the largest assigned id.
#[test]
fn test_create_table_sparse_preserved_ids_seed_assignment() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("bare1", DataType::STRING, true),
        fixtures::cm_field("a", 1, "phys-a", DataType::INTEGER),
        StructField::new("bare2", DataType::STRING, true),
        fixtures::cm_field("b", 100, "phys-b", DataType::INTEGER),
        fixtures::cm_field("c", 5, "phys-c", DataType::INTEGER),
        StructField::new("bare3", DataType::STRING, true),
    ])?);

    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);
    let read_schema = snapshot.schema();

    // Preserved ids untouched.
    assert_eq!(read_schema.field("a").unwrap().column_mapping_id(), Some(1));
    assert_eq!(
        read_schema.field("b").unwrap().column_mapping_id(),
        Some(100)
    );
    assert_eq!(read_schema.field("c").unwrap().column_mapping_id(), Some(5));

    // Bare fields seed above the preserved max (100), in field order.
    for (name, expected_id) in [("bare1", 101), ("bare2", 102), ("bare3", 103)] {
        assert_eq!(
            read_schema.field(name).unwrap().column_mapping_id(),
            Some(expected_id),
            "bare field {name} should get id {expected_id}"
        );
    }

    Ok(())
}

/// Preserved CM metadata under a nested struct is kept verbatim; bare fields elsewhere are
/// assigned ids strictly above the nested preserved max, in field order.
#[test]
fn test_create_table_preserves_preexisting_metadata_in_nested_types() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Nested struct with one fully-annotated leaf preserved at id=42.
    let nested_struct = StructType::try_new(vec![fixtures::cm_field(
        "leaf",
        42,
        "phys-leaf",
        DataType::INTEGER,
    )])?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("bare", DataType::STRING, true),
        StructField::new("outer", DataType::Struct(Box::new(nested_struct)), true),
    ])?);

    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;

    assert_column_mapping_config(&snapshot, ColumnMappingMode::Name);
    let read_schema = snapshot.schema();

    // Preserved leaf kept verbatim.
    let outer = read_schema.field("outer").unwrap();
    let DataType::Struct(inner) = outer.data_type() else {
        panic!("outer must remain a struct");
    };
    let leaf = inner.field("leaf").unwrap();
    assert_eq!(leaf.column_mapping_id(), Some(42));
    assert_eq!(
        leaf.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
        Some(&MetadataValue::String("phys-leaf".to_string()))
    );

    // bare and outer seed above the nested preserved max (42), in field order.
    assert_eq!(
        read_schema.field("bare").unwrap().column_mapping_id(),
        Some(43)
    );
    assert_eq!(outer.column_mapping_id(), Some(44));

    Ok(())
}
