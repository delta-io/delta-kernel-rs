//! StateInfo handles the state that we use through log-replay in order to correctly construct all
//! the physical->logical transforms needed for each add file

use std::collections::HashSet;
use std::sync::Arc;

use crate::expressions::ColumnName;
use crate::scan::data_skipping::stats_schema::build_stats_schema;
use crate::scan::field_classifiers::TransformFieldClassifier;
use crate::scan::transform_spec::TransformSpec;
use crate::scan::PhysicalPredicate;
use crate::scan::StatsOutputMode;
use crate::schema::{LogicalSchema, LogicalSchemaRef, SchemaRef};
use crate::table_configuration::TableConfiguration;

use crate::{DeltaResult, PredicateRef};

/// All the state needed to process a scan.
#[derive(Debug, Clone)]
pub(crate) struct StateInfo {
    /// The logical schema, column mapping mode, and partition columns for this scan
    pub(crate) logical_schema: LogicalSchemaRef,
    /// The physical read schema computed from the logical schema
    pub(crate) physical_schema: SchemaRef,
    /// The physical predicate for data skipping
    pub(crate) physical_predicate: PhysicalPredicate,
    /// Transform specification for converting physical to logical data
    pub(crate) transform_spec: Option<Arc<TransformSpec>>,
    /// Physical stats schema for reading/parsing stats from checkpoint files.
    /// Used to construct checkpoint read schema with stats_parsed.
    pub(crate) physical_stats_schema: Option<SchemaRef>,
    /// Logical stats schema for the file statistics. When `stats_columns` is requested,
    /// the engine receives stats with physical column names (for column mapping). This
    /// logical schema maps those stats back to the table's logical column names.
    pub(crate) logical_stats_schema: Option<SchemaRef>,
}

impl StateInfo {
    /// Create StateInfo with a custom field classifier for different scan types.
    /// Get the state needed to process a scan.
    ///
    /// `logical_schema` - The logical schema of the scan output, which includes partition columns
    /// `table_configuration` - The TableConfiguration for this table
    /// `predicate` - Optional predicate to filter data during the scan
    /// `stats_output_mode` - Controls how file statistics are handled during the scan
    /// `classifier` - The classifier to use for different scan types. Use `()` if not needed
    pub(crate) fn try_new<C: TransformFieldClassifier>(
        logical_schema: LogicalSchema,
        table_configuration: &TableConfiguration,
        predicate: Option<PredicateRef>,
        stats_output_mode: StatsOutputMode,
        classifier: C,
    ) -> DeltaResult<Self> {
        let logical_schema = Arc::new(logical_schema);
        let (physical_schema, transform_spec) =
            logical_schema.compute_physical_read_schema_and_transform(&classifier)?;

        // Extract column names referenced by the predicate so we can include them
        // in the stats schema when stats_columns is requested. This ensures the
        // DataSkippingFilter has the stats it needs for data skipping.
        let predicate_column_names: Vec<ColumnName> = predicate
            .as_ref()
            .map(|p| p.references().into_iter().cloned().collect())
            .unwrap_or_default();

        let physical_predicate = match predicate {
            Some(pred) => PhysicalPredicate::try_new(&pred, &logical_schema)?,
            None => PhysicalPredicate::None,
        };

        // Build stats schemas based on StatsOutputMode:
        // - AllColumns: output all stats from expected_stats_schema
        // - Columns(non-empty): merge requested + predicate columns for data skipping
        // - Columns(empty): predicate-only internal data skipping (no stats output)
        // - Skip: no stats at all (handled at the Scan level, no schemas needed here)
        let (physical_stats_schema, logical_stats_schema) =
            match (&stats_output_mode, &physical_predicate) {
                // Output all table stats columns in stats_parsed. The DataSkippingFilter
                // reads stats_parsed from the transformed batch, which uses this schema.
                (StatsOutputMode::AllColumns, _) => {
                    let expected_stats_schemas =
                        table_configuration.build_expected_stats_schemas(None, None)?;
                    (
                        Some(expected_stats_schemas.physical),
                        Some(expected_stats_schemas.logical),
                    )
                }
                // Non-empty requested columns -- include predicate-referenced columns
                // alongside the user-requested stats columns so that the DataSkippingFilter
                // has the stats it needs.
                (StatsOutputMode::Columns(requested_columns), _)
                    if !requested_columns.is_empty() =>
                {
                    let existing: HashSet<&ColumnName> = requested_columns.iter().collect();
                    let mut all_needed_stats_columns = requested_columns.clone();
                    for col in &predicate_column_names {
                        if !existing.contains(col) {
                            all_needed_stats_columns.push(col.clone());
                        }
                    }
                    // Translate logical names to physical for build_expected_stats_schemas,
                    // which works on the physical data schema.
                    let physical_columns: Vec<ColumnName> = all_needed_stats_columns
                        .iter()
                        .filter_map(|col| logical_schema.get_physical_column_name(col).ok())
                        .collect();
                    let expected_stats_schemas = table_configuration
                        .build_expected_stats_schemas(None, Some(&physical_columns))?;
                    (
                        Some(expected_stats_schemas.physical),
                        Some(expected_stats_schemas.logical),
                    )
                }
                // Columns(empty) or Skip with a physical predicate -- build stats directly
                // from the physical predicate's referenced schema for internal data skipping
                // only (no logical schema needed for output).
                (_, PhysicalPredicate::Some(_, ref_schema)) => {
                    (build_stats_schema(ref_schema), None)
                }
                // No stats output and no predicate
                (_, _) => (None, None),
            };

        Ok(StateInfo {
            logical_schema,
            physical_schema,
            physical_predicate,
            transform_spec,
            physical_stats_schema,
            logical_stats_schema,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{collections::HashMap, sync::Arc};

    use url::Url;

    use crate::actions::{Metadata, Protocol};
    use crate::expressions::{column_expr, column_name, Expression as Expr};
    use crate::schema::{ColumnMetadataKey, MetadataValue};
    use crate::table_features::{FeatureType, TableFeature};
    use crate::utils::test_utils::assert_result_error_with_message;

    use super::*;

    // get a state info with no predicate or extra metadata
    pub(crate) fn get_simple_state_info(
        schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> DeltaResult<StateInfo> {
        get_state_info(schema, partition_columns, None, &[], HashMap::new(), vec![])
    }

    /// When features are non-empty, uses protocol (3,7) with explicit feature lists.
    /// When features are empty, uses legacy protocol (2,5).
    pub(crate) fn get_state_info(
        schema: SchemaRef,
        partition_columns: Vec<String>,
        predicate: Option<PredicateRef>,
        features: &[TableFeature],
        metadata_configuration: HashMap<String, String>,
        metadata_cols: Vec<(&str, MetadataColumnSpec)>,
    ) -> DeltaResult<StateInfo> {
        get_state_info_with_stats(
            schema,
            partition_columns,
            predicate,
            features,
            metadata_configuration,
            metadata_cols,
            StatsOutputMode::default(),
        )
    }

    pub(crate) fn get_state_info_with_stats(
        schema: SchemaRef,
        partition_columns: Vec<String>,
        predicate: Option<PredicateRef>,
        features: &[TableFeature],
        metadata_configuration: HashMap<String, String>,
        metadata_cols: Vec<(&str, MetadataColumnSpec)>,
        stats_output_mode: StatsOutputMode,
    ) -> DeltaResult<StateInfo> {
        let metadata = Metadata::try_new(
            None,
            None,
            schema.clone(),
            partition_columns,
            10,
            metadata_configuration,
        )?;
        let protocol = if features.is_empty() {
            Protocol::try_new_legacy(2, 5)?
        } else {
            // This helper only handles known features. Unknown features would need
            // explicit placement on reader vs writer lists.
            assert!(
                features
                    .iter()
                    .all(|f| f.feature_type() != FeatureType::Unknown),
                "Test helper does not support unknown features"
            );
            let reader_features = features
                .iter()
                .filter(|f| f.feature_type() == FeatureType::ReaderWriter);
            Protocol::try_new_modern(reader_features, features)?
        };
        let table_configuration = TableConfiguration::try_new(
            metadata,
            protocol,
            Url::parse("s3://my-table").unwrap(),
            1,
        )?;

        let mut schema = schema;
        for (name, spec) in metadata_cols.into_iter() {
            schema = Arc::new(
                schema
                    .add_metadata_column(name, spec)
                    .expect("Couldn't add metadata col"),
            );
        }

        let logical_schema = LogicalSchema::new(schema, &table_configuration);
        StateInfo::try_new(
            logical_schema,
            &table_configuration,
            predicate,
            stats_output_mode,
            (),
        )
    }

    pub(crate) fn assert_transform_spec(
        transform_spec: &TransformSpec,
        requested_row_indexes: bool,
        expected_row_id_name: &str,
        expected_row_index_name: &str,
    ) {
        // if we requested row indexes, there's only one transform for the row id col, otherwise the
        // first transform drops the row index column, and the second one adds the row ids
        let expected_transform_count = if requested_row_indexes { 1 } else { 2 };
        let generate_offset = if requested_row_indexes { 0 } else { 1 };

        assert_eq!(transform_spec.len(), expected_transform_count);

        if !requested_row_indexes {
            // ensure we have a drop transform if we didn't request row indexes
            match &transform_spec[0] {
                FieldTransformSpec::StaticDrop { field_name } => {
                    assert_eq!(field_name, expected_row_index_name);
                }
                _ => panic!("Expected StaticDrop transform"),
            }
        }

        match &transform_spec[generate_offset] {
            FieldTransformSpec::GenerateRowId {
                field_name,
                row_index_field_name,
            } => {
                assert_eq!(field_name, expected_row_id_name);
                assert_eq!(row_index_field_name, expected_row_index_name);
            }
            _ => panic!("Expected GenerateRowId transform"),
        }
    }

    use crate::scan::transform_spec::FieldTransformSpec;
    use crate::schema::{DataType, MetadataColumnSpec, StructType};

    #[test]
    fn no_partition_columns() {
        // Test case: No partition columns, no column mapping
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_simple_state_info(schema.clone(), vec![]).unwrap();

        // Should have no transform spec (no partitions, no column mapping)
        assert!(state_info.transform_spec.is_none());

        // Physical schema should match logical schema
        assert_eq!(state_info.logical_schema.raw_schema(), &schema);
        assert_eq!(state_info.physical_schema.fields().len(), 2);

        // No predicate
        assert_eq!(state_info.physical_predicate, PhysicalPredicate::None);
    }

    #[test]
    fn with_partition_columns() {
        // Test case: With partition columns
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("date", DataType::DATE), // Partition column
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_simple_state_info(
            schema.clone(),
            vec!["date".to_string()], // date is a partition column
        )
        .unwrap();

        // Should have a transform spec for the partition column
        assert!(state_info.transform_spec.is_some());
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 1);

        // Check the transform spec for the partition column
        match &transform_spec[0] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 1); // Index of "date" in logical schema
                assert_eq!(insert_after, &Some("id".to_string())); // After "id" which is physical
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }

        // Physical schema should not include partition column
        assert_eq!(state_info.logical_schema.raw_schema(), &schema);
        assert_eq!(state_info.physical_schema.fields().len(), 2); // Only id and value
    }

    #[test]
    fn multiple_partition_columns() {
        // Test case: Multiple partition columns interspersed with regular columns
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("col1", DataType::STRING),
            StructField::nullable("part1", DataType::STRING), // Partition
            StructField::nullable("col2", DataType::LONG),
            StructField::nullable("part2", DataType::INTEGER), // Partition
        ]));

        let state_info = get_simple_state_info(
            schema.clone(),
            vec!["part1".to_string(), "part2".to_string()],
        )
        .unwrap();

        // Should have transforms for both partition columns
        assert!(state_info.transform_spec.is_some());
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 2);

        // Check first partition column transform
        match &transform_spec[0] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 1); // Index of "part1"
                assert_eq!(insert_after, &Some("col1".to_string()));
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }

        // Check second partition column transform
        match &transform_spec[1] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 3); // Index of "part2"
                assert_eq!(insert_after, &Some("col2".to_string()));
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }

        // Physical schema should only have non-partition columns
        assert_eq!(state_info.physical_schema.fields().len(), 2); // col1 and col2
    }

    #[test]
    fn with_predicate() {
        // Test case: With a valid predicate
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let predicate = Arc::new(column_expr!("value").gt(Expr::literal(10i64)));

        let state_info = get_state_info(
            schema.clone(),
            vec![], // no partition columns
            Some(predicate),
            &[],            // no table features
            HashMap::new(), // no extra metadata
            vec![],         // no metadata
        )
        .unwrap();

        // Should have a physical predicate
        match &state_info.physical_predicate {
            PhysicalPredicate::Some(_pred, schema) => {
                // Physical predicate exists
                assert_eq!(schema.fields().len(), 1); // Only "value" is referenced
            }
            _ => panic!("Expected PhysicalPredicate::Some"),
        }
    }

    #[test]
    fn partition_at_beginning() {
        // Test case: Partition column at the beginning
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("date", DataType::DATE), // Partition column
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_simple_state_info(schema.clone(), vec!["date".to_string()]).unwrap();

        // Should have a transform spec for the partition column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 1);

        match &transform_spec[0] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 0); // Index of "date"
                assert_eq!(insert_after, &None); // No physical field before it, so prepend
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }
    }

    pub(crate) const ROW_TRACKING_FEATURES: &[TableFeature] =
        &[TableFeature::RowTracking, TableFeature::DomainMetadata];

    fn get_string_map(slice: &[(&str, &str)]) -> HashMap<String, String> {
        slice
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn request_row_ids() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "some_row_commit_version_col",
                ),
            ]),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            false, // we did not request row indexes
            "some_row_id_col",
            "row_indexes_for_row_id_0",
        );
    }

    #[test]
    fn request_row_ids_conflicting_row_index_col_name() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "row_indexes_for_row_id_0", // this will conflict with the first generated name for row indexes
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "some_row_commit_version_col",
                ),
            ]),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            false, // we did not request row indexes
            "some_row_id_col",
            "row_indexes_for_row_id_1", // ensure we didn't conflict with the col in the schema
        );
    }

    #[test]
    fn request_row_ids_and_indexes() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "some_row_commit_version_col",
                ),
            ]),
            vec![
                ("row_id", MetadataColumnSpec::RowId),
                ("row_index", MetadataColumnSpec::RowIndex),
            ],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            true, // we did request row indexes
            "some_row_id_col",
            "row_index",
        );
    }

    #[test]
    fn invalid_rowtracking_config() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        // Row IDs requested but row tracking not enabled → error
        let res = get_state_info(
            schema.clone(),
            vec![],
            None,
            &[], // no table features
            HashMap::new(),
            vec![("row_id", MetadataColumnSpec::RowId)],
        );
        assert_result_error_with_message(
            res,
            "Unsupported: Row IDs require row tracking to be enabled with a configured materialized column name",
        );

        // Row tracking enabled but missing materializedRowIdColumnName → error
        let res = get_state_info(
            schema,
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[("delta.enableRowTracking", "true")]),
            vec![("row_id", MetadataColumnSpec::RowId)],
        );
        assert_result_error_with_message(
            res,
            "Unsupported: Row IDs require row tracking to be enabled with a configured materialized column name",
        );
    }

    #[test]
    fn metadata_column_matches_partition_column() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));
        let res = get_state_info(
            schema.clone(),
            vec!["part_col".to_string()],
            None,
            &[], // no table features
            HashMap::new(),
            vec![("part_col", MetadataColumnSpec::RowId)],
        );
        assert_result_error_with_message(
            res,
            "Schema error: Metadata column names must not match partition columns: part_col",
        );
    }

    #[test]
    fn metadata_column_matches_read_field() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )
        .with_metadata(HashMap::<String, MetadataValue>::from([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                1.into(),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                "other".into(),
            ),
        ]))]));
        let res = get_state_info(
            schema.clone(),
            vec![],
            None,
            &[], // no table features
            get_string_map(&[("delta.columnMapping.mode", "name")]),
            vec![("other", MetadataColumnSpec::RowIndex)],
        );
        assert_result_error_with_message(
            res,
            "Schema error: Metadata column names must not match physical columns, but logical column 'id' has physical name 'other'"
        );
    }

    #[test]
    fn stats_columns_with_predicate() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let predicate = Arc::new(column_expr!("value").gt(Expr::literal(10i64)));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            Some(predicate),
            &[], // no table features
            HashMap::new(),
            vec![],
            StatsOutputMode::AllColumns,
        )
        .unwrap();

        // physical_stats_schema should be set (from expected_stats_schema)
        assert!(
            state_info.physical_stats_schema.is_some(),
            "physical_stats_schema should be Some when AllColumns is set"
        );
        // physical_predicate should still be active for data skipping
        assert!(
            matches!(state_info.physical_predicate, PhysicalPredicate::Some(..)),
            "physical_predicate should be PhysicalPredicate::Some for data skipping"
        );
    }

    #[test]
    fn stats_columns_with_predicate_merges_columns() {
        // When specific stats_columns are requested alongside a predicate, the stats
        // schema should include both the requested columns and predicate-referenced columns.
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
            StructField::nullable("extra", DataType::LONG),
        ]));

        let predicate = Arc::new(column_expr!("extra").gt(Expr::literal(5i64)));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            Some(predicate),
            &[],
            HashMap::new(),
            vec![],
            StatsOutputMode::Columns(vec![column_name!("value")]),
        )
        .unwrap();

        let stats_schema = state_info
            .physical_stats_schema
            .expect("should have physical stats schema");

        let min_values = stats_schema
            .field("minValues")
            .expect("should have minValues");
        if let DataType::Struct(inner) = min_values.data_type() {
            assert!(
                inner.field("value").is_some(),
                "minValues should have 'value' (requested)"
            );
            assert!(
                inner.field("extra").is_some(),
                "minValues should have 'extra' (from predicate)"
            );
            assert!(
                inner.field("id").is_none(),
                "minValues should not have 'id' (neither requested nor in predicate)"
            );
        } else {
            panic!("minValues should be a struct");
        }
    }

    #[test]
    fn non_empty_stats_columns_filters_schema() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            None,
            &[], // no table features
            HashMap::new(),
            vec![],
            StatsOutputMode::Columns(vec![column_name!("value")]),
        )
        .unwrap();

        let stats_schema = state_info
            .physical_stats_schema
            .expect("should have physical stats schema");

        // Check that minValues/maxValues only contain 'value', not 'id'
        let min_values = stats_schema
            .field("minValues")
            .expect("should have minValues");
        if let DataType::Struct(inner) = min_values.data_type() {
            assert!(
                inner.field("value").is_some(),
                "minValues should have 'value'"
            );
            assert!(
                inner.field("id").is_none(),
                "minValues should not have 'id'"
            );
        } else {
            panic!("minValues should be a struct");
        }
    }

    #[test]
    fn stats_columns_with_column_mapping_uses_physical_names() {
        let field_a: StructField = serde_json::from_value(serde_json::json!({
            "name": "col_a",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 1,
                "delta.columnMapping.physicalName": "phys_a"
            }
        }))
        .unwrap();

        let field_b: StructField = serde_json::from_value(serde_json::json!({
            "name": "col_b",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 2,
                "delta.columnMapping.physicalName": "phys_b"
            }
        }))
        .unwrap();

        let field_c: StructField = serde_json::from_value(serde_json::json!({
            "name": "col_c",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 3,
                "delta.columnMapping.physicalName": "phys_c"
            }
        }))
        .unwrap();

        let schema = Arc::new(StructType::new_unchecked(vec![field_a, field_b, field_c]));
        let mut props = HashMap::new();
        props.insert("delta.columnMapping.mode".to_string(), "name".to_string());

        // Request col_a via stats_columns (logical), and reference col_b via predicate (logical).
        // Both must be translated to physical names in the output stats schema.
        let predicate = Arc::new(column_expr!("col_b").gt(Expr::literal(5i64)));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            Some(predicate),
            &[],
            props,
            vec![],
            StatsOutputMode::Columns(vec![column_name!("col_a")]),
        )
        .unwrap();

        let stats_schema = state_info
            .physical_stats_schema
            .expect("should have physical stats schema");

        let present = ["phys_a", "phys_b"];
        let absent = ["col_a", "col_b", "phys_c"];
        for stats_field in ["minValues", "maxValues"] {
            let DataType::Struct(inner) = stats_schema
                .field(stats_field)
                .unwrap_or_else(|| panic!("should have {stats_field}"))
                .data_type()
            else {
                panic!("{stats_field} should be a struct");
            };
            for name in present {
                assert!(
                    inner.field(name).is_some(),
                    "{stats_field} expected '{name}'"
                );
            }
            for name in absent {
                assert!(
                    inner.field(name).is_none(),
                    "{stats_field} unexpected '{name}'"
                );
            }
        }
    }

    use crate::StructField;
}
