//! StateInfo handles the state that we use through log-replay in order to correctly construct all
//! the physical->logical transforms needed for each add file

use std::collections::HashSet;
use std::sync::Arc;

use tracing::debug;

use crate::scan::field_classifiers::TransformFieldClassifier;
use crate::scan::PhysicalPredicate;
use crate::schema::{DataType, MetadataColumnSpec, SchemaRef, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::ColumnMappingMode;
use crate::transforms::{FieldTransformSpec, TransformSpec};
use crate::{DeltaResult, Error, PredicateRef, StructField};

/// All the state needed to process a scan.
#[derive(Debug)]
pub(crate) struct StateInfo {
    /// The logical schema for this scan
    pub(crate) logical_schema: SchemaRef,
    /// The physical schema to read from parquet files
    pub(crate) physical_schema: SchemaRef,
    /// The physical predicate for data skipping
    pub(crate) physical_predicate: PhysicalPredicate,
    /// Transform specification for converting physical to logical data
    pub(crate) transform_spec: Option<Arc<TransformSpec>>,
}

/// Validating the metadata columns also extracts information needed to properly construct the full
/// `StateInfo`. We use this struct to group this information so it can be cleanly passed back from
/// `validate_metadata_columns`
#[derive(Default)]
struct MetadataInfo<'a> {
    /// What are the names of the requested metadata fields
    metadata_field_names: HashSet<&'a String>,
    /// The name of the column that's selecting row indexes if that's been requested or None if they
    /// are not requested. We remember this if it's been requested explicitly. this is so we can
    /// reference this column and not re-add it as a requested column if we're _also_ requesting
    /// row-ids.
    selected_row_index_col_name: Option<&'a String>,
    /// the materializedRowIdColumnName extracted from the table config if row ids are requested, or
    /// None if they are not requested
    materialized_row_id_column_name: Option<&'a String>,
}

/// This validates that we have sensible metadata columns, and that the requested metadata is
/// supported by the table. Also computes and returns any extra info needed to build the transform
/// for the requested columns.
// Runs in O(supported_number_of_metadata_columns) time since each metadata
// column can appear at most once in the schema
fn validate_metadata_columns<'a>(
    logical_schema: &'a SchemaRef,
    table_configuration: &'a TableConfiguration,
) -> DeltaResult<MetadataInfo<'a>> {
    let mut metadata_info = MetadataInfo::default();
    let partition_columns = table_configuration.metadata().partition_columns();
    for metadata_column in logical_schema.metadata_columns() {
        // Ensure we don't have a metadata column with same name as a partition column
        if partition_columns.contains(metadata_column.name()) {
            return Err(Error::Schema(format!(
                "Metadata column names must not match partition columns: {}",
                metadata_column.name()
            )));
        }
        match metadata_column.get_metadata_column_spec() {
            Some(MetadataColumnSpec::RowIndex) => {
                metadata_info.selected_row_index_col_name = Some(metadata_column.name());
            }
            Some(MetadataColumnSpec::RowId) => {
                if table_configuration.table_properties().enable_row_tracking != Some(true) {
                    return Err(Error::unsupported("Row ids are not enabled on this table"));
                }
                let row_id_col = table_configuration
                    .metadata()
                    .configuration()
                    .get("delta.rowTracking.materializedRowIdColumnName")
                    .ok_or(Error::generic("No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration"))?;
                metadata_info.materialized_row_id_column_name = Some(row_id_col);
            }
            Some(MetadataColumnSpec::RowCommitVersion) => {}
            None => {}
        }
        metadata_info
            .metadata_field_names
            .insert(metadata_column.name());
    }
    Ok(metadata_info)
}

impl StateInfo {
    /// Create StateInfo with a custom field classifier for different scan types.
    /// Get the state needed to process a scan.
    ///
    /// `logical_schema` - The logical schema of the scan output, which includes partition columns
    /// `table_configuration` - The TableConfiguration for this table
    /// `predicate` - Optional predicate to filter data during the scan
    /// `classifier` - The classifier to use for different scan types. Use `()` if not needed
    pub(crate) fn try_new<C: TransformFieldClassifier>(
        logical_schema: SchemaRef,
        table_configuration: &TableConfiguration,
        predicate: Option<PredicateRef>,
        classifier: C,
    ) -> DeltaResult<Self> {
        let partition_columns = table_configuration.metadata().partition_columns();
        let column_mapping_mode = table_configuration.column_mapping_mode();
        let mut read_fields = Vec::with_capacity(logical_schema.num_fields());
        let mut transform_spec = Vec::new();
        let mut last_physical_field: Option<String> = None;

        let metadata_info = validate_metadata_columns(&logical_schema, table_configuration)?;

        // Loop over all selected fields and build both the physical schema and transform spec
        for (index, logical_field) in logical_schema.fields().enumerate() {
            if let Some(spec) =
                classifier.classify_field(logical_field, index, &last_physical_field)
            {
                // Classifier has handled this field via a transformation, just push it and move on
                transform_spec.push(spec);
            } else if partition_columns.contains(logical_field.name()) {
                // push the transform for this partition column
                transform_spec.push(FieldTransformSpec::MetadataDerivedColumn {
                    field_index: index,
                    insert_after: last_physical_field.clone(),
                });
            } else {
                // Regular field field or a metadata column, figure out which and handle it
                match logical_field.get_metadata_column_spec() {
                    Some(MetadataColumnSpec::RowId) => {
                        let index_column_name = match metadata_info.selected_row_index_col_name {
                            Some(index_column_name) => index_column_name.to_string(),
                            None => {
                                // the index column isn't being explicitly requested, so add it to
                                // `read_fields` so the parquet_reader will generate it, and add a
                                // transform to drop it before returning logical data

                                // ensure we have a column name that isn't already in our schema
                                let index_column_name = (0..)
                                    .map(|i| format!("row_indexes_for_row_id_{}", i))
                                    .find(|name| logical_schema.field(name).is_none())
                                    .ok_or(Error::generic(
                                        "Couldn't generate row index column name",
                                    ))?;
                                read_fields.push(StructField::create_metadata_column(
                                    &index_column_name,
                                    MetadataColumnSpec::RowIndex,
                                ));
                                transform_spec.push(FieldTransformSpec::StaticDrop {
                                    field_name: index_column_name.clone(),
                                });
                                index_column_name
                            }
                        };
                        let Some(row_id_col_name) = metadata_info.materialized_row_id_column_name
                        else {
                            return Err(Error::internal_error(
                                "Should always return a materialized_row_id_column_name if selecting row ids"
                            ));
                        };

                        read_fields.push(StructField::nullable(row_id_col_name, DataType::LONG));
                        transform_spec.push(FieldTransformSpec::GenerateRowId {
                            field_name: row_id_col_name.to_string(),
                            row_index_field_name: index_column_name,
                        });
                    }
                    Some(MetadataColumnSpec::RowCommitVersion) => {
                        return Err(Error::unsupported("Row commit versions not supported"));
                    }
                    Some(MetadataColumnSpec::RowIndex) | None => {
                        // note that RowIndex is handled in the parquet reader so we just add it as
                        // if it's a normal physical column
                        let physical_field = logical_field.make_physical(column_mapping_mode);
                        debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                        let physical_name = physical_field.name.clone();

                        if !logical_field.is_metadata_column()
                            && metadata_info.metadata_field_names.contains(&physical_name)
                        {
                            return Err(Error::Schema(format!(
                                "Metadata column names must not match physical columns, but logical column '{}' has physical name '{}'",
                                logical_field.name(), physical_name,
                            )));
                        }
                        last_physical_field = Some(physical_name);
                        read_fields.push(physical_field);
                    }
                }
            }
        }

        let physical_schema = Arc::new(StructType::try_new(read_fields)?);

        let physical_predicate = match predicate {
            Some(pred) => PhysicalPredicate::try_new(&pred, &logical_schema)?,
            None => PhysicalPredicate::None,
        };

        let transform_spec =
            if !transform_spec.is_empty() || column_mapping_mode != ColumnMappingMode::None {
                Some(Arc::new(transform_spec))
            } else {
                None
            };

        Ok(StateInfo {
            logical_schema,
            physical_schema,
            physical_predicate,
            transform_spec,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{collections::HashMap, sync::Arc};

    use url::Url;

    use crate::actions::{Metadata, Protocol};
    use crate::expressions::{column_expr, Expression as Expr};
    use crate::schema::{ColumnMetadataKey, MetadataValue};
    use crate::utils::test_utils::assert_result_error_with_message;

    use super::*;

    // get a state info with no predicate or extra metadata
    pub(crate) fn get_simple_state_info(
        schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> DeltaResult<StateInfo> {
        get_state_info(schema, partition_columns, None, HashMap::new(), vec![])
    }

    pub(crate) fn get_state_info(
        schema: SchemaRef,
        partition_columns: Vec<String>,
        predicate: Option<PredicateRef>,
        metadata_configuration: HashMap<String, String>,
        metadata_cols: Vec<(&str, MetadataColumnSpec)>,
    ) -> DeltaResult<StateInfo> {
        let metadata = Metadata::try_new(
            None,
            None,
            schema.as_ref().clone(),
            partition_columns,
            10,
            metadata_configuration,
        )?;
        let no_features: Option<Vec<String>> = None; // needed for type annotation
        let protocol = Protocol::try_new(2, 2, no_features.clone(), no_features)?;
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

        StateInfo::try_new(schema.clone(), &table_configuration, predicate, ())
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
        assert_eq!(state_info.logical_schema, schema);
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
        assert_eq!(state_info.logical_schema, schema);
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
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
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
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
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
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
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

        for (metadata_config, metadata_cols, expected_error) in [
            (HashMap::new(), vec![("row_id", MetadataColumnSpec::RowId)], "Unsupported: Row ids are not enabled on this table"),
            (
                get_string_map(&[("delta.enableRowTracking", "true")]),
                vec![("row_id", MetadataColumnSpec::RowId)],
                "Generic delta kernel error: No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration",
            ),
        ] {
            let res = get_state_info(schema.clone(), vec![], None, metadata_config, metadata_cols);
            assert_result_error_with_message(res, expected_error);
        }
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
            get_string_map(&[("delta.columnMapping.mode", "name")]),
            vec![("other", MetadataColumnSpec::RowIndex)],
        );
        assert_result_error_with_message(
            res,
            "Schema error: Metadata column names must not match physical columns, but logical column 'id' has physical name 'other'"
        );
    }
}
