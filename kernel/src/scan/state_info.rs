//! StateInfo handles the state that we use through log-replay in order to correctly construct all
//! the physical->logical transforms needed for each add file

use std::collections::HashSet;
use std::sync::Arc;

use tracing::debug;

use crate::scan::field_classifiers::TransformFieldClassifier;
use crate::scan::PhysicalPredicate;
use crate::schema::{MetadataColumnSpec, SchemaRef, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::ColumnMappingMode;
use crate::transforms::TransformSpec;
use crate::{DeltaResult, Error, PredicateRef};

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

impl StateInfo {
    /// Create StateInfo with a custom field classifier for different scan types.
    /// Get the state needed to process a scan.
    ///
    /// `logical_schema` - The logical schema of the scan output, which includes partition columns
    /// `partition_columns` - List of column names that are partition columns in the table
    /// `column_mapping_mode` - The column mapping mode used by the table for physical to logical mapping
    /// `predicate` - Optional predicate to filter data during the scan
    /// `classifier` - The classifier to use for different scan types
    pub(crate) fn try_new<C: TransformFieldClassifier>(
        logical_schema: SchemaRef,
        table_configuration: &TableConfiguration,
        predicate: Option<PredicateRef>,
        classifier: C,
    ) -> DeltaResult<Self> {
        let partition_columns = table_configuration.metadata().partition_columns();
        let column_mapping_mode = table_configuration.column_mapping_mode();
        let mut read_fields = Vec::with_capacity(logical_schema.num_fields());
        let mut read_field_names = HashSet::with_capacity(logical_schema.num_fields());
        let mut transform_spec = Vec::new();
        let mut last_physical_field: Option<String> = None;

        // We remember the name of the column that's selecting row indexes, if it's been requested
        // explicitly. this is so we can reference this column and not re-add it as a requested
        // column if we're _also_ requesting row-ids
        let mut selected_row_index_col_name = None;

        // This iteration runs in O(supported_number_of_metadata_columns) time since each metadata
        // column can appear at most once in the schema
        for metadata_column in logical_schema.metadata_columns() {
            if read_field_names.contains(metadata_column.name()) {
                return Err(Error::Schema(format!(
                    "Metadata column names must not match physical columns: {}",
                    metadata_column.name()
                )));
            }
            if let Some(MetadataColumnSpec::RowIndex) = metadata_column.get_metadata_column_spec() {
                selected_row_index_col_name = Some(metadata_column.name().to_string());
            }
        }

        // Loop over all selected fields and build both the physical schema and transform spec
        for (index, logical_field) in logical_schema.fields().enumerate() {
            let requirements = classifier.classify_field(
                logical_field,
                index,
                table_configuration,
                &last_physical_field,
            )?;

            if let Some(requirements) = requirements {
                // Field needs transformation - not in physical schema
                for spec in requirements.transform_specs.into_iter() {
                    transform_spec.push(spec);
                }
            } else {
                // Physical field - should be read from parquet
                // Validate metadata column doesn't conflict with partition columns
                if logical_field.is_metadata_column()
                    && partition_columns.contains(logical_field.name())
                {
                    return Err(Error::Schema(format!(
                        "Metadata column names must not match partition columns: {}",
                        logical_field.name()
                    )));
                }

                // <<<<<<< HEAD
                //                 // Partition column: needs to be injected via transform
                //                 transform_spec.push(FieldTransformSpec::MetadataDerivedColumn {
                //                     field_index: index,
                //                     insert_after: last_physical_field.clone(),
                //                 });
                //             } else {
                //                 // Regular field field, or metadata columns: add to physical schema
                //                 let mut select_physical_field = || {
                //                     let physical_field = logical_field.make_physical(column_mapping_mode);
                //                     debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                //                     let physical_name = physical_field.name.clone();
                // ||||||| f431de0c
                //                 // Partition column: needs to be injected via transform
                //                 transform_spec.push(FieldTransformSpec::MetadataDerivedColumn {
                //                     field_index: index,
                //                     insert_after: last_physical_field.clone(),
                //                 });
                //             } else {
                //                 // Regular field: add to physical schema
                //                 let physical_field = logical_field.make_physical(column_mapping_mode);
                //                 debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                //                 let physical_name = physical_field.name.clone();
                // =======
                // Add to physical schema
                let physical_field = logical_field.make_physical(column_mapping_mode);
                debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                let physical_name = physical_field.name.clone();

                if !logical_field.is_metadata_column() {
                    read_field_names.insert(physical_name.clone());
                }
                last_physical_field = Some(physical_name);
                read_fields.push(physical_field);
            }
            //     match logical_field.get_metadata_column_spec() {
            //         Some(MetadataColumnSpec::RowId) => {
            //             if table_configuration.table_properties().enable_row_tracking == Some(true)
            //             {
            //                 let row_id_col = table_configuration
            //                     .metadata()
            //                     .configuration()
            //                     .get("delta.rowTracking.materializedRowIdColumnName")
            //                     .ok_or(Error::generic("No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration"))?;
            //                 // we can `take` as we should only have one RowId col
            //                 let index_column_name = match selected_row_index_col_name.take() {
            //                     Some(index_column_name) => index_column_name,
            //                     None => {
            //                         // ensure we have a column name that isn't already in our schema
            //                         let index_column_name = (0..)
            //                             .map(|i| format!("row_indexes_for_row_id_{}", i))
            //                             .find(|name| logical_schema.field(name).is_none())
            //                             .ok_or(Error::generic(
            //                                 "Couldn't generate row index column name",
            //                             ))?;
            //                         read_fields.push(StructField::create_metadata_column(
            //                             &index_column_name,
            //                             MetadataColumnSpec::RowIndex,
            //                         ));
            //                         transform_spec.push(FieldTransformSpec::StaticDrop {
            //                             field_name: index_column_name.clone(),
            //                         });
            //                         index_column_name
            //                     }
            //                 };

            //                 read_fields.push(StructField::nullable(row_id_col, DataType::LONG));
            //                 transform_spec.push(FieldTransformSpec::GenerateRowId {
            //                     field_name: row_id_col.to_string(),
            //                     row_index_field_name: index_column_name,
            //                 });
            //             } else {
            //                 return Err(Error::unsupported(
            //                     "Row ids are not enabled on this table",
            //                 ));
            //             }
            //         }
            //         Some(MetadataColumnSpec::RowIndex) => {
            //             // handled in parquet reader
            //             select_physical_field()
            //         }
            //         Some(MetadataColumnSpec::RowCommitVersion) => {
            //             return Err(Error::unsupported("Row commit versions not supported"));
            //         }
            //         _ => select_physical_field(),
            //     }
            // }
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
mod tests {

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
        let protocol = Protocol::try_new(1, 2, no_features.clone(), no_features)?;
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

        StateInfo::try_new(
            schema.clone(),
            &table_configuration,
            predicate,
            ScanTransformFieldClassifierieldClassifier,
        )
    }

    #[test]
    fn test_state_info_no_partition_columns() {
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
    fn test_state_info_with_partition_columns() {
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
    fn test_state_info_multiple_partition_columns() {
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
    fn test_state_info_with_predicate() {
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
    fn test_state_info_partition_at_beginning() {
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

    #[test]
    fn test_state_info_request_row_ids() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            [
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some-row-id_col",
                ),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 2); // one for rowid col, one to drop indexes

        match &transform_spec[0] {
            FieldTransformSpec::StaticDrop { field_name } => {
                assert_eq!(field_name, "row_indexes_for_row_id_0");
            }
            _ => panic!("Expected StaticDrop transform"),
        }

        match &transform_spec[1] {
            FieldTransformSpec::GenerateRowId {
                field_name,
                row_index_field_name,
            } => {
                assert_eq!(field_name, "some-row-id_col");
                assert_eq!(row_index_field_name, "row_indexes_for_row_id_0");
            }
            _ => panic!("Expected GenerateRowId transform"),
        }
    }

    #[test]
    fn test_state_info_request_row_ids_and_indexes() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            [
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some-row-id_col",
                ),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            vec![
                ("row_id", MetadataColumnSpec::RowId),
                ("row_index", MetadataColumnSpec::RowIndex),
            ],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 1); // just one because we don't want to drop indexes now

        match &transform_spec[0] {
            FieldTransformSpec::GenerateRowId {
                field_name,
                row_index_field_name,
            } => {
                assert_eq!(field_name, "some-row-id_col");
                assert_eq!(row_index_field_name, "row_index");
            }
            _ => panic!("Expected GenerateRowId transform"),
        }
    }

    #[test]
    fn test_state_info_invalid_rowtracking_config() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        for (metadata_config, metadata_cols, expected_error) in [
            (HashMap::new(), vec![("row_id", MetadataColumnSpec::RowId)], "Unsupported: Row ids are not enabled on this table"),
            (
                [("delta.enableRowTracking", "true")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                vec![("row_id", MetadataColumnSpec::RowId)],
                "Generic delta kernel error: No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration",
            ),
        ] {
            match get_state_info(schema.clone(), vec![], None, metadata_config, metadata_cols) {
                Ok(_) => {
                    panic!("Should not have succeeded generating state info with invalid config")
                }
                Err(e) => {
                    assert_eq!(
                        e.to_string(),
                        expected_error,
                    )
                }
            }
        }
    }
}
