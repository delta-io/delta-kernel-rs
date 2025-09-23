//! Transform-related types and utilities for Delta Kernel.
//!
//! This module contains the types and functions needed to handle transforms
//! during scan and table changes operations, including partition value processing
//! and expression generation.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use crate::expressions::{Expression, ExpressionRef, Scalar, Transform};
use crate::schema::{DataType, SchemaRef, StructType};
use crate::{DeltaResult, Error};

/// Categorizes columns in a scan based on their data source and processing requirements.
///
/// This enum determines how each column in the logical schema maps to the physical data
/// and what transformations are needed during query execution.
#[derive(PartialEq, Debug)]
pub(crate) enum ColumnType {
    /// A column that exists in the physical parquet files and is read directly.
    /// The string contains the physical column name (after any column mapping).
    Selected(String),

    /// A metadata-derived column (e.g., partition columns, CDF metadata columns).
    /// The usize is the index of this column in the logical schema.
    MetadataDerivedColumn(usize),

    /// A column whose source varies by context (physical vs. metadata-derived).
    /// Used for CDF's _change_type which exists physically in CDC files but
    /// must be computed for Add/Remove files.
    Dynamic {
        /// Index of this column in the logical schema
        logical_index: usize,
        /// Name to look for in the physical schema
        physical_name: String,
    },
}

/// A list of field transforms that describes a transform expression to be created at scan time.
pub(crate) type TransformSpec = Vec<FieldTransformSpec>;

/// Describes a single field transformation to apply when converting physical data to logical schema.
///
/// These transformations are "sparse" - they only specify what changes, while unchanged fields
/// pass through implicitly in their original order.
#[derive(Debug)]
pub(crate) enum FieldTransformSpec {
    /// Insert a static expression after the named input column.
    /// If `insert_after` is None, prepend before all fields.
    // NOTE: Row tracking and other features may use this for computed columns.
    #[allow(unused)]
    StaticInsert {
        insert_after: Option<String>,
        expr: ExpressionRef,
    },
    /// Replace the named input column with a new expression.
    // NOTE: Row tracking will use this to replace physical rowid columns with
    // COALESCE expressions for non-materialized row ids.
    #[allow(unused)]
    StaticReplace {
        field_name: String,
        expr: ExpressionRef,
    },
    /// Drops the named input column
    // NOTE: Row tracking will need to drop metadata columns that were used to compute rowids, since
    // they should not appear in the query's output.
    #[allow(unused)]
    StaticDrop { field_name: String },
    /// Insert a partition column after the named input column.
    /// The partition column is identified by its field index in the logical table schema.
    /// Its value varies from file to file and is obtained from file metadata.
    MetadataDerivedColumn {
        /// Index in the logical schema to get the column's data type
        field_index: usize,
        /// Insert after this physical column (None = prepend)
        insert_after: Option<String>,
    },
    /// Insert or reorder a dynamic column that may be physical or metadata-derived.
    /// Used for CDF's _change_type column which requires different handling per file type.
    DynamicColumn {
        /// Index in the logical schema
        field_index: usize,
        /// Name to check for in physical schema
        physical_name: String,
        /// Where to insert/reorder this column
        insert_after: Option<String>,
    },
}

/// Parse a single partition value from the raw string representation
pub(crate) fn parse_partition_value(
    field_idx: usize,
    logical_schema: &SchemaRef,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<(usize, (String, Scalar))> {
    let Some(field) = logical_schema.field_at_index(field_idx) else {
        return Err(Error::InternalError(format!(
            "out of bounds partition column field index {field_idx}"
        )));
    };
    let name = field.physical_name();
    let partition_value = parse_partition_value_raw(partition_values.get(name), field.data_type())?;
    Ok((field_idx, (name.to_string(), partition_value)))
}

/// Parse all partition values from a transform spec.
pub(crate) fn parse_partition_values(
    logical_schema: &SchemaRef,
    transform_spec: &TransformSpec,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<HashMap<usize, (String, Scalar)>> {
    transform_spec
        .iter()
        .filter_map(|field_transform| match field_transform {
            FieldTransformSpec::MetadataDerivedColumn { field_index, .. } => Some(
                parse_partition_value(*field_index, logical_schema, partition_values),
            ),
            FieldTransformSpec::DynamicColumn { field_index, .. } => {
                // Dynamic columns may also need metadata values when not physical
                Some(parse_partition_value(
                    *field_index,
                    logical_schema,
                    partition_values,
                ))
            }
            FieldTransformSpec::StaticInsert { .. }
            | FieldTransformSpec::StaticReplace { .. }
            | FieldTransformSpec::StaticDrop { .. } => None,
        })
        .try_collect()
}

/// Build a transform expression that converts physical data to the logical schema.
///
/// An empty `transform_spec` is valid and represents the case where only column mapping is needed.
/// The resulting empty `Expression::Transform` will pass all input fields through unchanged
/// while applying the output schema for name mapping.
pub(crate) fn get_transform_expr(
    transform_spec: &TransformSpec,
    mut partition_values: HashMap<usize, (String, Scalar)>,
    physical_schema: &StructType,
) -> DeltaResult<ExpressionRef> {
    let mut transform = Transform::new_top_level();

    for field_transform in transform_spec {
        use FieldTransformSpec::*;
        transform = match field_transform {
            StaticInsert { insert_after, expr } => {
                transform.with_inserted_field(insert_after.clone(), expr.clone())
            }
            StaticReplace { field_name, expr } => {
                transform.with_replaced_field(field_name.clone(), expr.clone())
            }
            StaticDrop { field_name } => transform.with_dropped_field(field_name.clone()),
            MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                let Some((_, partition_value)) = partition_values.remove(field_index) else {
                    return Err(Error::MissingData(format!(
                        "missing partition value for field index {field_index}"
                    )));
                };

                let partition_value = Arc::new(partition_value.into());
                transform.with_inserted_field(insert_after.clone(), partition_value)
            }
            DynamicColumn {
                field_index,
                physical_name,
                insert_after,
            } => {
                // Check if this column exists in the physical schema
                let exists_physically = physical_schema.field(physical_name).is_some();

                if exists_physically {
                    // Column exists physically - reorder it via drop+insert
                    // This ensures consistent column ordering across file types
                    transform = transform
                        .with_dropped_field(physical_name.clone())
                        .with_inserted_field(
                            insert_after.clone(),
                            Arc::new(Expression::column([physical_name.clone()])),
                        );
                    transform
                } else {
                    // Column doesn't exist physically - treat as partition column
                    let Some((_, partition_value)) = partition_values.remove(field_index) else {
                        return Err(Error::MissingData(format!(
                            "missing partition value for dynamic column '{}' at index {}",
                            physical_name, field_index
                        )));
                    };

                    let partition_value = Arc::new(partition_value.into());
                    transform.with_inserted_field(insert_after.clone(), partition_value)
                }
            }
        }
    }

    Ok(Arc::new(Expression::Transform(transform)))
}

/// Generate a transform specification that describes how to convert physical data to logical schema.
///
/// The transform spec captures only the fields that need to be added, replaced, or reordered.
/// Unchanged fields pass through implicitly in their original order (sparse transform).
pub(crate) fn get_transform_spec(all_fields: &[ColumnType]) -> TransformSpec {
    let mut transform_spec = TransformSpec::new();
    let mut last_physical_field: Option<&str> = None;

    for field in all_fields {
        match field {
            ColumnType::Selected(physical_name) => {
                // Track the last physical field for calculating insertion points
                last_physical_field = Some(physical_name);
            }
            ColumnType::MetadataDerivedColumn(logical_idx) => {
                // Partition columns are inserted after the last physical field
                transform_spec.push(FieldTransformSpec::MetadataDerivedColumn {
                    insert_after: last_physical_field.map(String::from),
                    field_index: *logical_idx,
                });
            }
            ColumnType::Dynamic {
                logical_index,
                physical_name,
            } => {
                // Dynamic columns may need reordering or insertion depending on physical schema
                transform_spec.push(FieldTransformSpec::DynamicColumn {
                    field_index: *logical_index,
                    physical_name: physical_name.clone(),
                    insert_after: last_physical_field.map(String::from),
                });
            }
        }
    }

    transform_spec
}

/// Parse a partition value from the raw string representation
pub(crate) fn parse_partition_value_raw(
    raw: Option<&String>,
    data_type: &DataType,
) -> DeltaResult<Scalar> {
    match (raw, data_type.as_primitive_opt()) {
        (Some(v), Some(primitive)) => primitive.parse_scalar(v),
        (Some(_), None) => Err(Error::generic(format!(
            "Unexpected partition column type: {data_type:?}"
        ))),
        _ => Ok(Scalar::Null(data_type.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, PrimitiveType, StructField, StructType};
    use std::collections::HashMap;

    #[test]
    fn test_parse_partition_value_invalid_index() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "col1",
            DataType::STRING,
        )]));
        let partition_values = HashMap::new();

        let result = parse_partition_value(5, &schema, &partition_values);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn test_parse_partition_values_mixed_transforms() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("age", DataType::LONG),
        ]));
        let transform_spec = vec![
            FieldTransformSpec::MetadataDerivedColumn {
                field_index: 1,
                insert_after: Some("id".to_string()),
            },
            FieldTransformSpec::StaticDrop {
                field_name: "unused".to_string(),
            },
            FieldTransformSpec::MetadataDerivedColumn {
                field_index: 0,
                insert_after: None,
            },
        ];
        let mut partition_values = HashMap::new();
        partition_values.insert("age".to_string(), "30".to_string());
        partition_values.insert("id".to_string(), "test".to_string());

        let result = parse_partition_values(&schema, &transform_spec, &partition_values).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&0));
        assert!(result.contains_key(&1));
    }

    #[test]
    fn test_parse_partition_values_empty_spec() {
        let schema = Arc::new(StructType::new_unchecked(vec![]));
        let transform_spec = vec![];
        let partition_values = HashMap::new();

        let result = parse_partition_values(&schema, &transform_spec, &partition_values).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_transform_expr_missing_partition_value() {
        let transform_spec = vec![FieldTransformSpec::MetadataDerivedColumn {
            field_index: 0,
            insert_after: None,
        }];
        let partition_values = HashMap::new(); // Missing required partition value

        // Create a minimal physical schema for test
        let physical_schema = StructType::new_unchecked(vec![]);
        let result = get_transform_expr(&transform_spec, partition_values, &physical_schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing partition value"));
    }

    #[test]
    fn test_get_transform_expr_static_transforms() {
        let expr = Arc::new(Expression::literal(42));
        let transform_spec = vec![
            FieldTransformSpec::StaticInsert {
                insert_after: Some("col1".to_string()),
                expr: expr.clone(),
            },
            FieldTransformSpec::StaticReplace {
                field_name: "col2".to_string(),
                expr: expr.clone(),
            },
            FieldTransformSpec::StaticDrop {
                field_name: "col3".to_string(),
            },
        ];
        let metadata_values = HashMap::new();

        // Create a minimal physical schema for test
        let physical_schema = StructType::new_unchecked(vec![]);
        let result =
            get_transform_expr(&transform_spec, metadata_values, &physical_schema).unwrap();
        assert!(matches!(result.as_ref(), Expression::Transform(_)));
    }

    #[test]
    fn test_get_transform_spec_selected_only() {
        let all_fields = vec![
            ColumnType::Selected("col1".to_string()),
            ColumnType::Selected("col2".to_string()),
        ];

        let result = get_transform_spec(&all_fields);
        assert!(result.is_empty()); // No metadata columns = empty transform spec
    }

    #[test]
    fn test_dynamic_column_transform_physical_reorder() {
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Dynamic {
                logical_index: 1,
                physical_name: "_change_type".to_string(),
            },
            ColumnType::MetadataDerivedColumn(2),
        ];

        let transform_spec = get_transform_spec(&all_fields);
        assert_eq!(transform_spec.len(), 2); // Dynamic and Metadata columns

        // Verify the transform spec contains expected operations
        match &transform_spec[0] {
            FieldTransformSpec::DynamicColumn {
                field_index,
                physical_name,
                insert_after,
            } => {
                assert_eq!(*field_index, 1);
                assert_eq!(physical_name, "_change_type");
                assert_eq!(insert_after, &Some("id".to_string()));
            }
            _ => panic!("Expected DynamicColumn transform"),
        }

        // Test transform expression with physical _change_type
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
        ]);
        let mut metadata_values = HashMap::new();
        metadata_values.insert(2, ("_commit_version".to_string(), Scalar::Long(42)));

        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        let transform_expr = result.expect("Transform expression should be created successfully");

        // Verify it's a Transform expression that will reorder _change_type
        assert!(
            matches!(transform_expr.as_ref(), Expression::Transform(_)),
            "Expected Transform expression for reordering"
        );
    }

    #[test]
    fn test_dynamic_column_transform_metadata_insertion() {
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Dynamic {
                logical_index: 1,
                physical_name: "_change_type".to_string(),
            },
            ColumnType::MetadataDerivedColumn(2),
        ];

        let transform_spec = get_transform_spec(&all_fields);

        // Test transform expression without physical _change_type (Add/Remove files)
        let physical_schema_no_change_type =
            StructType::new_unchecked(vec![StructField::nullable("id", DataType::STRING)]);
        let mut metadata_values = HashMap::new();
        metadata_values.insert(
            1,
            (
                "_change_type".to_string(),
                Scalar::String("insert".to_string()),
            ),
        );
        metadata_values.insert(2, ("_commit_version".to_string(), Scalar::Long(42)));

        let result = get_transform_expr(
            &transform_spec,
            metadata_values,
            &physical_schema_no_change_type,
        );
        let transform_expr = result.expect("Transform expression should be created successfully");

        // Verify it's a Transform expression that will insert _change_type from metadata
        assert!(
            matches!(transform_expr.as_ref(), Expression::Transform(_)),
            "Expected Transform expression for metadata insertion"
        );
    }

    #[test]
    fn test_get_transform_spec_with_metadata() {
        let all_fields = vec![
            ColumnType::Selected("col1".to_string()),
            ColumnType::MetadataDerivedColumn(1),
            ColumnType::Selected("col2".to_string()),
            ColumnType::MetadataDerivedColumn(2),
        ];

        let result = get_transform_spec(&all_fields);
        assert_eq!(result.len(), 2);

        // Check first metadata column
        if let FieldTransformSpec::MetadataDerivedColumn {
            field_index,
            insert_after,
        } = &result[0]
        {
            assert_eq!(*field_index, 1);
            assert_eq!(insert_after.as_ref().unwrap(), "col1");
        } else {
            panic!("Expected MetadataDerivedColumn transform");
        }

        // Check second metadata column
        if let FieldTransformSpec::MetadataDerivedColumn {
            field_index,
            insert_after,
        } = &result[1]
        {
            assert_eq!(*field_index, 2);
            assert_eq!(insert_after.as_ref().unwrap(), "col2");
        } else {
            panic!("Expected MetadataDerivedColumn transform");
        }
    }

    #[test]
    fn test_parse_partition_value_raw_string() {
        let result =
            parse_partition_value_raw(Some(&"test_string".to_string()), &DataType::STRING).unwrap();
        assert_eq!(result, Scalar::String("test_string".to_string()));
    }

    #[test]
    fn test_parse_partition_value_raw_integer() {
        let result = parse_partition_value_raw(
            Some(&"42".to_string()),
            &DataType::Primitive(PrimitiveType::Integer),
        )
        .unwrap();
        assert_eq!(result, Scalar::Integer(42));
    }

    #[test]
    fn test_parse_partition_value_raw_null() {
        let result = parse_partition_value_raw(None, &DataType::STRING).unwrap();
        assert!(matches!(result, Scalar::Null(_)));
    }

    #[test]
    fn test_parse_partition_value_raw_invalid_type() {
        let result = parse_partition_value_raw(
            Some(&"value".to_string()),
            &DataType::struct_type_unchecked(vec![]), // Non-primitive type
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unexpected partition column type"));
    }

    #[test]
    fn test_parse_partition_value_raw_invalid_parse() {
        let result = parse_partition_value_raw(
            Some(&"not_a_number".to_string()),
            &DataType::Primitive(PrimitiveType::Integer),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_cdf_partition_values_with_cdc_file() {
        // Test partition values + CDF columns for CDC files (where _change_type is physical)

        // Create a logical schema with regular columns, partition columns, and CDF columns
        let logical_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("year", DataType::INTEGER), // Partition column
            StructField::nullable("month", DataType::INTEGER), // Partition column
            StructField::nullable("_change_type", DataType::STRING), // CDF Dynamic column
            StructField::nullable("_commit_version", DataType::LONG), // CDF metadata column
        ]));

        // Define the all_fields with mix of selected, partition, and CDF columns
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::MetadataDerivedColumn(1), // year partition
            ColumnType::MetadataDerivedColumn(2), // month partition
            ColumnType::Dynamic {
                logical_index: 3,
                physical_name: "_change_type".to_string(),
            },
            ColumnType::MetadataDerivedColumn(4), // _commit_version
        ];

        // Get the transform spec
        let transform_spec = get_transform_spec(&all_fields);
        assert_eq!(transform_spec.len(), 4); // 2 partition columns + 1 dynamic + 1 CDF metadata

        // Physical schema for CDC files includes _change_type
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
        ]);

        // Prepare all metadata values (partitions + CDF metadata)
        let mut partition_values = HashMap::new();
        partition_values.insert("year".to_string(), "2024".to_string());
        partition_values.insert("month".to_string(), "3".to_string());

        let mut all_metadata_values =
            parse_partition_values(&logical_schema, &transform_spec, &partition_values)
                .expect("Should parse partition values");

        // parse_partition_values will try to parse all MetadataDerivedColumn entries
        // It will return nulls for CDF columns not in partition_values
        assert!(all_metadata_values.contains_key(&1)); // year
        assert!(all_metadata_values.contains_key(&2)); // month
        assert_eq!(
            all_metadata_values.get(&1).unwrap().1,
            Scalar::Integer(2024)
        );
        assert_eq!(all_metadata_values.get(&2).unwrap().1, Scalar::Integer(3));
        // _commit_version (index 4) will be null since not in partition_values
        assert!(matches!(
            all_metadata_values.get(&4).map(|(_, v)| v),
            Some(Scalar::Null(_))
        ));

        // Add CDF metadata
        all_metadata_values.insert(4, ("_commit_version".to_string(), Scalar::Long(100)));

        // Create and validate the transform expression
        let transform_expr =
            get_transform_expr(&transform_spec, all_metadata_values, &physical_schema)
                .expect("Should create transform expression");

        // Verify it creates a transform that will reorder _change_type from physical
        assert!(matches!(transform_expr.as_ref(), Expression::Transform(_)));
    }

    #[test]
    fn test_dynamic_column_missing_metadata_error() {
        // Test that we get an error when a Dynamic column needs metadata but it's not provided
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Dynamic {
                logical_index: 1,
                physical_name: "_change_type".to_string(),
            },
        ];

        let transform_spec = get_transform_spec(&all_fields);

        // Physical schema without _change_type (so it needs to come from metadata)
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
        ]);

        // Empty metadata values - missing required _change_type
        let metadata_values = HashMap::new();

        // Should fail with missing data error
        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("missing partition value for dynamic column"));
        assert!(err.to_string().contains("_change_type"));
    }

    #[test]
    fn test_cdf_partition_values_with_add_remove_file() {
        // Test partition values + CDF columns for Add/Remove files (where _change_type is metadata)

        // Create a logical schema with regular columns, partition columns, and CDF columns
        let logical_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("year", DataType::INTEGER), // Partition column
            StructField::nullable("month", DataType::INTEGER), // Partition column
            StructField::nullable("_change_type", DataType::STRING), // CDF Dynamic column
            StructField::nullable("_commit_version", DataType::LONG), // CDF metadata column
        ]));

        // Define the all_fields with mix of selected, partition, and CDF columns
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::MetadataDerivedColumn(1), // year partition
            ColumnType::MetadataDerivedColumn(2), // month partition
            ColumnType::Dynamic {
                logical_index: 3,
                physical_name: "_change_type".to_string(),
            },
            ColumnType::MetadataDerivedColumn(4), // _commit_version
        ];

        // Get the transform spec
        let transform_spec = get_transform_spec(&all_fields);
        assert_eq!(transform_spec.len(), 4);

        // Physical schema for Add/Remove files - no _change_type
        let physical_schema =
            StructType::new_unchecked(vec![StructField::nullable("id", DataType::STRING)]);

        // Prepare all metadata values including _change_type
        let mut partition_values = HashMap::new();
        partition_values.insert("year".to_string(), "2024".to_string());
        partition_values.insert("month".to_string(), "3".to_string());
        partition_values.insert("_change_type".to_string(), "insert".to_string()); // Computed for Add file

        let mut all_metadata_values =
            parse_partition_values(&logical_schema, &transform_spec, &partition_values)
                .expect("Should parse all values including dynamic _change_type");

        // Verify we parsed all expected values including _change_type
        // parse_partition_values processes all metadata columns, returning nulls for missing ones
        assert!(all_metadata_values.contains_key(&1)); // year
        assert!(all_metadata_values.contains_key(&2)); // month
        assert!(all_metadata_values.contains_key(&3)); // _change_type
        assert!(all_metadata_values.contains_key(&4)); // _commit_version (will be null)
        assert_eq!(
            all_metadata_values.get(&1).unwrap().1,
            Scalar::Integer(2024)
        );
        assert_eq!(all_metadata_values.get(&2).unwrap().1, Scalar::Integer(3));
        assert_eq!(
            all_metadata_values.get(&3).unwrap().1,
            Scalar::String("insert".to_string())
        );
        // _commit_version will be null since not provided
        assert!(matches!(
            all_metadata_values.get(&4).map(|(_, v)| v),
            Some(Scalar::Null(_))
        ));

        // Add CDF metadata
        all_metadata_values.insert(4, ("_commit_version".to_string(), Scalar::Long(100)));

        // Create and validate the transform expression
        let transform_expr =
            get_transform_expr(&transform_spec, all_metadata_values, &physical_schema)
                .expect("Should create transform expression with metadata _change_type");

        // Verify it creates a transform that will insert _change_type from metadata
        assert!(matches!(transform_expr.as_ref(), Expression::Transform(_)));
    }
}
