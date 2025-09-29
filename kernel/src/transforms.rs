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

    /// A metadata-derived column (e.g., partition columns, CDF version and timestamp columns).
    /// The usize is the index of this column in the logical schema.
    MetadataDerivedColumn(usize),

    /// A column whose source varies by context (physical vs. metadata-derived).
    /// If the column exists in the physical schema, this reorders it to the correct index.
    /// Otherwise, this is treated as a MetadataDerivedColumn.
    ///
    /// This is used for CDF's _change_type which may exist physically in `.cdc` files but
    /// is metadata derived Add/Remove files.
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
    /// Insert the given expression after the named input column (None = prepend instead)
    // NOTE: It's quite likely we will sometimes need to reorder columns for one reason or another,
    // which would usually be expressed as a drop+insert pair of transforms.
    #[allow(unused)]
    StaticInsert {
        insert_after: Option<String>,
        expr: ExpressionRef,
    },
    /// Replace the named input column with an expression
    // NOTE: Row tracking will eventually need to replace the physical rowid column with a COALESCE
    // to compute non-materialized row ids and row commit versions.
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
            FieldTransformSpec::DynamicColumn { .. }
            | FieldTransformSpec::StaticInsert { .. }
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
    mut metadata_values: HashMap<usize, (String, Scalar)>,
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
                let Some((_, partition_value)) = metadata_values.remove(field_index) else {
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
                    let Some((_, partition_value)) = metadata_values.remove(field_index) else {
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
    use crate::utils::test_utils::assert_result_error_with_message;
    use std::collections::HashMap;

    // Tests for parse_partition_value function
    #[test]
    fn test_parse_partition_value_invalid_index() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "col1",
            DataType::STRING,
        )]));
        let partition_values = HashMap::new();

        let result = parse_partition_value(5, &schema, &partition_values);
        assert_result_error_with_message(result, "out of bounds");
    }

    // Tests for parse_partition_values function
    #[test]
    fn test_parse_partition_values_mixed_transforms() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("age", DataType::LONG),
            StructField::nullable("_change_type", DataType::STRING),
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
            FieldTransformSpec::DynamicColumn {
                field_index: 2,
                physical_name: "_change_type".to_string(),
                insert_after: Some("id".to_string()),
            },
        ];
        let mut partition_values = HashMap::new();
        partition_values.insert("age".to_string(), "30".to_string());
        partition_values.insert("id".to_string(), "test".to_string());
        partition_values.insert("_change_type".to_string(), "insert".to_string());

        let result = parse_partition_values(&schema, &transform_spec, &partition_values).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&0));
        assert!(result.contains_key(&1));
        assert!(!result.contains_key(&2));

        // Verify the parsed values
        assert_eq!(
            result.get(&0).unwrap().1,
            Scalar::String("test".to_string())
        );
        assert_eq!(result.get(&1).unwrap().1, Scalar::Long(30));
    }

    #[test]
    fn test_parse_partition_values_empty_spec() {
        let schema = Arc::new(StructType::new_unchecked(vec![]));
        let transform_spec = vec![];
        let partition_values = HashMap::new();

        let result = parse_partition_values(&schema, &transform_spec, &partition_values).unwrap();
        assert!(result.is_empty());
    }

    // Tests for parse_partition_value_raw function
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
        assert_result_error_with_message(result, "Unexpected partition column type");
    }

    #[test]
    fn test_parse_partition_value_raw_invalid_parse() {
        let result = parse_partition_value_raw(
            Some(&"not_a_number".to_string()),
            &DataType::Primitive(PrimitiveType::Integer),
        );
        assert_result_error_with_message(result, "Failed to parse value");
    }

    // Tests for get_transform_spec function
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
    fn test_get_transform_spec_dynamic_column() {
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Dynamic {
                logical_index: 1,
                physical_name: "_change_type".to_string(),
            },
        ];

        let transform_spec = get_transform_spec(&all_fields);
        assert_eq!(transform_spec.len(), 1);

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

    // Tests for get_transform_expr function
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
        assert_result_error_with_message(result, "missing partition value");
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

        // Create a physical schema with the relevant columns
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("col1", DataType::STRING),
            StructField::nullable("col2", DataType::INTEGER),
            StructField::nullable("col3", DataType::LONG),
        ]);
        let result =
            get_transform_expr(&transform_spec, metadata_values, &physical_schema).unwrap();

        let Expression::Transform(transform) = result.as_ref() else {
            panic!("Expected Transform expression");
        };

        // Verify StaticInsert: should insert after col1
        assert!(transform.field_transforms.contains_key("col1"));
        assert!(!transform.field_transforms["col1"].is_replace);
        assert_eq!(transform.field_transforms["col1"].exprs.len(), 1);
        let Expression::Literal(scalar) = transform.field_transforms["col1"].exprs[0].as_ref()
        else {
            panic!("Expected literal expression for insert");
        };
        assert_eq!(scalar, &Scalar::Integer(42));

        // Verify StaticReplace: should replace col2 with the expression
        assert!(transform.field_transforms.contains_key("col2"));
        assert!(transform.field_transforms["col2"].is_replace);
        assert_eq!(transform.field_transforms["col2"].exprs.len(), 1);
        let Expression::Literal(scalar) = transform.field_transforms["col2"].exprs[0].as_ref()
        else {
            panic!("Expected literal expression for replace");
        };
        assert_eq!(scalar, &Scalar::Integer(42));

        // Verify StaticDrop: should drop col3 (empty expressions and is_replace = true)
        assert!(transform.field_transforms.contains_key("col3"));
        assert!(transform.field_transforms["col3"].is_replace);
        assert!(transform.field_transforms["col3"].exprs.is_empty());
    }

    #[test]
    fn test_get_transform_expr_dynamic_column_physical() {
        let transform_spec = vec![FieldTransformSpec::DynamicColumn {
            field_index: 1,
            physical_name: "_change_type".to_string(),
            insert_after: Some("id".to_string()),
        }];

        // Physical schema contains change_type
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
        ]);
        let metadata_values = HashMap::new();

        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        let transform_expr = result.expect("Transform expression should be created successfully");

        let Expression::Transform(transform) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };

        // Should drop _change_type and insert it after id
        assert!(transform.field_transforms.contains_key("_change_type"));
        assert!(transform.field_transforms["_change_type"].is_replace);
        assert!(transform.field_transforms["_change_type"].exprs.is_empty());

        assert!(transform.field_transforms.contains_key("id"));
        assert!(!transform.field_transforms["id"].is_replace);
        assert_eq!(transform.field_transforms["id"].exprs.len(), 1);

        let Expression::Column(column_name) = transform.field_transforms["id"].exprs[0].as_ref()
        else {
            panic!("Expected column reference");
        };
        assert_eq!(column_name.as_ref(), &["_change_type"]);
    }

    #[test]
    fn test_get_transform_expr_dynamic_column_metadata() {
        let transform_spec = vec![FieldTransformSpec::DynamicColumn {
            field_index: 1,
            physical_name: "_change_type".to_string(),
            insert_after: Some("id".to_string()),
        }];

        // Physical schema does not contain change_type
        let physical_schema =
            StructType::new_unchecked(vec![StructField::nullable("id", DataType::STRING)]);
        let mut metadata_values = HashMap::new();
        metadata_values.insert(
            1,
            (
                "_change_type".to_string(),
                Scalar::String("insert".to_string()),
            ),
        );

        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        let transform_expr = result.expect("Transform expression should be created successfully");

        let Expression::Transform(transform) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };

        // Should not drop _change_type (doesn't exist physically) and insert metadata value after id
        assert!(!transform.field_transforms.contains_key("_change_type"));

        assert!(transform.field_transforms.contains_key("id"));
        assert!(!transform.field_transforms["id"].is_replace);
        assert_eq!(transform.field_transforms["id"].exprs.len(), 1);

        let Expression::Literal(scalar) = transform.field_transforms["id"].exprs[0].as_ref() else {
            panic!("Expected literal");
        };
        assert_eq!(scalar, &Scalar::String("insert".to_string()));
    }

    #[test]
    fn test_get_transform_expr_metadata_derived_column() {
        let transform_spec = vec![FieldTransformSpec::MetadataDerivedColumn {
            field_index: 1,
            insert_after: Some("id".to_string()),
        }];

        let physical_schema =
            StructType::new_unchecked(vec![StructField::nullable("id", DataType::STRING)]);
        let mut metadata_values = HashMap::new();
        metadata_values.insert(1, ("year".to_string(), Scalar::Integer(2024)));

        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        let transform_expr = result.expect("Transform expression should be created successfully");

        let Expression::Transform(transform) = transform_expr.as_ref() else {
            panic!("Expected Transform expression");
        };

        // Should insert metadata value after id
        assert!(transform.field_transforms.contains_key("id"));
        assert!(!transform.field_transforms["id"].is_replace);
        assert_eq!(transform.field_transforms["id"].exprs.len(), 1);

        let Expression::Literal(scalar) = transform.field_transforms["id"].exprs[0].as_ref() else {
            panic!("Expected literal");
        };
        assert_eq!(scalar, &Scalar::Integer(2024));
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
        let physical_schema =
            StructType::new_unchecked(vec![StructField::nullable("id", DataType::STRING)]);

        // Empty metadata values - missing required _change_type
        let metadata_values = HashMap::new();

        // Should fail with missing data error
        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        assert_result_error_with_message(result, "missing partition value for dynamic column");
    }
}
