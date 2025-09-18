//! Transform-related types and utilities for Delta Kernel.
//!
//! This module contains the types and functions needed to handle transforms
//! during scan and table changes operations, including partition value processing
//! and expression generation.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

<<<<<<< HEAD
use crate::expressions::{Expression, ExpressionRef};
use crate::schema::{DataType, SchemaRef};
use crate::{DeltaResult, Error};

/// Scan uses this to set up what kinds of top-level columns it is scanning. For `Selected` we just
/// store the name of the column, as that's all that's needed during the actual query. For
/// `Partition` we store an index into the logical schema for this query since later we need the
/// data type as well to materialize the partition column.
#[derive(PartialEq, Debug)]
pub(crate) enum ColumnType {
    // A column, selected from the data, as is
    Selected(String),
    // A partition column that needs to be added back in
    Partition(usize),
=======
use crate::expressions::{Expression, ExpressionRef, Scalar, Transform};
use crate::schema::{DataType, SchemaRef};
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

    /// A partition column that needs to be added back in.
    /// The usize is the index of this column in the logical schema.
    Partition(usize),

    /// A column whose source varies by context (physical vs. metadata-derived).
    /// Used for CDF's _change_type which exists physically in CDC files but
    /// must be computed for Add/Remove files.
    Dynamic {
        /// Index of this column in the logical schema
        logical_index: usize,
        /// Name to look for in the physical schema
        physical_name: String,
    },
>>>>>>> 2b01c19 (what)
}

/// A list of field transforms that describes a transform expression to be created at scan time.
pub(crate) type TransformSpec = Vec<FieldTransformSpec>;

<<<<<<< HEAD
/// Transforms aren't computed all at once. So static ones can just go straight to `Expression`, but
/// things like partition columns need to filled in. This enum holds an expression that's part of a
/// [`TransformSpec`].
#[derive(Debug)]
pub(crate) enum FieldTransformSpec {
    /// Insert the given expression after the named input column (None = prepend instead)
    // NOTE: It's quite likely we will sometimes need to reorder columns for one reason or another,
    // which would usually be expressed as a drop+insert pair of transforms.
=======
/// Describes a single field transformation to apply when converting physical data to logical schema.
///
/// These transformations are "sparse" - they only specify what changes, while unchanged fields
/// pass through implicitly in their original order.
#[derive(Debug)]
pub(crate) enum FieldTransformSpec {
    /// Insert a static expression after the named input column.
    /// If `insert_after` is None, prepend before all fields.
    // NOTE: Row tracking and other features may use this for computed columns.
>>>>>>> 2b01c19 (what)
    #[allow(unused)]
    StaticInsert {
        insert_after: Option<String>,
        expr: ExpressionRef,
    },
<<<<<<< HEAD
    /// Replace the named input column with an expression
    // NOTE: Row tracking will eventually need to replace the physical rowid column with a COALESCE
    // to compute non-materialized row ids and row commit versions.
=======

    /// Replace the named input column with a new expression.
    // NOTE: Row tracking will use this to replace physical rowid columns with
    // COALESCE expressions for non-materialized row ids.
>>>>>>> 2b01c19 (what)
    #[allow(unused)]
    StaticReplace {
        field_name: String,
        expr: ExpressionRef,
    },
<<<<<<< HEAD
    /// Drops the named input column
    // NOTE: Row tracking will need to drop metadata columns that were used to compute rowids, since
    // they should not appear in the query's output.
    #[allow(unused)]
    StaticDrop { field_name: String },
    /// Inserts a partition column after the named input column. The partition column is identified
    /// by its field index in the logical table schema (the column is not present in the physical
    /// read schema). Its value varies from file to file and is obtained from file metadata.
    PartitionColumn {
        field_index: usize,
=======

    /// Remove the named input column from the output.
    // NOTE: Row tracking will use this to drop internal metadata columns.
    #[allow(unused)]
    StaticDrop { field_name: String },

    /// Insert a partition column after the named input column.
    /// The partition column is identified by its field index in the logical table schema.
    /// Its value varies from file to file and is obtained from file metadata.
    PartitionColumn {
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
>>>>>>> 2b01c19 (what)
        insert_after: Option<String>,
    },
}

/// Parse a single partition value from the raw string representation
pub(crate) fn parse_partition_value(
    field_idx: usize,
    logical_schema: &SchemaRef,
    partition_values: &HashMap<String, String>,
<<<<<<< HEAD
) -> DeltaResult<(usize, (String, crate::expressions::Scalar))> {
=======
) -> DeltaResult<(usize, (String, Scalar))> {
>>>>>>> 2b01c19 (what)
    let Some(field) = logical_schema.field_at_index(field_idx) else {
        return Err(Error::InternalError(format!(
            "out of bounds partition column field index {field_idx}"
        )));
    };
    let name = field.physical_name();
    let partition_value = parse_partition_value_raw(partition_values.get(name), field.data_type())?;
    Ok((field_idx, (name.to_string(), partition_value)))
}

<<<<<<< HEAD
/// Parse all partition values from a transform spec
=======
/// Parse all partition values from a transform spec.
///
/// Extracts partition column indices from the transform spec and looks up their values
/// in the provided HashMap, parsing them according to their data types in the schema.
>>>>>>> 2b01c19 (what)
pub(crate) fn parse_partition_values(
    logical_schema: &SchemaRef,
    transform_spec: &TransformSpec,
    partition_values: &HashMap<String, String>,
<<<<<<< HEAD
) -> DeltaResult<HashMap<usize, (String, crate::expressions::Scalar)>> {
=======
) -> DeltaResult<HashMap<usize, (String, Scalar)>> {
>>>>>>> 2b01c19 (what)
    transform_spec
        .iter()
        .filter_map(|field_transform| match field_transform {
            FieldTransformSpec::PartitionColumn { field_index, .. } => Some(parse_partition_value(
                *field_index,
                logical_schema,
                partition_values,
            )),
<<<<<<< HEAD
=======
            FieldTransformSpec::DynamicColumn { field_index, .. } => {
                // Dynamic columns may also need metadata values when not physical
                Some(parse_partition_value(
                    *field_index,
                    logical_schema,
                    partition_values,
                ))
            }
>>>>>>> 2b01c19 (what)
            FieldTransformSpec::StaticInsert { .. }
            | FieldTransformSpec::StaticReplace { .. }
            | FieldTransformSpec::StaticDrop { .. } => None,
        })
        .try_collect()
}

<<<<<<< HEAD
/// Compute an expression that will transform from physical to logical for a given Add file action
///
/// An empty `transform_spec` is valid and represents the case where only column mapping is needed
/// (e.g., no partition columns to inject). The resulting empty `Expression::Transform` will
/// pass all input fields through unchanged while applying the output schema for name mapping.
pub(crate) fn get_transform_expr(
    transform_spec: &TransformSpec,
    mut partition_values: HashMap<usize, (String, crate::expressions::Scalar)>,
) -> DeltaResult<ExpressionRef> {
    let mut transform = crate::expressions::Transform::new_top_level();
=======
/// Build a transform expression that converts physical data to the logical schema.
///
/// # Arguments
/// * `transform_spec` - The list of field transformations to apply
/// * `partition_values` - Values for partition columns and dynamic columns when not physical
/// * `physical_schema` - Optional physical schema, required when using Dynamic columns
///
/// # Returns
/// An expression that transforms physical data to match the logical schema
///
/// An empty `transform_spec` is valid and represents the case where only column mapping is needed.
/// The resulting empty `Expression::Transform` will pass all input fields through unchanged
/// while applying the output schema for name mapping.
pub(crate) fn get_transform_expr(
    transform_spec: &TransformSpec,
    mut partition_values: HashMap<usize, (String, Scalar)>,
    physical_schema: &crate::schema::StructType,
) -> DeltaResult<ExpressionRef> {
    let mut transform = Transform::new_top_level();
>>>>>>> 2b01c19 (what)

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
            PartitionColumn {
                field_index,
                insert_after,
            } => {
                let Some((_, partition_value)) = partition_values.remove(field_index) else {
                    return Err(Error::InternalError(format!(
                        "missing partition value for field index {field_index}"
                    )));
                };

                let partition_value = Arc::new(partition_value.into());
                transform.with_inserted_field(insert_after.clone(), partition_value)
            }
<<<<<<< HEAD
=======
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
                        return Err(Error::InternalError(format!(
                            "missing partition value for dynamic column '{}' at index {}",
                            physical_name, field_index
                        )));
                    };

                    let partition_value = Arc::new(partition_value.into());
                    transform.with_inserted_field(insert_after.clone(), partition_value)
                }
            }
>>>>>>> 2b01c19 (what)
        }
    }

    Ok(Arc::new(Expression::Transform(transform)))
}

<<<<<<< HEAD
/// Computes the transform spec for this scan. Static (query-level) transforms can already be
/// turned into expressions now, but file-level transforms like partition values can only be
/// described now; they are converted to expressions during the scan, using file metadata.
///
/// NOTE: Transforms are "sparse" in the sense that they only mention fields which actually
/// change (added, replaced, dropped); the transform implicitly captures all fields that pass
/// from input to output unchanged and in the same relative order.
=======
/// Generate a transform specification that describes how to convert physical data to logical schema.
///
/// The transform spec captures only the fields that need to be added, replaced, or reordered.
/// Unchanged fields pass through implicitly in their original order (sparse transform).
///
/// # Arguments
/// * `all_fields` - The column types for all fields in the logical schema, in order
///
/// # Returns
/// A list of field transformations to apply to the physical data
>>>>>>> 2b01c19 (what)
pub(crate) fn get_transform_spec(all_fields: &[ColumnType]) -> TransformSpec {
    let mut transform_spec = TransformSpec::new();
    let mut last_physical_field: Option<&str> = None;

    for field in all_fields {
        match field {
            ColumnType::Selected(physical_name) => {
<<<<<<< HEAD
                // Track physical field for calculating partition value insertion points.
                last_physical_field = Some(physical_name);
            }
            ColumnType::Partition(logical_idx) => {
=======
                // Track the last physical field for calculating insertion points
                last_physical_field = Some(physical_name);
            }
            ColumnType::Partition(logical_idx) => {
                // Partition columns are inserted after the last physical field
>>>>>>> 2b01c19 (what)
                transform_spec.push(FieldTransformSpec::PartitionColumn {
                    insert_after: last_physical_field.map(String::from),
                    field_index: *logical_idx,
                });
            }
<<<<<<< HEAD
=======
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
>>>>>>> 2b01c19 (what)
        }
    }

    transform_spec
}

/// Parse a partition value from the raw string representation
/// This was originally `parse_partition_value` in scan/mod.rs
pub(crate) fn parse_partition_value_raw(
    raw: Option<&String>,
    data_type: &DataType,
<<<<<<< HEAD
) -> DeltaResult<crate::expressions::Scalar> {
    use crate::expressions::Scalar;
=======
) -> DeltaResult<Scalar> {
>>>>>>> 2b01c19 (what)
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
<<<<<<< HEAD
    use crate::expressions::Scalar;
=======
>>>>>>> 2b01c19 (what)
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
            FieldTransformSpec::PartitionColumn {
                field_index: 1,
                insert_after: Some("id".to_string()),
            },
            FieldTransformSpec::StaticDrop {
                field_name: "unused".to_string(),
            },
            FieldTransformSpec::PartitionColumn {
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
        let transform_spec = vec![FieldTransformSpec::PartitionColumn {
            field_index: 0,
            insert_after: None,
        }];
        let partition_values = HashMap::new(); // Missing required partition value

<<<<<<< HEAD
        let result = get_transform_expr(&transform_spec, partition_values);
=======
        // Create a minimal physical schema for test
        let physical_schema = StructType::new_unchecked(vec![]);
        let result = get_transform_expr(&transform_spec, partition_values, &physical_schema);
>>>>>>> 2b01c19 (what)
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
<<<<<<< HEAD
        let partition_values = HashMap::new();

        let result = get_transform_expr(&transform_spec, partition_values).unwrap();
=======
        let metadata_values = HashMap::new();

        // Create a minimal physical schema for test
        let physical_schema = StructType::new_unchecked(vec![]);
        let result =
            get_transform_expr(&transform_spec, metadata_values, &physical_schema).unwrap();
>>>>>>> 2b01c19 (what)
        assert!(matches!(result.as_ref(), Expression::Transform(_)));
    }

    #[test]
    fn test_get_transform_spec_selected_only() {
        let all_fields = vec![
            ColumnType::Selected("col1".to_string()),
            ColumnType::Selected("col2".to_string()),
        ];

        let result = get_transform_spec(&all_fields);
<<<<<<< HEAD
        assert!(result.is_empty()); // No partition columns = empty transform spec
    }

    #[test]
    fn test_get_transform_spec_with_partitions() {
=======
        assert!(result.is_empty()); // No metadata columns = empty transform spec
    }

    #[test]
    fn test_dynamic_column_transform() {
        // Test Dynamic column that exists physically (like _change_type in CDC files)
        let _schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
            StructField::nullable("_commit_version", DataType::LONG),
        ]));

        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Dynamic {
                logical_index: 1,
                physical_name: "_change_type".to_string(),
            },
            ColumnType::Partition(2),
        ];

        let transform_spec = get_transform_spec(&all_fields);
        assert_eq!(transform_spec.len(), 2); // Dynamic and Metadata columns

        // Test transform expression with physical _change_type
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
        ]);
        let mut metadata_values = HashMap::new();
        metadata_values.insert(2, ("_commit_version".to_string(), Scalar::Long(42)));

        let result = get_transform_expr(&transform_spec, metadata_values, &physical_schema);
        assert!(result.is_ok());
        // The transform should drop and reinsert _change_type for consistent ordering

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
        assert!(result.is_ok());
        // The transform should insert _change_type from metadata
    }

    #[test]
    fn test_get_transform_spec_with_metadata() {
>>>>>>> 2b01c19 (what)
        let all_fields = vec![
            ColumnType::Selected("col1".to_string()),
            ColumnType::Partition(1),
            ColumnType::Selected("col2".to_string()),
            ColumnType::Partition(2),
        ];

        let result = get_transform_spec(&all_fields);
        assert_eq!(result.len(), 2);

<<<<<<< HEAD
        // Check first partition column
=======
        // Check first metadata column
>>>>>>> 2b01c19 (what)
        if let FieldTransformSpec::PartitionColumn {
            field_index,
            insert_after,
        } = &result[0]
        {
            assert_eq!(*field_index, 1);
            assert_eq!(insert_after.as_ref().unwrap(), "col1");
        } else {
            panic!("Expected PartitionColumn transform");
        }

<<<<<<< HEAD
        // Check second partition column
=======
        // Check second metadata column
>>>>>>> 2b01c19 (what)
        if let FieldTransformSpec::PartitionColumn {
            field_index,
            insert_after,
        } = &result[1]
        {
            assert_eq!(*field_index, 2);
            assert_eq!(insert_after.as_ref().unwrap(), "col2");
        } else {
            panic!("Expected PartitionColumn transform");
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
}
