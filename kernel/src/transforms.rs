//! Transform-related types and utilities for Delta Kernel.
//!
//! This module contains the types and functions needed to handle transforms
//! during scan and table changes operations, including partition value processing
//! and expression generation.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

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
}

/// A list of field transforms that describes a transform expression to be created at scan time.
pub(crate) type TransformSpec = Vec<FieldTransformSpec>;

/// Transforms aren't computed all at once. So static ones can just go straight to `Expression`, but
/// things like partition columns need to filled in. This enum holds an expression that's part of a
/// [`TransformSpec`].
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
    /// Inserts a partition column after the named input column. The partition column is identified
    /// by its field index in the logical table schema (the column is not present in the physical
    /// read schema). Its value varies from file to file and is obtained from file metadata.
    PartitionColumn {
        field_index: usize,
        insert_after: Option<String>,
    },
}

/// Parse a single partition value from the raw string representation
pub(crate) fn parse_partition_value(
    field_idx: usize,
    logical_schema: &SchemaRef,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<(usize, (String, crate::expressions::Scalar))> {
    let Some(field) = logical_schema.field_at_index(field_idx) else {
        return Err(Error::InternalError(format!(
            "out of bounds partition column field index {field_idx}"
        )));
    };
    let name = field.physical_name();
    let partition_value = parse_partition_value_raw(partition_values.get(name), field.data_type())?;
    Ok((field_idx, (name.to_string(), partition_value)))
}

/// Parse all partition values from a transform spec
pub(crate) fn parse_partition_values(
    logical_schema: &SchemaRef,
    transform_spec: &TransformSpec,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<HashMap<usize, (String, crate::expressions::Scalar)>> {
    transform_spec
        .iter()
        .filter_map(|field_transform| match field_transform {
            FieldTransformSpec::PartitionColumn { field_index, .. } => Some(parse_partition_value(
                *field_index,
                logical_schema,
                partition_values,
            )),
            FieldTransformSpec::StaticInsert { .. }
            | FieldTransformSpec::StaticReplace { .. }
            | FieldTransformSpec::StaticDrop { .. } => None,
        })
        .try_collect()
}

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
        }
    }

    Ok(Arc::new(Expression::Transform(transform)))
}

/// Computes the transform spec for this scan. Static (query-level) transforms can already be
/// turned into expressions now, but file-level transforms like partition values can only be
/// described now; they are converted to expressions during the scan, using file metadata.
///
/// NOTE: Transforms are "sparse" in the sense that they only mention fields which actually
/// change (added, replaced, dropped); the transform implicitly captures all fields that pass
/// from input to output unchanged and in the same relative order.
pub(crate) fn get_transform_spec(all_fields: &[ColumnType]) -> TransformSpec {
    let mut transform_spec = TransformSpec::new();
    let mut last_physical_field: Option<&str> = None;

    for field in all_fields {
        match field {
            ColumnType::Selected(physical_name) => {
                // Track physical field for calculating partition value insertion points.
                last_physical_field = Some(physical_name);
            }
            ColumnType::Partition(logical_idx) => {
                transform_spec.push(FieldTransformSpec::PartitionColumn {
                    insert_after: last_physical_field.map(String::from),
                    field_index: *logical_idx,
                });
            }
        }
    }

    transform_spec
}

/// Parse a partition value from the raw string representation
/// This was originally `parse_partition_value` in scan/mod.rs
pub(crate) fn parse_partition_value_raw(
    raw: Option<&String>,
    data_type: &DataType,
) -> DeltaResult<crate::expressions::Scalar> {
    use crate::expressions::Scalar;
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
    use crate::expressions::Scalar;
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

        let result = get_transform_expr(&transform_spec, partition_values);
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
        let partition_values = HashMap::new();

        let result = get_transform_expr(&transform_spec, partition_values).unwrap();
        assert!(matches!(result.as_ref(), Expression::Transform(_)));
    }

    #[test]
    fn test_get_transform_spec_selected_only() {
        let all_fields = vec![
            ColumnType::Selected("col1".to_string()),
            ColumnType::Selected("col2".to_string()),
        ];

        let result = get_transform_spec(&all_fields);
        assert!(result.is_empty()); // No partition columns = empty transform spec
    }

    #[test]
    fn test_get_transform_spec_with_partitions() {
        let all_fields = vec![
            ColumnType::Selected("col1".to_string()),
            ColumnType::Partition(1),
            ColumnType::Selected("col2".to_string()),
            ColumnType::Partition(2),
        ];

        let result = get_transform_spec(&all_fields);
        assert_eq!(result.len(), 2);

        // Check first partition column
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

        // Check second partition column
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
