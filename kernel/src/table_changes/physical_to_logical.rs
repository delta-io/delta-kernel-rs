use std::collections::HashMap;
use std::sync::Arc;

use crate::expressions::{Expression, ExpressionRef};
use crate::scan::FieldTransformSpec;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error};

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, REMOVE_CHANGE_TYPE};

/// Gets the physical schema that will be used to read data in the `scan_file` path.
pub(crate) fn scan_file_physical_schema(
    scan_file: &CdfScanFile,
    physical_schema: &StructType,
) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING);
        let fields = physical_schema.fields().cloned().chain(Some(change_type));
        StructType::new(fields).into()
    } else {
        physical_schema.clone().into()
    }
}

/// Creates a CDF-specific transform expression by combining partition values with CDF metadata.
/// This is the CDF-specific wrapper around the generic get_transform_expr function.
pub(crate) fn get_cdf_transform_expr(
    transform_spec: &[FieldTransformSpec],
    partition_values: &HashMap<String, String>,
    logical_schema: &StructType,
    scan_file: &CdfScanFile,
) -> DeltaResult<ExpressionRef> {
    // Create CDF constant value map
    // Convert partition values to expressions using proper data type conversion
    let mut field_values =
        parse_partition_values_to_expressions(logical_schema, transform_spec, partition_values)?;

    // Add CDF constants directly to the indexed field values map
    for field_transform in transform_spec {
        match field_transform {
            FieldTransformSpec::CdfChangeType { field_index, .. } => {
                // For Add/Remove files, inject a constant _change_type value; for CDC, it's in the data.
                if let Some((_, field)) = logical_schema.fields.get_index(*field_index) {
                    let expr = match scan_file.scan_type {
                        CdfScanFileType::Add => Some(Expression::literal(ADD_CHANGE_TYPE)),
                        CdfScanFileType::Remove => Some(Expression::literal(REMOVE_CHANGE_TYPE)),
                        CdfScanFileType::Cdc => None,
                    };
                    if let Some(expression) = expr {
                        field_values.insert(*field_index, (field.name().to_string(), expression));
                    }
                }
            }
            FieldTransformSpec::CdfCommitVersion { field_index, .. } => {
                if let Some((_, field)) = logical_schema.fields.get_index(*field_index) {
                    field_values.insert(
                        *field_index,
                        (
                            field.name().to_string(),
                            Expression::literal(scan_file.commit_version),
                        ),
                    );
                }
            }
            FieldTransformSpec::CdfCommitTimestamp { field_index, .. } => {
                if let (Ok(timestamp), Some((_, field))) = (
                    crate::expressions::Scalar::timestamp_from_millis(scan_file.commit_timestamp),
                    logical_schema.fields.get_index(*field_index),
                ) {
                    field_values.insert(
                        *field_index,
                        (field.name().to_string(), Expression::literal(timestamp)),
                    );
                }
            }
            _ => continue,
        }
    }

    // For Add/Remove files, adjust insert_after for fields that should come after _change_type
    //
    // Problem: When _change_type is generated (not physical), other generated columns like
    // _commit_version and _commit_timestamp that reference _change_type in their insert_after
    // will fail because _change_type doesn't exist in the physical schema yet.
    //
    // Solution: For Add/Remove files, we adjust the insert_after of these columns to match
    // the insert_after of _change_type itself, so they all get inserted after the same
    // physical column.
    //
    // Example:
    // - Physical schema: [id, name]
    // - _change_type wants to insert after "name"
    // - _commit_version wants to insert after "_change_type" (fails!)
    // - After adjustment: _commit_version also inserts after "name"
    // - Result: [id, name, _change_type, _commit_version] ✅
    let modified_transform_spec = if scan_file.scan_type != CdfScanFileType::Cdc {
        adjust_insert_after_for_generated_change_type(transform_spec)
    } else {
        transform_spec.to_vec()
    };

    build_cdf_transform_expr(&modified_transform_spec, field_values)
}

/// Adjusts insert_after for fields that should come after _change_type when _change_type is generated
///
/// This function fixes the column ordering issue where generated columns try to insert after
/// another generated column (_change_type) that doesn't exist in the physical schema yet.
///
/// It works by:
/// 1. Finding the insert_after value of the _change_type column (which points to a physical column)
/// 2. Updating all other generated columns (_commit_version, _commit_timestamp, partition columns)
///    to use the same insert_after value as _change_type
///
/// This ensures all generated columns are inserted after the same physical column, maintaining
/// the correct order: [physical_cols..., _change_type, _commit_version, _commit_timestamp, ...]
fn adjust_insert_after_for_generated_change_type(
    transform_spec: &[FieldTransformSpec],
) -> Vec<FieldTransformSpec> {
    // Find the insert_after value for _change_type
    let change_type_insert_after = transform_spec
        .iter()
        .find_map(|field_transform| {
            if let FieldTransformSpec::CdfChangeType { insert_after, .. } = field_transform {
                Some(insert_after)
            } else {
                None
            }
        })
        .expect("CdfChangeType should always be present in transform spec");

    // Update fields that should come after _change_type to use the same insert_after
    transform_spec
        .iter()
        .map(|field_transform| match field_transform {
            FieldTransformSpec::CdfCommitVersion { field_index, .. } => {
                FieldTransformSpec::CdfCommitVersion {
                    field_index: *field_index,
                    insert_after: change_type_insert_after.clone(),
                }
            }
            FieldTransformSpec::CdfCommitTimestamp { field_index, .. } => {
                FieldTransformSpec::CdfCommitTimestamp {
                    field_index: *field_index,
                    insert_after: change_type_insert_after.clone(),
                }
            }
            FieldTransformSpec::PartitionColumn { field_index, .. } => {
                FieldTransformSpec::PartitionColumn {
                    field_index: *field_index,
                    insert_after: change_type_insert_after.clone(),
                }
            }
            _ => field_transform.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::expressions::Expression as Expr;
    use crate::scan::{ColumnType, Scan};
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::physical_to_logical::get_cdf_transform_expr;
    use crate::table_changes::scan_file::{CdfScanFile, CdfScanFileType};
    use crate::table_changes::{
        CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
    };

    fn create_test_scan_file(scan_type: CdfScanFileType) -> CdfScanFile {
        CdfScanFile {
            scan_type,
            path: "fake_path".to_string(),
            dv_info: Default::default(),
            remove_dv: None,
            partition_values: HashMap::from([("age".to_string(), "20".to_string())]),
            commit_version: 42,
            commit_timestamp: 1234,
        }
    }

    fn create_test_logical_schema() -> StructType {
        StructType::new([
            StructField::nullable("id", DataType::STRING),
            StructField::not_null("age", DataType::LONG),
            StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING),
            StructField::not_null(COMMIT_VERSION_COL_NAME, DataType::LONG),
            StructField::not_null(COMMIT_TIMESTAMP_COL_NAME, DataType::TIMESTAMP),
        ])
    }

    #[test]
    fn test_get_transform_expr() {
        let logical_schema = create_test_logical_schema();

        // Test with partition columns and CDF metadata
        let scan_file = create_test_scan_file(CdfScanFileType::Add);
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::Partition(1), // age column
            ColumnType::CdfChangeType {
                physical_name: CHANGE_TYPE_COL_NAME.to_string(),
                logical_idx: 2,
            },
        ];
        let transform_spec = Scan::get_transform_spec(&all_fields);
        let result = get_cdf_transform_expr(
            &transform_spec,
            &scan_file.partition_values,
            &logical_schema,
            &scan_file,
        );
        assert!(result.is_ok());
        assert!(matches!(result.unwrap().as_ref(), Expr::Transform(_)));

        // Test with null partition (the critical bug fix)
        let mut null_scan_file = create_test_scan_file(CdfScanFileType::Add);
        null_scan_file.partition_values.clear();
        let result = get_cdf_transform_expr(
            &transform_spec,
            &null_scan_file.partition_values,
            &logical_schema,
            &null_scan_file,
        );
        assert!(result.is_ok());
        assert!(matches!(result.unwrap().as_ref(), Expr::Transform(_)));
    }

    #[test]
    fn test_dynamic_metadata_physical_schema_check() {
        let logical_schema = create_test_logical_schema();

        // Test 1: Physical schema WITHOUT _change_type (Add/Remove files)

        let add_scan_file = create_test_scan_file(CdfScanFileType::Add);
        let all_fields = vec![
            ColumnType::Selected("id".to_string()),
            ColumnType::CdfChangeType {
                physical_name: CHANGE_TYPE_COL_NAME.to_string(),
                logical_idx: 2,
            },
        ];
        let transform_spec = Scan::get_transform_spec(&all_fields);

        let result = get_cdf_transform_expr(
            &transform_spec,
            &add_scan_file.partition_values,
            &logical_schema,
            &add_scan_file,
        );
        assert!(
            result.is_ok(),
            "Should generate _change_type for Add file without it in physical schema"
        );

        // Test 2: Physical schema WITH _change_type (CDC files)

        let cdc_scan_file = create_test_scan_file(CdfScanFileType::Cdc);
        let result = get_cdf_transform_expr(
            &transform_spec,
            &cdc_scan_file.partition_values,
            &logical_schema,
            &cdc_scan_file,
        );
        assert!(
            result.is_ok(),
            "Should NOT generate _change_type for CDC file with it in physical schema"
        );

        // Test 3: Verify different scan types generate correct expressions
        let remove_scan_file = create_test_scan_file(CdfScanFileType::Remove);
        let result = get_cdf_transform_expr(
            &transform_spec,
            &remove_scan_file.partition_values,
            &logical_schema,
            &remove_scan_file,
        );
        assert!(
            result.is_ok(),
            "Should generate _change_type for Remove file without it in physical schema"
        );
    }
}

/// Build a transform expression from field transform specifications and field values.
pub(crate) fn build_cdf_transform_expr(
    transform_spec: &[FieldTransformSpec],
    mut field_values: HashMap<usize, (String, Expression)>,
) -> DeltaResult<ExpressionRef> {
    let mut transform = crate::expressions::Transform::new();

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
                if let Some((_, field_value)) = field_values.remove(field_index) {
                    let field_value = Arc::new(field_value);
                    transform.with_inserted_field(insert_after.clone(), field_value)
                } else {
                    // Missing field value - skip this transform step.
                    // This can happen when a metadata column isn't applicable to this file type.
                    transform
                }
            }
            CdfChangeType {
                field_index,
                insert_after,
            } => {
                if let Some((_, field_value)) = field_values.remove(field_index) {
                    let field_value = Arc::new(field_value);
                    transform.with_inserted_field(insert_after.clone(), field_value)
                } else {
                    // Missing field value - skip this transform step
                    transform
                }
            }
            CdfCommitVersion {
                field_index,
                insert_after,
            } => {
                // _commit_version is always generated (never in physical data)
                if let Some((_, field_value)) = field_values.remove(field_index) {
                    let field_value = Arc::new(field_value);
                    transform.with_inserted_field(insert_after.clone(), field_value)
                } else {
                    // Missing field value - skip this transform step.
                    transform
                }
            }
            CdfCommitTimestamp {
                field_index,
                insert_after,
            } => {
                // _commit_timestamp is always generated (never in physical data)
                if let Some((_, field_value)) = field_values.remove(field_index) {
                    let field_value = Arc::new(field_value);
                    transform.with_inserted_field(insert_after.clone(), field_value)
                } else {
                    // Missing field value - skip this transform step.
                    transform
                }
            }
        }
    }

    Ok(Arc::new(Expression::Transform(transform)))
}

/// Parse partition values for the given transform spec and convert them to expressions.
fn parse_partition_values_to_expressions(
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
    partition_values: &HashMap<String, String>,
) -> DeltaResult<HashMap<usize, (String, Expression)>> {
    let mut result = HashMap::new();

    // Process all partition columns in the transform spec
    for field_transform in transform_spec {
        let field_index = match field_transform {
            FieldTransformSpec::PartitionColumn { field_index, .. } => *field_index,
            _ => continue,
        };

        let field = logical_schema.fields.get_index(field_index);
        let Some((_, field)) = field else {
            return Err(Error::InternalError(format!(
                "out of bounds field index {field_index}"
            )));
        };
        let physical_name = field.physical_name();

        // Convert string partition value to expression
        let partition_value = crate::scan::parse_partition_value(
            partition_values.get(physical_name),
            field.data_type(),
        )?;
        result.insert(
            field_index,
            (field.name().to_string(), partition_value.into()),
        );
    }

    Ok(result)
}
