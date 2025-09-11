use std::collections::HashMap;
use std::sync::Arc;

use crate::expressions::{Expression, ExpressionRef};
use crate::scan::FieldTransformSpec;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::DeltaResult;

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{ADD_CHANGE_TYPE, CHANGE_TYPE_COL_NAME, REMOVE_CHANGE_TYPE};

/// Parse partition values for the given transform spec and convert them to expressions.
/// This uses the core partition parsing function from the scan module and converts Scalars to Expressions.
pub(crate) fn parse_partition_values_to_expressions(
    logical_schema: &StructType,
    transform_spec: &[FieldTransformSpec],
    partition_values: &HashMap<String, String>,
) -> DeltaResult<HashMap<usize, (String, Expression)>> {
    // Use the core partition parsing function from scan module
    let scalars = crate::scan::parse_partition_values_to_scalars(
        logical_schema,
        transform_spec,
        partition_values,
    )?;

    // Convert Scalars to Expressions
    Ok(scalars
        .into_iter()
        .map(|(field_idx, (name, scalar))| (field_idx, (name, scalar.into())))
        .collect())
}

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
    // Build all field values in one pass
    let mut field_values =
        parse_partition_values_to_expressions(logical_schema, transform_spec, partition_values)?;

    // Add CDF constants
    for field_transform in transform_spec {
        if let FieldTransformSpec::Cdf {
            col_type,
            field_index,
            ..
        } = field_transform
        {
            if let Some((_, field)) = logical_schema.fields.get_index(*field_index) {
                if let Some(expr) = create_cdf_expression(col_type, scan_file) {
                    field_values.insert(*field_index, (field.name().to_string(), expr));
                }
            }
        }
    }

    // Find the _change_type insert_after for Add/Remove files
    let change_type_insert_after = if scan_file.scan_type != CdfScanFileType::Cdc {
        transform_spec.iter().find_map(|field_transform| {
            if let FieldTransformSpec::Cdf {
                col_type: crate::scan::CdfCol::ChangeType(_),
                insert_after,
                ..
            } = field_transform
            {
                Some(insert_after)
            } else {
                None
            }
        })
    } else {
        None
    };

    // Build the transform expression
    let mut transform = crate::expressions::Transform::new_top_level();
    let use_change_type_insert_after =
        scan_file.scan_type != CdfScanFileType::Cdc && change_type_insert_after.is_some();

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
                    let actual_insert_after = if use_change_type_insert_after {
                        change_type_insert_after.unwrap()
                    } else {
                        insert_after
                    };
                    transform.with_inserted_field(actual_insert_after.clone(), field_value)
                } else {
                    transform
                }
            }
            Cdf {
                col_type,
                field_index,
                insert_after,
            } => {
                let should_use_change_type_insert_after = use_change_type_insert_after
                    && !matches!(col_type, crate::scan::CdfCol::ChangeType(_));

                let actual_insert_after = if should_use_change_type_insert_after {
                    change_type_insert_after.unwrap()
                } else {
                    insert_after
                };

                if let Some((_, field_value)) = field_values.remove(field_index) {
                    let field_value = Arc::new(field_value);
                    transform.with_inserted_field(actual_insert_after.clone(), field_value)
                } else {
                    transform
                }
            }
        }
    }

    Ok(Arc::new(Expression::Transform(transform)))
}

/// Creates the appropriate expression for a CDF column type
fn create_cdf_expression(
    col_type: &crate::scan::CdfCol,
    scan_file: &CdfScanFile,
) -> Option<Expression> {
    match col_type {
        crate::scan::CdfCol::ChangeType(_) => match scan_file.scan_type {
            CdfScanFileType::Add => Some(Expression::literal(ADD_CHANGE_TYPE)),
            CdfScanFileType::Remove => Some(Expression::literal(REMOVE_CHANGE_TYPE)),
            CdfScanFileType::Cdc => None, // Comes from physical data
        },
        crate::scan::CdfCol::CommitVersion => Some(Expression::literal(scan_file.commit_version)),
        crate::scan::CdfCol::CommitTimestamp => {
            crate::expressions::Scalar::timestamp_from_millis(scan_file.commit_timestamp)
                .ok()
                .map(Expression::literal)
        }
    }
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
            ColumnType::Cdf {
                col_type: crate::scan::CdfCol::ChangeType(CHANGE_TYPE_COL_NAME.to_string()),
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
            ColumnType::Cdf {
                col_type: crate::scan::CdfCol::ChangeType(CHANGE_TYPE_COL_NAME.to_string()),
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
